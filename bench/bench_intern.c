/*
 * bench_intern.c - Intern Write-Path Microbenchmark (Issue #1472)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Measures the cost of wl_intern_put() on the shared symbol table:
 *
 *   --mode unique   every put inserts a new string (speculative copy,
 *                   writer lock, lookup, governor reservation, publish)
 *   --mode dup      every put hits an existing id (speculative copy,
 *                   writer lock, lookup, free)
 *
 * for 1, 4 and 8 writer threads, with and without an enforcing memory
 * governor attached to the table (#1431).  --iters is the total number
 * of puts per scenario, split evenly across the writers, so every
 * scenario walks the same table growth schedule and the writer axis
 * measures contention only.
 *
 * Output: one clock_ns=<N> line (cost of the clock pair around a put),
 * one line per scenario, then one peak_rss_kb line:
 *   mode=<unique|dup> writers=<N> governor=<on|off> iters=<total>
 *   puts_per_writer=<N> ns_put=<mean bracketed latency, includes lock
 *   wait and the clock> agg_ns_put=<wall over total puts> p50=<ns>
 *   p99=<ns> max=<ns> grow_thr_ns=<16 x max(p50, clock)>
 *   grow_events=<count or n/a> grow_share=<time share or n/a>
 *   wall_ms=<N> [gov_reserved_delta=<N>] clock_included_ns=<N>
 *
 * grow_events/grow_share count the puts whose latency exceeds the
 * threshold; with one writer these are the slot-array resizes and id
 * segment opens, with several writers lock waits dominate the tail, so
 * the fields are reported only for writers=1.  gov_reserved_delta is
 * printed for governed duplicate puts and must be 0: a duplicate never
 * reserves.  The bench exits 1 on any denied put, an unexpected final
 * count or a non-zero duplicate delta.
 *
 * Usage:
 *   bench_intern [--mode unique|dup|all] [--writers N|all]
 *                [--iters N] [--governor on|off|both]
 */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1  /* same prologue as bench_compound.c */
#endif
#include "bench_argv.h"
#include "bench_util.h"
#include "../tests/test_perf_util.h"
#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/intern.h"
#include "../wirelog/thread.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BENCH_MAX_WRITERS 64
#define BENCH_MAX_ITERS   999999999u
#define BENCH_DEFAULT_ITERS 200000u
#define BENCH_KEY_LEN 16
#define BENCH_DUP_KEYS 1024u
#define BENCH_GOVERNOR_BYTES (UINT64_C(1) << 30)
#define BENCH_GROW_THR_MULT 16u

typedef enum {
    BENCH_MODE_UNIQUE,
    BENCH_MODE_DUP,
} bench_mode_t;

/* Start barrier: writers park until main releases them together. */
typedef struct {
    wl_mutex_t lock;
    wl_cond_t cond;
    int ready;
    int go;
} bench_barrier_t;

typedef struct {
    wl_intern_t *intern;
    const char *keys;     /* fixed-width key block */
    uint32_t nkeys;       /* keys in the block */
    uint32_t puts;        /* puts this writer performs */
    wl_perf_ns_t *lat;    /* per-put latency, puts entries, or NULL */
    bench_barrier_t *barrier;
    wl_perf_ns_t t_start;
    wl_perf_ns_t t_end;
    int failed;
} bench_writer_t;

static const char *
mode_name(bench_mode_t mode)
{
    return mode == BENCH_MODE_UNIQUE ? "unique" : "dup";
}

static void
usage(const char *argv0)
{
    fprintf(stderr,
        "Usage: %s [--mode unique|dup|all] [--writers N|all] [--iters N]\n"
        "          [--governor on|off|both]\n"
        "  --mode      unique inserts, duplicate hits, or both (default all)\n"
        "  --writers   writer threads, 1..%d, or all = 1,4,8 (default all)\n"
        "  --iters     total puts per scenario, split across writers\n"
        "              (default %u, max %u)\n"
        "  --governor  attach an enforcing memory governor (default both)\n",
        argv0, BENCH_MAX_WRITERS, BENCH_DEFAULT_ITERS, BENCH_MAX_ITERS);
}

/* Decimal argument; anything but digits yields 0, which every bound
 * check below rejects. */
static unsigned long
parse_number(const char *text)
{
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    if (end == text || *end != '\0')
        return 0;
    return value;
}

static void *
writer_main(void *arg)
{
    bench_writer_t *w = (bench_writer_t *)arg;
    wl_intern_t *intern = w->intern;
    const char *keys = w->keys;
    const uint32_t nkeys = w->nkeys;
    const uint32_t puts = w->puts;
    wl_perf_ns_t *lat = w->lat;
    int failed = 0;

    wl_mutex_lock(&w->barrier->lock);
    w->barrier->ready++;
    wl_cond_broadcast(&w->barrier->cond);
    while (!w->barrier->go)
        wl_cond_wait(&w->barrier->cond, &w->barrier->lock);
    wl_mutex_unlock(&w->barrier->lock);

    w->t_start = wl_perf_now_ns();
    for (uint32_t i = 0; i < puts; i++) {
        const char *key = keys + (size_t)(i % nkeys) * BENCH_KEY_LEN;
        wl_perf_ns_t t0 = wl_perf_now_ns();
        int64_t id = wl_intern_put(intern, key);
        wl_perf_ns_t t1 = wl_perf_now_ns();
        if (id < 0)
            failed = 1;
        if (lat)
            lat[i] = t1 - t0;
    }
    w->t_end = wl_perf_now_ns();
    w->failed = failed;
    return NULL;
}

/* Fixed-width, NUL-terminated keys.  Unique keys carry the writer index
 * so blocks never collide across writers; duplicate keys use another
 * prefix so a pre-interned key can never match a unique one. */
static char *
make_keys(uint32_t count, int writer, bool unique)
{
    char *keys = (char *)calloc((size_t)count, BENCH_KEY_LEN);
    if (!keys)
        return NULL;
    for (uint32_t i = 0; i < count; i++) {
        char *key = keys + (size_t)i * BENCH_KEY_LEN;
        if (unique)
            snprintf(key, BENCH_KEY_LEN, "u%02d-%09u", writer, i);
        else
            snprintf(key, BENCH_KEY_LEN, "d%09u", i);
    }
    return keys;
}

static wl_columnar_memory_governor_ref_t *
make_governor(void)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = BENCH_GOVERNOR_BYTES;
    resolution.usable_bytes = BENCH_GOVERNOR_BYTES;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static uint64_t
governor_reserved(wl_columnar_memory_governor_ref_t *ref)
{
    return ref ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref)) : 0;
}

/* Cost of the clock pair that brackets every put, so readers can subtract
 * it from ns_put; agg_ns_put is derived from wall time and already
 * includes it. */
static wl_perf_ns_t
measure_clock_ns(void)
{
    enum { SAMPLES = 4096 };
    static wl_perf_ns_t samples[SAMPLES];
    for (int i = 0; i < SAMPLES; i++) {
        wl_perf_ns_t t0 = wl_perf_now_ns();
        wl_perf_ns_t t1 = wl_perf_now_ns();
        samples[i] = t1 - t0;
    }
    qsort(samples, SAMPLES, sizeof(samples[0]), wl_perf_cmp_ns);
    return wl_perf_percentile_ns(samples, SAMPLES, 0.50);
}

/*
 * Run one scenario on a fresh table.  With @timed false this is the
 * warm-up: same shape, a tenth of the puts, nothing reported.  Returns 0
 * on success, -1 when a put was denied or the table ended with an
 * unexpected count.
 */
static int
run_scenario(bench_mode_t mode, int writers, bool governed, uint32_t iters,
    bool timed, wl_perf_ns_t clock_ns)
{
    wl_columnar_memory_governor_ref_t *ref = NULL;
    wl_intern_t *intern = NULL;
    bench_writer_t *ws = NULL;
    wl_thread_t threads[BENCH_MAX_WRITERS];
    char *dup_keys = NULL;
    wl_perf_ns_t *lat = NULL;
    bench_barrier_t barrier;
    uint64_t reserved_before = 0;
    uint32_t per_writer = iters / (uint32_t)writers;
    uint32_t remainder = iters % (uint32_t)writers;
    uint32_t expected_count;
    uint64_t total_puts = 0;
    int started = 0;
    int rc = -1;

    memset(&barrier, 0, sizeof(barrier));
    if (wl_mutex_init(&barrier.lock) != 0)
        return -1;
    if (wl_cond_init(&barrier.cond) != 0) {
        wl_mutex_destroy(&barrier.lock);
        return -1;
    }

    intern = wl_intern_create();
    if (!intern)
        goto alloc_failed;
    if (governed) {
        ref = make_governor();
        if (!ref || wl_intern_attach_memory_governor(intern, ref) != 0)
            goto alloc_failed;
    }

    ws = (bench_writer_t *)calloc((size_t)writers, sizeof(*ws));
    if (!ws)
        goto alloc_failed;
    if (timed) {
        lat = (wl_perf_ns_t *)calloc((size_t)iters, sizeof(*lat));
        if (!lat)
            goto alloc_failed;
    }
    if (mode == BENCH_MODE_DUP) {
        dup_keys = make_keys(BENCH_DUP_KEYS, 0, false);
        if (!dup_keys)
            goto alloc_failed;
        for (uint32_t i = 0; i < BENCH_DUP_KEYS; i++) {
            if (wl_intern_put(intern,
                dup_keys + (size_t)i * BENCH_KEY_LEN) < 0)
                goto alloc_failed;
        }
        expected_count = BENCH_DUP_KEYS;
    } else {
        expected_count = iters;
    }

    for (int t = 0; t < writers; t++) {
        bench_writer_t *w = &ws[t];
        w->intern = intern;
        w->barrier = &barrier;
        w->puts = per_writer + (t == writers - 1 ? remainder : 0u);
        if (mode == BENCH_MODE_DUP) {
            w->keys = dup_keys;
            w->nkeys = BENCH_DUP_KEYS;
        } else {
            w->keys = make_keys(w->puts > 0 ? w->puts : 1u, t, true);
            w->nkeys = w->puts > 0 ? w->puts : 1u;
            if (!w->keys)
                goto alloc_failed;
        }
        w->lat = lat ? lat + total_puts : NULL;
        total_puts += w->puts;
    }

    reserved_before = governor_reserved(ref);

    for (started = 0; started < writers; started++) {
        if (wl_thread_create(&threads[started], writer_main, &ws[started])
            != 0)
            break;
    }
    wl_mutex_lock(&barrier.lock);
    while (barrier.ready < started)
        wl_cond_wait(&barrier.cond, &barrier.lock);
    barrier.go = 1;
    wl_cond_broadcast(&barrier.cond);
    wl_mutex_unlock(&barrier.lock);
    for (int t = 0; t < started; t++)
        wl_thread_join(&threads[t]);
    if (started != writers) {
        printf("mode=%s writers=%d governor=%s error=wl_thread_create\n",
            mode_name(mode), writers, governed ? "on" : "off");
        goto out;
    }

    for (int t = 0; t < writers; t++) {
        if (ws[t].failed) {
            printf("mode=%s writers=%d governor=%s error=put_failed\n",
                mode_name(mode), writers, governed ? "on" : "off");
            goto out;
        }
    }
    if (wl_intern_count(intern) != expected_count) {
        printf("mode=%s writers=%d governor=%s error=count "
            "expected=%u actual=%u\n", mode_name(mode), writers,
            governed ? "on" : "off", expected_count,
            wl_intern_count(intern));
        goto out;
    }

    if (timed) {
        wl_perf_ns_t first_start = ws[0].t_start;
        wl_perf_ns_t last_end = ws[0].t_end;
        uint64_t sum = 0;
        uint64_t grow_sum = 0;
        uint64_t grow_events = 0;
        wl_perf_ns_t p50, p99, max, thr;
        double wall_ns;

        for (int t = 1; t < writers; t++) {
            if (ws[t].t_start < first_start)
                first_start = ws[t].t_start;
            if (ws[t].t_end > last_end)
                last_end = ws[t].t_end;
        }
        wall_ns = (double)(last_end - first_start);
        for (uint64_t i = 0; i < total_puts; i++)
            sum += lat[i];
        qsort(lat, (size_t)total_puts, sizeof(*lat), wl_perf_cmp_ns);
        p50 = wl_perf_percentile_ns(lat, (size_t)total_puts, 0.50);
        p99 = wl_perf_percentile_ns(lat, (size_t)total_puts, 0.99);
        max = total_puts ? lat[total_puts - 1] : 0;
        /* Floor the threshold at the clock cost: on a coarse clock p50 can
         * round to zero and every put would count as growth. */
        thr = (p50 > clock_ns ? p50 : clock_ns) * BENCH_GROW_THR_MULT;
        for (uint64_t i = total_puts; i > 0; i--) {
            if (lat[i - 1] <= thr)
                break;
            grow_events++;
            grow_sum += lat[i - 1];
        }

        printf("mode=%s writers=%d governor=%s iters=%u puts_per_writer=%u "
            "ns_put=%.1f agg_ns_put=%.1f p50=%" PRIu64 " p99=%" PRIu64
            " max=%" PRIu64 " grow_thr_ns=%" PRIu64,
            mode_name(mode), writers, governed ? "on" : "off", iters,
            per_writer,
            total_puts ? (double)sum / (double)total_puts : 0.0,
            total_puts ? wall_ns / (double)total_puts : 0.0,
            (uint64_t)p50, (uint64_t)p99, (uint64_t)max, (uint64_t)thr);
        if (writers == 1)
            printf(" grow_events=%" PRIu64 " grow_share=%.2f%%",
                grow_events,
                sum ? 100.0 * (double)grow_sum / (double)sum : 0.0);
        else
            printf(" grow_events=n/a grow_share=n/a");
        printf(" wall_ms=%.3f", wall_ns / 1e6);
        if (mode == BENCH_MODE_DUP && governed) {
            uint64_t reserved_after = governor_reserved(ref);
            int64_t delta = (int64_t)reserved_after
                - (int64_t)reserved_before;
            printf(" gov_reserved_delta=%" PRId64, delta);
            if (delta != 0) {
                printf(" error=dup_reserved\n");
                goto out;
            }
        }
        printf(" clock_included_ns=%" PRIu64 "\n", (uint64_t)clock_ns);
        fflush(stdout);
    }
    rc = 0;
    goto out;

alloc_failed:
    printf("mode=%s writers=%d governor=%s error=alloc\n", mode_name(mode),
        writers, governed ? "on" : "off");

out:
    if (ws) {
        if (mode == BENCH_MODE_UNIQUE) {
            for (int t = 0; t < writers; t++)
                free((void *)ws[t].keys);
        }
        free(ws);
    }
    free(dup_keys);
    free(lat);
    if (intern)
        wl_intern_free(intern);
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    wl_cond_destroy(&barrier.cond);
    wl_mutex_destroy(&barrier.lock);
    return rc;
}

static void
report_rss(void)
{
    int64_t rss_kb = bench_peak_rss_kb();
    printf("peak_rss_kb=%" PRId64 "\n", rss_kb);
    fflush(stdout);
}

int
main(int argc, char **argv)
{
    static const bench_argv_long_t longs[] = {
        { "mode",     required_argument, 'm' },
        { "writers",  required_argument, 'w' },
        { "iters",    required_argument, 'n' },
        { "governor", required_argument, 'g' },
        { "help",     no_argument,       'h' },
        { NULL,       0,                  0  },
    };
    static const int all_writers[] = { 1, 4, 8 };
    const char *mode_arg = "all";
    const char *writers_arg = "all";
    const char *governor_arg = "both";
    unsigned long iters_arg = BENCH_DEFAULT_ITERS;
    int writers_list[3];
    int nwriters = 0;
    bool run_unique, run_dup, run_off, run_on;
    bool full_size_warmup_done = false;
    wl_perf_ns_t clock_ns;

    bench_argv_state_t st = BENCH_ARGV_INIT;
    int opt;
    while ((opt = bench_argv_next(argc, argv, "m:w:n:g:h", longs, &st))
        != -1) {
        switch (opt) {
        case 'm': mode_arg = st.optarg; break;
        case 'w': writers_arg = st.optarg; break;
        case 'n': iters_arg = parse_number(st.optarg); break;
        case 'g': governor_arg = st.optarg; break;
        case 'h': usage(argv[0]); return 0;
        default:  usage(argv[0]); return 1;
        }
    }

    run_unique = strcmp(mode_arg, "unique") == 0
        || strcmp(mode_arg, "all") == 0;
    run_dup = strcmp(mode_arg, "dup") == 0 || strcmp(mode_arg, "all") == 0;
    if (!run_unique && !run_dup) {
        fprintf(stderr, "bench_intern: unknown mode '%s'\n", mode_arg);
        return 1;
    }
    run_off = strcmp(governor_arg, "off") == 0
        || strcmp(governor_arg, "both") == 0;
    run_on = strcmp(governor_arg, "on") == 0
        || strcmp(governor_arg, "both") == 0;
    if (!run_off && !run_on) {
        fprintf(stderr, "bench_intern: unknown governor setting '%s'\n",
            governor_arg);
        return 1;
    }
    if (strcmp(writers_arg, "all") == 0) {
        for (int i = 0; i < 3; i++)
            writers_list[nwriters++] = all_writers[i];
    } else {
        unsigned long w = parse_number(writers_arg);
        if (w < 1 || w > BENCH_MAX_WRITERS) {
            fprintf(stderr, "bench_intern: --writers must be 1..%d or all\n",
                BENCH_MAX_WRITERS);
            return 1;
        }
        writers_list[nwriters++] = (int)w;
    }
    if (iters_arg < 1 || iters_arg > BENCH_MAX_ITERS) {
        fprintf(stderr, "bench_intern: --iters must be 1..%u\n",
            BENCH_MAX_ITERS);
        return 1;
    }

    /* Do not pin the process to one CPU (bench_stability_prep): the
     * writer threads must be able to contend on distinct cores. */
    clock_ns = measure_clock_ns();
    printf("clock_ns=%" PRIu64 "\n", (uint64_t)clock_ns);

    for (int m = 0; m < 2; m++) {
        bench_mode_t mode = m == 0 ? BENCH_MODE_UNIQUE : BENCH_MODE_DUP;
        if ((mode == BENCH_MODE_UNIQUE && !run_unique)
            || (mode == BENCH_MODE_DUP && !run_dup))
            continue;
        for (int wi = 0; wi < nwriters; wi++) {
            for (int g = 0; g < 2; g++) {
                bool governed = g == 1;
                uint32_t iters = (uint32_t)iters_arg;
                uint32_t warm = iters / 10u;
                if ((governed && !run_on) || (!governed && !run_off))
                    continue;
                /* Pre-fault a full-size throwaway table before the first
                 * reported row.  Later scenarios retain the lightweight
                 * warm-up so their setup cost stays bounded. */
                if (!full_size_warmup_done) {
                    if (run_scenario(mode, writers_list[wi], governed, iters,
                        false, clock_ns) != 0)
                        return 1;
                    full_size_warmup_done = true;
                }
                if (warm > 0
                    && run_scenario(mode, writers_list[wi], governed, warm,
                    false, clock_ns) != 0)
                    return 1;
                if (run_scenario(mode, writers_list[wi], governed, iters,
                    true, clock_ns) != 0)
                    return 1;
            }
        }
    }
    report_rss();
    return 0;
}
