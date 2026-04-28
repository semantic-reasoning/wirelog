/*
 * bench_compound.c - Compound Term Microbenchmarks (Issue #536)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Measures latency of compound-term hot paths introduced in #530-#535:
 *
 *   --mode parser      parse Datalog programs with compound term syntax
 *   --mode match       inline compound equality filter (wl_col_rel_inline_compound_equals)
 *   --mode semijoin    nested-loop semijoin with compound keys
 *   --mode multi-graph named-graph filter via compound graph-name column
 *
 * Output (two lines per mode, stdout):
 *   mode=<name> iters=<N> p50=<X>us p95=<X>us p99=<X>us
 *   peak_rss_kb=<N>
 *
 * Usage:
 *   bench_compound --mode MODE [--iters N]
 */

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1  /* expose ru_maxrss over strict _POSIX_C_SOURCE */
#endif

#include "bench_argv.h"
#include "bench_util.h"

/* Shared sort + percentile helpers; consolidated in #599 to keep the
 * private percentile math in tests/test_log_perf_gate.c, this bench,
 * and tests/test_rotate_latency.c from drifting.  test_perf_util.h
 * unconditionally pulls in <math.h>, <string.h>, <stdio.h>, <stdlib.h>;
 * including it before the project headers below is intentional. */
#include "../tests/test_perf_util.h"

#include "../wirelog/columnar/internal.h"
#include "../wirelog/parser/ast.h"
#include "../wirelog/parser/parser.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
/* Windows has no <sys/resource.h>; RSS reporting is stubbed below.
 * Cross-platform timing routes through bench_util.h (QueryPerformanceCounter). */
#else
#include <sys/resource.h>
#endif

/* ======================================================================== */
/* Configuration                                                            */
/* ======================================================================== */

#define DEFAULT_ITERS   10000
/* Number of rows in relations for match / semijoin / multi-graph modes.
* Keep small enough to stay L1-cache resident during the timing loop. */
#define BENCH_NROWS     256u
/* Named graphs for multi-graph mode. */
#define BENCH_NGRAPHS   4u

/* ======================================================================== */
/* Timing helpers                                                           */
/* ======================================================================== */

/* Sort samples and print p50/p95/p99 percentiles.  Shares the sort
 * comparator + percentile pick with #599's rotate-latency gate via
 * tests/test_perf_util.h to keep the percentile math from forking
 * across consumers. */
static void
report_pct(const char *mode, int iters, double *samples)
{
    size_t n = (size_t)iters;
    qsort(samples, n, sizeof(double), wl_perf_cmp_double);
    double p50 = wl_perf_percentile_ms(samples, n, 0.50);
    double p95 = wl_perf_percentile_ms(samples, n, 0.95);
    double p99 = wl_perf_percentile_ms(samples, n, 0.99);
    printf("mode=%s iters=%d p50=%.2fus p95=%.2fus p99=%.2fus\n",
        mode, iters, p50, p95, p99);
    fflush(stdout);
}

/* Report peak RSS (kilobytes).
 * Linux: getrusage ru_maxrss is already in KB.
 * macOS: getrusage ru_maxrss is in bytes.
 * Windows: no getrusage; stub to -1 (bench still emits the line so the
 * smoke test's peak_rss_kb= assertion holds cross-platform). */
static void
report_rss(void)
{
#if defined(_WIN32)
    printf("peak_rss_kb=-1\n");
    fflush(stdout);
#else
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        printf("peak_rss_kb=-1\n");
        fflush(stdout);
        return;
    }
#if defined(__APPLE__)
    long rss_kb = (long)(ru.ru_maxrss / 1024L);
#else
    long rss_kb = (long)ru.ru_maxrss;
#endif
    printf("peak_rss_kb=%ld\n", rss_kb);
    fflush(stdout);
#endif
}

/* ======================================================================== */
/* Mode: parser                                                             */
/* Parse a compound-term-heavy Datalog program string N times.             */
/* ======================================================================== */

/* Program containing compound terms: f(X,Y), g(Y,b), graph(G,H).
 * Compound terms appear only in rule body atoms (parser limitation: head
 * positions are scalar).  Covers functor call sites in AST node type
 * WL_PARSER_AST_NODE_COMPOUND_TERM (Issue #530). */
static const char *COMPOUND_SRC =
    ".decl edge(x: symbol, y: symbol)\n"
    ".decl path(x: symbol, y: symbol)\n"
    ".decl named(g: symbol, x: symbol)\n"
    "path(X, Y) :- edge(X, Y).\n"
    "path(X, Z) :- path(X, Y), edge(Y, Z).\n"
    "named(X, Y) :- edge(X, Y), path(f(X, a), Y).\n"
    "named(X, Y) :- edge(X, Y), path(g(a, X), f(Y, b)).\n"
    "named(X, Y) :- path(X, graph(Y, a)), edge(f(X, b), g(Y, c)).\n";

static void
run_parser(int iters)
{
    double *samples = (double *)malloc((size_t)iters * sizeof(double));
    if (!samples) {
        fprintf(stderr, "bench_compound: parser: out of memory\n");
        return;
    }

    char err[256];
    int ok = 1;

    for (int i = 0; i < iters; i++) {
        bench_time_t t0 = bench_time_now();
        wl_parser_ast_node_t *ast =
            wl_parser_parse_string(COMPOUND_SRC, err, sizeof(err));
        bench_time_t t1 = bench_time_now();

        if (!ast) {
            fprintf(stderr, "bench_compound: parser: parse error: %s\n", err);
            ok = 0;
            break;
        }
        wl_parser_ast_node_free(ast);
        samples[i] = bench_time_diff_ms(t0, t1) * 1000.0;
    }

    if (ok) {
        report_pct("parser", iters, samples);
        report_rss();
    }
    free(samples);
}

/* ======================================================================== */
/* Shared fixture: inline compound relation (arity 2)                      */
/*                                                                          */
/* Schema: 1 logical column, INLINE kind, arity 2.                         */
/* Physical: 2 slots.  Values are deterministic pseudo-random (LCG).       */
/* ======================================================================== */

static uint32_t
lcg_next(uint32_t s)
{
    return s * 1664525u + 1013904223u;
}

static col_rel_t *
make_inline2_rel(const char *name, uint32_t nrows, uint32_t seed)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, name) != 0)
        return NULL;

    const col_rel_logical_col_t logical[1] = {
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
    };
    if (col_rel_apply_compound_schema(r, logical, 1u) != 0)
        goto fail;
    /* Physical ncols = arity = 2. */
    if (col_rel_set_schema(r, 2u, NULL) != 0)
        goto fail;

    for (uint32_t i = 0; i < nrows; i++) {
        seed = lcg_next(seed);
        int64_t a0 = (int64_t)(seed & 0xFFFF);
        seed = lcg_next(seed);
        int64_t a1 = (int64_t)(seed & 0xFFFF);

        const int64_t row[2] = { a0, a1 };
        if (col_rel_append_row(r, row) != 0)
            goto fail;
    }
    return r;

fail:
    col_rel_destroy(r);
    return NULL;
}

/* ======================================================================== */
/* Mode: match                                                              */
/* Scan BENCH_NROWS inline compound rows; count equality hits per iter.    */
/* ======================================================================== */

static void
run_match(int iters)
{
    col_rel_t *r = make_inline2_rel("match_rel", BENCH_NROWS, 0xABCDu);
    if (!r) {
        fprintf(stderr, "bench_compound: match: relation alloc failed\n");
        return;
    }

    /* Probe matches row 0 exactly (guaranteed one hit per scan). */
    int64_t probe[2] = { 0, 0 };
    if (wl_col_rel_retrieve_inline_compound(r, 0u, 0u, probe, 2u) != 0) {
        fprintf(stderr, "bench_compound: match: retrieve probe failed\n");
        col_rel_destroy(r);
        return;
    }

    double *samples = (double *)malloc((size_t)iters * sizeof(double));
    if (!samples) {
        fprintf(stderr, "bench_compound: match: out of memory\n");
        col_rel_destroy(r);
        return;
    }

    volatile int64_t sink = 0; /* prevent dead-code elimination */
    uint32_t nrows = r->nrows;

    for (int i = 0; i < iters; i++) {
        bench_time_t t0 = bench_time_now();
        int64_t hits = 0;
        for (uint32_t row = 0; row < nrows; row++) {
            if (wl_col_rel_inline_compound_equals(r, row, 0u, probe, 2u))
                hits++;
        }
        bench_time_t t1 = bench_time_now();
        sink = hits;
        samples[i] = bench_time_diff_ms(t0, t1) * 1000.0;
    }
    (void)sink;

    report_pct("match", iters, samples);
    report_rss();
    free(samples);
    col_rel_destroy(r);
}

/* ======================================================================== */
/* Mode: semijoin                                                           */
/* Nested-loop semijoin: for each probe row find first matching build row. */
/* Probe and build share seed 0x1111 so 25% rows overlap.                  */
/* ======================================================================== */

static void
run_semijoin(int iters)
{
    /* Build: BENCH_NROWS/4 rows (25% selectivity vs probe). */
    col_rel_t *probe = make_inline2_rel("sj_probe", BENCH_NROWS,      0x1111u);
    col_rel_t *build = make_inline2_rel("sj_build", BENCH_NROWS / 4u, 0x1111u);
    if (!probe || !build) {
        fprintf(stderr, "bench_compound: semijoin: relation alloc failed\n");
        col_rel_destroy(probe);
        col_rel_destroy(build);
        return;
    }

    double *samples = (double *)malloc((size_t)iters * sizeof(double));
    if (!samples) {
        fprintf(stderr, "bench_compound: semijoin: out of memory\n");
        col_rel_destroy(probe);
        col_rel_destroy(build);
        return;
    }

    volatile int64_t sink = 0;
    uint32_t prows = probe->nrows;
    uint32_t brows = build->nrows;

    for (int i = 0; i < iters; i++) {
        bench_time_t t0 = bench_time_now();
        int64_t hits = 0;
        int64_t p[2] = { 0, 0 };

        for (uint32_t pi = 0; pi < prows; pi++) {
            if (wl_col_rel_retrieve_inline_compound(probe, pi, 0u, p, 2u) != 0)
                continue;
            for (uint32_t bi = 0; bi < brows; bi++) {
                if (wl_col_rel_inline_compound_equals(build, bi, 0u, p, 2u)) {
                    hits++;
                    break; /* semijoin: first match is sufficient */
                }
            }
        }
        bench_time_t t1 = bench_time_now();
        sink = hits;
        samples[i] = bench_time_diff_ms(t0, t1) * 1000.0;
    }
    (void)sink;

    report_pct("semijoin", iters, samples);
    report_rss();
    free(samples);
    col_rel_destroy(probe);
    col_rel_destroy(build);
}

/* ======================================================================== */
/* Mode: multi-graph                                                        */
/* Named-graph filter: compound (graph_id, graph_rev) + scalar payload.    */
/*                                                                          */
/* Schema: 2 logical columns.                                               */
/*   logical 0: INLINE arity 2  (graph_id, graph_rev)                      */
/*   logical 1: NONE             (payload)                                  */
/* Physical: 2 + 1 = 3 slots.                                               */
/* ======================================================================== */

static col_rel_t *
make_multigraph_rel(uint32_t rows_per_graph)
{
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, "mg_rel") != 0)
        return NULL;

    const col_rel_logical_col_t logical[2] = {
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
    };
    if (col_rel_apply_compound_schema(r, logical, 2u) != 0)
        goto fail;
    /* Physical: 2 (inline/2) + 1 (scalar) = 3. */
    if (col_rel_set_schema(r, 3u, NULL) != 0)
        goto fail;

    for (uint32_t g = 0; g < BENCH_NGRAPHS; g++) {
        for (uint32_t j = 0; j < rows_per_graph; j++) {
            /* graph name stored inline as (g, 0); payload = j */
            const int64_t row[3] = { (int64_t)g, 0LL, (int64_t)j };
            if (col_rel_append_row(r, row) != 0)
                goto fail;
        }
    }
    return r;

fail:
    col_rel_destroy(r);
    return NULL;
}

static void
run_multigraph(int iters)
{
    uint32_t rows_per_graph = BENCH_NROWS / BENCH_NGRAPHS;
    col_rel_t *r = make_multigraph_rel(rows_per_graph);
    if (!r) {
        fprintf(stderr, "bench_compound: multi-graph: relation alloc failed\n");
        return;
    }

    /* Filter for graph (2, 0) — 25% of all rows should match. */
    const int64_t probe[2] = { 2LL, 0LL };

    double *samples = (double *)malloc((size_t)iters * sizeof(double));
    if (!samples) {
        fprintf(stderr, "bench_compound: multi-graph: out of memory\n");
        col_rel_destroy(r);
        return;
    }

    volatile int64_t sink = 0;
    uint32_t nrows = r->nrows;

    for (int i = 0; i < iters; i++) {
        bench_time_t t0 = bench_time_now();
        int64_t count = 0;
        for (uint32_t row = 0; row < nrows; row++) {
            if (wl_col_rel_inline_compound_equals(r, row, 0u, probe, 2u))
                count++;
        }
        bench_time_t t1 = bench_time_now();
        sink = count;
        samples[i] = bench_time_diff_ms(t0, t1) * 1000.0;
    }
    (void)sink;

    report_pct("multi-graph", iters, samples);
    report_rss();
    free(samples);
    col_rel_destroy(r);
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

static void
usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s --mode MODE [--iters N]\n"
        "\n"
        "  --mode MODE    Benchmark mode: parser | match | semijoin | multi-graph\n"
        "  --iters N      Number of timing samples (default: %d)\n"
        "  --help         Show this message\n",
        prog, DEFAULT_ITERS);
}

int
main(int argc, char **argv)
{
    static const bench_argv_long_t longs[] = {
        { "mode",  required_argument, 'm' },
        { "iters", required_argument, 'n' },
        { "help",  no_argument,       'h' },
        { NULL,    0,                  0  },
    };

    const char *mode = NULL;
    int iters = DEFAULT_ITERS;

    bench_argv_state_t st = BENCH_ARGV_INIT;
    int opt;
    while ((opt = bench_argv_next(argc, argv, "m:n:h", longs, &st)) != -1) {
        switch (opt) {
        case 'm': mode = st.optarg;        break;
        case 'n': iters = (int)strtol(st.optarg, NULL, 10); break;
        case 'h': usage(argv[0]);           return 0;
        default:  usage(argv[0]);           return 1;
        }
    }

    if (!mode) {
        fprintf(stderr, "bench_compound: --mode is required\n");
        usage(argv[0]);
        return 1;
    }
    if (iters < 1) {
        fprintf(stderr, "bench_compound: --iters must be >= 1\n");
        return 1;
    }

    bench_stability_prep();

    if (strcmp(mode, "parser") == 0) {
        run_parser(iters);
    } else if (strcmp(mode, "match") == 0) {
        run_match(iters);
    } else if (strcmp(mode, "semijoin") == 0) {
        run_semijoin(iters);
    } else if (strcmp(mode, "multi-graph") == 0) {
        run_multigraph(iters);
    } else {
        fprintf(stderr, "bench_compound: unknown mode '%s'\n", mode);
        usage(argv[0]);
        return 1;
    }

    return 0;
}
