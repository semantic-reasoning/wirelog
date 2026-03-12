/*
 * bench_flowlog.c - FlowLog Benchmark Driver
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Runs FlowLog-style Datalog workloads and measures wall-clock time
 * and peak RSS memory.  Output is TSV for easy consumption by scripts.
 *
 * Note: CSV data is converted to inline Datalog facts because the
 * .input CSV loading feature is not yet merged to main.
 *
 * Usage:
 *   bench_flowlog --workload {tc|reach|cc|sssp|all} --data FILE
 *                 [--data-weighted FILE] [--workers N] [--repeat R]
 */

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

#include "bench_util.h"

#include <inttypes.h>

#include "../wirelog/backend.h"
#include "../wirelog/backend/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

/* Forward declaration for CSE cache statistics extraction */
extern void
col_session_get_cache_stats(wl_session_t *sess, uint64_t *out_hits,
                            uint64_t *out_misses);

/* ----------------------------------------------------------------
 * Global output format and per-run profiling accumulators (3B-003)
 *
 * g_last_consolidation_ns / g_last_kfusion_ns are written by
 * run_pipeline_count after each trial and read by workload reporters.
 * They reflect the most-recent (last) run — acceptable for MVP.
 *
 * WITH_K_FUSION: compile-time constant injected by the meson target.
 *   bench_flowlog     → WITH_K_FUSION=1 (default, K-fusion enabled)
 *   bench_flowlog_seq → WITH_K_FUSION=0 (sequential baseline)
 * ---------------------------------------------------------------- */
static bool g_format_json = false;
static uint64_t g_last_consolidation_ns = 0;
static uint64_t g_last_kfusion_ns = 0;
static uint64_t g_last_kfusion_alloc_ns = 0;
static uint64_t g_last_kfusion_dispatch_ns = 0;
static uint64_t g_last_kfusion_merge_ns = 0;
static uint64_t g_last_kfusion_cleanup_ns = 0;

#ifndef WITH_K_FUSION
#define WITH_K_FUSION 1
#endif

/* Forward declaration — defined after print_header() */
static void
output_json_row(const char *wl_name, int32_t edges, uint32_t workers,
                int repeat, double min_ms, double median_ms, double max_ms,
                int64_t peak_rss_kb, int64_t tuples, uint32_t iters,
                uint64_t consolidation_ns, uint64_t kfusion_ns,
                uint64_t kfusion_alloc_ns, uint64_t kfusion_dispatch_ns,
                uint64_t kfusion_merge_ns, uint64_t kfusion_cleanup_ns);

#ifndef _MSC_VER
/* getopt.h and getopt_long are POSIX-only; not available on Windows MSVC */
#include <getopt.h>
#else
/* Windows MSVC: getopt/getopt_long not available */
extern int
getopt(int argc, char *const argv[], const char *optstring);
extern int optind;
extern char *optarg;

/* Stub struct option for MSVC compatibility (getopt_long not used) */
struct option {
    const char *name;
    int has_arg;
    int *flag;
    int val;
};
#define no_argument 0
#define required_argument 1

/* MSVC: strtok_r not available, use thread-local strtok fallback */
#define strtok_r(str, delim, saveptr) strtok((str), (delim))
#endif

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * CSV to inline facts conversion
 * ---------------------------------------------------------------- */

/* Maximum source buffer (128 KB should handle several thousand edges) */
#define SRC_BUFSZ ((size_t)128 * 1024)

static int
csv_to_inline_facts(const char *csv_path, const char *relation, char *buf,
                    size_t bufsz, int32_t *out_edge_count)
{
    FILE *f = fopen(csv_path, "r");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", csv_path);
        return -1;
    }

    size_t pos = 0;
    int32_t count = 0;
    char line[256];

    while (fgets(line, sizeof(line), f)) {
        /* Strip trailing newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0)
            continue;

        /* Parse comma-separated integers */
        int32_t vals[8];
        int ncols = 0;
        char *saveptr = NULL;
        char *tok = strtok_r(line, ",", &saveptr);
        while (tok && ncols < 8) {
            vals[ncols++] = (int32_t)strtol(tok, NULL, 10);
            tok = strtok_r(NULL, ",", &saveptr);
        }
        if (ncols == 0)
            continue;

        /* Emit: relation(v1, v2, ...). */
        int n = snprintf(buf + pos, bufsz - pos, "%s(", relation);
        if (n < 0 || pos + (size_t)n >= bufsz)
            goto overflow;
        pos += (size_t)n;

        for (int c = 0; c < ncols; c++) {
            if (c > 0) {
                n = snprintf(buf + pos, bufsz - pos, ", ");
                if (n < 0 || pos + (size_t)n >= bufsz)
                    goto overflow;
                pos += (size_t)n;
            }
            n = snprintf(buf + pos, bufsz - pos, "%d", vals[c]);
            if (n < 0 || pos + (size_t)n >= bufsz)
                goto overflow;
            pos += (size_t)n;
        }

        n = snprintf(buf + pos, bufsz - pos, "). ");
        if (n < 0 || pos + (size_t)n >= bufsz)
            goto overflow;
        pos += (size_t)n;

        count++;
    }

    fclose(f);
    if (pos < bufsz)
        buf[pos] = '\0';
    if (out_edge_count)
        *out_edge_count = count;
    return 0;

overflow:
    fclose(f);
    fprintf(stderr, "error: source buffer overflow at %d edges\n", count);
    return -1;
}

/* ----------------------------------------------------------------
 * Workload definitions
 * ---------------------------------------------------------------- */

#define WL_TC 0
#define WL_REACH 1
#define WL_CC 2
#define WL_SSSP 3
#define WL_SG 4
#define WL_BP 5
#define WL_COUNT 6

static const char *wl_names[WL_COUNT]
    = { "tc", "reach", "cc", "sssp", "sg", "bipartite" };

/* Relation name used for CSV-to-facts conversion per workload */
static const char *wl_relations[WL_COUNT]
    = { "edge", "edge", "edge", "wedge", "edge", "edge" };

/* Templates: %s is replaced by inline facts block */
static const char *wl_templates[WL_COUNT] = {
    /* TC */
    ".decl edge(x: int32, y: int32)\n"
    "%s\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n",
    /* Reach */
    ".decl edge(x: int32, y: int32)\n"
    "%s\n"
    ".decl reach(x: int32)\n"
    "reach(1).\n"
    "reach(y) :- reach(x), edge(x, y).\n",
    /* CC (Connected Components) */
    ".decl edge(x: int32, y: int32)\n"
    "%s\n"
    ".decl cc(x: int32, c: int32)\n"
    "cc(x, x) :- edge(x, _).\n"
    "cc(x, x) :- edge(_, x).\n"
    "cc(y, min(c)) :- cc(x, c), edge(x, y).\n",
    /* SSSP (Single-Source Shortest Path) */
    ".decl wedge(x: int32, y: int32, w: int32)\n"
    "%s\n"
    ".decl dist(x: int32, d: int32)\n"
    "dist(1, 0).\n"
    "dist(y, min(d + w)) :- dist(x, d), wedge(x, y, w).\n",
    /* SG (Same Generation) */
    ".decl edge(x: int32, y: int32)\n"
    "%s\n"
    ".decl sg(x: int32, y: int32)\n"
    "sg(x, y) :- edge(p, x), edge(p, y), x != y.\n"
    "sg(x, y) :- edge(a, x), sg(a, b), edge(b, y).\n",
    /* Bipartite Detection */
    ".decl edge(x: int32, y: int32)\n"
    "%s\n"
    ".decl blue(x: int32)\n"
    ".decl red(x: int32)\n"
    ".decl not_bipartite(x: int32)\n"
    "blue(1).\n"
    "red(y) :- blue(x), edge(x, y).\n"
    "blue(y) :- red(x), edge(x, y).\n"
    "not_bipartite(x) :- blue(x), red(x).\n",
};

/* ----------------------------------------------------------------
 * Tuple counting callback
 * ---------------------------------------------------------------- */

struct count_ctx {
    int64_t count;
};

static void
count_tuple_cb(const char *relation, const int64_t *row, uint32_t ncols,
               void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
}

/* ----------------------------------------------------------------
 * Pipeline with counting
 * ---------------------------------------------------------------- */

static int
run_pipeline_count(const char *source, uint32_t num_workers, int64_t *out_count,
                   uint32_t *out_iters)
{
    if (!source)
        return -1;

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return -1;

    /* Optimize */
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_input_files(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_tuple_cb, &ctx);

    uint32_t total_iters = col_session_get_iteration_count(sess);

    /* Extract CSE cache statistics before destroying session */
    uint64_t cache_hits = 0, cache_misses = 0;
    col_session_get_cache_stats(sess, &cache_hits, &cache_misses);
    if (cache_hits + cache_misses > 0) {
        double hit_rate
            = 100.0 * (double)cache_hits / (double)(cache_hits + cache_misses);
        fprintf(stderr,
                "CSE Cache: %" PRIu64 " hits, %" PRIu64
                " misses (hit_rate=%.1f%%)\n",
                cache_hits, cache_misses, hit_rate);
    }

    /* Store profiling counters for the workload reporter (3B-003).
     * Written to globals so workload functions can include them in output
     * without changing the run_pipeline_count signature. */
    col_session_get_perf_stats(
        sess, &g_last_consolidation_ns, &g_last_kfusion_ns,
        &g_last_kfusion_alloc_ns, &g_last_kfusion_dispatch_ns,
        &g_last_kfusion_merge_ns, &g_last_kfusion_cleanup_ns);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    if (rc == 0 && out_count)
        *out_count = ctx.count;
    if (out_iters)
        *out_iters = total_iters;

    return rc;
}

/* ----------------------------------------------------------------
 * Run a single workload
 * ---------------------------------------------------------------- */

static int
run_workload(int wl_id, const char *data_path, uint32_t workers, int repeat)
{
    /* Convert CSV to inline facts */
    char *facts_buf = (char *)malloc(SRC_BUFSZ);
    if (!facts_buf)
        return -1;

    int32_t edge_count = 0;
    if (csv_to_inline_facts(data_path, wl_relations[wl_id], facts_buf,
                            SRC_BUFSZ, &edge_count)
        != 0) {
        free(facts_buf);
        return -1;
    }

    /* Build Datalog source with inline facts */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        free(facts_buf);
        return -1;
    }

    int n = snprintf(source, SRC_BUFSZ, wl_templates[wl_id], facts_buf);
    free(facts_buf);
    if (n < 0 || (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    /* Sort times for min/median/max */
    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        int32_t nodes = (edge_count > 0) ? edge_count + 1 : 0;

        if (g_format_json)
            output_json_row(wl_names[wl_id], edge_count, workers, repeat,
                            min_ms, median_ms, max_ms, peak_rss, tuples,
                            total_iters, g_last_consolidation_ns,
                            g_last_kfusion_ns, g_last_kfusion_alloc_ns,
                            g_last_kfusion_dispatch_ns, g_last_kfusion_merge_ns,
                            g_last_kfusion_cleanup_ns);
        else
            printf("%s\t%d\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
                   "\t%u\t%s\n",
                   wl_names[wl_id], nodes, edge_count, workers, repeat, min_ms,
                   median_ms, max_ms, peak_rss, tuples, total_iters, "OK");
    } else {
        printf("%s\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", wl_names[wl_id],
               workers, repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * Andersen pointer analysis (multi-relation workload)
 * ---------------------------------------------------------------- */

static const char *andersen_template
    = ".decl addressOf(v: int32, o: int32)\n"
      "%s\n"
      ".decl assign(v1: int32, v2: int32)\n"
      "%s\n"
      ".decl load(v1: int32, v2: int32)\n"
      "%s\n"
      ".decl store(v1: int32, v2: int32)\n"
      "%s\n"
      ".decl pointsTo(v: int32, o: int32)\n"
      "pointsTo(v, o) :- addressOf(v, o).\n"
      "pointsTo(v1, o) :- assign(v1, v2), pointsTo(v2, o).\n"
      "pointsTo(v1, o) :- load(v1, v2), pointsTo(v2, p), pointsTo(p, o).\n"
      "pointsTo(v1, o) :- store(v1, v2), pointsTo(v1, p), pointsTo(v2, o).\n";

static const char *andersen_rels[4]
    = { "addressOf", "assign", "load", "store" };
static const char *andersen_files[4]
    = { "addressOf.csv", "assign.csv", "load.csv", "store.csv" };

static int
run_andersen_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 4 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / 4;
    char *bufs[4];
    int32_t total_facts = 0;

    for (int i = 0; i < 4; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            for (int j = 0; j < i; j++)
                free(bufs[j]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, andersen_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, andersen_rels[i], bufs[i], per_buf,
                                &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build combined source */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < 4; i++)
            free(bufs[i]);
        return -1;
    }

    int n = snprintf(source, SRC_BUFSZ, andersen_template, bufs[0], bufs[1],
                     bufs[2], bufs[3]);
    for (int i = 0; i < 4; i++)
        free(bufs[i]);

    if (n < 0 || (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        if (g_format_json)
            output_json_row("andersen", total_facts, workers, repeat, min_ms,
                            median_ms, max_ms, peak_rss, tuples, total_iters,
                            g_last_consolidation_ns, g_last_kfusion_ns,
                            g_last_kfusion_alloc_ns, g_last_kfusion_dispatch_ns,
                            g_last_kfusion_merge_ns, g_last_kfusion_cleanup_ns);
        else
            printf("andersen\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64
                   "\t%" PRId64 "\t%u\t%s\n",
                   total_facts, workers, repeat, min_ms, median_ms, max_ms,
                   peak_rss, tuples, total_iters, "OK");
    } else {
        printf("andersen\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers,
               repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * Dyck-2 reachability (multi-relation workload)
 * ---------------------------------------------------------------- */

static const char *dyck_template
    = ".decl open1(x: int32, y: int32)\n"
      "%s\n"
      ".decl close1(x: int32, y: int32)\n"
      "%s\n"
      ".decl open2(x: int32, y: int32)\n"
      "%s\n"
      ".decl close2(x: int32, y: int32)\n"
      "%s\n"
      ".decl dyck(x: int32, y: int32)\n"
      "dyck(x, x) :- open1(x, _).\n"
      "dyck(x, x) :- close1(x, _).\n"
      "dyck(x, x) :- open2(x, _).\n"
      "dyck(x, x) :- close2(x, _).\n"
      "dyck(x, z) :- dyck(x, y), dyck(y, z).\n"
      "dyck(x, z) :- open1(x, y), dyck(y, w), close1(w, z).\n"
      "dyck(x, z) :- open2(x, y), dyck(y, w), close2(w, z).\n";

static const char *dyck_rels[4] = { "open1", "close1", "open2", "close2" };
static const char *dyck_files[4]
    = { "open1.csv", "close1.csv", "open2.csv", "close2.csv" };

static int
run_dyck_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 4 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / 4;
    char *bufs[4];
    int32_t total_facts = 0;

    for (int i = 0; i < 4; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            for (int j = 0; j < i; j++)
                free(bufs[j]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, dyck_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, dyck_rels[i], bufs[i], per_buf, &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build combined source */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < 4; i++)
            free(bufs[i]);
        return -1;
    }

    int n = snprintf(source, SRC_BUFSZ, dyck_template, bufs[0], bufs[1],
                     bufs[2], bufs[3]);
    for (int i = 0; i < 4; i++)
        free(bufs[i]);

    if (n < 0 || (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("dyck\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
               "\t%u\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, total_iters, "OK");
    } else {
        printf("dyck\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers, repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * CSPA - Context-Sensitive Points-to Analysis (multi-relation workload)
 * ---------------------------------------------------------------- */

static const char *cspa_template
    = ".decl assign(x: int32, y: int32)\n"
      "%s\n"
      ".decl dereference(x: int32, y: int32)\n"
      "%s\n"
      ".decl valueFlow(x: int32, y: int32)\n"
      ".decl memoryAlias(x: int32, y: int32)\n"
      ".decl valueAlias(x: int32, y: int32)\n"
      "valueFlow(y, x) :- assign(y, x).\n"
      "valueFlow(x, x) :- assign(x, _).\n"
      "valueFlow(x, x) :- assign(_, x).\n"
      "memoryAlias(x, x) :- assign(_, x).\n"
      "memoryAlias(x, x) :- assign(x, _).\n"
      "valueFlow(x, y) :- valueFlow(x, z), valueFlow(z, y).\n"
      "valueFlow(x, y) :- assign(x, z), memoryAlias(z, y).\n"
      "memoryAlias(x, w) :- dereference(y, x), valueAlias(y, z), "
      "dereference(z, w).\n"
      "valueAlias(x, y) :- valueFlow(z, x), valueFlow(z, y).\n"
      "valueAlias(x, y) :- valueFlow(z, x), memoryAlias(z, w), "
      "valueFlow(w, y).\n";

static const char *cspa_rels[2] = { "assign", "dereference" };
static const char *cspa_files[2] = { "assign.csv", "dereference.csv" };

static int
run_cspa_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 2 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / 2;
    char *bufs[2];
    int32_t total_facts = 0;

    for (int i = 0; i < 2; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            if (i > 0)
                free(bufs[0]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, cspa_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, cspa_rels[i], bufs[i], per_buf, &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build combined source */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < 2; i++)
            free(bufs[i]);
        return -1;
    }

    int n = snprintf(source, SRC_BUFSZ, cspa_template, bufs[0], bufs[1]);
    for (int i = 0; i < 2; i++)
        free(bufs[i]);

    if (n < 0 || (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        if (g_format_json)
            output_json_row("cspa", total_facts, workers, repeat, min_ms,
                            median_ms, max_ms, peak_rss, tuples, total_iters,
                            g_last_consolidation_ns, g_last_kfusion_ns,
                            g_last_kfusion_alloc_ns, g_last_kfusion_dispatch_ns,
                            g_last_kfusion_merge_ns, g_last_kfusion_cleanup_ns);
        else
            printf("cspa\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64
                   "\t%" PRId64 "\t%u\t%s\n",
                   total_facts, workers, repeat, min_ms, median_ms, max_ms,
                   peak_rss, tuples, total_iters, "OK");
    } else {
        printf("cspa\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers, repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * CSPA Incremental - Phase 4 incremental re-evaluation benchmark
 *
 * Demonstrates frontier-skip speedup:
 *   Run 1 (baseline): full fresh CSPA evaluation from scratch
 *   Run 2 (incremental):
 *     a. Initial eval on 100% facts (should match baseline)
 *     b. Insert 20% new facts via col_session_insert_incremental
 *     c. Re-eval: session_snapshot with frontier active (skip converged strata)
 *
 * Output columns (TSV):
 *   workload  facts  baseline_ms  initial_ms  insert_ms  reeval_ms
 *   speedup  tuples_before  tuples_after  iters_before  iters_after  status
 * ---------------------------------------------------------------- */

/*
 * csv_load_int64: Load a 2-column CSV into a freshly allocated int64_t array.
 * Returns number of rows loaded, or -1 on error.  *out_data must be freed by
 * the caller.  Rows beyond max_rows are silently ignored.
 */
static int32_t
csv_load_int64(const char *path, int64_t **out_data, uint32_t max_rows)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    int64_t *buf = (int64_t *)malloc(sizeof(int64_t) * 2 * (size_t)max_rows);
    if (!buf) {
        fclose(f);
        return -1;
    }

    int32_t count = 0;
    char line[256];
    while (fgets(line, sizeof(line), f) && (uint32_t)count < max_rows) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0)
            continue;

        char *saveptr = NULL;
        char *tok = strtok_r(line, ",", &saveptr);
        if (!tok)
            continue;
        int64_t v0 = strtol(tok, NULL, 10);
        tok = strtok_r(NULL, ",", &saveptr);
        if (!tok)
            continue;
        int64_t v1 = strtol(tok, NULL, 10);

        buf[(size_t)count * 2] = v0;
        buf[(size_t)count * 2 + 1] = v1;
        count++;
    }
    fclose(f);

    *out_data = buf;
    return count;
}

static int
run_cspa_incremental_workload(const char *data_dir, uint32_t workers,
                              int repeat)
{
    /* CSPA source: empty EDB declarations, IDB rules only.
     * Facts are loaded via col_session_insert_incremental.
     * NOTE: Currently unused (incremental measurement disabled).
     * Kept for reference: Phase 4+ delta-only evaluation. */
    static const char *cspa_incr_source
#ifndef _MSC_VER
        __attribute__((unused))
#endif
        = ".decl assign(x: int32, y: int32)\n"
          ".decl dereference(x: int32, y: int32)\n"
          ".decl valueFlow(x: int32, y: int32)\n"
          ".decl memoryAlias(x: int32, y: int32)\n"
          ".decl valueAlias(x: int32, y: int32)\n"
          "valueFlow(y, x) :- assign(y, x).\n"
          "valueFlow(x, x) :- assign(x, _).\n"
          "valueFlow(x, x) :- assign(_, x).\n"
          "memoryAlias(x, x) :- assign(_, x).\n"
          "memoryAlias(x, x) :- assign(x, _).\n"
          "valueFlow(x, y) :- valueFlow(x, z), valueFlow(z, y).\n"
          "valueFlow(x, y) :- assign(x, z), memoryAlias(z, y).\n"
          "memoryAlias(x, w) :- dereference(y, x), valueAlias(y, z), "
          "dereference(z, w).\n"
          "valueAlias(x, y) :- valueFlow(z, x), valueFlow(z, y).\n"
          "valueAlias(x, y) :- valueFlow(z, x), memoryAlias(z, w), "
          "valueFlow(w, y).\n";

    /* Load assign.csv and dereference.csv */
    char assign_path[1024], deref_path[1024];
    snprintf(assign_path, sizeof(assign_path), "%s/assign.csv", data_dir);
    snprintf(deref_path, sizeof(deref_path), "%s/dereference.csv", data_dir);

#define CSPA_INCR_MAX_ROWS 65536
    int64_t *assign_data = NULL;
    int64_t *deref_data = NULL;

    int32_t assign_count
        = csv_load_int64(assign_path, &assign_data, CSPA_INCR_MAX_ROWS);
    if (assign_count < 0)
        return -1;

    int32_t deref_count
        = csv_load_int64(deref_path, &deref_data, CSPA_INCR_MAX_ROWS);
    if (deref_count < 0) {
        free(assign_data);
        return -1;
    }

    int32_t total_facts = assign_count + deref_count;

    /* Generate 20% synthetic new assign facts by offsetting existing values */
    int32_t new_count = assign_count / 5;
    if (new_count < 1)
        new_count = 1;
    int64_t *new_assign
        = (int64_t *)malloc(sizeof(int64_t) * 2 * (size_t)new_count);
    if (!new_assign) {
        free(assign_data);
        free(deref_data);
        return -1;
    }
    /* Synthetic facts: shift node IDs by a large offset to ensure novelty */
    int64_t offset = 10000;
    for (int32_t i = 0; i < new_count; i++) {
        new_assign[(size_t)i * 2]
            = assign_data[(size_t)(i % assign_count) * 2] + offset;
        new_assign[(size_t)i * 2 + 1]
            = assign_data[(size_t)(i % assign_count) * 2 + 1] + offset;
    }

    /* ---- Run 1 (baseline): full fresh evaluation from scratch ----------- */
    double *baseline_times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!baseline_times) {
        free(assign_data);
        free(deref_data);
        free(new_assign);
        return -1;
    }

    int64_t baseline_tuples = 0;
    uint32_t baseline_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        int64_t cnt = 0;
        uint32_t iters = 0;
        /* Build full inline source for baseline */
        char *facts_assign = (char *)malloc(SRC_BUFSZ / 2);
        char *facts_deref = (char *)malloc(SRC_BUFSZ / 2);
        if (!facts_assign || !facts_deref) {
            free(facts_assign);
            free(facts_deref);
            status_ok = 0;
            break;
        }
        if (csv_to_inline_facts(assign_path, "assign", facts_assign,
                                SRC_BUFSZ / 2, NULL)
                != 0
            || csv_to_inline_facts(deref_path, "dereference", facts_deref,
                                   SRC_BUFSZ / 2, NULL)
                   != 0) {
            free(facts_assign);
            free(facts_deref);
            status_ok = 0;
            break;
        }
        char *src = (char *)malloc(SRC_BUFSZ);
        if (!src) {
            free(facts_assign);
            free(facts_deref);
            status_ok = 0;
            break;
        }
        int n = snprintf(src, SRC_BUFSZ, cspa_template, facts_assign,
                         facts_deref);
        free(facts_assign);
        free(facts_deref);
        if (n < 0 || (size_t)n >= SRC_BUFSZ) {
            free(src);
            status_ok = 0;
            break;
        }

        bench_time_t t0 = bench_time_now();
        int rc = run_pipeline_count(src, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();
        free(src);

        baseline_times[r] = bench_time_diff_ms(t0, t1);
        if (rc != 0) {
            status_ok = 0;
            break;
        }
        baseline_tuples = cnt;
        baseline_iters = iters;
    }

    if (!status_ok) {
        free(baseline_times);
        free(assign_data);
        free(deref_data);
        free(new_assign);
        printf("cspa_incr\t%d\t-\t-\t-\t-\t-\t-\t-\t-\t-\tFAIL\n", total_facts);
        return -1;
    }

    /* ---- Run 2 (incremental): initial eval + insert + re-eval ----------- */
    double *initial_times = (double *)malloc(sizeof(double) * (size_t)repeat);
    double *insert_times = (double *)malloc(sizeof(double) * (size_t)repeat);
    double *reeval_times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!initial_times || !insert_times || !reeval_times) {
        free(baseline_times);
        free(initial_times);
        free(insert_times);
        free(reeval_times);
        free(assign_data);
        free(deref_data);
        free(new_assign);
        return -1;
    }

    int64_t initial_tuples = 0;
    int64_t reeval_tuples = 0;
    uint32_t initial_iters = 0;
    uint32_t reeval_iters = 0;

    /* Parse program once — reused across repeat runs */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(cspa_incr_source, &err);
    if (!prog) {
        free(baseline_times);
        free(initial_times);
        free(insert_times);
        free(reeval_times);
        free(assign_data);
        free(deref_data);
        free(new_assign);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        free(baseline_times);
        free(initial_times);
        free(insert_times);
        free(reeval_times);
        free(assign_data);
        free(deref_data);
        free(new_assign);
        return -1;
    }

    for (int r = 0; r < repeat; r++) {
        /* Create a fresh session for each repeat */
        wl_session_t *sess = NULL;
        rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
        if (rc != 0) {
            status_ok = 0;
            break;
        }

        /* Load all facts incrementally (frontier-preserving) */
        rc = col_session_insert_incremental(sess, "assign", assign_data,
                                            (uint32_t)assign_count, 2);
        if (rc == 0)
            rc = col_session_insert_incremental(sess, "dereference", deref_data,
                                                (uint32_t)deref_count, 2);
        if (rc != 0) {
            wl_session_destroy(sess);
            status_ok = 0;
            break;
        }

        /* Initial evaluation (full, frontier not yet set) */
        struct count_ctx ctx1 = { 0 };
        bench_time_t t0 = bench_time_now();
        rc = wl_session_snapshot(sess, count_tuple_cb, &ctx1);
        bench_time_t t1 = bench_time_now();
        initial_times[r] = bench_time_diff_ms(t0, t1);
        if (rc != 0) {
            wl_session_destroy(sess);
            status_ok = 0;
            break;
        }
        initial_tuples = ctx1.count;
        (void)initial_tuples;
        initial_iters = col_session_get_iteration_count(sess);
        (void)initial_iters;

        /* Insert 20% new assign facts WITHOUT resetting frontier */
        bench_time_t ti0 = bench_time_now();
        rc = col_session_insert_incremental(sess, "assign", new_assign,
                                            (uint32_t)new_count, 2);
        bench_time_t ti1 = bench_time_now();
        insert_times[r] = bench_time_diff_ms(ti0, ti1);
        if (rc != 0) {
            wl_session_destroy(sess);
            status_ok = 0;
            break;
        }

        /* Incremental re-evaluation: frontier skip active for converged strata */
        struct count_ctx ctx2 = { 0 };
        bench_time_t tr0 = bench_time_now();
        rc = wl_session_snapshot(sess, count_tuple_cb, &ctx2);
        bench_time_t tr1 = bench_time_now();
        reeval_times[r] = bench_time_diff_ms(tr0, tr1);
        if (rc != 0) {
            wl_session_destroy(sess);
            status_ok = 0;
            break;
        }
        reeval_tuples = ctx2.count;
        reeval_iters = col_session_get_iteration_count(sess);

        wl_session_destroy(sess);
    }

    wl_plan_free(plan);
    wirelog_program_free(prog);
    free(assign_data);
    free(deref_data);
    free(new_assign);

    int64_t peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(baseline_times, (size_t)repeat, sizeof(double), bench_cmp_double);
        qsort(initial_times, (size_t)repeat, sizeof(double), bench_cmp_double);
        qsort(insert_times, (size_t)repeat, sizeof(double), bench_cmp_double);
        qsort(reeval_times, (size_t)repeat, sizeof(double), bench_cmp_double);

        double baseline_ms = baseline_times[repeat / 2];
        double initial_ms = initial_times[repeat / 2];
        double insert_ms = insert_times[repeat / 2];
        double reeval_ms = reeval_times[repeat / 2];
        double speedup = (reeval_ms > 0.0) ? baseline_ms / reeval_ms : 0.0;
        const char *status = (speedup >= 2.0) ? "OK" : "SLOW";

        /* Print custom incremental header on first line */
        printf("# cspa_incr columns: workload facts baseline_ms initial_ms "
               "insert_ms reeval_ms speedup tuples_before tuples_after "
               "iters_before iters_after peak_rss_kb status\n");
        printf("cspa_incr\t%d\t%.1f\t%.1f\t%.3f\t%.1f\t%.2f\t%" PRId64
               "\t%" PRId64 "\t%u\t%u\t%" PRId64 "\t%s\n",
               total_facts, baseline_ms, initial_ms, insert_ms, reeval_ms,
               speedup, baseline_tuples, reeval_tuples, baseline_iters,
               reeval_iters, peak_rss, status);

        fprintf(stderr,
                "cspa_incr: baseline=%.1fms initial=%.1fms insert=%.3fms "
                "reeval=%.1fms speedup=%.2fx tuples=%lld->%lld "
                "iters=%u->%u\n",
                baseline_ms, initial_ms, insert_ms, reeval_ms, speedup,
                (long long)baseline_tuples, (long long)reeval_tuples,
                baseline_iters, reeval_iters);
    } else {
        printf("cspa_incr\t%d\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\tFAIL\n",
               total_facts);
    }

    free(baseline_times);
    free(initial_times);
    free(insert_times);
    free(reeval_times);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * CSDA - Context-Sensitive Dataflow Analysis (multi-relation workload)
 * ---------------------------------------------------------------- */

static const char *csda_template
    = ".decl nullEdge(x: int32, y: int32)\n"
      "%s\n"
      ".decl edge(x: int32, y: int32)\n"
      "%s\n"
      ".decl nullNode(x: int32, y: int32)\n"
      "nullNode(x, y) :- nullEdge(x, y).\n"
      "nullNode(x, y) :- nullNode(x, w), edge(w, y).\n";

static const char *csda_rels[2] = { "nullEdge", "edge" };
static const char *csda_files[2] = { "nullEdge.csv", "edge.csv" };

static int
run_csda_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 2 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / 2;
    char *bufs[2];
    int32_t total_facts = 0;

    for (int i = 0; i < 2; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            if (i > 0)
                free(bufs[0]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, csda_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, csda_rels[i], bufs[i], per_buf, &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build combined source */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < 2; i++)
            free(bufs[i]);
        return -1;
    }

    int n = snprintf(source, SRC_BUFSZ, csda_template, bufs[0], bufs[1]);
    for (int i = 0; i < 2; i++)
        free(bufs[i]);

    if (n < 0 || (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("csda\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
               "\t%u\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, total_iters, "OK");
    } else {
        printf("csda\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers, repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * Galen - Medical Ontology Inference (multi-relation workload)
 * ---------------------------------------------------------------- */

static const char *galen_template
    = ".decl inputP(x: int32, y: int32)\n"
      "%s\n"
      ".decl inputQ(x: int32, y: int32, z: int32)\n"
      "%s\n"
      ".decl s(r: int32, p: int32)\n"
      "%s\n"
      ".decl rc(r: int32, p: int32, e: int32)\n"
      "%s\n"
      ".decl c(y: int32, w: int32, z: int32)\n"
      "%s\n"
      ".decl u(w: int32, r: int32, z: int32)\n"
      "%s\n"
      ".decl outP(x: int32, z: int32)\n"
      ".decl outQ(x: int32, y: int32, z: int32)\n"
      "outP(x, y) :- inputP(x, y).\n"
      "outQ(x, y, z) :- inputQ(x, y, z).\n"
      "outP(x, z) :- outP(x, y), outP(y, z).\n"
      "outQ(x, r, z) :- outP(x, y), outQ(y, r, z).\n"
      "outP(x, z) :- outP(y, w), u(w, r, z), outQ(x, r, y).\n"
      "outP(x, z) :- c(y, w, z), outP(x, w), outP(x, y).\n"
      "outQ(x, q, z) :- outQ(x, r, z), s(r, q).\n"
      "outQ(x, e, o) :- outQ(x, y, z), rc(y, u, e), outQ(z, u, o).\n";

#define GALEN_NRELS 6
static const char *galen_rels[GALEN_NRELS]
    = { "inputP", "inputQ", "s", "rc", "c", "u" };
static const char *galen_files[GALEN_NRELS]
    = { "inputP.csv", "inputQ.csv", "s.csv", "rc.csv", "c.csv", "u.csv" };

static int
run_galen_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 6 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / GALEN_NRELS;
    char *bufs[GALEN_NRELS];
    int32_t total_facts = 0;

    for (int i = 0; i < GALEN_NRELS; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            for (int j = 0; j < i; j++)
                free(bufs[j]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, galen_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, galen_rels[i], bufs[i], per_buf, &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build combined source */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < GALEN_NRELS; i++)
            free(bufs[i]);
        return -1;
    }

    int n = snprintf(source, SRC_BUFSZ, galen_template, bufs[0], bufs[1],
                     bufs[2], bufs[3], bufs[4], bufs[5]);
    for (int i = 0; i < GALEN_NRELS; i++)
        free(bufs[i]);

    if (n < 0 || (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("galen\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
               "\t%u\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, total_iters, "OK");
    } else {
        printf("galen\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers,
               repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * Polonius - Rust Borrow Checker (multi-relation workload)
 * ---------------------------------------------------------------- */

#define POLONIUS_NRELS 17
static const char *polonius_rels[POLONIUS_NRELS] = {
    "cfg_edge",
    "subset_base",
    "loan_issued_at",
    "loan_killed_at",
    "loan_invalidated_at",
    "universal_region",
    "known_placeholder_subset",
    "var_used_at",
    "var_defined_at",
    "var_dropped_at",
    "use_of_var_derefs_origin",
    "drop_of_var_derefs_origin",
    "child_path",
    "path_is_var",
    "path_assigned_at_base",
    "path_moved_at_base",
    "path_accessed_at_base",
};
static const char *polonius_files[POLONIUS_NRELS] = {
    "cfg_edge.csv",
    "subset_base.csv",
    "loan_issued_at.csv",
    "loan_killed_at.csv",
    "loan_invalidated_at.csv",
    "universal_region.csv",
    "known_placeholder_subset.csv",
    "var_used_at.csv",
    "var_defined_at.csv",
    "var_dropped_at.csv",
    "use_of_var_derefs_origin.csv",
    "drop_of_var_derefs_origin.csv",
    "child_path.csv",
    "path_is_var.csv",
    "path_assigned_at_base.csv",
    "path_moved_at_base.csv",
    "path_accessed_at_base.csv",
};
static const char *polonius_decls[POLONIUS_NRELS] = {
    ".decl cfg_edge(p1: int32, p2: int32)\n",
    ".decl subset_base(o1: int32, o2: int32, p: int32)\n",
    ".decl loan_issued_at(o: int32, l: int32, p: int32)\n",
    ".decl loan_killed_at(l: int32, p: int32)\n",
    ".decl loan_invalidated_at(l: int32, p: int32)\n",
    ".decl universal_region(o: int32)\n",
    ".decl known_placeholder_subset(o1: int32, o2: int32)\n",
    ".decl var_used_at(v: int32, p: int32)\n",
    ".decl var_defined_at(v: int32, p: int32)\n",
    ".decl var_dropped_at(v: int32, p: int32)\n",
    ".decl use_of_var_derefs_origin(v: int32, o: int32)\n",
    ".decl drop_of_var_derefs_origin(v: int32, o: int32)\n",
    ".decl child_path(c: int32, p: int32)\n",
    ".decl path_is_var(p: int32, v: int32)\n",
    ".decl path_assigned_at_base(p: int32, pt: int32)\n",
    ".decl path_moved_at_base(p: int32, pt: int32)\n",
    ".decl path_accessed_at_base(p: int32, pt: int32)\n",
};

/* IDB declarations + 37 rules */
static const char *polonius_rules
    = ".decl ancestor_path(a: int32, d: int32)\n"
      ".decl path_moved_at(p: int32, pt: int32)\n"
      ".decl path_assigned_at(p: int32, pt: int32)\n"
      ".decl path_accessed_at(p: int32, pt: int32)\n"
      ".decl path_begins_with_var(p: int32, v: int32)\n"
      ".decl path_maybe_initialized_on_exit(p: int32, pt: int32)\n"
      ".decl path_maybe_uninitialized_on_exit(p: int32, pt: int32)\n"
      ".decl var_maybe_partly_initialized_on_exit(v: int32, pt: int32)\n"
      ".decl move_error(p: int32, pt: int32)\n"
      ".decl cfg_node(pt: int32)\n"
      ".decl var_live_on_entry(v: int32, pt: int32)\n"
      ".decl var_drop_live_on_entry(v: int32, pt: int32)\n"
      ".decl var_maybe_partly_initialized_on_entry(v: int32, pt: int32)\n"
      ".decl origin_live_on_entry(o: int32, pt: int32)\n"
      ".decl placeholder_origin(o: int32)\n"
      ".decl subset(o1: int32, o2: int32, pt: int32)\n"
      ".decl origin_contains_loan_on_entry(o: int32, l: int32, pt: int32)\n"
      ".decl loan_live_at(l: int32, pt: int32)\n"
      ".decl errors(l: int32, pt: int32)\n"
      ".decl subset_error(o1: int32, o2: int32, pt: int32)\n"
      "ancestor_path(x, y) :- child_path(x, y).\n"
      "ancestor_path(gp, c) :- ancestor_path(p, c), child_path(p, gp).\n"
      "path_moved_at(x, y) :- path_moved_at_base(x, y).\n"
      "path_moved_at(c, p) :- path_moved_at(pa, p), ancestor_path(pa, c).\n"
      "path_assigned_at(x, y) :- path_assigned_at_base(x, y).\n"
      "path_assigned_at(c, p) :- path_assigned_at(pa, p), "
      "ancestor_path(pa, c).\n"
      "path_accessed_at(x, y) :- path_accessed_at_base(x, y).\n"
      "path_accessed_at(c, p) :- path_accessed_at(pa, p), "
      "ancestor_path(pa, c).\n"
      "path_begins_with_var(x, v) :- path_is_var(x, v).\n"
      "path_begins_with_var(c, v) :- path_begins_with_var(pa, v), "
      "ancestor_path(pa, c).\n"
      "path_maybe_initialized_on_exit(path, point) :- "
      "path_assigned_at(path, point).\n"
      "path_maybe_uninitialized_on_exit(path, point) :- "
      "path_moved_at(path, point).\n"
      "path_maybe_initialized_on_exit(path, p2) :- "
      "path_maybe_initialized_on_exit(path, p1), cfg_edge(p1, p2), "
      "!path_moved_at(path, p2).\n"
      "path_maybe_uninitialized_on_exit(path, p2) :- "
      "path_maybe_uninitialized_on_exit(path, p1), cfg_edge(p1, p2), "
      "!path_assigned_at(path, p2).\n"
      "var_maybe_partly_initialized_on_exit(v, p) :- "
      "path_maybe_initialized_on_exit(path, p), "
      "path_begins_with_var(path, v).\n"
      "move_error(path, t) :- "
      "path_maybe_uninitialized_on_exit(path, s), cfg_edge(s, t).\n"
      "cfg_node(p1) :- cfg_edge(p1, _).\n"
      "cfg_node(p2) :- cfg_edge(_, p2).\n"
      "var_live_on_entry(v, p) :- var_used_at(v, p).\n"
      "var_live_on_entry(v, p1) :- var_live_on_entry(v, p2), "
      "cfg_edge(p1, p2), !var_defined_at(v, p1).\n"
      "var_maybe_partly_initialized_on_entry(v, p2) :- "
      "var_maybe_partly_initialized_on_exit(v, p1), cfg_edge(p1, p2).\n"
      "var_drop_live_on_entry(v, p) :- var_dropped_at(v, p), "
      "var_maybe_partly_initialized_on_entry(v, p).\n"
      "var_drop_live_on_entry(v, s) :- var_drop_live_on_entry(v, t), "
      "cfg_edge(s, t), !var_defined_at(v, s), "
      "var_maybe_partly_initialized_on_exit(v, s).\n"
      "origin_live_on_entry(o, p) :- cfg_node(p), universal_region(o).\n"
      "origin_live_on_entry(o, p) :- var_drop_live_on_entry(v, p), "
      "drop_of_var_derefs_origin(v, o).\n"
      "origin_live_on_entry(o, p) :- var_live_on_entry(v, p), "
      "use_of_var_derefs_origin(v, o).\n"
      "placeholder_origin(o) :- universal_region(o).\n"
      "known_placeholder_subset(x, z) :- "
      "known_placeholder_subset(x, y), known_placeholder_subset(y, z).\n"
      "subset(o1, o2, p) :- subset_base(o1, o2, p).\n"
      "subset(o1, o3, p) :- subset(o1, o2, p), "
      "subset_base(o2, o3, p), o1 != o3.\n"
      "subset(o1, o2, p2) :- subset(o1, o2, p1), cfg_edge(p1, p2), "
      "origin_live_on_entry(o1, p2), origin_live_on_entry(o2, p2).\n"
      "origin_contains_loan_on_entry(o, l, p) :- "
      "loan_issued_at(o, l, p).\n"
      "origin_contains_loan_on_entry(o2, l, p) :- "
      "origin_contains_loan_on_entry(o1, l, p), subset(o1, o2, p).\n"
      "origin_contains_loan_on_entry(o, l, p2) :- "
      "origin_contains_loan_on_entry(o, l, p1), cfg_edge(p1, p2), "
      "!loan_killed_at(l, p1), origin_live_on_entry(o, p2).\n"
      "loan_live_at(l, p) :- origin_contains_loan_on_entry(o, l, p), "
      "origin_live_on_entry(o, p).\n"
      "errors(l, p) :- loan_invalidated_at(l, p), loan_live_at(l, p).\n"
      "subset_error(o1, o2, p) :- subset(o1, o2, p), "
      "placeholder_origin(o1), placeholder_origin(o2), "
      "!known_placeholder_subset(o1, o2), o1 != o2.\n";

static int
run_polonius_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 17 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / POLONIUS_NRELS;
    char *bufs[POLONIUS_NRELS];
    int32_t total_facts = 0;

    for (int i = 0; i < POLONIUS_NRELS; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            for (int j = 0; j < i; j++)
                free(bufs[j]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, polonius_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, polonius_rels[i], bufs[i], per_buf,
                                &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build source: decl + facts per EDB, then IDB rules */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < POLONIUS_NRELS; i++)
            free(bufs[i]);
        return -1;
    }

    size_t pos = 0;
    for (int i = 0; i < POLONIUS_NRELS; i++) {
        int n = snprintf(source + pos, SRC_BUFSZ - pos, "%s%s\n",
                         polonius_decls[i], bufs[i]);
        if (n < 0 || pos + (size_t)n >= SRC_BUFSZ) {
            fprintf(stderr, "error: source buffer overflow\n");
            for (int j = 0; j < POLONIUS_NRELS; j++)
                free(bufs[j]);
            free(source);
            return -1;
        }
        pos += (size_t)n;
    }
    for (int i = 0; i < POLONIUS_NRELS; i++)
        free(bufs[i]);

    /* Append IDB declarations and rules */
    int n = snprintf(source + pos, SRC_BUFSZ - pos, "%s", polonius_rules);
    if (n < 0 || pos + (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("polonius\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64
               "\t%" PRId64 "\t%u\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, total_iters, "OK");
    } else {
        printf("polonius\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers,
               repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * DDISASM - Disassembly Analysis (multi-relation workload)
 * ---------------------------------------------------------------- */

#define DDISASM_NRELS 8
static const char *ddisasm_rels[DDISASM_NRELS] = {
    "instruction", "next",        "entry_point", "direct_jump",
    "cond_jump",   "direct_call", "return_inst", "nop",
};
static const char *ddisasm_files[DDISASM_NRELS] = {
    "instruction.csv", "next.csv",        "entry_point.csv", "direct_jump.csv",
    "cond_jump.csv",   "direct_call.csv", "return_inst.csv", "nop.csv",
};
static const char *ddisasm_decls[DDISASM_NRELS] = {
    ".decl instruction(ea: int32, sz: int32)\n",
    ".decl next(ea: int32, next_ea: int32)\n",
    ".decl entry_point(ea: int32)\n",
    ".decl direct_jump(ea: int32, dest: int32)\n",
    ".decl cond_jump(ea: int32, dest: int32)\n",
    ".decl direct_call(ea: int32, dest: int32)\n",
    ".decl return_inst(ea: int32)\n",
    ".decl nop(ea: int32)\n",
};

/* IDB declarations + 28 rules */
static const char *ddisasm_rules
    = ".decl possible_target(ea: int32)\n"
      ".decl code(ea: int32)\n"
      ".decl no_fallthrough(ea: int32)\n"
      ".decl may_fallthrough(ea: int32, next_ea: int32)\n"
      ".decl code_in_block(ea: int32, block: int32)\n"
      ".decl block_head(b: int32)\n"
      ".decl block_has_return(b: int32)\n"
      ".decl block_has_jump(b: int32)\n"
      ".decl block_has_cond_jump(b: int32)\n"
      ".decl block_has_call(b: int32)\n"
      ".decl cfg_edge(src: int32, dest: int32)\n"
      ".decl call_target(dest: int32)\n"
      ".decl reachable(b: int32)\n"
      ".decl dead_block(b: int32)\n"
      ".decl function_entry(b: int32)\n"
      ".decl in_function(b: int32, func: int32)\n"
      /* Phase 1: Code Discovery (mutual recursion) */
      "possible_target(ea) :- entry_point(ea).\n"
      "possible_target(dest) :- direct_jump(_, dest).\n"
      "possible_target(dest) :- cond_jump(_, dest).\n"
      "possible_target(dest) :- direct_call(_, dest).\n"
      "possible_target(next_ea) :- cond_jump(ea, _), next(ea, next_ea).\n"
      "possible_target(next_ea) :- direct_call(ea, _), next(ea, next_ea).\n"
      "code(ea) :- possible_target(ea), instruction(ea, _).\n"
      "code(next_ea) :- may_fallthrough(_, next_ea).\n"
      "no_fallthrough(ea) :- return_inst(ea).\n"
      "no_fallthrough(ea) :- direct_jump(ea, _).\n"
      "may_fallthrough(ea, next_ea) :- code_in_block(ea, _), "
      "next(ea, next_ea), !no_fallthrough(ea).\n"
      "code_in_block(ea, ea) :- possible_target(ea), code(ea).\n"
      "code_in_block(ea, block) :- code(ea), may_fallthrough(prev, ea), "
      "code_in_block(prev, block), !possible_target(ea).\n"
      "block_head(b) :- code_in_block(_, b).\n"
      /* Phase 2: Block Properties */
      "block_has_return(b) :- code_in_block(ea, b), return_inst(ea).\n"
      "block_has_jump(b) :- code_in_block(ea, b), direct_jump(ea, _).\n"
      "block_has_cond_jump(b) :- code_in_block(ea, b), cond_jump(ea, _).\n"
      "block_has_call(b) :- code_in_block(ea, b), direct_call(ea, _).\n"
      /* Phase 3: Control Flow Graph */
      "cfg_edge(src, dest) :- code_in_block(ea, src), "
      "direct_jump(ea, dest), block_head(dest).\n"
      "cfg_edge(src, dest) :- code_in_block(ea, src), "
      "cond_jump(ea, dest), block_head(dest).\n"
      "cfg_edge(src, dest) :- may_fallthrough(ea, dest), "
      "code_in_block(ea, src), block_head(dest).\n"
      "call_target(dest) :- direct_call(_, dest), block_head(dest).\n"
      /* Phase 4: Reachability & Functions */
      "reachable(b) :- entry_point(ea), code_in_block(ea, b).\n"
      "reachable(dest) :- reachable(src), cfg_edge(src, dest).\n"
      "dead_block(b) :- block_head(b), !reachable(b).\n"
      "function_entry(b) :- call_target(b), reachable(b).\n"
      "in_function(func, func) :- function_entry(func).\n"
      "in_function(b, func) :- in_function(src, func), cfg_edge(src, b), "
      "!function_entry(b).\n";

static int
run_ddisasm_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Load 8 CSV files into inline facts buffers */
    size_t per_buf = SRC_BUFSZ / DDISASM_NRELS;
    char *bufs[DDISASM_NRELS];
    int32_t total_facts = 0;

    for (int i = 0; i < DDISASM_NRELS; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            for (int j = 0; j < i; j++)
                free(bufs[j]);
            return -1;
        }

        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", data_dir, ddisasm_files[i]);

        int32_t count = 0;
        if (csv_to_inline_facts(path, ddisasm_rels[i], bufs[i], per_buf, &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
        total_facts += count;
    }

    /* Build source: decl + facts per EDB, then IDB rules */
    char *source = (char *)malloc(SRC_BUFSZ);
    if (!source) {
        for (int i = 0; i < DDISASM_NRELS; i++)
            free(bufs[i]);
        return -1;
    }

    size_t pos = 0;
    for (int i = 0; i < DDISASM_NRELS; i++) {
        int n = snprintf(source + pos, SRC_BUFSZ - pos, "%s%s\n",
                         ddisasm_decls[i], bufs[i]);
        if (n < 0 || pos + (size_t)n >= SRC_BUFSZ) {
            fprintf(stderr, "error: source buffer overflow\n");
            for (int j = 0; j < DDISASM_NRELS; j++)
                free(bufs[j]);
            free(source);
            return -1;
        }
        pos += (size_t)n;
    }
    for (int i = 0; i < DDISASM_NRELS; i++)
        free(bufs[i]);

    /* Append IDB declarations and rules */
    int n = snprintf(source + pos, SRC_BUFSZ - pos, "%s", ddisasm_rules);
    if (n < 0 || pos + (size_t)n >= SRC_BUFSZ) {
        fprintf(stderr, "error: source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("ddisasm\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
               "\t%u\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, total_iters, "OK");
    } else {
        printf("ddisasm\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers,
               repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * CRDT - Conflict-Free Replicated Data Types Verification
 * ---------------------------------------------------------------- */

/* CRDT uses .input directives to load 260K rows from CSV files.
 * The template has two %s placeholders for the data directory path. */

#define CRDT_SRC_BUFSZ ((size_t)8 * 1024)

static const char *crdt_template
    = ".decl Insert_input(ctr: int32, node: int32, parent_ctr: int32, "
      "parent_node: int32)\n"
      ".input Insert_input(filename=\"%s/Insert_input.csv\", delimiter=\",\")\n"
      ".decl Remove_input(ctr: int32, node: int32)\n"
      ".input Remove_input(filename=\"%s/Remove_input.csv\", delimiter=\",\")\n"
      ".decl insert(ctr: int32, node: int32, parent_ctr: int32, parent_node: "
      "int32)\n"
      "insert(a, b, c, d) :- Insert_input(a, b, c, d).\n"
      ".decl remove(ctr: int32, node: int32)\n"
      "remove(a, b) :- Remove_input(a, b).\n"
      ".decl assign(id_ctr: int32, id_node: int32, elem_ctr: int32, "
      "elem_node: int32, value: int32)\n"
      "assign(ctr, n, ctr, n, n) :- insert(ctr, n, _, _).\n"
      ".decl hasChild(parent_ctr: int32, parent_node: int32)\n"
      "hasChild(pc, pn) :- insert(_, _, pc, pn).\n"
      ".decl laterChild(parent_ctr: int32, parent_node: int32, child_ctr: "
      "int32, child_node: int32)\n"
      "laterChild(pc, pn, c2, n2) :- insert(c1, n1, pc, pn), insert(c2, n2, "
      "pc, pn), c1 * 10 + n1 > c2 * 10 + n2.\n"
      ".decl firstChild(parent_ctr: int32, parent_node: int32, child_ctr: "
      "int32, child_node: int32)\n"
      "firstChild(pc, pn, cc, cn) :- insert(cc, cn, pc, pn), !laterChild(pc, "
      "pn, cc, cn).\n"
      ".decl sibling(c1: int32, n1: int32, c2: int32, n2: int32)\n"
      "sibling(c1, n1, c2, n2) :- insert(c1, n1, pc, pn), insert(c2, n2, pc, "
      "pn).\n"
      ".decl laterSibling(c1: int32, n1: int32, c2: int32, n2: int32)\n"
      "laterSibling(c1, n1, c2, n2) :- sibling(c1, n1, c2, n2), c1 * 10 + n1 "
      "> c2 * 10 + n2.\n"
      ".decl laterSibling2(c1: int32, n1: int32, c3: int32, n3: int32)\n"
      "laterSibling2(c1, n1, c3, n3) :- sibling(c1, n1, c2, n2), sibling(c1, "
      "n1, c3, n3), c1 * 10 + n1 > c2 * 10 + n2, c2 * 10 + n2 > c3 * 10 + "
      "n3.\n"
      ".decl nextSibling(c1: int32, n1: int32, c2: int32, n2: int32)\n"
      "nextSibling(c1, n1, c2, n2) :- laterSibling(c1, n1, c2, n2), "
      "!laterSibling2(c1, n1, c2, n2).\n"
      ".decl hasNextSibling(c: int32, n: int32)\n"
      "hasNextSibling(c, n) :- laterSibling(c, n, _, _).\n"
      ".decl nextSiblingAnc(start_ctr: int32, start_node: int32, next_ctr: "
      "int32, next_node: int32)\n"
      "nextSiblingAnc(sc, sn, nc, nn) :- nextSibling(sc, sn, nc, nn).\n"
      "nextSiblingAnc(sc, sn, nc, nn) :- !hasNextSibling(sc, sn), insert(sc, "
      "sn, pc, pn), nextSiblingAnc(pc, pn, nc, nn).\n"
      ".decl nextElem(prev_ctr: int32, prev_node: int32, next_ctr: int32, "
      "next_node: int32)\n"
      "nextElem(pc, pn, nc, nn) :- firstChild(pc, pn, nc, nn).\n"
      "nextElem(pc, pn, nc, nn) :- !hasChild(pc, pn), nextSiblingAnc(pc, pn, "
      "nc, nn).\n"
      ".decl currentValue(elem_ctr: int32, elem_node: int32, value: int32)\n"
      "currentValue(ec, en, v) :- assign(ic, in, ec, en, v), !remove(ic, "
      "in).\n"
      ".decl hasValue(elem_ctr: int32, elem_node: int32)\n"
      "hasValue(ec, en) :- currentValue(ec, en, _).\n"
      ".decl valueStep(from_ctr: int32, from_node: int32, to_ctr: int32, "
      "to_node: int32)\n"
      "valueStep(fc, fn, tc, tn) :- hasValue(fc, fn), nextElem(fc, fn, tc, "
      "tn).\n"
      ".decl blankStep(from_ctr: int32, from_node: int32, to_ctr: int32, "
      "to_node: int32)\n"
      "blankStep(fc, fn, tc, tn) :- !valueStep(fc, fn, tc, tn), nextElem(fc, "
      "fn, tc, tn).\n"
      ".decl value_blank_star(from_ctr: int32, from_node: int32, to_ctr: "
      "int32, to_node: int32)\n"
      "value_blank_star(fc, fn, tc, tn) :- valueStep(fc, fn, tc, tn).\n"
      "value_blank_star(fc, fn, tc, tn) :- value_blank_star(fc, fn, vc, vn), "
      "blankStep(vc, vn, tc, tn).\n"
      ".decl nextVisible(prev_ctr: int32, prev_node: int32, next_ctr: int32, "
      "next_node: int32)\n"
      "nextVisible(pc, pn, nc, nn) :- value_blank_star(pc, pn, nc, nn), "
      "hasValue(nc, nn).\n"
      ".decl result(ctr1: int32, ctr2: int32, value: int32)\n"
      "result(c1, c2, v) :- nextVisible(c1, _, c2, n2), currentValue(c2, n2, "
      "v).\n";

static int
run_crdt_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Build source with .input paths pointing to data directory */
    char *source = (char *)malloc(CRDT_SRC_BUFSZ);
    if (!source)
        return -1;

    int n = snprintf(source, CRDT_SRC_BUFSZ, crdt_template, data_dir, data_dir);
    if (n < 0 || (size_t)n >= CRDT_SRC_BUFSZ) {
        fprintf(stderr, "error: CRDT source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Count total input facts */
    int32_t total_facts = 0;
    {
        char path[1024];
        char line[256];
        FILE *f;

        snprintf(path, sizeof(path), "%s/Insert_input.csv", data_dir);
        f = fopen(path, "r");
        if (f) {
            while (fgets(line, sizeof(line), f))
                total_facts++;
            fclose(f);
        }

        snprintf(path, sizeof(path), "%s/Remove_input.csv", data_dir);
        f = fopen(path, "r");
        if (f) {
            while (fgets(line, sizeof(line), f))
                total_facts++;
            fclose(f);
        }
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("crdt\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
               "\t%u\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, total_iters, "OK");
    } else {
        printf("crdt\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers, repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * DOOP - Java Points-to Analysis (zxing dataset)
 * ---------------------------------------------------------------- */

/* DOOP uses .input directives to load 34 CSV files (~3.5M tuples).
 * EDB definitions are generated programmatically; rules are a static string.
 * Dataset: zxing (smallest FlowLog DOOP artifact, 83MB).
 * Magic constants are zxing-specific pre-hashed integer IDs. */

#define DOOP_SRC_BUFSZ ((size_t)64 * 1024)
#define DOOP_NRELS 34

struct doop_edb {
    const char *name;
    const char *cols;
    const char *csv;
};

static const struct doop_edb doop_edbs[DOOP_NRELS] = {
    { "DirectSuperclass", "(class: int32, superclass: int32)",
      "DirectSuperclass.csv" },
    { "DirectSuperinterface", "(ref: int32, interface: int32)",
      "DirectSuperinterface.csv" },
    { "MainClass", "(class: int32)", "MainClass.csv" },
    { "FormalParam", "(index: int32, method: int32, var: int32)",
      "FormalParam.csv" },
    { "ComponentType", "(arrayType: int32, componentType: int32)",
      "ComponentType.csv" },
    { "AssignReturnValue", "(invocation: int32, to: int32)",
      "AssignReturnValue.csv" },
    { "ActualParam", "(index: int32, invocation: int32, var: int32)",
      "ActualParam.csv" },
    { "Method_Modifier", "(mod: int32, method: int32)", "Method_Modifier.csv" },
    { "Var_Type", "(var: int32, type: int32)", "Var_Type.csv" },
    { "HeapAllocation_Type", "(heap: int32, type: int32)",
      "HeapAllocation_Type.csv" },
    { "_ClassType", "(class: int32)", "ClassType.csv" },
    { "_ArrayType", "(arrayType: int32)", "ArrayType.csv" },
    { "_InterfaceType", "(interface: int32)", "InterfaceType.csv" },
    { "_Var_DeclaringMethod", "(var: int32, method: int32)",
      "Var_DeclaringMethod.csv" },
    { "_ApplicationClass", "(type: int32)", "ApplicationClass.csv" },
    { "_ThisVar", "(method: int32, var: int32)", "ThisVar.csv" },
    { "_NormalHeap", "(id: int32, type: int32)", "NormalHeap.csv" },
    { "_StringConstant", "(id: int32)", "StringConstant.csv" },
    { "_AssignHeapAllocation",
      "(instruction: int32, idx: int32, heap: int32, to: int32, "
      "inmethod: int32, linenumber: int32)",
      "AssignHeapAllocation.csv" },
    { "_AssignLocal",
      "(instruction: int32, idx: int32, from: int32, to: int32, "
      "inmethod: int32)",
      "AssignLocal.csv" },
    { "_AssignCast",
      "(instruction: int32, idx: int32, from: int32, to: int32, "
      "type: int32, inmethod: int32)",
      "AssignCast.csv" },
    { "_Field",
      "(signature: int32, declaringClass: int32, simplename: int32, "
      "type: int32)",
      "Field.csv" },
    { "_StaticMethodInvocation",
      "(instruction: int32, idx: int32, signature: int32, method: int32)",
      "StaticMethodInvocation.csv" },
    { "_SpecialMethodInvocation",
      "(instruction: int32, idx: int32, signature: int32, base: int32, "
      "method: int32)",
      "SpecialMethodInvocation.csv" },
    { "_VirtualMethodInvocation",
      "(instruction: int32, idx: int32, signature: int32, base: int32, "
      "method: int32)",
      "VirtualMethodInvocation.csv" },
    { "_Method",
      "(method: int32, simplename: int32, params: int32, "
      "declaringType: int32, returnType: int32, jvmDescriptor: int32, "
      "arity: int32)",
      "Method.csv" },
    { "Method_Descriptor", "(method: int32, descriptor: int32)",
      "Method_Descriptor.csv" },
    { "_StoreInstanceField",
      "(instruction: int32, idx: int32, from: int32, base: int32, "
      "signature: int32, method: int32)",
      "StoreInstanceField.csv" },
    { "_LoadInstanceField",
      "(instruction: int32, idx: int32, to: int32, base: int32, "
      "signature: int32, method: int32)",
      "LoadInstanceField.csv" },
    { "_StoreStaticField",
      "(instruction: int32, idx: int32, from: int32, signature: int32, "
      "method: int32)",
      "StoreStaticField.csv" },
    { "_LoadStaticField",
      "(instruction: int32, idx: int32, to: int32, signature: int32, "
      "method: int32)",
      "LoadStaticField.csv" },
    { "_StoreArrayIndex",
      "(instruction: int32, idx: int32, from: int32, base: int32, "
      "method: int32)",
      "StoreArrayIndex.csv" },
    { "_LoadArrayIndex",
      "(instruction: int32, idx: int32, to: int32, base: int32, "
      "method: int32)",
      "LoadArrayIndex.csv" },
    { "_Return", "(instruction: int32, idx: int32, var: int32, method: int32)",
      "Return.csv" },
};

/* IDB declarations + all ~130 rules */
static const char *doop_rules
    /* IDB: Narrow schema */
    = ".decl isType(t: int32)\n"
      ".decl isReferenceType(t: int32)\n"
      ".decl isArrayType(t: int32)\n"
      ".decl isClassType(t: int32)\n"
      ".decl isInterfaceType(t: int32)\n"
      ".decl ApplicationClass(ref: int32)\n"
      ".decl Field_DeclaringType(field: int32, declaringClass: int32)\n"
      ".decl Method_DeclaringType(method: int32, declaringType: int32)\n"
      ".decl Method_SimpleName(method: int32, simpleName: int32)\n"
      ".decl ThisVar(method: int32, var: int32)\n"
      ".decl Var_DeclaringMethod(var: int32, method: int32)\n"
      ".decl Instruction_Method(insn: int32, inMethod: int32)\n"
      ".decl isVirtualMethodInvocation_Insn(insn: int32)\n"
      ".decl isStaticMethodInvocation_Insn(insn: int32)\n"
      ".decl FieldInstruction_Signature(insn: int32, sign: int32)\n"
      ".decl LoadInstanceField_Base(insn: int32, var: int32)\n"
      ".decl LoadInstanceField_To(insn: int32, var: int32)\n"
      ".decl StoreInstanceField_From(insn: int32, var: int32)\n"
      ".decl StoreInstanceField_Base(insn: int32, var: int32)\n"
      ".decl LoadStaticField_To(insn: int32, var: int32)\n"
      ".decl StoreStaticField_From(insn: int32, var: int32)\n"
      ".decl LoadArrayIndex_Base(insn: int32, var: int32)\n"
      ".decl LoadArrayIndex_To(insn: int32, var: int32)\n"
      ".decl StoreArrayIndex_From(insn: int32, var: int32)\n"
      ".decl StoreArrayIndex_Base(insn: int32, var: int32)\n"
      ".decl AssignInstruction_To(insn: int32, to: int32)\n"
      ".decl AssignCast_From(insn: int32, from: int32)\n"
      ".decl AssignCast_Type(insn: int32, type: int32)\n"
      ".decl AssignLocal_From(insn: int32, from: int32)\n"
      ".decl AssignHeapAllocation_Heap(insn: int32, heap: int32)\n"
      ".decl ReturnNonvoid_Var(ret: int32, var: int32)\n"
      ".decl MethodInvocation_Method(invocation: int32, signature: int32)\n"
      ".decl VirtualMethodInvocation_Base(invocation: int32, base: int32)\n"
      ".decl VirtualMethodInvocation_SimpleName(invocation: int32, "
      "simplename: int32)\n"
      ".decl VirtualMethodInvocation_Descriptor(invocation: int32, "
      "descriptor: int32)\n"
      ".decl SpecialMethodInvocation_Base(invocation: int32, base: int32)\n"
      ".decl MethodInvocation_Base(invocation: int32, base: int32)\n"
      /* IDB: Fat schema */
      ".decl LoadInstanceField(base: int32, sig: int32, to: int32, "
      "inmethod: int32)\n"
      ".decl StoreInstanceField(from: int32, base: int32, signature: int32, "
      "inmethod: int32)\n"
      ".decl LoadStaticField(sig: int32, to: int32, inmethod: int32)\n"
      ".decl StoreStaticField(from: int32, signature: int32, "
      "inmethod: int32)\n"
      ".decl LoadArrayIndex(base: int32, to: int32, inmethod: int32)\n"
      ".decl StoreArrayIndex(from: int32, base: int32, inmethod: int32)\n"
      ".decl AssignCast(type: int32, from: int32, to: int32, "
      "inmethod: int32)\n"
      ".decl AssignLocal(from: int32, to: int32, inmethod: int32)\n"
      ".decl AssignHeapAllocation(heap: int32, to: int32, "
      "inmethod: int32)\n"
      ".decl ReturnVar(var: int32, method: int32)\n"
      ".decl StaticMethodInvocation(invocation: int32, signature: int32, "
      "inmethod: int32)\n"
      /* IDB: Type hierarchy */
      ".decl MethodLookup(simplename: int32, descriptor: int32, "
      "type: int32, method: int32)\n"
      ".decl MethodImplemented(simplename: int32, descriptor: int32, "
      "type: int32, method: int32)\n"
      ".decl DirectSubclass(a: int32, c: int32)\n"
      ".decl Subclass(c: int32, a: int32)\n"
      ".decl Superclass(c: int32, a: int32)\n"
      ".decl Superinterface(k: int32, c: int32)\n"
      ".decl SubtypeOf(subtype: int32, type: int32)\n"
      ".decl SupertypeOf(supertype: int32, type: int32)\n"
      ".decl SubtypeOfDifferent(subtype: int32, type: int32)\n"
      ".decl MainMethodDeclaration(method: int32)\n"
      /* IDB: Class initialization */
      ".decl ClassInitializer(type: int32, method: int32)\n"
      ".decl InitializedClass(classOrInterface: int32)\n"
      /* IDB: Main analysis */
      ".decl Assign(to: int32, from: int32)\n"
      ".decl VarPointsTo(heap: int32, var: int32)\n"
      ".decl InstanceFieldPointsTo(heap: int32, fld: int32, "
      "baseheap: int32)\n"
      ".decl StaticFieldPointsTo(heap: int32, fld: int32)\n"
      ".decl CallGraphEdge(invocation: int32, meth: int32)\n"
      ".decl ArrayIndexPointsTo(baseheap: int32, heap: int32)\n"
      ".decl Reachable(method: int32)\n"
      /* Phase 1: EDB staging & decomposition */
      "isType(class) :- _ClassType(class).\n"
      "isReferenceType(class) :- _ClassType(class).\n"
      "isClassType(class) :- _ClassType(class).\n"
      "isType(at) :- _ArrayType(at).\n"
      "isReferenceType(at) :- _ArrayType(at).\n"
      "isArrayType(at) :- _ArrayType(at).\n"
      "isType(intf) :- _InterfaceType(intf).\n"
      "isReferenceType(intf) :- _InterfaceType(intf).\n"
      "isInterfaceType(intf) :- _InterfaceType(intf).\n"
      "Var_DeclaringMethod(var, method) :- "
      "_Var_DeclaringMethod(var, method).\n"
      "isType(type) :- _ApplicationClass(type).\n"
      "isReferenceType(type) :- _ApplicationClass(type).\n"
      "ApplicationClass(type) :- _ApplicationClass(type).\n"
      "ThisVar(method, var) :- _ThisVar(method, var).\n"
      "isType(type) :- _NormalHeap(_, type).\n"
      /* Decompose _AssignHeapAllocation */
      "Instruction_Method(i, m) :- _AssignHeapAllocation(i, _, _, _, m, _).\n"
      "AssignInstruction_To(i, t) :- _AssignHeapAllocation(i, _, _, t, _, _).\n"
      "AssignHeapAllocation_Heap(i, h) :- "
      "_AssignHeapAllocation(i, _, h, _, _, _).\n"
      /* Decompose _AssignLocal */
      "Instruction_Method(i, m) :- _AssignLocal(i, _, _, _, m).\n"
      "AssignLocal_From(i, f) :- _AssignLocal(i, _, f, _, _).\n"
      "AssignInstruction_To(i, t) :- _AssignLocal(i, _, _, t, _).\n"
      /* Decompose _AssignCast */
      "Instruction_Method(i, m) :- _AssignCast(i, _, _, _, _, m).\n"
      "AssignCast_Type(i, tp) :- _AssignCast(i, _, _, _, tp, _).\n"
      "AssignCast_From(i, f) :- _AssignCast(i, _, f, _, _, _).\n"
      "AssignInstruction_To(i, t) :- _AssignCast(i, _, _, t, _, _).\n"
      /* Decompose _Field */
      "Field_DeclaringType(sig, dt) :- _Field(sig, dt, _, _).\n"
      /* MethodInvocation_Base */
      "MethodInvocation_Base(inv, b) :- "
      "VirtualMethodInvocation_Base(inv, b).\n"
      "MethodInvocation_Base(inv, b) :- "
      "SpecialMethodInvocation_Base(inv, b).\n"
      /* Decompose _StaticMethodInvocation */
      "Instruction_Method(i, m) :- _StaticMethodInvocation(i, _, _, m).\n"
      "isStaticMethodInvocation_Insn(i) :- "
      "_StaticMethodInvocation(i, _, _, _).\n"
      "MethodInvocation_Method(i, sig) :- "
      "_StaticMethodInvocation(i, _, sig, _).\n"
      /* Decompose _SpecialMethodInvocation */
      "Instruction_Method(i, m) :- "
      "_SpecialMethodInvocation(i, _, _, _, m).\n"
      "SpecialMethodInvocation_Base(i, b) :- "
      "_SpecialMethodInvocation(i, _, _, b, _).\n"
      "MethodInvocation_Method(i, sig) :- "
      "_SpecialMethodInvocation(i, _, sig, _, _).\n"
      /* Decompose _VirtualMethodInvocation */
      "Instruction_Method(i, m) :- "
      "_VirtualMethodInvocation(i, _, _, _, m).\n"
      "isVirtualMethodInvocation_Insn(i) :- "
      "_VirtualMethodInvocation(i, _, _, _, _).\n"
      "VirtualMethodInvocation_Base(i, b) :- "
      "_VirtualMethodInvocation(i, _, _, b, _).\n"
      "MethodInvocation_Method(i, sig) :- "
      "_VirtualMethodInvocation(i, _, sig, _, _).\n"
      /* Decompose _Method */
      "Method_SimpleName(m, sn) :- _Method(m, sn, _, _, _, _, _).\n"
      "Method_DeclaringType(m, dt) :- _Method(m, _, _, dt, _, _, _).\n"
      /* Decompose _StoreInstanceField */
      "Instruction_Method(i, m) :- "
      "_StoreInstanceField(i, _, _, _, _, m).\n"
      "FieldInstruction_Signature(i, sig) :- "
      "_StoreInstanceField(i, _, _, _, sig, _).\n"
      "StoreInstanceField_Base(i, b) :- "
      "_StoreInstanceField(i, _, _, b, _, _).\n"
      "StoreInstanceField_From(i, f) :- "
      "_StoreInstanceField(i, _, f, _, _, _).\n"
      /* Decompose _LoadInstanceField */
      "Instruction_Method(i, m) :- "
      "_LoadInstanceField(i, _, _, _, _, m).\n"
      "FieldInstruction_Signature(i, sig) :- "
      "_LoadInstanceField(i, _, _, _, sig, _).\n"
      "LoadInstanceField_Base(i, b) :- "
      "_LoadInstanceField(i, _, _, b, _, _).\n"
      "LoadInstanceField_To(i, t) :- "
      "_LoadInstanceField(i, _, t, _, _, _).\n"
      /* Decompose _StoreStaticField */
      "Instruction_Method(i, m) :- _StoreStaticField(i, _, _, _, m).\n"
      "FieldInstruction_Signature(i, sig) :- "
      "_StoreStaticField(i, _, _, sig, _).\n"
      "StoreStaticField_From(i, f) :- _StoreStaticField(i, _, f, _, _).\n"
      /* Decompose _LoadStaticField */
      "Instruction_Method(i, m) :- _LoadStaticField(i, _, _, _, m).\n"
      "FieldInstruction_Signature(i, sig) :- "
      "_LoadStaticField(i, _, _, sig, _).\n"
      "LoadStaticField_To(i, t) :- _LoadStaticField(i, _, t, _, _).\n"
      /* Decompose _StoreArrayIndex */
      "Instruction_Method(i, m) :- _StoreArrayIndex(i, _, _, _, m).\n"
      "StoreArrayIndex_Base(i, b) :- _StoreArrayIndex(i, _, _, b, _).\n"
      "StoreArrayIndex_From(i, f) :- _StoreArrayIndex(i, _, f, _, _).\n"
      /* Decompose _LoadArrayIndex */
      "Instruction_Method(i, m) :- _LoadArrayIndex(i, _, _, _, m).\n"
      "LoadArrayIndex_Base(i, b) :- _LoadArrayIndex(i, _, _, b, _).\n"
      "LoadArrayIndex_To(i, t) :- _LoadArrayIndex(i, _, t, _, _).\n"
      /* Decompose _Return */
      "Instruction_Method(i, m) :- _Return(i, _, _, m).\n"
      "ReturnNonvoid_Var(i, v) :- _Return(i, _, v, _).\n"
      /* Fat schema population */
      "LoadInstanceField(base, sig, to, im) :- "
      "Instruction_Method(insn, im), LoadInstanceField_Base(insn, base), "
      "FieldInstruction_Signature(insn, sig), "
      "LoadInstanceField_To(insn, to).\n"
      "StoreInstanceField(from, base, sig, im) :- "
      "Instruction_Method(insn, im), StoreInstanceField_From(insn, from), "
      "StoreInstanceField_Base(insn, base), "
      "FieldInstruction_Signature(insn, sig).\n"
      "LoadStaticField(sig, to, im) :- Instruction_Method(insn, im), "
      "FieldInstruction_Signature(insn, sig), "
      "LoadStaticField_To(insn, to).\n"
      "StoreStaticField(from, sig, im) :- Instruction_Method(insn, im), "
      "StoreStaticField_From(insn, from), "
      "FieldInstruction_Signature(insn, sig).\n"
      "LoadArrayIndex(base, to, im) :- Instruction_Method(insn, im), "
      "LoadArrayIndex_Base(insn, base), LoadArrayIndex_To(insn, to).\n"
      "StoreArrayIndex(from, base, im) :- Instruction_Method(insn, im), "
      "StoreArrayIndex_From(insn, from), "
      "StoreArrayIndex_Base(insn, base).\n"
      "AssignCast(type, from, to, im) :- Instruction_Method(insn, im), "
      "AssignCast_From(insn, from), AssignInstruction_To(insn, to), "
      "AssignCast_Type(insn, type).\n"
      "AssignLocal(from, to, im) :- AssignInstruction_To(insn, to), "
      "Instruction_Method(insn, im), AssignLocal_From(insn, from).\n"
      "AssignHeapAllocation(heap, to, im) :- "
      "Instruction_Method(insn, im), "
      "AssignHeapAllocation_Heap(insn, heap), "
      "AssignInstruction_To(insn, to).\n"
      "ReturnVar(var, method) :- Instruction_Method(insn, method), "
      "ReturnNonvoid_Var(insn, var).\n"
      "StaticMethodInvocation(inv, sig, im) :- "
      "isStaticMethodInvocation_Insn(inv), Instruction_Method(inv, im), "
      "MethodInvocation_Method(inv, sig).\n"
      /* VirtualMethodInvocation derived */
      "VirtualMethodInvocation_SimpleName(inv, sn) :- "
      "isVirtualMethodInvocation_Insn(inv), "
      "MethodInvocation_Method(inv, sig), "
      "Method_SimpleName(sig, sn), Method_Descriptor(sig, desc).\n"
      "VirtualMethodInvocation_Descriptor(inv, desc) :- "
      "isVirtualMethodInvocation_Insn(inv), "
      "MethodInvocation_Method(inv, sig), "
      "Method_SimpleName(sig, sn), Method_Descriptor(sig, desc).\n"
      /* Phase 2: Type hierarchy */
      "MethodLookup(sn, d, t, m) :- MethodImplemented(sn, d, t, m).\n"
      "MethodLookup(sn, d, t, m) :- DirectSuperclass(t, st), "
      "MethodLookup(sn, d, st, m), !MethodImplemented(sn, d, t, _).\n"
      "MethodLookup(sn, d, t, m) :- DirectSuperinterface(t, st), "
      "MethodLookup(sn, d, st, m), !MethodImplemented(sn, d, t, _).\n"
      "MethodImplemented(sn, d, t, m) :- Method_SimpleName(m, sn), "
      "Method_Descriptor(m, d), Method_DeclaringType(m, t), "
      "!Method_Modifier(1928492, m).\n"
      "MainMethodDeclaration(m) :- MainClass(t), "
      "Method_DeclaringType(m, t), m != 536048, m != 1057660, "
      "m != 796639, Method_SimpleName(m, 2648290), "
      "Method_Descriptor(m, 2671384), Method_Modifier(760051, m), "
      "Method_Modifier(841804, m).\n"
      "DirectSubclass(a, c) :- DirectSuperclass(a, c).\n"
      "Subclass(c, a) :- DirectSubclass(a, c).\n"
      "Subclass(c, a) :- Subclass(b, a), DirectSubclass(b, c).\n"
      "Superclass(c, a) :- Subclass(a, c).\n"
      "Superinterface(k, c) :- DirectSuperinterface(c, k).\n"
      "Superinterface(k, c) :- DirectSuperinterface(c, j), "
      "Superinterface(k, j).\n"
      "Superinterface(k, c) :- DirectSuperclass(c, s), "
      "Superinterface(k, s).\n"
      "SubtypeOf(s, s) :- isClassType(s).\n"
      "SubtypeOf(t, t) :- isType(t).\n"
      "SubtypeOf(s, t) :- Subclass(t, s).\n"
      "SubtypeOf(s, s) :- isInterfaceType(s).\n"
      "SubtypeOf(s, t) :- isClassType(s), Superinterface(t, s).\n"
      "SubtypeOf(s, t) :- isInterfaceType(s), isType(t), t = 613907.\n"
      "SubtypeOf(s, t) :- isArrayType(s), isType(t), t = 613907.\n"
      "SubtypeOf(s, t) :- isInterfaceType(s), Superinterface(t, s).\n"
      "SubtypeOf(s, t) :- SubtypeOf(sc, tc), ComponentType(s, sc), "
      "ComponentType(t, tc), isReferenceType(sc), "
      "isReferenceType(tc).\n"
      "SubtypeOf(s, t) :- isArrayType(s), isInterfaceType(t), "
      "isType(t), t = 935673.\n"
      "SubtypeOf(s, t) :- isArrayType(s), isInterfaceType(t), "
      "isType(t), t = 619327.\n"
      "SupertypeOf(s, t) :- SubtypeOf(t, s).\n"
      "SubtypeOfDifferent(s, t) :- SubtypeOf(s, t), s != t.\n"
      /* Phase 3: Class initialization */
      "ClassInitializer(t, m) :- MethodImplemented(777634, 2670449, t, m).\n"
      "InitializedClass(sc) :- InitializedClass(c), "
      "DirectSuperclass(c, sc).\n"
      "InitializedClass(si) :- InitializedClass(ci), "
      "DirectSuperinterface(ci, si).\n"
      "InitializedClass(c) :- MainMethodDeclaration(m), "
      "Method_DeclaringType(m, c).\n"
      "InitializedClass(c) :- Reachable(im), "
      "AssignHeapAllocation(h, _, im), HeapAllocation_Type(h, c).\n"
      "InitializedClass(c) :- Reachable(im), "
      "Instruction_Method(inv, im), "
      "isStaticMethodInvocation_Insn(inv), "
      "MethodInvocation_Method(inv, sig), "
      "Method_DeclaringType(sig, c).\n"
      "InitializedClass(ci) :- Reachable(im), "
      "StoreStaticField(_, sig, im), "
      "Field_DeclaringType(sig, ci).\n"
      "InitializedClass(ci) :- Reachable(im), "
      "LoadStaticField(sig, _, im), Field_DeclaringType(sig, ci).\n"
      "Reachable(cl) :- InitializedClass(c), "
      "ClassInitializer(c, cl).\n"
      /* Phase 4: Main analysis */
      "Assign(act, frm) :- CallGraphEdge(inv, m), "
      "FormalParam(idx, m, frm), ActualParam(idx, inv, act).\n"
      "Assign(ret, loc) :- CallGraphEdge(inv, m), ReturnVar(ret, m), "
      "AssignReturnValue(inv, loc).\n"
      "VarPointsTo(h, v) :- AssignHeapAllocation(h, v, im), "
      "Reachable(im).\n"
      "VarPointsTo(h, to) :- Assign(from, to), VarPointsTo(h, from).\n"
      "VarPointsTo(h, to) :- Reachable(im), "
      "AssignLocal(from, to, im), VarPointsTo(h, from).\n"
      "VarPointsTo(h, to) :- Reachable(im), "
      "AssignCast(tp, from, to, im), SupertypeOf(tp, ht), "
      "HeapAllocation_Type(h, ht), VarPointsTo(h, from).\n"
      "ArrayIndexPointsTo(bh, h) :- Reachable(im), "
      "StoreArrayIndex(from, base, im), VarPointsTo(bh, base), "
      "VarPointsTo(h, from), HeapAllocation_Type(h, ht), "
      "HeapAllocation_Type(bh, bht), ComponentType(bht, ct), "
      "SupertypeOf(ct, ht).\n"
      "VarPointsTo(h, to) :- Reachable(im), "
      "LoadArrayIndex(base, to, im), VarPointsTo(bh, base), "
      "ArrayIndexPointsTo(bh, h), Var_Type(to, tp), "
      "HeapAllocation_Type(bh, bht), ComponentType(bht, bct), "
      "SupertypeOf(tp, bct).\n"
      "VarPointsTo(h, to) :- Reachable(im), "
      "LoadInstanceField(base, sig, to, im), "
      "VarPointsTo(bh, base), "
      "InstanceFieldPointsTo(h, sig, bh).\n"
      "VarPointsTo(h, to) :- Reachable(im), "
      "LoadStaticField(fld, to, im), "
      "StaticFieldPointsTo(h, fld).\n"
      "VarPointsTo(h, this) :- Reachable(im), "
      "Instruction_Method(inv, im), "
      "VirtualMethodInvocation_Base(inv, base), "
      "VarPointsTo(h, base), HeapAllocation_Type(h, ht), "
      "VirtualMethodInvocation_SimpleName(inv, sn), "
      "VirtualMethodInvocation_Descriptor(inv, desc), "
      "MethodLookup(sn, desc, ht, tm), ThisVar(tm, this).\n"
      "InstanceFieldPointsTo(h, fld, bh) :- Reachable(im), "
      "StoreInstanceField(from, base, fld, im), "
      "VarPointsTo(h, from), VarPointsTo(bh, base).\n"
      "StaticFieldPointsTo(h, fld) :- Reachable(im), "
      "StoreStaticField(from, fld, im), VarPointsTo(h, from).\n"
      "Reachable(tm) :- Reachable(im), Instruction_Method(inv, im), "
      "VirtualMethodInvocation_Base(inv, base), "
      "VarPointsTo(h, base), HeapAllocation_Type(h, ht), "
      "VirtualMethodInvocation_SimpleName(inv, sn), "
      "VirtualMethodInvocation_Descriptor(inv, desc), "
      "MethodLookup(sn, desc, ht, tm).\n"
      "CallGraphEdge(inv, tm) :- Reachable(im), "
      "Instruction_Method(inv, im), "
      "VirtualMethodInvocation_Base(inv, base), "
      "VarPointsTo(h, base), HeapAllocation_Type(h, ht), "
      "VirtualMethodInvocation_SimpleName(inv, sn), "
      "VirtualMethodInvocation_Descriptor(inv, desc), "
      "MethodLookup(sn, desc, ht, tm).\n"
      "Reachable(tm) :- Reachable(im), "
      "StaticMethodInvocation(inv, tm, im).\n"
      "CallGraphEdge(inv, tm) :- Reachable(im), "
      "StaticMethodInvocation(inv, tm, im).\n"
      "Reachable(tm) :- Reachable(im), Instruction_Method(inv, im), "
      "SpecialMethodInvocation_Base(inv, base), "
      "VarPointsTo(h, base), "
      "MethodInvocation_Method(inv, tm), ThisVar(tm, this).\n"
      "CallGraphEdge(inv, tm) :- Reachable(im), "
      "Instruction_Method(inv, im), "
      "SpecialMethodInvocation_Base(inv, base), "
      "VarPointsTo(h, base), "
      "MethodInvocation_Method(inv, tm), ThisVar(tm, this).\n"
      "VarPointsTo(h, this) :- Reachable(im), "
      "Instruction_Method(inv, im), "
      "SpecialMethodInvocation_Base(inv, base), "
      "VarPointsTo(h, base), "
      "MethodInvocation_Method(inv, tm), ThisVar(tm, this).\n"
      "Reachable(m) :- MainMethodDeclaration(m).\n";

static int
run_doop_workload(const char *data_dir, uint32_t workers, int repeat)
{
    /* Build source: generate .decl + .input for each EDB, then rules */
    char *source = (char *)malloc(DOOP_SRC_BUFSZ);
    if (!source)
        return -1;

    size_t pos = 0;
    for (int i = 0; i < DOOP_NRELS; i++) {
        int n = snprintf(source + pos, DOOP_SRC_BUFSZ - pos,
                         ".decl %s%s\n"
                         ".input %s(filename=\"%s/%s\", delimiter=\",\")\n",
                         doop_edbs[i].name, doop_edbs[i].cols,
                         doop_edbs[i].name, data_dir, doop_edbs[i].csv);
        if (n < 0 || pos + (size_t)n >= DOOP_SRC_BUFSZ) {
            fprintf(stderr, "error: DOOP EDB source buffer overflow\n");
            free(source);
            return -1;
        }
        pos += (size_t)n;
    }

    /* Append IDB declarations + rules */
    int n = snprintf(source + pos, DOOP_SRC_BUFSZ - pos, "%s", doop_rules);
    if (n < 0 || pos + (size_t)n >= DOOP_SRC_BUFSZ) {
        fprintf(stderr, "error: DOOP rules source buffer overflow\n");
        free(source);
        return -1;
    }

    /* Count total input facts across all 34 CSV files */
    int32_t total_facts = 0;
    {
        char path[1024];
        char line[256];
        for (int i = 0; i < DOOP_NRELS; i++) {
            snprintf(path, sizeof(path), "%s/%s", data_dir, doop_edbs[i].csv);
            FILE *f = fopen(path, "r");
            if (f) {
                while (fgets(line, sizeof(line), f))
                    total_facts++;
                fclose(f);
            }
        }
    }

    /* Collect timing samples */
    double *times = (double *)malloc(sizeof(double) * (size_t)repeat);
    if (!times) {
        free(source);
        return -1;
    }

    int64_t tuples = 0;
    int64_t peak_rss = -1;
    uint32_t total_iters = 0;
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        uint32_t iters = 0;
        int rc = run_pipeline_count(source, workers, &cnt, &iters);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
        total_iters = iters;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        if (g_format_json)
            output_json_row("doop", total_facts, workers, repeat, min_ms,
                            median_ms, max_ms, peak_rss, tuples, total_iters,
                            g_last_consolidation_ns, g_last_kfusion_ns,
                            g_last_kfusion_alloc_ns, g_last_kfusion_dispatch_ns,
                            g_last_kfusion_merge_ns, g_last_kfusion_cleanup_ns);
        else
            printf("doop\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64
                   "\t%" PRId64 "\t%u\t%s\n",
                   total_facts, workers, repeat, min_ms, median_ms, max_ms,
                   peak_rss, tuples, total_iters, "OK");
    } else {
        printf("doop\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\t-\tFAIL\n", workers, repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

/*
 * output_json_row: emit one benchmark result as a JSON object (3B-003).
 *
 * Percentages are computed relative to median wall time.
 * consolidation_ns and kfusion_ns come from g_last_* (last run only).
 * evaluation_pct = 100 - consolidation_pct - kfusion_pct (residual).
 */
static void
output_json_row(const char *wl_name, int32_t edges, uint32_t workers,
                int repeat, double min_ms, double median_ms, double max_ms,
                int64_t peak_rss_kb, int64_t tuples, uint32_t iters,
                uint64_t consolidation_ns, uint64_t kfusion_ns,
                uint64_t kfusion_alloc_ns, uint64_t kfusion_dispatch_ns,
                uint64_t kfusion_merge_ns, uint64_t kfusion_cleanup_ns)
{
    double wall_ns = median_ms * 1e6;
    double cons_pct
        = wall_ns > 0 ? 100.0 * (double)consolidation_ns / wall_ns : 0.0;
    double kfus_pct = wall_ns > 0 ? 100.0 * (double)kfusion_ns / wall_ns : 0.0;
    double eval_pct = 100.0 - cons_pct - kfus_pct;
    if (eval_pct < 0.0)
        eval_pct = 0.0;

    /* Per-phase breakdown percentages (relative to total kfusion_ns) */
    double kf_ns = (double)kfusion_ns;
    double alloc_pct
        = kf_ns > 0 ? 100.0 * (double)kfusion_alloc_ns / kf_ns : 0.0;
    double dispatch_pct
        = kf_ns > 0 ? 100.0 * (double)kfusion_dispatch_ns / kf_ns : 0.0;
    double merge_pct
        = kf_ns > 0 ? 100.0 * (double)kfusion_merge_ns / kf_ns : 0.0;
    double cleanup_pct
        = kf_ns > 0 ? 100.0 * (double)kfusion_cleanup_ns / kf_ns : 0.0;

    printf("{\n");
    printf("  \"workload\": \"%s\",\n", wl_name);
    if (edges > 0)
        printf("  \"edges\": %d,\n", edges);
    printf("  \"k_fusion\": %s,\n", WITH_K_FUSION ? "true" : "false");
    printf("  \"workers\": %u,\n", workers);
    printf("  \"repeat\": %d,\n", repeat);
    printf("  \"tuples\": %" PRId64 ",\n", tuples);
    printf("  \"iterations\": %u,\n", iters);
    printf("  \"wall_time_ms\": {\"min\": %.1f, \"median\": %.1f, \"max\": "
           "%.1f},\n",
           min_ms, median_ms, max_ms);
    printf("  \"consolidation_ms\": %.3f,\n", (double)consolidation_ns / 1e6);
    printf("  \"kfusion_ms\": %.3f,\n", (double)kfusion_ns / 1e6);
    printf("  \"kfusion_phase_ms\": {\"alloc\": %.4f, \"dispatch\": %.4f, "
           "\"merge\": %.4f, \"cleanup\": %.4f},\n",
           (double)kfusion_alloc_ns / 1e6, (double)kfusion_dispatch_ns / 1e6,
           (double)kfusion_merge_ns / 1e6, (double)kfusion_cleanup_ns / 1e6);
    printf("  \"kfusion_phase_pct\": {\"alloc\": %.1f, \"dispatch\": %.1f, "
           "\"merge\": %.1f, \"cleanup\": %.1f},\n",
           alloc_pct, dispatch_pct, merge_pct, cleanup_pct);
    printf("  \"evaluation_ms\": %.3f,\n", median_ms
                                               - (double)consolidation_ns / 1e6
                                               - (double)kfusion_ns / 1e6);
    printf("  \"consolidation_pct\": %.1f,\n", cons_pct);
    printf("  \"kfusion_pct\": %.1f,\n", kfus_pct);
    printf("  \"evaluation_pct\": %.1f,\n", eval_pct);
    printf("  \"peak_rss_kb\": %" PRId64 ",\n", peak_rss_kb);
    printf("  \"profiling_from_last_run\": true\n");
    printf("}\n");
}

static void
print_header(void)
{
    if (g_format_json)
        return; /* JSON output is self-describing */
    printf("workload\tnodes\tedges\tworkers\trepeat\tmin_ms\tmedian_ms"
           "\tmax_ms\tpeak_rss_kb\ttuples\titerations\tstatus\n");
}

static void
usage(const char *prog)
{
    fprintf(
        stderr,
        "Usage: %s --workload "
        "{tc|reach|cc|sssp|sg|bipartite|andersen|dyck|cspa|csda|"
        "galen|polonius|ddisasm|crdt|doop|all} --data FILE\n"
        "          [--data-weighted FILE] [--data-andersen DIR]\n"
        "          [--data-dyck DIR] [--data-cspa DIR]\n"
        "          [--data-csda DIR] [--data-galen DIR]\n"
        "          [--data-polonius DIR] [--data-ddisasm DIR]\n"
        "          [--workers N] [--repeat R] [--format {tsv|json}]\n"
        "\n"
        "  --data FILE           Unweighted edge CSV (src,dst)\n"
        "  --data-weighted FILE  Weighted edge CSV (src,dst,weight) for SSSP\n"
        "  --data-andersen DIR   Directory with addressOf/assign/load/store "
        "CSVs\n"
        "  --data-dyck DIR       Directory with open1/close1/open2/close2 "
        "CSVs\n"
        "  --data-cspa DIR       Directory with assign/dereference CSVs\n"
        "  --data-csda DIR       Directory with nullEdge/edge CSVs\n"
        "  --data-galen DIR      Directory with Galen ontology CSVs\n"
        "  --data-polonius DIR   Directory with Polonius borrow checker "
        "CSVs\n"
        "  --data-ddisasm DIR    Directory with DDISASM disassembly CSVs\n"
        "  --data-crdt DIR       Directory with CRDT Insert/Remove CSVs\n"
        "  --data-doop DIR       Directory with DOOP zxing CSVs\n",
        prog);
}

int
main(int argc, char **argv)
{
    const char *workload = NULL;
    const char *data_path = NULL;
    const char *data_weighted_path = NULL;
    const char *data_andersen_path = NULL;
    const char *data_dyck_path = NULL;
    const char *data_cspa_path = NULL;
    const char *data_csda_path = NULL;
    const char *data_galen_path = NULL;
    const char *data_polonius_path = NULL;
    const char *data_ddisasm_path = NULL;
    const char *data_crdt_path = NULL;
    const char *data_doop_path = NULL;
    uint32_t workers = 1;
    int repeat = 3;

    static struct option long_opts[] = {
        { "workload", required_argument, NULL, 'w' },
        { "data", required_argument, NULL, 'd' },
        { "data-weighted", required_argument, NULL, 'W' },
        { "data-andersen", required_argument, NULL, 'A' },
        { "data-dyck", required_argument, NULL, 'D' },
        { "data-cspa", required_argument, NULL, 'C' },
        { "data-csda", required_argument, NULL, 'S' },
        { "data-galen", required_argument, NULL, 'G' },
        { "data-polonius", required_argument, NULL, 'P' },
        { "data-ddisasm", required_argument, NULL, 'I' },
        { "data-crdt", required_argument, NULL, 'R' },
        { "data-doop", required_argument, NULL, 'O' },
        { "workers", required_argument, NULL, 'j' },
        { "repeat", required_argument, NULL, 'r' },
        { "format", required_argument, NULL, 'F' },
        { "help", no_argument, NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };

    int opt;
#ifndef _MSC_VER
    while ((opt = getopt_long(argc, argv, "w:d:W:A:D:C:S:G:P:I:R:O:j:r:F:h",
                              long_opts, NULL))
           != -1) {
#else
    /* MSVC: getopt_long not available; use simple getopt fallback */
    while ((opt = getopt(argc, argv, "w:d:W:A:D:C:S:G:P:I:R:O:j:r:F:h"))
           != -1) {
#endif
        switch (opt) {
        case 'w':
            workload = optarg;
            break;
        case 'd':
            data_path = optarg;
            break;
        case 'W':
            data_weighted_path = optarg;
            break;
        case 'A':
            data_andersen_path = optarg;
            break;
        case 'D':
            data_dyck_path = optarg;
            break;
        case 'C':
            data_cspa_path = optarg;
            break;
        case 'S':
            data_csda_path = optarg;
            break;
        case 'G':
            data_galen_path = optarg;
            break;
        case 'P':
            data_polonius_path = optarg;
            break;
        case 'I':
            data_ddisasm_path = optarg;
            break;
        case 'R':
            data_crdt_path = optarg;
            break;
        case 'O':
            data_doop_path = optarg;
            break;
        case 'j':
            workers = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'r':
            repeat = (int)strtol(optarg, NULL, 10);
            break;
        case 'F':
            g_format_json = (strcmp(optarg, "json") == 0);
            break;
        case 'h':
        default:
            usage(argv[0]);
            return (opt == 'h') ? 0 : 1;
        }
    }

    if (!workload
        || (!data_path && !data_andersen_path && !data_dyck_path
            && !data_cspa_path && !data_csda_path && !data_galen_path
            && !data_polonius_path && !data_ddisasm_path && !data_crdt_path
            && !data_doop_path)) {
        usage(argv[0]);
        return 1;
    }

    if (repeat < 1)
        repeat = 1;

    print_header();

    int rc = 0;

    if (strcmp(workload, "tc") == 0) {
        rc = run_workload(WL_TC, data_path, workers, repeat);
    } else if (strcmp(workload, "reach") == 0) {
        rc = run_workload(WL_REACH, data_path, workers, repeat);
    } else if (strcmp(workload, "cc") == 0) {
        rc = run_workload(WL_CC, data_path, workers, repeat);
    } else if (strcmp(workload, "sssp") == 0) {
        if (!data_weighted_path) {
            fprintf(stderr, "error: sssp requires --data-weighted FILE\n");
            return 1;
        }
        rc = run_workload(WL_SSSP, data_weighted_path, workers, repeat);
    } else if (strcmp(workload, "sg") == 0) {
        rc = run_workload(WL_SG, data_path, workers, repeat);
    } else if (strcmp(workload, "bipartite") == 0) {
        rc = run_workload(WL_BP, data_path, workers, repeat);
    } else if (strcmp(workload, "andersen") == 0) {
        if (!data_andersen_path) {
            fprintf(stderr, "error: andersen requires --data-andersen DIR\n");
            return 1;
        }
        rc = run_andersen_workload(data_andersen_path, workers, repeat);
    } else if (strcmp(workload, "dyck") == 0) {
        if (!data_dyck_path) {
            fprintf(stderr, "error: dyck requires --data-dyck DIR\n");
            return 1;
        }
        rc = run_dyck_workload(data_dyck_path, workers, repeat);
    } else if (strcmp(workload, "cspa") == 0) {
        if (!data_cspa_path) {
            fprintf(stderr, "error: cspa requires --data-cspa DIR\n");
            return 1;
        }
        rc = run_cspa_incremental_workload(data_cspa_path, workers, repeat);
    } else if (strcmp(workload, "cspa-fast") == 0) {
        if (!data_cspa_path) {
            fprintf(stderr, "error: cspa-fast requires --data-cspa DIR\n");
            return 1;
        }
        rc = run_cspa_workload(data_cspa_path, workers, repeat);
    } else if (strcmp(workload, "csda") == 0) {
        if (!data_csda_path) {
            fprintf(stderr, "error: csda requires --data-csda DIR\n");
            return 1;
        }
        rc = run_csda_workload(data_csda_path, workers, repeat);
    } else if (strcmp(workload, "galen") == 0) {
        if (!data_galen_path) {
            fprintf(stderr, "error: galen requires --data-galen DIR\n");
            return 1;
        }
        rc = run_galen_workload(data_galen_path, workers, repeat);
    } else if (strcmp(workload, "polonius") == 0) {
        if (!data_polonius_path) {
            fprintf(stderr, "error: polonius requires --data-polonius DIR\n");
            return 1;
        }
        rc = run_polonius_workload(data_polonius_path, workers, repeat);
    } else if (strcmp(workload, "ddisasm") == 0) {
        if (!data_ddisasm_path) {
            fprintf(stderr, "error: ddisasm requires --data-ddisasm DIR\n");
            return 1;
        }
        rc = run_ddisasm_workload(data_ddisasm_path, workers, repeat);
    } else if (strcmp(workload, "crdt") == 0) {
        if (!data_crdt_path) {
            fprintf(stderr, "error: crdt requires --data-crdt DIR\n");
            return 1;
        }
        rc = run_crdt_workload(data_crdt_path, workers, repeat);
    } else if (strcmp(workload, "doop") == 0) {
        if (!data_doop_path) {
            fprintf(stderr, "error: doop requires --data-doop DIR\n");
            return 1;
        }
        rc = run_doop_workload(data_doop_path, workers, repeat);
    } else if (strcmp(workload, "all") == 0) {
        for (int i = 0; i < WL_COUNT; i++) {
            if (i == WL_SSSP && !data_weighted_path) {
                fprintf(stderr, "note: skipping sssp (no --data-weighted)\n");
                continue;
            }
            const char *dp = (i == WL_SSSP) ? data_weighted_path : data_path;
            int r = run_workload(i, dp, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_andersen_path) {
            int r = run_andersen_workload(data_andersen_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_dyck_path) {
            int r = run_dyck_workload(data_dyck_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_cspa_path) {
            int r = run_cspa_workload(data_cspa_path, workers, repeat);
            if (r != 0)
                rc = r;
            r = run_cspa_incremental_workload(data_cspa_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_csda_path) {
            int r = run_csda_workload(data_csda_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_galen_path) {
            int r = run_galen_workload(data_galen_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_polonius_path) {
            int r = run_polonius_workload(data_polonius_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_ddisasm_path) {
            int r = run_ddisasm_workload(data_ddisasm_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_crdt_path) {
            int r = run_crdt_workload(data_crdt_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
        if (data_doop_path) {
            int r = run_doop_workload(data_doop_path, workers, repeat);
            if (r != 0)
                rc = r;
        }
    } else {
        fprintf(stderr, "error: unknown workload '%s'\n", workload);
        return 1;
    }

    return rc;
}
