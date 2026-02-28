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

#include "bench_util.h"

#include "../wirelog/ffi/dd_ffi.h"
#include "../wirelog/wirelog.h"

#include <getopt.h>
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
#define WL_COUNT 5

static const char *wl_names[WL_COUNT] = { "tc", "reach", "cc", "sssp", "sg" };

/* Relation name used for CSV-to-facts conversion per workload */
static const char *wl_relations[WL_COUNT]
    = { "edge", "edge", "edge", "wedge", "edge" };

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
run_pipeline_count(const char *source, uint32_t num_workers, int64_t *out_count)
{
    if (!source)
        return -1;

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return -1;

    wl_dd_plan_t *dd_plan = NULL;
    int rc = wl_dd_plan_generate(prog, &dd_plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_ffi_plan_t *ffi = NULL;
    rc = wl_dd_marshal_plan(dd_plan, &ffi);
    if (rc != 0) {
        wl_dd_plan_free(dd_plan);
        wirelog_program_free(prog);
        return -1;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(num_workers);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wirelog_load_all_facts(prog, w);
    if (rc != 0) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct count_ctx ctx = { 0 };
    rc = wl_dd_execute_cb(ffi, w, count_tuple_cb, &ctx);

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    wirelog_program_free(prog);

    if (rc == 0 && out_count)
        *out_count = ctx.count;

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
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        int rc = run_pipeline_count(source, workers, &cnt);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
    }

    peak_rss = bench_peak_rss_kb();

    /* Sort times for min/median/max */
    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        int32_t nodes = (edge_count > 0) ? edge_count + 1 : 0;

        printf("%s\t%d\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64 "\t%" PRId64
               "\t%s\n",
               wl_names[wl_id], nodes, edge_count, workers, repeat, min_ms,
               median_ms, max_ms, peak_rss, tuples, "OK");
    } else {
        printf("%s\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\tFAIL\n", wl_names[wl_id],
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
    int status_ok = 1;

    for (int r = 0; r < repeat; r++) {
        bench_time_t t0 = bench_time_now();
        int64_t cnt = 0;
        int rc = run_pipeline_count(source, workers, &cnt);
        bench_time_t t1 = bench_time_now();

        times[r] = bench_time_diff_ms(t0, t1);

        if (rc != 0) {
            status_ok = 0;
            break;
        }
        tuples = cnt;
    }

    peak_rss = bench_peak_rss_kb();

    if (status_ok) {
        qsort(times, (size_t)repeat, sizeof(double), bench_cmp_double);
        double min_ms = times[0];
        double median_ms = times[repeat / 2];
        double max_ms = times[repeat - 1];

        printf("andersen\t-\t%d\t%u\t%d\t%.1f\t%.1f\t%.1f\t%" PRId64
               "\t%" PRId64 "\t%s\n",
               total_facts, workers, repeat, min_ms, median_ms, max_ms,
               peak_rss, tuples, "OK");
    } else {
        printf("andersen\t-\t-\t%u\t%d\t-\t-\t-\t-\t-\tFAIL\n", workers,
               repeat);
    }

    free(times);
    free(source);
    return status_ok ? 0 : -1;
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

static void
print_header(void)
{
    printf("workload\tnodes\tedges\tworkers\trepeat\tmin_ms\tmedian_ms"
           "\tmax_ms\tpeak_rss_kb\ttuples\tstatus\n");
}

static void
usage(const char *prog)
{
    fprintf(
        stderr,
        "Usage: %s --workload {tc|reach|cc|sssp|sg|andersen|all} --data FILE\n"
        "          [--data-weighted FILE] [--data-andersen DIR]\n"
        "          [--workers N] [--repeat R]\n"
        "\n"
        "  --data FILE           Unweighted edge CSV (src,dst)\n"
        "  --data-weighted FILE  Weighted edge CSV (src,dst,weight) for SSSP\n"
        "  --data-andersen DIR   Directory with addressOf/assign/load/store "
        "CSVs\n",
        prog);
}

int
main(int argc, char **argv)
{
    const char *workload = NULL;
    const char *data_path = NULL;
    const char *data_weighted_path = NULL;
    const char *data_andersen_path = NULL;
    uint32_t workers = 1;
    int repeat = 3;

    static struct option long_opts[] = {
        { "workload", required_argument, NULL, 'w' },
        { "data", required_argument, NULL, 'd' },
        { "data-weighted", required_argument, NULL, 'W' },
        { "data-andersen", required_argument, NULL, 'A' },
        { "workers", required_argument, NULL, 'j' },
        { "repeat", required_argument, NULL, 'r' },
        { "help", no_argument, NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "w:d:W:A:j:r:h", long_opts, NULL))
           != -1) {
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
        case 'j':
            workers = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'r':
            repeat = (int)strtol(optarg, NULL, 10);
            break;
        case 'h':
        default:
            usage(argv[0]);
            return (opt == 'h') ? 0 : 1;
        }
    }

    if (!workload || !data_path) {
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
    } else if (strcmp(workload, "andersen") == 0) {
        if (!data_andersen_path) {
            fprintf(stderr, "error: andersen requires --data-andersen DIR\n");
            return 1;
        }
        rc = run_andersen_workload(data_andersen_path, workers, repeat);
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
    } else {
        fprintf(stderr, "error: unknown workload '%s'\n", workload);
        return 1;
    }

    return rc;
}
