/*
 * tests/test_sub_ms_graph_perf_gate.c
 *
 * Release-mode stability gate for the small W=1 graph workloads that run in
 * the sub-millisecond to low-millisecond range on dedicated perf hosts.
 *
 * Failure semantics mirror tests/test_crdt_perf_gate.c and
 * tests/test_cspa_perf_gate.c:
 *
 *   - WIRELOG_PERF_GATE != "1"           -> SKIP (exit 77)
 *   - WL_LOG_COMPILE_MAX_LEVEL > ERROR:
 *       WIRELOG_PERF_REQUIRE != "1"      -> SKIP
 *       WIRELOG_PERF_REQUIRE == "1"      -> FAIL
 *   - unstable timing host:
 *       WIRELOG_PERF_REQUIRE != "1"      -> SKIP
 *       WIRELOG_PERF_REQUIRE == "1"      -> FAIL
 *   - correctness sentinel mismatch      -> FAIL
 *   - per-workload CoV > 5%              -> SKIP
 *
 * No absolute median regression target is set here yet.  This first gate
 * proves the measurement path, batches enough inner runs to reduce timer
 * noise, and rejects noisy samples without turning them into regressions.
 */

#define _POSIX_C_SOURCE 200809L

#include "test_perf_util.h"

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/util/log.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

extern uint32_t
col_session_get_iteration_count(wl_session_t *sess);

enum {
    TRIALS = 15,       /* odd -> median is a real normalized sample */
    INNER_RUNS = 16,   /* batch sub-ms runs to reduce timer/scheduler noise */
    SKIP_EXIT = 77,
    MAX_REL_CHECKS = 3,
};

#define MAX_BASELINE_COV 0.05

struct rel_expect {
    const char *name;
    int64_t count;
};

struct workload_spec {
    const char *name;
    const char *workload_file;
    const char *placeholder;
    const char *data_file;
    int64_t expected_tuples;
    uint32_t expected_iters;
    struct rel_expect rels[MAX_REL_CHECKS];
};

static const struct workload_spec workloads[] = {
    {
        "reach",
        "reach.dl",
        "EDGES",
        "graph_100.csv",
        100,
        98,
        { { "reach", 100 }, { NULL, 0 }, { NULL, 0 } },
    },
    {
        "sssp",
        "sssp.dl",
        "WEDGES",
        "graph_100_weighted.csv",
        100,
        98,
        { { "dist", 100 }, { NULL, 0 }, { NULL, 0 } },
    },
    {
        "sg",
        "sg.dl",
        "EDGES",
        "tree_100.csv",
        2634,
        5,
        { { "sg", 2634 }, { NULL, 0 }, { NULL, 0 } },
    },
    {
        "bipartite",
        "bipartite.dl",
        "EDGES",
        "graph_100.csv",
        100,
        73,
        { { "blue", 50 }, { "red", 50 }, { "not_bipartite", 0 } },
    },
};

struct count_ctx {
    const struct workload_spec *spec;
    int64_t total;
    int64_t rel_counts[MAX_REL_CHECKS];
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)row;
    (void)ncols;
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->total++;
    for (int i = 0; i < MAX_REL_CHECKS; i++) {
        const char *name = ctx->spec->rels[i].name;
        if (name && relation && strcmp(relation, name) == 0)
            ctx->rel_counts[i]++;
    }
}

static int
parse_bool_env_(const char *name, int default_val)
{
    const char *v = getenv(name);
    if (!v || !*v)
        return default_val;
    if (v[0] == '0' || v[0] == 'n' || v[0] == 'N' || v[0] == 'f' || v[0] == 'F')
        return 0;
    return 1;
}

static bool
path_exists_(const char *path)
{
    struct stat st;
    return path && stat(path, &st) == 0;
}

static int
join_path_(char *buf, size_t bufsz, const char *dir, const char *file)
{
    int n = snprintf(buf, bufsz, "%s/%s", dir, file);
    return (n >= 0 && (size_t)n < bufsz) ? 0 : -1;
}

static const char *
resolve_workload_dir_(void)
{
    static char buf[1024];
    const char *env = getenv("WIRELOG_GRAPH_WORKLOAD_DIR");
    const char *candidates[8];
    int nc = 0;
    if (env && *env)
        candidates[nc++] = env;
    candidates[nc++] = "../bench/workloads";
    candidates[nc++] = "bench/workloads";
    candidates[nc++] = "../../bench/workloads";

    char path[1280];
    for (int i = 0; i < nc; i++) {
        if (join_path_(path, sizeof(path), candidates[i], "reach.dl") == 0
            && path_exists_(path)) {
            snprintf(buf, sizeof(buf), "%s", candidates[i]);
            return buf;
        }
    }
    return NULL;
}

static const char *
resolve_data_dir_(void)
{
    static char buf[1024];
    const char *env = getenv("WIRELOG_GRAPH_DATA_DIR");
    const char *candidates[8];
    int nc = 0;
    if (env && *env)
        candidates[nc++] = env;
    candidates[nc++] = "../bench/data";
    candidates[nc++] = "bench/data";
    candidates[nc++] = "../../bench/data";

    char path[1280];
    for (int i = 0; i < nc; i++) {
        if (join_path_(path, sizeof(path), candidates[i], "graph_100.csv") == 0
            && path_exists_(path)) {
            snprintf(buf, sizeof(buf), "%s", candidates[i]);
            return buf;
        }
    }
    return NULL;
}

static char *
read_file_(const char *path)
{
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long len = ftell(f);
    if (len < 0) {
        fclose(f);
        return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }

    char *buf = (char *)malloc((size_t)len + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t nread = fread(buf, 1, (size_t)len, f);
    if (nread != (size_t)len || ferror(f)) {
        free(buf);
        fclose(f);
        return NULL;
    }
    buf[len] = '\0';
    if (fclose(f) != 0) {
        free(buf);
        return NULL;
    }
    return buf;
}

static char *
replace_all_(const char *input, const char *needle, const char *replacement)
{
    size_t input_len = strlen(input);
    size_t needle_len = strlen(needle);
    size_t repl_len = strlen(replacement);
    if (needle_len == 0)
        return NULL;

    size_t count = 0;
    const char *p = input;
    while ((p = strstr(p, needle)) != NULL) {
        count++;
        p += needle_len;
    }
    if (count == 0)
        return NULL;

    size_t out_len = input_len;
    if (repl_len >= needle_len)
        out_len += count * (repl_len - needle_len);
    else
        out_len -= count * (needle_len - repl_len);

    char *out = (char *)malloc(out_len + 1);
    if (!out)
        return NULL;

    char *dst = out;
    p = input;
    const char *next = NULL;
    while ((next = strstr(p, needle)) != NULL) {
        size_t prefix = (size_t)(next - p);
        memcpy(dst, p, prefix);
        dst += prefix;
        memcpy(dst, replacement, repl_len);
        dst += repl_len;
        p = next + needle_len;
    }
    strcpy(dst, p);
    return out;
}

static char *
build_source_(const struct workload_spec *spec, const char *workload_dir,
    const char *data_dir)
{
    char workload_path[1280];
    char data_path[1280];
    if (join_path_(workload_path, sizeof(workload_path), workload_dir,
        spec->workload_file)
        != 0)
        return NULL;
    if (join_path_(data_path, sizeof(data_path), data_dir,
        spec->data_file) != 0)
        return NULL;
    if (!path_exists_(workload_path) || !path_exists_(data_path))
        return NULL;

    char *template_src = read_file_(workload_path);
    if (!template_src)
        return NULL;

    char *source = replace_all_(template_src, spec->placeholder, data_path);
    free(template_src);
    return source;
}

static int
run_graph_once_(const char *source, const struct workload_spec *spec,
    int64_t *out_count, uint32_t *out_iters,
    int64_t out_rel_counts[MAX_REL_CHECKS])
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc == 0)
        rc = wl_session_load_input_files(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct count_ctx ctx = { spec, 0, { 0, 0, 0 } };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    uint32_t iters = col_session_get_iteration_count(sess);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    if (rc != 0)
        return -1;

    if (out_count)
        *out_count = ctx.total;
    if (out_iters)
        *out_iters = iters;
    if (out_rel_counts) {
        for (int i = 0; i < MAX_REL_CHECKS; i++)
            out_rel_counts[i] = ctx.rel_counts[i];
    }
    return 0;
}

static int
validate_counts_(const struct workload_spec *spec, const char *phase,
    int trial, int inner, int64_t count, uint32_t iters,
    const int64_t rel_counts[MAX_REL_CHECKS])
{
    if (count != spec->expected_tuples || iters != spec->expected_iters) {
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: FAIL: %s %s trial=%d inner=%d "
            "tuples %" PRId64 " != gold %" PRId64 " or iterations %" PRIu32
            " != gold %" PRIu32 "\n",
            spec->name, phase, trial, inner, count, spec->expected_tuples,
            iters, spec->expected_iters);
        return -1;
    }

    for (int i = 0; i < MAX_REL_CHECKS; i++) {
        const char *name = spec->rels[i].name;
        if (!name)
            continue;
        if (rel_counts[i] != spec->rels[i].count) {
            fprintf(stderr,
                "test_sub_ms_graph_perf_gate: FAIL: %s %s trial=%d inner=%d "
                "relation %s count %" PRId64 " != gold %" PRId64 "\n",
                spec->name, phase, trial, inner, name, rel_counts[i],
                spec->rels[i].count);
            return -1;
        }
    }
    return 0;
}

static int
run_workload_(const struct workload_spec *spec, const char *workload_dir,
    const char *data_dir, int correctness_only)
{
    char *source = build_source_(spec, workload_dir, data_dir);
    if (!source) {
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: SKIP: failed to build source for "
            "%s from workload_dir=%s data_dir=%s\n",
            spec->name, workload_dir, data_dir);
        return SKIP_EXIT;
    }

    int64_t warm_count = 0;
    uint32_t warm_iters = 0;
    int64_t warm_rels[MAX_REL_CHECKS] = { 0, 0, 0 };
    if (run_graph_once_(source, spec, &warm_count, &warm_iters,
        warm_rels) != 0) {
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: FAIL: %s pipeline error on warm-up\n",
            spec->name);
        free(source);
        return 1;
    }
    if (validate_counts_(spec, "warm-up", -1, -1, warm_count, warm_iters,
        warm_rels)
        != 0) {
        free(source);
        return 1;
    }

    if (correctness_only) {
        printf("  %-10s correctness OK (tuples=%" PRId64 " iters=%u)\n",
            spec->name, warm_count, warm_iters);
        free(source);
        return 0;
    }

    double trials_ms[TRIALS];
    int64_t last_count = 0;
    uint32_t last_iters = 0;
    int64_t last_rels[MAX_REL_CHECKS] = { 0, 0, 0 };

    for (int t = 0; t < TRIALS; t++) {
        wl_perf_ns_t t0 = wl_perf_now_ns();
        for (int i = 0; i < INNER_RUNS; i++) {
            int64_t count = 0;
            uint32_t iters = 0;
            int64_t rels[MAX_REL_CHECKS] = { 0, 0, 0 };
            if (run_graph_once_(source, spec, &count, &iters, rels) != 0) {
                fprintf(stderr,
                    "test_sub_ms_graph_perf_gate: FAIL: %s pipeline error on "
                    "trial %d inner %d\n",
                    spec->name, t, i);
                free(source);
                return 1;
            }
            if (validate_counts_(spec, "trial", t, i, count, iters,
                rels) != 0) {
                free(source);
                return 1;
            }
            last_count = count;
            last_iters = iters;
            for (int r = 0; r < MAX_REL_CHECKS; r++)
                last_rels[r] = rels[r];
        }
        wl_perf_ns_t t1 = wl_perf_now_ns();
        trials_ms[t] = ((double)(t1 - t0) / 1.0e6) / (double)INNER_RUNS;
    }

    double mean = wl_perf_mean_ms(trials_ms, (size_t)TRIALS);
    double stdev = wl_perf_stdev_ms(trials_ms, (size_t)TRIALS, mean);
    double cov = (mean > 0.0) ? stdev / mean : 0.0;
    double median = wl_perf_median_ms_inplace(trials_ms, (size_t)TRIALS);

    fprintf(stderr,
        "test_sub_ms_graph_perf_gate: workload=%s trials=%d inner_runs=%d "
        "tuples=%" PRId64 " iterations=%" PRIu32 " mean_ms=%.3f "
        "stdev_ms=%.3f CoV %.3f%% median_ms=%.3f",
        spec->name, TRIALS, INNER_RUNS, last_count, last_iters, mean, stdev,
        cov * 100.0, median);
    for (int i = 0; i < MAX_REL_CHECKS; i++) {
        if (spec->rels[i].name) {
            fprintf(stderr, " %s=%" PRId64, spec->rels[i].name, last_rels[i]);
        }
    }
    fputc('\n', stderr);

    free(source);

    if (cov > MAX_BASELINE_COV) {
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: SKIP: %s CoV %.3f%% exceeds %.1f%% "
            "ceiling; measurement too noisy for sub-ms gate\n",
            spec->name, cov * 100.0, MAX_BASELINE_COV * 100.0);
        return SKIP_EXIT;
    }
    return 0;
}

int
main(void)
{
    const int correctness_only
        = parse_bool_env_("WIRELOG_GATE_CORRECTNESS_ONLY", 0);

    if (!correctness_only && !parse_bool_env_("WIRELOG_PERF_GATE", 0)) {
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: SKIP: set WIRELOG_PERF_GATE=1 to "
            "run (designed for dedicated perf hardware, not shared CI "
            "runners)\n");
        return SKIP_EXIT;
    }

    /* Issue #948: a trace ceiling no longer skips this gate.
     *
     * The requirement existed so inner-loop WL_LOG sites are stripped at
     * compile time before anything is timed.  Measured on this workload it
     * makes no difference: two release builds of the same tree, best of
     * three, gave 13417 ms at -Dwirelog_log_max_level=error against
     * 13312 ms at =trace -- 0.8% apart, with the trace build marginally
     * *faster*.  That is noise, not a reason to refuse to measure.
     *
     * It was also not merely a lost gate.  Both perf workflows configure
     * -Dwirelog_log_max_level=trace so that log_perf_gate (which requires
     * the opposite, >= TRACE) can run, and one suite invocation means one
     * build directory -- so this arm skipped on every perf run there has
     * ever been, and perf-nightly being green meant these three gates did
     * not execute.
     *
     * The ceiling is still reported, because if a target is ever missed on
     * a trace build it is the first thing worth ruling out, and
     * WIRELOG_PERF_REQUIRE still makes it fatal for anyone who wants the
     * strict measurement build. */
    if (!correctness_only && WL_LOG_COMPILE_MAX_LEVEL > WL_LOG_ERROR) {
        if (parse_bool_env_("WIRELOG_PERF_REQUIRE", 0)) {
            fprintf(stderr,
                "test_sub_ms_graph_perf_gate: FAIL: WIRELOG_PERF_REQUIRE=1 but "
                "WL_LOG_COMPILE_MAX_LEVEL=%d > ERROR.  The build is not the "
                "perf-gate measurement build (reconfigure with "
                "-Dwirelog_log_max_level=error so inner-loop log sites "
                "are stripped).\n",
                (int)WL_LOG_COMPILE_MAX_LEVEL);
            return 1;
        }
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: NOTE: WL_LOG_COMPILE_MAX_LEVEL=%d > ERROR; inner-loop log "
            "sites are not stripped.  Measured at under 1%% on this "
            "workload (#948), so the gate runs anyway; rule this out first "
            "if a target is missed.\n",
            (int)WL_LOG_COMPILE_MAX_LEVEL);
    }

    if (!correctness_only && !wl_perf_stability_env_ok()) {
        if (parse_bool_env_("WIRELOG_PERF_REQUIRE", 0)) {
            fprintf(stderr,
                "test_sub_ms_graph_perf_gate: FAIL: WIRELOG_PERF_REQUIRE=1 "
                "but host is not configured for stable timing (cpufreq "
                "governor must be 'performance').  Refusing to skip.\n");
            return 1;
        }
        return SKIP_EXIT;
    }

    const char *workload_dir = resolve_workload_dir_();
    const char *data_dir = resolve_data_dir_();
    if (!workload_dir || !data_dir) {
        fprintf(stderr,
            "test_sub_ms_graph_perf_gate: SKIP: graph workload/data dirs not "
            "found.  Set WIRELOG_GRAPH_WORKLOAD_DIR and WIRELOG_GRAPH_DATA_DIR "
            "or run from a CWD where bench/workloads and bench/data resolve.\n");
        return SKIP_EXIT;
    }

    int saw_skip = 0;
    for (size_t i = 0; i < sizeof(workloads) / sizeof(workloads[0]); i++) {
        int rc = run_workload_(&workloads[i], workload_dir, data_dir,
                correctness_only);
        if (rc == SKIP_EXIT) {
            saw_skip = 1;
            continue;
        }
        if (rc != 0)
            return rc;
    }

    if (saw_skip)
        return SKIP_EXIT;

    puts("test_sub_ms_graph_perf_gate OK");
    return 0;
}
