/*
 * tests/test_cspa_perf_gate.c
 *
 * Release-mode wall-time gate for the CSPA workload
 * (Context-Sensitive Points-to Analysis, multi-relation variant) using the
 * same Datalog source as bench_flowlog --workload cspa-fast and measures
 * CSPA W=1 median wall time over a fixed trial buffer.
 *
 * Failure semantics (same as tests/test_crdt_perf_gate.c):
 *
 *   - WIRELOG_PERF_GATE != "1"           -> SKIP (exit 77)
 *
 * Correctness-only mode (Issue #947): WIRELOG_GATE_CORRECTNESS_ONLY=1 runs
 * the workload once, checks the gold tuple and iteration counts and exits,
 * requiring neither WIRELOG_PERF_GATE nor a stripped log ceiling nor a pinned
 * governor.  Output validation is deterministic; gating it behind those meant
 * it never executed anywhere.
 *   - WL_LOG_COMPILE_MAX_LEVEL > ERROR:
 *       WIRELOG_PERF_REQUIRE != "1"      -> SKIP (default)
 *       WIRELOG_PERF_REQUIRE == "1"      -> FAIL (measurement build must
 *                                           keep TRACE-level logging stripped)
 *   - cpufreq governor != performance:
 *       WIRELOG_PERF_REQUIRE != "1"      -> SKIP (default)
 *       WIRELOG_PERF_REQUIRE == "1"      -> FAIL (merge gating requires
 *                                           pinned timing environment)
 *   - tuple count != gold (20,381)       -> FAIL (correctness sentinel checked
 *                                           before timing, each trial)
 *   - warm-up tuple count != gold
 *   - iteration count != 6 (gold)
 *   - baseline CoV > 3%                  -> SKIP
 *   - median wall > target               -> FAIL
 *
 * Baseline provenance:
 *   README / issue baseline for #818 and #736: 1,950 ms median at W=1
 *   for cspa-fast with --repeat 5. A 5% envelope yields 2050 ms
 *   target.
 *
 * The CSPA data fixtures live in bench/data/cspa/.  Meson registers this test
 * with WIRELOG_CSPA_DATA_DIR pointing at the project source tree so the
 * relative path is stable regardless of CWD.
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
    TRIALS = 9,                 /* odd -> median is a real sample */
    SKIP_EXIT = 77,
};

/* Median target in milliseconds.
 * 1.95s baseline for cspa-fast W=1 * 1.05 ~= 2.050s. */
#define WL_CSPA_PERF_GATE_TARGET_MS 2050

/* Trial buffer CoV ceiling mirrors CRDT's 3% noise envelope. */
#define MAX_BASELINE_COV 0.03

/* 128 KiB is enough for the two short CSPA csv sources in this workload. */
#define CSPA_SRC_BUFSZ (128 * 1024)

/* cspa-fast source used by bench_flowlog. */
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

struct count_ctx {
    int64_t total;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->total++;
}

/* Resolve the CSPA data directory.  Order:
 *   1. WIRELOG_CSPA_DATA_DIR (set by meson at test time).
 *   2. Relative fallback candidates for hand-run binaries.
 * Returns NULL if no candidate contains assign.csv. */
static const char *
resolve_cspa_data_dir_(void)
{
    static char buf[1024];

    const char *env = getenv("WIRELOG_CSPA_DATA_DIR");
    const char *candidates[8];
    int nc = 0;
    if (env && *env)
        candidates[nc++] = env;
    candidates[nc++] = "../bench/data/cspa";
    candidates[nc++] = "bench/data/cspa";
    candidates[nc++] = "../../bench/data/cspa";

    char path[1280];
    for (int i = 0; i < nc; i++) {
        snprintf(path, sizeof(path), "%s/%s", candidates[i], cspa_files[0]);
        struct stat st;
        if (stat(path, &st) == 0) {
            snprintf(buf, sizeof(buf), "%s", candidates[i]);
            return buf;
        }
    }
    return NULL;
}

/* Parse boolean-like env vars where anything in [0, n, N, f, F] means false.
 * Returns default_val when unset. */
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

/* Convert CSV rows "a,b" (or additional cols) into inline Datalog facts.
 * This mirrors the conversion used in bench_flowlog so the perf gate tracks
 * the same W=1 semantics as cspa-fast.
 */
static int
csv_to_inline_facts(const char *csv_path, const char *relation, char *buf,
    size_t bufsz, int32_t *out_row_count)
{
    FILE *f = fopen(csv_path, "r");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", csv_path);
        return -1;
    }

    size_t pos = 0;
    buf[0] = '\0';
    int32_t count = 0;
    char line[256];

    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0)
            continue;

        int32_t vals[8];
        int ncols = 0;
        size_t cursor = 0;
        while (line[cursor] != '\0' && ncols < 8) {
            while (line[cursor] == ',') {
                ++cursor;
            }
            if (line[cursor] == '\0') {
                break;
            }

            size_t tok_end = cursor;
            while (line[tok_end] != '\0' && line[tok_end] != ',') {
                ++tok_end;
            }

            char saved = '\0';
            if (line[tok_end] != '\0') {
                saved = line[tok_end];
                line[tok_end] = '\0';
            }

            char *stop = NULL;
            vals[ncols++] = (int32_t)strtol(line + cursor, &stop, 10);
            (void)stop;

            if (saved != '\0') {
                line[tok_end] = saved;
            }
            cursor = tok_end + (line[tok_end] != '\0');
        }
        if (ncols == 0)
            continue;

        int n = snprintf(buf + pos, bufsz - pos, "%s(", relation);
        if (n < 0 || pos + (size_t)n >= bufsz)
            goto overflow;
        pos += (size_t)n;

        for (int c = 0; c < ncols; c++) {
            n = snprintf(buf + pos, bufsz - pos, "%" PRId32 "%s",
                    vals[c], (c + 1 < ncols ? ", " : ")"));
            if (n < 0 || pos + (size_t)n >= bufsz)
                goto overflow;
            pos += (size_t)n;
            if (c + 1 < ncols)
                continue;
            n = snprintf(buf + pos, bufsz - pos, ".\n");
            if (n < 0 || pos + (size_t)n >= bufsz)
                goto overflow;
            pos += (size_t)n;
        }
        count++;
    }
    if (pos >= 1 && pos < bufsz) {
        buf[pos] = '\0';
    } else if (pos == 0) {
        buf[0] = '\0';
    }
    if (fclose(f) != 0) {
        fprintf(stderr, "warning: failed to close '%s'\n", csv_path);
    }
    if (out_row_count)
        *out_row_count = count;
    return 0;

overflow:
    fclose(f);
    return -1;
}

/* Run the CSPA pipeline once.  Returns 0 on success and writes tuple count and
 * iteration count to the out-pointers; -1 on any pipeline error.
 * Mirrors run_pipeline_count() in bench/bench_flowlog.c, keeping this test
 * independent of the benchmark binary. */
static int
run_cspa_once_(const char *data_dir, uint32_t num_workers,
    int64_t *out_count, uint32_t *out_iters)
{
    char *bufs[2] = { NULL, NULL };
    size_t per_buf = CSPA_SRC_BUFSZ / 2;

    for (int i = 0; i < 2; i++) {
        bufs[i] = (char *)malloc(per_buf);
        if (!bufs[i]) {
            if (i > 0 && bufs[0]) {
                free(bufs[0]);
            }
            return -1;
        }

        char path[1024];
        int n = snprintf(path, sizeof(path), "%s/%s", data_dir, cspa_files[i]);
        if (n < 0 || (size_t)n >= sizeof(path)) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }

        int32_t count = 0;
        if (csv_to_inline_facts(path, cspa_rels[i], bufs[i], per_buf, &count)
            != 0) {
            for (int j = 0; j <= i; j++)
                free(bufs[j]);
            return -1;
        }
    }

    char *source = (char *)malloc(CSPA_SRC_BUFSZ);
    if (!source) {
        free(bufs[0]);
        free(bufs[1]);
        return -1;
    }

    int n = snprintf(source, CSPA_SRC_BUFSZ, cspa_template, bufs[0], bufs[1]);
    for (int i = 0; i < 2; i++)
        free(bufs[i]);

    if (n < 0 || (size_t)n >= CSPA_SRC_BUFSZ) {
        free(source);
        fprintf(stderr, "error: CSPA source buffer overflow\n");
        return -1;
    }

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog) {
        free(source);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        free(source);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        free(source);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        free(source);
        return -1;
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    uint32_t iters = col_session_get_iteration_count(sess);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    free(source);

    if (rc != 0)
        return -1;

    if (out_count)
        *out_count = ctx.total;
    if (out_iters)
        *out_iters = iters;
    return 0;
}

int
main(void)
{
    const int correctness_only
        = parse_bool_env_("WIRELOG_GATE_CORRECTNESS_ONLY", 0);

    if (!correctness_only && !parse_bool_env_("WIRELOG_PERF_GATE", 0)) {
        fprintf(stderr,
            "test_cspa_perf_gate: SKIP: set WIRELOG_PERF_GATE=1 to run "
            "(designed for dedicated perf hardware, not shared CI runners)\n");
        return SKIP_EXIT;
    }

    if (!correctness_only && WL_LOG_COMPILE_MAX_LEVEL > WL_LOG_ERROR) {
        if (parse_bool_env_("WIRELOG_PERF_REQUIRE", 0)) {
            fprintf(stderr,
                "test_cspa_perf_gate: FAIL: WIRELOG_PERF_REQUIRE=1 but "
                "WL_LOG_COMPILE_MAX_LEVEL=%d > ERROR.  The build is not the "
                "perf-gate measurement build (reconfigure with "
                "-Dwirelog_log_max_level=error so inner-loop log sites "
                "are stripped).\n",
                (int)WL_LOG_COMPILE_MAX_LEVEL);
            return 1;
        }
        fprintf(stderr,
            "test_cspa_perf_gate: SKIP: WL_LOG_COMPILE_MAX_LEVEL=%d > ERROR; "
            "build with -Dwirelog_log_max_level=error so the inner-loop "
            "log sites are stripped at compile time\n",
            (int)WL_LOG_COMPILE_MAX_LEVEL);
        return SKIP_EXIT;
    }

    if (!correctness_only && !wl_perf_stability_env_ok()) {
        if (parse_bool_env_("WIRELOG_PERF_REQUIRE", 0)) {
            fprintf(stderr,
                "test_cspa_perf_gate: FAIL: WIRELOG_PERF_REQUIRE=1 but host "
                "is not configured for stable timing (cpufreq governor must be "
                "'performance').  Refusing to skip.\n");
            return 1;
        }
        return SKIP_EXIT;
    }

    const char *data_dir = resolve_cspa_data_dir_();
    if (!data_dir) {
        fprintf(stderr,
            "test_cspa_perf_gate: SKIP: CSPA data dir not found.  Set "
            "WIRELOG_CSPA_DATA_DIR or run from a CWD where "
            "bench/data/cspa/assign.csv resolves.\n");
        return SKIP_EXIT;
    }

    int64_t warm_count = 0;
    uint32_t warm_iters = 0;
    if (run_cspa_once_(data_dir, 1, &warm_count, &warm_iters) != 0) {
        fprintf(stderr,
            "test_cspa_perf_gate: FAIL: CSPA pipeline error on warm-up\n");
        return 1;
    }
    if (warm_count != 20381 || warm_iters != 6) {
        fprintf(stderr,
            "test_cspa_perf_gate: FAIL: warm-up tuple count %" PRId64
            " != gold 20381 or iterations %" PRIu32 " != gold 6\n",
            warm_count, warm_iters);
        return 1;
    }

    if (correctness_only) {
        printf("test_cspa_perf_gate: correctness OK (tuples=%" PRId64
            " iters=%" PRIu32 ")\n", warm_count, warm_iters);
        return 0;
    }

    double trials_ms[TRIALS];
    int64_t last_count = 0;
    uint32_t last_iters = 0;

    for (int i = 0; i < TRIALS; i++) {
        wl_perf_ns_t t0 = wl_perf_now_ns();
        int64_t count = 0;
        uint32_t iters = 0;
        int rc = run_cspa_once_(data_dir, 1, &count, &iters);
        wl_perf_ns_t t1 = wl_perf_now_ns();

        if (rc != 0) {
            fprintf(stderr,
                "test_cspa_perf_gate: FAIL: CSPA pipeline error on trial %d\n",
                i);
            return 1;
        }
        if (count != 20381 || iters != 6) {
            fprintf(stderr,
                "test_cspa_perf_gate: FAIL: trial %d tuple count %" PRId64
                " != gold 20381 or iterations %" PRIu32 " != gold 6\n",
                i, count, iters);
            return 1;
        }
        trials_ms[i] = (double)(t1 - t0) / 1.0e6;
        last_count = count;
        last_iters = iters;
    }

    double mean = wl_perf_mean_ms(trials_ms, (size_t)TRIALS);
    double stdev = wl_perf_stdev_ms(trials_ms, (size_t)TRIALS, mean);
    double cov = (mean > 0.0) ? stdev / mean : 0.0;
    double median = wl_perf_median_ms_inplace(trials_ms, (size_t)TRIALS);

    fprintf(stderr,
        "test_cspa_perf_gate: trials=%d workers=1\n"
        "  data_dir   = %s\n"
        "  tuples     = %" PRId64 "/20,381\n"
        "  iterations = %" PRIu32 "/6\n"
        "  mean_ms    = %.1f\n"
        "  stdev_ms   = %.2f (CoV %.3f%%)\n"
        "  median_ms  = %.1f (target %d)\n",
        TRIALS, data_dir,
        last_count,
        last_iters,
        mean, stdev, cov * 100.0,
        median, WL_CSPA_PERF_GATE_TARGET_MS);

    if (cov > MAX_BASELINE_COV) {
        fprintf(stderr,
            "test_cspa_perf_gate: SKIP: baseline CoV %.3f%% exceeds %.1f%% "
            "ceiling; measurement too noisy to gate at %d ms\n",
            cov * 100.0, MAX_BASELINE_COV * 100.0,
            WL_CSPA_PERF_GATE_TARGET_MS);
        return SKIP_EXIT;
    }

    if (median > (double)WL_CSPA_PERF_GATE_TARGET_MS) {
        fprintf(stderr,
            "test_cspa_perf_gate: FAIL: median %.1f ms exceeds target %d ms "
            "(regression)\n",
            median, WL_CSPA_PERF_GATE_TARGET_MS);
        return 1;
    }

    puts("test_cspa_perf_gate OK");
    return 0;
}
