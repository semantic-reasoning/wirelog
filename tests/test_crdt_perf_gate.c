/*
 * tests/test_crdt_perf_gate.c
 *
 * Release-mode wall-time gate for the CRDT verification workload
 * (Kleppmann sequence CRDT, 23 rules across 8 strata).  Drives the
 * same Datalog source as bench_flowlog --workload crdt and measures
 * CRDT W=1 median wall time over a small fixed-size trial buffer.
 *
 * Failure semantics (mirrors tests/test_log_perf_gate.c plus an
 * explicit FAIL-LOUD mode for misconfigured CI runners):
 *
 *   - WIRELOG_PERF_GATE != "1"           -> SKIP (exit 77)
 *   - WL_LOG_COMPILE_MAX_LEVEL > ERROR:
 *       WIRELOG_PERF_REQUIRE != "1"      -> SKIP (default; un-stripped
 *                                           log sites pessimize the
 *                                           inner loop, so the gate
 *                                           would measure the wrong
 *                                           thing)
 *       WIRELOG_PERF_REQUIRE == "1"      -> FAIL (loud; mirrors the
 *                                           governor escalator -- the
 *                                           merge runner that asserts
 *                                           REQUIRE must be on the
 *                                           perf-gate measurement
 *                                           build, not a default
 *                                           trace-level build)
 *   - cpufreq governor != performance:
 *       WIRELOG_PERF_REQUIRE != "1"      -> SKIP (default; lets dev
 *                                           hosts and unconfigured
 *                                           runners opt out)
 *       WIRELOG_PERF_REQUIRE == "1"      -> FAIL (loud, for the merge
 *                                           runner where the operator
 *                                           is asserting the host is
 *                                           configured)
 *   - tuple count != gold (1,301,914)    -> FAIL (correctness sentinel
 *                                           checked BEFORE the timing
 *                                           assertion so a wrong-output
 *                                           bug never trips the timing
 *                                           gate spuriously)
 *   - baseline CoV > 3%                  -> SKIP (measurement noise
 *                                           exceeds the budget)
 *   - median wall > target               -> FAIL (the gate fires)
 *
 * The CRDT data fixtures live in bench/data/crdt/.  meson registers
 * the test with WIRELOG_CRDT_DATA_DIR pointing at the project source
 * tree so the relative path is stable regardless of CWD.
 */

#define _POSIX_C_SOURCE 200809L

#include "test_perf_util.h"

#include "../bench/bench_crdt_workload.h"
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

/* Median budget, in milliseconds.  Provenance: baseline median 18,890 ms
 * captured on the dev box with W=1, repeat=1, governor=NOT-asserted.
 * 1.05 envelope accounts for run-to-run variance + the tighter governor
 * pinning the perf-gate runner uses.  See .omc/perf/CRDT_BASELINE.md
 * section 1.  Tighten this when post-optimization commits land: each
 * commit's PR records a fresh n=9 median and the gate ratchets down. */
#define WL_CRDT_PERF_GATE_TARGET_MS 19840

/* Coefficient-of-variation ceiling for the trial buffer.  3% mirrors
 * the WL_LOG perf gate's tolerance; CRDT runs ~19 s per trial so even
 * 3% is ~570 ms of jitter, well within "still meaningful" if the
 * cpufreq governor is pinned. */
#define MAX_BASELINE_COV 0.03

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

/* Resolve the CRDT data directory.  Order:
 *   1. WIRELOG_CRDT_DATA_DIR (set by meson at test time).
 *   2. Relative-from-build-dir candidates so a hand-run binary still
 *      finds the data when meson did not export the env var.
 * Returns NULL if no candidate resolves to a directory containing
 * Insert_input.csv. */
static const char *
resolve_crdt_data_dir_(void)
{
    static char buf[1024];

    const char *env = getenv("WIRELOG_CRDT_DATA_DIR");
    const char *candidates[8];
    int nc = 0;
    if (env && *env)
        candidates[nc++] = env;
    candidates[nc++] = "../bench/data/crdt";
    candidates[nc++] = "bench/data/crdt";
    candidates[nc++] = "../../bench/data/crdt";

    char path[1280];
    for (int i = 0; i < nc; i++) {
        snprintf(path, sizeof(path), "%s/Insert_input.csv", candidates[i]);
        struct stat st;
        if (stat(path, &st) == 0) {
            snprintf(buf, sizeof(buf), "%s", candidates[i]);
            return buf;
        }
    }
    return NULL;
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

/* Run the CRDT pipeline once.  Returns 0 on success and writes the
 * tuple count and iteration count to the out-pointers; -1 on any
 * pipeline error.  Mirrors run_pipeline_count() in
 * bench/bench_flowlog.c, kept independent so this test does not depend
 * on the bench driver as a library. */
static int
run_crdt_once_(const char *source, uint32_t num_workers,
    int64_t *out_count, uint32_t *out_iters)
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
    return 0;
}

int
main(void)
{
    if (!parse_bool_env_("WIRELOG_PERF_GATE", 0)) {
        fprintf(stderr,
            "test_crdt_perf_gate: SKIP: set WIRELOG_PERF_GATE=1 to run "
            "(designed for dedicated perf hardware, not shared CI runners)\n");
        return SKIP_EXIT;
    }

    if (WL_LOG_COMPILE_MAX_LEVEL > WL_LOG_ERROR) {
        if (parse_bool_env_("WIRELOG_PERF_REQUIRE", 0)) {
            fprintf(stderr,
                "test_crdt_perf_gate: FAIL: WIRELOG_PERF_REQUIRE=1 but "
                "WL_LOG_COMPILE_MAX_LEVEL=%d > ERROR.  The build is not the "
                "perf-gate measurement build (reconfigure with "
                "-Dwirelog_log_max_level=error so the inner-loop log sites "
                "are stripped at compile time).  Refusing to skip.\n",
                (int)WL_LOG_COMPILE_MAX_LEVEL);
            return 1;
        }
        fprintf(stderr,
            "test_crdt_perf_gate: SKIP: WL_LOG_COMPILE_MAX_LEVEL=%d > ERROR; "
            "build with -Dwirelog_log_max_level=error so the inner-loop "
            "log sites are stripped at compile time\n",
            (int)WL_LOG_COMPILE_MAX_LEVEL);
        return SKIP_EXIT;
    }

    if (!wl_perf_stability_env_ok()) {
        if (parse_bool_env_("WIRELOG_PERF_REQUIRE", 0)) {
            fprintf(stderr,
                "test_crdt_perf_gate: FAIL: WIRELOG_PERF_REQUIRE=1 but the "
                "host is not configured for stable timing (cpufreq governor "
                "must be 'performance').  Refusing to skip.\n");
            return 1;
        }
        return SKIP_EXIT;
    }

    const char *data_dir = resolve_crdt_data_dir_();
    if (!data_dir) {
        fprintf(stderr,
            "test_crdt_perf_gate: SKIP: CRDT data dir not found.  Set "
            "WIRELOG_CRDT_DATA_DIR or run from a CWD where "
            "bench/data/crdt/Insert_input.csv resolves.\n");
        return SKIP_EXIT;
    }

    char *source = (char *)malloc(WL_BENCH_CRDT_SRC_BUFSZ);
    if (!source) {
        fprintf(stderr,
            "test_crdt_perf_gate: FAIL: out of memory allocating CRDT source\n");
        return 1;
    }
    int n = snprintf(source, WL_BENCH_CRDT_SRC_BUFSZ, wl_bench_crdt_template,
            data_dir, data_dir);
    if (n < 0 || (size_t)n >= WL_BENCH_CRDT_SRC_BUFSZ) {
        fprintf(stderr,
            "test_crdt_perf_gate: FAIL: CRDT source buffer overflow "
            "(data_dir too long?)\n");
        free(source);
        return 1;
    }

    /* One untimed warm-up to amortize page cache + first-run engine
     * lazy init.  CRDT is too long to median-of-9 with no warm-up
     * without paying it as the worst trial. */
    int64_t warm_count = 0;
    uint32_t warm_iters = 0;
    if (run_crdt_once_(source, 1, &warm_count, &warm_iters) != 0) {
        fprintf(stderr,
            "test_crdt_perf_gate: FAIL: CRDT pipeline error on warm-up\n");
        free(source);
        return 1;
    }
    if (warm_count != WL_BENCH_CRDT_GOLD_TUPLES) {
        fprintf(stderr,
            "test_crdt_perf_gate: FAIL: warm-up tuple count %" PRId64
            " != gold %" PRId64
            " (correctness regression; refusing to time)\n",
            warm_count, WL_BENCH_CRDT_GOLD_TUPLES);
        free(source);
        return 1;
    }

    double trials_ms[TRIALS];
    int64_t last_count = 0;
    uint32_t last_iters = 0;

    for (int i = 0; i < TRIALS; i++) {
        wl_perf_ns_t t0 = wl_perf_now_ns();
        int64_t count = 0;
        uint32_t iters = 0;
        int rc = run_crdt_once_(source, 1, &count, &iters);
        wl_perf_ns_t t1 = wl_perf_now_ns();

        if (rc != 0) {
            fprintf(stderr,
                "test_crdt_perf_gate: FAIL: CRDT pipeline error on trial %d\n",
                i);
            free(source);
            return 1;
        }
        if (count != WL_BENCH_CRDT_GOLD_TUPLES) {
            fprintf(stderr,
                "test_crdt_perf_gate: FAIL: trial %d tuple count %" PRId64
                " != gold %" PRId64 "\n",
                i, count, WL_BENCH_CRDT_GOLD_TUPLES);
            free(source);
            return 1;
        }
        trials_ms[i] = (double)(t1 - t0) / 1.0e6;
        last_count = count;
        last_iters = iters;
    }
    free(source);

    double mean = wl_perf_mean_ms(trials_ms, (size_t)TRIALS);
    double stdev = wl_perf_stdev_ms(trials_ms, (size_t)TRIALS, mean);
    double cov = (mean > 0.0) ? stdev / mean : 0.0;
    double median = wl_perf_median_ms_inplace(trials_ms, (size_t)TRIALS);

    fprintf(stderr,
        "test_crdt_perf_gate: trials=%d workers=1\n"
        "  data_dir   = %s\n"
        "  tuples     = %" PRId64 " (gold %" PRId64 ")\n"
        "  iterations = %" PRIu32 "\n"
        "  mean_ms    = %.1f\n"
        "  stdev_ms   = %.2f (CoV %.3f%%)\n"
        "  median_ms  = %.1f (target %d)\n",
        TRIALS, data_dir,
        last_count, WL_BENCH_CRDT_GOLD_TUPLES,
        last_iters, mean, stdev, cov * 100.0,
        median, WL_CRDT_PERF_GATE_TARGET_MS);

    if (cov > MAX_BASELINE_COV) {
        fprintf(stderr,
            "test_crdt_perf_gate: SKIP: baseline CoV %.3f%% exceeds %.1f%% "
            "ceiling; measurement too noisy to gate at %d ms\n",
            cov * 100.0, MAX_BASELINE_COV * 100.0,
            WL_CRDT_PERF_GATE_TARGET_MS);
        return SKIP_EXIT;
    }

    if (median > (double)WL_CRDT_PERF_GATE_TARGET_MS) {
        fprintf(stderr,
            "test_crdt_perf_gate: FAIL: median %.1f ms exceeds target %d ms "
            "(regression)\n",
            median, WL_CRDT_PERF_GATE_TARGET_MS);
        return 1;
    }

    puts("test_crdt_perf_gate OK");
    return 0;
}
