/*
 * test_bench_compound.c - Smoke test for bench/bench_compound (Issue #536)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD smoke test: execs bench_compound --mode parser --iters 10 and
 * asserts that stdout contains the percentile tokens p50=, p95=, p99=.
 * The test is registered as bench_compound_smoke.
 *
 * The path to the bench_compound binary is passed as argv[1] by the
 * meson test() call (via depends: and args:); if omitted it defaults to
 * "./bench/bench_compound" relative to the build root.
 */

/* popen/pclose require POSIX — match bench_flowlog.c convention.
 * MSVC exposes these as _popen/_pclose under <stdio.h>. */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#define popen  _popen
#define pclose _pclose
#endif

#define BENCH_CMD_MAX 512
#define BENCH_OUT_MAX 4096

int
main(int argc, char **argv)
{
    const char *bin = (argc > 1) ? argv[1] : "./bench/bench_compound";

    char cmd[BENCH_CMD_MAX];
    int n = snprintf(cmd, sizeof(cmd), "%s --mode parser --iters 10", bin);
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        fprintf(stderr, "bench_compound_smoke: binary path too long\n");
        return 1;
    }

    FILE *fp = popen(cmd, "r");
    if (!fp) {
        fprintf(stderr, "bench_compound_smoke: popen failed for: %s\n", cmd);
        return 1;
    }

    char out[BENCH_OUT_MAX];
    size_t total = 0;
    size_t got;
    while (total < sizeof(out) - 1 &&
        (got = fread(out + total, 1, sizeof(out) - 1 - total, fp)) > 0) {
        total += got;
    }
    out[total] = '\0';

    int rc = pclose(fp);
    if (rc != 0) {
        fprintf(stderr,
            "bench_compound_smoke: bench_compound exited non-zero (rc=%d)\n"
            "output: %s\n", rc, out);
        return 1;
    }

    /* Assert percentile tokens and RSS measurement appear in stdout. */
    int ok = 1;
    if (!strstr(out, "p50=")) {
        fprintf(stderr, "bench_compound_smoke: missing p50= in output\n");
        ok = 0;
    }
    if (!strstr(out, "p95=")) {
        fprintf(stderr, "bench_compound_smoke: missing p95= in output\n");
        ok = 0;
    }
    if (!strstr(out, "p99=")) {
        fprintf(stderr, "bench_compound_smoke: missing p99= in output\n");
        ok = 0;
    }
    if (!strstr(out, "peak_rss_kb=")) {
        fprintf(stderr,
            "bench_compound_smoke: missing peak_rss_kb= in output\n");
        ok = 0;
    }

    if (!ok) {
        fprintf(stderr, "bench_compound_smoke: stdout was:\n%s\n", out);
        return 1;
    }

    printf("bench_compound_smoke OK\n");
    return 0;
}
