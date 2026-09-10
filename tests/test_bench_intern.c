/*
 * test_bench_intern.c - Smoke test for bench/bench_intern (Issue #1472)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Execs bench_intern over every mode, writer count and governor setting
 * with a small put count and asserts that it exits 0, emits the metric
 * tokens for every scenario, reports a zero governor delta for duplicate
 * puts, and never prints an error token.  The bench's own hard checks
 * (every put admitted, final count as expected, duplicates never
 * reserve) turn a broken run into a non-zero exit, which this test
 * turns into a failure.  Timing values are not asserted: this is a
 * smoke test, not a gate.
 *
 * The path to the bench_intern binary is passed as argv[1] by the meson
 * test() call (via depends: and args:); if omitted it defaults to
 * "./bench/bench_intern" relative to the build root.
 */
/* popen/pclose require POSIX; MSVC exposes them as _popen/_pclose. */
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
#define BENCH_OUT_MAX 16384

static int
count_token(const char *out, const char *token)
{
    int n = 0;
    const char *p = out;
    while ((p = strstr(p, token)) != NULL) {
        n++;
        p += strlen(token);
    }
    return n;
}

int
main(int argc, char **argv)
{
    const char *bin = (argc > 1) ? argv[1] : "./bench/bench_intern";
    char cmd[BENCH_CMD_MAX];
    int n = snprintf(cmd, sizeof(cmd),
            "%s --mode all --writers all --iters 2000 --governor both", bin);
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        fprintf(stderr, "bench_intern_smoke: binary path too long\n");
        return 1;
    }
    FILE *fp = popen(cmd, "r");
    if (!fp) {
        fprintf(stderr, "bench_intern_smoke: popen failed for: %s\n", cmd);
        return 1;
    }
    char out[BENCH_OUT_MAX];
    size_t total = 0;
    size_t got;
    while (total < sizeof(out) - 1
        && (got = fread(out + total, 1, sizeof(out) - 1 - total, fp)) > 0)
        total += got;
    out[total] = '\0';
    int rc = pclose(fp);
    if (rc != 0) {
        fprintf(stderr,
            "bench_intern_smoke: bench_intern exited non-zero (rc=%d)\n"
            "output: %s\n", rc, out);
        return 1;
    }

    /* 2 modes x 3 writer counts x 2 governor settings = 12 scenarios. */
    static const struct {
        const char *token;
        int expected;
    } checks[] = {
        { "mode=unique ", 6 },
        { "mode=dup ", 6 },
        { "writers=1 ", 4 },
        { "writers=4 ", 4 },
        { "writers=8 ", 4 },
        { "governor=on ", 6 },
        { "governor=off ", 6 },
        { " ns_put=", 12 }, /* leading space: agg_ns_put= must not count */
        { "agg_ns_put=", 12 },
        { "p99=", 12 },
        { "grow_share=", 12 },
        { "wall_ms=", 12 },
        { "gov_reserved_delta=0", 3 },
        { "clock_ns=", 1 },
        { "peak_rss_kb=", 1 },
    };
    int ok = 1;
    for (size_t i = 0; i < sizeof(checks) / sizeof(checks[0]); i++) {
        int seen = count_token(out, checks[i].token);
        if (seen != checks[i].expected) {
            fprintf(stderr,
                "bench_intern_smoke: token '%s' seen %d times, expected %d\n",
                checks[i].token, seen, checks[i].expected);
            ok = 0;
        }
    }
    if (strstr(out, "error=")) {
        fprintf(stderr, "bench_intern_smoke: bench reported an error\n");
        ok = 0;
    }
    if (!ok) {
        fprintf(stderr, "bench_intern_smoke: stdout was:\n%s\n", out);
        return 1;
    }
    printf("bench_intern_smoke: OK\n");
    return 0;
}
