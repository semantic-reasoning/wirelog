/*
 * bench_batch_append.c - public batch append mutation-set microbenchmark
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Measures col_rel_append_rows_atomic() for the #2036 cases: 1x1, 1x256,
 * and 32x256 (columns x rows per call). Relations are private, ungoverned,
 * pre-reserved to 512 rows, and receive disjoint input. Construction,
 * reservation, correctness checks, and output are outside the timed region.
 * Each operation resets only nrows before the call; a matching reset-only
 * control reports that loop's cost separately instead of subtracting it.
 *
 * 32x256-float (#2125) is the 32x256 shape with every column typed FLOAT, so
 * each cell takes the float validation and -0.0 normalization path.  It runs
 * only when named with --case; "all" stays the three #2036 cases.
 *
 * Raw per-sample timings are evidence; this executable does not decide
 * whether a change is an improvement. Pin the process and collect paired
 * base/candidate runs with scripts/perf tooling for comparisons.
 */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "bench_util.h"
#include "../tests/test_perf_util.h"
#include "../wirelog/columnar/internal.h"

#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BENCH_CAPACITY 512u
#define BENCH_MAX_ITERATIONS 100000000u
#define BENCH_MAX_SAMPLES 101u

typedef struct {
    uint32_t ncols;
    uint32_t nrows;
    uint64_t initial_iterations;
    const char *name;
    bool float_columns; /* every column WIRELOG_TYPE_FLOAT */
    bool named_only;    /* excluded from --case all */
} append_case_t;

static const append_case_t cases[] = {
    { 1, 1, UINT64_C(2000000), "1x1" },
    { 1, 256, UINT64_C(10000), "1x256" },
    { 32, 256, UINT64_C(10000), "32x256" },
    { 32, 256, UINT64_C(10000), "32x256-float", true, true },
};

static BENCH_NOINLINE void
reset_nrows(col_rel_t *rel)
{
    rel->nrows = 0;
}

static uint64_t
parse_uint(const char *text)
{
    char *end = NULL;
    errno = 0;
    unsigned long long value = strtoull(text, &end, 10);
    if (errno || end == text || *end != '\0')
        return 0;
    return value;
}

static int
parse_options(int argc, char **argv, const char **which, uint64_t *iterations,
    uint32_t *samples, uint32_t *warmups)
{
    *which = "all";
    *iterations = 0; /* Use the case's calibration starting point. */
    *samples = 9;
    *warmups = 2;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--case") == 0 && i + 1 < argc) {
            *which = argv[++i];
        } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            uint64_t value = parse_uint(argv[++i]);
            if (value == 0)
                return EINVAL;
            *iterations = value;
        } else if (strcmp(argv[i], "--samples") == 0 && i + 1 < argc) {
            uint64_t value = parse_uint(argv[++i]);
            if (value > UINT32_MAX)
                return EINVAL;
            *samples = (uint32_t)value;
        } else if (strcmp(argv[i], "--warmups") == 0 && i + 1 < argc) {
            uint64_t value = parse_uint(argv[++i]);
            if (value > UINT32_MAX)
                return EINVAL;
            *warmups = (uint32_t)value;
        } else {
            return EINVAL;
        }
    }
    if (*iterations > BENCH_MAX_ITERATIONS
        || *samples == 0 || *samples > BENCH_MAX_SAMPLES || *warmups > 20)
        return EINVAL;
    if (strcmp(*which, "all") != 0) {
        bool found = false;
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
            found |= strcmp(*which, cases[i].name) == 0;
        if (!found)
            return EINVAL;
    }
    return 0;
}

static int
prepare_relation(const append_case_t *test_case, col_rel_t **out_rel,
    int64_t **out_rows)
{
    col_rel_t *rel = col_rel_new_auto("bench_batch_append",
            test_case->ncols);
    uint64_t cells = (uint64_t)test_case->ncols * test_case->nrows;
    if (!rel || cells > SIZE_MAX / sizeof(int64_t)) {
        col_rel_destroy(rel);
        return ENOMEM;
    }
    int64_t *rows = malloc((size_t)cells * sizeof(*rows));
    if (!rows) {
        col_rel_destroy(rel);
        return ENOMEM;
    }
    for (uint32_t row = 0; row < test_case->nrows; row++) {
        for (uint32_t col = 0; col < test_case->ncols; col++) {
            int64_t *cell = &rows[(size_t)row * test_case->ncols + col];
            if (test_case->float_columns) {
                /* Finite, non-zero bit patterns; the probe's +17 on the bits
                 * stays finite and non-zero, so stored bits must match. */
                double value = (double)row * 0.5 + (double)col + 1.0;
                memcpy(cell, &value, sizeof(*cell));
            } else {
                *cell = (int64_t)row * INT64_C(1000003) + col;
            }
        }
    }
    if (test_case->float_columns) {
        wirelog_column_type_t *types = malloc(
            (size_t)test_case->ncols * sizeof(*types));
        int type_rc = types ? 0 : ENOMEM;
        for (uint32_t col = 0; types && col < test_case->ncols; col++)
            types[col] = WIRELOG_TYPE_FLOAT;
        if (type_rc == 0)
            type_rc = col_rel_set_column_types(rel, types, test_case->ncols);
        free(types);
        if (type_rc != 0 || !rel->column_types) {
            fprintf(stderr, "setup column types failed: rc=%d\n", type_rc);
            free(rows);
            col_rel_destroy(rel);
            return type_rc ? type_rc : EINVAL;
        }
    }
    bool denied = false;
    int rc = col_rel_reserve_capacity_admitted(rel, BENCH_CAPACITY, &denied);
    if (rc != 0 || denied || rel->capacity < BENCH_CAPACITY) {
        fprintf(stderr, "setup reserve failed: rc=%d denied=%d capacity=%u\n",
            rc, denied, rel->capacity);
        free(rows);
        col_rel_destroy(rel);
        return rc ? rc : ENOMEM;
    }
    *out_rel = rel;
    *out_rows = rows;
    return 0;
}

static int
check_result(const append_case_t *test_case, const col_rel_t *rel,
    const int64_t *rows, uint64_t expected_capacity)
{
    if (rel->nrows != test_case->nrows || rel->capacity != expected_capacity)
        return 1;
    for (uint32_t row = 0; row < test_case->nrows; row++) {
        for (uint32_t col = 0; col < test_case->ncols; col++) {
            int64_t expected = rows[(size_t)row * test_case->ncols + col];
            if (rel->columns[col][row] != expected)
                return 1;
        }
    }
    return 0;
}

static int
run_case(const append_case_t *test_case, uint64_t iterations,
    uint32_t samples, uint32_t warmups)
{
    col_rel_t *rel = NULL;
    int64_t *rows = NULL, *probe_rows = NULL;
    int rc = prepare_relation(test_case, &rel, &rows);
    if (rc != 0)
        return 1;

    size_t cells = (size_t)test_case->ncols * test_case->nrows;
    probe_rows = malloc(cells * sizeof(*probe_rows));
    if (!probe_rows) {
        free(rows);
        col_rel_destroy(rel);
        return 1;
    }
    for (size_t i = 0; i < cells; i++)
        probe_rows[i] = rows[i] + INT64_C(17);

    uint64_t *append_ns = calloc(samples, sizeof(*append_ns));
    uint64_t *reset_ns = calloc(samples, sizeof(*reset_ns));
    if (!append_ns || !reset_ns) {
        free(append_ns);
        free(reset_ns);
        free(probe_rows);
        free(rows);
        col_rel_destroy(rel);
        return 1;
    }

    for (uint32_t sample = 0; sample < warmups + samples; sample++) {
        bool timed = sample >= warmups;
        uint32_t index = timed ? sample - warmups : 0;
        bench_time_t start, end;

        /* Alternate control/append order to reduce within-process drift. */
        bool control_first = ((sample & 1u) == 0);
        for (unsigned pass = 0; pass < 2; pass++) {
            bool is_control = pass == 0 ? control_first : !control_first;
            if (is_control) {
                start = bench_time_now();
                for (uint64_t i = 0; i < iterations; i++)
                    reset_nrows(rel);
                end = bench_time_now();
                if (timed)
                    reset_ns[index] = (uint64_t)(bench_time_diff_ms(start, end)
                        * 1000000.0);
            } else {
                start = bench_time_now();
                for (uint64_t i = 0; i < iterations; i++) {
                    reset_nrows(rel);
                    bool denied = false;
                    rc = col_rel_append_rows_atomic(rel, rows,
                            test_case->nrows, test_case->ncols, &denied);
                    if (rc != 0 || denied) {
                        fprintf(stderr,
                            "append failed: case=%s iteration=%" PRIu64
                            " rc=%d denied=%d\n", test_case->name, i, rc,
                            denied);
                        goto done;
                    }
                }
                end = bench_time_now();
                if (timed)
                    append_ns[index] = (uint64_t)(bench_time_diff_ms(start, end)
                        * 1000000.0);
                if (check_result(test_case, rel, rows, BENCH_CAPACITY) != 0) {
                    fprintf(stderr, "correctness/capacity check failed: %s\n",
                        test_case->name);
                    rc = EINVAL;
                    goto done;
                }
                /* Verify a different tuple outside the timed region so stale
                 * values from the previous timed append cannot mask skipped
                 * copies. The next measured append still starts from nrows=0
                 * and uses the original disjoint input. */
                reset_nrows(rel);
                bool probe_denied = false;
                rc = col_rel_append_rows_atomic(rel, probe_rows,
                        test_case->nrows, test_case->ncols, &probe_denied);
                if (rc != 0 || probe_denied
                    || check_result(test_case, rel, probe_rows,
                    BENCH_CAPACITY) != 0) {
                    fprintf(stderr,
                        "distinct-input correctness probe failed: case=%s"
                        " rc=%d denied=%d\n", test_case->name, rc,
                        probe_denied);
                    rc = EINVAL;
                    goto done;
                }
            }
        }
    }

    uint64_t *ordered = malloc((size_t)samples * sizeof(*ordered));
    if (!ordered) {
        rc = ENOMEM;
        goto done;
    }
    memcpy(ordered, append_ns, (size_t)samples * sizeof(*ordered));
    qsort(ordered, samples, sizeof(*ordered), wl_perf_cmp_ns);
    double mean_append = 0.0, variance_append = 0.0;
    double mean_reset = 0.0;
    for (uint32_t i = 0; i < samples; i++) {
        mean_append += (double)append_ns[i] / (double)iterations;
        mean_reset += (double)reset_ns[i] / (double)iterations;
    }
    mean_append /= samples;
    mean_reset /= samples;
    for (uint32_t i = 0; i < samples; i++) {
        double delta = (double)append_ns[i] / (double)iterations
            - mean_append;
        variance_append += delta * delta;
    }
    variance_append /= samples;
    double median_total = samples & 1u
        ? (double)ordered[samples / 2]
        : ((double)ordered[samples / 2 - 1]
        + (double)ordered[samples / 2]) / 2.0;
    for (uint32_t i = 0; i < samples; i++) {
        printf("sample\tcontract=wirelog.batch-append-benchmark.v2"
            "\tcase=%s\tindex=%u\titerations=%" PRIu64
            "\tappend_total_ns=%" PRIu64 "\treset_total_ns=%" PRIu64
            "\tappend_ns_per_call=%.3f\treset_ns_per_call=%.3f"
            "\tdenied=0\trow_count_check=OK\tcapacity_check=OK"
            "\tvalue_check=OK\tdistinct_input_probe=OK\tstatus=OK\n",
            test_case->name, i, iterations, append_ns[i], reset_ns[i],
            (double)append_ns[i] / (double)iterations,
            (double)reset_ns[i] / (double)iterations);
    }
    printf("summary\tcontract=wirelog.batch-append-benchmark.v2"
        "\tcase=%s\tmedian_append_ns_per_call=%.3f"
        "\tmin_append_ns_per_call=%.3f\tmax_append_ns_per_call=%.3f"
        "\tcov_append_percent=%.3f\tmean_reset_ns_per_call=%.3f\n",
        test_case->name,
        median_total / (double)iterations,
        (double)ordered[0] / (double)iterations,
        (double)ordered[samples - 1] / (double)iterations,
        mean_append > 0.0 ? 100.0 * sqrt(variance_append) / mean_append
            : 0.0,
        mean_reset);
    free(ordered);
    printf("case\tcontract=wirelog.batch-append-benchmark.v2"
        "\tname=%s\tcolumns=%u\trows_per_call=%u\tcapacity=%u"
        "\titerations=%" PRIu64 "\tsamples=%u\twarmups=%u"
        "\tdenied=0\trow_count_check=OK\tcapacity_check=OK"
        "\tvalue_check=OK\tdistinct_input_probe=OK\tstatus=OK\n",
        test_case->name, test_case->ncols, test_case->nrows, BENCH_CAPACITY,
        iterations, samples, warmups);
    rc = 0;

done:
    free(append_ns);
    free(reset_ns);
    free(probe_rows);
    free(rows);
    col_rel_destroy(rel);
    return rc == 0 ? 0 : 1;
}

int
main(int argc, char **argv)
{
    const char *which;
    uint64_t iterations;
    uint32_t samples, warmups;
    int rc = parse_options(argc, argv, &which, &iterations, &samples,
            &warmups);
    if (rc != 0) {
        fprintf(stderr, "Usage: %s [--case all|1x1|1x256|32x256|32x256-float]"
            " [--iterations N] [--samples N] [--warmups N]\n", argv[0]);
        return 2;
    }
    printf("bench_batch_append\tcontract=wirelog.batch-append-benchmark.v2"
        "\treset=nrows-only-before-each-call"
        "\tinput=disjoint\tgovernor=off\treserved_capacity=%u\n",
        BENCH_CAPACITY);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        if (strcmp(which, "all") == 0 ? cases[i].named_only
            : strcmp(which, cases[i].name) != 0)
            continue;
        uint64_t case_iterations = iterations == 0
            ? cases[i].initial_iterations : iterations;
        if (run_case(&cases[i], case_iterations, samples, warmups) != 0)
            return 1;
    }
    return 0;
}
