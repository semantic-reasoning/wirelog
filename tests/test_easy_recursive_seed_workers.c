/*
 * test_easy_recursive_seed_workers.c - Issue #2105 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * A recursive rule head seeded by an inline fact failed to evaluate on more
 * than one worker: the step and the snapshot returned an error and the head
 * stayed empty.  The fact gives the head the width its `.decl` declares,
 * the rows the workers derive carry none, and merging them refused the pair
 * as incompatible.
 *
 * Each case evaluates a chain closure from an inline seed and requires the
 * whole closure, with plain steps, with snapshots alone and with
 * delta-callback steps, on one, two and four workers.
 * WIRELOG_TDD_MIN_ROWS_PER_WORKER=1 makes the plain-step and snapshot runs
 * use their workers on these small inputs.  A delta-callback step evaluates
 * on the serial path whatever the worker count, so those cases only check
 * that the result does not depend on it.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "wirelog/wirelog-easy.h"
#include "wirelog/columnar/internal.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value ? value : "");
}

#  define setenv wl_test_setenv_
#endif

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

wl_session_t *wirelog_easy_testhook_inner(wirelog_easy_session_t *s);

#define CHAIN 40

typedef struct {
    uint32_t count;
    uint32_t seen[CHAIN + 1]; /* times each 0..CHAIN value appeared */
    uint32_t outside;         /* rows outside 0..CHAIN */
} closure_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    closure_t *c = (closure_t *)user_data;

    (void)relation;
    (void)ncols;
    c->count++;
    if (row[0] >= 0 && row[0] <= CHAIN)
        c->seen[row[0]]++;
    else
        c->outside++;
}

static void
ignore_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)diff;
    (void)user_data;
}

typedef enum {
    MODE_PLAIN_STEP,
    MODE_SNAPSHOT_ONLY,
    MODE_CALLBACK_STEP,
} mode_t_;

static const char *const MODE_NAME[] = {
    "plain steps", "snapshots alone", "delta-callback steps"
};

static const char *const PROGRAM =
    ".decl edge(a: int64, b: int64)\n"
    ".decl r(x: int64)\n"
    "r(0).\n"
    "r(y) :- r(x), edge(x, y).\n";

static void
test_float_seed_easy_contract(uint32_t workers)
{
    static const char *src =
        ".decl s(x: float)\n.decl r(x: float)\n"
        "r(0.0).\ns(1.0).\nr(y) :- s(y).\n";
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    closure_t c = { 0 };
    bool ok;

    tests_run++;
    printf("  [%d] float seed evaluates; legacy snapshot refuses (%u workers)",
        tests_run, workers);
    opts.num_workers = workers;
    ok = wirelog_easy_open_opts(src, &opts, &s) == WIRELOG_OK
        && wirelog_easy_step(s) == WIRELOG_OK
        && wirelog_easy_snapshot(s, "r", collect, &c) == WIRELOG_ERR_EXEC
        && c.count == 0;
    if (s)
        wirelog_easy_close(s);
    if (ok) {
        tests_passed++;
        printf(" ... PASS\n");
    } else {
        tests_failed++;
        printf(" ... FAIL\n");
    }
}

static void
test_seeded_closure(mode_t_ mode, uint32_t workers)
{
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    closure_t c;
    const char *why = NULL;

    tests_run++;
    printf("  [%d] inline-seeded closure (%s, %u worker%s)", tests_run,
        MODE_NAME[mode], workers, workers == 1 ? "" : "s");
    opts.num_workers = workers;
    if (wirelog_easy_open_opts(PROGRAM, &opts, &s) != WIRELOG_OK) {
        why = "open";
        goto done;
    }
    if (mode == MODE_CALLBACK_STEP
        && wirelog_easy_set_delta_cb(s, ignore_delta, NULL) != WIRELOG_OK) {
        why = "set_delta_cb";
        goto done;
    }
    for (int64_t i = 0; i < CHAIN; i++) {
        int64_t edge[2] = { i, i + 1 };
        if (wirelog_easy_insert(s, "edge", edge, 2) != WIRELOG_OK) {
            why = "insert edge";
            goto done;
        }
    }
    if (mode != MODE_SNAPSHOT_ONLY && wirelog_easy_step(s) != WIRELOG_OK) {
        why = "step failed";
        goto done;
    }
    memset(&c, 0, sizeof(c));
    if (wirelog_easy_snapshot(s, "r", collect, &c) != WIRELOG_OK) {
        why = "snapshot failed";
        goto done;
    }
    if (c.count != CHAIN + 1 || c.outside != 0) {
        why = "closure has the wrong number of rows";
        goto done;
    }
    for (uint32_t v = 0; v <= CHAIN; v++) {
        if (c.seen[v] != 1) {
            why = "closure row missing or repeated";
            goto done;
        }
    }
done:
    if (s)
        wirelog_easy_close(s);
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s\n", why);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

enum { GRAPH_NODES = 26 };

typedef struct {
    unsigned seen[GRAPH_NODES][GRAPH_NODES];
    unsigned invalid;
} graph_result_t;

static int
graph_index(int64_t value)
{
    if (value >= 0 && value < 24)
        return (int)value;
    return value == 1000 ? 24 : value == 1001 ? 25 : -1;
}

static void
collect_graph(const char *relation, const int64_t *row, uint32_t ncols,
    void *data)
{
    graph_result_t *result = data;
    (void)relation;
    int x = ncols == 2 ? graph_index(row[0]) : -1;
    int y = ncols == 2 ? graph_index(row[1]) : -1;
    if (x < 0 || y < 0)
        result->invalid++;
    else
        result->seen[x][y]++;
}

static bool
graph_parallel_ran(wirelog_easy_session_t *s)
{
    wl_col_session_t *inner = COL_SESSION(wirelog_easy_testhook_inner(s));
    return inner->tdd_audit.strategy
           && strcmp(inner->tdd_audit.strategy, "global_read") == 0
           && inner->tdd_audit.selected_workers >= 2
           && inner->tdd_audit.submitted_tasks >= 2
           && inner->tdd_audit.completed_rounds > 0
           && inner->tdd_workers_count == 0;
}

/* Independent finite-set evaluation of the two-sided rules. The seeds are
 * deliberately unsorted: global-read workers must not sort borrowed heads. */
static void
test_global_seeded_graph(uint32_t workers, bool seeded, bool snapshot_only)
{
    bool edge[GRAPH_NODES][GRAPH_NODES] = { { false } };
    bool expected[GRAPH_NODES][GRAPH_NODES] = { { false } };
    char program[4096];
    size_t used = (size_t)snprintf(program, sizeof(program),
            ".decl edge(x: int32, y: int32)\n"
            ".decl tc(x: int32, y: int32)\n.output tc\n"
            "tc(x,y) :- edge(x,y).\n"
            "tc(x,z) :- tc(x,y), edge(y,z).\n"
            "tc(x,z) :- edge(x,y), tc(y,z).\n%s",
            seeded ? "tc(1001,0).\ntc(1000,1001).\n" : "");
    for (unsigned i = 0; i < 23; i++) {
        edge[i][i + 1] = true;
        used += (size_t)snprintf(program + used, sizeof(program) - used,
                "edge(%u,%u).\n", i, i + 1);
    }
    for (unsigned i = 0; i <= 20; i += 2) {
        edge[i][i + 3] = true;
        used += (size_t)snprintf(program + used, sizeof(program) - used,
                "edge(%u,%u).\n", i, i + 3);
    }
    memcpy(expected, edge, sizeof(expected));
    if (seeded) {
        expected[25][0] = true;
        expected[24][25] = true;
    }
    bool changed;
    do {
        changed = false;
        for (unsigned x = 0; x < GRAPH_NODES; x++)
            for (unsigned y = 0; y < GRAPH_NODES; y++)
                for (unsigned z = 0; z < GRAPH_NODES; z++)
                    if (!expected[x][z]
                        && ((expected[x][y] && edge[y][z])
                        || (edge[x][y] && expected[y][z]))) {
                        expected[x][z] = true;
                        changed = true;
                    }
    } while (changed);

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    wirelog_easy_session_t *s = NULL;
    const char *why = NULL;
    tests_run++;
    printf("  [%d] global-read graph (workers=%u seeded=%d snapshot=%d)",
        tests_run, workers, seeded, snapshot_only);
    opts.num_workers = workers;
    opts.eager_build = true;
    if (wirelog_easy_open_opts(program, &opts, &s) != WIRELOG_OK) {
        why = "open";
        goto done;
    }
    /* Plain step does not enable the optional audit from the environment;
     * snapshot does. Observe dispatch immediately after each entry point. */
    COL_SESSION(wirelog_easy_testhook_inner(s))->tdd_audit.enabled = true;
    if (!snapshot_only && wirelog_easy_step(s) != WIRELOG_OK) {
        why = "step";
        goto done;
    }
    if (!snapshot_only && workers > 1 && !graph_parallel_ran(s)) {
        why = "parallel global-read step or cleanup";
        goto done;
    }
    for (unsigned repeat = 0; repeat < 2; repeat++) {
        graph_result_t result = { 0 };
        if (wirelog_easy_snapshot(s, "tc", collect_graph, &result)
            != WIRELOG_OK || result.invalid) {
            why = "snapshot";
            goto done;
        }
        unsigned count = 0;
        for (unsigned x = 0; x < GRAPH_NODES; x++)
            for (unsigned y = 0; y < GRAPH_NODES; y++) {
                count += result.seen[x][y];
                if (result.seen[x][y] != (unsigned)expected[x][y]) {
                    why = "missing, extra or duplicate tuple";
                    goto done;
                }
            }
        if (count != (seeded ? 301u : 276u)) {
            why = "oracle cardinality";
            goto done;
        }
        if (repeat == 0 && snapshot_only && workers > 1
            && !graph_parallel_ran(s)) {
            why = "parallel global-read snapshot or cleanup";
            goto done;
        }
    }
done:
    if (s)
        wirelog_easy_close(s);
    if (why) {
        tests_failed++;
        printf(" ... FAIL: %s\n", why);
    } else {
        tests_passed++;
        printf(" ... PASS\n");
    }
}

int
main(void)
{
    static const uint32_t workers[] = { 1, 2, 4 };

    printf("test_easy_recursive_seed_workers (Issues #2105, #2109)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++)
        for (int m = MODE_PLAIN_STEP; m <= MODE_CALLBACK_STEP; m++)
            test_seeded_closure((mode_t_)m, workers[w]);
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++)
        test_float_seed_easy_contract(workers[w]);
    if (setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1) != 0)
        return 1;
    const uint32_t graph_workers[] = { 1, 2, 8 };
    for (unsigned w = 0; w < 3; w++)
        for (unsigned seeded = 0; seeded < 2; seeded++)
            for (unsigned snapshot = 0; snapshot < 2; snapshot++)
                test_global_seeded_graph(graph_workers[w], seeded, snapshot);
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
