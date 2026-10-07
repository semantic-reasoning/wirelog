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

int
main(void)
{
    static const uint32_t workers[] = { 1, 2, 4 };

    printf("test_easy_recursive_seed_workers (Issue #2105)\n");
    if (setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1) != 0) {
        printf("setenv failed\n");
        return 1;
    }
    for (uint32_t w = 0; w < sizeof(workers) / sizeof(workers[0]); w++)
        for (int m = MODE_PLAIN_STEP; m <= MODE_CALLBACK_STEP; m++)
            test_seeded_closure((mode_t_)m, workers[w]);
    printf("%d run, %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed ? 1 : 0;
}
