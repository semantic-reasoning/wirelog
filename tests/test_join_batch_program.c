/*
 * test_join_batch_program.c - bounded JOIN through the public session API
 * in both planner configurations (Issue #1382)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Built twice by tests/meson.build: once with the default planner and once
 * with -DENABLE_K_FUSION=0, with the library sources compiled into the
 * test.  Each case sets WIRELOG_JOIN_BATCH_BYTES (and, for the strict
 * cases, WIRELOG_JOIN_BATCH_STRICT) before creating its session, runs a
 * program through wl_session_*, and checks the snapshot against an oracle
 * the test computes from its own edge list.  The fallback count and last
 * reason are read after the snapshot returns, when every worker of that
 * snapshot has been destroyed and has folded its records into the session.
 *
 * Strict mode refuses every recorded fallback, so in these programs a
 * strict snapshot that succeeds ran no excluded shape: each of its joins
 * was bounded or had nothing to bound.
 */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
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
    return _putenv_s(name, (value && *value) ? value : "1");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#define setenv wl_test_setenv_
#define unsetenv wl_test_unsetenv_
#endif

static int tests_run;
static int tests_failed;

/* K-Fusion branch sessions cloned, through the test hook (default build).
 * When check_no_clones is set, run_case fails a case that cloned any. */
static unsigned kfusion_clones;
static bool check_no_clones;

static void
count_kfusion_clone(wl_col_session_t *parent, wl_col_session_t *child,
    bool before)
{
    (void)parent;
    (void)child;
    if (before)
        kfusion_clones++;
}

/* ---- the input and its oracles ------------------------------------------ */

/* edge(i, i + 1) for i < CHAIN, edge(0, 1000 + j) for j < FAN, and
 * edge(HUB, 0): every row reaching 0 meets FAN + 1 successors, so a join
 * on that key spans many 7-row batches.  INSERTED is the incremental row. */
#define CHAIN 40u
#define FAN 300u
#define HUB 500
#define MAX_EDGES (CHAIN + FAN + 2u)

typedef struct {
    int64_t from[MAX_EDGES];
    int64_t to[MAX_EDGES];
    uint32_t n;
} edges_t;

static void
edges_init(edges_t *e, bool with_inserted)
{
    e->n = 0;
    for (uint32_t i = 0; i < CHAIN; i++) {
        e->from[e->n] = i;
        e->to[e->n++] = (int64_t)i + 1;
    }
    for (uint32_t j = 0; j < FAN; j++) {
        e->from[e->n] = 0;
        e->to[e->n++] = 1000 + (int64_t)j;
    }
    e->from[e->n] = HUB;
    e->to[e->n++] = 0;
    if (with_inserted) {
        e->from[e->n] = CHAIN;
        e->to[e->n++] = (int64_t)CHAIN + 1;
    }
}

#define MAX_ROWS 8192u

typedef struct {
    int64_t row[MAX_ROWS][2];
    uint32_t count;
    bool overflow;
} pairs_t;

static void
pairs_add(pairs_t *p, int64_t a, int64_t b)
{
    for (uint32_t i = 0; i < p->count; i++)
        if (p->row[i][0] == a && p->row[i][1] == b)
            return;
    if (p->count >= MAX_ROWS) {
        p->overflow = true;
        return;
    }
    p->row[p->count][0] = a;
    p->row[p->count][1] = b;
    p->count++;
}

/* hop(x, z) :- edge(x, y), edge(y, z), as a set. */
static void
oracle_two_hop(const edges_t *e, pairs_t *out)
{
    memset(out, 0, sizeof(*out));
    for (uint32_t i = 0; i < e->n; i++)
        for (uint32_t j = 0; j < e->n; j++)
            if (e->to[i] == e->from[j])
                pairs_add(out, e->from[i], e->to[j]);
}

/* The transitive closure of edge, by repeated two-hop extension. */
static void
oracle_closure(const edges_t *e, pairs_t *out)
{
    bool grew = true;

    memset(out, 0, sizeof(*out));
    for (uint32_t i = 0; i < e->n; i++)
        pairs_add(out, e->from[i], e->to[i]);
    while (grew && !out->overflow) {
        uint32_t before = out->count;
        for (uint32_t p = 0; p < before; p++)
            for (uint32_t i = 0; i < e->n; i++)
                if (out->row[p][1] == e->from[i])
                    pairs_add(out, out->row[p][0], e->to[i]);
        grew = out->count != before;
    }
}

/* r(x, y) :- edge(x, y).  r(a, f) :- r(a, b), r(b, c), r(c, d), r(d, f).
 * r holds exactly the pairs joined by an edge path whose length is 1 mod 3
 * (1, 4, 7, ...), found here by a walk that tracks length mod 3.  The edge
 * graph is acyclic, so the walk ends. */
static void
oracle_four_way(const edges_t *e, pairs_t *out)
{
    /* reach[i][m]: pairs (a, b) with a path of positive length = m mod 3 */
    static pairs_t reach[3];
    bool grew = true;

    memset(out, 0, sizeof(*out));
    for (int m = 0; m < 3; m++)
        memset(&reach[m], 0, sizeof(reach[m]));
    for (uint32_t i = 0; i < e->n; i++)
        pairs_add(&reach[1], e->from[i], e->to[i]);
    while (grew) {
        grew = false;
        for (int m = 0; m < 3; m++) {
            uint32_t before = reach[m].count;
            for (uint32_t p = 0; p < before; p++)
                for (uint32_t i = 0; i < e->n; i++) {
                    pairs_t *dst = &reach[(m + 1) % 3];
                    uint32_t had = dst->count;
                    if (reach[m].row[p][1] != e->from[i])
                        continue;
                    pairs_add(dst, reach[m].row[p][0], e->to[i]);
                    grew = grew || dst->count != had;
                }
        }
    }
    for (uint32_t p = 0; p < reach[1].count; p++)
        pairs_add(out, reach[1].row[p][0], reach[1].row[p][1]);
    out->overflow = reach[0].overflow || reach[1].overflow
        || reach[2].overflow;
}

/* ---- running a program ------------------------------------------------- */

typedef struct {
    const char *relation;
    pairs_t rows; /* the snapshot's rows, duplicates kept apart */
    uint32_t duplicates;
} collected_t;

static void
collect(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collected_t *c = (collected_t *)user;
    uint32_t before;

    if (strcmp(relation, c->relation) != 0 || ncols != 2)
        return;
    before = c->rows.count;
    pairs_add(&c->rows, row[0], row[1]);
    if (c->rows.count == before && !c->rows.overflow)
        c->duplicates++;
}

static bool
same_set(const pairs_t *got, uint32_t duplicates, const pairs_t *want)
{
    if (got->overflow || want->overflow || duplicates
        || got->count != want->count)
        return false;
    for (uint32_t i = 0; i < want->count; i++) {
        bool found = false;
        for (uint32_t j = 0; j < got->count && !found; j++)
            found = got->row[j][0] == want->row[i][0]
                && got->row[j][1] == want->row[i][1];
        if (!found)
            return false;
    }
    return true;
}

typedef struct {
    int rc;
    uint32_t fallbacks;
    uint8_t reason;
    uint32_t routes; /* insert routes taken by this snapshot */
    collected_t got;
} outcome_t;

typedef enum { MODE_RECORD, MODE_STRICT } knob_mode_t;

static void
set_mode(knob_mode_t mode)
{
    setenv("WIRELOG_JOIN_BATCH_BYTES", "224", 1);
    if (mode == MODE_STRICT)
        setenv("WIRELOG_JOIN_BATCH_STRICT", "1", 1);
    else
        unsetenv("WIRELOG_JOIN_BATCH_STRICT");
}

static void
clear_env(void)
{
    unsetenv("WIRELOG_JOIN_BATCH_STRICT");
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");
}

static int
insert_edges(wl_session_t *s, const edges_t *e)
{
    for (uint32_t i = 0; i < e->n; i++) {
        int64_t row[2] = { e->from[i], e->to[i] };
        if (wl_session_insert(s, "edge", row, 1, 2) != 0)
            return -1;
    }
    return 0;
}

static wl_session_t *
open_session(const char *src, uint32_t workers, wl_plan_t **plan,
    wirelog_program_t **prog)
{
    wirelog_error_t err;
    wl_session_t *s = NULL;

    *plan = NULL;
    *prog = wirelog_parse_string(src, &err);
    if (!*prog || wl_plan_from_program(*prog, plan) != 0)
        return NULL;
    if (wl_session_create(wl_backend_columnar(), *plan, workers, &s) != 0)
        return NULL;
    if (wl_session_load_facts(s, *prog) != 0) {
        wl_session_destroy(s);
        return NULL;
    }
    return s;
}

static void
close_session(wl_session_t *s, wl_plan_t *plan, wirelog_program_t *prog)
{
    if (s)
        wl_session_destroy(s);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
}

static void
snapshot(wl_session_t *s, const char *relation, outcome_t *out)
{
    uint32_t routes = wl_columnar_session_test_insert_route_taken;

    memset(out, 0, sizeof(*out));
    out->got.relation = relation;
    COL_SESSION(s)->join_batch_fallback_count = 0;
    COL_SESSION(s)->join_batch_last_reason = 0;
    out->rc = wl_session_snapshot(s, collect, &out->got);
    out->fallbacks = COL_SESSION(s)->join_batch_fallback_count;
    out->reason = COL_SESSION(s)->join_batch_last_reason;
    out->routes = wl_columnar_session_test_insert_route_taken - routes;
}

/* What one snapshot must show: its rc, whether its rows must equal the
 * oracle, the last recorded reason (ELIGIBLE: nothing recorded;
 * REASON_COUNT: not checked), and whether it took the insert route. */
typedef struct {
    int rc;
    bool exact;
    col_join_batch_eligibility_t reason;
    bool route;
} expect_t;

static bool
check(const char *label, const outcome_t *o, const expect_t *want,
    const pairs_t *oracle)
{
    bool ok = o->rc == want->rc
        && (!want->exact || same_set(&o->got.rows, o->got.duplicates, oracle))
        && (want->reason == COL_JOIN_BATCH_REASON_COUNT
        || (want->reason == COL_JOIN_BATCH_ELIGIBLE
        ? o->fallbacks == 0
        : (o->fallbacks > 0 && o->reason == (uint8_t)want->reason)))
        && (o->routes != 0) == want->route;

    if (!ok)
        printf("\n    %s: rc=%d rows=%u dup=%u fallbacks=%u reason=%s "
            "route=%u", label, o->rc, o->got.rows.count, o->got.duplicates,
            o->fallbacks, col_join_batch_eligibility_name(
                (col_join_batch_eligibility_t)o->reason), o->routes);
    return ok;
}

/* ---- programs ------------------------------------------------------------ */

static const char *TWO_HOP =
    ".decl edge(x: int64, y: int64)\n"
    ".decl hop(x: int64, z: int64)\n"
    "hop(x, z) :- edge(x, y), edge(y, z).\n";

static const char *CLOSURE =
    ".decl edge(x: int64, y: int64)\n"
    ".decl path(x: int64, y: int64)\n"
    "path(x, y) :- edge(x, y).\n"
    "path(x, z) :- path(x, y), edge(y, z).\n";

/* A K = 4 recursive rule, which the default planner fuses. */
static const char *FOUR_WAY =
    ".decl edge(x: int64, y: int64)\n"
    ".decl r(x: int64, y: int64)\n"
    "r(x, y) :- edge(x, y).\n"
    "r(a, f) :- r(a, b), r(b, c), r(c, d), r(d, f).\n";

/* Run @src: load the edges, snapshot, insert the incremental row, snapshot
 * again, and check both snapshots.  @oracle_fn computes the expected rows
 * of @relation for the edges each snapshot sees. */
static void
run_case(const char *name, const char *src, const char *relation,
    void (*oracle_fn)(const edges_t *, pairs_t *), uint32_t workers,
    knob_mode_t mode, const expect_t *first, const expect_t *second)
{
    static pairs_t oracle;
    static outcome_t o;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_session_t *s;
    edges_t e;
    bool ok;

    tests_run++;
    printf("  [%2d] %s, W=%u, %s", tests_run, name, workers,
        mode == MODE_STRICT ? "strict" : "recorded");
    set_mode(mode);
    s = open_session(src, workers, &plan, &prog);
    edges_init(&e, false);
    ok = s && insert_edges(s, &e) == 0;
    if (ok) {
        oracle_fn(&e, &oracle);
        snapshot(s, relation, &o);
        ok = check("first snapshot", &o, first, &oracle);
    }
    if (ok && second) {
        int64_t row[2] = { (int64_t)CHAIN, (int64_t)CHAIN + 1 };
        edges_init(&e, true);
        oracle_fn(&e, &oracle);
        ok = col_session_insert_incremental(s, "edge", row, 1, 2) == 0;
        if (ok) {
            snapshot(s, relation, &o);
            ok = check("snapshot after insert", &o, second, &oracle);
        }
    }
    close_session(s, plan, prog);
    unsetenv("WIRELOG_JOIN_BATCH_STRICT");
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    if (ok && check_no_clones && kfusion_clones != 0) {
        printf("\n    cloned %u K-Fusion branch sessions", kfusion_clones);
        ok = false;
    }
    if (ok) {
        printf(" ... PASS\n");
    } else {
        printf(" ... FAIL\n");
        tests_failed++;
    }
}

/* After a snapshot fails with EOVERFLOW under the legacy row cap, past at
 * least one committed batch of the hub key's 301 matches, the session is
 * reusable: with the cap cleared the next snapshot is exact. */
static void
run_reuse_after_overflow(void)
{
    static pairs_t oracle;
    static outcome_t o;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_session_t *s;
    edges_t e;
    bool ok;

    tests_run++;
    printf("  [%2d] two-hop, W=1, reuse after a capped snapshot", tests_run);
    set_mode(MODE_STRICT);
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "100", 1);
    s = open_session(TWO_HOP, 1, &plan, &prog);
    unsetenv("WIRELOG_JOIN_OUTPUT_LIMIT");
    edges_init(&e, false);
    ok = s && insert_edges(s, &e) == 0;
    if (ok) {
        COL_SESSION(s)->join_output_peak = 0;
        snapshot(s, "hop", &o);
        /* The cap is checked after each committed batch against the join's
         * own output, so the overflow came after committed rows reached
         * the cap. */
        ok = o.rc == EOVERFLOW && COL_SESSION(s)->join_output_peak >= 100u;
        if (!ok)
            printf("\n    capped snapshot rc=%d peak=%llu", o.rc,
                (unsigned long long)COL_SESSION(s)->join_output_peak);
    }
    if (ok) {
        const expect_t exact = { 0, true, COL_JOIN_BATCH_ELIGIBLE, false };
        COL_SESSION(s)->join_output_limit = 0;
        oracle_two_hop(&e, &oracle);
        snapshot(s, "hop", &o);
        ok = check("snapshot after clearing the cap", &o, &exact, &oracle);
    }
    close_session(s, plan, prog);
    clear_env();
    if (ok) {
        printf(" ... PASS\n");
    } else {
        printf(" ... FAIL\n");
        tests_failed++;
    }
}

int
main(void)
{
    /* Nothing recorded, rows exact; with or without the insert route. */
    const expect_t bounded = { 0, true, COL_JOIN_BATCH_ELIGIBLE, false };
    const expect_t routed = {
        0, true, COL_JOIN_BATCH_EXCLUDED_INSERT_ROUTE, true
    };
    const expect_t in_workers = {
        0, true, COL_JOIN_BATCH_EXCLUDED_WORKER, false
    };
    const expect_t refused = { ENOTSUP, false, COL_JOIN_BATCH_ELIGIBLE, false };
    const expect_t exact_only = {
        0, true, COL_JOIN_BATCH_REASON_COUNT, false
    };
    const uint32_t workers[] = { 1, 4 };

#if defined(ENABLE_K_FUSION) && !ENABLE_K_FUSION
    printf("test_join_batch_program (nofusion)\n");
#else
    printf("test_join_batch_program (default)\n");
#endif
    clear_env();

    /* Coordinator routes: the first snapshot evaluates on the calling
     * session (these strata are below the parallel thresholds), so it runs
     * bounded and strict mode succeeds.  The snapshot after an incremental
     * insert takes the insert route and records insert-route; in strict
     * mode the route is refused and the full re-evaluation is bounded. */
    for (uint32_t w = 0; w < 2; w++) {
        run_case("two-hop", TWO_HOP, "hop", oracle_two_hop, workers[w],
            MODE_RECORD, &bounded, &routed);
        run_case("two-hop", TWO_HOP, "hop", oracle_two_hop, workers[w],
            MODE_STRICT, &bounded, &bounded);
        run_case("closure", CLOSURE, "path", oracle_closure, workers[w],
            MODE_RECORD, &bounded, &routed);
        run_case("closure", CLOSURE, "path", oracle_closure, workers[w],
            MODE_STRICT, &bounded, &bounded);
        /* The four-way rule records several fallbacks (a JOIN feeding
         * another JOIN is pipeline-not-consumable, and its semi-naive
         * rounds read right deltas), so strict mode would refuse it and
         * only its rows are checked.  The default planner fuses the rule;
         * with the knob set no K-Fusion branch session is cloned at any
         * worker count (#1382). */
        kfusion_clones = 0;
        check_no_clones = true;
        wl_columnar_kfusion_test_clone = count_kfusion_clone;
        run_case("four-way rule", FOUR_WAY, "r", oracle_four_way, workers[w],
            MODE_RECORD, &exact_only, NULL);
        wl_columnar_kfusion_test_clone = NULL;
        check_no_clones = false;
    }

    /* Worker routes at W=4: a recursive stratum on TDD workers, and a
     * non-recursive stratum split across workers.  Joins inside a worker
     * are one-shot and recorded as worker-session; strict mode refuses. */
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    run_case("closure on TDD workers", CLOSURE, "path", oracle_closure, 4,
        MODE_RECORD, &in_workers, NULL);
    run_case("closure on TDD workers", CLOSURE, "path", oracle_closure, 4,
        MODE_STRICT, &refused, NULL);
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    setenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    run_case("two-hop on parallel workers", TWO_HOP, "hop", oracle_two_hop, 4,
        MODE_RECORD, &in_workers, NULL);
    run_case("two-hop on parallel workers", TWO_HOP, "hop", oracle_two_hop, 4,
        MODE_STRICT, &refused, NULL);
    unsetenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");

    run_reuse_after_overflow();

    printf("%d run, %d failed\n", tests_run, tests_failed);
    return tests_failed ? 1 : 0;
}
