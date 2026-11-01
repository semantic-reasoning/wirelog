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
 * reason, and the bounded runs completed per route by the session and by
 * its workers (#2201), are read after the snapshot returns, when every
 * worker of that snapshot has been destroyed and has folded its records
 * into the session.
 *
 * A snapshot after an incremental insert takes the insert route (#2114),
 * whose worker clones keep the knob since #2201 and run their JOINs on the
 * bounded producer.
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

/* The most tasks one insert-route round ran in parallel, and the strata
 * whose heads the insert route merged, through the test hooks (#2201). */
static uint32_t insert_max_tasks;
static uint32_t insert_merges;

static void
record_insert_round(const wl_plan_stratum_t *sp, uint32_t units,
    uint32_t pieces, uint32_t tasks)
{
    (void)sp;
    (void)units;
    (void)pieces;
    if (tasks > insert_max_tasks)
        insert_max_tasks = tasks;
}

static int
count_insert_merge(const wl_plan_stratum_t *sp)
{
    (void)sp;
    insert_merges++;
    return 0;
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

/* hop(x, z) :- edge(x, y), edge(y, z), z != 7, as a set. */
static void
oracle_two_hop_filtered(const edges_t *e, pairs_t *out)
{
    memset(out, 0, sizeof(*out));
    for (uint32_t i = 0; i < e->n; i++)
        for (uint32_t j = 0; j < e->n; j++)
            if (e->to[i] == e->from[j] && e->to[j] != 7)
                pairs_add(out, e->from[i], e->to[j]);
}

/* quad(x, w) :- hop(x, y), edge(y, z), edge(z, w), with hop the two-hop
 * relation: the pairs joined by an edge path of length 4, as a set. */
static void
oracle_four_hop(const edges_t *e, pairs_t *out)
{
    static pairs_t hop, three;

    oracle_two_hop(e, &hop);
    memset(&three, 0, sizeof(three));
    for (uint32_t p = 0; p < hop.count; p++)
        for (uint32_t i = 0; i < e->n; i++)
            if (hop.row[p][1] == e->from[i])
                pairs_add(&three, hop.row[p][0], e->to[i]);
    memset(out, 0, sizeof(*out));
    for (uint32_t p = 0; p < three.count; p++)
        for (uint32_t i = 0; i < e->n; i++)
            if (three.row[p][1] == e->from[i])
                pairs_add(out, three.row[p][0], e->to[i]);
    out->overflow = hop.overflow || three.overflow;
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
    uint16_t warned; /* reasons warned about, folded from workers too */
    /* bounded producer runs by route: the session's own, and its workers' */
    uint32_t runs[COL_JOIN_BATCH_RUN_ROUTES];
    uint32_t worker_runs[COL_JOIN_BATCH_RUN_ROUTES];
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
    COL_SESSION(s)->join_batch_warned_reasons = 0;
    memset(COL_SESSION(s)->join_batch_runs, 0,
        sizeof(COL_SESSION(s)->join_batch_runs));
    memset(COL_SESSION(s)->join_batch_worker_runs, 0,
        sizeof(COL_SESSION(s)->join_batch_worker_runs));
    out->rc = wl_session_snapshot(s, collect, &out->got);
    out->fallbacks = COL_SESSION(s)->join_batch_fallback_count;
    out->reason = COL_SESSION(s)->join_batch_last_reason;
    out->warned = COL_SESSION(s)->join_batch_warned_reasons;
    memcpy(out->runs, COL_SESSION(s)->join_batch_runs, sizeof(out->runs));
    memcpy(out->worker_runs, COL_SESSION(s)->join_batch_worker_runs,
        sizeof(out->worker_runs));

    out->routes = wl_columnar_session_test_insert_route_taken - routes;
}

/* The warned-reasons bit of worker-session, which a worker's teardown
 * folds into the session whatever reason it recorded last. */
#define WARNED_WORKER ((uint16_t)(1u << COL_JOIN_BATCH_EXCLUDED_WORKER))

/* Route bits for expect_t's run masks (#2201). */
#define RUN_ORD (1u << COL_JOIN_BATCH_RUN_ORDINARY)
#define RUN_DIFF (1u << COL_JOIN_BATCH_RUN_DIFFERENTIAL)
#define RUN_PIPE (1u << COL_JOIN_BATCH_RUN_PIPELINE)

/* What one snapshot must show: its rc, whether its rows must equal the
 * oracle, the last recorded reason (ELIGIBLE: nothing recorded;
 * REASON_COUNT: not checked), whether it took the insert route, and
 * exactly the routes on which the session itself, and its workers,
 * completed a bounded run: each listed route nonzero, each other zero.
 * When workers must have completed runs, no worker may have recorded
 * worker-session either. */
typedef struct {
    int rc;
    bool exact;
    col_join_batch_eligibility_t reason;
    bool route;
    unsigned own;
    unsigned workers;
} expect_t;

/* True when exactly the routes in @mask have a nonzero count in @runs. */
static bool
runs_match(const uint32_t *runs, unsigned mask)
{
    for (unsigned r = 0; r < COL_JOIN_BATCH_RUN_ROUTES; r++)
        if (((mask & (1u << r)) != 0) != (runs[r] != 0))
            return false;
    return true;
}

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
        && (o->routes != 0) == want->route
        && runs_match(o->runs, want->own)
        && runs_match(o->worker_runs, want->workers)
        && (want->workers == 0 || (o->warned & WARNED_WORKER) == 0);

    if (!ok)
        printf("\n    %s: rc=%d rows=%u dup=%u fallbacks=%u reason=%s "
            "route=%u runs=%u/%u/%u worker runs=%u/%u/%u warned=%#x",
            label, o->rc, o->got.rows.count, o->got.duplicates,
            o->fallbacks, col_join_batch_eligibility_name(
                (col_join_batch_eligibility_t)o->reason), o->routes,
            o->runs[0], o->runs[1], o->runs[2], o->worker_runs[0],
            o->worker_runs[1], o->worker_runs[2], (unsigned)o->warned);
    return ok;
}

/* ---- programs ------------------------------------------------------------ */

static const char *TWO_HOP =
    ".decl edge(x: int64, y: int64)\n"
    ".decl hop(x: int64, z: int64)\n"
    "hop(x, z) :- edge(x, y), edge(y, z).\n";

/* The two-hop rule with a row-local FILTER after the JOIN, which the fused
 * JOIN pipeline consumes. */
static const char *TWO_HOP_FILTERED =
    ".decl edge(x: int64, y: int64)\n"
    ".decl hop(x: int64, z: int64)\n"
    "hop(x, z) :- edge(x, y), edge(y, z), z != 7.\n";

static const char *CLOSURE =
    ".decl edge(x: int64, y: int64)\n"
    ".decl path(x: int64, y: int64)\n"
    "path(x, y) :- edge(x, y).\n"
    "path(x, z) :- path(x, y), edge(y, z).\n";

/* The closure as a self-join of the recursive relation, which TDD evaluates
 * with the bdx strategy where the linear closure takes owner. */
static const char *CLOSURE_SELF_JOIN =
    ".decl edge(x: int64, y: int64)\n"
    ".decl path(x: int64, y: int64)\n"
    "path(x, y) :- edge(x, y).\n"
    "path(x, z) :- path(x, y), path(y, z).\n";

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
            insert_max_tasks = 0;
            snapshot(s, relation, &o);
            ok = check("snapshot after insert", &o, second, &oracle);
            /* At four workers the insert route's clones ran concurrently,
             * so TSan sees their producers side by side (#2201). */
            if (ok && second->route && workers > 1 && insert_max_tasks < 2) {
                printf("\n    insert route ran %u task(s)", insert_max_tasks);
                ok = false;
            }
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
 * reusable: with the cap cleared the next snapshot is exact.  With
 * @on_workers the stratum is split across four non-recursive workers,
 * which share one output counter (#2201); when they overflow it, the
 * parallel evaluation returns EAGAIN and the stratum is evaluated again on
 * the calling session, which overflows the cap in turn. */
static void
run_reuse_after_overflow(bool on_workers)
{
    static pairs_t oracle;
    static outcome_t o;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_session_t *s;
    edges_t e;
    bool ok;

    tests_run++;
    printf("  [%2d] two-hop%s, W=%u, reuse after a capped snapshot",
        tests_run, on_workers ? " on parallel workers" : "",
        on_workers ? 4u : 1u);
    set_mode(MODE_STRICT);
    setenv("WIRELOG_JOIN_OUTPUT_LIMIT", "100", 1);
    if (on_workers)
        setenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    s = open_session(TWO_HOP, on_workers ? 4u : 1u, &plan, &prog);
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
        const expect_t exact = {
            0, true, COL_JOIN_BATCH_ELIGIBLE, false,
            on_workers ? 0u : RUN_ORD, on_workers ? RUN_ORD : 0u
        };
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

/* A program whose insert route reaches two strata: hop, a JOIN followed by
 * a MAP, and quad, whose first JOIN feeds a second, which strict mode
 * refuses as pipeline-not-consumable. */
static const char *TWO_STRATA =
    ".decl edge(x: int64, y: int64)\n"
    ".decl hop(x: int64, z: int64)\n"
    ".decl quad(x: int64, w: int64)\n"
    "hop(x, z) :- edge(x, y), edge(y, z).\n"
    "quad(x, w) :- hop(x, y), edge(y, z), edge(z, w).\n";

/* #2201: the insert route's clones keep the knob, so a JOIN they run can
 * fail after an earlier stratum's heads were merged.  The first snapshot
 * runs without strict mode; with strict mode set before the insert, the
 * route merges hop's heads, and quad's clone refuses its first JOIN with
 * ENOTSUP.  The snapshot then evaluates in full, which strict mode refuses
 * as well, so it returns ENOTSUP with the full evaluation still pending.
 * With strict mode cleared the next snapshot, a full evaluation on the
 * calling session, is exact and records quad's first JOIN as
 * pipeline-not-consumable. */
static void
run_strict_after_merge(void)
{
    static pairs_t oracle;
    static outcome_t o;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_session_t *s;
    edges_t e;
    bool ok;
    const expect_t exact = {
        0, true, COL_JOIN_BATCH_EXCLUDED_PIPELINE, false, RUN_ORD, 0
    };

    tests_run++;
    printf("  [%2d] strict insert route fails after a merged stratum",
        tests_run);
    set_mode(MODE_RECORD);
    s = open_session(TWO_STRATA, 1, &plan, &prog);
    edges_init(&e, false);
    ok = s && insert_edges(s, &e) == 0;
    if (ok) {
        oracle_four_hop(&e, &oracle);
        snapshot(s, "quad", &o);
        ok = o.rc == 0 && same_set(&o.got.rows, o.got.duplicates, &oracle);
        if (!ok)
            printf("\n    first snapshot rc=%d rows=%u", o.rc,
                o.got.rows.count);
    }
    if (ok) {
        int64_t row[2] = { (int64_t)CHAIN, (int64_t)CHAIN + 1 };
        COL_SESSION(s)->join_batch_strict = true;
        ok = col_session_insert_incremental(s, "edge", row, 1, 2) == 0;
    }
    if (ok) {
        insert_merges = 0;
        wl_columnar_eval_tdd_insert_test_after_merge = count_insert_merge;
        snapshot(s, "quad", &o);
        wl_columnar_eval_tdd_insert_test_after_merge = NULL;
        ok = o.rc == ENOTSUP && insert_merges >= 1
            && COL_SESSION(s)->pending_full_input_eval;
        if (!ok)
            printf("\n    strict snapshot rc=%d merges=%u pending=%d", o.rc,
                insert_merges, (int)COL_SESSION(s)->pending_full_input_eval);
    }
    if (ok) {
        edges_init(&e, true);
        oracle_four_hop(&e, &oracle);
        COL_SESSION(s)->join_batch_strict = false;
        snapshot(s, "quad", &o);
        ok = check("snapshot without strict mode", &o, &exact, &oracle);
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

/* #2201: a clone of the insert route that overflows the legacy row cap
 * fails the snapshot with EOVERFLOW, which the route returns directly;
 * with the cap cleared the next snapshot, a full evaluation, is exact.
 * The cap is set after the first snapshot, whose own hub join would
 * overflow it, and the inserted edge (HUB + 1, 0) reaches the hub key's
 * FAN + 1 successors.  At @workers clones each clone gets its share of
 * the cap. */
static void
run_insert_overflow_reuse(uint32_t workers)
{
    static pairs_t oracle;
    static outcome_t o;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_session_t *s;
    edges_t e;
    bool ok;
    const expect_t exact = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, RUN_ORD, 0
    };

    tests_run++;
    printf("  [%2d] two-hop, W=%u, reuse after a capped insert route",
        tests_run, workers);
    set_mode(MODE_RECORD);
    if (workers > 1)
        setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    s = open_session(TWO_HOP, workers, &plan, &prog);
    edges_init(&e, false);
    ok = s && insert_edges(s, &e) == 0;
    if (ok) {
        snapshot(s, "hop", &o);
        ok = o.rc == 0;
    }
    if (ok) {
        int64_t row[2] = { (int64_t)HUB + 1, 0 };
        COL_SESSION(s)->join_output_limit = 100;
        ok = col_session_insert_incremental(s, "edge", row, 1, 2) == 0;
    }
    if (ok) {
        snapshot(s, "hop", &o);
        ok = o.rc == EOVERFLOW && o.routes == 0
            && COL_SESSION(s)->pending_full_input_eval;
        if (!ok)
            printf("\n    capped snapshot rc=%d routes=%u", o.rc, o.routes);
    }
    if (ok) {
        e.from[e.n] = HUB + 1;
        e.to[e.n++] = 0;
        oracle_two_hop(&e, &oracle);
        COL_SESSION(s)->join_output_limit = 0;
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
    /* Nothing recorded, rows exact, and the calling session completed a
     * bounded run: on the ordinary route for the two-hop rule, on the
     * pipeline route for the filtered two-hop rule, and on the
     * differential route for the recursive closure.  After an incremental
     * insert, the insert route with nothing recorded and its clones'
     * bounded runs on the pipeline route (#2201): in the bound slices of
     * these programs each JOIN is followed by a MAP. */
    const expect_t two_hop_bounded = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, RUN_ORD, 0
    };
    const expect_t filtered_bounded = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, RUN_PIPE, 0
    };
    const expect_t closure_bounded = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, RUN_DIFF, 0
    };
    const expect_t insert_pipe = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, true, 0, RUN_PIPE
    };
    const expect_t four_way_rows = {
        0, true, COL_JOIN_BATCH_REASON_COUNT, false, RUN_DIFF, 0
    };
    /* On workers (#2201): nothing recorded and rows exact, and the workers
     * completed bounded runs -- differential runs on TDD workers (from
     * their second iteration on, a recursive worker evaluates its JOIN
     * differentially; the calling session's own differential runs come
     * from the rounds it evaluates itself), ordinary or pipeline runs on
     * non-recursive workers.  The self-joined closure still records
     * delta-right, an exclusion of its own, and no worker-session. */
    const expect_t tdd_bounded = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, RUN_DIFF, RUN_DIFF
    };
    const expect_t tdd_self_join = {
        0, true, COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT, false, 0, RUN_DIFF
    };
    const expect_t parallel_bounded = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, 0, RUN_ORD
    };
    const expect_t parallel_pipeline = {
        0, true, COL_JOIN_BATCH_ELIGIBLE, false, 0, RUN_PIPE
    };
    const uint32_t workers[] = { 1, 4 };

#if defined(ENABLE_K_FUSION) && !ENABLE_K_FUSION
    printf("test_join_batch_program (nofusion)\n");
#else
    printf("test_join_batch_program (default)\n");
#endif
    clear_env();
    wl_columnar_eval_tdd_insert_test_round = record_insert_round;

    /* Coordinator routes: the first snapshot evaluates on the calling
     * session (these strata are below the parallel thresholds), so it runs
     * bounded and strict mode succeeds.  The snapshot after an incremental
     * insert takes the insert route in both modes. */
    for (uint32_t w = 0; w < 2; w++) {
        run_case("two-hop", TWO_HOP, "hop", oracle_two_hop, workers[w],
            MODE_RECORD, &two_hop_bounded, &insert_pipe);
        run_case("two-hop", TWO_HOP, "hop", oracle_two_hop, workers[w],
            MODE_STRICT, &two_hop_bounded, &insert_pipe);
        run_case("filtered two-hop", TWO_HOP_FILTERED, "hop",
            oracle_two_hop_filtered, workers[w], MODE_RECORD,
            &filtered_bounded, &insert_pipe);
        run_case("filtered two-hop", TWO_HOP_FILTERED, "hop",
            oracle_two_hop_filtered, workers[w], MODE_STRICT,
            &filtered_bounded, &insert_pipe);
        run_case("closure", CLOSURE, "path", oracle_closure, workers[w],
            MODE_RECORD, &closure_bounded, &insert_pipe);
        run_case("closure", CLOSURE, "path", oracle_closure, workers[w],
            MODE_STRICT, &closure_bounded, &insert_pipe);
        /* The four-way rule records several fallbacks (a JOIN feeding
         * another JOIN is pipeline-not-consumable, and its semi-naive
         * rounds read right deltas), so strict mode would refuse it and
         * its reason is not checked.  The default planner fuses the rule;
         * with the knob set no K-Fusion branch session is cloned at any
         * worker count (#1382). */
        kfusion_clones = 0;
        check_no_clones = true;
        wl_columnar_kfusion_test_clone = count_kfusion_clone;
        run_case("four-way rule", FOUR_WAY, "r", oracle_four_way, workers[w],
            MODE_RECORD, &four_way_rows, NULL);
        wl_columnar_kfusion_test_clone = NULL;
        check_no_clones = false;
    }

    /* Worker routes at W=4 (#2201): recursive strata on TDD workers (the
     * linear closure takes the owner strategy, the self-joined closure
     * bdx), and non-recursive strata split across workers.  Joins inside
     * the workers run bounded, and strict mode succeeds where the calling
     * session's would.  The snapshot after an incremental insert takes the
     * insert route in both modes, with clones running concurrently; for
     * the self-joined closure, whose first snapshot records delta-right,
     * the insert route's clones read a delta right operand bounded. */
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    run_case("closure on TDD workers", CLOSURE, "path", oracle_closure, 4,
        MODE_RECORD, &tdd_bounded, &insert_pipe);
    run_case("closure on TDD workers", CLOSURE, "path", oracle_closure, 4,
        MODE_STRICT, &tdd_bounded, &insert_pipe);
    run_case("self-joined closure on TDD workers", CLOSURE_SELF_JOIN, "path",
        oracle_closure, 4, MODE_RECORD, &tdd_self_join, &insert_pipe);
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    setenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    run_case("two-hop on parallel workers", TWO_HOP, "hop", oracle_two_hop, 4,
        MODE_RECORD, &parallel_bounded, &insert_pipe);
    run_case("two-hop on parallel workers", TWO_HOP, "hop", oracle_two_hop, 4,
        MODE_STRICT, &parallel_bounded, &insert_pipe);
    run_case("filtered two-hop on parallel workers", TWO_HOP_FILTERED, "hop",
        oracle_two_hop_filtered, 4, MODE_RECORD, &parallel_pipeline,
        &insert_pipe);
    run_case("filtered two-hop on parallel workers", TWO_HOP_FILTERED, "hop",
        oracle_two_hop_filtered, 4, MODE_STRICT, &parallel_pipeline,
        &insert_pipe);
    unsetenv("WIRELOG_NONREC_TDD_MIN_ROWS_PER_WORKER");

    run_reuse_after_overflow(false);
    run_reuse_after_overflow(true);
    run_insert_overflow_reuse(1);
    run_insert_overflow_reuse(4);
    run_strict_after_merge();

    printf("%d run, %d failed\n", tests_run, tests_failed);
    return tests_failed ? 1 : 0;
}
