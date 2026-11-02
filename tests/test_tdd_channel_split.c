/*
 * test_tdd_channel_split.c - multi-batch TDD channel delivery (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * A WL_SESSION_TEST_HOOKS hook asks every TDD worker to publish each delta
 * as three batches with identical timestamps (at most one batch per row, so
 * a one-row delta stays whole), and in one variant repeats a row in
 * neighbouring batches.  The recursive result at W=2 and W=8 must equal the
 * expected set, with no duplicate rows, under every TDD strategy the
 * programs below reach.  The overlap variant only shows that the repeated
 * rows are tolerated end to end: downstream head dedup would hide a missing
 * cross-batch consolidation, which the coalesce unit tests pin instead
 * (test_tdd_channel_spill coalesce_overlap_dedup, test_tdd_multiworker
 * test_channel_delivery_duplicate).
 */
#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/eval_tdd_channel.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

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
#define setenv wl_test_setenv_
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}
#define unsetenv wl_test_unsetenv_
#endif

static unsigned failures;

#define CHECK(cond, ...)                                            \
        do {                                                        \
            if (!(cond)) {                                          \
                fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
                fprintf(stderr, __VA_ARGS__);                       \
                fputc('\n', stderr);                                \
                failures++;                                         \
            }                                                       \
        } while (0)

/* Hook state.  The main thread writes the parameters before a snapshot
 * starts its workers and reads the counter after it returns; the workers
 * only read the parameters and bump the counter. */
static wl_atomic_u64 split_parts;
static wl_atomic_u64 split_overlap;
static wl_atomic_u64 split_fired;

static uint32_t
split_hook(wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows,
    bool *overlap)
{
    (void)worker;
    (void)rel_idx;
    uint64_t parts = atomic_load_explicit(&split_parts, memory_order_relaxed);
    if (parts >= 2 && rows >= 2)
        atomic_fetch_add_explicit(&split_fired, 1, memory_order_relaxed);
    *overlap = atomic_load_explicit(&split_overlap, memory_order_relaxed)
        != 0;
    return (uint32_t)parts;
}

typedef struct {
    int64_t x;
    int64_t y;
} pair_t;

typedef struct {
    pair_t *rows;
    size_t count;
    size_t cap;
    bool bad;
} collect_t;

static void
collect_tc(const char *relation, const int64_t *row, uint32_t ncols,
    void *arg)
{
    collect_t *out = arg;
    if (!relation || strcmp(relation, "tc") != 0)
        return;
    if (ncols != 2) {
        out->bad = true;
        return;
    }
    if (out->count == out->cap) {
        size_t cap = out->cap ? out->cap * 2 : 256;
        pair_t *rows = realloc(out->rows, cap * sizeof(*rows));
        if (!rows) {
            out->bad = true;
            return;
        }
        out->rows = rows;
        out->cap = cap;
    }
    out->rows[out->count].x = row[0];
    out->rows[out->count].y = row[1];
    out->count++;
}

static int
pair_cmp(const void *a, const void *b)
{
    const pair_t *l = a;
    const pair_t *r = b;
    if (l->x != r->x)
        return l->x < r->x ? -1 : 1;
    if (l->y != r->y)
        return l->y < r->y ? -1 : 1;
    return 0;
}

/* Sort @c; returns whether it held a duplicate row. */
static bool
sort_has_duplicate(collect_t *c)
{
    if (c->count > 1)
        qsort(c->rows, c->count, sizeof(*c->rows), pair_cmp);
    for (size_t i = 1; i < c->count; i++)
        if (pair_cmp(&c->rows[i - 1], &c->rows[i]) == 0)
            return true;
    return false;
}

#define NODES 24

typedef struct {
    const char *name;
    const char *rule;      /* the recursive tc rule */
    const char *global_read; /* WIRELOG_TDD_GLOBAL_READ, NULL to unset */
} program_t;

/* Strategies observed on this base: the one-sided rules run in owner mode,
 * the two-sided pair in global-read mode and the non-linear rule in BDX
 * mode.  The aligned hash exchange is reached by test_aligned_plan() below.
 * The coverage checks in main() keep these claims honest. */
static const program_t programs[] = {
    { "left-linear", "tc(x, z) :- tc(x, y), edge(y, z).\n", NULL },
    { "right-linear", "tc(x, z) :- edge(x, y), tc(y, z).\n", NULL },
    { "two-sided", "tc(x, z) :- tc(x, y), edge(y, z).\n"
      "tc(x, z) :- edge(x, y), tc(y, z).\n", NULL },
    { "non-linear", "tc(x, z) :- tc(x, y), tc(y, z).\n", NULL },
};

typedef struct {
    const char *strategy;
    const char *replay;
    uint32_t selected;
    uint64_t fired;
} run_info_t;

/* Evaluate @p on @workers workers and collect tc.  Returns 0 or an rc. */
static int
run_program(const program_t *p, uint32_t workers, collect_t *out,
    run_info_t *info)
{
    char src[512];
    snprintf(src, sizeof(src),
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n%s", p->rule);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return EINVAL;
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return rc ? rc : EINVAL;
    }
    plan_fixture_hold(prog);
    if (p->global_read)
        setenv("WIRELOG_TDD_GLOBAL_READ", p->global_read, 1);
    else
        unsetenv("WIRELOG_TDD_GLOBAL_READ");

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &session);
    if (rc == 0) {
        /* A DAG with skip edges: diameter and row count stay small enough
         * that owner mode never replays serially. */
        int64_t edges[2 * 2 * NODES];
        uint32_t count = 0;
        for (int64_t i = 0; i + 1 < NODES; i++) {
            edges[2 * count] = i;
            edges[2 * count + 1] = i + 1;
            count++;
            if (i % 2 == 0 && i + 3 < NODES) {
                edges[2 * count] = i;
                edges[2 * count + 1] = i + 3;
                count++;
            }
        }
        rc = wl_session_insert(session, "edge", edges, count, 2);
    }
    atomic_store_explicit(&split_fired, 0, memory_order_relaxed);
    if (rc == 0)
        rc = wl_session_snapshot(session, collect_tc, out);
    if (session) {
        const wl_col_session_t *coord = COL_SESSION(session);
        info->strategy = coord->tdd_audit.strategy;
        info->replay = coord->tdd_audit.replay;
        info->selected = coord->tdd_audit.selected_workers;
    }
    info->fired = atomic_load_explicit(&split_fired, memory_order_relaxed);
    unsetenv("WIRELOG_TDD_GLOBAL_READ");
    wl_session_destroy(session);
    wl_plan_free(plan);
    if (rc == 0 && out->bad)
        rc = EIO;
    return rc;
}

static bool
same_set(const collect_t *a, const collect_t *b)
{
    return a->count == b->count
           && (a->count == 0
           || memcmp(a->rows, b->rows, a->count * sizeof(*a->rows)) == 0);
}

typedef struct {
    bool owner;
    bool global_read;
    bool aligned;
    bool bdx;
} coverage_t;

static void
note_strategy(coverage_t *cov, const char *strategy)
{
    if (!strategy)
        return;
    if (strcmp(strategy, "owner") == 0)
        cov->owner = true;
    else if (strcmp(strategy, "global_read") == 0)
        cov->global_read = true;
    else if (strcmp(strategy, "aligned") == 0)
        cov->aligned = true;
    else if (strcmp(strategy, "bdx") == 0)
        cov->bdx = true;
}

/* Check one split run against the serial result and record its strategy. */
static void
check_split_run(const char *name, uint32_t workers, unsigned overlap,
    int rc, collect_t *got, const collect_t *serial, const run_info_t *info,
    coverage_t *cov)
{
    bool dup = sort_has_duplicate(got);
    fprintf(stderr, "  %s W=%u overlap=%u: strategy=%s selected=%u "
        "split deltas=%llu rows=%zu/%zu\n", name, workers, overlap,
        info->strategy ? info->strategy : "(none)", info->selected,
        (unsigned long long)info->fired, got->count, serial->count);
    CHECK(rc == 0, "%s W=%u overlap=%u: rc=%d", name, workers, overlap, rc);
    CHECK(info->selected == workers, "%s W=%u overlap=%u: %u workers selected",
        name, workers, overlap, info->selected);
    CHECK(info->fired > 0,
        "%s W=%u overlap=%u: no delta was split into batches", name, workers,
        overlap);
    CHECK(!dup, "%s W=%u overlap=%u: duplicate rows", name, workers, overlap);
    CHECK(info->replay == NULL, "%s W=%u overlap=%u: replayed serially (%s)",
        name, workers, overlap, info->replay ? info->replay : "");
    CHECK(same_set(got, serial), "%s W=%u overlap=%u: %zu rows, serial has %zu",
        name, workers, overlap, got->count, serial->count);
    if (rc == 0 && info->fired > 0 && info->selected == workers)
        note_strategy(cov, info->strategy);
}

static void
arm_split(unsigned overlap)
{
    atomic_store_explicit(&split_parts, 3, memory_order_relaxed);
    atomic_store_explicit(&split_overlap, overlap, memory_order_relaxed);
    atomic_store_explicit(&split_fired, 0, memory_order_relaxed);
    wl_columnar_eval_test_channel_split = split_hook;
}

static void
test_program(const program_t *p, coverage_t *cov)
{
    collect_t serial = { 0 };
    run_info_t info = { 0 };
    wl_columnar_eval_test_channel_split = NULL;
    int rc = run_program(p, 1, &serial, &info);
    CHECK(rc == 0 && serial.count > 0, "%s: serial run rc=%d rows=%zu",
        p->name, rc, serial.count);
    CHECK(!sort_has_duplicate(&serial), "%s: serial result has duplicates",
        p->name);

    static const uint32_t widths[] = { 2, 8 };
    for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
        for (unsigned overlap = 0; overlap < 2; overlap++) {
            uint32_t workers = widths[wi];
            collect_t got = { 0 };
            arm_split(overlap);
            rc = run_program(p, workers, &got, &info);
            wl_columnar_eval_test_channel_split = NULL;
            check_split_run(p->name, workers, overlap, rc, &got, &serial,
                &info, cov);
            free(got.rows);
        }
    }
    free(serial.rows);
}

/*
 * A hand-built aligned self-join, as test_tdd_recursive's worker-segment
 * fixture builds one: out(y, z) :- out(x, y), out(x, z), seeded from a
 * binary tree in `input` and exchanged on column 0.  Both join keys equal
 * the exchange key, so TDD runs it with the aligned hash exchange and
 * ordinary worker publication.  The serial evaluator does not run this
 * hand-built plan's rule, so the expected set comes from aligned_oracle().
 */
#define TREE_NODES 31

static void
tree_edges(int64_t *edges)
{
    for (int64_t p = 0; p < 15; p++) {
        edges[4 * p] = p;
        edges[4 * p + 1] = 2 * p + 1;
        edges[4 * p + 2] = p;
        edges[4 * p + 3] = 2 * p + 2;
    }
}

/* The fixpoint of the rule over the tree, computed naively. */
static void
aligned_oracle(collect_t *out)
{
    static bool in[TREE_NODES][TREE_NODES];
    int64_t edges[2 * 30];
    memset(in, 0, sizeof(in));
    tree_edges(edges);
    for (int i = 0; i < 30; i++)
        in[edges[2 * i]][edges[2 * i + 1]] = true;
    for (bool changed = true; changed;) {
        changed = false;
        for (int x = 0; x < TREE_NODES; x++)
            for (int y = 0; y < TREE_NODES; y++)
                for (int z = 0; in[x][y] && z < TREE_NODES; z++)
                    if (in[x][z] && !in[y][z]) {
                        in[y][z] = true;
                        changed = true;
                    }
    }
    for (int x = 0; x < TREE_NODES; x++)
        for (int y = 0; y < TREE_NODES; y++)
            if (in[x][y]) {
                int64_t row[2] = { x, y };
                collect_tc("tc", row, 2, out);
            }
}
static int
run_aligned(uint32_t workers, collect_t *out, run_info_t *info)
{
    uint32_t key = 0;
    uint32_t projection[] = { 1, 3 };
    const char *keys[] = { "col0" };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "out" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "out", .left_keys = keys,
          .right_keys = keys, .key_count = 1, .project_indices = projection,
          .project_count = 2 },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange },
    };
    wl_plan_relation_t rel = { .name = "out", .delta_name = "$d$out",
                               .ops = ops, .op_count = 5 };
    wl_plan_stratum_t stratum = { .relations = &rel, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *empty = NULL;
    int rc = wl_session_create(wl_backend_columnar(), &plan, workers,
            &session);
    wl_col_session_t *sess = COL_SESSION(session);
    if (rc == 0) {
        int64_t edges[2 * 30];
        tree_edges(edges);
        rc = wl_session_insert(session, "input", edges, 30, 2);
    }
    if (rc == 0) {
        empty = col_rel_new_auto("out", 2);
        rc = empty ? 0 : ENOMEM;
    }
    if (rc == 0) {
        wirelog_column_type_t types[2] = { WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_INT64 };
        rc = col_rel_set_column_types(empty, types, 2);
    }
    if (rc == 0) {
        rc = session_add_rel(sess, empty);
        if (rc == 0)
            empty = NULL;
    }
    if (rc == 0) {
        sess->tdd_audit.enabled = true;
        rc = col_eval_stratum_tdd(&stratum, sess, 0);
        info->strategy = sess->tdd_audit.strategy;
        info->replay = sess->tdd_audit.replay;
        info->selected = sess->tdd_audit.selected_workers;
        sess->tdd_audit.enabled = false;
    }
    info->fired = atomic_load_explicit(&split_fired, memory_order_relaxed);
    const col_rel_t *result = rc == 0 ? session_find_rel(sess, "out") : NULL;
    if (rc == 0 && (!result || result->ncols != 2))
        rc = EPROTO;
    for (uint32_t i = 0; rc == 0 && i < result->nrows; i++) {
        int64_t row[2] = { col_rel_get(result, i, 0),
                           col_rel_get(result, i, 1) };
        collect_tc("tc", row, 2, out);
    }
    col_rel_destroy(empty);
    wl_session_destroy(session);
    if (rc == 0 && out->bad)
        rc = EIO;
    return rc;
}

static void
test_aligned_plan(coverage_t *cov)
{
    collect_t serial = { 0 };
    run_info_t info = { 0 };
    aligned_oracle(&serial);
    CHECK(!serial.bad && !sort_has_duplicate(&serial),
        "aligned: oracle failed");
    int rc;
    static const uint32_t widths[] = { 2, 8 };
    for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
        for (unsigned overlap = 0; overlap < 2; overlap++) {
            collect_t got = { 0 };
            arm_split(overlap);
            rc = run_aligned(widths[wi], &got, &info);
            wl_columnar_eval_test_channel_split = NULL;
            check_split_run("aligned self-join", widths[wi], overlap, rc,
                &got, &serial, &info, cov);
            free(got.rows);
        }
    }
    free(serial.rows);
}

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    coverage_t cov = { false, false, false, false };
    for (size_t i = 0; i < sizeof(programs) / sizeof(programs[0]); i++)
        test_program(&programs[i], &cov);
    test_aligned_plan(&cov);
    CHECK(cov.owner, "no split run reached the owner strategy");
    CHECK(cov.global_read, "no split run reached the global-read strategy");
    CHECK(cov.aligned, "no split run reached the aligned hash exchange");
    CHECK(cov.bdx, "no split run reached the BDX exchange");
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_TDD_STRATUM_PROFILE");
    fprintf(stderr, "tdd_channel_split: %u failure(s)\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
