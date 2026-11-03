/*
 * test_tdd_channel_held.c - consumer-held cap and consumer spill on the
 * recursive TDD path (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Sessions with an enforcing governor and a spill store in a private
 * directory under WL_TDD_CHANNEL_SPILL_DIR (the build tree when run by
 * meson, never the system temporary directory) evaluate recursive strata
 * at W=2 and W=8 with a small WIRELOG_TDD_HELD_BYTES.  While a round's
 * producers run, the coordinator spills the batches it holds beyond the
 * cap as it takes them, and reads them back when it stages them after the
 * join.  Every result must equal the serial (W=1) result as a set; the
 * numeric runs must spill and rehydrate and hold no more than the cap at
 * any pump, which a hook before every pump checks, and a STRING relation,
 * which cannot spill, must stay over the cap and still succeed.  A cap
 * between the batches' charges mixes adopted and spilled slots in one
 * exchange, and a round that fails holding a spilled batch must release
 * it.  With several relations in one stratum, the exchange stages one
 * relation at a time and spills the others down to make room for it.
 * The directory must be empty at the end, so no spill segment may leak.
 *
 * Usage: test_tdd_channel_held [test-name]   (no name runs every test)
 */
#define _POSIX_C_SOURCE 200809L
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE /* mkdtemp() under _POSIX_C_SOURCE on macOS */
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/eval_tdd_channel.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

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

static char spill_dir[1024];

#define BUDGET (UINT64_C(1) << 30)

/* ------------------------------------------------------------------------ */
/* Results                                                                  */
/* ------------------------------------------------------------------------ */

typedef struct {
    int64_t rel; /* index into collect_t.relations */
    int64_t x;
    int64_t y;
} pair_t;

typedef struct {
    const char *const *relations; /* the relations collected */
    size_t nrelations;
    pair_t *rows;
    size_t count;
    size_t cap;
    bool bad;
} collect_t;

static const char *const tc_relation[] = { "tc" };

static void
collect_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *arg)
{
    collect_t *out = arg;
    size_t rel = 0;
    while (relation && rel < out->nrelations
        && strcmp(relation, out->relations[rel]) != 0)
        rel++;
    if (!relation || rel == out->nrelations)
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
    out->rows[out->count].rel = (int64_t)rel;
    out->rows[out->count].x = row[0];
    out->rows[out->count].y = row[1];
    out->count++;
}

static int
pair_cmp(const void *a, const void *b)
{
    const pair_t *l = a;
    const pair_t *r = b;
    if (l->rel != r->rel)
        return l->rel < r->rel ? -1 : 1;
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

static bool
same_set(const collect_t *a, const collect_t *b)
{
    return a->count == b->count
           && (a->count == 0
           || memcmp(a->rows, b->rows, a->count * sizeof(*a->rows)) == 0);
}

/* ------------------------------------------------------------------------ */
/* Sessions                                                                 */
/* ------------------------------------------------------------------------ */

/* A governor that reports @budget and admits up to BUDGET bytes, so a
 * small @budget still leaves room for the session's own reservations. */
static wl_columnar_memory_governor_ref_t *
make_governor(wl_columnar_memory_mode_t mode, uint64_t budget)
{
    wl_columnar_memory_resolution_t r;
    memset(&r, 0, sizeof(r));
    r.budget_bytes = budget;
    r.usable_bytes = BUDGET;
    r.mode = mode;
    r.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    r.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&r);
}

/* How a run is configured. */
typedef struct {
    const char *held_env;  /* WIRELOG_TDD_HELD_BYTES, NULL to unset */
    bool advisory;         /* an advisory governor instead of enforcing */
    bool spill;            /* open a spill store */
    uint64_t budget;       /* the governor's budget, 0 for BUDGET */
    uint64_t spill_quota;  /* the spill store's quota, 0 for 256 MiB */
} run_config_t;

/* What a run observed. */
typedef struct {
    const char *strategy;
    uint32_t selected;
    wl_tdd_consumer_stats_t consumer;
    bool spill_store;
    uint64_t pumps;           /* pumps the hook saw */
    uint64_t pump_held_max;   /* the most bytes held at one of them */
    uint64_t channel_cap;     /* the channel capacity at the last of them */
    uint64_t mixed_rounds;    /* rounds that ended with mixed lists */
    uint64_t open_spills;     /* pumps that saw a spill with a lane open */
    const char *replay;       /* the stratum's replay, "none" without one */
    /* The audit of the last stratum the run evaluated. */
    uint64_t audit_cancelled;
    int audit_channel_rc;
    uint64_t audit_orphans;
    uint64_t audit_rejected;
    uint64_t audit_spills;
    uint64_t audit_held_over_cap;
    uint64_t audit_stage_over_cap;
    uint64_t audit_stage_peak;
    const char *audit_refusal;
} run_info_t;

/* The coordinator of the run in progress, for pump_hook(). */
static wl_col_session_t *pump_coord;
static uint64_t pump_count;
static uint64_t pump_held_max;
static uint64_t pump_channel_cap;
static uint32_t pump_workers;     /* the run's width: lanes per list row */
static uint64_t pump_mixed_rounds;
static bool pump_last_mixed;
static uint64_t pump_open_spills;
static uint64_t pump_last_spilled; /* spilled batches at the last pump */

/* Spilled batches pump_hook() has newly seen on the lists, counted over the
 * run; a reserve under RESERVE_FAIL_AFTER_SPILL waits for it to grow. */
static wl_atomic_u64 spill_events;

/* Set by pump_hook() once it has seen a spilled batch while a lane was
 * still open; worker 0 waits for it under RESERVE_HOLD_LANE. */
static wl_atomic_u64 open_spill_seen;

/* What reserve_hook() does on a worker thread before each reserve. */
enum {
    RESERVE_PLAIN,           /* nothing */
    RESERVE_HOLD_LANE,       /* worker 0 waits until a spill is seen */
    RESERVE_FAIL_AFTER_SPILL /* one reserve fails once a batch of its round
                              * was spilled while it waited */
};
static wl_atomic_u64 reserve_mode;
/* The failing reserve's state: FAIL_IDLE, FAIL_WAITING while one waits,
 * and FAIL_FIRED for good once one has failed. */
enum { FAIL_IDLE, FAIL_WAITING, FAIL_FIRED };
static wl_atomic_u64 fail_state;

/*
 * Whether, for one relation, the lists hold both a slot the coalesce will
 * adopt (a single RESIDENT batch) and a slot built from SPILLED batches
 * only.  Owner and global-read rounds then exchange an adopted slot,
 * created like the plan's result, next to one created like the worker's
 * relation.
 */
static bool
lists_mixed(wl_tdd_batch_t *const *lists, uint32_t workers, uint32_t nrels)
{
    for (uint32_t ri = 0; ri < nrels; ri++) {
        bool adopted = false;
        bool spilled = false;
        for (uint32_t w = 0; w < workers; w++) {
            const wl_tdd_batch_t *b = lists[(size_t)ri * workers + w];
            if (!b)
                continue;
            if (!b->next && b->kind == WL_TDD_BATCH_RESIDENT)
                adopted = true;
            bool all_spilled = true;
            for (; b; b = b->next)
                all_spilled = all_spilled && b->kind == WL_TDD_BATCH_SPILLED;
            spilled = spilled || all_spilled;
        }
        if (adopted && spilled)
            return true;
    }
    return false;
}

/*
 * Before every pump of a round, on the coordinator thread: record the
 * bytes the consumer holds on its lists at that moment, and the channel's
 * capacity.  The lists only grow during a round and the coalesce empties
 * them, so the state at the last pump before empty lists is the one the
 * round's coalesce saw.
 */
static int
pump_hook(wl_tdd_channel_t *ch)
{
    wl_tdd_channel_stats_t stats;
    wl_tdd_channel_stats(ch, &stats);
    pump_channel_cap = stats.cap_bytes;
    if (!pump_coord || !pump_coord->tdd_channel_lists)
        return 0;
    wl_tdd_batch_t *const *lists = pump_coord->tdd_channel_lists;
    uint32_t count = (uint32_t)pump_coord->tdd_channel_list_count;
    uint32_t lanes = pump_workers;
    uint64_t held = wl_columnar_eval_tdd_held_bytes(lists, 1, count,
            UINT32_MAX);
    if (held > pump_held_max)
        pump_held_max = held;
    pump_count++;
    bool empty = true;
    uint64_t spilled = 0;
    for (uint32_t i = 0; i < count; i++) {
        for (const wl_tdd_batch_t *b = lists[i]; b; b = b->next) {
            empty = false;
            if (b->kind == WL_TDD_BATCH_SPILLED)
                spilled++;
        }
    }
    if (empty) {
        if (pump_last_mixed)
            pump_mixed_rounds++;
        pump_last_mixed = false;
    } else if (lanes && count % lanes == 0) {
        pump_last_mixed = lists_mixed(lists, lanes, count / lanes);
    }
    /* A batch stays on its list until the round's coalesce, so a rise in
     * the spilled count is a spill made since the last pump. */
    if (spilled > pump_last_spilled)
        atomic_fetch_add_explicit(&spill_events, spilled - pump_last_spilled,
            memory_order_relaxed);
    pump_last_spilled = spilled;
    if (spilled && stats.lanes_open > 0) {
        pump_open_spills++;
        atomic_store_explicit(&open_spill_seen, 1, memory_order_relaxed);
    }
    return 0;
}

/*
 * A spill was seen with a lane open; or no other lane is open, nothing is
 * queued and the consumer waits in pump again.  The consumer spills a
 * batch it takes before its next pump, and pump_hook() runs before that
 * pump, so a batch another worker published has been spilled and seen by
 * then.
 */
static bool
pred_open_spill(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return atomic_load_explicit(&open_spill_seen, memory_order_relaxed) != 0
           || (stats->lanes_open <= 1 && stats->queued_batches == 0
           && stats->consumer_waiting);
}

/* A spill was seen after *@ctx spill events; or no other lane is open,
 * nothing is queued and the consumer waits in pump again. */
static bool
pred_new_spill(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    const uint64_t *start = ctx;
    return atomic_load_explicit(&spill_events, memory_order_relaxed) > *start
           || (stats->lanes_open <= 1 && stats->queued_batches == 0
           && stats->consumer_waiting);
}

/*
 * RESERVE_FAIL_AFTER_SPILL: one reserve at a time waits, with its lane
 * open, until the coordinator spills a batch, which can then only be one of
 * the same round.  If one was spilled, the reserve fails with ENOMEM and
 * the round fails holding that spilled batch on its lists; otherwise it
 * goes on and a later reserve tries again.  Only one reserve fails.
 */
static int
fail_after_spill(wl_tdd_channel_t *ch)
{
    uint64_t start = atomic_load_explicit(&spill_events,
            memory_order_relaxed);
    uint64_t idle = FAIL_IDLE;
    /* FAIL_FIRED is final, so once a reserve has failed no claim succeeds. */
    if (!atomic_compare_exchange_strong_explicit(&fail_state, &idle,
        FAIL_WAITING, memory_order_relaxed, memory_order_relaxed))
        return 0;
    (void)wl_tdd_channel_test_wait(ch, pred_new_spill, &start, 10000u);
    bool fire = atomic_load_explicit(&spill_events, memory_order_relaxed)
        > start;
    atomic_store_explicit(&fail_state, fire ? FAIL_FIRED : FAIL_IDLE,
        memory_order_relaxed);
    return fire ? ENOMEM : 0;
}

static int
reserve_hook(wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows)
{
    (void)rel_idx;
    (void)rows;
    uint64_t mode = atomic_load_explicit(&reserve_mode, memory_order_relaxed);
    if (mode == RESERVE_FAIL_AFTER_SPILL)
        return fail_after_spill(worker->coordinator->tdd_channel);
    /* Keep worker 0's lane open until the coordinator has spilled another
     * worker's batch, so a spill runs while a producer is still running.
     * Its own lane counts as open, hence a round in which no other worker
     * publishes ends the wait when worker 0's is the last lane open; the
     * timeout only keeps a broken run from hanging. */
    if (mode == RESERVE_HOLD_LANE && worker->worker_id == 0
        && atomic_load_explicit(&open_spill_seen, memory_order_relaxed) == 0)
        (void)wl_tdd_channel_test_wait(worker->coordinator->tdd_channel,
            pred_open_spill, NULL, 10000u);
    return 0;
}

/* Watch the pumps of @session's rounds, on @workers workers, from now on. */
static void
watch_pumps(wl_session_t *session, uint32_t workers)
{
    pump_coord = COL_SESSION(session);
    pump_workers = workers;
    pump_count = 0;
    pump_held_max = 0;
    pump_channel_cap = 0;
    pump_mixed_rounds = 0;
    pump_last_mixed = false;
    pump_open_spills = 0;
    pump_last_spilled = 0;
    atomic_store_explicit(&open_spill_seen, 0, memory_order_relaxed);
    atomic_store_explicit(&spill_events, 0, memory_order_relaxed);
    atomic_store_explicit(&fail_state, FAIL_IDLE, memory_order_relaxed);
}

/* Create @plan's session on @workers workers under @cfg; NULL @cfg means
 * plain wl_session_create(). */
static int
open_session(const wl_plan_t *plan, uint32_t workers,
    const run_config_t *cfg, wl_columnar_memory_governor_ref_t **gov_out,
    wl_session_t **out)
{
    *gov_out = NULL;
    if (!cfg)
        return wl_session_create(wl_backend_columnar(), plan, workers, out);
    if (cfg->held_env)
        setenv("WIRELOG_TDD_HELD_BYTES", cfg->held_env, 1);
    else
        unsetenv("WIRELOG_TDD_HELD_BYTES");
    wl_columnar_memory_governor_ref_t *g = make_governor(cfg->advisory
            ? WL_COLUMNAR_MEMORY_MODE_ADVISORY
            : WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            cfg->budget ? cfg->budget : BUDGET);
    if (!g)
        return ENOMEM;
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = g;
    if (cfg->spill) {
        options.spill_directory = spill_dir;
        options.spill_quota_bytes = cfg->spill_quota ? cfg->spill_quota
            : UINT64_C(1) << 28;
        options.spill_max_row_bytes = 256;
    }
    int rc = wl_session_create_with_options(wl_backend_columnar(), plan,
            workers, &options, out);
    if (rc != 0) {
        wl_columnar_memory_governor_ref_release(g);
        return rc;
    }
    *gov_out = g;
    return 0;
}

static void
note_run(wl_session_t *session, run_info_t *info)
{
    const wl_col_session_t *coord = COL_SESSION(session);
    info->strategy = coord->tdd_audit.strategy;
    info->selected = coord->tdd_audit.selected_workers;
    info->replay = coord->tdd_audit.replay;
    info->audit_cancelled = coord->tdd_audit.channel_cancelled;
    info->audit_channel_rc = coord->tdd_audit.channel_rc;
    info->audit_orphans = coord->tdd_audit.orphan_reclaims;
    info->audit_rejected = coord->tdd_audit.rejected;
    info->audit_spills = coord->tdd_audit.consumer_spills;
    info->audit_held_over_cap = coord->tdd_audit.held_over_cap;
    info->audit_stage_over_cap = coord->tdd_audit.stage_over_cap;
    info->audit_stage_peak = coord->tdd_audit.stage_peak_bytes;
    info->audit_refusal = coord->tdd_audit.refusal;
    info->consumer = coord->tdd_consumer;
    info->spill_store = coord->spill_store != NULL;
    info->pumps = pump_count;
    info->pump_held_max = pump_held_max;
    info->channel_cap = pump_channel_cap;
    info->mixed_rounds = pump_mixed_rounds + (pump_last_mixed ? 1u : 0u);
    info->open_spills = pump_open_spills;
    pump_coord = NULL;
}

static void
close_session(wl_session_t *session)
{
    wl_session_destroy(session);
    unsetenv("WIRELOG_TDD_HELD_BYTES");
}

/* Once the session, its plan and the program whose intern table it
 * attached are gone, nothing may still hold bytes under @g. */
static void
release_governor(wl_columnar_memory_governor_ref_t *g)
{
    if (!g)
        return;
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g))
        == 0, "governor not back at 0 after the session");
    wl_columnar_memory_governor_ref_release(g);
}

/* ------------------------------------------------------------------------ */
/* Programs                                                                 */
/* ------------------------------------------------------------------------ */

#define NODES 24

typedef struct {
    const char *name;
    const char *rule;      /* the recursive rule(s); see mutual[] */
    const char *strategy;  /* the strategy the program reaches */
    bool linear;           /* with no rule: run_aligned_plan()'s linear form */
    bool replicate;        /* with no rule: as if no input was inserted */
} program_t;

static const program_t programs[] = {
    { "left-linear", "tc(x, z) :- tc(x, y), edge(y, z).\n", "owner" },
    { "two-sided", "tc(x, z) :- tc(x, y), edge(y, z).\n"
      "tc(x, z) :- edge(x, y), tc(y, z).\n", "global_read" },
    { "non-linear", "tc(x, z) :- tc(x, y), tc(y, z).\n", "bdx" },
};

/*
 * A DAG with skip edges: diameter and row count stay small enough that
 * owner mode never replays serially.  With @strings, node i is "n<i>".
 */
static void
append_edges(char *src, size_t cap, bool strings)
{
    size_t len = strlen(src);
    for (int i = 0; i + 1 < NODES; i++) {
        int to[2] = { i + 1, (i % 2 == 0 && i + 3 < NODES) ? i + 3 : -1 };
        for (int k = 0; k < 2; k++) {
            if (to[k] < 0)
                continue;
            int n = strings
                ? snprintf(src + len, cap - len, "edge(\"n%d\", \"n%d\").\n",
                    i, to[k])
                : snprintf(src + len, cap - len, "edge(%d, %d).\n", i, to[k]);
            if (n > 0 && (size_t)n < cap - len)
                len += (size_t)n;
        }
    }
}

/* Evaluate @p on @workers workers and collect tc.  Returns 0 or an rc. */
static int
run_program(const program_t *p, bool strings, uint32_t workers,
    const run_config_t *cfg, collect_t *out, run_info_t *info)
{
    static char src[8192];
    snprintf(src, sizeof(src),
        ".decl edge(x: %s, y: %s)\n"
        ".decl tc(x: %s, y: %s)\n"
        "tc(x, y) :- edge(x, y).\n%s",
        strings ? "string" : "int32", strings ? "string" : "int32",
        strings ? "string" : "int32", strings ? "string" : "int32",
        p->rule);
    append_edges(src, sizeof(src), strings);
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
    wl_columnar_memory_governor_ref_t *g = NULL;
    wl_session_t *session = NULL;
    rc = open_session(plan, workers, cfg, &g, &session);
    if (rc == 0) {
        watch_pumps(session, workers);
        rc = wl_session_load_facts(session, prog);
    }
    out->relations = tc_relation;
    out->nrelations = 1;
    if (rc == 0)
        rc = wl_session_snapshot(session, collect_rows, out);
    if (session) {
        note_run(session, info);
        close_session(session);
    }
    wl_plan_free(plan);
    /* The program's intern table holds governor bytes until it is freed. */
    plan_fixture_release();
    release_governor(g);
    if (rc == 0 && out->bad)
        rc = EIO;
    return rc;
}

/*
 * The aligned self-join test_tdd_channel_split.c builds by hand:
 * out(y, z) :- out(x, y), out(x, z) over a binary tree, exchanged on
 * column 0, so TDD runs it with the aligned hash exchange and ordinary
 * worker publication.
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

/* The closure the hand-built plans reach, once for each of @relations:
 * with @linear the transitive closure of the edges, otherwise the closure
 * under r(y, z) :- r(x, y), r(x, z). */
static void
aligned_oracle_for(collect_t *out, const char *const *relations,
    size_t nrelations, bool linear)
{
    static bool edge[TREE_NODES][TREE_NODES];
    static bool in[TREE_NODES][TREE_NODES];
    int64_t edges[2 * 30];
    memset(edge, 0, sizeof(edge));
    memset(in, 0, sizeof(in));
    tree_edges(edges);
    for (int i = 0; i < 30; i++) {
        edge[edges[2 * i]][edges[2 * i + 1]] = true;
        in[edges[2 * i]][edges[2 * i + 1]] = true;
    }
    for (bool changed = true; changed;) {
        changed = false;
        for (int x = 0; x < TREE_NODES; x++)
            for (int y = 0; y < TREE_NODES; y++)
                for (int z = 0; in[x][y] && z < TREE_NODES; z++) {
                    if (linear && edge[y][z] && !in[x][z]) {
                        in[x][z] = true;
                        changed = true;
                    } else if (!linear && in[x][z] && !in[y][z]) {
                        in[y][z] = true;
                        changed = true;
                    }
                }
    }
    out->relations = relations;
    out->nrelations = nrelations;
    for (size_t r = 0; r < nrelations; r++)
        for (int x = 0; x < TREE_NODES; x++)
            for (int y = 0; y < TREE_NODES; y++)
                if (in[x][y]) {
                    int64_t row[2] = { x, y };
                    collect_rows(relations[r], row, 2, out);
                }
}

static void
aligned_oracle(collect_t *out)
{
    aligned_oracle_for(out, tc_relation, 1, false);
}

#define ALIGNED_MAX_RELS 3u

/*
 * Evaluate a hand-built stratum of @nrels relations, at most
 * ALIGNED_MAX_RELS, that reaches the aligned strategy, and collect them.
 * Each relation r closes over r(y, z) :- r(x, y), r(x, z), a self-join that
 * tdd_exchange_deltas() broadcasts, or with @linear over
 * r(x, z) :- r(x, y), input(y, z), whose deltas it scatters by hash and
 * gathers; that form runs with WIRELOG_TDD_GLOBAL_READ=0, since it would
 * otherwise take the global-read exchange.  With @replicate the session
 * forgets that input was inserted, as when no input changed since the last
 * step, which sends the stratum to the replicate strategy and
 * tdd_broadcast_deltas().  The first relation is seeded by input and, with
 * several, fed by the last; every other relation is fed by the one before
 * it, so all of them reach the same closure.  The serial evaluator does not
 * iterate these hand-built strata, so their results are checked against
 * aligned_oracle_for().
 */
static int
run_aligned_plan(const char *const *names, uint32_t nrels, bool linear,
    bool replicate, uint32_t workers, const run_config_t *cfg,
    collect_t *out, run_info_t *info)
{
    uint32_t key = 0;
    uint32_t projection[] = { 1, 3 };
    uint32_t linear_projection[] = { 0, 3 };
    const char *keys[] = { "col0" };
    const char *left_keys[] = { "col1" };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[ALIGNED_MAX_RELS][7];
    wl_plan_relation_t rels[ALIGNED_MAX_RELS];
    char delta_names[ALIGNED_MAX_RELS][32];
    if (nrels == 0 || nrels > ALIGNED_MAX_RELS)
        return EINVAL;
    memset(ops, 0, sizeof(ops));
    memset(rels, 0, sizeof(rels));
    for (uint32_t r = 0; r < nrels; r++) {
        uint32_t n = 0;
        ops[r][n].op = WL_PLAN_OP_VARIABLE;
        ops[r][n++].relation_name = names[r];
        ops[r][n].op = WL_PLAN_OP_JOIN;
        ops[r][n].right_relation = linear ? "input" : names[r];
        ops[r][n].left_keys = linear ? left_keys : keys;
        ops[r][n].right_keys = keys;
        ops[r][n].key_count = 1;
        ops[r][n].project_indices = linear ? linear_projection : projection;
        ops[r][n++].project_count = 2;
        ops[r][n].op = WL_PLAN_OP_VARIABLE;
        ops[r][n++].relation_name = r == 0 ? "input" : names[r - 1];
        ops[r][n++].op = WL_PLAN_OP_CONCAT;
        if (r == 0 && nrels > 1) {
            ops[r][n].op = WL_PLAN_OP_VARIABLE;
            ops[r][n++].relation_name = names[nrels - 1];
            ops[r][n++].op = WL_PLAN_OP_CONCAT;
        }
        ops[r][n].op = WL_PLAN_OP_EXCHANGE;
        ops[r][n++].opaque_data = &exchange;
        snprintf(delta_names[r], sizeof(delta_names[r]), "$d$%s", names[r]);
        rels[r].name = names[r];
        rels[r].delta_name = delta_names[r];
        rels[r].ops = ops[r];
        rels[r].op_count = n;
    }
    wl_plan_stratum_t stratum = { .relations = rels,
                                  .relation_count = nrels,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_columnar_memory_governor_ref_t *g = NULL;
    wl_session_t *session = NULL;
    col_rel_t *empty = NULL;
    int rc = open_session(&plan, workers, cfg, &g, &session);
    wl_col_session_t *sess = rc == 0 ? COL_SESSION(session) : NULL;
    if (sess)
        watch_pumps(session, workers);
    if (rc == 0) {
        int64_t edges[2 * 30];
        tree_edges(edges);
        rc = wl_session_insert(session, "input", edges, 30, 2);
    }
    for (uint32_t r = 0; rc == 0 && r < nrels; r++) {
        empty = col_rel_new_auto(names[r], 2);
        rc = empty ? 0 : ENOMEM;
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
    }
    if (rc == 0) {
        memset(&sess->tdd_consumer, 0, sizeof(sess->tdd_consumer));
        sess->tdd_audit.enabled = true;
        if (replicate)
            sess->last_inserted_relation = NULL;
        if (linear)
            setenv("WIRELOG_TDD_GLOBAL_READ", "0", 1);
        rc = col_eval_stratum_tdd(&stratum, sess, 0);
        if (linear)
            unsetenv("WIRELOG_TDD_GLOBAL_READ");
        sess->tdd_audit.enabled = false;
    }
    out->relations = names;
    out->nrelations = nrels;
    for (uint32_t r = 0; rc == 0 && r < nrels; r++) {
        const col_rel_t *result = session_find_rel(sess, names[r]);
        if (!result || result->ncols != 2) {
            rc = EPROTO;
            break;
        }
        for (uint32_t i = 0; i < result->nrows; i++) {
            int64_t row[2] = { col_rel_get(result, i, 0),
                               col_rel_get(result, i, 1) };
            collect_rows(names[r], row, 2, out);
        }
    }
    col_rel_destroy(empty);
    if (session) {
        note_run(session, info);
        close_session(session);
    }
    release_governor(g);
    if (rc == 0 && out->bad)
        rc = EIO;
    return rc;
}

static const char *const aligned_relation[] = { "out" };

static int
run_aligned(uint32_t workers, const run_config_t *cfg, collect_t *out,
    run_info_t *info)
{
    return run_aligned_plan(aligned_relation, 1, false, false, workers, cfg,
               out, info);
}

/* ------------------------------------------------------------------------ */
/* Checks                                                                   */
/* ------------------------------------------------------------------------ */

static const uint32_t widths[] = { 2, 8 };

/* The held bound a run must keep: after a round's cap pass the consumer
 * holds no more than the cap, unless a pass recorded that nothing more
 * could be spilled. */
static void
check_held_bound(const char *name, uint32_t workers,
    const wl_tdd_consumer_stats_t *c)
{
    CHECK(c->max_held_bytes <= c->held_cap || c->held_over_cap > 0,
        "%s W=%u: max_held=%llu cap=%llu with no over-cap pass", name,
        workers, (unsigned long long)c->max_held_bytes,
        (unsigned long long)c->held_cap);
}

/*
 * After a successful run whose last stratum is its only TDD stratum, that
 * stratum's audit repeats the consumer counts, which start at 0 for each
 * snapshot, and records no cancel, channel error, orphan, rejected
 * reservation or refusal.
 */
static void
check_audit(const char *name, uint32_t workers, const run_info_t *info)
{
    const wl_tdd_consumer_stats_t *c = &info->consumer;
    CHECK(info->audit_spills == c->consumer_spills
        && info->audit_held_over_cap == c->held_over_cap
        && info->audit_stage_over_cap == c->stage_over_cap
        && info->audit_stage_peak == c->stage_peak_bytes,
        "%s W=%u: audit spills=%llu over_cap=%llu stage_over_cap=%llu "
        "stage_peak=%llu", name, workers,
        (unsigned long long)info->audit_spills,
        (unsigned long long)info->audit_held_over_cap,
        (unsigned long long)info->audit_stage_over_cap,
        (unsigned long long)info->audit_stage_peak);
    CHECK(info->audit_cancelled == 0 && info->audit_channel_rc == 0
        && info->audit_orphans == 0 && info->audit_rejected == 0
        && !info->audit_refusal,
        "%s W=%u: audit cancelled=%llu channel_rc=%d refusal=%s", name,
        workers, (unsigned long long)info->audit_cancelled,
        info->audit_channel_rc,
        info->audit_refusal ? info->audit_refusal : "(none)");
}

/* A numeric run with the held cap at 1 byte: every batch is spilled as
 * it is taken, so no pump sees a held byte, and read back when it is
 * staged. */
static void
check_spill_run(const char *name, const char *want_strategy,
    uint32_t workers, int rc, collect_t *got, const collect_t *serial,
    const run_info_t *info)
{
    const wl_tdd_consumer_stats_t *c = &info->consumer;
    bool dup = sort_has_duplicate(got);
    fprintf(stderr, "  %s W=%u: strategy=%s selected=%u spills=%llu "
        "rehydrated=%llu over_cap=%llu take_peak=%llu max_held=%llu "
        "pumps=%llu pump_held=%llu open_spills=%llu rows=%zu/%zu\n", name,
        workers, info->strategy ? info->strategy : "(none)", info->selected,
        (unsigned long long)c->consumer_spills,
        (unsigned long long)c->rehydrated_batches,
        (unsigned long long)c->held_over_cap,
        (unsigned long long)c->round_held_peak_bytes,
        (unsigned long long)c->max_held_bytes,
        (unsigned long long)info->pumps,
        (unsigned long long)info->pump_held_max,
        (unsigned long long)info->open_spills, got->count, serial->count);
    CHECK(rc == 0, "%s W=%u: rc=%d", name, workers, rc);
    CHECK(info->spill_store, "%s W=%u: no spill store", name, workers);
    CHECK(info->selected == workers, "%s W=%u: %u workers selected", name,
        workers, info->selected);
    CHECK(info->strategy && strcmp(info->strategy, want_strategy) == 0,
        "%s W=%u: strategy %s, expected %s", name, workers,
        info->strategy ? info->strategy : "(none)", want_strategy);
    CHECK(c->held_cap == 1, "%s W=%u: held cap %llu", name, workers,
        (unsigned long long)c->held_cap);
    CHECK(c->consumer_spills > 0, "%s W=%u: nothing spilled", name, workers);
    CHECK(c->rehydrated_batches > 0, "%s W=%u: nothing rehydrated", name,
        workers);
    CHECK(c->round_held_peak_bytes > c->held_cap,
        "%s W=%u: the cap never mattered", name, workers);
    /* The spills happen while the round runs, not after its join, and one
     * happened while worker 0's lane was held open. */
    CHECK(info->pumps > 0, "%s W=%u: no pump seen", name, workers);
    CHECK(info->open_spills > 0, "%s W=%u: no spill with a lane open", name,
        workers);
    CHECK(info->pump_held_max <= c->held_cap,
        "%s W=%u: %llu bytes held at a pump, cap %llu", name, workers,
        (unsigned long long)info->pump_held_max,
        (unsigned long long)c->held_cap);
    /* Numeric, timestamped batches always spill. */
    CHECK(c->held_over_cap == 0 && c->max_held_bytes == 0,
        "%s W=%u: over_cap=%llu max_held=%llu", name, workers,
        (unsigned long long)c->held_over_cap,
        (unsigned long long)c->max_held_bytes);
    check_held_bound(name, workers, c);
    check_audit(name, workers, info);
    CHECK(!dup, "%s W=%u: duplicate rows", name, workers);
    CHECK(same_set(got, serial), "%s W=%u: %zu rows, serial has %zu", name,
        workers, got->count, serial->count);
}

/* Run the capped runs with worker 0 holding its lane open until a spill
 * has been seen, or with plain reserves. */
static void
set_reserve_mode(uint64_t mode)
{
    atomic_store_explicit(&reserve_mode, mode, memory_order_relaxed);
}

/* T7: owner, global-read and BDX strata spill every held batch (a 1-byte
 * cap) and read them back; the results stay exact.  No slot is adopted
 * here; test_mixed_spill() covers rounds that mix adopted and spilled
 * slots. */
static void
test_numeric_spill(void)
{
    static const run_config_t cfg = { "1", false, true };
    for (size_t i = 0; i < sizeof(programs) / sizeof(programs[0]); i++) {
        const program_t *p = &programs[i];
        collect_t serial = { 0 };
        run_info_t info;
        memset(&info, 0, sizeof(info));
        int rc = run_program(p, false, 1, NULL, &serial, &info);
        CHECK(rc == 0 && serial.count > 0, "%s: serial rc=%d rows=%zu",
            p->name, rc, serial.count);
        (void)sort_has_duplicate(&serial);
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            collect_t got = { 0 };
            memset(&info, 0, sizeof(info));
            set_reserve_mode(RESERVE_HOLD_LANE);
            rc = run_program(p, false, widths[wi], &cfg, &got, &info);
            set_reserve_mode(RESERVE_PLAIN);
            check_spill_run(p->name, p->strategy, widths[wi], rc, &got,
                &serial, &info);
            free(got.rows);
        }
        free(serial.rows);
    }
}

/*
 * A cap between the batches' charges, as the default quarter of a budget
 * is under pressure: a round spills its largest batches and keeps the
 * others resident, so one relation's slots can be adopted and built from
 * spilled batches in the same exchange.  The owner and global-read
 * exchanges refuse slots whose schemas are incompatible.  These
 * relations carry the default col<N> names on both kinds of slot, so the
 * runs cannot tell whether a slot of spilled batches takes the worker's
 * relation as its template; test_tdd_channel_spill checks that.  The cap
 * is the largest charge an uncapped run saw, so it does not depend on the
 * platform's charges.  Owner and global-read runs must end at least two
 * rounds with such mixed slots.
 */
static void
test_mixed_spill(void)
{
    char held[32];
    for (size_t i = 0; i < sizeof(programs) / sizeof(programs[0]); i++) {
        const program_t *p = &programs[i];
        bool outbound = strcmp(p->strategy, "bdx") != 0;
        collect_t serial = { 0 };
        run_info_t info;
        memset(&info, 0, sizeof(info));
        int rc = run_program(p, false, 1, NULL, &serial, &info);
        CHECK(rc == 0 && serial.count > 0, "%s: serial rc=%d rows=%zu",
            p->name, rc, serial.count);
        (void)sort_has_duplicate(&serial);
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            uint32_t workers = widths[wi];
            run_config_t cfg = { "1099511627776", false, true, 0 };
            collect_t got = { 0 };
            memset(&info, 0, sizeof(info));
            rc = run_program(p, false, workers, &cfg, &got, &info);
            uint64_t largest = info.consumer.max_batch_charge;
            CHECK(rc == 0 && largest > 0 && info.consumer.consumer_spills == 0,
                "%s W=%u calibration: rc=%d largest=%llu spills=%llu",
                p->name, workers, rc, (unsigned long long)largest,
                (unsigned long long)info.consumer.consumer_spills);
            free(got.rows);

            snprintf(held, sizeof(held), "%llu", (unsigned long long)largest);
            cfg.held_env = held;
            memset(&got, 0, sizeof(got));
            memset(&info, 0, sizeof(info));
            rc = run_program(p, false, workers, &cfg, &got, &info);
            const wl_tdd_consumer_stats_t *c = &info.consumer;
            bool dup = sort_has_duplicate(&got);
            fprintf(stderr, "  %s W=%u mixed: strategy=%s cap=%s spills=%llu "
                "max_held=%llu over_cap=%llu mixed_rounds=%llu "
                "pump_held=%llu rows=%zu/%zu\n", p->name, workers,
                info.strategy ? info.strategy : "(none)", held,
                (unsigned long long)c->consumer_spills,
                (unsigned long long)c->max_held_bytes,
                (unsigned long long)c->held_over_cap,
                (unsigned long long)info.mixed_rounds,
                (unsigned long long)info.pump_held_max, got.count,
                serial.count);
            CHECK(rc == 0, "%s W=%u mixed: rc=%d", p->name, workers, rc);
            CHECK(info.selected == workers && info.strategy
                && strcmp(info.strategy, p->strategy) == 0,
                "%s W=%u mixed: strategy %s on %u workers", p->name, workers,
                info.strategy ? info.strategy : "(none)", info.selected);
            CHECK(c->consumer_spills > 0 && c->max_held_bytes > 0,
                "%s W=%u mixed: spills=%llu max_held=%llu", p->name,
                workers, (unsigned long long)c->consumer_spills,
                (unsigned long long)c->max_held_bytes);
            CHECK(!outbound || info.mixed_rounds >= 2,
                "%s W=%u mixed: %llu rounds ended with mixed slots", p->name,
                workers, (unsigned long long)info.mixed_rounds);
            CHECK(info.pump_held_max <= c->held_cap,
                "%s W=%u mixed: %llu bytes held at a pump, cap %llu",
                p->name, workers, (unsigned long long)info.pump_held_max,
                (unsigned long long)c->held_cap);
            check_held_bound(p->name, workers, c);
            CHECK(!dup, "%s W=%u mixed: duplicate rows", p->name, workers);
            CHECK(same_set(&got, &serial),
                "%s W=%u mixed: %zu rows, serial has %zu", p->name, workers,
                got.count, serial.count);
            free(got.rows);
        }
        free(serial.rows);
    }
}

/*
 * A round that fails holding a spilled batch: a reserve waits until the
 * coordinator has spilled a batch of its round, then fails with ENOMEM.
 * The stratum reports ENOMEM, and the spilled batch goes down the failure
 * path instead of being read back, so more batches were spilled than read
 * back.  Releasing it is checked by the governor returning to 0 after the
 * session and by the spill directory ending empty; in owner mode it goes
 * through the lifetime's retained list.
 */
static void
test_spill_then_fail(void)
{
    static const run_config_t cfg = { "1", false, true };
    for (size_t i = 0; i < sizeof(programs) / sizeof(programs[0]); i++) {
        const program_t *p = &programs[i];
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            uint32_t workers = widths[wi];
            collect_t got = { 0 };
            run_info_t info;
            memset(&info, 0, sizeof(info));
            set_reserve_mode(RESERVE_FAIL_AFTER_SPILL);
            int rc = run_program(p, false, workers, &cfg, &got, &info);
            set_reserve_mode(RESERVE_PLAIN);
            bool fired = atomic_load_explicit(&fail_state,
                    memory_order_relaxed) == FAIL_FIRED;
            const wl_tdd_consumer_stats_t *c = &info.consumer;
            fprintf(stderr, "  %s W=%u fail: rc=%d fired=%d spills=%llu "
                "rehydrated=%llu\n", p->name, workers, rc, (int)fired,
                (unsigned long long)c->consumer_spills,
                (unsigned long long)c->rehydrated_batches);
            CHECK(fired, "%s W=%u fail: the reserve never failed", p->name,
                workers);
            CHECK(rc == ENOMEM, "%s W=%u fail: rc=%d", p->name, workers, rc);
            CHECK(c->consumer_spills > c->rehydrated_batches,
                "%s W=%u fail: no spilled batch reached the failure path",
                p->name, workers);
            free(got.rows);
        }
    }
}

/* T7: the aligned strategy, whose self-join stratum tdd_exchange_deltas()
 * broadcasts (ordinary worker publication). */
static void
test_aligned_spill(void)
{
    static const run_config_t cfg = { "1", false, true };
    collect_t oracle = { 0 };
    aligned_oracle(&oracle);
    (void)sort_has_duplicate(&oracle);
    for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
        collect_t got = { 0 };
        run_info_t info;
        memset(&info, 0, sizeof(info));
        set_reserve_mode(RESERVE_HOLD_LANE);
        int rc = run_aligned(widths[wi], &cfg, &got, &info);
        set_reserve_mode(RESERVE_PLAIN);
        check_spill_run("aligned", "aligned", widths[wi], rc, &got, &oracle,
            &info);
        free(got.rows);
    }
    free(oracle.rows);
}

/* T7: a STRING relation cannot spill.  Its batches stay resident over the
 * cap, every attempt is counted, and the result is still exact. */
static void
test_string_over_cap(void)
{
    static const run_config_t cfg = { "1", false, true };
    const program_t *p = &programs[0];
    collect_t serial = { 0 };
    run_info_t info;
    memset(&info, 0, sizeof(info));
    int rc = run_program(p, true, 1, NULL, &serial, &info);
    CHECK(rc == 0 && serial.count > 0, "strings: serial rc=%d rows=%zu", rc,
        serial.count);
    (void)sort_has_duplicate(&serial);
    for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
        uint32_t workers = widths[wi];
        collect_t got = { 0 };
        memset(&info, 0, sizeof(info));
        rc = run_program(p, true, workers, &cfg, &got, &info);
        const wl_tdd_consumer_stats_t *c = &info.consumer;
        bool dup = sort_has_duplicate(&got);
        fprintf(stderr, "  strings W=%u: strategy=%s selected=%u spills=%llu "
            "unsupported=%llu over_cap=%llu max_held=%llu rows=%zu/%zu\n",
            workers, info.strategy ? info.strategy : "(none)", info.selected,
            (unsigned long long)c->consumer_spills,
            (unsigned long long)
            c->consumer_spill_fail[WL_COLUMNAR_SPILL_UNSUPPORTED],
            (unsigned long long)c->held_over_cap,
            (unsigned long long)c->max_held_bytes, got.count, serial.count);
        CHECK(rc == 0, "strings W=%u: rc=%d", workers, rc);
        CHECK(info.selected == workers, "strings W=%u: %u selected", workers,
            info.selected);
        CHECK(c->held_over_cap > 0, "strings W=%u: never over the cap",
            workers);
        CHECK(c->consumer_spill_fail[WL_COLUMNAR_SPILL_UNSUPPORTED] > 0,
            "strings W=%u: no refused spill counted", workers);
        CHECK(c->consumer_spills == 0, "strings W=%u: %llu spills", workers,
            (unsigned long long)c->consumer_spills);
        CHECK(c->max_held_bytes > c->held_cap,
            "strings W=%u: max_held %llu not over the cap", workers,
            (unsigned long long)c->max_held_bytes);
        check_held_bound("strings", workers, c);
        check_audit("strings", workers, &info);
        CHECK(!dup, "strings W=%u: duplicate rows", workers);
        CHECK(same_set(&got, &serial), "strings W=%u: %zu rows, serial %zu",
            workers, got.count, serial.count);
        free(got.rows);
    }
    free(serial.rows);
}

/*
 * A snapshot with two recursive TDD strata: tc over the DAG, then u, which
 * joins tc with step edges tc does not hold, so u's one round publishes
 * nothing and stages nothing.  The audit of u, the last stratum, must
 * count only u's own consumer events, which are none, while the
 * snapshot's consumer counts include tc's.  Under a 1-byte held cap tc's
 * batches spill and stage; with a 1-byte spill quota as well none can
 * spill, so tc's cap passes end over the cap.  tc is the only relation of
 * its stratum, so none of its stagings has other relations to spill and
 * stage_over_cap stays 0 throughout.
 */
static void
test_audit_per_stratum(void)
{
    static const run_config_t spilling = { "1", false, true, 0, 0 };
    static const run_config_t starved = { "1", false, true, 0, 1 };
    static const program_t p = { "two-strata",
                                 ".decl step(x: int32, y: int32)\n"
                                 ".decl u(x: int32, y: int32)\n"
                                 "tc(x, z) :- tc(x, y), edge(y, z).\n"
                                 "u(x, y) :- step(x, y), tc(x, y).\n"
                                 "u(x, z) :- u(x, y), step(y, z).\n"
                                 "step(100, 101).\nstep(101, 102).\n",
                                 "owner" };
    collect_t serial = { 0 };
    run_info_t info;
    memset(&info, 0, sizeof(info));
    int rc = run_program(&p, false, 1, NULL, &serial, &info);
    CHECK(rc == 0 && serial.count > 0, "two strata: serial rc=%d rows=%zu",
        rc, serial.count);
    (void)sort_has_duplicate(&serial);
    for (int k = 0; k < 2; k++) {
        bool spill = k == 0;
        const char *kind = spill ? "spilling" : "starved";
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            uint32_t workers = widths[wi];
            collect_t got = { 0 };
            memset(&info, 0, sizeof(info));
            rc = run_program(&p, false, workers, spill ? &spilling : &starved,
                    &got, &info);
            const wl_tdd_consumer_stats_t *c = &info.consumer;
            bool dup = sort_has_duplicate(&got);
            fprintf(stderr, "  two strata %s W=%u: strategy=%s selected=%u "
                "spills=%llu/%llu over_cap=%llu/%llu "
                "stage_over_cap=%llu/%llu stage_peak=%llu/%llu "
                "rows=%zu/%zu\n", kind, workers,
                info.strategy ? info.strategy : "(none)", info.selected,
                (unsigned long long)info.audit_spills,
                (unsigned long long)c->consumer_spills,
                (unsigned long long)info.audit_held_over_cap,
                (unsigned long long)c->held_over_cap,
                (unsigned long long)info.audit_stage_over_cap,
                (unsigned long long)c->stage_over_cap,
                (unsigned long long)info.audit_stage_peak,
                (unsigned long long)c->stage_peak_bytes, got.count,
                serial.count);
            CHECK(rc == 0, "two strata %s W=%u: rc=%d", kind, workers, rc);
            CHECK(info.strategy && strcmp(info.strategy, "owner") == 0
                && info.selected == workers,
                "two strata %s W=%u: last stratum strategy %s on %u workers",
                kind, workers, info.strategy ? info.strategy : "(none)",
                info.selected);
            CHECK(info.audit_spills == 0 && info.audit_held_over_cap == 0
                && info.audit_stage_over_cap == 0
                && info.audit_stage_peak == 0,
                "two strata %s W=%u: u's audit counts tc's events", kind,
                workers);
            if (spill) {
                CHECK(c->consumer_spills > 0 && c->stage_peak_bytes > 0,
                    "two strata %s W=%u: tc did not spill and stage", kind,
                    workers);
            } else {
                CHECK(c->consumer_spills == 0 && c->held_over_cap > 0,
                    "two strata %s W=%u: tc's cap passes were not over the "
                    "cap", kind, workers);
            }
            CHECK(!dup && same_set(&got, &serial),
                "two strata %s W=%u: %zu rows, serial %zu", kind, workers,
                got.count, serial.count);
            free(got.rows);
        }
    }
    free(serial.rows);
}

/* The held cap's defaults and the override's parsing.  An enforcing
 * governor gives a quarter of its budget, an advisory one no cap; only a
 * whole positive decimal that fits in 64 bits overrides either. */
static void
test_held_cap_defaults(void)
{
    static const struct {
        const char *env;
        bool advisory;
        uint64_t want;
    } cases[] = {
        { NULL, false, BUDGET / 4 },
        { NULL, true, UINT64_MAX },
        { "4096", true, 4096 },
        { "4096", false, 4096 },
        { "0", false, BUDGET / 4 },
        { "12x", false, BUDGET / 4 },
        { "-5", false, BUDGET / 4 },
        { " 7", false, BUDGET / 4 },
        { "18446744073709551616", false, BUDGET / 4 },
        { "18446744073709551615", false, UINT64_MAX },
    };
    const program_t *p = &programs[0];
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        run_config_t cfg = { cases[i].env, cases[i].advisory, true };
        collect_t got = { 0 };
        run_info_t info;
        memset(&info, 0, sizeof(info));
        int rc = run_program(p, false, 2, &cfg, &got, &info);
        CHECK(rc == 0, "defaults[%zu]: rc=%d", i, rc);
        CHECK(info.consumer.held_cap == cases[i].want,
            "defaults[%zu] env=%s advisory=%d: cap %llu, expected %llu", i,
            cases[i].env ? cases[i].env : "(unset)", (int)cases[i].advisory,
            (unsigned long long)info.consumer.held_cap,
            (unsigned long long)cases[i].want);
        if (cases[i].want >= BUDGET / 4)
            CHECK(info.consumer.consumer_spills == 0,
                "defaults[%zu]: %llu spills under a large cap", i,
                (unsigned long long)info.consumer.consumer_spills);
        free(got.rows);
    }
}

/* An injected budget below 32 MiB: the channel capacity stops at its
 * 4 MiB floor, and the held cap is still a quarter of the budget.  A budget
 * resolved from the environment or the host is at least 256 MiB, so only
 * an injected governor reaches the floor. */
static void
test_injected_floor(void)
{
    static const run_config_t cfg = { NULL, false, true,
                                      UINT64_C(16) << 20 };
    const program_t *p = &programs[0];
    collect_t serial = { 0 };
    run_info_t info;
    memset(&info, 0, sizeof(info));
    int rc = run_program(p, false, 1, NULL, &serial, &info);
    CHECK(rc == 0 && serial.count > 0, "floor: serial rc=%d", rc);
    (void)sort_has_duplicate(&serial);
    collect_t got = { 0 };
    memset(&info, 0, sizeof(info));
    rc = run_program(p, false, 2, &cfg, &got, &info);
    fprintf(stderr, "  floor: rc=%d selected=%u channel_cap=%llu "
        "held_cap=%llu pumps=%llu\n", rc, info.selected,
        (unsigned long long)info.channel_cap,
        (unsigned long long)info.consumer.held_cap,
        (unsigned long long)info.pumps);
    CHECK(rc == 0, "floor: rc=%d", rc);
    CHECK(info.selected == 2 && info.pumps > 0, "floor: no TDD round ran");
    CHECK(info.channel_cap == UINT64_C(4) << 20,
        "floor: channel capacity %llu, expected 4 MiB",
        (unsigned long long)info.channel_cap);
    CHECK(info.consumer.held_cap == UINT64_C(4) << 20,
        "floor: held cap %llu, expected 4 MiB",
        (unsigned long long)info.consumer.held_cap);
    (void)sort_has_duplicate(&got);
    CHECK(same_set(&got, &serial), "floor: %zu rows, serial has %zu",
        got.count, serial.count);
    free(got.rows);
    free(serial.rows);
}

/* ------------------------------------------------------------------------ */
/* T8: staging one relation at a time                                       */
/* ------------------------------------------------------------------------ */

/* Hook state: the main thread arms the split before a snapshot starts its
 * workers; workers only read it. */
static wl_atomic_u64 split_parts;

static uint32_t
split_hook(wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows,
    bool *overlap)
{
    (void)worker;
    (void)rel_idx;
    (void)rows;
    *overlap = false;
    return (uint32_t)atomic_load_explicit(&split_parts, memory_order_relaxed);
}

/*
 * Three relations in one stratum, each recursive on its own and each fed
 * by the one before it, cyclically, so later rounds deliver batches of all
 * three at once.  The entries without a rule are run_aligned_plan()'s
 * hand-built strata of three relations, which reach the aligned and
 * replicate strategies that none of the planned programs here reaches.
 */
static const program_t mutual[] = {
    { "mutual-linear",
      "a(x, z) :- a(x, y), edge(y, z).\n"
      "b(x, z) :- b(x, y), edge(y, z).\n"
      "c(x, z) :- c(x, y), edge(y, z).\n"
      "b(x, y) :- a(x, y).\nc(x, y) :- b(x, y).\na(x, y) :- c(x, y).\n",
      "owner" },
    { "mutual-two-sided",
      "a(x, z) :- a(x, y), edge(y, z).\na(x, z) :- edge(x, y), a(y, z).\n"
      "b(x, z) :- b(x, y), edge(y, z).\nb(x, z) :- edge(x, y), b(y, z).\n"
      "c(x, z) :- c(x, y), edge(y, z).\nc(x, z) :- edge(x, y), c(y, z).\n"
      "b(x, y) :- a(x, y).\nc(x, y) :- b(x, y).\na(x, y) :- c(x, y).\n",
      "global_read" },
    { "mutual-non-linear",
      "a(x, z) :- a(x, y), a(y, z).\n"
      "b(x, z) :- b(x, y), edge(y, z).\n"
      "c(x, z) :- c(x, y), edge(y, z).\n"
      "b(x, y) :- a(x, y).\nc(x, y) :- b(x, y).\na(x, y) :- c(x, y).\n",
      "bdx" },
    { "mutual-self-join", NULL, "aligned", false, false },
    { "mutual-scatter", NULL, "aligned", true, false },
    { "mutual-replicate", NULL, "replicate", false, true },
};

static const char *const mutual_relations[] = { "a", "b", "c" };

/* The result @p must reach: the serial (W=1) result, or for a hand-built
 * stratum the closure every relation of it reaches. */
static int run_mutual(const program_t *p, bool strings, uint32_t workers,
    const run_config_t *cfg, collect_t *out, run_info_t *info);

static int
mutual_expected(const program_t *p, bool strings, collect_t *out)
{
    run_info_t info;
    memset(&info, 0, sizeof(info));
    if (!p->rule) {
        aligned_oracle_for(out, mutual_relations, 3, p->linear);
        return 0;
    }
    wl_columnar_eval_test_channel_split = NULL;
    return run_mutual(p, strings, 1, NULL, out, &info);
}

/* Evaluate the mutual program @p, over STRING columns with @strings (only
 * with a rule), and collect a, b and c. */
static int
run_mutual(const program_t *p, bool strings, uint32_t workers,
    const run_config_t *cfg, collect_t *out, run_info_t *info)
{
    if (!p->rule)
        return run_aligned_plan(mutual_relations, 3, p->linear, p->replicate,
                   workers, cfg, out, info);
    static char src[8192];
    const char *type = strings ? "string" : "int32";
    snprintf(src, sizeof(src),
        ".decl edge(x: %s, y: %s)\n"
        ".decl a(x: %s, y: %s)\n"
        ".decl b(x: %s, y: %s)\n"
        ".decl c(x: %s, y: %s)\n"
        "a(x, y) :- edge(x, y).\n%s", type, type, type, type, type, type,
        type, type, p->rule);
    append_edges(src, sizeof(src), strings);
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
    wl_columnar_memory_governor_ref_t *g = NULL;
    wl_session_t *session = NULL;
    rc = open_session(plan, workers, cfg, &g, &session);
    if (rc == 0)
        rc = wl_session_load_facts(session, prog);
    out->relations = mutual_relations;
    out->nrelations = 3;
    if (rc == 0)
        rc = wl_session_snapshot(session, collect_rows, out);
    if (session) {
        note_run(session, info);
        close_session(session);
    }
    wl_plan_free(plan);
    plan_fixture_release();
    release_governor(g);
    if (rc == 0 && out->bad)
        rc = EIO;
    return rc;
}

/* Stage hook state: the main thread arms it before a snapshot, and the
 * hooks run on the coordinator, which is the main thread. */
static int stage_fault_rc;
static bool stage_fault_fired;
static uint32_t stage_fault_workers;
static uint64_t stage_deltas_seen;     /* workers with rows in $d$a */
static uint64_t stage_deltas_governed; /* of those, governed ones */
static uint64_t stage_parts_seen;      /* partitions the exchanges made */
static uint64_t stage_parts_governed;  /* of those, under the governor */

extern void (*wl_columnar_eval_test_after_govern_parts)(wl_col_session_t *,
    col_rel_t *const *, uint32_t);

/* Count the partitions an exchange has put under the governor, and those
 * that carry the session's. */
static void
stage_parts_hook(wl_col_session_t *coord, col_rel_t *const *parts,
    uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        if (!parts[i])
            continue;
        stage_parts_seen++;
        if (parts[i]->memory_governor == coord->memory_governor)
            stage_parts_governed++;
    }
}

/* When relation 1 is staged, every exchange but the owner one has
 * installed relation a's new delta on the workers that receive rows; owner
 * mode publishes at its commit, so its workers still hold the previous
 * round's.  Count the non-empty deltas and those that carry a governor. */
static int
stage_observe_hook(wl_col_session_t *coord, uint32_t ri)
{
    if (ri != 1)
        return 0;
    for (uint32_t w = 0; w < coord->tdd_workers_count; w++) {
        const col_rel_t *d = session_find_rel(&coord->tdd_workers[w], "$d$a");
        if (!d || d->nrows == 0)
            continue;
        stage_deltas_seen++;
        if (d->memory_governor)
            stage_deltas_governed++;
    }
    return 0;
}

/* The bound a staging run keeps: what one staging holds is at most the cap
 * plus the slots staged for the relation itself, unless stage_over_cap
 * counts a staging. */
static void
check_stage_bound(const char *name, uint32_t workers,
    const wl_tdd_consumer_stats_t *c)
{
    CHECK(c->stage_peak_bytes <= c->held_cap + c->max_stage_bytes
        || c->stage_over_cap > 0,
        "%s W=%u: stage_peak=%llu > cap %llu + max_stage %llu", name,
        workers, (unsigned long long)c->stage_peak_bytes,
        (unsigned long long)c->held_cap,
        (unsigned long long)c->max_stage_bytes);
}

/*
 * T8: with every delta of three or more rows published as three batches
 * (a smaller one as one batch a row), staging a relation builds fresh
 * slots and consolidates them, which needs room the held bytes of the
 * other relations must give up.  A calibration run with no
 * effective cap measures the most bytes one relation's staged slots took;
 * the measured run caps the held bytes at twice that, so the cap does not
 * depend on the platform's charges.  Staging must then spill the other
 * relations down, keep its peak within the cap plus one relation's staged
 * slots with no staging over the cap, release owner mode's private slots
 * before the commit, and leave the results exact, in the owner,
 * global-read, BDX, self-join broadcast, hash scatter and replicate
 * exchanges.  The four that partition a relation, all but the self-join
 * broadcast and the replicate exchange, must put each partition under the
 * session's governor.
 */
static void
test_stage_bound(void)
{
    char held[32];
    for (size_t i = 0; i < sizeof(mutual) / sizeof(mutual[0]); i++) {
        const program_t *p = &mutual[i];
        collect_t serial = { 0 };
        run_info_t info;
        int rc = mutual_expected(p, false, &serial);
        CHECK(rc == 0 && serial.count > 0, "%s: serial rc=%d rows=%zu",
            p->name, rc, serial.count);
        (void)sort_has_duplicate(&serial);
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            uint32_t workers = widths[wi];
            atomic_store_explicit(&split_parts, 3, memory_order_relaxed);
            wl_columnar_eval_test_channel_split = split_hook;

            run_config_t cfg = { "1099511627776", false, true };
            collect_t got = { 0 };
            memset(&info, 0, sizeof(info));
            rc = run_mutual(p, false, workers, &cfg, &got, &info);
            const wl_tdd_consumer_stats_t *c = &info.consumer;
            uint64_t stage = c->max_stage_bytes;
            (void)sort_has_duplicate(&got);
            CHECK(rc == 0 && c->consumer_spills == 0 && stage > 0,
                "%s W=%u calibration: rc=%d spills=%llu max_stage=%llu",
                p->name, workers, rc, (unsigned long long)c->consumer_spills,
                (unsigned long long)stage);
            CHECK(same_set(&got, &serial),
                "%s W=%u calibration: %zu rows, serial %zu", p->name,
                workers, got.count, serial.count);
            free(got.rows);

            snprintf(held, sizeof(held), "%llu",
                (unsigned long long)(2 * stage));
            cfg.held_env = held;
            memset(&got, 0, sizeof(got));
            memset(&info, 0, sizeof(info));
            stage_deltas_seen = 0;
            stage_deltas_governed = 0;
            stage_parts_seen = 0;
            stage_parts_governed = 0;
            wl_columnar_eval_test_after_stage = stage_observe_hook;
            wl_columnar_eval_test_after_govern_parts = stage_parts_hook;
            rc = run_mutual(p, false, workers, &cfg, &got, &info);
            wl_columnar_eval_test_after_stage = NULL;
            wl_columnar_eval_test_after_govern_parts = NULL;
            wl_columnar_eval_test_channel_split = NULL;
            bool dup = sort_has_duplicate(&got);
            fprintf(stderr, "  %s W=%u: strategy=%s selected=%u cap=%s "
                "spills=%llu stage_spills=%llu over_cap=%llu "
                "stage_peak=%llu max_stage=%llu private=%llu/%llu "
                "deltas=%llu/%llu parts=%llu/%llu rows=%zu/%zu\n", p->name,
                workers,
                info.strategy ? info.strategy : "(none)", info.selected,
                held, (unsigned long long)c->consumer_spills,
                (unsigned long long)c->stage_spills,
                (unsigned long long)c->stage_over_cap,
                (unsigned long long)c->stage_peak_bytes,
                (unsigned long long)c->max_stage_bytes,
                (unsigned long long)c->owner_private_released,
                (unsigned long long)c->owner_private_at_commit,
                (unsigned long long)stage_deltas_governed,
                (unsigned long long)stage_deltas_seen,
                (unsigned long long)stage_parts_governed,
                (unsigned long long)stage_parts_seen, got.count,
                serial.count);
            CHECK(rc == 0, "%s W=%u: rc=%d", p->name, workers, rc);
            CHECK(info.selected == workers && info.strategy
                && strcmp(info.strategy, p->strategy) == 0,
                "%s W=%u: strategy %s on %u workers, expected %s", p->name,
                workers, info.strategy ? info.strategy : "(none)",
                info.selected, p->strategy);
            CHECK(c->stage_spills > 0, "%s W=%u: staging never spilled",
                p->name, workers);
            CHECK(c->stage_peak_bytes > 0, "%s W=%u: no staging recorded",
                p->name, workers);
            /* The spill-down spills only what its target needs: some
             * staging held more than its own slots, so other relations'
             * batches (or, in owner mode, their slots) stayed resident. */
            CHECK(c->stage_peak_bytes > c->max_stage_bytes,
                "%s W=%u: no staging held more than its own slots",
                p->name, workers);
            CHECK(c->stage_over_cap == 0, "%s W=%u: %llu stagings over the cap",
                p->name, workers, (unsigned long long)c->stage_over_cap);
            check_stage_bound(p->name, workers, c);
            check_held_bound(p->name, workers, c);
            check_audit(p->name, workers, &info);
            /* The exchanges create the deltas they install under the
             * governor, except the shared views a broadcast installs next
             * to the one relation it builds; owner mode's deltas are the
             * candidates its commit publishes. */
            bool owner = strcmp(p->strategy, "owner") == 0;
            CHECK(owner || stage_deltas_governed > 0,
                "%s W=%u: no installed delta is governed", p->name, workers);
            bool partitions = strcmp(p->strategy, "replicate") != 0
                && (strcmp(p->strategy, "aligned") != 0 || p->linear);
            CHECK(!partitions || (stage_parts_seen > 0
                && stage_parts_governed == stage_parts_seen),
                "%s W=%u: %llu of %llu exchange partitions governed",
                p->name, workers, (unsigned long long)stage_parts_governed,
                (unsigned long long)stage_parts_seen);
            CHECK(!(p->linear || strcmp(p->strategy, "global_read") == 0)
                || stage_deltas_governed == stage_deltas_seen,
                "%s W=%u: %llu of %llu installed deltas governed", p->name,
                workers, (unsigned long long)stage_deltas_governed,
                (unsigned long long)stage_deltas_seen);
            if (info.strategy && strcmp(info.strategy, "owner") == 0) {
                CHECK(c->owner_private_released > 0,
                    "%s W=%u: no private slot released", p->name, workers);
                CHECK(c->owner_private_at_commit == 0,
                    "%s W=%u: %llu private slots reached the commit",
                    p->name, workers,
                    (unsigned long long)c->owner_private_at_commit);
            }
            CHECK(!dup, "%s W=%u: duplicate rows", p->name, workers);
            CHECK(same_set(&got, &serial), "%s W=%u: %zu rows, serial %zu",
                p->name, workers, got.count, serial.count);
            free(got.rows);
        }
        free(serial.rows);
    }
}

/* The spill attempts that failed, whatever their status. */
static uint64_t
failed_spills(const wl_tdd_consumer_stats_t *c)
{
    uint64_t n = 0;
    for (size_t i = 0; i < WL_TDD_SPILL_STATUS_SLOTS; i++)
        n += c->consumer_spill_fail[i];
    return n;
}

/*
 * Owner mode keeps the slots it adopted until its commit, and they count
 * against the spill-down of every relation staged after them.  With every
 * delta published as one batch, staging adopts each batch still resident.
 * One run caps the held bytes at twice the most one relation's staged
 * slots took in an uncapped run: some slots are adopted, the staging bound
 * holds, and no private slot reaches the commit.  Another caps them at 1
 * byte with a spill quota of 1 byte, so no batch can spill and the held
 * batches and adopted slots stay over the cap: stage_over_cap must count
 * those stagings, and the results stay exact.  test_tdd_channel_spill
 * checks the staging whose other relations' slots alone exceed the cap.
 */
static void
test_stage_adopted(void)
{
    const program_t *p = &mutual[0];
    char held[32];
    wl_columnar_eval_test_channel_split = NULL;
    collect_t serial = { 0 };
    int rc = mutual_expected(p, false, &serial);
    CHECK(rc == 0 && serial.count > 0, "adopted: serial rc=%d rows=%zu", rc,
        serial.count);
    (void)sort_has_duplicate(&serial);
    for (int s = 0; s < 2; s++) {
        bool starved = s == 1;
        const char *kind = starved ? "starved" : "capped";
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            uint32_t workers = widths[wi];
            run_config_t cfg = { "1", false, true, 0, starved ? 1u : 0u };
            collect_t got = { 0 };
            run_info_t info;
            if (!starved) {
                cfg.held_env = "1099511627776";
                memset(&info, 0, sizeof(info));
                rc = run_mutual(p, false, workers, &cfg, &got, &info);
                uint64_t stage = info.consumer.max_stage_bytes;
                CHECK(rc == 0 && stage > 0,
                    "adopted %s W=%u calibration: rc=%d max_stage=%llu", kind,
                    workers, rc, (unsigned long long)stage);
                free(got.rows);
                memset(&got, 0, sizeof(got));
                snprintf(held, sizeof(held), "%llu",
                    (unsigned long long)(2 * stage));
                cfg.held_env = held;
            }
            memset(&info, 0, sizeof(info));
            rc = run_mutual(p, false, workers, &cfg, &got, &info);
            const wl_tdd_consumer_stats_t *c = &info.consumer;
            bool dup = sort_has_duplicate(&got);
            fprintf(stderr, "  adopted %s W=%u: strategy=%s cap=%s "
                "adopted=%llu spills=%llu failed=%llu stage_spills=%llu "
                "stage_over_cap=%llu stage_peak=%llu max_stage=%llu "
                "private=%llu/%llu rows=%zu/%zu\n", kind, workers,
                info.strategy ? info.strategy : "(none)", cfg.held_env,
                (unsigned long long)c->stage_adopted,
                (unsigned long long)c->consumer_spills,
                (unsigned long long)failed_spills(c),
                (unsigned long long)c->stage_spills,
                (unsigned long long)c->stage_over_cap,
                (unsigned long long)c->stage_peak_bytes,
                (unsigned long long)c->max_stage_bytes,
                (unsigned long long)c->owner_private_released,
                (unsigned long long)c->owner_private_at_commit, got.count,
                serial.count);
            CHECK(rc == 0, "adopted %s W=%u: rc=%d", kind, workers, rc);
            CHECK(info.strategy && strcmp(info.strategy, "owner") == 0,
                "adopted %s W=%u: strategy %s", kind, workers,
                info.strategy ? info.strategy : "(none)");
            CHECK(c->stage_adopted > 0, "adopted %s W=%u: nothing adopted",
                kind, workers);
            check_stage_bound(kind, workers, c);
            check_audit(kind, workers, &info);
            CHECK(c->owner_private_at_commit == 0,
                "adopted %s W=%u: %llu private slots reached the commit", kind,
                workers, (unsigned long long)c->owner_private_at_commit);
            CHECK(!starved || (c->consumer_spills == 0
                && failed_spills(c) > 0 && c->stage_over_cap > 0),
                "adopted %s W=%u: spills=%llu failed=%llu stage_over_cap=%llu",
                kind, workers, (unsigned long long)c->consumer_spills,
                (unsigned long long)failed_spills(c),
                (unsigned long long)c->stage_over_cap);
            CHECK(!dup, "adopted %s W=%u: duplicate rows", kind, workers);
            CHECK(same_set(&got, &serial),
                "adopted %s W=%u: %zu rows, serial %zu", kind, workers,
                got.count, serial.count);
            free(got.rows);
        }
    }
    free(serial.rows);
}

/* Fail the staging of relation 1 once, while relation 2's lists still
 * hold a spilled batch. */
static int
stage_fault_hook(wl_col_session_t *coord, uint32_t ri)
{
    uint32_t workers = stage_fault_workers;
    if (stage_fault_fired || ri != 1 || !coord->tdd_channel_lists
        || coord->tdd_channel_list_count != (size_t)workers * 3u)
        return 0;
    bool spilled = false;
    for (uint32_t w = 0; w < workers && !spilled; w++)
        for (const wl_tdd_batch_t *b =
            coord->tdd_channel_lists[(size_t)2u * workers + w];
            b && !spilled; b = b->next)
            spilled = b->kind == WL_TDD_BATCH_SPILLED;
    if (!spilled)
        return 0;
    stage_fault_fired = true;
    return stage_fault_rc;
}

/*
 * An exchange that fails between the relations it stages: with every batch
 * spilled as it is taken, the staging of relation 1 fails once, with its
 * staged slots in the matrix, while relation 2's lists still hold spilled
 * batches that no staging will read.  The stratum must report the
 * failure, and the batches must go down the failure path, so more batches
 * were spilled than read back.  Releasing them and the slots is checked by
 * the governor returning to 0 after the session, by the spill directory
 * ending empty and, under ASan, by the leak check; in owner mode they go
 * through the lifetime record.  In global-read mode an EOVERFLOW replays
 * the stratum serially instead, which must give the expected result.
 */
static void
test_stage_fault(void)
{
    static const run_config_t cfg = { "1", false, true };
    static const int faults[] = { ENOMEM, EOVERFLOW };
    for (size_t i = 0; i < sizeof(mutual) / sizeof(mutual[0]); i++) {
        const program_t *p = &mutual[i];
        collect_t expected = { 0 };
        int rc = mutual_expected(p, false, &expected);
        CHECK(rc == 0 && expected.count > 0, "%s: expected rc=%d rows=%zu",
            p->name, rc, expected.count);
        (void)sort_has_duplicate(&expected);
        bool global = strcmp(p->strategy, "global_read") == 0;
        for (size_t wi = 0; wi < sizeof(widths) / sizeof(widths[0]); wi++) {
            uint32_t workers = widths[wi];
            for (size_t k = 0; k < sizeof(faults) / sizeof(faults[0]); k++) {
                int injected = faults[k];
                atomic_store_explicit(&split_parts, 3, memory_order_relaxed);
                wl_columnar_eval_test_channel_split = split_hook;
                stage_fault_rc = injected;
                stage_fault_fired = false;
                stage_fault_workers = workers;
                wl_columnar_eval_test_after_stage = stage_fault_hook;
                collect_t got = { 0 };
                run_info_t info;
                memset(&info, 0, sizeof(info));
                rc = run_mutual(p, false, workers, &cfg, &got, &info);
                wl_columnar_eval_test_after_stage = NULL;
                wl_columnar_eval_test_channel_split = NULL;
                const wl_tdd_consumer_stats_t *c = &info.consumer;
                bool replay = global && injected == EOVERFLOW;
                fprintf(stderr, "  %s W=%u fault=%d: rc=%d strategy=%s "
                    "replay=%s spills=%llu rehydrated=%llu rows=%zu\n",
                    p->name, workers, injected, rc,
                    info.strategy ? info.strategy : "(none)",
                    info.replay ? info.replay : "(none)",
                    (unsigned long long)c->consumer_spills,
                    (unsigned long long)c->rehydrated_batches, got.count);
                CHECK(stage_fault_fired, "%s W=%u fault=%d: never fired",
                    p->name, workers, injected);
                CHECK(info.strategy && strcmp(info.strategy, p->strategy) == 0,
                    "%s W=%u fault=%d: strategy %s", p->name, workers,
                    injected, info.strategy ? info.strategy : "(none)");
                CHECK(c->consumer_spills > c->rehydrated_batches,
                    "%s W=%u fault=%d: no spilled batch reached the failure "
                    "path", p->name, workers, injected);
                if (replay) {
                    CHECK(rc == 0 && info.replay
                        && strcmp(info.replay, "global_read_overflow") == 0,
                        "%s W=%u fault=%d: rc=%d replay=%s", p->name, workers,
                        injected, rc, info.replay ? info.replay : "(none)");
                    bool dup = sort_has_duplicate(&got);
                    CHECK(!dup && same_set(&got, &expected),
                        "%s W=%u fault=%d: %zu rows, expected %zu", p->name,
                        workers, injected, got.count, expected.count);
                } else {
                    CHECK(rc == injected, "%s W=%u fault=%d: rc=%d",
                        p->name, workers, injected, rc);
                }
                free(got.rows);
            }
        }
        free(expected.rows);
    }
}

/* ------------------------------------------------------------------------ */

typedef struct {
    const char *name;
    void (*fn)(void);
} test_case_t;

static const test_case_t tests[] = {
    { "numeric_spill", test_numeric_spill },
    { "mixed_spill", test_mixed_spill },
    { "spill_then_fail", test_spill_then_fail },
    { "aligned_spill", test_aligned_spill },
    { "string_over_cap", test_string_over_cap },
    { "audit_per_stratum", test_audit_per_stratum },
    { "held_cap_defaults", test_held_cap_defaults },
    { "injected_floor", test_injected_floor },
    { "stage_bound", test_stage_bound },
    { "stage_adopted", test_stage_adopted },
    { "stage_fault", test_stage_fault },
};

static int
make_spill_dir(void)
{
    const char *base = getenv("WL_TDD_CHANNEL_SPILL_DIR");
    if (!base || !base[0])
        base = ".";
#ifdef _WIN32
    snprintf(spill_dir, sizeof(spill_dir), "%s/tdd-channel-held-%lu", base,
        (unsigned long)GetCurrentProcessId());
    return _mkdir(spill_dir) == 0 ? 0 : -1;
#else
    snprintf(spill_dir, sizeof(spill_dir), "%s/tdd-channel-held-XXXXXX",
        base);
    return mkdtemp(spill_dir) ? 0 : -1;
#endif
}

static int
remove_spill_dir(void)
{
    /* Fails if any spill segment was left behind. */
#ifdef _WIN32
    return _rmdir(spill_dir);
#else
    return rmdir(spill_dir);
#endif
}

int
main(int argc, char **argv)
{
    const char *only = argc > 1 ? argv[1] : NULL;
    unsigned ran = 0;
    if (make_spill_dir() != 0) {
        fprintf(stderr, "cannot create a spill directory under %s\n",
            spill_dir);
        return EXIT_FAILURE;
    }
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    wl_columnar_eval_test_before_pump = pump_hook;
    wl_columnar_eval_test_channel_reserve = reserve_hook;
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        if (only && strcmp(only, tests[i].name) != 0)
            continue;
        fprintf(stderr, "%s\n", tests[i].name);
        tests[i].fn();
        ran++;
    }
    wl_columnar_eval_test_before_pump = NULL;
    wl_columnar_eval_test_channel_reserve = NULL;
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_TDD_STRATUM_PROFILE");
    CHECK(remove_spill_dir() == 0, "spill directory %s not empty",
        spill_dir);
    if (ran == 0) {
        fprintf(stderr, "no test named %s\n", only ? only : "(none)");
        return EXIT_FAILURE;
    }
    fprintf(stderr, "tdd_channel_held: %u failure(s)\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
