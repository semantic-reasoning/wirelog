/*
 * test_tdd_channel_round.c - TDD channel round protocol at session level
 * (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * The coordinator pumps the channel while its workers produce.  These tests
 * drive real recursive TDD strata through test hooks and check how a round
 * ends:
 *
 * - rc selection: a worker error alone does not cancel the round and is
 *   the round's rc; a forced pump error is the round's rc rather than the
 *   ECANCELED of a producer that only saw its cancel; and when a pump error
 *   cancels a round in which the last worker has already failed, that
 *   worker's error outranks both the pump error and the ECANCELED of the
 *   lower-numbered workers that were waiting for space;
 * - I1: a round the driver leaves without joining is cancelled by the
 *   teardown path before it drains the workqueue, so a producer that waits
 *   on the channel returns;
 * - a finite capacity: its default and override, backpressure with exact
 *   results at W=2 and W=8, a heavy lane that waits for space while the
 *   light lanes close, and a cancel that releases waiting producers.
 *
 * Every wait in a hook is condition based: it waits on channel state
 * through wl_tdd_channel_test_wait() with a deadline, and an expired
 * deadline is recorded as a failure instead of hanging.
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
#include <time.h>

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

extern bool (*wl_columnar_eval_test_abandon_round)(wl_col_session_t *);
extern void (*wl_columnar_eval_test_subpass_boundary)(wl_col_session_t *,
    uint32_t, bool);

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

/* A hook's wait for channel state.  Long enough for a loaded sanitizer
 * runner; an expiry is a failure, not a hang. */
#define WAIT_MS 10000u

/* ------------------------------------------------------------------------ */
/* Hook state.  The main thread sets the parameters before a snapshot
 * starts its workers and reads the results after it returns.  Worker
 * threads write only the atomics below; the coordinator, which is the
 * thread that runs the snapshot, also writes the coordinator-only state. */
/* ------------------------------------------------------------------------ */

typedef enum {
    RESERVE_NONE = 0,
    RESERVE_FAIL_LAST_WORKER, /* the last worker's first reserve fails */
    RESERVE_WAIT_CANCEL,      /* the first reserve waits for a cancel */
    RESERVE_HEAVY,            /* the first reserver waits for the others */
    RESERVE_FAIL_BLOCKED,     /* the first reserve fails once one waits */
    RESERVE_FAIL_LAST_BLOCKED, /* likewise, for the last worker only */
} reserve_mode_t;

typedef enum {
    PUMP_NONE = 0,
    PUMP_FAIL_ONCE,      /* fail the first pump once a producer waits */
    PUMP_WAIT_BLOCKED,   /* each round, hold back until a producer blocks */
    PUMP_SKEW,           /* hold back once only the heavy lane is open */
    PUMP_CANCEL_BLOCKED, /* fail the first pump once producers block */
    PUMP_WAIT_FAILED,    /* hold back until the failing worker failed */
    PUMP_FAIL_FAILED,    /* fail the first pump after the worker failed */
} pump_mode_t;

typedef enum {
    ABANDON_AFTER_PUBLISH = 0, /* once a producer waits in the reserve hook */
    ABANDON_AFTER_BLOCK,       /* once a producer waits for space */
} abandon_mode_t;

static reserve_mode_t reserve_mode;
static pump_mode_t pump_mode;
static abandon_mode_t abandon_mode;
static uint32_t reserve_fail_worker;
static int reserve_fail_rc;
static int pump_fail_rc;

/* Bumped on worker threads. */
static wl_atomic_u64 reserve_fired;
static wl_atomic_u64 reserve_saw_cancel;
static wl_atomic_u64 reserve_timeouts;
static wl_atomic_u64 heavy_worker; /* worker id + 1, 0 until chosen */
static wl_atomic_u64 heavy_round;  /* current_round when it was chosen */
static wl_atomic_u64 worker_failed;

/* Bumped on the coordinator, which is the thread that runs the snapshot. */
static wl_atomic_u64 pump_calls;
static wl_atomic_u64 abandon_calls;
static wl_atomic_u64 publish_timeouts;
static wl_atomic_u64 pump_timeouts;

/* Written by the coordinator at each round's start, read by workers. */
static wl_atomic_u64 current_round;

/* Coordinator-only state, read by the main thread after the snapshot. */
static bool round_started;    /* set at each round's start */
static uint64_t round_blocked_base; /* blocked_waits at the round's start */
static uint32_t queue_threads; /* the workqueue's real thread count */
static bool skew_paused;
static bool skew_heavy_blocked; /* the heavy lane waited, the rest closed */
static uint64_t hold_blocked;   /* producers waiting when a hold ended */
static bool final_valid;
static wl_tdd_channel_stats_t final_stats; /* after the last joined round */

static uint64_t
load(wl_atomic_u64 *counter)
{
    return atomic_load_explicit(counter, memory_order_relaxed);
}

static void
bump(wl_atomic_u64 *counter)
{
    atomic_fetch_add_explicit(counter, 1, memory_order_relaxed);
}

/* True for the first caller only. */
static bool
first(wl_atomic_u64 *counter)
{
    return atomic_fetch_add_explicit(counter, 1, memory_order_relaxed) == 0;
}

static bool
pred_cancelled(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->cancelled;
}

static bool
pred_published(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->published >= 1;
}

static bool
pred_space_waiter(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->space_waiters >= 1;
}

/* Only the caller's own lane is still open. */
static bool
pred_others_closed(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->lanes_open == 1;
}

static bool
pred_blocked_since(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    const uint64_t *base = ctx;
    return stats->blocked_waits > *base || stats->lanes_open == 0;
}

static bool
pred_heavy_blocked(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->space_waiters >= 1 || stats->lanes_open == 0;
}

/* Every producer that can run waits for space: as many wait as there are
 * open lanes, or as the workqueue has threads, whichever is smaller. */
static bool
pred_all_blocked(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    uint32_t threads = *(const uint32_t *)ctx;
    uint32_t runnable = stats->lanes_open < threads ? stats->lanes_open
        : threads;
    return stats->space_waiters >= 1 && stats->space_waiters >= runnable;
}

static bool
pred_worker_failed(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)stats;
    (void)ctx;
    return load(&worker_failed) != 0;
}

/* The failing worker has failed and every lane still open waits for space.
 * Its own lane never waits for space, so it has closed by then. */
static bool
pred_failed_rest_blocked(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return load(&worker_failed) != 0 && stats->space_waiters >= 1
           && stats->space_waiters == stats->lanes_open;
}

/* Wait on @ch for @pred; an expiry bumps @timeouts. */
static void
wait_on(wl_tdd_channel_t *ch,
    bool (*pred)(const wl_tdd_channel_stats_t *, void *), void *ctx,
    wl_atomic_u64 *timeouts)
{
    if (wl_tdd_channel_test_wait(ch, pred, ctx, WAIT_MS) != 0)
        bump(timeouts);
}

/* The first call waits until a producer waits for space, then fails with
 * reserve_fail_rc; later calls reserve normally. */
static int
fail_once_blocked(wl_tdd_channel_t *ch)
{
    if (!first(&reserve_fired))
        return 0;
    wait_on(ch, pred_space_waiter, NULL, &reserve_timeouts);
    atomic_store_explicit(&worker_failed, 1, memory_order_relaxed);
    return reserve_fail_rc;
}

static int
reserve_hook(wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows)
{
    (void)rel_idx;
    wl_tdd_channel_t *ch = worker->coordinator->tdd_channel;
    switch (reserve_mode) {
    case RESERVE_FAIL_LAST_WORKER:
        if (worker->worker_id != reserve_fail_worker || !first(&reserve_fired))
            return 0;
        return reserve_fail_rc;
    case RESERVE_WAIT_CANCEL:
        if (!first(&reserve_fired))
            return 0;
        if (wl_tdd_channel_test_wait(ch, pred_cancelled, NULL, WAIT_MS) == 0)
            bump(&reserve_saw_cancel);
        else
            bump(&reserve_timeouts);
        return 0;
    case RESERVE_HEAVY:
        /* The first delta that splits into the most batches makes its
         * worker the heavy one, in its round. */
        if (rows < WL_TDD_CHANNEL_TEST_SPLIT_MAX || !first(&reserve_fired))
            return 0;
        atomic_store_explicit(&heavy_round,
            atomic_load_explicit(&current_round, memory_order_relaxed),
            memory_order_relaxed);
        atomic_store_explicit(&heavy_worker, (uint64_t)worker->worker_id + 1u,
            memory_order_relaxed);
        wait_on(ch, pred_others_closed, NULL, &reserve_timeouts);
        return 0;
    case RESERVE_FAIL_BLOCKED:
        return fail_once_blocked(ch);
    case RESERVE_FAIL_LAST_BLOCKED:
        return worker->worker_id == reserve_fail_worker
               ? fail_once_blocked(ch) : 0;
    default:
        return 0;
    }
}

/* The heavy worker publishes each delta as the most batches it may. */
static uint32_t
split_hook(wl_col_session_t *worker, uint32_t rel_idx, uint32_t rows,
    bool *overlap)
{
    (void)rel_idx;
    (void)rows;
    *overlap = false;
    return load(&heavy_worker) == (uint64_t)worker->worker_id + 1u
           ? WL_TDD_CHANNEL_TEST_SPLIT_MAX : 0;
}

/*
 * Wait until a batch has been published in @ch's round.  With the reserve
 * hook in RESERVE_WAIT_CANCEL mode, the first producer to reserve stops in
 * the hook before it publishes, so a publication proves that producer is
 * already waiting there: a cancel issued now finds it running.
 */
static void
wait_for_waiting_producer(wl_tdd_channel_t *ch)
{
    wait_on(ch, pred_published, NULL, &publish_timeouts);
}

static int
pump_hook(wl_tdd_channel_t *ch)
{
    wl_tdd_channel_stats_t stats;
    switch (pump_mode) {
    case PUMP_FAIL_ONCE:
        /* The first pump reports pump_fail_rc once a producer waits in the
         * reserve hook. */
        if (!first(&pump_calls))
            return 0;
        wait_for_waiting_producer(ch);
        return pump_fail_rc;
    case PUMP_WAIT_BLOCKED:
        /* The first pump of each round holds back until a reservation made
         * since the round started has waited for space, or every lane has
         * closed. */
        if (!round_started)
            return 0;
        round_started = false;
        wait_on(ch, pred_blocked_since, &round_blocked_base, &pump_timeouts);
        return 0;
    case PUMP_SKEW:
        /* Once only the heavy lane is open in the heavy worker's round,
         * hold back until it waits for space or closes. */
        if (skew_paused || load(&heavy_worker) == 0
            || load(&heavy_round) != load(&current_round))
            return 0;
        wl_tdd_channel_stats(ch, &stats);
        if (stats.lanes_open != 1)
            return 0;
        skew_paused = true;
        wait_on(ch, pred_heavy_blocked, NULL, &pump_timeouts);
        wl_tdd_channel_stats(ch, &stats);
        skew_heavy_blocked = stats.space_waiters >= 1
            && stats.lanes_open == 1;
        return 0;
    case PUMP_CANCEL_BLOCKED:
        /* The first pump reports pump_fail_rc once every producer that can
         * run waits for space. */
        if (!first(&pump_calls))
            return 0;
        wait_on(ch, pred_all_blocked, &queue_threads, &pump_timeouts);
        wl_tdd_channel_stats(ch, &stats);
        hold_blocked = stats.space_waiters;
        return pump_fail_rc;
    case PUMP_WAIT_FAILED:
        /* The first pump holds back until the failing worker has failed. */
        if (!first(&pump_calls))
            return 0;
        wait_on(ch, pred_worker_failed, NULL, &pump_timeouts);
        wl_tdd_channel_stats(ch, &stats);
        hold_blocked = stats.space_waiters;
        return 0;
    case PUMP_FAIL_FAILED:
        /* The first pump reports pump_fail_rc once the failing worker's
         * lane has closed and every other open lane waits for space. */
        if (!first(&pump_calls))
            return 0;
        wait_on(ch, pred_failed_rest_blocked, NULL, &pump_timeouts);
        wl_tdd_channel_stats(ch, &stats);
        hold_blocked = stats.space_waiters;
        return pump_fail_rc;
    default:
        return 0;
    }
}

/* The first dispatched round is left without its pump or join, once a
 * producer waits in the reserve hook or for space. */
static bool
abandon_hook(wl_col_session_t *coord)
{
    if (!first(&abandon_calls))
        return false;
    if (abandon_mode == ABANDON_AFTER_BLOCK) {
        wl_tdd_channel_stats_t stats;
        wait_on(coord->tdd_channel, pred_space_waiter, NULL,
            &publish_timeouts);
        wl_tdd_channel_stats(coord->tdd_channel, &stats);
        hold_blocked = stats.space_waiters;
    } else {
        wait_for_waiting_producer(coord->tdd_channel);
    }
    return true;
}

static void
boundary_hook(wl_col_session_t *coord, uint32_t eff_iter, bool before)
{
    (void)eff_iter;
    if (before) {
        /* No task of the round has been submitted yet. */
        round_started = true;
        bump(&current_round);
        queue_threads = coord->wq ? wl_workqueue_thread_count(coord->wq) : 0;
        if (coord->tdd_channel) {
            wl_tdd_channel_stats_t stats;
            wl_tdd_channel_stats(coord->tdd_channel, &stats);
            round_blocked_base = stats.blocked_waits;
        }
        return;
    }
    if (coord->tdd_channel) {
        wl_tdd_channel_stats(coord->tdd_channel, &final_stats);
        final_valid = true;
    }
}

/* Unhook everything; the counters keep their values for the checks. */
static void
hooks_disarm(void)
{
    wl_columnar_eval_test_channel_reserve = NULL;
    wl_columnar_eval_test_before_pump = NULL;
    wl_columnar_eval_test_abandon_round = NULL;
    wl_columnar_eval_test_channel_split = NULL;
    wl_columnar_eval_test_subpass_boundary = NULL;
    reserve_mode = RESERVE_NONE;
    pump_mode = PUMP_NONE;
}

/* Unhook everything, clear the parameters and counters, and install the
 * subpass-boundary hook, which only observes. */
static void
hooks_reset(void)
{
    hooks_disarm();
    abandon_mode = ABANDON_AFTER_PUBLISH;
    reserve_fail_worker = 0;
    reserve_fail_rc = 0;
    pump_fail_rc = 0;
    atomic_store_explicit(&reserve_fired, 0, memory_order_relaxed);
    atomic_store_explicit(&reserve_saw_cancel, 0, memory_order_relaxed);
    atomic_store_explicit(&reserve_timeouts, 0, memory_order_relaxed);
    atomic_store_explicit(&heavy_worker, 0, memory_order_relaxed);
    atomic_store_explicit(&heavy_round, 0, memory_order_relaxed);
    atomic_store_explicit(&current_round, 0, memory_order_relaxed);
    atomic_store_explicit(&worker_failed, 0, memory_order_relaxed);
    atomic_store_explicit(&pump_calls, 0, memory_order_relaxed);
    atomic_store_explicit(&abandon_calls, 0, memory_order_relaxed);
    atomic_store_explicit(&publish_timeouts, 0, memory_order_relaxed);
    atomic_store_explicit(&pump_timeouts, 0, memory_order_relaxed);
    round_started = false;
    round_blocked_base = 0;
    queue_threads = 0;
    skew_paused = false;
    skew_heavy_blocked = false;
    hold_blocked = 0;
    final_valid = false;
    memset(&final_stats, 0, sizeof(final_stats));
    wl_columnar_eval_test_subpass_boundary = boundary_hook;
}

/* ------------------------------------------------------------------------ */
/* Result collection                                                         */
/* ------------------------------------------------------------------------ */

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

static void
collect_reset(collect_t *c)
{
    free(c->rows);
    memset(c, 0, sizeof(*c));
}

/* Sort both and compare them as sets; duplicates make them differ. */
static bool
same_set(collect_t *a, collect_t *b)
{
    if (a->bad || b->bad || a->count != b->count)
        return false;
    if (a->count > 1) {
        qsort(a->rows, a->count, sizeof(*a->rows), pair_cmp);
        qsort(b->rows, b->count, sizeof(*b->rows), pair_cmp);
    }
    for (size_t i = 1; i < a->count; i++)
        if (pair_cmp(&a->rows[i - 1], &a->rows[i]) == 0)
            return false;
    return a->count == 0
           || memcmp(a->rows, b->rows, a->count * sizeof(*a->rows)) == 0;
}

/* ------------------------------------------------------------------------ */
/* Sessions                                                                  */
/* ------------------------------------------------------------------------ */

#define NODES 24
#define MAX_NODES 64

typedef struct {
    const char *name;
    const char *rule;     /* the recursive tc rule */
    const char *strategy; /* the TDD strategy the rule reaches */
} program_t;

/* As test_tdd_channel_split.c observes on this base: the one-sided rule
 * runs in owner mode and the two-sided pair in global-read mode.  Each test
 * checks the strategy it relies on. */
static const program_t left_linear = {
    "left-linear", "tc(x, z) :- tc(x, y), edge(y, z).\n", "owner"
};
static const program_t two_sided = {
    "two-sided", "tc(x, z) :- tc(x, y), edge(y, z).\n"
    "tc(x, z) :- edge(x, y), tc(y, z).\n", "global_read"
};
static const program_t non_linear = {
    "non-linear", "tc(x, z) :- tc(x, y), tc(y, z).\n", "bdx"
};

typedef struct {
    wl_plan_t *plan;
    wl_session_t *session;
    wl_col_session_t *coord;
} fixture_t;

static int
fixture_open(fixture_t *f, const program_t *p, uint32_t workers,
    uint32_t nodes)
{
    memset(f, 0, sizeof(*f));
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
    int rc = wl_plan_from_program(prog, &f->plan);
    if (rc != 0 || !f->plan) {
        wirelog_program_free(prog);
        return rc ? rc : EINVAL;
    }
    plan_fixture_hold(prog);
    rc = wl_session_create(wl_backend_columnar(), f->plan, workers,
            &f->session);
    if (rc != 0)
        return rc;
    f->coord = COL_SESSION(f->session);
    /* A DAG with skip edges, as in test_tdd_channel_split.c: small enough
     * that owner mode never replays serially.  A larger graph adds longer
     * skips, so every worker gets rows while the diameter stays short. */
    int64_t edges[2 * 3 * MAX_NODES];
    uint32_t count = 0;
    if (nodes > MAX_NODES)
        return EINVAL;
    for (int64_t i = 0; i + 1 < (int64_t)nodes; i++) {
        edges[2 * count] = i;
        edges[2 * count + 1] = i + 1;
        count++;
        if (i % 2 == 0 && i + 3 < (int64_t)nodes) {
            edges[2 * count] = i;
            edges[2 * count + 1] = i + 3;
            count++;
        }
        if (nodes > NODES && i % 4 == 0 && i + 7 < (int64_t)nodes) {
            edges[2 * count] = i;
            edges[2 * count + 1] = i + 7;
            count++;
        }
    }
    return wl_session_insert(f->session, "edge", edges, count, 2);
}

static void
fixture_close(fixture_t *f)
{
    wl_session_destroy(f->session);
    wl_plan_free(f->plan);
    memset(f, 0, sizeof(*f));
}

static uint64_t
channel_ledger(const wl_col_session_t *coord)
{
    wl_mem_ledger_snapshot_t ledger;
    wl_mem_ledger_snapshot(&coord->mem_ledger, &ledger);
    return ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL];
}

/* The channel is gone and charges nothing once a stratum has returned. */
static bool
channel_released(const wl_col_session_t *coord)
{
    return !coord->tdd_channel && !coord->tdd_channel_lists
           && !coord->tdd_channel_discard && !coord->tdd_owner_lifetime
           && coord->mem_channel_side_bytes == 0
           && coord->tdd_channel_reservation.bytes == 0
           && channel_ledger(coord) == 0;
}

static int
serial_result(const program_t *p, uint32_t nodes, collect_t *out)
{
    fixture_t f;
    int rc = fixture_open(&f, p, 1, nodes);
    if (rc == 0)
        rc = wl_session_snapshot(f.session, collect_tc, out);
    fixture_close(&f);
    return rc != 0 ? rc : (out->bad || out->count == 0 ? EIO : 0);
}

/* ------------------------------------------------------------------------ */
/* T5: rc selection                                                          */
/* ------------------------------------------------------------------------ */

/*
 * The last worker's first reservation fails with EOVERFLOW in a global-read
 * stratum while the other workers keep producing.  A worker error does not
 * cancel the round, so the EOVERFLOW is the round's rc and the stratum
 * replays serially with the exact serial result.  Without a cancel, taking
 * the first non-zero worker rc would give the same rc;
 * test_worker_error_outranks_cancel() covers a round where it would not.
 */
static void
test_worker_error_selects_its_rc(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&two_sided, NODES, &serial) == 0,
        "two-sided serial run");
    /* Control: without the hook this stratum runs in global-read mode and
     * does not replay. */
    hooks_reset();
    int rc = fixture_open(&f, &two_sided, workers, NODES);
    if (rc == 0)
        rc = wl_session_snapshot(f.session, collect_tc, &got);
    CHECK(rc == 0 && f.coord->tdd_audit.strategy
        && strcmp(f.coord->tdd_audit.strategy, two_sided.strategy) == 0
        && !f.coord->tdd_audit.replay && same_set(&got, &serial),
        "W=%u control rc=%d", workers, rc);
    fixture_close(&f);
    collect_reset(&got);
    rc = fixture_open(&f, &two_sided, workers, NODES);
    CHECK(rc == 0, "W=%u fixture rc=%d", workers, rc);
    if (rc == 0) {
        hooks_reset();
        reserve_mode = RESERVE_FAIL_LAST_WORKER;
        reserve_fail_worker = workers - 1;
        reserve_fail_rc = EOVERFLOW;
        wl_columnar_eval_test_channel_reserve = reserve_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        const char *strategy = f.coord->tdd_audit.strategy;
        const char *replay = f.coord->tdd_audit.replay;
        fprintf(stderr, "  worker EOVERFLOW W=%u: rc=%d strategy=%s "
            "replay=%s replay_rc=%d fired=%llu rows=%zu/%zu\n", workers, rc,
            strategy ? strategy : "(none)", replay ? replay : "(none)",
            f.coord->tdd_audit.replay_rc,
            (unsigned long long)load(&reserve_fired), got.count,
            serial.count);
        CHECK(rc == 0, "W=%u snapshot rc=%d", workers, rc);
        CHECK(strategy && strcmp(strategy, two_sided.strategy) == 0,
            "W=%u strategy %s", workers, strategy ? strategy : "(none)");
        CHECK(replay && strcmp(replay, "global_read_overflow") == 0,
            "W=%u replay %s", workers, replay ? replay : "(none)");
        CHECK(f.coord->tdd_audit.replay_rc == EOVERFLOW,
            "W=%u replay_rc=%d", workers, f.coord->tdd_audit.replay_rc);
        CHECK(final_valid && !final_stats.cancelled
            && final_stats.channel_rc == 0,
            "W=%u a worker error cancelled the round: cancelled=%d "
            "channel_rc=%d", workers, final_stats.cancelled,
            final_stats.channel_rc);
        CHECK(same_set(&got, &serial), "W=%u result differs from serial",
            workers);
        CHECK(channel_released(f.coord), "W=%u channel not released",
            workers);
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/*
 * The first pump reports EPROTO.  The consumer cancels the round and pumps
 * it to its end; the first producer to reserve waits for that cancel, so it
 * fails with ECANCELED.  The stratum must fail with EPROTO, not with the
 * ECANCELED of a worker that only saw the cancel, and a retry on the same
 * session must give the serial result.
 */
static void
test_pump_error_selects_channel_rc(const program_t *p, uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(p, NODES, &serial) == 0, "%s serial run", p->name);
    int rc = fixture_open(&f, p, workers, NODES);
    CHECK(rc == 0, "%s W=%u fixture rc=%d", p->name, workers, rc);
    if (rc == 0) {
        hooks_reset();
        reserve_mode = RESERVE_WAIT_CANCEL;
        pump_mode = PUMP_FAIL_ONCE;
        pump_fail_rc = EPROTO;
        wl_columnar_eval_test_channel_reserve = reserve_hook;
        wl_columnar_eval_test_before_pump = pump_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        const char *strategy = f.coord->tdd_audit.strategy;
        fprintf(stderr, "  pump EPROTO %s W=%u: rc=%d strategy=%s "
            "saw_cancel=%llu timeouts=%llu\n", p->name, workers, rc,
            strategy ? strategy : "(none)",
            (unsigned long long)load(&reserve_saw_cancel),
            (unsigned long long)load(&reserve_timeouts));
        CHECK(rc == EPROTO, "%s W=%u snapshot rc=%d, want EPROTO", p->name,
            workers, rc);
        CHECK(strategy && strcmp(strategy, p->strategy) == 0,
            "%s W=%u strategy %s", p->name, workers,
            strategy ? strategy : "(none)");
        /* The stratum's audit records the cancel and its error. */
        CHECK(f.coord->tdd_audit.channel_cancelled == 1
            && f.coord->tdd_audit.channel_rc == EPROTO
            && !f.coord->tdd_audit.refusal,
            "%s W=%u audit cancelled=%llu channel_rc=%d", p->name, workers,
            (unsigned long long)f.coord->tdd_audit.channel_cancelled,
            f.coord->tdd_audit.channel_rc);
        CHECK(load(&publish_timeouts) == 0,
            "%s W=%u no batch was published while a producer waited",
            p->name, workers);
        CHECK(load(&reserve_saw_cancel) == 1 && load(&reserve_timeouts) == 0,
            "%s W=%u the waiting producer never saw the cancel", p->name,
            workers);
        CHECK(channel_released(f.coord), "%s W=%u channel not released",
            p->name, workers);
        collect_reset(&got);
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        CHECK(rc == 0 && same_set(&got, &serial),
            "%s W=%u retry rc=%d rows=%zu/%zu", p->name, workers, rc,
            got.count, serial.count);
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/* ------------------------------------------------------------------------ */
/* I1: an abandoned round is cancelled before the teardown drain             */
/* ------------------------------------------------------------------------ */

/*
 * In owner mode the driver leaves its first dispatched round without
 * pumping or joining it, while the first producer to reserve waits for a
 * cancel.  The worker cleanup quiesces the owner lifetime record, which
 * must cancel the open round before it drains the workqueue: the producer
 * returns, the drain completes, and the stratum fails with the hook's EIO
 * with the channel released.  A retry gives the serial result.
 */
static void
test_abandoned_round_is_cancelled(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&left_linear, NODES, &serial) == 0,
        "left-linear serial");
    int rc = fixture_open(&f, &left_linear, workers, NODES);
    CHECK(rc == 0, "W=%u fixture rc=%d", workers, rc);
    if (rc == 0) {
        hooks_reset();
        reserve_mode = RESERVE_WAIT_CANCEL;
        wl_columnar_eval_test_channel_reserve = reserve_hook;
        wl_columnar_eval_test_abandon_round = abandon_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        const char *strategy = f.coord->tdd_audit.strategy;
        fprintf(stderr, "  abandoned round W=%u: rc=%d strategy=%s "
            "abandoned=%llu saw_cancel=%llu timeouts=%llu\n", workers, rc,
            strategy ? strategy : "(none)",
            (unsigned long long)load(&abandon_calls),
            (unsigned long long)load(&reserve_saw_cancel),
            (unsigned long long)load(&reserve_timeouts));
        CHECK(rc == EIO, "W=%u snapshot rc=%d, want EIO", workers, rc);
        CHECK(strategy && strcmp(strategy, left_linear.strategy) == 0,
            "W=%u strategy %s", workers, strategy ? strategy : "(none)");
        CHECK(load(&abandon_calls) >= 1, "W=%u the round was not abandoned",
            workers);
        CHECK(load(&publish_timeouts) == 0,
            "W=%u no batch was published while a producer waited", workers);
        CHECK(load(&reserve_saw_cancel) == 1 && load(&reserve_timeouts) == 0,
            "W=%u the waiting producer never saw the cancel", workers);
        CHECK(channel_released(f.coord), "W=%u channel not released",
            workers);
        collect_reset(&got);
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        CHECK(rc == 0 && same_set(&got, &serial),
            "W=%u retry rc=%d rows=%zu/%zu", workers, rc, got.count,
            serial.count);
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/* ------------------------------------------------------------------------ */
/* U5b: a finite capacity                                                    */
/* ------------------------------------------------------------------------ */

/* Larger than any graph the phase-one tests use, so that every worker has
 * rows to publish in the first rounds. */
#define CAP_NODES 48

/* A capacity below any batch: every batch is oversize and is admitted only
 * into an empty channel, so while the consumer holds back, the second
 * reservation of a round waits for space. */
#define TINY_CAP "1"

static void
set_env(const char *name, const char *value)
{
    if (value)
        setenv(name, value, 1);
    else
        unsetenv(name);
}

#define MiB (UINT64_C(1024) * 1024)

/* The channel capacity a session with these settings gets, read from the
 * channel after its last joined round; 0 when no round was joined. */
static uint64_t
observed_cap(const char *budget, const char *override, uint32_t *mode,
    uint64_t *budget_bytes)
{
    fixture_t f;
    collect_t got = { 0 };
    uint64_t cap = 0;
    set_env("WIRELOG_MEMORY_BUDGET", budget);
    set_env("WIRELOG_TDD_CHANNEL_BYTES", override);
    hooks_reset();
    int rc = fixture_open(&f, &left_linear, 2, NODES);
    if (rc == 0) {
        const wl_columnar_memory_governor_t *governor = f.coord->memory_governor
            ? wl_columnar_memory_governor_ref_get(f.coord->memory_governor)
            : NULL;
        *mode = governor ? (uint32_t)governor->mode : UINT32_MAX;
        *budget_bytes = governor ? governor->budget_bytes : 0;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
    }
    hooks_disarm();
    if (rc == 0 && final_valid)
        cap = final_stats.cap_bytes;
    fixture_close(&f);
    collect_reset(&got);
    set_env("WIRELOG_MEMORY_BUDGET", NULL);
    set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
    return cap;
}

/* The default the session's governor implies: an eighth of an enforcing
 * budget clamped to [4 MiB, 1 GiB], or 256 MiB without an enforced one. */
static uint64_t
expected_cap(uint32_t mode, uint64_t budget_bytes)
{
    if (mode != (uint32_t)WL_COLUMNAR_MEMORY_MODE_ENFORCING)
        return 256 * MiB;
    uint64_t cap = budget_bytes / 8;
    if (cap < 4 * MiB)
        return 4 * MiB;
    return cap > 1024 * MiB ? 1024 * MiB : cap;
}

static void
test_channel_capacity(void)
{
    uint32_t mode = 0;
    uint64_t budget = 0;
    uint64_t cap = observed_cap(NULL, NULL, &mode, &budget);
    fprintf(stderr, "  capacity: default mode=%u budget=%llu cap=%llu\n",
        mode, (unsigned long long)budget, (unsigned long long)cap);
    CHECK(cap == expected_cap(mode, budget), "default capacity %llu",
        (unsigned long long)cap);

    cap = observed_cap("1073741824", NULL, &mode, &budget);
    CHECK(mode == (uint32_t)WL_COLUMNAR_MEMORY_MODE_ENFORCING
        && budget == 1024 * MiB && cap == 128 * MiB,
        "1 GiB budget: mode=%u budget=%llu cap=%llu", mode,
        (unsigned long long)budget, (unsigned long long)cap);
    cap = observed_cap("17179869184", NULL, &mode, &budget);
    CHECK(mode == (uint32_t)WL_COLUMNAR_MEMORY_MODE_ENFORCING
        && cap == 1024 * MiB, "16 GiB budget: mode=%u cap=%llu", mode,
        (unsigned long long)cap);
    cap = observed_cap("1073741824", "12345", &mode, &budget);
    CHECK(cap == 12345, "override under a budget: cap=%llu",
        (unsigned long long)cap);
    cap = observed_cap(NULL, "18446744073709551615", &mode, &budget);
    CHECK(cap == UINT64_MAX, "maximal override: cap=%llu",
        (unsigned long long)cap);

    /* Anything but a positive decimal that fits is ignored. */
    static const char *const ignored[] = {
        "", "0", "-1", "+5", " 5", "5 ", "12x", "0x10",
        "18446744073709551616",
    };
    for (size_t i = 0; i < sizeof(ignored) / sizeof(ignored[0]); i++) {
        cap = observed_cap("1073741824", ignored[i], &mode, &budget);
        CHECK(cap == 128 * MiB, "override \"%s\" was not ignored: cap=%llu",
            ignored[i], (unsigned long long)cap);
    }
}

/*
 * T2: with a capacity below any batch, the first pump of each round holds
 * back until a reservation of that round has waited for space (or every
 * lane has closed).  Producers must then be admitted one batch at a time
 * as the consumer takes, within the admission bound, and the result must
 * equal the serial one.
 */
static void
test_backpressure(const program_t *p, uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(p, CAP_NODES, &serial) == 0, "%s serial run",
        p->name);
    int rc = fixture_open(&f, p, workers, CAP_NODES);
    CHECK(rc == 0, "%s W=%u fixture rc=%d", p->name, workers, rc);
    if (rc == 0) {
        hooks_reset();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", TINY_CAP);
        pump_mode = PUMP_WAIT_BLOCKED;
        wl_columnar_eval_test_before_pump = pump_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
        const char *strategy = f.coord->tdd_audit.strategy;
        fprintf(stderr, "  backpressure %s W=%u: rc=%d strategy=%s "
            "threads=%u cap=%llu blocked=%llu takes=%llu oversize=%llu "
            "max_takes_blocked=%llu violations=%llu timeouts=%llu "
            "rows=%zu/%zu\n", p->name, workers, rc,
            strategy ? strategy : "(none)", queue_threads,
            (unsigned long long)final_stats.cap_bytes,
            (unsigned long long)final_stats.blocked_waits,
            (unsigned long long)final_stats.takes,
            (unsigned long long)final_stats.oversize_admits,
            (unsigned long long)final_stats.max_takes_while_blocked,
            (unsigned long long)final_stats.admission_bound_violations,
            (unsigned long long)load(&pump_timeouts), got.count,
            serial.count);
        CHECK(rc == 0, "%s W=%u snapshot rc=%d", p->name, workers, rc);
        CHECK(strategy && strcmp(strategy, p->strategy) == 0,
            "%s W=%u strategy %s", p->name, workers,
            strategy ? strategy : "(none)");
        CHECK(final_valid && final_stats.cap_bytes == 1,
            "%s W=%u the override did not reach the channel", p->name,
            workers);
        CHECK(final_stats.blocked_waits > 0,
            "%s W=%u no reservation waited for space", p->name, workers);
        CHECK(final_stats.admission_bound_violations == 0,
            "%s W=%u admission bound violated", p->name, workers);
        CHECK(load(&pump_timeouts) == 0, "%s W=%u a hold timed out",
            p->name, workers);
        CHECK(same_set(&got, &serial), "%s W=%u result differs from serial",
            p->name, workers);
        CHECK(channel_released(f.coord), "%s W=%u channel not released",
            p->name, workers);
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/*
 * Skew: the first producer to reserve a delta of at least
 * WL_TDD_CHANNEL_TEST_SPLIT_MAX rows becomes the heavy one.  It waits in
 * the reserve hook until every other lane of its round has closed, then
 * publishes its delta as that many batches through the split hook.  Once
 * only its lane is open, the consumer holds back until the heavy producer
 * waits for space: one lane blocked does not keep the light lanes from
 * closing, and the result is exact.  The light lanes need a thread while
 * the heavy task holds one, so the case needs two workqueue threads.
 */
static void
test_skewed_producers(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&left_linear, CAP_NODES, &serial) == 0,
        "left-linear serial");
    int rc = fixture_open(&f, &left_linear, workers, CAP_NODES);
    CHECK(rc == 0, "W=%u fixture rc=%d", workers, rc);
    if (rc == 0) {
        hooks_reset();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", TINY_CAP);
        reserve_mode = RESERVE_HEAVY;
        pump_mode = PUMP_SKEW;
        wl_columnar_eval_test_channel_reserve = reserve_hook;
        wl_columnar_eval_test_channel_split = split_hook;
        wl_columnar_eval_test_before_pump = pump_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
        fprintf(stderr, "  skew W=%u: rc=%d threads=%u heavy=%llu "
            "heavy_blocked=%d blocked=%llu violations=%llu timeouts=%llu/%llu "
            "rows=%zu/%zu\n", workers, rc, queue_threads,
            (unsigned long long)load(&heavy_worker), skew_heavy_blocked,
            (unsigned long long)final_stats.blocked_waits,
            (unsigned long long)final_stats.admission_bound_violations,
            (unsigned long long)load(&reserve_timeouts),
            (unsigned long long)load(&pump_timeouts), got.count,
            serial.count);
        if (queue_threads < 2) {
            fprintf(stderr, "  skew W=%u: SKIP, %u workqueue thread(s)\n",
                workers, queue_threads);
        } else {
            CHECK(rc == 0, "W=%u snapshot rc=%d", workers, rc);
            CHECK(load(&heavy_worker) != 0, "W=%u no heavy producer",
                workers);
            CHECK(load(&reserve_timeouts) == 0,
                "W=%u the light lanes did not close while the heavy one "
                "waited", workers);
            CHECK(skew_heavy_blocked && load(&pump_timeouts) == 0,
                "W=%u the heavy lane never waited for space alone", workers);
            CHECK(final_stats.admission_bound_violations == 0,
                "W=%u admission bound violated", workers);
            CHECK(same_set(&got, &serial), "W=%u result differs from serial",
                workers);
            CHECK(channel_released(f.coord), "W=%u channel not released",
                workers);
        }
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

static double
seconds_now(void)
{
    struct timespec ts;
    if (timespec_get(&ts, TIME_UTC) != TIME_UTC)
        return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* Governor bytes reserved by @coord's session. */
static uint64_t
governor_reserved(wl_col_session_t *coord)
{
    return coord->memory_governor
           ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(coord->memory_governor)) : 0;
}

/* One T3 snapshot: fail the first pump with EPROTO once every producer
 * that can run waits for space.  Returns the snapshot's rc. */
static int
cancel_blocked_once(fixture_t *f, double *elapsed)
{
    collect_t got = { 0 };
    hooks_reset();
    set_env("WIRELOG_TDD_CHANNEL_BYTES", TINY_CAP);
    pump_mode = PUMP_CANCEL_BLOCKED;
    pump_fail_rc = EPROTO;
    wl_columnar_eval_test_before_pump = pump_hook;
    double t0 = seconds_now();
    int rc = wl_session_snapshot(f->session, collect_tc, &got);
    *elapsed = seconds_now() - t0;
    hooks_disarm();
    set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
    collect_reset(&got);
    return rc;
}

/*
 * T3: producers wait for space while the consumer holds back; then the
 * consumer's pump fails with EPROTO.  The consumer cancels the round, the
 * waiting producers return ECANCELED, every lane closes and the stratum
 * fails with EPROTO well within the deadline.  The channel is released
 * with its CHANNEL charge, and a second identical failure leaves the
 * governor exactly where the first left it, so a cancelled round keeps no
 * reservation.  A retry gives the serial result.
 */
static void
test_cancel_releases_blocked(const program_t *p, uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(p, CAP_NODES, &serial) == 0, "%s serial run",
        p->name);
    int rc = fixture_open(&f, p, workers, CAP_NODES);
    CHECK(rc == 0, "%s W=%u fixture rc=%d", p->name, workers, rc);
    if (rc == 0) {
        double first_s = 0, second_s = 0;
        int first_rc = cancel_blocked_once(&f, &first_s);
        uint64_t first_blocked = hold_blocked;
        uint64_t first_timeouts = load(&pump_timeouts);
        bool first_released = channel_released(f.coord);
        uint64_t after_first = governor_reserved(f.coord);
        int second_rc = cancel_blocked_once(&f, &second_s);
        uint64_t after_second = governor_reserved(f.coord);
        fprintf(stderr, "  cancel blocked %s W=%u: rc=%d/%d threads=%u "
            "blocked=%llu/%llu timeouts=%llu/%llu elapsed=%.2fs/%.2fs "
            "reserved=%llu/%llu\n", p->name, workers, first_rc, second_rc,
            queue_threads, (unsigned long long)first_blocked,
            (unsigned long long)hold_blocked,
            (unsigned long long)first_timeouts,
            (unsigned long long)load(&pump_timeouts), first_s, second_s,
            (unsigned long long)after_first,
            (unsigned long long)after_second);
        CHECK(first_rc == EPROTO && second_rc == EPROTO,
            "%s W=%u snapshot rc=%d/%d, want EPROTO", p->name, workers,
            first_rc, second_rc);
        CHECK(first_blocked >= 1 && hold_blocked >= 1,
            "%s W=%u no producer waited for space at the cancel", p->name,
            workers);
        CHECK(first_timeouts == 0 && load(&pump_timeouts) == 0,
            "%s W=%u the producers never all waited", p->name, workers);
        CHECK(first_s < WAIT_MS / 1000.0 && second_s < WAIT_MS / 1000.0,
            "%s W=%u the cancelled round took %.2fs/%.2fs", p->name, workers,
            first_s, second_s);
        CHECK(first_released && channel_released(f.coord),
            "%s W=%u channel not released", p->name, workers);
        CHECK(after_second == after_first,
            "%s W=%u governor drifted across cancelled rounds: %llu -> %llu",
            p->name, workers, (unsigned long long)after_first,
            (unsigned long long)after_second);
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        CHECK(rc == 0 && same_set(&got, &serial),
            "%s W=%u retry rc=%d rows=%zu/%zu", p->name, workers, rc,
            got.count, serial.count);
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/*
 * I1 with a producer really waiting for space: in owner mode the driver
 * leaves its first round once a producer waits in reserve.  Nobody pumps,
 * so only the cancel in the owner lifetime record's quiesce lets that
 * producer return before the worker cleanup drains the workqueue; without
 * it the drain would never return.
 */
static void
test_abandoned_blocked_round(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&left_linear, CAP_NODES, &serial) == 0,
        "left-linear serial");
    int rc = fixture_open(&f, &left_linear, workers, CAP_NODES);
    CHECK(rc == 0, "W=%u fixture rc=%d", workers, rc);
    if (rc == 0) {
        hooks_reset();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", TINY_CAP);
        abandon_mode = ABANDON_AFTER_BLOCK;
        wl_columnar_eval_test_abandon_round = abandon_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
        fprintf(stderr, "  abandoned blocked round W=%u: rc=%d blocked=%llu "
            "timeouts=%llu\n", workers, rc, (unsigned long long)hold_blocked,
            (unsigned long long)load(&publish_timeouts));
        CHECK(rc == EIO, "W=%u snapshot rc=%d, want EIO", workers, rc);
        CHECK(hold_blocked >= 1 && load(&publish_timeouts) == 0,
            "W=%u no producer waited for space when the round was left",
            workers);
        CHECK(channel_released(f.coord), "W=%u channel not released",
            workers);
        collect_reset(&got);
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        CHECK(rc == 0 && same_set(&got, &serial),
            "W=%u retry rc=%d rows=%zu/%zu", workers, rc, got.count,
            serial.count);
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/*
 * T5 with producers waiting: in a global-read stratum the first producer
 * to reserve waits until another producer waits for space, then fails with
 * EOVERFLOW; the consumer holds back until it has failed.  The round is not
 * cancelled: the consumer drains the waiting producers in discard mode,
 * the EOVERFLOW is the round's rc, and the stratum replays serially with
 * the exact serial result.  The failing worker, a producer that publishes
 * first and one that waits must run at once, so the case needs three
 * workqueue threads.
 */
static void
test_worker_error_while_blocked(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&two_sided, CAP_NODES, &serial) == 0,
        "two-sided serial run");
    int rc = fixture_open(&f, &two_sided, workers, CAP_NODES);
    CHECK(rc == 0, "W=%u fixture rc=%d", workers, rc);
    if (rc == 0) {
        hooks_reset();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", TINY_CAP);
        reserve_mode = RESERVE_FAIL_BLOCKED;
        reserve_fail_rc = EOVERFLOW;
        pump_mode = PUMP_WAIT_FAILED;
        wl_columnar_eval_test_channel_reserve = reserve_hook;
        wl_columnar_eval_test_before_pump = pump_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
        const char *replay = f.coord->tdd_audit.replay;
        fprintf(stderr, "  worker EOVERFLOW while blocked W=%u: rc=%d "
            "threads=%u replay=%s blocked=%llu cancelled=%d "
            "timeouts=%llu/%llu rows=%zu/%zu\n", workers, rc, queue_threads,
            replay ? replay : "(none)", (unsigned long long)hold_blocked,
            final_stats.cancelled, (unsigned long long)load(&reserve_timeouts),
            (unsigned long long)load(&pump_timeouts), got.count,
            serial.count);
        if (queue_threads < 3) {
            fprintf(stderr, "  worker EOVERFLOW while blocked W=%u: SKIP, "
                "%u workqueue thread(s)\n", workers, queue_threads);
        } else {
            CHECK(rc == 0, "W=%u snapshot rc=%d", workers, rc);
            CHECK(load(&reserve_timeouts) == 0 && hold_blocked >= 1,
                "W=%u no producer waited when the worker failed", workers);
            CHECK(load(&pump_timeouts) == 0,
                "W=%u the consumer never saw the failure", workers);
            CHECK(final_valid && !final_stats.cancelled,
                "W=%u a worker error cancelled the round", workers);
            CHECK(replay && strcmp(replay, "global_read_overflow") == 0
                && f.coord->tdd_audit.replay_rc == EOVERFLOW,
                "W=%u replay %s rc=%d", workers, replay ? replay : "(none)",
                f.coord->tdd_audit.replay_rc);
            CHECK(same_set(&got, &serial), "W=%u result differs from serial",
                workers);
            CHECK(channel_released(f.coord), "W=%u channel not released",
                workers);
        }
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

/*
 * T5 with a cancel: in a global-read stratum the last worker's first
 * reservation waits until another producer waits for space, then fails
 * with EOVERFLOW.  Once that worker's lane has closed and every other open
 * lane waits for space, the first pump fails with EPROTO, which cancels the
 * round, so the waiting lower-numbered workers return ECANCELED.  The
 * round's rc is the first worker rc that is neither 0 nor ECANCELED, so
 * the EOVERFLOW outranks both the channel's EPROTO and their ECANCELED,
 * and the stratum replays serially with the exact serial result.  Taking
 * the first non-zero worker rc instead would pick an ECANCELED and fail
 * the snapshot.  Every lane must wait for space or close while the
 * consumer holds back, so the case needs a workqueue thread per worker.
 */
static void
test_worker_error_outranks_cancel(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&two_sided, CAP_NODES, &serial) == 0,
        "two-sided serial run");
    int rc = fixture_open(&f, &two_sided, workers, CAP_NODES);
    CHECK(rc == 0, "W=%u fixture rc=%d", workers, rc);
    if (rc == 0) {
        hooks_reset();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", TINY_CAP);
        reserve_mode = RESERVE_FAIL_LAST_BLOCKED;
        reserve_fail_worker = workers - 1;
        reserve_fail_rc = EOVERFLOW;
        pump_mode = PUMP_FAIL_FAILED;
        pump_fail_rc = EPROTO;
        wl_columnar_eval_test_channel_reserve = reserve_hook;
        wl_columnar_eval_test_before_pump = pump_hook;
        rc = wl_session_snapshot(f.session, collect_tc, &got);
        hooks_disarm();
        set_env("WIRELOG_TDD_CHANNEL_BYTES", NULL);
        const char *strategy = f.coord->tdd_audit.strategy;
        const char *replay = f.coord->tdd_audit.replay;
        fprintf(stderr, "  worker EOVERFLOW outranks cancel W=%u: rc=%d "
            "threads=%u strategy=%s replay=%s replay_rc=%d blocked=%llu "
            "cancelled=%d channel_rc=%d timeouts=%llu/%llu rows=%zu/%zu\n",
            workers, rc, queue_threads, strategy ? strategy : "(none)",
            replay ? replay : "(none)", f.coord->tdd_audit.replay_rc,
            (unsigned long long)hold_blocked, final_stats.cancelled,
            final_stats.channel_rc, (unsigned long long)load(&reserve_timeouts),
            (unsigned long long)load(&pump_timeouts), got.count,
            serial.count);
        if (queue_threads < workers) {
            fprintf(stderr, "  worker EOVERFLOW outranks cancel W=%u: SKIP, "
                "%u workqueue thread(s)\n", workers, queue_threads);
        } else {
            CHECK(rc == 0, "W=%u snapshot rc=%d", workers, rc);
            CHECK(strategy && strcmp(strategy, two_sided.strategy) == 0,
                "W=%u strategy %s", workers, strategy ? strategy : "(none)");
            CHECK(load(&reserve_timeouts) == 0 && load(&pump_timeouts) == 0,
                "W=%u a hook's wait timed out", workers);
            CHECK(hold_blocked >= 1,
                "W=%u no lower-numbered producer waited at the cancel",
                workers);
            CHECK(final_valid && final_stats.cancelled
                && final_stats.channel_rc == EPROTO,
                "W=%u the pump error did not cancel the round", workers);
            /* The audit keeps the channel's EPROTO although the worker's
             * EOVERFLOW is the round's rc. */
            CHECK(f.coord->tdd_audit.channel_cancelled == 1
                && f.coord->tdd_audit.channel_rc == EPROTO,
                "W=%u audit cancelled=%llu channel_rc=%d", workers,
                (unsigned long long)f.coord->tdd_audit.channel_cancelled,
                f.coord->tdd_audit.channel_rc);
            CHECK(replay && strcmp(replay, "global_read_overflow") == 0
                && f.coord->tdd_audit.replay_rc == EOVERFLOW,
                "W=%u replay %s rc=%d", workers, replay ? replay : "(none)",
                f.coord->tdd_audit.replay_rc);
            CHECK(same_set(&got, &serial), "W=%u result differs from serial",
                workers);
            CHECK(channel_released(f.coord), "W=%u channel not released",
                workers);
        }
    }
    fixture_close(&f);
    collect_reset(&got);
    collect_reset(&serial);
}

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    unsetenv("WIRELOG_TDD_GLOBAL_READ");

    unsetenv("WIRELOG_TDD_CHANNEL_BYTES");
    unsetenv("WIRELOG_MEMORY_BUDGET");

    test_worker_error_selects_its_rc(4);
    test_pump_error_selects_channel_rc(&left_linear, 4);
    test_pump_error_selects_channel_rc(&two_sided, 4);
    test_abandoned_round_is_cancelled(2);

    test_channel_capacity();
    static const uint32_t widths[] = { 2, 8 };
    for (size_t i = 0; i < sizeof(widths) / sizeof(widths[0]); i++) {
        test_backpressure(&left_linear, widths[i]);
        test_backpressure(&two_sided, widths[i]);
        test_backpressure(&non_linear, widths[i]);
        test_skewed_producers(widths[i]);
        test_cancel_releases_blocked(&left_linear, widths[i]);
        test_cancel_releases_blocked(&two_sided, widths[i]);
    }
    test_abandoned_blocked_round(2);
    test_worker_error_while_blocked(4);
    test_worker_error_outranks_cancel(4);

    hooks_reset();
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_TDD_STRATUM_PROFILE");
    fprintf(stderr, "tdd_channel_round: %u failure(s)\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
