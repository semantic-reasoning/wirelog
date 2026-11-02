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
 * - rc selection: a worker's own error wins over the ECANCELED of workers
 *   that only saw a cancel, and a consumer-side error surfaces as itself;
 * - I1: a round the driver leaves without joining is cancelled by the
 *   teardown path before it drains the workqueue, so a producer that waits
 *   on the channel returns.
 *
 * Every wait is condition based: a hook waits on channel state through
 * wl_tdd_channel_test_wait() with a deadline, and an expired deadline is
 * recorded as a failure instead of hanging.
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

extern bool (*wl_columnar_eval_test_abandon_round)(wl_col_session_t *);

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
/* Hook state.  The main thread arms the hooks before a snapshot starts its
 * workers and reads the counters after it returns; workers and the
 * coordinator only bump counters. */
/* ------------------------------------------------------------------------ */

typedef enum {
    RESERVE_NONE = 0,
    RESERVE_FAIL_LAST_WORKER, /* the last worker's first reserve fails */
    RESERVE_WAIT_CANCEL,      /* the first reserve waits for a cancel */
} reserve_mode_t;

static reserve_mode_t reserve_mode;
static uint32_t reserve_fail_worker;
static int reserve_fail_rc;
static wl_atomic_u64 reserve_fired;
static wl_atomic_u64 reserve_saw_cancel;
static wl_atomic_u64 reserve_timeouts;

static wl_atomic_u64 pump_calls;
static int pump_fail_rc;

static wl_atomic_u64 abandon_calls;
static wl_atomic_u64 publish_timeouts;

static bool
pred_cancelled(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->cancelled;
}

static int
reserve_hook(wl_col_session_t *worker, uint32_t rel_idx)
{
    (void)rel_idx;
    switch (reserve_mode) {
    case RESERVE_FAIL_LAST_WORKER:
        if (worker->worker_id != reserve_fail_worker)
            return 0;
        if (atomic_fetch_add_explicit(&reserve_fired, 1,
            memory_order_relaxed) != 0)
            return 0;
        return reserve_fail_rc;
    case RESERVE_WAIT_CANCEL: {
        if (atomic_fetch_add_explicit(&reserve_fired, 1,
            memory_order_relaxed) != 0)
            return 0;
        wl_tdd_channel_t *ch = worker->coordinator->tdd_channel;
        if (wl_tdd_channel_test_wait(ch, pred_cancelled, NULL, WAIT_MS) == 0)
            atomic_fetch_add_explicit(&reserve_saw_cancel, 1,
                memory_order_relaxed);
        else
            atomic_fetch_add_explicit(&reserve_timeouts, 1,
                memory_order_relaxed);
        return 0;
    }
    default:
        return 0;
    }
}

static bool
pred_published(const wl_tdd_channel_stats_t *stats, void *ctx)
{
    (void)ctx;
    return stats->published >= 1;
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
    if (wl_tdd_channel_test_wait(ch, pred_published, NULL, WAIT_MS) != 0)
        atomic_fetch_add_explicit(&publish_timeouts, 1, memory_order_relaxed);
}

/* The first pump after arming reports pump_fail_rc, once a producer waits
 * in the reserve hook. */
static int
pump_hook(wl_tdd_channel_t *ch)
{
    if (atomic_fetch_add_explicit(&pump_calls, 1, memory_order_relaxed) != 0)
        return 0;
    wait_for_waiting_producer(ch);
    return pump_fail_rc;
}

/* The first dispatched round is left without its pump or join, once a
 * producer waits in the reserve hook. */
static bool
abandon_hook(wl_col_session_t *coord)
{
    if (atomic_fetch_add_explicit(&abandon_calls, 1, memory_order_relaxed)
        != 0)
        return false;
    wait_for_waiting_producer(coord->tdd_channel);
    return true;
}

/* Unhook everything; the counters keep their values for the checks. */
static void
hooks_disarm(void)
{
    wl_columnar_eval_test_channel_reserve = NULL;
    wl_columnar_eval_test_before_pump = NULL;
    wl_columnar_eval_test_abandon_round = NULL;
    reserve_mode = RESERVE_NONE;
}

/* Unhook everything and clear the parameters and counters. */
static void
hooks_reset(void)
{
    hooks_disarm();
    reserve_fail_worker = 0;
    reserve_fail_rc = 0;
    pump_fail_rc = 0;
    atomic_store_explicit(&reserve_fired, 0, memory_order_relaxed);
    atomic_store_explicit(&reserve_saw_cancel, 0, memory_order_relaxed);
    atomic_store_explicit(&reserve_timeouts, 0, memory_order_relaxed);
    atomic_store_explicit(&pump_calls, 0, memory_order_relaxed);
    atomic_store_explicit(&abandon_calls, 0, memory_order_relaxed);
    atomic_store_explicit(&publish_timeouts, 0, memory_order_relaxed);
}

static uint64_t
load(wl_atomic_u64 *counter)
{
    return atomic_load_explicit(counter, memory_order_relaxed);
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

typedef struct {
    wl_plan_t *plan;
    wl_session_t *session;
    wl_col_session_t *coord;
} fixture_t;

static int
fixture_open(fixture_t *f, const program_t *p, uint32_t workers)
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
serial_result(const program_t *p, collect_t *out)
{
    fixture_t f;
    int rc = fixture_open(&f, p, 1);
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
 * replays serially with the exact serial result.
 */
static void
test_worker_error_selects_its_rc(uint32_t workers)
{
    collect_t serial = { 0 };
    collect_t got = { 0 };
    fixture_t f;
    CHECK(serial_result(&two_sided, &serial) == 0, "two-sided serial run");
    /* Control: without the hook this stratum runs in global-read mode and
     * does not replay. */
    hooks_reset();
    int rc = fixture_open(&f, &two_sided, workers);
    if (rc == 0)
        rc = wl_session_snapshot(f.session, collect_tc, &got);
    CHECK(rc == 0 && f.coord->tdd_audit.strategy
        && strcmp(f.coord->tdd_audit.strategy, two_sided.strategy) == 0
        && !f.coord->tdd_audit.replay && same_set(&got, &serial),
        "W=%u control rc=%d", workers, rc);
    fixture_close(&f);
    collect_reset(&got);
    rc = fixture_open(&f, &two_sided, workers);
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
    CHECK(serial_result(p, &serial) == 0, "%s serial run", p->name);
    int rc = fixture_open(&f, p, workers);
    CHECK(rc == 0, "%s W=%u fixture rc=%d", p->name, workers, rc);
    if (rc == 0) {
        hooks_reset();
        reserve_mode = RESERVE_WAIT_CANCEL;
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
    CHECK(serial_result(&left_linear, &serial) == 0, "left-linear serial");
    int rc = fixture_open(&f, &left_linear, workers);
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

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    unsetenv("WIRELOG_TDD_GLOBAL_READ");

    test_worker_error_selects_its_rc(4);
    test_pump_error_selects_channel_rc(&left_linear, 4);
    test_pump_error_selects_channel_rc(&two_sided, 4);
    test_abandoned_round_is_cancelled(2);

    hooks_reset();
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_TDD_STRATUM_PROFILE");
    fprintf(stderr, "tdd_channel_round: %u failure(s)\n", failures);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
