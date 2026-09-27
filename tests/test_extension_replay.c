/*
 * test_extension_replay.c - Issue #1986 regression test.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Pins how often the evaluator invokes a scalar addon that appears in a
 * value position, so that a change to the invocation contract in
 * docs/SEMANTICS.md, "Scalar addon invocation count", is deliberate rather
 * than silent.
 *
 * Issue #1986 reported that retracting one EDB row re-invokes the addon for
 * a different, unchanged surviving row.  That is real: the count follows the
 * rows the rule currently derives, not the size of the change.  It is not
 * the row count of the relations the rule reads either -- a selective body
 * narrows it, which the join case below pins.
 *
 * The counts below are a snapshot of this release's behavior, not a promise
 * to a host.  The contract a host may rely on is the prose in
 * docs/SEMANTICS.md.  If you change the engine on purpose, update these
 * numbers and re-read that section to check it still holds.
 *
 * Every case installs a delta callback and drives the session with
 * wirelog_easy_step().  Without a delta callback the addon runs once per
 * derived row on every step, including steps with nothing pending, but that
 * count is unspecified and a stepped session has no non-perturbing way to
 * observe anything through this API, so it is deliberately not pinned here:
 * pinning it would freeze behavior nobody has characterised.  Issue #1994.
 */

#include "wirelog/wirelog-easy.h"
#include "wirelog/wirelog-extension.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)
#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)
#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

/* ======================================================================== */
/* Observation state                                                        */
/* ======================================================================== */

#define MAX_ARGS   64
#define MAX_EVENTS 64
#define MAX_ROWS   64

/* One delta event, recorded for the step that published it. */
typedef struct {
    char relation[16];
    int64_t col[2];
    uint32_t ncols;
    int32_t diff;
} event_t;

/* Host-side z-set mirror of a derived relation, rebuilt from the deltas.
 * wirelog_easy_snapshot() is itself an evaluating call and must not follow
 * wirelog_easy_step() on the same session, so the deltas are the only
 * non-perturbing way to observe what the engine published. */
typedef struct {
    int64_t col[MAX_ROWS][2];
    int32_t mult[MAX_ROWS];
    uint32_t count;
    int overflowed;             /* set if a row did not fit; never silent */
} mirror_t;

static unsigned calls;                  /* addon invocations, cumulative */
static int64_t call_args[MAX_ARGS];     /* args seen since the last reset */
static unsigned ncall_args;
static event_t events[MAX_EVENTS];      /* events since the last reset */
static unsigned nevents;
static mirror_t started_mirror;
static int64_t next_ticket;             /* unstable addon only */

static void
observation_reset(void)
{
    ncall_args = 0;
    nevents = 0;
}

static void
mirror_apply(mirror_t *m, const event_t *e)
{
    for (uint32_t i = 0; i < m->count; i++) {
        if (m->col[i][0] == e->col[0] && m->col[i][1] == e->col[1]) {
            m->mult[i] += e->diff;
            return;
        }
    }
    if (m->count >= MAX_ROWS) {
        m->overflowed = 1;
        return;
    }
    m->col[m->count][0] = e->col[0];
    m->col[m->count][1] = e->col[1];
    m->mult[m->count] = e->diff;
    m->count++;
}

/* Rows the mirror currently holds with a non-zero multiplicity.  An
 * overflowed mirror answers UINT32_MAX so the caller's count assertion fails
 * rather than comparing against a silently truncated tally. */
static uint32_t
mirror_live_rows(const mirror_t *m)
{
    uint32_t live = 0;
    if (m->overflowed)
        return UINT32_MAX;
    for (uint32_t i = 0; i < m->count; i++)
        if (m->mult[i] != 0)
            live++;
    return live;
}

static int
mirror_holds(const mirror_t *m, int64_t a, int64_t b)
{
    for (uint32_t i = 0; i < m->count; i++)
        if (m->col[i][0] == a && m->col[i][1] == b && m->mult[i] == 1)
            return 1;
    return 0;
}

static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)user_data;
    if (nevents < MAX_EVENTS) {
        event_t *e = &events[nevents];
        snprintf(e->relation, sizeof(e->relation), "%s",
            relation ? relation : "");
        e->ncols = ncols;
        e->col[0] = ncols > 0 ? row[0] : 0;
        e->col[1] = ncols > 1 ? row[1] : 0;
        e->diff = diff;
        if (strcmp(e->relation, "started") == 0)
            mirror_apply(&started_mirror, e);
        nevents++;
    }
}

/* Stable addon: the same argument always yields the same value. */
static int
stable_action(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)user_data;
    calls++;
    if (nargs > 0 && ncall_args < MAX_ARGS)
        call_args[ncall_args++] = args[0].as.int64_value;
    result->type = WIRELOG_EXTENSION_VALUE_INT64;
    result->size = sizeof(int64_t);
    result->as.int64_value =
        (nargs > 0 ? args[0].as.int64_value : 0) * 10;
    return 0;
}

/* Unstable addon: a fresh value on every invocation, which is what a host
 * handing out tickets does.  Declares no PURE or DETERMINISTIC bit, and
 * nothing in the engine asks for one. */
static int
unstable_action(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)user_data;
    (void)args;
    (void)nargs;
    calls++;
    result->type = WIRELOG_EXTENSION_VALUE_INT64;
    result->size = sizeof(int64_t);
    result->as.int64_value = ++next_ticket;
    return 0;
}

/* ======================================================================== */
/* Session fixture                                                          */
/* ======================================================================== */

/* `started` reads only `ready`; `echo` reads only `other`.  The two rules
 * share no relation, which is what makes the `other` mutations below a
 * control rather than a second measurement of the same thing. */
static const char *const REPLAY_PROGRAM =
    ".decl ready(id: int64)\n"
    ".decl other(k: int64)\n"
    ".decl started(id: int64, ticket: int64)\n"
    ".decl echo(k: int64)\n"
    "started(id, @call(\"replay.action\", id)) :- ready(id).\n"
    "echo(k) :- other(k).\n";

/* The same call site behind a join.  `enabled` admits only some of `ready`,
 * which separates "rows the rule derives" from "rows the relation holds" --
 * the two coincide in REPLAY_PROGRAM and only there. */
static const char *const JOIN_PROGRAM =
    ".decl ready(id: int64)\n"
    ".decl enabled(id: int64)\n"
    ".decl started(id: int64, ticket: int64)\n"
    "started(id, @call(\"replay.action\", id)) :- ready(id),"
    " enabled(id).\n";

typedef struct {
    wirelog_extension_registry_t *registry;
    wirelog_extension_snapshot_t *snapshot;
    wirelog_easy_session_t *session;
} fixture_t;

static char message[256];

static const char *
fixture_open(fixture_t *fx, const char *program, uint32_t policy,
    wirelog_extension_scalar_fn fn)
{
    uint32_t types[] = { WIRELOG_EXTENSION_VALUE_INT64 };
    wirelog_extension_descriptor_t descriptor = {
        WIRELOG_EXTENSION_ABI_VERSION, sizeof(descriptor), "replay.action",
        1, types, WIRELOG_EXTENSION_VALUE_INT64, fn, NULL, NULL
    };
    /* The initializer above stops short of the appended fields. */
    descriptor.callback_policy = policy;

    memset(fx, 0, sizeof(*fx));
    fx->registry = wirelog_extension_registry_create();
    if (!fx->registry)
        return "registry_create";
    if (wirelog_extension_register(fx->registry, &descriptor) != 0) {
        /* Nothing is registered, so tear the registry down here: the closer
         * would stop at its own unregister step and leak it.  Copy the
         * diagnostic out first -- wirelog_extension_last_error() points into
         * a thread-local buffer that registry_destroy blanks on success, so
         * returning that pointer would report an empty reason. */
        snprintf(message, sizeof(message), "register: %s",
            wirelog_extension_last_error());
        wirelog_extension_registry_destroy(fx->registry);
        fx->registry = NULL;
        return message;
    }
    fx->snapshot = wirelog_extension_snapshot_acquire(fx->registry);
    if (!fx->snapshot)
        return "snapshot_acquire";

    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.eager_build = true;
    opts.num_workers = 1;
    opts.extension_snapshot = fx->snapshot;
    wirelog_error_t rc =
        wirelog_easy_open_opts(program, &opts, &fx->session);
    if (rc != WIRELOG_OK) {
        snprintf(message, sizeof(message), "open_opts returned %d", (int)rc);
        return message;
    }
    if (wirelog_easy_set_delta_cb(fx->session, on_delta, NULL) != WIRELOG_OK)
        return "set_delta_cb";
    return NULL;
}

/* Closes in the order the extension ABI requires: the session first, then
 * this reference to the snapshot, then the registration.  A non-zero
 * registry_destroy would mean a snapshot or lease reference outlived them. */
static const char *
fixture_close(fixture_t *fx)
{
    const char *why = NULL;
    if (fx->session)
        wirelog_easy_close(fx->session);
    if (fx->snapshot)
        wirelog_extension_snapshot_release(fx->snapshot);
    if (fx->registry) {
        if (wirelog_extension_unregister(fx->registry, "replay.action") != 0)
            why = "unregister";
        else if (wirelog_extension_registry_destroy(fx->registry) != 0)
            why = "registry_destroy reports an outstanding reference";
    }
    memset(fx, 0, sizeof(*fx));
    return why;
}

/* Steps once and reports how many times the addon ran.  A step that fails is
 * never silently read as an absence of invocations: on a route where a
 * scalar INT64 @call is refused, step() reports the error and the caller
 * fails the case instead of recording a zero. */
static const char *
step_once(fixture_t *fx, unsigned *invocations)
{
    unsigned before = calls;
    wirelog_error_t rc;

    observation_reset();
    rc = wirelog_easy_step(fx->session);
    if (rc != WIRELOG_OK) {
        snprintf(message, sizeof(message), "step returned %d", (int)rc);
        return message;
    }
    *invocations = calls - before;
    return NULL;
}

static const char *
expect_count(unsigned got, unsigned want, const char *what)
{
    if (got == want)
        return NULL;
    snprintf(message, sizeof(message), "%s: got %u, want %u", what, got,
        want);
    return message;
}

/* Args are compared as a multiset: the order in which the evaluator walks
 * the rows is not part of what this test pins. */
static const char *
expect_args(const int64_t *want, unsigned nwant, const char *what)
{
    int64_t got[MAX_ARGS];
    unsigned n = ncall_args < MAX_ARGS ? ncall_args : MAX_ARGS;

    if (n != nwant) {
        snprintf(message, sizeof(message), "%s: %u args, want %u", what, n,
            nwant);
        return message;
    }
    memcpy(got, call_args, n * sizeof(got[0]));
    for (unsigned i = 0; i + 1 < n; i++)
        for (unsigned j = 0; j + 1 < n - i; j++)
            if (got[j] > got[j + 1]) {
                int64_t swap = got[j];
                got[j] = got[j + 1];
                got[j + 1] = swap;
            }
    for (unsigned i = 0; i < n; i++)
        if (got[i] != want[i]) {
            snprintf(message, sizeof(message),
                "%s: arg %u is %" PRId64 ", want %" PRId64, what, i, got[i],
                want[i]);
            return message;
        }
    return NULL;
}

static const char *
expect_one_event(const char *relation, int64_t a, int64_t b, int32_t diff,
    const char *what)
{
    if (nevents != 1) {
        snprintf(message, sizeof(message), "%s: %u events, want 1", what,
            nevents);
        return message;
    }
    if (strcmp(events[0].relation, relation) != 0
        || events[0].diff != diff
        || events[0].col[0] != a
        || (events[0].ncols > 1 && events[0].col[1] != b)) {
        snprintf(message, sizeof(message),
            "%s: got %s%+d(%" PRId64 ",%" PRId64 ")", what,
            events[0].relation, (int)events[0].diff, events[0].col[0],
            events[0].col[1]);
        return message;
    }
    return NULL;
}

/* ======================================================================== */
/* Cases                                                                    */
/* ======================================================================== */

/* Drives the sequence both count cases share and checks the invocation
 * counts.  The first step is the anchor: five invocations with the five
 * loaded arguments prove the rule fires, the addon resolved, and the scalar
 * INT64 result reached the head, so a later zero can never be mistaken for a
 * clean baseline. */
static const char *
drive_sequence(fixture_t *fx)
{
    static const int64_t loaded[] = { 1, 2, 3, 4, 5 };
    static const int64_t survivors[] = { 2, 3, 4, 5 };
    static const int64_t regrown[] = { 2, 3, 4, 5, 99 };
    const char *why;
    unsigned n;
    int64_t value;

    for (unsigned i = 0; i < 5; i++) {
        value = (int64_t)(i + 1);
        if (wirelog_easy_insert(fx->session, "ready", &value, 1)
            != WIRELOG_OK)
            return "insert ready";
    }
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 5, "load")) != NULL)
        return why;
    if ((why = expect_args(loaded, 5, "load args")) != NULL)
        return why;
    if ((why = expect_count(nevents, 5, "load events")) != NULL)
        return why;
    if ((why = expect_count(mirror_live_rows(&started_mirror), 5,
        "rows after load")) != NULL)
        return why;

    /* Two idle steps.  Zero invocations and zero events here are what make
     * every other count in this case meaningful: they show the counter does
     * not simply advance with the number of steps. */
    for (unsigned i = 0; i < 2; i++) {
        if ((why = step_once(fx, &n)) != NULL)
            return why;
        if ((why = expect_count(n, 0, "idle step")) != NULL)
            return why;
        if ((why = expect_count(nevents, 0, "idle step events")) != NULL)
            return why;
    }

    /* Mutating a relation the rule does not read leaves the addon alone and
     * the derived relation untouched, while the rule that does read it
     * publishes its own event. */
    value = 7;
    if (wirelog_easy_insert(fx->session, "other", &value, 1) != WIRELOG_OK)
        return "insert other";
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 0, "insert into an unread relation")) != NULL)
        return why;
    if ((why = expect_one_event("echo", 7, 0, +1, "insert other")) != NULL)
        return why;
    if ((why = expect_count(mirror_live_rows(&started_mirror), 5,
        "rows after insert other")) != NULL)
        return why;

    value = 7;
    if (wirelog_easy_remove(fx->session, "other", &value, 1) != WIRELOG_OK)
        return "remove other";
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 0, "remove from an unread relation")) != NULL)
        return why;
    if ((why = expect_one_event("echo", 7, 0, -1, "remove other")) != NULL)
        return why;

    /* Issue #1986's report.  One row is retracted and the addon runs again
     * for each of the four that survive, publishing nothing for them. */
    value = 1;
    if (wirelog_easy_remove(fx->session, "ready", &value, 1) != WIRELOG_OK)
        return "remove ready";
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 4, "retraction with four survivors")) != NULL)
        return why;
    if ((why = expect_args(survivors, 4, "retraction args")) != NULL)
        return why;
    /* Exactly one event only because this addon answers the same value for
     * the same argument.  test_unstable_value_republishes_unchanged_rows is
     * the same step with an unstable addon, and publishes five. */
    if ((why = expect_one_event("started", 1, 10, -1, "retraction")) != NULL)
        return why;
    if ((why = expect_count(mirror_live_rows(&started_mirror), 4,
        "rows after retraction")) != NULL)
        return why;

    /* Growing the source again invokes the addon once per row the rule now
     * derives, not once for the row that arrived, and still publishes one
     * event.  With the step above that is four, then five: the count follows
     * the derivation, not the size of the change.  This rule has a single
     * body atom, so here the derivation happens to be the source relation's
     * rows; test_selective_body_narrows_the_count separates the two. */
    value = 99;
    if (wirelog_easy_insert(fx->session, "ready", &value, 1) != WIRELOG_OK)
        return "insert ready(99)";
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 5, "insertion into a four-row relation"))
        != NULL)
        return why;
    if ((why = expect_args(regrown, 5, "regrowth args")) != NULL)
        return why;
    if ((why = expect_one_event("started", 99, 990, +1, "regrowth")) != NULL)
        return why;
    if ((why = expect_count(mirror_live_rows(&started_mirror), 5,
        "rows after regrowth")) != NULL)
        return why;

    if (!mirror_holds(&started_mirror, 2, 20)
        || !mirror_holds(&started_mirror, 99, 990)
        || mirror_holds(&started_mirror, 1, 10)) {
        snprintf(message, sizeof(message),
            "derived relation contents disagree with the deltas");
        return message;
    }

    /* Emptying the source leaves nothing to derive, so the callback does not
     * run even though the step does a great deal of work: it retracts all
     * five derived rows.  This is the reporter's own control observation in
     * issue #1986, and the only route by which the doc's "a rule that derives
     * nothing does not invoke it at all" is pinned. */
    {
        static const int64_t remaining[] = { 2, 3, 4, 5, 99 };
        for (unsigned i = 0; i < 5; i++) {
            int64_t doomed = remaining[i];
            if (wirelog_easy_remove(fx->session, "ready", &doomed, 1)
                != WIRELOG_OK)
                return "remove the remaining rows";
        }
    }
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 0, "step that empties the source")) != NULL)
        return why;
    if ((why = expect_count(nevents, 5, "events emptying the source"))
        != NULL)
        return why;
    if ((why = expect_count(mirror_live_rows(&started_mirror), 0,
        "rows after the source is emptied")) != NULL)
        return why;
    if ((why = step_once(fx, &n)) != NULL)
        return why;
    if ((why = expect_count(n, 0, "idle step over an empty source")) != NULL)
        return why;
    return NULL;
}

static void
test_invocation_count_follows_the_derivation(void)
{
    fixture_t fx;
    const char *why;

    TEST("scalar @call runs once per row the rule derives");
    calls = 0;
    memset(&started_mirror, 0, sizeof(started_mirror));
    why = fixture_open(&fx, REPLAY_PROGRAM,
            WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE, stable_action);
    if (!why)
        why = drive_sequence(&fx);
    if (!why)
        why = expect_count(calls, 14, "total invocations");
    {
        const char *teardown = fixture_close(&fx);
        if (!why)
            why = teardown;
    }
    if (why)
        FAIL(why);
    else
        PASS();
}

/* PURE and DETERMINISTIC are recorded on the descriptor the session resolves
 * and change nothing about how often the callback runs.  This case fails the
 * moment either bit acquires a meaning, which is correct: that would be a
 * contract change and docs/SEMANTICS.md must move with it. */
static void
test_purity_bits_do_not_change_the_count(void)
{
    const uint32_t policy = WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE
        | WIRELOG_EXTENSION_CALLBACK_PURE
        | WIRELOG_EXTENSION_CALLBACK_DETERMINISTIC;
    fixture_t fx;
    const char *why;

    TEST("declaring PURE and DETERMINISTIC leaves the count unchanged");
    calls = 0;
    memset(&started_mirror, 0, sizeof(started_mirror));
    why = fixture_open(&fx, REPLAY_PROGRAM, policy, stable_action);
    if (!why) {
        const wirelog_extension_descriptor_t *resolved =
            wirelog_extension_snapshot_find(fx.snapshot, "replay.action");
        if (!resolved)
            why = "snapshot_find did not resolve the addon";
        else if (resolved->callback_policy != policy)
            why = "the snapshot did not carry the declared policy";
    }
    if (!why)
        why = drive_sequence(&fx);
    if (!why)
        why = expect_count(calls, 14, "total invocations");
    {
        const char *teardown = fixture_close(&fx);
        if (!why)
            why = teardown;
    }
    if (why)
        FAIL(why);
    else
        PASS();
}

/* The consequence the invocation count has for a host that does not return a
 * stable value.  Re-deriving an unchanged row with a fresh value retracts it
 * at the old value and republishes it at the new one, so the host sees a
 * -1/+1 pair for a row whose input never moved.  This is what makes value
 * stability an obligation rather than a suggestion. */
static void
test_unstable_value_republishes_unchanged_rows(void)
{
    fixture_t fx;
    const char *why;
    unsigned n;
    int64_t value;

    TEST("an unstable return value republishes rows whose input did not move");
    calls = 0;
    next_ticket = 0;
    memset(&started_mirror, 0, sizeof(started_mirror));
    why = fixture_open(&fx, REPLAY_PROGRAM,
            WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE, unstable_action);
    if (!why) {
        for (unsigned i = 0; i < 3 && !why; i++) {
            value = (int64_t)(i + 1);
            if (wirelog_easy_insert(fx.session, "ready", &value, 1)
                != WIRELOG_OK)
                why = "insert ready";
        }
    }
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 3, "load");
    if (!why)
        why = expect_count(nevents, 3, "load events");
    if (!why) {
        value = 1;
        if (wirelog_easy_remove(fx.session, "ready", &value, 1)
            != WIRELOG_OK)
            why = "remove ready";
    }
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 2, "retraction with two survivors");
    /* Five events: the retraction of the removed row, plus a -1/+1 pair for
     * each survivor whose ticket was re-minted. */
    if (!why)
        why = expect_count(nevents, 5, "retraction events");
    if (!why) {
        unsigned retractions = 0, assertions = 0;
        for (unsigned i = 0; i < nevents; i++) {
            if (events[i].diff < 0)
                retractions++;
            else
                assertions++;
        }
        if (retractions != 3 || assertions != 2)
            why = "the retraction did not republish both survivors";
    }
    if (!why)
        why = expect_count(mirror_live_rows(&started_mirror), 2,
                "rows after retraction");
    {
        const char *teardown = fixture_close(&fx);
        if (!why)
            why = teardown;
    }
    if (why)
        FAIL(why);
    else
        PASS();
}

/* The count follows the rule's derivation, which a selective body narrows
 * below the row count of the relation the rule reads.  This is the shape in
 * which the earlier wording of docs/SEMANTICS.md -- "once for every row the
 * relation then holds" -- was false, and the shape a host with a join over a
 * large EDB actually has. */
static void
test_selective_body_narrows_the_count(void)
{
    fixture_t fx;
    const char *why;
    unsigned n;
    int64_t value;
    static const int64_t enabled_only[] = { 4, 5 };
    static const int64_t still_enabled[] = { 5 };
    static const int64_t enabled_again[] = { 3, 5 };
    static const int64_t repeated[] = { 3, 3, 5 };

    TEST("a selective body narrows the count below the relation's size");
    calls = 0;
    memset(&started_mirror, 0, sizeof(started_mirror));
    why = fixture_open(&fx, JOIN_PROGRAM,
            WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE, stable_action);
    for (unsigned i = 0; i < 5 && !why; i++) {
        value = (int64_t)(i + 1);
        if (wirelog_easy_insert(fx.session, "ready", &value, 1) != WIRELOG_OK)
            why = "insert ready";
    }
    for (unsigned i = 0; i < 2 && !why; i++) {
        value = enabled_only[i];
        if (wirelog_easy_insert(fx.session, "enabled", &value, 1)
            != WIRELOG_OK)
            why = "insert enabled";
    }
    /* Five `ready` rows, two of them enabled: two derived rows, two calls. */
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 2, "load with two of five rows enabled");
    if (!why)
        why = expect_args(enabled_only, 2, "load args");
    if (!why)
        why = expect_count(nevents, 2, "load events");

    /* Retracting a row the body does not admit changes no derived row, so
     * the step publishes nothing at all -- and still runs the callback once
     * per derived row.  Two invocations, zero events: this is the sharpest
     * form of the invisibility the doc describes. */
    if (!why) {
        value = 1;
        if (wirelog_easy_remove(fx.session, "ready", &value, 1) != WIRELOG_OK)
            why = "remove ready(1)";
    }
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 2, "retracting a row the body excludes");
    if (!why)
        why = expect_args(enabled_only, 2, "excluded-row args");
    if (!why)
        why = expect_count(nevents, 0, "excluded-row events");

    /* Retracting one the body does admit drops the derivation to one row. */
    if (!why) {
        value = 4;
        if (wirelog_easy_remove(fx.session, "ready", &value, 1) != WIRELOG_OK)
            why = "remove ready(4)";
    }
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 1, "retracting a row the body admits");
    if (!why)
        why = expect_args(still_enabled, 1, "admitted-row args");
    if (!why)
        why = expect_one_event("started", 4, 40, -1, "admitted-row event");

    /* Widening the body admits a row that was there all along. */
    if (!why) {
        value = 3;
        if (wirelog_easy_insert(fx.session, "enabled", &value, 1)
            != WIRELOG_OK)
            why = "insert enabled(3)";
    }
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 2, "widening the body");
    if (!why)
        why = expect_args(enabled_again, 2, "widened args");
    if (!why)
        why = expect_one_event("started", 3, 30, +1, "widened event");

    /* A second copy of enabled(3) is a second substitution deriving the same
     * head row, and the callback runs again for it.  The derived row is
     * unchanged, so the step publishes nothing: another shape in which the
     * delta stream cannot account for the work.  This is what the doc means
     * by once more for each repeated derivation of the same row. */
    if (!why) {
        value = 3;
        if (wirelog_easy_insert(fx.session, "enabled", &value, 1)
            != WIRELOG_OK)
            why = "insert enabled(3) a second time";
    }
    if (!why)
        why = step_once(&fx, &n);
    if (!why)
        why = expect_count(n, 3, "a repeated derivation of one row");
    if (!why)
        why = expect_args(repeated, 3, "repeated-derivation args");
    if (!why)
        why = expect_count(nevents, 0, "repeated-derivation events");

    if (!why)
        why = expect_count(calls, 10, "total invocations");
    {
        const char *teardown = fixture_close(&fx);
        if (!why)
            why = teardown;
    }
    if (why)
        FAIL(why);
    else
        PASS();
}

int
main(void)
{
    printf("Scalar addon invocation count (#1986)\n");
    printf("=====================================\n\n");

    test_invocation_count_follows_the_derivation();
    test_selective_body_narrows_the_count();
    test_purity_bits_do_not_change_the_count();
    test_unstable_value_republishes_unchanged_rows();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
