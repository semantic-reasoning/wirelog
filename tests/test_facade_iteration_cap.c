/*
 * Public facades at the fixed-point iteration limit (#2345).
 *
 * A raw wl_session_snapshot()/wl_session_step() returns 0 when recursive
 * evaluation stops at the iteration limit, and a raw snapshot delivers the
 * partial image.  The public facades must instead return
 * WIRELOG_ERR_ITERATION_LIMIT and deliver no rows.  The limit is lowered with
 * the WL_SESSION_TEST_HOOKS override; production uses a fixed limit.
 *
 * Fixture: r(X) :- seed(X).  p(X) :- r(X).  q(X) :- p(X).
 * p(Y) :- q(X), edge(X,Y).  t(X) :- p(X), X > 10.  With seed 0 and edges
 * i -> i+1 for i < 13, p and q are exactly {0..13}, r is {0}, t is {11..13};
 * r's stratum precedes the p/q SCC and t's follows it.
 *
 * WIRELOG_TDD_MIN_ROWS_PER_WORKER=1 makes W=2 snapshots use their workers on
 * this small input; WIRELOG_TDD_STRATUM_PROFILE enables the TDD audit that
 * witnesses it.  WIRELOG_TDD_CORRECTNESS_CHECK is cleared so no serial
 * replay decides a cap that is attributed to the TDD route.
 */

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L /* setenv */
#endif

#include "../wirelog/wirelog-easy.h"
#include "../wirelog/wirelog-advanced.h"
#include "../wirelog/columnar/internal.h"

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
    return _putenv_s(name, value ? value : "");
}
#  define setenv wl_test_setenv_
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}
#  define unsetenv wl_test_unsetenv_
#endif

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

wl_session_t *wirelog_easy_testhook_inner(wirelog_easy_session_t *s);
wl_session_t *wirelog_session_testhook_inner(wirelog_session_t *session);
extern void (*wl_columnar_session_test_before_stable_emit)(
    wl_col_session_t *sess);
extern void (*wl_columnar_eval_serial_test_after_plan)(wl_col_session_t *sess,
    eval_stack_t *stack, eval_entry_t *result);

enum { LENGTH = 13, NVALUES = 16 };

static const char *program_text =
    ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
    ".decl p(x:int64)\n.decl q(x:int64)\n"
    ".decl r(x:int64)\n.decl t(x:int64)\n"
    "r(X) :- seed(X).\n"
    "p(X) :- r(X).\nq(X) :- p(X).\n"
    "p(Y) :- q(X), edge(X,Y).\n"
    "t(X) :- p(X), X > 10.\n";

/* A single-head linear closure: the shape the W>=2 TDD snapshot route takes
 * (the mutual p/q SCC above falls back to serial for want of an exchange
 * key).  p is exactly {0..13}. */
static const char *tdd_program_text =
    ".decl seed(x:int64)\n.decl edge(x:int64,y:int64)\n"
    ".decl p(x:int64)\n"
    "p(X) :- seed(X).\n"
    "p(Y) :- p(X), edge(X,Y).\n";

static void
set_limit(uint32_t limit)
{
    wl_columnar_eval_serial_test_iteration_limit = limit;
    wl_columnar_session_test_iteration_limit = limit;
}

/* ---- row capture ------------------------------------------------------- */

typedef struct {
    const char *relation;    /* NULL: accept any relation */
    unsigned seen[NVALUES];
    unsigned count;
    unsigned foreign;        /* rows of another relation */
} capture_t;

static void
capture_reset(capture_t *c, const char *relation)
{
    memset(c, 0, sizeof(*c));
    c->relation = relation;
}

static void
capture_row(const char *name, const int64_t *values, uint32_t width,
    void *data)
{
    capture_t *c = data;
    if (c->relation && strcmp(name, c->relation)) {
        c->foreign++;
        return;
    }
    CHECK(width == 1 && values[0] >= 0 && values[0] < NVALUES);
    CHECK(!c->seen[values[0]]++); /* no duplicate row */
    c->count++;
}

/* The capture holds exactly the values lo..hi. */
static bool
capture_exact(const capture_t *c, int64_t lo, int64_t hi)
{
    unsigned want = 0;
    for (int64_t v = 0; v < NVALUES; v++) {
        bool in = v >= lo && v <= hi;
        if (c->seen[v] != (in ? 1u : 0u))
            return false;
        want += in;
    }
    return c->count == want && !c->foreign;
}

/* ---- session helpers --------------------------------------------------- */

static wirelog_easy_session_t *
open_program(const char *text, uint32_t workers, bool with_seed)
{
    wirelog_easy_open_opts_t o = WIRELOG_EASY_OPEN_OPTS_INIT;
    o.num_workers = workers;
    o.eager_build = true;
    wirelog_easy_session_t *s = NULL;
    CHECK(wirelog_easy_open_opts(text, &o, &s) == WIRELOG_OK && s);
    if (with_seed) {
        int64_t seed = 0;
        CHECK(wirelog_easy_insert(s, "seed", &seed, 1) == WIRELOG_OK);
    }
    for (int64_t i = 0; i < LENGTH; i++) {
        int64_t edge[] = { i, i + 1 };
        CHECK(wirelog_easy_insert(s, "edge", edge, 2) == WIRELOG_OK);
    }
    return s;
}

static wirelog_easy_session_t *
open_session(uint32_t workers, bool with_seed)
{
    return open_program(program_text, workers, with_seed);
}

static wl_col_session_t *
inner(wirelog_easy_session_t *s)
{
    return (wl_col_session_t *)wirelog_easy_testhook_inner(s);
}

/* What a capped call must leave behind so the next call evaluates again. */
static void
check_retry_state(wl_col_session_t *cs, bool committed)
{
    CHECK(cs->pending_input_change && cs->pending_full_input_eval);
    CHECK(!cs->snapshot_stable_valid);
    CHECK(cs->has_evaluated == committed);
    CHECK(!cs->plain_step_completion_pending);
    CHECK(!cs->facade_cap_withhold && !cs->facade_cap_hit);
}

static void
reset_witness(wl_col_session_t *cs)
{
    cs->tdd_max_active_workers = 0;
}

/*
 * A W=2 snapshot of a one-stratum program was decided by the TDD loop with
 * two workers, not by a serial fallback or replay.  tdd_audit is written
 * while WIRELOG_TDD_STRATUM_PROFILE is set and keeps the last stratum's.
 */
static void
check_tdd_witness(wl_col_session_t *cs, uint32_t workers)
{
    if (workers < 2)
        return;
    CHECK(cs->tdd_audit.selected_workers >= 2);
    CHECK(cs->tdd_audit.replay == NULL);
    CHECK(cs->tdd_max_active_workers >= 2);
}

static wirelog_error_t
snap(wirelog_easy_session_t *s, const char *relation, capture_t *c)
{
    capture_reset(c, relation);
    return wirelog_easy_snapshot(s, relation, capture_row, c);
}

/* Stratum index of the stratum that defines @name. */
static uint32_t
stratum_of(wl_col_session_t *cs, const char *name)
{
    const wl_plan_t *plan = cs->plan;
    for (uint32_t i = 0; i < plan->stratum_count; i++)
        for (uint32_t j = 0; j < plan->strata[i].relation_count; j++)
            if (!strcmp(plan->strata[i].relations[j].name, name))
                return i;
    CHECK(false);
    return UINT32_MAX;
}

static unsigned stable_emits;
static void
count_stable_emit(wl_col_session_t *sess)
{
    (void)sess;
    stable_emits++;
}

/* ---- easy snapshot ----------------------------------------------------- */

/* Cap hit, then a same-handle retry once the limit is restored. */
static void
easy_snapshot_cap_and_retry(uint32_t workers)
{
    wirelog_easy_session_t *s = open_session(workers, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    set_limit(1);
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(c.count == 0 && c.foreign == 0);
    check_retry_state(cs, false);
    /* The limit is fixed: the same input reaches it again (#2345). */
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    check_retry_state(cs, false);
    set_limit(0);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    CHECK(snap(s, "q", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    CHECK(snap(s, "r", &c) == WIRELOG_OK && capture_exact(&c, 0, 0));
    CHECK(snap(s, "t", &c) == WIRELOG_OK && capture_exact(&c, 11, LENGTH));
    wirelog_easy_close(s);
}

/* W=2: the cap is reached inside the TDD snapshot loop itself. */
static void
easy_snapshot_tdd_route(void)
{
    wirelog_easy_session_t *s = open_program(tdd_program_text, 2, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    set_limit(1);
    reset_witness(cs);
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    check_tdd_witness(cs, 2);
    check_retry_state(cs, false);
    set_limit(0);
    reset_witness(cs);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    check_tdd_witness(cs, 2);
    wirelog_easy_close(s);
}

/* The W=1 boundary: limit 4 allows enough passes, limit 3 does not. */
static void
easy_snapshot_boundary(void)
{
    wirelog_easy_session_t *s = open_session(1, true);
    capture_t c;
    set_limit(4);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    wirelog_easy_close(s);
    s = open_session(1, true);
    set_limit(3);
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    set_limit(0);
    wirelog_easy_close(s);
}

/* Relations outside the capped stratum are refused with the whole call:
 * r is in an earlier stratum and complete, t depends on p. */
static void
easy_snapshot_unrelated(uint32_t workers)
{
    wirelog_easy_session_t *s = open_session(workers, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    CHECK(stratum_of(cs, "r") < stratum_of(cs, "p"));
    CHECK(stratum_of(cs, "p") == stratum_of(cs, "q"));
    CHECK(stratum_of(cs, "t") > stratum_of(cs, "p"));
    set_limit(1);
    CHECK(snap(s, "r", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    CHECK(snap(s, "t", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    check_retry_state(cs, false);
    set_limit(0);
    CHECK(snap(s, "r", &c) == WIRELOG_OK && capture_exact(&c, 0, 0));
    CHECK(snap(s, "t", &c) == WIRELOG_OK && capture_exact(&c, 11, LENGTH));
    wirelog_easy_close(s);
}

/* Guards: an empty model and the unchanged-snapshot fast path stay OK. */
static void
easy_snapshot_empty_and_fast_path(uint32_t workers)
{
    wirelog_easy_session_t *s = open_session(workers, false);
    capture_t c;
    set_limit(1);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && c.count == 0 && !c.foreign);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && c.count == 0 && !c.foreign);
    wirelog_easy_close(s);

    s = open_session(workers, true);
    set_limit(0);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    set_limit(1);
    stable_emits = 0;
    wl_columnar_session_test_before_stable_emit = count_stable_emit;
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    wl_columnar_session_test_before_stable_emit = NULL;
    CHECK(stable_emits == 1);
    set_limit(0);
    wirelog_easy_close(s);
}

/* A model dirtied after a successful snapshot, by a removal and by an EDB
 * insert.  wirelog_easy_insert() stages a full re-evaluation (it is not the
 * incremental insert, so the insert route is not taken), and that
 * re-evaluation reaches the limit. */
static void
easy_snapshot_dirtied(uint32_t workers)
{
    wirelog_easy_session_t *s = open_session(workers, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    int64_t last[] = { LENGTH - 1, LENGTH };
    CHECK(wirelog_easy_remove(s, "edge", last, 2) == WIRELOG_OK);
    set_limit(1);
    /* After the first evaluation the snapshot is not TDD-eligible, so this
     * capped re-evaluation runs serially whatever the worker count. */
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    check_retry_state(cs, true);
    set_limit(0);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH - 1));
    wirelog_easy_close(s);

    s = open_session(workers, true);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    int64_t more[] = { LENGTH, LENGTH + 1 };
    CHECK(wirelog_easy_insert(s, "edge", more, 2) == WIRELOG_OK);
    uint32_t routes = wl_columnar_session_test_insert_route_taken;
    set_limit(1);
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    CHECK(wl_columnar_session_test_insert_route_taken == routes);
    check_retry_state(inner(s), true);
    set_limit(0);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH + 1));
    wirelog_easy_close(s);
}

static unsigned raw_pq_rows;
static void
raw_pq_row(const char *name, const int64_t *values, uint32_t width,
    void *data)
{
    (void)values;
    (void)width;
    (void)data;
    if (!strcmp(name, "p") || !strcmp(name, "q"))
        raw_pq_rows++;
}

/* Guard: a raw capped snapshot inside the facade session is unchanged, and
 * a raw cap left behind is not reported by the next facade call. */
static void
easy_snapshot_raw_unchanged(void)
{
    wirelog_easy_session_t *s = open_session(1, true);
    wl_session_t *raw = wirelog_easy_testhook_inner(s);
    capture_t c;
    set_limit(1);
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    raw_pq_rows = 0;
    CHECK(wl_session_snapshot(raw, raw_pq_row, NULL) == 0);
    CHECK(raw_pq_rows > 0 && raw_pq_rows < 2 * (LENGTH + 1));
    set_limit(0);
    raw_pq_rows = 0;
    CHECK(wl_session_snapshot(raw, raw_pq_row, NULL) == 0);
    CHECK(raw_pq_rows == 2 * (LENGTH + 1));
    CHECK(((wl_col_session_t *)raw)->facade_cap_hit); /* stale raw cap */
    set_limit(1);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    set_limit(0);
    wirelog_easy_close(s);
}

/* A host callback that re-enters the facade does not disturb the outer
 * call's scope. */
typedef struct {
    wirelog_easy_session_t *s;
    capture_t outer;
    wirelog_error_t inner_rc;
    bool entered;
} reenter_t;

static void
reenter_row(const char *name, const int64_t *values, uint32_t width,
    void *data)
{
    reenter_t *r = data;
    capture_row(name, values, width, &r->outer);
    if (r->entered)
        return;
    r->entered = true;
    capture_t c;
    r->inner_rc = snap(r->s, "q", &c);
    CHECK(r->inner_rc != WIRELOG_OK || capture_exact(&c, 0, LENGTH));
}

static void
easy_snapshot_reentrant(void)
{
    wirelog_easy_session_t *s = open_session(1, true);
    wl_col_session_t *cs = inner(s);
    reenter_t r;
    memset(&r, 0, sizeof(r));
    r.s = s;
    capture_reset(&r.outer, "p");
    set_limit(0);
    CHECK(wirelog_easy_snapshot(s, "p", reenter_row, &r) == WIRELOG_OK);
    CHECK(r.entered && capture_exact(&r.outer, 0, LENGTH));
    CHECK(!cs->facade_cap_withhold && !cs->facade_cap_hit);
    wirelog_easy_close(s);
}

/* The snapshot that drains a pending plain STEP (W=1) withholds its rows
 * when the drained step reaches the limit. */
static unsigned drain_calls, drain_target;
static wl_columnar_source_access_reader_t drain_reader;
static int drain_rc;

static void
drain_after_plan(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)sess;
    (void)stack;
    if (++drain_calls != drain_target || !result->owned || !result->rel)
        return;
    drain_rc = col_rel_source_reader_acquire(result->rel, &drain_reader);
}

static void
easy_snapshot_drained_step(void)
{
    wirelog_easy_session_t *s = open_session(1, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    CHECK(cs->num_workers == 1 && !cs->delta_cb && !cs->delta_observer);
    CHECK(stratum_of(cs, "r") == 0 && stratum_of(cs, "p") == 1);
    /* Fail the plain step in the SCC after stratum 0 committed. */
    drain_calls = 0;
    drain_target = 2;
    drain_rc = -1;
    wl_columnar_eval_serial_test_after_plan = drain_after_plan;
    CHECK(wirelog_easy_step(s) != WIRELOG_OK);
    wl_columnar_eval_serial_test_after_plan = NULL;
    CHECK(drain_rc == 0);
    CHECK(col_rel_source_reader_release(&drain_reader) == 0);
    CHECK(cs->plain_step_completion_pending
        && !cs->plain_step_completion_active);
    set_limit(1);
    CHECK(snap(s, "p", &c) == WIRELOG_ERR_ITERATION_LIMIT && c.count == 0);
    check_retry_state(cs, false); /* the failed step was the first */
    set_limit(0);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    CHECK(snap(s, "r", &c) == WIRELOG_OK && capture_exact(&c, 0, 0));
    wirelog_easy_close(s);
}

/* ---- easy step ---------------------------------------------------------- */

/* Delta events of p and q, each (relation, value) at most once per step. */
typedef struct {
    unsigned plus[2][NVALUES];
    unsigned count, minus, foreign;
} deltas_t;

static void
delta_row(const char *name, const int64_t *values, uint32_t width, int diff,
    void *data)
{
    deltas_t *d = data;
    if (strcmp(name, "p") && strcmp(name, "q")) {
        d->foreign++;
        return;
    }
    CHECK(width == 1 && values[0] >= 0 && values[0] < NVALUES);
    if (diff < 0) {
        d->minus++;
        return;
    }
    CHECK(diff == 1 && !d->plus[name[0] == 'q'][values[0]]++);
    d->count++;
}

/* Exactly one +1 for each of p and q over lo..hi, and nothing else of p/q. */
static bool
deltas_exact(const deltas_t *d, int64_t lo, int64_t hi)
{
    unsigned want = 0;
    for (unsigned h = 0; h < 2; h++)
        for (int64_t v = 0; v < NVALUES; v++) {
            bool in = v >= lo && v <= hi;
            if (d->plus[h][v] != (in ? 1u : 0u))
                return false;
            want += in;
        }
    return d->count == want && d->minus == 0;
}

/* Plain step (no delta callback). */
static void
easy_step_plain(uint32_t workers)
{
    wirelog_easy_session_t *s = open_session(workers, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    set_limit(1);
    CHECK(wirelog_easy_step(s) == WIRELOG_ERR_ITERATION_LIMIT);
    check_retry_state(cs, false);
    CHECK(wirelog_easy_step(s) == WIRELOG_ERR_ITERATION_LIMIT);
    check_retry_state(cs, false);
    set_limit(0);
    CHECK(wirelog_easy_step(s) == WIRELOG_OK);
    stable_emits = 0;
    wl_columnar_session_test_before_stable_emit = count_stable_emit;
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    wl_columnar_session_test_before_stable_emit = NULL;
    CHECK(stable_emits == 1);
    /* Dirtied by a removal after a committed step. */
    int64_t last[] = { LENGTH - 1, LENGTH };
    CHECK(wirelog_easy_remove(s, "edge", last, 2) == WIRELOG_OK);
    set_limit(1);
    CHECK(wirelog_easy_step(s) == WIRELOG_ERR_ITERATION_LIMIT);
    check_retry_state(cs, true);
    set_limit(0);
    CHECK(wirelog_easy_step(s) == WIRELOG_OK);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH - 1));
    wirelog_easy_close(s);
}

/* W=2: the plain step's cap is reached with two workers selected. */
static void
easy_step_plain_tdd(void)
{
    wirelog_easy_session_t *s = open_program(tdd_program_text, 2, true);
    wl_col_session_t *cs = inner(s);
    capture_t c;
    set_limit(1);
    reset_witness(cs);
    CHECK(wirelog_easy_step(s) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(cs->tdd_max_active_workers >= 2);
    check_retry_state(cs, false);
    set_limit(0);
    CHECK(wirelog_easy_step(s) == WIRELOG_OK);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH));
    wirelog_easy_close(s);
}

/* Delta-callback step.  A step with an observer evaluates on the serial
 * path whatever the worker count.  A capped step reports no difference; on
 * a first evaluation the completing step reports the whole model, and on a
 * committed session it reports none (the observer baseline was given up at
 * the cap, #2348). */
static void
easy_step_delta(uint32_t workers)
{
    wirelog_easy_session_t *s = open_session(workers, true);
    wl_col_session_t *cs = inner(s);
    deltas_t d;
    capture_t c;
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_easy_set_delta_cb(s, delta_row, &d) == WIRELOG_OK);
    set_limit(1);
    CHECK(wirelog_easy_step(s) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(d.count == 0 && d.minus == 0 && d.foreign == 0);
    check_retry_state(cs, false);
    set_limit(0);
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_easy_step(s) == WIRELOG_OK && deltas_exact(&d, 0, LENGTH));

    int64_t last[] = { LENGTH - 1, LENGTH };
    CHECK(wirelog_easy_remove(s, "edge", last, 2) == WIRELOG_OK);
    set_limit(1);
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_easy_step(s) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(d.count == 0 && d.minus == 0);
    check_retry_state(cs, true);
    set_limit(0);
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_easy_step(s) == WIRELOG_OK);
    CHECK(d.count == 0 && d.minus == 0);
    CHECK(snap(s, "p", &c) == WIRELOG_OK && capture_exact(&c, 0, LENGTH - 1));
    /* The step after that reports from the completed model again. */
    CHECK(wirelog_easy_insert(s, "edge", last, 2) == WIRELOG_OK);
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_easy_step(s) == WIRELOG_OK && deltas_exact(&d, LENGTH,
        LENGTH));
    CHECK(wirelog_easy_set_delta_cb(s, NULL, NULL) == WIRELOG_OK);
    wirelog_easy_close(s);
}

/* ---- advanced snapshot ---------------------------------------------------- */

typedef struct {
    wirelog_program_t *program;
    wirelog_session_t *session;
    wl_col_session_t *inner;
} advanced_t;

static void
advanced_open(advanced_t *a, const char *text, uint32_t workers)
{
    wirelog_error_t err = WIRELOG_OK;
    a->program = wirelog_parse_string(text, &err);
    CHECK(a->program && err == WIRELOG_OK);
    a->session = NULL;
    CHECK(wirelog_session_create(a->program, WIRELOG_BACKEND_DEFAULT, workers,
        &a->session) == WIRELOG_OK && a->session);
    a->inner = (wl_col_session_t *)wirelog_session_testhook_inner(a->session);
    int64_t seed = 0, edges[2 * LENGTH];
    for (int64_t i = 0; i < LENGTH; i++) {
        edges[2 * i] = i;
        edges[2 * i + 1] = i + 1;
    }
    CHECK(wirelog_session_insert(a->session, "seed", &seed, 1, 1)
        == WIRELOG_OK);
    CHECK(wirelog_session_insert(a->session, "edge", edges, LENGTH, 2)
        == WIRELOG_OK);
}

static void
advanced_close(advanced_t *a)
{
    wirelog_session_destroy(a->session);
    wirelog_program_free(a->program);
}

/* Every IDB tuple, split by relation; p is checked exactly, the others
 * only counted. */
typedef struct {
    capture_t p, q;
    unsigned other;
} all_rows_t;

static void
all_row(const char *name, const int64_t *values, uint32_t width, void *data)
{
    all_rows_t *a = data;
    if (!strcmp(name, "p"))
        capture_row(name, values, width, &a->p);
    else if (!strcmp(name, "q"))
        capture_row(name, values, width, &a->q);
    else
        a->other++;
}

static wirelog_error_t
advanced_snap(advanced_t *a, all_rows_t *rows)
{
    memset(rows, 0, sizeof(*rows));
    capture_reset(&rows->p, "p");
    capture_reset(&rows->q, "q");
    return wirelog_session_snapshot(a->session, all_row, rows);
}

static void
advanced_snapshot(uint32_t workers)
{
    advanced_t a;
    all_rows_t rows;
    advanced_open(&a, program_text, workers);
    set_limit(1);
    CHECK(advanced_snap(&a, &rows) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(!rows.p.count && !rows.q.count && !rows.other);
    check_retry_state(a.inner, false);
    CHECK(advanced_snap(&a, &rows) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(!rows.p.count && !rows.q.count && !rows.other);
    set_limit(0);
    CHECK(advanced_snap(&a, &rows) == WIRELOG_OK);
    CHECK(capture_exact(&rows.p, 0, LENGTH) && capture_exact(&rows.q, 0,
        LENGTH));
    /* Guard: the stable fast path stays OK under the limit. */
    set_limit(1);
    stable_emits = 0;
    wl_columnar_session_test_before_stable_emit = count_stable_emit;
    CHECK(advanced_snap(&a, &rows) == WIRELOG_OK);
    wl_columnar_session_test_before_stable_emit = NULL;
    CHECK(stable_emits == 1 && capture_exact(&rows.p, 0, LENGTH));
    set_limit(0);
    advanced_close(&a);
}

/* ---- advanced typed snapshot ---------------------------------------------- */

typedef struct {
    unsigned seen[2][NVALUES];
    unsigned count, other;
} typed_rows_t;

static void
typed_row(const char *name, const wirelog_typed_row_v1_t *row, int32_t diff,
    void *data)
{
    typed_rows_t *t = data;
    if (strcmp(name, "p") && strcmp(name, "q")) {
        t->other++;
        return;
    }
    CHECK(diff == 1 && row->logical_ncols == 1 && row->physical_nlanes == 1);
    int64_t v = (int64_t)row->lanes[0];
    CHECK(v >= 0 && v < NVALUES && !t->seen[name[0] == 'q'][v]++);
    t->count++;
}

static bool
typed_exact(const typed_rows_t *t, int64_t lo, int64_t hi)
{
    unsigned want = 0;
    for (unsigned h = 0; h < 2; h++)
        for (int64_t v = 0; v < NVALUES; v++) {
            bool in = v >= lo && v <= hi;
            if (t->seen[h][v] != (in ? 1u : 0u))
                return false;
            want += in;
        }
    return t->count == want;
}

static void
advanced_snapshot_typed(uint32_t workers)
{
    advanced_t a;
    typed_rows_t t;
    advanced_open(&a, program_text, workers);
    set_limit(1);
    memset(&t, 0, sizeof(t));
    CHECK(wirelog_session_snapshot_typed(a.session, typed_row, &t)
        == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(!t.count && !t.other);
    check_retry_state(a.inner, false);
    set_limit(0);
    memset(&t, 0, sizeof(t));
    CHECK(wirelog_session_snapshot_typed(a.session, typed_row, &t)
        == WIRELOG_OK);
    CHECK(typed_exact(&t, 0, LENGTH));
    advanced_close(&a);
}

/* ---- advanced step -------------------------------------------------------- */

static void
typed_delta_row(const char *name, const wirelog_typed_row_v1_t *row,
    int32_t diff, void *data)
{
    deltas_t *d = data;
    CHECK(row->logical_ncols == 1 && row->physical_nlanes == 1);
    int64_t v = (int64_t)row->lanes[0];
    delta_row(name, &v, 1, diff, d);
}

/* mode 0: no callback, 1: delta callback, 2: typed delta callback. */
static void
advanced_step(uint32_t workers, unsigned mode)
{
    advanced_t a;
    deltas_t d;
    all_rows_t rows;
    advanced_open(&a, program_text, workers);
    memset(&d, 0, sizeof(d));
    if (mode == 1)
        CHECK(wirelog_session_set_delta_cb(a.session, delta_row, &d)
            == WIRELOG_OK);
    if (mode == 2)
        CHECK(wirelog_session_set_typed_delta_cb(a.session, typed_delta_row,
            &d) == WIRELOG_OK);
    set_limit(1);
    CHECK(wirelog_session_step(a.session) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(d.count == 0 && d.minus == 0 && d.foreign == 0);
    check_retry_state(a.inner, false);
    CHECK(wirelog_session_step(a.session) == WIRELOG_ERR_ITERATION_LIMIT);
    set_limit(0);
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_session_step(a.session) == WIRELOG_OK);
    CHECK(mode == 0 ? d.count == 0 : deltas_exact(&d, 0, LENGTH));

    int64_t last[] = { LENGTH - 1, LENGTH };
    CHECK(wirelog_session_remove(a.session, "edge", last, 1, 2)
        == WIRELOG_OK);
    set_limit(1);
    memset(&d, 0, sizeof(d));
    CHECK(wirelog_session_step(a.session) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(d.count == 0 && d.minus == 0);
    check_retry_state(a.inner, true);
    set_limit(0);
    CHECK(wirelog_session_step(a.session) == WIRELOG_OK);
    CHECK(d.count == 0 && d.minus == 0); /* baseline given up at the cap */
    if (mode != 2) {
        CHECK(wirelog_session_set_delta_cb(a.session, NULL, NULL)
            == WIRELOG_OK);
        CHECK(advanced_snap(&a, &rows) == WIRELOG_OK);
        CHECK(capture_exact(&rows.p, 0, LENGTH - 1));
    }
    advanced_close(&a);
}

/* W=2: a callback-free plain step capped with two workers active. */
static void
advanced_step_tdd(void)
{
    advanced_t a;
    advanced_open(&a, tdd_program_text, 2);
    set_limit(1);
    reset_witness(a.inner);
    CHECK(wirelog_session_step(a.session) == WIRELOG_ERR_ITERATION_LIMIT);
    CHECK(a.inner->tdd_max_active_workers >= 2);
    check_retry_state(a.inner, false);
    set_limit(0);
    CHECK(wirelog_session_step(a.session) == WIRELOG_OK);
    advanced_close(&a);
}

int
main(void)
{
    CHECK(!setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1));
    CHECK(!setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1));
    CHECK(!unsetenv("WIRELOG_TDD_CORRECTNESS_CHECK"));

    CHECK(strcmp(wirelog_error_string(WIRELOG_ERR_ITERATION_LIMIT),
        wirelog_error_string((wirelog_error_t)200)) != 0);

    for (uint32_t w = 1; w <= 2; w++) {
        easy_snapshot_cap_and_retry(w);
        easy_snapshot_unrelated(w);
        easy_snapshot_empty_and_fast_path(w);
        easy_snapshot_dirtied(w);
        easy_step_plain(w);
        easy_step_delta(w);
        advanced_snapshot(w);
        advanced_snapshot_typed(w);
        for (unsigned mode = 0; mode < 3; mode++)
            advanced_step(w, mode);
    }
    easy_snapshot_tdd_route();
    easy_snapshot_boundary();
    easy_snapshot_raw_unchanged();
    easy_snapshot_reentrant();
    easy_snapshot_drained_step();
    easy_step_plain_tdd();
    advanced_step_tdd();
    puts("facade iteration cap: easy and advanced facades passed");
    return 0;
}
