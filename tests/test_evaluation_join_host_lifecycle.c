/* Real private-head JOIN binding across STEP, SNAPSHOT and successors. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

typedef struct { bool a[9], b[8], denied[8], extra; } state_t;
typedef struct { int rows[2][9][9]; unsigned calls[2][9][9]; } capture_t;
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_evaluation_control_t *control;
    wl_session_t *sessions[2];
    capture_t events[2];
    state_t state;
} fixture_t;
static unsigned prepared, produced, acked, exchanges, probes, stop_event,
    stop_prepared;
static uint64_t generation, attempt, consumed;
static bool stopped, work_stop, strict_mode;
static wl_evaluation_control_t *snapshot_control;
static bool snapshot_cancelled, inject_funding, funding_pending, funding_used;
static unsigned funding_refusals;
static int snapshot_identity_exhaustion = -1;
static unsigned append_reads, append_joins, append_variables;
extern void (*wl_columnar_session_test_before_fresh_shape)(wl_col_session_t *);
static unsigned fresh_shape_reached;
static bool issued_cancel_enabled;
static unsigned issued_cancel_role, issued_cancel_hits;
static void native_report_same(const wl_evaluation_control_report_t *,
    const wl_evaluation_control_report_t *);
#if defined(_MSC_VER)
extern __declspec(thread) unsigned wl_columnar_session_test_step_reset_chunks;
#else
extern _Thread_local unsigned wl_columnar_session_test_step_reset_chunks;
#endif

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}

static unsigned
head_index(const char *name)
{
    return !strcmp(name, "mid") ? 0 : !strcmp(name, "output") ? 1 : 2;
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int diff,
    void *userdata)
{
    unsigned h = head_index(name);
    if (h == 2) return;
    CHECK(width == 2 && (diff == 1 || diff == -1));
    int64_t x = row[0] + 1;
    int64_t y = h ? row[1] == 207 ? 8 : row[1] - 100 : row[1];
    CHECK(x >= 0 && x < 9 && y >= 0 && y < (h ? 9 : 8));
    capture_t *events = userdata;
    CHECK(!events->calls[h][x][y]++);
    events->rows[h][x][y] += diff;
}

static void
snapshot(const char *name, const int64_t *row, uint32_t width, void *userdata)
{
    capture(name, row, width, 1, userdata);
    if (snapshot_control && !snapshot_cancelled && head_index(name) < 2) {
        snapshot_cancelled = true;
        wl_evaluation_control_request_cancel(snapshot_control);
    }
}

static bool
member(const state_t *state, unsigned h, unsigned x, unsigned y)
{
    if (!state->a[x] || (y == 8 && (!h || !state->extra))) return false;
    unsigned b = y == 8 ? 7 : y;
    return state->b[b] && (int)x - 1 < (int)b
           && (!h || !state->denied[b]);
}

static void
expect_events(const capture_t *events, const state_t *old, const state_t *now)
{
    for (unsigned h = 0; h < 2; ++h)
        for (unsigned x = 0; x < 9; ++x)
            for (unsigned y = 0; y < 9; ++y)
                CHECK(events->rows[h][x][y]
                    == (int)member(now, h, x, y) - (int)member(old, h, x, y));
}

static void
verify(wl_session_t *session, const state_t *state)
{
    capture_t rows = {0};
    for (unsigned h = 0; h < 2; ++h) {
        const char *name = h ? "output" : "mid";
        const col_rel_t *rel = session_find_rel((wl_col_session_t *)session,
                name);
        CHECK(rel && rel->ncols == 2 && !rel->timestamps);
        for (uint32_t r = 0; r < rel->nrows; ++r) {
            int64_t row[] = {col_rel_get(rel, r, 0), col_rel_get(rel, r, 1)};
            capture(name, row, 2, 1, &rows);
        }
    }
    state_t empty = {0};
    expect_events(&rows, &empty, state);
}

static void
checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t gen, uint32_t extent, uint64_t work)
{
    (void)extent; (void)work;
    uint64_t id = wl_evaluation_control_active_attempt_id(control), used = 0;
    CHECK(id && (!attempt || id == attempt));
    CHECK(!wl_evaluation_control_test_active_consumed(control, id, &used)
        && used >= consumed);
    attempt = id; consumed = used;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED) {
        ++prepared; generation = 0;
        if (inject_funding && !funding_used) funding_pending = true;
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE) ++produced;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED) ++exchanges;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PROBE_KEYS) ++probes;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) {
        CHECK(gen == generation + 1);
        generation = gen; ++acked;
    }
    if (!stopped && stop_event && event == stop_event
        && prepared >= stop_prepared) {
        stopped = true;
        if (work_stop) CHECK(wl_evaluation_control_charge_attempt(control, id,
                UINT64_C(1000000000)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(control);
    }
}

static void
trace(void)
{
    prepared = produced = acked = exchanges = probes = 0;
    attempt = consumed = generation = 0; stopped = false;
    wl_columnar_join_pipeline_test_numeric_checkpoint = checkpoint;
}

static const wl_compute_backend_t *issued_actual_backend;
static bool issued_rewrite_copy, issued_zero_presentation;
static wl_backend_financial_offer_t issued_offer_copy;

/* A live real Root is retained; only its financial transport is copied. */
static const wl_backend_financial_offer_t *
issued_copied_offer(const wl_session_t *session,
    wl_backend_pending_bind_t *pending)
{
    const wl_backend_financial_offer_t *actual =
        issued_actual_backend->session_controlled_financial_offer(session,
            pending);
    CHECK(actual);
    issued_offer_copy = *actual;
    if (issued_rewrite_copy) issued_offer_copy.identity = &issued_offer_copy;
    if (issued_zero_presentation) {
        issued_offer_copy.pending_result = 0;
        issued_offer_copy.foreign_result = 0;
    }
    return &issued_offer_copy;
}

static void *
issued_foreign_peer(void *payload)
{
    wl_session_t *session = payload;
    CHECK(wl_session_step(session) == EBUSY);
    wl_session_t *alias = session;
    CHECK(wl_session_try_destroy(&alias) == EBUSY && alias == session);
    return NULL;
}

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        uint64_t candidate[5], active = 0, used = 0;
        bool cancel = issued_cancel_enabled && !issued_cancel_hits
            && wl_columnar_session_test_metadata_candidate(
            (wl_col_session_t *)session, candidate) ==
            WL_BACKEND_FINANCIAL_SELECTED && candidate[0] == issued_cancel_role;
        wl_evaluation_control_report_t frozen;
        int frozen_status = ENOENT;
        if (cancel) {
            CHECK(!candidate[2] && !candidate[3] && !candidate[4]);
            active = wl_evaluation_control_active_attempt_id(
                session->evaluation_control);
            if (active) CHECK(!wl_evaluation_control_test_active_consumed(
                        session->evaluation_control, active, &used));
            frozen_status = wl_evaluation_control_report(
                session->evaluation_control, &frozen);
            CHECK(!frozen_status || frozen_status == ENOENT);
            issued_actual_backend = session->backend;
            wl_compute_backend_t copied_backend = *session->backend;
            copied_backend.session_controlled_financial_offer =
                issued_copied_offer;
            session->backend = &copied_backend;
            for (unsigned copy = 0; copy < 2; ++copy) {
                issued_rewrite_copy = copy != 0;
                CHECK(wl_session_step(session) == EINVAL);
                uint64_t unchanged[5];
                CHECK(wl_columnar_session_test_metadata_candidate(
                        (wl_col_session_t *)session, unchanged) ==
                    WL_BACKEND_FINANCIAL_SELECTED
                    && unchanged[0] == candidate[0]
                    && unchanged[1] == candidate[1]
                    && !unchanged[2] && !unchanged[3] && !unchanged[4]);
            }
            /* A malformed zero presentation cannot report successful ordinary
             * admission with an uninitialized ownership tag/Scope. */
            issued_zero_presentation = true;
            wl_thread_t rejected_peer;
            CHECK(!wl_thread_create(&rejected_peer, issued_foreign_peer,
                session));
            CHECK(!wl_thread_join(&rejected_peer));
            wl_evaluation_control_test_source_metadata_cancel_at = 1;
            CHECK(wl_session_step(session) == EAGAIN);
            wl_evaluation_control_report_t rejected;
            CHECK(!wl_session_test_source_pending_reset_cancel(session,
                &rejected)
                && rejected.result == WL_EVALUATION_CONTROL_CANCELLED
                && !rejected.consumed);
            issued_zero_presentation = false;
            session->backend = issued_actual_backend;
            issued_actual_backend = NULL;
            wl_evaluation_control_test_source_metadata_cancel_at = 1;
            wl_thread_t peer;
            CHECK(!wl_thread_create(&peer, issued_foreign_peer, session));
            CHECK(!wl_thread_join(&peer)
                && wl_evaluation_control_test_source_metadata_cancel_at == 1);
        }
        int rc = wl_session_step(session);
        if (cancel) {
            uint64_t after[5], consumed_after = 0;
            CHECK(rc == EAGAIN && wl_columnar_session_test_metadata_candidate(
                    (wl_col_session_t *)session, after) ==
                WL_BACKEND_FINANCIAL_SELECTED && after[0] == candidate[0]
                && after[1] == candidate[1] && !after[2] && !after[3]
                && !after[4]);
            wl_evaluation_control_report_t bank, original;
            CHECK(!wl_session_test_source_pending_reset_cancel(session, &bank)
                && bank.result == WL_EVALUATION_CONTROL_CANCELLED
                && bank.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
                && !bank.consumed && !bank.execution_status
                && !bank.cleanup_status);
            CHECK(wl_evaluation_control_active_attempt_id(
                    session->evaluation_control) == active);
            if (active) CHECK(!wl_evaluation_control_test_active_consumed(
                        session->evaluation_control, active, &consumed_after)
                    && consumed_after == used);
            CHECK(wl_evaluation_control_report(session->evaluation_control,
                &original) == frozen_status);
            if (!frozen_status) native_report_same(&frozen, &original);
            ++issued_cancel_hits;
        }
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

static void
init(fixture_t *f, bool observed)
{
    memset(f, 0, sizeof(*f));
    static const char source_format[] =
        ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl c(y:int64,z:int64)\n.decl mid(x:int64,y:int64)\n"
        ".decl deny(y:int64)\n.decl blocked(y:int64)\n"
        ".decl output(x:int64,z:int64)\n"
        "blocked(y):-deny(y).\n"
        "mid(x,y):-a(k,x),b(k,y),x<y.\n"
        "output(x,z):-%s,!blocked(y).\n";
    char source[512];
    snprintf(source, sizeof(source), source_format,
        strict_mode ? "c(y,z),mid(x,y)" : "mid(x,y),c(y,z)");
    wirelog_error_t error;
    f->program = wirelog_parse_string(source, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    unsigned joins = 0;
    for (uint32_t s = 0; s < f->plan->stratum_count; ++s)
        for (uint32_t r = 0; r < f->plan->strata[s].relation_count; ++r) {
            const wl_plan_relation_t *head = &f->plan->strata[s].relations[r];
            for (uint32_t i = 0; i < head->op_count; ++i) {
                CHECK(head->ops[i].op != WL_PLAN_OP_LFTJ);
                if (head->ops[i].op == WL_PLAN_OP_JOIN) ++joins;
            }
        }
    CHECK(joins == 2);
    if (strict_mode) {
        bool private_right = false;
        for (uint32_t s = 0; s < f->plan->stratum_count; ++s)
            for (uint32_t r = 0; r < f->plan->strata[s].relation_count; ++r) {
                const wl_plan_relation_t *head =
                    &f->plan->strata[s].relations[r];
                for (uint32_t i = 0; i < head->op_count; ++i)
                    if (head->ops[i].op == WL_PLAN_OP_JOIN
                        && !strcmp(head->ops[i].right_relation,
                        "mid")) private_right = true;
            }
        CHECK(private_right);
    }
    configure("WIRELOG_JOIN_BATCH_BYTES", "64");
    configure("WIRELOG_JOIN_BATCH_STRICT", strict_mode ? "1" : NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", "100");
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = f->control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->sessions[0]));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1,
        &f->sessions[1]));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    int64_t ab[16], c[16];
    for (unsigned i = 0; i < 8; ++i) {
        ab[2 * i] = 1; ab[2 * i + 1] = i;
        c[2 * i] = i; c[2 * i + 1] = 100 + i;
        f->state.a[i + 1] = true; f->state.b[i] = true;
    }
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(f->sessions[i], "a", ab, 8, 2));
        CHECK(!wl_session_insert(f->sessions[i], "b", ab, 8, 2));
        CHECK(!wl_session_insert(f->sessions[i], "c", c, 8, 2));
        CHECK(!wl_session_insert(f->sessions[i], "deny", (int64_t[]){42}, 1,
            1));
        CHECK(!wl_session_remove(f->sessions[i], "deny", (int64_t[]){42}, 1,
            1));
        if (observed) wl_session_set_delta_cb(f->sessions[i], capture,
                &f->events[i]);
    }
}

static void
destroy(fixture_t *f)
{
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    for (unsigned i = 0; i < 2;
        ++i) CHECK(!wl_session_try_destroy(&f->sessions[i]));
    wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
}

static void
step_pair(fixture_t *f, const state_t *old, bool observed,
    unsigned expected_prepared)
{
    trace();
    CHECK(!finish(f->sessions[0]));
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    verify(f->sessions[0], &f->state);
    if (prepared != expected_prepared || produced <= 2 || acked <= 2 ||
        exchanges <= 1 || !probes)
        fprintf(stderr,
            "route prepared=%u produced=%u acked=%u exchanges=%u probes=%u\n",
            prepared, produced, acked, exchanges, probes);
    CHECK(prepared == expected_prepared && produced > 2 && acked > 2 &&
        exchanges > 1
        && probes > 0);
    CHECK(!finish(f->sessions[1]));
    for (unsigned i = 0; i < 2; ++i) {
        verify(f->sessions[i], &f->state);
        if (observed) expect_events(&f->events[i], old, &f->state);
        memset(&f->events[i], 0, sizeof(f->events[i]));
    }
}

static void
successors(bool observed)
{
    wl_columnar_session_test_step_reset_chunks = 0;
    fixture_t f; init(&f, observed);
    state_t old = {0};
    step_pair(&f, &old, observed, 2);
    for (unsigned round = 0; round < 6; ++round) {
        old = f.state;
        for (unsigned i = 0; i < 2; ++i) {
            int64_t row[] = {round == 2 ? 7 : 1,
                             round == 2 ? 207 : round ? 0 : -1};
            int rc = round == 0 ? wl_session_insert(f.sessions[i], "a", row, 1,
                    2)
                : round == 1 ? wl_session_remove(f.sessions[i], "a", row, 1, 2)
                : round == 2 ? wl_session_insert(f.sessions[i], "c", row, 1, 2)
                : round == 3 ? wl_session_remove(f.sessions[i], "b",
                    (int64_t[]){1, 7}, 1, 2)
                : round == 4 ? wl_session_insert(f.sessions[i], "deny",
                    (int64_t[]){3}, 1, 1)
                : wl_session_remove(f.sessions[i], "deny", (int64_t[]){3}, 1,
                    1);
            CHECK(!rc);
        }
        if (round == 0) f.state.a[0] = true;
        if (round == 1) f.state.a[1] = false;
        if (round == 2) f.state.extra = true;
        if (round == 3) f.state.b[7] = false;
        if (round >= 4) f.state.denied[3] = round == 4;
        step_pair(&f, &old, observed, 2);
    }
    for (unsigned i = 0; i < 2; ++i) {
        capture_t rows = {0}; state_t empty = {0};
        CHECK(!wl_session_snapshot(f.sessions[i], snapshot, &rows));
        expect_events(&rows, &empty, &f.state);
    }
    CHECK(wl_columnar_session_test_step_reset_chunks >= 2);
    wl_columnar_session_test_step_reset_chunks = 0;
    destroy(&f);
}

static void
snapshots(void)
{
    fixture_t f; init(&f, false);
    for (unsigned round = 0; round < 2; ++round) {
        if (round) {
            for (unsigned i = 0; i < 2; ++i) CHECK(!wl_session_insert(
                        f.sessions[i], "a", (int64_t[]){1, -1}, 1, 2));
            f.state.a[0] = true;
        }
        trace();
        for (unsigned i = 0; i < 2; ++i) {
            capture_t rows = {0}; state_t empty = {0};
            CHECK(!wl_session_snapshot(f.sessions[i], snapshot, &rows));
            expect_events(&rows, &empty, &f.state);
            verify(f.sessions[i], &f.state);
        }
        wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
        CHECK(prepared == 2 && acked > 2 && exchanges > 1 && probes > 0);
    }
    destroy(&f);
}

static void
recovery(void)
{
    static const unsigned phases[] = {
        WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_ACK,
        WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK
    };
    for (unsigned p = 0; p < sizeof(phases) / sizeof(*phases); ++p)
        for (unsigned w = 0; w < 4; ++w) {
            fixture_t f; init(&f, true);
            work_stop = w % 2; stop_event = phases[p];
            stop_prepared = 1 + w / 2;
            if (work_stop) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_C(1000000000)));
            trace();
            int expected = work_stop ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(finish(f.sessions[0]) == expected && stopped);
            wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
            state_t empty = {0};
            expect_events(&f.events[0], &empty, &empty);
            for (unsigned h = 0; h < 2; ++h) {
                const col_rel_t *rel = session_find_rel(
                    (wl_col_session_t *)f.sessions[0], h ? "output" : "mid");
                CHECK(!rel || !rel->nrows);
            }
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report.attempt_id == attempt && report.result == expected
                && report.consumed && !report.cleanup_status);
            /* A work-limit retry can retain a completed private first head.
             * Its input lease still protects the old computation from mutation. */
            unsigned retry_prepared = work_stop && stop_prepared == 2 ? 1 : 2;
            if (work_stop) {
                CHECK(wl_session_insert(f.sessions[0], "a", (int64_t[]){1, -1},
                    1, 2)
                    == EBUSY);
                const col_rel_t *input =
                    session_find_rel((wl_col_session_t *)f.sessions[0], "a");
                CHECK(input && input->nrows == 8);
            }
            stop_event = stop_prepared = 0; work_stop = false;
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_evaluation_control_configure(f.control, 0));
            step_pair(&f, &empty, true, retry_prepared);
            destroy(&f);
        }
}

static uint64_t
funding_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t proposed)
{
    (void)session;
    if (funding_pending && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE) {
        funding_pending = false; funding_used = true;
        return 383; /* Exact existing framed384 admission refuses before BODY. */
    }
    return proposed;
}

static void
funding_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol)
{
    (void)session;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report));
    if (report.limit != 383) return;
    CHECK(action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE
        && protocol == WL_EVALUATION_CONTROL_WORK_LIMIT
        && report.result == protocol && !report.consumed);
    ++funding_refusals;
}

static void
funding(void)
{
    fixture_t f; init(&f, true); state_t empty = {0};
    inject_funding = true; funding_pending = funding_used = false;
    funding_refusals = 0;
    wl_columnar_session_test_framed_grant_limit = funding_limit;
    wl_columnar_session_test_framed_grant_finished = funding_finished;
    step_pair(&f, &empty, true, 2);
    CHECK(funding_used && funding_refusals == 1);
    wl_columnar_session_test_framed_grant_limit = NULL;
    wl_columnar_session_test_framed_grant_finished = NULL;
    inject_funding = false;
    destroy(&f);
}

static void
snapshot_recovery(void)
{
    fixture_t f; init(&f, false); state_t empty = {0};
    capture_t rows = {0};
    stop_event = WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE;
    trace();
    CHECK(wl_session_snapshot(f.sessions[0], snapshot, &rows)
        == WL_EVALUATION_CONTROL_CANCELLED && stopped);
    expect_events(&rows, &empty, &empty);
    stop_event = 0;
    CHECK(!wl_evaluation_control_reset(f.control));
    trace();
    snapshot_control = f.control; snapshot_cancelled = false;
    CHECK(!wl_session_snapshot(f.sessions[0], snapshot, &rows));
    snapshot_control = NULL;
    CHECK(snapshot_cancelled && prepared == 2 && probes > 0);
    expect_events(&rows, &empty, &f.state);
    memset(&rows, 0, sizeof(rows));
    CHECK(wl_session_snapshot(f.sessions[0], snapshot, &rows)
        == WL_EVALUATION_CONTROL_CANCELLED);
    expect_events(&rows, &empty, &empty);
    CHECK(!wl_evaluation_control_reset(f.control));
    trace();
    CHECK(!wl_session_snapshot(f.sessions[0], snapshot, &rows) && !prepared);
    expect_events(&rows, &empty, &f.state);
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    memset(&rows, 0, sizeof(rows));
    CHECK(!wl_session_snapshot(f.sessions[1], snapshot, &rows));
    expect_events(&rows, &empty, &f.state);
    destroy(&f);
}

static void
append_capture(const char *name, const int64_t *row, uint32_t width, int diff,
    void *userdata)
{
    CHECK(!strcmp(name, "mid") && width == 2 && (diff == 1 || diff == -1));
    int64_t x = row[0] + 1, y = row[1];
    CHECK(x >= 0 && x < 9 && y >= 0 && y < 9);
    capture_t *events = userdata;
    CHECK(!events->calls[0][x][y]++);
    events->rows[0][x][y] += diff;
}

static void
append_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)control;
    if (phase != 44) return;
    CHECK(bindings && bindings->dynamic_entries
        && progress < bindings->plan->op_count);
    wl_plan_op_type_t kind = bindings->plan->ops[progress].op;
    CHECK(kind == WL_PLAN_OP_JOIN || kind == WL_PLAN_OP_VARIABLE);
    ++append_reads;
    if (kind == WL_PLAN_OP_JOIN) ++append_joins;
    else ++append_variables;
}

static bool
append_member(unsigned round, unsigned x, unsigned y)
{
    return round && (x || round == 3) && (y < 8 || round >= 2)
           && (int)x - 1 < (int)y;
}

static void
appends(void)
{
    fixture_t f = {0};
    wirelog_error_t error;
    f.program = wirelog_parse_string(
        ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
        ".decl mid(x:int64,y:int64)\nmid(x,y):-a(k,x),b(k,y),x<y.\n", &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    CHECK(f.plan->stratum_count == 1 && f.plan->strata[0].relation_count == 1
        && f.plan->strata[0].relations[0].ops[1].op == WL_PLAN_OP_JOIN);
    configure("WIRELOG_JOIN_BATCH_BYTES", "64");
    configure("WIRELOG_JOIN_BATCH_STRICT", strict_mode ? "1" : NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", "100");
    CHECK(!wl_evaluation_control_create(0, &f.control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.sessions[0]));
    CHECK(!wl_session_create(wl_backend_columnar(), f.plan, 1, &f.sessions[1]));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    int64_t data[16];
    for (unsigned i = 0; i < 8; ++i) {
        data[2 * i] = 1; data[2 * i + 1] = i;
    }
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(f.sessions[i], "a", data, 8, 2));
        CHECK(!wl_session_insert(f.sessions[i], "b", data, 8, 2));
        wl_session_set_delta_cb(f.sessions[i], append_capture, &f.events[i]);
    }
    wl_columnar_session_test_observed_transaction_checkpoint =
        append_checkpoint;
    for (unsigned round = 1; round <= 3; ++round) {
        if (round > 1)
            for (unsigned i = 0; i < 2; ++i)
                CHECK(!wl_session_insert(f.sessions[i], round == 2 ? "b" : "a",
                    (int64_t[]){1, round == 2 ? 8 : -1}, 1, 2));
        append_reads = append_joins = append_variables = 0;
        trace();
        int rc = finish(f.sessions[0]);
        if (rc) fprintf(stderr,
                "append round=%u rc=%d reads=%u joins=%u variables=%u prepared=%u\n",
                round, rc, append_reads, append_joins, append_variables,
                prepared);
        CHECK(!rc);
        wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
        if (round == 2) CHECK(append_reads && append_joins && !prepared);
        else CHECK(prepared == 1 && produced > 1 && acked > 1);
        if (round == 3) CHECK(append_reads && append_variables);
        CHECK(!finish(f.sessions[1]));
        for (unsigned i = 0; i < 2; ++i) {
            capture_t rows = {0};
            const col_rel_t *head =
                session_find_rel((wl_col_session_t *)f.sessions[i], "mid");
            CHECK(head && head->ncols == 2);
            for (uint32_t r = 0; r < head->nrows; ++r) {
                int64_t row[] = {col_rel_get(head, r, 0),
                                 col_rel_get(head, r, 1)};
                append_capture("mid", row, 2, 1, &rows);
            }
            for (unsigned x = 0; x < 9; ++x)
                for (unsigned y = 0; y < 9; ++y) {
                    CHECK(rows.rows[0][x][y] == (int)append_member(round, x,
                        y));
                    CHECK(f.events[i].rows[0][x][y]
                        == (int)append_member(round, x,
                        y) - (int)append_member(round - 1, x, y));
                }
            memset(&f.events[i], 0, sizeof(f.events[i]));
        }
    }
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    destroy(&f);
}

static fixture_t *native_fixture;
static unsigned native_event, native_forward_after, native_produced,
    native_acked;
static bool native_pending, native_ended, native_successor, native_at_limit;
static bool native_late, native_refuse, native_refused;
static bool native_metadata_denial, native_metadata_refused,
    native_foreign_call;
static unsigned native_metadata_reports;
static uint64_t native_successor_id;
static wl_evaluation_control_report_t native_report;
static unsigned operator_finish_native_domains;
extern int wl_columnar_eval_stack_test_fail_allocation;

typedef struct {
    wl_session_t *session;
    int result;
} native_foreign_call_t;

static void *
native_foreign_step(void *data)
{
    native_foreign_call_t *call = data;
    call->result = wl_session_step(call->session);
    return NULL;
}

static void
native_report_same(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b)
{
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit
        && a->consumed == b->consumed && a->stop_reason == b->stop_reason
        && a->execution_status == b->execution_status
        && a->cleanup_status == b->cleanup_status && a->result == b->result);
}

static void
operator_finish_excludes_registered_native(const wl_col_session_t *session)
{
    unsigned bit = native_successor ? 2u : 1u;
    if ((operator_finish_native_domains & bit) || !native_ended
        || !session->cleanup_active ||
        wl_columnar_session_test_native_error_phase(
            session) != WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD) return;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
        (wl_col_session_t *)session, &ctx));
    if (!ctx->child_bound || wl_columnar_eval_stack_numeric_owner(ctx->frame,
        &ctx->child)) return;
    int proof_owner = 0, component_owner = 0, cause = 0;
    wl_evaluation_control_t *proof = NULL, *funding = NULL;
    CHECK(!wl_evaluation_control_create(0, &proof)
        && !wl_evaluation_control_attach(proof, &proof_owner)
        && !wl_evaluation_control_begin(proof, &proof_owner));
    uint64_t proof_id = wl_evaluation_control_active_attempt_id(proof);
    /* Actual registration/ended-generation proof, never seeded native bits. */
    CHECK(!wl_columnar_session_join_cleanup_prove(ctx, proof, proof_id, NULL,
        &cause) && cause);
    CHECK(!wl_evaluation_control_finish(proof, &proof_owner, 0, 0)
        && !wl_evaluation_control_detach(proof, &proof_owner));
    wl_evaluation_control_release(proof);
    wl_columnar_eval_serial_framed_attempt_t *saved = malloc(sizeof(*saved));
    CHECK(saved); memcpy(saved, ctx, sizeof(*saved));
    wl_columnar_eval_stack_unary_prepared_t p = {0}, inert = p;
    CHECK(!wl_evaluation_control_create(0, &funding)
        && !wl_evaluation_control_attach(funding, &component_owner)
        && !wl_evaluation_control_begin(funding, &component_owner));
    uint64_t id = wl_evaluation_control_active_attempt_id(funding), spent;
    CHECK(wl_columnar_eval_stack_unary_prepared_operator_finish_begin(&p,
        ctx->frame, &ctx->child, funding, id) == EBUSY
        && !memcmp(&p, &inert, sizeof(p))
        && !memcmp(ctx, saved, sizeof(*saved))
        && !wl_evaluation_control_test_active_consumed(funding, id, &spent)
        && !spent);
    CHECK(!wl_evaluation_control_finish(funding, &component_owner, 0, 0)
        && !wl_evaluation_control_detach(funding, &component_owner));
    wl_evaluation_control_release(funding); free(saved);
    wl_evaluation_control_report_t original;
    CHECK(!wl_evaluation_control_report(native_fixture->control, &original));
    native_report_same(&native_report, &original);
    operator_finish_native_domains |= bit;
}

static void
native_end_original(void)
{
    if (!native_pending || native_ended) return;
    wl_session_t *session = native_fixture->sessions[0];
    wl_evaluation_control_t *control = native_fixture->control;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id && id == attempt);
    wl_evaluation_control_attempt_finish_result_t ended =
        wl_evaluation_control_finish_attempt(control,
            session->operation_admission, id, 0, 0);
    CHECK(!ended.protocol && ended.report_valid
        && ended.kind == WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED
        && !ended.report.result && ended.report.consumed);
    native_report = ended.report;
    native_ended = true; native_produced = produced; native_acked = acked;
    if (native_successor) {
        CHECK(!wl_evaluation_control_reset(control));
        wl_evaluation_control_attempt_begin_result_t next =
            wl_evaluation_control_begin_attempt(control,
                session->operation_admission);
        CHECK(!next.protocol && next.attempt_id != id);
        native_successor_id = next.attempt_id;
    }
}

static void
native_end_checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t gen, uint32_t extent, uint64_t work)
{
    if (native_ended) {
        if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE
            || event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK
            || event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED)
            ++native_forward_after;
        return;
    }
    checkpoint(control, event, gen, extent, work);
    if (event == native_event) native_pending = true;
}

static void
native_grant_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *grant, int protocol)
{
    (void)session; (void)action; (void)grant; (void)protocol;
    if (!native_at_limit && (!native_late || !session->cleanup_active))
        native_end_original();
    operator_finish_excludes_registered_native(session);
}

static uint64_t
native_grant_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action, uint64_t proposed)
{
    (void)session; (void)action;
    if (native_at_limit) native_end_original();
    if (native_refuse && native_ended && !native_refused
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_NATIVE_CLEANUP) {
        native_refused = true;
        return WL_COLUMNAR_EVAL_SERIAL_FRAMED_HOST_WORK - 1;
    }
    return proposed;
}

static uint64_t
native_metadata_limit(const wl_col_session_t *session,
    wl_columnar_session_metadata_purpose_t purpose, uint64_t proposed)
{
    (void)session;
    if (purpose == WL_COLUMNAR_SESSION_METADATA_JOIN_CLEANUP
        && native_ended && !native_foreign_call && !native_metadata_refused) {
        native_metadata_refused = true;
        return 315; /* ENTRY140+GUARD80 succeeds; ROOT96 is one short. */
    }
    return proposed;
}

static void
native_metadata_report(const wl_col_session_t *session,
    wl_columnar_session_metadata_purpose_t purpose,
    wl_columnar_session_metadata_event_t event, int protocol,
    wl_evaluation_control_report_t report)
{
    (void)session;
    if (purpose != WL_COLUMNAR_SESSION_METADATA_JOIN_CLEANUP) return;
    CHECK(report.attempt_id && !report.execution_status &&
        !report.cleanup_status);
    if (report.limit == 315) {
        if (event != WL_COLUMNAR_SESSION_METADATA_JOIN_UNUSED_CLOSED
            || protocol != WL_EVALUATION_CONTROL_WORK_LIMIT
            || report.result != WL_EVALUATION_CONTROL_WORK_LIMIT
            || report.consumed != 220)
            fprintf(stderr,
                "metadata refusal event=%u protocol=%d result=%d stop=%d consumed=%llu id=%llu\n",
                event, protocol, report.result, report.stop_reason,
                (unsigned long long)report.consumed,
                (unsigned long long)report.attempt_id);
        CHECK(event == WL_COLUMNAR_SESSION_METADATA_JOIN_UNUSED_CLOSED
            && protocol == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.result == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.consumed == 220);
        ++native_metadata_reports;
        wl_evaluation_control_report_t frozen;
        CHECK(!wl_evaluation_control_report(native_fixture->control, &frozen));
        native_report_same(&native_report, &frozen);
    } else CHECK(!report.result && !report.stop_reason && report.consumed);
}

static void
native_end_history(bool observed, bool evaluating_snapshot, unsigned event,
    bool successor, bool at_limit, bool late, bool refuse)
{
    fixture_t f; init(&f, observed);
    native_fixture = &f; native_event = event;
    native_pending = native_ended = false; native_forward_after = 0;
    native_successor = successor; native_successor_id = 0;
    native_at_limit = at_limit;
    native_late = late; native_refuse = refuse; native_refused = false;
    native_metadata_refused = false; native_metadata_reports = 0;
    stop_event = 0;
    trace();
    wl_columnar_join_pipeline_test_numeric_checkpoint = native_end_checkpoint;
    wl_columnar_session_test_framed_grant_finished = native_grant_finished;
    wl_columnar_session_test_framed_grant_limit = native_grant_limit;
    if (native_metadata_denial) {
        wl_columnar_session_test_metadata_limit = native_metadata_limit;
        wl_columnar_session_test_metadata_finished = native_metadata_report;
    }
    capture_t rows = {0};
    int rc;
    if (evaluating_snapshot) rc = wl_session_snapshot(f.sessions[0], snapshot,
                &rows);
    else {
        for (unsigned i = 0;; ++i) {
            CHECK(i < 1000000);
            rc = wl_session_step(f.sessions[0]);
            if (native_ended || rc != EAGAIN) break;
        }
        CHECK(native_ended && rc == EAGAIN);
        wl_col_session_t *col = (wl_col_session_t *)f.sessions[0];
        wl_columnar_eval_stack_cleanup_frame_t *frame = col->cleanup_active;
        unsigned phase = wl_columnar_session_test_native_error_phase(col);
        unsigned produced_before = produced, acked_before = acked;
        native_foreign_call_t foreign = {f.sessions[0], 0};
        wl_thread_t thread;
        native_foreign_call = true;
        CHECK(!wl_thread_create(&thread, native_foreign_step, &foreign));
        CHECK(!wl_thread_join(&thread));
        native_foreign_call = false;
        CHECK(foreign.result == EBUSY && col->cleanup_active == frame
            && wl_columnar_session_test_native_error_phase(col) == phase
            && produced == produced_before && acked == acked_before);
        wl_evaluation_control_report_t frozen;
        CHECK(!wl_evaluation_control_report(f.control, &frozen));
        native_report_same(&native_report, &frozen);
        rc = finish(f.sessions[0]);
    }
    if (rc != EINVAL) fprintf(stderr,
            "native-end observed=%u snapshot=%u event=%u successor=%u limit=%u rc=%d phase=%u\n",
            observed, evaluating_snapshot, event, successor, at_limit, rc,
            wl_columnar_session_test_native_error_phase(
                (wl_col_session_t *)f.sessions[0]));
    CHECK(native_ended && rc == EINVAL);
    CHECK(!refuse || native_refused);
    CHECK(!native_metadata_denial || (native_metadata_refused
        && native_metadata_reports == 1));
    if (wl_columnar_session_test_native_error_phase(
            (wl_col_session_t *)f.sessions[0])
        != WL_COLUMNAR_SESSION_NATIVE_ERROR_DONE)
        fprintf(stderr,
            "native cleanup returned before DONE observed=%u snapshot=%u event=%u phase=%u\n",
            observed, evaluating_snapshot, event,
            wl_columnar_session_test_native_error_phase(
                (wl_col_session_t *)f.sessions[0]));
    CHECK(wl_columnar_session_test_native_error_phase(
            (wl_col_session_t *)f.sessions[0])
        == WL_COLUMNAR_SESSION_NATIVE_ERROR_DONE);
    CHECK(!native_forward_after && produced == native_produced &&
        acked == native_acked);
    wl_columnar_session_test_framed_grant_finished = NULL;
    wl_columnar_session_test_framed_grant_limit = NULL;
    wl_columnar_session_test_metadata_limit = NULL;
    wl_columnar_session_test_metadata_finished = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report));
    native_report_same(&native_report, &report);
    if (successor) {
        uint64_t used = UINT64_MAX;
        wl_session_t *saved = f.sessions[0];
        CHECK(wl_session_try_destroy(&f.sessions[0]) == EBUSY
            && f.sessions[0] == saved);
        CHECK(wl_evaluation_control_active_attempt_id(f.control) ==
            native_successor_id);
        CHECK(!wl_evaluation_control_test_active_consumed(f.control,
            native_successor_id, &used)
            && !used);
        CHECK(!wl_evaluation_control_report(f.control, &report));
        native_report_same(&native_report, &report);
        wl_evaluation_control_attempt_finish_result_t ended =
            wl_evaluation_control_finish_attempt(f.control,
                f.sessions[0]->operation_admission,
                native_successor_id, 0, 0);
        CHECK(!ended.protocol && ended.report_valid && !ended.report.result
            && !ended.report.consumed && !ended.report.stop_reason);
    }
    wl_col_session_t *col = (wl_col_session_t *)f.sessions[0];
    CHECK(!col->cleanup_active && !col->cleanup_pending &&
        !col->cleanup_reserved_bytes);
    const col_rel_t *mid = session_find_rel(col, "mid");
    const col_rel_t *output = session_find_rel(col, "output");
    if (late && !observed && !evaluating_snapshot) {
        CHECK(mid && mid->ncols == 2 && mid->nrows == 28);
        capture_t prefix = {0};
        for (uint32_t r = 0; r < mid->nrows; ++r) {
            int64_t row[] = {col_rel_get(mid, r, 0), col_rel_get(mid, r, 1)};
            capture("mid", row, 2, 1, &prefix);
        }
        for (unsigned x = 0; x < 9; ++x)
            for (unsigned y = 0; y < 9; ++y)
                CHECK(prefix.rows[0][x][y] == (int)member(&f.state, 0, x, y));
        CHECK(!output || !output->nrows);
    } else CHECK((!mid || !mid->nrows) && (!output || !output->nrows));
    state_t empty = {0};
    expect_events(&f.events[0], &empty, &empty);
    expect_events(&rows, &empty, &empty);
    CHECK(!wl_evaluation_control_reset(f.control));
    trace();
    capture_t retry_rows = {0};
    int retry_rc = evaluating_snapshot
        ? wl_session_snapshot(f.sessions[0], snapshot, &retry_rows)
        : finish(f.sessions[0]);
    if (retry_rc) fprintf(stderr,
            "native retry observed=%u snapshot=%u event=%u rc=%d phase=%u prepared=%u\n",
            observed, evaluating_snapshot, event, retry_rc,
            wl_columnar_session_test_native_error_phase(col), prepared);
    CHECK(!retry_rc);
    if (evaluating_snapshot) expect_events(&retry_rows, &empty, &f.state);
    unsigned expected_prepared = late && !observed &&
        !evaluating_snapshot ? 1 : 2;
    CHECK(prepared == expected_prepared && produced > 2 && acked > 2 &&
        exchanges > 1 && probes);
    verify(f.sessions[0], &f.state);
    CHECK(!finish(f.sessions[1])); verify(f.sessions[1], &f.state);
    if (observed) expect_events(&f.events[0], &empty, &f.state);
    destroy(&f); native_fixture = NULL;
}

static void
native_end(void)
{
    operator_finish_native_domains = 0;
    const unsigned events[] = {WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED,
                               WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE,
                               WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED,
                               WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
                               WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK};
    for (unsigned observed = 0; observed < 2; ++observed)
        for (size_t e = 0; e < sizeof(events)/sizeof(events[0]); ++e)
            native_end_history(observed, false, events[e], e % 2, e % 2, false,
                false);
    native_end_history(false, true, WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
        false, false, false, false);
    native_end_history(false, true, WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK,
        true, true, false, false);
    for (unsigned successor = 0; successor < 2; ++successor) {
        native_end_history(false, false,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED,
            successor, false, true, false);
        native_end_history(true, false, WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED,
            successor, false, true, false);
        native_end_history(false, true, WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED,
            successor, false, true, false);
    }
    native_end_history(true, false, WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
        true, false, false, true);
    strict_mode = true;
    native_end_history(false, false, WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
        true, false, false, false);
    native_end_history(true, false, WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK,
        false, false, false, true);
    native_end_history(false, true, WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED,
        true, false, true, false);
    strict_mode = false;
    native_metadata_denial = true;
    native_end_history(false, false, WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
        false, false, false, false);
    native_end_history(true, false, WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER,
        true, false, false, false);
    native_metadata_denial = false;
    CHECK(operator_finish_native_domains == 3);
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
}

static void
native_bootstrap_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *grant, int protocol)
{
    if (native_ended || action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE
        || protocol != ENOMEM) return;
    CHECK(!session->cleanup_active && !session->cleanup_pending);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report)
        && report.consumed && !report.result);
    attempt = wl_evaluation_control_active_attempt_id(native_fixture->control);
    native_pending = true;
    native_end_original();
}

static unsigned bootstrap_close_target;
static bool bootstrap_close_refused;
static unsigned bootstrap_permanent;
static unsigned bootstrap_outer_calls;
static wl_evaluation_control_report_t bootstrap_bank_stop;
static unsigned bootstrap_bank_role;
static bool bootstrap_outer_pair_active;
static unsigned bootstrap_outer_pair_role, bootstrap_outer_pair_phase;
static uint64_t bootstrap_outer_pair_source_id;
static wl_evaluation_control_report_t bootstrap_outer_pair_report;
static unsigned bootstrap_outer_main_calls, bootstrap_outer_maintenance_calls;

static void
bootstrap_outer_phase(wl_col_session_t *session, unsigned role,
    unsigned before, unsigned after_phase, bool after, uint64_t source_id,
    int result, bool called)
{
    bool close_phase = before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH
        || before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT
        || before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_DETACH
        || before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_RELEASE
        || before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_CONSUME
        || before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED;
    CHECK(native_fixture && &session->base == native_fixture->sessions[0]
        && bootstrap_permanent && bootstrap_close_refused && source_id
        && !result && close_phase
        && (role == WL_SESSION_SOURCE_METADATA_MAIN
        || role == WL_SESSION_SOURCE_METADATA_MAINTENANCE));
    wl_evaluation_control_report_t bank;
    CHECK(!wl_session_test_source_early_report(&session->base, &bank)
        && bank.result == WL_EVALUATION_CONTROL_CANCELLED
        && bank.stop_reason == WL_EVALUATION_CONTROL_CANCELLED
        && !bank.execution_status && !bank.cleanup_status);
    if (after) {
        CHECK(called && bootstrap_outer_pair_active
            && role == bootstrap_outer_pair_role
            && before == bootstrap_outer_pair_phase
            && source_id == bootstrap_outer_pair_source_id);
        native_report_same(&bootstrap_outer_pair_report, &bank);
        bootstrap_outer_pair_active = false;
        ++bootstrap_outer_calls;
        if (role == WL_SESSION_SOURCE_METADATA_MAIN)
            ++bootstrap_outer_main_calls;
        else ++bootstrap_outer_maintenance_calls;
    }else {
        CHECK(!called && before == after_phase && !bootstrap_outer_pair_active);
        bootstrap_outer_pair_role = role;
        bootstrap_outer_pair_phase = before;
        bootstrap_outer_pair_source_id = source_id;
        bootstrap_outer_pair_report = bank;
        bootstrap_outer_pair_active = true;
    }
    bootstrap_bank_role = role;
    bootstrap_bank_stop = bank;
    wl_evaluation_control_report_t original;
    CHECK(!wl_evaluation_control_report(native_fixture->control, &original));
    native_report_same(&native_report, &original);
}
static void
bootstrap_close_phase(wl_col_session_t *session, bool maintenance,
    wl_evaluation_control_t *funding, uint64_t id, unsigned phase,
    bool after, int financial)
{
    if (bootstrap_permanent && bootstrap_close_refused && after && financial) {
        bootstrap_bank_role =
            maintenance ? WL_SESSION_SOURCE_METADATA_MAINTENANCE
            : WL_SESSION_SOURCE_METADATA_MAIN;
        CHECK(!wl_session_test_source_early_report(&session->base,
            &bootstrap_bank_stop));
    }
    if (!maintenance ||
        !wl_columnar_session_test_bootstrap_finance_pending(session)) return;
    if (!after && phase == bootstrap_close_target && !bootstrap_close_refused) {
        bootstrap_close_refused = true;
        if (bootstrap_permanent == 2)
            wl_evaluation_control_test_source_lexical_cancel_before_action =
                true;
        else wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
    }
    if (after && financial) {
        CHECK(bootstrap_close_refused
            && financial == WL_EVALUATION_CONTROL_CANCELLED);
        if (bootstrap_permanent) {
            bootstrap_bank_role = WL_SESSION_SOURCE_METADATA_MAINTENANCE;
            CHECK(!wl_session_test_source_early_report(&session->base,
                &bootstrap_bank_stop));
        }else CHECK(!wl_session_test_source_final_reset_cancel(&session->base));
        CHECK(wl_columnar_session_test_bootstrap_finance_pending(session));
        CHECK(session->base.backend->session_controlled_step_state(
                &session->base)
            == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
        const int64_t row[2] = {1, 0};
        CHECK(wl_session_insert(&session->base, "a", row, 0, 2) == EBUSY);
        if (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_CONSUME
            || phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_OBSERVED)
            CHECK(!funding && !id); /* Actual physical release already ran. */
    }
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(native_fixture->control, &report));
    native_report_same(&native_report, &report);
}

static void
native_bootstrap_end(void)
{
    for (unsigned mode = 0; mode < 3; ++mode)
        for (unsigned successor = 0; successor < 2; ++successor)
            for (unsigned close = 0; close < (mode == 2 ? 6u : 1u); ++close)
                for (unsigned permanent = 0;
                    permanent < (mode == 2 && close ? 3u : 1u); ++permanent) {
                    fixture_t f; init(&f, mode == 1); trace();
                    bootstrap_permanent = permanent;
                    bootstrap_outer_calls = 0;
                    bootstrap_outer_main_calls =
                        bootstrap_outer_maintenance_calls = 0;
                    bootstrap_outer_pair_active = false;
                    bootstrap_outer_pair_role = bootstrap_outer_pair_phase = 0;
                    bootstrap_outer_pair_source_id = 0;
                    bootstrap_outer_pair_report =
                        (wl_evaluation_control_report_t){0};
                    wl_columnar_session_test_financier_outer_source_phase =
                        permanent ? bootstrap_outer_phase : NULL;
                    bootstrap_close_target = close == 5
                    ? WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT : close;
                    bootstrap_close_refused = false;
                    wl_columnar_session_test_terminal_finance_phase =
                        bootstrap_close_phase;
                    native_fixture = &f; native_pending = native_ended = false;
                    native_successor = successor; native_successor_id = 0;
                    wl_columnar_eval_stack_test_fail_allocation = 0;
                    wl_columnar_session_test_framed_grant_finished =
                        native_bootstrap_finished;
                    capture_t rows = {0};
                    issued_cancel_enabled = mode == 1;
                    issued_cancel_role = WL_SESSION_SOURCE_METADATA_MAINTENANCE;
                    issued_cancel_hits = 0;
                    int rc = mode == 2 ? wl_session_snapshot(f.sessions[0],
                            snapshot,
                            &rows)
            : finish(f.sessions[0]);
                    CHECK(!issued_cancel_enabled || issued_cancel_hits == 1);
                    issued_cancel_enabled = false;
                    wl_columnar_eval_stack_test_fail_allocation = -1;
                    wl_columnar_session_test_framed_grant_finished = NULL;
                    wl_columnar_session_test_terminal_finance_phase = NULL;
                    CHECK(!close || bootstrap_close_refused);
                    CHECK(!bootstrap_outer_pair_active);
                    wl_columnar_session_test_financier_outer_source_phase =
                        NULL;
                    if (permanent) {
                        wl_evaluation_control_report_t bank;
                        CHECK(bootstrap_outer_calls &&
                            bootstrap_outer_maintenance_calls
                            && bootstrap_outer_calls ==
                            bootstrap_outer_main_calls
                            + bootstrap_outer_maintenance_calls
                            && (bootstrap_bank_role ==
                            WL_SESSION_SOURCE_METADATA_MAIN
                            || bootstrap_bank_role ==
                            WL_SESSION_SOURCE_METADATA_MAINTENANCE)
                            && !wl_session_test_source_unstarted_reset_cancel(
                                f.sessions[0], &bank));
                        native_report_same(&bootstrap_bank_stop, &bank);
                    }
                    if (!native_ended || rc != ENOMEM) fprintf(stderr,
                            "bootstrap-end mode=%u successor=%u ended=%u rc=%d\n",
                            mode, successor, native_ended, rc);
                    CHECK(native_ended && rc == ENOMEM);
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report));
                    native_report_same(&native_report, &report);
                    wl_col_session_t *col = (wl_col_session_t *)f.sessions[0];
                    CHECK(!col->cleanup_active && !col->cleanup_pending
                        && !col->cleanup_reserved_bytes);
                    if (successor) {
                        uint64_t used = UINT64_MAX;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.control,
                                native_successor_id, &used) && !used);
                        wl_evaluation_control_attempt_finish_result_t end =
                            wl_evaluation_control_finish_attempt(f.control,
                                f.sessions[0]->operation_admission,
                                native_successor_id,
                                0, 0);
                        CHECK(!end.protocol && end.report_valid &&
                            !end.report.result
                            && !end.report.consumed);
                    }
                    CHECK(!wl_evaluation_control_reset(f.control));
                    trace();
                    int retry = mode == 2
            ? wl_session_snapshot(f.sessions[0], snapshot, &rows)
            : finish(f.sessions[0]);
                    CHECK(!retry); verify(f.sessions[0], &f.state);
                    if (mode == 2) {
                        state_t empty = {0};
                        expect_events(&rows, &empty, &f.state);
                    }
                    destroy(&f); native_fixture = NULL;
                }
}

typedef struct {
    wl_session_t *session;
    wl_evaluation_control_t *control;
    bool successor;
    unsigned calls, seen[17][17];
    uint64_t next_id;
    wl_evaluation_control_report_t report;
} native_snapshot_capture_t;

static void
native_snapshot_end_callback(const char *name, const int64_t *row,
    uint32_t width,
    void *userdata)
{
    native_snapshot_capture_t *out = userdata;
    CHECK(!strcmp(name, "output") && width == 2);
    CHECK(row[0] >= 0 && row[0] < 17 && row[1] >= 0 && row[1] < 17);
    CHECK(!out->seen[row[0]][row[1]]++);
    if (++out->calls != 1) return;
    uint64_t id = wl_evaluation_control_active_attempt_id(out->control);
    wl_evaluation_control_attempt_finish_result_t ended =
        wl_evaluation_control_finish_attempt(out->control,
            out->session->operation_admission, id, 0, 0);
    CHECK(!ended.protocol && ended.report_valid && !ended.report.result
        && ended.report.consumed);
    out->report = ended.report;
    if (out->successor) {
        CHECK(!wl_evaluation_control_reset(out->control));
        wl_evaluation_control_attempt_begin_result_t begin =
            wl_evaluation_control_begin_attempt(out->control,
                out->session->operation_admission);
        CHECK(!begin.protocol && begin.attempt_id != id);
        out->next_id = begin.attempt_id;
    }
    if (snapshot_identity_exhaustion >= 0) {
        /* Accelerate only the process counter, then really create/release
         * the final controller after the outer Snapshot financier exists. */
        CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1));
        wl_evaluation_control_t *last = NULL;
        CHECK(!wl_evaluation_control_create(0, &last) && last);
        wl_evaluation_control_release(last);
    }
}

static void
native_snapshot_end(void)
{
    for (unsigned completed = 0; completed < 2; ++completed)
        for (unsigned successor = 0; successor < 2; ++successor) {
            if (snapshot_identity_exhaustion >= 0
                && (unsigned)snapshot_identity_exhaustion !=
                completed * 2 + successor) continue;
            const char *source =
                ".decl a(k:int64,x:int64)\n.decl b(k:int64,y:int64)\n"
                ".decl output(x:int64,y:int64)\noutput(x,y):-a(k,x),b(k,y).\n";
            wirelog_error_t error;
            wirelog_program_t *program = wirelog_parse_string(source, &error);
            wl_plan_t *plan = NULL;
            CHECK(program && !wl_plan_from_program(program, &plan));
            native_snapshot_capture_t out = {0};
            out.successor = successor;
            CHECK(!wl_evaluation_control_create(0, &out.control));
            wl_session_options_t options;
            wl_session_options_init(&options);
            options.evaluation_control = out.control;
            configure("WIRELOG_JOIN_BATCH_BYTES", "64");
            configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
            configure("WIRELOG_JOIN_OUTPUT_LIMIT", "0");
            CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan,
                1,
                &options, &out.session));
            configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
            configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
            int64_t input[34];
            for (unsigned i = 0; i < 17; ++i) {
                input[2*i] = 1; input[2*i+1] = i;
            }
            CHECK(!wl_session_insert(out.session, "a", input, 17, 2));
            CHECK(!wl_session_insert(out.session, "b", input, 17, 2));
            stop_event = 0; trace();
            if (completed) CHECK(!finish(out.session));
            int snapshot_rc = wl_session_snapshot(out.session,
                    native_snapshot_end_callback, &out);
            if (snapshot_rc) fprintf(stderr,
                    "postcutoff snapshot completed=%u successor=%u rc=%d calls=%u phase=%u\n",
                    completed, successor, snapshot_rc, out.calls,
                    wl_columnar_session_test_native_error_phase(
                        (wl_col_session_t *)out.session));
            CHECK(!snapshot_rc);
            wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
            CHECK(prepared == 1 && produced > 2 && acked > 2 &&
                out.calls == 289);
            for (unsigned x = 0; x < 17; ++x)
                for (unsigned y = 0; y < 17; ++y) CHECK(out.seen[x][y] == 1);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(out.control, &report));
            native_report_same(&out.report, &report);
            if (successor) {
                uint64_t used = UINT64_MAX;
                CHECK(wl_evaluation_control_active_attempt_id(out.control) ==
                    out.next_id);
                CHECK(!wl_evaluation_control_test_active_consumed(out.control,
                    out.next_id, &used)
                    && !used);
                wl_evaluation_control_attempt_finish_result_t ended =
                    wl_evaluation_control_finish_attempt(out.control,
                        out.session->operation_admission, out.next_id, 0, 0);
                CHECK(!ended.protocol && ended.report_valid &&
                    !ended.report.result
                    && !ended.report.consumed && !ended.report.stop_reason);
            }
            CHECK(!((wl_col_session_t *)out.session)->cleanup_active
                && !((wl_col_session_t *)out.session)->cleanup_pending);
            CHECK(!wl_session_try_destroy(&out.session));
            wl_evaluation_control_release(out.control);
            wl_plan_free(plan); wirelog_program_free(program);
        }
}

/* Failed unpublished tx construction must not allocate another financier to
 * report failure, even when no process identity remains. */
static fixture_t *constructor_fixture;
static unsigned constructor_fault, constructor_target, constructor_calls[8],
    constructor_histories;
static bool constructor_snapshot, constructor_late, constructor_point_seen,
    constructor_refused, constructor_turnover, constructor_frozen,
    constructor_late_turnover, constructor_keep_bank_cancelled,
    constructor_without_c1, constructor_outer_started;
static uint64_t constructor_successor;
static wl_evaluation_control_report_t constructor_report;
static wl_evaluation_control_report_t constructor_bank_stop;
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

static void
constructor_peer(void)
{
    wl_evaluation_control_test_finance_birth_claim = NULL;
    wl_evaluation_control_t *peer = NULL;
    CHECK(!wl_evaluation_control_create(1, &peer));
    wl_evaluation_control_release(peer);
}

static void
constructor_checkpoint(wl_evaluation_control_t *c, unsigned point,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)progress; (void)bindings;
    if (point != 49) return;
    fixture_t *f = constructor_fixture;
    CHECK(c == f->control && !constructor_point_seen);
    uint64_t v[8], used = 0;
    CHECK(!wl_columnar_session_test_constructor_error_view(
            (wl_col_session_t *)f->sessions[0], v) && !v[0] && !v[1]);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(id && !wl_evaluation_control_test_active_consumed(c, id, &used)
        && used >= 64);
    constructor_point_seen = true;
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    if (!constructor_fault)
        wl_evaluation_control_test_finance_birth_claim = constructor_peer;
    else if (constructor_fault < 3)
        wl_evaluation_control_test_finance_birth_fail = constructor_fault;
    else {
        CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX - 1));
        wl_evaluation_control_t *last = NULL;
        CHECK(!wl_evaluation_control_create(1, &last));
        wl_evaluation_control_release(last);
    }
    if (constructor_turnover) {
        CHECK(wl_evaluation_control_finish(c,
            f->sessions[0]->operation_admission, 7, 0) == 7);
        CHECK(!wl_evaluation_control_report(c, &constructor_report));
        constructor_frozen = true;
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c,
            f->sessions[0]->operation_admission));
        constructor_successor = wl_evaluation_control_active_attempt_id(c);
        CHECK(constructor_successor && constructor_successor != id);
    }
}

static void *
constructor_foreign(void *argument)
{
    wl_session_t *session = argument;
    CHECK(wl_session_step(session) == EBUSY);
    CHECK(session->backend->session_controlled_step_state(session)
        == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
    return NULL;
}

static void
constructor_phase(wl_col_session_t *session, unsigned phase, bool after,
    int financial, bool called)
{
    fixture_t *f = constructor_fixture;
    CHECK(!constructor_outer_started); /* No BANK retry after the real stop. */
    CHECK(session == (wl_col_session_t *)f->sessions[0]);
    uint64_t v[8];
    CHECK(!wl_columnar_session_test_constructor_error_view(session, v)
        && v[0] && !v[1] && v[4] && v[5]);
    if (!after && phase == constructor_target && !constructor_refused) {
        constructor_refused = true;
        if (constructor_late)
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
        else wl_evaluation_control_test_source_lexical_cancel_before_action =
                true;
    }
    if (!after) return;
    wl_evaluation_control_report_t source;
    CHECK(!wl_session_test_source_early_report(f->sessions[0], &source)
        && source.result == financial
        && (financial ? (source.consumed == 0 || source.consumed == 10
        || source.consumed == 23) : source.consumed == 29));
    if (constructor_late_turnover && !constructor_frozen && called
        && phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_FINISH) {
        CHECK(!constructor_turnover && !constructor_snapshot);
        CHECK(!wl_evaluation_control_active_attempt_id(f->control));
        CHECK(!wl_evaluation_control_report(f->control, &constructor_report)
            && constructor_report.result == ENOMEM);
        constructor_frozen = true;
        CHECK(!wl_evaluation_control_reset(f->control));
        CHECK(!wl_evaluation_control_begin(f->control,
            f->sessions[0]->operation_admission));
        constructor_successor =
            wl_evaluation_control_active_attempt_id(f->control);
        CHECK(constructor_successor && constructor_successor !=
            constructor_report.attempt_id);
    }
    if (called) {
        CHECK(phase > 0 && phase < 8);
        ++constructor_calls[phase];
    }
    if (financial) {
        CHECK(constructor_refused &&
            financial == WL_EVALUATION_CONTROL_CANCELLED
            && called == constructor_late);
        constructor_bank_stop = source;
        if (!constructor_keep_bank_cancelled)
            CHECK(!wl_session_test_source_final_reset_cancel(f->sessions[0]));
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, constructor_foreign, f->sessions[0]));
        CHECK(!wl_thread_join(&thread));
        const int64_t row[2] = {1, 0};
        CHECK(wl_session_insert(f->sessions[0], "a", row, 0, 2) == EBUSY);
    }
    if (phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_RESET && called) {
        CHECK(v[0] == WL_COLUMNAR_SESSION_CONSTRUCTOR_READY);
        CHECK(v[2] == constructor_snapshot);
    }
    if (constructor_frozen) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f->control, &report));
        native_report_same(&constructor_report, &report);
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(f->control,
            constructor_successor, &used) && !used);
    }
}

static void
constructor_outer_phase(wl_col_session_t *session, unsigned phase, bool after,
    uint64_t source_id, int result, bool called)
{
    fixture_t *f = constructor_fixture;
    constructor_outer_started = true;
    CHECK(session == (wl_col_session_t *)f->sessions[0]
        && constructor_snapshot && constructor_refused && source_id && !result
        && (phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_GUARD
        || phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_QUIET
        || phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_QUIET_BORROWS
        || phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_RESET
        || phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_READY));
    uint64_t v[8];
    CHECK(!wl_columnar_session_test_constructor_error_view(session, v)
        && v[0] && v[4] && v[5] && v[2]);
    if (after) {
        CHECK(called || phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_READY);
        if (called) ++constructor_calls[phase];
        if (phase == WL_COLUMNAR_SESSION_CONSTRUCTOR_RESET)
            CHECK(v[0] == WL_COLUMNAR_SESSION_CONSTRUCTOR_READY);
    } else {
        CHECK(!called && v[0] == phase);
        CHECK(wl_session_step(f->sessions[0]) == EBUSY);
    }
    if (constructor_frozen) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f->control, &report));
        native_report_same(&constructor_report, &report);
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(f->control,
            constructor_successor, &used) && !used);
    }
}

static void
constructor_history(unsigned fault, bool snapshot_mode, unsigned target,
    bool late, bool turnover)
{
    fixture_t f; init(&f, !snapshot_mode);
    constructor_fixture = &f; constructor_fault = fault;
    constructor_snapshot = snapshot_mode; constructor_target = target;
    constructor_late = late; constructor_turnover = turnover;
    constructor_point_seen = constructor_refused = constructor_frozen = false;
    constructor_outer_started = false;
    constructor_successor = 0;
    memset(constructor_calls, 0, sizeof(constructor_calls));
    if (constructor_without_c1) {
        CHECK(snapshot_mode);
        f.sessions[0]->source_c1_admission = false;
        ((wl_col_session_t *)f.sessions[0])->join_batch_bytes = 0;
        ((wl_col_session_t *)f.sessions[0])->join_batch_strict = false;
    }
    col_rel_t *input = session_find_rel((wl_col_session_t *)f.sessions[0], "a");
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(input);
    wl_columnar_session_test_observed_transaction_checkpoint =
        constructor_checkpoint;
    wl_columnar_session_test_constructor_error_phase = constructor_phase;
    wl_columnar_session_test_constructor_outer_source_phase =
        constructor_outer_phase;
    capture_t rows = {0};
    issued_cancel_enabled = fault == 1 && !snapshot_mode && !target;
    issued_cancel_role = WL_SESSION_SOURCE_METADATA_CONSTRUCTOR;
    issued_cancel_hits = 0;
    int rc = snapshot_mode ? wl_session_snapshot(f.sessions[0], snapshot, &rows)
        : finish(f.sessions[0]);
    CHECK(!issued_cancel_enabled || issued_cancel_hits == 1);
    issued_cancel_enabled = false;
    CHECK(constructor_point_seen &&
        !wl_evaluation_control_test_finance_birth_fail
        && !wl_evaluation_control_test_finance_birth_claim);
    CHECK(rc == (fault == 0 ? 0 : fault < 3 ? ENOMEM : EOVERFLOW));
    CHECK(!target || constructor_refused);
    CHECK(constructor_outer_started == (snapshot_mode && target != 0));
    CHECK(constructor_calls[WL_COLUMNAR_SESSION_CONSTRUCTOR_FINISH] ==
        (!fault || snapshot_mode ? 0u : 1u));
    CHECK(constructor_calls[WL_COLUMNAR_SESSION_CONSTRUCTOR_RESET] ==
        (fault ? 1u : 0u));
    if (fault) CHECK(wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(input)));
    uint64_t v[8];
    CHECK(!wl_columnar_session_test_constructor_error_view(
            (wl_col_session_t *)f.sessions[0], v) && !v[0]);
    if (fault) {
        state_t empty = {0};
        expect_events(&f.events[0], &empty, &empty);
        expect_events(&rows, &empty, &empty);
        const col_rel_t *head =
            session_find_rel((wl_col_session_t *)f.sessions[0],
                "output");
        CHECK(!head || !head->nrows);
    } else verify(f.sessions[0], &f.state); /* CAS-loss genuinely retries forward. */
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report));
    if (constructor_frozen) {
        native_report_same(&constructor_report, &report);
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(f.control,
            constructor_successor, &used) && !used);
        CHECK(!wl_evaluation_control_finish(f.control,
            f.sessions[0]->operation_admission, 0, 0));
    } else CHECK(report.result == rc && report.consumed);
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    wl_columnar_session_test_constructor_error_phase = NULL;
    wl_columnar_session_test_constructor_outer_source_phase = NULL;
    if (constructor_keep_bank_cancelled) {
        wl_evaluation_control_report_t bank;
        CHECK(snapshot_mode && constructor_outer_started
            && !wl_session_test_source_unstarted_reset_cancel(f.sessions[0],
            &bank));
        native_report_same(&constructor_bank_stop, &bank);
    }
    if (fault && fault < 3) {
        CHECK(!wl_evaluation_control_reset(f.control));
        if (snapshot_mode) CHECK(!wl_session_snapshot(f.sessions[0], snapshot,
                &rows));
        else CHECK(!finish(f.sessions[0]));
        verify(f.sessions[0], &f.state);
    }
    destroy(&f); constructor_fixture = NULL;
    ++constructor_histories;
}

static void
constructor_faults(void)
{
    for (unsigned snap = 0; snap < 2; ++snap) {
        constructor_history(0, snap, 0, false, false);
        for (unsigned fault = 1; fault <= 2; ++fault)
            for (unsigned target = 0; target <= 7; ++target) {
                if (snap && (target == WL_COLUMNAR_SESSION_CONSTRUCTOR_FINISH
                    || target == WL_COLUMNAR_SESSION_CONSTRUCTOR_REPORT))
                    continue;
                for (unsigned late = 0; late < 2; ++late)
                    constructor_history(fault, snap, target, late,
                        target == WL_COLUMNAR_SESSION_CONSTRUCTOR_REPORT
                        ? false : target % 2);
            }
    }
    constructor_late_turnover = true;
    for (unsigned fault = 1; fault <= 2; ++fault)
        for (unsigned late = 0; late < 2; ++late)
            constructor_history(fault, false,
                WL_COLUMNAR_SESSION_CONSTRUCTOR_REPORT, late, false);
    constructor_late_turnover = false;
    constructor_keep_bank_cancelled = true;
    const unsigned phases[] = {WL_COLUMNAR_SESSION_CONSTRUCTOR_GUARD,
                               WL_COLUMNAR_SESSION_CONSTRUCTOR_QUIET,
                               WL_COLUMNAR_SESSION_CONSTRUCTOR_QUIET_BORROWS,
                               WL_COLUMNAR_SESSION_CONSTRUCTOR_RESET,
                               WL_COLUMNAR_SESSION_CONSTRUCTOR_READY};
    for (unsigned p = 0; p < sizeof(phases) / sizeof(*phases); ++p)
        for (unsigned late = 0; late < 2; ++late)
            constructor_history(1, true, phases[p], late, p % 2);
    constructor_without_c1 = true;
    for (unsigned late = 0; late < 2; ++late)
        constructor_history(1, true, WL_COLUMNAR_SESSION_CONSTRUCTOR_QUIET,
            late, false);
    constructor_without_c1 = constructor_keep_bank_cancelled = false;
    CHECK(constructor_histories == 74);
    printf("constructor failures: %u authentic public histories passed\n",
        constructor_histories);
}

static fixture_t *snapshot_finish_fixture;
static unsigned snapshot_finish_scenario, snapshot_finish_resets;
static bool snapshot_finish_pending_refused, snapshot_finish_selected_rejected;
static uint64_t snapshot_finish_id, snapshot_finish_consumed;
static uint64_t snapshot_finish_retained[12];
static unsigned snapshot_finish_outer_selected;

static void
snapshot_finish_outer_phase(wl_col_session_t *session, unsigned event)
{
    fixture_t *f = snapshot_finish_fixture;
    CHECK(session == (wl_col_session_t *)f->sessions[0]);
    uint64_t retained[12], used = 0;
    CHECK(!wl_columnar_session_test_snapshot_finish_retained_view(session,
        retained));
    for (unsigned i = 0; i < 12; ++i)
        CHECK(retained[i] == snapshot_finish_retained[i]);
    CHECK(wl_evaluation_control_active_attempt_id(f->control) ==
        snapshot_finish_id
        && !wl_evaluation_control_test_active_consumed(f->control,
        snapshot_finish_id, &used) && used == snapshot_finish_consumed);
    if (event == WL_COLUMNAR_SESSION_TEST_SNAPSHOT_OUTER_RESET)
        ++snapshot_finish_resets;
    if (event == WL_COLUMNAR_SESSION_TEST_SNAPSHOT_OUTER_AFTER_SELECTED)
        ++snapshot_finish_outer_selected;
    unsigned boundary = snapshot_finish_scenario == 3
        ? WL_COLUMNAR_SESSION_TEST_SNAPSHOT_OUTER_BEFORE_SELECTED
        : WL_COLUMNAR_SESSION_TEST_SNAPSHOT_OUTER_AFTER_SELECTED;
    if (snapshot_finish_scenario >= 3 && event == boundary
        && !snapshot_finish_selected_rejected) {
        wl_session_t copied = *f->sessions[0];
        CHECK(!wl_session_source_snapshot_checkpoint(&copied).selected
            && !wl_session_source_snapshot_checkpoint(f->sessions[1]).selected);
        wl_session_source_snapshot_checkpoint_result_t actual =
            wl_session_source_snapshot_checkpoint(f->sessions[0]);
        CHECK(actual.selected && !actual.financial_result);
        snapshot_finish_selected_rejected = true;
    }
}

static void
snapshot_finish_phase(wl_col_session_t *session, unsigned phase, bool after,
    int financial, bool called)
{
    fixture_t *f = snapshot_finish_fixture;
    CHECK(session == (wl_col_session_t *)f->sessions[0]);
    uint64_t v[8], used = 0;
    CHECK(!wl_columnar_session_test_snapshot_finish_view(session, v)
        && v[0] && v[2] && v[3] && v[4] && v[7]);
    uint64_t id = wl_evaluation_control_active_attempt_id(f->control);
    CHECK(id == v[4] && !wl_evaluation_control_test_active_consumed(f->control,
        id, &used));
    if (!after) {
        snapshot_finish_id = id; snapshot_finish_consumed = used;
        CHECK(!wl_columnar_session_test_snapshot_finish_retained_view(session,
            snapshot_finish_retained));
        if (phase == WL_COLUMNAR_SESSION_SNAPSHOT_FINISH_PENDING
            && snapshot_finish_scenario && !snapshot_finish_pending_refused) {
            snapshot_finish_pending_refused = true;
            if (snapshot_finish_scenario == 1)
                wl_evaluation_control_test_source_lexical_cancel_before_action =
                    true;
            else wl_evaluation_control_test_source_lexical_cancel_after_action =
                    true;
        }
        return;
    }
    CHECK(id == snapshot_finish_id && used == snapshot_finish_consumed);
    if (phase == WL_COLUMNAR_SESSION_SNAPSHOT_FINISH_PENDING && called && v[1])
        ++snapshot_finish_resets;
    if (financial) {
        CHECK(financial == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_session_test_source_final_reset_cancel(f->sessions[0]));
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, constructor_foreign, f->sessions[0]));
        CHECK(!wl_thread_join(&thread));
        const int64_t row[2] = {1, 0};
        CHECK(wl_session_insert(f->sessions[0], "a", row, 0, 2) == EBUSY);
    }
}

static void
snapshot_finish_refusals(void)
{
    unsigned histories = 0;
    for (unsigned prior = 0; prior < 2; ++prior)
        for (unsigned fail = 0; fail < 2; ++fail)
            for (unsigned scenario = 0; scenario < 5; ++scenario) {
                fixture_t f; init(&f, true);
                state_t old = {0}, empty = {0};
                if (prior) {
                    CHECK(!finish(f.sessions[0]));
                    old = f.state;
                    memset(&f.events[0], 0, sizeof(f.events[0]));
                    CHECK(!wl_session_insert(f.sessions[0], "c",
                        (int64_t[]){7, 207}, 1, 2));
                    f.state.extra = true;
                }
                snapshot_finish_fixture = &f;
                snapshot_finish_scenario = scenario;
                snapshot_finish_pending_refused =
                    snapshot_finish_selected_rejected = false;
                snapshot_finish_resets = snapshot_finish_outer_selected = 0;
                wl_columnar_session_test_snapshot_finish_outer_phase =
                    snapshot_finish_outer_phase;
                wl_columnar_session_test_snapshot_finish_phase =
                    snapshot_finish_phase;
                if (fail) wl_columnar_eval_stack_test_fail_allocation = 0;
                capture_t rows = {0};
                int rc = wl_session_snapshot(f.sessions[0], snapshot, &rows);
                wl_columnar_eval_stack_test_fail_allocation = -1;
                wl_columnar_session_test_snapshot_finish_phase = NULL;
                wl_columnar_session_test_snapshot_finish_outer_phase = NULL;
                CHECK(snapshot_finish_outer_selected == (scenario ? 1u : 0u));
                CHECK(rc == (fail ? ENOMEM : 0) && snapshot_finish_resets == 1);
                CHECK(snapshot_finish_pending_refused == (scenario != 0)
                    && snapshot_finish_selected_rejected == (scenario >= 3));
                uint64_t v[8];
                CHECK(!wl_columnar_session_test_snapshot_finish_view(
                        (wl_col_session_t *)f.sessions[0], v)
                    && !v[0] && !v[2] && !v[3]);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report)
                    && report.attempt_id == snapshot_finish_id
                    && report.result == rc && report.consumed);
                if (fail) {
                    expect_events(&rows, &empty, &empty);
                    expect_events(&f.events[0], &old, &old);
                    CHECK(!wl_evaluation_control_reset(f.control));
                    CHECK(!wl_session_snapshot(f.sessions[0], snapshot, &rows));
                } else expect_events(&f.events[0], &old, &f.state);
                expect_events(&rows, &empty, &f.state);
                verify(f.sessions[0], &f.state);
                destroy(&f); snapshot_finish_fixture = NULL;
                ++histories;
            }
    CHECK(histories == 20);
    printf("snapshot finish: %u authentic financial refusal histories passed\n",
        histories);
}

static wl_session_t *snapshot_prior_session;
static wl_evaluation_control_t *snapshot_prior_control;
static wl_evaluation_control_report_t snapshot_prior_report;
static unsigned snapshot_prior_target, snapshot_prior_stop_mode;
static bool snapshot_prior_refused, snapshot_prior_main_seen;

static void
snapshot_prior_finance(wl_col_session_t *session, bool maintenance,
    wl_evaluation_control_t *funding, uint64_t id, unsigned phase,
    bool after, int financial)
{
    CHECK(!maintenance && &session->base == snapshot_prior_session);
    uint64_t v[16], snapshot_view[8];
    CHECK(!wl_columnar_session_test_terminal_view(session, v)
        && v[1] == WL_COLUMNAR_SESSION_TERMINAL_SNAPSHOT_PRIOR && v[11]);
    CHECK(!wl_columnar_session_test_snapshot_prior_view(session, snapshot_view)
        && snapshot_view[0] && !snapshot_view[1] && !snapshot_view[2]
        && !snapshot_view[3] && !snapshot_view[4] && !snapshot_view[7]);
    snapshot_prior_main_seen = true;
    if (!after && phase == snapshot_prior_target && !snapshot_prior_refused) {
        snapshot_prior_refused = true;
        if (snapshot_prior_stop_mode == 1)
            wl_evaluation_control_test_source_lexical_cancel_before_action =
                true;
        else wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
    }
    if (!after && (phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH
        || phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT
        || phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_DETACH
        || phase == WL_COLUMNAR_SESSION_FINANCE_CLOSE_RELEASE))
        CHECK(funding && id); /* Semantic id0 never replaces funding.id. */
    if (after && financial) {
        CHECK(snapshot_prior_refused && financial ==
            WL_EVALUATION_CONTROL_CANCELLED);
        if (!snapshot_prior_stop_mode)
            CHECK(!wl_session_test_source_final_reset_cancel(&session->base));
    }
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(snapshot_prior_control, &report));
    native_report_same(&snapshot_prior_report, &report);
    CHECK(!wl_evaluation_control_active_attempt_id(snapshot_prior_control));
}

static void
snapshot_no_begin_prior(void)
{
    const unsigned stages[] = {0, WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT,
                               WL_COLUMNAR_SESSION_FINANCE_CLOSE_RELEASE};
    unsigned histories = 0;
    for (unsigned i = 0; i < sizeof(stages) / sizeof(*stages); ++i)
        for (unsigned stop_mode = 0; stop_mode < (i ? 3u : 1u);
            ++stop_mode) {
            fixture_t f; init(&f, true);
            snapshot_prior_stop_mode = stop_mode;
            capture_t baseline = {0};
            CHECK(!wl_session_snapshot(f.sessions[0], snapshot, &baseline)
                && !finish(f.sessions[1]));
            state_t baseline_empty = {0};
            expect_events(&baseline, &baseline_empty, &f.state);
            verify(f.sessions[0], &f.state);
            uint64_t v[8];
            CHECK(!wl_columnar_session_test_snapshot_prior_view(
                    (wl_col_session_t *)f.sessions[0], v)
                && v[5] && v[6] && !v[1]);
            wl_columnar_memory_governor_ref_t *governor =
                ((wl_col_session_t *)f.sessions[0])->memory_governor;
            wl_columnar_memory_governor_ref_retain(governor);
            memset(&f.events[0], 0, sizeof(f.events[0]));
            const int64_t row[2] = {7, 207};
            CHECK(!wl_session_insert(f.sessions[0], "c", row, 1, 2));
            CHECK(!wl_evaluation_control_test_seed_attempt_id(f.control,
                f.sessions[0]->operation_admission, UINT64_MAX));
            CHECK(!wl_evaluation_control_report(f.control,
                &snapshot_prior_report));
            snapshot_prior_control = f.control;
            snapshot_prior_session = f.sessions[0];
            snapshot_prior_target = stages[i];
            snapshot_prior_refused = snapshot_prior_main_seen = false;
            capture_t rows = {0};
            CHECK(wl_session_snapshot(f.sessions[0], snapshot,
                &rows) == EOVERFLOW);
            CHECK(!wl_columnar_session_test_snapshot_prior_view(
                    (wl_col_session_t *)f.sessions[0], v)
                && v[0] && !v[1] && !v[2] && !v[3] && !v[4]
                && v[5] && v[6] && !v[7]);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            native_report_same(&snapshot_prior_report, &report);
            state_t empty = {0};
            expect_events(&rows, &empty, &empty);
            expect_events(&f.events[0], &empty, &empty);
            verify(f.sessions[0], &f.state);     /* No new evaluation/cutoff. */
            uint64_t finance[12];
            CHECK(!wl_columnar_session_test_snapshot_finish_retained_view(
                    (wl_col_session_t *)f.sessions[0], finance)
                && finance[0] && finance[0] == finance[1]
                && !finance[2] && !finance[3]
                && finance[4] && finance[5]
                && !finance[6] && !finance[7]);
            wl_columnar_session_test_terminal_finance_phase =
                snapshot_prior_finance;
            int closing = wl_session_try_destroy(&f.sessions[0]);
            if (stages[i]) {
                CHECK(closing == EAGAIN && f.sessions[0]);
                if (stop_mode) {
                    for (unsigned stopped_retry = 0; stopped_retry < 3;
                        ++stopped_retry) {
                        wl_session_t *saved = f.sessions[0];
                        CHECK(wl_session_step(saved) == EBUSY);
                        CHECK(wl_session_snapshot(saved, snapshot,
                            &rows) == EBUSY);
                        CHECK(wl_session_insert(saved, "c", row, 0,
                            2) == EBUSY);
                        wl_session_exclusive_attempt_t exclusive = {0};
                        CHECK(wl_session_exclusive_attempt_acquire(saved,
                            &exclusive) == EBUSY);
                        CHECK(wl_session_try_destroy(&f.sessions[0]) ==
                            EAGAIN
                            && f.sessions[0] == saved);
                    }
                    wl_evaluation_control_report_t bank;
                    CHECK(!wl_session_test_source_parked_close_reset_cancel(
                            f.sessions[0], &bank)
                        && bank.result == WL_EVALUATION_CONTROL_CANCELLED
                        && bank.consumed == 0);
                }
                /* The hook or quiescent test helper genuinely reset the BANK. Each admitted
                 * retry must perform its own current financial observation;
                 * an old completed report does not authorize continuation. */
                for (unsigned retry = 0; closing == EAGAIN && retry < 256;
                    ++retry) {
                    CHECK(f.sessions[0]
                        && wl_session_step(f.sessions[0]) == EBUSY);
                    closing = wl_session_try_destroy(&f.sessions[0]);
                }
            }
            CHECK(!closing && !f.sessions[0]);
            wl_columnar_session_test_terminal_finance_phase = NULL;
            CHECK(snapshot_prior_main_seen
                && (!stages[i] || snapshot_prior_refused));
            CHECK(!wl_evaluation_control_report(f.control, &report));
            native_report_same(&snapshot_prior_report, &report);
            CHECK(!wl_session_try_destroy(&f.sessions[1]));
            wl_evaluation_control_release(f.control);
            wl_plan_free(f.plan); wirelog_program_free(f.program);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
            snapshot_prior_session = NULL; snapshot_prior_control = NULL;
            ++histories;
        }
    CHECK(histories == 7);
    printf("prior Snapshot: %u genuine Main close histories passed\n",
        histories);
}
/* Actual fresh and published reset binding under public admission. Only
 * the real private BANK cancellation seam is used; no Native owner is faked. */
static wl_session_t *metadata_bind_session;
static unsigned metadata_bind_stop, metadata_bind_seen;
static bool metadata_bind_reset, metadata_bind_snapshot, metadata_bind_fired,
    metadata_bind_closing;
static uint64_t metadata_bind_generation;

static void
metadata_bind_phase(wl_col_session_t *sess, unsigned cursor, bool after,
    int financial, bool published, uint64_t operation_id, uint64_t payload_id,
    bool snapshot_owned)
{
    if (&sess->base != metadata_bind_session) return;
    CHECK(snapshot_owned == metadata_bind_snapshot);
    CHECK(wl_session_step(&sess->base) == EBUSY);
    wl_session_t *same = &sess->base;
    CHECK(wl_session_try_destroy(&same) == EBUSY && same == &sess->base);
    if (published) {
        CHECK(metadata_bind_reset || !metadata_bind_snapshot);
        uint64_t view[10];
        CHECK(!wl_columnar_session_test_metadata_bind_view(sess, view)
            && view[0] == 1 && view[1] == cursor && view[3]);
        if (snapshot_owned && operation_id) {
            /* Genuine successful BEGIN was published before the new seam. */
            CHECK(payload_id == operation_id && view[4] == operation_id
                && view[5] && view[6] && view[7] && view[9] == operation_id);
            metadata_bind_generation = operation_id;
        }
    } else CHECK(!metadata_bind_reset && !operation_id && !payload_id);
    if (!after && !metadata_bind_fired) {
        if (!metadata_bind_snapshot) {
            if (cursor != metadata_bind_stop - 1) return;
            metadata_bind_fired = true;
            wl_evaluation_control_test_source_metadata_cancel_at =
                cursor == 4 ? 2 : 4;
            return;
        }
        CHECK(cursor == (metadata_bind_closing ? metadata_bind_stop - 1 : 0));
        metadata_bind_fired = true;
        wl_evaluation_control_test_source_metadata_cancel_at =
            metadata_bind_closing ? (cursor == 4 ? 1 : cursor + 2) :
            metadata_bind_stop;
    }
    if (after && financial) {
        CHECK(metadata_bind_fired
            && financial == WL_EVALUATION_CONTROL_CANCELLED
            && cursor == (!metadata_bind_snapshot ?
            (metadata_bind_stop < 5 ? metadata_bind_stop : 4) :
            metadata_bind_closing ?
            (metadata_bind_stop < 5 ? metadata_bind_stop : 4) :
            metadata_bind_stop - 1));
        if (!metadata_bind_snapshot) {
            wl_evaluation_control_report_t source;
            CHECK(!wl_session_test_source_early_report(&sess->base, &source)
                && source.consumed == (metadata_bind_stop < 5 ? 10u : 0u));
        }
        ++metadata_bind_seen;
        CHECK(!wl_session_test_source_final_reset_cancel(&sess->base));
    }
}

static unsigned issued_bind_cursor, issued_bind_slot, issued_bind_before,
    issued_bind_after, issued_bind_births;

static void
issued_bind_birth(void)
{
    ++issued_bind_births;
}

static void
issued_bind_phase(wl_col_session_t *sess, unsigned cursor, bool after,
    int financial, bool published, uint64_t operation_id, uint64_t payload_id,
    bool snapshot_owned)
{
    CHECK(&sess->base == metadata_bind_session && published && !snapshot_owned
        && !operation_id && !payload_id && !issued_bind_births);
    if (!after) {
        CHECK(cursor == issued_bind_cursor && !financial);
        ++issued_bind_before;
        wl_evaluation_control_test_source_metadata_cancel_at = issued_bind_slot;
    } else {
        ++issued_bind_after;
        CHECK(financial == WL_EVALUATION_CONTROL_CANCELLED
            && cursor ==
            issued_bind_cursor + (issued_bind_slot == 4 ? 1u : 0u));
    }
}

static void
metadata_binding_issued(void)
{
    unsigned histories = 0;
    for (issued_bind_cursor = 0; issued_bind_cursor <= 4; ++issued_bind_cursor)
        for (issued_bind_slot = 1; issued_bind_slot <= 4; ++issued_bind_slot) {
            if (issued_bind_cursor == 4 && issued_bind_slot == 4) continue;
            fixture_t f; init(&f, true);
            metadata_bind_session = f.sessions[0];
            CHECK(wl_session_step(f.sessions[0]) == EAGAIN);
            uint64_t view[10];
            CHECK(!wl_columnar_session_test_metadata_bind_view(
                    (wl_col_session_t *)f.sessions[0], view)
                && view[0] == 1 && !view[1] && !view[3] && !view[4] &&
                !view[5]);
            issued_bind_births = 0;
            wl_evaluation_control_test_finance_birth_claim = issued_bind_birth;
            for (unsigned cursor = 0; cursor < issued_bind_cursor; ++cursor) {
                CHECK(wl_session_step(f.sessions[0]) == EAGAIN
                    && !wl_columnar_session_test_metadata_bind_view(
                        (wl_col_session_t *)f.sessions[0], view)
                    && view[0] == 1 && view[1] == cursor + 1 && !view[3]
                    && !view[4] && !view[5] && !issued_bind_births);
                wl_evaluation_control_report_t report;
                CHECK(!wl_session_test_source_pending_report(
                        f.sessions[0], &report));
                CHECK(!report.result);
                CHECK(report.consumed == 10);
            }
            unsigned repetitions = issued_bind_cursor == 4 ? 3 : 1;
            for (unsigned repeat = 0; repeat < repetitions; ++repeat) {
                issued_bind_before = issued_bind_after = 0;
                wl_columnar_session_test_metadata_bind_phase =
                    issued_bind_phase;
                if (issued_bind_slot == 1)
                    wl_evaluation_control_test_source_metadata_cancel_at = 1;
                CHECK(wl_session_step(f.sessions[0]) == EAGAIN);
                wl_columnar_session_test_metadata_bind_phase = NULL;
                CHECK(issued_bind_before == (issued_bind_slot == 1 ? 0u : 1u)
                    && issued_bind_after == issued_bind_before
                    && !issued_bind_births
                    && !wl_columnar_session_test_metadata_bind_view(
                        (wl_col_session_t *)f.sessions[0], view)
                    && view[0] == 1 && view[1] == issued_bind_cursor +
                    (issued_bind_slot == 4 ? 1u : 0u)
                    && !view[3] && !view[4] && !view[5]);
                wl_evaluation_control_report_t report;
                CHECK(!wl_session_test_source_pending_reset_cancel(
                        f.sessions[0], &report)
                    && report.result == WL_EVALUATION_CONTROL_CANCELLED
                    && report.consumed == (issued_bind_slot == 4 ? 10u : 0u));
            }
            for (unsigned call = 0; view[0] == 1; ++call) {
                CHECK(call < 8 && wl_session_step(f.sessions[0]) == EAGAIN
                    && !issued_bind_births
                    && !wl_columnar_session_test_metadata_bind_view(
                        (wl_col_session_t *)f.sessions[0], view));
            }
            CHECK(view[0] == 2 && view[1] == 4 && !view[4] && !view[5]);
            wl_evaluation_control_test_finance_birth_claim = NULL;
            CHECK(!finish(f.sessions[0]) && !finish(f.sessions[1]));
            verify(f.sessions[0], &f.state); verify(f.sessions[1], &f.state);
            destroy(&f); metadata_bind_session = NULL;
            ++histories;
        }
    CHECK(histories == 19);
    puts("issued binding: 19 real initial/group/body refusal histories passed");
}

/* Preserve a real issued receipt while the BANK remains canceled. The next
 * ordinary admission must not reissue the already performed financial action. */
extern void (*wl_columnar_session_test_metadata_taken)(const wl_col_session_t *,
    unsigned, unsigned);
static const wl_session_source_metadata_recipe_t *head_recipe;
static unsigned head_takes, head_phase;

static void
metadata_head_taken(const wl_col_session_t *session, unsigned role,
    unsigned mask)
{
    CHECK(&session->base == head_recipe->session &&
        role == (unsigned)head_recipe->role
        && !mask && !session->base.source_financial_head
        && !head_recipe->result->ready && !head_recipe->result->scheduled
        && !head_recipe->result->semantic_valid && *head_recipe->busy
        && *head_recipe->phase == head_phase && !head_takes);
    ++head_takes;
}

static void
metadata_head(void)
{
    for (unsigned after_action = 0; after_action != 2; ++after_action) {
        fixture_t f; init(&f, true);
        wl_session_t *session = f.sessions[0];
        wl_columnar_memory_governor_ref_t *governor =
            ((wl_col_session_t *)session)->memory_governor;
        CHECK(governor);
        wl_columnar_memory_governor_ref_retain(governor);
        CHECK(session->source_c1_admission
            && session->backend->session_controlled_financial_head_complete);
        wl_columnar_eval_stack_test_fail_allocation = 0;
        const wl_backend_financial_offer_t *offer = NULL;
        wl_backend_pending_bind_t pending;
        for (unsigned call = 0; !offer; ++call) {
            int rc = wl_session_step(session);
            if (rc != EAGAIN) fprintf(stderr,
                    "metadata head: call=%u rc=%d head=%p state=%u\n", call, rc,
                    session->source_financial_head,
                    session->backend->session_controlled_step_state(session));
            CHECK(call < 100000 && rc == EAGAIN);
            offer = session->backend->session_controlled_financial_offer(
                session, &pending);
        }
        const wl_session_source_metadata_recipe_t *recipe = offer->recipe;
        wl_columnar_eval_stack_test_fail_allocation = -1;
        CHECK(recipe && recipe->root == session->source_financial_head
            && recipe->role == WL_SESSION_SOURCE_METADATA_MAINTENANCE
            && !recipe->result->ready && !recipe->result->scheduled
            && !*recipe->busy);
        unsigned before = *recipe->phase;
        CHECK(before == WL_COLUMNAR_SESSION_FINANCE_CLOSE_FINISH);
        uint64_t active = wl_evaluation_control_active_attempt_id(f.control);
        uint64_t used = 0;
        if (active) CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                active, &used));
        wl_evaluation_control_report_t original;
        int reported = wl_evaluation_control_report(f.control, &original);
        CHECK(!reported || reported == ENOENT);
        if (after_action)
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                true;
        else wl_evaluation_control_test_source_lexical_cancel_before_action =
                true;
        CHECK(wl_session_step(session) == EAGAIN);
        CHECK(recipe->result->ready && !recipe->result->scheduled
            && !*recipe->busy && recipe->result->semantic_valid
            && recipe->result->financial_result ==
            WL_EVALUATION_CONTROL_CANCELLED
            && recipe->result->outcome.action_called == (after_action != 0)
            && !recipe->result->outcome.action_result
            && recipe->result->before_phase == before
            && session->source_financial_head == recipe->root);
        unsigned saved_phase = *recipe->phase;
        CHECK(saved_phase == (after_action ?
            WL_COLUMNAR_SESSION_FINANCE_CLOSE_REPORT : before));
        head_recipe = recipe; head_phase = saved_phase; head_takes = 0;
        wl_columnar_session_test_metadata_taken = metadata_head_taken;
        CHECK(!session->backend->session_controlled_financial_offer(session,
            &pending) && !pending.root && !pending.refusal);
        for (unsigned retry = 0; retry != 3; ++retry) {
            CHECK(wl_session_step(session) == WL_EVALUATION_CONTROL_CANCELLED
                && recipe->result->ready && !recipe->result->scheduled
                && !*recipe->busy && *recipe->phase == saved_phase
                && !head_takes
                && session->source_financial_head == recipe->root
                && wl_evaluation_control_active_attempt_id(f.control) ==
                active);
            if (active) {
                uint64_t actual;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    active, &actual) && actual == used);
            }
            wl_evaluation_control_report_t actual;
            CHECK(wl_evaluation_control_report(f.control, &actual) == reported);
            if (!reported) native_report_same(&original, &actual);
        }
        wl_evaluation_control_report_t source;
        CHECK(!wl_session_test_source_pending_reset_cancel(session, &source)
            && source.result == WL_EVALUATION_CONTROL_CANCELLED);
        int resumed = wl_session_step(session);
        CHECK(resumed == EAGAIN
            && !recipe->result->ready && !recipe->result->scheduled
            && !recipe->result->semantic_valid && !*recipe->busy
            && *recipe->phase == saved_phase && head_takes == 1);
        wl_columnar_session_test_metadata_taken = NULL;
        head_recipe = NULL;
        CHECK(finish(session) == ENOMEM);
        state_t empty = {0};
        expect_events(&f.events[0], &empty, &empty);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!finish(session) && !finish(f.sessions[1]));
        verify(session, &f.state); verify(f.sessions[1], &f.state);
        expect_events(&f.events[0], &empty, &f.state);
        expect_events(&f.events[1], &empty, &f.state);
        state_t old = f.state;
        f.state.a[1] = false;
        for (unsigned i = 0; i != 2; ++i) {
            memset(&f.events[i], 0, sizeof(f.events[i]));
            CHECK(!wl_session_remove(f.sessions[i], "a", (int64_t[]){1, 0}, 1,
                2)
                && !finish(f.sessions[i]));
            verify(f.sessions[i], &f.state);
            expect_events(&f.events[i], &old, &f.state);
        }
        destroy(&f);
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)));
        wl_columnar_memory_governor_ref_release(governor);
    }
    puts(
        "metadata head: two genuine issued receipt/refusal/TAKE histories passed");
}
static void
metadata_binding(void)
{
    metadata_binding_issued();
    for (unsigned mode = 0; mode < 4; ++mode)
        for (unsigned stop = 1; stop <= 5; ++stop) {
            fixture_t f; init(&f, true);
            bool reset = mode != 0, snapshot_owned = mode >= 2;
            wl_evaluation_control_report_t prior = {0};
            if (reset) {
                CHECK(!finish(f.sessions[0]) && !finish(f.sessions[1]));
                CHECK(!wl_evaluation_control_report(f.control, &prior));
                for (unsigned i = 0; i < 2; ++i) {
                    CHECK(!wl_session_insert(f.sessions[i], "c",
                        (int64_t[]){7, 207}, 1, 2));
                    memset(&f.events[i], 0, sizeof(f.events[i]));
                }
                f.state.extra = true;
            }
            metadata_bind_session = f.sessions[0];
            metadata_bind_stop = stop; metadata_bind_seen = 0;
            metadata_bind_reset = reset;
            metadata_bind_snapshot = snapshot_owned;
            metadata_bind_fired = false; metadata_bind_generation = 0;
            metadata_bind_closing = false;
            wl_columnar_memory_governor_ref_t *governor =
                ((wl_col_session_t *)f.sessions[0])->memory_governor;
            wl_columnar_memory_governor_ref_retain(governor);
            wl_columnar_session_test_metadata_bind_phase = metadata_bind_phase;
            capture_t rows = {0};
            int rc;
            if (snapshot_owned)
                rc = wl_session_snapshot(f.sessions[0], snapshot, &rows);
            else {
                unsigned calls = 0;
                do {
                    CHECK(++calls < 1000000);
                    rc = wl_session_step(f.sessions[0]);
                } while (rc == EAGAIN && !metadata_bind_seen);
            }
            CHECK(rc == (snapshot_owned ? WL_EVALUATION_CONTROL_CANCELLED :
                EAGAIN)
                && metadata_bind_fired && metadata_bind_seen == 1);
            wl_columnar_session_test_metadata_bind_phase = NULL;
            if (reset || !snapshot_owned) {
                uint64_t view[10];
                CHECK(!wl_columnar_session_test_metadata_bind_view(
                        (wl_col_session_t *)f.sessions[0], view)
                    && view[0] == 1 && view[1] == (snapshot_owned ? stop - 1 :
                    stop < 5 ? stop : 4) && !view[3]);
                if (snapshot_owned) {
                    wl_evaluation_control_report_t report;
                    CHECK(!metadata_bind_generation
                        && !wl_evaluation_control_active_attempt_id(f.control)
                        && !wl_evaluation_control_report(f.control, &report)
                        && !view[4] && !view[5] && !view[6] && !view[7] &&
                        view[8]);
                    native_report_same(&prior, &report);
                    CHECK(f.sessions[0]->backend->session_controlled_step_state(
                            f.sessions[0]) ==
                        WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE);
                    CHECK(wl_session_step(f.sessions[0]) == EBUSY);
                } else {
                    CHECK(f.sessions[0]->backend->session_controlled_step_state(
                            f.sessions[0]) ==
                        WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
                    wl_session_t *same = f.sessions[0];
                    CHECK(wl_session_try_destroy(&same) == EBUSY
                        && same == f.sessions[0]);
                }
            }
            if (mode != 3) {
                if (snapshot_owned) {
                    memset(&rows, 0, sizeof(rows));
                    CHECK(!wl_session_snapshot(f.sessions[0], snapshot, &rows));
                    state_t empty = {0}; expect_events(&rows, &empty, &f.state);
                } else CHECK(!finish(f.sessions[0]));
                CHECK(!finish(f.sessions[1]));
                verify(f.sessions[0], &f.state);
                verify(f.sessions[1], &f.state);
            }
            /* Mode3 exercises genuine admitted CLOSE of partial Snapshot
             * binding, after its sole outer FINISH and payload relinquishment. */
            if (mode == 3) {
                metadata_bind_closing = true; metadata_bind_fired = false;
                metadata_bind_seen = 0;
                wl_columnar_session_test_metadata_bind_phase =
                    metadata_bind_phase;
                int closing = wl_session_try_destroy(&f.sessions[0]);
                CHECK(closing == EAGAIN && f.sessions[0]);
                for (unsigned retry = 0; closing == EAGAIN && retry < 8;
                    ++retry) {
                    CHECK(f.sessions[0]
                        && wl_session_step(f.sessions[0]) == EBUSY);
                    closing = wl_session_try_destroy(&f.sessions[0]);
                }
                CHECK(!closing && !f.sessions[0]);
            }
            destroy(&f);
            wl_columnar_session_test_metadata_bind_phase = NULL;
            if (mode == 3) CHECK(metadata_bind_fired &&
                    metadata_bind_seen == 1);
            CHECK(!wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(governor)));
            wl_columnar_memory_governor_ref_release(governor);
            metadata_bind_session = NULL;
        }
}

/* Intentional isolated refusal lifetime: its genuine private Normal counter
 * cannot be refilled. Globals retain the actual protected owners until exit. */
static wl_session_t *snapshot_overflow_session;
static wl_evaluation_control_t *snapshot_overflow_control;
static wirelog_program_t *snapshot_overflow_program;
static wl_plan_t *snapshot_overflow_plan;
static bool snapshot_overflow_seeded, snapshot_overflow_cancelled;

static void
snapshot_overflow_checkpoint(wl_evaluation_control_t *control, unsigned point,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)progress; (void)bindings;
    CHECK(control == snapshot_overflow_control);
    if (point == 49 && !snapshot_overflow_seeded) {
        wl_evaluation_control_test_finance_birth_attempt_id = UINT64_MAX - 1;
        snapshot_overflow_seeded = true;
    }
    if (point == 51 && !snapshot_overflow_cancelled) {
        uint64_t v[6];
        CHECK(!wl_columnar_session_test_producer_view(
                (wl_col_session_t *)snapshot_overflow_session, v));
        if (!v[2]) return; /* Stop only after a genuinely full initialized header. */
        wl_evaluation_control_request_cancel(control);
        snapshot_overflow_cancelled = true;
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    }
}

static void
snapshot_retained_generation_overflow(void)
{
    fixture_t f; init(&f, false);
    snapshot_overflow_session = f.sessions[0];
    snapshot_overflow_control = f.control;
    snapshot_overflow_program = f.program; snapshot_overflow_plan = f.plan;
    wl_columnar_session_test_observed_transaction_checkpoint =
        snapshot_overflow_checkpoint;
    capture_t rows = {0};
    CHECK(wl_session_snapshot(f.sessions[0], snapshot, &rows) == EOVERFLOW);
    CHECK(snapshot_overflow_seeded && snapshot_overflow_cancelled
        && !wl_evaluation_control_test_finance_birth_attempt_id);
    uint64_t snapshot_view[8], terminal[16];
    CHECK(!wl_columnar_session_test_snapshot_finish_view(
            (wl_col_session_t *)f.sessions[0], snapshot_view)
        && !snapshot_view[0] && !snapshot_view[2] && !snapshot_view[3]);
    CHECK(!wl_columnar_session_test_terminal_view(
            (wl_col_session_t *)f.sessions[0], terminal)
        && terminal[11] && terminal[10] && terminal[2]);
    wl_evaluation_control_report_t report, after;
    CHECK(!wl_evaluation_control_report(f.control, &report)
        && report.result == EOVERFLOW &&
        !wl_evaluation_control_active_attempt_id(f.control));
    CHECK(wl_session_step(f.sessions[0]) == EOVERFLOW);
    CHECK(!wl_evaluation_control_report(f.control, &after));
    native_report_same(&report, &after);
    CHECK(wl_session_try_destroy(&f.sessions[0]) == EBUSY
        && f.sessions[0] == snapshot_overflow_session);
    state_t empty = {0};
    expect_events(&rows, &empty, &empty);
    CHECK(!wl_session_try_destroy(&f.sessions[1]));
    /* The first session, caller control/program/plan references remain real.
     * No fake counter/report restoration or guarded Native free follows. */
    CHECK(snapshot_overflow_program && snapshot_overflow_plan);
}

/* Isolated process: exhausts global identities permanently after real fixture birth. */
static void
snapshot_finance_birth(void)
{
    fixture_t f; init(&f, true);
    state_t empty = {0};
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(f.control, &report) == ENOENT);
    CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX));
    trace();
    capture_t rows = {0};
    CHECK(wl_session_snapshot(f.sessions[0], snapshot, &rows) == EOVERFLOW);
    CHECK(!prepared && !produced && !acked && !attempt);
    CHECK(!wl_evaluation_control_active_attempt_id(f.control));
    CHECK(wl_evaluation_control_report(f.control, &report) == ENOENT);
    expect_events(&rows, &empty, &empty);
    expect_events(&f.events[0], &empty, &empty);
    for (unsigned h = 0; h < 2; ++h) {
        const col_rel_t *rel = session_find_rel(
            (wl_col_session_t *)f.sessions[0], h ? "output" : "mid");
        CHECK(!rel || !rel->nrows);
    }
    CHECK(!((wl_col_session_t *)f.sessions[0])->cleanup_active
        && !((wl_col_session_t *)f.sessions[0])->cleanup_pending);
    destroy(&f);
}

typedef struct {
    bool seen[MAX_STRATA][2];
    unsigned calls;
} admission_rows_t;

static void
admission_snapshot_row(const char *name, const int64_t *row, uint32_t width,
    void *userdata)
{
    if (!strcmp(name, "a")) return;
    CHECK(name[0] == 'r');
    char *end = NULL;
    unsigned long index = strtoul(name + 1, &end, 10);
    CHECK(end && !*end && index < MAX_STRATA && width == 1);
    CHECK(row[0] >= 0 && row[0] < 2);
    admission_rows_t *rows = userdata;
    CHECK(!rows->seen[index][row[0]]);
    rows->seen[index][row[0]] = true;
    ++rows->calls;
}

/* Negative dependencies force the compiler to retain all 32 real strata. */
static void
admission_strata(bool evaluating_snapshot, bool refuse_only)
{
    char source[8192];
    size_t used = 0;
    int written = snprintf(source, sizeof(source), ".decl a(x:int64)\n");
    CHECK(written > 0); used = (size_t)written;
    for (unsigned i = 0; i < MAX_STRATA; ++i) {
        written = snprintf(source + used, sizeof(source) - used,
                ".decl r%u(x:int64)\n", i);
        CHECK(written > 0 && (size_t)written < sizeof(source) - used);
        used += (size_t)written;
    }
    for (unsigned i = 0; i < MAX_STRATA; ++i) {
        written = i ? snprintf(source + used, sizeof(source) - used,
                "r%u(x):-a(x),!r%u(x).\n", i, i - 1)
            : snprintf(source + used, sizeof(source) - used, "r0(x):-a(x).\n");
        CHECK(written > 0 && (size_t)written < sizeof(source) - used);
        used += (size_t)written;
    }
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == MAX_STRATA);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options); options.evaluation_control = control;
    configure("WIRELOG_JOIN_BATCH_BYTES", "64");
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    wl_session_t *sessions[2] = {NULL, NULL};
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &sessions[0]));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &sessions[1]));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    CHECK(sessions[0]->source_c1_admission &&
        !sessions[1]->source_c1_admission);
    for (unsigned i = 0; i < 2; ++i)
        CHECK(!wl_session_insert(sessions[i], "a", (int64_t[]){0, 1}, 2, 1));
    if (refuse_only) {
        /* Invalid admission must not need another global controller identity.
         * No transaction or Native borrower exists on this fresh fixture. */
        CHECK(!wl_evaluation_control_test_seed_next_identity(UINT64_MAX));
        CHECK(wl_session_snapshot(sessions[0], NULL, NULL) == EINVAL);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_active_attempt_id(control));
        CHECK(wl_evaluation_control_report(control, &report) == ENOENT);
        CHECK(!((wl_col_session_t *)sessions[0])->controlled_step);
        for (unsigned i = 0; i < 2; ++i)
            CHECK(!wl_session_try_destroy(&sessions[i]));
        wl_evaluation_control_release(control);
        wl_plan_free(plan); wirelog_program_free(program);
        return;
    }
    admission_rows_t captured = {0};
    if (evaluating_snapshot)
        CHECK(!wl_session_snapshot(sessions[0], admission_snapshot_row,
            &captured));
    else CHECK(!finish(sessions[0]));
    CHECK(!finish(sessions[1]));
    for (unsigned i = 0; i < 2; ++i)
        for (unsigned j = 0; j < MAX_STRATA; ++j) {
            char name[16]; snprintf(name, sizeof(name), "r%u", j);
            const col_rel_t *rel =
                session_find_rel((wl_col_session_t *)sessions[i],
                    name);
            CHECK(rel && rel->ncols == 1 && rel->nrows == (j % 2 ? 0 : 2));
            bool seen[2] = {false, false};
            for (uint32_t k = 0; k < rel->nrows; ++k) {
                int64_t value = col_rel_get(rel, k, 0);
                CHECK(value >= 0 && value < 2 && !seen[value]);
                seen[value] = true;
            }
            CHECK(seen[0] == (j % 2 == 0) && seen[1] == (j % 2 == 0));
            if (evaluating_snapshot)
                CHECK(captured.seen[j][0] == seen[0]
                    && captured.seen[j][1] == seen[1]);
        }
    if (evaluating_snapshot) CHECK(captured.calls == MAX_STRATA);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(control, &before)
        && before.consumed && !before.result && !before.cleanup_status);
    CHECK(wl_session_snapshot(sessions[0], NULL, NULL) == EINVAL);
    CHECK(!wl_evaluation_control_active_attempt_id(control));
    CHECK(!wl_evaluation_control_report(control, &after));
    native_report_same(&before, &after);
    for (unsigned i = 0; i < 2;
        ++i) CHECK(!wl_session_try_destroy(&sessions[i]));
    CHECK(!wl_evaluation_control_report(control, &after));
    native_report_same(&before, &after);
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
}

static int
admission_count_read(void *payload)
{
    ++*(unsigned *)payload;
    return 0;
}

static void
admission_refuse_fresh_read(wl_col_session_t *session)
{
    CHECK(!session->controlled_step);
    ++fresh_shape_reached;
    wl_session_testhook_refuse_source_read(1);
}

static void
admission_read_failure(void)
{
    fixture_t f; init(&f, true);
    unsigned called = 0;
    /* No fake SOURCE scope: the real wrapper rejects this provenance. */
    CHECK(wl_session_source_admission_read(f.sessions[0],
        admission_count_read, &called) == EINVAL && !called);
    trace();
    fresh_shape_reached = 0;
    wl_columnar_session_test_before_fresh_shape = admission_refuse_fresh_read;
    CHECK(finish(f.sessions[0]) == EINVAL);
    wl_columnar_session_test_before_fresh_shape = NULL;
    wl_session_testhook_refuse_source_read(0);
    CHECK(fresh_shape_reached == 1 && !prepared && !produced && !acked);
    wl_col_session_t *session = (wl_col_session_t *)f.sessions[0];
    CHECK(!session->controlled_step && !session->cleanup_active
        && !session->cleanup_pending);
    CHECK(!wl_evaluation_control_active_attempt_id(f.control));
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(f.control, &report) == ENOENT);
    state_t empty = {0};
    expect_events(&f.events[0], &empty, &empty);
    for (unsigned h = 0; h < 2; ++h) {
        const col_rel_t *rel = session_find_rel(session, h ? "output" : "mid");
        CHECK(!rel || !rel->nrows);
    }
    step_pair(&f, &empty, true, 2);
    destroy(&f);
}

static void
private_output_cap(void)
{
    fixture_t f; init(&f, true);
    for (unsigned i = 0; i < 2; ++i)
        CHECK(!wl_session_insert(f.sessions[i], "c",
            (int64_t[]){7, 207}, 1, 2));
    f.state.extra = true;
    ((wl_col_session_t *)f.sessions[0])->join_output_limit = 32;
    trace();
    CHECK(finish(f.sessions[0]) == EOVERFLOW);
    CHECK(prepared == 2 && produced && acked && exchanges);
    state_t empty = {0};
    for (unsigned h = 0; h < 2; ++h) {
        const col_rel_t *rel = session_find_rel(
            (wl_col_session_t *)f.sessions[0], h ? "output" : "mid");
        CHECK(!rel || !rel->nrows);
    }
    expect_events(&f.events[0], &empty, &empty);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control, &report));
    CHECK(report.result == EOVERFLOW && report.execution_status == EOVERFLOW
        && !report.cleanup_status);
    CHECK(!wl_evaluation_control_active_attempt_id(f.control));
    ((wl_col_session_t *)f.sessions[0])->join_output_limit = 100;
    CHECK(!wl_evaluation_control_reset(f.control));
    trace();
    CHECK(!finish(f.sessions[0]));
    CHECK(prepared == 2 && produced && acked && exchanges);
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    CHECK(!finish(f.sessions[1]));
    for (unsigned i = 0; i < 2; ++i) {
        verify(f.sessions[i], &f.state);
        expect_events(&f.events[i], &empty, &f.state);
        const col_rel_t *mid = session_find_rel(
            (wl_col_session_t *)f.sessions[i], "mid");
        const col_rel_t *output = session_find_rel(
            (wl_col_session_t *)f.sessions[i], "output");
        CHECK(mid && mid->nrows == 28 && output && output->nrows == 35);
    }
    destroy(&f);
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    if (!strcmp(argv[1], "strict")) {
        strict_mode = true;
        for (unsigned observed = 0; observed < 2; ++observed) {
            fixture_t f; init(&f, observed);
            state_t empty = {0};
            step_pair(&f, &empty, observed, 2);
            CHECK(!((wl_col_session_t *)f.sessions[0])->arr_count);
            /* Internal prewarmed cache fixture on the prior public image.
             * A same-name private right input must not inherit this EBUSY. */
            uint32_t key = 1;
            col_arrangement_pin_t pin = {0};
            CHECK(!col_session_pin_arrangement(f.sessions[0], "mid", &key, 1,
                &pin));
            col_session_invalidate_arrangements(f.sessions[0], "mid");
            CHECK(pin.entry->rebuild_deferred);
            state_t old = f.state;
            for (unsigned i = 0; i < 2; ++i)
                CHECK(!wl_session_insert(f.sessions[i], "c",
                    (int64_t[]){7, 207}, 1, 2));
            f.state.extra = true;
            step_pair(&f, &old, observed, 2);
            CHECK(pin.active && pin.entry->rebuild_deferred);
            col_arrangement_pin_release(&pin);
            destroy(&f);
        }
        snapshots();
        recovery();
        funding();
        snapshot_recovery();
        configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    } else if (!strcmp(argv[1], "successors")) {
        successors(false); successors(true);
    } else if (!strcmp(argv[1], "snapshots")) snapshots();
    else if (!strcmp(argv[1], "recovery")) recovery();
    else if (!strcmp(argv[1], "funding")) funding();
    else if (!strcmp(argv[1], "snapshot-recovery")) snapshot_recovery();
    else if (!strcmp(argv[1], "appends")) appends();
    else if (!strcmp(argv[1], "native-end")) native_end();
    else if (!strcmp(argv[1], "bootstrap-end")) native_bootstrap_end();
    else if (!strcmp(argv[1], "snapshot-end")) native_snapshot_end();
    else if (!strcmp(argv[1], "constructor-faults")) constructor_faults();
    else if (!strcmp(argv[1],
        "snapshot-finish-refusal")) snapshot_finish_refusals();
    else if (!strcmp(argv[1], "metadata-binding")) metadata_binding();
    else if (!strcmp(argv[1], "metadata-head")) metadata_head();
    else if (!strcmp(argv[1], "snapshot-no-begin-prior"))
        snapshot_no_begin_prior();
    else if (!strcmp(argv[1], "snapshot-retained-overflow"))
        snapshot_retained_generation_overflow();
    else if (!strcmp(argv[1], "constructor-exhaust-step"))
        constructor_history(3, false, 0, false, false);
    else if (!strcmp(argv[1], "constructor-exhaust-snapshot"))
        constructor_history(3, true, 0, false, false);
    else if (!strcmp(argv[1],
        "snapshot-finance-birth")) snapshot_finance_birth();
    else if (!strcmp(argv[1], "snapshot-exhaust-evaluating-end")) {
        snapshot_identity_exhaustion = 0; native_snapshot_end();
    } else if (!strcmp(argv[1], "snapshot-exhaust-evaluating-successor")) {
        snapshot_identity_exhaustion = 1; native_snapshot_end();
    } else if (!strcmp(argv[1], "snapshot-exhaust-completed-end")) {
        snapshot_identity_exhaustion = 2; native_snapshot_end();
    } else if (!strcmp(argv[1], "snapshot-exhaust-completed-successor")) {
        snapshot_identity_exhaustion = 3; native_snapshot_end();
    } else if (!strcmp(argv[1], "admission-strata-step"))
        admission_strata(false, false);
    else if (!strcmp(argv[1], "admission-strata-snapshot"))
        admission_strata(true, false);
    else if (!strcmp(argv[1], "admission-strata-refusal"))
        admission_strata(false, true);
    else if (!strcmp(argv[1],
        "admission-read-failure")) admission_read_failure();
    else if (!strcmp(argv[1], "private-cap")) private_output_cap();
    else CHECK(false);
    return 0;
}
