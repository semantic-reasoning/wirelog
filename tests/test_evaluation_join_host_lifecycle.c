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

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(session);
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

static void
native_bootstrap_end(void)
{
    for (unsigned mode = 0; mode < 3; ++mode)
        for (unsigned successor = 0; successor < 2; ++successor) {
            fixture_t f; init(&f, mode == 1); trace();
            native_fixture = &f; native_pending = native_ended = false;
            native_successor = successor; native_successor_id = 0;
            wl_columnar_eval_stack_test_fail_allocation = 0;
            wl_columnar_session_test_framed_grant_finished =
                native_bootstrap_finished;
            capture_t rows = {0};
            int rc = mode == 2 ? wl_session_snapshot(f.sessions[0], snapshot,
                    &rows)
            : finish(f.sessions[0]);
            wl_columnar_eval_stack_test_fail_allocation = -1;
            wl_columnar_session_test_framed_grant_finished = NULL;
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
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    native_successor_id, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.sessions[0]->operation_admission, native_successor_id,
                        0, 0);
                CHECK(!end.protocol && end.report_valid && !end.report.result
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
    else CHECK(false);
    return 0;
}
