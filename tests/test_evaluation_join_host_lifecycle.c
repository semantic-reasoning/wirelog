/* Real private-head JOIN binding across STEP, SNAPSHOT and successors. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
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
    else CHECK(false);
    return 0;
}
