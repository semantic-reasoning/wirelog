/* Actual session JOIN producer, private sink and rollback qualification. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_session_t *session, *raw;
    unsigned width[2], rows[2], kind, events;
    int64_t data[2][8 * 129];
    bool seen[8][8];
} fixture_t;

static fixture_t *active;
static unsigned hits[11], target;
static bool stopped, work_stop;
static unsigned denial_event, denial_phase;
static bool sink_allocation_fault;
static wl_columnar_memory_reservation_t peer;
static uint64_t attempt, last_generation;

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false);
    return EIO;
}

static void
add(char *out, size_t cap, size_t *used, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    int n = vsnprintf(out + *used, cap - *used, format, args);
    va_end(args);
    CHECK(n >= 0 && (size_t)n < cap - *used);
    *used += (size_t)n;
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int diff,
    void *userdata)
{
    if (strcmp(name, "output")) return;
    fixture_t *f = userdata;
    CHECK(width == f->width[0] + f->width[1] && diff == 1);
    unsigned a = (unsigned)row[1], b = (unsigned)row[f->width[0] + 1];
    CHECK(a < 8 && b < 8 && a < b && !f->seen[a][b]);
    CHECK(row[0] == 1 && row[f->width[0]] == 1);
    for (unsigned c = 1; c < f->width[0]; ++c)
        CHECK(row[c] == (int64_t)(100 * (c - 1) + a));
    for (unsigned c = 1; c < f->width[1]; ++c)
        CHECK(row[f->width[0] + c] == (int64_t)(100 * (c - 1) + b));
    f->seen[a][b] = true;
    ++f->events;
}

static void
reserve_peer(void)
{
    wl_columnar_memory_governor_t *gov =
        wl_columnar_memory_governor_ref_get(active->governor);
    uint64_t available = (UINT64_C(512) << 20)
        - wl_columnar_memory_reserved(gov);
    CHECK(available > 1 && !peer.governor);
    CHECK(wl_columnar_memory_reserve(gov, available - 1, &peer));
}

static void
prepare_checkpoint(const wl_columnar_join_batch_prepare_t *a)
{
    if (a->phase == denial_phase && !peer.governor) reserve_peer();
}

static void
checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t producer_work)
{
    CHECK(active && control == active->control && event < 11);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id && (!attempt || attempt == id));
    attempt = id;
    ++hits[event];
    if (event == denial_event && !peer.governor) reserve_peer();
    if (sink_allocation_fault &&
        event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED)
        wl_columnar_relation_test_constructor_fail_allocation = 1;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) {
        CHECK(generation == last_generation + 1 && extent > 0
            && producer_work > 0);
        last_generation = generation;
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK)
        CHECK(generation && producer_work);
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCER_DONE
        && active->rows[0] && active->rows[1])
        CHECK(producer_work);
    if (event != target || stopped) return;
    stopped = true;
    if (work_stop) CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
observe(fixture_t *f)
{
    active = f; attempt = last_generation = 0;
    memset(hits, 0, sizeof(hits));
    wl_columnar_join_pipeline_test_numeric_checkpoint = checkpoint;
}

static void
init(fixture_t *f, unsigned kind, bool timestamps, unsigned budget_rows)
{
    memset(f, 0, sizeof(*f));
    f->kind = kind;
    f->width[0] = kind == 1 ? 128 : 2;
    f->width[1] = kind == 1 ? 129 : 2;
    f->rows[0] = kind == 4 ? 0 : 8;
    f->rows[1] = kind == 3 ? 0 : 8;
    char text[32768] = {0}; size_t used = 0;
    for (unsigned s = 0; s < 2; ++s) {
        add(text, sizeof(text), &used, ".decl %c(k:int64", 'a' + s);
        for (unsigned c = 1; c < f->width[s]; ++c)
            add(text, sizeof(text), &used, ",p%u:int64", c);
        add(text, sizeof(text), &used, ")\n");
        for (unsigned r = 0; r < 8; ++r)
            for (unsigned c = 0; c < f->width[s]; ++c)
                f->data[s][r * f->width[s] + c] = c
                    ? 100 * (c - 1) + r + (kind == 2 && !s ? 10 : 0) : 1;
    }
    add(text, sizeof(text), &used, ".decl output(");
    for (unsigned s = 0; s < 2; ++s)
        for (unsigned c = 0; c < f->width[s]; ++c)
            add(text, sizeof(text), &used, "%s%c%u:int64",
                s || c ? "," : "", 'a' + s, c);
    add(text, sizeof(text), &used, ")\noutput(");
    for (unsigned s = 0; s < 2; ++s)
        for (unsigned c = 0; c < f->width[s]; ++c)
            if (c) add(text, sizeof(text), &used, ",%c%u", 'a' + s, c);
            else add(text, sizeof(text), &used, "%sk", s ? "," : "");
    add(text, sizeof(text), &used, ") :- a(k");
    for (unsigned c = 1; c < f->width[0]; ++c)
        add(text, sizeof(text), &used, ",a%u", c);
    add(text, sizeof(text), &used, "), b(k");
    for (unsigned c = 1; c < f->width[1]; ++c)
        add(text, sizeof(text), &used, ",b%u", c);
    add(text, sizeof(text), &used, "), a1 < b1.\n");
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    const wl_plan_relation_t *head = &f->plan->strata[0].relations[0];
    CHECK(head->op_count == 5 && head->ops[1].op == WL_PLAN_OP_JOIN);
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->session));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->raw));
    uint64_t bytes = (f->width[0] + f->width[1]) * sizeof(int64_t)
        + (timestamps ? sizeof(col_delta_timestamp_t) : 0);
    ((wl_col_session_t *)f->session)->join_batch_bytes = budget_rows
        ? bytes * budget_rows : 1;
    ((wl_col_session_t *)f->raw)->join_batch_bytes = budget_rows
        ? bytes * budget_rows : 1;
    for (unsigned s = 0; s < 2; ++s) {
        char name[2] = {(char)('a' + s), 0};
        wl_session_t *sessions[] = {f->session, f->raw};
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(!wl_session_insert(sessions[i], name, f->data[s],
                f->rows[s] ? f->rows[s] : 1, f->width[s]));
            if (!f->rows[s]) CHECK(!wl_session_remove(sessions[i], name,
                    f->data[s], 1, f->width[s]));
            if (timestamps) {
                col_rel_t *rel =
                    session_find_rel((wl_col_session_t *)sessions[i], name);
                CHECK(!col_rel_enable_timestamps(rel));
                for (unsigned r = 0; r < rel->nrows; ++r)
                    rel->timestamps[r] = (col_delta_timestamp_t){
                        .iteration = 100 + r, .stratum = s, .worker = r,
                        ._reserved = 7, .multiplicity = (int64_t)r - 4
                    };
                wl_columnar_relation_touch_storage(rel);
            }
        }
    }
}

static void
destroy(fixture_t *f)
{
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL; active = NULL;
    CHECK(!wl_session_try_destroy(&f->session));
    CHECK(!wl_session_try_destroy(&f->raw));
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
    wl_evaluation_control_release(f->control);
}

static void
verify(fixture_t *f, wl_session_t *session)
{
    unsigned saved_events = f->events;
    bool saved[8][8]; memcpy(saved, f->seen, sizeof(saved));
    memset(f->seen, 0, sizeof(f->seen)); f->events = 0;
    col_rel_t *out = session_find_rel((wl_col_session_t *)session, "output");
    unsigned expected = f->kind >= 2 ? 0 : 28;
    CHECK(out && out->nrows == expected && !out->timestamps);
    int64_t row[257];
    for (unsigned r = 0; r < out->nrows; ++r) {
        for (unsigned c = 0; c < out->ncols; ++c) row[c] = out->columns[c][r];
        capture("output", row, out->ncols, 1, f);
    }
    CHECK(f->events == expected);
    for (unsigned a = 0; a < 8; ++a)
        for (unsigned b = 0; b < 8; ++b)
            CHECK(f->seen[a][b] == (expected && a < b));
    f->events = saved_events; memcpy(f->seen, saved, sizeof(saved));
    for (unsigned s = 0; s < 2; ++s) {
        char name[2] = {(char)('a' + s), 0};
        col_rel_t *input = session_find_rel((wl_col_session_t *)session, name);
        CHECK(input && input->nrows == f->rows[s]);
        for (unsigned r = 0; r < input->nrows; ++r) {
            for (unsigned c = 0; c < input->ncols; ++c)
                CHECK(input->columns[c][r] == f->data[s][r * input->ncols + c]);
            if (input->timestamps) {
                col_delta_timestamp_t stamp = input->timestamps[r];
                CHECK(stamp.iteration == 100 + r && stamp.stratum == s
                    && stamp.worker == r && stamp._reserved == 7
                    && stamp.multiplicity == (int64_t)r - 4);
            }
        }
    }
}

/* This first destination preserves the existing unobserved STEP admission.
* Observed/SNAPSHOT and strict/capped streaming are separate host units. */
static void
domains(void)
{
    for (unsigned kind = 0; kind < 5; ++kind)
        for (unsigned ts = 0; ts < 2; ++ts) {
            fixture_t f; init(&f, kind, ts, 2); observe(&f);
            CHECK(!finish(f.session));
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED] == 1
                && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCER_DONE] == 1);
            unsigned pairs = kind >= 3 ? 0 : 64;
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK] == pairs
                && hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] == pairs / 2);
            verify(&f, f.session);
            CHECK(!finish(f.raw)); verify(&f, f.raw);
            CHECK(!finish(f.session)); verify(&f, f.session);
            destroy(&f);
        }
    for (unsigned ts = 0; ts < 2; ++ts)
        for (unsigned rows = 0; rows < 2; ++rows) {
            fixture_t f; init(&f, 0, ts, rows); observe(&f);
            CHECK(!finish(f.session)); verify(&f, f.session);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED] == rows);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] == rows * 64);
            destroy(&f);
        }
}

static void
recovery(void)
{
    for (unsigned event = 1; event <= 10; ++event)
        for (unsigned work = 0; work < 2; ++work) {
            fixture_t f; init(&f, 0, true, 2); observe(&f);
            target = event; stopped = false; work_stop = work;
            if (work) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_C(1000000000)));
            int rc = finish(f.session);
            CHECK(stopped && rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
            col_rel_t *head = session_find_rel((wl_col_session_t *)f.session,
                    "output");
            CHECK(!head || !head->nrows);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report.result == rc && report.attempt_id == attempt);
            target = 0;
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_evaluation_control_configure(f.control, 0));
            observe(&f);
            CHECK(!finish(f.session)); verify(&f, f.session);
            CHECK(hits[WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK] == 32);
            destroy(&f);
        }
}

static void
failures(void)
{
    for (unsigned fail = 0; fail < 9; ++fail) {
        fixture_t f; init(&f, 0, true, 2); observe(&f);
        if (fail < 2) wl_columnar_join_test_attempt_fail_allocation = (int)fail;
        else wl_columnar_join_batch_test_prepare_fail_allocation = fail - 1;
        int rc = finish(f.session);
        if (rc != ENOMEM) fprintf(stderr, "failure=%u rc=%d bootstrap=%u\n",
                fail, rc, hits[9]);
        CHECK(rc == ENOMEM && !f.events);
        wl_columnar_join_test_attempt_fail_allocation = -1;
        wl_columnar_join_batch_test_prepare_fail_allocation = 0;
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!wl_evaluation_control_configure(f.control, 0));
        observe(&f);
        CHECK(!finish(f.session)); verify(&f, f.session);
        CHECK(!f.events);
        destroy(&f);
    }
    for (unsigned fault = 0; fault < 8; ++fault) {
        fixture_t f; init(&f, 0, true, 2); observe(&f);
        wl_columnar_memory_reservation_init(&peer);
        sink_allocation_fault = fault == 0;
        denial_event = fault == 1 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_BOOTSTRAP
            : fault == 2 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_KEYS
            : fault == 3 ? WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK : 0;
        static const unsigned phases[] = {1, 7, 11, 12};
        denial_phase = fault >= 4 ? phases[fault - 4] : 0;
        wl_columnar_join_batch_test_prepare_checkpoint = denial_phase
            ? prepare_checkpoint : NULL;
        int rc = finish(f.session);
        CHECK(rc == (sink_allocation_fault ? ENOMEM : ENOSPC));
        col_rel_t *head = session_find_rel((wl_col_session_t *)f.session,
                "output");
        CHECK(!head || !head->nrows);
        if (!sink_allocation_fault) {
            CHECK(peer.governor && peer.bytes
                && ((wl_col_session_t *)f.session)->memory_budget_denied);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(f.governor)) >=
                peer.bytes);
            CHECK(wl_columnar_memory_rollback(&peer));
        }
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_join_batch_test_prepare_checkpoint = NULL;
        sink_allocation_fault = false; denial_event = denial_phase = 0;
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!wl_evaluation_control_configure(f.control, 0));
        observe(&f);
        CHECK(!finish(f.session)); verify(&f, f.session);
        destroy(&f);
    }
}

int
main(int argc, char **argv)
{
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "domains"))) domains();
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "recovery"))) recovery();
    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "failures"))) failures();
    if (argc > 2 || (argc == 2 && strcmp(argv[1], "domains")
        && strcmp(argv[1], "recovery") && strcmp(argv[1],
        "failures"))) return 2;
    return 0;
}
