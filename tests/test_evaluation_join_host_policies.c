/* Actual configured session JOIN output policy and private sink recovery. */
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

static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}

static unsigned prepared, acked, produced, sunk, exchanged;
static uint64_t generation, attempt;
static unsigned stop_event;
static bool stopped;
static void
checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t gen, uint32_t extent, uint64_t work)
{
    (void)extent; (void)work;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id && (!attempt || attempt == id));
    attempt = id;
    if (!stopped && stop_event && event == stop_event) {
        stopped = true;
        wl_evaluation_control_request_cancel(control);
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED) ++prepared;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE) ++produced;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK) ++sunk;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED) ++exchanged;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) {
        CHECK(gen == generation + 1);
        generation = gen; ++acked;
    }
}

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
check_rows(wl_session_t *session, bool filtered, bool reject)
{
    const col_rel_t *out = session_find_rel((wl_col_session_t *)session,
            "output");
    unsigned count = reject ? 0 : filtered ? 28 : 64;
    CHECK(out && out->nrows == count && out->ncols == 2);
    bool seen[8][8] = {{false}};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t x = col_rel_get(out, r, 0), y = col_rel_get(out, r, 1);
        CHECK(x >= 0 && x < 8 && y >= 0 && y < 8
            && !seen[x][y] && (!filtered || x < y));
        seen[x][y] = true;
    }
    for (unsigned x = 0; x < 8; ++x)
        for (unsigned y = 0; y < 8; ++y)
            CHECK(seen[x][y] == (!reject && (!filtered || x < y)));
}

typedef enum { DIRECT, FILTERED, FILTERED_EMPTY, MATERIALIZED_EMPTY } shape_t;

static void
run_cap(shape_t shape, unsigned limit, unsigned stop)
{
    bool filtered = shape != DIRECT, reject = shape >= FILTERED_EMPTY;
    bool materialized = shape == MATERIALIZED_EMPTY;
    char source[256], cap[32];
    snprintf(source, sizeof(source), ".decl a(k:int64,x:int64)\n"
        ".decl b(k:int64,y:int64)\n.decl output(x:int64,y:int64)\n"
        "output(x,y):-a(k,x),b(k,y)%s.\n",
        materialized ? ",x>y+100" : filtered ? ",x<y" : "");
    snprintf(cap, sizeof(cap), "%u", limit);
    configure("WIRELOG_JOIN_BATCH_BYTES", "64");
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", cap);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *head = &plan->strata[0].relations[0];
    CHECK(head->ops[1].op == WL_PLAN_OP_JOIN);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    int64_t input[16];
    for (unsigned i = 0; i < 8; ++i) {
        input[2 * i] = 1; input[2 * i + 1] = i;
    }
    wl_session_t *sessions[] = {session, raw};
    for (unsigned i = 0; i < 2; ++i) {
        if (reject)
            for (unsigned row = 0; row < 8; ++row) input[2 * row + 1] += 16;
        CHECK(!wl_session_insert(sessions[i], "a", input, 8, 2));
        if (reject)
            for (unsigned row = 0; row < 8; ++row) input[2 * row + 1] -= 16;
        CHECK(!wl_session_insert(sessions[i], "b", input, 8, 2));
    }
    prepared = acked = produced = sunk = exchanged = 0;
    generation = attempt = 0;
    stop_event = stop; stopped = false;
    wl_columnar_join_pipeline_test_numeric_checkpoint = checkpoint;
    int rc = finish(session);
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    unsigned count = reject ? 0 : filtered ? 28 : 64;
    unsigned policy_rows = materialized ? 64 : count;
    int raw_expected = limit && policy_rows >= limit ? EOVERFLOW : 0;
    bool before = stop == WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_ACK;
    int expected = before ? WL_EVALUATION_CONTROL_CANCELLED : raw_expected;
    int raw_rc = finish(raw);
    if (rc != expected || raw_rc != raw_expected)
        fprintf(stderr,
            "filtered=%u reject=%u limit=%u stop=%u controlled=%d/%d raw=%d/%d prepared=%u acked=%u\n",
            filtered, reject, limit, stop, rc, expected, raw_rc, raw_expected,
            prepared, acked);
    CHECK(rc == expected && raw_rc == raw_expected);
    CHECK(stopped == (stop != 0));
    CHECK(prepared == 1 && produced > 0);
    if (before) CHECK(!acked && !generation);
    else {
        CHECK(acked > 0);
        if (!expected || limit > 4) CHECK(produced > 1 && acked > 1);
    }
    CHECK(policy_rows ? sunk > 0 : !sunk);
    CHECK(filtered && !materialized ? exchanged > 0 : !exchanged);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.attempt_id == attempt && report.result == expected
        && report.consumed && !report.cleanup_status);
    if (before) CHECK(!((wl_col_session_t *)session)->join_output_peak);
    else CHECK(((wl_col_session_t *)session)->join_output_peak
            == ((wl_col_session_t *)raw)->join_output_peak);
    if (!expected) check_rows(session, filtered, reject);
    else {
        const col_rel_t *out = session_find_rel((wl_col_session_t *)session,
                "output");
        CHECK(!out || !out->nrows);
        /* Reuse the real stopped session, retaining the configured producer.
         * This internal knob has no live setter; no plan/cursor is fabricated. */
        ((wl_col_session_t *)session)->join_output_limit = policy_rows + 1;
        CHECK(!wl_evaluation_control_reset(control));
        CHECK(!finish(session));
        check_rows(session, filtered, reject);
    }
    CHECK(!wl_session_try_destroy(&session));
    CHECK(!wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
}

int
main(void)
{
    for (unsigned filtered = 0; filtered < 2; ++filtered) {
        unsigned count = filtered ? 28 : 64;
        shape_t shape = filtered ? FILTERED : DIRECT;
        run_cap(shape, 0, 0);
        run_cap(shape, 1, 0);
        run_cap(shape, count - 1, 0);
        run_cap(shape, count, 0);
        run_cap(shape, count + 1, 0);
    }
    run_cap(FILTERED_EMPTY, 1, 0);
    run_cap(MATERIALIZED_EMPTY, 1, 0);
    run_cap(DIRECT, 1, WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_ACK);
    run_cap(DIRECT, 1, WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK);
    return 0;
}
