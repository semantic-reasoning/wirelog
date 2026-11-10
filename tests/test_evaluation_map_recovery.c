/* #2299: generic hot scalar MAP failures retain public correction/retry. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/session.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); exit(1); } } while (0)
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session;
    wl_evaluation_control_t *control;
    unsigned events, positive, negative;
    int balance[2];
    bool snapshot_seen[2];
    unsigned snapshot_count;
} fixture_t;
static int complete(wl_session_t *session) {
    for (unsigned i = 0; i < 10000; ++i) {
        int rc = wl_session_step(session); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static void capture(const char *name, const int64_t *row, uint32_t columns,
    int32_t diff, void *arg) {
    fixture_t *f = arg;
    CHECK(!strcmp(name, "out") && columns == 2 && (diff == 1 || diff == -1));
    const char *text = wl_intern_reverse(f->plan->intern, row[0]); CHECK(text);
    unsigned index = !strcmp(text, "12tail")?0:1;
    CHECK(!strcmp(text, index?" -3 ":"12tail") && row[1] == (index?-3:12));
    f->balance[index] += diff;
    CHECK(f->balance[index] >= 0 && f->balance[index] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static fixture_t create(void) {
    fixture_t f = {0}; wirelog_error_t error;
    f.program =
        wirelog_parse_string(
        ".decl input(x:string)\n.decl out(x:string,n:int64)\n"
        "out(x,to_number(x)) :- input(x).\n", &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    CHECK(!wl_evaluation_control_create(0, &f.control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    return f;
}
static int64_t insert(fixture_t *f, const char *text) {
    int64_t id = wl_intern_put(f->plan->intern, text); CHECK(id >= 0);
    CHECK(!wl_session_insert(f->session, "input", &id, 1, 1)); return id;
}
static void contents(fixture_t *f, unsigned expected) {
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == expected && out->ncols == 2 &&
        out->column_types[0] == WIRELOG_TYPE_STRING &&
        out->column_types[1] == WIRELOG_TYPE_INT64);
    bool seen[2] = {false, false};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        const char *text = wl_intern_reverse(f->plan->intern,
                out->columns[0][r]); CHECK(text);
        unsigned i = !strcmp(text, "12tail")?0:1;
        CHECK(!seen[i] && !strcmp(text,
            i?" -3 ":"12tail") && out->columns[1][r] == (i?-3:12));
        seen[i] = true;
    }
}
static void destroy(fixture_t *f) {
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f->session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g);
    wl_session_destroy(f->session); wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
static unsigned conversions;
static void overflow(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (phase == 100)++conversions;
    if (c && phase == 101 && conversions == 2) {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c);
    }
}
static void same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b) {
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit &&
        a->consumed == b->consumed &&
        a->stop_reason == b->stop_reason &&
        a->execution_status == b->execution_status &&
        a->cleanup_status == b->cleanup_status && a->result == b->result);
}
static void snapshot(const char *name, const int64_t *row, uint32_t columns,
    void *arg) {
    fixture_t *f = arg; CHECK(!strcmp(name, "out") && columns == 2);
    const char *text = wl_intern_reverse(f->plan->intern, row[0]); CHECK(text);
    unsigned i = !strcmp(text, "12tail")?0:1;
    CHECK(!f->snapshot_seen[i] && !strcmp(text,
        i?" -3 ":"12tail") && row[1] == (i?-3:12));
    f->snapshot_seen[i] = true; ++f->snapshot_count;
}
static void terminal(void) {
    for (unsigned prior = 0; prior < 2; ++prior)for (unsigned error = 0;
            error < 3;
            ++error){
            fixture_t f = create();
            wl_session_set_delta_cb(f.session, capture, &f);
            int64_t good = prior?insert(&f, "12tail"):-1;
            CHECK(!complete(f.session)); contents(&f, prior);
            col_rel_t *committed = session_find_rel(COL_SESSION(f.session),
                    "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(committed);
            unsigned before = f.events;
            if (!prior) good = insert(&f, "12tail");
            int64_t other = insert(&f, " -3 ");
            int64_t bad = error == 1?-1:insert(&f, "9223372036854775808");
            if (error == 1) wl_columnar_expr_test_fail_allocation = 0;
            if (error == 2) {
                conversions = 0; wl_columnar_expr_test_checkpoint = overflow;
            }
            int expected = error == 0?ERANGE:error == 1?ENOMEM:EOVERFLOW;
            CHECK(complete(f.session) == expected);
            wl_columnar_expr_test_fail_allocation = -1;
            wl_columnar_expr_test_checkpoint = NULL;
            contents(&f, prior); CHECK(f.events == before);
            CHECK(session_find_rel(COL_SESSION(f.session),
                "out") == committed &&
                wl_columnar_relation_snapshot_equal(generation,
                wl_columnar_relation_snapshot(committed)));
            CHECK(!COL_SESSION(f.session)->cleanup_active &&
                !COL_SESSION(f.session)->cleanup_pending &&
                !COL_SESSION(f.session)->cleanup_reserved_bytes);
            wl_evaluation_control_report_t frozen, report;
            CHECK(!wl_evaluation_control_report(f.control,
                &frozen) && frozen.result == expected &&
                frozen.execution_status == expected);
            /* A retry without a correction must not pretend the old image is a
             * newly successful result. The permanently invalid input still fails. */
            if (error == 0) {
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(complete(f.session) == ERANGE); contents(&f, prior);
                CHECK(f.events == before);
                CHECK(!wl_evaluation_control_report(f.control,
                    &report) && report.result == ERANGE &&
                    report.attempt_id > frozen.attempt_id); frozen = report;
            }
            CHECK(wl_session_remove(f.session, "missing", &good, 1,
                1) == ENOTSUP);
            int64_t wrong_width[2] = {good, 0};
            CHECK(wl_session_remove(f.session, "input", wrong_width, 1,
                2) == ENOTSUP);
            CHECK(!wl_session_remove(f.session, "input", &good, 0, 1));
            CHECK(wl_session_snapshot(f.session, snapshot,
                &f) == EBUSY && !f.snapshot_count);
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&frozen, &report);
            if (prior && error == 0) {
                /* An unrelated live generation grants no correction or replay.
                 * Closing it does not erase the earlier authentic terminal receipt. */
                CHECK(!wl_evaluation_control_reset(f.control));
                wl_evaluation_control_attempt_begin_result_t next =
                    wl_evaluation_control_begin_attempt(
                    f.control, f.session->operation_admission);
                CHECK(!next.protocol && next.attempt_id);
                CHECK(wl_session_remove(f.session, "input", &bad, 1,
                    1) == EBUSY);
                CHECK(complete(f.session) == EBUSY);
                uint64_t used = 999;
                CHECK(wl_evaluation_control_active_attempt_id(f.control) ==
                    next.attempt_id &&
                    !wl_evaluation_control_test_active_consumed(f.control,
                    next.attempt_id, &used) && !used);
                CHECK(!wl_evaluation_control_report(f.control, &report));
                same_report(&frozen, &report);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, next.attempt_id,
                    0, 0);
                CHECK(!end.protocol && end.report_valid && !end.report.result &&
                    !end.report.consumed);
                frozen = end.report;
            }
            int rc;
            if (error == 1) {
                /* Successful remove/reinsert cancels logically, but both real
                 * generations remain authorized and the next image is rebuilt. */
                CHECK(!wl_session_remove(f.session, "input", &good, 1, 1));
                CHECK(!wl_session_insert(f.session, "input", &good, 1, 1));
                rc = 0;
            } else rc = wl_session_remove(f.session, "input", &bad, 1, 1);
            if (rc) fprintf(stderr,
                    "terminal prior=%u error=%u correction=%d\n", prior, error,
                    rc);
            CHECK(!rc);
            CHECK(!wl_evaluation_control_report(f.control, &report));
            same_report(&frozen, &report);
            CHECK(!wl_evaluation_control_reset(f.control));
            if (prior && error == 0) {
                CHECK(!wl_session_snapshot(f.session, snapshot, &f));
                CHECK(f.snapshot_count == 2 && f.snapshot_seen[0] &&
                    f.snapshot_seen[1]);
            } else CHECK(!complete(f.session));
            contents(&f, 2);
            CHECK(f.events == 2 && f.positive == 2 && !f.negative);
            CHECK(!complete(f.session) && f.events == 2);
            CHECK(!wl_session_remove(f.session, "input", &other, 1, 1));
            CHECK(!complete(f.session)); contents(&f, 1);
            CHECK(f.events == 3 && f.negative == 1 && f.balance[0] == 1 &&
                !f.balance[1]);
            CHECK(!complete(f.session) && f.events == 3);
            CHECK(!wl_session_remove(f.session, "input", &good, 1, 1));
            CHECK(!complete(f.session)); contents(&f, 0);
            CHECK(f.events == 4 && f.negative == 2 && !f.balance[0]);
            destroy(&f);
        }
}
int main(void) {
    terminal(); puts("hot scalar MAP recovery: all checks passed"); return 0;
}
