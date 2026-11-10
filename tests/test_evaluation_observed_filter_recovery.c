/* #2299 one-column FILTER error/cancellation and genuine observed rollback. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do {if (!(x)) {fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                    __LINE__,#x); exit(1);}} while (0)
extern int wl_columnar_expr_test_fail_allocation;
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
typedef struct {wirelog_program_t *program; wl_plan_t *plan;
                wl_session_t *session;
                wl_evaluation_control_t *control;
                unsigned kind, events, positive, negative;
                int balance[4]; unsigned snapshot_mask, snapshot_count;
} fixture_t;
static int complete(wl_session_t *s){
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = wl_session_step(s); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static bool accepts(const fixture_t *f, int64_t x){
    return f->kind == 1?x >= 2:f->kind != 3;
}
static void row_check(const fixture_t *f, const char *name, const int64_t *row,
    uint32_t width){
    CHECK(!strcmp(name,
        "out") && width == 1 && row[0] >= 1 && row[0] <= 3 && accepts(f,
        row[0]));
}
static void capture(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data){
    fixture_t *f = data; row_check(f, name, row, width);
    CHECK(diff == 1 || diff == -1);
    f->balance[row[0]] += diff;
    CHECK(f->balance[row[0]] >= 0 && f->balance[row[0]] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static void snapshot(const char *name, const int64_t *row, uint32_t width,
    void *data){
    fixture_t *f = data; row_check(f, name, row, width);
    unsigned bit = 1u<<(unsigned)row[0];
    CHECK(!(f->snapshot_mask&bit)); f->snapshot_mask |= bit;
    ++f->snapshot_count;
}
static void contents(fixture_t *f, unsigned mask, unsigned count){
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == count && out->ncols == 1 && out->column_types &&
        out->column_types[0] == WIRELOG_TYPE_INT64);
    unsigned seen = 0; for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t x = out->columns[0][r];
        CHECK(x >= 1 && x <= 3 && accepts(f, x));
        unsigned bit = 1u<<(unsigned)x; CHECK(!(seen&bit)); seen |= bit;
    }
    CHECK(seen == mask);
}
static fixture_t create(unsigned kind, bool controlled){
    fixture_t f = {.kind = kind};
    const char *predicate = kind == 0?",strlen(to_string(x)) > 0":kind ==
        1?",x >= 2":kind == 3?",to_number(\"bad\") > x":"";
    char source[512]; snprintf(source, sizeof(source),
        ".decl input(x:int64)\n.decl out(x:int64)\nout(x) :- input(x)%s.\n",
        predicate);
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    int plan_rc = f.program?wl_plan_from_program(f.program, &f.plan):-99;
    if (!f.program || plan_rc)fprintf(stderr,
            "create kind%u controlled%d parse%d plan%d source%s\n", kind,
            controlled, error, plan_rc, source);
    CHECK(f.program && !plan_rc);
    const wl_plan_relation_t *r = &f.plan->strata[0].relations[0];
    CHECK(f.plan->stratum_count == 1 && f.plan->strata[0].relation_count == 1 &&
        r->op_count == (kind == 2?3u:4u) &&
        r->ops[0].op == WL_PLAN_OP_VARIABLE &&
        (kind == 2 || r->ops[1].op == WL_PLAN_OP_FILTER) &&
        r->ops[r->op_count-2].op == WL_PLAN_OP_MAP &&
        r->ops[r->op_count-2].project_count == 1 &&
        r->ops[r->op_count-2].project_indices[0] == 0 &&
        r->ops[r->op_count-1].op == WL_PLAN_OP_CONSOLIDATE);
    wl_session_options_t options; wl_session_options_init(&options);
    const wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = UINT64_C(64)<<20,
            .usable_bytes =
            UINT64_C(64)<<20, .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source =
            WL_COLUMNAR_MEMORY_SOURCE_ENV, .status = WL_COLUMNAR_MEMORY_OK};
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor); options.memory_governor = governor;
    if (controlled) {
        CHECK(!wl_evaluation_control_create(0, &f.control));
        options.evaluation_control = f.control;
    }
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    wl_columnar_memory_governor_ref_release(governor);
    return f;
}
static void close_fixture(fixture_t *f){
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f->session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g); wl_session_destroy(f->session);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    if (f->control)wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b){
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit &&
        a->consumed == b->consumed &&
        a->result == b->result && a->execution_status == b->execution_status &&
        a->cleanup_status == b->cleanup_status &&
        a->stop_reason == b->stop_reason);
}
static void closed(fixture_t *f){
    CHECK(!COL_SESSION(f->session)->delta_observer &&
        !COL_SESSION(f->session)->delta_publish_active &&
        !COL_SESSION(f->session)->cleanup_active &&
        !COL_SESSION(f->session)->cleanup_pending &&
        !COL_SESSION(f->session)->cleanup_reserved_bytes);
}
static void positives(void){
    for (unsigned controlled = 0; controlled < 2;
        ++controlled) for (unsigned kind = 0;
            kind < 4; ++kind){
            fixture_t f = create(kind, controlled);
            wl_session_set_delta_cb(f.session, capture, &f);
            CHECK(!complete(f.session)); contents(&f, 0, 0);
            const int64_t rows[] = {1, 2, 3}; int inserted = 0;
            if (controlled && kind == 2) {
                /* Preserve the legacy narrow identity admission: only individual
                 * append operations are supported, not this three-row batch. */
                wl_evaluation_control_report_t before, after;
                CHECK(!wl_evaluation_control_report(f.control, &before));
                CHECK(wl_session_insert(f.session, "input", rows, 3,
                    1) == ENOTSUP);
                CHECK(!wl_evaluation_control_report(f.control, &after));
                same_report(&before, &after);
                contents(&f, 0, 0); CHECK(!f.events);
                for (unsigned i = 0; i < 3;
                    ++i) CHECK(!wl_session_insert(f.session, "input", &rows[i],
                        1, 1));
            }else inserted = wl_session_insert(f.session, "input", rows, 3, 1);
            int evaluated = inserted?inserted:complete(f.session);
            if (inserted || evaluated) fprintf(stderr,
                    "positive kind%u controlled%u insert%d step%d observer%p\n",
                    kind, controlled, inserted, evaluated,
                    (void *)COL_SESSION(f.session)->delta_observer);
            CHECK(!inserted && !evaluated);
            unsigned mask = kind == 1?12u:kind == 3?0u:14u,
                count = kind == 1?2u:kind == 3?0u:3u;
            contents(&f, mask, count);
            CHECK(f.events == count && f.positive == count && !f.negative);
            f.snapshot_mask = f.snapshot_count = 0;
            CHECK(!wl_session_snapshot(f.session, snapshot, &f));
            CHECK(f.snapshot_mask == mask && f.snapshot_count == count &&
                f.events == count);
            int64_t two = 2;
            CHECK(!wl_session_remove(f.session, "input", &two, 1,
                1) && !complete(f.session));
            contents(&f, mask&~4u, count-(kind != 3));
            CHECK(f.events == count+(kind != 3) && f.negative == (kind != 3));
            CHECK(!wl_session_insert(f.session, "input", &two, 1,
                1) && !complete(f.session));
            contents(&f, mask, count); CHECK(f.events == count+2*(kind != 3) &&
                !complete(f.session) && f.events == count+2*(kind != 3));
            close_fixture(&f);
        }
}
static void allocation_errors(void){
    for (unsigned prior = 0; prior < 2; ++prior)for (unsigned query = 0;
            query < 2;
            ++query){
            fixture_t f = create(0, true);
            wl_session_set_delta_cb(f.session, capture, &f);
            int64_t one = 1, two = 2, three = 3;
            if (prior) CHECK(!wl_session_insert(f.session, "input", &one, 1,
                    1));
            CHECK(!complete(f.session)); contents(&f, prior?2u:0u, prior);
            if (query) {
                f.snapshot_mask = f.snapshot_count = 0;
                CHECK(!wl_session_snapshot(f.session, snapshot, &f));
                CHECK(f.snapshot_count == prior);
            }
            col_rel_t *committed = session_find_rel(COL_SESSION(f.session),
                    "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(committed);
            if (!prior) CHECK(!wl_session_insert(f.session, "input", &one, 1,
                    1));
            CHECK(!wl_session_insert(f.session, "input", &two, 1,
                1) && !wl_session_insert(f.session, "input", &three, 1, 1));
            for (unsigned retry = 0; retry < 2; ++retry) {
                wl_columnar_expr_test_fail_allocation = 0; /* Actual seam is one-shot. */
                int rc = complete(f.session);
                if (rc != ENOMEM) fprintf(stderr,
                        "allocation prior%u snapshot%u retry%u rc%d\n", prior,
                        query, retry, rc);
                CHECK(rc == ENOMEM); closed(&f);
                contents(&f, prior?2u:0u, prior);
                CHECK(f.events == prior &&
                    session_find_rel(COL_SESSION(f.session),
                    "out") == committed &&
                    wl_columnar_relation_snapshot_equal(generation,
                    wl_columnar_relation_snapshot(committed)));
                if (!retry) CHECK(!wl_evaluation_control_reset(f.control));
            }
            wl_columnar_expr_test_fail_allocation = -1;
            wl_evaluation_control_report_t before, after;
            CHECK(!wl_evaluation_control_report(f.control,
                &before) && before.result == ENOMEM && !before.cleanup_status);
            CHECK(!wl_session_insert(f.session, "input", &one, 0, 1));
            CHECK(wl_session_insert(f.session, "unknown", &one, 1,
                1) == ENOTSUP);
            int64_t wrong_width[] = {1, 2};
            CHECK(wl_session_insert(f.session, "input", wrong_width, 1,
                2) == ENOTSUP);
            CHECK(!wl_evaluation_control_report(f.control, &after));
            same_report(&before, &after);
            contents(&f, prior?2u:0u, prior); CHECK(f.events == prior);
            CHECK(!wl_session_remove(f.session, "input", &two, 1, 1));
            CHECK(!wl_evaluation_control_report(f.control, &after));
            same_report(&before, &after);
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !complete(f.session)); contents(&f, 10, 2);
            CHECK(f.events == 2 && f.positive == 2 && !f.negative);
            CHECK(!wl_session_insert(f.session, "input", &two, 1,
                1) && !complete(f.session)); contents(&f, 14, 3);
            CHECK(f.events == 3);
            CHECK(!wl_session_remove(f.session, "input", &three, 1,
                1) && !complete(f.session)); contents(&f, 6, 2);
            CHECK(f.events == 4 && f.negative == 1);
            CHECK(!wl_session_insert(f.session, "input", &three, 1,
                1) && !complete(f.session)); contents(&f, 14, 3);
            CHECK(f.events == 5 && f.positive == 4 && !complete(f.session) &&
                f.events == 5); close_fixture(&f);
        }
}
static fixture_t *active; static unsigned action, hits; static bool stopped;
static uint64_t successor;
static wl_evaluation_control_report_t original;
static void interrupt_original(wl_evaluation_control_t *control){
    stopped = true;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    if (action == 0)wl_evaluation_control_request_cancel(control);
    else if (action == 1)CHECK(wl_evaluation_control_charge(control,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else if (action == 4) {
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(control);
    }else {
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(control,
                active->session->operation_admission, id, 0, 0);
        CHECK(!end.protocol && end.report_valid && !end.report.result);
        original = end.report;
        if (action == 3) {
            CHECK(!wl_evaluation_control_reset(control));
            wl_evaluation_control_attempt_begin_result_t next =
                wl_evaluation_control_begin_attempt(control,
                    active->session->operation_admission);
            CHECK(!next.protocol && next.attempt_id != id);
            successor = next.attempt_id;
        }
    }
}
static void stop(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed){
    (void)failed;
    if (stopped || phase != 94 || progress != 1 || ++hits < 2) return;
    unsigned owners =
        wl_columnar_session_test_cleanup_owners(COL_SESSION(active->session));
    if (control != active->control || owners != 2) {
        wl_columnar_eval_serial_framed_attempt_t *view = NULL;
        int viewed =
            wl_columnar_eval_stack_framed_context(COL_SESSION(
                    active->session)->cleanup_active,
                COL_SESSION(active->session), &view);
        fprintf(stderr,
            "interrupt action%u samecontrol%d owners%u viewed%d ops%u capture%d\n",
            action, control == active->control, owners, viewed,
            view && view->plan?view->plan->op_count:0,
            view?view->capture_only:0);
        if (!viewed && view && view->plan)for (uint32_t i = 0;
                i < view->plan->op_count;
                ++i) fprintf(stderr, " %u", view->plan->ops[i].op);
        fputc('\n', stderr);
    }
    CHECK(control == active->control && owners == 2);
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(COL_SESSION(
            active->session)->cleanup_active, COL_SESSION(active->session),
        &ctx) &&
        !wl_columnar_eval_stack_framed_authenticate(ctx->frame,
        COL_SESSION(active->session), ctx) &&
        ctx->capture_only && ctx->private_bindings &&
        ctx->unary_native_cleanup);
    interrupt_original(control);
}

static void interruptions(void){
    for (unsigned query = 0; query < 2;
        ++query) for (action = 0; action < 5; ++action){
            fixture_t f = create(0, true); active = &f;
            wl_session_set_delta_cb(f.session, capture, &f);
            int64_t one = 1, other[] = {2, 3};
            CHECK(!wl_session_insert(f.session, "input", &one, 1,
                1) && !complete(f.session));
            if (query) {
                f.snapshot_mask = f.snapshot_count = 0;
                CHECK(!wl_session_snapshot(f.session, snapshot, &f));
                CHECK(f.snapshot_mask == 2);
            }
            col_rel_t *prior = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(prior);
            CHECK(!wl_session_insert(f.session, "input", other, 2, 1));
            stopped = false; hits = 0; successor = 0;
            if (action == 1) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_MAX-1));
            wl_columnar_expr_test_checkpoint = stop;
            int rc = complete(f.session);
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(stopped &&
                rc ==
                (action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                4?EOVERFLOW:EINVAL));
            closed(&f); contents(&f, 2, 1);
            CHECK(f.events == 1 && session_find_rel(COL_SESSION(f.session),
                "out") == prior &&
                wl_columnar_relation_snapshot_equal(generation,
                wl_columnar_relation_snapshot(prior)));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            if (action == 2 || action == 3) same_report(&original, &report);
            else CHECK(report.result == rc && !report.cleanup_status);
            if (successor) {
                uint64_t used = 99;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    successor, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, successor, 0, 0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control,
                0) && !complete(f.session));
            contents(&f, 14, 3);
            CHECK(f.events == 3 && f.positive == 3 && !f.negative &&
                !complete(f.session) && f.events == 3);
            close_fixture(&f); active = NULL;
        }
}
static unsigned audit_target, audit_sources, audit_copy, audit_numeric,
    audit_plan, numeric_runtime;
static bool audit_running;
static void audit_phase(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings){
    (void)control; (void)index; (void)bindings;
    audit_sources += phase == 43; audit_copy += phase == 26;
    audit_plan += phase == 27;
}
static void audit_pause(wl_evaluation_control_t *control, unsigned phase,
    bool scalar){
    if (phase == 0)audit_running = true;
    if (phase == 1 && !scalar)++audit_numeric;
    if (!stopped && audit_target < 2 && audit_target == phase) {
        CHECK(active && control == active->control && audit_sources &&
            !audit_copy && !audit_plan &&
            !COL_SESSION(active->session)->cleanup_active &&
            !COL_SESSION(active->session)->cleanup_pending);
        interrupt_original(control);
    }
    if (phase == 1)audit_running = false;
}
static void audit_child_pause(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed){
    (void)progress; (void)failed;
    if (!stopped && audit_running && audit_target >= 2 &&
        phase == 550+audit_target-2){
        CHECK(audit_sources && !audit_copy && !audit_plan &&
            !COL_SESSION(active->session)->cleanup_active &&
            !COL_SESSION(active->session)->cleanup_pending);
        interrupt_original(control);
    }
}
static void audit_interruptions(void){
    for (audit_target = 0; audit_target < 8; ++audit_target)for (action =
            0; action < 5;
            ++action){
            fixture_t f = create(0, true); active = &f;
            wl_session_set_delta_cb(f.session, capture, &f);
            int64_t one = 1, other[] = {2, 3};
            CHECK(!wl_session_insert(f.session, "input", &one, 1,
                1) && !complete(f.session));
            col_rel_t *prior = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(prior);
            CHECK(!wl_session_insert(f.session, "input", other, 2, 1));
            stopped = false; successor = 0; audit_running = false;
            audit_sources = audit_copy = audit_plan = audit_numeric = 0;
            if (action == 1) CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_MAX-1));
            wl_columnar_session_test_filter_append_checkpoint = audit_pause;
            wl_columnar_session_test_observed_transaction_checkpoint =
                audit_phase;
            wl_columnar_expr_test_checkpoint = audit_child_pause;
            int rc = complete(f.session);
            wl_columnar_session_test_filter_append_checkpoint = NULL;
            wl_columnar_session_test_observed_transaction_checkpoint = NULL;
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(stopped && audit_sources && !audit_copy && !audit_plan &&
                rc ==
                (action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                4?EOVERFLOW:EINVAL));
            closed(&f); contents(&f, 2, 1);
            CHECK(f.events == 1 && session_find_rel(COL_SESSION(f.session),
                "out") == prior &&
                wl_columnar_relation_snapshot_equal(generation,
                wl_columnar_relation_snapshot(prior)));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            if (action == 2 || action == 3) same_report(&original, &report);
            else CHECK(report.result == rc && !report.cleanup_status);
            if (successor) {
                uint64_t used = 99;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    successor, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, successor, 0, 0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control,
                0) && !complete(f.session));
            contents(&f, 14, 3);
            CHECK(f.events == 3 && !complete(f.session) && f.events == 3);
            close_fixture(&f); active = NULL;
        }
}
static void numeric_runtime_shape(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action_kind,
    wl_evaluation_control_t *grant, int protocol){
    (void)action_kind; (void)grant; (void)protocol;
    if (!session->cleanup_active)return;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    if (wl_columnar_eval_stack_framed_context(session->cleanup_active,
        (wl_col_session_t *)session,
        &ctx) || !ctx || ctx->identity != ctx) return;
    if (ctx->plan->op_count == 6) {
        CHECK(ctx->plan->ops[3].op == WL_PLAN_OP_VARIABLE &&
            ctx->plan->ops[4].op == WL_PLAN_OP_CONCAT &&
            ctx->plan->ops[5].op == WL_PLAN_OP_CONSOLIDATE); ++numeric_runtime;
    }
}
static void numeric_append_positive(void){
    fixture_t f = create(1, true);
    wl_session_set_delta_cb(f.session, capture, &f);
    int64_t one = 1, other[] = {2, 3};
    CHECK(!wl_session_insert(f.session, "input", &one, 1,
        1) && !complete(f.session));
    audit_target = UINT32_MAX; stopped = false;
    audit_numeric = audit_plan = audit_sources = audit_copy = numeric_runtime =
        0;
    wl_columnar_session_test_filter_append_checkpoint = audit_pause;
    wl_columnar_session_test_observed_transaction_checkpoint = audit_phase;
    wl_columnar_session_test_framed_grant_finished = numeric_runtime_shape;
    CHECK(!wl_session_insert(f.session, "input", other, 2,
        1) && !complete(f.session));
    wl_columnar_session_test_filter_append_checkpoint = NULL;
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    wl_columnar_session_test_framed_grant_finished = NULL;
    CHECK(audit_numeric && audit_plan && audit_copy && numeric_runtime);
    contents(&f, 12, 2); CHECK(f.events == 2 && f.positive == 2 && !f.negative);
    close_fixture(&f);
}

static void memory_policy(void){
    for (unsigned kind = 0; kind < 2; ++kind) {
        fixture_t f = create(kind, true);
        wl_session_set_delta_cb(f.session, capture, &f);
        int64_t one = 1, two = 2;
        CHECK(!wl_session_insert(f.session, "input", &one, 1,
            1) && !complete(f.session));
        unsigned before_count = kind == 0?1u:0u;
        col_rel_t *prior = session_find_rel(COL_SESSION(f.session), "out");
        col_relation_snapshot_t generation =
            wl_columnar_relation_snapshot(prior);
        CHECK(!wl_session_insert(f.session, "input", &two, 1, 1));
        wl_columnar_memory_governor_t *governor =
            wl_columnar_memory_governor_ref_get(COL_SESSION(
                    f.session)->memory_governor);
        CHECK(governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING);
        uint64_t normal = atomic_load_explicit(&governor->usable_bytes,
                memory_order_relaxed);
        uint64_t floor = wl_columnar_memory_reserved(governor);
        atomic_store_explicit(&governor->usable_bytes, floor,
            memory_order_release);
        int rc = complete(f.session);
        CHECK(rc > 0 &&
            wl_columnar_session_budget_denied(COL_SESSION(f.session)));
        closed(&f); contents(&f, kind == 0?2u:0u, before_count);
        CHECK(f.events == before_count &&
            session_find_rel(COL_SESSION(f.session), "out") == prior &&
            wl_columnar_relation_snapshot_equal(generation,
            wl_columnar_relation_snapshot(prior)));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report) &&
            report.result == rc && !report.cleanup_status);
        atomic_store_explicit(&governor->usable_bytes, normal,
            memory_order_release);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session));
        contents(&f, kind == 0?6u:4u, kind == 0?2u:1u);
        CHECK(f.events == (kind == 0?2u:1u) && !f.negative &&
            wl_columnar_memory_reserved(governor) <= normal); close_fixture(&f);
    }
}

static int oracle_owner;
static void append_oracle(uint8_t *bytes, uint32_t size, bool expected,
    unsigned child_payments){
    wl_plan_expr_buffer_t expr = {.data = bytes, .size = size};
    for (unsigned cut = 0; cut < child_payments+3; ++cut) {
        bool success = cut == child_payments+2;
        uint64_t total = 256+(uint64_t)child_payments*224;
        uint64_t limit = success?total:cut == 0?127:cut <= child_payments?
            128+(uint64_t)cut*224-1:total-1;
        uint64_t used = success?total:cut == 0?0:cut <= child_payments?
            128+(uint64_t)(cut-1)*224:128+(uint64_t)child_payments*224;
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(limit, &control) &&
            !wl_evaluation_control_attach(control,
            &oracle_owner) && !wl_evaluation_control_begin(control,
            &oracle_owner));
        bool supported = !expected;
        int rc =
            wl_columnar_session_filter_append_supported_attempt_controlled(
            &expr, &supported,
            control, wl_evaluation_control_active_attempt_id(control));
        CHECK(rc == (success?0:WL_EVALUATION_CONTROL_WORK_LIMIT) &&
            supported == (success?expected:!expected));
        CHECK(wl_evaluation_control_finish(control, &oracle_owner, 0, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control,
            &report) && report.consumed == used);
        CHECK(!wl_evaluation_control_detach(control, &oracle_owner));
        wl_evaluation_control_release(control);
    }
}
static void append_oracles(void){
    /* Independent literal child contracts: ENTRY/FINAL2T; BOOL7T; STRLEN9T;
     * unknown11T. Parent adds its distinct128 PRE and128 POST. */
    uint8_t boolean[] = {WL_PLAN_EXPR_BOOL, 1},
        scalar[] = {WL_PLAN_EXPR_STR_FN_STRLEN}, unknown[] = {255};
    append_oracle(NULL, 0, true, 2);
    append_oracle(boolean, sizeof(boolean), true, 7);
    append_oracle(scalar, sizeof(scalar), false, 9);
    append_oracle(unknown, sizeof(unknown), true, 11);
}

int main(void){
    append_oracles(); positives(); allocation_errors(); interruptions();
    audit_interruptions(); numeric_append_positive(); memory_policy();
    puts("observed FILTER recovery: all checks passed"); return 0;
}
