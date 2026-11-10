/* #2299 genuine observed JOIN + scalar native birth and Original-END recovery. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do{if (!(x)){fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                                  #x); exit(1);}}while (0)
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
typedef struct {wirelog_program_t *program; wl_plan_t *plan;
                wl_session_t *session; wl_evaluation_control_t *control;
                int64_t rows[3][2], expected;
                unsigned events, positive, negative; int balance[3];
                bool concat, cold; unsigned history_count;
                wl_columnar_eval_cleanup_receipt_key_t history_keys[31];
} fixture_t;
static fixture_t *active; static bool stopped;
static unsigned action, hits, cut; static uint64_t successor;
static wl_evaluation_control_report_t original;
static uint32_t boundary_index; static unsigned ack_successes;
static int complete(wl_session_t *session){
    /* 68-op constructed plans need more than10000 genuine framed actions.
     * This is a test watchdog, not an engine budget or polling oracle. */
    for (unsigned i = 0; i < 100000; ++i){
        int rc = wl_session_step(session); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static void capture(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *arg){
    fixture_t *f = arg;
    CHECK(!strcmp(name,
        "out") && width == 3 && row[0] >= 1 && row[0] <= 3 &&
        (diff == 1 || diff == -1)); unsigned i = (unsigned)(row[0]-1);
    CHECK(row[1] == f->rows[i][1] && row[2] == f->expected);
    f->balance[i] += diff; CHECK(f->balance[i] >= 0 && f->balance[i] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static fixture_t create_kind(bool concat, unsigned extra, bool joined){
    fixture_t f = {.concat = concat}; wirelog_error_t error; char source[512];
    snprintf(source, sizeof(source),
        ".decl input(i:int64,s:string)\n%s.decl out(i:int64,s:string,v:%s)\nout(i,s,%s) :- input(i,s)%s.\n",
        joined?".decl gate(i:int64)\n":"", concat?"string":"int64",
        concat?"cat(s,\"!\")":"strlen(s)", joined?",gate(i)":"");
    f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && !wl_plan_from_program(f.program,
        &f.plan) && !wl_evaluation_control_create(0, &f.control));
    CHECK(f.plan->stratum_count == 1 && f.plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *p = &f.plan->strata[0].relations[0];
    CHECK(p->op_count == (joined?4u:3u) &&
        p->ops[0].op == WL_PLAN_OP_VARIABLE &&
        (!joined || p->ops[1].op == WL_PLAN_OP_JOIN) &&
        p->ops[joined?2:1].op == WL_PLAN_OP_MAP &&
        p->ops[joined?3:2].op == WL_PLAN_OP_CONSOLIDATE);
    if (extra) {
        /* Constructed-plan stress: actual owning identity MAPs, before session
         * creation builds all scalar/operator views. No owner-state injection. */
        wl_plan_relation_t *r =
            (wl_plan_relation_t *)&f.plan->strata[0].relations[0];
        wl_plan_op_t *ops = calloc((size_t)r->op_count+extra, sizeof(*ops));
        CHECK(ops); ops[0] = r->ops[0];
        for (unsigned i = 0; i < extra; ++i) {
            ops[i+1].op = WL_PLAN_OP_MAP; ops[i+1].project_count = 2;
            uint32_t *indices = malloc(2*sizeof(*indices)); CHECK(indices);
            indices[0] = 0; indices[1] = 1; ops[i+1].project_indices = indices;
        }
        for (uint32_t i = 1; i < r->op_count; ++i) ops[i+extra] = r->ops[i];
        free((void *)r->ops); r->ops = ops; r->op_count += extra;
    }
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    int64_t text = wl_intern_put(f.plan->intern, "abc"); CHECK(text >= 0);
    f.expected = concat?wl_intern_put(f.plan->intern, "abc!"):3;
    CHECK(f.expected >= 0);
    for (unsigned i = 0; i < 3; ++i) {
        f.rows[i][0] = (int64_t)i+1; f.rows[i][1] = text;
        int64_t key = (int64_t)i+1;
        if (joined) CHECK(!wl_session_insert(f.session, "gate", &key, 1, 1));
    }
    return f;
}
static fixture_t create_extra(bool concat, unsigned extra){
    return create_kind(concat, extra, true);
}
static fixture_t create(bool concat){
    return create_extra(concat, 0);
}
/* Authentic historical FREED receipts from real empty physical frames.
 * This is fixture setup through native APIs, never manufactured state/authority. */
static void seed_history(fixture_t *f, unsigned count){
    CHECK(count <= 31 && !f->history_count);
    wl_evaluation_control_attempt_begin_result_t operation =
        wl_evaluation_control_begin_attempt(f->control,
            f->session->operation_admission);
    CHECK(!operation.protocol && operation.attempt_id);
    for (unsigned i = 0; i < count; ++i) {
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(
            !wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
                COL_SESSION(f->session), &frame, f->control,
                operation.attempt_id, (uint64_t)(uintptr_t)f,
                &f->history_keys[i]));
        CHECK(!wl_columnar_eval_stack_cleanup_receipt_prepare(frame,
            &f->history_keys[i], 0, false));
        wl_columnar_eval_cleanup_outcome_t outcome;
        CHECK(!wl_columnar_eval_stack_cleanup_finish_receipted(&frame,
            &f->history_keys[i],
            &outcome) && !frame &&
            outcome == WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_FREED);
        CHECK(COL_SESSION(f->session)->cleanup_receipts[i].state ==
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED);
    }
    wl_evaluation_control_attempt_finish_result_t end =
        wl_evaluation_control_finish_attempt(f->control,
            f->session->operation_admission, operation.attempt_id, 0, 0);
    CHECK(!end.protocol && end.report_valid && !end.report.result &&
        !wl_evaluation_control_reset(f->control)); f->history_count = count;
}
static void contents(fixture_t *f, unsigned rows){
    col_rel_t *out = session_find_rel(COL_SESSION(f->session), "out");
    CHECK(out && out->nrows == rows && out->ncols == 3 && out->column_types &&
        out->column_types[0] == WIRELOG_TYPE_INT64 &&
        out->column_types[1] == WIRELOG_TYPE_STRING &&
        out->column_types[2] ==
        (f->concat?WIRELOG_TYPE_STRING:WIRELOG_TYPE_INT64));
    bool seen[3] = {false, false, false};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        int64_t id = out->columns[0][r];
        CHECK(id >= 1 && id <= 3 && !seen[id-1]); seen[id-1] = true;
        CHECK(out->columns[1][r] == f->rows[id-1][1] &&
            out->columns[2][r] == f->expected);
    }
}
static void close_fixture(fixture_t *f){
    for (unsigned i = 0; i < f->history_count;
        ++i) CHECK(!wl_columnar_eval_stack_cleanup_receipt_ack(COL_SESSION(
                f->session), &f->history_keys[i]));
    wl_columnar_memory_governor_ref_t *g =
        COL_SESSION(f->session)->memory_governor;
    wl_columnar_memory_governor_ref_retain(g);
    wl_session_destroy(f->session); wl_plan_free(f->plan);
    wirelog_program_free(f->program); wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g); active = NULL;
}
static void same_report(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b){
    CHECK(a->attempt_id == b->attempt_id && a->limit == b->limit &&
        a->consumed == b->consumed && a->result == b->result &&
        a->stop_reason == b->stop_reason &&
        a->execution_status == b->execution_status &&
        a->cleanup_status == b->cleanup_status);
}
static void end_original(void){
    wl_evaluation_control_t *c = active->control;
    uint64_t id = wl_evaluation_control_active_attempt_id(c); CHECK(id);
    stopped = true;
    if (action == 0) {
        wl_evaluation_control_request_cancel(c); return;
    }
    if (action == 1) {
        CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT); return;
    }
    if (action == 4) {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        wl_evaluation_control_request_cancel(c); return;
    }
    wl_evaluation_control_attempt_finish_result_t end =
        wl_evaluation_control_finish_attempt(c,
            active->session->operation_admission, id, 0, 0);
    CHECK(!end.protocol && end.report_valid && !end.report.result);
    original = end.report;
    if (action == 3) {
        CHECK(!wl_evaluation_control_reset(c));
        wl_evaluation_control_attempt_begin_result_t next =
            wl_evaluation_control_begin_attempt(c,
                active->session->operation_admission);
        CHECK(!next.protocol && next.attempt_id != id);
        successor = next.attempt_id;
    }
}
static int probe_owner;
static void protected_owner(wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_attempt_t *ctx, uint32_t index){
    uint64_t before = 0, after = 0;
    CHECK(!wl_evaluation_control_test_active_consumed(ctx->control,
        ctx->attempt_id, &before));
    uint32_t output = 99;
    wl_columnar_eval_cleanup_receipt_key_t wrong = ctx->cleanup_receipt_key;
    wrong.operation_id ^= 1;
    CHECK(wl_columnar_session_native_cleanup_receipt_index(sess, &wrong,
        &output) == ENOENT && output == 99);
    CHECK(wl_columnar_eval_stack_cleanup_receipt_frame_index_bind(ctx->frame,
        (index+1)%32, &ctx->cleanup_receipt_key) == EINVAL);
    wl_columnar_eval_cleanup_receipt_t saved = sess->cleanup_receipts[index];
    sess->cleanup_receipts[index].key.operation_id ^= 1;
    CHECK(wl_columnar_session_native_cleanup_receipt_index(sess,
        &ctx->cleanup_receipt_key, &output) == EINVAL && output == 99);
    sess->cleanup_receipts[index] = saved;
    CHECK(wl_columnar_session_join_cleanup_register(sess, ctx, ctx->control,
        ctx->attempt_id, index, true) == EINVAL);
    /* Already-published owner cannot be replaced. Independent actual test
     * financier pays six256 packets=1536, never Original work. */
    for (unsigned below = 0; below < 2; ++below){
        wl_evaluation_control_t *grant = NULL;
        CHECK(!wl_evaluation_control_create(1536-below,
            &grant) && !wl_evaluation_control_attach(grant,
            &probe_owner) && !wl_evaluation_control_begin(grant, &probe_owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(grant);
        int rc = wl_columnar_session_join_cleanup_register(sess, ctx, grant, id,
                index, true);
        CHECK(rc == (below?WL_EVALUATION_CONTROL_WORK_LIMIT:EINVAL));
        CHECK(wl_evaluation_control_finish(grant, &probe_owner, rc > 0?rc:0,
            0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(grant,
            &report) && report.consumed == (below?1280u:1536u));
        CHECK(!wl_evaluation_control_detach(grant, &probe_owner));
        wl_evaluation_control_release(grant);
    }
    CHECK(!wl_evaluation_control_test_active_consumed(ctx->control,
        ctx->attempt_id, &after) && after == before &&
        !wl_columnar_session_native_cleanup_receipt_index(sess,
        &ctx->cleanup_receipt_key, &output) && output == index &&
        !memcmp(&saved, &sess->cleanup_receipts[index], sizeof(saved)));
}
static void expression(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed){
    (void)failed;
    if (stopped || phase != (active->concat?41u:94u) || progress != 2 ||
        ++hits < 2) return;
    CHECK(c == active->control);
    wl_col_session_t *sess = COL_SESSION(active->session);
    CHECK(wl_columnar_session_test_cleanup_owners(sess) == 1);
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(sess->cleanup_active, sess,
        &ctx) &&
        !wl_columnar_eval_stack_framed_authenticate(sess->cleanup_active, sess,
        ctx) && ctx->capture_only && ctx->abort_supported);
    uint32_t index = 99;
    CHECK(!wl_columnar_session_native_cleanup_receipt_index(sess,
        &ctx->cleanup_receipt_key, &index) && index == active->history_count);
    protected_owner(sess, ctx, index);
    end_original();
}
static void verify_error(fixture_t *f, int rc){
    CHECK(!COL_SESSION(f->session)->cleanup_active &&
        !COL_SESSION(f->session)->cleanup_pending &&
        !COL_SESSION(f->session)->cleanup_reserved_bytes);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->control, &report));
    if (action == 2 || action == 3) same_report(&original, &report);
    else CHECK(report.result == rc && !report.cleanup_status);
    if (successor) {
        uint64_t used = 999;
        CHECK(!wl_evaluation_control_test_active_consumed(f->control, successor,
            &used) && !used);
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(f->control,
                f->session->operation_admission, successor, 0, 0);
        CHECK(!end.protocol && end.report_valid && !end.report.consumed);
    }
    CHECK(!wl_evaluation_control_reset(f->control) &&
        !wl_evaluation_control_configure(f->control, 0));
}
static void mid_work(void){
    for (unsigned concat = 0; concat < 2; ++concat) for (unsigned cold = 0;
            cold < 3; ++cold) for (action = 0; action < 5; ++action){
                fixture_t f = create(concat); f.cold = cold; active = &f;
                if (cold) seed_history(&f, cold == 1?1:31);
                wl_session_set_delta_cb(f.session, capture, &f);
                if (!cold) CHECK(!wl_session_insert(f.session, "input",
                        f.rows[0], 1, 2));
                CHECK(!complete(f.session)); contents(&f, cold?0:1);
                col_rel_t *prior = session_find_rel(COL_SESSION(f.session),
                        "out");
                col_relation_snapshot_t generation =
                    wl_columnar_relation_snapshot(prior);
                wl_columnar_eval_cleanup_receipt_t historical =
                    COL_SESSION(f.session)->cleanup_receipts[0];
                if (cold) CHECK(historical.state ==
                        WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED);
                for (unsigned i = cold?0:1; i < 3;
                    ++i) CHECK(!wl_session_insert(f.session, "input", f.rows[i],
                        1, 2));
                stopped = false; hits = 0; successor = 0;
                if (action ==
                    1) CHECK(!wl_evaluation_control_configure(f.control,
                        UINT64_MAX-1));
                wl_columnar_expr_test_checkpoint = expression;
                int rc = complete(f.session);
                wl_columnar_expr_test_checkpoint = NULL;
                CHECK(stopped &&
                    rc ==
                    (action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                    1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                    4?EOVERFLOW:EINVAL));
                contents(&f, cold?0:1);
                CHECK(f.events == (cold?0u:1u) &&
                    session_find_rel(COL_SESSION(f.session),
                    "out") == prior &&
                    wl_columnar_relation_snapshot_equal(generation,
                    wl_columnar_relation_snapshot(prior)));
                verify_error(&f, rc);
                if (cold) CHECK(!memcmp(&historical,
                        &COL_SESSION(f.session)->cleanup_receipts[0],
                        sizeof(historical)));
                CHECK(!complete(f.session)); contents(&f, 3);
                CHECK(f.events == 3 && f.positive == 3 && !f.negative);
                if (cold) CHECK(!memcmp(&historical,
                        &COL_SESSION(f.session)->cleanup_receipts[0],
                        sizeof(historical)));
                CHECK(!wl_session_remove(f.session, "input", f.rows[0], 1,
                    2) && !complete(f.session)); contents(&f, 2);
                CHECK(f.events == 4 && f.negative == 1);
                CHECK(!wl_session_insert(f.session, "input", f.rows[0], 1,
                    2) && !complete(f.session)); contents(&f, 3);
                CHECK(f.events == 5 && f.positive == 4 &&
                    !complete(f.session) && f.events == 5);
                close_fixture(&f);
            }
}
static void boundary(const wl_col_session_t *read,
    wl_columnar_eval_serial_framed_host_action_t which,
    wl_evaluation_control_t *grant, int protocol){
    (void)grant;
    if (stopped) {
        return;
    }
    wl_col_session_t *sess = (wl_col_session_t *)read;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL; bool select = false;
    if (which == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK && !protocol) {
        ++ack_successes;
        CHECK(boundary_index < 32 &&
            sess->cleanup_receipts[boundary_index].state ==
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE);
    }
    if (sess->cleanup_active &&
        !wl_columnar_eval_stack_framed_context(sess->cleanup_active, sess,
        &ctx) && ctx->identity == ctx){
        CHECK(!wl_columnar_session_native_cleanup_receipt_index(sess,
            &ctx->cleanup_receipt_key, &boundary_index));
        select = cut == 0?ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP:
            cut == 1?ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING &&
            ctx->pending_action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN:
            cut == 2?ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING &&
            ctx->pending_action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD &&
            ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING &&
            ctx->child.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD:
            cut == 3?ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING &&
            ctx->pending_action ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_ACK:false;
    }else if (!sess->cleanup_active) {
        bool freed = false;
        uint64_t id = wl_evaluation_control_active_attempt_id(active->control);
        for (unsigned i = 0; i < 32;
            ++i) freed |= sess->cleanup_receipts[i].key.operation_attempt_id ==
                id &&
                sess->cleanup_receipts[i].state ==
                WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED;
        select = cut == 4?which == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE &&
            freed:cut == 5 &&
            which == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK && !protocol;
    }
    if (select && cut == 5) CHECK(ack_successes == 1);
    if (select) end_original();
}
static void boundaries(void){
    for (cut = 0; cut < 6; ++cut) {
        fixture_t f = create(false); active = &f;
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session));
        for (unsigned i = 0; i < 3;
            ++i) CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1, 2));
        action = 3; stopped = false; successor = 0; ack_successes = 0;
        boundary_index = UINT32_MAX;
        wl_columnar_session_test_framed_grant_finished = boundary;
        int rc = complete(f.session);
        wl_columnar_session_test_framed_grant_finished = NULL;
        if (!stopped || rc != EINVAL ||
            COL_SESSION(f.session)->cleanup_active ||
            COL_SESSION(f.session)->cleanup_pending ||
            COL_SESSION(f.session)->cleanup_reserved_bytes)
            fprintf(stderr,
                "boundary%u stopped%d rc%d active%p pending%p reserved%llu events%u\n",
                cut, stopped, rc,
                (void *)COL_SESSION(f.session)->cleanup_active,
                (void *)COL_SESSION(f.session)->cleanup_pending,
                (unsigned long long)COL_SESSION(
                    f.session)->cleanup_reserved_bytes, f.events);
        CHECK(stopped && rc == EINVAL && !f.events);
        if (cut == 5) {
            CHECK(ack_successes == 1);
        }
        contents(&f, 0);
        verify_error(&f, rc);
        CHECK(!complete(f.session)); contents(&f, 3);
        CHECK(f.events == 3 && !complete(f.session) && f.events == 3);
        close_fixture(&f);
    }
}

static uint64_t registration_cap, registration_used;
static bool registration_injected, registration_refused;
static unsigned registration_forward;
static uint64_t registration_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t which, uint64_t limit){
    if (!registration_injected && !session->cleanup_active &&
        which == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE){
        registration_injected = true; return registration_cap;
    }
    return limit;
}
static void registration_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t which,
    wl_evaluation_control_t *grant, int protocol){
    (void)which; (void)protocol; wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report));
    if (report.limit != registration_cap) return;
    CHECK(!registration_refused &&
        report.result == WL_EVALUATION_CONTROL_WORK_LIMIT &&
        report.consumed == registration_used && !report.execution_status &&
        !report.cleanup_status &&
        !session->cleanup_active && !session->cleanup_pending &&
        !session->cleanup_reserved_bytes); registration_refused = true;
}
static void registration_observe(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed){
    (void)c; (void)progress; (void)failed; if (phase == 94) {
        CHECK(registration_refused); ++registration_forward;
    }
}
static void birth_refusals(void){
    /* Compiler VARIABLE/JOIN/MAP/CONSOLIDATE; actual index1 lookup640+
     * host768+unary768 gives B2176. Ten independently paid JOIN256 packets. */
    const uint64_t caps[] = {2431, 2687, 2943, 3199, 3455, 3711, 3967, 4223,
                             4479, 4735};
    const uint64_t used[] = {2176, 2432, 2688, 2944, 3200, 3456, 3712, 3968,
                             4224, 4480};
    for (unsigned i = 0; i < sizeof(caps)/sizeof(*caps); ++i) {
        fixture_t f = create(false); active = &f; seed_history(&f, 1);
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session));
        wl_columnar_eval_cleanup_receipt_t historical =
            COL_SESSION(f.session)->cleanup_receipts[0];
        for (unsigned r = 0; r < 3;
            ++r) CHECK(!wl_session_insert(f.session, "input", f.rows[r], 1, 2));
        registration_cap = caps[i]; registration_used = used[i];
        registration_injected = registration_refused = false;
        registration_forward = 0;
        wl_columnar_session_test_framed_grant_limit = registration_limit;
        wl_columnar_session_test_framed_grant_finished = registration_finished;
        wl_columnar_expr_test_checkpoint = registration_observe;
        int rc = complete(f.session);
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_session_test_framed_grant_limit = NULL;
        wl_columnar_session_test_framed_grant_finished = NULL;
        CHECK(!rc && registration_injected && registration_refused &&
            registration_forward && f.events == 3 && f.positive == 3 &&
            !f.negative);
        contents(&f, 3);
        CHECK(!memcmp(&historical, &COL_SESSION(f.session)->cleanup_receipts[0],
            sizeof(historical)) && !COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        close_fixture(&f);
    }
}

/* Deterministic barriers inside fresh forward preparation. Actual cancellation
 * comes from a different thread retaining the real Original, with join as the
 * completion barrier. END/rebind uses the session's real operation admission. */
static unsigned poll_stage, poll_progress, poll_after_stop;
static uint64_t poll_prefix, poll_original_used;
static wl_evaluation_control_t *poll_grant;
static bool poll_seen, poll_finished, poll_funding_first;
static void *cancel_peer(void *data){
    wl_evaluation_control_t *control = data;
    wl_evaluation_control_request_cancel(control);
    wl_evaluation_control_release(control); return NULL;
}
static void fresh_poll(wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *grant, uint64_t grant_id, unsigned stage,
    uint32_t progress, bool funded){
    CHECK(ctx->control == active->control);
    if (stopped) {
        ++poll_after_stop; return;
    }
    if (stage != poll_stage || progress != poll_progress ||
        funded == poll_funding_first) return;
    CHECK(!poll_seen && !wl_columnar_session_test_cleanup_owners(ctx->session));
    poll_seen = true; poll_grant = grant;
    uint64_t used = 0;
    CHECK(!wl_evaluation_control_test_active_consumed(grant, grant_id, &used));
    CHECK(used == poll_prefix-(poll_funding_first?256u:0u));
    CHECK(!wl_evaluation_control_test_active_consumed(ctx->control,
        ctx->attempt_id, &poll_original_used));
    if (action == 0) {
        wl_thread_t thread; wl_evaluation_control_retain(ctx->control);
        CHECK(!wl_thread_create(&thread, cancel_peer, ctx->control));
        CHECK(!wl_thread_join(&thread)); stopped = true;
    } else end_original();
}
static void fresh_finished(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t which,
    wl_evaluation_control_t *grant, int protocol){
    (void)which; (void)protocol;
    if (grant != poll_grant || poll_finished) return;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant, &report));
    CHECK(report.consumed == poll_prefix-(poll_funding_first?256u:0u));
    if (poll_funding_first) CHECK(report.result ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!sess->cleanup_active && !sess->cleanup_pending &&
        !sess->cleanup_reserved_bytes &&
        !wl_columnar_session_test_cleanup_owners(sess)); poll_finished = true;
}
static uint64_t fresh_limit(const wl_col_session_t *sess,
    wl_columnar_eval_serial_framed_host_action_t which, uint64_t limit){
    if (poll_funding_first && !sess->cleanup_active &&
        which == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE) return poll_prefix-1;
    return limit;
}
static void fresh_observation(void){
    /* N=4 compiler control and N=68 constructed-plan stress. B includes the
     * actual unary prefix ending at JOIN, initial lookup and two host fronts.
     * With index1: B=2176+256*extra. k is independent JOIN packet ordinal. */
    for (unsigned stress = 0; stress < 2; ++stress)
        for (poll_stage = 0; poll_stage < 7; ++poll_stage)
            for (action = 0; action < 5; ++action) {
                if (stress && poll_stage != 1) continue;
                unsigned extra = stress?64u:0u;
                fixture_t f = create_extra(false, extra); active = &f;
                seed_history(&f, 1);
                wl_session_set_delta_cb(f.session, capture, &f);
                CHECK(!complete(f.session));
                for (unsigned i = 0; i < 3; ++i)
                    CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1,
                        2));
                poll_progress = poll_stage == 1?(stress?63u:2u):0u;
                uint64_t ordinal = poll_stage == 0?1u:poll_stage == 1?
                    (uint64_t)poll_progress+2u:(uint64_t)extra+poll_stage+4u;
                poll_prefix = 2176+256u*extra+256*ordinal;
                poll_funding_first = false; poll_after_stop = 0;
                poll_seen = poll_finished = stopped = false; poll_grant = NULL;
                successor = 0;
                if (action == 1)
                    CHECK(!wl_evaluation_control_configure(f.control,
                        UINT64_MAX-1));
                wl_columnar_session_test_join_birth_checkpoint = fresh_poll;
                wl_columnar_session_test_framed_grant_finished = fresh_finished;
                int rc = complete(f.session);
                wl_columnar_session_test_join_birth_checkpoint = NULL;
                wl_columnar_session_test_framed_grant_finished = NULL;
                CHECK(poll_seen && stopped && poll_finished &&
                    !poll_after_stop);
                CHECK(rc == (action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                    action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:
                    action == 4?EOVERFLOW:EINVAL));
                CHECK(!f.events); contents(&f, 0);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report));
                CHECK(report.consumed == poll_original_used);
                verify_error(&f, rc);
                CHECK(!complete(f.session)); contents(&f, 3);
                CHECK(f.events == 3 && f.positive == 3 && !f.negative &&
                    !complete(f.session) && f.events == 3);
                close_fixture(&f);
            }
    /* Funding refusal wins before Original cancellation is observed. The
     * pre-funding barrier causes both conditions at each actual packet. */
    for (poll_stage = 0; poll_stage < 7; ++poll_stage) {
        fixture_t f = create(false); active = &f; seed_history(&f, 1);
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session));
        for (unsigned i = 0; i < 3; ++i)
            CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1, 2));
        poll_progress = poll_stage == 1?2u:0u;
        uint64_t ordinal = poll_stage == 0?1u:poll_stage == 1?4u:poll_stage+4u;
        poll_prefix = 2176+256*ordinal; action = 0; successor = 0;
        poll_funding_first = true; poll_after_stop = 0;
        poll_seen = poll_finished = stopped = false; poll_grant = NULL;
        wl_columnar_session_test_join_birth_checkpoint = fresh_poll;
        wl_columnar_session_test_framed_grant_limit = fresh_limit;
        wl_columnar_session_test_framed_grant_finished = fresh_finished;
        int rc = complete(f.session);
        wl_columnar_session_test_join_birth_checkpoint = NULL;
        wl_columnar_session_test_framed_grant_limit = NULL;
        wl_columnar_session_test_framed_grant_finished = NULL;
        CHECK(poll_seen && poll_finished && !poll_after_stop && !f.events);
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
        contents(&f, 0); verify_error(&f, rc);
        CHECK(!complete(f.session)); contents(&f, 3); CHECK(f.events == 3);
        close_fixture(&f);
    }
}

static unsigned preframe_phase, preframe_progress;
static bool preframe_expression;
static uint64_t preframe_consumed;
static void preframe_stop(wl_evaluation_control_t *control, unsigned phase,
    uint32_t progress){
    if (stopped || phase != preframe_phase ||
        (preframe_progress != UINT32_MAX && progress != preframe_progress))
        return;
    wl_col_session_t *sess = COL_SESSION(active->session);
    CHECK(control == active->control && !sess->cleanup_active &&
        !sess->cleanup_pending && !sess->cleanup_reserved_bytes &&
        !wl_columnar_session_test_cleanup_owners(sess));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(!wl_evaluation_control_test_active_consumed(control, id,
        &preframe_consumed));
    if (action == 0) {
        wl_thread_t thread; wl_evaluation_control_retain(control);
        CHECK(!wl_thread_create(&thread, cancel_peer, control) &&
            !wl_thread_join(&thread)); stopped = true;
    } else end_original();
}
static void preframe_expr(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed){
    (void)failed;
    if (progress <= UINT32_MAX) preframe_stop(control, phase,
            (uint32_t)progress);
}
static void preframe_observation(void){
    for (unsigned region = 0; region < 12; ++region)
        for (action = 0; action < 5; ++action) {
            unsigned extra = region == 11?64u:0u;
            fixture_t f = create_extra(false, extra); active = &f;
            wl_session_set_delta_cb(f.session, capture, &f);
            CHECK(!complete(f.session));
            col_rel_t *prior = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t generation =
                wl_columnar_relation_snapshot(prior);
            for (unsigned i = 0; i < 3; ++i)
                CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1, 2));
            preframe_expression = region < 6;
            preframe_phase = region < 6?550+region:region < 11?560+region-6:561;
            preframe_progress = region >= 3 && region < 6?UINT32_MAX:
                region == 11?63u:region == 8?2u:0u;
            stopped = false; successor = 0;
            if (action == 1)
                CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_MAX-1));
            if (preframe_expression) wl_columnar_expr_test_checkpoint =
                    preframe_expr;
            else wl_columnar_session_test_head_scalar_checkpoint =
                    preframe_stop;
            int rc = complete(f.session);
            wl_columnar_expr_test_checkpoint = NULL;
            wl_columnar_session_test_head_scalar_checkpoint = NULL;
            if (!stopped || f.events ||
                rc !=
                (action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                4?EOVERFLOW:EINVAL))
                fprintf(stderr,
                    "preframe region%u phase%u progress%u action%u stopped%d rc%d events%u\n",
                    region, preframe_phase, preframe_progress, action, stopped,
                    rc, f.events);
            CHECK(stopped && !f.events &&
                rc == (action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                4?EOVERFLOW:EINVAL));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report) &&
                report.consumed == preframe_consumed);
            contents(&f, 0);
            CHECK(session_find_rel(COL_SESSION(f.session), "out") == prior &&
                wl_columnar_relation_snapshot_equal(generation,
                wl_columnar_relation_snapshot(prior)));
            verify_error(&f, rc);
            CHECK(!complete(f.session)); contents(&f, 3); CHECK(f.events == 3);
            close_fixture(&f);
        }
}

static bool cache_invalidated;
static uint64_t invalidate_cache(const wl_col_session_t *read,
    wl_columnar_eval_serial_framed_host_action_t which, uint64_t limit){
    if (!cache_invalidated && !read->cleanup_active &&
        which == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE) {
        uint64_t state[4];
        CHECK(!wl_columnar_session_test_head_scalar_state(
                (wl_col_session_t *)read,
                state, true) && state[0] && state[0] == state[1] &&
            state[2] && state[3]);
        cache_invalidated = true;
    }
    return limit;
}
static void cache_provenance(void){
    for (unsigned joined = 0; joined < 2; ++joined) {
        fixture_t f = create_kind(false, 0, joined); active = &f;
        wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session));
        for (unsigned i = 0; i < 3; ++i)
            CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1, 2));
        cache_invalidated = false;
        wl_columnar_session_test_framed_grant_limit = invalidate_cache;
        CHECK(complete(f.session) == EINVAL);
        wl_columnar_session_test_framed_grant_limit = NULL;
        CHECK(cache_invalidated && !f.events &&
            !COL_SESSION(f.session)->cleanup_active &&
            !COL_SESSION(f.session)->cleanup_pending &&
            !COL_SESSION(f.session)->cleanup_reserved_bytes);
        contents(&f, 0);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report) &&
            report.result == EINVAL && !report.cleanup_status);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session));
        contents(&f, 3); CHECK(f.events == 3); close_fixture(&f);
    }
}

static int classifier_owner;
static void classifier_case(const uint8_t *bytes, uint32_t size,
    bool expected, unsigned payments){
    /* Every selected case uses only224 tariffs. Model derives from the
     * literal ENTRY/PRE/POST/FINAL and existing decoder byte schedules. */
    for (unsigned k = 1; k <= payments+1; ++k) {
        uint64_t limit = k <= payments?(uint64_t)k*224-1:(uint64_t)payments*224;
        wl_evaluation_control_t *control = NULL;
        CHECK(!wl_evaluation_control_create(limit, &control) &&
            !wl_evaluation_control_attach(control, &classifier_owner) &&
            !wl_evaluation_control_begin(control, &classifier_owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        bool result = !expected;
        int rc = wl_columnar_expr_requires_scalar_attempt_controlled(bytes,
                size, &result, control, id);
        bool success = k > payments;
        CHECK(rc == (success?0:WL_EVALUATION_CONTROL_WORK_LIMIT));
        CHECK(result == (success?expected:!expected));
        CHECK(wl_evaluation_control_finish(control, &classifier_owner, 0,
            0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(control, &report));
        CHECK(report.consumed ==
            (success?(uint64_t)payments*224:(uint64_t)(k-1)*224));
        CHECK(!wl_evaluation_control_detach(control, &classifier_owner));
        wl_evaluation_control_release(control);
    }
}
static void classifier_oracles(void){
    /* Integer payload intentionally contains scalar opcode aliases. */
    uint8_t integer[] = {WL_PLAN_EXPR_CONST_INT, WL_PLAN_EXPR_CONST_STR,
                         WL_PLAN_EXPR_STR_FN_STRLEN, 255, 0, 1, 2, 3, 4};
    uint8_t last[10]; memcpy(last, integer, sizeof(integer));
    last[9] = WL_PLAN_EXPR_STR_FN_STRLEN;
    const uint8_t string[] = {WL_PLAN_EXPR_CONST_STR, 3, 0, 'a', 0, 'z'};
    const uint8_t variable[] = {WL_PLAN_EXPR_VAR_STRING, 4, 0, 'c', 'o', 'l',
                                '0'};
    const uint8_t boolean[] = {WL_PLAN_EXPR_BOOL, 1};
    const uint8_t scalar[] = {WL_PLAN_EXPR_STR_FN_STRLEN};
    const uint8_t unknown[] = {255};
    const uint8_t malformed[] = {WL_PLAN_EXPR_CONST_INT,
                                 WL_PLAN_EXPR_STR_FN_STRLEN};
    classifier_case(NULL, 0, false, 2);
    classifier_case(integer, sizeof(integer), false, 14);
    classifier_case(last, sizeof(last), true, 21);
    classifier_case(string, sizeof(string), true, 12);
    classifier_case(variable, sizeof(variable), true, 16);
    classifier_case(boolean, sizeof(boolean), false, 7);
    classifier_case(scalar, sizeof(scalar), true, 9);
    classifier_case(unknown, sizeof(unknown), false, 11);
    classifier_case(malformed, sizeof(malformed), false, 11);
}

static void unary_stage_observation(void){
    for (unsigned stage = 0; stage < 7; ++stage)
        for (action = 0; action < 5; ++action) {
            fixture_t f = create_kind(false, 0, false); active = &f;
            seed_history(&f, 1);
            wl_session_set_delta_cb(f.session, capture, &f);
            CHECK(!complete(f.session));
            for (unsigned i = 0; i < 3; ++i)
                CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1, 2));
            poll_stage = stage+100; poll_progress = stage == 1?1u:0u;
            uint64_t ordinal = stage == 0?1u:stage == 1?3u:stage+3u;
            poll_prefix = 1408+256*ordinal; poll_funding_first = false;
            poll_after_stop = 0; poll_seen = poll_finished = stopped = false;
            poll_grant = NULL; successor = 0;
            if (action == 1)
                CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_MAX-1));
            wl_columnar_session_test_join_birth_checkpoint = fresh_poll;
            wl_columnar_session_test_framed_grant_finished = fresh_finished;
            int rc = complete(f.session);
            wl_columnar_session_test_join_birth_checkpoint = NULL;
            wl_columnar_session_test_framed_grant_finished = NULL;
            CHECK(poll_seen && poll_finished && !poll_after_stop && !f.events);
            CHECK(rc == (action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                4?EOVERFLOW:EINVAL));
            contents(&f, 0); verify_error(&f, rc);
            CHECK(!complete(f.session)); contents(&f, 3); CHECK(f.events == 3);
            close_fixture(&f);
        }
}

/* The first independent scan is unary shape classification, before JOIN's
 * own scan. Stop OP63 in a real64-MAP prefix and require no later hook/body. */
static void unary_prefix_observation(void){
    for (action = 0; action < 5; ++action) {
        fixture_t f = create_extra(false, 64); active = &f;
        seed_history(&f, 1); wl_session_set_delta_cb(f.session, capture, &f);
        CHECK(!complete(f.session));
        for (unsigned i = 0; i < 3; ++i)
            CHECK(!wl_session_insert(f.session, "input", f.rows[i], 1, 2));
        poll_stage = 101; poll_progress = 63;
        /* Index1 lookup640+twohost768 precede ENTRY+64OP packets. */
        poll_prefix = 1408+65u*256; poll_funding_first = false;
        poll_after_stop = 0; poll_seen = poll_finished = stopped = false;
        poll_grant = NULL; successor = 0;
        if (action == 1)
            CHECK(!wl_evaluation_control_configure(f.control, UINT64_MAX-1));
        wl_columnar_session_test_join_birth_checkpoint = fresh_poll;
        wl_columnar_session_test_framed_grant_finished = fresh_finished;
        int rc = complete(f.session);
        wl_columnar_session_test_join_birth_checkpoint = NULL;
        wl_columnar_session_test_framed_grant_finished = NULL;
        CHECK(poll_seen && poll_finished && !poll_after_stop && !f.events);
        CHECK(rc == (action == 0?WL_EVALUATION_CONTROL_CANCELLED:
            action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
            4?EOVERFLOW:EINVAL));
        contents(&f, 0); verify_error(&f, rc);
        CHECK(!complete(f.session)); contents(&f, 3); CHECK(f.events == 3);
        close_fixture(&f);
    }
}

int main(void){
    classifier_oracles(); mid_work(); boundaries(); birth_refusals();
    fresh_observation(); unary_stage_observation();
    unary_prefix_observation(); preframe_observation(); cache_provenance();
    puts("JOIN scalar native birth: all checks passed"); return 0;
}
