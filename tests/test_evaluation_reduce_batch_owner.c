/* #2298: batch configuration does not exclude a genuine composed REDUCE. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/thread.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)

extern int wl_columnar_ops_test_reduce_batch_authorized(const wl_plan_op_t *,
    wl_columnar_eval_stack_cleanup_frame_t *, wl_col_session_t *,
    wl_evaluation_control_t *, uint64_t, wl_columnar_filter_status_t *,
    const void *);

typedef struct { int64_t k, n; int diff; } event_t;
typedef struct { event_t values[16]; unsigned count; } events_t;
typedef struct {
    wl_session_t *session;
    wl_evaluation_control_t *control, *alien;
    unsigned mode, hits, payments;
    bool fired, probed;
    uint64_t prefix, successor;
    wl_evaluation_control_report_t frozen;
} probe_t;
static probe_t *active;

static int
finish(wl_session_t *session)
{
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

static void
on_delta(const char *name, const int64_t *row, uint32_t width, int diff,
    void *user)
{
    if (strcmp(name, "output")) return;
    events_t *events = user;
    CHECK(width == 2 && events->count < 16 && (diff == 1 || diff == -1));
    events->values[events->count++] = (event_t){row[0], row[1], diff};
}

static void
on_tuple(const char *name, const int64_t *row, uint32_t width, void *user)
{
    on_delta(name, row, width, 1, user);
}

static bool
contains(const events_t *events, int64_t k, int64_t n, int diff)
{
    unsigned count = 0;
    for (unsigned i = 0; i < events->count; ++i)
        count += events->values[i].k == k && events->values[i].n == n
            && events->values[i].diff == diff;
    return count == 1;
}

static int
permission(const wl_plan_op_t *op, wl_columnar_eval_plan_numeric_attempt_t *n,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_ops_test_reduce_batch_authorized(op, frame, session,
               control, id, status, n);
}

typedef struct {
    const wl_plan_op_t *op;
    wl_columnar_eval_plan_numeric_attempt_t *n;
    int result;
} peer_t;

static void *
wrong_thread(void *user)
{
    peer_t *peer = user;
    wl_columnar_eval_plan_numeric_attempt_t *n = peer->n;
    peer->result = permission(peer->op, n, n->frame, n->session, n->control,
            n->attempt_id, &n->child_status);
    return NULL;
}

static void
component_probes(wl_columnar_eval_plan_numeric_attempt_t *n,
    const wl_plan_op_t *op)
{
    wl_columnar_eval_stack_cleanup_frame_t *frame = n->frame;
    wl_col_session_t *session = n->session;
    wl_evaluation_control_t *control = n->control;
    uint64_t id = n->attempt_id, before = 0, after = 0;
    CHECK(!wl_evaluation_control_test_active_consumed(control, id, &before));
    CHECK(!permission(op, n, frame, session, control, id, &n->child_status));
    wl_columnar_eval_plan_numeric_attempt_t *copy = malloc(sizeof(*copy));
    CHECK(copy); memcpy(copy, n, sizeof(*copy));
    CHECK(permission(op, copy, frame, session, control, id, &n->child_status));
    free(copy);
    wl_plan_op_t copied_op = *op;
    CHECK(permission(&copied_op, n, frame, session, control, id,
        &n->child_status) == EINVAL);
    CHECK(permission(op, n, NULL, session, control, id, &n->child_status));
    CHECK(permission(op, n, frame, NULL, control, id,
        &n->child_status) == EINVAL);
    CHECK(permission(op, n, frame, session, active->alien, id,
        &n->child_status) == EINVAL);
    CHECK(permission(op, n, frame, session, control, id + 1,
        &n->child_status) == EINVAL);
    wl_columnar_filter_status_t output;
    memset(&output, 0xa5, sizeof(output));
    wl_columnar_filter_status_t poison = output;
    CHECK(permission(op, n, frame, session, control, id, &output) == EINVAL);
    int rc = wl_columnar_ops_reduce_attempt_start_owned(op, frame, session,
            control, id, &output, n);
    CHECK((rc == EINVAL || rc == EBUSY) && !memcmp(&output, &poison,
        sizeof(output)));
    wl_columnar_filter_status_t *overlap = (void *)&n->frame;
    rc = wl_columnar_ops_reduce_attempt_start_owned(op, frame, session,
            control, id, overlap, n);
    CHECK((rc == EINVAL || rc == EBUSY) && n->frame == frame);
    for (unsigned variant = 0; variant < 23; ++variant) {
        wl_columnar_eval_plan_numeric_phase_t phase = n->phase,
            pending = n->pending_action;
        const wl_plan_relation_t *plan = n->plan;
        uint32_t begin = n->begin, cursor = n->cursor, end = n->end,
            workers = session->num_workers;
        atomic_uint_fast64_t shared;
        atomic_init(&shared, 19);
        if (variant == 0) n->controlled_materialized_batch_fallback = false;
        if (variant == 1) n->composed_mode = false;
        if (variant == 2) n->composed_busy = false;
        if (variant ==
            3) n->phase = WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY;
        if (variant ==
            4) n->pending_action =
                WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD;
        if (variant == 5) n->cursor = n->end;
        if (variant == 6) n->end = n->plan->op_count + 1;
        if (variant == 7) n->plan = NULL;
        if (variant == 8) session->current_iteration = 1;
        if (variant == 9) session->diff_operators_active = true;
        if (variant == 10) session->retraction_seeded = true;
        if (variant == 11) session->retraction_right_pass = true;
        if (variant == 12) session->tdd_subpass_active = true;
        if (variant == 13) session->tdd_outbound_only_active = true;
        if (variant == 14) session->join_output_shared_count = &shared;
        if (variant == 15) session->num_workers = 2;
        if (variant == 16) n->frame = NULL;
        if (variant == 17) n->session = NULL;
        if (variant == 18) n->control = active->alien;
        if (variant == 19) n->attempt_id = id + 1;
        if (variant == 20) session->tdd_input_run = (void *)(uintptr_t)1;
        if (variant == 21) session->coordinator = (void *)(uintptr_t)1;
        if (variant == 22) n->begin = n->cursor + 1;
        CHECK(permission(op, n, frame, session, control, id, &n->child_status));
        CHECK(atomic_load_explicit(&shared, memory_order_acquire) == 19);
        n->controlled_materialized_batch_fallback = true;
        n->composed_mode = true;
        n->composed_busy = true; n->phase = phase; n->pending_action = pending;
        n->plan = plan; n->begin = begin; n->cursor = cursor; n->end = end;
        n->frame = frame; n->session = session; n->control = control;
        n->attempt_id = id;
        session->current_iteration = 0; session->diff_operators_active = false;
        session->retraction_seeded = session->retraction_right_pass = false;
        session->tdd_subpass_active = session->tdd_outbound_only_active = false;
        session->join_output_shared_count = NULL;
        session->tdd_input_run = NULL; session->coordinator = NULL;
        session->num_workers = workers;
    }
    /* Match the supplied and numeric-record session while preserving the
     * genuine opaque frame. Only its actual embedded session can authorize. */
    wl_col_session_t copied_session = *session;
    n->session = &copied_session;
    CHECK(permission(op, n, frame, &copied_session, control, id,
        &n->child_status) == EINVAL);
    n->session = session;
    peer_t peer = {.op = op, .n = n}; wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, wrong_thread,
        &peer) && !wl_thread_join(&thread));
    CHECK(peer.result);
    CHECK(!permission(op, n, frame, session, control, id, &n->child_status)
        && !wl_evaluation_control_test_active_consumed(control, id, &after)
        && before == after && !memcmp(&output, &poison, sizeof(output)));
}

static void
checkpoint(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    CHECK(active && control == active->control);
    if (phase == 2 && active->fired) ++active->payments;
    if (phase != 1) return;
    wl_col_session_t *session = (wl_col_session_t *)active->session;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(session->cleanup_active,
        session, &ctx)
        && ctx->child_bound);
    wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    CHECK(n->composed_busy &&
        n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && n->pending_action == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY
        && n->cursor < n->end &&
        n->plan->ops[n->cursor].op == WL_PLAN_OP_REDUCE);
    (void)progress;
    ++active->hits;
    if (!active->prefix) CHECK(!wl_evaluation_control_test_active_consumed(
                control,
                n->attempt_id, &active->prefix));
    if (session->join_batch_bytes && !active->probed) {
        component_probes(n, &n->plan->ops[n->cursor]); active->probed = true;
    }
    if (!active->mode || active->fired) return;
    active->fired = true;
    if (active->mode == 1) wl_evaluation_control_request_cancel(control);
    if (active->mode == 2) CHECK(wl_evaluation_control_charge_attempt(control,
            n->attempt_id, UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    if (active->mode >= 5) {
        wl_evaluation_control_attempt_finish_result_t ended =
            wl_evaluation_control_finish_attempt(
            control, active->session->operation_admission, n->attempt_id, 0, 0);
        CHECK(!ended.protocol && ended.report_valid && !ended.report.result);
        active->frozen = ended.report;
        if (active->mode == 6) {
            CHECK(!wl_evaluation_control_reset(control));
            wl_evaluation_control_attempt_begin_result_t next =
                wl_evaluation_control_begin_attempt(
                control, active->session->operation_admission);
            CHECK(!next.protocol && next.attempt_id != n->attempt_id);
            active->successor = next.attempt_id;
        }
    }
}

static void
literal(wl_session_t *session, unsigned cycle)
{
    const int64_t expected[3][4] = {{1, 8, 2, 1}, {1, 4, 2, 1}, {1, 4, 2, 2}};
    col_rel_t *out = session_find_rel((wl_col_session_t *)session, "output");
    CHECK(out && out->nrows == 2 && out->ncols == 2 &&
        out->schema.n_children == 2
        && out->column_types && out->column_types[0] == WIRELOG_TYPE_INT64
        && out->column_types[1] == WIRELOG_TYPE_INT64 && !out->timestamps);
    for (unsigned r = 0; r < 2; ++r)
        for (unsigned c = 0; c < 2;
            ++c) CHECK(out->columns[c][r] == expected[cycle][r*2+c]);
}

static const char *program_text =
    ".decl a(k:int64,v:int64)\n.decl b(k:int64,p:int64)\n"
    ".decl c(k:int64,q:int64)\n.decl output(k:int64,n:int64)\n"
    "output(k,count(v)):-a(k,v),b(k,p),c(k,q).\n";

int
main(void)
{
    uint64_t prefixes[2][2] = {{0}};
    for (unsigned batch = 0; batch < 2; ++batch)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (unsigned snapshot = 0; snapshot < 2; ++snapshot)
                for (unsigned mode = 0; mode < (batch ? 7u : 1u); ++mode) {
                    wirelog_error_t error;
                    wirelog_program_t *program =
                        wirelog_parse_string(program_text, &error);
                    wl_plan_t *plan = NULL;
                    CHECK(program && !wl_plan_from_program(program, &plan));
                    const wl_plan_relation_t *head =
                        &plan->strata[0].relations[0];
                    CHECK(head->ops[0].op == WL_PLAN_OP_LFTJ);
                    bool reduce = false;
                    for (uint32_t i = 1; i < head->op_count;
                        ++i) reduce |= head->ops[i].op == WL_PLAN_OP_REDUCE;
                    CHECK(reduce);
                    wl_evaluation_control_t *control = NULL, *alien = NULL;
                    int alien_owner = 0;
                    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                        &control)
                        && !wl_evaluation_control_create(0, &alien)
                        && !wl_evaluation_control_attach(alien, &alien_owner)
                        && !wl_evaluation_control_begin(alien, &alien_owner));
                    wl_columnar_memory_resolution_t resolution =
                    {.budget_bytes = UINT64_C(512)<<20,
                            .usable_bytes
                         = UINT64_C(512)<<20,
                            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
                    wl_columnar_memory_governor_ref_t *governor =
                        wl_columnar_memory_governor_ref_create(&resolution);
                    CHECK(governor);
                    wl_session_options_t options;
                    wl_session_options_init(&options);
                    options.evaluation_control = control;
                    options.memory_governor = governor;
                    wl_session_t *session = NULL, *raw = NULL;
                    CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                        plan, 1, &options, &session)
                        && !wl_session_create(wl_backend_columnar(), plan, 1,
                        &raw));
                    ((wl_col_session_t *)session)->join_batch_bytes =
                        ((wl_col_session_t *)raw)->join_batch_bytes =
                        batch ? 64 : 0;
                    ((wl_col_session_t *)session)->join_batch_strict =
                        ((wl_col_session_t *)raw)->join_batch_strict = false;
                    const int64_t a[] = {1, 10, 1, 20, 2, 7},
                        b[] = {1, 100, 1, 101, 2, 102},
                        c[] = {1, 200, 1, 201, 2, 202};
                    wl_session_t *sessions[] = {session, raw};
                    for (unsigned i = 0; i < 2;
                        ++i) CHECK(!wl_session_insert(sessions[i], "a", a, 3, 2)
                            && !wl_session_insert(sessions[i], "b", b, 3,
                            2) && !wl_session_insert(sessions[i], "c", c, 3,
                            2));
                    if (mode == 3 ||
                        mode == 4) CHECK(prefixes[observed][snapshot]
                            && !wl_evaluation_control_configure(control,
                            prefixes[observed][snapshot] + (mode ==
                            4 ? 192 : 0)));
                    events_t events = {0}, tuples = {0};
                    if (observed) wl_session_set_delta_cb(session, on_delta,
                            &events);
                    probe_t probe = {.session = session, .control = control,
                                     .alien = alien, .mode = mode};
                    active = &probe;
                    wl_columnar_ops_test_reduce_attempt_checkpoint = checkpoint;
                    int rc = snapshot ? wl_session_snapshot(session, on_tuple,
                            &tuples) : finish(session);
                    wl_columnar_ops_test_reduce_attempt_checkpoint = NULL;
                    if (!mode) {
                        CHECK(!rc && probe.hits &&
                            (batch ? probe.probed : !probe.probed));
                        if (batch) prefixes[observed][snapshot] = probe.prefix;
                    } else {
                        int expected = mode ==
                            1 ? WL_EVALUATION_CONTROL_CANCELLED
                            : mode >=
                            5 ? EINVAL : WL_EVALUATION_CONTROL_WORK_LIMIT;
                        if (rc != expected) fprintf(stderr,
                                "owned REDUCE batch=%u observed=%u snapshot=%u mode=%u rc=%d expected=%d\n",
                                batch, observed, snapshot, mode, rc, expected);
                        CHECK(rc == expected && probe.fired && !events.count &&
                            !tuples.count
                            && !session_find_rel((wl_col_session_t *)session,
                            "output"));
                        wl_evaluation_control_report_t report;
                        CHECK(!wl_evaluation_control_report(control, &report));
                        if (mode >=
                            5) CHECK(report.attempt_id ==
                                probe.frozen.attempt_id
                                && report.consumed == probe.frozen.consumed &&
                                !report.result);
                        else CHECK(report.result == expected &&
                                !report.execution_status &&
                                !report.cleanup_status);
                        if (mode == 3 ||
                            mode ==
                            4) CHECK(report.consumed ==
                                prefixes[observed][snapshot] + (mode ==
                                4 ? 192 : 0)
                                && probe.prefix ==
                                prefixes[observed][snapshot] &&
                                probe.payments == (mode == 4 ? 1u : 0u));
                        if (probe.successor) {
                            uint64_t used = UINT64_MAX;
                            CHECK(wl_evaluation_control_active_attempt_id(
                                    control) == probe.successor
                                && !wl_evaluation_control_test_active_consumed(
                                    control, probe.successor, &used) && !used);
                            wl_evaluation_control_attempt_finish_result_t ended
                                = wl_evaluation_control_finish_attempt(
                                    control, session->operation_admission,
                                    probe.successor, 0, 0);
                            CHECK(!ended.protocol && ended.report_valid &&
                                !ended.report.consumed);
                        }
                        CHECK(!wl_evaluation_control_reset(control)
                            && !wl_evaluation_control_configure(control,
                            UINT64_C(1000000000)));
                        CHECK(!(snapshot ? wl_session_snapshot(session,
                            on_tuple, &tuples) : finish(session)));
                    }
                    CHECK(!finish(raw)); literal(session, 0); literal(raw, 0);
                    if (observed) CHECK(events.count == 2 && contains(&events,
                            1, 8, 1) && contains(&events, 2, 1, 1));
                    if (snapshot) CHECK(tuples.count == 2 && contains(&tuples,
                            1, 8, 1) && contains(&tuples, 2, 1, 1));
                    for (unsigned cycle = 1; cycle < 3; ++cycle) {
                        const int64_t removed[] = {1, 20}, added[] = {2, 9};
                        for (unsigned i = 0; i < 2; ++i) CHECK(!(cycle == 1
                            ? wl_session_remove(sessions[i], "a", removed, 1, 2)
                            : wl_session_insert(sessions[i], "a", added, 1,
                                2)));
                        events.count = tuples.count = 0;
                        CHECK(!(snapshot ? wl_session_snapshot(session,
                            on_tuple,
                            &tuples) : finish(session)) && !finish(raw));
                        literal(session, cycle); literal(raw, cycle);
                        if (observed) CHECK(events.count == 2 && (cycle == 1
                            ? contains(&events, 1, 8, -1) && contains(&events,
                                1, 4, 1)
                            : contains(&events, 2, 1, -1) && contains(&events,
                                2, 2, 1)));
                        unsigned delivered = events.count;
                        CHECK(!finish(session) && events.count == delivered);
                    }
                    active = NULL;
                    CHECK(!wl_session_try_destroy(&session) &&
                        !wl_session_try_destroy(&raw));
                    wl_plan_free(plan); wirelog_program_free(program);
                    CHECK(!wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(governor)));
                    wl_columnar_memory_governor_ref_release(governor);
                    wl_evaluation_control_release(control);
                    uint64_t alien_id =
                        wl_evaluation_control_active_attempt_id(alien),
                        used = UINT64_MAX;
                    CHECK(!wl_evaluation_control_test_active_consumed(alien,
                        alien_id, &used) && !used
                        && !wl_evaluation_control_finish(alien, &alien_owner, 0,
                        0)
                        && !wl_evaluation_control_detach(alien, &alien_owner));
                    wl_evaluation_control_release(alien);
                }
    puts("composed REDUCE batch ownership passed"); return 0;
}
