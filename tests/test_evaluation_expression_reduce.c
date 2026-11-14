/* #2299 compiler-selected scalar aggregate operand integration. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/wirelog-extension.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do {if (!(x)){fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                   __LINE__,#x); exit(1);}}while (0)
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
extern void (*wl_columnar_ops_test_reduce_attempt_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
extern void (*wl_columnar_session_test_head_scalar_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t);
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
extern int wl_evaluation_control_test_active_consumed(wl_evaluation_control_t *,
    uint64_t, uint64_t *);
extern int wl_columnar_ops_test_reduce_attempt_fail_allocation;
extern int wl_columnar_expr_test_fail_allocation;
extern uint64_t (*wl_columnar_session_test_framed_grant_limit)(
    const wl_col_session_t *, wl_columnar_eval_serial_framed_host_action_t,
    uint64_t);
extern void (*wl_columnar_session_test_framed_grant_finished)(
    const wl_col_session_t *, wl_columnar_eval_serial_framed_host_action_t,
    wl_evaluation_control_t *, int);
extern wl_columnar_session_native_error_phase_t
wl_columnar_session_test_native_error_phase(const wl_col_session_t *);
typedef struct {
    wirelog_program_t *program; wl_plan_t *plan; wl_session_t *raw, *bounded;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    unsigned width, events, compile, eval, auth; bool mixed, processing;
    int balance[4][64];
} fixture_t;
static fixture_t *active;
static unsigned stop_phase, stop_at, hits, action;
static bool stopped;
static uint64_t successor;
static bool probe_epochs;
static uint64_t seen_epochs;
static void latch_epoch(fixture_t *);
static bool actual_latch_cell;
static unsigned actual_latch_cells;
/* Registered unary retirement cell prices (#2299, columnar/session.c): two
 * owner proofs 2*1088 + facade 384 + framed 384 + child 128 + latch 456 +
 * dispatch 32 = 3560; the mixed JOIN route prices 2104. A cell whose filter
 * still holds a live non-bare scalar reader also performs one funded scalar
 * END action (#2300): plan dispatch 1 (eval_plan.c source_end_fund), filter
 * wrapper 128 and reader leaf 128, each pinned exact/one-short by
 * test_evaluation_numeric_plan.c and test_relation_scalar_source_end.c. The
 * cell keeps its facade/framed 384s; END adds to them and replaces nothing:
 * 3817<=4096 unary and 2361<=3072 mixed. */
enum {
    LATCH_CELL_UNARY = 3560, LATCH_CELL_MIXED = 2104,
    LATCH_SCALAR_END_DISPATCH = 1, LATCH_SCALAR_END_WRAPPER = 128,
    LATCH_SCALAR_END_LEAF = 128
};
static bool latch_scalar_end, latch_end_continues;
static unsigned latch_end_cells, latch_plain_cells, latch_end_drained,
    latch_end_cursor;
static wl_evaluation_control_report_t frozen;
static int complete(fixture_t *f){
    f->processing = false;
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = wl_session_step(f->bounded);
        if (rc != EAGAIN) return rc;
        if (probe_epochs && stopped) latch_epoch(f);
    }
    CHECK(false); return EAGAIN;
}
static void event(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data){
    fixture_t *f = data;
    CHECK(!strcmp(name,
        "out") && width == f->width && (diff == 1 || diff == -1));
    int64_t group = width == 2?row[0]:0, value = row[width-1];
    CHECK(group >= 0 && group < 4 && value >= 0 && value < 64);
    f->balance[group][value] += diff;
    CHECK(f->balance[group][value] >= 0 && f->balance[group][value] <= 1);
    ++f->events;
}
static bool report_equal(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b){
    return a->attempt_id == b->attempt_id && a->consumed == b->consumed &&
           a->limit == b->limit && a->stop_reason == b->stop_reason &&
           a->execution_status == b->execution_status &&
           a->result == b->result && a->cleanup_status == b->cleanup_status;
}
static void observe(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed){
    (void)progress; (void)failed; fixture_t *f = active;
    if (!f || f->control != control) return;
    if (f->processing && (phase == 70 || phase == 83)) {
        CHECK(wl_columnar_session_test_cleanup_owners(COL_SESSION(
                f->bounded)) == (f->mixed?1u:2u));
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(COL_SESSION(
                f->bounded)->cleanup_active, COL_SESSION(f->bounded), &ctx));
        CHECK(!wl_columnar_eval_stack_framed_authenticate(ctx->frame,
            COL_SESSION(f->bounded),
            ctx) && ctx->capture_only && ctx->private_bindings &&
            ctx->abort_supported && ctx->cleanup_receipt_key.receipt_id);
        CHECK(wl_columnar_session_observed_bindings_authorized(COL_SESSION(
                f->bounded), ctx->private_bindings)); ++f->auth;
        if (phase == 70)++f->compile; else ++f->eval;
    }
    if (!stop_phase || stopped || phase != stop_phase ||
        ++hits != stop_at) return;
    stopped = true;
    if (action == 0)wl_evaluation_control_request_cancel(control);
    else if (action == 1)CHECK(wl_evaluation_control_charge(control,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else if (action == 2 || action == 3) {
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(control,
                f->bounded->operation_admission, id, 0, 0);
        CHECK(!end.protocol && end.report_valid); frozen = end.report;
        if (action == 3) {
            CHECK(!wl_evaluation_control_reset(control));
            wl_evaluation_control_attempt_begin_result_t next =
                wl_evaluation_control_begin_attempt(control,
                    f->bounded->operation_admission);
            CHECK(!next.protocol && next.attempt_id != id);
            successor = next.attempt_id;
        }
    }else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) == EOVERFLOW);
}
static void reduce_observe(wl_evaluation_control_t *c, unsigned p, uint64_t n){
    if (active && c == active->control && p == 33)active->processing = true;
    observe(c, p, n, false);
}
static void head_observe(wl_evaluation_control_t *c, unsigned p, uint32_t n){
    observe(c, p, n, false);
}
static fixture_t create(const char *fn, bool grouped, bool mixed,
    const char *operand){
    fixture_t f = {.width = grouped?2u:1u, .mixed = mixed}; char source[800];
    snprintf(source, sizeof(source),
        ".decl input(k:int64,s:string)\n.decl gate(k:int64)\n.decl out(%s)\nout(%s%s(%s)):-input(k,s)%s.\n",
        grouped?"k:int64,v:int64":"v:int64", grouped?"k,":"", fn, operand,
        mixed?",gate(k)":"");
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    unsigned reduce = 0, join = 0;
    for (unsigned i = 0; i < f.plan->stratum_count; ++i)for (unsigned j = 0;
            j < f.plan->strata[i].relation_count; ++j){
            const wl_plan_relation_t *h = &f.plan->strata[i].relations[j];
            for (unsigned k = 0; k < h->op_count; ++k) {
                reduce += h->ops[k].op == WL_PLAN_OP_REDUCE;
                join += h->ops[k].op == WL_PLAN_OP_JOIN;
            }
        }
    CHECK(reduce == 1 && (!mixed || join));
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = UINT64_C(512)<<20, .usable_bytes = UINT64_C(512)<<20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f.governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.raw));
    CHECK(!wl_evaluation_control_create(0, &f.control));
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.bounded));
    return f;
}
static uint64_t grant_limit(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t action_kind, uint64_t limit){
    actual_latch_cell = false;
    if (active && session == COL_SESSION(active->bounded) &&
        action_kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_NATIVE_CLEANUP &&
        session->cleanup_active &&
        wl_columnar_session_test_native_error_phase(session) ==
        WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD){
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        if (!wl_columnar_eval_stack_framed_context(session->cleanup_active,
            (wl_col_session_t *)session,
            &ctx) && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD &&
            ctx->child.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD){
            actual_latch_cell = true;
            CHECK(limit >= (active->mixed?3072u:4096u));
            /* Classify the cell from the filter state it starts from: a
             * live non-bare scalar reader makes this registered retirement
             * cell perform one funded scalar END action. */
            wl_columnar_filter_context_t *filter = NULL;
            CHECK(!wl_columnar_eval_stack_filter_context_owned(ctx->frame,
                &filter, &ctx->child));
            if (!active->mixed) CHECK(!filter->source_bare);
            latch_scalar_end =
                (filter->stage == WL_COLUMNAR_FILTER_STAGE_SOURCE &&
                filter->source_initialized && !filter->source_bare) ||
                (filter->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE &&
                filter->join_right_initialized && !filter->join_right_bare);
            /* A scalar END that did not drain must be continued by the very
             * next retirement cell; it may not be skipped or replaced. */
            CHECK(!latch_end_continues || latch_scalar_end);
        }
    }
    return limit;
}
static void grant_finished(const wl_col_session_t *session,
    wl_columnar_eval_serial_framed_host_action_t kind,
    wl_evaluation_control_t *grant, int protocol){
    if (!actual_latch_cell)return;
    CHECK(active && session == COL_SESSION(active->bounded) &&
        kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_NATIVE_CLEANUP &&
        grant != active->control && !protocol);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(grant,
        &report) && report.attempt_id && report.limit &&
        !report.execution_status && !report.cleanup_status);
    uint64_t expected = (active->mixed?LATCH_CELL_MIXED:LATCH_CELL_UNARY) +
        (latch_scalar_end?LATCH_SCALAR_END_DISPATCH+LATCH_SCALAR_END_WRAPPER+
        LATCH_SCALAR_END_LEAF:0u);
    if (report.consumed != expected)fprintf(stderr,
            "real native mixed%u end%u consumed%llu expected%llu\n",
            active->mixed, (unsigned)latch_scalar_end,
            (unsigned long long)report.consumed, (unsigned long long)expected);
    CHECK(report.consumed == expected && expected <= report.limit);
    ++actual_latch_cells;
    latch_end_continues = false;
    if (!latch_scalar_end) {
        ++latch_plain_cells; return;
    }
    ++latch_end_cells;
    /* The END action ran on the real reader: either it drained and the
     * filter left the source stage, or the reader is ENDING with a cursor
     * that never moves backwards (a release step may legitimately need more
     * than one cell) and the next retirement cell must continue it. */
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    wl_columnar_filter_context_t *filter = NULL;
    CHECK(session->cleanup_active &&
        !wl_columnar_eval_stack_framed_context(session->cleanup_active,
        (wl_col_session_t *)session, &ctx) &&
        !wl_columnar_eval_stack_filter_context_owned(ctx->frame, &filter,
        &ctx->child));
    bool right = filter->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE;
    if (right ? filter->join_right_initialized :
        filter->stage == WL_COLUMNAR_FILTER_STAGE_SOURCE &&
        filter->source_initialized) {
        const wl_columnar_relation_scalar_source_t *source =
            right ? &filter->join_right : &filter->source;
        CHECK(source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_ENDING &&
            source->end_cursor >= latch_end_cursor &&
            source->end_cursor <= 4);
        latch_end_cursor = source->end_cursor;
        latch_end_continues = true;
    } else {
        CHECK(filter->stage == WL_COLUMNAR_FILTER_STAGE_DECIDE ||
            filter->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE ||
            filter->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_PHYSICAL);
        ++latch_end_drained; latch_end_cursor = 0;
    }
}
static void hook(fixture_t *f){
    active = f; actual_latch_cells = 0;
    latch_scalar_end = latch_end_continues = false;
    latch_end_cells = latch_plain_cells = latch_end_drained = 0;
    latch_end_cursor = 0;
    wl_columnar_session_test_framed_grant_limit = grant_limit;
    wl_columnar_session_test_framed_grant_finished = grant_finished;
    wl_session_set_delta_cb(f->bounded, event, f);
    wl_columnar_expr_test_checkpoint = observe;
    wl_columnar_ops_test_reduce_attempt_checkpoint = reduce_observe;
    wl_columnar_session_test_head_scalar_checkpoint = head_observe;
}
static void add(fixture_t *f, unsigned key, const char *value){
    int64_t row[] = {key, wl_intern_put(f->plan->intern, value)};
    CHECK(!wl_session_insert(f->raw, "input", row, 1,
        2) && !wl_session_insert(f->bounded, "input", row, 1, 2));
}
static void gate(fixture_t *f){
    if (f->mixed){
        int64_t keys[] = {1, 2, 3};
        CHECK(!wl_session_insert(f->raw, "gate", keys, 3,
            1) && !wl_session_insert(f->bounded, "gate", keys, 3, 1));
    }
}
static void closed(fixture_t *f){
    wl_col_session_t *s = COL_SESSION(f->bounded);
    CHECK(!s->cleanup_active && !s->cleanup_pending &&
        !s->cleanup_reserved_bytes && !s->delta_observer);
}
static void compare(fixture_t *f){
    const col_rel_t *a = session_find_rel(COL_SESSION(f->bounded), "out"),
        *b = session_find_rel(COL_SESSION(f->raw), "out");
    CHECK(a && b && a->nrows == b->nrows && a->ncols == f->width &&
        b->ncols == f->width);
    int expected[4][64] = {{0}};
    for (unsigned c = 0; c < f->width;
        ++c) CHECK(a->column_types && b->column_types &&
            a->column_types[c] == b->column_types[c]);
    for (unsigned r = 0; r < a->nrows; ++r) {
        unsigned found = 0; for (unsigned t = 0; t < b->nrows; ++t){
            bool equal = true;
            for (unsigned c = 0; c < f->width;
                ++c) equal = equal && a->columns[c][r] == b->columns[c][t];
            found += equal;
        }
        CHECK(found == 1);
        int64_t group = f->width == 2?a->columns[0][r]:0,
            value = a->columns[f->width-1][r];
        CHECK(group >= 0 && group < 4 && value >= 0 && value < 64);
        CHECK(!expected[group][value]); expected[group][value] = 1;
    }
    CHECK(!memcmp(expected, f->balance, sizeof(expected))); closed(f);
}
static void destroy(fixture_t *f){
    active = NULL; wl_columnar_session_test_framed_grant_limit = NULL;
    wl_columnar_session_test_framed_grant_finished = NULL;
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_ops_test_reduce_attempt_checkpoint = NULL;
    wl_columnar_session_test_head_scalar_checkpoint = NULL;
    wl_session_destroy(f->raw); wl_session_destroy(f->bounded);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
}
static void positives(void){
    const char *functions[] = {"sum", "min", "max"}; for (unsigned fn = 0;
        fn < 3;
        ++fn) for (unsigned grouped = 0; grouped < 2;
            ++grouped) for (unsigned mixed = 0;
                mixed < 2; ++mixed){
                fixture_t f = create(functions[fn], grouped, mixed,
                        "strlen(s)"); hook(&f); gate(&f); add(&f, 1, "a");
                add(&f, 1, "bb"); add(&f, 2, "ccc");
                CHECK(!complete(&f) && !wl_session_step(f.raw)); compare(&f);
                CHECK(f.compile && f.eval && f.auth);
                CHECK(f.balance[grouped?1:0][fn == 0?(grouped?3:6):fn ==
                    1?1:grouped?2:3] == 1);
                int64_t row[] = {1, wl_intern_put(f.plan->intern, "bb")};
                CHECK(!wl_session_remove(f.raw, "input", row, 1,
                    2) && !wl_session_remove(f.bounded, "input", row, 1, 2));
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(&f) && !wl_session_step(f.raw)); compare(&f);
                add(&f, 1, "bb");
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(&f) && !wl_session_step(f.raw)); compare(&f);
                unsigned events = f.events;
                CHECK(!wl_session_step(f.bounded) && events == f.events);
                destroy(&f);
            }
}
static void latch_epoch(fixture_t *f){
    wl_col_session_t *session = COL_SESSION(f->bounded);
    if (wl_columnar_session_test_native_error_phase(session) !=
        WL_COLUMNAR_SESSION_NATIVE_ERROR_CHILD ||
        !session->cleanup_active) return;
    wl_columnar_eval_serial_framed_attempt_t *parent = NULL;
    if (wl_columnar_eval_stack_framed_context(session->cleanup_active, session,
        &parent)) return;
    wl_columnar_eval_plan_numeric_attempt_t *record = &parent->child;
    if (record->phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) return;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(parent->frame, &ctx,
        record));
    CHECK(ctx->stage < 64); uint64_t bit = UINT64_C(1)<<ctx->stage;
    if (seen_epochs&bit)return; seen_epochs |= bit;
    const wl_plan_op_t *actual = ctx->join_operation;
    wl_plan_op_t foreign = {.op = WL_PLAN_OP_REDUCE};
    /* Separate genuine financiers prove the new fixed cut without advancing
     * cleanup.88 + proof(5*192+128) +112 +256 =1544. */
    for (unsigned mode = 0; mode < 6; ++mode) {
        int owner = 0; wl_evaluation_control_t *funding = NULL;
        CHECK(!wl_evaluation_control_create(mode?1544:1543,
            &funding) && !wl_evaluation_control_attach(funding,
            &owner) && !wl_evaluation_control_begin(funding, &owner));
        wl_columnar_filter_stage_t saved_stage = ctx->stage;
        bool saved_pending = ctx->join_decision_pending;
        wl_plan_op_t *current =
            (wl_plan_op_t *)&record->plan->ops[record->cursor];
        wl_plan_op_type_t saved_op = current->op;
        if (mode == 1)ctx->join_operation = &foreign;
        if (mode == 2) {
            if (actual) ctx->join_operation = NULL;
            else ctx->join_decision_pending = true;
        }
        if (mode == 3)current->op = WL_PLAN_OP_JOIN;
        if (mode == 4)ctx->stage = WL_COLUMNAR_FILTER_STAGE_DECIDE;
        bool bare = true;
        int rc =
            wl_columnar_filter_unary_native_cleanup_latch_owned(parent->frame,
                record, funding,
                wl_evaluation_control_active_attempt_id(funding), &bare);
        ctx->join_operation = actual;
        ctx->join_decision_pending = saved_pending; ctx->stage = saved_stage;
        current->op = saved_op;
        CHECK(rc ==
            (mode == 0?WL_EVALUATION_CONTROL_WORK_LIMIT:mode < 5?EINVAL:0));
        CHECK(mode != 5 || !bare);
        CHECK(wl_evaluation_control_finish(funding, &owner, 0,
            0) == (mode == 0?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(funding,
            &report) && report.consumed == (mode == 0?1288u:1544u));
        CHECK(!wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
    }
}
static void stops(void){
    const unsigned phases[] = {70, 83, 84, 88, 550, 551, 552, 553, 554, 555,
                               564};
    for (unsigned mixed = 0; mixed < 2; ++mixed)for (unsigned p = 0;
            p < sizeof(phases)/sizeof(phases[0]); ++p) for (action = 0;
                action < 5; ++action){
                fixture_t f = create("sum", true, mixed, "strlen(s)"); hook(&f);
                gate(&f); add(&f, 1, "a");
                CHECK(!complete(&f) && !wl_session_step(f.raw)); compare(&f);
                col_rel_t *old = session_find_rel(COL_SESSION(f.bounded),
                        "out");
                col_relation_snapshot_t image =
                    wl_columnar_relation_snapshot(old);
                unsigned events = f.events;
                add(&f, 1, "bb"); add(&f, 2, "ccc");
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control,
                    action == 1?UINT64_MAX:0));
                probe_epochs = !mixed && p == 1 && action == 2; seen_epochs = 0;
                stop_phase = phases[p]; stop_at = p == 0 || p >= 4?1:2;
                hits = 0; stopped = false; successor = 0; int rc = complete(&f);
                stop_phase = 0; probe_epochs = false;
                if (!mixed && p == 1 && action == 2) {
                    const uint64_t required = (UINT64_C(1)<<
                        WL_COLUMNAR_FILTER_STAGE_JOIN_PHYSICAL)|(UINT64_C(1)<<
                        WL_COLUMNAR_FILTER_STAGE_PROGRAM)|(UINT64_C(1)<<
                        WL_COLUMNAR_FILTER_STAGE_SOURCE)|(UINT64_C(1)<<
                        WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE)|(UINT64_C(1)<<
                        WL_COLUMNAR_FILTER_STAGE_CONTROL)|(UINT64_C(1)<<
                        WL_COLUMNAR_FILTER_STAGE_RESET);
                    CHECK((seen_epochs&required) == required);
                }
                /* Each retirement drain reaches a real scalar-reader cell on
                 * both routes: both priced cell kinds must occur, and every
                 * started END must have been carried to a drained reader. */
                if ((action == 2 || action == 3) && p > 0 &&
                    p < 4) CHECK(actual_latch_cells && latch_end_cells &&
                        latch_plain_cells && latch_end_drained &&
                        !latch_end_continues);
                int expected = action ==
                    0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                    1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                    4?EOVERFLOW:EINVAL;
                if (rc != expected) fprintf(stderr,
                        "mixed%u phase%u action%u rc%d expected%d\n", mixed,
                        phases[p], action, rc, expected);
                CHECK(stopped && rc == expected && f.events == events &&
                    session_find_rel(COL_SESSION(f.bounded),
                    "out") == old && wl_columnar_relation_snapshot_equal(image,
                    wl_columnar_relation_snapshot(old))); closed(&f);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report));
                if (action == 2 || action == 3) {
                    CHECK(report_equal(&frozen, &report)); if (action == 3){
                        uint64_t used = 999;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.control, successor, &used) && !used);
                        wl_evaluation_control_attempt_finish_result_t end =
                            wl_evaluation_control_finish_attempt(f.control,
                                f.bounded->operation_admission, successor, 0,
                                0);
                        CHECK(!end.protocol && end.report_valid &&
                            !end.report.consumed);
                    }
                }else CHECK(report.result == expected &&
                        !report.cleanup_status);
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control,
                    0) && !complete(&f) && !wl_session_step(f.raw));
                compare(&f); destroy(&f);
            }
}
static void ignored_count(void){
    for (unsigned malformed = 0; malformed < 2; ++malformed) {
        fixture_t f = create("count", true, false, "strlen(s)"); hook(&f);
        if (malformed)for (unsigned i = 0; i < f.plan->stratum_count; ++i)
                for (unsigned j = 0; j < f.plan->strata[i].relation_count;
                    ++j){
                    const wl_plan_relation_t *h =
                        &f.plan->strata[i].relations[j];
                    for (unsigned k = 0; k < h->op_count;
                        ++k) if (h->ops[k].op == WL_PLAN_OP_REDUCE){
                            CHECK(h->ops[k].agg_expr.data &&
                                h->ops[k].agg_expr.size);
                            h->ops[k].agg_expr.data[0] = 255;
                        }
                }
        CHECK(!complete(&f) && !wl_session_step(f.raw)); compare(&f);
        CHECK(!f.compile && !f.eval);
        CHECK(!wl_evaluation_control_reset(f.control));
        add(&f, 1, "a"); add(&f, 1, "bb"); add(&f, 2, "ccc");
        uint64_t symbols = wl_intern_count(f.plan->intern);
        CHECK(!complete(&f) && !wl_session_step(f.raw)); compare(&f);
        CHECK(f.balance[1][2] == 1 && f.balance[2][1] == 1 && !f.compile &&
            !f.eval && wl_intern_count(f.plan->intern) == symbols);
        destroy(&f);
    }
}
static void errors_and_allocations(void){
    fixture_t f = create("sum", true, false, "to_number(s)"); hook(&f);
    add(&f, 1, "2"); add(&f, 1, "3");
    CHECK(!complete(&f) && !wl_session_step(f.raw)); compare(&f);
    CHECK(f.balance[1][5] == 1);
    col_rel_t *old = session_find_rel(COL_SESSION(f.bounded), "out");
    col_relation_snapshot_t image = wl_columnar_relation_snapshot(old);
    unsigned events = f.events;
    add(&f, 2, "9223372036854775808");
    CHECK(!wl_evaluation_control_reset(f.control));
    int bounded_error = complete(&f), raw_error = wl_session_step(f.raw);
    CHECK(bounded_error == ERANGE && raw_error == ERANGE);
    CHECK(f.events == events && session_find_rel(COL_SESSION(f.bounded),
        "out") == old && wl_columnar_relation_snapshot_equal(image,
        wl_columnar_relation_snapshot(old))); closed(&f);
    int64_t row[] = {2, wl_intern_put(f.plan->intern, "9223372036854775808")};
    CHECK(!wl_session_remove(f.bounded, "input", row, 1,
        2) && !wl_session_remove(f.raw, "input", row, 1, 2));
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(&f) &&
        !wl_session_step(f.raw)); compare(&f); destroy(&f);
    for (unsigned family = 0; family < 2; ++family) {
        f = create("sum", true, false, "strlen(s)"); hook(&f); add(&f, 1, "a");
        CHECK(!complete(&f) && !wl_session_step(f.raw)); compare(&f);
        add(&f, 2, "bb");
        old = session_find_rel(COL_SESSION(f.bounded), "out");
        image = wl_columnar_relation_snapshot(old); events = f.events;
        for (unsigned repeat = 0; repeat < 3; ++repeat) {
            CHECK(!wl_evaluation_control_reset(f.control));
            if (family)wl_columnar_expr_test_fail_allocation = 0;
            else wl_columnar_ops_test_reduce_attempt_fail_allocation = 0;
            int rc = complete(&f); wl_columnar_expr_test_fail_allocation = -1;
            wl_columnar_ops_test_reduce_attempt_fail_allocation = -1;
            CHECK(rc == ENOMEM && f.events == events &&
                session_find_rel(COL_SESSION(f.bounded),
                "out") == old && wl_columnar_relation_snapshot_equal(image,
                wl_columnar_relation_snapshot(old))); closed(&f);
        }
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(&f) &&
            !wl_session_step(f.raw)); compare(&f); destroy(&f);
    }
}
static void numeric_classifier(void){
    fixture_t f = create("sum", false, false, "k");
    const wl_plan_expr_buffer_t *expr = NULL;
    for (unsigned i = 0; i < f.plan->stratum_count; ++i)for (unsigned j = 0;
            j < f.plan->strata[i].relation_count; ++j){
            const wl_plan_relation_t *h = &f.plan->strata[i].relations[j];
            for (unsigned k = 0;
                k < h->op_count;
                ++k) if (h->ops[k].op ==
                    WL_PLAN_OP_REDUCE) expr = &h->ops[k].agg_expr;
        }
    const uint8_t bytes[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'};
    CHECK(expr && expr->size == sizeof(bytes) && !memcmp(expr->data, bytes,
        sizeof(bytes)));
    const uint64_t fees[] = {224, 224, 224, 224, 224, 224, 224, 224, 224, 224,
                             192, 224, 224}; uint64_t prefix = 0;
    for (unsigned cut = 0; cut <= sizeof(fees)/sizeof(fees[0]); ++cut) {
        bool exact = cut == sizeof(fees)/sizeof(fees[0]);
        uint64_t limit = exact?2880:prefix+fees[cut]-1;
        int owner = 0; wl_evaluation_control_t *c = NULL;
        CHECK(!wl_evaluation_control_create(limit,
            &c) && !wl_evaluation_control_attach(c,
            &owner) && !wl_evaluation_control_begin(c, &owner));
        bool scalar = true;
        int rc = wl_columnar_expr_requires_scalar_attempt_controlled(expr->data,
                expr->size, &scalar, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == (exact?0:WL_EVALUATION_CONTROL_WORK_LIMIT) &&
            scalar == !exact);
        CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(c,
            &report) && report.consumed == (exact?UINT64_C(2880):prefix));
        CHECK(!wl_evaluation_control_detach(c, &owner));
        wl_evaluation_control_release(c); if (!exact) prefix += fees[cut];
    }
    CHECK(prefix == 2880); destroy(&f);
}
static unsigned forbidden_calls;
static int forbidden_callback(const wirelog_extension_value_t *args, uint32_t n,
    wirelog_extension_value_t *out, void *data){
    (void)args; (void)n; (void)out; (void)data; ++forbidden_calls; return -1;
}
static void find_boundary(wirelog_ir_node_t *node,
    wirelog_ir_node_t **aggregate, wirelog_ir_node_t **filter){
    if (node->type == WIRELOG_IR_AGGREGATE && node->agg_expr)*aggregate = node;
    if (node->type == WIRELOG_IR_FILTER && node->filter_expr &&
        node->filter_expr->type == WL_IR_EXPR_EXTENSION_CALL) *filter = node;
    for (unsigned i = 0; i < node->child_count;
        ++i) find_boundary(node->children[i], aggregate, filter);
}
static void compiler_boundary(void){
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create(); CHECK(registry);
    uint32_t argument = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "test.reduce", .arity = 1, .argument_types = &argument,
     .result_type = WIRELOG_EXTENSION_VALUE_BOOL, .invoke = forbidden_callback};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    wirelog_error_t error;
    wirelog_program_t *program =
        wirelog_parse_string(
        ".decl input(k:int64)\n.decl out(v:int64)\nout(sum(k)):-input(k),@call(\"test.reduce\",k).\n",
        &error);
    CHECK(program); wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program_with_snapshot(program, snapshot,
        &plan) && plan); wl_plan_free(plan); plan = NULL;
    wirelog_ir_node_t *aggregate = NULL, *filter = NULL;
    CHECK(program->rule_count == 1);
    find_boundary(program->rules[0].ir_root, &aggregate, &filter);
    CHECK(aggregate && filter && !strcmp(filter->filter_expr->extension_name,
        "test.reduce"));
    wl_ir_expr_t *original = aggregate->agg_expr;
    aggregate->agg_expr = filter->filter_expr; filter->filter_expr = original;
    CHECK(wl_plan_from_program_with_snapshot(program, snapshot,
        &plan) != 0 && !plan && !forbidden_calls && strstr(program->plan_error,
        "scalar extension call in aggregate expression is not supported"));
    wirelog_program_free(program); wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry,
        "test.reduce") && !wirelog_extension_registry_destroy(registry));
}
int main(void){
    numeric_classifier(); compiler_boundary(); positives(); ignored_count();
    errors_and_allocations(); stops();
    puts("bounded scalar REDUCE hosts passed"); return 0;
}
