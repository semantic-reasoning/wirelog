/* #2299 actual compiler-selected scalar host integration. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/wirelog-extension.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/ir.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); exit(1); } } while (0)
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
extern void (*wl_columnar_join_test_attempt_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
extern void (*wl_columnar_session_test_head_scalar_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t);
extern void (*wl_columnar_filter_test_map_schema)(const col_rel_t *, bool);
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
extern int wl_evaluation_control_test_active_consumed(wl_evaluation_control_t *,
    uint64_t, uint64_t *);
typedef struct {
    wirelog_program_t *program; wl_plan_t *plan;
    wl_session_t *session, *raw; wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    unsigned probe;
    wirelog_column_type_t map_type;
    unsigned width, events, positive, negative, evals, compile, selected, auth,
        empty, descriptions, observer_events, unknown_maps;
    int balance[64]; bool numeric, product, right_active, map, map_unobserved;
} host_t;
static host_t *active;
static wl_session_t *map_callback_session;
static unsigned map_extension_calls, map_extension_destroyed;
static bool map_extension_fail;
extern int wl_columnar_expr_test_fail_allocation;
static bool map_allocation_fail, map_allocation_injected;
static unsigned map_snapshot_rows;
static unsigned stop_phase, stop_at, action, hits;
static bool stopped, description;
static uint64_t successor;
static wl_evaluation_control_report_t frozen;
static int complete(wl_session_t *session){
    if (active && active->session == session)active->right_active = false;
    if (active && active->map)map_callback_session = session;
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = wl_session_step(session); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static unsigned code(const int64_t *row, unsigned width){
    if (active && active->map) {
        CHECK(width == 1 && row[0] >= 0 && row[0] < 64);
        return (unsigned)row[0];
    }
    CHECK(row[0] >= 1 && row[0] <= 4);
    if (width == 1)return (unsigned)row[0];
    CHECK(width == 2 && row[1] >= 1 && row[1] <= 4);
    return (unsigned)(row[0]*8+row[1]);
}
static void event(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data){
    host_t *f = data;
    if (f->map && COL_SESSION(f->session)->delta_observer)++f->observer_events;
    CHECK(!strcmp(name,
        "out") && width == f->width && (diff == 1 || diff == -1));
    unsigned k = code(row, width); f->balance[k] += diff;
    CHECK(f->balance[k] >= 0 && f->balance[k] <= 1);
    ++f->events; f->positive += diff == 1; f->negative += diff == -1;
}
static uint64_t image(const col_rel_t *r, unsigned width){
    CHECK(r && r->ncols == width); uint64_t mask = 0;
    for (uint32_t i = 0; i < r->nrows; ++i) {
        int64_t row[2] = {r->columns[0][i], width == 2?r->columns[1][i]:0};
        unsigned k = code(row, width); CHECK(!(mask&(UINT64_C(1)<<k)));
        mask |= UINT64_C(1)<<k;
    }
    return mask;
}
static void compare(host_t *f){
    const col_rel_t *a = session_find_rel(COL_SESSION(f->session), "out"),
        *b = session_find_rel(COL_SESSION(f->raw), "out");
    CHECK(a && b && a->nrows == b->nrows && image(a, f->width) == image(b,
        f->width));
    for (unsigned c = 0; c < f->width; ++c) {
        if (f->map) {
            /* Final canonical copies may spell implicit INT64 explicitly;
            * operator UNKNOWN is separately asserted before that copy. */
            wirelog_column_type_t at =
                a->column_types?a->column_types[c]:WIRELOG_TYPE_INT64;
            wirelog_column_type_t bt =
                b->column_types?b->column_types[c]:WIRELOG_TYPE_INT64;
            if (at != bt)fprintf(stderr,
                    "MAP finalschema rows%u/%u expectedoutput%d actual%d/raw%d types%p/%p\n",
                    a->nrows, b->nrows, f->map_type, at, bt,
                    (void *)a->column_types, (void *)b->column_types);
            CHECK(at == bt);
        } else CHECK(a->column_types && b->column_types &&
                a->column_types[c] == b->column_types[c]);
    }
    uint64_t mask = image(a, f->width);
    if (f->map_unobserved)CHECK(f->map && !f->events &&
            !COL_SESSION(f->session)->delta_cb);
    else for (unsigned k = 0; k < 64;
            ++k) CHECK(f->balance[k] == (int)((mask>>k)&1));
}
static void observe(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed){
    (void)progress; (void)failed; host_t *f = active;
    if (!f || control != f->control) return;
    if (f->map_unobserved) {
        /* Existing C1/plain path has its own frame/retirement protocol;
        * observed unary birth is intentionally not inferred from it. */
        if (phase == 83 ||
            (f->numeric && (phase == 20 || phase == 7))) ++f->evals;
        if (phase == 70)++f->compile;
        return;
    }
    if (f->map) {
        wl_columnar_eval_serial_framed_attempt_t *scope = NULL;
        wl_columnar_eval_stack_cleanup_frame_t *frame =
            COL_SESSION(f->session)->cleanup_active;
        f->right_active = frame && !wl_columnar_eval_stack_framed_context(frame,
                COL_SESSION(f->session), &scope)
            && scope->child.composed_mode &&
            scope->child.cursor < scope->child.end
            && scope->child.cursor < scope->child.plan->op_count
            && scope->child.plan->ops[scope->child.cursor].op == WL_PLAN_OP_MAP
            && (scope->child.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
            || (scope->child.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
            && (scope->child.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
            || scope->child.pending_action ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY)));
    }
    if (f->map && f->right_active && f->numeric && phase == 20) {
        CHECK(wl_columnar_session_test_cleanup_owners(COL_SESSION(
                f->session)) == 2);
        ++f->auth;
    }
    if (!f->right_active) {
        if (!description) return;
        CHECK(!COL_SESSION(f->session)->cleanup_active &&
            !wl_columnar_session_test_cleanup_owners(COL_SESSION(f->session)));
    }
    if (f->map && f->right_active && phase == 70 && map_allocation_fail) {
        CHECK(wl_columnar_session_test_cleanup_owners(COL_SESSION(
                f->session)) == 2);
        wl_columnar_expr_test_fail_allocation = 0; map_allocation_fail = false;
        map_allocation_injected = true;
    }
    if (f->right_active && !f->numeric &&
        (phase == 70 ||
        phase ==
        83)) CHECK(wl_columnar_session_test_cleanup_owners(COL_SESSION(
                f->session)) == (f->map?2u:1u));
    if (phase == 70 && (!f->map || f->right_active))++f->compile;
    if ((phase == 83 && (!f->map || f->right_active))
        || (f->map && f->right_active && phase == 70)) {
        if (phase == 83)++f->evals;
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(COL_SESSION(f->session)->cleanup_active &&
            !wl_columnar_eval_stack_framed_context(COL_SESSION(
                f->session)->cleanup_active, COL_SESSION(f->session), &ctx) &&
            !wl_columnar_eval_stack_framed_authenticate(ctx->frame,
            COL_SESSION(f->session), ctx));
        CHECK(ctx->session == COL_SESSION(f->session) && ctx->control == control
            && ctx->attempt_id ==
            wl_evaluation_control_active_attempt_id(control)
            && ctx->capture_only && ctx->private_bindings &&
            ctx->abort_supported
            && ctx->cleanup_receipt_key.receipt_id
            && wl_columnar_session_observed_bindings_authorized(COL_SESSION(
                f->session), ctx->private_bindings));
        ++f->auth;
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
                f->session->operation_admission, id, 0, 0);
        CHECK(!end.protocol && end.report_valid); frozen = end.report;
        if (action == 3) {
            CHECK(!wl_evaluation_control_reset(control));
            wl_evaluation_control_attempt_begin_result_t next =
                wl_evaluation_control_begin_attempt(control,
                    f->session->operation_admission);
            CHECK(!next.protocol && next.attempt_id != id);
            successor = next.attempt_id;
        }
    }else CHECK(wl_evaluation_control_charge(control, UINT64_MAX) == EOVERFLOW);
}
static void head_observe(wl_evaluation_control_t *c, unsigned phase,
    uint32_t progress){
    if (active && c == active->control && phase == 560)++active->descriptions;
    observe(c, phase, progress, false);
}
static void join_observe(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress){
    (void)progress; if (active && c == active->control) {
        if (phase == 43)active->right_active = true;
        if (phase == 24)active->right_active = false;
        if (phase == 46)++active->selected;
        if (phase == 47)++active->empty;
    }
}
static host_t create_kind(bool numeric, bool product, unsigned probe){
    host_t f = {.numeric = numeric, .product = product, .probe = probe,
                .width = product && !probe ? 2u : 1u};
    char source[600];
    snprintf(source, sizeof(source),
        ".decl left(k:int64)\n.decl right(k:int64,s:%s)\n.decl out(%s)\nout(%s):-left(%s),%sright(%s,%s).\n",
        numeric?"int64":"string", product && !probe?"x:int64,k:int64":"k:int64",
        product && !probe?"x,k":"k", product && !probe?"x":"k", probe?"!":"",
        product?(probe?"_":"k"):"k", numeric?"1":"\"yes\"");
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    const wl_plan_relation_t *h = &f.plan->strata[0].relations[0];
    wl_plan_op_t *join = NULL;
    for (unsigned i = 0; i < h->op_count;
        ++i) if (h->ops[i].op ==
            (probe ? WL_PLAN_OP_ANTIJOIN : WL_PLAN_OP_JOIN)) join =
                (wl_plan_op_t *)&h->ops[i];
    CHECK(join && join->right_filter_expr.data &&
        join->key_count == (product?0u:1u));
    if (probe == 2)join->op = WL_PLAN_OP_SEMIJOIN; /* Explicit internal-plan witness. */
    const wl_plan_expr_buffer_t *e = &join->right_filter_expr;
    CHECK(e->size == (numeric?17u:14u) && e->data[0] == WL_PLAN_EXPR_VAR
        && e->data[7] ==
        (numeric?WL_PLAN_EXPR_CONST_INT:WL_PLAN_EXPR_CONST_STR));
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
        &options, &f.session));
    return f;
}
static host_t create(bool numeric, bool product){
    return create_kind(numeric, product, 0);
}
static void map_schema(const col_rel_t *r, bool known){
    if (active && active->map) {
        if (!known) {
            CHECK(r && !r->column_types); ++active->unknown_maps;
        }else CHECK(r && r->column_types &&
                r->column_types[0] == active->map_type);
    }
}
static void hook(host_t *f){
    active = f; wl_columnar_filter_test_map_schema = map_schema;
    wl_session_set_delta_cb(f->session, event, f);
    wl_columnar_expr_test_checkpoint = observe;
    wl_columnar_join_test_attempt_checkpoint = join_observe;
    wl_columnar_session_test_head_scalar_checkpoint = head_observe;
}
static void add(host_t *f, unsigned k, bool yes){
    int64_t row[] = {k,
                     f->numeric?(yes?1:0):wl_intern_put(f->plan->intern,
                         yes?"yes":"no")};
    CHECK(!wl_session_insert(f->session, "right", row, 1,
        2) && !wl_session_insert(f->raw, "right", row, 1, 2));
}
static void left(host_t *f){
    int64_t rows[] = {1, 2, 3, 4};
    CHECK(!wl_session_insert(f->session, "left", rows, 4,
        1) && !wl_session_insert(f->raw, "left", rows, 4, 1));
}
static void closed(host_t *f){
    CHECK(!COL_SESSION(f->session)->cleanup_active &&
        !COL_SESSION(f->session)->cleanup_pending &&
        !COL_SESSION(f->session)->cleanup_reserved_bytes &&
        !COL_SESSION(f->session)->delta_observer);
}
static void destroy(host_t *f){
    active = NULL; wl_columnar_filter_test_map_schema = NULL;
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_join_test_attempt_checkpoint = NULL;
    wl_columnar_session_test_head_scalar_checkpoint = NULL;
    wl_session_destroy(f->session); wl_session_destroy(f->raw);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
}
static void numeric_right_oracle(void){
    host_t f = create(true, false);
    const wl_plan_relation_t *head = &f.plan->strata[0].relations[0];
    const wl_plan_expr_buffer_t *expr = NULL;
    for (uint32_t i = 0; i < head->op_count;
        ++i) if (head->ops[i].op ==
            WL_PLAN_OP_JOIN) expr = &head->ops[i].right_filter_expr;
    CHECK(expr && expr->size == 17);
    /* Independent literal grammar schedule for actual VAR col1/CONST1/EQ.
     * ENTRY; VAR PRE/DECODE/tag/len2/name4/digit/POST;
     * CONST PRE/DECODE/tag/payload8/POST; EQ PRE/DECODE/tag/POST; FINAL.
     * The one digit costs192; all other28 positive cuts cost224. */
    const uint64_t fees[] = {224,
                             224, 224, 224, 224, 224, 224, 224, 224, 224, 192,
                             224,
                             224, 224, 224, 224, 224, 224, 224, 224, 224, 224,
                             224, 224,
                             224, 224, 224, 224, 224};
    uint64_t prefix = 0;
    for (unsigned cut = 0; cut <= sizeof(fees)/sizeof(fees[0]); ++cut) {
        bool exact = cut == sizeof(fees)/sizeof(fees[0]);
        uint64_t limit = exact?6464:prefix+fees[cut]-1;
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
            &report) && report.consumed == (exact?UINT64_C(6464):prefix));
        CHECK(!wl_evaluation_control_detach(c, &owner));
        wl_evaluation_control_release(c);
        if (!exact)prefix += fees[cut];
    }
    CHECK(prefix == 6464); destroy(&f);
}
static void positives(void){
    for (unsigned numeric = 0; numeric < 2; ++numeric)for (unsigned product = 0;
            product < 2; ++product){
            host_t f = create(numeric, product); hook(&f); left(&f);
            add(&f, 1, true); add(&f, 2, false); add(&f, 3, true);
            CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
            CHECK(f.events == (product?8u:2u) && f.selected == 2);
            CHECK(numeric?(!f.evals && !f.compile):(f.evals == 3 &&
                f.auth == 3 && f.compile));
            int64_t removed[] = {1,
                                 numeric?1:wl_intern_put(f.plan->intern,
                                     "yes")};
            CHECK(!wl_session_remove(f.session, "right", removed, 1,
                2) && !wl_session_remove(f.raw, "right", removed, 1, 2));
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !complete(f.session) && !wl_session_step(f.raw)); compare(&f);
            CHECK(f.negative == (product?4u:1u)); add(&f, 1, true);
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !complete(f.session) && !wl_session_step(f.raw)); compare(&f);
            unsigned events = f.events;
            CHECK(!wl_session_step(f.session) && f.events == events);
            closed(&f); destroy(&f);
        }
}
static void empty_and_invalid(void){
    for (unsigned empty = 0; empty < 2; ++empty) {
        host_t f = create(false, false); hook(&f); left(&f); add(&f, 1, true);
        if (empty) {
            int64_t row[] = {1, wl_intern_put(f.plan->intern, "yes")};
            CHECK(!wl_session_remove(f.session, "right", row, 1,
                2) && !wl_session_remove(f.raw, "right", row, 1, 2));
            CHECK(!session_find_rel(COL_SESSION(f.session), "right")->nrows);
        }else {
            int64_t bad[] = {3, -1};
            CHECK(!wl_session_insert(f.session, "right", bad, 1,
                2) && !wl_session_insert(f.raw, "right", bad, 1, 2));
        }
        uint64_t intern_before = wl_intern_count(f.plan->intern);
        CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
        CHECK(f.events == (empty?0u:1u) &&
            !COL_SESSION(f.session)->extension_expr_status);
        if (empty)CHECK(f.empty && !f.evals &&
                wl_intern_count(f.plan->intern) == intern_before);
        closed(&f); destroy(&f);
    }
}
static unsigned forbidden_calls;
static int forbidden_callback(const wirelog_extension_value_t *args, uint32_t n,
    wirelog_extension_value_t *out, void *data){
    (void)args; (void)n; (void)out; (void)data; ++forbidden_calls; return -1;
}
static bool right_extension_ir(wirelog_ir_node_t **slot){
    wirelog_ir_node_t *node = *slot;
    if (node->type == WIRELOG_IR_FILTER && node->child_count == 1 &&
        node->children[0]->type == WIRELOG_IR_JOIN){
        wirelog_ir_node_t *join = node->children[0];
        CHECK(join->child_count == 2);
        node->children[0] = join->children[1]; join->children[1] = node;
        *slot = join; return true;
    }
    for (uint32_t i = 0; i < node->child_count;
        ++i) if (right_extension_ir(&node->children[i])) return true;
    return false;
}
static void compiler_boundary(void){
    wirelog_extension_registry_t *r = wirelog_extension_registry_create();
    CHECK(r);
    uint32_t type = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t d =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(d),
     .name = "test.right", .arity = 1, .argument_types = &type,
     .result_type = WIRELOG_EXTENSION_VALUE_BOOL, .invoke = forbidden_callback};
    CHECK(!wirelog_extension_register(r, &d));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(r); CHECK(snapshot);
    const char *source =
        ".decl left(k:int64)\n.decl right(k:int64,b:int64)\n.decl out(k:int64)\nout(k):-left(k),right(k,b),@call(\"test.right\",b).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program_with_snapshot(program, snapshot,
        &plan) && plan); wl_plan_free(plan); plan = NULL;
    /* A valid parsed direct call is legal as ordinary FILTER. Moving that
     * FILTER to the JOIN right input exercises the compiler's exact IR
     * capability boundary, not a parser error or executed callback. */
    CHECK(program->rule_count == 1 &&
        right_extension_ir(&program->rules[0].ir_root));
    CHECK(wl_plan_from_program_with_snapshot(program, snapshot,
        &plan) != 0 && !plan && !forbidden_calls
        && strstr(program->plan_error, "right-filter"));
    wirelog_program_free(program); wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(r,
        "test.right") && !wirelog_extension_registry_destroy(r));
}
static bool report_equal(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b){
    return a->attempt_id == b->attempt_id && a->consumed == b->consumed &&
           a->limit == b->limit && a->result == b->result &&
           a->execution_status == b->execution_status &&
           a->cleanup_status == b->cleanup_status &&
           a->stop_reason == b->stop_reason;
}
static void probe_positives(void){
    for (unsigned kind = 1; kind <= 2; ++kind)for (unsigned numeric = 0;
            numeric < 2;
            ++numeric)
            for (unsigned keyless = 0; keyless < 2; ++keyless) {
                host_t f = create_kind(numeric, keyless, kind); hook(&f);
                left(&f);
                add(&f, 1, false); add(&f, 1, true); add(&f, 2, false);
                add(&f, 3, true); add(&f, 3, false);
                CHECK(!complete(f.session) && !wl_session_step(f.raw));
                compare(&f);
                uint64_t expected = keyless?(kind == 1?0:30):(kind == 1?20:10);
                CHECK(image(session_find_rel(COL_SESSION(f.session), "out"),
                    1) == expected && f.selected == 2);
                int64_t row[] = {1,
                                 numeric?1:wl_intern_put(f.plan->intern,
                                     "yes")};
                CHECK(!wl_session_remove(f.session, "right", row, 1,
                    2) && !wl_session_remove(f.raw, "right", row, 1, 2));
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.session) && !wl_session_step(f.raw));
                compare(&f);
                row[0] = 3;
                CHECK(!wl_session_remove(f.session, "right", row, 1,
                    2) && !wl_session_remove(f.raw, "right", row, 1, 2));
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.session) && !wl_session_step(f.raw));
                compare(&f);
                CHECK(image(session_find_rel(COL_SESSION(f.session), "out"),
                    1) == (kind == 1?30u:0u));
                add(&f, 1, true);
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.session) && !wl_session_step(f.raw));
                compare(&f);
                unsigned events = f.events;
                CHECK(!wl_session_step(f.session) && f.events == events);
                closed(&f); destroy(&f);
            }
}
static void probe_empty(void){
    for (unsigned kind = 1; kind < 3; ++kind)for (unsigned keyless = 0;
            keyless < 2;
            ++keyless)
            for (unsigned empty = 0; empty < 2; ++empty) {
                host_t f = create_kind(false, keyless, kind); hook(&f);
                left(&f);
                add(&f, 1, false); add(&f, 2, false);
                if (empty) {
                    int64_t rows[] = {1, wl_intern_put(f.plan->intern, "no"), 2,
                                      wl_intern_put(f.plan->intern, "no")};
                    CHECK(!wl_session_remove(f.session, "right", rows, 2,
                        2) && !wl_session_remove(f.raw, "right", rows, 2, 2));
                }
                CHECK(!complete(f.session) && !wl_session_step(f.raw));
                compare(&f);
                CHECK(image(session_find_rel(COL_SESSION(f.session), "out"),
                    1) == (kind == 1?30u:0u));
                CHECK(empty?f.empty && !f.evals:f.evals > 0);
                closed(&f); destroy(&f);
            }
}
static void probe_collisions(void){
    for (unsigned kind = 1; kind < 3; ++kind) {
        host_t f = create_kind(false, false, kind); hook(&f); left(&f);
        const col_rel_t *r = session_find_rel(COL_SESSION(f.session), "right");
        uint32_t bucket = wl_columnar_hash_value(2166136261u, r, 0, 1)&15u;
        unsigned excluded[2], found = 0;
        for (unsigned k = 5; found < 2 && k < 10000; ++k)
            if ((wl_columnar_hash_value(2166136261u, r, 0, k)&15u) == bucket)
                excluded[found++] = k;
        CHECK(found == 2);
        add(&f, excluded[0], false); add(&f, 1, true); add(&f, 1, false);
        add(&f, excluded[1], false); add(&f, 3, true); add(&f, 2, false);
        CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
        CHECK(image(session_find_rel(COL_SESSION(f.session), "out"),
            1) == (kind == 1?20u:10u));
        int64_t rows[] = {1, wl_intern_put(f.plan->intern, "yes"), 3,
                          wl_intern_put(f.plan->intern, "yes")};
        CHECK(!wl_session_remove(f.session, "right", rows, 2,
            2) && !wl_session_remove(f.raw, "right", rows, 2, 2));
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
            !wl_session_step(f.raw)); compare(&f);
        CHECK(image(session_find_rel(COL_SESSION(f.session), "out"),
            1) == (kind == 1?30u:0u));
        closed(&f); destroy(&f);
    }
}
static void sip_positive(void){
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl a(x:int64,y:int64)\n.decl b(y:int64,w:int64)\n"
        ".decl c(w:int64,z:int64,s:string)\n.decl path(x:int64,z:int64)\n"
        "path(x,z):-a(x,y),b(y,w),c(w,z,s).\n", &error);
    CHECK(program); wl_sip_stats_t stats = {0};
    CHECK(!wl_sip_apply(program, &stats));
    wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
    unsigned semi = 0;
    for (unsigned i = 0; i < plan->stratum_count; ++i)
        for (unsigned j = 0; j < plan->strata[i].relation_count; ++j) {
            const wl_plan_relation_t *h = &plan->strata[i].relations[j];
            for (unsigned k = 0; k < h->op_count; ++k)
                if (h->ops[k].op == WL_PLAN_OP_SEMIJOIN) {
                    ++semi; CHECK(!h->ops[k].right_filter_expr.size);
                }
        }
    CHECK(semi == 1);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options; wl_session_options_init(&options);
    wl_session_t *raw = NULL, *bounded = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &raw));
    options.evaluation_control = control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &bounded));
    int64_t a[] = {1, 2, 4, 5}, b[] = {2, 3, 5, 6},
        c[] = {3, 4, wl_intern_put(plan->intern, "yes")};
    wl_session_t *sessions[] = {raw, bounded};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(sessions[i], "a", a, 2,
            2) && !wl_session_insert(sessions[i], "b", b, 2,
            2) && !wl_session_insert(sessions[i], "c", c, 1, 3));
        CHECK(!complete(sessions[i]));
        const col_rel_t *r = session_find_rel(COL_SESSION(sessions[i]), "path");
        CHECK(r && r->nrows == 1 && r->ncols == 2 && r->columns[0][0] == 1 &&
            r->columns[1][0] == 4);
        CHECK(!COL_SESSION(sessions[i])->cleanup_active &&
            !COL_SESSION(sessions[i])->cleanup_reserved_bytes);
    }
    wl_session_destroy(raw); wl_session_destroy(bounded);
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
}
static void stops(void){
    const unsigned phases[] = {70, 83, 84, 88, 550, 551, 552, 553, 554, 555,
                               564};
    for (unsigned kind = 0; kind < 3; ++kind)
        for (unsigned product = 0; product < 2; ++product) for (unsigned p = 0;
                p < sizeof(phases)/sizeof(phases[0]); ++p) for (action = 0;
                    action < 5; ++action) {
                    host_t f = create_kind(false, product, kind); hook(&f);
                    left(&f);
                    add(&f, 1, true);
                    CHECK(!complete(f.session) && !wl_session_step(f.raw));
                    compare(&f);
                    col_rel_t *old = session_find_rel(COL_SESSION(f.session),
                            "out");
                    col_relation_snapshot_t old_image =
                        wl_columnar_relation_snapshot(old);
                    unsigned events = f.events;
                    add(&f, 2, false); add(&f, 3, true);
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control,
                        action == 1?UINT64_MAX:0));
                    description = p >= 4;
                    stop_phase = phases[p]; stop_at = description || !p?1:2;
                    hits = 0; stopped = false; successor = 0;
                    int rc = complete(f.session); stop_phase = 0;
                    description = false; CHECK(stopped);
                    int expected = action ==
                        0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                        1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                        4?EOVERFLOW:EINVAL;
                    if (rc != expected) {
                        wl_evaluation_control_report_t r;
                        int rr = wl_evaluation_control_report(f.control, &r);
                        fprintf(stderr,
                            "stop phase%u action%u rc%d expected%d owners%u active%p pending%p reserved%llu report%d/%d/%d\n",
                            phases[p], action, rc, expected,
                            wl_columnar_session_test_cleanup_owners(COL_SESSION(
                                f.session)),
                            (void *)COL_SESSION(f.session)->cleanup_active,
                            (void *)COL_SESSION(f.session)->cleanup_pending,
                            (unsigned long long)COL_SESSION(
                                f.session)->cleanup_reserved_bytes, rr,
                            r.result,
                            r.cleanup_status);
                    }
                    CHECK(rc == expected && f.events == events &&
                        session_find_rel(COL_SESSION(f.session),
                        "out") == old &&
                        wl_columnar_relation_snapshot_equal(old_image,
                        wl_columnar_relation_snapshot(old))); closed(&f);
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report));
                    if (action == 2 || action == 3) {
                        CHECK(report_equal(&frozen, &report));
                        if (action == 3) {
                            uint64_t used = 999;
                            CHECK(!wl_evaluation_control_test_active_consumed(
                                    f.control, successor, &used) && !used);
                            wl_evaluation_control_attempt_finish_result_t end =
                                wl_evaluation_control_finish_attempt(f.control,
                                    f.session->operation_admission, successor,
                                    0,
                                    0);
                            CHECK(!end.protocol && end.report_valid &&
                                !end.report.consumed);
                        }
                    }else CHECK(report.result == expected &&
                            !report.cleanup_status);
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control,
                        0) && !complete(f.session) && !wl_session_step(f.raw));
                    compare(&f); closed(&f); destroy(&f);
                }
}
static host_t create_map(const char *input_type, const char *output_type,
    const char *expression, bool inline_facts,
    wirelog_extension_snapshot_t *snapshot){
    host_t f = {.width = 1, .map = true,
                .map_type = !strcmp(output_type,
                        "string")?WIRELOG_TYPE_STRING:WIRELOG_TYPE_INT64};
    char source[800];
    snprintf(source, sizeof(source),
        ".decl input(x:%s)\n.decl out(x:%s)\n%sout(%s):-input(x).\n",
        input_type, output_type, inline_facts?"input(1).\n":"", expression);
    wirelog_error_t error; f.program = wirelog_parse_string(source, &error);
    CHECK(f.program && !wl_plan_from_program_with_snapshot(f.program, snapshot,
        &f.plan));
    const wl_plan_relation_t *head = &f.plan->strata[0].relations[0];
    CHECK(head->op_count == 3 && head->ops[0].op == WL_PLAN_OP_VARIABLE &&
        head->ops[1].op == WL_PLAN_OP_MAP &&
        head->ops[2].op == WL_PLAN_OP_CONSOLIDATE);
    CHECK(head->ops[1].project_count == 1 && head->ops[1].project_indices &&
        head->ops[1].project_indices[0] == 0);
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = UINT64_C(512)<<20, .usable_bytes = UINT64_C(512)<<20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f.governor;
    CHECK(!wl_session_create_with_snapshot_options(wl_backend_columnar(),
        f.plan, 1, snapshot, &options, &f.raw));
    CHECK(!wl_evaluation_control_create(0, &f.control));
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_snapshot_options(wl_backend_columnar(),
        f.plan, 1, snapshot, &options, &f.session));
    if (inline_facts)CHECK(!wl_session_load_facts(f.raw,
            f.program) && !wl_session_load_facts(f.session, f.program));
    return f;
}
static void map_input(host_t *f, int64_t value, bool remove){
    CHECK(!(remove?wl_session_remove(f->session, "input", &value, 1,
        1):wl_session_insert(f->session, "input", &value, 1, 1)));
    CHECK(!(remove?wl_session_remove(f->raw, "input", &value, 1,
        1):wl_session_insert(f->raw, "input", &value, 1, 1)));
}
static void map_positives(void){
    const char *expressions[] = {"strlen(to_string(x))", "to_string(x)", "x+1",
                                 "x"};
    for (unsigned kind = 0; kind < 4; ++kind) {
        host_t f = create_map("int64", kind == 1?"string":"int64",
                expressions[kind], false, NULL); f.numeric = kind == 2;
        hook(&f);
        /* Genuine typed-empty source. First-missing logical schema has a
         * separate concrete adapter obligation, recorded in the ledger. */
        map_input(&f, 1, false); map_input(&f, 1, true);
        CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
        map_input(&f, 1, false); map_input(&f, 12, false);
        map_input(&f, 23, false);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
            !wl_session_step(f.raw)); compare(&f);
        uint64_t expected = 0;
        if (kind == 0)expected = 6;
        if (kind == 1) {
            const char *strings[] = {"1", "12", "23"}; for (unsigned i = 0;
                i < 3; ++i){
                int64_t id = wl_intern_get(f.plan->intern, strings[i]);
                CHECK(id >= 0 && id < 64); expected |= UINT64_C(1)<<id;
            }
        }
        if (kind ==
            2) expected = (UINT64_C(1)<<2)|(UINT64_C(1)<<13)|(UINT64_C(1)<<24);
        if (kind ==
            3) expected = (UINT64_C(1)<<1)|(UINT64_C(1)<<12)|(UINT64_C(1)<<23);
        CHECK(image(session_find_rel(COL_SESSION(f.session), "out"),
            1) == expected);
        if (kind == 3)CHECK(!f.descriptions && f.observer_events == 3 &&
                !f.compile);
        else CHECK(f.descriptions && f.auth);
        map_input(&f, 12, true);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
            !wl_session_step(f.raw)); compare(&f);
        CHECK(f.negative == (kind == 0?0u:1u));
        map_input(&f, 23, true);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
            !wl_session_step(f.raw)); compare(&f);
        map_input(&f, 12, false);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
            !wl_session_step(f.raw)); compare(&f);
        unsigned events = f.events;
        CHECK(!wl_session_step(f.session) && f.events == events); destroy(&f);
    }
    host_t f = create_map("int64", "int64", "strlen(to_string(x))", true, NULL);
    hook(&f);
    CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
    CHECK(f.auth && f.compile && f.balance[1] == 1);
    map_input(&f, 12, false);
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
        !wl_session_step(f.raw)); compare(&f); CHECK(f.balance[2] == 1);
    map_input(&f, 1, true);
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
        !wl_session_step(f.raw)); compare(&f);
    CHECK(!f.balance[1] && f.balance[2] == 1); destroy(&f);
}
static void map_snapshot(const char *name, const int64_t *row, uint32_t width,
    void *data){
    host_t *f = data;
    CHECK(!strcmp(name,
        "out") && width == 1 && row[0] >= 0 && row[0] < 64 &&
        f->balance[row[0]] == 1); ++map_snapshot_rows;
}
static void map_errors(void){
    host_t f = create_map("string", "int64", "to_number(x)", false, NULL);
    hook(&f);
    int64_t two = wl_intern_put(f.plan->intern, "2"),
        three = wl_intern_put(f.plan->intern, "3");
    map_input(&f, two, false); map_input(&f, three, false);
    CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
    map_snapshot_rows = 0;
    CHECK(!wl_session_snapshot(f.session, map_snapshot,
        &f) && map_snapshot_rows == 2);
    col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
    col_relation_snapshot_t old_image = wl_columnar_relation_snapshot(old);
    unsigned events = f.events;
    int64_t bad = wl_intern_put(f.plan->intern, "9223372036854775808");
    map_input(&f, bad, false); CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(complete(f.session) == ERANGE && wl_session_step(f.raw) == ERANGE);
    CHECK(f.events == events && session_find_rel(COL_SESSION(f.session),
        "out") == old && wl_columnar_relation_snapshot_equal(old_image,
        wl_columnar_relation_snapshot(old))); closed(&f);
    map_snapshot_rows = 0;
    CHECK(wl_session_snapshot(f.session, map_snapshot,
        &f) == EBUSY && !map_snapshot_rows);
    map_input(&f, bad, true);
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
        !wl_session_step(f.raw)); compare(&f); destroy(&f);
    f = create_map("int64", "int64", "strlen(to_string(x))", false, NULL);
    hook(&f); map_input(&f, 1, false);
    CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
    map_input(&f, 12, false);
    old = session_find_rel(COL_SESSION(f.session), "out");
    old_image = wl_columnar_relation_snapshot(old); events = f.events;
    for (unsigned repeat = 0; repeat < 3; ++repeat) {
        CHECK(!wl_evaluation_control_reset(f.control));
        map_allocation_fail = true; map_allocation_injected = false;
        int rc = complete(f.session);
        wl_columnar_expr_test_fail_allocation = -1;
        CHECK(map_allocation_injected && !map_allocation_fail && rc == ENOMEM &&
            f.events == events && session_find_rel(COL_SESSION(f.session),
            "out") == old && wl_columnar_relation_snapshot_equal(old_image,
            wl_columnar_relation_snapshot(old))); closed(&f);
    }
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
        !wl_session_step(f.raw)); compare(&f); destroy(&f);
}
static void map_extension_destroy(void *data){
    (void)data; ++map_extension_destroyed;
}
static int map_extension_callback(const wirelog_extension_value_t *args,
    uint32_t n,
    wirelog_extension_value_t *out, void *data){
    (void)data;
    CHECK(active && active->map && map_callback_session && n == 1 &&
        args[0].type == WIRELOG_EXTENSION_VALUE_STRING);
    wl_col_session_t *s = COL_SESSION(map_callback_session);
    CHECK(s->callback_configured_workers == 1 &&
        s->callback_active_workers == 1 && !s->callback_parallel_execution &&
        s->callback_session_key == s);
    CHECK(args[0].as.string_value.data || !args[0].as.string_value.length);
    wl_session_t *same = map_callback_session;
    CHECK(wl_session_try_destroy(&same) == EBUSY &&
        same == map_callback_session);
    ++map_extension_calls;
    if (map_extension_fail && args[0].as.string_value.length == 3)return -1;
    out->type = WIRELOG_EXTENSION_VALUE_INT64; out->size = sizeof(int64_t);
    out->as.int64_value = args[0].as.string_value.length;
    return 0;
}
static void map_extension(void){
    map_extension_calls = map_extension_destroyed = 0;
    map_extension_fail = false;
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create(); CHECK(registry);
    uint32_t argument = WIRELOG_EXTENSION_VALUE_STRING;
    wirelog_extension_descriptor_t descriptor =
    {.abi_version = WIRELOG_EXTENSION_ABI_VERSION, .size = sizeof(descriptor),
     .name = "test.map", .arity = 1, .argument_types = &argument,
     .result_type = WIRELOG_EXTENSION_VALUE_INT64,
     .invoke = map_extension_callback, .destroy = map_extension_destroy};
    CHECK(!wirelog_extension_register(registry, &descriptor));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry); CHECK(snapshot);
    host_t f = create_map("string", "int64", "@call(\"test.map\",x)", false,
            snapshot); hook(&f);
    CHECK(!wirelog_extension_unregister(registry, "test.map"));
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!map_extension_destroyed);
    int64_t a = wl_intern_put(f.plan->intern, "a"),
        bb = wl_intern_put(f.plan->intern, "bb"),
        bad = wl_intern_put(f.plan->intern, "bad");
    map_input(&f, a, false); map_input(&f, bb, false);
    CHECK(!complete(f.session) && !complete(f.raw)); compare(&f);
    CHECK(map_extension_calls == 4 && f.auth && f.unknown_maps &&
        f.balance[1] == 1 && f.balance[2] == 1 && !map_extension_destroyed);
    col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
    col_relation_snapshot_t image_before = wl_columnar_relation_snapshot(old);
    unsigned events = f.events;
    map_extension_fail = true; map_input(&f, bad, false);
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(complete(f.session) == WL_COLUMNAR_EXPR_CALLBACK_FAILURE &&
        complete(f.raw) == WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
    CHECK(f.events == events && session_find_rel(COL_SESSION(f.session),
        "out") == old && wl_columnar_relation_snapshot_equal(image_before,
        wl_columnar_relation_snapshot(old)) && !map_extension_destroyed);
    closed(&f);
    CHECK(COL_SESSION(f.session)->extension_expr_status ==
        WL_COLUMNAR_EXPR_CALLBACK_FAILURE);
    map_input(&f, bad, true); map_extension_fail = false;
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
        !complete(f.raw)); compare(&f);
    map_input(&f, a, true);
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
        !complete(f.raw)); compare(&f);
    CHECK(!f.balance[1] && f.balance[2] == 1);
    unsigned calls = map_extension_calls; events = f.events;
    CHECK(!complete(f.session) && calls == map_extension_calls &&
        events == f.events);
    destroy(&f);
    CHECK(map_extension_destroyed == 1 &&
        !wirelog_extension_registry_destroy(registry));
    map_callback_session = NULL;
}
static void map_empty_stops(void){
    for (action = 0; action < 5; ++action) {
        host_t f = create_map("int64", "string", "to_string(x)", false, NULL);
        hook(&f);
        map_input(&f, 1, false); map_input(&f, 1, true);
        col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
        col_relation_snapshot_t before =
            old?wl_columnar_relation_snapshot(old):(col_relation_snapshot_t){0};
        CHECK(!wl_evaluation_control_configure(f.control,
            action == 1?UINT64_MAX:0));
        stop_phase = 70; stop_at = 1; hits = 0; stopped = false; successor = 0;
        int rc = complete(f.session); stop_phase = 0;
        int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
            1?WL_EVALUATION_CONTROL_WORK_LIMIT:action == 4?EOVERFLOW:EINVAL;
        CHECK(stopped && rc == expected && !f.events && !f.evals &&
            session_find_rel(COL_SESSION(f.session), "out") == old);
        if (old)CHECK(wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(old))); closed(&f);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        if (action == 2 || action == 3) {
            CHECK(report_equal(&frozen, &report)); if (action == 3){
                uint64_t used = 999;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    successor, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, successor, 0, 0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
        }else CHECK(report.result == expected && !report.cleanup_status);
        CHECK(!wl_evaluation_control_reset(f.control) &&
            !wl_evaluation_control_configure(f.control,
            0) && !complete(f.session) && !wl_session_step(f.raw)); compare(&f);
        CHECK(!f.events && !f.evals); destroy(&f);
    }
}
static void map_identity_forms(void){
    for (unsigned form = 0; form < 4; ++form) {
        host_t f = create_map("int64", "int64", "x", false, NULL); hook(&f);
        wl_plan_op_t *map =
            (wl_plan_op_t *)&f.plan->strata[0].relations[0].ops[1];
        wl_plan_expr_buffer_t *original_array = map->map_exprs;
        uint32_t original_count = map->map_expr_count;
        wl_plan_expr_buffer_t *expr =
            (wl_plan_expr_buffer_t *)&map->map_exprs[0];
        uint8_t *original_data = expr->data;
        uint32_t original_size = expr->size;
        CHECK(original_count == 1 && original_data && original_size == 7);
        if (form == 0)map->map_expr_count = 0;
        if (form == 1)expr->size = 0;
        if (form == 2) {
            expr->size = 0; expr->data = NULL;
        }
        if (form == 3) {
            map->map_expr_count = 0; map->map_exprs = NULL;
        }
        map_input(&f, 1, false);
        CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
        CHECK(!f.descriptions && !f.compile && f.observer_events == 1 &&
            f.balance[1] == 1);
        map->map_exprs = original_array; map->map_expr_count = original_count;
        expr->data = original_data; expr->size = original_size;
        unsigned events = f.events;
        CHECK(!complete(f.session) && f.events == events); destroy(&f);
    }
}
static void map_unobserved(void){
    const char *expressions[] = {"strlen(to_string(x))", "to_string(x)", "x+1",
                                 "x"};
    for (unsigned kind = 0; kind < 4; ++kind) {
        host_t f = create_map("int64", kind == 1?"string":"int64",
                expressions[kind], false, NULL); f.numeric = kind == 2;
        f.map_unobserved = true; hook(&f);
        wl_session_set_delta_cb(f.session, NULL, NULL);
        map_input(&f, 1, false); map_input(&f, 12, false);
        CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control,
            &report) && report.consumed);
        CHECK(!f.descriptions); if (kind < 3)CHECK(f.evals);
        map_input(&f, 12, true);
        CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.session) &&
            !wl_session_step(f.raw)); compare(&f);
        CHECK(!complete(f.session)); compare(&f); destroy(&f);
    }
}
static void map_stops(void){
    const unsigned phases[] = {70, 83, 84, 88, 550, 551, 552, 553, 554, 555,
                               564, 610, 611, 614, 612, 613};
    for (unsigned p = 0; p < sizeof(phases)/sizeof(phases[0]); ++p)for (action =
            0;
            action < 5; ++action) {
            host_t f = create_map("int64", "int64", "strlen(to_string(x))",
                    false, NULL); hook(&f); map_input(&f, 1, false);
            CHECK(!complete(f.session) && !wl_session_step(f.raw)); compare(&f);
            col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t old_image =
                wl_columnar_relation_snapshot(old); unsigned events = f.events;
            map_input(&f, 12, false); map_input(&f, 23, false);
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control,
                action == 1?UINT64_MAX:0));
            description = phases[p] >= 550 && phases[p] <= 564;
            stop_phase = phases[p];
            stop_at = p == 1?2:p == 2?4:p == 3?2:1; hits = 0; stopped = false;
            successor = 0;
            int rc = complete(f.session); stop_phase = 0; description = false;
            int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                1?WL_EVALUATION_CONTROL_WORK_LIMIT:action == 4?EOVERFLOW:EINVAL;
            if (rc != expected) fprintf(stderr,
                    "map stop phase%u action%u rc%d expected%d\n", phases[p],
                    action, rc, expected);
            CHECK(stopped && rc == expected && f.events == events &&
                session_find_rel(COL_SESSION(f.session),
                "out") == old && wl_columnar_relation_snapshot_equal(old_image,
                wl_columnar_relation_snapshot(old))); closed(&f);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            if (action == 2 || action == 3) {
                CHECK(report_equal(&frozen, &report)); if (action == 3){
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                        successor, &used) && !used);
                    wl_evaluation_control_attempt_finish_result_t end =
                        wl_evaluation_control_finish_attempt(f.control,
                            f.session->operation_admission, successor, 0, 0);
                    CHECK(!end.protocol && end.report_valid &&
                        !end.report.consumed);
                }
            }else CHECK(report.result == expected && !report.cleanup_status);
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control,
                0) && !complete(f.session) && !wl_session_step(f.raw));
            compare(&f); destroy(&f);
        }
}
int main(void){
    map_unobserved(); map_identity_forms(); map_extension(); map_errors();
    map_empty_stops();
    map_stops(); map_positives(); numeric_right_oracle();
    compiler_boundary(); positives(); probe_positives(); probe_collisions();
    probe_empty(); sip_positive(); empty_and_invalid(); stops();
    puts("bounded expression hosts: passed"); return 0;
}
