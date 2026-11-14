/* #2299 actual empty physical inputs, real source leases and native hosts. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
extern void (*wl_columnar_filter_test_map_schema)(const col_rel_t *, bool);
extern size_t wl_columnar_session_test_general_original_bytes(void);
extern int wl_columnar_session_test_general_maintenance_consumed(
    const wl_col_session_t *, uint64_t *);
extern unsigned wl_columnar_session_test_general_route_flags(
    const wl_col_session_t *);
extern unsigned wl_columnar_session_test_cleanup_owners(
    const wl_col_session_t *);
extern int wl_columnar_session_test_pending_source_bytes(
    const wl_col_session_t *, uint64_t *);

static int owner;
static wl_evaluation_control_t *begin(uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner)); return c;
}
static void finish(wl_evaluation_control_t *c, int rc) {
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}
static wl_columnar_memory_governor_ref_t *governor(void) {
    wl_columnar_memory_resolution_t r = {.budget_bytes = UINT64_C(128)<<20,
                                         .usable_bytes = UINT64_C(128)<<20,
                                         .mode =
                                             WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g); return g;
}
static uint64_t reserved(wl_columnar_memory_governor_ref_t *g) {
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}
static void gates_free(col_rel_t *r) {
    wl_columnar_source_access_writer_t w = {0};
    CHECK(!wl_columnar_source_access_writer_acquire(&r->descriptor_access, &w));
    CHECK(!wl_columnar_source_access_writer_release(&w));
    CHECK(!wl_columnar_source_access_writer_acquire(&r->source_access, &w));
    CHECK(!wl_columnar_source_access_writer_release(&w));
}
typedef struct {uint16_t fee[512]; size_t count; uint64_t total;} prices_t;
static void fee(prices_t *p, uint16_t n) {
    CHECK(p->count < sizeof(p->fee)/sizeof(*p->fee)); p->fee[p->count++] = n;
    p->total += n;
}
/* Independent numeric vector derived from control flow, no production tariff
 * imports or observed histories. sizeof only expresses ABI geometry. All
 * name bytes include NUL; descriptor copies use eight-byte224 packets. */
static prices_t prices(const char *name, bool governed) {
    prices_t p = {0};
#define F(n) fee(&p, n)
#define R(n, c) do {for (size_t i = 0; i < (size_t)(c); ++i) F(n);} while (0)
    const uint16_t input[] = {256, 256, 96, 144, 256, 96, 144, 256, 256, 256,
                              256, 256};
    for (size_t k = 0; k < sizeof(input)/sizeof(*input); ++k)F(input[k]);
    R(224, COL_MAX_RUNS); F(256); /* actual run records and recipe */
    F(192); R(96, strlen(name)+1); /* builder INIT/name scan */
    R(96, 10); if (governed)F(96); /* geometry and scratch math */
    F(128); F(96); F(160);
    if (governed) {
        F(74); F(54); F(84); F(29);
    }
    F(112); R(224, (sizeof(col_rel_t)+7u)/8u);
    R(96, 5); F(112); R(224, (strlen(name)+8u)/8u);
    F(80); F(112); F(96); F(160);
    if (governed) {
        F(74); F(54); F(84); F(29);
    }
    F(160); F(96); F(112); F(224); /* no payload; Arrow root format */
    for (unsigned k = 0; k < 3; ++k) {
        F(128); if (governed && k < 2){
            F(74); F(54); F(29);
        }
    }
    F(220); F(256); F(256); /* install, normal source drain and final */
#undef R
#undef F
    return p;
}
static col_rel_t *bare(void) {
    col_rel_t *r = NULL; CHECK(!col_rel_alloc(&r, "original"));
    r->declared_ncols = 1; return r;
}
static void primitive_prices(void) {
    for (unsigned named = 0; named < 2; ++named)for (unsigned governed = 0;
            governed < 2; ++governed) {
            const char *name = named?"input":"";
            prices_t p = prices(name, governed != 0);
            /* Checksums on this ABI supplement the individual ordered cuts. */
            if (sizeof(col_rel_t) == 944 && COL_MAX_RUNS == 8) CHECK(p.total ==
                    (governed?(named?37096u:36616u):(named?36204u:35724u)));
            col_rel_t *r = bare();
            wl_columnar_memory_governor_ref_t *g = governor();
            col_relation_snapshot_t old = wl_columnar_relation_snapshot(r);
            uint64_t sum = 0;
            for (size_t cut = 0; cut < p.count; ++cut) {
                sum += p.fee[cut];
                for (unsigned below = 0; below < 2; ++below) {
                    wl_evaluation_control_t *c = begin(sum-below);
                    col_rel_t *out = r;
                    wirelog_column_type_t declared = WIRELOG_TYPE_STRING;
                    int rc =
                        wl_columnar_relation_new_empty_input_operator_attempt_controlled
                        (
                        name, r, &declared, governed?g:NULL, &out, c,
                        wl_evaluation_control_active_attempt_id(c));
                    uint64_t used = 99;
                    CHECK(!wl_evaluation_control_test_active_consumed(c,
                        wl_evaluation_control_active_attempt_id(c), &used));
                    if (cut+1 == p.count && !below) {
                        CHECK(!rc && out && out != r && out->schema_ok &&
                            !out->ncols && !out->nrows);
                        CHECK(out->declared_ncols == 1 && !out->column_types &&
                            !out->has_graph_column);
                        CHECK(used == p.total);
                        CHECK(!col_rel_destroy_checked(out));
                    }else {
                        CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !out);
                        CHECK(used == (below?sum-p.fee[cut]:sum));
                    }
                    gates_free(r);
                    CHECK(!reserved(g) &&
                        wl_columnar_memory_governor_ref_is_sole(g));
                    CHECK(wl_columnar_relation_snapshot_equal(old,
                        wl_columnar_relation_snapshot(r)) &&
                        !r->schema_ok && !r->ncols && r->declared_ncols == 1);
                    finish(c, rc);
                }
            }
            CHECK(sum == p.total); CHECK(!col_rel_destroy_checked(r));
            wl_columnar_memory_governor_ref_release(g);
        }
}
static unsigned target_phase, action; static bool stopped;
static col_rel_t *original;
static wl_evaluation_control_t *active_control; static uint64_t successor;
static wl_evaluation_control_report_t frozen;
static bool reports_equal(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b) {
    return a->attempt_id == b->attempt_id && a->consumed == b->consumed &&
           a->limit == b->limit &&
           a->result == b->result &&
           a->execution_status == b->execution_status &&
           a->cleanup_status == b->cleanup_status &&
           a->stop_reason == b->stop_reason;
}
static void stop_now(wl_evaluation_control_t *c) {
    stopped = true;
    if (!action)wl_evaluation_control_request_cancel(c);
    else if (action == 1 || action == 4)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) ==
            (action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:EOVERFLOW));
    else {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
        CHECK(!wl_evaluation_control_report(c, &frozen));
        if (action == 3) {
            CHECK(!wl_evaluation_control_reset(c));
            CHECK(!wl_evaluation_control_begin(c, &owner));
            successor = wl_evaluation_control_active_attempt_id(c);
        }
    }
}
static void source_point(wl_evaluation_control_t *c, unsigned phase) {
    if (c != active_control)return;
    if (phase == 623 || phase == 631) {
        wl_columnar_source_access_writer_t w = {0};
        CHECK(wl_columnar_source_access_writer_acquire(
                &original->descriptor_access, &w) == EBUSY);
        CHECK(wl_columnar_source_access_writer_acquire(&original->source_access,
            &w) == EBUSY);
    }
    if (phase == 632)gates_free(original);
    if (!stopped && phase == target_phase)stop_now(c);
}
static void reader_after(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired) {
    if (c != active_control || stopped || !acquired) return;
    if ((target_phase == 634 && gate == &original->descriptor_access) ||
        (target_phase == 635 && gate == &original->source_access)) stop_now(c);
}
static void before_release(const col_rel_t *r) {
    if (r == original && target_phase == 633 &&
        !stopped) stop_now(active_control);
}
static void primitive_stops(void) {
    const unsigned phases[] = {620, 621, 622, 623, 624, 625, 626, 627, 628, 629,
                               631, 632, 633, 634, 635};
    for (size_t k = 0; k < sizeof(phases)/sizeof(*phases); ++k)for (action = 0;
            action < 5; ++action) {
            original = bare();
            wl_columnar_memory_governor_ref_t *g = governor();
            active_control = begin(action == 1?UINT64_MAX:0);
            uint64_t id =
                wl_evaluation_control_active_attempt_id(active_control);
            target_phase = phases[k]; stopped = false; successor = 0;
            wl_columnar_relation_test_empty_input_checkpoint = source_point;
            wl_columnar_relation_test_new_like_before_release = before_release;
            wl_columnar_relation_test_after_reader_cas = reader_after;
            col_rel_t *out = original;
            int rc =
                wl_columnar_relation_new_empty_input_operator_attempt_controlled
                (
                "input", original, NULL, g, &out, active_control, id);
            wl_columnar_relation_test_empty_input_checkpoint = NULL;
            wl_columnar_relation_test_new_like_before_release = NULL;
            wl_columnar_relation_test_after_reader_cas = NULL;
            int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                4?EOVERFLOW:EINVAL;
            CHECK(stopped && rc == expected && !out && !reserved(g));
            gates_free(original);
            if (action == 2 || action == 3) {
                wl_evaluation_control_report_t r;
                CHECK(!wl_evaluation_control_report(active_control, &r));
                CHECK(reports_equal(&r, &frozen));
                if (action == 3) {
                    uint64_t used = 99;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            active_control, successor, &used) && !used);
                    finish(active_control, 0);
                }else {
                    CHECK(!wl_evaluation_control_detach(active_control,
                        &owner));
                    wl_evaluation_control_release(active_control);
                }
            }else finish(active_control, rc);
            CHECK(!original->schema_ok && !original->ncols &&
                original->declared_ncols == 1);
            CHECK(!col_rel_destroy_checked(original));
            wl_columnar_memory_governor_ref_release(g);
            original = NULL; active_control = NULL;
        }
}
static void primitive_resources(void) {
    col_rel_t *r = bare(); wl_columnar_memory_governor_ref_t *g = governor();
    for (unsigned allocation = 1; allocation <= 3; ++allocation)
        for (unsigned repeat = 0; repeat < 2; ++repeat) {
            wl_evaluation_control_t *c = begin(0); col_rel_t *out = r;
            wl_columnar_relation_test_constructor_fail_allocation = allocation;
            CHECK(
                wl_columnar_relation_new_empty_input_operator_attempt_controlled
                (
                    "input", r, NULL, g, &out, c,
                    wl_evaluation_control_active_attempt_id(c)) == ENOMEM &&
                !out);
            CHECK(!wl_columnar_relation_test_constructor_fail_allocation &&
                !reserved(g));
            gates_free(r); finish(c, ENOMEM);
            c = begin(0);
            CHECK(
                !
                wl_columnar_relation_new_empty_input_operator_attempt_controlled
                (
                    "input", r, NULL, g, &out, c,
                    wl_evaluation_control_active_attempt_id(c)) && out);
            CHECK(!col_rel_destroy_checked(out)); gates_free(r);
            CHECK(!reserved(g)); finish(c, 0);
        }
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(g);
    atomic_store_explicit(&budget->usable_bytes, 0, memory_order_seq_cst);
    wl_evaluation_control_t *c = begin(0); col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_new_empty_input_operator_attempt_controlled(
            "input", r, NULL, g, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOSPC && !out);
    gates_free(r); CHECK(!reserved(g)); finish(c, ENOSPC);
    atomic_store_explicit(&budget->usable_bytes, UINT64_C(128)<<20,
        memory_order_seq_cst);
    c = begin(0);
    CHECK(!wl_columnar_relation_new_empty_input_operator_attempt_controlled(
            "input", r, NULL, g, &out, c,
            wl_evaluation_control_active_attempt_id(c)) && out);
    CHECK(!col_rel_destroy_checked(out)); CHECK(!reserved(g)); finish(c, 0);
    CHECK(!col_rel_destroy_checked(r));
    wl_columnar_memory_governor_ref_release(g);
}

typedef struct {
    wirelog_program_t *program; wl_plan_t *plan; wl_session_t *raw, *controlled;
    wl_evaluation_control_t *control; wl_columnar_memory_governor_ref_t *g;
    col_rel_t *input; uint32_t width; bool graph, filter, constant, numeric;
    unsigned source_calls, metadata, unknown, bodies, events;
    bool empty_known; unsigned numeric_frames; bool no_observer;
    bool background;
    unsigned base_events;
    int base_balance[4]; int64_t balance;
    unsigned stop; bool did_stop; uint64_t next;
    unsigned fail_constructor; bool deny_constructor;
    wl_evaluation_control_report_t original_report;
} fixture_t;
static fixture_t *active;
static bool graph_fee_active;
static unsigned graph_source_witness, graph_framed_aux_witness;
static uint32_t graph_source_count;
extern void (*wl_columnar_eval_serial_test_framed_checkpoint)(
    wl_columnar_eval_serial_framed_attempt_t *);
static void graph_framed_visit(wl_columnar_eval_serial_framed_attempt_t *ctx) {
    if (!graph_fee_active || !active ||
        ctx->session != COL_SESSION(active->controlled) ||
        ctx->control != active->control) return;
    if (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP &&
        ctx->registry_slot < ctx->session->nrels && ctx->name_byte == 0 &&
        !strcmp(ctx->session->rels[ctx->registry_slot]->name,
        "__graph_metadata") && !strcmp(ctx->plan->name,
        "out")) ++graph_framed_aux_witness;
}

static int complete(wl_session_t *s) {
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = wl_session_step(s); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EAGAIN;
}
static void events(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *context) {
    fixture_t *f = context;
    if (f->background && !strcmp(name, "base")) {
        CHECK(width == 1 && row && row[0] > 0 && row[0] < 4 && diff);
        ++f->base_events; f->base_balance[row[0]] += diff; return;
    }

    CHECK(!strcmp(name, "out") && width == 1 && row && diff);
    ++f->events; f->balance += diff;
}
static void schema(const col_rel_t *r, bool known) {
    if (!active)return;
    if (active->background && known)return; /* Numeric base head; out stays UNKNOWN. */
    bool expected = active->input->ncols || active->constant ||
        active->numeric || active->empty_known;
    CHECK(known == expected && r && r->ncols == 1);
    if (active->no_observer){
        wl_col_session_t *session = COL_SESSION(active->controlled);
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        CHECK(session->cleanup_active &&
            !wl_columnar_eval_stack_framed_context(session->cleanup_active,
            session, &ctx));
        CHECK(!wl_columnar_eval_stack_framed_authenticate(ctx->frame, session,
            ctx) &&
            ctx->capture_only && ctx->private_bindings &&
            ctx->abort_supported && ctx->cleanup_receipt_key.receipt_id &&
            wl_columnar_session_observed_bindings_authorized(session,
            ctx->private_bindings) &&
            wl_columnar_eval_plan_numeric_owner_valid(&ctx->child,
            ctx->frame) &&
            !wl_columnar_eval_stack_numeric_owner(ctx->frame, &ctx->child) &&
            wl_columnar_session_test_general_route_flags(session) == 5u);
        ++active->numeric_frames;
    }

    if (!known) {
        CHECK(!r->column_types); ++active->unknown;
    }
}
static void session_source(wl_evaluation_control_t *, unsigned);
static void expression(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    fixture_t *f = active;
    if (!f || c != f->control || (phase != 610 && phase != 83)) return;
    wl_col_session_t *session = COL_SESSION(f->controlled);
    if (f->no_observer)CHECK(!session->delta_cb && !session->delta_observer &&
            !session->delta_observer_build &&
            !session->delta_observer_prepare &&
            wl_columnar_session_test_general_route_flags(session) == 5u);

    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    CHECK(session->cleanup_active && !wl_columnar_eval_stack_framed_context(
            session->cleanup_active, session, &ctx));
    CHECK(!wl_columnar_eval_stack_framed_authenticate(ctx->frame, session,
        ctx) &&
        ctx->capture_only && ctx->private_bindings && ctx->abort_supported &&
        ctx->cleanup_receipt_key.receipt_id &&
        wl_columnar_session_observed_bindings_authorized(session,
        ctx->private_bindings) &&
        wl_columnar_session_test_cleanup_owners(session) == 2);
    if (phase == 610) {
        if (graph_fee_active) {
            CHECK(ctx->child.private_bindings == ctx->private_bindings &&
                ctx->child.cursor == 1 &&
                ctx->child.source_count ==
                ctx->private_bindings->count+session->nrels &&
                ctx->child.source_slot == ctx->child.source_count &&
                !ctx->child.source_delta);
            graph_source_count = ctx->child.source_count;
            ++graph_source_witness;
        }

        wl_columnar_filter_context_t *map = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context_owned(ctx->frame, &map,
            &ctx->child));
        CHECK(map->source.width == f->input->ncols &&
            map->source.declared_ncols == f->input->declared_ncols &&
            map->source.has_graph_column == f->input->has_graph_column &&
            map->source.graph_col_idx == f->input->graph_col_idx);
        if (!f->input->ncols &&
            !f->input->schema_ok) CHECK(map->source.descriptor != f->input &&
                map->source.descriptor->schema_ok && !map->source.nrows);
        ++f->metadata;
    }else ++f->bodies;
    if (f->no_observer && f->stop == phase)session_source(c, phase);

}
static void session_source(wl_evaluation_control_t *c, unsigned phase) {
    fixture_t *f = active; if (!f || c != f->control)return;
    if (phase == 621)++f->source_calls;
    if (phase == 629 && f->fail_constructor) {
        wl_columnar_relation_test_constructor_fail_allocation =
            f->fail_constructor;
        f->fail_constructor = 0;
    }
    if (phase == 629 && f->deny_constructor) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(f->g);
        atomic_store_explicit(&g->usable_bytes, wl_columnar_memory_reserved(g),
            memory_order_seq_cst);
        f->deny_constructor = false;
    }

    if (f->stop != phase || f->did_stop)return;
    f->did_stop = true;
    if (!action)wl_evaluation_control_request_cancel(c);
    else if (action == 1 || action == 4)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) ==
            (action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:EOVERFLOW));
    else {
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_evaluation_control_attempt_finish_result_t end =
            wl_evaluation_control_finish_attempt(
            c, f->controlled->operation_admission, id, 0, 0);
        CHECK(!end.protocol && end.report_valid);
        f->original_report = end.report;
        if (action == 3) {
            CHECK(!wl_evaluation_control_reset(c));
            wl_evaluation_control_attempt_begin_result_t next =
                wl_evaluation_control_begin_attempt(
                c, f->controlled->operation_admission);
            CHECK(!next.protocol && next.attempt_id != id);
            f->next = next.attempt_id;
        }
    }
}
static void create_sessions(fixture_t *f, const char *text) {
    if (!f->g)f->g = governor();
    wirelog_error_t error; f->program = wirelog_parse_string(text, &error);
    int plan_rc = f->program?wl_plan_from_program(f->program, &f->plan):EINVAL;
    if (plan_rc)fprintf(stderr, "source plan%d: %s\n", plan_rc, text);
    CHECK(f->program && !plan_rc);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f->g;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->raw));
    CHECK(!wl_evaluation_control_create(0, &f->control));
    options.evaluation_control = f->control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->controlled));
    f->input = session_find_rel(COL_SESSION(f->controlled), "input");
    CHECK(f->input);
}
static fixture_t create(const char *type, const char *output, const char *expr,
    bool graph, bool filter) {
    fixture_t f = {0}; f.g = governor(); f.graph = graph; f.filter = filter;
    f.width = graph?2:1;
    f.constant = !strcmp(expr, "\"constant\"");
    f.numeric = !strcmp(expr, "x+1");
    char text[512]; snprintf(text, sizeof(text),
        ".decl input(x:%s%s)\n.decl out(y:%s)\nout(%s):-input(x%s)%s.\n",
        type, graph?",__graph_id:int64":"", output, expr, graph?",g":"",
        filter?",x>0":"");
    create_sessions(&f, text);
    return f;
}
static void hook(fixture_t *f) {
    active = f; wl_session_set_delta_cb(f->controlled, events, f);
    wl_columnar_filter_test_map_schema = schema;
    wl_columnar_expr_test_checkpoint = expression;
    wl_columnar_relation_test_empty_input_checkpoint = session_source;
}
static void bind_unobserved(fixture_t *f) {
    f->no_observer = true; active = f;
    CHECK(!COL_SESSION(f->controlled)->delta_cb);
    wl_columnar_filter_test_map_schema = schema;
    wl_columnar_expr_test_checkpoint = expression;
    wl_columnar_relation_test_empty_input_checkpoint = session_source;
}
static void closed(fixture_t *f) {
    CHECK(!COL_SESSION(f->controlled)->cleanup_active &&
        !COL_SESSION(f->controlled)->cleanup_pending &&
        !wl_columnar_session_test_cleanup_owners(COL_SESSION(f->controlled)));
    uint64_t retained = 0;
    CHECK(!wl_columnar_session_test_pending_source_bytes(COL_SESSION(
            f->controlled), &retained));
    if (retained) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f->control, &report));
        CHECK(report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_columnar_source_access_writer_t w = {0};
        CHECK(wl_columnar_source_access_writer_acquire(
                &f->input->descriptor_access, &w) == EBUSY &&
            wl_columnar_source_access_writer_acquire(&f->input->source_access,
            &w) == EBUSY);
    } else gates_free(f->input);
}
static void equal(fixture_t *f) {
    col_rel_t *a = session_find_rel(COL_SESSION(f->raw), "out");
    col_rel_t *b = session_find_rel(COL_SESSION(f->controlled), "out");
    CHECK(a && b && a->ncols == b->ncols && a->nrows == b->nrows &&
        a->ncols == 1);
    CHECK((a->column_types?a->column_types[0]:WIRELOG_TYPE_INT64) ==
        (b->column_types?b->column_types[0]:WIRELOG_TYPE_INT64));
    for (uint32_t i = 0; i < a->nrows; ++i) {
        bool found = false;
        for (uint32_t j = 0; j < b->nrows;
            ++j) if (a->columns[0][i] == b->columns[0][j]){
                found = true;
                if (a->timestamps &&
                    b->timestamps) CHECK(a->timestamps[i].multiplicity ==
                        b->timestamps[j].multiplicity);
                break;
            }
        CHECK(found);
    }
    closed(f);
}
static void destroy(fixture_t *f) {
    active = NULL; wl_columnar_filter_test_map_schema = NULL;
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_relation_test_empty_input_checkpoint = NULL;
    wl_session_destroy(f->raw); wl_session_destroy(f->controlled);
    wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    CHECK(!reserved(f->g) && wl_columnar_memory_governor_ref_is_sole(f->g));
    wl_columnar_memory_governor_ref_release(f->g);
}
static void insert(fixture_t *f, int64_t value, bool remove) {
    int64_t row[] = {value, 9};
    CHECK(!(remove?wl_session_remove(f->raw, "input", row, 1, f->width):
        wl_session_insert(f->raw, "input", row, 1, f->width)));
    CHECK(!(remove?wl_session_remove(f->controlled, "input", row, 1, f->width):
        wl_session_insert(f->controlled, "input", row, 1, f->width)));
}
static void session_matrix(void) {
    const char *types[] = {"int64", "int64", "int64", "int64", "string",
                           "float", "bool"};
    const char *outputs[] = {"string", "int64", "string", "int64", "string",
                             "string", "string"};
    const char *exprs[] = {"to_string(x)", "strlen(to_string(x))",
                           "\"constant\"", "x+1",
                           "cat(x,\"!\")", "to_string(x)"};
    for (unsigned kind = 0; kind < 6; ++kind) {
        fixture_t f = create(types[kind], outputs[kind], exprs[kind], false,
                false); hook(&f);
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(f.input);
        uint32_t width = f.input->ncols; bool ok = f.input->schema_ok;
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(!f.events &&
            f.input == session_find_rel(COL_SESSION(f.controlled), "input") &&
            f.input->ncols == width && f.input->schema_ok == ok &&
            f.input->declared_ncols == 1 &&
            wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(f.input)));
        if (!width) {
            CHECK(f.source_calls); if (kind < 2) CHECK(f.unknown);
        }
        int64_t first = 2, second = 12;
        if (kind == 4) {
            first = wl_intern_put(f.plan->intern, "s2");
            second = wl_intern_put(f.plan->intern, "s12");
        }
        if (kind == 5) {
            first = wl_columnar_float_to_bits(2.0);
            second = wl_columnar_float_to_bits(12.0);
        }
        int64_t wrong[] = {first, 9};
        int controlled_bad = wl_session_insert(f.controlled, "input", wrong, 1,
                2);
        int raw_bad = wl_session_insert(f.raw, "input", wrong, 1, 2);
        if (controlled_bad != ENOTSUP || raw_bad != EINVAL)fprintf(stderr,
                "kind%u badwidth control%d raw%d\n", kind, controlled_bad,
                raw_bad);
        /* The completed controlled STEP's existing mutation-shape admission
         * rejects this protected update with ENOTSUP; raw insertion reaches
         * its declaration-width EINVAL check. Both keep the original intact. */
        CHECK(controlled_bad == ENOTSUP && raw_bad == EINVAL);
        insert(&f, first, false);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        if (!f.numeric)CHECK(f.bodies);
        if (kind == 0) {
            col_rel_t *r = session_find_rel(COL_SESSION(f.controlled), "out");
            CHECK(r->nrows == 1 &&
                r->columns[0][0] == wl_intern_get(f.plan->intern, "2"));
        }
        if (kind == 4) {
            col_rel_t *r = session_find_rel(COL_SESSION(f.controlled), "out");
            CHECK(r->nrows == 1 &&
                r->columns[0][0] == wl_intern_get(f.plan->intern, "s2!"));
        }
        insert(&f, second, false);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        insert(&f, first, true); insert(&f, second, true);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        unsigned events_before = f.events;
        CHECK(!complete(f.controlled) && !complete(f.raw) &&
            f.events == events_before);
        CHECK(!f.balance); destroy(&f);
    }
    for (unsigned graph = 0; graph < 2; ++graph)for (unsigned filter = 0;
            filter < 2;
            ++filter) {
            fixture_t f = create("int64", "string", "to_string(x)", graph != 0,
                    filter != 0); hook(&f);
            CHECK(!f.input->ncols && !f.input->schema_ok &&
                f.input->declared_ncols == f.width &&
                f.input->has_graph_column == (graph != 0));
            int expected = graph && filter?EINVAL:0;
            CHECK(complete(f.controlled) == expected &&
                complete(f.raw) == expected);
            if (!expected) {
                equal(&f); CHECK(f.unknown && f.metadata);
            }else{
                CHECK(!session_find_rel(COL_SESSION(f.controlled), "out") &&
                    !session_find_rel(COL_SESSION(f.raw), "out")); closed(&f);
            }
            CHECK(!f.events && !f.input->ncols && !f.input->schema_ok &&
                f.input->declared_ncols == f.width &&
                f.input->has_graph_column == (graph != 0) &&
                (!graph || f.input->graph_col_idx == 1));
            insert(&f, 2, false);
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(f.bodies && f.balance == 1);
            if (filter) {
                unsigned events_before = f.events; insert(&f, -1, false);
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.controlled) && !complete(f.raw));
                equal(&f); CHECK(f.events == events_before && f.balance == 1);
            }
            destroy(&f);
        }
}
static void session_stops(void) {
    const unsigned phases[] = {620, 621, 622, 623, 624, 625, 626, 627, 628, 629,
                               631, 632};
    for (unsigned graph = 0; graph < 2; ++graph)
        for (unsigned unobserved = 0; unobserved < 2; ++unobserved)
            for (size_t k = 0; k < sizeof(phases)/sizeof(*phases);
                ++k) for (action = 0;
                    action < 5; ++action) {
                    fixture_t f = create("int64", "string", "to_string(x)",
                            graph != 0,
                            false);
                    if (unobserved) bind_unobserved(&f); else hook(&f);
                    col_relation_snapshot_t before =
                        wl_columnar_relation_snapshot(f.input);
                    uint64_t baseline = reserved(f.g); f.stop = phases[k];
                    col_rel_t *aux =
                        graph?COL_SESSION(f.controlled)->rels[1]:NULL;
                    col_relation_snapshot_t aux_before =
                        aux?wl_columnar_relation_snapshot(aux):
                        (col_relation_snapshot_t){0};
                    CHECK(!wl_evaluation_control_configure(f.control,
                        action == 1?UINT64_MAX:0));
                    int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                        action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                        4?EOVERFLOW:EINVAL;
                    CHECK(complete(f.controlled) == expected && f.did_stop &&
                        !f.events &&
                        !session_find_rel(COL_SESSION(f.controlled), "out"));
                    closed(&f);
                    uint64_t retained = 0;
                    CHECK(!wl_columnar_session_test_pending_source_bytes(
                            COL_SESSION(
                                f.controlled), &retained));
                    CHECK(wl_columnar_relation_snapshot_equal(before,
                        wl_columnar_relation_snapshot(f.input)) &&
                        !f.input->ncols && !f.input->schema_ok &&
                        f.input->declared_ncols == f.width);
                    CHECK(reserved(f.g) == baseline+retained);
                    CHECK((retained != 0) == (action == 1));
                    if (aux) {
                        CHECK(wl_columnar_relation_snapshot_equal(aux_before,
                            wl_columnar_relation_snapshot(aux)));
                        if (retained) {
                            wl_columnar_source_access_writer_t w = {0};
                            CHECK(wl_columnar_source_access_writer_acquire(
                                    &aux->descriptor_access, &w) == EBUSY);
                        }else gates_free(aux);
                    }

                    closed(&f); wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report));
                    if (action == 2 || action == 3) {
                        CHECK(reports_equal(&report, &f.original_report));
                        if (action == 3) {
                            uint64_t used = 99;
                            CHECK(!wl_evaluation_control_test_active_consumed(
                                    f.control,
                                    f.next, &used) && !used);
                            wl_evaluation_control_attempt_finish_result_t end =
                                wl_evaluation_control_finish_attempt(
                                f.control, f.controlled->operation_admission,
                                f.next, 0,
                                0);
                            CHECK(!end.protocol && end.report_valid &&
                                !end.report.consumed);
                        }
                    }else CHECK(report.result == expected &&
                            !report.cleanup_status);

                    if (action == 1) for (unsigned repeat = 0; repeat < 2;
                            ++repeat) {
                            f.did_stop = false;
                            CHECK(!wl_evaluation_control_reset(f.control) &&
                                !wl_evaluation_control_configure(f.control,
                                UINT64_MAX));
                            CHECK(complete(f.controlled) ==
                                WL_EVALUATION_CONTROL_WORK_LIMIT &&
                                f.did_stop &&
                                !f.events);
                            closed(&f); uint64_t again = 0;
                            CHECK(
                                !wl_columnar_session_test_pending_source_bytes(
                                    COL_SESSION(f.controlled), &again) &&
                                again == retained &&
                                reserved(f.g) == baseline+again &&
                                wl_columnar_relation_snapshot_equal(before,
                                wl_columnar_relation_snapshot(f.input)));
                        }
                    f.stop = 0;
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control, 0));
                    CHECK(!complete(f.controlled) && !complete(f.raw));
                    equal(&f);
                    CHECK(f.unknown && !f.events);
                    insert(&f, 2, false);
                    CHECK(!wl_evaluation_control_reset(f.control));
                    CHECK(!complete(f.controlled) && !complete(f.raw));
                    equal(&f);
                    CHECK(f.balance == (unobserved?0:1));
                    if (aux) {
                        unsigned count = 0; for (uint32_t j = 0;
                            j < COL_SESSION(f.controlled)->nrels;
                            ++j) count += COL_SESSION(f.controlled)->rels[j] ==
                                aux;
                        CHECK(count == 1 &&
                            wl_columnar_relation_snapshot_equal(aux_before,
                            wl_columnar_relation_snapshot(aux)));
                        gates_free(aux);
                    }

                    destroy(&f);
                }
}

static void old_public_image_stops(void) {
    const char *text = ".decl input(x:int64)\n.decl other(x:int64)\n"
        ".decl out(y:string)\n.decl base(y:int64)\n"
        "out(to_string(x)):-input(x).\nbase(x):-other(x).\n";
    for (action = 0; action < 5; ++action) {
        fixture_t f = {0}; f.width = 1; f.background = true;
        create_sessions(&f, text); hook(&f);
        int64_t value = 1;
        CHECK(!wl_session_insert(f.controlled, "other", &value, 1, 1) &&
            !wl_session_insert(f.raw, "other", &value, 1, 1));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(f.base_events == 1 && f.base_balance[1] == 1 && !f.events &&
            !f.input->ncols);
        col_rel_t *old_out = session_find_rel(COL_SESSION(f.controlled), "out");
        col_rel_t *old_base = session_find_rel(COL_SESSION(f.controlled),
                "base");
        col_relation_snapshot_t out_before =
            wl_columnar_relation_snapshot(old_out);
        col_relation_snapshot_t base_before =
            wl_columnar_relation_snapshot(old_base);
        value = 2;
        CHECK(!wl_session_insert(f.controlled, "other", &value, 1, 1) &&
            !wl_session_insert(f.raw, "other", &value, 1, 1));
        CHECK(!wl_evaluation_control_reset(f.control) &&
            !wl_evaluation_control_configure(f.control,
            action == 1?UINT64_MAX:0));
        f.stop = 631;
        int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:
            action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
            4?EOVERFLOW:EINVAL;
        CHECK(complete(f.controlled) == expected && f.did_stop);
        CHECK(session_find_rel(COL_SESSION(f.controlled), "out") == old_out &&
            session_find_rel(COL_SESSION(f.controlled), "base") == old_base &&
            wl_columnar_relation_snapshot_equal(out_before,
            wl_columnar_relation_snapshot(old_out)) &&
            wl_columnar_relation_snapshot_equal(base_before,
            wl_columnar_relation_snapshot(old_base)) &&
            f.base_events == 1 && f.base_balance[1] == 1 &&
            !f.base_balance[2] && !f.events);
        closed(&f);
        if (action == 2 || action == 3) {
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control,
                &report) && reports_equal(&report, &f.original_report));
            if (action == 3) {
                uint64_t used = 99;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    f.next, &used) && !used);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.controlled->operation_admission, f.next, 0, 0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
        }
        f.stop = 0;
        CHECK(!wl_evaluation_control_reset(f.control) &&
            !wl_evaluation_control_configure(f.control, 0));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        col_rel_t *actual = session_find_rel(COL_SESSION(f.controlled), "base");
        col_rel_t *reference = session_find_rel(COL_SESSION(f.raw), "base");
        CHECK(actual && reference && actual->nrows == 2 &&
            reference->nrows == 2 &&
            f.base_events == 2 && f.base_balance[1] == 1 &&
            f.base_balance[2] == 1 && !f.events);
        for (uint32_t i = 0; i < 2; ++i) {
            CHECK(actual->columns[0][i] == reference->columns[0][i]);
        }
        destroy(&f);
    }
}

static void session_resources(void) {
    for (unsigned unobserved = 0; unobserved < 2; ++unobserved)
        for (unsigned allocation = 1; allocation <= 4; ++allocation) {
            fixture_t f = create("int64", "string", "to_string(x)", false,
                    false);
            if (unobserved) bind_unobserved(&f); else hook(&f);
            col_relation_snapshot_t before =
                wl_columnar_relation_snapshot(f.input);
            uint64_t baseline = reserved(f.g);
            for (unsigned repeat = 0; repeat < 2; ++repeat) {
                f.fail_constructor = allocation < 4?allocation:0;
                f.deny_constructor = allocation == 4;
                int expected = allocation == 4?ENOSPC:ENOMEM;
                CHECK(complete(f.controlled) == expected &&
                    !f.fail_constructor &&
                    !f.deny_constructor &&
                    !wl_columnar_relation_test_constructor_fail_allocation &&
                    f.source_calls && !f.events);
                closed(&f);
                CHECK(!session_find_rel(COL_SESSION(f.controlled),
                    "out") && reserved(f.g) == baseline &&
                    wl_columnar_relation_snapshot_equal(before,
                    wl_columnar_relation_snapshot(f.input)));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report.execution_status == expected &&
                    !report.cleanup_status);
                atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                        f.g)->usable_bytes, UINT64_C(128)<<20,
                    memory_order_seq_cst);
                CHECK(!wl_evaluation_control_reset(f.control));
            }
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(f.unknown && !f.events);
            insert(&f, 2, false);
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(f.balance == (unobserved?0:1));
            destroy(&f);
        }
}

extern int wl_columnar_session_test_retirement_view(const wl_col_session_t *,
    uint64_t out[16]);
static unsigned late_epoch;
/* Rollback MAIN refusal (#2368): while observed rollback debt is being
 * retired, alternately grant one real MAIN packet a single unit and let the
 * next one run with its real grant, until this many packets were refused.
 * Each refusal must settle as a genuine WORK_LIMIT report that consumed
 * nothing, and the following packet must be a fresh attempt. */
extern uint64_t (*wl_columnar_session_test_main_limit)(const wl_col_session_t *,
    unsigned, unsigned, uint64_t);
extern void (*wl_columnar_session_test_main_action_report)(
    const wl_col_session_t *, unsigned, unsigned,
    wl_evaluation_control_report_t, int);
static unsigned late_rollback_refusals, late_rollback_refusals_left,
    late_rollback_refused, late_rollback_confirmed;
static bool late_rollback_pending, late_rollback_skip, late_rollback_settled;
static uint64_t late_rollback_refused_attempt;
static uint64_t late_rollback_main_limit(const wl_col_session_t *session,
    unsigned recipient, unsigned main_action, uint64_t capacity) {
    (void)recipient; (void)main_action;
    uint64_t view[16];
    CHECK(!late_rollback_pending);
    if (!late_rollback_refusals_left ||
        wl_columnar_session_test_retirement_view(session, view) ||
        !(view[5]&1))
        return capacity;
    if (late_rollback_skip) {
        late_rollback_skip = false; return capacity;
    }
    --late_rollback_refusals_left; ++late_rollback_refused;
    late_rollback_pending = late_rollback_skip = true;
    return 1;
}
/* After a settled refusal only the MAIN-funded rollback-debt rule can keep
 * a general route pending, and the state-only STEP observation reports it. */
static void late_rollback_observe(wl_session_t *session, int rc) {
    unsigned flags = wl_columnar_session_test_general_route_flags(COL_SESSION(
                session));
    if (rc != EAGAIN || !(flags&1u)) return;
    CHECK(session->backend->session_controlled_step_state(session) ==
        WL_CONTROLLED_STEP_PENDING);
    int state = session->backend->session_controlled_step_observe(session,
            WL_BACKEND_CONTROLLED_COMPLETION_STEP, true, NULL);
    CHECK(state == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING);
}
static void late_rollback_main_report(const wl_col_session_t *session,
    unsigned recipient, unsigned main_action,
    wl_evaluation_control_report_t report, int result) {
    (void)session; (void)recipient; (void)main_action;
    if (late_rollback_pending) {
        CHECK(report.limit == 1 && !report.consumed &&
            result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        late_rollback_pending = false; late_rollback_settled = true;
        late_rollback_refused_attempt = report.attempt_id;
        ++late_rollback_confirmed;
    } else if (late_rollback_refused_attempt) {
        CHECK(report.attempt_id != late_rollback_refused_attempt);
    }
}
static void late_matrix_stop(wl_evaluation_control_t *c, unsigned phase,
    uint32_t progress,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)bindings;
    fixture_t *f = active; if (!f || c != f->control || f->did_stop)return;
    wl_col_session_t *session = COL_SESSION(f->controlled);
    bool match = late_epoch == 0?(phase == 13 && progress == 0):
        late_epoch == 1?(phase == 13 && progress == session->nrels):phase == 14;
    if (!match)return;
    uint64_t view[16];
    CHECK(!wl_columnar_session_test_retirement_view(session, view));
    CHECK(view[1] == wl_evaluation_control_active_attempt_id(c) &&
        view[3] == phase && !view[5]);
    if (late_epoch) {
        CHECK(view[0]); /* Genuine registered reversible public-head claim. */
        col_rel_t *old = session_find_rel(session, "out"); CHECK(old);
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(wl_columnar_source_access_writer_acquire(&old->descriptor_access,
            &writer) == EBUSY);
    }
    session_source(c, 2100+late_epoch);
}
static void assert_original_frozen(fixture_t *f) {
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->control,
        &report) && reports_equal(&report, &f->original_report));
    if (f->next) {
        uint64_t used = 99;
        CHECK(!wl_evaluation_control_test_active_consumed(f->control, f->next,
            &used) && !used);
    }
}
static void snapshot_must_not_run(const char *name, const int64_t *row,
    uint32_t width, void *data) {
    (void)name; (void)row; (void)width; (void)data; CHECK(false);
}
static void observe_sentinel(wl_session_t *session) {
    wl_backend_controlled_completion_t out, before;
    memset(&out, 0xa5, sizeof(out)); out.selected = false;
    out.kind = WL_BACKEND_CONTROLLED_COMPLETION_CLOSE;
    out.state = WL_CONTROLLED_STEP_DIRTY;
    memcpy(&before, &out, sizeof(out));
    const wl_compute_backend_t *backend = session->backend;
    int state = backend->session_controlled_step_observe(session,
            WL_BACKEND_CONTROLLED_COMPLETION_STEP, true, &out);
    CHECK(state != WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY &&
        !memcmp(&out, &before, sizeof(out)));
    state = backend->session_controlled_step_observe(session,
            WL_BACKEND_CONTROLLED_COMPLETION_STEP, false, &out);
    CHECK(state == (int)backend->session_controlled_step_state(session) &&
        !memcmp(&out, &before, sizeof(out)));
    state = backend->session_controlled_step_observe(session,
            WL_BACKEND_CONTROLLED_COMPLETION_SNAPSHOT, true, &out);
    CHECK(state == (int)backend->session_controlled_step_state(session) &&
        !memcmp(&out, &before, sizeof(out)));
}
static void state_only_backend_pair(void) {
    fixture_t f = create("int64", "string", "to_string(x)", false, false);
    const wl_compute_backend_t *backend = wl_backend_columnar();
    CHECK(backend->session_controlled_step_observe &&
        backend->session_controlled_completion_commit);
    observe_sentinel(f.controlled);
    uint64_t baseline = reserved(f.g);
    for (unsigned controlled = 0; controlled < 2;
        ++controlled) for (unsigned missing =
            0;
            missing < 2; ++missing){
            wl_compute_backend_t bad = *backend;
            if (missing) bad.session_controlled_step_observe = NULL;
            else bad.session_controlled_completion_commit = NULL;
            wl_session_options_t options; wl_session_options_init(&options);
            options.memory_governor = f.g;
            options.evaluation_control = controlled?f.control:NULL;
            wl_session_t *out = NULL;
            CHECK(wl_session_create_with_options(&bad, f.plan, 1, &options,
                &out) == EINVAL && !out && reserved(f.g) == baseline);
        }
    destroy(&f);
}
static void late_two_head_stop(wl_evaluation_control_t *c, unsigned phase,
    uint32_t progress,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)bindings; fixture_t *f = active;
    if (!f || c != f->control || f->did_stop) return;
    bool match = late_epoch == 0?(phase == 13 &&
        progress == 0):late_epoch == 1?(phase == 13 &&
        progress == 3):phase == 14;
    if (!match)return;
    uint64_t view[16];
    CHECK(!wl_columnar_session_test_retirement_view(COL_SESSION(f->controlled),
        view));
    CHECK(COL_SESSION(f->controlled)->nrels == 4 && view[3] == phase);
    if (late_epoch == 1)CHECK(view[0] == 1 && view[15]);
    if (late_epoch == 2)CHECK(view[0] == 2);
    session_source(c, 2200+late_epoch);
}
static void late_two_head_matrix(void) {
    for (unsigned numeric = 0; numeric < 2; ++numeric)for (late_epoch = 0;
            late_epoch < 3; ++late_epoch)
            for (action = 2; action <= 3; ++action) {
                fixture_t f = {0}; f.g = governor(); f.width = 1;
                f.background = true; f.numeric = numeric != 0;
                char text[512];
                snprintf(text, sizeof(text),
                    ".decl input(x:int64)\n.decl other(x:int64)\n.decl out(y:%s)\n.decl base(y:int64)\nout(%s):-input(x).\nbase(x):-other(x).\n",
                    numeric?"int64":"string", numeric?"x+1":"to_string(x)");
                create_sessions(&f, text); hook(&f); insert(&f, 1, false);
                int64_t one = 1, two = 2;
                CHECK(!wl_session_insert(f.controlled, "other", &one, 1,
                    1) && !wl_session_insert(f.raw, "other", &one, 1, 1));
                CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
                col_rel_t *old_out = session_find_rel(COL_SESSION(f.controlled),
                        "out"),
                    *old_base = session_find_rel(COL_SESSION(f.controlled),
                        "base"),
                    *other = session_find_rel(COL_SESSION(f.controlled),
                        "other");
                CHECK(old_out && old_base && other && f.events == 1 &&
                    f.base_events == 1);
                col_relation_snapshot_t out_before =
                    wl_columnar_relation_snapshot(old_out),
                    base_before = wl_columnar_relation_snapshot(old_base);
                insert(&f, 2, false);
                CHECK(!wl_session_insert(f.controlled, "other", &two, 1,
                    1) && !wl_session_insert(f.raw, "other", &two, 1, 1));
                CHECK(!wl_evaluation_control_reset(f.control));
                col_relation_snapshot_t input_before =
                    wl_columnar_relation_snapshot(f.input),
                    other_before = wl_columnar_relation_snapshot(other);
                f.stop = 2200+late_epoch;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    late_two_head_stop;
                unsigned turns = 0; int rc = EAGAIN; bool discarded = false;
                for (; turns < 10000; ++turns) {
                    if (f.did_stop) assert_original_frozen(&f);
                    rc = wl_session_step(f.controlled);
                    if (f.did_stop) {
                        assert_original_frozen(&f); uint64_t view[16];
                        CHECK(!wl_columnar_session_test_retirement_view(
                                COL_SESSION(f.controlled), view));
                        discarded |= (view[5]&2) != 0;
                        CHECK(old_out ==
                            session_find_rel(COL_SESSION(f.controlled),
                            "out") &&
                            old_base ==
                            session_find_rel(COL_SESSION(f.controlled),
                            "base") &&
                            wl_columnar_relation_snapshot_equal(out_before,
                            wl_columnar_relation_snapshot(old_out)) &&
                            wl_columnar_relation_snapshot_equal(base_before,
                            wl_columnar_relation_snapshot(old_base)) &&
                            wl_columnar_relation_snapshot_equal(input_before,
                            wl_columnar_relation_snapshot(f.input)) &&
                            wl_columnar_relation_snapshot_equal(other_before,
                            wl_columnar_relation_snapshot(other)) &&
                            f.events == 1 && f.base_events == 1 &&
                            f.balance == 1 && f.base_balance[1] == 1 &&
                            !f.base_balance[2]);
                        if (rc == EAGAIN) CHECK(wl_session_insert(f.controlled,
                                "other", &two, 1, 1) == EBUSY);
                    }
                    if (rc != EAGAIN) break;
                }
                CHECK(turns < 10000 && f.did_stop && discarded && rc == EINVAL);
                uint64_t closed[16];
                CHECK(!wl_columnar_session_test_retirement_view(COL_SESSION(
                        f.controlled), closed) && !closed[0] && !closed[1]);
                gates_free(old_out); gates_free(old_base); gates_free(f.input);
                gates_free(other);
                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                f.stop = 0;
                if (f.next) {
                    wl_evaluation_control_attempt_finish_result_t end =
                        wl_evaluation_control_finish_attempt(f.control,
                            f.controlled->operation_admission, f.next, 0, 0);
                    CHECK(!end.protocol && end.report_valid &&
                        !end.report.consumed);
                }
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !complete(f.controlled) && !complete(f.raw)); equal(&f);
                CHECK(f.events == 2 && f.base_events == 2 &&
                    f.base_balance[1] == 1 && f.base_balance[2] == 1);
                col_rel_t *a = session_find_rel(COL_SESSION(f.controlled),
                        "base"),
                    *b = session_find_rel(COL_SESSION(f.raw), "base");
                CHECK(a && b && a->ncols == 1 && b->ncols == 1 &&
                    a->nrows == 2 && b->nrows == 2);
                for (uint32_t i = 0; i < 2;
                    ++i) CHECK(a->columns[0][i] == b->columns[0][i]);
                CHECK(!wl_session_try_destroy(&f.controlled) && !f.controlled);
                destroy(&f);
            }
}

static void late_end_matrix(void) {
    /* Numeric x+1 selects the genuine general host in both observer modes.
     * Single-head observed identity retains its separate narrow-route control. */
    for (unsigned numeric = 0; numeric < 2; ++numeric)for (unsigned observed =
            0;
            observed < 2; ++observed)
            for (unsigned graph = 0; graph < 2; ++graph) for (late_epoch = 0;
                    late_epoch < 3; ++late_epoch)
                    for (action = 2; action <= 3; ++action) {
                        fixture_t f = create("int64", numeric?"int64":"string",
                                numeric?"x+1":"to_string(x)", graph != 0,
                                false);
                        if (observed) hook(&f); else bind_unobserved(&f);
                        CHECK(!complete(f.controlled) && !complete(f.raw));
                        equal(&f);
                        insert(&f, 1, false);
                        CHECK(!wl_evaluation_control_reset(f.control));
                        CHECK(!complete(f.controlled) && !complete(f.raw));
                        equal(&f);
                        col_rel_t *old =
                            session_find_rel(COL_SESSION(f.controlled), "out");
                        col_rel_t *aux =
                            graph?session_find_rel(COL_SESSION(f.controlled),
                                "__graph_metadata"):NULL;
                        CHECK(old && (!graph || aux));
                        col_relation_snapshot_t old_before =
                            wl_columnar_relation_snapshot(old);
                        col_relation_snapshot_t aux_before =
                            aux?wl_columnar_relation_snapshot(aux):
                            (col_relation_snapshot_t){0};
                        insert(&f, 2, false);
                        CHECK(!wl_evaluation_control_reset(f.control));
                        col_relation_snapshot_t input_before =
                            wl_columnar_relation_snapshot(f.input);
                        unsigned events_before = f.events;
                        int64_t balance_before = f.balance;
                        f.stop = 2100+late_epoch;
                        wl_columnar_session_test_observed_transaction_checkpoint
                            = late_matrix_stop;
                        late_rollback_refusals_left = late_rollback_refusals;
                        late_rollback_refused = late_rollback_confirmed = 0;
                        late_rollback_pending = late_rollback_skip = false;
                        late_rollback_settled = false;
                        late_rollback_refused_attempt = 0;
                        wl_columnar_session_test_main_limit =
                            late_rollback_main_limit;
                        wl_columnar_session_test_main_action_report =
                            late_rollback_main_report;
                        unsigned turns = 0, drain = 0;
                        bool rollback = false, discard = false,
                            maintenance = false;
                        int rc = EAGAIN;
                        for (; turns < 10000; ++turns) {
                            if (f.did_stop) assert_original_frozen(&f);
                            rc = wl_session_step(f.controlled);
                            if (late_rollback_settled) {
                                /* Only the MAIN-funded rollback-debt rule
                                 * keeps a general route pending here. */
                                late_rollback_settled = false;
                                late_rollback_observe(f.controlled, rc);
                            }
                            if (f.did_stop) {
                                ++drain; assert_original_frozen(&f);
                                CHECK(old ==
                                    session_find_rel(COL_SESSION(f.controlled),
                                    "out") &&
                                    wl_columnar_relation_snapshot_equal(
                                        old_before,
                                        wl_columnar_relation_snapshot(old)) &&
                                    wl_columnar_relation_snapshot_equal(
                                        input_before,
                                        wl_columnar_relation_snapshot(
                                            f.input)) &&
                                    f.events == events_before &&
                                    f.balance == balance_before);
                                if (aux) CHECK(aux ==
                                        session_find_rel(COL_SESSION(
                                            f.controlled),
                                        "__graph_metadata") &&
                                        wl_columnar_relation_snapshot_equal(
                                            aux_before,
                                            wl_columnar_relation_snapshot(
                                                aux)));
                                uint64_t view[16];
                                CHECK(!wl_columnar_session_test_retirement_view(
                                        COL_SESSION(f.controlled), view));
                                rollback |= (view[5]&1) != 0;
                                discard |= (view[5]&2) != 0;
                                maintenance |= view[6] != 0;
                                observe_sentinel(f.controlled);
                                if (rc == EAGAIN) {
                                    int64_t row[] = {2, 9};
                                    CHECK(wl_session_insert(f.controlled,
                                        "input", row, 1, f.width) == EBUSY);
                                    CHECK(wl_session_remove(f.controlled,
                                        "input", row, 1, f.width) == EBUSY);
                                    CHECK(wl_session_snapshot(f.controlled,
                                        snapshot_must_not_run, NULL) == EBUSY);
                                    wl_session_set_delta_cb(f.controlled,
                                        observed?NULL:events, &f);
                                    CHECK(COL_SESSION(f.controlled)->delta_cb ==
                                        (observed?events:NULL));
                                    if (observed) CHECK(COL_SESSION(
                                                f.controlled)->delta_data ==
                                            &f);
                                    assert_original_frozen(&f);
                                }
                            }
                            if (rc != EAGAIN) break;
                        }
                        wl_columnar_session_test_main_limit = NULL;
                        wl_columnar_session_test_main_action_report = NULL;
                        /* Epoch-0 rollback has no begun Original retirement
                         * and runs no MAIN action. Later epochs retire real
                         * Originals through MAIN: every refused packet is
                         * retried by a fresh packet, never surfaced as EIO
                         * or replayed, and the drain still ends below. */
                        CHECK(late_rollback_refused ==
                            (late_epoch?late_rollback_refusals:0u) &&
                            late_rollback_confirmed == late_rollback_refused &&
                            !late_rollback_pending);
                        CHECK(turns < 10000 && f.did_stop && drain &&
                            rollback && discard && maintenance && rc == EINVAL);
                        CHECK(!COL_SESSION(f.controlled)->cleanup_active &&
                            !COL_SESSION(f.controlled)->cleanup_pending &&
                            !COL_SESSION(f.controlled)->delta_observer &&
                            !wl_columnar_session_test_cleanup_owners(
                                COL_SESSION(f.controlled)));
                        uint64_t closed[16];
                        CHECK(!wl_columnar_session_test_retirement_view(
                                COL_SESSION(f.controlled),
                                closed) && !closed[0] && !closed[1]);
                        gates_free(old); gates_free(f.input);
                        if (aux) gates_free(aux);
                        wl_columnar_session_test_observed_transaction_checkpoint
                            = NULL; f.stop = 0;
                        if (f.next) {
                            wl_evaluation_control_attempt_finish_result_t end =
                                wl_evaluation_control_finish_attempt(
                                f.control, f.controlled->operation_admission,
                                f.next, 0, 0);
                            CHECK(!end.protocol && end.report_valid &&
                                !end.report.consumed);
                        }
                        CHECK(!wl_evaluation_control_reset(f.control) &&
                            !wl_evaluation_control_configure(f.control, 0));
                        CHECK(!complete(f.controlled) && !complete(f.raw));
                        equal(&f);
                        CHECK(f.events == (observed?2u:0u));
                        CHECK(!wl_session_try_destroy(&f.controlled) &&
                            !f.controlled); destroy(&f);
                    }
}

static uint64_t graph_fee_entry[18], graph_fee_maintenance;
static unsigned graph_fee_seen;
static unsigned name_visits;
static uint64_t names_first_debit;
static void unobserved_transaction(wl_evaluation_control_t *c, unsigned phase,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings) {
    (void)bindings;
    if (!active || !active->no_observer || c != active->control)return;
    if (graph_fee_active && phase < 18 && !(graph_fee_seen&(1u<<phase))) {
        uint64_t used = 0;
        CHECK(!wl_evaluation_control_test_active_consumed(c,
            wl_evaluation_control_active_attempt_id(c), &used));
        graph_fee_seen |= 1u<<phase; graph_fee_entry[phase] = used;
        if (phase ==
            16) CHECK(!wl_columnar_session_test_general_maintenance_consumed(
                    COL_SESSION(active->controlled), &graph_fee_maintenance));
    }
    if (phase == 4 && progress == 0) {
        if (!name_visits) {
            uint64_t used = 0;
            CHECK(!wl_evaluation_control_test_active_consumed(c,
                wl_evaluation_control_active_attempt_id(c), &used) && used);
            names_first_debit = used;
        }
        ++name_visits;
    }
    if (active->stop == 1000+phase)session_source(c, 1000+phase);
}
static void unobserved_stop_report(fixture_t *f, int expected) {
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->control, &report));
    if (action == 2 || action == 3) {
        CHECK(reports_equal(&report, &f->original_report));
        if (action == 3) {
            uint64_t used = 99;
            CHECK(!wl_evaluation_control_test_active_consumed(f->control,
                f->next, &used) && !used);
            wl_evaluation_control_attempt_finish_result_t end =
                wl_evaluation_control_finish_attempt(f->control,
                    f->controlled->operation_admission, f->next, 0, 0);
            CHECK(!end.protocol && end.report_valid && !end.report.consumed);
        }
    }else CHECK(report.result == expected && !report.cleanup_status);
}
static void unobserved_body_final_stops(void) {
    const unsigned phases[] = {610, 83, 1014};
    for (unsigned k = 0; k < 3; ++k)for (action = 0; action < 5; ++action) {
            fixture_t f = create("int64", "string", "to_string(x)", false,
                    false); bind_unobserved(&f);
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            col_rel_t *old = session_find_rel(COL_SESSION(f.controlled), "out");
            col_relation_snapshot_t old_image =
                wl_columnar_relation_snapshot(old);
            insert(&f, 2, false);
            CHECK(!wl_evaluation_control_reset(f.control));
            col_relation_snapshot_t input_image =
                wl_columnar_relation_snapshot(f.input);
            f.stop = phases[k]; name_visits = 0;
            wl_columnar_session_test_observed_transaction_checkpoint =
                unobserved_transaction;
            CHECK(!wl_evaluation_control_configure(f.control,
                action == 1?UINT64_MAX:0));
            int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                1?WL_EVALUATION_CONTROL_WORK_LIMIT:action == 4?EOVERFLOW:EINVAL;
            int result = complete(f.controlled);
            if (result != expected || !f.did_stop ||
                session_find_rel(COL_SESSION(f.controlled),
                "out") != old || !wl_columnar_relation_snapshot_equal(old_image,
                wl_columnar_relation_snapshot(old)) ||
                !wl_columnar_relation_snapshot_equal(input_image,
                wl_columnar_relation_snapshot(f.input)))
                fprintf(stderr,
                    "unobserved stop phase%u action%u result%d expected%d hit%d oldptr%d oldsnapshot%d input%d\n",
                    f.stop, action, result, expected, f.did_stop,
                    session_find_rel(COL_SESSION(f.controlled), "out") == old,
                    wl_columnar_relation_snapshot_equal(old_image,
                    wl_columnar_relation_snapshot(old)),
                    wl_columnar_relation_snapshot_equal(input_image,
                    wl_columnar_relation_snapshot(f.input)));
            CHECK(result == expected && f.did_stop &&
                !f.events &&
                session_find_rel(COL_SESSION(f.controlled), "out") == old &&
                wl_columnar_relation_snapshot_equal(old_image,
                wl_columnar_relation_snapshot(old)) &&
                wl_columnar_relation_snapshot_equal(input_image,
                wl_columnar_relation_snapshot(f.input)) &&
                !COL_SESSION(f.controlled)->delta_observer);
            unobserved_stop_report(&f, expected);
            if (action == 1) {
                uint64_t held = reserved(f.g);
                for (unsigned repeat = 0; repeat < 2; ++repeat) {
                    f.did_stop = false;
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control,
                        UINT64_MAX));
                    CHECK(complete(f.controlled) == expected && f.did_stop &&
                        reserved(f.g) == held &&
                        session_find_rel(COL_SESSION(f.controlled),
                        "out") == old);
                }
            }
            f.stop = 0;
            wl_columnar_session_test_observed_transaction_checkpoint = NULL;
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control, 0));
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(f.numeric_frames && f.metadata && f.bodies && !f.events);
            destroy(&f);
        }
}
static void unobserved_long_names(void) {
    uint64_t first = 0;
    for (unsigned length = 32; length <= 240; length += 208) {
        char name[241], text[768]; memset(name, 'a', length); name[length] = 0;
        snprintf(text, sizeof(text),
            ".decl %s(x:int64)\n.decl out(y:string)\nout(to_string(x)):-%s(x).\n",
            name, name);
        fixture_t f = {0}; f.g = governor(); f.width = 1;
        wirelog_error_t error; f.program = wirelog_parse_string(text, &error);
        CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
        wl_session_options_t options; wl_session_options_init(&options);
        options.memory_governor = f.g;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
            &options, &f.raw));
        CHECK(!wl_evaluation_control_create(0, &f.control));
        options.evaluation_control = f.control;
        CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
            &options, &f.controlled));
        f.input = session_find_rel(COL_SESSION(f.controlled), name);
        CHECK(f.input); bind_unobserved(&f);
        name_visits = 0; names_first_debit = 0;
        wl_columnar_session_test_observed_transaction_checkpoint =
            unobserved_transaction;
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(name_visits >= length+1 && names_first_debit && f.source_calls &&
            f.metadata && f.numeric_frames && !f.events);
        if (first)CHECK(first == names_first_debit);
        else first = names_first_debit;
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
        destroy(&f);
    }
}

static void unobserved_graph_fee_oracle(void) {
    uint64_t entries[2][18], maintenance[2], totals[2];
    unsigned metadata[2], frames[2], sources[2], source_witness[2],
        aux_visits[2];
    uint32_t source_counts[2];
    for (unsigned graph = 0; graph < 2; ++graph) {
        fixture_t f = create("int64", "string", "to_string(x)", graph != 0,
                false); bind_unobserved(&f);
        if (graph)CHECK(!strcmp(COL_SESSION(f.controlled)->rels[1]->name,
                "__graph_metadata"));
        graph_fee_active = true; graph_fee_seen = 0; graph_fee_maintenance = 0;
        graph_source_witness = graph_framed_aux_witness = graph_source_count =
            0;
        wl_columnar_eval_serial_test_framed_checkpoint = graph_framed_visit;
        name_visits = 0;
        memset(graph_fee_entry, 0, sizeof(graph_fee_entry));
        wl_columnar_session_test_observed_transaction_checkpoint =
            unobserved_transaction;
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        const unsigned phases[] = {3, 6, 7, 8, 12, 13, 14, 15, 16};
        for (unsigned k = 0; k < sizeof(phases)/sizeof(*phases);
            ++k) CHECK(graph_fee_seen&(1u<<phases[k]));
        memcpy(entries[graph], graph_fee_entry, sizeof(graph_fee_entry));
        maintenance[graph] = graph_fee_maintenance;
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        totals[graph] = report.consumed;
        metadata[graph] = f.metadata; frames[graph] = f.numeric_frames;
        sources[graph] = f.source_calls;
        source_witness[graph] = graph_source_witness;
        aux_visits[graph] = graph_framed_aux_witness;
        source_counts[graph] = graph_source_count;
        wl_columnar_eval_serial_test_framed_checkpoint = NULL;
        graph_fee_active = false;
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
        destroy(&f);
    }
    /* Only ABI geometry comes from the real private record. Prices64,9,4,
     * per-lane SCHEMA256, other actions1, cutoff19->20 and two real128 aux visits (framed head
     * lookup and composed VARIABLE DELTA lookup) are source-derived literals.
     * No observed whole-host total is used to choose a work budget. */
    uint64_t bytes = wl_columnar_session_test_general_original_bytes();
    uint64_t zero = 64u*((2u*bytes+63u)/64u-(bytes+63u)/64u);
    const unsigned phases[] = {3, 6, 7, 8, 12, 13, 14, 15, 16};
    const uint64_t suffix[] = {0, 9, 13, 269, 526, 527, 528, 529, 529};
    for (unsigned k = 0; k < sizeof(phases)/sizeof(*phases); ++k) {
        unsigned phase = phases[k];
        CHECK(entries[1][phase] >= entries[0][phase]);
        CHECK(entries[1][phase]-entries[0][phase] == zero+suffix[k]);
    }
    CHECK(totals[1]-totals[0] == zero+529 &&
        maintenance[1]-maintenance[0] == 2);
    CHECK(metadata[0] && metadata[0] == metadata[1] && frames[0] &&
        frames[0] == frames[1] && sources[0] && sources[0] == sources[1]);
    CHECK(source_witness[0] && source_witness[0] == source_witness[1] &&
        source_counts[0] == 3 && source_counts[1] == 4 && aux_visits[0] == 0 &&
        aux_visits[1] == 1);
    if (bytes == 1224)CHECK(zero == 1216 && totals[1]-totals[0] == 1745);
}

static void unobserved_missing_matrix(void) {
    const char *exprs[] = {"x", "x+1", "to_string(x)", "strlen(to_string(x))",
                           "\"constant\""};
    for (unsigned k = 0; k < 5; ++k) {
        fixture_t f = create("int64", k == 2 || k == 4?"string":"int64",
                exprs[k], false, false);
        CHECK(COL_SESSION(f.controlled)->nrels == 1 && !f.input->ncols &&
            !f.input->schema_ok);
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(f.input);
        /* Actual raw MAP is the operator schema oracle, before session
        * effective/public type defaults can hide an UNKNOWN result. */
        eval_stack_t raw_map = {0};
        CHECK(!eval_stack_push(&raw_map,
            session_find_rel(COL_SESSION(f.raw), "input"), false));
        CHECK(!col_op_map(&f.plan->strata[0].relations[0].ops[1], &raw_map,
            COL_SESSION(f.raw)));
        CHECK(raw_map.top == 1 && raw_map.items[0].rel->ncols == 1 &&
            !raw_map.items[0].rel->nrows);
        f.empty_known = raw_map.items[0].rel->column_types != NULL;
        CHECK(!eval_stack_drain(&raw_map)); bind_unobserved(&f);
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        /* numeric_frames counts frames that schema() saw while the general
         * route held its initialized transaction (flags 5). After the
         * healthy completion, the completed owner (#2368) has disposed of
         * that transaction, so only the general-route bit remains. */
        CHECK(f.source_calls && f.numeric_frames && !f.events &&
            wl_columnar_session_test_general_route_flags(COL_SESSION(
                f.controlled)) == 1u &&
            wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(f.input)));
        insert(&f, 2, false); CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK((k < 2 || f.bodies) && !f.events && !f.balance);
        insert(&f, 2, true); CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        insert(&f, 2, false); CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        int64_t wrong[] = {9, 8};
        CHECK(wl_session_insert(f.controlled, "input", wrong, 1, 2) == ENOTSUP);
        CHECK(wl_session_insert(f.raw, "input", wrong, 1, 2) == EINVAL);
        CHECK(f.input == session_find_rel(COL_SESSION(f.controlled), "input") &&
            f.input->declared_ncols == 1 && !f.events &&
            !COL_SESSION(f.controlled)->delta_observer);
        destroy(&f);
    }
    for (unsigned graph = 0; graph < 2; ++graph)for (unsigned filter = 0;
            filter < 2;
            ++filter) {
            fixture_t f = create("int64", "string", "to_string(x)", graph != 0,
                    filter != 0);
            uint32_t initial = COL_SESSION(f.controlled)->nrels;
            CHECK(initial == (graph?2u:1u));
            col_rel_t *aux = graph?COL_SESSION(f.controlled)->rels[1]:NULL;
            col_relation_snapshot_t aux_before =
                aux?wl_columnar_relation_snapshot(aux):
                (col_relation_snapshot_t){0};
            bind_unobserved(&f); int expected = graph && filter?EINVAL:0;
            CHECK(complete(f.raw) == expected &&
                complete(f.controlled) == expected);
            if (!expected) equal(&f);
            CHECK(!f.events && !f.input->ncols && !f.input->schema_ok &&
                f.input->has_graph_column == (graph != 0));
            insert(&f, 2, false);
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(!f.events && f.metadata && f.bodies);
            if (aux) {
                unsigned count = 0; for (uint32_t i = 0;
                    i < COL_SESSION(f.controlled)->nrels;
                    ++i) count += COL_SESSION(f.controlled)->rels[i] == aux;
                CHECK(count == 1 &&
                    wl_columnar_relation_snapshot_equal(aux_before,
                    wl_columnar_relation_snapshot(aux))); gates_free(aux);
            }
            destroy(&f);
        }
}

static void unobserved_selector_controls(void) {
    /* Real copied typed-empty sources are outside the new bare hint. */
    const char *types[] = {"int64", "string", "float"};
    for (unsigned kind = 0; kind < 3; ++kind){
        fixture_t f = create(types[kind], "string",
                kind == 1?"cat(x,\"!\")":"to_string(x)", false, false);
        wl_session_t *sessions[] = {f.raw, f.controlled};
        for (unsigned n = 0; n < 2; ++n){
            wl_col_session_t *session = COL_SESSION(sessions[n]);
            col_rel_t *old = session_find_rel(session, "input"), *seed = old,
                *copy = NULL;
            int64_t type = kind == 0?WIRELOG_TYPE_INT64:kind ==
                1?WIRELOG_TYPE_STRING:WIRELOG_TYPE_FLOAT;
            wl_evaluation_control_t *setup = begin(0); seed = NULL;
            CHECK(
                !
                wl_columnar_relation_new_typed_codes_operator_attempt_controlled
                    ("input", 1, 0, &type, f.g, &seed, setup,
                wl_evaluation_control_active_attempt_id(setup)) && seed);
            finish(setup, 0);
            int copy_rc = wl_columnar_relation_deep_copy_governed(seed, &copy,
                    f.g);
            CHECK(!copy_rc && copy && copy->ncols == 1 && !copy->nrows &&
                !copy->capacity && copy->schema_ok);
            if (seed != old)CHECK(!col_rel_destroy_checked(seed));
            session_rel_free_hash(session);
            CHECK(session->nrels == 1 && session->rels[0] == old);
            session->rels[0] = copy;
            CHECK(!col_rel_destroy_checked(old)); if (n)f.input = copy;
        }
        active = &f; wl_columnar_filter_test_map_schema = schema;
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(!(wl_columnar_session_test_general_route_flags(COL_SESSION(
                f.controlled))&1u) && !f.events &&
            !COL_SESSION(f.controlled)->delta_cb &&
            !COL_SESSION(f.controlled)->delta_observer);
        destroy(&f);
    }
    /* A routing hint cannot bless malformed physical metadata. Real child
     * readers reject the noncanonical bare capacity before any frame is born. */
    fixture_t f = create("int64", "string", "to_string(x)", false, false);
    bind_unobserved(&f);
    uint64_t baseline = reserved(f.g); f.input->capacity = 1;
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(f.input);
    CHECK(complete(f.controlled) == EINVAL && f.source_calls && !f.metadata &&
        !f.bodies && !f.events &&
        !session_find_rel(COL_SESSION(f.controlled),
        "out") && f.input->capacity == 1 && reserved(f.g) == baseline &&
        wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(f.input)));
    closed(&f); f.input->capacity = 0;
    CHECK(!wl_evaluation_control_reset(f.control) && !complete(f.controlled) &&
        !complete(f.raw)); equal(&f);
    CHECK(f.metadata && f.numeric_frames && !f.events); destroy(&f);
}

static void unobserved_unknown_variable(void) {
    for (unsigned observed = 0; observed < 2; ++observed) {
        fixture_t f = create("int64", "string", "to_string(x)", false, false);
        wl_plan_op_t *var =
            (wl_plan_op_t *)&f.plan->strata[0].relations[0].ops[0];
        const char *name = var->relation_name;
        var->relation_name = "not_the_registered_edb";
        if (observed)hook(&f); else bind_unobserved(&f);
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(f.input);
        CHECK(complete(f.raw) == ENOENT && complete(f.controlled) == ENOENT);
        CHECK(!f.events && !f.metadata && !f.bodies &&
            !session_find_rel(COL_SESSION(f.controlled), "out") &&
            wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(f.input)));
        var->relation_name = name; destroy(&f);
    }
}

static void future_graph_output_guards(void) {
    col_rel_t *r = bare(); r->declared_ncols = 2; r->has_graph_column = true;
    r->graph_col_idx = 1;
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_evaluation_control_t *c = begin(0);
    col_rel_t *input = NULL;
    CHECK(!wl_columnar_relation_new_empty_input_operator_attempt_controlled(
            "input", r, NULL, g, &input, c,
            wl_evaluation_control_active_attempt_id(c)) && input);
    finish(c, 0);
    CHECK(!input->ncols && input->schema_ok && input->declared_ncols == 2 &&
        input->has_graph_column && input->graph_col_idx == 1);
    col_rel_t *raw = NULL;
    CHECK(wl_columnar_relation_new_like_governed_checked_mode("output", input,
        g, false, &raw) == EINVAL && !raw);
    uint64_t baseline = reserved(g);
    /* Real scalar source open1632, unchanged. New output guard follows LIKE128
     * with256 in all three siblings. Exact/one-short EACH positive output cut. */
    const uint64_t prefix[] = {128, 384};
    for (unsigned variant = 0; variant < 5; ++variant)for (unsigned cut = 0;
            cut < 2;
            ++cut)
            for (unsigned below = 0; below < 2; ++below) {
                c = begin(1632+prefix[cut]-below);
                wl_columnar_relation_scalar_source_t source;
                bool initialized = false;
                uint64_t id = wl_evaluation_control_active_attempt_id(c);
                CHECK(
                    !wl_columnar_relation_scalar_source_init_attempt_controlled(
                        &source, &initialized, c, id) && initialized);
                CHECK(
                    !wl_columnar_relation_scalar_source_begin_attempt_controlled
                        (&source, input, false));
                col_rel_t *out = input; int rc;
                if (!variant) rc =
                        wl_columnar_relation_scalar_source_new_like_attempt_controlled
                            (&source, "output", g, &out);
                else if (variant ==
                    1) rc =
                        wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
                            (&source, "output", g, &out);
                else if (variant == 2) rc =
                        wl_columnar_relation_scalar_source_new_like_typed_operator_attempt_controlled
                            (&source, "output", g, NULL, &out);
                else rc =
                        wl_columnar_relation_scalar_source_new_like_anti_operator_attempt_controlled
                        (
                        &source, "output", variant == 3?g:NULL, &out);
                CHECK(rc == (!below &&
                    cut ==
                    1?(variant ==
                    4?ENOMEM:EINVAL):WL_EVALUATION_CONTROL_WORK_LIMIT) && !out);
                uint64_t used = 99;
                CHECK(!wl_evaluation_control_test_active_consumed(c, id,
                    &used) &&
                    used == 1632+(below?(cut?128:0):prefix[cut]));
                CHECK(wl_columnar_relation_scalar_source_end(&source,
                    rc) == rc);
                gates_free(input); CHECK(reserved(g) == baseline);
                finish(c, rc);
            }
    CHECK(!col_rel_destroy_checked(input) && !col_rel_destroy_checked(r) &&
        !reserved(g));
    wl_columnar_memory_governor_ref_release(g);
}

static void anti_graph_checkpoint(wl_evaluation_control_t *c, uint64_t work) {
    if (c == active_control && !stopped && work == 256)stop_now(c);
}
static void anti_graph_control_priority(void) {
    col_rel_t *r = bare(); r->declared_ncols = 2; r->has_graph_column = true;
    r->graph_col_idx = 1;
    wl_evaluation_control_t *setup = begin(0); col_rel_t *input = NULL;
    CHECK(!wl_columnar_relation_new_empty_input_operator_attempt_controlled(
            "input", r, NULL, NULL, &input, setup,
            wl_evaluation_control_active_attempt_id(setup)));
    finish(setup, 0);
    for (unsigned variant = 0; variant < 4; ++variant)for (action = 0;
            action < 5;
            ++action){
            active_control = begin(action == 1?UINT64_MAX:0); successor = 0;
            stopped = false;
            uint64_t id =
                wl_evaluation_control_active_attempt_id(active_control);
            wl_columnar_relation_scalar_source_t source;
            bool initialized = false;
            CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(
                    &source, &initialized, active_control, id) && initialized);
            CHECK(!wl_columnar_relation_scalar_source_begin_attempt_controlled(
                    &source, input, false));
            col_rel_t *out = input;
            wl_columnar_relation_test_work_checkpoint = anti_graph_checkpoint;
            int rc;
            if (!variant) rc =
                    wl_columnar_relation_scalar_source_new_like_anti_operator_attempt_controlled
                        (&source, "output", NULL, &out);
            else if (variant ==
                1) rc =
                    wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
                        (&source, "output", NULL, &out);
            else if (variant ==
                2) rc =
                    wl_columnar_relation_scalar_source_new_like_attempt_controlled
                        (&source, "output", NULL, &out);
            else rc =
                    wl_columnar_relation_scalar_source_new_like_typed_operator_attempt_controlled
                        (&source, "output", NULL, NULL, &out);
            wl_columnar_relation_test_work_checkpoint = NULL;
            int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:action ==
                1?WL_EVALUATION_CONTROL_WORK_LIMIT:action == 4?EOVERFLOW:EINVAL;
            CHECK(stopped && rc == expected && !out);
            CHECK(wl_columnar_relation_scalar_source_end(&source, rc) == rc);
            gates_free(input);
            if (action == 2 || action == 3) {
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(active_control,
                    &report) && reports_equal(&report, &frozen));
                if (action == 3) {
                    uint64_t used = 99;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            active_control, successor, &used) && !used);
                    finish(active_control, 0);
                }else {
                    CHECK(!wl_evaluation_control_detach(active_control,
                        &owner));
                    wl_evaluation_control_release(active_control);
                }
            }else finish(active_control, rc);
            active_control = NULL;
        }
    CHECK(!col_rel_destroy_checked(input) && !col_rel_destroy_checked(r));
}

static void anti_bare_graph_errors(void) {
    fixture_t f = create("int64", "string", "to_string(x)", true, false);
    CHECK(!f.input->ncols && !f.input->schema_ok && f.input->has_graph_column);
    uint64_t baseline = reserved(f.g);
    for (unsigned variant = 0; variant < 3; ++variant){
        wl_evaluation_control_t *c = begin(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_relation_empty_source_t source = {0};
        CHECK(!wl_columnar_relation_empty_source_begin(&source, f.input, c,
            id));
        bool held = false;
        for (unsigned turn = 0; !held; ++turn){
            CHECK(turn < 100);
            CHECK(!wl_columnar_relation_empty_source_step(&source, &held));
        }
        col_rel_t *out = NULL;
        int rc =
            variant?
            wl_columnar_relation_empty_source_new_like_anti_operator_attempt_controlled
            (
            &source, "output", variant == 1?f.g:NULL, &out):
            wl_columnar_relation_empty_source_new_like_operator_attempt_controlled
                (&source, "output", NULL, &out);
        CHECK(rc == (variant == 2?ENOMEM:EINVAL) && !out);
        wl_evaluation_control_t *funding = begin(0); bool done = false;
        for (unsigned turn = 0; !done; ++turn){
            CHECK(turn < 100);
            CHECK(!wl_columnar_relation_empty_source_end(&source, funding,
                wl_evaluation_control_active_attempt_id(funding), &done));
        }
        finish(funding, 0); finish(c, rc); gates_free(f.input);
        CHECK(reserved(f.g) == baseline);
    }
    destroy(&f);
}

static col_rel_t *timestamp_source(void) {
    col_rel_t *r = bare(), *out = NULL; r->has_graph_column = true;
    r->graph_col_idx = 1; r->declared_ncols = 2;
    wl_evaluation_control_t *c = begin(0);
    CHECK(!wl_columnar_relation_new_empty_input_operator_attempt_controlled(
            "source", r, NULL, NULL, &out, c,
            wl_evaluation_control_active_attempt_id(c)) && out);
    finish(c, 0);
    CHECK(!col_rel_destroy_checked(r) && !col_rel_enable_timestamps(out));
    return out;
}
/* Independent true-mode constructor vector; actual source is schema-valid,
* hence no bare header chunks/run records. Descriptor/TS byte copies express
* only sizeof ABI geometry, with explicit numeric224 eight-byte packets. */
static prices_t timestamp_prices(const char *name, bool governed) {
    prices_t p = {0};
#define F(n) fee(&p, n)
#define R(n, c) do {for (size_t i = 0; i < (size_t)(c); ++i) F(n);}while (0)
    const uint16_t prefix[] = {256, 256, 96, 144, 256, 96, 144, 256, 256, 256,
                               256};
    /* 620/621/readers/622/623/627/629/636. */
    for (size_t k = 0; k < sizeof(prefix)/sizeof(*prefix); ++k)F(prefix[k]);
    F(192); R(96, strlen(name)+1); R(96, 10); if (governed)F(96);
    F(128); F(96); F(160); if (governed) {
        F(74); F(54); F(84); F(29);
    }
    F(112); R(224, (sizeof(col_rel_t)+7u)/8u);
    R(96, 5); F(112); R(224, (strlen(name)+8u)/8u); F(80); F(112); F(96);
    F(160); if (governed) {
        F(74); F(54); F(84); F(29);
    }
    F(160); if (governed) {
        F(74); F(54); F(84); F(29);
    }
    F(112); R(224, (64u*sizeof(col_delta_timestamp_t)+7u)/8u);
    F(96); F(112); F(224);
    for (unsigned k = 0; k < 3; ++k) {
        F(128); if (governed){
            F(74); F(54); F(29);
        }
    }
    F(220); F(256); F(256);
#undef R
#undef F
    return p;
}
static void timestamp_constructor_prices(void) {
    for (unsigned named = 0; named < 2; ++named)for (unsigned governed = 0;
            governed < 2; ++governed) {
            const char *name = named?"$consol":"input";
            prices_t p = timestamp_prices(name, governed != 0);
            if (sizeof(col_rel_t) == 944 &&
                sizeof(col_delta_timestamp_t) == 24) CHECK(p.total ==
                    (governed?(named?78502u:78310u):(named?77212u:77020u)));
            col_rel_t *r = timestamp_source();
            wl_columnar_memory_governor_ref_t *g = governor();
            col_relation_snapshot_t before = wl_columnar_relation_snapshot(r);
            uint64_t sum = 0;
            for (size_t cut = 0; cut < p.count; ++cut) {
                sum += p.fee[cut]; for (unsigned below = 0; below < 2; ++below){
                    wl_evaluation_control_t *c = begin(sum-below);
                    col_rel_t *out = r;
                    int rc =
                        wl_columnar_relation_new_empty_timestamp_input_operator_attempt_controlled
                        (
                        name, r, NULL, governed?g:NULL, &out, c,
                        wl_evaluation_control_active_attempt_id(c));
                    uint64_t used = 99;
                    CHECK(!wl_evaluation_control_test_active_consumed(c,
                        wl_evaluation_control_active_attempt_id(c), &used));
                    if (cut+1 == p.count && !below) {
                        CHECK(!rc && out && out != r && out->schema_ok &&
                            !out->ncols && !out->nrows &&
                            out->timestamps && out->timestamp_capacity == 64 &&
                            out->has_graph_column &&
                            out->graph_col_idx == 1 &&
                            out->declared_ncols == 2 && used == p.total);
                        CHECK(!col_rel_destroy_checked(out));
                    }else CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                            !out && used == (below?sum-p.fee[cut]:sum));
                    CHECK(!reserved(g) &&
                        wl_columnar_memory_governor_ref_is_sole(g) &&
                        wl_columnar_relation_snapshot_equal(before,
                        wl_columnar_relation_snapshot(r)) && r->timestamps);
                    gates_free(r); finish(c, rc);
                }
            }
            CHECK(sum == p.total && !col_rel_destroy_checked(r));
            wl_columnar_memory_governor_ref_release(g);
        }
}

static void timestamp_constructor_resources(void) {
    col_rel_t *input = timestamp_source();
    wl_columnar_memory_governor_ref_t *g = governor();
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(input);
    for (unsigned allocation = 1; allocation <= 4; ++allocation) {
        for (unsigned repeat = 0; repeat < 2; ++repeat) {
            wl_evaluation_control_t *c = begin(0); col_rel_t *out = NULL;
            wl_columnar_relation_test_constructor_fail_allocation = allocation;
            CHECK(
                wl_columnar_relation_new_empty_timestamp_input_operator_attempt_controlled
                (
                    "$consol", input, NULL, g, &out, c,
                    wl_evaluation_control_active_attempt_id(c)) == ENOMEM &&
                !out &&
                !wl_columnar_relation_test_constructor_fail_allocation &&
                !reserved(g));
            gates_free(input);
            CHECK(input->timestamps && wl_columnar_relation_snapshot_equal(
                    before, wl_columnar_relation_snapshot(input)));
            finish(c, ENOMEM);
        }
        wl_evaluation_control_t *c = begin(0); col_rel_t *out = NULL;
        CHECK(
            !
            wl_columnar_relation_new_empty_timestamp_input_operator_attempt_controlled
            (
                "$consol", input, NULL, g, &out, c,
                wl_evaluation_control_active_attempt_id(c)) && out &&
            out->timestamps);
        CHECK(!col_rel_destroy_checked(out) && !reserved(g)); finish(c, 0);
    }
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(g);
    atomic_store_explicit(&budget->usable_bytes, 0, memory_order_seq_cst);
    wl_evaluation_control_t *c = begin(0); col_rel_t *out = NULL;
    CHECK(
        wl_columnar_relation_new_empty_timestamp_input_operator_attempt_controlled
        (
            "$consol", input, NULL, g, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOSPC && !out &&
        !reserved(g));
    gates_free(input); finish(c, ENOSPC);
    atomic_store_explicit(&budget->usable_bytes, UINT64_C(128)<<20,
        memory_order_seq_cst);
    c = begin(0);
    CHECK(
        !
        wl_columnar_relation_new_empty_timestamp_input_operator_attempt_controlled
        (
            "$consol", input, NULL, g, &out, c,
            wl_evaluation_control_active_attempt_id(c)) && out &&
        out->timestamps);
    CHECK(!col_rel_destroy_checked(out) && !col_rel_destroy_checked(input) &&
        !reserved(g)); finish(c, 0);
    wl_columnar_memory_governor_ref_release(g);
}
static const char *inferred_programs[] = {
    "input(1). out(to_string(x)):-input(x).",
    ".decl input(x:int64)\n.decl out(y:string)\ninput(1). out(to_string(x)):-input(x).",
    "input(\"abc\"). out(strlen(x)):-input(x).",
    ".decl input(x:string)\n.decl out(y:int64)\ninput(\"abc\"). out(strlen(x)):-input(x).",
    "input(1.5). out(to_string(x)):-input(x).",
    ".decl input(x:float)\n.decl out(y:string)\ninput(1.5). out(to_string(x)):-input(x)."
};
static unsigned inferred_private, inferred_legacy;
static void inferred_expression(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    if (!active || c != active->control || (phase != 610 && phase != 83))return;
    wl_col_session_t *session = COL_SESSION(active->controlled);
    if (wl_columnar_session_test_general_route_flags(session)&1u) {
        bool saved = active->no_observer;
        active->no_observer = session->delta_cb == NULL;
        expression(c, phase, progress, failed); active->no_observer = saved;
        ++inferred_private;
    }else{
        /* Existing plain/C1 execution has its own qualified frame/retirement
         * route; observed-unary ownership is not inferred from it. */
        CHECK(!session->delta_cb && !session->delta_observer);
        ++inferred_legacy;
    }
}
static void bind_inferred(fixture_t *f, bool observed) {
    active = f; if (observed)wl_session_set_delta_cb(f->controlled, events, f);
    inferred_private = inferred_legacy = 0;
    wl_columnar_filter_test_map_schema = schema;
    wl_columnar_expr_test_checkpoint = inferred_expression;
    wl_columnar_relation_test_empty_input_checkpoint = session_source;
}
static fixture_t inferred_create(unsigned kind) {
    fixture_t f = {0}; f.g = governor(); f.width = 1;
    create_sessions(&f, inferred_programs[kind]); return f;
}
static int64_t inferred_value(fixture_t *f, unsigned kind, bool second) {
    if (kind/2 == 1)return wl_intern_put(f->plan->intern, second?"abcd":"abc");
    if (kind/2 == 2)return wl_columnar_float_to_bits(second?2.5:1.5);
    return second?2:1;
}
static void inferred_equal(fixture_t *f, unsigned kind) {
    equal(f);
    col_rel_t *a = session_find_rel(COL_SESSION(f->raw), "out"),
        *b = session_find_rel(COL_SESSION(f->controlled), "out");
    CHECK(a && b);
    if (kind/2 != 1)for (uint32_t i = 0; i < a->nrows; ++i) {
            const char *x = wl_intern_reverse(f->plan->intern,
                    a->columns[0][i]),
                *y = wl_intern_reverse(f->plan->intern, b->columns[0][i]);
            CHECK(x && y && !strcmp(x, y));
        }
}
static void inferred_step(fixture_t *f, unsigned kind) {
    CHECK(!complete(f->controlled) && !complete(f->raw));
    inferred_equal(f, kind);
}
static void inferred_six_domain_histories(void) {
    for (unsigned kind = 0; kind < 6; ++kind)for (unsigned observed = 0;
            observed < 2;
            ++observed) for (unsigned loaded = 0; loaded < 2; ++loaded){
                fixture_t f = inferred_create(kind);
                bool undeclared = (kind&1u) == 0;
                CHECK(f.plan->edb_count == 1 &&
                    f.plan->edb_column_type_counts[0] == 1 &&
                    f.plan->edb_declared_width[0] ==
                    (undeclared?WL_PLAN_WIDTH_UNDECLARED:1u) &&
                    f.input->declared_ncols == (undeclared?0u:1u));
                if (loaded) CHECK(!wl_session_load_facts(f.controlled,
                        f.program) && !wl_session_load_facts(f.raw, f.program));
                col_relation_snapshot_t before =
                    wl_columnar_relation_snapshot(f.input);
                bind_inferred(&f, observed != 0); inferred_step(&f, kind);
                CHECK(f.input == session_find_rel(COL_SESSION(f.controlled),
                    "input") &&
                    f.input->declared_ncols == (undeclared?0u:1u) &&
                    wl_columnar_relation_snapshot_equal(before,
                    wl_columnar_relation_snapshot(f.input)));
                /* Replace callbacks after a completed host to exercise its
                 * separate re-admission policy on the next real mutation. */
                wl_session_set_delta_cb(f.controlled, observed?NULL:events,
                    observed?NULL:&f);
                if (!loaded) {
                    CHECK(!f.input->nrows && f.input->ncols == (kind/2?1u:0u));
                    CHECK(!wl_session_load_facts(f.controlled,
                        f.program) && !wl_session_load_facts(f.raw, f.program));
                    CHECK(!wl_evaluation_control_reset(f.control));
                    inferred_step(&f, kind);
                }
                col_rel_t *out = session_find_rel(COL_SESSION(f.controlled),
                        "out");
                CHECK(out && out->nrows == 1 && f.input->nrows == 1);
                if (kind/2 == 1) CHECK(out->columns[0][0] == 3);
                else CHECK(!strcmp(wl_intern_reverse(f.plan->intern,
                        out->columns[0][0]),
                        kind/2 == 2?"4609434218613702656":"1"));
                int64_t two = inferred_value(&f, kind, true);
                CHECK(!wl_session_insert(f.controlled, "input", &two, 1,
                    1) && !wl_session_insert(f.raw, "input", &two, 1, 1));
                CHECK(!wl_evaluation_control_reset(f.control));
                inferred_step(&f, kind);
                CHECK(session_find_rel(COL_SESSION(f.controlled),
                    "out")->nrows == 2);
                before = wl_columnar_relation_snapshot(f.input);
                col_relation_snapshot_t model =
                    wl_columnar_relation_snapshot(session_find_rel(COL_SESSION(
                            f.controlled), "out"));
                unsigned prior = f.events; int64_t wrong[] = {two, two};
                CHECK(wl_session_insert(f.raw, "input", wrong, 1, 2) == EINVAL);
                CHECK(wl_session_insert(f.controlled, "input", wrong, 1,
                    2) == (undeclared?EINVAL:ENOTSUP));
                CHECK(wl_columnar_relation_snapshot_equal(before,
                    wl_columnar_relation_snapshot(f.input)) &&
                    wl_columnar_relation_snapshot_equal(model,
                    wl_columnar_relation_snapshot(session_find_rel(COL_SESSION(
                        f.controlled), "out"))) && f.events == prior);
                CHECK(wl_session_insert(f.controlled, "missing", &two, 1,
                    1) == ENOTSUP && wl_session_insert(f.controlled, "out",
                    &two, 1, 1) == ENOTSUP);
                CHECK(!wl_session_remove(f.controlled, "input", &two, 1,
                    1) && !wl_session_remove(f.raw, "input", &two, 1, 1));
                CHECK(!wl_evaluation_control_reset(f.control));
                inferred_step(&f, kind);
                CHECK(session_find_rel(COL_SESSION(f.controlled),
                    "out")->nrows == 1);
                CHECK(!wl_session_insert(f.controlled, "input", &two, 1,
                    1) && !wl_session_insert(f.raw, "input", &two, 1, 1));
                CHECK(!wl_evaluation_control_reset(f.control));
                inferred_step(&f, kind);
                prior = f.events;
                CHECK(!wl_evaluation_control_reset(f.control));
                inferred_step(&f, kind); CHECK(f.events == prior);
                CHECK(f.input->declared_ncols == (undeclared?0u:1u) &&
                    f.input == session_find_rel(COL_SESSION(f.controlled),
                    "input"));
                destroy(&f);
            }
}
static void inferred_first_arity(void) {
    for (unsigned observed = 0; observed < 2;
        ++observed) for (unsigned before_empty =
            0;
            before_empty < 2; ++before_empty) for (unsigned arity =
                0; arity < 4;
                ++arity) {
                fixture_t f = inferred_create(0);
                bind_inferred(&f, observed != 0);
                if (before_empty) {
                    inferred_step(&f, 0);
                    CHECK(!wl_evaluation_control_reset(f.control));
                }
                int64_t row[] = {2, 7, 11};
                CHECK(!wl_session_insert(f.controlled, "input", row, 1,
                    arity) && !wl_session_insert(f.raw, "input", row, 1,
                    arity));
                CHECK(f.input->ncols == arity && f.input->nrows == 1 &&
                    f.input->schema_ok && !f.input->declared_ncols);
                unsigned sources = f.source_calls;
                /* Actual raw MAP oracle includes the nonempty nullary missing-lane
                 * default, separately from public/effective output schema. */
                eval_stack_t stack = {0};
                CHECK(!eval_stack_push(&stack,
                    session_find_rel(COL_SESSION(f.raw), "input"), false));
                CHECK(!col_op_map(&f.plan->strata[0].relations[0].ops[1],
                    &stack, COL_SESSION(f.raw)) && stack.top == 1);
                f.empty_known = stack.items[0].rel->column_types != NULL;
                CHECK(!eval_stack_drain(&stack));
                inferred_step(&f, 0);
                CHECK(f.source_calls == sources && f.input->ncols == arity &&
                    !f.input->declared_ncols);
                col_rel_t *out = session_find_rel(COL_SESSION(f.controlled),
                        "out");
                CHECK(out && out->nrows == 1 &&
                    !strcmp(wl_intern_reverse(f.plan->intern,
                    out->columns[0][0]), arity?"2":"0"));
                col_relation_snapshot_t image =
                    wl_columnar_relation_snapshot(f.input);
                int64_t wrong[] = {3, 4, 5, 6};
                uint32_t wrong_width = arity?arity+1:1;
                CHECK(wl_session_insert(f.raw, "input", wrong, 1,
                    wrong_width) == EINVAL && wl_session_insert(f.controlled,
                    "input", wrong, 1, wrong_width) == EINVAL);
                CHECK(wl_columnar_relation_snapshot_equal(image,
                    wl_columnar_relation_snapshot(f.input)));
                CHECK(!wl_session_remove(f.controlled, "input", row, 1,
                    arity) && !wl_session_remove(f.raw, "input", row, 1,
                    arity));
                CHECK(!wl_evaluation_control_reset(f.control));
                inferred_step(&f, 0);
                CHECK(!session_find_rel(COL_SESSION(f.controlled),
                    "out")->nrows);
                CHECK(!wl_session_insert(f.controlled, "input", row, 1,
                    arity) && !wl_session_insert(f.raw, "input", row, 1,
                    arity));
                CHECK(!wl_evaluation_control_reset(f.control));
                inferred_step(&f, 0);
                CHECK(f.input->ncols == arity && !f.input->declared_ncols);
                destroy(&f);
            }
}

/* Exhaust a real attempt immediately before each SCHEMA action. Padding is
 * adversarial work, never an oracle for the price of a complete host. */
static unsigned schema_target, schema_seen, schema_after;
static uint64_t schema_remaining, schema_padded, schema_id;
static bool schema_injected;
static void schema_boundary(wl_evaluation_control_t *c, unsigned phase,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings) {
    (void)progress; (void)bindings;
    if (!active || c != active->control)return;
    if (schema_injected) {
        ++schema_after; return;
    }
    if (phase != 7 || schema_seen++ != schema_target)return;
    uint64_t used = 0;
    schema_id = wl_evaluation_control_active_attempt_id(c);
    CHECK(schema_id && !wl_evaluation_control_test_active_consumed(c,
        schema_id, &used) && used <= UINT64_MAX-schema_remaining);
    uint64_t padding = UINT64_MAX-schema_remaining-used;
    CHECK(!wl_evaluation_control_charge_attempt(c, schema_id, padding));
    schema_padded = used+padding;
    CHECK(wl_evaluation_control_active_attempt_id(c) == schema_id);
    schema_injected = true;
}
static void schema_source_boundary(wl_evaluation_control_t *c, unsigned phase) {
    if (schema_injected && active && c == active->control)++schema_after;
    session_source(c, phase);
}
static void inferred_schema_boundaries(void) {
    for (unsigned inferred = 0; inferred < 2; ++inferred)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (schema_target = 0; schema_target < (inferred?2u:3u);
                ++schema_target) for (schema_remaining = 255;
                    schema_remaining <= 256; ++schema_remaining) {
                    fixture_t f = inferred_create(inferred?0u:1u);
                    bind_inferred(&f, observed != 0);
                    /* The explicit unobserved fixture must use the qualified general
                     * first-missing route, not the legacy populated C1 route. */
                    col_relation_snapshot_t input =
                        wl_columnar_relation_snapshot(f.input);
                    col_rel_t *old = session_find_rel(COL_SESSION(f.controlled),
                            "out");
                    col_relation_snapshot_t model =
                        wl_columnar_relation_snapshot(old);
                    schema_seen = schema_after = 0; schema_injected = false;
                    CHECK(!wl_evaluation_control_configure(f.control,
                        UINT64_MAX));
                    wl_columnar_session_test_observed_transaction_checkpoint =
                        schema_boundary;
                    wl_columnar_relation_test_empty_input_checkpoint =
                        schema_source_boundary;
                    CHECK(complete(f.controlled) ==
                        WL_EVALUATION_CONTROL_WORK_LIMIT &&
                        schema_injected && schema_seen == schema_target+1 &&
                        !f.events);
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report) &&
                        report.result == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                        report.attempt_id == schema_id &&
                        report.consumed >= schema_padded &&
                        report.consumed-schema_padded ==
                        (schema_remaining == 256?256u:0u));
                    CHECK(
                        session_find_rel(COL_SESSION(f.controlled),
                        "out") == old &&
                        (!old || wl_columnar_relation_snapshot_equal(model,
                        wl_columnar_relation_snapshot(old))) &&
                        wl_columnar_relation_snapshot_equal(input,
                        wl_columnar_relation_snapshot(f.input)));
                    wl_columnar_session_test_observed_transaction_checkpoint =
                        NULL;
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control, 0));
                    wl_columnar_relation_test_empty_input_checkpoint =
                        session_source;
                    inferred_step(&f, inferred?0u:1u);
                    destroy(&f);
                }
}

static void schema_stop_boundary(wl_evaluation_control_t *c, unsigned phase,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings) {
    (void)progress; (void)bindings;
    if (!active || c != active->control || phase != 7)return;
    if (schema_seen++ == schema_target)session_source(c, 1007);
}
static void inferred_schema_stops(void) {
    for (unsigned inferred = 0; inferred < 2; ++inferred)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (schema_target = 0; schema_target < (inferred?2u:3u);
                ++schema_target) for (action = 0; action < 5; ++action) {
                    fixture_t f = inferred_create(inferred?0u:1u);
                    bind_inferred(&f, observed != 0);
                    col_relation_snapshot_t input =
                        wl_columnar_relation_snapshot(f.input);
                    col_rel_t *old = session_find_rel(COL_SESSION(f.controlled),
                            "out");
                    col_relation_snapshot_t model =
                        wl_columnar_relation_snapshot(old);
                    schema_seen = 0; f.stop = 1007;
                    CHECK(!wl_evaluation_control_configure(f.control,
                        action == 1?UINT64_MAX:0));
                    wl_columnar_session_test_observed_transaction_checkpoint =
                        schema_stop_boundary;
                    int expected = action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                        action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:action ==
                        4?EOVERFLOW:EINVAL;
                    CHECK(complete(f.controlled) == expected && f.did_stop &&
                        !f.events &&
                        session_find_rel(COL_SESSION(f.controlled),
                        "out") == old &&
                        (!old || wl_columnar_relation_snapshot_equal(model,
                        wl_columnar_relation_snapshot(old))) &&
                        wl_columnar_relation_snapshot_equal(input,
                        wl_columnar_relation_snapshot(f.input)));
                    unobserved_stop_report(&f, expected);
                    wl_columnar_session_test_observed_transaction_checkpoint =
                        NULL;
                    f.stop = 0;
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control, 0));
                    inferred_step(&f, inferred?0u:1u); destroy(&f);
                }
}

int main(void) {
    inferred_schema_stops(); inferred_schema_boundaries();
    inferred_six_domain_histories(); inferred_first_arity();
    state_only_backend_pair(); late_end_matrix();
    late_rollback_refusals = 3; late_end_matrix(); late_rollback_refusals = 0;
    late_two_head_matrix();
    unobserved_graph_fee_oracle();
    unobserved_missing_matrix(); unobserved_unknown_variable();
    unobserved_selector_controls();
    unobserved_body_final_stops(); unobserved_long_names();
    anti_bare_graph_errors();
    anti_graph_control_priority();
    primitive_prices(); timestamp_constructor_prices();
    timestamp_constructor_resources(); primitive_stops(); primitive_resources();
    future_graph_output_guards();
    session_matrix(); session_stops(); old_public_image_stops();
    session_resources();
    puts("empty expression sources: all checks passed"); return 0;
}
