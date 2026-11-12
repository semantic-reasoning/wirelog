/* #2299 actual empty physical inputs, real source leases and native hosts. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
extern void (*wl_columnar_filter_test_map_schema)(const col_rel_t *, bool);
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
    atomic_store(&budget->usable_bytes, 0);
    wl_evaluation_control_t *c = begin(0); col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_new_empty_input_operator_attempt_controlled(
            "input", r, NULL, g, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOSPC && !out);
    gates_free(r); CHECK(!reserved(g)); finish(c, ENOSPC);
    atomic_store(&budget->usable_bytes, UINT64_C(128)<<20);
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
    bool background; unsigned base_events; int base_balance[4]; int64_t balance;
    unsigned stop; bool did_stop; uint64_t next;
    unsigned fail_constructor; bool deny_constructor;
    wl_evaluation_control_report_t original_report;
} fixture_t;
static fixture_t *active;
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
    bool expected = active->input->ncols || active->constant || active->numeric;
    CHECK(known == expected && r && r->ncols == 1);
    if (!known) {
        CHECK(!r->column_types); ++active->unknown;
    }
}
static void expression(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    fixture_t *f = active;
    if (!f || c != f->control || (phase != 610 && phase != 83)) return;
    wl_col_session_t *session = COL_SESSION(f->controlled);
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
        wl_columnar_filter_context_t *map = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context_owned(ctx->frame, &map,
            &ctx->child));
        CHECK(map->source.width == f->input->ncols &&
            map->source.declared_ncols == f->width &&
            map->source.has_graph_column == f->input->has_graph_column &&
            map->source.graph_col_idx == f->input->graph_col_idx);
        if (!f->input->ncols)CHECK(map->source.descriptor != f->input &&
                map->source.descriptor->schema_ok && !map->source.nrows);
        ++f->metadata;
    }else ++f->bodies;
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
        atomic_store(&g->usable_bytes, wl_columnar_memory_reserved(g));
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
static void bind(fixture_t *f) {
    active = f; wl_session_set_delta_cb(f->controlled, events, f);
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
                false); bind(&f);
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
                    filter != 0); bind(&f);
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
    for (size_t k = 0; k < sizeof(phases)/sizeof(*phases); ++k)for (action = 0;
            action < 5; ++action) {
            fixture_t f = create("int64", "string", "to_string(x)", false,
                    false); bind(&f);
            col_relation_snapshot_t before =
                wl_columnar_relation_snapshot(f.input);
            uint64_t baseline = reserved(f.g); f.stop = phases[k];
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
            CHECK(!wl_columnar_session_test_pending_source_bytes(COL_SESSION(
                    f.controlled), &retained));
            CHECK(wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(f.input)) &&
                !f.input->ncols && !f.input->schema_ok &&
                f.input->declared_ncols == 1);
            CHECK(reserved(f.g) == baseline+retained);
            CHECK((retained != 0) == (action == 1));
            closed(&f); wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report));
            if (action == 2 || action == 3) {
                CHECK(reports_equal(&report, &f.original_report));
                if (action == 3) {
                    uint64_t used = 99;
                    CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                        f.next, &used) && !used);
                    wl_evaluation_control_attempt_finish_result_t end =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.controlled->operation_admission, f.next, 0,
                        0);
                    CHECK(!end.protocol && end.report_valid &&
                        !end.report.consumed);
                }
            }else CHECK(report.result == expected && !report.cleanup_status);

            if (action == 1)for (unsigned repeat = 0; repeat < 2; ++repeat) {
                    f.did_stop = false;
                    CHECK(!wl_evaluation_control_reset(f.control) &&
                        !wl_evaluation_control_configure(f.control,
                        UINT64_MAX));
                    CHECK(complete(f.controlled) ==
                        WL_EVALUATION_CONTROL_WORK_LIMIT && f.did_stop &&
                        !f.events);
                    closed(&f); uint64_t again = 0;
                    CHECK(!wl_columnar_session_test_pending_source_bytes(
                            COL_SESSION(f.controlled), &again) &&
                        again == retained && reserved(f.g) == baseline+again &&
                        wl_columnar_relation_snapshot_equal(before,
                        wl_columnar_relation_snapshot(f.input)));
                }
            f.stop = 0;
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control, 0));
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(f.unknown && !f.events);
            insert(&f, 2, false);
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
            CHECK(f.balance == 1);
            destroy(&f);
        }
}

static void old_public_image_stops(void) {
    const char *text = ".decl input(x:int64)\n.decl other(x:int64)\n"
        ".decl out(y:string)\n.decl base(y:int64)\n"
        "out(to_string(x)):-input(x).\nbase(x):-other(x).\n";
    for (action = 0; action < 5; ++action) {
        fixture_t f = {0}; f.width = 1; f.background = true;
        create_sessions(&f, text); bind(&f);
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
    for (unsigned allocation = 1; allocation <= 4; ++allocation) {
        fixture_t f = create("int64", "string", "to_string(x)", false, false);
        bind(&f);
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(f.input);
        uint64_t baseline = reserved(f.g);
        for (unsigned repeat = 0; repeat < 2; ++repeat) {
            f.fail_constructor = allocation < 4?allocation:0;
            f.deny_constructor = allocation == 4;
            int expected = allocation == 4?ENOSPC:ENOMEM;
            CHECK(complete(f.controlled) == expected && !f.fail_constructor &&
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
                report.execution_status == expected && !report.cleanup_status);
            atomic_store(&wl_columnar_memory_governor_ref_get(
                    f.g)->usable_bytes, UINT64_C(128)<<20);
            CHECK(!wl_evaluation_control_reset(f.control));
        }
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(f.unknown && !f.events);
        insert(&f, 2, false); CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(f.controlled) && !complete(f.raw)); equal(&f);
        CHECK(f.balance == 1);
        destroy(&f);
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
    atomic_store(&budget->usable_bytes, 0);
    wl_evaluation_control_t *c = begin(0); col_rel_t *out = NULL;
    CHECK(
        wl_columnar_relation_new_empty_timestamp_input_operator_attempt_controlled
        (
            "$consol", input, NULL, g, &out, c,
            wl_evaluation_control_active_attempt_id(c)) == ENOSPC && !out &&
        !reserved(g));
    gates_free(input); finish(c, ENOSPC);
    atomic_store(&budget->usable_bytes, UINT64_C(128)<<20);
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
int main(void) {
    anti_bare_graph_errors();
    anti_graph_control_priority();
    primitive_prices(); timestamp_constructor_prices();
    timestamp_constructor_resources(); primitive_stops(); primitive_resources();
    future_graph_output_guards();
    session_matrix(); session_stops(); old_public_image_stops();
    session_resources();
    puts("empty expression sources: all checks passed"); return 0;
}
