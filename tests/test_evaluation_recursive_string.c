/* Real retained recursive STEP integration for declared unary STRING heads. */
#define _POSIX_C_SOURCE 200809L
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include "controlled_step_observation.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

extern int wl_columnar_session_test_retirement_view(const wl_col_session_t *,
    uint64_t[16]);

static const char *const sources[] = {
    ".decl input(x:string)\n.decl out(x:string)\n"
    "input(\"z\").\ninput(\"a\").\ninput(\"z\").\n"
    "out(x):-input(x).\nout(x):-out(x).\n",
    ".decl input(x:string)\n.decl b(x:string)\n.decl a(x:string)\n"
    "input(\"z\").\ninput(\"a\").\ninput(\"z\").\n"
    "a(x):-input(x).\na(x):-b(x).\nb(x):-a(x).\n"
};

typedef struct {
    char name[32];
    int64_t value;
    int diff;
} event_t;
typedef struct { event_t events[128]; unsigned count; } events_t;
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *session, *reference;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    events_t actual, expected;
    unsigned heads, round;
    bool observed;
} fixture_t;

static int
environment(const char *name, const char *value)
{
#ifdef _WIN32
    return _putenv_s(name, value);
#else
    return setenv(name, value, 1);
#endif
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int diff,
    void *opaque)
{
    events_t *events = opaque;
    CHECK(name && row && width == 1 && (diff == 1 || diff == -1)
        && events->count < 128 &&
        strlen(name) < sizeof(events->events[0].name));
    event_t *event = &events->events[events->count++];
    memset(event, 0, sizeof(*event));
    strcpy(event->name, name); event->value = row[0]; event->diff = diff;
}

static int
event_order(const void *left, const void *right)
{
    const event_t *a = left, *b = right;
    int cmp = strcmp(a->name, b->name);
    if (cmp) return cmp;
    if (a->value != b->value) return a->value < b->value ? -1 : 1;
    return (a->diff > b->diff) - (a->diff < b->diff);
}

static const char *
head_name(const fixture_t *f, unsigned head)
{
    return f->heads == 1 ? "out" : head ? "b" : "a";
}

static void
fixture_init_text(fixture_t *f, const char *text, unsigned heads, bool observed,
    unsigned workers)
{
    memset(f, 0, sizeof(*f)); f->heads = heads; f->observed = observed;
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan,
        workers, &options, &f->session));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->reference));
    CHECK(!wl_session_load_facts(f->session, f->program)
        && !wl_session_load_facts(f->reference, f->program));
    if (observed) {
        wl_session_set_delta_cb(f->session, capture, &f->actual);
        wl_session_set_delta_cb(f->reference, capture, &f->expected);
    }
}

static void
fixture_init(fixture_t *f, unsigned heads, bool observed)
{
    fixture_init_text(f, sources[heads - 1], heads, observed, 1);
    CHECK(f->plan->stratum_count == 1 && f->plan->strata[0].is_recursive
        && f->plan->strata[0].relation_count == heads
        && !f->session->source_c1_admission);
    for (unsigned h = 0; h < heads; h++) {
        const wl_plan_body_t *body = &f->plan->strata[0].bodies[h];
        CHECK(body->declared_width == 1 && !body->declared_int64
            && body->lineage == WL_PLAN_BODY_LINEAGE_LOWERED
            && (body->op_count == 3 || body->op_count == 6));
        for (uint32_t op = 0; op < body->op_count; op++)
            CHECK(body->ops[op].op != WL_PLAN_OP_K_FUSION
                && body->ops[op].op != WL_PLAN_OP_JOIN);
    }
    CHECK(wl_intern_get(f->plan->intern, "z") == 0
        && wl_intern_get(f->plan->intern, "a") == 1);
}

static void
fixture_close(fixture_t *f)
{
    for (unsigned turn = 0; f->session; turn++) {
        CHECK(turn < 100000);
        int rc = wl_session_try_destroy(&f->session);
        CHECK(!rc || rc == EBUSY);
    }
    wl_session_destroy(f->reference);
    wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f->governor))
        && wl_columnar_memory_governor_ref_is_sole(f->governor));
    wl_columnar_memory_governor_ref_release(f->governor);
}

static int
step(fixture_t *f)
{
    for (unsigned turn = 0; turn < 1000000; turn++) {
        int rc = wl_session_step(f->session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

static void
check_rows(fixture_t *f, const int64_t *values, uint32_t count)
{
    for (unsigned h = 0; h < f->heads; h++) {
        col_rel_t *a = session_find_rel((wl_col_session_t *)f->session,
                head_name(f, h));
        col_rel_t *b = session_find_rel((wl_col_session_t *)f->reference,
                head_name(f, h));
        CHECK(a && b && a->ncols == 1 && b->ncols == 1
            && a->nrows == count && b->nrows == count && a->schema_ok &&
            b->schema_ok
            && a->column_types && b->column_types
            && a->column_types[0] == WIRELOG_TYPE_STRING
            && b->column_types[0] == WIRELOG_TYPE_STRING
            && !a->timestamps && !a->has_graph_column
            && a->compound_kind == WIRELOG_COMPOUND_KIND_NONE
            && a->sorted_nrows == count && a->storage_owner == a);
        CHECK(a->declared_ncols == b->declared_ncols
            && a->schema.n_children == b->schema.n_children
            && !strcmp(a->schema.children[0]->format,
            b->schema.children[0]->format)
            && !strcmp(a->schema.children[0]->name,
            b->schema.children[0]->name));
        for (uint32_t r = 0; r < count; r++)
            CHECK(a->columns[0][r] == values[r] &&
                b->columns[0][r] == values[r]);
    }
    CHECK(f->actual.count == f->expected.count);
    qsort(f->actual.events, f->actual.count, sizeof(event_t), event_order);
    qsort(f->expected.events, f->expected.count, sizeof(event_t), event_order);
    CHECK(!memcmp(f->actual.events, f->expected.events,
        f->actual.count * sizeof(event_t)));
}

static void
check_events(fixture_t *f, int64_t value, int diff)
{
    CHECK(f->actual.count == (f->observed ? f->heads : 0));
    for (unsigned h = 0; f->observed && h < f->heads; h++) {
        CHECK(!strcmp(f->actual.events[h].name, head_name(f, h))
            && f->actual.events[h].value == value &&
            f->actual.events[h].diff == diff);
    }
}

static void
run_rows(void)
{
    for (unsigned heads = 1; heads <= 2; heads++)
        for (unsigned observed = 0; observed < 2; observed++) {
            fixture_t f; fixture_init(&f, heads, observed != 0);
            const int64_t initial[] = {0, 1}, all[] = {0, 1, 2},
                remaining[] = {1, 2};
            const int64_t facts[] = {0, 1, 0};
            CHECK(!wl_session_remove(f.session, "input", facts, 3, 1)
                && !wl_session_remove(f.reference, "input", facts, 3, 1));
            CHECK(!step(&f) && !wl_session_step(f.reference));
            check_rows(&f, NULL, 0); CHECK(!f.actual.count);
            CHECK(!wl_session_insert(f.session, "input", facts, 3, 1)
                && !wl_session_insert(f.reference, "input", facts, 3, 1));
            CHECK(!step(&f) && !wl_session_step(f.reference));
            check_rows(&f, initial, 2);
            CHECK(f.actual.count == (observed ? 2 * heads : 0));
            unsigned delivered = f.actual.count;
            CHECK(!step(&f) && f.actual.count == delivered);
            f.actual.count = f.expected.count = 0;
            int64_t fresh = wl_intern_put(f.plan->intern, "fresh");
            CHECK(fresh == 2);
            CHECK(!wl_session_insert(f.session, "input", &fresh, 1, 1)
                && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
            CHECK(!step(&f) && !wl_session_step(f.reference));
            check_rows(&f, all, 3); check_events(&f, fresh, 1);
            f.actual.count = f.expected.count = 0;
            const int64_t remove[] = {0, 0};
            CHECK(!wl_session_remove(f.session, "input", remove, 2, 1)
                && !wl_session_remove(f.reference, "input", remove, 2, 1));
            CHECK(!step(&f) && !wl_session_step(f.reference));
            check_rows(&f, remaining, 2); check_events(&f, 0, -1);
            f.actual.count = f.expected.count = 0;
            CHECK(!wl_session_remove(f.session, "input", remaining, 2, 1)
                && !wl_session_remove(f.reference, "input", remaining, 2, 1));
            CHECK(!step(&f) && !wl_session_step(f.reference));
            check_rows(&f, NULL, 0);
            CHECK(f.actual.count == (observed ? 2 * heads : 0));
            f.actual.count = f.expected.count = 0;
            CHECK(!step(&f) && !wl_session_step(f.reference));
            check_rows(&f, NULL, 0);
            fixture_close(&f);
        }
    puts(
        "STRING projection rows, typed empty heads, mutations and signed events passed");
}

typedef struct {
    fixture_t *fixture;
    unsigned kind, outcome, target_round;
    bool hit;
    uint64_t successor;
    wl_evaluation_control_report_t frozen;
} stop_t;
static stop_t *stopping;

static void
inject(stop_t *s)
{
    CHECK(!s->hit); s->hit = true;
    fixture_t *f = s->fixture;
    if (s->outcome == 0) wl_evaluation_control_request_cancel(f->control);
    else if (s->outcome == 1) {
        CHECK(wl_evaluation_control_charge(f->control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else {
        uint64_t id = wl_evaluation_control_active_attempt_id(f->control);
        CHECK(id);
        wl_evaluation_control_attempt_finish_result_t ended =
            wl_evaluation_control_finish_attempt(
            f->control, f->session->operation_admission, id, 0, 0);
        CHECK(!ended.protocol && ended.report_valid && !ended.report.result);
        s->frozen = ended.report;
        if (s->outcome == 3) {
            CHECK(!wl_evaluation_control_begin(f->control,
                f->session->operation_admission));
            s->successor = wl_evaluation_control_active_attempt_id(f->control);
            CHECK(s->successor && s->successor != id);
        }
    }
}

static void
iteration(wl_evaluation_control_t *control, uint32_t round, bool changed)
{
    (void)changed;
    if (stopping &&
        control == stopping->fixture->control) stopping->fixture->round = round;
}

static void
transaction(wl_evaluation_control_t *control, unsigned phase, uint32_t progress,
    const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)progress; (void)bindings;
    stop_t *s = stopping;
    if (s && !s->hit && s->kind == 0 && phase == 19
        && control == s->fixture->control &&
        s->fixture->round == s->target_round)
        inject(s);
}

static void
source_end(const wl_columnar_relation_scalar_source_t *source,
    wl_evaluation_control_t *funding, uint64_t id, unsigned point)
{
    (void)funding; (void)id;
    stop_t *s = stopping;
    if (!s || s->hit || s->kind != 1 || point != 1
        || s->fixture->round != s->target_round
        || source->phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) return;
    wl_col_session_t *col = (wl_col_session_t *)s->fixture->session;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    if (!col->cleanup_active ||
        wl_columnar_eval_stack_framed_context(col->cleanup_active, col, &ctx)
        || !ctx->capture_only || !ctx->private_bindings || !ctx->child_bound
        || source != &ctx->child.source) return;
    inject(s);
}

static void
child_finished(const wl_col_session_t *col,
    wl_columnar_eval_serial_framed_host_action_t action,
    wl_evaluation_control_t *funding, int protocol)
{
    (void)funding; (void)protocol;
    stop_t *s = stopping;
    if (s && !s->hit && s->kind == 2
        && col == (wl_col_session_t *)s->fixture->session
        && action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK
        && s->fixture->round == s->target_round) inject(s);
}

static void
run_stops(unsigned heads, bool observed)
{
    for (unsigned kind = 0; kind < 3; kind++)
        for (unsigned round = 0; round < 2; round++)
            for (unsigned outcome = 0; outcome < 4; outcome++) {
                fixture_t f; fixture_init(&f, heads, observed);
                CHECK(!step(&f) && !wl_session_step(f.reference));
                f.actual.count = f.expected.count = 0;
                int64_t fresh = wl_intern_put(f.plan->intern, "fresh");
                CHECK(fresh == 2);
                CHECK(!wl_session_insert(f.session, "input", &fresh, 1, 1)
                    && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
                col_rel_t *old[2]; col_relation_snapshot_t snapshots[2];
                for (unsigned h = 0; h < heads; h++) {
                    old[h] = session_find_rel((wl_col_session_t *)f.session,
                            head_name(&f, h));
                    snapshots[h] = wl_columnar_relation_snapshot(old[h]);
                }
                CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_C(1000000000)));
                stop_t stop = {.fixture = &f, .kind = kind, .outcome = outcome,
                               .target_round = round};
                stopping = &stop; f.round = 0;
                wl_columnar_session_test_iteration_checkpoint = iteration;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    transaction;
                wl_columnar_relation_test_scalar_end_checkpoint = source_end;
                wl_columnar_session_test_framed_grant_finished = child_finished;
                int rc = EAGAIN;
                for (unsigned turn = 0; rc == EAGAIN && turn < 1000000;
                    turn++){
                    rc = wl_session_step(f.session);
                    CHECK(!f.actual.count);
                    for (unsigned h = 0; h < heads; h++) {
                        CHECK(session_find_rel((wl_col_session_t *)f.session,
                            head_name(&f, h)) == old[h]
                            && wl_columnar_relation_snapshot_equal(snapshots[h],
                            wl_columnar_relation_snapshot(old[h]))
                            && old[h]->nrows == 2 &&
                            old[h]->columns[0][0] == 0 &&
                            old[h]->columns[0][1] == 1);
                    }
                    if (stop.successor) {
                        uint64_t used = UINT64_MAX;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.control, stop.successor, &used) && !used);
                    }
                }
                wl_columnar_session_test_iteration_checkpoint = NULL;
                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                wl_columnar_relation_test_scalar_end_checkpoint = NULL;
                wl_columnar_session_test_framed_grant_finished = NULL;
                stopping = NULL;
                int expected = outcome == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                    : outcome == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL;
                if (!stop.hit || rc != expected)
                    fprintf(stderr,
                        "stop heads%u observed%d kind%u round%u outcome%u hit%d rc%d expected%d\n",
                        heads, observed, kind, round, outcome, stop.hit, rc,
                        expected);
                CHECK(stop.hit && rc == expected);
                wl_col_session_t *col = (wl_col_session_t *)f.session;
                CHECK(!col->cleanup_active && !col->cleanup_pending &&
                    !col->cleanup_reserved_bytes);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report));
                if (outcome >=
                    2) CHECK(wl_test_completed_step_report_equal(&report,
                        &stop.frozen));
                else CHECK(report.result == expected);
                if (stop.successor) {
                    wl_evaluation_control_attempt_finish_result_t ended =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.session->operation_admission,
                        stop.successor, 0, 0);
                    CHECK(!ended.protocol && ended.report_valid &&
                        !ended.report.consumed);
                }
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control, 0));
                CHECK(!step(&f) && !wl_session_step(f.reference));
                const int64_t all[] = {0, 1, 2}; check_rows(&f, all, 3);
                check_events(&f, 2, 1);
                fixture_close(&f);
            }
    printf(
        "STRING first/later-round stop/END/retry histories heads%u observer%d passed\n",
        heads, observed);
}

static unsigned refused_children;
static void
refusal_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)control; (void)index;
    if (phase == 8 && bindings) refused_children++;
}

static void
snapshot_count(const char *name, const int64_t *row, uint32_t width,
    void *opaque)
{
    (void)name; (void)row; (void)width;
    (*(unsigned *)opaque)++;
}

static void
assert_refused(fixture_t *f, bool snapshot)
{
    refused_children = 0;
    wl_columnar_session_test_observed_transaction_checkpoint =
        refusal_checkpoint;
    int rc = EAGAIN; unsigned callbacks = 0;
    for (unsigned turn = 0; rc == EAGAIN && turn < 1000000; turn++)
        rc = snapshot ? wl_session_snapshot(f->session, snapshot_count,
                &callbacks)
            : wl_session_step(f->session);
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    if (rc != ENOTSUP) fprintf(stderr, "refusal rc%d snapshot%d\n", rc,
            snapshot);
    CHECK(rc == ENOTSUP && !refused_children && !callbacks && !f->actual.count);
}

static void
run_refusals(void)
{
    static const char *const rejected[] = {
        ".decl out(x:string)\ninput(\"z\").\ninput(\"a\").\n"
        "out(x):-input(x).\nout(x):-out(x).\n",
        ".decl input(x:float)\n.decl out(x:float)\ninput(1.5).\n"
        "out(x):-input(x).\nout(x):-out(x).\n",
        ".decl input(x:string,y:string)\n.decl out(x:string,y:string)\n"
        "input(\"z\",\"a\").\nout(x,y):-input(x,y).\nout(x,y):-out(x,y).\n",
        ".decl input(x:string,y:int64)\n.decl out(x:string,y:int64)\n"
        "input(\"z\",1).\nout(x,y):-input(x,y).\nout(x,y):-out(x,y).\n",
        ".decl input(x:string)\ninput(\"z\").\nout(x):-input(x).\nout(x):-out(x).\n",
        ".decl input(x:string)\n.decl out(x:string)\ninput(\"z\").\n"
        "out(trim(x)):-input(x).\nout(x):-out(x).\n",
        ".decl input(x:string,y:string)\n.decl out(x:string)\ninput(\"z\",\"a\").\n"
        "out(x):-input(x,y).\nout(y):-out(x),input(x,y).\n",
        ".decl input(x:string)\n.decl out(x:string)\ninput(\"z\").\nout(\"seed\").\n"
        "out(x):-input(x).\nout(x):-out(x).\n"
    };
    for (unsigned i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
        fixture_t f; fixture_init_text(&f, rejected[i], 1, false, 1);
        assert_refused(&f, false); fixture_close(&f);
    }
    for (unsigned mode = 0; mode < 10; mode++) {
        fixture_t f;
        if (mode == 0) CHECK(!environment("WIRELOG_JOIN_BATCH_BYTES", "1024"));
        fixture_init(&f, 1, true);
        CHECK(!environment("WIRELOG_JOIN_BATCH_BYTES", ""));
        col_rel_t *input = session_find_rel((wl_col_session_t *)f.session,
                "input");
        CHECK(input);
        col_rel_t *alias = NULL;
        wl_plan_idb_schema_t changed = f.plan->idb_schemas[0];
        const wl_plan_idb_schema_t *original_schemas = f.plan->idb_schemas;
        const char *original_variable = NULL;
        wl_plan_body_t *body = (wl_plan_body_t *)&f.plan->strata[0].bodies[0];
        wl_plan_body_lineage_t original_lineage = body->lineage;
        const uint32_t *original_projection = NULL;
        if (mode == 2 || mode == 3) {
            int64_t value = 0;
            CHECK(!wl_session_insert(f.session, "out", &value, 1, 1));
            if (mode == 3) CHECK(!wl_session_remove(f.session, "out", &value, 1,
                    1));
        } else if (mode == 4) {
            wirelog_column_type_t type = WIRELOG_TYPE_INT64;
            CHECK(!col_rel_set_column_types(input, &type, 1));
        } else if (mode == 5) {
            changed.name = "unmatched_head";
            f.plan->idb_schemas = &changed;
        } else if (mode == 6) {
            alias = col_rel_new_auto("actual_alias", 1); CHECK(alias);
            CHECK(!col_rel_install_shared_view(alias, input)
                && col_rel_storage_alias_borrow_count(input) == 1);
        } else if (mode == 7) {
            wl_plan_op_t *op =
                (wl_plan_op_t *)f.plan->strata[0].relations[0].ops;
            original_variable = op[0].relation_name;
            op[0].relation_name = "not_input";
        }
        if (mode == 8) body->lineage = WL_PLAN_BODY_LINEAGE_NONE;
        if (mode == 9) {
            wl_plan_op_t *op = (wl_plan_op_t *)body->ops;
            original_projection = op[1].project_indices;
            op[1].project_indices = NULL;
        }
        col_rel_t *head = session_find_rel((wl_col_session_t *)f.session,
                "out");
        col_relation_snapshot_t input_snapshot =
            wl_columnar_relation_snapshot(input);
        col_relation_snapshot_t head_snapshot =
            wl_columnar_relation_snapshot(head);
        assert_refused(&f, mode == 1);
        CHECK(wl_columnar_relation_snapshot_equal(input_snapshot,
            wl_columnar_relation_snapshot(input))
            && session_find_rel((wl_col_session_t *)f.session, "out") == head);
        if (head) CHECK(wl_columnar_relation_snapshot_equal(head_snapshot,
                wl_columnar_relation_snapshot(head)));
        if (alias) CHECK(!col_rel_destroy_checked(alias)
                && !col_rel_storage_alias_borrow_count(input));
        f.plan->idb_schemas = original_schemas;
        body->lineage = original_lineage;
        if (original_projection) ((wl_plan_op_t *)body->ops)[1].project_indices
                = original_projection;
        if (original_variable) ((wl_plan_op_t *)f.plan->strata[0].relations[0].
            ops)[0].relation_name = original_variable;
        fixture_close(&f);
    }
    fixture_t workers;
    fixture_init_text(&workers, sources[0], 1, false, 2);
    if (((wl_col_session_t *)workers.session)->num_workers >
        1) assert_refused(&workers, false);
    else {
        const int64_t values[] = {0, 1};
        CHECK(!step(&workers) && !wl_session_step(workers.reference));
        check_rows(&workers, values, 2);
    }
    fixture_close(&workers);
    puts(
        "STRING projection actual type/shape/seed/alias/name/Snapshot/batch/worker refusals passed");
}

static fixture_t *fault_fixture;
static unsigned fault_kind;
static bool fault_hit;
static void
allocation_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)index; (void)bindings;
    if (!fault_fixture || fault_hit || control != fault_fixture->control ||
        phase != 18) return;
    fault_hit = true;
    if (fault_kind ==
        0) wl_columnar_relation_test_constructor_fail_allocation = 1;
    else {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(fault_fixture->governor);
        atomic_store_explicit(&g->usable_bytes, wl_columnar_memory_reserved(g),
            memory_order_release);
    }
}

static void
run_failures(void)
{
    for (unsigned kind = 0; kind < 3; kind++) {
        fixture_t f; fixture_init(&f, 2, true);
        CHECK(!step(&f) && !wl_session_step(f.reference));
        f.actual.count = f.expected.count = 0;
        int64_t fresh = wl_intern_put(f.plan->intern, "fresh");
        CHECK(fresh == 2);
        CHECK(!wl_session_insert(f.session, "input", &fresh, 1, 1)
            && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
        col_rel_t *old[2]; col_relation_snapshot_t snapshots[2];
        for (unsigned h = 0; h < 2; h++) {
            old[h] = session_find_rel((wl_col_session_t *)f.session,
                    head_name(&f, h));
            snapshots[h] = wl_columnar_relation_snapshot(old[h]);
        }
        fault_fixture = &f; fault_kind = kind; fault_hit = false;
        if (kind <
            2) wl_columnar_session_test_observed_transaction_checkpoint =
                allocation_checkpoint;
        else wl_columnar_session_test_iteration_limit = 1;
        int rc = step(&f);
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
        wl_columnar_session_test_iteration_limit = 0; fault_fixture = NULL;
        int expected = kind == 0 ? ENOMEM : kind == 1 ? ENOSPC : EOVERFLOW;
        if (rc != expected) fprintf(stderr,
                "failure kind%u hit%d rc%d expected%d\n", kind, fault_hit, rc,
                expected);
        CHECK(rc == expected && (kind == 2 || fault_hit) && !f.actual.count
            && !wl_columnar_relation_test_constructor_fail_allocation);
        for (unsigned h = 0; h < 2; h++)
            CHECK(session_find_rel((wl_col_session_t *)f.session,
                head_name(&f, h)) == old[h]
                && wl_columnar_relation_snapshot_equal(snapshots[h],
                wl_columnar_relation_snapshot(old[h])));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control,
            &report) && report.result == expected);
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                f.governor)->usable_bytes,
            WL_COLUMNAR_MEMORY_MIN_BUDGET, memory_order_release);
        CHECK(!wl_evaluation_control_reset(f.control) && !step(&f)
            && !wl_session_step(f.reference));
        const int64_t all[] = {0, 1, 2}; check_rows(&f, all, 3);
        check_events(&f, 2, 1);
        fixture_close(&f);
    }
    puts(
        "STRING typed-bottom allocation, governor and round-limit failures preserve error/retry passed");
}

/* Finite ordered checkpoint census. Equal phase/index/work values remain
 * separate occurrences; this is not a tariff or a concurrency-schedule map. */
enum { LIFE_METADATA, LIFE_HANDOFF, LIFE_ENTER, LIFE_WORK, LIFE_EXIT };
enum { LIFE_TRACE_CAPACITY = 2048 };
typedef struct {
    unsigned kind, phase, index, head, commits;
    uint64_t value;
} life_key_t;
typedef struct {
    life_key_t keys[LIFE_TRACE_CAPACITY];
    unsigned count, metadata[3], handoffs[2], works[2], allocations[2];
} life_trace_t;
typedef struct {
    stop_t stop;
    const life_trace_t *expected;
    life_trace_t *record;
    unsigned position, target, metadata[3], handoffs[2], works[2];
    unsigned entered[2], exited[2], allocations[2];
    unsigned scope_head, commits, allocation_head, allocation_ordinal;
    uint32_t allocation_initial;
    uint64_t original;
    bool metadata_active, constructor_active, allocation_failure,
        allocation_seen;
} life_state_t;
static life_state_t *lifecycle;

static void
life_original(life_state_t *s, wl_evaluation_control_t *control)
{
    CHECK(control == s->stop.fixture->control);
    if (s->stop.hit) return;
    uint64_t active = wl_evaluation_control_active_attempt_id(control);
    CHECK(active);
    if (!s->original) s->original = active;
    CHECK(active == s->original);
}

static void
life_emit(life_state_t *s, life_key_t key)
{
    if (s->stop.hit || s->allocation_failure) return;
    unsigned at = s->position++;
    CHECK(at < LIFE_TRACE_CAPACITY);
    if (s->record) s->record->keys[at] = key;
    if (s->expected) {
        CHECK(at < s->expected->count);
        const life_key_t *want = &s->expected->keys[at];
        bool same = key.kind == want->kind && key.phase == want->phase
            && key.index == want->index && key.head == want->head
            && key.commits == want->commits && key.value == want->value;
        if (!same) fprintf(stderr,
                "lifecycle prefix differs at%u: kind%u/%u phase%u/%u index%u/%u head%u/%u value%llu/%llu commits%u/%u\n",
                at, key.kind, want->kind, key.phase, want->phase, key.index,
                want->index, key.head, want->head,
                (unsigned long long)key.value, (unsigned long long)want->value,
                key.commits, want->commits);
        CHECK(same);
    }
    if (at == s->target) inject(&s->stop);
}

static void
life_metadata(wl_evaluation_control_t *control, unsigned phase,
    uint32_t progress, const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)bindings;
    life_state_t *s = lifecycle;
    if (!s || s->stop.hit || control != s->stop.fixture->control) return;
    if (phase >= 128 && phase <= 130) {
        life_original(s, control);
        unsigned occurrence = s->metadata[phase - 128]++;
        s->metadata_active = true;
        life_emit(s, (life_key_t){.kind = LIFE_METADATA, .phase = phase,
                                  .index = progress, .head = UINT32_MAX,
                                  .value = occurrence});
    } else if (s->metadata_active) {
        life_original(s, control);
        CHECK(phase == 4 || phase == 8); /* Actual NAMES / EVALUATE. */
        s->handoffs[phase == 8]++;
        s->metadata_active = false;
        life_emit(s, (life_key_t){.kind = LIFE_HANDOFF, .phase = phase,
                                  .index = progress, .head = UINT32_MAX});
    }
}

static void
life_constructor_work(wl_evaluation_control_t *control, uint64_t work)
{
    life_state_t *s = lifecycle;
    CHECK(s && s->constructor_active);
    life_original(s, control);
    if (s->stop.hit) return;
    unsigned occurrence = s->works[s->scope_head]++;
    life_emit(s, (life_key_t){.kind = LIFE_WORK, .phase = 21,
                              .index = occurrence, .head = s->scope_head,
                              .value = work, .commits = s->commits});
}

static void
life_constructor_commit(wl_evaluation_control_t *control, unsigned token)
{
    life_state_t *s = lifecycle;
    CHECK(s && s->constructor_active && !s->stop.hit && token < 3);
    life_original(s, control);
    CHECK(!(s->commits & (1u << token)));
    s->commits |= 1u << token;
}

static void
life_constructor(const wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t attempt, uint32_t round,
    uint32_t head, bool entering, int result, const col_rel_t *output)
{
    life_state_t *s = lifecycle;
    CHECK(s && session == (wl_col_session_t *)s->stop.fixture->session
        && control == s->stop.fixture->control && round == 0
        && head < s->stop.fixture->heads);
    life_original(s, control);
    CHECK(attempt == s->original);
    if (entering) {
        CHECK(!s->constructor_active && !output && !result
            && !wl_columnar_relation_test_work_checkpoint
            && !wl_columnar_relation_test_constructor_committed
            && !wl_columnar_relation_test_constructor_fail_allocation);
        CHECK(++s->entered[head] == 1);
        s->constructor_active = true; s->scope_head = head; s->commits = 0;
        bool allocation_target = head == s->allocation_head;
        s->allocation_initial = allocation_target
            ? s->allocation_ordinal : UINT32_MAX;
        CHECK(s->allocation_initial);
        wl_columnar_relation_test_constructor_fail_allocation =
            s->allocation_initial;
        wl_columnar_relation_test_work_checkpoint = life_constructor_work;
        wl_columnar_relation_test_constructor_committed =
            life_constructor_commit;
        life_emit(s,
            (life_key_t){.kind = LIFE_ENTER, .phase = 21, .head = head});
        if (allocation_target) {
            CHECK(s->expected &&
                s->allocation_ordinal <= s->expected->allocations[head] + 1);
            s->allocation_failure = s->allocation_ordinal <=
                s->expected->allocations[head];
        }
    } else {
        CHECK(s->constructor_active && s->scope_head == head);
        CHECK(++s->exited[head] == 1);
        uint32_t remaining =
            wl_columnar_relation_test_constructor_fail_allocation;
        s->allocations[head] = s->allocation_initial - remaining;
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_relation_test_work_checkpoint = NULL;
        wl_columnar_relation_test_constructor_committed = NULL;
        s->constructor_active = false;
        if (!result) {
            CHECK(output && output->ncols == 1 && !output->nrows
                && output->capacity == 1 && output->schema_ok
                && output->column_types &&
                output->column_types[0] == WIRELOG_TYPE_STRING
                && output->storage_owner == output && !output->col_shared
                && !output->pool_owned && !output->arena_owned &&
                !output->timestamps
                && output->name && !strcmp(output->name,
                head_name(s->stop.fixture, head))
                && s->commits == 7);
        } else CHECK(!output);
        if (head == s->allocation_head) {
            s->allocation_seen = true;
            if (s->allocation_failure) {
                CHECK(result == ENOMEM && !remaining && !s->stop.hit);
                s->stop.hit = true; return;
            }
            CHECK(!result && remaining == 1);
        }
        if (!s->stop.hit) {
            CHECK(!result && s->allocations[head]);
            if (s->expected) CHECK(s->allocations[head] ==
                    s->expected->allocations[head]);
        }
        life_emit(s, (life_key_t){.kind = LIFE_EXIT, .phase = 21, .head = head,
                                  .commits = s->commits});
    }
}

static void
life_public(fixture_t *f, col_rel_t *const old[2],
    const col_relation_snapshot_t snapshots[2], uint64_t successor)
{
    CHECK(!f->actual.count);
    for (unsigned h = 0; h < f->heads; h++) {
        CHECK(session_find_rel((wl_col_session_t *)f->session,
            head_name(f, h)) == old[h]
            && wl_columnar_relation_snapshot_equal(snapshots[h],
            wl_columnar_relation_snapshot(old[h]))
            && old[h]->ncols == 1 && old[h]->column_types
            && old[h]->column_types[0] == WIRELOG_TYPE_STRING
            && old[h]->nrows == 2 && old[h]->columns[0][0] == 0
            && old[h]->columns[0][1] == 1);
    }
    if (successor) {
        uint64_t used = UINT64_MAX;
        CHECK(!wl_evaluation_control_test_active_consumed(f->control, successor,
            &used) && !used);
    }
}

static void
life_case(unsigned heads, bool observed, const life_trace_t *expected,
    life_trace_t *record, unsigned target, unsigned outcome,
    unsigned allocation_head, unsigned allocation_ordinal)
{
    fixture_t f; fixture_init(&f, heads, observed);
    CHECK(!step(&f) && !wl_session_step(f.reference));
    f.actual.count = f.expected.count = 0;
    int64_t fresh = wl_intern_put(f.plan->intern, "fresh"); CHECK(fresh == 2);
    CHECK(!wl_session_insert(f.session, "input", &fresh, 1, 1)
        && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
    col_rel_t *old[2]; col_relation_snapshot_t snapshots[2];
    for (unsigned h = 0; h < heads; h++) {
        old[h] = session_find_rel((wl_col_session_t *)f.session,
                head_name(&f, h));
        snapshots[h] = wl_columnar_relation_snapshot(old[h]);
    }
    CHECK(!wl_evaluation_control_configure(f.control, UINT64_C(1000000000)));
    life_state_t state = {.stop = {.fixture = &f, .outcome = outcome},
                          .expected = expected, .record = record,
                          .target = target,
                          .allocation_head = allocation_head,
                          .allocation_ordinal = allocation_ordinal};
    CHECK(!lifecycle &&
        !wl_columnar_session_test_observed_transaction_checkpoint
        && !wl_columnar_session_test_string_bottom_checkpoint);
    lifecycle = &state;
    wl_columnar_session_test_observed_transaction_checkpoint = life_metadata;
    wl_columnar_session_test_string_bottom_checkpoint = life_constructor;
    bool failed_allocation = allocation_head < heads && expected
        && allocation_ordinal <= expected->allocations[allocation_head];
    bool stopping_case = target != UINT32_MAX || failed_allocation;
    int rc = EAGAIN;
    for (unsigned turn = 0; rc == EAGAIN && turn < 1000000; turn++) {
        rc = wl_session_step(f.session);
        if (stopping_case) life_public(&f, old, snapshots,
                state.stop.successor);
    }
    CHECK(!state.constructor_active &&
        !wl_columnar_relation_test_work_checkpoint
        && !wl_columnar_relation_test_constructor_committed
        && !wl_columnar_relation_test_constructor_fail_allocation);
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    wl_columnar_session_test_string_bottom_checkpoint = NULL;
    lifecycle = NULL;
    int wanted = failed_allocation ? ENOMEM : target == UINT32_MAX ? 0
        : outcome == 0 ? WL_EVALUATION_CONTROL_CANCELLED
        : outcome == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL;
    if (rc != wanted) fprintf(stderr,
            "lifecycle heads%u observer%d target%u outcome%u allochead%u ordinal%u hit%d rc%d wanted%d\n",
            heads, observed, target, outcome, allocation_head,
            allocation_ordinal,
            state.stop.hit, rc, wanted);
    CHECK(rc == wanted);
    if (stopping_case) {
        CHECK(state.stop.hit);
        if (!failed_allocation) CHECK(state.position == target + 1);
        wl_col_session_t *col = (wl_col_session_t *)f.session;
        CHECK(!col->cleanup_active && !col->cleanup_pending &&
            !col->cleanup_reserved_bytes);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        if (!failed_allocation && outcome >= 2)
            CHECK(wl_test_completed_step_report_equal(&report,
                &state.stop.frozen));
        else CHECK(report.result == wanted);
        if (state.stop.successor) {
            wl_evaluation_control_attempt_finish_result_t ended =
                wl_evaluation_control_finish_attempt(f.control,
                    f.session->operation_admission, state.stop.successor, 0, 0);
            CHECK(!ended.protocol && ended.report_valid &&
                !ended.report.consumed);
        }
        CHECK(!wl_evaluation_control_reset(f.control)
            && !wl_evaluation_control_configure(f.control, 0) && !step(&f));
    } else {
        if (expected) CHECK(state.position == expected->count);
        if (record) {
            record->count = state.position;
            memcpy(record->metadata, state.metadata, sizeof(record->metadata));
            memcpy(record->handoffs, state.handoffs, sizeof(record->handoffs));
            memcpy(record->works, state.works, sizeof(record->works));
            memcpy(record->allocations, state.allocations,
                sizeof(record->allocations));
        }
        for (unsigned h = 0; h < heads;
            h++) CHECK(state.entered[h] == 1 && state.exited[h] == 1);
    }
    if (allocation_head < heads) CHECK(state.allocation_seen);
    CHECK(!wl_session_step(f.reference));
    const int64_t all[] = {0, 1, 2}; check_rows(&f, all, 3);
    check_events(&f, 2, 1);
    fixture_close(&f);
}

static life_trace_t *
life_census(unsigned heads, bool observed)
{
    life_trace_t *trace = calloc(1, sizeof(*trace)); CHECK(trace);
    life_case(heads, observed, NULL, trace, UINT32_MAX, 0, UINT32_MAX, 0);
    CHECK(trace->count && trace->metadata[0] && trace->metadata[1] &&
        trace->metadata[2]
        && trace->handoffs[0] == 1 && trace->handoffs[1] == 1);
    for (unsigned h = 0; h < heads; h++) {
        unsigned masks = 0, zero = 0;
        for (unsigned i = 0; i < trace->count; i++) {
            const life_key_t *key = &trace->keys[i];
            if (key->kind != LIFE_WORK || key->head != h) continue;
            CHECK(key->commits <= 7);
            masks |= 1u << key->commits; zero += key->value == 0;
        }
        CHECK(trace->works[h] && trace->allocations[h] && zero
            && (masks & 139u) == 139u); /* Actual prefixes0/1/3/7. */
    }
    return trace;
}

/* Focused sanitizer/library profile: real ENTER/EXIT and both handoffs,
 * first/last metadata phase, commit-prefix and zero-tail occurrences only. */
static bool
life_focused(const life_trace_t *trace, unsigned at)
{
    const life_key_t *key = &trace->keys[at];
    if (key->kind == LIFE_HANDOFF || key->kind == LIFE_ENTER ||
        key->kind == LIFE_EXIT)
        return true;
    for (unsigned group = 0;
        group < (key->kind == LIFE_WORK && !key->value ? 2u : 1u); group++) {
        bool before = false, after = false;
        for (unsigned i = 0; i < trace->count; i++) {
            const life_key_t *other = &trace->keys[i];
            bool same = key->kind == other->kind
                && (key->kind == LIFE_METADATA ? key->phase == other->phase
                    : key->head == other->head
                && (group ? !other->value : key->commits == other->commits));
            if (!same) continue;
            before |= i < at; after |= i > at;
        }
        if (!before || !after) return true;
    }
    return false;
}

static void
run_lifecycle(unsigned family, unsigned heads, bool observed,
    unsigned shard, unsigned shards)
{
    CHECK(shards && shard < shards);
    life_trace_t *trace = life_census(heads, observed);
    unsigned selected = 0, ordinal = 0;
    if (family < 2 || family == 4) {
        for (unsigned i = 0; i < trace->count; i++) {
            bool metadata = trace->keys[i].kind == LIFE_METADATA ||
                trace->keys[i].kind == LIFE_HANDOFF;
            if (family == 4 ? !life_focused(trace,
                i) : metadata != (family == 0)) continue;
            unsigned own = ordinal++ % shards;
            if (own != shard) continue;
            for (unsigned outcome = 0; outcome < 4; outcome++) {
                life_case(heads, observed, trace, NULL, i, outcome, UINT32_MAX,
                    0);
                selected++;
            }
        }
    } else if (family == 2) {
        for (unsigned head = 0; head < heads; head++)
            for (unsigned allocation = 1;
                allocation <= trace->allocations[head] + 1; allocation++) {
                unsigned own = ordinal++ % shards;
                if (own != shard) continue;
                life_case(heads, observed, trace, NULL, UINT32_MAX, 0, head,
                    allocation);
                selected++;
            }
    }
    unsigned owned = ordinal / shards + (shard < ordinal % shards);
    CHECK(selected == owned * ((family < 2 || family == 4) ? 4u : 1u));
    printf(
        "lifecycle family%u heads%u observer%d shard%u/%u trace%u metadata%u/%u/%u handoffs%u/%u work%u/%u allocations%u/%u selected%u cuts%u\n",
        family, heads, observed, shard, shards, trace->count,
        trace->metadata[0], trace->metadata[1], trace->metadata[2],
        trace->handoffs[0], trace->handoffs[1], trace->works[0],
        trace->works[1],
        trace->allocations[0], trace->allocations[1], selected, ordinal);
    free(trace);
}

/* Finite public-return representatives; independent of internal lifecycle cuts. */
typedef wl_columnar_session_test_string_custody_t public_key_t;
typedef struct {
    public_key_t *keys;
    unsigned count, classes, transitions, pairs, selected, focused, segments;
    unsigned excluded, excluded_seed, excluded_retire, excluded_owner;
    unsigned *class_id, *public_ordinal;
    bool *pick, *focus;
} public_trace_t;
static fixture_t *public_fixture;
static bool public_bottom;

static void
public_constructor(const wl_col_session_t *col,
    wl_evaluation_control_t *control,
    uint64_t id, uint32_t round, uint32_t head, bool entering, int result,
    const col_rel_t *output)
{
    (void)output;
    CHECK(public_fixture && col == (wl_col_session_t *)public_fixture->session
        && control == public_fixture->control && !round
        && head < public_fixture->heads
        && id == wl_evaluation_control_active_attempt_id(control));
    if (!entering) {
        CHECK(!result); public_bottom = true;
    }
}

static bool
public_equal(const public_key_t *a, const public_key_t *b)
{
    /* The view consists entirely of initialized uint64_t words. */
    return !memcmp(a, b, sizeof(*a));
}

static void
public_snapshot(const char *name, const int64_t *row, uint32_t width,
    void *opaque)
{
    capture(name, row, width, 1, opaque);
}

static void
public_replay(unsigned heads, bool observed, const public_trace_t *expected,
    public_trace_t *record, unsigned target, unsigned outcome,
    unsigned *retained)
{
    fixture_t f; fixture_init(&f, heads, observed);
    wl_col_session_t *col = (wl_col_session_t *)f.session;
    CHECK(!col->cold_relation && !step(&f) && !wl_session_step(f.reference));
    f.actual.count = f.expected.count = 0;
    const int64_t fresh = wl_intern_put(f.plan->intern, "fresh");
    CHECK(fresh == 2);
    CHECK(!wl_session_insert(f.session, "input", &fresh, 1, 1)
        && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
    col_rel_t *old[2]; col_relation_snapshot_t snapshots[2];
    for (unsigned h = 0; h < heads; h++) {
        old[h] = session_find_rel(col, head_name(&f, h));
        snapshots[h] = wl_columnar_relation_snapshot(old[h]);
    }
    CHECK(!wl_evaluation_control_configure(f.control, UINT64_C(1000000000)));
    stop_t stop = {.fixture = &f, .outcome = outcome};
    CHECK(!public_fixture &&
        !wl_columnar_session_test_string_bottom_checkpoint);
    public_fixture = &f; public_bottom = false;
    wl_columnar_session_test_string_bottom_checkpoint = public_constructor;
    unsigned position = 0;
    uint64_t original = 0;
    int rc = EAGAIN;
    for (unsigned turn = 0; rc == EAGAIN && turn < 1000000; turn++) {
        rc = wl_session_step(f.session);
        public_key_t key;
        CHECK(!col->cold_relation &&
            !wl_columnar_session_test_string_custody(col, &key));
        if (stop.hit || (rc == EAGAIN && !key.published && !key.committed))
            life_public(&f, old, snapshots, stop.successor);
        if (record && rc == EAGAIN && public_bottom && key.round_active
            && !key.committed && !key.published && !key.eligible) {
            record->excluded++;
            record->excluded_seed += key.phase == 18;
            record->excluded_retire += key.phase == 21;
            record->excluded_owner += key.main[0] || key.retirement[0];
        }
        if (!stop.hit && rc == EAGAIN && public_bottom && key.eligible) {
            CHECK(key.available && key.round_active && !key.append_active
                && !key.round_frontiers && key.head_count == heads
                && (key.phase == 8 || key.phase == 19 || key.phase == 20)
                && !key.main[0] && !key.retirement[0]);
            uint64_t active =
                wl_evaluation_control_active_attempt_id(f.control);
            if (!original) original = active;
            CHECK(original && active == original);
            if (expected) {
                if (position >= expected->count ||
                    !public_equal(&key, &expected->keys[position])
                    || turn != expected->public_ordinal[position]) {
                    fprintf(stderr,
                        "public divergence heads%u observer%d ordinal%u target%u phase%llu round%llu head%llu\n",
                        heads, observed, position, target,
                        (unsigned long long)key.phase,
                        (unsigned long long)key.round,
                        (unsigned long long)key.head);
                    CHECK(false);
                }
            }
            if (record) {
                CHECK(position < 16384); record->keys[position] = key;
                record->public_ordinal[position] = turn;
            }
            if (position++ == target) {
                CHECK(expected &&
                    wl_evaluation_control_active_attempt_id(f.control));
                inject(&stop);
                life_public(&f, old, snapshots, stop.successor);
            }
        }
    }
    wl_columnar_session_test_string_bottom_checkpoint = NULL;
    public_fixture = NULL;
    CHECK(outcome < 2);
    int wanted = target == UINT32_MAX ? 0 : outcome == 0
        ? WL_EVALUATION_CONTROL_CANCELLED : WL_EVALUATION_CONTROL_WORK_LIMIT;
    if (rc != wanted) fprintf(stderr,
            "public heads%u observer%d target%u outcome%u hit%d rc%d wanted%d\n",
            heads, observed, target, outcome, stop.hit, rc, wanted);
    if (rc != wanted && expected) {
        const public_key_t *k = &expected->keys[target];
        fprintf(stderr,
            "failed key phase%llu round%llu ri%llu head%llu child%llu/%llu/%llu flags%llu main%llu/%llu/%llu retire%llu/%llu/%llu\n",
            (unsigned long long)k->phase, (unsigned long long)k->round,
            (unsigned long long)k->round_index, (unsigned long long)k->head,
            (unsigned long long)k->child[0], (unsigned long long)k->child[2],
            (unsigned long long)k->child[3],
            (unsigned long long)k->child[4], (unsigned long long)k->main[0],
            (unsigned long long)k->main[1], (unsigned long long)k->main[2],
            (unsigned long long)k->retirement[0],
            (unsigned long long)k->retirement[1],
            (unsigned long long)k->retirement[7]);
    }
    CHECK(rc == wanted && public_bottom);
    if (target != UINT32_MAX) {
        CHECK(stop.hit && position == target + 1 && !col->cleanup_active
            && !col->cleanup_pending && !col->cleanup_reserved_bytes);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report));
        CHECK(report.attempt_id == original &&
            report.limit == UINT64_C(1000000000)
            && report.result == wanted && report.stop_reason == wanted &&
            !report.cleanup_status);
        uint64_t owner[8];
        CHECK(!wl_columnar_session_test_retained_work_owner(col, owner));
        if (outcome == 1 && owner[0]) {
            (*retained)++;
            CHECK(owner[1] && owner[2]
                && f.session->backend->session_controlled_step_state(
                    f.session) == WL_CONTROLLED_STEP_PENDING);
            CHECK(wl_session_insert(f.session, "input", &fresh, 1, 1) == EBUSY
                && wl_session_remove(f.session, "input", &fresh, 1,
                1) == EBUSY);
            events_t unexpected = {0};
            CHECK(wl_session_snapshot(f.session, public_snapshot,
                &unexpected) == EBUSY
                && !unexpected.count);
            wl_session_t *same = f.session;
            CHECK(wl_session_try_destroy(&same) == EBUSY && same == f.session);
            wl_evaluation_control_report_t after;
            CHECK(!wl_evaluation_control_report(f.control, &after)
                && wl_test_completed_step_report_equal(&report, &after));
            life_public(&f, old, snapshots, stop.successor);
        } else {
            public_key_t closed;
            CHECK(!owner[0] && !wl_columnar_session_test_string_custody(col,
                &closed));
            wl_columnar_session_test_completed_view_t completion;
            CHECK(!closed.available &&
                !wl_columnar_session_test_completed_view(col, &completion)
                && !completion.transaction_present);
        }
        CHECK(!wl_evaluation_control_reset(f.control)
            && !wl_evaluation_control_configure(f.control, 0) && !step(&f));
    } else {
        if (expected) CHECK(position == expected->count);
        if (record) record->count = position;
    }
    CHECK(!wl_session_step(f.reference));
    const int64_t all[] = {0, 1, 2}; check_rows(&f, all, 3);
    check_events(&f, 2, 1);
    fixture_close(&f);
}

static void
run_public(unsigned family, unsigned heads, bool observed, unsigned shard,
    unsigned shards)
{
    CHECK(shards && shard < shards);
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.class_id = calloc(16384, sizeof(*t.class_id));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    t.pick = calloc(16384, sizeof(*t.pick));
    t.focus = calloc(16384, sizeof(*t.focus));
    CHECK(t.keys && t.class_id && t.public_ordinal && t.pick && t.focus);
    unsigned retained = 0;
    public_replay(heads, observed, NULL, &t, UINT32_MAX, 0, &retained);
    CHECK(t.count);
    unsigned first[2048], last[2048];
    struct public_pair { unsigned from, to, first, last; };
    struct public_pair *pairs = calloc(16384, sizeof(*pairs));
    CHECK(pairs);
    bool previous = false, candidate = false, child = false, receipt = false,
        compare = false, rebind = false;
    for (unsigned i = 0; i < t.count; i++) {
        public_key_t *k = &t.keys[i];
        unsigned c = 0;
        for (; c < t.classes;
            c++) if (public_equal(k, &t.keys[first[c]])) break;
        if (c == t.classes) {
            CHECK(c < 2048); first[c] = i; t.classes++;
        }
        t.class_id[i] = c; last[c] = i;
        for (unsigned h = 0; h < heads; h++) {
            previous |= (k->owners[h] & 1) != 0;
            candidate |= (k->owners[h] & 2) != 0;
        }
        child |= k->child[0] != 0;
        receipt |= k->receipt_pending || k->receipt_ack;
        compare |= k->phase == 19; rebind |= k->phase == 20;
        bool begin = !i || t.public_ordinal[i] != t.public_ordinal[i-1] + 1;
        if (begin) {
            t.segments++; t.pick[i] = t.focus[i] = true;
            if (i) t.pick[i-1] = t.focus[i-1] = true;
        }
        if (i && !begin && !public_equal(k, &t.keys[i-1])) {
            t.transitions++;
            unsigned pair = 0;
            for (; pair < t.pairs; pair++)
                if (pairs[pair].from == t.class_id[i-1] &&
                    pairs[pair].to == c) break;
            if (pair == t.pairs) {
                CHECK(pair < 16384); t.pairs++;
                pairs[pair].from = t.class_id[i-1]; pairs[pair].to = c;
                pairs[pair].first = i;
            }
            pairs[pair].last = i;
        }
    }
    CHECK(previous && candidate && child && receipt);
    /* Comparison/rebind may have no public yield; report rather than invent it. */
    t.pick[t.count-1] = t.focus[t.count-1] = true;
    for (unsigned c = 0; c < t.classes; c++) {
        t.pick[first[c]] = t.pick[last[c]] = true;
        t.focus[first[c]] = true;
    }
    for (unsigned p = 0; p < t.pairs; p++) {
        unsigned a = pairs[p].first, b = pairs[p].last;
        CHECK(a && b && a <= b);
        CHECK(t.class_id[a-1] == pairs[p].from && t.class_id[a] == pairs[p].to
            && t.class_id[b-1] == pairs[p].from &&
            t.class_id[b] == pairs[p].to);
        t.pick[a-1] = t.pick[a] = t.pick[b-1] = t.pick[b] = true;
        t.focus[a-1] = t.focus[a] = true;
    }
    for (unsigned c = 0; c < t.classes;
        c++) CHECK(t.pick[first[c]] && t.pick[last[c]] && t.focus[first[c]]);
    unsigned ordinal = 0, selected = 0;
    for (unsigned i = 0; i < t.count; i++) {
        t.selected += t.pick[i]; t.focused += t.focus[i];
        CHECK(!t.focus[i] || t.pick[i]);
        if (family == 2 || !(family == 1 ? t.focus[i] : t.pick[i])) continue;
        if (ordinal++ % shards != shard) continue;
        for (unsigned outcome = 0; outcome < 2; outcome++) {
            public_replay(heads, observed, &t, NULL, i, outcome, &retained);
            selected++;
        }
    }
    if (family != 2) {
        CHECK(ordinal == (family == 1 ? t.focused : t.selected));
        CHECK(selected == 2 * (ordinal / shards + (shard < ordinal % shards)));
    }
    printf(
        "public family%u heads%u observer%d shard%u/%u observations%u classes%u transitions%u pairs%u representatives%u focused%u selected%u cuts%u retained_work%u segments%u excluded%u/seed%u/retire%u/owner%u witnesses%u%u%u%u%u%u\n",
        family, heads, observed, shard, shards, t.count, t.classes,
        t.transitions,
        t.pairs, t.selected, t.focused, selected, ordinal, retained,
        t.segments, t.excluded, t.excluded_seed, t.excluded_retire,
        t.excluded_owner,
        previous, candidate, child, receipt, compare, rebind);
    free(t.public_ordinal); free(pairs); free(t.focus); free(t.pick);
    free(t.class_id); free(t.keys);
}

/* Selected MAIN packets must settle before sticky Original WORK is returned. */
typedef struct {
    fixture_t *fixture;
    bool selected;
    unsigned mode, advances, reports;
} retirement_case_t;
static retirement_case_t *retirement_case;

static void
retirement_advanced(wl_col_session_t *col, wl_evaluation_control_t *control,
    uint64_t id, unsigned recipient, unsigned before, unsigned after,
    int result)
{
    (void)before; (void)after; (void)result;
    retirement_case_t *s = retirement_case;
    if (!s || !s->selected || col != (wl_col_session_t *)s->fixture->session
        || recipient != 2) return;
    CHECK(control != s->fixture->control && id
        && wl_evaluation_control_active_attempt_id(control) == id);
    s->advances++;
}

static void
retirement_report(const wl_col_session_t *col, unsigned recipient,
    unsigned action, wl_evaluation_control_report_t report, int result)
{
    (void)result;
    retirement_case_t *s = retirement_case;
    if (!s || !s->selected || col != (wl_col_session_t *)s->fixture->session
        || recipient != 2) return;
    CHECK((s->mode ? action == 1 : action <= 2)
        && report.attempt_id && report.limit == 256);
    s->reports++;
}

static void
run_retirement(unsigned mode, unsigned heads, bool observed)
{
    fixture_t f; fixture_init(&f, heads, observed);
    CHECK(!step(&f) && !wl_session_step(f.reference));
    f.actual.count = f.expected.count = 0;
    const int64_t fresh = wl_intern_put(f.plan->intern, "fresh");
    CHECK(fresh == 2 && !wl_session_insert(f.session, "input", &fresh, 1, 1)
        && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
    wl_col_session_t *col = (wl_col_session_t *)f.session;
    col_rel_t *old[2]; col_relation_snapshot_t snapshots[2];
    for (unsigned h = 0; h < heads; h++) {
        old[h] = session_find_rel(col, head_name(&f, h));
        snapshots[h] = wl_columnar_relation_snapshot(old[h]);
    }
    CHECK(!col->cold_relation && !wl_evaluation_control_configure(f.control,
        UINT64_C(1000000000)));
    retirement_case_t state = {.fixture = &f, .mode = mode};
    CHECK(!retirement_case && !wl_columnar_session_test_main_action_advanced
        && !wl_columnar_session_test_main_action_report);
    retirement_case = &state;
    wl_columnar_session_test_main_action_advanced = retirement_advanced;
    wl_columnar_session_test_main_action_report = retirement_report;
    unsigned selected_turn = 0;
    uint64_t original = 0;
    int rc = EAGAIN;
    for (unsigned turn = 0; rc == EAGAIN && turn < 10000; turn++) {
        rc = wl_session_step(f.session);
        public_key_t key;
        CHECK(!wl_columnar_session_test_string_custody(col, &key));
        if (mode || (rc == EAGAIN && !key.published && !key.committed))
            life_public(&f, old, snapshots, 0);
        if (state.selected || rc != EAGAIN || !key.available || key.phase != 21
            || key.round || key.round_index || key.committed || key.published)
            continue;
        bool available = key.retirement[0] && !key.retirement[1]
            && (key.owners[0] & (UINT64_C(1) << 7)) && key.main[0]
            && key.main[1] == 2 && key.main[2] == 2 && !key.main[3];
        bool fresh_action = key.retirement[0] && !key.main[0];
        bool ready = key.retirement[0] && key.main[0]
            && key.main[1] == 4 && key.main[2] == 2;
        bool cleared_ready = !key.retirement[0] && key.main[0]
            && key.main[1] == 4 && key.main[2] == 2;
        bool selected = mode <= 1 ? available : mode == 2 ? fresh_action
            : mode == 3 ? ready : cleared_ready;
        if (!selected) continue;
        state.selected = true; selected_turn = turn;
        original = wl_evaluation_control_active_attempt_id(f.control);
        CHECK(original);
        if (mode) CHECK(wl_evaluation_control_charge(f.control, UINT64_MAX)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
    wl_columnar_session_test_main_action_advanced = NULL;
    wl_columnar_session_test_main_action_report = NULL;
    retirement_case = NULL;
    int wanted = mode ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
    if (!state.selected || rc != wanted)
        fprintf(stderr,
            "retirement mode%u heads%u observer%d selected%d turn%u rc%d wanted%d advances%u reports%u\n",
            mode, heads, observed, state.selected, selected_turn, rc, wanted,
            state.advances, state.reports);
    CHECK(state.selected && rc == wanted);
    if (mode) {
        CHECK(state.advances == (mode == 1 ? 1u : 0u)
            && state.reports == state.advances);
        uint64_t main_view[16], owner[8];
        CHECK(!wl_columnar_session_test_main_view(col, main_view)
            && !main_view[0] && !col->cleanup_active && !col->cleanup_pending
            && !wl_columnar_session_test_retained_work_owner(col, owner)
            && owner[0] && owner[1] && owner[2]);
        wl_evaluation_control_report_t report, after;
        CHECK(!wl_evaluation_control_report(f.control, &report)
            && report.attempt_id == original
            && report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT
            && report.result == wanted && !report.cleanup_status);
        CHECK(wl_session_insert(f.session, "input", &fresh, 1, 1) == EBUSY
            && wl_session_remove(f.session, "input", &fresh, 1, 1) == EBUSY);
        events_t unexpected = {0};
        CHECK(wl_session_snapshot(f.session, public_snapshot,
            &unexpected) == EBUSY
            && !unexpected.count);
        wl_session_t *same = f.session;
        CHECK(wl_session_try_destroy(&same) == EBUSY && same == f.session);
        CHECK(!wl_evaluation_control_report(f.control, &after)
            && wl_test_completed_step_report_equal(&report, &after));
        life_public(&f, old, snapshots, 0);
        CHECK(!wl_evaluation_control_reset(f.control)
            && !wl_evaluation_control_configure(f.control, 0) && !step(&f));
    } else CHECK(state.advances && state.advances == state.reports);
    CHECK(!wl_session_step(f.reference));
    const int64_t all[] = {0, 1, 2}; check_rows(&f, all, 3);
    check_events(&f, 2, 1);
    printf(
        "retirement mode%u heads%u observer%d witness_turn%u advances%u reports%u result%d retry_rows3 passed\n",
        mode, heads, observed, selected_turn, state.advances, state.reports,
        rc);
    fixture_close(&f);
}

/* Real public CONCAT owners, independent of historical diagnostic ordinals. */
typedef struct {
    public_key_t first_key, last_key;
    unsigned first, last, category;
    uint64_t round, head;
} ended_cut_t;
typedef struct { ended_cut_t cuts[16]; unsigned count; } ended_trace_t;

static int
ended_category(const public_key_t *key)
{
    if (!key->available || key->phase != 8 || !key->child[0]
        || !key->child[1] || !(key->child[4] & 4)
        || !(key->child[4] & 64) || !key->child[5]) return -1;
    if (key->child[9] == 1 + WL_PLAN_OP_VARIABLE) return 2;
    if (key->child[9] != 1 + WL_PLAN_OP_CONCAT) return -1;
    if (key->child[6] == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        && key->child[7] == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_START)
        return 0;
    return key->child[6] == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_CONCAT_CHILD
        ? 1 : -1;
}

/* Explicit initialized operation-stage tuple, not public row/copy offsets. */
static bool
endstage_eligible(const public_key_t *k)
{
    return k->available && k->eligible && k->phase == 8 && k->round_active
           && !k->published && !k->committed && !k->main[0] && !k->retirement[0]
           && k->child[0] && k->child[1] && (k->child[4] & 4)
           && (k->child[4] & 64) && k->child[5];
}

static bool
endstage_equal(const public_key_t *a, const public_key_t *b)
{
    return a->round == b->round && a->head == b->head
           && a->round_active == b->round_active
           && a->round_changed == b->round_changed
           && a->append_active == b->append_active
           && a->round_frontiers == b->round_frontiers
           && a->child_active == b->child_active
           && a->child_thread_valid == b->child_thread_valid
           && a->receipt_pending == b->receipt_pending
           && a->receipt_ack == b->receipt_ack
           && a->receipt_acknowledged == b->receipt_acknowledged
           && a->receipt_index_valid == b->receipt_index_valid
           && a->ack_committed == b->ack_committed
           && !memcmp(a->owners, b->owners, sizeof(a->owners))
           && !memcmp(a->child + 2, b->child + 2, 11 * sizeof(uint64_t));
}

/* Frame-present capture-only states with no bound child, before or after ACK. */
static bool
unbound_eligible(const public_key_t *k)
{
    return k->available && k->eligible && k->phase == 8 && k->round_active
           && !k->published && !k->committed && !k->main[0] && !k->retirement[0]
           && k->child[0] && k->child[1] && (k->child[4] & 64)
           && !(k->child[4] & 4);
}

/* A bound framed child whose numeric executor has already closed. */
static bool
cleared_eligible(const public_key_t *k)
{
    return k->available && k->eligible && k->phase == 8 && k->round_active
           && !k->published && !k->committed && !k->main[0] && !k->retirement[0]
           && k->child[0] && k->child[1] && (k->child[4] & 64)
           && (k->child[4] & 4) && !k->child[5];
}

/* Frame disposal has completed; the Session retains the captured receipt. */
static bool
receipt_eligible(const public_key_t *k)
{
    return k->available && k->eligible && k->phase == 8 && k->round_active
           && !k->published && !k->committed && !k->main[0] && !k->retirement[0]
           && !k->child[0] && (k->receipt_pending || k->receipt_ack
           || k->receipt_acknowledged || k->receipt_index_valid ||
           k->ack_committed);
}

/* Actual recursive retirement returns, including the terminal head boundary. */
static bool
owner_domain(const public_key_t *k)
{
    return k->available && k->round_active && k->phase == 21
           && !k->published && !k->committed;
}

/* Publication is a separate boundary with a different public-image oracle. */
static bool
observer_end_domain(const public_key_t *k)
{
    return k->available && !k->published && !k->committed
           && (k->phase == 9 || k->phase == 10);
}

static bool
prepublication_end_domain(const public_key_t *k)
{
    return k->available && !k->published && !k->committed
           && k->phase >= 11 && k->phase <= 14;
}

static bool
owner_unselected(const public_key_t *k)
{
    return owner_domain(k) && !k->main[0];
}

static bool
owner_selected(const public_key_t *k)
{
    return owner_domain(k) && k->main[0] && k->main[2] == 2;
}

typedef struct { fixture_t *fixture; unsigned advances, reports; } owner_auth_t;
static owner_auth_t *owner_auth;

static void
owner_advanced(wl_col_session_t *col, wl_evaluation_control_t *control,
    uint64_t id, unsigned recipient, unsigned before, unsigned after,
    int result)
{
    (void)before; (void)after; (void)result;
    if (!owner_auth || recipient != 2) return;
    CHECK(col == (wl_col_session_t *)owner_auth->fixture->session
        && control != owner_auth->fixture->control && id
        && wl_evaluation_control_active_attempt_id(control) == id);
    owner_auth->advances++;
}

static void
owner_report(const wl_col_session_t *col, wl_evaluation_control_t *control,
    uint64_t allocation_identity, wl_evaluation_control_report_t report)
{
    if (!owner_auth) return;
    uint64_t main[16];
    CHECK(col == (wl_col_session_t *)owner_auth->fixture->session
        && !wl_columnar_session_test_main_view(col, main));
    if (!main[0] || main[2] != 2) return;
    wl_evaluation_control_report_t actual;
    CHECK(control != owner_auth->fixture->control && allocation_identity
        && allocation_identity == wl_evaluation_control_identity(control)
        && !wl_evaluation_control_active_attempt_id(control)
        && !wl_evaluation_control_report(control, &actual)
        && wl_test_completed_step_report_equal(&actual, &report)
        && report.attempt_id && report.limit == 256);
    owner_auth->reports++;
}

static void
ended_case_record(unsigned heads, bool observed, ended_trace_t *record,
    const public_key_t *expected, unsigned target, unsigned outcome, bool early,
    public_trace_t *trace, bool (*record_domain)(const public_key_t *))
{
    fixture_t f; fixture_init(&f, heads, observed);
    CHECK(!step(&f) && !wl_session_step(f.reference));
    f.actual.count = f.expected.count = 0;
    const int64_t fresh = wl_intern_put(f.plan->intern, "fresh");
    CHECK(fresh == 2 && !wl_session_insert(f.session, "input", &fresh, 1, 1)
        && !wl_session_insert(f.reference, "input", &fresh, 1, 1));
    wl_col_session_t *col = (wl_col_session_t *)f.session;
    CHECK(!col->cold_relation && !wl_evaluation_control_configure(f.control,
        UINT64_C(1000000000)));
    col_rel_t *old[2]; col_relation_snapshot_t snapshots[2];
    for (unsigned h = 0; h < heads; h++) {
        old[h] = session_find_rel(col, head_name(&f, h));
        snapshots[h] = wl_columnar_relation_snapshot(old[h]);
    }
    stop_t stop = {.fixture = &f, .outcome = outcome};
    bool stopping_case = expected || early;
    owner_auth_t auth = {.fixture = &f};
    bool authenticate_main = expected && owner_selected(expected);
    if (authenticate_main) {
        CHECK(!owner_auth && !wl_columnar_session_test_main_action_advanced
            && !wl_columnar_session_test_main_report_finished);
        owner_auth = &auth;
        wl_columnar_session_test_main_action_advanced = owner_advanced;
        wl_columnar_session_test_main_report_finished = owner_report;
    }
    unsigned calls = 0, after_end = 0;
    int rc = EAGAIN, first_error = 0;
    bool terminal = false;
    for (; !terminal && calls < 20000; calls++) {
        rc = wl_session_step(f.session);
        public_key_t key;
        CHECK(!col->cold_relation
            && !wl_columnar_session_test_string_custody(col, &key));
        if (stopping_case || (rc == EAGAIN && !key.published && !key.committed))
            life_public(&f, old, snapshots, stop.successor);
        if (trace && rc == EAGAIN && record_domain(&key)) {
            CHECK(trace->count < 16384);
            unsigned at = trace->count++;
            trace->keys[at] = key;
            trace->public_ordinal[at] = calls;
        }
        int category = ended_category(&key);
        if (record && rc == EAGAIN && category >= 0 && category < 2) {
            unsigned i = 0;
            for (; i < record->count; i++)
                if (record->cuts[i].round == key.round
                    && record->cuts[i].head == key.head
                    && record->cuts[i].category == (unsigned)category) break;
            if (i == record->count) {
                CHECK(i < 16); record->count++;
                record->cuts[i] = (ended_cut_t){.first_key = key,
                                                .first = calls,
                                                .category = (unsigned)category,
                                                .round = key.round,
                                                .head = key.head};
            }
            record->cuts[i].last = calls; record->cuts[i].last_key = key;
        }
        if (!stop.hit && stopping_case && rc == EAGAIN
            && (early ? category == 2 : calls == target)) {
            CHECK(category >= 0 || endstage_eligible(&key)
                || unbound_eligible(&key) || cleared_eligible(&key)
                || receipt_eligible(&key) || owner_unselected(&key)
                || owner_selected(&key) || observer_end_domain(&key)
                || prepublication_end_domain(&key));
            if (expected) CHECK(public_equal(&key, expected));
            inject(&stop);
            CHECK(wl_session_insert(f.session, "input", &fresh, 1, 1) == EBUSY
                && wl_session_remove(f.session, "input", &fresh, 1,
                1) == EBUSY);
            events_t unexpected = {0};
            CHECK(wl_session_snapshot(f.session, public_snapshot,
                &unexpected) == EBUSY
                && !unexpected.count);
            wl_session_t *same = f.session;
            CHECK(wl_session_try_destroy(&same) == EBUSY && same == f.session);
        }
        if (stop.hit) {
            after_end++;
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && wl_test_completed_step_report_equal(&report, &stop.frozen));
            if (stop.successor) {
                uint64_t used = UINT64_MAX;
                CHECK(wl_evaluation_control_active_attempt_id(f.control) ==
                    stop.successor
                    && !wl_evaluation_control_test_active_consumed(f.control,
                    stop.successor, &used) && !used);
            }
            if (rc != EAGAIN) {
                if (rc != EINVAL) fprintf(stderr,
                        "ended unexpected target%u outcome%u rc%d first%d main%llu phase%llu progress%llu\n",
                        target, outcome, rc, first_error,
                        (unsigned long long)key.main[0],
                        (unsigned long long)key.main[1],
                        (unsigned long long)key.retirement[0]);
                CHECK(rc == EINVAL);
                if (!first_error) first_error = rc;
                CHECK(first_error == rc);
                int state =
                    f.session->backend->session_controlled_step_state(
                    f.session);
                terminal = state !=
                    WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING;
            }
        } else if (rc != EAGAIN) terminal = true;
    }
    if (!terminal) fprintf(stderr,
            "ended heads%u observer%d target%u outcome%u hit%d after%u first%d rc%d terminal%d\n",
            heads, observed, target, outcome, stop.hit, after_end, first_error,
            rc, terminal);
    CHECK(terminal);
    if (authenticate_main) {
        CHECK(auth.advances && auth.reports);
        wl_columnar_session_test_main_action_advanced = NULL;
        wl_columnar_session_test_main_report_finished = NULL;
        owner_auth = NULL;
    }
    if (stopping_case) {
        CHECK(stop.hit && rc == EINVAL && first_error == EINVAL
            && !col->cleanup_active && !col->cleanup_pending
            && !col->cleanup_reserved_bytes
            && f.session->backend->session_controlled_step_state(f.session) ==
            WL_CONTROLLED_STEP_IDLE);
        wl_columnar_session_test_completed_view_t completed;
        uint64_t main_view[16], retirement[16];
        CHECK(!wl_columnar_session_test_completed_view(col, &completed)
            && !completed.transaction_present && !completed.main_pending
            && !completed.parent_pending
            && !wl_columnar_session_test_main_view(col,
            main_view) && !main_view[0]
            && !wl_columnar_session_test_retirement_view(col, retirement)
            && !retirement[0]);
        if (expected && (observer_end_domain(expected)
            || prepublication_end_domain(expected)))
            CHECK(!col->delta_observer && !col->delta_observer_build
                && !col->delta_observer_prepare
                && !col->delta_observer_retirement_governor
                && !col->delta_observer_retirement_gc);
        if (stop.successor) {
            /* Its genuine owner may poll only after old cleanup is closed. */
            CHECK(!wl_evaluation_control_charge_attempt(f.control,
                stop.successor, 0));
            wl_evaluation_control_attempt_finish_result_t ended =
                wl_evaluation_control_finish_attempt(f.control,
                    f.session->operation_admission, stop.successor, 0, 0);
            CHECK(!ended.protocol && ended.report_valid
                && !ended.report.consumed && !ended.report.stop_reason
                && !ended.report.execution_status &&
                !ended.report.cleanup_status
                && !ended.report.result);
        } else CHECK(!wl_evaluation_control_active_attempt_id(f.control));
        CHECK(!wl_evaluation_control_reset(f.control)
            && !wl_evaluation_control_configure(f.control, 0));
        int retried = EAGAIN;
        for (unsigned retry_calls = 0;
            retried == EAGAIN && retry_calls < 1000000;
            retry_calls++) {
            retried = wl_session_step(f.session);
            if (expected &&
                (owner_domain(expected) || observer_end_domain(expected)
                || prepublication_end_domain(expected))
                && retried == EAGAIN)
                CHECK(f.session->backend->session_controlled_step_state(
                        f.session)
                    != WL_CONTROLLED_STEP_IDLE);
        }
        if (retried) fprintf(stderr,
                "ended retry target%u outcome%u result%d\n",
                target, outcome, retried);
        CHECK(!retried);
    } else CHECK(!rc);
    CHECK(!wl_session_step(f.reference));
    const int64_t all[] = {0, 1, 2}; check_rows(&f, all, 3);
    check_events(&f, 2, 1);
    fixture_close(&f);
}

static void
ended_case(unsigned heads, bool observed, ended_trace_t *record,
    const public_key_t *expected, unsigned target, unsigned outcome, bool early)
{
    ended_case_record(heads, observed, record, expected, target, outcome, early,
        NULL, NULL);
}

/* Only initialized semantic custody enters the finite key, never raw IDs or
 * row/copy offsets. The complete normalized snapshot still authenticates replay. */
static bool
owner_equal(const public_key_t *a, const public_key_t *b)
{
    return a->round == b->round && a->round_index == b->round_index
           && a->round_changed == b->round_changed
           && a->child_active == b->child_active
           && a->child_thread_valid == b->child_thread_valid
           && a->receipt_pending == b->receipt_pending
           && a->receipt_ack == b->receipt_ack
           && a->receipt_acknowledged == b->receipt_acknowledged
           && a->receipt_index_valid == b->receipt_index_valid
           && a->ack_committed == b->ack_committed
           && !memcmp(a->owners, b->owners, sizeof(a->owners))
           && !memcmp(a->retirement, b->retirement, sizeof(a->retirement))
           && !memcmp(a->main, b->main, sizeof(a->main))
           && !memcmp(a->child, b->child, sizeof(a->child));
}

static void
run_owner(unsigned heads, bool observed, bool census, bool selected_main,
    unsigned shard, unsigned shards)
{
    CHECK(shards && shard < shards);
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    t.class_id = calloc(16384, sizeof(*t.class_id));
    t.pick = calloc(16384, sizeof(*t.pick));
    CHECK(t.keys && t.public_ordinal && t.class_id && t.pick);
    ended_case_record(heads, observed, NULL, NULL, UINT32_MAX, 0, false, &t,
        owner_domain);
    unsigned first[2048], last[2048], classes = 0, observations = 0;
    unsigned markers[4] = {0}, rounds = 0;
    bool reached[16][3] = {{false}};
    uint64_t roles = 0, native = 0, main_phases = 0, finance_phases = 0;
    bool packet_seen[64][2] = {{false}};
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i]; t.class_id[i] = UINT32_MAX;
        CHECK(owner_domain(k) && k->round < 16 && k->round_index <= heads);
        markers[(k->retirement[0] != 0) | ((k->main[0] != 0) << 1)]++;
        if (!(selected_main ? owner_selected(k) :
            owner_unselected(k))) continue;
        if (selected_main) {
            CHECK(k->round_index < heads && k->main[1] < 64 && k->main[5] < 64);
            main_phases |= UINT64_C(1) << k->main[1];
            finance_phases |= UINT64_C(1) << k->main[5];
            unsigned live = k->retirement[0] != 0;
            if (census && !packet_seen[k->main[1]][live]) {
                packet_seen[k->main[1]][live] = true;
                printf(
                    "mainowner_witness public%u phase%llu called%llu result%lld finance%llu progress%u\n",
                    t.public_ordinal[i], (unsigned long long)k->main[1],
                    (unsigned long long)k->main[3],
                    (long long)(int64_t)k->main[14],
                    (unsigned long long)k->main[5], live);
            }
        }
        observations++; reached[k->round][k->round_index] = true;
        if (k->round + 1 > rounds) rounds = (unsigned)k->round + 1;
        if (k->retirement[0]) {
            CHECK(k->round_index < heads && k->retirement[4] < 64
                && k->retirement[5] == k->round_index && k->retirement[7] < 64);
            roles |= UINT64_C(1) << k->retirement[4];
            native |= UINT64_C(1) << k->retirement[7];
        }
        unsigned c = 0;
        for (; c < classes; c++) if (owner_equal(k, &t.keys[first[c]])) break;
        if (c == classes) {
            CHECK(c < 2048); first[c] = i; classes++;
        }
        last[c] = i; t.class_id[i] = c;
    }
    CHECK(observations && classes && rounds && roles && !markers[0] &&
        markers[1]);
    for (unsigned r = 0; r < rounds; r++)
        for (unsigned h = 0; h < heads; h++) CHECK(reached[r][h]);
    for (unsigned c = 0; c < classes;
        c++) t.pick[first[c]] = t.pick[last[c]] = true;
    unsigned cuts = 0, selected = 0;
    for (unsigned i = 0; i < t.count; i++) {
        if (!t.pick[i]) continue;
        unsigned index = cuts++, c = t.class_id[i]; CHECK(c < classes);
        if (census || index % shards != shard) continue;
        for (unsigned outcome = 2; outcome <= 3; outcome++) {
            ended_case(heads, observed, NULL, &t.keys[i],
                t.public_ordinal[i], outcome, false);
            selected++;
            printf(
                "%s_cut heads%u observer%d index%u class%u public%u first%d last%d outcome%u passed\n",
                selected_main ? "mainowner" : "owner", heads, observed, index,
                c,
                t.public_ordinal[i], i == first[c], i == last[c], outcome);
        }
    }
    unsigned owned = cuts / shards + (shard < cuts % shards);
    CHECK(census || selected == 2 * owned);
    printf(
        "%s heads%u observer%d census%d shard%u/%u total%u observations%u classes%u cuts%u selected%u rounds%u markers%u/%u/%u/%u roles%llu native%llu main_phases%llu finance_phases%llu passed\n",
        selected_main ? "mainowner" : "owner", heads, observed, census, shard,
        shards, t.count,
        observations, classes, cuts, selected,
        rounds, markers[0], markers[1], markers[2], markers[3],
        (unsigned long long)roles, (unsigned long long)native,
        (unsigned long long)main_phases, (unsigned long long)finance_phases);
    free(t.pick); free(t.class_id); free(t.public_ordinal); free(t.keys);
}

/* Replay the first and last return of each normalized observer work state. */
static void
run_observer_end(unsigned heads, bool observed, unsigned phase, bool census)
{
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    CHECK(t.keys && t.public_ordinal);
    ended_case_record(heads, observed, NULL, NULL, UINT32_MAX, 0, false, &t,
        observer_end_domain);
    unsigned counts[2] = {0};
    unsigned first[128], last[128], classes = 0, histories = 0;
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i];
        CHECK(observer_end_domain(k));
        counts[k->phase - 9]++;
        if (k->phase != phase) continue;
        CHECK(!k->round_active && !k->child_active && !k->child[0]
            && !k->receipt_pending && !k->receipt_ack && !k->retirement[0]
            && !k->main[0] && !k->observer[0] && !k->resource[0]
            && (phase == 9
            ? k->observer_work[0] && !k->observer_work[2]
            : !k->observer_work[0] && k->observer_work[2]));
        unsigned cls = 0;
        for (; cls < classes; cls++)
            if (public_equal(k, &t.keys[first[cls]])) break;
        if (cls == classes) {
            CHECK(classes < 128);
            first[classes++] = i;
        }
        last[cls] = i;
    }
    CHECK(observed ? counts[0] && counts[1] && classes : !t.count);
    if (!census)
        for (unsigned cls = 0; cls < classes; cls++)
            for (unsigned end = 0; end < 2; end++) {
                unsigned at = end ? last[cls] : first[cls];
                if (end && at == first[cls]) continue;
                for (unsigned outcome = 2; outcome <= 3; outcome++) {
                    ended_case(heads, observed, NULL, &t.keys[at],
                        t.public_ordinal[at], outcome, false);
                    histories++;
                    printf(
                        "observer_end_cut heads%u observer%d phase%u class%u public%u endpoint%u outcome%u passed\n",
                        heads, observed, phase, cls, t.public_ordinal[at], end,
                        outcome);
                }
            }
    printf(
        "observer_end heads%u observer%d baseline%u events%u phase%u classes%u histories%u\n",
        heads, observed, counts[0], counts[1], phase, classes, histories);
    free(t.public_ordinal);
    free(t.keys);
}

/* Audit the entire public observer domain before selecting any replay cuts. */
static void
run_observer_audit(unsigned heads, bool observed)
{
    public_trace_t trace = {0};
    trace.keys = calloc(16384, sizeof(*trace.keys));
    trace.public_ordinal = calloc(16384, sizeof(*trace.public_ordinal));
    CHECK(trace.keys && trace.public_ordinal);
    ended_case_record(heads, observed, NULL, NULL, UINT32_MAX, 0, false,
        &trace, observer_end_domain);
    unsigned returns[2] = {0}, readers[2] = {0};
    for (unsigned i = 0; i < trace.count; i++) {
        const public_key_t *key = &trace.keys[i];
        CHECK(observer_end_domain(key) && key->head_count == heads
            && !key->round_active && !key->child_active && !key->child[0]
            && !key->receipt_pending && !key->receipt_ack
            && !key->retirement[0] && !key->main[0]
            && !key->observer[0] && !key->resource[0]);
        unsigned family = (unsigned)key->phase - 9;
        unsigned work = 2 * family, other = 2 * (1 - family);
        CHECK(key->observer_work[work] && !key->observer_work[other]
            && !key->observer_work[other + 1]);
        returns[family]++;
        readers[family] += key->observer_work[work + 1] != 0;
    }
    if (observed) {
        CHECK(readers[0] && readers[0] < returns[0]
            && readers[1] && readers[1] < returns[1]);
    } else CHECK(!trace.count);
    printf(
        "observer_audit heads%u observer%d baseline%u readers%u events%u readers%u passed\n",
        heads, observed, returns[0], readers[0], returns[1], readers[1]);
    free(trace.public_ordinal);
    free(trace.keys);
}

static void
run_readers_end(unsigned heads, bool observed)
{
    public_trace_t trace = {0};
    trace.keys = calloc(16384, sizeof(*trace.keys));
    trace.public_ordinal = calloc(16384, sizeof(*trace.public_ordinal));
    CHECK(trace.keys && trace.public_ordinal);
    ended_case_record(heads, observed, NULL, NULL, UINT32_MAX, 0, false,
        &trace, prepublication_end_domain);
    unsigned readers = 0;
    for (unsigned i = 0; i < trace.count; i++) {
        const public_key_t *key = &trace.keys[i];
        if (key->phase != 12) continue;
        CHECK(key->claim[0] < key->claim[1] && key->claim[2]
            && key->claim[3] && !key->claim[4] && !key->main[0]);
        for (unsigned outcome = 2; outcome <= 3; outcome++)
            ended_case(heads, observed, NULL, key, trace.public_ordinal[i],
                outcome, false);
        readers++;
    }
    CHECK(readers == heads);
    printf("readers_end heads%u observer%d returns%u histories%u passed\n",
        heads, observed, readers, 2 * readers);
    free(trace.public_ordinal);
    free(trace.keys);
}

static void
run_ended(unsigned heads, bool observed)
{
    ended_trace_t *trace = calloc(1, sizeof(*trace)); CHECK(trace);
    ended_case(heads, observed, trace, NULL, UINT32_MAX, 2, false);
    CHECK(trace->count);
    unsigned histories = 0, rounds = 0;
    bool category[2] = {false, false};
    for (unsigned i = 0; i < trace->count; i++) {
        const ended_cut_t *cut = &trace->cuts[i];
        CHECK(cut->category < 2 && cut->head < heads &&
            cut->first <= cut->last);
        category[cut->category] = true;
        if (cut->round + 1 > rounds) rounds = (unsigned)cut->round + 1;
        for (unsigned end = 0; end < 2; end++) {
            unsigned target = end ? cut->last : cut->first;
            if (end && cut->last == cut->first) continue;
            for (unsigned outcome = 2; outcome <= 3; outcome++) {
                ended_case(heads, observed, NULL,
                    end ? &cut->last_key : &cut->first_key, target, outcome,
                    false);
                histories++;
            }
        }
    }
    CHECK(category[0] && category[1] && trace->count == 2 * rounds);
    for (unsigned outcome = 2; outcome <= 3; outcome++)
        ended_case(heads, observed, NULL, NULL, UINT32_MAX, outcome, true);
    printf(
        "ended heads%u observer%d categories%u rounds%u concat_histories%u early_controls2 healthy_controls1 passed\n",
        heads, observed, trace->count, rounds, histories);
    free(trace);
}

/* First/last representatives of each discovered authenticated child stage. */
static void
run_endstage(unsigned heads, bool observed, unsigned shard, unsigned shards)
{
    CHECK(shards && shard < shards);
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    t.pick = calloc(16384, sizeof(*t.pick));
    t.class_id = calloc(16384, sizeof(*t.class_id));
    CHECK(t.keys && t.public_ordinal && t.pick && t.class_id);
    unsigned retained = 0;
    public_replay(heads, observed, NULL, &t, UINT32_MAX, 0, &retained);
    unsigned first[2048], last[2048], classes = 0, observations = 0;
    unsigned unbound = 0, absent = 0, other = 0, rounds = 0;
    bool reached[16][2] = {{false}}, concat[16][2][2] = {{{false}}};
    uint64_t ops = 0, outer = 0, numeric = 0, receipts = 0;
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i];
        t.class_id[i] = UINT32_MAX;
        if (!endstage_eligible(k)) {
            if (k->phase != 8) other++;
            else if (!k->child[0]) absent++;
            else if (!(k->child[4] & 4)) unbound++;
            else other++;
            continue;
        }
        CHECK(k->round < 16 && k->head < heads
            && k->child[9] < 64 && k->child[2] < 64 && k->child[6] < 64);
        observations++;
        reached[k->round][k->head] = true;
        if (k->round + 1 > rounds) rounds = (unsigned)k->round + 1;
        ops |= UINT64_C(1) << k->child[9];
        outer |= UINT64_C(1) << k->child[2];
        numeric |= UINT64_C(1) << k->child[6];
        receipts |= UINT64_C(1) << (k->receipt_pending | (k->receipt_ack << 1)
            | (k->receipt_acknowledged << 2) | (k->receipt_index_valid << 3)
            | (k->ack_committed << 4));
        int category = ended_category(k);
        if (category >= 0 && category < 2)
            concat[k->round][k->head][category] = true;
        unsigned c = 0;
        for (; c < classes; c++)
            if (endstage_equal(k, &t.keys[first[c]])) break;
        if (c == classes) {
            CHECK(c < 2048); first[c] = i; classes++;
        }
        last[c] = i; t.class_id[i] = c;
    }
    CHECK(classes && observations && rounds && (ops &
        (UINT64_C(1) << (1 + WL_PLAN_OP_VARIABLE)))
        && (ops & (UINT64_C(1) << (1 + WL_PLAN_OP_CONCAT))));
    bool declared_concat[2] = {false, false};
    fixture_t declared; fixture_init(&declared, heads, observed);
    for (unsigned h = 0; h < heads; h++) {
        const wl_plan_body_t *body = &declared.plan->strata[0].bodies[h];
        for (uint32_t op = 0; op < body->op_count; op++)
            declared_concat[h] |= body->ops[op].op == WL_PLAN_OP_CONCAT;
    }
    fixture_close(&declared);
    for (unsigned r = 0; r < rounds; r++)
        for (unsigned h = 0; h < heads; h++)
            CHECK(reached[r][h] && concat[r][h][0] == declared_concat[h]
                && concat[r][h][1] == declared_concat[h]);
    for (unsigned c = 0; c < classes; c++) {
        CHECK(first[c] <= last[c] && t.class_id[first[c]] == c
            && t.class_id[last[c]] == c);
        t.pick[first[c]] = t.pick[last[c]] = true;
    }
    unsigned cuts = 0, selected = 0;
    for (unsigned i = 0; i < t.count; i++) {
        if (!t.pick[i]) continue;
        unsigned ordinal = cuts++;
        if (ordinal % shards != shard) continue;
        unsigned c = t.class_id[i]; CHECK(c < classes);
        for (unsigned outcome = 2; outcome <= 3; outcome++) {
            ended_case(heads, observed, NULL, &t.keys[i],
                t.public_ordinal[i], outcome, false);
            selected++;
            printf(
                "endstage_cut heads%u observer%d shard%u/%u index%u class%u public%u first%d last%d outcome%u passed\n",
                heads, observed, shard, shards, ordinal, c,
                t.public_ordinal[i], i == first[c], i == last[c], outcome);
        }
    }
    CHECK(selected == 2 * (cuts / shards + (shard < cuts % shards)));
    printf(
        "endstage heads%u observer%d shard%u/%u observations%u classes%u cuts%u selected%u rounds%u ops%llu outer%llu numeric%llu receipts%llu excluded_unbound%u/frame_absent%u/other%u/retire%u/owner%u passed\n",
        heads, observed, shard, shards, observations, classes, cuts, selected,
        rounds, (unsigned long long)ops, (unsigned long long)outer,
        (unsigned long long)numeric, (unsigned long long)receipts, unbound,
        absent, other, t.excluded_retire, t.excluded_owner);
    free(t.class_id); free(t.pick); free(t.public_ordinal); free(t.keys);
}

/* Exact first/last unbound classes; numeric identity may still be present. */
static void
run_unbound(unsigned heads, bool observed)
{
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    t.class_id = calloc(16384, sizeof(*t.class_id));
    t.pick = calloc(16384, sizeof(*t.pick));
    CHECK(t.keys && t.public_ordinal && t.class_id && t.pick);
    unsigned retained = 0;
    public_replay(heads, observed, NULL, &t, UINT32_MAX, 0, &retained);
    unsigned first[2048], last[2048], classes = 0, observations = 0, rounds = 0;
    unsigned absent = 0, bound = 0, cleared = 0, other = 0;
    bool reached[16][2] = {{false}}, lookup[16][2] = {{false}},
        spare[16][2] = {{false}};
    uint64_t phases = 0, pending = 0, flags = 0, identities = 0, receipts = 0;
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i]; t.class_id[i] = UINT32_MAX;
        if (!unbound_eligible(k)) {
            if (!k->child[0]) absent++;
            else if (k->child[4] & 4) {
                if (k->child[5]) bound++;
                else cleared++;
            } else other++;
            continue;
        }
        CHECK(k->round < 16 && k->head < heads && k->child[2] < 64
            && k->child[3] < 64 && k->child[5] < 2);
        observations++; reached[k->round][k->head] = true;
        if (k->round + 1 > rounds) rounds = (unsigned)k->round + 1;
        if (k->child[2] == WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP)
            CHECK(!(k->child[4] & 128));
        if (k->child[2] == WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF)
            CHECK(k->child[4] & 128);
        lookup[k->round][k->head] |= k->child[2] ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP;
        spare[k->round][k->head] |= k->child[2] ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF;
        phases |= UINT64_C(1) << k->child[2];
        pending |= UINT64_C(1) << k->child[3]; flags |= k->child[4];
        identities |= UINT64_C(1) << k->child[5];
        receipts |= UINT64_C(1) << (k->receipt_pending | (k->receipt_ack << 1)
            | (k->receipt_acknowledged << 2) | (k->receipt_index_valid << 3)
            | (k->ack_committed << 4));
        unsigned c = 0;
        for (; c < classes; c++)
            if (endstage_equal(k, &t.keys[first[c]])) break;
        if (c == classes) {
            CHECK(c < 2048); first[c] = i; classes++;
        }
        last[c] = i; t.class_id[i] = c;
    }
    CHECK(observations && classes && rounds);
    for (unsigned r = 0; r < rounds; r++)
        for (unsigned h = 0; h < heads; h++)
            CHECK(reached[r][h] && lookup[r][h] && spare[r][h]);
    for (unsigned c = 0; c < classes; c++) {
        CHECK(first[c] <= last[c] && t.class_id[first[c]] == c
            && t.class_id[last[c]] == c);
        t.pick[first[c]] = t.pick[last[c]] = true;
    }
    unsigned cuts = 0, selected = 0;
    for (unsigned i = 0; i < t.count; i++) {
        if (!t.pick[i]) continue;
        unsigned index = cuts++, c = t.class_id[i]; CHECK(c < classes);
        for (unsigned outcome = 2; outcome <= 3; outcome++) {
            ended_case(heads, observed, NULL, &t.keys[i],
                t.public_ordinal[i], outcome, false);
            selected++;
            printf(
                "unbound_cut heads%u observer%d index%u class%u public%u first%d last%d outcome%u passed\n",
                heads, observed, index, c, t.public_ordinal[i],
                i == first[c], i == last[c], outcome);
        }
    }
    CHECK(selected == 2 * cuts);
    printf(
        "unbound heads%u observer%d observations%u classes%u cuts%u selected%u rounds%u phases%llu pending%llu flags%llu numeric_identities%llu receipts%llu excluded_frame_absent%u/bound_numeric%u/bound_cleared%u/other%u/retire%u/owner%u passed\n",
        heads, observed, observations, classes, cuts, selected, rounds,
        (unsigned long long)phases, (unsigned long long)pending,
        (unsigned long long)flags, (unsigned long long)identities,
        (unsigned long long)receipts, absent, bound, cleared, other,
        t.excluded_retire, t.excluded_owner);
    free(t.pick); free(t.class_id); free(t.public_ordinal); free(t.keys);
}

/* Discover finite stage classes, then authenticate complete copied keys on replay. */
static void
run_custody(unsigned heads, bool observed, bool receipt)
{
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    t.class_id = calloc(16384, sizeof(*t.class_id));
    t.pick = calloc(16384, sizeof(*t.pick));
    CHECK(t.keys && t.public_ordinal && t.class_id && t.pick);
    unsigned retained = 0;
    public_replay(heads, observed, NULL, &t, UINT32_MAX, 0, &retained);
    unsigned first[2048], last[2048], classes = 0, observations = 0, rounds = 0;
    bool reached[16][2] = {{false}};
    uint64_t phases = 0, pending = 0, flags = 0, receipts = 0;
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i]; t.class_id[i] = UINT32_MAX;
        if (!(receipt ? receipt_eligible(k) : cleared_eligible(k))) continue;
        CHECK(k->round < 16 && k->head < heads && k->child[2] < 64
            && k->child[3] < 64);
        if (receipt) {
            for (unsigned j = 0; j < 16; j++) CHECK(!k->child[j]);
            CHECK(i && t.keys[i - 1].round == k->round
                && t.keys[i - 1].head == k->head
                && unbound_eligible(&t.keys[i - 1]));
        } else for (unsigned j = 5; j <= 9; j++) CHECK(!k->child[j]);
        observations++; reached[k->round][k->head] = true;
        if (k->round + 1 > rounds) rounds = (unsigned)k->round + 1;
        phases |= UINT64_C(1) << k->child[2];
        pending |= UINT64_C(1) << k->child[3]; flags |= k->child[4];
        unsigned receipt = k->receipt_pending | (k->receipt_ack << 1)
            | (k->receipt_acknowledged << 2) | (k->receipt_index_valid << 3)
            | (k->ack_committed << 4);
        CHECK(receipt < 32); receipts |= UINT64_C(1) << receipt;
        unsigned c = 0;
        for (; c < classes; c++)
            if (endstage_equal(k, &t.keys[first[c]])) break;
        if (c == classes) {
            CHECK(c < 2048); first[c] = i; classes++;
        }
        last[c] = i; t.class_id[i] = c;
    }
    CHECK(observations && classes && rounds);
    for (unsigned r = 0; r < rounds; r++)
        for (unsigned h = 0; h < heads; h++) CHECK(reached[r][h]);
    for (unsigned c = 0; c < classes; c++) {
        CHECK(first[c] <= last[c]); t.pick[first[c]] = t.pick[last[c]] = true;
    }
    unsigned cuts = 0, selected = 0;
    for (unsigned i = 0; i < t.count; i++) {
        if (!t.pick[i]) continue;
        unsigned index = cuts++, c = t.class_id[i]; CHECK(c < classes);
        for (unsigned outcome = 2; outcome <= 3; outcome++) {
            ended_case(heads, observed, NULL, &t.keys[i],
                t.public_ordinal[i], outcome, false);
            selected++;
            printf(
                "%s_cut heads%u observer%d index%u class%u public%u first%d last%d outcome%u passed\n",
                receipt ? "receipt" : "cleared", heads, observed, index, c,
                t.public_ordinal[i],
                i == first[c], i == last[c], outcome);
        }
    }
    CHECK(selected == 2 * cuts);
    printf(
        "%s heads%u observer%d observations%u classes%u cuts%u selected%u rounds%u phases%llu pending%llu flags%llu receipts%llu passed\n",
        receipt ? "receipt" : "cleared", heads, observed, observations, classes,
        cuts, selected, rounds,
        (unsigned long long)phases, (unsigned long long)pending,
        (unsigned long long)flags, (unsigned long long)receipts);
    free(t.pick); free(t.class_id); free(t.public_ordinal); free(t.keys);
}

/* Domain comes from all eligible EVALUATE observations, not accepted families. */
static void
run_partition(unsigned heads, bool observed)
{
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    CHECK(t.keys && t.public_ordinal);
    unsigned retained = 0;
    public_replay(heads, observed, NULL, &t, UINT32_MAX, 0, &retained);
    unsigned counts[4] = {0}, domain = 0, other_phase = 0, rounds = 0;
    bool reached[16][2][4] = {{{false}}};
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i];
        CHECK(k->available && k->eligible);
        if (k->phase != 8) {
            other_phase++; continue;
        }
        domain++;
        CHECK(k->round < 16 && k->head < heads);
        if (k->round + 1 > rounds) rounds = (unsigned)k->round + 1;
        const bool family[] = {endstage_eligible(k), unbound_eligible(k),
                               cleared_eligible(k), receipt_eligible(k)};
        unsigned matches = 0;
        for (unsigned f = 0; f < 4; f++) {
            if (!family[f]) continue;
            matches++; counts[f]++; reached[k->round][k->head][f] = true;
        }
        CHECK(matches == 1);
        if (family[2]) {
            CHECK(k->child[2] == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
                && k->child[3] == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_ACK
                && (k->child[4] & 128));
            for (unsigned j = 5; j <= 9; j++) CHECK(!k->child[j]);
            CHECK(!k->receipt_pending && !k->receipt_ack
                && !k->receipt_acknowledged && !k->receipt_index_valid
                && !k->ack_committed);
        }
        if (family[3]) {
            for (unsigned j = 0; j < 16; j++) CHECK(!k->child[j]);
            CHECK(k->receipt_pending && k->receipt_ack
                && !k->receipt_acknowledged && !k->receipt_index_valid
                && !k->ack_committed && i
                && t.keys[i - 1].round == k->round
                && t.keys[i - 1].head == k->head
                && unbound_eligible(&t.keys[i - 1]));
        }
    }
    CHECK(domain && rounds &&
        domain == counts[0] + counts[1] + counts[2] + counts[3]
        && domain + other_phase == t.count);
    for (unsigned r = 0; r < rounds; r++)
        for (unsigned h = 0; h < heads; h++)
            for (unsigned f = 0; f < 4; f++) CHECK(reached[r][h][f]);
    printf(
        "partition heads%u observer%d domain%u bound_live%u unbound%u cleared%u receipt%u other_phase%u rounds%u excluded%u retirement%u owner%u passed\n",
        heads, observed, domain, counts[0], counts[1], counts[2], counts[3],
        other_phase, rounds, t.excluded, t.excluded_retire, t.excluded_owner);
    free(t.public_ordinal); free(t.keys);
}

/* All recorded phase21 returns are the domain, independently of predicates.
 * Neither marker is possible as a descriptor but unobserved/unqualified here. */
static void
run_owner_audit(unsigned heads, bool observed)
{
    public_trace_t t = {0};
    t.keys = calloc(16384, sizeof(*t.keys));
    t.public_ordinal = calloc(16384, sizeof(*t.public_ordinal));
    CHECK(t.keys && t.public_ordinal);
    ended_case_record(heads, observed, NULL, NULL, UINT32_MAX, 0, false, &t,
        owner_domain);
    unsigned counts[4] = {0}, rounds = 0;
    bool reached[16][2][4] = {{{false}}};
    uint64_t slots[4] = {0}, phases = 0, finance = 0, roles = 0;
    for (unsigned i = 0; i < t.count; i++) {
        const public_key_t *k = &t.keys[i];
        CHECK(owner_domain(k) && k->round < 16 && k->round_index <= heads);
        const bool family[] = {!k->retirement[0] && !k->main[0],
                               k->retirement[0] && owner_unselected(k),
                               !k->retirement[0] && owner_selected(k),
                               k->retirement[0] && owner_selected(k)};
        unsigned matches = 0, bucket = 0;
        for (unsigned f = 0; f < 4; f++) {
            if (!family[f]) continue;
            matches++; counts[f]++; bucket = f;
        }
        CHECK(matches == 1);
        CHECK(!k->child_active && !k->receipt_pending && !k->receipt_ack);
        for (unsigned j = 0; j < 16; j++) CHECK(!k->child[j]);
        if (!bucket) continue;
        CHECK(k->round_index < heads);
        reached[k->round][k->round_index][bucket] = true;
        if (k->round + 1 > rounds) rounds = (unsigned)k->round + 1;
        uint64_t slot = (k->owners[k->round_index] >> 7) & 7;
        CHECK(slot && !(slot & (slot - 1)));
        slots[bucket] |= slot;
        for (unsigned h = 0; h < heads; h++)
            if (h !=
                k->round_index) CHECK(!(k->owners[h] & (UINT64_C(7) << 7)));
        if (k->retirement[0]) {
            CHECK(k->retirement[4] == 2 && k->retirement[5] == k->round_index);
            roles |= UINT64_C(1) << k->retirement[4];
        }
        if (k->main[0]) {
            CHECK(k->main[1] >= 1 && k->main[1] <= 4 && k->main[5] < 64);
            phases |= UINT64_C(1) << k->main[1];
            finance |= UINT64_C(1) << k->main[5];
            CHECK(k->main[3] == (k->main[1] >= 3));
            if (!k->retirement[0])
                CHECK(k->main[3] && !k->main[14]);
        }
    }
    CHECK(t.count && rounds && !counts[0] && counts[1] && counts[2] && counts[3]
        && counts[0] + counts[1] + counts[2] + counts[3] == t.count
        && phases == 30 && roles == 4);
    for (unsigned r = 0; r < rounds; r++)
        for (unsigned h = 0; h < heads; h++)
            for (unsigned f = 1; f < 4; f++) CHECK(reached[r][h][f]);
    printf(
        "owner_audit heads%u observer%d domain%u neither%u fresh%u cleared_main%u live_main%u rounds%u slots%llu/%llu/%llu main_phases%llu finance%llu roles%llu passed\n",
        heads, observed, t.count, counts[0], counts[1], counts[2], counts[3],
        rounds,
        (unsigned long long)slots[1], (unsigned long long)slots[2],
        (unsigned long long)slots[3], (unsigned long long)phases,
        (unsigned long long)finance, (unsigned long long)roles);
    free(t.public_ordinal); free(t.keys);
}

int
main(int argc, char **argv)
{
    CHECK(!environment("WIRELOG_JOIN_BATCH_BYTES", "")
        && !environment("WIRELOG_JOIN_BATCH_STRICT", ""));
    if (argc == 4 && !strcmp(argv[1], "readers-end")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_readers_end(heads, observed != 0);
        return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "observer-audit")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_observer_audit(heads, observed != 0);
        return 0;
    }
    if (argc == 4 && (!strcmp(argv[1], "observer-baseline")
        || !strcmp(argv[1], "observer-events")
        || !strcmp(argv[1], "observer-census"))) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_observer_end(heads, observed != 0,
            !strcmp(argv[1], "observer-events") ? 10u : 9u,
            !strcmp(argv[1], "observer-census"));
        return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "owner-audit")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_owner_audit(heads, observed != 0); return 0;
    }
    if ((argc == 4 || argc == 6) && (!strcmp(argv[1],
        "owner") || !strcmp(argv[1],
        "owner-census") || !strcmp(argv[1], "mainowner")
        || !strcmp(argv[1], "mainowner-census"))) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_owner(heads, observed != 0, strstr(argv[1], "census") != NULL,
            !strncmp(argv[1], "mainowner", 9),
            argc == 6 ? (unsigned)strtoul(argv[4], NULL, 10) : 0,
            argc == 6 ? (unsigned)strtoul(argv[5], NULL, 10) : 1);
        return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "partition")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_partition(heads, observed != 0); return 0;
    }
    if (argc == 4 && (!strcmp(argv[1], "cleared") || !strcmp(argv[1],
        "receipt"))) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_custody(heads, observed != 0, !strcmp(argv[1], "receipt"));
        return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "unbound")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_unbound(heads, observed != 0); return 0;
    }
    if (argc == 6 && !strcmp(argv[1], "endstage")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        unsigned shard = (unsigned)strtoul(argv[4], NULL, 10);
        unsigned shards = (unsigned)strtoul(argv[5], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_endstage(heads, observed != 0, shard, shards); return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "ended")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_ended(heads, observed != 0); return 0;
    }
    if (argc == 5 && !strcmp(argv[1], "retirement")) {
        unsigned mode = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned heads = (unsigned)strtoul(argv[3], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[4], NULL, 10);
        CHECK(mode <= 4 && heads >= 1 && heads <= 2 && observed <= 1);
        run_retirement(mode, heads, observed != 0); return 0;
    }
    if (argc == 7 && !strcmp(argv[1], "public")) {
        unsigned family = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned heads = (unsigned)strtoul(argv[3], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[4], NULL, 10);
        unsigned shard = (unsigned)strtoul(argv[5], NULL, 10);
        unsigned shards = (unsigned)strtoul(argv[6], NULL, 10);
        CHECK(family <= 2 && heads >= 1 && heads <= 2 && observed <= 1);
        run_public(family, heads, observed != 0, shard, shards); return 0;
    }
    if (argc == 7 && !strcmp(argv[1], "lifecycle")) {
        unsigned family = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned heads = (unsigned)strtoul(argv[3], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[4], NULL, 10);
        unsigned shard = (unsigned)strtoul(argv[5], NULL, 10);
        unsigned shards = (unsigned)strtoul(argv[6], NULL, 10);
        CHECK(family <= 4 && heads >= 1 && heads <= 2 && observed <= 1);
        run_lifecycle(family, heads, observed != 0, shard, shards); return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "refusals")) {
        run_refusals(); return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "failures")) {
        run_failures(); return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "rows")) {
        run_rows(); return 0;
    }
    if (argc == 4 && !strcmp(argv[1], "stops")) {
        unsigned heads = (unsigned)strtoul(argv[2], NULL, 10);
        unsigned observed = (unsigned)strtoul(argv[3], NULL, 10);
        CHECK(heads >= 1 && heads <= 2 && observed <= 1);
        run_stops(heads, observed != 0); return 0;
    }
    return 2;
}
