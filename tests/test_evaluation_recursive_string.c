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

int
main(int argc, char **argv)
{
    CHECK(!environment("WIRELOG_JOIN_BATCH_BYTES", "")
        && !environment("WIRELOG_JOIN_BATCH_STRICT", ""));
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
