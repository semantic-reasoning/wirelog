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

int
main(int argc, char **argv)
{
    CHECK(!environment("WIRELOG_JOIN_BATCH_BYTES", "")
        && !environment("WIRELOG_JOIN_BATCH_STRICT", ""));
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
