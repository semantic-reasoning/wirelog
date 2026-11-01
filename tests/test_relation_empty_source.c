/* Genuine no-insert EDB authority: both gates, fixed metadata and funded END. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static unsigned stop_point;
static bool stop_work, stopped;

static void
checkpoint(const wl_columnar_relation_empty_source_t *a)
{
    unsigned point = a->phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_ACQUIRE
        ? a->reader.acquire_stage == 1 ? 1 : 0
        : a->phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_VALIDATE ? 2 : 3;
    if (!stop_point || stopped || stop_point != point) return;
    stopped = true;
    if (stop_work) CHECK(wl_evaluation_control_charge(a->control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(a->control);
}

static void
drain(wl_columnar_relation_empty_source_t *a)
{
    int owner; wl_evaluation_control_t *funding = NULL;
    CHECK(!wl_evaluation_control_create(384, &funding));
    CHECK(!wl_evaluation_control_attach(funding, &owner));
    bool done = false;
    for (unsigned turn = 0; !done; ++turn) {
        CHECK(turn < 100 && !wl_evaluation_control_reset(funding));
        CHECK(!wl_evaluation_control_begin(funding, &owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        wl_columnar_relation_empty_source_t copied = *a;
        CHECK(wl_columnar_relation_empty_source_end(&copied, funding, id,
            &done) == EINVAL && !done);
        CHECK(wl_columnar_relation_empty_source_end(a, funding, id + 1,
            &done) == EINVAL && !done);
        CHECK(!wl_columnar_relation_empty_source_end(a, funding, id, &done));
        CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
    }
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    CHECK(a->phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED
        && !a->reader.owner && !a->reader.secondary_owner && !a->control
        && !a->descriptor);
    done = false;
    CHECK(!wl_columnar_relation_empty_source_end(a, NULL, 0, &done) && done);
}

typedef struct {
    wl_columnar_relation_empty_source_t *source;
    wl_evaluation_control_t *funding;
    uint64_t id;
    int step_result, end_result;
} thread_case_t;

#if defined(WL_HAVE_C11_THREADS)
static int
wrong_thread(void *arg)
#elif defined(_WIN32) || defined(_WIN64)
static DWORD WINAPI
wrong_thread(LPVOID arg)
#else
static void *
wrong_thread(void *arg)
#endif
{
    thread_case_t *c = arg; bool done = true;
    c->step_result = wl_columnar_relation_empty_source_step(c->source, &done);
    CHECK(!done);
    c->end_result = wl_columnar_relation_empty_source_end(c->source,
            c->funding, c->id, &done);
    CHECK(!done);
#if defined(WL_HAVE_C11_THREADS) || defined(_WIN32) || defined(_WIN64)
    return 0;
#else
    return NULL;
#endif
}

static void
check_thread(wl_columnar_relation_empty_source_t *a)
{
    int owner; wl_evaluation_control_t *funding = NULL;
    CHECK(!wl_evaluation_control_create(384, &funding)
        && !wl_evaluation_control_attach(funding, &owner)
        && !wl_evaluation_control_begin(funding, &owner));
    thread_case_t c = {.source = a, .funding = funding,
                       .id = wl_evaluation_control_active_attempt_id(funding)};
#if defined(WL_HAVE_C11_THREADS)
    thrd_t thread;
    CHECK(thrd_create(&thread, wrong_thread, &c) == thrd_success);
    CHECK(thrd_join(thread, NULL) == thrd_success);
#elif defined(_WIN32) || defined(_WIN64)
    HANDLE thread = CreateThread(NULL, 0, wrong_thread, &c, 0, NULL);
    CHECK(thread && WaitForSingleObject(thread, INFINITE) == WAIT_OBJECT_0);
    CHECK(CloseHandle(thread));
#else
    pthread_t thread;
    CHECK(!pthread_create(&thread, NULL, wrong_thread, &c));
    CHECK(!pthread_join(thread, NULL));
#endif
    CHECK(c.step_result == EINVAL && c.end_result == EINVAL);
    CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(funding, &report) && !report.consumed);
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
}

static void
run(unsigned point, bool work, unsigned invalid)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1u << 20, .usable_bytes = 1u << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    col_rel_t *r = NULL;
    CHECK(!wl_columnar_relation_alloc_governed(&r, "empty", governor));
    CHECK(r && !r->schema_ok && !r->nrows && !r->ncols && !r->capacity
        && r->storage_owner == r &&
        r->storage_owner_identity == r->relation_identity
        && r->storage_owner_generation == r->storage_generation);
    r->declared_ncols = 3; r->has_graph_column = true; r->graph_col_idx = 2;
    col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(r);
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
    CHECK(baseline == r->descriptor_reserved_bytes);
    int owner; wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(work ? UINT64_C(1000000000) : 0,
        &control));
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    int64_t *forged_columns[] = {NULL};
    if (invalid == 1) r->columns = forged_columns;
    if (invalid == 2) r->capacity = 1;
    if (invalid == 3) r->storage_owner = NULL;
    if (invalid == 4) r->retained_reserved_bytes = 1;
    if (invalid == 5) r->schema_ok = true;
    if (invalid == 6) r->retract_backup_run_ends[COL_MAX_RUNS - 1] = 1;
    if (invalid == 7) r->schema.format = "l";
    if (invalid == 8) atomic_store_explicit(&r->storage_alias_borrows, 1,
            memory_order_release);
    wl_columnar_relation_empty_source_t a = {0};
    CHECK(!wl_columnar_relation_empty_source_begin(&a, r, control, id));
    wl_columnar_relation_empty_source_t copied = a;
    bool done = false;
    CHECK(wl_columnar_relation_empty_source_step(&copied, &done) == EINVAL);
    stop_point = point; stop_work = work; stopped = false;
    wl_columnar_relation_test_empty_source_checkpoint = checkpoint;
    int rc = 0;
    for (unsigned turn = 0; !done && !rc; ++turn) {
        CHECK(turn < 100);
        rc = wl_columnar_relation_empty_source_step(&a, &done);
    }
    if (point == 4 && !rc) {
        CHECK(done && !wl_evaluation_control_finish(control, &owner, 0, 0));
        CHECK(!wl_evaluation_control_begin(control, &owner));
        CHECK(wl_evaluation_control_active_attempt_id(control) > id);
        CHECK(wl_columnar_relation_empty_source_step(&a,
            &done) == EINVAL && !done);
        CHECK(a.result == EINVAL);
    }
    if (point == 3 && !rc) {
        CHECK(done && a.phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_HELD);
        rc = wl_columnar_relation_empty_source_step(&a, &done);
    }
    wl_columnar_relation_test_empty_source_checkpoint = NULL;
    if (point &&
        point < 4) CHECK(stopped &&
            rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : WL_EVALUATION_CONTROL_CANCELLED));
    else if (point == 4) CHECK(!rc && !stopped);
    else if (invalid) CHECK(rc == (invalid == 5 ? ENOTSUP : EINVAL));
    else {
        CHECK(!rc && done && a.phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_HELD);
        CHECK(a.snapshot.relation_identity == snapshot.relation_identity
            && a.declared_ncols == 3 && a.has_graph_column &&
            a.graph_col_idx == 2);
        CHECK(a.reader.owner == &r->source_access
            && a.reader.secondary_owner == &r->descriptor_access);
        check_thread(&a);
        const int64_t values[] = {1, 2, 3};
        CHECK(col_rel_append_row(r, values) == EBUSY);
        CHECK(col_rel_set_schema(r, 3, NULL) == EBUSY);
        CHECK(col_rel_destroy_checked(r) == EBUSY);
        CHECK(!r->schema_ok && !r->nrows && !r->ncols);
        CHECK(wl_columnar_relation_empty_source_begin(&a, r, control,
            id) == EBUSY);
    }
    CHECK(wl_evaluation_control_finish(control, &owner, rc, 0) == rc);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(control, &before));
    CHECK(wl_columnar_relation_empty_source_end(&a, control, id,
        &done) == EINVAL);
    drain(&a);
    CHECK(!wl_evaluation_control_report(control, &after)
        && before.consumed == after.consumed && before.result == after.result
        && before.attempt_id == after.attempt_id &&
        before.stop_reason == after.stop_reason);
    CHECK(wl_columnar_relation_snapshot_equal(snapshot,
        wl_columnar_relation_snapshot(r))
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == baseline);
    r->columns = NULL; r->capacity = 0; r->storage_owner = r;
    r->retained_reserved_bytes = 0; r->schema_ok = false;
    r->retract_backup_run_ends[COL_MAX_RUNS - 1] = 0;
    r->schema.format = NULL;
    atomic_store_explicit(&r->storage_alias_borrows, 0, memory_order_release);
    const int64_t values[] = {1, 2, 3};
    CHECK(!col_rel_set_schema(r, 3, NULL) && !col_rel_append_row(r, values));
    CHECK(r->schema_ok && r->ncols == 3 && r->nrows == 1
        && r->columns[0][0] == 1 && r->columns[2][0] == 3);
    CHECK(!wl_evaluation_control_reset(control) &&
        !wl_evaluation_control_begin(control, &owner));
    id = wl_evaluation_control_active_attempt_id(control);
    wl_columnar_relation_scalar_source_t scalar = {0}; bool initialized = false;
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(&scalar,
        &initialized, control, id) && initialized);
    CHECK(!wl_columnar_relation_scalar_source_begin_attempt_controlled(&scalar,
        r, false));
    CHECK(scalar.width == 3 && scalar.nrows == 1);
    CHECK(!wl_columnar_relation_scalar_source_end(&scalar, 0));
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    CHECK(!col_rel_destroy_checked(r));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
first_admission(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1u << 20, .usable_bytes = 1u << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    col_rel_t *r = NULL;
    CHECK(!wl_columnar_relation_alloc_governed(&r, "empty", governor));
    int owner; wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(1, &control)
        && !wl_evaluation_control_attach(control, &owner)
        && !wl_evaluation_control_begin(control, &owner));
    wl_columnar_relation_empty_source_t a = {0};
    CHECK(wl_columnar_relation_empty_source_begin(&a, r, control,
        wl_evaluation_control_active_attempt_id(control)) ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!a.identity && !a.reader.owner && !a.control && !a.descriptor);
    CHECK(!col_rel_destroy_checked(r));
    CHECK(wl_evaluation_control_finish(control, &owner,
        WL_EVALUATION_CONTROL_WORK_LIMIT,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
natural_constructors(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl integers(x:int64)\n.decl floats(x:float)\n.decl strings(x:string)\n"
        ".decl out(x:int64)\nout(x):-integers(x).\n", &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1u << 20, .usable_bytes = 1u << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = governor;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    wl_col_session_t *s = (wl_col_session_t *)session;
    col_rel_t *integers = session_find_rel(s, "integers");
    col_rel_t *floats = session_find_rel(s, "floats");
    col_rel_t *strings = session_find_rel(s, "strings");
    CHECK(integers && !integers->schema_ok && !integers->ncols &&
        !integers->nrows
        && integers->storage_owner == integers &&
        integers->declared_ncols == 1);
    CHECK(floats && floats->schema_ok && floats->ncols == 1
        && !floats->nrows && floats->column_types[0] == WIRELOG_TYPE_FLOAT);
    CHECK(strings && strings->schema_ok && strings->ncols == 1
        && !strings->nrows && strings->column_types[0] == WIRELOG_TYPE_STRING);
    int owner; wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0,
        &control) && !wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    col_rel_t *sources[] = {integers, floats, strings};
    for (unsigned i = 0; i < 3; ++i) {
        wl_columnar_relation_empty_source_t a = {0};
        CHECK(!wl_columnar_relation_empty_source_begin(&a, sources[i], control,
            id));
        bool done = false; int rc = 0;
        while (!done && !rc) rc = wl_columnar_relation_empty_source_step(&a,
                    &done);
        CHECK(i ? rc == ENOTSUP : !rc && done);
        drain(&a);
    }
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    CHECK(!wl_session_try_destroy(&session));
    wl_plan_free(plan); wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

int
main(void)
{
    run(0, false, 0);
    for (unsigned point = 1; point <= 3; ++point)
        for (unsigned work = 0; work < 2; ++work) run(point, work, 0);
    for (unsigned invalid = 1; invalid <= 8; ++invalid) run(0, false, invalid);
    run(4, false, 0);
    first_admission();
    natural_constructors();
    return 0;
}
