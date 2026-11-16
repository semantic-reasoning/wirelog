/* Actual canonical inline-seed host integration, including its owning tails. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/session_seed.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog-extension.h"
#include "../wirelog/wirelog.h"
#include <limits.h>
#include <stdio.h>
#if !defined(_WIN32)
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <stdlib.h>
#include <string.h>
#define CHECK(x)                                                               \
        do {                                                                         \
            if (!(x)) {                                                                \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
                abort();                                                                 \
            }                                                                          \
        } while (0)
extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
extern int wl_columnar_session_test_seed_host_view(const wl_col_session_t *,
    uint64_t[32],
    wl_evaluation_control_t **);
extern void (*wl_columnar_session_test_seed_parent_checkpoint)(
    wl_evaluation_control_t *, uint64_t, unsigned, bool);
extern int
wl_columnar_session_test_seed_raw_header_view(const wl_col_session_t *,
    uint64_t[8]);
extern int
wl_columnar_session_test_seed_native_terminal_resolve(wl_col_session_t *,
    uint64_t);
extern int
wl_columnar_session_test_seed_maintenance_view(const wl_col_session_t *,
    uint64_t[8]);
extern int
wl_columnar_session_seed_test_reader_view(const wl_columnar_session_seed_t *,
    uint64_t[8]);
extern void (*wl_columnar_memory_governor_test_checkpoint)(
    wl_evaluation_control_t *, wl_columnar_memory_reservation_t *, unsigned);
extern void (*wl_columnar_memory_governor_test_work_checkpoint)(
    wl_evaluation_control_t *, uint64_t);
extern int wl_evaluation_control_test_active_consumed(wl_evaluation_control_t *,
    uint64_t, uint64_t *);
typedef struct {
    const char *source;
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *raw, *session;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_columnar_memory_governor_ref_t *actual_governors[2];
    unsigned events, positive, negative, effects;
    unsigned phase, mode, hits, init_word;
    bool injected, published, denied_witness, end_on_event;
    unsigned denied_site;
    uint64_t saved_usable, reserved_floor, denial_before;
    uint64_t successor;
    wl_evaluation_control_report_t frozen;
    uint64_t image[32];
    bool ordinary, stop_in_effect, parent_armed, parent_checked;
    unsigned parent_kind;
    bool arena_armed, arena_tail;
    const wl_columnar_memory_reservation_t *arena_token;
    uint64_t arena_before_work, arena_before_reserved;
    unsigned name_cuts, name_expected;
    uint64_t name_base;
    bool name_finished;
} fixture_t;
static fixture_t *active;
static wirelog_extension_snapshot_t *test_snapshot;
static bool load_facts = true;
static void end_original(fixture_t *f, bool successor) {
    uint64_t id = wl_evaluation_control_active_attempt_id(f->control);
    CHECK(id && !f->injected);
    wl_evaluation_control_attempt_finish_result_t ended =
        wl_evaluation_control_finish_attempt(
        f->control, f->session->operation_admission, id, 0, 0);
    CHECK(!ended.protocol && ended.report_valid);
    f->frozen = ended.report;
    f->injected = true;
    if (successor) {
        wl_evaluation_control_attempt_begin_result_t next =
            wl_evaluation_control_begin_attempt(f->control,
                f->session->operation_admission);
        CHECK(!next.protocol && next.attempt_id != id);
        f->successor = next.attempt_id;
    }
}
static void event(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data) {
    fixture_t *f = data;
    CHECK(!strcmp(name,
        "out") && width == 1 && row && (diff == 1 || diff == -1));
    f->events++;
    f->positive += diff == 1;
    f->negative += diff == -1;
    uint64_t view[32];
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        view,
        NULL));
    CHECK(view[13] && view[14]);
    if (!f->ordinary)
        CHECK(view[1] && view[15]);
    if (!f->ordinary)
        CHECK(!view[21]); /* Public delivery precedes paid RETIRE rebind. */
    int64_t value = 99;
    CHECK(wl_session_insert(f->session, "input", &value, 1, 1) == EBUSY);
    f->published = true;
    if (f->end_on_event && !f->injected)
        end_original(f, f->mode == 3);
}
static fixture_t create(const char *source, bool observed,
    bool injected_governor) {
    fixture_t f = {0};
    f.source = source;
    wirelog_error_t error;
    f.program = wirelog_parse_string(source, &error);
    CHECK(f.program &&
        !wl_plan_from_program_with_snapshot(f.program, test_snapshot, &f.plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor && !wl_evaluation_control_create(0, &f.control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = injected_governor ? f.governor : NULL;
    CHECK(!wl_session_create_with_snapshot_options(
            wl_backend_columnar(), f.plan, 1, test_snapshot, &options, &f.raw));
    options.evaluation_control = f.control;
    CHECK(!wl_session_create_with_snapshot_options(
            wl_backend_columnar(), f.plan, 1, test_snapshot, &options,
            &f.session));
    f.actual_governors[0] = COL_SESSION(f.session)->memory_governor;
    f.actual_governors[1] = COL_SESSION(f.raw)->memory_governor;
    CHECK(f.actual_governors[0] && f.actual_governors[1]);
    wl_columnar_memory_governor_ref_retain(f.actual_governors[0]);
    wl_columnar_memory_governor_ref_retain(f.actual_governors[1]);
    if (load_facts)
        CHECK(!wl_session_load_facts(f.raw, f.program) &&
            !wl_session_load_facts(f.session, f.program));
    if (observed)
        wl_session_set_delta_cb(f.session, event, NULL);
    return f;
}
static void fixture_hook(fixture_t *f, bool observed) {
    active = f;
    if (observed)
        wl_session_set_delta_cb(f->session, event, f);
}
static int complete(fixture_t *f) {
    for (unsigned i = 0; i < 100000; ++i) {
        int rc = wl_session_step(f->session);
        if (f->parent_kind == 83 && f->injected && !f->parent_checked) {
            uint64_t view[32];
            CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(
                    f->session),
                view, NULL));
            CHECK(view[0] == (f->mode == 5 ? 84u : 108u) && !view[3] &&
                !view[28] &&
                view[11]);
            f->parent_checked = true;
        }
        if (rc != EAGAIN)
            return rc;
    }
    fprintf(stderr, "stalled phase%u mode%u injected%d hits%u\n", f->phase,
        f->mode, f->injected, f->hits);
    uint64_t view[32];
    if (!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session), view,
        NULL))
        for (unsigned j = 0; j < 32; j++)
            fprintf(stderr, "v%u=%llu ", j, (unsigned long long)view[j]);
    fputc('\n', stderr);
    abort();
}
static void equal(fixture_t *f) {
    col_rel_t *a = session_find_rel(COL_SESSION(f->session), "out");
    col_rel_t *b = session_find_rel(COL_SESSION(f->raw), "out");
    CHECK(a && b && a->ncols == b->ncols && a->nrows == b->nrows);
    if (!f->ordinary && (a->capacity != b->capacity ||
        a->timestamp_capacity != b->timestamp_capacity))
        fprintf(stderr, "raw geometry capacity%u/%u TS%u/%u source%s\n",
            a->capacity, b->capacity, a->timestamp_capacity,
            b->timestamp_capacity, f->source);
    if (!f->ordinary)
        CHECK(a->capacity == b->capacity &&
            a->timestamp_capacity == b->timestamp_capacity);
    if (!f->ordinary) {
        CHECK(!!a->column_types == !!b->column_types);
        CHECK(!!a->row_scratch == !!b->row_scratch);
    }
    CHECK(a->schema_ok == b->schema_ok &&
        a->has_graph_column == b->has_graph_column);
    CHECK(a->declared_ncols == b->declared_ncols);
    CHECK(!!a->schema.format == !!b->schema.format);
    if (a->schema.format)
        CHECK(!strcmp(a->schema.format, b->schema.format));
    CHECK(a->schema.n_children == b->schema.n_children);
    CHECK(a->schema.flags == b->schema.flags);
    CHECK(!!a->schema.name == !!b->schema.name);
    if (a->schema.name)
        CHECK(!strcmp(a->schema.name, b->schema.name));
    CHECK(!!a->schema.metadata == !!b->schema.metadata);
    CHECK(!!a->schema.dictionary == !!b->schema.dictionary);
    for (int64_t c = 0; c < a->schema.n_children; ++c) {
        const struct ArrowSchema *x = a->schema.children[c],
            *y = b->schema.children[c];
        CHECK(!!x == !!y);
        if (x) {
            CHECK(!!x->format == !!y->format);
            if (x->format)
                CHECK(!strcmp(x->format, y->format));
            CHECK(x->flags == y->flags && !!x->name == !!y->name);
            if (x->name)
                CHECK(!strcmp(x->name, y->name));
            CHECK(!!x->metadata == !!y->metadata &&
                !!x->dictionary == !!y->dictionary);
        }
    }
    for (uint32_t c = 0; c < a->ncols; ++c) {
        CHECK((a->column_types ? a->column_types[c] : WIRELOG_TYPE_INT64) ==
            (b->column_types ? b->column_types[c] : WIRELOG_TYPE_INT64));
        CHECK(!memcmp(a->columns[c], b->columns[c],
            a->nrows * sizeof(int64_t)));
    }
    CHECK(!!a->timestamps == !!b->timestamps);
    if (a->timestamps)
        CHECK(!memcmp(a->timestamps, b->timestamps,
            a->nrows * sizeof(*a->timestamps)));
}
static void destroy(fixture_t *f) {
    active = NULL;
    wl_columnar_session_test_observed_transaction_checkpoint = NULL;
    wl_columnar_session_seed_test_checkpoint = NULL;
    wl_columnar_session_test_seed_parent_checkpoint = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_session_destroy(f->session);
    wl_session_destroy(f->raw);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    wl_evaluation_control_release(f->control);
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(f->actual_governors[i])));
        wl_columnar_memory_governor_ref_release(f->actual_governors[i]);
    }
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
}
static const char *numeric =
    ".decl input(x:int64)\n.decl out(y:int64)\n"
    "input(1). out(7). out(strlen(to_string(x))):-input(x).";
static void checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)index;
    (void)bindings;
    fixture_t *f = active;
    if (!f || f->injected || phase != f->phase)
        return;
    CHECK(control == f->control);
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        f->image, NULL));
    f->hits++;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(id);
    f->injected = true;
    if (f->mode == 0)
        wl_evaluation_control_request_cancel(control);
    else if (f->mode == 1) {
        CHECK(wl_evaluation_control_charge_attempt(control, id, UINT64_MAX) ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
    } else if (f->mode == 4 || f->mode == 5) {
        uint64_t used;
        CHECK(!wl_evaluation_control_test_active_consumed(control, id, &used));
        CHECK(used < UINT64_MAX - 255);
        CHECK(!wl_evaluation_control_charge_attempt(
                control, id, UINT64_MAX - (f->mode == 4 ? 255 : 256) - used));
    } else {
        wl_evaluation_control_attempt_finish_result_t ended =
            wl_evaluation_control_finish_attempt(
            control, f->session->operation_admission, id, 0, 0);
        CHECK(!ended.protocol && ended.report_valid);
        f->frozen = ended.report;
        if (f->mode == 3) {
            wl_evaluation_control_attempt_begin_result_t next =
                wl_evaluation_control_begin_attempt(control,
                    f->session->operation_admission);
            CHECK(!next.protocol && next.attempt_id != id);
            f->successor = next.attempt_id;
        }
    }
}
static bool report_equal(const wl_evaluation_control_report_t *a,
    const wl_evaluation_control_report_t *b) {
    return a->attempt_id == b->attempt_id && a->limit == b->limit &&
           a->consumed == b->consumed && a->result == b->result &&
           a->stop_reason == b->stop_reason &&
           a->execution_status == b->execution_status &&
           a->cleanup_status == b->cleanup_status;
}
static void positives(void) {
    const char *sources[] = {
        numeric,
        ".decl out(y:int64)\n.decl input(x:int64)\n"
        "out(7). input(1). out(strlen(to_string(x))):-input(x).",
        ".decl input(x:string)\n.decl out(y:string)\n"
        "input(\"a\"). out(\"z\"). out(cat(x,\"!\")):-input(x).",
        ".decl input(x:float)\n.decl out(y:float)\n"
        "input(1.5). out(7.5). out(x):-input(x).",
        "input(1). out(7). out(strlen(to_string(x))):-input(x).",
        ".decl input(x:int64)\n.decl out(y:int64)\n"
        "input(1). out(7). out(strlen(to_string(x))):-input(x),x>9.",
        ".decl input(x:int64)\n.decl out(y:int64)\n"
        "input(1). out(7). out(strlen(to_string(x))):-input(x),x>0.",
        ".decl input(x:int64)\n.decl out(y:int64)\n"
        "out(7). out(strlen(to_string(x))):-input(x)."
    };
    for (unsigned source = 0; source < sizeof(sources) / sizeof(*sources);
        ++source)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (unsigned injected_governor = 0; injected_governor < 2;
                ++injected_governor) {
                fixture_t f = create(sources[source], observed,
                        injected_governor);
                fixture_hook(&f, observed);
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                unsigned expected = source == 5 || source == 7 ? 1 : 2;
                CHECK(!observed || (f.events == expected && f.published));
                CHECK(observed || !f.events);
                col_rel_t *out = session_find_rel(COL_SESSION(f.session),
                        "out");
                CHECK(out->nrows == expected);
                /* A second actual attempt disposes the retained transaction
                 * without retiring the published descriptor. */
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                CHECK(session_find_rel(COL_SESSION(f.session), "out"));
                if (!source) {
                    int64_t value = 12;
                    CHECK(!wl_session_insert(f.session, "input", &value, 1,
                        1) &&
                        !wl_session_insert(f.raw, "input", &value, 1, 1));
                    CHECK(!wl_evaluation_control_reset(f.control));
                    CHECK(!complete(&f) && !wl_session_step(f.raw));
                    equal(&f);
                }
                destroy(&f);
            }
}
static void stops(void) {
    /* Actual parent phases: before arena, failed Init, prepared actor,
     * real rule, after Take, closed actor, and reversible publication claims. */
    const unsigned phases[] = {64, 65, 66, 80,  83,  86,  88, 90,
                               92, 95, 99, 100, 101, 103, 106};
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned phase = 0; phase < sizeof(phases) / sizeof(*phases);
            ++phase)
            for (unsigned mode = 0; mode < 6; ++mode) {
                if (mode >= 4 && phases[phase] > 66)
                    continue;
                fixture_t f = create(numeric, observed, true);
                fixture_hook(&f, observed);
                f.phase = phases[phase];
                f.mode = mode;
                if (mode == 1 || mode >= 4)
                    CHECK(!wl_evaluation_control_configure(f.control,
                        UINT64_MAX));
                col_rel_t *old = session_find_rel(COL_SESSION(f.session),
                        "out");
                col_relation_snapshot_t old_image =
                    wl_columnar_relation_snapshot(old);
                wl_columnar_session_test_observed_transaction_checkpoint =
                    checkpoint;
                int rc = complete(&f);
                if (!f.injected ||
                    rc !=
                    (mode == 0                ? WL_EVALUATION_CONTROL_CANCELLED
                   : mode == 1 || mode >= 4 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                                            : EINVAL))
                    fprintf(stderr,
                        "stop phase%u mode%u observed%u rc%d injected%d\n",
                        f.phase, mode, observed, rc, f.injected);
                CHECK(f.injected && f.hits == 1);
                CHECK(rc ==
                    (mode == 0                ? WL_EVALUATION_CONTROL_CANCELLED
                     : mode == 1 || mode >= 4 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                                              : EINVAL));
                uint64_t closed[32];
                CHECK(wl_columnar_session_test_seed_host_view(COL_SESSION(
                        f.session),
                    closed, NULL) == ENOENT);
                CHECK(!f.events &&
                    session_find_rel(COL_SESSION(f.session), "out") == old);
                CHECK(wl_columnar_relation_snapshot_equal(
                        old_image, wl_columnar_relation_snapshot(old)));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report));
                if (mode >= 4)
                    CHECK(report.consumed ==
                        UINT64_MAX - (mode == 4 ? 255 : 0));
                if (mode == 2 || mode == 3)
                    CHECK(report_equal(&f.frozen, &report));
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.control, f.successor, &used) &&
                        !used);
                    wl_evaluation_control_attempt_finish_result_t ended =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.session->operation_admission, f.successor,
                        0, 0);
                    CHECK(!ended.protocol && ended.report_valid &&
                        !ended.report.consumed);
                }

                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control, 0));
                int retry = complete(&f);
                if (retry)
                    fprintf(stderr, "retry rc%d\n", retry);
                CHECK(!retry && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
}
static void child_error(void) {
    const char *source =
        ".decl input(x:string)\n.decl out(y:int64)\n"
        "input(\"9223372036854775808\"). out(7). out(to_number(x)):-input(x).";
    for (unsigned observed = 0; observed < 2; ++observed) {
        fixture_t f = create(source, observed, true);
        fixture_hook(&f, observed);
        col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
        col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(old);
        int actual = complete(&f), raw = wl_session_step(f.raw);
        if (actual != raw)
            fprintf(stderr, "child error actual%d raw%d\n", actual, raw);
        CHECK(actual == ERANGE && actual == raw && !f.events);
        CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
            wl_columnar_relation_snapshot_equal(
                snapshot, wl_columnar_relation_snapshot(old)));
        uint64_t view[32];
        CHECK(wl_columnar_session_test_seed_host_view(COL_SESSION(f.session),
            view,
            NULL) == ENOENT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report) &&
            report.result == ERANGE && !report.cleanup_status);
        destroy(&f);
    }
}
static void actor_denial(const wl_columnar_session_seed_t *actor,
    unsigned phase, unsigned stage) {
    fixture_t *f = active;
    if (!f)
        return;
    if (f->injected) {
        if (phase == 1503 && !f->denied_witness) {
            uint64_t admission[8];
            CHECK(!wl_columnar_session_seed_test_admission(actor,
                f->denied_site,
                admission));
            CHECK(admission[0] == WL_COLUMNAR_MEMORY_ADMISSION_DENIED &&
                admission[1]);
            f->denied_witness = true;
        }
        return;
    }
    if (phase != 1402 && phase != 1101)
        return;
    uint64_t view[16];
    CHECK(!wl_columnar_session_seed_test_view(actor, view));
    bool selected = f->phase == 0 ? phase == 1402 && view[1] == 4 && stage == 2
                                : phase == 1101 && view[1] == 7 && view[4] == 5;
    if (!selected)
        return;
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        f->image, NULL));
    if (!f->phase)
        CHECK(!f->image[6] && !f->image[26] && !f->effects);
    else {
        const col_rel_t *rule = (const col_rel_t *)(uintptr_t)f->image[6];
        CHECK(rule && rule->nrows == 70 && f->effects == 70 &&
            f->image[26] == f->image[27] && f->image[26] && !f->image[23] &&
            !f->image[24] && !f->image[25]);
    }
    f->denied_site = f->phase == 0 ? WL_COLUMNAR_SESSION_SEED_TEST_RESTORE
                                 : WL_COLUMNAR_SESSION_SEED_TEST_RESIZE;
    if (f->phase)
        CHECK(view[12]);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(f->governor);
    f->saved_usable =
        atomic_load_explicit(&g->usable_bytes, memory_order_acquire);
    f->reserved_floor = wl_columnar_memory_reserved(g);
    f->denial_before = wl_columnar_memory_denials(g);
    CHECK(f->reserved_floor < f->saved_usable);
    atomic_store_explicit(&g->usable_bytes, f->reserved_floor,
        memory_order_release);
    f->injected = true;
}
static int effect(const wirelog_extension_value_t *args, uint32_t count,
    wirelog_extension_value_t *out, void *data) {
    (void)data;
    CHECK(active && count == 1 &&
        args[0].type == WIRELOG_EXTENSION_VALUE_INT64);
    active->effects++;
    if (active->stop_in_effect && !active->injected && active->effects == 2) {
        CHECK(COL_SESSION(active->session)->cleanup_active);
        if (active->mode == 4) {
            CHECK(!wl_columnar_session_test_seed_host_view(
                    COL_SESSION(active->session), active->image, NULL));
            active->injected = true;
            return -1;
        }
        checkpoint(active->control, active->phase, 0, NULL);
    }
    out->type = WIRELOG_EXTENSION_VALUE_INT64;
    out->size = sizeof(int64_t);
    out->as.int64_value = args[0].as.int64_value;
    return 0;
}
static void governor_denials(void) {
    const char *source =
        ".decl input(x:int64)\n.decl out(y:int64)\n"
        "input(1). out(700). out(@call(\"seed.host\",x)):-input(x).";
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create();
    CHECK(registry);
    uint32_t argument = WIRELOG_EXTENSION_VALUE_INT64;
    wirelog_extension_descriptor_t descriptor = {
        .abi_version = WIRELOG_EXTENSION_ABI_VERSION,
        .size = sizeof(descriptor),
        .name = "seed.host",
        .arity = 1,
        .argument_types = &argument,
        .result_type = WIRELOG_EXTENSION_VALUE_INT64,
        .invoke = effect
    };
    CHECK(!wirelog_extension_register(registry, &descriptor));
    test_snapshot = wirelog_extension_snapshot_acquire(registry);
    CHECK(test_snapshot);
    for (unsigned phase = 0; phase < 2; ++phase) {
        fixture_t f = create(source, true, true);
        fixture_hook(&f, true);
        f.phase = phase;
        for (int64_t value = 2; value <= 70; ++value) {
            CHECK(!wl_session_insert(f.session, "input", &value, 1, 1) &&
                !wl_session_insert(f.raw, "input", &value, 1, 1));
        }
        col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
        col_relation_snapshot_t snapshot = wl_columnar_relation_snapshot(old);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(f.governor);
        uint64_t before_reserved = wl_columnar_memory_reserved(g);
        col_rel_t *peer = session_find_rel(COL_SESSION(f.raw), "out");
        col_relation_snapshot_t peer_snapshot =
            wl_columnar_relation_snapshot(peer);
        wl_columnar_session_seed_test_checkpoint = actor_denial;
        int rc = complete(&f);
        if (rc != ENOSPC || !f.injected || !f.denied_witness)
            fprintf(stderr, "gov phase%u rc%d injected%d witness%d\n", phase,
                rc,
                f.injected, f.denied_witness);
        CHECK(rc == ENOSPC && f.injected && f.denied_witness && !f.events);
        CHECK(f.effects == (phase ? 70u : 0u));
        CHECK(wl_columnar_memory_reserved(g) == before_reserved);
        CHECK(wl_columnar_relation_snapshot_equal(
                peer_snapshot, wl_columnar_relation_snapshot(peer)) &&
            peer->nrows == 1 && peer->columns[0][0] == 700);
        CHECK(wl_columnar_memory_denials(g) > f.denial_before);
        CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
            wl_columnar_relation_snapshot_equal(
                snapshot, wl_columnar_relation_snapshot(old)));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report) &&
            report.result == ENOSPC && report.execution_status == ENOSPC &&
            report.stop_reason == WL_EVALUATION_CONTROL_NONE &&
            !report.cleanup_status);
        atomic_store_explicit(&g->usable_bytes, f.saved_usable,
            memory_order_release);
        wl_columnar_session_seed_test_checkpoint = NULL;
        CHECK(!wl_evaluation_control_reset(f.control));
        unsigned before = f.effects;
        CHECK(!complete(&f) && f.effects == before + 70);
        CHECK(!wl_session_step(f.raw) && f.effects == before + 140);
        equal(&f);
        destroy(&f);
    }
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned mode = 0; mode < 5; ++mode) {
            fixture_t f = create(source, observed, true);
            fixture_hook(&f, observed);
            f.stop_in_effect = true;
            f.phase = 4242;
            f.mode = mode;
            if (mode == 1)
                CHECK(!wl_evaluation_control_configure(f.control, UINT64_MAX));
            for (int64_t value = 2; value <= 3; ++value)
                CHECK(!wl_session_insert(f.session, "input", &value, 1, 1) &&
                    !wl_session_insert(f.raw, "input", &value, 1, 1));
            col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t image = wl_columnar_relation_snapshot(old);
            int rc = complete(&f);
            int expected = mode == 0   ? WL_EVALUATION_CONTROL_CANCELLED
                     : mode == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                     : mode == 4 ? WL_COLUMNAR_EXPR_CALLBACK_FAILURE
                                 : EINVAL;
            if (rc != expected || f.effects != 2)
                fprintf(
                    stderr,
                    "callback stop mode%u observed%u actual%d expected%d effects%u\n",
                    mode, observed, rc, expected, f.effects);
            CHECK(rc == expected && f.injected && f.effects == 2 &&
                f.image[23] &&
                !f.events);
            CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
                wl_columnar_relation_snapshot_equal(
                    image, wl_columnar_relation_snapshot(old)));
            if (mode == 2 || mode == 3) {
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report_equal(&f.frozen, &report));
            }
            if (f.successor) {
                uint64_t used = 999;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    f.successor, &used) &&
                    !used);
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, f.successor, 0,
                    0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
            f.stop_in_effect = false;
            CHECK(!wl_evaluation_control_reset(f.control) &&
                !wl_evaluation_control_configure(f.control, 0));
            CHECK(!complete(&f) && f.effects == 5 && !wl_session_step(f.raw) &&
                f.effects == 8);
            equal(&f);
            destroy(&f);
        }
    wirelog_extension_snapshot_release(test_snapshot);
    test_snapshot = NULL;
    CHECK(!wirelog_extension_unregister(registry, "seed.host"));
    CHECK(!wirelog_extension_registry_destroy(registry));
}
static void
name_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)index;
    (void)bindings;
    fixture_t *f = active;
    if (!f || f->name_finished || (phase < 64 && phase != 1) || phase > 66)
        return;
    uint64_t used;
    CHECK(!wl_evaluation_control_test_active_consumed(
            control, wl_evaluation_control_active_attempt_id(control), &used));
    if (phase == 64) {
        CHECK(!f->name_cuts);
        f->name_base = used;
    } else
        CHECK(f->name_cuts && used - f->name_base == 256 * f->name_cuts);
    if (phase == 1) {
        CHECK(f->name_cuts == f->name_expected);
        uint64_t view[32];
        CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
            view, NULL));
        CHECK(view[9] == (f->ordinary ? 3u : 2u) && !view[18] && !view[17] &&
            !view[28]);
        f->name_finished = true;
    } else
        f->name_cuts++;
}
static void name_vectors(void) {
    const char *ordinary = ".decl input(x:int64)\n.decl seed(y:int64)\n"
        ".decl out(z:int64)\ninput(1). seed(7). "
        "out(strlen(to_string(x))):-input(x).";
    for (unsigned canonical = 0; canonical < 2; ++canonical)
        for (unsigned observed = 0; observed < 2; ++observed) {
            fixture_t f = create(canonical ? numeric : ordinary, observed,
                    true);
            f.ordinary = !canonical;
            f.name_expected = canonical ? 7 : 4;
            fixture_hook(&f, observed);
            wl_columnar_session_test_observed_transaction_checkpoint =
                name_checkpoint;
            CHECK(!complete(&f) && f.name_finished && !wl_session_step(f.raw));
            equal(&f);
            destroy(&f);
        }
}
static void committed_end(void) {
    for (unsigned point = 0; point < 4; ++point)
        for (unsigned successor = 0; successor < 2; ++successor) {
            fixture_t f = create(numeric, true, true);
            fixture_hook(&f, true);
            f.mode = successor ? 3 : 2;
            f.end_on_event = point == 0;
            if (point == 1) {
                f.phase = 15;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    checkpoint;
            }
            int rc = EAGAIN;
            for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; ++turn) {
                rc = wl_session_step(f.session);
                uint64_t view[32];
                int viewed = wl_columnar_session_test_seed_host_view(
                    COL_SESSION(f.session), view, NULL);
                if (!f.injected && !viewed && view[14] &&
                    ((point == 2 && view[10] == 2) ||
                    (point == 3 && view[10] == 3)))
                    end_original(&f, successor);
                if (f.injected) {
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report) &&
                        report_equal(&f.frozen, &report));
                    if (f.successor) {
                        uint64_t used = 999;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.control, f.successor, &used) &&
                            !used);
                    }
                }
                if (rc == EAGAIN && f.injected) {
                    int64_t value = 12;
                    CHECK(wl_session_insert(f.session, "input", &value, 1,
                        1) == EBUSY);
                }
            }
            CHECK(!rc && f.injected && f.events == 2 && f.published);
            CHECK(!wl_session_step(f.raw));
            equal(&f);
            uint64_t view[32];
            int viewed = wl_columnar_session_test_seed_host_view(
                COL_SESSION(f.session), view, NULL);
            CHECK(!viewed || viewed == ENOENT);
            if (!viewed)
                CHECK(view[10] == 4 && view[21] && !view[28]);
            if (f.successor) {
                wl_evaluation_control_attempt_finish_result_t ended =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, f.successor, 0,
                    0);
                CHECK(!ended.protocol && ended.report_valid &&
                    !ended.report.consumed);
            }
            destroy(&f);
        }
}
extern bool wl_columnar_session_test_fail_seed_arena_allocation;
static void snapshot_tuple(const char *relation, const int64_t *row,
    uint32_t width, void *context) {
    (void)relation; (void)row; (void)width;
    ++*(unsigned *)context;
}
static void snapshot_boundary(void) {
    for (unsigned observed = 0; observed < 2; ++observed) {
        fixture_t f = create(numeric, observed, true);
        fixture_hook(&f, observed);
        unsigned tuples = 0;
        wl_columnar_session_test_fail_seed_arena_allocation = true;
        /* Main's general transaction rejects the duplicate EDB/head name.
        * Snapshot must not enter the ordinary seed actor/birth issuer. */
        CHECK(wl_session_snapshot(f.session, snapshot_tuple,
            &tuples) == EINVAL);
        CHECK(!tuples && !f.events
            && wl_columnar_session_test_fail_seed_arena_allocation);
        wl_columnar_session_test_fail_seed_arena_allocation = false;
        uint64_t v[32]; wl_evaluation_control_t *seed = NULL;
        int viewed = wl_columnar_session_test_seed_host_view(
            COL_SESSION(f.session), v, &seed);
        CHECK(!viewed || viewed == ENOENT);
        CHECK(!seed);
        if (!viewed) CHECK(!v[1] && !v[3] && !v[11] && !v[28]);
        destroy(&f);
        f = create(numeric, observed, true); fixture_hook(&f, observed);
        CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
        destroy(&f);
    }
}
static void unloaded(void) {
    const char *sources[] = {
        numeric, "input(1). out(7). out(strlen(to_string(x))):-input(x)."
    };
    load_facts = false;
    for (unsigned source = 0; source < 2; ++source)
        for (unsigned observed = 0; observed < 2; ++observed) {
            fixture_t f = create(sources[source], observed, true);
            fixture_hook(&f, observed);
            col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t before = wl_columnar_relation_snapshot(old);
            int actual = complete(&f), raw = wl_session_step(f.raw);
            /* Main gives the bare raw head its rule width. The physical
             * seed actor still refuses that schema change. */
            col_rel_t *reference = session_find_rel(COL_SESSION(f.raw), "out");
            CHECK(actual == EINVAL && !raw && !f.events);
            CHECK(session_find_rel(COL_SESSION(f.session), "out") == old
                && wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(old)));
            CHECK(!old->ncols && reference->ncols == 1
                && !old->nrows && !reference->nrows);
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!wl_session_load_facts(f.raw, f.program) &&
                !wl_session_load_facts(f.session, f.program));
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            CHECK(!observed || f.events == 2);
            destroy(&f);
        }
    load_facts = true;
}
static void init_checkpoint(const wl_columnar_session_seed_t *actor,
    unsigned phase, unsigned word) {
    fixture_t *f = active;
    if (!f || f->injected || phase != f->phase || word != f->init_word)
        return;
    CHECK(!actor);
    uint64_t view[32];
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        view,
        NULL));
    CHECK(!view[3] && view[2] == 1 && view[28] && view[11]);
    checkpoint(f->control, phase, 0, NULL);
}
static void partial_init(void) {
    unsigned words = (unsigned)((wl_columnar_session_seed_size() + 7) / 8);
    for (unsigned cut = 0; cut < 4; ++cut)
        for (unsigned observed = 0; observed < 2; ++observed)
            for (unsigned mode = 0; mode < 4; ++mode) {
                fixture_t f = create(numeric, observed, true);
                fixture_hook(&f, observed);
                f.phase = cut == 3 ? 9001 : 9000;
                f.init_word = cut == 0   ? 0
                      : cut == 1 ? words / 2
                      : cut == 2 ? words - 1
                                 : 0;
                f.mode = mode;
                if (mode == 1)
                    CHECK(!wl_evaluation_control_configure(f.control,
                        UINT64_MAX));
                col_rel_t *old = session_find_rel(COL_SESSION(f.session),
                        "out");
                col_relation_snapshot_t snapshot =
                    wl_columnar_relation_snapshot(old);
                wl_columnar_session_seed_test_checkpoint = init_checkpoint;
                int rc = complete(&f);
                CHECK(f.injected &&
                    rc == (mode == 0   ? WL_EVALUATION_CONTROL_CANCELLED
                     : mode == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                                 : EINVAL));
                CHECK(!f.events &&
                    session_find_rel(COL_SESSION(f.session), "out") == old &&
                    wl_columnar_relation_snapshot_equal(
                        snapshot, wl_columnar_relation_snapshot(old)));
                uint64_t view[32];
                CHECK(wl_columnar_session_test_seed_host_view(COL_SESSION(
                        f.session),
                    view, NULL) == ENOENT);
                if (mode >= 2) {
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report) &&
                        report_equal(&f.frozen, &report));
                }
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.control, f.successor, &used) &&
                        !used);
                    wl_evaluation_control_attempt_finish_result_t ended =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.session->operation_admission, f.successor,
                        0, 0);
                    CHECK(!ended.protocol && ended.report_valid &&
                        !ended.report.consumed);
                }
                wl_columnar_session_seed_test_checkpoint = NULL;
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control, 0));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
}
extern unsigned wl_evaluation_control_test_finance_birth_fail;
static void
birth_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)bindings;
    fixture_t *f = active;
    if (!f || f->injected || phase != f->phase || (phase == 16 && index != 2))
        return;
    CHECK(control == f->control &&
        !wl_evaluation_control_test_finance_birth_fail);
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        f->image, NULL));
    f->injected = true;
    if (phase == 84)
        wl_columnar_session_test_fail_seed_arena_allocation = true;
    else
        wl_evaluation_control_test_finance_birth_fail = f->mode == 2 ? 2 : 1;
}
static void birth_failures(void) {
    for (unsigned site = 0; site < 3; ++site)
        for (unsigned observed = 0; observed < 2; ++observed) {
            fixture_t f = create(numeric, observed, true);
            fixture_hook(&f, observed);
            f.phase = site == 0 ? 84 : 85;
            f.mode = site;
            wl_columnar_session_test_observed_transaction_checkpoint =
                birth_checkpoint;
            CHECK(complete(&f) == ENOMEM && f.injected &&
                !wl_evaluation_control_test_finance_birth_fail);
            CHECK(!f.image[3] && f.image[2] == (site ? 1u : 0u) &&
                (!!f.image[28] == (site != 0)) && f.image[11] && !f.events &&
                !wl_columnar_session_test_fail_seed_arena_allocation);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report) &&
                report.result == ENOMEM && report.execution_status == ENOMEM &&
                report.stop_reason == WL_EVALUATION_CONTROL_NONE);
            wl_columnar_session_test_observed_transaction_checkpoint = NULL;
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            destroy(&f);
        }
    for (unsigned stage = 1; stage <= 3; ++stage) {
        fixture_t f = create(numeric, true, true);
        fixture_hook(&f, true);
        if (stage == 1) {
            f.phase = 16;
            wl_columnar_session_test_observed_transaction_checkpoint =
                birth_checkpoint;
        }
        int rc = EAGAIN;
        bool refused = false;
        for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; ++turn) {
            rc = wl_session_step(f.session);
            uint64_t view[32];
            int read =
                wl_columnar_session_test_seed_host_view(COL_SESSION(f.session),
                    view, NULL);
            if (!f.injected && !read && view[14] && view[10] == stage) {
                memcpy(f.image, view, sizeof(view));
                f.injected = true;
                CHECK(!wl_evaluation_control_test_finance_birth_fail);
                wl_evaluation_control_test_finance_birth_fail = 1;
            } else if (f.injected && !refused &&
                !wl_evaluation_control_test_finance_birth_fail){
                CHECK(!read && rc == EAGAIN && view[10] == stage && view[15] &&
                    view[22] == f.image[22]);
                refused = true;
            }
        }
        CHECK(!rc && f.injected && refused && f.events == 2);
        CHECK(!wl_session_step(f.raw));
        equal(&f);
        destroy(&f);
    }
}
static void compact_recompute(void) {
    const char *source =
        ".decl input(x:int64)\n.decl out(y:int64)\n"
        "input(1). out(7). out(to_number(to_string(x))):-input(x).";
    for (unsigned injected_governor = 0; injected_governor < 2;
        ++injected_governor) {
        fixture_t f = create(source, false, injected_governor);
        fixture_hook(&f, false);
        for (int64_t value = 2; value <= 1101; ++value)
            CHECK(!wl_session_insert(f.session, "input", &value, 1, 1) &&
                !wl_session_insert(f.raw, "input", &value, 1, 1));
        CHECK(!complete(&f) && !wl_session_step(f.raw));
        equal(&f);
        col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(old->capacity > 256);
        for (int64_t value = 1; value <= 1101; ++value)
            if (value != 12)
                CHECK(!wl_session_remove(f.session, "input", &value, 1, 1) &&
                    !wl_session_remove(f.raw, "input", &value, 1, 1));
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(&f) && !wl_session_step(f.raw));
        equal(&f);
        col_rel_t *out = session_find_rel(COL_SESSION(f.session), "out");
        CHECK(out->nrows == 2 && out->capacity == 64 &&
            out->columns[0][0] == 7 &&
            out->columns[0][1] == 12);
        destroy(&f);
    }
}
/* The completed main owner may already have freed its old header. END is
 * injected only after the next actual canonical producer has been observed. */
static void retained_header_end(void) {
    const unsigned phases[] = {64, 66, 80, 83, 86, 95, 99, 101};
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned point = 0; point < sizeof phases / sizeof *phases;
            ++point)
            for (unsigned successor = 0; successor < 2; ++successor) {
                fixture_t f = create(numeric, observed, true);
                fixture_hook(&f, observed);
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                col_rel_t *published = session_find_rel(COL_SESSION(f.session),
                        "out");
                col_relation_snapshot_t image =
                    wl_columnar_relation_snapshot(published);
                unsigned events = f.events;
                int64_t input = 12;
                CHECK(!wl_session_insert(f.session, "input", &input, 1, 1)
                    && !wl_session_insert(f.raw, "input", &input, 1, 1));
                CHECK(!wl_evaluation_control_reset(f.control));
                f.phase = phases[point]; f.mode = successor ? 3 : 2;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    checkpoint;
                CHECK(complete(&f) == EINVAL && f.injected && f.hits == 1);
                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                CHECK(session_find_rel(COL_SESSION(f.session),
                    "out") == published
                    && wl_columnar_relation_snapshot_equal(image,
                    wl_columnar_relation_snapshot(published)) &&
                    f.events == events);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report)
                    && report_equal(&f.frozen, &report));
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                        f.successor, &used) && !used);
                    wl_evaluation_control_attempt_finish_result_t end =
                        wl_evaluation_control_finish_attempt(f.control,
                            f.session->operation_admission, f.successor, 0, 0);
                    CHECK(!end.protocol && end.report_valid);
                }
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
                destroy(&f);
            }
}

static void parent_arm(const wl_columnar_session_seed_t *actor, unsigned phase,
    unsigned stage) {
    (void)stage;
    fixture_t *f = active;
    if (!f || f->injected || !actor || f->parent_kind != 97 || phase != 1501)
        return;
    uint64_t readers[8];
    CHECK(!wl_columnar_session_seed_test_reader_view(actor, readers));
    if (readers[0] == 5 && readers[1]) {
        CHECK(readers[2]);
        f->parent_armed = true;
    }
}
static void parent_checkpoint(wl_evaluation_control_t *control, uint64_t id,
    unsigned kind, bool yield) {
    fixture_t *f = active;
    if (!f || f->injected || yield || kind != f->parent_kind ||
        (kind == 97 && !f->parent_armed))
        return;
    CHECK(control == f->control &&
        id == wl_evaluation_control_active_attempt_id(control));
    checkpoint(control, f->phase, 0, NULL);
    if (kind == 83)
        CHECK(!f->image[3] && !f->image[28] && f->image[11]);
    else
        CHECK(f->image[3] && f->image[2] == 2 && f->image[28]);
}
static void parent_gate_literals(void) {
    for (unsigned kind = 0; kind < 3; ++kind)
        for (unsigned exact = 0; exact < 2; ++exact)
            for (unsigned observed = 0; observed < 2; ++observed) {
                fixture_t f = create(numeric, observed, kind != 2);
                fixture_hook(&f, observed);
                f.parent_kind = kind == 0 ? 86 : kind == 1 ? 97 : 83;
                f.phase = 4243;
                f.mode = exact ? 5 : 4;
                CHECK(!wl_evaluation_control_configure(f.control, UINT64_MAX));
                col_rel_t *old = session_find_rel(COL_SESSION(f.session),
                        "out");
                col_relation_snapshot_t image =
                    wl_columnar_relation_snapshot(old);
                wl_columnar_session_seed_test_checkpoint = parent_arm;
                wl_columnar_session_test_seed_parent_checkpoint =
                    parent_checkpoint;
                CHECK(complete(&f) == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    f.injected &&
                    f.hits == 1 && !f.events);
                CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
                    wl_columnar_relation_snapshot_equal(
                        image, wl_columnar_relation_snapshot(old)));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report.result == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    report.consumed == UINT64_MAX - (exact ? 0 : 255));
                wl_columnar_session_seed_test_checkpoint = NULL;
                wl_columnar_session_test_seed_parent_checkpoint = NULL;
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control, 0));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
}
/* Metadata binding and current MAIN construction precede the raw header.
 * Match its actual initialized extent, independently of public STEP count. */
static void wait_header(fixture_t *f, uint64_t extent, uint64_t raw[8]) {
    for (unsigned turn = 0; turn < 10000; ++turn) {
        CHECK(wl_session_step(f->session) == EAGAIN);
        CHECK(!wl_columnar_session_test_seed_raw_header_view(
                COL_SESSION(f->session), raw));
        if (raw[3] && raw[0] >= extent) return;
    }
    CHECK(false);
}
static void partial_headers_selected(unsigned observed, unsigned shard,
    unsigned partitions) {
    fixture_t geometry = create(numeric, false, true);
    fixture_hook(&geometry, false);
    uint64_t raw[8];
    wait_header(&geometry, 64, raw);
    CHECK(!wl_columnar_session_test_seed_raw_header_view(
            COL_SESSION(geometry.session), raw));
    unsigned blocks = (unsigned)((raw[1] + 63) / 64);
    CHECK(raw[0] == 64 && !raw[2] && raw[3] && blocks > 1);
    wl_evaluation_control_request_cancel(geometry.control);
    CHECK(complete(&geometry) == WL_EVALUATION_CONTROL_CANCELLED);
    destroy(&geometry);
    unsigned replayed = 0;
    for (unsigned boundary = 1; boundary <= blocks; ++boundary) {
        if ((boundary - 1) % partitions != shard)
            continue;
        ++replayed;
        for (unsigned successor = 0; successor < 2; ++successor) {
            fixture_t f = create(numeric, observed, true);
            fixture_hook(&f, observed);
            wait_header(&f, boundary == blocks ? raw[1] : 64 * boundary,
                raw);
            CHECK(!wl_columnar_session_test_seed_raw_header_view(
                    COL_SESSION(f.session), raw));
            CHECK(raw[3] &&
                raw[0] == (boundary == blocks ? raw[1] : 64 * boundary) &&
                raw[2] == (boundary == blocks));
            uintptr_t owned = (uintptr_t)raw[3];
            end_original(&f, successor);
            bool saw_free = false;
            int rc = EAGAIN;
            for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; ++turn) {
                rc = wl_session_step(f.session);
                CHECK(!wl_columnar_session_test_seed_raw_header_view(
                        COL_SESSION(f.session), raw));
                if (!raw[3]) {
                    saw_free = true;
                } else CHECK((uintptr_t)raw[3] == owned);
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report_equal(&f.frozen, &report));
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.control, f.successor, &used) &&
                        !used);
                }
                if (rc == EAGAIN) {
                    int64_t value = 99;
                    CHECK(wl_session_insert(f.session, "input", &value, 1,
                        1) == EBUSY);
                }
            }
            CHECK(rc == EINVAL && saw_free && !raw[3] && !raw[4] && !raw[5] &&
                !f.events);
            if (f.successor) {
                wl_evaluation_control_attempt_finish_result_t end =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, f.successor, 0,
                    0);
                CHECK(!end.protocol && end.report_valid &&
                    !end.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            destroy(&f);
        }
    }
    CHECK(replayed == blocks / partitions + (shard < blocks % partitions));
}
static void partial_header_close(void) {
    uint64_t raw[8];
    /* Close scope drains both the still-owned raw header and the truthful
     * post-free pending Original FINISH boundary. */
    for (unsigned after_free = 0; after_free < 2; ++after_free) {
        fixture_t f = create(numeric, false, true);
        fixture_hook(&f, false);
        wait_header(&f, 64, raw);
        end_original(&f, false);
        if (after_free) {
            bool freed = false;
            for (unsigned turn = 0; turn < 10000 && !freed; ++turn) {
                CHECK(wl_session_step(f.session) == EAGAIN);
                CHECK(!wl_columnar_session_test_seed_raw_header_view(
                        COL_SESSION(f.session), raw));
                freed = !raw[3];
            }
            CHECK(freed);
        }
        CHECK(wl_session_try_destroy(&f.session) == EBUSY && f.session);
        CHECK(complete(&f) == EINVAL);
        CHECK(!wl_session_try_destroy(&f.session) && !f.session);
        destroy(&f);
    }
}
static void arena_arm(wl_evaluation_control_t *control, unsigned phase,
    uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)index;
    (void)bindings;
    fixture_t *f = active;
    if (!f || f->injected || phase != 83)
        return;
    CHECK(control == f->control && COL_SESSION(f->session)->memory_governor);
    f->arena_armed = true;
    CHECK(!wl_evaluation_control_test_active_consumed(
            control, wl_evaluation_control_active_attempt_id(control),
            &f->arena_before_work));
    f->arena_before_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->actual_governors[0]));
}
static void arena_reserved(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token,
    unsigned phase) {
    fixture_t *f = active;
    if (!f || !f->arena_armed || f->injected || control != f->control ||
        phase != WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED)
        return;
    CHECK(token->identity == token &&
        token->bytes == wl_columnar_session_seed_size() &&
        atomic_load_explicit(&token->state, memory_order_acquire) ==
        WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
    f->arena_token = token;
    uint64_t used;
    CHECK(!wl_evaluation_control_test_active_consumed(
            control, wl_evaluation_control_active_attempt_id(control), &used));
    /* Actual saved-ID API: ENTRY74, CLAIM54, ROUND84, PUBLISH29.
     * This independent literal is unrelated to radix's source-only sibling. */
    CHECK(used - f->arena_before_work == UINT64_C(241));
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->actual_governors[0])) ==
        f->arena_before_reserved + token->bytes);
    if (!f->arena_tail)
        checkpoint(control, 83, 0, NULL);
}
static void arena_tail(wl_evaluation_control_t *control, uint64_t work) {
    fixture_t *f = active;
    if (f && f->arena_armed && f->arena_tail && f->arena_token &&
        !f->injected &&
        control == f->control && work == 29)
        checkpoint(control, 83, 0, NULL);
}
static void arena_attempt_restoration(void) {
    for (unsigned injected = 0; injected < 2; ++injected)
        for (unsigned tail = 0; tail < 2; ++tail)
            for (unsigned mode = 0; mode < 4; ++mode) {
                fixture_t f = create(numeric, false, injected);
                fixture_hook(&f, false);
                f.phase = 83;
                f.mode = mode;
                f.arena_tail = tail;
                if (mode == 1)
                    CHECK(!wl_evaluation_control_configure(f.control,
                        UINT64_MAX));
                wl_columnar_session_test_observed_transaction_checkpoint =
                    arena_arm;
                wl_columnar_memory_governor_test_checkpoint = arena_reserved;
                wl_columnar_memory_governor_test_work_checkpoint = arena_tail;
                int rc = EAGAIN;
                for (unsigned turn = 0; turn < 100000 && !f.injected; ++turn) {
                    rc = wl_session_step(f.session);
                    CHECK(rc == EAGAIN);
                }
                CHECK(f.injected && f.arena_token);
                const wl_columnar_memory_reservation_t *token = f.arena_token;
                CHECK(token->identity == token && !token->governor &&
                    !token->bytes &&
                    !token->replacement_bytes && !token->transition_kind &&
                    !atomic_load_explicit(&token->owner_bits,
                    memory_order_acquire) &&
                    atomic_load_explicit(&token->state, memory_order_acquire) ==
                    WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            f.actual_governors[0])) == f.arena_before_reserved);
                uint64_t view[32];
                CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(
                        f.session),
                    view, NULL));
                CHECK(!view[11] && !view[28] && !view[3] && view[15]);
                f.arena_token = NULL;
                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                wl_columnar_memory_governor_test_checkpoint = NULL;
                wl_columnar_memory_governor_test_work_checkpoint = NULL;
                rc = complete(&f);
                CHECK(rc == (mode == 0   ? WL_EVALUATION_CONTROL_CANCELLED
                     : mode == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                                 : EINVAL));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report.consumed == f.arena_before_work + 241);
                if (mode >= 2)
                    CHECK(report_equal(&f.frozen, &report));
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.control, f.successor, &used) &&
                        !used);
                    wl_evaluation_control_attempt_finish_result_t ended =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.session->operation_admission, f.successor,
                        0, 0);
                    CHECK(!ended.protocol && ended.report_valid &&
                        !ended.report.consumed);
                }
                CHECK(!wl_evaluation_control_reset(f.control) &&
                    !wl_evaluation_control_configure(f.control, 0));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
}
/* Census records actual forward turns, independent from the modulo partition.
* Every discovered ordinal belongs to exactly one of the registered shards. */
typedef struct {
    unsigned turn, phase, witness;
} frontier_point_t;
static unsigned frontier_witness(const uint64_t view[32]) {
    return ((view[0] == 2 && !view[18]) ? 1u : 0u) | (view[23] ? 2u : 0u) |
           (view[3] ? 4u : 0u) | (view[5] ? 8u : 0u);
}
static frontier_point_t *frontier_census(unsigned observed,
    unsigned *count_out) {
    frontier_point_t *points = NULL;
    unsigned count = 0, witnesses = 0;
    size_t capacity = 0;
    fixture_t census = create(numeric, observed, true);
    fixture_hook(&census, observed);
    int rc = EAGAIN;
    for (unsigned turn = 1; turn < 10000 && rc == EAGAIN; ++turn) {
        rc = wl_session_step(census.session);
        uint64_t view[32];
        if (rc == EAGAIN &&
            !wl_columnar_session_test_seed_host_view(COL_SESSION(
                census.session),
            view, NULL) &&
            view[1] && !view[14]) {
            if (count == capacity) {
                size_t next = capacity ? capacity * 2 : 64;
                CHECK(next > capacity && SIZE_MAX / sizeof(*points) >= next);
                frontier_point_t *grown = realloc(points,
                        next * sizeof(*points));
                CHECK(grown);
                points = grown;
                capacity = next;
            }
            unsigned witness = frontier_witness(view);
            points[count++] = (frontier_point_t){turn, (unsigned)view[0],
                                                 witness};
            witnesses |= witness;
            if (view[23])
                CHECK(view[0] == 8 && !view[15]);
        }
    }
    CHECK(!rc && count && (witnesses & 7u) == 7u);
    destroy(&census);
    *count_out = count;
    return points;
}
static bool frontier_replay(unsigned observed, const frontier_point_t *point,
    unsigned ordinal, unsigned successor) {
    bool saw_reader_binding_packet = false;
    fixture_t f = create(numeric, observed, true);
    fixture_hook(&f, observed);
    col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
    col_relation_snapshot_t image = wl_columnar_relation_snapshot(old);
    for (unsigned turn = 1; turn <= point->turn; ++turn)
        CHECK(wl_session_step(f.session) == EAGAIN);
    uint64_t view[32];
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f.session), view,
        NULL));
    CHECK(view[1] && !view[14]);
    CHECK(frontier_witness(view) == point->witness);
    if (view[23])
        CHECK(view[0] == 8 && !view[15]);
    end_original(&f, successor);
    int result = EAGAIN;
    for (unsigned turn = 0; turn < 100000 && result == EAGAIN; ++turn) {
        uint64_t before[32], paid_before = 0;
        wl_evaluation_control_t *maintenance = NULL;
        bool binding_packet = !wl_columnar_session_test_seed_host_view(
            COL_SESSION(f.session), before, &maintenance) &&
            before[12] == 9;
        if (binding_packet) {
            col_rel_t *candidate = (col_rel_t *)(uintptr_t)before[6];
            CHECK(observed && candidate && maintenance && !before[14]);
            CHECK(!atomic_load_explicit(&candidate->descriptor_access.state,
                memory_order_acquire));
            CHECK(!atomic_load_explicit(&candidate->source_access.state,
                memory_order_acquire));
            CHECK(!wl_evaluation_control_test_active_consumed(maintenance,
                before[22],
                &paid_before));
            saw_reader_binding_packet = true;
        }
        result = wl_session_step(f.session);
        if (binding_packet) {
            uint64_t paid_after = 0, after[32];
            CHECK(!wl_evaluation_control_test_active_consumed(maintenance,
                before[22],
                &paid_after));
            CHECK(paid_after == paid_before + 256);
            CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(
                    f.session),
                after, NULL));
            CHECK(after[12] == 4 && after[6] == before[6]);
        }
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report) &&
            report_equal(&f.frozen, &report));
        if (f.successor) {
            uint64_t used = 999;
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                f.successor,
                &used) &&
                !used);
        }
        CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
            wl_columnar_relation_snapshot_equal(
                image, wl_columnar_relation_snapshot(old)) &&
            !f.events);
        if (result == EAGAIN) {
            int64_t value = 99;
            CHECK(wl_session_insert(f.session, "input", &value, 1, 1) == EBUSY);
        }
    }
    if (result != EINVAL)
        fprintf(stderr, "public frontier point%u turn%u successor%u result%d\n",
            ordinal, point->turn, successor, result);
    CHECK(result == EINVAL);
    if (f.successor) {
        wl_evaluation_control_attempt_finish_result_t ended =
            wl_evaluation_control_finish_attempt(
            f.control, f.session->operation_admission, f.successor, 0, 0);
        CHECK(!ended.protocol && ended.report_valid && !ended.report.consumed);
    }
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!complete(&f) && !wl_session_step(f.raw));
    equal(&f);
    destroy(&f);
    return saw_reader_binding_packet;
}
static void public_frontier_selected(unsigned observed, unsigned shard,
    unsigned partitions) {
    unsigned count = 0, replayed = 0;
    frontier_point_t *points = frontier_census(observed, &count);
    bool saw_reader_binding_packet = false;
    for (unsigned point = 0; point < count; ++point) {
        if (point % partitions != shard)
            continue;
        ++replayed;
        for (unsigned successor = 0; successor < 2; ++successor)
            saw_reader_binding_packet |=
                frontier_replay(observed, &points[point], point, successor);
    }
    CHECK(replayed == count / partitions + (shard < count % partitions));
    /* Full local runs retain the old aggregate oracle. Registered partitions
     * each retain the literal binding checks; the separate qualification below
     * requires the positive observer witness independently of membership. */
    if (partitions == 1)
        CHECK(saw_reader_binding_packet == (observed != 0));
    free(points);
}
static void reader_binding_qualification(unsigned observed) {
    unsigned count = 0;
    frontier_point_t *points = frontier_census(observed, &count);
    unsigned point = 0;
    /* Actual EVENTS follows observer build, so its real owner must traverse
     * reader closure. The unobserved negative control selects a genuine live
     * child frame; REGISTRY need not be exposed as a public yield there.
     * These are read-only census observations, never injected flags. */
    while (point < count && (observed ? points[point].phase != 10u
                                    : !(points[point].witness & 2u)))
        ++point;
    CHECK(point < count);
    for (unsigned successor = 0; successor < 2; ++successor)
        CHECK(frontier_replay(observed, &points[point], point, successor) ==
            (observed != 0));
    free(points);
}
/* A compressed reachable history on the actual attached private owner. The
 * next genuine positive purchase, not this hook, creates sticky overflow. */
static void maintenance_overflow_seed(void) {
    fixture_t *f = active;
    if (!f || f->hits)
        return;
    uint64_t view[32], raw[8];
    wl_evaluation_control_t *maintenance = NULL;
    CHECK(!wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        view,
        &maintenance));
    CHECK(maintenance && view[22] && view[10] == 1);
    CHECK(!wl_columnar_session_test_seed_raw_header_view(COL_SESSION(
            f->session),
        raw) &&
        raw[2] && raw[3]);
    CHECK(!wl_evaluation_control_retain(maintenance));
    CHECK(!wl_evaluation_control_test_seed_active_consumed(
            maintenance, (const void *)(uintptr_t)raw[3], view[22],
            UINT64_MAX - 128));
    f->image[0] = (uintptr_t)maintenance;
    f->image[1] = view[22];
    f->image[2] = wl_evaluation_control_identity(maintenance);
    f->hits++;
}
static void
maintenance_actor_checkpoint(const wl_columnar_session_seed_t *actor,
    unsigned phase, unsigned stage) {
    (void)stage;
    if (active && actor && active->phase == phase)
        maintenance_overflow_seed();
}
static void maintenance_parent_checkpoint(
    wl_evaluation_control_t *control, unsigned phase, uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings) {
    (void)control;
    (void)index;
    (void)bindings;
    if (active && active->phase == 15 && phase == 15)
        maintenance_overflow_seed();
}
extern void (*wl_columnar_memory_governor_test_before_total_cas)(
    wl_evaluation_control_t *, wl_columnar_memory_governor_t *, uint64_t, bool);
static const wl_columnar_memory_retirement_receipt_t *credit_receipt;
static const wl_columnar_memory_reservation_t *credit_token;
static wl_columnar_memory_reservation_t credit_peer;
static bool credit_contended, credit_failed, credit_peer_held;
static unsigned credit_acks;
static uint64_t credit_bytes, credit_owner, credit_total;
static void maintenance_credit_checkpoint(
    const wl_columnar_session_seed_t *actor, unsigned phase, unsigned step,
    int rc, size_t offset, const char *role,
    const wl_columnar_memory_reservation_t *token,
    const wl_columnar_memory_retirement_receipt_t *receipt, uint64_t bytes,
    bool retiring) {
    (void)actor;
    (void)offset;
    (void)role;
    (void)bytes;
    (void)retiring;
    if (phase == 1504 && step == 2 && !rc && !credit_contended) {
        credit_receipt = receipt;
        credit_token = token;
    }
    if (phase == 1505 && step == 2 && !rc && credit_contended &&
        receipt == credit_receipt)
        credit_acks++;
}
static void
maintenance_credit_before_cas(wl_evaluation_control_t *control,
    wl_columnar_memory_governor_t *governor,
    uint64_t old, bool adding) {
    if (adding || credit_contended || !credit_receipt || !active)
        return;
    uint64_t view[32];
    wl_evaluation_control_t *maintenance = NULL;
    if (wl_columnar_session_test_seed_host_view(COL_SESSION(active->session),
        view, &maintenance) ||
        control != maintenance)
        return;
    CHECK(credit_receipt->phase == WL_COLUMNAR_MEMORY_RETIREMENT_CREDIT &&
        credit_receipt->captured_governor == governor &&
        credit_receipt->token == credit_token);
    credit_bytes = credit_receipt->captured_bytes;
    credit_owner =
        atomic_load_explicit(&credit_token->owner_bits, memory_order_acquire);
    CHECK(credit_bytes && old >= credit_bytes &&
        atomic_load_explicit(&credit_token->state, memory_order_acquire) ==
        WL_COLUMNAR_MEMORY_RESERVATION_RETIREMENT_CLAIMED);
    wl_columnar_memory_reservation_init(&credit_peer);
    CHECK(wl_columnar_memory_reserve_checked(governor, 1, &credit_peer) ==
        WL_COLUMNAR_MEMORY_ADMISSION_OK);
    credit_peer_held = true;
    credit_contended = true;
    credit_total = old;
    CHECK(wl_columnar_memory_reserved(governor) == old + 1);
    maintenance_overflow_seed();
    /* The actual zero immediately before strong CAS still succeeds. The
     * peer changes its expected total, so the genuine CAS must refuse. */
    CHECK(!wl_evaluation_control_charge_attempt(control, view[22], 0));
}
static void maintenance_credit_overflows(void) {
    for (unsigned observed = 0; observed < 2; observed++)
        for (unsigned successor = 0; successor < 2; successor++) {
            fixture_t f = create(numeric, observed, true);
            fixture_hook(&f, observed);
            credit_receipt = NULL;
            credit_token = NULL;
            credit_contended = credit_failed = credit_peer_held = false;
            credit_acks = 0;
            wl_columnar_session_seed_test_credit_checkpoint =
                maintenance_credit_checkpoint;
            wl_columnar_memory_governor_test_before_total_cas =
                maintenance_credit_before_cas;
            bool saw_probe = false, saw_new = false;
            int rc = EAGAIN;
            for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; turn++) {
                rc = wl_session_step(f.session);
                uint64_t v[8];
                if (!wl_columnar_session_test_seed_maintenance_view(
                        COL_SESSION(f.session), v)) {
                    if (credit_contended && !credit_failed) {
                        /* This route has no after-CAS hook. Its actual saved
                         * CREDIT owner plus the changed aggregate witnesses
                         * the failed strong CAS; no work counter is inferred. */
                        CHECK(rc == EAGAIN &&
                            credit_receipt->phase ==
                            WL_COLUMNAR_MEMORY_RETIREMENT_CREDIT &&
                            wl_columnar_memory_reserved(
                                credit_receipt->captured_governor) ==
                            credit_total + 1);
                        credit_failed = true;
                    }
                    if (v[0] || (credit_contended && !saw_new)) {
                        CHECK(credit_failed &&
                            credit_receipt->phase ==
                            WL_COLUMNAR_MEMORY_RETIREMENT_CREDIT &&
                            credit_receipt->token == credit_token &&
                            credit_receipt->captured_bytes == credit_bytes);
                        CHECK(atomic_load_explicit(&credit_token->state,
                            memory_order_acquire) ==
                            WL_COLUMNAR_MEMORY_RESERVATION_RETIREMENT_CLAIMED &&
                            atomic_load_explicit(&credit_token->owner_bits,
                            memory_order_acquire) == credit_owner);
                    }
                    if (v[0] == 1 && !saw_probe) {
                        CHECK(wl_evaluation_control_charge_attempt(
                                (wl_evaluation_control_t *)(uintptr_t)v[1],
                                v[2], 0) ==
                            EOVERFLOW);
                        saw_probe = true;
                        end_original(&f, successor != 0);
                    }
                    if (credit_contended && saw_probe && !v[0] &&
                        v[1] != f.image[0] &&
                        !saw_new) {
                        CHECK(v[1] && v[3] != f.image[2] && credit_peer_held &&
                            !credit_acks);
                        CHECK(wl_columnar_memory_rollback(&credit_peer));
                        credit_peer_held = false;
                        saw_new = true;
                    }
                }
                if (f.injected) {
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report) &&
                        report_equal(&f.frozen, &report));
                }
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            f.control, f.successor, &used) &&
                        !used);
                }
            }
            wl_columnar_session_seed_test_credit_checkpoint = NULL;
            wl_columnar_memory_governor_test_before_total_cas = NULL;
            CHECK(rc == EOVERFLOW && f.hits == 1 && saw_probe && saw_new &&
                credit_failed && credit_acks == 1 && !credit_peer_held);
            wl_evaluation_control_release(
                (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
            if (f.successor) {
                wl_evaluation_control_attempt_finish_result_t done =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, f.successor, 0,
                    0);
                CHECK(!done.protocol && done.report_valid &&
                    !done.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            destroy(&f);
        }
}
static bool native_credit_fault;
static uint64_t native_credit_total;
static void
maintenance_native_underflow_checkpoint(const wl_columnar_session_seed_t *actor,
    unsigned phase, unsigned stage) {
    (void)stage;
    if (!actor || phase != 1501 || native_credit_fault || !credit_receipt ||
        credit_receipt->phase != WL_COLUMNAR_MEMORY_RETIREMENT_CREDIT)
        return;
    fixture_t *f = active;
    uint64_t view[32];
    wl_evaluation_control_t *maintenance = NULL;
    CHECK(f && !wl_columnar_session_test_seed_host_view(COL_SESSION(f->session),
        view, &maintenance));
    CHECK(maintenance &&
        !wl_evaluation_control_charge_attempt(maintenance, view[22], 0));
    CHECK(!wl_evaluation_control_retain(maintenance));
    f->image[0] = (uintptr_t)maintenance;
    f->image[1] = view[22];
    f->image[2] = wl_evaluation_control_identity(maintenance);
    credit_bytes = credit_receipt->captured_bytes;
    CHECK(credit_bytes);
    credit_owner =
        atomic_load_explicit(&credit_token->owner_bits, memory_order_acquire);
    native_credit_total =
        wl_columnar_memory_reserved(credit_receipt->captured_governor);
    CHECK(native_credit_total >= credit_bytes);
    /* Explicit invariant-fault diagnostic, not production cancellation or a
     * resource denial. The real CREDIT method must return nativeEOVERFLOW. */
    atomic_store_explicit(&credit_receipt->captured_governor->reserved_bytes,
        credit_bytes - 1, memory_order_release);
    native_credit_fault = true;
    credit_contended = true;
    f->hits++;
}
static void maintenance_native_underflows(void) {
    for (unsigned observed = 0; observed < 2; observed++)
        for (unsigned successor = 0; successor < 2; successor++) {
            fixture_t f = create(numeric, observed, true);
            fixture_hook(&f, observed);
            native_credit_fault = false;
            credit_contended = false;
            credit_acks = 0;
            credit_receipt = NULL;
            credit_token = NULL;
            wl_columnar_session_seed_test_credit_checkpoint =
                maintenance_credit_checkpoint;
            wl_columnar_session_seed_test_checkpoint =
                maintenance_native_underflow_checkpoint;
            int rc = EAGAIN;
            bool saw_probe = false, saw_terminal = false;
            for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; turn++) {
                rc = wl_session_step(f.session);
                uint64_t v[8];
                if (!wl_columnar_session_test_seed_maintenance_view(
                        COL_SESSION(f.session), v) &&
                    v[0]) {
                    CHECK(native_credit_fault && v[1] == f.image[0] &&
                        v[2] == f.image[1] && v[3] == f.image[2] && !v[5] &&
                        (v[0] == 1 || v[0] == 6));
                    CHECK(credit_receipt->phase ==
                        WL_COLUMNAR_MEMORY_RETIREMENT_CREDIT &&
                        credit_receipt->captured_bytes == credit_bytes &&
                        atomic_load_explicit(&credit_token->state,
                        memory_order_acquire) ==
                        WL_COLUMNAR_MEMORY_RESERVATION_RETIREMENT_CLAIMED &&
                        atomic_load_explicit(&credit_token->owner_bits,
                        memory_order_acquire) == credit_owner);
                    CHECK(!wl_evaluation_control_charge_attempt(
                            (wl_evaluation_control_t *)(uintptr_t)v[1], v[2],
                            0));
                    saw_probe |= v[0] == 1;
                    saw_terminal |= v[0] == 6;
                }
            }
            CHECK(rc == EOVERFLOW && f.hits == 1 && saw_probe && saw_terminal &&
                !credit_acks && !f.events);
            end_original(&f, successor != 0);
            for (unsigned repeat = 0; repeat < 3; repeat++) {
                CHECK(wl_session_step(f.session) == EOVERFLOW);
                uint64_t v[8];
                CHECK(!wl_columnar_session_test_seed_maintenance_view(
                        COL_SESSION(f.session), v));
                CHECK(v[0] == 6 && v[1] == f.image[0] && !v[5]);
            }
            CHECK(wl_session_try_destroy(&f.session) == EBUSY && f.session);
            CHECK(wl_columnar_session_test_seed_native_terminal_resolve(
                    COL_SESSION(f.session), native_credit_total) == EINVAL);
            /* Restore only the actual deliberately corrupted aggregate. The
             * diagnostic then clears descriptive terminal metadata; genuine
             * owner methods still perform CREDIT and ACK exactly once. */
            atomic_store_explicit(
                &credit_receipt->captured_governor->reserved_bytes,
                native_credit_total, memory_order_release);
            CHECK(!wl_columnar_session_test_seed_native_terminal_resolve(
                    COL_SESSION(f.session), native_credit_total));
            CHECK(complete(&f) == EOVERFLOW && credit_acks == 1);
            wl_columnar_session_seed_test_credit_checkpoint = NULL;
            wl_columnar_session_seed_test_checkpoint = NULL;
            wl_evaluation_control_release(
                (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report) &&
                report_equal(&f.frozen, &report));
            if (f.successor) {
                uint64_t used = 999;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    f.successor, &used) &&
                    !used);
                wl_evaluation_control_attempt_finish_result_t done =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, f.successor, 0,
                    0);
                CHECK(!done.protocol && done.report_valid &&
                    !done.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            destroy(&f);
        }
}
static void maintenance_overflows(void) {
    const unsigned sites[] = {1501, 1503, 15};
    for (unsigned observed = 0; observed < 2; observed++)
        for (unsigned site = 0; site < 3; site++)
            for (unsigned ended = 0; ended < 3; ended++) {
                fixture_t f = create(numeric, observed, true);
                fixture_hook(&f, observed);
                f.phase = sites[site];
                wl_columnar_session_seed_test_checkpoint =
                    maintenance_actor_checkpoint;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    maintenance_parent_checkpoint;
                bool saw[7] = {false}, stopped = false;
                int rc = EAGAIN;
                for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; turn++) {
                    rc = wl_session_step(f.session);
                    uint64_t v[8];
                    if (!wl_columnar_session_test_seed_maintenance_view(
                            COL_SESSION(f.session), v)) {
                        CHECK(v[0] < 7);
                        saw[v[0]] = true;
                        if (v[0]) {
                            CHECK(rc == EAGAIN && v[0] != 6 && f.hits == 1);
                            CHECK(v[1] == f.image[0] && v[2] == f.image[1] &&
                                v[3] == f.image[2]);
                            if (v[0] == 1) {
                                CHECK(wl_evaluation_control_charge_attempt(
                                        (wl_evaluation_control_t *)(uintptr_t)v[
                                            1], v[2],
                                        0) == EOVERFLOW);
                                if (ended && !stopped) {
                                    end_original(&f, ended == 2);
                                    stopped = true;
                                }
                            }
                            if (v[0] >= 3) {
                                wl_evaluation_control_report_t report;
                                CHECK(!wl_evaluation_control_report(
                                        (wl_evaluation_control_t *)(uintptr_t)f.
                                        image[0], &report));
                                CHECK(report.attempt_id == f.image[1] &&
                                    report.execution_status == EOVERFLOW &&
                                    report.consumed == UINT64_MAX - 128);
                            }
                            CHECK((v[0] >= 4) == (v[5] != 0));
                            if (v[5])
                                CHECK(v[6] && v[7] && v[7] != v[3]);
                            if (v[0] == 5)
                                CHECK(v[4] == 3);
                        }
                    }
                    if (f.injected) {
                        wl_evaluation_control_report_t report;
                        CHECK(!wl_evaluation_control_report(f.control,
                            &report) &&
                            report_equal(&f.frozen, &report));
                    }
                    if (f.successor) {
                        uint64_t used = 999;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.control, f.successor, &used) &&
                            !used);
                    }
                }
                wl_columnar_session_seed_test_checkpoint = NULL;
                wl_columnar_session_test_observed_transaction_checkpoint = NULL;
                if (rc != (site == 2 ? 0 : EOVERFLOW))
                    fprintf(stderr,
                        "maintenance overflow site%u observed%u ended%u rc%d\n",
                        sites[site], observed, ended, rc);
                CHECK(f.hits == 1 && rc == (site == 2 ? 0 : EOVERFLOW));
                for (unsigned action = 1; action <= 5; action++)
                    CHECK(saw[action]);
                CHECK(stopped == (ended != 0));
                wl_evaluation_control_release(
                    (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
                if (f.successor) {
                    wl_evaluation_control_attempt_finish_result_t done =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.session->operation_admission, f.successor,
                        0, 0);
                    CHECK(!done.protocol && done.report_valid &&
                        !done.report.consumed);
                }
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
}
static void maintenance_recovery_refusals(void) {
    for (unsigned observed = 0; observed < 2; observed++)
        for (unsigned action = 1; action <= 5; action++)
            for (unsigned fault = 1; fault <= 2; fault++) {
                fixture_t f = create(numeric, observed, true);
                fixture_hook(&f, observed);
                f.phase = 1503;
                wl_columnar_session_seed_test_checkpoint =
                    maintenance_actor_checkpoint;
                int rc = EAGAIN;
                uint64_t before[8];
                bool reached = false;
                for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; turn++) {
                    rc = wl_session_step(f.session);
                    if (!wl_columnar_session_test_seed_maintenance_view(
                            COL_SESSION(f.session), before) &&
                        before[0] == action) {
                        reached = true;
                        break;
                    }
                }
                CHECK(reached && f.hits == 1 && rc == EAGAIN);
                end_original(&f, true);
                CHECK(!wl_evaluation_control_test_finance_birth_fail);
                uint64_t total = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(f.governor));
                wl_evaluation_control_test_finance_birth_fail = fault;
                CHECK(wl_session_step(f.session) == EAGAIN &&
                    !wl_evaluation_control_test_finance_birth_fail);
                uint64_t after[8];
                CHECK(!wl_columnar_session_test_seed_maintenance_view(
                        COL_SESSION(f.session), after));
                CHECK(!memcmp(before, after, sizeof before));
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(f.governor)) ==
                    total);
                CHECK(wl_session_try_destroy(&f.session) == EBUSY && f.session);
                if (after[5]) {
                    uint64_t used = 999;
                    CHECK(wl_evaluation_control_active_attempt_id((
                            wl_evaluation_control_t *)(uintptr_t)after[5]) ==
                        after[6]);
                    CHECK(!wl_evaluation_control_test_active_consumed(
                            (wl_evaluation_control_t *)(uintptr_t)after[5],
                            after[6],
                            &used) &&
                        !used);
                }
                CHECK(complete(&f) == EOVERFLOW);
                wl_columnar_session_seed_test_checkpoint = NULL;
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(f.control, &report) &&
                    report_equal(&f.frozen, &report));
                uint64_t used = 999;
                CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                    f.successor, &used) &&
                    !used);
                wl_evaluation_control_attempt_finish_result_t done =
                    wl_evaluation_control_finish_attempt(
                    f.control, f.session->operation_admission, f.successor, 0,
                    0);
                CHECK(!done.protocol && done.report_valid &&
                    !done.report.consumed);
                wl_evaluation_control_release(
                    (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
    const char *error_source =
        ".decl input(x:string)\n.decl out(y:int64)\n"
        "input(\"9223372036854775808\"). out(7). out(to_number(x)):-input(x).";
    for (unsigned observed = 0; observed < 2; observed++) {
        fixture_t f = create(error_source, observed, true);
        fixture_hook(&f, observed);
        f.phase = 1503;
        wl_columnar_session_seed_test_checkpoint = maintenance_actor_checkpoint;
        CHECK(complete(&f) == ERANGE && f.hits == 1 && !f.events &&
            wl_session_step(f.raw) == ERANGE);
        wl_columnar_session_seed_test_checkpoint = NULL;
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control, &report) &&
            report.execution_status == ERANGE && report.result == ERANGE);
        CHECK(!wl_evaluation_control_report(
                (wl_evaluation_control_t *)(uintptr_t)f.image[0], &report) &&
            report.execution_status == EOVERFLOW);
        wl_evaluation_control_release(
            (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
        destroy(&f);
    }
}
static void maintenance_native_dispose_overflows(void) {
    const unsigned stages[] = {0, 1, 4, 7};
    for (unsigned observed = 0; observed < 2; observed++)
        for (unsigned stage = 0; stage < 4; stage++)
            for (unsigned successor = 0; successor < 2; successor++) {
                fixture_t f = create(numeric, observed, true);
                fixture_hook(&f, observed);
                bool framed = false;
                int rc = EAGAIN;
                col_rel_t *old = session_find_rel(COL_SESSION(f.session),
                        "out");
                col_relation_snapshot_t image =
                    wl_columnar_relation_snapshot(old);
                for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; turn++) {
                    rc = wl_session_step(f.session);
                    uint64_t view[32];
                    if (!wl_columnar_session_test_seed_host_view(COL_SESSION(
                            f.session),
                        view, NULL) &&
                        view[23]) {
                        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
                        if (!wl_columnar_eval_stack_framed_context(
                                COL_SESSION(f.session)->cleanup_active,
                                COL_SESSION(f.session), &ctx) &&
                            ctx->child_bound && ctx->unary_native_cleanup) {
                            CHECK(view[0] == 8 && !view[15] && !view[24] &&
                                !view[25]);
                            framed = true;
                            end_original(&f, successor != 0);
                            break;
                        }
                    }
                }
                CHECK(framed && rc == EAGAIN);
                bool injected = false, saw[6] = {false}, birth_refused = false;
                for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; turn++) {
                    uint64_t before[32], v[8];
                    if (!wl_columnar_session_test_seed_host_view(COL_SESSION(
                            f.session),
                        before, NULL) &&
                        wl_columnar_session_test_native_error_phase(COL_SESSION(
                            f.session)) ==
                        WL_COLUMNAR_SESSION_NATIVE_ERROR_DISPOSE &&
                        before[12] == stages[stage] && !injected) {
                        CHECK(!before[23] && !before[24] && !before[25] &&
                            !COL_SESSION(f.session)->cleanup_active);
                        maintenance_overflow_seed();
                        injected = true;
                    }
                    if (injected && !birth_refused &&
                        !wl_columnar_session_test_seed_maintenance_view(
                            COL_SESSION(f.session), v) &&
                        v[0] == 3) {
                        CHECK(!wl_evaluation_control_test_finance_birth_fail);
                        wl_evaluation_control_test_finance_birth_fail = 1;
                        CHECK(wl_session_step(f.session) == EAGAIN &&
                            !wl_evaluation_control_test_finance_birth_fail);
                        uint64_t after[8];
                        CHECK(!wl_columnar_session_test_seed_maintenance_view(
                                COL_SESSION(f.session), after));
                        CHECK(!memcmp(v, after, sizeof v));
                        birth_refused = true;
                    }
                    rc = wl_session_step(f.session);
                    if (!wl_columnar_session_test_seed_maintenance_view(
                            COL_SESSION(f.session), v) &&
                        v[0]) {
                        CHECK(v[0] < 6 && injected);
                        saw[v[0]] = true;
                        CHECK(wl_columnar_session_test_native_error_phase(
                                COL_SESSION(
                                    f.session)) ==
                            WL_COLUMNAR_SESSION_NATIVE_ERROR_DISPOSE);
                    }
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report) &&
                        report_equal(&f.frozen, &report));
                    if (f.successor) {
                        uint64_t used = 999;
                        CHECK(!wl_evaluation_control_test_active_consumed(
                                f.control, f.successor, &used) &&
                            !used);
                    }
                    CHECK(session_find_rel(COL_SESSION(f.session),
                        "out") == old &&
                        wl_columnar_relation_snapshot_equal(
                            image, wl_columnar_relation_snapshot(old)) &&
                        !f.events);
                }
                if (rc != EINVAL)
                    fprintf(
                        stderr,
                        "Native dispose overflow observed%u stage%u successor%u rc%d\n",
                        observed, stages[stage], successor, rc);
                CHECK(rc == EINVAL && injected && f.hits == 1 && birth_refused);
                for (unsigned action = 1; action <= 5; action++)
                    CHECK(saw[action]);
                wl_evaluation_control_release(
                    (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
                if (f.successor) {
                    wl_evaluation_control_attempt_finish_result_t done =
                        wl_evaluation_control_finish_attempt(
                        f.control, f.session->operation_admission, f.successor,
                        0, 0);
                    CHECK(!done.protocol && done.report_valid &&
                        !done.report.consumed);
                }
                CHECK(!wl_evaluation_control_reset(f.control));
                CHECK(!complete(&f) && !wl_session_step(f.raw));
                equal(&f);
                destroy(&f);
            }
}
/* The ordinary successor path must publish a truthful error root before
 * recovery; a private-financier overflow cannot strand future STEP calls. */
static void maintenance_healthy_successor(void) {
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned ended = 0; ended < 3; ++ended) {
            fixture_t f = create(numeric, observed, true);
            fixture_hook(&f, observed);
            CHECK(!complete(&f));
            unsigned events = f.events;
            col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
            col_relation_snapshot_t image = wl_columnar_relation_snapshot(old);
            int64_t input = 12;
            CHECK(!wl_session_insert(f.session, "input", &input, 1, 1) &&
                !wl_session_insert(f.raw, "input", &input, 1, 1));
            CHECK(!wl_evaluation_control_reset(f.control));
            uint64_t v[32];
            wl_evaluation_control_t *maintenance = NULL;
            for (unsigned turn = 0; turn < 100000 && !maintenance; ++turn) {
                CHECK(wl_session_step(f.session) == EAGAIN);
                int viewed = wl_columnar_session_test_seed_host_view(
                    COL_SESSION(f.session), v, &maintenance);
                CHECK(!viewed || viewed == ENOENT);
                CHECK(session_find_rel(COL_SESSION(f.session), "out") == old
                    && f.events == events);
            }
            CHECK(maintenance && v[10] == 1 && !v[30]);
            maintenance_overflow_seed();
            CHECK(f.hits == 1);
            bool saw[6] = {false};
            int rc = EAGAIN;
            for (unsigned turn = 0; turn < 100000 && rc == EAGAIN; ++turn) {
                rc = wl_session_step(f.session);
                uint64_t action[8], raw[8];
                if (!wl_columnar_session_test_seed_maintenance_view(
                        COL_SESSION(f.session), action) &&
                    action[0]) {
                    CHECK(action[0] < 6 && rc == EAGAIN);
                    saw[action[0]] = true;
                    if (ended && !f.injected && action[0] == 1)
                        end_original(&f, ended == 2);
                    CHECK(!wl_columnar_session_test_seed_raw_header_view(
                            COL_SESSION(f.session), raw));
                    CHECK(raw[3] && raw[4] == raw[3] && raw[6] == EOVERFLOW);
                }
                if (f.injected) {
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(f.control, &report) &&
                        report_equal(&f.frozen, &report));
                }
                if (f.successor) {
                    uint64_t used = 999;
                    CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                        f.successor, &used) && !used);
                }
                if (rc == EAGAIN) {
                    wl_session_t *same = f.session;
                    CHECK(wl_session_try_destroy(&same) == EBUSY &&
                        same == f.session);
                }
                CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
                    wl_columnar_relation_snapshot_equal(
                        image, wl_columnar_relation_snapshot(old)) &&
                    f.events == events);
            }
            CHECK(rc == EOVERFLOW);
            for (unsigned action = 1; action <= 5; ++action)
                CHECK(saw[action]);
            wl_evaluation_control_release(
                (wl_evaluation_control_t *)(uintptr_t)f.image[0]);
            CHECK(f.injected == (ended != 0));
            if (f.successor) {
                wl_evaluation_control_attempt_finish_result_t finished =
                    wl_evaluation_control_finish_attempt(f.control,
                        f.session->operation_admission, f.successor, 0, 0);
                CHECK(!finished.protocol && finished.report_valid &&
                    !finished.report.consumed);
            }
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            destroy(&f);
        }
}
static void inactive_destructor(void) {
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned ended = 0; ended < 2; ++ended) {
            fixture_t f = create(numeric, observed, true);
            fixture_hook(&f, observed);
            f.end_on_event = observed && ended;
            if (!observed && ended) {
                f.phase = 15; f.mode = 2;
                wl_columnar_session_test_observed_transaction_checkpoint =
                    checkpoint;
            }
            CHECK(!complete(&f) && !wl_session_step(f.raw));
            equal(&f);
            CHECK(f.injected == (ended != 0));
            uint64_t v[32];
            wl_evaluation_control_t *seed = NULL;
            int viewed = wl_columnar_session_test_seed_host_view(
                COL_SESSION(f.session), v, &seed);
            CHECK(!viewed || viewed == ENOENT);
            CHECK(!seed);
            if (!viewed) CHECK(v[10] == 4 && !v[28] && !v[11]);
            unsigned events = f.events;
            wl_evaluation_control_report_t before, after;
            CHECK(!wl_evaluation_control_report(f.control, &before));
            CHECK(!wl_session_try_destroy(&f.session) && !f.session);
            CHECK(!wl_evaluation_control_report(f.control, &after)
                && report_equal(&before, &after) && f.events == events);
            destroy(&f);
        }
}
#if !defined(_WIN32)

#endif
/* A completed metadata release must survive an error in the separately paid
 * failed-retry suffix. Discover every real public stage8 boundary rather than
 * baking the review reproduction's ordinal into the regression. */
static unsigned retry_suffix_case(unsigned observed, unsigned target,
    unsigned ended, bool *recovered)
{
    *recovered = false;
    fixture_t f = create(numeric, observed, true); fixture_hook(&f, observed);
    CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
    unsigned events = f.events;
    col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
    col_relation_snapshot_t image = wl_columnar_relation_snapshot(old);
    int64_t input = 12;
    CHECK(!wl_session_insert(f.session, "input", &input, 1, 1) &&
        !wl_session_insert(f.raw, "input", &input, 1, 1));
    CHECK(!wl_evaluation_control_reset(f.control));
    wl_columnar_session_test_fail_seed_arena_allocation = true;
    unsigned points = 0; bool injected = false, saw[6] = {false};
    wl_evaluation_control_t *retained = NULL;
    int rc = EAGAIN;
    for (unsigned turn = 0; turn < 10000 && rc == EAGAIN; ++turn) {
        uint64_t v[32], raw[8]; wl_evaluation_control_t *maintenance = NULL;
        if (!wl_columnar_session_test_seed_host_view(COL_SESSION(f.session), v,
            &maintenance) && v[12] == 7 && !v[14] && v[30] == ENOMEM &&
            !v[29] && v[10] == 1 && maintenance &&
            wl_evaluation_control_active_attempt_id(maintenance) == v[22]) {
            if (points++ == target) {
                CHECK(!injected && maintenance && v[22]);
                CHECK(!wl_columnar_session_test_seed_raw_header_view(
                        COL_SESSION(f.session), raw) && raw[2] && raw[3]);
                CHECK(!wl_evaluation_control_retain(maintenance));
                retained = maintenance;
                CHECK(!wl_evaluation_control_test_seed_active_consumed(
                        maintenance,
                        (const void *)(uintptr_t)raw[3], v[22],
                        UINT64_MAX-255));
                injected = true;
            }
        }
        rc = wl_session_step(f.session);
        uint64_t action[8];
        if (injected && !wl_columnar_session_test_seed_maintenance_view(
                COL_SESSION(f.session), action) && action[0]) {
            CHECK(action[0] < 6 && rc == EAGAIN); saw[action[0]] = true;
            *recovered = true;
            if (ended && !f.injected && action[0] == 1)end_original(&f,
                    ended == 2);
            CHECK(!wl_columnar_session_test_seed_raw_header_view(
                    COL_SESSION(f.session), raw) && raw[6] == ENOMEM);
        }
        if (f.injected) {
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report) &&
                report_equal(&f.frozen, &report));
        }
        if (f.successor) {
            uint64_t used = 999;
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                f.successor, &used) && !used);
        }
        CHECK(session_find_rel(COL_SESSION(f.session), "out") == old &&
            wl_columnar_relation_snapshot_equal(image,
            wl_columnar_relation_snapshot(old)) && f.events == events);
        if (rc == EAGAIN) {
            wl_session_t *same = f.session;
            CHECK(wl_session_try_destroy(&same) == EBUSY && same == f.session);
        }
    }
    if (rc != ENOMEM)fprintf(stderr,
            "retry suffix observed%u target%u ended%u points%u rc%d\n",
            observed, target, ended, points, rc);
    CHECK(rc == ENOMEM && !wl_columnar_session_test_fail_seed_arena_allocation);
    CHECK(injected == (target != UINT32_MAX) &&
        f.injected == (*recovered && ended != 0));
    if (*recovered)for (unsigned action = 1; action <= 5;
            ++action) CHECK(saw[action]);
    wl_evaluation_control_release(retained);
    uint64_t raw[8];
    CHECK(!wl_columnar_session_test_seed_raw_header_view(COL_SESSION(f.session),
        raw) &&
        !raw[3] && !raw[4] && !raw[5]);
    if (!ended) {
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.control,
            &report) && report.result == ENOMEM);
    }
    if (f.successor) {
        wl_evaluation_control_attempt_finish_result_t finished =
            wl_evaluation_control_finish_attempt(f.control,
                f.session->operation_admission, f.successor, 0, 0);
        CHECK(!finished.protocol && finished.report_valid &&
            !finished.report.consumed);
    }
    /* Paid failed-retry closure legitimately protects mutation until the
     * next actual STEP settles. Exercise that retry before testing admission. */
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
    input = 123;
    CHECK(!wl_session_insert(f.session, "input", &input, 1, 1) &&
        !wl_session_insert(f.raw, "input", &input, 1, 1));
    CHECK(!wl_evaluation_control_reset(f.control));
    CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
    destroy(&f); return points;
}
void wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase) {
    (void)token; (void)phase;
}
extern void (*wl_columnar_memory_governor_test_before_finish_credit_cas)(
    wl_columnar_memory_reservation_t *, uint64_t);
static unsigned terminal_credit_calls, terminal_credit_done,
    terminal_credit_reports;
static unsigned terminal_end_mode;
static const wl_columnar_memory_reservation_t *terminal_credit_token;
static void retry_terminal_credit(wl_columnar_memory_reservation_t *token,
    uint64_t observed) {
    (void)observed;
    if (active &&
        wl_columnar_session_test_terminal_credit(COL_SESSION(active->session),
        token)) {
        CHECK(!terminal_credit_token || terminal_credit_token == token);
        terminal_credit_token = token;
        ++terminal_credit_calls;
    }
}
static void retry_terminal_advanced(wl_col_session_t *session,
    wl_evaluation_control_t *funding, uint64_t id, unsigned before,
    unsigned after, int result) {
    (void)id;
    if (before != WL_COLUMNAR_SESSION_TERMINAL_CREDIT || result ||
        after != WL_COLUMNAR_SESSION_TERMINAL_HANDOFF) return;
    uint64_t v[16];
    CHECK(active && session == COL_SESSION(active->session)
        && !wl_columnar_session_test_terminal_view(session, v));
    CHECK(!v[2] && !v[3] && !v[12] && terminal_credit_token
        && terminal_credit_calls == 1 && !terminal_credit_done);
    CHECK(atomic_load_explicit(&terminal_credit_token->state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
    ++terminal_credit_done;
    wl_evaluation_control_request_cancel(funding);
    if (terminal_end_mode) end_original(active, terminal_end_mode == 2);
}
static void retry_terminal_finished(const wl_col_session_t *session,
    unsigned phase, bool maintenance, wl_evaluation_control_report_t report) {
    if (phase != WL_COLUMNAR_SESSION_TERMINAL_CREDIT || maintenance
        || !terminal_credit_done) return;
    CHECK(active && session == COL_SESSION(active->session)
        && terminal_credit_done == 1 && !report.cleanup_status
        && report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
    ++terminal_credit_reports;
}
static void retry_terminal_report(unsigned observed) {
    for (unsigned ended = 0; ended < 3; ++ended) {
        fixture_t f = create(numeric, observed, true);
        fixture_hook(&f, observed);
        CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
        col_rel_t *old = session_find_rel(COL_SESSION(f.session), "out");
        col_relation_snapshot_t image = wl_columnar_relation_snapshot(old);
        unsigned events = f.events;
        int64_t value = 12;
        CHECK(!wl_session_insert(f.session, "input", &value, 1, 1)
            && !wl_session_insert(f.raw, "input", &value, 1, 1)
            && !wl_evaluation_control_reset(f.control));
        terminal_credit_calls = terminal_credit_done = terminal_credit_reports =
            0;
        terminal_credit_token = NULL; terminal_end_mode = ended;
        wl_columnar_session_test_fail_seed_arena_allocation = true;
        wl_columnar_memory_governor_test_before_finish_credit_cas =
            retry_terminal_credit;
        wl_columnar_session_test_terminal_advanced = retry_terminal_advanced;
        wl_columnar_session_test_terminal_finished = retry_terminal_finished;
        CHECK(complete(&f) == ENOMEM);
        wl_columnar_memory_governor_test_before_finish_credit_cas = NULL;
        wl_columnar_session_test_terminal_advanced = NULL;
        wl_columnar_session_test_terminal_finished = NULL;
        CHECK(terminal_credit_calls == 1 && terminal_credit_done == 1
            && terminal_credit_reports == 1 && f.events == events
            && session_find_rel(COL_SESSION(f.session), "out") == old
            && wl_columnar_relation_snapshot_equal(image,
            wl_columnar_relation_snapshot(old)));
        if (ended) {
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report_equal(&f.frozen, &report));
        }
        if (f.successor) {
            uint64_t used = 999;
            CHECK(!wl_evaluation_control_test_active_consumed(f.control,
                f.successor, &used) && !used);
            wl_evaluation_control_attempt_finish_result_t end =
                wl_evaluation_control_finish_attempt(f.control,
                    f.session->operation_admission, f.successor, 0, 0);
            CHECK(!end.protocol && end.report_valid);
        }
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
        value = 123;
        CHECK(!wl_session_insert(f.session, "input", &value, 1, 1)
            && !wl_session_insert(f.raw, "input", &value, 1, 1)
            && !wl_evaluation_control_reset(f.control));
        CHECK(!complete(&f) && !wl_session_step(f.raw)); equal(&f);
        destroy(&f);
    }
}
static void retry_suffix(unsigned observed)
{
    bool recovered;
    unsigned count = retry_suffix_case(observed, UINT32_MAX, 0, &recovered);
    CHECK(count && !recovered);
    bool saw_recovery = false;
    for (unsigned target = 0; target < count; ++target)
        for (unsigned ended = 0; ended < 3; ++ended) {
            retry_suffix_case(observed, target, ended, &recovered);
            saw_recovery |= recovered;
        }
    CHECK(saw_recovery);
    retry_terminal_report(observed);
}
typedef struct {
    const char *name;
    void (*run)(void);
} seed_family_t;
static const seed_family_t seed_families[] = {
    {"inactive-destructor", inactive_destructor},
    {"healthy-successor", maintenance_healthy_successor},
    {"maintenance-overflows", maintenance_overflows},
    {"maintenance-credit", maintenance_credit_overflows},
    {"maintenance-native-underflows", maintenance_native_underflows},
    {"maintenance-refusals", maintenance_recovery_refusals},
    {"maintenance-native-dispose", maintenance_native_dispose_overflows},
    {"positives", positives},
    {"stops", stops},
    {"child-error", child_error},
    {"governor-denials", governor_denials},
    {"name-vectors", name_vectors},
    {"committed-end", committed_end},
    {"unloaded", unloaded},
    {"snapshot-boundary", snapshot_boundary},
    {"partial-init", partial_init},
    {"birth-failures", birth_failures},
    {"compact-recompute", compact_recompute},
    {"retained-header-end", retained_header_end},
    {"parent-gate-literals", parent_gate_literals},
    {"arena-attempt-restoration", arena_attempt_restoration},
    {"partial-header-close", partial_header_close},
};
static bool selector_unsigned(const char *text, unsigned maximum,
    unsigned *out) {
    unsigned value = 0;
    if (!*text)
        return false;
    for (const char *p = text; *p; ++p) {
        if (*p < '0' || *p > '9')
            return false;
        unsigned digit = (unsigned)(*p - '0');
        if (value > maximum / 10 ||
            (value == maximum / 10 && digit > maximum % 10))
            return false;
        value = value * 10 + digit;
    }
    *out = value;
    return true;
}
int main(int argc, char **argv) {
    if (argc == 1) {
        for (unsigned i = 0; i < sizeof(seed_families) / sizeof(*seed_families);
            ++i)
            seed_families[i].run();
        for (unsigned observed = 0; observed < 2; ++observed) {
            partial_headers_selected(observed, 0, 1);
            public_frontier_selected(observed, 0, 1);
            reader_binding_qualification(observed);
            retry_suffix(observed);
        }
    } else if (argc == 2) {
        unsigned i = 0;
        while (i < sizeof(seed_families) / sizeof(*seed_families) &&
            strcmp(argv[1], seed_families[i].name))
            ++i;
        if (i == sizeof(seed_families) / sizeof(*seed_families))
            return 2;
        seed_families[i].run();
    } else if (argc == 3 && (!strcmp(argv[1], "reader-binding") ||
        !strcmp(argv[1], "retry-suffix"))) {
        unsigned observed;
        if (!selector_unsigned(argv[2], 1, &observed))
            return 2;
        if (!strcmp(argv[1], "reader-binding"))
            reader_binding_qualification(observed);
        else retry_suffix(observed);
    } else if (argc == 4 && (!strcmp(argv[1], "frontier") ||
        !strcmp(argv[1], "partial-headers"))){
        unsigned observed, shard;
        if (!selector_unsigned(argv[2], 1, &observed) ||
            !selector_unsigned(argv[3], 31, &shard))
            return 2;
        if (!strcmp(argv[1], "frontier"))
            public_frontier_selected(observed, shard, 32);
        else
            partial_headers_selected(observed, shard, 32);
    } else
        return 2;
    puts("canonical seed hosts: passed");
    return 0;
}
