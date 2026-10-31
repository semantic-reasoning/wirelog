/* Session wrappers over legal scalar physical records. Tuple callbacks do not
 * stand in for the physical source/intermediate/head assertions below. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
extern void (*wl_columnar_eval_stack_test_physical_publication)(
    wl_col_session_t *, const col_rel_t *, const col_rel_t *);
extern void (*wl_columnar_eval_serial_test_after_delta_stamp)(
    wl_col_session_t *, col_rel_t *, col_rel_t *);
static wl_session_t *raw_active;
static unsigned raw_deltas;
static void derived_delta(wl_col_session_t *s, col_rel_t *head,
    col_rel_t *delta)
{
    if (&s->base != raw_active)return;
    CHECK(head && delta->timestamps && delta->nrows);
    for (unsigned r = 0; r < delta->nrows; ++r) {
        const col_delta_timestamp_t *t = &delta->timestamps[r];
        CHECK(t->iteration == s->current_iteration && t->worker == 0
            && t->_reserved == 0 && t->multiplicity == 1);
        CHECK(delta->ncols == 2 && delta->columns[0][r] >= 1
            && delta->columns[1][r] <= 4 &&
            delta->columns[0][r] < delta->columns[1][r]);
        CHECK(t->iteration ==
            (uint32_t)(delta->columns[1][r]-delta->columns[0][r]-1));
    }
    unsigned stratum = UINT32_MAX;
    for (unsigned si = 0; si < s->plan->stratum_count; ++si)
        for (unsigned ri = 0; ri < s->plan->strata[si].relation_count; ++ri)
            if (!strcmp(s->plan->strata[si].relations[ri].name,
                head->name)) stratum = si;
    CHECK(stratum != UINT32_MAX);
    for (unsigned r = 0; r < delta->nrows;
        ++r) CHECK(delta->timestamps[r].stratum == stratum);
    ++raw_deltas;
}
static wl_session_t *active_session;
static unsigned active_kind, stop_mode, filter_witnesses, projections;
static bool stop_hit;
static void equal_stamp(const col_delta_timestamp_t *a,
    const col_delta_timestamp_t *b)
{
    CHECK(a->iteration == b->iteration && a->stratum == b->stratum
        && a->worker == b->worker && a->_reserved == 0 && b->_reserved == 0
        && a->multiplicity == b->multiplicity);
}
static void publication(wl_col_session_t *s, const col_rel_t *in,
    const col_rel_t *out)
{
    if (&s->base != active_session) return;
    if (!active_kind && !strcmp(out->name, "$filter") && in->timestamps) {
        CHECK(out->timestamps && out->ncols == 1 && in->ncols == 1);
        unsigned kept = 0;
        for (unsigned r = 0; r < in->nrows; ++r)if (in->columns[0][r] > 0) {
                CHECK(kept < out->nrows &&
                    out->columns[0][kept] == in->columns[0][r]);
                equal_stamp(&out->timestamps[kept++], &in->timestamps[r]);
            }
        CHECK(kept == out->nrows && kept);
        ++filter_witnesses;
        if (stop_mode && !stop_hit) {
            stop_hit = true;
            if (stop_mode ==
                1) wl_evaluation_control_request_cancel(
                    s->base.evaluation_control);
            else CHECK(wl_evaluation_control_charge(s->base.evaluation_control,
                    UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
    }
    if (!strcmp(out->name, "$map") || !strcmp(out->name, "$join")
        || !strcmp(out->name, "$semi") || !strcmp(out->name, "$anti")
        || !strcmp(out->name, "$reduce") || !strcmp(out->name, "$concat")) {
        CHECK(!out->timestamps); ++projections;
        if (((active_kind == 6 && !strcmp(out->name, "$join"))
            || (active_kind == 5 && !strcmp(out->name,
            "$concat"))) && stop_mode && !stop_hit){
            CHECK(session_find_rel(s,
                active_kind == 6?"edge":"input")->timestamps);
            stop_hit = true;
            if (stop_mode ==
                1) wl_evaluation_control_request_cancel(
                    s->base.evaluation_control);
            else CHECK(wl_evaluation_control_charge(s->base.evaluation_control,
                    UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
    }
}
typedef struct { int64_t a, b; int diff; unsigned width; } event_t;
typedef struct { event_t rows[64]; unsigned count; } capture_t;
static void on_delta(const char *name, const int64_t *v, uint32_t width,
    int diff, void *data)
{
    if (strcmp(name, "output") && strcmp(name, "path"))return;
    capture_t *c = data; CHECK(width <= 2 && width && c->count < 64);
    c->rows[c->count++] = (event_t){v[0], width == 2?v[1]:0, diff, width};
}
static void on_tuple(const char *name, const int64_t *v, uint32_t width,
    void *data)
{
    on_delta(name, v, width, 1, data);
}
static int order(const void *a, const void *b)
{
    const event_t *x = a, *y = b;
    if (x->a != y->a)return (x->a > y->a)-(x->a < y->a);
    if (x->b != y->b)return (x->b > y->b)-(x->b < y->b);
    if (x->width != y->width)return (x->width > y->width)-(x->width < y->width);
    return (x->diff > y->diff)-(x->diff < y->diff);
}
static void equal_capture(capture_t *a, capture_t *b)
{
    if (a->count != b->count) {
        fprintf(stderr, "capture kind=%u actual=%u expected=%u\n", active_kind,
            a->count, b->count);
        for (unsigned i = 0; i < a->count;
            ++i) fprintf(stderr, " row %lld %lld diff%d\n",
                (long long)a->rows[i].a, (long long)a->rows[i].b,
                a->rows[i].diff);
    }
    CHECK(a->count == b->count);
    qsort(a->rows, a->count, sizeof(event_t), order);
    qsort(b->rows, b->count, sizeof(event_t), order);
    for (unsigned i = 0; i < a->count;
        ++i) CHECK(!order(&a->rows[i], &b->rows[i]));
}
static int step(wl_session_t *s)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(s); if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}
typedef struct { col_rel_t *rel; col_relation_snapshot_t identity;
                 col_delta_timestamp_t stamps[6]; int64_t cells[12];
                 unsigned rows, width; } physical_t;
static void stamp(physical_t *p, wl_session_t *s, const char *name,
    unsigned salt)
{
    p->rel = session_find_rel((wl_col_session_t *)s, name); CHECK(p->rel);
    CHECK(!col_rel_enable_timestamps(p->rel));
    p->rows = p->rel->nrows; p->width = p->rel->ncols;
    CHECK(p->rows <= 6 && p->width <= 2);
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(!col_rel_source_writer_acquire(p->rel, &writer));
    const int64_t weights[] = {0, 1, -1, 3, -4};
    wl_columnar_relation_touch_storage(p->rel);
    for (unsigned r = 0; r < p->rows; ++r) {
        p->stamps[r] = (col_delta_timestamp_t){.iteration = 1000+salt*100-r,
                                               .stratum = r%3, .worker = r%2,
                                               ._reserved = 0,
                                               .multiplicity = weights[r%5]};
        p->rel->timestamps[r] = p->stamps[r];
        for (unsigned c = 0; c < p->width;
            ++c) p->cells[r*p->width+c] = p->rel->columns[c][r];
    }
    CHECK(!wl_columnar_source_access_writer_release(&writer));
    p->identity = wl_columnar_relation_snapshot(p->rel);
}
static void unchanged(const physical_t *p)
{
    /* A successful raw snapshot may compact oversized storage. The physical
     * row image stays exact; failed attempts are checked strictly below. */
    col_relation_snapshot_t now = wl_columnar_relation_snapshot(p->rel);
    CHECK(now.relation_identity == p->identity.relation_identity
        && now.view_generation == p->identity.view_generation
        && now.storage_generation >= p->identity.storage_generation);
    for (unsigned r = 0; r < p->rows; ++r) {
        equal_stamp(&p->rel->timestamps[r], &p->stamps[r]);
        for (unsigned c = 0; c < p->width;
            ++c) CHECK(p->rel->columns[c][r] == p->cells[r*p->width+c]);
    }
}
static void populate(wl_session_t *s, unsigned kind)
{
    const int64_t scalar[] = {1, 2, 3, 4, 5}, filter[] = {0, 1, 1, 2, 3, -1};
    const int64_t left[] = {1, 10, 2, 20, 3, 30, 4, 40, 5, 50};
    const int64_t right[] = {10, 100, 20, 200, 20, 201, 60, 600};
    const int64_t keys[] = {10, 20, 20, 60}, mask[] = {2, 4, 7, 8},
        more[] = {4, 6, 6, 7};
    if (kind == 6) {
        const int64_t chain[] = {1, 2, 2, 3, 3, 4};
        CHECK(!wl_session_insert(s, "edge", chain, 3, 2)); return;
    }
    CHECK(!wl_session_insert(s, "input",
        kind == 0?filter:kind == 1 || kind == 2?left:scalar, kind == 0?6:5,
        kind == 1 || kind == 2?2:1));
    if (kind == 1)CHECK(!wl_session_insert(s, "rhs", right, 4, 2));
    if (kind == 2)CHECK(!wl_session_insert(s, "rhs", keys, 4, 1));
    if (kind == 3)CHECK(!wl_session_insert(s, "rhs", mask, 4, 1));
    if (kind == 5)CHECK(!wl_session_insert(s, "rhs", more, 4, 1));
}
static void expected(unsigned kind, capture_t *e)
{
    const int64_t f[] = {1, 2, 3}, j[][2] = {{1, 100}, {2, 200}, {2, 201}},
        semi[] = {1, 2}, anti[] = {1, 3, 5};
    if (kind == 6) {
        for (int64_t a = 1; a <= 3; ++a) for (int64_t b = a+1; b <= 4; ++b) {
                int64_t pair[] = {a, b}; on_tuple("output", pair, 2, e);
            }
        return;
    }
    if (kind == 1)for (unsigned i = 0; i < 3;
            ++i) on_tuple("output", j[i], 2, e);
    else {
        unsigned n = kind == 0 || kind == 3?3:kind == 2?2:kind == 4?1:7;
        for (unsigned i = 0; i < n; ++i) {
            int64_t v = kind == 0?f[i]:kind == 2?semi[i]:kind ==
                3?anti[i]:kind == 4?5:i+1; on_tuple("output", &v, 1, e);
        }
    }
}
static void evaluate(wl_session_t *s, bool snapshot, capture_t *image,
    capture_t *events, unsigned fault)
{
    col_rel_t *input = session_find_rel((wl_col_session_t *)s,
            active_kind == 6?"edge":"input");
    col_relation_snapshot_t input_image = wl_columnar_relation_snapshot(input);
    col_delta_timestamp_t stamps[6]; CHECK(input->nrows <= 6);
    memcpy(stamps, input->timestamps, input->nrows*sizeof(*stamps));
    col_rel_t *old = session_find_rel((wl_col_session_t *)s,
            active_kind == 6?"path":"output");
    col_relation_snapshot_t old_image = wl_columnar_relation_snapshot(old);
    stop_mode = fault; stop_hit = false;
    int rc = snapshot?wl_session_snapshot(s, on_tuple, image):step(s);
    int want = fault == 1?WL_EVALUATION_CONTROL_CANCELLED:fault ==
        2?WL_EVALUATION_CONTROL_WORK_LIMIT:0;
    if (rc != want)fprintf(stderr,
            "kind%u snapshot%u fault%u result%d expected%d\n", active_kind,
            snapshot, fault, rc, want);
    CHECK(rc == want && (!fault || stop_hit));
    if (fault) {
        CHECK(!image->count && !events->count);
        CHECK(wl_columnar_relation_snapshot_equal(input_image,
            wl_columnar_relation_snapshot(input)));
        for (unsigned r = 0; r < input->nrows;
            ++r) equal_stamp(&input->timestamps[r], &stamps[r]);
        CHECK(session_find_rel((wl_col_session_t *)s,
            active_kind == 6?"path":"output") == old);
        if (old)CHECK(wl_columnar_relation_snapshot_equal(old_image,
                wl_columnar_relation_snapshot(old)));
        wl_evaluation_control_report_t report = {0}, again = {0};
        CHECK(!wl_evaluation_control_report(s->evaluation_control, &report));
        CHECK(report.result == want && !report.cleanup_status &&
            report.stop_reason == want);
        CHECK(!wl_evaluation_control_report(s->evaluation_control, &again));
        CHECK(!memcmp(&report, &again, sizeof(report)));
        CHECK(!wl_evaluation_control_reset(s->evaluation_control));
        stop_mode = 0;
        CHECK(!(snapshot?wl_session_snapshot(s, on_tuple, image):step(s)));
    }
}
int main(int argc, char **argv)
{
    const char *programs[] = {
        ".decl input(x:int64)\n.decl output(x:int64)\noutput(x) :- input(x), x>0.\n",
        ".decl input(x:int64,y:int64)\n.decl rhs(y:int64,z:int64)\n.decl output(x:int64,z:int64)\noutput(x,z) :- input(x,y), rhs(y,z).\n",
        ".decl input(x:int64,y:int64)\n.decl rhs(y:int64)\n.decl output(x:int64)\noutput(x) :- input(x,y), rhs(y).\n",
        ".decl input(x:int64)\n.decl rhs(x:int64)\n.decl output(x:int64)\noutput(x) :- input(x), !rhs(x).\n",
        ".decl input(x:int64)\n.decl output(n:int64)\noutput(count(x)) :- input(x).\n",
        ".decl input(x:int64)\n.decl rhs(x:int64)\n.decl output(x:int64)\noutput(x) :- input(x).\noutput(x) :- rhs(x).\n",
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\npath(x,y) :- edge(x,y).\npath(x,z) :- path(x,y), edge(y,z).\n"
    };
    unsigned first = argc > 1?(unsigned)strtoul(argv[1], NULL, 10):0;
    CHECK(first < 7);
    for (unsigned kind = first; kind < (argc > 1?first+1:7);
        ++kind) for (unsigned snapshot = 0;
            snapshot < 2; ++snapshot)
            for (unsigned observed = 0; observed < 2;
                ++observed) for (unsigned fault = 0;
                    fault < ((!kind || kind == 5 || kind == 6)?3u:1u);
                    ++fault){
                    wirelog_error_t error;
                    wirelog_program_t *program =
                        wirelog_parse_string(programs[kind], &error);
                    wl_plan_t *plan = NULL;
                    CHECK(program && !wl_plan_from_program(program, &plan));
                    wl_columnar_memory_resolution_t resolution =
                    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                     .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
                     .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
                    wl_columnar_memory_governor_ref_t *governor =
                        wl_columnar_memory_governor_ref_create(&resolution);
                    CHECK(governor);
                    wl_evaluation_control_t *control = NULL;
                    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                        &control));
                    wl_session_options_t options;
                    wl_session_options_init(&options);
                    options.evaluation_control = control;
                    options.memory_governor = governor;
                    wl_session_t *s = NULL, *raw = NULL;
                    CHECK(!wl_session_create_with_options(wl_backend_columnar(),
                        plan, 1, &options, &s));
                    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1,
                        &raw));
                    populate(s, kind); populate(raw, kind);
                    physical_t a[2] = {0}, b[2] = {0};
                    stamp(&a[0], s, kind == 6?"edge":"input", 0);
                    stamp(&b[0], raw, kind == 6?"edge":"input", 0);
                    bool rhs = kind == 1 || kind == 2 || kind == 3 || kind == 5;
                    if (rhs){
                        stamp(&a[1], s, "rhs", 1); stamp(&b[1], raw, "rhs", 1);
                    }
                    capture_t events = {0}, reference_events = {0}, image = {0},
                        reference_image = {0}, literal = {0};
                    if (observed) {
                        wl_session_set_delta_cb(s, on_delta, &events);
                        wl_session_set_delta_cb(raw, on_delta,
                            &reference_events);
                    }
                    active_session = s; raw_active = raw; raw_deltas = 0;
                    active_kind = kind; filter_witnesses = projections = 0;
                    wl_columnar_eval_stack_test_physical_publication =
                        publication;
                    wl_columnar_eval_serial_test_after_delta_stamp =
                        derived_delta;
                    evaluate(s, snapshot, &image, &events, kind == 6?0:fault);
                    CHECK(!(snapshot?wl_session_snapshot(raw, on_tuple,
                        &reference_image):step(raw)));
                    unchanged(&a[0]); unchanged(&b[0]); if (rhs) {
                        unchanged(&a[1]); unchanged(&b[1]);
                    }
                    CHECK(!session_find_rel((wl_col_session_t *)s,
                        active_kind == 6?"path":"output")->timestamps);
                    if (!kind) CHECK(filter_witnesses);
                    if (kind != 6) CHECK(projections);
                    if (kind == 6) CHECK(raw_deltas);
                    expected(kind, &literal); if (snapshot) {
                        equal_capture(&image, &literal);
                        equal_capture(&image, &reference_image);
                    }
                    if (observed) {
                        equal_capture(&events, &literal);
                        /* Evaluating raw SNAPSHOT emits its tuple image; it does not
                         * run the STEP notification transaction used by this bounded
                         * host. Qualify that disposition explicitly. */
                        if (!snapshot) equal_capture(&events,
                                &reference_events);
                        else CHECK(!reference_events.count);
                    }
                    capture_t final = {0};
                    CHECK(!wl_session_snapshot(s, on_tuple, &final));
                    equal_capture(&final, &literal); unchanged(&a[0]);
                    /* Remove a real timestamped source row after a completed host.
                     * Scalar projection and recursive reconstruction retain the
                     * surviving physical source records and publish signed losses. */
                    if (kind == 0 || kind == 6) {
                        events.count = reference_events.count = 0;
                        image.count = reference_image.count = 0;
                        int64_t removed[] = {2, 3};
                        CHECK(!wl_session_remove(s, kind == 6?"edge":"input",
                            removed, 1, kind == 6?2:1));
                        CHECK(!wl_session_remove(raw, kind == 6?"edge":"input",
                            removed, 1, kind == 6?2:1));
                        col_rel_t *retraction =
                            session_find_rel((wl_col_session_t *)s,
                                kind == 6?"$r$edge":"$r$input");
                        if (retraction) {
                            CHECK(retraction->timestamps &&
                                retraction->nrows == 1);
                            unsigned selected = kind == 6?1:3;
                            equal_stamp(&retraction->timestamps[0],
                                &a[0].stamps[selected]);
                            for (unsigned c = 0; c < a[0].width; ++c)
                                CHECK(retraction->columns[c][0] == removed[c]);
                        }
                        col_rel_t *old = session_find_rel((wl_col_session_t *)s,
                                active_kind == 6?"path":"output");
                        col_relation_snapshot_t old_image =
                            wl_columnar_relation_snapshot(old);
                        CHECK(!old->timestamps);
                        col_rel_t *pending =
                            session_find_rel((wl_col_session_t *)s,
                                active_kind == 6?"edge":"input");
                        col_relation_snapshot_t pending_image =
                            wl_columnar_relation_snapshot(pending);
                        col_delta_timestamp_t pending_stamps[6];
                        CHECK(pending->nrows <= 6);
                        memcpy(pending_stamps, pending->timestamps,
                            pending->nrows*sizeof(*pending_stamps));
                        stop_mode = fault; stop_hit = false;
                        int removal_rc = step(s);
                        if (removal_rc && !fault) fprintf(stderr,
                                "removal kind=%u snapshot=%u observed=%u rc=%d\n",
                                kind, snapshot, observed, removal_rc);
                        if (fault) {
                            CHECK(stop_hit &&
                                removal_rc ==
                                (fault ==
                                1?WL_EVALUATION_CONTROL_CANCELLED:
                                WL_EVALUATION_CONTROL_WORK_LIMIT));
                            CHECK(!events.count &&
                                session_find_rel((wl_col_session_t *)s,
                                active_kind == 6?"path":"output") == old);
                            CHECK(wl_columnar_relation_snapshot_equal(old_image,
                                wl_columnar_relation_snapshot(old)));
                            CHECK(wl_columnar_relation_snapshot_equal(
                                    pending_image,
                                    wl_columnar_relation_snapshot(pending)));
                            for (unsigned r = 0; r < pending->nrows;
                                ++r) equal_stamp(&pending->timestamps[r],
                                    &pending_stamps[r]);
                            wl_evaluation_control_report_t report = {0},
                                frozen = {0};
                            CHECK(!wl_evaluation_control_report(control,
                                &report));
                            CHECK(report.result == removal_rc &&
                                !report.cleanup_status);
                            CHECK(!wl_evaluation_control_report(control,
                                &frozen));
                            CHECK(!memcmp(&report, &frozen, sizeof(report)));
                            CHECK(!wl_evaluation_control_reset(control));
                            stop_mode = 0; CHECK(!step(s));
                        }else CHECK(!removal_rc);
                        CHECK(!step(raw));
                        CHECK(session_find_rel((wl_col_session_t *)s,
                            active_kind == 6?"path":"output") != old
                            || !wl_columnar_relation_snapshot_equal(old_image,
                            wl_columnar_relation_snapshot(old)));
                        CHECK(!wl_session_snapshot(s, on_tuple, &image));
                        CHECK(!wl_session_snapshot(raw, on_tuple,
                            &reference_image));
                        equal_capture(&image, &reference_image);
                        capture_t remaining = {0};
                        if (!kind) {
                            int64_t x = 1, y = 3;
                            on_tuple("output", &x, 1, &remaining);
                            on_tuple("output", &y, 1, &remaining);
                        }else {
                            int64_t a[] = {1, 2}, b[] = {3, 4};
                            on_tuple("output", a, 2, &remaining);
                            on_tuple("output", b, 2, &remaining);
                        }
                        equal_capture(&image, &remaining);
                        if (observed) equal_capture(&events, &reference_events);
                        col_rel_t *source =
                            session_find_rel((wl_col_session_t *)s,
                                active_kind == 6?"edge":"input");
                        CHECK(source->timestamps &&
                            source->nrows == a[0].rows-1);
                        unsigned survivors = 0;
                        for (unsigned r = 0; r < source->nrows; ++r) {
                            bool found = false;
                            for (unsigned i = 0; i < a[0].rows;
                                ++i) if (a[0].stamps[i].iteration ==
                                    source->timestamps[r].iteration) {
                                    CHECK(a[0].cells[i*a[0].width] != 2);
                                    equal_stamp(&source->timestamps[r],
                                        &a[0].stamps[i]);
                                    for (unsigned c = 0; c < a[0].width;
                                        ++c) CHECK(source->columns[c][r] ==
                                            a[0].cells[i*a[0].width+c]);
                                    found = true; ++survivors; break;
                                }
                            CHECK(found);
                        }
                        CHECK(survivors == source->nrows);
                    }
                    wl_columnar_eval_stack_test_physical_publication = NULL;
                    wl_columnar_eval_serial_test_after_delta_stamp = NULL;
                    active_session = raw_active = NULL;
                    CHECK(!wl_session_try_destroy(&s)); wl_session_destroy(raw);
                    wl_evaluation_control_release(control); wl_plan_free(plan);
                    wirelog_program_free(program);
                    CHECK(!wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(governor)));
                    wl_columnar_memory_governor_ref_release(governor);
                }
    puts(
        "physical sessions: source/FILTER stamps, set projections and wrapper retry passed");
    return 0;
}
