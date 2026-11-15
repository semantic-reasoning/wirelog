/* Public compiler/session JOIN histories. Internal components are tested elsewhere. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

typedef struct { int64_t x, y; } pair_t;
typedef struct { pair_t rows[64]; unsigned count; } image_t;
typedef struct { pair_t row; int sign; } event_t;
typedef struct { event_t rows[64]; unsigned count; } events_t;
static wl_evaluation_control_t *active;
static unsigned filtered, private_rows, persistent_rows, probes, full, delta;
static unsigned stop_kind, produced;
static bool stopped, string_output;

static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}

static void
checkpoint(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    CHECK(control == active);
    if (phase == 7) {
        if (progress) ++delta; else ++full;
    }
    if (phase == 28) ++private_rows;
    if (phase == 47) ++persistent_rows;
    if (phase == 17 || phase == 48) ++probes;
    if (phase != 44) return;
    ++filtered;
    /* r=1 has completed two genuine RHS evaluations, including rejected work.
    * This is a real stop injection, not a primitive exact-boundary oracle. */
    if (!stop_kind || stopped || progress != 1) return;
    stopped = true;
    if (stop_kind == 1) wl_evaluation_control_request_cancel(control);
    else {
        uint64_t id = wl_evaluation_control_active_attempt_id(control);
        CHECK(wl_evaluation_control_charge_attempt(control, id, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    }
}

static void
pipeline_checkpoint(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t work)
{
    (void)generation; (void)extent; (void)work;
    if (control == active && event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE)
        ++produced;
}

static int
finish(wl_session_t *session)
{
    for (unsigned turn = 0; turn < 100000; ++turn) {
        int rc = wl_session_step(session);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

static bool
equal(pair_t a, pair_t b)
{
    return a.x == b.x && a.y == b.y;
}

static bool
contains(const image_t *image, pair_t row)
{
    for (unsigned i = 0; i < image->count; ++i)
        if (equal(image->rows[i], row)) return true;
    return false;
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int32_t sign,
    void *arg)
{
    events_t *events = arg;
    CHECK(!strcmp(name, "output") && width == 2 && (sign == 1 || sign == -1)
        && events->count < 64);
    events->rows[events->count++] = (event_t){{row[0], row[1]}, sign};
}

static void
check_image(wl_session_t *session, const image_t *expected)
{
    const col_rel_t *out = session_find_rel(COL_SESSION(session), "output");
    CHECK(out && out->ncols == 2 && out->nrows == expected->count);
    if (out->nrows) {
        CHECK((out->column_types ? out->column_types[0] : WIRELOG_TYPE_INT64)
            == WIRELOG_TYPE_INT64);
        CHECK((out->column_types ? out->column_types[1] : WIRELOG_TYPE_INT64)
            == (string_output ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64));
    }
    image_t found = {0};
    for (uint32_t r = 0; r < out->nrows; ++r) {
        pair_t row = {col_rel_get(out, r, 0), col_rel_get(out, r, 1)};
        CHECK(contains(expected, row) && !contains(&found, row));
        found.rows[found.count++] = row;
    }
}

static void
check_events(events_t *events, const image_t *before, const image_t *after)
{
    unsigned expected = 0;
    const image_t *images[] = {before, after};
    for (unsigned side = 0; side < 2; ++side)
        for (unsigned i = 0; i < images[side]->count; ++i) {
            pair_t row = images[side]->rows[i];
            if (contains(images[1 - side], row)) continue;
            ++expected;
            unsigned hits = 0;
            for (unsigned e = 0; e < events->count; ++e)
                hits += equal(events->rows[e].row, row)
                    && events->rows[e].sign == (side ? 1 : -1);
            CHECK(hits == 1);
        }
    CHECK(events->count == expected);
    events->count = 0;
}

/* Literal relation-qualified reference: accepted RHS values are prescribed by
 * fixture data, without running the production predicate or raw JOIN as oracle.
 * Three keyed left rows share key1; product admits the key2 row as well. */
static image_t
expected(bool product, bool added, bool removed, bool extra_left, int64_t y0,
    int64_t y1, int64_t y2)
{
    image_t image = {0};
    const int64_t left[] = {10, 11, 20, 12};
    for (unsigned l = 0; l < (extra_left ? 4u : 3u); ++l) {
        if (product || l != 2) {
            if (!removed) image.rows[image.count++] = (pair_t){left[l], y0};
            image.rows[image.count++] = (pair_t){left[l], y1};
        }
        if (added && (product || l == 2))
            image.rows[image.count++] = (pair_t){left[l], y2};
    }
    return image;
}

static void
run(unsigned kind, bool product, bool batch, unsigned stop)
{
    string_output = kind == 1 || kind == 2;
    const char *type = kind == 1 || kind == 2 ? "string" : "int64";
    const char *flag_type = kind == 1 ? "string" : "int64";
    const char *flag = kind == 1 ? "\"keep\"" : kind == 3 ? "flag" : "1";
    const char *predicate = kind == 2 ? ",strlen(y)>2" : "";
    char source[512];
    snprintf(source, sizeof(source), ".decl a(k:int64,x:int64)\n"
        ".decl b(j:int64,y:%s,flag:%s)\n.decl output(x:int64,y:%s)\n"
        "output(x,y):-a(k,x),b(%s,y,%s)%s.\n", type, flag_type, type,
        product ? "j" : "k", flag, predicate);
    configure("WIRELOG_JOIN_BATCH_BYTES", batch ? "256" : NULL);
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *head = &plan->strata[0].relations[0];
    const wl_plan_op_t *join = NULL;
    for (uint32_t o = 0; o < head->op_count; ++o)
        if (head->ops[o].op == WL_PLAN_OP_JOIN) {
            CHECK(!join); join = &head->ops[o];
        }
    CHECK(join && !strcmp(join->right_relation, "b")
        && join->key_count == (product ? 0u : 1u)
        && (join->right_filter_expr.size != 0) == (kind != 3));
    int64_t ys[] = {2, 4, 6, 5};
    if (kind == 1 || kind == 2) {
        const char *strings[] = {"keep", "also", "next", "x"};
        for (unsigned i = 0; i < 4; ++i) {
            ys[i] = wl_intern_put(plan->intern, strings[i]); CHECK(ys[i] >= 0);
        }
    }
    int64_t keep = kind == 1 ? wl_intern_put(plan->intern, "keep") : 1;
    int64_t reject = kind == 1 ? wl_intern_put(plan->intern, "reject") : 0;
    CHECK(keep >= 0 && reject >= 0);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(100000000), &control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control; options.memory_governor = governor;
    wl_session_t *session = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    events_t events = {0}; wl_session_set_delta_cb(session, capture, &events);
    const int64_t left[] = {1, 10, 1, 11, 2, 20, 1, 10};
    for (unsigned i = 0; i < 2; ++i) {
        wl_session_t *s = i ? raw : session;
        CHECK(!wl_session_insert(s, "a", left, 4, 2));
    }
    active = control; stop_kind = 0; stopped = false;
    filtered = private_rows = persistent_rows = probes = full = delta =
        produced = 0;
    wl_columnar_join_test_attempt_checkpoint = checkpoint;
    wl_columnar_join_pipeline_test_numeric_checkpoint = pipeline_checkpoint;
    /* No RHS insertion: assert registry fact rather than calling it an
     * absent selector merely because no rows were inserted. */
    const col_rel_t *right0 = session_find_rel(COL_SESSION(session), "b");
    CHECK(!right0 || !right0->nrows);
    CHECK(!finish(session) && !finish(raw));
    image_t old = {0}; check_image(session, &old); check_image(raw, &old);
    check_events(&events, &old, &old);
    CHECK(session_find_rel(COL_SESSION(session), "b")
        && !session_find_rel(COL_SESSION(session), "b")->nrows);
    if (kind != 3) {
        for (unsigned i = 0; i < 2; ++i)
            CHECK(!wl_session_insert(i ? raw : session, "b",
                (int64_t[]){2, ys[3], reject}, 1, 3));
        unsigned before = filtered;
        CHECK(!finish(session) && !finish(raw));
        CHECK(filtered > before);
        check_image(session, &old); check_image(raw, &old);
        check_events(&events, &old, &old);
    }
    for (unsigned stage = 0; stage < 4; ++stage) {
        int64_t rows[] = {1, ys[0], keep, 1, ys[1], keep,
                          1, ys[0], keep};
        for (unsigned i = 0; i < 2; ++i) {
            wl_session_t *s = i ? raw : session;
            if (!stage) {
                /* Duplicate input does not authorize duplicate public events.
                * The unfiltered control intentionally omits rejected data. */
                CHECK(!wl_session_insert(s, "b", rows, kind == 3 ? 2 : 3, 3));
            } else if (stage == 1) {
                CHECK(!wl_session_insert(s, "b", (int64_t[]){2, ys[2], keep}, 1,
                    3));
            } else if (stage == 2) {
                CHECK(!wl_session_remove(s, "b", (int64_t[]){1, ys[0], keep,
                                                             1, ys[0], keep},
                    kind == 3 ? 1 : 2, 3));
            } else CHECK(!wl_session_insert(s, "a", (int64_t[]){1, 12}, 1, 2));
        }
        unsigned filter_before = filtered;
        if (stage == 2 && stop) {
            stop_kind = stop; stopped = false;
            col_rel_t *public_head = session_find_rel(COL_SESSION(session),
                    "output");
            CHECK(public_head && old.count &&
                COL_SESSION(session)->pending_input_change);
            int rc = finish(session);
            CHECK(stopped && filtered > filter_before && rc == (stop == 1
                ? WL_EVALUATION_CONTROL_CANCELLED :
                WL_EVALUATION_CONTROL_WORK_LIMIT));
            CHECK(session_find_rel(COL_SESSION(session),
                "output") == public_head
                && COL_SESSION(session)->pending_input_change && !events.count);
            check_image(session, &old);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(control, &report)
                && report.result == rc && report.consumed &&
                !report.cleanup_status);
            CHECK(!wl_evaluation_control_reset(control));
            stop_kind = 0;
        }
        CHECK(!finish(session) && !finish(raw));
        image_t now = expected(product, stage >= 1, stage >= 2, stage >= 3,
                ys[0], ys[1], ys[2]);
        check_image(session, &now); check_image(raw, &now);
        check_events(&events, &old, &now); old = now;
        CHECK(!finish(session) && !finish(raw));
        check_image(session, &old); check_events(&events, &old, &old);
    }
    CHECK(full && probes);
    /* Phase7 exposes only used_delta; no source-identity claim follows. */
    fprintf(stdout, "kind=%u product=%u batch=%u stop=%u full=%u delta=%u "
        "filter=%u private=%u persistent=%u probes=%u\n", kind, product,
        batch, stop, full, delta, filtered, private_rows, persistent_rows,
        probes);
    if (kind != 3) CHECK(filtered && (!product ? private_rows : true));
    if (kind == 3 && !product) CHECK(batch ? produced : persistent_rows);
    wl_columnar_join_test_attempt_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    CHECK(!wl_session_try_destroy(&session) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    bool batch = !strcmp(argv[1], "batch");
    CHECK(batch || !strcmp(argv[1], "plain"));
    for (unsigned kind = 0; kind < 4; ++kind)
        for (unsigned product = 0; product < 2; ++product)
            for (unsigned stop = 0; stop < (kind == 3 ? 1u : 3u); ++stop)
                run(kind, product, batch, stop);
    puts(
        "public JOIN numeric/string histories, private/persistent destinations and retry passed");
    return 0;
}
