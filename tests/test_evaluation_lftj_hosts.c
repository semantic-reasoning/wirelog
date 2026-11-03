/* Real session wrappers over compiler-selected LFTJ. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static int
finish_step(wl_session_t *s)
{
    uint64_t attempt = 0;
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(s);
        if (rc != EAGAIN) return rc;
        uint64_t active = wl_evaluation_control_active_attempt_id(
            s->evaluation_control);
        CHECK(active && (!attempt || attempt == active));
        attempt = active;
    }
    CHECK(false);
    return EIO;
}

static void
test_session_initial(void)
{
    const char *text = ".decl a(x:int64)\n.decl b(x:int64)\n"
        ".decl c(x:int64)\n.decl output(x:int64)\n"
        "output(x) :- a(x), b(x), c(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *head = &plan->strata[0].relations[0];
    CHECK(head->ops[0].op == WL_PLAN_OP_LFTJ);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *session = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    const int64_t values[] = {3, 1, 2, 2};
    const char *names[] = {"a", "b", "c"};
    for (unsigned i = 0; i < 3; ++i)
        CHECK(!wl_session_insert(session, names[i], values, 4, 1));
    int rc = finish_step(session);
    if (rc) fprintf(stderr, "LFTJ initial session rc=%d\n", rc);
    CHECK(!rc);
    col_rel_t *out = session_find_rel((wl_col_session_t *)session, "output");
    CHECK(out && out->nrows == 3 && out->ncols == 1);
    CHECK(out->columns[0][0] == 1 && out->columns[0][1] == 2
        && out->columns[0][2] == 3);
    CHECK(!wl_session_try_destroy(&session));
    wl_evaluation_control_release(control);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

typedef struct { int64_t cells[4]; int diff; } tuple_t;
typedef struct { tuple_t rows[128]; unsigned count; } capture_t;
typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
    wl_session_t *session, *raw;
    int64_t data[3][4 * 25];
    unsigned widths[3], counts[3];
    bool floating;
    capture_t events;
} fixture_t;

typedef struct {
    col_rel_t *relation;
    col_relation_snapshot_t snapshot;
    int64_t values[4 * 25];
    col_delta_timestamp_t stamps[4];
    unsigned rows, width;
    bool timestamped;
} source_image_t;

static source_image_t
image(wl_session_t *s, unsigned source)
{
    char name[2] = {(char)('a' + source), 0};
    source_image_t out = {0};
    out.relation = session_find_rel((wl_col_session_t *)s, name);
    CHECK(out.relation && out.relation->nrows <= 4
        && out.relation->ncols <= 25);
    out.rows = out.relation->nrows; out.width = out.relation->ncols;
    out.timestamped = out.relation->timestamps != NULL;
    out.snapshot = wl_columnar_relation_snapshot(out.relation);
    for (unsigned r = 0; r < out.rows; ++r) {
        if (out.timestamped) out.stamps[r] = out.relation->timestamps[r];
        for (unsigned c = 0; c < out.width; ++c)
            out.values[r * out.width + c] = out.relation->columns[c][r];
    }
    return out;
}

static void
unchanged(wl_session_t *s, unsigned source, const source_image_t *before,
    bool exact)
{
    source_image_t after = image(s, source);
    CHECK(after.rows == before->rows && after.width == before->width
        && after.timestamped == before->timestamped);
    if (exact) CHECK(after.relation == before->relation
            && wl_columnar_relation_snapshot_equal(before->snapshot,
            after.snapshot));
    size_t cells = (size_t)after.rows * after.width;
    CHECK(cells <= sizeof(after.values) / sizeof(after.values[0]));
    CHECK(!memcmp(after.values, before->values,
        cells * sizeof(after.values[0])));
    for (unsigned r = 0; after.timestamped && r < after.rows; ++r) {
        const col_delta_timestamp_t *a = &after.stamps[r],
            *b = &before->stamps[r];
        CHECK(a->iteration == b->iteration && a->stratum == b->stratum
            && a->worker == b->worker && a->_reserved == b->_reserved
            && a->multiplicity == b->multiplicity);
    }
}

static void
on_delta(const char *name, const int64_t *row, uint32_t width, int diff,
    void *data)
{
    if (strcmp(name, "output")) return;
    capture_t *c = data;
    CHECK(width == 4 && c->count < 128);
    tuple_t *t = &c->rows[c->count++];
    memcpy(t->cells, row, sizeof(t->cells));
    t->diff = diff;
}

static void
on_tuple(const char *name, const int64_t *row, uint32_t width, void *data)
{
    on_delta(name, row, width, 1, data);
}

static int
compare_tuple(const void *a, const void *b)
{
    const tuple_t *x = a, *y = b;
    for (unsigned i = 0; i < 4; ++i)
        if (x->cells[i] != y->cells[i])
            return (x->cells[i] > y->cells[i]) - (x->cells[i] < y->cells[i]);
    return (x->diff > y->diff) - (x->diff < y->diff);
}

static void
equal(capture_t *a, capture_t *b)
{
    CHECK(a->count == b->count);
    qsort(a->rows, a->count, sizeof(*a->rows), compare_tuple);
    qsort(b->rows, b->count, sizeof(*b->rows), compare_tuple);
    for (unsigned i = 0; i < a->count; ++i)
        CHECK(!compare_tuple(&a->rows[i], &b->rows[i]));
}

static void
text_add(char *out, size_t capacity, size_t *used, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    int n = vsnprintf(out + *used, capacity - *used, format, args);
    va_end(args);
    CHECK(n >= 0 && (size_t)n < capacity - *used);
    *used += (size_t)n;
}

static int64_t
key_bits(bool floating, unsigned key)
{
    if (!floating) return key;
    double value = key;
    int64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

static void
fixture_init(fixture_t *f, bool wide, bool floating, bool observed,
    bool timestamps, bool empty)
{
    memset(f, 0, sizeof(*f));
    f->floating = floating;
    char text[8192] = {0}; size_t used = 0;
    for (unsigned s = 0; s < 3; ++s) {
        unsigned width = wide ? (s ? 24 : 25) : 2;
        f->widths[s] = width;
        f->counts[s] = empty && s == 1 ? 0 : 4;
        text_add(text, sizeof(text), &used, ".decl %c(", 'a' + s);
        for (unsigned col = 0; col < width; ++col) {
            bool key = col == (s ? 0 : width - 1);
            text_add(text, sizeof(text), &used, "%sp%u:%s",
                col ? "," : "", col, key && floating ? "float" : "int64");
        }
        text_add(text, sizeof(text), &used, ")\n");
        for (unsigned row = 0; row < f->counts[s]; ++row)
            for (unsigned col = 0; col < width; ++col) {
                static const unsigned keys[] = {3, 1, 2, 2};
                f->data[s][row * width + col] =
                    col == (s ? 0 : width - 1)
                    ? key_bits(floating, keys[row])
                    : (int64_t)(10 * (row + 1) + s + col);
            }
    }
    text_add(text, sizeof(text), &used,
        ".decl output(k:%s,v:int64,w:int64,z:int64)\n"
        "output(k,v,w,z) :- ", floating ? "float" : "int64");
    for (unsigned s = 0; s < 3; ++s) {
        text_add(text, sizeof(text), &used, "%s%c(", s ? ", " : "", 'a'+s);
        for (unsigned col = 0; col < f->widths[s]; ++col) {
            const char *name = col == (s ? 0 : f->widths[s] - 1) ? "k"
                : col == (s ? 1 : 0) ? (s == 0 ? "v" : s == 1 ? "w" : "z")
                : "_";
            text_add(text, sizeof(text), &used, "%s%s", col ? "," : "", name);
        }
        text_add(text, sizeof(text), &used, ")");
    }
    text_add(text, sizeof(text), &used, ".\n");
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    CHECK(f->plan->stratum_count == 1);
    CHECK(f->plan->strata[0].relations[0].ops[0].op == WL_PLAN_OP_LFTJ);
    CHECK(!wl_evaluation_control_create(0, &f->control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->session));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->raw));
    if (observed) wl_session_set_delta_cb(f->session, on_delta, &f->events);
    for (unsigned s = 0; s < 3; ++s) {
        char name[2] = {(char)('a'+s), 0};
        wl_session_t *sessions[] = {f->session, f->raw};
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(!wl_session_insert(sessions[i], name, f->data[s],
                f->counts[s], f->widths[s]));
            if (!f->counts[s]) {
                /* Establish the accepted empty relation schema through the
                 * API; an uninitialized zero-column source is a raw error. */
                CHECK(!wl_session_insert(sessions[i], name, f->data[s], 1,
                    f->widths[s]));
                CHECK(!wl_session_remove(sessions[i], name, f->data[s], 1,
                    f->widths[s]));
            }
            if (timestamps && f->counts[s]) {
                col_rel_t *rel =
                    session_find_rel((wl_col_session_t *)sessions[i],
                        name);
                CHECK(rel && !col_rel_enable_timestamps(rel));
                for (unsigned r = 0; r < rel->nrows; ++r)
                    rel->timestamps[r] = (col_delta_timestamp_t){
                        .iteration = 100-r, .stratum = s, .worker = r,
                        .multiplicity = r ? (int64_t)r-2 : -4
                    };
                wl_columnar_relation_touch_storage(rel);
            }
        }
    }
}

static void
fixture_destroy(fixture_t *f)
{
    CHECK(!wl_session_try_destroy(&f->session));
    CHECK(!wl_session_try_destroy(&f->raw));
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    /* Both sessions and the program share the intern table. Its reservation
     * is live until the last program/session owner is released. */
    uint64_t reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->governor));
    if (reserved) fprintf(stderr, "LFTJ teardown retained=%llu\n",
            (unsigned long long)reserved);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
    wl_evaluation_control_release(f->control);
}

/* Independent literal equijoin over the fixture's input arrays. */
static capture_t
oracle(const fixture_t *f)
{
    capture_t out = {0};
    for (unsigned a = 0; a < f->counts[0]; ++a)
        for (unsigned b = 0; b < f->counts[1]; ++b)
            for (unsigned c = 0; c < f->counts[2]; ++c) {
                int64_t key = f->data[0][a * f->widths[0]+f->widths[0]-1];
                if (key != f->data[1][b * f->widths[1]]
                    || key != f->data[2][c * f->widths[2]]) continue;
                tuple_t *t = &out.rows[out.count++];
                t->cells[0] = key;
                t->cells[1] = f->data[0][a * f->widths[0]];
                t->cells[2] = f->data[1][b * f->widths[1]+1];
                t->cells[3] = f->data[2][c * f->widths[2]+1];
                t->diff = 1;
            }
    return out;
}

static capture_t
head_capture(wl_session_t *base)
{
    col_rel_t *head = session_find_rel((wl_col_session_t *)base, "output");
    CHECK(head && head->ncols == 4 && !head->timestamps);
    capture_t c = {0};
    for (unsigned r = 0; r < head->nrows; ++r) {
        int64_t row[4];
        for (unsigned col = 0; col < 4; ++col) row[col] = head->columns[col][r];
        on_delta("output", row, 4, 1, &c);
    }
    return c;
}

static void
head_schema_equal(const fixture_t *f)
{
    const col_rel_t *actual = session_find_rel((wl_col_session_t *)f->session,
            "output");
    const col_rel_t *raw = session_find_rel((wl_col_session_t *)f->raw,
            "output");
    CHECK(actual && raw && actual->ncols == 4 && raw->ncols == 4
        && actual->schema_ok && raw->schema_ok
        && actual->declared_ncols == raw->declared_ncols
        && actual->has_graph_column == raw->has_graph_column
        && actual->graph_col_idx == raw->graph_col_idx
        && actual->column_types && raw->column_types
        && actual->schema.n_children == raw->schema.n_children);
    for (unsigned c = 0; c < 4; ++c) {
        char name[16]; snprintf(name, sizeof(name), "col%u", c);
        wirelog_column_type_t expected = !c &&
            f->floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
        CHECK(!strcmp(actual->col_names[c], name) && !strcmp(raw->col_names[c],
            name)
            && actual->column_types[c] == expected &&
            raw->column_types[c] == expected
            && !strcmp(actual->schema.children[c]->format,
            raw->schema.children[c]->format));
    }
}

static void
test_session_domains(void)
{
    for (unsigned wide = 0; wide < 2; ++wide)
        for (unsigned floating = 0; floating < 2; ++floating)
            for (unsigned observed = 0; observed < 2; ++observed)
                for (unsigned snapshot = 0; snapshot < 2; ++snapshot)
                    for (unsigned empty = 0; empty < 2; ++empty) {
                        fixture_t f;
                        fixture_init(&f, wide, floating, observed, true, empty);
                        source_image_t before[3];
                        for (unsigned s = 0; s < 3; ++s)
                            before[s] = image(f.session, s);
                        capture_t expected = oracle(&f), captured = {0};
                        fprintf(stderr,
                            "domains wide=%u float=%u observed=%u snapshot=%u empty=%u\n",
                            wide, floating, observed, snapshot, empty);
                        CHECK(expected.count == (empty ? 0u : 10u));
                        if (snapshot)
                            CHECK(!wl_session_snapshot(f.session, on_tuple,
                                &captured));
                        else {
                            int rc = finish_step(f.session);
                            if (rc) fprintf(stderr, "domains rc=%d\n", rc);
                            CHECK(!rc);
                        }
                        capture_t actual = head_capture(f.session);
                        equal(&actual, &expected);
                        if (snapshot) equal(&captured, &expected);
                        if (observed) equal(&f.events, &expected);
                        CHECK(!finish_step(f.raw));
                        head_schema_equal(&f);
                        actual = head_capture(f.raw);
                        equal(&actual, &expected);
                        CHECK(!((wl_col_session_t *)f.session)->sarr_count);
                        for (unsigned s = 0; s < 3; ++s)
                            unchanged(f.session, s, &before[s], false);
                        /* No-op repeat is stable and does not replay events. */
                        unsigned delivered = f.events.count;
                        CHECK(!finish_step(f.session));
                        CHECK(f.events.count == delivered);
                        fixture_destroy(&f);
                    }
}

static bool
contains(const capture_t *c, const tuple_t *t)
{
    for (unsigned i = 0; i < c->count; ++i)
        if (!memcmp(c->rows[i].cells, t->cells, sizeof(t->cells))) return true;
    return false;
}

static capture_t
event_difference(const capture_t *before, const capture_t *after)
{
    capture_t out = {0};
    for (unsigned i = 0; i < before->count; ++i)
        if (!contains(after, &before->rows[i])) {
            out.rows[out.count] = before->rows[i];
            out.rows[out.count++].diff = -1;
        }
    for (unsigned i = 0; i < after->count; ++i)
        if (!contains(before, &after->rows[i]))
            out.rows[out.count++] = after->rows[i];
    return out;
}

static void
test_successors(void)
{
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned snapshot = 0; snapshot < 2; ++snapshot) {
            fixture_t f;
            fixture_init(&f, true, false, observed, true, false);
            capture_t prior = {0};
            for (unsigned cycle = 0; cycle < 3; ++cycle) {
                f.events.count = 0;
                if (cycle == 1) {
                    for (unsigned s = 0; s < 2; ++s)
                        CHECK(!wl_session_remove(s ? f.raw : f.session,
                            "a", f.data[0] + 3 * f.widths[0], 1, f.widths[0]));
                    f.counts[0] = 3;
                } else if (cycle == 2) {
                    f.data[0][3 * f.widths[0]] = 999;
                    for (unsigned s = 0; s < 2; ++s)
                        CHECK(!wl_session_insert(s ? f.raw : f.session,
                            "a", f.data[0] + 3 * f.widths[0], 1, f.widths[0]));
                    f.counts[0] = 4;
                }
                capture_t expected = oracle(&f), captured = {0};
                if (snapshot) CHECK(!wl_session_snapshot(f.session, on_tuple,
                        &captured));
                else CHECK(!finish_step(f.session));
                capture_t actual = head_capture(f.session);
                equal(&actual, &expected);
                CHECK(expected.count == (cycle == 1 ? 6u : 10u));
                if (observed) {
                    capture_t events = event_difference(&prior, &expected);
                    equal(&f.events, &events);
                }
                CHECK(!finish_step(f.raw));
                actual = head_capture(f.raw);
                equal(&actual, &expected);
                prior = expected;
            }
            fixture_destroy(&f);
        }
}

static fixture_t *stopping;
static unsigned stop_target, stop_work;
static bool stop_hit;
static unsigned phase_hits[WL_COLUMNAR_LFTJ_ATTEMPT_DONE + 1];

static void
stop_checkpoint(wl_evaluation_control_t *control,
    const wl_columnar_lftj_attempt_t *a)
{
    CHECK(stopping && control == stopping->control);
    CHECK(a->attempt_id == wl_evaluation_control_active_attempt_id(control));
    ++phase_hits[a->phase];
    if (stop_hit || (unsigned)a->phase != stop_target) return;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_COPY
        && (!a->inputs || !a->inputs[0].copy_cell)) return;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_SCHEMA && !a->fill_col) return;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_BEGIN)
        CHECK(a->output && a->output_owned && !a->output->column_types
            && !a->output->timestamps && !a->registered);
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_SORT
        && !a->inputs[0].sort_run && !a->inputs[0].sort_copying) return;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_JOIN
        && (!a->output_pass ||
        a->join_phase != WL_COLUMNAR_LFTJ_JOIN_PRODUCT_EMIT
        || a->fill_input || !a->fill_col)) return;
    stop_hit = true;
    if (stop_work) CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
test_stops(void)
{
    const unsigned targets[] = {
        WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE, WL_COLUMNAR_LFTJ_ATTEMPT_ACQUIRE,
        WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC_PRECHARGE,
        WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC,
        WL_COLUMNAR_LFTJ_ATTEMPT_SCHEMA, WL_COLUMNAR_LFTJ_ATTEMPT_COPY,
        WL_COLUMNAR_LFTJ_ATTEMPT_SORT, WL_COLUMNAR_LFTJ_ATTEMPT_JOIN,
        WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_CREATE,
        WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_BEGIN,
        WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_END,
        WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE
    };
    for (unsigned target = 0; target < sizeof(targets)/sizeof(targets[0]);
        ++target)
        for (unsigned work = 0; work < 2; ++work) {
            fixture_t f;
            fixture_init(&f, true, false, true, true, false);
            CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_C(1000000000)));
            source_image_t before[3];
            for (unsigned s = 0; s < 3; ++s) before[s] = image(f.session, s);
            wl_col_session_t *session = (wl_col_session_t *)f.session;
            col_rel_t **registry = session->rels;
            uint32_t registered = session->nrels;
            uint64_t reserved = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(f.governor));
            stopping = &f; stop_target = targets[target]; stop_work = work;
            stop_hit = false;
            wl_columnar_lftj_test_attempt_checkpoint = stop_checkpoint;
            int rc = finish_step(f.session);
            wl_columnar_lftj_test_attempt_checkpoint = NULL;
            stopping = NULL;
            CHECK(stop_hit && rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(!f.events.count && !session_find_rel(session, "output"));
            CHECK(session->rels == registry && session->nrels == registered);
            uint64_t after_reserved = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(f.governor));
            /* STEP work exhaustion may retain the prepared host image for
            * retry. The LFTJ child is drained; retained host readers still
            * protect input mutation. Cancellation discards that image. */
            if (work) {
                CHECK(after_reserved >= reserved);
                if (after_reserved > reserved)
                    CHECK(wl_session_insert(f.session, "a", f.data[0], 1,
                        f.widths[0]) == EBUSY);
            } else CHECK(after_reserved == reserved);
            CHECK(!session->cleanup_active && !session->cleanup_pending
                && !session->cleanup_reserved_bytes);
            for (unsigned s = 0; s < 3;
                ++s) unchanged(f.session, s, &before[s], true);
            wl_evaluation_control_report_t a = {0}, b = {0};
            CHECK(!wl_evaluation_control_report(f.control, &a));
            CHECK(!wl_evaluation_control_report(f.control, &b));
            CHECK(a.result == rc && !a.cleanup_status
                && !memcmp(&a, &b, sizeof(a)));
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(f.governor)) ==
                after_reserved);
            CHECK(!wl_evaluation_control_reset(f.control));
            CHECK(!finish_step(f.session));
            capture_t expected = oracle(&f), actual = head_capture(f.session);
            equal(&actual, &expected); equal(&f.events, &expected);
            CHECK(!wl_evaluation_control_report(f.control, &b));
            CHECK(!b.result && a.attempt_id != b.attempt_id);
            fixture_destroy(&f);
        }
    for (unsigned i = 0; i < sizeof(targets)/sizeof(targets[0]); ++i)
        CHECK(phase_hits[targets[i]]);
}

static int
drive(fixture_t *f, bool snapshot, capture_t *out)
{
    return snapshot ? wl_session_snapshot(f->session, on_tuple, out)
        : finish_step(f->session);
}

static void
test_dirty_stops(void)
{
    for (unsigned observed = 0; observed < 2; ++observed)
        for (unsigned snapshot = 0; snapshot < 2; ++snapshot)
            for (unsigned work = 0; work < 2; ++work) {
                fixture_t f;
                fixture_init(&f, true, false, observed, true, false);
                capture_t captured = {0};
                CHECK(!drive(&f, snapshot, &captured));
                capture_t prior = oracle(&f);
                f.events.count = 0;
                col_rel_t *input =
                    session_find_rel((wl_col_session_t *)f.session, "a");
                wl_columnar_source_access_reader_t foreign = {0};
                CHECK(!col_rel_source_reader_acquire(input, &foreign));
                CHECK(wl_session_remove(f.session, "a",
                    f.data[0] + 3 * f.widths[0],
                    1, f.widths[0]) == EBUSY);
                CHECK(!col_rel_source_reader_release(&foreign));
                CHECK(!wl_session_remove(f.session, "a",
                    f.data[0] + 3 * f.widths[0],
                    1, f.widths[0]));
                f.counts[0] = 3;
                source_image_t before[3];
                for (unsigned s = 0; s < 3;
                    ++s) before[s] = image(f.session, s);
                col_rel_t *head =
                    session_find_rel((wl_col_session_t *)f.session, "output");
                col_relation_snapshot_t head_before =
                    wl_columnar_relation_snapshot(head);
                CHECK(!wl_evaluation_control_configure(f.control,
                    UINT64_C(1000000000)));
                stopping = &f; stop_target = WL_COLUMNAR_LFTJ_ATTEMPT_SCHEMA;
                stop_work = work; stop_hit = false;
                wl_columnar_lftj_test_attempt_checkpoint = stop_checkpoint;
                captured.count = 0;
                int rc = drive(&f, snapshot, &captured);
                wl_columnar_lftj_test_attempt_checkpoint = NULL;
                stopping = NULL;
                CHECK(stop_hit && rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
                    : WL_EVALUATION_CONTROL_CANCELLED));
                CHECK(!captured.count && !f.events.count);
                CHECK(session_find_rel((wl_col_session_t *)f.session,
                    "output") == head);
                CHECK(wl_columnar_relation_snapshot_equal(head_before,
                    wl_columnar_relation_snapshot(head)));
                capture_t actual = head_capture(f.session);
                equal(&actual, &prior);
                for (unsigned s = 0; s < 3;
                    ++s) unchanged(f.session, s, &before[s], true);
                CHECK(!wl_evaluation_control_reset(f.control));
                captured.count = 0;
                CHECK(!drive(&f, snapshot, &captured));
                capture_t expected = oracle(&f);
                actual = head_capture(f.session); equal(&actual, &expected);
                CHECK(expected.count == 6);
                if (observed) {
                    capture_t delta = event_difference(&prior, &expected);
                    CHECK(delta.count == 4);
                    equal(&f.events, &delta);
                }
                unsigned delivered = f.events.count;
                captured.count = 0;
                CHECK(!drive(&f, snapshot, &captured));
                CHECK(f.events.count == delivered);
                fixture_destroy(&f);
            }
}

static unsigned failure_kind;
static bool failure_hit;
static wl_columnar_memory_reservation_t peer;

static bool authentication_hit;

static void
check_private_authority(wl_evaluation_control_t *control,
    const wl_columnar_lftj_attempt_t *a)
{
    if (authentication_hit ||
        a->phase != WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE) return;
    CHECK(stopping && control == stopping->control && a->private_bindings
        && a->private_bindings->dynamic_entries);
    authentication_hit = true;
    const char *name = NULL; col_rel_t *source = NULL;
    CHECK(!wl_columnar_eval_private_binding_get(a->private_bindings, 0,
        &name, &source) && name && source);
    wl_columnar_eval_private_bindings_t copied = *a->private_bindings;
    copied.identity = (uintptr_t)&copied;
    CHECK(wl_columnar_eval_private_binding_get(&copied, 0, &name, &source)
        == EINVAL);
    wl_columnar_eval_plan_numeric_attempt_t forged =
        *(const wl_columnar_eval_plan_numeric_attempt_t *)a->numeric_owner;
    forged.identity = &forged;
    forged.private_bindings = &copied;
    wl_columnar_lftj_attempt_t child = {0};
    wl_plan_op_t op = {.op = WL_PLAN_OP_LFTJ,
                       .opaque_data = (void *)a->metadata};
    CHECK(wl_columnar_ops_lftj_attempt_begin(&child, &op, a->frame,
        a->session, a->control, a->attempt_id, &forged) == EINVAL);
    CHECK(!child.identity && !child.inputs && !child.output);
}

static void
test_private_authority(void)
{
    fixture_t f;
    fixture_init(&f, true, false, true, true, false);
    stopping = &f; authentication_hit = false;
    wl_columnar_lftj_test_attempt_checkpoint = check_private_authority;
    CHECK(!finish_step(f.session));
    wl_columnar_lftj_test_attempt_checkpoint = NULL; stopping = NULL;
    CHECK(authentication_hit);
    capture_t expected = oracle(&f), actual = head_capture(f.session);
    equal(&actual, &expected); equal(&f.events, &expected);
    fixture_destroy(&f);
}

static bool failure_credit_hit;
static void
fail_output_credit(wl_evaluation_control_t *control, unsigned phase)
{
    if (!failure_hit || failure_credit_hit || phase) return;
    CHECK(stopping && control == stopping->control && failure_kind >= 3);
    failure_credit_hit = true;
    if (failure_kind == 4)
        CHECK(wl_evaluation_control_charge(control,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
fail_output(wl_evaluation_control_t *control,
    const wl_columnar_lftj_attempt_t *a)
{
    if (failure_hit ||
        a->phase != WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_CREATE) return;
    CHECK(stopping && control == stopping->control && a->output_rows == 10);
    failure_hit = true;
    if (failure_kind == 1) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(stopping->governor);
        uint64_t available = atomic_load_explicit(&g->usable_bytes,
                memory_order_acquire) - wl_columnar_memory_reserved(g);
        CHECK(wl_columnar_memory_reserve_checked(g, available, &peer)
            == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    } else if (failure_kind == 0 || failure_kind == 2)
        wl_columnar_relation_test_constructor_fail_allocation = 1;
    if (failure_kind == 2) wl_evaluation_control_request_cancel(control);
}

static void
test_output_failures(void)
{
    for (unsigned kind = 0; kind < 5; ++kind) {
        fixture_t f;
        fixture_init(&f, true, false, true, true, false);
        if (kind == 4) CHECK(!wl_evaluation_control_configure(f.control,
                UINT64_C(1000000000)));
        source_image_t before[3];
        for (unsigned s = 0; s < 3; ++s) before[s] = image(f.session, s);
        uint64_t reserved = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f.governor));
        wl_columnar_memory_reservation_init(&peer);
        stopping = &f; failure_kind = kind;
        failure_hit = failure_credit_hit = false;
        wl_columnar_lftj_test_attempt_checkpoint = fail_output;
        wl_columnar_relation_test_constructor_committed = kind >=
            3 ? fail_output_credit : NULL;
        int rc = finish_step(f.session);
        wl_columnar_lftj_test_attempt_checkpoint = NULL; stopping = NULL;
        wl_columnar_relation_test_constructor_committed = NULL;
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        if (peer.governor) CHECK(wl_columnar_memory_rollback(&peer));
        CHECK(failure_hit && rc == (kind == 0 ? ENOMEM : kind == 1 ? ENOSPC
            : kind ==
            4 ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(failure_credit_hit == (kind >= 3));
        for (unsigned s = 0; s < 3;
            ++s) unchanged(f.session, s, &before[s], true);
        CHECK(!f.events.count &&
            !session_find_rel((wl_col_session_t *)f.session,
            "output"));
        uint64_t after_reserved = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f.governor));
        CHECK(kind == 4 ? after_reserved >= reserved : after_reserved ==
            reserved);
        if (after_reserved > reserved)
            CHECK(wl_session_insert(f.session, "a", f.data[0], 1,
                f.widths[0]) == EBUSY);
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!finish_step(f.session));
        capture_t expected = oracle(&f), actual = head_capture(f.session);
        equal(&actual, &expected); equal(&f.events, &expected);
        fixture_destroy(&f);
    }
}

static const wl_columnar_lftj_attempt_t *emitting;
static unsigned append_tails;

static void
observe_emit(wl_evaluation_control_t *control,
    const wl_columnar_lftj_attempt_t *a)
{
    CHECK(stopping && control == stopping->control);
    emitting = a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_JOIN && a->output_pass
        && a->join_phase == WL_COLUMNAR_LFTJ_JOIN_PRODUCT_EMIT
        && a->fill_input == a->input_count ? a : NULL;
}

static void
stop_append(wl_evaluation_control_t *control, uint64_t work)
{
    (void)work;
    if (stop_hit || !emitting || control != stopping->control) return;
    if (++append_tails != 100) return;
    CHECK(emitting->output && emitting->writer_active
        && emitting->output->nrows == emitting->output_written);
    stop_hit = true;
    if (stop_work) CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
test_partial_append(void)
{
    for (unsigned work = 0; work < 2; ++work) {
        fixture_t f;
        fixture_init(&f, true, false, false, true, false);
        CHECK(!wl_evaluation_control_configure(f.control,
            UINT64_C(1000000000)));
        stopping = &f; stop_hit = false; stop_work = work;
        emitting = NULL; append_tails = 0;
        wl_columnar_lftj_test_attempt_checkpoint = observe_emit;
        wl_columnar_relation_test_work_checkpoint = stop_append;
        int rc = finish_step(f.session);
        wl_columnar_lftj_test_attempt_checkpoint = NULL;
        wl_columnar_relation_test_work_checkpoint = NULL;
        emitting = NULL; stopping = NULL;
        CHECK(stop_hit && append_tails == 100
            && rc == (work ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(!session_find_rel((wl_col_session_t *)f.session, "output"));
        CHECK(!wl_evaluation_control_reset(f.control));
        CHECK(!finish_step(f.session));
        capture_t expected = oracle(&f), actual = head_capture(f.session);
        equal(&actual, &expected);
        fixture_destroy(&f);
    }
}

static wl_session_t *metadata_maximum_session;
static unsigned metadata_maximum_names, metadata_maximum_installs;

static void
metadata_maximum_host_checkpoint(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    CHECK(metadata_maximum_session &&
        r->session == (wl_col_session_t *)metadata_maximum_session
        && r->composed_busy && !wl_columnar_eval_stack_numeric_owner(r->frame,
        r));
    if (r->lftj_metadata.phase == WL_COLUMNAR_LFTJ_METADATA_NAMES) {
        CHECK(r->lftj_metadata.k == 64 &&
            r->lftj_metadata.name_index == metadata_maximum_names);
        ++metadata_maximum_names;
    }
    if (r->lftj_metadata.phase == WL_COLUMNAR_LFTJ_METADATA_INSTALL) {
        CHECK(r->lftj_metadata.prefix_cleared && !r->lftj.identity);
        ++metadata_maximum_installs;
    }
}

static void
test_session_metadata_maximum(void)
{
    /* Enlarge a valid internal plan before any session borrows it. This is
     * actual session hosting, not a claim that the optimizer emits64 sources. */
    const char *text = ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
        ".decl output(x:int64)\noutput(x) :- a(x), b(x), c(x).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    const wl_plan_relation_t *head = &plan->strata[0].relations[0];
    CHECK(head->ops[0].op == WL_PLAN_OP_LFTJ);
    wl_plan_op_lftj_t *metadata = head->ops[0].opaque_data;
    CHECK(metadata && metadata->k == 3);
    char **names = calloc(64, sizeof(*names));
    uint32_t *keys = calloc(64, sizeof(*keys));
    CHECK(names && keys);
    for (unsigned i = 0; i < 64; ++i) {
        const char *name = metadata->rel_names[i % 3];
        names[i] = malloc(strlen(name) + 1); CHECK(names[i]);
        memcpy(names[i], name, strlen(name) + 1);
        keys[i] = metadata->key_cols[i % 3];
    }
    for (uint32_t i = 0; i < metadata->k; ++i) free(metadata->rel_names[i]);
    free(metadata->rel_names); free(metadata->key_cols);
    metadata->rel_names = names; metadata->key_cols = keys; metadata->k = 64;
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .usable_bytes =
         WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution); CHECK(g);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control; options.memory_governor = g;
    wl_session_t *session = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    const int64_t values[] = {3, 1, 2}; const char *sources[] = {"a", "b", "c"};
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(!wl_session_insert(session, sources[i], values, 3, 1));
        CHECK(!wl_session_insert(raw, sources[i], values, 3, 1));
    }
    metadata_maximum_session = session;
    metadata_maximum_names = metadata_maximum_installs = 0;
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint =
        metadata_maximum_host_checkpoint;
    CHECK(!finish_step(session));
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_maximum_session = NULL;
    CHECK(metadata_maximum_names == 64 && metadata_maximum_installs == 1);
    CHECK(!wl_session_step(raw));
    const col_rel_t *actual = session_find_rel((wl_col_session_t *)session,
            "output");
    const col_rel_t *expected = session_find_rel((wl_col_session_t *)raw,
            "output");
    CHECK(actual && expected && actual->ncols == 1 && expected->ncols == 1
        && actual->nrows == 3 && expected->nrows == 3);
    for (unsigned row = 0; row < 3;
        ++row) CHECK(actual->columns[0][row] == (int64_t)row + 1
            && actual->columns[0][row] == expected->columns[0][row]);
    for (unsigned i = 0; i < 3; ++i) {
        const col_rel_t *input = session_find_rel((wl_col_session_t *)session,
                sources[i]);
        CHECK(input && input->nrows == 3);
        for (unsigned row = 0; row < 3;
            ++row) CHECK(input->columns[0][row] == values[row]);
    }
    CHECK(!wl_session_try_destroy(&session) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g))
        && wl_columnar_memory_governor_ref_is_sole(g));
    wl_columnar_memory_governor_ref_release(g);
}

static wl_session_t *metadata_dynamic_session;
static unsigned metadata_dynamic_hits, metadata_dynamic_events;
static unsigned metadata_dynamic_masks[2];

static void
metadata_dynamic_checkpoint(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    if (r->lftj_metadata.phase !=
        WL_COLUMNAR_LFTJ_METADATA_BIND_DYNAMIC) return;
    CHECK(metadata_dynamic_session &&
        r->session == (wl_col_session_t *)metadata_dynamic_session
        && r->private_bindings && r->private_bindings->dynamic_entries
        && r->private_bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX
        && r->lftj_metadata.binding_count == r->private_bindings->count
        && r->lftj_metadata.binding_generation ==
        r->private_bindings->generation
        && wl_columnar_session_observed_bindings_authorized(r->session,
        r->private_bindings));
    ++metadata_dynamic_hits;
}

static void
metadata_dynamic_delta(const char *name, const int64_t *row, uint32_t width,
    int diff, void *data)
{
    (void)data;
    CHECK(metadata_dynamic_session && width == 1 && diff == 1 && row[0] >= 1 &&
        row[0] <= 3);
    unsigned head = !strcmp(name, "one") ? 0 : !strcmp(name, "two") ? 1 : 2;
    CHECK(head < 2 &&
        !(metadata_dynamic_masks[head] & (1u << (unsigned)row[0])));
    metadata_dynamic_masks[head] |= 1u << (unsigned)row[0];
    ++metadata_dynamic_events;
    CHECK(session_find_rel((wl_col_session_t *)metadata_dynamic_session, "one")
        && session_find_rel((wl_col_session_t *)metadata_dynamic_session,
        "two"));
}

static void
test_session_metadata_dynamic_bindings(void)
{
    const char *text = ".decl a(x:int64)\n.decl b(x:int64)\n.decl c(x:int64)\n"
        ".decl one(x:int64)\n.decl two(x:int64)\n"
        "one(x) :- a(x), b(x), c(x).\ntwo(x) :- a(x), x > 1.\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(0, &control));
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .usable_bytes =
         WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution); CHECK(g);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control; options.memory_governor = g;
    wl_session_t *session = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session)
        && !wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    const int64_t rows[] = {3, 1, 2}; const char *names[] = {"a", "b", "c"};
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(!wl_session_insert(session, names[i], rows, 3, 1)
            && !wl_session_insert(raw, names[i], rows, 3, 1));
    }
    metadata_dynamic_session = session;
    metadata_dynamic_hits = metadata_dynamic_events = 0;
    metadata_dynamic_masks[0] = metadata_dynamic_masks[1] = 0;
    wl_session_set_delta_cb(session, metadata_dynamic_delta, NULL);
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint =
        metadata_dynamic_checkpoint;
    int rc = finish_step(session);
    if (rc) fprintf(stderr, "dynamic metadata session: %d\n", rc);
    CHECK(!rc && metadata_dynamic_hits == 1 && metadata_dynamic_events == 5
        && metadata_dynamic_masks[0] == 14 && metadata_dynamic_masks[1] == 12);
    wl_columnar_eval_plan_test_lftj_metadata_checkpoint = NULL;
    metadata_dynamic_session = NULL;
    CHECK(!wl_session_step(raw));
    for (unsigned head = 0; head < 2; ++head) {
        const char *name = head ? "two" : "one";
        const col_rel_t *actual = session_find_rel((wl_col_session_t *)session,
                name);
        const col_rel_t *expected = session_find_rel((wl_col_session_t *)raw,
                name);
        CHECK(actual && expected && actual->nrows == (head ? 2u : 3u)
            && actual->nrows == expected->nrows && actual->ncols == 1 &&
            expected->ncols == 1);
        for (unsigned r = 0; r < actual->nrows; ++r)
            CHECK(actual->columns[0][r] == expected->columns[0][r]);
    }
    CHECK(!wl_session_try_destroy(&session) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g))
        && wl_columnar_memory_governor_ref_is_sole(g));
    wl_columnar_memory_governor_ref_release(g);
}

int
main(int argc, char **argv)
{
    if (argc > 2 || (argc == 2 && strcmp(argv[1], "domains")
        && strcmp(argv[1], "recovery") && strcmp(argv[1], "failures"))) {
        fprintf(stderr, "usage: %s [domains|recovery|failures]\n", argv[0]);
        return EXIT_FAILURE;
    }
    if (argc == 1 || !strcmp(argv[1], "domains")) {
        test_session_initial();
        test_session_metadata_maximum();
        test_session_metadata_dynamic_bindings();
        test_session_domains();
    }
    if (argc == 1 || !strcmp(argv[1], "recovery")) {
        test_successors();
        test_stops();
        test_dirty_stops();
        test_partial_append();
    }
    if (argc == 1 || !strcmp(argv[1], "failures")) {
        test_output_failures();
        test_private_authority();
    }
    puts("LFTJ session tests passed");
    return 0;
}
