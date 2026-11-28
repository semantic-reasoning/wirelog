/*
 * Observed removal staging and its consuming JOIN domains.
 * Copyright (C) CleverPlant. Licensed under LGPL-3.0.
 */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/intern.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)
#define SIDE 16u

typedef struct {
    char names[2][513];
    int64_t facts[2][64];
    uint32_t rows[2], width, count;
    bool keyed;
    unsigned key_type;
    unsigned image[2][SIDE][SIDE], snapshot[2][SIDE][SIDE];
    int events[2][SIDE][SIDE];
    unsigned deliveries[2][SIDE][SIDE];
    wl_session_t *session;
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_evaluation_control_t *control;
    wl_columnar_memory_governor_ref_t *governor;
} fixture_t;

static int
output_index(const char *name)
{
    if (!strcmp(name, "out0")) return 0;
    if (!strcmp(name, "out1")) return 1;
    return -1;
}

static void
changed(const char *name, const int64_t *row, uint32_t width, int32_t diff,
    void *data)
{
    fixture_t *f = data;
    int index = output_index(name);
    if (index < 0) return;
    CHECK((unsigned)index < f->count && width == 2 && row[0] >= 0
        && row[0] < SIDE && row[1] >= 0 && row[1] < SIDE
        && (diff == 1 || diff == -1));
    unsigned a = (unsigned)row[0], b = (unsigned)row[1];
    CHECK(!f->deliveries[index][a][b]++);
    f->events[index][a][b] += diff;
}

static void
captured(const char *name, const int64_t *row, uint32_t width, void *data)
{
    fixture_t *f = data;
    int index = output_index(name);
    if (index < 0) return;
    CHECK((unsigned)index < f->count && width == 2 && row[0] >= 0
        && row[0] < SIDE && row[1] >= 0 && row[1] < SIDE);
    CHECK(!f->snapshot[index][row[0]][row[1]]++);
}

/* Independently owned physical input bags determine complete set-valued JOIN
 * images. No expected row is read from an engine relation or callback. */
static void
reference(const fixture_t *f, unsigned image[2][SIDE][SIDE])
{
    memset(image, 0, sizeof(f->image));
    for (unsigned n = 0; n < f->count; ++n)
        for (uint32_t i = 0; i < f->rows[n]; ++i)
            for (uint32_t j = 0; j < f->rows[n]; ++j) {
                const int64_t *a = f->facts[n] + i * f->width;
                const int64_t *b = f->facts[n] + j * f->width;
                if (f->keyed && a[0] != b[0]) continue;
                int64_t x = a[f->width - 1], y = b[f->width - 1];
                CHECK(x >= 0 && x < SIDE && y >= 0 && y < SIDE);
                image[n][x][y] = 1;
            }
}

static void
step(fixture_t *f)
{
    unsigned expected[2][SIDE][SIDE];
    reference(f, expected);
    int rc;
    unsigned calls = 0;
    do {
        CHECK(++calls < 100000); rc = wl_session_step(f->session);
    }while (rc == EAGAIN);
    CHECK(!rc);
    for (unsigned n = 0; n < f->count; ++n)
        for (unsigned a = 0; a < SIDE; ++a)
            for (unsigned b = 0; b < SIDE; ++b) {
                int difference = (int)expected[n][a][b] -
                    (int)f->image[n][a][b];
                CHECK(f->events[n][a][b] == difference
                    && f->deliveries[n][a][b] == (unsigned)(difference != 0));
            }
    memcpy(f->image, expected, sizeof(expected));
    memset(f->events, 0, sizeof(f->events));
    memset(f->deliveries, 0, sizeof(f->deliveries));
}

static void
snapshot(fixture_t *f)
{
    memset(f->snapshot, 0, sizeof(f->snapshot));
    int rc;
    unsigned calls = 0;
    do {
        CHECK(++calls < 100000);
        rc = wl_session_snapshot(f->session, captured, f);
    }while (rc == EAGAIN);
    CHECK(!rc && !memcmp(f->image, f->snapshot, sizeof(f->image)));
    for (unsigned n = 0; n < f->count; ++n)
        for (unsigned a = 0; a < SIDE; ++a)
            for (unsigned b = 0; b < SIDE; ++b) CHECK(!f->events[n][a][b]);
}

static void
insert(fixture_t *f, unsigned n, const int64_t *rows, uint32_t count)
{
    CHECK(f->rows[n] + count <= 32
        && !wl_session_insert(f->session, f->names[n], rows, count, f->width));
    memcpy(f->facts[n] + f->rows[n] * f->width, rows,
        (size_t)count * f->width * sizeof(*rows));
    f->rows[n] += count;
}

static void
remove_rows(fixture_t *f, unsigned n, const int64_t *rows, uint32_t count)
{
    CHECK(!wl_session_remove(f->session, f->names[n], rows, count, f->width));
    for (uint32_t q = 0; q < count; ++q)
        for (uint32_t i = 0; i < f->rows[n]; ++i)
            if (!memcmp(f->facts[n] + i * f->width, rows + q * f->width,
                f->width * sizeof(*rows))) {
                memmove(f->facts[n] + i * f->width,
                    f->facts[n] + (i + 1) * f->width,
                    (size_t)(f->rows[n] - i - 1) * f->width * sizeof(*rows));
                --f->rows[n]; break;
            }
}

static col_rel_t *
delta(fixture_t *f, unsigned n)
{
    return wl_columnar_session_find_prefixed(COL_SESSION(f->session),
               "$r$", f->names[n]);
}

static void
fixture_open_impl(fixture_t *f, unsigned length, bool keyed, unsigned key_type,
    bool controlled, bool governed, bool siblings, bool duplicates,
    bool input_head)
{
    memset(f, 0, sizeof(*f));
    f->key_type = key_type;
    f->keyed = keyed; f->width = keyed ? 2u : 1u; f->count = siblings ? 2u : 1u;
    CHECK(length < sizeof(f->names[0]));
    const char *type = key_type == 1 ? "float" : key_type ==
        2 ? "symbol" : "int64";
    char text[16384]; size_t used = 0;
    for (unsigned n = 0; n < f->count; ++n) {
        memset(f->names[n], 'a', length); f->names[n][length] = '\0';
        if (n) f->names[n][length - 1] = 'b';
        int written = keyed ? snprintf(text + used, sizeof(text) - used,
                ".decl %s(k:%s,v:int64)\n.decl out%u(v:int64,w:int64)\n"
                "out%u(v,w):-%s(k,v),%s(k,w).\n", f->names[n], type, n, n,
                f->names[n], f->names[n]) : snprintf(text + used,
                sizeof(text) - used,
                ".decl %s(x:int64)\n.decl out%u(x:int64,y:int64)\n"
                "out%u(x,y):-%s(x),%s(y).\n", f->names[n], n, n,
                f->names[n], f->names[n]);
        CHECK(written > 0 && (size_t)written < sizeof(text) - used);
        used += (size_t)written;
    }
    if (input_head) {
        CHECK(keyed && f->count == 1);
        int written = snprintf(text + used, sizeof(text) - used,
                ".decl seed_feed(k:%s,v:int64)\n%s(k,v):-seed_feed(k,v).\n",
                type, f->names[0]);
        CHECK(written > 0 && (size_t)written < sizeof(text) - used);
    }
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    int64_t keys[] = {1, 7, 1, 9, 2, 8};
    if (key_type == 1) {
        keys[0] = keys[2] = wl_columnar_float_to_bits(1.5);
        keys[4] = wl_columnar_float_to_bits(2.5);
    } else if (key_type == 2) {
        keys[0] = keys[2] = wl_intern_put(f->plan->intern, "red");
        keys[4] = wl_intern_put(f->plan->intern, "blue");
        CHECK(keys[0] >= 0 && keys[4] >= 0 && keys[0] != keys[4]);
    }
    if (governed) {
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = UINT64_C(256) << 20,
                .usable_bytes = UINT64_C(256) << 20,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
        };
        f->governor = wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(f->governor);
    }
    if (controlled) CHECK(!wl_evaluation_control_create(UINT64_C(100000000),
            &f->control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f->governor;
    options.evaluation_control = f->control;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->session));
    wl_session_set_delta_cb(f->session, changed, f);
    const int64_t product[] = {1, 2, 3}, duplicate[] = {1, 2, 2, 3};
    for (unsigned n = 0; n < f->count; ++n)
        insert(f, n, keyed ? keys : duplicates ? duplicate : product,
            duplicates ? 4u : 3u);
    step(f); snapshot(f);
}

static void
fixture_open(fixture_t *f, unsigned length, bool keyed, bool controlled,
    bool governed, bool siblings, bool duplicates)
{
    fixture_open_impl(f, length, keyed, 0, controlled, governed, siblings,
        duplicates, false);
}

static void
fixture_open_typed(fixture_t *f, unsigned length, unsigned key_type,
    bool controlled, bool governed, bool siblings, bool duplicates)
{
    fixture_open_impl(f, length, true, key_type, controlled, governed, siblings,
        duplicates, false);
}

static void
fixture_close(fixture_t *f)
{
    wl_session_destroy(f->session);
    if (f->control) wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    if (f->governor) {
        CHECK(!wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(f->governor)));
        wl_columnar_memory_governor_ref_release(f->governor);
    }
}

static void
public_histories(void)
{
    const unsigned lengths[] = {4, 252, 253, 300, 511};
    for (unsigned i = 0; i < 5; ++i) {
        fixture_t f;
        fixture_open(&f, lengths[i], false, false, true, false, false);
        const int64_t row[] = {2}; remove_rows(&f, 0, row, 1);
        col_rel_t *pending = delta(&f, 0);
        CHECK(pending && strlen(pending->name) == lengths[i] + 3u
            && pending->nrows == 1 && pending->columns[0][0] == 2);
        step(&f); CHECK(!delta(&f, 0)); step(&f); snapshot(&f);
        insert(&f, 0, row, 1); step(&f);
        remove_rows(&f, 0, row, 1); step(&f); step(&f); snapshot(&f);
        fixture_close(&f);
    }
    for (unsigned controlled = 0; controlled < 2; ++controlled) {
        fixture_t f;
        fixture_open(&f, 300, true, controlled != 0, true, false, false);
        const int64_t row[] = {1, 7}; remove_rows(&f, 0, row, 1);
        step(&f); step(&f); snapshot(&f); insert(&f, 0, row, 1); step(&f);
        const int64_t later[] = {2, 8}; remove_rows(&f, 0, later, 1);
        step(&f); step(&f); snapshot(&f); fixture_close(&f);
    }
    fixture_t unmanaged;
    fixture_open(&unmanaged, 300, false, false, false, false, false);
    const int64_t row[] = {2}; remove_rows(&unmanaged, 0, row, 1);
    step(&unmanaged); snapshot(&unmanaged); fixture_close(&unmanaged);
    puts(
        "public long-name product/keyed raw+controlled signed/future histories passed");
}

static void
pending_histories(void)
{
    fixture_t f; fixture_open(&f, 300, false, false, true, true, false);
    const int64_t two[] = {2}, three[] = {3}, absent[] = {99};
    remove_rows(&f, 0, two, 1);
    col_rel_t *prior = delta(&f, 0); CHECK(prior && !delta(&f, 1));
    wl_col_session_t *s = COL_SESSION(f.session);
    uint32_t epoch = s->outer_epoch;
    col_rel_t *other = session_find_rel(s, f.names[1]);
    col_relation_snapshot_t other_before = wl_columnar_relation_snapshot(other);
    remove_rows(&f, 1, absent, 1);
    CHECK(delta(&f, 0) == prior && s->outer_epoch == epoch);
    CHECK(wl_session_remove(f.session, f.names[1], two, 1, 1) == EBUSY
        && delta(&f, 0) == prior && !delta(&f, 1) && s->outer_epoch == epoch
        && wl_columnar_relation_snapshot_equal(other_before,
        wl_columnar_relation_snapshot(other)));
    remove_rows(&f, 0, three, 1);
    CHECK(delta(&f, 0) && delta(&f, 0) != prior && delta(&f, 0)->nrows == 2);
    step(&f); step(&f); remove_rows(&f, 1, two, 1);
    CHECK(!delta(&f, 0) && delta(&f, 1)); step(&f); snapshot(&f);
    fixture_close(&f);

    fixture_open(&f, 300, false, false, true, false, true);
    col_rel_t *source = session_find_rel(COL_SESSION(f.session), f.names[0]);
    CHECK(source && source->nrows == 4 && !col_rel_enable_timestamps(source));
    for (uint32_t i = 0; i < source->nrows;
        ++i) source->timestamps[i].iteration = 100u + i;
    remove_rows(&f, 0, two, 1); remove_rows(&f, 0, two, 1);
    col_rel_t *pending = delta(&f, 0);
    CHECK(pending && pending->nrows == 2 && pending->timestamps
        && pending->timestamps[0].iteration == 101 &&
        pending->timestamps[1].iteration == 102);
    epoch = COL_SESSION(f.session)->outer_epoch;
    remove_rows(&f, 0, two, 1); CHECK(delta(&f, 0) == pending
        && COL_SESSION(f.session)->outer_epoch == epoch);
    const int64_t twice[] = {3, 3}; remove_rows(&f, 0, twice, 2);
    pending = delta(&f, 0);
    CHECK(pending && pending->nrows == 3 &&
        pending->timestamps[2].iteration == 103);
    step(&f); step(&f); snapshot(&f); fixture_close(&f);
    puts(
        "exact long siblings/pending replacement/miss/physical multiplicity/timestamps passed");
}

/* This helper is deliberately not selected by public STEP. Qualify only its
 * exact long-name right-pass probe, forwarding every pass to the real engine. */
static col_rel_t *legacy_delta;
static unsigned legacy_passes[2];
static int
legacy_evaluate(const wl_plan_stratum_t *sp, wl_col_session_t *s,
    uint32_t index)
{
    CHECK(s->retraction_seeded && wl_columnar_session_find_prefixed(s, "$r$",
        s->last_removed_relation) == legacy_delta);
    ++legacy_passes[s->retraction_right_pass ? 1 : 0];
    return col_eval_stratum(sp, s, index);
}

static void
legacy_right_probe(void)
{
    fixture_t f; fixture_open(&f, 300, false, false, true, false, false);
    const int64_t two[] = {2}; remove_rows(&f, 0, two, 1);
    legacy_delta = delta(&f, 0); CHECK(legacy_delta);
    col_relation_snapshot_t before =
        wl_columnar_relation_snapshot(legacy_delta);
    const wl_plan_stratum_t *stratum = NULL; uint32_t index = 0;
    for (uint32_t i = 0; i < f.plan->stratum_count; ++i)
        for (uint32_t j = 0; j < f.plan->strata[i].relation_count; ++j)
            if (!strcmp(f.plan->strata[i].relations[j].name, "out0")) {
                stratum = &f.plan->strata[i]; index = i;
            }
    CHECK(stratum); memset(legacy_passes, 0, sizeof(legacy_passes));
    wl_columnar_eval_delta_test_retraction_eval = legacy_evaluate;
    int rc = wl_columnar_eval_delta_test_retraction_step(stratum,
            COL_SESSION(f.session), index);
    wl_columnar_eval_delta_test_retraction_eval = NULL;
    CHECK(!rc && legacy_passes[0] == 1 && legacy_passes[1] == 1
        && wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(legacy_delta)));
    legacy_delta = NULL; fixture_close(&f);
    puts(
        "direct dormant legacy right-pass exact long-name probe reached both real passes");
}

static void
name_admission(void)
{
    for (unsigned existing = 0; existing < 2; ++existing) {
        fixture_t f; fixture_open(&f, 300, false, false, true, false, false);
        const int64_t one[] = {1}, two[] = {2};
        if (existing) remove_rows(&f, 0, one, 1);
        wl_col_session_t *s = COL_SESSION(f.session);
        col_rel_t *source = session_find_rel(s, f.names[0]),
            *prior = delta(&f, 0);
        col_rel_t *seed = wl_columnar_session_seed_rows(s, f.names[0]);
        CHECK(!seed); /* This declared EDB has no rule-head seed shadow. */
        col_relation_snapshot_t before = wl_columnar_relation_snapshot(source);
        col_relation_snapshot_t old_delta =
            wl_columnar_relation_snapshot(prior);
        const char *last_removed = s->last_removed_relation;
        uint32_t epoch = s->outer_epoch;
        bool pending = s->pending_input_change,
            stable = s->snapshot_stable_valid;
        wl_columnar_memory_governor_t *gov =
            wl_columnar_memory_governor_ref_get(f.governor);
        uint64_t baseline = wl_columnar_memory_reserved(gov),
            denials = wl_columnar_memory_denials(gov);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        uint64_t bytes = (UINT64_C(256) << 20) - baseline - 303u;
        CHECK(wl_columnar_memory_reserve_checked(gov, bytes,
            &peer) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        CHECK(wl_session_remove(f.session, f.names[0], two, 1, 1) == ENOSPC
            && wl_columnar_memory_denials(gov) == denials + 1u
            && wl_columnar_memory_reserved(gov) == baseline + bytes
            && wl_columnar_relation_snapshot_equal(before,
            wl_columnar_relation_snapshot(source))
            && delta(&f, 0) == prior && s->last_removed_relation == last_removed
            && s->outer_epoch == epoch && s->pending_input_change == pending
            && s->snapshot_stable_valid == stable &&
            !wl_columnar_session_seed_rows(s, f.names[0]));
        if (prior) CHECK(wl_columnar_relation_snapshot_equal(old_delta,
                wl_columnar_relation_snapshot(prior)) && prior->nrows == 1 &&
                prior->columns[0][0] == 1);
        for (unsigned a = 0; a < SIDE; ++a) for (unsigned b = 0; b < SIDE; ++b)
                CHECK(!f.events[0][a][b] && !f.deliveries[0][a][b]);
        CHECK(wl_columnar_memory_rollback(&peer)
            && wl_columnar_memory_reserved(gov) == baseline);
        remove_rows(&f, 0, two, 1); step(&f); step(&f); snapshot(&f);
        insert(&f, 0, two, 1); step(&f); snapshot(&f); fixture_close(&f);
    }
    puts(
        "long transient-name real peer admission refusal/prior pending/refund/retry passed");
}

/* These direct consumers use real publicly staged descriptors. Public STEP
 * continues its separately verified full-diff route; no legacy optimization is
 * enabled by this fixture. */
static const wl_plan_relation_t *
compiled_output(fixture_t *f)
{
    for (uint32_t i = 0; i < f->plan->stratum_count; ++i)
        for (uint32_t j = 0; j < f->plan->strata[i].relation_count; ++j)
            if (!strcmp(f->plan->strata[i].relations[j].name, "out0"))
                return &f->plan->strata[i].relations[j];
    CHECK(false); return NULL;
}

static void
check_delta_schema(const col_rel_t *d, const col_rel_t *source)
{
    CHECK(d && d != source && d->name != source->name
        && d->relation_identity != source->relation_identity
        && d->storage_owner == d && d->ncols == source->ncols
        && d->declared_ncols == source->declared_ncols
        && d->has_graph_column == source->has_graph_column
        && d->graph_col_idx == source->graph_col_idx
        && d->compound_arity_len == source->compound_arity_len);
    for (uint32_t c = 0; c < source->ncols; ++c) {
        CHECK(d->col_names[c] && source->col_names[c]
            && !strcmp(d->col_names[c], source->col_names[c]));
        if (source->column_types) CHECK(d->column_types
                && d->column_types[c] == source->column_types[c]);
    }
    if (source->compound_arity_len) {
        CHECK(d->compound_arity_map &&
            d->compound_arity_map != source->compound_arity_map
            && !memcmp(d->compound_arity_map, source->compound_arity_map,
            (size_t)source->compound_arity_len * sizeof(uint32_t))
            && d->compound_kind == source->compound_kind
            && d->compound_count == source->compound_count
            && d->inline_physical_offset == source->inline_physical_offset
            && wl_columnar_relation_compound_map_valid(d));
    }
}

static void
direct_typed_handoff(fixture_t *f)
{
    wl_col_session_t *s = COL_SESSION(f->session);
    col_rel_t *source = session_find_rel(s, f->names[0]), *d = delta(f, 0);
    check_delta_schema(d, source);
    CHECK(d->nrows == 1 && d->ncols == 2 && d->columns[1][0] == 7);
    if (f->key_type == 1) CHECK(d->column_types[0] == WIRELOG_TYPE_FLOAT
            && d->columns[0][0] == wl_columnar_float_to_bits(1.5));
    if (f->key_type == 2) CHECK(d->column_types[0] == WIRELOG_TYPE_STRING
            && !strcmp(wl_intern_reverse(s->intern, d->columns[0][0]), "red"));
    col_relation_snapshot_t before[] = {wl_columnar_relation_snapshot(source),
                                        wl_columnar_relation_snapshot(d)};
    bool seeded = s->retraction_seeded, right = s->retraction_right_pass;
    uint32_t iteration = s->current_iteration;
    const wl_plan_relation_t *rp = compiled_output(f);
    CHECK(rp->ops[0].op == WL_PLAN_OP_VARIABLE);
    for (unsigned pass = 0; pass < 3; ++pass) {
        s->retraction_seeded = pass < 2; s->retraction_right_pass = pass == 1;
        s->current_iteration = 0;
        eval_stack_t variable = {0};
        CHECK(!col_op_variable(&rp->ops[0], &variable, s)
            && variable.top == 1 && !variable.items[0].owned
            && variable.items[0].rel == (pass == 0 ? d : source)
            && variable.items[0].is_delta == (pass == 0));
        eval_stack_drain(&variable);
        eval_stack_t stack = {0}; CHECK(!col_eval_relation_plan(rp, &stack, s)
            && stack.top == 1 && stack.items[0].owned);
        const col_rel_t *out = stack.items[0].rel;
        CHECK(out && out->ncols == 2 && out->nrows == (pass < 2 ? 1u : 2u));
        unsigned seen[SIDE][SIDE] = {{0}};
        for (uint32_t i = 0; i < out->nrows; ++i) {
            int64_t a = out->columns[0][i], b = out->columns[1][i];
            CHECK(a >= 0 && a < SIDE && b >= 0 && b < SIDE && !seen[a][b]++);
        }
        for (unsigned a = 0; a < SIDE;
            ++a) for (unsigned b = 0; b < SIDE; ++b){
                bool expected = pass == 0 ? a == 7 && b == 9
                : pass == 1 ? a == 9 && b == 7
                : (a == 8 && b == 8) || (a == 9 && b == 9);
                CHECK(seen[a][b] == (unsigned)expected);
            }
        eval_stack_drain(&stack);
        CHECK(wl_columnar_relation_snapshot_equal(before[0],
            wl_columnar_relation_snapshot(source))
            && wl_columnar_relation_snapshot_equal(before[1],
            wl_columnar_relation_snapshot(d)));
    }
    s->retraction_seeded = seeded; s->retraction_right_pass = right;
    s->current_iteration = iteration;
}

static void
typed_histories(void)
{
    for (unsigned type = 0; type < 3; ++type)
        for (unsigned long_name = 0; long_name < 2; ++long_name) {
            fixture_t f;
            fixture_open_typed(&f, long_name ? 300 : 4, type, false, true,
                false, false);
            int64_t removed[] = {f.facts[0][0], 7};
            remove_rows(&f, 0, removed, 1);
            direct_typed_handoff(&f); step(&f); step(&f); snapshot(&f);
            insert(&f, 0, removed, 1); step(&f); snapshot(&f);
            fixture_close(&f);
        }
    for (unsigned type = 1; type < 3; ++type)
        for (unsigned controlled = 0; controlled < 2; ++controlled) {
            fixture_t f;
            fixture_open_typed(&f, 300, type, controlled != 0, controlled != 0,
                false, false);
            int64_t removed[] = {f.facts[0][0], 7};
            remove_rows(&f, 0, removed, 1);
            if (!controlled) direct_typed_handoff(&f);
            step(&f); step(&f); snapshot(&f); insert(&f, 0, removed, 1);
            step(&f); remove_rows(&f, 0, removed, 1); step(&f); snapshot(&f);
            fixture_close(&f);
        }
    puts(
        "public-staged INT64/FLOAT/STRING exact typed raw left/right/full handoff and public signed futures passed");
}

typedef struct {
    col_rel_t *relation;
    col_relation_snapshot_t snapshot;
    uint32_t rows, width, declared, capacity, base, sorted, runs,
        timestamp_capacity;
    uint32_t run_ends[COL_MAX_RUNS];
    int64_t **columns;
    col_delta_timestamp_t *timestamp_storage, timestamps[16];
    int64_t cells[64];
    bool typed;
    wirelog_column_type_t types[8];
    char names[8][64];
} relation_image_t;

static void
save_relation(relation_image_t *image, col_rel_t *r)
{
    memset(image, 0, sizeof(*image)); image->relation = r;
    image->snapshot = wl_columnar_relation_snapshot(r);
    if (!r) return;
    CHECK(r->ncols <= 8 && r->nrows <= 16 &&
        (uint64_t)r->nrows * r->ncols <= 64);
    image->rows = r->nrows; image->width = r->ncols;
    image->capacity = r->capacity; image->base = r->base_nrows;
    image->sorted = r->sorted_nrows; image->runs = r->run_count;
    memcpy(image->run_ends, r->run_ends, sizeof(image->run_ends));
    image->columns = r->columns; image->timestamp_storage = r->timestamps;
    image->timestamp_capacity = r->timestamp_capacity;
    if (r->timestamps) memcpy(image->timestamps, r->timestamps,
            (size_t)r->nrows * sizeof(*r->timestamps));
    image->declared = r->declared_ncols; image->typed = r->column_types != NULL;
    for (uint32_t c = 0; c < r->ncols; ++c) {
        if (image->typed) image->types[c] = r->column_types[c];
        CHECK(r->col_names[c] &&
            strlen(r->col_names[c]) < sizeof(image->names[c]));
        strcpy(image->names[c], r->col_names[c]);
    }
    for (uint32_t i = 0; i < r->nrows; ++i)
        col_rel_row_copy_out(r, i, image->cells + i * r->ncols);
}

static void
check_relation(const relation_image_t *image, col_rel_t *r)
{
    CHECK(r == image->relation);
    if (!r) return; /* Absence has no valid relation generation. */
    CHECK(wl_columnar_relation_snapshot_equal(image->snapshot,
        wl_columnar_relation_snapshot(r)));
    CHECK(r->nrows == image->rows && r->ncols == image->width
        && r->declared_ncols == image->declared
        && (r->column_types != NULL) == image->typed
        && r->capacity == image->capacity && r->base_nrows == image->base
        && r->sorted_nrows == image->sorted && r->run_count == image->runs
        && !memcmp(r->run_ends, image->run_ends, sizeof(image->run_ends))
        && r->columns == image->columns &&
        r->timestamps == image->timestamp_storage
        && r->timestamp_capacity == image->timestamp_capacity);
    if (r->timestamps) CHECK(!memcmp(r->timestamps, image->timestamps,
            (size_t)r->nrows * sizeof(*r->timestamps)));
    for (uint32_t c = 0; c < r->ncols; ++c) {
        CHECK(!strcmp(image->names[c], r->col_names[c]));
        if (image->typed) CHECK(image->types[c] == r->column_types[c]);
    }
    int64_t row[8];
    for (uint32_t i = 0; i < r->nrows; ++i) {
        col_rel_row_copy_out(r, i, row);
        CHECK(!memcmp(row, image->cells + i * r->ncols,
            r->ncols * sizeof(*row)));
    }
    /* Both source gates must admit a real reader after failed staging. */
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!col_rel_source_reader_acquire(r, &reader)
        && !col_rel_source_reader_release(&reader));
}

static void
staging_faults(void)
{
    for (unsigned input_head = 0; input_head < 2; ++input_head)
        for (unsigned pending = 0; pending < 2; ++pending)
            for (unsigned failure = 0; failure < 4; ++failure) {
                if ((!pending && failure == 3) ||
                    (input_head && failure >= 2)) continue;
                fixture_t f;
                fixture_open_impl(&f, input_head && !pending ? 6 : 300, true, 1,
                    false, true, false, false, input_head != 0);
                col_rel_t *source = session_find_rel(COL_SESSION(f.session),
                        f.names[0]);
                if (failure == 2) {
                    CHECK(!col_rel_enable_timestamps(source));
                    for (uint32_t i = 0; i < source->nrows; ++i)
                        source->timestamps[i].iteration = 500u + i;
                }
                int64_t removed[2];
                memcpy(removed, f.facts[0], sizeof(removed));
                if (pending) remove_rows(&f, 0, removed, 1);
                memcpy(removed, f.facts[0], sizeof(removed));
                wl_col_session_t *s = COL_SESSION(f.session);
                col_rel_t *prior = delta(&f, 0);
                relation_image_t saved[5];
                save_relation(&saved[0], source);
                save_relation(&saved[1], prior);
                save_relation(&saved[2], session_find_rel(s, "out0"));
                save_relation(&saved[3],
                    wl_columnar_session_seed_rows(s, f.names[0]));
                save_relation(&saved[4],
                    wl_columnar_session_retired_rows(s, f.names[0]));
                if (input_head) CHECK(saved[3].relation && saved[3].rows);
                const char *last = s->last_removed_relation;
                uint32_t epoch = s->outer_epoch, nrels = s->nrels;
                bool changed_input = s->pending_input_change,
                    stable = s->snapshot_stable_valid;
                wl_columnar_memory_governor_t *gov =
                    wl_columnar_memory_governor_ref_get(f.governor);
                uint64_t baseline = wl_columnar_memory_reserved(gov),
                    denials = wl_columnar_memory_denials(gov);
                wl_columnar_memory_reservation_t peer;
                wl_columnar_memory_reservation_init(&peer);
                wl_columnar_source_access_reader_t held = {0};
                uint64_t peer_bytes = 0;
                if (failure ==
                    0) wl_columnar_relation_test_fail_next_metadata_alloc();
                else if (failure == 1) {
                    /* Admit the exact transient key and independent descriptor/name;
                     * leave no room for the subsequent positive schema admission. */
                    uint64_t key_bytes = strlen(f.names[0]) + 4u;
                    uint64_t window = sizeof(col_rel_t) + key_bytes
                        + (key_bytes > 256u ? key_bytes : 0u);
                    CHECK(baseline + window < (UINT64_C(256) << 20));
                    peer_bytes = (UINT64_C(256) << 20) - baseline - window;
                    CHECK(wl_columnar_memory_reserve_checked(gov, peer_bytes,
                        &peer)
                        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
                } else if (failure ==
                    2)
                    wl_columnar_relation_test_fail_next_governed_timestamp_alloc();
                else CHECK(prior && !col_rel_source_reader_acquire(prior,
                        &held));
                int rc = wl_session_remove(f.session, f.names[0], removed, 1,
                        2);
                CHECK(rc ==
                    (failure == 1 ? ENOSPC : failure == 3 ? EBUSY : ENOMEM)
                    && wl_columnar_memory_reserved(gov) == baseline + peer_bytes
                    && wl_columnar_memory_denials(gov) ==
                    denials + (failure == 1 ? 1u : 0u)
                    && s->outer_epoch == epoch && s->nrels == nrels
                    && s->last_removed_relation == last &&
                    s->pending_input_change == changed_input
                    && s->snapshot_stable_valid == stable);
                check_relation(&saved[0], source);
                check_relation(&saved[1], delta(&f, 0));
                check_relation(&saved[2], session_find_rel(s, "out0"));
                check_relation(&saved[3],
                    wl_columnar_session_seed_rows(s, f.names[0]));
                check_relation(&saved[4],
                    wl_columnar_session_retired_rows(s, f.names[0]));
                for (unsigned a = 0; a < SIDE; ++a) for (unsigned b = 0;
                        b < SIDE; ++b)
                        CHECK(!f.events[0][a][b] && !f.deliveries[0][a][b]);
                if (failure ==
                    2) CHECK(
                        !
                        wl_columnar_relation_test_governed_timestamp_alloc_pending());
                if (held.owner) CHECK(!col_rel_source_reader_release(&held));
                if (peer_bytes) CHECK(wl_columnar_memory_rollback(&peer));
                CHECK(wl_columnar_memory_reserved(gov) == baseline);
                remove_rows(&f, 0, removed, 1);
                check_delta_schema(delta(&f, 0), source);
                step(&f); step(&f); snapshot(&f); insert(&f, 0, removed, 1);
                step(&f); snapshot(&f); fixture_close(&f);
            }
    puts(
        "populated seed-shadow and EDB initial/prior-pending schema+timestamp ENOMEM/real private-prefix ENOSPC/registration-reader EBUSY exact rollback/refund/retry passed");
}

static wl_evaluation_control_t *stop_control;
static uint64_t stop_spent, stop_id;
static unsigned stop_witnesses;
static bool cancel_probe;

static void
staged_join_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase != 29 || progress != 0) return;
    CHECK(control == stop_control);
    ++stop_witnesses;
    stop_id = wl_evaluation_control_active_attempt_id(control);
    CHECK(stop_id && !wl_evaluation_control_test_active_consumed(control,
        stop_id, &stop_spent));
    if (cancel_probe) wl_evaluation_control_request_cancel(control);
}

static void
controlled_staging_stops(void)
{
    uint64_t budget = 0;
    for (unsigned mode = 0; mode < 3; ++mode) {
        fixture_t f; fixture_open_typed(&f, 300, 1, true, true, false, false);
        const int64_t row[] = {wl_columnar_float_to_bits(1.5), 7};
        remove_rows(&f, 0, row, 1); /* Synchronous removal has already committed. */
        wl_col_session_t *s = COL_SESSION(f.session);
        relation_image_t saved[3];
        save_relation(&saved[0], session_find_rel(s, f.names[0]));
        save_relation(&saved[1], delta(&f, 0));
        save_relation(&saved[2], session_find_rel(s, "out0"));
        CHECK(saved[0].rows == 2 && saved[1].rows == 1 && saved[2].rows == 5);
        uint32_t epoch = s->outer_epoch;
        const char *last = s->last_removed_relation;
        wl_columnar_memory_governor_t *gov =
            wl_columnar_memory_governor_ref_get(f.governor);
        uint64_t baseline = wl_columnar_memory_reserved(gov),
            denials = wl_columnar_memory_denials(gov);
        if (mode == 2) CHECK(budget && !wl_evaluation_control_reset(f.control)
                && !wl_evaluation_control_configure(f.control, budget));
        stop_control = f.control; cancel_probe = mode == 1;
        stop_witnesses = 0; stop_spent = stop_id = 0;
        wl_columnar_join_test_attempt_checkpoint = staged_join_checkpoint;
        if (!mode) {
            step(&f); budget = stop_spent;
            CHECK(stop_witnesses == 1 && budget);
        } else {
            int rc; unsigned calls = 0;
            do {
                CHECK(++calls < 100000); rc = wl_session_step(f.session);
            }while (rc == EAGAIN);
            int cause = mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(rc == cause && stop_witnesses == 1 && stop_id
                && !wl_evaluation_control_active_attempt_id(f.control)
                && !s->cleanup_active_count && !s->cleanup_pending_count
                && !s->cleanup_reserved_bytes
                && wl_columnar_memory_reserved(gov) == baseline
                && wl_columnar_memory_denials(gov) == denials
                && s->outer_epoch == epoch && s->last_removed_relation == last);
            check_relation(&saved[0], session_find_rel(s, f.names[0]));
            check_relation(&saved[1], delta(&f, 0));
            check_relation(&saved[2], session_find_rel(s, "out0"));
            for (unsigned a = 0; a < SIDE; ++a) for (unsigned b = 0; b < SIDE;
                    ++b)
                    CHECK(!f.events[0][a][b] && !f.deliveries[0][a][b]);
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(f.control, &report)
                && report.attempt_id == stop_id && report.result == cause
                && report.stop_reason == cause && !report.execution_status
                && !report.cleanup_status);
            if (mode == 2) CHECK(report.limit == budget &&
                    report.consumed == budget);
        }
        wl_columnar_join_test_attempt_checkpoint = NULL;
        cancel_probe = false; stop_control = NULL;
        if (mode) {
            CHECK(!wl_evaluation_control_reset(f.control)
                && !wl_evaluation_control_configure(f.control, 0));
            step(&f); CHECK(!delta(&f, 0) && !s->last_removed_relation);
        }
        step(&f); snapshot(&f); insert(&f, 0, row, 1); step(&f);
        remove_rows(&f, 0, row, 1); step(&f); step(&f); snapshot(&f);
        fixture_close(&f);
    }
    puts(
        "post-staging real public controlled JOIN CANCEL/WORK, original heads+pending delta, exact refund/reset/retry/future events passed");
}

static void
inline_image_step(fixture_t *f, const int64_t *values, unsigned count)
{
    unsigned expected[2][SIDE][SIDE] = {{{0}}};
    for (unsigned i = 0; i < count; ++i) {
        CHECK(values[i] >= 0 && values[i] < SIDE);
        expected[0][values[i]][values[i]] = 1;
    }
    int rc; unsigned calls = 0;
    do {
        CHECK(++calls < 100000); rc = wl_session_step(f->session);
    }while (rc == EAGAIN);
    CHECK(!rc);
    for (unsigned a = 0; a < SIDE; ++a) for (unsigned b = 0; b < SIDE; ++b) {
            int difference = (int)expected[0][a][b] - (int)f->image[0][a][b];
            CHECK(f->events[0][a][b] == difference
                && f->deliveries[0][a][b] == (unsigned)(difference != 0));
        }
    memcpy(f->image, expected, sizeof(expected));
    memset(f->events, 0, sizeof(f->events));
    memset(f->deliveries, 0, sizeof(f->deliveries));
    snapshot(f);
}

/* Actual parsed INLINE EDB metadata, staged through public remove. This is a
 * one-source projection history, not a new INLINE JOIN admission certificate. */
static void
inline_staging_metadata(void)
{
    fixture_t f = {0}; f.count = 1;
    strcpy(f.names[0], "inline_source");
    const char *text =
        ".decl inline_source(p:pair(symbol,float) inline,v:int64)\n"
        ".decl out0(x:int64,y:int64)\nout0(v,v):-inline_source(pair(t,n),v).\n";
    wirelog_error_t error; f.program = wirelog_parse_string(text, &error);
    CHECK(f.program && !wl_plan_from_program(f.program, &f.plan));
    int64_t red = wl_intern_put(f.plan->intern, "red"),
        blue = wl_intern_put(f.plan->intern, "blue");
    CHECK(red >= 0 && blue >= 0 && red != blue);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(256) << 20,
            .usable_bytes = UINT64_C(256) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f.governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f.governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = f.governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f.plan, 1,
        &options, &f.session));
    wl_session_set_delta_cb(f.session, changed, &f);
    const int64_t data[] = {red, wl_columnar_float_to_bits(1.5), 7,
                            blue, wl_columnar_float_to_bits(2.5), 9};
    CHECK(!wl_session_insert(f.session, f.names[0], data, 2, 3));
    const int64_t both[] = {7, 9}, nine[] = {9}, seven[] = {7};
    inline_image_step(&f, both, 2);
    col_rel_t *source = session_find_rel(COL_SESSION(f.session), f.names[0]);
    CHECK(source && source->ncols == 3 && source->compound_arity_len
        && wl_columnar_relation_compound_map_valid(source));
    CHECK(!wl_session_remove(f.session, f.names[0], data, 1, 3));
    col_rel_t *d = delta(&f, 0); check_delta_schema(d, source);
    CHECK(d->nrows == 1 && d->columns[0][0] == red
        && d->columns[1][0] == wl_columnar_float_to_bits(1.5) &&
        d->columns[2][0] == 7);
    inline_image_step(&f, nine, 1); inline_image_step(&f, nine, 1);
    CHECK(!wl_session_insert(f.session, f.names[0], data, 1, 3));
    inline_image_step(&f, both, 2);
    CHECK(!wl_session_remove(f.session, f.names[0], data + 3, 1, 3));
    check_delta_schema(delta(&f, 0), source); inline_image_step(&f, seven, 1);
    fixture_close(&f);
    puts(
        "public INLINE symbol/FLOAT physical schema/map/declared width staged independently with signed projection futures passed");
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    if (!strcmp(argv[1], "public")) public_histories();
    else if (!strcmp(argv[1], "staging")) {
        pending_histories(); name_admission(); legacy_right_probe();
    } else if (!strcmp(argv[1], "typed")) typed_histories();
    else if (!strcmp(argv[1], "faults")) staging_faults();
    else if (!strcmp(argv[1], "stops")) controlled_staging_stops();
    else if (!strcmp(argv[1], "metadata")) inline_staging_metadata();
    else CHECK(false);
    return 0;
}
