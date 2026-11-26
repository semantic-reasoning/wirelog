/*
 * Observed removal staging and its consuming JOIN domains.
 * Copyright (C) CleverPlant. Licensed under LGPL-3.0.
 */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
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
fixture_open(fixture_t *f, unsigned length, bool keyed, bool controlled,
    bool governed, bool siblings, bool duplicates)
{
    memset(f, 0, sizeof(*f));
    f->keyed = keyed; f->width = keyed ? 2u : 1u; f->count = siblings ? 2u : 1u;
    CHECK(length < sizeof(f->names[0]));
    char text[16384]; size_t used = 0;
    for (unsigned n = 0; n < f->count; ++n) {
        memset(f->names[n], 'a', length); f->names[n][length] = '\0';
        if (n) f->names[n][length - 1] = 'b';
        int written = keyed ? snprintf(text + used, sizeof(text) - used,
                ".decl %s(k:int64,v:int64)\n.decl out%u(v:int64,w:int64)\n"
                "out%u(v,w):-%s(k,v),%s(k,w).\n", f->names[n], n, n,
                f->names[n], f->names[n]) : snprintf(text + used,
                sizeof(text) - used,
                ".decl %s(x:int64)\n.decl out%u(x:int64,y:int64)\n"
                "out%u(x,y):-%s(x),%s(y).\n", f->names[n], n, n,
                f->names[n], f->names[n]);
        CHECK(written > 0 && (size_t)written < sizeof(text) - used);
        used += (size_t)written;
    }
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
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
    const int64_t keys[] = {1, 7, 1, 9, 2, 8};
    for (unsigned n = 0; n < f->count; ++n)
        insert(f, n, keyed ? keys : duplicates ? duplicate : product,
            duplicates ? 4u : 3u);
    step(f); snapshot(f);
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

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    if (!strcmp(argv[1], "public")) public_histories();
    else if (!strcmp(argv[1], "staging")) {
        pending_histories(); name_admission(); legacy_right_probe();
    }else CHECK(false);
    return 0;
}
