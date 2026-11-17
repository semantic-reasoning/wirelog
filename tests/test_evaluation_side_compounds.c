/* #2301: side compound EDBs in controlled single-worker sessions.
 *
 * A column declared `pair/2 side` holds the handle that
 * wl_session_make_compound returns; the compound's arguments are a row of
 * the side relation `__compound_pair_2`, and a body that destructures the
 * column joins with that relation. This test runs such programs through a
 * controlled session (an evaluation control, a minimum-budget enforcing
 * governor, one worker) and an uncontrolled one with the same operations.
 *
 * make_compound after a completed controlled evaluation: the test checks
 * that it returns 0 after a completed STEP, a completed SNAPSHOT and a
 * completed observed STEP, also when the side relation does not exist yet,
 * and that a later evaluation reads the new compound, both through a row
 * inserted with it and through a row inserted before it with the handle it
 * was then given. On the recursive edge host and on the observed
 * single-rule host it checks EBUSY beside an insert those hosts admit in
 * the same state. */
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
                                      __LINE__, #x); abort(); } } while (0)

static const char *where = "";

/* ---- sessions ------------------------------------------------------------ */

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_columnar_memory_governor_ref_t *governor;
    wl_evaluation_control_t *control;
    wl_session_t *s;   /* controlled */
    wl_session_t *raw; /* uncontrolled */
} fixture_t;

static void
fixture(fixture_t *f, const char *text)
{
    memset(f, 0, sizeof(*f));
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &f->control));
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.evaluation_control = f->control;
    options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->s));
    CHECK(!wl_session_create(wl_backend_columnar(), f->plan, 1, &f->raw));
}

static void
fixture_destroy(fixture_t *f)
{
    wl_session_destroy(f->s);
    wl_session_destroy(f->raw);
    wl_evaluation_control_release(f->control);
    wl_plan_free(f->plan);
    wirelog_program_free(f->program);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(f->governor)) == 0);
    wl_columnar_memory_governor_ref_release(f->governor);
}

/* ---- images and events --------------------------------------------------- */

#define MAX_ROWS 64

typedef struct {
    const char *head;
    uint32_t count;
    int64_t rows[MAX_ROWS][4];
    int diff[MAX_ROWS];
} image_t;

static void
image_add(image_t *m, const int64_t *v, uint32_t width, int diff)
{
    CHECK(width <= 4 && m->count < MAX_ROWS);
    memset(m->rows[m->count], 0, sizeof(m->rows[m->count]));
    memcpy(m->rows[m->count], v, width * sizeof(*v));
    m->diff[m->count] = diff;
    m->count++;
}

static void
on_tuple(const char *name, const int64_t *v, uint32_t width, void *data)
{
    image_t *m = data;
    if (m && !strcmp(name, m->head))
        image_add(m, v, width, 1);
}

static image_t *events;

static void
on_delta(const char *name, const int64_t *v, uint32_t width, int diff,
    void *data)
{
    (void)data;
    if (events && !strcmp(name, events->head))
        image_add(events, v, width, diff);
}

static int
row_cmp(const void *a, const void *b)
{
    return memcmp(a, b, 4 * sizeof(int64_t));
}

/* Sort rows with their diffs, then compare. */
static void
image_sort(image_t *m)
{
    for (uint32_t i = 1; i < m->count; i++)
        for (uint32_t j = i; j > 0; j--) {
            int c = row_cmp(m->rows[j - 1], m->rows[j]);
            if (c < 0 || (c == 0 && m->diff[j - 1] <= m->diff[j]))
                break;
            int64_t t[4];
            memcpy(t, m->rows[j], sizeof(t));
            memcpy(m->rows[j], m->rows[j - 1], sizeof(t));
            memcpy(m->rows[j - 1], t, sizeof(t));
            int d = m->diff[j];
            m->diff[j] = m->diff[j - 1];
            m->diff[j - 1] = d;
        }
}

static bool
image_equal(image_t *a, image_t *b)
{
    image_sort(a);
    image_sort(b);
    if (a->count != b->count)
        return false;
    for (uint32_t i = 0; i < a->count; i++)
        if (row_cmp(a->rows[i], b->rows[i]) || a->diff[i] != b->diff[i])
            return false;
    return true;
}

static void
image_print(const char *label, const image_t *m)
{
    fprintf(stderr, "%s: %s %s (%u):", where, label, m->head, m->count);
    for (uint32_t i = 0; i < m->count; i++)
        fprintf(stderr, " (%lld,%lld,%lld)%+d", (long long)m->rows[i][0],
            (long long)m->rows[i][1], (long long)m->rows[i][2], m->diff[i]);
    fprintf(stderr, "\n");
}

/* The bound turns a livelock into a prompt CHECK failure. */
static int
until_done(wl_session_t *s, bool snapshot, image_t *m)
{
    for (unsigned i = 0; i < 200000; i++) {
        int rc = snapshot ? wl_session_snapshot(s, on_tuple, m)
            : wl_session_step(s);
        if (rc != EAGAIN)
            return rc;
    }
    CHECK(false);
    return EIO;
}

/* ---- compounds ----------------------------------------------------------- */

static int
make_pair(wl_session_t *s, int64_t x, int64_t y, uint64_t *handle)
{
    wirelog_compound_arg_t args[2] = {
        {WIRELOG_TYPE_INT64, x}, {WIRELOG_TYPE_INT64, y}
    };
    *handle = UINT64_C(123);
    return wl_session_make_compound(s, "pair", 2, args, handle);
}

static uint32_t
side_rows(wl_session_t *s)
{
    const col_rel_t *r = session_find_rel((wl_col_session_t *)s,
            "__compound_pair_2");
    return r ? r->nrows : 0;
}

#define DESTR ".decl e(id:int64,h:pair/2 side)\n" \
        ".decl out(id:int64,x:int64,y:int64)\n" \
        "out(i,x,y) :- e(i,pair(x,y)).\n"

/* Evaluate both sessions (@how: 's' STEP, 'S' SNAPSHOT) and require 0 and
 * the literal image @want of @n rows (x of id i is 10 * i, y is x + 1), and
 * with an observer the literal events @ev of @nev rows. */
static void
evaluate(fixture_t *f, char how, const int64_t *want, uint32_t n,
    const int64_t *ev, const int *evd, uint32_t nev, bool observed)
{
    image_t expect = {.head = "out"}, expect_ev = {.head = "out"};
    for (uint32_t i = 0; i < n; i++)
        image_add(&expect, (int64_t[3]){want[i], 10 * want[i],
                                        10 * want[i] + 1}, 3, 1);
    for (uint32_t i = 0; i < nev; i++)
        image_add(&expect_ev, (int64_t[3]){ev[i], 10 * ev[i],
                                           10 * ev[i] + 1}, 3, evd[i]);
    wl_session_t *both[2] = {f->s, f->raw};
    for (int k = 0; k < 2; k++) {
        image_t ev_seen = {.head = "out"}, img = {.head = "out"};
        events = &ev_seen;
        CHECK(!until_done(both[k], how == 'S', &img));
        events = NULL;
        if (how == 's') {
            img.count = 0;
            CHECK(!until_done(both[k], true, &img));
        }
        if (!image_equal(&img, &expect)) {
            image_print(k ? "uncontrolled" : "controlled", &img);
            image_print("expected", &expect);
        }
        CHECK(image_equal(&img, &expect));
        if (observed && how == 's') {
            if (!image_equal(&ev_seen, &expect_ev)) {
                image_print(k ? "uncontrolled events" : "controlled events",
                    &ev_seen);
                image_print("expected events", &expect_ev);
            }
            CHECK(image_equal(&ev_seen, &expect_ev));
        }
    }
}

/* make_compound pair(10 * id, 10 * id + 1) in both sessions, require 0,
 * and insert e(id, handle) unless @insert is false. */
static void
make_and_insert(fixture_t *f, int64_t id, bool insert, uint64_t handles[2])
{
    wl_session_t *both[2] = {f->s, f->raw};
    for (int k = 0; k < 2; k++) {
        uint32_t before = side_rows(both[k]);
        int rc = make_pair(both[k], 10 * id, 10 * id + 1, &handles[k]);
        if (rc)
            fprintf(stderr, "%s: %s make_compound %d\n", where,
                k ? "uncontrolled" : "controlled", rc);
        CHECK(rc == 0 && handles[k] != WIRELOG_COMPOUND_HANDLE_NULL);
        CHECK(side_rows(both[k]) == before + 1);
        if (insert)
            CHECK(!wl_session_insert(both[k], "e",
                (int64_t[2]){id, (int64_t)handles[k]}, 1, 2));
    }
}

/* After each kind of completed controlled evaluation, make_compound
 * returns 0 and the next evaluation reads the compound. */
static void
after_completion(char first, bool observed, bool make_first)
{
    fixture_t f;
    fixture(&f, DESTR);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, NULL);
        wl_session_set_delta_cb(f.raw, on_delta, NULL);
    }
    uint64_t h[2];
    static const int plus[] = {1, 1, 1};
    if (make_first) {
        make_and_insert(&f, 1, true, h);
        evaluate(&f, first, (int64_t[]){1}, 1, (int64_t[]){1}, plus, 1,
            observed);
    } else {
        /* The side relation does not exist when the evaluation completes. */
        CHECK(!session_find_rel((wl_col_session_t *)f.s,
            "__compound_pair_2"));
        evaluate(&f, first, NULL, 0, NULL, NULL, 0, observed);
        make_and_insert(&f, 1, true, h);
        evaluate(&f, 's', (int64_t[]){1}, 1, (int64_t[]){1}, plus, 1,
            observed);
    }
    make_and_insert(&f, 2, true, h);
    evaluate(&f, 's', (int64_t[]){1, 2}, 2, (int64_t[]){2}, plus, 1,
        observed);
    /* A second make_compound with no insert between the evaluations. */
    make_and_insert(&f, 3, true, h);
    make_and_insert(&f, 4, false, h);
    CHECK(!wl_session_insert(f.s, "e", (int64_t[2]){4, (int64_t)h[0]}, 1, 2));
    CHECK(!wl_session_insert(f.raw, "e", (int64_t[2]){4, (int64_t)h[1]}, 1,
        2));
    evaluate(&f, first, (int64_t[]){1, 2, 3, 4}, 4, (int64_t[]){3, 4}, plus,
        2, observed);
    fixture_destroy(&f);
}

static wl_controlled_step_state_t
step_state(wl_session_t *s)
{
    return s->backend->session_controlled_step_state(s);
}

/* A row inserted with a handle no compound holds yet derives nothing; the
 * make_compound that later yields that handle, with no insert after it,
 * makes the next evaluation derive the row. Handles here are the arena's
 * consecutive allocations within one epoch: the third is predicted from
 * the first two in each session. */
static void
forward_reference(bool observed)
{
    fixture_t f;
    fixture(&f, DESTR);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, NULL);
        wl_session_set_delta_cb(f.raw, on_delta, NULL);
    }
    uint64_t h0[2], h1[2], h2[2];
    make_and_insert(&f, 1, true, h0);
    make_and_insert(&f, 2, false, h1);
    static const int plus[] = {1};
    evaluate(&f, 's', (int64_t[]){1}, 1, (int64_t[]){1}, plus, 1, observed);
    wl_session_t *both[2] = {f.s, f.raw};
    uint64_t next[2];
    for (int k = 0; k < 2; k++) {
        next[k] = h1[k] + (h1[k] - h0[k]);
        CHECK(!wl_session_insert(both[k], "e",
            (int64_t[2]){3, (int64_t)next[k]}, 1, 2));
    }
    evaluate(&f, 's', (int64_t[]){1}, 1, NULL, NULL, 0, observed);
    wl_controlled_step_state_t state = step_state(f.s);
    if (state != WL_CONTROLLED_STEP_COMPLETE)
        fprintf(stderr, "%s: state %d after the evaluation\n", where,
            (int)state);
    CHECK(state == WL_CONTROLLED_STEP_COMPLETE);
    make_and_insert(&f, 3, false, h2);
    CHECK(h2[0] == next[0] && h2[1] == next[1]);
    /* The compound alone makes the completed host dirty, as an insert
     * does. */
    state = step_state(f.s);
    if (state != WL_CONTROLLED_STEP_DIRTY)
        fprintf(stderr, "%s: state %d after make_compound\n", where,
            (int)state);
    CHECK(state == WL_CONTROLLED_STEP_DIRTY);
    evaluate(&f, 's', (int64_t[]){1, 3}, 2, (int64_t[]){3}, plus, 1,
        observed);
    fixture_destroy(&f);
}

/* Hosts that refuse make_compound after a completed controlled evaluation,
 * each beside an insert that it admits in the same state; the uncontrolled
 * session returns 0 for both. */
static void
refusal(const char *text, const char *input, uint32_t width, bool observed)
{
    fixture_t f;
    fixture(&f, text);
    if (observed) {
        wl_session_set_delta_cb(f.s, on_delta, NULL);
        wl_session_set_delta_cb(f.raw, on_delta, NULL);
    }
    int64_t row[2] = {1, 2};
    CHECK(!wl_session_insert(f.s, input, row, 1, width));
    CHECK(!wl_session_insert(f.raw, input, row, 1, width));
    int first = until_done(f.s, false, NULL);
    if (first)
        fprintf(stderr, "%s: controlled first STEP %d\n", where, first);
    CHECK(!first);
    CHECK(!until_done(f.raw, false, NULL));
    uint64_t h = 0;
    int rc = make_pair(f.s, 1, 2, &h);
    if (rc != EBUSY)
        fprintf(stderr, "%s: controlled make_compound %d\n", where, rc);
    CHECK(rc == EBUSY);
    CHECK(!make_pair(f.raw, 1, 2, &h));
    row[0] = 2; row[1] = 3;
    CHECK(!wl_session_insert(f.s, input, row, 1, width));
    CHECK(!wl_session_insert(f.raw, input, row, 1, width));
    CHECK(!until_done(f.s, false, NULL));
    CHECK(!until_done(f.raw, false, NULL));
    fixture_destroy(&f);
}

int
main(void)
{
    static const char modes[] = {'s', 'S'};
    for (int m = 0; m < 2; m++)
        for (int observed = 0; observed < 2; observed++)
            for (int make_first = 0; make_first < 2; make_first++) {
                char label[64];
                snprintf(label, sizeof(label), "after %c%s%s", modes[m],
                    observed ? " observed" : "",
                    make_first ? " made first" : "");
                where = label;
                after_completion(modes[m], observed, make_first);
            }
    where = "forward";
    forward_reference(false);
    where = "forward observed";
    forward_reference(true);
    where = "recursive edge host";
    refusal(".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        "path(x,y) :- edge(x,y).\npath(x,z) :- path(x,y), edge(y,z).\n",
        "edge", 2, false);
    where = "observed single-rule host";
    refusal(".decl in(x:int64)\n.decl out(x:int64)\nout(x) :- in(x).\n",
        "in", 1, true);
    printf("ok\n");
    return 0;
}
