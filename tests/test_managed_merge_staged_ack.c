/* Genuine merge retirement histories exercise the additive finite ACK leaf.
 * Session/Source caller qualification is separate from this direct fixture. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static int owner, wrong_parent;
static unsigned checkpoints;
static bool refuse_cancel;

static wl_evaluation_control_t *
control_new(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    return c;
}

static void
control_end(wl_evaluation_control_t *c)
{
    if (wl_evaluation_control_active_attempt_id(c))
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}

static uint64_t
restart(wl_evaluation_control_t *c, uint64_t limit)
{
    CHECK(!wl_evaluation_control_active_attempt_id(c));
    CHECK(!wl_evaluation_control_reset(c));
    CHECK(!wl_evaluation_control_configure(c, limit));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    return wl_evaluation_control_active_attempt_id(c);
}

static col_rel_t *
build_relation(wl_columnar_memory_governor_ref_t *g, bool canonical,
    const int64_t *values, uint32_t rows)
{
    wl_evaluation_control_t *c = control_new(0);
    wl_columnar_relation_managed_binary_build_t b = {0};
    CHECK(!wl_columnar_relation_managed_binary_build_begin(&b, rows, g));
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    for (unsigned i = 0;
        b.phase != WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_READY; ++i) {
        CHECK(i < 1000);
        int rc = wl_columnar_relation_managed_binary_build_step(&b, c, id);
        CHECK(!rc || rc == EAGAIN);
    }
    col_rel_t *r = b.relation;
    CHECK(r);
    for (uint32_t row = 0; row < rows; ++row)
        for (unsigned col = 0; col < 2; ++col)
            r->columns[col][row] = values[2 * row + col];
    r->nrows = rows;
    r->sorted_nrows = canonical ? rows : 0;
    r->run_count = 1;
    r->run_ends[0] = rows;
    control_end(c);
    return r;
}

static size_t
atomic_offset(unsigned index)
{
    return offsetof(wl_columnar_managed_candidate_merge_t, retirements)
           + index / 8u * sizeof(wl_columnar_relation_managed_retirement_t)
           + offsetof(wl_columnar_relation_managed_retirement_t, tokens)
           + (index % 8u) / 2u * sizeof(wl_columnar_memory_reservation_t)
           + (index % 2u ? offsetof(wl_columnar_memory_reservation_t, state)
                      : offsetof(wl_columnar_memory_reservation_t, owner_bits));
}

static void
checkpoint(wl_columnar_managed_candidate_merge_ack_progress_t *p,
    wl_evaluation_control_t *c, uint64_t id)
{
    CHECK(p->performed && p->report_pending && p->ack_prepaid && !p->busy);
    CHECK(p->support == c && p->support_id == id);
    ++checkpoints;
    /* Every performed action meets a real refusing footer, including all
     * 24 distinct typed atomic resets and the final ordinary tail. */
    if (refuse_cancel) wl_evaluation_control_request_cancel(c);
    else CHECK(wl_evaluation_control_charge_attempt(c, id, 1)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

typedef struct {
    wl_columnar_managed_candidate_merge_ack_progress_t *p;
    wl_columnar_managed_candidate_merge_t *target;
    wl_evaluation_control_t *c;
    uint64_t id;
    int rc;
} foreign_t;

static void *
foreign_step(void *arg)
{
    foreign_t *f = arg;
    bool done = false;
    f->rc = wl_columnar_managed_candidate_merge_ack_step_owned(f->p, &owner,
            f->target, f->c, f->id, &done);
    return NULL;
}

static void
settle(wl_columnar_managed_candidate_merge_ack_progress_t *p,
    wl_columnar_managed_candidate_merge_t *a, wl_evaluation_control_t *c,
    uint64_t id)
{
    size_t offset = p->clear_offset;
    unsigned ai = p->clear_atomic_index;
    bool done = false;
    CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(p, &owner,
        a, c, id, &done) == EBUSY);
    CHECK(p->clear_offset == offset && p->clear_atomic_index == ai);
    CHECK(wl_columnar_managed_candidate_merge_ack_action_ack_owned(p, &owner,
        c, id) == EBUSY);
    int rc = wl_evaluation_control_finish(c, &owner, 0, 0);
    CHECK(rc == (refuse_cancel ? WL_EVALUATION_CONTROL_CANCELLED
                              : WL_EVALUATION_CONTROL_WORK_LIMIT));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    CHECK(report.attempt_id == id && report.consumed == 256
        && !report.execution_status && !report.cleanup_status);
    CHECK(wl_columnar_managed_candidate_merge_ack_action_ack_owned(p,
        &wrong_parent, c, id) == EINVAL);
    CHECK(wl_columnar_managed_candidate_merge_ack_action_ack_owned(p,
        &owner, c, id + 1) == EBUSY);
    wl_evaluation_control_t *other = control_new(256);
    CHECK(!wl_evaluation_control_finish(other, &owner, 0, 0));
    CHECK(wl_columnar_managed_candidate_merge_ack_action_ack_owned(p,
        &owner, other, id) == EBUSY);
    control_end(other);
    CHECK(!wl_columnar_managed_candidate_merge_ack_action_ack_owned(p,
        &owner, c, id));
    CHECK(wl_columnar_managed_candidate_merge_ack_action_ack_owned(p,
        &owner, c, id) != 0);
}

static void
run_case(unsigned history, bool cancel)
{
    wl_columnar_memory_resolution_t resolution = {0};
    resolution.budget_bytes = resolution.usable_bytes =
        WL_COLUMNAR_MEMORY_MIN_BUDGET;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    const int64_t source_values[] = {1, 1, 2, 1};
    const int64_t candidate_values[] = {3, 0, 1, 1, 2, 2, 3, 0};
    col_rel_t *source = build_relation(g, true, source_values, 2);
    col_rel_t *candidate = build_relation(g, false, candidate_values, 4);
    col_rel_t *replacement = NULL, *delta = NULL;
    wl_evaluation_control_t *semantic = control_new(0);
    wl_evaluation_control_t *maintenance = control_new(0);
    wl_columnar_managed_candidate_merge_t a = {0};
    wl_columnar_managed_candidate_merge_ack_progress_t p = {0};
    uint64_t id = wl_evaluation_control_active_attempt_id(semantic);
    CHECK(!wl_columnar_managed_candidate_merge_begin(&a, source, candidate,
        semantic, id));
    bool selected = false;
    CHECK(wl_columnar_managed_candidate_merge_ack_begin_owned(&p, &owner,
        &a, maintenance, wl_evaluation_control_active_attempt_id(maintenance),
        &selected) == EINVAL && !selected && !p.identity);
    if (history != 0) {
        for (unsigned i = 0; a.phase != WL_COLUMNAR_MANAGED_CANDIDATE_READY;
            ++i) {
            CHECK(i < 10000);
            int rc = wl_columnar_managed_candidate_merge_advance(&a, 1);
            CHECK(!rc || rc == EAGAIN);
        }
    }
    if (history == 1) {
        CHECK(!wl_columnar_managed_candidate_merge_take_result(&a,
            &replacement, &delta));
        CHECK(replacement->nrows == 4 && delta->nrows == 2);
    } else CHECK(!wl_columnar_managed_candidate_merge_abort(&a));
    control_end(semantic);
    id = wl_evaluation_control_active_attempt_id(maintenance);
    for (unsigned i = 0; a.phase != WL_COLUMNAR_MANAGED_CANDIDATE_DONE; ++i) {
        CHECK(i < 10000);
        CHECK(!wl_columnar_managed_candidate_merge_cleanup_step(&a, maintenance,
            id));
    }
    CHECK(!wl_evaluation_control_finish(maintenance, &owner, 0, 0));
    unsigned unstarted = 0, released = 0, empty = 0;
    for (unsigned ri = 0; ri < 3; ++ri) {
        if (!a.retirement_started[ri]) {
            ++unstarted; continue;
        }
        for (unsigned ti = 0; ti < 4; ++ti) {
            wl_columnar_memory_reservation_t *t = &a.retirements[ri].tokens[ti];
            uint64_t state = atomic_load_explicit(&t->state,
                    memory_order_acquire);
            if (a.retirements[ri].token_empty[ti]) {
                CHECK(state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY); ++empty;
            } else {
                CHECK(state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
                CHECK(t->bytes && atomic_load_explicit(&t->owner_bits,
                    memory_order_acquire));
                ++released;
            }
        }
    }
    CHECK(history == 2 ? released > 0 : unstarted > 0);
    CHECK(history == 0 || (released && empty));
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(g));
    id = restart(maintenance, 255);
    CHECK(wl_columnar_managed_candidate_merge_ack_begin_owned(&p, &owner,
        &a, maintenance, id, &selected) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!selected && !p.identity && a.identity == (uintptr_t)&a);
    CHECK(wl_evaluation_control_finish(maintenance, &owner, 0, 0)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    id = restart(maintenance, 256);
    CHECK(wl_columnar_managed_candidate_merge_ack_begin_owned(&p, &owner,
        &a, maintenance, id, (bool *)&a) == EINVAL);
    refuse_cancel = cancel;
    checkpoints = 0;
    wl_columnar_managed_candidate_merge_test_ack_checkpoint = checkpoint;
    int refused = cancel ? WL_EVALUATION_CONTROL_CANCELLED
                         : WL_EVALUATION_CONTROL_WORK_LIMIT;
    CHECK(wl_columnar_managed_candidate_merge_ack_begin_owned(&p, &owner,
        &a, maintenance, id, &selected) == refused && selected);
    foreign_t f = {&p, &a, maintenance, id, 0};
    wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_step, &f));
    CHECK(!wl_thread_join(&thread) && f.rc == EBUSY);
    wl_columnar_managed_candidate_merge_ack_progress_t copy = p;
    bool done = false;
    CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&copy, &owner,
        &a, maintenance, id, &done) == EINVAL);
    settle(&p, &a, maintenance, id);
    unsigned atomic_actions = 0, ordinary_actions = 0;
    for (unsigned i = 0; wl_columnar_managed_candidate_merge_ack_pending(&p);
        ++i) {
        CHECK(i < 1000);
        size_t before = p.clear_offset;
        unsigned ai = p.clear_atomic_index;
        id = restart(maintenance, 255);
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, &done) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!p.report_pending && p.clear_offset == before
            && p.clear_atomic_index == ai);
        CHECK(wl_evaluation_control_finish(maintenance, &owner, 0, 0)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        id = restart(maintenance, 256);
        /* Corrupt only the authentic external cursor, observe rejection,
         * then restore it without fabricating any target resource state. */
        p.clear_offset = sizeof(a) + 1;
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, &done) == EINVAL && !p.report_pending);
        p.clear_offset = before;
        p.clear_atomic_index = 25;
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, &done) == EINVAL && !p.report_pending);
        p.clear_atomic_index = ai;
        unsigned ri = p.retirement_index;
        p.retirement_index = p.phase == WL_COLUMNAR_MANAGED_MERGE_ACK_CLEAR
            ? 2 : 3;
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, &done) == EINVAL && !p.report_pending);
        p.retirement_index = ri;
        unsigned ti = p.token_index;
        p.token_index = 4;
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, &done) == EINVAL && !p.report_pending);
        p.token_index = ti;
        if (ai < 24) {
            p.clear_offset = atomic_offset(ai) + 1;
            CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p,
                &owner, &a, maintenance, id, &done) == EINVAL
                && !p.report_pending);
            p.clear_offset = before;
        }
        if (ai) {
            p.clear_offset = atomic_offset(ai - 1);
            CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p,
                &owner, &a, maintenance, id, &done) == EINVAL
                && !p.report_pending);
            p.clear_offset = before;
        }
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, (bool *)&p) == EINVAL && !p.report_pending);
        CHECK(wl_columnar_managed_candidate_merge_ack_begin_owned(&p, &owner,
            &a, maintenance, id, &selected) == EBUSY);
        bool clearing = p.phase == WL_COLUMNAR_MANAGED_MERGE_ACK_CLEAR;
        CHECK(wl_columnar_managed_candidate_merge_ack_step_owned(&p, &owner,
            &a, maintenance, id, &done) == refused);
        if (clearing) {
            if (p.clear_atomic_index != ai) {
                CHECK(ai < 24 && before == atomic_offset(ai));
                CHECK(p.clear_atomic_index == ai + 1
                    && p.clear_offset == before + sizeof(wl_atomic_u64));
                ++atomic_actions;
            } else {
                size_t boundary = ai < 24 ? atomic_offset(ai) : sizeof(a);
                CHECK(p.clear_offset > before && p.clear_offset <= boundary
                    && p.clear_offset - before <= 64);
                ++ordinary_actions;
            }
            CHECK(!a.identity);
        }
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g))
            == baseline);
        settle(&p, &a, maintenance, id);
    }
    wl_columnar_managed_candidate_merge_test_ack_checkpoint = NULL;
    CHECK(done && atomic_actions == 24 && ordinary_actions > 24
        && checkpoints > 24 && !p.identity && !a.identity);
    const unsigned char *bytes = (const unsigned char *)&a;
    size_t begin = 0;
    for (unsigned k = 0; k <= 24; ++k) {
        size_t end = k < 24 ? atomic_offset(k) : sizeof(a);
        for (size_t pos = begin; pos < end; ++pos) CHECK(!bytes[pos]);
        if (k < 24) begin = end + sizeof(wl_atomic_u64);
    }
    for (unsigned ri = 0; ri < 3; ++ri)
        for (unsigned ti = 0; ti < 4; ++ti) {
            CHECK(!atomic_load_explicit(
                    &a.retirements[ri].tokens[ti].owner_bits,
                    memory_order_acquire));
            CHECK(!atomic_load_explicit(&a.retirements[ri].tokens[ti].state,
                memory_order_acquire));
            CHECK(!a.retirements[ri].tokens[ti].identity);
        }
    control_end(maintenance);
    if (replacement) CHECK(!col_rel_destroy_checked(replacement));
    if (delta) CHECK(!col_rel_destroy_checked(delta));
    CHECK(!col_rel_destroy_checked(candidate));
    CHECK(!col_rel_destroy_checked(source));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}

int
main(void)
{
    for (unsigned history = 0; history < 3; ++history)
        for (unsigned cancel = 0; cancel < 2; ++cancel)
            run_case(history, cancel != 0);
    puts(
        "managed merge finite ACK: six genuine retirement/refusal histories passed");
    return 0;
}
