/* Real resumable baseline construction and original reader transitions. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/evaluation_control.h"
#include "../wirelog/thread.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

extern void (*wl_columnar_relation_test_before_reader_cas)(
    wl_evaluation_control_t *, wl_columnar_source_access_gate_t *, uint64_t);
extern void (*wl_columnar_relation_test_after_reader_cas)(
    wl_evaluation_control_t *, wl_columnar_source_access_gate_t *, bool);

static int owner;
static unsigned events;
static int64_t total;
static void delta(const char *name, const int64_t *row, uint32_t width,
    int diff, void *data)
{
    (void)data;
    assert(strcmp(name, "head") == 0 && width == 1 && diff == -1);
    events++; total += row[0];
}
static wl_evaluation_control_t *control(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    assert(!wl_evaluation_control_create(limit, &c));
    assert(!wl_evaluation_control_attach(c, &owner));
    assert(!wl_evaluation_control_begin(c, &owner));
    return c;
}
static void close_control(wl_evaluation_control_t *c)
{
    (void)wl_evaluation_control_finish(c, &owner, 0, 0);
    assert(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}
static void stop_after_cas(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *g, bool acquired)
{
    (void)g;
    if (acquired) wl_evaluation_control_request_cancel(c);
}
static void interfere(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *g, uint64_t value)
{
    (void)c;
    atomic_store_explicit(&g->state, value + 1, memory_order_relaxed);
    wl_columnar_relation_test_before_reader_cas = NULL;
}
static void *release_on_thread(void *arg)
{
    wl_columnar_relation_reader_step_t *record = arg;
    const col_rel_t *r = record->relation;
    assert(wl_columnar_relation_reader_step(r, record, true, NULL,
        0) == EAGAIN);
    assert(!wl_columnar_relation_reader_step(r, record, true, NULL, 0));
    return NULL;
}
static void reader_steps(void)
{
    col_rel_t *r = col_rel_new_auto("reader", 1);
    assert(r);
    wl_columnar_relation_reader_step_t reader = { 0 };
    wl_evaluation_control_t *c = control(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_columnar_relation_test_before_reader_cas = interfere;
    assert(wl_columnar_relation_reader_step(r, &reader, false, c,
        id) == EAGAIN);
    assert(!reader.identity && !reader.token.identity);
    atomic_store_explicit(&r->descriptor_access.state, 0, memory_order_relaxed);
    wl_columnar_relation_test_after_reader_cas = stop_after_cas;
    assert(wl_columnar_relation_reader_step(r, &reader, false, c, id)
        == WL_EVALUATION_CONTROL_CANCELLED);
    assert(reader.descriptor_held && !reader.token.identity &&
        !reader.token.owner);
    assert(col_rel_destroy_checked(r) == EBUSY);
    wl_columnar_relation_test_after_reader_cas = NULL;
    close_control(c); c = control(0);
    id = wl_evaluation_control_active_attempt_id(c);
    assert(!wl_columnar_relation_reader_step(r, &reader, false, c, id));
    wl_columnar_relation_test_before_reader_cas = interfere;
    assert(wl_columnar_relation_reader_step(r, &reader, true, NULL,
        0) == EAGAIN);
    assert(reader.token.owner == &r->source_access && reader.descriptor_held);
    assert(atomic_fetch_sub_explicit(&r->source_access.state, 1,
        memory_order_relaxed) == 2);
    atomic_store_explicit(&r->source_access.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER, memory_order_relaxed);
    assert(wl_columnar_relation_reader_step(r, &reader, true, NULL,
        0) == EINVAL);
    assert(reader.token.owner == &r->source_access && reader.descriptor_held);
    atomic_store_explicit(&r->source_access.state, 1, memory_order_relaxed);
    wl_columnar_relation_reader_step_t copy = reader;
    assert(wl_columnar_relation_reader_step(r, &copy, true, NULL, 0) == EINVAL);
    wl_thread_t thread;
    assert(!wl_thread_create(&thread, release_on_thread, &reader));
    wl_thread_join(&thread);
    assert(!reader.identity && !reader.descriptor_held && !reader.token.owner);
    assert(!atomic_load_explicit(&r->source_access.state,
        memory_order_relaxed));
    assert(!atomic_load_explicit(&r->descriptor_access.state,
        memory_order_relaxed));
    close_control(c);
    assert(!col_rel_destroy_checked(r));
}
static void build(bool stepped)
{
    wl_plan_relation_t heads[2] = { { .name = "head" }, { .name = "head" } };
    wl_plan_stratum_t stratum = { .relations = heads, .relation_count = 2 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    col_rel_t *rels[3] = { col_rel_new_auto("head", 1),
                           col_rel_new_auto("$out$head", 1),
                           col_rel_new_auto("$in$head", 1) };
    wl_col_session_t sess = { .plan = &plan, .rels = rels, .nrels = 3,
                              .delta_cb = delta };
    wl_columnar_memory_reservation_init(
        &sess.delta_observer_retirement_metadata);
    int64_t values[] = { 3, 1, 2, 2, 7 };
    for (unsigned i = 0; i < 4;
        i++) assert(!col_rel_append_row(rels[0], &values[i]));
    assert(!col_rel_append_row(rels[1], &values[4]));
    assert(!col_rel_append_row(rels[2], &values[2]));
    if (stepped) {
        assert(!wl_columnar_eval_delta_observer_build_start(&sess, UINT64_MAX));
        wl_col_session_t worker = { 0 };
        assert(col_worker_session_create(&sess, 0, NULL, 0, &worker) == EBUSY);
        assert(!worker.delta_observer_build);
        bool ready = false;
        wl_evaluation_control_t *c = control(7);
        unsigned restarts = 0, calls = 0;
        while (!ready) {
            assert(!sess.delta_observer);
            int rc = wl_columnar_eval_delta_observer_build_advance(&sess, c,
                    wl_evaluation_control_active_attempt_id(c), &ready);
            assert(++calls < 20000);
            if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT) {
                uint64_t old_id = wl_evaluation_control_active_attempt_id(c);
                (void)wl_evaluation_control_finish(c, &owner, 0, 0);
                assert(!wl_evaluation_control_reset(c));
                assert(!wl_evaluation_control_begin(c, &owner));
                assert(wl_columnar_eval_delta_observer_build_advance(&sess, c,
                    old_id, &ready) == EINVAL);
                restarts++;
            } else assert(rc == EAGAIN || (!rc && ready));
        }
        assert(restarts > 3 && sess.delta_observer &&
            !sess.delta_observer_build);
        close_control(c);
    } else assert(!wl_columnar_eval_delta_observer_begin(&sess, UINT64_MAX));
    assert(!atomic_load_explicit(&rels[0]->source_access.state,
        memory_order_relaxed));
    rels[0]->nrows = 0;
    events = 0; total = 0;
    assert(!wl_columnar_eval_delta_observer_prepare(&sess));
    wl_columnar_eval_delta_observer_publish(&sess);
    assert(events == 4 && total == 13);
    assert(!wl_columnar_eval_delta_observer_discard(&sess));
    session_rel_free_hash(&sess);
    for (unsigned i = 0; i < 3; i++) assert(!col_rel_destroy_checked(rels[i]));
}
static void byte_order_no_change(bool stepped)
{
    wl_plan_relation_t heads[2] = { { .name = "head" }, { .name = "head" } };
    wl_plan_stratum_t stratum = { .relations = heads, .relation_count = 2 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    col_rel_t *rels[] = { col_rel_new_auto("head", 1),
                          col_rel_new_auto("$out$head", 1),
                          col_rel_new_auto("$in$head", 1) };
    assert(rels[0] && rels[1] && rels[2]);
    const int64_t values[] = { 1, 256, -1, -2, 0 };
    const int64_t retired[] = { 17, -257 };
    const int64_t pending[] = { 256, -2 };
    for (unsigned i = 0; i < sizeof(values) / sizeof(values[0]); i++)
        assert(!col_rel_append_row(rels[0], &values[i]));
    for (unsigned i = 0; i < sizeof(retired) / sizeof(retired[0]); i++)
        assert(!col_rel_append_row(rels[1], &retired[i]));
    for (unsigned i = 0; i < sizeof(pending) / sizeof(pending[0]); i++)
        assert(!col_rel_append_row(rels[2], &pending[i]));
    wl_col_session_t s = { .plan = &plan, .rels = rels, .nrels = 3,
                           .delta_cb = delta };
    wl_columnar_memory_reservation_init(&s.delta_observer_retirement_metadata);
    if (stepped) {
        assert(!wl_columnar_eval_delta_observer_build_start(&s, UINT64_MAX));
        wl_evaluation_control_t *c = control(0);
        bool ready = false;
        while (!ready) {
            int rc = wl_columnar_eval_delta_observer_build_advance(&s, c,
                    wl_evaluation_control_active_attempt_id(c), &ready);
            assert(rc == EAGAIN || (!rc && ready));
        }
        close_control(c);
    } else assert(!wl_columnar_eval_delta_observer_begin(&s, UINT64_MAX));
    rels[0]->nrows = 0;
    const int64_t current[] = { 1, -1, 0, 17, -257 };
    for (unsigned i = 0; i < sizeof(current) / sizeof(current[0]); i++)
        assert(!col_rel_append_row(rels[0], &current[i]));
    events = 0;
    assert(!wl_columnar_eval_delta_observer_prepare(&s));
    assert(!events);
    wl_columnar_eval_delta_observer_publish(&s);
    assert(!events);
    assert(!wl_columnar_eval_delta_observer_discard(&s));
    session_rel_free_hash(&s);
    for (unsigned i = 0; i < 3; i++) assert(!col_rel_destroy_checked(rels[i]));
}
static void large_reverse_input_is_bounded(void)
{
    enum { ROWS = 4096, MAX_ADVANCES = 1000000 };
    wl_plan_relation_t head = { .name = "head" };
    wl_plan_stratum_t stratum = { .relations = &head, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    col_rel_t *r = col_rel_new_auto("head", 1);
    assert(r);
    for (int64_t i = ROWS - 1; i >= 0; i--)
        assert(!col_rel_append_row(r, &i));
    col_rel_t *rels[] = { r };
    wl_col_session_t s = { .plan = &plan, .rels = rels, .nrels = 1,
                           .delta_cb = delta };
    wl_columnar_memory_reservation_init(&s.delta_observer_retirement_metadata);
    assert(!wl_columnar_eval_delta_observer_build_start(&s, UINT64_MAX));
    wl_evaluation_control_t *c = control(0);
    bool ready = false;
    unsigned calls = 0;
    while (!ready) {
        int rc = wl_columnar_eval_delta_observer_build_advance(&s, c,
                wl_evaluation_control_active_attempt_id(c), &ready);
        assert((rc == EAGAIN || (!rc && ready))
            && ++calls < MAX_ADVANCES);
    }
    close_control(c);
    events = 0;
    assert(!wl_columnar_eval_delta_observer_prepare(&s));
    assert(!events);
    assert(!wl_columnar_eval_delta_observer_discard(&s));
    session_rel_free_hash(&s);
    assert(!col_rel_destroy_checked(r));
}
static void unsupported(void)
{
    wl_plan_relation_t head = { .name = "bad" };
    wl_plan_stratum_t stratum = { .relations = &head, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    col_rel_t *r = col_rel_new_auto("bad", 2);
    wl_col_session_t s = { .plan = &plan, .rels = &r, .nrels = 1 };
    assert(!wl_columnar_eval_delta_observer_build_start(&s, 1));
    wl_evaluation_control_t *c = control(0);
    bool ready; int rc;
    do {
        rc = wl_columnar_eval_delta_observer_build_advance(&s, c,
                wl_evaluation_control_active_attempt_id(c), &ready);
    } while (rc == EAGAIN);
    assert(rc == ENOTSUP && !ready && !s.delta_observer);
    assert(!atomic_load_explicit(&r->descriptor_access.state,
        memory_order_relaxed));
    assert(!wl_columnar_eval_delta_observer_build_discard(&s));
    close_control(c); assert(!col_rel_destroy_checked(r));
}
static uint64_t baseline_reservation(unsigned head_count)
{
    wl_plan_relation_t heads[2] = { { .name = "head" }, { .name = "head" } };
    wl_plan_stratum_t stratum = { .relations = heads,
                                  .relation_count = head_count };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    col_rel_t *r = col_rel_new_auto("head", 1);
    assert(r);
    for (int64_t i = 0; i < 256; i++) assert(!col_rel_append_row(r, &i));
    col_rel_t *rels[] = { r };
    wl_col_session_t s = { .plan = &plan, .rels = rels, .nrels = 1 };
    wl_columnar_memory_reservation_init(&s.delta_observer_retirement_metadata);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    s.memory_governor = wl_columnar_memory_governor_ref_create(&resolution);
    assert(s.memory_governor);
    wl_evaluation_control_t *c = control(0);
    assert(!wl_columnar_eval_delta_observer_build_start(&s, 1));
    bool ready = false;
    unsigned calls = 0;
    while (!ready) {
        int rc = wl_columnar_eval_delta_observer_build_advance(&s, c,
                wl_evaluation_control_active_attempt_id(c), &ready);
        assert((rc == EAGAIN || (!rc && ready)) && ++calls < 10000);
    }
    uint64_t reserved = s.delta_observer_reserved_bytes;
    assert(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s.memory_governor)) ==
        reserved);
    assert(!wl_columnar_eval_delta_observer_discard(&s));
    assert(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s.memory_governor)));
    session_rel_free_hash(&s);
    wl_columnar_memory_governor_ref_release(s.memory_governor);
    close_control(c);
    assert(!col_rel_destroy_checked(r));
    return reserved;
}
static void duplicate_heads_do_not_double_reserve(void)
{
    uint64_t single = baseline_reservation(1);
    uint64_t duplicate = baseline_reservation(2);
    assert(single == duplicate);
}
static void *discard_on_thread(void *arg)
{
    wl_col_session_t *s = arg;
    int rc;
    do {
        rc = wl_columnar_eval_delta_observer_build_discard(s);
    } while (rc == EAGAIN);
    assert(!rc && !s->delta_observer_build);
    return NULL;
}
static void partial_cleanup(void)
{
    wl_plan_relation_t head = { .name = "head" };
    wl_plan_stratum_t stratum = { .relations = &head, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    col_rel_t *r = col_rel_new_auto("head", 1);
    int64_t values[] = { 4, 3, 2, 1 };
    for (unsigned i = 0; i < 4; i++) assert(!col_rel_append_row(r, &values[i]));
    wl_col_session_t s = { .plan = &plan, .rels = &r, .nrels = 1 };
    wl_columnar_memory_reservation_init(&s.delta_observer_retirement_metadata);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    s.memory_governor = wl_columnar_memory_governor_ref_create(&resolution);
    assert(s.memory_governor);
    bool completed = false;
    unsigned held = 0;
    /* Every production transition has a discard witness, including partially
     * zeroed metadata and descriptor-only acquisition, not synthetic states. */
    for (unsigned cut = 0; cut < 3000 && !completed; cut++) {
        assert(!wl_columnar_eval_delta_observer_build_start(&s, 1));
        wl_evaluation_control_t *c = control(0);
        for (unsigned i = 0; i < cut; i++) {
            int rc = wl_columnar_eval_delta_observer_build_advance(&s, c,
                    wl_evaluation_control_active_attempt_id(c), &completed);
            assert(rc == EAGAIN || (!rc && completed));
            if (completed) break;
        }
        if (atomic_load_explicit(&r->descriptor_access.state,
            memory_order_relaxed)) held++;
        close_control(c);
        if (completed) assert(!wl_columnar_eval_delta_observer_discard(&s));
        else {
            wl_thread_t thread;
            assert(!wl_thread_create(&thread, discard_on_thread, &s));
            wl_thread_join(&thread);
        }
        assert(!atomic_load_explicit(&r->descriptor_access.state,
            memory_order_relaxed));
        assert(!atomic_load_explicit(&r->source_access.state,
            memory_order_relaxed));
    }
    assert(completed && held >= 2);
    assert(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s.memory_governor)));
    wl_columnar_memory_governor_ref_release(s.memory_governor);
    assert(!col_rel_destroy_checked(r));
}
int main(void)
{
    reader_steps(); build(false); build(true);
    byte_order_no_change(false); byte_order_no_change(true);
    large_reverse_input_is_bounded(); unsupported();
    duplicate_heads_do_not_double_reserve(); partial_cleanup();
    puts("observer stepped construction PASS");
    return 0;
}
