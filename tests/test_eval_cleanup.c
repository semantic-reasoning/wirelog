/* Persistent evaluator cleanup ownership (Issue #1661).
* Copyright (C) CleverPlant. Licensed under LGPL-3.0. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <stdio.h>
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #c); exit(1); \
                      } } while (0)

static wl_col_session_t *
new_session(void)
{
    wl_col_session_t *s = calloc(1, sizeof(*s));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1024 * 1024, .usable_bytes = 1024 * 1024,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK
    };
    CHECK(s);
    wl_mem_ledger_init(&s->mem_ledger, 0);
    s->frontier_ops = &col_frontier_epoch_ops;
    s->memory_governor = wl_columnar_memory_governor_ref_create(&resolution);
    s->delta_pool = delta_pool_create(64, sizeof(col_rel_t), 65536);
    s->eval_arena = wl_arena_create(65536);
    CHECK(s->memory_governor && s->delta_pool && s->eval_arena);
    return s;
}

static uint64_t
reserved(wl_col_session_t *s)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
}

static void
destroy_session(wl_col_session_t *s)
{
    CHECK(!s->cleanup_active && !s->cleanup_pending);
    CHECK(s->cleanup_reserved_bytes == 0 && reserved(s) == 0);
    col_session_mem_sample(s);
    CHECK(atomic_load_explicit(&s->mem_ledger.subsys_bytes[
            WL_MEM_SUBSYS_TEMPORARY], memory_order_relaxed) == 0);
    delta_pool_destroy(s->delta_pool);
    wl_arena_free(s->eval_arena);
    wl_columnar_memory_governor_ref_release(s->memory_governor);
    free(s);
}

static uint32_t *
segments(void)
{
    uint32_t *p = malloc(2 * sizeof(*p));
    CHECK(p);
    p[0] = 0;
    p[1] = 1;
    return p;
}

static void
test_admission_and_nesting(void)
{
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *frames[
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES] = { 0 };
    wl_columnar_eval_stack_cleanup_frame_t *extra = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frames[0]) == 0);
    CHECK(wl_columnar_session_cleanup_ready(s) == 0);
    uint64_t bytes = reserved(s);
    CHECK(bytes > 0 && s->cleanup_reserved_bytes == bytes);
    wl_col_session_t *worker = calloc(1, sizeof(*worker));
    CHECK(worker && col_worker_session_create(s, 0, NULL, 0, worker) == 0);
    CHECK(!worker->cleanup_active && !worker->cleanup_pending
        && worker->cleanup_active_count == 0 &&
        worker->cleanup_pending_count == 0
        && worker->cleanup_reserved_bytes == 0);
    wl_columnar_eval_stack_cleanup_frame_t *worker_frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(worker, &worker_frame) == 0);
    CHECK(wl_columnar_session_cleanup_ready(worker) == 0);
    delta_pool_t *worker_pool = worker->delta_pool;
    wl_arena_t *worker_arena = worker->eval_arena;
    uint64_t active_bytes = reserved(s);
    CHECK(col_worker_session_destroy(worker) == EBUSY);
    CHECK(!worker->teardown_started && worker->cleanup_active == worker_frame
        && worker->cleanup_active_count == 1 &&
        worker->delta_pool == worker_pool
        && worker->eval_arena == worker_arena && reserved(s) == active_bytes);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&worker_frame) == 0);
    CHECK(!worker_frame && !worker->cleanup_active);
    CHECK(col_worker_session_destroy(worker) == 0);
    free(worker);
    atomic_store_explicit(
        &wl_columnar_memory_governor_ref_get(s->memory_governor)->usable_bytes,
        bytes, memory_order_relaxed);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &extra) == ENOSPC);
    CHECK(extra == NULL && reserved(s) == bytes &&
        s->cleanup_active_count == 1);
    atomic_store_explicit(
        &wl_columnar_memory_governor_ref_get(s->memory_governor)->usable_bytes,
        1024 * 1024, memory_order_relaxed);
    for (uint32_t i = 1; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; i++)
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frames[i]) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &extra) == ENOBUFS);
    CHECK(extra == NULL && reserved(s)
        == bytes * WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frames[0]) == EBUSY);
    CHECK(frames[0] != NULL);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == EBUSY);
    col_session_mem_sample(s);
    CHECK(atomic_load_explicit(&s->mem_ledger.subsys_bytes[
            WL_MEM_SUBSYS_TEMPORARY], memory_order_relaxed) == reserved(s));
    for (uint32_t i = WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; i > 0; i--)
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frames[i - 1]) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    s->cleanup_reserved_bytes = UINT64_MAX;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &extra) == EOVERFLOW);
    CHECK(extra == NULL && reserved(s) == 0);
    s->cleanup_reserved_bytes = 0;
    destroy_session(s);
}

static void
test_storage_refusal(unsigned storage)
{
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL, *extra = NULL;
    col_rel_t *r = storage == 0 ? col_rel_new_auto("heap", 1)
        : col_rel_pool_new_auto(s->delta_pool,
            storage == 2 ? s->eval_arena : NULL, "pool", 1);
    col_rel_t *borrowed = col_rel_new_auto("borrowed", 1);
    wl_columnar_source_access_reader_t reader = { 0 };
    int64_t value = 42;
    CHECK(r && borrowed && col_rel_append_row(r, &value) == 0);
    CHECK(col_rel_source_reader_acquire(r, &reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
    uint64_t bytes = reserved(s);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    CHECK(eval_stack_push(stack, borrowed, false) == 0);
    stack->items[0].seg_boundaries = segments();
    stack->items[0].seg_count = 1;
    CHECK(eval_stack_push(stack, col_rel_new_auto("lower-owned", 1),
        true) == 0);
    CHECK(stack->items[1].rel);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    result->rel = r;
    result->owned = true;
    result->seg_boundaries = segments();
    result->seg_count = 1;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY);
    CHECK(!frame && s->cleanup_pending_count == 1 && reserved(s) == bytes);
    col_session_mem_sample(s);
    CHECK(atomic_load_explicit(&s->mem_ledger.subsys_bytes[
            WL_MEM_SUBSYS_TEMPORARY], memory_order_relaxed) >= bytes);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &extra) == EBUSY && !extra);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == EBUSY);
    CHECK(r->columns[0][0] == 42 && reserved(s) == bytes);
    CHECK(borrowed->ncols == 1);
    CHECK(col_rel_source_reader_release(&reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    CHECK(!s->cleanup_pending && s->cleanup_pending_count == 0);
    if (storage != 0)
        CHECK(r->columns == NULL && r->name == NULL);
    col_rel_destroy(borrowed);
    destroy_session(s);
}

static void
test_progress_past_refusal(void)
{
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *a = NULL, *b = NULL;
    wl_columnar_source_access_reader_t ra = { 0 }, rb = { 0 };
    col_rel_t *one = col_rel_new_auto("one", 1);
    col_rel_t *two = col_rel_new_auto("two", 1);
    CHECK(one && two);
    CHECK(col_rel_source_reader_acquire(one, &ra) == 0);
    CHECK(col_rel_source_reader_acquire(two, &rb) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &a) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &b) == 0);
    CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(a), one,
        true) == 0);
    CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(b), two,
        true) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&b) == EBUSY && !b);
    CHECK(wl_columnar_session_cleanup_ready(s) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&a) == EBUSY && !a);
    CHECK(s->cleanup_pending_count == 2);
    uint64_t bytes = reserved(s);
    CHECK(col_rel_source_reader_release(&rb) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == EBUSY);
    CHECK(s->cleanup_pending_count == 1 && reserved(s) == bytes / 2);
    CHECK(col_rel_source_reader_release(&ra) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    destroy_session(s);
}

struct reader_context {
    wl_columnar_source_access_reader_t reader;
    unsigned destroyed;
};

static wl_columnar_continuation_status_t
unused_produce(void *ctx, const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch)
{
    (void)ctx;
    (void)cursor;
    (void)batch;
    return WL_COLUMNAR_CONTINUATION_DONE;
}

static void
release_reader(void *ctx)
{
    struct reader_context *context = ctx;
    CHECK(col_rel_source_reader_release(&context->reader) == 0);
    context->destroyed++;
}

static void
test_continuation_reader_cleanup(void)
{
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *outer = NULL, *inner = NULL;
    struct reader_context context = { 0 };
    col_rel_t *r = col_rel_new_auto("reader-owned", 1);
    CHECK(r && col_rel_source_reader_acquire(r, &context.reader) == 0);
    wl_columnar_continuation_producer_t producer = {
        .context = &context, .produce = unused_produce,
        .destroy = release_reader
    };
    wl_columnar_continuation_cursor_t cursor = { 0 };
    wl_columnar_continuation_t *cont =
        wl_columnar_continuation_create(&producer,
            &cursor);
    CHECK(cont);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &outer) == 0);
    CHECK(eval_stack_push_continuation(
            wl_columnar_eval_stack_cleanup_stack(outer), cont) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &inner) == 0);
    CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(inner), r,
        true) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&inner) == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&outer) == 0);
    CHECK(context.destroyed == 1);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    CHECK(context.destroyed == 1);
    destroy_session(s);
}

static int frame_owner;
static wl_evaluation_control_t *frame_control;
static wl_columnar_memory_governor_t *frame_governor;
static wl_columnar_memory_reservation_t frame_peer;
static unsigned frame_event, frame_stop, frame_mode, frame_later;
static unsigned frame_kinds[2048], frame_phases[2048];
static uint64_t frame_progress[2048];
static unsigned frame_failed_cas, frame_physical;
static bool frame_restarted, frame_contention, frame_peer_added;
static bool frame_cleanup_restart;
static wl_columnar_memory_governor_ref_t *frame_drop_ref;
static bool frame_target;
static unsigned frame_target_kind, frame_target_phase;
static wl_evaluation_control_report_t frame_old;
enum { FRAME_LOCAL, FRAME_PHASE, FRAME_WORK, FRAME_BEFORE, FRAME_AFTER };

static wl_evaluation_control_t *
frame_begin(uint64_t limit)
{
    frame_event = frame_later = frame_failed_cas = frame_physical = 0;
    frame_restarted = frame_peer_added = false;
    frame_control = NULL;
    CHECK(wl_evaluation_control_create(limit, &frame_control) == 0);
    CHECK(wl_evaluation_control_attach(frame_control, &frame_owner) == 0);
    CHECK(wl_evaluation_control_begin(frame_control, &frame_owner) == 0);
    return frame_control;
}

static void
frame_restart(void)
{
    CHECK(!frame_restarted);
    int result = wl_evaluation_control_finish(frame_control, &frame_owner, 0,
            0);
    CHECK(result == 0 || (frame_cleanup_restart
        && result == WL_EVALUATION_CONTROL_WORK_LIMIT));
    CHECK(wl_evaluation_control_report(frame_control, &frame_old) == 0);
    CHECK(wl_evaluation_control_reset(frame_control) == 0);
    CHECK(wl_evaluation_control_begin(frame_control, &frame_owner) == 0);
    frame_restarted = true;
}

static uint64_t
frame_finish(int result)
{
    wl_evaluation_control_report_t report;
    CHECK(frame_later == 0);
    uint64_t id = wl_evaluation_control_active_attempt_id(frame_control);
    CHECK(wl_evaluation_control_finish(frame_control, &frame_owner,
        frame_restarted ? 0 : result > 0 ? result : 0, 0)
        == (frame_restarted ? 0 : result));
    CHECK(wl_evaluation_control_report(frame_control, &report) == 0);
    CHECK(report.attempt_id == id);
    if (frame_restarted)
        CHECK(id != frame_old.attempt_id && report.consumed == 0
            && report.result == 0 && report.execution_status == 0
            && report.cleanup_status == 0
            && report.stop_reason == WL_EVALUATION_CONTROL_NONE);
    CHECK(wl_evaluation_control_detach(frame_control, &frame_owner) == 0);
    wl_evaluation_control_release(frame_control);
    frame_control = NULL;
    return frame_restarted ? frame_old.consumed : report.consumed;
}

static void
frame_forward(wl_evaluation_control_t *c, unsigned kind, unsigned phase)
{
    CHECK(c == frame_control);
    if (frame_restarted) {
        frame_later++;
        return;
    }
    CHECK(frame_event < 2048);
    frame_kinds[frame_event] = kind;
    frame_phases[frame_event++] = phase;
    if (frame_target && !frame_stop && kind == frame_target_kind
        && phase == frame_target_phase)
        frame_stop = frame_event;
    if (frame_event != frame_stop)
        return;
    if (frame_mode == 0)
        wl_evaluation_control_request_cancel(c);
    else if (frame_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else
        frame_restart();
}

static void
frame_checkpoint(wl_evaluation_control_t *c, unsigned phase, uint64_t progress)
{
    (void)progress;
    if (phase == WL_COLUMNAR_EVAL_STACK_PHYSICAL_FREED_PHASE) {
        CHECK(!c);
        frame_physical++;
        if (frame_governor)
            CHECK(wl_columnar_memory_reserved(frame_governor)
                > (frame_peer_added ? 17 : 0));
        if (frame_cleanup_restart)
            frame_restart();
        return;
    }
    if (frame_drop_ref && phase == WL_COLUMNAR_EVAL_STACK_MOVED_PHASE) {
        wl_columnar_memory_governor_ref_release(frame_drop_ref);
        frame_drop_ref = NULL;
    }
    if (!frame_restarted && frame_event < 2048)
        frame_progress[frame_event] = progress;
    frame_forward(c, FRAME_LOCAL, phase);
}

static void
frame_gov_phase(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (!c) return;
    CHECK(token && token->identity == token);
    frame_forward(c, FRAME_PHASE, phase);
}

static void
frame_gov_work(wl_evaluation_control_t *c, uint64_t work)
{
    if (!c) return;
    CHECK(work > 0 && work <= WL_EVALUATION_CONTROL_QUANTUM);
    frame_forward(c, FRAME_WORK, 0);
}

static void *
frame_peer_reserve(void *arg)
{
    CHECK(wl_columnar_memory_reserve(arg, 17, &frame_peer));
    return NULL;
}

static void
frame_gov_before(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t total, bool overflow)
{
    (void)total; (void)overflow;
    if (!c) return;
    if (frame_contention && !frame_peer_added) {
        wl_thread_t tid;
        CHECK(wl_thread_create(&tid, frame_peer_reserve, g) == 0);
        CHECK(wl_thread_join(&tid) == 0);
        frame_peer_added = true;
    }
    frame_forward(c, FRAME_BEFORE, 0);
}

static void
frame_gov_after(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, bool success, bool overflow)
{
    (void)g; (void)overflow;
    if (!c) return;
    if (!success) frame_failed_cas++;
    frame_forward(c, FRAME_AFTER, 0);
}

static void
frame_hooks(bool enabled)
{
    wl_columnar_eval_stack_test_checkpoint = enabled ? frame_checkpoint : NULL;
    wl_columnar_memory_governor_test_checkpoint =
        enabled ? frame_gov_phase : NULL;
    wl_columnar_memory_governor_test_work_checkpoint =
        enabled ? frame_gov_work : NULL;
    wl_columnar_memory_governor_test_before_total_cas =
        enabled ? frame_gov_before : NULL;
    wl_columnar_memory_governor_test_after_total_cas =
        enabled ? frame_gov_after : NULL;
}

static int
frame_admit(wl_col_session_t *s, wl_columnar_eval_stack_cleanup_frame_t **out)
{
    return wl_columnar_eval_stack_cleanup_begin_attempt_controlled(s, out,
               frame_control,
               wl_evaluation_control_active_attempt_id(frame_control));
}

static uint64_t
frame_cost(bool governed)
{
    uint64_t n = (wl_columnar_eval_stack_test_frame_size() + 15) / 16;
    return (governed ? UINT64_C(1726) : UINT64_C(928)) + UINT64_C(224) * n;
}

static void
test_frame_literal_admissions(void)
{
    /* Independent literal debit schedule; getter supplies physical size only. */
    uint64_t n = (wl_columnar_eval_stack_test_frame_size() + 15) / 16;
    for (unsigned governed = 0; governed < 2; governed++) {
        wl_col_session_t *s = new_session();
        wl_columnar_memory_governor_ref_t *saved = s->memory_governor;
        if (!governed) s->memory_governor = NULL;
        uint64_t fees[1024]; unsigned count = 0;
        fees[count++] = 128; fees[count++] = 160;
        if (governed) {
            fees[count++] = 240; fees[count++] = 74; fees[count++] = 54;
            fees[count++] = 84; fees[count++] = 29;
        }
        fees[count++] = 160;
        CHECK(n + count + 8 < 1024);
        for (uint64_t i = 0; i < n; i++) fees[count++] = 224;
        fees[count++] = 128; fees[count++] = 192;
        if (governed) {
            fees[count++] = 160; fees[count++] = 74;
            fees[count++] = 54; fees[count++] = 29;
        }
        fees[count++] = 160;
        uint64_t prefix = 0;
        for (unsigned i = 0; i < count; i++) {
            uint64_t previous = prefix; prefix += fees[i];
            for (unsigned exact = 0; exact < 2; exact++) {
                frame_begin(exact ? prefix : prefix - 1);
                wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
                int rc = frame_admit(s, &out);
                bool final = exact && i + 1 == count;
                CHECK(rc == (final ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
                CHECK(frame_finish(rc) == (exact ? prefix : previous));
                if (final) {
                    CHECK(out &&
                        wl_columnar_eval_stack_cleanup_stack(out)->top == 0);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&out) == 0);
                } else CHECK(!out);
                CHECK(!s->cleanup_active && !s->cleanup_pending
                    && s->cleanup_active_count == 0 &&
                    s->cleanup_reserved_bytes == 0);
                if (governed) CHECK(reserved(s) == 0);
                CHECK(wl_columnar_memory_governor_ref_is_sole(saved));
            }
        }
        CHECK(prefix == frame_cost(governed));
        s->memory_governor = saved;
        destroy_session(s);
    }
}

static void
test_frame_all_interruptions(void)
{
    frame_hooks(true);
    for (unsigned governed = 0; governed < 2; governed++) {
        wl_col_session_t *s = new_session();
        wl_columnar_memory_governor_ref_t *saved = s->memory_governor;
        if (!governed) s->memory_governor = NULL;
        frame_governor =
            governed ? wl_columnar_memory_governor_ref_get(saved) : NULL;
        frame_stop = 0;
        frame_begin(0);
        wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
        CHECK(frame_admit(s, &out) == 0);
        unsigned total = frame_event;
        unsigned kinds[2048]; memcpy(kinds, frame_kinds, sizeof(kinds));
        CHECK(frame_finish(0) == frame_cost(governed));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&out) == 0);
        for (unsigned mode = 0; mode < 3; mode++) {
            for (unsigned stop = 1; stop <= total; stop++) {
                frame_stop = stop; frame_mode = mode;
                frame_begin(0);
                CHECK(!out);
                int rc = frame_admit(s, &out);
                CHECK(rc == (mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                    : mode == 1 ? EOVERFLOW : EINVAL));
                CHECK(!out && !s->cleanup_active && !s->cleanup_pending
                    && s->cleanup_active_count == 0 &&
                    s->cleanup_reserved_bytes == 0);
                unsigned suffix = mode == 2 ? 0
                    : kinds[stop - 1] == FRAME_BEFORE ? 2
                    : kinds[stop - 1] == FRAME_AFTER ? 1 : 0;
                CHECK(frame_event == stop + suffix);
                (void)frame_finish(rc);
                if (governed) CHECK(reserved(s) == 0);
                CHECK(wl_columnar_memory_governor_ref_is_sole(saved));
            }
        }
        frame_stop = 0;
        s->memory_governor = saved;
        destroy_session(s);
    }
    frame_governor = NULL;
    frame_hooks(false);
}

static void
test_frame_faults_and_peer(void)
{
    wl_col_session_t *s = new_session();
    frame_governor = wl_columnar_memory_governor_ref_get(s->memory_governor);
    frame_hooks(true); frame_stop = 0;
    wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
    wl_columnar_eval_stack_test_fail_allocation = 0;
    frame_begin(0);
    CHECK(frame_admit(s, &out) == ENOMEM && !out);
    CHECK(frame_finish(ENOMEM) == 929 && reserved(s) == 0 &&
        frame_physical == 0);
    wl_columnar_eval_stack_test_fail_allocation = -1;
    atomic_store_explicit(&frame_governor->usable_bytes, 1,
        memory_order_relaxed);
    frame_begin(0);
    CHECK(frame_admit(s, &out) == ENOSPC && !out && s->memory_budget_denied);
    CHECK(frame_finish(ENOSPC) == 740 && reserved(s) == 0);
    s->memory_budget_denied = false;
    atomic_store_explicit(&frame_governor->usable_bytes, 1024 * 1024,
        memory_order_relaxed);
    frame_contention = true;
    wl_columnar_memory_reservation_init(&frame_peer);
    frame_begin(0);
    CHECK(frame_admit(s,
        &out) == 0 && out && frame_peer_added && frame_failed_cas == 1);
    CHECK(frame_finish(0) == frame_cost(true) + 84);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&out) == 0 &&
        reserved(s) == 17);
    CHECK(wl_columnar_memory_rollback(&frame_peer) && reserved(s) == 0);
    frame_contention = false;
    /* Mandatory partial-allocation retirement may restart the control, but
     * cannot charge or publish into the new attempt. Stop first zero chunk. */
    frame_stop = 0;
    frame_cleanup_restart = true;
    frame_begin(929);
    CHECK(frame_admit(s, &out) == WL_EVALUATION_CONTROL_WORK_LIMIT && !out);
    CHECK(frame_physical == 1 && frame_restarted);
    CHECK(frame_finish(WL_EVALUATION_CONTROL_WORK_LIMIT) == 929);
    frame_cleanup_restart = false;
    frame_hooks(false); frame_governor = NULL;
    destroy_session(s);
}

static void
test_frame_typed_precedence(void)
{
    wl_col_session_t *s = new_session();
    frame_governor = wl_columnar_memory_governor_ref_get(s->memory_governor);
    frame_hooks(true);
    for (unsigned allocation = 0; allocation < 2; allocation++) {
        for (unsigned mode = 0; mode < 3; mode++) {
            frame_mode = mode; frame_stop = 0; frame_target = true;
            frame_target_kind = FRAME_LOCAL;
            frame_target_phase =
                allocation ? WL_COLUMNAR_EVAL_STACK_ALLOCATION_PHASE
                : WL_COLUMNAR_EVAL_STACK_RESERVE_PHASE;
            s->memory_budget_denied = false;
            atomic_store_explicit(&frame_governor->usable_bytes,
                allocation ? 1024 * 1024 : 1, memory_order_relaxed);
            wl_columnar_eval_stack_test_fail_allocation = allocation ? 0 : -1;
            frame_begin(0);
            wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
            int expected = mode == 0 ? allocation ? ENOMEM : ENOSPC
                : mode == 1 ? EOVERFLOW : EINVAL;
            CHECK(frame_admit(s, &out) == expected && !out && frame_stop);
            CHECK(frame_event == frame_stop && reserved(s) == 0);
            CHECK(s->memory_budget_denied == !allocation);
            (void)frame_finish(expected);
        }
    }
    wl_columnar_eval_stack_test_fail_allocation = -1;
    /* Child VERDICT cancellation owes the work seam and returned-status
     * caller seam. Positive/stale stop before authoritative status/flag. */
    for (unsigned mode = 0; mode < 3; mode++) {
        frame_mode = mode; frame_stop = 0; frame_target = true;
        frame_target_kind = FRAME_PHASE;
        frame_target_phase = WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT;
        s->memory_budget_denied = false;
        atomic_store_explicit(&frame_governor->usable_bytes, 1,
            memory_order_relaxed);
        frame_begin(0);
        wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
        int expected = mode == 0 ? ENOSPC : mode == 1 ? EOVERFLOW : EINVAL;
        CHECK(frame_admit(s, &out) == expected && !out && frame_stop);
        CHECK(frame_event == frame_stop + (mode == 0 ? 2 : 0));
        CHECK(s->memory_budget_denied == (mode == 0) && reserved(s) == 0);
        (void)frame_finish(expected);
    }
    frame_target = false; frame_stop = 0;
    atomic_store_explicit(&frame_governor->usable_bytes, 1024 * 1024,
        memory_order_relaxed);
    frame_hooks(false); frame_governor = NULL;
    destroy_session(s);
}

static void
test_frame_contract_and_retained_cleanup(void)
{
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(s, &out, NULL,
        0) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&out) == 0);
    frame_begin(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(frame_control);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(NULL, &out,
        frame_control, id) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(s, NULL,
        frame_control, id) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(s, &out,
        frame_control, 0) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(s, &out,
        frame_control, id + 1) == EINVAL);
    CHECK(frame_finish(EINVAL) == 0);
    frame_begin(0);
    id = wl_evaluation_control_active_attempt_id(frame_control);
    CHECK(wl_evaluation_control_finish(frame_control, &frame_owner, 0, 0) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_begin_attempt_controlled(s, &out,
        frame_control, id) == EINVAL && !out);
    wl_evaluation_control_report_t inactive;
    CHECK(wl_evaluation_control_report(frame_control, &inactive) == 0
        && inactive.consumed == 0);
    CHECK(wl_evaluation_control_detach(frame_control, &frame_owner) == 0);
    wl_evaluation_control_release(frame_control); frame_control = NULL;
    frame_begin(0); s->cleanup_reserved_bytes = UINT64_MAX;
    CHECK(frame_admit(s, &out) == EOVERFLOW && !out);
    CHECK(frame_finish(EOVERFLOW) == 128);
    s->cleanup_reserved_bytes = 0;
    frame_begin(0);
    CHECK(frame_admit(s, &out) == 0);
    CHECK(frame_finish(0) == frame_cost(true));
    wl_columnar_eval_stack_cleanup_frame_t *original = out;
    frame_begin(0);
    CHECK(frame_admit(s, &out) == EINVAL && out == original);
    CHECK(frame_finish(EINVAL) == 0);
    col_rel_t *r = col_rel_new_auto("attempt-retained", 1);
    wl_columnar_source_access_reader_t reader = { 0 };
    CHECK(r && col_rel_source_reader_acquire(r, &reader) == 0);
    CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(out), r,
        true) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&out) == EBUSY && !out);
    frame_begin(0);
    CHECK(frame_admit(s, &out) == EBUSY && !out);
    CHECK(frame_finish(EBUSY) == 128);
    CHECK(col_rel_source_reader_release(&reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    /* Active cap refusal with real raw frames, no fabricated handles. */
    wl_columnar_eval_stack_cleanup_frame_t *frames[
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES] = { 0 };
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; i++)
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frames[i]) == 0);
    frame_begin(0);
    CHECK(frame_admit(s, &out) == ENOBUFS && !out);
    CHECK(frame_finish(ENOBUFS) == 128);
    for (unsigned i = WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; i > 0; i--)
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frames[i - 1]) == 0);
    destroy_session(s);
}

static void
test_frame_sole_pin(void)
{
    wl_col_session_t *s = new_session();
    wl_columnar_memory_governor_ref_t *g = s->memory_governor;
    frame_governor = wl_columnar_memory_governor_ref_get(g);
    frame_hooks(true); frame_stop = 0; frame_drop_ref = g;
    frame_begin(0);
    wl_columnar_eval_stack_cleanup_frame_t *out = NULL;
    CHECK(frame_admit(s, &out) == 0 && out && !frame_drop_ref);
    CHECK(wl_columnar_memory_governor_ref_is_sole(g));
    CHECK(frame_finish(0) == frame_cost(true));
    /* Frame alone pins the original governor; no access after its free. */
    frame_hooks(false); frame_governor = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&out) == 0 && !out);
    s->memory_governor = NULL;
    CHECK(!s->cleanup_active && !s->cleanup_pending &&
        !s->cleanup_reserved_bytes);
    delta_pool_destroy(s->delta_pool);
    wl_arena_free(s->eval_arena);
    free(s);
}

static col_rel_t *
unary_relation(wl_col_session_t *s, int64_t value)
{
    col_rel_t *r = NULL;
    if (s->memory_governor)
        CHECK(wl_columnar_relation_new_auto_governed("unary", 1, 1, false,
            s->memory_governor, &r) == 0);
    else
        r = col_rel_new_auto("unary", 1);
    CHECK(r && col_rel_append_row(r, &value) == 0);
    return r;
}

static void
unary_row(col_rel_t *r, int64_t value)
{
    CHECK(r && r->nrows == 1 && r->ncols == 1 && r->columns[0][0] == value);
}

static bool
unary_entry_equal(const eval_entry_t *a, const eval_entry_t *b)
{
    return a->rel == b->rel && a->owned == b->owned
           && a->is_delta == b->is_delta &&
           a->seg_boundaries == b->seg_boundaries
           && a->seg_count == b->seg_count && a->kind == b->kind
           && a->continuation == b->continuation;
}

static col_rel_t *
unary_frame(wl_col_session_t *s, wl_columnar_eval_stack_cleanup_frame_t **f,
    unsigned top, bool top_owned)
{
    CHECK(top > 0 && top <= COL_STACK_MAX);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, f) == 0);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(*f);
    for (unsigned i = 0; i < top; i++)
        CHECK(eval_stack_push(stack, unary_relation(s, 1000 + i),
            i + 1 == top ? top_owned : true) == 0);
    return stack->items[top - 1].rel;
}

static int
unary_begin(wl_columnar_eval_stack_cleanup_frame_t *f)
{
    return wl_columnar_eval_stack_unary_begin_attempt_controlled(f,
               frame_control,
               wl_evaluation_control_active_attempt_id(frame_control));
}

static col_rel_t **
unary_output(wl_columnar_eval_stack_cleanup_frame_t *f)
{
    col_rel_t **slot = NULL;
    /* Complete accessor work belongs to its caller, independently of the
     * begin/commit tariffs. These literal48/56 are accepted caller envelopes. */
    CHECK(wl_evaluation_control_charge(frame_control, 56) == 0);
    CHECK(wl_columnar_eval_stack_unary_output(f, &slot) == 0 && slot && !*slot);
    return slot;
}

struct unary_thread_context {
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    bool retry;
    int result, reset_result, input_result, output_result;
};

static void *
unary_other_thread(void *arg)
{
    struct unary_thread_context *ctx = arg;
    const col_rel_t *input = (const col_rel_t *)&frame_owner;
    col_rel_t **output = (col_rel_t **)&frame_owner;
    if (!ctx->retry) {
        ctx->input_result = wl_columnar_eval_stack_unary_input(ctx->frame,
                &input);
        ctx->output_result = wl_columnar_eval_stack_unary_output(ctx->frame,
                &output);
        CHECK(input == (const col_rel_t *)&frame_owner
            && output == (col_rel_t **)&frame_owner);
    }
    ctx->result = wl_columnar_eval_stack_unary_finish(ctx->frame, EIO);
    ctx->reset_result = wl_columnar_eval_stack_unary_reset(ctx->frame);
    return NULL;
}

static void
test_unary_frame_initialization(void)
{
    uint64_t bytes = wl_columnar_eval_stack_test_frame_size();
    CHECK(bytes > sizeof(eval_stack_t) + sizeof(eval_entry_t)
        + sizeof(wl_columnar_memory_reservation_t) + 3 * sizeof(void *));
    for (unsigned type = 0; type < 4; type++) {
        wl_col_session_t *s = new_session();
        wl_columnar_memory_governor_ref_t *saved = s->memory_governor;
        if (type & 1) s->memory_governor = NULL;
        wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
        if (type & 2) {
            frame_begin(0);
            CHECK(frame_admit(s, &f) == 0);
            CHECK(frame_finish(0) == frame_cost(!(type & 1)));
        } else CHECK(wl_columnar_eval_stack_cleanup_begin(s, &f) == 0);
        CHECK(s->cleanup_reserved_bytes == bytes);
        if (!(type & 1)) CHECK(reserved(s) == bytes);
        col_rel_t *r = unary_relation(s, 7);
        CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(f), r,
            true) == 0);
        frame_begin(0);
        CHECK(unary_begin(f) == 0);
        const col_rel_t *input = NULL;
        CHECK(wl_evaluation_control_charge(frame_control, 48) == 0);
        CHECK(wl_columnar_eval_stack_unary_input(f, &input) == 0 && input == r);
        CHECK(wl_columnar_eval_stack_unary_finish(f, 0) == 0);
        CHECK(frame_finish(0) == 544 + 48);
        CHECK(wl_columnar_eval_stack_unary_reset(f) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
        s->memory_governor = saved;
        destroy_session(s);
    }
    wl_col_session_t *s = new_session();
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(s->memory_governor);
    wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
    atomic_store_explicit(&g->usable_bytes, bytes - 1, memory_order_relaxed);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &f) == ENOSPC && !f);
    CHECK(!s->cleanup_active && !s->cleanup_reserved_bytes && reserved(s) == 0);
    atomic_store_explicit(&g->usable_bytes, bytes, memory_order_relaxed);
    CHECK(wl_columnar_eval_stack_cleanup_begin(s,
        &f) == 0 && reserved(s) == bytes);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0 && reserved(s) == 0);
    destroy_session(s);
}

static void
test_unary_literal_boundaries(void)
{
    const uint64_t begin_fees[] = { 160, 128, 256 };
    uint64_t prefix = 0;
    for (unsigned i = 0; i < 3; i++) {
        uint64_t previous = prefix; prefix += begin_fees[i];
        for (unsigned exact = 0; exact < 2; exact++) {
            wl_col_session_t *s = new_session();
            wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
            col_rel_t *input = unary_frame(s, &f, 1, true);
            eval_entry_t original =
                wl_columnar_eval_stack_cleanup_stack(f)->items[0];
            frame_begin(exact ? prefix : prefix - 1);
            int rc = unary_begin(f);
            bool active = exact && i == 2;
            CHECK(rc == (active ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
            CHECK(wl_columnar_eval_stack_unary_finish(f,
                rc) == (active ? 0 : EINVAL));
            CHECK(frame_finish(rc) == (exact ? prefix : previous));
            CHECK(unary_entry_equal(&original,
                &wl_columnar_eval_stack_cleanup_stack(f)->items[0]));
            unary_row(input, 1000);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
            destroy_session(s);
        }
    }
    CHECK(prefix == 544);
    /* Top32 covers every actual per-entry debit and the final cutoff. */
    uint64_t fees[COL_STACK_MAX + 3];
    fees[0] = 192; fees[1] = 128;
    for (unsigned i = 0; i < COL_STACK_MAX; i++) fees[i + 2] = 128;
    fees[COL_STACK_MAX + 2] = 128;
    prefix = 0;
    for (unsigned i = 0; i < COL_STACK_MAX + 3; i++) {
        uint64_t previous = prefix; prefix += fees[i];
        for (unsigned exact = 0; exact < 2; exact++) {
            wl_col_session_t *s = new_session();
            wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
            (void)unary_frame(s, &f, COL_STACK_MAX, true);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
            eval_entry_t original[COL_STACK_MAX];
            memcpy(original, stack->items, sizeof(original));
            frame_begin(544 + 56 + (exact ? prefix : prefix - 1));
            CHECK(unary_begin(f) == 0);
            col_rel_t **slot = unary_output(f);
            col_rel_t *output = unary_relation(s, 9000); *slot = output;
            bool committed = true;
            int rc = wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
                    &committed);
            bool final = exact && i == COL_STACK_MAX + 2;
            CHECK(rc == (final ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
            CHECK(committed == final && stack->top == COL_STACK_MAX);
            for (unsigned j = 0; j < COL_STACK_MAX - 1; j++)
                CHECK(unary_entry_equal(&original[j], &stack->items[j]));
            if (final) CHECK(stack->items[COL_STACK_MAX - 1].rel == output);
            else CHECK(unary_entry_equal(&original[COL_STACK_MAX - 1],
                    &stack->items[COL_STACK_MAX - 1]));
            CHECK(wl_columnar_eval_stack_unary_finish(f, rc) == rc);
            CHECK(frame_finish(rc) == 544 + 56 + (exact ? prefix : previous));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
            destroy_session(s);
        }
    }
    CHECK(prefix == 448 + 128 * COL_STACK_MAX);
}

static void
test_unary_aliases(void)
{
    for (unsigned owned = 0; owned < 2; owned++)
        for (unsigned target_owned = 0; target_owned < 2; target_owned++)
            for (unsigned alias_kind = 0; alias_kind < 3; alias_kind++)
                for (unsigned commit = 0; commit < 2; commit++)
                    for (unsigned first_kind = 0; first_kind < 3;
                        first_kind++) {
                        wl_col_session_t *s = new_session();
                        wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
                        /* At31 the original index30 is caught by the fixed head.
                         * Lower index29 genuinely exercises the scalar remainder. */
                        unsigned top = alias_kind == 1 ? 31 : 32;
                        col_rel_t *input = unary_frame(s, &f, top, owned != 0);
                        eval_stack_t *stack =
                            wl_columnar_eval_stack_cleanup_stack(f);
                        eval_entry_t *result =
                            wl_columnar_eval_stack_cleanup_result(f);
                        result->rel = unary_relation(s, 7000);
                        result->owned = true;
                        eval_entry_t *target = alias_kind == 0
                        ? &stack->items[top - 1] : alias_kind == 1
                        ? &stack->items[29] : result;
                        target->owned = target_owned != 0;
                        col_rel_t *candidate = target->rel;
                        bool input_borrowed = !stack->items[top - 1].owned;
                        bool candidate_borrowed = !target->owned;
                        bool external_candidate = candidate_borrowed
                            && candidate != input;
                        eval_entry_t original[COL_STACK_MAX];
                        memcpy(original, stack->items, sizeof(original));
                        eval_entry_t original_result = *result;
                        uint64_t baseline = reserved(s);
                        frame_begin(0);
                        CHECK(unary_begin(f) == 0);
                        col_rel_t **slot = unary_output(f); *slot = candidate;
                        uint64_t extra = 0;
                        if (commit) {
                            bool published = true;
                            CHECK(
                                wl_columnar_eval_stack_unary_commit_attempt_controlled
                                    (f, &published) == EINVAL && !published);
                            extra = alias_kind == 1 ? 320 + 128 * 30 : 320;
                        }
                        int first = first_kind == 0 ? 0 : first_kind ==
                            1 ? WL_EVALUATION_CONTROL_CANCELLED : EIO;
                        if (first_kind == 1) {
                            wl_evaluation_control_request_cancel(frame_control);
                            CHECK(wl_evaluation_control_charge(frame_control,
                                0) == WL_EVALUATION_CONTROL_CANCELLED);
                        }
                        int expected = first ? first : EINVAL;
                        CHECK(wl_columnar_eval_stack_unary_finish(f,
                            first) == expected);
                        CHECK(wl_columnar_eval_stack_unary_finish(f,
                            ENOMEM) == expected);
                        CHECK(stack->top == top && reserved(s) == baseline);
                        for (unsigned i = 0; i < top; i++) {
                            CHECK(unary_entry_equal(&original[i],
                                &stack->items[i]));
                            unary_row(stack->items[i].rel, 1000 + i);
                        }
                        CHECK(unary_entry_equal(&original_result, result));
                        unary_row(result->rel, 7000);
                        CHECK(frame_finish(expected) == 544 + 56 + extra);
                        CHECK(wl_columnar_eval_stack_unary_reset(f) == 0);
                        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
                        if (input_borrowed) {
                            unary_row(input, 1000 + top - 1);
                            CHECK(col_rel_destroy_checked(input) == 0);
                        }
                        if (external_candidate) {
                            unary_row(candidate, alias_kind == 1 ? 1029 : 7000);
                            CHECK(col_rel_destroy_checked(candidate) == 0);
                        }
                        CHECK(reserved(s) == 0);
                        destroy_session(s);
                    }
    /* Inactive copied pointers are not live owners and must not collide. */
    for (unsigned commit = 0; commit < 2; commit++) {
        wl_col_session_t *s = new_session();
        wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
        col_rel_t *input = unary_frame(s, &f, 5, true);
        col_rel_t *candidate = unary_relation(s, 12345);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
        stack->items[5].rel = candidate; stack->items[5].owned = true;
        frame_begin(0); CHECK(unary_begin(f) == 0);
        *unary_output(f) = candidate;
        if (commit) {
            bool published = false;
            CHECK(wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
                &published) == 0 && published);
            CHECK(stack->items[4].rel == candidate);
        }
        CHECK(wl_columnar_eval_stack_unary_finish(f, 0) == 0);
        if (!commit) unary_row(input, 1004);
        CHECK(frame_finish(0) == 544 + 56 + (commit ? 448 + 128 * 5 : 0));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
        destroy_session(s);
    }
}

static void
test_unary_retained_and_thread_lifetime(void)
{
    for (unsigned commit = 0; commit < 2; commit++)
        for (unsigned first_kind = 0; first_kind < 3; first_kind++) {
            wl_col_session_t *s = new_session();
            wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
            col_rel_t *input = unary_frame(s, &f, 32, true);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
            stack->items[31].is_delta = true;
            stack->items[31].seg_boundaries = segments();
            stack->items[31].seg_count = 1;
            uint32_t *saved_segments = stack->items[31].seg_boundaries;
            eval_entry_t original = stack->items[31];
            eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(f);
            result->rel = unary_relation(s, 7000); result->owned = true;
            frame_begin(0); CHECK(unary_begin(f) == 0);
            const col_rel_t *borrowed = NULL;
            CHECK(wl_evaluation_control_charge(frame_control, 48) == 0);
            CHECK(wl_columnar_eval_stack_unary_input(f,
                &borrowed) == 0 && borrowed == input);
            col_rel_t *output = unary_relation(s, 9000);
            *unary_output(f) = output;
            struct unary_thread_context wrong = { .frame = f };
            wl_thread_t tid;
            CHECK(wl_thread_create(&tid, unary_other_thread, &wrong) == 0);
            CHECK(wl_thread_join(&tid) == 0);
            CHECK(wrong.result == EINVAL && wrong.reset_result == EBUSY
                && wrong.input_result == EINVAL &&
                wrong.output_result == EINVAL);
            CHECK(unary_entry_equal(&original, &stack->items[31]));
            wl_columnar_eval_stack_cleanup_frame_t *handle = f;
            CHECK(wl_columnar_eval_stack_cleanup_finish(&handle) == EBUSY &&
                handle == f);
            wl_columnar_source_access_reader_t reader = { 0 };
            CHECK(col_rel_source_reader_acquire(commit ? input : output,
                &reader) == 0);
            if (commit) {
                bool published = false;
                CHECK(wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
                    &published) == 0 && published);
                CHECK(stack->top == 32 && stack->items[31].rel == output
                    && stack->items[31].owned && !stack->items[31].is_delta
                    && !stack->items[31].seg_boundaries &&
                    !stack->items[31].seg_count
                    && stack->items[31].kind == WL_COLUMNAR_EVAL_ENTRY_RELATION
                    && !stack->items[31].continuation);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&handle) == EBUSY &&
                    handle == f);
            }
            int first = first_kind == 0 ? 0 : first_kind ==
                1 ? WL_EVALUATION_CONTROL_CANCELLED : EIO;
            if (first_kind == 1) {
                wl_evaluation_control_request_cancel(frame_control);
                CHECK(wl_evaluation_control_charge(frame_control,
                    0) == WL_EVALUATION_CONTROL_CANCELLED);
            }
            int expected = first ? first : EBUSY;
            CHECK(wl_columnar_eval_stack_unary_finish(f, first) == expected);
            CHECK(wl_columnar_eval_stack_unary_reset(f) == EBUSY);
            unary_row(input, 1031); unary_row(output, 9000);
            unary_row(result->rel, 7000);
            CHECK(saved_segments[0] == 0 && saved_segments[1] == 1);
            CHECK(frame_finish(expected) ==
                544 + 48 + 56 + (commit ? 448 + 128 * 32 : 0));
            /* The old control has been released. Joined later host thread
             * retires only established authority, never old saved bindings. */
            CHECK(col_rel_source_reader_release(&reader) == 0);
            struct unary_thread_context retry = { .frame = f, .retry = true };
            CHECK(wl_thread_create(&tid, unary_other_thread, &retry) == 0);
            CHECK(wl_thread_join(&tid) == 0);
            CHECK(retry.result == first && retry.reset_result == 0);
            if (!commit) CHECK(unary_entry_equal(&original, &stack->items[31]));
            else unary_row(stack->items[31].rel, 9000);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
            destroy_session(s);
        }
}

static uint64_t
unary_hook_prefix(unsigned phase, uint64_t progress)
{
    if (phase == WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_VALID_PHASE) return 160;
    if (phase == WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_PREPARED_PHASE
        || phase ==
        WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE_CUTOFF_PHASE) return 544;
    if (phase ==
        WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_VALID_PHASE) return 544 + 56 + 192;
    if (phase ==
        WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE) return 544 + 56 + 320 +
               128 * progress;
    CHECK(phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_CUTOFF_PHASE);
    return 544 + 56 + 448 + 128 * progress;
}

static void
test_unary_all_interruptions(void)
{
    frame_hooks(true); frame_stop = 0;
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
    (void)unary_frame(s, &f, 32, true);
    frame_begin(0); CHECK(unary_begin(f) == 0);
    *unary_output(f) = unary_relation(s, 9000);
    bool published = false;
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
        &published) == 0 && published);
    unsigned total = frame_event;
    CHECK(total == 6 + 32);
    unsigned phases[2048]; uint64_t progress[2048];
    memcpy(phases, frame_phases, sizeof(phases));
    memcpy(progress, frame_progress, sizeof(progress));
    CHECK(wl_columnar_eval_stack_unary_finish(f, 0) == 0);
    CHECK(frame_finish(0) == 544 + 56 + 448 + 128 * 32);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0); destroy_session(s);
    for (unsigned mode = 0; mode < 3; mode++)
        for (unsigned stop = 1; stop <= total; stop++) {
            s = new_session(); f = NULL;
            (void)unary_frame(s, &f, 32, true);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
            eval_entry_t original[32];
            memcpy(original, stack->items, sizeof(original));
            frame_stop = stop; frame_mode = mode; frame_begin(0);
            int rc = unary_begin(f); bool active = rc == 0;
            published = true;
            if (!rc) {
                *unary_output(f) = unary_relation(s, 9000);
                rc = wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
                        &published);
                CHECK(!published);
            }
            int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED : mode ==
                1 ? EOVERFLOW : EINVAL;
            CHECK(rc == expected && frame_event == stop);
            for (unsigned i = 0; i < 32; i++) {
                CHECK(unary_entry_equal(&original[i], &stack->items[i]));
                unary_row(stack->items[i].rel, 1000 + i);
            }
            CHECK(wl_columnar_eval_stack_unary_finish(f,
                rc) == (active ? rc : EINVAL));
            CHECK(frame_finish(rc) == unary_hook_prefix(phases[stop - 1],
                progress[stop - 1]));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
            destroy_session(s);
        }
    frame_stop = 0; frame_hooks(false);
}

static void
test_unary_interrupted_aliases(void)
{
    /* Independent stable ordinals: begin three, commit valid one, head one,
     * then one event per paid row. Assert both named phase and progress. */
    const unsigned stops[] = { 4, 5, 12, 35 };
    const unsigned phases[] = {
        WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_VALID_PHASE,
        WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE,
        WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE,
        WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE
    };
    const uint64_t progresses[] = { 31, 0, 7, 30 };
    const uint64_t consumed[] = {
        544 + 56 + 192, 544 + 56 + 320,
        544 + 56 + 320 + 128 * 7,
        544 + 56 + 320 + 128 * 30
    };
    for (unsigned owned = 0; owned < 2; owned++)
        for (unsigned mode = 0; mode < 4; mode++)
            for (unsigned seam = 0; seam < (mode == 3 ? 1u : 4u); seam++) {
                wl_col_session_t *s = new_session();
                wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
                (void)unary_frame(s, &f, 31, true);
                eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
                stack->items[29].owned = owned != 0;
                col_rel_t *candidate = stack->items[29].rel;
                eval_entry_t *result =
                    wl_columnar_eval_stack_cleanup_result(f);
                result->rel = unary_relation(s, 7000); result->owned = true;
                eval_entry_t original[COL_STACK_MAX];
                memcpy(original, stack->items, sizeof(original));
                eval_entry_t original_result = *result;
                uint64_t baseline = reserved(s);
                frame_hooks(true); frame_target = false;
                frame_stop = mode == 3 ? 0 : stops[seam];
                frame_mode = mode;
                frame_begin(mode == 3 ? 544 + 56 + 320 + 8 * 128 - 1 : 0);
                CHECK(unary_begin(f) == 0);
                *unary_output(f) = candidate;
                bool published = true;
                int rc = wl_columnar_eval_stack_unary_commit_attempt_controlled(
                    f, &published);
                int expected = mode == 0 && seam != 3
                    ? WL_EVALUATION_CONTROL_CANCELLED : mode == 1
                    ? EOVERFLOW : mode == 3
                    ? WL_EVALUATION_CONTROL_WORK_LIMIT : EINVAL;
                CHECK(rc == expected && !published);
                unsigned events = frame_event;
                if (mode != 3) {
                    CHECK(events == stops[seam]);
                    CHECK(frame_phases[events - 1] == phases[seam]);
                    CHECK(frame_progress[events - 1] == progresses[seam]);
                } else {
                    CHECK(events == 12);
                    CHECK(frame_phases[events - 1] ==
                        WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE);
                    CHECK(frame_progress[events - 1] == 7);
                }
                CHECK(wl_columnar_eval_stack_unary_finish(f, rc) == rc);
                CHECK(wl_columnar_eval_stack_unary_finish(f, ENOMEM) == rc);
                CHECK(frame_event == events && frame_later == 0);
                CHECK(stack->top == 31 && reserved(s) == baseline);
                for (unsigned i = 0; i < 31; i++) {
                    CHECK(unary_entry_equal(&original[i], &stack->items[i]));
                    unary_row(stack->items[i].rel, 1000 + i);
                }
                CHECK(unary_entry_equal(&original_result, result));
                unary_row(result->rel, 7000);
                CHECK(frame_finish(rc) == (mode == 3
                    ? 544 + 56 + 320 + 7 * 128 : consumed[seam]));
                CHECK(wl_columnar_eval_stack_unary_reset(f) == 0);
                frame_stop = 0; frame_hooks(false);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
                if (!owned) {
                    unary_row(candidate, 1029);
                    CHECK(col_rel_destroy_checked(candidate) == 0);
                }
                CHECK(reserved(s) == 0);
                destroy_session(s);
            }
    frame_stop = 0; frame_hooks(false);
}

static void
test_unary_contract_and_generic_cleanup(void)
{
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
    col_rel_t *input = unary_frame(s, &f, 2, true);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
    frame_begin(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(frame_control);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(f, NULL,
        id) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(f,
        frame_control, id + 1) == EINVAL);
    CHECK(frame_finish(EINVAL) == 0);
    frame_begin(0); CHECK(unary_begin(f) == 0);
    col_rel_t *output = unary_relation(s, 9000); *unary_output(f) = output;
    uint32_t saved_top = stack->top;
    /* Controlled malformed-state injection; restore before legitimate end.
     * The refusal must neither clamp top nor touch candidate/original owners. */
    stack->top = COL_STACK_MAX + 1;
    CHECK(wl_columnar_eval_stack_unary_finish(f, 0) == EINVAL);
    CHECK(stack->top == COL_STACK_MAX + 1);
    wl_columnar_eval_stack_cleanup_frame_t *unchanged_handle = f;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&unchanged_handle) == EINVAL
        && unchanged_handle == f);
    stack->top = saved_top;
    unary_row(input, 1001); unary_row(output, 9000);
    CHECK(wl_columnar_eval_stack_unary_finish(f, 0) == 0);
    CHECK(frame_finish(0) == 544 + 56);
    CHECK(wl_columnar_eval_stack_unary_finish(f, EIO) == 0);
    const col_rel_t *unchanged = input;
    CHECK(wl_columnar_eval_stack_unary_input(f,
        &unchanged) == EINVAL && unchanged == input);
    CHECK(wl_columnar_eval_stack_unary_reset(f) == 0);
    /* Pending cleanup must visit retired even when result refuses. */
    uint64_t frame_bytes = s->cleanup_reserved_bytes;
    uint64_t relation_bytes = (reserved(s) - frame_bytes) / 2;
    CHECK(relation_bytes > 0 &&
        reserved(s) == frame_bytes + 2 * relation_bytes);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(f);
    result->rel = unary_relation(s, 7000); result->owned = true;
    wl_columnar_source_access_reader_t result_reader = { 0 },
        input_reader = { 0 };
    CHECK(col_rel_source_reader_acquire(result->rel, &result_reader) == 0);
    CHECK(col_rel_source_reader_acquire(input, &input_reader) == 0);
    frame_begin(0); CHECK(unary_begin(f) == 0);
    *unary_output(f) = unary_relation(s, 9001);
    bool committed = false;
    CHECK(wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
        &committed) == 0 && committed);
    CHECK(wl_columnar_eval_stack_unary_finish(f, 0) == EBUSY);
    CHECK(frame_finish(EBUSY) == 544 + 56 + 448 + 128 * 2);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == EBUSY && !f);
    CHECK(s->cleanup_pending && s->cleanup_pending_count == 1);
    CHECK(reserved(s) == frame_bytes + 2 * relation_bytes);
    CHECK(col_rel_source_reader_release(&input_reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == EBUSY);
    CHECK(reserved(s) == frame_bytes + relation_bytes);
    CHECK(col_rel_source_reader_release(&result_reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    destroy_session(s);
}

static void
test_unary_snapshot_and_refusals(void)
{
    for (unsigned field = 0; field < 7; field++) {
        wl_col_session_t *s = new_session();
        wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
        (void)unary_frame(s, &f, 2, true);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f);
        stack->items[1].is_delta = true;
        stack->items[1].seg_boundaries = segments();
        stack->items[1].seg_count = 1;
        eval_entry_t original = stack->items[1];
        frame_begin(0); CHECK(unary_begin(f) == 0);
        col_rel_t *output = unary_relation(s, 9000); *unary_output(f) = output;
        /* Deliberate validation fault, restored before mandatory ownership
         * cleanup. No corruption repair or active raw mutation is supported. */
        if (field == 0) stack->items[1].rel = stack->items[0].rel;
        else if (field == 1) stack->items[1].owned = false;
        else if (field == 2) stack->items[1].is_delta = false;
        else if (field == 3) stack->items[1].seg_boundaries = NULL;
        else if (field == 4) stack->items[1].seg_count = 2;
        else if (field ==
            5) stack->items[1].kind = WL_COLUMNAR_EVAL_ENTRY_CONTINUATION;
        else stack->items[1].continuation =
                (wl_columnar_continuation_t *)&frame_owner;
        bool committed = true;
        CHECK(wl_columnar_eval_stack_unary_commit_attempt_controlled(f,
            &committed) == EINVAL && !committed);
        stack->items[1] = original;
        unary_row(original.rel, 1001); unary_row(output, 9000);
        CHECK(wl_columnar_eval_stack_unary_finish(f, EINVAL) == EINVAL);
        CHECK(frame_finish(EINVAL) == 544 + 56 + 192);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0);
        destroy_session(s);
    }
    /* Genuine continuation input is rejected without popping or destruction,
     * and an established typed verdict wins later same-ID cancellation. */
    wl_col_session_t *s = new_session();
    wl_columnar_eval_stack_cleanup_frame_t *f = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &f) == 0);
    struct reader_context ctx = { 0 };
    col_rel_t *r = unary_relation(s, 777);
    CHECK(col_rel_source_reader_acquire(r, &ctx.reader) == 0);
    wl_columnar_continuation_producer_t producer = {
        .context = &ctx, .produce = unused_produce, .destroy = release_reader
    };
    wl_columnar_continuation_cursor_t cursor = { 0 };
    wl_columnar_continuation_t *cont =
        wl_columnar_continuation_create(&producer, &cursor);
    CHECK(cont &&
        eval_stack_push_continuation(wl_columnar_eval_stack_cleanup_stack(f),
        cont) == 0);
    frame_hooks(true); frame_target = true; frame_target_kind = FRAME_LOCAL;
    frame_target_phase = WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_VALID_PHASE;
    frame_stop = 0; frame_mode = 0; frame_begin(0);
    CHECK(unary_begin(f) == ENOTSUP);
    CHECK(ctx.destroyed == 0 &&
        wl_columnar_eval_stack_cleanup_stack(f)->top == 1);
    CHECK(frame_finish(ENOTSUP) == 160);
    frame_target = false; frame_stop = 0; frame_hooks(false);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&f) == 0 && ctx.destroyed == 1);
    col_rel_destroy(r); destroy_session(s);
}

int
main(void)
{
    test_admission_and_nesting();
    for (unsigned storage = 0; storage < 3; storage++)
        test_storage_refusal(storage);
    test_progress_past_refusal();
    test_continuation_reader_cleanup();
    test_frame_literal_admissions();
    test_frame_all_interruptions();
    test_frame_faults_and_peer();
    test_frame_typed_precedence();
    test_frame_contract_and_retained_cleanup();
    test_frame_sole_pin();
    test_unary_frame_initialization();
    test_unary_literal_boundaries();
    test_unary_aliases();
    test_unary_retained_and_thread_lifetime();
    test_unary_all_interruptions();
    test_unary_interrupted_aliases();
    test_unary_contract_and_generic_cleanup();
    test_unary_snapshot_and_refusals();
    puts("eval cleanup: all ownership/admission cases passed");
    return 0;
}
