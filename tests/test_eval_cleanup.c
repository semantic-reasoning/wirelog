/* Persistent evaluator cleanup ownership (Issue #1661).
* Copyright (C) CleverPlant. Licensed under LGPL-3.0. */
#include "../wirelog/columnar/internal.h"
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

int
main(void)
{
    test_admission_and_nesting();
    for (unsigned storage = 0; storage < 3; storage++)
        test_storage_refusal(storage);
    test_progress_past_refusal();
    test_continuation_reader_cleanup();
    puts("eval cleanup: all ownership/admission cases passed");
    return 0;
}
