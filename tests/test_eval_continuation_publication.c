/* Focused contract tests for issue #1450. */

#include "../wirelog/columnar/internal.h"

#include <stdio.h>
#include <string.h>

typedef struct {
    int produce_calls;
    int destroy_calls;
    bool stale;
    bool empty_temporary;
    bool completion_only;
    bool unsupported;
    bool return_done;
    int cancel_calls;
    unsigned char payload[2];
} fake_producer_t;

typedef struct {
    uint64_t budget;
    uint64_t reserved;
    int begins;
    int appends;
    int commits;
    int aborts;
    int commit_mode; /* 0 success, 1 pre-commit failure, 2 post-commit */
    uint64_t reserved_before;
} fake_sink_t;

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", message); \
                failures++; \
            } \
} while (0)

static wl_columnar_continuation_status_t
produce(void *context, const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch)
{
    fake_producer_t *producer = context;

    producer->produce_calls++;
    if (producer->unsupported)
        return WL_COLUMNAR_CONTINUATION_UNSUPPORTED;
    if (producer->return_done)
        return WL_COLUMNAR_CONTINUATION_DONE;
    if (producer->completion_only) {
        batch->payload = NULL;
        batch->bytes = 0;
        batch->rows = 0;
        batch->complete = true;
        batch->next_cursor = *cursor;
        batch->next_cursor.position = UINT64_C(999);
        batch->next_cursor.sequence = UINT64_C(999);
        return WL_COLUMNAR_CONTINUATION_OK;
    }
    if (producer->empty_temporary) {
        batch->payload = producer->payload;
        batch->bytes = 1;
        batch->rows = 0;
        batch->complete = false;
        batch->next_cursor = *cursor;
        return WL_COLUMNAR_CONTINUATION_OK;
    }
    if (cursor->position >= 2)
        return WL_COLUMNAR_CONTINUATION_DONE;
    batch->payload = producer->payload + cursor->position;
    batch->bytes = cursor->position == 0 ? 8 : 16;
    batch->rows = 1;
    batch->complete = cursor->position == 1;
    batch->next_cursor = *cursor;
    batch->next_cursor.position++;
    batch->next_cursor.sequence++;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static bool
validate(void *context, const wl_columnar_continuation_cursor_t *cursor)
{
    fake_producer_t *producer = context;
    (void)cursor;
    return !producer->stale;
}

static void
destroy_producer(void *context)
{
    fake_producer_t *producer = context;
    producer->destroy_calls++;
}

static void
cancel_producer(void *context)
{
    fake_producer_t *producer = context;
    producer->cancel_calls++;
}

static wl_columnar_continuation_status_t
sink_begin(void *context, const wl_columnar_continuation_batch_t *batch)
{
    fake_sink_t *sink = context;
    (void)batch;
    sink->begins++;
    sink->reserved_before = sink->reserved;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
sink_reserve(void *context, uint64_t bytes, uint32_t rows)
{
    fake_sink_t *sink = context;
    (void)rows;
    if (bytes > sink->budget - sink->reserved)
        return WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED;
    sink->reserved += bytes;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
sink_append(void *context, const wl_columnar_continuation_batch_t *batch)
{
    fake_sink_t *sink = context;
    CHECK(batch->rows == 1, "sink receives a non-empty batch");
    sink->appends++;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
sink_commit(void *context, bool *committed)
{
    fake_sink_t *sink = context;
    sink->commits++;
    if (sink->commit_mode == 1) {
        *committed = false;
        return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE;
    }
    if (sink->commit_mode == 2) {
        sink->commit_mode = 0;
        *committed = true;
        return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE;
    }
    *committed = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
sink_abort(void *context)
{
    fake_sink_t *sink = context;
    sink->aborts++;
    sink->reserved = sink->reserved_before;
}

static wl_columnar_continuation_t *
make_continuation(fake_producer_t *producer)
{
    const wl_columnar_continuation_producer_t spec = {
        .context = producer,
        .produce = produce,
        .validate = validate,
        .destroy = destroy_producer,
        .cancel = cancel_producer
    };
    const wl_columnar_continuation_cursor_t cursor = {
        UINT64_C(11), UINT64_C(22), 7, 0, 0
    };
    return wl_columnar_continuation_create(&spec, &cursor);
}

static wl_columnar_continuation_sink_t
make_sink(fake_sink_t *sink)
{
    const wl_columnar_continuation_sink_t result = {
        sink, sink_begin, sink_reserve, sink_append, sink_commit, sink_abort
    };
    return result;
}

static void
test_retry_and_commit_boundary(void)
{
    fake_producer_t producer = { 0 };
    fake_sink_t sink = { UINT64_C(128), 0, 0, 0, 0, 0, 1 };
    wl_columnar_continuation_t *continuation = make_continuation(&producer);
    wl_columnar_continuation_sink_t sink_spec = make_sink(&sink);
    uint64_t position;

    CHECK(continuation != NULL, "continuation is created");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE,
        "pre-commit failure is reported");
    position = wl_columnar_continuation_cursor(continuation)->position;
    CHECK(position == 0, "pre-commit failure leaves cursor unchanged");
    CHECK(sink.aborts == 1, "pre-commit failure aborts sink");
    CHECK(sink.reserved == 0, "pre-commit abort restores reservation");

    sink.commit_mode = 0;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "retry publishes the first batch");
    CHECK(wl_columnar_continuation_cursor(continuation)->position == 1,
        "cursor advances only after commit");
    CHECK(sink.appends == 2, "retry consumes the first batch once");

    sink.commit_mode = 2;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS,
        "post-commit failure is reported as ambiguous");
    CHECK(wl_columnar_continuation_cursor(continuation)->position == 2,
        "post-commit failure advances the durable cursor");
    CHECK(wl_columnar_continuation_is_done(continuation),
        "ambiguous commit marks a durable final batch done");
    CHECK(sink.reserved == UINT64_C(24),
        "ambiguous commit does not falsely release reservation");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_DONE,
        "durable ambiguous batch is not replayed");
    CHECK(sink.appends == 3,
        "post-commit retry does not append the batch again");
    wl_columnar_continuation_destroy(continuation);
    CHECK(producer.destroy_calls == 1, "continuation owns producer lifetime");
}

static void
test_stale_and_empty_batches(void)
{
    fake_producer_t producer = { 0 };
    fake_sink_t sink = { UINT64_C(128), 0, 0, 0, 0, 0, 0 };
    wl_columnar_continuation_sink_t sink_spec = make_sink(&sink);
    wl_columnar_continuation_t *continuation;

    producer.stale = true;
    continuation = make_continuation(&producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_STALE,
        "stale input is rejected before publication");
    CHECK(sink.begins == 0, "stale input does not begin a sink transaction");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    producer.return_done = true;
    continuation = make_continuation(&producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_DONE,
        "producer DONE becomes persistent terminal state");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_DONE,
        "persistent DONE avoids another producer call");
    CHECK(producer.produce_calls == 1, "DONE producer is called only once");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    producer.empty_temporary = true;
    continuation = make_continuation(&producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID,
        "empty temporary batch is not completion");
    CHECK(wl_columnar_continuation_cursor(continuation)->position == 0,
        "invalid empty batch leaves cursor unchanged");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    producer.completion_only = true;
    continuation = make_continuation(&producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_DONE,
        "completion-only empty batch completes without publication");
    CHECK(wl_columnar_continuation_cursor(continuation)->position == 0,
        "completion-only batch cannot overwrite the cursor");
    CHECK(sink.begins == 0, "completion-only batch skips sink begin");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    producer.unsupported = true;
    continuation = make_continuation(&producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_UNSUPPORTED,
        "unsupported boundary is explicit");
    CHECK(sink.begins == 0, "unsupported boundary does not begin a sink");
    wl_columnar_continuation_cancel(continuation);
    CHECK(producer.cancel_calls == 1, "cancellation is delivered to producer");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID,
        "cancelled continuation cannot publish");
    wl_columnar_continuation_destroy(continuation);
}

static void
test_reservation_and_stack_ownership(void)
{
    fake_producer_t producer = { 0 };
    fake_sink_t sink = { UINT64_C(4), 0, 0, 0, 0, 0, 0 };
    wl_columnar_continuation_sink_t sink_spec = make_sink(&sink);
    wl_columnar_continuation_t *continuation = make_continuation(&producer);
    eval_stack_t stack;
    col_rel_t *relation;

    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED,
        "reservation denial is explicit");
    CHECK(wl_columnar_continuation_cursor(continuation)->position == 0,
        "reservation denial leaves cursor unchanged");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    sink = (fake_sink_t){ UINT64_C(8), 0, 0, 0, 0, 0, 1, 0 };
    sink_spec = make_sink(&sink);
    continuation = make_continuation(&producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE,
        "exact-fit pre-commit failure is retryable");
    CHECK(sink.reserved == 0, "abort restores exact-fit reservation");
    sink.commit_mode = 0;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "exact-fit retry succeeds after reservation rollback");
    CHECK(sink.reserved == UINT64_C(8),
        "successful exact-fit retry retains its reservation");
    wl_columnar_continuation_destroy(continuation);

    sink = (fake_sink_t){ UINT64_C(128), 0, 0, 0, 0, 0, 0, 0 };
    sink_spec = make_sink(&sink);
    eval_stack_init(&stack);
    relation = col_rel_new_auto("complete", 1);
    CHECK(relation != NULL, "complete relation can still be pushed");
    CHECK(eval_stack_push(&stack, relation, true) == 0,
        "complete relation push remains available");
    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "continuation is retained as an owned stack entry");
    {
        eval_entry_t entry = eval_stack_pop(&stack);
        CHECK(entry.kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION,
            "stack distinguishes continuation entries");
        CHECK(entry.rel == NULL && entry.continuation == continuation,
            "continuation-aware pop preserves the handoff ownership");
        CHECK(wl_columnar_continuation_publish(entry.continuation, &sink_spec)
            == WL_COLUMNAR_CONTINUATION_OK,
            "continuation-aware consumer can publish the carried result");
        wl_columnar_continuation_destroy(entry.continuation);
    }
    CHECK(producer.destroy_calls == 1,
        "handed-off continuation is destroyed exactly once");

    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "continuation can reach a legacy boundary");
    {
        eval_entry_t rejected;
        CHECK(eval_stack_pop_relation(&stack, &rejected) == ENOTSUP,
            "legacy relation-only boundary rejects continuation explicitly");
        CHECK(rejected.continuation == NULL && rejected.rel == NULL,
            "legacy rejection does not expose a materialized relation");
    }
    CHECK(producer.destroy_calls == 1,
        "legacy rejection destroys continuation exactly once");

    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "second continuation is retained for drain");
    {
        wl_plan_op_t op = { 0 };
        CHECK(col_op_map(&op, &stack, NULL) == ENOTSUP,
            "materializing MAP boundary rejects continuation before output");
    }
    CHECK(producer.destroy_calls == 1,
        "materializing boundary rejection does not leak continuation");

    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "continuation can reach a REDUCE boundary");
    {
        wl_plan_op_t op = { 0 };
        CHECK(col_op_reduce(&op, &stack, NULL) == ENOTSUP,
            "materializing REDUCE boundary rejects continuation");
    }
    CHECK(producer.destroy_calls == 1,
        "REDUCE boundary destroys continuation exactly once");

    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "mixed stack accepts a continuation for drain");
    eval_stack_drain(&stack);
    CHECK(producer.destroy_calls == 1,
        "drain destroys an unpopped continuation exactly once");
    CHECK(stack.top == 0, "complete relation stack still drains its owner");

    /* Raw top-level collection preserves the historical NULL/no-result
     * value, while relation-only boundaries reject that value with EINVAL. */
    eval_stack_init(&stack);
    CHECK(eval_stack_push(&stack, NULL, false) == 0,
        "legacy NULL no-result can be pushed");
    {
        eval_entry_t no_result;
        no_result = eval_stack_pop(&stack);
        CHECK(no_result.rel == NULL
            && no_result.kind == WL_COLUMNAR_EVAL_ENTRY_RELATION,
            "raw top-level pop preserves legacy NULL no-result");
    }
    CHECK(eval_stack_push(&stack, NULL, false) == 0,
        "relation-only NULL operand can be pushed");
    {
        eval_entry_t rejected;
        CHECK(eval_stack_pop_relation(&stack, &rejected) == EINVAL,
            "relation-only boundary rejects NULL with historical EINVAL");
        CHECK(rejected.rel == NULL && stack.top == 0,
            "relation-only NULL rejection consumes the entry safely");
    }
    CHECK(eval_stack_push(&stack, NULL, false) == 0,
        "MAP NULL operand can be pushed");
    {
        wl_plan_op_t op = { 0 };
        CHECK(col_op_map(&op, &stack, NULL) == EINVAL,
            "MAP retains its historical NULL operand rejection");
    }

    /* CONCAT pops the right relation before the left operand.  Make the
     * left operand a continuation and attach owned metadata to the right
     * operand so the second-pop rejection exercises every cleanup owner. */
    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    relation = col_rel_new_auto("concat-cleanup", 1);
    CHECK(relation != NULL, "concat cleanup relation can be created");
    eval_stack_init(&stack);
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "concat cleanup continuation is pushed first");
    CHECK(eval_stack_push(&stack, relation, true) == 0,
        "concat cleanup relation is pushed second");
    stack.items[stack.top - 1].seg_boundaries
        = (uint32_t *)malloc(2 * sizeof(uint32_t));
    CHECK(stack.items[stack.top - 1].seg_boundaries != NULL,
        "concat cleanup metadata is allocated");
    if (stack.items[stack.top - 1].seg_boundaries) {
        stack.items[stack.top - 1].seg_boundaries[0] = 0;
        stack.items[stack.top - 1].seg_boundaries[1] = 0;
        stack.items[stack.top - 1].seg_count = 1;
    }
    CHECK(col_op_concat(&stack, NULL) == ENOTSUP,
        "concat rejects a continuation on its second pop");
    CHECK(producer.destroy_calls == 1,
        "concat rejection destroys continuation exactly once");
    CHECK(stack.top == 0,
        "concat rejection consumes both owned eval entries");

    /* CONCAT's first pop rejects the top entry before touching the lower
     * operand.  Keep an owned relation with boundary metadata below it so
     * the remaining stack ownership is explicit and can be drained safely. */
    memset(&producer, 0, sizeof(producer));
    continuation = make_continuation(&producer);
    relation = col_rel_new_auto("concat-first-pop", 1);
    CHECK(relation != NULL, "concat first-pop relation can be created");
    eval_stack_init(&stack);
    CHECK(eval_stack_push(&stack, relation, true) == 0,
        "concat first-pop relation is pushed first");
    stack.items[stack.top - 1].seg_boundaries
        = (uint32_t *)malloc(2 * sizeof(uint32_t));
    CHECK(stack.items[stack.top - 1].seg_boundaries != NULL,
        "concat first-pop metadata is allocated");
    if (stack.items[stack.top - 1].seg_boundaries) {
        stack.items[stack.top - 1].seg_boundaries[0] = 0;
        stack.items[stack.top - 1].seg_boundaries[1] = 0;
        stack.items[stack.top - 1].seg_count = 1;
    }
    CHECK(eval_stack_push_continuation(&stack, continuation) == 0,
        "concat first-pop continuation is pushed second");
    CHECK(col_op_concat(&stack, NULL) == ENOTSUP,
        "concat rejects a continuation on its first pop");
    CHECK(producer.destroy_calls == 1,
        "concat first-pop rejection destroys continuation exactly once");
    CHECK(stack.top == 1 && stack.items[0].rel == relation,
        "concat first-pop rejection leaves the lower relation on the stack");
    eval_stack_drain(&stack);
    CHECK(stack.top == 0,
        "concat first-pop remainder drains its relation and boundaries");
}

typedef struct {
    wl_columnar_continuation_t *continuation;
    const wl_columnar_continuation_sink_t *sink_spec;
    unsigned char payload[8];
    char events[64];
    size_t event_count;
    uint64_t seen_position[8];
    int produce_calls, validate_calls, ack_calls, dispose_calls;
    int cancel_calls, destroy_calls;
    bool progress_first, progress_done, stale, batch_ready;
    char reenter_event;
    bool reenter_publish, reenter_cancel;
    bool reenter_destroy, destroy_reenter;
    int reentrant_publishes, reentrant_cancels;
    int reentrant_destroys;
    wl_columnar_continuation_status_t reentrant_status;
    bool published_cursor_visible_in_ack;
    const void *disposed_payload;
} protocol_producer_t;

typedef struct {
    protocol_producer_t *producer;
    uint64_t reserved, reserved_before;
    int begin_calls, reserve_calls, append_calls, commit_calls, abort_calls;
    unsigned begin_refusals, reserve_refusals, append_refusals;
    bool commit_uncommitted, commit_ambiguous;
    const void *last_payload;
} protocol_sink_t;

static void
protocol_event(protocol_producer_t *producer, char event)
{
    CHECK(producer->event_count + 1 < sizeof(producer->events),
        "protocol event log has capacity");
    if (producer->event_count + 1 < sizeof(producer->events)) {
        producer->events[producer->event_count++] = event;
        producer->events[producer->event_count] = '\0';
    }
}

static void
protocol_reenter(protocol_producer_t *producer, char event)
{
    if (producer->reenter_event != event)
        return;
    if (producer->reenter_publish) {
        ++producer->reentrant_publishes;
        producer->reentrant_status = wl_columnar_continuation_publish(
            producer->continuation, producer->sink_spec);
    }
    if (producer->reenter_cancel) {
        ++producer->reentrant_cancels;
        wl_columnar_continuation_cancel(producer->continuation);
    }
    if (producer->reenter_destroy) {
        ++producer->reentrant_destroys;
        wl_columnar_continuation_destroy(producer->continuation);
    }
}

static wl_columnar_continuation_status_t
protocol_produce(void *context, const wl_columnar_continuation_cursor_t *cursor,
    wl_columnar_continuation_batch_t *batch)
{
    protocol_producer_t *producer = context;
    protocol_event(producer, 'P');
    protocol_reenter(producer, 'P');
    CHECK(producer->produce_calls < (int)(sizeof(producer->seen_position)
        / sizeof(producer->seen_position[0])), "producer call bound");
    if (producer->produce_calls < (int)(sizeof(producer->seen_position)
        / sizeof(producer->seen_position[0])))
        producer->seen_position[producer->produce_calls] = cursor->position;
    ++producer->produce_calls;
    if (producer->progress_first && !producer->progress_done) {
        producer->progress_done = true;
        batch->next_cursor = *cursor;
        ++batch->next_cursor.position;
        ++batch->next_cursor.sequence;
        return WL_COLUMNAR_CONTINUATION_PROGRESS;
    }
    if (producer->batch_ready)
        return WL_COLUMNAR_CONTINUATION_DONE;
    producer->batch_ready = true;
    batch->payload = producer->payload;
    batch->bytes = sizeof(producer->payload);
    batch->rows = 1;
    batch->complete = false;
    batch->next_cursor = *cursor;
    ++batch->next_cursor.position;
    ++batch->next_cursor.sequence;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static bool
protocol_validate(void *context,
    const wl_columnar_continuation_cursor_t *cursor)
{
    protocol_producer_t *producer = context;
    (void)cursor;
    ++producer->validate_calls;
    protocol_reenter(producer, 'V');
    return !producer->stale;
}

static void
protocol_disposition(void *context,
    const wl_columnar_continuation_batch_t *batch, bool committed)
{
    protocol_producer_t *producer = context;
    CHECK(batch->payload == producer->payload,
        "disposition receives the exact frozen payload");
    producer->disposed_payload = batch->payload;
    if (committed) {
        ++producer->ack_calls;
        protocol_event(producer, 'K');
        producer->published_cursor_visible_in_ack = producer->continuation
            && wl_columnar_continuation_cursor(
            producer->continuation)->position
            == batch->next_cursor.position;
        protocol_reenter(producer, 'K');
    } else {
        ++producer->dispose_calls;
        protocol_event(producer, 'X');
        protocol_reenter(producer, 'X');
    }
}

static void
protocol_cancel(void *context)
{
    protocol_producer_t *producer = context;
    ++producer->cancel_calls;
    protocol_event(producer, 'Z');
    protocol_reenter(producer, 'Z');
}

static void
protocol_destroy(void *context)
{
    protocol_producer_t *producer = context;
    ++producer->destroy_calls;
    protocol_event(producer, 'D');
    if (producer->destroy_reenter)
        wl_columnar_continuation_destroy(producer->continuation);
}

static wl_columnar_continuation_status_t
protocol_begin(void *context, const wl_columnar_continuation_batch_t *batch)
{
    protocol_sink_t *sink = context;
    ++sink->begin_calls;
    sink->last_payload = batch->payload;
    sink->reserved_before = sink->reserved;
    protocol_event(sink->producer, 'B');
    protocol_reenter(sink->producer, 'B');
    if (sink->begin_refusals) {
        --sink->begin_refusals;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
protocol_reserve(void *context, uint64_t bytes, uint32_t rows)
{
    protocol_sink_t *sink = context;
    ++sink->reserve_calls;
    protocol_event(sink->producer, 'R');
    protocol_reenter(sink->producer, 'R');
    CHECK(rows == 1, "protocol sink reserves one row");
    if (sink->reserve_refusals) {
        --sink->reserve_refusals;
        return WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED;
    }
    sink->reserved += bytes;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
protocol_append(void *context, const wl_columnar_continuation_batch_t *batch)
{
    protocol_sink_t *sink = context;
    ++sink->append_calls;
    protocol_event(sink->producer, 'A');
    protocol_reenter(sink->producer, 'A');
    CHECK(batch->payload == sink->last_payload,
        "append sees the payload retained at begin");
    if (sink->append_refusals) {
        --sink->append_refusals;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
protocol_commit(void *context, bool *committed)
{
    protocol_sink_t *sink = context;
    ++sink->commit_calls;
    protocol_event(sink->producer, 'C');
    protocol_reenter(sink->producer, 'C');
    if (sink->commit_uncommitted) {
        sink->commit_uncommitted = false;
        *committed = false;
        return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE;
    }
    *committed = true;
    return sink->commit_ambiguous
        ? WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE
        : WL_COLUMNAR_CONTINUATION_OK;
}

static void
protocol_abort(void *context)
{
    protocol_sink_t *sink = context;
    ++sink->abort_calls;
    sink->reserved = sink->reserved_before;
    protocol_event(sink->producer, 'T');
    protocol_reenter(sink->producer, 'T');
}

static wl_columnar_continuation_t *
make_protocol_continuation(protocol_producer_t *producer, bool progress_first)
{
    const wl_columnar_continuation_producer_t spec = {
        .context = producer,
        .produce = protocol_produce,
        .validate = protocol_validate,
        .destroy = protocol_destroy,
        .cancel = protocol_cancel,
        .disposition = protocol_disposition
    };
    const wl_columnar_continuation_cursor_t cursor = {
        UINT64_C(11), UINT64_C(22), 7, 0, 0
    };
    producer->progress_first = progress_first;
    return wl_columnar_continuation_create(&spec, &cursor);
}

static wl_columnar_continuation_sink_t
make_protocol_sink(protocol_sink_t *sink, protocol_producer_t *producer)
{
    const wl_columnar_continuation_sink_t spec = {
        .context = sink,
        .begin = protocol_begin,
        .reserve = protocol_reserve,
        .append = protocol_append,
        .commit = protocol_commit,
        .abort = protocol_abort
    };
    sink->producer = producer;
    return spec;
}

static void
test_protocol_progress_and_frozen_begin_refusal(void)
{
    protocol_producer_t producer = { 0 };
    protocol_sink_t sink = { .begin_refusals = 2 };
    wl_columnar_continuation_t *continuation =
        make_protocol_continuation(&producer, true);
    wl_columnar_continuation_sink_t sink_spec =
        make_protocol_sink(&sink, &producer);
    const wl_columnar_continuation_cursor_t *published;

    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_PROGRESS,
        "zero-payload progress is distinct from completion");
    published = wl_columnar_continuation_cursor(continuation);
    CHECK(published->position == 0 && published->sequence == 0,
        "progress advances only the private scan cursor");
    CHECK(producer.produce_calls == 1 && producer.seen_position[0] == 0,
        "first producer call uses the initial scan cursor");

    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_SINK_FAILURE,
        "first begin refusal retains the candidate batch");
    CHECK(wl_columnar_continuation_cursor(continuation)->position == 0
        && producer.produce_calls == 2 && producer.seen_position[1] == 1
        && producer.ack_calls == 0 && producer.dispose_calls == 0,
        "batch scan endpoint is ahead of public cursor and not yet acknowledged");
    const void *payload = sink.last_payload;
    CHECK(payload == producer.payload && sink.abort_calls == 0,
        "begin refusal starts no transaction and does not abort");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_SINK_FAILURE,
        "repeated begin refusal retries the same frozen batch");
    CHECK(producer.produce_calls == 2 && sink.last_payload == payload
        && sink.abort_calls == 0,
        "begin refusal never calls the producer again or aborts");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "retained batch commits after begin becomes available");
    published = wl_columnar_continuation_cursor(continuation);
    CHECK(published->position == 2 && published->sequence == 2
        && producer.ack_calls == 1 && producer.dispose_calls == 0
        && producer.disposed_payload == payload && producer.produce_calls == 2,
        "commit acknowledges once and publishes the frozen endpoint");
    CHECK(strcmp(producer.events, "PPBBBRACK") == 0,
        "producer, sink and acknowledgment callbacks retain transaction order");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_DONE,
        "producer completion follows the committed scan endpoint");
    CHECK(producer.seen_position[2] == 2
        && wl_columnar_continuation_cursor(continuation)->position == 2,
        "completion preserves the last published cursor");
    wl_columnar_continuation_destroy(continuation);
    CHECK(producer.cancel_calls == 0 && producer.destroy_calls == 1,
        "completed producer is destroyed without cancellation");
}

static void
test_protocol_reserve_append_and_uncommitted_retries(void)
{
    for (unsigned failure = 0; failure < 3; ++failure) {
        protocol_producer_t producer = { 0 };
        protocol_sink_t sink = { 0 };
        wl_columnar_continuation_t *continuation =
            make_protocol_continuation(&producer, false);
        wl_columnar_continuation_sink_t sink_spec =
            make_protocol_sink(&sink, &producer);
        if (failure == 0) sink.reserve_refusals = 1;
        else if (failure == 1) sink.append_refusals = 1;
        else sink.commit_uncommitted = true;
        wl_columnar_continuation_status_t expected = failure == 0
            ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
            : failure == 2 ? WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE
            : WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
            == expected, "post-begin failure is reported without losing batch");
        CHECK(sink.abort_calls == 1 && sink.reserved == 0
            && producer.produce_calls == 1 && producer.ack_calls == 0
            && producer.dispose_calls == 0
            && wl_columnar_continuation_cursor(continuation)->position == 0,
            "post-begin refusal aborts once, rolls back, and retains payload");
        CHECK(sink.last_payload == producer.payload,
            "failing transaction received the producer's payload");
        CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
            == WL_COLUMNAR_CONTINUATION_OK,
            "retained batch succeeds on retry after sink refusal");
        CHECK(producer.produce_calls == 1 && producer.ack_calls == 1
            && producer.dispose_calls == 0 && sink.abort_calls == 1
            && sink.reserved == sizeof(producer.payload)
            && wl_columnar_continuation_cursor(continuation)->position == 1,
            "retry reuses payload and commits exactly once");
        wl_columnar_continuation_destroy(continuation);
    }
}

static void
test_protocol_ambiguous_cancel_stale_and_destroy(void)
{
    protocol_producer_t producer = { 0 };
    protocol_sink_t sink = { .commit_ambiguous = true };
    wl_columnar_continuation_t *continuation =
        make_protocol_continuation(&producer, false);
    wl_columnar_continuation_sink_t sink_spec =
        make_protocol_sink(&sink, &producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS,
        "ambiguous durable commit consumes the pending batch");
    CHECK(producer.ack_calls == 1 && producer.dispose_calls == 0
        && sink.abort_calls == 0 && producer.produce_calls == 1
        && wl_columnar_continuation_cursor(continuation)->position == 1,
        "ambiguous commit acknowledges and advances without rollback");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_DONE && producer.produce_calls == 2,
        "ambiguous committed payload cannot be replayed");
    wl_columnar_continuation_destroy(continuation);
    CHECK(producer.cancel_calls == 0 && producer.destroy_calls == 1,
        "done producer does not receive terminal cancellation");

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, true);
    sink_spec = make_protocol_sink(&sink, &producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_PROGRESS,
        "producer can yield during private scan progress");
    wl_columnar_continuation_cancel(continuation);
    wl_columnar_continuation_cancel(continuation);
    CHECK(producer.cancel_calls == 1 && producer.dispose_calls == 0
        && producer.destroy_calls == 0,
        "cancel during progress retires producer once without a batch");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID,
        "cancelled protocol cannot advance again");
    wl_columnar_continuation_destroy(continuation);
    CHECK(producer.cancel_calls == 1 && producer.destroy_calls == 1,
        "cancel followed by destroy does not cancel twice");
    CHECK(strcmp(producer.events, "PZD") == 0,
        "progress cancellation retires before producer destruction");

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    sink.begin_refusals = 1;
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_SINK_FAILURE,
        "stale test freezes a pending payload");
    producer.stale = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_STALE,
        "stale pending input terminally rejects publication");
    CHECK(producer.dispose_calls == 1 && producer.ack_calls == 0
        && producer.cancel_calls == 1 && sink.abort_calls == 0,
        "stale pending batch is disposed and producer retired once");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_STALE,
        "stale terminal status is persistent");
    wl_columnar_continuation_cancel(continuation);
    wl_columnar_continuation_destroy(continuation);
    CHECK(producer.produce_calls == 1 && producer.dispose_calls == 1
        && producer.cancel_calls == 1 && producer.destroy_calls == 1,
        "stale then cancel/destroy does not repeat producer lifecycle callbacks");
    CHECK(strcmp(producer.events, "PBXZD") == 0,
        "stale cleanup drops payload before retirement and destruction");

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    sink.begin_refusals = 1;
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_SINK_FAILURE,
        "destroy test freezes a pending payload");
    wl_columnar_continuation_destroy(continuation);
    CHECK(producer.dispose_calls == 1 && producer.ack_calls == 0
        && producer.cancel_calls == 1 && producer.destroy_calls == 1
        && sink.reserved == 0,
        "destroy disposes pending payload and retires producer exactly once");
    CHECK(strcmp(producer.events, "PBXZD") == 0,
        "destroy cleanup drops payload before retirement and destruction");
}

static void
test_protocol_callback_reentrancy(void)
{
    protocol_producer_t producer = { 0 };
    protocol_sink_t sink = { 0 };
    wl_columnar_continuation_t *continuation =
        make_protocol_continuation(&producer, false);
    wl_columnar_continuation_sink_t sink_spec =
        make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'B';
    producer.reenter_publish = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "reentrant publish cannot interrupt the owning publication");
    CHECK(producer.reentrant_publishes == 1
        && producer.reentrant_status == WL_COLUMNAR_CONTINUATION_INVALID
        && producer.produce_calls == 1 && sink.begin_calls == 1
        && producer.ack_calls == 1,
        "reentrant publish is rejected without producing or committing twice");
    CHECK(producer.published_cursor_visible_in_ack,
        "published cursor advances before producer acknowledgment callback");
    wl_columnar_continuation_destroy(continuation);

    const char events[] = {'B', 'R', 'A'};
    const char *expected[] = {"PBTXZ", "PBRTXZ", "PBRATXZ"};
    for (size_t i = 0; i < sizeof(events) / sizeof(events[0]); ++i) {
        memset(&producer, 0, sizeof(producer));
        memset(&sink, 0, sizeof(sink));
        continuation = make_protocol_continuation(&producer, false);
        sink_spec = make_protocol_sink(&sink, &producer);
        producer.continuation = continuation;
        producer.sink_spec = &sink_spec;
        producer.reenter_event = events[i];
        producer.reenter_publish = true;
        producer.reenter_cancel = true;
        CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
            == WL_COLUMNAR_CONTINUATION_INVALID,
            "deferred cancel is honored after an active transaction callback");
        CHECK(producer.reentrant_publishes == 1
            && producer.reentrant_status == WL_COLUMNAR_CONTINUATION_INVALID
            && producer.reentrant_cancels == 1 && sink.commit_calls == 0
            && sink.abort_calls == 1 && sink.reserved == 0
            && producer.ack_calls == 0 && producer.dispose_calls == 1
            && producer.cancel_calls == 1
            && wl_columnar_continuation_cursor(continuation)->position == 0,
            "callback cancellation aborts once and drops the uncommitted batch");
        CHECK(strcmp(producer.events, expected[i]) == 0,
            "callback cancel orders abort, disposal and producer retirement");
        CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
            == WL_COLUMNAR_CONTINUATION_INVALID
            && producer.produce_calls == 1,
            "terminal cancellation prevents later producer reentry");
        wl_columnar_continuation_destroy(continuation);
        CHECK(producer.cancel_calls == 1 && producer.destroy_calls == 1,
            "destroy after reentrant cancellation does not retire twice");
    }

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    sink.commit_uncommitted = true;
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'C';
    producer.reenter_cancel = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID,
        "cancel requested during uncommitted commit aborts the transaction");
    CHECK(sink.abort_calls == 1 && producer.dispose_calls == 1
        && producer.ack_calls == 0 && producer.cancel_calls == 1
        && wl_columnar_continuation_cursor(continuation)->position == 0,
        "uncommitted commit cancellation retains no pending payload");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'C';
    producer.reenter_cancel = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "durable commit wins cancellation requested by commit callback");
    CHECK(sink.abort_calls == 0 && producer.ack_calls == 1
        && producer.dispose_calls == 0 && producer.cancel_calls == 1
        && producer.published_cursor_visible_in_ack
        && wl_columnar_continuation_cursor(continuation)->position == 1,
        "durable commit is acknowledged after cursor publication exactly once");
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID && producer.produce_calls == 1,
        "deferred post-commit cancellation prevents further scanning");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'K';
    producer.reenter_publish = true;
    producer.reenter_cancel = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "ack callback may request cancellation after a durable commit");
    CHECK(producer.ack_calls == 1 && producer.cancel_calls == 1
        && producer.reentrant_publishes == 1
        && producer.reentrant_status == WL_COLUMNAR_CONTINUATION_INVALID
        && producer.published_cursor_visible_in_ack && sink.abort_calls == 0,
        "ack reentry observes committed state and rejects nested publication");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, true);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'P';
    producer.reenter_publish = true;
    producer.reenter_cancel = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID,
        "producer callback cancellation terminates a progress turn safely");
    CHECK(producer.reentrant_publishes == 1
        && producer.reentrant_status == WL_COLUMNAR_CONTINUATION_INVALID
        && producer.cancel_calls == 1 && producer.produce_calls == 1
        && wl_columnar_continuation_cursor(continuation)->position == 0,
        "progress callback reentry cannot advance the published cursor");
    wl_columnar_continuation_destroy(continuation);

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'C';
    producer.reenter_destroy = true;
    producer.destroy_reenter = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "destroy requested during durable commit is deferred through publish");
    CHECK(producer.reentrant_destroys == 1 && sink.abort_calls == 0
        && producer.ack_calls == 1 && producer.dispose_calls == 0
        && producer.cancel_calls == 1 && producer.destroy_calls == 1
        && producer.published_cursor_visible_in_ack,
        "durable commit is acknowledged before deferred cancel and destroy");
    CHECK(strcmp(producer.events, "PBRACKZD") == 0,
        "durable commit teardown follows ack, retirement, destruction order");

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    sink.commit_uncommitted = true;
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'C';
    producer.reenter_destroy = true;
    producer.destroy_reenter = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_INVALID,
        "destroy requested during uncommitted commit waits for abort and cleanup");
    CHECK(producer.reentrant_destroys == 1 && sink.abort_calls == 1
        && producer.ack_calls == 0 && producer.dispose_calls == 1
        && producer.cancel_calls == 1 && producer.destroy_calls == 1,
        "uncommitted destroy aborts and disposes before freeing continuation");
    CHECK(strcmp(producer.events, "PBRACTXZD") == 0,
        "uncommitted teardown follows abort, drop, retire, destruction order");

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'K';
    producer.reenter_destroy = true;
    producer.destroy_reenter = true;
    CHECK(wl_columnar_continuation_publish(continuation, &sink_spec)
        == WL_COLUMNAR_CONTINUATION_OK,
        "destroy requested from disposition waits until callback and publish finish");
    CHECK(producer.reentrant_destroys == 1 && producer.ack_calls == 1
        && producer.cancel_calls == 1 && producer.destroy_calls == 1
        && producer.published_cursor_visible_in_ack,
        "disposition observes durable state before exactly-once teardown");
    CHECK(strcmp(producer.events, "PBRACKZD") == 0,
        "disposition destroy reentry cannot free continuation mid-callback");

    memset(&producer, 0, sizeof(producer));
    memset(&sink, 0, sizeof(sink));
    continuation = make_protocol_continuation(&producer, false);
    sink_spec = make_protocol_sink(&sink, &producer);
    producer.continuation = continuation;
    producer.sink_spec = &sink_spec;
    producer.reenter_event = 'Z';
    producer.reenter_destroy = true;
    wl_columnar_continuation_cancel(continuation);
    CHECK(producer.reentrant_destroys == 1 && producer.cancel_calls == 1
        && producer.destroy_calls == 1,
        "destroy from producer cancellation is deferred through teardown");
    CHECK(strcmp(producer.events, "ZD") == 0,
        "producer cancellation completes before deferred destruction");
}

int
main(void)
{
    test_retry_and_commit_boundary();
    test_stale_and_empty_batches();
    test_reservation_and_stack_ownership();
    test_protocol_progress_and_frozen_begin_refusal();
    test_protocol_reserve_append_and_uncommitted_retries();
    test_protocol_ambiguous_cancel_stale_and_destroy();
    test_protocol_callback_reentrancy();
    if (failures != 0)
        fprintf(stderr, "%d continuation contract checks failed\n", failures);
    return failures == 0 ? 0 : 1;
}
