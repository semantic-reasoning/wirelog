/* Configured no-tail INLINE JOIN: ordinary consumption, strict policy order. */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_pipeline.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static wl_session_t *active;
static unsigned held, produced, successors, cache_steps;
static unsigned consumers, before_acks, after_acks, prepared_inline;
static uint64_t ack_generation;
static const wl_columnar_join_batch_prepare_t *actual_prepare;
static eval_entry_t saved_original;
static unsigned restored, exchanged, schema_exchanges, stream_stop;
static bool stream_stopped;
static unsigned cap_limit, cache_mode, descriptor_mode;
static void
configure(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}
static int
finish(wl_session_t *s)
{
    for (unsigned i = 0; i < 1000000; ++i) {
        int rc = wl_session_step(s);
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}
static void
host(wl_evaluation_control_t *control, unsigned event, uint64_t gen,
    uint32_t extent, uint64_t work)
{
    (void)control; (void)gen; (void)extent; (void)work;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE) ++produced;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_SUCCESSOR) ++successors;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_STRICT_CACHE) ++cache_steps;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER) ++consumers;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED) {
        ++exchanged;
        wl_columnar_eval_stack_cleanup_frame_t *frame =
            COL_SESSION(active)->cleanup_active;
        wl_columnar_eval_serial_framed_attempt_t *root = NULL;
        CHECK(!wl_columnar_eval_stack_framed_context(frame, COL_SESSION(active),
            &root));
        eval_entry_t parked;
        CHECK(!wl_columnar_eval_stack_test_stream_original(frame, &root->child,
            &parked));
        const col_rel_t *input = NULL; const wl_plan_op_t *op = NULL;
        uint64_t generation = 0; bool schema = false;
        CHECK(!wl_columnar_join_pipeline_test_stream_view(&root->child, &input,
            &generation, &schema, &op));
        CHECK(parked.rel == saved_original.rel &&
            parked.owned == saved_original.owned
            && parked.is_delta == saved_original.is_delta
            && input && input->compound_kind == WIRELOG_COMPOUND_KIND_NONE);
        if (stream_stop && !stream_stopped) {
            stream_stopped = true;
            if (stream_stop == 1) wl_evaluation_control_request_cancel(control);
            else CHECK(wl_evaluation_control_charge_attempt(control,
                    wl_evaluation_control_active_attempt_id(control),
                    UINT64_C(1000000000)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        }
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_ACK) ++before_acks;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK) {
        CHECK(gen == ++ack_generation); ++after_acks;
    }
}
static void
prepare_observe(const wl_columnar_join_batch_prepare_t *a)
{
    actual_prepare = a;
    if (a->phase < 4) return;
    const col_rel_t *sources[] = {a->left, a->right};
    const wl_columnar_source_access_reader_t *readers[] =
    {&a->left_reader, &a->right_reader};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(readers[i]->identity == (uintptr_t)readers[i]
            && readers[i]->secondary_owner == &sources[i]->descriptor_access
            && readers[i]->owner == &sources[i]->storage_owner->source_access);
        if (sources[i]->compound_kind == WIRELOG_COMPOUND_KIND_INLINE)
            prepared_inline |= 1u << i;
    }
}
struct original_foreign {
    const void *owner, *host;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    const col_rel_t *original;
};
static void *
original_foreign_thread(void *payload)
{
    const struct original_foreign *p = payload;
    CHECK(wl_columnar_join_pipeline_stream_original_admit(p->owner, p->frame,
        p->host, p->original) == EINVAL);
    return NULL;
}
static void
before_exchange(const void *owner,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const void *host_owner, const col_rel_t *input, uint64_t generation,
    bool schema)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t parked;
    CHECK(stack && stack->top &&
        !wl_columnar_eval_stack_test_stream_original(frame, owner,
        &parked) && !parked.rel);
    eval_entry_t *entry = &stack->items[stack->top - 1];
    saved_original = *entry;
    if (schema) ++schema_exchanges;
    CHECK(actual_prepare && actual_prepare->left == entry->rel
        && actual_prepare->ready && !actual_prepare->aborting);
    CHECK(!wl_columnar_join_pipeline_stream_authorized(owner, frame, host_owner,
        WL_COLUMNAR_JOIN_STREAM_ENTER, NULL, input, generation + 1, schema));
    CHECK(wl_columnar_eval_stack_stream_enter_owned(frame, owner, host_owner,
        entry->rel, generation, schema, false) == EINVAL);
    if (entry->rel->compound_kind == WIRELOG_COMPOUND_KIND_INLINE) {
        col_rel_t foreign = *entry->rel;
        CHECK(wl_columnar_join_pipeline_stream_original_admit(owner, frame,
            host_owner, &foreign) == EINVAL);
        CHECK(wl_columnar_join_pipeline_stream_original_admit(owner, frame,
            (const void *)(uintptr_t)1, entry->rel) == EINVAL);
        wl_columnar_eval_plan_numeric_attempt_t copy =
            *(const wl_columnar_eval_plan_numeric_attempt_t *)owner;
        CHECK(wl_columnar_join_pipeline_stream_original_admit(&copy, frame,
            host_owner, entry->rel) == EINVAL);
        wl_columnar_join_batch_prepare_t *p =
            (wl_columnar_join_batch_prepare_t *)actual_prepare;
        uintptr_t identity = p->left_reader.identity;
        p->left_reader.identity = 0;
        CHECK(wl_columnar_join_pipeline_stream_original_admit(owner, frame,
            host_owner, entry->rel) == EINVAL);
        p->left_reader.identity = identity;
        p->left_reader.acquire_stage = 1;
        CHECK(wl_columnar_join_pipeline_stream_original_admit(owner, frame,
            host_owner, entry->rel) == EINVAL);
        p->left_reader.acquire_stage = 0;
        uint64_t id = p->attempt_id; ++p->attempt_id;
        CHECK(wl_columnar_join_pipeline_stream_original_admit(owner, frame,
            host_owner, entry->rel) == EINVAL);
        p->attempt_id = id;
        struct original_foreign payload = {owner, host_owner, frame,
                                           entry->rel};
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, original_foreign_thread, &payload)
            && !wl_thread_join(&thread));
        CHECK(!wl_columnar_join_pipeline_stream_original_admit(owner, frame,
            host_owner, entry->rel));
    }
    CHECK(entry->rel == saved_original.rel &&
        entry->owned == saved_original.owned
        && entry->is_delta == saved_original.is_delta
        && !wl_columnar_eval_stack_test_stream_original(frame, owner,
        &parked) && !parked.rel);
}
static void
after_restore(const void *owner, wl_columnar_eval_stack_cleanup_frame_t *frame,
    const void *host_owner)
{
    (void)owner; (void)host_owner;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t parked;
    CHECK(stack && stack->top &&
        !wl_columnar_eval_stack_test_stream_original(frame, owner,
        &parked) && !parked.rel);
    eval_entry_t *entry = &stack->items[stack->top - 1];
    CHECK(entry->rel == saved_original.rel &&
        entry->owned == saved_original.owned
        && entry->is_delta == saved_original.is_delta
        && actual_prepare->left_reader.owner
        && actual_prepare->left_reader.secondary_owner ==
        &entry->rel->descriptor_access);
    ++restored;
}
static void
gather(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    (void)control; (void)progress;
    if (phase != 18) return;
    wl_col_session_t *s = COL_SESSION(active);
    wl_columnar_eval_serial_framed_attempt_t *root = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &root));
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(s->cleanup_active,
        &ctx, &root->child));
    const wl_columnar_relation_scalar_source_t *sources[] =
    {&ctx->source, &ctx->join_right};
    for (unsigned i = 0; i < 2; ++i) {
        const wl_columnar_relation_scalar_source_t *r = sources[i];
        CHECK(r->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
            && r->descriptor && r->columns ==
            (const int64_t *const *)r->descriptor->columns);
        if (r->descriptor->compound_kind == WIRELOG_COMPOUND_KIND_INLINE)
            held |= 1u << i;
    }
}
static void
verify(wl_session_t *s, bool empty)
{
    const col_rel_t *out = session_find_rel(COL_SESSION(s), "out");
    if (empty) {
        CHECK(!out || !out->nrows); return;
    }
    CHECK(out && out->nrows == 2 && out->ncols == 2);
    bool first = false, second = false;
    for (uint32_t i = 0; i < out->nrows; ++i) {
        int64_t x = col_rel_get(out, i, 0), y = col_rel_get(out, i, 1);
        if (x == 7 && y == 8) {
            CHECK(!first); first = true;
        }else {
            CHECK(x == 9 && y == 10 && !second); second = true;
        }
    }
    CHECK(first && second);
}

/* Four natural plans: left, right, both INLINE, scalar control. No constant
 * RHS predicate; the key and projection lie after the INLINE physical lanes. */
static void
run(unsigned kind, bool strict, unsigned bytes, unsigned shape, bool empty)
{
    bool li = kind == 0 || kind == 2, ri = kind == 1 || kind == 2;
    char text[768], budget[32], cap[32];
    snprintf(text, sizeof(text), ".decl a(%sk:int64,x:int64)\n"
        ".decl b(%sk:int64,y:int64)\n.decl out(x:int64,y:int64)\n"
        "out(x,y):-a(%sk,x),b(%sk,y)%s.\n",
        li ? "p:pair/2 inline," : "", ri ? "p:pair/2 inline," : "",
        li ? "pair(v,w)," : "", ri ? "pair(t,u)," : "",
        shape == 1 ? ",x>y+100" : shape >= 2 ? ",x<y" : "");
    snprintf(budget, sizeof(budget), "%u", bytes);
    configure("WIRELOG_JOIN_BATCH_BYTES", budget);
    configure("WIRELOG_JOIN_BATCH_STRICT", strict ? "1" : NULL);
    snprintf(cap, sizeof(cap), "%u", cap_limit);
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", cap_limit ? cap : NULL);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *p = &plan->strata[0].relations[0];
    uint32_t ji = UINT32_MAX;
    for (uint32_t i = 0; i < p->op_count; ++i)
        if (p->ops[i].op == WL_PLAN_OP_JOIN) {
            CHECK(ji == UINT32_MAX); ji = i;
        }
    CHECK(ji != UINT32_MAX && p->ops[ji].key_count == 1
        && !p->ops[ji].right_filter_expr.size && !p->ops[ji].materialized);
    CHECK(!strcmp(p->ops[ji].left_keys[0], li ? "col2" : "col0")
        && !strcmp(p->ops[ji].right_keys[0], ri ? "col2" : "col0"));
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *s = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &s));
    CHECK(!wl_session_create(wl_backend_columnar(), plan, 1, &raw));
    uint32_t tail = UINT32_MAX;
    bool streaming = wl_columnar_join_pipeline_preflight(p, ji, COL_SESSION(s),
            &tail) == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
    CHECK(streaming == (shape >= 2));
    if (shape == 1) CHECK(p->ops[ji + 1].op == WL_PLAN_OP_FILTER);
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    int64_t left[] = {91, 92, 1, 7, 93, 94, 2, 9, 95, 96, 3, 11};
    const int64_t right[] = {81, 82, 1, 8, 83, 84, 2, 10, 85, 86, 4, 12,
                             87, 88, 1, 6};
    int64_t ls[] = {1, 7, 2, 9, 3, 11}, rs[] = {1, 8, 2, 10, 4, 12, 1, 6};
    if (shape == 3) for (unsigned row = 0; row < 3; ++row) {
            left[row * 4 + 3] += 100; ls[row * 2 + 1] += 100;
        }
    wl_session_t *sessions[] = {s, raw};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(sessions[i], "a", li ? left : ls,
            empty ? 0 : 3, li ? 4 : 2));
        if (empty && shape == 2) {
            CHECK(!wl_session_insert(sessions[i], "a", li ? left : ls, 1,
                li ? 4 : 2));
            CHECK(!wl_session_remove(sessions[i], "a", li ? left : ls, 1,
                li ? 4 : 2));
        }
        CHECK(!wl_session_insert(sessions[i], "b", ri ? right : rs,
            shape >= 2 ? 4 : 3,
            ri ? 4 : 2));
    }
    /* Internal cache/error fixtures use genuine cache insertion and pins;
     * only the explicitly malformed descriptor changes metadata directly. */
    col_arrangement_pin_t pins[2] = {{0}};
    uint32_t key = ri ? 2u : 0u;
    for (unsigned i = 0; i < 2 && cache_mode; ++i) {
        CHECK(col_session_get_arrangement(sessions[i], "b", &key, 1));
        CHECK(!col_session_pin_arrangement(sessions[i], "b", &key, 1,
            &pins[i]));
        if (cache_mode == 2) {
            col_session_invalidate_arrangements(sessions[i], "b");
            CHECK(pins[i].active && pins[i].entry->rebuild_deferred);
        }
    }
    col_rel_t *bad = session_find_rel(COL_SESSION(s), li ? "a" : "b");
    uint32_t saved_map = 0;
    wirelog_compound_kind_t saved_kind = bad->compound_kind;
    if (descriptor_mode == 1) {
        CHECK(bad->compound_arity_len && bad->compound_arity_map);
        saved_map = bad->compound_arity_map[0];
        bad->compound_arity_map[0] = UINT32_MAX;
    } else if (descriptor_mode) {
        bad->compound_kind = descriptor_mode == 2 ? WIRELOG_COMPOUND_KIND_SIDE
            : (wirelog_compound_kind_t)99;
    }
    active = s; held = produced = successors = cache_steps = 0;
    consumers = before_acks = after_acks = prepared_inline = 0;
    ack_generation = 0;
    restored = exchanged = schema_exchanges = 0;
    stream_stopped = false;
    wl_columnar_join_pipeline_test_before_exchange = before_exchange;
    wl_columnar_join_pipeline_test_after_restore = after_restore;
    wl_columnar_join_batch_test_prepare_checkpoint = prepare_observe;
    wl_columnar_join_pipeline_test_numeric_checkpoint = host;
    wl_columnar_join_test_attempt_checkpoint = gather;
    int rc = finish(s);
    wl_columnar_join_batch_test_prepare_checkpoint = NULL;
    wl_columnar_join_test_attempt_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    wl_columnar_join_pipeline_test_before_exchange = NULL;
    wl_columnar_join_pipeline_test_after_restore = NULL;
    if (descriptor_mode == 1) bad->compound_arity_map[0] = saved_map;
    else if (descriptor_mode) bad->compound_kind = saved_kind;
    int rr = finish(raw);
    int raw_expected = strict && shape == 1 ? ENOTSUP
        : strict && !empty && cache_mode == 2 ? EBUSY
        : strict && !empty && bytes < 16 ? ENOTSUP
        : !empty && shape != 1 && shape != 3 && cap_limit &&
        cap_limit <= 2 ? EOVERFLOW : 0;
    /* The legacy raw streamed arrangement lookup loses the stale-pin errno
     * through its pointer API; controlled strict admission preserves EBUSY. */
    int controlled_policy = raw_expected;
    if (strict && !empty && shape >= 2 && cache_mode == 2)
        raw_expected = ENOMEM;
    int expected = stream_stop == 1 ? WL_EVALUATION_CONTROL_CANCELLED
        : stream_stop == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : descriptor_mode == 1 ? EINVAL
        : descriptor_mode == 2 ? ENOTSUP : descriptor_mode == 3 ? EINVAL
        : controlled_policy;
    if (rc != expected || rr != raw_expected) fprintf(stderr,
            "kind=%u strict=%u bytes=%u shape=%u empty=%u rc=%d/%d raw=%d/%d\n",
            kind, strict, bytes, shape, empty, rc, expected, rr, raw_expected);
    if (rc != expected) fprintf(stderr,
            "prepared_inline=%u produce=%u consumer=%u ack=%u phase=%u\n",
            prepared_inline, produced, consumers, after_acks,
            wl_columnar_session_test_native_error_phase(COL_SESSION(s)));
    CHECK(rc == expected && rr == raw_expected);
    verify(s, expected || empty || shape == 1 || shape == 3);
    verify(raw,
        raw_expected || empty || shape == 1 || shape == 3 || shape == 3);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.result == expected && report.consumed &&
        !report.cleanup_status);
    if (!expected && !empty && !shape) {
        CHECK(kind == 3 && bytes >= 16 ? produced > 0 : !produced);
        CHECK(held == (kind == 3 ? 0u : kind == 2 ? 3u : kind == 0 ? 1u : 2u));
    }
    if (shape >= 2 && bytes >= 16 && !expected && !empty) {
        if (!produced || !consumers || !before_acks ||
            before_acks != after_acks) fprintf(stderr,
                "shape=%u kind=%u cap=%u produce=%u consume=%u ack=%u/%u\n",
                shape, kind, cap_limit, produced, consumers, before_acks,
                after_acks);
        CHECK(produced && consumers && before_acks &&
            before_acks == after_acks);
        CHECK(prepared_inline ==
            (kind == 3 ? 0u : kind == 2 ? 3u : kind == 0 ? 1u : 2u));
    }
    if (shape == 2 && empty) CHECK(schema_exchanges && restored == exchanged);
    if (stream_stop) {
        CHECK(stream_stopped && exchanged == 1 && restored == exchanged);
    }
    if (strict && shape < 2 && !empty &&
        !descriptor_mode) CHECK(successors > 0);
    if (strict && cache_mode && !shape && !empty) CHECK(cache_steps > 0);
    if (expected) CHECK(COL_SESSION(s)->pending_input_change);
    for (unsigned i = 0; i < 2; ++i) col_arrangement_pin_release(&pins[i]);
    if (descriptor_mode) CHECK(!successors && !produced && !held);
    if (expected && !shape && !empty) {
        /* Internal configured knobs have no live public setter. The real
         * stopped session/input is retried after restoring these policies. */
        COL_SESSION(s)->join_output_limit = 3;
        COL_SESSION(s)->join_batch_bytes = 256;
        CHECK(!wl_evaluation_control_reset(control) && !finish(s));
        verify(s, false);
    }
    if (stream_stop) {
        CHECK(!wl_evaluation_control_reset(control) && !finish(s));
        verify(s, false);
    }
    CHECK(!wl_session_try_destroy(&s) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
}
/* A published canonical RHS with a real stale pin must not impose EBUSY
 * on the next same-name transaction-private RHS. Generated two-head plan. */
static void
private_right(bool streamed)
{
    char text[512];
    snprintf(text, sizeof(text), ".decl a(p:pair/2 inline,k:int64,x:int64)\n"
        ".decl src(k:int64,y:int64)\n.decl mid(k:int64,y:int64)\n"
        ".decl out(x:int64,y:int64)\nmid(k,y):-src(k,y).\n"
        "out(x,y):-a(pair(v,w),k,x),mid(k,y)%s.\n", streamed ? ",x<y" : "");
    configure("WIRELOG_JOIN_BATCH_BYTES", streamed ? "64" : "256");
    configure("WIRELOG_JOIN_BATCH_STRICT", "1");
    configure("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    wl_plan_t *plan = NULL;
    CHECK(program && !wl_plan_from_program(program, &plan));
    bool found = false;
    for (uint32_t si = 0; si < plan->stratum_count; ++si)
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri) {
            const wl_plan_relation_t *r = &plan->strata[si].relations[ri];
            for (uint32_t oi = 0; oi < r->op_count; ++oi)
                if (r->ops[oi].op == WL_PLAN_OP_JOIN &&
                    !strcmp(r->ops[oi].right_relation, "mid")) found = true;
        }
    CHECK(found);
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(1000000000), &control));
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control;
    wl_session_t *s = NULL, *raw = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &s) && !wl_session_create(wl_backend_columnar(), plan, 1,
        &raw));
    configure("WIRELOG_JOIN_BATCH_BYTES", NULL);
    configure("WIRELOG_JOIN_BATCH_STRICT", NULL);
    wl_session_t *ss[] = {s, raw};
    for (unsigned i = 0; i < 2; ++i) {
        CHECK(!wl_session_insert(ss[i], "a",
            (int64_t[]){91, 92, 1, 7, 93, 94, 2, 9}, 2, 4));
        CHECK(!wl_session_insert(ss[i], "src", (int64_t[]){1, 8}, 1, 2));
        CHECK(!finish(ss[i]));
    }
    col_arrangement_pin_t pin = {0};
    uint32_t key = 0;
    CHECK(!col_session_pin_arrangement(s, "mid", &key, 1, &pin));
    col_session_invalidate_arrangements(s, "mid");
    CHECK(pin.active && pin.entry->rebuild_deferred);
    CHECK(!wl_session_insert(s, "src", (int64_t[]){2, 10}, 1, 2));
    /* Raw reference has no equivalent transaction-private publication:
     * inject the stale pin only into the controlled session. */
    CHECK(!wl_session_insert(raw, "src", (int64_t[]){2, 10}, 1, 2));
    active = s; held = produced = successors = cache_steps = 0;
    consumers = before_acks = after_acks = 0; ack_generation = 0;
    restored = exchanged = schema_exchanges = 0;
    stream_stopped = false;
    wl_columnar_join_pipeline_test_before_exchange = before_exchange;
    wl_columnar_join_pipeline_test_after_restore = after_restore;
    wl_columnar_join_batch_test_prepare_checkpoint = prepare_observe;
    wl_columnar_join_test_attempt_checkpoint = gather;
    wl_columnar_join_pipeline_test_numeric_checkpoint = host;
    CHECK(!finish(s));
    wl_columnar_join_test_attempt_checkpoint = NULL;
    wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
    wl_columnar_join_pipeline_test_before_exchange = NULL;
    wl_columnar_join_pipeline_test_after_restore = NULL;
    wl_columnar_join_batch_test_prepare_checkpoint = NULL;
    CHECK(streamed ? produced && consumers && before_acks == after_acks
        : held == 1 && !produced && successors);
    CHECK(!cache_steps);
    CHECK(!finish(raw));
    verify(s, false); verify(raw, false);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && !report.result && report.consumed && !report.cleanup_status);
    CHECK(pin.active && pin.entry->rebuild_deferred);
    col_arrangement_pin_release(&pin);
    CHECK(!wl_session_try_destroy(&s) && !wl_session_try_destroy(&raw));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
}
int
main(void)
{
    for (unsigned kind = 0; kind < 4; ++kind)
        for (unsigned strict = 0; strict < 2; ++strict) {
            run(kind, strict, 256, 0, false);
            run(kind, strict, 1, 0, false);
            run(kind, strict, 1, 0, true);
            run(kind, strict, 256, 1, false);
            run(kind, strict, 256, 2, false);
        }
    for (unsigned kind = 0; kind < 3; ++kind) {
        for (unsigned strict = 0; strict < 2; ++strict)
            for (cap_limit = 1; cap_limit <= 3; ++cap_limit)
                run(kind, strict, 256, 0, false);
        cap_limit = 0;
        for (cache_mode = 1; cache_mode <= 2; ++cache_mode) {
            run(kind, true, 256, 0, false);
            run(kind, true, 1, 0, false); /* stale pin precedes row-fit */
        }
        cache_mode = 0;
        for (descriptor_mode = 1; descriptor_mode <= 3; ++descriptor_mode) {
            run(kind, true, 1, 0, false);
            run(kind, true, 256, 1, false); /* source before successor */
        }
        descriptor_mode = 0;
    }
    for (unsigned kind = 0; kind < 3; ++kind) {
        run(kind, true, 256, 2, true);
        for (stream_stop = 1; stream_stop <= 2; ++stream_stop)
            run(kind, false, 256, 2, false);
        stream_stop = 0;
    }
    for (unsigned kind = 0; kind < 3; ++kind) {
        for (unsigned strict = 0; strict < 2; ++strict) {
            for (cap_limit = 1; cap_limit <= 3; ++cap_limit)
                run(kind, strict, 64, 2, false);
            cap_limit = 1;
            run(kind, strict, 64, 3, false); /* all real matches rejected */
            cap_limit = 0;
            run(kind, strict, 1, 2, false);
        }
        for (cache_mode = 1; cache_mode <= 2; ++cache_mode) {
            run(kind, true, 64, 2, false);
            run(kind, true, 1, 2, false);
        }
        cache_mode = 0;
    }
    private_right(false);
    private_right(true);
    return 0;
}
