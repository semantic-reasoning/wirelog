/* Initial scalar CONSOLIDATE: real host turns, independent raw twins and
 * fault/authority tests. Fixture setup/assertions/destruction are external. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog-parser.h"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #c); exit(1); \
                      } } while (0)

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *credit,
    unsigned phase)
{
    (void)credit; (void)phase;
}

typedef struct {
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_plan_op_t op;
    wl_evaluation_control_t *control, *funding;
    int owner, funding_owner;
    uint64_t id, funding_id, turns, witnesses;
    wl_columnar_filter_status_t status;
    bool started, acknowledge, done, parked;
    int protocol;
} host_t;

static wl_evaluation_control_t *
control_new(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(limit, &control));
    CHECK(!wl_evaluation_control_attach(control, owner));
    CHECK(!wl_evaluation_control_begin(control, owner));
    return control;
}

/* Exactly one fresh, distinct funding grant per real start/step/ACK/retry.
* No assertions, frame destruction, report, or fixture scan inside this action.
* A refusal parks the original handle before entering any engine action. */
static void
host_action(host_t *host)
{
    if (wl_evaluation_control_active_attempt_id(host->funding) !=
        host->funding_id){
        host->protocol = EINVAL; host->parked = true; return;
    }
    int rc = wl_evaluation_control_charge(host->funding, 384);
    if (rc) {
        host->protocol = rc; host->parked = true; return;
    }
    if (host->acknowledge) {
        host->protocol = wl_columnar_filter_op_attempt_ack(host->frame);
        host->done = !host->protocol;
        host->parked = host->protocol != 0;
        return;
    }
    rc = host->started
        ? wl_columnar_filter_op_attempt_step(host->frame, &host->status)
        : wl_columnar_filter_consolidate_attempt_start(&host->op, host->frame,
            host->session, host->control, host->id, &host->status);
    host->started = true;
    host->protocol = rc;
    if (rc) {
        host->parked = true; return;
    }
    if (host->status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE)
        host->acknowledge = true;
    else if (host->status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED)
        host->parked = true;
}

/* Two-job FIFO: each action returns through an independent witness before
 * another action is dispatched. The external scheduler owns this loop. */
static void
host_run(host_t *host)
{
    enum { ACTION, WITNESS } queue[2] = { ACTION, WITNESS };
    unsigned head = 0;
    for (unsigned turn = 0; turn < 512
        && ((!host->done && !host->parked) || head == 1);
        ++turn){
        unsigned job = queue[head]; head ^= 1;
        if (job == ACTION) {
            CHECK(host->turns == host->witnesses);
            ++host->turns; host_action(host);
        } else ++host->witnesses;
    }
    CHECK(host->done || host->parked);
}

static host_t *
host_new(col_rel_t *input, bool owned, uint64_t limit, uint64_t funding_limit)
{
    host_t *host = calloc(1, sizeof(*host)); CHECK(host);
    host->session = calloc(1, sizeof(*host->session)); CHECK(host->session);
    wl_mem_ledger_init(&host->session->mem_ledger, 0);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(host->session, &host->frame));
    CHECK(!eval_stack_push(wl_columnar_eval_stack_cleanup_stack(host->frame),
        input, owned));
    host->op.op = WL_PLAN_OP_CONSOLIDATE;
    host->control = control_new(limit, &host->owner);
    host->funding = control_new(funding_limit, &host->funding_owner);
    host->id = wl_evaluation_control_active_attempt_id(host->control);
    host->funding_id = wl_evaluation_control_active_attempt_id(host->funding);
    return host;
}

static wl_evaluation_control_report_t
host_finish(host_t *host)
{
    CHECK(host->done && !host->protocol);
    int result = wl_evaluation_control_active_attempt_id(host->control) ==
        host->id
        ? wl_evaluation_control_finish(host->control, &host->owner,
            host->status.execution_result, host->status.cleanup_result) : 0;
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(host->control, &report));
    CHECK(report.attempt_id == host->id && report.result == result);
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&host->frame));
    CHECK(!host->frame && !host->session->cleanup_pending_count);
    CHECK(!wl_evaluation_control_active_attempt_id(host->control));
    CHECK(!wl_evaluation_control_detach(host->control, &host->owner));
    wl_evaluation_control_release(host->control);
    (void)wl_evaluation_control_finish(host->funding, &host->funding_owner, 0,
        0);
    CHECK(!wl_evaluation_control_active_attempt_id(host->funding));
    CHECK(!wl_evaluation_control_detach(host->funding, &host->funding_owner));
    wl_evaluation_control_release(host->funding);
    wl_columnar_memory_governor_ref_release(host->session->memory_governor);
    free(host->session); free(host);
    return report;
}

static col_rel_t *
source_new(uint32_t width, uint32_t rows, unsigned pattern, bool typed)
{
    col_rel_t *input = NULL;
    CHECK(!col_rel_alloc(&input, "consolidate original"));
    char **names = calloc(width ? width : 1, sizeof(*names)); CHECK(names);
    for (uint32_t c = 0; c < width; ++c) {
        names[c] = malloc(64); CHECK(names[c]);
        snprintf(names[c], 64, "column_%u_named_input", c);
    }
    CHECK(!col_rel_set_schema(input, width, (const char *const *)names));
    for (uint32_t c = 0; c < width; ++c) free(names[c]);
    free(names);
    if (typed && width) {
        wirelog_column_type_t *types = malloc(width * sizeof(*types));
        CHECK(types);
        for (uint32_t c = 0; c < width;
            ++c) types[c] = (wirelog_column_type_t)(c % 7);
        types[width - 1] = WIRELOG_TYPE_FLOAT;
        CHECK(!col_rel_set_column_types(input, types, width)); free(types);
    }
    int64_t *values = calloc(width ? width : 1, sizeof(*values)); CHECK(values);
    for (uint32_t row = 0; row < rows; ++row) {
        int64_t key = pattern == 0 ? 0 : pattern == 1 ? rows - row
            : pattern == 2 ? row : (int64_t)(row % 7) - 3;
        for (uint32_t c = 0; c < width; ++c) {
            values[c] = c + 1 == width ? key : pattern == 4 ? 0 : key + c;
            if (typed && input->column_types[c] == WIRELOG_TYPE_FLOAT)
                values[c] =
                    wl_columnar_float_to_bits(values[c] ? (double)values[c]
                        : row % 2 ? -0.0 : 0.0);
        }
        CHECK(!col_rel_append_row(input, values));
    }
    free(values);
    input->has_graph_column = width != 0;
    input->graph_col_idx = width ? width - 1 : 0;
    input->declared_ncols = width;
    return input;
}

static void
relations_equal(const col_rel_t *actual, const col_rel_t *raw)
{
    CHECK(actual->ncols == raw->ncols && actual->nrows == raw->nrows);
    CHECK(actual->has_graph_column == raw->has_graph_column);
    CHECK(actual->graph_col_idx == raw->graph_col_idx);
    CHECK(actual->declared_ncols == raw->declared_ncols);
    CHECK(actual->sorted_nrows == actual->nrows);
    CHECK(actual->run_count == (actual->nrows != 0));
    if (actual->nrows) CHECK(actual->run_ends[0] == actual->nrows);
    for (uint32_t c = 0; c < actual->ncols; ++c) {
        CHECK(!strcmp(actual->col_names[c], raw->col_names[c]));
        CHECK((actual->column_types != NULL) == (raw->column_types != NULL));
        if (actual->column_types) CHECK(actual->column_types[c] ==
                raw->column_types[c]);
        for (uint32_t r = 0; r < actual->nrows; ++r) {
            if (actual->column_types &&
                actual->column_types[c] == WIRELOG_TYPE_FLOAT) {
                CHECK(wl_columnar_float_from_bits(actual->columns[c][r])
                    == wl_columnar_float_from_bits(raw->columns[c][r]));
            } else CHECK(actual->columns[c][r] == raw->columns[c][r]);
        }
    }
}

static uint64_t
parity(uint32_t width, uint32_t rows, unsigned pattern, bool typed)
{
    col_rel_t *input = source_new(width, rows, pattern, typed);
    col_rel_t *twin = source_new(width, rows, pattern, typed);
    eval_stack_t raw = {0}; CHECK(!eval_stack_push(&raw, twin, true));
    wl_col_session_t *session = calloc(1, sizeof(*session)); CHECK(session);
    wl_mem_ledger_init(&session->mem_ledger, 0);
    CHECK(!col_op_consolidate(&raw, session));
    host_t *host = host_new(input, false, 0, 0);
    host_run(host); CHECK(host->done && !host->status.semantic_result);
    CHECK(host->status.committed && host->turns > 2 && host->witnesses > 1);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(host->frame);
    CHECK(stack->top == 1 && stack->items[0].owned);
    CHECK(stack->items[0].rel != input && !stack->items[0].is_delta);
    relations_equal(stack->items[0].rel, raw.items[0].rel);
    CHECK(input->nrows == rows); /* Borrowed original remains valid. */
    uint64_t consumed = host_finish(host).consumed;
    col_rel_destroy(input); CHECK(!eval_stack_drain(&raw)); free(session);
    return consumed;
}

static unsigned target_phase;
static uint64_t target_progress;
static bool restart_hook, hook_hit;
static int *hook_owner;
static uint64_t successor_id;
static void
checkpoint(wl_evaluation_control_t *control, unsigned phase, uint64_t progress)
{
    if (hook_hit || phase != target_phase ||
        progress != target_progress) return;
    hook_hit = true;
    if (restart_hook) {
        (void)wl_evaluation_control_finish(control, hook_owner, 0, 0);
        CHECK(!wl_evaluation_control_begin(control, hook_owner));
        successor_id = wl_evaluation_control_active_attempt_id(control);
    } else wl_evaluation_control_request_cancel(control);
}

static void
interruptions(void)
{
    static const unsigned phases[] = {1, 2, 3, 4, 5, 7, 10, 11, 200, 201,
                                      202, 203, 204, 205, 206, 207};
    for (unsigned mode = 0; mode < 2; ++mode)
        for (unsigned k = 0; k < sizeof(phases) / sizeof(phases[0]); ++k) {
            col_rel_t *input = source_new(17, 19, 4, true);
            host_t *host = host_new(input, false, 0, 0);
            uint64_t generation = input->storage_generation;
            target_phase = phases[k]; target_progress = 0;
            if (target_phase == 4 || target_phase == 5 || target_phase == 7)
                target_progress = 17;
            if (target_phase == 206) target_progress = 1;
            hook_hit = false; restart_hook = mode != 0;
            hook_owner = &host->owner;
            wl_columnar_filter_test_attempt_checkpoint = checkpoint;
            host_run(host);
            wl_columnar_filter_test_attempt_checkpoint = NULL;
            CHECK(hook_hit && host->done && host->status.semantic_result);
            CHECK(!host->status.committed &&
                input->storage_generation == generation);
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(host->frame);
            CHECK(stack->top == 1 && stack->items[0].rel == input);
            if (mode) {
                CHECK(!wl_evaluation_control_finish(host->control, &host->owner,
                    0, 0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(host->control, &report));
                CHECK(report.attempt_id == successor_id &&
                    !report.consumed);
                host->id = successor_id;
            }
            (void)host_finish(host); col_rel_destroy(input);
        }
}

static wl_evaluation_control_t *watched_control;
static uint64_t watched_id;
static unsigned child_events, child_stop, successor_callbacks;
static void
child_checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    (void)work;
    if (control != watched_control) return;
    if (wl_evaluation_control_active_attempt_id(control) != watched_id) {
        ++successor_callbacks; return;
    }
    ++child_events;
    if (child_events == child_stop) checkpoint(control, target_phase,
            target_progress);
}

static void
child_interruptions(void)
{
    unsigned reached = 0;
    for (unsigned mode = 0; mode < 2; ++mode)
        for (unsigned event = 0; event <= reached; ++event) {
            if (!event && mode) continue;
            col_rel_t *input = source_new(1, 3, 3, true);
            host_t *host = host_new(input, false, 0, 0);
            watched_control = host->control; watched_id = host->id;
            child_events = 0; child_stop = event; successor_callbacks = 0;
            target_phase = 0; target_progress = 0; hook_hit = false;
            restart_hook = mode != 0; hook_owner = &host->owner;
            wl_columnar_relation_test_work_checkpoint = child_checkpoint;
            host_run(host);
            wl_columnar_relation_test_work_checkpoint = NULL;
            watched_control = NULL;
            CHECK(host->done && !successor_callbacks);
            if (!event) {
                reached = child_events; CHECK(reached);
            }else {
                CHECK(hook_hit && !host->status.committed &&
                    host->status.semantic_result);
                CHECK(wl_columnar_eval_stack_cleanup_stack(
                        host->frame)->items[0].rel == input);
                if (mode) {
                    CHECK(!wl_evaluation_control_finish(host->control,
                        &host->owner, 0, 0));
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(host->control,
                        &report));
                    CHECK(report.attempt_id == successor_id &&
                        !report.consumed);
                    host->id = successor_id;
                }
            }
            (void)host_finish(host); col_rel_destroy(input);
        }
}

static void
negative_cases(void)
{
    for (unsigned mode = 0; mode < 6; ++mode)
        for (uint32_t rows = 0; rows <= 3; rows += rows ? 2 : 1) {
            col_rel_t *input = source_new(1, rows, 3, true);
            host_t *host = host_new(input, false, 0, 0);
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(host->frame);
            if (mode == 0) stack->items[0].is_delta = true;
            if (mode == 1) CHECK(!col_rel_enable_timestamps(input));
            if (mode == 2) host->session->current_iteration = 1;
            if (mode == 3) host->session->diff_operators_active = true;
            if (mode == 4) host->session->num_workers = 2;
            if (mode == 5) host->op.op = WL_PLAN_OP_FILTER;
            eval_entry_t before = stack->items[0];
            host_run(host);
            if (mode == 1) {
                CHECK(host->done && !host->status.semantic_result
                    && host->status.committed);
                CHECK(stack->items[0].rel->timestamps);
                CHECK(stack->items[0].rel->nrows == rows);
            } else {
                CHECK(host->done && host->status.semantic_result == ENOTSUP);
                CHECK(!memcmp(&before, &stack->items[0], sizeof(before)));
            }
            (void)host_finish(host); col_rel_destroy(input);
        }
    for (unsigned special = 0; special < 3; ++special) {
        col_rel_t *input = source_new(1, 1, 0, true);
        input->columns[0][0] = wl_columnar_float_to_bits(special == 0 ? NAN
                : special == 1 ? INFINITY : -INFINITY);
        host_t *host = host_new(input, false, 0, 0); host_run(host);
        CHECK(host->done && host->status.semantic_result == EINVAL);
        (void)host_finish(host); col_rel_destroy(input);
    }
}

static void
budgets_and_allocation(void)
{
    /* Independent first admission literal, no telemetry-derived oracle. */
    static const uint64_t limits[] = {1, 223, 224, 225, 415, 416, 608, 1024,
                                      8192, 16384};
    for (unsigned k = 0; k < sizeof(limits) / sizeof(limits[0]); ++k) {
        col_rel_t *input = source_new(17, 19, 4, true);
        host_t *host = host_new(input, false, limits[k], 0); host_run(host);
        CHECK(host->done && host->status.semantic_result &&
            !host->status.committed);
        wl_evaluation_control_report_t report = host_finish(host);
        CHECK(report.consumed <= limits[k]);
        if (limits[k] < 224) CHECK(!report.consumed);
        col_rel_destroy(input);
    }
    col_rel_t *input = source_new(17, 19, 4, true);
    host_t *host = host_new(input, false, 0, 383); host_run(host);
    CHECK(host->parked && !host->started);
    CHECK(wl_evaluation_control_active_attempt_id(host->control) == host->id);
    /* Fresh funding attempt, same original handle, then resume externally. */
    (void)wl_evaluation_control_finish(host->funding, &host->funding_owner, 0,
        0);
    CHECK(!wl_evaluation_control_active_attempt_id(host->funding));
    CHECK(!wl_evaluation_control_detach(host->funding, &host->funding_owner));
    wl_evaluation_control_release(host->funding);
    host->funding = control_new(0, &host->funding_owner);
    host->funding_id = wl_evaluation_control_active_attempt_id(host->funding);
    host->parked = false; host->turns = host->witnesses = 0;
    host_run(host); CHECK(host->done && !host->status.semantic_result);
    (void)host_finish(host); col_rel_destroy(input);
    input = source_new(17, 19, 4, true); host = host_new(input, false, 0, 0);
    wl_columnar_filter_test_attempt_fail_allocation = 0;
    host_run(host); wl_columnar_filter_test_attempt_fail_allocation = -1;
    CHECK(host->done && host->status.semantic_result == ENOMEM);
    (void)host_finish(host); col_rel_destroy(input);
}

typedef struct {
    uint64_t *prefix;
    size_t count, capacity;
    uint64_t total;
} oracle_literal_t;

static void
oracle_literal_add(oracle_literal_t *literal,
    uint64_t fee)
{
    CHECK(fee && fee <= 256 && literal->count < literal->capacity);
    literal->total += fee;
    literal->prefix[literal->count++] = literal->total;
}

static void
oracle_literal_repeat(
    oracle_literal_t *literal,
    uint64_t fee, uint64_t count)
{
    for (uint64_t i = 0; i < count; ++i)
        oracle_literal_add(literal, fee);
}

static unsigned
oracle_digits(unsigned value)
{
    unsigned digits = 1;
    while (value >= 10) {
        value /= 10; ++digits;
    }
    return digits;
}

static void
oracle_literal_reserve(
    oracle_literal_t *literal)
{
    static const uint64_t fees[] = {74, 54, 84, 29};
    for (size_t i = 0; i < sizeof(fees) / sizeof(fees[0]); ++i)
        oracle_literal_add(literal, fees[i]);
}

static void
oracle_literal_commit(
    oracle_literal_t *literal)
{
    static const uint64_t fees[] = {74, 54, 29};
    for (size_t i = 0; i < sizeof(fees) / sizeof(fees[0]); ++i)
        oracle_literal_add(literal, fees[i]);
}

/* Independent scalar original-single schedule, one uncontended nonce round. */
static void
oracle_literal_single(
    oracle_literal_t *literal)
{
    static const uint64_t fees[] = {94, 19, 39, 39, 45, 52, 45, 21, 144};
    for (size_t i = 0; i < sizeof(fees) / sizeof(fees[0]); ++i)
        oracle_literal_add(literal, fees[i]);
}

static void
oracle_literal_constructor(
    oracle_literal_t *literal,
    uint32_t width, bool typed, bool timestamps, bool long_names)
{
#define L(v) oracle_literal_add(literal, (v))
#define R(v, n) oracle_literal_repeat(literal, (v), (n))
    L(128); L(128); L(128); L(192);
    R(96, 8); /* "$filter", including its terminator. */
    R(96, 6); L(96);
    if (typed && width) R(96, 2);
    R(96, 4); L(128);
    for (uint32_t c = 0; c < width; ++c) {
        uint64_t length = long_names
            ? strlen("long_column__with_several_eight_byte_chunks") +
            oracle_digits(c) + 1u
            : strlen("column__named_input") + oracle_digits(c) + 1u;
        L(96); R(96, length); R(96, 3);
    }
    L(96); L(160); oracle_literal_reserve(literal);
    L(112); R(224, (sizeof(col_rel_t) + 7u) / 8u);
    R(96, 5); L(112); L(224); L(80); L(112); L(96);
    L(160); oracle_literal_reserve(literal);
    L(160);
    if (width || timestamps) oracle_literal_reserve(literal);
    if (width) {
        L(112); R(192, (width + 7u) / 8u); R(112, width);
        L(112); /* Governor-backed row scratch. */
        L(112); R(192, (width + 7u) / 8u);
        for (uint32_t c = 0; c < width; ++c) {
            uint64_t length = long_names
                ? strlen("long_column__with_several_eight_byte_chunks") +
                oracle_digits(c) + 1u
                : strlen("column__named_input") + oracle_digits(c) + 1u;
            L(96); R(96, length); L(112); R(224, (length + 7u) / 8u);
        }
    }
    if (typed && width) {
        L(112); R(96, width);
    }
    if (timestamps) {
        L(112); R(224, (64u * sizeof(col_delta_timestamp_t) + 7u) / 8u);
    }
    L(96); L(112); L(224);
    if (width) {
        L(112); R(192, (width + 7u) / 8u);
        for (uint32_t c = 0; c < width; ++c) {
            uint64_t length = long_names
                ? strlen("long_column__with_several_eight_byte_chunks") +
                oracle_digits(c) + 1u
                : strlen("column__named_input") + oracle_digits(c) + 1u;
            L(128); L(112);
            if (typed) L(96);
            L(224); L(96); R(96, length); L(112); R(224, (length + 7u) / 8u);
        }
    }
    for (unsigned token = 0; token < 3; ++token) {
        L(128);
        if (token < 2 || width || timestamps)
            oracle_literal_commit(literal);
    }
    L(220); L(128);
#undef R
#undef L
}

static void
oracle_literal_selected(
    oracle_literal_t *literal,
    uint32_t width, uint32_t selected_before, uint32_t capacity,
    bool timestamps,
    bool renewal)
{
#define L(v) oracle_literal_add(literal, (v))
#define R(v, n) oracle_literal_repeat(literal, (v), (n))
    L(192); L(192); L(256);
    if (timestamps) L(128);
    L(128); L(220); L(80);
    if (width > 16) {
        L(80); oracle_literal_reserve(literal);
        oracle_literal_commit(literal); L(32);
    }
    R(36, width);
    if (renewal) {
        L(128); oracle_literal_single(literal); L(220);
    }
    L(63); L(103); R(36, (uint64_t)width * 2u);
    if (selected_before < capacity) {
        R(36, width);
        if (timestamps) R(183, (sizeof(col_delta_timestamp_t) + 7u) / 8u);
        L(48);
    } else {
        R(83, 5); L(76);
        if (width ||
            timestamps) oracle_literal_reserve(literal);
        if (width) {
            L(32); /* Pointer table allocation; lanes are initialized individually. */
            for (uint32_t c = 0; c < width; ++c) {
                L(32); R(183, selected_before); L(36);
            }
        }
        if (timestamps) {
            L(32);
            R(183,
                (uint64_t)capacity *
                ((sizeof(col_delta_timestamp_t) + 7u) / 8u));
            R(183, (sizeof(col_delta_timestamp_t) + 7u) / 8u);
        }
        L(69); L(151);
        if (width || timestamps) {
            L(26); L(228);
        }
        L(48);
    }
    L(192); L(256);
    if (timestamps) L(128);
#undef R
#undef L
}

/* Independent ordered schedule for all-equal integer tuples. Merge comparison
 * geometry follows run lengths, never production enums/hooks/measurements. */
static oracle_literal_t
literal_schedule(uint32_t width, uint32_t rows)
{
    oracle_literal_t literal = {0};
    literal.capacity = 65536;
    literal.prefix = malloc(literal.capacity * sizeof(*literal.prefix));
    CHECK(literal.prefix);
#define L(v) oracle_literal_add(&literal, (v))
#define R(v, n) oracle_literal_repeat(&literal, (v), (n))
    L(224); R(192, 10); L(192); R(192, 2); L(192);
    L(160); L(128); L(256); L(192); L(192);
    L(192); L(192); L(256); L(192); L(256);
    L(96); L(144); L(224); L(96); L(144); L(224);
    L(192); R(224, (width + 7u) / 8u);
    L(192); L(128); /* Shape caller/child precede the finite-value scan. */
    for (uint32_t r = 0; r < rows; ++r) {
        L(128); R(128, width);
    }
    L(192); L(192); L(192); L(192);
    oracle_literal_constructor(&literal, width, false, false, false);
    R(192, 2); L(192); L(192); oracle_literal_reserve(&literal);
    L(192); L(192); oracle_literal_commit(&literal);
    R(192, 36); L(192); L(192); L(170); oracle_literal_single(&literal);
    R(128, rows);
    for (uint64_t span = 1; span < rows; span *= 2) {
        L(128);
        for (uint64_t base = 0; base < rows; base += 2 * span) {
            L(128);
            uint64_t left = rows - base < span ? rows - base : span;
            uint64_t right = rows - base - left <
                span ? rows - base - left : span;
            uint64_t comparisons = right ? left : 0;
            for (uint64_t r = 0; r < left + right; ++r) {
                L(128);
                if (r < comparisons) {
                    L(128); R(128, width);
                }
                L(128);
            }
        }
    }
    for (uint32_t r = 0; r < rows; ++r) {
        L(128);
        if (r) {
            L(128); R(128, width);
        }
        L(128);
        if (!r) {
            L(128); R(224, (width + 7u) / 8u); L(192);
            oracle_literal_selected(&literal, width, 0, 64, false, false);
        }
    }
    L(128); L(128); L(192); L(128); /* caller and sorted/unique seal */
    L(192); L(128); L(128); L(128); /* original commit scan/cutoff */
#undef R
#undef L
    return literal;
}

static void
governor_install(host_t *host, uint64_t budget)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget, .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    host->session->memory_governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(host->session->memory_governor);
}

static void
literal_budgets(void)
{
    static const uint32_t dimensions[][2] = {{0, 0}, {0, 3}, {1, 3}, {17, 3}};
    for (unsigned d = 0; d < sizeof(dimensions) / sizeof(dimensions[0]); ++d) {
        uint32_t width = dimensions[d][0], rows = dimensions[d][1];
        oracle_literal_t oracle = literal_schedule(width, rows);
        col_rel_t *input = source_new(width, rows, 0, false);
        host_t *host = host_new(input, false, 0, 0);
        governor_install(host, 1048576);
        host_run(host); CHECK(host->done && !host->status.semantic_result);
        wl_evaluation_control_report_t report = host_finish(host);
        if (report.consumed != oracle.total)
            fprintf(stderr, "literal width%u rows%u expected%llu actual%llu\n",
                width, rows,
                (unsigned long long)oracle.total,
                (unsigned long long)report.consumed);
        CHECK(report.consumed == oracle.total); col_rel_destroy(input);
        for (unsigned i = 0; i < oracle.count; ++i) {
            if (d && i % 17 && i + 1 != oracle.count) continue;
            for (unsigned minus = 0; minus < 2; ++minus) {
                uint64_t limit = oracle.prefix[i] - minus;
                input = source_new(width, rows, 0, false);
                host = host_new(input, false, limit, 0);
                governor_install(host, 1048576);
                host_run(host); CHECK(host->done);
                report = host_finish(host);
                uint64_t expected = minus ? i ? oracle.prefix[i -
                        1] : 0 : oracle.prefix[i];
                CHECK(report.consumed == expected);
                CHECK((!report.result) == (limit == oracle.total));
                col_rel_destroy(input);
            }
        }
        free(oracle.prefix);
    }
}

static void
allocation_ordinals(void)
{
    unsigned counts[2] = {0};
    for (unsigned family = 0; family < 2; ++family) {
        col_rel_t *input = source_new(17, 133, 2, true);
        host_t *host = host_new(input, false, 0, 0);
        if (!family) wl_columnar_relation_test_constructor_fail_allocation =
                UINT32_MAX;
        else wl_columnar_relation_test_growth_fail_allocation = UINT32_MAX;
        host_run(host); CHECK(host->done && !host->status.semantic_result);
        counts[family] = UINT32_MAX - (family
            ? wl_columnar_relation_test_growth_fail_allocation
            : wl_columnar_relation_test_constructor_fail_allocation);
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_relation_test_growth_fail_allocation = 0;
        (void)host_finish(host); col_rel_destroy(input);
        /* Discovery is an allocation-coverage count, never a work oracle. */
        CHECK(counts[family]);
        for (unsigned ordinal = 1; ordinal <= counts[family]; ++ordinal) {
            input = source_new(17, 133, 2, true);
            host = host_new(input, false, 0, 0);
            if (!family) wl_columnar_relation_test_constructor_fail_allocation =
                    ordinal;
            else wl_columnar_relation_test_growth_fail_allocation = ordinal;
            host_run(host);
            wl_columnar_relation_test_constructor_fail_allocation = 0;
            wl_columnar_relation_test_growth_fail_allocation = 0;
            CHECK(host->done && host->status.semantic_result == ENOMEM);
            CHECK(!host->status.committed);
            CHECK(wl_columnar_eval_stack_cleanup_stack(
                    host->frame)->items[0].rel == input);
            (void)host_finish(host); col_rel_destroy(input);
        }
    }
}

static void
retained_input(void)
{
    col_rel_t *input = source_new(17, 19, 4, true);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!wl_columnar_source_access_reader_acquire(&input->source_access,
        &reader));
    host_t *host = host_new(input, true, 0, 0);
    host_run(host);
    CHECK(host->parked &&
        host->status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    CHECK(host->status.committed && !host->status.execution_result);
    col_rel_t *published =
        wl_columnar_eval_stack_cleanup_stack(host->frame)->items[0].rel;
    uint64_t generation = published->storage_generation;
    for (unsigned retry = 0; retry < 2; ++retry) {
        host->parked = false; host->turns = host->witnesses = 0;
        host_run(host);
        CHECK(host->parked &&
            host->status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
        CHECK(wl_columnar_eval_stack_cleanup_stack(host->frame)->items[0].rel ==
            published);
        CHECK(published->storage_generation == generation);
    }
    CHECK(!wl_columnar_source_access_reader_release(&reader));
    host->parked = false; host->turns = host->witnesses = 0;
    host_run(host); CHECK(host->done && !host->status.semantic_result);
    (void)host_finish(host); /* Original owned input retired exactly once. */
}

static void
governed_cases(void)
{
    static const uint64_t budgets[] = {64, 1024, 8192, 65536, 1048576};
    for (unsigned k = 0; k < sizeof(budgets) / sizeof(budgets[0]); ++k) {
        col_rel_t *input = source_new(17, 133, 2, true);
        host_t *host = host_new(input, false, 0, 0);
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = budgets[k], .usable_bytes = budgets[k],
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK,
        };
        wl_columnar_memory_governor_ref_t *reference =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(reference); host->session->memory_governor = reference;
        wl_columnar_memory_governor_ref_retain(reference);
        wl_columnar_memory_governor_t *governor =
            wl_columnar_memory_governor_ref_get(reference);
        wl_columnar_memory_reservation_t peer;
        wl_columnar_memory_reservation_init(&peer);
        CHECK(wl_columnar_memory_reserve(governor, 17, &peer));
        host_run(host); CHECK(host->done);
        if (host->status.semantic_result) {
            CHECK(host->status.semantic_result == ENOSPC);
            CHECK(host->session->memory_budget_denied &&
                !host->status.committed);
            CHECK(wl_columnar_memory_reserved(governor) == 17);
        } else {
            CHECK(host->status.committed &&
                wl_columnar_memory_reserved(governor) > 17);
        }
        CHECK(wl_columnar_memory_rollback(&peer));
        (void)host_finish(host);
        CHECK(!wl_columnar_memory_reserved(governor));
        wl_columnar_memory_governor_ref_release(reference);
        col_rel_destroy(input);
    }
}

static void
generated_union(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl a(x: int64)\n.decl b(x: int64)\n.decl result(x: int64)\n"
        "a(3). a(1). b(1). b(2).\nresult(X) :- a(X).\nresult(X) :- b(X).\n",
        &error);
    CHECK(program);
    wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
    const wl_plan_op_t *selected = NULL;
    for (uint32_t i = 0; i < plan->stratum_count; ++i)
        for (uint32_t j = 0; j < plan->strata[i].relation_count; ++j) {
            const wl_plan_relation_t *relation = &plan->strata[i].relations[j];
            if (strcmp(relation->name, "result")) continue;
            CHECK(!plan->strata[i].is_recursive);
            for (uint32_t k = 0; k < relation->op_count; ++k)
                if (relation->ops[k].op ==
                    WL_PLAN_OP_CONSOLIDATE) selected = &relation->ops[k];
        }
    CHECK(selected);
    /* Actual preceding raw bag is setup, outside the controlled certificate. */
    col_rel_t *input = source_new(1, 4, 0, false);
    input->columns[0][0] = 3; input->columns[0][1] = 1;
    input->columns[0][2] = 1; input->columns[0][3] = 2;
    host_t *host = host_new(input, false, 0, 0); host->op = *selected;
    host_run(host); CHECK(host->done && !host->status.semantic_result);
    col_rel_t *output =
        wl_columnar_eval_stack_cleanup_stack(host->frame)->items[0].rel;
    CHECK(output->nrows == 3);
    for (unsigned i = 0; i < 3;
        ++i) CHECK(output->columns[0][i] == (int64_t)i + 1);
    (void)host_finish(host); col_rel_destroy(input);
    wl_plan_free(plan); wirelog_program_free(program);
}

static void
edge_parities(void)
{
    static const int64_t extremes[] = {INT64_MAX, 0, INT64_MIN, -1, 0,
                                       INT64_MAX, 1};
    static const int64_t segmented[] = {1, 2, 4, 0, 1, 3, 4};
    for (unsigned kind = 0; kind < 4; ++kind) {
        col_rel_t *root = source_new(1, 7, 0, false);
        col_rel_t *twin = source_new(1, 7, 0, false);
        const int64_t *values = kind == 1 ? segmented : extremes;
        for (unsigned row = 0; row < 7; ++row)
            root->columns[0][row] = twin->columns[0][row] = values[row];
        col_rel_t *input = root;
        if (kind == 2) {
            input = source_new(1, 0, 0, false);
            CHECK(!col_rel_install_shared_view(input, root));
            CHECK(col_rel_storage_alias_borrow_count(root) == 1);
        }
        host_t *host = host_new(input, false, 0, 0);
        eval_stack_t raw = {0}; CHECK(!eval_stack_push(&raw, twin, true));
        if (kind == 1) {
            eval_entry_t *entry =
                &wl_columnar_eval_stack_cleanup_stack(host->frame)->items[0];
            entry->seg_boundaries = malloc(3 * sizeof(uint32_t));
            raw.items[0].seg_boundaries = malloc(3 * sizeof(uint32_t));
            CHECK(entry->seg_boundaries && raw.items[0].seg_boundaries);
            entry->seg_count = raw.items[0].seg_count = 2;
            entry->seg_boundaries[0] = raw.items[0].seg_boundaries[0] = 0;
            entry->seg_boundaries[1] = raw.items[0].seg_boundaries[1] = 3;
            entry->seg_boundaries[2] = raw.items[0].seg_boundaries[2] = 7;
            input->sorted_nrows = twin->sorted_nrows = 3;
        }
        wl_col_session_t *raw_session = calloc(1, sizeof(*raw_session));
        CHECK(raw_session);
        wl_mem_ledger_init(&raw_session->mem_ledger, 0);
        CHECK(!col_op_consolidate(&raw, raw_session));
        if (kind == 3) {
            /* Legacy NULL-control route is outside the work certificate. */
            CHECK(!wl_columnar_filter_consolidate_attempt_start(&host->op,
                host->frame, host->session, NULL, 0, &host->status));
            CHECK(host->status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE);
            host->started = true; host->acknowledge = true;
        }
        host_run(host); CHECK(host->done && !host->status.semantic_result);
        relations_equal(wl_columnar_eval_stack_cleanup_stack(
                host->frame)->items[0].rel,
            raw.items[0].rel);
        (void)host_finish(host); CHECK(!eval_stack_drain(&raw));
        free(raw_session);
        if (input != root) {
            CHECK(col_rel_storage_alias_borrow_count(root) == 1);
            col_rel_destroy(input);
            CHECK(!col_rel_storage_alias_borrow_count(root));
        }
        col_rel_destroy(root);
    }
}

static uint64_t prefix_visits, suffix_visits, merge_visits, prefix_anchor;
static uint32_t first_suffix;
static void
incremental_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase == 208) ++prefix_visits;
    if (phase == 204) {
        if (!suffix_visits) first_suffix = (uint32_t)progress;
        ++suffix_visits;
    }
    if (phase == 209) ++merge_visits;
    checkpoint(control, phase, progress);
}

static void
incremental_prefix_boundary(void)
{
    /* After the empirical setup anchor, each one-column discovery comparison
     * costs 128 traversal + 128 compare entry + 128 cell + 128 decision.
     * The target budget is derived from those source fees, not total runtime. */
    col_rel_t *input = source_new(1, 9, 2, false);
    for (unsigned trial = 0; trial < 4; ++trial) {
        uint64_t limit = trial >=
            2 ? prefix_anchor + 2 * 512 - (trial == 3) : 0;
        host_t *host = host_new(input, false, limit, 0);
        host->session->diff_enabled = true;
        prefix_visits = 0; hook_hit = false; restart_hook = false;
        target_phase = 208;
        target_progress = trial == 0 ? 1 : trial == 1 ? 3 : UINT64_MAX;
        wl_columnar_filter_test_attempt_checkpoint = incremental_checkpoint;
        host_run(host);
        wl_columnar_filter_test_attempt_checkpoint = NULL;
        CHECK(host->status.semantic_result == (trial >= 2
            ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(prefix_visits == (trial == 0 ? 1 : trial == 3 ? 2 : 3));
        uint64_t consumed = host_finish(host).consumed;
        if (!trial) prefix_anchor = consumed;
        if (trial == 1) CHECK(consumed == prefix_anchor + 2 * 512);
    }
    col_rel_destroy(input);
    input = source_new(1, 2, 0, true);
    input->columns[0][1] = wl_columnar_float_to_bits(NAN);
    host_t *host = host_new(input, false, 0, 0);
    host->session->diff_enabled = true;
    prefix_visits = 0; hook_hit = true;
    wl_columnar_filter_test_attempt_checkpoint = incremental_checkpoint;
    host_run(host);
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    CHECK(host->status.semantic_result == EINVAL && !prefix_visits);
    (void)host_finish(host); col_rel_destroy(input);
}

static void
incremental_prefixes(void)
{
    /* Includes equal boundaries, inversions, suffix-before-prefix, wide keys,
     * empty/nullary inputs, signed zeros and fully ordered inputs. */
    static const int64_t values[][9] = {
        {1, 2, 3, 4, 5, 6, 7, 8, 9},
        {1, 2, 3, 3, 0, -1, 5, 2, 4},
        {9, 8, 7, 6, 5, 4, 3, 2, 1},
        {2, 2, 2, 2, 2, 2, 2, 2, 2},
        {-3, -2, -1, 0, 0, 2, 1, 0, -2},
    };
    for (unsigned shape = 0; shape < 5; ++shape)
        for (unsigned width_case = 0; width_case < 4; ++width_case)
            for (unsigned empty = 0; empty < 2; ++empty) {
                uint32_t width = width_case == 0 ? 0
                    : width_case == 1 ? 1 : 17;
                bool typed = width_case == 3;
                uint32_t rows = empty ? 0 : 9;
                col_rel_t *input = source_new(width, rows, 0, typed);
                col_rel_t *twin = source_new(width, rows, 0, typed);
                for (uint32_t row = 0; row < rows; ++row)
                    if (width) {
                        int64_t value = values[shape][row];
                        if (typed) value = wl_columnar_float_to_bits(
                                value ? (double)value : row % 2 ? -0.0 : 0.0);
                        input->columns[width - 1][row] = value;
                        twin->columns[width - 1][row] = value;
                    }
                /* Cache metadata is deliberately unusable: ordering must be
                 * discovered from the leased values without changing it. */
                input->sorted_nrows = rows + 11;
                uint64_t generation = input->storage_generation;
                host_t *host = host_new(input, false, 0, 0);
                host->session->diff_enabled = true;
                prefix_visits = suffix_visits = merge_visits = 0;
                hook_hit = true;
                wl_columnar_filter_test_attempt_checkpoint =
                    incremental_checkpoint;
                host_run(host);
                wl_columnar_filter_test_attempt_checkpoint = NULL;
                CHECK(!host->status.semantic_result);
                eval_stack_t raw = {0};
                CHECK(!eval_stack_push(&raw, twin, true));
                wl_col_session_t *reference = calloc(1, sizeof(*reference));
                CHECK(reference);
                wl_mem_ledger_init(&reference->mem_ledger, 0);
                CHECK(!col_op_consolidate(&raw, reference));
                relations_equal(wl_columnar_eval_stack_cleanup_stack(
                        host->frame)->items[0].rel, raw.items[0].rel);
                CHECK(input->sorted_nrows == rows + 11
                    && input->storage_generation == generation);
                if (!empty && width && shape == 0)
                    CHECK(prefix_visits == 8 && !suffix_visits &&
                        !merge_visits);
                if (!empty && width && shape == 1)
                    CHECK(prefix_visits == 3 && suffix_visits
                        && first_suffix == 3 && merge_visits == rows);
                (void)host_finish(host);
                col_rel_destroy(input); CHECK(!eval_stack_drain(&raw));
                free(reference);
            }

    static const unsigned phases[] = {208, 204, 209, 207};
    for (unsigned stop = 0; stop < 4; ++stop) {
        col_rel_t *input = source_new(1, 9, 0, false);
        for (uint32_t row = 0; row < 9; ++row)
            input->columns[0][row] = values[1][row];
        uint64_t generation = input->storage_generation;
        host_t *host = host_new(input, false, 0, 0);
        host->session->diff_enabled = true;
        target_phase = phases[stop];
        target_progress = target_phase == 208 ? 1 : target_phase == 204 ? 3 : 0;
        hook_hit = restart_hook = false;
        wl_columnar_filter_test_attempt_checkpoint = incremental_checkpoint;
        host_run(host);
        wl_columnar_filter_test_attempt_checkpoint = NULL;
        CHECK(hook_hit &&
            host->status.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!host->status.committed &&
            input->storage_generation == generation);
        CHECK(wl_columnar_eval_stack_cleanup_stack(host->frame)->items[0].rel ==
            input);
        (void)host_finish(host);
        /* Retry the unchanged source after all private owners are drained. */
        host = host_new(input, false, 0, 0);
        host->session->diff_enabled = true;
        host_run(host); CHECK(!host->status.semantic_result);
        uint64_t exact = host_finish(host).consumed;
        for (unsigned shorter = 0; shorter < 2; ++shorter) {
            host = host_new(input, false, exact - shorter, 0);
            host->session->diff_enabled = true;
            host_run(host);
            CHECK(host->status.semantic_result == (shorter
                ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
            CHECK(host->status.committed == !shorter);
            (void)host_finish(host);
        }
        col_rel_destroy(input);
    }
}

/* Stable representative selection must preserve provenance, not choose the
 * smallest timestamp or sum signed weights of equal value tuples. */
static void
physical_representatives(void)
{
    const uint32_t widths[] = {0, 1, 17};
    const int64_t weights[] = {0, 1, -1, 3, -4};
    for (unsigned w = 0; w < 3; ++w) for (unsigned prefix = 0; prefix < 2;
            ++prefix) {
            col_rel_t *input = source_new(widths[w], 14, 3, false);
            col_rel_t *reference = source_new(widths[w], 14, 3, false);
            CHECK(!col_rel_enable_timestamps(input));
            CHECK(!col_rel_enable_timestamps(reference));
            col_delta_timestamp_t saved[14];
            for (unsigned r = 0; r < 14; ++r) {
                saved[r] = (col_delta_timestamp_t){.iteration = 100-r,
                                                   .stratum = r%3,
                                                   .worker = r%2,
                                                   ._reserved = 0,
                                                   .multiplicity = weights[r%
                                                           5]};
                input->timestamps[r] = reference->timestamps[r] = saved[r];
            }
            if (prefix) {
                uint32_t sorted = widths[w] ? 7 : 1;
                input->sorted_nrows = reference->sorted_nrows = sorted;
                input->run_count = reference->run_count = 1;
                input->run_ends[0] = reference->run_ends[0] = sorted;
            }
            col_relation_snapshot_t before =
                wl_columnar_relation_snapshot(input);
            host_t *host = host_new(input, false, 0, 0);
            eval_stack_t raw = {0};
            CHECK(!eval_stack_push(&raw, reference, false));
            CHECK(!col_op_consolidate(&raw, host->session));
            host_run(host);
            CHECK(host->done && !host->status.semantic_result &&
                host->status.committed);
            col_rel_t *actual =
                wl_columnar_eval_stack_cleanup_stack(host->frame)->items[0].rel;
            CHECK(actual->timestamps && raw.items[0].rel->timestamps);
            CHECK(actual->nrows == (widths[w] ? 7u : 1u));
            relations_equal(actual, raw.items[0].rel);
            for (unsigned r = 0; r < actual->nrows; ++r) {
                unsigned first = widths[w] ? r : 0;
                const col_delta_timestamp_t *a = &actual->timestamps[r];
                const col_delta_timestamp_t *b =
                    &raw.items[0].rel->timestamps[r];
                const col_delta_timestamp_t *e = &saved[first];
                CHECK(a->iteration == e->iteration && a->stratum == e->stratum
                    && a->worker == e->worker && a->_reserved == 0
                    && a->multiplicity == e->multiplicity);
                CHECK(b->iteration == e->iteration && b->stratum == e->stratum
                    && b->worker == e->worker && b->_reserved == 0
                    && b->multiplicity == e->multiplicity);
            }
            CHECK(wl_columnar_relation_snapshot_equal(before,
                wl_columnar_relation_snapshot(input)));
            for (unsigned r = 0; r < 14; ++r) {
                CHECK(input->timestamps[r].iteration == saved[r].iteration
                    && input->timestamps[r].stratum == saved[r].stratum
                    && input->timestamps[r].worker == saved[r].worker
                    && input->timestamps[r]._reserved == 0
                    && input->timestamps[r].multiplicity ==
                    saved[r].multiplicity);
            }
            CHECK(!eval_stack_drain(&raw)); col_rel_destroy(reference);
            (void)host_finish(host); col_rel_destroy(input);
        }
}

int
main(void)
{
    physical_representatives();
    static const uint32_t widths[] = {0, 1, 7, 17, 129};
    static const uint32_t rows[] = {0, 1, 2, 19, 130};
    for (unsigned w = 0; w < sizeof(widths) / sizeof(widths[0]); ++w)
        for (unsigned r = 0; r < sizeof(rows) / sizeof(rows[0]); ++r)
            for (unsigned pattern = 0; pattern < 5; ++pattern)
                for (unsigned typed = 0; typed < 2; ++typed)
                    CHECK(parity(widths[w], rows[r], pattern, typed)
                        == parity(widths[w], rows[r], pattern, typed));
    incremental_prefixes(); incremental_prefix_boundary();
    negative_cases(); interruptions(); budgets_and_allocation();
    allocation_ordinals(); retained_input(); governed_cases();
    generated_union();
    literal_budgets(); child_interruptions(); edge_parities();
    puts("evaluation CONSOLIDATE: PASS");
    return 0;
}
