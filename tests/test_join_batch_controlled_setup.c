/* Controlled typed/stamped scratch prerequisite for session JOIN batches. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); abort(); \
                      } } while (0)

typedef struct {
    int owner;
    wl_evaluation_control_t *control;
    uint64_t id;
    wl_columnar_memory_governor_ref_t *governor;
} fixture_t;

static void
fixture_begin(fixture_t *f, uint64_t memory, uint64_t work)
{
    memset(f, 0, sizeof(*f));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = memory, .usable_bytes = memory,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    CHECK(!wl_evaluation_control_create(work, &f->control));
    CHECK(!wl_evaluation_control_attach(f->control, &f->owner));
    CHECK(!wl_evaluation_control_begin(f->control, &f->owner));
    f->id = wl_evaluation_control_active_attempt_id(f->control);
}

static uint64_t
reserved(const fixture_t *f)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->governor));
}

static void
fixture_end(fixture_t *f, int result)
{
    CHECK(!reserved(f));
    if (wl_evaluation_control_active_attempt_id(f->control))
        CHECK(wl_evaluation_control_finish(f->control, &f->owner, result, 0)
            == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f->control, &report)
        && report.result == result);
    CHECK(!wl_evaluation_control_detach(f->control, &f->owner));
    wl_evaluation_control_release(f->control);
    wl_columnar_memory_governor_ref_release(f->governor);
}

static int
construct(fixture_t *f, uint32_t width, uint32_t capacity,
    const int64_t *codes, bool stamped, col_rel_t **out)
{
    return
        wl_columnar_relation_new_typed_codes_stamped_operator_attempt_controlled
        (
        "$join_batch", width, capacity, codes, stamped, f->governor,
        out, f->control, f->id);
}

static void
check_empty(const col_rel_t *out, uint32_t width, uint32_t capacity,
    const int64_t *codes, bool stamped)
{
    CHECK(out && out->ncols == width && !out->nrows
        && out->capacity == capacity && out->column_types);
    for (uint32_t c = 0; c < width; ++c)
        CHECK(out->column_types[c] == codes[c]);
    CHECK((out->timestamps != NULL) == (stamped && capacity));
    CHECK(out->timestamp_capacity == (stamped ? capacity : 0));
    if (stamped)
        for (uint32_t r = 0; r < capacity; ++r) {
            const col_delta_timestamp_t *ts = &out->timestamps[r];
            CHECK(!ts->iteration && !ts->stratum && !ts->worker
                && !ts->_reserved && !ts->multiplicity);
        }
}

static void
test_domains(void)
{
    const uint32_t widths[] = {3, 65, 257};
    const uint32_t capacities[] = {1, 3, 129};
    for (unsigned w = 0; w < 3; ++w)
        for (unsigned cap = 0; cap < 3; ++cap) {
            uint32_t width = widths[w], capacity = capacities[cap];
            int64_t *codes = malloc((size_t)width * sizeof(*codes));
            CHECK(codes);
            for (uint32_t c = 0; c < width; ++c)
                codes[c] = c % 3 == 0 ? WIRELOG_TYPE_INT32
                    : c % 3 == 1 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_BOOL;
            uint64_t credit[2] = {0}, work[2] = {0};
            for (unsigned stamped = 0; stamped < 2; ++stamped) {
                fixture_t f; fixture_begin(&f, 16u * 1024u * 1024u, 0);
                col_rel_t *out = NULL;
                CHECK(!construct(&f, width, capacity, codes, stamped, &out));
                check_empty(out, width, capacity, codes, stamped);
                credit[stamped] = reserved(&f);
                uint64_t payload = 0;
                CHECK(col_rel_retained_bytes_for(out, 1, &payload));
                CHECK(payload == (uint64_t)width * sizeof(int64_t)
                    + (stamped ? sizeof(col_delta_timestamp_t) : 0));
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_finish(f.control, &f.owner, 0, 0));
                CHECK(!wl_evaluation_control_report(f.control, &report));
                work[stamped] = report.consumed;
                /* Caller scratch is borrowed only during construction. */
                codes[1] = WIRELOG_TYPE_STRING;
                CHECK(out->column_types[1] == WIRELOG_TYPE_FLOAT);
                codes[1] = WIRELOG_TYPE_FLOAT;
                CHECK(!col_rel_destroy_checked(out));
                fixture_end(&f, 0);
            }
            CHECK(credit[1] - credit[0]
                == (uint64_t)capacity * sizeof(col_delta_timestamp_t));
            CHECK(work[1] > work[0]);
            /* Exact and one-short target limits exercise the same original
             * attempt, including the final result handoff. */
            for (unsigned shortfall = 0; shortfall < 2; ++shortfall) {
                fixture_t f;
                fixture_begin(&f, 16u * 1024u * 1024u, work[1] - shortfall);
                col_rel_t *out = NULL;
                int rc = construct(&f, width, capacity, codes, true, &out);
                CHECK(rc == (shortfall ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
                if (shortfall) CHECK(!out);
                else {
                    check_empty(out, width, capacity, codes, true);
                    CHECK(!col_rel_destroy_checked(out));
                }
                fixture_end(&f, rc);
            }
            free(codes);
        }
}

static wl_evaluation_control_t *expected_control;
static uint64_t expected_id;
static unsigned cancel_phase, observed;
static fixture_t *rollover_fixture;

static void
cancel_commit(wl_evaluation_control_t *control, unsigned phase)
{
    CHECK(control == expected_control
        && wl_evaluation_control_active_attempt_id(control) == expected_id);
    ++observed;
    if (phase == cancel_phase) wl_evaluation_control_request_cancel(control);
}

static void
cancel_identity(wl_evaluation_control_t *control, uint64_t identity)
{
    (void)identity;
    CHECK(control == expected_control
        && wl_evaluation_control_active_attempt_id(control) == expected_id);
    ++observed;
    wl_evaluation_control_request_cancel(control);
}

static void
replace_attempt(wl_evaluation_control_t *control, uint64_t identity)
{
    (void)identity;
    CHECK(rollover_fixture && control == rollover_fixture->control);
    ++observed;
    /* Privileged deterministic injection of a forbidden host lifecycle
     * change. The constructor must reject its saved ID and leave the new
     * attempt uncharged, rather than silently adopting it. */
    CHECK(!wl_evaluation_control_finish(control, &rollover_fixture->owner, 0,
        0));
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_begin(control, &rollover_fixture->owner));
}

static void
test_failures(void)
{
    const int64_t codes[] = {WIRELOG_TYPE_INT32, WIRELOG_TYPE_FLOAT,
                             WIRELOG_TYPE_BOOL};
    fixture_t f; fixture_begin(&f, 16u * 1024u * 1024u, 0);
    col_rel_t *out = NULL;
    wl_columnar_relation_test_constructor_fail_allocation = UINT32_MAX;
    CHECK(!construct(&f, 3, 3, codes, true, &out));
    uint32_t allocations = UINT32_MAX
        - wl_columnar_relation_test_constructor_fail_allocation;
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    CHECK(allocations > 3 && !col_rel_destroy_checked(out));
    fixture_end(&f, 0);
    for (uint32_t allocation = 1; allocation <= allocations; ++allocation) {
        fixture_begin(&f, 16u * 1024u * 1024u, 0); out = NULL;
        wl_columnar_relation_test_constructor_fail_allocation = allocation;
        CHECK(construct(&f, 3, 3, codes, true, &out) == ENOMEM && !out);
        CHECK(!wl_columnar_relation_test_constructor_fail_allocation);
        fixture_end(&f, ENOMEM);
    }
    for (unsigned phase = 0; phase < 4; ++phase) {
        fixture_begin(&f, 16u * 1024u * 1024u, 0); out = NULL;
        expected_control = f.control; expected_id = f.id;
        cancel_phase = phase; observed = 0;
        if (phase == 3)
            wl_columnar_relation_test_before_identity_cas = cancel_identity;
        else wl_columnar_relation_test_constructor_committed = cancel_commit;
        int rc = construct(&f, 3, 129, codes, true, &out);
        wl_columnar_relation_test_constructor_committed = NULL;
        wl_columnar_relation_test_before_identity_cas = NULL;
        expected_control = NULL;
        CHECK(observed && rc == WL_EVALUATION_CONTROL_CANCELLED && !out);
        fixture_end(&f, rc);
    }
    fixture_begin(&f, 1, 0); out = NULL;
    CHECK(construct(&f, 3, 3, codes, true, &out) == ENOSPC && !out);
    fixture_end(&f, ENOSPC);
    fixture_begin(&f, 1, 0); out = NULL;
    wl_evaluation_control_request_cancel(f.control);
    CHECK(construct(&f, 3, 3, codes, true, &out)
        == WL_EVALUATION_CONTROL_CANCELLED && !out);
    fixture_end(&f, WL_EVALUATION_CONTROL_CANCELLED);
    fixture_begin(&f, 16u * 1024u * 1024u, 0); out = NULL;
    rollover_fixture = &f; observed = 0;
    wl_columnar_relation_test_before_identity_cas = replace_attempt;
    CHECK(construct(&f, 3, 3, codes, true, &out) == EINVAL && !out);
    wl_columnar_relation_test_before_identity_cas = NULL;
    rollover_fixture = NULL;
    CHECK(observed == 1 && !reserved(&f)
        && wl_evaluation_control_active_attempt_id(f.control) != f.id);
    CHECK(!wl_evaluation_control_finish(f.control, &f.owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.control,
        &report) && !report.consumed);
    fixture_end(&f, 0);
    fixture_begin(&f, 16u * 1024u * 1024u, 0); out = NULL;
    CHECK(
        wl_columnar_relation_new_typed_codes_stamped_operator_attempt_controlled
        (
            "$join_batch", 3, 3, codes, true, f.governor, &out, f.control,
            f.id + 1) == EINVAL && !out && !reserved(&f));
    const int64_t invalid[] = {-1, INT64_MAX, (int64_t)WIRELOG_TYPE_BOOL + 1};
    for (unsigned i = 0; i < 3; ++i)
        CHECK(construct(&f, 1, 3, &invalid[i], true, &out) == EINVAL && !out);
    CHECK(!construct(&f, 0, 0, NULL, true, &out));
    CHECK(out && !out->ncols && !out->capacity && !out->timestamps);
    CHECK(!col_rel_destroy_checked(out)); out = NULL;
    CHECK(!construct(&f, 3, 0, codes, true, &out));
    check_empty(out, 3, 0, codes, true);
    CHECK(!col_rel_destroy_checked(out));
    fixture_end(&f, 0);
}

static unsigned prepare_stop_phase;
static bool prepare_work_stop;
static unsigned prepare_denial_phase;
static wl_columnar_memory_reservation_t *prepare_peer;

static void
prepare_stop(const wl_columnar_join_batch_prepare_t *a)
{
    if (a->phase == prepare_denial_phase && !prepare_peer->governor) {
        uint64_t available = 16u * 1024u * 1024u
            - wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(a->governor));
        CHECK(available > 1 && wl_columnar_memory_reserve(
                wl_columnar_memory_governor_ref_get(a->governor), available - 1,
                prepare_peer));
    }
    if (a->phase != prepare_stop_phase) return;
    if (prepare_work_stop)
        CHECK(wl_evaluation_control_charge_attempt(a->control, a->attempt_id,
            UINT64_C(1000000000)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(a->control);
}

static col_rel_t *
prepare_source(fixture_t *f, const char *name, uint32_t width, bool stamped,
    bool floating, bool right)
{
    col_rel_t *rel = NULL;
    CHECK(!wl_columnar_relation_new_auto_governed(name, width, 3, stamped,
        f->governor, &rel));
    wirelog_column_type_t *types = malloc((size_t)width * sizeof(*types));
    int64_t *values = malloc((size_t)width * sizeof(*values));
    CHECK(types && values);
    for (uint32_t c = 0; c < width; ++c)
        types[c] = c == 0 && floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    CHECK(!col_rel_set_column_types(rel, types, width));
    for (uint32_t row = 0; row < 3; ++row) {
        int64_t key = right ? (row < 2 ? 1 : 3) : (row == 1 ? 2 : 1);
        for (uint32_t c = 0; c < width; ++c)
            values[c] = c == 0 ? floating
                ? wl_columnar_float_to_bits((double)(key - 1)) : key
                : (int64_t)c + key * 10;
        CHECK(!col_rel_append_row(rel, values));
        if (stamped) rel->timestamps[row] = (col_delta_timestamp_t){
                .iteration = row + 7, .stratum = 3, .worker = 9,
                .multiplicity = -2
            };
    }
    free(types); free(values);
    return rel;
}

static wl_columnar_join_batch_prepare_t *retirement_parent;
static unsigned retirement_callbacks[5];

static void
prepare_retirement_checkpoint(
    wl_columnar_relation_scalar_retirement_initializer_t *i,
    wl_evaluation_control_t *funding, unsigned api)
{
    wl_columnar_join_batch_prepare_t *a = retirement_parent;
    CHECK(a && i == &a->retirement_initializer && a->cleanup_busy && a->aborting
        && api >= 1 && api <= 4);
    ++retirement_callbacks[api];
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    bool done = true;
    CHECK(wl_columnar_join_batch_prepare_abort_step(a, funding, id,
        &done) == EBUSY && done);
    CHECK(wl_columnar_join_batch_prepare_abort_step(a, a->control,
        a->attempt_id,
        &done) == EINVAL && done);
    CHECK(wl_columnar_join_batch_prepare_step(a, 1, &done) == EINVAL && !done);
    wl_columnar_join_batch_prepare_t copied = *a;
    done = true;
    CHECK(wl_columnar_join_batch_prepare_abort_step(&copied, funding, id,
        &done) == EINVAL && done);
}

static void
prepare_drain(wl_columnar_join_batch_prepare_t *a)
{
    int owner;
    wl_evaluation_control_t *funding = NULL;
    bool done = false, tried[WL_COLUMNAR_RELATION_SCALAR_INIT_DONE+1] = {false};
    retirement_parent = a;
    memset(retirement_callbacks, 0, sizeof(retirement_callbacks));
    wl_columnar_relation_scalar_retirement_init_test_checkpoint =
        prepare_retirement_checkpoint;
    for (unsigned turn = 0; !done; ++turn) {
        CHECK(turn < 100000);
        unsigned phase = a->retirement_initializer.phase;
        bool short_grant = phase && !tried[phase];
        uint64_t limit = 384;
        if (short_grant) {
            tried[phase] = true;
            limit = phase == WL_COLUMNAR_RELATION_SCALAR_INIT_ZERO ? 255
                : (phase == WL_COLUMNAR_RELATION_SCALAR_INIT_BIND
                || phase ==
                WL_COLUMNAR_RELATION_SCALAR_INIT_TOKENS) ? 191 : 127;
        }
        CHECK(!wl_evaluation_control_create(limit, &funding));
        CHECK(!wl_evaluation_control_attach(funding, &owner));
        CHECK(!wl_evaluation_control_begin(funding, &owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        if (!turn) CHECK(wl_columnar_join_batch_prepare_abort_step(a, funding,
                id + 1,
                &done) == EINVAL && !done && !a->aborting);
        unsigned char historic[sizeof(a->retirement)];
        memcpy(historic, &a->retirement, sizeof(historic));
        bool begun = a->retirement_started;
        int rc = wl_columnar_join_batch_prepare_abort_step(a, funding, id,
                &done);
        CHECK(rc == (short_grant ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        if (short_grant) CHECK(!done && a->retirement_initializer.phase == phase
                && a->retirement_started == begun
                && !memcmp(historic, &a->retirement, sizeof(historic)));
        if (a->retirement_initializer.phase ==
            WL_COLUMNAR_RELATION_SCALAR_INIT_DONE)
            CHECK(a->retirement_started && a->retirement.storage.relation
                == a->retirement_initializer.relation);
        CHECK(wl_evaluation_control_finish(funding, &owner, rc, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(funding, &report)
            && report.consumed <= limit && (!short_grant || !report.consumed));
        CHECK(!wl_evaluation_control_detach(funding, &owner));
        wl_evaluation_control_release(funding);
    }
    wl_columnar_relation_scalar_retirement_init_test_checkpoint = NULL;
    retirement_parent = NULL;
    if (retirement_callbacks[1]) CHECK(retirement_callbacks[2] &&
            retirement_callbacks[3]
            && retirement_callbacks[4] && tried[1] && tried[2] && tried[3]);
    CHECK(a->drained && !a->producer && !a->continuation
        && !a->left_reader.owner && !a->right_reader.owner);
}

static wl_columnar_continuation_status_t
unexpected_batch(void *context, const wl_columnar_continuation_batch_t *batch)
{
    (void)context; (void)batch; abort();
}

static wl_columnar_continuation_status_t
unexpected_reserve(void *context, uint64_t bytes, uint32_t rows)
{
    (void)context; (void)bytes; (void)rows; abort();
}

static wl_columnar_continuation_status_t
unexpected_commit(void *context, bool *committed)
{
    (void)context; (void)committed; abort();
}

static void
unexpected_abort(void *context)
{
    (void)context; abort();
}

/* The input has four literal equal-key pairs. Check every payload lane,
 * native and general producers, real private tagged heads and one-row batches.
 * No raw constructor or persistent arrangement is used for preparation. */
static void
test_prepared_primary(unsigned shape, bool stamped, unsigned stop,
    bool work_stop, uint32_t fail_allocation)
{
    fixture_t f; fixture_begin(&f, 16u * 1024u * 1024u,
        work_stop ? UINT64_C(1000000000) : 0);
    uint32_t left_width = shape == 3 ? 128 : shape == 1 ? 2 : 1;
    uint32_t right_width = shape == 3 ? 129 : shape == 1 ? 2 : 1;
    bool floating = shape == 2;
    col_rel_t *left = prepare_source(&f, "left", left_width, stamped, floating,
            false);
    col_rel_t *right = prepare_source(&f, "right", right_width, false, floating,
            true);
    if (shape == 4) {
        right->nrows = 0; wl_columnar_relation_touch_view(right);
    }
    if (shape == 5) {
        left->nrows = 0; wl_columnar_relation_touch_view(left);
    }
    uint64_t baseline = reserved(&f);
    col_relation_snapshot_t left_snapshot = wl_columnar_relation_snapshot(left);
    col_relation_snapshot_t right_snapshot =
        wl_columnar_relation_snapshot(right);
    wl_col_session_t session = {0};
    wl_mem_ledger_init(&session.mem_ledger, 0);
    session.memory_governor = f.governor;
    session.base.evaluation_control = f.control;
    uint32_t keys[] = {0, 1};
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "right"};
    uint32_t kc = shape == 1 ? 2 : 1;
    op.key_count = kc;
    wl_columnar_join_batch_prepare_t a = {0};
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer); prepare_peer = &peer;
    uint64_t bytes = ((uint64_t)left_width + right_width) * sizeof(int64_t)
        + (stamped ? sizeof(col_delta_timestamp_t) : 0);
    CHECK(!wl_columnar_join_batch_prepare_begin(&a, &session, &op, left, right,
        stamped, keys, keys, kc, bytes, f.control, f.id));
    bool invalid_done = true;
    CHECK(wl_columnar_join_batch_prepare_step(&a, 0, &invalid_done) == EINVAL
        && !invalid_done && !a.result);
    CHECK(wl_columnar_join_batch_prepare_step(&a, 257, &invalid_done) == EINVAL
        && !invalid_done && !a.result);
    prepare_stop_phase = stop; prepare_work_stop = work_stop;
    wl_columnar_join_batch_test_prepare_checkpoint = stop ||
        prepare_denial_phase
        ? prepare_stop : NULL;
    wl_columnar_join_batch_test_prepare_fail_allocation = fail_allocation;
    bool done = false;
    int rc = 0;
    for (unsigned turn = 0; !done && !rc; ++turn) {
        CHECK(turn < 100000);
        rc = wl_columnar_join_batch_prepare_step(&a, 1, &done);
    }
    wl_columnar_join_batch_test_prepare_checkpoint = NULL;
    if (stop) CHECK(rc == (work_stop ? WL_EVALUATION_CONTROL_WORK_LIMIT
                                  : WL_EVALUATION_CONTROL_CANCELLED));
    else if (fail_allocation) CHECK(rc == ENOMEM);
    else if (prepare_denial_phase) CHECK(rc == ENOSPC &&
            session.memory_budget_denied);
    else CHECK(!rc && done && a.ready && a.continuation);
    wl_columnar_join_batch_test_prepare_fail_allocation = 0;
    CHECK(!session.arr_count && !session.arr_entries);
    if (!rc) {
        wl_columnar_continuation_sink_t sink = {
            .begin = unexpected_batch, .reserve = unexpected_reserve,
            .append = unexpected_batch, .commit = unexpected_commit,
            .abort = unexpected_abort
        };
        CHECK(wl_columnar_continuation_publish(a.continuation, &sink)
            == WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(a.left_reader.identity == (uintptr_t)&a.left_reader
            && a.right_reader.identity == (uintptr_t)&a.right_reader);
        CHECK(col_join_batch_control_discard(&a.continuation) == EBUSY
            && a.continuation);
        int64_t *values = calloc(left_width, sizeof(*values)); CHECK(values);
        CHECK(col_rel_append_row(left, values) == EBUSY); free(values);
        CHECK(a.arrangement.generation == 1
            && a.arrangement.indexed_rows == right->nrows
            && a.arrangement.ht_cap == right->nrows);
        if (right->nrows)
            CHECK(a.arrangement.ht_next[1] == 0
                && a.arrangement.ht_next[0] == UINT32_MAX);
        uint32_t rows = 0, batches = 0;
        for (unsigned turn = 0; ; ++turn) {
            CHECK(turn < 100000);
            col_join_batch_control_result_t result;
            col_join_batch_control_status_t status =
                col_join_batch_control_step(
                a.continuation, 256, &result);
            if (status == COL_JOIN_BATCH_CONTROL_DONE) break;
            if (status == COL_JOIN_BATCH_CONTROL_PROGRESS) continue;
            CHECK(status == COL_JOIN_BATCH_CONTROL_BATCH_READY);
            const col_rel_t *batch = NULL; uint64_t generation = 0;
            CHECK(!col_join_batch_control_peek(a.continuation, &batch,
                &generation));
            CHECK(batch && batch->nrows == 1 && generation == ++batches);
            for (uint32_t c = 0; c < batch->ncols; ++c) {
                uint32_t src = c < left_width ? c : c - left_width;
                int64_t expected = src ? (int64_t)src + 10
                    : floating ? wl_columnar_float_to_bits(0) : 1;
                CHECK(batch->columns[c][0] == expected);
            }
            if (stamped) {
                CHECK(batch->timestamps && !batch->timestamps[0].iteration
                    && !batch->timestamps[0].stratum &&
                    !batch->timestamps[0].worker
                    && !batch->timestamps[0]._reserved
                    && !batch->timestamps[0].multiplicity);
            }
            ++rows;
            CHECK(!col_join_batch_control_precommit(a.continuation,
                generation));
            CHECK(!col_join_batch_control_ack(a.continuation, generation));
        }
        CHECK(rows == (shape >= 4 ? 0u : 4u) && batches == rows);
    }
    CHECK(wl_evaluation_control_finish(f.control, &f.owner, rc, 0) == rc);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(f.control, &before));
    prepare_drain(&a);
    CHECK(!wl_evaluation_control_report(f.control, &after)
        && before.attempt_id == after.attempt_id
        && before.consumed == after.consumed && before.result == after.result
        && before.stop_reason == after.stop_reason && !after.cleanup_status);
    CHECK(reserved(&f) == baseline + peer.bytes && !session.arr_count);
    if (peer.governor) CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(reserved(&f) == baseline); prepare_peer = NULL;
    CHECK(wl_columnar_relation_snapshot_equal(left_snapshot,
        wl_columnar_relation_snapshot(left)));
    CHECK(wl_columnar_relation_snapshot_equal(right_snapshot,
        wl_columnar_relation_snapshot(right)));
    if (stamped)
        for (uint32_t r = 0; r < 3; ++r)
            CHECK(left->timestamps[r].iteration == r + 7
                && left->timestamps[r].multiplicity == -2);
    CHECK(!col_rel_destroy_checked(left) && !col_rel_destroy_checked(right));
    fixture_end(&f, rc);
}

static void
test_preparation(void)
{
    for (unsigned shape = 0; shape < 6; ++shape)
        for (unsigned stamped = 0; stamped < 2; ++stamped)
            test_prepared_primary(shape, stamped, 0, false, 0);
    /* Every actual setup phase is observed before cancellation/work denial. */
    for (unsigned phase = 1; phase <= 13; ++phase)
        for (unsigned work = 0; work < 2; ++work)
            test_prepared_primary(3, true, phase, work, 0);
    for (uint32_t allocation = 1; allocation <= 7; ++allocation)
        test_prepared_primary(1, true, 0, false, allocation);
    const unsigned denial_phases[] = {1, 7, 11, 12};
    for (unsigned i = 0; i < 4; ++i) {
        prepare_denial_phase = denial_phases[i];
        test_prepared_primary(3, true, 0, false, 0);
    }
    prepare_denial_phase = 0;
}

static bool schema_stop_work;

static void
schema_stop(const wl_columnar_join_schema_attempt_t *a)
{
    if (a->phase != WL_COLUMNAR_JOIN_SCHEMA_COLUMNS || a->column != 17) return;
    if (schema_stop_work)
        CHECK(wl_evaluation_control_charge(a->control, UINT64_C(1000000000))
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(a->control);
}

static void
test_schema_case(unsigned kind, bool stamps, unsigned stop, bool work)
{
    fixture_t f; fixture_begin(&f, 16u * 1024u * 1024u,
        work ? UINT64_C(1000000000) : 0);
    col_rel_t *left = prepare_source(&f, "left", 65, stamps, true, false);
    col_rel_t *right = prepare_source(&f, "right", 192, stamps, false, true);
    char labels[65][12]; const char *names[65];
    for (unsigned c = 0; c < 65; ++c) {
        snprintf(labels[c], sizeof(labels[c]), "named%u", c);
        names[c] = labels[c];
    }
    CHECK(!col_rel_set_schema(left, 65, names));
    left->has_graph_column = true; left->graph_col_idx = 1;
    left->declared_ncols = 65;
    wl_columnar_relation_touch_view(left);
    CHECK(left->column_types && left->column_types[0] == WIRELOG_TYPE_FLOAT);
    col_relation_snapshot_t saved_left = wl_columnar_relation_snapshot(left);
    col_relation_snapshot_t saved_right = wl_columnar_relation_snapshot(right);
    uint32_t projection[] = {0, 65, 64, UINT32_MAX};
    uint32_t semi_projection[] = {0, 64, UINT32_MAX};
    wl_plan_op_t op = {.op = kind < 2 ? WL_PLAN_OP_JOIN
        : kind == 2 ? WL_PLAN_OP_SEMIJOIN : WL_PLAN_OP_ANTIJOIN};
    if (kind == 1 || kind == 3) {
        op.project_count = 4; op.project_indices = projection;
    } else if (kind == 2) {
        op.project_count = 3; op.project_indices = semi_projection;
    }
    wl_col_session_t session = {0};
    wl_mem_ledger_init(&session.mem_ledger, 0);
    session.memory_governor = f.governor;
    unsigned width = kind == 0 ? 257 : kind == 1 ? 4 : kind == 2 ? 3 : 65;
    col_rel_t *raw = NULL, *out = NULL;
    if (kind < 2) {
        CHECK(!wl_columnar_relation_new_auto_governed("$raw", width,
            COL_REL_INIT_CAP, false, f.governor, &raw));
        CHECK(!col_join_set_output_types(raw, left, right, &op));
    } else if (kind == 2)
        CHECK(!wl_columnar_semijoin_new_output(&op, &session, left, right,
            &raw));
    else CHECK(!wl_columnar_antijoin_new_output(&op, &session, left, right,
            &raw));
    uint64_t baseline = reserved(&f);
    wl_columnar_relation_scalar_source_t ls, rs;
    bool li = false, ri = false;
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(&ls, &li,
        f.control, f.id) && li);
    CHECK(!wl_columnar_relation_scalar_source_begin_attempt_controlled(&ls,
        left, false));
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(&rs, &ri,
        f.control, f.id) && ri);
    CHECK(!wl_columnar_relation_scalar_source_begin_attempt_controlled(&rs,
        right, false));
    wl_columnar_join_schema_attempt_t a = {0};
    CHECK(!wl_columnar_join_schema_attempt_begin(&a, &op, &ls, &rs, "$schema",
        f.governor, &out, f.control, f.id));
    wl_columnar_join_schema_attempt_t copied = a;
    bool done = false;
    CHECK(wl_columnar_join_schema_attempt_step(&copied, 1, &done) == EINVAL);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    if (stop == 3) {
        uint64_t available = 16u * 1024u * 1024u - reserved(&f);
        CHECK(available > 1 && wl_columnar_memory_reserve(
                wl_columnar_memory_governor_ref_get(f.governor), available - 1,
                &peer));
    }
    if (stop == 4) {
        if (kind ==
            3) wl_columnar_relation_test_constructor_fail_allocation = 1;
        else wl_columnar_join_test_attempt_fail_allocation = 0;
    }
    schema_stop_work = work;
    wl_columnar_join_test_schema_checkpoint = stop == 1 ? schema_stop : NULL;
    int rc = 0;
    for (unsigned turn = 0; !done && !rc; ++turn) {
        CHECK(turn < 10000);
        rc = wl_columnar_join_schema_attempt_step(&a, 1, &done);
    }
    wl_columnar_join_test_schema_checkpoint = NULL;
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    wl_columnar_join_test_attempt_fail_allocation = -1;
    if (stop == 2 && !rc) {
        CHECK(done && a.ready && out && a.output_transferred);
        if (work) CHECK(wl_evaluation_control_charge(f.control,
                UINT64_C(1000000000)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        else wl_evaluation_control_request_cancel(f.control);
        rc = wl_columnar_join_schema_attempt_step(&a, 1, &done);
        CHECK(out && a.output_transferred);
    }
    if (stop == 1 || stop == 2)
        CHECK(rc ==
            (work ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED));
    else if (stop == 3) CHECK(rc == ENOSPC && !out);
    else if (stop == 4) CHECK(rc == ENOMEM && !out);
    else CHECK(!rc && done && a.ready && a.output_transferred);
    if (out) {
        CHECK(!out->nrows && out->ncols == width &&
            out->capacity == COL_REL_INIT_CAP
            && !out->timestamps);
        CHECK(out->has_graph_column == (kind == 3));
        if (kind == 3) CHECK(out->graph_col_idx == 1 &&
                out->declared_ncols == 65);
        for (unsigned c = 0; c < width; ++c) {
            wirelog_column_type_t expected = c ==
                0 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
            CHECK(out->column_types[c] == expected &&
                raw->column_types[c] == expected);
            CHECK(!strcmp(out->col_names[c], raw->col_names[c]));
        }
    }
    CHECK(wl_evaluation_control_finish(f.control, &f.owner, rc, 0) == rc);
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(f.control, &before));
    CHECK(wl_columnar_relation_scalar_source_end(&ls, rc) == rc);
    CHECK(wl_columnar_relation_scalar_source_end(&rs, rc) == rc);
    done = false;
    CHECK(wl_columnar_join_schema_attempt_end(&a, f.control, f.id,
        &done) == EINVAL);
    int owner; wl_evaluation_control_t *funding = NULL;
    CHECK(!wl_evaluation_control_create(384, &funding));
    CHECK(!wl_evaluation_control_attach(funding, &owner));
    for (unsigned turn = 0; !done; ++turn) {
        CHECK(turn < 10000 && !wl_evaluation_control_reset(funding));
        CHECK(!wl_evaluation_control_begin(funding, &owner));
        uint64_t id = wl_evaluation_control_active_attempt_id(funding);
        if (!turn) CHECK(wl_columnar_join_schema_attempt_end(&a, funding,
                id + 1, &done) == EINVAL && !done && !a.ending);
        CHECK(!wl_columnar_join_schema_attempt_end(&a, funding, id, &done));
        CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
    }
    CHECK(a.drained && !a.codes && !a.codes_admitted && !a.control_retained);
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    CHECK(!wl_evaluation_control_report(f.control, &after)
        && before.attempt_id == after.attempt_id &&
        before.consumed == after.consumed
        && before.result == after.result &&
        before.stop_reason == after.stop_reason);
    if (out) CHECK(!col_rel_destroy_checked(out));
    CHECK(reserved(&f) == baseline + peer.bytes);
    if (peer.governor) CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(wl_columnar_relation_snapshot_equal(saved_left,
        wl_columnar_relation_snapshot(left))
        && wl_columnar_relation_snapshot_equal(saved_right,
        wl_columnar_relation_snapshot(right)));
    if (stamps) CHECK(left->timestamps[0].iteration == 7 &&
            left->timestamps[0].multiplicity == -2);
    CHECK(!col_rel_destroy_checked(raw) && !col_rel_destroy_checked(left)
        && !col_rel_destroy_checked(right));
    fixture_end(&f, rc);
}

static void
test_schema_preparation(void)
{
    for (unsigned kind = 0; kind < 4; ++kind)
        for (unsigned stamps = 0; stamps < 2; ++stamps) {
            test_schema_case(kind, stamps, 0, false);
            test_schema_case(kind, stamps, 2, false);
            test_schema_case(kind, stamps, 3, false);
            test_schema_case(kind, stamps, 4, false);
        }
    for (unsigned work = 0; work < 2; ++work) {
        test_schema_case(0, true, 1, work);
        test_schema_case(0, true, 2, work);
    }
}

int
main(void)
{
    test_domains();
    test_failures();
    test_preparation();
    test_schema_preparation();
    return 0;
}
