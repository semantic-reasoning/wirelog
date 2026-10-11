/* #1821: ordinary recursive production and original incremental authorities. */
#define WL_TEST_RELATION_RESIZE_HOOK 1
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"
#include <stdio.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); exit(1); } } while (0)

static void destroy(wl_col_session_t *s)
{
    wl_evaluation_control_t *c = s->base.evaluation_control;
    if (c) {
        CHECK(wl_evaluation_control_detach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = NULL;
        wl_evaluation_control_release(c);
    }
    wl_session_destroy(&s->base);
}

/* This is explicitly a real pair-held payload primitive proof, separate from
 * the generated recursive backend acceptance below. */
static void borrowed_reserve(void)
{
    wl_plan_t plan = {0};
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = NULL, *delta = NULL;
    source = col_rel_new_auto("source", 1);
    delta = col_rel_new_auto("delta", 1);
    CHECK(source && delta);
    int64_t value = 7;
    CHECK(col_rel_append_row(source, &value) == 0);
    int64_t second = 3, third = 5;
    CHECK(col_rel_append_row(source, &second) == 0);
    CHECK(col_rel_append_row(source, &third) == 0);
    CHECK(col_rel_enable_timestamps(source) == 0);
    source->timestamps[0].iteration = 7;
    source->timestamps[1].iteration = 3;
    source->timestamps[2].iteration = 5;
    CHECK(col_rel_append_row(delta, &value) == 0);
    CHECK(session_add_rel(s, source) == 0);
    CHECK(session_add_rel(s, delta) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_merge_incremental_scratch_t *scratch = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_incremental_owner(s, frame,
        &authority, &scratch, c) == 0);
    CHECK(wl_columnar_relation_incremental_acquire(authority, source, delta,
        c) == 0);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    CHECK(bulk && !bulk->active);
    uintptr_t original = authority->leases[1].identity;
    uint64_t nonce = authority->leases[1].acquisition_nonce;
    uint64_t generation = delta->storage_generation;
    CHECK(wl_columnar_relation_incremental_reserve_rows(bulk, delta,
        authority, 1, 100, false, c) == 0);
    CHECK(!bulk->active && !bulk->borrowed_mode);
    CHECK(delta->nrows == 1 && delta->capacity == 128
        && delta->columns[0][0] == value);
    CHECK(delta->storage_generation == generation + 1
        && authority->leases[1].identity == original
        && authority->leases[1].acquisition_nonce == nonce
        && col_rel_mutation_lease_validate(&authority->leases[1], delta) == 0);
    bool committed = false;
    int64_t emitted = 11;
    CHECK(wl_columnar_relation_incremental_append_row(authority, 1,
        &emitted, &committed, c) == 0 && committed);
    CHECK(delta->nrows == 2 && delta->columns[0][1] == emitted);
    scratch->lane = 1;
    wl_columnar_memory_reservation_init(&scratch->sort.workspace.admission);
    CHECK(wl_columnar_relation_incremental_radix_prepare(source, 0, 3,
        &scratch->sort.workspace, &authority->leases[0], c) == 0);
    CHECK(wl_columnar_relation_incremental_radix_execute(source, 0, 3,
        &scratch->sort.workspace, authority, 0, bulk, c) == 0);
    CHECK(source->columns[0][0] == 3 && source->columns[0][1] == 5
        && source->columns[0][2] == 7 && source->timestamps[0].iteration == 3
        && source->timestamps[1].iteration == 5 &&
        source->timestamps[2].iteration == 7);
    uint64_t view = source->view_generation;
    uint64_t storage = source->storage_generation;
    CHECK(wl_columnar_relation_incremental_reserve_grid(bulk, source,
        authority, 0, 97, c) == 0);
    CHECK(source->merge_buf_cap == 97 && source->merge_columns
        && source->storage_generation == storage &&
        source->view_generation == view);
    source->merge_columns[0][96] = 1234;
    CHECK(wl_columnar_relation_incremental_reserve_grid(bulk, source,
        authority, 0, 193, c) == 0);
    CHECK(source->merge_buf_cap == 193 && source->merge_columns[0][96] == 1234
        && source->storage_generation == storage &&
        source->view_generation == view
        && col_rel_mutation_lease_validate(&authority->leases[0], source) == 0);
    CHECK(!bulk->active && !bulk->borrowed_mode);
    CHECK(wl_columnar_merge_incremental_complete_storage(authority, scratch,
        bulk, false, c) == 0);
    CHECK(source->timestamps == NULL && source->timestamp_capacity == 0
        && source->storage_generation == storage + 1u
        && !bulk->active && authority->set.terminal_sequence == NULL
        && authority->set.terminal_sequence_consumed
        && col_rel_mutation_lease_validate(&authority->leases[0], source) == 0);
    CHECK(wl_columnar_merge_incremental_retire(authority, scratch, 0) == 0);
    CHECK(!authority->active && !authority->set.identity);
    CHECK(delta->nrows == 2 && delta->columns[0][1] == emitted);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(s);
}

static void adaptive_incremental(uint32_t mode)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = col_rel_new_auto("source", 1);
    col_rel_t *delta = col_rel_new_auto("delta", 1);
    col_rel_t *raw = col_rel_new_auto("raw", 1);
    col_rel_t *raw_delta = col_rel_new_auto("raw_delta", 1);
    CHECK(source && delta && raw && raw_delta);
    uint32_t old = mode == 1 ? 64 : mode == 0 ? 3 : 4;
    for (uint32_t row = 0; row < old; row++) {
        int64_t value = mode == 0 ? row + 1 : 2u * row;
        CHECK(col_rel_append_row(source, &value) == 0);
        CHECK(col_rel_append_row(raw, &value) == 0);
    }
    int64_t fast[] = {5, 4}, small[] = {63, 8}, large[] = {5, 1, 6, 3};
    int64_t *suffix = mode == 0 ? fast : mode == 1 ? small : large;
    uint32_t count = mode == 2 ? 4 : 2;
    for (uint32_t row = 0; row < count; row++) {
        CHECK(col_rel_append_row(source, &suffix[row]) == 0);
        CHECK(col_rel_append_row(raw, &suffix[row]) == 0);
    }
    source->sorted_nrows = raw->sorted_nrows = old;
    source->run_count = raw->run_count = 1;
    source->run_ends[0] = raw->run_ends[0] = old;
    CHECK(session_add_rel(s, source) == 0);
    CHECK(session_add_rel(s, delta) == 0);
    int raw_fast = -1, actual_fast = -1;
    CHECK(col_op_consolidate_incremental_delta(raw, old, raw_delta,
        &raw_fast) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_merge_incremental_scratch_t *scratch = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_incremental_owner(s, frame,
        &authority, &scratch, c) == 0);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    int result = wl_columnar_merge_incremental_controlled(authority, scratch,
            bulk, source, old, delta, &actual_fast, c);
    if (result) fprintf(stderr, "adaptive mode=%u rc=%d\n", mode, result);
    CHECK(result == 0);
    CHECK(actual_fast == raw_fast && actual_fast == (mode == 0));
    CHECK(source->nrows == raw->nrows && delta->nrows == raw_delta->nrows
        && source->capacity == raw->capacity
        && source->merge_buf_cap == raw->merge_buf_cap
        && source->run_count == raw->run_count
        && source->sorted_nrows == raw->sorted_nrows
        && source->view_generation == raw->view_generation
        && source->storage_generation == raw->storage_generation);
    for (uint32_t row = 0; row < source->nrows; row++)
        CHECK(source->columns[0][row] == raw->columns[0][row]);
    for (uint32_t row = 0; row < delta->nrows; row++)
        CHECK(delta->columns[0][row] == raw_delta->columns[0][row]);
    CHECK(!authority->active && !bulk->active && scratch->lane == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(col_rel_destroy_checked(raw) == 0);
    CHECK(col_rel_destroy_checked(raw_delta) == 0);
    destroy(s);
}

/* Each unequal sorted run below is produced by the real raw incremental
 * caller, rather than a fabricated run boundary or copied authority. */
static col_rel_t *multirun_source(const char *name,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *r = NULL;
    CHECK(wl_columnar_relation_new_auto_governed(name, 1, COL_REL_INIT_CAP,
        false, governor, &r) == 0 && r);
    for (uint32_t run = 0; run < COL_MAX_RUNS; run++) {
        uint32_t old = r->nrows;
        uint32_t count = 40u + run;
        for (uint32_t row = 0; row < count; row++) {
            int64_t value = 2 * (int64_t)(old + row);
            CHECK(col_rel_append_row(r, &value) == 0);
        }
        int fast = 0;
        CHECK(col_op_consolidate_incremental_delta(r, old, NULL, &fast) == 0
            && fast == 1 && r->run_count == run + 1u);
    }
    CHECK(r->run_count == COL_MAX_RUNS && r->nrows > 300);
    for (uint32_t run = 1; run < COL_MAX_RUNS; run++)
        CHECK(r->run_ends[run] - r->run_ends[run - 1] == 40u + run);
    return r;
}

static wl_columnar_merge_incremental_authority_t *compact_authority;
static wl_columnar_merge_incremental_scratch_t *compact_scratch;
static uint32_t compact_entries, compact_cells;
static bool compact_cancel;
static void compact_checkpoint(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *rel)
{
    (void)record;
    if (stage == 125) {
        CHECK(progress == COL_MAX_RUNS && compact_authority->active
            && compact_authority->source == rel && compact_scratch->lane == 3
            && compact_scratch->compacted && compact_scratch->admission_active
            && compact_scratch->compaction_admission.identity ==
            (uintptr_t)&compact_scratch->compaction_admission
            && atomic_load_explicit(
                &compact_scratch->compaction_admission.state,
                memory_order_acquire) ==
            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
        compact_entries++;
    }
    if (stage == 126) {
        CHECK(progress == (uint64_t)compact_cells + 1u);
        compact_cells++;
        CHECK(rel->columns[0][progress - 1u] ==
            compact_scratch->compacted[progress - 1u]);
        if (compact_cancel &&
            progress == 257) wl_evaluation_control_request_cancel(c);
    }
}
static void original_multirun_compaction(uint32_t mode, bool stop)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = multirun_source("source", s->memory_governor);
    col_rel_t *raw = multirun_source("raw", s->memory_governor);
    col_rel_t *delta = col_rel_new_auto("delta", 1);
    col_rel_t *raw_delta = col_rel_new_auto("raw_delta", 1);
    CHECK(delta && raw_delta);
    uint32_t old = source->nrows;
    uint32_t count = mode == 2 ? 32u : 2u;
    for (uint32_t row = 0; row < count; row++) {
        int64_t value = mode == 0 ? 2 * (int64_t)(old + row)
            : 2 * (int64_t)row + 1;
        CHECK(col_rel_append_row(source, &value) == 0
            && col_rel_append_row(raw, &value) == 0);
    }
    CHECK(session_add_rel(s, source) == 0 && session_add_rel(s, delta) == 0);
    int raw_fast = -1, actual_fast = -1;
    CHECK(col_op_consolidate_incremental_delta(raw, old, raw_delta,
        &raw_fast) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0
        && wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    uint64_t original_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
    for (uint32_t attempt = 0; attempt < (stop ? 2u : 1u); attempt++) {
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *a = NULL;
        wl_columnar_merge_incremental_scratch_t *scratch = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_incremental_owner(s, frame,
            &a, &scratch, c) == 0);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        compact_authority = a; compact_scratch = scratch;
        compact_entries = compact_cells = 0; compact_cancel = stop && !attempt;
        CHECK(!wl_columnar_consolidate_test_checkpoint);
        wl_columnar_consolidate_test_checkpoint = compact_checkpoint;
        int result = wl_columnar_merge_incremental_controlled(a, scratch, bulk,
                source, old, delta, &actual_fast, c);
        wl_columnar_consolidate_test_checkpoint = NULL;
        if (compact_cancel) {
            CHECK(result == WL_EVALUATION_CONTROL_CANCELLED &&
                compact_entries == 1
                && compact_cells == 257 && !a->active && !bulk->active
                && !scratch->lane && delta->nrows == 0);
            /* The raw-created prefix is globally ordered: actual copied cells
            * match their original values. No physical rollback is assumed. */
            for (uint32_t row = 0; row < old; row++)
                CHECK(source->columns[0][row] == 2 * (int64_t)row);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0) == result);
            CHECK(original_reserved == wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(s->memory_governor)));
            CHECK(wl_evaluation_control_reset(c) == 0);
            continue;
        }
        CHECK(result == 0);
        if (!stop) CHECK(compact_entries == 1 && compact_cells ==
                (mode == 0 ? old + count : old));
        /* A real stop preserves extended fast-run metadata. Fresh retry repairs
         * the old prefix to one run and registers the same suffix as a second.
         * One additional actual sort touch remains; no generation refund. */
        if (stop) CHECK(source->run_count == 2 && source->run_ends[0] == old
                && source->run_ends[1] == source->nrows
                && source->view_generation == raw->view_generation + 1u);
        CHECK(actual_fast == raw_fast && actual_fast == (mode == 0)
            && source->nrows == raw->nrows
            && source->run_count == (stop ? 2u : raw->run_count)
            && source->run_count == (stop || mode == 1 ? 2u : 1u)
            && source->sorted_nrows == raw->sorted_nrows
            && source->capacity == raw->capacity
            && source->merge_buf_cap == raw->merge_buf_cap
            && (stop || source->view_generation == raw->view_generation)
            && source->storage_generation == raw->storage_generation
            && delta->nrows == count && delta->nrows == raw_delta->nrows
            && !a->active && !bulk->active && !scratch->lane);
        for (uint32_t row = 0; row < source->nrows; row++)
            CHECK(source->columns[0][row] == raw->columns[0][row]);
        for (uint32_t row = 0; row < delta->nrows; row++)
            CHECK(delta->columns[0][row] == raw_delta->columns[0][row]);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0, 0)
            == 0);
    }
    if (stop) CHECK(original_reserved == wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(s->memory_governor)));
    compact_authority = NULL; compact_scratch = NULL; compact_cancel = false;
    CHECK(col_rel_destroy_checked(raw) == 0
        && col_rel_destroy_checked(raw_delta) == 0);
    destroy(s);
}

static uint32_t sort_stop_seen;
static wl_columnar_merge_incremental_authority_t *sort_stop_authority;
static wl_columnar_merge_incremental_scratch_t *sort_stop_scratch;
static void suffix_stop(wl_evaluation_control_t *c, int phase, uint64_t visits,
    wl_columnar_consolidate_record_t *unused, col_rel_t *r)
{
    (void)unused;
    if (phase == 9 && visits == 257) {
        CHECK(!sort_stop_seen++ && sort_stop_authority->active
            && sort_stop_authority->set.owners_acquired
            && sort_stop_scratch->lane == 1
            && sort_stop_scratch->sort.workspace.perm_a
            && sort_stop_authority->source == r);
        wl_evaluation_control_request_cancel(c);
    }
}

static void suffix_stop_retry(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = col_rel_new_auto("source", 1);
    col_rel_t *delta = col_rel_new_auto("delta", 1);
    col_rel_t *raw = col_rel_new_auto("raw", 1);
    col_rel_t *raw_delta = col_rel_new_auto("raw_delta", 1);
    CHECK(source && delta && raw && raw_delta);
    for (uint32_t row = 0; row <= 300; row++) {
        int64_t value = row ? 301u - row : 0;
        CHECK(col_rel_append_row(source, &value) == 0);
        CHECK(col_rel_append_row(raw, &value) == 0);
    }
    source->sorted_nrows = raw->sorted_nrows = 1;
    source->run_count = raw->run_count = 1;
    source->run_ends[0] = raw->run_ends[0] = 1;
    CHECK(session_add_rel(s, source) == 0 && session_add_rel(s, delta) == 0);
    CHECK(col_op_consolidate_incremental_delta(raw, 1, raw_delta, NULL) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    for (uint32_t attempt = 0; attempt < 2; attempt++) {
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_merge_incremental_scratch_t *scratch = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_incremental_owner(s, frame,
            &authority, &scratch, c) == 0);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        sort_stop_authority = authority; sort_stop_scratch = scratch;
        wl_columnar_consolidate_test_checkpoint = attempt ? NULL : suffix_stop;
        int result = wl_columnar_merge_incremental_controlled(authority,
                scratch,
                bulk, source, 1, delta, NULL, c);
        wl_columnar_consolidate_test_checkpoint = NULL;
        CHECK(result == (attempt ? 0 : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(!authority->active && !bulk->active && scratch->lane == 0);
        if (!attempt) {
            CHECK(sort_stop_seen == 1 && delta->nrows == 0 &&
                source->nrows == 301);
            for (uint32_t row = 0; row <= 300; row++)
                CHECK(source->columns[0][row] ==
                    (int64_t)(row ? 301u - row : 0));
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == result);
        if (!attempt) CHECK(wl_evaluation_control_reset(c) == 0);
    }
    CHECK(source->nrows == raw->nrows && delta->nrows == raw_delta->nrows);
    for (uint32_t row = 0; row < source->nrows; row++)
        CHECK(source->columns[0][row] == raw->columns[0][row]);
    for (uint32_t row = 0; row < delta->nrows; row++)
        CHECK(delta->columns[0][row] == raw_delta->columns[0][row]);
    CHECK(col_rel_destroy_checked(raw) == 0 &&
        col_rel_destroy_checked(raw_delta) == 0);
    sort_stop_authority = NULL; sort_stop_scratch = NULL;
    destroy(s);
}

static wl_columnar_memory_reservation_t *transfer_expected;
static const void *transfer_new_owner;
static unsigned transfer_seen;
static void transfer_tail_stop(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (token != transfer_expected
        || phase != WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED) return;
    CHECK(!transfer_seen++ && token->identity == token);
    CHECK(atomic_load_explicit(&token->owner_bits, memory_order_acquire)
        == (uint64_t)(uintptr_t)transfer_new_owner);
    CHECK(atomic_load_explicit(&token->state, memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_TRANSFERRING);
    wl_evaluation_control_request_cancel(control);
}

/* Source-derived uncontended transfer: validation/load2 + claim/terminal4.
 * The last case stops only after the original owner transition is consumed. */
static void original_transfer(void)
{
    for (unsigned committed = 0; committed < 2; committed++) {
        for (unsigned mode = 0; mode < 4; mode++) {
            wl_plan_t plan = {0}; wl_session_t *base = NULL;
            CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
                &base) == 0);
            wl_col_session_t *s = COL_SESSION(base);
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = 1024, .usable_bytes = 1024,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
                .status = WL_COLUMNAR_MEMORY_OK
            };
            wl_columnar_memory_governor_t governor;
            CHECK(wl_columnar_memory_governor_init(&governor,
                &resolution) == 0);
            wl_columnar_memory_reservation_t token;
            wl_columnar_memory_reservation_init(&token);
            CHECK(wl_columnar_memory_reserve(&governor, 64, &token));
            int original_owner, new_owner;
            CHECK(wl_columnar_memory_transfer(&token, &original_owner));
            if (committed) CHECK(wl_columnar_memory_commit(&token,
                    &original_owner));
            wl_evaluation_control_t *c = NULL;
            CHECK(wl_evaluation_control_create(mode == 1 ? 6 : mode ==
                2 ? 5 : 0,
                &c) == 0);
            CHECK(wl_evaluation_control_attach(c,
                s->base.operation_admission) == 0);
            s->base.evaluation_control = c;
            CHECK(wl_evaluation_control_begin(c,
                s->base.operation_admission) == 0);
            transfer_expected = &token; transfer_new_owner = &new_owner;
            transfer_seen = 0;
            wl_columnar_memory_governor_test_checkpoint = mode == 3
                ? transfer_tail_stop : NULL;
            bool transferred = false;
            int rc = wl_columnar_memory_governor_transfer_controlled(&token,
                    &new_owner, &transferred, c);
            wl_columnar_memory_governor_test_checkpoint = NULL;
            CHECK(rc == (mode == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : mode == 3 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
            CHECK(transferred == (mode != 2));
            CHECK(token.identity == &token && token.bytes == 64
                && wl_columnar_memory_reserved(&governor) == 64);
            CHECK(atomic_load_explicit(&token.state, memory_order_acquire)
                == (committed ? WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                    : WL_COLUMNAR_MEMORY_RESERVATION_RESERVED));
            CHECK(atomic_load_explicit(&token.owner_bits, memory_order_acquire)
                == (uint64_t)(uintptr_t)(mode ==
                2 ? &original_owner : &new_owner));
            CHECK(transfer_seen == (mode == 3));
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0) == rc);
            /* Checked mandatory reverse consumes the original token without
             * fresh work, even after cancellation. No token copy or reset. */
            bool reversed = false;
            CHECK(wl_columnar_memory_governor_transfer_controlled(&token,
                &original_owner, &reversed, NULL) == 0 && reversed);
            CHECK(committed ? wl_columnar_memory_release(&token)
                : wl_columnar_memory_rollback(&token));
            CHECK(wl_columnar_memory_reserved(&governor) == 0);
            destroy(s);
        }
    }
    transfer_expected = NULL; transfer_new_owner = NULL;
}

static void original_stratum_payload(void)
{
    wl_plan_relation_t relation = {0};
    wl_plan_stratum_t stratum = {.is_recursive = true,
                                 .relation_count = 1, .relations = &relation};
    wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    wl_evaluation_control_t *c = NULL;
    col_rel_t *source = col_rel_new_auto("clone_source", 1);
    CHECK(source && session_add_rel(s, source) == 0);
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
    CHECK(s->recursive.active && s->recursive.ready && s->recursive.snap
        && s->recursive.delta_rels && !s->recursive.delta_rels[0]
        && s->recursive.reservation.identity == &s->recursive.reservation);
    CHECK(s->recursive.reservation.bytes ==
        sizeof(col_rel_t *) + sizeof(uint32_t));
    CHECK(atomic_load_explicit(&s->recursive.reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
    CHECK(wl_columnar_eval_serial_recursive_clone(s, source, "$d$clone_source",
        0, c) == 0);
    CHECK(s->recursive.delta_rels[0] && s->recursive.delta_rels[0]->pool_owned
        && s->recursive.delta_rels[0]->nrows == 0
        && s->recursive.delta_rels[0]->ncols == 1
        && s->recursive.delta_rels[0]->timestamps
        && s->recursive.delta_rels[0]->timestamp_capacity == 64
        && !s->cleanup_active && !s->cleanup_pending);
    const void *identity = s->recursive.reservation.identity;
    s->recursive.reservation.identity = NULL;
    CHECK(wl_columnar_eval_serial_recursive_retire(s, EOVERFLOW,
        NULL) == EOVERFLOW);
    CHECK(s->recursive.active && s->recursive.cleanup_pending
        && !s->recursive.snap && !s->recursive.delta_rels
        && s->recursive.reservation.bytes ==
        sizeof(col_rel_t *) + sizeof(uint32_t));
    /* Restore the single corrupted scalar only after actual serialized return.
     * The original RESERVED credit is still at its real session address. */
    s->recursive.reservation.identity = identity;
    CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
    CHECK(!s->recursive.active && !s->recursive.cleanup_pending
        && !s->recursive.reservation.bytes &&
        !s->recursive.reservation.governor);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(s);
}

static void original_timestamp_payload(void)
{
    for (unsigned fail = 0; fail < 2; fail++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *source = NULL, *raw = NULL;
        CHECK(wl_columnar_relation_new_auto_governed("source", 1, 64, false,
            s->memory_governor, &source) == 0);
        CHECK(wl_columnar_relation_new_auto_governed("raw", 1, 64, false,
            s->memory_governor, &raw) == 0);
        int64_t value = 7;
        CHECK(col_rel_append_row(source, &value) == 0
            && col_rel_append_row(raw, &value) == 0);
        CHECK(session_add_rel(s, source) == 0);
        uint64_t prior = source->storage_generation;
        uint64_t credit = source->retained_reserved_bytes;
        if (fail) wl_columnar_relation_test_fail_next_governed_timestamp_alloc();
        CHECK(col_rel_enable_timestamps(raw) == (fail ? ENOMEM : 0));
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_merge_incremental_scratch_t *scratch = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_incremental_owner(s, frame,
            &authority, &scratch, c) == 0);
        CHECK(wl_columnar_relation_incremental_acquire(authority, source, NULL,
            c) == 0);
        uintptr_t identity = authority->leases[0].identity;
        uint64_t nonce = authority->leases[0].acquisition_nonce;
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        if (fail) wl_columnar_relation_test_fail_next_governed_timestamp_alloc();
        CHECK(wl_columnar_relation_incremental_timestamps(bulk, authority, 0, c)
            == (fail ? ENOMEM : 0));
        CHECK(!bulk->active && !bulk->borrowed_mode
            && authority->leases[0].identity == identity
            && authority->leases[0].acquisition_nonce == nonce
            && col_rel_mutation_lease_validate(&authority->leases[0],
            source) == 0);
        CHECK(source->storage_generation == prior + !fail
            && source->timestamp_capacity == raw->timestamp_capacity
            && source->retained_reserved_bytes == raw->retained_reserved_bytes);
        if (fail) CHECK(!source->timestamps &&
                source->retained_reserved_bytes == credit);
        else {
            CHECK(source->timestamps && source->timestamp_capacity == 64);
            for (uint32_t row = 0; row < 64; row++)
                CHECK(memcmp(&source->timestamps[row], &raw->timestamps[row],
                    sizeof(col_delta_timestamp_t)) == 0);
            /* Timestamp allocation already consumed this original lease's
            * storage transition. Stamping acquires a fresh real epoch. */
            CHECK(wl_columnar_relation_incremental_finish(authority, 0) == 0);
            CHECK(wl_columnar_relation_incremental_acquire(authority, source,
                NULL, c) == 0);
            uint64_t stamp_generation = source->storage_generation;
            CHECK(wl_columnar_relation_incremental_stamp(authority, 0, 17, 3,
                c) == 0);
            CHECK(source->timestamps[0].iteration == 17
                && source->timestamps[0].stratum == 3
                && source->timestamps[0].multiplicity == 1
                && source->timestamps[0].worker == 0
                && source->storage_generation == stamp_generation + 1u
                && authority->cursor == 1 && authority->consumed
                && col_rel_mutation_lease_validate(&authority->leases[0],
                source) == 0);
        }
        CHECK(wl_columnar_merge_incremental_retire(authority, scratch, 0) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(col_rel_destroy_checked(raw) == 0);
        destroy(s);
    }
}

/* Supplied immutable operator selection proofs; these do not assert a
 * generated backend fixed point or pretend that host-set iteration is one. */
static void original_delta_selection(void)
{
    for (unsigned mode = 0; mode < 8; mode++) {
        wl_plan_relation_t relation = {0};
        wl_plan_stratum_t stratum = {.is_recursive = true,
                                     .relation_count = 1,
                                     .relations = &relation};
        wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
        wl_col_session_t *sessions[2];
        for (unsigned which = 0; which < 2; which++) {
            wl_session_t *base = NULL;
            CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
                &base) == 0);
            sessions[which] = COL_SESSION(base);
            col_rel_t *full = col_rel_new_auto("A", 1);
            CHECK(full && session_add_rel(sessions[which], full) == 0);
            for (int64_t value = 1; value <= 3; value++)
                CHECK(col_rel_append_row(full, &value) == 0);
            if (mode < 4) {
                col_rel_t *delta = col_rel_new_auto("$d$A", 1);
                int64_t value = 3;
                CHECK(delta && col_rel_append_row(delta, &value) == 0
                    && session_add_rel(sessions[which], delta) == 0);
            }
            sessions[which]->current_iteration = mode == 5 ? 0 : 1;
        }
        wl_plan_op_t op = {.op = WL_PLAN_OP_VARIABLE, .relation_name = "A",
                           .delta_mode = mode == 0 ? WL_DELTA_AUTO : mode ==
                               1 ? WL_DELTA_FORCE_FULL
                : mode == 2 ? WL_DELTA_FORCE_EMPTY : WL_DELTA_FORCE_DELTA};
        eval_stack_t raw;
        eval_stack_init(&raw);
        CHECK(col_op_variable(&op, &raw, sessions[0]) == 0 && raw.top == 1);
        wl_col_session_t *s = sessions[1]; wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        CHECK(col_op_variable(&op, stack, s) == 0 && stack->top == 1);
        CHECK(stack->items[0].is_delta == raw.items[0].is_delta
            && stack->items[0].owned == raw.items[0].owned
            && stack->items[0].rel->nrows == raw.items[0].rel->nrows
            && stack->items[0].rel->ncols == raw.items[0].rel->ncols);
        for (uint32_t row = 0; row < raw.items[0].rel->nrows; row++)
            CHECK(stack->items[0].rel->columns[0][row]
                == raw.items[0].rel->columns[0][row]);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(eval_stack_drain(&raw) == 0);
        destroy(sessions[0]); destroy(sessions[1]);
    }
}

static eval_entry_t *descriptor_result;
static wl_columnar_session_promotion_scratch_t *descriptor_state;
static const void *descriptor_saved_identity;
static unsigned descriptor_mode, descriptor_seen;
static void descriptor_checkpoint(wl_evaluation_control_t *c, int phase,
    uint64_t progress)
{
    if (descriptor_mode == 5 && phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME
        && progress == 128) {
        CHECK(!descriptor_seen++ && descriptor_result->rel
            && !descriptor_state->descriptor_ready);
        wl_evaluation_control_request_cancel(c);
    }
    bool fault = descriptor_mode == 6
        ? phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_BEFORE_COMMIT
        : descriptor_mode == 7 &&
        phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED;
    if (fault) {
        CHECK(!descriptor_seen++ && descriptor_state->descriptor_moved);
        wl_columnar_memory_reservation_t *token =
            &descriptor_result->rel->descriptor_reservation;
        CHECK(token->identity == token);
        descriptor_saved_identity = token->identity;
        token->identity = NULL;
    }
}

static void original_descriptor_preparation(void)
{
    char long_name[301];
    memset(long_name, 'x', sizeof(long_name) - 1u);
    long_name[sizeof(long_name) - 1u] = '\0';
    for (unsigned mode = 0; mode < 8; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
        const char *name = mode == 0 ? NULL : mode == 1 ? "" : long_name;
        col_rel_t *raw = NULL;
        if (mode < 3) {
            CHECK(wl_columnar_relation_alloc_governed(&raw, name,
                s->memory_governor) == 0);
        }
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_session_promotion_scratch_t *state = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_promotion_owner(s, frame,
            &authority, &state, c) == 0);
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        descriptor_result = result; descriptor_state = state;
        descriptor_mode = mode; descriptor_seen = 0;
        descriptor_saved_identity = NULL;
        wl_columnar_relation_test_constructor_checkpoint =
            descriptor_checkpoint;
        wl_columnar_relation_test_constructor_fail_allocation = mode == 3 ? 0
            : mode == 4 ? 1 : -1;
        int rc = wl_columnar_relation_descriptor_prepare(bulk, state, result,
                name, s->memory_governor, c);
        wl_columnar_relation_test_constructor_checkpoint = NULL;
        int expected = mode < 3 ? 0 : mode < 5 ? ENOMEM
            : mode == 5 ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL;
        if (rc != expected) fprintf(stderr,
                "descriptor mode=%u rc=%d expected=%d phase=%u moved=%d committed=%d seen=%u\n",
                mode, rc, expected, state->phase, state->descriptor_moved,
                state->descriptor_committed, descriptor_seen);
        CHECK(rc == (mode < 3 ? 0 : mode < 5 ? ENOMEM
            : mode == 5 ? WL_EVALUATION_CONTROL_CANCELLED : EINVAL));
        if (mode < 3) {
            CHECK(result->rel && state->descriptor_ready &&
                state->descriptor_committed
                && result->rel->descriptor_reserved_bytes ==
                raw->descriptor_reserved_bytes
                && result->rel->ncols == 0 && !result->rel->schema_ok
                && !result->rel->columns && !result->rel->col_names
                && !result->rel->timestamps && !result->rel->row_scratch
                && result->rel->storage_owner == result->rel);
            if (name) CHECK(strcmp(result->rel->name, raw->name) == 0);
            else CHECK(!result->rel->name && !raw->name);
        }
        col_rel_t *retained = mode >= 6 ? result->rel : NULL;
        if (retained) {
            CHECK(descriptor_seen == 1 && state->descriptor_moved
                && retained->descriptor_reservation.bytes ==
                sizeof(col_rel_t) + sizeof(long_name)
                && !retained->descriptor_reservation.identity);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL &&
                !frame
                && s->cleanup_pending && s->cleanup_pending_count == 1);
            /* Serialized return occurred with the real original token and
             * physical candidate retained; restore only its saved scalar. */
            retained->descriptor_reservation.identity =
                descriptor_saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0
                && !s->cleanup_pending && !s->cleanup_pending_count);
        } else CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
            rc > 0 ? rc : 0, 0) == rc);
        if (raw) CHECK(col_rel_destroy_checked(raw) == 0);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                s->memory_governor)) == baseline && !s->cleanup_reserved_bytes);
        destroy(s);
    }
    descriptor_result = NULL; descriptor_state = NULL;
}

/* This reaches actual pooled source and original credit transport, separately
 * from the still-closed generated recursive backend admission. */
static wl_columnar_merge_incremental_authority_t *stamp_authority;
static unsigned stamp_seen;
static void stamp_stop(wl_evaluation_control_t *control, uint64_t work)
{
    if (work != 32 || stamp_authority->cursor != 257) return;
    CHECK(!stamp_seen++ && stamp_authority->active &&
        stamp_authority->consumed);
    CHECK(stamp_authority->source->timestamps[256].iteration == 19);
    wl_evaluation_control_request_cancel(control);
}

static void original_stamp_stop_retry(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("stamp_source", 1, 512, true,
        s->memory_governor, &source) == 0);
    for (uint32_t row = 0; row < 303; row++) {
        int64_t value = row;
        CHECK(col_rel_append_row(source, &value) == 0);
    }
    CHECK(session_add_rel(s, source) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c; stamp_seen = 0;
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_merge_incremental_scratch_t *scratch = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_incremental_owner(s, frame,
            &authority, &scratch, c) == 0);
        CHECK(wl_columnar_relation_incremental_acquire(authority, source, NULL,
            c) == 0);
        stamp_authority = authority;
        wl_columnar_relation_test_work_checkpoint = attempt ? NULL : stamp_stop;
        int rc = wl_columnar_relation_incremental_stamp(authority, 0, 19, 4, c);
        wl_columnar_relation_test_work_checkpoint = NULL;
        CHECK(rc == (attempt ? 0 : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(authority->cursor == (attempt ? 303u : 257u)
            && authority->active && authority->consumed && source->nrows == 303
            && col_rel_mutation_lease_validate(&authority->leases[0],
            source) == 0);
        for (uint32_t row = 0; row < 303; row++) {
            CHECK(source->columns[0][row] == (int64_t)row);
            if (attempt ||
                row < 257) CHECK(source->timestamps[row].iteration == 19
                    && source->timestamps[row].stratum == 4
                    && source->timestamps[row].multiplicity == 1);
            else CHECK(source->timestamps[row].iteration == 0);
        }
        CHECK(wl_columnar_relation_incremental_finish(authority, rc) == rc);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == rc);
        if (!attempt) CHECK(wl_evaluation_control_reset(c) == 0);
    }
    CHECK(stamp_seen == 1); stamp_authority = NULL;
    destroy(s);
}

static unsigned promotion_stage_seen;
static void promotion_stage_stop(wl_evaluation_control_t *control,
    wl_columnar_session_promotion_scratch_t *state, eval_entry_t *result)
{
    CHECK(state->phase == 6 && state->cursor == 128 && !state->payload_owned
        && state->source && result->rel->columns == state->source->columns);
    CHECK(!promotion_stage_seen++);
    wl_evaluation_control_request_cancel(control);
}

static void original_promotion_credit_transport(void)
{
    for (unsigned mode = 0; mode < 4; mode++) {
        wl_plan_relation_t relation = {0};
        wl_plan_stratum_t stratum = {.is_recursive = true,
                                     .relation_count = 1,
                                     .relations = &relation};
        wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *input = col_rel_new_auto("promotion_source", 1);
        CHECK(input && session_add_rel(s, input) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        CHECK(wl_columnar_eval_serial_recursive_clone(s, input,
            "$d$promotion_source",
            0, c) == 0);
        col_rel_t *source = s->recursive.delta_rels[0];
        CHECK(source && source->pool_owned &&
            source->memory_governor == s->memory_governor);
        uint64_t metadata = source->metadata_reserved_bytes;
        uint64_t retained = source->retained_reserved_bytes;
        uint64_t name = source->pool_name_reserved_bytes;
        CHECK(metadata && retained && name);
        int64_t **columns = source->columns;
        char *original_name = source->name;
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_session_promotion_scratch_t *state = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_promotion_owner(s, frame,
            &authority, &state, c) == 0);
        CHECK(wl_columnar_relation_incremental_acquire(authority, source, NULL,
            c) == 0);
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(wl_columnar_relation_descriptor_prepare(bulk, state, result,
            source->name, s->memory_governor, c) == 0);
        transfer_expected = &source->pool_name_reservation;
        transfer_new_owner = result->rel; transfer_seen = 0;
        wl_columnar_memory_governor_test_checkpoint = mode ==
            1 ? transfer_tail_stop : NULL;
        int rc = wl_columnar_session_promotion_move_credits(authority, state,
                result, c);
        wl_columnar_memory_governor_test_checkpoint = NULL;
        if (rc != (mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED : 0))
            fprintf(stderr,
                "promotion mode=%u rc=%d phase=%u moves=%u mask=%u source_owner=%d\n",
                mode, rc, authority->phase, state->move_index,
                state->moved_mask,
                source->storage_owner == source);
        CHECK(rc == (mode == 1 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
        CHECK(source->columns == columns && source->name == original_name
            && source->pool_owned && authority->active &&
            !state->payload_owned);
        if (mode == 1) CHECK(transfer_seen == 1 && state->move_index == 2
                && state->owner_transferred && state->moved_mask == 3);
        else CHECK(state->move_index == 4 && state->moved_mask == 7);
        col_rel_t *candidate = result->rel;
        if (mode != 1) {
            uint64_t named_credit = candidate->descriptor_reserved_bytes;
            promotion_stage_seen = 0;
            wl_columnar_session_test_promotion_checkpoint = mode == 3
                ? promotion_stage_stop : NULL;
            rc = wl_columnar_session_promotion_stage_payload(authority, state,
                    result, c);
            wl_columnar_session_test_promotion_checkpoint = NULL;
            CHECK(rc == (mode == 3 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
            CHECK(state->phase == (mode == 3 ? 6 : 7) && !state->payload_owned
                && candidate->columns == columns &&
                candidate->name == original_name
                && candidate->descriptor_reserved_bytes == named_credit
                && named_credit ==
                sizeof(col_rel_t) + strlen(original_name) + 1u
                && candidate->retained_reserved_bytes == retained);
            CHECK(promotion_stage_seen == (mode == 3));
        }
        const void *saved_identity = candidate->retained_reservation.identity;
        if (mode == 2) {
            candidate->retained_reservation.identity = NULL;
            CHECK(wl_columnar_session_promotion_retire(s, authority, state,
                bulk,
                result, EOVERFLOW) == EOVERFLOW);
            CHECK(result->rel == candidate && authority->active &&
                state->moved_mask);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL
                && !frame && s->cleanup_pending);
            candidate->retained_reservation.identity = saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0 &&
                !s->cleanup_pending);
        } else {
            CHECK(wl_columnar_session_promotion_retire(s, authority, state,
                bulk,
                result, rc) == rc);
            CHECK(!result->rel && !authority->active && !state->moved_mask);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        }
        CHECK(source->columns == columns && source->name == original_name
            && source->metadata_reserved_bytes == metadata
            && source->retained_reserved_bytes == retained
            && source->pool_name_reserved_bytes == name);
        CHECK(atomic_load_explicit(&source->retained_reservation.owner_bits,
            memory_order_acquire) == (uintptr_t)source);
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
            0, 0) == rc);
        destroy(s);
    }
    transfer_expected = NULL; transfer_new_owner = NULL;
}

static void original_recursive_consolidation_caller(void)
{
    wl_plan_relation_t relation = {.name = "A", .delta_name = "$d$A"};
    wl_plan_stratum_t stratum = {.is_recursive = true,
                                 .relation_count = 1, .relations = &relation};
    wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
    wl_col_session_t *sessions[2]; col_rel_t *heads[2];
    const int64_t values[4] = {1, 3, 2, 3};
    for (unsigned which = 0; which < 2; which++) {
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        sessions[which] = COL_SESSION(base);
        heads[which] = col_rel_new_auto("A", 1);
        CHECK(heads[which] && session_add_rel(sessions[which],
            heads[which]) == 0);
        for (unsigned row = 0; row < 4;
            row++) CHECK(col_rel_append_row(heads[which], &values[row]) == 0);
    }
    col_rel_t *raw_delta = NULL; wl_col_session_t *raw = sessions[0];
    CHECK(wl_columnar_relation_pool_new_like_governed_checked(&raw_delta,
        raw->delta_pool, "$d$A", heads[0], raw->memory_governor) == 0);
    CHECK(col_rel_enable_timestamps(raw_delta) == 0);
    int raw_fast = -1;
    CHECK(col_op_consolidate_incremental_delta(heads[0], 2, raw_delta,
        &raw_fast) == 0);
    wl_columnar_relation_touch_storage(raw_delta);
    for (uint32_t row = 0; row < raw_delta->nrows; row++) {
        raw_delta->timestamps[row].iteration = 7;
        raw_delta->timestamps[row].stratum = 0;
        raw_delta->timestamps[row].multiplicity = 1;
    }
    CHECK(col_rel_enable_timestamps(heads[0]) == 0);
    wl_col_session_t *s = sessions[1]; wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
    s->recursive.effective_iteration = 7;
    int fast = -1;
    int rc = wl_columnar_eval_serial_recursive_consolidate(s, heads[1], 0, 2,
            &fast, c);
    if (rc) fprintf(stderr, "recursive consolidate caller rc=%d\n", rc);
    CHECK(rc == 0 && fast == raw_fast);
    col_rel_t *delta = s->recursive.delta_rels[0];
    CHECK(delta && delta->pool_owned && delta->nrows == 1 &&
        heads[1]->nrows == 3
        && delta->columns[0][0] == 2 && raw_delta->columns[0][0] == 2);
    CHECK(delta->timestamp_capacity == raw_delta->timestamp_capacity
        && heads[1]->timestamp_capacity == heads[0]->timestamp_capacity);
    CHECK(memcmp(&delta->timestamps[0], &raw_delta->timestamps[0],
        sizeof(col_delta_timestamp_t)) == 0);
    for (uint32_t row = 0; row < 3; row++) CHECK(heads[1]->columns[0][row]
            == heads[0]->columns[0][row]);
    CHECK(col_rel_destroy_checked(raw_delta) == 0);
    CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(raw); destroy(s);
}

/* Real supplied AUTO JOIN callers prove the raw delta heuristic and timestamp
 * metadata boundary separately from generated fixed-point admission. */
static void original_delta_join(void)
{
    for (unsigned left_delta = 0; left_delta < 2; left_delta++) {
        wl_plan_relation_t relation = {0};
        wl_plan_stratum_t stratum = {.is_recursive = true,
                                     .relation_count = 1,
                                     .relations = &relation};
        wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
        wl_col_session_t *sessions[2];
        for (unsigned which = 0; which < 2; which++) {
            wl_session_t *base = NULL;
            CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
                &base) == 0);
            sessions[which] = COL_SESSION(base);
            const char *names[4] = {"A", "B", "$d$A", "$d$B"};
            for (unsigned i = 0; i < 4; i++) {
                col_rel_t *r = col_rel_new_auto(names[i], 1);
                CHECK(r && session_add_rel(sessions[which], r) == 0);
                if (i < 2) {
                    for (int64_t v = 1; v <= 3;
                        v++) CHECK(col_rel_append_row(r, &v) == 0);
                } else {
                    int64_t v = i == 2 ? 3 : 2;
                    CHECK(col_rel_append_row(r, &v) == 0);
                }
                CHECK(col_rel_enable_timestamps(r) == 0);
            }
            sessions[which]->current_iteration = 1;
        }
        const char *keys[1] = {"col0"};
        wl_plan_op_t variable = {.op = WL_PLAN_OP_VARIABLE,
                                 .relation_name = "A",
                                 .delta_mode =
                                     left_delta ? WL_DELTA_FORCE_DELTA :
                                     WL_DELTA_FORCE_FULL};
        wl_plan_op_t join = {.op = WL_PLAN_OP_JOIN, .right_relation = "B",
                             .delta_mode = WL_DELTA_AUTO, .key_count = 1,
                             .left_keys = keys, .right_keys = keys};
        eval_stack_t raw; eval_stack_init(&raw);
        CHECK(col_op_variable(&variable, &raw, sessions[0]) == 0);
        CHECK(wl_columnar_join_op(&join, &raw,
            sessions[0]) == 0 && raw.top == 1);
        wl_col_session_t *s = sessions[1]; wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        CHECK(col_op_variable(&variable, stack, s) == 0);
        int rc = wl_columnar_join_op(&join, stack, s);
        if (rc) fprintf(stderr, "delta JOIN left=%u rc=%d\n", left_delta, rc);
        CHECK(rc == 0 && stack->top == 1);
        CHECK(raw.items[0].rel->nrows == 1 && raw.items[0].rel->ncols == 2
            && stack->items[0].rel->nrows == 1 &&
            stack->items[0].rel->ncols == 2
            && raw.items[0].is_delta && stack->items[0].is_delta
            && !stack->items[0].rel->timestamps);
        for (unsigned col = 0; col < 2;
            col++) CHECK(stack->items[0].rel->columns[col][0]
                == (left_delta ? 3 : 2) &&
                raw.items[0].rel->columns[col][0] == (left_delta ? 3 : 2));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(eval_stack_drain(&raw) == 0);
        destroy(sessions[0]); destroy(sessions[1]);
    }
}

static wl_col_session_t *registry_stop_session;
static wl_columnar_source_access_reader_t registry_stop_reader;
static bool registry_stop_hold, registry_stop_cancel, registry_name_fault;
static const void *registry_saved_name_identity;
static unsigned registry_stop_seen;
static void registry_publication_stop(wl_evaluation_control_t *control,
    wl_columnar_session_promotion_scratch_t *state, eval_entry_t *result)
{
    if (state->phase || result->rel || !registry_stop_session
        || registry_stop_session->recursive.phase !=
        WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_PROMOTED) return;
    CHECK(!registry_stop_seen++);
    col_rel_t *candidate = registry_stop_session->recursive.delta_rels[0];
    CHECK(candidate && !candidate->pool_owned);
    if (registry_stop_hold)
        CHECK(wl_columnar_source_access_reader_acquire(
                &candidate->source_access,
                &registry_stop_reader) == 0);
    if (registry_name_fault) {
        registry_saved_name_identity =
            candidate->pool_name_reservation.identity;
        candidate->pool_name_reservation.identity = NULL;
    }
    if (registry_stop_cancel) wl_evaluation_control_request_cancel(control);
}

static wl_columnar_memory_reservation_t *registry_fault_token;
static const void *registry_fault_identity;
static unsigned registry_admissions;
static void registry_credit_fault(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (!registry_stop_session || registry_stop_session->serial_completion.mode
        != WL_COLUMNAR_SERIAL_FRAME_REGISTRY
        || phase != WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED) return;
    if (!registry_admissions++) {
        registry_fault_token = token; registry_fault_identity = token->identity;
        CHECK(registry_fault_identity == token && token->bytes);
        return;
    }
    CHECK(registry_admissions == 2 && token != registry_fault_token
        && registry_fault_token->identity == registry_fault_token);
    /* The first original reservation already owns the real relation image;
     * stop after the second reserve, then actual retirement frees that image
     * before refusing this original scalar identity. */
    registry_fault_token->identity = NULL;
    wl_evaluation_control_request_cancel(control);
}

static void original_promotion_registry(void)
{
    for (unsigned trial = 0; trial < 14; trial++) {
        unsigned collision = trial % 7; bool after_reset = trial >= 7;
        wl_plan_relation_t relation = {0};
        wl_plan_stratum_t stratum = {.is_recursive = true,
                                     .relation_count = 1,
                                     .relations = &relation};
        wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *input = col_rel_new_auto("registry_source", 1);
        CHECK(input && session_add_rel(s, input) == 0);
        if (collision == 1 || collision == 5) {
            col_rel_t *existing = col_rel_new_auto("$d$registry_source", 1);
            CHECK(existing && session_add_rel(s, existing) == 0);
        }
        uint32_t registered_before = s->nrels;
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        CHECK(wl_columnar_eval_serial_recursive_clone(s, input,
            "$d$registry_source", 0, c) == 0);
        col_rel_t *source = s->recursive.delta_rels[0];
        if (after_reset) {
            CHECK(wl_columnar_eval_serial_recursive_pool_reset(s, c) == 0
                && s->delta_pool->slot_used == 0 && s->recursive.reset_active
                && s->recursive.reset_high_water > 0);
        }
        int64_t **columns = source->columns;
        uint64_t retained = source->retained_reserved_bytes;
        registry_stop_session = s;
        registry_stop_hold = collision == 2 || collision == 5;
        registry_stop_cancel = collision == 2 || collision == 3;
        registry_name_fault = collision == 4;
        registry_saved_name_identity = NULL; registry_stop_seen = 0;
        wl_columnar_session_test_promotion_checkpoint = collision >= 2
            ? registry_publication_stop : NULL;
        registry_fault_token = NULL; registry_fault_identity = NULL;
        registry_admissions = 0;
        wl_columnar_memory_governor_test_checkpoint = collision == 6
            ? registry_credit_fault : NULL;
        int rc = wl_columnar_eval_serial_recursive_register_delta(s, 0, c);
        wl_columnar_session_test_promotion_checkpoint = NULL;
        wl_columnar_memory_governor_test_checkpoint = NULL;
        int expected = collision == 1 ||
            collision == 5 ? EEXIST : collision == 2 ? EBUSY
            : collision == 3 ? WL_EVALUATION_CONTROL_CANCELLED : collision ==
            4 || collision == 6 ? EINVAL : 0;
        if (rc != expected)
            fprintf(stderr,
                "registry collision=%u rc=%d phase=%u pending=%d mode=%u\n",
                collision, rc, s->recursive.phase, s->cleanup_pending != NULL,
                s->serial_completion.mode);
        CHECK(rc == expected);
        CHECK(registry_stop_seen == (collision >= 2));
        if (collision == 2 || collision == 5) {
            CHECK(s->cleanup_pending && s->recursive.phase ==
                WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTRY_FAILED);
            wl_columnar_eval_stack_cleanup_frame_t *pending =
                s->cleanup_pending;
            CHECK(s->serial_completion.mode_frame == pending
                && s->serial_completion.mode ==
                WL_COLUMNAR_SERIAL_FRAME_PROMOTION
                && registry_stop_reader.owner);
            col_rel_t *candidate = s->recursive.delta_rels[0];
            CHECK(atomic_load_explicit(&candidate->descriptor_access.state,
                memory_order_acquire) == WL_COLUMNAR_SOURCE_ACCESS_WRITER
                && atomic_load_explicit(&candidate->source_access.state,
                memory_order_acquire) == 1);
            CHECK(wl_columnar_source_access_reader_release(
                    &registry_stop_reader) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0 &&
                !s->cleanup_pending);
            CHECK(s->recursive.phase ==
                WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_ROLLBACK_RESTORED);
        } else if (collision == 6) {
            CHECK(registry_admissions == 2 && registry_fault_token
                && registry_fault_token->identity == NULL &&
                registry_fault_token->bytes
                && atomic_load_explicit(&registry_fault_token->state,
                memory_order_acquire) ==
                WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
            CHECK(s->cleanup_pending &&
                s->serial_completion.mode_frame == s->cleanup_pending
                && s->serial_completion.mode ==
                WL_COLUMNAR_SERIAL_FRAME_REGISTRY
                && s->recursive.phase ==
                WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTRY_FAILED);
            registry_fault_token->identity = registry_fault_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0 &&
                !s->cleanup_pending
                && s->recursive.phase ==
                WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_ROLLBACK_RESTORED);
        } else if (collision == 4) {
            CHECK(!s->cleanup_pending && s->recursive.cleanup_pending
                && s->recursive.phase ==
                WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTERED
                && s->recursive.canonical_consumed &&
                s->nrels == registered_before + 1);
            col_rel_t *published = s->recursive.delta_rels[0];
            CHECK(published == session_find_rel(s, "$d$registry_source")
                && published->columns == columns &&
                published->pool_name_reserved_bytes
                && published->descriptor_reserved_bytes == sizeof(col_rel_t)
                + strlen("$d$registry_source") + 1u);
            /* Restore only the saved original scalar after serialized return;
             * genuine name-credit retirement cannot reinsert or reverse. */
            published->pool_name_reservation.identity =
                registry_saved_name_identity;
            CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
            CHECK(!published->pool_name_reserved_bytes &&
                s->nrels == registered_before + 1
                && published->columns == columns && !s->recursive.active);
        } else CHECK(!s->cleanup_pending && s->recursive.phase == 2);
        if (collision && collision != 4) {
            CHECK(s->recursive.delta_rels[0] == source &&
                source->columns == columns
                && source->pool_owned &&
                source->retained_reserved_bytes == retained
                && s->nrels == registered_before);
        } else if (!collision) {
            CHECK(!s->recursive.delta_rels[0] && s->recursive.canonical_consumed
                && s->nrels == registered_before + 1);
            col_rel_t *published = session_find_rel(s, "$d$registry_source");
            CHECK(published && !published->pool_owned &&
                published->columns == columns
                && published->retained_reserved_bytes == retained
                && !published->pool_name_reserved_bytes
                && published->descriptor_reserved_bytes == sizeof(col_rel_t)
                + strlen("$d$registry_source") + 1u);
            if (!after_reset) {
                /* Unlike a checked destructor, genuine promotion leaves
                 * moved-from EMPTY tokens with their original self identity.
                 * Sampling accepts that proven tombstone without resetting
                 * the pool cursor or weakening mutating pool-reuse policy. */
                col_rel_t *sampled = NULL;
                bool tombstone = false; uint64_t sampled_bytes = UINT64_MAX;
                uint32_t used = s->delta_pool->slot_used;
                CHECK(used == 1 && col_rel_transport_bytes(source) == 0
                    && wl_columnar_relation_pool_sample_controlled(s, 0,
                    &sampled, &tombstone, &sampled_bytes, c) == 0
                    && sampled == source && tombstone && sampled_bytes == 0
                    && s->delta_pool->slot_used == used
                    && published == session_find_rel(s, "$d$registry_source")
                    && published->columns == columns);
            }
        }
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0
            && !s->recursive.reset_active && !s->recursive.reset_pool);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, rc,
            rc) == rc);
        destroy(s);
    }
    registry_stop_session = NULL;
}

/* Exact registry suffix boundaries across an explicit, successful host setup
 * attempt. This is not the generated backend same-attempt work allowance. */
static void original_registry_prefix_boundaries(void)
{
    const uint64_t C = sizeof(wl_columnar_session_hash_controlled_registry_t);
    CHECK(C > 128);
    const uint64_t limits[4] = {128, C, C + 128, C + 5u * 128u};
    const unsigned prefixes[4] = {0, 0, 1, 5};
    for (unsigned trial = 0; trial < 8; trial++) {
        unsigned mode = trial % 4; bool after_reset = trial >= 4;
        wl_plan_relation_t relation = {0};
        wl_plan_stratum_t stratum = {.is_recursive = true,
                                     .relation_count = 1,
                                     .relations = &relation};
        wl_plan_t plan = {.stratum_count = 1, .strata = &stratum};
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *input = col_rel_new_auto("registry_source", 1);
        CHECK(input && session_add_rel(s, input) == 0 && s->nrels == 1);
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        CHECK(wl_columnar_eval_serial_recursive_clone(s, input,
            "$d$registry_source", 0, c) == 0);
        if (after_reset) CHECK(wl_columnar_eval_serial_recursive_pool_reset(s,
                c) == 0
                && s->recursive.reset_active && !s->delta_pool->slot_used);
        col_rel_t *source = s->recursive.delta_rels[0];
        int64_t **columns = source->columns; char *name = source->name;
        uint64_t retained = source->retained_reserved_bytes;
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *original_frame = frame;
        uint64_t original_frame_bytes = s->cleanup_reserved_bytes;
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_session_promotion_scratch_t *state = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_promotion_owner(s, frame,
            &authority, &state, c) == 0);
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(wl_columnar_relation_incremental_acquire(authority, source, NULL,
            c) == 0);
        CHECK(wl_columnar_relation_descriptor_prepare(bulk, state, result,
            source->name, s->memory_governor, c) == 0);
        CHECK(wl_columnar_session_promotion_move_credits(authority, state,
            result, c) == 0);
        CHECK(wl_columnar_session_promotion_stage_payload(authority, state,
            result, c) == 0);
        CHECK(wl_columnar_session_promotion_publish_payload(s, authority, state,
            result, c) == 0);
        col_rel_t *candidate = s->recursive.delta_rels[0];
        CHECK(candidate != source && !candidate->pool_owned &&
            candidate->columns == columns
            && candidate->name == name && !result->rel && !result->owned
            && s->recursive.phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_PROMOTED
            && s->cleanup_active == original_frame && !source->columns);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        wl_evaluation_control_report_t setup;
        CHECK(wl_evaluation_control_report(c,
            &setup) == 0 && setup.attempt_id == 1);
        CHECK(wl_evaluation_control_configure(c, limits[mode]) == 0);
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_session_hash_controlled_registry_t *registry = NULL;
        int rc = wl_columnar_eval_stack_cleanup_registry(frame, &registry, c);
        if (!rc) rc = wl_columnar_session_hash_registry_controlled(registry, s,
                    candidate, c);
        CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
            && s->serial_completion.mode_frame == original_frame
            && s->serial_completion.mode == WL_COLUMNAR_SERIAL_FRAME_REGISTRY
            && s->cleanup_reserved_bytes == original_frame_bytes
            && wl_columnar_eval_stack_cleanup_registry_matches(frame,
            registry));
        if (!mode) CHECK(!registry && !s->serial_completion.payload_ready);
        else {
            CHECK(registry && s->serial_completion.payload_ready &&
                registry->ready
                && registry->initialized_tokens == prefixes[mode] &&
                !registry->published);
            wl_columnar_memory_reservation_t *tokens[5] = {
                &registry->image.rels_reservation,
                &registry->image.hash_reservation,
                &registry->image.expected_rels_reservation, &registry->old_rels,
                &registry->old_hash
            };
            for (unsigned i = 0; i < prefixes[mode];
                i++) CHECK(tokens[i]->identity == tokens[i]
                    && atomic_load_explicit(&tokens[i]->state,
                    memory_order_acquire)
                    == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        }
        CHECK(wl_columnar_eval_serial_recursive_register_finish(s, 0, &frame,
            registry, rc, 0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!frame && !s->cleanup_pending && !s->cleanup_active &&
            s->nrels == 1
            && s->recursive.phase == 2 && s->recursive.delta_rels[0] == source
            && source->columns == columns && source->name == name &&
            source->pool_owned
            && source->retained_reserved_bytes == retained);
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                s->memory_governor)) == baseline);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0, 0)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(c,
            &report) == 0 && report.attempt_id == 2
            && report.consumed == limits[mode] && report.limit == limits[mode]);
        destroy(s);
    }
}

static wl_columnar_relation_bulk_append_t *schema_original;
static const void *schema_saved_identity;
static unsigned schema_mode, schema_seen;
static void schema_checkpoint(wl_evaluation_control_t *c, int phase,
    uint64_t progress)
{
    if (schema_seen) return;
    if (!schema_mode && phase == WL_COLUMNAR_RELATION_METADATA_NAME &&
        progress == 128){
        CHECK(schema_original->prepared_names && !schema_original->published
            && schema_original->dst->ncols == 0);
        schema_seen++; wl_evaluation_control_request_cancel(c);
    } else if (schema_mode == 1 && phase == WL_COLUMNAR_RELATION_METADATA_CHILD
        && progress == 1) {
        CHECK(schema_original->prepared_columns &&
            schema_original->prepared_names
            && schema_original->metadata_pending.identity ==
            &schema_original->metadata_pending);
        schema_seen++;
        schema_saved_identity = schema_original->metadata_pending.identity;
        schema_original->metadata_pending.identity = NULL;
        wl_columnar_relation_test_fail_next_metadata_alloc();
    } else if (schema_mode == 2 &&
        phase == WL_COLUMNAR_RELATION_METADATA_COMMITTED){
        CHECK(schema_original->published && schema_original->dst->ncols == 300
            && schema_original->old_metadata.bytes == 3
            && schema_original->old_metadata.identity ==
            &schema_original->old_metadata);
        schema_seen++;
        schema_saved_identity = schema_original->old_metadata.identity;
        schema_original->old_metadata.identity = NULL;
        wl_evaluation_control_request_cancel(c);
    }
}

static void original_canonical_schema_stops(void)
{
    for (schema_mode = 0; schema_mode < 3; schema_mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *dst = NULL;
        CHECK(wl_columnar_relation_alloc_governed(&dst, "H",
            s->memory_governor) == 0);
        col_rel_t *src = col_rel_new_auto("source", 300);
        CHECK(src && session_add_rel(s, dst) == 0);
        if (!schema_mode) {
            free(src->col_names[0]); src->col_names[0] = malloc(301);
            CHECK(src->col_names[0]); memset(src->col_names[0], 'n', 300);
            src->col_names[0][300] = 0;
        }
        if (schema_mode == 2) CHECK(col_rel_set_schema(dst, 0, NULL) == 0);
        uint64_t view = dst->view_generation;
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        schema_original = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        schema_seen = 0; schema_saved_identity = NULL;
        wl_columnar_relation_test_metadata_checkpoint = schema_checkpoint;
        int rc =
            wl_columnar_relation_canonical_schema_controlled(schema_original,
                dst, src, c);
        wl_columnar_relation_test_metadata_checkpoint = NULL;
        CHECK(schema_seen == 1 &&
            rc == (!schema_mode ? WL_EVALUATION_CONTROL_CANCELLED
            : schema_mode == 1 ? ENOMEM : EINVAL));
        CHECK(!schema_original->prepared_columns &&
            !schema_original->prepared_names
            && !schema_original->prepared_row_scratch &&
            !schema_original->prepared_schema_ok
            && dst->nrows == 0 && dst->ncols == (schema_mode == 2 ? 300u : 0u)
            && dst->view_generation == view + (schema_mode == 2));
        if (schema_mode) {
            CHECK(schema_original->active && schema_original->schema_install
                && schema_original->writer.owner &&
                schema_original->descriptor_writer.owner);
            wl_columnar_memory_reservation_t *token = schema_mode == 1
                ? &schema_original->metadata_pending :
                &schema_original->old_metadata;
            CHECK(token->identity == NULL && token->bytes
                && atomic_load_explicit(&token->state, memory_order_acquire)
                == (schema_mode == 1 ? WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                        : WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED));
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) ==
            (schema_mode ? EINVAL : 0)
            && !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
            rc > 0 ? rc : 0, 0)
            == rc);
        /* Only the saved identity scalar is restored after serialized return. */
        if (schema_mode) {
            wl_columnar_memory_reservation_t *token = schema_mode == 1
                ? &schema_original->metadata_pending :
                &schema_original->old_metadata;
            token->identity = schema_saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0 &&
                !s->cleanup_pending
                && !s->serial_completion.frame);
        }
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        schema_original = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(wl_columnar_relation_canonical_schema_controlled(schema_original,
            dst, src, c) == 0);
        CHECK(dst->ncols == 300 && !dst->nrows
            && dst->view_generation == view + 1);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(col_rel_destroy_checked(src) == 0); destroy(s);
    }
    schema_original = NULL;
}

static void original_canonical_schema(void)
{
    for (unsigned mode = 0; mode < 2; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *dst = NULL, *raw = NULL;
        CHECK(wl_columnar_relation_alloc_governed(&dst, "H",
            s->memory_governor) == 0);
        CHECK(wl_columnar_relation_alloc_governed(&raw, "raw",
            s->memory_governor) == 0);
        uint32_t width = mode ? 1u : 300u;
        col_rel_t *src = col_rel_new_auto("source", width);
        CHECK(src && session_add_rel(s, dst) == 0);
        if (mode) {
            CHECK(col_rel_set_schema(dst, 0, NULL) == 0
                && col_rel_set_schema(raw, 0, NULL) == 0);
            CHECK(col_rel_enable_timestamps(dst) == 0
                && col_rel_enable_timestamps(raw) == 0);
        }
        col_delta_timestamp_t *timestamps = dst->timestamps;
        uint64_t storage = dst->storage_generation;
        uint64_t view = dst->view_generation;
        CHECK(col_rel_set_schema(raw, width,
            (const char *const *)src->col_names) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(wl_columnar_relation_canonical_schema_controlled(bulk, dst, src,
            c) == 0);
        CHECK(!bulk->active && !bulk->schema_install && dst->nrows == 0
            && dst->ncols == width && dst->capacity == raw->capacity
            && dst->timestamps == timestamps &&
            dst->timestamp_capacity == raw->timestamp_capacity
            && dst->storage_generation == storage &&
            dst->view_generation == view + 1
            && dst->retained_reserved_bytes == raw->retained_reserved_bytes
            && dst->metadata_reserved_bytes == raw->metadata_reserved_bytes);
        for (uint32_t col = 0; col < width; col++)
            CHECK(!strcmp(dst->col_names[col], raw->col_names[col])
                && !strcmp(dst->schema.children[col]->format,
                raw->schema.children[col]->format));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(col_rel_destroy_checked(src) == 0 &&
            col_rel_destroy_checked(raw) == 0);
        destroy(s);
    }
}

static void original_initial_sort(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = col_rel_new_auto("initial", 1);
    col_rel_t *raw = col_rel_new_auto("raw", 1);
    CHECK(source && raw);
    int64_t values[4] = {7, -3, 7, 2};
    for (unsigned i = 0; i < 4; i++) {
        CHECK(col_rel_append_row(source, &values[i]) == 0);
        CHECK(col_rel_append_row(raw, &values[i]) == 0);
    }
    CHECK(col_rel_enable_timestamps(source) == 0
        && col_rel_enable_timestamps(raw) == 0);
    for (unsigned i = 0; i < 4; i++) {
        source->timestamps[i].iteration = raw->timestamps[i].iteration = i + 1;
        source->timestamps[i].multiplicity = raw->timestamps[i].multiplicity =
            1;
    }
    CHECK(session_add_rel(s, source) == 0);
    col_delta_timestamp_t *timestamps = source->timestamps;
    uint64_t storage = source->storage_generation;
    CHECK(col_rel_radix_sort_int64(raw) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_serial_recursive_initial_sort(s, source, c) == 0);
    CHECK(source->nrows == 4 && source->sorted_nrows == 4
        && source->timestamps == timestamps &&
        source->storage_generation == storage
        && !s->serial_completion.frame && !s->cleanup_pending);
    for (unsigned i = 0; i < 4;
        i++) CHECK(source->columns[0][i] == raw->columns[0][i]
            && source->timestamps[i].iteration == raw->timestamps[i].iteration);
    col_frontier_t frontier = {0, 0}; int64_t net = 0;
    CHECK(wl_columnar_eval_serial_recursive_frontier(s, source, &frontier, &net,
        c) == 0
        && frontier.iteration == 1 && frontier.stratum == 0 && net == 4
        && !s->serial_completion.frame && !s->cleanup_pending);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(col_rel_destroy_checked(raw) == 0);
    destroy(s);
}

/* Actual reset certificate holds multiple original slab owners. */
static void original_pool_reset_window(void)
{
    for (unsigned mode = 0; mode < 6; mode++) {
        wl_plan_relation_t relations[2] = {{0}, {0}};
        wl_plan_stratum_t sp = {.is_recursive = true, .relation_count = 2,
                                .relations = relations};
        wl_plan_t plan = {.stratum_count = 1, .strata = &sp};
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *input = col_rel_new_auto("reset_input", 1);
        CHECK(input && session_add_rel(s, input) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        CHECK(wl_columnar_eval_serial_recursive_clone(s, input, "$d$reset0", 0,
            c) == 0);
        CHECK(wl_columnar_eval_serial_recursive_clone(s, input, "$d$reset1", 1,
            c) == 0);
        col_rel_t *source = s->recursive.delta_rels[1];
        CHECK(source ==
            (col_rel_t *)(s->delta_pool->slab + s->delta_pool->slot_size)
            && s->delta_pool->slot_used == 2);
        int64_t **columns = source->columns;
        if (mode == 2) {
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0) == 0);
            CHECK(wl_evaluation_control_configure(c, 80) == 0);
            CHECK(wl_evaluation_control_begin(c,
                s->base.operation_admission) == 0);
            CHECK(wl_columnar_eval_serial_recursive_pool_reset(s, c)
                == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !s->recursive.reset_active && s->delta_pool->slot_used == 2
                && source->columns == columns && s->recursive.delta_rels[0]);
            CHECK(wl_columnar_eval_serial_recursive_retire(s,
                WL_EVALUATION_CONTROL_WORK_LIMIT,
                NULL) == WL_EVALUATION_CONTROL_WORK_LIMIT
                && !s->recursive.active && !s->cleanup_pending);
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            destroy(s);
            continue;
        }
        if (mode >= 6) {
            s->recursive.delta_rels[1] = mode == 6 ? s->recursive.delta_rels[0]
                : (col_rel_t *)(s->delta_pool->slab + 2u *
                s->delta_pool->slot_size);
            CHECK(wl_columnar_eval_serial_recursive_pool_reset(s, c) == EINVAL
                && !s->recursive.reset_active && s->delta_pool->slot_used == 2);
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                EINVAL, EINVAL) == EINVAL);
            /* A malformed original journal pointer is restored only after
             * the bounded read-only validation returned. Actual source1
             * remains physically intact; no duplicate disposal is attempted. */
            s->recursive.delta_rels[1] = source;
            CHECK(wl_evaluation_control_reset(c) == 0);
            CHECK(wl_evaluation_control_begin(c,
                s->base.operation_admission) == 0);
        }
        if (mode == 1) {
            /* The negative reuses a genuinely disposed slot0, leaving the
             * credited journal source at slot1 physically intact. */
            CHECK(col_rel_destroy_checked(s->recursive.delta_rels[0]) == 0);
            s->recursive.delta_rels[0] = NULL;
        }
        CHECK(wl_columnar_eval_serial_recursive_pool_reset(s, c) == 0
            && s->recursive.reset_active && s->recursive.reset_high_water == 2
            && !s->delta_pool->slot_used);
        if (!mode) {
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 0, c) == 0
                && s->recursive.reset_active && !s->recursive.delta_rels[0]
                && s->recursive.delta_rels[1] == source);
            CHECK(wl_columnar_eval_serial_recursive_pool_barrier(s,
                c) == EBUSY);
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 1, c) == 0
                && s->recursive.reset_active && !s->recursive.delta_rels[1]);
            CHECK(wl_columnar_eval_serial_recursive_pool_barrier(s, c) == 0
                && !s->recursive.reset_active && !s->recursive.reset_pool
                && !s->cleanup_pending && !s->serial_completion.frame);
            CHECK(session_find_rel(s, "$d$reset1")->columns == columns);
        } else if (mode >= 3 && mode <= 5) {
            char *slab = s->recursive.reset_slab;
            size_t stride = s->recursive.reset_stride;
            uint32_t high_water = s->recursive.reset_high_water;
            if (mode == 3) s->recursive.reset_stride++;
            if (mode == 4) s->recursive.reset_slab = NULL;
            if (mode ==
                5) s->recursive.reset_high_water = s->delta_pool->slot_cap + 1u;
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 1,
                c) == EINVAL
                && s->recursive.reset_active &&
                s->recursive.delta_rels[1] == source
                && source->columns == columns && source->pool_owned
                && !s->cleanup_pending && !s->serial_completion.frame);
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                EINVAL, EINVAL) == EINVAL);
            /* Restore only the saved certificate scalars after serialized
             * return. No reservation or gate authority is copied/repaired. */
            s->recursive.reset_slab = slab; s->recursive.reset_stride = stride;
            s->recursive.reset_high_water = high_water;
            CHECK(wl_evaluation_control_reset(c) == 0);
            CHECK(wl_evaluation_control_begin(c,
                s->base.operation_admission) == 0);
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 1,
                c) == 0);
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 0,
                c) == 0);
            CHECK(wl_columnar_eval_serial_recursive_pool_barrier(s, c) == 0
                && !s->recursive.reset_active && !s->cleanup_pending);
        } else if (mode == 1) {
            col_rel_t *replacement = NULL;
            CHECK(wl_columnar_relation_pool_new_like_governed_checked(
                    &replacement,
                    s->delta_pool, "replacement", input,
                    s->memory_governor) == 0);
            CHECK(replacement == (col_rel_t *)s->delta_pool->slab
                && s->delta_pool->slot_used == 1);
            uint64_t extent = 0; uint32_t slots = 0;
            CHECK(wl_columnar_session_recursive_pool_extent(s, &extent,
                &slots) == EINVAL
                && s->recursive.reset_active && source->columns == columns
                && source->pool_owned);
            CHECK(wl_columnar_eval_serial_recursive_pool_barrier(s,
                c) == EINVAL);
            CHECK(col_rel_destroy_checked(replacement) == 0);
            /* No scalar cursor repair or second reset. Actual journal
             * disposal retires intact slot1 before certificate retirement. */
        } else {
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 0,
                c) == 0);
            CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 1,
                c) == 0);
            CHECK(wl_columnar_eval_serial_recursive_pool_barrier(s, c) == 0);
        }
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0
            && !s->recursive.reset_active && !s->recursive.reset_pool);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        destroy(s);
    }
}

/* Real generated immutable body, deliberately separate from the still-held
* backend orchestration: this phase fixture supplies its canonical head. */
/* Separate completed-constructor host attempt, literal handoff160 and
 * bridge240 before registry clear. This is not a whole-backend work total. */
static void original_descriptor_handoff(void)
{
    const uint64_t limits[] = {159, 160, 160 + 240 + 128, 0, 0, 0};
    for (unsigned mode = 0; mode < 6; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *existing = NULL;
        if (mode == 4) CHECK(wl_columnar_relation_alloc_governed(&existing,
                "canonical", s->memory_governor) == 0 && session_add_rel(s,
                existing) == 0);
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_session_promotion_scratch_t *state = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_promotion_owner(s, frame,
            &authority, &state, c) == 0);
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        wl_columnar_relation_bulk_append_t *bulk =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(wl_columnar_relation_descriptor_prepare(bulk, state, result,
            "canonical", s->memory_governor, c) == 0);
        col_rel_t *original = result->rel;
        CHECK(original && original->descriptor_reservation.identity
            == &original->descriptor_reservation);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_evaluation_control_configure(c, limits[mode]) == 0);
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        int rc = wl_columnar_relation_descriptor_handoff(bulk, state, result,
                stack, c);
        if (!mode) CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
                && result->rel == original && !stack->top &&
                state->descriptor_moved);
        else {
            CHECK(!rc && !result->rel && stack->top == 1
                && stack->items[0].rel == original && stack->items[0].owned
                && !state->descriptor_moved && !state->governor_retained
                && original->memory_governor == s->memory_governor
                && original->descriptor_reservation.identity ==
                &original->descriptor_reservation);
            wl_columnar_session_hash_controlled_registry_t *registry = NULL;
            if (mode == 3) wl_evaluation_control_request_cancel(c);
            rc = wl_columnar_eval_stack_cleanup_descriptor_registry(frame,
                    &registry, c);
            if (mode >= 4) {
                CHECK(!rc && registry && result->rel == original &&
                    result->owned);
                if (mode == 5) wl_evaluation_control_request_cancel(c);
                rc = wl_columnar_session_hash_registry_controlled(registry, s,
                        original, c);
                CHECK(rc ==
                    (mode == 4 ? EEXIST : WL_EVALUATION_CONTROL_CANCELLED)
                    && !registry->published);
                const void *identity =
                    original->descriptor_reservation.identity;
                CHECK(identity == &original->descriptor_reservation
                    && atomic_load_explicit(
                        &original->descriptor_reservation.state,
                        memory_order_acquire) ==
                    WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
                original->descriptor_reservation.identity = NULL;
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL
                    && !frame && s->cleanup_pending && original->name
                    && (mode == 4 ? s->rels[0] == existing : !s->nrels));
                int reported = mode == 4 ? EEXIST : EINVAL;
                CHECK(wl_evaluation_control_finish(c,
                    s->base.operation_admission,
                    reported, reported) == reported);
                wl_evaluation_control_report_t report;
                CHECK(wl_evaluation_control_report(c, &report) == 0
                    && report.result == reported &&
                    report.execution_status == reported
                    && (mode != 5 ||
                    report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED));
                original->descriptor_reservation.identity = identity;
                CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0
                    && !s->cleanup_pending && !s->cleanup_active);
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            s->memory_governor)) == baseline);
                destroy(s); continue;
            }
            CHECK(rc == (mode == 3 ? WL_EVALUATION_CONTROL_CANCELLED
                : WL_EVALUATION_CONTROL_WORK_LIMIT));
            if (mode == 2) CHECK(!wl_columnar_eval_stack_cleanup_stack(frame)
                    && result->rel == original
                    && s->serial_completion.mode ==
                    WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
                    && !s->serial_completion.payload_ready && !registry);
            else CHECK(stack->top == 1 && !result->rel
                    && s->serial_completion.mode ==
                    WL_COLUMNAR_SERIAL_FRAME_PROMOTION);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
            && !s->cleanup_pending && !s->cleanup_active);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(s->memory_governor)) ==
            baseline);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == rc);
        destroy(s);
    }
}

/* Dead union bytes are not authority. The real external plain registry
 * mode must ignore a numeric marker at the retired promotion phase address. */
static void original_registry_dead_union_marker(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
    delta_pool_t *pool = s->delta_pool;
    wl_arena_t *arena = s->eval_arena;
    uint32_t pool_used = pool ? pool->slot_used : 0;
    col_rel_t *candidate = NULL; int64_t row = 7;
    CHECK(wl_columnar_relation_alloc_governed(&candidate, "generic",
        s->memory_governor) == 0
        && col_rel_set_schema(candidate, 1, NULL) == 0
        && col_rel_append_row(candidate, &row) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0
        && wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_session_promotion_scratch_t *state = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0
        && wl_columnar_eval_stack_cleanup_promotion_owner(s, frame, &authority,
        &state, c) == 0);
    wl_columnar_session_hash_controlled_registry_t *registry = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_registry(frame, &registry, c) == 0
        && s->serial_completion.mode == WL_COLUMNAR_SERIAL_FRAME_REGISTRY
        && !state->phase && !state->descriptor_moved &&
        !state->governor_retained);
    state->phase = WL_COLUMNAR_SESSION_PROMOTION_CANONICAL_DESCRIPTOR;
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    *result = (eval_entry_t){.rel = candidate, .owned = true,
                             .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION};
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    uint64_t usage = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
    if (cleanup || frame || s->cleanup_pending || s->cleanup_reserved_bytes ||
        usage != baseline)
        fprintf(stderr,
            "dead union cleanup=%d frame=%d pending=%d cleanup_bytes=%llu usage=%llu baseline=%llu\n",
            cleanup, frame != NULL, s->cleanup_pending != NULL,
            (unsigned long long)s->cleanup_reserved_bytes,
            (unsigned long long)usage,
            (unsigned long long)baseline);
    CHECK(cleanup == 0 && !frame && !s->cleanup_pending &&
        !s->cleanup_reserved_bytes && usage == baseline
        && s->delta_pool == pool && s->eval_arena == arena
        && (!pool || pool->slot_used == pool_used) && !s->nrels);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(s);
}

static wl_columnar_session_hash_controlled_registry_t *canonical_fault_registry;
static const void *canonical_old_identity;
static uint64_t canonical_old_hash_bytes, canonical_old_rels_bytes;
static col_rel_t *canonical_published_candidate;

static void canonical_published_credit_fault(
    wl_columnar_session_hash_controlled_registry_t *registry)
{
    if (!registry->published || canonical_old_identity) return;
    CHECK(registry->ready && registry->initialized_tokens == 5
        && !registry->image.rels && !registry->image.hash_head
        && !registry->image.hash_next && !registry->image.expected_rel_contents
        && !registry->retired_rels && !registry->retired_head &&
        !registry->retired_next);
    wl_columnar_memory_reservation_t *token = &registry->old_hash;
    CHECK(token->identity == token && token->bytes && token->governor
        && atomic_load_explicit(&token->state, memory_order_acquire)
        == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
    canonical_fault_registry = registry;
    canonical_old_identity = token->identity;
    canonical_old_hash_bytes = token->bytes;
    canonical_old_rels_bytes = registry->old_rels.bytes;
    wl_col_session_t *s = registry->image.session;
    CHECK(s && s->nrels == 2 && s->rels[1]);
    canonical_published_candidate = s->rels[1];
    token->identity = NULL;
}

/* One real canonical caller and one separately labelled original-frame
 * publication phase exercise both independent ownership acknowledgements. */
static void original_canonical_publication_refusal(void)
{
    for (unsigned direct_frame = 0; direct_frame < 2; direct_frame++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *prior = NULL;
        CHECK(wl_columnar_relation_alloc_governed(&prior, "prior",
            s->memory_governor) == 0
            && session_add_rel(s, prior) == 0 && session_find_rel(s,
            "prior") == prior);
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        canonical_fault_registry = NULL; canonical_old_identity = NULL;
        canonical_published_candidate = NULL;
        wl_columnar_session_hash_test_registry_retirement_checkpoint =
            canonical_published_credit_fault;
        int rc;
        if (!direct_frame) rc =
                wl_columnar_eval_serial_recursive_register_head(s, "canonical",
                    c);
        else {
            wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
            wl_columnar_merge_incremental_authority_t *authority = NULL;
            wl_columnar_session_promotion_scratch_t *state = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0
                && wl_columnar_eval_stack_cleanup_promotion_owner(s, frame,
                &authority, &state, c) == 0);
            eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
            wl_columnar_relation_bulk_append_t *bulk =
                wl_columnar_eval_stack_cleanup_bulk_append(frame);
            CHECK(wl_columnar_relation_descriptor_prepare(bulk, state, result,
                "canonical", s->memory_governor, c) == 0
                && wl_columnar_relation_descriptor_handoff(bulk, state, result,
                wl_columnar_eval_stack_cleanup_stack(frame), c) == 0);
            wl_columnar_session_hash_controlled_registry_t *registry = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_descriptor_registry(frame,
                &registry, c) == 0);
            rc = wl_columnar_session_hash_registry_controlled(registry, s,
                    result->rel, c);
            CHECK(rc == EINVAL && registry->published &&
                result->rel == s->rels[1]
                && result->owned && state->descriptor_moved &&
                state->governor_retained);
            /* No manual result/journal adjustment: the true frame disposer
            * must consume actual publication before its credit refusal. */
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL &&
                !frame
                && !result->rel && !result->owned && !state->descriptor_moved
                && !state->governor_retained);
        }
        wl_columnar_session_hash_test_registry_retirement_checkpoint = NULL;
        CHECK(rc == EINVAL && canonical_fault_registry &&
            canonical_fault_registry->published
            && canonical_old_identity && s->cleanup_pending &&
            !s->cleanup_active
            && s->rels[0] == prior &&
            s->rels[1] == canonical_published_candidate
            && !canonical_published_candidate->ncols &&
            !canonical_published_candidate->schema_ok
            && canonical_published_candidate->descriptor_reservation.identity ==
            &canonical_published_candidate->descriptor_reservation
            && atomic_load_explicit(
                &canonical_published_candidate->descriptor_reservation.state,
                memory_order_acquire) ==
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, rc,
            rc) == rc);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(c, &report) == 0
            && report.result == EINVAL && report.execution_status == EINVAL);
        /* Only this original scalar is restored after serialized return. */
        canonical_fault_registry->old_hash.identity = canonical_old_identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0 &&
            !s->cleanup_pending
            && !s->serial_completion.frame && !s->cleanup_reserved_bytes
            && s->nrels == 2 && s->rels[1] == canonical_published_candidate);
        uint64_t expected = baseline - canonical_old_hash_bytes -
            canonical_old_rels_bytes
            + s->rel_hash_reservation.bytes + s->rels_reservation.bytes
            + canonical_published_candidate->descriptor_reserved_bytes;
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(s->memory_governor)) ==
            expected);
        CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0 && s->nrels == 2
            && s->rels[1] == canonical_published_candidate);
        destroy(s);
    }
}

static void original_registered_delta_removal(void)
{
    for (unsigned held = 0; held < 2; held++) {
        wl_plan_relation_t rp = {.name = "head"};
        wl_plan_stratum_t sp = {.is_recursive = true, .relation_count = 1,
                                .relations = &rp};
        wl_plan_t plan = {.stratum_count = 1, .strata = &sp};
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *delta = col_rel_new_auto("$d$head", 1);
        int64_t row = 7;
        CHECK(delta && col_rel_append_row(delta,
            &row) == 0 && session_add_rel(s, delta) == 0);
        wl_columnar_source_access_reader_t reader = {0};
        if (held) CHECK(col_rel_source_reader_acquire(delta, &reader) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        int rc = wl_columnar_session_remove_controlled(s, "$d$head", c);
        CHECK(rc == (held ? EBUSY : 0) && s->recursive.phase == 2
            && !s->recursive.removal_lease && !s->recursive.removal_name);
        if (held) CHECK(session_find_rel(s, "$d$head") == delta
                && delta->nrows == 1 && delta->columns[0][0] == 7);
        else CHECK(s->nrels == 1 && !s->rels[0] && !s->rel_hash_head
                && !s->rel_hash_next && !s->rel_hash_nbuckets &&
                !s->rel_hash_chain_cap);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        if (held) {
            CHECK(col_rel_source_reader_release(&reader) == 0);
            CHECK(wl_evaluation_control_reset(c) == 0);
            CHECK(wl_evaluation_control_begin(c,
                s->base.operation_admission) == 0);
            CHECK(wl_columnar_session_remove_controlled(s, "$d$head", c) == 0
                && !session_find_rel(s, "$d$head"));
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0) == 0);
        }
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
        destroy(s);
    }
}

/* Actual cache builds followed by a source-derived invalidation boundary.
 * This is a direct phase proof, not a full backend cost assertion. */
static void original_removal_cache_boundary(void)
{
    wl_plan_relation_t rp = {.name = "head"};
    wl_plan_stratum_t sp = {.is_recursive = true, .relation_count = 1,
                            .relations = &rp};
    wl_plan_t plan = {.stratum_count = 1, .strata = &sp};
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *delta = col_rel_new_auto("$d$head", 300);
    int64_t row[300];
    for (unsigned i = 0; i < 300; i++) row[i] = (int64_t)i;
    CHECK(delta && col_rel_append_row(delta, row) == 0 && session_add_rel(s,
        delta) == 0);
    for (uint32_t key = 0; key < 257; key++)
        CHECK(col_session_get_arrangement(base, "$d$head", &key, 1));
    CHECK(s->arr_count == 257);
    for (unsigned i = 0; i < 257;
        i++) CHECK(s->arr_entries[i].arr.indexed_rows == 1);
    col_sorted_arr_t *sorted = col_session_get_sorted_arrangement(s, "$d$head",
            0);
    CHECK(sorted && sorted->sorted && sorted->nrows == 1 && s->sarr_count == 1);
    void *original_sorted = sorted->sorted;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    /* Registry8 + eight name bytes*2 + selection32; each real matching
     * primary: visit16 + eight name bytes + mutation32. */
    CHECK(wl_evaluation_control_configure(c, 56 + 257 * 56) == 0);
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    int rc = wl_columnar_session_remove_controlled(s, "$d$head", c);
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && s->recursive.phase ==
        WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL &&
        s->recursive.removal_family == 1
        && !s->recursive.removal_cursor && s->rels[0] == delta &&
        delta->nrows == 1
        && sorted->sorted == original_sorted && sorted->nrows == 1);
    for (unsigned i = 0; i < 257; i++) CHECK(!s->arr_entries[i].arr.indexed_rows
            && !s->arr_entries[i].source_snapshot.storage_generation);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == rc);
    CHECK(wl_columnar_session_recursive_removal_drain(s, 0) == 0
        && s->recursive.phase == 2 && s->rels[0] == delta);
    CHECK(wl_evaluation_control_reset(c) == 0);
    /* The completed host attempt retains its configured limit on reset.
     * The independent retry explicitly selects unlimited work. */
    CHECK(wl_evaluation_control_configure(c, 0) == 0);
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    rc = wl_columnar_session_remove_controlled(s, "$d$head", c);
    if (rc || s->rels[0] || sorted->sorted || sorted->nrows)
        fprintf(stderr,
            "cache retry rc=%d phase=%u family=%u cursor=%u relation=%d sorted=%d rows=%u\n",
            rc, s->recursive.phase, s->recursive.removal_family,
            s->recursive.removal_cursor, s->rels[0] != NULL,
            sorted->sorted != NULL, sorted->nrows);
    CHECK(rc == 0 && !s->rels[0] && !sorted->sorted && !sorted->nrows);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
    destroy(s);
}

static const void *hash_saved_identity;
static uint64_t hash_expected_state;
static bool hash_cancel;
static unsigned hash_fault_seen;
static void hash_retirement_fault(wl_col_session_t *s,
    wl_columnar_memory_reservation_t *token)
{
    CHECK(!hash_fault_seen++ && token == &s->rel_hash_reservation
        && token->identity == token && token->bytes && token->governor
        && !s->rel_hash_head && !s->rel_hash_next
        && !s->rel_hash_nbuckets && !s->rel_hash_chain_cap
        && atomic_load_explicit(&token->state,
        memory_order_acquire) == hash_expected_state);
    hash_saved_identity = token->identity; token->identity = NULL;
    if (hash_cancel) wl_evaluation_control_request_cancel(
            s->base.evaluation_control);
}

/* RESERVED is actual production hash provenance. COMMITTED explicitly uses
 * a separately completed checked-commit setup on this SAME original token. */
static void original_hash_retirement_fault(void)
{
    for (unsigned mode = 0; mode < 4; mode++) {
        wl_plan_relation_t rp = {.name = "head"};
        wl_plan_stratum_t sp = {.is_recursive = true, .relation_count = 1,
                                .relations = &rp};
        wl_plan_t plan = {.stratum_count = 1, .strata = &sp};
        wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *delta = col_rel_new_auto("$d$head", 1);
        int64_t row = 7;
        CHECK(delta && col_rel_append_row(delta,
            &row) == 0 && session_add_rel(s, delta) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        if (mode & 1u) {
            bool committed = false;
            CHECK(wl_columnar_memory_governor_commit_controlled(
                    &s->rel_hash_reservation,
                    s, &committed, c) == 0 && committed);
        }
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_serial_recursive_begin(s, &plan, 0, c) == 0);
        uint64_t before = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
        uint64_t bytes = s->rel_hash_reservation.bytes;
        hash_saved_identity = NULL; hash_fault_seen = 0;
        hash_expected_state = mode &
            1u ? WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            : WL_COLUMNAR_MEMORY_RESERVATION_RESERVED;
        hash_cancel = mode < 2;
        wl_columnar_session_hash_test_retirement_checkpoint =
            hash_retirement_fault;
        CHECK(wl_columnar_session_remove_controlled(s, "$d$head", c) == EINVAL);
        wl_columnar_session_hash_test_retirement_checkpoint = NULL;
        CHECK(hash_fault_seen == 1 && !s->rels[0] && !s->rel_hash_head
            && !s->rel_hash_next && !s->rel_hash_nbuckets &&
            !s->rel_hash_chain_cap
            && s->recursive.cleanup_pending && s->recursive.removal_family == 8
            && s->recursive.phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL
            && s->rel_hash_reservation.bytes == bytes
            && atomic_load_explicit(&s->rel_hash_reservation.state,
            memory_order_acquire)
            == hash_expected_state
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(s->memory_governor)) ==
            before);
        int primary = WL_EVALUATION_CONTROL_CANCELLED;
        if (mode >= 2) {
            primary = wl_evaluation_control_charge(c, UINT64_MAX);
            CHECK(primary == EOVERFLOW);
        }
        int rc = wl_columnar_session_recursive_removal_drain(s, primary);
        CHECK(rc == (mode >= 2 ? EOVERFLOW : EINVAL));
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, rc,
            rc) == rc);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(c, &report) == 0
            && report.result == rc && report.execution_status == rc);
        s->rel_hash_reservation.identity = hash_saved_identity;
        CHECK(wl_columnar_session_recursive_removal_drain(s, 0) == 0
            && !s->recursive.cleanup_pending && s->recursive.phase == 2
            && !s->recursive.removal_lease
            && atomic_load_explicit(&s->rel_hash_reservation.state,
            memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(s->memory_governor)) ==
            before - bytes);
        CHECK(wl_columnar_session_recursive_removal_drain(s, 0) == 0);
        CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
        destroy(s);
    }
}

/* Capture the genuine original embedded frame token at the existing checked
 * commit cutoff. The hook only observes; faults occur after the frame retires. */
static wl_columnar_memory_reservation_t *retained_frame_token;
static wl_evaluation_control_t *retained_frame_control;
static void retained_frame_commit(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (control == retained_frame_control
        && phase == WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF) {
        CHECK(!retained_frame_token && token->identity == token);
        retained_frame_token = token;
    }
}

/* Internal no-governor compatibility only: this fresh context has never
 * owned governed session objects. Public backend sessions always have a ref. */
/* Physical-shape sampling helper boundary: setup has a separately completed
 * host attempt, and the original source group remains at its frame address.
 * The literal stops before column258 after257 actual charged visits. */
static wl_columnar_source_access_writer_t *sample_writer_original;
static wl_evaluation_control_t *sample_writer_control;
static uint32_t sample_writer_calls;
static void sample_writer_released(wl_evaluation_control_t *c,
    wl_columnar_source_access_writer_t *writer,
    wl_columnar_source_access_gate_t *gate)
{
    if (c != sample_writer_control) return;
    CHECK(writer == sample_writer_original && !writer->owner
        && !writer->secondary_owner && !writer->identity &&
        !writer->thread_valid
        && !atomic_load(&gate->state) && !sample_writer_calls);
    sample_writer_calls++;
    wl_evaluation_control_request_cancel(c);
}

/* Isolated partial source-group lifetime: its original descriptor writer is
 * genuinely acquired before any source reader, as in a partial admission.
 * No token bytes are copied and no artificial gate release result is used. */
static void original_sampling_writer_release(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *r = col_rel_new_auto("writer_sample", 1);
    CHECK(r && session_add_rel(s, r) == 0);
    col_rel_t *owner = r->storage_owner;
    uint64_t owner_identity = r->storage_owner_identity;
    uint64_t owner_generation = r->storage_owner_generation;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0
        && wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_sources(frame);
    CHECK(group);
    wl_columnar_relation_source_group_descriptor_t *desc =
        &group->descriptors[0];
    desc->relation = r; group->descriptor_count = 1;
    CHECK(wl_columnar_relation_writer_acquire_controlled(&r->descriptor_access,
        &desc->writer, c) == 0);
    desc->writer_held = desc->writer.owner != NULL;
    sample_writer_original = &desc->writer; sample_writer_control = c;
    sample_writer_calls = 0;
    CHECK(!wl_columnar_relation_test_after_single_writer_release);
    wl_columnar_relation_test_after_single_writer_release =
        sample_writer_released;
    int rc = wl_columnar_relation_source_group_finish_controlled(group, c);
    wl_columnar_relation_test_after_single_writer_release = NULL;
    sample_writer_control = NULL; sample_writer_original = NULL;
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && sample_writer_calls == 1
        && !desc->writer_held && !desc->journal && !desc->writer.owner
        && !atomic_load(&r->descriptor_access.state)
        && r->storage_owner == owner &&
        r->storage_owner_identity == owner_identity
        && r->storage_owner_generation == owner_generation);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
        && !s->cleanup_pending && !s->cleanup_active);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, rc,
        rc) == rc);
    CHECK(wl_evaluation_control_reset(c) == 0
        && wl_evaluation_control_begin(c, s->base.operation_admission) == 0
        && wl_columnar_session_mem_sample_controlled(s, c) == 0
        && !s->cleanup_pending && !s->cleanup_reserved_bytes);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(s);
}

static uint64_t sample_completed_slots;
static void sample_pool_stop(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)record; (void)r;
    if (stage != 125) return;
    sample_completed_slots = progress;
    if (progress == 257) wl_evaluation_control_request_cancel(c);
}

/* A fresh internal context permits an actual300-slot pool. Public default
 * pools have256 slots; this is explicitly helper extent evidence, not a
 * claim that their constructor has a larger capacity. */
static void original_pool_sample(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *host = COL_SESSION(base);
    wl_col_session_t *context = calloc(1, sizeof(*context));
    CHECK(context);
    context->num_workers = 1;
    wl_mem_ledger_init(&context->mem_ledger, 0);
    context->delta_pool = delta_pool_create(300, sizeof(col_rel_t), 1024);
    col_rel_t *like = col_rel_new_auto("empty", 0);
    CHECK(context->delta_pool && like);
    for (uint32_t i = 0; i < 300; i++) {
        col_rel_t *slot = col_rel_pool_new_like(context->delta_pool, "dead",
                like);
        CHECK(slot && slot->pool_owned && col_rel_destroy_checked(slot) == 0);
    }
    CHECK(context->delta_pool->slot_used == 300);
    char *slab = context->delta_pool->slab;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0
        && wl_evaluation_control_attach(c,
        host->base.operation_admission) == 0);
    host->base.evaluation_control = c; context->base.evaluation_control = c;
    wl_mem_ledger_set_gauge(&context->mem_ledger, WL_MEM_SUBSYS_STORED, 55);
    wl_mem_ledger_set_gauge(&context->mem_ledger, WL_MEM_SUBSYS_TEMPORARY, 77);
    CHECK(wl_evaluation_control_begin(c, host->base.operation_admission) == 0);
    sample_completed_slots = 0;
    CHECK(!wl_columnar_consolidate_test_checkpoint);
    wl_columnar_consolidate_test_checkpoint = sample_pool_stop;
    int rc = wl_columnar_session_mem_sample_controlled(context, c);
    wl_columnar_consolidate_test_checkpoint = NULL;
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && sample_completed_slots == 257
        && context->delta_pool->slot_used == 300 &&
        context->delta_pool->slab == slab
        && !context->cleanup_active && !context->cleanup_pending &&
        !context->cleanup_reserved_bytes);
    wl_mem_ledger_snapshot_t snapshot;
    wl_mem_ledger_snapshot(&context->mem_ledger, &snapshot);
    CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_STORED] == 55
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] == 77);
    CHECK(wl_evaluation_control_finish(c, host->base.operation_admission, rc,
        rc) == rc);
    /* Cancellation is sticky across completed attempts; the caller resets
     * only after the original frame has truly drained and returned. */
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_configure(c, 0) == 0
        && wl_evaluation_control_begin(c, host->base.operation_admission) == 0
        && wl_columnar_session_mem_sample_controlled(context, c) == 0);
    wl_mem_ledger_snapshot(&context->mem_ledger, &snapshot);
    CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_STORED] == 0
        && snapshot.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] > 0
        && !context->cleanup_reserved_bytes &&
        context->delta_pool->slot_used == 300);
    CHECK(wl_evaluation_control_finish(c, host->base.operation_admission, 0,
        0) == 0);
    col_rel_t *slot = (col_rel_t *)slab;
    const void *identity = slot->retained_reservation.identity;
    CHECK(identity == &slot->retained_reservation);
    slot->retained_reservation.identity = NULL;
    CHECK(wl_evaluation_control_begin(c, host->base.operation_admission) == 0);
    CHECK(wl_columnar_session_mem_sample_controlled(context, c) == EINVAL
        && !context->cleanup_active && !context->cleanup_pending);
    CHECK(wl_evaluation_control_finish(c, host->base.operation_admission,
        EINVAL, EINVAL) == EINVAL);
    slot->retained_reservation.identity = identity;
    slot->ncols = 1; slot->capacity = 1;
    CHECK(col_rel_transport_bytes(slot) == UINT64_MAX);
    CHECK(wl_evaluation_control_begin(c, host->base.operation_admission) == 0
        && wl_columnar_session_mem_sample_controlled(context, c) == 0);
    wl_mem_ledger_snapshot(&context->mem_ledger, &snapshot);
    CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] == UINT64_MAX);
    CHECK(wl_evaluation_control_finish(c, host->base.operation_admission, 0,
        0) == 0);
    slot->ncols = 0; slot->capacity = 0;
    col_session_mem_sample(context);
    wl_mem_ledger_snapshot(&context->mem_ledger, &snapshot);
    CHECK(snapshot.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] == 0);
    CHECK(col_rel_destroy_checked(like) == 0);
    delta_pool_destroy(context->delta_pool); free(context); destroy(host);
}

static void original_compound_gc(void)
{
    for (uint32_t mode = 0; mode < 3; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        wl_compound_arena_t *arena = s->compound_arena;
        wl_compound_arena_t *raw = wl_compound_arena_create(7, 256,
                arena->max_epochs);
        CHECK(arena && raw);
        for (uint32_t i = 0; i < 300; i++) {
            uint64_t a = wl_compound_arena_alloc(arena, 8);
            uint64_t b = wl_compound_arena_alloc(raw, 8);
            CHECK(a && b);
            if (!(i & 1)) CHECK(wl_compound_arena_retain(arena, a, -1) == 0
                    && wl_compound_arena_retain(raw, b, -1) == 0);
        }
        uint32_t used = arena->gens[0].used;
        uint64_t live = arena->live_handles;
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        wl_arena_compound_arena_gc_record_t *record = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0
            && wl_columnar_eval_stack_cleanup_compound_gc(s, frame, &record,
            c) == 0);
        CHECK(record && !record->mutation.arena && record->phase == 1);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_evaluation_control_configure(c,
            mode == 1 ? 32+16+32+257*8 : 0) == 0
            && wl_evaluation_control_begin(c,
            s->base.operation_admission) == 0);
        int rc = wl_arena_compound_arena_gc_step(record, arena, c);
        if (mode == 1) {
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                record->cursor == 257
                && !record->consumed && arena->current_epoch == 0
                && arena->gens[0].entry_count == 300 &&
                arena->gens[0].used == used
                && arena->live_handles == live
                && record->mutation.identity == (uintptr_t)&record->mutation);
            CHECK(wl_arena_compound_arena_gc_retire(record, rc, NULL) == rc
                && !record->mutation.arena);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && wl_evaluation_control_finish(c, s->base.operation_admission,
                rc, rc) == rc);
            CHECK(wl_evaluation_control_configure(c, 0) == 0
                && wl_evaluation_control_begin(c,
                s->base.operation_admission) == 0);
            record = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0
                && wl_columnar_eval_stack_cleanup_compound_gc(s, frame, &record,
                c) == 0
                && wl_arena_compound_arena_gc_step(record, arena, c) == 0);
        } else CHECK(rc == 0);
        CHECK(record->consumed && record->result == 150 &&
            arena->current_epoch == 1
            && arena->gens[0].entry_count == 0 && arena->gens[0].used == 0
            && arena->live_handles == live);
        CHECK(wl_compound_arena_gc_epoch_boundary(raw) == record->result
            && raw->current_epoch == arena->current_epoch &&
            raw->live_handles == live);
        if (mode == 2) {
            uintptr_t identity = record->mutation.identity;
            CHECK(identity == (uintptr_t)&record->mutation);
            int primary = wl_evaluation_control_charge(c, UINT64_MAX);
            CHECK(primary == EOVERFLOW);
            record->mutation.identity = 0;
            CHECK(wl_arena_compound_arena_gc_retire(record, primary,
                NULL) == primary
                && record->mutation.arena == arena && record->consumed);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL
                && !frame && s->cleanup_pending && arena->current_epoch == 1);
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                primary, primary) == primary);
            record->mutation.identity = identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0
                && !s->cleanup_pending && arena->current_epoch == 1
                && !atomic_load(&arena->access_gate));
        } else {
            CHECK(wl_arena_compound_arena_gc_retire(record, 0, c) == 0
                && !record->mutation.arena);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0) == 0);
        }
        wl_compound_arena_free(raw); destroy(s);
    }
}

/* Primitive policy proof: real holds/gates/freeze and an actually exhausted
 * single-epoch arena. This does not claim backend subpass integration. */
static void original_compound_gc_skips(void)
{
    for (uint32_t mode = 0; mode < 4; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        wl_compound_arena_t *arena = wl_compound_arena_create(9, 256, 1);
        wl_compound_arena_t *raw = wl_compound_arena_create(9, 256, 1);
        CHECK(arena && raw && wl_compound_arena_alloc(arena, 8)
            && wl_compound_arena_alloc(raw, 8));
        wl_compound_arena_mutation_t busy = {0}, raw_busy = {0};
        wl_arena_compound_arena_gc_hold_t hold = {0}, raw_hold = {0};
        if (mode == 0) {
            CHECK(wl_compound_arena_mutation_begin(arena, &busy)
                && wl_compound_arena_mutation_begin(raw, &raw_busy));
        } else if (mode == 1) {
            CHECK(wl_arena_compound_arena_gc_hold_acquire(arena, &hold) == 0
                && wl_arena_compound_arena_gc_hold_acquire(raw,
                &raw_hold) == 0);
        } else if (mode == 2) {
            wl_compound_arena_freeze(arena); wl_compound_arena_freeze(raw);
            CHECK(arena->frozen && raw->frozen);
        } else {
            CHECK(wl_compound_arena_gc_epoch_boundary(arena) == 0
                && wl_compound_arena_gc_epoch_boundary(raw) == 0
                && arena->current_epoch == arena->max_epochs);
            /* Raw saturation wins over freeze. Both states are real. */
            wl_compound_arena_freeze(arena); wl_compound_arena_freeze(raw);
        }
        uint32_t epoch = arena->current_epoch, used = arena->gens[0].used;
        uint32_t count = arena->gens[0].entry_count;
        uint64_t live = arena->live_handles;
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        wl_arena_compound_arena_gc_record_t *record = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0
            && wl_columnar_eval_stack_cleanup_compound_gc(s, frame, &record,
            c) == 0
            && wl_arena_compound_arena_gc_step(record, arena, c) == 0);
        CHECK(record->consumed && record->cursor == 0
            && record->result == wl_compound_arena_gc_epoch_boundary(raw)
            && arena->current_epoch == epoch && raw->current_epoch == epoch
            && arena->gens[0].used == used &&
            arena->gens[0].entry_count == count
            && arena->live_handles == live);
        CHECK(wl_arena_compound_arena_gc_retire(record, 0, c) == 0
            && !record->mutation.arena);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        if (mode == 0) CHECK(wl_compound_arena_mutation_end(&busy)
                && wl_compound_arena_mutation_end(&raw_busy));
        if (mode == 1) CHECK(wl_arena_compound_arena_gc_hold_release(&hold) == 0
                && wl_arena_compound_arena_gc_hold_release(&raw_hold) == 0);
        wl_compound_arena_free(arena); wl_compound_arena_free(raw); destroy(s);
    }
}

static void original_transport_shape(void)
{
    for (uint32_t mode = 0; mode < 2; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        col_rel_t *r = col_rel_new_auto("sample", 300);
        int64_t row[300] = {0};
        CHECK(r && col_rel_append_row(r, row) == 0
            && col_rel_enable_timestamps(r) == 0 && session_add_rel(s, r) == 0);
        uint64_t raw = col_rel_transport_bytes(r);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        uint64_t frame_bytes = s->cleanup_reserved_bytes;
        wl_columnar_relation_source_group_t *group =
            wl_columnar_eval_stack_cleanup_sources(frame);
        CHECK(group && wl_columnar_relation_source_group_acquire_controlled(
                group, r, NULL, c) == 0);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_evaluation_control_configure(c, mode ? 64+6+64+257*8 : 0) == 0
            && wl_evaluation_control_begin(c,
            s->base.operation_admission) == 0);
        uint64_t bytes = 17;
        int rc = wl_columnar_relation_transport_bytes_controlled(s, frame, r,
                &bytes, c);
        if (mode) {
            wl_evaluation_control_report_t report;
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && bytes == 17
                && group->owners[0].held
                && (group->descriptors[0].reader_held ||
                group->descriptors[0].writer_held));
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                rc, rc) == rc);
            CHECK(wl_evaluation_control_report(c, &report) == 0
                && report.consumed == 64+6+64+257*8);
        } else {
            CHECK(rc == 0 && bytes == raw);
            CHECK(wl_columnar_relation_source_group_finish_controlled(group,
                c) == 0
                && !group->descriptor_count && !group->owner_count);
            CHECK(wl_evaluation_control_finish(c, s->base.operation_admission,
                0, 0) == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && !s->cleanup_pending && !s->cleanup_active
            && !wl_columnar_source_access_gate_busy(&r->descriptor_access)
            && !wl_columnar_source_access_gate_busy(&r->source_access));
        CHECK(wl_evaluation_control_configure(c, 0) == 0
            && wl_evaluation_control_begin(c,
            s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_begin(s, &frame) == 0);
        group = wl_columnar_eval_stack_cleanup_sources(frame);
        CHECK(group &&
            wl_columnar_relation_source_group_acquire_controlled(group, r, NULL,
            c) == 0
            && wl_columnar_relation_transport_bytes_controlled(s, frame, r,
            &bytes, c) == 0
            && bytes == raw && col_rel_transport_bytes(r) == raw);
        CHECK(wl_columnar_relation_source_group_finish_controlled(group, c) == 0
            && !group->descriptor_count && !group->owner_count);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0
            && wl_columnar_session_mem_sample_controlled(s, c) == 0
            && !s->cleanup_reserved_bytes && !s->cleanup_pending);
        wl_mem_ledger_snapshot_t sampled;
        wl_mem_ledger_snapshot(&s->mem_ledger, &sampled);
        CHECK(sampled.subsys_bytes[WL_MEM_SUBSYS_STORED] == raw
            && sampled.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] == frame_bytes);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        destroy(s);
    }
}

static void original_retained_frame_without_governor(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *host = COL_SESSION(base);
    wl_col_session_t *context = calloc(1, sizeof(*context));
    CHECK(context && !context->memory_governor);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, host->base.operation_admission) == 0);
    host->base.evaluation_control = c; context->base.evaluation_control = c;
    context->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(c, host->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(context, &frame, c) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *original = frame;
    uint64_t bytes = context->cleanup_reserved_bytes;
    CHECK(bytes && !context->memory_governor);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
        && context->serial_completion.frame == original
        && wl_columnar_eval_stack_cleanup_idle(context, NULL) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_acquire(context, &frame, c) == 0
        && frame == original && context->cleanup_reserved_bytes == bytes);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
        && wl_columnar_eval_stack_cleanup_retained(context) == 0
        && !context->serial_completion.frame &&
        !context->cleanup_reserved_bytes);
    CHECK(wl_evaluation_control_finish(c, host->base.operation_admission, 0,
        0) == 0);
    free(context); destroy(host);
}

static void original_retained_frame_stop(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        CHECK(s->memory_governor);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(s->memory_governor);
        uint64_t baseline = wl_columnar_memory_reserved(g);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        s->serial_completion.pending = true; s->serial_completion.plan = &plan;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        retained_frame_token = NULL; retained_frame_control = c;
        CHECK(!wl_columnar_memory_governor_test_checkpoint);
        wl_columnar_memory_governor_test_checkpoint = retained_frame_commit;
        int acquired = wl_columnar_eval_stack_cleanup_acquire(s, &frame, c);
        wl_columnar_memory_governor_test_checkpoint = NULL;
        retained_frame_control = NULL;
        CHECK(acquired == 0);
        wl_columnar_eval_stack_cleanup_frame_t *original = frame;
        CHECK(retained_frame_token &&
            retained_frame_token->identity == retained_frame_token
            && atomic_load_explicit(&retained_frame_token->state,
            memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            && atomic_load_explicit(&retained_frame_token->owner_bits,
            memory_order_acquire)
            == (uintptr_t)original);
        uint64_t frame_bytes = retained_frame_token->bytes;
        CHECK(frame_bytes == s->cleanup_reserved_bytes
            && wl_columnar_memory_reserved(g) == baseline + frame_bytes);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
            && s->serial_completion.frame == original
            && s->serial_completion.mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
            && !s->serial_completion.payload_ready);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_evaluation_control_configure(c, mode ? 0 : 32 + 128) == 0);
        const void *identity = retained_frame_token->identity;
        if (mode == 1) retained_frame_token->identity = NULL;
        if (mode == 2) s->serial_completion.mode_frame = NULL;
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        int rc = wl_columnar_eval_stack_cleanup_acquire(s, &frame, c);
        CHECK(rc == (mode ? EINVAL : WL_EVALUATION_CONTROL_WORK_LIMIT)
            && !frame && s->serial_completion.frame == original
            && !s->cleanup_active && !s->cleanup_pending
            && s->cleanup_reserved_bytes == frame_bytes
            && wl_columnar_memory_reserved(g) == baseline + frame_bytes);
        if (!mode) CHECK(s->serial_completion.mode ==
                WL_COLUMNAR_SERIAL_FRAME_OUTPUT
                && !s->serial_completion.payload_ready);
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, rc,
            rc > 0 ? rc : 0) == rc);
        /* Only saved scalars are restored after the serialized return. */
        if (mode == 1) retained_frame_token->identity = identity;
        if (mode == 2) s->serial_completion.mode_frame = original;
        CHECK(wl_columnar_eval_stack_cleanup_retained_drain(s) == 0
            && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0);
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_evaluation_control_configure(c, 0) == 0);
        CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0
            && frame == original && retained_frame_token->identity == identity
            && retained_frame_token->bytes == frame_bytes
            && wl_columnar_memory_reserved(g) == baseline + frame_bytes);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0
            && !s->serial_completion.frame && !s->cleanup_reserved_bytes
            && wl_columnar_memory_reserved(g) == baseline);
        s->serial_completion.pending = false;
        CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
            0) == 0);
        retained_frame_token = NULL; destroy(s);
    }
}

static void original_generated_rule_body(bool retained)
{
    const char *text =
        ".decl edge(x:int32,y:int32)\n.decl reach(x:int32,y:int32)\n"
        "reach(x,y):-edge(x,y).\nreach(x,z):-reach(x,y),edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program, &plan) == 0 && plan->stratum_count == 1
        && plan->strata[0].is_recursive && plan->strata[0].relation_count == 1);
    const wl_plan_relation_t *rp = &plan->strata[0].relations[0];
    const wl_plan_op_type_t expected[7] = {WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP,
                                           WL_PLAN_OP_VARIABLE, WL_PLAN_OP_JOIN,
                                           WL_PLAN_OP_CONCAT,
                                           WL_PLAN_OP_CONSOLIDATE,
                                           WL_PLAN_OP_EXCHANGE};
    CHECK(rp->op_count == 7);
    for (unsigned i = 0; i < 7; i++) CHECK(rp->ops[i].op == expected[i]
            && rp->ops[i].delta_mode == WL_DELTA_AUTO);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    CHECK(!s->diff_enabled && s->delta_pool && s->eval_arena
        && !session_find_rel(s, "reach"));
    int64_t edges[4] = {1, 2, 2, 3};
    CHECK(wl_backend_columnar()->session_insert(base, "edge", edges, 2,
        2) == 0);
    col_rel_t *head = NULL;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    if (retained) {
        s->serial_completion.pending = true;
        s->serial_completion.plan = plan;
    }
    CHECK(wl_columnar_eval_serial_recursive_register_head(s, "reach", c) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *original_frame =
        s->serial_completion.frame;
    if (retained) CHECK(original_frame
            && s->serial_completion.mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
            && !s->serial_completion.payload_ready
            && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0);
    head = session_find_rel(s, "reach");
    CHECK(head && head->ncols == 0 && head->nrows == 0
        && !head->schema_ok && !head->columns
        && !s->recursive.active && !s->cleanup_pending
        && s->serial_completion.frame == original_frame);
    CHECK(wl_columnar_eval_serial_recursive_begin(s, plan, 0, c) == 0);
    bool supported = false;
    CHECK(wl_columnar_eval_plan_recursive_validate(rp, c,
        &supported) == 0 && supported);
    CHECK(wl_columnar_eval_serial_framed_relation(rp, s, true) == 0);
    CHECK(head->ncols == 2 && head->nrows == 2
        && head->columns[0][0] == 1 && head->columns[1][0] == 2
        && head->columns[0][1] == 2 && head->columns[1][1] == 3
        && !s->cleanup_pending && s->serial_completion.frame == original_frame);
    int fast = 0;
    CHECK(wl_columnar_eval_serial_recursive_consolidate(s, head, 0, 0, &fast,
        c) == 0
        && s->recursive.delta_rels[0] &&
        s->recursive.delta_rels[0]->nrows == 2);
    if (retained) CHECK(s->serial_completion.frame == original_frame
            && s->cleanup_active_count == 0 && s->cleanup_pending_count == 0
            && s->cleanup_reserved_bytes == 3864
            && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0);
    uint32_t compound_epoch = s->compound_arena->current_epoch;
    uint64_t arena_capacity = s->eval_arena->capacity;
    CHECK(wl_columnar_eval_serial_recursive_boundary(s, c) == 0
        && s->compound_arena->current_epoch == compound_epoch + 1u
        && s->recursive.reset_active && s->delta_pool->slot_used == 0
        && s->delta_pool->arena_used == 0 && s->eval_arena->used == 0
        && s->eval_arena->capacity == arena_capacity
        && s->serial_completion.frame == original_frame);
    CHECK(wl_columnar_eval_serial_recursive_register_delta(s, 0, c) == 0);
    CHECK(wl_columnar_eval_serial_recursive_pool_barrier(s, c) == 0);
    s->current_iteration = 1;
    s->recursive.effective_iteration = 1;
    CHECK(wl_columnar_eval_serial_recursive_snapshot(s, 0, c) == 0
        && s->recursive.snap[0] == 2 &&
        s->serial_completion.frame == original_frame);
    int body_rc = wl_columnar_eval_serial_framed_relation(rp, s, true);
    if (body_rc || head->nrows != 5) fprintf(stderr,
            "generated recursive body iteration1 rc=%d rows=%u phase=%u pending=%d\n",
            body_rc, head->nrows, s->recursive.phase,
            s->cleanup_pending != NULL);
    CHECK(body_rc == 0 && head->nrows == 5);
    int merge_rc = wl_columnar_eval_serial_recursive_consolidate(s, head, 0,
            s->recursive.snap[0], &fast, c);
    if (merge_rc || head->nrows != 3 || !s->recursive.delta_rels[0]
        || s->recursive.delta_rels[0]->nrows != 1) fprintf(stderr,
            "generated recursive merge iteration1 rc=%d rows=%u delta=%u\n",
            merge_rc,
            head->nrows,
            s->recursive.delta_rels[0] ? s->recursive.delta_rels[0]->nrows :
            UINT32_MAX);
    CHECK(merge_rc == 0
        && head->nrows == 3 && s->recursive.delta_rels[0]
        && s->recursive.delta_rels[0]->nrows == 1
        && s->recursive.delta_rels[0]->columns[0][0] == 1
        && s->recursive.delta_rels[0]->columns[1][0] == 3
        && s->recursive.delta_rels[0]->timestamps[0].iteration == 1
        && !s->cleanup_pending && s->serial_completion.frame == original_frame);
    CHECK(wl_columnar_eval_serial_recursive_retire(s, 0, NULL) == 0);
    if (retained) {
        CHECK(s->serial_completion.frame == original_frame
            && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0
            && !s->serial_completion.frame && !s->cleanup_reserved_bytes);
        s->serial_completion.pending = false;
    }
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(s); wl_plan_free(plan); wirelog_program_free(program);
}

typedef struct wl_recursive_capture {
    uint64_t count, digest;
} wl_recursive_capture_t;
static void recursive_capture(const char *name, const int64_t *row,
    uint32_t width, void *data)
{
    wl_recursive_capture_t *capture = data;
    CHECK(name && strcmp(name, "reach") == 0 && row && width == 2);
    for (uint32_t i = 0; i < width; i++)
        capture->digest = capture->digest *
            UINT64_C(1099511628211) ^ (uint64_t)row[i];
    capture->count++;
}

static wl_col_session_t *terminal_reader_session;
static wl_columnar_source_access_reader_t terminal_reader;
static uint32_t terminal_plan_visits;
static void terminal_reader_after_plan(wl_col_session_t *sess,
    eval_stack_t *stack, eval_entry_t *result)
{
    (void)stack; (void)result;
    if (sess != terminal_reader_session) return;
    terminal_plan_visits++;
    if (sess->current_iteration < 2 || terminal_reader.owner) return;
    col_rel_t *delta = session_find_rel(sess, "$d$reach");
    CHECK(delta && !delta->pool_owned
        && wl_columnar_source_access_reader_acquire(&delta->source_access,
        &terminal_reader) == 0);
}

static void original_generated_terminal_reader(void)
{
    const char *text =
        ".decl edge(x:int32,y:int32)\n.decl reach(x:int32,y:int32)\n"
        "reach(x,y):-edge(x,y).\nreach(x,z):-reach(x,y),edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program, &plan) == 0 && plan->stratum_count == 1
        && plan->strata[0].is_recursive &&
        plan->strata[0].relations[0].op_count == 7);
    wl_session_t *base = NULL, *raw_base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
        && wl_session_create(wl_backend_columnar(), plan, 1, &raw_base) == 0);
    wl_col_session_t *s = COL_SESSION(base), *raw = COL_SESSION(raw_base);
    int64_t edges[] = {1, 2, 2, 3};
    CHECK(wl_backend_columnar()->session_insert(base, "edge", edges, 2, 2) == 0
        && wl_backend_columnar()->session_insert(raw_base, "edge", edges, 2,
        2) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0
        && wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    terminal_reader_session = s; terminal_plan_visits = 0;
    CHECK(!terminal_reader.owner && !wl_columnar_eval_serial_test_after_plan);
    wl_columnar_eval_serial_test_after_plan = terminal_reader_after_plan;
    wl_recursive_capture_t got = {0}, twin = {0};
    int rc = wl_backend_columnar()->session_snapshot(base, recursive_capture,
            &got);
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (rc != EBUSY) fprintf(stderr,
            "terminal reader rc=%d visits=%u held=%d phase=%u stage=%u relation=%u\n",
            rc, terminal_plan_visits, terminal_reader.owner != NULL,
            s->serial_completion.phase, s->serial_completion.compact,
            s->serial_completion.relation);
    col_rel_t *head = session_find_rel(s, "reach");
    col_rel_t *delta = session_find_rel(s, "$d$reach");
    CHECK(rc == EBUSY && terminal_reader.owner && delta && head &&
        head->nrows == 3
        && got.count == 0 && s->eval_stratum_heads_final
        && s->serial_completion.pending
        && s->serial_completion.phase ==
        WL_COLUMNAR_SERIAL_PHASE_RECURSIVE_TERMINAL
        && s->serial_completion.compact == 6 && !s->recursive.active
        && !s->recursive.cleanup_pending && !s->cleanup_pending);
    uint32_t visits = terminal_plan_visits;
    uint32_t frontier_epoch = s->frontiers[0].outer_epoch;
    uint32_t frontier_iteration = s->frontiers[0].iteration;
    CHECK(wl_columnar_source_access_reader_release(&terminal_reader) == 0);
    wl_columnar_eval_serial_test_after_plan = terminal_reader_after_plan;
    rc = wl_backend_columnar()->session_snapshot(base, recursive_capture, &got);
    wl_columnar_eval_serial_test_after_plan = NULL;
    CHECK(rc == 0 && terminal_plan_visits == visits && got.count == 3
        && head->nrows == 3 && !session_find_rel(s, "$d$reach")
        && s->frontiers[0].outer_epoch == frontier_epoch
        && s->frontiers[0].iteration == frontier_iteration
        && !s->serial_completion.pending && !s->serial_completion.frame
        && !s->cleanup_reserved_bytes);
    CHECK(wl_backend_columnar()->session_snapshot(raw_base, recursive_capture,
        &twin) == 0
        && twin.count == got.count && twin.digest == got.digest);
    terminal_reader_session = NULL;
    destroy(s); destroy(raw); wl_plan_free(plan); wirelog_program_free(program);
}

static wl_col_session_t *recursive_stop_session;
static bool recursive_stop_terminal, recursive_stop_reached;
static uint32_t recursive_stop_plan_visits;
static void recursive_stop_after_plan(wl_col_session_t *sess,
    eval_stack_t *stack, eval_entry_t *result)
{
    (void)stack; (void)result;
    if (sess == recursive_stop_session) recursive_stop_plan_visits++;
}
static void recursive_sample_stop(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)record; (void)r;
    if (stage != 124 || progress != 2 || recursive_stop_reached
        || recursive_stop_session->eval_stratum_heads_final !=
        recursive_stop_terminal) return;
    recursive_stop_reached = true;
    wl_evaluation_control_request_cancel(c);
}

static void original_generated_stopped_retry(void)
{
    for (uint32_t terminal = 0; terminal < 2; terminal++) {
        const char *text =
            ".decl edge(x:int32,y:int32)\n.decl reach(x:int32,y:int32)\n"
            "reach(x,y):-edge(x,y).\nreach(x,z):-reach(x,y),edge(y,z).\n";
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        CHECK(program && error == WIRELOG_OK);
        wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
        wl_sip_apply(program, NULL);
        wl_plan_t *plan = NULL;
        CHECK(wl_plan_from_program(program,
            &plan) == 0 && plan->stratum_count == 1
            && plan->strata[0].is_recursive &&
            plan->strata[0].relation_count == 1);
        const wl_plan_relation_t *rp = &plan->strata[0].relations[0];
        const wl_plan_op_type_t shape[7] = {WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP,
                                            WL_PLAN_OP_VARIABLE,
                                            WL_PLAN_OP_JOIN, WL_PLAN_OP_CONCAT,
                                            WL_PLAN_OP_CONSOLIDATE,
                                            WL_PLAN_OP_EXCHANGE};
        CHECK(rp->op_count == 7);
        for (uint32_t i = 0; i < 7; i++) CHECK(rp->ops[i].op == shape[i]
                && rp->ops[i].delta_mode == WL_DELTA_AUTO);
        wl_session_t *base = NULL, *raw_base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
            && wl_session_create(wl_backend_columnar(), plan, 1,
            &raw_base) == 0);
        wl_col_session_t *s = COL_SESSION(base), *raw = COL_SESSION(raw_base);
        CHECK(!s->diff_enabled && s->delta_pool && s->eval_arena &&
            s->compound_arena
            && !session_find_rel(s, "reach") && !session_find_rel(raw,
            "reach"));
        int64_t edges[] = {1, 2, 2, 3};
        CHECK(wl_backend_columnar()->session_insert(base, "edge", edges, 2,
            2) == 0
            && wl_backend_columnar()->session_insert(raw_base, "edge", edges, 2,
            2) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        recursive_stop_session = s; recursive_stop_terminal = terminal != 0;
        recursive_stop_reached = false; recursive_stop_plan_visits = 0;
        CHECK(!wl_columnar_eval_serial_test_after_plan);
        wl_columnar_eval_serial_test_after_plan = recursive_stop_after_plan;
        CHECK(!wl_columnar_consolidate_test_checkpoint);
        wl_columnar_consolidate_test_checkpoint = recursive_sample_stop;
        wl_recursive_capture_t got = {0}, twin = {0};
        int rc = wl_backend_columnar()->session_snapshot(base,
                recursive_capture, &got);
        wl_columnar_consolidate_test_checkpoint = NULL;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && recursive_stop_reached
            && got.count == 0 && s->serial_completion.pending
            && s->eval_stratum_heads_final == (terminal != 0)
            && !s->recursive.active && !s->recursive.cleanup_pending
            && !s->cleanup_pending);
        uint32_t before_retry_visits = recursive_stop_plan_visits;
        CHECK(before_retry_visits > 0);
        CHECK(wl_evaluation_control_reset(c) == 0);
        rc = wl_backend_columnar()->session_snapshot(base, recursive_capture,
                &got);
        CHECK(wl_backend_columnar()->session_snapshot(raw_base,
            recursive_capture, &twin) == 0);
        col_rel_t *head = session_find_rel(s, "reach");
        if (rc || got.count != twin.count || got.digest != twin.digest)
            fprintf(stderr,
                "recursive stopped retry terminal=%u rc=%d rows=%u count=%llu raw=%llu phase=%u\n",
                terminal, rc, head ? head->nrows : UINT32_MAX,
                (unsigned long long)got.count, (unsigned long long)twin.count,
                s->serial_completion.phase);
        CHECK((terminal ? recursive_stop_plan_visits == before_retry_visits
            : recursive_stop_plan_visits > before_retry_visits));
        wl_columnar_eval_serial_test_after_plan = NULL;
        CHECK(rc == 0 && got.count == 3 && twin.count == got.count
            && twin.digest == got.digest && head && head->nrows == 3
            && !s->recursive.active && !s->recursive.cleanup_pending
            && !s->cleanup_pending && !s->serial_completion.pending
            && !s->serial_completion.frame && !s->cleanup_reserved_bytes);
        recursive_stop_session = NULL;
        destroy(s); destroy(raw); wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static void original_generated_backend(void)
{
    for (uint32_t snapshot = 0; snapshot < 2; snapshot++) {
        const char *text =
            ".decl edge(x:int32,y:int32)\n.decl reach(x:int32,y:int32)\n"
            "reach(x,y):-edge(x,y).\nreach(x,z):-reach(x,y),edge(y,z).\n";
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        CHECK(program && error == WIRELOG_OK);
        wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
        wl_sip_apply(program, NULL);
        wl_plan_t *plan = NULL;
        CHECK(wl_plan_from_program(program,
            &plan) == 0 && plan->stratum_count == 1
            && plan->strata[0].is_recursive &&
            plan->strata[0].relation_count == 1);
        const wl_plan_relation_t *rp = &plan->strata[0].relations[0];
        const wl_plan_op_type_t shape[7] = {WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP,
                                            WL_PLAN_OP_VARIABLE,
                                            WL_PLAN_OP_JOIN, WL_PLAN_OP_CONCAT,
                                            WL_PLAN_OP_CONSOLIDATE,
                                            WL_PLAN_OP_EXCHANGE};
        CHECK(rp->op_count == 7);
        for (uint32_t i = 0; i < 7; i++) CHECK(rp->ops[i].op == shape[i]
                && rp->ops[i].delta_mode == WL_DELTA_AUTO);
        wl_session_t *base = NULL, *raw_base = NULL;
        /* Finite sampled overlap proof under the same enforcing budget.
         * Each twin owns its genuine default arena/pool and separate governor. */
        const uint64_t budget = UINT64_C(129) * 1024u * 1024u;
        for (uint32_t twin_index = 0; twin_index < 2; twin_index++) {
            wl_columnar_memory_resolution_t resolution = {
                .budget_bytes = budget, .usable_bytes = budget,
                .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
            };
            wl_columnar_memory_governor_ref_t *governor =
                wl_columnar_memory_governor_ref_create(&resolution);
            CHECK(governor);
            wl_session_options_t options; wl_session_options_init(&options);
            options.memory_governor = governor;
            CHECK(wl_session_create_with_options(wl_backend_columnar(), plan,
                1, &options, twin_index ? &raw_base : &base) == 0);
            wl_columnar_memory_governor_ref_release(governor);
        }
        wl_col_session_t *s = COL_SESSION(base), *raw = COL_SESSION(raw_base);
        CHECK(!s->diff_enabled && s->delta_pool && s->eval_arena &&
            s->compound_arena
            && !session_find_rel(s, "reach") && !session_find_rel(raw,
            "reach"));
        int64_t edges[] = {1, 2, 2, 3};
        CHECK(wl_backend_columnar()->session_insert(base, "edge", edges, 2,
            2) == 0
            && wl_backend_columnar()->session_insert(raw_base, "edge", edges, 2,
            2) == 0);
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        for (uint32_t call = 0; call < 2; call++) {
            if (call) {
                int64_t next[] = {3, 4};
                CHECK(wl_backend_columnar()->session_insert(base, "edge", next,
                    1, 2) == 0
                    && wl_backend_columnar()->session_insert(raw_base, "edge",
                    next, 1, 2) == 0);
            }
            wl_recursive_capture_t got = {0}, twin = {0};
            int rc = snapshot ? wl_backend_columnar()->session_snapshot(base,
                    recursive_capture,
                    &got) : wl_backend_columnar()->session_step(base);
            if (rc) fprintf(stderr,
                    "recursive backend snapshot=%u call=%u rc=%d phase=%u recursive=%u active=%d pending=%d\n",
                    snapshot, call, rc, s->serial_completion.phase,
                    s->recursive.phase,
                    s->recursive.active, s->cleanup_pending != NULL);
            CHECK(rc == 0);
            CHECK((snapshot ? wl_backend_columnar()->session_snapshot(raw_base,
                recursive_capture,
                &twin) : wl_backend_columnar()->session_step(raw_base)) == 0);
            col_rel_t *head = session_find_rel(s, "reach"),
                *raw_head = session_find_rel(raw, "reach");
            if (!head || !raw_head || head->ncols != 2
                || head->nrows != (call ? 6u : 3u) ||
                raw_head->nrows != head->nrows)
                fprintf(stderr,
                    "recursive content snapshot=%u call=%u width=%u rows=%u rawrows=%u iteration=%u rawiteration=%u\n",
                    snapshot, call, head ? head->ncols : UINT32_MAX,
                    head ? head->nrows : UINT32_MAX,
                    raw_head ? raw_head->nrows : UINT32_MAX,
                    s->total_iterations, raw->total_iterations);
            CHECK(head && raw_head && head->ncols == 2 &&
                head->nrows == (call ? 6u : 3u)
                && raw_head->nrows == head->nrows);
            for (uint32_t row = 0; row < head->nrows; row++)
                for (uint32_t column = 0; column < 2; column++)
                    CHECK(head->columns[column][row] ==
                        raw_head->columns[column][row]);
            CHECK(!s->recursive.active && !s->recursive.cleanup_pending &&
                !s->cleanup_pending
                && !s->serial_completion.pending && !s->serial_completion.frame
                && !s->cleanup_reserved_bytes && !s->cleanup_active_count &&
                !s->cleanup_pending_count);
            if (snapshot) CHECK(got.count == head->nrows &&
                    twin.count == got.count
                    && twin.digest == got.digest);
            wl_columnar_memory_governor_t *governor =
                wl_columnar_memory_governor_ref_get(s->memory_governor);
            wl_columnar_memory_governor_t *raw_governor =
                wl_columnar_memory_governor_ref_get(raw->memory_governor);
            CHECK(governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && raw_governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
                && governor->budget_bytes == budget &&
                raw_governor->budget_bytes == budget
                && wl_columnar_memory_reserved(governor) <= budget
                && wl_columnar_memory_reserved(raw_governor) <= budget);
        }
        destroy(s); destroy(raw); wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static void original_empty_forced_delta(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *delta = col_rel_new_auto("$d$A", 1);
    CHECK(delta && session_add_rel(s, delta) == 0);
    wl_plan_op_t ops[2] = {0};
    ops[0].op = WL_PLAN_OP_VARIABLE;
    ops[0].relation_name = "A";
    ops[0].delta_mode = WL_DELTA_FORCE_DELTA;
    ops[1].op = WL_PLAN_OP_CONCAT;
    wl_plan_relation_t rp = {0}; rp.ops = ops; rp.op_count = 1;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    bool empty = true;
    CHECK(wl_columnar_eval_plan_empty_forced_delta(&rp, s, 0, &empty, c) == 0
        && !empty && !has_empty_forced_delta(&rp, s, 0));
    CHECK(wl_columnar_eval_plan_empty_forced_delta(&rp, s, 1, &empty, c) == 0
        && empty && has_empty_forced_delta(&rp, s, 1)
        && !s->serial_completion.frame && !s->cleanup_pending);
    rp.op_count = 2;
    CHECK(wl_columnar_eval_plan_empty_forced_delta(&rp, s, 1, &empty, c) == 0
        && !empty && !has_empty_forced_delta(&rp, s, 1));
    rp.op_count = 1; ops[0].relation_name = "missing";
    CHECK(wl_columnar_eval_plan_empty_forced_delta(&rp, s, 1, &empty, c) == 0
        && empty && has_empty_forced_delta(&rp, s, 1));
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    int64_t value = 9;
    CHECK(col_rel_append_row(delta, &value) == 0);
    ops[0].relation_name = "A";
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_plan_empty_forced_delta(&rp, s, 1, &empty, c) == 0
        && !empty && !has_empty_forced_delta(&rp, s, 1)
        && !s->serial_completion.frame && !s->cleanup_pending);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    destroy(s);
}

static void original_generated_aggregate_refusal(void)
{
    const char *text = ".decl edge(x:int32,y:int32)\n.decl L(x:int32,v:int32)\n"
        "L(x,min(x)):-edge(x,y).\nL(y,min(y)):-edge(x,y).\n"
        "L(x,min(v)):-L(y,v),edge(y,x).\n";
    for (uint32_t dirty = 0; dirty < 2; dirty++) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        CHECK(program && error == WIRELOG_OK);
        wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
        wl_sip_apply(program, NULL);
        wl_plan_t *plan = NULL;
        CHECK(wl_plan_from_program(program, &plan) == 0
            && plan->stratum_count == 1 && plan->strata[0].is_recursive
            && plan->strata[0].relation_count == 1);
        const wl_plan_relation_t *rp = &plan->strata[0].relations[0];
        CHECK(rp->recursive_agg.has_spec && rp->op_count == 11);
        const wl_plan_op_type_t shape[] = {WL_PLAN_OP_VARIABLE,
                                           WL_PLAN_OP_REDUCE,
                                           WL_PLAN_OP_VARIABLE,
                                           WL_PLAN_OP_REDUCE,
                                           WL_PLAN_OP_CONCAT,
                                           WL_PLAN_OP_VARIABLE, WL_PLAN_OP_JOIN,
                                           WL_PLAN_OP_REDUCE, WL_PLAN_OP_CONCAT,
                                           WL_PLAN_OP_CONSOLIDATE,
                                           WL_PLAN_OP_EXCHANGE};
        for (uint32_t i = 0; i < 11; i++) CHECK(rp->ops[i].op == shape[i]
                && rp->ops[i].delta_mode == WL_DELTA_AUTO);
        wl_session_t *base = NULL, *raw_base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
            && wl_session_create(wl_backend_columnar(), plan, 1,
            &raw_base) == 0);
        wl_col_session_t *s = COL_SESSION(base), *raw = COL_SESSION(raw_base);
        int64_t edges[] = {1, 2, 2, 3};
        CHECK(wl_backend_columnar()->session_insert(base, "edge", edges, 2,
            2) == 0
            && wl_backend_columnar()->session_insert(raw_base, "edge", edges, 2,
            2) == 0);
        if (dirty) {
            CHECK(wl_backend_columnar()->session_step(base) == 0
                && wl_backend_columnar()->session_step(raw_base) == 0);
            int64_t next[] = {3, 4};
            CHECK(wl_backend_columnar()->session_insert(base, "edge", next, 1,
                2) == 0
                && wl_backend_columnar()->session_insert(raw_base, "edge", next,
                1, 2) == 0);
        }
        col_rel_t *head = session_find_rel(s, "L");
        uint64_t view = head ? head->view_generation : 0;
        uint64_t storage = head ? head->storage_generation : 0;
        uint32_t rows = head ? head->nrows : 0;
        uint32_t registry = s->nrels, pool_used = s->delta_pool->slot_used;
        uint64_t reserved = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
        wl_evaluation_control_t *c = NULL;
        CHECK(wl_evaluation_control_create(0, &c) == 0
            && wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
        CHECK(wl_backend_columnar()->session_step(base) == ENOTSUP);
        CHECK(session_find_rel(s, "L") == head && s->nrels == registry
            && s->delta_pool->slot_used == pool_used && !s->recursive.active
            && !s->cleanup_pending && !s->serial_completion.frame
            && reserved == wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(s->memory_governor)));
        if (head) {
            CHECK(head->nrows == rows && head->view_generation == view
                && head->storage_generation == storage);
            for (uint32_t row = 0; row < rows; row++)
                CHECK(head->columns[0][row] == row+1 &&
                    head->columns[1][row] == 1);
        }
        CHECK(wl_backend_columnar()->session_step(raw_base) == 0);
        col_rel_t *raw_head = session_find_rel(raw, "L");
        CHECK(raw_head && raw_head->nrows == (dirty?4u:3u));
        for (uint32_t row = 0; row < raw_head->nrows; row++)
            CHECK(raw_head->columns[0][row] == row+1 &&
                raw_head->columns[1][row] == 1);
        destroy(s); destroy(raw); wl_plan_free(plan);
        wirelog_program_free(program);
    }
}

static wl_col_session_t *scc_session;
static wl_columnar_source_access_reader_t scc_reader;
static uint32_t scc_visits;
static void scc_after_plan(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *result)
{
    (void)stack; (void)result;
    if (sess != scc_session) return;
    scc_visits++;
    if (sess->current_iteration < 2 || scc_reader.owner) return;
    col_rel_t *delta = session_find_rel(sess, "$d$A");
    CHECK(delta && !delta->pool_owned
        && wl_columnar_source_access_reader_acquire(&delta->source_access,
        &scc_reader) == 0);
}
static void scc_capture(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    wl_recursive_capture_t *capture = data;
    CHECK(name && (!strcmp(name, "A") || !strcmp(name,
        "B")) && row && width == 2);
    capture->digest = capture->digest*UINT64_C(1099511628211)^(uint64_t)name[0];
    for (uint32_t i = 0; i < width; i++)capture->digest = capture->digest*
            UINT64_C(1099511628211)^(uint64_t)row[i];
    capture->count++;
}
static void original_generated_multiple_heads(void)
{
    const char *text = ".decl edge(x:int32,y:int32)\n.decl A(x:int32,y:int32)\n"
        ".decl B(x:int32,y:int32)\nA(x,y):-edge(x,y).\n"
        "A(x,z):-B(x,y),edge(y,z).\nB(x,y):-edge(x,y).\n"
        "B(x,z):-A(x,y),edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program && error == WIRELOG_OK);
    wl_fusion_apply(program, NULL); wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program, &plan) == 0 && plan->stratum_count == 1
        && plan->strata[0].is_recursive && plan->strata[0].relation_count == 2);
    CHECK(!strcmp(plan->strata[0].relations[0].name, "A")
        && !strcmp(plan->strata[0].relations[1].name, "B")
        && plan->strata[0].relations[0].op_count == 7
        && plan->strata[0].relations[1].op_count == 7);
    for (uint32_t head = 0; head < 2; head++) {
        const wl_plan_relation_t *rp = &plan->strata[0].relations[head];
        CHECK(!rp->recursive_agg.has_spec);
        const wl_plan_op_type_t shape[] = {WL_PLAN_OP_VARIABLE, WL_PLAN_OP_MAP,
                                           WL_PLAN_OP_VARIABLE, WL_PLAN_OP_JOIN,
                                           WL_PLAN_OP_CONCAT,
                                           WL_PLAN_OP_CONSOLIDATE,
                                           WL_PLAN_OP_EXCHANGE};
        for (uint32_t i = 0; i < rp->op_count;
            i++) CHECK(rp->ops[i].op == shape[i]
                && rp->ops[i].delta_mode == WL_DELTA_AUTO);
    }
    wl_session_t *base = NULL, *raw_base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
        && wl_session_create(wl_backend_columnar(), plan, 1, &raw_base) == 0);
    wl_col_session_t *s = COL_SESSION(base), *raw = COL_SESSION(raw_base);
    int64_t edges[] = {1, 2, 2, 3};
    CHECK(wl_backend_columnar()->session_insert(base, "edge", edges, 2, 2) == 0
        && wl_backend_columnar()->session_insert(raw_base, "edge", edges, 2,
        2) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0
        && wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    for (uint32_t call = 0; call < 2; call++) {
        if (call) {
            int64_t next[] = {3, 4};
            CHECK(wl_backend_columnar()->session_insert(base, "edge", next, 1,
                2) == 0
                && wl_backend_columnar()->session_insert(raw_base, "edge", next,
                1, 2) == 0);
        }
        scc_session = s; scc_visits = 0;
        CHECK(!scc_reader.owner && !wl_columnar_eval_serial_test_after_plan);
        wl_columnar_eval_serial_test_after_plan = scc_after_plan;
        wl_recursive_capture_t got = {0}, twin = {0};
        int rc = wl_backend_columnar()->session_snapshot(base, scc_capture,
                &got);
        wl_columnar_eval_serial_test_after_plan = NULL;
        if (rc != EBUSY)fprintf(stderr,
                "SCC call%u rc%d visits%u phase%u stage%u\n",
                call, rc, scc_visits, s->serial_completion.phase,
                s->serial_completion.compact);
        CHECK(rc == EBUSY && scc_reader.owner && got.count == 0
            && s->eval_stratum_heads_final
            && s->serial_completion.phase ==
            WL_COLUMNAR_SERIAL_PHASE_RECURSIVE_TERMINAL
            && !s->recursive.active && !s->cleanup_pending
            && session_find_rel(s, "$d$A") && !session_find_rel(s, "$d$B"));
        uint32_t visits = scc_visits, epoch = s->frontiers[0].outer_epoch,
            iteration = s->frontiers[0].iteration;
        CHECK(wl_columnar_source_access_reader_release(&scc_reader) == 0);
        wl_columnar_eval_serial_test_after_plan = scc_after_plan;
        CHECK(wl_backend_columnar()->session_snapshot(base, scc_capture,
            &got) == 0);
        wl_columnar_eval_serial_test_after_plan = NULL;
        CHECK(scc_visits == visits && got.count == (call?12u:6u)
            && s->frontiers[0].outer_epoch == epoch &&
            s->frontiers[0].iteration == iteration
            && !session_find_rel(s, "$d$A") && !session_find_rel(s, "$d$B")
            && !s->serial_completion.frame && !s->cleanup_reserved_bytes);
        CHECK(wl_backend_columnar()->session_snapshot(raw_base, scc_capture,
            &twin) == 0
            && twin.count == got.count && twin.digest == got.digest);
        for (uint32_t i = 0; i < 2; i++) {
            const char *name = i?"B":"A";
            col_rel_t *head = session_find_rel(s, name),
                *other = session_find_rel(raw, name);
            CHECK(head && other && head->nrows == (call?6u:3u) &&
                head->nrows == other->nrows);
            for (uint32_t row = 0; row < head->nrows; row++)for (uint32_t col =
                    0;
                    col < 2; col++)
                    CHECK(head->columns[col][row] == other->columns[col][row]);
        }
    }
    scc_session = NULL;
    destroy(s); destroy(raw); wl_plan_free(plan); wirelog_program_free(program);
}

int main(void)
{
    CHECK(setenv("WIRELOG_DIFF_ENABLED", "0", 1) == 0);
    borrowed_reserve();
    for (uint32_t mode = 0; mode < 3; mode++) adaptive_incremental(mode);
    for (uint32_t mode = 0; mode < 3;
        mode++) original_multirun_compaction(mode, false);
    original_multirun_compaction(0, true);
    suffix_stop_retry();
    original_transfer();
    original_stratum_payload();
    original_timestamp_payload();
    original_delta_selection();
    original_descriptor_preparation();
    original_promotion_credit_transport();
    original_stamp_stop_retry();
    original_promotion_registry();
    original_delta_join();
    original_recursive_consolidation_caller();
    original_registry_prefix_boundaries();
    original_initial_sort();
    original_empty_forced_delta();
    original_pool_reset_window();
    original_registered_delta_removal();
    original_removal_cache_boundary();
    original_hash_retirement_fault();
    original_descriptor_handoff();
    original_registry_dead_union_marker();
    original_canonical_publication_refusal();
    original_sampling_writer_release();
    original_pool_sample();
    original_compound_gc();
    original_compound_gc_skips();
    original_transport_shape();
    original_retained_frame_without_governor();
    original_retained_frame_stop();
    original_generated_rule_body(false);
    original_generated_rule_body(true);
    original_generated_backend();
    original_generated_aggregate_refusal();
    original_generated_multiple_heads();
    puts(
        "recursive R2 aggregate refusal/multirun copy-back257/two-head terminal retry OK");
    original_generated_terminal_reader();
    original_generated_stopped_retry();
    original_canonical_schema();
    original_canonical_schema_stops();
    puts(
        "evaluation_recursive original owner/merge/stamp/registry/AUTO-JOIN primitives OK");
    return 0;
}
