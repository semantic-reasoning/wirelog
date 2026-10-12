/* #1821: original observer ownership and genuine observed STEP callers. */
#define WL_TEST_RELATION_RESIZE_HOOK 1
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/session.h"
#include <stdarg.h>
#include <stdio.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); exit(1); } } while (0)

/* Helper lifecycle evidence, distinct from generated backend admission. */
static wl_columnar_memory_reservation_t *fault_token;
static const wl_columnar_memory_reservation_t *saved_identity;
static unsigned physical_disposals;
static uint64_t expected_metadata_state;

static void metadata_identity_fault(
    const wl_columnar_eval_delta_preparation_t *record,
    wl_columnar_memory_reservation_t *token)
{
    CHECK(record->object == NULL && record->physical_disposed
        && token == &record->metadata && token->identity == token
        && atomic_load_explicit(&token->state, memory_order_acquire)
        == expected_metadata_state);
    CHECK(!physical_disposals++);
    fault_token = token;
    saved_identity = token->identity;
    token->identity = NULL;
}

static void original_metadata_disposal(bool fault, bool completed_capture)
{
    wl_plan_t plan = {0};
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    if (completed_capture) {
        CHECK(wl_columnar_eval_delta_observer_begin_controlled(s, UINT64_MAX,
            c) == 0);
    } else {
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
        wl_columnar_eval_delta_preparation_t *preparation = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_observer(s, frame, &preparation,
            c) == 0);
        CHECK(wl_columnar_eval_delta_observer_allocate_controlled(s, UINT64_MAX,
            preparation, c) == 0 && s->delta_observer);
        CHECK(wl_columnar_eval_delta_hold_controlled(s, preparation, false,
            c) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    }
    wl_columnar_eval_stack_cleanup_frame_t *original =
        s->serial_completion.frame;
    uint64_t frame_bytes = s->cleanup_reserved_bytes;
    CHECK(original && !s->cleanup_active && s->compound_arena
        && atomic_load_explicit(&s->compound_arena->gc_hold_count,
        memory_order_acquire) == 1
        && atomic_load_explicit(&s->compound_arena->access_gate,
        memory_order_acquire) == 0);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    if (fault) {
        physical_disposals = 0;
        expected_metadata_state = completed_capture
            ? WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            : WL_COLUMNAR_MEMORY_RESERVATION_RESERVED;
        wl_columnar_eval_delta_test_physical_disposal = metadata_identity_fault;
    }
    int disposal = wl_columnar_eval_delta_observer_discard(s);
    wl_columnar_eval_delta_test_physical_disposal = NULL;
    if (fault) {
        CHECK(disposal == EINVAL && physical_disposals == 1
            && !s->delta_observer && s->cleanup_pending
            && fault_token && fault_token->identity == NULL
            && atomic_load_explicit(&fault_token->state, memory_order_acquire)
            == expected_metadata_state);
        /* Only the saved original scalar is restored after serialized return. */
        fault_token->identity = saved_identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
        fault_token = NULL; saved_identity = NULL;
        disposal = wl_columnar_eval_delta_observer_discard(s);
    }
    CHECK(disposal == 0
        && !s->delta_observer && !s->delta_observer_reserved_bytes
        && !s->cleanup_active && !s->cleanup_pending
        && s->serial_completion.frame == original
        && s->cleanup_reserved_bytes == frame_bytes
        && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0
        && atomic_load_explicit(&s->compound_arena->gc_hold_count,
        memory_order_acquire) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0
        && !s->serial_completion.frame && !s->cleanup_reserved_bytes);
    wl_evaluation_control_release(c);
    wl_session_destroy(base);
}

/* Separate source-derived release boundary on the genuine original-frame
* replacement slot: validation24 + root load1 + successful root CAS16. */
static void original_replacement_writer(void)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *target = col_rel_new_auto("$writer", 1);
    CHECK(target && target->storage_owner == target);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    CHECK(wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0);
    wl_columnar_eval_delta_replacement_t *record = NULL;
    wl_columnar_relation_bulk_append_t *bulk = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_observer_replacement(s, frame,
        &record, &bulk, control) == 0 && record && record->ready
        && record->initialized_tokens == 3 && bulk);
    record->target = target;
    CHECK(wl_columnar_relation_replacement_writer_acquire_controlled(target,
        &record->transaction, control) == 0);
    wl_columnar_source_access_writer_t *original = &record->transaction.writer;
    CHECK(original->identity == (uintptr_t)original
        && original->owner == &target->source_access
        && original->secondary_owner == &target->descriptor_access
        && record->transaction.writer_acquired
        && atomic_load_explicit(&target->descriptor_access.state,
        memory_order_acquire) == 1
        && atomic_load_explicit(&target->source_access.state,
        memory_order_acquire)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER);
    /* Finish setup before changing host allowance. The original frame and
     * token remain at their actual addresses for the separate release trial. */
    CHECK(wl_evaluation_control_finish(control, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_configure(control, 41) == 0);
    CHECK(wl_evaluation_control_begin(control,
        s->base.operation_admission) == 0);
    int stopped = wl_columnar_relation_replacement_writer_release_controlled(
        target, &record->transaction, control);
    CHECK(stopped == WL_EVALUATION_CONTROL_WORK_LIMIT
        && original == &record->transaction.writer && !original->owner
        && original->secondary_owner == &target->descriptor_access
        && original->identity == (uintptr_t)original && original->thread_valid
        && record->transaction.writer_acquired
        && atomic_load_explicit(&target->source_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&target->descriptor_access.state,
        memory_order_acquire) == 1);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
        && !s->cleanup_active && !s->cleanup_pending
        && atomic_load_explicit(&target->source_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&target->descriptor_access.state,
        memory_order_acquire) == 0);
    CHECK(wl_evaluation_control_finish(control, s->base.operation_admission,
        stopped, 0) == stopped);
    CHECK(wl_evaluation_control_detach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    CHECK(col_rel_destroy_checked(target) == 0);
    wl_evaluation_control_release(control); wl_session_destroy(base);
}

static wl_columnar_eval_delta_replacement_t *staging_record;
static wl_columnar_relation_bulk_append_t *staging_bulk;
static unsigned staged_schema_visits;
static void stage_schema_stop(wl_evaluation_control_t *control,
    int phase, uint64_t progress)
{
    if (phase != 3 || progress != 257) return;
    CHECK(staging_record && staging_record->phase == 2
        && staging_record->transaction.staged
        && staging_bulk && staging_bulk->prepared_schema_ok
        && staging_bulk->prepared_schema.n_children == 257);
    CHECK(!staged_schema_visits++);
    wl_evaluation_control_request_cancel(control);
}

/* Supplied raw transient physical-policy twin. Production exact governor
 * admissions, normalized candidate disposal and final install remain separate. */
static void original_transient_stage(bool stop)
{
    uint32_t width = stop ? 300 : 2;
    col_rel_t *source = col_rel_new_auto("$stage_source", width);
    col_rel_t *target = col_rel_new_auto("$stage_target", width), *raw = NULL;
    CHECK(source && target);
    int64_t *row = malloc((size_t)width * sizeof(*row)); CHECK(row);
    for (uint32_t c = 0; c < width; c++) row[c] = (int64_t)c - 17;
    CHECK(col_rel_append_row(source, row) == 0); free(row);
    CHECK(col_rel_deep_copy(source, &raw, NULL) == 0 && raw);
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    CHECK(wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0);
    eval_entry_t *owner = wl_columnar_eval_stack_cleanup_result(frame);
    CHECK(owner && !owner->rel && !owner->owned);
    owner->rel = source; owner->owned = true;
    wl_columnar_eval_delta_replacement_t *record = NULL;
    wl_columnar_relation_bulk_append_t *bulk = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_observer_replacement(s, frame,
        &record, &bulk, control) == 0);
    record->target = target;
    staged_schema_visits = 0;
    if (stop) {
        staging_record = record; staging_bulk = bulk;
        wl_columnar_relation_test_metadata_checkpoint = stage_schema_stop;
    }
    int rc = wl_columnar_relation_replacement_stage_controlled(record,
            bulk, source, control);
    wl_columnar_relation_test_metadata_checkpoint = NULL;
    staging_record = NULL; staging_bulk = NULL;
    if (stop) {
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && staged_schema_visits == 1
            && !bulk->prepared_schema_ok && !bulk->prepared_schema.release
            && record->transaction.staged && record->reader.owner
            && record->reader.secondary_owner
            && !record->transaction.staged->schema_ok);
    } else {
        col_rel_t *stage = record->transaction.staged;
        CHECK(rc == 0 && record->phase == 3 && stage && stage != source
            && !record->reader.owner && !record->reader.secondary_owner
            && stage->storage_owner == stage && stage->nrows == raw->nrows
            && stage->ncols == raw->ncols && stage->capacity == raw->capacity
            && !stage->memory_governor && !stage->mem_ledger
            && !stage->row_scratch && !stage->dedup_slots
            && stage->schema_ok == raw->schema_ok && stage->col_names);
        for (uint32_t c = 0; c < width; c++) {
            CHECK(stage->columns[c] != source->columns[c]
                && stage->columns[c][0] == raw->columns[c][0]
                && strcmp(stage->col_names[c], raw->col_names[c]) == 0
                && strcmp(stage->schema.children[c]->format,
                raw->schema.children[c]->format) == 0);
        }
    }
    int cleanup_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (cleanup_rc) fprintf(stderr,
            "staged cleanup rc=%d phase=%u width=%u stop=%u\n",
            cleanup_rc, record->phase, width, stop);
    CHECK(cleanup_rc == 0 && !s->cleanup_active && !s->cleanup_pending);
    CHECK(wl_evaluation_control_finish(control, s->base.operation_admission,
        rc, 0) == rc);
    if (stop) {
        /* The original candidate was truly disposed with the stopped frame.
         * Supply an equivalent real input before a fresh host attempt. */
        col_rel_t *retry = NULL;
        CHECK(col_rel_deep_copy(raw, &retry, NULL) == 0 && retry);
        CHECK(wl_evaluation_control_reset(control) == 0);
        CHECK(wl_evaluation_control_begin(control,
            s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0);
        owner = wl_columnar_eval_stack_cleanup_result(frame);
        CHECK(owner && !owner->rel && !owner->owned);
        owner->rel = retry; owner->owned = true;
        record = NULL; bulk = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_observer_replacement(s, frame,
            &record, &bulk, control) == 0);
        record->target = target;
        CHECK(wl_columnar_relation_replacement_stage_controlled(record, bulk,
            retry, control) == 0 && record->phase == 3
            && !record->reader.owner && !record->reader.secondary_owner);
        col_rel_t *stage = record->transaction.staged;
        CHECK(stage && stage->nrows == raw->nrows && stage->ncols == raw->ncols
            && stage->capacity == raw->capacity &&
            stage->schema_ok == raw->schema_ok);
        for (uint32_t c = 0; c < width; c++)
            CHECK(stage->columns[c][0] == raw->columns[c][0]
                && strcmp(stage->col_names[c], raw->col_names[c]) == 0
                && strcmp(stage->schema.children[c]->format,
                raw->schema.children[c]->format) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && !s->cleanup_active && !s->cleanup_pending);
        CHECK(wl_evaluation_control_finish(control, s->base.operation_admission,
            0, 0) == 0);
    }
    CHECK(wl_evaluation_control_detach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    CHECK(col_rel_destroy_checked(raw) == 0 &&
        col_rel_destroy_checked(target) == 0);
    wl_evaluation_control_release(control); wl_session_destroy(base);
}

/* Actual prepare trajectory, separately labelled from backend normalization:
 * raw payload/metadata/dedup admissions precede the transient physical copy. */
static void original_replacement_prepare(bool governed, unsigned fault)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = col_rel_new_auto("$prepare", 2);
    col_rel_t *target = NULL, *raw_target = NULL;
    CHECK(source && col_rel_append_row(source, (int64_t[]){17, -9}) == 0);
    if (fault == 3) CHECK(wl_columnar_eval_dedup_set_init_from_rel(source) == 0
            && source->dedup_cap == 1024 && source->dedup_count == 1);
    if (governed) {
        CHECK(wl_columnar_relation_new_auto_governed("$target", 2, 64, false,
            s->memory_governor, &target) == 0);
        CHECK(wl_columnar_relation_new_auto_governed("$raw_target", 2, 64,
            false,
            s->memory_governor, &raw_target) == 0);
    } else {
        target = col_rel_new_auto("$target", 2);
        raw_target = col_rel_new_auto("$raw_target", 2);
    }
    CHECK(target && raw_target);
    col_rel_replacement_t raw = {0};
    CHECK(col_rel_prepare_replacement(raw_target, source,
        &raw) == 0 && raw.staged);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    CHECK(wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0);
    eval_entry_t *owner = wl_columnar_eval_stack_cleanup_result(frame);
    CHECK(owner); owner->rel = source; owner->owned = true;
    wl_columnar_eval_delta_replacement_t *record = NULL;
    wl_columnar_relation_bulk_append_t *bulk = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_observer_replacement(s, frame,
        &record, &bulk, control) == 0);
    int prepared = wl_columnar_relation_replacement_prepare_controlled(record,
            bulk, target, source, control);
    if (prepared) fprintf(stderr, "replacement prepare rc=%d governed=%d\n",
            prepared, governed);
    CHECK(prepared == 0 && record->phase == 3 && record->transaction.staged
        && !record->reader.owner && !record->reader.secondary_owner
        && record->transaction.writer_acquired
        && record->transaction.reserved_bytes == raw.reserved_bytes
        && record->transaction.metadata_reserved_bytes ==
        raw.metadata_reserved_bytes
        && record->transaction.dedup_reserved_bytes ==
        raw.dedup_reserved_bytes);
    col_rel_t *stage = record->transaction.staged;
    CHECK(stage->nrows == raw.staged->nrows &&
        stage->capacity == raw.staged->capacity
        && !!stage->row_scratch == !!raw.staged->row_scratch &&
        !stage->memory_governor);
    for (uint32_t c = 0; c < 2; c++)
        CHECK(stage->columns[c][0] == raw.staged->columns[c][0]
            && strcmp(stage->col_names[c], raw.staged->col_names[c]) == 0
            && strcmp(stage->schema.children[c]->format,
            raw.staged->schema.children[c]->format) == 0);
    if (governed) CHECK(record->transaction.reservation_active
            && record->transaction.metadata_reservation_active
            && atomic_load_explicit(&record->transaction.reservation.state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            && atomic_load_explicit(&record->transaction.reservation.owner_bits,
            memory_order_acquire) == (uintptr_t)target
            && record->transaction.metadata_reservation.identity
            == &record->transaction.metadata_reservation
            && atomic_load_explicit(
                &record->transaction.metadata_reservation.state,
                memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && atomic_load_explicit(
                &record->transaction.metadata_reservation.owner_bits,
                memory_order_acquire) == 0
            && atomic_load_explicit(&raw.metadata_reservation.state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
    if (fault == 3) CHECK(record->transaction.dedup_reservation_active
            && record->transaction.dedup_reservation.identity
            == &record->transaction.dedup_reservation
            && atomic_load_explicit(
                &record->transaction.dedup_reservation.state,
                memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && atomic_load_explicit(
                &record->transaction.dedup_reservation.owner_bits,
                memory_order_acquire) == 0
            && atomic_load_explicit(&raw.dedup_reservation.state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && stage->dedup_cap == raw.staged->dedup_cap
            && stage->dedup_count == raw.staged->dedup_count
            && memcmp(stage->dedup_slots, raw.staged->dedup_slots,
            (size_t)stage->dedup_cap * sizeof(*stage->dedup_slots)) == 0);
    if (fault) {
        CHECK(governed);
        wl_columnar_memory_reservation_t *original = fault == 1
            ? &record->transaction.reservation : fault == 2
            ? &record->transaction.metadata_reservation :
            &record->transaction.dedup_reservation;
        uint64_t actual_state = fault == 1
            ? WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED :
            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED;
        const wl_columnar_memory_reservation_t *identity = original->identity;
        int64_t **head = target->columns;
        uint64_t view = target->view_generation,
            storage = target->storage_generation;
        original->identity = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL
            && !s->cleanup_active && s->cleanup_pending
            && record->transaction.staged == NULL && original->identity == NULL
            && atomic_load_explicit(&original->state, memory_order_acquire)
            == actual_state
            && record->transaction.writer_acquired
            && target->columns == head && target->view_generation == view
            && target->storage_generation == storage);
        /* The original scalar is restored only after serialized refusal.
         * The real pending frame then drains physical-NULL credits/writers. */
        original->identity = identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0
            && !s->cleanup_pending
            && atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&target->descriptor_access.state,
            memory_order_acquire) == 0);
    } else CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && !s->cleanup_pending && !s->cleanup_active);
    CHECK(wl_evaluation_control_finish(control, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    col_rel_discard_replacement(&raw);
    CHECK(col_rel_destroy_checked(target) == 0 &&
        col_rel_destroy_checked(raw_target) == 0);
    wl_evaluation_control_release(control); wl_session_destroy(base);
}

static wl_columnar_eval_delta_replacement_t *candidate_disposal_record;
static wl_columnar_relation_bulk_append_t *candidate_disposal_bulk;
static eval_entry_t *candidate_disposal_owner;
static wl_columnar_memory_reservation_t *candidate_disposal_fault;
static const wl_columnar_memory_reservation_t *candidate_disposal_identity;
static unsigned candidate_disposal_visits, candidate_disposal_mode;

static void candidate_disposal_checkpoint(wl_evaluation_control_t *control,
    uint64_t work)
{
    if (!candidate_disposal_record) return;
    if (candidate_disposal_mode == 1 && work == 32
        && candidate_disposal_record->phase ==
        WL_COLUMNAR_OBSERVER_CANDIDATE_MOVE
        && !candidate_disposal_visits) {
        col_rel_t *candidate = candidate_disposal_owner->rel;
        CHECK(candidate && !candidate->columns && !candidate->col_names);
        candidate_disposal_fault = &candidate->descriptor_reservation;
        CHECK(candidate_disposal_fault->bytes
            && candidate_disposal_fault->identity == candidate_disposal_fault);
        candidate_disposal_identity = candidate_disposal_fault->identity;
        candidate_disposal_fault->identity = NULL;
        candidate_disposal_visits++;
    } else if (candidate_disposal_mode == 2
        && candidate_disposal_record->phase ==
        WL_COLUMNAR_OBSERVER_CANDIDATE_HEADER_NULL
        && work == 16 && !candidate_disposal_visits) {
        CHECK(!candidate_disposal_owner->rel && !candidate_disposal_bulk->dst
            && candidate_disposal_record->candidate_disposed
            && (candidate_disposal_record->column_cursor
            & WL_COLUMNAR_OBSERVER_CANDIDATE_REFERENCE));
        candidate_disposal_fault = &candidate_disposal_bulk->scratch;
        CHECK(candidate_disposal_fault->bytes
            && candidate_disposal_fault->identity == candidate_disposal_fault);
        candidate_disposal_identity = candidate_disposal_fault->identity;
        candidate_disposal_fault->identity = NULL;
        candidate_disposal_visits++;
    } else if (candidate_disposal_mode == 3 && work == 8
        && candidate_disposal_record->phase ==
        WL_COLUMNAR_OBSERVER_CANDIDATE_PHYSICAL){
        candidate_disposal_visits++;
        if (candidate_disposal_visits == 257) {
            col_rel_t *candidate = candidate_disposal_owner->rel;
            CHECK(candidate && candidate->ncols == 300 && candidate->columns
                && !candidate->columns[256] && candidate->columns[257]
                && candidate_disposal_bulk->writer.owner ==
                &candidate->source_access
                && candidate_disposal_bulk->descriptor_writer.owner
                == &candidate->descriptor_access);
            wl_evaluation_control_request_cancel(control);
        }
    }
}

/* Separate original-frame lifecycle evidence: these are real governed
 * candidates prepared before any final canonical replacement. */
static void original_candidate_disposal(unsigned fault)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(s->memory_governor);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    uint32_t width = fault == 3 ? 300 : 2;
    col_rel_t *source = NULL, *target = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("$candidate", width, 64, false,
        s->memory_governor, &source) == 0);
    CHECK(wl_columnar_relation_new_auto_governed("$target", width, 64, false,
        s->memory_governor, &target) == 0);
    int64_t *row = malloc((size_t)width * sizeof(*row)); CHECK(row);
    for (uint32_t c = 0; c < width; c++) row[c] = (int64_t)c - 21;
    CHECK(col_rel_append_row(source, row) == 0); free(row);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0);
    CHECK(wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *original_frame = frame;
    eval_entry_t *owner = wl_columnar_eval_stack_cleanup_result(frame);
    CHECK(owner); owner->rel = source; owner->owned = true;
    wl_columnar_eval_delta_replacement_t *record = NULL;
    wl_columnar_relation_bulk_append_t *bulk = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_observer_replacement(s, frame,
        &record, &bulk, control) == 0);
    CHECK(wl_columnar_relation_replacement_prepare_controlled(record, bulk,
        target, source, control) == 0 && record->phase == 3
        && record->transaction.staged && record->transaction.writer_acquired);
    int64_t **canonical = target->columns;
    uint64_t view = target->view_generation,
        storage = target->storage_generation;
    wl_columnar_memory_reservation_t *embedded =
        &source->descriptor_reservation;
    const wl_columnar_memory_reservation_t *identity = embedded->identity;
    uint64_t candidate_bytes = source->descriptor_reserved_bytes
        + source->metadata_reserved_bytes + source->retained_reserved_bytes
        + source->pool_name_reserved_bytes + source->dedup_reserved_bytes;
    uint64_t before_disposal = wl_columnar_memory_reserved(g);
    candidate_disposal_record = record; candidate_disposal_bulk = bulk;
    candidate_disposal_owner = owner; candidate_disposal_visits = 0;
    candidate_disposal_mode = fault; candidate_disposal_fault = NULL;
    candidate_disposal_identity = NULL;
    wl_columnar_relation_test_work_checkpoint = candidate_disposal_checkpoint;
    int rc = wl_columnar_relation_observer_candidate_retire(record, bulk,
            owner, s->memory_governor, control, 0);
    wl_columnar_relation_test_work_checkpoint = NULL;
    candidate_disposal_record = NULL; candidate_disposal_bulk = NULL;
    candidate_disposal_owner = NULL;
    CHECK(target->columns == canonical && target->view_generation == view
        && target->storage_generation == storage &&
        record->initialized_tokens == 3
        && record->transaction.staged && record->transaction.writer_acquired);
    if (fault == 1) {
        CHECK(rc == EINVAL && candidate_disposal_visits == 1
            && owner->rel == source && owner->owned
            && record->phase == WL_COLUMNAR_OBSERVER_CANDIDATE_MOVE
            && !source->columns && !source->col_names
            && embedded->identity == NULL && embedded->bytes
            && (record->column_cursor &
            WL_COLUMNAR_OBSERVER_CANDIDATE_PREFIX_MASK) == 0);
        embedded->identity = identity;
    } else if (fault == 2) {
        CHECK(rc == EINVAL && candidate_disposal_visits == 1
            && !owner->rel && !owner->owned && !bulk->dst
            && record->phase == WL_COLUMNAR_OBSERVER_CANDIDATE_HEADER_NULL
            && candidate_disposal_fault &&
            candidate_disposal_fault->identity == NULL
            && candidate_disposal_fault->bytes);
        candidate_disposal_fault->identity = candidate_disposal_identity;
    } else if (fault == 3) {
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED &&
            candidate_disposal_visits == 257
            && owner->rel == source && owner->owned
            && record->phase == WL_COLUMNAR_OBSERVER_CANDIDATE_PHYSICAL
            && !source->columns[256] && source->columns[257]);
    } else CHECK(rc == 0 && !owner->rel && !owner->owned
            && record->phase == WL_COLUMNAR_OBSERVER_CANDIDATE_DONE);
    if (fault) CHECK(wl_columnar_relation_observer_candidate_retire(record,
            bulk, owner, s->memory_governor, NULL, 0) == 0);
    CHECK(!owner->rel && !owner->owned && !bulk->dst
        && record->candidate_disposed
        && record->phase == WL_COLUMNAR_OBSERVER_CANDIDATE_DONE
        && !(record->column_cursor & WL_COLUMNAR_OBSERVER_CANDIDATE_REFERENCE));
    CHECK(wl_columnar_memory_reserved(g) == before_disposal - candidate_bytes);
    CHECK(wl_columnar_relation_observer_candidate_retire(record, bulk, owner,
        s->memory_governor, NULL, 0) == 0
        && wl_columnar_memory_reserved(g) == before_disposal - candidate_bytes);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
        && !s->cleanup_active && !s->cleanup_pending);
    CHECK(wl_evaluation_control_finish(control, s->base.operation_admission, rc,
        0) == rc);
    if (fault == 3) {
        /* The consumed candidate was truly disposed. A fresh real source is
         * supplied between completed host attempts in this same context. */
        col_rel_t *retry = NULL;
        CHECK(wl_columnar_relation_new_auto_governed("$retry_candidate", width,
            64,
            false, s->memory_governor, &retry) == 0);
        row = malloc((size_t)width * sizeof(*row)); CHECK(row);
        for (uint32_t c = 0; c < width; c++) row[c] = (int64_t)c - 21;
        CHECK(col_rel_append_row(retry, row) == 0); free(row);
        CHECK(wl_evaluation_control_reset(control) == 0);
        CHECK(wl_evaluation_control_begin(control,
            s->base.operation_admission) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0
            && frame == original_frame);
        owner = wl_columnar_eval_stack_cleanup_result(frame);
        CHECK(owner && !owner->rel && !owner->owned);
        owner->rel = retry; owner->owned = true;
        record = NULL; bulk = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_observer_replacement(s, frame,
            &record, &bulk, control) == 0);
        CHECK(wl_columnar_relation_replacement_prepare_controlled(record, bulk,
            target, retry, control) == 0 && record->transaction.staged);
        for (uint32_t c = 0; c < width; c++)
            CHECK(record->transaction.staged->columns[c][0] == (int64_t)c - 21);
        CHECK(wl_columnar_relation_observer_candidate_retire(record, bulk,
            owner,
            s->memory_governor, control, 0) == 0 && !owner->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && !s->cleanup_active && !s->cleanup_pending
            && target->columns == canonical && target->view_generation == view
            && target->storage_generation == storage);
        CHECK(wl_evaluation_control_finish(control, s->base.operation_admission,
            0, 0) == 0);
    }
    CHECK(wl_evaluation_control_detach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    CHECK(col_rel_destroy_checked(target) == 0);
    CHECK(wl_columnar_memory_reserved(g) == baseline);
    wl_evaluation_control_release(control); wl_session_destroy(base);
}

static unsigned helper_events;
static void helper_delta(const char *name, const int64_t *row, uint32_t width,
    int32_t diff, void *data)
{
    (void)name; (void)row; (void)width; (void)diff; (void)data;
    helper_events++;
}

/* Genuine raw generated head; direct observer helper lifecycle only. The
 * controlled backend STEP integration is a separate acceptance obligation. */
static void original_nonempty_stage(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(k:int64,v:int64)\n.decl H(v:int64)\nH(v):-A(k,v).\n", &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program, &plan) == 0 && plan);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    int64_t input[] = {1, -7, 2, 9};
    CHECK(wl_backend_columnar()->session_insert(base, "A", input, 2, 2) == 0);
    CHECK(wl_backend_columnar()->session_step(base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *head = session_find_rel(s, "H");
    CHECK(s->diff_enabled && head && head->nrows == 2
        && head->storage_owner == head);
    s->delta_cb = helper_delta;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_delta_observer_begin_controlled(s, UINT64_MAX,
        c) == 0);
    wl_columnar_eval_delta_observer_set_evaluated(s);
    CHECK(wl_columnar_eval_delta_observer_prepare_controlled(s, c) == 0);
    CHECK(wl_columnar_eval_delta_observer_publish_controlled(s, c) == 0
        && helper_events == 0 && !s->delta_publish_active);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_delta_observer_discard(s) == 0
        && !s->cleanup_active && !s->cleanup_pending);
    /* Actual original snapshot and DETACH lifecycle; this remains a direct
     * helper proof until the observed STEP caller is integrated. */
    int64_t saved_rows[2] = {head->columns[0][0], head->columns[0][1]};
    CHECK(plan->stratum_count == 1 && plan->strata[0].relation_count == 1);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_delta_capture_controlled(&plan->strata[0], s, c) == 0
        && wl_columnar_eval_delta_rollback_active(s));
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    CHECK(bulk && wl_columnar_relation_delta_detach_controlled(bulk, s,
        s->delta_rollback, 0, head, c) == 0);
    CHECK(bulk->published && !bulk->active && head->storage_owner == head
        && head->columns == NULL && head->capacity == 0 && head->nrows == 0);
    CHECK(wl_columnar_session_retire_source_lease_controlled(s, frame, head,
        c) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    wl_evaluation_control_request_cancel(c);
    int stopped = wl_evaluation_control_charge(c, 0);
    CHECK(stopped < 0 && wl_columnar_eval_delta_rollback_finish(s,
        stopped) == stopped
        && !wl_columnar_eval_delta_rollback_active(s));
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, stopped,
        stopped)
        == stopped);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    uint64_t detached_view = head->view_generation,
        detached_storage = head->storage_generation;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
    bulk = wl_columnar_eval_stack_cleanup_bulk_append(frame);
    CHECK(bulk && wl_columnar_relation_delta_recovery_detach_controlled(bulk, s,
        s->delta_rollback, 0, head, c) == 0 && !bulk->published
        && head->columns == NULL && head->view_generation == detached_view
        && head->storage_generation == detached_storage);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
    bulk = wl_columnar_eval_stack_cleanup_bulk_append(frame);
    CHECK(bulk && wl_columnar_relation_restore_flat_controlled(bulk, head, s,
        s->delta_rollback, 0, head->relation_identity, true, c) == 0);
    CHECK(bulk->published && !bulk->active && head->nrows == 2
        && head->capacity == 2 && head->columns[0][0] == saved_rows[0]
        && head->columns[0][1] == saved_rows[1]
        && head->view_generation == detached_view + 1
        && head->storage_generation == detached_storage + 1);
    CHECK(wl_columnar_eval_delta_rollback_lease_controlled(s, frame, 0, head,
        c) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_delta_rollback_discard(s) == 0
        && !s->delta_rollback && !s->cleanup_pending && !s->cleanup_active);
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    wl_evaluation_control_release(c); wl_session_destroy(base);
    wl_plan_free(plan); wirelog_program_free(program);
}

/* Supplied original-owner recovery proof. The partial row is produced by the
 * real controlled bulk appender while the captured empty snapshot is active. */
static void original_empty_recovery(bool populated, bool caller)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(v:int64)\n.decl H(v:int64)\nH(v):-A(v).\n", &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program,
        &plan) == 0 && plan->stratum_count == 1);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    CHECK(wl_backend_columnar()->session_step(base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *head = session_find_rel(s, "H"), *partial = NULL;
    CHECK(head && !head->nrows && !head->columns &&
        head->storage_owner == head);
    partial = col_rel_new_auto("$partial", 1);
    CHECK(partial);
    int64_t row = 17;
    CHECK(col_rel_append_row(partial, &row) == 0);
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_delta_capture_controlled(&plan->strata[0], s,
        c) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    CHECK(bulk && wl_columnar_relation_delta_detach_controlled(bulk, s,
        s->delta_rollback, 0, head, c) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    if (populated) {
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
        bulk = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(bulk && wl_columnar_relation_bulk_append_controlled(bulk, head,
            partial, c) == 0 && head->nrows == 1 && head->columns[0][0] == row);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    }
    wl_evaluation_control_request_cancel(c);
    int stopped = wl_evaluation_control_charge(c, 0);
    CHECK(stopped < 0 && wl_columnar_eval_delta_rollback_finish(s,
        stopped) == stopped);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, stopped,
        stopped)
        == stopped);
    if (caller) {
        /* Real host cleanup runs before BEGIN. Neither an inactive attached
         * handle nor detachment permits unmetered canonical restoration. */
        wl_columnar_eval_delta_rollback_t *original = s->delta_rollback;
        int64_t **columns = head->columns;
        uint64_t view = head->view_generation,
            storage = head->storage_generation;
        CHECK(wl_columnar_session_cleanup_ready_quiescent(s) == EBUSY
            && s->delta_rollback == original && head->columns == columns
            && head->view_generation == view &&
            head->storage_generation == storage);
        CHECK(wl_evaluation_control_detach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = NULL;
        CHECK(wl_columnar_session_cleanup_ready_quiescent(s) == EBUSY
            && s->delta_rollback == original && head->columns == columns
            && head->view_generation == view &&
            head->storage_generation == storage);
        CHECK(wl_evaluation_control_attach(c,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = c;
    }
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    uint64_t before_view = head->view_generation,
        before_storage = head->storage_generation;
    if (caller) {
        CHECK(wl_columnar_eval_delta_rollback_retry(s) == 0
            && !s->delta_rollback && !s->cleanup_active && !s->cleanup_pending);
    } else {
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, c) == 0);
        bulk = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        CHECK(bulk &&
            wl_columnar_relation_delta_recovery_detach_controlled(bulk, s,
            s->delta_rollback, 0, head,
            c) == 0 && bulk->published == populated);
        CHECK(!head->columns && !head->nrows && !head->capacity
            && head->view_generation == before_view + populated
            && head->storage_generation == before_storage + populated);
        CHECK(wl_columnar_eval_delta_rollback_empty_controlled(s, frame, 0,
            head, c) == 0);
        CHECK(wl_columnar_eval_delta_rollback_lease_controlled(s, frame, 0,
            head, c) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    }
    CHECK(!head->columns && !head->nrows && !head->capacity
        && head->view_generation == before_view + populated
        && head->storage_generation == before_storage + populated);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_delta_rollback_discard(s) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    CHECK(col_rel_destroy_checked(partial) == 0);
    wl_evaluation_control_release(c); wl_session_destroy(base);
    wl_plan_free(plan); wirelog_program_free(program);
}

static unsigned capture_stop_seen;
static void capture_gather_stop(wl_evaluation_control_t *control,
    const int64_t *rows, uint32_t width, uint64_t completed)
{
    if (completed != 257) return;
    CHECK(width == 1 && rows && rows[256] == 256 && !capture_stop_seen++);
    wl_evaluation_control_request_cancel(control);
}

/* A stopped incomplete snapshot is disposed; the next attempt constructs a
* fresh image from unchanged canonical content. This is helper evidence. */
static void original_capture_stop(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(v:int64)\n.decl H(v:int64)\nH(v):-A(v).\n", &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(program,
        &plan) == 0 && plan->stratum_count == 1);
    wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    int64_t input[300];
    for (uint32_t i = 0; i < 300; i++) input[i] = i;
    CHECK(wl_backend_columnar()->session_insert(base, "A", input, 300, 1) == 0);
    CHECK(wl_backend_columnar()->session_step(base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *head = session_find_rel(s, "H");
    CHECK(head && head->nrows == 300 && head->storage_owner == head);
    uint64_t identity = head->relation_identity, view = head->view_generation,
        storage = head->storage_generation;
    uint32_t capacity = head->capacity;
    int64_t **columns = head->columns;
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(0, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = c; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    capture_stop_seen = 0;
    wl_columnar_eval_delta_test_gather_checkpoint = capture_gather_stop;
    int stopped = wl_columnar_eval_delta_capture_controlled(&plan->strata[0], s,
            c);
    wl_columnar_eval_delta_test_gather_checkpoint = NULL;
    CHECK(stopped < 0 && capture_stop_seen == 1 && !s->delta_rollback
        && !s->cleanup_active && !s->cleanup_pending &&
        s->serial_completion.frame
        && head->relation_identity == identity && head->view_generation == view
        && head->storage_generation == storage && head->capacity == capacity
        && head->columns == columns && head->nrows == 300);
    for (uint32_t i = 0; i < 300; i++) CHECK(head->columns[0][i] == input[i]);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, stopped,
        stopped)
        == stopped);
    /* Host clears sticky cancellation only after the attempt and genuine
     * partial-image drain have finished. Original frame credit is retained. */
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, s->base.operation_admission) == 0);
    CHECK(wl_columnar_eval_delta_capture_controlled(&plan->strata[0], s,
        c) == 0);
    CHECK(wl_columnar_eval_delta_rollback_finish(s, 0) == 0);
    CHECK(wl_evaluation_control_finish(c, s->base.operation_admission, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(c, s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_delta_rollback_discard(s) == 0
        && !s->delta_rollback && !s->cleanup_pending && !s->cleanup_active);
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0);
    wl_evaluation_control_release(c); wl_session_destroy(base);
    wl_plan_free(plan); wirelog_program_free(program);
}

static unsigned descriptor_name_stops;
static void descriptor_name_stop(wl_evaluation_control_t *control,
    int phase, uint64_t completed)
{
    if (phase != WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME) return;
    CHECK(completed == sizeof("$consol") && !descriptor_name_stops++);
    wl_evaluation_control_request_cancel(control);
}

static unsigned timestamp_zero_stops;
static void timestamp_zero_stop(wl_evaluation_control_t *control,
    int phase, uint64_t completed)
{
    if (phase != WL_COLUMNAR_RELATION_CONSTRUCTOR_TIMESTAMP_ZERO ||
        completed != 17) return;
    CHECK(!timestamp_zero_stops++);
    wl_evaluation_control_request_cancel(control);
}

/* Original constructor/schema/type/compound/timestamp helper lifecycle.
 * Appending and backend STEP are separate production obligations. */
static void original_normalized_descriptor(unsigned fault)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    col_rel_t *source = col_rel_new_auto("source", 1), *raw = NULL,
        *raw_typed = NULL;
    CHECK(source && wl_columnar_relation_alloc_governed(&raw, "$consol",
        s->memory_governor) == 0);
    wirelog_column_type_t source_type = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(source, &source_type, 1) == 0);
    source->compound_arity_map = malloc(sizeof(uint32_t));
    CHECK(source->compound_arity_map);
    source->compound_arity_map[0] = 1; source->compound_arity_len = 1;
    source->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
    CHECK(col_rel_enable_timestamps(source) == 0);
    uint64_t raw_descriptor_view = raw->view_generation,
        raw_descriptor_storage = raw->storage_generation;
    CHECK(col_rel_set_schema(raw, source->ncols,
        (const char *const *)source->col_names) == 0);
    CHECK(wl_columnar_relation_new_like_governed_checked(&raw_typed,
        "$consol", source, s->memory_governor) == 0);
    uint64_t raw_types_metadata = raw_typed->metadata_reserved_bytes
        - sizeof(*source->compound_arity_map);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control; s->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        s->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_delta_replacement_t *record = NULL;
    wl_columnar_relation_bulk_append_t *bulk = NULL; eval_entry_t *entry = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0
        && wl_columnar_eval_stack_cleanup_observer_constructor(s, frame,
        &record, &bulk, &entry, control) == 0);
    CHECK(record && record->ready && !record->initialized_tokens && entry
        && !wl_columnar_eval_stack_cleanup_result(frame)
        && !wl_columnar_eval_stack_cleanup_bulk_append(frame)
        && !wl_columnar_eval_stack_cleanup_sources(frame)
        && !wl_columnar_eval_stack_cleanup_stack(frame));
    if (fault == 1) {
        descriptor_name_stops = 0;
        wl_columnar_relation_test_constructor_checkpoint = descriptor_name_stop;
    }
    int rc = wl_columnar_relation_observer_descriptor_controlled(record, bulk,
            entry, source, s->memory_governor, control);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    col_rel_t *candidate = entry->rel;
    if (!candidate || !record->reader.owner || !record->reader.secondary_owner)
        fprintf(stderr,
            "descriptor prefix rc=%d fault=%u phase=%u init=%u pointer=%d reader=%d/%d\n",
            rc, fault, record->phase, record->initialized_tokens,
            candidate != NULL,
            record->reader.owner != NULL,
            record->reader.secondary_owner != NULL);
    CHECK(candidate && !entry->owned && record->reader.owner
        && record->reader.secondary_owner && !strcmp(candidate->name,
        "$consol"));
    if (fault == 1) {
        CHECK(rc < 0 && descriptor_name_stops == 1 && record->phase == 2
            && !record->initialized_tokens && !record->published
            && bulk->scratch.identity == &bulk->scratch
            && atomic_load_explicit(&bulk->scratch.state, memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
    } else {
        CHECK(rc == 0 && record->phase == 4 && record->initialized_tokens == 5
            && record->published &&
            candidate->memory_governor == s->memory_governor
            && candidate->descriptor_reserved_bytes ==
            raw->descriptor_reserved_bytes
            && candidate->descriptor_reservation.identity ==
            &candidate->descriptor_reservation
            && candidate->descriptor_reservation.owner_bits ==
            (uintptr_t)candidate
            && atomic_load_explicit(&candidate->descriptor_reservation.state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            && candidate->view_generation == raw_descriptor_view
            && candidate->storage_generation == raw_descriptor_storage
            && candidate->storage_owner == candidate && !candidate->columns
            && !candidate->schema_ok && !candidate->capacity &&
            !candidate->ncols);
        CHECK(wl_columnar_relation_observer_schema_controlled(record, bulk,
            entry, source, control) == 0 && record->phase == 5
            && candidate->ncols == source->ncols &&
            candidate->capacity == raw->capacity
            && candidate->schema_ok && candidate->columns &&
            candidate->row_scratch
            && candidate->view_generation == raw->view_generation
            && candidate->storage_generation == raw->storage_generation
            && candidate->metadata_reserved_bytes ==
            raw->metadata_reserved_bytes
            && candidate->retained_reserved_bytes ==
            raw->retained_reserved_bytes
            && !strcmp(candidate->col_names[0], raw->col_names[0])
            && !strcmp(candidate->schema.children[0]->format,
            raw->schema.children[0]->format)
            && !bulk->active && !bulk->writer.owner &&
            !bulk->descriptor_writer.owner
            && !bulk->sources.descriptor_count && !bulk->sources.owner_count
            && record->reader.owner && record->reader.secondary_owner);
        CHECK(wl_columnar_relation_observer_types_controlled(record, bulk,
            entry, source, control) == 0 && !bulk->active
            && candidate->column_types[0] == WIRELOG_TYPE_FLOAT
            && candidate->metadata_reserved_bytes == raw_types_metadata
            && !strcmp(candidate->schema.children[0]->format, "g")
            && candidate->view_generation == raw_typed->view_generation
            && record->reader.owner && record->reader.secondary_owner);
        CHECK(wl_columnar_relation_observer_compound_controlled(record, bulk,
            entry, source, control) == 0 && !bulk->active && record->phase == 5
            && candidate->compound_kind == raw_typed->compound_kind
            && candidate->compound_arity_len == 1 &&
            candidate->compound_arity_map[0] == 1
            && candidate->metadata_reserved_bytes ==
            raw_typed->metadata_reserved_bytes
            && candidate->view_generation == raw_typed->view_generation
            && candidate->storage_generation == raw->storage_generation
            && record->reader.owner && record->reader.secondary_owner);
        if (fault == 3) {
            timestamp_zero_stops = 0;
            wl_columnar_relation_test_constructor_checkpoint =
                timestamp_zero_stop;
        }
        int timestamp_rc =
            wl_columnar_relation_observer_timestamps_controlled(record,
                bulk, entry, source, control);
        wl_columnar_relation_test_constructor_checkpoint = NULL;
        if (fault == 3) {
            rc = timestamp_rc;
            CHECK(rc < 0 && timestamp_zero_stops == 1 && record->phase == 6
                && bulk->active && bulk->transaction_active &&
                bulk->pending.growth
                && bulk->prepared_timestamps && !candidate->timestamps
                && candidate->retained_reserved_bytes
                == raw_typed->retained_reserved_bytes
                - candidate->capacity * sizeof(*candidate->timestamps)
                && atomic_load_explicit(&candidate->retained_reservation.state,
                memory_order_acquire) ==
                WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
            for (unsigned n = 0; n < 17; n++)
                CHECK(!memcmp(&bulk->prepared_timestamps[n],
                    &(col_delta_timestamp_t){0},
                    sizeof(col_delta_timestamp_t)));
        } else CHECK(timestamp_rc == 0 && !bulk->active && record->phase == 5
                && candidate->timestamps &&
                candidate->timestamp_capacity == raw_typed->timestamp_capacity
                && candidate->retained_reserved_bytes ==
                raw_typed->retained_reserved_bytes
                && candidate->storage_generation ==
                raw_typed->storage_generation
                && candidate->view_generation == raw_typed->view_generation
                && !memcmp(candidate->timestamps, raw_typed->timestamps,
                candidate->capacity * sizeof(*candidate->timestamps))
                && record->reader.owner && record->reader.secondary_owner);
    }
    wl_columnar_memory_reservation_t *token = fault == 1
        ? &bulk->scratch : &candidate->descriptor_reservation;
    const wl_columnar_memory_reservation_t *identity = token->identity;
    if (fault == 1 || fault == 2) token->identity = NULL;
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (fault == 1 || fault == 2) {
        CHECK(cleanup == EINVAL && !frame && s->cleanup_pending
            && token->identity == NULL && (fault == 1 ? entry->rel == NULL
            && record->candidate_disposed : entry->rel == candidate)
            && record->reader.owner && record->reader.secondary_owner);
        /* Malformed original authority, restored only after serialized
         * return. This does not simulate healthy governor contention. */
        token->identity = identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
    } else CHECK(cleanup == 0);
    CHECK(!s->cleanup_active && !s->cleanup_pending
        && atomic_load_explicit(&source->source_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&source->descriptor_access.state,
        memory_order_acquire) == 0);
    CHECK(wl_evaluation_control_finish(control, s->base.operation_admission,
        rc, cleanup) == (rc > 0 ? rc : cleanup ? cleanup : rc));
    if (fault) {
        CHECK(wl_evaluation_control_reset(control) == 0
            && wl_evaluation_control_begin(control,
            s->base.operation_admission) == 0);
        record = NULL; bulk = NULL; entry = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0
            && wl_columnar_eval_stack_cleanup_observer_constructor(s, frame,
            &record, &bulk, &entry, control) == 0
            && wl_columnar_relation_observer_descriptor_controlled(record, bulk,
            entry, source, s->memory_governor, control) == 0
            && entry->rel && record->phase == 4 && record->published
            && wl_columnar_relation_observer_schema_controlled(record, bulk,
            entry, source, control) == 0
            && wl_columnar_relation_observer_types_controlled(record, bulk,
            entry, source, control) == 0
            && wl_columnar_relation_observer_compound_controlled(record, bulk,
            entry, source, control) == 0
            && wl_columnar_relation_observer_timestamps_controlled(record, bulk,
            entry, source, control) == 0 && entry->rel->timestamps
            && wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && !s->cleanup_pending && !s->cleanup_active
            && atomic_load_explicit(&source->source_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&source->descriptor_access.state,
            memory_order_acquire) == 0
            && wl_evaluation_control_finish(control,
            s->base.operation_admission, 0, 0) == 0);
    }
    CHECK(wl_evaluation_control_detach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0
        && col_rel_destroy_checked(raw) == 0 &&
        col_rel_destroy_checked(raw_typed) == 0
        && col_rel_destroy_checked(source) == 0);
    wl_evaluation_control_release(control); wl_session_destroy(base);
}

static wl_columnar_eval_delta_replacement_t *append_record;
static wl_columnar_relation_bulk_append_t *append_bulk;
static col_rel_t *append_candidate;
static col_rel_t *append_source;
static unsigned append_stops;
static void append_cell_stop(wl_evaluation_control_t *control, uint64_t work)
{
    if (!append_record || append_record->phase != 10 || work != 256
        || !append_bulk->prepared_columns || append_stops) return;
    CHECK(append_bulk->sources.owner_count == 1
        && append_bulk->sources.descriptor_count == 0
        && append_bulk->sources.owners[0].held
        && append_bulk->sources.owners[0].reader.owner
        && append_bulk->writer.owner == &append_candidate->source_access
        && append_bulk->pending.growth && append_bulk->transaction_active);
    append_stops++;
    wl_evaluation_control_request_cancel(control);
}

static void append_reader_stop(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!acquired || !append_source ||
        gate != &append_source->source_access) return;
    CHECK(!append_stops++ && append_record->phase == 10
        && append_bulk->sources.owner_count == 1
        && append_bulk->sources.descriptor_count == 0
        && append_bulk->sources.owners[0].held
        && append_bulk->sources.owners[0].reader.owner == gate
        && append_bulk->sources.owners[0].reader.identity
        == (uintptr_t)&append_bulk->sources.owners[0].reader
        && !append_bulk->writer.owner
        && atomic_load_explicit(&gate->state, memory_order_acquire) == 2);
    wl_evaluation_control_request_cancel(control);
}

/* Actual new-like constructor and append authorities with known/raw tuples.
 * This is helper evidence; the observed backend caller remains separate. */
static wl_columnar_eval_delta_timestamp_normalization_t *timestamp_observed;
static wl_col_session_t *timestamp_session;
static unsigned timestamp_stop_stage, timestamp_stops;
static void timestamp_column_stop(wl_evaluation_control_t *control, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record,
    col_rel_t *candidate)
{
    if ((unsigned)stage != timestamp_stop_stage || progress != 257) return;
    CHECK(!record && timestamp_observed && timestamp_session &&
        !timestamp_stops++
        && candidate == timestamp_observed->candidate && candidate->ncols == 300
        && timestamp_observed->ready && timestamp_observed->indices
        && timestamp_observed->accounting_installed
        && timestamp_observed->transaction.writer_acquired
        && timestamp_observed->transaction.writer.owner ==
        &candidate->source_access
        && timestamp_observed->transaction.writer.secondary_owner ==
        &candidate->descriptor_access
        && !timestamp_observed->normalized);
    if (stage ==
        152) CHECK(timestamp_observed->output < timestamp_observed->row);
    wl_evaluation_control_request_cancel(control);
}

static col_rel_t *sampling_source;
static wl_columnar_eval_stack_cleanup_frame_t *sampling_frame;
static uint64_t sampling_columns;
static unsigned sampling_stops;
static uintptr_t sampling_saved_identity;
static wl_columnar_source_access_reader_t *sampling_faulted_reader;
static void sampling_column_stop(wl_evaluation_control_t *control,
    uint64_t work)
{
    if (!sampling_source || work != 8 || !timestamp_observed
        || !timestamp_observed->accounting_installed) return;
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_observer_sampling_sources(
        timestamp_session, sampling_frame, true);
    if (!group || group->descriptor_count != 1 || group->owner_count != 1
        || group->descriptors[0].relation != sampling_source
        || !group->descriptors[0].reader_held || !group->owners[0].held) return;
    if (++sampling_columns != 257) return;
    CHECK(!sampling_stops++ && timestamp_observed->indices
        && timestamp_observed->transaction.writer_acquired
        && group->owners[0].reader.owner == &sampling_source->source_access);
    wl_evaluation_control_request_cancel(control);
}
static void sampling_reader_fault(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!acquired || !sampling_source ||
        gate != &sampling_source->source_access) return;
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_observer_sampling_sources(
        timestamp_session, sampling_frame, true);
    CHECK(group && !sampling_stops++ && group->descriptor_count == 1
        && group->owner_count == 1 && group->descriptors[0].reader_held);
    sampling_faulted_reader = &group->owners[0].reader;
    CHECK(sampling_faulted_reader->owner == gate
        && sampling_faulted_reader->identity ==
        (uintptr_t)sampling_faulted_reader);
    sampling_saved_identity = sampling_faulted_reader->identity;
    sampling_faulted_reader->identity = 0; /* Genuine original scalar authority fault. */
    (void)control;
}

static wl_col_session_t *normalization_session;
static wl_columnar_eval_stack_cleanup_frame_t *normalization_frame;
static col_rel_t *normalization_candidate;
static unsigned normalization_hash_complete, normalization_hash_fallback,
    normalization_sequence_prepared;
static unsigned normalization_stop_stage, normalization_stops;
static bool normalization_credit_fault;
static wl_columnar_memory_reservation_t *normalization_faulted_credit;
static const wl_columnar_memory_reservation_t *normalization_saved_identity;
static wl_columnar_consolidate_record_t *normalization_record;
static void normalization_path_observe(wl_evaluation_control_t *control,
    int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record,
    col_rel_t *candidate)
{
    if (!normalization_session || candidate != normalization_candidate) return;
    if (normalization_credit_fault && stage == 99) {
        CHECK(record && record->ready &&
            record->lane == WL_COLUMNAR_CONSOLIDATE_HASH
            && !normalization_stops++ && record->hash.admission_active
            && !record->hash.values && !record->hash.new_values
            && !record->hash.used && !record->hash.new_used
            && !record->hash.unique && !record->hash.new_unique &&
            !record->hash.row_scratch);
        normalization_faulted_credit = &record->hash.admission;
        CHECK(normalization_faulted_credit->identity ==
            normalization_faulted_credit
            && atomic_load_explicit(&normalization_faulted_credit->state,
            memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        normalization_saved_identity = normalization_faulted_credit->identity;
        normalization_faulted_credit->identity = NULL;
        normalization_record = record;
        return;
    }
    if (stage == 30) normalization_record = record;
    if (normalization_stop_stage &&
        (unsigned)stage == normalization_stop_stage && progress == 257){
        wl_columnar_consolidate_record_t *actual =
            record ? record : normalization_record;
        CHECK(actual && actual->ready && !normalization_stops++
            && normalization_session->cleanup_active == normalization_frame);
        if (stage == 81) CHECK(actual->lane == WL_COLUMNAR_CONSOLIDATE_HASH
                && actual->hash.prepared && actual->hash.relation == candidate
                && actual->hash.admission.identity == &actual->hash.admission
                && atomic_load_explicit(&actual->hash.admission.state,
                memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                && actual->hash.row_scratch && actual->hash.values &&
                actual->hash.used
                && actual->hash.lease &&
                col_rel_mutation_lease_validate(actual->hash.lease,
                candidate) == 0);
        else CHECK(stage == 5 && actual->lane == WL_COLUMNAR_CONSOLIDATE_RADIX
                && actual->sequence.prepared &&
                actual->sequence.needs_sort_count == 1
                && actual->sequence.relation == candidate &&
                actual->sequence.lease
                && col_rel_mutation_lease_validate(actual->sequence.lease,
                candidate) == 0);
        normalization_record = actual;
        wl_evaluation_control_request_cancel(control);
        return;
    }
    if (stage != 85 && stage != 97 && stage != 30) return;
    CHECK(record &&
        normalization_session->cleanup_active == normalization_frame);
    if (stage == 85) {
        CHECK(progress == 4096); normalization_hash_fallback++;
    }
    if (stage == 97) {
        CHECK(progress == 7); normalization_hash_complete++;
    }
    if (stage == 30) {
        CHECK(record->sequence.prepared); normalization_sequence_prepared++;
    }
    (void)control;
}

static wl_columnar_eval_delta_replacement_t *publication_record;
static wl_columnar_relation_bulk_append_t *publication_bulk;
static unsigned publication_stop_kind, publication_stops;
static wl_columnar_memory_reservation_t *publication_faulted_credit;
static const wl_columnar_memory_reservation_t *publication_saved_identity;
static void publication_retirement_observe(wl_evaluation_control_t *control,
    uint64_t work)
{
    if (!publication_record || !publication_bulk || work != 8) return;
    if (publication_stop_kind == 5 &&
        publication_record->phase == WL_COLUMNAR_OBSERVER_IMAGE_PRIMARY
        && publication_record->column_cursor == 257) {
        CHECK(!publication_stops++ && publication_record->published &&
            publication_bulk->published
            && publication_bulk->replacement_physical.ncols == 300
            && publication_bulk->replacement_physical.columns);
        for (uint32_t col = 0; col < 257; col++)
            CHECK(!publication_bulk->replacement_physical.columns[col]);
        CHECK(publication_bulk->replacement_physical.columns[257]);
        wl_evaluation_control_request_cancel(control);
    } else if (publication_stop_kind == 6
        && publication_record->phase == WL_COLUMNAR_OBSERVER_IMAGE_FOOTER &&
        publication_bulk->count == 9){
        CHECK(!publication_stops++ && publication_record->published &&
            publication_bulk->published
            && !publication_bulk->replacement_physical.columns
            && !publication_bulk->replacement_physical.schema.release);
        publication_faulted_credit = &publication_bulk->previous;
        publication_saved_identity = publication_faulted_credit->identity;
        CHECK(publication_saved_identity == publication_faulted_credit &&
            publication_faulted_credit->bytes
            && atomic_load_explicit(&publication_faulted_credit->state,
            memory_order_acquire)
            == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        publication_faulted_credit->identity = NULL;
    }
}

static void original_normalized_append_case(uint32_t rows, unsigned stop,
    uint64_t handoff_allowance, bool handoff_prefix_fault,
    uint64_t image_allowance, bool image_identity_fault,
    uint64_t publication_case)
{
    wl_plan_t plan = {0}; wl_session_t *base = NULL;
    CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
    wl_col_session_t *s = COL_SESSION(base);
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
    uint32_t width = stop >= 9 ? 300 : 2;
    col_rel_t *source = col_rel_new_auto("$append", width), *raw = NULL;
    CHECK(source);
    col_rel_t *registered_sample = NULL; uint64_t registered_bytes = 0;
    if (stop >= 12) {
        registered_sample = col_rel_new_auto("Sample", 300);
        int64_t sample_row[300] = {0};
        CHECK(registered_sample && col_rel_append_row(registered_sample,
            sample_row) == 0
            && session_add_rel(s, registered_sample) == 0 && s->nrels == 1);
        registered_bytes = col_rel_transport_bytes(registered_sample);
        col_session_mem_sample(s); /* Genuine raw gauge reference before BEGIN. */
        baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor));
    }
    wirelog_column_type_t types[300];
    for (uint32_t col = 0; col < width; col++) types[col] = WIRELOG_TYPE_INT64;
    types[width - 1] = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(source, types, width) == 0);
    if (stop < 16) CHECK(col_rel_enable_timestamps(source) == 0);
    else {
        source->compound_arity_map = malloc(sizeof(uint32_t));
        CHECK(source->compound_arity_map);
        source->compound_arity_map[0] = width; source->compound_arity_len = 1;
        source->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        CHECK(wl_columnar_relation_compound_map_valid(source));
    }
    for (uint32_t row = 0; row < rows; row++) {
        /* FLOAT fallback uses the raw insertion sort. Ascending unique input
         * reaches its real hash heuristic fallback without quadratic shifts. */
        uint32_t key = stop == 18 ? row + 1u : stop >=
            8 ? (rows - row) % 7u : row;
        double value = stop >= 8 ? (key ? (double)key : -0.0)
            : row % 2 ? (double)row : -0.0;
        int64_t tuple[300] = {0};
        if (width == 2) tuple[0] = (int64_t)key;
        memcpy(&tuple[width - 1], &value, sizeof(value));
        CHECK(col_rel_append_row(source, tuple) == 0);
        if (source->timestamps) source->timestamps[row] =
                (col_delta_timestamp_t){
                .iteration = row, .stratum = 1, .worker = 0,
                .multiplicity = row % 2 ? -1 : 1
            };
    }
    CHECK(wl_columnar_relation_new_like_governed_checked(&raw, "$consol",
        source, s->memory_governor) == 0 && col_rel_append_all(raw, source,
        NULL) == 0);
    wl_session_t *raw_base = NULL; col_rel_t *normalized_raw = NULL;
    if (stop >= 8) {
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1,
            &raw_base) == 0);
        CHECK(wl_columnar_relation_new_like_governed_checked(&normalized_raw,
            "$raw-ts",
            source, COL_SESSION(raw_base)->memory_governor) == 0
            && col_rel_append_all(normalized_raw, source, NULL) == 0
            && wl_columnar_eval_delta_consolidate(normalized_raw,
            COL_SESSION(raw_base)) == 0
            && normalized_raw->nrows == (stop == 18 ? rows : rows ? 7 : 0));
    }
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = control; s->serial_completion.pending = true;
    wl_columnar_eval_stack_cleanup_frame_t *original = NULL;
    for (unsigned trial = 0;
        trial < ((stop && stop < 3) ||
        (stop >= 9 && stop <= 15 && stop != 12) || stop >= 19 ||
        handoff_prefix_fault ? 2u : 1u); trial++){
        if (trial) CHECK(wl_evaluation_control_reset(control) == 0
                && wl_evaluation_control_configure(control, 0) == 0);
        uint64_t suffix_allowance = handoff_prefix_fault &&
            trial ? 1648 : handoff_allowance;
        wl_columnar_source_access_writer_t sample_writer = {0};
        if (stop == 14 && !trial)
            CHECK(col_rel_source_writer_acquire(registered_sample,
                &sample_writer) == 0);
        CHECK(wl_evaluation_control_begin(control,
            s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        wl_columnar_eval_delta_replacement_t *record = NULL;
        wl_columnar_relation_bulk_append_t *bulk = NULL;
        eval_entry_t *entry = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0);
        if (!original) original = frame; else CHECK(frame == original);
        CHECK(wl_columnar_eval_stack_cleanup_observer_constructor(s, frame,
            &record, &bulk, &entry, control) == 0
            && wl_columnar_relation_observer_descriptor_controlled(record, bulk,
            entry, source, s->memory_governor, control) == 0
            && wl_columnar_relation_observer_schema_controlled(record, bulk,
            entry, source, control) == 0
            && wl_columnar_relation_observer_types_controlled(record, bulk,
            entry, source, control) == 0
            && wl_columnar_relation_observer_compound_controlled(record, bulk,
            entry, source, control) == 0
            && wl_columnar_relation_observer_timestamps_controlled(record, bulk,
            entry, source, control) == 0);
        col_rel_t *candidate = entry->rel;
        uint64_t source_view = source->view_generation,
            source_storage = source->storage_generation;
        if (stop && stop < 3 && !trial) {
            append_record = record; append_bulk = bulk;
            append_candidate = candidate;
            append_source = source;
            append_stops = 0;
            if (stop ==
                1) wl_columnar_relation_test_work_checkpoint = append_cell_stop;
            else wl_columnar_relation_test_after_reader_cas =
                    append_reader_stop;
        }
        int rc = wl_columnar_relation_observer_append_controlled(record, bulk,
                entry, source, control);
        wl_columnar_relation_test_work_checkpoint = NULL;
        wl_columnar_relation_test_after_reader_cas = NULL;
        append_record = NULL; append_bulk = NULL; append_candidate = NULL;
        append_source = NULL;
        if (stop && stop < 3 && !trial) {
            CHECK(rc < 0 && append_stops == 1 && record->phase == 10
                && !candidate->nrows
                && bulk->sources.owners[0].held
                && record->reader.owner && record->reader.secondary_owner);
            if (stop == 1) {
                CHECK(bulk->pending.growth && bulk->writer.owner);
                for (uint32_t i = 0; i < 256; i++)
                    CHECK(bulk->prepared_columns[0][i] == (int64_t)i);
            } else CHECK(!bulk->pending.growth && !bulk->writer.owner
                    && atomic_load_explicit(&source->source_access.state,
                    memory_order_acquire) == 2);
        } else {
            if (rc) fprintf(stderr, "append rc=%d rows=%u phase=%u\n", rc, rows,
                    record->phase);
            CHECK(rc == 0 && record->phase == 12 && !bulk->active
                && !bulk->writer.owner && !bulk->sources.owner_count
                && !record->reader.owner && !record->reader.secondary_owner
                && candidate->nrows == rows &&
                candidate->capacity == raw->capacity
                && candidate->timestamp_capacity == raw->timestamp_capacity
                && candidate->retained_reserved_bytes ==
                raw->retained_reserved_bytes
                && candidate->view_generation == raw->view_generation
                && candidate->storage_generation == raw->storage_generation);
            for (uint32_t c = 0; c < width; c++) for (uint32_t i = 0; i < rows;
                    i++)
                    CHECK(candidate->columns[c][i] == raw->columns[c][i]
                        && (c ||
                        candidate->columns[c][i] == source->columns[c][i]));
            if (source->timestamps) CHECK(!memcmp(candidate->timestamps,
                    raw->timestamps,
                    rows * sizeof(*candidate->timestamps)));
        }
        CHECK(source->view_generation == source_view &&
            source->storage_generation == source_storage);
        bool attempt_finished = false;
        wl_columnar_memory_reservation_t *handoff_faulted_token = NULL;
        const wl_columnar_memory_reservation_t *handoff_saved_identity = NULL;
        wl_columnar_memory_reservation_t *image_faulted_token = NULL;
        const wl_columnar_memory_reservation_t *image_saved_identity = NULL;
        if (!(stop && stop < 3 && !trial)) {
            uint64_t expected_count =
                sizeof(wl_columnar_merge_consolidate_original_record_t);
            uint64_t allowance = 0, expected_work = 0;
            if (stop >= 3 && stop <= 7) {
                /* Source-expanded768 activation plus128/128/128/128/40
                 * clear. Real setup ends BEFORE this separate host attempt;
                 * these are helper phase totals, never backend totals. */
                const uint64_t limits[] = {896, 1024, 1280, 1320, 1319};
                const uint64_t counts[] = {128, 256, 512, 552, 512};
                const uint64_t consumed[] = {896, 1024, 1280, 1320, 1280};
                CHECK(stop < 8 && wl_evaluation_control_finish(control,
                    s->base.operation_admission, 0, 0) == 0);
                allowance = limits[stop - 3]; expected_count = counts[stop - 3];
                expected_work = consumed[stop - 3];
                CHECK(wl_evaluation_control_configure(control, allowance) == 0
                    && wl_evaluation_control_begin(control,
                    s->base.operation_admission) == 0);
            }
            rc = wl_columnar_eval_stack_cleanup_observer_completed(s, frame,
                    control);
            int expected = stop >= 3 && stop <= 7 &&
                stop != 6 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
            CHECK(rc == expected
                && bulk->borrowed_mode ==
                WL_COLUMNAR_OBSERVER_COMPLETED_CONSTRUCTOR
                && bulk->exact_phase == WL_COLUMNAR_OBSERVER_COMPLETED_CLEAR
                && bulk->count == expected_count
                && bulk->dst == candidate && entry->owned
                && !s->serial_completion.payload_ready
                && !wl_columnar_eval_stack_cleanup_result(frame)
                && !wl_columnar_eval_stack_cleanup_bulk_append(frame));
            if (stop >= 16) {
                normalization_session = s; normalization_frame = frame;
                normalization_candidate = candidate;
                normalization_hash_complete = 0;
                normalization_hash_fallback = 0;
                normalization_sequence_prepared = 0;
                normalization_stops = 0; normalization_record = NULL;
                normalization_stop_stage = !trial &&
                    (stop == 19 || stop == 20) ? (stop == 19 ? 81 : 5) : 0;
                normalization_credit_fault = stop == 21 && !trial;
                normalization_faulted_credit = NULL;
                normalization_saved_identity = NULL;
                wl_columnar_consolidate_test_checkpoint =
                    normalization_path_observe;
                rc = wl_columnar_merge_observer_normalize_controlled(s, frame,
                        control);
                wl_columnar_consolidate_test_checkpoint = NULL;
                normalization_session = NULL; normalization_frame = NULL;
                normalization_candidate = NULL;
                if (stop >= 19 && !trial) {
                    CHECK(rc ==
                        (stop ==
                        21 ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED) &&
                        normalization_stops == 1
                        && normalization_record && normalization_record->ready
                        && bulk->exact_phase ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_CONS_ACTIVE
                        && candidate->nrows == (stop == 21 ? 7u : rows)
                        && s->cleanup_active == original
                        && !s->cleanup_pending && entry->owned &&
                        entry->rel == candidate
                        && atomic_load_explicit(&candidate->source_access.state,
                        memory_order_acquire)
                        == WL_COLUMNAR_SOURCE_ACCESS_WRITER);
                } else {
                    CHECK(rc == 0 && s->cleanup_active == original &&
                        !s->cleanup_pending
                        && bulk->borrowed_mode ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_CONSTRUCTOR
                        && bulk->exact_phase ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_NORMALIZED
                        && !s->serial_completion.payload_ready
                        && !wl_columnar_eval_stack_cleanup_result(frame)
                        && candidate->nrows == normalized_raw->nrows
                        && candidate->sorted_nrows == candidate->nrows &&
                        candidate->run_count == 1
                        && candidate->run_ends[0] == candidate->nrows &&
                        !candidate->timestamps
                        && candidate->compound_kind ==
                        WIRELOG_COMPOUND_KIND_SIDE
                        && candidate->compound_arity_len == 1 &&
                        candidate->compound_arity_map[0] == width);
                    CHECK(normalization_hash_complete == (stop == 17 ||
                        stop == 19 || stop == 21)
                        && normalization_hash_fallback == (stop == 18)
                        && normalization_sequence_prepared == (rows &&
                        stop != 17 && stop != 19 && stop != 21));
                    for (uint32_t row = 0; row < candidate->nrows; row++) {
                        double key = (double)(stop == 18 ? row + 1u : row);
                        int64_t bits;
                        memcpy(&bits, &key, sizeof(bits));
                        CHECK(candidate->columns[width - 1][row] == bits);
                        for (uint32_t col = 0; col < width; col++)
                            CHECK(candidate->columns[col][row] ==
                                normalized_raw->columns[col][row]);
                    }
                }
                normalization_stop_stage = 0;
                normalization_credit_fault = false;
            }
            if (stop >= 8 && stop <= 15) {
                uint64_t view = candidate->view_generation,
                    storage = candidate->storage_generation;
                uint64_t accounting = s->cleanup_reserved_bytes;
                wl_columnar_eval_delta_timestamp_normalization_t *timestamp =
                    NULL;
                CHECK(wl_columnar_eval_stack_cleanup_observer_timestamp(s,
                    frame,
                    &timestamp, control) == 0);
                if (stop >= 9 && stop <= 11 && !trial) {
                    timestamp_observed = timestamp; timestamp_session = s;
                    timestamp_stop_stage = 150u + stop - 9u;
                    timestamp_stops = 0;
                    wl_columnar_consolidate_test_checkpoint =
                        timestamp_column_stop;
                }
                if ((stop == 13 || stop == 15) && !trial) {
                    timestamp_observed = timestamp; timestamp_session = s;
                    sampling_source = registered_sample; sampling_frame = frame;
                    sampling_columns = 0; sampling_stops = 0;
                    sampling_faulted_reader = NULL; sampling_saved_identity = 0;
                    if (stop ==
                        13) wl_columnar_relation_test_work_checkpoint =
                            sampling_column_stop;
                    else wl_columnar_relation_test_after_reader_cas =
                            sampling_reader_fault;
                }
                rc = wl_columnar_eval_delta_timestamp_normalize_controlled(s,
                        frame,
                        timestamp, control);
                wl_columnar_relation_test_work_checkpoint = NULL;
                wl_columnar_relation_test_after_reader_cas = NULL;
                sampling_source = NULL; sampling_frame = NULL;
                wl_columnar_consolidate_test_checkpoint = NULL;
                timestamp_observed = NULL; timestamp_session = NULL;
                if (stop >= 9 && stop <= 11 && !trial) {
                    CHECK(rc < 0 && timestamp_stops == 1 && timestamp->ready
                        && !timestamp->normalized && timestamp->indices
                        && timestamp->accounting_installed &&
                        timestamp->transaction.writer_acquired
                        && s->cleanup_active == original && !s->cleanup_pending
                        && bulk->exact_phase ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_TS_ACTIVE
                        && s->cleanup_reserved_bytes ==
                        accounting + rows * sizeof(uint32_t));
                } else if (stop >= 13 && !trial) {
                    CHECK(rc == (stop == 13 ? WL_EVALUATION_CONTROL_CANCELLED
                        : stop == 14 ? EBUSY : EINVAL)
                        && timestamp->ready && timestamp->indices &&
                        !timestamp->normalized
                        && timestamp->accounting_installed &&
                        timestamp->transaction.writer_acquired
                        && s->cleanup_reserved_bytes ==
                        accounting + rows * sizeof(uint32_t)
                        && s->cleanup_active == original &&
                        !s->cleanup_pending);
                    wl_columnar_relation_source_group_t *group =
                        wl_columnar_eval_stack_cleanup_observer_sampling_sources
                            (s, frame, false);
                    CHECK(group);
                    wl_mem_ledger_snapshot_t gauges;
                    wl_mem_ledger_snapshot(&s->mem_ledger, &gauges);
                    CHECK(gauges.subsys_bytes[WL_MEM_SUBSYS_STORED] ==
                        registered_bytes
                        && gauges.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY] == 0);
                    if (stop == 13) CHECK(sampling_stops == 1 &&
                            sampling_columns == 257
                            && !group->owner_count && !group->descriptor_count);
                    else if (stop == 14) CHECK(!group->owner_count &&
                            !group->descriptor_count
                            && sample_writer.owner ==
                            &registered_sample->source_access
                            && atomic_load_explicit(
                                &registered_sample->source_access.state,
                                memory_order_acquire) ==
                            WL_COLUMNAR_SOURCE_ACCESS_WRITER);
                    else {
                        CHECK(sampling_stops == 1 && sampling_faulted_reader
                            && group->owner_count == 1 && group->owners[0].held
                            && sampling_faulted_reader ==
                            &group->owners[0].reader
                            && !sampling_faulted_reader->identity
                            && wl_columnar_relation_source_group_drain(group,
                            0) == EINVAL
                            && timestamp->indices &&
                            timestamp->transaction.writer_acquired);
                        sampling_faulted_reader->identity =
                            sampling_saved_identity;
                        CHECK(wl_columnar_relation_source_group_drain(group,
                            0) == 0);
                        sampling_faulted_reader = NULL;
                    }
                } else {
                    CHECK(rc == 0 && timestamp->normalized &&
                        !timestamp->ready && !timestamp->indices
                        && !timestamp->accounting_installed &&
                        !timestamp->transaction.writer_acquired
                        && s->cleanup_active == original && !s->cleanup_pending
                        && bulk->borrowed_mode ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_CONSTRUCTOR
                        && bulk->exact_phase ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_NORMALIZED
                        && bulk->count ==
                        sizeof(wl_columnar_merge_consolidate_original_record_t)
                        && timestamp->indices_reservation.identity ==
                        &timestamp->indices_reservation
                        && timestamp->indices_reservation.bytes ==
                        rows * sizeof(uint32_t)
                        && atomic_load_explicit(
                            &timestamp->indices_reservation.state,
                            memory_order_acquire)
                        == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
                        && s->cleanup_reserved_bytes == accounting
                        && candidate->nrows == (rows ? 7u : 0u) &&
                        candidate->sorted_nrows == (rows ? 7u : 0u)
                        && candidate->run_count == 1 &&
                        candidate->run_ends[0] == 7
                        && candidate->view_generation == view &&
                        candidate->storage_generation == storage);
                    if (stop >= 12) {
                        wl_mem_ledger_snapshot_t gauges;
                        wl_mem_ledger_snapshot(&s->mem_ledger, &gauges);
                        CHECK(gauges.subsys_bytes[WL_MEM_SUBSYS_STORED] ==
                            registered_bytes
                            && gauges.subsys_bytes[WL_MEM_SUBSYS_TEMPORARY]
                            == accounting + rows * sizeof(uint32_t));
                    }
                    for (uint32_t row = 0; row < 7; row++) {
                        uint32_t original_row = 0;
                        while ((rows - original_row) % 7u !=
                            row) original_row++;
                        CHECK(candidate->columns[0][row] ==
                            (width == 2 ? (int64_t)row : 0)
                            && !memcmp(&candidate->timestamps[row],
                            &source->timestamps[original_row],
                            sizeof(*candidate->timestamps))
                            && !memcmp(&candidate->timestamps[row],
                            &normalized_raw->timestamps[row],
                            sizeof(*candidate->timestamps)));
                        for (uint32_t col = 0; col < width; col++)
                            CHECK(candidate->columns[col][row] ==
                                normalized_raw->columns[col][row]);
                    }
                }
            }
            if (rc == 0 &&
                bulk->exact_phase ==
                WL_COLUMNAR_OBSERVER_COMPLETED_NORMALIZED){
                wl_columnar_eval_delta_replacement_t *replacement = NULL;
                wl_columnar_relation_bulk_append_t *replacement_bulk = NULL;
                int64_t **columns = candidate->columns;
                col_delta_timestamp_t *timestamps = candidate->timestamps;
                uint64_t reserved = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(s->memory_governor));
                if (suffix_allowance) {
                    /* Genuine normalization setup finishes BEFORE this fresh
                    * helper suffix attempt. No backend total is asserted. */
                    CHECK(wl_evaluation_control_finish(control,
                        s->base.operation_admission, 0, 0) == 0
                        && wl_evaluation_control_configure(control,
                        suffix_allowance) == 0
                        && wl_evaluation_control_begin(control,
                        s->base.operation_admission) == 0);
                }
                rc =
                    wl_columnar_eval_stack_cleanup_observer_normalized_replacement
                        (s, frame,
                        &replacement, &replacement_bulk, control);
                int expected = suffix_allowance && suffix_allowance < 1648
                    ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
                CHECK(rc == expected && s->cleanup_active == original &&
                    entry->owned
                    && entry->rel == candidate &&
                    bulk->dst == (rc ? candidate : NULL)
                    && candidate->columns == columns &&
                    candidate->timestamps == timestamps
                    && candidate->descriptor_reservation.identity ==
                    &candidate->descriptor_reservation
                    && wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            s->memory_governor)) == reserved);
                if (!rc) {
                    CHECK(replacement && replacement->ready &&
                        replacement->initialized_tokens == 3
                        && replacement_bulk == bulk && !bulk->borrowed_mode
                        && s->serial_completion.mode ==
                        WL_COLUMNAR_SERIAL_FRAME_OBSERVER_REPLACEMENT
                        && s->serial_completion.payload_ready);
                } else {
                    uint32_t count = suffix_allowance == 896 ? 128
                        : suffix_allowance == 1024 ? 256 : 336;
                    CHECK(!replacement && !replacement_bulk
                        && s->serial_completion.mode ==
                        WL_COLUMNAR_SERIAL_FRAME_OBSERVER_CONSTRUCTOR
                        && !s->serial_completion.payload_ready
                        && bulk->borrowed_mode ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_CONSTRUCTOR
                        && bulk->exact_phase ==
                        WL_COLUMNAR_OBSERVER_COMPLETED_TRANSFER_INITIALIZING
                        && bulk->count == count);
                    /* The actual constructor record address aliases replacement;
                     * inspect it only after all336 clear bytes are complete. */
                    if (count == sizeof(*record)) {
                        uint32_t prefix = suffix_allowance == 1104 ? 0
                            : suffix_allowance == 1232 ? 1
                            : suffix_allowance == 1360 ? 2 : 3;
                        CHECK(!record->ready &&
                            record->initialized_tokens == prefix);
                        wl_columnar_memory_reservation_t *tokens[3] = {
                            &record->transaction.reservation,
                            &record->transaction.metadata_reservation,
                            &record->transaction.dedup_reservation
                        };
                        for (uint32_t i = 0; i < prefix;
                            i++) CHECK(tokens[i]->identity == tokens[i]
                                && atomic_load_explicit(&tokens[i]->state,
                                memory_order_acquire)
                                == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
                        if (handoff_prefix_fault && !trial) {
                            CHECK(suffix_allowance == 1232 && prefix == 1);
                            handoff_faulted_token = tokens[0];
                            handoff_saved_identity = tokens[0]->identity;
                            /* Real initialized EMPTY-token validation fault,
                             * not a live credit-release refusal. */
                            CHECK(!tokens[0]->bytes && !tokens[0]->governor
                                && atomic_load_explicit(&tokens[0]->owner_bits,
                                memory_order_acquire) == 0);
                            tokens[0]->identity = NULL;
                        }
                    }
                }
                if (suffix_allowance) {
                    CHECK(wl_evaluation_control_finish(control,
                        s->base.operation_admission, rc, rc) == rc);
                    attempt_finished = true;
                    wl_evaluation_control_report_t report;
                    CHECK(wl_evaluation_control_report(control, &report) == 0
                        && report.limit == suffix_allowance
                        && report.consumed ==
                        (suffix_allowance == 1647 ? 1584 : suffix_allowance)
                        && report.result == rc);
                    if (!rc) {
                        /* A distinct real preparation attempt reaches the
                         * unchanged phase0 guard and reacquires candidate pair. */
                        CHECK(wl_evaluation_control_configure(control, 0) == 0
                            && wl_evaluation_control_begin(control,
                            s->base.operation_admission) == 0);
                        CHECK(
                            wl_columnar_relation_replacement_prepare_controlled(
                                replacement,
                                bulk, raw, candidate, control) == 0
                            && replacement->target == raw &&
                            replacement->phase == 3
                            && replacement->transaction.staged
                            && !replacement->transaction.staged->memory_governor
                            && replacement->transaction.writer_acquired
                            && replacement->transaction.writer.owner ==
                            &raw->source_access
                            && replacement->transaction.writer.secondary_owner
                            == &raw->descriptor_access
                            && !replacement->reader.owner &&
                            !replacement->reader.secondary_owner
                            && !bulk->dst && entry->rel == candidate &&
                            entry->owned
                            && replacement->transaction.reservation_active
                            && atomic_load_explicit(
                                &replacement->transaction.reservation.state,
                                memory_order_acquire) ==
                            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                            && replacement->transaction.
                            metadata_reservation_active
                            && atomic_load_explicit(
                                &replacement->transaction.metadata_reservation.
                                state,
                                memory_order_acquire) ==
                            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                            && atomic_load_explicit(
                                &replacement->transaction.metadata_reservation.
                                owner_bits,
                                memory_order_acquire) == 0);
                        int64_t **canonical_columns = raw->columns;
                        uint64_t canonical_view = raw->view_generation,
                            canonical_storage = raw->storage_generation;
                        CHECK(wl_columnar_relation_observer_candidate_retire(
                                replacement, bulk,
                                entry, s->memory_governor, control, 0) == 0
                            && replacement->phase ==
                            WL_COLUMNAR_OBSERVER_CANDIDATE_DONE
                            && replacement->candidate_disposed &&
                            replacement->column_cursor == 5
                            && !entry->rel && !entry->owned && !bulk->dst);
                        CHECK(bulk->published == (rows != 0));
                        bool prior_publication = bulk->published;
                        if (image_allowance || image_identity_fault) {
                            /* Actual DONE24 setup finishes; this fresh attempt
                             * measures only the production preparation suffix. */
                            CHECK(wl_evaluation_control_finish(control,
                                s->base.operation_admission, 0, 0) == 0
                                && wl_evaluation_control_configure(control,
                                image_allowance) == 0
                                && wl_evaluation_control_begin(control,
                                s->base.operation_admission) == 0);
                            CHECK(sizeof(bulk->replacement_physical) == 168);
                            if (image_identity_fault) {
                                image_faulted_token = &bulk->scratch;
                                image_saved_identity =
                                    image_faulted_token->identity;
                                CHECK(image_saved_identity ==
                                    image_faulted_token
                                    && !image_faulted_token->bytes &&
                                    !image_faulted_token->governor
                                    && atomic_load_explicit(
                                        &image_faulted_token->state,
                                        memory_order_acquire)
                                    == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
                                image_faulted_token->identity = NULL;
                            }
                        }
                        int image_rc =
                            wl_columnar_relation_replacement_image_prepare_controlled
                                (replacement,
                                bulk, entry, control);
                        if (image_identity_fault) {
                            CHECK(image_rc == EINVAL &&
                                replacement->phase ==
                                WL_COLUMNAR_OBSERVER_CANDIDATE_DONE
                                && bulk->published == prior_publication &&
                                !replacement->published);
                        } else if (image_allowance) {
                            uint64_t consumed = image_allowance <= 487 ? 456
                                : image_allowance <=
                                615 ? 488 : image_allowance <= 655 ? 616 : 656;
                            uint32_t count = image_allowance <=
                                615 ? 0 : image_allowance <= 655 ? 128 : 168;
                            CHECK(image_rc < 0 && !replacement->published
                                && replacement->phase == (image_allowance <= 487
                                    ? WL_COLUMNAR_OBSERVER_CANDIDATE_DONE :
                                WL_COLUMNAR_OBSERVER_IMAGE_CLEAR)
                                && bulk->count == count
                                && bulk->published ==
                                (image_allowance <=
                                487 ? prior_publication : false));
                            CHECK(wl_evaluation_control_finish(control,
                                s->base.operation_admission,
                                image_rc, image_rc) == image_rc);
                            wl_evaluation_control_report_t image_report;
                            CHECK(wl_evaluation_control_report(control,
                                &image_report) == 0
                                && image_report.limit == image_allowance &&
                                image_report.consumed == consumed
                                && image_report.result == image_rc);
                        } else CHECK(image_rc == 0
                                && replacement->phase ==
                                WL_COLUMNAR_OBSERVER_IMAGE_PREPARE
                                && bulk->count ==
                                sizeof(bulk->replacement_physical)
                                && !replacement->published && !bulk->published
                                && bulk->replacement_physical.columns ==
                                canonical_columns
                                && bulk->replacement_physical.ncols ==
                                raw->ncols
                                && !bulk->replacement_physical.schema.release
                                && replacement->transaction.staged
                                && replacement->transaction.writer_acquired
                                && raw->columns == canonical_columns &&
                                raw->nrows == rows
                                && raw->view_generation == canonical_view
                                && raw->storage_generation ==
                                canonical_storage);
                        CHECK(raw->columns == canonical_columns &&
                            raw->nrows == rows
                            && raw->view_generation == canonical_view &&
                            raw->storage_generation == canonical_storage);
                        if (publication_case) {
                            CHECK(!image_allowance && !image_identity_fault &&
                                image_rc == 0
                                && normalized_raw && rows == 14
                                && sizeof(struct ArrowSchema) == 72 &&
                                sizeof(raw->run_ends) == 32);
                            unsigned actual_commits = 0;
                            wl_columnar_memory_reservation_t *fresh[2] = {
                                &replacement->transaction.metadata_reservation,
                                &replacement->transaction.dedup_reservation
                            };
                            bool fresh_active[2] =
                            {replacement->transaction.
                             metadata_reservation_active,
                             replacement->transaction.
                             dedup_reservation_active};
                            for (unsigned i = 0; i < 2;
                                i++) if (fresh_active[i]){
                                    CHECK(fresh[i]->identity == fresh[i] &&
                                        fresh[i]->bytes
                                        && atomic_load_explicit(
                                            &fresh[i]->state,
                                            memory_order_acquire)
                                        ==
                                        WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                                        && atomic_load_explicit(
                                            &fresh[i]->owner_bits,
                                            memory_order_acquire) == 0);
                                    actual_commits++;
                                }
                            /* Independent source bound456+856+4*m; fresh
                             * RESERVED originals alone define m, not observations. */
                            uint64_t publication_cutoff = 1312 + 4u *
                                actual_commits;
                            uint64_t publication_limit = publication_case ==
                                2 ? publication_cutoff - 1
                                : publication_case ==
                                3 ? publication_cutoff : 1311;
                            if (publication_case > 1 &&
                                publication_case < 5) CHECK(
                                    wl_evaluation_control_finish(control,
                                    s->base.operation_admission, 0, 0) == 0
                                    && wl_evaluation_control_configure(control,
                                    publication_limit) == 0
                                    && wl_evaluation_control_begin(control,
                                    s->base.operation_admission) == 0);
                            if (publication_case >= 5) {
                                publication_record = replacement;
                                publication_bulk = bulk;
                                publication_stop_kind = publication_case;
                                publication_stops = 0;
                                publication_faulted_credit = NULL;
                                publication_saved_identity = NULL;
                                wl_columnar_relation_test_work_checkpoint =
                                    publication_retirement_observe;
                            }
                            image_rc =
                                wl_columnar_relation_replacement_image_publish_controlled
                                (
                                replacement, bulk, control);
                            if (publication_case >= 5) {
                                wl_columnar_relation_test_work_checkpoint =
                                    NULL;
                                CHECK(publication_stops == 1 &&
                                    replacement->published && bulk->published
                                    && (publication_case == 5 ? image_rc <
                                    0 : image_rc == EINVAL)
                                    && replacement->phase ==
                                    (publication_case == 5
                                        ? WL_COLUMNAR_OBSERVER_IMAGE_PRIMARY :
                                    WL_COLUMNAR_OBSERVER_IMAGE_CREDITS));
                            }
                            if (publication_case > 1 && publication_case < 5) {
                                CHECK(image_rc < 0);
                                CHECK(wl_evaluation_control_finish(control,
                                    s->base.operation_admission,
                                    image_rc, image_rc) == image_rc);
                                wl_evaluation_control_report_t
                                    publication_report;
                                CHECK(wl_evaluation_control_report(control,
                                    &publication_report) == 0
                                    && publication_report.limit ==
                                    publication_limit
                                    && publication_report.consumed ==
                                    (publication_case == 3 ? publication_cutoff
                                        : publication_case == 2 &&
                                    actual_commits ? publication_cutoff -
                                    2 : 456)
                                    && publication_report.result == image_rc);
                                if (publication_case !=
                                    3) CHECK(!replacement->published &&
                                        !bulk->published
                                        && replacement->phase ==
                                        WL_COLUMNAR_OBSERVER_IMAGE_PREPARE
                                        && raw->columns == canonical_columns &&
                                        raw->nrows == rows
                                        && raw->view_generation ==
                                        canonical_view
                                        && raw->storage_generation ==
                                        canonical_storage);
                                else CHECK(replacement->published &&
                                        bulk->published
                                        && replacement->phase ==
                                        WL_COLUMNAR_OBSERVER_IMAGE_PRIMARY
                                        && !replacement->column_cursor &&
                                        !bulk->count);
                            }
                            if (publication_case == 1 ||
                                publication_case >= 3 &&
                                publication_case != 4){
                                CHECK((publication_case == 1 ? image_rc ==
                                    0 : publication_case == 6 ? image_rc ==
                                    EINVAL : image_rc < 0)
                                    && replacement->published && bulk->published
                                    && replacement->phase ==
                                    (publication_case == 1
                                    ? WL_COLUMNAR_OBSERVER_IMAGE_DONE :
                                    publication_case == 6
                                        ? WL_COLUMNAR_OBSERVER_IMAGE_CREDITS :
                                    WL_COLUMNAR_OBSERVER_IMAGE_PRIMARY)
                                    && !replacement->transaction.staged
                                    && (publication_case ==
                                    1 ?
                                    !replacement->transaction.writer_acquired
                                    : replacement->transaction.writer_acquired)
                                    && raw->nrows == normalized_raw->nrows &&
                                    raw->ncols == normalized_raw->ncols
                                    && raw->view_generation ==
                                    canonical_view + 1
                                    && raw->storage_generation ==
                                    canonical_storage + 1
                                    && raw->schema_ok ==
                                    normalized_raw->schema_ok
                                    && raw->schema.release &&
                                    (publication_case == 1 ||
                                    publication_case == 6
                                    ? !bulk->replacement_physical.schema.release
                                    : bulk->replacement_physical.schema.release
                                    != NULL));
                                for (uint32_t col = 0; col < raw->ncols;
                                    col++){
                                    CHECK(raw->column_types[col] ==
                                        normalized_raw->column_types[col]);
                                    for (uint32_t row = 0; row < raw->nrows;
                                        row++)
                                        CHECK(raw->columns[col][row] ==
                                            normalized_raw->columns[col][row]);
                                }
                                CHECK((raw->timestamps != NULL) ==
                                    (normalized_raw->timestamps != NULL));
                                if (raw->timestamps) CHECK(!memcmp(
                                            raw->timestamps,
                                            normalized_raw->timestamps,
                                            raw->nrows *
                                            sizeof(*raw->timestamps)));
                                CHECK(raw->retained_reservation.identity ==
                                    &raw->retained_reservation
                                    && atomic_load_explicit(
                                        &raw->retained_reservation.state,
                                        memory_order_acquire)
                                    == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                                    && atomic_load_explicit(
                                        &raw->metadata_reservation.state,
                                        memory_order_acquire)
                                    == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
                            }
                        }
                        if ((!image_allowance || image_identity_fault) &&
                            (publication_case <= 1 || publication_case >= 5))
                            CHECK(wl_evaluation_control_finish(control,
                                s->base.operation_admission,
                                image_rc, image_rc) == image_rc);
                    }
                }
            }
            if (stop >= 3 && stop <= 7) {
                CHECK(wl_evaluation_control_finish(control,
                    s->base.operation_admission, rc, rc) == rc);
                attempt_finished = true;
                wl_evaluation_control_report_t report;
                CHECK(wl_evaluation_control_report(control, &report) == 0
                    && report.limit == allowance &&
                    report.consumed == expected_work
                    && report.result == rc);
            }
        }
        if (image_faulted_token) {
            CHECK(frame == original && s->cleanup_active == original
                && !s->cleanup_pending &&
                image_faulted_token->identity == NULL);
            /* This EMPTY slot has no cleanup credit obligation. Restore only
             * its identity after serialized preparation/FINISH returns, while
             * the original active frame still owns the slot. */
            image_faulted_token->identity = image_saved_identity;
        }
        int finished = wl_columnar_eval_stack_cleanup_finish(&frame);
        if (publication_case == 6) {
            CHECK(finished == EINVAL && !frame && !s->cleanup_active
                && s->cleanup_pending == original && publication_faulted_credit
                && publication_faulted_credit->identity == NULL
                && publication_record->published && publication_bulk->published
                && publication_record->phase ==
                WL_COLUMNAR_OBSERVER_IMAGE_CREDITS
                && wl_columnar_eval_stack_cleanup_retry(s) == EINVAL);
            publication_faulted_credit->identity = publication_saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
        } else if (handoff_faulted_token) {
            CHECK(finished == EINVAL && !frame && !s->cleanup_active
                && s->cleanup_pending == original && bulk->dst == candidate
                && candidate->columns && handoff_faulted_token->identity == NULL
                && bulk->borrowed_mode ==
                WL_COLUMNAR_OBSERVER_COMPLETED_CONSTRUCTOR
                && bulk->exact_phase ==
                WL_COLUMNAR_OBSERVER_COMPLETED_TRANSFER_INITIALIZING
                && bulk->count == sizeof(*record) &&
                record->initialized_tokens == 1
                && atomic_load_explicit(&handoff_faulted_token->state,
                memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
                && !handoff_faulted_token->bytes &&
                !handoff_faulted_token->governor
                && atomic_load_explicit(&handoff_faulted_token->owner_bits,
                memory_order_acquire) == 0
                && wl_columnar_eval_stack_cleanup_retry(s) == EINVAL);
            /* Restore only the saved scalar AFTER the serialized refusal. */
            handoff_faulted_token->identity = handoff_saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0
                && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0);
        } else if (stop == 21 && !trial) {
            CHECK(finished == EINVAL && !frame && !s->cleanup_active
                && s->cleanup_pending == original && normalization_record
                && normalization_record->hash.admission_active
                && normalization_faulted_credit ==
                &normalization_record->hash.admission
                && normalization_faulted_credit->identity == NULL
                && atomic_load_explicit(&normalization_faulted_credit->state,
                memory_order_acquire)
                == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                && wl_columnar_eval_stack_cleanup_retry(s) == EINVAL);
            normalization_faulted_credit->identity =
                normalization_saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0
                && wl_columnar_eval_stack_cleanup_idle(s, NULL) == 0);
        } else CHECK(finished == 0);
        publication_record = NULL; publication_bulk = NULL;
        publication_faulted_credit = NULL; publication_saved_identity = NULL;
        normalization_record = NULL; normalization_faulted_credit = NULL;
        normalization_saved_identity = NULL;
        CHECK(!s->cleanup_pending && !s->cleanup_active
            && atomic_load_explicit(&source->source_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&source->descriptor_access.state,
            memory_order_acquire) == 0);
        if (sample_writer.owner || sample_writer.secondary_owner)
            CHECK(wl_columnar_source_access_writer_release(&sample_writer) ==
                0);
        if (!attempt_finished)
            CHECK(wl_evaluation_control_finish(control,
                s->base.operation_admission, rc, rc) == rc);
        if (registered_sample) CHECK(
                atomic_load_explicit(
                    &registered_sample->descriptor_access.state,
                    memory_order_acquire) == 0
                && atomic_load_explicit(&registered_sample->source_access.state,
                memory_order_acquire) == 0);
    }
    CHECK(wl_evaluation_control_detach(control,
        s->base.operation_admission) == 0);
    s->base.evaluation_control = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0
        && col_rel_destroy_checked(raw) == 0 &&
        col_rel_destroy_checked(source) == 0
        && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(s->memory_governor)) ==
        baseline);
    wl_evaluation_control_release(control); wl_session_destroy(base);
    if (normalized_raw) CHECK(col_rel_destroy_checked(normalized_raw) == 0);
    if (raw_base) wl_session_destroy(raw_base);
}

static void original_normalized_append(uint32_t rows, unsigned stop)
{
    original_normalized_append_case(rows, stop, 0, false, 0, false, false);
}

static void original_normalized_handoff_boundaries(void)
{
    /* Source P768 + clear336 + three128 inits + three32 validations +64.
     * Each suffix starts from a fresh genuine normalized candidate. */
    const uint64_t limits[] = {896, 1024, 1104, 1232, 1360, 1488, 1647, 1648,
                               1648};
    for (unsigned route = 0; route < 2; route++) {
        for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); i++)
            original_normalized_append_case(14, route ? 16 : 8, limits[i],
                false, 0, false, false);
        original_normalized_append_case(14, route ? 16 : 8, 1232, true, 0,
            false, false);
        /* Fresh genuine successful normalization after actual refused-prefix
         * cleanup, never forward resume of the erased transfer overlay. */
        original_normalized_append_case(14, route ? 16 : 8, 1648, false, 0,
            false, false);
    }
}

static wl_columnar_eval_delta_replacement_t *preflight_record;
static col_rel_t *preflight_source;
static unsigned preflight_stops;
static void preflight_type_stop(wl_evaluation_control_t *control, uint64_t work)
{
    if (work != 4 || !preflight_record ||
        preflight_record->phase != WL_COLUMNAR_OBSERVER_SOURCE_PREFLIGHT
        || preflight_record->column_cursor != 257) return;
    CHECK(!preflight_stops++ && preflight_record->target == preflight_source
        && preflight_record->reader.owner &&
        preflight_record->reader.secondary_owner);
    wl_evaluation_control_request_cancel(control);
}
static void preflight_descriptor_stop(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!acquired || !preflight_source ||
        gate != &preflight_source->descriptor_access) return;
    CHECK(preflight_record && !preflight_stops++ &&
        !preflight_record->reader.owner
        && preflight_record->reader.secondary_owner == gate);
    wl_evaluation_control_request_cancel(control);
}

static void original_source_preflight(void)
{
    for (unsigned scenario = 0; scenario < 9; scenario++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        uint64_t baseline =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    s->memory_governor));
        uint32_t width = scenario == 5 ? 300 : 2;
        col_rel_t *source = col_rel_new_auto("$preflight", width);
        CHECK(source);
        wirelog_column_type_t types[300];
        for (uint32_t col = 0; col < width;
            col++) types[col] = WIRELOG_TYPE_INT64;
        types[width - 1] = WIRELOG_TYPE_FLOAT;
        CHECK(col_rel_set_column_types(source, types, width) == 0);
        if (scenario < 2) CHECK(col_rel_enable_timestamps(source) == 0);
        uint32_t rows = scenario < 4 ? scenario % 2 : scenario == 7 ? 2 : 1;
        for (uint32_t row = 0; row < rows; row++) {
            int64_t tuple[300] = {0}; double floating = (double)row + 1;
            tuple[0] = row;
            memcpy(&tuple[width - 1], &floating, sizeof(floating));
            CHECK(col_rel_append_row(source, tuple) == 0);
        }
        if (scenario ==
            4) source->columns[width - 1][0] = INT64_C(0x7ff8000000000000);
        uint64_t identity = source->relation_identity,
            view = source->view_generation,
            storage = source->storage_generation;
        int64_t **columns = source->columns;
        uint32_t capacity = source->capacity;
        wl_evaluation_control_t *control = NULL;
        CHECK(wl_evaluation_control_create(0, &control) == 0
            && wl_evaluation_control_attach(control,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = control;
        s->serial_completion.pending = true;
        CHECK(wl_evaluation_control_begin(control,
            s->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        wl_columnar_eval_delta_replacement_t *record = NULL;
        wl_columnar_relation_bulk_append_t *bulk = NULL;
        eval_entry_t *entry = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_acquire(s, &frame, control) == 0
            && wl_columnar_eval_stack_cleanup_observer_constructor(s, frame,
            &record, &bulk, &entry, control) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *original = frame;
        preflight_record = record; preflight_source = source;
        preflight_stops = 0;
        if (scenario ==
            5) wl_columnar_relation_test_work_checkpoint = preflight_type_stop;
        if (scenario ==
            6) wl_columnar_relation_test_after_reader_cas =
                preflight_descriptor_stop;
        uint64_t observed_identity = 0, observed_view = 0, observed_storage = 0;
        bool small = false;
        int rc =
            wl_columnar_relation_observer_source_preflight_controlled(record,
                bulk, entry,
                source, &observed_identity, &observed_view, &observed_storage,
                &small, control);
        wl_columnar_relation_test_work_checkpoint = NULL;
        wl_columnar_relation_test_after_reader_cas = NULL;
        CHECK(!entry->rel && !entry->owned && !record->initialized_tokens &&
            !record->published
            && !bulk->initialized_tokens && !bulk->scratch.bytes &&
            !bulk->scratch.governor
            && !record->transaction.staged && source->columns == columns
            && source->capacity == capacity && source->nrows == rows
            && source->relation_identity == identity &&
            source->view_generation == view
            && source->storage_generation == storage);
        if (scenario == 4) CHECK(rc == EINVAL &&
                !wl_columnar_relation_float_values_valid(source)
                && record->phase == WL_COLUMNAR_OBSERVER_SOURCE_PREFLIGHT);
        else if (scenario == 5 ||
            scenario == 6) CHECK(rc < 0 && preflight_stops == 1
                && record->phase == WL_COLUMNAR_OBSERVER_SOURCE_PREFLIGHT);
        else CHECK(rc == 0 && observed_identity == identity &&
                observed_view == view
                && observed_storage == storage && small == (rows <= 1)
                && record->phase ==
                WL_COLUMNAR_OBSERVER_SOURCE_PREFLIGHT_VALID);
        if (scenario == 7) {
            CHECK(
                wl_columnar_relation_observer_descriptor_from_preflight_controlled
                    (record, bulk,
                entry, source, s->memory_governor, control) == 0
                && wl_columnar_relation_observer_schema_controlled(record, bulk,
                entry, source, control) == 0
                && wl_columnar_relation_observer_types_controlled(record, bulk,
                entry, source, control) == 0
                && wl_columnar_relation_observer_compound_controlled(record,
                bulk, entry, source, control) == 0
                && wl_columnar_relation_observer_timestamps_controlled(record,
                bulk, entry, source, control) == 0
                && wl_columnar_relation_observer_append_controlled(record, bulk,
                entry, source, control) == 0
                && entry->rel && entry->rel->nrows == rows &&
                record->phase == 12
                && !record->reader.owner && !record->reader.secondary_owner);
            for (uint32_t col = 0; col < width; col++) for (uint32_t row = 0;
                    row < rows; row++)
                    CHECK(entry->rel->columns[col][row] ==
                        source->columns[col][row]);
            CHECK(observed_identity == identity && observed_view == view &&
                observed_storage == storage);
        }
        CHECK(wl_evaluation_control_finish(control, s->base.operation_admission,
            rc, rc) == rc);
        uintptr_t saved_identity = record->reader.identity;
        if (scenario == 8) {
            CHECK(record->reader.owner && record->reader.secondary_owner);
            record->reader.identity = 0;
        }
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        if (scenario == 8) {
            CHECK(cleanup == EINVAL && !frame &&
                s->cleanup_pending == original && record->reader.identity == 0);
            record->reader.identity = saved_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(s) == 0);
        } else CHECK(cleanup == 0);
        preflight_record = NULL; preflight_source = NULL;
        CHECK(!s->cleanup_active && !s->cleanup_pending
            && atomic_load_explicit(&source->source_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&source->descriptor_access.state,
            memory_order_acquire) == 0
            && source->columns == columns && source->view_generation == view &&
            source->storage_generation == storage);
        CHECK(wl_evaluation_control_detach(control,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0 &&
            col_rel_destroy_checked(source) == 0
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                s->memory_governor)) == baseline);
        wl_evaluation_control_release(control); wl_session_destroy(base);
    }
}

extern void (*wl_columnar_eval_delta_test_consolidation_boundary)(
    wl_col_session_t *, col_rel_t *, col_rel_t *, unsigned);
static unsigned caller_boundary_count;
static unsigned caller_cancel_boundary;
static bool caller_old_dedup_absent_fresh;
static wl_columnar_eval_stack_cleanup_frame_t *caller_original_frame;
static void consolidation_caller_boundary(wl_col_session_t *s,
    col_rel_t *target,
    col_rel_t *candidate, unsigned boundary)
{
    CHECK(candidate && candidate != target && s->cleanup_active);
    if (!caller_original_frame) caller_original_frame = s->cleanup_active;
    CHECK(s->cleanup_active == caller_original_frame);
    caller_boundary_count++;
    if (caller_old_dedup_absent_fresh && boundary == 3)
        CHECK(target->dedup_slots && target->dedup_reserved_bytes > 0
            && target->dedup_reservation.identity == &target->dedup_reservation
            && !candidate->dedup_slots && !candidate->dedup_reserved_bytes);
    if (caller_cancel_boundary == 4 && boundary == 2) {
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(col_rel_source_writer_acquire(target, &writer) == 0);
        wl_columnar_relation_touch_view(target);
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    } else if (boundary == caller_cancel_boundary)
        wl_evaluation_control_request_cancel(s->base.evaluation_control);
}

/* Calls the production consolidation entry, preserving raw <=1 policy and
 * genuine constructor/normalizer/replacement transitions on both routes. */
static void original_consolidation_caller(void)
{
    for (unsigned scenario = 0; scenario < 20; scenario++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL, *raw_base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0
            && wl_session_create(wl_backend_columnar(), &plan, 1,
            &raw_base) == 0);
        wl_col_session_t *s = COL_SESSION(base);
        uint64_t baseline =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    s->memory_governor));
        col_rel_t *seed = col_rel_new_auto("$caller-seed", 2), *target = NULL,
            *raw = NULL;
        wirelog_column_type_t types[2] = {WIRELOG_TYPE_INT64,
                                          WIRELOG_TYPE_FLOAT};
        CHECK(seed && col_rel_set_column_types(seed, types, 2) == 0);
        bool timestamps = !(scenario & 1);
        if (timestamps) CHECK(col_rel_enable_timestamps(seed) == 0);
        uint32_t rows = scenario == 16 ||
            scenario == 17 ? 1 : scenario < 4 ? scenario / 2 : 14;
        for (uint32_t row = 0; row < rows; row++) {
            double value = (double)((rows - row) % 7);
            int64_t tuple[2] = {(rows - row) % 7, 0};
            memcpy(&tuple[1], &value, sizeof(value));
            CHECK(col_rel_append_row(seed, tuple) == 0);
            if (timestamps) seed->timestamps[row] =
                    (col_delta_timestamp_t){.iteration = row, .stratum = 1,
                                            .worker = 0, .multiplicity = 1};
        }
        CHECK(wl_columnar_relation_new_like_governed_checked(&target, "$caller",
            seed,
            scenario == 12 || scenario == 13 ? NULL : s->memory_governor) == 0
            && col_rel_append_all(target, seed, NULL) == 0
            && wl_columnar_relation_new_like_governed_checked(&raw,
            "$caller-raw", seed, COL_SESSION(raw_base)->memory_governor) == 0
            && col_rel_append_all(raw, seed, NULL) == 0);
        if (scenario >= 18) {
            for (uint32_t row = 0; row < rows; row++) {
                bool inserted = false;
                CHECK(wl_columnar_eval_dedup_set_insert_checked(target,
                    wl_columnar_eval_dedup_row_hash(target, row),
                    &inserted) == 0
                    && wl_columnar_eval_dedup_set_insert_checked(raw,
                    wl_columnar_eval_dedup_row_hash(raw, row), &inserted) == 0);
            }
            CHECK(target->dedup_slots && target->dedup_reserved_bytes > 0);
        }
        if (scenario == 16 || scenario == 17) {
            target->columns[1][0] = INT64_C(0x7ff8000000000000);
            raw->columns[1][0] = INT64_C(0x7ff8000000000000);
        }
        CHECK(wl_columnar_eval_delta_consolidate(raw, COL_SESSION(raw_base))
            == (scenario == 16 || scenario == 17 ? EINVAL : 0));
        uint64_t identity = target->relation_identity,
            view = target->view_generation,
            storage = target->storage_generation;
        int64_t **old_columns = target->columns;
        wl_evaluation_control_t *control = NULL;
        CHECK(wl_evaluation_control_create(0, &control) == 0
            && wl_evaluation_control_attach(control,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = control;
        s->serial_completion.pending = true;
        caller_original_frame = NULL;
        caller_old_dedup_absent_fresh = scenario >= 18;
        unsigned stop = scenario >= 16 ? 0 : scenario >= 14 ? 4 : scenario >=
            6 && scenario < 12 ? (scenario - 6) / 2 + 1 : 0;
        for (unsigned trial = 0; trial < (stop ? 2u : 1u); trial++) {
            if (trial) CHECK(wl_evaluation_control_reset(control) == 0);
            CHECK(wl_evaluation_control_begin(control,
                s->base.operation_admission) == 0);
            caller_boundary_count = 0;
            caller_cancel_boundary = trial ? 0 : stop;
            wl_columnar_eval_delta_test_consolidation_boundary =
                consolidation_caller_boundary;
            int rc = wl_columnar_eval_delta_consolidate(target, s);
            wl_columnar_eval_delta_test_consolidation_boundary = NULL;
            CHECK(wl_evaluation_control_finish(control,
                s->base.operation_admission, rc, rc) == rc);
            CHECK(!s->cleanup_active && !s->cleanup_pending
                && atomic_load_explicit(&target->source_access.state,
                memory_order_acquire) == 0
                && atomic_load_explicit(&target->descriptor_access.state,
                memory_order_acquire) == 0);
            if (stop && !trial) {
                if (stop == 4) {
                    CHECK(rc == EBUSY && caller_boundary_count == 2); view++;
                }else CHECK(rc < 0 && caller_boundary_count == stop);
                CHECK(target->nrows == rows && target->columns == old_columns
                    && target->relation_identity == identity &&
                    target->view_generation == view
                    && target->storage_generation == storage);
                continue;
            }
            if (scenario == 16 || scenario == 17) {
                CHECK(rc == EINVAL && !caller_boundary_count &&
                    target->columns == old_columns
                    && target->nrows == 1 && target->view_generation == view &&
                    target->storage_generation == storage);
                continue;
            }
            CHECK(rc == 0 && target->nrows == raw->nrows &&
                target->relation_identity == identity
                && caller_boundary_count == (rows <= 1 ? 0u : 3u));
            if (scenario >= 18) CHECK(!target->dedup_slots &&
                    !target->dedup_reserved_bytes
                    && target->dedup_reservation.identity ==
                    &target->dedup_reservation
                    && atomic_load_explicit(&target->dedup_reservation.state,
                    memory_order_acquire)
                    == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
            if (rows <= 1) CHECK(target->columns == old_columns &&
                    target->view_generation == view &&
                    target->storage_generation == storage);
            for (uint32_t col = 0; col < 2; col++) {
                CHECK(target->column_types[col] == raw->column_types[col]);
                for (uint32_t row = 0; row < raw->nrows;
                    row++) CHECK(target->columns[col][row] ==
                        raw->columns[col][row]);
            }
            if (timestamps) for (uint32_t row = 0; row < raw->nrows; row++)
                    CHECK(memcmp(&target->timestamps[row],
                        &raw->timestamps[row],
                        sizeof(col_delta_timestamp_t)) == 0);
        }
        caller_original_frame = NULL;
        CHECK(wl_evaluation_control_detach(control,
            s->base.operation_admission) == 0);
        s->base.evaluation_control = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_retained(s) == 0 &&
            col_rel_destroy_checked(target) == 0
            && col_rel_destroy_checked(seed) == 0 &&
            col_rel_destroy_checked(raw) == 0
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                s->memory_governor)) == baseline);
        wl_evaluation_control_release(control); wl_session_destroy(raw_base);
        wl_session_destroy(base);
    }
}

static void original_image_publication(void)
{
    /* Genuine constructor/normalizer/handoff/preparation on both routes;
     * original canonical replacement is compared with the raw normalizer. */
    original_normalized_append_case(14, 8, 1648, false, 0, false, true);
    original_normalized_append_case(14, 16, 1648, false, 0, false, true);
    original_normalized_append_case(14, 16, 1648, false, 0, false, 5);
    original_normalized_append_case(14, 8, 1648, false, 0, false, 6);
    original_normalized_append_case(14, 16, 1648, false, 0, false, 6);
    for (unsigned route = 0; route < 2; route++)
        for (unsigned boundary = 2; boundary <= 4; boundary++)
            original_normalized_append_case(14, route ? 16 : 8, 1648, false, 0,
                false, boundary);
}

static void original_image_preparation_boundaries(void)
{
    /* Actual main128 + full writer24 + alias16 + five32 + two64 =456;
     * activation32 then actual physical128/40 clear. */
    const uint64_t limits[] = {456, 487, 488, 615, 616, 655, 656};
    for (unsigned route = 0; route < 2; route++) {
        for (unsigned i = 0; i < sizeof(limits) / sizeof(limits[0]); i++)
            original_normalized_append_case(14, route ? 16 : 8, 1648, false,
                limits[i], false, false);
        original_normalized_append_case(14, route ? 16 : 8, 1648, false, 0,
            true, false);
    }
    /* Engine-created empty noTS helper purpose only. Raw backend <=1 skips
     * candidate/index/replacement for both TS/noTS and is proved separately. */
    original_normalized_append_case(0, 16, 1648, false, 0, false, false);
}

/* Original mode19 helper lifecycle, separate from seed/backend publication. */
static wl_columnar_eval_delta_seed_restore_t *seed_single_original;
static col_rel_t *seed_single_target;
static unsigned seed_single_stop, seed_single_reached;
static void seed_single_acquisition_stop(wl_evaluation_control_t *control,
    uint64_t work)
{
    wl_columnar_relation_mutation_single_t *single =
        &seed_single_original->single;
    if (work != 17 || seed_single_reached) return;
    if (seed_single_stop == 1 && single->set.descriptors_acquired == 1
        && !single->set.owners_acquired) {
        CHECK(single->descriptor.writer.identity ==
            (uintptr_t)&single->descriptor.writer
            && single->descriptor.writer.owner ==
            &seed_single_target->descriptor_access);
    } else if (seed_single_stop == 2 && single->set.owners_acquired == 1) {
        CHECK(single->owners[0].writer.identity ==
            (uintptr_t)&single->owners[0].writer
            && single->owners[0].writer.owner ==
            &seed_single_target->source_access);
    } else return;
    seed_single_reached++;
    wl_evaluation_control_request_cancel(control);
}
static void seed_single_release_stop(wl_evaluation_control_t *control,
    wl_columnar_source_access_writer_t *token,
    wl_columnar_source_access_gate_t *gate)
{
    if (seed_single_reached || (seed_single_stop == 3
        && gate != &seed_single_target->source_access)
        || (seed_single_stop == 4 &&
        gate != &seed_single_target->descriptor_access)) return;
    CHECK(!token->owner && !token->secondary_owner && !token->identity
        && !token->thread_valid && atomic_load_explicit(&gate->state,
        memory_order_acquire) == 0);
    seed_single_reached++;
    wl_evaluation_control_request_cancel(control);
}
static void original_seed_single(void)
{
    for (unsigned mode = 0; mode < 7; mode++) {
        wl_plan_t plan = {0}; wl_session_t *base = NULL;
        CHECK(wl_session_create(wl_backend_columnar(), &plan, 1, &base) == 0);
        wl_col_session_t *session = COL_SESSION(base);
        col_rel_t *target = col_rel_new_auto("$seed-single", 1);
        CHECK(target && target->storage_owner == target);
        wl_evaluation_control_t *control = NULL;
        CHECK(wl_evaluation_control_create(0, &control) == 0
            && wl_evaluation_control_attach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = control;
        session->serial_completion.pending = true;
        wl_columnar_eval_stack_cleanup_frame_t *original_frame = NULL;
        for (unsigned trial = 0; trial < 2; trial++) {
            if (trial) CHECK(wl_evaluation_control_reset(control) == 0);
            CHECK(wl_evaluation_control_begin(control,
                session->base.operation_admission) == 0);
            wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_acquire(session, &frame,
                control) == 0);
            if (!original_frame) original_frame = frame;
            CHECK(frame == original_frame);
            wl_columnar_eval_delta_seed_restore_t *record = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_seed(session, frame, &record,
                control) == 0
                && record && record->ready && record->session == session);
            record = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_seed_single(session, frame,
                target,
                &record, control) == 0 && record);
            seed_single_original = record; seed_single_target = target;
            seed_single_stop = trial ? 0 : mode; seed_single_reached = 0;
            if (seed_single_stop == 1 || seed_single_stop == 2)
                wl_columnar_relation_test_work_checkpoint =
                    seed_single_acquisition_stop;
            int rc = wl_columnar_relation_seed_single_acquire_controlled(target,
                    &record->single, control);
            wl_columnar_relation_test_work_checkpoint = NULL;
            if (seed_single_stop == 1 || seed_single_stop == 2) {
                CHECK(rc < 0 && seed_single_reached == 1
                    && record->single.set.descriptors_acquired == 1
                    && record->single.set.owners_acquired ==
                    (seed_single_stop == 2));
            } else {
                CHECK(rc == 0 && record->single.set.descriptors_acquired == 1
                    && record->single.set.owners_acquired == 1);
                if (seed_single_stop == 3 || seed_single_stop == 4) {
                    wl_columnar_relation_test_after_single_writer_release =
                        seed_single_release_stop;
                    rc =
                        wl_columnar_relation_seed_single_retire_controlled(
                        &record->single, 0, control);
                    wl_columnar_relation_test_after_single_writer_release =
                        NULL;
                    CHECK(rc < 0 && seed_single_reached == 1
                        && record->single.set.owners_acquired == 0
                        && record->single.set.descriptors_acquired ==
                        (seed_single_stop == 3));
                } else if (seed_single_stop == 6) {
                    CHECK(wl_columnar_relation_seed_single_retire_controlled(
                            &record->single, 0, control) == 0
                        && !record->single.set.owners_acquired
                        && !record->single.set.descriptors_acquired
                        && !record->single.set.initialization_count);
                }
            }
            uintptr_t identity = record->single.owners[0].writer.identity;
            if (seed_single_stop ==
                5) record->single.owners[0].writer.identity = 0;
            CHECK(wl_evaluation_control_finish(control,
                session->base.operation_admission, rc, 0) == rc);
            int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
            if (seed_single_stop == 5) {
                CHECK(cleanup == EINVAL && session->cleanup_pending && !frame
                    && session->serial_completion.frame == original_frame
                    && session->serial_completion.mode_frame == original_frame
                    && session->serial_completion.mode ==
                    WL_COLUMNAR_SERIAL_FRAME_OBSERVER_SEED
                    && session->serial_completion.payload_ready
                    && record->single.set.owners_acquired == 1
                    && record->single.owners[0].writer.owner ==
                    &target->source_access);
                record->single.owners[0].writer.identity = identity;
                CHECK(wl_columnar_eval_stack_cleanup_retry(session) == 0);
            } else CHECK(cleanup == 0);
            CHECK(!session->cleanup_active && !session->cleanup_pending
                && session->serial_completion.frame == original_frame
                && wl_columnar_eval_stack_cleanup_idle(session, NULL) == 0
                && atomic_load_explicit(&target->descriptor_access.state,
                memory_order_acquire) == 0
                && atomic_load_explicit(&target->source_access.state,
                memory_order_acquire) == 0);
            seed_single_original = NULL; seed_single_target = NULL;
        }
        CHECK(wl_columnar_eval_stack_cleanup_retained(session) == 0
            && wl_evaluation_control_detach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = NULL;
        wl_evaluation_control_release(control); col_rel_destroy(target);
        wl_session_destroy(base);
    }
}

/* Genuine host seed creation; gathering remains a helper phase until append
 * and the default observed STEP caller are connected. */
static unsigned seed_gather_cancelled;
static void seed_gather_stop(wl_evaluation_control_t *control,
    const int64_t *rows, uint32_t width, uint64_t completed)
{
    if (completed != 257) return;
    CHECK(width == 300 && rows && rows[256] == 256 && !seed_gather_cancelled++);
    wl_evaluation_control_request_cancel(control);
}
static wl_columnar_eval_delta_seed_restore_t *seed_gather_original;
static col_rel_t *seed_gather_source;
static unsigned seed_gather_boundary;
static void seed_gather_pin_stop(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!acquired || seed_gather_cancelled
        || gate != &seed_gather_source->descriptor_access) return;
    CHECK(seed_gather_original &&
        seed_gather_original->seed_reader.secondary_owner == gate
        && !seed_gather_original->seed_reader.owner
        && seed_gather_original->seed_reader.identity ==
        (uintptr_t)&seed_gather_original->seed_reader);
    seed_gather_cancelled++;
    wl_evaluation_control_request_cancel(control);
}
static void seed_gather_release_stop(wl_evaluation_control_t *control,
    wl_columnar_source_access_reader_t *token,
    wl_columnar_source_access_gate_t *gate)
{
    if (seed_gather_cancelled || (seed_gather_boundary == 6
        && gate != &seed_gather_source->source_access)
        || (seed_gather_boundary == 7 &&
        gate != &seed_gather_source->descriptor_access)) return;
    CHECK(token == &seed_gather_original->seed_reader && !token->owner
        && atomic_load_explicit(&gate->state, memory_order_acquire) == 0);
    if (seed_gather_boundary ==
        6) CHECK(token->secondary_owner ==
            &seed_gather_source->descriptor_access);
    else CHECK(!token->secondary_owner && !token->identity &&
            !token->thread_valid);
    seed_gather_cancelled++;
    wl_evaluation_control_request_cancel(control);
}
static wl_columnar_eval_delta_seed_restore_t *seed_append_original;
static wl_columnar_relation_bulk_append_t *seed_append_bulk;
static unsigned seed_append_cancelled;
static void seed_append_copy_stop(wl_evaluation_control_t *control, int phase,
    wl_columnar_relation_bulk_append_t *bulk)
{
    if (phase != 10 || bulk != seed_append_bulk || seed_append_cancelled
        || seed_append_original->row ||
        seed_append_original->column != 257) return;
    CHECK(bulk->dst->nrows == 1 && !bulk->published &&
        !seed_append_original->published
        && bulk->dst->columns[256][bulk->base] == 256
        && seed_append_original->single.set.owners_acquired == 1);
    seed_append_cancelled++;
    wl_evaluation_control_request_cancel(control);
}
static void wl_observer_append(char *buffer, size_t capacity, size_t *offset,
    const char *format, ...)
{
    CHECK(*offset < capacity);
    size_t remaining = capacity - *offset;
    va_list args;
    va_start(args, format);
    int written = vsnprintf(buffer + *offset, remaining, format, args);
    va_end(args);
    CHECK(written >= 0 && (size_t)written < remaining);
    *offset += (size_t)written;
}

static void original_seed_gather(void)
{
    for (unsigned scenario = 0; scenario < 9; scenario++) {
        uint32_t width = (scenario == 3 ||
            scenario == 8) ? 300 : scenario == 4 ? 0 : 1;
        uint32_t count = scenario == 4 ? 2 : 1;
        char text[32768]; size_t at = 0;
        wl_observer_append(text, sizeof(text), &at, ".decl A(");
        for (uint32_t c = 0; c < width; c++)
            wl_observer_append(text, sizeof(text), &at, "%sc%u:int64",
                c ? "," : "", c);
        wl_observer_append(text, sizeof(text), &at, ")\n.decl H(");
        for (uint32_t c = 0; c < width; c++)
            wl_observer_append(text, sizeof(text), &at, "%sc%u:int64",
                c ? "," : "", c);
        wl_observer_append(text, sizeof(text), &at, ")\nH(");
        for (uint32_t c = 0; c < width; c++)
            wl_observer_append(text, sizeof(text), &at, "%sx%u",
                c ? "," : "", c);
        wl_observer_append(text, sizeof(text), &at, "):-A(");
        for (uint32_t c = 0; c < width; c++)
            wl_observer_append(text, sizeof(text), &at, "%sx%u",
                c ? "," : "", c);
        wl_observer_append(text, sizeof(text), &at, ").\n");
        CHECK(at < sizeof(text));
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        wl_plan_t *plan = NULL; wl_session_t *base = NULL;
        CHECK(program && wl_plan_from_program(program, &plan) == 0
            && wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
            && wl_backend_columnar()->session_step(base) == 0);
        int64_t input[300]; for (uint32_t c = 0; c < width; c++) input[c] = c;
        if (scenario != 2) {
            CHECK(wl_backend_columnar()->session_insert(base, "H", input, count,
                width) == 0);
            if (scenario == 1) CHECK(wl_backend_columnar()->session_remove(base,
                    "H", input, count, width) == 0);
        }
        wl_col_session_t *session = COL_SESSION(base);
        col_rel_t *target = session_find_rel(session, "H");
        col_rel_t *seed = session_find_rel(session, "$in$H");
        CHECK(target &&
            (scenario == 2 ? !seed : seed && seed->storage_owner == seed));
        uint32_t before = target->nrows;
        uint64_t generation = target->view_generation;
        col_rel_t *raw_append = NULL;
        if (scenario == 8) CHECK(col_rel_deep_copy(target, &raw_append,
                NULL) == 0
                && col_rel_append_rows_atomic(raw_append, input, count, width,
                NULL) == 0);
        wl_evaluation_control_t *control = NULL;
        CHECK(wl_evaluation_control_create(0, &control) == 0
            && wl_evaluation_control_attach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = control;
        session->serial_completion.pending = true;
        wl_columnar_eval_stack_cleanup_frame_t *original = NULL;
        for (unsigned trial = 0; trial < (scenario >= 3 ? 2u : 1u); trial++) {
            if (trial) {
                CHECK(wl_evaluation_control_reset(control) == 0);
                if (scenario ==
                    4) CHECK(wl_evaluation_control_configure(control, 0) == 0);
            }
            CHECK(wl_evaluation_control_begin(control,
                session->base.operation_admission) == 0);
            wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_acquire(session, &frame,
                control) == 0);
            if (!original) original = frame; CHECK(frame == original);
            seed_gather_original = NULL; seed_gather_source = seed;
            seed_gather_cancelled = 0; seed_gather_boundary = scenario;
            if (!trial) {
                if (scenario ==
                    3) wl_columnar_eval_delta_test_gather_checkpoint =
                        seed_gather_stop;
                if (scenario ==
                    5) wl_columnar_relation_test_after_reader_cas =
                        seed_gather_pin_stop;
                if (scenario == 6 || scenario == 7)
                    wl_columnar_relation_test_after_single_reader_release =
                        seed_gather_release_stop;
            }
            int rc = wl_columnar_eval_delta_seed_gather_controlled(session,
                    frame,
                    target, &seed_gather_original, control);
            wl_columnar_eval_delta_test_gather_checkpoint = NULL;
            wl_columnar_relation_test_after_reader_cas = NULL;
            wl_columnar_relation_test_after_single_reader_release = NULL;
            wl_columnar_eval_delta_seed_restore_t *record =
                seed_gather_original;
            CHECK(record && record->ready && record->session == session
                && target->nrows == before &&
                target->view_generation == generation);
            if (scenario == 3 && !trial) {
                CHECK(rc < 0 && seed_gather_cancelled == 1 && record->rows
                    && record->row == 0 && record->column == 257
                    && record->seed_reader.owner &&
                    record->seed_reader.secondary_owner);
            } else if (scenario >= 5 && scenario <= 7 && !trial) {
                CHECK(rc < 0 && seed_gather_cancelled == 1 &&
                    !record->seed_reader.owner);
                if (scenario == 5) CHECK(!record->rows &&
                        record->seed_reader.secondary_owner
                        && record->phase == WL_COLUMNAR_OBSERVER_SEED_PIN);
                else {
                    CHECK(record->rows && record->row == count &&
                        !record->column);
                    CHECK((record->seed_reader.secondary_owner != NULL) ==
                        (scenario == 6));
                }
            } else {
                CHECK(rc == 0 && !record->seed_reader.owner &&
                    !record->seed_reader.secondary_owner);
                if (scenario == 1 ||
                    scenario == 2) CHECK(!record->nrows && !record->rows);
                else {
                    CHECK(record->nrows == count && record->ncols == width
                        && record->row == count && !record->column &&
                        record->rows);
                    for (uint32_t c = 0; c < width;
                        c++) CHECK(record->rows[c] == input[c]);
                }
            }
            if (scenario == 4 && !trial) {
                /* Separate completed gather setup: source dimensions and the
                 * real malloc(0) owner survive the first128 SINGLE clear.
                 * Actual initializer charge32 then128, next128 refuses. */
                CHECK(record->nrows == 2 && !record->ncols && !record->cells
                    && !record->bytes && record->row == 2 && record->rows
                    && wl_evaluation_control_finish(control,
                    session->base.operation_admission, 0, 0) == 0
                    && wl_evaluation_control_configure(control, 160) == 0
                    && wl_evaluation_control_begin(control,
                    session->base.operation_admission) == 0);
                wl_columnar_eval_delta_seed_restore_t *initialized = NULL;
                rc = wl_columnar_eval_stack_cleanup_seed_single(session, frame,
                        target,
                        &initialized, control);
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !initialized
                    && record->phase ==
                    WL_COLUMNAR_OBSERVER_SEED_SINGLE_INITIALIZING
                    && record->row == 128 && record->nrows == 2 &&
                    !record->ncols
                    && record->rows && !record->seed_reader.owner &&
                    !record->seed_reader.secondary_owner
                    && target->nrows == before &&
                    target->view_generation == generation);
            }
            if (scenario == 8) {
                wl_columnar_eval_delta_seed_restore_t *held = NULL;
                int single_init =
                    wl_columnar_eval_stack_cleanup_seed_single(session, frame,
                        target, &held, control);
                if (single_init) fprintf(stderr,
                        "wide seed SINGLE init rc=%d trial=%u phase=%u\n",
                        single_init, trial, record->phase);
                CHECK(single_init == 0 && held == record);
                int single_acquire =
                    wl_columnar_relation_seed_single_acquire_controlled(target,
                        &held->single, control);
                if (single_acquire) fprintf(stderr,
                        "wide seed SINGLE acquire rc=%d trial=%u phase=%u\n",
                        single_acquire, trial, record->phase);
                CHECK(single_acquire == 0);
                seed_append_bulk = NULL; /* New getter output, not an owner reset. */
                int bulk_begin =
                    wl_columnar_eval_stack_cleanup_seed_bulk(session, frame,
                        &seed_append_bulk, control);
                if (bulk_begin) fprintf(stderr,
                        "wide seed BULK begin rc=%d trial=%u phase=%u\n",
                        bulk_begin, trial, record->phase);
                CHECK(bulk_begin == 0);
                seed_append_original = held; seed_append_cancelled = 0;
                if (!trial) wl_columnar_relation_test_bulk_checkpoint =
                        seed_append_copy_stop;
                rc = wl_columnar_eval_stack_cleanup_seed_append(session, frame,
                        control);
                wl_columnar_relation_test_bulk_checkpoint = NULL;
                if (!trial) CHECK(rc < 0 && seed_append_cancelled == 1
                        && !held->row && held->column == 257 &&
                        target->nrows == before
                        && target->view_generation == generation &&
                        !held->published
                        && held->single.set.owners_acquired == 1);
                else {
                    CHECK(rc == 0 && held->published &&
                        target->nrows == raw_append->nrows
                        && target->view_generation == generation + 1);
                    for (uint32_t c = 0; c < width; c++)
                        CHECK(target->columns[c][1] == input[c]
                            && target->columns[c][1] ==
                            raw_append->columns[c][1]);
                }
            }
            CHECK(wl_evaluation_control_finish(control,
                session->base.operation_admission, rc, 0) == rc
                && wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && !session->cleanup_active && !session->cleanup_pending
                && session->serial_completion.frame == original
                && wl_columnar_eval_stack_cleanup_idle(session, NULL) == 0);
            if (seed) CHECK(atomic_load_explicit(&seed->descriptor_access.state,
                    memory_order_acquire) == 0
                    && atomic_load_explicit(&seed->source_access.state,
                    memory_order_acquire) == 0);
        }
        seed_gather_original = NULL; seed_gather_source = NULL;
        seed_append_original = NULL; seed_append_bulk = NULL;
        if (raw_append) CHECK(col_rel_destroy_checked(raw_append) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retained(session) == 0
            && wl_evaluation_control_detach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = NULL;
        wl_evaluation_control_release(control); wl_session_destroy(base);
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

static void original_seed_name_boundaries(void)
{
    for (size_t length = 251; length <= 252; length++) {
        char name[253], text[2048]; memset(name, 'h', length); name[length] = 0;
        CHECK(snprintf(text, sizeof(text),
            ".decl A(v:int64)\n.decl %s(v:int64)\n%s(v):-A(v).\n", name,
            name) > 0);
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(text, &error);
        wl_plan_t *plan = NULL; wl_session_t *base = NULL;
        CHECK(program && wl_plan_from_program(program, &plan) == 0
            && wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
            && wl_backend_columnar()->session_step(base) == 0);
        int64_t input = 9;
        int inserted = wl_backend_columnar()->session_insert(base, name, &input,
                1, 1);
        CHECK(inserted == (length == 251 ? 0 : EOVERFLOW));
        wl_col_session_t *session = COL_SESSION(base);
        col_rel_t *target = session_find_rel(session, name);
        CHECK(target && target->nrows == (length == 251));
        wl_evaluation_control_t *control = NULL;
        CHECK(wl_evaluation_control_create(0, &control) == 0
            && wl_evaluation_control_attach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = control;
        session->serial_completion.pending = true;
        CHECK(wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_acquire(session, &frame,
            control) == 0);
        wl_columnar_eval_delta_seed_restore_t *record = NULL;
        CHECK(wl_columnar_eval_delta_seed_gather_controlled(session, frame,
            target, &record, control) == 0 && record && record->ready
            && !record->seed_reader.owner &&
            !record->seed_reader.secondary_owner);
        if (length == 251) CHECK(record->seed && record->nrows == 1 &&
                record->ncols == 1
                && record->rows && record->rows[0] == input &&
                record->row == 1 && !record->column);
        else CHECK(!record->seed && !record->nrows && !record->rows
                && record->phase == WL_COLUMNAR_OBSERVER_SEED_LOOKUP);
        CHECK(wl_evaluation_control_finish(control,
            session->base.operation_admission, 0, 0) == 0
            && wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && !session->cleanup_active && !session->cleanup_pending
            && wl_columnar_eval_stack_cleanup_retained(session) == 0
            && wl_evaluation_control_detach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = NULL;
        wl_evaluation_control_release(control); wl_session_destroy(base);
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

/* Isolated first-try native reader CAS source formula, excluding completed
 * frame setup. R1 incorrectly charged checkpoint arguments and is preserved
 * in the HOME artifacts; corrected actual source is seed sizeof +463. */
static void original_seed_gather_exact(void)
{
    const uint64_t full = sizeof(wl_columnar_eval_delta_seed_restore_t) + 463u;
    for (unsigned trial = 0; trial < 3; trial++) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(
            ".decl A(v:int64)\n.decl H(v:int64)\nH(v):-A(v).\n", &error);
        wl_plan_t *plan = NULL; wl_session_t *base = NULL; int64_t input = 0;
        CHECK(program && wl_plan_from_program(program, &plan) == 0
            && wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
            && wl_backend_columnar()->session_step(base) == 0
            && wl_backend_columnar()->session_insert(base, "H", &input, 1,
            1) == 0);
        wl_col_session_t *session = COL_SESSION(base);
        CHECK(session->nrels == 3 && session->rels[0] && session->rels[1] &&
            session->rels[2]
            && strcmp(session->rels[0]->name,
            "A") == 0 && strcmp(session->rels[1]->name, "H") == 0
            && strcmp(session->rels[2]->name, "$in$H") == 0);
        col_rel_t *target = session->rels[1], *seed = session->rels[2];
        CHECK(target->ncols == 1 && seed->ncols == 1 && seed->nrows == 1
            && seed->storage_owner == seed && seed->columns[0][0] == input);
        wl_evaluation_control_t *control = NULL;
        CHECK(wl_evaluation_control_create(0, &control) == 0
            && wl_evaluation_control_attach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = control;
        session->serial_completion.pending = true;
        CHECK(wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_acquire(session, &frame,
            control) == 0
            && session->serial_completion.mode ==
            WL_COLUMNAR_SERIAL_FRAME_EVALUATION
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, 0, 0) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *original = frame;
        uint64_t allowance = trial == 2 ? full - 1u : full;
        CHECK(wl_evaluation_control_configure(control, allowance) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_eval_delta_seed_restore_t *record = NULL;
        int rc = wl_columnar_eval_delta_seed_gather_controlled(session, frame,
                target, &record, control);
        CHECK(record && record->rows && record->rows[0] == input &&
            record->nrows == 1
            && record->ncols == 1 && record->row == 1 && !record->column &&
            !record->seed_reader.owner);
        if (trial == 2) CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT
                && record->seed_reader.secondary_owner ==
                &seed->descriptor_access
                && record->seed_reader.identity ==
                (uintptr_t)&record->seed_reader
                && atomic_load_explicit(&seed->descriptor_access.state,
                memory_order_acquire) == 1
                && atomic_load_explicit(&seed->source_access.state,
                memory_order_acquire) == 0);
        else CHECK(rc == 0 && !record->seed_reader.secondary_owner &&
                !record->seed_reader.identity);
        CHECK(wl_evaluation_control_finish(control,
            session->base.operation_admission, rc, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control,
            &report) == 0 && report.limit == allowance
            && report.consumed == (trial == 2 ? full - 16u : full) &&
            report.result == rc);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
            && !session->cleanup_active && !session->cleanup_pending
            && session->serial_completion.frame == original
            && atomic_load_explicit(&seed->descriptor_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&seed->source_access.state,
            memory_order_acquire) == 0);
        if (trial == 2) {
            CHECK(wl_evaluation_control_configure(control, 0) == 0
                && wl_evaluation_control_begin(control,
                session->base.operation_admission) == 0
                && wl_columnar_eval_stack_cleanup_acquire(session, &frame,
                control) == 0 && frame == original);
            record = NULL;
            CHECK(wl_columnar_eval_delta_seed_gather_controlled(session, frame,
                target, &record, control) == 0
                && record && record->rows && record->rows[0] == input
                && !record->seed_reader.owner &&
                !record->seed_reader.secondary_owner
                && target->nrows == 1
                && wl_evaluation_control_finish(control,
                session->base.operation_admission, 0, 0) == 0
                && wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_retained(session) == 0
            && wl_evaluation_control_detach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = NULL;
        wl_evaluation_control_release(control); wl_session_destroy(base);
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

/* The seed allocator and SINGLE run before this lifecycle. The original
 * token slots start as actual constructor-zero, then are reused only after
 * genuine typed retirement; no target row is published by this helper. */
static void original_seed_bulk(unsigned prior)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(v:int64)\n.decl H(v:int64)\nH(v):-A(v).\n", &error);
    wl_plan_t *plan = NULL; wl_session_t *base = NULL; int64_t input = 7;
    CHECK(program && wl_plan_from_program(program, &plan) == 0
        && wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0
        && wl_backend_columnar()->session_step(base) == 0
        && wl_backend_columnar()->session_insert(base, "H", &input, 1, 1) == 0);
    wl_col_session_t *session = COL_SESSION(base);
    col_rel_t *target = session->rels[1], *raw = NULL;
    if (prior >= 11 && prior <= 13) {
        /* Genuine completed metadata setup, distinct from default implicit
         * generated INT64 heads; no writer/control/frame is held yet. */
        wirelog_column_type_t type = WIRELOG_TYPE_INT64;
        CHECK(col_rel_set_column_types(target, &type, 1) == 0
            && target->column_types && target->column_types[0] == type
            && target->nrows == 1 && target->capacity == 64 &&
            !target->timestamps);
    }
    if (prior >= 4 && prior <= 6) CHECK(col_rel_deep_copy(target, &raw,
            NULL) == 0 && raw);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        session->base.operation_admission) == 0);
    session->base.evaluation_control = control;
    session->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        session->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(session, &frame,
        control) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *original = frame;
    wl_columnar_relation_bulk_append_t *initial_bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    CHECK(initial_bulk && initial_bulk->ready && !initial_bulk->active
        && !initial_bulk->initialized_tokens && !initial_bulk->scratch.identity
        && !initial_bulk->pending.fresh.identity);
    if (prior && prior < 3) {
        if (prior == 2) {
            CHECK(wl_columnar_relation_canonical_schema_controlled(initial_bulk,
                target, session->rels[2], control) == 0 && !initial_bulk->active
                && !initial_bulk->borrowed_mode &&
                initial_bulk->initialized_tokens == 5
                && !initial_bulk->sources.descriptor_count &&
                !initial_bulk->sources.owner_count
                && !initial_bulk->writer.owner &&
                !initial_bulk->descriptor_writer.owner);
        }
        CHECK(wl_evaluation_control_finish(control,
            session->base.operation_admission, 0, 0) == 0
            && wl_evaluation_control_configure(control, 256) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        int stopped =
            wl_columnar_relation_canonical_schema_controlled(initial_bulk,
                target, session->rels[2], control);
        CHECK(stopped == WL_EVALUATION_CONTROL_WORK_LIMIT &&
            !initial_bulk->active
            && !initial_bulk->borrowed_mode &&
            initial_bulk->initialized_tokens == 1
            && initial_bulk->scratch.identity == &initial_bulk->scratch
            && initial_bulk->pending.fresh.identity ==
            (prior == 2 ? &initial_bulk->pending.fresh : NULL)
            && !initial_bulk->sources.descriptor_count &&
            !initial_bulk->sources.owner_count
            && !initial_bulk->writer.owner &&
            !initial_bulk->descriptor_writer.owner
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, stopped, 0) == stopped);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == 0
            && report.limit == 256 && report.consumed == 256 &&
            report.result == stopped
            && wl_evaluation_control_configure(control, 0) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
    }
    wl_columnar_eval_delta_seed_restore_t *seed = NULL;
    CHECK(wl_columnar_eval_delta_seed_gather_controlled(session, frame, target,
        &seed, control) == 0
        && seed && seed->rows && seed->rows[0] == input &&
        seed->row == seed->nrows);
    seed = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_seed_single(session, frame, target,
        &seed, control) == 0
        && wl_columnar_relation_seed_single_acquire_controlled(target,
        &seed->single, control) == 0
        && wl_evaluation_control_finish(control,
        session->base.operation_admission, 0, 0) == 0);
    wl_columnar_relation_bulk_append_t *original_bulk = NULL;
    const uint64_t limits[5] = {416, 544, 1055, 1056, 1056};
    const unsigned prefixes[5] = {0, 1, 4, 5, 5};
    const uint64_t consumed[5] = {416, 544, 928, 1056, 1056};
    for (unsigned trial = 0; trial < 5; trial++) {
        CHECK(wl_evaluation_control_configure(control, limits[trial]) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_relation_bulk_append_t *bulk = NULL;
        int rc = wl_columnar_eval_stack_cleanup_seed_bulk(session, frame, &bulk,
                control);
        CHECK(rc == (trial < 3 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0)
            && bulk && bulk->ready && bulk->active &&
            bulk->initialized_tokens == prefixes[trial]
            && bulk->borrowed_mode == WL_COLUMNAR_RELATION_BULK_SEED_APPEND
            && bulk->borrowed.single == &seed->single &&
            bulk->borrowed.lease == &seed->single.lease
            && bulk->dst == target && seed->single.set.descriptors_acquired == 1
            && seed->single.set.owners_acquired == 1 && seed->rows[0] == input
            && target->nrows == 1 && target->columns[0][0] == input);
        if (!original_bulk) original_bulk = bulk;
        CHECK(bulk == original_bulk
            && (!bulk->initialized_tokens ||
            bulk->scratch.identity == &bulk->scratch)
            && (bulk->initialized_tokens != 5 ||
            bulk->pending.fresh.identity == &bulk->pending.fresh)
            && wl_columnar_relation_bulk_append_retire(bulk, rc) == rc
            && !bulk->active && bulk->exact_phase == 39
            && bulk->borrowed_mode == WL_COLUMNAR_RELATION_BULK_SEED_APPEND
            && wl_columnar_relation_bulk_append_retire(bulk, 0) == 0
            && seed->single.set.descriptors_acquired == 1
            && seed->single.set.owners_acquired == 1
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, rc, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == 0
            && report.limit == limits[trial] &&
            report.consumed == consumed[trial] && report.result == rc);
    }
    if (prior >= 4 && prior <= 6) {
        uint64_t view = target->view_generation,
            storage = target->storage_generation;
        CHECK(col_rel_append_rows_atomic(raw, seed->rows, seed->nrows,
            seed->ncols, NULL) == 0
            && wl_evaluation_control_configure(control, 0) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_relation_bulk_append_t *bulk = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_bulk(session, frame, &bulk,
            control) == 0
            && bulk == original_bulk
            && wl_columnar_eval_stack_cleanup_seed_append(session, frame,
            control) == 0
            && seed->published && seed->single.set.published && bulk->published
            && target->nrows == 2 && target->nrows == raw->nrows
            && target->columns[0][0] == input && target->columns[0][1] == input
            && target->columns[0][0] == raw->columns[0][0]
            && target->columns[0][1] == raw->columns[0][1]
            && target->view_generation == view + 1 &&
            target->storage_generation == storage);
        wl_columnar_relation_bulk_append_t *again = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_bulk(session, frame, &again,
            control) == EINVAL
            && wl_columnar_eval_stack_cleanup_seed_append(session, frame,
            control) == EINVAL
            && target->nrows == 2 && target->view_generation == view + 1
            && seed->published && seed->single.set.published);
        int result = 0;
        if (prior == 5) {
            CHECK(wl_columnar_relation_bulk_append_retire(bulk, 0) == 0);
            seed_single_target = target; seed_single_stop = 3;
            seed_single_reached = 0;
            wl_columnar_relation_test_after_single_writer_release =
                seed_single_release_stop;
            result =
                wl_columnar_relation_seed_single_retire_controlled(
                &seed->single, 0, control);
            wl_columnar_relation_test_after_single_writer_release = NULL;
            CHECK(result < 0 && seed_single_reached == 1
                && !seed->single.set.owners_acquired
                && seed->single.set.descriptors_acquired == 1
                && seed->published && target->nrows == 2
                && target->view_generation == view + 1);
        }
        CHECK(wl_evaluation_control_finish(control,
            session->base.operation_admission, result, 0) == result);
        if (prior == 6) {
            /* Consumed rows survive a real original held-writer refusal.
             * Only the saved identity is restored after serialized return. */
            uintptr_t identity = seed->single.owners[0].writer.identity;
            CHECK(identity == (uintptr_t)&seed->single.owners[0].writer);
            seed->single.owners[0].writer.identity = 0;
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL &&
                !frame
                && session->cleanup_pending &&
                session->serial_completion.frame == original
                && session->serial_completion.payload_ready && seed->published
                && seed->single.set.owners_acquired == 1
                && seed->single.owners[0].writer.owner == &target->source_access
                && target->nrows == 2 && target->view_generation == view + 1);
            seed->single.owners[0].writer.identity = identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(session) == 0
                && !session->cleanup_pending && target->nrows == 2
                && target->view_generation == view + 1
                && atomic_load_explicit(&target->source_access.state,
                memory_order_acquire) == 0
                && atomic_load_explicit(&target->descriptor_access.state,
                memory_order_acquire) == 0
                && wl_columnar_eval_stack_cleanup_retained(session) == 0
                && wl_evaluation_control_detach(control,
                session->base.operation_admission) == 0);
            CHECK(col_rel_destroy_checked(raw) == 0);
            session->base.evaluation_control = NULL;
            wl_evaluation_control_release(control); wl_session_destroy(base);
            wl_plan_free(plan); wirelog_program_free(program);
            return;
        }
    }
    if (prior >= 8 && prior <= 13) {
        /* Source-bound publisher suffix: original gather/SINGLE/BULK setup
         * completed before this new BEGIN; no constructor debit is rebased. */
        bool explicit_type = prior >= 11;
        bool minus_one = prior == 10 || prior == 13;
        CHECK((explicit_type ? target->column_types &&
            target->column_types[0] == WIRELOG_TYPE_INT64
                : !target->column_types) && !target->timestamps &&
            target->capacity == 64
            && wl_evaluation_control_configure(control, 0) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_relation_bulk_append_t *bulk = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_bulk(session, frame, &bulk,
            control) == 0
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, 0, 0) == 0);
        uint64_t exact = explicit_type ? 568 : 564;
        uint64_t limit = exact - minus_one;
        uint64_t view = target->view_generation,
            storage = target->storage_generation;
        CHECK(wl_evaluation_control_configure(control, limit) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        int result = wl_columnar_eval_stack_cleanup_seed_append(session, frame,
                control);
        CHECK(result == (minus_one ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0)
            && seed->row == 1 && !seed->column
            && target->nrows == (minus_one ? 1 : 2)
            && target->view_generation == view + !minus_one
            && target->storage_generation == storage
            && seed->published == !minus_one
            && bulk->published == !minus_one
            && seed->single.set.published == !minus_one
            && target->columns[0][1] == input
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, result, 0) == result);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(control, &report) == 0
            && report.limit == limit &&
            report.consumed == (minus_one ? exact - 32 : exact)
            && report.result == result);
    }
    if (prior == 7) {
        CHECK(wl_evaluation_control_configure(control, 0) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0);
        wl_columnar_relation_bulk_append_t *bulk = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_bulk(session, frame, &bulk,
            control) == 0);
        const void *identity = bulk->scratch.identity;
        uint64_t view = target->view_generation;
        bool denial = target->memory_budget_denial_pending;
        bulk->scratch.identity = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_append(session, frame,
            control) == EINVAL
            && target->nrows == 1 && target->columns[0][0] == input
            && target->view_generation == view &&
            target->memory_budget_denial_pending == denial
            && !seed->row && !seed->column && !seed->published &&
            !bulk->published
            && seed->single.set.owners_acquired == 1
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, EINVAL, 0) == EINVAL
            && wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL && !frame
            && session->cleanup_pending &&
            session->serial_completion.frame == original
            && seed->rows && seed->single.set.owners_acquired == 1);
        bulk->scratch.identity = identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(session) == 0
            && !session->cleanup_pending && target->nrows == 1
            && target->view_generation == view
            && atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&target->descriptor_access.state,
            memory_order_acquire) == 0
            && wl_columnar_eval_stack_cleanup_retained(session) == 0
            && wl_evaluation_control_detach(control,
            session->base.operation_admission) == 0);
        session->base.evaluation_control = NULL;
        wl_evaluation_control_release(control); wl_session_destroy(base);
        wl_plan_free(plan); wirelog_program_free(program);
        return;
    }
    if (prior == 3) {
        /* Genuine terminal8 is still checked while inactive. This is an
         * EMPTY original-slot validation fault, not live-credit refusal. */
        const void *identity = original_bulk->scratch.identity;
        CHECK(identity == &original_bulk->scratch && !original_bulk->active);
        original_bulk->scratch.identity = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL && !frame
            && session->cleanup_pending &&
            session->serial_completion.frame == original
            && session->serial_completion.mode_frame == original
            && session->serial_completion.mode ==
            WL_COLUMNAR_SERIAL_FRAME_OBSERVER_SEED
            && session->serial_completion.payload_ready && seed->rows
            && seed->single.set.descriptors_acquired == 1
            && seed->single.set.owners_acquired == 1);
        original_bulk->scratch.identity = identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(session) == 0
            && !session->cleanup_pending &&
            session->serial_completion.frame == original
            && atomic_load_explicit(&target->descriptor_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) == 0
            && target->nrows == 1 && target->columns[0][0] == input
            && wl_evaluation_control_configure(control, 0) == 0
            && wl_evaluation_control_begin(control,
            session->base.operation_admission) == 0
            && wl_columnar_eval_stack_cleanup_acquire(session, &frame,
            control) == 0
            && frame == original);
        seed = NULL;
        CHECK(wl_columnar_eval_delta_seed_gather_controlled(session, frame,
            target, &seed, control) == 0);
        seed = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_single(session, frame, target,
            &seed, control) == 0
            && wl_columnar_relation_seed_single_acquire_controlled(target,
            &seed->single, control) == 0);
        wl_columnar_relation_bulk_append_t *fresh = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_seed_bulk(session, frame, &fresh,
            control) == 0
            && fresh == original_bulk && fresh->initialized_tokens == 5
            && wl_evaluation_control_finish(control,
            session->base.operation_admission, 0, 0) == 0);
    }
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
        && session->serial_completion.frame == original &&
        !session->cleanup_pending
        && atomic_load_explicit(&target->descriptor_access.state,
        memory_order_acquire) == 0
        && atomic_load_explicit(&target->source_access.state,
        memory_order_acquire) == 0
        && wl_columnar_eval_stack_cleanup_retained(session) == 0
        && wl_evaluation_control_detach(control,
        session->base.operation_admission) == 0);
    if (raw) CHECK(col_rel_destroy_checked(raw) == 0);
    session->base.evaluation_control = NULL;
    wl_evaluation_control_release(control); wl_session_destroy(base);
    wl_plan_free(plan); wirelog_program_free(program);
}

static wl_columnar_relation_bulk_append_t *fresh_seed_bulk;
static unsigned fresh_seed_stop, fresh_seed_fault, fresh_seed_seen;
static uint64_t fresh_seed_payload_bytes, fresh_seed_metadata_bytes;
static wl_columnar_memory_reservation_t *fresh_seed_bad;
static const wl_columnar_memory_reservation_t *fresh_seed_identity;

/* Independent one-column, uncontended enforcing-governor source oracle.
 * See evidence/fresh-seed-cost.json; setup/gather/SINGLE/BULK precede configure. */
static uint64_t fresh_seed_expected(bool governed, uint32_t rows)
{
    const uint64_t common = 128u + 5u * 32u + 16u + 48u;
    const uint64_t shape = 128u + 32u;
    const uint64_t allocation = 1u + 1u + 1u + 1u + 1u + 64u;
    const uint64_t schema = 3u + 1u + 1u + 1u + 2u + 1u + 5u + 5u;
    const uint64_t install = 4u * 64u + 2u * sizeof(struct ArrowSchema) + 128u;
    const uint64_t footer = 128u + 64u + 128u;
    const uint64_t admission = 2u * (3u + 1u + 5u + 4u + 1u + 1u);
    const uint64_t commit = 2u * (1u + 1u + 2u);
    return common + shape + allocation + schema + install + 16u * rows
           + 32u + footer + (governed ? admission + commit + 1u : 0u);
}

static void fresh_seed_credit(const wl_columnar_memory_reservation_t *token,
    const wl_columnar_memory_reservation_t *identity,
    wl_columnar_memory_governor_t *governor, const col_rel_t *owner,
    uint64_t bytes, uint64_t state)
{
    CHECK(token->identity == identity && token->governor == governor
        && token->bytes == bytes && !token->replacement_bytes
        && !token->transition_kind
        && atomic_load_explicit(&token->owner_bits, memory_order_acquire)
        == (uintptr_t)owner
        && atomic_load_explicit(&token->state, memory_order_acquire) == state);
}

static void fresh_seed_checkpoint(wl_evaluation_control_t *control, int phase,
    wl_columnar_relation_bulk_append_t *bulk)
{
    if (bulk != fresh_seed_bulk || fresh_seed_seen) return;
    if (fresh_seed_fault == 3 && phase == 17) {
        CHECK(!bulk->published && bulk->exact_phase == 33
            && bulk->pending.fresh.identity == &bulk->pending.fresh
            && atomic_load_explicit(&bulk->pending.fresh.state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && atomic_load_explicit(&bulk->dst->retained_reservation.state,
            memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        col_rel_t *dst = bulk->dst;
        wl_columnar_memory_governor_t *governor =
            wl_columnar_memory_governor_ref_get(dst->memory_governor);
        CHECK(governor && bulk->final_bytes == fresh_seed_payload_bytes
            && bulk->metadata_bytes == fresh_seed_metadata_bytes
            && bulk->final_bytes && bulk->metadata_bytes
            && bulk->prepared_columns && bulk->prepared_names
            && bulk->prepared_row_scratch && bulk->prepared_schema_ok
            && bulk->prepared_schema.release && !dst->columns
            && !dst->col_names && !dst->schema_ok);
        fresh_seed_credit(&bulk->pending.fresh, &bulk->pending.fresh,
            NULL, NULL, 0, WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        fresh_seed_credit(&dst->retained_reservation,
            &dst->retained_reservation, governor, dst, bulk->final_bytes,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        fresh_seed_credit(&bulk->metadata_pending, &bulk->metadata_pending,
            governor, dst, bulk->metadata_bytes,
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        fresh_seed_credit(&dst->metadata_reservation,
            &dst->metadata_reservation,
            NULL, NULL, 0, WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        fresh_seed_credit(&bulk->previous, &bulk->previous, NULL, NULL, 0,
            WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        fresh_seed_credit(&bulk->old_metadata, &bulk->old_metadata,
            NULL, NULL, 0, WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        fresh_seed_credit(&bulk->scratch, &bulk->scratch, NULL, NULL, 0,
            WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        fresh_seed_bad = &bulk->dst->metadata_reservation;
        fresh_seed_identity = fresh_seed_bad->identity;
        fresh_seed_bad->identity = &bulk->scratch;
        fresh_seed_seen++;
    } else if (fresh_seed_fault && phase == 16) {
        CHECK(bulk->published && bulk->exact_phase == 38
            && bulk->active && bulk->borrowed.seed_schema_fresh);
        fresh_seed_bad = fresh_seed_fault == 1
            ? &bulk->dst->retained_reservation :
            &bulk->dst->metadata_reservation;
        fresh_seed_identity = fresh_seed_bad->identity;
        fresh_seed_bad->identity = &bulk->scratch;
        fresh_seed_seen++;
    } else if (fresh_seed_stop == (unsigned)phase) {
        CHECK(control);
        fresh_seed_seen++;
        wl_evaluation_control_request_cancel(control);
    }
}

/* This target is a real separate constructor-owned relation. Name lookup
 * gathers the session's real $in$H, without claiming whole STEP activation. */
static uint64_t fresh_seed_schema_rows(bool governed, unsigned stop,
    unsigned fault,
    uint64_t allowance, bool limited, uint32_t rows)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl A(v:int64)\n.decl H(v:int64)\nH(v):-A(v).\n", &error);
    wl_plan_t *plan = NULL; wl_session_t *base = NULL;
    int64_t *input = malloc((size_t)rows * sizeof(*input));
    CHECK(input && rows);
    for (uint32_t i = 0; i < rows; i++) input[i] = (int64_t)i * 11 + 7;
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *owner =
        wl_columnar_memory_governor_ref_create(&resolution);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = owner;
    CHECK(program && wl_plan_from_program(program, &plan) == 0
        && owner && wl_session_create_with_options(wl_backend_columnar(), plan,
        1, &options, &base) == 0
        && wl_backend_columnar()->session_step(base) == 0
        && wl_backend_columnar()->session_insert(base, "H", input, rows,
        1) == 0);
    wl_columnar_memory_governor_ref_release(owner);
    wl_col_session_t *session = COL_SESSION(base);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(session->memory_governor);
    uint64_t initial_reserved = wl_columnar_memory_reserved(governor);
    uint64_t usable = atomic_load_explicit(&governor->usable_bytes,
            memory_order_relaxed);
    col_rel_t *target = NULL, *raw = NULL;
    CHECK((governed ? wl_columnar_relation_alloc_governed(&target, "H",
        session->memory_governor) : col_rel_alloc(&target, "H")) == 0);
    CHECK((governed ? wl_columnar_relation_alloc_governed(&raw, "H",
        session->memory_governor) : col_rel_alloc(&raw, "H")) == 0);
    CHECK(!target->schema_ok && !target->columns && !target->capacity
        && col_rel_append_rows_atomic(raw, input, rows, 1, NULL) == 0);
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(0, &control) == 0
        && wl_evaluation_control_attach(control,
        session->base.operation_admission) == 0);
    session->base.evaluation_control = control;
    session->serial_completion.pending = true;
    CHECK(wl_evaluation_control_begin(control,
        session->base.operation_admission) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_delta_seed_restore_t *seed = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_acquire(session, &frame, control) == 0
        && wl_columnar_eval_delta_seed_gather_controlled(session, frame, target,
        &seed, control) == 0 && seed->nrows == rows && seed->ncols == 1);
    if (fault == 4 || fault == 5) {
        CHECK(governed && governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
            && !seed->seed_reader.owner && !seed->seed_reader.secondary_owner);
        atomic_store_explicit(&governor->usable_bytes,
            wl_columnar_memory_reserved(governor)
            + (fault == 5 ? raw->metadata_reserved_bytes : 0),
            memory_order_relaxed);
    }
    seed = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_seed_single(session, frame, target,
        &seed, control) == 0
        && wl_columnar_relation_seed_single_acquire_controlled(target,
        &seed->single, control) == 0);
    fresh_seed_bulk = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_seed_bulk(session, frame,
        &fresh_seed_bulk, control) == 0
        && wl_evaluation_control_finish(control,
        session->base.operation_admission, 0, 0) == 0
        && wl_evaluation_control_configure(control, allowance) == 0
        && wl_evaluation_control_begin(control,
        session->base.operation_admission) == 0);
    fresh_seed_stop = stop; fresh_seed_fault = fault; fresh_seed_seen = 0;
    fresh_seed_bad = NULL;
    uint64_t before_append = wl_columnar_memory_reserved(governor);
    fresh_seed_payload_bytes = raw->retained_reserved_bytes;
    fresh_seed_metadata_bytes = raw->metadata_reserved_bytes;
    if (fault == 6) wl_columnar_relation_test_fail_next_metadata_alloc();
    wl_columnar_relation_test_bulk_checkpoint = fresh_seed_checkpoint;
    int result = wl_columnar_eval_stack_cleanup_seed_append(session, frame,
            control);
    wl_columnar_relation_test_bulk_checkpoint = NULL;
    CHECK(fault == 6 ? result == ENOMEM && !fresh_seed_seen
        : fault >= 4 ? result == ENOSPC && !fresh_seed_seen
        : fault ? result == EINVAL && fresh_seed_seen == 1
        : stop ? result == WL_EVALUATION_CONTROL_CANCELLED &&
        fresh_seed_seen == 1
        : limited ? result == WL_EVALUATION_CONTROL_WORK_LIMIT : result == 0);
    CHECK(wl_evaluation_control_finish(control,
        session->base.operation_admission, result, 0) == result);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control, &report) == 0);
    if (limited) {
        CHECK(allowance == fresh_seed_expected(governed, rows) - 1u
            && report.consumed == fresh_seed_expected(governed, rows) - 128u
            && fresh_seed_bulk->exact_phase == 38 && fresh_seed_bulk->active
            && fresh_seed_bulk->transaction_active && fresh_seed_bulk->published
            && seed->published && seed->row == rows && !seed->column
            && seed->single.set.descriptors_acquired == 1
            && seed->single.set.owners_acquired == 1);
    } else if (!stop && !fault)
        CHECK(report.consumed == fresh_seed_expected(governed, rows));
    if (!stop && fault < 3) {
        CHECK(target->schema_ok && target->nrows == raw->nrows
            && target->ncols == raw->ncols && target->capacity == raw->capacity
            && target->view_generation == raw->view_generation
            && target->storage_generation == raw->storage_generation
            && target->retained_reserved_bytes == raw->retained_reserved_bytes
            && target->metadata_reserved_bytes == raw->metadata_reserved_bytes
            && strcmp(target->schema.format, raw->schema.format) == 0
            && strcmp(target->col_names[0], raw->col_names[0]) == 0);
        for (uint32_t i = 0; i < rows; i++)
            CHECK(target->columns[0][i] == raw->columns[0][i]);
    }
    if (stop >= 12 && stop <= 14)
        CHECK(!target->schema_ok && !target->columns && !target->nrows
            && !target->retained_reserved_bytes &&
            !target->metadata_reserved_bytes);
    if (fault >= 4) {
        CHECK(target->memory_budget_denial_pending == (fault != 6)
            && !target->schema_ok
            && !target->columns && !target->retained_reserved_bytes
            && !target->metadata_reserved_bytes);
        CHECK(atomic_load_explicit(&fresh_seed_bulk->metadata_pending.state,
            memory_order_acquire) == (governed && fault != 4
            ? WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            : WL_COLUMNAR_MEMORY_RESERVATION_EMPTY));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame
            && wl_columnar_memory_reserved(governor) == before_append);
        atomic_store_explicit(&governor->usable_bytes, usable,
            memory_order_relaxed);
    } else if (fault) {
        uint64_t reserved = wl_columnar_memory_reserved(governor);
        if (fault == 3)
            CHECK(reserved == before_append + fresh_seed_payload_bytes
                + fresh_seed_metadata_bytes);
        wl_columnar_eval_stack_cleanup_frame_t *original = frame;
        if (fault == 1)
            CHECK(wl_columnar_relation_seed_restore_retire(seed,
                fresh_seed_bulk, EOVERFLOW) == EOVERFLOW
                && fresh_seed_bulk->active &&
                seed->single.set.owners_acquired == 1
                && wl_columnar_memory_reserved(governor) == reserved);
        CHECK(fresh_seed_bulk->exact_phase == (fault == 3 ? 33 : 38)
            && fresh_seed_bulk->active
            && seed->single.set.descriptors_acquired == 1
            && seed->single.set.owners_acquired == 1
            && wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL
            && !frame && session->cleanup_pending
            && session->serial_completion.frame == original);
        if (fault == 3) {
            CHECK(!target->schema_ok && !target->columns && !target->nrows
                && fresh_seed_bulk->exact_phase == 42
                && atomic_load_explicit(&fresh_seed_bulk->pending.fresh.state,
                memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
                && atomic_load_explicit(&target->retained_reservation.state,
                memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
                && wl_columnar_memory_reserved(governor)
                == reserved - fresh_seed_bulk->final_bytes
                && wl_columnar_memory_reserved(governor)
                == before_append + fresh_seed_bulk->metadata_bytes
                && !fresh_seed_bulk->prepared_columns
                && !fresh_seed_bulk->prepared_names
                && !fresh_seed_bulk->prepared_row_scratch
                && !fresh_seed_bulk->prepared_timestamps
                && !fresh_seed_bulk->prepared_schema_ok
                && !fresh_seed_bulk->prepared_schema.release
                && !fresh_seed_bulk->count && !fresh_seed_bulk->owned_columns
                && !fresh_seed_bulk->base);
            fresh_seed_credit(&fresh_seed_bulk->pending.fresh,
                &fresh_seed_bulk->pending.fresh, governor, target,
                fresh_seed_bulk->final_bytes,
                WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
            fresh_seed_credit(&fresh_seed_bulk->metadata_pending,
                &fresh_seed_bulk->metadata_pending, governor, target,
                fresh_seed_bulk->metadata_bytes,
                WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
            fresh_seed_credit(&target->retained_reservation,
                &target->retained_reservation, NULL, NULL, 0,
                WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
            fresh_seed_credit(&target->metadata_reservation,
                &fresh_seed_bulk->scratch, NULL, NULL, 0,
                WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
        } else CHECK(wl_columnar_memory_reserved(governor) == reserved);
        CHECK(session->serial_completion.frame == original);
        fresh_seed_bad->identity = fresh_seed_identity;
        CHECK(wl_columnar_eval_stack_cleanup_retry(session) == 0
            && !session->cleanup_pending
            && wl_columnar_memory_reserved(governor)
            == (fault == 3 ? before_append : reserved));
    } else CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
    CHECK(!atomic_load_explicit(&target->descriptor_access.state,
        memory_order_acquire)
        && !atomic_load_explicit(&target->source_access.state,
        memory_order_acquire)
        && wl_columnar_eval_stack_cleanup_retained(session) == 0
        && wl_evaluation_control_detach(control,
        session->base.operation_admission) == 0);
    CHECK(col_rel_destroy_checked(raw) == 0
        && col_rel_destroy_checked(target) == 0
        && wl_columnar_memory_reserved(governor) == initial_reserved);
    session->base.evaluation_control = NULL;
    wl_evaluation_control_release(control); wl_session_destroy(base);
    wl_plan_free(plan); wirelog_program_free(program);
    free(input);
    fresh_seed_bulk = NULL; fresh_seed_bad = NULL;
    return report.consumed;
}

static uint64_t fresh_seed_schema(bool governed, unsigned stop, unsigned fault,
    uint64_t allowance, bool limited)
{
    return fresh_seed_schema_rows(governed, stop, fault, allowance, limited, 3);
}

int main(void)
{
    for (unsigned governed = 0; governed < 2; governed++) {
        uint64_t full = fresh_seed_expected(governed != 0, 3);
        CHECK(fresh_seed_schema(governed != 0, 0, 0, 0, false) == full);
        CHECK(fresh_seed_schema(governed != 0, 0, 0, full, false) == full);
        CHECK(fresh_seed_schema(governed != 0, 0, 0, full, false) == full);
        CHECK(fresh_seed_schema(governed != 0, 0, 0, full - 1, true)
            == full - 128u);
        fresh_seed_schema(governed != 0, 0, 1, 0, false);
        fresh_seed_schema(governed != 0, 0, 2, 0, false);
        fresh_seed_schema(governed != 0, 0, 6, 0, false);
        if (governed) {
            fresh_seed_schema(true, 0, 3, 0, false);
            fresh_seed_schema(true, 0, 4, 0, false);
            fresh_seed_schema(true, 0, 5, 0, false);
        }
        fresh_seed_schema(governed != 0, 12, 0, 0, false);
        if (governed) fresh_seed_schema(true, 13, 0, 0, false);
        fresh_seed_schema(governed != 0, 14, 0, 0, false);
        fresh_seed_schema(governed != 0, 15, 0, 0, false);
        fresh_seed_schema(governed != 0, 10, 0, 0, false);
        fresh_seed_schema(governed != 0, 16, 0, 0, false);
        fresh_seed_schema_rows(governed != 0, 0, 0, 0, false, 300);
    }
    original_seed_single();
    original_seed_gather();
    original_seed_name_boundaries();
    original_seed_gather_exact();
    original_seed_bulk(0);
    original_seed_bulk(1);
    original_seed_bulk(2);
    original_seed_bulk(3);
    original_seed_bulk(4);
    original_seed_bulk(5);
    original_seed_bulk(6);
    original_seed_bulk(7);
    original_seed_bulk(8);
    original_seed_bulk(9);
    original_seed_bulk(10);
    original_seed_bulk(11);
    original_seed_bulk(12);
    original_seed_bulk(13);
    original_consolidation_caller();
    original_source_preflight();
    original_image_publication();
    original_image_preparation_boundaries();
    original_normalized_handoff_boundaries();
    original_normalized_append(300, 8);
    original_normalized_append(300, 9);
    original_normalized_append(300, 10);
    original_normalized_append(300, 11);
    original_normalized_append(300, 12);
    original_normalized_append(300, 13);
    original_normalized_append(300, 14);
    original_normalized_append(300, 15);
    original_normalized_append(300, 16);
    original_normalized_append(10001, 17);
    original_normalized_append(10001, 18);
    original_normalized_append(10001, 19);
    original_normalized_append(300, 20);
    original_normalized_append(10001, 21);
    original_normalized_descriptor(0);
    original_normalized_descriptor(1);
    original_normalized_descriptor(2);
    original_normalized_descriptor(3);
    original_normalized_append(0, false);
    original_normalized_append(3, false);
    original_normalized_append(300, false);
    original_normalized_append(300, true);
    original_normalized_append(3, 2);
    original_normalized_append(3, 3);
    original_normalized_append(3, 4);
    original_normalized_append(3, 5);
    original_normalized_append(3, 6);
    original_normalized_append(3, 6);
    original_normalized_append(3, 7);
    original_replacement_writer();
    original_transient_stage(false);
    original_transient_stage(true);
    original_replacement_prepare(false, false);
    original_replacement_prepare(true, false);
    original_replacement_prepare(true, true);
    original_replacement_prepare(true, 2);
    original_replacement_prepare(true, 3);
    original_candidate_disposal(0);
    original_candidate_disposal(1);
    original_candidate_disposal(2);
    original_candidate_disposal(3);
    original_metadata_disposal(false, false);
    original_metadata_disposal(true, false);
    original_metadata_disposal(true, true);
    original_nonempty_stage();
    original_capture_stop();
    original_empty_recovery(false, false);
    original_empty_recovery(true, false);
    original_empty_recovery(true, true);
    puts("evaluation_observer: original metadata disposal OK");
    return 0;
}
