/* Actual scalar reader ownership, bounded END and stopped financial returns. */
#include "../wirelog/columnar/internal.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

static int original_owner, financial_owner;
static wl_columnar_source_access_reader_t contender;
static bool contended, stop_seen;
static unsigned stop_cursor, stop_mode;

static wl_columnar_relation_scalar_source_t *begin_source;
static const col_rel_t *begin_descriptor, *begin_root;
static unsigned begin_stop_point;
static bool begin_stopped, begin_turnover;
static wl_evaluation_control_report_t begin_frozen;
static uint64_t begin_successor;

static void
begin_fail(wl_evaluation_control_t *control)
{
    if (!begin_turnover) {
        wl_evaluation_control_request_cancel(control); return;
    }
    CHECK(!wl_evaluation_control_finish(control, &original_owner, 0, 0));
    CHECK(!wl_evaluation_control_report(control, &begin_frozen));
    CHECK(!wl_evaluation_control_begin(control, &original_owner));
    begin_successor = wl_evaluation_control_active_attempt_id(control);
    CHECK(begin_successor != begin_frozen.attempt_id);
}

static void
begin_reader_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!begin_source || begin_stopped || !acquired) return;
    const wl_columnar_source_access_gate_t *target = begin_stop_point == 2
        ? &begin_descriptor->descriptor_access : &begin_root->source_access;
    if ((begin_stop_point == 2 || begin_stop_point == 3) && gate == target) {
        begin_stopped = true;
        begin_fail(control);
    }
}

static void
begin_work_checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    (void)work;
    if (!begin_source || begin_stopped
        || begin_source->phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD)
        return;
    if ((begin_stop_point == 4 && !begin_source->descriptor_identity)
        || (begin_stop_point == 5 && begin_source->descriptor_identity)) {
        begin_stopped = true;
        begin_fail(control);
    }
}

static wl_evaluation_control_t *
new_control(uint64_t limit, int *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(limit, &control));
    CHECK(!wl_evaluation_control_attach(control, owner));
    CHECK(!wl_evaluation_control_begin(control, owner));
    return control;
}

static wl_evaluation_control_report_t
close_control(wl_evaluation_control_t *control, int *owner)
{
    (void)wl_evaluation_control_finish(control, owner, 0, 0);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(!wl_evaluation_control_detach(control, owner));
    wl_evaluation_control_release(control);
    return report;
}

static void
race_release(const wl_columnar_source_access_reader_t *reader,
    wl_columnar_source_access_gate_t *gate, uint64_t observed)
{
    (void)reader;
    if (contended) return;
    CHECK(observed && !wl_columnar_source_access_reader_acquire(gate,
        &contender));
    contended = true;
}

static void
end_checkpoint(const wl_columnar_relation_scalar_source_t *source,
    wl_evaluation_control_t *funding, uint64_t id, unsigned point)
{
    if (stop_seen || point != 1) return;
    unsigned cursor = source->phase ==
        WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED
        ? 5 : source->end_cursor;
    if (!stop_mode || cursor != stop_cursor) return;
    stop_seen = true;
    if (stop_mode == 1) wl_evaluation_control_request_cancel(funding);
    else CHECK(wl_evaluation_control_charge_attempt(funding, id, 1)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
}

typedef struct {
    wl_columnar_relation_scalar_source_t *source;
    wl_evaluation_control_t *funding;
    uint64_t id;
} peer_t;

static void *
wrong_thread(void *arg)
{
    peer_t *peer = arg;
    bool done = false;
    CHECK(wl_columnar_relation_scalar_source_end_step(peer->source, EIO,
        peer->funding, peer->id, &done) == EINVAL && !done);
    return NULL;
}

static void
same_report(wl_evaluation_control_t *control,
    const wl_evaluation_control_report_t *expected)
{
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report));
    CHECK(report.attempt_id == expected->attempt_id &&
        report.limit == expected->limit
        && report.consumed == expected->consumed &&
        report.stop_reason == expected->stop_reason
        && report.execution_status == expected->execution_status
        && report.cleanup_status == expected->cleanup_status &&
        report.result == expected->result);
}

static void
run_end(uint32_t width, bool alias, unsigned phase, unsigned original_mode,
    unsigned financial_mode, unsigned cursor, bool semantic_error, bool race)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    col_rel_t *root = NULL;
    CHECK(!wl_columnar_relation_new_auto_governed("root", width, 4, false,
        governor, &root));
    int64_t row[257] = {0};
    CHECK(width <= 257);
    for (uint32_t i = 0; i < width; ++i) row[i] = i + 1;
    wirelog_column_type_t types[257];
    for (uint32_t i = 0; i < width;
        ++i) types[i] = (wirelog_column_type_t)(i % 7);
    if (width) CHECK(!col_rel_set_column_types(root, types, width));
    if (alias) CHECK(!col_rel_enable_timestamps(root));
    CHECK(!col_rel_append_row(root, row));
    if (root->timestamps) root->timestamps[0].multiplicity = -7;
    col_rel_t *descriptor = root;
    if (alias) {
        descriptor = col_rel_new_like("alias", root);
        CHECK(descriptor && !col_rel_install_shared_view(descriptor, root));
    }
    col_relation_snapshot_t snapshot =
        wl_columnar_relation_snapshot(descriptor);
    uint64_t baseline =
        wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor));
    wl_evaluation_control_t *original = new_control(1000000, &original_owner);
    uint64_t original_id = wl_evaluation_control_active_attempt_id(original);
    wl_columnar_relation_scalar_source_t source;
    bool initialized = false;
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(&source,
        &initialized, original, original_id) && initialized);
    if (phase >= 2) {
        begin_source = &source; begin_descriptor = descriptor;
        begin_root = root;
        begin_stop_point = phase >= 6 ? phase - 4 : phase;
        begin_stopped = false; begin_turnover = phase >= 6; begin_successor = 0;
        wl_columnar_relation_test_after_reader_cas = begin_reader_checkpoint;
        wl_columnar_relation_test_work_checkpoint = begin_work_checkpoint;
        CHECK(
            wl_columnar_relation_scalar_source_begin_retained_attempt_controlled
            (
                &source, descriptor,
                alias) ==
            (phase >= 6 ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
        wl_columnar_relation_test_after_reader_cas = NULL;
        wl_columnar_relation_test_work_checkpoint = NULL;
        begin_source = NULL;
        CHECK(begin_stopped &&
            source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_ENDING
            && source.first_result_saved &&
            source.first_result ==
            (phase >= 6 ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
        CHECK(atomic_load_explicit(&descriptor->descriptor_access.state,
            memory_order_acquire) == 1);
        CHECK(atomic_load_explicit(&root->source_access.state,
            memory_order_acquire)
            == (begin_stop_point == 2 ? 0u : 1u));
        CHECK(
            wl_columnar_relation_scalar_source_begin_retained_attempt_controlled
            (
                &source, descriptor, alias) == EINVAL);
    } else if (phase) CHECK(
            !wl_columnar_relation_scalar_source_begin_attempt_controlled(
                &source, descriptor, alias));
    uint64_t used_before = 0;
    if (phase >= 6) used_before = begin_frozen.consumed;
    else CHECK(!wl_evaluation_control_test_active_consumed(original,
            original_id, &used_before));
    wl_evaluation_control_report_t frozen = {0};
    uint64_t successor = 0;
    if (phase >= 6) {
        frozen = begin_frozen; successor = begin_successor;
        CHECK(original_mode == 2 && successor);
    } else if (original_mode) {
        CHECK(wl_evaluation_control_finish(original, &original_owner, 0, 0)
            == (phase >= 2 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
        CHECK(!wl_evaluation_control_report(original, &frozen));
        if (original_mode == 2) {
            CHECK(!wl_evaluation_control_reset(original));
            CHECK(!wl_evaluation_control_begin(original, &original_owner));
            successor = wl_evaluation_control_active_attempt_id(original);
            CHECK(successor != original_id);
        }
    }
    bool done = false;
    CHECK(wl_columnar_relation_scalar_source_end_step(&source, EIO,
        original, successor ? successor : original_id,
        &done) == EINVAL && !done);
    wl_evaluation_control_t *funding = new_control(127, &financial_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    wl_columnar_relation_scalar_source_t copy = source;
    CHECK(wl_columnar_relation_scalar_source_end_step(&copy, EIO, funding, id,
        &done) == EINVAL && !done);
    CHECK(wl_columnar_relation_scalar_source_end_step(&source, EIO, funding,
        id + 1,
        &done) == EINVAL && !done);
    peer_t peer = {&source, funding, id}; wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, wrong_thread,
        &peer) && !wl_thread_join(&thread));
    CHECK(wl_columnar_relation_scalar_source_end_step(&source, EIO, funding, id,
        &done) == WL_EVALUATION_CONTROL_WORK_LIMIT && !done);
    wl_evaluation_control_report_t report = close_control(funding,
            &financial_owner);
    CHECK(!report.consumed &&
        report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(source.phase ==
        (phase >= 2 ? WL_COLUMNAR_RELATION_SCALAR_SOURCE_ENDING
        : phase ? WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD :
        WL_COLUMNAR_RELATION_SCALAR_SOURCE_INERT)
        && source.first_result_saved == (phase >= 2));
    funding = new_control(128, &financial_owner);
    id = wl_evaluation_control_active_attempt_id(funding);
    wl_evaluation_control_request_cancel(funding);
    CHECK(wl_columnar_relation_scalar_source_end_step(&source, EIO, funding, id,
        &done) == WL_EVALUATION_CONTROL_CANCELLED && !done);
    report = close_control(funding, &financial_owner);
    CHECK(!report.consumed && report.result == WL_EVALUATION_CONTROL_CANCELLED
        && source.first_result_saved == (phase >= 2));
    stop_mode = financial_mode; stop_cursor = cursor; stop_seen = false;
    contended = false; memset(&contender, 0, sizeof(contender));
    wl_columnar_relation_test_scalar_end_checkpoint = end_checkpoint;
    if (race) wl_columnar_source_access_test_before_reader_release_cas =
            race_release;
    unsigned calls = 0;
    do {
        CHECK(++calls < 20);
        funding = new_control(128, &financial_owner);
        id = wl_evaluation_control_active_attempt_id(funding);
        uint64_t owner_before = atomic_load_explicit(&root->source_access.state,
                memory_order_acquire);
        uint64_t descriptor_before =
            atomic_load_explicit(&descriptor->descriptor_access.state,
                memory_order_acquire);
        bool terminal_before = source.phase ==
            WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED;
        int rc = wl_columnar_relation_scalar_source_end_step(&source,
                calls == 1 ? semantic_error ? EIO : 0 : ERANGE, funding, id,
                &done);
        if (rc != 0 && rc != WL_EVALUATION_CONTROL_CANCELLED &&
            rc != WL_EVALUATION_CONTROL_WORK_LIMIT)
            fprintf(stderr,
                "width=%u alias=%u initial=%u original=%u financial=%u target=%u call=%u phase=%u cursor=%u rc=%d\n",
                width, alias, phase, original_mode, financial_mode, cursor,
                calls, source.phase, source.end_cursor, rc);
        CHECK(rc == 0 || rc == WL_EVALUATION_CONTROL_CANCELLED
            || rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
        report = close_control(funding, &financial_owner);
        CHECK(report.consumed == (terminal_before ? 0 : 128) &&
            report.result == rc);
        if (contender.owner) {
            CHECK(contended &&
                !wl_columnar_source_access_reader_release(&contender));
            wl_columnar_source_access_test_before_reader_release_cas = NULL;
        }
        uint64_t owner_after = atomic_load_explicit(&root->source_access.state,
                memory_order_acquire);
        uint64_t descriptor_after =
            atomic_load_explicit(&descriptor->descriptor_access.state,
                memory_order_acquire);
        CHECK(owner_after <= owner_before &&
            descriptor_after <= descriptor_before
            && owner_before - owner_after + descriptor_before -
            descriptor_after <= 1);
        if (source.phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) {
            uint32_t out_width, rows; bool timestamps;
            CHECK(wl_columnar_relation_scalar_source_shape_attempt_controlled(
                    &source,
                    &out_width, &rows, &timestamps) == EINVAL);
        }
        if (original_mode) same_report(original, &frozen);
    } while (!done);
    wl_columnar_relation_test_scalar_end_checkpoint = NULL;
    wl_columnar_source_access_test_before_reader_release_cas = NULL;
    CHECK(calls >= 5 && source.first_result_saved
        && source.first_result ==
        (phase >= 6 ? EINVAL : phase >= 2 ? WL_EVALUATION_CONTROL_CANCELLED
            : semantic_error ? EIO : 0));
    CHECK(!financial_mode || stop_seen);
    CHECK(!race || contended);
    uint64_t used = UINT64_MAX;
    if (!original_mode || successor) {
        CHECK(!wl_evaluation_control_test_active_consumed(original,
            successor ? successor : original_id, &used));
        CHECK(used == (successor ? 0 : used_before));
        CHECK(wl_evaluation_control_finish(original, &original_owner, 0, 0)
            == (phase >= 2 &&
            !successor ? WL_EVALUATION_CONTROL_CANCELLED : 0));
    }
    CHECK(!wl_evaluation_control_detach(original, &original_owner));
    wl_evaluation_control_release(original);
    done = false;
    CHECK(!wl_columnar_relation_scalar_source_end_step(&source, ERANGE, NULL, 0,
        &done) && done);
    CHECK(wl_columnar_relation_scalar_source_end(&source, ERANGE)
        == (phase >= 6 ? EINVAL : phase >=
        2 ? WL_EVALUATION_CONTROL_CANCELLED : semantic_error ? EIO : 0));
    CHECK(wl_columnar_relation_snapshot_equal(snapshot,
        wl_columnar_relation_snapshot(descriptor)));
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)) == baseline);
    if (root->timestamps) CHECK(root->timestamps[0].multiplicity == -7);
    if (width) CHECK(!memcmp(root->column_types, types,
            width * sizeof(*types)));
    for (uint32_t i = 0; i < width; ++i) CHECK(root->columns[i][0] == row[i]);
    CHECK(!atomic_load_explicit(&root->source_access.state,
        memory_order_acquire)
        && !atomic_load_explicit(&descriptor->descriptor_access.state,
        memory_order_acquire));
    if (alias) CHECK(!col_rel_destroy_checked(descriptor));
    CHECK(!col_rel_destroy_checked(root));
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
}

static void
legacy_begin_control(unsigned point, bool alias)
{
    col_rel_t *root = col_rel_new_auto("legacy", 1);
    CHECK(root); int64_t value = 9; CHECK(!col_rel_append_row(root, &value));
    col_rel_t *descriptor = alias ? col_rel_new_like("alias", root) : root;
    CHECK(descriptor);
    if (alias) CHECK(!col_rel_install_shared_view(descriptor, root));
    wl_evaluation_control_t *control = new_control(1000000, &original_owner);
    wl_columnar_relation_scalar_source_t source; bool initialized = false;
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(&source,
        &initialized, control,
        wl_evaluation_control_active_attempt_id(control)) && initialized);
    begin_source = &source; begin_descriptor = descriptor; begin_root = root;
    begin_stop_point = point; begin_stopped = false; begin_turnover = false;
    wl_columnar_relation_test_after_reader_cas = begin_reader_checkpoint;
    wl_columnar_relation_test_work_checkpoint = begin_work_checkpoint;
    CHECK(wl_columnar_relation_scalar_source_begin_attempt_controlled(&source,
        descriptor, false) == WL_EVALUATION_CONTROL_CANCELLED);
    wl_columnar_relation_test_after_reader_cas = NULL;
    wl_columnar_relation_test_work_checkpoint = NULL;
    begin_source = NULL;
    CHECK(begin_stopped &&
        source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED
        && source.first_result == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!atomic_load_explicit(&root->source_access.state,
        memory_order_acquire)
        && !atomic_load_explicit(&descriptor->descriptor_access.state,
        memory_order_acquire));
    wl_evaluation_control_report_t report = close_control(control,
            &original_owner);
    CHECK(report.result == WL_EVALUATION_CONTROL_CANCELLED);
    if (alias) CHECK(!col_rel_destroy_checked(descriptor));
    CHECK(!col_rel_destroy_checked(root));
}

static void
unsupported_begin(bool retained)
{
    col_rel_t *descriptor = NULL;
    CHECK(!col_rel_alloc(&descriptor, "unsupported"));
    wl_evaluation_control_t *original = new_control(1000000, &original_owner);
    wl_columnar_relation_scalar_source_t source; bool initialized = false;
    CHECK(!wl_columnar_relation_scalar_source_init_attempt_controlled(&source,
        &initialized, original,
        wl_evaluation_control_active_attempt_id(original)) && initialized);
    int rc =
        retained ?
        wl_columnar_relation_scalar_source_begin_retained_attempt_controlled(
        &source, descriptor,
        false) : wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &source, descriptor, false);
    CHECK(rc == ENOTSUP && source.first_result_saved &&
        source.first_result == ENOTSUP);
    CHECK(source.phase == (retained ? WL_COLUMNAR_RELATION_SCALAR_SOURCE_ENDING
        : WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED));
    CHECK(atomic_load_explicit(&descriptor->descriptor_access.state,
        memory_order_acquire)
        == (retained ? 1u : 0u));
    if (retained) {
        bool done = false;
        for (unsigned calls = 0; !done; ++calls) {
            CHECK(calls < 5);
            wl_evaluation_control_t *funding = new_control(128,
                    &financial_owner);
            CHECK(!wl_columnar_relation_scalar_source_end_step(&source, ERANGE,
                funding, wl_evaluation_control_active_attempt_id(funding),
                &done));
            wl_evaluation_control_report_t report = close_control(funding,
                    &financial_owner);
            CHECK(report.consumed == 128 && !report.result);
        }
        CHECK(source.first_result == ENOTSUP &&
            source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED);
    }
    CHECK(!atomic_load_explicit(&descriptor->descriptor_access.state,
        memory_order_acquire));
    (void)close_control(original, &original_owner);
    CHECK(!col_rel_destroy_checked(descriptor));
}

int
main(void)
{
    const uint32_t widths[] = {0, 1, 17, 257};
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned alias = 0; alias < 2; ++alias)
            for (unsigned phase = 0; phase < 6; ++phase)
                for (unsigned original = 0; original < 3; ++original)
                    for (unsigned semantic = 0; semantic < 2; ++semantic)
                        run_end(widths[w], alias, phase, original, 0, 0,
                            semantic, false);
    for (unsigned mode = 1; mode <= 2; ++mode)
        for (unsigned cursor = 1; cursor <= 5; ++cursor)
            run_end(17, true, 1, 2, mode, cursor, true, false);
    for (unsigned alias = 0; alias < 2; ++alias)
        for (unsigned point = 6; point <= 9; ++point)
            run_end(17, alias, point, 2, 0, 0, false, false);
    for (unsigned alias = 0; alias < 2; ++alias)
        for (unsigned point = 2; point <= 5; ++point)
            legacy_begin_control(point, alias);
    run_end(257, true, 1, 2, 0, 0, false, true);
    unsupported_begin(false); unsupported_begin(true);
    return 0;
}
