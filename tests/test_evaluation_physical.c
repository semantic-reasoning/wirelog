/* Physical transport oracles: these compare timestamp arrays, not callbacks. */
#include "../wirelog/columnar/internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); abort(); \
                      } } while (0)

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    (void)token; (void)phase;
}

static int owner;
static unsigned fault;
static bool hit;
static col_rel_t *draft;
static wl_columnar_relation_scalar_source_t *capturing_source;

static void
physical_checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    if (hit || !fault || fault > 4) return;
    bool metadata = fault <= 2 && !draft && capturing_source
        && capturing_source->phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
        && capturing_source->descriptor && capturing_source->owner
        && capturing_source->timestamps
        && capturing_source->preserve_timestamps;
    bool value_tail = fault >= 3 && draft && draft->nrows && work == 128;
    if (!metadata && !value_tail) return;
    hit = true;
    if (fault == 2 || fault == 4)
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
equal_stamp(const col_delta_timestamp_t *actual,
    const col_delta_timestamp_t *expected)
{
    CHECK(actual->iteration == expected->iteration);
    CHECK(actual->stratum == expected->stratum);
    CHECK(actual->worker == expected->worker);
    CHECK(actual->_reserved == 0 && expected->_reserved == 0);
    CHECK(actual->multiplicity == expected->multiplicity);
}

static wl_columnar_memory_governor_ref_t *
make_governor(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *result =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(result);
    return result;
}

/* No output becomes externally visible before all selected physical rows and
 * their source leases have completed. A failed row may leave a private prefix
 * whose timestamp tail is incomplete; it is discarded as a whole. */
static int
selected_transport(col_rel_t *input,
    wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control, col_rel_t **published)
{
    const uint32_t selected[] = {1, 2, 3, 4, 0};
    wl_columnar_relation_scalar_source_t source = {0};
    wl_columnar_relation_append_batch_t batch = {0};
    bool initialized = false;
    *published = NULL; draft = NULL; capturing_source = &source;
    int rc = wl_columnar_relation_scalar_source_init_attempt_controlled(&source,
            &initialized, control,
            wl_evaluation_control_active_attempt_id(control));
    if (!rc) rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &source, input, true);
    if (!rc && fault == 5)
        wl_columnar_relation_test_constructor_fail_allocation = 1;
    if (!rc && fault == 6) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(governor);
        atomic_store_explicit(&g->usable_bytes, wl_columnar_memory_reserved(g),
            memory_order_relaxed);
    }
    if (!rc) rc =
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
            (
            &source, "physical selection", governor, &draft);
    if (!rc) rc = wl_columnar_relation_append_batch_begin_controlled(&batch,
                draft, control);
    for (unsigned i = 0; !rc && i < 5; ++i) {
        int64_t values[17] = {0};
        CHECK(input->ncols <= 17);
        rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
            &source, selected[i], values, input->ncols);
        if (!rc) rc =
                wl_columnar_relation_append_batch_selected_source_row_controlled
                (
                &batch, &source, selected[i], values, input->ncols);
    }
    if (batch.phase == WL_COLUMNAR_RELATION_BATCH_HELD) {
        int end = wl_columnar_relation_append_batch_end_controlled(&batch, rc);
        CHECK(batch.phase == WL_COLUMNAR_RELATION_BATCH_ENDED);
        if (!rc) rc = end;
    }
    if (initialized) {
        int end = wl_columnar_relation_scalar_source_end(&source, rc);
        CHECK(source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED);
        if (!rc) rc = end;
    }
    if (rc) {
        if (draft) CHECK(!col_rel_destroy_checked(draft));
    } else *published = draft;
    draft = NULL; capturing_source = NULL;
    return rc;
}

static void
test_selected_physical_transport(void)
{
    const uint32_t widths[] = {0, 3, 17};
    const int64_t weights[] = {0, 1, -1, 3, -4};
    const uint32_t selected[] = {1, 2, 3, 4, 0};
    for (unsigned wi = 0; wi < 3; ++wi)
        for (unsigned alias = 0; alias < 2; ++alias)
            for (unsigned scenario = 0; scenario < 7; ++scenario) {
                uint32_t width = widths[wi];
                wl_columnar_memory_governor_ref_t *governor = make_governor();
                wl_columnar_memory_governor_t *g =
                    wl_columnar_memory_governor_ref_get(governor);
                col_rel_t *root = NULL;
                CHECK(!wl_columnar_relation_new_auto_governed("physical input",
                    width, 5, true, governor, &root));
                root->nrows = 5;
                for (uint32_t r = 0; r < 5; ++r) {
                    for (uint32_t c = 0; c < width; ++c)
                        root->columns[c][r] = (int64_t)(r % 2) + 7 * c;
                    root->timestamps[r] = (col_delta_timestamp_t){
                        .iteration = 101 + r, .stratum = 2 * r,
                        .worker = r % 3, ._reserved = 0,
                        .multiplicity = weights[r]
                    };
                }
                col_rel_t *input = root;
                col_delta_timestamp_t root_stamps[5];
                memcpy(root_stamps, root->timestamps, sizeof(root_stamps));
                if (alias) {
                    input = col_rel_new_like("physical alias", root);
                    CHECK(input && !col_rel_install_shared_view(input, root));
                    CHECK(input->timestamps &&
                        input->timestamps != root->timestamps);
                    for (unsigned r = 0; r < 5; ++r)
                        input->timestamps[r].iteration += 1000;
                }
                col_delta_timestamp_t expected[5];
                memcpy(expected, input->timestamps, sizeof(expected));
                col_relation_snapshot_t before =
                    wl_columnar_relation_snapshot(input);
                uint64_t baseline = wl_columnar_memory_reserved(g);
                wl_evaluation_control_t *control = NULL;
                CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                    &control));
                CHECK(!wl_evaluation_control_attach(control, &owner));
                for (unsigned retry = 0; retry < (scenario ? 2u : 1u);
                    ++retry) {
                    CHECK(!wl_evaluation_control_begin(control, &owner));
                    fault = retry ? 0 : scenario; hit = false;
                    wl_columnar_relation_test_work_checkpoint =
                        physical_checkpoint;
                    col_rel_t *out = NULL;
                    int rc = selected_transport(input, governor, control, &out);
                    wl_columnar_relation_test_work_checkpoint = NULL;
                    wl_columnar_relation_test_constructor_fail_allocation = 0;
                    int wanted = !fault ? 0 : fault == 1 || fault == 3
                        ? WL_EVALUATION_CONTROL_CANCELLED : fault == 2 ||
                        fault == 4
                        ? WL_EVALUATION_CONTROL_WORK_LIMIT : fault ==
                        5 ? ENOMEM : ENOSPC;
                    CHECK(rc == wanted && (!fault || fault > 4 || hit));
                    CHECK(wl_columnar_relation_snapshot_equal(before,
                        wl_columnar_relation_snapshot(input)));
                    for (unsigned r = 0; r < 5; ++r) {
                        equal_stamp(&input->timestamps[r], &expected[r]);
                        equal_stamp(&root->timestamps[r], &root_stamps[r]);
                    }
                    if (rc) CHECK(!out);
                    else {
                        CHECK(out && out->timestamps && out->nrows == 5);
                        for (unsigned r = 0; r < 5; ++r) {
                            equal_stamp(&out->timestamps[r],
                                &expected[selected[r]]);
                            for (unsigned c = 0; c < width; ++c)
                                CHECK(out->columns[c][r]
                                    == (int64_t)(selected[r] % 2) + 7 * c);
                        }
                        CHECK(!col_rel_destroy_checked(out));
                        /* This separate value projection intentionally omits
                         * timestamps by its raw API contract. */
                        col_rel_t *projection = NULL;
                        CHECK(!wl_columnar_relation_copy_rows_controlled(
                                "value projection", input, 0, 5, governor,
                                &projection, control));
                        CHECK(projection && !projection->timestamps
                            && projection->nrows == 5);
                        CHECK(!col_rel_destroy_checked(projection));
                    }
                    CHECK(wl_evaluation_control_finish(control, &owner,
                        rc > 0 ? rc : 0, 0) == rc);
                    wl_evaluation_control_report_t report;
                    CHECK(!wl_evaluation_control_report(control, &report)
                        && report.result == rc && !report.cleanup_status);
                    CHECK(wl_columnar_memory_reserved(g) == baseline);
                    atomic_store_explicit(&g->usable_bytes,
                        WL_COLUMNAR_MEMORY_MIN_BUDGET, memory_order_relaxed);
                    CHECK(!wl_evaluation_control_reset(control));
                    CHECK(!wl_evaluation_control_configure(control,
                        UINT64_C(1000000000)));
                }
                CHECK(!wl_evaluation_control_detach(control, &owner));
                wl_evaluation_control_release(control);
                if (alias) CHECK(!col_rel_destroy_checked(input));
                CHECK(!col_rel_destroy_checked(root));
                CHECK(!wl_columnar_memory_reserved(g));
                wl_columnar_memory_governor_ref_release(governor);
            }
}

int
main(void)
{
    test_selected_physical_transport();
    puts("physical timestamp and signed multiplicity transport passed");
    return 0;
}
