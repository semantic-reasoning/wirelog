/* Controlled typed/stamped scratch prerequisite for session JOIN batches. */
#include "../wirelog/columnar/internal.h"
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

int
main(void)
{
    test_domains();
    test_failures();
    return 0;
}
