/* Timestamp retirement owns physical storage and charged ledger bytes
 * independently. Deferred ledger accounting must not fabricate a refund. */
#include "../wirelog/columnar/internal.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
static int owner;
static void restart(wl_evaluation_control_t *c, uint64_t limit)
{
    if (wl_evaluation_control_active_attempt_id(c))
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
    CHECK(!wl_evaluation_control_reset(c));
    CHECK(!wl_evaluation_control_configure(c, limit));
    CHECK(!wl_evaluation_control_begin(c, &owner));
}
static uint64_t reserved(wl_columnar_memory_governor_ref_t *g)
{
    return g?
           wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)):
           0;
}
static void stamps_equal(const col_rel_t *r, const col_delta_timestamp_t *saved)
{
    for (unsigned i = 0; i < 3; ++i) {
        const col_delta_timestamp_t *a = &r->timestamps[i], *b = &saved[i];
        CHECK(a->iteration == b->iteration && a->stratum == b->stratum
            && a->worker == b->worker && a->_reserved == 0 &&
            a->multiplicity == b->multiplicity);
    }
}
static uint64_t ledger_bytes(wl_mem_ledger_t *l)
{
    wl_mem_ledger_snapshot_t s; wl_mem_ledger_snapshot(l, &s);
    return s.current_bytes;
}

static void retirement(uint32_t width, bool governed, unsigned ledger_mode)
{
    wl_columnar_memory_resolution_t resolution =
    {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .usable_bytes =
         WL_COLUMNAR_MEMORY_MIN_BUDGET,
     .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    wl_columnar_memory_governor_ref_t *g =
        governed?wl_columnar_memory_governor_ref_create(&resolution):NULL;
    CHECK(!governed || g);
    col_rel_t *r = NULL;
    if (g)CHECK(!wl_columnar_relation_new_auto_governed("physical retire",
            width,
            4, false, g, &r));
    else r = col_rel_new_auto("physical retire", width);
    CHECK(r);
    int64_t values[257] = {0};
    for (unsigned row = 0; row < 3;
        ++row) CHECK(!col_rel_append_row(r, values));
    wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
    void *other_owner = malloc(37); CHECK(other_owner);
    wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
    /* For ungoverned mode2 the existing enable API creates the legal unbilled
     * timestamp image on an attached ledger. Governed mode2 attaches the new
     * ledger after physical creation, before its first reconciliation. */
    if (ledger_mode && !(governed && ledger_mode == 2)) {
        r->mem_ledger = &ledger;
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
            col_rel_owned_ledger_bytes(r));
    }
    CHECK(!col_rel_enable_timestamps(r));
    if (governed && ledger_mode == 2) {
        r->mem_ledger = &ledger;
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
            col_rel_owned_ledger_bytes(r));
    }
    uint64_t physical_ts = (uint64_t)r->timestamp_capacity*
        sizeof(*r->timestamps);
    if (ledger_mode == 1 && !r->ledger_ts_bytes) {
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, physical_ts);
        r->ledger_ts_bytes = physical_ts;
    }
    CHECK(ledger_mode != 2 || !r->ledger_ts_bytes);
    col_delta_timestamp_t saved[3]; const int64_t weights[] = {0, 3, -4};
    for (unsigned i = 0; i < 3;
        ++i) r->timestamps[i] = saved[i] = (col_delta_timestamp_t){
            .iteration = 91-i, .stratum = i+2, .worker = i%2, ._reserved = 0,
            .multiplicity = weights[i]
        };
    col_delta_timestamp_t *allocation = r->timestamps;
    uint32_t capacity = r->timestamp_capacity;
    col_relation_snapshot_t before = wl_columnar_relation_snapshot(r);
    uint64_t retained = reserved(g), charged = ledger_bytes(&ledger),
        ts_charge = r->ledger_ts_bytes;
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(0, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, 1);
    wl_columnar_relation_scalar_retirement_t receipt = {0};
    wl_columnar_source_access_reader_t pin = {0};
    CHECK(!col_rel_source_reader_acquire(r, &pin));
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) == EAGAIN);
    CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
    CHECK(!col_rel_source_reader_release(&pin));
    restart(c, 0);
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) == EAGAIN);
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) == EAGAIN);
    CHECK(receipt.phase == 2); /* actual timestamp shape/charge captured */
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)) ==
        WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
    CHECK(r->timestamps == allocation && r->timestamp_capacity == capacity
        && r->ledger_ts_bytes == ts_charge && reserved(g) == retained &&
        ledger_bytes(&ledger) == charged);
    CHECK(wl_columnar_relation_snapshot_equal(before,
        wl_columnar_relation_snapshot(r))); stamps_equal(r, saved);
    restart(c, 0);
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    int rc; unsigned turns = 0;
    do{
        rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(++turns < 100000);
    }while (rc == EAGAIN);
    CHECK(!rc && receipt.ready);
    CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
    stamps_equal(r, saved);
    CHECK(reserved(g) == retained && ledger_bytes(&ledger) == charged);
    CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
    do{
        rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                wl_evaluation_control_active_attempt_id(c));
    }while (rc == EAGAIN);
    CHECK(!rc && !wl_columnar_relation_scalar_retirement_commit(&receipt));
    CHECK(wl_columnar_relation_scalar_retirement_cancel(&receipt) == EINVAL);
    bool freed_ts = false, stopped_after_free = false; turns = 0;
    do{
        bool timestamp_stage = receipt.phase == 14;
        restart(c, (receipt.storage.relation && receipt.phase != 12)
            || turns%2 ? 1 : 4096);
        rc = wl_columnar_relation_scalar_retirement_step(&receipt, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(rc == 0 || rc == EAGAIN ||
            rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
        if (timestamp_stage) {
            CHECK(receipt.storage.relation && !r->timestamps &&
                !r->timestamp_capacity && receipt.phase == 12);
            CHECK(reserved(g) == retained && ledger_bytes(&ledger) == charged);
            freed_ts = true; restart(c, 0);
            wl_evaluation_control_request_cancel(c);
            CHECK(wl_columnar_relation_scalar_retirement_step(&receipt, c,
                wl_evaluation_control_active_attempt_id(c)) ==
                WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(receipt.phase == 12 &&
                receipt.ledger_timestamp_bytes == ts_charge
                && r->ledger_ts_bytes == ts_charge &&
                ledger_bytes(&ledger) == charged && reserved(g) == retained);
            stopped_after_free = true;
        }
        CHECK(++turns < 10000);
    }while (rc);
    CHECK(freed_ts && stopped_after_free && !reserved(g) &&
        ledger_bytes(&ledger) == 37);
    wl_mem_ledger_snapshot_t final; wl_mem_ledger_snapshot(&ledger, &final);
    CHECK(!final.subsys_bytes[WL_MEM_SUBSYS_RELATION]
        && final.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 37);
    free(other_owner); wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
    CHECK(!ledger_bytes(&ledger));
    CHECK(!wl_columnar_relation_scalar_retirement_step(&receipt, c,
        wl_evaluation_control_active_attempt_id(c)));
    (void)wl_evaluation_control_finish(c, &owner, 0, 0);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
    if (g)wl_columnar_memory_governor_ref_release(g);
}

/* An accepted raw set projection may free stamps before ledger reconcile. */
static void deferred_projection(void)
{
    for (unsigned governed = 0; governed < 2; ++governed) {
        wl_columnar_memory_resolution_t resolution =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes =
             WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        wl_columnar_memory_governor_ref_t *g =
            governed?wl_columnar_memory_governor_ref_create(&resolution):NULL;
        CHECK(!governed || g); col_rel_t *r = NULL;
        if (g)CHECK(!wl_columnar_relation_new_auto_governed(
                    "deferred projection", 1, 4, true, g, &r));
        else {
            r = col_rel_new_auto("deferred projection", 1);
            CHECK(r && !col_rel_enable_timestamps(r));
        }
        int64_t rows[] = {1, 2};
        CHECK(!col_rel_append_rows_atomic(r, rows, 2, 1, NULL));
        wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
        r->mem_ledger = &ledger;
        col_rel_ledger_reconcile(r, 0); CHECK(r->ledger_ts_bytes);
        void *other = malloc(37); CHECK(other);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
        int fast = 0;
        CHECK(!col_op_consolidate_incremental_delta(r, 0, NULL, &fast));
        CHECK(!r->timestamps && !r->timestamp_capacity);
        if (!governed)CHECK(r->ledger_ts_bytes > 0);
        uint64_t outstanding = r->ledger_ts_bytes;
        wl_evaluation_control_t *c = NULL;
        CHECK(!wl_evaluation_control_create(0, &c));
        CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, 0);
        wl_columnar_relation_scalar_retirement_t receipt = {0};
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
        int rc; do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                    wl_evaluation_control_active_attempt_id(c));
        }while (rc == EAGAIN);
        CHECK(!rc && receipt.ledger_timestamp_bytes == outstanding);
        CHECK(!wl_columnar_relation_scalar_retirement_commit(&receipt));
        unsigned turns = 0;
        do{
            rc = wl_columnar_relation_scalar_retirement_step(&receipt, c,
                    wl_evaluation_control_active_attempt_id(c));
            CHECK(++turns < 10000);
        }while (rc == EAGAIN);
        CHECK(!rc && !reserved(g) && ledger_bytes(&ledger) == 37);
        wl_mem_ledger_snapshot_t final; wl_mem_ledger_snapshot(&ledger, &final);
        CHECK(!final.subsys_bytes[WL_MEM_SUBSYS_RELATION] &&
            final.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP] == 37);
        free(other); wl_mem_ledger_free(&ledger, WL_MEM_SUBSYS_TIMESTAMP, 37);
        CHECK(!ledger_bytes(&ledger));
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
        CHECK(!wl_evaluation_control_detach(c, &owner));
        wl_evaluation_control_release(c);
        if (g) wl_columnar_memory_governor_ref_release(g);
    }
}
static void malformed(void)
{
    for (unsigned shape = 0; shape < 8; ++shape) {
        wl_columnar_memory_resolution_t resolution =
        {.budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .usable_bytes =
             WL_COLUMNAR_MEMORY_MIN_BUDGET,
         .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING};
        wl_columnar_memory_governor_ref_t *g =
            wl_columnar_memory_governor_ref_create(&resolution); CHECK(g);
        col_rel_t *r = NULL;
        CHECK(!wl_columnar_relation_new_auto_governed("bad TS", 1, 4, true, g,
            &r));
        r->nrows = 3; col_delta_timestamp_t *allocation = r->timestamps;
        uint32_t capacity = r->timestamp_capacity;
        uint64_t bytes = r->retained_reserved_bytes;
        uint64_t reserved_before = reserved(g);
        if (shape == 0)r->timestamp_capacity = 0;
        if (shape == 1)r->timestamp_capacity = 2;
        if (shape == 2)r->timestamps = NULL;
        if (shape == 3)r->timestamps = (col_delta_timestamp_t *)r->columns[0];
        if (shape ==
            4) r->timestamps =
                (col_delta_timestamp_t *)r->schema.children[0]->format;
        if (shape == 5)r->timestamps = (col_delta_timestamp_t *)r;
        if (shape == 6) {
            --r->retained_reserved_bytes; --r->retained_reservation.bytes;
        }
        if (shape ==
            7) r->ledger_ts_bytes = (uint64_t)capacity*sizeof(*allocation)+1;
        wl_evaluation_control_t *c = NULL;
        CHECK(!wl_evaluation_control_create(0, &c));
        CHECK(!wl_evaluation_control_attach(c, &owner)); restart(c, 0);
        wl_columnar_relation_scalar_retirement_t receipt = {0};
        CHECK(!wl_columnar_relation_scalar_retirement_begin(&receipt, r));
        int rc; do {
            rc = wl_columnar_relation_scalar_retirement_prepare(&receipt, c,
                    wl_evaluation_control_active_attempt_id(c));
        }while (rc == EAGAIN);
        CHECK(rc == EINVAL && !receipt.ready && !receipt.committed &&
            reserved(g) == reserved_before);
        CHECK(!wl_columnar_relation_scalar_retirement_cancel(&receipt));
        r->timestamps = allocation; r->timestamp_capacity = capacity;
        r->retained_reserved_bytes = bytes;
        r->retained_reservation.bytes = bytes; r->ledger_ts_bytes = 0;
        CHECK(!col_rel_destroy_checked(r) && !reserved(g));
        (void)wl_evaluation_control_finish(c, &owner, 0, 0);
        CHECK(!wl_evaluation_control_detach(c, &owner));
        wl_evaluation_control_release(c);
        wl_columnar_memory_governor_ref_release(g);
    }
}
int main(void)
{
    const uint32_t widths[] = {0, 1, 3, 257};
    for (unsigned w = 0; w < 4; ++w)for (unsigned governed = 0; governed < 2;
            ++governed)
            for (unsigned ledger = 0; ledger < 3;
                ++ledger) retirement(widths[w], governed, ledger);
    deferred_projection(); malformed();
    puts(
        "physical retirement: reversible ownership, timestamp disposal and exact ledger credit passed");
    return 0;
}
