#include "columnar/trace_store.h"
#include "columnar/internal.h"
#include "test_tmpdir.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
#define OK WL_COLUMNAR_SPILL_OK
#define RESIDENT WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT
#define SPILLED WL_COLUMNAR_BATCH_PAYLOAD_SPILLED
extern void wl_columnar_trace_store_test_fail_allocation(unsigned);
extern void wl_columnar_trace_store_test_state(wl_columnar_trace_store_t *,
    wl_columnar_trace_store_state_t);
extern void wl_columnar_batch_payload_test_fail_allocation(unsigned);
static unsigned spill_alloc, read_fault, write_fault;
static int write_error;
bool wl_columnar_spill_test_allocation(void) {
    return spill_alloc && --spill_alloc == 0;
}
int wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)offset;
    if (writing && write_fault) {
        if (*length > 3) *length = 3;
        return --write_fault == 0 ? write_error : 0;
    }
    return !writing && read_fault && --read_fault == 0 ? EIO : 0;
}
static const wirelog_column_type_t schema[] = {WIRELOG_TYPE_INT64,
                                               WIRELOG_TYPE_FLOAT,
                                               WIRELOG_TYPE_BOOL};
static const int64_t weights[] = {INT64_MIN, -2, 0, 1, INT64_MAX};
static wl_columnar_memory_governor_ref_t *governor(void)
{
    wl_columnar_memory_resolution_t r = {.budget_bytes = 16*1024*1024,
                                         .usable_bytes = 16*1024*1024,
                                         .mode =
                                             WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                                         .source =
                                             WL_COLUMNAR_MEMORY_SOURCE_ENV,
                                         .status = WL_COLUMNAR_MEMORY_OK};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g); return g;
}
static uint64_t reserved(wl_columnar_memory_governor_ref_t *g)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}
static col_rel_t *source(wl_columnar_memory_governor_ref_t *g, uint32_t start,
    uint32_t rows)
{
    col_rel_t *r = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("trace_fixture", 3,
        rows ? rows : 1, true, g, &r) == 0);
    CHECK(col_rel_set_column_types(r, schema, 3) == 0);
    for (uint32_t i = 0; i < rows; i++) {
        int64_t value = start + i;
        double f = -(double)value / 2;
        int64_t lanes[] = {value, 0, value%2};
        memcpy(&lanes[1], &f, 8);
        CHECK(col_rel_append_row(r, lanes) == 0);
        r->columns[1][i] = lanes[1]; /* Preserve signed zero bitwise. */
        r->timestamps[i] = (col_delta_timestamp_t){0, 0, 0, 0,
                                                   weights[(start+i)%5]};
    }
    return r;
}
static wl_columnar_spill_store_t *store(wl_columnar_memory_governor_ref_t *g)
{
    wl_columnar_spill_store_t *s = NULL;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 64*1024*1024, 4096,
        &s) == OK);
    return s;
}
static wl_columnar_trace_store_t *trace(wl_columnar_memory_governor_ref_t *g)
{
    wl_columnar_trace_store_t *t = NULL;
    CHECK(wl_columnar_trace_store_create(g, schema, 3, &t) == OK); return t;
}
static void same_state(wl_columnar_trace_store_t *t,
    wl_columnar_trace_store_state_t expected)
{
    wl_columnar_trace_store_state_t actual = wl_columnar_trace_store_state(t);
    CHECK(actual.generation == expected.generation &&
        actual.rows == expected.rows && actual.segments == expected.segments);
}
static void row(wl_columnar_trace_store_cursor_t *c, uint32_t ordinal,
    uint32_t value)
{
    const uint64_t *lanes = NULL;
    wl_columnar_spill_metadata_t m;
    CHECK(wl_columnar_trace_store_cursor_read(c, ordinal, NULL, NULL, &lanes,
        &m) == OK);
    double f = -(double)value/2; uint64_t bits; memcpy(&bits, &f, 8);
    CHECK(lanes && lanes[0] == value && lanes[1] == bits &&
        lanes[2] == value%2);
    CHECK(!m.iteration && !m.stratum && !m.worker &&
        m.multiplicity == weights[value%5]);
}
static bool cancel(void *context)
{
    unsigned *remaining = context;
    return !*remaining || --*remaining == 0;
}
static void numeric_types(void)
{
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT32,
                                           WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_UINT32,
                                           WIRELOG_TYPE_UINT64,
                                           WIRELOG_TYPE_FLOAT,
                                           WIRELOG_TYPE_BOOL};
    const uint64_t expected[] = {UINT64_C(0xffffffff80000000),
                                 UINT64_C(0x8000000000000000),
                                 UINT32_MAX, UINT64_MAX,
                                 UINT64_C(0x8000000000000000), 1};
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_spill_store_t *store_handle = store(g);
    for (unsigned storage = 0; storage < 2; storage++) {
        wl_columnar_trace_store_t *t;
        CHECK(wl_columnar_trace_store_create(g, types, 6, &t) == OK);
        col_rel_t *r;
        CHECK(wl_columnar_relation_new_auto_governed("all_numeric", 6, 1, false,
            g, &r) == 0);
        CHECK(col_rel_set_column_types(r, types, 6) == 0);
        int64_t values[6]; memcpy(values, expected, sizeof(values));
        CHECK(col_rel_append_row(r, values) == 0);
        for (uint32_t i = 0; i < 6;
            i++) memcpy(&r->columns[i][0], &expected[i], 8);
        CHECK(wl_columnar_trace_store_append_capture(t, r,
            (wl_columnar_batch_payload_storage_t)storage, store_handle, NULL,
            NULL) == OK);
        CHECK(col_rel_destroy_checked(r) == 0);
        wl_columnar_trace_store_snapshot_t *snap;
        wl_columnar_trace_store_cursor_t *c;
        CHECK(wl_columnar_trace_store_snapshot_acquire(t, &snap) == OK);
        CHECK(wl_columnar_trace_store_cursor_acquire(snap, &c) == OK);
        const uint64_t *lanes; wl_columnar_spill_metadata_t m;
        CHECK(wl_columnar_trace_store_cursor_read(c, 0, NULL, NULL, &lanes,
            &m) == OK);
        CHECK(!memcmp(lanes, expected,
            sizeof(expected)) && m.multiplicity == 1 &&
            !m.iteration && !m.stratum && !m.worker);
        CHECK(!memcmp(wl_columnar_trace_store_snapshot_schema(snap, NULL, NULL),
            types, sizeof(types)));
        wl_columnar_trace_store_cursor_release(c);
        wl_columnar_trace_store_snapshot_release(snap);
        wl_columnar_trace_store_retire(t);
    }
    CHECK(wl_columnar_spill_store_close(store_handle) == OK &&
        reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void mixed_snapshots(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_spill_store_t *s = store(g);
    wl_columnar_trace_store_t *t = trace(g);
    uint64_t baseline = reserved(g);
    wl_columnar_trace_store_snapshot_t *empty, *old, *newer;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &empty) == OK);
    col_rel_t *r = source(g, 0, 5);
    CHECK(wl_columnar_trace_store_append_capture(t, r, RESIDENT, NULL, NULL,
        NULL) == OK);
    CHECK(col_rel_destroy_checked(r) == 0);
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &old) == OK);
    r = source(g, 5, 0);
    CHECK(wl_columnar_trace_store_append_capture(t, r, SPILLED, s, NULL,
        NULL) == OK);
    CHECK(col_rel_destroy_checked(r) == 0);
    r = source(g, 5, 7);
    uint64_t before = reserved(g);
    CHECK(wl_columnar_trace_store_append_take(t, &r, s, NULL,
        NULL) == OK && !r);
    CHECK(reserved(g) < before); /* Actual source storage retired. */
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &newer) == OK);
    same_state(t, (wl_columnar_trace_store_state_t){3, 12, 3});
    CHECK(wl_columnar_trace_store_snapshot_state(old).rows == 5);
    uint32_t cols, rows;
    const wirelog_column_type_t *types =
        wl_columnar_trace_store_snapshot_schema(newer, &cols, &rows);
    CHECK(cols == 3 && rows == 12 && !memcmp(types, schema, sizeof(schema)));
    wl_columnar_trace_store_cursor_t *a, *b, *e;
    CHECK(wl_columnar_trace_store_cursor_acquire(old, &a) == OK);
    CHECK(wl_columnar_trace_store_cursor_acquire(newer, &b) == OK);
    CHECK(wl_columnar_trace_store_cursor_acquire(empty, &e) == OK);
    wl_columnar_trace_store_snapshot_release(old);
    wl_columnar_trace_store_snapshot_release(newer);
    wl_columnar_trace_store_snapshot_release(empty);
    CHECK(wl_columnar_trace_store_demote(t, 0, s, NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY);
    const uint64_t *lanes = (const uint64_t *)1;
    wl_columnar_spill_metadata_t m = {1, 1, 1, 1};
    CHECK(wl_columnar_trace_store_cursor_read(a, 5, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_INVALID && !lanes && !m.multiplicity);
    CHECK(wl_columnar_trace_store_cursor_read(e, 0, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_INVALID);
    wl_columnar_trace_store_retire(t); /* Store remains open: later lazy reads work. */
    for (uint32_t i = 0; i < 5; i++) row(a, i, i);
    for (uint32_t i = 12; i-- > 0;) row(b, i, i);
    wl_columnar_trace_store_cursor_release(a);
    wl_columnar_trace_store_cursor_release(b);
    wl_columnar_trace_store_cursor_release(e);
    CHECK(wl_columnar_spill_store_bytes(s) == 0 && reserved(g) < baseline);
    CHECK(wl_columnar_spill_store_close(s) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void demotion(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_spill_store_t *s = store(g);
    wl_columnar_trace_store_t *t = trace(g);
    col_rel_t *r = source(g, 0, 2048);
    CHECK(wl_columnar_trace_store_append_capture(t, r, RESIDENT, NULL, NULL,
        NULL) == OK);
    CHECK(col_rel_destroy_checked(r) == 0);
    wl_columnar_trace_store_snapshot_t *snap;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &snap) == OK);
    CHECK(wl_columnar_trace_store_demote(t, 0, s, NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY);
    wl_columnar_trace_store_snapshot_release(snap);
    uint64_t before = reserved(g);
    write_fault = 4; write_error = ENOSPC;
    CHECK(wl_columnar_trace_store_demote(t, 0, s, NULL,
        NULL) == WL_COLUMNAR_SPILL_CAPACITY);
    CHECK(reserved(g) == before && wl_columnar_spill_store_bytes(s) == 0);
    CHECK(wl_columnar_trace_store_demote(t, 0, s, NULL, NULL) == OK);
    CHECK(reserved(g) + 2048*24 < before &&
        wl_columnar_spill_store_bytes(s) > 2048*24);
    same_state(t, (wl_columnar_trace_store_state_t){1, 2048, 1});
    CHECK(wl_columnar_trace_store_demote(t, 0, s, NULL,
        NULL) == WL_COLUMNAR_SPILL_INVALID);
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &snap) == OK);
    wl_columnar_trace_store_cursor_t *c;
    CHECK(wl_columnar_trace_store_cursor_acquire(snap, &c) == OK);
    row(c, 0, 0); row(c, 2047, 2047); row(c, 1024, 1024);
    wl_columnar_trace_store_cursor_release(c);
    wl_columnar_trace_store_snapshot_release(snap);
    wl_columnar_trace_store_retire(t);
    CHECK(wl_columnar_spill_store_close(s) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void store_retirement(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_spill_store_t *s = store(g);
    wl_columnar_trace_store_t *t = trace(g);
    for (uint32_t i = 0; i < 2; i++) {
        col_rel_t *r = source(g, i, 1);
        CHECK(wl_columnar_trace_store_append_take(t, &r, s, NULL,
            NULL) == OK && !r);
    }
    wl_columnar_trace_store_snapshot_t *snap;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &snap) == OK);
    wl_columnar_trace_store_cursor_t *a, *b;
    CHECK(wl_columnar_trace_store_cursor_acquire(snap, &a) == OK);
    CHECK(wl_columnar_trace_store_cursor_acquire(snap, &b) == OK);
    wl_columnar_trace_store_snapshot_release(snap);
    row(a, 0, 0);
    wl_columnar_trace_store_retire(t);
    wl_columnar_spill_store_retire(s);
    row(a, 0, 0); /* Previously acquired reader survives. */
    const uint64_t *lanes; wl_columnar_spill_metadata_t m;
    CHECK(wl_columnar_trace_store_cursor_read(b, 0, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_CLOSED && !lanes && !m.multiplicity);
    CHECK(wl_columnar_trace_store_cursor_read(a, 1, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_CLOSED && !lanes && !m.multiplicity);
    wl_columnar_trace_store_cursor_release(a);
    wl_columnar_trace_store_cursor_release(b);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}
static void failures(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(), *other = governor();
    wl_columnar_spill_store_t *s = store(g), *tiny;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 16, 4096,
        &tiny) == OK);
    wl_columnar_trace_store_t *t = trace(g), *failed = (void *)1;
    uint64_t before = reserved(g);
    wl_columnar_trace_store_test_fail_allocation(1);
    CHECK(wl_columnar_trace_store_create(g, schema, 3,
        &failed) == WL_COLUMNAR_SPILL_ALLOCATION && !failed);
    wirelog_column_type_t bad[] = {WIRELOG_TYPE_STRING};
    CHECK(wl_columnar_trace_store_create(g, bad, 1,
        &failed) == WL_COLUMNAR_SPILL_UNSUPPORTED && !failed);
    CHECK(reserved(g) == before);
    col_rel_t *r = source(g, 0, 8), *saved = r;
    before = reserved(g);
    for (unsigned kind = 0; kind < 13; kind++) {
        unsigned remaining = 0;
        wl_columnar_trace_store_state_t original =
            wl_columnar_trace_store_state(t);
        if (kind == 0) wl_columnar_trace_store_test_fail_allocation(1);
        if (kind == 1) wl_columnar_batch_payload_test_fail_allocation(1);
        if (kind == 2) spill_alloc = 1;
        if (kind == 3 || kind == 4) {
            write_fault = 4; write_error = kind == 3 ? EIO : ENOSPC;
        }
        if (kind == 5) r->timestamps[7].iteration = 1;
        if (kind == 6) r->timestamps[7]._reserved = 1;
        if (kind == 7) r->column_types[1] = WIRELOG_TYPE_STRING;
        if (kind == 8) r->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        if (kind == 9) r->has_graph_column = true;
        wl_columnar_trace_store_state_t state = original;
        if (kind == 10) state.generation = UINT64_MAX;
        if (kind == 11) state.rows = UINT32_MAX;
        if (kind == 12) state.segments = UINT32_MAX;
        if (kind >= 10) wl_columnar_trace_store_test_state(t, state);
        wl_columnar_spill_status_t rc = wl_columnar_trace_store_append_take(t,
                &r, s, NULL, NULL);
        CHECK(rc != OK && r == saved && reserved(g) == before &&
            wl_columnar_spill_store_bytes(s) == 0);
        same_state(t, state);
        wl_columnar_trace_store_test_state(t, original);
        r->timestamps[7].iteration = r->timestamps[7]._reserved = 0;
        r->column_types[1] = WIRELOG_TYPE_FLOAT;
        r->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
        r->has_graph_column = false;
        CHECK(wl_columnar_trace_store_append_take(t, &r, s, cancel,
            &remaining) == WL_COLUMNAR_SPILL_CANCELLED && r == saved);
    }
    CHECK(wl_columnar_trace_store_append_take(t, &r, tiny, NULL,
        NULL) == WL_COLUMNAR_SPILL_QUOTA && r == saved);
    col_rel_t *wrong = source(other, 0, 1);
    CHECK(wl_columnar_trace_store_append_capture(t, wrong, RESIDENT, NULL, NULL,
        NULL) == WL_COLUMNAR_SPILL_INVALID);
    CHECK(col_rel_destroy_checked(wrong) == 0);
    /* A live source reader prevents terminal retirement after a complete write. */
    wl_columnar_source_access_reader_t lease = {0};
    CHECK(col_rel_source_reader_acquire(r, &lease) == 0);
    CHECK(wl_columnar_trace_store_append_take(t, &r, s, NULL,
        NULL) == WL_COLUMNAR_SPILL_BUSY && r == saved);
    CHECK(col_rel_source_reader_release(&lease) == 0);
    CHECK(reserved(g) == before && wl_columnar_spill_store_bytes(s) == 0);
    CHECK(wl_columnar_trace_store_append_take(t, &r, s, NULL,
        NULL) == OK && !r);
    before = reserved(g);
    wl_columnar_trace_store_snapshot_t *snap = (void *)1;
    wl_columnar_trace_store_test_fail_allocation(1);
    CHECK(wl_columnar_trace_store_snapshot_acquire(t,
        &snap) == WL_COLUMNAR_SPILL_ALLOCATION && !snap &&
        reserved(g) == before);
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &snap) == OK);
    before = reserved(g);
    wl_columnar_trace_store_cursor_t *c = (void *)1;
    wl_columnar_trace_store_test_fail_allocation(1);
    CHECK(wl_columnar_trace_store_cursor_acquire(snap,
        &c) == WL_COLUMNAR_SPILL_ALLOCATION && !c && reserved(g) == before);
    CHECK(wl_columnar_trace_store_cursor_acquire(snap, &c) == OK);
    before = reserved(g);
    const uint64_t *lanes; wl_columnar_spill_metadata_t m;
    wl_columnar_batch_payload_test_fail_allocation(1);
    CHECK(wl_columnar_trace_store_cursor_read(c, 0, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_ALLOCATION && !lanes && reserved(g) == before);
    row(c, 0, 0); before = reserved(g);
    read_fault = 3;
    CHECK(wl_columnar_trace_store_cursor_read(c, 7, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_IO && !lanes && !m.multiplicity);
    CHECK(reserved(g) == before); row(c, 7, 7);
    unsigned remaining = 0;
    CHECK(wl_columnar_trace_store_cursor_read(c, 0, cancel, &remaining, &lanes,
        &m) == WL_COLUMNAR_SPILL_CANCELLED && !lanes);
    wl_columnar_trace_store_cursor_release(c);
    wl_columnar_trace_store_snapshot_release(snap);
    wl_columnar_trace_store_retire(t);
    CHECK(wl_columnar_spill_store_close(tiny) == OK);
    CHECK(wl_columnar_spill_store_close(s) == OK && reserved(g) == 0 &&
        reserved(other) == 0);
    wl_columnar_memory_governor_ref_release(g);
    wl_columnar_memory_governor_ref_release(other);
}
static void admission_and_capture_rollback(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_spill_store_t *store_handle = store(g);
    wl_columnar_trace_store_t *t = trace(g);
    col_rel_t *r = source(g, 0, 64);
    CHECK(wl_columnar_trace_store_append_capture(t, r, RESIDENT, NULL, NULL,
        NULL) == OK);
    wl_columnar_trace_store_snapshot_t *snap;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &snap) == OK);
    wl_columnar_trace_store_cursor_t *old;
    CHECK(wl_columnar_trace_store_cursor_acquire(snap, &old) == OK);
    row(old, 63, 63);
    wl_columnar_trace_store_state_t state = wl_columnar_trace_store_state(t);
    uint64_t before = reserved(g);
    unsigned checkpoints[] = {1, 65, 66, 130};
    for (unsigned i = 0; i < sizeof(checkpoints)/sizeof(checkpoints[0]); i++) {
        unsigned remaining = checkpoints[i];
        CHECK(wl_columnar_trace_store_append_capture(t, r, RESIDENT, NULL,
            cancel, &remaining) == WL_COLUMNAR_SPILL_CANCELLED);
        CHECK(reserved(g) == before); same_state(t, state); row(old, 0, 0);
    }
    /* Real managed admission denial with a live earlier generation. */
    wl_columnar_memory_governor_t *ledger =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t limit = atomic_load_explicit(&ledger->usable_bytes,
            memory_order_acquire);
    atomic_store_explicit(&ledger->usable_bytes, before, memory_order_release);
    CHECK(wl_columnar_trace_store_append_capture(t, r, SPILLED, store_handle,
        NULL, NULL) == WL_COLUMNAR_SPILL_MEMORY);
    col_rel_t *saved = r;
    CHECK(wl_columnar_trace_store_append_take(t, &r, store_handle,
        NULL, NULL) == WL_COLUMNAR_SPILL_MEMORY && r == saved);
    wl_columnar_trace_store_snapshot_t *failed = (void *)1;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t,
        &failed) == WL_COLUMNAR_SPILL_MEMORY && !failed);
    wl_columnar_trace_store_cursor_t *failed_cursor = (void *)1;
    CHECK(wl_columnar_trace_store_cursor_acquire(snap,
        &failed_cursor) == WL_COLUMNAR_SPILL_MEMORY && !failed_cursor);
    CHECK(reserved(g) == before &&
        wl_columnar_spill_store_bytes(store_handle) == 0);
    same_state(t, state); row(old, 63, 63);
    atomic_store_explicit(&ledger->usable_bytes, limit, memory_order_release);
    CHECK(wl_columnar_trace_store_append_take(t, &r, store_handle, NULL,
        NULL) == OK && !r);
    row(old, 0, 0);
    wl_columnar_trace_store_cursor_release(old);
    wl_columnar_trace_store_snapshot_release(snap);
    wl_columnar_trace_store_retire(t);
    CHECK(wl_columnar_spill_store_close(store_handle) == OK &&
        reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void empty_directory(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_trace_store_t *t = trace(g);
    col_rel_t *empty = source(g, 0, 0), *r = source(g, 0, 1);
    for (unsigned i = 0; i < 1024; i++) {
        CHECK(wl_columnar_trace_store_append_capture(t, empty, RESIDENT, NULL,
            NULL, NULL) == OK);
        if (i == 511) CHECK(wl_columnar_trace_store_append_capture(t, r,
                RESIDENT, NULL, NULL, NULL) == OK);
    }
    CHECK(col_rel_destroy_checked(empty) == 0 &&
        col_rel_destroy_checked(r) == 0);
    uint64_t owner, nodes, snapshot, cursor;
    CHECK(wl_columnar_trace_store_floor(3, 1025, &owner, &nodes, &snapshot,
        &cursor) == OK);
    uint64_t before = reserved(g);
    wl_columnar_trace_store_snapshot_t *snap;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t,
        &snap) == OK && reserved(g) == before + snapshot);
    wl_columnar_trace_store_cursor_t *c;
    CHECK(wl_columnar_trace_store_cursor_acquire(snap,
        &c) == OK && reserved(g) == before + snapshot + cursor);
    row(c, 0, 0);
    const uint64_t *lanes; wl_columnar_spill_metadata_t m;
    CHECK(wl_columnar_trace_store_cursor_read(c, 1, NULL, NULL, &lanes,
        &m) == WL_COLUMNAR_SPILL_INVALID);
    wl_columnar_trace_store_cursor_release(c);
    wl_columnar_trace_store_snapshot_release(snap);
    wl_columnar_trace_store_retire(t); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
typedef struct {
    wl_columnar_trace_store_cursor_t *cursor;
    uint32_t rows;
} concurrent_job_t;
static void *concurrent_reader(void *context)
{
    concurrent_job_t *job = context;
    for (unsigned repeat = 0; repeat < 32; repeat++) {
        for (uint32_t i = 0; i < job->rows; i++) {
            row(job->cursor, i, i);
            row(job->cursor, job->rows - 1 - i, job->rows - 1 - i);
        }
    }
    return NULL;
}
static void concurrent_transitions(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_columnar_spill_store_t *store_handle = store(g);
    wl_columnar_trace_store_t *t = trace(g);
    wl_columnar_trace_store_snapshot_t *snapshots[2];
    for (uint32_t i = 0; i < 8; i++) {
        col_rel_t *empty = source(g, 0, 0), *r = source(g, i*4, 4);
        CHECK(wl_columnar_trace_store_append_capture(t, empty, RESIDENT, NULL,
            NULL, NULL) == OK);
        CHECK(col_rel_destroy_checked(empty) == 0);
        CHECK(wl_columnar_trace_store_append_capture(t, r,
            i%2 ? SPILLED : RESIDENT,
            store_handle, NULL, NULL) == OK);
        CHECK(col_rel_destroy_checked(r) == 0);
        if (i == 3 || i == 7) CHECK(wl_columnar_trace_store_snapshot_acquire(t,
                &snapshots[i/4]) == OK);
    }
    concurrent_job_t jobs[4]; wl_thread_t threads[4];
    for (unsigned i = 0; i < 4; i++) {
        jobs[i].rows = i%2 ? 32 : 16;
        CHECK(wl_columnar_trace_store_cursor_acquire(snapshots[i%2],
            &jobs[i].cursor) == OK);
    }
    wl_columnar_trace_store_snapshot_release(snapshots[0]);
    wl_columnar_trace_store_snapshot_release(snapshots[1]);
    wl_columnar_trace_store_retire(t);
    for (unsigned i = 0; i < 4;
        i++) CHECK(wl_thread_create(&threads[i], concurrent_reader,
            &jobs[i]) == 0);
    for (unsigned i = 0; i < 4; i++) wl_thread_join(&threads[i]);
    for (unsigned i = 0; i < 4;
        i++) wl_columnar_trace_store_cursor_release(jobs[i].cursor);
    CHECK(wl_columnar_spill_store_close(store_handle) == OK &&
        reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static wl_intern_t *reference_intern(wl_columnar_memory_governor_ref_t *g)
{
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    CHECK(wl_intern_attach_memory_governor(intern, g) == 0);
    CHECK(wl_intern_put(intern, "pair") == 0);
    CHECK(wl_intern_put(intern, "") == 1);
    CHECK(wl_intern_put(intern, "한글 λ") == 2);
    return intern;
}
static void reference_schema(unsigned kind, wirelog_column_type_t *types,
    wirelog_column_t *columns)
{
    types[0] = WIRELOG_TYPE_INT64;
    types[1] = kind == 2 ? WIRELOG_TYPE_INT64 : WIRELOG_TYPE_STRING;
    types[2] = WIRELOG_TYPE_INT64;
    columns[0] = (wirelog_column_t){.name = "key", .type = WIRELOG_TYPE_INT64};
    columns[1] = (wirelog_column_t){.name = "pair", .type = WIRELOG_TYPE_INT64,
                                    .compound_kind =
                                        WIRELOG_COMPOUND_KIND_INLINE,
                                    .compound_arity = 2,
                                    .compound_inline_col_offset = 1};
}
static col_rel_t *reference_source(wl_columnar_memory_governor_ref_t *g,
    unsigned kind, uint32_t first, uint32_t rows)
{
    wirelog_column_type_t types[3]; wirelog_column_t columns[2];
    reference_schema(kind, types, columns);
    col_rel_t *r;
    CHECK(wl_columnar_relation_new_auto_governed("reference_trace", 3,
        rows ? rows : 1, true, g, &r) == 0);
    CHECK(col_rel_set_column_types(r, types, 3) == 0);
    if (kind) {
        const col_rel_logical_col_t layout[] = {{WIRELOG_COMPOUND_KIND_NONE, 0,
                                                 0},
                                                {WIRELOG_COMPOUND_KIND_INLINE,
                                                 2, 1}};
        CHECK(col_rel_apply_compound_schema(r, layout, 2) == 0);
    }
    for (uint32_t i = 0; i < rows; i++) {
        r->columns[0][i] = first + i;
        r->columns[1][i] = 1 + (first+i)%2;
        r->columns[2][i] = -(int64_t)(first+i);
        r->timestamps[i] =
            (col_delta_timestamp_t){.multiplicity = (int64_t)((first+i)%3)-1};
    }
    r->nrows = rows; return r;
}
static void reference_row(wl_columnar_trace_store_cursor_t *cursor, uint32_t id,
    bool numeric)
{
    const uint64_t *lanes; wl_columnar_spill_metadata_t meta; const char *text;
    CHECK(wl_columnar_trace_store_cursor_read(cursor, id, NULL, NULL, &lanes,
        &meta) == OK);
    CHECK(lanes[0] == id && lanes[1] == 1+id%2 &&
        (int64_t)lanes[2] == -(int64_t)id);
    CHECK(meta.multiplicity == (int64_t)(id%3)-1 && !meta.iteration &&
        !meta.stratum && !meta.worker);
    if (!numeric) {
        CHECK(wl_columnar_trace_store_cursor_resolve_string(cursor, lanes[1],
            &text) == OK);
        CHECK(strcmp(text, id%2 ? "한글 λ" : "") == 0);
    }
    CHECK(wl_columnar_trace_store_cursor_resolve_string(cursor, 0,
        &text) == OK && !strcmp(text, "pair"));
}
typedef struct { wl_columnar_trace_store_cursor_t *cursor; bool numeric;
} reference_worker_t;
static void *reference_worker(void *context)
{
    reference_worker_t *w = context;
    for (unsigned pass = 0; pass < 32; pass++)
        for (uint32_t i = 0; i < 22;
            i++) reference_row(w->cursor, pass%2 ? 21-i : i, w->numeric);
    return NULL;
}
static void reference_storage(void)
{
    for (unsigned kind = 0; kind < 3; kind++) for (unsigned mode = 0; mode < 3;
            mode++){
            wl_columnar_memory_governor_ref_t *g = governor();
            wl_intern_t *intern = reference_intern(g);
            wl_columnar_spill_store_t *disk = store(g);
            wirelog_column_type_t types[3]; wirelog_column_t columns[2];
            reference_schema(kind, types, columns);
            wl_columnar_trace_store_t *t;
            uint64_t owner, nodes, snap, cursor, before = reserved(g);
            CHECK(wl_columnar_trace_store_floor_with_inline(3, 0, kind ? 2 : 0,
                &owner, &nodes, &snap, &cursor) == OK);
            CHECK(wl_columnar_trace_store_create_reference(g, types, 3,
                kind ? columns : NULL, kind ? 2 : 0, intern, &t) == OK);
            CHECK(reserved(g) == before + owner);
            col_rel_t *r = reference_source(g, kind, 0, 11);
            CHECK(wl_columnar_trace_store_append_capture_reference(t, r,
                kind ? columns : NULL, kind ? 2 : 0, intern,
                mode == 1 ? SPILLED : RESIDENT, disk, NULL, NULL) == OK);
            col_rel_destroy(r);
            if (mode == 2) CHECK(wl_columnar_trace_store_demote(t, 0, disk,
                    NULL, NULL) == OK);
            wl_columnar_trace_store_snapshot_t *old;
            CHECK(wl_columnar_trace_store_snapshot_acquire(t, &old) == OK);
            CHECK(wl_columnar_trace_store_snapshot_uses_intern(old, intern));
            uint32_t logical;
            const wirelog_column_t *copy =
                wl_columnar_trace_store_snapshot_inline_schema(old, &logical);
            CHECK(logical == (kind ? 2u : 0u));
            if (kind) {
                CHECK(copy &&
                    (uintptr_t)copy % _Alignof(wirelog_column_t) == 0);
                CHECK(!copy[0].name && !copy[1].name &&
                    copy[1].compound_arity == 2);
                columns[1].compound_functor_id = 2;
                CHECK(copy[1].compound_functor_id == 0);
                columns[1].compound_functor_id = 0;
            }
            r = reference_source(g, kind, 11, 11);
            CHECK(wl_columnar_trace_store_append_take_reference(t, &r,
                kind ? columns : NULL, kind ? 2 : 0, intern, disk, NULL,
                NULL) == OK && !r);
            CHECK(wl_columnar_trace_store_snapshot_state(old).rows == 11);
            CHECK(wl_columnar_trace_store_demote(t, 0, disk, NULL,
                NULL) == WL_COLUMNAR_SPILL_BUSY);
            wl_columnar_trace_store_snapshot_t *newer;
            CHECK(wl_columnar_trace_store_snapshot_acquire(t, &newer) == OK);
            reference_worker_t workers[2] = {{.numeric = kind == 2},
                                             {.numeric = kind == 2}};
            for (unsigned i = 0; i < 2;
                i++) CHECK(wl_columnar_trace_store_cursor_acquire(newer,
                    &workers[i].cursor) == OK);
            wl_columnar_trace_store_snapshot_release(old);
            wl_columnar_trace_store_snapshot_release(newer);
            wl_intern_free(intern); wl_columnar_trace_store_retire(t);
            /* First lazy acquisitions and alternating switches use retained pins. */
            wl_thread_t threads[2];
            for (unsigned i = 0; i < 2;
                i++) CHECK(wl_thread_create(&threads[i], reference_worker,
                    &workers[i]) == 0);
            for (unsigned i = 0; i < 2; i++) wl_thread_join(&threads[i]);
            for (unsigned i = 0; i < 2;
                i++) wl_columnar_trace_store_cursor_release(workers[i].cursor);
            wl_columnar_spill_store_retire(disk); CHECK(reserved(g) == 0);
            wl_columnar_memory_governor_ref_release(g);
        }
}
static void reference_empty_and_closed(void)
{
    /* Also bind a purely numeric scalar schema: the explicit reference owner
     * still holds an empty-owner pin and does not reopen retired admission. */
    for (unsigned kind = 0; kind < 4; kind++) {
        wl_columnar_memory_governor_ref_t *g = governor();
        wl_intern_t *intern = reference_intern(g);
        wirelog_column_type_t types[3]; wirelog_column_t columns[2];
        reference_schema(kind == 3 ? 2 : kind, types, columns);
        unsigned logical = kind == 1 || kind == 2 ? 2 : 0;
        wl_columnar_trace_store_t *t;
        CHECK(wl_columnar_trace_store_create_reference(g, types, 3,
            logical ? columns : NULL, logical, intern, &t) == OK);
        wl_intern_free(intern);
        wl_columnar_trace_store_snapshot_t *s;
        CHECK(wl_columnar_trace_store_snapshot_acquire(t, &s) == OK);
        wl_columnar_trace_store_cursor_t *c;
        CHECK(wl_columnar_trace_store_cursor_acquire(s, &c) == OK);
        const char *text;
        CHECK(wl_columnar_trace_store_cursor_resolve_string(c, 2,
            &text) == OK && !strcmp(text, "한글 λ"));
        CHECK(wl_columnar_trace_store_cursor_resolve_string(c, UINT64_MAX,
            &text) == WL_COLUMNAR_SPILL_INVALID && !text);
        uint64_t before = reserved(g);
        wl_columnar_trace_store_t *bad = (void *)1;
        CHECK(wl_columnar_trace_store_create_reference(g, types, 3,
            logical ? columns : NULL, logical, intern,
            &bad) == WL_COLUMNAR_SPILL_CLOSED && !bad);
        CHECK(reserved(g) == before);
        col_rel_t *r = reference_source(g, kind == 3 ? 0 : kind, 0, 0);
        if (kind == 3) CHECK(col_rel_set_column_types(r, types, 3) == 0);
        before = reserved(g);
        CHECK(wl_columnar_trace_store_append_capture_reference(t, r,
            logical ? columns : NULL, logical, intern, RESIDENT, NULL, NULL,
            NULL) == WL_COLUMNAR_SPILL_CLOSED);
        CHECK(reserved(g) == before &&
            !wl_columnar_trace_store_state(t).segments);
        col_rel_destroy(r); wl_columnar_trace_store_retire(t);
        wl_columnar_trace_store_snapshot_release(s);
        CHECK(wl_columnar_trace_store_cursor_resolve_string(c, 0,
            &text) == OK && !strcmp(text, "pair"));
        wl_columnar_trace_store_cursor_release(c); CHECK(reserved(g) == 0);
        wl_columnar_memory_governor_ref_release(g);
    }
}
static void reference_failures(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_intern_t *intern = reference_intern(g), *foreign = reference_intern(g);
    wl_columnar_spill_store_t *disk = store(g);
    wirelog_column_type_t types[3]; wirelog_column_t columns[2];
    reference_schema(1, types, columns);
    wl_columnar_trace_store_t *t;
    CHECK(wl_columnar_trace_store_create_reference(g, types, 3, columns, 2,
        intern, &t) == OK);
    col_rel_t *r = reference_source(g, 1, 0, 64);
    CHECK(wl_columnar_trace_store_append_capture_reference(t, r, columns, 2,
        intern, RESIDENT, disk, NULL, NULL) == OK);
    wl_columnar_trace_store_snapshot_t *old;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &old) == OK);
    wl_columnar_trace_store_cursor_t *c;
    CHECK(wl_columnar_trace_store_cursor_acquire(old, &c) == OK);
    reference_row(c, 0, false);
    uint64_t before = reserved(g), bytes = wl_columnar_spill_store_bytes(disk);
    wl_columnar_trace_store_state_t state = wl_columnar_trace_store_state(t);
    for (unsigned which = 0; which < 17; which++) {
        wirelog_column_t supplied[2];
        memcpy(supplied, columns, sizeof(supplied));
        col_rel_t *original = r; unsigned stop = 1;
        if (which == 1) supplied[1].compound_functor_id = 2;
        if (which == 2) supplied[1].compound_arity = 1;
        if (which == 3) supplied[1].compound_inline_col_offset = 0;
        if (which == 4) r->columns[1][63] = INT64_MAX;
        if (which == 5) r->timestamps[63]._reserved = 1;
        if (which == 6) wl_columnar_trace_store_test_fail_allocation(1);
        if (which == 7) wl_columnar_batch_payload_test_fail_allocation(1);
        if (which == 8) spill_alloc = 1;
        if (which == 9) {
            write_fault = 3; write_error = ENOSPC;
        }
        if (which == 10) {
            write_fault = 3; write_error = EIO;
        }
        if (which == 13) r->has_graph_column = true;
        if (which == 14) r->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        if (which == 15) r->compound_arity_map[1] = 1;
        if (which == 16) r->column_types[1] = WIRELOG_TYPE_INT64;
        wl_columnar_source_access_reader_t lease = {0};
        if (which == 12) CHECK(col_rel_source_reader_acquire(r, &lease) == 0);
        wl_columnar_spill_status_t status =
            wl_columnar_trace_store_append_take_reference(t, &r, supplied, 2,
                which == 0 ? foreign : intern, disk,
                which == 11 ? cancel : NULL, &stop);
        CHECK(status != OK && r == original && reserved(g) == before &&
            wl_columnar_spill_store_bytes(disk) == bytes);
        if (which == 12) CHECK(status == WL_COLUMNAR_SPILL_BUSY &&
                col_rel_source_reader_release(&lease) == 0);
        same_state(t, state); reference_row(c, 0, false);
        r->columns[1][63] = 2; r->timestamps[63]._reserved = 0;
        r->has_graph_column = false;
        r->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        r->compound_arity_map[1] = 2; r->column_types[1] = WIRELOG_TYPE_STRING;
        spill_alloc = write_fault = 0; write_error = 0;
    }
    CHECK(wl_columnar_trace_store_append_capture(t, r, RESIDENT, disk, NULL,
        NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_trace_store_append_take(t, &r, disk, NULL,
        NULL) == WL_COLUMNAR_SPILL_UNSUPPORTED && r);
    CHECK(wl_columnar_trace_store_append_take_reference(t, &r, columns, 2,
        intern, disk, NULL, NULL) == OK && !r);
    wl_columnar_trace_store_cursor_release(c);
    wl_columnar_trace_store_snapshot_release(old);
    before = reserved(g); wl_intern_free(intern);
    CHECK(wl_columnar_trace_store_demote(t, 0, disk, NULL,
        NULL) == WL_COLUMNAR_SPILL_CLOSED && reserved(g) == before);
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &old) == OK);
    CHECK(wl_columnar_trace_store_cursor_acquire(old, &c) == OK);
    reference_row(c, 0, false);
    wl_columnar_spill_store_retire(disk);
    const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
    CHECK(wl_columnar_trace_store_cursor_read(c, 64, NULL, NULL, &lanes,
        &meta) == WL_COLUMNAR_SPILL_CLOSED && !lanes);
    wl_columnar_trace_store_cursor_release(c);
    wl_columnar_trace_store_snapshot_release(old);
    wl_columnar_trace_store_retire(t); wl_intern_free(foreign);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

static void reference_layout_and_reclaim(void)
{
    wl_columnar_memory_governor_ref_t *g = governor();
    wl_intern_t *intern = reference_intern(g);
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_STRING,
                                           WIRELOG_TYPE_STRING,
                                           WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_INT64};
    wirelog_column_t columns[] = {
        {.type = WIRELOG_TYPE_INT64,
         .compound_kind = WIRELOG_COMPOUND_KIND_INLINE, .compound_arity = 2},
        {.type = WIRELOG_TYPE_STRING},
        {.type = WIRELOG_TYPE_INT64,
         .compound_kind = WIRELOG_COMPOUND_KIND_INLINE, .compound_arity = 2,
         .compound_inline_col_offset = 3}
    };
    uint64_t before = reserved(g);
    for (unsigned which = 0; which < 6; which++) {
        wirelog_column_t bad[3]; memcpy(bad, columns, sizeof(bad));
        if (which == 0) bad[0].compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        if (which == 1) bad[0].compound_functor_id = UINT32_MAX;
        if (which == 2) bad[2].compound_inline_col_offset = 2;
        if (which == 3) bad[2].compound_arity = 0;
        if (which == 4) bad[1].type = WIRELOG_TYPE_INT64;
        if (which == 5) wl_columnar_trace_store_test_fail_allocation(1);
        wl_columnar_trace_store_t *t = (void *)1;
        CHECK(wl_columnar_trace_store_create_reference(g, types, 5, bad, 3,
            intern, &t) != OK && !t);
        CHECK(reserved(g) == before);
    }
    wl_columnar_memory_governor_ref_t *other = governor();
    wl_columnar_trace_store_t *t;
    CHECK(wl_columnar_trace_store_create_reference(other, types, 5, columns, 3,
        intern, &t) == WL_COLUMNAR_SPILL_INVALID && !t);
    CHECK(reserved(other) == 0); wl_columnar_memory_governor_ref_release(other);
    uint64_t owner, nodes, snap, cursor;
    CHECK(wl_columnar_trace_store_floor_with_inline(5, 0, 3, &owner, &nodes,
        &snap, &cursor) == OK);
    wl_columnar_memory_governor_t *ledger =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t limit = atomic_load_explicit(&ledger->usable_bytes,
            memory_order_acquire);
    atomic_store_explicit(&ledger->usable_bytes, before + owner - 1,
        memory_order_release);
    CHECK(wl_columnar_trace_store_create_reference(g, types, 5, columns, 3,
        intern, &t) == WL_COLUMNAR_SPILL_MEMORY && !t);
    CHECK(reserved(g) == before);
    atomic_store_explicit(&ledger->usable_bytes, limit, memory_order_release);
    CHECK(wl_columnar_trace_store_create_reference(g, types, 5, columns, 3,
        intern, &t) == OK);
    CHECK(reserved(g) == before + owner);
    col_rel_t *r;
    CHECK(wl_columnar_relation_new_auto_governed("multi_inline", 5, 1024, true,
        g, &r) == 0);
    CHECK(col_rel_set_column_types(r, types, 5) == 0);
    const col_rel_logical_col_t layout[] = {{WIRELOG_COMPOUND_KIND_INLINE, 2,
                                             1},
                                            {WIRELOG_COMPOUND_KIND_NONE, 0, 0},
                                            {WIRELOG_COMPOUND_KIND_INLINE, 2,
                                             1}};
    CHECK(col_rel_apply_compound_schema(r, layout, 3) == 0);
    for (uint32_t i = 0; i < 1024; i++) {
        r->columns[0][i] = i; r->columns[1][i] = 1; r->columns[2][i] = 2;
        r->columns[3][i] = -1; r->columns[4][i] = 99;
        r->timestamps[i] = (col_delta_timestamp_t){.multiplicity = -2};
    }
    r->nrows = 1024;
    wl_columnar_spill_store_t *tiny;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1, 4096, &tiny) == OK);
    before = reserved(g); col_rel_t *original = r;
    CHECK(wl_columnar_trace_store_append_take_reference(t, &r, columns, 3,
        intern, tiny, NULL, NULL) == WL_COLUMNAR_SPILL_QUOTA);
    CHECK(r == original && reserved(g) == before &&
        !wl_columnar_trace_store_state(t).rows &&
        !wl_columnar_spill_store_bytes(tiny));
    wl_columnar_spill_store_retire(tiny);
    wl_columnar_spill_store_t *disk = store(g);
    CHECK(wl_columnar_trace_store_append_capture_reference(t, r, columns, 3,
        intern, RESIDENT, disk, NULL, NULL) == OK);
    before = reserved(g);
    CHECK(wl_columnar_trace_store_demote(t, 0, disk, NULL, NULL) == OK);
    CHECK(before > reserved(g) + 1024*5*8 &&
        wl_columnar_spill_store_bytes(disk) > 1024*5*8);
    before = reserved(g);
    CHECK(wl_columnar_trace_store_append_take_reference(t, &r, columns, 3,
        intern, disk, NULL, NULL) == OK && !r);
    CHECK(before > reserved(g) + 1024*5*8);
    wl_columnar_trace_store_snapshot_t *ss;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &ss) == OK);
    uint32_t count;
    const wirelog_column_t *copy =
        wl_columnar_trace_store_snapshot_inline_schema(ss, &count);
    CHECK(count == 3 && (uintptr_t)copy % _Alignof(wirelog_column_t) == 0 &&
        copy[2].compound_inline_col_offset == 3);
    wl_columnar_trace_store_cursor_t *c;
    CHECK(wl_columnar_trace_store_cursor_acquire(ss, &c) == OK);
    wl_columnar_trace_store_snapshot_release(ss); wl_intern_free(intern);
    wl_columnar_trace_store_retire(t);
    for (unsigned i = 0; i < 2; i++) {
        const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
        const char *text;
        if (i) {
            read_fault = 1;
            CHECK(wl_columnar_trace_store_cursor_read(c, i*1024+1023, NULL,
                NULL, &lanes, &meta) == WL_COLUMNAR_SPILL_IO && !lanes);
        }
        CHECK(wl_columnar_trace_store_cursor_read(c, i*1024+1023, NULL, NULL,
            &lanes, &meta) == OK);
        CHECK(lanes[0] == 1023 && lanes[4] == 99 && meta.multiplicity == -2);
        CHECK(wl_columnar_trace_store_cursor_resolve_string(c, lanes[2],
            &text) == OK && !strcmp(text, "한글 λ"));
    }
    wl_columnar_trace_store_cursor_release(c);
    wl_columnar_spill_store_retire(disk);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

int main(void)
{
    reference_layout_and_reclaim();
    reference_storage(); reference_empty_and_closed(); reference_failures();
    numeric_types(); mixed_snapshots(); demotion(); store_retirement();
    failures(); admission_and_capture_rollback(); empty_directory();
    concurrent_transitions();
    puts("trace_store: OK"); return 0;
}
