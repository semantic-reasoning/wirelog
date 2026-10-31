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

int main(void)
{
    numeric_types(); mixed_snapshots(); demotion(); store_retirement();
    failures(); admission_and_capture_rollback(); empty_directory();
    concurrent_transitions();
    puts("trace_store: OK"); return 0;
}
