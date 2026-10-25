/* Numeric payload ownership, bounded reads and transaction-independent lifetime. */
#include "columnar/batch_payload.h"
#include "columnar/internal.h"
#include "columnar/columnar_nanoarrow.h"
#include "session.h"
#include "ir/program.h"
#include "test_tmpdir.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
#define OK WL_COLUMNAR_SPILL_OK
extern void wl_columnar_batch_payload_test_fail_allocation(unsigned);

static wl_columnar_memory_governor_ref_t *
governor(uint64_t budget)
{
    wl_columnar_memory_resolution_t r = {.budget_bytes = budget,
                                         .usable_bytes = budget,
                                         .mode =
                                             WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                                         .source =
                                             WL_COLUMNAR_MEMORY_SOURCE_ENV,
                                         .status = WL_COLUMNAR_MEMORY_OK};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g); return g;
}

static uint64_t
reserved(wl_columnar_memory_governor_ref_t *g)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}

static bool
cancel(void *context)
{
    unsigned *remaining = context;
    if (!*remaining) return true;
    --*remaining; return false;
}

static col_rel_t *
source(void)
{
    col_rel_t *r = col_rel_new_auto("numeric", 3);
    CHECK(r);
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT,
                                     WIRELOG_TYPE_BOOL};
    CHECK(col_rel_set_column_types(r, types, 3) == 0);
    for (int64_t i = 0; i < 8; i++) {
        double value = -(double)i / 2;
        int64_t row[] = {-i, 0, i % 2};
        memcpy(&row[1], &value, 8);
        CHECK(col_rel_append_row(r, row) == 0);
        /* append normalizes -0.0; capture must preserve the source bits,
         * including a deliberately installed negative-zero lane. */
        r->columns[1][i] = row[1];
    }
    CHECK(col_rel_enable_timestamps(r) == 0);
    for (uint32_t i = 0; i < 8; i++)
        r->timestamps[i] = (col_delta_timestamp_t){i, 7, 3, 0, i % 2 ? -2 : 1};
    return r;
}

static void
check_row(wl_columnar_batch_payload_reader_t *reader, uint32_t i)
{
    const uint64_t *lanes;
    wl_columnar_spill_metadata_t metadata;
    CHECK(wl_columnar_batch_payload_read(reader, i, NULL, NULL, &lanes,
        &metadata) == OK);
    int64_t integer; double floating;
    memcpy(&integer, lanes, 8); memcpy(&floating, lanes+1, 8);
    CHECK(integer == -(int64_t)i && floating == -(double)i/2 &&
        lanes[2] == i%2);
    double expected = -(double)i / 2;
    uint64_t bits;
    memcpy(&bits, &expected, sizeof(bits));
    CHECK(lanes[1] == bits);
    CHECK(metadata.iteration == i && metadata.stratum == 7 &&
        metadata.worker == 3
        && metadata.multiplicity == (i%2 ? -2 : 1));
}

typedef struct { wl_columnar_batch_payload_reader_t *reader; } job_t;
static void *
read_thread(void *context)
{
    job_t *job = context;
    for (unsigned i = 0; i < 1000; i++)check_row(job->reader, i%8);
    return NULL;
}

static void
round_trip(wl_columnar_batch_payload_storage_t storage)
{
    wl_columnar_memory_governor_ref_t *g = governor(1<<24);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 4096, 48,
        &store) == OK);
    col_rel_t *r = source();
    wl_columnar_batch_payload_t *p;
    uint64_t bytes, reader_bytes;
    CHECK(wl_columnar_batch_payload_floor(3, 8, storage, &bytes,
        &reader_bytes) == OK);
    uint64_t baseline = reserved(g);
    CHECK(wl_columnar_batch_payload_capture(g, store, r, 8, storage, NULL, NULL,
        &p) == OK);
    col_rel_destroy(r);
    uint32_t ncols, nrows;
    const wirelog_column_type_t *types = wl_columnar_batch_payload_schema(p,
            &ncols, &nrows);
    CHECK(ncols == 3 && nrows == 8 && types[1] == WIRELOG_TYPE_FLOAT &&
        types[2] == WIRELOG_TYPE_BOOL);
    wl_columnar_spill_floor_t floor;
    CHECK(wl_columnar_spill_floor(test_tmpdir(), 3, &floor) == OK);
    CHECK(reserved(g) ==
        baseline+bytes+(storage ==
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED ? floor.segment_bytes : 0));
    wl_columnar_batch_payload_reader_t *readers[2];
    uint64_t before_failure = reserved(g);
    wl_columnar_batch_payload_test_fail_allocation(1);
    CHECK(wl_columnar_batch_payload_acquire(p,
        &readers[0]) == WL_COLUMNAR_SPILL_ALLOCATION);
    CHECK(!readers[0] && reserved(g) == before_failure);
    CHECK(wl_columnar_batch_payload_acquire(p, &readers[0]) == OK);
    CHECK(wl_columnar_batch_payload_acquire(p, &readers[1]) == OK);
    CHECK(wl_columnar_batch_payload_release(p) == WL_COLUMNAR_SPILL_BUSY);
    job_t jobs[] = {{readers[0]}, {readers[1]}};
    wl_thread_t threads[2];
    CHECK(wl_thread_create(&threads[0], read_thread, &jobs[0]) == 0);
    CHECK(wl_thread_create(&threads[1], read_thread, &jobs[1]) == 0);
    wl_columnar_spill_store_retire(store);
    wl_thread_join(&threads[0]); wl_thread_join(&threads[1]);
    wl_columnar_batch_payload_reader_t *extra;
    if (storage == WL_COLUMNAR_BATCH_PAYLOAD_SPILLED)
        CHECK(wl_columnar_batch_payload_acquire(p,
            &extra) == WL_COLUMNAR_SPILL_CLOSED && !extra);
    const uint64_t *lanes = (const uint64_t *)1;
    wl_columnar_spill_metadata_t meta = {1, 1, 1, 1};
    CHECK(wl_columnar_batch_payload_read(readers[0], 8, NULL, NULL, &lanes,
        &meta) == WL_COLUMNAR_SPILL_INVALID);
    CHECK(!lanes && !meta.multiplicity);
    unsigned remaining = 0;
    CHECK(wl_columnar_batch_payload_read(readers[0], 0, cancel, &remaining,
        &lanes, &meta) == WL_COLUMNAR_SPILL_CANCELLED);
    CHECK(!lanes && !meta.multiplicity);
    wl_columnar_batch_payload_reader_release(readers[0]);
    wl_columnar_batch_payload_reader_release(readers[1]);
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
failures(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1<<24),
        *other = governor(1);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 64, 48, &store) == OK);
    col_rel_t *r = source();
    wl_columnar_batch_payload_t *p;
    uint64_t baseline = reserved(g), bytes, readers;
    CHECK(wl_columnar_batch_payload_floor(UINT32_MAX, UINT32_MAX,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, &bytes,
        &readers) == WL_COLUMNAR_SPILL_OVERFLOW);
    CHECK(wl_columnar_batch_payload_capture(other, store, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, NULL, NULL,
        &p) == WL_COLUMNAR_SPILL_INVALID && !p);
    CHECK(wl_columnar_batch_payload_capture(other, NULL, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL,
        &p) == WL_COLUMNAR_SPILL_MEMORY && !p);
    CHECK(reserved(other) == 0);
    CHECK(wl_columnar_batch_payload_capture(g, store, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, NULL, NULL,
        &p) == WL_COLUMNAR_SPILL_QUOTA && !p);
    for (unsigned mode = 0; mode < 2; mode++) {
        unsigned remaining = 3;
        /* A separate roomy store permits cancellation after private progress. */
        wl_columnar_spill_store_t *room;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 4096, 48,
            &room) == OK);
        CHECK(wl_columnar_batch_payload_capture(g, room, r, 8,
            (wl_columnar_batch_payload_storage_t)mode, cancel, &remaining,
            &p) == WL_COLUMNAR_SPILL_CANCELLED && !p);
        CHECK(wl_columnar_spill_store_bytes(room) == 0);
        CHECK(wl_columnar_spill_store_close(room) == OK &&
            reserved(g) == baseline);
    }
    wl_columnar_batch_payload_test_fail_allocation(1);
    CHECK(wl_columnar_batch_payload_capture(g, store, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL,
        &p) == WL_COLUMNAR_SPILL_ALLOCATION && !p);
    CHECK(reserved(g) == baseline);
    r->column_types[0] = WIRELOG_TYPE_STRING;
    CHECK(wl_columnar_batch_payload_capture(g, store, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL,
        &p) == WL_COLUMNAR_SPILL_UNSUPPORTED && !p);
    r->column_types[0] = WIRELOG_TYPE_INT64;
    r->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
    CHECK(wl_columnar_batch_payload_capture(g, store, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, NULL, NULL,
        &p) == WL_COLUMNAR_SPILL_UNSUPPORTED && !p);
    r->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
    CHECK(reserved(g) == baseline);
    /* All failed captures relinquish source leases. */
    CHECK(col_rel_set(r, 0, 0, 0) == 0);
    CHECK(wl_columnar_batch_payload_capture(g, store, r, 0,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, NULL, NULL, &p) == OK);
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    CHECK(wl_columnar_spill_store_close(store) == OK && reserved(g) == 0);
    col_rel_destroy(r); wl_columnar_memory_governor_ref_release(g);
    wl_columnar_memory_governor_ref_release(other);
}

static void
session_lifetime(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1<<28);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = g; options.spill_directory = test_tmpdir();
    options.spill_quota_bytes = 4096; options.spill_max_row_bytes = 48;
    wl_plan_t plan = {0}; wl_session_t *session;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == 0);
    col_rel_t *r = source(); wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture(g,
        COL_SESSION(session)->spill_store, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, NULL, NULL, &p) == OK);
    wl_columnar_batch_payload_reader_t *reader;
    CHECK(wl_columnar_batch_payload_acquire(p, &reader) == OK);
    col_rel_destroy(r); wl_session_destroy(session);
    check_row(reader, 7);
    wl_columnar_batch_payload_reader_release(reader);
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
check_payload(wl_columnar_batch_payload_t *p)
{
    uint32_t cols, rows;
    const wirelog_column_type_t *types =
        wl_columnar_batch_payload_schema(p, &cols, &rows);
    CHECK(cols == 3 && rows == 8 && types[0] == WIRELOG_TYPE_INT64 &&
        types[1] == WIRELOG_TYPE_FLOAT && types[2] == WIRELOG_TYPE_BOOL);
    wl_columnar_batch_payload_reader_t *reader;
    CHECK(wl_columnar_batch_payload_acquire(p, &reader) == OK);
    const unsigned order[] = {7, 0, 3, 1, 6, 2, 5, 4};
    for (unsigned i = 0; i < 8; i++) check_row(reader, order[i]);
    wl_columnar_batch_payload_reader_release(reader);
}

static void
demotion(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1<<24);
    wl_columnar_spill_store_t *store, *quota_store, *other_store;
    wl_columnar_memory_governor_ref_t *other = governor(1<<20);
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 4096, 48,
        &store) == OK);
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1, 48,
        &quota_store) == OK);
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), other, 4096, 48,
        &other_store) == OK);
    col_rel_t *r = source();
    wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture(g, NULL, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL, &p) == OK);
    col_rel_destroy(r);
    wl_columnar_batch_payload_t *original = p;
    uint64_t baseline = reserved(g);
    CHECK(wl_columnar_batch_payload_spill(NULL, store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_INVALID);
    CHECK(wl_columnar_batch_payload_spill(&p, other_store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_INVALID);
    CHECK(wl_columnar_batch_payload_spill(&p, quota_store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_QUOTA);
    wl_columnar_batch_payload_test_fail_allocation(1);
    CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_ALLOCATION);
    CHECK(p == original && reserved(g) == baseline &&
        wl_columnar_spill_store_bytes(store) == 0);
    check_payload(p);
    wl_columnar_batch_payload_reader_t *a, *b;
    CHECK(wl_columnar_batch_payload_acquire(p, &a) == OK);
    CHECK(wl_columnar_batch_payload_acquire(p, &b) == OK);
    CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_BUSY);
    check_row(a, 0); check_row(b, 7);
    wl_columnar_batch_payload_reader_release(a);
    CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_BUSY);
    wl_columnar_batch_payload_reader_release(b);
    /* Sweep every cancellation point, including after private publication.
     * Each failed attempt must be retryable with the same original owner. */
    bool completed = false;
    for (unsigned point = 0; point < 128; point++) {
        unsigned remaining = point;
        wl_columnar_spill_status_t rc =
            wl_columnar_batch_payload_spill(&p, store, cancel, &remaining);
        if (rc == OK) {
            completed = true; break;
        }
        CHECK(rc == WL_COLUMNAR_SPILL_CANCELLED && p == original &&
            reserved(g) == baseline &&
            wl_columnar_spill_store_bytes(store) == 0);
        check_payload(p);
    }
    CHECK(completed && wl_columnar_spill_store_bytes(store) > 0);
    uint64_t resident_bytes, spilled_bytes, reader_bytes;
    wl_columnar_spill_floor_t floor;
    CHECK(wl_columnar_spill_floor(test_tmpdir(), 3, &floor) == OK);
    CHECK(wl_columnar_batch_payload_floor(3, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
        &resident_bytes, &reader_bytes) == OK);
    CHECK(wl_columnar_batch_payload_floor(3, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED,
        &spilled_bytes, &reader_bytes) == OK);
    CHECK(reserved(g) ==
        baseline - resident_bytes + spilled_bytes + floor.segment_bytes);
    CHECK(reserved(g) < baseline);
    check_payload(p);
    original = p;
    CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_INVALID && p == original);
    CHECK(wl_columnar_batch_payload_acquire(p, &a) == OK);
    wl_columnar_spill_store_retire(store);
    check_row(a, 7);
    /* The existing spilled payload keeps the retired store pointer valid. */
    r = source();
    wl_columnar_batch_payload_t *resident;
    CHECK(wl_columnar_batch_payload_capture(g, NULL, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL, &resident) == OK);
    original = resident; baseline = reserved(g);
    CHECK(wl_columnar_batch_payload_spill(&resident, store, NULL, NULL) ==
        WL_COLUMNAR_SPILL_CLOSED && resident == original &&
        reserved(g) == baseline);
    check_payload(resident);
    col_rel_destroy(r);
    CHECK(wl_columnar_batch_payload_release(resident) == OK);
    wl_columnar_batch_payload_reader_release(a);
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    CHECK(wl_columnar_spill_store_close(quota_store) == OK && reserved(g) == 0);
    CHECK(wl_columnar_spill_store_close(other_store) == OK &&
        reserved(other) == 0);
    wl_columnar_memory_governor_ref_release(other);
    wl_columnar_memory_governor_ref_release(g);
}

static void
demotion_floor(void)
{
    wl_columnar_spill_floor_t floor;
    uint64_t resident_bytes, descriptor, reader_bytes;
    CHECK(wl_columnar_spill_floor(test_tmpdir(), 3, &floor) == OK);
    CHECK(wl_columnar_batch_payload_floor(3, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
        &resident_bytes, &reader_bytes) == OK);
    CHECK(wl_columnar_batch_payload_floor(3, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED,
        &descriptor, &reader_bytes) == OK);
    for (unsigned fits = 0; fits < 2; fits++) {
        uint64_t budget = floor.store_bytes + resident_bytes + descriptor +
            floor.segment_bytes + floor.writer_bytes - 1 + fits;
        wl_columnar_memory_governor_ref_t *g = governor(budget);
        wl_columnar_spill_store_t *store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 4096, 48,
            &store) == OK);
        col_rel_t *r = source();
        wl_columnar_batch_payload_t *p;
        CHECK(wl_columnar_batch_payload_capture(g, NULL, r, 8,
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL, &p) == OK);
        col_rel_destroy(r);
        wl_columnar_batch_payload_t *original = p;
        uint64_t baseline = reserved(g);
        wl_columnar_spill_status_t rc = wl_columnar_batch_payload_spill(&p,
                store, NULL, NULL);
        if (fits) CHECK(rc == OK);
        else CHECK(rc == WL_COLUMNAR_SPILL_MEMORY && p == original &&
                reserved(g) == baseline &&
                wl_columnar_spill_store_bytes(store) == 0);
        check_payload(p);
        CHECK(wl_columnar_batch_payload_release(p) == OK);
        CHECK(wl_columnar_spill_store_close(store) == OK && reserved(g) == 0);
        wl_columnar_memory_governor_ref_release(g);
    }
    /* Empty demotion is valid but makes no promise of a memory reduction. */
    wl_columnar_memory_governor_ref_t *g = governor(1<<20);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 4096, 48,
        &store) == OK);
    col_rel_t *r = source(); wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture(g, NULL, r, 0,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL, &p) == OK);
    col_rel_destroy(r);
    CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL) == OK);
    uint32_t rows;
    CHECK(wl_columnar_batch_payload_schema(p, NULL, &rows) && rows == 0);
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    CHECK(wl_columnar_spill_store_close(store) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

typedef struct {
    wl_columnar_batch_payload_reader_t *reader;
    const char **values;
} string_job_t;

static void *
string_reader_thread(void *context)
{
    string_job_t *job = context;
    for (unsigned i = 0; i < 1000; i++) {
        const uint64_t *lanes; wl_columnar_spill_metadata_t metadata;
        uint32_t row = i % 8;
        CHECK(wl_columnar_batch_payload_read(job->reader, row, NULL, NULL,
            &lanes, &metadata) == OK);
        CHECK(lanes[0] == row % 3 && metadata.iteration == row &&
            metadata.stratum == 7 && metadata.worker == 3 &&
            metadata.multiplicity == (row % 2 ? -2 : 1));
        const char *value;
        CHECK(wl_columnar_batch_payload_resolve_string(job->reader, lanes[0],
            &value) == OK);
        CHECK(strcmp(value, job->values[row % 3]) == 0);
    }
    return NULL;
}

static void
string_payloads(void)
{
    char long_value[4097]; memset(long_value, 'x', sizeof(long_value) - 1);
    long_value[sizeof(long_value) - 1] = 0;
    const char *values[] = {"", "\xeb\xac\xb8\xec\x9e\x90\xec\x97\xb4",
                            long_value};
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_columnar_memory_governor_ref_t *g = governor(1u<<24),
            *other = governor(1u<<20);
        wl_intern_t *intern = wl_intern_create();
        CHECK(wl_intern_attach_memory_governor(intern, g) == 0);
        for (unsigned i = 0; i < 3;
            i++) CHECK(wl_intern_put(intern, values[i]) == i);
        wl_columnar_spill_store_t *store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 4096, 48,
            &store) == OK);
        col_rel_t *r = source(); r->column_types[0] = WIRELOG_TYPE_STRING;
        for (unsigned i = 0; i < 8; i++) r->columns[0][i] = i % 3;
        wl_columnar_batch_payload_t *p;
        wl_columnar_batch_payload_storage_t storage = mode == 1 ?
            WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT;
        CHECK(wl_columnar_batch_payload_capture(g, store, r, 8, storage,
            NULL, NULL, &p) == WL_COLUMNAR_SPILL_UNSUPPORTED && !p);
        CHECK(wl_columnar_batch_payload_capture_with_intern(other, NULL, r, 8,
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, intern, NULL, NULL,
            &p) == WL_COLUMNAR_SPILL_INVALID && !p);
        CHECK(reserved(other) == 0);
        wl_columnar_spill_store_t *quota_store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1, 48,
            &quota_store) == OK);
        CHECK(wl_columnar_batch_payload_capture_with_intern(g, quota_store, r,
            8,
            WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, intern, NULL, NULL,
            &p) == WL_COLUMNAR_SPILL_QUOTA && !p);
        CHECK(wl_columnar_spill_store_close(quota_store) == OK);
        CHECK(wl_intern_detach_memory_governor(intern, g) == 0);
        CHECK(wl_intern_attach_memory_governor(intern, g) == 0);
        wl_columnar_batch_payload_test_fail_allocation(1);
        CHECK(wl_columnar_batch_payload_capture_with_intern(g, store, r, 8,
            storage, intern, NULL, NULL,
            &p) == WL_COLUMNAR_SPILL_ALLOCATION && !p);
        uint64_t baseline = reserved(g);
        r->columns[0][7] = -1;
        CHECK(wl_columnar_batch_payload_capture_with_intern(g, store, r, 8,
            storage, intern, NULL, NULL,
            &p) == WL_COLUMNAR_SPILL_INVALID && !p);
        r->columns[0][7] = 3;
        CHECK(wl_columnar_batch_payload_capture_with_intern(g, store, r, 8,
            storage, intern, NULL, NULL,
            &p) == WL_COLUMNAR_SPILL_INVALID && !p);
        CHECK(reserved(g) == baseline &&
            wl_columnar_spill_store_bytes(store) == 0);
        CHECK(wl_intern_detach_memory_governor(intern, g) == 0);
        CHECK(wl_intern_attach_memory_governor(intern, g) == 0);
        r->columns[0][7] = 1;
        /* Every cancellation point refunds both private storage and pins. */
        bool captured = false;
        for (unsigned point = 0; point < 128; point++) {
            unsigned remaining = point;
            wl_columnar_spill_status_t rc =
                wl_columnar_batch_payload_capture_with_intern(
                g, store, r, 8, storage, intern, cancel, &remaining, &p);
            if (rc == OK) {
                captured = true; break;
            }
            CHECK(rc == WL_COLUMNAR_SPILL_CANCELLED && !p &&
                reserved(g) == baseline &&
                wl_columnar_spill_store_bytes(store) == 0);
            CHECK(wl_intern_detach_memory_governor(intern, g) == 0);
            CHECK(wl_intern_attach_memory_governor(intern, g) == 0);
        }
        CHECK(captured);
        col_rel_destroy(r);
        if (mode == 2) {
            wl_columnar_batch_payload_t *original = p;
            baseline = reserved(g);
            wl_columnar_batch_payload_test_fail_allocation(1);
            CHECK(wl_columnar_batch_payload_spill(&p, store, NULL,
                NULL) == WL_COLUMNAR_SPILL_ALLOCATION);
            CHECK(p == original && reserved(g) == baseline);
            bool spilled = false;
            for (unsigned point = 0; point < 128; point++) {
                unsigned remaining = point;
                wl_columnar_spill_status_t rc =
                    wl_columnar_batch_payload_spill(&p, store, cancel,
                        &remaining);
                if (rc == OK) {
                    spilled = true; break;
                }
                CHECK(rc == WL_COLUMNAR_SPILL_CANCELLED && p == original &&
                    reserved(g) == baseline &&
                    wl_columnar_spill_store_bytes(store) == 0);
            }
            CHECK(spilled);
        }
        uint32_t cols, rows;
        CHECK(wl_columnar_batch_payload_schema(p, &cols,
            &rows)[0] == WIRELOG_TYPE_STRING && cols == 3 && rows == 8);
        CHECK(wl_intern_detach_memory_governor(intern, g) == EBUSY);
        for (unsigned i = 0; i < 1024; i++) {
            char value[32]; snprintf(value, sizeof(value), "growth-%u", i);
            CHECK(wl_intern_put(intern, value) >= 3);
        }
        if (mode != 1) wl_intern_free(intern);
        if (mode == 0) {
            wl_columnar_batch_payload_t *original = p;
            baseline = reserved(g);
            CHECK(wl_columnar_batch_payload_spill(&p, store, NULL,
                NULL) == WL_COLUMNAR_SPILL_CLOSED);
            CHECK(p == original && reserved(g) == baseline);
        }
        wl_columnar_batch_payload_reader_t *a, *b;
        CHECK(wl_columnar_batch_payload_acquire(p, &a) == OK);
        CHECK(wl_columnar_batch_payload_acquire(p, &b) == OK);
        string_job_t jobs[] = {{a, values}, {b, values}};
        wl_thread_t threads[2];
        CHECK(wl_thread_create(&threads[0], string_reader_thread,
            &jobs[0]) == 0);
        CHECK(wl_thread_create(&threads[1], string_reader_thread,
            &jobs[1]) == 0);
        if (mode == 1) wl_intern_free(intern); /* Retire while pinned readers run. */
        wl_columnar_spill_store_retire(store);
        CHECK(wl_columnar_batch_payload_release(p) == WL_COLUMNAR_SPILL_BUSY);
        wl_thread_join(&threads[0]); wl_thread_join(&threads[1]);
        const char *bad = "sentinel";
        CHECK(wl_columnar_batch_payload_resolve_string(a, UINT64_MAX,
            &bad) == WL_COLUMNAR_SPILL_INVALID && !bad);
        wl_columnar_batch_payload_reader_release(a);
        wl_columnar_batch_payload_reader_release(b);
        CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
        wl_columnar_memory_governor_ref_release(g);
        wl_columnar_memory_governor_ref_release(other);
    }
}

static void
string_session_lifetime(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1u<<24);
    struct wirelog_program *program = wl_ir_program_create(); CHECK(program);
    wl_intern_t *intern = program->intern;
    CHECK(wl_intern_put(intern, "alpha") == 0 && wl_intern_put(intern,
        "beta") == 1);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = g; options.spill_directory = test_tmpdir();
    options.spill_quota_bytes = 4096; options.spill_max_row_bytes = 48;
    wl_plan_t plan = {.intern = intern}; wl_session_t *session;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == 0);
    col_rel_t *r = source(); r->column_types[0] = WIRELOG_TYPE_STRING;
    for (unsigned i = 0; i < 8; i++) r->columns[0][i] = i % 2;
    wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture_with_intern(g,
        COL_SESSION(session)->spill_store,
        r, 8, WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, intern, NULL, NULL, &p) == OK);
    wl_columnar_batch_payload_reader_t *reader;
    CHECK(wl_columnar_batch_payload_acquire(p, &reader) == OK);
    col_rel_destroy(r); wl_session_destroy(session);
    wl_ir_program_free(program);
    const uint64_t *lanes; wl_columnar_spill_metadata_t meta; const char *value;
    CHECK(wl_columnar_batch_payload_read(reader, 7, NULL, NULL, &lanes,
        &meta) == OK);
    CHECK(meta.multiplicity == -2 && lanes[0] == 1);
    CHECK(wl_columnar_batch_payload_resolve_string(reader, lanes[0],
        &value) == OK && strcmp(value, "beta") == 0);
    wl_columnar_batch_payload_reader_release(reader);
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static col_rel_t *
inline_source(wl_intern_t *intern, wirelog_column_t columns[4])
{
    CHECK(wl_intern_put(intern, "pair") == 0);
    CHECK(wl_intern_put(intern, "unary") == 1);
    CHECK(wl_intern_put(intern, "alpha") == 2);
    CHECK(wl_intern_put(intern, "beta") == 3);
    columns[0] = (wirelog_column_t){.name = "key", .type = WIRELOG_TYPE_INT64};
    columns[1] = (wirelog_column_t){.name = "pair", .type = WIRELOG_TYPE_INT64,
                                    .compound_kind =
                                        WIRELOG_COMPOUND_KIND_INLINE,
                                    .compound_functor_id = 0,
                                    .compound_arity = 2,
                                    .compound_inline_col_offset = 1};
    columns[2] = (wirelog_column_t){.name = "unary", .type = WIRELOG_TYPE_INT64,
                                    .compound_kind =
                                        WIRELOG_COMPOUND_KIND_INLINE,
                                    .compound_functor_id = 1,
                                    .compound_arity = 1,
                                    .compound_inline_col_offset = 3};
    columns[3] = (wirelog_column_t){.name = "flag", .type = WIRELOG_TYPE_BOOL};
    col_rel_t *r = col_rel_new_auto("inline", 5);
    const col_rel_logical_col_t layout[] = {
        {WIRELOG_COMPOUND_KIND_NONE, 0, 0},
        {WIRELOG_COMPOUND_KIND_INLINE, 2, 1},
        {WIRELOG_COMPOUND_KIND_INLINE, 1, 1},
        {WIRELOG_COMPOUND_KIND_NONE, 0, 0}
    };
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING,
                                     WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64,
                                     WIRELOG_TYPE_BOOL};
    CHECK(r && col_rel_set_column_types(r, types, 5) == 0);
    CHECK(col_rel_apply_compound_schema(r, layout, 4) == 0);
    for (int64_t i = 0; i < 8; i++) {
        int64_t row[] = {i, 2 + i % 2, -i, 100+i, i % 2};
        CHECK(col_rel_append_row(r, row) == 0);
    }
    CHECK(col_rel_enable_timestamps(r) == 0);
    for (uint32_t i = 0; i < 8; i++)
        r->timestamps[i] = (col_delta_timestamp_t){i, 7, 3, 0, i % 2 ? -2 : 1};
    return r;
}

static void
inline_check(wl_columnar_batch_payload_reader_t *reader, uint32_t row)
{
    const uint64_t *lanes; wl_columnar_spill_metadata_t meta; const char *text;
    CHECK(wl_columnar_batch_payload_read(reader, row, NULL, NULL, &lanes,
        &meta) == OK);
    CHECK(lanes[0] == row && (int64_t)lanes[2] == -(int64_t)row
        && lanes[3] == 100 + row && lanes[4] == row % 2);
    CHECK(meta.iteration == row && meta.stratum == 7 && meta.worker == 3
        && meta.multiplicity == (row % 2 ? -2 : 1));
    CHECK(wl_columnar_batch_payload_resolve_string(reader, lanes[1],
        &text) == OK);
    CHECK(strcmp(text, row % 2 ? "beta" : "alpha") == 0);
    CHECK(wl_columnar_batch_payload_resolve_string(reader, 0, &text) == OK
        && strcmp(text, "pair") == 0);
}

static void *
inline_thread(void *arg)
{
    for (unsigned i = 0; i < 100; i++) inline_check(arg, i % 8);
    return NULL;
}

static void
inline_payloads(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_columnar_memory_governor_ref_t *g = governor(1<<24);
        wl_intern_t *intern = wl_intern_create();
        CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
        wirelog_column_t columns[4];
        col_rel_t *r = inline_source(intern, columns);
        wl_columnar_spill_store_t *store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 65536, 64,
            &store) == OK);
        wl_columnar_batch_payload_storage_t storage = mode == 1
            ? WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT;
        wl_columnar_batch_payload_t *p = NULL;
        CHECK(wl_columnar_batch_payload_capture_with_intern(g, store, r, 8,
            storage, intern, NULL, NULL,
            &p) == WL_COLUMNAR_SPILL_UNSUPPORTED && !p);
        uint64_t bytes, reader_bytes, before = reserved(g);
        CHECK(wl_columnar_batch_payload_floor_with_inline(5, 8, 4, storage,
            &bytes, &reader_bytes) == OK);
        if (mode == 0) {
            atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                    g)->usable_bytes,
                before + bytes - 1, memory_order_release);
            CHECK(wl_columnar_batch_payload_capture_with_inline(g, store, r, 8,
                storage, columns, 4, intern, NULL, NULL,
                &p) == WL_COLUMNAR_SPILL_MEMORY);
            CHECK(!p && reserved(g) == before);
            atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                    g)->usable_bytes,
                before + bytes, memory_order_release);
        }
        CHECK(wl_columnar_batch_payload_capture_with_inline(g, store, r, 8,
            storage, columns, 4, intern, NULL, NULL, &p) == OK);
        wl_columnar_spill_floor_t floor;
        CHECK(wl_columnar_spill_floor_with_inline(test_tmpdir(), 5, 4,
            &floor) == OK);
        CHECK(reserved(g) ==
            before + bytes + (mode == 1 ? floor.segment_bytes : 0));
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                g)->usable_bytes,
            1<<24, memory_order_release);
        columns[1].compound_functor_id = 999; /* Caller array is not retained. */
        uint32_t logical_count;
        const wirelog_column_t *logical =
            wl_columnar_batch_payload_inline_schema(p, &logical_count);
        CHECK(logical_count == 4 && logical[1].compound_functor_id == 0
            && logical[2].compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && logical[2].compound_arity == 1 && !logical[0].name &&
            !logical[1].name);
        if (mode == 2) {
            wl_columnar_batch_payload_t *old = p;
            unsigned remaining = 2;
            uint64_t retained = reserved(g);
            CHECK(wl_columnar_batch_payload_spill(&p, store, cancel, &remaining)
                == WL_COLUMNAR_SPILL_CANCELLED);
            CHECK(p == old && reserved(g) == retained);
            wl_columnar_batch_payload_test_fail_allocation(1);
            CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL)
                == WL_COLUMNAR_SPILL_ALLOCATION && p == old &&
                reserved(g) == retained);
            CHECK(wl_columnar_batch_payload_spill(&p, store, NULL,
                NULL) == OK && p != old);
            logical = wl_columnar_batch_payload_inline_schema(p,
                    &logical_count);
            CHECK(logical_count == 4 && logical[1].compound_functor_id == 0
                && logical[2].compound_inline_col_offset == 3);
        }
        col_rel_destroy(r);
        wl_intern_free(intern);
        if (mode == 0) {
            wl_columnar_batch_payload_t *old = p;
            uint64_t retained = reserved(g);
            CHECK(wl_columnar_batch_payload_spill(&p, store, NULL, NULL)
                == WL_COLUMNAR_SPILL_CLOSED && p == old &&
                reserved(g) == retained);
        }
        wl_columnar_batch_payload_reader_t *readers[2];
        CHECK(wl_columnar_batch_payload_acquire(p, &readers[0]) == OK);
        CHECK(wl_columnar_batch_payload_acquire(p, &readers[1]) == OK);
        CHECK(wl_columnar_batch_payload_release(p) == WL_COLUMNAR_SPILL_BUSY);
        wl_columnar_spill_store_retire(store);
        wl_thread_t threads[2];
        CHECK(wl_thread_create(&threads[0], inline_thread, readers[0]) == 0);
        CHECK(wl_thread_create(&threads[1], inline_thread, readers[1]) == 0);
        wl_thread_join(&threads[0]); wl_thread_join(&threads[1]);
        wl_columnar_batch_payload_reader_release(readers[0]);
        wl_columnar_batch_payload_reader_release(readers[1]);
        CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
        wl_columnar_memory_governor_ref_release(g);
    }
}

static void
inline_numeric_functors(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1<<24);
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
    CHECK(wl_intern_put(intern, "unary") == 0);
    col_rel_t *r = source();
    const col_rel_logical_col_t layout[] = {
        {WIRELOG_COMPOUND_KIND_NONE, 0, 0},
        {WIRELOG_COMPOUND_KIND_INLINE, 1, 1},
        {WIRELOG_COMPOUND_KIND_NONE, 0, 0}
    };
    CHECK(col_rel_apply_compound_schema(r, layout, 3) == 0);
    const wirelog_column_t columns[] = {
        {.type = WIRELOG_TYPE_INT64},
        {.type = WIRELOG_TYPE_INT64,
         .compound_kind = WIRELOG_COMPOUND_KIND_INLINE,
         .compound_arity = 1, .compound_inline_col_offset = 1},
        {.type = WIRELOG_TYPE_BOOL}
    };
    wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture_with_inline(g, NULL, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, columns, 3, intern, NULL, NULL,
        &p) == OK);
    col_rel_destroy(r); wl_intern_free(intern);
    wl_columnar_batch_payload_reader_t *reader;
    CHECK(wl_columnar_batch_payload_acquire(p, &reader) == OK);
    check_row(reader, 7);
    const char *name;
    CHECK(wl_columnar_batch_payload_resolve_string(reader, 0, &name) == OK
        && strcmp(name, "unary") == 0);
    wl_columnar_batch_payload_reader_release(reader);
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
inline_failures(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1<<24);
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
    wirelog_column_t columns[4];
    col_rel_t *r = inline_source(intern, columns);
    uint64_t before = reserved(g);
    for (unsigned which = 0; which < 11; which++) {
        wirelog_column_t bad[4]; memcpy(bad, columns, sizeof(bad));
        uint32_t count = 4;
        if (which == 0) bad[2].compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        if (which == 1) bad[1].compound_arity = 0;
        if (which == 2) bad[1].compound_arity = 5;
        if (which == 3) bad[2].compound_inline_col_offset = 2;
        if (which == 4) bad[1].compound_functor_id = UINT32_MAX;
        if (which == 5) bad[0].type = WIRELOG_TYPE_STRING;
        if (which == 6) count = 3;
        if (which == 7) r->compound_count = 1;
        if (which == 8) r->has_graph_column = true;
        if (which == 9) r->columns[1][7] = INT64_MAX;
        if (which == 10) r->compound_arity_map[2] = 2;
        wl_columnar_batch_payload_t *p = (void *)1;
        CHECK(wl_columnar_batch_payload_capture_with_inline(g, NULL, r, 8,
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, bad, count, intern, NULL, NULL,
            &p) != OK);
        CHECK(!p && reserved(g) == before);
        r->compound_count = 2; r->has_graph_column = false;
        r->columns[1][7] = 3; r->compound_arity_map[2] = 1;
    }
    wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture_with_inline(g, NULL, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, columns, 4, NULL, NULL, NULL, &p)
        == WL_COLUMNAR_SPILL_INVALID && !p);
    wl_columnar_memory_governor_ref_t *other = governor(1<<24);
    CHECK(wl_columnar_batch_payload_capture_with_inline(other, NULL, r, 8,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, columns, 4, intern, NULL, NULL, &p)
        == WL_COLUMNAR_SPILL_INVALID && !p && reserved(other) == 0);
    wl_columnar_memory_governor_ref_release(other);
    col_rel_destroy(r); wl_intern_free(intern);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

int main(void)
{
    uint64_t inline_bytes, inline_reader_bytes;
    CHECK(wl_columnar_batch_payload_floor_with_inline(UINT32_MAX, UINT32_MAX,
        UINT32_MAX, WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, &inline_bytes,
        &inline_reader_bytes) == WL_COLUMNAR_SPILL_OVERFLOW);
    CHECK(wl_columnar_batch_payload_floor_with_inline(2, 1, 3,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, &inline_bytes,
        &inline_reader_bytes) == WL_COLUMNAR_SPILL_INVALID);
    inline_payloads(); inline_numeric_functors(); inline_failures();
    round_trip(WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT);
    round_trip(WL_COLUMNAR_BATCH_PAYLOAD_SPILLED);
    failures(); session_lifetime(); demotion(); demotion_floor();
    string_payloads(); string_session_lifetime();
    puts("batch_payload: OK"); return 0;
}
