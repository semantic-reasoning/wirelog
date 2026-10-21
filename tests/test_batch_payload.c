/* Numeric payload ownership, bounded reads and transaction-independent lifetime. */
#include "columnar/batch_payload.h"
#include "columnar/internal.h"
#include "columnar/columnar_nanoarrow.h"
#include "session.h"
#include "test_tmpdir.h"
#include <stdio.h>
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

int main(void)
{
    round_trip(WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT);
    round_trip(WL_COLUMNAR_BATCH_PAYLOAD_SPILLED);
    failures(); session_lifetime();
    puts("batch_payload: OK"); return 0;
}
