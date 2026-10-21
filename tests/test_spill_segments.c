/* Deterministic primitive coverage for #1372; not evaluator spill evidence. */
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L
#include "columnar/spill.h"
#include "test_tmpdir.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#else
#include <dirent.h>
#include <unistd.h>
#endif

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
#define OK WL_COLUMNAR_SPILL_OK

static int fault;
static unsigned calls, allocation_count, allocation_fail;
static const wirelog_column_type_t types[] = {
    WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_BOOL
};

int wl_columnar_spill_test_damage(wl_columnar_spill_segment_t *, bool);

bool
wl_columnar_spill_test_allocation(void)
{
    return allocation_fail && ++allocation_count == allocation_fail;
}

int
wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)writing;
    (void)offset;
    if (!fault) return 0; /* Concurrent tests never write shared hook state. */
    calls++;
    if (fault == 1 && *length > 3) *length = 3;
    if (fault == 2 && calls > 3) return ENOSPC;
    if (fault == 3) return EINTR;
    if (fault == 4) *length = 0;
    if (fault == 5 && calls > 2) return EIO;
    return 0;
}

static wl_columnar_spill_status_t
source(void *context, uint64_t ordinal, uint64_t *lanes,
    wl_columnar_spill_metadata_t *metadata)
{
    (void)context;
    int64_t integer = -(int64_t)ordinal;
    double floating = -(double)ordinal / 2.0;
    memcpy(&lanes[0], &integer, 8);
    memcpy(&lanes[1], &floating, 8);
    lanes[2] = ordinal % 2;
    metadata->iteration = (uint32_t)ordinal;
    metadata->stratum = 7;
    metadata->worker = 3;
    metadata->multiplicity = ordinal % 2 ? -2 : 1;
    return OK;
}

static bool
cancel(void *context)
{
    unsigned *remaining = context;
    if (*remaining == 0) return true;
    --*remaining;
    return false;
}

static wl_columnar_memory_governor_ref_t *
governor(uint64_t bytes)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = bytes, .usable_bytes = bytes,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    return g;
}

static uint64_t
reserved(wl_columnar_memory_governor_ref_t *g)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}

static void
check_row(wl_columnar_spill_reader_t *reader, uint64_t ordinal)
{
    const uint64_t *lanes = NULL;
    wl_columnar_spill_metadata_t meta;
    CHECK(wl_columnar_spill_read(reader, ordinal, NULL, NULL, &lanes,
        &meta) == OK);
    int64_t integer;
    double floating;
    memcpy(&integer, lanes, 8);
    memcpy(&floating, lanes + 1, 8);
    CHECK(integer == -(int64_t)ordinal);
    CHECK(floating == -(double)ordinal / 2.0);
    CHECK(lanes[2] == ordinal % 2);
    CHECK(meta.iteration == ordinal && meta.stratum == 7 && meta.worker == 3);
    CHECK(meta.multiplicity == (ordinal % 2 ? -2 : 1));
}

typedef struct {
    wl_columnar_spill_reader_t *reader;
    wl_atomic_u64 *start;
    unsigned parity;
} reader_job_t;

static void *
read_thread(void *context)
{
    reader_job_t *job = context;
    while (!atomic_load_explicit(job->start, memory_order_acquire)) {
    }
    for (uint64_t i = job->parity; i < 10000; i += 2) check_row(job->reader, i);
    return NULL;
}

static void
round_trip(const char *directory, bool retire)
{
    wl_columnar_spill_floor_t floor;
    CHECK(wl_columnar_spill_floor(directory, 3, &floor) == OK);
    uint64_t create_floor = floor.store_bytes + floor.segment_bytes +
        floor.writer_bytes;
    uint64_t read_floor = floor.store_bytes + floor.segment_bytes + 2 *
        floor.reader_bytes;
    uint64_t budget = create_floor > read_floor ? create_floor : read_floor;
    CHECK(budget < 4096);
    printf("spill fixture: disk=480044 managed_peak=%llu reader=%llu\n",
        (unsigned long long)budget, (unsigned long long)floor.reader_bytes);
    wl_columnar_memory_governor_ref_t *g = governor(budget);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(directory, g, 480044, 48, &store) == OK);
    CHECK(reserved(g) == floor.store_bytes);
    wl_columnar_spill_segment_t *segment;
    CHECK(wl_columnar_spill_create(store, types, 3, 10000, source, NULL,
        NULL, NULL, &segment) == OK);
    CHECK(wl_columnar_spill_store_bytes(store) == 480044);
    CHECK(reserved(g) == floor.store_bytes + floor.segment_bytes);
    uint32_t cols;
    uint64_t rows;
    const wirelog_column_type_t *schema = wl_columnar_spill_schema(segment,
            &cols, &rows);
    CHECK(cols == 3 && rows == 10000 && memcmp(schema, types,
        sizeof(types)) == 0);
    wl_columnar_spill_reader_t *readers[2];
    CHECK(wl_columnar_spill_acquire(segment, &readers[0]) == OK);
    CHECK(wl_columnar_spill_acquire(segment, &readers[1]) == OK);
    CHECK(reserved(g) == read_floor);
    CHECK(wl_columnar_spill_store_close(store) == WL_COLUMNAR_SPILL_BUSY);
    wl_atomic_u64 start;
    atomic_init(&start, 0);
    reader_job_t jobs[] = { { readers[0], &start, 0 },
                            { readers[1], &start, 1 } };
    wl_thread_t threads[2];
    CHECK(wl_thread_create(&threads[0], read_thread, &jobs[0]) == 0);
    CHECK(wl_thread_create(&threads[1], read_thread, &jobs[1]) == 0);
    atomic_store_explicit(&start, 1, memory_order_release);
    if (retire) wl_columnar_spill_store_retire(store);
    CHECK(wl_columnar_spill_release(segment) == WL_COLUMNAR_SPILL_BUSY);
    wl_thread_join(&threads[0]);
    wl_thread_join(&threads[1]);
    CHECK(reserved(g) == read_floor);
    const uint64_t *lanes = (const uint64_t *)1;
    wl_columnar_spill_metadata_t meta = { 9, 9, 9, 9 };
    CHECK(wl_columnar_spill_read(readers[0], 10000, NULL, NULL, &lanes, &meta)
        == WL_COLUMNAR_SPILL_INVALID);
    CHECK(lanes == NULL && meta.multiplicity == 0);
    unsigned remaining = 2;
    CHECK(wl_columnar_spill_read(readers[0], 0, cancel, &remaining, &lanes,
        &meta)
        == WL_COLUMNAR_SPILL_CANCELLED);
    CHECK(lanes == NULL && meta.iteration == 0);
    check_row(readers[0], 9999);
    fault = 1;
    check_row(readers[0], 777); /* Real short positioned reads, including header. */
    fault = 5; calls = 0;
    CHECK(wl_columnar_spill_read(readers[0], 0, NULL, NULL, &lanes, &meta)
        == WL_COLUMNAR_SPILL_IO);
    CHECK(!lanes && !meta.multiplicity);
    fault = 0;
    check_row(readers[0], 0);
    CHECK(wl_columnar_spill_test_damage(segment, true) == 0);
    CHECK(wl_columnar_spill_read(readers[0], 0, NULL, NULL, &lanes, &meta)
        == WL_COLUMNAR_SPILL_IO);
    CHECK(!lanes);
    wl_columnar_spill_reader_release(readers[0]);
    wl_columnar_spill_reader_release(readers[1]);
    CHECK(wl_columnar_spill_release(segment) == OK);
    if (!retire) {
        CHECK(wl_columnar_spill_store_bytes(store) == 0);
        CHECK(reserved(g) == floor.store_bytes);
        CHECK(wl_columnar_spill_store_close(store) == OK);
    }
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
failures(const char *directory)
{
    wl_columnar_spill_floor_t floor;
    CHECK(wl_columnar_spill_floor(directory, 3, &floor) == OK);
    wl_columnar_memory_governor_ref_t *g = governor(4096);
    wl_columnar_spill_store_t *store;
    wl_columnar_spill_segment_t *s;
    CHECK(wl_columnar_spill_store_open(directory, g, 524, 48, &store) == OK);
    uint64_t baseline = reserved(g);
    CHECK(wl_columnar_spill_create(store, types, 3, 11, source, NULL, NULL,
        NULL, &s)
        == WL_COLUMNAR_SPILL_QUOTA && s == NULL);
    CHECK(wl_columnar_spill_create(store, types, 3, UINT64_MAX, source, NULL,
        NULL, NULL, &s) == WL_COLUMNAR_SPILL_OVERFLOW);
    wirelog_column_type_t reference = WIRELOG_TYPE_STRING;
    CHECK(wl_columnar_spill_create(store, &reference, 1, 1, source, NULL,
        NULL, NULL, &s) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    for (unsigned i = 1; i <= 2; i++) {
        allocation_count = 0; allocation_fail = i;
        CHECK(wl_columnar_spill_create(store, types, 3, 10, source, NULL,
            NULL, NULL, &s) == WL_COLUMNAR_SPILL_ALLOCATION);
        CHECK(!s && reserved(g) == baseline &&
            !wl_columnar_spill_store_bytes(store));
    }
    allocation_fail = 0;
    for (int i = 2; i <= 5; i++) {
        fault = i; calls = 0;
        CHECK(wl_columnar_spill_create(store, types, 3, 10, source, NULL,
            NULL, NULL,
            &s) ==
            (i == 2 ? WL_COLUMNAR_SPILL_CAPACITY : WL_COLUMNAR_SPILL_IO));
        CHECK(!s && reserved(g) == baseline &&
            !wl_columnar_spill_store_bytes(store));
        CHECK(calls <= 16);
    }
    fault = 0;
    unsigned remaining = 7;
    CHECK(wl_columnar_spill_create(store, types, 3, 10, source, NULL,
        cancel, &remaining, &s) == WL_COLUMNAR_SPILL_CANCELLED);
    CHECK(!s && reserved(g) == baseline &&
        !wl_columnar_spill_store_bytes(store));
    fault = 1; calls = 0;
    CHECK(wl_columnar_spill_create(store, types, 3, 10, source, NULL, NULL,
        NULL, &s) == OK);
    CHECK(calls > 10);
    fault = 0;
    wl_columnar_spill_reader_t *reader;
    allocation_count = 0; allocation_fail = 1;
    CHECK(wl_columnar_spill_acquire(s,
        &reader) == WL_COLUMNAR_SPILL_ALLOCATION && !reader);
    allocation_fail = 0;
    CHECK(wl_columnar_spill_acquire(s, &reader) == OK);
    check_row(reader, 9);
    CHECK(wl_columnar_spill_test_damage(s, false) == 0);
    const uint64_t *lanes;
    wl_columnar_spill_metadata_t meta;
    CHECK(wl_columnar_spill_read(reader, 0, NULL, NULL, &lanes,
        &meta) == WL_COLUMNAR_SPILL_IO);
    wl_columnar_spill_reader_release(reader);
    CHECK(wl_columnar_spill_release(s) == OK);
    CHECK(wl_columnar_spill_create(store, types, 3, 0, NULL, NULL, NULL, NULL,
        &s) == OK);
    CHECK(wl_columnar_spill_acquire(s, &reader) == OK);
    CHECK(wl_columnar_spill_read(reader, 0, NULL, NULL, &lanes,
        &meta) == WL_COLUMNAR_SPILL_INVALID);
    wl_columnar_spill_reader_release(reader);
    CHECK(wl_columnar_spill_release(s) == OK);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
    g = governor(floor.store_bytes + floor.segment_bytes + floor.writer_bytes -
            1);
    CHECK(wl_columnar_spill_store_open(directory, g, 524, 48, &store) == OK);
    CHECK(wl_columnar_spill_create(store, types, 3, 10, source, NULL, NULL,
        NULL, &s)
        == WL_COLUMNAR_SPILL_MEMORY);
    CHECK(!s && reserved(g) == floor.store_bytes);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static wl_columnar_spill_status_t
wide_source(void *context, uint64_t ordinal, uint64_t *lanes,
    wl_columnar_spill_metadata_t *metadata)
{
    uint32_t ncols = *(uint32_t *)context;
    for (uint32_t c = 0; c < ncols; c++) lanes[c] = ordinal + c;
    metadata->multiplicity = INT64_MIN;
    return OK;
}

static void
wide_rows(const char *directory)
{
    uint32_t ncols = 1024;
    wirelog_column_type_t schema[1024];
    for (unsigned i = 0; i < 1024; i++) schema[i] = WIRELOG_TYPE_UINT64;
    wl_columnar_memory_governor_ref_t *g = governor(65536);
    wl_columnar_spill_store_t *store;
    wl_columnar_spill_segment_t *s;
    wl_columnar_spill_reader_t *reader;
    CHECK(wl_columnar_spill_store_open(directory, g, 100000, 8216,
        &store) == OK);
    CHECK(wl_columnar_spill_create(store, schema, ncols, 2, wide_source, &ncols,
        NULL, NULL, &s) == OK);
    CHECK(wl_columnar_spill_acquire(s, &reader) == OK);
    const uint64_t *lanes;
    wl_columnar_spill_metadata_t metadata;
    CHECK(wl_columnar_spill_read(reader, 1, NULL, NULL, &lanes,
        &metadata) == OK);
    for (unsigned i = 0; i < 1024; i++) CHECK(lanes[i] == i + 1);
    CHECK(metadata.multiplicity == INT64_MIN);
    /* Header, schema, then a partial row crosses the 4096-byte I/O boundary. */
    unsigned remaining = 3;
    CHECK(wl_columnar_spill_read(reader, 0, cancel, &remaining, &lanes,
        &metadata)
        == WL_COLUMNAR_SPILL_CANCELLED);
    CHECK(!lanes && metadata.multiplicity == 0);
    wl_columnar_spill_reader_release(reader);
    CHECK(wl_columnar_spill_release(s) == OK);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(wl_columnar_spill_store_open(directory, g, 100000, 8215,
        &store) == OK);
    CHECK(wl_columnar_spill_create(store, schema, ncols, 2, wide_source, &ncols,
        NULL, NULL, &s) == WL_COLUMNAR_SPILL_MEMORY && !s);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

#ifndef _WIN32
static unsigned
fd_count(void)
{
    DIR *dir = opendir("/proc/self/fd");
    if (!dir) return 0; /* Linux-only descriptor leak witness. */
    unsigned count = 0;
    while (readdir(dir)) count++;
    closedir(dir);
    return count;
}
#endif

int
main(void)
{
    char directory[512];
#ifdef _WIN32
    snprintf(directory, sizeof(directory), "%s/wirelog-spill-test-%lu",
        test_tmpdir(),
        (unsigned long)GetCurrentProcessId());
    CHECK(_mkdir(directory) == 0);
#else
    snprintf(directory, sizeof(directory), "%s/wirelog-spill-test-XXXXXX",
        test_tmpdir());
    CHECK(mkdtemp(directory));
    unsigned descriptors = fd_count();
#endif
    round_trip(directory, false);
    round_trip(directory, true);
    failures(directory);
    wide_rows(directory);
#ifdef _WIN32
    CHECK(_rmdir(directory) == 0);
#else
    CHECK(fd_count() == descriptors);
    CHECK(rmdir(directory) == 0);
#endif
    puts("spill segments: OK");
    return 0;
}
