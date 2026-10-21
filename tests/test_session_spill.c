/* #1372: internal opt-in, shared admission and detached numeric lifetime. */
#include "columnar/internal.h"
#include "columnar/columnar_nanoarrow.h"
#include "session.h"
#include "test_tmpdir.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
#define OK WL_COLUMNAR_SPILL_OK
#define BUDGET (UINT64_C(256) * 1024 * 1024)

static uint64_t
reserved(wl_columnar_memory_governor_ref_t *g)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}

static wl_columnar_spill_status_t
source(void *context, uint64_t ordinal, uint64_t *lanes,
    wl_columnar_spill_metadata_t *metadata)
{
    (void)context;
    lanes[0] = ordinal + 42;
    metadata->multiplicity = -1;
    return OK;
}

int
main(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = BUDGET, .usable_bytes = BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    wl_plan_t plan = {0};
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = g;
    wl_session_t *session;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == 0);
    CHECK(!COL_SESSION(session)->spill_store &&
        !COL_SESSION(session)->owns_spill_store);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);

    /* Failure at the last fallible creation step cleans prior allocations. */
    options.spill_directory = "";
    options.spill_quota_bytes = 4096;
    options.spill_max_row_bytes = 32;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == EINVAL && !session);
    char missing[4096];
    test_tmppath(missing, sizeof(missing),
        "wirelog-nonexistent-spill-dir/no-parent");
    options.spill_directory = missing;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == EIO && !session);
    CHECK(reserved(g) == 0);

    char directory[4096];
    CHECK(strlen(test_tmpdir()) < sizeof(directory));
    strcpy(directory, test_tmpdir());
    options.spill_directory = directory;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == 0);
    wl_col_session_t *coordinator = COL_SESSION(session);
    CHECK(coordinator->memory_governor == g && coordinator->owns_spill_store);
    wl_columnar_spill_store_t *store = coordinator->spill_store;
    CHECK(store);
    /* The store has copied its path; it does not borrow options storage. */
    memset(directory, 0, sizeof(directory));
    uint64_t baseline = reserved(g);
    wl_col_session_t *worker = calloc(1, sizeof(*worker));
    CHECK(worker);
    for (unsigned i = 0; i < 2; i++) {
        CHECK(col_worker_session_create(coordinator, 0, NULL, 0, worker) == 0);
        CHECK(worker->spill_store == store && !worker->owns_spill_store);
        CHECK(col_worker_session_destroy(worker) == 0);
        CHECK(reserved(g) == baseline);
    }
    /* Force a worker failure after copying ownership, before partitions move. */
    wl_columnar_memory_reservation_t blocker;
    wl_columnar_memory_reservation_init(&blocker);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(g),
        BUDGET - reserved(g), &blocker));
    col_rel_t *partitions[1] = {NULL};
    CHECK(col_worker_session_create(coordinator, 0, partitions, 1,
        worker) != 0);
    CHECK(!worker->spill_store && !worker->owns_spill_store);
    CHECK(wl_columnar_memory_rollback(&blocker));
    CHECK(reserved(g) == baseline);
    free(worker);

    wirelog_column_type_t type = WIRELOG_TYPE_UINT64;
    wl_columnar_spill_segment_t *segments[2];
    wl_columnar_spill_reader_t *readers[2];
    for (unsigned i = 0; i < 2; i++) {
        CHECK(wl_columnar_spill_create(store, &type, 1, 2, source, NULL,
            NULL, NULL, &segments[i]) == OK);
        CHECK(wl_columnar_spill_acquire(segments[i], &readers[i]) == OK);
    }
    wl_columnar_spill_floor_t floor;
    CHECK(wl_columnar_spill_floor(test_tmpdir(), 1, &floor) == OK);
    CHECK(reserved(g) ==
        baseline + 2 * (floor.segment_bytes + floor.reader_bytes));
    wl_session_destroy(session);
    CHECK(reserved(g) ==
        floor.store_bytes + 2 * (floor.segment_bytes + floor.reader_bytes));
    wl_columnar_spill_segment_t *extra = NULL;
    CHECK(wl_columnar_spill_create(store, &type, 1, 2, source, NULL,
        NULL, NULL, &extra) == WL_COLUMNAR_SPILL_CLOSED && !extra);
    for (unsigned i = 0; i < 2; i++) {
        wl_columnar_spill_reader_t *new_reader = NULL;
        CHECK(wl_columnar_spill_acquire(segments[i], &new_reader)
            == WL_COLUMNAR_SPILL_CLOSED && !new_reader);
        uint64_t before = reserved(g);
        CHECK(wl_columnar_spill_release(segments[i]) == WL_COLUMNAR_SPILL_BUSY);
        CHECK(reserved(g) == before);
        const uint64_t *lanes;
        wl_columnar_spill_metadata_t metadata;
        CHECK(wl_columnar_spill_read(readers[i], 1, NULL, NULL, &lanes,
            &metadata) == OK && lanes[0] == 43 && metadata.multiplicity == -1);
        wl_columnar_spill_reader_release(readers[i]);
        CHECK(wl_columnar_spill_release(segments[i]) == OK);
    }
    CHECK(reserved(g) == 0);
    /* Empty-store teardown frees immediately. */
    options.spill_directory = test_tmpdir();
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
    puts("session_spill: OK");
    return 0;
}
