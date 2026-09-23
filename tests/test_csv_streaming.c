/*
 * test_csv_streaming.c - bounded built-in CSV reader tests
 */

#include "../wirelog/io/csv_reader.h"
#include "../wirelog/io/csv_adapter_internal.h"
#include "../wirelog/io/io_ctx_internal.h"
#include "../wirelog/intern.h"
#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/wirelog-types.h"
#include "test_tmpdir.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    uint32_t callbacks;
    uint32_t rows;
    uint32_t fail_at;
    int64_t values[16][2];
    wl_columnar_memory_governor_t *governor;
    uint64_t expected_peak;
} stream_observer_t;

static int
intern_cb(void *opaque, const char *value, int64_t *out_id)
{
    return wl_intern_put_checked((wl_intern_t *)opaque, value, out_id);
}

static int
observe_rows(void *opaque, const int64_t *rows, uint32_t nrows,
    uint32_t ncols)
{
    stream_observer_t *obs = (stream_observer_t *)opaque;
    if (!obs || !rows || ncols != 2)
        return EINVAL;
    if (obs->governor
        && wl_columnar_memory_reserved(obs->governor) != obs->expected_peak)
        return EINVAL;
    obs->callbacks++;
    if (obs->fail_at != 0 && obs->callbacks == obs->fail_at)
        return ENOMEM;
    for (uint32_t i = 0; i < nrows; i++) {
        if (obs->rows >= 16)
            return EOVERFLOW;
        memcpy(obs->values[obs->rows++], rows + (size_t)i * ncols,
            sizeof(obs->values[0]));
    }
    return 0;
}

static int
write_fixture(char *path, size_t path_size, const char *name,
    const char *contents)
{
    test_tmppath(path, path_size, name);
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    fputs(contents, f);
    fclose(f);
    return 0;
}

static int
test_batches_and_strings(void)
{
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_streaming.csv",
        "1,\"alpha\"\n2,\"beta\"\n2,\"alpha\"\n3,\"gamma\"\n"
        "5,\"delta\"\n") != 0)
        return 1;

    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING,
    };
    wl_intern_t *intern = wl_intern_create();
    stream_observer_t obs = {0};
    int rc = wl_csv_read_file_via_ctx_stream(path, ',', types, 2, 2,
            observe_rows, &obs, intern_cb, intern);
    remove(path);
    wl_intern_free(intern);
    return rc == 0 && obs.callbacks == 3 && obs.rows == 5
           && obs.values[0][0] == 1 && obs.values[1][0] == 2
           && obs.values[2][0] == 2 && obs.values[4][0] == 5 ? 0 : 1;
}

static int
test_callback_failure(void)
{
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_streaming_fail.csv",
        "1,one\n2,two\n3,three\n4,four\n") != 0)
        return 1;
    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING,
    };
    wl_intern_t *intern = wl_intern_create();
    stream_observer_t obs = {.fail_at = 2};
    int rc = wl_csv_read_file_via_ctx_stream(path, ',', types, 2, 2,
            observe_rows, &obs, intern_cb, intern);
    remove(path);
    wl_intern_free(intern);
    return rc != 0 && obs.callbacks == 2 && obs.rows == 2 ? 0 : 1;
}

static int
test_admission_denial(void)
{
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_streaming_budget.csv",
        "1,one\n") != 0)
        return 1;

    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING,
    };
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 1,
        .headroom_bytes = 0,
        .usable_bytes = 1,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    stream_observer_t obs = {0};
    int rc = ref
        ? wl_csv_read_file_via_ctx_stream_admitted(path, ',', types, 2, 2,
            observe_rows, &obs, intern_cb, intern,
            wl_columnar_memory_governor_ref_get(ref))
        : WL_CSV_ERR_MEMORY;
    int clean = ref != NULL
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref)) == 0
        && rc == WL_CSV_ERR_BUDGET && obs.rows == 0;
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    wl_intern_free(intern);
    remove(path);
    return clean ? 0 : 1;
}

static wl_columnar_memory_governor_ref_t *
new_governor_ref(uint64_t budget)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = budget,
        .headroom_bytes = 0,
        .usable_bytes = budget,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static int
test_intern_budget_status_is_preserved(void)
{
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_intern_budget.csv",
        "new-symbol\n") != 0)
        return 1;

    wirelog_column_type_t types[] = { WIRELOG_TYPE_STRING };
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *intern_ref = new_governor_ref(256);
    wl_columnar_memory_governor_ref_t *reader_ref
        = new_governor_ref(UINT64_C(64) * 1024 * 1024);
    wirelog_io_ctx_t *ctx = NULL;
    int64_t values[1] = {-1};
    uint32_t count = 0;
    stream_observer_t obs = {0};
    int clean = 0;

    if (!intern || !intern_ref || !reader_ref
        || wl_intern_attach_memory_governor(intern, intern_ref) != 0)
        goto out;
    int parse_rc = wl_csv_parse_line_ex("new-symbol", ',', types, 1,
            values, 1, &count, intern);
    const char *keys[] = {"filename"};
    const char *params[] = {path};
    ctx = wirelog_io_ctx_create_test("csv_intern_budget", types, 1,
            keys, params, 1, intern);
    if (!ctx)
        goto out;
    wirelog_io_ctx_set_memory_governor(ctx,
        wl_columnar_memory_governor_ref_get(reader_ref));
    int adapter_rc = wl_csv_adapter_stream_read(ctx, 2, observe_rows, &obs);
    clean = parse_rc == WL_CSV_ERR_BUDGET
        && wl_intern_get(intern, "new-symbol") == -1
        && adapter_rc == WL_CSV_ERR_BUDGET
        && obs.callbacks == 0 && obs.rows == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(reader_ref)) == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(intern_ref)) == 256;

out:
    if (ctx)
        wirelog_io_ctx_destroy(ctx);
    if (intern)
        wl_intern_free(intern);
    if (intern_ref)
        wl_columnar_memory_governor_ref_release(intern_ref);
    if (reader_ref)
        wl_columnar_memory_governor_ref_release(reader_ref);
    remove(path);
    return clean ? 0 : 1;
}

static int
test_adapter_schema_and_reader_overlap(void)
{
    const uint64_t baseline_bytes = 37;
    uint64_t schema_bytes = 2u * sizeof(wirelog_column_type_t);
    uint64_t reader_bytes
        = 2u * 2u * sizeof(int64_t) + 2u * sizeof(int64_t)
        + WL_CSV_READ_CHUNK + 2u * (WL_CSV_MAX_LINE + 1u);
    uint64_t exact_budget = baseline_bytes + schema_bytes + reader_bytes;
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_adapter_exact.csv",
        "1,2\n") != 0)
        return 1;

    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64,
    };
    const char *keys[] = {"filename", "delimiter"};
    const char *values[] = {path, ","};
    wl_intern_t *intern = wl_intern_create();
    wirelog_io_ctx_t *ctx = wirelog_io_ctx_create_test("csv_exact", types,
            2, keys, values, 2, intern);
    wl_columnar_memory_governor_ref_t *exact =
        new_governor_ref(exact_budget);
    wl_columnar_memory_governor_ref_t *short_ref =
        new_governor_ref(exact_budget - 1u);
    wl_columnar_memory_governor_ref_t *schema_short =
        new_governor_ref(baseline_bytes + schema_bytes - 1u);
    wl_columnar_memory_reservation_t baseline;
    wl_columnar_memory_reservation_init(&baseline);
    stream_observer_t obs = {0};
    int clean = 0;

    if (!intern || !ctx || !exact || !short_ref || !schema_short)
        goto out;
    if (wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(exact), baseline_bytes,
            &baseline) != WL_COLUMNAR_MEMORY_ADMISSION_OK)
        goto out;
    wirelog_io_ctx_set_memory_governor(ctx,
        wl_columnar_memory_governor_ref_get(exact));
    obs.governor = wl_columnar_memory_governor_ref_get(exact);
    obs.expected_peak = exact_budget;
    int rc = wl_csv_adapter_stream_read(ctx, 2, observe_rows, &obs);
    if (rc != WL_CSV_OK || obs.callbacks != 1 || obs.rows != 1
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(exact)) != baseline_bytes)
        goto out;
    (void)wl_columnar_memory_release(&baseline);

    wl_columnar_memory_reservation_init(&baseline);
    if (wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(short_ref), baseline_bytes,
            &baseline) != WL_COLUMNAR_MEMORY_ADMISSION_OK)
        goto out;
    memset(&obs, 0, sizeof(obs));
    wirelog_io_ctx_set_memory_governor(ctx,
        wl_columnar_memory_governor_ref_get(short_ref));
    rc = wl_csv_adapter_stream_read(ctx, 2, observe_rows, &obs);
    if (rc != WL_CSV_ERR_BUDGET || obs.callbacks != 0 || obs.rows != 0
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(short_ref)) != baseline_bytes)
        goto out;
    (void)wl_columnar_memory_release(&baseline);

    wl_columnar_memory_reservation_init(&baseline);
    if (wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(schema_short), baseline_bytes,
            &baseline) != WL_COLUMNAR_MEMORY_ADMISSION_OK)
        goto out;
    memset(&obs, 0, sizeof(obs));
    wirelog_io_ctx_set_memory_governor(ctx,
        wl_columnar_memory_governor_ref_get(schema_short));
    rc = wl_csv_adapter_stream_read(ctx, 2, observe_rows, &obs);
    clean = rc == WL_CSV_ERR_BUDGET && obs.callbacks == 0 && obs.rows == 0
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(schema_short))
        == baseline_bytes;

out:
    if (baseline.governor)
        (void)wl_columnar_memory_release(&baseline);
    if (intern)
        wl_intern_free(intern);
    if (ctx)
        wirelog_io_ctx_destroy(ctx);
    if (exact)
        wl_columnar_memory_governor_ref_release(exact);
    if (short_ref)
        wl_columnar_memory_governor_ref_release(short_ref);
    if (schema_short)
        wl_columnar_memory_governor_ref_release(schema_short);
    remove(path);
    return clean ? 0 : 1;
}

static int
test_admission_release(void)
{
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_streaming_release.csv",
        "1,one\n2,two\n") != 0)
        return 1;

    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING,
    };
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(64) * 1024 * 1024,
        .headroom_bytes = 0,
        .usable_bytes = UINT64_C(64) * 1024 * 1024,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    stream_observer_t obs = {0};
    int rc = ref
        ? wl_csv_read_file_via_ctx_stream_admitted(path, ',', types, 2, 2,
            observe_rows, &obs, intern_cb, intern,
            wl_columnar_memory_governor_ref_get(ref))
        : WL_CSV_ERR_MEMORY;
    int clean = ref != NULL
        && rc == 0 && obs.rows == 2
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref)) == 0;
    if (ref)
        wl_columnar_memory_governor_ref_release(ref);
    wl_intern_free(intern);
    remove(path);
    return clean ? 0 : 1;
}

/* Issue #1431: a program-owned intern table attached to an exhausted
 * governor denies the first unique string a CSV load tries to intern.  The
 * load must report an error, the ids interned before the load must still
 * resolve, and neither governor may keep a stale reservation. */
static int
test_intern_denial_keeps_prior_ids(void)
{
    char path[512];
    if (write_fixture(path, sizeof(path), "wirelog_csv_streaming_intern.csv",
        "1,alpha\n2,beta\n") != 0)
        return 1;

    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING,
    };
    wl_columnar_memory_resolution_t generous = {
        .budget_bytes = 1u << 20,
            .headroom_bytes = 0,
            .usable_bytes = 1u << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
            .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
            .status = WL_COLUMNAR_MEMORY_OK,
    };
    wl_columnar_memory_resolution_t exact = generous;
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *probe
        = wl_columnar_memory_governor_ref_create(&generous);
    wl_columnar_memory_governor_ref_t *csv_ref
        = wl_columnar_memory_governor_ref_create(&generous);
    wl_columnar_memory_governor_ref_t *tight = NULL;
    stream_observer_t obs = {0};
    uint64_t footprint = 0;
    int rc = -1;
    int clean = 0;

    if (!intern || !probe || !csv_ref)
        goto out;
    /* "alpha" is interned before the load; measure the table's footprint
     * through a probe governor, then re-attach it to a governor whose budget
     * is exactly that footprint so any growth is denied. */
    if (wl_intern_put(intern, "alpha") != 0
        || wl_intern_attach_memory_governor(intern, probe) != 0)
        goto out;
    footprint = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(probe));
    if (footprint == 0 || wl_intern_detach_memory_governor(intern, probe) != 0)
        goto out;
    exact.budget_bytes = footprint;
    exact.usable_bytes = footprint;
    tight = wl_columnar_memory_governor_ref_create(&exact);
    if (!tight || wl_intern_attach_memory_governor(intern, tight) != 0)
        goto out;

    rc = wl_csv_read_file_via_ctx_stream_admitted(path, ',', types, 2, 2,
            observe_rows, &obs, intern_cb, intern,
            wl_columnar_memory_governor_ref_get(csv_ref));
    clean = rc != 0
        && wl_intern_count(intern) == 1u
        && wl_intern_get(intern, "alpha") == 0
        && strcmp(wl_intern_reverse(intern, 0), "alpha") == 0
        && wl_intern_get(intern, "beta") == -1
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(tight)) == footprint
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(csv_ref)) == 0;
    if (!clean)
        fprintf(stderr, "intern denial: rc=%d count=%u reserved=%llu/%llu\n",
            rc, (unsigned)wl_intern_count(intern),
            (unsigned long long)wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(tight)),
            (unsigned long long)footprint);
out:
    if (intern)
        wl_intern_free(intern);
    if (tight)
        clean = clean && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(tight)) == 0;
    if (tight)
        wl_columnar_memory_governor_ref_release(tight);
    if (csv_ref)
        wl_columnar_memory_governor_ref_release(csv_ref);
    if (probe)
        wl_columnar_memory_governor_ref_release(probe);
    remove(path);
    return clean ? 0 : 1;
}

int
main(void)
{
    return test_batches_and_strings() || test_callback_failure()
           || test_admission_denial()
           || test_intern_budget_status_is_preserved()
           || test_adapter_schema_and_reader_overlap()
           || test_admission_release()
           || test_intern_denial_keeps_prior_ids();
}
