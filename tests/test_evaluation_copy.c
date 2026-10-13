/* #1821: actual physical-row projection and original attempt authority. */
#define WL_TEST_RELATION_RESIZE_HOOK 1
#include "../wirelog/columnar/internal.h"
#include <math.h>
#include <stdio.h>
#ifndef _WIN32
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

static int owner;
static uint64_t seen, maximum, peak;
static unsigned checkpoint_count, stop_checkpoint, stop_mode;
static bool restarted;
static unsigned successor_callbacks;
static wl_evaluation_control_report_t old_report;
static wl_columnar_memory_governor_ref_t *watched;
static col_rel_t **external_out;

static wl_columnar_memory_governor_ref_t *
governor(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref);
    return ref;
}

static uint64_t
reserved(void)
{
    return watched ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(watched)) : 0;
}

static wl_evaluation_control_t *
control(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(limit, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    seen = maximum = peak = 0;
    checkpoint_count = successor_callbacks = 0;
    restarted = false;
    return c;
}

static uint64_t
finish(wl_evaluation_control_t *c, int expected)
{
    CHECK(wl_evaluation_control_finish(c, &owner,
        expected > 0 ? expected : 0, 0) == expected);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    /* Reached-tail labels are site telemetry, not accounting: renewal has
     * multiple tails financed by one earlier caller admission. Literal
     * schedules and actual reports, never a sum of labels, prove budgets. */
    CHECK(report.result == expected);
    return report.consumed;
}

static void
release(wl_evaluation_control_t *c)
{
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

static void
restart(wl_evaluation_control_t *c)
{
    CHECK(!restarted);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &old_report) == 0);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) != old_report.attempt_id);
    restarted = true;
}

static void
checkpoint(wl_evaluation_control_t *c, uint64_t work)
{
    if (restarted) {
        successor_callbacks++;
        return;
    }
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    CHECK(!external_out || !*external_out);
    seen += work;
    if (work > maximum)
        maximum = work;
    if (reserved() > peak)
        peak = reserved();
    checkpoint_count++;
    if (!stop_checkpoint || checkpoint_count != stop_checkpoint)
        return;
    if (stop_mode == 0)
        wl_evaluation_control_request_cancel(c);
    else if (stop_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else
        restart(c);
}

static col_rel_t *
source(uint32_t width, uint32_t rows, bool typed, bool timestamps,
    wl_columnar_memory_governor_ref_t *ref)
{
    col_rel_t *r = NULL;
    if (ref) {
        CHECK(wl_columnar_relation_new_auto_governed("copy source", width,
            rows ? rows : 1, timestamps, ref, &r) == 0 && r);
    } else {
        r = col_rel_new_auto("copy source", width);
        CHECK(r && !r->memory_governor);
    }
    CHECK(r->nrows == 0);
    if (typed && width) {
        wirelog_column_type_t *types = malloc(width * sizeof(*types));
        CHECK(types);
        for (uint32_t column = 0; column < width; column++)
            types[column] = (wirelog_column_type_t)(column % 7);
        CHECK(col_rel_set_column_types(r, types, width) == 0);
        free(types);
    }
    if (ref) {
        CHECK(r->capacity >= rows && (!width || r->columns));
        for (uint32_t column = 0; column < width; column++)
            CHECK(r->columns[column]);
        r->nrows = rows;
    } else {
        int64_t *zero = calloc(width ? width : 1, sizeof(*zero));
        CHECK(zero);
        for (uint32_t row = 0; row < rows; row++)
            CHECK(col_rel_append_row(r, zero) == 0);
        free(zero);
        if (timestamps)
            CHECK(col_rel_enable_timestamps(r) == 0);
    }
    CHECK(r->nrows == rows && r->capacity >= rows);
    CHECK(!timestamps || !rows
        || (r->timestamps && r->timestamp_capacity >= rows));
    r->has_graph_column = width != 0;
    r->graph_col_idx = width ? width - 1 : 0;
    r->declared_ncols = width;
    for (uint32_t column = 0; column < width; column++) {
        for (uint32_t row = 0; row < rows; row++) {
            int64_t value = (int64_t)column * 100 + row % 7;
            if (typed && r->column_types[column] == WIRELOG_TYPE_BOOL)
                value = row % 2;
            if (typed && r->column_types[column] == WIRELOG_TYPE_FLOAT)
                value = wl_columnar_float_to_bits(row ? (double)(row % 7)
                                                     : -0.0);
            r->columns[column][row] = value;
        }
    }
    if (timestamps) {
        for (uint32_t row = 0; row < rows; row++) {
            r->timestamps[row].iteration = row % 11;
            r->timestamps[row].stratum = row % 3;
            r->timestamps[row].worker = row % 2;
            r->timestamps[row]._reserved = 0;
            r->timestamps[row].multiplicity = row % 3 == 0 ? -3
                : row % 3 == 1 ? 5 : 1;
        }
    }
    if (r->row_scratch)
        for (uint32_t column = 0; column < width; column++)
            r->row_scratch[column] = INT64_C(0x1234567800) + column;
    return r;
}

typedef struct {
    unsigned char descriptor[sizeof(col_rel_t)];
    int64_t *cells;
    col_delta_timestamp_t *timestamps;
    int64_t *scratch;
} source_snapshot_t;

static source_snapshot_t
snapshot(const col_rel_t *src)
{
    source_snapshot_t saved;
    memcpy(saved.descriptor, src, sizeof(*src));
    size_t cells = (size_t)src->ncols * src->nrows;
    saved.cells = cells ? malloc(cells * sizeof(*saved.cells)) : NULL;
    CHECK(!cells || saved.cells);
    for (uint32_t column = 0; column < src->ncols; column++)
        if (src->nrows)
            memcpy(saved.cells + (size_t)column * src->nrows,
                src->columns[column], src->nrows * sizeof(*saved.cells));
    saved.timestamps = src->timestamps && src->timestamp_capacity
        ? malloc(src->timestamp_capacity * sizeof(*saved.timestamps)) : NULL;
    CHECK(!src->timestamps || !src->timestamp_capacity || saved.timestamps);
    if (saved.timestamps)
        memcpy(saved.timestamps, src->timestamps,
            src->timestamp_capacity * sizeof(*saved.timestamps));
    saved.scratch = src->row_scratch && src->ncols
        ? malloc(src->ncols * sizeof(*saved.scratch)) : NULL;
    CHECK(!src->row_scratch || !src->ncols || saved.scratch);
    if (saved.scratch)
        memcpy(saved.scratch, src->row_scratch,
            src->ncols * sizeof(*saved.scratch));
    return saved;
}

static void
unchanged(const col_rel_t *src, source_snapshot_t *saved)
{
    CHECK(memcmp(saved->descriptor, src, sizeof(*src)) == 0);
    for (uint32_t column = 0; column < src->ncols; column++)
        if (src->nrows)
            CHECK(memcmp(saved->cells + (size_t)column * src->nrows,
                src->columns[column], src->nrows * sizeof(*saved->cells)) == 0);


    if (saved->timestamps)
        CHECK(memcmp(saved->timestamps, src->timestamps,
            src->timestamp_capacity * sizeof(*saved->timestamps)) == 0);
    if (saved->scratch)
        CHECK(memcmp(saved->scratch, src->row_scratch,
            src->ncols * sizeof(*saved->scratch)) == 0);
}

static void
snapshot_free(source_snapshot_t *saved)
{
    free(saved->cells); free(saved->timestamps); free(saved->scratch);
}

static void
readers_released(col_rel_t *src)
{
    col_rel_t *root = NULL;
    CHECK(col_rel_storage_owner_resolve(src, &root) == 0);
    wl_columnar_source_access_writer_t descriptor = { 0 }, owner_writer = { 0 };
    CHECK(wl_columnar_source_access_writer_acquire(&src->descriptor_access,
        &descriptor) == 0);
    CHECK(wl_columnar_source_access_writer_acquire(&root->source_access,
        &owner_writer) == 0);
    CHECK(wl_columnar_source_access_writer_release(&owner_writer) == 0);
    CHECK(wl_columnar_source_access_writer_release(&descriptor) == 0);
}

static void
equal(const col_rel_t *actual, const col_rel_t *raw)
{
    CHECK(actual && raw && actual->ncols == raw->ncols
        && actual->nrows == raw->nrows && !actual->timestamps
        && !raw->timestamps && !actual->timestamp_capacity
        && actual->has_graph_column == raw->has_graph_column
        && actual->graph_col_idx == raw->graph_col_idx
        && actual->declared_ncols == raw->declared_ncols);
    CHECK(strcmp(actual->name, raw->name) == 0);
    CHECK((actual->column_types != NULL) == (raw->column_types != NULL));
    for (uint32_t column = 0; column < actual->ncols; column++) {
        CHECK(strcmp(actual->col_names[column], raw->col_names[column]) == 0);
        CHECK(strcmp(actual->schema.children[column]->format,
            raw->schema.children[column]->format) == 0);
        if (actual->column_types)
            CHECK(actual->column_types[column] == raw->column_types[column]);
        for (uint32_t row = 0; row < actual->nrows; row++)
            CHECK(actual->columns[column][row] == raw->columns[column][row]);
    }
}

static void
twins(void)
{
    const uint32_t widths[] = {0, 1, 16, 17};
    for (unsigned index = 0; index < 4; index++) {
        for (unsigned typed = 0; typed < 2; typed++) {
            for (unsigned governed = 0; governed < 2; governed++) {
                wl_columnar_memory_governor_ref_t *source_ref = governor();
                col_rel_t *src = source(widths[index], 133, typed != 0, true,
                        source_ref);
                col_rel_t *raw_src = source(widths[index], 133, typed != 0,
                        true, NULL);
                source_snapshot_t saved = snapshot(src);
                uint64_t source_bytes = wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(source_ref));
                watched = governor();
                const uint32_t ranges[][2] = {{0, 0}, {0, 133}, {2, 131}};
                for (unsigned range = 0; range < 3; range++) {
                    wl_evaluation_control_t *c = control(0);
                    col_rel_t *out = NULL, *raw = NULL;
                    external_out = &out;
                    CHECK(wl_columnar_relation_copy_rows_controlled("copy", src,
                        ranges[range][0], ranges[range][1],
                        governed ? watched : NULL, &out, c) == 0);
                    external_out = NULL;
                    CHECK(finish(c, 0) > 0
                        && maximum <= WL_EVALUATION_CONTROL_QUANTUM);
                    CHECK(wl_columnar_relation_copy_rows("copy", raw_src,
                        ranges[range][0], ranges[range][1], NULL, &raw) == 0);
                    equal(out, raw);
                    CHECK(out->nrows == ranges[range][1] - ranges[range][0]);
                    CHECK(out->memory_governor == (governed ? watched : NULL));
                    CHECK(reserved() == out->descriptor_reserved_bytes
                        + out->metadata_reserved_bytes
                        + out->retained_reserved_bytes);
                    unchanged(src, &saved); readers_released(src);
                    CHECK(wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(source_ref))
                        == source_bytes);
                    CHECK(col_rel_destroy_checked(out) == 0
                        && col_rel_destroy_checked(raw) == 0);
                    CHECK(reserved() == 0
                        && wl_columnar_memory_governor_ref_is_sole(watched));
                    release(c);
                }
                snapshot_free(&saved);
                CHECK(col_rel_destroy_checked(src) == 0
                    && col_rel_destroy_checked(raw_src) == 0);
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(source_ref)) == 0);
                wl_columnar_memory_governor_ref_release(source_ref);
                wl_columnar_memory_governor_ref_release(watched);
                watched = NULL;
            }
        }
    }
}

static void
domain_and_raw(void)
{
    col_rel_t *src = source(1, 3, false, false, NULL);
    watched = governor();
    const uint32_t ranges[][2] = {{2, 1}, {0, 4}, {4, 4}};
    for (unsigned i = 0; i < 3; i++) {
        wl_evaluation_control_t *c = control(0);
        col_rel_t *out = NULL;
        CHECK(wl_columnar_relation_copy_rows_controlled("bad", src,
            ranges[i][0], ranges[i][1], watched, &out, c) == EINVAL && !out);
        CHECK(finish(c, EINVAL) > 0 && reserved() == 0);
        readers_released(src); release(c);
    }
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_evaluation_control_finish(c, &owner, 0, 0) == 0);
    col_rel_t *out = src; /* borrowed sentinel, not an owned output */
    CHECK(wl_columnar_relation_copy_rows_controlled("inactive", src, 0, 1,
        watched, &out, c) == EINVAL && !out);
    release(c);
    out = src;
    CHECK(wl_columnar_relation_copy_rows_controlled(NULL, src, 0, 1, NULL,
        &out, NULL) == EINVAL && out == src);
    CHECK(wl_columnar_relation_copy_rows_controlled("raw", src, 0, 3, NULL,
        &out, NULL) == 0 && out && out->nrows == 3);
    CHECK(col_rel_destroy_checked(out) == 0);
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
assert_failed_copy(col_rel_t *src, uint32_t begin, uint32_t end, int expected)
{
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_copy_rows_controlled("bad source", src, begin,
        end, watched, &out, c) == expected && !out);
    CHECK(finish(c, expected) > 0 && reserved() == 0
        && wl_columnar_memory_governor_ref_is_sole(watched));
    release(c);
}

static void
shape_and_busy(void)
{
    col_rel_t *src = source(1, 3, false, false, NULL);
    watched = governor();
    uint32_t capacity = src->capacity;
    src->capacity = 2;
    assert_failed_copy(src, 0, 1, EINVAL);
    src->capacity = capacity;
    readers_released(src);
    int64_t *lane = src->columns[0];
    src->columns[0] = NULL;
    assert_failed_copy(src, 0, 1, EINVAL);
    src->columns[0] = lane;
    readers_released(src);
    int64_t **columns = src->columns;
    src->columns = NULL;
    assert_failed_copy(src, 0, 1, EINVAL);
    /* An empty range never dereferences the source grid. */
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_copy_rows_controlled("empty", src, 2, 2,
        watched, &out, c) == 0 && out && !out->nrows);
    CHECK(finish(c, 0) > 0 && col_rel_destroy_checked(out) == 0);
    release(c); src->columns = columns;
    readers_released(src);
    col_rel_t *saved_owner = src->storage_owner;
    src->storage_owner = NULL;
    assert_failed_copy(src, 0, 1, ENOTSUP);
    src->storage_owner = saved_owner;
    readers_released(src);
    for (unsigned gate = 0; gate < 2; gate++) {
        wl_columnar_source_access_writer_t writer = { 0 };
        CHECK(wl_columnar_source_access_writer_acquire(gate
            ? &src->source_access : &src->descriptor_access, &writer) == 0);
        assert_failed_copy(src, 0, 1, EBUSY);
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
        readers_released(src);
    }
    CHECK(reserved() == 0 && col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
one_source_copy(col_rel_t *src)
{
    source_snapshot_t saved = snapshot(src);
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_copy_rows_controlled("borrowed copy", src, 0,
        src->nrows, watched, &out, c) == 0 && out);
    CHECK(out->nrows == src->nrows && out->ncols == src->ncols
        && !out->timestamps);
    for (uint32_t column = 0; column < src->ncols; column++)
        for (uint32_t row = 0; row < src->nrows; row++)
            CHECK(out->columns[column][row]
                == (int64_t)wl_columnar_value_key(src, column,
                src->columns[column][row]));
    CHECK(finish(c, 0) > 0);
    unchanged(src, &saved); readers_released(src);
    snapshot_free(&saved);
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 0
        && wl_columnar_memory_governor_ref_is_sole(watched));
    release(c); wl_columnar_memory_governor_ref_release(watched);
    watched = NULL;
}

static void
borrowed_sources(void)
{
    col_rel_t *src = source(17, 131, true, true, NULL);
    col_rel_t *alias = col_rel_new_like("alias", src);
    CHECK(alias && col_rel_install_shared_view(alias, src) == 0);
    CHECK(alias->storage_owner == src);
    one_source_copy(alias);
    CHECK(col_rel_destroy_checked(alias) == 0);
    delta_pool_t *pool = delta_pool_create(2, sizeof(col_rel_t), 65536);
    CHECK(pool);
    col_rel_t *pooled = col_rel_pool_new_like(pool, "pool", src);
    CHECK(pooled && pooled->pool_owned);
    int64_t values[17];
    for (unsigned column = 0; column < 17; column++)
        values[column] = 0;
    CHECK(col_rel_append_row(pooled, values) == 0);
    one_source_copy(pooled);
    CHECK(col_rel_destroy_checked(pooled) == 0); delta_pool_destroy(pool);
    wl_arena_t *arena = wl_arena_create(65536);
    pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    CHECK(arena && pool);
    pooled = col_rel_pool_new_auto(pool, arena, "arena", 1);
    CHECK(pooled && pooled->pool_owned && pooled->arena_owned);
    CHECK(col_rel_append_row(pooled, values) == 0);
    one_source_copy(pooled);
    CHECK(col_rel_destroy_checked(pooled) == 0); delta_pool_destroy(pool);
    wl_arena_free(arena);
    CHECK(col_rel_destroy_checked(src) == 0);
}

static void
invalid_float(void)
{
    col_rel_t *src = source(1, 0, false, false, NULL);
    CHECK(src->nrows == 0 && !src->memory_governor);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    int64_t zero = wl_columnar_float_to_bits(0.0);
    for (unsigned row = 0; row < 3; row++)
        CHECK(col_rel_append_row(src, &zero) == 0);
    CHECK(src->nrows == 3 && src->capacity >= 3
        && src->columns && src->columns[0]);
    src->columns[0][0] = wl_columnar_float_to_bits(1.0);
    src->columns[0][1] = wl_columnar_float_to_bits(NAN);
    src->columns[0][2] = wl_columnar_float_to_bits(2.0);
    source_snapshot_t saved = snapshot(src);
    watched = governor();
    assert_failed_copy(src, 0, 3, EINVAL);
    unchanged(src, &saved); readers_released(src); snapshot_free(&saved);
    CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

/* Independent literal schedule, including child BEGIN and CHECK admissions.
 * No runtime checkpoint total or implementation enum constructs the oracle. */
static void
literal_nullary(void)
{
    uint64_t prefixes[2048];
    for (unsigned rows = 0; rows <= 2; rows++) {
        size_t count = 0;
        uint64_t total = 0;
#define LITERAL(work) do { \
            CHECK(count < sizeof(prefixes) / sizeof(prefixes[0])); \
            total += (work); prefixes[count++] = total; \
} \
        while (0)
        const uint64_t source_and_builder[] = {
            128, 192, 128, /* copy entry, complete context, source caller */
            128, 96, 144, 128, 96, 144, 160, /* original source pair/recipe */
            128, 128, 192, /* copy shape/builder caller, private build init */
            96, /* empty relation name */
            96, 96, 96, 96, 96, 96, /* checked constructor sizes */
            96, 96, 96, 96, /* type presence and grid/table/TS arithmetic */
            128, 96, 160, 112 /* fits, credits, skipped D, descriptor malloc */
        };
        for (size_t i = 0;
            i < sizeof(source_and_builder) / sizeof(source_and_builder[0]); i++)
            LITERAL(source_and_builder[i]);
        for (size_t pos = 0; pos < sizeof(col_rel_t); pos += 16)
            LITERAL(224);
        for (unsigned i = 0; i < 5; i++)
            LITERAL(96); /* original embedded destination tokens */
        const uint64_t constructor_and_begin[] = {
            112, 224, 80, 112, 96, 160, 160, 96, 112, 224,
            128, 128, 128, 220, /* complete private no-ledger install */
            224, /* paid copy begin bootstrap and caller */
            170, 94, 19, 39, 39, 45, 52, 45, 21, 144
        };
        for (size_t i = 0; i < sizeof(constructor_and_begin)
            / sizeof(constructor_and_begin[0]); i++)
            LITERAL(constructor_and_begin[i]);
        const uint64_t one_row[] = {
            128, 192, /* copy progress and pre-CHECK caller */
            220, 80, 63, 103, 48 /* actual nullary no-growth held row */
        };
        for (unsigned row = 0; row < rows; row++)
            for (size_t i = 0; i < sizeof(one_row) / sizeof(one_row[0]); i++)
                LITERAL(one_row[i]);
        LITERAL(96); /* original drains, final external cutoff */
#undef LITERAL
        col_rel_t *src = source(0, 2, false, false, NULL);
        source_snapshot_t saved = snapshot(src);
        watched = governor();
        for (size_t i = 0; i < count; i++) {
            for (unsigned below = 0; below < 2; below++) {
                wl_evaluation_control_t *c = control(prefixes[i] - below);
                col_rel_t *out = NULL;
                int rc = wl_columnar_relation_copy_rows_controlled("", src,
                        0, rows, NULL, &out, c);
                if (!below && i + 1 == count) {
                    CHECK(rc == 0 && out && out->nrows == rows && !out->ncols);
                    CHECK(finish(c, 0) == total);
                    CHECK(col_rel_destroy_checked(out) == 0);
                } else {
                    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !out);
                    CHECK(finish(c, rc) == (below
                        ? (i ? prefixes[i - 1] : 0) : prefixes[i]));
                }
                unchanged(src, &saved); readers_released(src);
                CHECK(reserved() == 0
                    && wl_columnar_memory_governor_ref_is_sole(watched));
                release(c);
            }
        }
        snapshot_free(&saved); CHECK(col_rel_destroy_checked(src) == 0);
        wl_columnar_memory_governor_ref_release(watched); watched = NULL;
    }
}

static void
all_work_seams(void)
{
    col_rel_t *src = source(17, 2, true, true, NULL);
    source_snapshot_t saved = snapshot(src);
    watched = governor();
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_copy_rows_controlled("seams", src, 0, 2,
        watched, &out, c) == 0 && out);
    unsigned checkpoints = checkpoint_count;
    CHECK(checkpoints > 100 && finish(c, 0) > 0);
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 0);
    release(c);
    /* Observed site enumeration tests runtime interruption coverage only.
    * It never supplies tariffs or the independent source-bound oracle. */
    for (unsigned mode = 0; mode < 3; mode++) {
        for (unsigned stop = 1; stop <= checkpoints; stop++) {
            c = control(0); stop_checkpoint = stop; stop_mode = mode;
            out = NULL; external_out = &out;
            int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                : mode == 1 ? EOVERFLOW : EINVAL;
            CHECK(wl_columnar_relation_copy_rows_controlled("seams", src, 0, 2,
                watched, &out, c) == expected && !out);
            external_out = NULL; stop_checkpoint = 0;
            if (mode == 2) {
                CHECK(restarted && !successor_callbacks
                    && old_report.result == 0 && old_report.consumed > 0);
                CHECK(finish(c, 0) == 0);
            } else {
                CHECK(!restarted && finish(c, expected) > 0);
            }
            unchanged(src, &saved); readers_released(src);
            CHECK(reserved() == 0
                && wl_columnar_memory_governor_ref_is_sole(watched));
            release(c);
        }
    }
    snapshot_free(&saved); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static unsigned phase_target, phase_occurrence, phase_seen, phase_mode;
static bool phase_hit;
static unsigned phase_counts[32];

static void
governor_phase_record(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)c; (void)token;
    CHECK(phase < sizeof(phase_counts) / sizeof(phase_counts[0]));
    phase_counts[phase]++;
}

static void
governor_phase(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (restarted) {
        successor_callbacks++;
        return;
    }
    if (phase != phase_target || ++phase_seen != phase_occurrence)
        return;
    phase_hit = true;
    if (phase_mode == 0)
        wl_evaluation_control_request_cancel(c);
    else if (phase_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else
        restart(c);
}

static void
governor_seams(void)
{
    const unsigned phases[] = {
        WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED,
        WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
        WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
        WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED,
        WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
        WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED,
        WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
        WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF,
    };
    col_rel_t *src = source(17, 2, true, true, NULL);
    source_snapshot_t saved = snapshot(src);
    watched = governor();
    memset(phase_counts, 0, sizeof(phase_counts));
    wl_evaluation_control_t *baseline = control(0);
    wl_columnar_memory_governor_test_checkpoint = governor_phase_record;
    col_rel_t *probe = NULL;
    CHECK(wl_columnar_relation_copy_rows_controlled("phase", src, 0, 2,
        watched, &probe, baseline) == 0 && probe);
    wl_columnar_memory_governor_test_checkpoint = NULL;
    CHECK(finish(baseline, 0) > 0 && col_rel_destroy_checked(probe) == 0);
    release(baseline);
    /* Enumerate actual D/M/P, gather and both capture reserve/commit phases;
     * VALIDATED/CLAIMED can occur in both APIs, so never assume six hits. */
    for (unsigned phase = 0; phase < sizeof(phases) / sizeof(phases[0]);
        phase++) {
        CHECK(phase_counts[phases[phase]] > 0);
        for (unsigned occurrence = 1; occurrence <= phase_counts[phases[phase]];
            occurrence++) {
            for (unsigned mode = 0; mode < 3; mode++) {
                wl_evaluation_control_t *c = control(0);
                phase_target = phases[phase]; phase_occurrence = occurrence;
                phase_seen = 0; phase_mode = mode; phase_hit = false;
                wl_columnar_memory_governor_test_checkpoint = governor_phase;
                col_rel_t *out = NULL;
                int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                    : mode == 1 ? EOVERFLOW : EINVAL;
                CHECK(wl_columnar_relation_copy_rows_controlled("phase", src,
                    0, 2, watched, &out, c) == expected && !out && phase_hit);
                wl_columnar_memory_governor_test_checkpoint = NULL;
                if (mode == 2) {
                    CHECK(restarted && !successor_callbacks
                        && old_report.result == 0 && old_report.consumed > 0);
                    CHECK(finish(c, 0) == 0);
                } else {
                    CHECK(finish(c, expected) > 0);
                }
                unchanged(src, &saved); readers_released(src);
                CHECK(reserved() == 0
                    && wl_columnar_memory_governor_ref_is_sole(watched));
                release(c);
            }
        }
    }
    snapshot_free(&saved); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static unsigned renewal_mode, renewal_hits;
static wl_evaluation_control_t *cleanup_control;
static bool cleanup_hit;

static void
renewal_stop(wl_columnar_relation_append_batch_t *batch)
{
    CHECK(batch->phase == WL_COLUMNAR_RELATION_BATCH_ENDED
        && !batch->raw.active && !batch->raw.single.set.identity
        && batch->raw.relation->nrows == 128
        && !batch->raw.relation->mem_ledger);
    renewal_hits++;
    if (renewal_mode == 0)
        wl_evaluation_control_request_cancel(batch->control);
    else if (renewal_mode == 1)
        CHECK(wl_evaluation_control_charge(batch->control, UINT64_MAX)
            == EOVERFLOW);
    else
        restart(batch->control);
}

static int
release_restart(wl_columnar_relation_mutation_single_t *single, unsigned slot)
{
    if (slot == 1 && !cleanup_hit) {
        CHECK(single->set.identity == (uintptr_t)&single->set
            && single->set.owners_acquired &&
            single->role.relation->nrows == 2);
        cleanup_hit = true;
        restart(cleanup_control);
    }
    return 0;
}

static void
source_release_restart(const col_rel_t *src)
{
    CHECK(src && !cleanup_hit);
    cleanup_hit = true;
    restart(cleanup_control);
}

static void
renewal_and_cleanup(void)
{
    col_rel_t *src = source(17, 133, true, true, NULL);
    source_snapshot_t saved = snapshot(src);
    watched = governor();
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_evaluation_control_t *c = control(0);
        renewal_mode = mode; renewal_hits = 0;
        wl_columnar_relation_test_batch_renewed = renewal_stop;
        col_rel_t *out = NULL;
        int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
            : mode == 1 ? EOVERFLOW : EINVAL;
        CHECK(wl_columnar_relation_copy_rows_controlled("renew", src, 0, 133,
            watched, &out, c) == expected && !out && renewal_hits == 1);
        wl_columnar_relation_test_batch_renewed = NULL;
        if (mode == 2) {
            CHECK(restarted && !successor_callbacks && old_report.consumed > 0);
            CHECK(finish(c, 0) == 0);
        } else {
            CHECK(finish(c, expected) > 0);
        }
        unchanged(src, &saved); readers_released(src);
        CHECK(reserved() == 0
            && wl_columnar_memory_governor_ref_is_sole(watched));
        release(c);
    }
    for (unsigned seam = 0; seam < 2; seam++) {
        cleanup_control = control(0); cleanup_hit = false;
        if (seam == 0)
            wl_columnar_relation_test_single_release = release_restart;
        else
            wl_columnar_relation_test_new_like_before_release =
                source_release_restart;
        col_rel_t *out = NULL;
        CHECK(wl_columnar_relation_copy_rows_controlled("retire", src, 0, 2,
            watched, &out, cleanup_control) == EINVAL && !out && cleanup_hit);
        wl_columnar_relation_test_single_release = NULL;
        wl_columnar_relation_test_new_like_before_release = NULL;
        CHECK(restarted && !successor_callbacks && old_report.result == 0
            && old_report.consumed > 0 && finish(cleanup_control, 0) == 0);
        unchanged(src, &saved); readers_released(src);
        CHECK(reserved() == 0
            && wl_columnar_memory_governor_ref_is_sole(watched));
        release(cleanup_control); cleanup_control = NULL;
    }
    snapshot_free(&saved); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

static void
allocations_and_budget(void)
{
    col_rel_t *src = source(17, 133, true, true, NULL);
    source_snapshot_t saved = snapshot(src);
    watched = governor();
    for (unsigned family = 0; family < 2; family++) {
        bool completed = false;
        for (uint32_t ordinal = 1; ordinal < 512; ordinal++) {
            wl_evaluation_control_t *c = control(0);
            if (family == 0)
                wl_columnar_relation_test_constructor_fail_allocation = ordinal;
            else
                wl_columnar_relation_test_growth_fail_allocation = ordinal;
            col_rel_t *out = NULL;
            int rc = wl_columnar_relation_copy_rows_controlled("allocation",
                    src,
                    0, 133, watched, &out, c);
            wl_columnar_relation_test_constructor_fail_allocation = 0;
            wl_columnar_relation_test_growth_fail_allocation = 0;
            if (!rc) {
                CHECK(out && ordinal > 20 && finish(c, 0) > 0);
                CHECK(col_rel_destroy_checked(out) == 0);
                completed = true;
            } else {
                CHECK(rc == ENOMEM && !out && finish(c, ENOMEM) > 0);
            }
            unchanged(src, &saved); readers_released(src);
            CHECK(reserved() == 0
                && wl_columnar_memory_governor_ref_is_sole(watched));
            release(c);
            if (completed)
                break;
        }
        CHECK(completed);
    }
    col_rel_t *probe = NULL;
    CHECK(wl_columnar_relation_new_like_governed_checked_mode("copy", src,
        watched, false, &probe) == 0 && probe);
    uint64_t d = probe->descriptor_reserved_bytes;
    uint64_t m = probe->metadata_reserved_bytes;
    uint64_t p = probe->retained_reserved_bytes;
    CHECK(d && m && p && col_rel_destroy_checked(probe) == 0 &&
        reserved() == 0);
    const uint64_t scratch = 17 * sizeof(int64_t);
    const uint64_t budgets[] = {
        d - 1, d + m - 1, d + m + p - 1,
        d + m + p + scratch - 1, d + m + p + 2 * scratch - 1
    };
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(watched);
    for (unsigned i = 0; i < sizeof(budgets) / sizeof(budgets[0]); i++) {
        atomic_store_explicit(&g->usable_bytes, budgets[i],
            memory_order_relaxed);
        wl_evaluation_control_t *c = control(0);
        col_rel_t *out = NULL;
        int expected = i < 3 ? ENOSPC : ENOMEM;
        CHECK(wl_columnar_relation_copy_rows_controlled("copy", src, 0, 1,
            watched, &out, c) == expected && !out);
        CHECK(finish(c, expected) > 0 && reserved() == 0
            && wl_columnar_memory_governor_ref_is_sole(watched));
        unchanged(src, &saved); readers_released(src); release(c);
    }
    /* Raw metadata replacement has a different temporary peak. Run this
     * semantic reference under an ample budget; its scratch is independent. */
    atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
        memory_order_relaxed);
    col_rel_t *raw_src = source(17, 133, true, true, NULL);
    col_rel_t *raw = NULL;
    CHECK(wl_columnar_relation_copy_rows("copy", raw_src, 0, 1, watched,
        &raw) == 0 && raw);
    CHECK(col_rel_destroy_checked(raw) == 0
        && col_rel_destroy_checked(raw_src) == 0 && reserved() == 0);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(g, 17, &peer)
        && wl_columnar_memory_commit(&peer, &peer));
    wl_evaluation_control_t *c = control(0);
    col_rel_t *out = NULL;
    CHECK(wl_columnar_relation_copy_rows_controlled("copy", src, 0, 1,
        watched, &out, c) == 0 && out && finish(c, 0) > 0);
    CHECK(reserved() == 17 + d + m + p
        && peak >= 17 + d + m + p + 2 * scratch);
    CHECK(col_rel_destroy_checked(out) == 0 && reserved() == 17);
    CHECK(wl_columnar_memory_release(&peer) && reserved() == 0
        && wl_columnar_memory_governor_ref_is_sole(watched));
    unchanged(src, &saved); readers_released(src); release(c);
    snapshot_free(&saved); CHECK(col_rel_destroy_checked(src) == 0);
    wl_columnar_memory_governor_ref_release(watched); watched = NULL;
}

#ifndef _WIN32
static int death_pipe;

static int
release_refused(wl_columnar_relation_mutation_single_t *single, unsigned slot)
{
    if (slot == 1) {
        CHECK(single->set.identity == (uintptr_t)&single->set
            && single->set.descriptors_acquired == 1
            && single->set.owners_acquired == 1
            && single->role.relation->nrows == 1
            && single->role.relation->columns
            && single->role.relation->memory_governor && reserved() > 0);
        CHECK(write(death_pipe, "H", 1) == 1);
    }
    return EIO;
}

static void
draining_fail_stop(void)
{
    int pipe_fds[2];
    CHECK(pipe(pipe_fds) == 0);
    pid_t child = fork(); CHECK(child >= 0);
    if (child == 0) {
        close(pipe_fds[0]); death_pipe = pipe_fds[1];
        struct rlimit no_core = {0, 0};
        CHECK(setrlimit(RLIMIT_CORE, &no_core) == 0);
        col_rel_t *src = source(17, 1, true, false, NULL);
        watched = governor();
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_test_single_release = release_refused;
        col_rel_t *out = NULL;
        (void)wl_columnar_relation_copy_rows_controlled("death", src, 0, 1,
            watched, &out, c);
        _exit(99);
    }
    close(pipe_fds[1]);
    char witness = 0;
    CHECK(read(pipe_fds[0], &witness, 1) == 1 && witness == 'H');
    close(pipe_fds[0]);
    int status;
    CHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}
#endif

int
main(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    twins(); domain_and_raw(); shape_and_busy(); borrowed_sources();
    invalid_float();
    literal_nullary();
    all_work_seams(); governor_seams();
    renewal_and_cleanup();
    allocations_and_budget();
#ifndef _WIN32
    draining_fail_stop();
#endif
    puts("evaluation_copy: OK");
    return 0;
}
