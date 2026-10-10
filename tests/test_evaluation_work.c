/* #1821: bounded real scalar materialization, before engine integration. */
#define WL_TEST_RELATION_RESIZE_HOOK 1
#include "../wirelog/columnar/internal.h"
#include <stdio.h>
#include <limits.h>
#include <math.h>
#include <float.h>
#include "../wirelog/intern.h"
#include "../wirelog/string_ops.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/thread.h"
#include <xxhash.h>
#ifdef WL_MBEDTLS_ENABLED
#include <psa/crypto.h>
#endif

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

static int owner;
static uint64_t seen, cancel_at, max_chunk;
static void
checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    if (work > max_chunk)
        max_chunk = work;
    seen += work;
    if (cancel_at && seen >= cancel_at)
        wl_evaluation_control_request_cancel(control);
}

static col_rel_t *
relation(uint32_t width, uint32_t capacity, uint32_t rows, bool timestamps,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *r = NULL;
    bool local_governor = governor == NULL;
    if (local_governor) {
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        };
        governor = wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor != NULL);
    }
    CHECK(wl_columnar_relation_new_auto_governed("work", width, capacity,
        timestamps, governor, &r) == 0);
    CHECK(r != NULL);
    CHECK((uintptr_t)&r->storage_alias_borrows % 8 == 0);
    if (local_governor)
        wl_columnar_memory_governor_ref_release(governor);
    r->nrows = rows;
    for (uint32_t c = 0; c < width; c++)
        for (uint32_t row = 0; row < rows; row++)
            r->columns[c][row] = (int64_t)c * 10000 + row;
    if (timestamps)
        for (uint32_t row = 0; row < rows; row++) {
            r->timestamps[row].iteration = row;
            r->timestamps[row].stratum = row + 1;
            r->timestamps[row].worker = row + 2;
            r->timestamps[row]._reserved = 0;
            r->timestamps[row].multiplicity = -(int64_t)row;
        }
    return r;
}

static void
destroy(col_rel_t *r)
{
    CHECK(col_rel_destroy_checked(r) == 0);
}

static wl_evaluation_control_t *
control(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(limit, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    seen = max_chunk = cancel_at = 0;
    return c;
}

static uint64_t
finish(wl_evaluation_control_t *c, int result)
{
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_finish(c, &owner,
        result > 0 ? result : 0, 0) == result);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.result == result);
    return report.consumed;
}

static void
release(wl_evaluation_control_t *c)
{
    CHECK(wl_evaluation_control_detach(c, &owner) == 0);
    wl_evaluation_control_release(c);
}

static void
retry(wl_evaluation_control_t *c, uint64_t limit)
{
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_configure(c, limit) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner) == 0);
    seen = max_chunk = cancel_at = 0;
}

static void
released(col_rel_t *r)
{
    wl_columnar_source_access_writer_t token = { 0 };
    CHECK(wl_columnar_source_access_writer_acquire(&r->source_access,
        &token) == 0);
    CHECK(wl_columnar_source_access_writer_release(&token) == 0);
}

static void
payload_equal(col_rel_t *dst, col_rel_t *src, uint32_t base)
{
    for (uint32_t c = 0; c < src->ncols; c++)
        for (uint32_t row = 0; row < src->nrows; row++)
            CHECK(dst->columns[c][base + row] == src->columns[c][row]);
    if (src->timestamps && dst->timestamps)
        for (uint32_t row = 0; row < src->nrows; row++) {
            col_delta_timestamp_t *a = &dst->timestamps[base + row];
            col_delta_timestamp_t *b = &src->timestamps[row];
            CHECK(a->iteration == b->iteration && a->stratum == b->stratum);
            CHECK(a->worker == b->worker && a->_reserved == b->_reserved);
            CHECK(a->multiplicity == b->multiplicity);
        }
}

static void
test_exact_and_denied(uint32_t width, uint32_t rows, bool timestamps)
{
    uint64_t cost = 22 + width + (uint64_t)width * rows
        + (timestamps ? (uint64_t)rows * 5 : 0);
    col_rel_t *src = relation(width, rows, rows, timestamps, NULL);
    col_rel_t *dst = relation(width, rows + 1, 1, timestamps, NULL);
    wl_evaluation_control_t *c = control(cost - 1);
    uint64_t generation = dst->view_generation;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
    released(src); released(dst);
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost && seen == cost);
    CHECK(dst->nrows == rows + 1 && dst->view_generation == generation + 1);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
}

static void
test_cancel_and_retained(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g != NULL);
    col_rel_t *src = relation(2, 300, 300, true, g);
    col_rel_t *dst = relation(2, 301, 1, true, g);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t retained = atomic_load(&governor->reserved_bytes);
    uint64_t generation = dst->view_generation;
    wl_evaluation_control_t *c = control(0);
    cancel_at = 272; /* Metadata 2 + first real copy chunk 256. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 272);
    CHECK(max_chunk == 256 && dst->nrows == 1);
    CHECK(dst->view_generation == generation);
    CHECK(atomic_load(&governor->reserved_bytes) == retained);
    released(src); released(dst);
    retry(c, 0);
    /* Last timestamp chunk also has a tail poll before publication. */
    cancel_at = 14 + 2 + 600 + 1500;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cancel_at);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 2124);
    payload_equal(dst, src, 1);
    CHECK(atomic_load(&governor->reserved_bytes) == retained);
    release(c); destroy(dst); destroy(src);
    CHECK(atomic_load(&governor->reserved_bytes) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_same_owner_and_null(void)
{
    col_rel_t *r = relation(1, 8, 3, true, NULL);
    wl_evaluation_control_t *c = control(40);
    CHECK(wl_columnar_relation_append_all_controlled(r, r, NULL, c) == 0);
    CHECK(finish(c, 0) == 40 && r->nrows == 6);
    for (uint32_t row = 0; row < 3; row++) {
        CHECK(r->columns[0][row + 3] == r->columns[0][row]);
        CHECK(r->timestamps[row + 3].iteration == row);
    }
    released(r);
    release(c); destroy(r);
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, NULL);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        NULL) == 0);
    CHECK(dst->nrows == 3); payload_equal(dst, src, 1);
    destroy(dst); destroy(src);
}

static void
test_types(void)
{
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 3, 1, false, NULL);
    src->nrows = dst->nrows = 0;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    CHECK(col_rel_set_column_types(dst, &type, 1) == 0);
    src->nrows = 2; dst->nrows = 1;
    src->columns[0][0] = INT64_MIN; /* binary64 negative zero */
    src->columns[0][1] = wl_columnar_float_to_bits(2.5);
    wl_evaluation_control_t *c = control(29); /* type+validation+copy+normalize */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 29);
    CHECK(dst->columns[0][1] == 0 && dst->columns[0][2] == src->columns[0][1]);
    dst->nrows = 1;
    src->columns[0][0] = wl_columnar_float_to_bits(NAN);
    uint64_t generation = dst->view_generation;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 16);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    released(src); released(dst);
    src->columns[0][0] = 0;
    src->nrows = 0;
    wirelog_column_type_t integer = WIRELOG_TYPE_INT64;
    CHECK(col_rel_set_column_types(src, &integer, 1) == 0);
    src->nrows = 2;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 15);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    release(c); destroy(dst); destroy(src);
}

static void
test_integer_family_and_empty(void)
{
    col_rel_t *src = relation(1, 2, 1, false, NULL);
    col_rel_t *dst = relation(1, 2, 1, false, NULL);
    wirelog_column_type_t integer = WIRELOG_TYPE_INT32;
    dst->nrows = 0;
    CHECK(col_rel_set_column_types(dst, &integer, 1) == 0);
    dst->nrows = 1;
    wl_evaluation_control_t *c = control(24);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 24);
    payload_equal(dst, src, 1);
    src->nrows = 0;
    uint64_t generation = dst->view_generation;
    retry(c, 15);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 15 && dst->view_generation == generation);
    retry(c, 0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    release(c); destroy(dst); destroy(src);
    src = relation(0, 3, 2, false, NULL);
    dst = relation(0, 3, 1, false, NULL);
    c = control(22);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 22 && dst->nrows == 3);
    release(c); destroy(dst); destroy(src);
}

static void
test_real_alias_and_gate(void)
{
    col_rel_t *src = relation(1, 8, 2, false, NULL);
    col_rel_t *dst = relation(1, 8, 1, false, NULL);
    col_rel_t *alias = relation(1, 8, 0, false, NULL);
    CHECK(col_rel_install_shared_view(alias, dst) == 0);
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 14 && dst->nrows == 1);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(alias, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0 && alias->nrows == 1);
    destroy(alias);
    wl_columnar_source_access_reader_t token = { 0 };
    CHECK(wl_columnar_source_access_reader_acquire(&dst->source_access,
        &token) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EBUSY);
    CHECK(finish(c, EBUSY) == 14);
    /* Failed destination admission must retire the already acquired source. */
    released(src);
    CHECK(wl_columnar_source_access_reader_release(&token) == 0);
    released(dst);
    src->nrows = src->capacity + 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 14);
    src->storage_owner = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 6 && !src->storage_owner);
    src->storage_owner = src;
    src->nrows = 2;
    release(c); destroy(dst); destroy(src);
}

static void
test_malformed_plain_compound_metadata(void)
{
    col_rel_t *src = relation(1, 4, 2, false, NULL);
    col_rel_t *dst = relation(1, 4, 1, false, NULL);
    uint64_t generation = dst->view_generation;
    int64_t value = dst->columns[0][0];
    wl_evaluation_control_t *c = control(0);
    uint32_t *fields[] = { &src->compound_count,
                           &src->inline_physical_offset, &dst->compound_count,
                           &dst->inline_physical_offset };
    for (uint32_t i = 0; i < 4; i++) {
        if (i)
            retry(c, 0);
        *fields[i] = 1;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
            == EINVAL);
        CHECK(finish(c, EINVAL) == 0 && seen == 0);
        CHECK(dst->nrows == 1 && dst->view_generation == generation);
        CHECK(dst->columns[0][0] == value && src->nrows == 2);
        released(src); released(dst);
        *fields[i] = 0;
    }
    release(c); destroy(dst); destroy(src);
}

static void
test_rejected_shapes(void)
{
    col_rel_t *src = relation(1, 4, 4, false, NULL);
    col_rel_t *dst = relation(1, 4, 1, false, NULL);
    wl_evaluation_control_t *c = control(0);
    dst->arena_owned = true;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0 && dst->nrows == 1);
    dst->arena_owned = false;
    src->nrows = 1;
    dst->compound_arity_len = UINT32_MAX;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0);
    dst->compound_arity_len = 0;
    bool shared = true;
    dst->col_shared = &shared;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0);
    dst->col_shared = NULL;
    dst->nrows = UINT32_MAX;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 14);
    dst->nrows = 1;
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    src->nrows = 0;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    src->nrows = 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) > 0 && dst->column_types != NULL);
    released(src); released(dst);
    release(c); destroy(dst); destroy(src);
}

typedef struct {
    int64_t **columns;
    col_delta_timestamp_t *timestamps;
    uint32_t capacity, timestamp_capacity, rows;
    uint64_t view, storage, reserved, credit;
} growth_snapshot_t;

static growth_snapshot_t
snapshot(col_rel_t *r)
{
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(r->memory_governor);
    return (growth_snapshot_t){ r->columns, r->timestamps, r->capacity,
                                r->timestamp_capacity, r->nrows,
                                r->view_generation,
                                r->storage_generation,
                                atomic_load(&g->reserved_bytes),
                                r->retained_reserved_bytes };
}

static void
unchanged(col_rel_t *r, growth_snapshot_t before)
{
    growth_snapshot_t now = snapshot(r);
    CHECK(now.columns == before.columns && now.timestamps == before.timestamps);
    CHECK(now.capacity == before.capacity &&
        now.timestamp_capacity == before.timestamp_capacity);
    CHECK(now.rows == before.rows && now.view == before.view &&
        now.storage == before.storage);
    CHECK(now.reserved == before.reserved && now.credit == before.credit);
    for (uint32_t c = 0; c < r->ncols; c++)
        for (uint32_t row = 0; row < r->nrows; row++)
            CHECK(r->columns[c][row] == (int64_t)c * 10000 + row);
    if (r->timestamps)
        for (uint32_t row = 0; row < r->nrows; row++)
            CHECK(r->timestamps[row].iteration == row);
}

static void
test_growth_exact_and_cancel(void)
{
    col_rel_t *src = relation(2, 300, 300, true, NULL);
    col_rel_t *dst = relation(2, 300, 300, true, NULL);
    col_rel_t *reference = relation(2, 300, 300, true, NULL);
    uint64_t cost = 4321; /* 2 metadata, 1 doubling, 4 allocations, 1200 cells, 3000 fields. */
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
    unchanged(dst, before); released(dst); released(src);
    const uint64_t boundaries[] = { 290, 2736, 4236 };
    for (uint32_t i = 0; i < 3; i++) {
        retry(c, 0);
        cancel_at = boundaries[i];
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == boundaries[i]);
        CHECK(max_chunk <= 256);
        unchanged(dst, before); released(dst); released(src);
    }
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost && seen == cost);
    CHECK(dst->columns != before.columns &&
        dst->timestamps != before.timestamps);
    CHECK(dst->capacity == 600 && dst->nrows == 600);
    CHECK(dst->storage_generation == before.storage + 1 &&
        dst->view_generation == before.view + 1);
    CHECK(wl_columnar_relation_append_all_controlled(reference, src, NULL,
        NULL) == 0);
    payload_equal(dst, reference, 0);
    release(c); destroy(reference); destroy(dst); destroy(src);
}

static void
test_growth_wide_and_same_owner(void)
{
    col_rel_t *src = relation(300, 2, 2, false, NULL);
    col_rel_t *dst = relation(300, 1, 1, false, NULL);
    /* Capacity1->4 takes two doublings, table1 and columns300. */
    uint64_t cost = 14 + 300 + 2 + 1 + 300 + 300 + 600 + 100;
    wl_evaluation_control_t *c = control(cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
    dst = relation(1, 3, 3, true, NULL);
    c = control(154);
    CHECK(wl_columnar_relation_append_all_controlled(dst, dst, NULL, c) == 0);
    CHECK(finish(c, 0) == 154 && dst->nrows == 6);
    for (uint32_t row = 0; row < 3; row++) {
        CHECK(dst->columns[0][row] == dst->columns[0][row + 3]);
        CHECK(dst->timestamps[row].multiplicity ==
            dst->timestamps[row + 3].multiplicity);
    }
    released(dst); release(c); destroy(dst);
    src = relation(0, 3, 2, false, NULL);
    dst = relation(0, 1, 1, false, NULL);
    c = control(88);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 88 && dst->nrows == 3 && dst->capacity == 4);
    release(c); destroy(dst); destroy(src);
}

static void
test_growth_float_and_timestamp_shortage(void)
{
    col_rel_t *src = relation(1, 2, 0, true, NULL);
    col_rel_t *dst = relation(1, 1, 0, true, NULL);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    CHECK(col_rel_set_column_types(dst, &type, 1) == 0);
    src->nrows = 2; dst->nrows = 1;
    src->columns[0][0] = INT64_MIN;
    src->columns[0][1] = wl_columnar_float_to_bits(3.5);
    dst->columns[0][0] = wl_columnar_float_to_bits(1.5);
    wl_evaluation_control_t *c = control(142); /* meta1+validation2+doubling2+alloc3+old1+new2+norm2+timestamp15 */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 142);
    CHECK(dst->columns[0][0] == wl_columnar_float_to_bits(1.5));
    CHECK(dst->columns[0][1] == 0 && dst->columns[0][2] == src->columns[0][1]);
    release(c); destroy(dst); destroy(src);
    src = relation(1, 2, 2, true, NULL);
    dst = relation(1, 4, 1, false, NULL);
    CHECK(col_rel_enable_timestamps(dst) == 0);
    /* Valid independent timestamp storage can be smaller than the column grid.
    * Construct its private owner and retire surplus credit before the test. */
    col_delta_timestamp_t *small = calloc(1, sizeof(*small));
    CHECK(small != NULL);
    free(dst->timestamps);
    dst->timestamps = small;
    dst->timestamp_capacity = 1;
    col_rel_retire_payload_credit(dst);
    uint64_t generation = dst->storage_generation;
    c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 136); /* metadata1+alloc3+old1+new2+timestamp15 */
    CHECK(dst->capacity == 4 && dst->timestamp_capacity == 4);
    CHECK(dst->storage_generation == generation + 1);
    release(c); destroy(dst); destroy(src);
}

static void
test_growth_auxiliary_and_ledger(void)
{
    col_rel_t *src = relation(2, 2, 2, true, NULL);
    col_rel_t *dst = relation(2, 1, 1, true, NULL);
    CHECK(col_rel_reserve_merge_grid(dst, 3) == 0);
    int64_t **merge = dst->merge_columns;
    int64_t *scratch = dst->row_scratch;
    merge[0][0] = 99;
    uint64_t old_payload = dst->retained_reserved_bytes;
    uint64_t old_grid = 2 * sizeof(int64_t *) + 2 * sizeof(int64_t);
    uint64_t new_grid = 2 * sizeof(int64_t *) + 2 * 4 * sizeof(int64_t);
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    dst->mem_ledger = &ledger;
    col_rel_ledger_reconcile(dst, 0);
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(finish(c, ENOMEM) == 226);
    unchanged(dst, before);
    CHECK(dst->merge_columns == merge && dst->row_scratch == scratch);
    CHECK(atomic_load(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION]) == 16);
    CHECK(atomic_load(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]) ==
        sizeof(col_delta_timestamp_t));
    retry(c, 239);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 239);
    CHECK(dst->merge_columns == merge && dst->row_scratch == scratch &&
        merge[0][0] == 99);
    CHECK(dst->retained_reserved_bytes == old_payload - old_grid + new_grid
        + 3 * sizeof(col_delta_timestamp_t));
    CHECK(atomic_load(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION]) == 64);
    CHECK(atomic_load(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]) ==
        4 * sizeof(col_delta_timestamp_t));
    release(c); destroy(dst);
    CHECK(atomic_load(&ledger.current_bytes) == 0);
    destroy(src);
}

static void
test_growth_faults(void)
{
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, NULL);
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    uint64_t usable = atomic_load(&g->usable_bytes);
    atomic_store(&g->usable_bytes, before.reserved);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 29); /* metadata + doubling2, before allocation */
    unchanged(dst, before); released(dst); released(src);
    atomic_store(&g->usable_bytes, usable);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_prepare_resize();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 33);
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 109); /* Actual copied source and old prefix admitted. */
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    dst->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 14);
    dst->storage_generation = before.storage;
    unchanged(dst, before);
    retry(c, 122);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 122);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
}

static int metadata_cancel_phase;
static uint64_t metadata_cancel_progress, allocation_after_child;
static bool committed_seen, fail_payload_after_metadata,
    metadata_accounting_overflow;

static void
metadata_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    if (phase == WL_COLUMNAR_RELATION_METADATA_COMMITTED) {
        CHECK(progress != 0);
        committed_seen = true;
        if (metadata_accounting_overflow)
            CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
        if (fail_payload_after_metadata)
            wl_columnar_relation_test_fail_next_reservation_commit();
    }
    if (phase == WL_COLUMNAR_RELATION_METADATA_CHILD && allocation_after_child
        && progress == allocation_after_child)
        wl_columnar_relation_test_fail_next_metadata_alloc();
    if (phase == metadata_cancel_phase && progress >= metadata_cancel_progress)
        wl_evaluation_control_request_cancel(c);
}

static col_rel_t *
typed_source(uint32_t width, uint32_t rows, wirelog_column_type_t type,
    bool timestamps)
{
    col_rel_t *r = relation(width, rows, 0, timestamps, NULL);
    wirelog_column_type_t *types = malloc(width * sizeof(*types));
    CHECK(types != NULL);
    for (uint32_t c = 0; c < width; c++)
        types[c] = type;
    CHECK(col_rel_set_column_types(r, types, width) == 0);
    free(types);
    r->nrows = rows;
    for (uint32_t c = 0; c < width; c++)
        for (uint32_t row = 0; row < rows; row++)
            r->columns[c][row] = type == WIRELOG_TYPE_FLOAT
                ? wl_columnar_float_to_bits(row ? 1.5 : -0.0)
                : (int64_t)c * 10000 + row;
    return r;
}

static uint64_t
installation_cost(col_rel_t *dst, col_rel_t *src)
{
    uint64_t lengths = 0;
    for (uint32_t c = 0; c < dst->ncols; c++)
        lengths += strlen(dst->col_names &&
                dst->col_names[c] ? dst->col_names[c] : "") + 1;
    /* 8 width actions: compatibility, length-write, type-copy, zero-table,
     * child-init, format two chars, name allocation. Fixed6 covers scratch,
     * types table, root format3, child-pointer table. Visit/copy names includes NUL. */
    uint64_t cost = 14 + 8 * (uint64_t)dst->ncols + 2 * lengths + 6
        + 2 * (uint64_t)src->ncols * src->nrows
        + (uint64_t)dst->ncols * dst->nrows;
    for (uint32_t c = 0; c < src->ncols; c++)
        if (src->column_types[c] == WIRELOG_TYPE_FLOAT)
            cost += src->nrows;
    if (dst->timestamps && src->timestamps)
        cost += 5 * src->nrows;
    return cost + (dst->memory_governor ? 154u : 120u);
}

static void
schema_equal(col_rel_t *a, col_rel_t *b)
{
    CHECK(a->schema_ok && b->schema_ok &&
        a->schema.n_children == b->schema.n_children);
    CHECK(strcmp(a->schema.format, b->schema.format) == 0);
    for (uint32_t c = 0; c < a->ncols; c++) {
        CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(strcmp(a->schema.children[c]->format,
            b->schema.children[c]->format) == 0);
        CHECK(strcmp(a->schema.children[c]->name,
            b->schema.children[c]->name) == 0);
    }
}

static void
metadata_unchanged(col_rel_t *dst, growth_snapshot_t before,
    struct ArrowSchema old_schema, uint64_t metadata_credit)
{
    unchanged(dst, before);
    CHECK(dst->column_types == NULL);
    CHECK(dst->schema.children == old_schema.children &&
        dst->schema.format == old_schema.format);
    CHECK(dst->schema.n_children == old_schema.n_children);
    CHECK(dst->metadata_reserved_bytes == metadata_credit);
    released(dst);
}

static void
test_metadata_partial_construction(void)
{
    col_rel_t *src = typed_source(600, 1, WIRELOG_TYPE_INT32, false);
    col_rel_t *dst = relation(600, 4, 0, false, NULL);
    col_rel_t *reference = relation(600, 4, 0, false, NULL);
    growth_snapshot_t before = snapshot(dst);
    struct ArrowSchema old_schema = dst->schema;
    uint64_t metadata_credit = dst->metadata_reserved_bytes;
    uint64_t cost = installation_cost(dst, src);
    wl_evaluation_control_t *c = control(cost - 1);
    wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) < cost);
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    const int phases[] = { WL_COLUMNAR_RELATION_METADATA_TYPES,
                           WL_COLUMNAR_RELATION_METADATA_TABLE_ZERO,
                           WL_COLUMNAR_RELATION_METADATA_CHILD };
    const uint64_t progress[3][3] = { { 1, 300, 600 }, { 256, 512, 600 },
                                      { 1, 300, 600 } };
    for (uint32_t phase = 0; phase < 3; phase++)
        for (uint32_t point = 0; point < 3; point++) {
            retry(c, 0);
            metadata_cancel_phase = phases[phase];
            metadata_cancel_progress = progress[phase][point];
            CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
                == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == seen);
            metadata_unchanged(dst, before, old_schema, metadata_credit);
            released(src);
        }
    metadata_cancel_phase = 0;
    retry(c, 0);
    allocation_after_child = 300;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(finish(c, ENOMEM) > 0);
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    allocation_after_child = 0;
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(wl_columnar_relation_append_all_controlled(reference, src, NULL,
        NULL) == 0);
    schema_equal(dst, reference); payload_equal(dst, reference, 0);
    release(c); destroy(reference); destroy(dst); destroy(src);
    wl_columnar_relation_test_metadata_checkpoint = NULL;
}

static void
test_metadata_long_name_and_compatibility(void)
{
    char name[701]; memset(name, 'n', 700); name[700] = 0;
    const char *names[] = { name };
    col_rel_t *src = typed_source(1, 2, WIRELOG_TYPE_INT32, true);
    col_rel_t *dst = relation(0, 0, 0, false, NULL);
    col_rel_t *reference = relation(0, 0, 0, false, NULL);
    CHECK(col_rel_set_schema(dst, 1, names) == 0);
    CHECK(col_rel_set_schema(reference, 1, names) == 0);
    CHECK(col_rel_reserve_capacity_admitted(dst, 4, NULL) == 0);
    CHECK(col_rel_reserve_capacity_admitted(reference, 4, NULL) == 0);
    CHECK(col_rel_enable_timestamps(dst) == 0);
    CHECK(col_rel_enable_timestamps(reference) == 0);
    CHECK(strlen(dst->col_names[0]) == 700);
    CHECK(dst->timestamps != NULL && src->timestamps != NULL);
    growth_snapshot_t before = snapshot(dst);
    struct ArrowSchema old_schema = dst->schema;
    uint64_t metadata_credit = dst->metadata_reserved_bytes;
    uint64_t cost = installation_cost(dst, src);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
    metadata_cancel_phase = WL_COLUMNAR_RELATION_METADATA_NAME;
    const uint64_t points[] = { 256, 512, 701 };
    for (uint32_t i = 0; i < 3; i++) {
        if (i)
            retry(c, 0);
        metadata_cancel_progress = points[i];
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == seen);
        metadata_unchanged(dst, before, old_schema, metadata_credit);
    }
    metadata_cancel_phase = 0;
    retry(c, 0);
    cancel_at = cost; /* The final actual timestamp tail must poll. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(dst->columns[0][0] == 0 &&
        dst->column_types[0] == WIRELOG_TYPE_INT32);
    CHECK(wl_columnar_relation_append_all_controlled(reference, src, NULL,
        NULL) == 0);
    schema_equal(dst, reference); payload_equal(dst, reference, 0);
    release(c); destroy(reference); destroy(dst); destroy(src);
    wl_columnar_relation_test_metadata_checkpoint = NULL;
    src = typed_source(1, 1, WIRELOG_TYPE_FLOAT, false);
    dst = relation(1, 4, 1, false, NULL);
    before = snapshot(dst); old_schema = dst->schema;
    metadata_credit = dst->metadata_reserved_bytes;
    c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 15);
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    dst->nrows = 0;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 15 && dst->column_types == NULL);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        NULL) == EINVAL);
    src->nrows = 0;
    uint64_t generation = dst->view_generation;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 15 && dst->column_types == NULL);
    CHECK(dst->nrows == 0 && dst->view_generation == generation);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        NULL) == 0);
    release(c); destroy(dst); destroy(src);
}

static void
test_metadata_quota_provenance(void)
{
    col_rel_t *src = typed_source(1, 2, WIRELOG_TYPE_INT32, false);
    col_rel_t *dst = relation(1, 1, 1, false, NULL);
    growth_snapshot_t before = snapshot(dst);
    struct ArrowSchema old_schema = dst->schema;
    uint64_t metadata_credit = dst->metadata_reserved_bytes;
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    uint64_t usable = atomic_load(&g->usable_bytes);
    wl_evaluation_control_t *c = control(0);
    atomic_store(&g->usable_bytes, before.reserved);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOSPC);
    CHECK(dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOSPC) == 32); /* Compatibility + two source visits + old visit. */
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    atomic_store(&g->usable_bytes, before.reserved + sizeof(uint64_t));
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOSPC);
    CHECK(dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOSPC) == 54); /* Scratch admitted, actual name length gathered. */
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    uint64_t incoming_metadata = metadata_credit +
        sizeof(wirelog_column_type_t);
    atomic_store(&g->usable_bytes,
        before.reserved + incoming_metadata + sizeof(uint64_t));
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) > 11); /* Metadata staged, payload coexistence denied. */
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    atomic_store(&g->usable_bytes, usable);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 34);
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    release(c); destroy(dst); destroy(src);
}

static void
test_metadata_no_governor(void)
{
    col_rel_t *src = typed_source(1, 2, WIRELOG_TYPE_INT32, false);
    col_rel_t *dst = col_rel_new_auto("unmanaged", 1);
    CHECK(dst != NULL && dst->memory_governor == NULL);
    CHECK(col_rel_reserve_capacity_admitted(dst, 4, NULL) == 0);
    uint64_t cost = installation_cost(dst, src);
    wl_evaluation_control_t *c = control(cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost && dst->nrows == 2);
    CHECK(dst->column_types[0] == WIRELOG_TYPE_INT32 &&
        dst->metadata_reserved_bytes == 0);
    CHECK(dst->schema_ok && strcmp(dst->schema.children[0]->format, "l") == 0);
    release(c); destroy(dst); destroy(src);
}

static void
test_metadata_commit_order(bool growth)
{
    col_rel_t *src = typed_source(1, 2, WIRELOG_TYPE_INT32, false);
    col_rel_t *dst = relation(1, growth ? 1 : 4, 1, false, NULL);
    growth_snapshot_t before = snapshot(dst);
    struct ArrowSchema old_schema = dst->schema;
    uint64_t metadata_credit = dst->metadata_reserved_bytes;
    uint64_t cost = installation_cost(dst, src) + (growth ? 97 : 0);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
    committed_seen = false;
    wl_columnar_relation_test_fail_metadata_commit = true;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!committed_seen && !wl_columnar_relation_test_fail_metadata_commit);
    CHECK(finish(c, ENOMEM) == cost - 4 - (growth ? 13 : 0));
    metadata_unchanged(dst, before, old_schema, metadata_credit); released(src);
    if (growth) {
        retry(c, 0);
        committed_seen = false; fail_payload_after_metadata = true;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == ENOMEM);
        CHECK(committed_seen);
        CHECK(finish(c, ENOMEM) == cost - 13);
        metadata_unchanged(dst, before, old_schema, metadata_credit);
        released(src);
        fail_payload_after_metadata = false;
    }
    retry(c, cost); committed_seen = false;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(committed_seen && finish(c, 0) == cost);
    CHECK(dst->column_types[0] == WIRELOG_TYPE_INT32 && dst->nrows == 3);
    release(c); destroy(dst); destroy(src);
    wl_columnar_relation_test_metadata_checkpoint = NULL;
}

static wl_columnar_memory_governor_ref_t *
fresh_governor(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g != NULL);
    return g;
}

static void
governor_empty(wl_columnar_memory_governor_ref_t *g)
{
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == 0);
    CHECK(wl_columnar_memory_governor_ref_is_sole(g));
}

static uint64_t
constructor_cost(const char *name, uint32_t width, uint32_t capacity,
    bool timestamps)
{
    uint64_t length = strlen(name) + 1;
    uint64_t generated = 0;
    for (uint32_t c = 0; c < width; c++) {
        char text[32]; snprintf(text, sizeof(text), "col%u", c);
        generated += strlen(text) + 1;
    }
    return (width ? 2 * length + 11 + 12 * (uint64_t)width + 3 * generated
           + (timestamps && capacity ? 1 : 0)
        : 2 * length + 6 + (timestamps && capacity ? 1 : 0))
           + (width ? 84u : 46u + (timestamps && capacity ? 23u : 0u));
}

static void
constructor_equal(col_rel_t *a, col_rel_t *b)
{
    CHECK(a->ncols == b->ncols && a->capacity == b->capacity &&
        a->nrows == b->nrows);
    CHECK((a->column_types != NULL) == (b->column_types != NULL));
    CHECK(a->memory_governor == b->memory_governor);
    CHECK(a->pool_owned == b->pool_owned &&
        a->pool_name_reserved_bytes == b->pool_name_reserved_bytes);
    CHECK(a->declared_ncols == b->declared_ncols &&
        a->has_graph_column == b->has_graph_column &&
        a->graph_col_idx == b->graph_col_idx);
    CHECK((a->columns != NULL) == (b->columns != NULL));
    CHECK((a->col_names != NULL) == (b->col_names != NULL));
    CHECK((a->row_scratch != NULL) == (b->row_scratch != NULL));
    CHECK((a->timestamps != NULL) == (b->timestamps != NULL));
    CHECK(a->timestamp_capacity == b->timestamp_capacity);
    CHECK(a->schema_ok == b->schema_ok &&
        a->schema.n_children == b->schema.n_children);
    CHECK((a->schema.children != NULL) == (b->schema.children != NULL));
    CHECK(strcmp(a->schema.format, b->schema.format) == 0);
    CHECK(a->view_generation == b->view_generation &&
        a->storage_generation == b->storage_generation);
    CHECK(a->descriptor_reserved_bytes == b->descriptor_reserved_bytes);
    CHECK(a->metadata_reserved_bytes == b->metadata_reserved_bytes);
    CHECK(a->retained_reserved_bytes == b->retained_reserved_bytes);
    CHECK(a->relation_identity != 0 && a->storage_owner == a);
    CHECK(a->storage_owner_identity == a->relation_identity);
    for (uint32_t c = 0; c < a->ncols; c++) {
        CHECK(a->columns[c] != NULL && b->columns[c] != NULL);
        if (a->column_types)
            CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(strcmp(a->col_names[c], b->col_names[c]) == 0);
        CHECK(strcmp(a->schema.children[c]->name,
            b->schema.children[c]->name) == 0);
        CHECK(strcmp(a->schema.children[c]->format,
            b->schema.children[c]->format) == 0);
    }
    for (uint32_t row = 0; row < a->timestamp_capacity; row++) {
        CHECK(a->timestamps[row].iteration == 0 &&
            a->timestamps[row].stratum == 0);
        CHECK(a->timestamps[row].worker == 0 &&
            a->timestamps[row]._reserved == 0);
        CHECK(a->timestamps[row].multiplicity == 0);
    }
}

static int constructor_cancel_phase;
static uint64_t constructor_cancel_progress;
static bool identity_cancel_failure;
static uint64_t identity_failures, identity_successes, constructor_committed;

static void
constructor_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    if (phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_IDENTITY) {
        if (progress)
            identity_successes++;
        else {
            identity_failures++;
            if (identity_cancel_failure)
                wl_evaluation_control_request_cancel(c);
        }
    }
    if (phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED)
        constructor_committed = progress;
    if (phase == constructor_cancel_phase &&
        progress >= constructor_cancel_progress)
        wl_evaluation_control_request_cancel(c);
}

static col_rel_t *identity_winner;
static void
consume_identity(wl_evaluation_control_t *c, uint64_t expected)
{
    (void)c;
    wl_columnar_relation_test_before_identity_cas = NULL;
    identity_winner = col_rel_new_auto("identity-winner", 0);
    CHECK(identity_winner != NULL &&
        identity_winner->relation_identity == expected);
}

static void
test_constructor_matrix(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    const uint32_t widths[] = { 0, 2 };
    const uint32_t capacities[] = { 0, 4 };
    for (uint32_t w = 0; w < 2; w++)
        for (uint32_t cap = 0; cap < 2; cap++)
            for (uint32_t ts = 0; ts < 2; ts++) {
                col_rel_t *actual = NULL, *reference = NULL;
                uint64_t cost = constructor_cost("fresh", widths[w],
                        capacities[cap], ts != 0);
                wl_evaluation_control_t *c = control(cost);
                CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh",
                    widths[w],
                    capacities[cap], ts != 0, g, &actual, c) == 0);
                CHECK(finish(c, 0) == cost && seen == cost);
                CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh",
                    widths[w],
                    capacities[cap], ts != 0, g, &reference, NULL) == 0);
                constructor_equal(actual, reference);
                released(actual);
                release(c); destroy(reference); destroy(actual);
                governor_empty(g);
            }
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_constructor_wide_and_stops(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *actual = NULL, *reference = NULL;
    uint64_t cost = constructor_cost("fresh", 600, 4, true);
    CHECK(cost == 19578);
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 600, 4,
        true,
        g, &actual, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(actual == NULL && finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) < cost);
    governor_empty(g);
    wl_columnar_relation_test_constructor_checkpoint = constructor_checkpoint;
    const int phases[] = { WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMNS_ZERO,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_NAMES_ZERO,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_GENERATED_NAME };
    const uint64_t points[4][3] = { { 256, 512, 600 }, { 256, 512, 600 },
                                    { 1, 300, 600 }, { 1, 300, 600 } };
    for (uint32_t phase = 0; phase < 4; phase++)
        for (uint32_t point = 0; point < 3; point++) {
            retry(c, 0);
            constructor_cancel_phase = phases[phase];
            constructor_cancel_progress = points[phase][point];
            CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh",
                600, 4, true,
                g, &actual, c) == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(actual == NULL && finish(c,
                WL_EVALUATION_CONTROL_CANCELLED) == seen);
            governor_empty(g);
        }
    constructor_cancel_phase = 0;
    wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
    metadata_cancel_phase = WL_COLUMNAR_RELATION_METADATA_TABLE_ZERO;
    for (uint32_t point = 0; point < 3; point++) {
        retry(c, 0);
        metadata_cancel_progress = points[0][point];
        CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 600, 4,
            true,
            g, &actual, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(actual == NULL && finish(c,
            WL_EVALUATION_CONTROL_CANCELLED) == seen);
        governor_empty(g);
    }
    metadata_cancel_phase = WL_COLUMNAR_RELATION_METADATA_CHILD;
    metadata_cancel_progress = 300;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 600, 4,
        true,
        g, &actual, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == seen);
    governor_empty(g);
    metadata_cancel_phase = 0;
    wl_columnar_relation_test_metadata_checkpoint = NULL;
    constructor_cancel_phase = WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL;
    constructor_cancel_progress = 0;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 600, 4,
        true,
        g, &actual, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    governor_empty(g);
    constructor_cancel_phase = 0;
    retry(c, cost);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 600, 4,
        true,
        g, &actual, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(wl_columnar_relation_new_auto_governed("fresh", 600, 4, true, g,
        &reference) == 0);
    constructor_equal(actual, reference);
    release(c); destroy(reference); destroy(actual); governor_empty(g);
    wl_columnar_memory_governor_ref_release(g);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
}

static void
test_constructor_identity(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    wl_evaluation_control_t *c = control(0);
    col_rel_t *r = NULL;
    wl_columnar_relation_test_constructor_checkpoint = constructor_checkpoint;
    wl_columnar_relation_test_before_identity_cas = consume_identity;
    identity_failures = identity_successes = 0;
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
        false,
        g, &r, c) == 0);
    CHECK(identity_failures == 1 && identity_successes == 1);
    CHECK(finish(c, 0) == constructor_cost("fresh", 2, 4, false) + 1);
    CHECK(r->relation_identity > identity_winner->relation_identity);
    destroy(identity_winner); identity_winner = NULL; destroy(r);
    governor_empty(g);
    retry(c, 0);
    identity_cancel_failure = true;
    identity_failures = identity_successes = 0;
    wl_columnar_relation_test_before_identity_cas = consume_identity;
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
        false,
        g, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(r == NULL && identity_failures == 1 && identity_successes == 0);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 30);
    uint64_t next = identity_winner->relation_identity + 1;
    destroy(identity_winner); identity_winner = NULL; governor_empty(g);
    identity_cancel_failure = false;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
        false,
        g, &r, c) == 0);
    CHECK(r->relation_identity == next);
    CHECK(finish(c, 0) == constructor_cost("fresh", 2, 4, false));
    next = r->relation_identity + 1;
    destroy(r); governor_empty(g);
    /* A successful CAS followed by cancellation consumes that ID forever. */
    constructor_cancel_phase = WL_COLUMNAR_RELATION_CONSTRUCTOR_IDENTITY;
    constructor_cancel_progress = 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
        false,
        g, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 30);
    governor_empty(g);
    constructor_cancel_phase = 0;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
        false,
        g, &r, c) == 0);
    CHECK(r->relation_identity == next + 1);
    CHECK(finish(c, 0) == constructor_cost("fresh", 2, 4, false));
    next = r->relation_identity + 1;
    destroy(r); governor_empty(g);
    CHECK(col_rel_test_set_next_identity(UINT64_MAX) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
        false,
        g, &r, c) == EOVERFLOW);
    CHECK(r == NULL && finish(c, EOVERFLOW) == 29);
    governor_empty(g);
    CHECK(col_rel_test_set_next_identity(next) == 0);
    release(c); wl_columnar_memory_governor_ref_release(g);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
}

static void
test_constructor_faults_and_long_name(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    col_rel_t *r = NULL;
    wl_evaluation_control_t *c = control(0);
    const uint64_t limits[] = { 0, sizeof(col_rel_t) + 6,
                                sizeof(col_rel_t) + 6 + 2 * sizeof(uint64_t),
                                sizeof(col_rel_t) + 6 + 2 * sizeof(uint64_t) + 3
                                + 2 *
                                (sizeof(char *) + sizeof(struct ArrowSchema *) +
                                sizeof(struct ArrowSchema) + 2) + 20 };
    uint64_t usable = atomic_load(&governor->usable_bytes);
    for (uint32_t i = 0; i < 4; i++) {
        if (i)
            retry(c, 0);
        atomic_store(&governor->usable_bytes, limits[i]);
        CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
            false,
            g, &r, c) == ENOSPC);
        CHECK(r == NULL && finish(c, ENOSPC) > 0);
        governor_empty(g);
    }
    atomic_store(&governor->usable_bytes, usable);
    for (int64_t fail = 0; fail < 11; fail++) {
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = fail;
        CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
            true,
            g, &r, c) == ENOMEM);
        CHECK(r == NULL && finish(c, ENOMEM) > 0);
        governor_empty(g);
    }
    wl_columnar_relation_test_constructor_checkpoint = constructor_checkpoint;
    for (int fail = 1; fail <= 3; fail++) {
        retry(c, 0);
        constructor_committed = 0;
        wl_columnar_relation_test_constructor_fail_commit = fail;
        CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", 2, 4,
            true,
            g, &r, c) == ENOMEM);
        CHECK(constructor_committed == (uint64_t)fail - 1);
        CHECK(r == NULL && finish(c, ENOMEM) > 0);
        governor_empty(g);
    }
    retry(c, 0);
    CHECK(wl_columnar_relation_new_auto_governed_controlled("fresh", UINT32_MAX,
        UINT32_MAX, true, g, &r, c) == EOVERFLOW);
    CHECK(r == NULL && finish(c, EOVERFLOW) == 6);
    governor_empty(g);
    char name[701]; memset(name, 'r', 700); name[700] = 0;
    constructor_cancel_phase = WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME;
    const uint64_t points[] = { 256, 512, 701 };
    for (uint32_t i = 0; i < 3; i++) {
        retry(c, 0);
        constructor_cancel_progress = points[i];
        CHECK(wl_columnar_relation_new_auto_governed_controlled(name, 2, 4,
            true,
            g, &r, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == seen);
        governor_empty(g);
    }
    constructor_cancel_phase = 0;
    retry(c, constructor_cost(name, 2, 4, true));
    CHECK(wl_columnar_relation_new_auto_governed_controlled(name, 2, 4, true,
        g, &r, c) == 0);
    CHECK(finish(c, 0) == constructor_cost(name, 2, 4, true));
    CHECK(strcmp(r->name, name) == 0);
    destroy(r); governor_empty(g);
    release(c); wl_columnar_memory_governor_ref_release(g);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
}

static uint64_t
new_like_cost(const char *name, const col_rel_t *src, bool governed,
    bool preserve_timestamps)
{
    uint64_t n = strlen(name) + 1, lengths = 0, named = 0;
    for (uint32_t i = 0; i < src->ncols; i++) {
        char text[32];
        const char *column = src->col_names ? src->col_names[i] : NULL;
        if (column)
            named++;
        else {
            snprintf(text, sizeof(text), "col%u", i);
            column = text;
        }
        lengths += strlen(column) + 1;
    }
    uint64_t cost = src->ncols
        ? 2 * n + 11 + 14 * (uint64_t)src->ncols + 3 * lengths - 2 * named
        : 2 * n + 6;
    cost += 9 + 6 + (preserve_timestamps && src->timestamps ? 1 : 0);
    if (src->ncols && !governed)
        cost--;
    if (src->ncols && src->column_types)
        cost += 1 + src->ncols;
    if (governed)
        cost += src->ncols ? 84u : 46u + (preserve_timestamps &&
            src->timestamps ? 23u : 0u);
    return cost;
}

static void
source_unpinned(col_rel_t *src, col_rel_t *canonical)
{
    CHECK(atomic_load(&src->descriptor_access.state) == 0);
    CHECK(atomic_load(&canonical->source_access.state) == 0);
}

static void
test_new_like_matrix(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    for (uint32_t width_mode = 0; width_mode < 2; width_mode++)
        for (uint32_t typed = 0; typed < 2; typed++)
            for (uint32_t timestamped = 0; timestamped < 2; timestamped++) {
                uint32_t width = width_mode ? 3 : 0;
                col_rel_t *src = relation(width, 4, 0, timestamped != 0, g);
                wirelog_column_type_t types[] = {
                    WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT32
                };
                if (typed) {
                    CHECK(col_rel_set_column_types(src, types, width) == 0);
                    /* Zero-width legacy still touches view for a nonNULL input
                     * type pointer, but returns a NULL result type array. */
                    if (!width)
                        src->column_types = types;
                }
                src->declared_ncols = 17;
                src->has_graph_column = width != 0;
                src->graph_col_idx = width ? 2 : 41;
                if (width) {
                    src->nrows = 1;
                    src->columns[0][0] = INT64_C(0x7ff8000000000001);
                }
                uint64_t baseline = atomic_load(&
                        wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
                uint64_t view = src->view_generation,
                    storage = src->storage_generation;
                for (uint32_t governed = 0; governed < 2; governed++)
                    for (uint32_t preserve = 0; preserve < 2; preserve++) {
                        col_rel_t *actual = NULL, *reference = NULL;
                        wl_evaluation_control_t *c = control(new_like_cost(
                                    "empty", src, governed != 0,
                                    preserve != 0));
                        int matrix_rc =
                            wl_columnar_relation_new_like_governed_checked_mode_controlled
                            (
                            "empty", src, governed ? g : NULL, preserve != 0,
                            &actual, c);
                        CHECK(matrix_rc == 0);
                        CHECK(finish(c, 0) == new_like_cost("empty", src,
                            governed != 0, preserve != 0));
                        CHECK(
                            wl_columnar_relation_new_like_governed_checked_mode_controlled
                            (
                                "empty", src, governed ? g : NULL,
                                preserve != 0,
                                &reference, NULL) == 0);
                        constructor_equal(actual, reference);
                        CHECK(actual->nrows == 0 &&
                            actual->capacity == COL_REL_INIT_CAP);
                        CHECK(actual->memory_governor == (governed ? g : NULL));
                        CHECK((actual->row_scratch != NULL) == (governed &&
                            width));
                        CHECK((actual->timestamps != NULL) == (preserve &&
                            timestamped));
                        if (width)
                            CHECK(src->columns[0][0] ==
                                INT64_C(0x7ff8000000000001));
                        CHECK(src->view_generation == view &&
                            src->storage_generation == storage);
                        source_unpinned(src, src);
                        destroy(actual); destroy(reference); release(c);
                        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                                g)->reserved_bytes) == baseline);
                    }
                if (typed && !width)
                    src->column_types = NULL;
                destroy(src); governor_empty(g);
            }
    col_rel_t *unmanaged = col_rel_new_auto("unmanaged-source", 2);
    CHECK(unmanaged != NULL && unmanaged->memory_governor == NULL);
    for (uint32_t governed = 0; governed < 2; governed++) {
        col_rel_t *actual = NULL, *reference = NULL;
        wl_evaluation_control_t *c = control(new_like_cost("unmanaged-empty",
                unmanaged, governed != 0, true));
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "unmanaged-empty", unmanaged, governed ? g : NULL, c) == 0);
        CHECK(finish(c, 0) == new_like_cost("unmanaged-empty", unmanaged,
            governed != 0, true));
        CHECK(wl_columnar_relation_new_like_governed_checked(&reference,
            "unmanaged-empty", unmanaged, governed ? g : NULL) == 0);
        constructor_equal(actual, reference);
        destroy(actual); destroy(reference); release(c); governor_empty(g);
    }
    destroy(unmanaged);
    wl_columnar_memory_governor_ref_release(g);
}

static col_rel_t *new_like_source, *new_like_canonical;
static bool new_like_checked_pins;
static void
new_like_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    if (phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME ||
        phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY) {
        CHECK(atomic_load(&new_like_source->descriptor_access.state) == 1);
        CHECK(atomic_load(&new_like_canonical->source_access.state) == 1);
        wl_columnar_source_access_writer_t token = { 0 };
        CHECK(col_rel_source_writer_acquire(new_like_source, &token) == EBUSY);
        new_like_checked_pins = true;
    }
    constructor_checkpoint(c, phase, progress);
}

static void
test_new_like_wide_alias_and_names(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *src = NULL;
    CHECK(wl_columnar_relation_alloc_governed(&src, "wide-named", g) == 0);
    char long_name[701]; memset(long_name, 'x', 700); long_name[700] = 0;
    const char *names[600];
    wirelog_column_type_t types[600];
    for (uint32_t i = 0; i < 600; i++) {
        names[i] = i == 300 ? long_name : NULL;
        types[i] = i == 300 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    }
    CHECK(col_rel_set_schema(src, 600, names) == 0);
    CHECK(col_rel_enable_timestamps(src) == 0);
    CHECK(col_rel_set_column_types(src, types, 600) == 0);
    col_rel_t *alias = relation(600, 4, 0, false, g);
    CHECK(col_rel_install_shared_view(alias, src) == 0);
    new_like_source = alias; new_like_canonical = src;
    wl_columnar_relation_test_constructor_checkpoint = new_like_checkpoint;
    uint64_t baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    uint64_t cost = new_like_cost("alias-empty", alias, true, true);
    col_rel_t *actual = NULL, *reference = NULL;
    wl_evaluation_control_t *c = control(cost);
    new_like_checked_pins = false;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "alias-empty", alias, g, c) == 0);
    CHECK(finish(c, 0) == cost && new_like_checked_pins);
    CHECK(wl_columnar_relation_new_like_governed_checked(&reference,
        "alias-empty", alias, g) == 0);
    constructor_equal(actual, reference);
    destroy(actual); destroy(reference);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    retry(c, cost - 1);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "alias-empty", alias, g, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(actual == NULL && finish(c,
        WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
    source_unpinned(alias, src);
    const int phases[] = { WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_TYPES,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMNS_ZERO,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL };
    const uint64_t points[] = { 301, 512, 512, 512, 0 };
    for (uint32_t i = 0; i < 5; i++) {
        retry(c, 0);
        constructor_cancel_phase = phases[i];
        constructor_cancel_progress = points[i];
        int stop_rc =
            wl_columnar_relation_new_like_governed_checked_controlled(&actual,
                "alias-empty", alias, g, c);
        CHECK(stop_rc == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(actual == NULL && finish(c,
            WL_EVALUATION_CONTROL_CANCELLED) == seen);
        source_unpinned(alias, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    constructor_cancel_phase = 0;
    wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
    const int schema_phases[] = { WL_COLUMNAR_RELATION_METADATA_TABLE_ZERO,
                                  WL_COLUMNAR_RELATION_METADATA_CHILD,
                                  WL_COLUMNAR_RELATION_METADATA_NAME };
    const uint64_t schema_points[] = { 512, 301, 512 };
    for (uint32_t i = 0; i < 3; i++) {
        retry(c, 0); metadata_cancel_phase = schema_phases[i];
        metadata_cancel_progress = schema_points[i];
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "alias-empty", alias, g, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
        source_unpinned(alias, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    metadata_cancel_phase = 0;
    wl_columnar_relation_test_metadata_checkpoint = NULL;
    retry(c, cost);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "alias-empty", alias, g, c) == 0);
    CHECK(finish(c, 0) == cost);
    destroy(actual); release(c);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    destroy(alias); destroy(src); governor_empty(g);
    wl_columnar_memory_governor_ref_release(g);
}

static bool new_like_release_cancel;
static uint64_t new_like_release_calls;
static void
consume_new_like_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_reader_t *reader)
{
    wl_columnar_relation_test_new_like_before_reader_release = NULL;
    CHECK(col_rel_source_reader_release(reader) == 0);
    new_like_release_calls++;
    if (new_like_release_cancel)
        wl_evaluation_control_request_cancel(c);
}

static void
cancel_after_new_like_reads(wl_evaluation_control_t *c,
    wl_columnar_source_access_reader_t *reader)
{
    CHECK(reader->owner != NULL && reader->secondary_owner != NULL);
    wl_columnar_relation_test_new_like_before_reader_release = NULL;
    wl_evaluation_control_request_cancel(c);
}

static void
test_new_like_failures_and_defaults(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *src = relation(2, 4, 0, true, g);
    uint64_t baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    col_rel_t *actual = NULL, *reference = NULL;
    wl_evaluation_control_t *c = control(0);
    /* Missing names and missing input schema do not suppress fresh schema. */
    char **saved_names = src->col_names;
    bool saved_schema = src->schema_ok;
    src->col_names = NULL; src->schema_ok = false;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "defaults", src, NULL, c) == 0);
    CHECK(finish(c, 0) == new_like_cost("defaults", src, false, true));
    CHECK(wl_columnar_relation_new_like_governed_checked(&reference,
        "defaults", src, NULL) == 0);
    constructor_equal(actual, reference);
    destroy(actual); destroy(reference);
    src->col_names = saved_names; src->schema_ok = saved_schema;
    char *saved_name = src->col_names[1]; src->col_names[1] = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "defaults", src, g, c) == 0);
    CHECK(finish(c, 0) == new_like_cost("defaults", src, true, true));
    CHECK(wl_columnar_relation_new_like_governed_checked(&reference,
        "defaults", src, g) == 0);
    constructor_equal(actual, reference);
    destroy(actual); destroy(reference); src->col_names[1] = saved_name;
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    /* Genuine actual-token cleanup refusal, with both source pins consumed. */
    for (uint32_t disposition = 0; disposition < 3; disposition++) {
        retry(c, 0); new_like_release_cancel = false;
        constructor_cancel_phase = disposition == 2
            ? WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL : 0;
        constructor_cancel_progress = 0;
        wl_columnar_relation_test_constructor_checkpoint =
            constructor_checkpoint;
        src->has_graph_column = disposition == 1; src->graph_col_idx = 2;
        wl_columnar_relation_test_new_like_before_reader_release =
            consume_new_like_reader;
        int expected = disposition == 1 ? EINVAL : EBUSY;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "cleanup", src, g, c) == expected);
        CHECK(actual == NULL); finish(c, expected);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(c, &report) == 0);
        if (disposition == 2)
            CHECK(report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
        source_unpinned(src, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    CHECK(new_like_release_calls == 3);
    constructor_cancel_phase = 0;
    retry(c, new_like_cost("cleanup", src, true, true) - 1);
    src->has_graph_column = false;
    wl_columnar_relation_test_new_like_before_reader_release =
        consume_new_like_reader;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "cleanup", src, g, c) == EBUSY);
    CHECK(actual == NULL); finish(c, EBUSY);
    wl_evaluation_control_report_t limit_report;
    CHECK(wl_evaluation_control_report(c, &limit_report) == 0);
    CHECK(limit_report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
    source_unpinned(src, src);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    constructor_cancel_phase = 0;
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    src->has_graph_column = false;
    retry(c, 0);
    wl_columnar_relation_test_new_like_before_reader_release =
        cancel_after_new_like_reads;
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "tail", src, g, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    source_unpinned(src, src);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    wl_columnar_source_access_writer_t writer = { 0 };
    CHECK(col_rel_source_writer_acquire(src, &writer) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "blocked", src, g, c) == EBUSY);
    CHECK(actual == NULL && finish(c, EBUSY) == 9);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    source_unpinned(src, src);
    wirelog_column_type_t fault_types[] = { WIRELOG_TYPE_FLOAT,
                                            WIRELOG_TYPE_INT64 };
    CHECK(col_rel_set_column_types(src, fault_types, 2) == 0);
    baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    for (uint32_t malformed = 0; malformed < 4; malformed++) {
        retry(c, 0);
        src->compound_count = malformed == 0 ? 1 : 0;
        src->inline_physical_offset = malformed == 1 ? 1 : 0;
        src->compound_arity_len = malformed == 2 ? 1 : 0;
        src->has_graph_column = malformed == 3;
        src->graph_col_idx = 2;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "malformed", src, g, c) == EINVAL);
        CHECK(actual == NULL && finish(c, EINVAL) == 15);
        CHECK(wl_columnar_relation_new_like_governed_checked(&reference,
            "malformed", src, g) == EINVAL && reference == NULL);
        source_unpinned(src, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    src->compound_count = 0; src->inline_physical_offset = 0;
    src->compound_arity_len = 0; src->has_graph_column = false;
    /* Admission and real preparation/commit faults retain stage evidence. */
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t usable = atomic_load(&governor->usable_bytes);
    CHECK(wl_columnar_relation_new_like_governed_checked(&reference,
        "faults", src, g) == 0);
    uint64_t descriptor = reference->descriptor_reserved_bytes;
    uint64_t metadata = reference->metadata_reserved_bytes;
    destroy(reference);
    const uint64_t limits[] = { baseline, baseline + descriptor,
                                baseline + descriptor + 2 * sizeof(uint64_t),
                                baseline + descriptor + 2 * sizeof(uint64_t) +
                                metadata };
    for (uint32_t i = 0; i < 4; i++) {
        atomic_store(&governor->usable_bytes, limits[i]);
        retry(c, 0);
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "faults", src, g, c) == ENOSPC);
        CHECK(actual == NULL); finish(c, ENOSPC); source_unpinned(src, src);
        CHECK(atomic_load(&governor->reserved_bytes) == baseline);
    }
    atomic_store(&governor->usable_bytes, usable);
    for (int64_t allocation = 0; allocation < 12; allocation++) {
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = allocation;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "faults", src, g, c) == ENOMEM);
        CHECK(actual == NULL); finish(c, ENOMEM); source_unpinned(src, src);
        CHECK(atomic_load(&governor->reserved_bytes) == baseline);
    }
    wl_columnar_relation_test_constructor_checkpoint = constructor_checkpoint;
    for (int commit = 1; commit <= 3; commit++) {
        retry(c, 0); constructor_committed = 0;
        wl_columnar_relation_test_constructor_fail_commit = commit;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
            "faults", src, g, c) == ENOMEM);
        CHECK(actual == NULL && constructor_committed == (uint64_t)commit - 1);
        finish(c, ENOMEM); source_unpinned(src, src);
        CHECK(atomic_load(&governor->reserved_bytes) == baseline);
    }
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&actual,
        "faults", src, g, c) == 0);
    finish(c, 0); destroy(actual); release(c);
    destroy(src); governor_empty(g); wl_columnar_memory_governor_ref_release(g);
}

static uint64_t
logical_run_visits(const col_rel_t *src)
{
    if (src->run_count > COL_MAX_RUNS)
        return 0;
    uint64_t visits = 0;
    uint32_t previous = 0;
    for (uint32_t i = 0; i < src->run_count; i++) {
        uint32_t end = src->run_ends[i]; visits++;
        if (end <= previous || end > src->nrows)
            break;
        previous = end;
    }
    return visits;
}

static uint64_t
logical_cost(const col_rel_t *src, bool governed)
{
    uint64_t cost = new_like_cost(src->name ? src->name : "", src,
            governed, true) + 6 + 12 + logical_run_visits(src);
    if (!src->name)
        cost -= 3; /* Neither empty-name char passes nor name allocation. */
    if (src->nrows)
        cost += (uint64_t)src->ncols * (src->nrows + 1u);
    if (src->timestamps)
        cost += 5u * (uint64_t)(src->timestamp_capacity > src->capacity
            ? src->timestamp_capacity : src->capacity);
    if (!src->schema_ok) {
        if (governed && !src->ncols) cost -= 23;
        cost -= 3;
        if (src->ncols) {
            uint64_t lengths = 0;
            for (uint32_t i = 0; i < src->ncols; i++) {
                char generated[32];
                const char *name = src->col_names ? src->col_names[i] : NULL;
                if (!name) {
                    snprintf(generated, sizeof(generated), "col%u", i);
                    name = generated;
                }
                lengths += strlen(name) + 1;
            }
            cost -= 1 + 5u * (uint64_t)src->ncols + lengths;
        }
    }
    return cost;
}

static void
logical_equal(const col_rel_t *a, const col_rel_t *b)
{
    CHECK((a->name != NULL) == (b->name != NULL));
    if (a->name)
        CHECK(strcmp(a->name, b->name) == 0);
    CHECK(a->ncols == b->ncols && a->nrows == b->nrows &&
        a->capacity == b->capacity);
    CHECK(a->declared_ncols == b->declared_ncols &&
        a->has_graph_column == b->has_graph_column &&
        a->graph_col_idx == b->graph_col_idx);
    CHECK(a->view_generation == b->view_generation &&
        a->storage_generation == b->storage_generation);
    CHECK(a->view_generation == 1 && a->storage_generation == 1);
    CHECK(a->relation_identity != b->relation_identity &&
        a->storage_owner == a);
    CHECK(a->memory_governor == b->memory_governor && a->mem_ledger == NULL);
    CHECK(a->descriptor_reserved_bytes == b->descriptor_reserved_bytes &&
        a->metadata_reserved_bytes == b->metadata_reserved_bytes &&
        a->retained_reserved_bytes == b->retained_reserved_bytes);
    CHECK((a->row_scratch != NULL) == (b->row_scratch != NULL));
    CHECK((a->column_types != NULL) == (b->column_types != NULL));
    CHECK(a->schema_ok == b->schema_ok);
    if (a->schema_ok)
        CHECK(a->schema.n_children == b->schema.n_children &&
            strcmp(a->schema.format, b->schema.format) == 0);
    else
        CHECK(a->schema.release == NULL && b->schema.release == NULL);
    for (uint32_t c = 0; c < a->ncols; c++) {
        CHECK(a->columns[c] != NULL && b->columns[c] != NULL);
        CHECK(a->col_names[c] != b->col_names[c] &&
            strcmp(a->col_names[c], b->col_names[c]) == 0);
        if (a->column_types)
            CHECK(a->column_types[c] == b->column_types[c]);
        if (a->schema_ok)
            CHECK(strcmp(a->schema.children[c]->name,
                b->schema.children[c]->name) == 0 &&
                strcmp(a->schema.children[c]->format,
                b->schema.children[c]->format) == 0);
        for (uint32_t row = 0; row < a->nrows; row++)
            CHECK(a->columns[c][row] == b->columns[c][row]);
    }
    CHECK(a->timestamp_capacity == b->timestamp_capacity &&
        (a->timestamps != NULL) == (b->timestamps != NULL));
    for (uint32_t row = 0; row < a->timestamp_capacity; row++) {
        CHECK(a->timestamps[row].iteration == b->timestamps[row].iteration &&
            a->timestamps[row].stratum == b->timestamps[row].stratum &&
            a->timestamps[row].worker == b->timestamps[row].worker &&
            a->timestamps[row]._reserved == b->timestamps[row]._reserved &&
            a->timestamps[row].multiplicity == b->timestamps[row].multiplicity);
        if (row >= a->nrows)
            CHECK(a->timestamps[row].iteration == 0 &&
                a->timestamps[row].stratum == 0 &&
                a->timestamps[row].worker == 0 &&
                a->timestamps[row]._reserved == 0 &&
                a->timestamps[row].multiplicity == 0);
    }
    CHECK(a->base_nrows == b->base_nrows &&
        a->sorted_nrows == b->sorted_nrows &&
        a->run_count == b->run_count);
    for (uint32_t i = 0; i < COL_MAX_RUNS; i++)
        CHECK(a->run_ends[i] == b->run_ends[i]);
    CHECK(a->merge_columns == NULL && a->retract_backup_columns == NULL &&
        a->retract_backup_timestamps == NULL && a->dedup_slots == NULL &&
        a->merge_buf_cap == 0 && !a->pool_owned && !a->arena_owned &&
        a->col_shared == NULL);
}

static void
test_logical_shape_and_governors(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    wl_columnar_memory_governor_ref_t *override = fresh_governor();
    for (uint32_t source_governed = 0; source_governed < 2; source_governed++)
        for (uint32_t width_case = 0; width_case < 2; width_case++)
            for (uint32_t absence = 0; absence < 2; absence++) {
                col_rel_t *src = source_governed ? relation(width_case ? 2 : 0,
                        4, 0, false, g)
                    : col_rel_new_auto("logical-source", width_case ? 2 : 0);
                CHECK(src != NULL);
                if (source_governed && width_case) {
                    wirelog_column_type_t types[] = { WIRELOG_TYPE_FLOAT,
                                                      WIRELOG_TYPE_INT32 };
                    CHECK(col_rel_set_column_types(src, types, 2) == 0);
                }
                char *saved_name = src->name;
                char **saved_names = src->col_names;
                int64_t **saved_columns = src->columns;
                uint32_t saved_capacity = src->capacity;
                bool saved_schema = src->schema_ok;
                if (absence) {
                    src->name = NULL; src->col_names = NULL;
                    src->schema_ok = false;
                    src->capacity = 0; src->columns = NULL;
                }
                src->has_graph_column = true; src->graph_col_idx = 999;
                src->declared_ncols = 41; src->base_nrows = 77;
                src->sorted_nrows = 9;
                uint64_t baseline =
                    atomic_load(&wl_columnar_memory_governor_ref_get(
                            g)->reserved_bytes);
                for (uint32_t passed = 0; passed < 2; passed++) {
                    col_rel_t *actual = NULL, *reference = NULL;
                    wl_columnar_memory_governor_ref_t *effective =
                        passed ? override : src->memory_governor;
                    wl_evaluation_control_t *c = control(logical_cost(src,
                            effective != NULL));
                    CHECK(wl_columnar_relation_deep_copy_governed_controlled(
                            src,
                            &actual, passed ? override : NULL, c) == 0);
                    CHECK(finish(c, 0) == logical_cost(src, effective != NULL));
                    CHECK(wl_columnar_relation_deep_copy_governed_controlled(
                            src,
                            &reference, passed ? override : NULL, NULL) == 0);
                    logical_equal(actual, reference);
                    CHECK(actual->memory_governor == effective &&
                        actual->sorted_nrows == 0);
                    if (absence)
                        CHECK(actual->name == NULL && actual->capacity == 0 &&
                            !actual->schema_ok);
                    source_unpinned(src, src);
                    destroy(actual); destroy(reference); release(c);
                    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                            g)->reserved_bytes) == baseline);
                    governor_empty(override);
                }
                src->name = saved_name; src->col_names = saved_names;
                src->columns = saved_columns; src->capacity = saved_capacity;
                src->schema_ok = saved_schema;
                destroy(src); governor_empty(g);
            }
    wl_columnar_memory_governor_ref_release(override);
    wl_columnar_memory_governor_ref_release(g);
}

static int logical_cancel_copy_phase;
static uint64_t logical_cancel_chunk;
static bool logical_copy_reached;
static void
logical_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    CHECK(atomic_load(&new_like_source->descriptor_access.state) == 1);
    CHECK(atomic_load(&new_like_canonical->source_access.state) == 1);
    if (phase == logical_cancel_copy_phase) {
        logical_copy_reached = true;
        cancel_at = seen + logical_cancel_chunk;
    }
    constructor_checkpoint(c, phase, progress);
}

static void
test_logical_wide_alias_payload_and_runs(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *src = NULL;
    CHECK(col_rel_alloc(&src, "logical-wide") == 0);
    char long_name[701]; memset(long_name, 'l', 700); long_name[700] = 0;
    const char *names[300]; wirelog_column_type_t types[300];
    for (uint32_t i = 0; i < 300; i++) {
        names[i] = i == 150 ? long_name : NULL;
        types[i] = i == 0 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    }
    CHECK(col_rel_set_schema(src, 300, names) == 0);
    CHECK(col_rel_set_column_types(src, types, 300) == 0);
    bool denied = false;
    CHECK(col_rel_reserve_capacity_admitted(src, 320, &denied) == 0);
    CHECK(col_rel_enable_timestamps(src) == 0);
    src->timestamps = realloc(src->timestamps, 380 * sizeof(*src->timestamps));
    CHECK(src->timestamps != NULL); src->timestamp_capacity = 380;
    src->nrows = 300; src->base_nrows = 999; src->sorted_nrows = 250;
    for (uint32_t c = 0; c < 300; c++)
        for (uint32_t row = 0; row < 300; row++)
            src->columns[c][row] = (int64_t)c * 10000 + row;
    src->columns[0][0] = INT64_MIN;
    src->columns[0][1] = INT64_C(0x7ff8000000000001);
    for (uint32_t row = 0; row < 380; row++) {
        src->timestamps[row].iteration = row + 1;
        src->timestamps[row].stratum = row + 2;
        src->timestamps[row].worker = row + 3;
        src->timestamps[row]._reserved = 4;
        src->timestamps[row].multiplicity = -(int64_t)row - 5;
    }
    src->run_count = 2; src->run_ends[0] = 150; src->run_ends[1] = 300;
    for (uint32_t i = 2; i < COL_MAX_RUNS; i++)
        src->run_ends[i] = 900 + i;
    CHECK(col_rel_reserve_merge_grid(src, 4) == 0);
    src->merge_columns[0][0] = 555;
    src->retract_backup_columns = col_columns_alloc(300, 2);
    src->retract_backup_timestamps = calloc(2,
            sizeof(*src->retract_backup_timestamps));
    CHECK(src->retract_backup_columns && src->retract_backup_timestamps);
    src->retract_backup_capacity = 2;
    src->retract_backup_timestamp_capacity = 2;
    src->retract_backup_nrows = 1;
    src->retract_backup_columns[0][0] = 777;
    src->retract_backup_timestamps[0].multiplicity = 888;
    wl_columnar_eval_dedup_set_insert(src, 123);
    CHECK(src->dedup_slots != NULL);
    CHECK(col_rel_attach_memory_governor(src, g) == 0);
    col_rel_t *alias = relation(300, 4, 0, false, g);
    CHECK(col_rel_install_shared_view(alias, src) == 0);
    new_like_source = alias; new_like_canonical = src;
    /* Logical graph metadata copies without importing new-like validation. */
    alias->has_graph_column = true; alias->graph_col_idx = 999;
    uint64_t source_view = src->view_generation,
        alias_view = alias->view_generation;
    uint64_t baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
    alias->mem_ledger = &ledger;
    wl_mem_ledger_t ledger_before = ledger;
    wl_columnar_relation_test_constructor_checkpoint = logical_checkpoint;
    col_rel_t *actual = NULL, *reference = NULL;
    wl_evaluation_control_t *c = control(logical_cost(alias, true));
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(alias, &actual,
        NULL, c) == 0);
    uint64_t cost = finish(c, 0);
    CHECK(cost == logical_cost(alias, true));
    CHECK(wl_columnar_relation_deep_copy_governed(alias, &reference,
        NULL) == 0);
    logical_equal(actual, reference);
    CHECK(actual->columns[0][0] == INT64_MIN &&
        actual->columns[0][1] == INT64_C(0x7ff8000000000001));
    destroy(actual); destroy(reference);
    /* Owner auxiliary state is independently cloned, then deliberately dropped. */
    retry(c, logical_cost(src, true));
    new_like_source = src;
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual, NULL,
        c) == 0);
    CHECK(finish(c, 0) == logical_cost(src, true));
    CHECK(wl_columnar_relation_deep_copy_governed(src, &reference, NULL) == 0);
    logical_equal(actual, reference); destroy(actual); destroy(reference);
    CHECK(src->merge_columns[0][0] == 555 &&
        src->retract_backup_columns[0][0] == 777 &&
        src->retract_backup_timestamps[0].multiplicity == 888 &&
        src->dedup_slots != NULL);
    new_like_source = alias;
    retry(c, cost - 1);
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(alias, &actual,
        NULL, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    const int copy_phases[] = { WL_COLUMNAR_RELATION_CONSTRUCTOR_CELLS,
                                WL_COLUMNAR_RELATION_CONSTRUCTOR_TIMESTAMP_LIVE,
                                WL_COLUMNAR_RELATION_CONSTRUCTOR_TIMESTAMP_ZERO };
    const uint64_t chunks[] = { 256, 255, 255 };
    for (uint32_t i = 0; i < 3; i++) {
        retry(c, 0); logical_copy_reached = false;
        logical_cancel_copy_phase = copy_phases[i];
        logical_cancel_chunk = chunks[i];
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(alias, &actual,
            NULL, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(actual == NULL && logical_copy_reached);
        finish(c, WL_EVALUATION_CONTROL_CANCELLED);
        source_unpinned(alias, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    logical_cancel_copy_phase = 0;
    const int phases[] = { WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_TYPES,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_RUN_VALIDATE,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_RUN_COPY,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL };
    const uint64_t points[] = { 151, 512, 256, 1, 8, 0 };
    for (uint32_t i = 0; i < 6; i++) {
        retry(c, 0); constructor_cancel_phase = phases[i];
        constructor_cancel_progress = points[i];
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(alias, &actual,
            NULL, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
        source_unpinned(alias, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    constructor_cancel_phase = 0;
    retry(c, cost);
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(alias, &actual,
        NULL, c) == 0);
    CHECK(finish(c, 0) == cost); destroy(actual); release(c);
    CHECK(memcmp(&ledger, &ledger_before, sizeof(ledger)) == 0);
    CHECK(src->view_generation == source_view &&
        alias->view_generation == alias_view);
    alias->mem_ledger = NULL;
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    destroy(alias); destroy(src); governor_empty(g);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_logical_invalid_runs_and_failures(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *src = relation(2, 4, 0, true, g);
    wirelog_column_type_t types[] = { WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT };
    CHECK(col_rel_set_column_types(src, types, 2) == 0);
    src->nrows = 3;
    for (uint32_t col = 0; col < 2; col++)
        for (uint32_t row = 0; row < 3; row++)
            src->columns[col][row] = (int64_t)col * 10000 + row;
    uint64_t baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    col_rel_t *actual = NULL, *reference = NULL;
    wl_evaluation_control_t *c = control(0);
    for (uint32_t mode = 0; mode < 5; mode++) {
        if (mode)
            retry(c, 0);
        src->run_count = mode == 0 ? 2 : mode == 1 ? 9 : 2;
        src->run_ends[0] = mode == 2 ? 0 : 1;
        src->run_ends[1] = mode == 3 ? 1 : mode == 4 ? 2 : 3;
        for (uint32_t i = 2; i < COL_MAX_RUNS; i++) src->run_ends[i] = 70 + i;
        src->sorted_nrows = 2; src->base_nrows = 99;
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual,
            NULL, c) == 0);
        CHECK(finish(c, 0) == logical_cost(src, true));
        CHECK(wl_columnar_relation_deep_copy_governed(src, &reference,
            NULL) == 0);
        logical_equal(actual, reference);
        CHECK(actual->run_count == (mode ? 0 : 2) &&
            actual->sorted_nrows == (mode ? 0 : 2));
        destroy(actual); destroy(reference);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    src->run_count = 0;
    for (uint32_t disposition = 0; disposition < 3; disposition++) {
        retry(c, 0);
        constructor_cancel_phase = disposition ==
            2 ? WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL : 0;
        constructor_cancel_progress = 0;
        wl_columnar_relation_test_constructor_checkpoint =
            constructor_checkpoint;
        wl_columnar_relation_test_new_like_before_reader_release =
            consume_new_like_reader;
        int64_t *saved = src->columns[1];
        if (disposition == 1)
            src->columns[1] = NULL;
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual,
            NULL, c) == EINVAL);
        CHECK(actual == NULL); finish(c, EINVAL);
        if (disposition == 2) {
            wl_evaluation_control_report_t report;
            CHECK(wl_evaluation_control_report(c,
                &report) == 0 &&
                report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
        }
        src->columns[1] = saved;
        source_unpinned(src, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    constructor_cancel_phase = 0;
    retry(c, logical_cost(src, true) - 1);
    wl_columnar_relation_test_new_like_before_reader_release =
        consume_new_like_reader;
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual, NULL,
        c) == EINVAL);
    CHECK(actual == NULL); finish(c, EINVAL);
    wl_evaluation_control_report_t limit_report;
    CHECK(wl_evaluation_control_report(c, &limit_report) == 0 &&
        limit_report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
    source_unpinned(src, src);
    retry(c, 0);
    wl_columnar_relation_test_new_like_before_reader_release =
        cancel_after_new_like_reads;
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual, NULL,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    source_unpinned(src, src);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    for (uint32_t bad = 0; bad < 4; bad++) {
        retry(c, 0);
        uint32_t saved_rows = src->nrows,
            saved_timestamp_capacity = src->timestamp_capacity;
        int64_t **saved_columns = src->columns;
        int64_t *saved_lane = src->columns[1];
        if (bad == 0) src->nrows = src->capacity + 1;
        if (bad == 1) src->timestamp_capacity = src->nrows - 1;
        if (bad == 2) src->columns = NULL;
        if (bad == 3) src->columns[1] = NULL;
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual,
            NULL, c) == EINVAL);
        CHECK(actual == NULL); finish(c, EINVAL);
        CHECK(wl_columnar_relation_deep_copy_governed(src, &reference,
            NULL) == EINVAL && reference == NULL);
        src->nrows = saved_rows;
        src->timestamp_capacity = saved_timestamp_capacity;
        src->columns = saved_columns; src->columns[1] = saved_lane;
        source_unpinned(src, src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
    }
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t usable = atomic_load(&governor->usable_bytes);
    atomic_store(&governor->usable_bytes, baseline);
    retry(c, 0);
    wl_columnar_relation_test_new_like_before_reader_release =
        consume_new_like_reader;
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual, NULL,
        c) == ENOSPC);
    CHECK(actual == NULL); finish(c, ENOSPC); source_unpinned(src, src);
    atomic_store(&governor->usable_bytes, usable);
    CHECK(wl_columnar_relation_deep_copy_governed(src, &reference, NULL) == 0);
    uint64_t descriptor_bytes = reference->descriptor_reserved_bytes;
    uint64_t metadata_bytes = reference->metadata_reserved_bytes;
    destroy(reference);
    const uint64_t later_limits[] = { baseline + descriptor_bytes,
                                      baseline + descriptor_bytes + 2 *
                                      sizeof(uint64_t),
                                      baseline + descriptor_bytes + 2 *
                                      sizeof(uint64_t) + metadata_bytes };
    wl_columnar_relation_test_constructor_checkpoint = constructor_checkpoint;
    for (uint32_t i = 0; i < 3; i++) {
        retry(c, 0); constructor_committed = 0;
        atomic_store(&governor->usable_bytes, later_limits[i]);
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual,
            NULL, c) == ENOSPC);
        CHECK(actual == NULL && constructor_committed == 1);
        finish(c, ENOSPC); source_unpinned(src, src);
        CHECK(atomic_load(&governor->reserved_bytes) == baseline);
    }
    atomic_store(&governor->usable_bytes, usable);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    for (int64_t allocation = 0; allocation < 12; allocation++) {
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = allocation;
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual,
            NULL, c) == ENOMEM);
        CHECK(actual == NULL); finish(c, ENOMEM); source_unpinned(src, src);
        CHECK(atomic_load(&governor->reserved_bytes) == baseline);
    }
    wl_columnar_relation_test_constructor_checkpoint = constructor_checkpoint;
    for (int stage = 1; stage <= 3; stage++) {
        retry(c, 0); constructor_committed = 0;
        wl_columnar_relation_test_constructor_fail_commit = stage;
        CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual,
            NULL, c) == ENOMEM);
        CHECK(actual == NULL && constructor_committed == (uint64_t)stage - 1);
        finish(c, ENOMEM); source_unpinned(src, src);
        CHECK(atomic_load(&governor->reserved_bytes) == baseline);
    }
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    retry(c, logical_cost(src, true));
    CHECK(wl_columnar_relation_deep_copy_governed_controlled(src, &actual, NULL,
        c) == 0);
    CHECK(finish(c, 0) == logical_cost(src, true)); destroy(actual); release(c);
    destroy(src); governor_empty(g); wl_columnar_memory_governor_ref_release(g);
}

static uint64_t
pool_cost(const delta_pool_t *pool, const char *name, const col_rel_t *src,
    bool governed)
{
    uint64_t cost = new_like_cost(name, src, governed, false);
    if (!pool)
        return cost;
    if (pool->slot_used == pool->slot_cap)
        return cost + 4;
    return cost + pool->slot_size + 3; /* byte zeroing + shape4 - heapalloc1 */
}

static delta_pool_t *pool_fixture;
static col_rel_t *pool_prior;
static unsigned char *pool_prior_bytes;
static size_t pool_saved_arena_used;
static bool pool_acquired_reached;
static col_rel_t *pool_later;
static bool pool_acquire_later;

static void
pool_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    CHECK(memcmp(pool_fixture->slab, pool_prior_bytes,
        pool_fixture->slot_size) == 0);
    CHECK(strcmp(pool_prior->name,
        "prior-slot") == 0 && pool_prior->columns[0][0] == 909);
    CHECK(pool_fixture->arena_used == pool_saved_arena_used);
    for (size_t i = 0; i < pool_saved_arena_used; i++)
        CHECK((unsigned char)pool_fixture->arena[i] == 0x5a);
    if (phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ACQUIRED) {
        CHECK(pool_fixture->slot_used == 2 && progress == 1);
        pool_acquired_reached = true;
        if (pool_acquire_later) {
            pool_acquire_later = false;
            CHECK(wl_columnar_relation_pool_new_like_governed_checked(
                    &pool_later,
                    pool_fixture, "later-slot", new_like_source, NULL) == 0);
            CHECK(pool_later ==
                (col_rel_t *)(pool_fixture->slab + 2 *
                pool_fixture->slot_size));
            pool_later->columns[0][0] = 707;
            wl_evaluation_control_request_cancel(c);
        }
    }
    if (phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME ||
        phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY) {
        CHECK(atomic_load(&new_like_source->descriptor_access.state) == 1);
        CHECK(atomic_load(&new_like_canonical->source_access.state) == 1);
    }
    if (phase == constructor_cancel_phase
        && (phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED
        || phase == WL_COLUMNAR_RELATION_CONSTRUCTOR_BEFORE_COMMIT)
        && progress != constructor_cancel_progress)
        return;
    constructor_checkpoint(c, phase, progress);
}

static void
pool_baseline(delta_pool_t *pool, uint64_t credits,
    wl_columnar_memory_governor_ref_t *g)
{
    CHECK(pool->slot_used == 1 && pool->arena_used == pool_saved_arena_used);
    CHECK(memcmp(pool->slab, pool_prior_bytes, pool->slot_size) == 0);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == credits);
    source_unpinned(new_like_source, new_like_canonical);
}

static void
test_pool_real_slot_stops_and_faults(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *src = NULL;
    CHECK(wl_columnar_relation_alloc_governed(&src, "pool-source", g) == 0);
    char long_name[701]; memset(long_name, 'p', 700); long_name[700] = 0;
    const char *names[300]; wirelog_column_type_t types[300];
    for (uint32_t i = 0; i < 300; i++) {
        names[i] = i == 150 ? long_name : NULL;
        types[i] = i == 150 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    }
    CHECK(col_rel_set_schema(src, 300, names) == 0);
    CHECK(col_rel_set_column_types(src, types, 300) == 0);
    CHECK(col_rel_enable_timestamps(src) == 0);
    src->nrows = 1; src->columns[150][0] = INT64_C(0x7ff8000000000001);
    col_rel_t *alias = relation(300, 4, 0, false, g);
    CHECK(col_rel_install_shared_view(alias, src) == 0);
    size_t slot_size = sizeof(col_rel_t) + 512;
    delta_pool_t *pool = delta_pool_create_managed(3, slot_size, 64,
            wl_columnar_memory_governor_ref_get(g));
    delta_pool_t *reference_pool = delta_pool_create(2, slot_size, 64);
    CHECK(pool && reference_pool);
    col_rel_t stack_relation;
    CHECK((uintptr_t)&stack_relation.storage_alias_borrows % 8 == 0);
    for (uint32_t slot = 0; slot < pool->slot_cap; slot++) {
        col_rel_t *actual_slot = (col_rel_t *)(pool->slab + slot *
            pool->slot_size);
        CHECK((uintptr_t)&actual_slot->storage_alias_borrows % 8 == 0);
    }
    memset(pool->slab, 0xa5, pool->slot_size * pool->slot_cap);
    CHECK(wl_columnar_relation_pool_new_like_governed_checked(&pool_prior,
        pool, "prior-slot", alias, g) == 0);
    pool_prior->columns[0][0] = 909;
    CHECK(delta_pool_alloc_data(pool, 32) != NULL);
    memset(pool->arena, 0x5a, pool->arena_used);
    pool_fixture = pool; pool_saved_arena_used = pool->arena_used;
    pool_prior_bytes = malloc(slot_size); CHECK(pool_prior_bytes != NULL);
    memcpy(pool_prior_bytes, pool->slab, slot_size);
    new_like_source = alias; new_like_canonical = src;
    wl_columnar_relation_test_constructor_checkpoint = pool_checkpoint;
    uint64_t baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    uint64_t cost = pool_cost(pool, long_name, alias, true);
    wl_evaluation_control_t *c = control(cost);
    col_rel_t *actual = NULL, *reference = NULL;
    pool_acquired_reached = false;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == 0);
    CHECK(actual == (col_rel_t *)(pool->slab + slot_size) &&
        pool_acquired_reached);
    CHECK(actual->pool_owned && !actual->arena_owned &&
        actual->timestamps == NULL);
    CHECK(actual->descriptor_reserved_bytes == 0 &&
        actual->pool_name_reserved_bytes == 701);
    CHECK(finish(c, 0) == cost);
    CHECK(wl_columnar_relation_pool_new_like_governed_checked(&reference,
        reference_pool, long_name, alias, g) == 0);
    constructor_equal(actual, reference);
    for (size_t byte = sizeof(col_rel_t); byte < slot_size; byte++)
        CHECK((unsigned char)pool->slab[slot_size + byte] == 0);
    destroy(actual); destroy(reference);
    CHECK(pool->slot_used == 2); /* Success destroy does not pop a pool slot. */
    /* Actual checked bulk cleanup followed by real reset, then reuse prior. */
    destroy(pool_prior); delta_pool_reset(pool);
    delta_pool_reset(reference_pool);
    CHECK(wl_columnar_relation_pool_new_like_governed_checked(&pool_prior,
        pool, "prior-slot", alias, g) == 0);
    pool_prior->columns[0][0] = 909;
    CHECK(delta_pool_alloc_data(pool, 32) != NULL);
    memset(pool->arena, 0x5a, pool->arena_used);
    memcpy(pool_prior_bytes, pool->slab, slot_size);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    retry(c, cost - 1);
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    pool_baseline(pool, baseline, g);
    const int phases[] = { WL_COLUMNAR_RELATION_CONSTRUCTOR_IDENTITY,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ZERO,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ZERO,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ZERO,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ACQUIRED,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_TYPES,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_BEFORE_COMMIT,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_BEFORE_COMMIT,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_BEFORE_COMMIT,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_COMMITTED,
                           WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL };
    const uint64_t points[] = { 1, 256, 512, slot_size, 1, 512, 151, 150, 512,
                                256, 4, 4, 2, 2, 3, 3, 0 };
    for (uint32_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
        retry(c, 0); pool_acquired_reached = false;
        constructor_cancel_phase = phases[i];
        constructor_cancel_progress = points[i];
        CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
                &actual,
                pool, long_name, alias, g,
                c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
        if (i > 3) CHECK(pool_acquired_reached);
        pool_baseline(pool, baseline, g);
    }
    constructor_cancel_phase = 0;
    /* Genuine independent legacy identity consumption before counted CAS. */
    retry(c, 0); identity_failures = identity_successes = 0;
    identity_cancel_failure = true;
    wl_columnar_relation_test_before_identity_cas = consume_identity;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL && identity_failures == 1 && identity_successes == 0);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    destroy(identity_winner); identity_winner = NULL;
    identity_cancel_failure = false;
    pool_baseline(pool, baseline, g);
    const int64_t allocations[] = { 0, 1, 2, 3, 152, 302, 303, 304, 305, 454,
                                    604, 605 };
    for (uint32_t i = 0; i < sizeof(allocations) / sizeof(allocations[0]);
        i++) {
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = allocations[i];
        CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
                &actual,
                pool, long_name, alias, g, c) == ENOMEM);
        CHECK(actual == NULL); finish(c, ENOMEM);
        pool_baseline(pool, baseline, g);
    }
    const int stages[] = { 4, 2, 3 };
    const uint64_t previous[] = { 0, 4, 2 };
    for (uint32_t i = 0; i < 3; i++) {
        retry(c, 0); constructor_committed = 0;
        wl_columnar_relation_test_constructor_fail_commit = stages[i];
        int expected = stages[i] == 4 ? EINVAL : ENOMEM;
        CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
                &actual,
                pool, long_name, alias, g, c) == expected);
        CHECK(actual == NULL && constructor_committed == previous[i]);
        finish(c, expected); pool_baseline(pool, baseline, g);
    }
    /* Same positive admitted name-token fault on the actual NULL legacy branch. */
    wl_columnar_relation_test_constructor_fail_commit = 4;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, NULL) == EINVAL && actual == NULL);
    pool_baseline(pool, baseline, g);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t usable = atomic_load(&governor->usable_bytes);
    CHECK(wl_columnar_relation_pool_new_like_governed_checked(&reference,
        reference_pool, long_name, alias, g) == 0);
    uint64_t name_bytes = reference->pool_name_reserved_bytes;
    uint64_t metadata_bytes = reference->metadata_reserved_bytes;
    destroy(reference); delta_pool_reset(reference_pool);
    const uint64_t limits[] = { baseline, baseline + name_bytes,
                                baseline + name_bytes + 300 * sizeof(uint64_t),
                                baseline + name_bytes + 300 * sizeof(uint64_t) +
                                metadata_bytes };
    for (uint32_t i = 0; i < 4; i++) {
        retry(c, 0); atomic_store(&governor->usable_bytes, limits[i]);
        CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
                &actual,
                pool, long_name, alias, g, c) == ENOSPC);
        CHECK(actual == NULL); finish(c, ENOSPC);
        pool_baseline(pool, baseline, g);
    }
    atomic_store(&governor->usable_bytes, usable);
    /* Real release consumes both pins; checked second release fails EBUSY. */
    for (uint32_t stop = 0; stop < 2; stop++) {
        retry(c, 0);
        constructor_cancel_phase =
            stop ? WL_COLUMNAR_RELATION_CONSTRUCTOR_FINAL : 0;
        constructor_cancel_progress = 0;
        wl_columnar_relation_test_new_like_before_reader_release =
            consume_new_like_reader;
        CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
                &actual,
                pool, long_name, alias, g, c) == EBUSY);
        CHECK(actual == NULL); finish(c, EBUSY);
        if (stop) {
            wl_evaluation_control_report_t report;
            CHECK(wl_evaluation_control_report(c,
                &report) == 0 &&
                report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
        }
        pool_baseline(pool, baseline, g);
    }
    constructor_cancel_phase = 0;
    retry(c, cost - 1);
    wl_columnar_relation_test_new_like_before_reader_release =
        consume_new_like_reader;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == EBUSY);
    CHECK(actual == NULL); finish(c, EBUSY);
    wl_evaluation_control_report_t limit_report;
    CHECK(wl_evaluation_control_report(c, &limit_report) == 0 &&
        limit_report.stop_reason == WL_EVALUATION_CONTROL_WORK_LIMIT);
    pool_baseline(pool, baseline, g);
    retry(c, 0);
    wl_columnar_relation_test_new_like_before_reader_release =
        cancel_after_new_like_reads;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    pool_baseline(pool, baseline, g);
    retry(c, 0); wl_columnar_relation_test_constructor_fail_commit = 4;
    wl_columnar_relation_test_new_like_before_reader_release =
        consume_new_like_reader;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == EINVAL);
    CHECK(actual == NULL); finish(c, EINVAL); pool_baseline(pool, baseline, g);
    /* Defensive post-acquisition later slot: never pop its descriptor. */
    retry(c, 0); pool_acquire_later = true; pool_later = NULL;
    CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
            &actual,
            pool, long_name, alias, g, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(actual == NULL && pool->slot_used == 3 && pool_later != NULL);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    col_rel_t *tombstone = (col_rel_t *)(pool->slab + slot_size);
    CHECK(tombstone->relation_identity == 0 &&
        tombstone->storage_owner == NULL && tombstone->memory_governor == NULL);
    CHECK(atomic_load(&tombstone->pool_name_reservation.state) ==
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
    CHECK(pool_later->columns[0][0] == 707 && strcmp(pool_later->name,
        "later-slot") == 0);
    CHECK(col_rel_pool_destroy_roots_checked(pool, 1, 3) == 0);
    CHECK(memcmp(pool->slab, pool_prior_bytes, slot_size) == 0);
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    destroy(pool_prior); release(c); free(pool_prior_bytes);
    pool_prior_bytes = NULL;
    delta_pool_destroy(reference_pool); delta_pool_destroy(pool);
    destroy(alias); destroy(src); governor_empty(g);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_pool_fallback_nullary_and_shapes(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    for (uint32_t width = 0; width < 2; width++)
        for (uint32_t managed = 0; managed < 2; managed++) {
            col_rel_t *src = relation(width, 4, 0, true, g);
            bool schema = src->schema_ok; src->schema_ok = false;
            delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 32);
            delta_pool_t *reference_pool = delta_pool_create(1,
                    sizeof(col_rel_t), 32);
            CHECK(pool && reference_pool);
            col_rel_t *actual = NULL, *reference = NULL;
            wl_evaluation_control_t *c = control(pool_cost(pool, "pool-empty",
                    src, managed != 0));
            if (!managed)
                wl_columnar_relation_test_constructor_fail_commit = 4;
            CHECK(
                wl_columnar_relation_pool_new_like_governed_checked_controlled(
                    &actual,
                    pool, "pool-empty", src, managed ? g : NULL, c) == 0);
            CHECK(finish(c, 0) == pool_cost(reference_pool, "pool-empty", src,
                managed != 0));
            CHECK(wl_columnar_relation_pool_new_like_governed_checked(
                    &reference,
                    reference_pool, "pool-empty", src,
                    managed ? g : NULL) == 0);
            constructor_equal(actual, reference);
            if (!managed) {
                CHECK(wl_columnar_relation_test_constructor_fail_commit == 4);
                wl_columnar_relation_test_constructor_fail_commit = 0;
            }
            CHECK(actual->schema_ok && actual->column_types == NULL &&
                actual->timestamps == NULL);
            destroy(actual); destroy(reference);
            uint64_t baseline =
                atomic_load(&wl_columnar_memory_governor_ref_get(
                        g)->reserved_bytes);
            for (uint32_t exhausted = 0; exhausted < 2; exhausted++) {
                delta_pool_t *selected = exhausted ? pool : NULL;
                uint64_t cost = pool_cost(selected, "fallback", src,
                        managed != 0);
                retry(c, cost);
                CHECK(
                    wl_columnar_relation_pool_new_like_governed_checked_controlled
                        (&actual,
                    selected, "fallback", src, managed ? g : NULL, c) == 0);
                CHECK(!actual->pool_owned && actual->timestamps == NULL &&
                    finish(c, 0) == cost);
                CHECK(wl_columnar_relation_pool_new_like_governed_checked(
                        &reference,
                        selected, "fallback", src, managed ? g : NULL) == 0);
                constructor_equal(actual, reference); destroy(actual);
                destroy(reference);
                retry(c, cost - 1);
                CHECK(
                    wl_columnar_relation_pool_new_like_governed_checked_controlled
                        (&actual,
                    selected, "fallback", src, managed ? g : NULL,
                    c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
                CHECK(actual == NULL);
                finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
                CHECK(pool->slot_used == 1 && pool->arena_used == 0);
                CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                        g)->reserved_bytes) == baseline);
            }
            src->schema_ok = schema;
            release(c); delta_pool_destroy(pool);
            delta_pool_destroy(reference_pool);
            destroy(src); governor_empty(g);
        }
    col_rel_t *src = relation(1, 4, 0, false, g), *out = NULL;
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    CHECK(pool != NULL);
    wl_evaluation_control_t *c = control(0);
    size_t original_size = pool->slot_size; char *original_slab = pool->slab;
    for (uint32_t malformed = 0; malformed < 6; malformed++) {
        if (malformed) retry(c, 0);
        pool->slot_used = 1; /* Check invalid shape even when apparently exhausted. */
        pool->slot_size = original_size; pool->slab = original_slab;
        pool->slot_cap = 1;
        int expected = EINVAL;
        if (malformed == 0) pool->slot_used = 2;
        if (malformed == 1) pool->slot_size = sizeof(col_rel_t) - 1;
        if (malformed == 2) pool->slab = original_slab + 1;
        if (malformed == 3) pool->slot_size = sizeof(col_rel_t) + 1;
        if (malformed == 4) {
            pool->slot_cap = 2; pool->slot_size = SIZE_MAX;
            expected = EOVERFLOW;
        }
        if (malformed == 5) pool->slab = NULL;
        CHECK(wl_columnar_relation_pool_new_like_governed_checked_controlled(
                &out,
                pool, "bad-pool", src, g, c) == expected);
        CHECK(out == NULL && finish(c, expected) == 4);
        source_unpinned(src, src);
    }
    pool->slot_used = 0; pool->slot_cap = 1; pool->slot_size = original_size;
    pool->slab = original_slab;
    release(c); delta_pool_destroy(pool); destroy(src); governor_empty(g);
    wl_columnar_memory_governor_ref_release(g);
}

static col_rel_t *
reserve_image(wl_columnar_memory_governor_ref_t *g)
{
    col_rel_t *r = col_rel_new_auto("reserve-image", 300);
    CHECK(r != NULL);
    wirelog_column_type_t types[300];
    for (uint32_t c = 0; c < 300;
        c++) types[c] = c ? WIRELOG_TYPE_INT64 : WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(r, types, 300) == 0);
    CHECK(col_rel_reserve_capacity_admitted(r, 320, NULL) == 0);
    CHECK(col_rel_enable_timestamps(r) == 0);
    r->timestamps = realloc(r->timestamps, 380 * sizeof(*r->timestamps));
    CHECK(r->timestamps); r->timestamp_capacity = 380; r->nrows = 300;
    for (uint32_t c = 0; c < 300; c++)
        for (uint32_t row = 0; row < 300;
            row++) r->columns[c][row] = (int64_t)c * 10000 + row;
    r->columns[0][0] = INT64_MIN;
    r->columns[0][1] = INT64_C(0x7ff8000000000001);
    for (uint32_t row = 0; row < 380; row++) {
        r->timestamps[row].iteration = row + 1;
        r->timestamps[row].stratum = row + 2;
        r->timestamps[row].worker = row + 3; r->timestamps[row]._reserved = 4;
        r->timestamps[row].multiplicity = row + 5;
    }
    CHECK(col_rel_reserve_merge_grid(r, 4) == 0); r->merge_columns[0][0] = 77;
    r->retract_backup_columns = col_columns_alloc(300, 2);
    r->retract_backup_timestamps = calloc(2,
            sizeof(*r->retract_backup_timestamps));
    CHECK(r->retract_backup_columns && r->retract_backup_timestamps);
    r->retract_backup_capacity = 2; r->retract_backup_nrows = 1;
    r->retract_backup_timestamp_capacity = 2;
    r->retract_backup_columns[0][0] = 88;
    r->base_nrows = 111; r->sorted_nrows = 200; r->run_count = 2;
    r->run_ends[0] = 100; r->run_ends[1] = 300;
    r->declared_ncols = 299; r->has_graph_column = true; r->graph_col_idx = 299;
    if (g) CHECK(col_rel_attach_memory_governor(r, g) == 0);
    return r;
}

static void
reserve_equal(const col_rel_t *a, const col_rel_t *b)
{
    CHECK(a->capacity == b->capacity && a->ncols == b->ncols &&
        a->nrows == b->nrows);
    CHECK(a->view_generation == b->view_generation &&
        a->storage_generation == b->storage_generation);
    CHECK(a->retained_reserved_bytes == b->retained_reserved_bytes);
    CHECK(a->declared_ncols == b->declared_ncols &&
        a->graph_col_idx == b->graph_col_idx &&
        a->has_graph_column == b->has_graph_column);
    CHECK(a->base_nrows == b->base_nrows &&
        a->sorted_nrows == b->sorted_nrows && a->run_count == b->run_count);
    for (uint32_t i = 0; i < COL_MAX_RUNS;
        i++) CHECK(a->run_ends[i] == b->run_ends[i]);
    for (uint32_t c = 0; c < a->ncols; c++) {
        CHECK(a->column_types[c] == b->column_types[c]);
        CHECK(strcmp(a->col_names[c], b->col_names[c]) == 0 &&
            strcmp(a->schema.children[c]->format,
            b->schema.children[c]->format) == 0);
        for (uint32_t row = 0; row < a->nrows;
            row++) CHECK(a->columns[c][row] == b->columns[c][row]);
    }
    CHECK(a->timestamp_capacity == b->timestamp_capacity);
    for (uint32_t row = 0; row < a->timestamp_capacity; row++) {
        CHECK(a->timestamps[row].iteration == b->timestamps[row].iteration &&
            a->timestamps[row].stratum == b->timestamps[row].stratum &&
            a->timestamps[row].worker == b->timestamps[row].worker &&
            a->timestamps[row]._reserved == b->timestamps[row]._reserved &&
            a->timestamps[row].multiplicity == b->timestamps[row].multiplicity);
    }
    CHECK(a->merge_columns[0][0] == 77 && b->merge_columns[0][0] == 77 &&
        a->retract_backup_columns[0][0] == 88 &&
        b->retract_backup_columns[0][0] == 88);
}

static int reserve_cancel_phase, reserve_chunk_phase;
static uint64_t reserve_cancel_progress, reserve_chunk_size;
static bool reserve_stage_reached;
static void
reserve_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    if (phase == reserve_chunk_phase) {
        reserve_stage_reached = true; cancel_at = seen + reserve_chunk_size;
    }
    if (phase == reserve_cancel_phase && progress >= reserve_cancel_progress) {
        reserve_stage_reached = true; wl_evaluation_control_request_cancel(c);
    }
}

static uint64_t
reserve_cost(const col_rel_t *r, uint32_t target)
{
    return (r->columns ? r->ncols : 0) + (r->ncols ? 1u : 0u)
           + (uint64_t)r->ncols * (1u + r->nrows)
           + (r->timestamps ? 1u + 5u * (uint64_t)target : 0u) + 64u +
           (r->mem_ledger ? 32u + 32u * (r->ncols != 0)
           + 32u * (r->timestamps != NULL) : 0u) +
           (r->memory_governor ? (r->retained_reserved_bytes ? 28u : 25u) : 0u);
}

static void
test_reserve_owned_atomic(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    for (uint32_t managed = 0; managed < 2; managed++) {
        col_rel_t *r = reserve_image(managed ? g : NULL);
        col_rel_t *reference = reserve_image(managed ? g : NULL);
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        uint64_t relation_bytes = (uint64_t)r->ncols * r->capacity *
            sizeof(int64_t);
        uint64_t timestamp_bytes = (uint64_t)r->timestamp_capacity *
            sizeof(*r->timestamps);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION, relation_bytes);
        wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP, timestamp_bytes);
        r->mem_ledger = &ledger; r->ledger_ts_bytes = timestamp_bytes;
        wl_mem_ledger_t ledger_before = ledger;
        col_rel_t before = *r;
        uint64_t baseline =
            atomic_load(&wl_columnar_memory_governor_ref_get(
                    g)->reserved_bytes);
        uint64_t cost = reserve_cost(r, 351);
        wl_evaluation_control_t *c = control(cost - 1);
        bool denied = true;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 351,
            &denied, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(!denied && r->columns == before.columns &&
            r->timestamps == before.timestamps &&
            r->capacity == before.capacity &&
            r->storage_generation == before.storage_generation);
        finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == baseline);
        wl_columnar_relation_test_reserve_checkpoint = reserve_checkpoint;
        const int phases[] = { WL_COLUMNAR_RELATION_RESERVE_SHAPE,
                               WL_COLUMNAR_RELATION_RESERVE_COLUMN,
                               WL_COLUMNAR_RELATION_RESERVE_CELLS,
                               WL_COLUMNAR_RELATION_RESERVE_TIMESTAMP_LIVE,
                               WL_COLUMNAR_RELATION_RESERVE_TIMESTAMP_ZERO,
                               WL_COLUMNAR_RELATION_RESERVE_FINAL };
        for (uint32_t i = 0; i < 6; i++) {
            retry(c, 0); reserve_stage_reached = false;
            reserve_cancel_phase = i == 0 || i == 1 || i == 5 ? phases[i] : 0;
            reserve_cancel_progress = i == 0 || i == 1 ? 150 : 0;
            reserve_chunk_phase = i >= 2 && i <= 4 ? phases[i] : 0;
            reserve_chunk_size = i == 2 ? 256 : 255;
            CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
                351, &denied, c) == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(reserve_stage_reached && !denied &&
                !r->memory_budget_denial_pending);
            CHECK(r->columns == before.columns &&
                r->timestamps == before.timestamps &&
                r->capacity == before.capacity &&
                r->storage_generation == before.storage_generation);
            finish(c, WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                    g)->reserved_bytes) == baseline);
            reserve_equal(r, reference);
            CHECK(memcmp(&ledger, &ledger_before, sizeof(ledger)) == 0);
        }
        reserve_cancel_phase = reserve_chunk_phase = 0;
        if (managed) {
            retry(c, 0);
            wl_columnar_relation_test_fail_next_reservation_commit();
            CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
                351, &denied, c) == ENOMEM);
            CHECK(!denied && finish(c, ENOMEM) == cost - 13);
            CHECK(r->columns == before.columns &&
                r->capacity == before.capacity);
            CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                    g)->reserved_bytes) == baseline);
            wl_columnar_memory_governor_t *governor =
                wl_columnar_memory_governor_ref_get(g);
            uint64_t usable = atomic_load(&governor->usable_bytes);
            atomic_store(&governor->usable_bytes, baseline);
            retry(c, 0);
            CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
                351, &denied, c) == ENOMEM);
            CHECK(denied && r->memory_budget_denial_pending); finish(c, ENOMEM);
            atomic_store(&governor->usable_bytes, usable);
        }
        retry(c, 0); wl_columnar_relation_test_fail_prepare_resize = true;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 351,
            &denied, c) == ENOMEM);
        CHECK(!denied && !r->memory_budget_denial_pending); finish(c, ENOMEM);
        retry(c, cost);
        wl_columnar_source_access_writer_t writer = { 0 };
        CHECK(col_rel_source_writer_acquire(r, &writer) == 0);
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 351,
            &denied, c) == 0);
        CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
        CHECK(!denied && finish(c, 0) == cost);
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(
                reference, 351, NULL, NULL) == 0);
        reserve_equal(r, reference);
        CHECK(r->capacity == 351 &&
            r->view_generation == before.view_generation &&
            r->storage_generation == before.storage_generation + 1);
        CHECK(r->column_types == before.column_types &&
            r->col_names == before.col_names &&
            r->schema.children == before.schema.children &&
            r->merge_columns == before.merge_columns &&
            r->retract_backup_columns == before.retract_backup_columns &&
            r->row_scratch == before.row_scratch);
        CHECK(atomic_load(&ledger.current_bytes) ==
            (uint64_t)300 * 351 * sizeof(int64_t)
            + (uint64_t)351 * sizeof(*r->timestamps));
        CHECK(r->ledger_ts_bytes == (uint64_t)351 * sizeof(*r->timestamps));
        wl_columnar_relation_test_reserve_checkpoint = NULL;
        destroy(r); CHECK(atomic_load(&ledger.current_bytes) == 0);
        destroy(reference); release(c); governor_empty(g);
    }
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_reserve_no_growth_and_empty(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *r = relation(2, 4, 0, true, g);
    col_rel_t *alias = relation(2, 4, 0, false, g);
    CHECK(col_rel_install_shared_view(alias, r) == 0);
    uint64_t storage = r->storage_generation;
    r->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    r->storage_owner_generation = r->storage_generation;
    /* Alias binding must continue to refer to the original storage epoch. */
    alias->storage_owner_generation = r->storage_generation;
    wl_evaluation_control_t *c = control(2); bool denied = true;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 4,
        &denied, c) == 0);
    CHECK(!denied && finish(c, 0) == 2);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 5,
        &denied, c) == EBUSY);
    CHECK(!denied); finish(c, EBUSY);
    /* Real existing unadmitted payload, with aliases and exhausted generation. */
    CHECK(wl_columnar_memory_release(&r->retained_reservation));
    r->retained_reserved_bytes = 0;
    uint64_t baseline =
        atomic_load(&wl_columnar_memory_governor_ref_get(g)->reserved_bytes);
    wl_columnar_relation_test_reserve_checkpoint = reserve_checkpoint;
    reserve_cancel_phase = WL_COLUMNAR_RELATION_RESERVE_FINAL;
    reserve_cancel_progress = 0;
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 4,
        &denied, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!denied && r->retained_reserved_bytes == 0);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    reserve_cancel_phase = 0;
    retry(c, 0); wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 4,
        &denied, c) == ENOMEM);
    CHECK(!denied && !r->memory_budget_denial_pending &&
        r->retained_reserved_bytes == 0);
    CHECK(finish(c, ENOMEM) == 117);
    CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
            g)->reserved_bytes) == baseline);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t usable = atomic_load(&governor->usable_bytes);
    atomic_store(&governor->usable_bytes, baseline);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 4,
        &denied, c) == ENOMEM);
    CHECK(denied && r->memory_budget_denial_pending); finish(c, ENOMEM);
    atomic_store(&governor->usable_bytes, usable);
    retry(c, 127);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 4,
        &denied, c) == 0);
    CHECK(!denied && r->retained_reserved_bytes > 0 && finish(c, 0) == 127);
    CHECK(r->storage_generation == WL_COLUMNAR_REL_GENERATION_INVALID - 1u);
    wl_columnar_relation_test_reserve_checkpoint = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(alias, 4,
        &denied, c) == 0 && !denied);
    CHECK(finish(c, 0) == 0);
    r->storage_generation = storage; r->storage_owner_generation = storage;
    alias->storage_owner_generation = storage;
    destroy(alias);
    r->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 5,
        &denied, c) == EOVERFLOW && !denied);
    finish(c, EOVERFLOW); r->storage_generation = storage;
    destroy(r); release(c); governor_empty(g);
    for (uint32_t width = 0; width < 2; width++) {
        r = relation(width, 0, 0, false, g);
        /* Supported empty absent grid, with actual retained credit retired. */
        if (r->retained_reserved_bytes) {
            CHECK(wl_columnar_memory_release(&r->retained_reservation));
            r->retained_reserved_bytes = 0;
        }
        col_columns_free(r->columns, width); r->columns = NULL;
        c = control(reserve_cost(r, 7));
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 7,
            &denied, c) == 0);
        CHECK(!denied && r->capacity == 7 && finish(c, 0) == (width ? 91 : 64));
        if (width) CHECK(r->columns && r->columns[0]);
        destroy(r); release(c); governor_empty(g);
    }
    r = relation(1, 4, 0, false, g);
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    col_rel_t *pooled = NULL;
    CHECK(pool && wl_columnar_relation_pool_new_like_governed_checked(&pooled,
        pool, "reserve-pooled", r, g) == 0);
    c = control(reserve_cost(pooled, 77));
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(pooled, 77,
        &denied, c) == 0);
    CHECK(pooled->pool_owned && !pooled->arena_owned &&
        pooled->capacity == 77 && finish(c, 0) == 95);
    destroy(pooled); delta_pool_destroy(pool); destroy(r); release(c);
    governor_empty(g);
    /* Genuine arena-backed no-growth is a no-op, growth remains excluded. */
    wl_arena_t *arena = wl_arena_create(4096);
    pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    CHECK(arena && pool);
    r = col_rel_pool_new_auto(pool, arena, "reserve-arena", 1);
    CHECK(r && r->arena_owned);
    c = control(0); int64_t **arena_columns = r->columns;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, &denied, c) == 0);
    CHECK(!denied && finish(c, 0) == 0 && r->columns == arena_columns);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity + 1, &denied, c) == ENOTSUP);
    CHECK(!denied && finish(c, ENOTSUP) == 0 && r->columns == arena_columns);
    destroy(r); delta_pool_destroy(pool); wl_arena_free(arena); release(c);
    CHECK(col_rel_alloc(&r, "reserve-overflow") == 0);
    r->ncols = UINT32_MAX;
    c = control(0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        UINT32_MAX, &denied, c) == EOVERFLOW);
    CHECK(!denied && finish(c,
        EOVERFLOW) == 0 && r->columns == NULL && r->capacity == 0);
    r->ncols = 0; destroy(r); release(c);
    wl_columnar_memory_governor_ref_release(g);
}

/* Admission faults use only genuine tokens and genuine competing operations. */
static wl_columnar_source_access_reader_t contender[2];
static unsigned contender_count, contention_failures, contention_goal;
static bool contention_cancel, cancel_acquired;
static void
admission_before_cas(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, uint64_t expected)
{
    (void)c;
    contender_count = expected == 1 ? 2u : 1u;
    for (unsigned i = 0; i < contender_count; i++)
        CHECK(wl_columnar_source_access_reader_acquire(gate,
            &contender[i]) == 0);
    CHECK(atomic_load(&gate->state) != expected);
}
static void
admission_after_cas(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    (void)gate;
    if (contender_count) {
        CHECK(!acquired);
        contention_failures++;
        for (unsigned i = 0; i < contender_count; i++)
            CHECK(wl_columnar_source_access_reader_release(&contender[i]) == 0);
        contender_count = 0;
        if (contention_failures == contention_goal) {
            wl_columnar_relation_test_before_reader_cas = NULL;
            wl_columnar_relation_test_after_reader_cas = NULL;
            if (contention_cancel)
                wl_evaluation_control_request_cancel(c);
        }
    } else if (acquired && cancel_acquired) {
        wl_columnar_relation_test_after_reader_cas = NULL;
        wl_evaluation_control_request_cancel(c);
    }
}
static void
test_counted_gates(void)
{
    wl_columnar_source_access_gate_t gate;
    wl_columnar_source_access_gate_init(&gate);
    wl_columnar_source_access_reader_t reader = {0};
    wl_columnar_source_access_writer_t writer = {0};
    for (int cancel = 0; cancel < 2; cancel++) {
        wl_evaluation_control_t *c = control(0);
        contention_failures = 0; contention_goal = 300;
        contention_cancel = cancel != 0;
        wl_columnar_relation_test_before_reader_cas = admission_before_cas;
        wl_columnar_relation_test_after_reader_cas = admission_after_cas;
        int expected = cancel ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader,
            c) == expected);
        CHECK(contention_failures == 300 && contender_count == 0);
        CHECK(finish(c, expected) == (cancel ? 300u : 302u));
        if (!cancel)
            CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
        CHECK(!reader.owner && atomic_load(&gate.state) == 0);
        retry(c, 1);
        CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader,
            c) == 0);
        CHECK(finish(c, 0) == 1);
        CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
        release(c);
    }
    wl_evaluation_control_t *c = control(0);
    cancel_acquired = true;
    wl_columnar_relation_test_after_reader_cas = admission_after_cas;
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 1);
    CHECK(!reader.owner && atomic_load(&gate.state) == 0);
    cancel_acquired = false;
    retry(c, 1);
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == 0);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader,
        c) == EBUSY);
    CHECK(finish(c, EBUSY) == 1);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == 0);
    retry(c, 1);
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &writer,
        c) == EBUSY);
    CHECK(finish(c, EBUSY) == 1 && !writer.owner);
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
    retry(c, 1);
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &writer,
        c) == 0);
    CHECK(finish(c, 0) == 1);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    retry(c, 0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    CHECK(!reader.owner && atomic_load(&gate.state) == 0);
    retry(c, 0); cancel_at = 1;
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &writer, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 1);
    CHECK(!writer.owner && atomic_load(&gate.state) == 0);
    /* Synthetic arithmetic initial state on an isolated gate only. No live
     * token/retirement or genuine contention proof is claimed by this setup. */
    atomic_store(&gate.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u);
    retry(c, 1);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 1);
    CHECK(atomic_load(&gate.state) == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u);
    CHECK(!reader.owner && !reader.secondary_owner && !reader.identity
        && !reader.thread_valid && !reader.transferable);
    release(c);
}

static void
make_lazy(col_rel_t *src)
{
    src->storage_owner = NULL;
    src->storage_owner_identity = 123;
    src->storage_owner_generation = 456;
}
static void
lazy_unchanged(col_rel_t *src)
{
    CHECK(src->storage_owner == NULL && src->storage_owner_identity == 123
        && src->storage_owner_generation == 456);
    source_unpinned(src, src);
}
static unsigned lazy_hook_calls;
static void
consume_lazy_writer(wl_evaluation_control_t *c,
    wl_columnar_source_access_writer_t *writer)
{
    (void)c;
    wl_columnar_relation_test_after_lazy_commit = NULL;
    CHECK(wl_columnar_source_access_writer_release(writer) == 0);
    lazy_hook_calls++;
}
static void
cancel_lazy_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_reader_t *reader)
{
    CHECK(reader->owner && !reader->secondary_owner);
    wl_columnar_relation_test_new_like_before_reader_release = NULL;
    wl_evaluation_control_request_cancel(c);
}
static void
consume_lazy_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_reader_t *reader)
{
    (void)c;
    CHECK(reader->owner && !reader->secondary_owner);
    wl_columnar_relation_test_new_like_before_reader_release = NULL;
    CHECK(col_rel_source_reader_release(reader) == 0);
}
static bool overflow_consume_reader;
static unsigned overflow_hook_calls;
static void
latch_accounting_overflow(wl_evaluation_control_t *c,
    wl_columnar_source_access_reader_t *reader)
{
    CHECK(reader->owner);
    wl_columnar_relation_test_new_like_before_reader_release = NULL;
    /* A real control API error, not a fabricated consumed counter/status.
     * This test host call is coordination, not a bounded engine work region. */
    CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    if (overflow_consume_reader)
        CHECK(col_rel_source_reader_release(reader) == 0);
    overflow_hook_calls++;
}
static col_rel_t *initializer_source;
static unsigned initializer_calls;
static bool stop_after_initializer;
static void
compete_lazy_initializer(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate)
{
    CHECK(gate == &initializer_source->descriptor_access);
    CHECK(atomic_load(&gate->state) == 0 && !initializer_source->storage_owner);
    wl_columnar_relation_test_before_writer_cas = NULL;
    CHECK(col_rel_storage_owner_ensure_initialized(initializer_source) == 0);
    CHECK(initializer_source->storage_owner == initializer_source);
    source_unpinned(initializer_source, initializer_source);
    initializer_calls++;
    if (stop_after_initializer)
        wl_evaluation_control_request_cancel(c);
}
static void
test_lazy_initializer_winner(void)
{
    col_rel_t *src = relation(1, 4, 1, false, NULL);
    initializer_source = src;
    for (int stop = 0; stop < 3; stop++) {
        make_lazy(src); initializer_calls = 0;
        stop_after_initializer = stop == 1;
        wl_evaluation_control_t *c = control(0);
        if (stop == 2) cancel_at = 7;
        col_rel_t *out = NULL;
        wl_columnar_relation_test_before_writer_cas = compete_lazy_initializer;
        int expected = stop ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(wl_columnar_relation_new_like_governed_checked_controlled(&out,
            "winner", src, NULL, c) == expected);
        CHECK(initializer_calls == 1 && src->storage_owner == src);
        CHECK(src->storage_owner_identity == src->relation_identity &&
            src->storage_owner_generation == src->storage_generation);
        CHECK(finish(c, expected) == (stop == 1 ? 3 : stop == 2 ? 7 :
            new_like_cost("winner", src, false, true) + 5));
        source_unpinned(src, src);
        if (stop) CHECK(!out); else destroy(out);
        release(c);
    }
    initializer_source = NULL; destroy(src);
}

static void
test_lazy_source_admission(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    for (int mode = 0; mode < 3; mode++) {
        col_rel_t *src = relation(1, 4, 2, true, g);
        uint64_t retained =
            atomic_load(&wl_columnar_memory_governor_ref_get(
                    g)->reserved_bytes);
        delta_pool_t *pool = mode == 2 ? delta_pool_create(1, sizeof(col_rel_t),
                32) : NULL;
        uint64_t cost = mode == 1 ? logical_cost(src, true)
            : mode == 2 ? pool_cost(pool, "lazy", src, true)
            : new_like_cost("lazy", src, true, true);
        cost += 5; /* probe -> writer and four real lazy metadata actions. */
        CHECK(mode != 2 || pool);
        make_lazy(src);
        wl_evaluation_control_t *c = control(cost - 1);
        col_rel_t *out = NULL;
#define LAZY_CALL() (mode == \
        0 ? wl_columnar_relation_new_like_governed_checked_controlled( \
            &out, "lazy", src, g, c) : mode == 1 ? \
        wl_columnar_relation_deep_copy_governed_controlled(src, &out, g, c) : \
        wl_columnar_relation_pool_new_like_governed_checked_controlled( \
            &out, pool, "lazy", src, g, c))
        CHECK(LAZY_CALL() == WL_EVALUATION_CONTROL_WORK_LIMIT && out == NULL);
        CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
        lazy_unchanged(src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == retained);
        retry(c, 0);
        cancel_at = 7; /* actual journal installed under descriptor writer. */
        CHECK(LAZY_CALL() == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(out == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 7);
        lazy_unchanged(src);
        retry(c, 0);
        wl_columnar_relation_test_new_like_before_reader_release =
            cancel_lazy_reader;
        CHECK(LAZY_CALL() == WL_EVALUATION_CONTROL_CANCELLED && !out);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
        lazy_unchanged(src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == retained);
        retry(c, 0);
        wl_columnar_relation_test_new_like_before_reader_release =
            consume_lazy_reader;
        int cleanup_error = mode == 1 ? EINVAL : EBUSY;
        CHECK(LAZY_CALL() == cleanup_error && !out);
        CHECK(finish(c, cleanup_error) == cost);
        lazy_unchanged(src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == retained);
        retry(c, 0);
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_relation_test_new_like_before_reader_release =
            consume_lazy_reader;
        CHECK(LAZY_CALL() == ENOMEM && !out);
        finish(c, ENOMEM);
        wl_columnar_relation_test_constructor_fail_allocation = -1;
        lazy_unchanged(src);
        CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                g)->reserved_bytes) == retained);
        for (int consume = 0; consume < 2; consume++) {
            retry(c, 0); overflow_hook_calls = 0;
            overflow_consume_reader = consume != 0;
            wl_columnar_relation_test_new_like_before_reader_release =
                latch_accounting_overflow;
            CHECK(LAZY_CALL() == EOVERFLOW && !out && overflow_hook_calls == 1);
            CHECK(finish(c, EOVERFLOW) == cost);
            lazy_unchanged(src);
            CHECK(atomic_load(&wl_columnar_memory_governor_ref_get(
                    g)->reserved_bytes) == retained);
        }
        retry(c, cost);
        CHECK(LAZY_CALL() == 0 && out);
        CHECK(finish(c, 0) == cost);
        CHECK(src->storage_owner == src &&
            src->storage_owner_identity == src->relation_identity);
        source_unpinned(src, src); destroy(out); out = NULL;
        /* Actual writer second-release failure happens after cutoff. */
        if (pool) {
            delta_pool_destroy(pool);
            pool = delta_pool_create(1, sizeof(col_rel_t), 32); CHECK(pool);
        }
        make_lazy(src); retry(c, 0); lazy_hook_calls = 0;
        wl_columnar_relation_test_after_lazy_commit = consume_lazy_writer;
        CHECK(LAZY_CALL() == cleanup_error && !out && lazy_hook_calls == 1);
        CHECK(finish(c, cleanup_error) == cost);
        CHECK(src->storage_owner == src);
        source_unpinned(src, src);
#undef LAZY_CALL
        destroy(src); delta_pool_destroy(pool); release(c); governor_empty(g);
    }
    wl_columnar_memory_governor_ref_release(g);
}

static col_rel_t *nonce_competitor;
static unsigned nonce_competitions;
static void
compete_nonce(wl_evaluation_control_t *control, uint64_t expected)
{
    (void)control;
    wl_columnar_relation_mutation_single_t other = {0};
    wl_columnar_relation_test_before_nonce_cas = NULL;
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(
            nonce_competitor, &other, NULL) == 0);
    CHECK(other.set.acquisition_nonce == expected);
    CHECK(col_rel_mutation_set_finish(&other.set, true) == 0);
    nonce_competitions++;
}
static bool single_lose_descriptor;
static unsigned single_cleanup_calls;
static void
consume_single_authority(wl_evaluation_control_t *c,
    wl_columnar_relation_mutation_single_t *single)
{
    wl_columnar_relation_test_single_before_cleanup = NULL;
    CHECK(single->set.descriptors_acquired == 1 && single->set.owners_acquired);
    if (single_lose_descriptor)
        CHECK(wl_columnar_source_access_writer_release(
                &single->descriptor.writer) == 0);
    else
        CHECK(wl_columnar_source_access_writer_release(
                &single->owners[0].writer) == 0);
    wl_evaluation_control_request_cancel(c);
    single_cleanup_calls++;
}
static void
test_fixed_single_admission(void)
{
    col_rel_t *src = relation(1, 4, 2, false, NULL);
    nonce_competitor = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_mutation_single_t single = {0};
    const uint64_t cost = 92;
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(src, &single,
        c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
    CHECK(!single.set.identity); source_unpinned(src, src);
    retry(c, cost);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(src, &single,
        c) == 0);
    CHECK(col_rel_mutation_lease_validate(&single.lease, src) == 0);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(!single.set.identity); source_unpinned(src, src);
    retry(c, cost + 1); nonce_competitions = 0;
    wl_columnar_relation_test_before_nonce_cas = compete_nonce;
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(src, &single,
        c) == 0);
    CHECK(nonce_competitions == 1);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        c) == 0);
    CHECK(finish(c, 0) == cost + 1);
    retry(c, 0); cancel_at = 33; nonce_competitions = 0;
    wl_columnar_relation_test_before_nonce_cas = compete_nonce;
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(src, &single,
        c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(nonce_competitions == 1 && finish(c,
        WL_EVALUATION_CONTROL_CANCELLED) == 33);
    CHECK(!single.set.identity); source_unpinned(src, src);
    /* Busy owner after real descriptor admission retires the descriptor. */
    wl_columnar_source_access_reader_t held = {0};
    CHECK(wl_columnar_source_access_reader_acquire(&src->source_access,
        &held) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(src, &single,
        c) == EBUSY);
    CHECK(finish(c, EBUSY) == 59);
    CHECK(!single.set.identity &&
        atomic_load(&src->descriptor_access.state) == 0);
    CHECK(wl_columnar_source_access_reader_release(&held) == 0);
    for (int fault = 0; fault < 2; fault++) {
        make_lazy(src); retry(c, 0); cancel_at = cost;
        single_cleanup_calls = 0; single_lose_descriptor = fault != 0;
        wl_columnar_relation_test_single_before_cleanup =
            consume_single_authority;
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(src,
            &single, c) == EINVAL);
        CHECK(single_cleanup_calls == 1 && finish(c, EINVAL) == cost);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(c, &report) == 0 &&
            report.stop_reason == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!single.set.identity); source_unpinned(src, src);
        if (!fault)
            lazy_unchanged(src);
        else
            CHECK(src->storage_owner == src); /* authority-loss positive exception */
        src->storage_owner = src;
        src->storage_owner_identity = src->relation_identity;
        src->storage_owner_generation = src->storage_generation;
    }
    col_rel_t *alias = relation(1, 4, 0, false, NULL);
    CHECK(col_rel_install_shared_view(alias, src) == 0);
    retry(c, 0); cancel_at = cost + 1;
    single_lose_descriptor = false; single_cleanup_calls = 0;
    wl_columnar_relation_test_single_before_cleanup = consume_single_authority;
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(alias,
        &single, c) == EINVAL);
    CHECK(single_cleanup_calls == 1 && finish(c, EINVAL) == cost + 1);
    CHECK(!single.set.identity); source_unpinned(alias, src);
    CHECK(atomic_load(&alias->source_access.state) == 0);
    retry(c, cost + 1);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(alias,
        &single, c) == 0);
    CHECK(col_rel_mutation_lease_validate(&single.lease, alias) == 0);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        c) == 0);
    CHECK(finish(c, 0) == cost + 1);
    destroy(alias); destroy(src); destroy(nonce_competitor);
    nonce_competitor = NULL; release(c);
}

static unsigned governor_verdict_hits;
static void
governor_verdict_overflow(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (phase == WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT) {
        governor_verdict_hits++;
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    }
}
static void
test_governor_control_error_provenance(void)
{
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    col_rel_t *r = relation(2, 4, 1, false, g);
    growth_snapshot_t before = snapshot(r);
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(g);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    uint64_t remaining = atomic_load(&governor->usable_bytes) - before.reserved;
    CHECK(wl_columnar_memory_reserve(governor, remaining, &held));
    governor_verdict_hits = 0;
    wl_columnar_memory_governor_test_checkpoint = governor_verdict_overflow;
    wl_evaluation_control_t *c = control(0); bool denied = true;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 8,
        &denied, c) == EOVERFLOW);
    CHECK(governor_verdict_hits == 1 && !denied &&
        !r->memory_budget_denial_pending);
    CHECK(r->columns == before.columns && r->capacity == before.capacity
        && r->retained_reserved_bytes == before.credit);
    CHECK(atomic_load(&governor->reserved_bytes) ==
        before.reserved + remaining);
    CHECK(finish(c, EOVERFLOW) > 0);
    wl_columnar_memory_governor_test_checkpoint = NULL;
    CHECK(wl_columnar_memory_rollback(&held));
    unchanged(r, before);
    retry(c, reserve_cost(r, 8));
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r, 8,
        &denied, c) == 0);
    CHECK(!denied && finish(c, 0) > 0);
    release(c); destroy(r); governor_empty(g);
    wl_columnar_memory_governor_ref_release(g);
}

static wl_mem_ledger_t *exclusive_actual, *exclusive_raw;
static wl_mem_ledger_snapshot_t exclusive_sibling;
static unsigned exclusive_hits;
static uint64_t exclusive_boundary;
static bool exclusive_cancel;
static void
exclusive_publication_checkpoint(wl_evaluation_control_t *c, uint64_t work)
{
    checkpoint(c, work);
    if (work != exclusive_boundary)
        return;
    exclusive_hits++;
    /* Real same-thread sibling operations after requests were prepared. */
    wl_mem_ledger_alloc(exclusive_actual, WL_MEM_SUBSYS_CACHE, 23);
    wl_mem_ledger_free(exclusive_actual, WL_MEM_SUBSYS_CACHE, 7);
    wl_mem_ledger_free(exclusive_actual, WL_MEM_SUBSYS_RELATION, 13);
    wl_mem_ledger_alloc(exclusive_raw, WL_MEM_SUBSYS_CACHE, 23);
    wl_mem_ledger_free(exclusive_raw, WL_MEM_SUBSYS_CACHE, 7);
    wl_mem_ledger_free(exclusive_raw, WL_MEM_SUBSYS_RELATION, 13);
    wl_mem_ledger_snapshot(exclusive_actual, &exclusive_sibling);
    if (exclusive_cancel)
        wl_evaluation_control_request_cancel(c);
}
static void
exclusive_ledger_equal(const wl_mem_ledger_snapshot_t *a,
    const wl_mem_ledger_snapshot_t *b)
{
    CHECK(a->current_bytes == b->current_bytes &&
        a->peak_bytes == b->peak_bytes);
    for (unsigned i = 0; i < WL_MEM_SUBSYS_COUNT; i++)
        CHECK(a->subsys_bytes[i] == b->subsys_bytes[i]
            && a->subsys_peak[i] == b->subsys_peak[i]);
}
static void
test_exclusive_ledger_publication(void)
{
    for (unsigned append = 0; append < 2; append++)
        for (unsigned disposition = 0; disposition < 3; disposition++) {
            wl_columnar_memory_governor_ref_t *g = fresh_governor();
            col_rel_t *r = relation(1, 2, 1, true, g);
            col_rel_t *raw = relation(1, 2, 1, true, g);
            col_rel_t *src = relation(1, 3, 3, true, g);
            wl_mem_ledger_t ledger, raw_ledger;
            wl_mem_ledger_init(&ledger, 0); wl_mem_ledger_init(&raw_ledger, 0);
            wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
                UINT64_MAX - 128);
            wl_mem_ledger_alloc(&raw_ledger, WL_MEM_SUBSYS_RELATION,
                UINT64_MAX - 128);
            uint64_t old_timestamp_charge = 6u * sizeof(*r->timestamps);
            wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_TIMESTAMP,
                old_timestamp_charge);
            wl_mem_ledger_alloc(&raw_ledger, WL_MEM_SUBSYS_TIMESTAMP,
                old_timestamp_charge);
            r->mem_ledger = &ledger; raw->mem_ledger = &raw_ledger;
            r->ledger_ts_bytes = raw->ledger_ts_bytes = old_timestamp_charge;
            growth_snapshot_t before = snapshot(r);
            char **original_names = r->col_names;
            struct ArrowSchema original_schema = r->schema;
            uint64_t original_metadata = r->metadata_reserved_bytes;
            CHECK(r->column_types == NULL && raw->column_types == NULL);
            uint64_t cost = append ? 239 : reserve_cost(r, 4);
            wl_evaluation_control_t *c = control(cost - 1); bool denied;
            int rc = append ? wl_columnar_relation_append_all_controlled(r, src,
                    NULL, c)
                : wl_columnar_relation_reserve_capacity_admitted_controlled(r,
                    4, &denied, c);
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
            finish(c, rc); unchanged(r, before);
            CHECK(r->ledger_ts_bytes == old_timestamp_charge);
            wl_mem_ledger_snapshot_t actual_snapshot, raw_snapshot;
            wl_mem_ledger_snapshot(&ledger, &actual_snapshot);
            wl_mem_ledger_snapshot(&raw_ledger, &raw_snapshot);
            exclusive_ledger_equal(&actual_snapshot, &raw_snapshot);
            retry(c, cost);
            exclusive_actual = &ledger; exclusive_raw = &raw_ledger;
            exclusive_hits = 0; exclusive_boundary = append ? 72 : 64;
            exclusive_cancel = disposition == 1;
            wl_columnar_relation_test_work_checkpoint =
                exclusive_publication_checkpoint;
            if (disposition == 2)
                wl_columnar_relation_test_fail_next_reservation_commit();
            rc = append ? wl_columnar_relation_append_all_controlled(r, src,
                    NULL, c)
                : wl_columnar_relation_reserve_capacity_admitted_controlled(r,
                    4, &denied, c);
            CHECK(exclusive_hits == 1);
            CHECK(rc == (disposition == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : disposition == 2 ? ENOMEM : 0));
            CHECK(finish(c, rc) <= cost);
            wl_columnar_relation_test_work_checkpoint = checkpoint;
            if (disposition) {
                unchanged(r, before);
                CHECK(r->ledger_ts_bytes == old_timestamp_charge);
                wl_mem_ledger_snapshot(&ledger, &actual_snapshot);
                exclusive_ledger_equal(&actual_snapshot, &exclusive_sibling);
                CHECK(actual_snapshot.subsys_bytes[WL_MEM_SUBSYS_CACHE] == 16);
                retry(c, cost);
                CHECK((append ? wl_columnar_relation_append_all_controlled(r,
                    src, NULL, c)
                    : wl_columnar_relation_reserve_capacity_admitted_controlled(
                        r, 4, &denied, c)) == 0);
                CHECK(finish(c, 0) == cost);
            }
            CHECK((append ? wl_columnar_relation_append_all_controlled(raw, src,
                NULL, NULL)
                : wl_columnar_relation_reserve_capacity_admitted_controlled(raw,
                4, &denied, NULL)) == 0);
            wl_mem_ledger_snapshot(&ledger, &actual_snapshot);
            wl_mem_ledger_snapshot(&raw_ledger, &raw_snapshot);
            exclusive_ledger_equal(&actual_snapshot, &raw_snapshot);
            CHECK(r->ledger_ts_bytes == raw->ledger_ts_bytes
                && r->ledger_ts_bytes == 4u * sizeof(*r->timestamps));
            CHECK(actual_snapshot.peak_bytes == UINT64_MAX);
            CHECK(r->capacity == raw->capacity && r->nrows == raw->nrows
                && r->timestamp_capacity == raw->timestamp_capacity
                && r->storage_generation == raw->storage_generation
                && r->view_generation == raw->view_generation);
            CHECK(r->column_types == NULL && raw->column_types == NULL
                && r->col_names == original_names
                && r->schema.children == original_schema.children
                && r->schema.format == original_schema.format
                && r->metadata_reserved_bytes == original_metadata);
            for (uint32_t column = 0; column < r->ncols; column++) {
                CHECK(strcmp(r->col_names[column],
                    raw->col_names[column]) == 0);
                CHECK(strcmp(r->schema.children[column]->format,
                    raw->schema.children[column]->format) == 0);
            }
            payload_equal(r, raw, 0);
            release(c); destroy(r); destroy(raw); destroy(src);
            governor_empty(g);
            wl_columnar_memory_governor_ref_release(g);
        }
    exclusive_actual = exclusive_raw = NULL;
}
static void
test_private_metadata_final_cutoff(void)
{
    for (unsigned growth = 0; growth < 2; growth++)
        for (unsigned accounting = 0; accounting < 2; accounting++) {
            col_rel_t *src = typed_source(1, 2, WIRELOG_TYPE_INT32, false);
            col_rel_t *dst = relation(1, growth ? 1 : 4, 1, false, NULL);
            wl_mem_ledger_t ledger; wl_mem_ledger_init(&ledger, 0);
            wl_mem_ledger_alloc(&ledger, WL_MEM_SUBSYS_RELATION,
                (uint64_t)dst->capacity * sizeof(int64_t));
            dst->mem_ledger = &ledger;
            growth_snapshot_t before = snapshot(dst);
            struct ArrowSchema schema = dst->schema;
            uint64_t metadata_credit = dst->metadata_reserved_bytes;
            wl_mem_ledger_snapshot_t baseline, after;
            wl_mem_ledger_snapshot(&ledger, &baseline);
            wl_evaluation_control_t *c = control(0);
            wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
            metadata_accounting_overflow = accounting != 0;
            metadata_cancel_phase =
                accounting ? 0 : WL_COLUMNAR_RELATION_METADATA_COMMITTED;
            metadata_cancel_progress = 1; committed_seen = false;
            int expected =
                accounting ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
                c) == expected);
            CHECK(committed_seen && finish(c, expected) > 0);
            metadata_unchanged(dst, before, schema, metadata_credit);
            wl_mem_ledger_snapshot(&ledger, &after);
            exclusive_ledger_equal(&baseline, &after);
            CHECK(dst->ledger_ts_bytes == 0);
            released(dst); released(src);
            metadata_cancel_phase = 0; committed_seen = false;
            metadata_accounting_overflow = false;
            retry(c, 0);
            CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
                c) == 0);
            CHECK(committed_seen && finish(c, 0) > 0);
            CHECK(dst->column_types[0] == WIRELOG_TYPE_INT32 &&
                dst->nrows == 3);
            release(c); destroy(dst); destroy(src);
        }
    wl_columnar_relation_test_metadata_checkpoint = NULL;
}

static int row_stop_stage;
static uint64_t row_stop_progress;
static bool row_stage_reached;
static void
row_checkpoint(wl_evaluation_control_t *c, int stage, uint64_t progress)
{
    if (stage == row_stop_stage && progress >= row_stop_progress) {
        row_stage_reached = true;
        wl_evaluation_control_request_cancel(c);
    }
}

static void
row_equal(col_rel_t *a, col_rel_t *b)
{
    CHECK(a->ncols == b->ncols && a->nrows == b->nrows &&
        a->capacity == b->capacity &&
        a->timestamp_capacity == b->timestamp_capacity);
    CHECK(a->storage_generation == b->storage_generation &&
        a->view_generation == b->view_generation);
    CHECK(a->retained_reserved_bytes == b->retained_reserved_bytes);
    for (uint32_t c = 0; c < a->ncols; c++)
        for (uint32_t row = 0; row < a->nrows; row++)
            CHECK(a->columns[c][row] == b->columns[c][row]);
    for (uint32_t row = 0; a->timestamps && row < a->nrows; row++)
        CHECK(!memcmp(&a->timestamps[row], &b->timestamps[row],
            sizeof(col_delta_timestamp_t)));
}

static uint64_t
row_cost(uint32_t width, uint32_t rows, uint32_t capacity, bool ts,
    bool typed)
{
    /* B0 92, fixed lease check 32, final allowance173. Each lane structural+
     * overlap+copy; typed adds one validity and two normalization actions.
     * G growth15+publish13, pointer allocation1+width, raw prefix width*rows,
     * short timestamps allocation1+live five fields+new-record five fields. */
    uint64_t cost = 377u + (uint64_t)width * (typed ? 6u : 3u);
    if (rows >= capacity)
        cost += 64u + ((width || ts) ? 28u : 0u) + (width ? 1u : 0u)
            + width + (uint64_t)width * rows
            + (ts ? 1u + 5u * rows : 0u);
    if (ts)
        cost += 5;
    return cost;
}

static void
test_controlled_row_matrix(void)
{
    const uint32_t widths[] = {0, 16, 17, 300};
    int64_t tuple[300];
    for (size_t w = 0; w < 4; w++)
        for (int grow = 0; grow < 2; grow++)
            for (int ts = 0; ts < 2; ts++) {
                uint32_t width = widths[w], capacity = grow ? 1 : 3;
                for (uint32_t c = 0; c < width; c++) tuple[c] = 700 + c;
                col_rel_t *r = relation(width, capacity, 1, ts, NULL);
                col_rel_t *ref = relation(width, capacity, 1, ts, NULL);
                growth_snapshot_t before = snapshot(r);
                uint64_t cost = row_cost(width, 1, capacity, ts, false);
                wl_evaluation_control_t *c = control(cost - 1);
                CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c)
                    == WL_EVALUATION_CONTROL_WORK_LIMIT);
                CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
                unchanged(r, before); source_unpinned(r, r);
                retry(c, cost);
                CHECK(wl_columnar_relation_append_row_controlled(r, tuple,
                    c) == 0);
                CHECK(finish(c, 0) == cost && seen == cost);
                CHECK(wl_columnar_relation_append_row_controlled(ref, tuple,
                    NULL) == 0);
                row_equal(r, ref); source_unpinned(r, r);
                release(c); destroy(r); destroy(ref);
            }
    /* Capacity zero and NULL column grid is the actual raw accepted shape. */
    col_rel_t *r = relation(17, 0, 0, false, NULL);
    col_rel_t *ref = relation(17, 0, 0, false, NULL);
    col_columns_free(r->columns, r->ncols); r->columns = NULL;
    col_columns_free(ref->columns, ref->ncols); ref->columns = NULL;
    col_rel_retire_payload_credit(r); col_rel_retire_payload_credit(ref);
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == 0);
    finish(c, 0);
    CHECK(col_rel_append_row(ref, tuple) == 0);
    row_equal(r, ref); release(c); destroy(r); destroy(ref);
}

static void
test_controlled_row_float_and_stops(void)
{
    int64_t tuple[300];
    for (uint32_t c = 0; c < 300;
        c++) tuple[c] = wl_columnar_float_to_bits(-0.0);
    col_rel_t *r = typed_source(300, 1, WIRELOG_TYPE_FLOAT, false);
    col_rel_t *ref = typed_source(300, 1, WIRELOG_TYPE_FLOAT, false);
    /* Live invalid FLOAT bits are copied raw, not revalidated. */
    r->columns[0][0] = ref->columns[0][0] = INT64_C(0x7ff8000000000042);
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == 0);
    CHECK(finish(c, 0) == row_cost(300, 1, 1, false, true));
    CHECK(col_rel_append_row(ref, tuple) == 0); row_equal(r, ref);
    for (uint32_t n = 0; n < 300; n++) CHECK(r->columns[n][1] == 0);
    retry(c, 0); tuple[299] = INT64_C(0x7ff8000000000001);
    growth_snapshot_t before = snapshot(r);
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == EINVAL);
    finish(c, EINVAL); CHECK(col_rel_append_row(ref, tuple) == EINVAL);
    CHECK(r->columns == before.columns && r->nrows == before.rows &&
        r->view_generation == before.view); row_equal(r, ref);
    release(c); destroy(r); destroy(ref);

    for (int stage = WL_COLUMNAR_RELATION_ROW_STRUCTURE;
        stage <= WL_COLUMNAR_RELATION_ROW_FINAL; stage++) {
        /* Overlap tuple is a real contiguous live lane, long enough for width.
         * Typed validity checks use integer types here; its actual payload is
         * also copied unchanged into reference and candidate. */
        r = relation(300, 300, 300, false, NULL);
        ref = relation(300, 300, 300, false, NULL);
        wirelog_column_type_t types[300];
        for (uint32_t n = 0; n < 300; n++) types[n] = WIRELOG_TYPE_INT64;
        CHECK(col_rel_set_column_types(r, types, 300) == 0);
        CHECK(col_rel_set_column_types(ref, types, 300) == 0);
        before = snapshot(r);
        c = control(0); row_stage_reached = false;
        row_stop_stage = stage; row_stop_progress = 1;
        if (stage == WL_COLUMNAR_RELATION_ROW_FINAL) row_stop_progress = 0;
        wl_columnar_relation_test_row_checkpoint = row_checkpoint;
        CHECK(wl_columnar_relation_append_row_controlled(r, r->columns[0], c)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(row_stage_reached); finish(c, WL_EVALUATION_CONTROL_CANCELLED);
        unchanged(r, before); source_unpinned(r, r);
        row_stop_stage = 0; retry(c, 0);
        CHECK(wl_columnar_relation_append_row_controlled(r, r->columns[0],
            c) == 0);
        finish(c, 0); CHECK(col_rel_append_row(ref, ref->columns[0]) == 0);
        row_equal(r, ref); release(c); destroy(r); destroy(ref);
    }
    wl_columnar_relation_test_row_checkpoint = NULL;
    r = relation(300, 300, 300, false, NULL);
    before = snapshot(r); c = control(0);
    /* 92 admission +32 lease +64 shape +600 width +16 interval facts
     * +64 footprint +15 growth +1 pointer table +1 column alloc +256 cells. */
    cancel_at = 1141;
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cancel_at);
    unchanged(r, before); source_unpinned(r, r);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == 0);
    finish(c, 0); release(c); destroy(r);
}

static void
test_controlled_row_growth_faults(void)
{
    int64_t tuple[17] = {0};
    col_rel_t *r = relation(17, 1, 1, true, NULL);
    growth_snapshot_t before = snapshot(r);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_test_fail_next_prepare_resize();
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == ENOMEM);
    finish(c, ENOMEM); unchanged(r, before); source_unpinned(r, r);
    retry(c, 0); wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == ENOMEM);
    finish(c, ENOMEM); unchanged(r, before); source_unpinned(r, r);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == 0);
    finish(c, 0); release(c); destroy(r);
}

static void
test_controlled_row_scratch_and_modes(void)
{
    /* Genuine live-lane overlap on each actual scratch boundary. */
    for (uint32_t width = 16; width <= 17; width++)
        for (int growth = 0; growth < 2; growth++) {
            col_rel_t *r = relation(width, width + !growth, width, false, NULL);
            col_rel_t *ref = relation(width, width + !growth, width, false,
                    NULL);
            wl_evaluation_control_t *c = control(0);
            CHECK(wl_columnar_relation_append_row_controlled(r, r->columns[0],
                c) == 0);
            finish(c, 0); CHECK(col_rel_append_row(ref, ref->columns[0]) == 0);
            row_equal(r, ref); release(c); destroy(r); destroy(ref);
        }
    col_rel_t *r = relation(17, 20, 17, false, NULL);
    growth_snapshot_t before = snapshot(r);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(r->memory_governor);
    wl_columnar_memory_reservation_t quota;
    wl_columnar_memory_reservation_init(&quota);
    uint64_t remaining = atomic_load(&g->usable_bytes) -
        atomic_load(&g->reserved_bytes);
    CHECK(wl_columnar_memory_reserve_checked(g, remaining, &quota)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    uint64_t admitted = atomic_load(&g->reserved_bytes);
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_row_controlled(r, r->columns[0],
        c) == ENOMEM);
    CHECK(r->memory_budget_denial_pending && r->nrows == before.rows &&
        r->capacity == before.capacity && r->columns == before.columns);
    CHECK(atomic_load(&g->reserved_bytes) == admitted);
    finish(c, ENOMEM); source_unpinned(r, r);
    CHECK(wl_columnar_memory_release(&quota));
    for (int fault = 0; fault < 2; fault++) {
        retry(c, 0);
        wl_columnar_relation_test_fail_row_scratch_alloc = fault == 0;
        wl_columnar_relation_test_fail_row_scratch_commit = fault == 1;
        CHECK(wl_columnar_relation_append_row_controlled(r, r->columns[0],
            c) == ENOMEM);
        CHECK(!wl_columnar_relation_test_fail_row_scratch_alloc &&
            !wl_columnar_relation_test_fail_row_scratch_commit);
        CHECK(!r->memory_budget_denial_pending);
        finish(c, ENOMEM); unchanged(r, before); source_unpinned(r, r);
    }
    retry(c, 0);
    CHECK(wl_columnar_relation_append_row_controlled(r, r->columns[0], c) == 0);
    CHECK(!r->memory_budget_denial_pending); finish(c, 0);
    release(c); destroy(r);

    /* Short-only transition and nonshort resize preserve their distinct raw
     * timestamp initialization ranges, auxiliary grids and ledger ordering. */
    for (int mode = 0; mode < 2; mode++) {
        r = relation(2, 4, 1, true, NULL);
        col_rel_t *ref = relation(2, 4, 1, true, NULL);
        if (!mode) {
            r->timestamp_capacity = ref->timestamp_capacity = 1;
        } else {
            r->capacity = ref->capacity = 1;
        }
        CHECK(col_rel_reserve_merge_grid(r, 3) == 0);
        CHECK(col_rel_reserve_merge_grid(ref, 3) == 0);
        int64_t **merge = r->merge_columns;
        merge[0][0] = ref->merge_columns[0][0] = 123;
        wl_mem_ledger_t ledger, raw_ledger;
        wl_mem_ledger_init(&ledger, 0); wl_mem_ledger_init(&raw_ledger, 0);
        r->mem_ledger = &ledger; ref->mem_ledger = &raw_ledger;
        col_rel_ledger_reconcile(r, 0); col_rel_ledger_reconcile(ref, 0);
        int64_t tuple[2] = {23, 45}; c = control(0);
        CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == 0);
        finish(c, 0); CHECK(col_rel_append_row(ref, tuple) == 0);
        row_equal(r, ref);
        CHECK(r->merge_columns == merge && merge[0][0] == 123);
        wl_mem_ledger_snapshot_t actual, raw;
        wl_mem_ledger_snapshot(&ledger, &actual);
        wl_mem_ledger_snapshot(&raw_ledger, &raw);
        exclusive_ledger_equal(&actual, &raw);
        if (mode)
            for (uint32_t row = r->nrows; row < r->timestamp_capacity; row++)
                CHECK(!memcmp(&r->timestamps[row], &(col_delta_timestamp_t){0},
                    sizeof(col_delta_timestamp_t)));
        release(c); destroy(r); destroy(ref);
    }
    /* Unmanaged actual heap and pool descriptor exercise the same core. */
    for (int pooled = 0; pooled < 2; pooled++) {
        delta_pool_t *pool = pooled ? delta_pool_create(1, sizeof(col_rel_t),
                32) : NULL;
        col_rel_t *src = col_rel_new_auto("row-source", 17);
        CHECK(src);
        r = pooled ? col_rel_pool_new_like(pool, "pool-row", src)
            : col_rel_new_auto("heap-row", 17);
        CHECK(r && !r->memory_governor && r->pool_owned == (bool)pooled);
        int64_t tuple[17] = {19}; c = control(0);
        CHECK(wl_columnar_relation_append_row_controlled(r, tuple, c) == 0);
        finish(c, 0); CHECK(r->nrows == 1 && r->columns[0][0] == 19);
        source_unpinned(r, r); release(c); destroy(r); destroy(src);
        delta_pool_destroy(pool);
    }
}

static void
test_controlled_row_batch_journal(void)
{
    int64_t tuple = 91;
    for (int commit = 0; commit < 2; commit++)
        for (int cancel = 0; cancel < 2; cancel++) {
            col_rel_t *r = relation(1, 1, 0, false, NULL); make_lazy(r);
            col_rel_append_batch_t batch = {0};
            wl_evaluation_control_t *c = control(0);
            CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
                c) == 0);
            CHECK(batch.single.set.initialization_count == 1);
            if (cancel) wl_evaluation_control_request_cancel(c);
            int rc = wl_columnar_relation_append_batch_end_controlled(&batch,
                    commit, c);
            int expected = cancel &&
                commit ? WL_EVALUATION_CONTROL_CANCELLED : 0;
            CHECK(rc == expected && !batch.active); finish(c, expected);
            if (commit && !cancel) CHECK(r->storage_owner == r);
            else lazy_unchanged(r);
            source_unpinned(r, r);
            CHECK(col_rel_storage_owner_ensure_initialized(r) == 0);
            release(c); destroy(r);
        }
    for (int first_stop = 0; first_stop < 2; first_stop++) {
        col_rel_t *r = relation(1, 1, 0, false, NULL); make_lazy(r);
        col_rel_append_batch_t batch = {0};
        wl_evaluation_control_t *c = control(0);
        CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
            c) == 0);
        if (first_stop) wl_evaluation_control_request_cancel(c);
        int rc = wl_columnar_relation_append_batch_row_controlled(&batch,
                &tuple, c);
        if (first_stop) {
            CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
                false, c) == 0);
            finish(c, rc); lazy_unchanged(r);
        } else {
            CHECK(rc == 0 && !batch.single.set.initialization_count);
            CHECK(r->nrows == 1 && r->storage_owner == r);
            wl_evaluation_control_request_cancel(c);
            CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
                &tuple, c)
                == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
                false, c) == 0);
            finish(c, WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(r->nrows == 1 && r->columns[0][0] == tuple &&
                r->storage_owner == r);
        }
        source_unpinned(r, r);
        CHECK(col_rel_storage_owner_ensure_initialized(r) == 0);
        release(c); destroy(r);
    }
    /* Two real growth epochs; cancelled next reacquire retires committed old
     * authority without polling away the successful prefix. */
    col_rel_t *r = relation(1, 1, 0, false, NULL);
    col_rel_append_batch_t batch = {0}; wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    for (int i = 0; i < 4; i++)
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple,
            c) == 0);
    CHECK(r->capacity == 4 && r->nrows == 4);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!batch.active && r->nrows == 4);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, true,
        c) == 0);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED); source_unpinned(r, r);
    release(c); destroy(r);

    /* Real cleanup failure after publication preserves rows/init, no stop poll. */
    r = relation(1, 2, 0, false, NULL); make_lazy(r);
    c = control(0); batch = (col_rel_append_batch_t){0};
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple,
        c) == 0);
    single_lose_descriptor = false; single_cleanup_calls = 0;
    wl_columnar_relation_test_single_before_cleanup = consume_single_authority;
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, false,
        c) == EINVAL);
    CHECK(single_cleanup_calls == 1 && r->nrows == 1 && r->storage_owner == r);
    finish(c, EINVAL); source_unpinned(r, r);
    CHECK(col_rel_storage_owner_ensure_initialized(r) == 0);
    release(c); destroy(r);
}

static col_rel_t *row_epoch_relation;
static wl_columnar_source_access_reader_t row_epoch_reader;
static unsigned row_epoch_hook_calls;
static void
row_epoch_busy(wl_evaluation_control_t *c, uint64_t nonce)
{
    (void)c; (void)nonce;
    wl_columnar_relation_test_before_nonce_cas = NULL;
    CHECK(wl_columnar_source_access_reader_acquire(
            &row_epoch_relation->source_access,
            &row_epoch_reader) == 0);
    row_epoch_hook_calls++;
}
static void
row_independent_initializer(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate)
{
    (void)c;
    CHECK(gate == &row_epoch_relation->descriptor_access);
    wl_columnar_relation_test_before_writer_cas = NULL;
    CHECK(col_rel_storage_owner_ensure_initialized(row_epoch_relation) == 0);
    row_epoch_hook_calls++;
}

static void
test_controlled_row_authority_precedence(void)
{
    int64_t tuple = 7;
    col_rel_t *r = relation(1, 1, 0, false, NULL);
    col_rel_append_batch_t batch = {0};
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    for (int n = 0; n < 4; n++)
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple,
            c) == 0);
    row_epoch_relation = r; row_epoch_hook_calls = 0;
    wl_columnar_relation_test_before_nonce_cas = row_epoch_busy;
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple,
        c) == EBUSY);
    CHECK(row_epoch_hook_calls == 1 && !batch.active && r->nrows == 4 &&
        r->capacity == 4);
    CHECK(atomic_load(&r->descriptor_access.state) == 0);
    CHECK(wl_columnar_source_access_reader_release(&row_epoch_reader) == 0);
    finish(c, EBUSY); source_unpinned(r, r); release(c); destroy(r);

    /* Real winner initializes before controlled descriptor writer CAS. A
     * failed first row cannot roll back that independent committed baseline. */
    r = relation(1, 2, 0, false, NULL); make_lazy(r);
    c = control(0); batch = (col_rel_append_batch_t){0};
    row_epoch_relation = r; row_epoch_hook_calls = 0;
    wl_columnar_relation_test_before_writer_cas = row_independent_initializer;
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    CHECK(row_epoch_hook_calls == 1 &&
        batch.single.set.initialization_count == 0);
    uint64_t identity = r->storage_owner_identity,
        generation = r->storage_owner_generation;
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, false,
        c) == 0);
    CHECK(r->storage_owner == r && r->storage_owner_identity == identity &&
        r->storage_owner_generation == generation && r->nrows == 0);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED); source_unpinned(r, r);
    release(c); destroy(r);

    /* Published first row followed directly by cancelled end has no new
     * negative cleanup result, for either commit flag. */
    for (int commit = 0; commit < 2; commit++) {
        r = relation(1, 2, 0, false, NULL); make_lazy(r);
        c = control(0); batch = (col_rel_append_batch_t){0};
        CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
            c) == 0);
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch, &tuple,
            c) == 0);
        wl_evaluation_control_request_cancel(c);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, commit,
            c) == 0);
        CHECK(r->storage_owner == r && r->nrows == 1 && r->columns[0][0] == 7);
        /* End did not observe it; the next actual admission still does. */
        CHECK(wl_columnar_relation_append_row_controlled(r, &tuple, c)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r->nrows == 1 && r->storage_owner == r);
        finish(c, WL_EVALUATION_CONTROL_CANCELLED); source_unpinned(r, r);
        release(c); destroy(r);
    }
    /* Positive execution wins real cleanup error; cleanup positive wins a
     * negative body stop. Both release every remaining actual token. */
    for (int positive = 0; positive < 2; positive++) {
        r = relation(1, 2, 0, false, NULL); make_lazy(r); c = control(0);
        if (positive) r->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID -
                1u;
        else {
            row_stop_stage = WL_COLUMNAR_RELATION_ROW_FINAL;
            row_stop_progress = 0; row_stage_reached = false;
            wl_columnar_relation_test_row_checkpoint = row_checkpoint;
        }
        single_lose_descriptor = false; single_cleanup_calls = 0;
        wl_columnar_relation_test_single_before_cleanup =
            consume_single_authority;
        int expected = positive ? EOVERFLOW : EINVAL;
        CHECK(wl_columnar_relation_append_row_controlled(r, &tuple,
            c) == expected);
        CHECK(single_cleanup_calls == 1 && r->nrows == 0);
        if (!positive) CHECK(row_stage_reached);
        finish(c, expected); lazy_unchanged(r);
        wl_columnar_relation_test_row_checkpoint = NULL; row_stop_stage = 0;
        CHECK(col_rel_storage_owner_ensure_initialized(r) == 0);
        release(c); destroy(r);
    }
    row_epoch_relation = NULL;
}

/* Actual internal plan dispatcher, registered session right and admitted frame. */
static wl_col_session_t *
join_session(void)
{
    wl_col_session_t *sess = calloc(1, sizeof(*sess));
    CHECK(sess);
    sess->num_workers = 1;
    sess->frontier_ops = &col_frontier_epoch_ops;
    wl_mem_ledger_init(&sess->mem_ledger, 0);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    sess->memory_governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(sess->memory_governor);
    return sess;
}
static void
join_session_destroy(wl_col_session_t *sess)
{
    CHECK(!sess->cleanup_active && !sess->cleanup_pending);
    for (uint32_t i = 0; i < sess->nrels; i++) {
        CHECK(col_rel_storage_owner_ensure_initialized(sess->rels[i]) == 0);
        destroy(sess->rels[i]);
    }
    free(sess->rels);
    if (sess->rels_reservation.bytes)
        CHECK(wl_columnar_memory_release(&sess->rels_reservation));
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        free(sess->arr_entries[i].rel_name);
        free(sess->arr_entries[i].key_cols);
        arr_free_contents(&sess->arr_entries[i].arr);
        col_arr_detach_memory_governor(&sess->arr_entries[i].arr);
    }
    free(sess->arr_entries);
    col_mat_cache_clear(&sess->mat_cache);
    session_rel_free_hash(sess);
    CHECK(!wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(sess->memory_governor)));
    wl_columnar_memory_governor_ref_release(sess->memory_governor);
    free(sess);
}
static wl_columnar_eval_stack_cleanup_frame_t *
join_frame(wl_col_session_t *sess, col_rel_t *left, bool owned)
{
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
    CHECK(eval_stack_push(wl_columnar_eval_stack_cleanup_stack(frame), left,
        owned) == 0);
    return frame;
}
static int
join_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_plan_op_t *op, wl_evaluation_control_t *c)
{
    sess->base.evaluation_control = c;
    wl_plan_relation_t plan = {.ops = op, .op_count = 1};
    int rc = col_eval_relation_plan(&plan,
            wl_columnar_eval_stack_cleanup_stack(frame), sess);
    sess->base.evaluation_control = NULL;
    return rc;
}
static col_rel_t *
join_output(wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    CHECK(stack->top == 1 && stack->items[0].owned);
    return stack->items[0].rel;
}
static void
join_multiset_equal(const col_rel_t *a, const col_rel_t *b)
{
    CHECK(a->ncols == b->ncols && a->nrows == b->nrows);
    for (uint32_t c = 0; c < a->ncols; c++)
        CHECK(a->column_types[c] == b->column_types[c]);
    bool *matched = calloc(b->nrows ? b->nrows : 1, sizeof(bool));
    CHECK(matched);
    for (uint32_t ar = 0; ar < a->nrows; ar++) {
        bool found = false;
        for (uint32_t br = 0; br < b->nrows; br++) {
            if (matched[br]) continue;
            bool equal = true;
            for (uint32_t c = 0; c < a->ncols; c++)
                if (a->columns[c][ar] != b->columns[c][br]) equal = false;
            if (equal) {
                matched[br] = true; found = true; break;
            }
        }
        CHECK(found);
    }
    free(matched);
}
static uint64_t
private_writer_begin_cost(void)
{
    uint64_t bytes = sizeof(wl_columnar_relation_mutation_single_t);
    /* Shape64; inert64/phase8/full clear/per-chunk8/final8; bind16;
     * nonce1/install8+96/writer1+counter8/resolve6+owner96/lease128/
     * writer1+counter8/identity16/validator32/HELD8; active16. */
    return 64u + 64u + 8u + bytes + ((bytes + 127u) / 128u) * 8u + 8u
           + 16u + 1u + 8u + 96u + 1u + 8u + 6u + 96u + 128u + 1u
           + 8u + 16u + 32u + 8u + 16u;
}
static void
test_controlled_join_cartesian(void)
{
    wl_col_session_t *sess = join_session(), *raw = join_session();
    col_rel_t *left = relation(1, 17, 17, false, sess->memory_governor);
    col_rel_t *right = relation(1, 19, 19, false, sess->memory_governor);
    col_rel_t *raw_left = relation(1, 17, 17, false, raw->memory_governor);
    col_rel_t *raw_right = relation(1, 19, 19, false, raw->memory_governor);
    CHECK(session_add_rel(sess, right) == 0 && session_add_rel(raw,
        raw_right) == 0);
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                       .delta_mode = WL_DELTA_FORCE_FULL};
    wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
            false);
    wl_columnar_eval_stack_cleanup_frame_t *reference = join_frame(raw,
            raw_left, false);
    wl_evaluation_control_t *c = control(0);
    CHECK(join_dispatch(sess, frame, &op, c) == 0);
    uint64_t cost = finish(c, 0);
    CHECK(join_dispatch(raw, reference, &op, NULL) == 0);
    CHECK(join_output(frame)->nrows == 323);
    join_multiset_equal(join_output(frame), join_output(reference));
    source_unpinned(left, left); source_unpinned(right, right);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
    frame = join_frame(sess, left, false); retry(c, cost - 1);
    CHECK(join_dispatch(sess, frame, &op,
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->top == 0);
    CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = join_frame(sess, left, false); retry(c, cost);
    CHECK(join_dispatch(sess, frame, &op, c) == 0 && finish(c, 0) == cost);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    release(c); destroy(left); destroy(raw_left);
    join_session_destroy(sess); join_session_destroy(raw);
}
static void
test_controlled_join_keyed_first(void)
{
    const char *keys[1] = {"col0"};
    for (int empty = 0; empty < 2; empty++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        uint32_t rows = empty ? 513 : 3;
        col_rel_t *left = relation(1, rows, rows, false, sess->memory_governor);
        col_rel_t *right = relation(1, rows, rows, false,
                sess->memory_governor);
        col_rel_t *raw_left = relation(1, rows, rows, false,
                raw->memory_governor);
        col_rel_t *raw_right = relation(1, rows, rows, false,
                raw->memory_governor);
        for (uint32_t row = 0; row < rows; row++) {
            left->columns[0][row] = raw_left->columns[0][row] = empty ? -1 : 7;
            right->columns[0][row] = raw_right->columns[0][row] =
                empty ? row : 7;
        }
        if (empty) {
            bool occupied[2048] = {false};
            for (uint32_t row = 0; row < rows; row++)
                occupied[wl_columnar_hash_value(2166136261u, right, 0,
                    right->columns[0][row]) & 2047u] = true;
            int64_t value = -1;
            while (occupied[wl_columnar_hash_value(2166136261u, left, 0,
                value) & 2047u]) value--;
            for (uint32_t row = 0; row < rows; row++)
                left->columns[0][row] = raw_left->columns[0][row] = value;
        }
        CHECK(session_add_rel(sess, right) == 0 && session_add_rel(raw,
            raw_right) == 0);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL, .left_keys = keys,
                           .right_keys = keys,
                           .key_count = 1};
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_columnar_eval_stack_cleanup_frame_t *reference = join_frame(raw,
                raw_left, false);
        wl_evaluation_control_t *c = control(0);
        CHECK(join_dispatch(sess, frame, &op, c) == 0);
        uint64_t cost = finish(c, 0);
        if (empty) {
            /* Entry guard40 + name/entry22 + grouped inputs98 + shape34,
             * arithmetic64 + bucket doubling11 + scratch16 + keys13 + types2,
             * fresh ctor161 + type BUILD186 + ledger56 + durable writer admission,
             * bucket zero2048 + build1539 + empty probes1026 + outer513 + final116. */
            CHECK(cost == 40 + 22 + 98 + 34 + 64 + 11 + 16 + 13 + 2
                + 161 + 186 + 56 + private_writer_begin_cost() + 2048 + 1539
                + 1026 + 513 + 116 + 96 + 64);
        }
        CHECK(join_dispatch(raw, reference, &op, NULL) == 0);
        CHECK(join_output(frame)->nrows == (empty ? 0u : 9u));
        join_multiset_equal(join_output(frame), join_output(reference));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        frame = join_frame(sess, left, false); retry(c, cost - 1);
        CHECK(join_dispatch(sess, frame, &op,
            c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = join_frame(sess, left, false); retry(c, cost);
        CHECK(join_dispatch(sess, frame, &op, c) == 0 && finish(c, 0) == cost);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        source_unpinned(left, left); source_unpinned(right, right);
        release(c); destroy(left); destroy(raw_left);
        join_session_destroy(sess); join_session_destroy(raw);
    }
}

/* Same-input NULL delegation, genuine hash collisions and typed BUILD output. */
static void
test_controlled_join_shapes(void)
{
    const char *keys[2] = {"col0", "col1"};
    for (int mode = 0; mode < 6; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        uint32_t width = mode == 0 ? 257 : mode == 2 ? 2 : 1;
        uint32_t rows = mode == 1 ? 300 : mode == 3 ? 2 : 1;
        col_rel_t *left = relation(width, 1, 1, false, sess->memory_governor);
        col_rel_t *right = relation(width, rows, rows, false,
                sess->memory_governor);
        col_rel_t *rl = relation(width, 1, 1, false, raw->memory_governor);
        col_rel_t *rr = relation(width, rows, rows, false,
                raw->memory_governor);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        if (mode == 1) {
            /* Choose real different values mapping to one actual 1024 bucket. */
            uint32_t target = wl_columnar_hash_value(2166136261u, left, 0,
                    left->columns[0][0]) & 1023u;
            uint32_t found = 0;
            for (int64_t value = 1; found < rows; value++)
                if ((wl_columnar_hash_value(2166136261u, right, 0,
                    value) & 1023u) == target)
                    right->columns[0][found] = rr->columns[0][found] = value,
                    found++;
            op.key_count = 1; op.left_keys = op.right_keys = keys;
        } else if (mode == 2) {
            uint32_t target = wl_columnar_hash_value(
                wl_columnar_hash_value(2166136261u, left, 0,
                left->columns[0][0]),
                left, 1, left->columns[1][0]) & 1u;
            int64_t value = left->columns[1][0] + 1;
            while ((wl_columnar_hash_value(
                    wl_columnar_hash_value(2166136261u, right, 0,
                    right->columns[0][0]),
                    right, 1, value) & 1u) != target) value++;
            right->columns[1][0] = rr->columns[1][0] = value;
            op.key_count = 2; op.left_keys = op.right_keys = keys;
        } else if (mode == 3 || mode == 4) {
            wirelog_column_type_t lt = mode ==
                3 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT32;
            wirelog_column_type_t rt = mode ==
                3 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
            left->nrows = rl->nrows = right->nrows = rr->nrows = 0;
            CHECK(col_rel_set_column_types(left, &lt, 1) == 0);
            CHECK(col_rel_set_column_types(rl, &lt, 1) == 0);
            CHECK(col_rel_set_column_types(right, &rt, 1) == 0);
            CHECK(col_rel_set_column_types(rr, &rt, 1) == 0);
            left->nrows = rl->nrows = 1; right->nrows = rr->nrows = rows;
            left->columns[0][0] = rl->columns[0][0] = 0;
            right->columns[0][0] = rr->columns[0][0] = 0;
            if (mode == 3) right->columns[0][1] = rr->columns[0][1] = INT64_MIN;
            op.key_count = 1; op.left_keys = op.right_keys = keys;
        } else if (mode == 5) {
            op.right_relation = "missing";
            static const uint32_t projection[2] = {0, UINT32_MAX};
            op.project_count = 2; op.project_indices = projection;
        }
        CHECK(session_add_rel(sess, right) == 0 && session_add_rel(raw,
            rr) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_columnar_eval_stack_cleanup_frame_t *reference = join_frame(raw, rl,
                false);
        wl_evaluation_control_t *c = control(0);
        CHECK(join_dispatch(sess, frame, &op, c) == 0);
        uint64_t cost = finish(c, 0);
        CHECK(join_dispatch(raw, reference, &op, NULL) == 0);
        join_multiset_equal(join_output(frame), join_output(reference));
        CHECK(join_output(frame)->nrows ==
            (mode == 0 ? 1u : mode == 3 ? 2u : 0u));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        frame = join_frame(sess, left, false); retry(c, cost - 1);
        CHECK(join_dispatch(sess, frame, &op,
            c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = join_frame(sess, left, false); retry(c, cost);
        CHECK(join_dispatch(sess, frame, &op, c) == 0 && finish(c, 0) == cost);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        source_unpinned(left, left); source_unpinned(right, right);
        release(c); destroy(left); destroy(rl);
        join_session_destroy(sess); join_session_destroy(raw);
    }
}
static int join_stop_stage;
static uint64_t join_stop_progress;
static bool join_positive_stop;
static unsigned join_stage_reached;
static void
join_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *left)
{
    (void)group; (void)left;
    if (stage != join_stop_stage || progress != join_stop_progress) return;
    wl_columnar_join_test_controlled_checkpoint = NULL;
    join_stage_reached++;
    if (join_positive_stop) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_controlled_join_group_cutoffs(void)
{
    const int stages[4] = {1, 6, 7, 8};
    for (int identical = 0; identical < 2; identical++)
        for (unsigned n = 0; n < 4; n++)
            for (int positive = 0; positive < 2; positive++) {
                wl_col_session_t *sess = join_session();
                col_rel_t *left = relation(1, 1, 1, false,
                        sess->memory_governor);
                col_rel_t *right = identical ? left : relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, right) == 0);
                make_lazy(left); if (!identical) make_lazy(right);
                wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN,
                                   .right_relation = "work",
                                   .delta_mode = WL_DELTA_FORCE_FULL};
                wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess,
                        left, false);
                wl_evaluation_control_t *c = control(0);
                join_stop_stage = stages[n];
                join_stop_progress = stages[n] == 1 ? (identical ? 1 : 2)
                    : stages[n] == 6 ? 1 : 0;
                join_positive_stop = positive; join_stage_reached = 0;
                wl_columnar_join_test_controlled_checkpoint = join_stop;
                int expected =
                    positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(join_dispatch(sess, frame, &op, c) == expected);
                CHECK(join_stage_reached == 1 && !sess->memory_budget_denied);
                finish(c, expected); lazy_unchanged(left);
                if (!identical) lazy_unchanged(right);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                frame = join_frame(sess, left, false); retry(c, 0);
                CHECK(join_dispatch(sess, frame, &op, c) == 0);
                finish(c, 0);
                CHECK(left->storage_owner == left &&
                    right->storage_owner == right);
                source_unpinned(left, left); source_unpinned(right, right);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                release(c);
                if (!identical) destroy(left);
                join_session_destroy(sess);
            }
    wl_columnar_join_test_controlled_checkpoint = NULL;
}

static wl_columnar_source_access_reader_t join_retained_reader;
static unsigned join_cleanup_reached;
static bool join_consume_writer;
static void
join_cleanup_fault(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *left)
{
    (void)progress;
    if (stage != 9) return;
    CHECK(!c && group->committed);
    wl_columnar_join_test_controlled_checkpoint = NULL;
    join_cleanup_reached++;
    if (join_consume_writer) {
        CHECK(group->descriptors[0].writer_held &&
            !group->descriptors[0].journal);
        CHECK(wl_columnar_source_access_writer_release(
                &group->descriptors[0].writer) == 0);
    } else CHECK(col_rel_source_reader_acquire(left,
            &join_retained_reader) == 0);
}
static void
test_controlled_join_committed_cleanup(void)
{
    for (int writer = 0; writer < 2; writer++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 1, 1, false, sess->memory_governor);
        col_rel_t *right = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, right) == 0);
        if (writer) make_lazy(left);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                true);
        wl_evaluation_control_t *c = control(0);
        join_cleanup_reached = 0; join_consume_writer = writer;
        wl_columnar_join_test_controlled_checkpoint = join_cleanup_fault;
        int rc = join_dispatch(sess, frame, &op, c);
        CHECK(rc == (writer ? EINVAL : EBUSY) && join_cleanup_reached == 1);
        finish(c, rc);
        CHECK(left->storage_owner == left &&
            left->storage_owner_identity == left->relation_identity);
        CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->top == 1);
        CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->items[0].rel ==
            left);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel
            && wl_columnar_eval_stack_cleanup_result(frame)->owned);
        if (!writer) {
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
                !frame);
            CHECK(sess->cleanup_pending);
            CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        } else CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); join_session_destroy(sess);
    }
}
static void
test_controlled_join_alias_and_winner(void)
{
    for (int mode = 0; mode < 4; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 2, 2, false, sess->memory_governor);
        col_rel_t *right = relation(1, 2, 2, false, sess->memory_governor);
        if (mode < 2) CHECK(col_rel_install_shared_view(right, left) == 0);
        else make_lazy(left);
        CHECK(session_add_rel(sess, right) == 0);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_evaluation_control_t *c = control(0);
        if (mode == 1) {
            join_stop_stage = 8; join_stop_progress = 0;
            join_positive_stop = false;
            join_stage_reached = 0;
            wl_columnar_join_test_controlled_checkpoint = join_stop;
        } else if (mode >= 2) {
            initializer_source = left; initializer_calls = 0;
            stop_after_initializer = mode == 3;
            wl_columnar_relation_test_before_writer_cas =
                compete_lazy_initializer;
        }
        int expected = mode == 1 ||
            mode == 3 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(join_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected);
        if (mode == 1) CHECK(join_stage_reached == 1);
        if (mode >= 2) CHECK(initializer_calls == 1 &&
                left->storage_owner == left
                && left->storage_owner_identity == left->relation_identity
                && left->storage_owner_generation == left->storage_generation);
        if (!expected) CHECK(join_output(frame)->nrows == 4);
        source_unpinned(left, left);
        source_unpinned(right, mode < 2 ? left : right);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); destroy(right); sess->nrels = 0; destroy(left);
        join_session_destroy(sess);
    }
    initializer_source = NULL;
}

static void
test_controlled_join_long_stage_stops(void)
{
    const char *key[1] = {"col0"};
    const int stages[4] = {2, 3, 4, 5};
    const uint64_t progress[4] = {300, 513, 513, 300};
    for (unsigned n = 0; n < 4; n++)
        for (int positive = 0; positive < 2; positive++) {
            wl_col_session_t *sess = join_session();
            uint32_t count = n == 3 ? 300 : 513;
            col_rel_t *left = relation(1, n == 3 ? 1 : 513,
                    n == 3 ? 1 : 513, false, sess->memory_governor);
            col_rel_t *right = relation(1, count, count, false,
                    sess->memory_governor);
            for (uint32_t row = 0; row < left->nrows;
                row++) left->columns[0][row] = -1;
            if (n == 3) {
                uint32_t target = wl_columnar_hash_value(2166136261u, left, 0,
                        -1) & 1023u;
                uint32_t found = 0;
                for (int64_t value = 0; found < count; value++)
                    if ((wl_columnar_hash_value(2166136261u, right, 0,
                        value) & 1023u) == target)
                        right->columns[0][found++] = value;
            }
            CHECK(session_add_rel(sess, right) == 0);
            wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                               .delta_mode = WL_DELTA_FORCE_FULL,
                               .left_keys = key,
                               .right_keys = key, .key_count = 1};
            wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess,
                    left, false);
            wl_evaluation_control_t *c = control(0);
            join_stop_stage = stages[n]; join_stop_progress = progress[n];
            join_positive_stop = positive; join_stage_reached = 0;
            wl_columnar_join_test_controlled_checkpoint = join_stop;
            int expected =
                positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(join_dispatch(sess, frame, &op, c) == expected);
            CHECK(join_stage_reached == 1 && !sess->memory_budget_denied);
            finish(c, expected);
            source_unpinned(left, left); source_unpinned(right, right);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = join_frame(sess, left, false); retry(c, 0);
            CHECK(join_dispatch(sess, frame, &op, c) == 0);
            CHECK(join_output(frame)->nrows == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            release(c); destroy(left); join_session_destroy(sess);
        }
}
static col_rel_t *join_second_initializer;
static bool join_second_positive;
static void
join_initialize_second(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate)
{
    if (gate != &join_second_initializer->descriptor_access) return;
    wl_columnar_relation_test_before_writer_cas = NULL;
    CHECK(col_rel_storage_owner_ensure_initialized(join_second_initializer) ==
        0);
    join_stage_reached++;
    if (join_second_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_controlled_join_second_admission(void)
{
    for (int positive = 0; positive < 2; positive++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 1, 1, false, sess->memory_governor);
        col_rel_t *right = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, right) == 0);
        make_lazy(left); make_lazy(right);
        join_second_initializer = right; join_stage_reached = 0;
        join_second_positive = positive;
        wl_columnar_relation_test_before_writer_cas = join_initialize_second;
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_evaluation_control_t *c = control(0);
        int expected = positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(join_dispatch(sess, frame, &op, c) == expected);
        CHECK(join_stage_reached == 1); finish(c, expected);
        lazy_unchanged(left); source_unpinned(right, right);
        CHECK(right->storage_owner == right &&
            right->storage_owner_identity == right->relation_identity);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(col_rel_storage_owner_ensure_initialized(left) == 0);
        release(c); destroy(left); join_session_destroy(sess);
        join_second_initializer = NULL;
    }
}

extern bool wl_columnar_join_test_fail_next_key_scratch_alloc;
static void
test_controlled_join_errors_and_zero(void)
{
    const char *key[1] = {"col0"};
    for (int mode = 0; mode < 8; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        uint32_t width = mode >= 3 && mode < 7 ? 0 : 1;
        uint32_t rows = mode == 3 ? 0 : 1;
        col_rel_t *left = relation(width, rows, rows, false,
                sess->memory_governor);
        col_rel_t *right = relation(mode == 6 ? 0 : 1, 1, 1, false,
                sess->memory_governor);
        col_rel_t *rl = relation(width, rows, rows, false,
                raw->memory_governor);
        col_rel_t *rr = relation(mode == 6 ? 0 : 1, 1, 1, false,
                raw->memory_governor);
        CHECK(session_add_rel(sess, right) == 0 && session_add_rel(raw,
            rr) == 0);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL, .left_keys = key,
                           .right_keys = key,
                           .key_count = mode == 5 || mode == 6 ? 0u : 1u};
        static const uint32_t invalid_projection[1] = {UINT32_MAX};
        if (mode == 7) {
            op.project_count = 1; op.project_indices = invalid_projection;
        }
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_columnar_eval_stack_cleanup_frame_t *reference = join_frame(raw, rl,
                false);
        wl_columnar_memory_reservation_t quota;
        wl_columnar_memory_reservation_init(&quota);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        uint64_t baseline = wl_columnar_memory_reserved(g);
        if (mode == 0) {
            CHECK(wl_columnar_memory_reserve_checked(g,
                g->budget_bytes - baseline, &quota)
                == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        } else if (mode ==
            1) wl_columnar_join_test_fail_next_key_scratch_alloc = true;
        else if (mode ==
            2) sess->join_output_limit = raw->join_output_limit = 1;
        wl_evaluation_control_t *c = control(0);
        int expected = mode == 0 ? ENOSPC : mode == 1 ? ENOMEM : mode ==
            2 ? EOVERFLOW
            : mode == 4 ? EINVAL : 0;
        CHECK(join_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected);
        CHECK(sess->memory_budget_denied == (mode == 0));
        if (mode ==
            1) CHECK(!wl_columnar_join_test_fail_next_key_scratch_alloc);
        if (mode == 2) {
            CHECK(sess->join_output_peak == 1);
            CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->top == 0);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
        }
        if (mode < 2) {
            CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->top == 1);
            CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->items[0].rel ==
                left);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
        }
        if (mode >= 2 && mode != 4) {
            CHECK(join_dispatch(raw, reference, &op, NULL) == expected);
            if (!expected) join_multiset_equal(join_output(frame),
                    join_output(reference));
        }
        if (mode == 0) CHECK(wl_columnar_memory_release(&quota));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        source_unpinned(left, left); source_unpinned(right, right);
        release(c); destroy(left); destroy(rl);
        join_session_destroy(sess); join_session_destroy(raw);
    }
}

static int join_reader_cleanup_mode;
static void
join_consume_reader(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *left)
{
    (void)left;
    if (stage != 6 || progress) return;
    wl_columnar_join_test_controlled_checkpoint = NULL;
    CHECK(group->owner_count == 2 && group->owners[1].held);
    CHECK(wl_columnar_source_access_reader_release(&group->owners[1].reader) ==
        0);
    join_stage_reached++;
    if (join_reader_cleanup_mode == 1) wl_evaluation_control_request_cancel(c);
    if (join_reader_cleanup_mode == 2)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
}
static void
test_controlled_join_cleanup_precedence(void)
{
    for (int mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 1, 1, false, sess->memory_governor);
        col_rel_t *right = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, right) == 0); make_lazy(left);
        make_lazy(right);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_evaluation_control_t *c = control(0);
        join_reader_cleanup_mode = mode; join_stage_reached = 0;
        wl_columnar_join_test_controlled_checkpoint = join_consume_reader;
        int expected = mode == 2 ? EOVERFLOW : EINVAL;
        CHECK(join_dispatch(sess, frame, &op, c) == expected);
        CHECK(join_stage_reached == 1); finish(c, expected);
        lazy_unchanged(left); lazy_unchanged(right);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(col_rel_storage_owner_ensure_initialized(left) == 0);
        release(c); destroy(left); join_session_destroy(sess);
    }
}

static unsigned join_entry_reached;
static bool join_entry_positive;
static void
join_entry_stop(wl_evaluation_control_t *c)
{
    wl_columnar_eval_plan_test_entry_checkpoint = NULL;
    join_entry_reached++;
    if (join_entry_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_controlled_join_entry_refusal(void)
{
    /* VARIABLE->JOIN proves no raw VARIABLE lookup. VARIABLE->MAP additionally
    * would enter the actual iteration>0 pre-switch full-plan/registry scan. */
    for (int mode = 0; mode < 21; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 1, 1, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        eval_entry_t before; memcpy(&before, &stack->items[0], sizeof(before));
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(sess->memory_governor));
        char name[601]; memset(name, 'n', 600); name[600] = 0;
        wl_plan_op_t ops[2] = {{.op = WL_PLAN_OP_JOIN,
                                .right_relation = name,
                                .delta_mode = WL_DELTA_FORCE_FULL}, {0}};
        wl_plan_relation_t plan = {.ops = ops, .op_count = 1};
        wl_columnar_eval_tdd_run_t run = {.evaluating = true,
                                          .current_op = &ops[1]};
        atomic_uint_fast64_t shared; atomic_init(&shared, 0);
        wl_columnar_memory_governor_ref_t *governor = sess->memory_governor;
        if (mode < 2) {
            ops[0].op = WL_PLAN_OP_VARIABLE; ops[0].relation_name = name;
            ops[0].delta_mode = WL_DELTA_AUTO; sess->current_iteration = 1;
            ops[1].op = mode ? WL_PLAN_OP_MAP : WL_PLAN_OP_JOIN;
            ops[1].right_relation = name; plan.op_count = 2;
        } else if (mode == 2) ops[0].op = WL_PLAN_OP_VARIABLE;
        else if (mode == 3) plan.op_count = 0;
        else if (mode == 4) ops[0].op = WL_PLAN_OP_MAP;
        else if (mode == 5) sess->tdd_input_run = &run;
        else if (mode == 6) sess->num_workers = 2;
        else if (mode == 7) sess->diff_operators_active = true;
        else if (mode == 8) sess->join_batch_bytes = 1;
        else if (mode == 9) sess->join_output_shared_count = &shared;
        else if (mode == 10) ops[0].right_filter_expr.size = 1;
        else if (mode == 11) ops[0].materialized = true;
        else if (mode == 12) ops[0].delta_mode = WL_DELTA_AUTO;
        else if (mode == 13) sess->delta_seeded = true;
        else if (mode == 14) sess->retraction_seeded = true;
        else if (mode == 15) sess->retraction_right_pass = true;
        else if (mode == 16) sess->tdd_subpass_active = true;
        else if (mode == 17) sess->tdd_outbound_only_active = true;
        else if (mode == 18) sess->memory_governor = NULL;
        else if (mode == 19) {
            stack->items[0].is_delta = true;
            memcpy(&before, &stack->items[0], sizeof(before));
        } else {
            plan.op_count = 0;
        }
        wl_evaluation_control_t *c = control(mode == 20 ? 39 : 40);
        sess->base.evaluation_control = c;
        int expected = mode == 20 ? WL_EVALUATION_CONTROL_WORK_LIMIT : ENOTSUP;
        CHECK(col_eval_relation_plan(&plan, stack, sess) == expected);
        CHECK(finish(c, expected) == (mode == 20 ? 0u : 40u));
        CHECK(stack->top == 1 && !memcmp(&before, &stack->items[0],
            sizeof(before)));
        CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel &&
            !sess->cleanup_pending);
        CHECK(run.current_op == &ops[1] && left->storage_owner == left);
        source_unpinned(left, left);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) == baseline);
        sess->base.evaluation_control = NULL; sess->memory_governor = governor;
        sess->tdd_input_run = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); destroy(left); join_session_destroy(sess);
    }
    for (int positive = 0; positive < 2; positive++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 1, 1, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "missing",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_evaluation_control_t *c = control(0);
        join_entry_reached = 0; join_entry_positive = positive;
        wl_columnar_eval_plan_test_entry_checkpoint = join_entry_stop;
        int expected = positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(join_dispatch(sess, frame, &op,
            c) == expected && join_entry_reached == 1);
        CHECK(finish(c, expected) == 40);
        CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->top == 1);
        CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
        source_unpinned(left, left);
        retry(c, 0); CHECK(join_dispatch(sess, frame, &op, c) == 0);
        finish(c, 0);
        CHECK(join_output(frame)->nrows == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); destroy(left); join_session_destroy(sess);
    }
}

static void
test_controlled_join_group_reuse(void)
{
    wl_col_session_t *sess = join_session();
    col_rel_t *left = relation(1, 2, 2, false, sess->memory_governor);
    col_rel_t *right = relation(1, 2, 2, false, sess->memory_governor);
    CHECK(session_add_rel(sess, right) == 0);
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                       .delta_mode = WL_DELTA_FORCE_FULL};
    wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
            false);
    wl_evaluation_control_t *c = control(0);
    CHECK(join_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
    CHECK(join_output(frame)->nrows == 4 && join_output(frame)->ncols == 2);
    retry(c, 0);
    CHECK(join_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
    CHECK(join_output(frame)->nrows == 8 && join_output(frame)->ncols == 3);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    source_unpinned(left, left); source_unpinned(right, right);
    release(c); destroy(left); join_session_destroy(sess);

    /* Invalid entry owns an actual prior token. Rejection may neither clear
     * it nor steal its retirement; only the fixture releases its real pin. */
    left = relation(1, 1, 1, false, NULL);
    wl_columnar_relation_source_group_t group = {0}, before;
    CHECK(wl_columnar_source_access_reader_acquire(&left->descriptor_access,
        &group.descriptors[0].reader) == 0);
    memcpy(&before, &group, sizeof(before)); c = control(0);
    CHECK(wl_columnar_relation_source_group_acquire_controlled(&group, left,
        NULL, c) == EINVAL);
    CHECK(finish(c, EINVAL) == 80 && !memcmp(&before, &group, sizeof(before)));
    CHECK(atomic_load(&left->descriptor_access.state) == 1);
    CHECK(wl_columnar_source_access_reader_release(
            &group.descriptors[0].reader) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_source_group_acquire_controlled(&group, left,
        NULL, c) == 0);
    CHECK(wl_columnar_relation_source_group_drain(&group, 0) == 0);
    finish(c, 0); source_unpinned(left, left);
    release(c); destroy(left);
}

static void
test_controlled_join_empty_right_visits(void)
{
    const char *key[1] = {"col0"};
    for (int zero = 0; zero < 2; zero++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *left = relation(1, 513, 513, false, sess->memory_governor);
        col_rel_t *right = relation(zero ? 0 : 1, 0, 0, false,
                sess->memory_governor);
        col_rel_t *rl = relation(1, 513, 513, false, raw->memory_governor);
        col_rel_t *rr = relation(zero ? 0 : 1, 0, 0, false,
                raw->memory_governor);
        CHECK(session_add_rel(sess, right) == 0 && session_add_rel(raw,
            rr) == 0);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL,
                           .key_count = zero ? 1u : 0u,
                           .left_keys = key, .right_keys = key};
        wl_evaluation_control_t *c = control(0);
        for (int positive = 0; positive < 2; positive++) {
            if (positive) retry(c, 0);
            make_lazy(left); make_lazy(right);
            uint64_t retained = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(sess->memory_governor));
            wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess,
                    left, false);
            join_stop_stage = 10; join_stop_progress = 300;
            join_positive_stop = positive; join_stage_reached = 0;
            wl_columnar_join_test_controlled_checkpoint = join_stop;
            int expected =
                positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(join_dispatch(sess, frame, &op, c) == expected);
            CHECK(join_stage_reached == 1); finish(c, expected);
            lazy_unchanged(left); lazy_unchanged(right);
            CHECK(left->nrows == 513 && left->columns[0][512] == 512 &&
                !right->nrows);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            CHECK(atomic_load(&sess->mem_ledger.current_bytes) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(
                        sess->memory_governor)) == retained);
        }
        CHECK(col_rel_storage_owner_ensure_initialized(left) == 0);
        CHECK(col_rel_storage_owner_ensure_initialized(right) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_columnar_eval_stack_cleanup_frame_t *reference = join_frame(raw, rl,
                false);
        retry(c, 0); CHECK(join_dispatch(sess, frame, &op, c) == 0);
        uint64_t cost = finish(c, 0);
        /* Fixed entry/group/shape/scratch, actual type and constructor widths,
        * BUILD schema, one L request, B1 admission,513 visits and final116. */
        CHECK(cost == 40 + 22 + 98 + 33 + 64 + 16 + (zero ? 1u : 2u)
            + constructor_cost("$join", zero ? 1u : 2u, COL_REL_INIT_CAP, false)
            + (zero ? 169u : 186u) + 56 + private_writer_begin_cost()
            + 513 + 116 + 96 + 64);
        CHECK(join_dispatch(raw, reference, &op, NULL) == 0);
        join_multiset_equal(join_output(frame), join_output(reference));
        CHECK(!join_output(frame)->nrows &&
            join_output(frame)->ncols == (zero ? 1u : 2u));
        CHECK(join_output(frame)->schema.n_children ==
            join_output(reference)->schema.n_children);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        frame = join_frame(sess, left, false); retry(c, cost - 1);
        CHECK(join_dispatch(sess, frame, &op,
            c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = join_frame(sess, left, false); retry(c, cost);
        CHECK(join_dispatch(sess, frame, &op, c) == 0 && finish(c, 0) == cost);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        source_unpinned(left, left); source_unpinned(right, right);
        release(c); destroy(left); destroy(rl); join_session_destroy(sess);
        join_session_destroy(raw);
    }
}
static void
test_controlled_join_numeric_keys(void)
{
    char maximum[64], minimum[64], huge[1028], negative_huge[1029];
    snprintf(maximum, sizeof(maximum), "col%ld", LONG_MAX);
    snprintf(minimum, sizeof(minimum), "col%ld", LONG_MIN);
    memcpy(huge, "col", 3); memset(huge + 3, '9', 1024); huge[1027] = 0;
    memcpy(negative_huge, "col-", 4); memset(negative_huge + 4, '9', 1024);
    negative_huge[1028] = 0;
    const char *names[] = {"col4294967297", "col4294967296", "col2147483649",
                           "col+1", "col-0", "col-1", "col  +1", "col1x", "col",
                           "col+", "col  ",
                           maximum, minimum, huge, negative_huge,
                           "col4294967297"};
    for (unsigned n = 0; n < sizeof(names) / sizeof(names[0]); n++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *left = relation(2, 1, 1, false, sess->memory_governor);
        col_rel_t *right = relation(2, 1, 1, false, sess->memory_governor);
        col_rel_t *rl = relation(2, 1, 1, false, raw->memory_governor);
        col_rel_t *rr = relation(2, 1, 1, false, raw->memory_governor);
        left->columns[0][0] = rl->columns[0][0] = 1;
        right->columns[0][0] = rr->columns[0][0] = 2;
        left->columns[1][0] = right->columns[1][0] = rl->columns[1][0] =
            rr->columns[1][0] = 9;
        CHECK(session_add_rel(sess, right) == 0 && session_add_rel(raw,
            rr) == 0);
        if (n == sizeof(names) / sizeof(names[0]) - 1) {
            /* Explicit name precedence defeats the LP64 decimal wrap to1. */
            const char *explicit_names[2] = {"col4294967297", "other"};
            CHECK(col_rel_set_schema(left, 2, explicit_names) == 0);
            CHECK(col_rel_set_schema(right, 2, explicit_names) == 0);
            CHECK(col_rel_set_schema(rl, 2, explicit_names) == 0);
            CHECK(col_rel_set_schema(rr, 2, explicit_names) == 0);
        }
        int expected_index = col_rel_col_idx(left, names[n]);
        if (!n) {
#if LONG_MAX > INT32_MAX
            CHECK(expected_index == 1);
#else
            CHECK(expected_index < 0);
#endif
        }
        const char *keys[1] = {names[n]};
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL, .key_count = 1,
                           .left_keys = keys, .right_keys = keys};
        wl_evaluation_control_t *c = control(0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, left,
                false);
        wl_columnar_eval_stack_cleanup_frame_t *reference = join_frame(raw, rl,
                false);
        if (names[n] == huge || names[n] == negative_huge) {
            join_stop_stage = 11; join_stop_progress = 300;
            join_positive_stop = false;
            join_stage_reached = 0;
            wl_columnar_join_test_controlled_checkpoint = join_stop;
            CHECK(join_dispatch(sess, frame, &op,
                c) == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(join_stage_reached == 1);
            finish(c, WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = join_frame(sess, left, false); retry(c, 0);
        }
        CHECK(join_dispatch(sess, frame, &op, c) == 0);
        uint64_t cost = finish(c, 0);
        CHECK(join_dispatch(raw, reference, &op, NULL) == 0);
        join_multiset_equal(join_output(frame), join_output(reference));
        CHECK(join_output(frame)->nrows == (expected_index == 1 ? 1u : 0u));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        frame = join_frame(sess, left, false); retry(c, cost - 1);
        CHECK(join_dispatch(sess, frame, &op,
            c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = join_frame(sess, left, false); retry(c, cost);
        CHECK(join_dispatch(sess, frame, &op, c) == 0 && finish(c, 0) == cost);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        source_unpinned(left, left); source_unpinned(right, right);
        release(c); destroy(left); destroy(rl); join_session_destroy(sess);
        join_session_destroy(raw);
    }
}

extern void (*wl_columnar_filter_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t,
    wl_columnar_relation_source_group_t *, col_rel_t *);
extern bool wl_columnar_filter_test_fail_next_scratch_alloc;
extern bool wl_columnar_filter_test_fail_next_scratch_commit;
static int
filter_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_plan_expr_buffer_t expr,
    const char *name, wl_evaluation_control_t *c)
{
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .delta_mode = WL_DELTA_FORCE_FULL,
         .relation_name = name},
        {.op = WL_PLAN_OP_FILTER, .filter_expr = expr},
    };
    wl_plan_relation_t plan = {.ops = ops, .op_count = 2};
    sess->base.evaluation_control = c;
    int rc = col_eval_relation_plan(&plan,
            wl_columnar_eval_stack_cleanup_stack(frame), sess);
    sess->base.evaluation_control = NULL;
    return rc;
}
static wl_columnar_eval_stack_cleanup_frame_t *
filter_frame(wl_col_session_t *sess)
{
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
    return frame;
}
static wl_plan_expr_buffer_t
filter_expression(uint8_t *buf, wl_plan_expr_tag_t tag, bool variable,
    int64_t constant)
{
    uint16_t length = 4;
    uint32_t pos = 0;
    buf[pos++] = WL_PLAN_EXPR_VAR;
    memcpy(buf + pos, &length, 2); pos += 2;
    memcpy(buf + pos, "col0", 4); pos += 4;
    if (variable) {
        buf[pos++] = WL_PLAN_EXPR_VAR;
        memcpy(buf + pos, &length, 2); pos += 2;
        memcpy(buf + pos, "col1", 4); pos += 4;
    } else {
        buf[pos++] = WL_PLAN_EXPR_CONST_INT;
        memcpy(buf + pos, &constant, 8); pos += 8;
    }
    buf[pos++] = (uint8_t)tag;
    return (wl_plan_expr_buffer_t){.data = buf, .size = pos};
}
static void
filter_equal(const col_rel_t *actual, const col_rel_t *reference)
{
    CHECK(actual->ncols == reference->ncols);
    CHECK(actual->nrows == reference->nrows);
    CHECK(actual->declared_ncols == reference->declared_ncols);
    CHECK(actual->has_graph_column == reference->has_graph_column);
    CHECK(actual->graph_col_idx == reference->graph_col_idx);
    CHECK(actual->schema_ok == reference->schema_ok);
    CHECK((actual->column_types != NULL) == (reference->column_types != NULL));
    for (uint32_t c = 0; c < actual->ncols; c++) {
        CHECK(!strcmp(actual->col_names[c], reference->col_names[c]));
        if (actual->column_types)
            CHECK(actual->column_types[c] == reference->column_types[c]);
        CHECK(!strcmp(actual->schema.children[c]->name,
            reference->schema.children[c]->name));
        CHECK(!strcmp(actual->schema.children[c]->format,
            reference->schema.children[c]->format));
        for (uint32_t r = 0; r < actual->nrows; r++)
            CHECK(actual->columns[c][r] == reference->columns[c][r]);
    }
}
static void
test_controlled_filter_comparisons(void)
{
    const wl_plan_expr_tag_t tags[] = {WL_PLAN_EXPR_CMP_EQ,
                                       WL_PLAN_EXPR_CMP_NEQ,
                                       WL_PLAN_EXPR_CMP_LT,
                                       WL_PLAN_EXPR_CMP_LTE,
                                       WL_PLAN_EXPR_CMP_GT,
                                       WL_PLAN_EXPR_CMP_GTE};
    for (uint32_t t = 0; t < 6; t++) for (int variable = 0; variable < 2;
            variable++){
            wl_col_session_t *sess = join_session(), *raw = join_session();
            col_rel_t *source = relation(2, 513, 513, false,
                    sess->memory_governor);
            col_rel_t *ref = relation(2, 513, 513, false, raw->memory_governor);
            for (uint32_t r = 0; r < 513; r++) {
                source->columns[0][r] = ref->columns[0][r] = r % 3 ==
                    0 ? INT64_MIN
                : r % 3 == 1 ? INT64_MAX : -1;
                source->columns[1][r] = ref->columns[1][r] = -1;
            }
            if (variable) {
                wirelog_column_type_t types[2] = {WIRELOG_TYPE_UINT64,
                                                  WIRELOG_TYPE_UINT32};
                CHECK(col_rel_set_column_types(source, types,
                    source->ncols) == 0);
                CHECK(col_rel_set_column_types(ref, types, ref->ncols) == 0);
            }
            CHECK(session_add_rel(sess, source) == 0);
            CHECK(session_add_rel(raw, ref) == 0);
            uint8_t buf[32];
            wl_plan_expr_buffer_t expr = filter_expression(buf, tags[t],
                    variable, -1);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_eval_stack_cleanup_frame_t *reference =
                filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            CHECK(filter_dispatch(sess, frame, expr, "work", c) == 0);
            uint64_t cost = finish(c, 0);
            CHECK(filter_dispatch(raw, reference, expr, "work", NULL) == 0);
            filter_equal(join_output(frame), join_output(reference));
            source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, cost - 1);
            CHECK(filter_dispatch(sess, frame, expr, "work", c)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, cost);
            CHECK(filter_dispatch(sess, frame, expr, "work", c) == 0);
            CHECK(finish(c, 0) == cost);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
}
static void
test_controlled_filter_empty_metadata(void)
{
    for (int variant = 0; variant < 8; variant++) {
        uint32_t width = variant == 0 ? 0 : 3;
        uint32_t rows = variant < 3 ? 5 : 0;
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *source = relation(width, rows, rows, false,
                sess->memory_governor);
        col_rel_t *ref = relation(width, rows, rows, false,
                raw->memory_governor);
        source->declared_ncols = ref->declared_ncols = 19;
        source->graph_col_idx = ref->graph_col_idx = 2;
        if (variant >= 4) {
            source->has_graph_column = ref->has_graph_column = true;
        }
        if (variant == 5) {
            wirelog_column_type_t types[3] = {WIRELOG_TYPE_INT32,
                                              WIRELOG_TYPE_FLOAT,
                                              WIRELOG_TYPE_UINT64};
            CHECK(col_rel_set_column_types(source, types, source->ncols) == 0);
            CHECK(col_rel_set_column_types(ref, types, ref->ncols) == 0);
        }
        if (variant == 6) {
            ArrowSchemaRelease(&source->schema); source->schema_ok = false;
            ArrowSchemaRelease(&ref->schema); ref->schema_ok = false;
        }
        if (width) {
            free(source->col_names[0]); source->col_names[0] = strdup("");
            free(ref->col_names[0]); ref->col_names[0] = strdup("");
            free(source->col_names[1]); source->col_names[1] = strdup("custom");
            free(ref->col_names[1]); ref->col_names[1] = strdup("custom");
        }
        char **saved_names = source->col_names,
            **saved_ref_names = ref->col_names;
        if (variant == 7) source->col_names = ref->col_names = NULL;
        CHECK(session_add_rel(sess, source) == 0);
        CHECK(session_add_rel(raw, ref) == 0);
        uint8_t ignored = 0xff;
        wl_plan_expr_buffer_t expr = {.data = variant % 3 ==
                                          0 ? NULL : &ignored,
                                      .size = variant % 3 ==
                                          1 ? 0u : variant % 3 == 2 ? 0u : 7u};
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_columnar_eval_stack_cleanup_frame_t *reference = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        CHECK(filter_dispatch(sess, frame, expr, "work", c) == 0); finish(c, 0);
        CHECK(filter_dispatch(raw, reference, expr, "work", NULL) == 0);
        filter_equal(join_output(frame), join_output(reference));
        CHECK(!join_output(frame)->column_types || variant == 5);
        source->col_names = saved_names; ref->col_names = saved_ref_names;
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        release(c); join_session_destroy(sess); join_session_destroy(raw);
    }
}

static int filter_stop_stage;
static uint64_t filter_stop_progress;
static bool filter_positive;
static unsigned filter_reached;
static void
filter_stage_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    (void)group; (void)source;
    if (stage != filter_stop_stage || progress != filter_stop_progress) return;
    filter_reached++;
    wl_columnar_filter_test_controlled_checkpoint = NULL;
    if (filter_positive)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_controlled_filter_stops_and_lazy(void)
{
    const int stages[] = {1, 2, 3, 4, 5, 6, 7};
    const uint64_t progress[] = {0, 5, 300, 257, 1, 0, 0};
    for (unsigned i = 0; i < 7; i++) for (int positive = 0; positive < 2;
            positive++){
            wl_col_session_t *sess = join_session();
            uint32_t width = stages[i] == 4 ? 300 : 2;
            col_rel_t *source = relation(width, 513, 513, false,
                    sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            make_lazy(source);
            uint64_t baseline = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(sess->memory_governor));
            uint8_t buf[32]; wl_plan_expr_buffer_t expr = filter_expression(buf,
                    WL_PLAN_EXPR_CMP_GTE, false, 0);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            filter_stop_stage = stages[i]; filter_stop_progress = progress[i];
            filter_positive = positive; filter_reached = 0;
            wl_columnar_filter_test_controlled_checkpoint = filter_stage_stop;
            int expected =
                positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(filter_dispatch(sess, frame, expr, "work", c) == expected);
            CHECK(filter_reached == 1); finish(c, expected);
            lazy_unchanged(source);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(
                        sess->memory_governor)) == baseline);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(filter_dispatch(sess, frame, expr, "work", c) == 0);
            finish(c, 0); CHECK(join_output(frame)->nrows == 513);
            CHECK(source->storage_owner == source);
            source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            release(c); join_session_destroy(sess);
        }
    wl_columnar_filter_test_controlled_checkpoint = NULL;
    /* Independent real initializer wins between actual probe release/CAS. */
    for (int stop = 0; stop < 2; stop++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        initializer_source = source; initializer_calls = 0;
        stop_after_initializer = stop;
        wl_columnar_relation_test_before_writer_cas = compete_lazy_initializer;
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        int expected = stop ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, "work",
            c)
            == expected);
        CHECK(initializer_calls == 1); finish(c, expected);
        CHECK(source->storage_owner == source &&
            source->storage_owner_identity == source->relation_identity);
        source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        initializer_source = NULL; release(c); join_session_destroy(sess);
    }
}
static void
test_controlled_filter_refusals_and_faults(void)
{
    for (int mode = 0; mode < 7; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        uint32_t rows = mode == 0 ? 0 : 2;
        col_rel_t *source = relation(2, 2, rows, false, sess->memory_governor);
        col_rel_t *reference_source = relation(2, 2, rows, false,
                raw->memory_governor);
        CHECK(session_add_rel(sess, source) == 0);
        CHECK(session_add_rel(raw, reference_source) == 0);
        uint8_t buf[32]; wl_plan_expr_buffer_t expr = filter_expression(buf,
                WL_PLAN_EXPR_CMP_GTE, false, 0);
        int expected = 0;
        if (mode == 1) buf[0] = 0xff;
        if (mode == 2) buf[6] = '9';
        if (mode == 3) {
            wirelog_column_type_t types[2] = {WIRELOG_TYPE_FLOAT,
                                              WIRELOG_TYPE_INT64};
            source->nrows = 0;
            CHECK(col_rel_set_column_types(source, types, 2) == 0);
            source->nrows = rows;
        }
        if (mode == 4) {
            source->has_graph_column = true; source->graph_col_idx = 2;
            expr = (wl_plan_expr_buffer_t){0}; expected = EINVAL;
        }
        if (mode == 5) {
            wl_columnar_filter_test_fail_next_scratch_alloc = true;
            expected = ENOMEM;
        }
        if (mode == 6) {
            wl_columnar_filter_test_fail_next_scratch_commit = true;
            expected = ENOMEM;
        }
        uint64_t baseline = wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(sess->memory_governor));
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        CHECK(filter_dispatch(sess, frame, expr, "work", c) == expected);
        finish(c, expected); source_unpinned(source, source);
        CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(!sess->memory_budget_denied);
        CHECK(!wl_columnar_filter_test_fail_next_scratch_alloc &&
            !wl_columnar_filter_test_fail_next_scratch_commit);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(sess->memory_governor)) ==
            baseline);
        if (mode == 0) {
            frame = filter_frame(raw);
            CHECK(filter_dispatch(raw, frame, expr, "work", NULL) == 0);
            CHECK(join_output(frame)->nrows == 0 &&
                join_output(frame)->schema_ok);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        }
        source->has_graph_column = false;
        release(c); join_session_destroy(sess); join_session_destroy(raw);
    }
}
static void
filter_cleanup_fault(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    (void)progress;
    if (stage != 8) return;
    CHECK(!c && group->committed);
    wl_columnar_filter_test_controlled_checkpoint = NULL;
    filter_reached++;
    CHECK(col_rel_source_reader_acquire(source, &join_retained_reader) == 0);
}
static void
test_controlled_filter_owned_cleanup(void)
{
    wl_col_session_t *sess = join_session();
    col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
    wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
            true);
    wl_plan_op_t op = {.op = WL_PLAN_OP_FILTER};
    wl_evaluation_control_t *c = control(0);
    sess->base.evaluation_control = c; filter_reached = 0;
    wl_columnar_filter_test_controlled_checkpoint = filter_cleanup_fault;
    CHECK(wl_columnar_filter_op(&op,
        wl_columnar_eval_stack_cleanup_stack(frame), sess) == EBUSY);
    sess->base.evaluation_control = NULL; finish(c, EBUSY);
    CHECK(filter_reached == 1);
    CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->items[0].rel == source);
    CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY && !frame);
    CHECK(sess->cleanup_pending);
    CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
    release(c); join_session_destroy(sess);
}

static void
test_controlled_filter_rejected_cost_and_registry(void)
{
    wl_col_session_t *sess = join_session();
    col_rel_t *source = relation(1, 513, 513, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0);
    uint8_t buf[32]; wl_plan_expr_buffer_t expr = filter_expression(buf,
            WL_PLAN_EXPR_CMP_LT, false, -1);
    /* Remeasure actual durable-writer lifecycle on equal fresh output state;
     * this rejecting predicate has no callback/intern effects to warm. */
    wl_evaluation_control_t *c = control(0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    CHECK(filter_dispatch(sess, frame, expr, "work", c) == 0);
    uint64_t cost = finish(c, 0);
    CHECK(join_output(frame)->nrows == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, cost - 1);
    CHECK(filter_dispatch(sess, frame, expr, "work",
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, 0);
    CHECK(filter_dispatch(sess, frame, expr, NULL, c) == ENOENT);
    finish(c, ENOENT);
    CHECK(!wl_columnar_eval_stack_cleanup_stack(frame)->top);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, 0);
    CHECK(filter_dispatch(sess, frame, expr, "absent", c) == ENOENT);
    finish(c, ENOENT);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    release(c); join_session_destroy(sess);
    /* Real registered descriptor name longer than a work quantum. */
    char name[602]; memset(name, 'n', sizeof(name) - 1); name[601] = 0;
    sess = join_session(); source = NULL;
    CHECK(wl_columnar_relation_new_auto_governed(name, 0, 0, false,
        sess->memory_governor, &source) == 0);
    CHECK(session_add_rel(sess, source) == 0);
    c = control(0); frame = filter_frame(sess);
    CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, name,
        c) == 0);
    cost = finish(c, 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, cost - 1);
    CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, name, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, 0);
    join_stop_stage = 12; join_stop_progress = 300;
    join_positive_stop = false; join_stage_reached = 0;
    wl_columnar_join_test_controlled_checkpoint = join_stop;
    CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, name, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 56u + 16u + 1u + 300u);
    CHECK(join_stage_reached == 1);
    CHECK(!wl_columnar_eval_stack_cleanup_stack(frame)->top);
    source_unpinned(source, source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, cost);
    CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, name,
        c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    release(c); join_session_destroy(sess);
}
static void
test_controlled_filter_quota_and_build(void)
{
    for (int mode = 0; mode < 7; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(300, 1, 1, false, sess->memory_governor);
        wirelog_column_type_t types[300];
        for (uint32_t c = 0; c < 300; c++) types[c] = WIRELOG_TYPE_INT64;
        CHECK(col_rel_set_column_types(source, types, 300) == 0);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        uint64_t retained = wl_columnar_memory_reserved(g);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        uint64_t baseline = wl_columnar_memory_reserved(g);
        wl_columnar_memory_reservation_t quota;
        wl_columnar_memory_reservation_init(&quota);
        if (mode < 2) {
            CHECK(wl_columnar_memory_reserve_checked(g,
                g->budget_bytes - baseline - (mode == 1 ? 2400u : 0u), &quota)
                == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        }
        if (mode ==
            2) wl_columnar_relation_test_constructor_fail_allocation = 2;
        if (mode == 3) wl_columnar_relation_test_constructor_fail_commit = 2;
        if (mode >= 4) {
            constructor_cancel_phase = mode ==
                4 ? WL_COLUMNAR_RELATION_CONSTRUCTOR_SOURCE_NAME
                : mode == 5 ? WL_COLUMNAR_RELATION_CONSTRUCTOR_COLUMN_NAME_COPY
                : WL_COLUMNAR_RELATION_CONSTRUCTOR_TYPES;
            constructor_cancel_progress = mode == 6 ? 256 : 3;
            wl_columnar_relation_test_constructor_checkpoint =
                constructor_checkpoint;
        }
        wl_evaluation_control_t *c = control(0);
        int expected = mode < 2 ? ENOSPC : mode <
            4 ? ENOMEM : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, "work",
            c) == expected);
        finish(c, expected); lazy_unchanged(source);
        CHECK(sess->memory_budget_denied == (mode < 2));
        CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        if (mode < 2) CHECK(wl_columnar_memory_rollback(&quota));
        CHECK(wl_columnar_memory_reserved(g) == retained);
        wl_columnar_relation_test_constructor_checkpoint = NULL;
        constructor_cancel_phase = 0;
        frame = filter_frame(sess); retry(c, 0);
        CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, "work",
            c) == 0);
        finish(c, 0); CHECK(join_output(frame)->nrows == 1);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); join_session_destroy(sess);
    }
}
static void
test_controlled_filter_alias_float(void)
{
    for (int mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *owner = relation(2, 2, 0, false, sess->memory_governor);
        col_rel_t *ref = relation(2, 2, 0, false, raw->memory_governor);
        wirelog_column_type_t types[2] = {WIRELOG_TYPE_INT64,
                                          WIRELOG_TYPE_FLOAT};
        CHECK(col_rel_set_column_types(owner, types, 2) == 0);
        CHECK(col_rel_set_column_types(ref, types, 2) == 0);
        owner->nrows = ref->nrows = 2;
        owner->columns[0][0] = ref->columns[0][0] = 0;
        owner->columns[0][1] = ref->columns[0][1] = 1;
        owner->columns[1][0] = ref->columns[1][0] = INT64_MIN;
        owner->columns[1][1] = ref->columns[1][1] = mode == 2
            ? INT64_C(0x7ff8000000000001) : INT64_C(0x3ff0000000000000);
        col_rel_t *source = relation(2, 2, 0, false, sess->memory_governor);
        CHECK(col_rel_install_shared_view(source, owner) == 0);
        CHECK(session_add_rel(sess, source) == 0);
        CHECK(session_add_rel(raw, ref) == 0);
        uint8_t buf[32]; wl_plan_expr_buffer_t expr = filter_expression(buf,
                WL_PLAN_EXPR_CMP_GTE, false, 0);
        if (mode == 1) expr = (wl_plan_expr_buffer_t){0};
        wl_evaluation_control_t *c = control(0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_columnar_eval_stack_cleanup_frame_t *reference = filter_frame(raw);
        int expected = mode == 2 ? EINVAL : 0;
        CHECK(filter_dispatch(sess, frame, expr, "work", c) == expected);
        finish(c, expected);
        CHECK(filter_dispatch(raw, reference, expr, "work", NULL) == expected);
        if (!expected) {
            filter_equal(join_output(frame), join_output(reference));
            CHECK(join_output(frame)->columns[1][0] == 0);
        }
        CHECK(owner->columns[1][0] == INT64_MIN);
        source_unpinned(source, owner);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        release(c); destroy(source); sess->nrels = 0; destroy(owner);
        join_session_destroy(sess); join_session_destroy(raw);
    }
}

static int filter_cleanup_disposition;
static void
filter_reader_cleanup_fault(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_relation_source_group_t *group,
    col_rel_t *source)
{
    (void)source;
    if (stage != 5) return;
    if (!progress) {
        CHECK(group->descriptors[0].writer_held && group->descriptors[0].journal
            && group->owners[0].held);
        /* Actual lazy descriptor writer remains valid for journal rollback;
        * only the genuinely acquired canonical owner reader is consumed. */
        CHECK(wl_columnar_source_access_reader_release(
                &group->owners[0].reader) == 0);
        filter_reached++;
    } else {
        wl_columnar_filter_test_controlled_checkpoint = NULL;
        if (filter_cleanup_disposition ==
            1) wl_evaluation_control_request_cancel(c);
        if (filter_cleanup_disposition == 2)
            CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    }
}
static void
test_controlled_filter_cleanup_precedence(void)
{
    for (int mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        filter_cleanup_disposition = mode; filter_reached = 0;
        wl_columnar_filter_test_controlled_checkpoint =
            filter_reader_cleanup_fault;
        int expected = mode == 2 ? EOVERFLOW : EINVAL;
        CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, "work",
            c) == expected);
        finish(c, expected); CHECK(filter_reached == 1); lazy_unchanged(source);
        CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = filter_frame(sess); retry(c, 0);
        CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0}, "work",
            c) == 0);
        finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); join_session_destroy(sess);
    }
}

/* Malformed graph metadata precedes real resource and expression refusals. */
static void
test_controlled_filter_graph_precedence(void)
{
    for (int populated = 0; populated < 2; populated++)
        for (int failure = 0; failure < 5; failure++) {
            wl_col_session_t *sess = join_session(), *raw = join_session();
            uint32_t rows = populated ? 2u : 0u;
            col_rel_t *source = relation(2, 2, rows, false,
                    sess->memory_governor);
            col_rel_t *reference = relation(2, 2, rows, false,
                    raw->memory_governor);
            source->has_graph_column = reference->has_graph_column = true;
            source->graph_col_idx = reference->graph_col_idx = 2;
            CHECK(session_add_rel(sess, source) == 0);
            CHECK(session_add_rel(raw, reference) == 0);
            make_lazy(source);
            wl_columnar_memory_governor_t *g =
                wl_columnar_memory_governor_ref_get(sess->memory_governor);
            uint64_t retained = wl_columnar_memory_reserved(g);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_eval_stack_cleanup_frame_t *raw_frame =
                filter_frame(raw);
            uint64_t baseline = wl_columnar_memory_reserved(g);
            wl_columnar_memory_reservation_t quota;
            wl_columnar_memory_reservation_init(&quota);
            if (failure == 0)
                CHECK(wl_columnar_memory_reserve_checked(g,
                    g->budget_bytes - baseline,
                    &quota) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
            if (failure ==
                1) wl_columnar_filter_test_fail_next_scratch_alloc = true;
            if (failure ==
                2) wl_columnar_filter_test_fail_next_scratch_commit = true;
            /* Constructor hook must remain unconsumed for EVERY graph case. */
            wl_columnar_relation_test_constructor_fail_allocation = 1;
            uint8_t expr_buf[32];
            wl_plan_expr_buffer_t expr = {0};
            if (failure == 3) {
                expr_buf[0] = 0xff;
                expr = (wl_plan_expr_buffer_t){.data = expr_buf, .size = 1};
            }
            if (failure == 4)
                expr = filter_expression(expr_buf, WL_PLAN_EXPR_CMP_EQ, false,
                        0);
            wl_evaluation_control_t *c = control(0);
            CHECK(filter_dispatch(sess, frame, expr, "work", c) == EINVAL);
            /* Dispatch56 + VARIABLE30 + frame owner96 + FILTER16 + lazy group94 + shape16.
             * No expression, scratch or constructor primitive was reached. */
            CHECK(finish(c, EINVAL) == 308);
            CHECK(wl_columnar_relation_test_constructor_fail_allocation == 1);
            CHECK(wl_columnar_filter_test_fail_next_scratch_alloc ==
                (failure == 1));
            CHECK(wl_columnar_filter_test_fail_next_scratch_commit ==
                (failure == 2));
            CHECK(!sess->memory_budget_denied);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            lazy_unchanged(source);
            CHECK(source->has_graph_column && source->graph_col_idx == 2);
            CHECK(source->nrows == rows && source->ncols == 2);
            for (uint32_t col = 0; col < 2; col++)
                for (uint32_t row = 0; row < rows; row++)
                    CHECK(source->columns[col][row] ==
                        (int64_t)col * 10000 + row);
            CHECK(wl_columnar_memory_reserved(g) == baseline + quota.bytes);
            CHECK(filter_dispatch(raw, raw_frame, expr, "work",
                NULL) == EINVAL);
            CHECK(!raw->memory_budget_denied);
            CHECK(!wl_columnar_eval_stack_cleanup_result(raw_frame)->rel);
            wl_columnar_relation_test_constructor_fail_allocation = -1;
            wl_columnar_filter_test_fail_next_scratch_alloc = false;
            wl_columnar_filter_test_fail_next_scratch_commit = false;
            if (failure == 0) CHECK(wl_columnar_memory_rollback(&quota));
            CHECK(wl_columnar_memory_reserved(g) == baseline);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&raw_frame) == 0);
            CHECK(wl_columnar_memory_reserved(g) == retained);
            source->has_graph_column = reference->has_graph_column = false;
            frame = filter_frame(sess); raw_frame = filter_frame(raw);
            retry(c, 0);
            CHECK(filter_dispatch(sess, frame, (wl_plan_expr_buffer_t){0},
                "work", c) == 0);
            finish(c, 0);
            CHECK(filter_dispatch(raw, raw_frame, (wl_plan_expr_buffer_t){0},
                "work", NULL) == 0);
            filter_equal(join_output(frame), join_output(raw_frame));
            source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&raw_frame) == 0);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
}

extern void (*wl_columnar_expr_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t, bool);
extern int wl_columnar_expr_test_fail_allocation;
extern bool wl_columnar_expr_test_fail_commit;
static uint32_t
compiled_const(uint8_t *buf, uint32_t pos, int64_t value)
{
    buf[pos++] = WL_PLAN_EXPR_CONST_INT;
    memcpy(buf + pos, &value, 8);
    return pos + 8;
}
static uint32_t
compiled_var(uint8_t *buf, uint32_t pos, const char *name)
{
    uint16_t length = (uint16_t)strlen(name);
    buf[pos++] = WL_PLAN_EXPR_VAR; memcpy(buf + pos, &length, 2); pos += 2;
    memcpy(buf + pos, name, length); return pos + length;
}
static int
compiled_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_plan_op_t *ops,
    uint32_t count, wl_evaluation_control_t *c)
{
    wl_plan_relation_t plan = {.ops = ops, .op_count = count};
    sess->base.evaluation_control = c;
    int rc = col_eval_relation_plan(&plan,
            wl_columnar_eval_stack_cleanup_stack(frame), sess);
    sess->base.evaluation_control = NULL;
    return rc;
}
static void
test_compiled_filter_map_pipeline(void)
{
    wl_col_session_t *sess = join_session(), *raw = join_session();
    col_rel_t *source = relation(2, 513, 513, false, sess->memory_governor);
    col_rel_t *ref = relation(2, 513, 513, false, raw->memory_governor);
    source->declared_ncols = ref->declared_ncols = 19;
    source->graph_col_idx = ref->graph_col_idx = 1;
    CHECK(session_add_rel(sess, source) == 0);
    CHECK(session_add_rel(raw, ref) == 0);
    uint8_t predicate[64], map_buf[64], second_buf[64];
    uint32_t p = compiled_var(predicate, 0, "col0");
    p = compiled_const(predicate, p, 2);
    predicate[p++] = WL_PLAN_EXPR_ARITH_MUL;
    p = compiled_const(predicate, p, 100);
    predicate[p++] = WL_PLAN_EXPR_CMP_GTE;
    uint32_t m = compiled_var(map_buf, 0, "col0");
    m = compiled_const(map_buf, m, 1); map_buf[m++] = WL_PLAN_EXPR_ARITH_ADD;
    uint32_t n = compiled_var(second_buf, 0, "col1");
    second_buf[n++] = WL_PLAN_EXPR_ARITH_BNOT;
    wl_plan_expr_buffer_t expressions[2] = {{map_buf, m}, {second_buf, n}};
    uint32_t projections[4] = {1, 0, 1, 99};
    wl_plan_op_t ops[3] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_FILTER, .filter_expr = {predicate, p}},
        {.op = WL_PLAN_OP_MAP, .project_count = 4,
         .project_indices = projections,
         .map_exprs = expressions, .map_expr_count = 2},
    };
    wl_evaluation_control_t *c = control(0);
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_columnar_eval_stack_cleanup_frame_t *reference = filter_frame(raw);
    CHECK(compiled_dispatch(sess, frame, ops, 3, c) == 0);
    uint64_t cost = finish(c, 0);
    CHECK(compiled_dispatch(raw, reference, ops, 3, NULL) == 0);
    filter_equal(join_output(frame), join_output(reference));
    CHECK(join_output(frame)->nrows == 463 &&
        !join_output(frame)->column_types);
    CHECK(!join_output(frame)->has_graph_column &&
        join_output(frame)->graph_col_idx == 0
        && join_output(frame)->declared_ncols == 0);
    source_unpinned(source, source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, cost - 1);
    CHECK(compiled_dispatch(sess, frame, ops, 3,
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    frame = filter_frame(sess); retry(c, cost);
    CHECK(compiled_dispatch(sess, frame, ops, 3, c) == 0);
    CHECK(finish(c, 0) == cost);
    filter_equal(join_output(frame), join_output(reference));
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
    release(c); join_session_destroy(sess); join_session_destroy(raw);
}
static int compiled_stop_stage;
static uint64_t compiled_stop_progress;
static bool compiled_stop_positive, compiled_require_failed;
static unsigned compiled_reached;
static void
compiled_stage_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    if (stage != compiled_stop_stage ||
        progress != compiled_stop_progress) return;
    if (compiled_require_failed) CHECK(failed);
    wl_columnar_expr_test_controlled_checkpoint = NULL; compiled_reached++;
    if (compiled_stop_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_compiled_semantic_tail_precedence(void)
{
    for (int map = 0; map < 2; map++) for (int overflow = 0; overflow < 2;
            overflow++)
            for (int positive = 0; positive < 2; positive++) {
                wl_col_session_t *sess = join_session(), *raw = join_session();
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                col_rel_t *ref = relation(1, 1, 1, false, raw->memory_governor);
                CHECK(session_add_rel(sess, source) == 0);
                CHECK(session_add_rel(raw, ref) == 0);
                make_lazy(source);
                uint8_t buf[32];
                uint32_t size = compiled_const(buf, 0,
                        overflow ? INT64_MAX : 1);
                size = compiled_const(buf, size, overflow ? 1 : 0);
                buf[size++] =
                    overflow ? WL_PLAN_EXPR_ARITH_ADD : WL_PLAN_EXPR_ARITH_DIV;
                wl_plan_expr_buffer_t expr = {buf, size};
                wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                     .filter_expr = expr, .map_exprs = &expr,
                     .map_expr_count = 1, .project_count = 1},
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_columnar_eval_stack_cleanup_frame_t *reference =
                    filter_frame(raw);
                wl_evaluation_control_t *c = control(0);
                compiled_stop_stage = 3; compiled_stop_progress = 3;
                compiled_stop_positive = positive;
                compiled_require_failed = true; compiled_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint =
                    compiled_stage_stop;
                int expected =
                    positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected); CHECK(compiled_reached == 1);
                lazy_unchanged(source);
                CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                frame = filter_frame(sess); retry(c, 0);
                compiled_require_failed = false;
                expected = map ? ERANGE : 0;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(compiled_dispatch(raw, reference, ops, 2,
                    NULL) == expected);
                if (!map) {
                    CHECK(!join_output(frame)->nrows);
                    filter_equal(join_output(frame), join_output(reference));
                }
                source_unpinned(source, source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
                release(c); join_session_destroy(sess);
                join_session_destroy(raw);
            }
    /* Direct interface leaves both sentinel outputs untouched on execution rc. */
    uint8_t buf[32]; uint32_t size = compiled_const(buf, 0, 1);
    size = compiled_const(buf, size, 0); buf[size++] = WL_PLAN_EXPR_ARITH_DIV;
    wl_evaluation_control_t *c = control(0);
    wl_columnar_expr_compiled_t *expr = NULL; bool supported;
    CHECK(wl_columnar_expr_compile_controlled(buf, size, NULL, NULL, &expr,
        &supported, c) == 0 && supported);
    int64_t value = 123; bool failed = false;
    compiled_stop_stage = 3; compiled_stop_progress = 3;
    compiled_stop_positive = false;
    compiled_require_failed = true; compiled_reached = 0;
    wl_columnar_expr_test_controlled_checkpoint = compiled_stage_stop;
    CHECK(wl_columnar_expr_eval_compiled_controlled(expr, NULL, 0, &value,
        &failed, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(value == 123 && !failed && compiled_reached == 1);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    wl_columnar_expr_compiled_free_controlled(expr, c); release(c);
}

static void
test_compiled_long_stops(void)
{
    uint8_t buf[1026];
    for (uint32_t i = 0; i < 513; i++) {
        buf[2*i] = WL_PLAN_EXPR_BOOL; buf[2*i+1] = 0;
    }
    buf[sizeof(buf) - 1] = 1; /* Actual saturated stack must ignore this push. */
    wl_plan_expr_buffer_t expr = {buf, sizeof(buf)};
    for (int map = 0; map < 2; map++) for (int stage = 1; stage <= 4; stage++)
            for (int positive = 0; positive < 2; positive++) {
                wl_col_session_t *sess = join_session();
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t retained =
                    wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(sess->memory_governor));
                wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1},
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                compiled_stop_stage = stage;
                compiled_stop_progress = stage == 4 ? 513 : 300;
                compiled_stop_positive = positive;
                compiled_require_failed = false; compiled_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint =
                    compiled_stage_stop;
                int expected =
                    positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected); CHECK(compiled_reached == 1);
                lazy_unchanged(source);
                CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            sess->memory_governor)) == retained);
                frame = filter_frame(sess); retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                finish(c, 0);
                CHECK(join_output(frame)->nrows == (map ? 1u : 0u));
                if (map) CHECK(join_output(frame)->columns[0][0] == 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                release(c); join_session_destroy(sess);
            }
    /* Independent actual compile/evaluate action totals, in-range VAR extra1. */
    wl_columnar_memory_governor_ref_t *g = fresh_governor();
    wl_evaluation_control_t *c = control(513u*73u+101u);
    wl_columnar_expr_compiled_t *compiled; bool supported;
    CHECK(wl_columnar_expr_compile_controlled(buf, sizeof(buf), g, NULL,
        &compiled, &supported, c) == 0 && supported);
    CHECK(finish(c, 0) == 513u*73u+101u);
    retry(c, 513u*33u+8u); int64_t value; bool failed;
    CHECK(wl_columnar_expr_eval_compiled_controlled(compiled, NULL, 0, &value,
        &failed, c) == 0);
    CHECK(!failed && value == 0 && finish(c, 0) == 513u*33u+8u);
    wl_columnar_expr_compiled_free_controlled(compiled, c); governor_empty(g);
    uint8_t var[16]; uint32_t size = compiled_var(var, 0, "col0"); retry(c, 0);
    CHECK(wl_columnar_expr_compile_controlled(var, size, g, NULL,
        &compiled, &supported, c) == 0 && supported); finish(c, 0);
    int64_t row = 9; retry(c, 42);
    CHECK(wl_columnar_expr_eval_compiled_controlled(compiled, &row, 1, &value,
        &failed, c) == 0);
    CHECK(value == 9 && !failed && finish(c, 0) == 42);
    retry(c, 41);
    CHECK(wl_columnar_expr_eval_compiled_controlled(compiled, NULL, 0, &value,
        &failed, c) == 0);
    CHECK(value == 0 && !failed && finish(c, 0) == 41);
    wl_columnar_expr_compiled_free_controlled(compiled, c); release(c);
    governor_empty(g); wl_columnar_memory_governor_ref_release(g);
}
static wl_columnar_memory_governor_t *compiled_quota_governor;
static wl_columnar_memory_reservation_t compiled_quota;
static void
compiled_quota_hook(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)c; (void)progress; (void)failed;
    if (stage != 5) return;
    wl_columnar_expr_test_controlled_checkpoint = NULL; compiled_reached++;
    CHECK(wl_columnar_memory_reserve_checked(compiled_quota_governor,
        compiled_quota_governor->budget_bytes -
        wl_columnar_memory_reserved(compiled_quota_governor),
        &compiled_quota) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
}
static void
test_compiled_zero_row_failures(void)
{
    uint8_t buf[32]; uint32_t size = compiled_const(buf, 0, 1);
    size = compiled_const(buf, size, 1); buf[size++] = WL_PLAN_EXPR_ARITH_ADD;
    wl_plan_expr_buffer_t expr = {buf, size};
    for (int map = 0; map < 2; map++) for (int failure = 0; failure < 5;
            failure++){
            wl_col_session_t *sess = join_session(), *raw = join_session();
            col_rel_t *source = relation(2, 0, 0, false, sess->memory_governor);
            col_rel_t *ref = relation(2, 0, 0, false, raw->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            CHECK(session_add_rel(raw, ref) == 0);
            make_lazy(source);
            compiled_quota_governor =
                wl_columnar_memory_governor_ref_get(sess->memory_governor);
            uint64_t retained =
                wl_columnar_memory_reserved(compiled_quota_governor);
            wl_columnar_memory_reservation_init(&compiled_quota);
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1},
            };
            if (failure == 0) {
                compiled_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint =
                    compiled_quota_hook;
            }
            if (failure == 1 ||
                failure ==
                2) wl_columnar_expr_test_fail_allocation = failure - 1;
            if (failure == 3) wl_columnar_expr_test_fail_commit = true;
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_eval_stack_cleanup_frame_t *reference =
                filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            int expected = failure == 0 ? ENOSPC : failure < 4 ? ENOMEM : 0;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            finish(c, expected);
            CHECK(sess->memory_budget_denied == (failure == 0));
            CHECK(wl_columnar_expr_test_fail_allocation == -1 &&
                !wl_columnar_expr_test_fail_commit);
            if (failure < 4) {
                lazy_unchanged(source);
                CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            }
            CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == 0);
            if (!expected) filter_equal(join_output(frame),
                    join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            if (failure == 0) {
                CHECK(compiled_reached == 1);
                CHECK(wl_columnar_memory_reserved(compiled_quota_governor) ==
                    retained + compiled_quota.bytes);
                CHECK(wl_columnar_memory_rollback(&compiled_quota));
            }
            CHECK(wl_columnar_memory_reserved(compiled_quota_governor) ==
                retained);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
    compiled_quota_governor = NULL;
}

extern void (*wl_columnar_ops_map_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t,
    wl_columnar_relation_source_group_t *, col_rel_t *);
static void
test_compiled_map_wide_sparse(void)
{
    for (int mode = 0; mode < 4; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *owner = relation(300, 3, 3, false, sess->memory_governor);
        col_rel_t *ref = relation(300, 3, 3, false, raw->memory_governor);
        wirelog_column_type_t types[300];
        for (uint32_t c = 0; c < 300; c++) types[c] = WIRELOG_TYPE_INT64;
        types[299] = WIRELOG_TYPE_FLOAT;
        owner->nrows = ref->nrows = 0;
        CHECK(col_rel_set_column_types(owner, types, 300) == 0);
        CHECK(col_rel_set_column_types(ref, types, 300) == 0);
        owner->nrows = ref->nrows = 3;
        owner->columns[299][0] = ref->columns[299][0] = INT64_MIN;
        owner->columns[299][1] = ref->columns[299][1] =
            INT64_C(0x3ff0000000000000);
        owner->columns[299][2] = ref->columns[299][2] =
            INT64_C(0x7ff8000000000001);
        owner->has_graph_column = ref->has_graph_column = true;
        owner->graph_col_idx = ref->graph_col_idx = 299;
        owner->declared_ncols = ref->declared_ncols = 1000;
        col_rel_t *source = relation(300, 3, 0, false, sess->memory_governor);
        CHECK(col_rel_install_shared_view(source, owner) == 0);
        CHECK(session_add_rel(sess, source) == 0);
        CHECK(session_add_rel(raw, ref) == 0);
        uint32_t projections[513];
        wl_plan_expr_buffer_t expressions[520] = {0};
        for (uint32_t c = 0; c < 513; c++) projections[c] = c % 350;
        uint8_t buf[32]; uint32_t size = compiled_var(buf, 0, "col0");
        size = compiled_const(buf, size, 2);
        buf[size++] = WL_PLAN_EXPR_ARITH_ADD;
        expressions[0] = (wl_plan_expr_buffer_t){buf, size};
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = WL_PLAN_OP_MAP, .project_count = mode == 3 ? 0u : 513u,
             .project_indices = mode == 2 ? NULL : projections,
             .map_exprs = expressions,
             .map_expr_count = mode == 0 ? 0u : mode == 1 ? 2u : 520u},
        };
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_columnar_eval_stack_cleanup_frame_t *reference = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
        uint64_t cost = finish(c, 0);
        CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == 0);
        filter_equal(join_output(frame), join_output(reference));
        CHECK(join_output(frame)->nrows == 3 &&
            !join_output(frame)->column_types
            && !join_output(frame)->has_graph_column &&
            !join_output(frame)->declared_ncols);
        if (mode !=
            3) CHECK(join_output(frame)->columns[299][2] ==
                INT64_C(0x7ff8000000000001));
        source_unpinned(source, owner);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = filter_frame(sess); retry(c, cost - 1);
        CHECK(compiled_dispatch(sess, frame, ops, 2,
            c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = filter_frame(sess); retry(c, cost);
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
        CHECK(finish(c, 0) == cost);
        filter_equal(join_output(frame), join_output(reference));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        release(c); destroy(source); sess->nrels = 0; destroy(owner);
        join_session_destroy(sess); join_session_destroy(raw);
    }
}
static void
map_stage_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    wl_columnar_filter_test_controlled_checkpoint = filter_stage_stop;
    filter_stage_stop(c, stage, progress, group, source);
    if (!wl_columnar_filter_test_controlled_checkpoint)
        wl_columnar_ops_map_test_controlled_checkpoint = NULL;
    wl_columnar_filter_test_controlled_checkpoint = NULL;
}
static void
test_compiled_map_wide_stops(void)
{
    for (int stage = 2; stage <= 4; stage++) for (int positive =
            0; positive < 2;
            positive++){
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(300, 1, 1, false,
                    sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            wl_plan_expr_buffer_t expressions[513] = {0};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = WL_PLAN_OP_MAP, .project_count = 513,
                 .map_exprs = expressions, .map_expr_count = 1},
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            filter_stop_stage = stage; filter_stop_progress = 257;
            filter_positive = positive; filter_reached = 0;
            wl_columnar_ops_map_test_controlled_checkpoint = map_stage_stop;
            int expected =
                positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            finish(c, expected);
            CHECK(filter_reached == 1); lazy_unchanged(source);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            release(c); join_session_destroy(sess);
        }
}
static void
map_owned_cleanup_fault(wl_evaluation_control_t *c, int stage,
    uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    if (stage != 8) return;
    wl_columnar_ops_map_test_controlled_checkpoint = NULL;
    filter_cleanup_fault(c, stage, progress, group, source);
}
static void
test_compiled_map_owned_cleanup(void)
{
    wl_col_session_t *sess = join_session();
    col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
    wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
            true);
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 2};
    wl_evaluation_control_t *c = control(0);
    sess->base.evaluation_control = c; filter_reached = 0;
    wl_columnar_ops_map_test_controlled_checkpoint = map_owned_cleanup_fault;
    CHECK(col_op_map(&op, wl_columnar_eval_stack_cleanup_stack(frame),
        sess) == EBUSY);
    sess->base.evaluation_control = NULL; finish(c, EBUSY);
    CHECK(filter_reached == 1 &&
        wl_columnar_eval_stack_cleanup_stack(frame)->items[0].rel == source);
    CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY && !frame &&
        sess->cleanup_pending);
    CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
    release(c); join_session_destroy(sess);
}
static void
test_compiled_arithmetic_stack_twins(void)
{
    const wl_plan_expr_tag_t operators[] = {WL_PLAN_EXPR_ARITH_ADD,
                                            WL_PLAN_EXPR_ARITH_SUB,
                                            WL_PLAN_EXPR_ARITH_MUL,
                                            WL_PLAN_EXPR_ARITH_DIV,
                                            WL_PLAN_EXPR_ARITH_MOD,
                                            WL_PLAN_EXPR_ARITH_SHL,
                                            WL_PLAN_EXPR_ARITH_SHR,
                                            WL_PLAN_EXPR_ARITH_BAND,
                                            WL_PLAN_EXPR_ARITH_BOR,
                                            WL_PLAN_EXPR_ARITH_BXOR,
                                            WL_PLAN_EXPR_ARITH_BNOT,
                                            WL_PLAN_EXPR_CMP_EQ,
                                            WL_PLAN_EXPR_CMP_NEQ,
                                            WL_PLAN_EXPR_CMP_LT,
                                            WL_PLAN_EXPR_CMP_LTE,
                                            WL_PLAN_EXPR_CMP_GT,
                                            WL_PLAN_EXPR_CMP_GTE,
                                            WL_PLAN_EXPR_AGG_COUNT,
                                            WL_PLAN_EXPR_AGG_SUM,
                                            WL_PLAN_EXPR_AGG_MIN,
                                            WL_PLAN_EXPR_AGG_MAX};
    for (unsigned i = 0; i < sizeof(operators)/sizeof(operators[0]); i++)
        for (int boundary = 0; boundary < 3; boundary++) {
            uint8_t buf[32]; uint32_t size = 0;
            if (boundary != 2) {
                size = compiled_const(buf, size, boundary ? INT64_MIN : 5);
                size = compiled_const(buf, size, boundary ? -1 : 2);
            }
            buf[size++] = operators[i];
            wl_columnar_expr_compiled_t *raw = wl_columnar_expr_compile(buf,
                    size, NULL);
            CHECK(raw);
            int64_t expected_value = 456;
            int expected_failed = wl_columnar_expr_eval_compiled(raw, NULL, 0,
                    &expected_value);
            wl_evaluation_control_t *c = control(0);
            wl_columnar_expr_compiled_t *compiled; bool supported, failed;
            CHECK(wl_columnar_expr_compile_controlled(buf, size, NULL, NULL,
                &compiled, &supported, c) == 0 && supported);
            int64_t actual;
            CHECK(wl_columnar_expr_eval_compiled_controlled(compiled, NULL, 0,
                &actual, &failed, c) == 0);
            CHECK(failed == (expected_failed != 0) && actual == expected_value);
            finish(c, 0);
            wl_columnar_expr_compiled_free_controlled(compiled, c);
            wl_columnar_expr_compiled_free(raw); release(c);
        }
    /* Real caller mappings for divide/mod zero, invalid shifts and min/-1. */
    const wl_plan_expr_tag_t bad[] = {WL_PLAN_EXPR_ARITH_DIV,
                                      WL_PLAN_EXPR_ARITH_MOD,
                                      WL_PLAN_EXPR_ARITH_SHL,
                                      WL_PLAN_EXPR_ARITH_SHR,
                                      WL_PLAN_EXPR_ARITH_DIV};
    for (unsigned i = 0; i < 5; i++) for (int map = 0; map < 2; map++) {
            wl_col_session_t *sess = join_session(), *raw = join_session();
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            col_rel_t *ref = relation(1, 1, 1, false, raw->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            CHECK(session_add_rel(raw, ref) == 0);
            uint8_t buf[32];
            uint32_t size = compiled_const(buf, 0, i == 4 ? INT64_MIN : 1);
            size = compiled_const(buf, size, i == 4 ? -1 : i >= 2 ? 64 : 0);
            buf[size++] = bad[i];
            wl_plan_expr_buffer_t expr = {buf, size};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1},
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_eval_stack_cleanup_frame_t *reference =
                filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            int expected = map ? ERANGE : 0;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            finish(c, expected);
            CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == expected);
            if (!map) CHECK(!join_output(frame)->nrows &&
                    !join_output(reference)->nrows);
            source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
}

static uint32_t
numeric_float(uint8_t *buf, uint32_t pos, double value)
{
    buf[pos++] = WL_PLAN_EXPR_CONST_FLOAT;
    memcpy(buf + pos, &value, 8);
    return pos + 8;
}
/* Real ordinary dispatcher twins, including the private output constructor. */
static void
numeric_caller_twin(uint8_t *buf, uint32_t size, uint32_t rows,
    int expected_map, bool exact)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *source = relation(17, rows ? rows : 1, rows, false,
                sess->memory_governor);
        col_rel_t *ref = relation(17, rows ? rows : 1, rows, false,
                raw->memory_governor);
        for (uint32_t r = 0; r < rows; r++) {
            double d = r ? -0.0 : 1.5;
            memcpy(&source->columns[0][r], &d, 8);
            memcpy(&ref->columns[0][r], &d, 8);
            d = NAN;
            memcpy(&source->columns[1][r], &d, 8);
            memcpy(&ref->columns[1][r], &d, 8);
        }
        col_rel_t *alias_owner = NULL;
        if (rows && exact && buf[0] == WL_PLAN_EXPR_VAR_FLOAT) {
            alias_owner = source;
            source = relation(17, rows, 0, false, sess->memory_governor);
            CHECK(col_rel_install_shared_view(source, alias_owner) == 0);
        }
        CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
            ref) == 0);
        wl_plan_expr_buffer_t expr = {buf, size};
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
             .filter_expr = expr,
             .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
        };
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
            *reference = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        int expected = map && rows ? expected_map : 0;
        if (!rows) {
            compiled_stop_stage = 3; compiled_stop_progress = 1;
            compiled_stop_positive = false; compiled_require_failed = false;
            compiled_reached = 0;
            wl_columnar_expr_test_controlled_checkpoint = compiled_stage_stop;
        }
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
        if (!rows) {
            CHECK(compiled_reached == 0);
            wl_columnar_expr_test_controlled_checkpoint = NULL;
        }
        uint64_t cost = finish(c, expected);
        CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == expected);
        if (!expected) filter_equal(join_output(frame), join_output(reference));
        source_unpinned(source, alias_owner ? alias_owner : source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        if (exact && !expected) {
            frame = filter_frame(sess); retry(c, cost - 1);
            CHECK(compiled_dispatch(sess, frame, ops, 2,
                c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, cost);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0 && finish(c,
                0) == cost);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        release(c);
        if (alias_owner) {
            destroy(source); sess->nrels = 0; destroy(alias_owner);
        }
        join_session_destroy(sess); join_session_destroy(raw);
    }
}
static void
test_numeric_float_and_interpreter_twins(void)
{
    const uint8_t floats[] = {WL_PLAN_EXPR_ARITH_FLOAT_ADD,
                              WL_PLAN_EXPR_ARITH_FLOAT_SUB,
                              WL_PLAN_EXPR_ARITH_FLOAT_MUL,
                              WL_PLAN_EXPR_ARITH_FLOAT_DIV,
                              WL_PLAN_EXPR_CMP_FLOAT_EQ,
                              WL_PLAN_EXPR_CMP_FLOAT_NEQ,
                              WL_PLAN_EXPR_CMP_FLOAT_LT,
                              WL_PLAN_EXPR_CMP_FLOAT_GT,
                              WL_PLAN_EXPR_CMP_FLOAT_LTE,
                              WL_PLAN_EXPR_CMP_FLOAT_GTE};
    for (uint32_t k = 0; k < sizeof(floats); k++) {
        uint8_t buf[64]; uint32_t n = numeric_float(buf, 0, -0.0);
        n = numeric_float(buf, n, k == 3 ? 1.0 : 0x1p-1074);
        buf[n++] = floats[k];
        numeric_caller_twin(buf, n, 2, 0, k == 0);
        n = compiled_var(buf, 0, "col0"); buf[0] = WL_PLAN_EXPR_VAR_FLOAT;
        n = numeric_float(buf, n, 1.0); buf[n++] = floats[k];
        numeric_caller_twin(buf, n, 2, 0, k == 0);
    }
    const char *names[] = {"col  +0junk", "col-1", "col99",
                           "col0000000000000009tail",
                           "col9223372036854775807", "col+0000000000000",
                           "other", "col0", "col1"};
    for (uint32_t k = 0; k < sizeof(names)/sizeof(*names); k++) {
        uint8_t buf[96]; uint32_t n = compiled_var(buf, 0, names[k]);
        if (k == 7) buf[6] = 0; /* Embedded NUL, native length retained. */
        numeric_caller_twin(buf, n, 2, 0, k == 0);
        if (k != 7) {
            buf[0] = WL_PLAN_EXPR_VAR_FLOAT;
            numeric_caller_twin(buf, n, 2,
                k == 1 || k == 2 || k == 4 || k == 8 ? ERANGE : 0, false);
        }
    }
    uint8_t malformed[][12] = {{0xff, WL_PLAN_EXPR_CONST_STR},
                               {WL_PLAN_EXPR_CONST_FLOAT,
                                WL_PLAN_EXPR_CONST_STR},
                               {WL_PLAN_EXPR_VAR_FLOAT, 8, 0, 'c', 'o', 'l',
                                WL_PLAN_EXPR_CONST_STR}};
    const uint32_t lengths[] = {2, 2, 7};
    for (uint32_t k = 0; k < 3; k++) {
        numeric_caller_twin(malformed[k], lengths[k], 1, ERANGE, false);
        numeric_caller_twin(malformed[k], lengths[k], 0, 0, true);
    }
    /* Finite validation and computed result overflow are actual row errors. */
    const double bad[] = {INFINITY, NAN, 0x1.fffffffffffffp1023};
    for (uint32_t k = 0; k < 3; k++) {
        uint8_t buf[32]; uint32_t n = numeric_float(buf, 0, bad[k]);
        if (k == 2) {
            n = numeric_float(buf, n, 2.0);
            buf[n++] = WL_PLAN_EXPR_ARITH_FLOAT_MUL;
        }
        numeric_caller_twin(buf, n, 1, ERANGE, false);
        numeric_caller_twin(buf, n, 0, 0, true);
    }
}
static void
test_numeric_widened_failure_tail(void)
{
    for (int map = 0; map < 2; map++) for (int floating = 0; floating < 4;
            floating++)
            for (int action = 0; action < 3; action++) {
                wl_col_session_t *sess = join_session();
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint8_t buf[32];
                uint32_t size = compiled_var(buf, 0, "col4294967296");
                if (floating == 1) buf[0] = WL_PLAN_EXPR_VAR_FLOAT;
                if (floating == 2) size = numeric_float(buf, 0, NAN);
                if (floating == 3) {
                    size = compiled_var(buf, 0, "col0");
                    buf[0] = WL_PLAN_EXPR_VAR_FLOAT;
                    double nan = NAN; memcpy(&source->columns[0][0], &nan, 8);
                }
                wl_plan_expr_buffer_t expr = {buf, size};
                wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                uint64_t retained =
                    wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(sess->memory_governor));
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                bool unsafe = LONG_MAX > INT32_MAX || floating;
                int expected = unsafe && map ? ERANGE : 0;
                if (action && unsafe) {
                    compiled_stop_stage = 3; compiled_stop_progress = 1;
                    compiled_stop_positive = action == 2;
                    compiled_require_failed = true; compiled_reached = 0;
                    wl_columnar_expr_test_controlled_checkpoint =
                        compiled_stage_stop;
                    expected = action ==
                        2 ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                }
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                if (action && unsafe) {
                    CHECK(compiled_reached == 1); lazy_unchanged(source);
                }else source_unpinned(source, source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            sess->memory_governor)) == retained);
                frame = filter_frame(sess); retry(c, 0);
                expected = unsafe && map ? ERANGE : 0;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                release(c); join_session_destroy(sess);
            }
}
static void
test_numeric_float_zero_row_failures(void)
{
    uint8_t buf[32]; uint32_t size = numeric_float(buf, 0, 1.0);
    size = numeric_float(buf, size, 1.0);
    buf[size++] = WL_PLAN_EXPR_ARITH_FLOAT_ADD;
    wl_plan_expr_buffer_t expr = {buf, size};
    for (int map = 0; map < 2; map++) for (int failure = 0; failure < 5;
            failure++){
            wl_col_session_t *sess = join_session(), *raw = join_session();
            col_rel_t *source = relation(2, 0, 0, false, sess->memory_governor);
            col_rel_t *ref = relation(2, 0, 0, false, raw->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            CHECK(session_add_rel(raw, ref) == 0);
            make_lazy(source);
            compiled_quota_governor =
                wl_columnar_memory_governor_ref_get(sess->memory_governor);
            uint64_t retained =
                wl_columnar_memory_reserved(compiled_quota_governor);
            wl_columnar_memory_reservation_init(&compiled_quota);
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1},
            };
            if (failure == 0) {
                compiled_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint =
                    compiled_quota_hook;
            }
            if (failure == 1 ||
                failure ==
                2) wl_columnar_expr_test_fail_allocation = failure - 1;
            if (failure == 3) wl_columnar_expr_test_fail_commit = true;
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_eval_stack_cleanup_frame_t *reference =
                filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            int expected = failure == 0 ? ENOSPC : failure < 4 ? ENOMEM : 0;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            finish(c, expected);
            CHECK(sess->memory_budget_denied == (failure == 0));
            CHECK(wl_columnar_expr_test_fail_allocation == -1 &&
                !wl_columnar_expr_test_fail_commit);
            if (failure < 4) {
                lazy_unchanged(source);
                CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            }
            CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == 0);
            if (!expected) filter_equal(join_output(frame),
                    join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            if (failure == 0) {
                CHECK(compiled_reached == 1);
                CHECK(wl_columnar_memory_reserved(compiled_quota_governor) ==
                    retained + compiled_quota.bytes);
                CHECK(wl_columnar_memory_rollback(&compiled_quota));
            }
            CHECK(wl_columnar_memory_reserved(compiled_quota_governor) ==
                retained);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
    compiled_quota_governor = NULL;
}

static void
test_numeric_long_paths(void)
{
    for (int interpreted = 0; interpreted < 2; interpreted++) {
        uint8_t buf[4624]; uint32_t n = 0;
        for (uint32_t k = 0; k < 513; k++) {
            if (interpreted) {
                buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 0;
            }else n = numeric_float(buf, n, -0.0);
        }
        if (interpreted) {
            uint32_t begin = n; n = compiled_var(buf, n, "col0");
            buf[begin] = WL_PLAN_EXPR_VAR_FLOAT;
        }
        numeric_caller_twin(buf, n, 1, 0, true);
        for (int map = 0; map < 2; map++) for (int stage_index = 0;
                stage_index < 3; stage_index++)
                for (int positive = 0; positive < 2; positive++) {
                    wl_col_session_t *sess = join_session();
                    col_rel_t *source = relation(17, 1, 1, false,
                            sess->memory_governor);
                    CHECK(session_add_rel(sess, source) == 0);
                    make_lazy(source);
                    uint64_t retained =
                        wl_columnar_memory_reserved(
                        wl_columnar_memory_governor_ref_get(
                            sess->memory_governor));
                    wl_plan_expr_buffer_t expr = {buf, n};
                    wl_plan_op_t ops[2] = {
                        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                         .delta_mode = WL_DELTA_FORCE_FULL},
                        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                         .filter_expr = expr,
                         .project_count = 1, .map_exprs = &expr,
                         .map_expr_count = 1}
                    };
                    wl_columnar_eval_stack_cleanup_frame_t *frame =
                        filter_frame(sess);
                    wl_evaluation_control_t *c = control(0);
                    compiled_stop_stage = stage_index ==
                        2 ? 3 : interpreted ? 6 + stage_index : 1 + stage_index;
                    compiled_stop_progress = 300;
                    compiled_stop_positive = positive;
                    compiled_require_failed = false; compiled_reached = 0;
                    wl_columnar_expr_test_controlled_checkpoint =
                        compiled_stage_stop;
                    int expected =
                        positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                    CHECK(compiled_dispatch(sess, frame, ops, 2,
                        c) == expected); finish(c, expected);
                    CHECK(compiled_reached == 1); lazy_unchanged(source);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                    CHECK(wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(
                                sess->memory_governor)) == retained);
                    frame = filter_frame(sess); retry(c, 0);
                    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                    finish(c, 0);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                    release(c); join_session_destroy(sess);
                }
    }
}
static void
test_numeric_owned_interpreter_cleanup(void)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[16]; uint32_t n = compiled_var(buf, 0, "col0");
        buf[0] = WL_PLAN_EXPR_VAR_FLOAT;
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(filter_reached == 1 &&
            wl_columnar_eval_stack_cleanup_stack(frame)->items[0].rel ==
            source);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        release(c); join_session_destroy(sess);
    }
}

static uint32_t
string_literal(uint8_t *buf, uint32_t pos, const char *value, uint16_t length)
{
    buf[pos++] = WL_PLAN_EXPR_CONST_STR;
    memcpy(buf + pos, &length, 2); pos += 2;
    memcpy(buf + pos, value, length);
    return pos + length;
}
static uint64_t
string_reserved(wl_columnar_memory_governor_ref_t *ref)
{
    return ref ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref)) : 0;
}
static wl_intern_t *
string_table(wl_columnar_memory_governor_ref_t *ref, uint32_t entries)
{
    wl_intern_t *table = wl_intern_create(); CHECK(table);
    if (ref) CHECK(wl_intern_attach_memory_governor(table, ref) == 0);
    for (uint32_t i = 0; i < entries; i++) {
        char text[64]; snprintf(text, sizeof(text), "seed-%u", i);
        CHECK(wl_intern_put(table, text) == i);
    }
    return table;
}
static unsigned string_stop_stage;
static uint64_t string_stop_progress;
static bool string_stop_positive;
static unsigned string_stop_reached;
static void
string_intern_stop(wl_evaluation_control_t *c, unsigned stage,
    uint64_t progress)
{
    if (stage != string_stop_stage || progress < string_stop_progress) return;
    wl_intern_test_controlled_checkpoint = NULL; string_stop_reached++;
    if (string_stop_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
string_expr_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)failed;
    if ((unsigned)stage != string_stop_stage || progress < string_stop_progress)
        return;
    wl_columnar_expr_test_controlled_checkpoint = NULL; string_stop_reached++;
    if (string_stop_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
string_caller_twins(const uint8_t *buf, uint32_t size, int expected_map,
    int domain, bool exact)
{
    uint64_t measured[2] = {0};
    for (int map = 0; map < 2; map++) for (int trial = 0;
            trial < (exact ? 3 : 1);
            trial++) {
            wl_col_session_t *sess = join_session(), *raw = join_session();
            wl_columnar_memory_governor_ref_t *t = domain ==
                2 ? fresh_governor() : NULL;
            wl_columnar_memory_governor_ref_t *tr = domain ==
                2 ? fresh_governor() : NULL;
            if (domain >= 0) {
                sess->intern = string_table(domain ==
                        1 ? sess->memory_governor : t, 0);
                raw->intern = string_table(domain ==
                        1 ? raw->memory_governor : tr, 0);
                const char *seed[] = {"alpha", "beta", "",
                                      "\xc3\xa9x\xf0\x9f\x98\x80",
                                      "\xff\xe2x\x80z"};
                for (uint32_t i = 0; i < 5; i++) {
                    CHECK(wl_intern_put(sess->intern, seed[i]) == i);
                    CHECK(wl_intern_put(raw->intern, seed[i]) == i);
                }
            }
            col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
            col_rel_t *ref = relation(2, 2, 2, false, raw->memory_governor);
            source->columns[0][0] = ref->columns[0][0] = 0;
            source->columns[0][1] = ref->columns[0][1] = 1;
            source->columns[1][0] = ref->columns[1][0] = 1;
            source->columns[1][1] = ref->columns[1][1] = 99;
            CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
                ref) == 0);
            make_lazy(source);
            wl_plan_expr_buffer_t expr = {(uint8_t *)buf, size};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr, .project_count = 1,
                 .map_exprs = &expr, .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_eval_stack_cleanup_frame_t *reference =
                filter_frame(raw);
            wl_evaluation_control_t *c = control(trial == 1 ? measured[map] - 1
                : trial == 2 ? measured[map] : 0);
            int expected = map ? expected_map : 0;
            if (trial == 1) expected = WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            uint64_t cost = finish(c, expected);
            if (!trial) measured[map] = cost;
            if (trial == 2) CHECK(cost == measured[map]);
            CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) ==
                (map ? expected_map : 0));
            if (!expected) filter_equal(join_output(frame),
                    join_output(reference));
            if (!expected && size == 10 && buf[0] == WL_PLAN_EXPR_CONST_INT
                && (buf[9] == WL_PLAN_EXPR_STR_FN_STRLEN ||
                buf[9] == WL_PLAN_EXPR_STR_FN_STR_ORD)){
                int64_t operand; memcpy(&operand, buf + 1, 8);
                if (operand == 99) {
                    int64_t bits = domain < 0 ? 0 : -1;
                    col_rel_t *actual = join_output(frame);
                    CHECK(actual->nrows == (map || bits ? 2u : 0u));
                    if (map) CHECK(actual->columns[0][0] == bits &&
                            actual->columns[0][1] == bits);
                }
            }
            source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            if (sess->intern && !expected) {
                CHECK(wl_intern_count(sess->intern) ==
                    wl_intern_count(raw->intern));
                for (uint32_t id = 0; id < wl_intern_count(sess->intern); id++)
                    CHECK(strcmp(wl_intern_reverse(sess->intern, id),
                        wl_intern_reverse(raw->intern, id)) == 0);
            }
            wl_intern_free(sess->intern); wl_intern_free(raw->intern);
            sess->intern = raw->intern = NULL;
            release(c); join_session_destroy(sess); join_session_destroy(raw);
            if (t) {
                governor_empty(t); governor_empty(tr);
                wl_columnar_memory_governor_ref_release(t);
                wl_columnar_memory_governor_ref_release(tr);
            }
        }
}
static void
test_string_actual_caller_twins(void)
{
    const uint8_t comparisons[] = {WL_PLAN_EXPR_CMP_STR_EQ,
                                   WL_PLAN_EXPR_CMP_STR_NEQ,
                                   WL_PLAN_EXPR_CMP_STR_LT,
                                   WL_PLAN_EXPR_CMP_STR_LTE,
                                   WL_PLAN_EXPR_CMP_STR_GT,
                                   WL_PLAN_EXPR_CMP_STR_GTE};
    for (int domain = -1; domain < 3; domain++) {
        for (uint32_t k = 0; k < sizeof(comparisons); k++) {
            uint8_t buf[64]; uint32_t n = compiled_var(buf, 0, "col0");
            n = compiled_var(buf, n, "col1"); buf[n++] = comparisons[k];
            string_caller_twins(buf, n, 0, domain, k == 0);
            n = string_literal(buf, 0, "alpha", 5);
            n = string_literal(buf, n, "beta", 4); buf[n++] = comparisons[k];
            string_caller_twins(buf, n, 0, domain, false);
        }
        uint8_t buf[128]; uint32_t n = compiled_var(buf, 0, "col0");
        buf[0] = WL_PLAN_EXPR_VAR_STRING;
        string_caller_twins(buf, n, domain < 0 ? ERANGE : 0, domain, false);
        n = string_literal(buf, 0, "alpha\0hidden", 12);
        string_caller_twins(buf, n, 0, domain, true);
        n = compiled_const(buf, 0, 0); n = compiled_const(buf, n, 1);
        buf[n++] = WL_PLAN_EXPR_STR_FN_CAT;
        string_caller_twins(buf, n, 0, domain, true);
        for (int malformed = 0; malformed < 2; malformed++)
            for (int start = -1; start < 5; start++) {
                n = compiled_const(buf, 0, malformed ? 4 : 3);
                n = compiled_const(buf, n, start);
                n = compiled_const(buf, n, 2);
                buf[n++] = WL_PLAN_EXPR_STR_FN_SUBSTR;
                string_caller_twins(buf, n, 0, domain, false);
            }
        n = compiled_const(buf, 0, 99); n = compiled_const(buf, n, 1);
        buf[n++] = WL_PLAN_EXPR_STR_FN_CAT;
        string_caller_twins(buf, n, 0, domain, false);
    }
}
static void
test_string_long_literal_fresh_limits(void)
{
    char text[600]; memset(text, 'L', 599); text[599] = 0;
    uint8_t buf[620]; uint32_t n = string_literal(buf, 0, text, 599);
    for (int domain = 0; domain < 3; domain++)
        string_caller_twins(buf, n, 0, domain, true);
}
static void
test_string_intern_transaction_stops(void)
{
    char text[600]; memset(text, 'q', sizeof(text) - 1); text[599] = 0;
    const unsigned stages[] = {1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13};
    for (int managed = 0; managed < 2; managed++)
        for (uint32_t k = 0; k < sizeof(stages)/sizeof(*stages); k++)
            for (int positive = 0; positive < 2; positive++) {
                wl_columnar_memory_governor_ref_t *t =
                    managed ? fresh_governor() : NULL;
                uint32_t entries = stages[k] == 7 ? 64 :
                    stages[k] == 8 || stages[k] == 10 || stages[k] == 11 ||
                    stages[k] == 12 ? 768 : 0;
                wl_intern_t *table = string_table(t, entries);
                if (stages[k] == 4) CHECK(wl_intern_put(table,
                        text) == entries++);
                uint64_t baseline = string_reserved(t);
                string_stop_stage = stages[k]; string_stop_positive = positive;
                string_stop_progress = stages[k] == 1 || stages[k] == 2
                    || stages[k] == 3 || stages[k] == 4 ||
                    stages[k] == 8 ? 513 :
                    stages[k] == 10 ? 513 : stages[k] == 11 ? 3 : 0;
                string_stop_reached = 0;
                wl_intern_test_controlled_checkpoint = string_intern_stop;
                wl_evaluation_control_t *c = control(0);
                int64_t id = INT64_MIN; int status = -123; bool denied = true;
                int expected =
                    positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(wl_intern_put_controlled(table, text, &id, &status,
                    &denied, c) == expected);
                finish(c, expected); CHECK(string_stop_reached == 1);
                CHECK(id == INT64_MIN && status == -123 && denied);
                CHECK(wl_intern_count(table) == entries &&
                    string_reserved(t) == baseline);
                for (uint32_t i = 0; i < entries;
                    i++) CHECK(wl_intern_reverse(table, i));
                retry(c, 0);
                CHECK(wl_intern_put_controlled(table, text, &id, &status,
                    &denied, c) == 0 && status == 0 && !denied);
                finish(c, 0);
                CHECK(id == (stages[k] == 4 ? entries - 1 : entries));
                CHECK(strcmp(wl_intern_reverse(table, id), text) == 0);
                wl_intern_free(table); release(c);
                if (t) {
                    governor_empty(t);
                    wl_columnar_memory_governor_ref_release(t);
                }
            }
}

static void
test_string_caller_long_and_retained_stops(void)
{
    char text[600]; memset(text, 'r', 599); text[599] = 0;
    const unsigned stages[] = {14, 15, 16, 17, 3};
    for (int map = 0; map < 2; map++) for (int domain = 0; domain < 3; domain++)
            for (uint32_t k = 0; k < sizeof(stages)/sizeof(*stages); k++)
                for (int positive = 0; positive < 2; positive++) {
                    wl_col_session_t *sess = join_session();
                    wl_columnar_memory_governor_ref_t *t = domain ==
                        2 ? fresh_governor() :
                        domain == 1 ? sess->memory_governor : NULL;
                    sess->intern = string_table(t, 0);
                    CHECK(wl_intern_put(sess->intern, text) == 0);
                    char other[600]; memcpy(other, text, 600); other[598] = 's';
                    CHECK(wl_intern_put(sess->intern, other) == 1);
                    uint8_t buf[1400]; uint32_t n;
                    if (stages[k] == 14) {
                        n = compiled_var(buf, 0, "col0");
                        buf[0] = WL_PLAN_EXPR_VAR_STRING;
                    } else if (stages[k] == 15) {
                        n = compiled_const(buf, 0, 0);
                        n = compiled_const(buf, n, 1);
                        buf[n++] = WL_PLAN_EXPR_CMP_STR_EQ; /* rejected row, deep compare */
                    } else if (stages[k] == 17) {
                        n = string_literal(buf, 0, "\xe2x", 2);
                        n = compiled_const(buf, n, 0);
                        n = compiled_const(buf, n, 1);
                        buf[n++] = WL_PLAN_EXPR_STR_FN_SUBSTR;
                    } else if (stages[k] == 16) {
                        char literal[600];
                        memcpy(literal, text, sizeof(literal));
                        literal[0] = 'u';
                        n = string_literal(buf, 0, literal, 599);
                    } else n = string_literal(buf, 0, "published-after-stop",
                                20);
                    col_rel_t *source = relation(1, 1, 1, false,
                            sess->memory_governor);
                    source->columns[0][0] = 0;
                    CHECK(session_add_rel(sess, source) == 0);
                    make_lazy(source);
                    uint64_t before = string_reserved(t);
                    uint32_t count = wl_intern_count(sess->intern);
                    wl_plan_expr_buffer_t expr = {buf, n};
                    wl_plan_op_t ops[2] = {
                        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                         .delta_mode = WL_DELTA_FORCE_FULL},
                        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                         .filter_expr = expr, .project_count = 1,
                         .map_exprs = &expr, .map_expr_count = 1}
                    };
                    wl_columnar_eval_stack_cleanup_frame_t *frame =
                        filter_frame(sess);
                    wl_evaluation_control_t *c = control(0);
                    string_stop_stage = stages[k];
                    string_stop_positive = positive;
                    string_stop_progress = stages[k] == 14 || stages[k] == 15
                        || stages[k] == 16 ? 513 : 1;
                    string_stop_reached = 0;
                    wl_columnar_expr_test_controlled_checkpoint =
                        string_expr_stop;
                    int expected =
                        positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                    CHECK(compiled_dispatch(sess, frame, ops, 2,
                        c) == expected);
                    finish(c, expected); CHECK(string_stop_reached == 1);
                    lazy_unchanged(source);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                    bool retained = stages[k] == 3 || stages[k] == 17;
                    CHECK(wl_intern_count(sess->intern) == count + retained);
                    CHECK(string_reserved(t) == before + (t && retained ?
                        stages[k] == 3 ? 21 : 3 : 0));
                    if (retained) CHECK(wl_intern_reverse(sess->intern, count));
                    frame = filter_frame(sess); retry(c, 0);
                    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                    finish(c, 0);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                    CHECK(wl_intern_count(sess->intern) == count +
                        (stages[k] == 16 ||
                        stages[k] == 3 ? 1 : stages[k] == 17 ? 2 : 0));
                    CHECK(strcmp(wl_intern_reverse(sess->intern, 0),
                        text) == 0);
                    wl_intern_free(sess->intern); sess->intern = NULL;
                    release(c); join_session_destroy(sess);
                    if (domain == 2) {
                        governor_empty(t);
                        wl_columnar_memory_governor_ref_release(t);
                    }
                }
}
static wl_columnar_memory_governor_t *string_quota_governor;
static wl_columnar_memory_reservation_t string_quota;
static bool string_quota_reached;
static unsigned string_quota_stage;
static void
string_quota_hook(wl_evaluation_control_t *c, unsigned stage, uint64_t progress)
{
    (void)c; (void)progress;
    if (stage != string_quota_stage) return;
    wl_intern_test_controlled_checkpoint = NULL;
    uint64_t usable = atomic_load(&string_quota_governor->usable_bytes);
    uint64_t used = wl_columnar_memory_reserved(string_quota_governor);
    CHECK(usable > used);
    CHECK(wl_columnar_memory_reserve(string_quota_governor, usable - used,
        &string_quota));
    string_quota_reached = true;
}
static void
test_string_domain_quota_and_allocator_faults(void)
{
    for (int map = 0; map < 2; map++) for (int domain = 0; domain < 3; domain++)
            for (int fault = 0; fault < 6; fault++) {
                wl_col_session_t *sess = join_session();
                wl_columnar_memory_governor_ref_t *t = domain ==
                    2 ? fresh_governor() :
                    domain == 1 ? sess->memory_governor : NULL;
                sess->intern = string_table(t, fault == 2 ? 48 : 0);
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before_s = string_reserved(sess->memory_governor),
                    before_t = string_reserved(t);
                uint32_t count = wl_intern_count(sess->intern);
                uint8_t buf[32];
                uint32_t n = string_literal(buf, 0, "new-value", 9);
                wl_plan_expr_buffer_t expr = {buf, n};
                wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                     .filter_expr = expr, .project_count = 1,
                     .map_exprs = &expr, .map_expr_count = 1}
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                if (fault < 3) wl_intern_test_fail_allocation = fault ==
                        2 ? 1 : fault;
                if (fault == 3 && t) wl_intern_test_fail_commit = true;
                if (fault >= 4 && t) {
                    string_quota_governor =
                        wl_columnar_memory_governor_ref_get(t);
                    wl_columnar_memory_reservation_init(&string_quota);
                    string_quota_reached = false;
                    string_quota_stage = fault == 4 ? 1 : 6;
                    wl_intern_test_controlled_checkpoint = string_quota_hook;
                }
                bool failing = fault < 2 || fault == 2 || (fault >= 3 && t);
                int expected = failing ? ENOMEM : 0;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(sess->memory_budget_denied == (fault >= 4 && t));
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                if (fault >= 4 && t) {
                    CHECK(string_quota_reached);
                    CHECK(wl_columnar_memory_rollback(&string_quota));
                }
                CHECK(wl_intern_count(sess->intern) == count + !failing);
                CHECK(string_reserved(sess->memory_governor) == before_s +
                    (!failing && domain == 1 ? 522 : 0));
                CHECK(string_reserved(t) == before_t + (!failing &&
                    t ? 522 : 0));
                CHECK(wl_intern_test_fail_allocation == -1);
                CHECK(!wl_intern_test_fail_commit);
                frame = filter_frame(sess); retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                finish(c, 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                wl_intern_free(sess->intern); sess->intern = NULL;
                release(c); join_session_destroy(sess);
                if (domain == 2) {
                    governor_empty(t);
                    wl_columnar_memory_governor_ref_release(t);
                }
            }
}
static void
test_string_owned_cleanup(void)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[64];
        uint32_t n = string_literal(buf, 0, "owned-string", 12);
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(filter_reached == 1 && wl_intern_count(sess->intern) == 1);
        CHECK(strcmp(wl_intern_reverse(sess->intern, 0), "owned-string") == 0);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL;
        release(c); join_session_destroy(sess);
    }
}

static wl_columnar_memory_governor_t *string_overlap_governor;
static uint64_t string_overlap_before, string_overlap_copy,
    string_overlap_image;
static unsigned string_overlap_flags;
static void
string_overlap_hook(wl_evaluation_control_t *c, unsigned stage,
    uint64_t progress)
{
    (void)c; (void)progress;
    uint64_t current = wl_columnar_memory_reserved(string_overlap_governor);
    if (stage == 1 && !(string_overlap_flags & 1)) {
        string_overlap_before = current; string_overlap_flags |= 1;
    }
    if (stage == 6 || stage == 9) {
        CHECK(current == string_overlap_before + string_overlap_copy);
        string_overlap_flags |= 2;
    }
    if (stage == 13) {
        CHECK(current ==
            string_overlap_before + string_overlap_copy + string_overlap_image);
        string_overlap_flags |= 4;
    }
}
static void
test_string_full_image_domain_overlap(void)
{
    for (int domain = 1; domain < 3; domain++) for (int duplicate = 0;
            duplicate < 2;
            duplicate++) {
            wl_col_session_t *sess = join_session();
            wl_columnar_memory_governor_ref_t *t = domain ==
                1 ? sess->memory_governor : fresh_governor();
            sess->intern = string_table(t, 0);
            if (duplicate) CHECK(wl_intern_put(sess->intern,
                    "overlap-new") == 0);
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            uint64_t before_t = string_reserved(t),
                before_s = string_reserved(sess->memory_governor);
            uint8_t buf[32];
            uint32_t n = string_literal(buf, 0, "overlap-new", 11);
            wl_plan_expr_buffer_t expr = {buf, n};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
                 .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            string_overlap_governor = wl_columnar_memory_governor_ref_get(t);
            string_overlap_copy = 12; string_overlap_image = 256+512+12;
            string_overlap_flags = 0;
            wl_intern_test_controlled_checkpoint = string_overlap_hook;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            wl_intern_test_controlled_checkpoint = NULL;
            CHECK(string_overlap_flags == (duplicate ? 3u:7u));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(string_reserved(t) == before_t+(duplicate ? 0:524));
            CHECK(string_reserved(sess->memory_governor) == before_s+
                (domain == 1 && !duplicate ? 524:0));
            wl_intern_free(sess->intern); sess->intern = NULL; release(c);
            join_session_destroy(sess);
            if (domain == 2) {
                governor_empty(t); wl_columnar_memory_governor_ref_release(t);
            }
        }
    string_overlap_governor = NULL;
}
typedef struct {
    wl_intern_t *table;
    const char *new_text;
    const char *const *expected_texts;
    uint32_t expected_count;
    wl_atomic_u64 stop;
    wl_mutex_t lock;
    wl_cond_t condition;
    bool ready;
    bool observed_new;
    uint32_t max_seen;
} string_reader_t;
static void *
string_reverse_reader(void *arg)
{
    string_reader_t *reader = arg;
    wl_mutex_lock(&reader->lock); reader->ready = true;
    wl_cond_signal(&reader->condition); wl_mutex_unlock(&reader->lock);
    while (!atomic_load(&reader->stop)) {
        uint32_t count = wl_intern_count(reader->table);
        for (uint32_t i = 0; i < count; i++) {
            const char *text = wl_intern_reverse(reader->table, i);
            CHECK(text && (reader->expected_texts ?
                i < reader->expected_count && strcmp(text,
                reader->expected_texts[i]) == 0 :
                (i == 0 ? strcmp(text, "seed-0") == 0:
                strcmp(text,
                reader->new_text ? reader->new_text : "shared-new") == 0)));
        }
        if (count > reader->max_seen) {
            reader->max_seen = count;
            if (count ==
                (reader->expected_count ? reader->expected_count : 2)){
                wl_mutex_lock(&reader->lock); reader->observed_new = true;
                wl_cond_signal(&reader->condition);
                wl_mutex_unlock(&reader->lock);
            }
        }
    }
    return NULL;
}
static void
test_string_shared_reverse_parent_stop(void)
{
    wl_col_session_t *sess = join_session();
    wl_columnar_memory_governor_ref_t *t = fresh_governor();
    sess->intern = string_table(t, 1);
    string_reader_t reader = {.table = sess->intern};
    atomic_init(&reader.stop, 0);
    CHECK(wl_mutex_init(&reader.lock) == 0 &&
        wl_cond_init(&reader.condition) == 0);
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, string_reverse_reader, &reader) == 0);
    wl_mutex_lock(&reader.lock);
    while (!reader.ready) wl_cond_wait(&reader.condition, &reader.lock);
    wl_mutex_unlock(&reader.lock);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(t);
    uint8_t buf[32]; uint32_t n = string_literal(buf, 0, "shared-new", 10);
    wl_plan_expr_buffer_t expr = {buf, n};
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
         .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    string_stop_stage = 3; string_stop_progress = 1;
    string_stop_positive = false; string_stop_reached = 0;
    wl_columnar_expr_test_controlled_checkpoint = string_expr_stop;
    CHECK(compiled_dispatch(sess, frame, ops, 2,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED); CHECK(string_stop_reached == 1);
    lazy_unchanged(source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_intern_count(sess->intern) == 2 &&
        string_reserved(t) == before+11);
    CHECK(wl_intern_get(sess->intern, "shared-new") == 1);
    frame = filter_frame(sess); retry(c, 0);
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_intern_count(sess->intern) == 2 &&
        string_reserved(t) == before+11);
    wl_mutex_lock(&reader.lock);
    while (!reader.observed_new) wl_cond_wait(&reader.condition, &reader.lock);
    wl_mutex_unlock(&reader.lock);
    atomic_store(&reader.stop, 1); CHECK(wl_thread_join(&thread) == 0);
    CHECK(reader.max_seen == 2);
    wl_cond_destroy(&reader.condition); wl_mutex_destroy(&reader.lock);
    wl_intern_free(sess->intern); sess->intern = NULL;
    release(c); join_session_destroy(sess); governor_empty(t);
    wl_columnar_memory_governor_ref_release(t);
}

static uint32_t
string_fixture_hash(const char *text)
{
    uint32_t hash = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)text; *p; p++) {
        hash ^= *p; hash *= 16777619u;
    }
    return hash;
}
static unsigned string_collision_visits;
static void
string_collision_stop(wl_evaluation_control_t *c, unsigned stage,
    uint64_t progress)
{
    (void)progress;
    if (stage != 5 || ++string_collision_visits != 513) return;
    wl_intern_test_controlled_checkpoint = NULL;
    if (string_stop_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_string_actual_collision_probe(void)
{
    for (int positive = 0; positive < 2; positive++) {
        wl_col_session_t *sess = join_session();
        wl_columnar_memory_governor_ref_t *t = fresh_governor();
        sess->intern = string_table(t, 0);
        const char *query = "collision-query";
        uint32_t bucket = string_fixture_hash(query) & 1023;
        for (uint32_t serial = 0, count = 0; count < 600; serial++) {
            char text[64]; snprintf(text, sizeof(text), "collision-%u", serial);
            if ((string_fixture_hash(text) & 1023) == bucket)
                CHECK(wl_intern_put(sess->intern, text) == count++);
        }
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t before = string_reserved(t);
        uint8_t buf[32]; uint32_t n = string_literal(buf, 0, query, 15);
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
             .map_expr_count = 1}
        };
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        string_collision_visits = 0; string_stop_positive = positive;
        wl_intern_test_controlled_checkpoint = string_collision_stop;
        int expected = positive ? EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
        finish(c, expected);
        CHECK(string_collision_visits == 513 &&
            wl_intern_count(sess->intern) == 600);
        lazy_unchanged(source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(string_reserved(t) == before);
        frame = filter_frame(sess); retry(c, 0);
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_intern_get(sess->intern,
            query) == 600 && string_reserved(t) == before+16);
        wl_intern_free(sess->intern); sess->intern = NULL; release(c);
        join_session_destroy(sess);
        governor_empty(t); wl_columnar_memory_governor_ref_release(t);
    }
}

static void
string_s_quota_hook(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)c; (void)failed;
    if (stage != 7 || progress < 3) return;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    uint64_t usable = atomic_load(&string_quota_governor->usable_bytes);
    uint64_t used = wl_columnar_memory_reserved(string_quota_governor);
    CHECK(usable > used && wl_columnar_memory_reserve(string_quota_governor,
        usable-used, &string_quota)); string_quota_reached = true;
}
static void
test_string_caller_s_scratch_faults(void)
{
    for (int map = 0; map < 2; map++)for (int quota = 0; quota < 2; quota++) {
            wl_col_session_t *sess = join_session();
            wl_columnar_memory_governor_ref_t *t = fresh_governor();
            sess->intern = string_table(t, 0);
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint64_t before_s = string_reserved(sess->memory_governor),
                before_t = string_reserved(t);
            uint8_t buf[32];
            uint32_t n = string_literal(buf, 0, "S-temporary", 11);
            wl_plan_expr_buffer_t expr = {buf, n};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            if (quota) {
                string_quota_governor =
                    wl_columnar_memory_governor_ref_get(sess->memory_governor);
                wl_columnar_memory_reservation_init(&string_quota);
                string_quota_reached = false;
                wl_columnar_expr_test_controlled_checkpoint =
                    string_s_quota_hook;
            } else wl_columnar_expr_test_fail_allocation = 0;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == ENOMEM);
            finish(c, ENOMEM);
            CHECK(sess->memory_budget_denied == (quota != 0));
            lazy_unchanged(source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            if (quota) {
                CHECK(string_quota_reached);
                CHECK(wl_columnar_memory_rollback(&string_quota));
            }
            CHECK(string_reserved(sess->memory_governor) == before_s &&
                string_reserved(t) == before_t);
            CHECK(wl_intern_count(sess->intern) == 0);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            CHECK(wl_intern_count(sess->intern) == 1 &&
                string_reserved(t) == before_t+524);
            wl_intern_free(sess->intern); sess->intern = NULL; release(c);
            join_session_destroy(sess);
            governor_empty(t); wl_columnar_memory_governor_ref_release(t);
        }
}

static void
test_string_unmanaged_caller_governed_table(void)
{
    wl_columnar_memory_governor_ref_t *t = fresh_governor();
    wl_intern_t *table = string_table(t, 0);
    wl_col_session_t sess = {.intern = table};
    uint8_t buf[32]; uint32_t n = string_literal(buf, 0, "unmanaged-S", 11);
    wl_evaluation_control_t *c = control(0);
    int64_t value = INT64_MIN; bool failed = true;
    wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_MALFORMED;
    CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0, &value,
        &failed, &sess, &status, false, true, c) == 0);
    finish(c, 0); CHECK(!failed && status == WL_COLUMNAR_EXPR_OK && value == 0);
    CHECK(wl_intern_count(table) == 1 && string_reserved(t) == 256+512+12);
    retry(c, 0);
    CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0, &value,
        &failed, &sess, &status, false, true, c) == 0); finish(c, 0);
    CHECK(!failed && value == 0 && string_reserved(t) == 256+512+12);
    wl_intern_free(table); release(c); governor_empty(t);
    wl_columnar_memory_governor_ref_release(t);
}

static void
test_string_variable_failure_tail_and_empty_rows(void)
{
    for (int map = 0; map < 2; map++)for (int rows = 0; rows < 2; rows++)
            for (int action = 0; action < 3; action++) {
                wl_col_session_t *sess = join_session();
                sess->intern = string_table(sess->memory_governor, 1);
                col_rel_t *source = relation(1, 1, rows, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before = string_reserved(sess->memory_governor);
                uint8_t buf[32];
                uint32_t n = compiled_var(buf, 0, "col4294967296");
                buf[0] = WL_PLAN_EXPR_VAR_STRING;
                wl_plan_expr_buffer_t expr = {buf, n};
                wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                int expected = rows && map ? ERANGE:0;
                if (action) {
                    compiled_stop_stage = 3; compiled_stop_progress = 1;
                    compiled_stop_positive = action == 2;
                    compiled_require_failed = true;
                    compiled_reached = 0;
                    wl_columnar_expr_test_controlled_checkpoint =
                        compiled_stage_stop;
                    if (rows) expected = action ==
                            2 ? EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED;
                }
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                if (action) {
                    CHECK(compiled_reached == (unsigned)rows);
                    wl_columnar_expr_test_controlled_checkpoint = NULL;
                    if (rows) lazy_unchanged(source);
                }
                CHECK(!sess->memory_budget_denied);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(string_reserved(sess->memory_governor) == before &&
                    wl_intern_count(sess->intern) == 1);
                frame = filter_frame(sess); retry(c, 0);
                expected = rows && map ? ERANGE:0;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                wl_intern_free(sess->intern); sess->intern = NULL; release(c);
                join_session_destroy(sess);
            }
}

static void
test_string_zero_row_literal_no_put(void)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        wl_columnar_memory_governor_ref_t *t = fresh_governor();
        sess->intern = string_table(t, 0);
        wl_columnar_memory_governor_t *governor =
            wl_columnar_memory_governor_ref_get(t);
        wl_columnar_memory_reservation_t quota;
        wl_columnar_memory_reservation_init(&quota);
        CHECK(wl_columnar_memory_reserve(governor,
            atomic_load(&governor->usable_bytes)-string_reserved(t), &quota));
        col_rel_t *source = relation(1, 0, 0, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0);
        uint8_t buf[32];
        uint32_t n = string_literal(buf, 0, "not-evaluated", 13);
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER, .filter_expr = expr,
             .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
        };
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        wl_intern_test_fail_allocation = 0;
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
        CHECK(wl_intern_test_fail_allocation == 0 &&
            wl_intern_count(sess->intern) == 0);
        CHECK(!sess->memory_budget_denied);
        wl_intern_test_fail_allocation = -1;
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(wl_columnar_memory_rollback(&quota));
        CHECK(string_reserved(t) == 256);
        wl_intern_free(sess->intern); sess->intern = NULL; release(c);
        join_session_destroy(sess);
        governor_empty(t); wl_columnar_memory_governor_ref_release(t);
    }
}

static void
test_builtin_actual_null_and_value_twins(void)
{
    const uint8_t ops[] = {WL_PLAN_EXPR_STR_FN_STRLEN,
                           WL_PLAN_EXPR_STR_FN_CONTAINS,
                           WL_PLAN_EXPR_STR_FN_STR_PREFIX,
                           WL_PLAN_EXPR_STR_FN_STR_SUFFIX,
                           WL_PLAN_EXPR_STR_FN_STR_ORD,
                           WL_PLAN_EXPR_STR_FN_TO_UPPER,
                           WL_PLAN_EXPR_STR_FN_TO_LOWER,
                           WL_PLAN_EXPR_STR_FN_STR_REPLACE,
                           WL_PLAN_EXPR_STR_FN_TRIM,
                           WL_PLAN_EXPR_STR_FN_TO_STRING,
                           WL_PLAN_EXPR_STR_FN_TO_NUMBER};
    for (int domain = -1; domain < 3; domain++)for (uint32_t k = 0;
            k < sizeof(ops);
            k++) for (int missing = 0; missing < 2; missing++){
                uint8_t buf[64];
                uint32_t n = compiled_const(buf, 0, missing ? 99:3);
                if (ops[k] == WL_PLAN_EXPR_STR_FN_CONTAINS ||
                    ops[k] == WL_PLAN_EXPR_STR_FN_STR_PREFIX
                    || ops[k] == WL_PLAN_EXPR_STR_FN_STR_SUFFIX ||
                    ops[k] == WL_PLAN_EXPR_STR_FN_STR_REPLACE)
                    n = compiled_const(buf, n, missing ? 99:2);
                if (ops[k] ==
                    WL_PLAN_EXPR_STR_FN_STR_REPLACE) n = compiled_const(buf, n,
                            missing ? 99:1);
                buf[n++] = ops[k]; string_caller_twins(buf, n,
                    domain < 0 &&
                    ops[k] == WL_PLAN_EXPR_STR_FN_TO_NUMBER ? ERANGE:0, domain,
                    k == 0 && missing);
            }
    const char *utf8[] = {"", "\xff", "\xe2x", "\xf0\x9f", "\xc0\x80",
                          "\xed\xa0\x80",
                          "\xf4\x90\x80\x80", "e\xcc\x81", "Aa\xe9z"};
    const uint8_t unary[] = {WL_PLAN_EXPR_STR_FN_STRLEN,
                             WL_PLAN_EXPR_STR_FN_STR_ORD,
                             WL_PLAN_EXPR_STR_FN_TO_UPPER,
                             WL_PLAN_EXPR_STR_FN_TO_LOWER,
                             WL_PLAN_EXPR_STR_FN_TRIM};
    for (uint32_t k = 0; k < sizeof(unary); k++)for (uint32_t j = 0;
            j < sizeof(utf8)/sizeof(*utf8); j++){
            uint8_t buf[64];
            uint32_t n = string_literal(buf, 0, utf8[j],
                    (uint16_t)strlen(utf8[j]));
            buf[n++] = unary[k]; string_caller_twins(buf, n, 0, 2, false);
        }
    const char *patterns[] = {"", "a", "aa", "aaaaaa", "b"},
        *replacements[] = {"", "X", "XYZ"};
    for (uint32_t k = 0; k < 5; k++)for (uint32_t j = 0; j < 3; j++) {
            uint8_t buf[96]; uint32_t n = string_literal(buf, 0, "aaaaa", 5);
            n = string_literal(buf, n, patterns[k],
                    (uint16_t)strlen(patterns[k]));
            n = string_literal(buf, n, replacements[j],
                    (uint16_t)strlen(replacements[j]));
            buf[n++] = WL_PLAN_EXPR_STR_FN_STR_REPLACE;
            string_caller_twins(buf, n, 0, 1, k == 1 && j == 2);
        }
    uint8_t buf[64]; uint32_t n = compiled_const(buf, 0, 0);
    n = compiled_const(buf, n, 2);
    n = compiled_const(buf, n, 99); buf[n++] = WL_PLAN_EXPR_STR_FN_STR_REPLACE;
    string_caller_twins(buf, n, 0, 2, false);
    for (int extreme = 0; extreme < 2; extreme++) {
        n = compiled_const(buf, 0, extreme ? INT64_MAX:INT64_MIN);
        buf[n++] = WL_PLAN_EXPR_STR_FN_TO_STRING;
        for (int domain = -1; domain < 3;
            domain++) string_caller_twins(buf, n, 0, domain, true);
    }
}
static void
test_builtin_checked_number_native_twins(void)
{
    const char *text[] = {"9223372036854775807", "-9223372036854775808",
                          "9223372036854775808",
                          "-9223372036854775809", "", "x", "+", "-", "++1",
                          "--1", "+-1", "-+1", "-0tail", "+0tail",
                          " \t\n\v\f\r42x", "\x80" "42", "  -00012junk", "1 2"};
    for (uint32_t k = 0; k < sizeof(text)/sizeof(*text); k++) {
        wl_intern_t *table = wl_intern_create(); CHECK(table);
        int64_t id = wl_intern_put(table, text[k]), reference = 123;
        int rc = wl_string_ops_to_number_checked(id, table, &reference);
        CHECK(rc == 0 || rc == ERANGE); wl_intern_free(table);
        uint8_t buf[128];
        uint32_t n = string_literal(buf, 0, text[k], (uint16_t)strlen(text[k]));
        buf[n++] = WL_PLAN_EXPR_STR_FN_TO_NUMBER;
        string_caller_twins(buf, n, rc, 2, k < 2);
    }
    const char whitespace[] = {' ', '\t', '\n', '\v', '\f', '\r'};
    for (uint32_t i = 0; i < sizeof(whitespace); i++) {
        char text[4] = {whitespace[i], '4', '2', 0}; uint8_t buf[16];
        uint32_t n = string_literal(buf, 0, text, 3);
        buf[n++] = WL_PLAN_EXPR_STR_FN_TO_NUMBER;
        string_caller_twins(buf, n, 0, 1, false);
    }
    char digits[1502]; memset(digits, '9', 1500); digits[1500] = 'x';
    digits[1501] = 0;
    uint8_t buf[1520]; uint32_t n = string_literal(buf, 0, digits, 1501);
    buf[n++] = WL_PLAN_EXPR_STR_FN_TO_NUMBER;
    string_caller_twins(buf, n, ERANGE, 2, false);
}
static void
test_builtin_long_reached_stops(void)
{
    const int stages[] = {18, 16, 22, 23, 24, 19, 21, 3};
    for (int map = 0; map < 2;
        map++) for (uint32_t mode = 0; mode < 8; mode++) for (int positive = 0;
                positive < 2; positive++){
                wl_col_session_t *sess = join_session();
                wl_columnar_memory_governor_ref_t *t = fresh_governor();
                sess->intern = string_table(t, 0); char text[602], pattern[602];
                memset(text, 'a', 600); text[600] = 0;
                memcpy(pattern, text, 601); pattern[599] = 'b';
                if (mode == 3) {
                    memset(text, ' ', 600); text[600] = 'a'; text[601] = 0;
                }
                if (mode == 4) {
                    text[0] = 'a'; memset(text+1, ' ', 599);
                }
                if (mode == 5) {
                    memset(text, ' ', 599); text[599] = '1';
                }
                if (mode == 6) memset(text, '9', 600);
                CHECK(wl_intern_put(sess->intern, text) == 0);
                CHECK(wl_intern_put(sess->intern, pattern) == 1);
                uint8_t buf[128];
                uint32_t n = compiled_const(buf, 0, mode == 7 ? 99:0);
                if (mode == 0) {
                    n = compiled_const(buf, n, 1);
                    buf[n++] = WL_PLAN_EXPR_STR_FN_CONTAINS;
                }
                if (mode == 1) buf[n++] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
                if (mode == 2) buf[n++] = WL_PLAN_EXPR_STR_FN_STRLEN;
                if (mode == 3 || mode == 4) buf[n++] = WL_PLAN_EXPR_STR_FN_TRIM;
                if (mode == 5 ||
                    mode == 6) buf[n++] = WL_PLAN_EXPR_STR_FN_TO_NUMBER;
                if (mode == 7) buf[n++] = WL_PLAN_EXPR_STR_FN_STR_ORD;
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before_s = string_reserved(sess->memory_governor),
                    before_t = string_reserved(t);
                wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess); wl_evaluation_control_t *c = control(0);
                string_stop_stage = stages[mode];
                string_stop_progress = mode == 7 ? 2:300;
                string_stop_positive = positive; string_stop_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint = string_expr_stop;
                int expected =
                    positive ? EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(string_stop_reached == 1 && !sess->memory_budget_denied);
                lazy_unchanged(source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(string_reserved(sess->memory_governor) == before_s &&
                    string_reserved(t) == before_t);
                frame = filter_frame(sess); retry(c, 0);
                expected = mode == 6 && map ? ERANGE:0;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                wl_intern_free(sess->intern); sess->intern = NULL; release(c);
                join_session_destroy(sess);
                governor_empty(t); wl_columnar_memory_governor_ref_release(t);
            }
}
static void
test_builtin_pending_number_c2(void)
{
    for (int map = 0; map < 2;
        map++) for (int positive = 0; positive < 2; positive++){
            wl_col_session_t *sess = join_session();
            sess->intern = string_table(sess->memory_governor, 0);
            CHECK(wl_intern_put(sess->intern, "9223372036854775808tail") == 0);
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint64_t before = string_reserved(sess->memory_governor);
            uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, 0);
            buf[n++] = WL_PLAN_EXPR_STR_FN_TO_NUMBER;
            wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            compiled_stop_stage = 3; compiled_stop_progress = 2;
            compiled_stop_positive = positive;
            compiled_require_failed = true; compiled_reached = 0;
            wl_columnar_expr_test_controlled_checkpoint = compiled_stage_stop;
            int expected = positive ? EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            finish(c, expected);
            CHECK(compiled_reached == 1); lazy_unchanged(source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                string_reserved(sess->memory_governor) == before);
            wl_intern_free(sess->intern); sess->intern = NULL; release(c);
            join_session_destroy(sess);
        }
}

static void
test_builtin_long_replace_search_and_copy(void)
{
    for (int map = 0; map < 2;
        map++) for (int copy = 0; copy < 2; copy++) for (int positive =
                0;
                positive < 2; positive++){
                wl_col_session_t *sess = join_session();
                wl_columnar_memory_governor_ref_t *t = fresh_governor();
                sess->intern = string_table(t, 0);
                char text[602], pattern[602], replacement[602];
                memset(text, 'a', 600); text[600] = 0;
                memcpy(pattern, text, 601); pattern[599] = 'b';
                memset(replacement, 'Z', 600); replacement[600] = 0;
                CHECK(wl_intern_put(sess->intern, copy ? "a":text) == 0);
                CHECK(wl_intern_put(sess->intern,
                    copy ? "a":pattern) == (copy ? 0:1));
                int64_t replacement_id = wl_intern_put(sess->intern,
                        replacement); CHECK(replacement_id >= 0);
                uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, 0);
                n = compiled_const(buf, n, copy ? 0:1);
                n = compiled_const(buf, n, replacement_id);
                buf[n++] = WL_PLAN_EXPR_STR_FN_STR_REPLACE;
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before_s = string_reserved(sess->memory_governor),
                    before_t = string_reserved(t);
                wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess); wl_evaluation_control_t *c = control(0);
                string_stop_stage = copy ? 16:18;
                string_stop_progress = copy ? 257:300;
                string_stop_positive = positive; string_stop_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint = string_expr_stop;
                int expected =
                    positive ? EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(string_stop_reached == 1); lazy_unchanged(source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(string_reserved(sess->memory_governor) == before_s &&
                    string_reserved(t) == before_t);
                frame = filter_frame(sess); retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                finish(c, 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(string_reserved(t) == before_t); /* no match or existing replacement ID */
                wl_intern_free(sess->intern); sess->intern = NULL; release(c);
                join_session_destroy(sess);
                governor_empty(t); wl_columnar_memory_governor_ref_release(t);
            }
}
static void
test_builtin_actual_resource_and_publication(void)
{
    for (int map = 0; map < 2;
        map++) for (int domain = 0; domain < 3; domain++) for (int mode = 0;
                mode < 6; mode++){
                wl_col_session_t *sess = join_session();
                wl_columnar_memory_governor_ref_t *t = domain ==
                    2 ? fresh_governor():domain ==
                    1 ? sess->memory_governor:NULL;
                sess->intern = string_table(t, 0);
                CHECK(wl_intern_put(sess->intern, "input-lower") == 0);
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before_s = string_reserved(sess->memory_governor),
                    before_t = string_reserved(t);
                uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, 0);
                buf[n++] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
                wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess); wl_evaluation_control_t *c = control(0);
                bool failing = mode == 0 || mode == 1 ||
                    (mode >= 2 && mode < 4 && t);
                int expected = failing ? ENOMEM : 0;
                if (mode == 4) expected = WL_EVALUATION_CONTROL_CANCELLED;
                if (mode == 5) expected = EOVERFLOW;
                if (mode == 0) wl_columnar_expr_test_fail_allocation = 0;
                if (mode == 1) wl_intern_test_fail_allocation = 0;
                if (mode == 2 && t) wl_intern_test_fail_commit = true;
                if (mode == 3 && t) {
                    string_quota_governor =
                        wl_columnar_memory_governor_ref_get(t);
                    wl_columnar_memory_reservation_init(&string_quota);
                    string_quota_reached = false;
                    string_quota_stage = 6;
                    wl_intern_test_controlled_checkpoint = string_quota_hook;
                }
                if (mode >= 4) {
                    string_stop_stage = 3; string_stop_progress = 2;
                    string_stop_positive = mode == 5; string_stop_reached = 0;
                    wl_columnar_expr_test_controlled_checkpoint =
                        string_expr_stop;
                }
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(sess->memory_budget_denied == (mode == 3 && t));
                if (expected) lazy_unchanged(source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                if (mode == 3 && t) {
                    CHECK(string_quota_reached);
                    CHECK(wl_columnar_memory_rollback(&string_quota));
                }
                CHECK(wl_intern_count(sess->intern) == (failing ? 1u:2u));
                CHECK(string_reserved(sess->memory_governor) ==
                    before_s+(!failing && domain == 1 ? 12:0));
                CHECK(string_reserved(t) == before_t+(!failing && t ? 12:0));
                if (!failing) CHECK(strcmp(wl_intern_reverse(sess->intern, 1),
                        "INPUT-LOWER") == 0);
                frame = filter_frame(sess); retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                finish(c, 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                    wl_intern_count(sess->intern) == 2);
                wl_intern_free(sess->intern); sess->intern = NULL; release(c);
                join_session_destroy(sess);
                if (domain == 2) {
                    governor_empty(t);
                    wl_columnar_memory_governor_ref_release(t);
                }
            }
}

static void
test_builtin_owned_cleanup(void)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        CHECK(wl_intern_put(sess->intern, "input-lower") == 0);
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[64];
        uint32_t n = compiled_const(buf, 0, 0);
        buf[n++] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(filter_reached == 1 && wl_intern_count(sess->intern) == 2);
        CHECK(strcmp(wl_intern_reverse(sess->intern, 1), "INPUT-LOWER") == 0);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL;
        release(c); join_session_destroy(sess);
    }
}

static void
test_builtin_shared_reverse_parent_stop(void)
{
    wl_col_session_t *sess = join_session();
    wl_columnar_memory_governor_ref_t *t = fresh_governor();
    sess->intern = string_table(t, 1);
    string_reader_t reader = {.table = sess->intern, .new_text = "SEED-0"};
    atomic_init(&reader.stop, 0);
    CHECK(wl_mutex_init(&reader.lock) == 0 &&
        wl_cond_init(&reader.condition) == 0);
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, string_reverse_reader, &reader) == 0);
    wl_mutex_lock(&reader.lock);
    while (!reader.ready) wl_cond_wait(&reader.condition, &reader.lock);
    wl_mutex_unlock(&reader.lock);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(t);
    uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, 0);
    buf[n++] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
    wl_plan_expr_buffer_t expr = {buf, n};
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
         .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    string_stop_stage = 3; string_stop_progress = 2;
    string_stop_positive = false; string_stop_reached = 0;
    wl_columnar_expr_test_controlled_checkpoint = string_expr_stop;
    CHECK(compiled_dispatch(sess, frame, ops, 2,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED); CHECK(string_stop_reached == 1);
    lazy_unchanged(source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_intern_count(sess->intern) == 2 &&
        string_reserved(t) == before+7);
    CHECK(wl_intern_get(sess->intern, "SEED-0") == 1);
    frame = filter_frame(sess); retry(c, 0);
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_intern_count(sess->intern) == 2 &&
        string_reserved(t) == before+7);
    wl_mutex_lock(&reader.lock);
    while (!reader.observed_new) wl_cond_wait(&reader.condition, &reader.lock);
    wl_mutex_unlock(&reader.lock);
    atomic_store(&reader.stop, 1); CHECK(wl_thread_join(&thread) == 0);
    CHECK(reader.max_seen == 2);
    wl_cond_destroy(&reader.condition); wl_mutex_destroy(&reader.lock);
    wl_intern_free(sess->intern); sess->intern = NULL;
    release(c); join_session_destroy(sess); governor_empty(t);
    wl_columnar_memory_governor_ref_release(t);
}

extern void (*wl_columnar_expr_hash_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t, bool);
static void
hash_stage_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    string_expr_stop(c, stage, progress, failed);
    if (string_stop_reached) wl_columnar_expr_hash_test_controlled_checkpoint =
            NULL;
}
static void
test_crc_actual_twins(void)
{
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_CRC32_ETH,
                           WL_PLAN_EXPR_ARITH_CRC32_ETH_S,
                           WL_PLAN_EXPR_ARITH_CRC32_CAST,
                           WL_PLAN_EXPR_ARITH_CRC32_CAST_S};
    const int64_t values[] = {0, 2, 3, 4, -1, 99, INT64_MIN, INT64_MAX};
    for (uint32_t k = 0; k < sizeof(ops); k++)
        for (int domain = -1; domain < 3; domain++)
            for (uint32_t j = 0; j < sizeof(values)/sizeof(*values); j++) {
                uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, values[j]);
                buf[n++] = ops[k];
                string_caller_twins(buf, n, 0, domain, j == 0);
            }
    /* Typed serialized length retains the embedded NUL, while CRC consumes
     * only the actual intern prefix. Saturated prefix really ignores pushes;
     * unary CRC then pops, so its own push necessarily fits. */
    const char embedded[] = {'a', 'b', 0, 'c', 'd'};
    for (uint32_t k = 1; k < sizeof(ops); k += 2) {
        uint8_t buf[1100]; uint32_t n = string_literal(buf, 0, embedded, 5);
        buf[n++] = ops[k]; string_caller_twins(buf, n, 0, 2, true);
        n = 0;
        for (uint32_t j = 0; j < 513; j++) {
            buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 0;
        }
        buf[n-1] = 1; buf[n++] = ops[k];
        string_caller_twins(buf, n, 0, 2, true);
        buf[0] = ops[k]; string_caller_twins(buf, 1, 0, -1, true);
    }
}
/* Actual ordinary caller, preseeded borrowed symbol, no expression put. */
static uint64_t
crc_caller_trial_expr(uint8_t op, const char *text, int map, uint64_t limit,
    int expected, int stage, bool positive, int64_t golden, bool reject,
    const wl_plan_expr_buffer_t *generated)
{
    wl_col_session_t *sess = join_session(), *raw = join_session();
    sess->intern = string_table(sess->memory_governor, 0);
    raw->intern = string_table(raw->memory_governor, 0);
    CHECK(wl_intern_put(sess->intern, text) == 0);
    CHECK(wl_intern_put(raw->intern, text) == 0);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    col_rel_t *ref = relation(1, 1, 1, false, raw->memory_governor);
    source->columns[0][0] = ref->columns[0][0] = 0;
    CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw, ref) == 0);
    make_lazy(source);
    uint64_t before = string_reserved(sess->memory_governor);
    uint8_t buf[64]; uint32_t n = compiled_var(buf, 0, "col0");
    buf[n++] = op;
    if (reject) {
        n = compiled_const(buf, n, -1); buf[n++] = WL_PLAN_EXPR_CMP_EQ;
    }
    wl_plan_expr_buffer_t expr = generated ? *generated :
        (wl_plan_expr_buffer_t){buf, n};
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
         .filter_expr = expr, .project_count = 1,
         .map_exprs = &expr, .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_columnar_eval_stack_cleanup_frame_t *reference = filter_frame(raw);
    wl_evaluation_control_t *c = control(limit);
    if (stage) {
        string_stop_stage = (unsigned)stage;
        string_stop_progress = stage == 3 ? 2 : stage >= 27 ? 576 : 300;
        string_stop_positive = positive; string_stop_reached = 0;
        if (stage >= 26)
            wl_columnar_expr_hash_test_controlled_checkpoint = hash_stage_stop;
        else wl_columnar_expr_test_controlled_checkpoint = string_expr_stop;
    }
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
    uint64_t cost = finish(c, expected);
    CHECK(!sess->memory_budget_denied && wl_intern_count(sess->intern) == 1);
    if (stage) CHECK(string_stop_reached == 1);
    CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == 0);
    if (!expected) {
        filter_equal(join_output(frame), join_output(reference));
        if (golden >= 0 && map && !reject)
            CHECK(join_output(frame)->columns[0][0] == golden);
        if (reject || (!*text && !map
            && op !=
            WL_PLAN_EXPR_ARITH_HASH_S)) CHECK(join_output(frame)->nrows ==
                (map ? 1u : 0u));
    } else {
        lazy_unchanged(source);
        CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
    }
    source_unpinned(source, source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
    CHECK(string_reserved(sess->memory_governor) == before);
    /* Same attempt reset, same original intern mapping and raw CRC cache.
     * Source shape may have been published on success, so equality of work
     * is asserted across fresh equivalent trials below, not this warm retry. */
    frame = filter_frame(sess);
    retry(c, expected == WL_EVALUATION_CONTROL_WORK_LIMIT ? limit+1 : 0);
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
    uint64_t retry_cost = finish(c, 0);
    if (expected == WL_EVALUATION_CONTROL_WORK_LIMIT)
        CHECK(retry_cost == limit+1);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_intern_count(sess->intern) == 1 &&
        string_reserved(sess->memory_governor) == before);
    wl_intern_free(sess->intern); wl_intern_free(raw->intern);
    sess->intern = raw->intern = NULL; release(c);
    join_session_destroy(sess); join_session_destroy(raw);
    return cost;
}
static uint64_t
crc_caller_trial(uint8_t op, const char *text, int map, uint64_t limit,
    int expected, int stage, bool positive, int64_t golden, bool reject)
{
    return crc_caller_trial_expr(op, text, map, limit, expected, stage,
               positive, golden, reject, NULL);
}
static void
test_crc_declared_symbol_plan(void)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl S(s: symbol)\nS(\"123456789\").\n"
        ".decl C(e: int64, k: int64)\n"
        "C(crc32_ethernet(s), crc32_castagnoli(s)) :- S(s).\n", &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    unsigned found = 0;
    for (uint32_t st = 0; st < plan->stratum_count; st++)
        for (uint32_t r = 0; r < plan->strata[st].relation_count; r++) {
            const wl_plan_relation_t *rel = &plan->strata[st].relations[r];
            for (uint32_t i = 0; i < rel->op_count; i++) {
                const wl_plan_op_t *op = &rel->ops[i];
                if (op->op != WL_PLAN_OP_MAP) continue;
                for (uint32_t j = 0; j < op->map_expr_count; j++) {
                    const wl_plan_expr_buffer_t *expr = &op->map_exprs[j];
                    if (!expr->size) continue;
                    uint8_t tag = expr->data[expr->size-1];
                    if (tag != WL_PLAN_EXPR_ARITH_CRC32_ETH_S &&
                        tag != WL_PLAN_EXPR_ARITH_CRC32_CAST_S) continue;
                    found |= tag == WL_PLAN_EXPR_ARITH_CRC32_ETH_S ? 1 : 2;
                    for (int map = 0; map < 2; map++)
                        crc_caller_trial_expr(tag, "123456789", map, 0, 0, 0,
                            false, tag == WL_PLAN_EXPR_ARITH_CRC32_ETH_S ?
                            INT64_C(0xcbf43926) : INT64_C(0xe3069283), false,
                            expr);
                }
            }
        }
    CHECK(found == 3); wl_plan_free(plan); wirelog_program_free(program);
}
static void
test_crc_lengths_goldens_and_stops(void)
{
    const uint32_t lengths[] = {0, 1, 63, 64, 127, 128, 255, 256, 513, 4097};
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_CRC32_ETH_S,
                           WL_PLAN_EXPR_ARITH_CRC32_CAST_S};
    char text[4098];
    for (uint32_t k = 0; k < sizeof(ops);
        k++) for (int map = 0; map < 2; map++) {
            CHECK(crc_caller_trial(ops[k], "123456789", map, 0, 0, 0, false,
                k ? INT64_C(0xe3069283) : INT64_C(0xcbf43926), false) > 0);
            for (uint32_t j = 0; j < sizeof(lengths)/sizeof(*lengths); j++) {
                memset(text, 'a', lengths[j]); text[lengths[j]] = 0;
                uint64_t cost = crc_caller_trial(ops[k], text, map, 0, 0, 0,
                        false, -1, false);
                CHECK(cost > 1);
                crc_caller_trial(ops[k], text, map, cost-1,
                    WL_EVALUATION_CONTROL_WORK_LIMIT, 0, false, -1, false);
                CHECK(crc_caller_trial(ops[k], text, map, cost, 0, 0, false,
                    -1, false) == cost);
                /* Actual scan2 per visited byte+NUL, CRC17 per byte. All other
                 * bytecode and caller shape are identical for nonempty strings. */
                if (j == 9) {
                    text[513] = 0;
                    uint64_t short_cost = crc_caller_trial(ops[k], text, map, 0,
                            0, 0, false, -1, false);
                    CHECK(cost-short_cost == (4097u-513u)*19u);
                }
            }
            memset(text, 'a', 4097); text[4097] = 0;
            for (int stage = 14; stage <= 25; stage += 11)
                for (int positive = 0; positive < 2; positive++)
                    crc_caller_trial(ops[k], text, map, 0,
                        positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED,
                        stage, positive, -1, true);
            crc_caller_trial(ops[k], text, map, 0, 0, 0, false, -1, true);
        }
}

static void
test_crc_owned_cleanup(void)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        CHECK(wl_intern_put(sess->intern, "input-lower") == 0);
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[64];
        uint32_t n = compiled_const(buf, 0, 0);
        buf[n++] = WL_PLAN_EXPR_ARITH_CRC32_ETH_S;
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(filter_reached == 1 && wl_intern_count(sess->intern) == 1);
        CHECK(strcmp(wl_intern_reverse(sess->intern, 0), "input-lower") == 0);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL;
        release(c); join_session_destroy(sess);
    }
}

static void
test_hash_actual_twins_and_lengths(void)
{
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_HASH, WL_PLAN_EXPR_ARITH_HASH_S};
    const int64_t values[] = {0, 2, 3, 4, -1, 99, INT64_MIN, INT64_MAX,
                              INT64_C(0x1234567812345678)};
    for (uint32_t k = 0; k < sizeof(ops); k++)
        for (int domain = -1; domain < 3; domain++)
            for (uint32_t j = 0; j < sizeof(values)/sizeof(*values); j++) {
                uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, values[j]);
                buf[n++] = ops[k]; string_caller_twins(buf, n, 0, domain,
                    j == 0);
            }
    const char embedded[] = {'a', 'b', 0, 'c', 'd'};
    uint8_t buf[1100]; uint32_t n = string_literal(buf, 0, embedded, 5);
    buf[n++] = WL_PLAN_EXPR_ARITH_HASH_S;
    string_caller_twins(buf, n, 0, 2, true);
    n = 0;
    for (uint32_t i = 0; i < 513; i++) {
        buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 0;
    }
    buf[n-1] = 1; buf[n++] = WL_PLAN_EXPR_ARITH_HASH_S;
    string_caller_twins(buf, n, 0, 2, true);
    buf[0] = WL_PLAN_EXPR_ARITH_HASH; string_caller_twins(buf, 1, 0, -1, true);
    const uint32_t lengths[] = {0, 1, 3, 4, 8, 9, 16, 17, 31, 32, 63, 64, 65,
                                127, 128, 129,
                                239, 240, 241, 255, 256, 257, 319, 320, 321,
                                511, 512, 513, 1023, 1024, 1025, 4097};
    char text[4098];
    for (int map = 0; map < 2; map++) {
        for (uint32_t j = 0; j < sizeof(lengths)/sizeof(*lengths); j++) {
            uint32_t length = lengths[j]; memset(text, 'a', length);
            text[length] = 0;
            uint64_t cost = crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S, text,
                    map, 0, 0, 0, false, -1, false);
            crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S, text, map, cost-1,
                WL_EVALUATION_CONTROL_WORK_LIMIT, 0, false, -1, false);
            CHECK(crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S, text, map,
                cost, 0, 0, false, -1, false) == cost);
            if (length) {
                text[length-1] = 'b';
                CHECK(crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S, text,
                    map, 0, 0, 0, false, -1, false) == cost);
            }
            if (length == 4097) {
                text[4096] = 0;
                uint64_t shorter = crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S,
                        text, map, 0, 0, 0, false, -1, false);
                /* Extra byte has scan2 + input1 + new chunk fixed140
                 * (selection8, primitive128, offset4). */
                CHECK(cost-shorter == 143);
            }
        }
        memset(text, 'a', 4097); text[4097] = 0;
        const int stages[] = {14, 27, 28, 29, 3};
        for (uint32_t i = 0; i < sizeof(stages)/sizeof(*stages); i++)
            for (int positive = 0; positive < 2; positive++)
                crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S, text, map, 0,
                    positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED,
                    stages[i], positive, -1, true);
        crc_caller_trial(WL_PLAN_EXPR_ARITH_HASH_S, text, map, 0, 0, 0,
            false, -1, true);
    }
}
static void
test_hash_symbol_assignment_independence(void)
{
    for (int map = 0; map < 2; map++) for (int extra = 0; extra < 2; extra++) {
            wl_col_session_t *sess = join_session();
            sess->intern = string_table(sess->memory_governor, 0);
            if (extra) CHECK(wl_intern_put(sess->intern, "unrelated") == 0);
            CHECK(wl_intern_put(sess->intern, "aa") == extra);
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, extra);
            buf[n++] = WL_PLAN_EXPR_ARITH_HASH_S;
            if (!map) {
                n = compiled_const(buf, n, INT64_C(3932278706564862771));
                buf[n++] = WL_PLAN_EXPR_CMP_EQ;
            }
            wl_plan_expr_buffer_t expr = {buf, n};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr, .project_count = 1, .map_exprs = &expr,
                 .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            CHECK(join_output(frame)->nrows == 1);
            if (map) CHECK(join_output(frame)->columns[0][0] ==
                    INT64_C(3932278706564862771));
            CHECK(wl_intern_count(sess->intern) == (uint32_t)extra+1);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            wl_intern_free(sess->intern); sess->intern = NULL;
            release(c); join_session_destroy(sess);
        }
}
static uint32_t hash_alignment_seen;
static void
hash_alignment(wl_evaluation_control_t *c, int stage, uint64_t address,
    bool failed)
{
    (void)c;
    if (stage == 26) {
        CHECK(!failed && address && address % 64 == 0);
        hash_alignment_seen++;
    }
}
static void
test_hash_real_alignment_and_unaligned_input(void)
{
    uint8_t input[1027];
    for (uint32_t i = 0; i < sizeof(input); i++) input[i] = (uint8_t)(i*31+7);
    CHECK((uintptr_t)(input+1) % 64 != 0 ||
        (uintptr_t)(input+2) % 64 != 0);
    hash_alignment_seen = 0;
    wl_columnar_expr_hash_test_controlled_checkpoint = hash_alignment;
    for (uint32_t offset = 1; offset <= 2; offset++) {
        wl_evaluation_control_t *c = control(0); int64_t value = 123;
        bool failed = true;
        CHECK(wl_columnar_expr_hash_run(input+offset, 1025, &value, &failed,
            c) == 0 && !failed);
        CHECK(value == (int64_t)XXH3_64bits(input+offset, 1025));
        uint64_t cost = finish(c, 0);
        CHECK(cost == 8+128+128+1025+17*(8+128+4));
        retry(c, cost); value = 123; failed = true;
        CHECK(wl_columnar_expr_hash_run(input+offset, 1025, &value, &failed,
            c) == 0 && !failed);
        CHECK(finish(c, 0) == cost); release(c);
    }
    CHECK(hash_alignment_seen == 4);
    wl_columnar_expr_hash_test_controlled_checkpoint = NULL;
    wl_col_session_t *sess = join_session();
    col_rel_t *source = relation(1, 0, 0, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0);
    uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, 99);
    buf[n++] = WL_PLAN_EXPR_ARITH_HASH_S;
    wl_plan_expr_buffer_t expr = {buf, n};
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
         .map_expr_count = 1}
    };
    hash_alignment_seen = 0;
    wl_columnar_expr_hash_test_controlled_checkpoint = hash_alignment;
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
    CHECK(!hash_alignment_seen && !join_output(frame)->nrows);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    wl_columnar_expr_hash_test_controlled_checkpoint = NULL;
    release(c); join_session_destroy(sess);
}

static void
test_hash_owned_cleanup(void)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        CHECK(wl_intern_put(sess->intern, "input-lower") == 0);
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[64];
        uint32_t n = compiled_const(buf, 0, 0);
        buf[n++] = WL_PLAN_EXPR_ARITH_HASH_S;
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(filter_reached == 1 && wl_intern_count(sess->intern) == 1);
        CHECK(strcmp(wl_intern_reverse(sess->intern, 0), "input-lower") == 0);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL;
        release(c); join_session_destroy(sess);
    }
}

extern void (*wl_columnar_expr_digest_test_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t, bool);
extern int64_t (*wl_columnar_expr_digest_test_provider_result)(
    int, int64_t, size_t *);
static bool
digest_available(uint8_t op)
{
    uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, 0); buf[n++] = op;
    int64_t value; wl_columnar_expr_status_t status;
    return wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &value, NULL,
               &status) == 0 && status == WL_COLUMNAR_EXPR_OK;
}
static void
test_digest_actual_twins(void)
{
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_MD5, WL_PLAN_EXPR_ARITH_MD5_S,
                           WL_PLAN_EXPR_ARITH_SHA1, WL_PLAN_EXPR_ARITH_SHA1_S,
                           WL_PLAN_EXPR_ARITH_SHA256,
                           WL_PLAN_EXPR_ARITH_SHA256_S,
                           WL_PLAN_EXPR_ARITH_SHA512,
                           WL_PLAN_EXPR_ARITH_SHA512_S};
    const int64_t values[] = {0, 2, 3, 4, -1, 99, INT64_MIN, INT64_MAX};
    for (uint32_t k = 0; k < sizeof(ops); k++) {
        int expected = digest_available(ops[k]) ? 0 : ERANGE;
        for (int domain = -1; domain < 3; domain++)
            for (uint32_t j = 0; j < sizeof(values)/sizeof(*values); j++) {
                uint8_t buf[16]; uint32_t n = compiled_const(buf, 0, values[j]);
                buf[n++] = ops[k]; string_caller_twins(buf, n, expected,
                    domain, j == 0);
            }
        uint8_t buf[1100]; uint32_t n = 0;
        for (uint32_t i = 0; i < 513; i++) {
            buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 0;
        }
        buf[n-1] = 1; buf[n++] = ops[k];
        string_caller_twins(buf, n, expected, 2, true);
        buf[0] = ops[k]; string_caller_twins(buf, 1, expected, -1, true);
        if (k % 2) {
            const char embedded[] = {'a', 'b', 0, 'c', 'd'};
            n = string_literal(buf, 0, embedded, 5); buf[n++] = ops[k];
            string_caller_twins(buf, n, expected, 2, true);
            const uint32_t lengths[] = {0, 1, 63, 64, 65, 127, 128, 129, 255,
                                        256,
                                        513, 4097};
            char text[4098]; uint8_t large[4120];
            for (uint32_t j = 0; j < sizeof(lengths)/sizeof(*lengths); j++) {
                memset(text, 'a', lengths[j]); text[lengths[j]] = 0;
                n = string_literal(large, 0, text, (uint16_t)lengths[j]);
                large[n++] = ops[k];
                string_caller_twins(large, n, expected, 2, j == 11);
            }
        }
    }
}
static int digest_stop_stage, digest_primary;
static uint64_t digest_stop_progress;
static uint32_t digest_reached, digest_calls[5];
static int digest_override_stage = -1;
static bool digest_abort_error, digest_length_error;
static void
digest_checkpoint_stop(wl_evaluation_control_t *c, int stage,
    uint64_t progress, bool failed)
{
    (void)failed;
    if (stage != digest_stop_stage || progress < digest_stop_progress) return;
    wl_columnar_expr_digest_test_checkpoint = NULL;
    wl_columnar_expr_hash_test_controlled_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    digest_reached++;
    if (digest_primary > 0) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else if (digest_primary < 0) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(c);
}
static int64_t
digest_provider_report(int stage, int64_t actual, size_t *length)
{
    CHECK(stage >= 0 && stage < 5); digest_calls[stage]++;
    if (stage == 3 && digest_length_error && actual == 0) {
        CHECK(length && *length); (*length)--;
    }
    /* Real provider has already run; abort override is only after genuine
     * terminal call. This is report injection, not hardware reclaim proof. */
    if (stage == digest_override_stage || (stage == 4 && digest_abort_error))
#ifdef WL_MBEDTLS_ENABLED
        return stage == 4 ? PSA_ERROR_GENERIC_ERROR :
               PSA_ERROR_INSUFFICIENT_MEMORY;
#else
        return actual; /* no real PSA call can reach this disabled fixture */
#endif
    return actual;
}
static bool uuid_pair_available(void);
static void
digest_lifecycle_trial_kind(int map, int stop_stage, int primary, int override,
    bool abort_error, bool length_error, int pair)
{
    wl_col_session_t *sess = join_session();
    sess->intern = string_table(sess->memory_governor, 0);
    char text[601]; memset(text, 'a', 600); text[600] = 0;
    if (pair >= 3) {
        CHECK(wl_intern_put(sess->intern, pair == 4 ? text :
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8") == 0);
        CHECK(wl_intern_put(sess->intern, text) == (pair == 4 ? 0 : 1));
    } else CHECK(wl_intern_put(sess->intern, text) == 0);
    uint32_t initial_count = wl_intern_count(sess->intern);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(sess->memory_governor);
    uint8_t buf[32]; uint32_t n = compiled_const(buf, 0,
            pair == 2 ? INT64_MAX : 0);
    if (pair) n = compiled_const(buf, n, pair >= 3 && pair != 4 ? 1 : 0);
    buf[n++] = pair >= 3 ? WL_PLAN_EXPR_STR_FN_UUID5_RFC : pair ==
        2 ? WL_PLAN_EXPR_ARITH_UUID5_IS :
        pair ? WL_PLAN_EXPR_ARITH_UUID5_SS : WL_PLAN_EXPR_ARITH_SHA256_S;
    wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
         .filter_expr = expr, .project_count = 1, .map_exprs = &expr,
         .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(primary < 0 ? UINT64_MAX-1 : 0);
    memset(digest_calls, 0, sizeof(digest_calls)); digest_reached = 0;
    digest_stop_stage = stop_stage;
    digest_stop_progress = stop_stage == 14 ? 300 : stop_stage == 32 ? 320 :
        stop_stage == 3 ? (pair ? 3 : 2) :
        (stop_stage == 41 || stop_stage == 43) ? 320 :
        stop_stage == 49 ? 18 : stop_stage == 47 ? 8 : 0;
    digest_primary = primary; digest_override_stage = override;
    digest_abort_error = abort_error; digest_length_error = length_error;
    wl_columnar_expr_digest_test_provider_result = digest_provider_report;
    if (stop_stage) {
        if (stop_stage == 14 || stop_stage == 3 || stop_stage == 49 ||
            stop_stage == 50)
            wl_columnar_expr_test_controlled_checkpoint =
                digest_checkpoint_stop;
        else if (stop_stage == 28)
            wl_columnar_expr_hash_test_controlled_checkpoint =
                digest_checkpoint_stop;
        else wl_columnar_expr_digest_test_checkpoint = digest_checkpoint_stop;
    }
    int expected = 0;
    if (stop_stage) expected = primary > 0 ? EOVERFLOW : primary < 0 ?
            WL_EVALUATION_CONTROL_WORK_LIMIT : WL_EVALUATION_CONTROL_CANCELLED;
#ifdef WL_MBEDTLS_ENABLED
    if (override >= 0 || length_error) expected = map ? ERANGE : 0;
    bool abort_needed = stop_stage == 31 || stop_stage == 32 ||
        override == 1 || override == 2 || stop_stage == 36 ||
        stop_stage == 40 || stop_stage == 41 || stop_stage == 42 ||
        stop_stage == 43;
    if (abort_needed && abort_error && !(stop_stage && primary > 0))
        expected = EIO;
#else
    if (!stop_stage) expected = map ? ERANGE : 0;
#endif
    int actual_rc = compiled_dispatch(sess, frame, ops, 2, c);
    if (actual_rc != expected) fprintf(stderr,
            "digest lifecycle map%d stop%d primary%d override%d abort%d length%d: "
            "actual%d expected%d\n", map, stop_stage, primary, override,
            abort_error, length_error, actual_rc, expected);
    CHECK(actual_rc == expected);
    finish(c, expected); CHECK(!sess->memory_budget_denied);
    if (stop_stage) CHECK(digest_reached == 1);
#ifdef WL_MBEDTLS_ENABLED
    if (stop_stage == 14 || stop_stage == 37 || stop_stage == 49)
        CHECK(!digest_calls[0] && !digest_calls[1]);
    else if (override == 0) {
        CHECK(digest_calls[0] == 1 && !digest_calls[1] && !digest_calls[4]);
    } else {
        CHECK(digest_calls[0] == 1 && digest_calls[1] == 1);
        CHECK(digest_calls[4] == (abort_needed ? 1u : 0u));
        if (!abort_needed) CHECK(digest_calls[3] == 1);
    }
#else
    for (uint32_t i = 0; i < 5; i++) CHECK(!digest_calls[i]);
#endif
    if (expected) lazy_unchanged(source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    bool retained = false;
#ifdef WL_MBEDTLS_ENABLED
    retained = pair == 3 && stop_stage == 3;
#endif
    CHECK(string_reserved(sess->memory_governor) == before + (retained ? 37 : 0)
        && wl_intern_count(sess->intern) == initial_count + retained);
    if (retained) CHECK(wl_intern_reverse(sess->intern, initial_count));
    wl_columnar_expr_digest_test_provider_result = NULL;
    wl_columnar_expr_digest_test_checkpoint = NULL;
    wl_columnar_expr_hash_test_controlled_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    digest_override_stage = -1;
    digest_abort_error = digest_length_error = false;
    frame = filter_frame(sess); retry(c, 0);
    expected =
        pair == 4 ? (map ? ERANGE : 0) :
        (pair ? uuid_pair_available() :
        digest_available(WL_PLAN_EXPR_ARITH_SHA256)) ? 0 : map ? ERANGE : 0;
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
    finish(c, expected);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    wl_intern_free(sess->intern); sess->intern = NULL; release(c);
    join_session_destroy(sess);
}
static void
digest_lifecycle_trial(int map, int stop_stage, int primary, int override,
    bool abort_error, bool length_error)
{
    digest_lifecycle_trial_kind(map, stop_stage, primary, override, abort_error,
        length_error, false);
}
static void
test_digest_lifecycle_and_stops(void)
{
    for (int map = 0; map < 2; map++) {
#ifdef WL_MBEDTLS_ENABLED
        if (!digest_available(WL_PLAN_EXPR_ARITH_SHA256)) continue;
        const int stages[] = {14, 31, 32, 33, 28, 34, 3};
        for (uint32_t i = 0; i < sizeof(stages)/sizeof(*stages); i++)
            for (int positive = 0; positive < 2; positive++)
                digest_lifecycle_trial(map, stages[i], positive, -1, false,
                    false);
        for (int stage = 1; stage <= 3; stage++)
            digest_lifecycle_trial(map, 0, 0, stage, false, false);
        digest_lifecycle_trial(map, 0, 0, -1, false, true);
        /* Genuine abort runs FIRST then reports error: EIO wins pending0 and
         * negative control, while first positive EOVERFLOW is preserved. */
        digest_lifecycle_trial(map, 0, 0, 2, true, false);
        digest_lifecycle_trial(map, 31, 0, -1, true, false);
        digest_lifecycle_trial(map, 31, -1, -1, true, false);
        digest_lifecycle_trial(map, 32, 1, -1, true, false);
#else
        for (int positive = 0; positive < 2; positive++)
            digest_lifecycle_trial(map, 3, positive, -1, false, false);
        digest_lifecycle_trial(map, 0, 0, -1, false, false);
#endif
    }
}

static void
test_digest_zero_calls_and_id_independence_kind(bool pair)
{
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_MD5, WL_PLAN_EXPR_ARITH_MD5_S,
                           WL_PLAN_EXPR_ARITH_SHA1, WL_PLAN_EXPR_ARITH_SHA1_S,
                           WL_PLAN_EXPR_ARITH_SHA256,
                           WL_PLAN_EXPR_ARITH_SHA256_S,
                           WL_PLAN_EXPR_ARITH_SHA512,
                           WL_PLAN_EXPR_ARITH_SHA512_S};
    for (int map = 0; map < 2;
        map++) for (uint32_t k = 0; k < (pair ? 4 : sizeof(ops)); k++) {
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(1, 0, 0, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, 99);
            if (pair) n = compiled_const(buf, n, 99);
            const uint8_t pair_ops[] = {WL_PLAN_EXPR_ARITH_UUID5,
                                        WL_PLAN_EXPR_ARITH_UUID5_SS,
                                        WL_PLAN_EXPR_ARITH_UUID5_SI,
                                        WL_PLAN_EXPR_ARITH_UUID5_IS};
            buf[n++] = pair ? pair_ops[k] : ops[k];
            wl_plan_expr_buffer_t expr = {buf, n};
            wl_plan_op_t plan_ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr, .project_count = 1, .map_exprs = &expr,
                 .map_expr_count = 1}
            };
            memset(digest_calls, 0, sizeof(digest_calls));
            wl_columnar_expr_digest_test_provider_result =
                digest_provider_report;
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            CHECK(compiled_dispatch(sess, frame, plan_ops, 2, c) == 0);
            finish(c, 0);
            CHECK(!join_output(frame)->nrows);
            for (uint32_t i = 0; i < 5; i++) CHECK(!digest_calls[i]);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            wl_columnar_expr_digest_test_provider_result = NULL;
            release(c); join_session_destroy(sess);
        }
    if (!(pair ? uuid_pair_available() :
        digest_available(WL_PLAN_EXPR_ARITH_SHA256))) return;
    int64_t reference = 0;
    for (int map = 0; map < 2; map++) for (int extra = 0; extra < 2; extra++) {
            wl_col_session_t *sess = join_session();
            sess->intern = string_table(sess->memory_governor, 0);
            if (extra) CHECK(wl_intern_put(sess->intern, "unrelated") == 0);
            CHECK(wl_intern_put(sess->intern, "digest-body") == extra);
            uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, extra);
            if (pair) n = compiled_const(buf, n, extra);
            buf[n++] = pair ? WL_PLAN_EXPR_ARITH_UUID5_SS :
                WL_PLAN_EXPR_ARITH_SHA256_S;
            wl_columnar_expr_context_t ctx = {.intern = sess->intern};
            wl_columnar_expr_status_t status; int64_t actual_raw;
            CHECK(wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &actual_raw,
                &ctx, &status) == 0 && status == WL_COLUMNAR_EXPR_OK);
            if (!map && !extra) reference = actual_raw;
            CHECK(reference == actual_raw);
            if (!map) {
                n = compiled_const(buf, n, reference);
                buf[n++] = WL_PLAN_EXPR_CMP_EQ;
            }
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t plan_ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr, .project_count = 1, .map_exprs = &expr,
                 .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            CHECK(compiled_dispatch(sess, frame, plan_ops, 2, c) == 0);
            finish(c, 0);
            CHECK(join_output(frame)->nrows == 1);
            if (map) CHECK(join_output(frame)->columns[0][0] == reference);
            CHECK(wl_intern_count(sess->intern) == (uint32_t)extra+1);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            wl_intern_free(sess->intern); sess->intern = NULL;
            release(c); join_session_destroy(sess);
        }
}

static void
test_digest_zero_calls_and_id_independence(void)
{
    test_digest_zero_calls_and_id_independence_kind(false);
}
static void
test_digest_owned_cleanup_kind(int pair)
{
    if (!(pair ? uuid_pair_available() :
        digest_available(WL_PLAN_EXPR_ARITH_SHA256))) return;
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        if (pair == 2) CHECK(wl_intern_put(sess->intern,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8") == 0);
        CHECK(wl_intern_put(sess->intern,
            "input-lower") == (pair == 2 ? 1 : 0));
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[64];
        uint32_t n = compiled_const(buf, 0, 0);
        if (pair) n = compiled_const(buf, n, pair == 2 ? 1 : 0);
        buf[n++] = pair == 2 ? WL_PLAN_EXPR_STR_FN_UUID5_RFC :
            pair ? WL_PLAN_EXPR_ARITH_UUID5_SS :
            WL_PLAN_EXPR_ARITH_SHA256_S;
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        filter_reached = 0;
        if (pair == 2) {
            memset(digest_calls, 0, sizeof(digest_calls));
            wl_columnar_expr_digest_test_provider_result =
                digest_provider_report;
        }
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        if (pair == 2) {
            CHECK(digest_calls[1] >= 1 && digest_calls[3] >= 1 &&
                !digest_calls[4]);
            wl_columnar_expr_digest_test_provider_result = NULL;
        }
        CHECK(filter_reached == 1 &&
            wl_intern_count(sess->intern) == (pair == 2 ? 3u : 1u));
        if (pair == 2) CHECK(strcmp(wl_intern_reverse(sess->intern, 2),
                "b02acf5f-2a8c-5c50-8471-177fae928cab") == 0);
        CHECK(strcmp(wl_intern_reverse(sess->intern, pair == 2 ? 1 : 0),
            "input-lower") == 0);
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL;
        release(c); join_session_destroy(sess);
    }
}

static void
test_digest_owned_cleanup(void)
{
    test_digest_owned_cleanup_kind(false);
}
static bool
uuid_pair_available(void)
{
    uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, 0);
    n = compiled_const(buf, n, 0); buf[n++] = WL_PLAN_EXPR_ARITH_UUID5;
    int64_t value; wl_columnar_expr_status_t status;
    return wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &value, NULL,
               &status) == 0 && status == WL_COLUMNAR_EXPR_OK;
}
static void
test_uuid_pair_twins(void)
{
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_UUID5,
                           WL_PLAN_EXPR_ARITH_UUID5_SS,
                           WL_PLAN_EXPR_ARITH_UUID5_SI,
                           WL_PLAN_EXPR_ARITH_UUID5_IS};
    const int64_t values[] = {0, 2, 3, 4, -1, 99, INT64_MIN, INT64_MAX};
    int expected = uuid_pair_available() ? 0 : ERANGE;
    for (unsigned k = 0; k < sizeof(ops); k++) {
        for (int domain = -1; domain < 3; domain++)
            for (unsigned j = 0; j < sizeof(values)/sizeof(*values); j++) {
                uint8_t buf[32]; uint32_t n = compiled_const(buf, 0,
                        values[j]);
                n = compiled_const(buf, n, values[7-j]); buf[n++] = ops[k];
                string_caller_twins(buf, n, expected, domain, j == 0);
            }
        uint8_t full[1100]; uint32_t n = 0;
        for (unsigned i = 0; i < 513; i++) {
            full[n++] = WL_PLAN_EXPR_BOOL; full[n++] = 1;
        }
        full[n++] = ops[k];
        string_caller_twins(full, n, expected, -1, true);
        full[0] = ops[k]; string_caller_twins(full, 1, expected, -1, true);
        if (!k) continue;
        const uint32_t lengths[] = {0, 1, 63, 64, 65, 127, 128, 129, 513, 4097};
        char text[4098]; uint8_t buf[8300];
        for (unsigned j = 0; j < sizeof(lengths)/sizeof(*lengths); j++) {
            memset(text, 'a', lengths[j]); text[lengths[j]] = 0;
            n = k != 3 ? string_literal(buf, 0, text,
                    (uint16_t)lengths[j]) : compiled_const(buf, 0, INT64_MIN);
            n = k != 2 ? string_literal(buf, n, text,
                    (uint16_t)lengths[j]) : compiled_const(buf, n, INT64_MAX);
            buf[n++] = ops[k];
            string_caller_twins(buf, n, expected, 2, j == 9);
            if (j == 9 && !expected) {
                n = compiled_const(buf, n, -1); buf[n++] = WL_PLAN_EXPR_CMP_EQ;
                string_caller_twins(buf, n, 0, 2, true);
            }
        }
        const char embedded[] = {'a', '\xff', 0, 'b'};
        n = string_literal(buf, 0, embedded, 4);
        n = string_literal(buf, n, embedded, 4); buf[n++] = ops[k];
        string_caller_twins(buf, n, expected, 2, true);
    }
}
/* Expected SHA1 bytes below are independently computed from explicitly framed
 * input, not from the production pair helper. Native prefix is read with memcpy. */
static void
uuid_known_vector(const char *ns, const char *name, const uint8_t expected[20])
{
    if (!uuid_pair_available()) return;
    uint8_t buf[128]; uint32_t n = string_literal(buf, 0, ns,
            (uint16_t)strlen(ns));
    n = string_literal(buf, n, name, (uint16_t)strlen(name));
    buf[n++] = WL_PLAN_EXPR_ARITH_UUID5_SS;
    uint8_t digest[20]; memcpy(digest, expected, 20);
    digest[6] = (digest[6] & 15) | 0x50;
    digest[8] = (digest[8] & 63) | 0x80;
    int64_t value; memcpy(&value, digest, 8);
    n = compiled_const(buf, n, value); buf[n++] = WL_PLAN_EXPR_CMP_EQ;
    string_caller_twins(buf, n, 0, 2, true);
    /* Above BOTH callers must emit the independently known true value. */
    wl_col_session_t *sess = join_session();
    sess->intern = string_table(sess->memory_governor, 0);
    wl_columnar_expr_status_t status; bool failed; int64_t actual;
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0,
        &actual, &failed, sess, &status, false, true, c) == 0);
    CHECK(!failed && actual == 1 && status == WL_COLUMNAR_EXPR_OK);
    finish(c, 0); release(c); wl_intern_free(sess->intern);
    sess->intern = NULL; join_session_destroy(sess);
}
static void
test_uuid_pair_lifecycle(void)
{
    for (int map = 0; map < 2; map++) {
#ifdef WL_MBEDTLS_ENABLED
        if (!uuid_pair_available()) continue;
        const int stages[] = {14, 37, 31, 36, 41, 43, 33, 45, 46, 3};
        for (unsigned i = 0; i < sizeof(stages)/sizeof(*stages); i++)
            for (int positive = 0; positive < 2; positive++)
                digest_lifecycle_trial_kind(map, stages[i], positive, -1,
                    false, false, true);
        for (int positive = 0; positive < 2; positive++)
            digest_lifecycle_trial_kind(map, 14, positive, -1, false, false, 2);
        for (int stage = 0; stage <= 3; stage++)
            digest_lifecycle_trial_kind(map, 0, 0, stage, false, false, true);
        digest_lifecycle_trial_kind(map, 0, 0, -1, false, true, true);
        digest_lifecycle_trial_kind(map, 0, 0, 2, true, false, true);
        digest_lifecycle_trial_kind(map, 31, 0, -1, true, false, true);
        digest_lifecycle_trial_kind(map, 31, -1, -1, true, false, true);
        digest_lifecycle_trial_kind(map, 41, 1, -1, true, false, true);
#else
        for (int positive = 0; positive < 2; positive++)
            digest_lifecycle_trial_kind(map, 3, positive, -1, false, false,
                true);
#endif
    }
}
static void
test_uuid_pair_native_and_fallback_vectors(void)
{
#ifdef WL_MBEDTLS_ENABLED
    if (!uuid_pair_available()) return;
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_UUID5,
                           WL_PLAN_EXPR_ARITH_UUID5_SS,
                           WL_PLAN_EXPR_ARITH_UUID5_SI,
                           WL_PLAN_EXPR_ARITH_UUID5_IS};
    const int64_t operands[][2] = {{INT64_MIN, INT64_MAX}, {99, -1}, {0, 1}};
    for (unsigned k = 0; k < sizeof(ops); k++)
        for (unsigned j = 0; j < 3; j++) {
            unsigned char input[34]; size_t length = 0;
            for (unsigned i = 0; i < 2; i++) {
                if (k) {
                    input[length++] = 'I'; input[length++] = 8;
                    for (unsigned b = 1; b < 8; b++) input[length++] = 0;
                }
                memcpy(input + length, &operands[j][i], 8); length += 8;
            }
            unsigned char digest[20]; size_t actual_length;
            CHECK(psa_hash_compute(PSA_ALG_SHA_1, input, length, digest, 20,
                &actual_length) == PSA_SUCCESS && actual_length == 20);
            digest[6] = (digest[6] & 15) | 0x50;
            digest[8] = (digest[8] & 63) | 0x80;
            int64_t expected; memcpy(&expected, digest, 8);
            uint8_t buf[48]; uint32_t n = compiled_const(buf, 0,
                    operands[j][0]);
            n = compiled_const(buf, n, operands[j][1]); buf[n++] = ops[k];
            n = compiled_const(buf, n, expected);
            buf[n++] = WL_PLAN_EXPR_CMP_EQ;
            string_caller_twins(buf, n, 0, -1, true);
            bool failed; int64_t value; wl_columnar_expr_status_t status;
            wl_evaluation_control_t *c = control(0);
            CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL,
                0, &value, &failed, NULL, &status, false, true,
                c) == 0 && !failed &&
                value == 1);
            finish(c, 0); release(c);
        }
#endif
}
static void
test_uuid_pair_mixed_vectors(void)
{
#ifdef WL_MBEDTLS_ENABLED
    if (!uuid_pair_available()) return;
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_UUID5_SS,
                           WL_PLAN_EXPR_ARITH_UUID5_SI,
                           WL_PLAN_EXPR_ARITH_UUID5_IS};
    const char text[] = "abcdefgh";
    int64_t native; memcpy(&native, text, 8);
    for (unsigned k = 0; k < sizeof(ops); k++) {
        bool symbols[2] = {k != 2, k != 1};
        unsigned char input[34]; size_t length = 0;
        uint8_t buf[64]; uint32_t n = 0;
        for (unsigned i = 0; i < 2; i++) {
            input[length++] = symbols[i] ? 'S' : 'I';
            input[length++] = 8;
            for (unsigned b = 1; b < 8; b++) input[length++] = 0;
            memcpy(input + length, text, 8); length += 8;
            n = symbols[i] ? string_literal(buf, n, text, 8) :
                compiled_const(buf, n, native);
        }
        unsigned char digest[20]; size_t actual_length;
        CHECK(psa_hash_compute(PSA_ALG_SHA_1, input, length, digest, 20,
            &actual_length) == PSA_SUCCESS && actual_length == 20);
        digest[6] = (digest[6] & 15) | 0x50;
        digest[8] = (digest[8] & 63) | 0x80;
        int64_t expected; memcpy(&expected, digest, 8);
        buf[n++] = ops[k]; n = compiled_const(buf, n, expected);
        buf[n++] = WL_PLAN_EXPR_CMP_EQ;
        string_caller_twins(buf, n, 0, 2, true);
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        bool failed; int64_t value; wl_columnar_expr_status_t status;
        wl_evaluation_control_t *c = control(0);
        CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0,
            &value, &failed, sess, &status, false, true,
            c) == 0 && !failed && value == 1);
        finish(c, 0); release(c); wl_intern_free(sess->intern);
        sess->intern = NULL; join_session_destroy(sess);
    }
#endif
}
static void
test_uuid_pair_vectors(void)
{
    { const uint8_t expected[20] = {0x84, 0x85, 0x06, 0x9a, 0xb7, 0x9f, 0x21,
                                    0xcd, 0xfc, 0x50, 0x59, 0x19, 0x82, 0x8c,
                                    0x91, 0x0e, 0xe9, 0x80, 0x50, 0x41};
      uuid_known_vector("ab", "c", expected); }
    { const uint8_t expected[20] = {0x24, 0x44, 0xa9, 0xc0, 0xfd, 0x88, 0xf7,
                                    0xea, 0x3d, 0x1b, 0xeb, 0x38, 0xe8, 0xdf,
                                    0xc2, 0xd7, 0x62, 0xf4, 0x21, 0xe0};
      uuid_known_vector("a", "bc", expected); }
    { const uint8_t expected[20] = {0x8a, 0x85, 0xda, 0x92, 0xa1, 0xfa, 0x7e,
                                    0xb2, 0xf5, 0x75, 0x10, 0x6f, 0x87, 0x2f,
                                    0x84, 0x45, 0x33, 0x2f, 0xc1, 0x08};
      uuid_known_vector("", "", expected); }
    { const uint8_t expected[20] = {0x13, 0xe1, 0xe1, 0x7b, 0xed, 0xd4, 0xa4,
                                    0x2b, 0x99, 0x80, 0x8b, 0x46, 0xf2, 0x9f,
                                    0x48, 0x5b, 0xb4, 0x45, 0xf9, 0x07};
      uuid_known_vector("abcdefgh", "abcdefgh", expected); }
}

static const char rfc_namespace[] = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
static const char rfc_python[] = "886313e1-3b8a-5372-9b90-0c9aee199e5d";
static uint32_t
rfc_ids(uint8_t *buf, int64_t ns, int64_t name)
{
    uint32_t n = compiled_const(buf, 0, ns);
    n = compiled_const(buf, n, name); buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    return n;
}
static void
rfc_known_twins(int domain, int mode, const char *ns, const char *name,
    const char *expected)
{
    if (!uuid_pair_available()) return;
    uint64_t measured[2] = {0};
    for (int map = 0; map < 2; map++) for (int trial = 0; trial < 3; trial++) {
            wl_col_session_t *sess = join_session(), *raw = join_session();
            wl_columnar_memory_governor_ref_t *t = domain ==
                2 ? fresh_governor() : NULL;
            wl_columnar_memory_governor_ref_t *tr = domain ==
                2 ? fresh_governor() : NULL;
            sess->intern = string_table(domain == 1 ? sess->memory_governor : t,
                    0);
            raw->intern = string_table(domain == 1 ? raw->memory_governor : tr,
                    0);
            if (mode == 2) {
                CHECK(wl_intern_put(sess->intern, "unrelated") == 0);
                CHECK(wl_intern_put(raw->intern, "unrelated") == 0);
            }
            if (mode) {
                CHECK(wl_intern_put(sess->intern, expected) == mode-1);
                CHECK(wl_intern_put(raw->intern, expected) == mode-1);
            }
            int64_t ns_id = wl_intern_put(sess->intern, ns);
            int64_t name_id = wl_intern_put(sess->intern, name);
            CHECK(wl_intern_put(raw->intern, ns) == ns_id &&
                wl_intern_put(raw->intern, name) == name_id);
            uint32_t count = wl_intern_count(sess->intern);
            int64_t result = mode ? (int64_t)mode-1 : (int64_t)count;
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            col_rel_t *ref = relation(1, 1, 1, false, raw->memory_governor);
            CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
                ref) == 0);
            make_lazy(source);
            uint64_t before = string_reserved(domain ==
                    1 ? sess->memory_governor : t);
            uint8_t buf[32]; uint32_t n = rfc_ids(buf, ns_id, name_id);
            wl_plan_expr_buffer_t expr = {buf, n};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
                *reference = filter_frame(raw);
            wl_evaluation_control_t *c = control(trial == 1?measured[map]-1:
                    trial == 2?measured[map]:0);
            int rc = trial == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:0;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == rc);
            uint64_t cost = finish(c, rc);
            if (!trial) measured[map] = cost;
            if (trial == 2) CHECK(cost == measured[map]);
            CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == 0);
            if (!rc) {
                filter_equal(join_output(frame), join_output(reference));
                CHECK(join_output(frame)->nrows == (map || result != 0));
                if (map) CHECK(join_output(frame)->columns[0][0] == result);
                CHECK(strcmp(wl_intern_reverse(sess->intern, result),
                    expected) == 0);
            } else lazy_unchanged(source);
            CHECK(!sess->memory_budget_denied); source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            /* Work can stop after successful dictionary publication. */
            uint32_t after = wl_intern_count(sess->intern);
            CHECK(after == count || (!mode && after == count+1));
            if (after > count) CHECK(strcmp(wl_intern_reverse(sess->intern,
                    count), expected) == 0);
            CHECK(string_reserved(domain == 1?sess->memory_governor:t) ==
                before+((domain && after > count)?37:0));
            if (!rc) {
                CHECK(after == wl_intern_count(raw->intern));
                for (uint32_t id = 0; id < after; id++) CHECK(strcmp(
                            wl_intern_reverse(sess->intern, id),
                            wl_intern_reverse(raw->intern, id)) == 0);
            }
            wl_intern_free(sess->intern); wl_intern_free(raw->intern);
            sess->intern = raw->intern = NULL; release(c);
            join_session_destroy(sess); join_session_destroy(raw);
            if (t) {
                governor_empty(t); governor_empty(tr);
                wl_columnar_memory_governor_ref_release(t);
                wl_columnar_memory_governor_ref_release(tr);
            }
        }
}
static void
test_rfc_twins_and_vectors(void)
{
    int expected = uuid_pair_available()?0:ERANGE;
    for (int domain = 0; domain < 3; domain++)for (int mode = 0; mode < 3;
            mode++){
            rfc_known_twins(domain, mode, rfc_namespace, "python.org",
                rfc_python);
            rfc_known_twins(domain, mode,
                "6BA7B810-9DAD-11D1-80B4-00C04FD430C8",
                "python.org", rfc_python);
        }
    rfc_known_twins(2, 0, rfc_namespace, "",
        "4ebd0208-8328-5d69-8c44-ec50939c0967");
    rfc_known_twins(2, 0, rfc_namespace, "a\xff",
        "4808b8f9-4d10-5659-803b-63f917162f06");
    uint8_t buf[8300]; char text[4098];
    const unsigned lengths[] = {0, 35, 37, 513, 4097};
    for (unsigned j = 0; j < sizeof(lengths)/sizeof(*lengths); j++) {
        memset(text, 'a', lengths[j]); text[lengths[j]] = 0;
        uint32_t n = string_literal(buf, 0, text, (uint16_t)lengths[j]);
        n = string_literal(buf, n, "name", 4);
        buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
        string_caller_twins(buf, n, ERANGE, 2, j == 4);
    }
    const unsigned badpos[] = {0, 8, 13, 18, 23, 35};
    for (unsigned j = 0; j < sizeof(badpos)/sizeof(*badpos); j++) {
        memcpy(text, rfc_namespace, 37); text[badpos[j]] = j?'x':'\xff';
        uint32_t n = string_literal(buf, 0, text, 36);
        n = string_literal(buf, n, "name", 4);
        buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
        string_caller_twins(buf, n, ERANGE, 2, false);
    }
    const char *namespaces[] = {"{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
                                " 6ba7b810-9dad-11d1-80b4-00c04fd430c8", "bad",
                                ""};
    for (unsigned j = 0; j < 4; j++) {
        uint32_t n = string_literal(buf, 0, namespaces[j],
                (uint16_t)strlen(namespaces[j]));
        n = string_literal(buf, n, "name", 4);
        buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
        string_caller_twins(buf, n, ERANGE, 2, false);
    }
    const unsigned namelen[] = {0, 1, 63, 64, 65, 127, 128, 129, 513, 4097};
    for (unsigned j = 0; j < sizeof(namelen)/sizeof(*namelen); j++) {
        memset(text, 'a', namelen[j]); text[namelen[j]] = 0;
        uint32_t n = string_literal(buf, 0, rfc_namespace, 36);
        n = string_literal(buf, n, text, (uint16_t)namelen[j]);
        buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
        string_caller_twins(buf, n, expected, 2, j == 9);
    }
    const char hidden[] = {'a', '\xff', 0, 'x'};
    uint32_t n = string_literal(buf, 0, rfc_namespace, 36);
    n = string_literal(buf, n, hidden, 4);
    buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    string_caller_twins(buf, n, expected, 2, true);
    memcpy(text, rfc_namespace, 37); memcpy(text+37, "hidden", 6);
    n = string_literal(buf, 0, text, 43); n = string_literal(buf, n, "name", 4);
    buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    string_caller_twins(buf, n, expected, 2, true);
    text[20] = 0; n = string_literal(buf, 0, text, 43);
    n = string_literal(buf, n, "name", 4);
    buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    string_caller_twins(buf, n, ERANGE, 2, true);
    n = string_literal(buf, 0, "ffffffff-ffff-ffff-ffff-ffffffffffff", 36);
    n = string_literal(buf, n, "name", 4);
    buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    string_caller_twins(buf, n, expected, 2, false);
    for (int domain = -1; domain < 3; domain++)for (int missing =
            0; missing < 2;
            missing++) {
            n = rfc_ids(buf, missing?99:0, missing?0:99);
            string_caller_twins(buf, n, ERANGE, domain, true);
        }
    buf[0] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    string_caller_twins(buf, 1, ERANGE, -1, true);
}

static void
test_rfc_lifecycle(void)
{
    for (int map = 0; map < 2; map++) {
#ifdef WL_MBEDTLS_ENABLED
        if (!uuid_pair_available()) continue;
        const int stages[] = {14, 49, 31, 36, 40, 41, 33, 45, 47, 48, 50, 3};
        for (unsigned j = 0; j < sizeof(stages)/sizeof(*stages); j++)
            for (int positive = 0; positive < 2; positive++)
                digest_lifecycle_trial_kind(map, stages[j], positive, -1, false,
                    false, 3);
        for (int positive = 0; positive < 2; positive++)
            digest_lifecycle_trial_kind(map, 14, positive, -1, false, false, 4);
        for (int stage = 0; stage <= 3; stage++)
            digest_lifecycle_trial_kind(map, 0, 0, stage, false, false, 3);
        digest_lifecycle_trial_kind(map, 0, 0, -1, false, true, 3);
        digest_lifecycle_trial_kind(map, 0, 0, 2, true, false, 3);
        digest_lifecycle_trial_kind(map, 31, 0, -1, true, false, 3);
        digest_lifecycle_trial_kind(map, 31, -1, -1, true, false, 3);
        digest_lifecycle_trial_kind(map, 41, 1, -1, true, false, 3);
#else
        for (int positive = 0; positive < 2; positive++)
            digest_lifecycle_trial_kind(map, 3, positive, -1, false, false, 3);
#endif
    }
}
static void
test_rfc_original_table_faults(void)
{
    if (!uuid_pair_available())return;
    for (int map = 0; map < 2; map++)for (int domain = 0; domain < 3; domain++)
            for (int fault = 0; fault < 5; fault++) {
                wl_col_session_t *sess = join_session();
                wl_columnar_memory_governor_ref_t *t = domain ==
                    2?fresh_governor():
                    domain == 1?sess->memory_governor:NULL;
                sess->intern = string_table(t, fault == 1?46:0);
                int64_t ns = wl_intern_put(sess->intern, rfc_namespace),
                    name = wl_intern_put(sess->intern, "python.org");
                uint32_t count = wl_intern_count(sess->intern);
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before_s = string_reserved(sess->memory_governor),
                    before_t = string_reserved(t);
                uint8_t buf[32]; uint32_t n = rfc_ids(buf, ns, name);
                wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                if (fault < 2) wl_intern_test_fail_allocation = fault;
                if (fault == 2 && t) wl_intern_test_fail_commit = true;
                if (fault >= 3 && t) {
                    string_quota_governor =
                        wl_columnar_memory_governor_ref_get(t);
                    wl_columnar_memory_reservation_init(&string_quota);
                    string_quota_reached = false;
                    string_quota_stage = fault == 3?1:6;
                    wl_intern_test_controlled_checkpoint = string_quota_hook;
                }
                memset(digest_calls, 0, sizeof(digest_calls));
                wl_columnar_expr_digest_test_provider_result =
                    digest_provider_report;
                bool failing = fault < 2 || t != NULL;
                int expected = failing?ENOMEM:0;
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
                finish(c, expected);
                CHECK(digest_calls[0] == 1 && digest_calls[1] == 1 &&
                    digest_calls[2] == 2 && digest_calls[3] == 1 &&
                    !digest_calls[4]);
                wl_columnar_expr_digest_test_provider_result = NULL;
                CHECK(sess->memory_budget_denied == (fault >= 3 && t));
                if (failing) lazy_unchanged(source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                if (fault >= 3 && t) {
                    CHECK(string_quota_reached);
                    CHECK(wl_columnar_memory_rollback(&string_quota));
                }
                CHECK(wl_intern_count(sess->intern) == count+!failing);
                uint64_t growth = fault == 1?293:37;
                CHECK(string_reserved(t) == before_t+(t && !failing?growth:0));
                CHECK(string_reserved(sess->memory_governor) == before_s+
                    (domain == 1 && !failing?growth:0));
                CHECK(wl_intern_test_fail_allocation == -1 &&
                    !wl_intern_test_fail_commit);
                wl_intern_test_controlled_checkpoint = NULL;
                frame = filter_frame(sess); retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                uint64_t fresh = finish(c, 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(wl_intern_count(sess->intern) == count+1 && strcmp(
                        wl_intern_reverse(sess->intern, count),
                        rfc_python) == 0);
                frame = filter_frame(sess); retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                uint64_t warm = finish(c, 0); CHECK(warm <= fresh);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                    wl_intern_count(sess->intern) == count+1);
                wl_intern_free(sess->intern); sess->intern = NULL; release(c);
                join_session_destroy(sess);
                if (domain == 2) {
                    governor_empty(t);
                    wl_columnar_memory_governor_ref_release(t);
                }
            }
}
static void
test_rfc_original_table_overlap_and_stops(void)
{
    if (!uuid_pair_available())return;
    for (int map = 0; map < 2; map++)for (int domain = 0; domain < 3; domain++)
            for (int stage = 0; stage < 3; stage++) for (int positive = 0;
                    positive < 2; positive++){
                    wl_col_session_t *sess = join_session();
                    wl_columnar_memory_governor_ref_t *t = domain ==
                        2?fresh_governor():
                        domain == 1?sess->memory_governor:NULL;
                    uint64_t initial = string_reserved(t);
                    sess->intern = string_table(t, 0);
                    CHECK(wl_intern_put(sess->intern, rfc_namespace) == 0 &&
                        wl_intern_put(sess->intern, "python.org") == 1);
                    uint64_t own_image = string_reserved(t)-initial;
                    col_rel_t *source = relation(1, 1, 1, false,
                            sess->memory_governor);
                    CHECK(session_add_rel(sess, source) == 0);
                    make_lazy(source);
                    uint64_t before_s = string_reserved(sess->memory_governor),
                        before_t = string_reserved(t);
                    uint8_t buf[32]; uint32_t n = rfc_ids(buf, 0, 1);
                    wl_plan_expr_buffer_t expr = {buf, n};
                    wl_plan_op_t ops[2] = {
                        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                         .delta_mode = WL_DELTA_FORCE_FULL},
                        {.op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                         .filter_expr = expr,
                         .project_count = 1, .map_exprs = &expr,
                         .map_expr_count = 1}
                    };
                    wl_columnar_eval_stack_cleanup_frame_t *frame =
                        filter_frame(sess);
                    wl_evaluation_control_t *c = control(0);
                    if (!stage && t) {
                        string_overlap_governor =
                            wl_columnar_memory_governor_ref_get(t);
                        string_overlap_copy = 37;
                        string_overlap_image = own_image+37;
                        string_overlap_flags = 0;
                        wl_intern_test_controlled_checkpoint =
                            string_overlap_hook;
                    } else if (stage) {
                        string_stop_stage = stage == 1?13:3;
                        string_stop_progress = stage == 1?0:3;
                        string_stop_positive = positive != 0;
                        string_stop_reached = 0;
                        if (stage ==
                            1) wl_intern_test_controlled_checkpoint =
                                string_intern_stop;
                        else wl_columnar_expr_test_controlled_checkpoint =
                                string_expr_stop;
                    }
                    int expected =
                        stage?
                        (positive?EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED):0;
                    CHECK(compiled_dispatch(sess, frame, ops, 2,
                        c) == expected);
                    finish(c, expected);
                    if (!stage && t) CHECK(string_overlap_flags == 7);
                    if (stage) {
                        CHECK(string_stop_reached == 1); lazy_unchanged(source);
                    }
                    wl_intern_test_controlled_checkpoint = NULL;
                    wl_columnar_expr_test_controlled_checkpoint = NULL;
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                    bool retained = stage != 1;
                    CHECK(wl_intern_count(sess->intern) ==
                        2u+(uint32_t)retained);
                    CHECK(string_reserved(t) == before_t+(t && retained?37:0));
                    CHECK(string_reserved(sess->memory_governor) == before_s+
                        (domain == 1 && retained?37:0));
                    CHECK(strcmp(wl_intern_reverse(sess->intern, 0),
                        rfc_namespace) == 0 &&
                        strcmp(wl_intern_reverse(sess->intern, 1),
                        "python.org") == 0);
                    if (retained) CHECK(strcmp(wl_intern_reverse(sess->intern,
                            2), rfc_python) == 0);
                    frame = filter_frame(sess); retry(c, 0);
                    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                    finish(c, 0);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                        wl_intern_count(sess->intern) == 3 &&
                        strcmp(wl_intern_reverse(sess->intern, 2),
                        rfc_python) == 0);
                    wl_intern_free(sess->intern); sess->intern = NULL;
                    release(c); join_session_destroy(sess);
                    if (domain == 2) {
                        governor_empty(t);
                        wl_columnar_memory_governor_ref_release(t);
                    }
                }
    string_overlap_governor = NULL;
}
static void
test_rfc_zero_and_short_circuit(void)
{
    for (int map = 0; map < 2; map++)for (int kind = 0; kind < 3; kind++) {
            wl_col_session_t *sess = join_session();
            sess->intern = string_table(sess->memory_governor, 0);
            char long_text[601]; memset(long_text, 'a', 600);
            long_text[600] = 0;
            const char *ns = kind == 1?long_text:kind == 2?
                "xba7b810-9dad-11d1-80b4-00c04fd430c8":rfc_namespace;
            CHECK(wl_intern_put(sess->intern,
                ns) == 0 && wl_intern_put(sess->intern,
                kind == 1?"name":long_text) == 1);
            col_rel_t *source = relation(1, kind?1:0, kind?1:0, false,
                    sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint8_t buf[32]; uint32_t n = rfc_ids(buf, 0, kind == 1?99:1);
            wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            memset(digest_calls, 0, sizeof(digest_calls)); digest_reached = 0;
            digest_stop_stage = 14; digest_stop_progress = 300;
            digest_primary = 0;
            wl_columnar_expr_test_controlled_checkpoint =
                digest_checkpoint_stop;
            wl_columnar_expr_digest_test_provider_result =
                digest_provider_report;
            wl_intern_test_fail_allocation = 0;
            int expected = kind && map?ERANGE:0;
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
            finish(c, expected);
            CHECK(!digest_reached && wl_intern_test_fail_allocation == 0);
            for (unsigned i = 0; i < 5; i++) CHECK(!digest_calls[i]);
            wl_intern_test_fail_allocation = -1;
            wl_columnar_expr_test_controlled_checkpoint = NULL;
            wl_columnar_expr_digest_test_provider_result = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                wl_intern_count(sess->intern) == 2);
            wl_intern_free(sess->intern); sess->intern = NULL; release(c);
            join_session_destroy(sess);
        }
}
static void
test_rfc_shared_reverse_parent_stop(void)
{
    if (!uuid_pair_available())return;
    wl_col_session_t *sess = join_session();
    wl_columnar_memory_governor_ref_t *t = fresh_governor();
    sess->intern = string_table(t, 0);
    CHECK(wl_intern_put(sess->intern, rfc_namespace) == 0 &&
        wl_intern_put(sess->intern, "python.org") == 1);
    const char *expected_texts[] = {rfc_namespace, "python.org", rfc_python};
    string_reader_t reader = {.table = sess->intern,
                              .expected_texts = expected_texts,
                              .expected_count = 3};
    atomic_init(&reader.stop, 0);
    CHECK(wl_mutex_init(&reader.lock) == 0 &&
        wl_cond_init(&reader.condition) == 0);
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, string_reverse_reader, &reader) == 0);
    wl_mutex_lock(&reader.lock);
    while (!reader.ready)wl_cond_wait(&reader.condition, &reader.lock);
    wl_mutex_unlock(&reader.lock);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(t);
    uint8_t buf[32]; uint32_t n = rfc_ids(buf, 0, 1);
    wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
         .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    string_stop_stage = 3; string_stop_progress = 3;
    string_stop_positive = false;
    string_stop_reached = 0;
    wl_columnar_expr_test_controlled_checkpoint = string_expr_stop;
    CHECK(compiled_dispatch(sess, frame, ops, 2,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED); CHECK(string_stop_reached == 1);
    lazy_unchanged(source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(wl_intern_count(sess->intern) == 3 &&
        string_reserved(t) == before+37 &&
        wl_intern_get(sess->intern, rfc_python) == 2);
    frame = filter_frame(sess); retry(c, 0);
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
    finish(c, 0); CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
        wl_intern_count(sess->intern) == 3 && string_reserved(t) == before+37);
    wl_mutex_lock(&reader.lock);
    while (!reader.observed_new)wl_cond_wait(&reader.condition, &reader.lock);
    wl_mutex_unlock(&reader.lock); atomic_store(&reader.stop, 1);
    CHECK(wl_thread_join(&thread) == 0 && reader.max_seen == 3);
    wl_cond_destroy(&reader.condition); wl_mutex_destroy(&reader.lock);
    wl_intern_free(sess->intern); sess->intern = NULL; release(c);
    join_session_destroy(sess);
    governor_empty(t); wl_columnar_memory_governor_ref_release(t);
}
static void
test_rfc_saturated_operands(void)
{
    if (!uuid_pair_available())return;
    for (int map = 0; map < 2; map++)for (int saturated = 0; saturated < 2;
            saturated++){
            wl_col_session_t *sess = join_session();
            sess->intern = string_table(sess->memory_governor, 0);
            CHECK(wl_intern_put(sess->intern, rfc_namespace) == 0);
            col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            uint8_t buf[1100]; uint32_t n = 0;
            if (saturated) for (unsigned i = 0; i < 513; i++) {
                    buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 0;
                }
            buf[n++] = WL_PLAN_EXPR_STR_FN_UUID5_RFC;
            wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
            };
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
            CHECK(join_output(frame)->nrows == 1);
            if (map) CHECK(join_output(frame)->columns[0][0] == 1);
            CHECK(wl_intern_count(sess->intern) == 2 && strcmp(
                    wl_intern_reverse(sess->intern, 1),
                    "4cbce65e-cfcd-541c-b23c-74415552d2be") == 0);
            wl_columnar_expr_context_t ctx = {.intern = sess->intern};
            int64_t value; wl_columnar_expr_status_t status;
            CHECK(wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &value, &ctx,
                &status) == 0 &&
                status == WL_COLUMNAR_EXPR_OK && value == 1);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            wl_intern_free(sess->intern); sess->intern = NULL; release(c);
            join_session_destroy(sess);
        }
}
extern void (*wl_columnar_expr_uuid4_test_checkpoint)(wl_evaluation_control_t *,
    int, uint64_t, bool);
extern int64_t (*wl_columnar_expr_uuid4_test_provider_result)(int, int64_t);
extern void (*wl_columnar_expr_uuid4_test_bytes)(int, const uint8_t *);
extern void (*wl_columnar_expr_uuid4_test_override_bytes)(uint8_t *);
static uint32_t uuid4_calls[2], uuid4_seen[2], uuid4_reached;
static int64_t uuid4_actual[2];
static int uuid4_override = -1, uuid4_stage, uuid4_primary;
static bool uuid4_known;
static uint8_t uuid4_recorded[2][16];
static const uint8_t uuid4_vector[16] = {0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5,
                                         0xf6, 0xf7,
                                         0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd,
                                         0xfe, 0xff};
static int64_t
uuid4_provider_report(int stage, int64_t actual)
{
    CHECK(stage >= 0 && stage < 2); uuid4_calls[stage]++;
    uuid4_actual[stage] = actual;
#ifdef WL_MBEDTLS_ENABLED
    if (stage == uuid4_override &&
        actual == PSA_SUCCESS) return PSA_ERROR_INSUFFICIENT_MEMORY;
#endif
    return actual;
}
static void
uuid4_record_bytes(int stage, const uint8_t *bytes)
{
    CHECK(stage == 0 || stage == 1); uuid4_seen[stage]++;
    memcpy(uuid4_recorded[stage], bytes, 16);
    if (stage == 1) {
        uint8_t expected[16];
        memcpy(expected, uuid4_known?uuid4_vector:uuid4_recorded[0], 16);
        expected[6] = (expected[6]&15)|0x40;
        expected[8] = (expected[8]&63)|0x80;
        CHECK(memcmp(expected, bytes, 16) == 0 && (bytes[6]&0xf0) == 0x40 &&
            (bytes[8]&0xc0) == 0x80);
    }
}
static void
uuid4_known_override(uint8_t *bytes)
{
    /* Separate deterministic transform proof, AFTER genuine success. */
    memcpy(bytes, uuid4_vector, 16);
}
static void
uuid4_checkpoint_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)progress; (void)failed;
    if (stage != uuid4_stage)return;
    wl_columnar_expr_uuid4_test_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    uuid4_reached++;
    if (uuid4_primary)CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
uuid4_hooks_reset(void)
{
    memset(uuid4_calls, 0, sizeof(uuid4_calls));
    memset(uuid4_seen, 0, sizeof(uuid4_seen));
    uuid4_reached = 0; uuid4_actual[0] = uuid4_actual[1] = INT64_MIN;
}
static void
uuid4_hooks_clear(void)
{
    wl_columnar_expr_uuid4_test_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    wl_columnar_expr_uuid4_test_provider_result = NULL;
    wl_columnar_expr_uuid4_test_bytes = NULL;
    wl_columnar_expr_uuid4_test_override_bytes = NULL;
    uuid4_override = -1; uuid4_known = false;
}
static uint32_t
uuid4_bytecode(uint8_t *buf, int map, bool saturated)
{
    uint32_t n = 0;
    if (saturated)for (unsigned j = 0; j < 513; j++) {
            buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 0;
        }
    buf[n++] = WL_PLAN_EXPR_ARITH_UUID4;
    if (!map && !saturated) {
        buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 1;
    }
    return n;
}
static uint64_t
uuid4_caller_trial(int map, bool rows, bool saturated, bool known, int stop,
    int primary, int override, uint64_t budget, bool reset, bool budget_denied)
{
    wl_col_session_t *sess = join_session();
    sess->intern = string_table(sess->memory_governor, 1);
    col_rel_t *source = relation(1, rows?1:0, rows?1:0, false,
            sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(sess->memory_governor);
    uint8_t buf[1100]; uint32_t n = uuid4_bytecode(buf, map, saturated);
    wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER, .filter_expr = expr,
         .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(budget);
    uuid4_hooks_reset(); uuid4_known = known; uuid4_override = override;
    wl_columnar_expr_uuid4_test_provider_result = uuid4_provider_report;
    wl_columnar_expr_uuid4_test_bytes = uuid4_record_bytes;
    wl_columnar_expr_uuid4_test_override_bytes =
        known?uuid4_known_override:NULL;
    uuid4_stage = stop; uuid4_primary = primary;
    if (stop) {
        if (stop ==
            3) wl_columnar_expr_test_controlled_checkpoint =
                uuid4_checkpoint_stop;
        else wl_columnar_expr_uuid4_test_checkpoint = uuid4_checkpoint_stop;
    }
    int expected = 0;
#ifdef WL_MBEDTLS_ENABLED
    if (rows && override >= 0)expected = map?ERANGE:0;
#else
    if (rows)expected = map?ERANGE:0;
#endif
    if (rows &&
        stop) expected = primary?EOVERFLOW:WL_EVALUATION_CONTROL_CANCELLED;
    if (rows && budget_denied)expected = WL_EVALUATION_CONTROL_WORK_LIMIT;
    int rc = compiled_dispatch(sess, frame, ops, 2, c);
    if (rc != expected)fprintf(stderr,
            "uuid4 map%d rows%d stop%d primary%d override%d budget%llu rc%d expected%d\n",
            map, rows, stop, primary, override, (unsigned long long)budget, rc,
            expected);
    CHECK(rc == expected); uint64_t work = finish(c, expected);
    CHECK(uuid4_reached == (uint32_t)(rows && stop));
#ifdef WL_MBEDTLS_ENABLED
    uint32_t init = rows && stop != 60;
    uint32_t random = init && stop != 61 && stop != 62 && override != 0;
    CHECK(uuid4_calls[0] == init && uuid4_calls[1] == random);
    if (init)CHECK(uuid4_actual[0] == PSA_SUCCESS);
    if (random)CHECK(uuid4_actual[1] == PSA_SUCCESS);
    if (rows && !stop && override < 0 &&
        !budget) CHECK(uuid4_seen[0] == 1 && uuid4_seen[1] == 1);
#else
    CHECK(!uuid4_calls[0] && !uuid4_calls[1] && !uuid4_seen[0] &&
        !uuid4_seen[1]);
#endif
    CHECK(!sess->memory_budget_denied && wl_intern_count(sess->intern) == 1 &&
        strcmp(wl_intern_reverse(sess->intern, 0), "seed-0") == 0);
    if (expected)lazy_unchanged(source);
    if (!expected && rows && override < 0) {
#ifdef WL_MBEDTLS_ENABLED
        CHECK(join_output(frame)->nrows == (map || !saturated));
        if (map) {
            int64_t value; memcpy(&value, uuid4_recorded[1], 8);
            CHECK(join_output(frame)->columns[0][0] == (saturated?0:value));
        }
#endif
    }
    source_unpinned(source, source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
        string_reserved(sess->memory_governor) == before);
    uuid4_hooks_clear();
    if (reset) {
        retry(c, 0); frame = filter_frame(sess); uuid4_hooks_reset();
        wl_columnar_expr_uuid4_test_provider_result = uuid4_provider_report;
        wl_columnar_expr_uuid4_test_bytes = uuid4_record_bytes;
#ifdef WL_MBEDTLS_ENABLED
        expected = 0;
#else
        expected = rows && map?ERANGE:0;
#endif
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
        finish(c, expected);
#ifdef WL_MBEDTLS_ENABLED
        CHECK(uuid4_calls[0] == (uint32_t)rows &&
            uuid4_calls[1] == (uint32_t)rows);
        CHECK(uuid4_seen[0] == (uint32_t)rows &&
            uuid4_seen[1] == (uint32_t)rows);
#endif
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
            string_reserved(sess->memory_governor) == before);
        uuid4_hooks_clear();
    }
    wl_intern_free(sess->intern); sess->intern = NULL; release(c);
    join_session_destroy(sess);
    return work;
}
static void
test_uuid4_actual_bytes_and_work(void)
{
    for (int map = 0; map < 2; map++) {
        uuid4_caller_trial(map, false, false, false, 0, 0, -1, 0, false, false);
#ifdef WL_MBEDTLS_ENABLED
        /* Unmodified REAL random16 is distinct from the known-vector fixture. */
        uint64_t work = uuid4_caller_trial(map, true, false, false, 0, 0, -1, 0,
                true, false);
        CHECK(uuid4_caller_trial(map, true, false, false, 0, 0, -1, work,
            false, false) == work);
        uuid4_caller_trial(map, true, false, false, 0, 0, -1, work-1, true,
            true);
        uuid4_caller_trial(map, true, false, true, 0, 0, -1, 0, false, false);
        uuid4_caller_trial(map, true, true, false, 0, 0, -1, 0, false, false);
#else
        uuid4_caller_trial(map, true, false, false, 0, 0, -1, 0, true, false);
#endif
    }
}
static void
test_uuid4_reached_stops_and_status(void)
{
    for (int map = 0; map < 2; map++) {
#ifdef WL_MBEDTLS_ENABLED
        const int stages[] = {60, 61, 62, 63, 64, 65, 3};
        for (unsigned j = 0; j < sizeof(stages)/sizeof(*stages); j++)
            for (int positive = 0; positive < 2; positive++)
                uuid4_caller_trial(map, true, false, false, stages[j], positive,
                    -1, 0, true, false);
        for (int fault = 0; fault < 2; fault++) {
            uuid4_caller_trial(map, true, false, false, 0, 0, fault, 0, true,
                false);
            for (int positive = 0; positive < 2; positive++) {
                uuid4_caller_trial(map, true, false, false, fault?63:61,
                    positive, fault, 0, true, false);
                uuid4_caller_trial(map, true, false, false, 3, positive, fault,
                    0, true, false);
            }
        }
#else
        for (int positive = 0; positive < 2; positive++)
            uuid4_caller_trial(map, true, false, false, 3, positive, -1, 0,
                true, false);
#endif
    }
}
static void
test_uuid4_owned_cleanup(void)
{
#ifdef WL_MBEDTLS_ENABLED
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 1);
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[8]; uint32_t n = uuid4_bytecode(buf, map, false);
        wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t op = {
            .op = map?WL_PLAN_OP_MAP:WL_PLAN_OP_FILTER, .filter_expr = expr,
            .project_count = 1, .map_exprs = &expr, .map_expr_count = 1
        };
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        uuid4_hooks_reset(); uuid4_known = false;
        wl_columnar_expr_uuid4_test_provider_result = uuid4_provider_report;
        wl_columnar_expr_uuid4_test_bytes = uuid4_record_bytes;
        filter_reached = 0;
        if (map)wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map?col_op_map(&op, wl_columnar_eval_stack_cleanup_stack(frame),
            sess):
            wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(uuid4_calls[1] >= 1 && uuid4_seen[1] >= 1 &&
            filter_reached == 1 &&
            wl_intern_count(sess->intern) == 1 &&
            strcmp(wl_intern_reverse(sess->intern, 0), "seed-0") == 0);
        uuid4_hooks_clear();
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL; release(c);
        join_session_destroy(sess);
    }
#endif
}
extern bool (*wl_columnar_expr_hmac_test_eligible)(void);
extern void (*wl_columnar_expr_hmac_test_checkpoint)(wl_evaluation_control_t *,
    int, uint64_t, bool);
extern int64_t (*wl_columnar_expr_hmac_test_provider_result)(int, int64_t,
    size_t *);
extern void (*wl_columnar_expr_hmac_test_normalized_key)(const uint8_t *,
    size_t);
extern void (*wl_columnar_expr_hmac_test_mac)(const uint8_t *, size_t);
static uint32_t hmac_calls[7], hmac_reached;
static int hmac_stage, hmac_primary, hmac_override = -1;
static bool hmac_abort_error, hmac_destroy_error, hmac_length_error;
static uint8_t hmac_expected_key[64], hmac_expected_mac[32];
static size_t hmac_expected_key_length;
static uint32_t hmac_key_seen, hmac_mac_seen;
static int64_t
hmac_provider_report(int stage, int64_t actual, size_t *length)
{
    CHECK(stage >= 0 && stage < 7); hmac_calls[stage]++;
#ifdef WL_MBEDTLS_ENABLED
    CHECK(actual == PSA_SUCCESS);
    if (stage == 4 && hmac_length_error) {
        CHECK(length && *length == 32); (*length)--;
    }
    /* Provider FIRST, including actual abort/destroy FIRST. Report overrides
    * prove protocol/precedence, never genuine failed-release reclamation. */
    if (stage == hmac_override || (stage == 5 && hmac_abort_error) ||
        (stage == 6 &&
        hmac_destroy_error)) return PSA_ERROR_INSUFFICIENT_MEMORY;
#else
    (void)length;
#endif
    return actual;
}
static void
hmac_stop(wl_evaluation_control_t *c, int stage, uint64_t progress, bool failed)
{
    (void)failed;
    uint64_t threshold = stage == 14 ? 300 : stage == 32 ||
        stage == 75 ? 320 : stage == 3 ? 3 : 0;
    if (stage != hmac_stage || progress < threshold) return;
    wl_columnar_expr_hmac_test_checkpoint = NULL;
    wl_columnar_expr_digest_test_checkpoint = NULL;
    wl_columnar_expr_hash_test_controlled_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    hmac_reached++;
    if (hmac_primary > 0) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else if (hmac_primary < 0) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(c);
}
static void
hmac_hooks_clear(void)
{
    wl_columnar_expr_hmac_test_checkpoint = NULL;
    wl_columnar_expr_hmac_test_provider_result = NULL;
    wl_columnar_expr_hmac_test_eligible = NULL;
    wl_columnar_expr_hmac_test_normalized_key = NULL;
    wl_columnar_expr_hmac_test_mac = NULL;
    wl_columnar_expr_digest_test_checkpoint = NULL;
    wl_columnar_expr_digest_test_provider_result = NULL;
    wl_columnar_expr_hash_test_controlled_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    hmac_override = -1;
    hmac_abort_error = hmac_destroy_error = hmac_length_error = false;
}
static void
hmac_normalized_observe(const uint8_t *key, size_t length)
{
    CHECK(length == hmac_expected_key_length && length <= 64 &&
        memcmp(key, hmac_expected_key, length) == 0); hmac_key_seen++;
}
static void
hmac_mac_observe(const uint8_t *mac, size_t length)
{
    CHECK(length == 32 && memcmp(mac, hmac_expected_mac, 32) == 0);
    hmac_mac_seen++;
}
static void
test_hmac_actual_twins(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    !defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    return; /* Genuine optional configured-false is tested separately. */
#endif
#ifdef WL_MBEDTLS_ENABLED
    wl_evaluation_control_t *qualification = control(0);
    CHECK(wl_columnar_expr_hmac_eligible(qualification) == 0);
    finish(qualification, 0); release(qualification);
    printf("HMAC qualification: audited software predicate=true; ");
#ifdef WL_COLUMNAR_HMAC_MBED_VERSION_GETTER
    printf("Mbed getter=linked; ");
#else
    printf("Mbed getter=unavailable; ");
#endif
#ifdef WL_COLUMNAR_HMAC_TF_VERSION_GETTER
    printf("TF getter=linked\n");
#else
    printf("TF getter=unavailable\n");
#endif
    int expected = 0;
#else
    int expected = ERANGE;
#endif
    const uint8_t ops[] = {WL_PLAN_EXPR_ARITH_HMAC_SHA256,
                           WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS,
                           WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
                           WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS};
    const int64_t values[] = {0, 2, 3, 4, -1, 99, INT64_MIN, INT64_MAX};
    for (unsigned k = 0; k < sizeof(ops); k++) {
        for (int domain = -1; domain < 3; domain++)
            for (unsigned j = 0; j < sizeof(values)/sizeof(*values); j++) {
                uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, values[j]);
                n = compiled_const(buf, n, values[7-j]); buf[n++] = ops[k];
                string_caller_twins(buf, n, expected, domain, j == 0);
            }
        uint8_t full[1100]; uint32_t n = 0;
        for (unsigned j = 0; j < 513; j++) {
            full[n++] = WL_PLAN_EXPR_BOOL; full[n++] = 1;
        }
        full[n++] = ops[k]; string_caller_twins(full, n, expected, -1, true);
        full[0] = ops[k]; string_caller_twins(full, 1, expected, -1, true);
        const uint32_t keys[] = {0, 1, 8, 31, 32, 63, 64, 65, 127, 128, 129,
                                 513, 4097};
        const uint32_t messages[] = {0, 1, 63, 64, 65, 513, 4097};
        char key[4098], message[4098]; uint8_t buf[8300];
        for (unsigned which = 0; which < 2; which++) {
            unsigned count = which ? sizeof(messages)/
                sizeof(*messages) : sizeof(keys)/sizeof(*keys);
            for (unsigned j = 0; j < count; j++) {
                uint32_t kl = which ? 65 : keys[j],
                    ml = which ? messages[j] : 513;
                memset(key, 'k', kl); key[kl] = 0; memset(message, 'm', ml);
                message[ml] = 0;
                n = k == 1 || k == 2 ? string_literal(buf, 0, message,
                        (uint16_t)ml) : compiled_const(buf, 0, INT64_MIN);
                n = k == 1 || k == 3 ? string_literal(buf, n, key,
                        (uint16_t)kl) : compiled_const(buf, n, INT64_MAX);
                buf[n++] = ops[k];
                string_caller_twins(buf, n, expected, 2, j == count-1);
                if (j == count-1 && !expected) {
                    n = compiled_const(buf, n, -1);
                    buf[n++] = WL_PLAN_EXPR_CMP_EQ;
                    string_caller_twins(buf, n, 0, 2, true); /* computed before rejection */
                }
            }
        }
        const char bytes[] = {'a', '\xff', 0, 'b'};
        n = string_literal(buf, 0, bytes, 4);
        n = string_literal(buf, n, bytes, 4); buf[n++] = ops[k];
        string_caller_twins(buf, n, expected, 2, true);
    }
}
static void
test_hmac_id_independence(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    const uint8_t tags[] = {WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS};
    for (unsigned tag = 0; tag < sizeof(tags); tag++) for (int map = 0; map < 2;
            map++){
            int64_t expected_value = 0; uint32_t expected_rows = 0;
            for (uint32_t seeds = 0; seeds < 8; seeds += 7) {
                wl_col_session_t *sess = join_session();
                sess->intern = string_table(sess->memory_governor, seeds);
                int64_t message = wl_intern_put(sess->intern, "same message");
                int64_t key = wl_intern_put(sess->intern, "same key");
                CHECK(message == seeds && key == seeds + 1);
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0);
                uint8_t buf[32]; uint32_t n = compiled_const(buf, 0,
                        tag == 2 ? INT64_MIN : message);
                n = compiled_const(buf, n, tag == 1 ? INT64_MAX : key);
                buf[n++] = tags[tag]; wl_plan_expr_buffer_t expr = {buf, n};
                wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                     .filter_expr = expr, .project_count = 1,
                     .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                wl_columnar_expr_context_t ctx = {.intern = sess->intern};
                int64_t raw; wl_columnar_expr_status_t status;
                CHECK(wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &raw, &ctx,
                    &status) == 0 && status == WL_COLUMNAR_EXPR_OK);
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                finish(c, 0);
                col_rel_t *out = join_output(frame);
                CHECK(out->nrows == (uint32_t)(map || raw != 0));
                if (map) CHECK(out->columns[0][0] == raw);
                if (!seeds) {
                    expected_value = raw; expected_rows = out->nrows;
                }else CHECK(raw == expected_value &&
                        out->nrows == expected_rows);
                source_unpinned(source, source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                wl_intern_free(sess->intern); sess->intern = NULL; release(c);
                join_session_destroy(sess);
            }
        }
#endif
}
static void
test_hmac_full_bytes(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    /* RFC2104 data construction and independent Python hashlib/hmac vectors
     * are fixed fixture oracles, separate from actual one-shot PSA raw twins. */
    const char *messages[] = {"Hi There",
                              "Test Using Larger Than Block-Size Key - Hash Key First"};
    const size_t lengths[] = {20, 131}; const uint8_t fill[] = {0x0b, 0xaa};
    static const uint8_t expected_mac[2][32] = {
        {0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce,
         0xaf, 0x0b, 0xf1, 0x2b, 0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7,
         0x26, 0xe9, 0x37, 0x6c, 0x2e, 0x32, 0xcf, 0xf7},
        {0x60, 0xe4, 0x31, 0x59, 0x1e, 0xe0, 0xb6, 0x7f, 0x0d, 0x8a, 0x26, 0xaa,
         0xcb, 0xf5, 0xb7, 0x7f, 0x8e, 0x0b, 0xc6, 0x21, 0x37, 0x28, 0xc5, 0x14,
         0x05, 0x46, 0x04, 0x0f, 0x0e, 0xe3, 0x7f, 0x54}
    };
    for (unsigned j = 0; j < 2; j++) {
        char key[132]; memset(key, fill[j], lengths[j]); key[lengths[j]] = 0;
        hmac_expected_key_length = lengths[j];
        if (j) {
            size_t out;
            CHECK(psa_hash_compute(PSA_ALG_SHA_256, (const uint8_t *)key,
                lengths[j], hmac_expected_key, 32,
                &out) == PSA_SUCCESS && out == 32);
            hmac_expected_key_length = 32;
        } else memcpy(hmac_expected_key, key, lengths[j]);
        memcpy(hmac_expected_mac, expected_mac[j], 32);
        hmac_key_seen = hmac_mac_seen = 0;
        wl_columnar_expr_hmac_test_normalized_key = hmac_normalized_observe;
        wl_columnar_expr_hmac_test_mac = hmac_mac_observe;
        uint8_t buf[256];
        uint32_t n = string_literal(buf, 0, messages[j],
                (uint16_t)strlen(messages[j]));
        n = string_literal(buf, n, key, (uint16_t)lengths[j]);
        buf[n++] = WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS;
        string_caller_twins(buf, n, 0, 2, true);
        CHECK(hmac_key_seen && hmac_mac_seen); hmac_hooks_clear();
    }
#endif
}
static void
hmac_lifecycle_trial(int map, int stop, int primary, int override,
    bool abort_error, bool destroy_error, bool length_error)
{
    wl_col_session_t *sess = join_session();
    sess->intern = string_table(sess->memory_governor, 0);
    char body[601]; memset(body, 'm', 600); body[600] = 0;
    CHECK(wl_intern_put(sess->intern, body) == 0);
    memset(body, 'k', 600); CHECK(wl_intern_put(sess->intern, body) == 1);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(sess->memory_governor);
    uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, 0);
    n = compiled_const(buf, n, 1); buf[n++] = WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS;
    wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER, .filter_expr = expr,
         .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(primary < 0 ? UINT64_MAX-1 : 0);
    memset(hmac_calls, 0, sizeof(hmac_calls));
    memset(digest_calls, 0, sizeof(digest_calls)); hmac_reached = 0;
    hmac_stage = stop; hmac_primary = primary; hmac_override = override;
    hmac_abort_error = abort_error; hmac_destroy_error = destroy_error;
    hmac_length_error = length_error;
    wl_columnar_expr_hmac_test_provider_result = hmac_provider_report;
    digest_override_stage = -1;
    digest_abort_error = digest_length_error = false;
    wl_columnar_expr_digest_test_provider_result = digest_provider_report;
    if (stop == 14 ||
        stop == 3) wl_columnar_expr_test_controlled_checkpoint = hmac_stop;
    else if (stop ==
        28) wl_columnar_expr_hash_test_controlled_checkpoint = hmac_stop;
    else if (stop == 31 || stop == 32 || stop == 33 ||
        stop == 66) wl_columnar_expr_digest_test_checkpoint = hmac_stop;
    else if (stop) wl_columnar_expr_hmac_test_checkpoint = hmac_stop;
    int expected = 0;
#ifdef WL_MBEDTLS_ENABLED
    if (override >= 0 || length_error) expected = map ? ERANGE : 0;
#else
    expected = map ? ERANGE : 0;
#endif
    if (stop) expected = primary > 0 ? EOVERFLOW : primary <
            0 ? WL_EVALUATION_CONTROL_WORK_LIMIT :
            WL_EVALUATION_CONTROL_CANCELLED;
#ifdef WL_MBEDTLS_ENABLED
    bool mac_abort = stop == 74 || stop == 75 || override == 2 || override == 3;
    bool imported = !(stop == 14 || stop == 70 || stop == 71 || stop == 79 ||
        stop == 80 || stop == 72 || stop == 31 || stop == 32 || stop == 33 ||
        stop == 66 || override == 0);
    if ((abort_error && mac_abort) || (destroy_error && imported)) {
        if (!(stop && primary > 0)) expected = EIO;
    }
#endif
    int rc = compiled_dispatch(sess, frame, ops, 2, c);
    if (rc != expected) fprintf(stderr,
            "hmac map%d stop%d primary%d override%d abort%d destroy%d rc%d expected%d\n",
            map, stop, primary, override, abort_error, destroy_error, rc,
            expected);
    CHECK(rc == expected); finish(c, expected);
    CHECK(!sess->memory_budget_denied);
    if (stop) CHECK(hmac_reached == 1);
#ifdef WL_MBEDTLS_ENABLED
    CHECK(hmac_calls[6] == (uint32_t)imported &&
        hmac_calls[5] == (uint32_t)mac_abort);
    if (stop == 14 || stop == 70) CHECK(!digest_calls[0] && !hmac_calls[0]);
    if (stop == 31 || stop == 32) CHECK(digest_calls[4] == 1 && !hmac_calls[0]);
    if (stop == 73) CHECK(hmac_calls[1] == 1 && !hmac_calls[2] &&
            hmac_calls[6] == 1);
    if (stop == 76 || stop == 77 || stop == 78 || stop == 28 || stop == 3 ||
        override == 4 || length_error)
        CHECK(hmac_calls[4] == 1 && !hmac_calls[5] && hmac_calls[6] == 1);
#else
    for (unsigned j = 0; j < 7; j++) CHECK(!hmac_calls[j]);
    for (unsigned j = 0; j < 5; j++) CHECK(!digest_calls[j]);
#endif
    if (expected) lazy_unchanged(source);
    source_unpinned(source, source);
#ifdef WL_MBEDTLS_ENABLED
    psa_key_id_t other_key = 0; uint8_t other_before[32], other_after[32];
    const uint8_t other_bytes[8] = {'o', 't', 'h', 'e', 'r', 'k', 'e', 'y'};
    if (destroy_error) {
        psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
        psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
        psa_set_key_usage_flags(&attributes, PSA_KEY_USAGE_SIGN_MESSAGE);
        psa_set_key_algorithm(&attributes, PSA_ALG_HMAC(PSA_ALG_SHA_256));
        CHECK(psa_import_key(&attributes, other_bytes, 8,
            &other_key) == PSA_SUCCESS);
        psa_reset_key_attributes(&attributes);
        size_t length;
        CHECK(psa_mac_compute(other_key, PSA_ALG_HMAC(PSA_ALG_SHA_256),
            other_bytes, 8,
            other_before, 32, &length) == PSA_SUCCESS && length == 32);
    }
#endif
    CHECK(wl_intern_count(sess->intern) == 2);
    for (unsigned j = 0; j < 600; j++) {
        CHECK(wl_intern_reverse(sess->intern,
            0)[j] == 'm' && wl_intern_reverse(sess->intern, 1)[j] == 'k');
    }
    CHECK(!wl_intern_reverse(sess->intern,
        0)[600] && !wl_intern_reverse(sess->intern, 1)[600]);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
        string_reserved(sess->memory_governor) == before);
    hmac_hooks_clear(); frame = filter_frame(sess); retry(c, 0);
#ifdef WL_MBEDTLS_ENABLED
    expected = 0;
#else
    expected = map ? ERANGE : 0;
#endif
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
    finish(c, expected);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
        string_reserved(sess->memory_governor) == before);
#ifdef WL_MBEDTLS_ENABLED
    if (destroy_error) {
        size_t length;
        CHECK(psa_mac_compute(other_key, PSA_ALG_HMAC(PSA_ALG_SHA_256),
            other_bytes, 8,
            other_after, 32,
            &length) == PSA_SUCCESS && length == 32 && memcmp(other_before,
            other_after, 32) == 0);
        CHECK(psa_destroy_key(other_key) == PSA_SUCCESS);
    }
#endif
    wl_intern_free(sess->intern); sess->intern = NULL; release(c);
    join_session_destroy(sess);
}
static void
test_hmac_lifecycle(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    !defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    return; /* Genuine optional configured-false is tested separately. */
#endif
    for (int map = 0; map < 2; map++) {
#ifdef WL_MBEDTLS_ENABLED
        const int stages[] = {14, 70, 31, 32, 33, 66, 71, 79, 80, 72, 73, 74,
                              75, 76, 77, 28, 78, 3};
        for (unsigned j = 0; j < sizeof(stages)/sizeof(*stages);
            j++) for (int positive = 0;
                positive < 2; positive++)
                hmac_lifecycle_trial(map, stages[j], positive, -1, false, false,
                    false);
        for (int stage = 0; stage <= 4;
            stage++) hmac_lifecycle_trial(map, 0, 0, stage, false, false,
                false);
        hmac_lifecycle_trial(map, 0, 0, -1, false, false, true);
        hmac_lifecycle_trial(map, 0, 0, 3, true, true, false);
        for (int primary = -1; primary < 2; primary++) {
            hmac_lifecycle_trial(map, 74, primary, -1, true, true, false);
            hmac_lifecycle_trial(map, 73, primary, -1, false, true, false);
        }
        hmac_lifecycle_trial(map, 0, 0, -1, false, true, false);
#else
        for (int positive = 0; positive < 2;
            positive++) hmac_lifecycle_trial(map, 3, positive, -1, false, false,
                false);
        hmac_lifecycle_trial(map, 0, 0, -1, false, false, false);
#endif
    }
}
static bool hmac_refuse(void) {
    return false;
}
static unsigned hmac_scan_visits;
static void
hmac_refusal_scan(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)c; (void)progress; (void)failed;
    if (stage == 14) hmac_scan_visits++;
}
static void
test_hmac_zero_and_early_refusal(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    !defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    return; /* Non-hook configured-false matrix is separate. */
#endif
    const uint8_t tags[] = {WL_PLAN_EXPR_ARITH_HMAC_SHA256,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS};
    for (int map = 0; map < 2; map++) for (unsigned k = 0; k < sizeof(tags);
            k++) for (int refuse = 0; refuse < 2; refuse++) for (int rows = 0;
                    rows <= refuse; rows++){
#ifndef WL_MBEDTLS_ENABLED
                    if (refuse) continue; /* disabled pending semantics, not optional-profile ENOTSUP */
#endif
                    wl_col_session_t *sess = join_session();
                    sess->intern = string_table(sess->memory_governor, 1);
                    col_rel_t *source = relation(1, rows, rows, false,
                            sess->memory_governor);
                    CHECK(session_add_rel(sess, source) == 0);
                    make_lazy(source);
                    uint64_t before = string_reserved(sess->memory_governor);
                    uint8_t buf[128];
                    uint32_t n = string_literal(buf, 0, "must-not-intern", 15);
                    n = compiled_const(buf, n, 0); buf[n++] = tags[k];
                    wl_plan_expr_buffer_t expr = {buf, n};
                    wl_plan_op_t ops[2] = {
                        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                         .delta_mode = WL_DELTA_FORCE_FULL},
                        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                         .filter_expr = expr, .project_count = 1,
                         .map_exprs = &expr, .map_expr_count = 1}
                    };
                    memset(hmac_calls, 0, sizeof(hmac_calls));
                    memset(digest_calls, 0, sizeof(digest_calls));
                    wl_columnar_expr_hmac_test_provider_result =
                        hmac_provider_report;
                    wl_columnar_expr_digest_test_provider_result =
                        digest_provider_report;
                    if (refuse) wl_columnar_expr_hmac_test_eligible =
                            hmac_refuse;
                    hmac_scan_visits = 0;
                    wl_columnar_expr_test_controlled_checkpoint =
                        hmac_refusal_scan;
                    wl_columnar_eval_stack_cleanup_frame_t *frame =
                        filter_frame(sess);
                    wl_evaluation_control_t *c = control(0);
                    int expected = refuse ? ENOTSUP : 0;
                    CHECK(compiled_dispatch(sess, frame, ops, 2,
                        c) == expected); finish(c, expected);
                    for (unsigned j = 0; j < 7; j++) CHECK(!hmac_calls[j]);
                    for (unsigned j = 0; j < 5; j++) CHECK(!digest_calls[j]);
                    CHECK(!hmac_scan_visits);
                    CHECK(wl_intern_count(sess->intern) == 1 &&
                        strcmp(wl_intern_reverse(sess->intern, 0),
                        "seed-0") == 0);
                    if (refuse) lazy_unchanged(source);
                    source_unpinned(source, source);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                        string_reserved(sess->memory_governor) == before);
                    hmac_hooks_clear(); wl_intern_free(sess->intern);
                    sess->intern = NULL; release(c); join_session_destroy(sess);
                }
}
static void
test_hmac_owned_cleanup(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        CHECK(wl_intern_put(sess->intern, "message") == 0);
        CHECK(wl_intern_put(sess->intern, "key") == 1);
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        uint8_t buf[64]; uint32_t n = compiled_const(buf, 0, 0);
        n = compiled_const(buf, n, 1);
        buf[n++] = WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS;
        if (!map) {
            buf[n++] = WL_PLAN_EXPR_BOOL; buf[n++] = 1;
        }
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        memset(hmac_calls, 0, sizeof(hmac_calls));
        wl_columnar_expr_hmac_test_provider_result = hmac_provider_report;
        filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame),
            sess) : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(filter_reached == 1 && hmac_calls[1] >= 1 &&
            hmac_calls[1] == hmac_calls[6] && !hmac_calls[5] &&
            wl_intern_count(sess->intern) == 2);
        hmac_hooks_clear();
        CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL; release(c);
        join_session_destroy(sess);
    }
#endif
}

static void
hmac_required_startup(void)
{
    const char *required = getenv("WIRELOG_REQUIRE_HMAC_QUALIFICATION");
    if (!required || !*required) return;
    if (strcmp(required, "1") != 0) {
        fputs("invalid WIRELOG_REQUIRE_HMAC_QUALIFICATION: expected 1\n",
            stderr);
        exit(1);
    }
#if defined(WL_MBEDTLS_ENABLED) && \
    defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_expr_hmac_eligible(c) == 0); finish(c, 0); release(c);
#else
    fputs(
        "required HMAC qualification unavailable: enabled audited software profile required\n",
        stderr);
    exit(1);
#endif
}
static void
test_hmac_configured_false_raw(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    !defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    const uint8_t tags[] = {WL_PLAN_EXPR_ARITH_HMAC_SHA256,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS};
    CHECK(psa_crypto_init() == PSA_SUCCESS);
    const size_t lengths[] = {65, 513};
    for (unsigned j = 0; j < sizeof(lengths)/sizeof(*lengths); j++) {
        char message[514], key[514]; memset(message, 'm', lengths[j]);
        memset(key, 'k', lengths[j]); message[lengths[j]] = key[lengths[j]] = 0;
        wl_intern_t *table = string_table(NULL, 0);
        CHECK(wl_intern_put(table, message) == 0 && wl_intern_put(table,
            key) == 1);
        for (unsigned k = 0; k < sizeof(tags); k++) for (int missing = 0;
                missing < 2; missing++){
                int64_t a = missing ? INT64_MIN : 0,
                    b = missing ? INT64_MAX : 1;
                const uint8_t *m = (const uint8_t *)&a,
                    *secret = (const uint8_t *)&b;
                size_t ml = 8, kl = 8;
                if (!missing && (k == 1 || k == 2)) {
                    m = (const uint8_t *)message; ml = lengths[j];
                }
                if (!missing && (k == 1 || k == 3)) {
                    secret = (const uint8_t *)key; kl = lengths[j];
                }
                psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
                psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
                psa_set_key_usage_flags(&attributes,
                    PSA_KEY_USAGE_SIGN_MESSAGE);
                psa_set_key_algorithm(&attributes,
                    PSA_ALG_HMAC(PSA_ALG_SHA_256));
                psa_key_id_t id;
                CHECK(psa_import_key(&attributes, secret, kl,
                    &id) == PSA_SUCCESS);
                psa_reset_key_attributes(&attributes); uint8_t mac[32];
                size_t out;
                CHECK(psa_mac_compute(id, PSA_ALG_HMAC(PSA_ALG_SHA_256), m, ml,
                    mac, 32, &out) == PSA_SUCCESS && out == 32);
                CHECK(psa_destroy_key(id) == PSA_SUCCESS);
                uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, a);
                n = compiled_const(buf, n, b); buf[n++] = tags[k];
                wl_columnar_expr_context_t ctx = {.intern = table};
                int64_t value; wl_columnar_expr_status_t status;
                CHECK(wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &value,
                    &ctx,
                    &status) == 0 && status == WL_COLUMNAR_EXPR_OK &&
                    value == (int64_t)XXH3_64bits(mac, 32));
            }
        wl_intern_free(table);
    }
    /* Independent known FULL32 vector, separate from one-shot raw parity. */
    const uint8_t expected[32] = {0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38,
                                  0x53,
                                  0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b, 0xf1,
                                  0x2b, 0x88, 0x1d, 0xc2, 0x00,
                                  0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37,
                                  0x6c, 0x2e, 0x32, 0xcf, 0xf7};
    char key[21]; memset(key, 11, 20); key[20] = 0;
    wl_intern_t *table = string_table(NULL, 0);
    CHECK(wl_intern_put(table, "Hi There") == 0 && wl_intern_put(table,
        key) == 1);
    uint8_t buf[32]; uint32_t n = compiled_const(buf, 0, 0);
    n = compiled_const(buf, n, 1); buf[n++] = WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS;
    wl_columnar_expr_context_t ctx = {.intern = table};
    int64_t value; wl_columnar_expr_status_t status;
    CHECK(wl_columnar_expr_eval_run_ctx(buf, n, NULL, 0, &value, &ctx,
        &status) == 0 && status == WL_COLUMNAR_EXPR_OK &&
        value == (int64_t)XXH3_64bits(expected, 32));
    wl_intern_free(table);
#endif
}
static void
test_hmac_configured_false_refusal(void)
{
#if defined(WL_MBEDTLS_ENABLED) && \
    !defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    const uint8_t tags[] = {WL_PLAN_EXPR_ARITH_HMAC_SHA256,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
                            WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS};
    for (unsigned k = 0; k < sizeof(tags); k++) for (int map = 0; map < 2;
            map++)
            for (int rows = 0; rows < 2; rows++) {
                wl_col_session_t *sess = join_session();
                sess->intern = string_table(sess->memory_governor, 1);
                char body[514]; memset(body, 'm', 513); body[513] = 0;
                CHECK(wl_intern_put(sess->intern, body) == 1);
                memset(body, 'k', 513);
                CHECK(wl_intern_put(sess->intern, body) == 2);
                col_rel_t *source = relation(1, rows, rows, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t before = string_reserved(sess->memory_governor);
                uint8_t buf[128];
                uint32_t n = string_literal(buf, 0, "must-not-intern", 15);
                n = compiled_const(buf, n, 1); n = compiled_const(buf, n, 2);
                buf[n++] = tags[k];
                wl_plan_expr_buffer_t expr = {buf, n}; wl_plan_op_t ops[2] = {
                    {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                     .delta_mode = WL_DELTA_FORCE_FULL},
                    {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                     .filter_expr = expr,
                     .project_count = 1, .map_exprs = &expr,
                     .map_expr_count = 1}
                };
                hmac_hooks_clear(); hmac_scan_visits = 0;
                memset(hmac_calls, 0, sizeof(hmac_calls));
                memset(digest_calls, 0, sizeof(digest_calls));
                wl_columnar_expr_hmac_test_provider_result =
                    hmac_provider_report;
                wl_columnar_expr_digest_test_provider_result =
                    digest_provider_report;
                wl_columnar_expr_test_controlled_checkpoint = hmac_refusal_scan;
                CHECK(!wl_columnar_expr_hmac_test_eligible);
                wl_evaluation_control_t *c = control(0);
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == ENOTSUP);
                finish(c, ENOTSUP);
                CHECK(!hmac_scan_visits && !sess->memory_budget_denied);
                for (unsigned j = 0; j < 7; j++) CHECK(!hmac_calls[j]);
                for (unsigned j = 0; j < 5; j++) CHECK(!digest_calls[j]);
                CHECK(wl_intern_count(sess->intern) == 3 &&
                    !strcmp(wl_intern_reverse(sess->intern, 0), "seed-0"));
                CHECK(strlen(wl_intern_reverse(sess->intern,
                    1)) == 513 && strlen(wl_intern_reverse(sess->intern,
                    2)) == 513);
                lazy_unchanged(source); source_unpinned(source, source);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                    string_reserved(sess->memory_governor) == before);
                hmac_hooks_clear(); wl_intern_free(sess->intern);
                sess->intern = NULL;
                release(c); join_session_destroy(sess);
            }
    /* Preflight refusal leaves actual owned input; a real retained reader makes
     * cleanup EBUSY, then release/drain restores ownership, without fake output. */
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        CHECK(col_rel_source_reader_acquire(source,
            &join_retained_reader) == 0);
        uint8_t tag = WL_PLAN_EXPR_ARITH_HMAC_SHA256;
        wl_plan_expr_buffer_t expr = {&tag, 1};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)
            : wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess)) == ENOTSUP);
        sess->base.evaluation_control = NULL; finish(c, ENOTSUP);
        CHECK(wl_columnar_eval_stack_cleanup_stack(frame)->items[0].rel ==
            source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        release(c); join_session_destroy(sess);
    }
#endif
}

/* Actual extension lookup, callback effects and operator result channels. */
extern void (*wl_extension_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, size_t);
static wirelog_extension_registry_t *extension_registry;
static wirelog_extension_snapshot_t *extension_snapshot;
static char extension_registered_names[520][256];
static unsigned extension_registered_count;
static bool extension_unregistered;
static unsigned extension_calls, extension_destroyed, extension_mode;
static wl_evaluation_control_t *extension_callback_control;
static bool extension_positive;
static uint32_t extension_expected_args, extension_declared_result;
static unsigned extension_zero_lookup_visits;
static const uint8_t *extension_string;
static size_t extension_string_size;
static wl_col_session_t *extension_nested_session;
static const uint8_t *extension_nested_buf;
static uint32_t extension_nested_size;
static wl_columnar_expr_status_t extension_nested_status;
static bool extension_destructor_controlled;
static int extension_session_keys[65];
static unsigned extension_depth, extension_tls_visits;

static uint32_t
extension_encode(uint8_t *buf, uint32_t pos, const void *name,
    uint16_t length, uint32_t nargs, bool abi)
{
    buf[pos++] = abi ? WL_PLAN_EXPR_EXTENSION_CALL_ABI :
        WL_PLAN_EXPR_EXTENSION_CALL;
    buf[pos++] = (uint8_t)length; buf[pos++] = (uint8_t)(length >> 8);
    if (length) memcpy(buf + pos, name, length);
    pos += length;
    for (unsigned j = 0; j < 4; j++) buf[pos++] = (uint8_t)(nargs >> (j * 8));
    if (abi) {
        for (unsigned j = 0; j < 8;
            j++) buf[pos++] = (uint8_t)(UINT64_C(0x1020304050607080) >>
                (j * 8));
        for (unsigned j = 0; j < 4; j++) buf[pos++] = (uint8_t)(7u >> (j * 8));
    }
    return pos;
}
static void
extension_nested_eval(bool controlled)
{
    int64_t value = INT64_MIN; bool failed = false;
    extension_nested_status = WL_COLUMNAR_EXPR_OK;
    wl_evaluation_control_t *c = controlled ? control(0) : NULL;
    CHECK(wl_columnar_expr_eval_interpreter_controlled(extension_nested_buf,
        extension_nested_size, NULL, 0, &value, &failed,
        extension_nested_session, &extension_nested_status, false, true,
        c) == 0);
    CHECK(failed);
    if (c) {
        finish(c, 0); release(c);
    }
}
static void
extension_destroy(void *data)
{
    (void)data;
    extension_destroyed++;
    if (extension_nested_session) extension_nested_eval(
            extension_destructor_controlled);
}
static int
extension_invoke(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *data)
{
    (void)data;
    extension_calls++;
    CHECK(nargs == extension_expected_args);
    if (extension_string) {
        CHECK(nargs == 1 && args[0].type == WIRELOG_EXTENSION_VALUE_STRING &&
            args[0].size == extension_string_size &&
            args[0].as.string_value.length == extension_string_size &&
            memcmp(args[0].as.string_value.data, extension_string,
            extension_string_size) == 0);
    }
    if (extension_mode == 4 && extension_snapshot) {
        CHECK(wirelog_extension_unregister(extension_registry, "ext") == 0);
        extension_unregistered = true;
        wirelog_extension_snapshot_t *held = extension_snapshot;
        extension_snapshot = NULL;
        wirelog_extension_snapshot_release(held);
        CHECK(!extension_destroyed);
    }
    if (extension_mode == 5) {
        extension_depth++;
        const void *old_key = extension_nested_session->callback_session_key;
        extension_nested_session->callback_session_key =
            &extension_session_keys[extension_depth];
        int64_t value; bool failed; wl_columnar_expr_status_t status;
        wl_evaluation_control_t *c = control(0);
        CHECK(wl_columnar_expr_eval_interpreter_controlled(extension_nested_buf,
            extension_nested_size, NULL, 0, &value, &failed,
            extension_nested_session, &status, false, true, c) == 0);
        finish(c, 0); release(c);
        if (extension_depth == 64) CHECK(failed &&
                status == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT);
        else CHECK(!failed && value == 1);
        extension_nested_session->callback_session_key = old_key;
        extension_depth--;
    }
    if (extension_mode == 6 && extension_depth < 2) {
        extension_depth++;
        int64_t value; bool failed; wl_columnar_expr_status_t status;
        wl_evaluation_control_t *nested_control =
            extension_destructor_controlled ? control(0) : NULL;
        CHECK(wl_columnar_expr_eval_interpreter_controlled(extension_nested_buf,
            extension_nested_size, NULL, 0, &value, &failed,
            extension_nested_session,
            &status, false, true,
            nested_control) == 0 && !failed && value == 1);
        if (nested_control) {
            finish(nested_control, 0); release(nested_control);
        }
        extension_depth--;
    }
    if (extension_mode == 7) {
        extension_nested_eval(extension_destructor_controlled);
        CHECK(extension_nested_status == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT);
    }
    if (extension_mode == 8) {
        wl_plan_stratum_t nested_stratum = {0};
        CHECK(col_eval_stratum(&nested_stratum, extension_nested_session,
            0) == EBUSY);
    }
    if (extension_callback_control) {
        if (extension_positive) CHECK(wl_evaluation_control_charge(
                    extension_callback_control, UINT64_MAX) == EOVERFLOW);
        else wl_evaluation_control_request_cancel(extension_callback_control);
    }
    result->type = extension_mode == 2 ? WIRELOG_EXTENSION_VALUE_STRING :
        extension_mode == 3 ? WIRELOG_EXTENSION_VALUE_INT64 :
        WIRELOG_EXTENSION_VALUE_BOOL;
    result->size = extension_mode == 3 ? sizeof(int64_t) : sizeof(uint8_t);
    if (extension_mode == 3) result->as.int64_value = -19;
    else result->as.bool_value = 1;
    return extension_mode == 1 ? -777 : 0;
}
static void
extension_register(const char *name, uint32_t arity, uint32_t type,
    const uint32_t *arguments, uint32_t policy, bool destructor)
{
    /* Public registration requires a valid type for every actual argument;
    * the evaluator's defensive zero-wildcard branch is not registrable. */
    uint32_t integer_types[COL_FILTER_STACK];
    CHECK(arity <= COL_FILTER_STACK);
    if (arity && !arguments) {
        for (uint32_t j = 0; j < arity;
            j++) integer_types[j] = WIRELOG_EXTENSION_VALUE_INT64;
        arguments = integer_types;
    }
    wirelog_extension_descriptor_t descriptor = {
        .abi_version = WIRELOG_EXTENSION_ABI_VERSION,
        .size = sizeof(descriptor),
        .name = name, .arity = arity, .argument_types = arguments,
        .result_type = type, .invoke = extension_invoke,
        .destroy = destructor ? extension_destroy : NULL,
        .callback_policy = policy,
        .addon_abi_identity = UINT64_C(0x1020304050607080),
        .addon_abi_version = 7
    };
    CHECK(wirelog_extension_register(extension_registry, &descriptor) == 0);
    CHECK(extension_registered_count < 520);
    strcpy(extension_registered_names[extension_registered_count++], name);
}
static void
extension_setup(const char *name, uint32_t arity, uint32_t type,
    const uint32_t *arguments, uint32_t policy, bool destructor)
{
    extension_calls = extension_destroyed = extension_mode = 0;
    extension_registered_count = 0; extension_unregistered = false;
    extension_expected_args = arity; extension_declared_result = type;
    extension_callback_control = NULL;
    extension_string = NULL; extension_nested_session = NULL;
    extension_registry = wirelog_extension_registry_create();
    CHECK(extension_registry);
    extension_register(name, arity, type, arguments, policy, destructor);
    extension_snapshot = wirelog_extension_snapshot_acquire(extension_registry);
    CHECK(extension_snapshot);
}
static void
extension_teardown(void)
{
    if (extension_snapshot) wirelog_extension_snapshot_release(
            extension_snapshot);
    extension_snapshot = NULL;
    for (unsigned i = 0; i < extension_registered_count; i++) {
        if (extension_unregistered && strcmp(extension_registered_names[i],
            "ext") == 0) continue;
        CHECK(wirelog_extension_unregister(extension_registry,
            extension_registered_names[i]) == 0);
    }
    CHECK(wirelog_extension_registry_destroy(extension_registry) == 0);
    extension_registry = NULL;
}
static void
extension_zero_lookup(wl_evaluation_control_t *c, int stage, size_t progress)
{
    (void)c; (void)stage; (void)progress;
    extension_zero_lookup_visits++;
}
static void
extension_caller_twins(uint8_t *buf, uint32_t size, uint32_t rows,
    int filter_error, int map_error, bool exact)
{
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        sess->base.extension_snapshot = raw->base.extension_snapshot =
            extension_snapshot;
        sess->callback_session_key = sess; raw->callback_session_key = raw;
        sess->intern = string_table(sess->memory_governor, 0);
        raw->intern = string_table(raw->memory_governor, 0);
        col_rel_t *source = relation(1, rows, rows, false,
                sess->memory_governor);
        col_rel_t *ref = relation(1, rows, rows, false, raw->memory_governor);
        CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
            ref) == 0);
        wl_plan_expr_buffer_t expr = {buf, size};
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
             .filter_expr = expr,
             .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
        };
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
            *reference = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        unsigned before = extension_calls;
        if (!rows) {
            extension_zero_lookup_visits = 0;
            wl_extension_test_controlled_checkpoint = extension_zero_lookup;
        }
        int expected = rows ? (map ? map_error : filter_error) : 0;
        if (extension_mode == 8) extension_nested_session = sess;
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
        uint64_t cost = finish(c, expected);
        if (!rows) {
            CHECK(!extension_zero_lookup_visits);
            wl_extension_test_controlled_checkpoint = NULL;
        }
        unsigned actual_calls = extension_calls - before;
        if (!map &&
            extension_declared_result ==
            WIRELOG_EXTENSION_VALUE_INT64) CHECK(!actual_calls);
        before = extension_calls;
        if (extension_mode == 8) extension_nested_session = raw;
        CHECK(compiled_dispatch(raw, reference, ops, 2, NULL) == expected);
        if (extension_mode == 8) extension_nested_session = NULL;
        CHECK(extension_calls - before == actual_calls);
        if (!rows) CHECK(!actual_calls);
        if (expected) CHECK(sess->extension_expr_status == expected &&
                raw->extension_expr_status == expected);
        else filter_equal(join_output(frame), join_output(reference));
        source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        if (exact && !expected) {
            wl_intern_free(sess->intern);
            sess->intern = string_table(sess->memory_governor, 0);
            frame = filter_frame(sess); retry(c, cost - 1);
            CHECK(compiled_dispatch(sess, frame, ops, 2,
                c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            wl_intern_free(sess->intern);
            sess->intern = string_table(sess->memory_governor, 0);
            frame = filter_frame(sess); retry(c, cost);
            CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0 && finish(c,
                0) == cost);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        wl_intern_free(sess->intern); wl_intern_free(raw->intern);
        sess->intern = raw->intern = NULL;
        release(c); join_session_destroy(sess); join_session_destroy(raw);
    }
}
static void
test_extension_actual_twins(void)
{
    for (int abi = 0; abi < 2; abi++) {
        uint8_t buf[6000];
        extension_setup("ext", 1, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
        uint32_t n = compiled_var(buf, 0, "col0");
        n = extension_encode(buf, n, "ext", 3, 1, abi);
        extension_caller_twins(buf, n, 3, 0, 0, true);
        extension_caller_twins(buf, n, 0, 0, 0, false);
        for (unsigned mode = 1; mode <= 3; mode++) {
            extension_mode = mode;
            extension_caller_twins(buf, n, 1,
                mode ==
                1 ? WL_COLUMNAR_EXPR_CALLBACK_FAILURE :
                WL_COLUMNAR_EXPR_INVALID_RESULT,
                mode == 1 ? WL_COLUMNAR_EXPR_CALLBACK_FAILURE : mode ==
                2 ? WL_COLUMNAR_EXPR_INVALID_RESULT : 0, false);
        }
        extension_mode = 0;
        if (abi) {
            buf[n-1] ^= 1;
            extension_caller_twins(buf, n, 1,
                WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH,
                WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH, false); buf[n-1] ^= 1;
            extension_caller_twins(buf, n-1, 1,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED, false);
        }
        n = compiled_const(buf, 0, 3);
        const char serialized[] = {' ', '\t', '\n', '\r', 'e', 'x', 't', ' ',
                                   '\t', '\r', '\n', 0, 'z'};
        n = extension_encode(buf, n, serialized, sizeof(serialized), 1, abi);
        extension_caller_twins(buf, n, 1, 0, 0, false);
        n = extension_encode(buf, 0, "ext", 3, 1, abi);
        extension_caller_twins(buf, n, 1, WL_COLUMNAR_EXPR_ARITY_MISMATCH,
            WL_COLUMNAR_EXPR_ARITY_MISMATCH, false);
        n = extension_encode(buf, 0, "missing", 7, 0, abi);
        extension_caller_twins(buf, n, 1, WL_COLUMNAR_EXPR_MISSING_EXTENSION,
            WL_COLUMNAR_EXPR_MISSING_EXTENSION, false);
        for (unsigned length = 0; length <= 256; length += 256) {
            char name[256]; memset(name, 'n', sizeof(name));
            n = extension_encode(buf, 0, name, (uint16_t)length, 0, abi);
            extension_caller_twins(buf, n, 1,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED,
                WL_COLUMNAR_EXPR_EXTENSION_MALFORMED, false);
        }
        n = extension_encode(buf, 0, "e", 1, 0, abi);
        extension_caller_twins(buf, n, 1, WL_COLUMNAR_EXPR_MISSING_EXTENSION,
            WL_COLUMNAR_EXPR_MISSING_EXTENSION, false);
        n = extension_encode(buf, 0, "ext", 3, 1, false);
        extension_caller_twins(buf, n-1, 1,
            WL_COLUMNAR_EXPR_EXTENSION_MALFORMED,
            WL_COLUMNAR_EXPR_EXTENSION_MALFORMED, false);
        extension_teardown();
        uint32_t required = WIRELOG_EXTENSION_VALUE_STRING;
        extension_setup("ext", 1, WIRELOG_EXTENSION_VALUE_BOOL, &required, 0,
            false);
        const uint8_t text[] = {'a', 0, 'b', 255};
        extension_string = text; extension_string_size = sizeof(text);
        n = string_literal(buf, 0, (const char *)text, sizeof(text));
        n = extension_encode(buf, n, "ext", 3, 1, abi);
        extension_caller_twins(buf, n, 1, 0, 0, true);
        extension_string = NULL;
        n = compiled_const(buf, 0, 4);
        n = extension_encode(buf, n, "ext", 3, 1, abi);
        extension_caller_twins(buf, n, 1, WL_COLUMNAR_EXPR_TYPE_MISMATCH,
            WL_COLUMNAR_EXPR_TYPE_MISMATCH, false);
        extension_teardown();
        required = WIRELOG_EXTENSION_VALUE_BOOL;
        extension_setup("ext", 1, WIRELOG_EXTENSION_VALUE_BOOL, &required, 0,
            false);
        buf[0] = WL_PLAN_EXPR_BOOL; buf[1] = 1;
        n = extension_encode(buf, 2, "ext", 3, 1, abi);
        extension_caller_twins(buf, n, 1, 0, 0, false); extension_teardown();
        extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_INT64, NULL, 0,
            false);
        extension_mode = 3; n = extension_encode(buf, 0, "ext", 3, 0, abi);
        extension_caller_twins(buf, n, 1, WL_COLUMNAR_EXPR_INVALID_RESULT, 0,
            false); extension_teardown();
        extension_setup("ext", COL_FILTER_STACK, WIRELOG_EXTENSION_VALUE_BOOL,
            NULL, 0, false);
        n = 0;
        for (unsigned j = 0; j < COL_FILTER_STACK;
            j++) n = compiled_const(buf, n, j);
        n = extension_encode(buf, n, "ext", 3, COL_FILTER_STACK, abi);
        extension_caller_twins(buf, n, 1, 0, 0, true); extension_teardown();
    }
}

static int extension_stop_stage;
static size_t extension_stop_progress;
static unsigned extension_stop_reached;
static void
extension_registry_stop(wl_evaluation_control_t *c, int stage, size_t progress)
{
    if (stage != extension_stop_stage ||
        progress != extension_stop_progress) return;
    extension_stop_reached++;
    wl_extension_test_controlled_checkpoint = NULL;
    if (extension_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
extension_expr_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)failed;
    if (stage == 85) extension_tls_visits++;
    if (stage != extension_stop_stage ||
        progress != extension_stop_progress) return;
    extension_stop_reached++;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    if (extension_positive) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
extension_stop_trial(uint8_t *buf, uint32_t size, int map, int stage,
    size_t progress, bool positive, unsigned mode)
{
    wl_col_session_t *sess = join_session();
    sess->base.extension_snapshot = extension_snapshot;
    sess->callback_session_key = sess;
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t before = string_reserved(sess->memory_governor);
    wl_plan_expr_buffer_t expr = {buf, size};
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER, .filter_expr = expr,
         .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
    };
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    unsigned calls = extension_calls;
    extension_mode = mode;
    extension_stop_stage = stage; extension_stop_progress = progress;
    extension_stop_reached = 0; extension_positive = positive;
    if (stage >= 81 &&
        stage <=
        84) wl_extension_test_controlled_checkpoint = extension_registry_stop;
    else if (stage) wl_columnar_expr_test_controlled_checkpoint =
            extension_expr_stop;
    else {
        CHECK(wl_evaluation_control_retain(c) == 0);
        extension_callback_control = c;
    }
    int expected = positive ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
    finish(c, expected);
    CHECK(!sess->memory_budget_denied && !sess->extension_expr_status);
    if (stage) CHECK(extension_stop_reached == 1);
    CHECK(extension_calls - calls == (unsigned)(stage == 0 || stage == 88 ||
        stage == 3));
    lazy_unchanged(source); source_unpinned(source, source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
        string_reserved(sess->memory_governor) == before);
    wl_extension_test_controlled_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    if (!stage) wl_evaluation_control_release(extension_callback_control);
    extension_callback_control = NULL; extension_mode = 0;
    frame = filter_frame(sess); retry(c, 0); calls = extension_calls;
    CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
    CHECK(extension_calls == calls + 1);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
        string_reserved(sess->memory_governor) == before);
    release(c); join_session_destroy(sess);
}
static void
test_extension_registry_and_callback_stops(void)
{
    uint8_t buf[6000]; char name[256]; memset(name, 'n', 255); name[255] = 0;
    extension_setup(name, COL_FILTER_STACK, WIRELOG_EXTENSION_VALUE_BOOL, NULL,
        0, false);
    wirelog_extension_snapshot_release(extension_snapshot);
    for (unsigned i = 0; i < 513; i++) {
        char other[256]; memcpy(other, name, 256);
        snprintf(other + 248, 8, "%07u", i);
        extension_register(other, COL_FILTER_STACK,
            WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
    }
    extension_snapshot = wirelog_extension_snapshot_acquire(extension_registry);
    CHECK(extension_snapshot);
    uint32_t n = 0;
    for (unsigned i = 0; i < COL_FILTER_STACK;
        i++) n = compiled_const(buf, n, i);
    char last[256]; memcpy(last, name, 256);
    snprintf(last + 248, 8, "%07u", 512u);
    n = extension_encode(buf, n, last, 255, COL_FILTER_STACK, true);
    extension_caller_twins(buf, n, 1, 0, 0, true);
    for (int map = 0; map < 2; map++) for (int positive = 0; positive < 2;
            positive++){
            const int stages[] = {81, 82, 83, 84, 86, 87, 88, 3};
            const size_t progress[] = {128, 300, 128, 1, 32, COL_FILTER_STACK,
                                       COL_FILTER_STACK, COL_FILTER_STACK+1};
            for (unsigned i = 0; i < sizeof(stages)/sizeof(*stages); i++)
                extension_stop_trial(buf, n, map, stages[i], progress[i],
                    positive, 0);
            for (unsigned mode = 0; mode < 4;
                mode++) extension_stop_trial(buf, n, map, 0, 0, positive, mode);
        }
    last[254] = 'x';
    n = extension_encode(buf, 0, last, 255, 0, false);
    extension_caller_twins(buf, n, 1, WL_COLUMNAR_EXPR_MISSING_EXTENSION,
        WL_COLUMNAR_EXPR_MISSING_EXTENSION, false);
    extension_teardown(); /* Fails if any acquired lease leaked on a stop. */
}
static unsigned extension_quota_reached;
static void
extension_full_sink_quota(wl_evaluation_control_t *c, int stage,
    uint64_t progress)
{
    (void)c;
    if (stage != WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME || progress != 5) return;
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    extension_quota_reached++;
    /* $map descriptor/name are already admitted. Leave exactly one length
     * slot and real column schema metadata; the full row payload must fail. */
    uint64_t available = compiled_quota_governor->budget_bytes -
        wl_columnar_memory_reserved(compiled_quota_governor);
    uint64_t metadata_and_lengths = sizeof(uint64_t) + 3 + sizeof(char *) +
        sizeof(struct ArrowSchema *) + sizeof(struct ArrowSchema) + 2 + 10;
    CHECK(available >= metadata_and_lengths);
    CHECK(wl_columnar_memory_reserve_checked(compiled_quota_governor,
        available - metadata_and_lengths,
        &compiled_quota) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
}
static void
test_extension_initial_map_admission(void)
{
    extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
    uint8_t buf[32]; uint32_t n = extension_encode(buf, 0, "ext", 3, 0, false);
    for (int failure = 0; failure < 6; failure++) {
        wl_col_session_t *sess = join_session();
        sess->base.extension_snapshot = extension_snapshot;
        col_rel_t *source = relation(1, 2, 2, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = WL_PLAN_OP_MAP, .project_count = 1, .map_exprs = &expr,
             .map_expr_count = 1}
        };
        uint64_t before = string_reserved(sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        unsigned calls = extension_calls;
        compiled_quota_governor =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        wl_columnar_memory_reservation_init(&compiled_quota);
        if (!failure) CHECK(wl_columnar_memory_reserve_checked(
                    compiled_quota_governor,
                    compiled_quota_governor->budget_bytes -
                    string_reserved(sess->memory_governor),
                    &compiled_quota) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        if (failure ==
            1) wl_columnar_relation_test_constructor_fail_allocation = 0;
        if (failure == 2) wl_columnar_relation_test_constructor_fail_commit = 1;
        if (failure == 3) {
            extension_quota_reached = 0;
            wl_columnar_relation_test_constructor_checkpoint =
                extension_full_sink_quota;
        }
        if (failure ==
            4) wl_columnar_relation_test_constructor_fail_allocation = 3;
        if (failure == 5) wl_columnar_relation_test_constructor_fail_commit = 3;
        int expected = failure == 0 || failure == 3 ? ENOSPC : ENOMEM;
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == expected);
        finish(c, expected);
        CHECK(extension_calls == calls &&
            sess->memory_budget_denied == (expected == ENOSPC));
        lazy_unchanged(source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        if (expected ==
            ENOSPC) CHECK(wl_columnar_memory_rollback(&compiled_quota));
        if (failure == 3) CHECK(extension_quota_reached == 1);
        CHECK(string_reserved(sess->memory_governor) == before);
        frame = filter_frame(sess); retry(c, 0);
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
        CHECK(extension_calls == calls+2);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); join_session_destroy(sess);
    }
    extension_teardown();
}
static void
test_extension_owned_cleanup(void)
{
    extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
    uint8_t buf[32]; uint32_t n = extension_encode(buf, 0, "ext", 3, 0, true);
    for (int map = 0; map < 2; map++) {
        wl_col_session_t *sess = join_session();
        sess->base.extension_snapshot = extension_snapshot;
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        wl_plan_expr_buffer_t expr = {buf, n};
        wl_plan_op_t op = {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .filter_expr = expr, .project_count = 1,
                           .map_exprs = &expr, .map_expr_count = 1};
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        unsigned calls = extension_calls; filter_reached = 0;
        if (map) wl_columnar_ops_map_test_controlled_checkpoint =
                map_owned_cleanup_fault;
        else wl_columnar_filter_test_controlled_checkpoint =
                filter_cleanup_fault;
        CHECK((map ? col_op_map(&op,
            wl_columnar_eval_stack_cleanup_stack(frame), sess) :
            wl_columnar_filter_op(&op,
            wl_columnar_eval_stack_cleanup_stack(frame),
            sess)) == EBUSY);
        sess->base.evaluation_control = NULL; finish(c, EBUSY);
        CHECK(extension_calls == calls+1 && filter_reached == 1 &&
            wl_columnar_eval_stack_cleanup_result(frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY &&
            !frame && sess->cleanup_pending);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        release(c); join_session_destroy(sess);
    }
    extension_teardown();
}

static void
test_extension_real_snapshot_owner(void)
{
    for (int map = 0; map < 2; map++) for (int governed = 0; governed < 2;
            governed++) {
            extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0,
                true);
            wl_plan_t plan = {0}; wl_session_t *session = NULL;
            CHECK(wl_session_create_with_snapshot(wl_backend_columnar(), &plan,
                1, extension_snapshot, &session) == 0);
            CHECK(session->owns_extension_snapshot &&
                session->extension_snapshot == extension_snapshot);
            wl_col_session_t *sess = COL_SESSION(session);
            col_rel_t *source = relation(1, 3, 3, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0);
            uint8_t buf[32];
            uint32_t n = extension_encode(buf, 0, "ext", 3, 0, true);
            wl_plan_expr_buffer_t expr = {buf, n};
            wl_plan_op_t ops[2] = {
                {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
                 .delta_mode = WL_DELTA_FORCE_FULL},
                {.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                 .filter_expr = expr,
                 .project_count = 1, .map_exprs = &expr, .map_expr_count = 1}
            };
            extension_mode = 4;
            wl_evaluation_control_t *c = governed ? control(0) : NULL;
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                if (repeat && c) retry(c, 0);
                CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0);
                if (c) finish(c, 0);
                CHECK(extension_calls == 3*(repeat+1) && !extension_destroyed &&
                    !extension_snapshot);
                CHECK(join_output(frame)->nrows == 3);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            }
            if (c) release(c);
            wl_session_destroy(session);
            CHECK(extension_destroyed == 1);
            extension_teardown();
        }
}
static void
test_extension_direct_release_and_destructor_tls(void)
{
    for (int governed = 0; governed < 2; governed++) {
        extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, true);
        wirelog_extension_registry_t *original = extension_registry;
        wirelog_extension_registry_t *nested_registry =
            wirelog_extension_registry_create(); CHECK(nested_registry);
        extension_registry = nested_registry;
        unsigned saved_count = extension_registered_count;
        extension_register("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0,
            false);
        wirelog_extension_snapshot_t *nested =
            wirelog_extension_snapshot_acquire(nested_registry); CHECK(nested);
        extension_registry = original;
        extension_registered_count = saved_count;
        uint8_t buf[32];
        uint32_t n = extension_encode(buf, 0, "ext", 3, 0, false);
        wl_col_session_t sess = {0}, nested_sess = {0};
        sess.base.extension_snapshot = extension_snapshot;
        nested_sess.base.extension_snapshot = nested;
        sess.callback_session_key = nested_sess.callback_session_key = &sess;
        extension_nested_session = &nested_sess; extension_nested_buf = buf;
        extension_nested_size = n;
        extension_destructor_controlled = governed; extension_mode = 4;
        wl_evaluation_control_t *c = governed ? control(0) : NULL;
        int64_t value = INT64_MIN; bool failed = true;
        wl_columnar_expr_status_t status;
        CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0,
            &value, &failed,
            &sess, &status, false, true,
            c) == 0 && !failed && value == 1 && status == WL_COLUMNAR_EXPR_OK);
        /* This is one call: the borrowed direct snapshot was consumed by host.
         * Clear it immediately; never use it as later-row ownership evidence. */
        sess.base.extension_snapshot = NULL;
        if (c) {
            finish(c, 0); release(c);
        }
        CHECK(extension_calls == 1 && extension_destroyed == 1 &&
            !extension_snapshot &&
            extension_nested_status == WL_COLUMNAR_EXPR_CALLBACK_REENTRANT);
        extension_nested_session = NULL;
        wirelog_extension_snapshot_release(nested);
        CHECK(wirelog_extension_unregister(nested_registry, "ext") == 0);
        CHECK(wirelog_extension_registry_destroy(nested_registry) == 0);
        extension_teardown();
    }
}
static void
test_extension_tls_depth_and_policy(void)
{
    extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
    uint8_t buf[32]; uint32_t n = extension_encode(buf, 0, "ext", 3, 0, true);
    wl_col_session_t sess = {0};
    sess.base.extension_snapshot = extension_snapshot;
    sess.callback_session_key = &extension_session_keys[0];
    extension_nested_session = &sess; extension_nested_buf = buf;
    extension_nested_size = n;
    extension_depth = extension_tls_visits = 0; extension_mode = 5;
    extension_stop_stage = -1;
    wl_columnar_expr_test_controlled_checkpoint = extension_expr_stop;
    wl_evaluation_control_t *c = control(0); int64_t value; bool failed;
    wl_columnar_expr_status_t status;
    CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0, &value,
        &failed, &sess, &status, false, true, c) == 0 &&
        !failed && value == 1 && extension_calls == 64 &&
        extension_tls_visits == (64*65)/2);
    finish(c, 0); release(c);
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    extension_nested_session = NULL; extension_mode = 0;
    sess.callback_parallel_execution = true; sess.callback_active_workers = 2;
    for (int controlled = 0; controlled < 2; controlled++) {
        c = controlled ? control(0) : NULL;
        CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0,
            &value, &failed, &sess, &status, false, true, c) == 0 &&
            failed && status == WL_COLUMNAR_EXPR_CALLBACK_POLICY &&
            extension_calls == 64);
        if (c) {
            finish(c, 0); release(c);
        }
    }
    sess.callback_parallel_execution = false;
    c = control(0);
    CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0, &value,
        &failed, &sess, &status, false, true, c) == 0 && !failed && value == 1);
    finish(c, 0); release(c);
    extension_teardown();
}

static void
test_extension_same_session_policy_and_owner_guard(void)
{
    uint8_t buf[32]; uint32_t n = extension_encode(buf, 0, "ext", 3, 0, false);
    for (int governed = 0; governed < 2; governed++) for (unsigned mode = 6;
            mode < 8; mode++) {
            extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL,
                mode == 6 ? WIRELOG_EXTENSION_CALLBACK_REENTRANT : 0, false);
            wl_col_session_t sess = {0};
            sess.base.extension_snapshot = extension_snapshot;
            sess.callback_session_key = &sess;
            extension_nested_session = &sess; extension_nested_buf = buf;
            extension_nested_size = n;
            extension_depth = 0; extension_mode = mode;
            extension_destructor_controlled = governed;
            wl_evaluation_control_t *c = governed ? control(0) : NULL;
            int64_t value; bool failed; wl_columnar_expr_status_t status;
            CHECK(wl_columnar_expr_eval_interpreter_controlled(buf, n, NULL, 0,
                &value, &failed, &sess, &status, false, true,
                c) == 0 && !failed && value == 1);
            if (c) {
                finish(c, 0); release(c);
            }
            CHECK(extension_calls == (mode == 6 ? 3u : 1u));
            extension_nested_session = NULL;
            extension_teardown();
        }
    extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
    extension_mode = 8;
    extension_caller_twins(buf, n, 1, 0, 0, false);
    extension_teardown();
}
/* REDUCE exercises the actual VARIABLE dispatcher and original-address private
 * frame record. Raw references never receive the unsafe nonempty zero-width
 * grouped shape. */
static int
reduce_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_plan_op_t *reduce,
    wl_evaluation_control_t *c)
{
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL}, *reduce
    };
    return compiled_dispatch(sess, frame, ops, 2, c);
}

static void
test_reduce_actual_twins(void)
{
    for (unsigned mode = 0; mode < 15; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        uint32_t width = mode == 10 ? 301 : mode == 11 || mode == 12 ? 0 : 3;
        uint32_t rows = mode == 9 || mode == 12 ? 0 : mode == 10 ? 3 : 19;
        col_rel_t *source = relation(width, rows, rows, false,
                sess->memory_governor);
        col_rel_t *ref = relation(width, rows, rows, false,
                raw->memory_governor);
        uint32_t indices[300];
        for (uint32_t k = 0; k < 300; k++) indices[k] = k;
        bool floating = mode >= 5 && mode <= 8;
        for (uint32_t r = 0; r < rows; r++) {
            for (uint32_t k = 0; k < width; k++)
                source->columns[k][r] = ref->columns[k][r] = k + r % 3;
            if (width) source->columns[0][r] = ref->columns[0][r] = r % 3;
            if (width > 1) source->columns[1][r] = ref->columns[1][r] = floating
                ? wl_columnar_float_to_bits((double)(r % 5) -
                        2.0) : (int64_t)r - 8;
        }
        if (floating) {
            wirelog_column_type_t types[3] = {WIRELOG_TYPE_INT64,
                                              WIRELOG_TYPE_FLOAT,
                                              WIRELOG_TYPE_INT64};
            source->nrows = ref->nrows = 0;
            CHECK(col_rel_set_column_types(source, types, 3) == 0);
            CHECK(col_rel_set_column_types(ref, types, 3) == 0);
            source->nrows = ref->nrows = rows;
        }
        CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
            ref) == 0);
        uint8_t expr[10000]; uint32_t n = 0;
        if (mode == 13) {
            n = compiled_var(expr, n, "col1");
            n = compiled_const(expr, n, 2); expr[n++] = WL_PLAN_EXPR_ARITH_MUL;
        } else if (mode == 14) {
            n = compiled_const(expr, n, 0);
            expr[n++] = WL_PLAN_EXPR_STR_FN_STRLEN;
        }
        wirelog_agg_fn_t fns[] = {WIRELOG_AGG_COUNT, WIRELOG_AGG_SUM,
                                  WIRELOG_AGG_MIN, WIRELOG_AGG_MAX,
                                  WIRELOG_AGG_AVG};
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE,
                           .group_by_count = mode == 10 ? 300 : mode ==
                               11 ? 0 : 1,
                           .group_by_indices = indices,
                           .agg_fn = floating ? fns[mode - 4] : fns[mode % 5],
                           .agg_operand_type =
                               floating ? WL_PLAN_AGG_OPERAND_FLOAT
                : WL_PLAN_AGG_OPERAND_SCALAR,
                           .aggregate_index = mode == 4 ? 0 : UINT32_MAX,
                           .agg_expr = {expr, n}};
        if (mode == 14) op.agg_fn = WIRELOG_AGG_SUM;
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
            *reference = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        int rc = reduce_dispatch(sess, frame, &op, c);
        int expected = reduce_dispatch(raw, reference, &op, NULL);
        if (rc != expected) fprintf(stderr, "reduce mode=%u rc=%d raw=%d\n",
                mode, rc, expected);
        CHECK(rc == expected); uint64_t cost = finish(c, rc);
        if (!rc) filter_equal(join_output(frame), join_output(reference));
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        if (!rc && mode != 14) {
            frame = filter_frame(sess); retry(c, cost - 1u);
            CHECK(reduce_dispatch(sess, frame, &op,
                c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, cost);
            CHECK(reduce_dispatch(sess, frame, &op, c) == 0);
            CHECK(finish(c, 0) == cost);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        source_unpinned(source, source); release(c);
        join_session_destroy(sess); join_session_destroy(raw);
    }
}

static unsigned reduce_stage, reduce_reached, reduce_grows, reduce_epoch;
static uint64_t reduce_at;
static bool reduce_overflow, reduce_keep_result;
static wl_columnar_relation_private_batch_t *reduce_record;
static void
reduce_stop(wl_evaluation_control_t *c)
{
    reduce_reached++;
    if (reduce_overflow) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
reduce_operator_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_private_batch_t *record, col_rel_t *source)
{
    (void)source;
    if ((unsigned)stage != reduce_stage || progress < reduce_at) return;
    wl_columnar_ops_reduce_test_checkpoint = NULL;
    reduce_record = record;
    reduce_stop(c);
}
static void
reduce_private_stop(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t progress)
{
    if (stage == 4) {
        reduce_grows++; return;
    }
    if ((unsigned)stage != reduce_stage || progress < reduce_at
        || reduce_grows < reduce_epoch) return;
    wl_columnar_relation_test_private_checkpoint = NULL;
    reduce_record = record;
    if (reduce_keep_result)
        CHECK(col_rel_source_reader_acquire(record->batch.relation,
            &join_retained_reader) == 0);
    reduce_stop(c);
}
static void
test_reduce_long_reached_stops(void)
{
    const unsigned stages[] = {1, 2, 3, 4, 6, 7};
    for (unsigned s = 0; s < sizeof(stages)/sizeof(stages[0]); s++)
        for (unsigned overflow = 0; overflow < 2; overflow++) {
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(301, 3, 3, false,
                    sess->memory_governor);
            for (uint32_t col = 0; col < 301; col++) {
                source->columns[col][0] = col;
                source->columns[col][1] = col;
                source->columns[col][2] = col+1;
            }
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE,
                               .group_by_count = 300,
                               .aggregate_index = UINT32_MAX,
                               .agg_fn = WIRELOG_AGG_SUM};
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            reduce_stage = stages[s]; reduce_at = 300;
            reduce_overflow = overflow; reduce_reached = 0;
            wl_columnar_ops_reduce_test_checkpoint = reduce_operator_stop;
            int expected =
                overflow ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
            finish(c, expected); CHECK(reduce_reached == 1);
            lazy_unchanged(source); source_unpinned(source, source);
            CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
            CHECK(join_output(frame)->nrows == 2);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            release(c); join_session_destroy(sess);
        }
}

static void
test_reduce_two_growth_reset(void)
{
    for (unsigned epoch = 1; epoch <= 2; epoch++)
        for (unsigned overflow = 0; overflow < 2; overflow++)
            for (unsigned retained = 0; retained < 2; retained++) {
                wl_col_session_t *sess = join_session();
                uint32_t rows = COL_REL_INIT_CAP * 4u + 1u;
                col_rel_t *source = relation(2, rows, rows, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                                   .aggregate_index = UINT32_MAX,
                                   .agg_fn = WIRELOG_AGG_SUM};
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                reduce_stage = 1; reduce_at = 128; reduce_epoch = epoch;
                reduce_grows = reduce_reached = 0; reduce_overflow = overflow;
                reduce_keep_result = retained;
                wl_columnar_relation_test_private_checkpoint =
                    reduce_private_stop;
                int expected =
                    overflow ? EOVERFLOW : retained ? EBUSY :
                    WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
                finish(c, expected);
                CHECK(reduce_reached == 1 && reduce_grows == epoch);
                CHECK(reduce_record ==
                    wl_columnar_eval_stack_cleanup_reduce_batch(frame));
                CHECK(reduce_record->phase ==
                    WL_COLUMNAR_RELATION_PRIVATE_RESETTING
                    && !reduce_record->batch.active);
                /* The single prefix is partially cleared. Forward refusal must
                * read the independent phase before any pointer/token field. */
                CHECK(
                    wl_columnar_relation_private_append_batch_begin_controlled(
                        reduce_record, source, c) == EBUSY);
                lazy_unchanged(source); source_unpinned(source, source);
                if (retained) {
                    CHECK(wl_columnar_eval_stack_cleanup_result(frame)->rel
                        == reduce_record->batch.relation);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) ==
                        EBUSY);
                    CHECK(sess->cleanup_pending && !frame);
                    CHECK(
                        wl_columnar_relation_private_append_batch_begin_controlled
                        (
                            reduce_record, source, c) == EBUSY);
                    CHECK(col_rel_source_reader_release(
                            &join_retained_reader) == 0);
                    CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
                } else CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) ==
                        0);
                reduce_keep_result = false;
                frame = filter_frame(sess); retry(c, 0);
                CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
                CHECK(join_output(frame)->nrows == rows);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                release(c); join_session_destroy(sess);
            }
}

static void
test_reduce_extension_context(void)
{
    for (unsigned result = 0; result < 2; result++)
        for (unsigned abi = 0; abi < 2; abi++)
            for (unsigned count = 0; count < 2; count++) {
                extension_setup("ext", 0, result ? WIRELOG_EXTENSION_VALUE_INT64
                    : WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
                extension_mode = 0; extension_calls = 0;
                uint8_t buf[32];
                uint32_t n = extension_encode(buf, 0, "ext", 3, 0, abi);
                wl_col_session_t *sess = join_session();
                sess->base.extension_snapshot = extension_snapshot;
                sess->intern = string_table(sess->memory_governor, 0);
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0);
                wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 0,
                                   .aggregate_index = UINT32_MAX,
                                   .agg_fn =
                                       count ? WIRELOG_AGG_COUNT :
                                       WIRELOG_AGG_SUM,
                                   .agg_expr = {buf, n}};
                extension_zero_lookup_visits = 0;
                wl_extension_test_controlled_checkpoint = extension_zero_lookup;
                for (unsigned controlled = 0; controlled < 2; controlled++) {
                    wl_columnar_eval_stack_cleanup_frame_t *frame =
                        filter_frame(sess);
                    wl_evaluation_control_t *c = controlled ? control(0) : NULL;
                    CHECK(reduce_dispatch(sess, frame, &op,
                        c) == (count ? 0 : ERANGE));
                    if (c) {
                        finish(c, count ? 0 : ERANGE); release(c);
                    }
                    CHECK(!extension_calls && !sess->extension_expr_status
                        && !extension_zero_lookup_visits);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                }
                wl_extension_test_controlled_checkpoint = NULL;
                wl_intern_free(sess->intern); sess->intern = NULL;
                sess->base.extension_snapshot = NULL;
                join_session_destroy(sess); extension_teardown();
            }
}
static void
reduce_frame_stop(wl_evaluation_control_t *c, uint64_t bytes)
{
    if (bytes < 300) return;
    wl_columnar_eval_stack_test_constructor_checkpoint = NULL;
    reduce_stop(c);
}
static void
test_reduce_controlled_frame_factory(void)
{
    for (unsigned mode = 0; mode < 7; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0);
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        uint64_t usable = atomic_load(&g->usable_bytes);
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        int expected = 0;
        reduce_reached = 0;
        if (mode < 2) {
            reduce_overflow = mode;
            wl_columnar_eval_stack_test_constructor_checkpoint =
                reduce_frame_stop;
            expected = mode ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
        } else if (mode == 2) {
            wl_columnar_eval_stack_test_fail_allocation = true;
            expected = ENOMEM;
        } else if (mode == 3) {
            wl_columnar_eval_stack_test_fail_commit = true; expected = ENOMEM;
        }
        if (mode == 6) {
            atomic_store(&g->usable_bytes, baseline); expected = ENOSPC;
        }
        CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == expected);
        atomic_store(&g->usable_bytes, usable);
        CHECK(sess->memory_budget_denied == (mode == 6));
        sess->base.evaluation_control = NULL;
        if (expected) {
            finish(c, expected);
            CHECK(!frame && !sess->cleanup_active && !sess->cleanup_pending
                && !sess->cleanup_active_count &&
                !sess->cleanup_reserved_bytes);
            CHECK(string_reserved(sess->memory_governor) == baseline);
            if (mode < 2) CHECK(reduce_reached == 1);
            retry(c, 0); sess->base.evaluation_control = c;
            CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
            sess->base.evaluation_control = NULL;
        }
        CHECK(wl_columnar_eval_stack_cleanup_reduce_batch(frame)->ready);
        wl_plan_op_t ops[2] = {
            {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
             .delta_mode = WL_DELTA_FORCE_FULL},
            {.op = mode == 5 ? WL_PLAN_OP_FILTER : WL_PLAN_OP_MAP,
             .project_count = 1}
        };
        CHECK(compiled_dispatch(sess, frame, ops, 2, c) == 0); finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(string_reserved(sess->memory_governor) == baseline);
        release(c); join_session_destroy(sess);
    }
}

static void
test_reduce_shape_and_resource_failures(void)
{
    for (unsigned mode = 0; mode < 6; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(mode == 0 ? 0 : 2, 2, 2, false,
                sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t before = string_reserved(sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                           .aggregate_index = UINT32_MAX,
                           .agg_fn = WIRELOG_AGG_SUM};
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        uint64_t usable = atomic_load(&g->usable_bytes);
        int expected = mode == 0 ? EINVAL : mode == 1 ? ENOSPC : ENOMEM;
        if (mode == 1) atomic_store(&g->usable_bytes,
                string_reserved(sess->memory_governor));
        if (mode ==
            2) wl_columnar_ops_reduce_test_fail_scratch_allocation = true;
        if (mode == 3) wl_columnar_ops_reduce_test_fail_scratch_commit = true;
        if (mode ==
            4) wl_columnar_relation_test_constructor_fail_allocation = 1;
        if (mode == 5) wl_columnar_relation_test_constructor_fail_commit = 1;
        CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected); atomic_store(&g->usable_bytes, usable);
        CHECK(sess->memory_budget_denied == (mode == 1));
        lazy_unchanged(source); source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(string_reserved(sess->memory_governor) == before);
        release(c); join_session_destroy(sess);
    }
}

static void
test_reduce_probe_symbol_expression_stops(void)
{
    for (unsigned kind = 0; kind < 3; kind++)
        for (unsigned overflow = 0; overflow < 2; overflow++) {
            wl_col_session_t *sess = join_session();
            uint32_t rows = kind == 0 ? 513 : 2;
            col_rel_t *source = relation(2, rows, rows, false,
                    sess->memory_governor);
            uint8_t expr[10000]; uint32_t n = 0;
            if (kind == 0) for (uint32_t r = 0; r < rows; r++)
                    source->columns[0][r] = (int64_t)r * 2048;
            if (kind == 1) {
                sess->intern = string_table(sess->memory_governor, 0);
                char a[602], b[602]; memset(a, 'a', 600); memset(b, 'a', 600);
                a[600] = 'z'; b[600] = 'b'; a[601] = b[601] = 0;
                source->columns[0][0] = source->columns[0][1] = 0;
                source->columns[1][0] = wl_intern_put(sess->intern, a);
                source->columns[1][1] = wl_intern_put(sess->intern, b);
            }
            if (kind == 2) {
                for (unsigned j = 0; j < 513; j++) {
                    n = compiled_const(expr, n, 1);
                    if (j) expr[n++] = WL_PLAN_EXPR_ARITH_ADD;
                }
            }
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                               .aggregate_index = UINT32_MAX,
                               .agg_fn = kind == 1 ? WIRELOG_AGG_MIN
                    : WIRELOG_AGG_SUM,
                               .agg_operand_type = kind ==
                                   1 ? WL_PLAN_AGG_OPERAND_STRING
                    : WL_PLAN_AGG_OPERAND_SCALAR,
                               .agg_expr = {expr, n}};
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            reduce_overflow = overflow; reduce_reached = 0;
            if (kind < 2) {
                reduce_stage = kind == 0 ? 5 : 9; reduce_at = 300;
                wl_columnar_ops_reduce_test_checkpoint = reduce_operator_stop;
            } else {
                compiled_stop_stage = 3; compiled_stop_progress = 300;
                compiled_stop_positive = overflow;
                compiled_require_failed = false;
                compiled_reached = 0;
                wl_columnar_expr_test_controlled_checkpoint =
                    compiled_stage_stop;
            }
            int expected =
                overflow ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
            finish(c, expected);
            CHECK(kind < 2 ? reduce_reached == 1 : compiled_reached == 1);
            lazy_unchanged(source); source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            frame = filter_frame(sess); retry(c, 0);
            CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
            CHECK(join_output(frame)->nrows ==
                (kind == 0 ? rows : kind == 1 ? 1 : 2));
            if (kind ==
                1) CHECK(join_output(frame)->columns[1][0] ==
                    source->columns[1][1]);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            if (sess->intern) {
                wl_intern_free(sess->intern); sess->intern = NULL;
            }
            release(c); join_session_destroy(sess);
        }
}
static wl_columnar_source_access_writer_t *reduce_fault_writer;
static bool reduce_original_thread_valid;
static unsigned reduce_fault_kind;
static uint64_t reduce_cell_before, reduce_cell_generation;
static void
reduce_refusal_fault(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t progress)
{
    (void)progress;
    if ((reduce_fault_kind == 0 && stage != 2)
        || (reduce_fault_kind == 2 && stage != 4)
        || (reduce_fault_kind == 3 && stage != 5)
        || (reduce_fault_kind == 4 && stage != 7)
        || reduce_fault_kind == 1) return;
    wl_columnar_relation_test_private_checkpoint = NULL;
    reduce_record = record; reduce_reached++;
    reduce_fault_writer = reduce_fault_kind == 0 || reduce_fault_kind == 2
        ? &record->batch.single.descriptor.writer :
        &record->batch.single.owners[0].writer;
    reduce_original_thread_valid = reduce_fault_writer->thread_valid;
    CHECK(reduce_original_thread_valid && reduce_fault_writer->owner
        && reduce_fault_writer->identity == (uintptr_t)reduce_fault_writer
        && atomic_load(&reduce_fault_writer->owner->state) ==
        WL_COLUMNAR_SOURCE_ACCESS_WRITER);
    if (reduce_fault_kind >= 3) {
        col_rel_t *out = record->batch.relation;
        reduce_cell_before = out->columns[1][0];
        reduce_cell_generation = out->view_generation;
        if (reduce_fault_kind ==
            3) out->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
        else {
            record->batch.single.lease.relation_generation--;
            CHECK(col_rel_mutation_lease_validate(&record->batch.single.lease,
                out) == EINVAL);
        }
    } else wl_evaluation_control_request_cancel(c);
    reduce_fault_writer->thread_valid = false;
}
static void
reduce_held_fault(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_private_batch_t *record, col_rel_t *source)
{
    (void)progress; (void)source;
    if (stage != 11) return;
    wl_columnar_ops_reduce_test_checkpoint = NULL;
    CHECK(record->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD);
    reduce_record = record; reduce_reached++;
    reduce_fault_writer = &record->batch.single.owners[0].writer;
    reduce_original_thread_valid = reduce_fault_writer->thread_valid;
    CHECK(reduce_original_thread_valid && reduce_fault_writer->owner);
    reduce_fault_writer->thread_valid = false;
    wl_evaluation_control_request_cancel(c);
}
static void
test_reduce_retirement_authority_faults(void)
{
    /* Deliberately malformed thread-authority validation, NOT healthy reader
     * contention or a release-call failure: only this saved bool is changed. */
    for (unsigned kind = 0; kind < 5; kind++) {
        wl_col_session_t *sess = join_session();
        uint32_t rows = kind == 2 ? COL_REL_INIT_CAP * 4u + 1u : 2;
        col_rel_t *source = relation(2, rows, rows, false,
                sess->memory_governor);
        if (kind >= 3) source->columns[0][1] = source->columns[0][0];
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                           .aggregate_index = UINT32_MAX,
                           .agg_fn = WIRELOG_AGG_SUM};
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        reduce_fault_kind = kind; reduce_reached = 0;
        if (kind ==
            1) wl_columnar_ops_reduce_test_checkpoint = reduce_held_fault;
        else wl_columnar_relation_test_private_checkpoint =
                reduce_refusal_fault;
        int expected = kind == 3 ? EOVERFLOW : EINVAL;
        CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected); CHECK(reduce_reached == 1);
        CHECK(reduce_record ==
            wl_columnar_eval_stack_cleanup_reduce_batch(frame));
        CHECK(reduce_record->phase == WL_COLUMNAR_RELATION_PRIVATE_RETIRING
            && !reduce_record->batch.active);
        col_rel_t *out = wl_columnar_eval_stack_cleanup_result(frame)->rel;
        CHECK(out && out == reduce_record->batch.relation);
        CHECK(reduce_fault_writer->owner && !reduce_fault_writer->thread_valid
            && reduce_fault_writer->identity == (uintptr_t)reduce_fault_writer
            && atomic_load(&reduce_fault_writer->owner->state) ==
            WL_COLUMNAR_SOURCE_ACCESS_WRITER);
        if (kind == 0) CHECK(!reduce_record->batch.single.owners[0].writer.owner
                && !reduce_record->batch.single.set.owner_count &&
                out->nrows == 0);
        if (kind >= 3) {
            CHECK(out->columns[1][0] == (int64_t)reduce_cell_before);
            CHECK(out->view_generation ==
                (kind == 3 ? WL_COLUMNAR_REL_GENERATION_INVALID - 1u
                    : reduce_cell_generation));
        }
        lazy_unchanged(source); source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL &&
            !frame);
        CHECK(sess->cleanup_pending && sess->cleanup_pending_count == 1
            && string_reserved(sess->memory_governor) > baseline);
        CHECK(wl_columnar_eval_stack_cleanup_reduce_batch(
                sess->cleanup_pending) == reduce_record);
        reduce_fault_writer->thread_valid = reduce_original_thread_valid;
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        CHECK(!sess->cleanup_pending &&
            string_reserved(sess->memory_governor) == baseline);
        frame = filter_frame(sess); retry(c, 0);
        CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); join_session_destroy(sess);
    }
}
static void
test_reduce_typed_edges_and_symbols(void)
{
    for (unsigned mode = 0; mode < 10; mode++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        col_rel_t *source = relation(2, 3, 0, false, sess->memory_governor),
            *ref = relation(2, 3, 0, false, raw->memory_governor);
        bool floating = mode >= 1 && mode <= 4;
        wirelog_column_type_t types[2] = {mode == 0 ? WIRELOG_TYPE_FLOAT
            : WIRELOG_TYPE_INT64,
                                          floating ? WIRELOG_TYPE_FLOAT :
                                          WIRELOG_TYPE_INT64};
        CHECK(col_rel_set_column_types(source, types, 2) == 0);
        CHECK(col_rel_set_column_types(ref, types, 2) == 0);
        source->nrows = ref->nrows = mode == 9 ? 0 : 3;
        for (unsigned r = 0; r < 3; r++) {
            source->columns[0][r] = ref->columns[0][r] = mode == 0
                ? wl_columnar_float_to_bits(r == 0 ? -0.0 : r ==
                    1 ? 0.0 : 1.0) : 0;
            source->columns[1][r] = ref->columns[1][r] = floating
                ? wl_columnar_float_to_bits(mode == 1 ? (r ? 0.0 : -0.0)
                    : mode == 4 ? NAN : DBL_MAX)
                : mode == 5 ? (r ? 1 : INT64_MAX) : (int64_t)r;
        }
        uint32_t fallback = UINT32_MAX;
        uint8_t buf[32]; uint32_t n = 0;
        if (mode == 8 || mode == 9) {
            n = compiled_const(buf, 0, 1); n = compiled_const(buf, n, 2);
            buf[n++] = WL_PLAN_EXPR_ARITH_HMAC_SHA256;
        }
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                           .group_by_indices = mode == 6 ? &fallback : NULL,
                           .aggregate_index = mode == 7 ? 0 : UINT32_MAX,
                           .agg_fn = mode == 0 || mode == 8 ||
                               mode == 9 ? WIRELOG_AGG_COUNT
                : mode == 3 ? WIRELOG_AGG_AVG : WIRELOG_AGG_SUM,
                           .agg_operand_type =
                               floating ? WL_PLAN_AGG_OPERAND_FLOAT :
                               WL_PLAN_AGG_OPERAND_SCALAR,
                           .agg_expr = {buf, n}};
        CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
            ref) == 0);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
            *reference = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        int expected = mode == 2 || mode == 3 ||
            mode == 5 ? ERANGE : mode == 4 ? EINVAL : 0;
        memset(hmac_calls, 0, sizeof(hmac_calls));
        wl_columnar_expr_hmac_test_provider_result = hmac_provider_report;
        CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected);
        CHECK(reduce_dispatch(raw, reference, &op, NULL) == expected);
        wl_columnar_expr_hmac_test_provider_result = NULL;
        for (unsigned k = 0; k < 7; k++) CHECK(!hmac_calls[k]);
        if (!expected) {
            filter_equal(join_output(frame), join_output(reference));
            if (mode == 0) CHECK(join_output(frame)->nrows == 2
                    && join_output(frame)->columns[0][0] == 0
                    && join_output(frame)->columns[1][0] == 2);
            if (mode == 1) CHECK(join_output(frame)->columns[1][0] == 0);
        }
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
        release(c); join_session_destroy(sess); join_session_destroy(raw);
    }
    for (unsigned maximum = 0; maximum < 2; maximum++)
        for (unsigned missing = 0; missing < 3; missing++) {
            wl_col_session_t *sess = join_session(), *raw = join_session();
            sess->intern = string_table(sess->memory_governor, 0);
            raw->intern = string_table(raw->memory_governor, 0);
            CHECK(wl_intern_put(sess->intern,
                "z") == 0 && wl_intern_put(raw->intern, "z") == 0);
            CHECK(wl_intern_put(sess->intern,
                "a") == 1 && wl_intern_put(raw->intern, "a") == 1);
            col_rel_t *source = relation(2, 3, 3, false, sess->memory_governor),
                *ref = relation(2, 3, 3, false, raw->memory_governor);
            for (unsigned r = 0; r < 3; r++) {
                source->columns[0][r] = ref->columns[0][r] = 0;
                source->columns[1][r] = ref->columns[1][r] = missing == 2
                    ? (r ? INT64_MAX : INT64_MIN) : missing == 1 && r != 1
                    ? INT64_MAX : (int64_t)(r % 2);
            }
            CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
                ref) == 0);
            wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                               .aggregate_index = UINT32_MAX,
                               .agg_fn =
                                   maximum ? WIRELOG_AGG_MAX : WIRELOG_AGG_MIN,
                               .agg_operand_type = WL_PLAN_AGG_OPERAND_STRING};
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
                *reference = filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
            CHECK(reduce_dispatch(raw, reference, &op, NULL) == 0);
            filter_equal(join_output(frame), join_output(reference));
            CHECK(join_output(frame)->columns[1][0] == (missing == 2
                ? maximum ? INT64_MAX : INT64_MIN : missing ==
                1 ? 1 : maximum ? 0 : 1));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
            wl_intern_free(sess->intern); wl_intern_free(raw->intern);
            sess->intern = raw->intern = NULL;
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
}

static void
test_reduce_intern_context_and_c2(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session();
        sess->intern = string_table(sess->memory_governor, 0);
        CHECK(wl_intern_put(sess->intern, "prior") == 0);
        col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0);
        uint8_t buf[64]; uint32_t n = string_literal(buf, 0, "prefix", 6);
        n = string_literal(buf, n, "-value", 6);
        buf[n++] = WL_PLAN_EXPR_STR_FN_CAT;
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 0,
                           .agg_fn = WIRELOG_AGG_MIN,
                           .agg_operand_type = WL_PLAN_AGG_OPERAND_STRING,
                           .agg_expr = {buf, n}};
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        if (mode) {
            compiled_stop_stage = 3; compiled_stop_progress = 3;
            compiled_stop_positive = mode == 2; compiled_require_failed = false;
            compiled_reached = 0;
            wl_columnar_expr_test_controlled_checkpoint = compiled_stage_stop;
        }
        int expected = mode == 2 ? EOVERFLOW : mode ==
            1 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected);
        CHECK(wl_intern_count(sess->intern) == 4);
        CHECK(!strcmp(wl_intern_reverse(sess->intern, 0), "prior")
            && !strcmp(wl_intern_reverse(sess->intern, 3), "prefix-value"));
        if (!mode) CHECK(join_output(frame)->columns[0][0] == 3);
        else CHECK(compiled_reached == 1);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = filter_frame(sess); retry(c, 0);
        CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
        CHECK(join_output(frame)->columns[0][0] == 3 &&
            wl_intern_count(sess->intern) == 4);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        frame = filter_frame(sess);
        CHECK(reduce_dispatch(sess, frame, &op, NULL) == 0);
        CHECK(join_output(frame)->columns[0][0] == 3);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        wl_intern_free(sess->intern); sess->intern = NULL;
        release(c); join_session_destroy(sess);
    }
}

static void
reduce_growth_quota(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t progress)
{
    (void)c; (void)progress;
    if (stage != 4) return;
    wl_columnar_relation_test_private_checkpoint = NULL;
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(
        record->batch.relation->memory_governor);
    atomic_store(&g->usable_bytes, wl_columnar_memory_reserved(g));
    reduce_reached++;
}
static void
test_reduce_growth_quota_and_alias(void)
{
    for (unsigned mode = 0; mode < 2; mode++) {
        wl_col_session_t *sess = join_session();
        uint32_t rows = mode ? 3 : COL_REL_INIT_CAP * 4u + 1u;
        col_rel_t *owner = relation(2, rows, rows, false,
                sess->memory_governor);
        col_rel_t *source = owner;
        if (mode) {
            source = relation(2, 0, 0, false, sess->memory_governor);
            CHECK(col_rel_install_shared_view(source, owner) == 0);
        }
        CHECK(session_add_rel(sess, source) == 0);
        uint64_t before = string_reserved(sess->memory_governor);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        uint64_t usable = atomic_load(&g->usable_bytes);
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                           .aggregate_index = UINT32_MAX,
                           .agg_fn = WIRELOG_AGG_SUM};
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        reduce_reached = 0;
        if (!mode) wl_columnar_relation_test_private_checkpoint =
                reduce_growth_quota;
        int expected = mode ? 0 : ENOSPC;
        CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
        finish(c, expected);
        CHECK(sess->memory_budget_denied == !mode);
        if (!mode) CHECK(reduce_reached == 1);
        else CHECK(join_output(frame)->nrows == rows);
        atomic_store(&g->usable_bytes, usable);
        source_unpinned(source, owner);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(string_reserved(sess->memory_governor) == before);
        release(c);
        if (mode) {
            destroy(source); sess->nrels = 0; destroy(owner);
        }
        join_session_destroy(sess);
    }
}

static unsigned reduce_owned_phase;
static void
reduce_owned_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_private_batch_t *record, col_rel_t *source)
{
    (void)record; (void)progress;
    if ((unsigned)stage != reduce_owned_phase) return;
    wl_columnar_ops_reduce_test_checkpoint = NULL;
    CHECK(col_rel_source_reader_acquire(source, &join_retained_reader) == 0);
    reduce_reached++;
    if (c) wl_evaluation_control_request_cancel(c);
}
static void
test_reduce_owned_cleanup(void)
{
    for (unsigned phase = 12; phase <= 12; phase += 2) {
        reduce_owned_phase = phase;
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 3, 3, false, sess->memory_governor);
        make_lazy(source);
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                true);
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                           .aggregate_index = UINT32_MAX,
                           .agg_fn = WIRELOG_AGG_SUM};
        wl_evaluation_control_t *c = control(0);
        wl_columnar_ops_reduce_test_checkpoint = reduce_owned_stop;
        reduce_reached = 0;
        sess->base.evaluation_control = c;
        CHECK(col_op_reduce(&op, wl_columnar_eval_stack_cleanup_stack(frame),
            sess) == EBUSY);
        sess->base.evaluation_control = NULL;
        finish(c, EBUSY); CHECK(reduce_reached == 1);
        if (phase == 10) lazy_unchanged(source);
        else CHECK(source->storage_owner == source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY);
        CHECK(sess->cleanup_pending && !frame);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        release(c); join_session_destroy(sess);
    }
}
static uint64_t reduce_observed_view, reduce_observed_storage;
static unsigned reduce_cell_visits;
static void
reduce_cell_observe(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t row)
{
    (void)c; (void)row;
    if (stage != 5) return;
    col_rel_t *out = record->batch.relation;
    CHECK(record->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD
        && record->batch.single.lease.relation_generation ==
        out->storage_generation
        && record->batch.single.lease.owner_generation ==
        out->storage_generation);
    CHECK(col_rel_mutation_lease_validate(&record->batch.single.lease,
        out) == 0);
    reduce_observed_view = out->view_generation;
    reduce_observed_storage = out->storage_generation;
    reduce_cell_visits++;
}
static void
test_reduce_growth_current_cell(void)
{
    wl_col_session_t *sess = join_session(), *raw = join_session();
    uint32_t rows = COL_REL_INIT_CAP * 4u + 2u;
    col_rel_t *source = relation(2, rows, rows, false, sess->memory_governor),
        *ref = relation(2, rows, rows, false, raw->memory_governor);
    source->columns[0][rows-1] = ref->columns[0][rows-1] = 0;
    source->columns[1][rows-1] = ref->columns[1][rows-1] = 7;
    CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw, ref) == 0);
    wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                       .aggregate_index = UINT32_MAX,
                       .agg_fn = WIRELOG_AGG_SUM};
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
        *reference = filter_frame(raw);
    wl_evaluation_control_t *c = control(0);
    reduce_cell_visits = 0;
    wl_columnar_relation_test_private_checkpoint = reduce_cell_observe;
    CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
    wl_columnar_relation_test_private_checkpoint = NULL;
    CHECK(reduce_dispatch(raw, reference, &op, NULL) == 0);
    filter_equal(join_output(frame), join_output(reference));
    col_rel_t *out = join_output(frame);
    CHECK(reduce_cell_visits == 1 && out->nrows == rows-1
        && out->columns[1][0] == 10007);
    CHECK(out->view_generation == reduce_observed_view + 1
        && out->storage_generation == reduce_observed_storage);
    wl_columnar_relation_private_batch_t *record =
        wl_columnar_eval_stack_cleanup_reduce_batch(frame);
    CHECK(record->batch.single.set.published &&
        record->batch.controlled_row_committed);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
        && wl_columnar_eval_stack_cleanup_finish(&reference) == 0);
    release(c); join_session_destroy(sess); join_session_destroy(raw);
}
static void
test_reduce_interpreter_allocation_policy(void)
{
    wl_col_session_t *sess = join_session();
    sess->intern = string_table(sess->memory_governor, 0);
    col_rel_t *source = relation(1, 1, 1, false, sess->memory_governor);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint8_t buf[32]; uint32_t n = string_literal(buf, 0, "allocation", 10);
    wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 0,
                       .agg_fn = WIRELOG_AGG_MIN,
                       .agg_operand_type = WL_PLAN_AGG_OPERAND_STRING,
                       .agg_expr = {buf, n}};
    uint64_t baseline = string_reserved(sess->memory_governor);
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_expr_test_fail_allocation = 0;
    CHECK(reduce_dispatch(sess, frame, &op, c) == ERANGE); finish(c, ERANGE);
    CHECK(wl_columnar_expr_test_fail_allocation == -1 &&
        !sess->memory_budget_denied
        && !sess->extension_expr_status && wl_intern_count(sess->intern) == 0);
    lazy_unchanged(source); source_unpinned(source, source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
    CHECK(string_reserved(sess->memory_governor) == baseline);
    wl_intern_free(sess->intern); sess->intern = NULL;
    release(c); join_session_destroy(sess);
}
static unsigned reduce_cell_shape_kind, reduce_cell_shape_reached;
static void
reduce_cell_shape_refusal(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_relation_private_batch_t *record,
    col_rel_t *source)
{
    (void)progress;
    if (stage != 11) return;
    col_rel_t *out = record->batch.relation;
    CHECK(record->ready && record->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD
        && record->batch.active && out->nrows == 1 && out->ncols == 2
        && out->capacity && out->column_types);
    CHECK(col_rel_mutation_lease_validate(&record->batch.single.lease,
        out) == 0);
    int64_t payload[2] = {out->columns[0][0], out->columns[1][0]};
    uint64_t view = out->view_generation, storage = out->storage_generation;
    uint64_t source_view = source->view_generation,
        source_storage = source->storage_generation;
    bool published = record->batch.single.set.published,
        committed = record->batch.controlled_row_committed;
    uint64_t credit = string_reserved(out->memory_governor);
    uint32_t capacity = out->capacity;
    wirelog_column_type_t type = out->column_types[1];
    /* Intentionally malformed private metadata only. The physical payload
     * remains allocated; the refused call must never write out of bounds. */
    if (!reduce_cell_shape_kind) out->capacity = 0;
    else out->column_types[1] = WIRELOG_TYPE_STRING;
    CHECK(wl_columnar_relation_private_cell_controlled(record, 0, 1, 777, c)
        == EINVAL);
    CHECK(out->columns[0][0] == payload[0] && out->columns[1][0] == payload[1]
        && out->view_generation == view && out->storage_generation == storage
        && record->batch.single.set.published == published
        && record->batch.controlled_row_committed == committed);
    CHECK(source->view_generation == source_view
        && source->storage_generation == source_storage
        && string_reserved(out->memory_governor) == credit
        && record->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD
        && record->batch.active);
    CHECK(col_rel_mutation_lease_validate(&record->batch.single.lease,
        out) == 0);
    out->capacity = capacity;
    out->column_types[1] = type;
    CHECK(col_rel_mutation_lease_validate(&record->batch.single.lease,
        out) == 0);
    reduce_cell_shape_reached++;
}
static void
test_reduce_cell_shape_schema_refusals(void)
{
    for (unsigned kind = 0; kind < 2; kind++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 2, 2, false, sess->memory_governor);
        source->columns[0][1] = source->columns[0][0];
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                           .aggregate_index = UINT32_MAX,
                           .agg_fn = WIRELOG_AGG_SUM};
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        reduce_cell_shape_kind = kind; reduce_cell_shape_reached = 0;
        wl_columnar_ops_reduce_test_checkpoint = reduce_cell_shape_refusal;
        CHECK(reduce_dispatch(sess, frame, &op, c) == 0); finish(c, 0);
        wl_columnar_ops_reduce_test_checkpoint = NULL;
        CHECK(reduce_cell_shape_reached == 1 && !sess->memory_budget_denied);
        col_rel_t *out = join_output(frame);
        CHECK(out->nrows == 1 && out->columns[1][0] == 20001);
        source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(string_reserved(sess->memory_governor) == baseline);
        release(c); join_session_destroy(sess);
    }
}
static void
test_reduce_forbidden_extension_c2(void)
{
    for (unsigned result = 0; result < 2; result++)
        for (unsigned abi = 0; abi < 2; abi++)
            for (unsigned overflow = 0; overflow < 2; overflow++) {
                extension_setup("ext", 0, result ? WIRELOG_EXTENSION_VALUE_INT64
                    : WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0, false);
                extension_mode = 0; extension_calls = 0;
                uint8_t buf[32];
                uint32_t n = extension_encode(buf, 0, "ext", 3, 0, abi);
                wl_col_session_t *sess = join_session();
                sess->base.extension_snapshot = extension_snapshot;
                col_rel_t *source = relation(1, 1, 1, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t baseline = string_reserved(sess->memory_governor);
                wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 0,
                                   .agg_fn = WIRELOG_AGG_SUM,
                                   .agg_expr = {buf, n}};
                wl_columnar_eval_stack_cleanup_frame_t *frame =
                    filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                compiled_stop_stage = 3; compiled_stop_progress = 1;
                compiled_stop_positive = overflow;
                compiled_require_failed = true;
                compiled_reached = extension_zero_lookup_visits = 0;
                wl_columnar_expr_test_controlled_checkpoint =
                    compiled_stage_stop;
                wl_extension_test_controlled_checkpoint = extension_zero_lookup;
                int expected =
                    overflow ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
                CHECK(reduce_dispatch(sess, frame, &op, c) == expected);
                finish(c, expected);
                CHECK(compiled_reached == 1 && !extension_calls
                    && !extension_zero_lookup_visits &&
                    !sess->extension_expr_status
                    && !sess->memory_budget_denied);
                wl_extension_test_controlled_checkpoint = NULL;
                lazy_unchanged(source); source_unpinned(source, source);
                CHECK(!wl_columnar_eval_stack_cleanup_result(frame)->rel);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
                CHECK(string_reserved(sess->memory_governor) == baseline);
                sess->base.extension_snapshot = NULL;
                release(c); join_session_destroy(sess); extension_teardown();
            }
}
static bool shared_is_map, shared_source_committed;
static wl_plan_expr_buffer_t shared_expression;
static wl_columnar_eval_stack_cleanup_frame_t *shared_frame;
static wl_columnar_relation_private_batch_t *shared_record;
static wl_columnar_source_access_writer_t *shared_writer;
static bool shared_thread_valid;
static unsigned shared_fault_stage, shared_visits, shared_epoch,
    shared_target_epoch;
static bool shared_overflow, shared_keep_reader, shared_growth,
    shared_reuse_reset;
static col_rel_t *shared_output;
static unsigned shared_predicate_visits, shared_predicate_at_reset;
static int
shared_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_evaluation_control_t *c)
{
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = shared_is_map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
         .project_count = 2, .filter_expr = shared_expression,
         .map_exprs = shared_expression.size ? &shared_expression : NULL,
         .map_expr_count = shared_expression.size ? 1u : 0u},
    };
    return compiled_dispatch(sess, frame, ops, 2, c);
}
static void
shared_authority_fault(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t progress)
{
    (void)progress;
    if ((unsigned)stage != shared_fault_stage || shared_visits) return;
    CHECK(record == wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame));
    shared_record = record;
    shared_writer = stage == 2 ? &record->batch.single.descriptor.writer
        : &record->batch.single.owners[0].writer;
    shared_thread_valid = shared_writer->thread_valid;
    CHECK(shared_thread_valid && shared_writer->owner
        && shared_writer->identity == (uintptr_t)shared_writer);
    shared_writer->thread_valid = false;
    shared_visits++;
    wl_evaluation_control_request_cancel(c);
}
static void
shared_caller_fault(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    (void)progress; (void)group; (void)source;
    if (stage != 9 || shared_fault_stage != 9 || shared_visits) return;
    shared_authority_fault(c,
        wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame), 9, 0);
}
static void
test_shared_writer_authority_refusals(void)
{
    /* Actual original token boolean validation fault, not healthy contention. */
    for (unsigned caller = 0; caller < 2; caller++)
        for (unsigned fault = 0; fault < 3; fault++) {
            shared_is_map = caller;
            uint8_t effect[64];
            shared_expression = (wl_plan_expr_buffer_t){0};
            if (fault == 2) {
                extension_setup("ext", 0, WIRELOG_EXTENSION_VALUE_BOOL, NULL, 0,
                    false);
                uint32_t n = string_literal(effect, 0, "retained", 8);
                n = extension_encode(effect, n, "ext", 3, 0, false);
                shared_expression = (wl_plan_expr_buffer_t){effect, n};
            }
            wl_col_session_t *sess = join_session();
            if (fault == 2) {
                sess->intern = string_table(NULL, 0);
                sess->base.extension_snapshot = extension_snapshot;
            }
            col_rel_t *source = relation(2, 3, 3, false, sess->memory_governor);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint64_t baseline = string_reserved(sess->memory_governor);
            shared_frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            shared_fault_stage = fault == 0 ? 2 : fault == 1 ? 3 : 9;
            shared_visits = 0;
            wl_columnar_relation_test_private_checkpoint =
                shared_authority_fault;
            if (caller) wl_columnar_ops_map_test_controlled_checkpoint =
                    shared_caller_fault;
            else wl_columnar_filter_test_controlled_checkpoint =
                    shared_caller_fault;
            CHECK(shared_dispatch(sess, shared_frame, c) == EINVAL);
            finish(c, EINVAL);
            if (fault == 2) CHECK(extension_calls == 3 &&
                    wl_intern_count(sess->intern) == 1
                    && strcmp(wl_intern_reverse(sess->intern, 0),
                    "retained") == 0);
            wl_columnar_relation_test_private_checkpoint = NULL;
            wl_columnar_ops_map_test_controlled_checkpoint = NULL;
            wl_columnar_filter_test_controlled_checkpoint = NULL;
            CHECK(shared_visits == 1 && shared_record->phase ==
                WL_COLUMNAR_RELATION_PRIVATE_RETIRING &&
                !shared_record->batch.active);
            CHECK(shared_writer->owner && !shared_writer->thread_valid
                && shared_writer->identity == (uintptr_t)shared_writer
                && atomic_load(&shared_writer->owner->state) ==
                WL_COLUMNAR_SOURCE_ACCESS_WRITER);
            CHECK(wl_columnar_eval_stack_cleanup_result(shared_frame)->rel
                == shared_record->batch.relation);
            lazy_unchanged(source); source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == EINVAL
                && !shared_frame && sess->cleanup_pending);
            CHECK(string_reserved(sess->memory_governor) > baseline);
            shared_writer->thread_valid = shared_thread_valid;
            CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
            CHECK(!sess->cleanup_pending &&
                string_reserved(sess->memory_governor) == baseline);
            shared_frame = filter_frame(sess); retry(c, 0);
            CHECK(shared_dispatch(sess, shared_frame, c) == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0);
            if (fault == 2) {
                CHECK(extension_calls == 6 &&
                    wl_intern_count(sess->intern) == 1);
                sess->base.extension_snapshot = NULL;
                wl_intern_free(sess->intern); sess->intern = NULL;
                extension_teardown();
            }
            shared_expression = (wl_plan_expr_buffer_t){0};
            release(c); join_session_destroy(sess);
        }
}
static void
shared_reset_stop(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t progress)
{
    if (stage == 4) {
        shared_epoch++;
        CHECK(progress == ((uint64_t)COL_REL_INIT_CAP << shared_epoch));
        if (shared_epoch == shared_target_epoch) {
            shared_growth = true; shared_output = record->batch.relation;
        }
        return;
    }
    if (stage != 1 || progress != 128 || shared_visits
        || (!shared_growth && !shared_reuse_reset)) return;
    CHECK(record->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING);
    shared_record = record; shared_visits++;
    shared_predicate_at_reset = shared_predicate_visits;
    if (shared_keep_reader) {
        CHECK(shared_output);
        CHECK(col_rel_source_reader_acquire(shared_output,
            &join_retained_reader) == 0);
    }
    wl_columnar_relation_test_private_checkpoint = NULL;
    if (shared_overflow) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
shared_filter_progress(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    (void)c; (void)progress; (void)group; (void)source;
    if (stage == 3) shared_predicate_visits++;
}
static void
test_shared_filter_growth_reset(void)
{
    shared_is_map = false;
    for (unsigned epoch = 1; epoch <= 2; epoch++)
        for (unsigned overflow = 0; overflow < 2; overflow++)
            for (unsigned retained = 0; retained < 2; retained++) {
                wl_col_session_t *sess = join_session();
                col_rel_t *source = relation(2, 513, 513, false,
                        sess->memory_governor);
                CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
                uint64_t baseline = string_reserved(sess->memory_governor);
                shared_frame = filter_frame(sess);
                wl_evaluation_control_t *c = control(0);
                shared_visits = shared_epoch = shared_predicate_visits = 0;
                wl_columnar_filter_test_controlled_checkpoint =
                    shared_filter_progress; shared_target_epoch = epoch;
                shared_growth = shared_reuse_reset = false;
                shared_overflow = overflow; shared_keep_reader = retained;
                wl_columnar_relation_test_private_checkpoint =
                    shared_reset_stop;
                int expected =
                    overflow ? EOVERFLOW : retained ? EBUSY :
                    WL_EVALUATION_CONTROL_CANCELLED;
                int actual = shared_dispatch(sess, shared_frame, c);
                if (actual != expected) fprintf(stderr,
                        "shared growth epoch=%u overflow=%u retained=%u actual=%d expected=%d visits=%u epochs=%u\n",
                        epoch, overflow, retained, actual, expected,
                        shared_visits, shared_epoch);
                CHECK(actual == expected);
                finish(c, expected);
                wl_columnar_filter_test_controlled_checkpoint = NULL;
                CHECK(shared_predicate_visits == shared_predicate_at_reset
                    && shared_predicate_visits ==
                    (((unsigned)COL_REL_INIT_CAP << epoch) + 1u));
                CHECK(shared_visits == 1 && shared_epoch == epoch);
                CHECK(shared_record->phase ==
                    WL_COLUMNAR_RELATION_PRIVATE_RESETTING
                    && !shared_record->batch.active);
                CHECK(
                    wl_columnar_relation_private_append_batch_begin_controlled(
                        shared_record, source, c) == EBUSY);
                retry(c, 0);
                wl_columnar_relation_private_batch_t *found = NULL;
                CHECK(wl_columnar_eval_stack_cleanup_private_batch_owner(sess,
                    wl_columnar_eval_stack_cleanup_stack(shared_frame), &found,
                    c) == EBUSY && !found);
                finish(c, EBUSY);
                lazy_unchanged(source); source_unpinned(source, source);
                int cleanup =
                    wl_columnar_eval_stack_cleanup_finish(&shared_frame);
                CHECK(cleanup == (retained ? EBUSY : 0));
                if (retained) {
                    CHECK(sess->cleanup_pending &&
                        string_reserved(sess->memory_governor) > baseline);
                    CHECK(col_rel_source_reader_release(
                            &join_retained_reader) == 0);
                    CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
                }
                CHECK(string_reserved(sess->memory_governor) == baseline);
                shared_frame = filter_frame(sess); retry(c, 0);
                CHECK(shared_dispatch(sess, shared_frame, c) == 0);
                finish(c, 0);
                CHECK(join_output(shared_frame)->nrows == 513);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) ==
                    0);
                release(c); join_session_destroy(sess);
            }
}
static void
shared_owned_reader(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    (void)progress;
    if (stage == 8) {
        CHECK(!c && group->committed);
        shared_source_committed = true;
        return;
    }
    if (stage != 10) return;
    CHECK(!c && shared_source_committed && !group->descriptor_count &&
        !group->owner_count);
    CHECK(col_rel_source_reader_acquire(source, &join_retained_reader) == 0);
    shared_visits++;
}
static void
test_shared_owned_reader_and_reuse(void)
{
    for (unsigned caller = 0; caller < 2; caller++) {
        shared_is_map = caller;
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 3, 3, false, sess->memory_governor);
        shared_frame = join_frame(sess, source, true);
        wl_evaluation_control_t *c = control(0);
        wl_plan_op_t op = {.op = caller ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER,
                           .project_count = 2};
        shared_visits = 0; shared_source_committed = false;
        sess->base.evaluation_control = c;
        if (caller) wl_columnar_ops_map_test_controlled_checkpoint =
                shared_owned_reader;
        else wl_columnar_filter_test_controlled_checkpoint =
                shared_owned_reader;
        int rc = caller ? col_op_map(&op,
                wl_columnar_eval_stack_cleanup_stack(shared_frame), sess)
            : wl_columnar_filter_op(&op,
                wl_columnar_eval_stack_cleanup_stack(shared_frame), sess);
        sess->base.evaluation_control = NULL;
        wl_columnar_ops_map_test_controlled_checkpoint = NULL;
        wl_columnar_filter_test_controlled_checkpoint = NULL;
        CHECK(rc == EBUSY && shared_visits == 1); finish(c, rc);
        CHECK(wl_columnar_eval_stack_cleanup_stack(
                shared_frame)->items[0].rel == source
            && wl_columnar_eval_stack_cleanup_result(shared_frame)->rel);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == EBUSY);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        release(c); join_session_destroy(sess);
    }
    for (unsigned stop = 0; stop < 2; stop++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 3, 3, false, sess->memory_governor);
        CHECK(session_add_rel(sess, source) == 0);
        shared_frame = filter_frame(sess); shared_is_map = true;
        wl_evaluation_control_t *c = control(0);
        CHECK(shared_dispatch(sess, shared_frame, c) == 0);
        shared_record =
            wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame);
        sess->base.evaluation_control = c;
        for (unsigned step = 0; step < 3; step++) {
            wl_plan_op_t op = {.op = step ==
                                   1 ? WL_PLAN_OP_FILTER : WL_PLAN_OP_MAP,
                               .project_count = 2};
            if (stop && step == 2) {
                shared_visits = 0; shared_growth = false;
                shared_reuse_reset = true;
                shared_keep_reader = shared_overflow = false;
                wl_columnar_relation_test_private_checkpoint =
                    shared_reset_stop;
            }
            int rc = step == 1 ? wl_columnar_filter_op(&op,
                    wl_columnar_eval_stack_cleanup_stack(shared_frame), sess)
                : col_op_map(&op,
                    wl_columnar_eval_stack_cleanup_stack(shared_frame), sess);
            CHECK(rc == (stop &&
                step == 2 ? WL_EVALUATION_CONTROL_CANCELLED : 0));
            CHECK(shared_record ==
                wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame));
            if (rc) CHECK(shared_visits == 1 && shared_record->phase ==
                    WL_COLUMNAR_RELATION_PRIVATE_RESETTING);
            else CHECK(shared_record->phase ==
                    WL_COLUMNAR_RELATION_PRIVATE_RETIRED
                    && wl_columnar_relation_private_batch_disposable(
                        shared_record));
        }
        sess->base.evaluation_control = NULL;
        if (!stop) {
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(shared_frame);
            eval_entry_t former;
            col_rel_t *old_pointer = shared_record->batch.relation;
            CHECK(eval_stack_pop_relation(stack, &former) == 0 && former.owned
                && former.rel == old_pointer);
            CHECK(eval_entry_dispose(&former) == 0);
            /* Retired metadata now really contains a freed relation pointer.
             * Only compare its address; actual owner lookup/reset must not read it. */
            CHECK(shared_record->batch.relation == old_pointer);
            CHECK(shared_dispatch(sess, shared_frame, c) == 0);
            CHECK(shared_record ==
                wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame)
                && shared_record->phase ==
                WL_COLUMNAR_RELATION_PRIVATE_RETIRED);
        }
        finish(c, stop ? WL_EVALUATION_CONTROL_CANCELLED : 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0);
        if (stop) {
            shared_frame = filter_frame(sess); retry(c, 0);
            CHECK(shared_dispatch(sess, shared_frame, c) == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0);
        }
        shared_reuse_reset = false;
        release(c); join_session_destroy(sess);
    }
}
static unsigned join_writer_pairs, join_writer_pairs_at_reset;
static bool join_writer_committed;
static void
join_writer_checkpoint(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *left)
{
    (void)progress;
    if (stage == 5) join_writer_pairs++;
    if (stage == 13 && shared_fault_stage == 13)
        shared_authority_fault(c,
            wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame), 13, 0);
    if (stage == 9) {
        CHECK(!c && group->committed); join_writer_committed = true;
    }
    if (stage == 14 && shared_keep_reader) {
        CHECK(!c && join_writer_committed && !group->descriptor_count &&
            !group->owner_count);
        CHECK(col_rel_source_reader_acquire(left, &join_retained_reader) == 0);
        shared_visits++;
    }
}
static void
join_writer_reset_stop(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *record, int stage, uint64_t progress)
{
    unsigned before = shared_visits;
    shared_reset_stop(c, record, stage, progress);
    if (shared_visits != before) join_writer_pairs_at_reset = join_writer_pairs;
}
static uint64_t
join_writer_ledger(wl_col_session_t *sess)
{
    return atomic_load(&sess->mem_ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION]);
}
static void
test_join_writer_authority_and_ledger(void)
{
    for (unsigned fault = 0; fault < 3; fault++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 3, 3, false, sess->memory_governor),
            *right = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, right) == 0); make_lazy(left);
        make_lazy(right);
        uint64_t baseline = string_reserved(sess->memory_governor),
            ledger = join_writer_ledger(sess);
        shared_frame = join_frame(sess, left, false);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_evaluation_control_t *c = control(0);
        shared_fault_stage = fault == 0 ? 2 : fault == 1 ? 3 : 13;
        shared_visits = 0; shared_keep_reader = false;
        wl_columnar_relation_test_private_checkpoint = shared_authority_fault;
        wl_columnar_join_test_controlled_checkpoint = join_writer_checkpoint;
        CHECK(join_dispatch(sess, shared_frame, &op, c) == EINVAL);
        finish(c, EINVAL);
        wl_columnar_relation_test_private_checkpoint = NULL;
        wl_columnar_join_test_controlled_checkpoint = NULL;
        shared_fault_stage = 0;
        CHECK(shared_visits == 1 &&
            shared_record->phase == WL_COLUMNAR_RELATION_PRIVATE_RETIRING
            && !shared_record->batch.active);
        col_rel_t *out =
            wl_columnar_eval_stack_cleanup_result(shared_frame)->rel;
        CHECK(out && out == shared_record->batch.relation &&
            out->mem_ledger == &sess->mem_ledger);
        CHECK(join_writer_ledger(sess) ==
            ledger + (uint64_t)out->ncols * out->capacity * sizeof(int64_t));
        CHECK(shared_writer->owner && !shared_writer->thread_valid
            && shared_writer->identity == (uintptr_t)shared_writer
            && atomic_load(&shared_writer->owner->state) ==
            WL_COLUMNAR_SOURCE_ACCESS_WRITER);
        lazy_unchanged(left); lazy_unchanged(right);
        source_unpinned(left, left); source_unpinned(right, right);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == EINVAL &&
            sess->cleanup_pending);
        CHECK(string_reserved(sess->memory_governor) > baseline &&
            join_writer_ledger(sess) > ledger);
        shared_writer->thread_valid = shared_thread_valid;
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        CHECK(string_reserved(sess->memory_governor) == baseline &&
            join_writer_ledger(sess) == ledger);
        CHECK(!sess->memory_budget_denied);
        CHECK(col_rel_storage_owner_ensure_initialized(left) == 0);
        shared_frame = join_frame(sess, left, false); retry(c, 0);
        CHECK(join_dispatch(sess, shared_frame, &op, c) == 0); finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0);
        release(c); destroy(left); join_session_destroy(sess);
    }
}
static void
test_join_writer_growth_reset(void)
{
    const char *keys[1] = {"col0"};
    for (unsigned keyed = 0; keyed < 2; keyed++)
        for (unsigned epoch = 1; epoch <= 2; epoch++)
            for (unsigned overflow = 0; overflow < 2; overflow++)
                for (unsigned retained = 0; retained < 2; retained++) {
                    wl_col_session_t *sess = join_session(),
                        *raw = join_session();
                    col_rel_t *left = relation(1, 1, 1, false,
                            sess->memory_governor),
                        *right = relation(2, 513, 513, false,
                            sess->memory_governor),
                        *rl = relation(1, 1, 1, false, raw->memory_governor),
                        *rr = relation(2, 513, 513, false,
                            raw->memory_governor);
                    for (unsigned i = 0; i < 513;
                        i++) right->columns[0][i] = rr->columns[0][i] = 0;
                    CHECK(session_add_rel(sess,
                        right) == 0 && session_add_rel(raw, rr) == 0);
                    make_lazy(left); make_lazy(right);
                    uint64_t baseline = string_reserved(sess->memory_governor);
                    shared_frame = join_frame(sess, left, false);
                    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN,
                                       .right_relation = "work",
                                       .delta_mode = WL_DELTA_FORCE_FULL,
                                       .key_count = keyed,
                                       .left_keys = keys, .right_keys = keys};
                    wl_evaluation_control_t *c = control(0);
                    shared_visits = shared_epoch = join_writer_pairs = 0;
                    shared_target_epoch = epoch; shared_fault_stage = 0;
                    shared_growth = shared_reuse_reset = false;
                    shared_overflow = overflow; shared_keep_reader = retained;
                    wl_columnar_relation_test_private_checkpoint =
                        join_writer_reset_stop;
                    wl_columnar_join_test_controlled_checkpoint =
                        join_writer_checkpoint;
                    int expected =
                        overflow ? EOVERFLOW : retained ? EBUSY :
                        WL_EVALUATION_CONTROL_CANCELLED;
                    CHECK(join_dispatch(sess, shared_frame, &op,
                        c) == expected); CHECK(finish(c, expected) > 0);
                    uint64_t output_peak = sess->join_output_peak,
                        ledger_peak = atomic_load(&sess->mem_ledger.peak_bytes);
                    CHECK(output_peak == ((uint64_t)COL_REL_INIT_CAP << epoch)
                        && ledger_peak > 0 && !sess->memory_budget_denied);
                    wl_columnar_join_test_controlled_checkpoint = NULL;
                    CHECK(shared_visits == 1 && shared_epoch == epoch
                        && join_writer_pairs == join_writer_pairs_at_reset
                        && join_writer_pairs ==
                        (((unsigned)COL_REL_INIT_CAP << epoch) + 1u));
                    CHECK(shared_record->phase ==
                        WL_COLUMNAR_RELATION_PRIVATE_RESETTING);
                    CHECK(
                        wl_columnar_relation_private_append_batch_begin_controlled
                            (shared_record, left, c) == EBUSY);
                    retry(c, 0);
                    wl_columnar_relation_private_batch_t *found = NULL;
                    CHECK(wl_columnar_eval_stack_cleanup_private_batch_owner(
                            sess,
                            wl_columnar_eval_stack_cleanup_stack(shared_frame),
                            &found, c) == EBUSY && !found);
                    finish(c, EBUSY);
                    lazy_unchanged(left); lazy_unchanged(right);
                    source_unpinned(left, left); source_unpinned(right, right);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(
                            &shared_frame) == (retained ? EBUSY : 0));
                    if (retained) {
                        CHECK(sess->cleanup_pending && join_writer_ledger(sess)
                            == (uint64_t)shared_output->ncols *
                            shared_output->capacity * sizeof(int64_t));
                        CHECK(string_reserved(sess->memory_governor) >
                            baseline);
                        CHECK(col_rel_source_reader_release(
                                &join_retained_reader) == 0);
                        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
                    }
                    CHECK(string_reserved(sess->memory_governor) == baseline &&
                        !join_writer_ledger(sess));
                    CHECK(sess->join_output_peak == output_peak
                        && atomic_load(&sess->mem_ledger.peak_bytes) ==
                        ledger_peak);
                    shared_keep_reader = false;
                    shared_frame = join_frame(sess, left, false); retry(c, 0);
                    CHECK(join_dispatch(sess, shared_frame, &op, c) == 0);
                    finish(c, 0);
                    wl_columnar_eval_stack_cleanup_frame_t *reference =
                        join_frame(raw, rl, false);
                    CHECK(join_dispatch(raw, reference, &op, NULL) == 0);
                    join_multiset_equal(join_output(shared_frame),
                        join_output(reference));
                    CHECK(join_output(shared_frame)->nrows == 513);
                    CHECK(wl_columnar_eval_stack_cleanup_finish(
                            &shared_frame) == 0
                        && wl_columnar_eval_stack_cleanup_finish(&reference) ==
                        0);
                    CHECK(!join_writer_ledger(sess));
                    release(c); destroy(left); destroy(rl);
                    join_session_destroy(sess); join_session_destroy(raw);
                }
}
static void
test_join_writer_owned_and_reuse(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *left = relation(1, 3, 3, false, sess->memory_governor),
            *right = relation(1, 1, 1, false, sess->memory_governor);
        CHECK(session_add_rel(sess, right) == 0);
        if (!mode) make_lazy(left);
        shared_frame = join_frame(sess, left, !mode);
        wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "work",
                           .delta_mode = WL_DELTA_FORCE_FULL};
        wl_evaluation_control_t *c = control(0);
        shared_keep_reader = !mode; shared_visits = 0; shared_fault_stage = 0;
        join_writer_committed = false;
        wl_columnar_join_test_controlled_checkpoint = join_writer_checkpoint;
        int expected = !mode ? EBUSY : 0;
        CHECK(join_dispatch(sess, shared_frame, &op, c) == expected);
        wl_columnar_join_test_controlled_checkpoint = NULL;
        if (!mode) {
            finish(c, EBUSY);
            CHECK(join_writer_committed && shared_visits == 1);
            CHECK(wl_columnar_eval_stack_cleanup_stack(
                    shared_frame)->items[0].rel == left
                && wl_columnar_eval_stack_cleanup_result(shared_frame)->rel);
            CHECK(join_writer_ledger(sess) &&
                string_reserved(sess->memory_governor));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) ==
                EBUSY);
            /* Independent retired output is physically disposable while the
             * real owned input reader still retains the pending frame. */
            CHECK(sess->cleanup_pending && !join_writer_ledger(sess)
                && string_reserved(sess->memory_governor));
            CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0 &&
                !join_writer_ledger(sess));
        } else {
            wl_columnar_relation_private_batch_t *record =
                wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame);
            CHECK(join_dispatch(sess, shared_frame, &op, c) == 0);
            CHECK(wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame) ==
                record);
            eval_entry_t former;
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(shared_frame);
            CHECK(eval_stack_pop_relation(stack, &former) == 0 && former.owned);
            CHECK(eval_entry_dispose(&former) == 0 &&
                !join_writer_ledger(sess));
            CHECK(eval_stack_push(stack, left, false) == 0);
            if (mode == 2) {
                shared_visits = 0; shared_growth = false;
                shared_reuse_reset = true;
                shared_keep_reader = shared_overflow = false;
                wl_columnar_relation_test_private_checkpoint =
                    shared_reset_stop;
            }
            expected = mode == 2 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
            CHECK(join_dispatch(sess, shared_frame, &op, c) == expected);
            finish(c, expected);
            CHECK(wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame) ==
                record);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0 &&
                !join_writer_ledger(sess));
            if (mode == 2) {
                CHECK(shared_visits == 1); shared_reuse_reset = false;
                shared_frame = join_frame(sess, left, false); retry(c, 0);
                CHECK(join_dispatch(sess, shared_frame, &op, c) == 0);
                finish(c, 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) ==
                    0);
            }
            destroy(left);
        }
        shared_keep_reader = false; release(c); join_session_destroy(sess);
    }
}
/* Actual VARIABLE -> controlled borrowed CONSOLIDATE, with raw twins. */
static int consolidate_last_stage;
static uint64_t consolidate_last_progress;
static void
consolidate_diagnose(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)c; (void)record; (void)r;
    consolidate_last_stage = stage; consolidate_last_progress = progress;
}
static int
consolidate_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_evaluation_control_t *c)
{
    wl_plan_op_t ops[2] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    return compiled_dispatch(sess, frame, ops, 2, c);
}
static uint64_t
consolidate_source_hash(col_rel_t *r)
{
    uint64_t h = UINT64_C(14695981039346656037);
    for (uint32_t col = 0; col < r->ncols; col++)
        for (uint32_t row = 0; row < r->nrows; row++) {
            h ^= (uint64_t)r->columns[col][row]; h *= UINT64_C(1099511628211);
        }
    return h;
}
static void
consolidate_populate(col_rel_t *r, unsigned order, bool floating)
{
    wirelog_column_type_t *types = calloc(r->ncols ? r->ncols : 1u,
            sizeof(*types));
    CHECK(types);
    for (uint32_t col = 0; col < r->ncols; col++)
        types[col] = floating ? WIRELOG_TYPE_FLOAT
            : col == 1 ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64;
    /* Establish FLOAT schema on the empty private fixture before population;
     * the public API rightly refuses retagging an implicit numeric payload. */
    uint32_t rows = r->nrows;
    r->nrows = 0;
    if (r->ncols) CHECK(col_rel_set_column_types(r, types, r->ncols) == 0);
    r->nrows = rows;
    free(types);
    for (uint32_t col = 0; col < r->ncols; col++) {
        r->column_types[col] = floating ? WIRELOG_TYPE_FLOAT
            : col == 1 ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64;
        for (uint32_t row = 0; row < r->nrows; row++) {
            int64_t v = order == 0 ? row : order == 1 ? r->nrows - row
                : order == 2 ? (row * 37u) % (r->nrows ? r->nrows : 1u) : 0;
            if (order == 2) v /= 2;
            if (!floating && order == 2 && col != 1 &&
                row % 11u == 0) v = INT64_MIN;
            if (!floating && order == 2 && col != 1 &&
                row % 13u == 0) v = INT64_MAX;
            if (floating) {
                double value = row % 7u == 0 ? -0.0 : (double)v / 2.0;
                if (row == 1) value = DBL_MAX;
                if (row == 2) value = -DBL_MAX;
                memcpy(&r->columns[col][row], &value, sizeof(value));
            } else r->columns[col][row] = v;
        }
    }
}
static void
test_consolidate_actual_twins(void)
{
    const uint32_t sizes[] = {0, 1, 2, 31, 32, 33, 513, 10000};
    for (unsigned item = 0; item < sizeof(sizes) / sizeof(sizes[0]); item++)
        for (unsigned order = 0; order < 4; order++) {
            wl_col_session_t *sess = join_session(), *raw = join_session();
            uint32_t nr = sizes[item];
            col_rel_t *source = relation(2, nr ? nr : 1, nr, false,
                    sess->memory_governor);
            col_rel_t *ref = relation(2, nr ? nr : 1, nr, false,
                    raw->memory_governor);
            consolidate_populate(source, order, false);
            consolidate_populate(ref, order, false);
            CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
                ref) == 0);
            make_lazy(source);
            uint64_t hash = consolidate_source_hash(source),
                view = source->view_generation;
            uint64_t baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
                *raw_frame = filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            wl_columnar_consolidate_test_checkpoint = consolidate_diagnose;
            int actual = consolidate_dispatch(sess, frame, c);
            wl_columnar_consolidate_test_checkpoint = NULL;
            if (actual) fprintf(stderr,
                    "consolidate twins nr=%u order=%u rc=%d stage=%d progress=%llu\n",
                    nr, order,
                    actual, consolidate_last_stage,
                    (unsigned long long)consolidate_last_progress);
            CHECK(actual == 0); finish(c, 0);
            CHECK(consolidate_dispatch(raw, raw_frame, NULL) == 0);
            eval_stack_t *st = wl_columnar_eval_stack_cleanup_stack(frame);
            eval_stack_t *rt = wl_columnar_eval_stack_cleanup_stack(raw_frame);
            CHECK(st->top == 1 && rt->top == 1);
            col_rel_t *out = st->items[0].rel, *expected = rt->items[0].rel;
            CHECK(out->ncols == expected->ncols &&
                out->nrows == expected->nrows);
            for (uint32_t col = 0; col < out->ncols; col++) {
                CHECK(out->column_types[col] == expected->column_types[col]);
                for (uint32_t row = 0; row < out->nrows; row++)
                    CHECK(out->columns[col][row] ==
                        expected->columns[col][row]);
            }
            CHECK(source->nrows == nr && source->view_generation == view
                && consolidate_source_hash(source) == hash);
            if (nr <= 1) {
                CHECK(out == source && !st->items[0].owned);
                lazy_unchanged(source);
            }else CHECK(out != source && st->items[0].owned &&
                    out->run_count == 1
                    && out->run_ends[0] == out->nrows &&
                    out->sorted_nrows == out->nrows);
            source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && wl_columnar_eval_stack_cleanup_finish(&raw_frame) == 0);
            CHECK(string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
    /* Orthogonal wide/zero-arity and small FLOAT insertion cases. */
    for (unsigned kind = 0; kind < 3; kind++) {
        wl_col_session_t *sess = join_session(), *raw = join_session();
        uint32_t nc = kind == 0 ? 300u : kind == 1 ? 0u : 2u;
        uint32_t nr = kind == 2 ? 31u : 33u;
        col_rel_t *source = relation(nc, nr, nr, false, sess->memory_governor);
        col_rel_t *ref = relation(nc, nr, nr, false, raw->memory_governor);
        consolidate_populate(source, 2, kind == 2);
        consolidate_populate(ref, 2, kind == 2);
        CHECK(session_add_rel(sess, source) == 0 && session_add_rel(raw,
            ref) == 0);
        uint64_t hash = consolidate_source_hash(source);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
            *rf = filter_frame(raw);
        wl_evaluation_control_t *c = control(0);
        CHECK(consolidate_dispatch(sess, frame, c) == 0); finish(c, 0);
        CHECK(consolidate_dispatch(raw, rf, NULL) == 0);
        col_rel_t *out = join_output(frame), *expected = join_output(rf);
        CHECK(out->nrows == expected->nrows && out->ncols == nc);
        for (uint32_t col = 0; col < nc; col++) for (uint32_t row = 0;
                row < out->nrows; row++)
                CHECK(out->columns[col][row] == expected->columns[col][row]);
        CHECK(consolidate_source_hash(source) == hash);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && wl_columnar_eval_stack_cleanup_finish(&rf) == 0);
        release(c); join_session_destroy(sess); join_session_destroy(raw);
    }
}

static int consolidate_stop_stage;
static uint64_t consolidate_stop_progress;
static unsigned consolidate_stop_visits;
static bool consolidate_stop_overflow;
static void
consolidate_stop(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)record; (void)r;
    if (!c || stage != consolidate_stop_stage ||
        progress < consolidate_stop_progress
        || consolidate_stop_visits) return;
    consolidate_stop_visits++;
    if (consolidate_stop_overflow) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_consolidate_reached_stops(void)
{
    const struct {int stage; uint64_t progress; uint32_t width, rows;
                  bool floating;} cases[] = {
        {20, 128, 2, 513, false}, {27, 300, 2, 513, false},
        {28, 300, 2, 513, false},
        {2, 300, 300, 2, false}, {3, 300, 300, 2, false},
        {4, 300, 300, 2, false},
        {5, 300, 300, 3, true}, {6, 300, 300, 3, true}, {7, 300, 300, 3, true},
        {8, 300, 2, 513, false}, {9, 300, 2, 513, false},
        {10, 128, 2, 513, false},
        {11, 300, 2, 513, false}, {12, 128, 2, 513, false},
        {13, 300, 2, 513, false},
        {14, 300, 2, 513, false}, {15, 300, 2, 513, false},
        {22, 300, 2, 513, false},
        {23, 300, 2, 513, false}, {24, 0, 2, 513, false},
        {30, 0, 2, 513, false},
        {31, 0, 2, 513, false}, {33, 0, 2, 513, false}
    };
    for (unsigned item = 0; item < sizeof(cases) / sizeof(cases[0]); item++)
        for (unsigned overflow = 0; overflow < 2; overflow++) {
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(cases[item].width, cases[item].rows,
                    cases[item].rows, false, sess->memory_governor);
            consolidate_populate(source, cases[item].floating ? 1u : 2u,
                cases[item].floating);
            if (cases[item].stage == 4)
                for (uint32_t col = 0; col < source->ncols; col++)
                    source->columns[col][1] = source->columns[col][0];
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint64_t hash = consolidate_source_hash(source),
                view = source->view_generation;
            uint64_t storage = source->storage_generation,
                baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            consolidate_stop_stage = cases[item].stage;
            consolidate_stop_progress = cases[item].progress;
            consolidate_stop_visits = 0; consolidate_stop_overflow = overflow;
            wl_columnar_consolidate_test_checkpoint = consolidate_stop;
            int actual = consolidate_dispatch(sess, frame, c);
            wl_columnar_consolidate_test_checkpoint = NULL;
            int expected =
                overflow ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            if (actual != expected || !consolidate_stop_visits)
                fprintf(stderr,
                    "consolidate stop stage=%d progress=%llu overflow=%u rc=%d visits=%u\n",
                    cases[item].stage, (unsigned long long)cases[item].progress,
                    overflow, actual, consolidate_stop_visits);
            CHECK(actual == expected && consolidate_stop_visits == 1);
            finish(c, expected);
            CHECK(consolidate_source_hash(source) == hash &&
                source->view_generation == view
                && source->storage_generation == storage &&
                source->nrows == cases[item].rows);
            lazy_unchanged(source); source_unpinned(source, source);
            eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
            CHECK(stack->top == 1 && stack->items[0].rel == source &&
                !stack->items[0].owned);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && string_reserved(sess->memory_governor) == baseline);
            retry(c, 0); frame = filter_frame(sess);
            CHECK(consolidate_dispatch(sess, frame, c) == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess);
        }
}
static unsigned consolidate_constructor_calls;
static void
consolidate_constructor_count(wl_evaluation_control_t *c, int stage,
    uint64_t progress)
{
    (void)c; (void)stage; (void)progress;
    consolidate_constructor_calls++;
}
static void
test_consolidate_original_noop(void)
{
    for (unsigned rows = 0; rows <= 1; rows++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 1, rows, false, sess->memory_governor);
        consolidate_populate(source, 0, true); make_lazy(source);
        source->sorted_nrows = 0; source->run_count = 2;
        source->run_ends[0] = 77; source->run_ends[1] = 88;
        wl_columnar_eval_stack_cleanup_frame_t *frame = join_frame(sess, source,
                false);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
        uint32_t *segments = malloc(3 * sizeof(*segments)); CHECK(segments);
        segments[0] = 7; segments[1] = 8; segments[2] = 9;
        stack->items[0].seg_boundaries = segments;
        stack->items[0].seg_count = 2;
        uint64_t hash = consolidate_source_hash(source),
            view = source->view_generation;
        uint64_t storage = source->storage_generation,
            baseline = string_reserved(sess->memory_governor);
        wl_evaluation_control_t *c = control(0);
        consolidate_constructor_calls = 0;
        wl_columnar_relation_test_constructor_checkpoint =
            consolidate_constructor_count;
        wl_columnar_consolidate_test_fail_allocation = 0;
        sess->base.evaluation_control = c;
        CHECK(col_op_consolidate(stack, sess) == 0);
        sess->base.evaluation_control = NULL; finish(c, 0);
        wl_columnar_relation_test_constructor_checkpoint = NULL;
        CHECK(!consolidate_constructor_calls &&
            wl_columnar_consolidate_test_fail_allocation == 0);
        wl_columnar_consolidate_test_fail_allocation = -1;
        CHECK(stack->top == 1 && stack->items[0].rel == source &&
            !stack->items[0].owned
            && stack->items[0].seg_boundaries == segments &&
            stack->items[0].seg_count == 2
            && segments[0] == 7 && segments[1] == 8 && segments[2] == 9
            && source->run_count == 2 && source->run_ends[0] == 77 &&
            source->run_ends[1] == 88
            && source->sorted_nrows == 0 && source->view_generation == view
            && source->storage_generation == storage &&
            consolidate_source_hash(source) == hash);
        lazy_unchanged(source); source_unpinned(source, source);
        CHECK(string_reserved(sess->memory_governor) == baseline);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c);
        CHECK(col_rel_storage_owner_ensure_initialized(source) == 0);
        destroy(source); join_session_destroy(sess);
    }
}

static unsigned consolidate_provenance_checks;
static uint64_t consolidate_clone_nonce;
static void
consolidate_provenance(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)progress;
    if (stage == 29) {
        wl_columnar_relation_private_batch_t *writer =
            wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame);
        CHECK(writer && writer->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD);
        consolidate_clone_nonce = writer->batch.single.lease.acquisition_nonce;
        return;
    }
    if (stage != 30 && stage != 31) return;
    wl_columnar_relation_radix_sequence_t *seq = &record->sequence;
    wl_columnar_relation_mutation_lease_t *lease = seq->lease;
    CHECK(lease && lease->acquisition_nonce != consolidate_clone_nonce
        && col_rel_mutation_lease_validate(lease, r) == 0);
    uint64_t hash = consolidate_source_hash(r), view = r->view_generation;
    bool published = lease->set->published;
    if (stage == 31) {
        CHECK(seq->consumed);
        CHECK(wl_columnar_relation_radix_sequence_execute_with_lease_controlled(
                seq, lease, c) == EINVAL);
        CHECK(consolidate_source_hash(r) == hash && r->view_generation == view
            && lease->set->published == published);
        consolidate_provenance_checks++;
        return;
    }
    CHECK(seq->prepared && !seq->consumed && seq->needs_sort_count == 1);
    for (unsigned kind = 0; kind < 9; kind++) {
        int64_t **columns = r->columns;
        uint32_t capacity = r->capacity, boundary = seq->boundaries[1];
        wirelog_column_type_t type = r->column_types[0];
        uint64_t storage = r->storage_generation,
            nonce = lease->acquisition_nonce;
        size_t count = lease->set->lease_count;
        uint32_t *permutation = seq->workspace.perm_a;
        if (kind == 0) r->view_generation++;
        else if (kind == 1) r->storage_generation++;
        else if (kind == 2) r->columns = NULL;
        else if (kind == 3) r->column_types[0] = WIRELOG_TYPE_BOOL;
        else if (kind == 4) r->capacity = r->nrows - 1u;
        else if (kind == 5) lease->acquisition_nonce = consolidate_clone_nonce;
        else if (kind == 6) lease->set->lease_count = 2;
        else if (kind == 7) seq->boundaries[1]--;
        else seq->workspace.perm_a = NULL;
        CHECK(wl_columnar_relation_radix_sequence_execute_with_lease_controlled(
                seq, lease, c) == EINVAL);
        r->view_generation = view; r->storage_generation = storage;
        r->columns = columns; r->column_types[0] = type; r->capacity = capacity;
        lease->acquisition_nonce = nonce; lease->set->lease_count = count;
        seq->boundaries[1] = boundary; seq->workspace.perm_a = permutation;
        CHECK(!seq->consumed && consolidate_source_hash(r) == hash
            && lease->set->published == published &&
            col_rel_mutation_lease_validate(lease, r) == 0);
        consolidate_provenance_checks++;
    }
}
static void
test_consolidate_prepared_provenance(void)
{
    wl_col_session_t *sess = join_session();
    col_rel_t *source = relation(2, 513, 513, false, sess->memory_governor);
    consolidate_populate(source, 2, false);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t source_hash = consolidate_source_hash(source);
    shared_frame = filter_frame(sess); wl_evaluation_control_t *c = control(0);
    consolidate_provenance_checks = 0; consolidate_clone_nonce = 0;
    wl_columnar_consolidate_test_checkpoint = consolidate_provenance;
    CHECK(consolidate_dispatch(sess, shared_frame, c) == 0); finish(c, 0);
    wl_columnar_consolidate_test_checkpoint = NULL;
    CHECK(consolidate_provenance_checks == 10 &&
        consolidate_source_hash(source) == source_hash);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0);
    release(c); join_session_destroy(sess);
}
static unsigned consolidate_pool_zero_visits;
static void
consolidate_pool_zero_stop(wl_evaluation_control_t *c, int stage,
    uint64_t progress)
{
    if (stage != WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ZERO ||
        progress < 128) return;
    consolidate_pool_zero_visits++;
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    wl_evaluation_control_request_cancel(c);
}
static bool consolidate_pool_partial_acquired;
static void
consolidate_pool_partial_stop(wl_evaluation_control_t *c, int stage,
    uint64_t progress)
{
    if (stage == WL_COLUMNAR_RELATION_CONSTRUCTOR_POOL_ACQUIRED)
        consolidate_pool_partial_acquired = true;
    if (stage != WL_COLUMNAR_RELATION_CONSTRUCTOR_NAME ||
        progress != sizeof("$consol")) return;
    CHECK(consolidate_pool_partial_acquired);
    consolidate_pool_zero_visits++;
    wl_columnar_relation_test_constructor_checkpoint = NULL;
    wl_evaluation_control_request_cancel(c);
}
static void
test_consolidate_actual_pools(void)
{
    for (unsigned mode = 0; mode < 5; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 33, 33, false, sess->memory_governor);
        consolidate_populate(source, 2, false);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        delta_pool_t *pool = mode ? delta_pool_create(1, sizeof(col_rel_t),
                64) : NULL;
        CHECK(!mode || pool); sess->delta_pool = pool;
        col_rel_t *prior = NULL;
        if (mode == 2) {
            CHECK(wl_columnar_relation_pool_new_like_governed_checked(&prior,
                pool, "prior", source, sess->memory_governor) == 0);
            prior->columns[0][0] = 987;
            make_lazy(source);
        }
        if (mode >= 3) memset(pool->slab, 0xa5, pool->slot_size);
        uint32_t slot_used = pool ? pool->slot_used : 0;
        uint64_t source_hash = consolidate_source_hash(source),
            baseline = string_reserved(sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        consolidate_pool_zero_visits = 0;
        consolidate_pool_partial_acquired = false;
        if (mode ==
            3) wl_columnar_relation_test_constructor_checkpoint =
                consolidate_pool_zero_stop;
        if (mode ==
            4) wl_columnar_relation_test_constructor_checkpoint =
                consolidate_pool_partial_stop;
        int expected = mode >= 3 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(consolidate_dispatch(sess, frame, c) == expected);
        finish(c, expected);
        wl_columnar_relation_test_constructor_checkpoint = NULL;
        if (mode == 3) {
            CHECK(consolidate_pool_zero_visits == 1 &&
                pool->slot_used == slot_used);
            for (size_t byte = 0; byte < 128;
                byte++) CHECK(pool->slab[byte] == 0);
            for (size_t byte = 128; byte < pool->slot_size;
                byte++) CHECK((unsigned char)pool->slab[byte] == 0xa5);
            lazy_unchanged(source);
        } else if (mode == 4) {
            CHECK(consolidate_pool_zero_visits == 1 &&
                consolidate_pool_partial_acquired
                && pool->slot_used == slot_used);
            col_rel_t *slot = (col_rel_t *)pool->slab;
            CHECK(!slot->name && !slot->columns && !slot->column_types
                && !slot->col_names && !slot->row_scratch && !slot->timestamps
                && !slot->nrows && !slot->ncols && !slot->capacity
                && !slot->schema_ok && !slot->relation_identity &&
                !slot->storage_owner);
            wl_columnar_memory_reservation_t *tokens[] = {
                &slot->descriptor_reservation, &slot->pool_name_reservation,
                &slot->metadata_reservation, &slot->retained_reservation,
                &slot->dedup_reservation
            };
            for (unsigned token = 0; token < 5; token++) CHECK(
                    tokens[token]->identity == tokens[token] &&
                    !tokens[token]->governor
                    && !tokens[token]->bytes &&
                    !tokens[token]->replacement_bytes
                    && !tokens[token]->transition_kind &&
                    !atomic_load(&tokens[token]->owner_bits)
                    && atomic_load(&tokens[token]->state) ==
                    WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
            lazy_unchanged(source);
        } else {
            col_rel_t *out = join_output(frame);
            CHECK(out->capacity == COL_REL_INIT_CAP);
            if (mode == 1) CHECK((char *)out == pool->slab &&
                    pool->slot_used == 1);
            if (mode == 2) CHECK((char *)out != pool->slab &&
                    pool->slot_used == slot_used
                    && prior->columns[0][0] == 987);
        }
        CHECK(consolidate_source_hash(source) == source_hash);
        source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
            && string_reserved(sess->memory_governor) == baseline);
        if (prior) destroy(prior);
        sess->delta_pool = NULL; if (pool) delta_pool_destroy(pool);
        release(c); join_session_destroy(sess);
    }
}

static unsigned consolidate_fault_kind, consolidate_fault_visits;
static wl_columnar_source_access_writer_t *consolidate_fault_writer;
static bool consolidate_fault_thread_valid;
static wl_columnar_relation_source_group_t *consolidate_fault_group;
static void
consolidate_authority_fault(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *writer, int stage, uint64_t progress)
{
    if (consolidate_fault_visits || (consolidate_fault_kind == 0 ? stage != 2
        : consolidate_fault_kind == 2 ? stage != 4 ||
        progress != 128 : true)) return;
    consolidate_fault_writer = &writer->batch.single.descriptor.writer;
    CHECK(consolidate_fault_writer->owner &&
        consolidate_fault_writer->identity ==
        (uintptr_t)consolidate_fault_writer
        && consolidate_fault_writer->thread_valid);
    consolidate_fault_thread_valid = consolidate_fault_writer->thread_valid;
    /* Intentionally malformed authority validation, not healthy contention. */
    consolidate_fault_writer->thread_valid = false;
    consolidate_fault_visits++; wl_evaluation_control_request_cancel(c);
}
static void
consolidate_owner_checkpoint(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)progress; (void)record; (void)r;
    if (consolidate_fault_visits) return;
    if (consolidate_fault_kind == 1 && stage == 29) {
        wl_columnar_relation_private_batch_t *writer =
            wl_columnar_eval_stack_cleanup_reduce_batch(shared_frame);
        CHECK(writer && writer->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD);
        consolidate_fault_writer = &writer->batch.single.owners[0].writer;
    } else if (consolidate_fault_kind == 3 && stage == 26) {
        CHECK(consolidate_fault_group->descriptor_count == 1
            && consolidate_fault_group->descriptors[0].journal);
        consolidate_fault_writer =
            &consolidate_fault_group->descriptors[0].writer;
    } else return;
    CHECK(consolidate_fault_writer->owner &&
        consolidate_fault_writer->identity ==
        (uintptr_t)consolidate_fault_writer
        && consolidate_fault_writer->thread_valid);
    consolidate_fault_thread_valid = consolidate_fault_writer->thread_valid;
    consolidate_fault_writer->thread_valid = false;
    consolidate_fault_visits++; wl_evaluation_control_request_cancel(c);
}
static void
test_consolidate_authority_retention(void)
{
    for (unsigned kind = 0; kind < 4; kind++) {
        wl_col_session_t *sess = join_session();
        uint32_t rows = kind == 3 ? 1u : 513u;
        col_rel_t *source = relation(2, rows, rows, false,
                sess->memory_governor);
        consolidate_populate(source, 2, false);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t source_hash = consolidate_source_hash(source),
            baseline = string_reserved(sess->memory_governor);
        shared_frame = filter_frame(sess);
        eval_entry_t *result;
        CHECK(wl_columnar_eval_stack_cleanup_join_owner(sess,
            wl_columnar_eval_stack_cleanup_stack(shared_frame),
            &consolidate_fault_group, &result) == 0);
        wl_evaluation_control_t *c = control(0);
        consolidate_fault_kind = kind; consolidate_fault_visits = 0;
        consolidate_fault_writer = NULL;
        wl_columnar_relation_test_private_checkpoint =
            consolidate_authority_fault;
        wl_columnar_consolidate_test_checkpoint = consolidate_owner_checkpoint;
        CHECK(consolidate_dispatch(sess, shared_frame, c) == EINVAL);
        finish(c, EINVAL);
        wl_columnar_relation_test_private_checkpoint = NULL;
        wl_columnar_consolidate_test_checkpoint = NULL;
        CHECK(consolidate_fault_visits == 1 && consolidate_fault_writer->owner
            && !consolidate_fault_writer->thread_valid
            && consolidate_fault_writer->identity ==
            (uintptr_t)consolidate_fault_writer
            && atomic_load(&consolidate_fault_writer->owner->state) ==
            WL_COLUMNAR_SOURCE_ACCESS_WRITER);
        CHECK(consolidate_source_hash(source) == source_hash);
        eval_stack_t *stack =
            wl_columnar_eval_stack_cleanup_stack(shared_frame);
        CHECK(stack->top == 1 && stack->items[0].rel == source &&
            !stack->items[0].owned);
        if (kind == 3) CHECK(source->storage_owner == source &&
                consolidate_fault_group->descriptors[0].journal);
        else CHECK(result->rel && result->owned);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == EINVAL
            && !shared_frame && sess->cleanup_pending);
        CHECK(string_reserved(sess->memory_governor) > baseline);
        consolidate_fault_writer->thread_valid = consolidate_fault_thread_valid;
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0 &&
            !sess->cleanup_pending
            && string_reserved(sess->memory_governor) == baseline);
        lazy_unchanged(source); source_unpinned(source, source);
        shared_frame = filter_frame(sess); retry(c, 0);
        CHECK(consolidate_dispatch(sess, shared_frame, c) == 0); finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&shared_frame) == 0);
        release(c); join_session_destroy(sess);
    }
}
static unsigned consolidate_resource_kind, consolidate_resource_visits;
static uint64_t consolidate_resource_usable;
static void
consolidate_resource_checkpoint(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)c; (void)progress; (void)record;
    if (stage != 37 || consolidate_resource_visits) return;
    consolidate_resource_visits++;
    if (consolidate_resource_kind ==
        0) r->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    else {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(r->memory_governor);
        consolidate_resource_usable = atomic_load(&g->usable_bytes);
        atomic_store(&g->usable_bytes, wl_columnar_memory_reserved(g));
    }
}
static void
test_consolidate_resource_failures(void)
{
    for (unsigned mode = 0; mode < 11; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 513, 513, false, sess->memory_governor);
        consolidate_populate(source, 2, false);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t source_hash = consolidate_source_hash(source),
            baseline = string_reserved(sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        if (mode < 8) wl_columnar_consolidate_test_fail_allocation = (int)mode;
        else if (mode == 8) wl_columnar_consolidate_test_fail_commit = true;
        else {
            consolidate_resource_kind = mode - 9u;
            consolidate_resource_visits = 0;
            wl_columnar_consolidate_test_checkpoint =
                consolidate_resource_checkpoint;
        }
        int expected = mode < 9 ? ENOMEM : mode == 9 ? EOVERFLOW : ENOSPC;
        int actual = consolidate_dispatch(sess, frame, c);
        if (actual != expected) fprintf(stderr,
                "consolidate resource mode=%u rc=%d expected=%d\n", mode,
                actual, expected);
        CHECK(actual == expected); finish(c, expected);
        if (mode ==
            10) atomic_store(&wl_columnar_memory_governor_ref_get(
                    sess->memory_governor)->usable_bytes,
                consolidate_resource_usable);
        wl_columnar_consolidate_test_checkpoint = NULL;
        CHECK(wl_columnar_consolidate_test_fail_allocation == -1 &&
            !wl_columnar_consolidate_test_fail_commit);
        CHECK(sess->memory_budget_denied == (mode == 10));
        CHECK(consolidate_source_hash(source) == source_hash);
        lazy_unchanged(source); source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
            string_reserved(sess->memory_governor) == baseline);
        retry(c, 0); frame = filter_frame(sess);
        CHECK(consolidate_dispatch(sess, frame, c) == 0); finish(c, 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); join_session_destroy(sess);
    }
}

static wl_columnar_consolidate_record_t *consolidate_credit_record;
static const void *consolidate_credit_identity;
static unsigned consolidate_credit_visits;
static void
consolidate_credit_fault(wl_evaluation_control_t *c, int stage,
    uint64_t progress,
    wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)progress; (void)r;
    if (stage != 37 || consolidate_credit_visits) return;
    CHECK(record->ready && record->phase == WL_COLUMNAR_CONSOLIDATE_PREPARING
        && record->merge_admission_active && record->starts && record->ends
        && record->merge_admission.bytes == 8
        && record->merge_admission.identity == &record->merge_admission
        && atomic_load(&record->merge_admission.state) ==
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
    consolidate_credit_record = record;
    consolidate_credit_identity = record->merge_admission.identity;
    /* Deliberately malformed reservation validation, not healthy contention. */
    record->merge_admission.identity = NULL;
    consolidate_credit_visits++; wl_evaluation_control_request_cancel(c);
}
static void
test_consolidate_credit_retention(void)
{
    wl_col_session_t *sess = join_session();
    col_rel_t *source = relation(2, 33, 33, false, sess->memory_governor);
    consolidate_populate(source, 2, false);
    CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
    uint64_t baseline = string_reserved(sess->memory_governor),
        source_hash = consolidate_source_hash(source);
    wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
    wl_evaluation_control_t *c = control(0);
    consolidate_credit_visits = 0;
    wl_columnar_consolidate_test_checkpoint = consolidate_credit_fault;
    CHECK(consolidate_dispatch(sess, frame, c) == EINVAL);
    wl_columnar_consolidate_test_checkpoint = NULL;
    wl_columnar_consolidate_record_t *record = consolidate_credit_record;
    CHECK(consolidate_credit_visits == 1 &&
        record->phase == WL_COLUMNAR_CONSOLIDATE_RETIRING
        && !record->starts && !record->ends && record->merge_admission_active
        && record->merge_admission.bytes == 8 &&
        !record->merge_admission.identity
        && atomic_load(&record->merge_admission.state) ==
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && !record->sequence.boundaries && !record->sequence.needs_sort
        && !record->sequence.metadata_admission_active &&
        !record->sequence.workspace.admission_active);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    CHECK(stack->top == 1 && stack->items[0].rel == source &&
        !stack->items[0].owned);
    CHECK(wl_columnar_eval_stack_cleanup_result(frame)->owned
        && wl_columnar_eval_stack_cleanup_result(frame)->rel);
    CHECK(consolidate_source_hash(source) == source_hash);
    lazy_unchanged(source);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL && !frame
        && sess->cleanup_pending && sess->cleanup_pending_count == 1
        && string_reserved(sess->memory_governor) > baseline);
    CHECK(record->merge_admission.identity == NULL
        && consolidate_credit_identity == &record->merge_admission);
    record->merge_admission.identity = consolidate_credit_identity;
    CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0 &&
        !sess->cleanup_pending
        && !sess->cleanup_pending_count &&
        string_reserved(sess->memory_governor) == baseline);
    CHECK(wl_evaluation_control_charge(c,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    finish(c, EINVAL); release(c); join_session_destroy(sess);
}
static unsigned consolidate_growth_epoch, consolidate_growth_target,
    consolidate_growth_visits;
static bool consolidate_growth_reset, consolidate_growth_overflow;
static wl_columnar_relation_private_batch_t *consolidate_growth_record;
static int64_t *consolidate_row_scratch;
static void
consolidate_growth_stop(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *writer, int stage, uint64_t progress)
{
    if (stage == 4) {
        consolidate_growth_epoch++;
        CHECK(progress ==
            ((uint64_t)COL_REL_INIT_CAP << consolidate_growth_epoch));
        CHECK(writer->batch.relation->row_scratch == consolidate_row_scratch);
        if (consolidate_growth_epoch ==
            consolidate_growth_target) consolidate_growth_reset = true;
        return;
    }
    if (stage != 1 || progress != 128 || !consolidate_growth_reset ||
        consolidate_growth_visits) return;
    CHECK(writer->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING &&
        !writer->batch.active);
    consolidate_growth_record = writer; consolidate_growth_visits++;
    if (consolidate_growth_overflow) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
consolidate_scratch_observe(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)c; (void)progress; (void)record;
    if (stage == 27 &&
        !consolidate_row_scratch) consolidate_row_scratch = r->row_scratch;
    if (stage == 27) CHECK(r->row_scratch &&
            r->row_scratch == consolidate_row_scratch);
}
static void
test_consolidate_clone_growth_reset(void)
{
    for (unsigned epoch = 1; epoch <= 2; epoch++) for (unsigned overflow = 0;
            overflow < 2; overflow++) {
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(2, 513, 513, false,
                    sess->memory_governor);
            consolidate_populate(source, 2, false);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint64_t baseline = string_reserved(sess->memory_governor),
                hash = consolidate_source_hash(source);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            consolidate_growth_epoch = consolidate_growth_visits = 0;
            consolidate_growth_target = epoch;
            consolidate_growth_reset = false;
            consolidate_growth_overflow = overflow;
            consolidate_row_scratch = NULL;
            wl_columnar_relation_test_private_checkpoint =
                consolidate_growth_stop;
            wl_columnar_consolidate_test_checkpoint =
                consolidate_scratch_observe;
            int expected =
                overflow ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(consolidate_dispatch(sess, frame, c) == expected);
            finish(c, expected);
            wl_columnar_relation_test_private_checkpoint = NULL;
            wl_columnar_consolidate_test_checkpoint = NULL;
            CHECK(consolidate_growth_visits == 1 &&
                consolidate_growth_epoch == epoch
                && consolidate_growth_record->phase ==
                WL_COLUMNAR_RELATION_PRIVATE_RESETTING);
            CHECK(consolidate_source_hash(source) == hash);
            lazy_unchanged(source); source_unpinned(source, source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                string_reserved(sess->memory_governor) == baseline);
            retry(c, 0); frame = filter_frame(sess);
            CHECK(consolidate_dispatch(sess, frame, c) == 0); finish(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            release(c); join_session_destroy(sess);
        }
}
static unsigned consolidate_reader_mode, consolidate_reader_visits;
static void
consolidate_retained_reader(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)progress;
    if (stage != (consolidate_reader_mode == 2 ? 34 : 33) ||
        consolidate_reader_visits) return;
    CHECK(record->phase == WL_COLUMNAR_CONSOLIDATE_RETIRED);
    CHECK(col_rel_source_reader_acquire(r, &join_retained_reader) == 0);
    consolidate_reader_visits++;
    if (consolidate_reader_mode == 0) wl_evaluation_control_request_cancel(c);
    else if (consolidate_reader_mode == 1) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else CHECK(!c);
}
static void
test_consolidate_healthy_reader_cleanup(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 33, 33, false, sess->memory_governor);
        consolidate_populate(source, 2, false);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t baseline = string_reserved(sess->memory_governor),
            hash = consolidate_source_hash(source);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        consolidate_reader_mode = mode; consolidate_reader_visits = 0;
        wl_columnar_consolidate_test_checkpoint = consolidate_retained_reader;
        int expected = mode == 0 ? EBUSY : mode == 1 ? EOVERFLOW : 0;
        CHECK(consolidate_dispatch(sess, frame, c) == expected);
        finish(c, expected);
        wl_columnar_consolidate_test_checkpoint = NULL;
        CHECK(consolidate_reader_visits == 1 && join_retained_reader.owner
            && consolidate_source_hash(source) == hash);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY && !frame
            && sess->cleanup_pending &&
            string_reserved(sess->memory_governor) > baseline);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0
            && !sess->cleanup_pending &&
            string_reserved(sess->memory_governor) == baseline);
        source_unpinned(source, source);
        release(c); join_session_destroy(sess);
    }
}

static unsigned consolidate_absent_visits;
static uint64_t consolidate_sort_view;
static bool consolidate_sort_needed;
static void
consolidate_absent_schema(wl_evaluation_control_t *c, int stage,
    uint64_t progress,
    wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)c; (void)progress;
    if (stage == 30) {
        CHECK(!r->column_types &&
            record->sequence.type_fingerprint == UINT64_C(14695981039346656037)
            && record->sequence.workspace.prepared_type_fingerprint ==
            UINT64_C(14695981039346656037));
        consolidate_sort_view = r->view_generation;
        consolidate_sort_needed = record->sequence.needs_sort_count != 0;
        consolidate_absent_visits++;
    } else if (stage == 31) {
        CHECK(r->view_generation ==
            consolidate_sort_view + consolidate_sort_needed);
        consolidate_absent_visits++;
    } else if (stage == 32) {
        CHECK(r->view_generation ==
            consolidate_sort_view + consolidate_sort_needed + 1u);
        consolidate_absent_visits++;
    }
}
static void
test_consolidate_absent_and_shared(void)
{
    for (unsigned mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *owner = NULL;
        if (mode == 2) {
            CHECK(wl_columnar_relation_new_auto_governed("canonical", 2, 33,
                false, sess->memory_governor, &owner) == 0);
            owner->nrows = 33;
        } else owner = relation(2, 33, 33, false, sess->memory_governor);
        if (mode != 1) for (uint32_t col = 0; col < 2; col++)
                for (uint32_t row = 0; row < 33;
                    row++) owner->columns[col][row] = 33 - row;
        col_rel_t *source = owner;
        if (mode == 2) {
            source = relation(2, 1, 0, false, sess->memory_governor);
            CHECK(col_rel_install_shared_view(source, owner) == 0);
            CHECK(source->storage_owner == owner && source->col_shared
                && col_rel_storage_alias_borrow_count(owner) == 1);
        }
        CHECK(session_add_rel(sess, source) == 0);
        if (mode == 2) CHECK(session_add_rel(sess, owner) == 0);
        uint64_t hash = consolidate_source_hash(source),
            view = source->view_generation;
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_columnar_consolidate_record_t *record =
            wl_columnar_eval_stack_cleanup_consolidate(frame);
        CHECK(record && record->ready &&
            record->phase == WL_COLUMNAR_CONSOLIDATE_INERT
            && record->sequence.metadata_admission.identity ==
            &record->sequence.metadata_admission
            && record->sequence.workspace.admission.identity ==
            &record->sequence.workspace.admission
            && record->merge_admission.identity == &record->merge_admission
            && !record->sequence.prepared && !record->merge_admission_active);
        CHECK((uintptr_t)record >= (uintptr_t)frame
            && (uintptr_t)record + sizeof(*record) <=
            (uintptr_t)frame + sess->cleanup_reserved_bytes);
        wl_evaluation_control_t *c = control(0);
        consolidate_absent_visits = 0;
        wl_columnar_consolidate_test_checkpoint = consolidate_absent_schema;
        CHECK(consolidate_dispatch(sess, frame, c) == 0); finish(c, 0);
        wl_columnar_consolidate_test_checkpoint = NULL;
        CHECK(consolidate_absent_visits == 3 &&
            !join_output(frame)->column_types);
        CHECK(join_output(frame)->nrows == 33 &&
            consolidate_source_hash(source) == hash
            && source->view_generation == view);
        if (mode == 2) CHECK(source->storage_owner == owner
                && col_rel_storage_alias_borrow_count(owner) == 1);
        source_unpinned(source, owner);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
            string_reserved(sess->memory_governor) == baseline);
        release(c); join_session_destroy(sess);

    }
}
static void
test_consolidate_invalid_float(void)
{
    for (unsigned rows = 1; rows <= 2; rows++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(1, 2, rows, false, sess->memory_governor);
        consolidate_populate(source, 0, true);
        source->columns[0][0] = (int64_t)UINT64_C(0x7ff8000000000000);
        CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
        uint64_t baseline = string_reserved(sess->memory_governor),
            hash = consolidate_source_hash(source);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        wl_evaluation_control_t *c = control(0);
        consolidate_constructor_calls = 0;
        wl_columnar_relation_test_constructor_checkpoint =
            consolidate_constructor_count;
        CHECK(consolidate_dispatch(sess, frame, c) == EINVAL);
        finish(c, EINVAL);
        wl_columnar_relation_test_constructor_checkpoint = NULL;
        CHECK(!consolidate_constructor_calls &&
            consolidate_source_hash(source) == hash);
        lazy_unchanged(source); source_unpinned(source, source);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
            string_reserved(sess->memory_governor) == baseline);
        release(c); join_session_destroy(sess);
    }
}
static void
test_consolidate_exact_work(void)
{
    for (unsigned rows_case = 0; rows_case < 3; rows_case++) {
        uint64_t cost = 0;
        for (unsigned attempt = 0; attempt < 3; attempt++) {
            wl_col_session_t *sess = join_session();
            uint32_t rows = rows_case == 0 ? 1u : rows_case == 1 ? 31u : 513u;
            col_rel_t *source = relation(2, rows, rows, false,
                    sess->memory_governor);
            consolidate_populate(source, 2, false);
            CHECK(session_add_rel(sess, source) == 0); make_lazy(source);
            uint64_t hash = consolidate_source_hash(source),
                baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(attempt == 0 ? 0 : attempt ==
                    1 ? cost : cost - 1u);
            int expected = attempt == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
            CHECK(consolidate_dispatch(sess, frame, c) == expected);
            uint64_t used = finish(c, expected);
            if (!attempt) {
                cost = used; CHECK(cost > 84);
            }else if (attempt == 1) CHECK(used == cost);
            else CHECK(used <= cost - 1u);
            CHECK(consolidate_source_hash(source) == hash);
            source_unpinned(source, source);
            if (attempt == 2 || rows == 1) lazy_unchanged(source);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0
                && string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess);
        }
    }
}

/* Complete actual MAP producer -> owned SET suffix. MAP's output schema is
 * implicit INT64, including any integer intern-ID payload. */
static wl_columnar_eval_stack_cleanup_frame_t *owned_consolidate_frame;
static col_rel_t *owned_consolidate_output;
static wl_columnar_relation_mutation_lease_t *owned_consolidate_lease;
static uint64_t owned_consolidate_old_nonce, owned_consolidate_view;
static uint64_t owned_consolidate_storage;
static unsigned owned_consolidate_mode, owned_consolidate_seen;
static int owned_consolidate_stop_stage;
static bool owned_consolidate_overflow, owned_consolidate_map_done;
static bool owned_consolidate_reset_stop, owned_consolidate_needs_sort;
static unsigned owned_consolidate_constructor_baseline,
    owned_consolidate_adoptions;
static void
owned_consolidate_map(wl_evaluation_control_t *c, int stage, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    (void)c; (void)progress; (void)group; (void)source;
    if (stage != 9) return;
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(
        owned_consolidate_frame);
    wl_columnar_relation_private_batch_t *writer =
        wl_columnar_eval_stack_cleanup_reduce_batch(owned_consolidate_frame);
    CHECK(result->owned && writer && writer->batch.relation == result->rel
        && writer->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD);
    owned_consolidate_output = result->rel;
    CHECK(!result->rel->column_types);
    owned_consolidate_lease = &writer->batch.single.lease;
    CHECK(col_rel_mutation_lease_validate(owned_consolidate_lease,
        result->rel) == 0);
    owned_consolidate_old_nonce = owned_consolidate_lease->acquisition_nonce;
    owned_consolidate_view = result->rel->view_generation;
    owned_consolidate_storage = result->rel->storage_generation;
    owned_consolidate_needs_sort = false;
    for (uint32_t row = 1; row < result->rel->nrows; row++)
        if (col_rel_row_cmp(result->rel, row - 1u, row) > 0)
            owned_consolidate_needs_sort = true;
    owned_consolidate_constructor_baseline = consolidate_constructor_calls;
    /* Metadata fixture writes occur under this actual producer writer. */
    if (owned_consolidate_mode == 1) {
        result->rel->sorted_nrows = result->rel->nrows;
        result->rel->run_count = 1;
        result->rel->run_ends[0] = result->rel->nrows;
    } else if (owned_consolidate_mode == 2) {
        result->rel->run_count = 0;
    }
    if (owned_consolidate_mode == 1 || owned_consolidate_mode == 2)
        wl_columnar_consolidate_test_fail_allocation = 0;
    owned_consolidate_map_done = true;
}
static void
owned_consolidate_private(wl_evaluation_control_t *c,
    wl_columnar_relation_private_batch_t *writer, int stage, uint64_t progress)
{
    if (!owned_consolidate_map_done || !owned_consolidate_reset_stop
        || stage != 1 || progress != 128 || owned_consolidate_seen) return;
    CHECK(writer->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING);
    owned_consolidate_seen++;
    wl_evaluation_control_request_cancel(c);
}
static void
owned_consolidate_checkpoint(wl_evaluation_control_t *c, int stage,
    uint64_t progress, wl_columnar_consolidate_record_t *record, col_rel_t *r)
{
    (void)progress; (void)record;
    if (!c) return;
    eval_stack_t *st = wl_columnar_eval_stack_cleanup_stack(
        owned_consolidate_frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(
        owned_consolidate_frame);
    if (stage == 42) {
        /* Actual stack-owned metadata after MAP publication; no schema retag. */
        if (owned_consolidate_mode == 1) st->items[0].seg_count = 7;
        else if (owned_consolidate_mode == 2) {
            st->items[0].seg_boundaries = malloc(2u * sizeof(uint32_t));
            CHECK(st->items[0].seg_boundaries);
            st->items[0].seg_boundaries[0] = 0;
            st->items[0].seg_boundaries[1] = r->nrows;
            st->items[0].seg_count = 1;
        }
    }
    if (stage == 25) {
        CHECK(r == owned_consolidate_output && st->top == 1
            && st->items[0].rel == r && st->items[0].owned && !result->rel);
        CHECK(wl_columnar_relation_private_batch_disposable(
                wl_columnar_eval_stack_cleanup_reduce_batch(
                    owned_consolidate_frame)));
    }
    CHECK(consolidate_constructor_calls ==
        owned_consolidate_constructor_baseline);
    if (stage == 39) owned_consolidate_adoptions++;
    if (stage == 26 || stage == 40) {
        CHECK(record->phase == WL_COLUMNAR_CONSOLIDATE_INERT
            && !record->starts && !record->ends && !record->sequence.boundaries
            && !record->sequence.workspace.perm_a
            && !record->sequence.workspace.insertion_rows
            && !record->sequence.workspace.admission_active);
        if (stage == 26) CHECK(!owned_consolidate_adoptions);
    }
    if (stage == 38) CHECK(st->items[0].owned && !result->rel);
    if (stage == 39 || stage == 40 || stage == 41 || stage == 37) {
        CHECK(st->top == 1 && !st->items[0].rel && !st->items[0].owned
            && result->owned && result->rel == owned_consolidate_output);
    }
    if (stage == 37 || stage == 40) {
        wl_columnar_relation_private_batch_t *writer =
            wl_columnar_eval_stack_cleanup_reduce_batch(
            owned_consolidate_frame);
        CHECK(writer && &writer->batch.single.lease == owned_consolidate_lease
            && owned_consolidate_lease->acquisition_nonce !=
            owned_consolidate_old_nonce
            && col_rel_mutation_lease_validate(owned_consolidate_lease,
            r) == 0);
    }
    if (stage != owned_consolidate_stop_stage || owned_consolidate_seen) return;
    owned_consolidate_seen++;
    if (owned_consolidate_mode == 3 || owned_consolidate_mode == 4) {
        CHECK(col_rel_source_reader_acquire(r, &join_retained_reader) == 0);
        if (owned_consolidate_mode == 3) return;
    }
    if (owned_consolidate_overflow)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static int
owned_consolidate_dispatch(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_evaluation_control_t *c,
    uint32_t width)
{
    uint32_t *projections = malloc((width ? width : 1u) * sizeof(uint32_t));
    CHECK(projections);
    for (uint32_t col = 0; col < width; col++) projections[col] = col;
    wl_plan_op_t ops[3] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "work",
         .delta_mode = WL_DELTA_FORCE_FULL},
        {.op = WL_PLAN_OP_MAP, .project_count = width,
         .project_indices = projections},
        {.op = WL_PLAN_OP_CONSOLIDATE}
    };
    int rc = compiled_dispatch(sess, frame, ops, 3, c);
    free(projections);
    return rc;
}
static void
owned_consolidate_hooks(wl_columnar_eval_stack_cleanup_frame_t *frame,
    unsigned mode, int stop, bool overflow, bool reset)
{
    owned_consolidate_frame = frame; owned_consolidate_mode = mode;
    owned_consolidate_stop_stage = stop; owned_consolidate_overflow = overflow;
    owned_consolidate_reset_stop = reset; owned_consolidate_seen = 0;
    owned_consolidate_map_done = false; owned_consolidate_adoptions = 0;
    consolidate_constructor_calls = 0;
    wl_columnar_relation_test_constructor_checkpoint =
        consolidate_constructor_count;
    wl_columnar_ops_map_test_controlled_checkpoint = owned_consolidate_map;
    wl_columnar_consolidate_test_checkpoint = owned_consolidate_checkpoint;
    wl_columnar_relation_test_private_checkpoint = owned_consolidate_private;
}
static void
owned_consolidate_unhook(void)
{
    if (owned_consolidate_map_done)
        CHECK(consolidate_constructor_calls ==
            owned_consolidate_constructor_baseline);
    if (owned_consolidate_mode == 1 || owned_consolidate_mode == 2)
        CHECK(wl_columnar_consolidate_test_fail_allocation == 0);
    wl_columnar_consolidate_test_fail_allocation = -1;
    wl_columnar_ops_map_test_controlled_checkpoint = NULL;
    wl_columnar_consolidate_test_checkpoint = NULL;
    wl_columnar_relation_test_private_checkpoint = NULL;
    wl_columnar_relation_test_constructor_checkpoint = NULL;
}
static void
test_owned_consolidate_actual_twins(void)
{
    const uint32_t sizes[] = {0, 1, 2, 31, 32, 33, 513, 10000, 33, 33};
    for (unsigned item = 0; item < sizeof(sizes) / sizeof(sizes[0]); item++)
        for (unsigned order = 0; order < 4; order++) {
            uint32_t rows = sizes[item], width = item == 8 ? 0u :
                item == 9 ? 300u : 2u;
            wl_col_session_t *sess = join_session(), *raw = join_session();
            col_rel_t *source = relation(width, rows ? rows : 1u, rows, false,
                    sess->memory_governor);
            col_rel_t *ref = relation(width, rows ? rows : 1u, rows, false,
                    raw->memory_governor);
            consolidate_populate(source, order, false);
            consolidate_populate(ref, order, false);
            CHECK(session_add_rel(sess, source) == 0 &&
                session_add_rel(raw, ref) == 0);
            uint64_t hash = consolidate_source_hash(source),
                view = source->view_generation,
                storage = source->storage_generation,
                baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess),
                *rf = filter_frame(raw);
            wl_evaluation_control_t *c = control(0);
            owned_consolidate_hooks(frame, 0, -1, false, false);
            int actual = owned_consolidate_dispatch(sess, frame, c, width);
            if (actual) fprintf(stderr,
                    "owned twin rows=%u width=%u order=%u rc=%d\n",
                    rows, width, order, actual);
            CHECK(actual == 0);
            finish(c, 0); owned_consolidate_unhook();
            CHECK(owned_consolidate_dispatch(raw, rf, NULL, width) == 0);
            col_rel_t *out = join_output(frame), *expected = join_output(rf);
            CHECK(out == owned_consolidate_output && !out->column_types
                && out->nrows == expected->nrows && out->ncols == width
                && out->storage_generation == owned_consolidate_storage
                && out->sorted_nrows == out->nrows && out->run_count == 1
                && out->run_ends[0] == out->nrows);
            for (uint32_t col = 0; col < width; col++)
                for (uint32_t row = 0; row < out->nrows; row++)
                    CHECK(out->columns[col][row] ==
                        expected->columns[col][row]);
            CHECK(out->view_generation == owned_consolidate_view +
                (rows > 1 ? 1u + owned_consolidate_needs_sort : 0u));
            CHECK(consolidate_source_hash(source) == hash &&
                source->view_generation == view &&
                source->storage_generation == storage);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                wl_columnar_eval_stack_cleanup_finish(&rf) == 0 &&
                string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess); join_session_destroy(raw);
        }
}
static void
test_owned_consolidate_lifecycle(void)
{
    const int stages[] = {25, 38, 39, 37, 30, 31, 24, 22, 23, 41, 10, 12};
    for (unsigned item = 0; item < sizeof(stages) / sizeof(stages[0]); item++)
        for (unsigned overflow = 0; overflow < 2; overflow++) {
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(2, 513, 513, false,
                    sess->memory_governor);
            consolidate_populate(source, 2, false);
            CHECK(session_add_rel(sess, source) == 0);
            uint64_t hash = consolidate_source_hash(source),
                baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            owned_consolidate_hooks(frame, 0, stages[item], overflow, false);
            int expected =
                overflow ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(owned_consolidate_dispatch(sess, frame, c, 2) == expected);
            CHECK(owned_consolidate_seen == 1); finish(c, expected);
            owned_consolidate_unhook();
            CHECK(consolidate_source_hash(source) == hash);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess);
        }
    /* Genuine readers cause acquisition/disposal refusal; no token repair. */
    for (unsigned zero = 0; zero < 2; zero++)
        for (unsigned mode = 1; mode <= (zero ? 2u : 5u); mode++) {
            wl_col_session_t *sess = join_session();
            uint32_t rows = mode <= 2 ? (zero ? 0u : 1u) : 33u;
            col_rel_t *source = relation(2, rows ? rows : 1u, rows, false,
                    sess->memory_governor);
            consolidate_populate(source, 2, false);
            CHECK(session_add_rel(sess, source) == 0);
            uint64_t baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(0);
            owned_consolidate_hooks(frame, mode,
                mode == 3 ? 25 : mode == 4 ? 41 :
                -1, false, mode == 5);
            int expected = mode <= 2 ? 0 : mode <= 4 ? EBUSY :
                WL_EVALUATION_CONTROL_CANCELLED;
            int actual = owned_consolidate_dispatch(sess, frame, c, 2);
            if (actual != expected) fprintf(stderr,
                    "owned lifecycle mode=%u rc=%d expected=%d\n",
                    mode, actual, expected);
            CHECK(actual == expected);
            owned_consolidate_unhook();
            if (mode <= 2) {
                eval_stack_t *st = wl_columnar_eval_stack_cleanup_stack(frame);
                CHECK(st->items[0].rel == owned_consolidate_output &&
                    st->items[0].rel->view_generation ==
                    owned_consolidate_view);
                CHECK(st->items[0].seg_count == (mode == 1 ? 7u : 0u)
                    && !st->items[0].seg_boundaries);
            } else if (mode <= 4) {
                CHECK(owned_consolidate_seen == 1 && join_retained_reader.owner
                    && string_reserved(sess->memory_governor) > baseline);
                eval_stack_t *st = wl_columnar_eval_stack_cleanup_stack(frame);
                eval_entry_t *result =
                    wl_columnar_eval_stack_cleanup_result(frame);
                CHECK(st->top == 1 && !st->items[0].rel && !st->items[0].owned
                    && result->rel == owned_consolidate_output &&
                    result->owned);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY
                    && !frame && sess->cleanup_pending);
                CHECK(col_rel_source_reader_release(&join_retained_reader) ==
                    0);
                CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0
                    && !sess->cleanup_pending);
            } else CHECK(owned_consolidate_seen == 1);
            if (frame) CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) ==
                    0);
            finish(c, expected);
            CHECK(string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess);
        }
}

static void
test_owned_consolidate_exact_work(void)
{
    for (unsigned kind = 0; kind < 3; kind++) {
        uint64_t cost = 0;
        for (unsigned attempt = 0; attempt < 3; attempt++) {
            uint32_t rows = kind == 0 ? 1u : kind == 1 ? 31u : 513u;
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(2, rows, rows, false,
                    sess->memory_governor);
            consolidate_populate(source, 2, false);
            CHECK(session_add_rel(sess, source) == 0);
            uint64_t baseline = string_reserved(sess->memory_governor),
                hash = consolidate_source_hash(source);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_evaluation_control_t *c = control(!attempt ? 0 : attempt == 1 ?
                    cost : cost - 1u);
            owned_consolidate_hooks(frame, 0, -1, false, false);
            int expected = attempt == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
            CHECK(owned_consolidate_dispatch(sess, frame, c, 2) == expected);
            uint64_t used = finish(c, expected); owned_consolidate_unhook();
            if (!attempt) cost = used;
            else if (attempt == 1) CHECK(used == cost);
            else CHECK(used <= cost - 1u);
            CHECK(consolidate_source_hash(source) == hash);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 &&
                string_reserved(sess->memory_governor) == baseline);
            release(c); join_session_destroy(sess);
        }
    }
}

/* Generated initial AUTO caller and genuine typed helper are distinct proofs. */
static wl_plan_t *publish_plan;
static wirelog_program_t *publish_program;
static wl_col_session_t *publish_session;
static col_rel_t *publish_target;
static unsigned publish_mode, publish_seen, publish_commits;
static wl_columnar_relation_bulk_append_t *publish_record;
static col_rel_t *publish_result;
static unsigned publish_payload_phase, publish_payload_seen,
    publish_restore_seen;
static bool publish_payload_overflow;

static const wl_plan_relation_t *
publish_generated_name(uint32_t width, const char *source_name)
{
    size_t capacity = (size_t)width * 160u + 256u + 2u * strlen(source_name),
        used = 0;
    char *text = malloc(capacity); CHECK(text);
    for (unsigned decl = 0; decl < 2; decl++) {
        used += snprintf(text + used, capacity - used, ".decl %s(",
                decl ? "H" : source_name);
        for (uint32_t c = 0; c < width; c++)
            used += snprintf(text + used, capacity - used, "%sv%u: int64",
                    c ? ", " : "", c);
        used += snprintf(text + used, capacity - used, ")\n");
    }
    used += snprintf(text + used, capacity - used, "H(");
    for (uint32_t c = 0; c < width; c++)
        used += snprintf(text + used, capacity - used, "%sv%u", c ? ", " : "",
                c);
    used += snprintf(text + used, capacity - used, ") :- %s(", source_name);
    for (uint32_t c = 0; c < width; c++)
        used += snprintf(text + used, capacity - used, "%sv%u", c ? ", " : "",
                c);
    used += snprintf(text + used, capacity - used, ").\n");
    CHECK(used < capacity);
    wirelog_error_t error;
    publish_program = wirelog_parse_string(text, &error); free(text);
    CHECK(publish_program && error == WIRELOG_OK);
    CHECK(wl_plan_from_program(publish_program, &publish_plan) == 0);
    for (uint32_t st = 0; st < publish_plan->stratum_count; st++)
        for (uint32_t r = 0; r < publish_plan->strata[st].relation_count; r++) {
            const wl_plan_relation_t *rp =
                &publish_plan->strata[st].relations[r];
            if (strcmp(rp->name, "H")) continue;
            CHECK(rp->op_count == 3 && rp->ops[0].op == WL_PLAN_OP_VARIABLE
                && rp->ops[0].delta_mode == WL_DELTA_AUTO
                && rp->ops[1].op == WL_PLAN_OP_MAP
                && rp->ops[2].op == WL_PLAN_OP_CONSOLIDATE);
            return rp;
        }
    CHECK(false); return NULL;
}
static const wl_plan_relation_t *
publish_generated(uint32_t width)
{
    return publish_generated_name(width, "S");
}
static void
publish_generated_free(void)
{
    wl_plan_free(publish_plan); wirelog_program_free(publish_program);
    publish_plan = NULL; publish_program = NULL;
}
static col_rel_t *
publish_named(wl_col_session_t *sess, const char *name, uint32_t width,
    uint32_t capacity, uint32_t rows)
{
    col_rel_t *r = relation(width, capacity, rows, false,
            sess->memory_governor);
    CHECK(wl_columnar_relation_rename_checked(r, name) == 0);
    CHECK(session_add_rel(sess, r) == 0); return r;
}
static void
publish_after_plan(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *entry)
{
    CHECK(!stack->top && entry->rel && entry->owned &&
        !entry->rel->column_types);
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_sources(sess->cleanup_active);
    CHECK(group && !group->descriptor_count && !group->owner_count);
    publish_result = entry->rel;
    if (publish_mode == 1) {
        CHECK(col_rel_source_reader_acquire(entry->rel,
            &join_retained_reader) == 0);
        publish_seen++;
    }
}
static void
publish_reader_cancel(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (publish_mode != 5 || !acquired || !publish_result
        || gate != &publish_result->descriptor_access) return;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(
        publish_session->cleanup_active);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(
        publish_session->cleanup_active);
    CHECK(stack->top == 1 && !stack->items[0].rel && stack->items[0].owned
        && result->rel == publish_result && result->owned);
    publish_seen++; wl_evaluation_control_request_cancel(c);
}
static void
publish_bulk_hook(wl_evaluation_control_t *c, int phase,
    wl_columnar_relation_bulk_append_t *record)
{
    if (phase == 5 && publish_payload_seen && record == publish_record) {
        CHECK(record->pending.growth && record->transaction_active
            && record->metadata_committed && !record->published
            && atomic_load(&record->dst->retained_reservation.state) ==
            WL_COLUMNAR_MEMORY_RESERVATION_REPLACING);
        publish_restore_seen++;
    }
    if (phase == 1 && publish_mode == 3) {
        CHECK(record->active && !record->published && record->metadata_committed
            && record->prepared_schema_ok && record->prepared_types
            && record->metadata_pending.identity == &record->metadata_pending
            && atomic_load(&record->metadata_pending.state) ==
            WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            && !record->dst->column_types
            && &record->metadata_pending != &record->dst->metadata_reservation);
        publish_seen++; wl_evaluation_control_request_cancel(c);
    }
    if (phase == 2 && record->dst == publish_target) {
        CHECK(record->published && !record->active);
        publish_commits++;
        if (publish_mode == 2) {
            eval_entry_t *entry = wl_columnar_eval_stack_cleanup_result(
                publish_session->cleanup_active);
            CHECK(entry->rel && entry->owned && entry->rel != publish_target);
            CHECK(col_rel_source_reader_acquire(entry->rel,
                &join_retained_reader) == 0);
            publish_record = record; publish_seen++;
        }
    }
}
static void
publish_payload_stop(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (token != &publish_record->dst->retained_reservation
        || phase != publish_payload_phase) return;
    CHECK(publish_record->active && !publish_record->published
        && publish_record->metadata_committed
        && atomic_load(&publish_record->metadata_pending.state) ==
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && atomic_load(&token->state) ==
        WL_COLUMNAR_MEMORY_RESERVATION_COMMITTING
        && !publish_record->dst->column_types);
    publish_payload_seen++;
    if (publish_payload_overflow) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static int
publish_dispatch(wl_col_session_t *sess, const wl_plan_relation_t *rp,
    wl_evaluation_control_t *c)
{
    sess->base.evaluation_control = c;
    int rc = wl_columnar_eval_serial_framed_relation(rp, sess, false);
    sess->base.evaluation_control = NULL; return rc;
}
static void
publish_hooks(wl_col_session_t *sess, col_rel_t *target, unsigned mode)
{
    publish_session = sess; publish_target = target; publish_mode = mode;
    publish_seen = publish_commits = 0; publish_record = NULL;
    publish_result = NULL;
    wl_columnar_eval_serial_test_after_plan = publish_after_plan;
    wl_columnar_relation_test_bulk_checkpoint = publish_bulk_hook;
    if (mode ==
        5) wl_columnar_relation_test_after_reader_cas = publish_reader_cancel;
}
static void
publish_unhook(void)
{
    wl_columnar_eval_serial_test_after_plan = NULL;
    wl_columnar_relation_test_bulk_checkpoint = NULL;
    wl_columnar_relation_test_after_reader_cas = NULL;
}
static void
test_publish_generated_twins(void)
{
    const uint32_t rows[] = {0, 1, 2, 31, 32, 33, 513, 10000};
    const uint32_t widths[] = {0, 2, 300};
    for (unsigned wi = 0; wi < 3; wi++) {
        const wl_plan_relation_t *rp = publish_generated(widths[wi]);
        for (unsigned ri = 0; ri < 8; ri++) for (unsigned prefix =
                0; prefix < 2;
                prefix++){
                wl_col_session_t *sess = join_session(), *raw = join_session();
                publish_named(sess, "S", widths[wi], rows[ri] ? rows[ri] : 1,
                    rows[ri]);
                publish_named(raw, "S", widths[wi], rows[ri] ? rows[ri] : 1,
                    rows[ri]);
                col_rel_t *target = publish_named(sess, "H", widths[wi], 40,
                        prefix ? 33 : 0);
                col_rel_t *reference = publish_named(raw, "H", widths[wi], 40,
                        prefix ? 33 : 0);
                uint64_t identity = target->relation_identity;
                wl_evaluation_control_t *c = control(0);
                publish_hooks(sess, target, 0);
                CHECK(publish_dispatch(sess, rp, c) == 0); finish(c, 0);
                CHECK(publish_commits == (rows[ri] ? 1u : 0u));
                publish_unhook();
                CHECK(publish_dispatch(raw, rp, NULL) == 0);
                CHECK(session_find_rel(sess,
                    "H") == target && target->relation_identity == identity
                    && target->nrows == reference->nrows &&
                    !target->column_types);
                payload_equal(target, reference, 0);
                CHECK(!sess->cleanup_active && !sess->cleanup_pending);
                release(c); join_session_destroy(sess);
                join_session_destroy(raw);
            }
        publish_generated_free();
    }
    puts(
        "serial publish: generated AUTO caller raw twins rows0..10000 widths0/2/300 passed");
}
static void
test_publish_auto_selection(void)
{
    const wl_plan_relation_t *rp = publish_generated(2);
    for (unsigned lane = 0; lane < 5; lane++) {
        wl_col_session_t *sess = join_session();
        publish_named(sess, "S", 2, 33, 33);
        col_rel_t *target = publish_named(sess, "H", 2, 1, 0);
        if (lane) publish_named(sess, "$d$S", 2, 40,
                lane == 1 ? 0 : lane == 2 ? 33 : lane == 3 ? 40 : 1);
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_evaluation_control_t *c = control(0);
        int expected = lane == 4 ? ENOTSUP : 0;
        CHECK(publish_dispatch(sess, rp, c) == expected); finish(c, expected);
        CHECK(target->nrows == (lane == 4 ? 0u : 33u));
        if (lane ==
            4) CHECK(string_reserved(sess->memory_governor) == baseline);
        release(c); join_session_destroy(sess);
    }
    publish_generated_free();
    puts(
        "serial publish: AUTO absent/empty/equal/larger full and genuine smaller delta refusal passed");
}
static void
test_publish_matching_declared_head(void)
{
    const wl_plan_relation_t *rp = publish_generated(2);
    wl_col_session_t *sess = join_session(), *raw = join_session();
    publish_named(sess, "S", 2, 33, 33);
    publish_named(raw, "S", 2, 33, 33);
    col_rel_t *head = publish_named(sess, "H", 2, 40, 3);
    col_rel_t *reference = publish_named(raw, "H", 2, 40, 3);
    const wirelog_column_type_t types[2] = {WIRELOG_TYPE_INT64,
                                            WIRELOG_TYPE_INT64};
    CHECK(col_rel_set_column_types(head, types, 2) == 0);
    CHECK(col_rel_set_column_types(reference, types, 2) == 0);
    const wirelog_column_type_t *original_types = head->column_types;
    wl_evaluation_control_t *c = control(0);
    publish_hooks(sess, head, 0);
    CHECK(publish_dispatch(sess, rp, c) == 0); finish(c, 0);
    publish_unhook(); CHECK(publish_dispatch(raw, rp, NULL) == 0);
    CHECK(head->nrows == 36 && reference->nrows == 36
        && head->column_types == original_types && publish_commits == 1);
    payload_equal(head, reference, 0);
    release(c); join_session_destroy(sess); join_session_destroy(raw);
    publish_generated_free();
    puts(
        "serial publish: genuine predeclared INT64 canonical head with untyped generated MAP passed");
}
static void
test_publish_refusal_recovery(void)
{
    const wl_plan_relation_t *rp = publish_generated(2);
    for (unsigned mode = 1; mode <= 5; mode++) {
        if (mode == 3) continue;
        wl_col_session_t *sess = join_session();
        publish_named(sess, "S", 2, 33, 33);
        col_rel_t *target = publish_named(sess, "H", 2, 40, 1);
        uint64_t hash = consolidate_source_hash(target),
            view = target->view_generation;
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_evaluation_control_t *c = control(0);
        publish_hooks(sess, target, mode);
        if (mode == 4) CHECK(col_rel_source_reader_acquire(target,
                &join_retained_reader) == 0);
        int expected = mode == 5 ? WL_EVALUATION_CONTROL_CANCELLED : EBUSY;
        CHECK(publish_dispatch(sess, rp, c) == expected); finish(c, expected);
        if (mode == 5) CHECK(publish_seen == 1 && !join_retained_reader.owner);
        else CHECK(join_retained_reader.owner);
        if (mode == 2) {
            CHECK(publish_seen == 1 && publish_commits == 1 &&
                target->nrows == 34
                && publish_record && publish_record->published &&
                !publish_record->active
                && sess->cleanup_pending_count == 1);
            hash = consolidate_source_hash(target);
            view = target->view_generation;
        } else CHECK(consolidate_source_hash(target) == hash &&
                target->view_generation == view);
        if (mode !=
            5) CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        unsigned commits = publish_commits;
        CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        CHECK(consolidate_source_hash(target) == hash &&
            target->view_generation == view
            && publish_commits == commits && !sess->cleanup_pending);
        publish_unhook();
        if (mode != 2) {
            CHECK(string_reserved(sess->memory_governor) == baseline);
            retry(c, 0); CHECK(publish_dispatch(sess, rp, c) == 0);
            finish(c, 0);
            CHECK(target->nrows == 34);
        }
        release(c); join_session_destroy(sess);
    }
    publish_generated_free();
    puts(
        "serial publish: genuine result/target readers and consumed cleanup-only retry passed");
}
static void
test_publish_exact_and_stops(void)
{
    const wl_plan_relation_t *rp = publish_generated(2);
    uint64_t cost = 0;
    for (unsigned trial = 0; trial < 3; trial++) {
        wl_col_session_t *sess = join_session();
        publish_named(sess, "S", 2, 513, 513);
        col_rel_t *target = publish_named(sess, "H", 2, 600, 33);
        uint64_t hash = consolidate_source_hash(target),
            view = target->view_generation;
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_evaluation_control_t *c = control(trial == 0 ? 0 : trial ==
                1 ? cost : cost - 1);
        int expected = trial == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
        CHECK(publish_dispatch(sess, rp, c) == expected);
        uint64_t used = finish(c, expected);
        if (!trial) cost = used; else if (trial == 1) CHECK(used == cost);
        if (trial == 2) {
            CHECK(consolidate_source_hash(target) == hash &&
                target->view_generation == view
                && string_reserved(sess->memory_governor) == baseline);
            retry(c, 0); CHECK(publish_dispatch(sess, rp, c) == 0);
            finish(c, 0);
        }
        CHECK(target->nrows == 546); release(c); join_session_destroy(sess);
    }
    publish_generated_free();
    puts(
        "serial publish: complete caller exact limit and precommit fresh retry passed");
}
static void
test_publish_typed_record(void)
{
    for (unsigned growth = 0; growth < 2; growth++) for (unsigned stop = 0;
            stop < 5; stop++){
            if (stop && !growth) continue;
            wl_col_session_t *sess = join_session();
            col_rel_t *source = relation(2, 33, 33, false,
                    sess->memory_governor);
            wirelog_column_type_t types[2] = {WIRELOG_TYPE_INT64,
                                              WIRELOG_TYPE_INT64};
            CHECK(col_rel_set_column_types(source, types, 2) == 0);
            col_rel_t *target = relation(2, growth ? 1 : 40, 1, false,
                    sess->memory_governor);
            uint64_t hash = consolidate_source_hash(target),
                view = target->view_generation;
            uint64_t storage = target->storage_generation;
            uint64_t metadata_reserved = target->metadata_reserved_bytes;
            uint64_t metadata_state =
                atomic_load(&target->metadata_reservation.state);
            uint64_t metadata_owner =
                atomic_load(&target->metadata_reservation.owner_bits);
            uint64_t payload_reserved = target->retained_reserved_bytes;
            const void *metadata_identity =
                target->metadata_reservation.identity;
            uint64_t baseline = string_reserved(sess->memory_governor);
            wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
            wl_columnar_relation_bulk_append_t *record =
                wl_columnar_eval_stack_cleanup_bulk_append(frame);
            CHECK(record && record->ready && !record->active);
            wl_evaluation_control_t *c = control(0);
            publish_mode = stop == 1 ? 3 : 0;
            publish_seen = 0; publish_target = NULL;
            publish_record = record;
            publish_payload_seen = publish_restore_seen = 0;
            publish_payload_phase = stop ==
                2 ? WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING
            : WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF;
            publish_payload_overflow = stop == 4;
            if (stop >=
                2) wl_columnar_memory_governor_test_checkpoint =
                    publish_payload_stop;
            wl_columnar_relation_test_bulk_checkpoint = publish_bulk_hook;
            int expected = stop ==
                4 ? EOVERFLOW : stop ? WL_EVALUATION_CONTROL_CANCELLED : 0;
            CHECK(wl_columnar_relation_bulk_append_controlled(record, target,
                source, c) == expected);
            finish(c, expected);
            wl_columnar_relation_test_bulk_checkpoint = NULL;
            wl_columnar_memory_governor_test_checkpoint = NULL;
            CHECK(!record->active && !record->scratch.bytes &&
                !record->metadata_pending.bytes
                && !record->lengths && !record->prepared_types &&
                !record->prepared_schema_ok);
            if (stop) CHECK(!record->published && (stop == 1 ? publish_seen == 1
            : publish_payload_seen == 1 && publish_restore_seen == 1));
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            if (stop) {
                CHECK(consolidate_source_hash(target) == hash &&
                    target->view_generation == view
                    && target->storage_generation == storage &&
                    !target->column_types
                    && target->metadata_reserved_bytes == metadata_reserved
                    && target->metadata_reservation.identity ==
                    metadata_identity
                    && atomic_load(&target->metadata_reservation.state) ==
                    metadata_state
                    && atomic_load(&target->metadata_reservation.owner_bits) ==
                    metadata_owner
                    && target->retained_reserved_bytes == payload_reserved
                    && string_reserved(sess->memory_governor) == baseline);
                retry(c, 0); frame = filter_frame(sess);
                record = wl_columnar_eval_stack_cleanup_bulk_append(frame);
                CHECK(wl_columnar_relation_bulk_append_controlled(record,
                    target, source, c) == 0);
                finish(c, 0);
                CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            }
            CHECK(target->nrows == 34 && target->column_types
                && target->column_types[0] == WIRELOG_TYPE_INT64);
            payload_equal(target, source, 1);
            release(c); destroy(source); destroy(target);
            join_session_destroy(sess);
        }
    puts(
        "serial publish: real typed record promotion growth/no-growth and preparatory commit stop passed");
}

static col_rel_t *publish_selection_source;
static unsigned publish_selection_seen;
static void
publish_selection_cancel(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!acquired ||
        gate != &publish_selection_source->descriptor_access) return;
    CHECK(publish_selection_seen == 0); publish_selection_seen++;
    wl_evaluation_control_request_cancel(c);
}
static void
test_publish_selection_authority(void)
{
    const wl_plan_relation_t *rp = publish_generated(2);
    for (unsigned mode = 0; mode < 4; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 33, 33, false, sess->memory_governor);
        CHECK(wl_columnar_relation_rename_checked(source, "S") == 0);
        CHECK(col_rel_storage_owner_ensure_initialized(source) == 0);
        col_rel_t *delta = relation(2, 33, 0, false, sess->memory_governor);
        CHECK(wl_columnar_relation_rename_checked(delta, "$d$S") == 0);
        if (mode == 3) CHECK(col_rel_install_shared_view(delta, source) == 0);
        /* Alias is destroyed first by real session teardown. */
        CHECK(session_add_rel(sess, delta) == 0);
        CHECK(session_add_rel(sess, source) == 0);
        col_rel_t *target = publish_named(sess, "H", 2, 1, 0);
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_evaluation_control_t *c = control(0);
        wl_columnar_source_access_writer_t conflict = {0};
        if (mode == 0 || mode == 1) CHECK(col_rel_source_writer_acquire(
                    mode ? delta : source, &conflict) == 0);
        if (mode == 2) {
            publish_selection_source = source; publish_selection_seen = 0;
            wl_columnar_relation_test_after_reader_cas =
                publish_selection_cancel;
        }
        int expected = mode < 2 ? EBUSY : mode ==
            2 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
        CHECK(publish_dispatch(sess, rp, c) == expected); finish(c, expected);
        wl_columnar_relation_test_after_reader_cas = NULL;
        if (mode <
            2) CHECK(wl_columnar_source_access_writer_release(&conflict) == 0);
        if (mode == 2) CHECK(publish_selection_seen == 1);
        CHECK(!sess->cleanup_pending && !sess->cleanup_active);
        if (mode != 3) {
            CHECK(!target->nrows &&
                string_reserved(sess->memory_governor) == baseline);
            retry(c, 0); CHECK(publish_dispatch(sess, rp, c) == 0);
            finish(c, 0);
        } else CHECK(delta->storage_owner == source &&
                col_rel_storage_alias_borrow_count(source) == 1);
        CHECK(target->nrows == 33);
        release(c); join_session_destroy(sess);
    }
    publish_generated_free();
    char long_name[601]; memset(long_name, 'A', 600); long_name[600] = '\0';
    char delta_name[256];
    snprintf(delta_name, sizeof(delta_name), "$d$%s", long_name);
    CHECK(strlen(delta_name) == 255 && !strncmp(delta_name, "$d$", 3));
    rp = publish_generated_name(2, long_name);
    wl_col_session_t *sess = join_session();
    publish_named(sess, long_name, 2, 33, 33);
    publish_named(sess, delta_name, 2, 1, 0);
    col_rel_t *head = publish_named(sess, "H", 2, 1, 0);
    wl_evaluation_control_t *c = control(0);
    CHECK(publish_dispatch(sess, rp, c) == 0); finish(c, 0);
    CHECK(head->nrows == 33); release(c); join_session_destroy(sess);
    publish_generated_free();
    puts(
        "serial publish: AUTO real conflicting writers/first reader cancellation/shared owner and legacy255-byte name passed");
}
static unsigned publish_preparation_stage, publish_preparation_seen;
static bool publish_preparation_overflow, publish_scratch_fault;
static const void *publish_scratch_identity;
static unsigned publish_reset_index;
static void
publish_prepare_stop(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    (void)progress;
    if ((unsigned)phase != publish_preparation_stage ||
        publish_preparation_seen) return;
    CHECK(publish_record && publish_record->active && !publish_record->published
        && publish_record->scratch.bytes && publish_record->lengths
        && publish_record->metadata_pending.bytes &&
        publish_record->prepared_types);
    publish_preparation_seen++;
    if (publish_scratch_fault) {
        publish_scratch_identity = publish_record->scratch.identity;
        CHECK(publish_scratch_identity == &publish_record->scratch);
        publish_record->scratch.identity = NULL;
    }
    if (publish_preparation_overflow) CHECK(wl_evaluation_control_charge(c,
            UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
publish_reset_stop(wl_evaluation_control_t *c, int phase,
    wl_columnar_relation_bulk_append_t *record)
{
    if (publish_reset_index ? phase != 6 ||
        record->initialized_tokens != publish_reset_index
        : phase != 3) return;
    CHECK(record->active && !record->published && record->ready
        && record->scratch.identity == &record->scratch
        && !record->sources.descriptor_count && !record->writer.owner);
    if (!publish_reset_index || publish_reset_index == 5)
        CHECK(record->pending.fresh.identity == &record->pending.fresh);
    publish_preparation_seen++; wl_evaluation_control_request_cancel(c);
}
static void
publish_terminal_stop(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (token != &publish_record->metadata_pending
        || phase != WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF) return;
    CHECK(!publish_record->growth && !publish_record->published
        && !publish_record->dst->column_types);
    publish_preparation_seen++; wl_evaluation_control_request_cancel(c);
}
static void
test_publish_preparation_lifetime(void)
{
    for (unsigned mode = 0; mode < 13; mode++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *source = relation(2, 33, 33, false, sess->memory_governor);
        const wirelog_column_type_t types[2] = {WIRELOG_TYPE_INT64,
                                                WIRELOG_TYPE_INT64};
        CHECK(col_rel_set_column_types(source, types, 2) == 0);
        col_rel_t *target = relation(2, mode == 7 ? 40 : 1, 1, false,
                sess->memory_governor);
        char name[601]; memset(name, 'n', 600); name[600] = '\0';
        const char *names[2] = {name, "second"};
        CHECK(col_rel_set_schema(target, 2, names) == 0);
        uint64_t hash = consolidate_source_hash(target),
            view = target->view_generation;
        uint64_t storage = target->storage_generation;
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_columnar_eval_stack_cleanup_frame_t *frame = filter_frame(sess);
        publish_record = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        wl_evaluation_control_t *c = control(0); publish_preparation_seen = 0;
        publish_preparation_stage = mode < 4 ? mode + 1 : 1;
        publish_preparation_overflow = mode == 4;
        publish_scratch_fault = mode == 5;
        publish_reset_index = mode >= 8 ? mode - 7 : 0;
        if (mode == 6 ||
            mode >=
            8) wl_columnar_relation_test_bulk_checkpoint = publish_reset_stop;
        else if (mode ==
            7) wl_columnar_memory_governor_test_checkpoint =
                publish_terminal_stop;
        else wl_columnar_relation_test_metadata_checkpoint =
                publish_prepare_stop;
        int expected = mode == 5 ? EINVAL : mode ==
            4 ? EOVERFLOW : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(wl_columnar_relation_bulk_append_controlled(publish_record,
            target, source, c) == expected);
        finish(c, expected);
        wl_columnar_relation_test_metadata_checkpoint = NULL;
        wl_columnar_relation_test_bulk_checkpoint = NULL;
        wl_columnar_memory_governor_test_checkpoint = NULL;
        CHECK(publish_preparation_seen == 1 && !publish_record->published
            && !publish_record->lengths && !publish_record->prepared_types
            && !publish_record->prepared_schema_ok &&
            !publish_record->prepared_columns);
        CHECK(consolidate_source_hash(target) == hash &&
            target->view_generation == view
            && target->storage_generation == storage && !target->column_types);
        if (mode == 5) {
            CHECK(publish_record->active && publish_record->scratch.bytes
                && !publish_record->scratch.identity &&
                publish_record->writer.owner);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EINVAL &&
                !frame
                && sess->cleanup_pending_count == 1);
            /* Intentionally malformed authority, not healthy contention. Only
             * the captured identity pointer is restored; no token copy/refund. */
            publish_record->scratch.identity = publish_scratch_identity;
            CHECK(wl_columnar_eval_stack_cleanup_retry(sess) == 0);
        } else CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        CHECK(string_reserved(sess->memory_governor) == baseline);
        retry(c, 0); frame = filter_frame(sess);
        publish_record = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        max_chunk = 0; wl_columnar_relation_test_work_checkpoint = checkpoint;
        CHECK(wl_columnar_relation_bulk_append_controlled(publish_record,
            target, source, c) == 0);
        CHECK(max_chunk <= WL_EVALUATION_CONTROL_QUANTUM && max_chunk >= 128);
        wl_columnar_relation_test_work_checkpoint = NULL;
        finish(c, 0); CHECK(target->nrows == 34 && target->column_types);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        release(c); destroy(source); destroy(target);
        join_session_destroy(sess);
    }
    puts(
        "serial publish: durable scratch/partial schema/reset/terminal cutoff stops and labeled identity-validation fault passed");
}

static bool publish_old_copy_armed;
static unsigned publish_old_copy_seen;
static uint64_t publish_old_copy_units;
static void
publish_old_copy_work(wl_evaluation_control_t *c, uint64_t work)
{
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    if (!publish_old_copy_armed ||
        work != WL_EVALUATION_CONTROL_QUANTUM) return;
    CHECK(publish_record && publish_record->dst == publish_target
        && publish_record->active && !publish_record->published
        && publish_record->base >= 513 && publish_record->count == 33
        && publish_record->transaction_active &&
        publish_record->pending.growth);
    publish_old_copy_units += work; publish_old_copy_seen++;
    publish_old_copy_armed = false; wl_evaluation_control_request_cancel(c);
}
static void
publish_old_copy_start(wl_evaluation_control_t *c, int phase,
    wl_columnar_relation_bulk_append_t *record)
{
    (void)c;
    if (phase == 4 && record->dst == publish_target) {
        CHECK(record->base >= 513 && record->count == 33 && record->growth);
        publish_record = record; publish_old_copy_armed = true;
    }
}
static void
test_publish_old_head_and_negatives(void)
{
    const uint32_t widths[] = {2, 2, 300};
    const uint32_t old_rows[] = {513, 10000, 513};
    for (unsigned fixture = 0; fixture < 3; fixture++) {
        const wl_plan_relation_t *rp = publish_generated(widths[fixture]);
        wl_col_session_t *sess = join_session(), *raw = join_session();
        publish_named(sess, "S", widths[fixture], 33, 33);
        publish_named(raw, "S", widths[fixture], 33, 33);
        col_rel_t *head = publish_named(sess, "H", widths[fixture],
                old_rows[fixture], old_rows[fixture]);
        col_rel_t *reference = publish_named(raw, "H", widths[fixture],
                old_rows[fixture], old_rows[fixture]);
        uint64_t hash = consolidate_source_hash(head),
            view = head->view_generation;
        uint64_t storage = head->storage_generation,
            identity = head->relation_identity;
        uint64_t baseline = string_reserved(sess->memory_governor);
        wl_evaluation_control_t *c = control(0);
        publish_target = head; publish_record = NULL;
        publish_old_copy_seen = 0; publish_old_copy_units = 0;
        publish_old_copy_armed = false;
        wl_columnar_relation_test_bulk_checkpoint = publish_old_copy_start;
        wl_columnar_relation_test_work_checkpoint = publish_old_copy_work;
        CHECK(publish_dispatch(sess, rp, c) == WL_EVALUATION_CONTROL_CANCELLED);
        finish(c, WL_EVALUATION_CONTROL_CANCELLED);
        wl_columnar_relation_test_bulk_checkpoint = NULL;
        wl_columnar_relation_test_work_checkpoint = NULL;
        CHECK(publish_old_copy_seen == 1 && publish_old_copy_units == 256
            && consolidate_source_hash(head) == hash &&
            head->view_generation == view
            && head->storage_generation == storage &&
            head->relation_identity == identity
            && string_reserved(sess->memory_governor) == baseline &&
            !sess->cleanup_pending);
        retry(c, 0); CHECK(publish_dispatch(sess, rp, c) == 0); finish(c, 0);
        CHECK(publish_dispatch(raw, rp, NULL) == 0);
        CHECK(head->nrows == old_rows[fixture] + 33 &&
            reference->nrows == head->nrows);
        payload_equal(head, reference, 0);
        release(c); join_session_destroy(sess); join_session_destroy(raw);
        publish_generated_free();
    }
    const wl_plan_relation_t *rp = publish_generated(2);
    wl_col_session_t *sess = join_session();
    publish_named(sess, "S", 2, 33, 33);
    uint64_t baseline = string_reserved(sess->memory_governor);
    wl_evaluation_control_t *c = control(0);
    CHECK(publish_dispatch(sess, rp, c) == ENOTSUP); finish(c, ENOTSUP);
    CHECK(!session_find_rel(sess,
        "H") && string_reserved(sess->memory_governor) == baseline);
    col_rel_t *head = publish_named(sess, "H", 2, 33, 33);
    col_rel_t *owner = relation(2, 33, 33, false, sess->memory_governor);
    CHECK(col_rel_install_shared_view(head, owner) == 0);
    CHECK(session_add_rel(sess, owner) == 0);
    uint64_t hash = consolidate_source_hash(head), view = head->view_generation;
    baseline = string_reserved(sess->memory_governor);
    retry(c, 0); CHECK(publish_dispatch(sess, rp, c) == ENOTSUP);
    finish(c, ENOTSUP);
    CHECK(consolidate_source_hash(head) == hash && head->view_generation == view
        && string_reserved(sess->memory_governor) == baseline &&
        !sess->cleanup_pending);
    release(c); join_session_destroy(sess); publish_generated_free();
    puts(
        "serial publish: actual old canonical513/10000/wide growth-copy stop, fresh raw retry and missing/alias head negatives passed");
}

/* Production backend attempts: generated default sessions retain their real
 * managed arena/pool and start with EDB registration only. */
typedef struct {
    uint64_t count, digest, live_bytes;
    uint32_t rows, width;
    wl_col_session_t *session;
    bool cancel_first, check_pins;
} serial_capture_t;
static unsigned serial_stop_phase, serial_stop_seen;
static uint64_t serial_stop_at;
static bool serial_overflow;
static wl_col_session_t *serial_session;
static wl_columnar_memory_governor_t *serial_peak_governor;
static uint64_t serial_peak_bytes;

static void
serial_peak(wl_evaluation_control_t *c, wl_columnar_memory_governor_t *governor,
    bool growth, bool acquired)
{
    (void)c; (void)growth; (void)acquired;
    if (governor != serial_peak_governor) return;
    uint64_t bytes = atomic_load(&governor->reserved_bytes);
    if (bytes > serial_peak_bytes) serial_peak_bytes = bytes;
}
static void
serial_checkpoint(wl_evaluation_control_t *c, unsigned phase, uint64_t progress,
    wl_columnar_serial_output_t *output)
{
    if (phase != serial_stop_phase || progress < serial_stop_at
        || serial_stop_seen) return;
    serial_stop_seen++;
    wl_evaluation_control_report_t report;
    if (output && phase == 5) {
        CHECK(progress > 0 && progress <= 300);
        col_rel_t *first = output->slots[output->first_slot].relation;
        CHECK(first->row_scratch[progress - 1] ==
            first->columns[progress - 1][0]);
        CHECK(first->ncols == 300 && output->rows > 0);
    }
    /* Record actual admitted work at the reached checkpoint; active reports
     * are not available, so exact-limit cases derive from healthy runs. */
    (void)report;
    if (serial_overflow) CHECK(wl_evaluation_control_charge(c, UINT64_MAX)
            == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
serial_capture(const char *name, const int64_t *row, uint32_t width, void *data)
{
    serial_capture_t *capture = data;
    CHECK(row && width == capture->width);
    for (const unsigned char *p = (const unsigned char *)name; *p; p++)
        capture->digest = capture->digest * 1099511628211ULL ^ *p;
    for (uint32_t c = 0; c < width; c++) {
        CHECK(row[c] == (int64_t)(capture->count % capture->rows) * width + c);
        capture->digest = capture->digest * 1099511628211ULL ^ (uint64_t)row[c];
    }
    capture->count++;
    if (capture->session && capture->count == 1) {
        capture->live_bytes =
            string_reserved(capture->session->memory_governor);
        if (capture->check_pins) {
            col_rel_t *head = session_find_rel(capture->session, name);
            CHECK(head && col_rel_compact(head) == EBUSY);
            CHECK(col_rel_destroy_checked(head) == EBUSY);
        }
        if (capture->cancel_first) wl_evaluation_control_request_cancel(
                capture->session->base.evaluation_control);
    }
}
static wl_plan_t *
serial_generated(uint32_t width, uint32_t heads, bool chain,
    wirelog_program_t **program)
{
    size_t capacity = (size_t)(heads + 1u) * (width + 1u) * 160u + 1024u;
    char *text = malloc(capacity); CHECK(text);
    size_t at = 0;
    for (uint32_t h = 0; h <= heads; h++) {
        at += snprintf(text + at, capacity - at, h ? ".decl H%u(" : ".decl S(",
                h - 1u);
        for (uint32_t c = 0; c < width; c++) at += snprintf(text + at,
                    capacity - at, "%sv%u: int64", c ? "," : "", c);
        at += snprintf(text + at, capacity - at, ")\n");
    }
    for (uint32_t h = 0; h < heads; h++) {
        at += snprintf(text + at, capacity - at, "H%u(", h);
        for (uint32_t c = 0; c < width; c++) at += snprintf(text + at,
                    capacity - at, "%sv%u", c ? "," : "", c);
        if (chain && h) at += snprintf(text + at, capacity - at,
                    ") :- H%u(", h - 1u);
        else at += snprintf(text + at, capacity - at, ") :- S(");
        for (uint32_t c = 0; c < width; c++) at += snprintf(text + at,
                    capacity - at, "%sv%u", c ? "," : "", c);
        at += snprintf(text + at, capacity - at, ").\n");
    }
    CHECK(at < capacity);
    wirelog_error_t error;
    *program = wirelog_parse_string(text, &error); free(text);
    CHECK(*program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL;
    CHECK(wl_plan_from_program(*program, &plan) == 0);
    uint32_t actual_heads = 0;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        CHECK(!plan->strata[si].is_recursive);
        actual_heads += plan->strata[si].relation_count;
    }
    CHECK(actual_heads == heads);
    return plan;
}
static wl_col_session_t *
serial_create(const wl_plan_t *plan, uint32_t width, uint32_t rows,
    uint64_t budget)
{
    wl_session_t *base = NULL;
    if (budget) {
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = budget, .usable_bytes = budget,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        };
        wl_columnar_memory_governor_ref_t *governor =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(governor);
        wl_session_options_t options;
        wl_session_options_init(&options);
        options.memory_governor = governor;
        int create_rc = wl_session_create_with_options(wl_backend_columnar(),
                plan, 1,
                &options, &base);
        if (create_rc) fprintf(stderr,
                "serial enforcing construction: rc=%d budget=%llu\n", create_rc,
                (unsigned long long)budget);
        CHECK(create_rc == 0);
        wl_columnar_memory_governor_ref_release(governor);
    } else CHECK(wl_session_create(wl_backend_columnar(), plan, 1, &base) == 0);
    wl_col_session_t *sess = COL_SESSION(base);
    CHECK(sess->eval_arena && sess->delta_pool && !sess->has_evaluated);
    CHECK(sess->nrels == plan->edb_count && !session_find_rel(sess, "H0"));
    int64_t *data = malloc(((size_t)width * rows + 1u) * sizeof(*data));
    CHECK(data);
    for (uint64_t i = 0; i < (uint64_t)width * rows; i++) data[i] = (int64_t)i;
    CHECK(wl_backend_columnar()->session_insert(base, "S", data, rows,
        width) == 0);
    free(data);
    return sess;
}
static wl_evaluation_control_t *
serial_attach(wl_col_session_t *sess, uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(limit, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, sess->base.operation_admission) == 0);
    sess->base.evaluation_control = c;
    return c;
}
static void
serial_destroy(wl_col_session_t *sess)
{
    wl_evaluation_control_t *c = sess->base.evaluation_control;
    if (c) {
        CHECK(wl_evaluation_control_detach(c,
            sess->base.operation_admission) == 0);
        sess->base.evaluation_control = NULL;
        wl_evaluation_control_release(c);
    }
    wl_session_destroy(&sess->base);
}
static uint64_t
serial_trial(uint32_t width, uint32_t rows, uint64_t limit, int expected,
    unsigned phase, bool overflow, bool late_cancel)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = serial_generated(width, 2, true, &program);
    CHECK(plan->stratum_count >= 2);
    wl_col_session_t *sess = serial_create(plan, width, rows, 0),
        *raw = serial_create(plan, width, rows, 0);
    wl_arena_t *arena = sess->eval_arena;
    delta_pool_t *pool = sess->delta_pool;
    wl_evaluation_control_t *c = serial_attach(sess, limit);
    serial_capture_t got = {.rows = rows, .width = width, .session = sess,
                            .cancel_first = late_cancel,
                            .check_pins = rows > 0 && width > 0},
                                reference = {.rows = rows, .width = width};
    serial_stop_phase = phase; serial_stop_at = phase == 5 ? 128 : 0;
    serial_stop_seen = 0; serial_overflow = overflow; serial_session = sess;
    wl_columnar_session_test_serial_checkpoint = serial_checkpoint;
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base, serial_capture,
        &got) == expected);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c,
        &report) == 0 && report.result == expected);
    uint64_t cost = report.consumed;
    if (phase) CHECK(serial_stop_seen == 1);
    wl_columnar_session_test_serial_checkpoint = NULL;
    if (expected) {
        CHECK(got.count == 0 && sess->serial_completion.pending);
        CHECK(wl_backend_columnar()->session_step(&sess->base) == EBUSY
            || expected == WL_EVALUATION_CONTROL_CANCELLED ||
            expected == EOVERFLOW);
        int64_t value = 99;
        CHECK(wl_backend_columnar()->session_insert(&sess->base, "S", &value,
            0, width) == 0);
        CHECK(wl_backend_columnar()->session_insert(&sess->base, "S", &value,
            1, 1) == EBUSY);
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_evaluation_control_configure(c, 0) == 0);
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture,
            &got) == 0);
    }
    CHECK(sess->eval_arena == arena && sess->delta_pool == pool);
    CHECK(wl_backend_columnar()->session_snapshot(&raw->base, serial_capture,
        &reference) == 0);
    CHECK(got.count == reference.count && got.digest == reference.digest);
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            col_rel_t *head = session_find_rel(sess, sp->relations[ri].name),
                *twin = session_find_rel(raw, sp->relations[ri].name);
            CHECK(head && twin && head->nrows == twin->nrows);
            payload_equal(head, twin, 0);
        }
    }
    if (late_cancel) {
        got.count = 0;
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture,
            &got) == WL_EVALUATION_CONTROL_CANCELLED && !got.count);
    }
    CHECK(!sess->serial_completion.pending && !sess->serial_completion.frame
        && !sess->cleanup_pending);
    serial_destroy(sess); serial_destroy(raw);
    wl_plan_free(plan); wirelog_program_free(program);
    return cost;
}
static void
test_serial_initial_attempts(void)
{
    uint64_t cost = serial_trial(2, 3, 0, 0, 0, false, false);
    CHECK(serial_trial(2, 3, cost, 0, 0, false, false) == cost);
    serial_trial(2, 3, cost - 1, WL_EVALUATION_CONTROL_WORK_LIMIT, 0, false,
        false);
    serial_trial(300, 3, 0, WL_EVALUATION_CONTROL_CANCELLED, 5, false, false);
    serial_trial(300, 3, 0, EOVERFLOW, 5, true, false);
    serial_trial(300, 3, 0, WL_EVALUATION_CONTROL_CANCELLED, 6, false, false);
    serial_trial(300, 3, 0, 0, 0, false, true);
    serial_trial(2, 0, 0, 0, 0, false, false);
    serial_trial(0, 1, 0, 0, 0, false, false);
    puts(
        "serial attempt: generated default missing heads, real multistrata, width300 gather stop, exact allowance and late cancellation passed");
}

static uint64_t
serial_step_trial(uint64_t limit, int expected, bool cancel_bookkeeping)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = serial_generated(2, 2, true, &program);
    wl_col_session_t *sess = serial_create(plan, 2, 3, 0),
        *raw = serial_create(plan, 2, 3, 0);
    wl_evaluation_control_t *c = serial_attach(sess, limit);
    serial_stop_phase = cancel_bookkeeping ? 3 : 0;
    serial_stop_at = 0; serial_stop_seen = 0; serial_overflow = false;
    wl_columnar_session_test_serial_checkpoint = serial_checkpoint;
    CHECK(wl_backend_columnar()->session_step(&sess->base) == expected);
    wl_columnar_session_test_serial_checkpoint = NULL;
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c,
        &report) == 0 && report.result == expected);
    if (cancel_bookkeeping) CHECK(serial_stop_seen == 1);
    if (expected) {
        CHECK(sess->serial_completion.pending &&
            !sess->serial_completion.snapshot);
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_evaluation_control_configure(c, 0) == 0);
        CHECK(wl_backend_columnar()->session_step(&sess->base) == 0);
    }
    CHECK(wl_backend_columnar()->session_step(&raw->base) == 0);
    for (unsigned i = 0; i < 2; i++) {
        const char *name = i ? "H1" : "H0";
        col_rel_t *head = session_find_rel(sess, name),
            *twin = session_find_rel(raw, name);
        CHECK(head && twin && head->nrows == 3 && head->base_nrows == 3);
        payload_equal(head, twin, 0);
    }
    CHECK(sess->has_evaluated && sess->snapshot_stable_valid
        && !sess->serial_completion.pending && !sess->serial_completion.frame);
    serial_destroy(sess); serial_destroy(raw);
    wl_plan_free(plan); wirelog_program_free(program);
    return report.consumed;
}
static void
test_serial_initial_steps(void)
{
    uint64_t cost = serial_step_trial(0, 0, false);
    CHECK(serial_step_trial(cost, 0, false) == cost);
    serial_step_trial(cost - 1, WL_EVALUATION_CONTROL_WORK_LIMIT, false);
    serial_step_trial(0, WL_EVALUATION_CONTROL_CANCELLED, true);
    puts(
        "serial attempt: real backend STEP exact/minus-one allowance and bounded bookkeeping stop/raw retry passed");
}

static unsigned serial_plan_visits;
static col_rel_t *serial_second_target;
static void
serial_second_plan_stop(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *entry)
{
    (void)stack;
    CHECK(entry->rel);
    if (++serial_plan_visits == 2) {
        col_rel_t *first = session_find_rel(sess, "H0");
        CHECK(first && first->nrows == 3);
        wl_evaluation_control_request_cancel(sess->base.evaluation_control);
    }
}
static void
serial_consumed_reader(wl_evaluation_control_t *control, int phase,
    wl_columnar_relation_bulk_append_t *record)
{
    (void)control;
    if (phase != 2 || record->dst != serial_second_target) return;
    CHECK(record->published && !record->active);
    eval_entry_t *entry = wl_columnar_eval_stack_cleanup_result(
        serial_session->cleanup_active);
    CHECK(entry->owned && entry->rel && entry->rel != record->dst);
    CHECK(col_rel_source_reader_acquire(entry->rel,
        &join_retained_reader) == 0);
    serial_plan_visits++;
}
static void
test_serial_rule_continuation(void)
{
    for (unsigned lane = 0; lane < 3; lane++) {
        wirelog_program_t *program = NULL;
        wl_plan_t *generated = serial_generated(2, 2, lane == 0, &program);
        /* This is a separately supplied immutable legacy layout, borrowing
         * valid operator programs. No generated stratum is modified. */
        wl_plan_relation_t rules[2];
        uint32_t count = 0;
        for (uint32_t si = 0; si < generated->stratum_count; si++)
            for (uint32_t ri = 0; ri < generated->strata[si].relation_count;
                ri++)
                rules[count++] = generated->strata[si].relations[ri];
        CHECK(count == 2);
        if (strcmp(rules[0].name, "H0")) {
            wl_plan_relation_t swap = rules[0]; rules[0] = rules[1];
            rules[1] = swap;
        }
        const wl_plan_stratum_t stratum = {.stratum_id = 0,
                                           .is_recursive = false,
                                           .is_monotone = true,
                                           .relations = rules,
                                           .relation_count = 2};
        wl_plan_t supplied = *generated;
        supplied.strata = &stratum; supplied.stratum_count = 1;
        const wl_plan_t *plan = lane ? &supplied : generated;
        wl_col_session_t *sess = serial_create(plan, 2, 3, 0),
            *raw = serial_create(plan, 2, 3, 0);
        if (lane == 2) {
            publish_named(sess, "H0", 2, 64, 0);
            serial_second_target = publish_named(sess, "H1", 2, 64, 0);
            publish_named(raw, "H0", 2, 64, 0);
            publish_named(raw, "H1", 2, 64, 0);
        }
        wl_evaluation_control_t *c = serial_attach(sess, 0);
        serial_capture_t got = {.rows = 3, .width = 2},
                reference = {.rows = 3, .width = 2};
        serial_session = sess; serial_plan_visits = 0;
        if (lane ==
            2) wl_columnar_relation_test_bulk_checkpoint =
                serial_consumed_reader;
        else wl_columnar_eval_serial_test_after_plan = serial_second_plan_stop;
        int expected = lane == 2 ? EBUSY : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture, &got) == expected);
        wl_columnar_relation_test_bulk_checkpoint = NULL;
        wl_columnar_eval_serial_test_after_plan = NULL;
        CHECK(!got.count && session_find_rel(sess, "H0")->nrows == 3);
        CHECK(sess->serial_completion.pending);
        if (lane == 2) {
            CHECK(serial_plan_visits == 1 &&
                sess->serial_completion.relation == 2);
            CHECK(serial_second_target->nrows == 3 && sess->cleanup_pending);
            CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
        } else if (lane == 1) CHECK(sess->serial_completion.relation == 1);
        else CHECK(sess->serial_completion.stratum == 1);
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture, &got) == 0);
        CHECK(wl_backend_columnar()->session_snapshot(&raw->base,
            serial_capture, &reference) == 0);
        CHECK(got.count == reference.count && got.digest == reference.digest);
        CHECK(session_find_rel(sess, "H0")->nrows == 3
            && session_find_rel(sess, "H1")->nrows == 3);
        serial_destroy(sess); serial_destroy(raw);
        wl_plan_free(generated); wirelog_program_free(program);
    }
    puts(
        "serial attempt: genuine generated later-stratum stop and supplied legacy same-stratum stop/consumed-reader refusal retry passed");
}

typedef struct {
    serial_capture_t capture;
    const wl_plan_relation_t *rules;
    uint32_t occurrences;
    uint64_t rows_per_head;
} serial_occurrence_capture_t;
static void
serial_occurrence_capture(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    serial_occurrence_capture_t *capture = data;
    uint64_t occurrence = capture->capture.count / capture->rows_per_head;
    CHECK(occurrence < capture->occurrences);
    CHECK(name == capture->rules[occurrence].name);
    serial_capture(name, row, width, &capture->capture);
}
static void
test_serial_occurrences_and_cohort(void)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *generated = serial_generated(2, 2, false, &program);
    const wl_plan_relation_t *prototype = &generated->strata[0].relations[0];
    enum { OCCURRENCES = 66, NAME_BYTES = 700 };
    wl_plan_relation_t rules[OCCURRENCES];
    char *names[OCCURRENCES];
    for (unsigned i = 0; i < OCCURRENCES; i++) {
        names[i] = malloc(NAME_BYTES + 1u); CHECK(names[i]);
        memset(names[i], 'a', NAME_BYTES);
        names[i][NAME_BYTES - 1u] = i % 2u ? 'B' : 'A';
        names[i][NAME_BYTES] = 0;
        rules[i] = *prototype; rules[i].name = names[i];
        rules[i].delta_name = NULL;
    }
    const wl_plan_stratum_t stratum = {.stratum_id = 0,
                                       .is_recursive = false,
                                       .is_monotone = true,
                                       .relations = rules,
                                       .relation_count = OCCURRENCES};
    wl_plan_t supplied = *generated;
    supplied.strata = &stratum; supplied.stratum_count = 1;
    wl_col_session_t *sess = serial_create(&supplied, 2, 2, 0),
        *raw = serial_create(&supplied, 2, 2, 0);
    serial_attach(sess, 0);
    serial_occurrence_capture_t got = {.capture = {.rows = 2, .width = 2},
                                       .rules = rules,
                                       .occurrences = OCCURRENCES,
                                       .rows_per_head = OCCURRENCES},
                                           reference = got;
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
        serial_occurrence_capture, &got) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(&raw->base,
        serial_occurrence_capture, &reference) == 0);
    CHECK(got.capture.count == reference.capture.count
        && got.capture.digest == reference.capture.digest
        && got.capture.count == (uint64_t)OCCURRENCES * OCCURRENCES);
    serial_destroy(sess); serial_destroy(raw);
    for (unsigned i = 0; i < OCCURRENCES; i++) free(names[i]);
    wl_plan_free(generated); wirelog_program_free(program);
    for (unsigned heads = 64; heads <= 65; heads++) {
        generated = serial_generated(2, heads, false, &program);
        sess = serial_create(generated, 2, 2, 0);
        wl_evaluation_control_t *control = serial_attach(sess, 0);
        uint64_t baseline = string_reserved(sess->memory_governor);
        serial_capture_t capture = {.rows = 2, .width = 2};
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture, &capture) == (heads == 64 ? 0 : ENOTSUP));
        if (heads == 65) CHECK(!capture.count &&
                !sess->serial_completion.pending
                && !sess->serial_completion.frame && !sess->cleanup_pending
                && sess->nrels == generated->edb_count
                && string_reserved(sess->memory_governor) == baseline);
        else {
            CHECK(capture.count == 128 && sess->rel_cap > 16);
            CHECK(sess->rels_reservation.identity == &sess->rels_reservation
                && atomic_load(&sess->rels_reservation.state)
                == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
            bool collision = false;
            for (uint32_t i = 0; i < sess->nrels; i++) {
                CHECK(session_find_rel(sess,
                    sess->rels[i]->name) == sess->rels[i]);
                if (sess->rel_hash_next[i] != UINT32_MAX) collision = true;
            }
            CHECK(collision);
        }
        (void)control;
        serial_destroy(sess); wl_plan_free(generated);
        wirelog_program_free(program);
    }
    puts(
        "serial attempt: supplied immutable A/B/A independent long-name occurrence pointers,66 occurrences/two heads and64/65 distinct-head preflight passed");
}

static void
test_serial_output_budget(void)
{
    wirelog_program_t *program = NULL;
    enum { WIDTH = 300, HEADS = 16, ROWS = 33 };
    wl_plan_t *plan = serial_generated(WIDTH, HEADS, false, &program);
    wl_col_session_t *probe = serial_create(plan, WIDTH, ROWS, 0);
    serial_attach(probe, 0);
    serial_peak_governor =
        wl_columnar_memory_governor_ref_get(probe->memory_governor);
    serial_peak_bytes = string_reserved(probe->memory_governor);
    wl_columnar_memory_governor_test_after_total_cas = serial_peak;
    serial_capture_t measured = {.rows = ROWS, .width = WIDTH,
                                 .session = probe};
    CHECK(wl_backend_columnar()->session_snapshot(&probe->base,
        serial_capture, &measured) == 0);
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    serial_peak_governor = NULL;
    uint64_t budget = serial_peak_bytes;
    uint64_t obsolete_cell_image = (uint64_t)WIDTH * HEADS * ROWS *
        sizeof(int64_t);
    CHECK(measured.count == HEADS * ROWS && measured.live_bytes <= budget);
    /* The obsolete R2 cell image alone exceeds actual remaining credit at
    * output publication. Its other staging allocations only add bytes. */
    CHECK(obsolete_cell_image > budget - measured.live_bytes);
    serial_destroy(probe);
    wl_col_session_t *sess = serial_create(plan, WIDTH, ROWS, budget),
        *raw = serial_create(plan, WIDTH, ROWS, budget);
    serial_attach(sess, 0); /* Work allowance zero means unlimited, not bypassed memory. */
    serial_capture_t got = {.rows = ROWS, .width = WIDTH}, reference = got;
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
        serial_capture, &got) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(&raw->base,
        serial_capture, &reference) == 0);
    CHECK(got.count == measured.count && reference.count == got.count
        && reference.digest == got.digest);
    serial_destroy(sess); serial_destroy(raw);
    wl_plan_free(plan); wirelog_program_free(program);
    puts(
        "serial attempt: real enforcing budget admits raw/unlimited-work output while obsolete full cell image exceeds actual publication credit passed");
}

static col_rel_t *serial_optional_target;
static uint64_t serial_optional_budget, serial_optional_live;
static unsigned serial_optional_denials, serial_optional_stop;
static void
serial_optional_denied(wl_evaluation_control_t *control, int phase,
    wl_columnar_relation_bulk_append_t *record)
{
    if (phase != 7 || record->dst != serial_optional_target) return;
    CHECK(record->active && record->compact && !record->published
        && !record->transaction_active && !record->pending.growth
        && !record->prepared_columns && record->writer.owner
        && record->descriptor_writer.owner);
    col_rel_t *rel = record->dst;
    CHECK(atomic_load(&rel->retained_reservation.state)
        == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && !rel->retained_reservation.replacement_bytes);
    serial_optional_live = string_reserved(rel->memory_governor);
    uint64_t incoming = (uint64_t)rel->ncols *
        (COL_REL_INIT_CAP * sizeof(int64_t) + sizeof(int64_t *));
    CHECK(serial_optional_live <= serial_optional_budget
        && incoming > serial_optional_budget - serial_optional_live);
    serial_optional_denials++;
    if (serial_optional_stop ==
        1) wl_evaluation_control_request_cancel(control);
    if (serial_optional_stop == 2) CHECK(wl_evaluation_control_charge(control,
            UINT64_MAX) == EOVERFLOW);
}
static void
test_serial_optional_compaction_denial(void)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = serial_generated(2, 2, true, &program);
    CHECK(plan->stratum_count >= 2);
    wl_col_session_t *probe = serial_create(plan, 2, 3, 0);
    publish_named(probe, "oversized", 300, 1000, 3);
    /* Keep normal generated evaluation's frame and small heap operators
     * admissible, while the real wide replacement needs over150KiB. */
    uint64_t budget = string_reserved(probe->memory_governor) + 64u * 1024u;
    serial_destroy(probe);
    for (unsigned lane = 0; lane < 4; lane++) {
        bool snapshot = lane != 0;
        unsigned stop = lane > 1 ? lane - 1u : 0;
        wl_col_session_t *sess = serial_create(plan, 2, 3, budget),
            *raw = serial_create(plan, 2, 3, budget);
        col_rel_t *target = publish_named(sess, "oversized", 300, 1000, 3),
            *twin = publish_named(raw, "oversized", 300, 1000, 3);
        uint64_t identity = target->relation_identity,
            storage = target->storage_generation,
            payload_credit = target->retained_reserved_bytes;
        int64_t **columns = target->columns;
        if (lane == 1) {
            /* Labeled preexisting telemetry marker: test compactor-boundary
             * preservation separately from raw session entry's flag reset. */
            target->memory_budget_denial_pending = true;
            twin->memory_budget_denial_pending = true;
            wl_evaluation_control_t *direct = control(0);
            sess->base.evaluation_control = direct;
            wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
            CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
            wl_columnar_relation_bulk_append_t *record =
                wl_columnar_eval_stack_cleanup_bulk_append(frame);
            serial_optional_target = target; serial_optional_budget = budget;
            serial_optional_denials = 0; serial_optional_stop = 0;
            wl_columnar_relation_test_bulk_checkpoint = serial_optional_denied;
            CHECK(wl_columnar_relation_compact_controlled(record, target,
                direct) == 0);
            wl_columnar_relation_test_bulk_checkpoint = NULL;
            CHECK(serial_optional_denials == 1 && !record->active);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            finish(direct, 0); release(direct);
            sess->base.evaluation_control = NULL;
            CHECK(col_rel_compact_many(&twin, 1) == 0);
            CHECK(target->memory_budget_denial_pending &&
                twin->memory_budget_denial_pending
                && target->capacity == 1000 && twin->capacity == 1000);
            target->memory_budget_denial_pending = false;
            twin->memory_budget_denial_pending = false;
        }
        wl_evaluation_control_t *control = serial_attach(sess, 0);
        serial_optional_target = target; serial_optional_budget = budget;
        serial_optional_denials = 0; serial_optional_stop = stop;
        serial_peak_governor =
            wl_columnar_memory_governor_ref_get(sess->memory_governor);
        serial_peak_bytes = string_reserved(sess->memory_governor);
        wl_columnar_memory_governor_test_after_total_cas = serial_peak;
        wl_columnar_relation_test_bulk_checkpoint = serial_optional_denied;
        serial_capture_t got = {.rows = 3, .width = 2}, reference = got;
        int expected = stop == 1 ? WL_EVALUATION_CONTROL_CANCELLED
            : stop == 2 ? EOVERFLOW : 0;
        CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture,
            &got) : wl_backend_columnar()->session_step(&sess->base))
            == expected);
        CHECK(serial_optional_denials == 1 && serial_peak_bytes <= budget);
        if (stop) {
            CHECK(!got.count && sess->serial_completion.pending);
            CHECK(wl_evaluation_control_reset(control) == 0);
            serial_optional_stop = 0;
            CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
                serial_capture, &got) == 0);
            CHECK(serial_optional_denials == 2);
        }
        wl_columnar_relation_test_bulk_checkpoint = NULL;
        wl_columnar_memory_governor_test_after_total_cas = NULL;
        serial_peak_governor = NULL;
        CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&raw->base,
            serial_capture,
            &reference) : wl_backend_columnar()->session_step(&raw->base)) ==
            0);
        CHECK(got.count == reference.count && got.digest == reference.digest
            && got.count == (snapshot ? 6u : 0u));
        CHECK(target->columns == columns && target->capacity == 1000
            && target->nrows == 3 && target->storage_generation == storage
            && target->relation_identity == identity
            && target->retained_reserved_bytes == payload_credit
            && twin->capacity == target->capacity &&
            twin->nrows == target->nrows
            && !target->memory_budget_denial_pending
            && !twin->memory_budget_denial_pending);
        payload_equal(target, twin, 0);
        for (uint32_t i = 0; i < sess->nrels; i++) {
            col_rel_t *rel = sess->rels[i];
            CHECK(rel && atomic_load(&rel->retained_reservation.state)
                == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                && !rel->retained_reservation.replacement_bytes);
        }
        CHECK(!sess->serial_completion.pending && !sess->serial_completion.frame
            && !sess->cleanup_pending && !sess->cleanup_active
            && !sess->cleanup_reserved_bytes
            && string_reserved(sess->memory_governor) <= budget
            && string_reserved(raw->memory_governor) <= budget);
        CHECK(session_find_rel(sess, "H0")->nrows == 3
            && session_find_rel(sess, "H1")->nrows == 3);
        serial_destroy(sess); serial_destroy(raw);
    }
    wl_plan_free(plan); wirelog_program_free(program);
    puts(
        "serial attempt: actual optional overlap DENIED preserves payload/credit/latch and completes raw/control STEP/SNAPSHOT, with cancellation/overflow retry passed");
}

static wl_columnar_source_access_reader_t *serial_original_reader;
static uintptr_t serial_original_identity;
static unsigned serial_ownership_seen;
static void
serial_partial_reader_stop(wl_evaluation_control_t *control,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    if (!acquired || serial_ownership_seen
        || serial_session->serial_completion.mode !=
        WL_COLUMNAR_SERIAL_FRAME_OUTPUT)
        return;
    col_rel_t *head = session_find_rel(serial_session, "H0");
    if (!head || gate != &head->descriptor_access) return;
    CHECK(atomic_load(&head->descriptor_access.state) == 1
        && !atomic_load(&head->source_access.state));
    serial_ownership_seen++;
    wl_evaluation_control_request_cancel(control);
}
static void
serial_output_identity_fault(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, wl_columnar_serial_output_t *output)
{
    (void)control; (void)progress;
    if (phase != 7 || !output) return;
    CHECK(output->count == 2 && output->initialized == 2);
    for (unsigned i = 0; i < output->count; i++) {
        CHECK(output->slots[i].reader.owner ==
            &output->slots[i].relation->source_access
            && output->slots[i].reader.secondary_owner
            == &output->slots[i].relation->descriptor_access);
    }
    serial_original_reader = &output->slots[0].reader;
    serial_original_identity = serial_original_reader->identity;
    CHECK(serial_original_identity == (uintptr_t)serial_original_reader);
    serial_original_reader->identity = 0; /* Labeled original-token validation fault. */
    serial_ownership_seen++;
}
static void
test_serial_output_ownership(void)
{
    for (unsigned lane = 0; lane < 3; lane++) {
        wirelog_program_t *program = NULL;
        wl_plan_t *plan = serial_generated(2, 2, false, &program);
        wl_col_session_t *sess = serial_create(plan, 2, lane == 2 ? 0 : 3, 0);
        wl_evaluation_control_t *c = serial_attach(sess, 0);
        serial_session = sess; serial_ownership_seen = 0;
        serial_capture_t got = {.rows = 3, .width = 2};
        if (!lane) wl_columnar_relation_test_after_reader_cas =
                serial_partial_reader_stop;
        else wl_columnar_session_test_serial_checkpoint =
                serial_output_identity_fault;
        int expected = lane ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture, &got) == expected);
        wl_columnar_relation_test_after_reader_cas = NULL;
        wl_columnar_session_test_serial_checkpoint = NULL;
        CHECK(serial_ownership_seen == 1 && sess->serial_completion.pending);
        if (!lane) {
            CHECK(!got.count);
            col_rel_t *head = session_find_rel(sess, "H0");
            CHECK(!atomic_load(&head->descriptor_access.state)
                && !atomic_load(&head->source_access.state));
        } else {
            CHECK(got.count == (lane == 2 ? 0u : 6u));
            CHECK(sess->serial_completion.phase == 5
                && serial_original_reader->owner
                && serial_original_reader->secondary_owner);
            serial_original_reader->identity = serial_original_identity;
        }
        uint64_t delivered = got.count;
        CHECK(wl_evaluation_control_reset(c) == 0);
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture, &got) == 0);
        CHECK(got.count == (lane ? delivered : 6u));
        CHECK(!sess->serial_completion.pending &&
            !sess->serial_completion.frame);
        serial_destroy(sess); wl_plan_free(plan); wirelog_program_free(program);
    }
    puts(
        "serial attempt: original descriptor-only stop, combined-token consumed cleanup fault and empty selected-head pins passed");
}

static wl_columnar_relation_bulk_append_t *serial_compact_record;
static unsigned serial_compact_visits;
static void
serial_compact_stop(wl_evaluation_control_t *control, uint64_t units)
{
    if (!serial_compact_record || !serial_compact_record->compact
        || !serial_compact_record->writer.owner
        || !serial_compact_record->descriptor_writer.owner ||
        units != 8) return;
    if (++serial_compact_visits ==
        128) wl_evaluation_control_request_cancel(control);
}
static void
test_serial_compact_and_registry_hole(void)
{
    const uint32_t widths[] = {0, 2, 2, 2, 300};
    const uint32_t capacities[] = {64, 64, 64, 1000, 1000};
    const uint32_t rows[] = {1, 0, 33, 3, 3};
    for (unsigned i = 0; i < 5; i++) {
        wl_col_session_t *sess = join_session();
        col_rel_t *rel = relation(widths[i], capacities[i], rows[i], false,
                sess->memory_governor),
            *raw = relation(widths[i], capacities[i],
                rows[i], false, sess->memory_governor);
        wl_evaluation_control_t *c = control(0);
        sess->base.evaluation_control = c;
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
        wl_columnar_relation_bulk_append_t *record =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        if (!widths[i]) CHECK(col_rel_source_reader_acquire(rel,
                &join_retained_reader) == 0);
        if (i == 4) {
            serial_compact_record = record; serial_compact_visits = 0;
            wl_columnar_relation_test_work_checkpoint = serial_compact_stop;
            CHECK(wl_columnar_relation_compact_controlled(record, rel, c)
                == WL_EVALUATION_CONTROL_CANCELLED);
            wl_columnar_relation_test_work_checkpoint = NULL;
            serial_compact_record = NULL;
            CHECK(serial_compact_visits == 128 && rel->capacity == capacities[i]
                && rel->nrows == rows[i] && !record->active);
            CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
            finish(c, WL_EVALUATION_CONTROL_CANCELLED);
            retry(c, 0);
            CHECK(wl_columnar_eval_stack_cleanup_begin(sess, &frame) == 0);
            record = wl_columnar_eval_stack_cleanup_bulk_append(frame);
        }
        CHECK(wl_columnar_relation_compact_controlled(record, rel, c) == 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0);
        if (!widths[i]) CHECK(col_rel_source_reader_release(
                    &join_retained_reader) == 0);
        finish(c, 0); sess->base.evaluation_control = NULL;
        CHECK(col_rel_compact(raw) == 0);
        CHECK(rel->capacity == raw->capacity && rel->nrows == raw->nrows
            && rel->sorted_nrows == raw->sorted_nrows
            && rel->base_nrows == raw->base_nrows
            && !rel->merge_columns && !frame);
        payload_equal(rel, raw, 0);
        release(c); CHECK(col_rel_destroy_checked(rel) == 0);
        CHECK(col_rel_destroy_checked(raw) == 0); join_session_destroy(sess);
    }
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = serial_generated(2, 2, false, &program);
    wl_col_session_t *sess = serial_create(plan, 2, 3, 0),
        *raw = serial_create(plan, 2, 3, 0);
    wl_col_session_t *sessions[] = {sess, raw};
    for (unsigned i = 0; i < 2; i++) {
        col_rel_t *discard = publish_named(sessions[i], "discard", 2, 64, 0);
        uint32_t slot = sessions[i]->nrels - 1u;
        CHECK(sessions[i]->rels[slot] == discard);
        CHECK(col_rel_destroy_checked(discard) == 0);
        sessions[i]->rels[slot] = NULL;
        CHECK(session_rel_hash_remove(sessions[i], slot) == 0);
    }
    serial_attach(sess, 0);
    serial_capture_t got = {.rows = 3, .width = 2}, reference = got;
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
        serial_capture, &got) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(&raw->base,
        serial_capture, &reference) == 0);
    CHECK(got.count == reference.count && got.digest == reference.digest
        && sess->nrels == raw->nrels);
    serial_destroy(sess); serial_destroy(raw);
    wl_plan_free(plan); wirelog_program_free(program);
    puts(
        "serial attempt: real compact empty/zeroarity/tight/shrink/wide300 twins and registry-hole replacement passed");
}

static wl_columnar_source_access_writer_t *serial_reset_fault_writer;
static uintptr_t serial_reset_fault_identity;
static unsigned serial_reset_mode, serial_reset_seen[2];
static col_rel_t *serial_reset_heads[2];
static int64_t **serial_reset_columns[2];
static uint64_t serial_reset_credit[2], serial_reset_storage[2];
static void
serial_reset_checkpoint(wl_evaluation_control_t *control, int phase,
    wl_columnar_relation_bulk_append_t *record)
{
    if (!record->reset || (phase != 8 && phase != 9)) return;
    unsigned slot = record->dst == serial_reset_heads[0] ? 0 : 1;
    CHECK(record->dst == serial_reset_heads[slot]);
    if (phase == 8 && ((serial_reset_mode == 1 && !slot)
        || (serial_reset_mode == 7 && slot)))
        wl_evaluation_control_request_cancel(control);
    if (phase != 9) return;
    col_rel_t *head = record->dst;
    CHECK(record->published && record->active && !record->growth
        && !record->transaction_active && head->nrows == 0 &&
        head->base_nrows == 0
        && head->columns == serial_reset_columns[slot]
        && head->retained_reserved_bytes == serial_reset_credit[slot]
        && head->storage_generation == serial_reset_storage[slot] + 1u);
    serial_reset_seen[slot]++;
    if (serial_reset_mode == 2 && !slot) {
        serial_reset_fault_writer = &record->writer;
        serial_reset_fault_identity = record->writer.identity;
        CHECK(serial_reset_fault_identity ==
            (uintptr_t)serial_reset_fault_writer);
        record->writer.identity = 0; /* Labeled original-writer validation refusal. */
    }
}
static void
serial_repeat_after_plan(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *entry)
{
    (void)stack; CHECK(entry->rel);
    if (++serial_plan_visits == 2) {
        CHECK(session_find_rel(sess, "H0")->nrows == 6);
        wl_evaluation_control_request_cancel(sess->base.evaluation_control);
    }
}
static uint64_t
serial_repeat_trial(uint32_t width, bool snapshot, uint64_t limit,
    unsigned mode, bool supplied)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *generated = serial_generated(width, 2, !supplied, &program);
    wl_plan_relation_t rules[3]; uint32_t count = 0;
    for (uint32_t si = 0; si < generated->stratum_count; si++)
        for (uint32_t ri = 0; ri < generated->strata[si].relation_count; ri++)
            rules[count++] = generated->strata[si].relations[ri];
    CHECK(count == 2);
    if (strcmp(rules[0].name, "H0")) {
        wl_plan_relation_t swap = rules[0]; rules[0] = rules[1];
        rules[1] = swap;
    }
    rules[2] = rules[0];
    const wl_plan_stratum_t stratum = {.is_recursive = false,
                                       .is_monotone = true,
                                       .relations = rules, .relation_count = 3};
    wl_plan_t legacy = *generated; legacy.strata = &stratum;
    legacy.stratum_count = 1;
    const wl_plan_t *plan = supplied ? &legacy : generated;
    wl_col_session_t *sess = serial_create(plan, width, 3, 0),
        *raw = serial_create(plan, width, 3, 0);
    wl_evaluation_control_t *control = serial_attach(sess, 0);
    serial_capture_t initial = {.rows = 3, .width = width},
            initial_raw = initial;
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base, serial_capture,
        &initial) == 0);
    CHECK(wl_backend_columnar()->session_snapshot(&raw->base, serial_capture,
        &initial_raw) == 0);
    CHECK(initial.count == initial_raw.count &&
        initial.digest == initial_raw.digest);
    int64_t *data = malloc((size_t)width * 3u * sizeof(*data)); CHECK(data);
    for (uint64_t i = 0; i < (uint64_t)width * 3u;
        i++) data[i] = (int64_t)width * 3 + i;
    CHECK(wl_backend_columnar()->session_insert(&sess->base, "S", data, 3,
        width) == 0);
    CHECK(wl_backend_columnar()->session_insert(&raw->base, "S", data, 3,
        width) == 0);
    free(data);
    CHECK(sess->has_evaluated && sess->pending_input_change &&
        sess->pending_full_input_eval
        && session_find_rel(sess, "S")->nrows == 6);
    wl_arena_t *arena = sess->eval_arena; delta_pool_t *pool = sess->delta_pool;
    for (unsigned i = 0; i < 2; i++) {
        serial_reset_heads[i] = session_find_rel(sess, i ? "H1" : "H0");
        serial_reset_columns[i] = serial_reset_heads[i]->columns;
        serial_reset_credit[i] = serial_reset_heads[i]->retained_reserved_bytes;
        serial_reset_storage[i] = serial_reset_heads[i]->storage_generation;
        serial_reset_seen[i] = 0;
    }
    CHECK(wl_evaluation_control_reset(control) == 0);
    CHECK(wl_evaluation_control_configure(control, limit) == 0);
    serial_reset_mode = mode; serial_plan_visits = 0;
    serial_stop_seen = 0; serial_stop_at = mode == 10 ? 128 : 0;
    serial_stop_phase = mode == 3 ? 8 : mode == 4 ? 10 : mode == 10 ? 5 : 0;
    serial_overflow = false;
    wl_columnar_relation_test_bulk_checkpoint = serial_reset_checkpoint;
    wl_columnar_session_test_serial_checkpoint = serial_checkpoint;
    if (mode ==
        5) wl_columnar_eval_serial_test_after_plan = serial_repeat_after_plan;
    if (mode == 6) CHECK(col_rel_source_reader_acquire(serial_reset_heads[1],
            &join_retained_reader) == 0);
    serial_capture_t got = {.rows = 6, .width = width, .session = sess,
                            .cancel_first = mode == 11},
                                reference = {.rows = 6, .width = width};
    int expected = mode == 2 ? EINVAL : mode == 6 ? EBUSY
        : mode == 12 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : mode && mode != 11 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
    int result = snapshot ? wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture,
            &got) : wl_backend_columnar()->session_step(&sess->base);
    CHECK(result == expected);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(control,
        &report) == 0 && report.result == expected);
    wl_columnar_session_test_serial_checkpoint = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
    if (mode == 6) {
        CHECK(!serial_reset_seen[0] && !serial_reset_seen[1]);
        CHECK(col_rel_source_reader_release(&join_retained_reader) == 0);
    }
    if (expected) {
        CHECK(!got.count && sess->pending_input_change &&
            sess->pending_full_input_eval);
        if (mode == 2) {
            CHECK(sess->serial_completion.reset_relation == 1 &&
                sess->cleanup_pending
                && serial_reset_seen[0] == 1);
            serial_reset_fault_writer->identity = serial_reset_fault_identity;
        }
        serial_reset_mode = 0;
        CHECK(wl_evaluation_control_reset(control) == 0);
        CHECK(wl_evaluation_control_configure(control, 0) == 0);
        CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture,
            &got) : wl_backend_columnar()->session_step(&sess->base)) == 0);
    }
    wl_columnar_relation_test_bulk_checkpoint = NULL;
    CHECK(serial_reset_seen[0] == 1 && serial_reset_seen[1] == 1);
    CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&raw->base,
        serial_capture,
        &reference) : wl_backend_columnar()->session_step(&raw->base)) == 0);
    CHECK(got.count == reference.count && got.digest == reference.digest);
    for (unsigned i = 0; i < 2; i++) {
        col_rel_t *head = session_find_rel(sess, i ? "H1" : "H0"),
            *twin = session_find_rel(raw, i ? "H1" : "H0");
        CHECK(head->nrows == twin->nrows &&
            head->nrows == (supplied && !i ? 12u : 6u));
        payload_equal(head, twin, 0);
    }
    CHECK(sess->eval_arena == arena && sess->delta_pool == pool
        && !sess->serial_completion.pending && !sess->serial_completion.frame
        && !sess->pending_input_change && !sess->pending_full_input_eval
        && !sess->cleanup_pending && !sess->cleanup_reserved_bytes);
    if (mode == 11) {
        got.count = 0;
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            serial_capture,
            &got) == WL_EVALUATION_CONTROL_CANCELLED && !got.count);
    }
    serial_destroy(sess); serial_destroy(raw); wl_plan_free(generated);
    wirelog_program_free(program);
    return report.consumed;
}
static void
serial_repeat_unsupported(unsigned mode)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = serial_generated(2, 2, true, &program);
    wl_col_session_t *sess = serial_create(plan, 2, 3, 0);
    wl_evaluation_control_t *control = serial_attach(sess, 0);
    serial_capture_t capture = {.rows = 3, .width = 2};
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
        serial_capture, &capture) == 0);
    const int64_t rows[] = {6, 7, 8, 9, 10, 11};
    CHECK(wl_backend_columnar()->session_insert(&sess->base, "S", rows, 3,
        2) == 0);
    col_rel_t *first = session_find_rel(sess, "H0"),
        *later = session_find_rel(sess, "H1");
    if (mode == 0) {
        /* Genuine host insertion into IDB creates the seed shadow. */
        CHECK(wl_backend_columnar()->session_insert(&sess->base, "H1", rows, 1,
            2) == 0);
    } else if (mode == 1) {
        CHECK(col_rel_enable_timestamps(later) == 0);
    } else {
        wl_columnar_eval_dedup_set_insert(later, UINT64_C(123));
        CHECK(later->dedup_slots && later->dedup_reserved_bytes);
    }
    uint32_t first_rows = first->nrows, later_rows = later->nrows;
    uint64_t first_generation = first->storage_generation,
        later_generation = later->storage_generation;
    int64_t **first_columns = first->columns, **later_columns = later->columns;
    uint64_t first_credit = first->retained_reserved_bytes,
        later_credit = later->retained_reserved_bytes;
    CHECK(wl_evaluation_control_reset(control) == 0);
    capture.count = 0;
    CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
        serial_capture, &capture) == ENOTSUP);
    CHECK(!capture.count && first->nrows == first_rows &&
        later->nrows == later_rows
        && first->storage_generation == first_generation
        && later->storage_generation == later_generation
        && first->columns == first_columns && later->columns == later_columns
        && first->retained_reserved_bytes == first_credit
        && later->retained_reserved_bytes == later_credit
        && sess->pending_input_change && sess->pending_full_input_eval
        && !sess->serial_completion.pending && !sess->cleanup_pending);
    serial_destroy(sess); wl_plan_free(plan); wirelog_program_free(program);
}
static void
test_serial_repeat_attempts(void)
{
    serial_repeat_trial(2, false, 0, 0, false);
    uint64_t cost = serial_repeat_trial(2, true, 0, 0, false);
    CHECK(serial_repeat_trial(2, true, cost, 0, false) == cost);
    serial_repeat_trial(2, true, cost - 1, 12, false);
    for (unsigned mode = 1; mode <= 7;
        mode++) serial_repeat_trial(2, true, 0, mode, false);
    serial_repeat_trial(2, true, 0, 5, true);
    serial_repeat_trial(300, true, 0, 10, false);
    serial_repeat_trial(300, true, 0, 11, false);
    for (unsigned mode = 0; mode < 3; mode++) serial_repeat_unsupported(mode);
    puts(
        "serial repeat: genuine second full EDB insertion STEP/SNAPSHOT, original reset/credit/writer cleanup, frontier/later-head stops and width300 output passed");
}

static unsigned filtered_mode, filtered_seen, filtered_plans, filtered_compiles;
static wl_columnar_memory_reservation_t *filtered_fault_token;
static wl_columnar_expr_operator_preparation_t *filtered_fault_record;
static uintptr_t filtered_fault_identity;
static wl_evaluation_control_t *filtered_control;
static wl_col_session_t *filtered_session;
static wl_columnar_relation_private_batch_t *filtered_batch;
static unsigned filtered_filter_drain, filtered_map_reuse;
static void
filtered_filter_observe(wl_evaluation_control_t *c, int phase,
    uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    if (phase == 9) {
        filtered_batch =
            wl_columnar_eval_stack_cleanup_reduce_batch(
            filtered_session->cleanup_active);
        CHECK(filtered_batch && filtered_batch->batch.active
            && filtered_batch->batch.single.lease.acquisition_nonce
            && filtered_batch->batch.single.descriptor.writer.identity
            == (uintptr_t)&filtered_batch->batch.single.descriptor.writer);
        if (source->nrows == 769 &&
            filtered_mode !=
            14) CHECK(filtered_batch->batch.relation->nrows == 513
                && filtered_batch->batch.relation->capacity >= 1024);
    }
    if (phase == 10) {
        CHECK(filtered_batch &&
            wl_columnar_relation_private_batch_disposable(filtered_batch)
            && !group->descriptor_count && !group->owner_count);
        filtered_filter_drain++;
    }
    if (filtered_mode == 1 || filtered_mode == 2)
        filter_stage_stop(c, phase, progress, group, source);
}
static void
filtered_map_observe(wl_evaluation_control_t *c, int phase, uint64_t progress,
    wl_columnar_relation_source_group_t *group, col_rel_t *source)
{
    if (phase == 1) {
        CHECK(filtered_filter_drain && filtered_batch
            == wl_columnar_eval_stack_cleanup_reduce_batch(
                filtered_session->cleanup_active));
        CHECK(source && source->storage_owner == source && !source->pool_owned);
        filtered_map_reuse++;
    }
    if (phase ==
        10) CHECK(wl_columnar_relation_private_batch_disposable(filtered_batch)
            && !group->descriptor_count && !group->owner_count);
    if (filtered_mode == 3) map_stage_stop(c, phase, progress, group, source);
}
static void
filtered_retirement(wl_columnar_expr_operator_preparation_t *r, int phase)
{
    if (filtered_seen) return;
    if (filtered_mode == 12 && phase == 5) {
        CHECK(r->compile_admission.bytes && !r->current
            && atomic_load(&r->compile_admission.state) ==
            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
        filtered_seen++; wl_evaluation_control_request_cancel(filtered_control);
        return;
    }
    if ((filtered_mode == 13 || filtered_mode == 15) && phase == 6) {
        CHECK(r->current && r->compile_admission.bytes
            && atomic_load(&r->compile_admission.state) ==
            WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
        filtered_fault_record = r; filtered_fault_token = &r->compile_admission;
        filtered_fault_identity = (uintptr_t)filtered_fault_token->identity;
        CHECK(filtered_fault_identity == (uintptr_t)filtered_fault_token);
        filtered_fault_token->identity = NULL; /* Labeled original RESERVED admission fault. */
        filtered_seen++;
        if (filtered_mode == 15) CHECK(wl_evaluation_control_charge(
                    filtered_control, UINT64_MAX) == EOVERFLOW);
        return;
    }
    if ((filtered_mode == 6 && phase == 1)
        || (filtered_mode == 7 && phase == 2)
        || (filtered_mode == 8 && phase == 4 && r->scratch.bytes)) {
        filtered_fault_record = r;
        filtered_fault_token = filtered_mode ==
            8 ? &r->scratch : &r->retired_expression;
        filtered_fault_identity = (uintptr_t)filtered_fault_token->identity;
        CHECK(filtered_fault_identity == (uintptr_t)filtered_fault_token);
        if (filtered_mode == 7) CHECK(r->retired_expression.bytes
                && !r->current && !r->single);
        if (filtered_mode == 8) CHECK(!r->scratch_payload);
        filtered_fault_token->identity = NULL; /* Actual original identity-validation fault. */
        filtered_seen++;
    } else if (filtered_mode == 9 && phase == 3) {
        CHECK(r->retiring > 0); filtered_seen++;
        wl_evaluation_control_request_cancel(filtered_control);
    }
}
static void
filtered_compile(wl_evaluation_control_t *c, int stage, uint64_t progress,
    bool failed)
{
    (void)failed;
    if (stage == 4) filtered_compiles++;
    if (filtered_mode == 10 && stage == 2 && progress > 0 && !filtered_seen) {
        filtered_seen++;
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    }
}
static void
filtered_after_plan(wl_col_session_t *sess, eval_stack_t *stack,
    eval_entry_t *entry)
{
    (void)stack;
    if (++filtered_plans == 2) {
        CHECK(entry->rel && session_find_rel(sess, "H0")->nrows);
        filtered_seen++;
        wl_evaluation_control_request_cancel(sess->base.evaluation_control);
    }
}
static void
filtered_capture(const char *name, const int64_t *row, uint32_t width,
    void *data)
{
    serial_capture_t *capture = data; CHECK(width == 1 && row);
    for (const unsigned char *p = (const unsigned char *)name; *p; p++)
        capture->digest = capture->digest * UINT64_C(1099511628211) ^ *p;
    capture->digest = capture->digest *
        UINT64_C(1099511628211) ^ (uint64_t)row[0];
    capture->count++;
}
static uint64_t
filtered_trial(uint32_t rows, bool repeat, bool snapshot, unsigned mode,
    uint64_t limit)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(
        ".decl S(x: int64,y: int64)\n.decl H0(x: int64)\n.decl H1(x: int64)\n"
        "H0(x+1) :- S(x,y), x+1 > y.\nH1(x) :- H0(x), x > 0.\n", &error);
    CHECK(program && error == WIRELOG_OK);
    wl_plan_t *plan = NULL; CHECK(wl_plan_from_program(program, &plan) == 0);
    CHECK(plan->stratum_count == 2);
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        CHECK(!sp->is_recursive && sp->relation_count == 1);
        const wl_plan_relation_t *rp = &sp->relations[0];
        CHECK(rp->op_count == 4 && rp->ops[0].op == WL_PLAN_OP_VARIABLE
            && rp->ops[0].delta_mode == WL_DELTA_AUTO &&
            rp->ops[1].op == WL_PLAN_OP_FILTER
            && rp->ops[2].op == WL_PLAN_OP_MAP &&
            rp->ops[3].op == WL_PLAN_OP_CONSOLIDATE);
        if (!si) CHECK(rp->ops[2].map_exprs && rp->ops[2].map_expr_count);
    }
    wl_col_session_t *sess = serial_create(plan, 2, 0, 0),
        *raw = serial_create(plan, 2, 0, 0);
    wl_evaluation_control_t *c = serial_attach(sess, 0);
    int64_t *data = malloc((size_t)(rows ? rows : 1u) * 2u * sizeof(*data));
    CHECK(data);
    for (uint32_t i = 0; i < rows; i++) {
        data[2u*i] = i/3u+1u;
        data[2u*i+1u] = data[2u*i] + (mode == 14 || i%3u == 2u ? 2 : -1);
    }
    CHECK(wl_backend_columnar()->session_insert(&sess->base, "S", data, rows,
        2) == 0);
    CHECK(wl_backend_columnar()->session_insert(&raw->base, "S", data, rows,
        2) == 0);
    if (repeat) {
        serial_capture_t first = {0}, twin = {0};
        CHECK(wl_backend_columnar()->session_snapshot(&sess->base,
            filtered_capture, &first) == 0);
        CHECK(wl_backend_columnar()->session_snapshot(&raw->base,
            filtered_capture, &twin) == 0);
        CHECK(first.count == twin.count && first.digest == twin.digest);
        for (uint32_t i = 0; i < rows; i++) {
            data[2u*i] += rows; data[2u*i+1u] += rows;
        }
        CHECK(wl_backend_columnar()->session_insert(&sess->base, "S", data,
            rows, 2) == 0);
        CHECK(wl_backend_columnar()->session_insert(&raw->base, "S", data, rows,
            2) == 0);
    }
    free(data);
    CHECK(wl_evaluation_control_reset(c) == 0 &&
        wl_evaluation_control_configure(c, limit) == 0);
    filtered_mode = mode;
    filtered_seen = filtered_plans = filtered_compiles = 0;
    filtered_control = c; filtered_fault_token = NULL;
    filtered_fault_record = NULL;
    filtered_session = sess; filtered_batch = NULL;
    filtered_filter_drain = filtered_map_reuse = 0;
    wl_columnar_expr_test_retirement_checkpoint = filtered_retirement;
    wl_columnar_expr_test_controlled_checkpoint = filtered_compile;
    if (mode == 1 || mode == 2) {
        filter_stop_stage = mode == 1 ? 3 : 4;
        filter_stop_progress = mode == 1 ? 257 : 1;
        filter_positive = false; filter_reached = 0;
        wl_columnar_filter_test_controlled_checkpoint = filtered_filter_observe;
    }
    if (mode == 3) {
        filter_stop_stage = 3; filter_stop_progress = 1;
        filter_positive = false;
        filter_reached = 0;
        wl_columnar_ops_map_test_controlled_checkpoint = filtered_map_observe;
    }
    if (mode == 4) {
        consolidate_stop_stage = 31; consolidate_stop_progress = 0;
        consolidate_stop_visits = 0; consolidate_stop_overflow = false;
        wl_columnar_consolidate_test_checkpoint = consolidate_stop;
    }
    if (mode != 1 &&
        mode !=
        2) wl_columnar_filter_test_controlled_checkpoint =
            filtered_filter_observe;
    if (mode !=
        3) wl_columnar_ops_map_test_controlled_checkpoint =
            filtered_map_observe;
    if (mode ==
        5) wl_columnar_eval_serial_test_after_plan = filtered_after_plan;
    serial_capture_t got = {0}, reference = {0};
    int expected = (mode >= 6 && mode <= 8) ||
        mode == 13 ? EINVAL : mode == 10 || mode == 15 ? EOVERFLOW
        : mode == 11 ? WL_EVALUATION_CONTROL_WORK_LIMIT
        : mode && mode != 14 ? WL_EVALUATION_CONTROL_CANCELLED : 0;
    int rc = snapshot ? wl_backend_columnar()->session_snapshot(&sess->base,
            filtered_capture, &got)
        : wl_backend_columnar()->session_step(&sess->base);
    CHECK(rc == expected);
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_report(c, &report) == 0
        && report.result == expected);
    if (mode == 15) CHECK(report.execution_status == EOVERFLOW);
    wl_columnar_filter_test_controlled_checkpoint = NULL;
    wl_columnar_ops_map_test_controlled_checkpoint = NULL;
    wl_columnar_consolidate_test_checkpoint = NULL;
    wl_columnar_eval_serial_test_after_plan = NULL;
    wl_columnar_expr_test_retirement_checkpoint = NULL;
    wl_columnar_expr_test_controlled_checkpoint = NULL;
    if ((mode >= 6 && mode <= 8) || mode == 13 || mode == 15) {
        CHECK(filtered_seen == 1 && filtered_fault_token &&
            sess->cleanup_pending
            && filtered_fault_record->ready);
        if (mode == 8) CHECK(!filtered_fault_record->scratch_payload);
        if (mode == 13 || mode == 15) CHECK(!filtered_fault_record->current
                && atomic_load(&filtered_fault_token->state) ==
                WL_COLUMNAR_MEMORY_RESERVATION_RESERVED);
        filtered_fault_token->identity =
            (wl_columnar_memory_reservation_t *)filtered_fault_identity;
    }
    if (rc) {
        CHECK(!got.count);
        CHECK(wl_evaluation_control_reset(c) == 0 &&
            wl_evaluation_control_configure(c, 0) == 0);
        CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&sess->base,
            filtered_capture, &got)
            : wl_backend_columnar()->session_step(&sess->base)) == 0);
    }
    CHECK((snapshot ? wl_backend_columnar()->session_snapshot(&raw->base,
        filtered_capture, &reference)
        : wl_backend_columnar()->session_step(&raw->base)) == 0);
    CHECK(got.count == reference.count && got.digest == reference.digest);
    for (unsigned i = 0; i < 2; i++) {
        col_rel_t *head = session_find_rel(sess, i ? "H1" : "H0"),
            *twin = session_find_rel(raw, i ? "H1" : "H0");
        CHECK(head && twin); payload_equal(head, twin, 0);
        CHECK(head->nrows ==
            (mode == 14 ? 0u : (rows + 2u)/3u * (repeat ? 2u : 1u)));
    }
    CHECK(!sess->cleanup_pending && !sess->cleanup_reserved_bytes
        && !sess->serial_completion.pending && !sess->pending_input_change);
    if (!mode ||
        mode == 14) CHECK(filtered_compiles >= 1 && filtered_filter_drain == 2
            && filtered_map_reuse == 2);
    serial_destroy(sess); serial_destroy(raw); wl_plan_free(plan);
    wirelog_program_free(program);
    return report.consumed;
}
static void
test_serial_filtered_attempts(void)
{
    filtered_trial(0, false, true, 0, 0); filtered_trial(1, false, false, 0, 0);
    uint64_t cost = filtered_trial(769, false, true, 0, 0);
    CHECK(filtered_trial(769, false, true, 0, cost) == cost);
    filtered_trial(769, false, true, 11, cost-1);
    for (unsigned mode = 1; mode <= 10;
        mode++) filtered_trial(769, false, true, mode, 0);
    filtered_trial(3, false, true, 12, 0);
    filtered_trial(3, false, true, 13, 0);
    filtered_trial(3, false, true, 14, 0);
    filtered_trial(3, false, true, 15, 0);
    filtered_trial(769, true, false, 0, 0);
    filtered_trial(769, true, true, 5, 0);
    puts(
        "serial filtered: genuine generated AUTO FILTER/MAP/CONS initial/repeat raw twins, compiled/growth/retirement stops and original checked cleanup passed");
}

int
main(void)
{
    hmac_required_startup();
    test_publish_generated_twins();
    test_publish_auto_selection();
    test_publish_matching_declared_head();
    test_publish_refusal_recovery();
    test_publish_exact_and_stops();
    test_publish_typed_record();
    test_publish_selection_authority();
    test_publish_preparation_lifetime();
    test_publish_old_head_and_negatives();
    test_owned_consolidate_actual_twins();
    test_owned_consolidate_lifecycle();
    test_owned_consolidate_exact_work();
    test_consolidate_actual_twins();
    test_consolidate_reached_stops();
    test_consolidate_original_noop();
    test_consolidate_prepared_provenance();
    test_consolidate_actual_pools();
    test_consolidate_authority_retention();
    test_consolidate_resource_failures();
    test_consolidate_credit_retention();
    test_consolidate_clone_growth_reset();
    test_consolidate_healthy_reader_cleanup();
    test_consolidate_absent_and_shared();
    test_consolidate_invalid_float();
    test_consolidate_exact_work();
    test_reduce_actual_twins();
    test_reduce_long_reached_stops();
    test_reduce_two_growth_reset();
    test_reduce_extension_context();
    test_reduce_controlled_frame_factory();
    test_reduce_shape_and_resource_failures();
    test_reduce_probe_symbol_expression_stops();
    test_reduce_retirement_authority_faults();
    test_reduce_typed_edges_and_symbols();
    test_reduce_intern_context_and_c2();
    test_reduce_growth_quota_and_alias();
    test_reduce_owned_cleanup();
    test_reduce_growth_current_cell();
    test_reduce_interpreter_allocation_policy();
    test_reduce_forbidden_extension_c2();
    test_reduce_cell_shape_schema_refusals();
    test_shared_writer_authority_refusals();
    test_shared_filter_growth_reset();
    test_shared_owned_reader_and_reuse();
    test_join_writer_authority_and_ledger();
    test_join_writer_growth_reset();
    test_join_writer_owned_and_reuse();
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    test_counted_gates();
    test_fixed_single_admission();
    test_lazy_source_admission();
    test_lazy_initializer_winner();
    test_exact_and_denied(2, 300, false);
    test_exact_and_denied(300, 2, false);
    test_exact_and_denied(1, 103, true);
    test_cancel_and_retained();
    test_same_owner_and_null();
    test_types();
    test_integer_family_and_empty();
    test_real_alias_and_gate();
    test_malformed_plain_compound_metadata();
    test_rejected_shapes();
    test_growth_exact_and_cancel();
    test_growth_wide_and_same_owner();
    test_growth_float_and_timestamp_shortage();
    test_growth_auxiliary_and_ledger();
    test_growth_faults();
    test_metadata_partial_construction();
    test_metadata_long_name_and_compatibility();
    test_metadata_quota_provenance();
    test_metadata_commit_order(false);
    test_metadata_commit_order(true);
    test_metadata_no_governor();
    test_constructor_matrix();
    test_constructor_wide_and_stops();
    test_constructor_identity();
    test_constructor_faults_and_long_name();
    test_new_like_matrix();
    test_new_like_wide_alias_and_names();
    test_new_like_failures_and_defaults();
    test_logical_shape_and_governors();
    test_logical_wide_alias_payload_and_runs();
    test_logical_invalid_runs_and_failures();
    test_pool_real_slot_stops_and_faults();
    test_pool_fallback_nullary_and_shapes();
    test_reserve_owned_atomic();
    test_reserve_no_growth_and_empty();
    test_governor_control_error_provenance();
    test_exclusive_ledger_publication();
    test_private_metadata_final_cutoff();
    test_controlled_row_matrix();
    test_controlled_row_float_and_stops();
    test_controlled_row_growth_faults();
    test_controlled_row_scratch_and_modes();
    test_controlled_row_batch_journal();
    test_controlled_row_authority_precedence();
    test_controlled_join_cartesian();
    test_controlled_join_keyed_first();
    test_controlled_join_shapes();
    test_controlled_join_group_cutoffs();
    test_controlled_join_committed_cleanup();
    test_controlled_join_alias_and_winner();
    test_controlled_join_long_stage_stops();
    test_controlled_join_second_admission();
    test_controlled_join_errors_and_zero();
    test_controlled_join_cleanup_precedence();
    test_controlled_join_entry_refusal();
    test_controlled_join_group_reuse();
    test_controlled_join_empty_right_visits();
    test_controlled_join_numeric_keys();
    test_controlled_filter_comparisons();
    test_controlled_filter_empty_metadata();
    test_controlled_filter_stops_and_lazy();
    test_controlled_filter_refusals_and_faults();
    test_controlled_filter_owned_cleanup();
    test_controlled_filter_rejected_cost_and_registry();
    test_controlled_filter_quota_and_build();
    test_controlled_filter_alias_float();
    test_controlled_filter_cleanup_precedence();
    test_controlled_filter_graph_precedence();
    test_compiled_filter_map_pipeline();
    test_compiled_semantic_tail_precedence();
    test_compiled_long_stops();
    test_compiled_zero_row_failures();
    test_compiled_map_wide_sparse();
    test_compiled_map_wide_stops();
    test_compiled_map_owned_cleanup();
    test_compiled_arithmetic_stack_twins();
    test_numeric_float_and_interpreter_twins();
    test_numeric_widened_failure_tail();
    test_numeric_float_zero_row_failures();
    test_numeric_long_paths();
    test_numeric_owned_interpreter_cleanup();
    test_string_actual_caller_twins();
    test_string_intern_transaction_stops();
    test_string_caller_long_and_retained_stops();
    test_string_domain_quota_and_allocator_faults();
    test_string_owned_cleanup();
    test_string_full_image_domain_overlap();
    test_string_shared_reverse_parent_stop();
    test_string_long_literal_fresh_limits();
    test_string_actual_collision_probe();
    test_string_caller_s_scratch_faults();
    test_string_unmanaged_caller_governed_table();
    test_string_variable_failure_tail_and_empty_rows();
    test_string_zero_row_literal_no_put();
    test_builtin_actual_null_and_value_twins();
    test_builtin_checked_number_native_twins();
    test_builtin_long_reached_stops();
    test_builtin_pending_number_c2();
    test_builtin_long_replace_search_and_copy();
    test_builtin_actual_resource_and_publication();
    test_builtin_owned_cleanup();
    test_builtin_shared_reverse_parent_stop();
    test_crc_actual_twins();
    test_crc_declared_symbol_plan();
    test_crc_lengths_goldens_and_stops();
    test_crc_owned_cleanup();
    test_hash_actual_twins_and_lengths();
    test_hash_symbol_assignment_independence();
    test_hash_real_alignment_and_unaligned_input();
    test_hash_owned_cleanup();
    test_digest_actual_twins();
    test_digest_lifecycle_and_stops();
    test_digest_zero_calls_and_id_independence();
    test_digest_owned_cleanup();
    test_uuid_pair_twins();
    test_uuid_pair_vectors();
    test_uuid_pair_native_and_fallback_vectors();
    test_uuid_pair_mixed_vectors();
    test_uuid_pair_lifecycle();
    test_rfc_twins_and_vectors();
    test_rfc_lifecycle();
    test_rfc_original_table_faults();
    test_rfc_original_table_overlap_and_stops();
    test_rfc_zero_and_short_circuit();
    test_rfc_shared_reverse_parent_stop();
    test_rfc_saturated_operands();
    test_uuid4_actual_bytes_and_work();
    test_uuid4_reached_stops_and_status();
    test_uuid4_owned_cleanup();
    test_hmac_configured_false_raw();
    test_hmac_configured_false_refusal();
    test_hmac_actual_twins();
    test_hmac_id_independence();
    test_hmac_full_bytes();
    test_hmac_lifecycle();
    test_hmac_zero_and_early_refusal();
    test_hmac_owned_cleanup();
    test_extension_actual_twins();
    test_extension_registry_and_callback_stops();
    test_extension_initial_map_admission();
    test_extension_owned_cleanup();
    test_extension_real_snapshot_owner();
    test_extension_direct_release_and_destructor_tls();
    test_extension_tls_depth_and_policy();
    test_extension_same_session_policy_and_owner_guard();
#if defined(WL_MBEDTLS_ENABLED) && \
    defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    puts(
        "HMAC qualification complete: audited software predicate=true; all four domains, long-key, multipart and cleanup assertions passed");
#elif defined(WL_MBEDTLS_ENABLED)
    puts(
        "HMAC configured-false proof complete: raw HMAC succeeded; controlled refusal before effects passed");
#endif
    test_digest_owned_cleanup_kind(2);
    test_digest_zero_calls_and_id_independence_kind(true);
    test_digest_owned_cleanup_kind(true);
    test_serial_initial_attempts();
    test_serial_initial_steps();
    test_serial_rule_continuation();
    test_serial_occurrences_and_cohort();
    test_serial_output_budget();
    test_serial_optional_compaction_denial();
    test_serial_repeat_attempts();
    test_serial_filtered_attempts();
    test_serial_output_ownership();
    test_serial_compact_and_registry_hole();
    puts("evaluation_work: OK");
    return 0;
}
