/* #1821: bounded real scalar materialization, before engine integration. */
#define WL_TEST_RELATION_RESIZE_HOOK 1
#include "../wirelog/columnar/internal.h"
#include <stdio.h>

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

int
main(void)
{
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
    puts("evaluation_work: OK");
    return 0;
}
