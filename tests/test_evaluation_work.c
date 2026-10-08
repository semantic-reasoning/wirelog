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
    uint64_t cost = width + (uint64_t)width * rows
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
    cancel_at = 258; /* Metadata 2 + first real copy chunk 256. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 258);
    CHECK(max_chunk == 256 && dst->nrows == 1);
    CHECK(dst->view_generation == generation);
    CHECK(atomic_load(&governor->reserved_bytes) == retained);
    released(src); released(dst);
    retry(c, 0);
    /* Last timestamp chunk also has a tail poll before publication. */
    cancel_at = 2 + 600 + 1500;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cancel_at);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 2102);
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
    wl_evaluation_control_t *c = control(19);
    CHECK(wl_columnar_relation_append_all_controlled(r, r, NULL, c) == 0);
    CHECK(finish(c, 0) == 19 && r->nrows == 6);
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
    wl_evaluation_control_t *c = control(7); /* type+validation+copy+normalize */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 7);
    CHECK(dst->columns[0][1] == 0 && dst->columns[0][2] == src->columns[0][1]);
    dst->nrows = 1;
    src->columns[0][0] = wl_columnar_float_to_bits(NAN);
    uint64_t generation = dst->view_generation;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 2);
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
    CHECK(finish(c, EINVAL) == 1);
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
    wl_evaluation_control_t *c = control(2);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 2);
    payload_equal(dst, src, 1);
    src->nrows = 0;
    uint64_t generation = dst->view_generation;
    retry(c, 1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 1 && dst->view_generation == generation);
    retry(c, 0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    release(c); destroy(dst); destroy(src);
    src = relation(0, 3, 2, false, NULL);
    dst = relation(0, 3, 1, false, NULL);
    c = control(1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 0 && dst->nrows == 3);
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
    CHECK(finish(c, ENOTSUP) == 0 && dst->nrows == 1);
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
    CHECK(finish(c, EBUSY) == 0);
    /* Failed destination admission must retire the already acquired source. */
    released(src);
    CHECK(wl_columnar_source_access_reader_release(&token) == 0);
    released(dst);
    src->nrows = src->capacity + 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 0);
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
    CHECK(finish(c, EOVERFLOW) == 0);
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
    uint64_t cost = 4207; /* 2 metadata, 1 doubling, 4 allocations, 1200 cells, 3000 fields. */
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) <= cost - 1);
    unchanged(dst, before); released(dst); released(src);
    const uint64_t boundaries[] = { 261, 2707, 4207 };
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
    uint64_t cost = 300 + 2 + 1 + 300 + 300 + 600;
    wl_evaluation_control_t *c = control(cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
    dst = relation(1, 3, 3, true, NULL);
    c = control(41);
    CHECK(wl_columnar_relation_append_all_controlled(dst, dst, NULL, c) == 0);
    CHECK(finish(c, 0) == 41 && dst->nrows == 6);
    for (uint32_t row = 0; row < 3; row++) {
        CHECK(dst->columns[0][row] == dst->columns[0][row + 3]);
        CHECK(dst->timestamps[row].multiplicity ==
            dst->timestamps[row + 3].multiplicity);
    }
    released(dst); release(c); destroy(dst);
    src = relation(0, 3, 2, false, NULL);
    dst = relation(0, 1, 1, false, NULL);
    c = control(2);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 2 && dst->nrows == 3 && dst->capacity == 4);
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
    wl_evaluation_control_t *c = control(28); /* meta1+validation2+doubling2+alloc3+old1+new2+norm2+timestamp15 */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 28);
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
    CHECK(finish(c, 0) == 22); /* metadata1+alloc3+old1+new2+timestamp15 */
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
    CHECK(finish(c, ENOMEM) == 29);
    unchanged(dst, before);
    CHECK(dst->merge_columns == merge && dst->row_scratch == scratch);
    CHECK(atomic_load(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION]) == 16);
    CHECK(atomic_load(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP]) ==
        sizeof(col_delta_timestamp_t));
    retry(c, 29);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 29);
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
    CHECK(finish(c, ENOMEM) == 3); /* metadata + doubling2, before allocation */
    unchanged(dst, before); released(dst); released(src);
    atomic_store(&g->usable_bytes, usable);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_prepare_resize();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 4);
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 8); /* Actual copied source and old prefix admitted. */
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    dst->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 0);
    dst->storage_generation = before.storage;
    unchanged(dst, before);
    retry(c, 8);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 8);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
}

static int metadata_cancel_phase;
static uint64_t metadata_cancel_progress, allocation_after_child;
static bool committed_seen, fail_payload_after_metadata;

static void
metadata_checkpoint(wl_evaluation_control_t *c, int phase, uint64_t progress)
{
    if (phase == WL_COLUMNAR_RELATION_METADATA_COMMITTED) {
        CHECK(progress != 0);
        committed_seen = true;
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
    uint64_t cost = 8 * (uint64_t)dst->ncols + 2 * lengths + 6
        + 2 * (uint64_t)src->ncols * src->nrows
        + (uint64_t)dst->ncols * dst->nrows;
    for (uint32_t c = 0; c < src->ncols; c++)
        if (src->column_types[c] == WIRELOG_TYPE_FLOAT)
            cost += src->nrows;
    if (dst->timestamps && src->timestamps)
        cost += 5 * src->nrows;
    return cost;
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
    CHECK(finish(c, EINVAL) == 1);
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    dst->nrows = 0;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 1 && dst->column_types == NULL);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        NULL) == EINVAL);
    src->nrows = 0;
    uint64_t generation = dst->view_generation;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 1 && dst->column_types == NULL);
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
    CHECK(finish(c, ENOSPC) == 4); /* Compatibility + two source visits + old visit. */
    metadata_unchanged(dst, before, old_schema, metadata_credit);
    atomic_store(&g->usable_bytes, before.reserved + sizeof(uint64_t));
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOSPC);
    CHECK(dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOSPC) == 11); /* Scratch admitted, actual name length gathered. */
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
    CHECK(finish(c, ENOMEM) == 5);
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
    uint64_t cost = installation_cost(dst, src) + (growth ? 5 : 0);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_test_metadata_checkpoint = metadata_checkpoint;
    committed_seen = false;
    wl_columnar_relation_test_fail_metadata_commit = true;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!committed_seen && !wl_columnar_relation_test_fail_metadata_commit);
    CHECK(finish(c, ENOMEM) == cost);
    metadata_unchanged(dst, before, old_schema, metadata_credit); released(src);
    if (growth) {
        retry(c, 0);
        committed_seen = false; fail_payload_after_metadata = true;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == ENOMEM);
        CHECK(committed_seen);
        CHECK(finish(c, ENOMEM) == cost);
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
    return width ? 2 * length + 11 + 12 * (uint64_t)width + 3 * generated
           + (timestamps && capacity ? 1 : 0)
        : 2 * length + 6 + (timestamps && capacity ? 1 : 0);
}

static void
constructor_equal(col_rel_t *a, col_rel_t *b)
{
    CHECK(a->ncols == b->ncols && a->capacity == b->capacity &&
        a->nrows == b->nrows);
    CHECK(a->column_types == NULL && b->column_types == NULL);
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
    CHECK(cost == 19494);
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
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 15);
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
    CHECK(r == NULL && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 15);
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
    CHECK(r == NULL && finish(c, EOVERFLOW) == 14);
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

int
main(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
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
    puts("evaluation_work: OK");
    return 0;
}
