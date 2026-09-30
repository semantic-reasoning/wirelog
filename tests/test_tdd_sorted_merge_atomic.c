/* Failure and ownership coverage for the retained TDD sorted merge symbol. */

#include "../wirelog/columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define ROWS 33u

#if defined(__linux__)
void *__real_calloc(size_t, size_t);
void *__real_malloc(size_t);
static uint32_t fault_columns;
static uint32_t fault_ordinal;
static uint32_t fault_seen;

void *
__wrap_calloc(size_t count, size_t size)
{
    if (fault_ordinal && count == fault_columns
        && size == sizeof(int64_t *) && ++fault_seen == fault_ordinal)
        return NULL;
    return __real_calloc(count, size);
}

void *
__wrap_malloc(size_t size)
{
    if (fault_ordinal && (size == ROWS * sizeof(int64_t)
        || size == 2u * ROWS * sizeof(int64_t))
        && ++fault_seen == fault_ordinal)
        return NULL;
    return __real_malloc(size);
}
#endif

static int64_t
value(uint32_t row, uint32_t col)
{
    return (int64_t)(1000u * col + row);
}

static bool inject_empty_probe_interloper;
static int empty_probe_interloper_rc;
static col_rel_t *empty_probe_source_to_reset;

void
wl_columnar_eval_test_after_empty_probe(col_rel_t *dst)
{
    if (empty_probe_source_to_reset) {
        wl_columnar_source_access_writer_t writer = { 0 };
        empty_probe_interloper_rc = col_rel_source_writer_acquire(
            empty_probe_source_to_reset, &writer);
        if (empty_probe_interloper_rc == 0) {
            empty_probe_interloper_rc = col_rel_reset_rows_locked(
                empty_probe_source_to_reset, &writer);
            int release_rc = wl_columnar_source_access_writer_release(
                &writer);
            if (empty_probe_interloper_rc == 0)
                empty_probe_interloper_rc = release_rc;
        }
        empty_probe_source_to_reset = NULL;
        return;
    }
    if (!inject_empty_probe_interloper)
        return;
    inject_empty_probe_interloper = false;
    int64_t row[8];
    for (uint32_t c = 0; c < dst->ncols; c++)
        row[c] = value(2, c);
    empty_probe_interloper_rc = col_rel_append_row(dst, row);
}

static col_rel_t *
make_relation(uint32_t ncols, bool odd)
{
    col_rel_t *rel = col_rel_new_auto(odd ? "src" : "dst", ncols);
    if (!rel)
        return NULL;
    for (uint32_t i = 0; i < ROWS; i++) {
        int64_t row[8];
        for (uint32_t c = 0; c < ncols; c++)
            row[c] = value(2u * i + (odd ? 1u : 0u), c);
        if (col_rel_append_row(rel, row) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    return rel;
}

static int
check_values(const col_rel_t *rel, bool merged)
{
    uint32_t count = merged ? 2u * ROWS : ROWS;
    if (!rel || rel->nrows != count)
        return 0;
    for (uint32_t r = 0; r < count; r++)
        for (uint32_t c = 0; c < rel->ncols; c++)
            if (rel->columns[c][r]
                != value(merged ? r : 2u * r, c))
                return 0;
    return 1;
}

static int
check_fault(uint32_t ncols, unsigned grid, uint32_t ordinal)
{
    col_rel_t *dst = make_relation(ncols, false);
    col_rel_t *src = make_relation(ncols, true);
    if (!dst || !src)
        return 0;
    uint32_t grid_capacity = grid == 1 ? ROWS : grid == 2 ? ROWS / 2u : 0;
    if (grid && col_rel_reserve_merge_grid(dst, grid_capacity) != 0)
        return 0;
    if (grid)
        for (uint32_t c = 0; c < ncols; c++)
            for (uint32_t r = 0; r < grid_capacity; r++)
                dst->merge_columns[c][r] = INT64_C(-777);
    uint32_t old_capacity = dst->capacity;
    uint32_t old_sorted = dst->sorted_nrows;
    uint32_t old_runs = dst->run_count;
    uint64_t old_storage = dst->storage_generation;
    uint64_t old_view = dst->view_generation;
    int64_t **old_outer = dst->columns;
    int64_t **old_grid = dst->merge_columns;
    uint32_t old_grid_capacity = dst->merge_buf_cap;
    int64_t *old_columns[8];
    for (uint32_t c = 0; c < ncols; c++)
        old_columns[c] = dst->columns[c];
#if defined(__linux__)
    fault_columns = ncols;
    fault_seen = 0;
    fault_ordinal = ordinal;
    int rc = tdd_sorted_merge_append(dst, src);
    fault_ordinal = 0;
    int ok = rc == ENOMEM && fault_seen == ordinal;
#else
    (void)ordinal;
    int ok = 1;
#endif
    ok = ok && check_values(dst, false);
    for (uint32_t c = 0; c < ncols; c++)
        for (uint32_t r = 0; r < ROWS; r++)
            ok = ok && src->columns[c][r] == value(2u * r + 1u, c);
    ok = ok && dst->capacity == old_capacity
        && dst->sorted_nrows == old_sorted
        && dst->run_count == old_runs
        && dst->storage_generation == old_storage
        && dst->view_generation == old_view
        && dst->columns == old_outer && dst->merge_columns == old_grid
        && dst->merge_buf_cap == old_grid_capacity;
    for (uint32_t c = 0; c < ncols; c++)
        ok = ok && dst->columns[c] == old_columns[c];
    if (grid)
        for (uint32_t c = 0; c < ncols; c++)
            for (uint32_t r = 0; r < grid_capacity; r++)
                ok = ok && dst->merge_columns[c][r] == INT64_C(-777);
    if (ok)
        ok = tdd_sorted_merge_append(dst, src) == 0
            && check_values(dst, true)
            && dst->sorted_nrows == 2u * ROWS
            && dst->run_count == 1
            && dst->run_ends[0] == 2u * ROWS
            && dst->merge_columns == old_grid
            && dst->merge_buf_cap == old_grid_capacity;
    if (ok && grid == 2)
        for (uint32_t c = 0; c < ncols; c++)
            for (uint32_t r = 0; r < grid_capacity; r++)
                ok = ok && dst->merge_columns[c][r] == INT64_C(-777);
    col_rel_destroy(dst);
    col_rel_destroy(src);
    return ok;
}

static int
check_reader_and_alias(uint32_t ncols)
{
    col_rel_t *dst = make_relation(ncols, false);
    col_rel_t *src = make_relation(ncols, true);
    col_rel_t *alias = col_rel_new_auto("alias", ncols);
    wl_columnar_source_access_reader_t held = { 0 };
    if (!dst || !src || !alias)
        return 0;
    int ok = col_rel_source_reader_acquire(dst, &held) == 0
        && tdd_sorted_merge_append(dst, src) == EBUSY
        && check_values(dst, false)
        && col_rel_source_reader_release(&held) == 0
        && col_rel_install_shared_view(alias, dst) == 0
        && tdd_sorted_merge_append(dst, src) == EBUSY
        && check_values(dst, false)
        && check_values(alias, false);
    col_rel_destroy(alias);
    if (ok)
        ok = tdd_sorted_merge_append(dst, src) == 0
            && check_values(dst, true);
    col_rel_destroy(dst);
    col_rel_destroy(src);
    return ok;
}

static int
check_shared_destination(uint32_t ncols, bool growth)
{
    col_rel_t *owner = make_relation(ncols, false);
    col_rel_t *dst = col_rel_new_auto("view", ncols);
    col_rel_t *src = make_relation(ncols, true);
    if (!owner || !dst || !src)
        return 0;
    if (!growth && col_rel_reserve_capacity_admitted(owner, 2u * ROWS,
        NULL) != 0)
        return 0;
    if (col_rel_install_shared_view(dst, owner) != 0)
        return 0;
    uint64_t old_view = dst->view_generation;
    uint64_t old_storage = dst->storage_generation;
    int ok = tdd_sorted_merge_append(dst, src) == EBUSY
        && dst->view_generation == old_view
        && dst->storage_generation == old_storage
        && dst->col_shared != NULL
        && check_values(dst, false)
        && check_values(owner, false)
        && dst->storage_owner == owner;
    col_rel_destroy(dst);
    col_rel_destroy(owner);
    col_rel_destroy(src);
    return ok;
}

static int
check_governed(uint32_t ncols, bool grid)
{
    col_rel_t *dst = make_relation(ncols, false);
    col_rel_t *src = make_relation(ncols, true);
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = 1024u * 1024u;
    resolution.usable_bytes = resolution.budget_bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    wl_columnar_memory_governor_ref_t *ref
        = wl_columnar_memory_governor_ref_create(&resolution);
    if (!dst || !src || !ref)
        return 0;
    if (grid && col_rel_reserve_merge_grid(dst, ROWS) != 0)
        return 0;
    if (col_rel_attach_memory_governor(dst, ref) != 0)
        return 0;
    wl_columnar_memory_governor_t *governor
        = wl_columnar_memory_governor_ref_get(ref);
    uint64_t before = wl_columnar_memory_reserved(governor);
    int ok = before > 0;
#if defined(__linux__)
    fault_columns = ncols;
    fault_seen = 0;
    fault_ordinal = grid ? ncols + 1u : 1u;
    int rc = tdd_sorted_merge_append(dst, src);
    fault_ordinal = 0;
    ok = ok && rc == ENOMEM && fault_seen == (grid ? ncols + 1u : 1u)
        && check_values(dst, false)
        && wl_columnar_memory_reserved(governor) == before;
#endif
    if (ok)
        ok = tdd_sorted_merge_append(dst, src) == 0
            && check_values(dst, true)
            && wl_columnar_memory_reserved(governor) > before;
    col_rel_destroy(dst);
    col_rel_destroy(src);
    ok = ok && wl_columnar_memory_reserved(governor) == 0;
    wl_columnar_memory_governor_ref_release(ref);
    return ok;
}

static int
check_invalid(void)
{
    col_rel_t *dst = make_relation(1, false);
    col_rel_t *src = make_relation(1, true);
    col_rel_t *wide = make_relation(3, true);
    if (!dst || !src || !wide)
        return 0;
    int ok = tdd_sorted_merge_append(dst, dst) == EINVAL
        && tdd_sorted_merge_append(dst, wide) == EINVAL
        && check_values(dst, false);
    uint64_t saved_view = dst->view_generation;
    dst->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    ok = ok && tdd_sorted_merge_append(dst, src) == EOVERFLOW
        && dst->view_generation == WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        && check_values(dst, false);
    dst->view_generation = saved_view;
    if (col_rel_enable_timestamps(src) != 0)
        ok = 0;
    else
        ok = ok && tdd_sorted_merge_append(dst, src) == EINVAL
            && check_values(dst, false);
    wirelog_column_type_t float_type = WIRELOG_TYPE_FLOAT;
    col_rel_t *float_dst = col_rel_new_auto("float-dst", 1);
    col_rel_t *float_src = col_rel_new_auto("float-src", 1);
    int float_dst_rc = float_dst
        ? col_rel_set_column_types(float_dst, &float_type, 1) : -1;
    int float_src_rc = float_src
        ? col_rel_set_column_types(float_src, &float_type, 1) : -1;
    for (uint32_t i = 0; i < ROWS && float_dst_rc == 0
        && float_src_rc == 0; i++) {
        int64_t even = wl_columnar_float_to_bits((double)(2u * i));
        int64_t odd = wl_columnar_float_to_bits((double)(2u * i + 1u));
        if (col_rel_append_row(float_dst, &even) != 0
            || col_rel_append_row(float_src, &odd) != 0)
            float_dst_rc = EINVAL;
    }
    int float_merge_rc = float_dst && float_src
        ? tdd_sorted_merge_append(float_dst, float_src) : -1;
    ok = ok && float_dst && float_src
        && float_dst_rc == 0 && float_src_rc == 0
        && float_merge_rc == EINVAL
        && float_dst->nrows == ROWS;
    col_rel_destroy(float_dst);
    col_rel_destroy(float_src);
    col_rel_destroy(dst);
    col_rel_destroy(src);
    col_rel_destroy(wide);
    return ok;
}

static int
check_preallocated(uint32_t ncols)
{
    col_rel_t *dst = make_relation(ncols, false);
    col_rel_t *src = make_relation(ncols, true);
    if (!dst || !src
        || col_rel_reserve_capacity_admitted(dst, 2u * ROWS, NULL) != 0
        || col_rel_reserve_merge_grid(dst, ROWS) != 0)
        return 0;
    uint64_t old_storage = dst->storage_generation;
    int ok = tdd_sorted_merge_append(dst, src) == 0
        && dst->capacity >= 2u * ROWS
        && dst->storage_generation == old_storage
        && check_values(dst, true)
        && dst->sorted_nrows == 2u * ROWS
        && dst->run_count == 1
        && dst->run_ends[0] == 2u * ROWS;
    col_rel_destroy(dst);
    col_rel_destroy(src);
    return ok;
}

static int
check_empty_fast_path(void)
{
    col_rel_t *dst = col_rel_new_auto("empty-dst", 1);
    col_rel_t *src = make_relation(1, true);
    col_rel_t *empty = col_rel_new_auto("empty-self", 1);
    col_rel_t *reference = col_rel_new_auto("empty-reference", 1);
    wl_columnar_source_access_reader_t held = { 0 };
    if (!dst || !src || !empty || !reference)
        return 0;
    uint64_t old_view = dst->view_generation;
    uint64_t old_storage = dst->storage_generation;
    uint32_t old_capacity = dst->capacity;
    int ok = tdd_sorted_merge_append(empty, empty) == 0
        && empty->nrows == 0
        && col_rel_source_reader_acquire(dst, &held) == 0
        && tdd_sorted_merge_append(dst, src) == EBUSY
        && dst->nrows == 0 && dst->capacity == old_capacity
        && dst->view_generation == old_view
        && dst->storage_generation == old_storage
        && col_rel_source_reader_release(&held) == 0
        && col_rel_append_all(reference, src, NULL) == 0
        && tdd_sorted_merge_append(dst, src) == 0
        && dst->nrows == ROWS
        && dst->sorted_nrows == reference->sorted_nrows
        && dst->run_count == reference->run_count
        && dst->run_ends[0] == reference->run_ends[0];
    for (uint32_t r = 0; r < ROWS; r++)
        ok = ok && dst->columns[0][r] == value(2u * r + 1u, 0);
    col_rel_destroy(dst);
    col_rel_destroy(src);
    col_rel_destroy(empty);
    col_rel_destroy(reference);
    return ok;
}

static int
check_empty_probe_race(uint32_t ncols)
{
    col_rel_t *dst = col_rel_new_auto("empty-race-dst", ncols);
    col_rel_t *src = make_relation(ncols, true);
    if (!dst || !src)
        return 0;
    empty_probe_interloper_rc = EINVAL;
    inject_empty_probe_interloper = true;
    int rc = tdd_sorted_merge_append(dst, src);
    inject_empty_probe_interloper = false;
    int ok = rc == EBUSY && empty_probe_interloper_rc == 0
        && dst->nrows == 1 && dst->sorted_nrows == 0;
    for (uint32_t c = 0; c < ncols; c++)
        ok = ok && dst->columns[c][0] == value(2, c);
    if (ok)
        ok = tdd_sorted_merge_append(dst, src) == 0
            && dst->nrows == ROWS + 1u
            && dst->sorted_nrows == ROWS + 1u
            && dst->run_count == 1
            && dst->run_ends[0] == ROWS + 1u;
    for (uint32_t r = 0; ok && r < ROWS + 1u; r++)
        for (uint32_t c = 0; c < ncols; c++)
            ok = ok && dst->columns[c][r]
                == value(r == 0 ? 1u : r == 1 ? 2u : 2u * r - 1u, c);
    col_rel_destroy(dst);
    col_rel_destroy(src);
    return ok;
}

static int
check_source_emptied_after_probe(void)
{
    col_rel_t *owner = col_rel_new_auto("empty-owner", 1);
    col_rel_t *dst = col_rel_new_auto("empty-shared-dst", 1);
    col_rel_t *src = make_relation(1, true);
    if (!owner || !dst || !src
        || col_rel_install_shared_view(dst, owner) != 0)
        return 0;
    dst->memory_budget_denial_pending = true;
    empty_probe_interloper_rc = EINVAL;
    empty_probe_source_to_reset = src;
    int rc = tdd_sorted_merge_append(dst, src);
    empty_probe_source_to_reset = NULL;
    int ok = rc == 0 && empty_probe_interloper_rc == 0
        && src->nrows == 0 && dst->nrows == 0
        && dst->col_shared == NULL && dst->storage_owner == dst
        && !dst->memory_budget_denial_pending
        && owner->nrows == 0;
    col_rel_destroy(dst);
    col_rel_destroy(owner);
    col_rel_destroy(src);
    return ok;
}

int
main(void)
{
    for (uint32_t ncols = 1; ncols <= 8; ncols = ncols == 1 ? 3 : 8) {
#if defined(__linux__)
        for (unsigned grid = 0; grid < 3; grid++)
            for (uint32_t ordinal = 1;
                ordinal <= (grid == 1 ? 1u : 2u) * (ncols + 1u);
                ordinal++) {
                if (!check_fault(ncols, grid, ordinal)) {
                    fprintf(stderr, "fault ncols=%u grid=%u ordinal=%u\n",
                        ncols, grid, ordinal);
                    return 1;
                }
            }
#endif
        if (!check_reader_and_alias(ncols)
            || !check_shared_destination(ncols, true)
            || !check_shared_destination(ncols, false)
            || !check_governed(ncols, false)
            || !check_governed(ncols, true)
            || !check_preallocated(ncols)
            || !check_empty_probe_race(ncols)) {
            fprintf(stderr, "ownership ncols=%u\n", ncols);
            return 1;
        }
        if (ncols == 8)
            break;
    }
    if (!check_invalid() || !check_empty_fast_path()
        || !check_source_emptied_after_probe()) {
        fputs("invalid-input cases failed\n", stderr);
        return 1;
    }
    puts("TDD sorted merge atomic cases passed");
    return 0;
}
