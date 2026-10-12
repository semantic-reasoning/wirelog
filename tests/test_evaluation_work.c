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
/* Fixture snapshots and budget writes are serialized; relaxed observations
 * do not publish or acquire production ownership. */
static col_rel_t *growth_watch;
static uint64_t growth_peak;
static void
checkpoint(wl_evaluation_control_t *control, uint64_t work)
{
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    if (work > max_chunk)
        max_chunk = work;
    seen += work;
    if (growth_watch && growth_watch->memory_governor) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(growth_watch->memory_governor);
        uint64_t held = atomic_load_explicit(&g->reserved_bytes,
                memory_order_relaxed);
        if (held > growth_peak)
            growth_peak = held;
    }
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
    uint64_t cost = 2 + width + (uint64_t)width * rows
        + (timestamps ? (uint64_t)rows * 5 : 0);
    col_rel_t *src = relation(width, rows, rows, timestamps, NULL);
    col_rel_t *dst = relation(width, rows + 1, 1, timestamps, NULL);
    wl_evaluation_control_t *c = control(cost - 1);
    uint64_t generation = dst->view_generation;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    uint64_t last_chunk = timestamps
        ? (uint64_t)((rows - 1) % (WL_EVALUATION_CONTROL_QUANTUM / 5) + 1) * 5
        : (rows - 1) % WL_EVALUATION_CONTROL_QUANTUM + 1;
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == cost - last_chunk);
    CHECK(seen == cost - last_chunk);
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
    uint64_t retained = atomic_load_explicit(&governor->reserved_bytes,
            memory_order_relaxed);
    uint64_t generation = dst->view_generation;
    wl_evaluation_control_t *c = control(0);
    cancel_at = 260; /* Gates 2 + metadata 2 + first copy chunk 256. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 260);
    CHECK(max_chunk == 256 && dst->nrows == 1);
    CHECK(dst->view_generation == generation);
    CHECK(dst->columns[0][0] == 0 && dst->columns[1][0] == 10000);
    CHECK(dst->timestamps[0].iteration == 0
        && dst->timestamps[0].stratum == 1
        && dst->timestamps[0].worker == 2
        && dst->timestamps[0].multiplicity == 0);
    CHECK(atomic_load_explicit(&governor->reserved_bytes,
        memory_order_relaxed) == retained);
    released(src); released(dst);
    retry(c, 0);
    /* Last timestamp chunk also has a tail poll before publication. */
    cancel_at = 2 + 2 + 600 + 1500;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cancel_at);
    CHECK(dst->nrows == 1 && dst->view_generation == generation);
    CHECK(dst->columns[0][0] == 0 && dst->columns[1][0] == 10000);
    CHECK(dst->timestamps[0].iteration == 0
        && dst->timestamps[0].stratum == 1
        && dst->timestamps[0].worker == 2
        && dst->timestamps[0].multiplicity == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 2104);
    payload_equal(dst, src, 1);
    CHECK(atomic_load_explicit(&governor->reserved_bytes,
        memory_order_relaxed) == retained);
    release(c); destroy(dst); destroy(src);
    CHECK(atomic_load_explicit(&governor->reserved_bytes,
        memory_order_relaxed) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_same_owner_and_null(void)
{
    col_rel_t *r = relation(1, 8, 3, true, NULL);
    wl_evaluation_control_t *c = control(20);
    CHECK(wl_columnar_relation_append_all_controlled(r, r, NULL, c) == 0);
    CHECK(finish(c, 0) == 20 && r->nrows == 6);
    for (uint32_t row = 0; row < 3; row++) {
        CHECK(r->columns[0][row] == row);
        CHECK(r->columns[0][row + 3] == r->columns[0][row]);
        CHECK(r->timestamps[row].iteration == row);
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
    wl_evaluation_control_t *c = control(9); /* gates+type+validation+copy+normalize */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 9);
    CHECK(dst->columns[0][1] == 0 && dst->columns[0][2] == src->columns[0][1]);
    dst->nrows = 1;
    uint64_t generation = dst->view_generation;
    const double invalid[] = { NAN, INFINITY, -INFINITY };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        src->columns[0][0] = wl_columnar_float_to_bits(invalid[i]);
        retry(c, 0);
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == EINVAL);
        CHECK(finish(c, EINVAL) == 4);
        CHECK(dst->nrows == 1 && dst->view_generation == generation);
        released(src); released(dst);
    }
    src->columns[0][0] = 0;
    src->nrows = 0;
    wirelog_column_type_t integer = WIRELOG_TYPE_INT64;
    CHECK(col_rel_set_column_types(src, &integer, 1) == 0);
    src->nrows = 2;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 3);
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
    wl_evaluation_control_t *c = control(4);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 4);
    payload_equal(dst, src, 1);
    src->nrows = 0;
    uint64_t generation = dst->view_generation;
    retry(c, 3);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 3 && dst->view_generation == generation);
    retry(c, 0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    release(c); destroy(dst); destroy(src);
    src = relation(0, 3, 2, false, NULL);
    dst = relation(0, 3, 1, false, NULL);
    c = control(2);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 2 && dst->nrows == 3);
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
    CHECK(finish(c, ENOTSUP) == 2 && dst->nrows == 1);
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
    CHECK(finish(c, EBUSY) == 2);
    /* Failed destination admission must retire the already acquired source. */
    released(src);
    CHECK(wl_columnar_source_access_reader_release(&token) == 0);
    released(dst);
    src->nrows = src->capacity + 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 2);
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
    CHECK(finish(c, EOVERFLOW) == 2);
    dst->nrows = 1;
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    src->nrows = 0;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    src->nrows = 1;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    released(src); released(dst);
    release(c); destroy(dst); destroy(src);
}

static void
test_raw_parity(bool typed, bool source_ts, bool destination_ts)
{
    col_rel_t *src = relation(1, 3, 3, source_ts, NULL);
    col_rel_t *dst = relation(1, 8, 1, destination_ts, NULL);
    col_rel_t *raw = relation(1, 8, 1, destination_ts, NULL);
    if (typed) {
        wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
        col_rel_t *rels[] = { src, dst, raw };
        for (unsigned i = 0; i < 3; i++) {
            uint32_t rows = rels[i]->nrows;
            rels[i]->nrows = 0;
            CHECK(col_rel_set_column_types(rels[i], &type, 1) == 0);
            rels[i]->nrows = rows;
        }
        src->columns[0][0] = INT64_MIN;
        src->columns[0][1] = wl_columnar_float_to_bits(2.5);
        src->columns[0][2] = wl_columnar_float_to_bits(-4.0);
        dst->columns[0][0] = raw->columns[0][0] =
            wl_columnar_float_to_bits(1.5);
    }
    /* Poison spare timestamp cells: one-sided timestamp tracking preserves
     * their prior contents, just as the raw append does. */
    if (destination_ts) {
        for (uint32_t row = 1; row < 8; row++) {
            col_delta_timestamp_t value = { 7, 3, 2, 0, -9 };
            dst->timestamps[row] = raw->timestamps[row] = value;
        }
    }
    uint64_t generation = dst->view_generation;
    uint64_t raw_generation = raw->view_generation;
    uint64_t retained = dst->retained_reservation.bytes;
    uint64_t metadata = dst->metadata_reservation.bytes;
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    uint64_t reserved = atomic_load_explicit(&governor->reserved_bytes,
            memory_order_relaxed);
    uint64_t cost = 2 + 1 + 3 + (typed ? 6 : 0)
        + (source_ts && destination_ts ? 15 : 0);
    wl_evaluation_control_t *c = control(cost);
    CHECK(col_rel_append_all(raw, src, NULL) == 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost && seen == cost);
    CHECK(dst->nrows == raw->nrows && dst->nrows == 4);
    CHECK(dst->view_generation == generation + 1);
    CHECK(raw->view_generation == raw_generation + 1);
    CHECK(dst->capacity == raw->capacity && dst->capacity == 8);
    CHECK(dst->retained_reservation.bytes == retained);
    CHECK(dst->metadata_reservation.bytes == metadata);
    CHECK(atomic_load_explicit(&governor->reserved_bytes,
        memory_order_relaxed) == reserved);
    for (uint32_t row = 0; row < 4; row++) {
        CHECK(dst->columns[0][row] == raw->columns[0][row]);
        if (destination_ts) {
            const col_delta_timestamp_t *a = &dst->timestamps[row];
            const col_delta_timestamp_t *b = &raw->timestamps[row];
            CHECK(a->iteration == b->iteration && a->stratum == b->stratum);
            CHECK(a->worker == b->worker && a->_reserved == b->_reserved);
            CHECK(a->multiplicity == b->multiplicity);
        }
    }
    released(src); released(dst); released(raw);
    release(c); destroy(raw); destroy(dst); destroy(src);
}

static wl_columnar_source_access_reader_t peer;
static wl_columnar_source_access_reader_t *active_reader;
static unsigned cas_calls, cas_failed, cas_succeeded;

static uint64_t
gate_state(wl_columnar_source_access_gate_t *gate)
{
    return atomic_load_explicit(&gate->state, memory_order_acquire);
}

static void
before_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, uint64_t observed)
{
    (void)c;
    if (cas_calls++ == 0) {
        CHECK(observed == 0);
        CHECK(wl_columnar_source_access_reader_acquire(gate, &peer) == 0);
    }
}

static void
after_reader(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate, bool acquired)
{
    (void)c;
    if (acquired) {
        cas_succeeded++;
        CHECK(active_reader->owner == gate);
        CHECK(active_reader->identity == (uintptr_t)active_reader);
        CHECK(active_reader->thread_valid && !active_reader->transferable);
        CHECK(gate_state(gate) == 2);
    } else {
        cas_failed++;
        CHECK(!active_reader->owner && !active_reader->identity);
    }
}

static void
test_gate_retry(unsigned mode)
{
    wl_columnar_source_access_gate_t gate;
    wl_columnar_source_access_reader_t token = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    wl_evaluation_control_t *c = control(mode == 1 ? 1 : 2);
    active_reader = &token;
    cas_calls = cas_failed = cas_succeeded = 0;
    wl_columnar_relation_test_before_reader_cas = before_reader;
    wl_columnar_relation_test_after_reader_cas = after_reader;
    if (mode >= 2)
        cancel_at = mode == 2 ? 1 : 2;
    int expected = mode == 0 ? 0 : mode == 1
        ? WL_EVALUATION_CONTROL_WORK_LIMIT : WL_EVALUATION_CONTROL_CANCELLED;
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &token, c)
        == expected);
    wl_columnar_relation_test_before_reader_cas = NULL;
    wl_columnar_relation_test_after_reader_cas = NULL;
    bool second_round = mode == 0 || mode == 3;
    CHECK(finish(c, expected) == (second_round ? 2 : 1));
    CHECK(seen == (second_round ? 2 : 1) && max_chunk == 1);
    CHECK(cas_failed == 1 && cas_succeeded == (second_round ? 1 : 0));
    CHECK(cas_calls == (second_round ? 2 : 1));
    if (mode == 0)
        CHECK(wl_columnar_source_access_reader_release(&token) == 0);
    CHECK(!token.owner && !token.secondary_owner && !token.identity
        && !token.thread_valid && !token.transferable);
    CHECK(peer.owner == &gate && gate_state(&gate) == 1);
    CHECK(wl_columnar_source_access_reader_release(&peer) == 0);
    CHECK(gate_state(&gate) == 0);
    retry(c, 1);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &token, c)
        == 0);
    CHECK(finish(c, 0) == 1 && gate_state(&gate) == 1);
    CHECK(wl_columnar_source_access_reader_release(&token) == 0);
    active_reader = NULL;
    release(c);
}

static void
test_gate_stops_and_raw(void)
{
    wl_columnar_source_access_gate_t gate;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    wl_evaluation_control_t *c = control(1);
    for (unsigned kind = 0; kind < 2; kind++) {
        if (kind)
            retry(c, 1);
        cancel_at = 1;
        int rc = kind
            ? wl_columnar_relation_writer_acquire_controlled(&gate, &writer, c)
            : wl_columnar_relation_reader_acquire_controlled(&gate, &reader, c);
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(finish(c, rc) == 1 && seen == 1 && gate_state(&gate) == 0);
        CHECK(!reader.owner && !reader.identity && !reader.secondary_owner
            && !reader.thread_valid && !reader.transferable);
        CHECK(!writer.owner && !writer.identity && !writer.secondary_owner
            && !writer.thread_valid);
    }
    retry(c, 1);
    CHECK(wl_evaluation_control_charge(c, 1) == 0);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 1);
    CHECK(seen == 0 && gate_state(&gate) == 0 && !reader.owner);
    retry(c, 0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &writer, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    CHECK(gate_state(&gate) == 0 && !writer.owner);
    retry(c, 0);
    CHECK(wl_columnar_relation_reader_acquire_controlled(NULL, &reader, c)
        == EINVAL);
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, NULL, c)
        == EINVAL);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader, NULL)
        == 0);
    wl_columnar_source_access_reader_t copied_reader = reader;
    CHECK(wl_columnar_source_access_reader_release(&copied_reader) == EINVAL);
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &copied_reader,
        c) == EINVAL);
    CHECK(gate_state(&gate) == 1 && reader.thread_valid &&
        !reader.transferable);
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
    CHECK(wl_columnar_source_access_reader_acquire_transferable(&gate, &reader)
        == 0);
    CHECK(reader.transferable && !reader.thread_valid
        && reader.identity == (uintptr_t)&reader && !reader.secondary_owner);
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &writer, NULL)
        == 0);
    wl_columnar_source_access_writer_t copied_writer = writer;
    CHECK(wl_columnar_source_access_writer_release(&copied_writer) == EINVAL);
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &copied_writer,
        c) == EINVAL);
    CHECK(writer.thread_valid && writer.identity == (uintptr_t)&writer);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    CHECK(finish(c, EINVAL) == 0 && seen == 0 && gate_state(&gate) == 0);
    release(c);
}

static void
before_writer(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate)
{
    (void)c;
    CHECK(wl_columnar_source_access_reader_acquire(gate, &peer) == 0);
}

static void
test_gate_busy_and_overflow(void)
{
    wl_columnar_source_access_gate_t gate;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 }, held = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    wl_evaluation_control_t *c = control(1);
    wl_columnar_relation_test_before_writer_cas = before_writer;
    cancel_at = 1; /* The established busy verdict precedes this tail stop. */
    CHECK(wl_columnar_relation_writer_acquire_controlled(&gate, &writer, c)
        == EBUSY);
    wl_columnar_relation_test_before_writer_cas = NULL;
    CHECK(finish(c, EBUSY) == 1 && gate_state(&gate) == 1 && !writer.owner);
    CHECK(wl_columnar_source_access_reader_release(&peer) == 0);
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &held) == 0);
    retry(c, 1);
    cancel_at = 1;
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader, c)
        == EBUSY);
    CHECK(finish(c, EBUSY) == 1 && !reader.owner);
    CHECK(gate_state(&gate) == WL_COLUMNAR_SOURCE_ACCESS_WRITER);
    CHECK(wl_columnar_source_access_writer_release(&held) == 0);
    /* Synthetic numeric boundary, not a claim to create this many readers. */
    atomic_store_explicit(&gate.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u,
        memory_order_relaxed);
    retry(c, 1);
    cancel_at = 1;
    CHECK(wl_columnar_relation_reader_acquire_controlled(&gate, &reader, c)
        == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 1 && !reader.owner);
    CHECK(gate_state(&gate) == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u);
    wl_columnar_source_access_gate_init(&gate);
    release(c);
}

static void
test_append_gate_stops(void)
{
    col_rel_t *src = relation(1, 3, 2, false, NULL);
    col_rel_t *dst = relation(1, 3, 1, false, NULL);
    uint64_t view = dst->view_generation;
    uint64_t retained = dst->retained_reservation.bytes;
    wl_evaluation_control_t *c = control(0);
    for (unsigned stop = 1; stop <= 2; stop++) {
        if (stop > 1)
            retry(c, 0);
        cancel_at = stop;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == stop);
        CHECK(dst->nrows == 1 && dst->view_generation == view);
        CHECK(dst->columns[0][0] == 0 && src->nrows == 2);
        CHECK(dst->retained_reservation.bytes == retained);
        released(src); released(dst);
    }
    retry(c, 1); /* Source reader held, destination writer charge refused. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 1);
    CHECK(dst->nrows == 1 && dst->view_generation == view);
    CHECK(dst->retained_reservation.bytes == retained);
    released(src); released(dst);
    retry(c, 5); /* Two gates, one metadata lane, two copied cells. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 5 && dst->nrows == 3);
    release(c); destroy(dst); destroy(src);
}

typedef struct {
    int64_t **columns;
    col_delta_timestamp_t *timestamps;
    uint32_t capacity, timestamp_capacity, rows;
    uint64_t view, storage, reserved, credit;
    uint64_t token_bytes, token_owner, token_state, replacement;
    const void *token_identity, *metadata_identity;
    uint64_t metadata_bytes, metadata_owner, metadata_state;
    int64_t **merge, **backup;
    int64_t *scratch;
    col_delta_timestamp_t *backup_ts;
} growth_snapshot_t;

static growth_snapshot_t
snapshot(col_rel_t *r)
{
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(r->memory_governor);
    return (growth_snapshot_t){
               .columns = r->columns, .timestamps = r->timestamps,
               .capacity = r->capacity,
               .timestamp_capacity = r->timestamp_capacity,
               .rows = r->nrows, .view = r->view_generation,
               .storage = r->storage_generation,
               .reserved = g ? atomic_load_explicit(&g->reserved_bytes,
                       memory_order_relaxed) : 0,
               .credit = r->retained_reserved_bytes,
               .token_bytes = r->retained_reservation.bytes,
               .token_owner =
                   atomic_load_explicit(&r->retained_reservation.owner_bits,
                       memory_order_relaxed),
               .token_state =
                   atomic_load_explicit(&r->retained_reservation.state,
                       memory_order_relaxed),
               .replacement = r->retained_reservation.replacement_bytes,
               .token_identity = r->retained_reservation.identity,
               .metadata_identity = r->metadata_reservation.identity,
               .metadata_bytes = r->metadata_reservation.bytes,
               .metadata_owner =
                   atomic_load_explicit(&r->metadata_reservation.owner_bits,
                       memory_order_relaxed),
               .metadata_state =
                   atomic_load_explicit(&r->metadata_reservation.state,
                       memory_order_relaxed),
               .merge = r->merge_columns, .backup = r->retract_backup_columns,
               .scratch = r->row_scratch,
               .backup_ts = r->retract_backup_timestamps,
    };
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
    CHECK(now.token_bytes == before.token_bytes &&
        now.token_owner == before.token_owner &&
        now.token_state == before.token_state);
    CHECK(now.replacement == before.replacement &&
        now.token_identity == before.token_identity);
    CHECK(now.metadata_identity == before.metadata_identity &&
        now.metadata_bytes == before.metadata_bytes &&
        now.metadata_owner == before.metadata_owner &&
        now.metadata_state == before.metadata_state);
    CHECK(now.merge == before.merge && now.backup == before.backup &&
        now.scratch == before.scratch && now.backup_ts == before.backup_ts);
    for (uint32_t c = 0; c < r->ncols; c++)
        for (uint32_t row = 0; row < r->nrows; row++)
            CHECK(r->columns[c][row] == (int64_t)c * 10000 + row);
    if (r->timestamps)
        for (uint32_t row = 0; row < r->nrows; row++)
            CHECK(r->timestamps[row].iteration == row &&
                r->timestamps[row].stratum == row + 1 &&
                r->timestamps[row].worker == row + 2 &&
                r->timestamps[row]._reserved == 0 &&
                r->timestamps[row].multiplicity == -(int64_t)row);
}

/* Governed one-lane fixture: gates2+metadata1+doubling2, begin55,
 * table1+lane1+old1+new2, commit20 =85. Each endpoint is source-derived. */
static void
test_growth_governor_boundaries(void)
{
    const uint64_t endpoints[] = {1, 2, 3, 4, 5, 45, 47, 48, 53, 56, 57,
                                  60, 61, 62, 63, 65, 76, 77, 78, 81, 85};
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, src->memory_governor);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    CHECK(wl_columnar_memory_reserve(g, 37, &held));
    CHECK(wl_columnar_memory_commit(&held, &held));
    growth_snapshot_t before = snapshot(dst);
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    dst->mem_ledger = &ledger;
    col_rel_ledger_reconcile(dst, 0);
    uint64_t ledger_before = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    wl_evaluation_control_t *c = control(0);
    for (unsigned i = 0; i < sizeof(endpoints)/sizeof(endpoints[0]); i++) {
        if (i) {
            retry(c, endpoints[i] - 1);
            CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
                c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(finish(c,
                WL_EVALUATION_CONTROL_WORK_LIMIT) == endpoints[i-1]);
            unchanged(dst, before);
        }
        if (i) retry(c, 0);
        cancel_at = endpoints[i];
        wl_columnar_relation_test_watch_next_rollback_cleanup();
        if (endpoints[i] <= 60)
            wl_columnar_relation_test_growth_fail_allocation = 1;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == endpoints[i]);
        if (endpoints[i] <= 60) {
            CHECK(wl_columnar_relation_test_growth_fail_allocation == 1);
            CHECK(!wl_columnar_relation_test_rollback_cleanup_was_ordered());
            wl_columnar_relation_test_growth_fail_allocation = 0;
        } else
            CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
        unchanged(dst, before); released(src); released(dst);
        CHECK(held.bytes == 37 && held.identity == &held);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == ledger_before);
    }
    retry(c, 85);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 85 && seen == 85);
    CHECK(dst->nrows == 3 && dst->capacity == 4);
    payload_equal(dst, src, 1);
    CHECK(wl_columnar_memory_release(&held));
    release(c); destroy(dst);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == 0);
    destroy(src);
}

static wl_columnar_memory_reservation_t growth_peer;
static unsigned growth_peer_rounds;
static uint64_t growth_peer_base;
static bool growth_peer_cancel;
static void
growth_before_total(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t expected, bool overflow)
{
    (void)c;
    CHECK(!overflow);
    uint64_t amount = expected == growth_peer_base + 1 ? 2 : 1;
    CHECK(wl_columnar_memory_reserve(g, amount, &growth_peer));
    CHECK(wl_columnar_memory_reserved(g) != expected);
}
static void
growth_after_total(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, bool success, bool overflow)
{
    (void)g;
    CHECK(!success && !overflow);
    if (++growth_peer_rounds == 3) {
        /* Keep the final real peer reservation alive through the operation.
         * Its refreshed expected value then needs no extra stale retry. */
        wl_columnar_memory_governor_test_before_total_cas = NULL;
        wl_columnar_memory_governor_test_after_total_cas = NULL;
        if (growth_peer_cancel) wl_evaluation_control_request_cancel(c);
    } else
        CHECK(wl_columnar_memory_rollback(&growth_peer));
}
static void
test_growth_governor_retry(void)
{
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, src->memory_governor);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    wl_columnar_memory_reservation_init(&growth_peer);
    CHECK(wl_columnar_memory_reserve(g, 41, &held));
    CHECK(wl_columnar_memory_commit(&held, &held));
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(0);
    for (unsigned mode = 0; mode < 3; mode++) {
        if (mode) retry(c, mode == 1 ? 59 : 88);
        growth_peer_rounds = 0;
        growth_peer_base = before.reserved;
        growth_peer_cancel = mode == 0;
        wl_columnar_memory_governor_test_before_total_cas = growth_before_total;
        wl_columnar_memory_governor_test_after_total_cas = growth_after_total;
        int result = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED :
            mode == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
        int actual = wl_columnar_relation_append_all_controlled(dst, src, NULL,
                c);
        if (actual != result)
            fprintf(stderr,
                "growth retry mode=%u actual=%d expected=%d rounds=%u seen=%llu\n",
                mode, actual, result, growth_peer_rounds,
                (unsigned long long)seen);
        CHECK(actual == result);
        CHECK(growth_peer_rounds == 3);
        CHECK(growth_peer.bytes != 0);
        CHECK(wl_columnar_memory_rollback(&growth_peer));
        CHECK(finish(c, result) == (mode == 2 ? 88 : 59));
        CHECK(held.bytes == 41 && held.identity == &held);
        if (mode != 2) unchanged(dst, before);
        released(src); released(dst);
    }
    CHECK(wl_columnar_memory_release(&held));
    release(c); destroy(dst); destroy(src);
}

static bool growth_verdict_positive;
static unsigned growth_verdict_hits;
static unsigned growth_verdict_phase;
static void
growth_verdict(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (phase != growth_verdict_phase) return;
    growth_verdict_hits++;
    if (growth_verdict_positive)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else wl_evaluation_control_request_cancel(c);
}
static void
test_growth_governor_verdicts(void)
{
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, src->memory_governor);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    growth_snapshot_t before = snapshot(dst);
    uint64_t original = atomic_load_explicit(&g->usable_bytes,
            memory_order_relaxed);
    atomic_store_explicit(&g->usable_bytes, before.reserved,
        memory_order_relaxed);
    wl_evaluation_control_t *c = control(0);
    for (unsigned mode = 0; mode < 2; mode++) {
        if (mode) retry(c, 0);
        growth_verdict_positive = mode != 0; growth_verdict_hits = 0;
        growth_verdict_phase = WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT;
        wl_columnar_memory_governor_test_checkpoint = growth_verdict;
        wl_columnar_relation_test_growth_fail_allocation = 1;
        int result = mode ? EOVERFLOW : ENOMEM;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
            == result);
        CHECK(growth_verdict_hits == 1);
        CHECK(dst->memory_budget_denial_pending == !mode);
        CHECK(finish(c, result) == 57);
        CHECK(wl_columnar_relation_test_growth_fail_allocation == 1);
        wl_columnar_relation_test_growth_fail_allocation = 0;
        unchanged(dst, before); released(src); released(dst);
        wl_columnar_memory_governor_test_checkpoint = NULL;
    }
    atomic_store_explicit(&g->usable_bytes, original, memory_order_relaxed);
    retry(c, 85);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 85 && !dst->memory_budget_denial_pending);
    release(c); destroy(dst); destroy(src);
}

/* A positive accounting error at the commit boundary must still retire the
 * staged physical image before refunding its growth credit. */
static void
test_growth_governor_commit_error(void)
{
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, src->memory_governor);
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(0);
    const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                               WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF};
    const uint64_t costs[] = {78, 85};
    for (unsigned i = 0; i < 2; i++) {
        if (i) retry(c, 0);
        growth_verdict_positive = true; growth_verdict_hits = 0;
        growth_verdict_phase = phases[i];
        wl_columnar_memory_governor_test_checkpoint = growth_verdict;
        wl_columnar_relation_test_watch_next_rollback_cleanup();
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
            == EOVERFLOW);
        CHECK(growth_verdict_hits == 1);
        CHECK(finish(c, EOVERFLOW) == costs[i]);
        CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
        CHECK(!dst->memory_budget_denial_pending);
        unchanged(dst, before); released(src); released(dst);
        wl_columnar_memory_governor_test_checkpoint = NULL;
    }
    retry(c, 85);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 85);
    release(c); destroy(dst); destroy(src);
}

static void
test_growth_governor_invalid_credit(void)
{
    col_rel_t *src = relation(1, 2, 2, false, NULL);
    col_rel_t *dst = relation(1, 1, 1, false, src->memory_governor);
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(0);
    /* Deliberately inconsistent mirror; this is a defensive rejection test,
     * not a claimed valid producer of zero-credit allocated storage. */
    dst->retained_reserved_bytes = 0;
    wl_columnar_relation_test_growth_fail_allocation = 1;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == EINVAL);
    CHECK(finish(c, EINVAL) == 45);
    CHECK(wl_columnar_relation_test_growth_fail_allocation == 1);
    wl_columnar_relation_test_growth_fail_allocation = 0;
    CHECK(dst->retained_reserved_bytes == 0);
    dst->retained_reserved_bytes = before.credit;
    unchanged(dst, before); released(src); released(dst);
    release(c); destroy(dst); destroy(src);
}

static void
test_growth_governor_advisory(void)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 0, .usable_bytes = UINT64_MAX,
        .mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY,
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref != NULL);
    col_rel_t *src = relation(1, 2, 2, false, ref);
    col_rel_t *dst = relation(1, 1, 1, false, ref);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(ref);
    growth_snapshot_t before = snapshot(dst);
    CHECK(before.reserved > 0);
    wl_evaluation_control_t *c = control(85);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 85 && !dst->memory_budget_denial_pending);
    CHECK(wl_columnar_memory_reserved(g) ==
        before.reserved + 3 * sizeof(int64_t));
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
    CHECK(wl_columnar_memory_reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(ref);
}

static void
test_growth_governor_attach_existing(void)
{
    col_rel_t *dst = col_rel_new_auto("attached", 1);
    CHECK(dst && !dst->memory_governor && dst->capacity > 0);
    uint32_t b = dst->capacity;
    dst->nrows = b;
    for (uint32_t row = 0; row < b; row++) dst->columns[0][row] = row;
    CHECK(col_rel_row(dst, 0) != NULL); /* real ungoverned scratch producer */
    CHECK(col_rel_reserve_merge_grid(dst, b + 1) == 0);
    dst->merge_columns[0][0] = 123;
    col_rel_t *src = relation(1, b, b, false, NULL);
    CHECK(col_rel_attach_memory_governor(dst, src->memory_governor) == 0);
    CHECK(dst->retained_reserved_bytes > 0);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(dst->memory_governor);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    CHECK(wl_columnar_memory_reserve(g, 43, &held));
    CHECK(wl_columnar_memory_commit(&held, &held));
    growth_snapshot_t before = snapshot(dst);
    uint64_t incoming = sizeof(int64_t *) + 2 * (uint64_t)b * sizeof(int64_t);
    uint64_t old_grid = sizeof(int64_t *) + (uint64_t)b * sizeof(int64_t);
    uint64_t budget = atomic_load_explicit(&g->usable_bytes,
            memory_order_relaxed);
    uint64_t cost = 75 + 2 + 1 + 1 + 2 + 2 * (uint64_t)b;
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    dst->mem_ledger = &ledger;
    col_rel_ledger_reconcile(dst, 0);
    uint64_t ledger_before = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    wl_evaluation_control_t *c = control(0);
    atomic_store_explicit(&g->usable_bytes, before.reserved + incoming - 1,
        memory_order_relaxed);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(finish(c, ENOMEM) == 4 + 52 && dst->memory_budget_denial_pending);
    unchanged(dst, before);
    atomic_store_explicit(&g->usable_bytes, before.reserved + incoming,
        memory_order_relaxed);
    retry(c, 0); cancel_at = cost;
    growth_watch = dst; growth_peak = 0;
    wl_columnar_relation_test_watch_next_rollback_cleanup();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
    CHECK(growth_peak == before.reserved + incoming);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == ledger_before);
    unchanged(dst, before);
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(dst->retained_reserved_bytes == before.credit - old_grid + incoming);
    CHECK(wl_columnar_memory_reserved(g) ==
        before.reserved - old_grid + incoming);
    CHECK(dst->row_scratch == before.scratch &&
        dst->merge_columns == before.merge
        && dst->merge_columns[0][0] == 123);
    CHECK(held.bytes == 43 && held.identity == &held);
    payload_equal(dst, src, b);
    growth_watch = NULL;
    atomic_store_explicit(&g->usable_bytes, budget, memory_order_relaxed);
    CHECK(wl_columnar_memory_release(&held));
    release(c); destroy(dst);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == 0);
    destroy(src);
}

static void
test_growth_exact_and_cancel(void)
{
    col_rel_t *src = relation(2, 300, 300, true, NULL);
    col_rel_t *dst = relation(2, 300, 300, true, src->memory_governor);
    col_rel_t *reference = relation(2, 300, 300, true, NULL);
    uint64_t cost = 75 + 2 + 2 + 1 + 4 + 2 * 300 + 2 * 300 + 5 * 300 + 5 * 300; /* 2 metadata, 1 doubling, 4 allocations, 1200 cells, 3000 fields. */
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == cost - 4);
    CHECK(seen == cost - 4);
    unchanged(dst, before); released(dst); released(src);
    /* Source-ordered endpoints: gates, metadata, doubling, table/lane,
     * old cell chunks, timestamp allocation/copy, new cells/timestamps. */
    /* Gates/meta/doubling5; governed begin prefixes40,42,43,48,51,52,55;
     * old staging/copy endpoints shifted55; commit prefixes11,12,13,16,20. */
    const uint64_t boundaries[] = { 1, 2, 3, 4, 5, 45, 47, 48, 53, 56,
                                    57, 60, 61, 62, 318, 362, 363, 619, 663,
                                    664, 919, 2164, 2420, 2464, 2720, 2764,
                                    3019, 4264, 4275, 4276, 4277, 4280, 4284 };
    for (uint32_t i = 0; i < sizeof(boundaries) / sizeof(boundaries[0]); i++) {
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
    uint64_t cost = 75 + 2 + 300 + 2 + 1 + 300 + 300 + 600;
    wl_evaluation_control_t *c = control(cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    for (uint32_t lane = 0; lane < 300; lane++)
        CHECK(dst->columns[lane][0] == (int64_t)lane * 10000);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
    dst = relation(1, 3, 3, true, NULL);
    int64_t saved_cells[3];
    col_delta_timestamp_t saved_ts[3];
    memcpy(saved_cells, dst->columns[0], sizeof(saved_cells));
    memcpy(saved_ts, dst->timestamps, sizeof(saved_ts));
    c = control(42 + 75);
    CHECK(wl_columnar_relation_append_all_controlled(dst, dst, NULL, c) == 0);
    CHECK(finish(c, 0) == 42 + 75 && dst->nrows == 6);
    for (uint32_t row = 0; row < 3; row++) {
        for (uint32_t copy = 0; copy < 2; copy++) {
            uint32_t index = row + copy * 3;
            CHECK(dst->columns[0][index] == saved_cells[row]);
            col_delta_timestamp_t *t = &dst->timestamps[index];
            CHECK(t->iteration == saved_ts[row].iteration &&
                t->stratum == saved_ts[row].stratum &&
                t->worker == saved_ts[row].worker &&
                t->_reserved == saved_ts[row]._reserved &&
                t->multiplicity == saved_ts[row].multiplicity);
        }
    }
    released(dst); release(c); destroy(dst);

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
    wl_evaluation_control_t *c = control(30 + 75); /* meta1+validation2+doubling2+alloc3+old1+new2+norm2+timestamp15 */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 30 + 75);
    CHECK(dst->columns[0][0] == wl_columnar_float_to_bits(1.5));
    CHECK(dst->columns[0][1] == 0 && dst->columns[0][2] == src->columns[0][1]);
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
    CHECK(finish(c, ENOMEM) == 31 + 55 + 11);
    unchanged(dst, before);
    CHECK(dst->merge_columns == merge && dst->row_scratch == scratch);
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
        memory_order_relaxed) == 16);
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP],
        memory_order_relaxed) ==
        sizeof(col_delta_timestamp_t));
    retry(c, 31 + 75);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 31 + 75);
    CHECK(dst->merge_columns == merge && dst->row_scratch == scratch &&
        merge[0][0] == 99);
    CHECK(dst->retained_reserved_bytes == old_payload - old_grid + new_grid
        + 3 * sizeof(col_delta_timestamp_t));
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
        memory_order_relaxed) == 64);
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP],
        memory_order_relaxed) ==
        4 * sizeof(col_delta_timestamp_t));
    release(c); destroy(dst);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == 0);
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
    uint64_t usable = atomic_load_explicit(&g->usable_bytes,
            memory_order_relaxed);
    atomic_store_explicit(&g->usable_bytes, before.reserved,
        memory_order_relaxed);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 5 + 52); /* gates + metadata + doubling2, before allocation */
    unchanged(dst, before); released(dst); released(src);
    atomic_store_explicit(&g->usable_bytes, usable, memory_order_relaxed);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_prepare_resize();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 6 + 55);
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 10 + 55 + 11); /* Actual copied source and old prefix admitted. */
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    dst->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 2);
    dst->storage_generation = before.storage;
    unchanged(dst, before);
    retry(c, 10 + 75);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 10 + 75);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
}

/* Allocation ordinal expectations come from table, lane+old-prefix pairs,
 * then timestamps. They are independent of checkpoint observations. */
static void
test_growth_partial_faults_and_overlap(void)
{
    const uint32_t ordinals[] = { 1, 2, 3, 4, 5, 0 };
    const uint64_t prefixes[] = { 8 + 55, 9 + 55, 12 + 55, 15 + 55, 18 + 55,
                                  52 + 55 + 11 };
    for (uint32_t i = 0; i < sizeof(ordinals) / sizeof(ordinals[0]); i++) {
        col_rel_t *src = relation(3, 3, 3, true, NULL);
        col_rel_t *dst = relation(3, 2, 2, true, src->memory_governor);
        CHECK(col_rel_reserve_merge_grid(dst, 4) == 0);
        dst->merge_columns[0][0] = 91;
        growth_snapshot_t before = snapshot(dst);
        CHECK(before.credit > 0 &&
            before.token_state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        uint64_t incoming = 3 * sizeof(int64_t *) + 3 * 8 * sizeof(int64_t)
            + 8 * sizeof(col_delta_timestamp_t);
        uint64_t old_grid = 3 * sizeof(int64_t *) + 3 * 2 * sizeof(int64_t)
            + 2 * sizeof(col_delta_timestamp_t);
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        dst->mem_ledger = &ledger;
        col_rel_ledger_reconcile(dst, 0);
        uint64_t old_ledger = atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed);
        wl_evaluation_control_t *c = control(0);
        growth_watch = dst;
        growth_peak = 0;
        wl_columnar_relation_test_growth_fail_allocation = ordinals[i];
        if (!ordinals[i])
            wl_columnar_relation_test_fail_next_reservation_commit();
        wl_columnar_relation_test_watch_next_rollback_cleanup();
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == ENOMEM);
        CHECK(finish(c, ENOMEM) == prefixes[i]);
        CHECK(wl_columnar_relation_test_growth_fail_allocation == 0);
        CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
        /* A table failure has no completed allocation tail; later failures
        * and commit failure observe the entire overlapping reservation. */
        if (ordinals[i] != 1)
            CHECK(growth_peak == before.reserved + incoming);
        unchanged(dst, before);
        CHECK(dst->merge_columns[0][0] == 91);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == old_ledger);
        released(dst); released(src);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(dst->memory_governor);
        uint64_t original_budget = atomic_load_explicit(&g->usable_bytes,
                memory_order_relaxed);
        atomic_store_explicit(&g->usable_bytes, before.reserved + incoming - 1,
            memory_order_relaxed);
        retry(c, 0);
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == ENOMEM);
        CHECK(finish(c, ENOMEM) == 7 + 52 && dst->memory_budget_denial_pending);
        unchanged(dst, before);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == old_ledger);
        atomic_store_explicit(&g->usable_bytes, before.reserved + incoming,
            memory_order_relaxed);
        retry(c, 52 + 75);
        growth_peak = 0;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == 0);
        CHECK(finish(c, 0) == 52 + 75 && !dst->memory_budget_denial_pending);
        CHECK(growth_peak == before.reserved + incoming);
        CHECK(dst->retained_reserved_bytes ==
            before.credit - old_grid + incoming);
        CHECK(atomic_load_explicit(&g->reserved_bytes, memory_order_relaxed) ==
            before.reserved - old_grid + incoming);
        CHECK(dst->metadata_reservation.identity == before.metadata_identity &&
            dst->metadata_reservation.bytes == before.metadata_bytes);
        CHECK(dst->merge_columns == before.merge &&
            dst->row_scratch == before.scratch &&
            dst->merge_columns[0][0] == 91);
        CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
            memory_order_relaxed) ==
            3 * 8 * sizeof(int64_t));
        CHECK(atomic_load_explicit(
                &ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP],
                memory_order_relaxed) ==
            8 * sizeof(col_delta_timestamp_t));
        payload_equal(dst, src, 2);
        atomic_store_explicit(&g->usable_bytes, original_budget,
            memory_order_relaxed);
        growth_watch = NULL;
        release(c); destroy(dst);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == 0);
        destroy(src);
    }
}

static void
test_growth_scope_and_gate_stops(void)
{
    col_rel_t *src = relation(1, 3, 3, true, NULL);
    col_rel_t *dst = relation(1, 2, 2, true, NULL);
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 1);
    unchanged(dst, before); released(src); released(dst);
    retry(c, 0);
    cancel_at = 2;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 2);
    unchanged(dst, before); released(src); released(dst);
    dst->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 2);
    dst->view_generation = before.view;
    unchanged(dst, before);
    /* Synthetic independent-capacity metadata exercises rejection, without
     * claiming this forged descriptor proves production reachability. */
    dst->timestamp_capacity++;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    dst->timestamp_capacity--;
    unchanged(dst, before);
    release(c); destroy(dst); destroy(src);
    src = relation(1, 3, 3, false, NULL);
    dst = relation(1, 2, 2, true, NULL);
    before = snapshot(dst);
    c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    unchanged(dst, before);
    release(c); destroy(dst); destroy(src);
    src = relation(0, 3, 3, false, NULL);
    dst = relation(0, 2, 2, false, NULL);
    before = snapshot(dst);
    c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    unchanged(dst, before);
    release(c); destroy(dst); destroy(src);
    src = relation(1, 1, 1, false, NULL);
    dst = relation(1, 0, 0, false, NULL);
    CHECK(dst->capacity == 0);
    before = snapshot(dst);
    c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    unchanged(dst, before);
    release(c); destroy(dst);
    dst = relation(1, 2, 1, false, NULL);
    before = snapshot(dst);
    /* Synthetic numeric boundary only: overflow is checked before a cell read. */
    dst->capacity = dst->nrows = UINT32_MAX / 2u + 1u;
    c = control(0);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 4);
    dst->capacity = before.capacity;
    dst->nrows = before.rows;
    unchanged(dst, before);
    release(c); destroy(dst); destroy(src);

}

static void
test_growth_ungoverned_and_raw_twins(void)
{
    col_rel_t *dst = col_rel_new_auto("plain", 1);
    col_rel_t *raw = col_rel_new_auto("plain", 1);
    CHECK(dst && raw && !dst->memory_governor);
    uint32_t b = dst->capacity;
    col_rel_t *src = relation(1, b, b, false, NULL);
    dst->nrows = raw->nrows = b;
    for (uint32_t i = 0; i < b; i++)
        dst->columns[0][i] = raw->columns[0][i] = i;
    uint64_t cost = 2 + 1 + 1 + 2 + 2 * (uint64_t)b;
    wl_evaluation_control_t *c = control(cost);
    growth_snapshot_t before = snapshot(dst);
    cancel_at = cost;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    unchanged(dst, before);
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(col_rel_append_all(raw, src, NULL) == 0);
    payload_equal(dst, raw, 0);
    CHECK(dst->capacity == raw->capacity && dst->retained_reserved_bytes == 0);
    release(c); destroy(dst); destroy(raw); destroy(src);
    /* Typed FLOAT distinct raw twin with source-only timestamps. */
    src = relation(1, 3, 0, true, NULL);
    dst = relation(1, 2, 0, false, NULL);
    raw = relation(1, 2, 0, false, NULL);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    CHECK(col_rel_set_column_types(dst, &type, 1) == 0);
    CHECK(col_rel_set_column_types(raw, &type, 1) == 0);
    src->nrows = 3; dst->nrows = raw->nrows = 2;
    for (uint32_t i = 0; i < 3; i++)
        src->columns[0][i] = i ? wl_columnar_float_to_bits(3.5) : INT64_MIN;
    for (uint32_t i = 0; i < 2; i++)
        dst->columns[0][i] = raw->columns[0][i] =
            wl_columnar_float_to_bits(1.5);
    cost = 75 + 2 + 1 + 3 + 2 + 2 + 2 + 3 + 3;
    c = control(cost);
    int64_t **old_columns = dst->columns;
    uint64_t old_credit = dst->retained_reserved_bytes;
    cancel_at = cost; /* Final governor commit cutoff: no publication yet. */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    CHECK(dst->columns == old_columns && dst->nrows == 2 &&
        dst->retained_reserved_bytes == old_credit);
    CHECK(dst->columns[0][0] == wl_columnar_float_to_bits(1.5) &&
        dst->columns[0][1] == wl_columnar_float_to_bits(1.5));
    retry(c, cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(col_rel_append_all(raw, src, NULL) == 0);
    payload_equal(dst, raw, 0);
    CHECK(dst->capacity == raw->capacity && !dst->timestamps);
    release(c); destroy(dst); destroy(raw); destroy(src);
}

/* Independent region oracle from the source contract: I75, nonce1+1,
 * descriptor1, owner-presence1, optional journal18, resolver12, binding32,
 * owner1 (alias adds1), borrow/identity4, full validation128. */
static size_t
single_regions(unsigned mode, uint64_t regions[12])
{
    size_t n = 0;
    regions[n++] = 75; regions[n++] = 1; regions[n++] = 1;
    regions[n++] = 1; regions[n++] = 1;
    if (mode == 1) regions[n++] = 18;
    regions[n++] = 12; regions[n++] = 32; regions[n++] = 1;
    if (mode >= 2) regions[n++] = 1;
    regions[n++] = 4; regions[n++] = 128;
    return n;
}

static col_rel_t *
single_fixture(unsigned mode, col_rel_t **root)
{
    *root = NULL;
    if (mode < 2) {
        col_rel_t *r = relation(1, 4, 1, false, NULL);
        if (mode == 1) {
            r->storage_owner = NULL;
            r->storage_owner_identity = 73;
            r->storage_owner_generation = 91;
        }
        return r;
    }
    col_rel_t *a = col_rel_new_auto("single-a", 1);
    col_rel_t *b = col_rel_new_auto("single-b", 1);
    CHECK(a && b);
    bool a_first = (uintptr_t)&a->source_access < (uintptr_t)&b->source_access;
    *root = (a_first == (mode == 2)) ? a : b;
    col_rel_t *alias = *root == a ? b : a;
    int64_t row = 42;
    CHECK(col_rel_append_row(*root, &row) == 0);
    CHECK(col_rel_install_shared_view(alias, *root) == 0);
    CHECK(alias->storage_owner == *root);
    return alias;
}

static void
single_dispose(col_rel_t *r, col_rel_t *root)
{
    if (!r->storage_owner) {
        r->storage_owner = r;
        r->storage_owner_identity = r->relation_identity;
        r->storage_owner_generation = r->storage_generation;
    }
    destroy(r);
    if (root) destroy(root);
}

static void
single_open(col_rel_t *r, col_rel_t *root)
{
    CHECK(!wl_columnar_source_access_gate_busy(&r->descriptor_access));
    CHECK(!wl_columnar_source_access_gate_busy(&r->source_access));
    if (root) {
        CHECK(!wl_columnar_source_access_gate_busy(&root->source_access));
        CHECK(col_rel_storage_alias_borrow_count(root) == 1);
    }
}

static void
single_writer_thread_equal(const wl_columnar_source_access_writer_t *a,
    const wl_columnar_source_access_writer_t *b)
{
    if (!a->thread_valid || !a->owner) return;
#if defined(WL_HAVE_C11_THREADS)
    CHECK(thrd_equal(a->owner_thread, b->owner_thread));
#elif defined(_WIN32) || defined(_WIN64)
    CHECK(a->owner_thread == b->owner_thread);
#else
    CHECK(pthread_equal(a->owner_thread, b->owner_thread));
#endif
}

static void
single_state_equal(const wl_columnar_relation_mutation_single_t *a,
    const wl_columnar_relation_mutation_single_t *b)
{
    CHECK(a->set.identity == b->set.identity);
    CHECK(a->set.acquisition_nonce == b->set.acquisition_nonce);
    CHECK(a->set.roles == b->set.roles);
    CHECK(a->set.descriptors == b->set.descriptors);
    CHECK(a->set.owners == b->set.owners);
    CHECK(a->set.leases == b->set.leases);
    CHECK(a->set.initializations == b->set.initializations);
    CHECK(a->set.descriptor_count == b->set.descriptor_count);
    CHECK(a->set.owner_count == b->set.owner_count);
    CHECK(a->set.lease_count == b->set.lease_count);
    CHECK(a->set.initialization_count == b->set.initialization_count);
    CHECK(a->set.descriptors_acquired == b->set.descriptors_acquired);
    CHECK(a->set.owners_acquired == b->set.owners_acquired);
    CHECK(a->set.terminal_sequence == b->set.terminal_sequence);
    CHECK(a->set.terminal_expected_events == b->set.terminal_expected_events);
    CHECK(a->set.terminal_observed_events == b->set.terminal_observed_events);
    CHECK(a->set.terminal_sequence_consumed ==
        b->set.terminal_sequence_consumed);
    CHECK(a->set.published == b->set.published);
    CHECK(a->role.relation == b->role.relation);
    CHECK(a->role.role_flags == b->role.role_flags);
    CHECK(a->descriptor.relation == b->descriptor.relation);
    CHECK(a->lease.identity == b->lease.identity);
    CHECK(a->lease.acquisition_nonce == b->lease.acquisition_nonce);
    CHECK(a->lease.set == b->lease.set);
    CHECK(a->lease.relation == b->lease.relation);
    CHECK(a->lease.owner == b->lease.owner);
    CHECK(a->lease.descriptor_slot == b->lease.descriptor_slot);
    CHECK(a->lease.owner_slot == b->lease.owner_slot);
    CHECK(a->lease.self_owner_slot == b->lease.self_owner_slot);
    CHECK(a->lease.relation_identity == b->lease.relation_identity);
    CHECK(a->lease.relation_generation == b->lease.relation_generation);
    CHECK(a->lease.owner_identity == b->lease.owner_identity);
    CHECK(a->lease.owner_generation == b->lease.owner_generation);
    CHECK(a->lease.role_flags == b->lease.role_flags);
    CHECK(a->lease.detached == b->lease.detached);
    CHECK(a->lease.storage_transitioned == b->lease.storage_transitioned);
    CHECK(a->initialization.relation == b->initialization.relation);
    CHECK(a->initialization.owner_identity == b->initialization.owner_identity);
    CHECK(a->initialization.owner_generation ==
        b->initialization.owner_generation);
    CHECK(a->initialization.borrows == b->initialization.borrows);
    CHECK(a->descriptor.writer.owner == b->descriptor.writer.owner);
    CHECK(a->descriptor.writer.secondary_owner ==
        b->descriptor.writer.secondary_owner);
    CHECK(a->descriptor.writer.identity == b->descriptor.writer.identity);
    CHECK(a->descriptor.writer.thread_valid ==
        b->descriptor.writer.thread_valid);
    CHECK(a->owners[0].owner == b->owners[0].owner);
    CHECK(a->owners[0].writer.owner == b->owners[0].writer.owner);
    CHECK(a->owners[0].writer.secondary_owner ==
        b->owners[0].writer.secondary_owner);
    CHECK(a->owners[0].writer.identity == b->owners[0].writer.identity);
    CHECK(a->owners[0].writer.thread_valid == b->owners[0].writer.thread_valid);
    CHECK(a->owners[1].owner == b->owners[1].owner);
    CHECK(a->owners[1].writer.owner == b->owners[1].writer.owner);
    CHECK(a->owners[1].writer.secondary_owner ==
        b->owners[1].writer.secondary_owner);
    CHECK(a->owners[1].writer.identity == b->owners[1].writer.identity);
    CHECK(a->owners[1].writer.thread_valid == b->owners[1].writer.thread_valid);
    single_writer_thread_equal(&a->descriptor.writer, &b->descriptor.writer);
    single_writer_thread_equal(&a->owners[0].writer, &b->owners[0].writer);
    single_writer_thread_equal(&a->owners[1].writer, &b->owners[1].writer);
}

static void
test_single_regions(void)
{
    for (unsigned mode = 0; mode < 4; ++mode) {
        uint64_t regions[12], total = 0;
        size_t count = single_regions(mode, regions);
        for (size_t i = 0; i < count; ++i) total += regions[i];
        CHECK(total == (mode == 0 ? 256u : mode == 1 ? 274u : 257u));
        for (unsigned tail = 0; tail < 2; ++tail) {
            uint64_t prefix = 0;
            for (size_t i = 0; i < count; ++i) {
                col_rel_t *root, *r = single_fixture(mode, &root);
                wl_columnar_relation_mutation_single_t single = {0}, zero = {0};
                int64_t **columns = r->columns;
                uint32_t capacity = r->capacity;
                uint64_t generation = r->storage_generation;
                uint64_t held =
                    r->memory_governor ? wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(r->memory_governor)) :
                    0;
                uint64_t prior = prefix;
                prefix += regions[i];
                wl_evaluation_control_t *c = control(tail ? 0 : prefix - 1);
                if (tail) cancel_at = prefix;
                int expected =
                    tail ? WL_EVALUATION_CONTROL_CANCELLED :
                    WL_EVALUATION_CONTROL_WORK_LIMIT;
                CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
                    &single, c) == expected);
                CHECK(finish(c, expected) == (tail ? prefix : prior));
                single_state_equal(&single, &zero);
                single_open(r, root);
                CHECK(r->columns == columns && r->capacity == capacity
                    && r->storage_generation == generation);
                if (r->memory_governor)
                    CHECK(wl_columnar_memory_reserved(
                            wl_columnar_memory_governor_ref_get(
                                r->memory_governor)) == held);
                CHECK(r->nrows == 1 && r->columns[0][0] == (root ? 42 : 0));
                if (mode == 1) CHECK(!r->storage_owner &&
                        r->storage_owner_identity == 73 &&
                        r->storage_owner_generation == 91);
                release(c); single_dispose(r, root);
            }
        }
        for (unsigned raw = 0; raw < 2; ++raw) {
            col_rel_t *root, *r = single_fixture(mode, &root);
            wl_columnar_relation_mutation_single_t single = {0}, zero = {0};
            wl_evaluation_control_t *c = raw ? NULL : control(total);
            CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
                &single, c) == 0);
            CHECK(col_rel_mutation_lease_validate(&single.lease, r) == 0);
            CHECK(single.set.owner_count == (root ? 2u : 1u));
            if (root) CHECK((uintptr_t)&single.owners[0].owner->source_access <
                    (uintptr_t)&single.owners[1].owner->source_access);
            CHECK(wl_columnar_relation_mutation_single_finish_controlled(
                    &single, 0, c) == 0);
            single_state_equal(&single, &zero); single_open(r, root);
            if (c) {
                CHECK(finish(c, 0) == total); release(c);
            }
            CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
                &single, NULL) == 0);
            CHECK(wl_columnar_relation_mutation_single_finish_controlled(
                    &single, 0, NULL) == 0);
            single_dispose(r, root);
        }
    }
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_mutation_single_t single = {0}, zero = {0};
    wl_evaluation_control_t *c = control(0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    single_state_equal(&single, &zero); single_open(r, NULL);
    release(c); destroy(r);
}

static void
test_single_dirty(void)
{
    col_rel_t *a = relation(1, 4, 1, false, NULL);
    col_rel_t *b = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_mutation_single_t live = {0};
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(a, &live,
        NULL) == 0);
    wl_columnar_relation_mutation_single_t before = live, copy = live;
    for (unsigned controlled = 0; controlled < 2; ++controlled) {
        wl_evaluation_control_t *c = controlled ? control(0) : NULL;
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(b, &live,
            c) == EINVAL);
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(b, &copy,
            c) == EINVAL);
        single_state_equal(&live, &before); single_state_equal(&copy, &before);
        CHECK(col_rel_mutation_lease_validate(&live.lease, a) == 0);
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&copy, 0,
            c) == EINVAL);
        CHECK(!wl_columnar_source_access_gate_busy(&b->descriptor_access));
        if (c) {
            CHECK(finish(c, EINVAL) == 150); release(c);
        }
    }
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&live, 0,
        NULL) == 0);
    for (unsigned mode = 0; mode < 8; ++mode) {
        wl_columnar_relation_mutation_single_t dirty = {0};
        switch (mode) {
        case 0: dirty.role.relation = a; break;
        case 1: dirty.set.terminal_sequence_consumed = true; break;
        case 2: dirty.set.published = true; break;
        case 3: dirty.owners[0].writer.identity = 7; break;
        case 4: dirty.owners[1].writer.thread_valid = true; break;
        case 5: dirty.lease.storage_transitioned = true; break;
        case 6: dirty.initialization.borrows = 1; break;
        default: dirty.lease.detached = true; break;
        }
        before = dirty;
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(b, &dirty,
            NULL) == EINVAL);
        single_state_equal(&dirty, &before);
    }
    /* Raw failed admission retains descriptive role residue, despite retiring
     * this ordinary busy failure's authority. Verify retirement before reset. */
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(wl_columnar_source_access_reader_acquire(&b->descriptor_access,
        &reader) == 0);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(b, &live,
        NULL) == EBUSY);
    CHECK(live.role.relation == b
        && live.role.role_flags == WL_COLUMNAR_RELATION_PAYLOAD_MUTATION);
    CHECK(!live.set.identity && !live.set.descriptors_acquired
        && !live.set.owners_acquired && !live.descriptor.writer.owner
        && !live.owners[0].writer.owner && !live.owners[1].writer.owner);
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
    single_open(b, NULL);
    live = (wl_columnar_relation_mutation_single_t){0};
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(b, &live,
        NULL) == 0);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&live, 0,
        NULL) == 0);
    destroy(a); destroy(b);
}

static col_rel_t *single_peer;
static unsigned single_nonce_retries;
static void
single_nonce_interfere(wl_evaluation_control_t *c, uint64_t observed)
{
    (void)c; (void)observed;
    if (!single_nonce_retries) return;
    --single_nonce_retries;
    wl_columnar_relation_mutation_single_t peer = {0};
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(single_peer,
        &peer, NULL) == 0);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&peer, 0,
        NULL) == 0);
}

static void
test_single_nonce(void)
{
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    single_peer = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_test_before_nonce_cas = single_nonce_interfere;
    single_nonce_retries = 3;
    wl_columnar_relation_mutation_single_t single = {0};
    wl_evaluation_control_t *c = control(259);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        c) == 0);
    CHECK(single_nonce_retries == 0);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        c) == 0);
    CHECK(finish(c, 0) == 259); release(c);
    wl_columnar_relation_test_before_nonce_cas = NULL;
    for (unsigned zero = 0; zero < 2; ++zero) {
        uint64_t terminal = zero ? 0 : UINT64_MAX;
        uint64_t saved =
            wl_columnar_relation_test_single_nonce_replace(terminal);
        c = control(0);
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
            &single, c) == EOVERFLOW);
        CHECK(finish(c, EOVERFLOW) == 77);
        CHECK(wl_columnar_relation_test_single_nonce_replace(saved) ==
            terminal);
        release(c); single_open(r, NULL);
    }
    destroy(single_peer); single_peer = NULL; destroy(r);
}

static unsigned single_fail_slot, single_release_calls[3];
static int
single_release_failure(wl_columnar_relation_mutation_single_t *single,
    unsigned slot)
{
    (void)single;
    CHECK(slot < 3); ++single_release_calls[slot];
    return slot == single_fail_slot ? EIO : 0;
}

static void *
single_wrong_thread(void *arg)
{
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(arg, 0,
        NULL) == EINVAL);
    return NULL;
}

static void
test_single_cleanup(void)
{
    for (unsigned slot = 0; slot < 3; ++slot) {
        col_rel_t *root, *r = single_fixture(2, &root);
        wl_columnar_relation_mutation_single_t single = {0};
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
            &single, NULL) == 0);
        wl_columnar_relation_mutation_single_t before = single;
        wl_thread_t thread;
        CHECK(wl_thread_create(&thread, single_wrong_thread, &single) == 0);
        CHECK(wl_thread_join(&thread) == 0);
        single_state_equal(&single, &before);
        single_fail_slot = slot;
        memset(single_release_calls, 0, sizeof(single_release_calls));
        wl_columnar_relation_test_single_release = single_release_failure;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
            NULL) == EIO);
        CHECK(single.set.published && single.set.descriptors_acquired == 1);
        CHECK(single.set.owners_acquired == slot);
        CHECK(wl_columnar_source_access_gate_busy(&r->descriptor_access));
        CHECK(single_release_calls[2] == 1);
        CHECK(single_release_calls[1] == (slot <= 1 ? 1u : 0u));
        CHECK(single_release_calls[0] == (slot == 0 ? 1u : 0u));
        single_fail_slot = 99;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
            NULL) == 0);
        CHECK(single_release_calls[2] == (slot == 2 ? 2u : 1u));
        CHECK(single_release_calls[1] == (slot == 1 ? 2u : 1u));
        CHECK(single_release_calls[0] == (slot == 0 ? 2u : 1u));
        wl_columnar_relation_test_single_release = NULL;
        single_open(r, root); single_dispose(r, root);
    }
    for (unsigned committed = 0; committed < 2; ++committed) {
        col_rel_t *root, *r = single_fixture(1, &root);
        wl_columnar_relation_mutation_single_t single = {0};
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
            &single, NULL) == 0);
        CHECK(r->storage_owner == r);
        int execution = committed ? 0 : WL_EVALUATION_CONTROL_CANCELLED;
        single_fail_slot = 1;
        wl_columnar_relation_test_single_release = single_release_failure;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
            execution, NULL) == EIO);
        CHECK(single.set.initialization_count == 1 && r->storage_owner == r);
        single_fail_slot = 0;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
            execution, NULL) == EIO);
        CHECK(!single.set.initialization_count && !single.set.owners_acquired);
        CHECK((r->storage_owner == r) == (committed != 0));
        if (!committed) CHECK(r->storage_owner_identity == 73 &&
                r->storage_owner_generation == 91);
        wl_columnar_relation_test_single_release = NULL;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
            execution, NULL) == execution);
        single_open(r, NULL); single_dispose(r, root);
    }
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_mutation_single_t single = {0};
    wl_evaluation_control_t *c = control(256);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        c) == 0);
    int64_t value = 987;
    CHECK(col_rel_append_row_with_lease(r, &value, &single.lease) == 0);
    wl_evaluation_control_request_cancel(c);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        c) == 0);
    CHECK(r->nrows == 2 && r->columns[0][1] == 987);
    /* No post-publication poll converts the pending flag into an observed
     * stop; both mandatory cleanup and the completed attempt remain success. */
    CHECK(finish(c, 0) == 256);
    release(c); single_open(r, NULL); destroy(r);
}

static void
test_single_refusals(void)
{
    col_rel_t *root, *r = single_fixture(2, &root);
    wl_columnar_source_access_gate_t *gates[] = {&r->descriptor_access,
                                                 &r->source_access,
                                                 &root->source_access};
    for (size_t i = 0; i < 3; ++i) {
        wl_columnar_source_access_reader_t reader = {0};
        CHECK(wl_columnar_source_access_reader_acquire(gates[i], &reader) == 0);
        wl_columnar_relation_mutation_single_t single = {0};
        wl_evaluation_control_t *c = control(0);
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
            &single, c) == EBUSY);
        CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
        (void)finish(c, EBUSY); release(c); single_open(r, root);
    }
    uint64_t identity = r->storage_owner_identity,
        generation = r->storage_owner_generation;
    for (unsigned mode = 0; mode < 3; ++mode) {
        wl_columnar_relation_mutation_single_t single = {0};
        wl_evaluation_control_t *c = control(0);
        if (mode == 0) ++r->storage_owner_identity;
        if (mode == 1) ++r->storage_owner_generation;
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(mode ==
            2 ? root : r, &single, c) == (mode == 2 ? EBUSY : EINVAL));
        r->storage_owner_identity = identity;
        r->storage_owner_generation = generation;
        (void)finish(c, mode == 2 ? EBUSY : EINVAL); release(c);
        single_open(r, root);
    }
    single_dispose(r, root);
}

static void
test_single_partial_acquisition_cleanup(void)
{
    for (unsigned slot = 0; slot < 2; ++slot) {
        col_rel_t *root, *r = single_fixture(1, &root);
        wl_columnar_relation_mutation_single_t single = {0};
        wl_evaluation_control_t *c = control(0);
        /* Legacy admission after Q: 75+1+1+1+1+18+12+32+1+4. */
        cancel_at = 146;
        single_fail_slot = slot;
        wl_columnar_relation_test_single_release = single_release_failure;
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r,
            &single, c) == EIO);
        CHECK(single.set.descriptors_acquired == 1 && !single.set.published);
        CHECK(single.set.owners_acquired == slot);
        CHECK(single.set.initialization_count == slot);
        CHECK((r->storage_owner == r) == (slot == 1));
        wl_columnar_relation_test_single_release = NULL;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
            WL_EVALUATION_CONTROL_CANCELLED,
            c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!r->storage_owner && r->storage_owner_identity == 73 &&
            r->storage_owner_generation == 91);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 146);
        release(c); single_open(r, NULL); single_dispose(r, root);
    }
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_mutation_single_t single = {0};
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        NULL) == 0);
    wl_columnar_relation_terminal_sequence_t terminal = {0};
    single.set.terminal_sequence = &terminal;
    wl_columnar_relation_mutation_single_t before = single;
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        NULL) == EINVAL);
    single_state_equal(&single, &before);
    single.set.terminal_sequence = NULL;
    single_fail_slot = 1;
    wl_columnar_relation_test_single_release = single_release_failure;
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
        ENOMEM, NULL) == ENOMEM);
    CHECK(single.set.owners_acquired == 1 && !single.set.published);
    wl_columnar_relation_test_single_release = NULL;
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
        ENOMEM, NULL) == ENOMEM);
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        NULL) == 0);
    int64_t row = 88;
    /* The raw leased storage transition, unlike an in-capacity row write,
     * records set.published itself. Exercise that real publisher. */
    for (unsigned i = 0; i < 4; ++i)
        CHECK(col_rel_append_row_with_lease(r, &row, &single.lease) == 0);
    CHECK(single.set.published);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single,
        WL_EVALUATION_CONTROL_CANCELLED, NULL) == 0);
    CHECK(r->nrows == 5 && r->columns[0][1] == 88 && r->columns[0][4] == 88);
    single_open(r, NULL); destroy(r);
}

static uint64_t single_seen_nonce;
static void
single_nonce_observe(wl_evaluation_control_t *c, uint64_t observed)
{
    (void)c;
    single_seen_nonce = observed;
}

static void
test_single_nonce_stopped_publication(void)
{
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    wl_columnar_relation_mutation_single_t single = {0};
    wl_evaluation_control_t *c = control(0);
    cancel_at = 77; /* Successful nonce CAS tail, before inline publication. */
    wl_columnar_relation_test_before_nonce_cas = single_nonce_observe;
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(single_seen_nonce != 0);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 77);
    release(c);
    uint64_t consumed = single_seen_nonce;
    wl_columnar_relation_test_before_nonce_cas = NULL;
    CHECK(wl_columnar_relation_mutation_single_acquire_controlled(r, &single,
        NULL) == 0);
    CHECK(single.set.acquisition_nonce == consumed + 1);
    CHECK(wl_columnar_relation_mutation_single_finish_controlled(&single, 0,
        NULL) == 0);
    single_open(r, NULL); destroy(r);
}

int
main(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    for (unsigned mode = 0; mode < 4; mode++)
        test_gate_retry(mode);
    test_gate_stops_and_raw();
    test_gate_busy_and_overflow();
    test_append_gate_stops();
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
    for (unsigned typed = 0; typed < 2; typed++)
        for (unsigned source_ts = 0; source_ts < 2; source_ts++)
            for (unsigned destination_ts = 0; destination_ts < 2;
                destination_ts++)
                test_raw_parity(typed != 0, source_ts != 0,
                    destination_ts != 0);
    test_growth_governor_attach_existing();
    test_growth_governor_boundaries();
    test_growth_governor_retry();
    test_growth_governor_verdicts();
    test_growth_governor_commit_error();
    test_growth_governor_invalid_credit();
    test_growth_governor_advisory();
    test_growth_exact_and_cancel();
    test_growth_wide_and_same_owner();
    test_growth_float_and_timestamp_shortage();
    test_growth_auxiliary_and_ledger();
    test_growth_faults();
    test_growth_partial_faults_and_overlap();
    test_growth_scope_and_gate_stops();
    test_growth_ungoverned_and_raw_twins();
    test_single_regions();
    test_single_dirty();
    test_single_nonce();
    test_single_cleanup();
    test_single_refusals();
    test_single_partial_acquisition_cleanup();
    test_single_nonce_stopped_publication();
    puts("evaluation_work: OK");
    return 0;
}
