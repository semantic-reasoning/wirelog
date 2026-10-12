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

static void
test_growth_exact_and_cancel(void)
{
    col_rel_t *src = relation(2, 300, 300, true, NULL);
    col_rel_t *dst = relation(2, 300, 300, true, src->memory_governor);
    col_rel_t *reference = relation(2, 300, 300, true, NULL);
    uint64_t cost = 2 + 2 + 1 + 4 + 2 * 300 + 2 * 300 + 5 * 300 + 5 * 300; /* 2 metadata, 1 doubling, 4 allocations, 1200 cells, 3000 fields. */
    growth_snapshot_t before = snapshot(dst);
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == cost - 45 * 5);
    CHECK(seen == cost - 45 * 5);
    unchanged(dst, before); released(dst); released(src);
    /* Source-ordered endpoints: gates, metadata, doubling, table/lane,
     * old cell chunks, timestamp allocation/copy, new cells/timestamps. */
    const uint64_t boundaries[] = { 1, 2, 3, 4, 5, 6, 7, 263, 307,
                                    308, 564, 608, 609, 864, 2109, 2365, 2409,
                                    2665, 2709,
                                    2964, 4209 };
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
    uint64_t cost = 2 + 300 + 2 + 1 + 300 + 300 + 600;
    wl_evaluation_control_t *c = control(cost);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
    dst = relation(1, 3, 3, true, NULL);
    int64_t saved_cells[3];
    col_delta_timestamp_t saved_ts[3];
    memcpy(saved_cells, dst->columns[0], sizeof(saved_cells));
    memcpy(saved_ts, dst->timestamps, sizeof(saved_ts));
    c = control(42);
    CHECK(wl_columnar_relation_append_all_controlled(dst, dst, NULL, c) == 0);
    CHECK(finish(c, 0) == 42 && dst->nrows == 6);
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
    wl_evaluation_control_t *c = control(30); /* meta1+validation2+doubling2+alloc3+old1+new2+norm2+timestamp15 */
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 30);
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
    CHECK(finish(c, ENOMEM) == 31);
    unchanged(dst, before);
    CHECK(dst->merge_columns == merge && dst->row_scratch == scratch);
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
        memory_order_relaxed) == 16);
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP],
        memory_order_relaxed) ==
        sizeof(col_delta_timestamp_t));
    retry(c, 31);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 31);
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
    CHECK(finish(c, ENOMEM) == 5); /* gates + metadata + doubling2, before allocation */
    unchanged(dst, before); released(dst); released(src);
    atomic_store_explicit(&g->usable_bytes, usable, memory_order_relaxed);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_prepare_resize();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 6);
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    wl_columnar_relation_test_fail_next_reservation_commit();
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOMEM);
    CHECK(!dst->memory_budget_denial_pending);
    CHECK(finish(c, ENOMEM) == 10); /* Actual copied source and old prefix admitted. */
    unchanged(dst, before); released(dst); released(src);
    retry(c, 0);
    dst->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == EOVERFLOW);
    CHECK(finish(c, EOVERFLOW) == 2);
    dst->storage_generation = before.storage;
    unchanged(dst, before);
    retry(c, 10);
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL, c) == 0);
    CHECK(finish(c, 0) == 10);
    payload_equal(dst, src, 1);
    release(c); destroy(dst); destroy(src);
}

/* Allocation ordinal expectations come from table, lane+old-prefix pairs,
 * then timestamps. They are independent of checkpoint observations. */
static void
test_growth_partial_faults_and_overlap(void)
{
    const uint32_t ordinals[] = { 1, 4, 5, 0 };
    const uint64_t prefixes[] = { 8, 15, 18, 52 };
    for (uint32_t i = 0; i < 4; i++) {
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
        CHECK(finish(c, ENOMEM) == 7 && dst->memory_budget_denial_pending);
        unchanged(dst, before);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == old_ledger);
        atomic_store_explicit(&g->usable_bytes, before.reserved + incoming,
            memory_order_relaxed);
        retry(c, 52);
        growth_peak = 0;
        CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
            c) == 0);
        CHECK(finish(c, 0) == 52 && !dst->memory_budget_denial_pending);
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
    cost = 2 + 1 + 3 + 2 + 2 + 2 + 3 + 3;
    c = control(cost);
    int64_t **old_columns = dst->columns;
    uint64_t old_credit = dst->retained_reserved_bytes;
    cancel_at = cost; /* Normalization tail: final positive region. */
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

int
main(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
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
    test_growth_exact_and_cancel();
    test_growth_wide_and_same_owner();
    test_growth_float_and_timestamp_shortage();
    test_growth_auxiliary_and_ledger();
    test_growth_faults();
    test_growth_partial_faults_and_overlap();
    test_growth_scope_and_gate_stops();
    test_growth_ungoverned_and_raw_twins();
    puts("evaluation_work: OK");
    return 0;
}
