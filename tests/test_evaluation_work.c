/* #1821: bounded real scalar materialization, before engine integration. */
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
    CHECK(wl_columnar_relation_append_all_controlled(dst, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2 && dst->nrows == 1);
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
    puts("evaluation_work: OK");
    return 0;
}
