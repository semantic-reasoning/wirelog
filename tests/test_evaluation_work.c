/* #1821: bounded real scalar materialization, before engine integration. */
#define WL_TEST_RELATION_RESIZE_HOOK 1
#include "../wirelog/columnar/internal.h"
#define WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TEST_HOOKS 1
#include "../wirelog/columnar/mem_ledger_publication.h"
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
    /* Empty typed-source installation remains outside the supported route. */
    src->nrows = 0;
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

static uint64_t
exact_cost(uint32_t width, uint32_t rows, uint32_t capacity, bool timestamps,
    bool governed)
{
    return 2 * (uint64_t)width + (uint64_t)width * rows + 5
           + (timestamps ? 1 + 5 * (uint64_t)capacity : 0)
           + (governed ? 75 : 0);
}

static void
exact_payload(const col_rel_t *r, uint32_t rows, uint32_t capacity)
{
    CHECK(r->nrows == rows && r->capacity == capacity);
    for (uint32_t c = 0; c < r->ncols; c++)
        for (uint32_t row = 0; row < rows; row++)
            CHECK(r->columns[c][row] == (int64_t)c * 10000 + row);
    if (r->timestamps) {
        CHECK(r->timestamp_capacity == capacity);
        for (uint32_t row = 0; row < capacity; row++) {
            const col_delta_timestamp_t *t = &r->timestamps[row];
            CHECK(t->iteration == (row < rows ? row : 0));
            CHECK(t->stratum == (row < rows ? row + 1 : 0));
            CHECK(t->worker == (row < rows ? row + 2 : 0));
            CHECK(t->_reserved == 0);
            CHECK(t->multiplicity == (row < rows ? -(int64_t)row : 0));
        }
    } else
        CHECK(r->timestamp_capacity == 0);
}

static void
test_exact_capacity_success(uint32_t width, uint32_t rows, bool timestamps)
{
    uint32_t base = rows + 2, capacity = base + 3;
    col_rel_t *r = relation(width, base, rows, timestamps, NULL);
    growth_snapshot_t before = snapshot(r);
    uint64_t cost = exact_cost(width, rows, capacity, timestamps, true);
    wl_evaluation_control_t *c = control(cost - 1);
    bool denied = true;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, &denied, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!denied && finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == cost - 4);
    unchanged(r, before);
    retry(c, 0); cancel_at = cost;
    wl_columnar_relation_test_watch_next_rollback_cleanup();
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, &denied, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost && !denied);
    CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
    unchanged(r, before);
    retry(c, cost);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, &denied, c) == 0);
    CHECK(!denied && finish(c, 0) == cost && seen == cost);
    exact_payload(r, rows, capacity);
    CHECK(r->view_generation == before.view);
    CHECK(r->storage_generation == before.storage + 1);
    CHECK(r->retained_reservation.identity == before.token_identity);
    uint64_t bytes;
    CHECK(col_rel_retained_live_bytes(r, &bytes));
    CHECK(r->retained_reserved_bytes == bytes);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(r->memory_governor))
        == before.reserved - before.credit + bytes);
    release(c); destroy(r);
}

static size_t
exact_regions(uint32_t width, uint32_t rows, uint32_t capacity, bool timestamps,
    uint64_t regions[2048])
{
    size_t n = 0;
    for (uint32_t i = 0; i < width; i++) regions[n++] = 1;
    const uint64_t begin[] = {40, 2, 1, 5, 3, 1, 3};
    for (size_t i = 0; i < sizeof(begin) / sizeof(begin[0]); i++)
        regions[n++] = begin[i];
    regions[n++] = 1;
    for (uint32_t col = 0; col < width; col++) {
        regions[n++] = 1;
        for (uint32_t row = 0; row < rows;) {
            uint32_t count = rows - row;
            if (count > WL_EVALUATION_CONTROL_QUANTUM)
                count = WL_EVALUATION_CONTROL_QUANTUM;
            regions[n++] = count; row += count;
        }
    }
    if (timestamps) {
        regions[n++] = 1;
        for (unsigned part = 0; part < 2; part++) {
            uint32_t rows_part = part ? capacity - rows : rows;
            for (uint32_t row = 0; row < rows_part;) {
                uint32_t count = rows_part - row;
                if (count > WL_EVALUATION_CONTROL_QUANTUM / 5)
                    count = WL_EVALUATION_CONTROL_QUANTUM / 5;
                regions[n++] = 5 * (uint64_t)count; row += count;
            }
        }
    }
    const uint64_t end[] = {4, 11, 1, 1, 3, 4};
    for (size_t i = 0; i < sizeof(end) / sizeof(end[0]); i++)
        regions[n++] = end[i];
    CHECK(n < 2048);
    return n;
}

static void
test_exact_capacity_boundaries(void)
{
    col_rel_t *r = relation(1, 302, 300, true, NULL);
    const uint32_t capacity = 307;
    growth_snapshot_t before = snapshot(r);
    uint64_t regions[2048], prefix = 0;
    size_t count = exact_regions(1, 300, capacity, true, regions);
    wl_evaluation_control_t *c = control(0);
    CHECK(finish(c, 0) == 0);
    for (size_t i = 0; i < count; i++) {
        retry(c, prefix + regions[i] - 1);
        /* A zero configured limit means unlimited; use explicit cancellation
         * for the initial one-unit region instead. */
        if (prefix + regions[i] > 1) {
            bool denied = true;
            CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
                capacity, &denied, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(!denied && finish(c,
                WL_EVALUATION_CONTROL_WORK_LIMIT) == prefix);
            unchanged(r, before);
        } else
            CHECK(finish(c, 0) == 0);
        prefix += regions[i];
        retry(c, 0); cancel_at = prefix;
        bool denied = true;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            capacity, &denied, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!denied && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == prefix);
        unchanged(r, before);
    }
    CHECK(prefix == exact_cost(1, 300, capacity, true, true));
    retry(c, prefix);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, NULL, c) == 0);
    CHECK(finish(c, 0) == prefix);
    exact_payload(r, 300, capacity);
    release(c); destroy(r);
}

static void
test_exact_capacity_faults(void)
{
    col_rel_t *r = relation(3, 4, 2, true, NULL);
    const uint32_t capacity = 7;
    const uint32_t ordinals[] = {1, 2, 3, 4, 5, 0};
    /* W3, begin55, table1, each lane1+2, TS1+10, zero5*5,
     * publication4, wrapper11; primitive9 excluded at commit seam. */
    const uint64_t prefixes[] = {59, 60, 63, 66, 69, 119};
    growth_snapshot_t before = snapshot(r);
    wl_evaluation_control_t *c = control(0);
    for (size_t i = 0; i < sizeof(ordinals) / sizeof(ordinals[0]); i++) {
        if (i) retry(c, 0);
        wl_columnar_relation_test_growth_fail_allocation = ordinals[i];
        if (!ordinals[i])
            wl_columnar_relation_test_fail_next_reservation_commit();
        wl_columnar_relation_test_watch_next_rollback_cleanup();
        bool denied = true;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            capacity, &denied, c) == ENOMEM);
        CHECK(!denied && finish(c, ENOMEM) == prefixes[i]);
        CHECK(wl_columnar_relation_test_growth_fail_allocation == 0);
        CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
        unchanged(r, before);
    }
    uint64_t cost = exact_cost(3, 2, capacity, true, true);
    CHECK(cost == 128);
    retry(c, cost);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    exact_payload(r, 2, capacity);
    release(c); destroy(r);
}

/* Actual independent timestamp allocation precedes governor/ledger attachment.
* B is the constructor's real column capacity, never an invented byte count. */
static col_rel_t *
exact_independent_fixture(unsigned mode, bool governed, bool auxiliary)
{
    col_rel_t *r = col_rel_new_auto("exact", 2);
    CHECK(r && !r->memory_governor && r->capacity >= 4);
    r->nrows = 2;
    for (uint32_t c = 0; c < r->ncols; c++)
        for (uint32_t row = 0; row < r->nrows; row++)
            r->columns[c][row] = (int64_t)c * 10000 + row;
    uint32_t ts_capacity = mode == 1 ? 2 : mode == 2 ? r->capacity + 8 : 0;
    if (ts_capacity) {
        CHECK(!r->timestamps);
        r->timestamps = calloc(ts_capacity, sizeof(*r->timestamps));
        CHECK(r->timestamps != NULL);
        r->timestamp_capacity = ts_capacity;
        for (uint32_t row = 0; row < r->nrows; row++) {
            r->timestamps[row].iteration = row;
            r->timestamps[row].stratum = row + 1;
            r->timestamps[row].worker = row + 2;
            r->timestamps[row].multiplicity = -(int64_t)row;
        }
        for (uint32_t row = r->nrows; row < ts_capacity; row++)
            r->timestamps[row].worker = 123;
    }
    if (auxiliary) {
        CHECK(col_rel_row(r, 0) != NULL);
        CHECK(col_rel_reserve_merge_grid(r, 3) == 0);
        r->merge_columns[0][0] = 91;
        r->retract_backup_columns = col_columns_alloc(r->ncols, 3);
        CHECK(r->retract_backup_columns != NULL);
        r->retract_backup_capacity = 3;
        r->retract_backup_nrows = 1;
        r->retract_backup_columns[0][0] = 92;
        r->retract_backup_timestamps = calloc(3,
                sizeof(*r->retract_backup_timestamps));
        CHECK(r->retract_backup_timestamps != NULL);
        r->retract_backup_timestamp_capacity = 3;
        r->retract_backup_timestamps[0].worker = 93;
    }
    if (governed) {
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        };
        wl_columnar_memory_governor_ref_t *g =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(g && col_rel_attach_memory_governor(r, g) == 0);
        wl_columnar_memory_governor_ref_release(g);
    }
    return r;
}

static void
test_exact_capacity_independent(unsigned mode, bool governed)
{
    col_rel_t *r = exact_independent_fixture(mode, governed, true);
    col_rel_t *raw = exact_independent_fixture(mode, governed, true);
    growth_snapshot_t before = snapshot(r);
    const uint32_t capacity = r->capacity + 3;
    uint64_t cost = exact_cost(2, 2, capacity, mode != 0, governed);
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    r->mem_ledger = &ledger; col_rel_ledger_reconcile(r, 0);
    uint64_t old_ledger = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, NULL, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == cost - 4);
    unchanged(r, before);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == old_ledger);
    retry(c, cost);
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(wl_columnar_source_access_writer_acquire(&r->source_access,
        &writer) == 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        capacity, NULL, c) == 0);
    CHECK(wl_columnar_source_access_writer_validate(&writer,
        &r->source_access) == 0);
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(col_rel_reserve_capacity_admitted(raw, capacity, NULL) == 0);
    payload_equal(r, raw, 0);
    exact_payload(r, 2, capacity);
    CHECK(r->view_generation == before.view &&
        r->storage_generation == before.storage + 1);
    CHECK(r->merge_columns == before.merge && r->merge_columns[0][0] == 91);
    CHECK(r->retract_backup_columns == before.backup &&
        r->retract_backup_columns[0][0] == 92);
    CHECK(r->retract_backup_timestamps == before.backup_ts &&
        r->retract_backup_timestamps[0].worker == 93);
    CHECK(r->row_scratch == before.scratch);
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
        memory_order_relaxed) == 2 * (uint64_t)capacity * sizeof(int64_t));
    CHECK(atomic_load_explicit(&ledger.subsys_bytes[WL_MEM_SUBSYS_TIMESTAMP],
        memory_order_relaxed) ==
        (mode ? capacity * sizeof(*r->timestamps) : 0));
    release(c); destroy(r); destroy(raw);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == 0);
}

static void
test_exact_capacity_float_and_null(void)
{
    col_rel_t *r = relation(1, 4, 0, true, NULL);
    col_rel_t *raw = relation(1, 4, 0, true, NULL);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(r, &type, 1) == 0);
    CHECK(col_rel_set_column_types(raw, &type, 1) == 0);
    r->nrows = raw->nrows = 2;
    for (uint32_t row = 0; row < 2; row++) {
        r->columns[0][row] = raw->columns[0][row] =
            row ? wl_columnar_float_to_bits(3.5) : INT64_MIN;
        r->timestamps[row] = raw->timestamps[row] = (col_delta_timestamp_t){
            .iteration = row + 11, .stratum = row + 12, .worker = row + 13,
            ._reserved = row + 14, .multiplicity = -(int64_t)row - 15,
        };
    }
    uint64_t cost = exact_cost(1, 2, 7, true, true);
    wl_evaluation_control_t *c = control(0);
    int64_t **columns = r->columns;
    col_delta_timestamp_t *timestamps = r->timestamps;
    cancel_at = cost;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        7, NULL, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    CHECK(r->columns == columns && r->timestamps == timestamps &&
        r->capacity == 4);
    payload_equal(r, raw, 0);
    retry(c, cost);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        7, NULL, c) == 0);
    CHECK(finish(c, 0) == cost);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(raw,
        7, NULL, NULL) == 0);
    CHECK(r->columns[0][0] == INT64_MIN); /* reserve never normalizes FLOAT */
    payload_equal(r, raw, 0);
    for (uint32_t row = 2; row < 7; row++) {
        const col_delta_timestamp_t *a = &r->timestamps[row];
        const col_delta_timestamp_t *b = &raw->timestamps[row];
        CHECK(a->iteration == 0 && a->stratum == 0 && a->worker == 0 &&
            a->_reserved == 0 && a->multiplicity == 0);
        CHECK(a->iteration == b->iteration && a->stratum == b->stratum &&
            a->worker == b->worker && a->_reserved == b->_reserved &&
            a->multiplicity == b->multiplicity);
    }
    release(c); destroy(r); destroy(raw);
}

static col_rel_t *exact_ledger_peer;
static bool exact_ledger_changed;
static void
exact_shared_checkpoint(wl_evaluation_control_t *c, uint64_t work)
{
    checkpoint(c, work);
    if (!exact_ledger_changed && seen >= 60) {
        exact_ledger_changed = true;
        CHECK(col_rel_reserve_capacity_admitted(exact_ledger_peer,
            exact_ledger_peer->capacity + 5, NULL) == 0);
    }
}

static void
test_exact_capacity_shared_ledger(void)
{
    for (unsigned success = 0; success < 2; success++) {
        col_rel_t *r = relation(1, 4, 2, true, NULL);
        col_rel_t *peer = col_rel_new_auto("ledger-peer", 1);
        CHECK(peer && !peer->memory_governor);
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        r->mem_ledger = peer->mem_ledger = &ledger;
        col_rel_ledger_reconcile(r, 0); col_rel_ledger_reconcile(peer, 0);
        growth_snapshot_t before = snapshot(r);
        uint32_t peer_before = peer->capacity;
        uint64_t cost = exact_cost(1, 2, 7, true, true);
        wl_evaluation_control_t *c = control(0);
        exact_ledger_peer = peer; exact_ledger_changed = false;
        wl_columnar_relation_test_work_checkpoint = exact_shared_checkpoint;
        wl_columnar_memory_governor_test_work_checkpoint =
            exact_shared_checkpoint;
        if (!success) cancel_at = cost;
        int result = success ? 0 : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            7, NULL, c) == result);
        CHECK(finish(c, result) == cost && exact_ledger_changed);
        wl_columnar_relation_test_work_checkpoint = checkpoint;
        wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
        exact_ledger_peer = NULL;
        CHECK(peer->capacity == peer_before + 5);
        if (success) exact_payload(r, 2, 7); else unchanged(r, before);
        uint64_t expected = (uint64_t)(peer->capacity + r->capacity) *
            sizeof(int64_t) + r->timestamp_capacity * sizeof(*r->timestamps);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == expected);
        CHECK(atomic_load_explicit(&ledger.peak_bytes,
            memory_order_relaxed) >= expected);
        release(c); destroy(r);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) ==
            (uint64_t)peer->capacity * sizeof(int64_t));
        destroy(peer);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == 0);
    }
}

static void
test_exact_capacity_governor(void)
{
    col_rel_t *r = relation(1, 4, 2, true, NULL);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(r->memory_governor);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    CHECK(wl_columnar_memory_reserve(g, 37, &held));
    CHECK(wl_columnar_memory_commit(&held, &held));
    growth_snapshot_t before = snapshot(r);
    uint64_t incoming = sizeof(int64_t *) + 7 * sizeof(int64_t)
        + 7 * sizeof(*r->timestamps);
    uint64_t budget = atomic_load_explicit(&g->usable_bytes,
            memory_order_relaxed);
    wl_evaluation_control_t *c = control(0);
    atomic_store_explicit(&g->usable_bytes, before.reserved + incoming - 1,
        memory_order_relaxed);
    bool denied = false;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        7, &denied, c) == ENOMEM);
    CHECK(denied && r->memory_budget_denial_pending && finish(c, ENOMEM) == 53);
    unchanged(r, before);
    for (unsigned positive = 0; positive < 2; positive++) {
        retry(c, 0);
        growth_verdict_positive = positive != 0; growth_verdict_hits = 0;
        growth_verdict_phase = WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT;
        wl_columnar_memory_governor_test_checkpoint = growth_verdict;
        wl_columnar_relation_test_growth_fail_allocation = 1;
        int result = positive ? EOVERFLOW : ENOMEM;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            7, &denied, c) == result);
        CHECK(growth_verdict_hits == 1 && denied == !positive);
        CHECK(finish(c, result) == 53);
        CHECK(wl_columnar_relation_test_growth_fail_allocation == 1);
        wl_columnar_relation_test_growth_fail_allocation = 0;
        wl_columnar_memory_governor_test_checkpoint = NULL;
        unchanged(r, before);
    }
    atomic_store_explicit(&g->usable_bytes, before.reserved + incoming,
        memory_order_relaxed);
    uint64_t cost = exact_cost(1, 2, 7, true, true);
    const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                               WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF};
    for (unsigned i = 0; i < 2; i++) {
        retry(c, 0);
        growth_verdict_positive = true; growth_verdict_hits = 0;
        growth_verdict_phase = phases[i];
        wl_columnar_memory_governor_test_checkpoint = growth_verdict;
        wl_columnar_relation_test_watch_next_rollback_cleanup();
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            7, &denied, c) == EOVERFLOW);
        CHECK(!denied && growth_verdict_hits == 1);
        CHECK(finish(c, EOVERFLOW) == cost - (i ? 0 : 7));
        CHECK(wl_columnar_relation_test_rollback_cleanup_was_ordered());
        wl_columnar_memory_governor_test_checkpoint = NULL;
        unchanged(r, before);
    }
    retry(c, cost); growth_watch = r; growth_peak = 0;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        7, &denied, c) == 0);
    CHECK(!denied && finish(c, 0) == cost);
    CHECK(growth_peak == before.reserved + incoming);
    growth_watch = NULL;
    CHECK(held.bytes == 37 && held.identity == &held);
    atomic_store_explicit(&g->usable_bytes, budget, memory_order_relaxed);
    CHECK(wl_columnar_memory_release(&held));
    release(c); destroy(r);
    /* Actual peer credit creates three failed total CAS operations. */
    r = relation(1, 4, 2, false, NULL);
    before = snapshot(r);
    c = control(0);
    wl_columnar_memory_reservation_init(&growth_peer);
    growth_peer_rounds = 0; growth_peer_base = before.reserved;
    growth_peer_cancel = false;
    wl_columnar_memory_governor_test_before_total_cas = growth_before_total;
    wl_columnar_memory_governor_test_after_total_cas = growth_after_total;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        7, &denied, c) == 0);
    CHECK(!denied && growth_peer_rounds == 3);
    CHECK(finish(c, 0) == exact_cost(1, 2, 7, false, true) + 3);
    CHECK(wl_columnar_memory_rollback(&growth_peer));
    release(c); destroy(r);
    /* Advisory governor uses its genuine unbounded usable limit. */
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 0, .usable_bytes = UINT64_MAX,
        .mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY,
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref);
    r = relation(1, 4, 2, false, ref);
    c = control(0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        7, &denied, c) == 0);
    CHECK(!denied && finish(c, 0) == exact_cost(1, 2, 7, false, true));
    release(c); destroy(r);
    CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            ref)) == 0);
    wl_columnar_memory_governor_ref_release(ref);
}

static void
test_exact_capacity_no_growth_and_scope(void)
{
    col_rel_t *r = exact_independent_fixture(2, true, false);
    growth_snapshot_t before = snapshot(r);
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    r->mem_ledger = &ledger; col_rel_ledger_reconcile(r, 0);
    uint64_t ledger_before = atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed);
    wl_evaluation_control_t *c = control(1);
    bool denied = true;
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, &denied, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!denied && finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 1);
    unchanged(r, before);
    r->storage_generation = r->storage_owner_generation =
        WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    retry(c, 2);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        1, &denied, c) == 0);
    CHECK(!denied && finish(c, 0) == 2);
    for (uint32_t row = 2; row < r->timestamp_capacity; row++)
        CHECK(r->timestamps[row].worker == 123);
    CHECK(r->storage_generation == WL_COLUMNAR_REL_GENERATION_INVALID - 1u);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity + 3, &denied, c) == EOVERFLOW);
    CHECK(!denied && finish(c, EOVERFLOW) == 0);
    r->storage_generation = r->storage_owner_generation = before.storage;
    unchanged(r, before);
    /* Malformed token, including a real active replacement, is not merely
     * unsupported missing credit. Every deliberate invalid edit is restored. */
    const void *identity = r->retained_reservation.identity;
    r->retained_reservation.identity = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, &denied, c) == EINVAL);
    CHECK(!denied && finish(c, EINVAL) == 2);
    r->retained_reservation.identity = identity;
    CHECK(wl_columnar_memory_begin_growth(&r->retained_reservation,
        1) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity + 3, &denied, c) == EINVAL);
    CHECK(!denied && finish(c, EINVAL) == 2);
    CHECK(wl_columnar_memory_rollback_growth(&r->retained_reservation));
    unchanged(r, before);
    int64_t *lane = r->columns[0]; r->columns[0] = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, &denied, c) == EINVAL);
    CHECK(finish(c, EINVAL) == 1); r->columns[0] = lane;
    int64_t **grid = r->columns; r->columns = NULL;
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, NULL, c) == EINVAL);
    CHECK(finish(c, EINVAL) == 0); r->columns = grid;
    col_rel_t *alias = relation(2, 4, 0, false, NULL);
    CHECK(col_rel_install_shared_view(alias, r) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, NULL, c) == EBUSY);
    CHECK(finish(c, EBUSY) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(alias,
        alias->capacity, NULL, c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(alias,
        alias->capacity, NULL, NULL) == 0); /* actual broad raw delegation */
    destroy(alias); unchanged(r, before);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == ledger_before);
    release(c); destroy(r);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == 0);
    for (unsigned width = 0; width < 2; width++) {
        r = relation(width, 0, 0, false, NULL);
        c = control(0);
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            7, NULL, c) == ENOTSUP);
        CHECK(finish(c, ENOTSUP) == 0);
        release(c); destroy(r);
    }
    wl_arena_t *arena = wl_arena_create(4096);
    delta_pool_t *pool = delta_pool_create(1, sizeof(col_rel_t), 32);
    CHECK(arena && pool);
    r = col_rel_pool_new_auto(pool, arena, "exact-arena", 1);
    CHECK(r && r->arena_owned);
    c = control(0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, NULL, c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0);
    release(c); destroy(r); delta_pool_destroy(pool); wl_arena_free(arena);
    r = relation(2, 4, 0, false, NULL);
    const col_rel_logical_col_t logical = {WIRELOG_COMPOUND_KIND_INLINE, 2u,
                                           1u};
    CHECK(col_rel_apply_compound_schema(r, &logical, 1) == 0);
    c = control(0);
    CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
        r->capacity, NULL, c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 0);
    release(c); destroy(r);
}

/* Unsupported partial initialization, not a production admitted relation:
 * actual descriptor/scratch credit exists, but this private grid has never
 * been admitted. No previously admitted live-grid credit is reduced. */
static void
test_exact_capacity_partial_initialization(unsigned mode)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref);
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    col_rel_t *r = NULL;
    CHECK(wl_columnar_relation_alloc_governed(&r, "partial-exact", ref) == 0);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    CHECK(wl_columnar_memory_reserve(g, 19, &held));
    CHECK(wl_columnar_memory_commit(&held, &held));
    if (mode) {
        CHECK(wl_columnar_memory_reserve(g, sizeof(int64_t),
            &r->retained_reservation));
        r->row_scratch = malloc(sizeof(int64_t));
        if (!r->row_scratch) {
            CHECK(wl_columnar_memory_rollback(&r->retained_reservation));
            CHECK(r->row_scratch != NULL);
        }
        CHECK(wl_columnar_memory_commit(&r->retained_reservation, r));
        r->row_scratch[0] = 97;
        r->retained_reserved_bytes = sizeof(int64_t);
        if (mode == 1) {
            free(r->row_scratch); r->row_scratch = NULL;
            CHECK(wl_columnar_memory_release(&r->retained_reservation));
            r->retained_reserved_bytes = 0;
        }
    }
    r->columns = col_columns_alloc(1, 4); CHECK(r->columns);
    r->ncols = 1; r->capacity = 4; r->nrows = 1; r->columns[0][0] = 0;
    growth_snapshot_t before = snapshot(r);
    CHECK(before.token_state ==
        (mode == 0 ? WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        : mode == 1 ? WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
        : WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED));
    if (mode == 1) CHECK(before.token_bytes == sizeof(int64_t) &&
            before.token_owner == (uintptr_t)r);
    wl_evaluation_control_t *c = control(0);
    for (unsigned growth = 0; growth < 2; growth++) {
        if (growth) retry(c, 0);
        bool denied = true;
        CHECK(wl_columnar_relation_reserve_capacity_admitted_controlled(r,
            growth ? 7 : 4, &denied, c) == ENOTSUP);
        CHECK(!denied && finish(c, ENOTSUP) == 1);
        unchanged(r, before);
        if (mode == 2) CHECK(r->row_scratch[0] == 97);
        CHECK(held.bytes == 19 && held.identity == &held);
    }
    col_columns_free(r->columns, 1); r->columns = NULL;
    r->ncols = r->capacity = r->nrows = 0;
    if (mode == 2) {
        free(r->row_scratch); r->row_scratch = NULL;
        CHECK(wl_columnar_memory_release(&r->retained_reservation));
        r->retained_reserved_bytes = 0;
    }
    release(c); destroy(r);
    CHECK(wl_columnar_memory_reserved(g) == 19);
    CHECK(wl_columnar_memory_release(&held));
    CHECK(wl_columnar_memory_reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(ref);
}

/* Real flat owned metadata, completed before governor attachment. Missing
 * names are physically retired and Arrow names replaced through its API. */
static col_rel_t *
metadata_fixture(uint32_t width, bool governed, unsigned names_mode,
    bool long_names, bool timestamps)
{
    char long_name[301];
    memset(long_name, 'x', sizeof(long_name) - 1);
    long_name[sizeof(long_name) - 1] = '\0';
    const char **names = calloc(width, sizeof(*names));
    CHECK(names != NULL);
    for (uint32_t c = 0; c < width; c++)
        names[c] = c % 3 == 0 ? (long_names ? long_name : "x")
            : c % 3 == 1 ? "" : NULL;
    col_rel_t *r = col_rel_new_auto("metadata", 0);
    CHECK(r && col_rel_set_schema(r, width, names) == 0);
    free(names);
    CHECK(r->capacity >= 4 && !r->column_types && r->schema_ok);
    if (names_mode) {
        for (uint32_t c = 0; c < width; c++) {
            if (names_mode == 1 && c % 2) continue;
            free(r->col_names[c]);
            r->col_names[c] = NULL;
            CHECK(ArrowSchemaSetName(r->schema.children[c], "") == 0);
        }
        if (names_mode == 2) {
            free(r->col_names);
            r->col_names = NULL;
        }
    }
    r->nrows = 2;
    for (uint32_t c = 0; c < width; c++)
        for (uint32_t row = 0; row < 2; row++)
            r->columns[c][row] = (int64_t)c * 10000 + row;
    if (timestamps) {
        CHECK(!r->timestamps);
        r->timestamps = calloc(r->capacity, sizeof(*r->timestamps));
        CHECK(r->timestamps != NULL);
        r->timestamp_capacity = r->capacity;
        for (uint32_t row = 0; row < 2; row++) {
            r->timestamps[row].iteration = row;
            r->timestamps[row].stratum = row + 1;
            r->timestamps[row].worker = row + 2;
            r->timestamps[row].multiplicity = -(int64_t)row;
        }
    }
    if (governed) {
        wl_columnar_memory_resolution_t resolution = {
            .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        };
        wl_columnar_memory_governor_ref_t *g =
            wl_columnar_memory_governor_ref_create(&resolution);
        CHECK(g && col_rel_attach_memory_governor(r, g) == 0);
        wl_columnar_memory_governor_ref_release(g);
    }
    return r;
}

static col_rel_t *
metadata_source(uint32_t width, bool timestamps)
{
    col_rel_t *r = relation(width, 4, 2, timestamps, NULL);
    wirelog_column_type_t *types = malloc(width * sizeof(*types));
    const wirelog_column_type_t family[] = {
        WIRELOG_TYPE_INT32, WIRELOG_TYPE_INT64,
        WIRELOG_TYPE_UINT32, WIRELOG_TYPE_UINT64
    };
    CHECK(types != NULL);
    for (uint32_t c = 0; c < width; c++) types[c] = family[c % 4];
    CHECK(col_rel_set_column_types(r, types, width) == 0);
    free(types);
    return r;
}

static uint64_t
metadata_name_length(const col_rel_t *r, uint32_t c)
{
    return r->col_names && r->col_names[c] ? strlen(r->col_names[c]) + 1 : 1;
}

static uint64_t
metadata_cost(const col_rel_t *r, const col_rel_t *src)
{
    uint64_t chars = 0, w = r->ncols, rows = src->nrows;
    for (uint32_t c = 0; c < r->ncols; c++) chars += metadata_name_length(r, c);
    return 2 + w + 2 * w * rows
           + (r->timestamps && src->timestamps ? 5 * rows : 0)
           + 6 + 7 * w + 2 * chars + (r->memory_governor ? 34 : 0);
}

/* Explicit protocol inventory; independent of observed checkpoint totals. */
static size_t
metadata_regions(const col_rel_t *r, const col_rel_t *src, uint64_t *out,
    uint64_t *allocation_prefixes, size_t *allocations)
{
    size_t n = 0, a = 0;
    uint64_t total = 0;
#define MR(v) do { uint64_t amount = (v); out[n++] = amount; total += amount; \
} while (0)
#define MA(v) do { MR(v); allocation_prefixes[a++] = total; } while (0)
#define RESERVE_REGIONS() do { MR(3); MR(1); MR(5); MR(4); MR(1); MR(1); \
} while (0)
    MR(1); MR(1);
    for (uint32_t c = 0; c < r->ncols; c++) MR(1);
    for (uint32_t row = 0; row < src->nrows; row++)
        for (uint32_t c = 0; c < r->ncols; c++) MR(1);
    if (r->memory_governor) RESERVE_REGIONS();
    MA(1); /* length cache */
    for (uint32_t c = 0; c < r->ncols; c++) {
        for (uint64_t j = 0; j < metadata_name_length(r, c); j++) MR(1);
        MR(1);
    }
    if (r->memory_governor) RESERVE_REGIONS();
    MA(1); /* type array */
    for (uint32_t c = 0; c < r->ncols; c++) MR(1);
    MA(3); MA(1); /* root format, child pointer table */
    for (uint32_t c = 0; c < r->ncols;) {
        uint32_t count = r->ncols - c;
        if (count > WL_EVALUATION_CONTROL_QUANTUM)
            count = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
        MR(count); c += count;
    }
    for (uint32_t c = 0; c < r->ncols; c++) {
        MA(1); MA(2); MA(1); /* child, fixed format, name */
        for (uint64_t j = 0, length = metadata_name_length(r, c); j < length;) {
            uint64_t count = length - j;
            if (count > WL_EVALUATION_CONTROL_QUANTUM)
                count = WL_EVALUATION_CONTROL_QUANTUM;
            MR(count); j += count;
        }
    }
    for (uint32_t c = 0; c < r->ncols; c++) {
        for (uint32_t row = 0; row < src->nrows;) {
            uint32_t count = src->nrows - row;
            if (count > WL_EVALUATION_CONTROL_QUANTUM)
                count = (uint32_t)WL_EVALUATION_CONTROL_QUANTUM;
            MR(count); row += count;
        }
    }
    if (r->timestamps && src->timestamps) {
        for (uint32_t row = 0; row < src->nrows;) {
            uint32_t count = src->nrows - row;
            if (count > WL_EVALUATION_CONTROL_QUANTUM / 5u)
                count = (uint32_t)(WL_EVALUATION_CONTROL_QUANTUM / 5u);
            MR(5u * count); row += count;
        }
    }
    if (r->memory_governor) {
        MR(1); MR(1); MR(2);
    }
#undef RESERVE_REGIONS
#undef MA
#undef MR
    CHECK(total == metadata_cost(r, src));
    *allocations = a;
    return n;
}

static void
metadata_unchanged(col_rel_t *r, growth_snapshot_t before,
    const struct ArrowSchema *schema, char **names, uint64_t credit)
{
    unchanged(r, before);
    CHECK(!r->column_types && r->col_names == names && r->schema_ok);
    CHECK(memcmp(&r->schema, schema, sizeof(*schema)) == 0);
    CHECK(r->metadata_reserved_bytes == credit);
    released(r);
}

static void
metadata_equal(col_rel_t *r, col_rel_t *raw, const col_rel_t *src)
{
    CHECK(r->nrows == 4 && raw->nrows == 4);
    payload_equal(r, raw, 0);
    CHECK(strcmp(r->schema.format, "+s") == 0);
    CHECK(r->schema.n_children == r->ncols);
    for (uint32_t c = 0; c < r->ncols; c++) {
        CHECK(r->column_types[c] == src->column_types[c]);
        CHECK(r->column_types[c] == raw->column_types[c]);
        CHECK(strcmp(r->schema.children[c]->format, "l") == 0);
        CHECK(strcmp(r->schema.children[c]->name,
            raw->schema.children[c]->name) == 0);
    }
}

static void
test_metadata_success(uint32_t width, bool governed, unsigned names_mode,
    bool long_names, bool timestamps)
{
    col_rel_t *r = metadata_fixture(width, governed, names_mode, long_names,
            timestamps);
    col_rel_t *raw = metadata_fixture(width, governed, names_mode, long_names,
            timestamps);
    col_rel_t *src = metadata_source(width, timestamps);
    growth_snapshot_t before = snapshot(r);
    struct ArrowSchema schema = r->schema;
    char **names = r->col_names;
    uint64_t old_credit = r->metadata_reserved_bytes;
    uint64_t cost = metadata_cost(r, src);
    uint64_t last = governed ? 2 : timestamps ? 10 : 2;
    wl_evaluation_control_t *c = control(cost - 1);
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c)
        == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == cost - last);
    metadata_unchanged(r, before, &schema, names, old_credit);
    retry(c, 0); cancel_at = cost;
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cost);
    metadata_unchanged(r, before, &schema, names, old_credit);
    retry(c, cost); growth_watch = r; growth_peak = before.reserved;
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c) == 0);
    growth_watch = NULL;
    CHECK(finish(c, 0) == cost && seen == cost);
    CHECK(wl_columnar_relation_append_all_controlled(raw, src, NULL,
        NULL) == 0);
    metadata_equal(r, raw, src);
    CHECK(r->col_names == names && r->columns == before.columns);
    CHECK(r->capacity == before.capacity &&
        r->storage_generation == before.storage);
    CHECK(r->view_generation == before.view + 1);
    if (governed) {
        uint64_t next = old_credit + width * sizeof(wirelog_column_type_t);
        CHECK(r->metadata_reserved_bytes == next);
        CHECK(r->metadata_reservation.bytes == next);
        CHECK(r->metadata_reservation.identity == &r->metadata_reservation);
        CHECK(atomic_load_explicit(&r->metadata_reservation.owner_bits,
            memory_order_relaxed) == (uint64_t)(uintptr_t)r);
        CHECK(atomic_load_explicit(&r->metadata_reservation.state,
            memory_order_relaxed) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED);
        CHECK(growth_peak == before.reserved + width * sizeof(uint64_t) + next);
        CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(r->memory_governor))
            == before.reserved + next - old_credit);
    }
    wl_columnar_memory_governor_ref_t *ref = r->memory_governor;
    if (ref) wl_columnar_memory_governor_ref_retain(ref);
    release(c); destroy(r); destroy(raw); destroy(src);
    if (ref) {
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                ref)) == 0);
        wl_columnar_memory_governor_ref_release(ref);
    }
}

static void
test_metadata_boundaries(void)
{
    col_rel_t *r = metadata_fixture(2, true, 0, true, true);
    col_rel_t *src = metadata_source(2, true);
    growth_snapshot_t before = snapshot(r);
    struct ArrowSchema schema = r->schema;
    char **names = r->col_names;
    uint64_t credit = r->metadata_reserved_bytes;
    uint64_t regions[2048], allocation_prefixes[32], prefix = 0;
    size_t allocations;
    size_t count = metadata_regions(r, src, regions, allocation_prefixes,
            &allocations);
    CHECK(allocations == 10);
    wl_evaluation_control_t *c = control(0);
    CHECK(finish(c, 0) == 0);
    for (size_t i = 0; i < count; i++) {
        retry(c, prefix + regions[i] - 1);
        if (prefix + regions[i] > 1) {
            CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c)
                == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == prefix);
            metadata_unchanged(r, before, &schema, names, credit);
        } else CHECK(finish(c, 0) == 0);
        prefix += regions[i]; retry(c, 0); cancel_at = prefix;
        CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == prefix);
        metadata_unchanged(r, before, &schema, names, credit); released(src);
    }
    CHECK(prefix == metadata_cost(r, src));
    release(c); destroy(r); destroy(src);
}

static void
test_metadata_faults(void)
{
    col_rel_t *r = metadata_fixture(3, true, 0, false, false);
    col_rel_t *src = metadata_source(3, false);
    growth_snapshot_t before = snapshot(r);
    struct ArrowSchema schema = r->schema;
    char **names = r->col_names;
    uint64_t credit = r->metadata_reserved_bytes;
    uint64_t regions[2048], allocation_prefixes[32];
    size_t allocations;
    (void)metadata_regions(r, src, regions, allocation_prefixes, &allocations);
    CHECK(allocations == 13);
    wl_evaluation_control_t *c = control(0);
    for (size_t i = 0; i <= allocations; i++) {
        if (i) retry(c, 0);
        wl_columnar_relation_test_metadata_fail_allocation =
            i < allocations ? (uint32_t)i + 1 : 0;
        wl_columnar_relation_test_metadata_fail_commit = i == allocations;
        int expected = i < allocations ? ENOMEM : EFAULT;
        CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
            c) == expected);
        uint64_t prefix = i < allocations ? allocation_prefixes[i]
            : metadata_cost(r, src) - 4;
        CHECK(finish(c, expected) == prefix);
        CHECK(!wl_columnar_relation_test_metadata_fail_allocation
            && !wl_columnar_relation_test_metadata_fail_commit);
        CHECK(!r->memory_budget_denial_pending);
        metadata_unchanged(r, before, &schema, names, credit);
    }
    retry(c, metadata_cost(r, src));
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c) == 0);
    CHECK(finish(c, 0) == seen);
    release(c); destroy(r); destroy(src);
}

static void
test_metadata_governor(void)
{
    col_rel_t *r = metadata_fixture(3, true, 0, false, false);
    col_rel_t *src = metadata_source(3, false);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(r->memory_governor);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    CHECK(wl_columnar_memory_reserve(g, 41, &held));
    CHECK(wl_columnar_memory_commit(&held, &held));
    growth_snapshot_t before = snapshot(r);
    struct ArrowSchema schema = r->schema;
    char **names = r->col_names;
    uint64_t old = r->metadata_reserved_bytes;
    uint64_t scratch = 3 * sizeof(uint64_t);
    uint64_t next = old + 3 * sizeof(wirelog_column_type_t);
    wl_evaluation_control_t *c = control(0);
    for (unsigned mode = 0; mode < 3; mode++) {
        if (mode) retry(c, 0);
        atomic_store_explicit(&g->usable_bytes,
            before.reserved + scratch + next - 1, memory_order_relaxed);
        growth_verdict_phase = WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT;
        growth_verdict_hits = 0; growth_verdict_positive = mode == 2;
        wl_columnar_memory_governor_test_checkpoint =
            mode ? growth_verdict : NULL;
        int expected = mode == 2 ? EOVERFLOW : ENOSPC;
        CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
            c) == expected);
        wl_columnar_memory_governor_test_checkpoint = NULL;
        CHECK(finish(c, expected) == seen);
        CHECK(r->memory_budget_denial_pending == (mode != 2));
        CHECK(growth_verdict_hits == (mode ? 1u : 0u));
        metadata_unchanged(r, before, &schema, names, old);
    }
    retry(c, 0);
    atomic_store_explicit(&g->usable_bytes, before.reserved + scratch - 1,
        memory_order_relaxed);
    wl_columnar_relation_test_metadata_fail_allocation = 1;
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
        c) == ENOSPC);
    CHECK(finish(c, ENOSPC) == 2 + 3 + 6 + 14);
    CHECK(wl_columnar_relation_test_metadata_fail_allocation == 1);
    wl_columnar_relation_test_metadata_fail_allocation = 0;
    metadata_unchanged(r, before, &schema, names, old);
    atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
        memory_order_relaxed);
    const unsigned phases[] = {WL_COLUMNAR_MEMORY_GOVERNOR_VALIDATED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_CLAIMED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_SNAPSHOT,
                               WL_COLUMNAR_MEMORY_GOVERNOR_PREPARED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_ADMITTED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED,
                               WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                               WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF};
    for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
        retry(c, 0); growth_verdict_phase = phases[i];
        growth_verdict_hits = 0; growth_verdict_positive = true;
        wl_columnar_memory_governor_test_checkpoint = growth_verdict;
        CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
            c) == EOVERFLOW);
        wl_columnar_memory_governor_test_checkpoint = NULL;
        CHECK(finish(c, EOVERFLOW) == seen && growth_verdict_hits == 1);
        CHECK(!r->memory_budget_denial_pending);
        metadata_unchanged(r, before, &schema, names, old);
    }
    retry(c, metadata_cost(r, src));
    atomic_store_explicit(&g->usable_bytes, before.reserved + scratch + next,
        memory_order_relaxed);
    growth_watch = r; growth_peak = before.reserved;
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c) == 0);
    growth_watch = NULL;
    CHECK(finish(c,
        0) == seen && growth_peak == before.reserved + scratch + next);
    CHECK(wl_columnar_memory_reserved(g) == before.reserved + next - old);
    CHECK(held.bytes == 41 && held.identity == &held);
    CHECK(wl_columnar_memory_release(&held));
    release(c); destroy(r); destroy(src);
}

static void
test_metadata_peer_and_ledger(void)
{
    col_rel_t *r = metadata_fixture(3, true, 0, false, false);
    col_rel_t *src = metadata_source(3, false);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(r->memory_governor);
    uint64_t before = wl_columnar_memory_reserved(g);
    uint64_t cost = metadata_cost(r, src);
    wl_columnar_memory_reservation_init(&growth_peer);
    growth_peer_rounds = 0; growth_peer_base = before;
    growth_peer_cancel = false;
    wl_columnar_memory_governor_test_before_total_cas = growth_before_total;
    wl_columnar_memory_governor_test_after_total_cas = growth_after_total;
    wl_evaluation_control_t *c = control(cost + 3);
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c) == 0);
    CHECK(finish(c, 0) == cost + 3 && growth_peer_rounds == 3);
    CHECK(wl_columnar_memory_reserved(g) == before + growth_peer.bytes
        + 3 * sizeof(wirelog_column_type_t));
    CHECK(wl_columnar_memory_rollback(&growth_peer));
    release(c); destroy(r); destroy(src);
    for (unsigned success = 0; success < 2; success++) {
        r = metadata_fixture(3, true, 0, false, false);
        src = metadata_source(3, false);
        col_rel_t *peer_rel = col_rel_new_auto("peer", 1);
        CHECK(peer_rel != NULL);
        wl_mem_ledger_t ledger;
        wl_mem_ledger_init(&ledger, 0);
        r->mem_ledger = peer_rel->mem_ledger = &ledger;
        col_rel_ledger_reconcile(r, 0); col_rel_ledger_reconcile(peer_rel, 0);
        uint64_t ledger_before = atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed);
        growth_snapshot_t snap = snapshot(r);
        struct ArrowSchema schema = r->schema;
        char **names = r->col_names;
        uint64_t credit = r->metadata_reserved_bytes;
        cost = metadata_cost(r, src); c = control(cost);
        if (!success) cancel_at = cost;
        exact_ledger_peer = peer_rel; exact_ledger_changed = false;
        wl_columnar_relation_test_work_checkpoint = exact_shared_checkpoint;
        wl_columnar_memory_governor_test_work_checkpoint =
            exact_shared_checkpoint;
        int result = success ? 0 : WL_EVALUATION_CONTROL_CANCELLED;
        CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
            c) == result);
        CHECK(finish(c, result) == cost && exact_ledger_changed);
        wl_columnar_relation_test_work_checkpoint = checkpoint;
        wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
        exact_ledger_peer = NULL;
        CHECK(atomic_load_explicit(&ledger.current_bytes, memory_order_relaxed)
            == ledger_before + 5 * sizeof(int64_t));
        if (!success) metadata_unchanged(r, snap, &schema, names, credit);
        release(c); destroy(r); destroy(peer_rel); destroy(src);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == 0);
    }
}

static void
test_metadata_scope(void)
{
    col_rel_t *r = metadata_fixture(1, true, 0, false, false);
    col_rel_t *src = metadata_source(1, false);
    wl_evaluation_control_t *c = control(0);
    src->nrows = 0;
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    src->nrows = 2;
    uint32_t rows = r->nrows;
    r->nrows = r->capacity;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 2);
    r->nrows = rows;
    const void *identity = r->metadata_reservation.identity;
    r->metadata_reservation.identity = &src->metadata_reservation;
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
        c) == EINVAL);
    CHECK(finish(c, EINVAL) == 2 + 1 + 2);
    r->metadata_reservation.identity = identity;
    wirelog_column_type_t type = WIRELOG_TYPE_BOOL;
    CHECK(col_rel_set_column_types(src, &type, 1) == 0);
    retry(c, 0);
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL,
        c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) == 3);
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, NULL) == 0);
    CHECK(r->column_types[0] == type && r->nrows == 4);
    release(c); destroy(r); destroy(src);
}

static void
test_metadata_wide_table_and_advisory(void)
{
    col_rel_t *r = metadata_fixture(300, true, 0, false, false);
    col_rel_t *src = metadata_source(300, false);
    uint64_t regions[16384], allocations[1024];
    size_t allocation_count;
    (void)metadata_regions(r, src, regions, allocations, &allocation_count);
    CHECK(allocation_count == 904);
    growth_snapshot_t before = snapshot(r);
    struct ArrowSchema schema = r->schema;
    char **names = r->col_names;
    uint64_t credit = r->metadata_reserved_bytes;
    wl_evaluation_control_t *c = control(0);
    cancel_at = allocations[3] + WL_EVALUATION_CONTROL_QUANTUM;
    CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == cancel_at);
    metadata_unchanged(r, before, &schema, names, credit);
    release(c); destroy(r); destroy(src);
    for (unsigned unlimited = 0; unlimited < 2; unlimited++) {
        r = metadata_fixture(1, true, 2, false, false);
        src = metadata_source(1, false);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(r->memory_governor);
        g->mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY;
        atomic_store_explicit(&g->usable_bytes,
            unlimited ? UINT64_MAX : WL_COLUMNAR_MEMORY_MIN_BUDGET,
            memory_order_relaxed);
        uint64_t cost = metadata_cost(r, src);
        c = control(cost);
        CHECK(wl_columnar_relation_append_all_controlled(r, src, NULL, c) == 0);
        CHECK(finish(c, 0) == cost && r->nrows == 4);
        release(c); destroy(r); destroy(src);
    }
}

/* Literal source-region oracle, independent of production constants. */
static const uint64_t attempt_single_regions[] =
{94, 19, 39, 39, 45, 52, 45, 21, 144};
static const uint64_t attempt_single_prefix[] =
{94, 113, 152, 152, 191, 236, 236, 288, 333, 333, 354, 498};
static unsigned attempt_single_seams, attempt_single_at, attempt_single_fault;
static uint64_t attempt_single_old_consumed;
static wl_columnar_relation_mutation_single_t *attempt_single_frame;
static col_rel_t *attempt_single_relation;

static void
attempt_single_inject(wl_evaluation_control_t *c)
{
    ++attempt_single_seams;
    if (attempt_single_seams != attempt_single_at) return;
    if (attempt_single_fault == 1) wl_evaluation_control_request_cancel(c);
    else if (attempt_single_fault == 2)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else if (attempt_single_fault == 3) {
        attempt_single_old_consumed = finish(c, 0);
        retry(c, 0);
    }
}

static void
attempt_single_work(wl_evaluation_control_t *c, uint64_t work)
{
    CHECK(work <= WL_EVALUATION_CONTROL_QUANTUM);
    if (attempt_single_frame) {
        /* At every callback, live gates already have original tokens/counts. */
        if (wl_columnar_source_access_gate_busy(
                &attempt_single_relation->descriptor_access)) {
            CHECK(attempt_single_frame->set.descriptors_acquired == 1);
            CHECK(wl_columnar_source_access_writer_validate(
                    &attempt_single_frame->descriptor.writer,
                    &attempt_single_relation->descriptor_access) == 0);
        }
        if (wl_columnar_source_access_gate_busy(
                &attempt_single_relation->source_access)) {
            CHECK(attempt_single_frame->set.owners_acquired == 1);
            CHECK(wl_columnar_source_access_writer_validate(
                    &attempt_single_frame->owners[0].writer,
                    &attempt_single_relation->source_access) == 0);
        }
    }
    attempt_single_inject(c);
}

static void
attempt_single_nonce(wl_evaluation_control_t *c, uint64_t observed)
{
    CHECK(observed && observed != UINT64_MAX);
    attempt_single_inject(c);
}

static void
attempt_single_writer(wl_evaluation_control_t *c,
    wl_columnar_source_access_gate_t *gate)
{
    (void)gate;
    attempt_single_inject(c);
}

static void
attempt_single_hooks(unsigned at, unsigned fault)
{
    attempt_single_at = at; attempt_single_fault = fault;
    attempt_single_seams = 0; attempt_single_old_consumed = 0;
    wl_columnar_relation_test_work_checkpoint = attempt_single_work;
    wl_columnar_relation_test_before_nonce_cas = attempt_single_nonce;
    wl_columnar_relation_test_before_writer_cas = attempt_single_writer;
}

static void
attempt_single_hooks_clear(void)
{
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_relation_test_before_nonce_cas = NULL;
    wl_columnar_relation_test_before_writer_cas = NULL;
    attempt_single_frame = NULL; attempt_single_relation = NULL;
}

static int
attempt_single_acquire(col_rel_t *r,
    wl_columnar_relation_mutation_single_t *s, wl_evaluation_control_t *c)
{
    return wl_columnar_relation_mutation_single_acquire_attempt_controlled(
        r, s, c, wl_evaluation_control_active_attempt_id(c));
}

static void
test_attempt_single_boundaries(void)
{
    for (unsigned exact = 0; exact < 2; ++exact) {
        uint64_t prefix = 0;
        for (size_t i = 0; i < sizeof(attempt_single_regions)
            / sizeof(attempt_single_regions[0]); ++i) {
            uint64_t prior = prefix;
            prefix += attempt_single_regions[i];
            col_rel_t *r = relation(1, 4, 1, true, NULL);
            int64_t **columns = r->columns;
            col_delta_timestamp_t *timestamps = r->timestamps;
            wl_columnar_relation_mutation_single_t s = {0}, zero = {0};
            wl_evaluation_control_t *c = control(exact ? prefix : prefix - 1);
            int rc = attempt_single_acquire(r, &s, c);
            int expected = exact &&
                i == 8 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(rc == expected);
            if (!rc) {
                CHECK(col_rel_mutation_lease_validate(&s.lease, r) == 0);
                CHECK(wl_columnar_relation_mutation_single_finish_controlled(
                        &s, 0, c) == 0);
            }
            CHECK(finish(c, expected) == (exact ? prefix : prior));
            single_state_equal(&s, &zero); single_open(r, NULL);
            CHECK(r->columns == columns && r->timestamps == timestamps
                && r->nrows == 1 && r->columns[0][0] == 0);
            release(c); destroy(r);
        }
        CHECK(prefix == 498);
    }
    /* Exact admission produces real authority usable by the existing row API. */
    for (unsigned width = 0; width < 3; ++width) {
        col_rel_t *r = relation(width, 4, 1, false, NULL);
        col_rel_t *raw = relation(width, 4, 1, false, NULL);
        wl_columnar_relation_mutation_single_t s = {0}, twin = {0};
        wl_evaluation_control_t *c = control(498);
        CHECK(attempt_single_acquire(r, &s, c) == 0);
        CHECK(wl_columnar_relation_mutation_single_acquire_controlled(
                raw, &twin, NULL) == 0);
        int64_t row[2] = {7, 11};
        CHECK(col_rel_append_row_with_lease(r, row, &s.lease) == 0);
        CHECK(col_rel_append_row_with_lease(raw, row, &twin.lease) == 0);
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&s, 0,
            c) == 0);
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&twin, 0,
            NULL) == 0);
        CHECK(finish(c, 0) == 498 && r->nrows == raw->nrows);
        for (unsigned lane = 0; lane < width; ++lane)
            CHECK(r->columns[lane][1] == raw->columns[lane][1]);
        release(c); destroy(r); destroy(raw);
    }
}

static void
test_attempt_single_seams(void)
{
    for (unsigned fault = 1; fault <= 3; ++fault)
        for (unsigned at = 1; at <= 12; ++at) {
            col_rel_t *r = relation(1, 4, 1, true, NULL);
            wl_columnar_relation_mutation_single_t s = {0}, zero = {0};
            wl_evaluation_control_t *c = control(0);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            uint64_t held = wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(r->memory_governor));
            uint64_t prior_nonce =
                wl_columnar_relation_test_single_nonce_replace(60000);
            single_fail_slot = 99;
            memset(single_release_calls, 0, sizeof(single_release_calls));
            wl_columnar_relation_test_single_release = single_release_failure;
            attempt_single_frame = &s; attempt_single_relation = r;
            attempt_single_hooks(at, fault);
            int rc =
                wl_columnar_relation_mutation_single_acquire_attempt_controlled(
                r, &s, c, id);
            int expected = fault == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : fault == 2 ? EOVERFLOW : EINVAL;
            CHECK(rc == expected);
            /* Negative/accounting before-CAS hooks poll at the following work
            * tail; stale identity prevents the CAS and any following hook. */
            unsigned last = at + ((fault != 3 &&
                (at == 3 || at == 6 || at == 9)) ? 1u : 0u);
            CHECK(attempt_single_seams == last);
            CHECK(wl_columnar_relation_test_single_nonce_replace(prior_nonce)
                == (last >= 4 ? 60001u : 60000u));
            CHECK(single_release_calls[0] == (last >= 7 ? 1u : 0u));
            CHECK(single_release_calls[1] == (last >= 10 ? 1u : 0u));
            CHECK(single_release_calls[2] == 0);
            wl_columnar_relation_test_single_release = NULL;
            if (fault == 3) {
                CHECK(attempt_single_old_consumed ==
                    attempt_single_prefix[at - 1]);
                CHECK(finish(c, 0) == 0);
            } else CHECK(finish(c, expected) == attempt_single_prefix[at - 1]);
            single_state_equal(&s, &zero); single_open(r, NULL);
            CHECK(r->nrows == 1 && r->columns[0][0] == 0);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(r->memory_governor)) ==
                held);
            attempt_single_hooks_clear(); release(c); destroy(r);
        }
}

static void
test_attempt_single_entry(void)
{
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    for (unsigned mode = 0; mode < 5; ++mode) {
        wl_columnar_relation_mutation_single_t s = {0}, zero = {0};
        wl_evaluation_control_t *c = control(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        if (mode == 4) {
            CHECK(finish(c, 0) == 0); retry(c, 0);
        }
        CHECK(wl_columnar_relation_mutation_single_acquire_attempt_controlled(
                r, mode == 3 ? NULL : &s, mode == 0 ? NULL : c,
                mode == 1 ? 0 : mode == 2 ? id + 1 : id) == EINVAL);
        CHECK(finish(c, 0) == 0); single_state_equal(&s, &zero);
        release(c);
    }
    for (unsigned mode = 0; mode < 8; ++mode) {
        wl_columnar_relation_mutation_single_t dirty = {0};
        switch (mode) {
        case 0: dirty.role.relation = r; break;
        case 1: dirty.set.terminal_sequence_consumed = true; break;
        case 2: dirty.set.published = true; break;
        case 3: dirty.owners[0].writer.identity = 7; break;
        case 4: dirty.owners[1].writer.thread_valid = true; break;
        case 5: dirty.lease.storage_transitioned = true; break;
        case 6: dirty.initialization.borrows = 1; break;
        default: dirty.lease.detached = true; break;
        }
        wl_columnar_relation_mutation_single_t before = dirty;
        wl_evaluation_control_t *c = control(0);
        CHECK(attempt_single_acquire(r, &dirty, c) == EINVAL);
        CHECK(finish(c, EINVAL) == 94);
        single_state_equal(&dirty, &before); single_open(r, NULL); release(c);
    }
    destroy(r);
}

static void
test_attempt_single_domains_and_busy(void)
{
    for (unsigned mode = 1; mode < 4; ++mode) {
        col_rel_t *root, *r = single_fixture(mode, &root);
        wl_columnar_relation_mutation_single_t s = {0}, zero = {0};
        wl_evaluation_control_t *c = control(0);
        CHECK(attempt_single_acquire(r, &s, c) == ENOTSUP);
        CHECK(finish(c, ENOTSUP) == 288);
        single_state_equal(&s, &zero); single_open(r, root);
        release(c); single_dispose(r, root);
    }
    for (unsigned shape = 0; shape < 5; ++shape) {
        col_rel_t *r = relation(1, 4, 1, false, NULL);
        wl_columnar_relation_mutation_single_t frame = {0}, zero = {0};
        /* Quiescent shape fixture; restore ownership metadata before destroy. */
        if (shape == 0) r->pool_owned = true;
        if (shape == 1) r->arena_owned = true;
        if (shape == 2) r->compound_kind = WIRELOG_COMPOUND_KIND_SIDE;
        if (shape == 3) r->storage_owner_identity = 0;
        if (shape == 4) r->storage_owner_generation = 0;
        wl_evaluation_control_t *c = control(0);
        int expected = shape < 3 ? ENOTSUP : EINVAL;
        CHECK(attempt_single_acquire(r, &frame, c) == expected);
        CHECK(finish(c, expected) == 288);
        single_state_equal(&frame, &zero); single_open(r, NULL);
        r->pool_owned = false; r->arena_owned = false;
        r->compound_kind = WIRELOG_COMPOUND_KIND_NONE;
        r->storage_owner_identity = r->relation_identity;
        r->storage_owner_generation = r->storage_generation;
        release(c); destroy(r);
    }
    /* A genuine live shared view makes the canonical owner's payload busy. */
    {
        col_rel_t *root, *alias = single_fixture(2, &root);
        wl_columnar_relation_mutation_single_t frame = {0}, zero = {0};
        wl_evaluation_control_t *c = control(0);
        CHECK(attempt_single_acquire(root, &frame, c) == EBUSY);
        CHECK(finish(c, EBUSY) == 354);
        single_state_equal(&frame, &zero); single_open(alias, root);
        release(c); single_dispose(alias, root);
    }
    for (unsigned slot = 0; slot < 2; ++slot)
        for (unsigned fault = 0; fault < 4; ++fault) {
            col_rel_t *r = relation(1, 4, 1, false, NULL);
            wl_columnar_source_access_reader_t peer = {0};
            wl_columnar_source_access_gate_t *gate =
                slot ? &r->source_access : &r->descriptor_access;
            CHECK(wl_columnar_source_access_reader_acquire(gate, &peer) == 0);
            wl_columnar_relation_mutation_single_t s = {0}, zero = {0};
            wl_evaluation_control_t *c = control(0);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            attempt_single_hooks(slot ? 9 : 6, fault);
            int rc =
                wl_columnar_relation_mutation_single_acquire_attempt_controlled(
                r, &s, c, id);
            int expected = fault < 2 ? EBUSY : fault == 2 ? EOVERFLOW : EINVAL;
            CHECK(rc == expected);
            single_state_equal(&s, &zero);
            CHECK(peer.owner == gate && peer.identity == (uintptr_t)&peer
                && wl_columnar_source_access_reader_thread_equal(&peer));
            CHECK(wl_columnar_source_access_reader_release(&peer) == 0);
            if (fault == 3) {
                CHECK(attempt_single_old_consumed == (slot ? 333u : 236u));
                CHECK(finish(c, 0) == 0);
            } else CHECK(finish(c, expected) == (slot ? 333u : 236u));
            attempt_single_hooks_clear(); single_open(r, NULL); release(c);
            destroy(r);
        }
}

static void
test_attempt_single_nonce_and_cleanup(void)
{
    col_rel_t *r = relation(1, 4, 1, false, NULL);
    single_peer = relation(1, 4, 1, false, NULL);
    for (unsigned failures = 1; failures <= 4; ++failures)
        for (unsigned denied = 0; denied < 2; ++denied) {
            wl_columnar_relation_mutation_single_t s = {0}, zero = {0};
            uint64_t limit = denied ? 113 + 39u * failures : 498 + 39u *
                failures;
            wl_evaluation_control_t *c = control(limit);
            single_nonce_retries = failures;
            wl_columnar_relation_test_before_nonce_cas = single_nonce_interfere;
            int expected = denied ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
            CHECK(attempt_single_acquire(r, &s, c) == expected);
            CHECK(!single_nonce_retries);
            wl_columnar_relation_test_before_nonce_cas = NULL;
            if (!denied) CHECK(
                    wl_columnar_relation_mutation_single_finish_controlled(&s,
                    0, c) == 0);
            CHECK(finish(c, expected) == limit);
            single_state_equal(&s, &zero); single_open(r, NULL); release(c);
        }
    for (unsigned zero = 0; zero < 2; ++zero) {
        uint64_t terminal = zero ? 0 : UINT64_MAX;
        uint64_t saved =
            wl_columnar_relation_test_single_nonce_replace(terminal);
        wl_columnar_relation_mutation_single_t s = {0}, empty = {0};
        wl_evaluation_control_t *c = control(0);
        CHECK(attempt_single_acquire(r, &s, c) == EOVERFLOW);
        CHECK(finish(c, EOVERFLOW) == 152);
        CHECK(wl_columnar_relation_test_single_nonce_replace(saved) ==
            terminal);
        single_state_equal(&s, &empty); release(c);
    }
    /* Stopping after a successful nonce CAS never rewinds global identity. */
    uint64_t saved = wl_columnar_relation_test_single_nonce_replace(50000);
    wl_columnar_relation_mutation_single_t s = {0};
    wl_evaluation_control_t *c = control(152);
    CHECK(attempt_single_acquire(r, &s, c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 152);
    CHECK(wl_columnar_relation_test_single_nonce_replace(saved) == 50001);
    release(c);
    for (unsigned slot = 0; slot < 2; ++slot) {
        c = control(0);
        attempt_single_hooks(12, 1);
        single_fail_slot = slot;
        wl_columnar_relation_test_single_release = single_release_failure;
        CHECK(attempt_single_acquire(r, &s, c) == EIO);
        CHECK(s.set.descriptors_acquired == 1 && s.set.owners_acquired == slot);
        CHECK(s.descriptor.writer.owner == &r->descriptor_access);
        CHECK(finish(c, EIO) == 498);
        wl_columnar_relation_test_single_release = NULL;
        CHECK(wl_columnar_relation_mutation_single_finish_controlled(&s,
            WL_EVALUATION_CONTROL_CANCELLED,
            c) == WL_EVALUATION_CONTROL_CANCELLED);
        attempt_single_hooks_clear(); single_open(r, NULL); release(c);
    }
    destroy(single_peer); single_peer = NULL; destroy(r);
}

/* B1 exercises the actual append operation, including original single,
 * governor, ledger, row staging and retirement in one call. */
static void
row_attach_ledger(col_rel_t *r, wl_mem_ledger_t *ledger)
{
    wl_mem_ledger_init(ledger, 0);
    wl_mem_ledger_alloc(ledger, WL_MEM_SUBSYS_ARENA, 37);
    r->mem_ledger = ledger;
    col_rel_ledger_reconcile(r, 0);
}

static void
row_ledger_equal(wl_mem_ledger_t *a, wl_mem_ledger_t *b)
{
    wl_mem_ledger_snapshot_t x, y;
    wl_mem_ledger_snapshot(a, &x);
    wl_mem_ledger_snapshot(b, &y);
    CHECK(x.current_bytes == y.current_bytes);
    CHECK(x.peak_bytes == y.peak_bytes);
    for (unsigned i = 0; i < WL_MEM_SUBSYS_COUNT; ++i)
        CHECK(x.subsys_bytes[i] == y.subsys_bytes[i]);
}

static void
row_twins_equal(col_rel_t *r, col_rel_t *raw)
{
    CHECK(r->nrows == raw->nrows && r->capacity == raw->capacity);
    CHECK(r->timestamp_capacity == raw->timestamp_capacity);
    CHECK(r->view_generation == raw->view_generation);
    CHECK(r->storage_generation == raw->storage_generation);
    CHECK(r->storage_owner_generation == r->storage_generation);
    CHECK(r->ledger_ts_bytes == raw->ledger_ts_bytes);
    payload_equal(r, raw, 0);
    if (r->timestamps)
        CHECK(memcmp(r->timestamps, raw->timestamps,
            (size_t)r->nrows * sizeof(*r->timestamps)) == 0);
    released(r);
}

static void
test_row_twins(void)
{
    const uint32_t widths[] = { 0, 1, 16, 17 };
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned ts = 0; ts < 2; ++ts)
            for (unsigned tracked = 0; tracked < 2; ++tracked) {
                col_rel_t *r = relation(widths[w], 2, 1, ts != 0, NULL);
                col_rel_t *raw = relation(widths[w], 2, 1, ts != 0, NULL);
                wl_mem_ledger_t a, b;
                if (tracked) {
                    row_attach_ledger(r, &a); row_attach_ledger(raw, &b);
                }
                int64_t values[17];
                for (unsigned c = 0; c < 17; ++c) values[c] = c + 90;
                wl_evaluation_control_t *c = control(0);
                for (unsigned i = 0; i < 9; ++i) {
                    CHECK(wl_columnar_relation_append_row_controlled(r,
                        values, c) == 0);
                    CHECK(col_rel_append_row(raw, values) == 0);
                    row_twins_equal(r, raw);
                    if (tracked) row_ledger_equal(&a, &b);
                }
                CHECK(finish(c, 0) > 0); release(c);
                if (tracked) {
                    uint64_t current = atomic_load_explicit(&a.current_bytes,
                            memory_order_relaxed);
                    col_rel_ledger_reconcile(r, col_rel_owned_ledger_bytes(r));
                    CHECK(atomic_load_explicit(&a.current_bytes,
                        memory_order_relaxed) == current);
                }
                destroy(r); destroy(raw);
                if (tracked) {
                    CHECK(atomic_load_explicit(&a.current_bytes,
                        memory_order_relaxed) == 37);
                    row_ledger_equal(&a, &b);
                }
            }
    /* Actual ungoverned enable_timestamps leaves deferred ledger tracking.
     * Exercise nongrowth then two genuine capacity transitions and destroy. */
    col_rel_t *r = col_rel_new_auto("drift", 1);
    col_rel_t *raw = col_rel_new_auto("drift", 1);
    CHECK(r && raw);
    wl_mem_ledger_t a, b;
    row_attach_ledger(r, &a); row_attach_ledger(raw, &b);
    CHECK(col_rel_enable_timestamps(r) == 0);
    CHECK(col_rel_enable_timestamps(raw) == 0);
    CHECK(r->timestamps && r->ledger_ts_bytes == 0);
    uint32_t original_capacity = r->capacity;
    int64_t value = 18;
    wl_evaluation_control_t *c = control(0);
    for (uint32_t i = 0; i <= original_capacity * 2u; ++i) {
        CHECK(wl_columnar_relation_append_row_controlled(r, &value, c) == 0);
        CHECK(col_rel_append_row(raw, &value) == 0);
        row_twins_equal(r, raw); row_ledger_equal(&a, &b);
        if (i < original_capacity) CHECK(r->ledger_ts_bytes == 0);
    }
    uint64_t current = atomic_load_explicit(&a.current_bytes,
            memory_order_relaxed);
    col_rel_ledger_reconcile(r, col_rel_owned_ledger_bytes(r));
    CHECK(atomic_load_explicit(&a.current_bytes,
        memory_order_relaxed) == current);
    CHECK(finish(c, 0) > 0); release(c); destroy(r); destroy(raw);
    CHECK(atomic_load_explicit(&a.current_bytes, memory_order_relaxed) == 37);
    row_ledger_equal(&a, &b);
}

static unsigned row_seams, row_fault_at, row_fault_kind;
static wl_mem_ledger_t *row_watched_ledger;
static uint64_t row_old_consumed;
static void
row_inject(wl_evaluation_control_t *c)
{
    ++row_seams;
    if (row_seams != row_fault_at) return;
    if (row_fault_kind == 1) wl_evaluation_control_request_cancel(c);
    if (row_fault_kind == 2)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    if (row_fault_kind == 3) {
        row_old_consumed = finish(c, 0);
        retry(c, 0);
    }
}

static void
row_work(wl_evaluation_control_t *c, uint64_t work)
{
    if (row_watched_ledger)
        CHECK(atomic_load_explicit(&row_watched_ledger->writer_gate,
            memory_order_relaxed) == 0);
    checkpoint(c, work);
    row_inject(c);
}

static void
row_governor(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token; (void)phase;
    row_inject(c);
}

static void
row_ledger(wl_evaluation_control_t *c,
    wl_columnar_mem_ledger_publication_prepared_t *prepared, unsigned phase)
{
    (void)phase;
    if (prepared->ledger) row_watched_ledger = prepared->ledger;
    if (row_watched_ledger)
        CHECK(atomic_load_explicit(&row_watched_ledger->writer_gate,
            memory_order_relaxed) == 0);
    row_inject(c);
}

static void
row_nonce(wl_evaluation_control_t *c, uint64_t observed)
{
    CHECK(observed != 0); row_inject(c);
}

static void
row_writer(wl_evaluation_control_t *c, wl_columnar_source_access_gate_t *gate)
{
    CHECK(gate != NULL); row_inject(c);
}

static void
row_total_before(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *governor, uint64_t old, bool overflow)
{
    (void)old; (void)overflow;
    CHECK(governor != NULL);
    row_inject(c);
}

static void
row_total_after(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *governor, bool admitted, bool overflow)
{
    (void)admitted; (void)overflow;
    CHECK(governor != NULL);
    row_inject(c);
}

static void
row_hooks(unsigned at, unsigned kind)
{
    row_seams = 0; row_fault_at = at; row_fault_kind = kind;
    row_old_consumed = 0;
    row_watched_ledger = NULL;
    wl_columnar_memory_governor_test_before_total_cas = row_total_before;
    wl_columnar_memory_governor_test_after_total_cas = row_total_after;
    wl_columnar_relation_test_work_checkpoint = row_work;
    wl_columnar_memory_governor_test_work_checkpoint = row_work;
    wl_columnar_memory_governor_test_checkpoint = row_governor;
    wl_columnar_mem_ledger_publication_test_checkpoint = row_ledger;
    wl_columnar_relation_test_before_nonce_cas = row_nonce;
    wl_columnar_relation_test_before_writer_cas = row_writer;
}

static void
row_hooks_clear(void)
{
    row_watched_ledger = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    wl_columnar_relation_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_mem_ledger_publication_test_checkpoint = NULL;
    wl_columnar_relation_test_before_nonce_cas = NULL;
    wl_columnar_relation_test_before_writer_cas = NULL;
}

static void
test_row_stops(void)
{
    int64_t values[17];
    for (unsigned i = 0; i < 17; ++i) values[i] = 90 + i;
    col_rel_t *r = relation(17, 2, 2, true, NULL);
    wl_mem_ledger_t ledger;
    row_attach_ledger(r, &ledger);
    wl_evaluation_control_t *c = control(0);
    row_hooks(0, 0);
    CHECK(wl_columnar_relation_append_row_controlled(r, values, c) == 0);
    unsigned seams = row_seams;
    uint64_t cost = finish(c, 0);
    CHECK(seams > 50 && cost > 498);
    row_hooks_clear(); release(c); destroy(r);
    /* Every actual nested seam is exercised independently. A restarted
     * successor must retain zero consumed work despite original cleanup. */
    for (unsigned kind = 1; kind <= 3; ++kind)
        for (unsigned seam = 1; seam <= seams; ++seam) {
            r = relation(17, 2, 2, true, NULL);
            row_attach_ledger(r, &ledger);
            growth_snapshot_t before = snapshot(r);
            uint64_t ledger_before = atomic_load_explicit(&ledger.current_bytes,
                    memory_order_relaxed);
            c = control(0); row_hooks(seam, kind);
            int expected = kind == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : kind == 2 ? EOVERFLOW : EINVAL;
            CHECK(wl_columnar_relation_append_row_controlled(r, values, c)
                == expected);
            CHECK(row_seams >= seam &&
                row_seams <= seam + (kind == 3 ? 0u : 2u));
            row_hooks_clear();
            unchanged(r, before); released(r);
            CHECK(atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed) == ledger_before);
            if (kind == 3) {
                CHECK(row_old_consumed > 0);
                CHECK(finish(c, 0) == 0);
            } else CHECK(finish(c, expected) > 0);
            release(c); destroy(r);
        }
    for (unsigned exact = 0; exact < 2; ++exact) {
        r = relation(17, 2, 2, true, NULL);
        row_attach_ledger(r, &ledger);
        growth_snapshot_t before = snapshot(r);
        c = control(cost - (exact ? 0 : 1));
        int expected = exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
        CHECK(wl_columnar_relation_append_row_controlled(r, values,
            c) == expected);
        uint64_t used = finish(c, expected);
        if (exact) CHECK(used == cost);
        else {
            CHECK(used < cost); unchanged(r, before);
        }
        release(c); destroy(r);
    }
}

/* B2: actual held row/set/end and storage renewal, independent raw twins. */
static void
test_controlled_batch_twins(void)
{
    const uint32_t widths[] = {0, 1, 16, 17};
    int64_t values[17];
    for (unsigned k = 0; k < 17; ++k) values[k] = 900 + k;
    for (unsigned mode = 0; mode < 2; ++mode)
        for (unsigned w = 0; w < 4; ++w)
            for (unsigned ts = 0; ts < 2; ++ts) {
                col_rel_t *r = relation(widths[w], 2, 1, ts != 0, NULL);
                col_rel_t *raw = relation(widths[w], 2, 1, ts != 0, NULL);
                wl_mem_ledger_t a, b;
                row_attach_ledger(r, &a); row_attach_ledger(raw, &b);
                wl_evaluation_control_t *c = control(0);
                wl_columnar_relation_append_batch_t batch = {0};
                col_rel_append_batch_t ref = {0};
                CHECK(wl_columnar_relation_append_batch_begin_controlled(
                        &batch, r, mode ? NULL : c) == 0);
                CHECK(col_rel_append_batch_begin(&ref, raw) == 0);
                uintptr_t original = (uintptr_t)&batch.raw.single;
                uint64_t nonce = batch.raw.single.set.acquisition_nonce;
                unsigned renewals = 0;
                for (unsigned i = 0; i < 20; ++i) {
                    CHECK(wl_columnar_relation_append_batch_row_controlled(
                            &batch, values) == 0);
                    CHECK(col_rel_append_batch_row(&ref, values) == 0);
                    CHECK((uintptr_t)&batch.raw.single == original);
                    if (batch.raw.single.set.acquisition_nonce != nonce) {
                        ++renewals;
                        nonce = batch.raw.single.set.acquisition_nonce;
                    }
                    if (widths[w]) {
                        CHECK(wl_columnar_relation_append_batch_set_controlled(
                                &batch, r->nrows - 1, 0, (int64_t)i) == 0);
                        CHECK(col_rel_append_batch_set(&ref,
                            raw->nrows - 1, 0, (int64_t)i) == 0);
                    }
                    CHECK(r->nrows == raw->nrows &&
                        r->capacity == raw->capacity);
                    CHECK(r->view_generation == raw->view_generation);
                    CHECK(r->storage_generation == raw->storage_generation);
                    payload_equal(r, raw, 0);
                    row_ledger_equal(&a, &b);
                }
                CHECK(renewals >= 3);
                CHECK(wl_columnar_relation_append_batch_end_controlled(
                        &batch, WL_EVALUATION_CONTROL_CANCELLED)
                    == WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(col_rel_append_batch_end(&ref, true) == 0);
                row_twins_equal(r, raw);
                CHECK(wl_columnar_relation_append_batch_end_controlled(
                        &batch, EIO) == WL_EVALUATION_CONTROL_CANCELLED);
                CHECK(wl_columnar_relation_append_batch_row_controlled(
                        &batch, values) == EINVAL);
                CHECK(wl_columnar_relation_append_batch_begin_controlled(
                        &batch, r, c) == EINVAL);
                uint64_t used = finish(c, 0);
                CHECK(mode ? used == 0 : used > 0); /* Exact source oracles are separately literal. */
                release(c); destroy(r); destroy(raw);
                CHECK(atomic_load_explicit(&a.current_bytes,
                    memory_order_relaxed) == 37);
                row_ledger_equal(&a, &b);
            }
}

static col_rel_t *batch_gap_candidate;
static unsigned batch_gap_calls;
static uintptr_t batch_gap_old_columns, batch_gap_old_scratch;
static void
batch_replace_in_gap(wl_columnar_relation_append_batch_t *batch)
{
    col_rel_t *r = batch->raw.relation;
    CHECK(batch->raw.single.set.identity == 0);
    CHECK(atomic_load_explicit(&r->descriptor_access.state,
        memory_order_relaxed) == 0);
    CHECK(atomic_load_explicit(&r->source_access.state,
        memory_order_relaxed) == 0);
    if (r->ncols > 16)
        CHECK(!wl_columnar_memory_governor_ref_is_sole(r->memory_governor));
    col_rel_replacement_t replacement = {0};
    CHECK(col_rel_prepare_replacement(r, batch_gap_candidate,
        &replacement) == 0);
    col_rel_commit_replacement_locked(r, &replacement);
    col_rel_discard_replacement(&replacement);
    CHECK((uintptr_t)r->columns != batch_gap_old_columns);
    CHECK(!batch_gap_old_scratch
        || (uintptr_t)r->row_scratch != batch_gap_old_scratch);
    ++batch_gap_calls;
}

static void
test_controlled_batch_gap_capture(void)
{
    for (unsigned wide = 0; wide < 2; ++wide)
        for (unsigned shape = 0; shape < 3; ++shape) {
            uint32_t width = wide ? 17 : 1;
            col_rel_t *r = relation(width, 2, 2, true, NULL);
            int64_t values[17], expected[17];
            for (unsigned k = 0; k < 17; ++k) values[k] = 300 + k;
            if (wide) CHECK(col_rel_row(r, 0) != NULL);
            wl_mem_ledger_t ledger; row_attach_ledger(r, &ledger);
            wl_evaluation_control_t *c = control(0);
            wl_columnar_relation_append_batch_t batch = {0};
            CHECK(wl_columnar_relation_append_batch_begin_controlled(
                    &batch, r, c) == 0);
            CHECK(wl_columnar_relation_append_batch_row_controlled(
                    &batch, values) == 0);
            CHECK(wl_columnar_relation_append_batch_row_controlled(
                    &batch, values) == 0);
            CHECK(r->nrows == r->capacity);
            const int64_t *input = wide ? r->row_scratch : r->columns[0];
            for (uint32_t k = 0; k < width; ++k) expected[k] = input[k];
            uint32_t new_width = shape == 0 ? width
                : shape == 1 ? width + 1 : width - 1;
            batch_gap_candidate = relation(new_width, 8, 1, true, NULL);
            batch_gap_old_columns = (uintptr_t)r->columns;
            batch_gap_old_scratch = (uintptr_t)r->row_scratch;
            batch_gap_calls = 0;
            wl_columnar_relation_test_batch_renewed = batch_replace_in_gap;
            CHECK(wl_columnar_relation_append_batch_row_controlled(
                    &batch, input) == (shape ? ENOTSUP : 0));
            wl_columnar_relation_test_batch_renewed = NULL;
            CHECK(batch_gap_calls == 1);
            CHECK(wl_columnar_memory_governor_ref_is_sole(r->memory_governor));
            CHECK(r->ncols == new_width && r->nrows == (shape ? 1u : 2u));
            if (!shape)
                for (uint32_t k = 0; k < width; ++k)
                    CHECK(r->columns[k][1] == expected[k]);
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
                shape ? ENOTSUP : 0) == (shape ? ENOTSUP : 0));
            CHECK(finish(c, shape ? ENOTSUP : 0) > 0);
            release(c); destroy(batch_gap_candidate);
            batch_gap_candidate = NULL;
            destroy(r);
            CHECK(atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed) == 37);
        }
}

static void
test_controlled_batch_end_authority(void)
{
    col_rel_t *r = relation(1, 2, 1, false, NULL);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_append_batch_t batch = {0};
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    wl_columnar_relation_append_batch_t copy = batch;
    int64_t value = 55;
    CHECK(wl_columnar_relation_append_batch_row_controlled(&copy,
        &value) == EINVAL);
    CHECK(wl_columnar_relation_append_batch_set_controlled(&copy, 0, 0,
        6) == EINVAL);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&copy,
        EIO) == EINVAL);
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
        &value) == 0);
    CHECK(finish(c, 0) > 0); retry(c, 0);
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
        &value) == EINVAL);
    single_fail_slot = 0;
    wl_columnar_relation_test_single_release = single_release_failure;
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
        WL_EVALUATION_CONTROL_CANCELLED) == EIO);
    CHECK(batch.phase == WL_COLUMNAR_RELATION_BATCH_DRAINING);
    CHECK(batch.raw.single.set.descriptors_acquired == 1);
    CHECK(batch.raw.single.set.owners_acquired == 0);
    wl_columnar_relation_test_single_release = NULL;
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, EINVAL)
        == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(finish(c, 0) == 0); release(c); released(r); destroy(r);
}

static void
batch_gap_fault(wl_columnar_relation_append_batch_t *batch)
{
    CHECK(batch->raw.single.set.identity == 0);
    CHECK(atomic_load_explicit(&batch->raw.relation->descriptor_access.state,
        memory_order_relaxed) == 0);
    CHECK(atomic_load_explicit(&batch->raw.relation->source_access.state,
        memory_order_relaxed) == 0);
    row_inject(batch->control);
}

static col_rel_t *
batch_at_second_growth(wl_columnar_relation_append_batch_t *batch,
    wl_evaluation_control_t *c, const int64_t *values, wl_mem_ledger_t *ledger)
{
    col_rel_t *r = relation(17, 2, 2, true, NULL);
    row_attach_ledger(r, ledger);
    CHECK(wl_columnar_relation_append_batch_begin_controlled(batch, r, c) == 0);
    CHECK(wl_columnar_relation_append_batch_row_controlled(batch, values) == 0);
    CHECK(wl_columnar_relation_append_batch_row_controlled(batch, values) == 0);
    CHECK(r->nrows == 4 && r->capacity == 4);
    CHECK(batch->raw.single.lease.storage_transitioned);
    return r;
}

static void
test_controlled_batch_seams(void)
{
    int64_t values[17];
    for (unsigned k = 0; k < 17; ++k) values[k] = 500 + k;
    wl_mem_ledger_t ledger;
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_append_batch_t batch = {0};
    col_rel_t *r = batch_at_second_growth(&batch, c, values, &ledger);
    row_hooks(0, 0);
    wl_columnar_relation_test_batch_renewed = batch_gap_fault;
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
        values) == 0);
    unsigned seams = row_seams;
    CHECK(seams > 60);
    row_hooks_clear(); wl_columnar_relation_test_batch_renewed = NULL;
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, 0) == 0);
    CHECK(finish(c, 0) > 0); release(c); destroy(r);
    for (unsigned kind = 1; kind <= 3; ++kind)
        for (unsigned seam = 1; seam <= seams; ++seam) {
            c = control(0); batch = (wl_columnar_relation_append_batch_t){0};
            r = batch_at_second_growth(&batch, c, values, &ledger);
            growth_snapshot_t before = snapshot(r);
            uint64_t ledger_before = atomic_load_explicit(&ledger.current_bytes,
                    memory_order_relaxed);
            int64_t old_values[17][4];
            col_delta_timestamp_t old_ts[4];
            for (unsigned k = 0; k < 17; ++k)
                memcpy(old_values[k], r->columns[k], sizeof(old_values[k]));
            memcpy(old_ts, r->timestamps, sizeof(old_ts));
            row_hooks(seam, kind);
            wl_columnar_relation_test_batch_renewed = batch_gap_fault;
            int expected = kind == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : kind == 2 ? EOVERFLOW : EINVAL;
            CHECK(wl_columnar_relation_append_batch_row_controlled(
                    &batch, values) == expected);
            CHECK(row_seams >= seam &&
                row_seams <= seam + (kind == 3 ? 0u : 2u));
            row_hooks_clear(); wl_columnar_relation_test_batch_renewed = NULL;
            growth_snapshot_t after = snapshot(r);
            CHECK(after.columns == before.columns &&
                after.timestamps == before.timestamps);
            CHECK(after.capacity == before.capacity &&
                after.rows == before.rows);
            CHECK(after.view == before.view && after.storage == before.storage);
            CHECK(after.reserved == before.reserved &&
                after.credit == before.credit);
            CHECK(after.token_identity == before.token_identity
                && after.token_owner == before.token_owner
                && after.token_state == before.token_state
                && after.token_bytes == before.token_bytes
                && after.replacement == before.replacement);
            for (unsigned k = 0; k < 17; ++k)
                CHECK(memcmp(old_values[k], r->columns[k],
                    sizeof(old_values[k])) == 0);
            CHECK(memcmp(old_ts, r->timestamps, sizeof(old_ts)) == 0);
            CHECK(atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed) == ledger_before);
            CHECK(wl_columnar_relation_append_batch_end_controlled(
                    &batch, expected) == expected);
            released(r);
            uint64_t used = finish(c, kind == 3 ? 0 : expected);
            CHECK(kind == 3 ? used == 0 : used > 0);
            release(c); destroy(r);
            CHECK(atomic_load_explicit(&ledger.current_bytes,
                memory_order_relaxed) == 37);
        }
}

static wl_evaluation_control_t *batch_release_control;
static unsigned batch_release_restart_calls;
static int
batch_release_restart(wl_columnar_relation_mutation_single_t *single,
    unsigned slot)
{
    (void)single;
    ++batch_release_restart_calls;
    if (slot == 1) {
        CHECK(finish(batch_release_control, 0) > 0);
        retry(batch_release_control, 0);
    }
    return 0;
}

static void *
batch_wrong_thread(void *arg)
{
    wl_columnar_relation_append_batch_t *batch = arg;
    int64_t value = 123;
    CHECK(wl_columnar_relation_append_batch_row_controlled(batch,
        &value) == EINVAL);
    CHECK(wl_columnar_relation_append_batch_set_controlled(batch, 0, 0,
        value) == EINVAL);
    CHECK(wl_columnar_relation_append_batch_end_controlled(batch,
        EIO) == EINVAL);
    return NULL;
}

static void
test_controlled_batch_renew_authority(void)
{
    int64_t values[17];
    for (unsigned k = 0; k < 17; ++k) values[k] = 600 + k;
    for (unsigned slot = 0; slot < 2; ++slot) {
        wl_mem_ledger_t ledger;
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_append_batch_t batch = {0};
        col_rel_t *r = batch_at_second_growth(&batch, c, values, &ledger);
        uint64_t nonce = batch.raw.single.set.acquisition_nonce;
        uint64_t reserved = snapshot(r).reserved;
        single_fail_slot = slot;
        wl_columnar_relation_test_single_release = single_release_failure;
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            values) == EIO);
        CHECK(batch.phase == WL_COLUMNAR_RELATION_BATCH_DRAINING);
        CHECK(batch.raw.single.set.acquisition_nonce == nonce);
        CHECK(batch.raw.single.set.descriptors_acquired == 1);
        CHECK(batch.raw.single.set.owners_acquired == slot);
        CHECK(snapshot(r).reserved == reserved);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
            WL_EVALUATION_CONTROL_CANCELLED) == EIO);
        wl_columnar_relation_test_single_release = NULL;
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, ENOTSUP)
            == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(r->nrows == 4 && r->capacity == 4);
        CHECK(finish(c, 0) > 0); release(c); released(r); destroy(r);
        CHECK(atomic_load_explicit(&ledger.current_bytes,
            memory_order_relaxed) == 37);
    }
    wl_mem_ledger_t ledger;
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_append_batch_t batch = {0};
    col_rel_t *r = batch_at_second_growth(&batch, c, values, &ledger);
    uint64_t nonce = batch.raw.single.set.acquisition_nonce;
    uint64_t next = wl_columnar_relation_test_single_nonce_replace(900000);
    batch_release_control = c; batch_release_restart_calls = 0;
    wl_columnar_relation_test_single_release = batch_release_restart;
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
        values) == EINVAL);
    wl_columnar_relation_test_single_release = NULL;
    CHECK(batch_release_restart_calls == 2);
    CHECK(batch.phase == WL_COLUMNAR_RELATION_BATCH_ENDED);
    CHECK(batch.raw.single.set.acquisition_nonce == 0);
    CHECK(nonce != 0);
    CHECK(wl_columnar_relation_test_single_nonce_replace(next) == 900000);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
        EINVAL) == EINVAL);
    CHECK(finish(c, 0) == 0); release(c); released(r); destroy(r);
    batch_release_control = NULL;

    c = control(0); batch = (wl_columnar_relation_append_batch_t){0};
    r = relation(1, 2, 1, false, NULL);
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, batch_wrong_thread, &batch) == 0);
    CHECK(wl_thread_join(&thread) == 0);
    CHECK(batch.phase == WL_COLUMNAR_RELATION_BATCH_HELD);
    CHECK(!batch.result_saved);
    CHECK(batch.raw.single.set.descriptors_acquired == 1);
    CHECK(batch.raw.single.set.owners_acquired == 1);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, 0) == 0);
    CHECK(finish(c, 0) > 0); release(c); released(r); destroy(r);
}

static void
test_controlled_batch_new_types(void)
{
    for (unsigned old_float = 0; old_float < 2; ++old_float)
        for (unsigned invalid = 0; invalid < 2; ++invalid) {
            col_rel_t *r = relation(1, 2, 2, false, NULL);
            wirelog_column_type_t int_type = WIRELOG_TYPE_INT64;
            CHECK(col_rel_set_column_types(r, &int_type, 1) == 0);
            wirelog_column_type_t old_type = old_float
                ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
            CHECK(col_rel_set_column_types(r, &old_type, 1) == 0);
            batch_gap_candidate = relation(1, 8, 1, false, NULL);
            CHECK(col_rel_set_column_types(batch_gap_candidate, &int_type,
                1) == 0);
            wirelog_column_type_t new_type = old_float
                ? WIRELOG_TYPE_INT64 : WIRELOG_TYPE_FLOAT;
            CHECK(col_rel_set_column_types(batch_gap_candidate, &new_type,
                1) == 0);
            wl_evaluation_control_t *c = control(0);
            wl_columnar_relation_append_batch_t batch = {0};
            CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
                c) == 0);
            int64_t ordinary = 0;
            CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
                &ordinary) == 0);
            CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
                &ordinary) == 0);
            uint64_t bits = invalid ? UINT64_C(0x7ff8000000000001)
                : UINT64_C(0x8000000000000000);
            int64_t input; memcpy(&input, &bits, sizeof(input));
            batch_gap_old_columns = (uintptr_t)r->columns;
            batch_gap_old_scratch = (uintptr_t)r->row_scratch;
            batch_gap_calls = 0;
            wl_columnar_relation_test_batch_renewed = batch_replace_in_gap;
            int expected = !old_float && invalid ? EINVAL : 0;
            CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
                &input) == expected);
            wl_columnar_relation_test_batch_renewed = NULL;
            CHECK(batch_gap_calls == 1 && r->column_types[0] == new_type);
            CHECK(r->nrows == (expected ? 1u : 2u));
            if (!expected) CHECK(r->columns[0][1] == (old_float ? input : 0));
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
                expected) == expected);
            CHECK(finish(c, expected) > 0); release(c);
            destroy(batch_gap_candidate); batch_gap_candidate = NULL;
            destroy(r);
        }
}

static void
test_controlled_batch_resources(void)
{
    int64_t values[17];
    for (unsigned k = 0; k < 17; ++k) values[k] = 700 + k;
    for (unsigned allocation = 1; allocation <= 20; ++allocation) {
        col_rel_t *r = relation(17, 2, 2, true, NULL);
        growth_snapshot_t before = snapshot(r);
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_append_batch_t batch = {0};
        CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
            c) == 0);
        wl_columnar_relation_test_growth_fail_allocation = allocation;
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            values) == ENOMEM);
        CHECK(wl_columnar_relation_test_growth_fail_allocation == 0);
        CHECK(!r->memory_budget_denial_pending);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
            ENOMEM) == ENOMEM);
        unchanged(r, before); released(r);
        CHECK(finish(c, ENOMEM) > 0); release(c); destroy(r);
    }
    for (unsigned wide = 0; wide < 2; ++wide) {
        col_rel_t *r = relation(wide ? 17 : 1, 2, 2, false, NULL);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(r->memory_governor);
        growth_snapshot_t before = snapshot(r);
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_append_batch_t batch = {0};
        CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
            c) == 0);
        atomic_store_explicit(&g->usable_bytes, before.reserved,
            memory_order_relaxed);
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            values) == ENOMEM);
        CHECK(r->memory_budget_denial_pending);
        atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
            memory_order_relaxed);
        wl_columnar_relation_test_growth_fail_allocation = 1;
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            values) == ENOMEM);
        CHECK(!r->memory_budget_denial_pending);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
            ENOMEM) == ENOMEM);
        unchanged(r, before); released(r);
        CHECK(finish(c, ENOMEM) > 0); release(c); destroy(r);
    }
    col_rel_t *r = relation(17, 2, 2, true, NULL);
    col_rel_t *peer = relation(1, 3, 1, false, r->memory_governor);
    CHECK(col_rel_reserve_merge_grid(r, 3) == 0);
    int64_t **merge = r->merge_columns;
    merge[0][0] = 991;
    int64_t *scratch = r->row_scratch;
    growth_snapshot_t before = snapshot(r);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_append_batch_t batch = {0};
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    growth_watch = r; growth_peak = 0;
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
        values) == 0);
    uint64_t incoming = 17 * (4 * sizeof(int64_t) + sizeof(int64_t *))
        + 4 * sizeof(col_delta_timestamp_t);
    uint64_t old_image = 17 * (2 * sizeof(int64_t) + sizeof(int64_t *))
        + 2 * sizeof(col_delta_timestamp_t);
    CHECK(growth_peak == before.reserved + incoming + 17 * sizeof(int64_t));
    CHECK(snapshot(r).reserved == before.reserved + incoming - old_image);
    CHECK(r->retained_reserved_bytes == before.credit + incoming - old_image);
    CHECK(r->merge_columns == merge && merge[0][0] == 991 &&
        r->row_scratch == scratch);
    growth_watch = NULL;
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, 0) == 0);
    CHECK(finish(c, 0) > 0); release(c); destroy(r);
    CHECK(peer->columns[0][0] == 0); released(peer); destroy(peer);
}

static wl_columnar_memory_governor_ref_t *batch_gap_governor;
static uint64_t batch_gap_attached_bytes;
static void
batch_attach_in_gap(wl_columnar_relation_append_batch_t *batch)
{
    col_rel_t *r = batch->raw.relation;
    CHECK(!r->memory_governor && batch->raw.single.set.identity == 0);
    CHECK(atomic_load_explicit(&r->descriptor_access.state,
        memory_order_relaxed) == 0);
    CHECK(atomic_load_explicit(&r->source_access.state,
        memory_order_relaxed) == 0);
    CHECK(col_rel_attach_memory_governor(r, batch_gap_governor) == 0);
    batch_gap_attached_bytes = snapshot(r).reserved;
}

static void
test_controlled_batch_attach_policy(void)
{
    col_rel_t *r = col_rel_new_auto("batch_attach", 17);
    CHECK(r && !r->memory_governor && r->capacity > 0);
    r->nrows = r->capacity;
    for (uint32_t k = 0; k < r->ncols; ++k)
        for (uint32_t i = 0; i < r->nrows; ++i)
            r->columns[k][i] = (int64_t)k * 1000 + i;
    CHECK(col_rel_row(r, 0) != NULL);
    int64_t expected[17];
    memcpy(expected, r->row_scratch, sizeof(expected));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    batch_gap_governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(batch_gap_governor != NULL);
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_append_batch_t batch = {0};
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
        c) == 0);
    uint32_t old_capacity = r->capacity;
    for (uint32_t i = 0; i < old_capacity; ++i)
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            expected) == 0);
    CHECK(r->nrows == r->capacity && r->capacity == old_capacity * 2u);
    uint64_t old_image = 17 *
        ((uint64_t)r->capacity * sizeof(int64_t) + sizeof(int64_t *));
    uint64_t incoming = 17 *
        ((uint64_t)r->capacity * 2u * sizeof(int64_t) + sizeof(int64_t *));
    wl_columnar_relation_test_batch_renewed = batch_attach_in_gap;
    growth_watch = r; growth_peak = 0;
    CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
        r->row_scratch) == 0);
    wl_columnar_relation_test_batch_renewed = NULL; growth_watch = NULL;
    CHECK(batch_gap_attached_bytes > 0);
    /* Capture was ungoverned: no retroactive external S charge after attach. */
    CHECK(growth_peak == batch_gap_attached_bytes + incoming);
    CHECK(snapshot(r).reserved ==
        batch_gap_attached_bytes + incoming - old_image);
    for (uint32_t k = 0; k < 17; ++k)
        CHECK(r->columns[k][r->nrows - 1] == expected[k]);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, 0) == 0);
    CHECK(finish(c, 0) > 0); release(c); destroy(r);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(batch_gap_governor);
    CHECK(atomic_load_explicit(&g->reserved_bytes, memory_order_relaxed) == 0);
    wl_columnar_memory_governor_ref_release(batch_gap_governor);
    batch_gap_governor = NULL;
}

/* Independent literal tariffs from the accepted source inventory; no observed
 * whole-call cost is reused to construct these exact/minus-one thresholds. */
static void
test_controlled_batch_literal_boundaries(void)
{
    static const uint64_t begin_regions[] = {170, 94, 19, 39, 39, 45, 52, 45,
                                             21, 144};
    static const uint64_t row_regions[] = {220, 80, 36, 63, 103, 36, 36, 36,
                                           48};
    static const uint64_t set_regions[] = {220, 70};
    static const uint64_t renew_regions[] = {
        220, 80, 36, 128,
        94, 19, 39, 39, 45, 52, 45, 21, 144, 220,
        63, 103, 36, 36, 83, 83, 83, 83, 83, 76,
        74, 54, 84, 29, 32, 32, 183, 183, 36, 69, 151, 26, 228, 48
    };
    const uint64_t *lists[] = {begin_regions, row_regions, set_regions,
                               renew_regions};
    const size_t lengths[] = {
        sizeof(begin_regions) / sizeof(begin_regions[0]),
        sizeof(row_regions) / sizeof(row_regions[0]),
        sizeof(set_regions) / sizeof(set_regions[0]),
        sizeof(renew_regions) / sizeof(renew_regions[0])
    };
    const uint64_t setup[] = {0, 668, 668, 3437};
    for (unsigned operation = 0; operation < 4; ++operation) {
        uint64_t prefix = 0;
        for (size_t region = 0; region < lengths[operation]; ++region) {
            uint64_t previous = prefix;
            prefix += lists[operation][region];
            for (unsigned exact = 0; exact < 2; ++exact) {
                col_rel_t *r = relation(1, operation == 3 ? 2 : 4,
                        operation == 3 ? 2 : 1, false, NULL);
                wl_evaluation_control_t *c = control(setup[operation] + prefix -
                        (exact ? 0 : 1));
                wl_columnar_relation_append_batch_t batch = {0};
                int64_t value = 71;
                if (operation) {
                    CHECK(wl_columnar_relation_append_batch_begin_controlled(
                            &batch, r, c) == 0);
                    if (operation == 3) {
                        CHECK(wl_columnar_relation_append_batch_row_controlled(
                                &batch, &value) == 0);
                        CHECK(wl_columnar_relation_append_batch_row_controlled(
                                &batch, &value) == 0);
                        CHECK(r->nrows == 4 && r->capacity == 4);
                    }
                }
                uint32_t rows = r->nrows, capacity = r->capacity;
                uint64_t view = r->view_generation,
                    storage = r->storage_generation;
                int64_t first = r->columns[0][0];
                uint64_t reserved = snapshot(r).reserved;
                int expected = exact && region + 1 == lengths[operation]
                    ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
                int rc = operation == 0
                    ? wl_columnar_relation_append_batch_begin_controlled(&batch,
                        r, c)
                    : operation == 2
                        ? wl_columnar_relation_append_batch_set_controlled(
                    &batch, 0, 0, value)
                        : wl_columnar_relation_append_batch_row_controlled(
                    &batch, &value);
                CHECK(rc == expected);
                if (expected) {
                    CHECK(r->nrows == rows && r->capacity == capacity);
                    CHECK(r->view_generation == view &&
                        r->storage_generation == storage);
                    CHECK(r->columns[0][0] == first &&
                        snapshot(r).reserved == reserved);
                }
                if (batch.identity) {
                    int first_result = expected;
                    CHECK(wl_columnar_relation_append_batch_end_controlled(
                            &batch, first_result) == first_result);
                    CHECK(wl_columnar_relation_append_batch_end_controlled(
                            &batch, EIO) == first_result);
                } else CHECK(operation == 0 && !batch.raw.single.set.identity);
                CHECK(finish(c,
                    expected) ==
                    setup[operation] + (exact ? prefix : previous));
                release(c); released(r); destroy(r);
            }
        }
    }
}

static wl_columnar_source_access_reader_t batch_gap_reader;
static void
batch_busy_in_gap(wl_columnar_relation_append_batch_t *batch)
{
    col_rel_t *r = batch->raw.relation;
    CHECK(batch->raw.single.set.identity == 0);
    CHECK(wl_columnar_source_access_reader_acquire(&r->source_access,
        &batch_gap_reader) == 0);
}

static void
test_controlled_batch_ended_result(void)
{
    const int results[] = {0, WL_EVALUATION_CONTROL_CANCELLED, EIO};
    for (unsigned k = 0; k < 3; ++k) {
        col_rel_t *r = relation(1, 2, 2, false, NULL);
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_append_batch_t batch = {0};
        CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, r,
            c) == 0);
        int64_t value = 73;
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            &value) == 0);
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            &value) == 0);
        wl_columnar_relation_test_batch_renewed = batch_busy_in_gap;
        CHECK(wl_columnar_relation_append_batch_row_controlled(&batch,
            &value) == EBUSY);
        wl_columnar_relation_test_batch_renewed = NULL;
        CHECK(wl_columnar_source_access_reader_release(&batch_gap_reader) == 0);
        CHECK(batch.phase == WL_COLUMNAR_RELATION_BATCH_ENDED);
        CHECK(!batch.result_saved && !batch.raw.single.set.identity);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
            results[k]) == results[k]);
        CHECK(batch.result_saved && batch.saved_result == results[k]);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
            ENOTSUP) == results[k]);
        CHECK(finish(c, EBUSY) > 0); release(c); released(r); destroy(r);
    }
}

static void
test_row_resources(void)
{
    int64_t values[17];
    for (unsigned i = 0; i < 17; ++i) values[i] = 80 + i;
    /* Wide S, table, each of seventeen lanes, and timestamp allocation. */
    for (unsigned allocation = 1; allocation <= 20; ++allocation) {
        col_rel_t *r = relation(17, 2, 2, true, NULL);
        growth_snapshot_t before = snapshot(r);
        wl_evaluation_control_t *c = control(0);
        wl_columnar_relation_test_growth_fail_allocation = allocation;
        CHECK(wl_columnar_relation_append_row_controlled(r, values,
            c) == ENOMEM);
        CHECK(wl_columnar_relation_test_growth_fail_allocation == 0);
        CHECK(!r->memory_budget_denial_pending);
        CHECK(finish(c, ENOMEM) > 0); unchanged(r, before); released(r);
        release(c); destroy(r);
    }
    for (unsigned wide = 0; wide < 2; ++wide) {
        col_rel_t *r = relation(wide ? 17 : 1, 2, 2, false, NULL);
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(r->memory_governor);
        growth_snapshot_t before = snapshot(r);
        atomic_store_explicit(&g->usable_bytes, before.reserved,
            memory_order_relaxed);
        wl_evaluation_control_t *c = control(0);
        CHECK(wl_columnar_relation_append_row_controlled(r, values,
            c) == ENOMEM);
        CHECK(r->memory_budget_denial_pending);
        CHECK(finish(c, ENOMEM) > 0); unchanged(r, before);
        retry(c, 1);
        CHECK(wl_columnar_relation_append_row_controlled(r, values, c)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(r->memory_budget_denial_pending);
        CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 0);
        retry(c, 0);
        atomic_store_explicit(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET,
            memory_order_relaxed);
        wl_columnar_relation_test_growth_fail_allocation = 1;
        CHECK(wl_columnar_relation_append_row_controlled(r, values,
            c) == ENOMEM);
        CHECK(!r->memory_budget_denial_pending);
        CHECK(finish(c, ENOMEM) > 0); unchanged(r, before);
        retry(c, 0);
        growth_watch = r; growth_peak = 0;
        CHECK(wl_columnar_relation_append_row_controlled(r, values, c) == 0);
        uint64_t incoming = (uint64_t)r->ncols * (4 * sizeof(int64_t)
            + sizeof(int64_t *));
        CHECK(growth_peak == before.reserved + incoming
            + (wide ? 17 * sizeof(int64_t) : 0));
        growth_watch = NULL;
        CHECK(finish(c, 0) > 0); release(c); destroy(r);
    }
}

static void
test_row_alias_and_nullary(void)
{
    for (unsigned wide = 0; wide < 2; ++wide) {
        uint32_t width = wide ? 17 : 2;
        col_rel_t *r = relation(width, 32, 32, false, NULL);
        col_rel_t *raw = relation(width, 32, 32, false, NULL);
        wl_evaluation_control_t *c = control(0);
        CHECK(wl_columnar_relation_append_row_controlled(r,
            r->columns[0] + 1, c) == 0);
        CHECK(col_rel_append_row(raw, raw->columns[0] + 1) == 0);
        row_twins_equal(r, raw);
        for (uint32_t i = 0; i < width; ++i)
            CHECK(r->columns[i][32] == (int64_t)i + 1);
        CHECK(r->row_scratch && raw->row_scratch);
        for (uint32_t i = 0; i < width; ++i)
            r->row_scratch[i] = raw->row_scratch[i] = 71 + i;
        CHECK(wl_columnar_relation_append_row_controlled(r, r->row_scratch,
            c) == 0);
        CHECK(col_rel_append_row(raw, raw->row_scratch) == 0);
        row_twins_equal(r, raw);
        CHECK(finish(c, 0) > 0); release(c); destroy(r); destroy(raw);
    }
    col_rel_t *r = relation(0, 0, 0, false, NULL);
    col_rel_t *raw = relation(0, 0, 0, false, NULL);
    int64_t dummy = 0;
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_relation_append_row_controlled(r, &dummy, c) == 0);
    CHECK(col_rel_append_row(raw, &dummy) == 0);
    CHECK(r->capacity == 64); row_twins_equal(r, raw);
    CHECK(finish(c, 0) > 0); release(c); destroy(r); destroy(raw);
    r = relation(1, 2, 1, false, NULL); raw = relation(1, 2, 1, false, NULL);
    CHECK(wl_columnar_relation_append_row_controlled(r, &dummy, NULL) == 0);
    CHECK(col_rel_append_row(raw, &dummy) == 0); row_twins_equal(r, raw);
    destroy(r); destroy(raw);
}

static void
test_row_float_and_refusal(void)
{
    col_rel_t *r = relation(1, 2, 0, false, NULL);
    col_rel_t *raw = relation(1, 2, 0, false, NULL);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    CHECK(col_rel_set_column_types(r, &type, 1) == 0);
    CHECK(col_rel_set_column_types(raw, &type, 1) == 0);
    int64_t values[] = { INT64_MIN, 0, INT64_C(0x3ff8000000000000),
                         INT64_C(0x7ff0000000000000),
                         INT64_C(0x7ff8000000000001) };
    wl_evaluation_control_t *c = control(0);
    for (unsigned i = 0; i < 5; ++i) {
        int expected = i < 3 ? 0 : EINVAL;
        CHECK(wl_columnar_relation_append_row_controlled(r, &values[i],
            c) == expected);
        CHECK(col_rel_append_row(raw, &values[i]) == expected);
        row_twins_equal(r, raw);
    }
    CHECK(r->columns[0][0] == 0 && r->columns[0][1] == 0);
    CHECK(finish(c, 0) > 0); release(c); destroy(r); destroy(raw);
    for (unsigned bad = 0; bad < 4; ++bad) {
        r = relation(bad == 0 ? 1 : 0, bad == 0 ? 0 : 2, 0, false, NULL);
        growth_snapshot_t before = snapshot(r);
        uint64_t original_view = r->view_generation;
        uint64_t original_storage = r->storage_generation;
        uint32_t original_rows = r->nrows, original_capacity = r->capacity;
        if (bad ==
            1) r->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
        if (bad == 2) {
            r->capacity = UINT32_MAX; r->nrows = UINT32_MAX;
        }
        if (bad == 3) {
            r->nrows = r->capacity;
            r->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
            r->storage_owner_generation = r->storage_generation;
        }
        int expected = bad ? EOVERFLOW : ENOTSUP;
        c = control(0);
        CHECK(wl_columnar_relation_append_row_controlled(r, values,
            c) == expected);
        CHECK(finish(c, expected) > 0); release(c);
        r->view_generation = original_view;
        r->storage_generation = r->storage_owner_generation = original_storage;
        r->nrows = original_rows; r->capacity = original_capacity;
        unchanged(r, before); released(r); destroy(r);
    }
    /* Quiescent ungoverned shape fixture with a genuinely allocated longer
     * timestamp image; no retained token or ledger tracking is fabricated. */
    r = col_rel_new_auto("long-ts", 1);
    CHECK(r && col_rel_enable_timestamps(r) == 0);
    uint32_t longer = r->capacity + 1u;
    col_delta_timestamp_t *timestamps = realloc(r->timestamps,
            (size_t)longer * sizeof(*timestamps));
    CHECK(timestamps != NULL);
    r->timestamps = timestamps;
    r->timestamp_capacity = longer;
    growth_snapshot_t before = snapshot(r);
    c = control(0);
    CHECK(wl_columnar_relation_append_row_controlled(r, values, c) == ENOTSUP);
    CHECK(finish(c, ENOTSUP) > 0); release(c); unchanged(r, before);
    destroy(r);
}

static void
test_row_auxiliary_peer(void)
{
    col_rel_t *r = relation(17, 2, 2, true, NULL);
    col_rel_t *peer = relation(1, 3, 1, false, r->memory_governor);
    CHECK(col_rel_reserve_merge_grid(r, 3) == 0);
    int64_t **merge = r->merge_columns;
    merge[0][0] = 993;
    int64_t *scratch = r->row_scratch;
    growth_snapshot_t before = snapshot(r);
    int64_t values[17];
    for (unsigned i = 0; i < 17; ++i) values[i] = 45 + i;
    wl_evaluation_control_t *c = control(0);
    growth_watch = r; growth_peak = 0;
    CHECK(wl_columnar_relation_append_row_controlled(r, values, c) == 0);
    uint64_t incoming = 17 * (4 * sizeof(int64_t) + sizeof(int64_t *))
        + 4 * sizeof(col_delta_timestamp_t);
    uint64_t old_image = 17 * (2 * sizeof(int64_t) + sizeof(int64_t *))
        + 2 * sizeof(col_delta_timestamp_t);
    CHECK(growth_peak == before.reserved + incoming + 17 * sizeof(int64_t));
    CHECK(r->retained_reserved_bytes == before.credit + incoming - old_image);
    CHECK(r->merge_columns == merge && merge[0][0] == 993);
    CHECK(r->row_scratch == scratch);
    wl_columnar_memory_governor_t *g =
        wl_columnar_memory_governor_ref_get(peer->memory_governor);
    CHECK(atomic_load_explicit(&g->reserved_bytes, memory_order_relaxed)
        == before.reserved + incoming - old_image);
    growth_watch = NULL;
    CHECK(finish(c, 0) > 0); release(c); destroy(r);
    CHECK(peer->columns[0][0] == 0); released(peer); destroy(peer);
}

#ifndef _WIN32
static int
row_release_refused(wl_columnar_relation_mutation_single_t *single,
    unsigned slot)
{
    (void)single;
    return slot == single_fail_slot ? EIO : 0;
}

static void
test_row_release_fail_stop(void)
{
    for (unsigned mode = 0; mode < 3; ++mode) {
        pid_t child = fork();
        CHECK(child >= 0);
        if (child == 0) {
            struct rlimit core_limit = {0, 0};
            (void)setrlimit(RLIMIT_CORE, &core_limit);
            fclose(stderr);
            col_rel_t *r = relation(mode == 2 ? 17 : 1, 2, 1,
                    false, NULL);
            int64_t values[17] = {0};
            wl_evaluation_control_t *c = control(0);
            if (mode < 2) {
                single_fail_slot = mode;
                wl_columnar_relation_test_single_release = row_release_refused;
            } else wl_columnar_memory_governor_test_refuse_next_release();
            (void)wl_columnar_relation_append_row_controlled(r, values, c);
            _exit(0);
        }
        int status = 0;
        CHECK(waitpid(child, &status, 0) == child && WIFSIGNALED(status)
            && WTERMSIG(status) == SIGABRT);
    }
}
#endif

static void
test_row_real_ledger_peer(void)
{
    wl_mem_ledger_t ledger;
    wl_mem_ledger_init(&ledger, 0);
    col_rel_t *r = relation(2, 2, 1, true, NULL);
    col_rel_t *peer = col_rel_new_auto("ledger-peer", 1);
    CHECK(peer != NULL);
    r->mem_ledger = peer->mem_ledger = &ledger;
    col_rel_ledger_reconcile(r, 0);
    col_rel_ledger_reconcile(peer, 0);
    const uint64_t peer_bytes = col_rel_owned_ledger_bytes(peer);
    int64_t values[2] = {71, 91};
    wl_evaluation_control_t *c = control(0);
    for (unsigned i = 0; i < 9; ++i) {
        CHECK(wl_columnar_relation_append_row_controlled(r, values, c) == 0);
        uint64_t owned = col_rel_owned_ledger_bytes(r);
        uint64_t ts = (uint64_t)r->timestamp_capacity * sizeof(*r->timestamps);
        CHECK(r->ledger_ts_bytes == ts);
        CHECK(atomic_load_explicit(&ledger.current_bytes, memory_order_relaxed)
            == peer_bytes + owned + ts);
        col_rel_ledger_reconcile(r, owned);
        CHECK(atomic_load_explicit(&ledger.current_bytes, memory_order_relaxed)
            == peer_bytes + owned + ts);
    }
    CHECK(finish(c, 0) > 0); release(c); destroy(r);
    CHECK(atomic_load_explicit(&ledger.current_bytes, memory_order_relaxed)
        == peer_bytes);
    released(peer); destroy(peer);
    CHECK(atomic_load_explicit(&ledger.current_bytes,
        memory_order_relaxed) == 0);
}

/* Literal operation-region oracle for one real governed one-lane growth,
 * no ledger/TS: B1 source table + AB-S and AB-G source tables. This is
 * independent of checkpoint observations and the whole-call measured cost. */
static void
test_row_region_boundaries(void)
{
    static const uint64_t regions[] = {
        83, 63,
        94, 19, 39, 39, 45, 52, 45, 21, 144,
        103, 36, 36,
        83, 83, 83, 83, 83, 76,
        74, 54, 84, 29,
        32, 32, 183, 36,
        69, 151, 26, 228, 48
    };
    uint64_t prefix = 0;
    const size_t count = sizeof(regions) / sizeof(regions[0]);
    for (size_t region = 0; region < count; ++region) {
        uint64_t previous = prefix;
        prefix += regions[region];
        for (unsigned exact = 0; exact < 2; ++exact) {
            col_rel_t *r = relation(1, 1, 1, false, NULL);
            growth_snapshot_t before = snapshot(r);
            int64_t value = 81;
            wl_evaluation_control_t *c = control(prefix - (exact ? 0 : 1));
            int expected = exact && region + 1 == count
                ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(wl_columnar_relation_append_row_controlled(r, &value,
                c) == expected);
            CHECK(finish(c, expected) == (exact ? prefix : previous));
            if (expected) unchanged(r, before);
            else CHECK(r->nrows == 2 && r->columns[0][1] == value);
            released(r); release(c); destroy(r);
        }
    }
}

static unsigned row_denial_fault, row_denial_hits;
static void
row_denial_verdict(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (phase != WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT) return;
    ++row_denial_hits;
    if (row_denial_fault == 1) wl_evaluation_control_request_cancel(c);
    if (row_denial_fault == 2)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    if (row_denial_fault == 3) {
        row_old_consumed = finish(c, 0);
        retry(c, 0);
    }
}

static void
test_row_denial_precedence(void)
{
    for (unsigned wide = 0; wide < 2; ++wide)
        for (unsigned fault = 1; fault <= 3; ++fault) {
            col_rel_t *r = relation(wide ? 17 : 1, 2, 2, false, NULL);
            growth_snapshot_t before = snapshot(r);
            wl_columnar_memory_governor_t *g =
                wl_columnar_memory_governor_ref_get(r->memory_governor);
            atomic_store_explicit(&g->usable_bytes, before.reserved,
                memory_order_relaxed);
            int64_t values[17] = {0};
            wl_evaluation_control_t *c = control(0);
            row_denial_fault = fault; row_denial_hits = 0;
            row_old_consumed = 0;
            wl_columnar_memory_governor_test_checkpoint = row_denial_verdict;
            int expected = fault == 1 ? ENOMEM : fault ==
                2 ? EOVERFLOW : EINVAL;
            CHECK(wl_columnar_relation_append_row_controlled(r, values,
                c) == expected);
            wl_columnar_memory_governor_test_checkpoint = NULL;
            CHECK(row_denial_hits == 1);
            CHECK(r->memory_budget_denial_pending == (fault == 1));
            unchanged(r, before); released(r);
            if (fault == 3) {
                CHECK(row_old_consumed > 0);
                CHECK(finish(c, 0) == 0);
            } else CHECK(finish(c, expected) > 0);
            release(c); destroy(r);
        }
}

/* Protected physical selected rows, including real post-return retirement. */
static uint64_t
selected_source_setup(wl_columnar_relation_scalar_source_t *s, col_rel_t *r,
    bool ts, wl_evaluation_control_t *c)
{
    bool initialized = false;
    CHECK(wl_columnar_relation_scalar_source_init_attempt_controlled(s,
        &initialized, c, wl_evaluation_control_active_attempt_id(c)) == 0
        && initialized);
    CHECK(wl_columnar_relation_scalar_source_begin_attempt_controlled(s, r, ts)
        == 0);
    return 1632+224*((r->ncols+7u)/8u);
}

static void
selected_report(wl_evaluation_control_t *c, uint64_t id, int rc, uint64_t work)
{
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0?rc:0, 0) == rc);
    CHECK(wl_evaluation_control_report(c, &report) == 0
        && report.attempt_id == id && report.consumed == work
        && report.result == rc);
}

static void
selected_literal_prefixes(uint32_t width, bool ts)
{
    /* Independent nested B2 schedule, with real wide S reserve/commit. */
    uint64_t fees[256]; size_t n = 0;
    fees[n++] = 192; fees[n++] = 256;
    if (ts)fees[n++] = 128;
    fees[n++] = 128;
    fees[n++] = 220; fees[n++] = 80;
    if (width > 16) {
        const uint64_t wide[] = {80, 74, 54, 84, 29, 74, 54, 29, 32};
        for (size_t i = 0; i < sizeof(wide)/sizeof(wide[0]);
            i++) fees[n++] = wide[i];
    }
    for (uint32_t i = 0; i < width; i++)fees[n++] = 36;
    fees[n++] = 63; fees[n++] = 103;
    for (unsigned pass = 0; pass < 3; pass++)
        for (uint32_t i = 0; i < width; i++)fees[n++] = 36;
    if (ts)for (size_t pos = 0; pos < sizeof(col_delta_timestamp_t); pos += 16)
            fees[n++] = 183;
    fees[n++] = 48; fees[n++] = 256;
    if (ts)fees[n++] = 128;
    uint64_t prefix = 0;
    for (size_t i = 0; i < n; i++) {
        uint64_t previous = prefix; prefix += fees[i];
        for (unsigned below = 0; below < 2; below++) {
            col_rel_t *src = relation(width, 2, 2, ts, NULL);
            col_rel_t *dst = relation(width, 4, 0, ts, NULL);
            if (ts) {
                src->timestamps[1]._reserved = 77;
                src->timestamps[1].multiplicity = -9;
            }
            wl_evaluation_control_t *c = control(1632+224*((width+7u)/8u)+668+
                    prefix-below);
            uint64_t id = wl_evaluation_control_active_attempt_id(c);
            wl_columnar_relation_scalar_source_t s;
            uint64_t setup = selected_source_setup(&s, src, ts, c)+668;
            wl_columnar_relation_append_batch_t batch = {0};
            CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch,
                dst, c) == 0);
            int64_t values[17];
            for (uint32_t col = 0; col < width;
                col++) values[col] = src->columns[col][1];
            int rc =
                wl_columnar_relation_append_batch_selected_source_row_controlled
                (
                &batch, &s, 1, values, width);
            int expected = !below &&
                i+1 == n?0:WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(rc == expected);
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, rc)
                == rc && batch.phase == WL_COLUMNAR_RELATION_BATCH_ENDED);
            CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
            selected_report(c, id, rc, setup+(below?previous:prefix));
            if (!rc) {
                CHECK(dst->nrows == 1);
                for (uint32_t col = 0; col < width;
                    col++) CHECK(dst->columns[col][0] == values[col]);
                if (ts) {
                    col_delta_timestamp_t *a = &dst->timestamps[0];
                    col_delta_timestamp_t *b = &src->timestamps[1];
                    CHECK(a->iteration == b->iteration &&
                        a->stratum == b->stratum
                        && a->worker == b->worker &&
                        a->_reserved == b->_reserved
                        && a->multiplicity == b->multiplicity);
                }
            }
            /* A late refusal may leave a private committed row. Discard it. */
            released(src); released(dst); release(c); destroy(dst);
            destroy(src);
        }
    }
    CHECK(prefix ==
        832+514+144*width+(width >
        16?510:0)+(ts?256+183*((sizeof(col_delta_timestamp_t)+15)/16):0));
}

static unsigned selected_renewals;
static void
selected_renewed(wl_columnar_relation_append_batch_t *batch)
{
    CHECK(batch->phase == WL_COLUMNAR_RELATION_BATCH_ENDED);
    selected_renewals++;
}

static void
selected_twins(uint32_t width, bool preserve, bool destination_ts)
{
    col_rel_t *src = relation(width, 270, 270, true, NULL);
    col_rel_t *dst = relation(width, 2, 0, destination_ts, NULL);
    col_rel_t *raw = relation(width, 2, 0, destination_ts, NULL);
    for (uint32_t i = 0; i < src->nrows; i++) {
        src->timestamps[i].iteration = 100+i;
        src->timestamps[i].stratum = 300-i;
        src->timestamps[i].worker = i%4;
        src->timestamps[i]._reserved = 701+i;
        src->timestamps[i].multiplicity = i%3 == 0?0:i%3 == 1?-7:5;
    }
    /* A noTS wrapper may target a TS-capable destination: raw defaults stay. */
    bool ts = preserve && destination_ts;
    wl_evaluation_control_t *c = control(0);
    wl_columnar_relation_scalar_source_t s;
    selected_source_setup(&s, src, ts, c);
    wl_columnar_relation_append_batch_t batch = {0};
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, dst,
        c) == 0);
    selected_renewals = 0;
    wl_columnar_relation_test_batch_renewed = selected_renewed;
    int64_t values[17]; CHECK(width <= 17);
    uint64_t scratch_credit = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(src->memory_governor));
    for (uint32_t row = 0; row < src->nrows; row++) {
        CHECK(wl_columnar_relation_scalar_source_gather_attempt_controlled(&s,
            row, values, width) == 0);
        CHECK(wl_columnar_relation_append_batch_selected_source_row_controlled(
                &batch, &s, row, values, width) == 0);
        CHECK(col_rel_append_row(raw, values) == 0);
        if (ts) {
            raw->timestamps[row].iteration = src->timestamps[row].iteration;
            raw->timestamps[row].stratum = src->timestamps[row].stratum;
            raw->timestamps[row].worker = src->timestamps[row].worker;
            raw->timestamps[row]._reserved = src->timestamps[row]._reserved;
            raw->timestamps[row].multiplicity =
                src->timestamps[row].multiplicity;
        }
    }
    wl_columnar_relation_test_batch_renewed = NULL;
    CHECK(selected_renewals > 0);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, 0) == 0);
    CHECK(wl_columnar_relation_scalar_source_end(&s, 0) == 0);
    CHECK(dst->nrows == 270 && raw->nrows == 270);
    payload_equal(dst, raw, 0);
    CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(src->memory_governor)) ==
        scratch_credit);
    CHECK(finish(c, 0) > 0); release(c); destroy(raw); destroy(dst);
    destroy(src);
}

static wl_evaluation_control_t *selected_capture_control;
static wl_columnar_memory_reservation_t *selected_capture_token;
static wl_columnar_memory_governor_t *selected_capture_governor;
static col_rel_t *selected_capture_destination;
static uint64_t selected_old_id, selected_old_work;
static unsigned selected_capture_phase, selected_capture_mode,
    selected_capture_hits;
static unsigned selected_successor_hooks;
static bool selected_capture_armed, selected_capture_restarted;
static bool selected_capture_observed;

static void
selected_capture_checkpoint(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (selected_capture_restarted) {
        selected_successor_hooks++; return;
    }
    if (selected_capture_armed &&
        phase == WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF
        && token->governor == selected_capture_governor
        && token->bytes == 17*sizeof(int64_t)) {
        CHECK(c == selected_capture_control && !selected_capture_observed);
        selected_capture_token = token;
        selected_capture_observed = true;
    }
}

static void
selected_capture_work(wl_evaluation_control_t *c, uint64_t work)
{
    if (selected_capture_restarted) {
        selected_successor_hooks++; return;
    }
    checkpoint(c, work);
}

void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *token,
    unsigned phase)
{
    if (!selected_capture_armed || token != selected_capture_token
        || phase != selected_capture_phase || selected_capture_hits)return;
    CHECK(token->identity == token && token->bytes == 17*sizeof(int64_t));
    CHECK(token->governor == selected_capture_governor);
    CHECK(selected_capture_destination->nrows == 1);
    selected_capture_hits++;
    if (selected_capture_mode == 0)
        wl_evaluation_control_request_cancel(selected_capture_control);
    else if (selected_capture_mode == 1)
        CHECK(wl_evaluation_control_charge(selected_capture_control, UINT64_MAX)
            == EOVERFLOW);
    else {
        CHECK(wl_evaluation_control_finish(selected_capture_control, &owner, 0,
            0) == 0);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(selected_capture_control,
            &report) == 0
            && report.attempt_id == selected_old_id);
        selected_old_work = report.consumed;
        CHECK(wl_evaluation_control_reset(selected_capture_control) == 0);
        CHECK(wl_evaluation_control_begin(selected_capture_control,
            &owner) == 0);
        CHECK(wl_evaluation_control_active_attempt_id(selected_capture_control)
            != selected_old_id);
        selected_capture_restarted = true;
    }
}

static void
selected_capture_retirement(void)
{
    unsigned phases[] = {WL_COLUMNAR_MEMORY_TEST_RELEASE_BEFORE_CLAIM,
                         WL_COLUMNAR_MEMORY_TEST_RELEASE_CREDITED};
    for (unsigned p = 0; p < 2; p++)for (unsigned mode = 0; mode < 3; mode++) {
            col_rel_t *src = relation(17, 2, 2, true, NULL);
            col_rel_t *dst = relation(17, 4, 0, true, NULL);
            wl_columnar_memory_governor_t *g =
                wl_columnar_memory_governor_ref_get(dst->memory_governor);
            wl_columnar_memory_reservation_t peer = {0};
            wl_columnar_memory_reservation_init(&peer);
            CHECK(wl_columnar_memory_reserve(g, 17, &peer)
                && wl_columnar_memory_commit(&peer, &peer));
            uint64_t baseline = wl_columnar_memory_reserved(g);
            wl_evaluation_control_t *c = control(0);
            wl_columnar_relation_scalar_source_t s;
            selected_source_setup(&s, src, true, c);
            wl_columnar_relation_append_batch_t batch = {0};
            CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch,
                dst, c) == 0);
            int64_t values[17];
            CHECK(wl_columnar_relation_scalar_source_gather_attempt_controlled(
                    &s,
                    1, values, 17) == 0);
            selected_capture_control = c; selected_capture_destination = dst;
            selected_capture_governor = g; selected_capture_token = NULL;
            selected_capture_phase = phases[p]; selected_capture_mode = mode;
            selected_capture_hits = selected_successor_hooks = 0;
            selected_old_id = wl_evaluation_control_active_attempt_id(c);
            selected_old_work = 0; selected_capture_restarted = false;
            selected_capture_observed = false;
            selected_capture_armed = true;
            wl_columnar_memory_governor_test_checkpoint =
                selected_capture_checkpoint;
            wl_columnar_relation_test_work_checkpoint = selected_capture_work;
            wl_columnar_memory_governor_test_work_checkpoint =
                selected_capture_work;
            int rc =
                wl_columnar_relation_append_batch_selected_source_row_controlled
                (
                &batch, &s, 1, values, 17);
            selected_capture_armed = false;
            selected_capture_token = NULL;
            CHECK(rc ==
                (mode == 0?WL_EVALUATION_CONTROL_CANCELLED:mode ==
                1?EOVERFLOW:EINVAL));
            CHECK(selected_capture_hits == 1 && selected_capture_observed);
            CHECK(dst->nrows == 1 && dst->timestamps[0].iteration == 0
                && dst->timestamps[0]._reserved == 0);
            CHECK(wl_columnar_memory_reserved(g) == baseline);
            CHECK(selected_successor_hooks == 0);
            CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
                rc) == rc);
            CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
            wl_columnar_memory_governor_test_checkpoint = NULL;
            wl_columnar_relation_test_work_checkpoint = checkpoint;
            wl_columnar_memory_governor_test_work_checkpoint = checkpoint;
            if (mode == 2) {
                CHECK(selected_old_work == 8314);
                selected_report(c, wl_evaluation_control_active_attempt_id(c),
                    0, 0);
            } else selected_report(c, selected_old_id, rc, 8314);
            CHECK(wl_columnar_memory_release(&peer));
            CHECK(wl_columnar_memory_reserved(g) == baseline-17);
            released(src); released(dst); release(c); destroy(dst);
            destroy(src);
        }
    selected_capture_restarted = false;
}

static void *
selected_wrong_thread(void *arg)
{
    wl_columnar_relation_scalar_source_t *s = arg;
    int64_t values[17] = {0};
    CHECK(wl_columnar_relation_append_batch_selected_source_row_controlled(
            NULL, s, 0, values, s->width) == EINVAL);
    return NULL;
}

static void
selected_domains(void)
{
    for (unsigned mode = 0; mode < 6; mode++) {
        col_rel_t *src = relation(1, 2, 2, true, NULL);
        col_rel_t *dst = relation(1, 4, 0, mode != 3, NULL);
        wl_evaluation_control_t *c = control(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_relation_scalar_source_t s;
        selected_source_setup(&s, src, true, c);
        wl_columnar_relation_append_batch_t batch = {0};
        CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, dst,
            c) == 0);
        int64_t value = 11;
        wl_columnar_relation_scalar_source_t copied_source = s;
        wl_columnar_relation_append_batch_t copied_batch = batch;
        int rc =
            wl_columnar_relation_append_batch_selected_source_row_controlled(
            mode == 4?&copied_batch:&batch, mode == 5?&copied_source:&s,
            mode == 0?2:1, mode == 1?NULL:&value, mode == 2?0:1);
        CHECK(rc == EINVAL && dst->nrows == 0);
        wl_thread_t thread;
        CHECK(wl_thread_create(&thread, selected_wrong_thread, &s) == 0);
        CHECK(wl_thread_join(&thread) == 0);
        CHECK(s.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
            && batch.phase == WL_COLUMNAR_RELATION_BATCH_HELD);
        CHECK(wl_columnar_relation_append_batch_end_controlled(&batch,
            rc) == rc);
        CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
        selected_report(c, id, rc, 2524+(mode == 5?0:mode == 3?448:192));
        release(c); destroy(dst); destroy(src);
    }
    /* Private wide capture allocation failure returns its original credit,
     * without consuming a source reader or publishing any destination row. */
    col_rel_t *src = relation(17, 2, 2, false, NULL);
    col_rel_t *dst = relation(17, 4, 0, false, NULL);
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(dst->memory_governor));
    wl_evaluation_control_t *c = control(0);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_columnar_relation_scalar_source_t s;
    selected_source_setup(&s, src, false, c);
    wl_columnar_relation_append_batch_t batch = {0};
    CHECK(wl_columnar_relation_append_batch_begin_controlled(&batch, dst,
        c) == 0);
    int64_t values[17] = {0};
    wl_columnar_relation_test_growth_fail_allocation = 1;
    int rc = wl_columnar_relation_append_batch_selected_source_row_controlled(
        &batch, &s, 1, values, 17);
    CHECK(rc == ENOMEM &&
        wl_columnar_relation_test_growth_fail_allocation == 0);
    CHECK(dst->nrows == 0 && wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(dst->memory_governor)) ==
        baseline);
    CHECK(wl_columnar_relation_append_batch_end_controlled(&batch, rc) == rc);
    CHECK(wl_columnar_relation_scalar_source_end(&s, rc) == rc);
    /* setup2972 + wrapper576 + B2 capture220+80+80+241+157+32 */
    selected_report(c, id, rc, 4358);
    release(c); destroy(dst); destroy(src);
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
#ifndef _WIN32
    test_row_release_fail_stop();
#endif
    test_row_float_and_refusal();
    test_row_auxiliary_peer();
    test_row_region_boundaries();
    test_row_real_ledger_peer();
    test_row_twins();
    test_row_stops();
    test_row_denial_precedence();
    test_row_resources();
    test_controlled_batch_twins();
    test_controlled_batch_gap_capture();
    test_controlled_batch_end_authority();
    test_controlled_batch_seams();
    test_controlled_batch_renew_authority();
    test_controlled_batch_new_types();
    test_controlled_batch_resources();
    test_controlled_batch_attach_policy();
    test_controlled_batch_literal_boundaries();
    test_controlled_batch_ended_result();
    test_row_alias_and_nullary();
    test_attempt_single_boundaries();
    test_attempt_single_seams();
    test_attempt_single_entry();
    test_attempt_single_domains_and_busy();
    test_attempt_single_nonce_and_cleanup();
    test_exact_capacity_success(1, 0, true);
    test_exact_capacity_success(1, 300, true);
    test_exact_capacity_success(300, 2, false);
    test_exact_capacity_boundaries();
    test_exact_capacity_faults();
    for (unsigned mode = 0; mode < 3; mode++)
        for (unsigned governed = 0; governed < 2; governed++)
            test_exact_capacity_independent(mode, governed != 0);
    test_exact_capacity_float_and_null();
    test_exact_capacity_shared_ledger();
    test_exact_capacity_governor();
    test_exact_capacity_no_growth_and_scope();
    for (unsigned mode = 0; mode < 3; mode++)
        test_exact_capacity_partial_initialization(mode);
    for (unsigned governed = 0; governed < 2; governed++) {
        for (unsigned names_mode = 0; names_mode < 3; names_mode++)
            test_metadata_success(4, governed != 0, names_mode, true, true);
        test_metadata_success(300, governed != 0, 0, false, false);
    }
    test_metadata_boundaries();
    test_metadata_faults();
    test_metadata_governor();
    test_metadata_peer_and_ledger();
    test_metadata_scope();
    test_metadata_wide_table_and_advisory();
    selected_domains();
    selected_literal_prefixes(0, false);
    selected_literal_prefixes(1, false);
    selected_literal_prefixes(1, true);
    selected_literal_prefixes(17, false);
    selected_literal_prefixes(17, true);
    selected_capture_retirement();
    const uint32_t selected_widths[] = {0, 1, 17};
    for (size_t i = 0; i < sizeof(selected_widths)/sizeof(selected_widths[0]);
        i++) {
        selected_twins(selected_widths[i], false, false);
        selected_twins(selected_widths[i], false, true);
        selected_twins(selected_widths[i], true, true);
    }
    puts("evaluation_work: OK");
    return 0;
}
