#include "columnar/payload_join.h"
#include "columnar/join_batch.h"
#include "test_tmpdir.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); abort(); } } while (0)
#define OK WL_COLUMNAR_SPILL_OK
static unsigned allocation_fail;
static bool read_fail;
static unsigned read_fail_after;
static int write_error;
static bool short_writes;
bool wl_columnar_payload_join_test_allocation(void)
{
    return allocation_fail && --allocation_fail == 0;
}
bool wl_columnar_spill_test_allocation(void) {
    return false;
}
int wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)offset;
    if (writing && write_error) return write_error;
    if (writing && short_writes && *length > 3) *length = 3;
    return !writing &&
           (read_fail || (read_fail_after && --read_fail_after == 0)) ? EIO : 0;
}

/* Run the same storage/ownership oracles against full and capped indexes. */
static uint64_t index_limit;
static uint64_t index_cap(uint32_t rows)
{
    uint64_t buckets = 1;
    while (buckets < rows && buckets < (UINT64_C(1) << 31)) buckets *= 2;
    return sizeof(wl_columnar_memory_reservation_t) + 4 * (buckets + rows);
}
static wl_columnar_spill_status_t create_join(
    wl_columnar_memory_governor_ref_t *g, wl_columnar_batch_payload_t *l,
    wl_columnar_batch_payload_t *r, uint32_t lk, uint32_t rk, uint64_t bytes,
    wl_columnar_spill_cancel_fn cancel, void *context,
    wl_columnar_continuation_t **out)
{
    return index_limit ? wl_columnar_payload_join_create_bounded(g, l, r,
               lk, rk, bytes, index_limit, cancel, context, out)
        : wl_columnar_payload_join_create(g, l, r, lk, rk, bytes, cancel,
               context, out);
}
static wl_columnar_spill_status_t create_weighted_join(
    wl_columnar_memory_governor_ref_t *g, wl_columnar_batch_payload_t *l,
    wl_columnar_batch_payload_t *r, uint32_t lk, uint32_t rk, uint64_t bytes,
    wl_columnar_spill_cancel_fn cancel, void *context,
    wl_columnar_continuation_t **out)
{
    return index_limit ? wl_columnar_payload_join_create_weighted_bounded(g,
               l, r, lk, rk, bytes, index_limit, cancel, context, out)
        : wl_columnar_payload_join_create_weighted(g, l, r, lk, rk, bytes,
               cancel, context, out);
}

static wl_columnar_memory_governor_ref_t *governor(uint64_t budget)
{
    wl_columnar_memory_resolution_t r = {.budget_bytes = budget,
                                         .usable_bytes = budget,
                                         .mode =
                                             WL_COLUMNAR_MEMORY_MODE_ENFORCING,
                                         .source =
                                             WL_COLUMNAR_MEMORY_SOURCE_ENV,
                                         .status = WL_COLUMNAR_MEMORY_OK};
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g); return g;
}
static uint64_t reserved(wl_columnar_memory_governor_ref_t *g)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}
static int64_t key(uint32_t row, uint32_t rows)
{
    return row == 0 ? INT64_MIN : row ==
           rows - 1 ? INT64_MAX : (int64_t)(row % 7) - 3;
}
static col_rel_t *source(uint32_t rows, uint32_t cols)
{
    col_rel_t *r = col_rel_new_auto("payload_join_fixture", cols); CHECK(r);
    wirelog_column_type_t *types = malloc((size_t)cols * sizeof(*types));
    CHECK(types);
    int64_t *lanes = malloc((size_t)cols * 8); CHECK(lanes);
    for (uint32_t c = 0; c < cols;
        c++) types[c] = c == 2 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
    CHECK(col_rel_set_column_types(r, types, cols) == 0);
    for (uint32_t i = 0; i < rows; i++) {
        for (uint32_t c = 0; c < cols;
            c++) lanes[c] = c == 0 ? key(i,
                    rows) : c == 1 ? i : (int64_t)i * 100 + c;
        if (cols > 2) {
            double value = i ? (double)i + .25 : -0.0;
            memcpy(&lanes[2], &value, 8);
        }
        CHECK(col_rel_append_row(r, lanes) == 0);
        if (cols > 2) memcpy(&r->columns[2][i], &lanes[2], 8);
    }
    free(lanes); free(types); return r;
}
static wl_columnar_batch_payload_t *capture(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, col_rel_t *r, unsigned mode)
{
    wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture(g, store, r, r->nrows,
        mode ==
        1 ? WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
        NULL, NULL, &p) == OK);
    if (mode == 2) CHECK(wl_columnar_batch_payload_spill(&p, store, NULL,
            NULL) == OK);
    return p;
}
/* Wrap the real owned-payload sink; faults occur at actual transaction edges. */
typedef struct {
    wl_columnar_join_batch_payload_sink_t owned;
    wl_columnar_continuation_sink_t inner;
    unsigned failure;
} sink_t;
static wl_columnar_continuation_status_t begin(void *v,
    const wl_columnar_continuation_batch_t *b)
{
    sink_t *s = v; return s->inner.begin(s->inner.context, b);
}
static wl_columnar_continuation_status_t reserve_sink(void *v, uint64_t bytes,
    uint32_t rows)
{
    sink_t *s = v;
    return s->failure == 4 ? WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
        : s->inner.reserve(s->inner.context, bytes, rows);
}
static wl_columnar_continuation_status_t append(void *v,
    const wl_columnar_continuation_batch_t *b)
{
    sink_t *s = v;
    return s->failure ==
           1 ? WL_COLUMNAR_CONTINUATION_SINK_FAILURE :
           s->inner.append(s->inner.context, b);
}
static wl_columnar_continuation_status_t commit(void *v, bool *committed)
{
    sink_t *s = v; *committed = false;
    if (s->failure == 2) return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE;
    wl_columnar_continuation_status_t rc = s->inner.commit(s->inner.context,
            committed);
    return s->failure == 3 ? WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE : rc;
}
static void abort_sink(void *v) {
    sink_t *s = v; s->inner.abort(s->inner.context);
}
static void sink_init(sink_t *s, wl_columnar_continuation_sink_t *sink,
    wl_columnar_memory_governor_ref_t *g)
{
    memset(s, 0, sizeof(*s));
    wl_columnar_join_batch_payload_sink_init(&s->owned, &s->inner, g, NULL,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT);
    *sink = (wl_columnar_continuation_sink_t){s, begin, reserve_sink, append,
                                              commit, abort_sink};
}
static unsigned drain(wl_columnar_continuation_t *join, sink_t *s,
    wl_columnar_continuation_sink_t *sink, uint32_t lr, uint32_t rr,
    uint32_t lc, uint32_t rc, bool *seen)
{
    unsigned total = 0, calls = 0;
    for (;;) {
        CHECK(++calls < 100000);
        wl_columnar_continuation_status_t status =
            wl_columnar_continuation_publish(join, sink);
        CHECK(status == WL_COLUMNAR_CONTINUATION_OK ||
            status == WL_COLUMNAR_CONTINUATION_DONE ||
            status == WL_COLUMNAR_CONTINUATION_PROGRESS ||
            status == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS);
        wl_columnar_batch_payload_t *out =
            wl_columnar_join_batch_payload_sink_take(&s->owned);
        if (out) {
            uint32_t cols, rows;
            const wirelog_column_type_t *types =
                wl_columnar_batch_payload_schema(out, &cols, &rows);
            CHECK(cols == lc + rc);
            if (lc > 2) CHECK(types[2] == WIRELOG_TYPE_FLOAT);
            if (rc > 2) CHECK(types[lc + 2] == WIRELOG_TYPE_FLOAT);
            wl_columnar_batch_payload_reader_t *reader;
            CHECK(wl_columnar_batch_payload_acquire(out, &reader) == OK);
            for (uint32_t row = 0; row < rows; row++) {
                const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                CHECK(wl_columnar_batch_payload_read(reader, row, NULL, NULL,
                    &lanes, &meta) == OK);
                uint32_t i = (uint32_t)lanes[1], j = (uint32_t)lanes[lc + 1];
                CHECK(i < lr && j < rr && key(i, lr) == key(j, rr));
                CHECK(!seen[(size_t)i * rr + j]);
                seen[(size_t)i * rr + j] = true;
                CHECK(meta.multiplicity == 1 && !meta.iteration &&
                    !meta.stratum && !meta.worker);
                for (unsigned side = 0; side < 2; side++) {
                    uint32_t ncols = side ? rc : lc, index = side ? j : i,
                        off = side ? lc : 0;
                    for (uint32_t c = 0; c < ncols; c++) {
                        uint64_t expected = c == 0 ? (uint64_t)key(index,
                                side ? rr : lr) : c ==
                            1 ? index : (uint64_t)index * 100 + c;
                        if (c == 2) {
                            double value = index ? (double)index + .25 : -0.0;
                            memcpy(&expected, &value, 8);
                        }
                        CHECK(lanes[off + c] == expected);
                    }
                }
                total++;
            }
            wl_columnar_batch_payload_reader_release(reader);
            CHECK(wl_columnar_batch_payload_release(out) == OK);
        }
        if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
    }
    for (uint32_t i = 0; i < lr; i++) for (uint32_t j = 0; j < rr; j++)
            CHECK(seen[(size_t)i * rr + j] == (key(i, lr) == key(j, rr)));
    CHECK(wl_columnar_payload_join_error(join) == OK);
    return total;
}
static void combinations(void)
{
    for (unsigned a = 0; a < 3; a++) for (unsigned b = 0; b < 3; b++) {
            wl_columnar_memory_governor_ref_t *g = governor(1 << 22);
            wl_columnar_spill_store_t *store;
            CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
                &store) == OK);
            col_rel_t *l = source(9, 3), *r = source(17, 4);
            wl_columnar_batch_payload_t *left = capture(g, store, l, a),
                *right = capture(g, store, r, b);
            col_rel_destroy(l); col_rel_destroy(r);
            uint64_t before = reserved(g);
            wl_columnar_continuation_t *join;
            CHECK(create_join(g, left, right, 0, 0,
                7 * 8 * 2, NULL, NULL, &join) == OK);
            CHECK(wl_columnar_batch_payload_release(left) ==
                WL_COLUMNAR_SPILL_BUSY);
            CHECK(wl_columnar_batch_payload_release(right) ==
                WL_COLUMNAR_SPILL_BUSY);
            if (!a) CHECK(wl_columnar_batch_payload_spill(&left, store, NULL,
                    NULL) == WL_COLUMNAR_SPILL_BUSY);
            sink_t s; wl_columnar_continuation_sink_t sink;
            sink_init(&s, &sink, g);
            wl_columnar_continuation_cursor_t cursor =
                *wl_columnar_continuation_cursor(join);
            for (unsigned fault = 1; fault <= 2; fault++) {
                s.failure = fault;
                CHECK(wl_columnar_continuation_publish(join,
                    &sink) ==
                    (fault ==
                    1 ? WL_COLUMNAR_CONTINUATION_SINK_FAILURE :
                    WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE));
                CHECK(!s.owned.staged && !s.owned.ready);
                CHECK(memcmp(&cursor, wl_columnar_continuation_cursor(join),
                    sizeof(cursor)) == 0);
            }
            s.failure = 3; /* Every durable commit returns ambiguous: never replay it. */
            bool seen[9 * 17] = {0};
            CHECK(drain(join, &s, &sink, 9, 17, 3, 4, seen) > 2);
            wl_columnar_continuation_destroy(join);
            CHECK(reserved(g) == before);
            wl_columnar_join_batch_payload_sink_cleanup(&s.owned);
            CHECK(wl_columnar_batch_payload_release(left) == OK);
            CHECK(wl_columnar_batch_payload_release(right) == OK);
            wl_columnar_spill_store_retire(store); CHECK(reserved(g) == 0);
            wl_columnar_memory_governor_ref_release(g);
        }
}
static bool cancelled(void *v) {
    return *(bool *)v;
}
static void failures(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20),
        *other = governor(1 << 20);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
        &store) == OK);
    col_rel_t *r = source(8, 3);
    wl_columnar_batch_payload_t *p = capture(g, store, r, 1);
    uint64_t before = reserved(g); wl_columnar_continuation_t *join = (void *)1;
    CHECK(create_join(other, p, p, 0, 0, 100, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_INVALID && !join);
    CHECK(create_join(g, p, p, 9, 0, 100, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_INVALID);
    CHECK(create_join(g, p, p, 2, 0, 100, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(create_join(g, p, p, 0, 0, 47, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_MEMORY);
    for (unsigned n = 1; n <= 2; n++) {
        allocation_fail = n;
        CHECK(create_join(g, p, p, 0, 0, 96, NULL, NULL,
            &join) == WL_COLUMNAR_SPILL_ALLOCATION);
        CHECK(!join && reserved(g) == before);
    }
    read_fail = true;
    CHECK(create_join(g, p, p, 0, 0, 96, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_IO);
    read_fail = false;
    CHECK(!join && reserved(g) == before);
    /* Sweep admission boundaries through descriptor, readers, index and output
     * construction without altering the governor or hiding pending charges. */
    bool admitted = false;
    for (uint64_t available = 0; available < 16384; available += 64) {
        wl_columnar_memory_reservation_t held;
        wl_columnar_memory_reservation_init(&held);
        CHECK(wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(g),
                (1 << 20) - before - available,
                &held) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        wl_columnar_spill_status_t status = create_join(g,
                p, p, 0, 0, 96, NULL, NULL, &join);
        if (status == OK) {
            wl_columnar_continuation_destroy(join); admitted = true;
        }else CHECK(status == WL_COLUMNAR_SPILL_MEMORY && !join);
        CHECK(reserved(g) == (1 << 20) - available);
        CHECK(wl_columnar_memory_rollback(&held)); CHECK(reserved(g) == before);
        if (admitted) break;
    }
    CHECK(admitted);
    bool stop = true;
    CHECK(create_join(g, p, p, 0, 0, 96, cancelled, &stop,
        &join) == WL_COLUMNAR_SPILL_CANCELLED);
    CHECK(!join && reserved(g) == before); stop = false;
    for (unsigned mode = 0; mode < 2; mode++) {
        CHECK(create_join(g, p, p, 0, 0, 96, cancelled,
            &stop, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        read_fail_after = mode == 0 ? 4 : 0; stop = mode == 1;
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(wl_columnar_payload_join_error(join) ==
            (mode ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_IO));
        read_fail = false; read_fail_after = 0; stop = false;
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(!s.owned.ready &&
            wl_columnar_continuation_cursor(join)->sequence == 0);
        wl_columnar_continuation_cancel(join);
        wl_columnar_continuation_destroy(join);
        CHECK(reserved(g) == before);
    }
    /* Metadata rejection checks the last row of each side, before publication. */
    CHECK(col_rel_enable_timestamps(r) == 0);
    for (uint32_t i = 0; i < 8;
        i++) r->timestamps[i] = (col_delta_timestamp_t){0, 0, 0, 0, 1};
    r->timestamps[7].multiplicity = -1;
    wl_columnar_batch_payload_t *bad = capture(g, store, r, 0);
    CHECK(create_join(g, p, bad, 0, 0, 96, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(create_join(g, bad, p, 0, 0, 96, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_batch_payload_release(bad) == OK); col_rel_destroy(r);
    CHECK(reserved(g) == before);
    wl_columnar_spill_store_retire(store);
    CHECK(create_join(g, p, p, 0, 0, 96, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_CLOSED);
    CHECK(!join && reserved(g) == before);
    CHECK(wl_columnar_batch_payload_release(p) == OK); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
    wl_columnar_memory_governor_ref_release(other);
}
static void budget(bool weighted)
{
    const uint64_t limit = 65536;
    wl_columnar_memory_governor_ref_t *g = governor(limit);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
        &store) == OK);
    /* External fixture preparation is not a claim of bounded ingestion. Its
     * source relation is destroyed before JOIN, without resetting the ledger. */
    col_rel_t *l = source(9, 3), *r = source(1024, 64);
    CHECK((uint64_t)r->nrows * r->ncols * 8 > limit);
    wl_columnar_batch_payload_t *left = capture(g, store, l, 0), *right = NULL;
    uint64_t before = reserved(g);
    CHECK(wl_columnar_batch_payload_capture(g, store, r, r->nrows,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, NULL, NULL,
        &right) == WL_COLUMNAR_SPILL_MEMORY);
    CHECK(!right && reserved(g) == before);
    right = capture(g, store, r, 1); col_rel_destroy(l); col_rel_destroy(r);
    wl_columnar_continuation_t *join;
    CHECK((weighted ? create_weighted_join :
        create_join)(g, left, right, 0, 0, 67 * 8 * 2,
        NULL, NULL, &join) == OK);
    CHECK(reserved(g) < limit);
    /* Refuse a second index/scratch while all remaining budget is held. */
    wl_columnar_memory_reservation_t slack;
    wl_columnar_memory_reservation_init(&slack);
    CHECK(wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(g), limit - reserved(g),
            &slack) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    wl_columnar_continuation_t *denied;
    CHECK((weighted ? create_weighted_join :
        create_join)(g, left, right, 0, 0, 67 * 8, NULL,
        NULL, &denied) == WL_COLUMNAR_SPILL_MEMORY && !denied);
    CHECK(wl_columnar_memory_rollback(&slack));
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    bool *seen = calloc(9 * 1024, sizeof(bool)); CHECK(seen);
    CHECK(drain(join, &s, &sink, 9, 1024, 3, 64, seen) > 100); free(seen);
    CHECK(reserved(g) < limit); wl_columnar_continuation_destroy(join);
    CHECK(wl_columnar_batch_payload_release(left) == OK);
    CHECK(wl_columnar_batch_payload_release(right) == OK);
    wl_columnar_spill_store_retire(store); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void misses(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    col_rel_t *l = source(600, 2), *r = source(1, 2);
    for (uint32_t i = 0; i < l->nrows; i++) l->columns[0][i] = 42;
    wl_columnar_batch_payload_t *left = capture(g, NULL, l, 0),
        *right = capture(g, NULL, r, 0);
    col_rel_destroy(l); col_rel_destroy(r);
    wl_columnar_continuation_t *join;
    CHECK(create_join(g, left, right, 0, 0, 32, NULL, NULL,
        &join) == OK);
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    unsigned progress = 0;
    for (;;) {
        wl_columnar_continuation_status_t status =
            wl_columnar_continuation_publish(join, &sink);
        if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
        CHECK(status == WL_COLUMNAR_CONTINUATION_PROGRESS);
        CHECK(++progress < 20);
        CHECK(!s.owned.ready &&
            wl_columnar_continuation_cursor(join)->position == 0);
    }
    CHECK(progress > 0); wl_columnar_continuation_destroy(join);
    CHECK(wl_columnar_batch_payload_release(left) == OK);
    CHECK(wl_columnar_batch_payload_release(right) == OK);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}
typedef struct {
    wl_columnar_continuation_t *join;
    sink_t sink_context;
    wl_columnar_continuation_sink_t sink;
} job_t;
static void *join_thread(void *v)
{
    job_t *job = v;
    bool seen[9 * 9] = {0};
    CHECK(drain(job->join, &job->sink_context, &job->sink, 9, 9, 3, 3,
        seen) >= 9);
    return NULL;
}
static void concurrent_self_join(bool weighted)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
        &store) == OK);
    col_rel_t *r = source(9, 3);
    wl_columnar_batch_payload_t *p = capture(g, store, r, 1);
    col_rel_destroy(r);
    job_t jobs[2]; wl_thread_t threads[2];
    for (unsigned i = 0; i < 2; i++) {
        CHECK((weighted ? create_weighted_join :
            create_join)(g, p, p, 0, 0, 96, NULL, NULL,
            &jobs[i].join) == OK);
        sink_init(&jobs[i].sink_context, &jobs[i].sink, g);
    }
    wl_columnar_spill_store_retire(store); /* Existing four readers remain valid. */
    CHECK(wl_columnar_batch_payload_release(p) == WL_COLUMNAR_SPILL_BUSY);
    for (unsigned i = 0; i < 2;
        i++) CHECK(wl_thread_create(&threads[i], join_thread, &jobs[i]) == 0);
    for (unsigned i = 0; i < 2; i++) wl_thread_join(&threads[i]);
    for (unsigned i = 0; i < 2;
        i++) wl_columnar_continuation_destroy(jobs[i].join);
    CHECK(wl_columnar_batch_payload_release(p) == OK); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void unsupported_and_empty(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_intern_t *intern = wl_intern_create(); CHECK(intern);
    CHECK(wl_intern_attach_memory_governor(intern, g) == 0);
    CHECK(wl_intern_put(intern, "unary") == 0);
    col_rel_t *r = source(2, 2);
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_STRING};
    CHECK(col_rel_set_column_types(r, types, 2) == 0);
    r->columns[1][0] = 0; r->columns[1][1] = 0;
    wl_columnar_batch_payload_t *p;
    CHECK(wl_columnar_batch_payload_capture_with_intern(g, NULL, r, 2,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, intern, NULL, NULL, &p) == OK);
    wl_columnar_continuation_t *join;
    CHECK(create_join(g, p, p, 0, 0, 64, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    types[1] = WIRELOG_TYPE_INT64;
    CHECK(col_rel_set_column_types(r, types, 2) == 0);
    col_rel_logical_col_t layout[] = {{WIRELOG_COMPOUND_KIND_NONE, 0, 0},
                                      {WIRELOG_COMPOUND_KIND_INLINE, 1, 1}};
    CHECK(col_rel_apply_compound_schema(r, layout, 2) == 0);
    wirelog_column_t columns[] = {{.type = WIRELOG_TYPE_INT64},
                                  {.type = WIRELOG_TYPE_INT64,
                                   .compound_kind =
                                       WIRELOG_COMPOUND_KIND_INLINE,
                                   .compound_arity = 1,
                                   .compound_inline_col_offset = 1}};
    CHECK(wl_columnar_batch_payload_capture_with_inline(g, NULL, r, 2,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, columns, 2, intern, NULL, NULL,
        &p) == OK);
    CHECK(create_join(g, p, p, 0, 0, 64, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_batch_payload_release(p) == OK); col_rel_destroy(r);
    wl_intern_free(intern);
    r = source(0, 2);
    wl_columnar_batch_payload_t *empty = capture(g, NULL, r, 0);
    col_rel_destroy(r);
    r = source(1, 2); p = capture(g, NULL, r, 0); col_rel_destroy(r);
    for (unsigned side = 0; side < 2; side++) {
        CHECK(create_join(g, side ? p : empty,
            side ? empty : p,
            0, 0, 64, NULL, NULL, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_DONE);
        CHECK(!s.owned.ready); wl_columnar_continuation_destroy(join);
    }
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    CHECK(wl_columnar_batch_payload_release(empty) == OK);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

/* Safe small products below are an independent oracle; the old weighted JOIN
 * is supplementary evidence only and is never invoked on overflow fixtures. */
static void weights(col_rel_t *r, const int64_t *values)
{
    CHECK(col_rel_enable_timestamps(r) == 0);
    for (uint32_t i = 0; i < r->nrows; i++)
        r->timestamps[i] = (col_delta_timestamp_t){.multiplicity = values[i]};
}
static void weighted_storage(void)
{
    for (unsigned a = 0; a < 3; a++) for (unsigned b = 0; b < 3; b++)
            for (unsigned output_mode = 0; output_mode < 3; output_mode++) {
                wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
                wl_columnar_spill_store_t *store;
                CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22,
                    1024, &store) == OK);
                col_rel_t *l = source(4, 3), *r = source(3, 2);
                int64_t lw[] = {1, -1, -3, 0}, rw[] = {2, -2, 0};
                weights(l, lw); weights(r, rw);
                /* Same tuples with opposite signs must cancel by aggregation;
                 * the raw producer still emits every pair, including zero weights. */
                for (uint32_t i = 0; i < 4; i++) {
                    l->columns[0][i] = 7; l->columns[1][i] = 11;
                    l->columns[2][i] = l->columns[2][0];
                }
                for (uint32_t i = 0; i < 3; i++) {
                    r->columns[0][i] = 7; r->columns[1][i] = 13;
                }
                int64_t expected[12]; bool used[12] = {0};
                for (unsigned i = 0; i < 4; i++) for (unsigned j = 0; j < 3;
                        j++) expected[i * 3 + j] = lw[i] * rw[j];
                col_rel_t reference_storage = {0};
                col_rel_t *reference = &reference_storage;
                CHECK(col_op_join_weighted(l, r, 0,
                    reference) == 0 && reference->nrows == 12);
                for (unsigned i = 0; i < 12;
                    i++) CHECK(reference->timestamps[i].multiplicity ==
                        expected[i]);
                /* This legacy helper fills a caller-zeroed descriptor, without a
                 * registered relation identity. Match its fixture ownership. */
                col_columns_free(reference->columns, reference->ncols);
                free(reference->timestamps);
                wl_columnar_batch_payload_t *left = capture(g, store, l, a),
                    *right = capture(g, store, r, b);
                uint64_t float_bits; memcpy(&float_bits, &l->columns[2][0], 8);
                col_rel_destroy(l); col_rel_destroy(r);
                wl_columnar_continuation_t *join;
                CHECK(create_join(g, left, right, 0, 0, 80,
                    NULL, NULL, &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
                CHECK(create_weighted_join(g, left, right,
                    0, 0, 80, NULL, NULL, &join) == OK);
                sink_t s; wl_columnar_continuation_sink_t sink;
                sink_init(&s, &sink, g);
                s.owned.store = store;
                s.owned.storage = output_mode ==
                    1 ? WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
                    WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT;
                for (unsigned fault = 1; fault <= 2; fault++) {
                    s.failure = fault;
                    CHECK(wl_columnar_continuation_publish(join,
                        &sink) ==
                        (fault ==
                        1 ? WL_COLUMNAR_CONTINUATION_SINK_FAILURE :
                        WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE));
                    CHECK(!s.owned.ready && !s.owned.staged &&
                        wl_columnar_continuation_cursor(join)->sequence == 0);
                }
                s.failure = 3;
                wl_columnar_batch_payload_t *outputs[6];
                wl_columnar_batch_payload_reader_t *readers[6];
                unsigned count = 0;
                for (;;) {
                    wl_columnar_continuation_status_t status =
                        wl_columnar_continuation_publish(join, &sink);
                    if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
                    CHECK(status == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS ||
                        status == WL_COLUMNAR_CONTINUATION_PROGRESS);
                    wl_columnar_batch_payload_t *out =
                        wl_columnar_join_batch_payload_sink_take(&s.owned);
                    if (!out) continue;
                    CHECK(count < 6); outputs[count] = out;
                    if (output_mode ==
                        2) CHECK(wl_columnar_batch_payload_spill(
                                &outputs[count], store, NULL, NULL) == OK);
                    CHECK(wl_columnar_batch_payload_acquire(outputs[count],
                        &readers[count]) == OK); count++;
                }
                CHECK(count == 6); wl_columnar_continuation_destroy(join);
                CHECK(wl_columnar_batch_payload_release(left) == OK &&
                    wl_columnar_batch_payload_release(right) == OK);
                wl_columnar_spill_store_retire(store);
                int64_t sum = 0; unsigned rows_seen = 0;
                for (unsigned k = 0; k < count; k++) {
                    uint32_t cols, rows;
                    const wirelog_column_type_t *types =
                        wl_columnar_batch_payload_schema(outputs[k], &cols,
                            &rows);
                    CHECK(cols == 5 && rows == 2 &&
                        types[2] == WIRELOG_TYPE_FLOAT);
                    for (uint32_t row = 0; row < rows; row++) {
                        const uint64_t *lanes;
                        wl_columnar_spill_metadata_t meta;
                        CHECK(wl_columnar_batch_payload_read(readers[k], row,
                            NULL, NULL, &lanes, &meta) == OK);
                        CHECK(lanes[0] == 7 && lanes[1] == 11 &&
                            lanes[2] == float_bits && lanes[3] == 7 &&
                            lanes[4] == 13);
                        CHECK(!meta.iteration && !meta.stratum && !meta.worker);
                        unsigned i;
                        for (i = 0; i < 12;
                            i++) if (!used[i] &&
                                expected[i] == meta.multiplicity){
                                used[i] = true; break;
                            }
                        CHECK(i < 12); sum += meta.multiplicity; rows_seen++;
                    }
                    wl_columnar_batch_payload_reader_release(readers[k]);
                    CHECK(wl_columnar_batch_payload_release(outputs[k]) == OK);
                }
                CHECK(rows_seen == 12 && sum == 0 && reserved(g) == 0);
                wl_columnar_memory_governor_ref_release(g);
            }
}
static void weighted_boundaries(void)
{
    const int64_t lhs[] = {INT64_MIN, INT64_MAX, INT64_MIN, -1, INT64_MAX,
                           INT64_MIN, -3, 0};
    const int64_t rhs[] = {1, 1, -1, INT64_MIN, 2, 0, -2, INT64_MIN};
    const int64_t results[] = {INT64_MIN, INT64_MAX, 0, 0, 0, 0, 6, 0};
    for (unsigned k = 0; k < 8; k++) for (unsigned mismatch = 0; mismatch < 2;
            mismatch++){
            wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
            col_rel_t *l = source(1, 2), *r = source(1, 2); weights(l, &lhs[k]);
            weights(r, &rhs[k]);
            if (mismatch) r->columns[0][0] = 7;
            wl_columnar_batch_payload_t *left = capture(g, NULL, l, 0),
                *right = capture(g, NULL, r, 0);
            col_rel_destroy(l); col_rel_destroy(r);
            wl_columnar_continuation_t *join;
            CHECK(create_weighted_join(g, left, right, 0, 0,
                32, NULL, NULL, &join) == OK);
            sink_t s; wl_columnar_continuation_sink_t sink;
            sink_init(&s, &sink, g);
            wl_columnar_continuation_status_t status =
                wl_columnar_continuation_publish(join, &sink);
            bool overflow = k >= 2 && k <= 4;
            if (mismatch) CHECK(status == WL_COLUMNAR_CONTINUATION_DONE &&
                    !s.owned.ready);
            else if (overflow) {
                CHECK(status == WL_COLUMNAR_CONTINUATION_INVALID &&
                    !s.owned.ready);
                CHECK(wl_columnar_payload_join_error(join) ==
                    WL_COLUMNAR_SPILL_OVERFLOW);
                CHECK(wl_columnar_continuation_publish(join,
                    &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
            } else {
                CHECK(status == WL_COLUMNAR_CONTINUATION_OK);
                wl_columnar_batch_payload_t *out =
                    wl_columnar_join_batch_payload_sink_take(&s.owned);
                wl_columnar_batch_payload_reader_t *reader;
                CHECK(wl_columnar_batch_payload_acquire(out, &reader) == OK);
                const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL,
                    &lanes, &meta) == OK && meta.multiplicity == results[k]);
                wl_columnar_batch_payload_reader_release(reader);
                CHECK(wl_columnar_batch_payload_release(out) == OK);
            }
            wl_columnar_continuation_destroy(join);
            CHECK(wl_columnar_batch_payload_release(left) == OK &&
                wl_columnar_batch_payload_release(right) == OK);
            CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
        }
}
static void weighted_partial_overflow(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
        &store) == OK);
    col_rel_t *l = source(4, 2), *r = source(4, 2);
    const int64_t lw[] = {2, -3, 4, INT64_MAX}, rw[] = {1, 1, 1, 2};
    weights(l, lw); weights(r, rw);
    for (uint32_t i = 0; i < 4; i++) l->columns[0][i] = r->columns[0][i] = i;
    wl_columnar_batch_payload_t *left = capture(g, store, l, 1),
        *right = capture(g, store, r, 1);
    col_rel_destroy(l); col_rel_destroy(r);
    wl_columnar_continuation_t *join;
    CHECK(create_weighted_join(g, left, right, 0, 0, 64,
        NULL, NULL, &join) == OK);
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    CHECK(wl_columnar_continuation_publish(join,
        &sink) == WL_COLUMNAR_CONTINUATION_OK);
    wl_columnar_batch_payload_t *first =
        wl_columnar_join_batch_payload_sink_take(&s.owned);
    wl_columnar_continuation_cursor_t cursor =
        *wl_columnar_continuation_cursor(join);
    CHECK(wl_columnar_continuation_publish(join,
        &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
    CHECK(wl_columnar_payload_join_error(join) == WL_COLUMNAR_SPILL_OVERFLOW &&
        !s.owned.ready && !s.owned.staged);
    CHECK(memcmp(&cursor, wl_columnar_continuation_cursor(join),
        sizeof(cursor)) == 0);
    CHECK(wl_columnar_continuation_publish(join,
        &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
    wl_columnar_continuation_destroy(join);
    wl_columnar_batch_payload_reader_t *reader;
    CHECK(wl_columnar_batch_payload_acquire(first, &reader) == OK);
    for (uint32_t row = 0; row < 2; row++) {
        const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
        CHECK(wl_columnar_batch_payload_read(reader, row, NULL, NULL, &lanes,
            &meta) == OK);
        CHECK(lanes[0] == row && meta.multiplicity == lw[row]);
    }
    wl_columnar_batch_payload_reader_release(reader);
    CHECK(wl_columnar_batch_payload_release(first) == OK);
    CHECK(wl_columnar_batch_payload_release(left) == OK &&
        wl_columnar_batch_payload_release(right) == OK);
    wl_columnar_spill_store_retire(store); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void weighted_coordinates(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    col_rel_t *r = source(3, 2); const int64_t values[] = {1, -1, 0};
    weights(r, values);
    wl_columnar_batch_payload_t *plain = capture(g, NULL, r, 0);
    for (unsigned field = 0; field < 3; field++) {
        r->timestamps[2] = (col_delta_timestamp_t){.multiplicity = 1};
        if (field == 0) r->timestamps[2].iteration = 1;
        if (field == 1) r->timestamps[2].stratum = 1;
        if (field == 2) r->timestamps[2].worker = 1;
        r->columns[0][2] = 99; /* Unsupported final row has no matching key. */
        wl_columnar_batch_payload_t *bad = capture(g, NULL, r, 0);
        uint64_t before = reserved(g); wl_columnar_continuation_t *join;
        CHECK(create_weighted_join(g, plain, bad, 0, 0, 64,
            NULL, NULL, &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
        CHECK(create_weighted_join(g, bad, plain, 0, 0, 64,
            NULL, NULL, &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
        CHECK(reserved(g) == before);
        CHECK(wl_columnar_batch_payload_release(bad) == OK);
    }
    col_rel_destroy(r); CHECK(wl_columnar_batch_payload_release(plain) == OK);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}
static bool weighted_admit_at(wl_columnar_memory_governor_ref_t *g,
    wl_columnar_batch_payload_t *p, uint64_t available, bool weighted)
{
    uint64_t before = reserved(g);
    wl_columnar_memory_reservation_t held;
    wl_columnar_memory_reservation_init(&held);
    CHECK(wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(g),
            (1 << 20) - before - available,
            &held) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    wl_columnar_continuation_t *join;
    wl_columnar_spill_status_t status =
        (weighted ? create_weighted_join :
        create_join)(g, p, p, 0, 0, 32 * 64, NULL, NULL,
            &join);
    if (status == OK) wl_columnar_continuation_destroy(join);
    else CHECK(status == WL_COLUMNAR_SPILL_MEMORY && !join);
    CHECK(reserved(g) == (1 << 20) - available);
    CHECK(wl_columnar_memory_rollback(&held) && reserved(g) == before);
    return status == OK;
}
static void weighted_floor(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    col_rel_t *r = source(1, 2);
    wl_columnar_batch_payload_t *p = capture(g, NULL, r, 0); col_rel_destroy(r);
    uint64_t floor[2];
    for (unsigned mode = 0; mode < 2; mode++) {
        uint64_t lo = 0, hi = 65536;
        CHECK(weighted_admit_at(g, p, hi, mode != 0));
        while (lo < hi) {
            uint64_t mid = lo + (hi - lo) / 2;
            if (weighted_admit_at(g, p, mid, mode != 0)) hi = mid;
            else lo = mid + 1;
        }
        floor[mode] = lo;
        CHECK(lo && !weighted_admit_at(g, p, lo - 1, mode != 0));
        CHECK(weighted_admit_at(g, p, lo, mode != 0));
    }
    CHECK(floor[1] >= floor[0] + 64 * sizeof(col_delta_timestamp_t));
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void weighted_stop(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
        &store) == OK);
    col_rel_t *r = source(2, 2); const int64_t values[] = {-1, 2};
    weights(r, values);
    wl_columnar_batch_payload_t *p = capture(g, store, r, 1);
    col_rel_destroy(r);
    uint64_t before = reserved(g);
    for (unsigned mode = 0; mode < 2; mode++) {
        bool stop = false; wl_columnar_continuation_t *join;
        CHECK(create_weighted_join(g, p, p, 0, 0, 64,
            cancelled, &stop, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        read_fail_after = mode == 0 ? 4 : 0; stop = mode == 1;
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(wl_columnar_payload_join_error(join) ==
            (mode ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_IO));
        CHECK(!s.owned.ready && !s.owned.staged);
        read_fail_after = 0; stop = false;
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        wl_columnar_continuation_cancel(join);
        wl_columnar_continuation_destroy(join);
        CHECK(reserved(g) == before);
    }
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    wl_columnar_spill_store_retire(store);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

typedef struct {
    uint32_t rows, calls;
    bool weighted, left;
    unsigned fault;
} input_stream_t;
static wl_columnar_spill_status_t input_stream(void *v, uint64_t ordinal,
    uint64_t *lanes, wl_columnar_spill_metadata_t *meta)
{
    input_stream_t *s = v;
    CHECK(ordinal == s->calls++ && ordinal < s->rows);
    if (s->fault && ordinal == 3) {
        if (s->fault == 1) return WL_COLUMNAR_SPILL_IO;
        write_error = s->fault == 2 ? EIO : ENOSPC;
    }
    lanes[0] = !s->left && ordinal == s->rows - 1 ? 99 : 7;
    lanes[1] = ordinal;
    *meta = (wl_columnar_spill_metadata_t){.multiplicity = s->weighted
        ? s->left ? -2 : (int64_t)(ordinal % 3) - 1 : 1};
    return OK;
}

/* Narrow build rows make the full ordinal index alone exceed the allowance.
 * Verification uses constant state and releases each output before proceeding. */
static void window_budget(bool weighted, bool streamed)
{
    const uint64_t limit = 65536;
    const uint32_t rows = 16385, window = 127;
    CHECK(index_cap(rows) > limit);
    wl_columnar_memory_governor_ref_t *g = governor(limit);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
        &store) == OK);
    wl_columnar_batch_payload_t *left, *right;
    if (streamed) {
        const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                               WIRELOG_TYPE_INT64};
        input_stream_t lhs = {.rows = 1, .weighted = weighted, .left = true},
                rhs = {.rows = rows, .weighted = weighted};
        CHECK(wl_columnar_batch_payload_create_spilled(g, store, types, 2, 1,
            input_stream, &lhs, NULL, NULL, &left) == OK);
        CHECK(wl_columnar_batch_payload_create_spilled(g, store, types, 2, rows,
            input_stream, &rhs, NULL, NULL, &right) == OK);
        CHECK(lhs.calls == 1 && rhs.calls == rows && reserved(g) <= limit);
    } else {
        col_rel_t *l = source(1, 2), *r = source(rows, 2);
        l->columns[0][0] = 7;
        if (weighted) {
            const int64_t weight = -2; weights(l, &weight);
            CHECK(col_rel_enable_timestamps(r) == 0);
        }
        for (uint32_t i = 0; i < rows; i++) {
            r->columns[0][i] = 7;
            if (weighted) r->timestamps[i] = (col_delta_timestamp_t){
                    .multiplicity = (int64_t)(i % 3) - 1
                };
        }
        left = capture(g, store, l, 1);
        right = capture(g, store, r, 1);
        col_rel_destroy(l); col_rel_destroy(r);
    }
    const uint32_t matches = streamed ? rows - 1 : rows;
    uint64_t before = reserved(g); wl_columnar_continuation_t *join;
    CHECK((weighted ? wl_columnar_payload_join_create_weighted :
        wl_columnar_payload_join_create)(g, left, right, 0, 0, 32 * 64,
        NULL, NULL, &join) == WL_COLUMNAR_SPILL_MEMORY && !join);
    CHECK(reserved(g) == before);
    if (streamed) {
        wl_columnar_memory_reservation_t held;
        wl_columnar_memory_reservation_init(&held);
        CHECK(wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(g),
                limit - before, &held) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
        CHECK(wl_columnar_payload_join_create_weighted_bounded(g, left, right,
            0, 0, 32, index_cap(1), NULL, NULL,
            &join) == WL_COLUMNAR_SPILL_MEMORY && !join);
        CHECK(reserved(g) == limit && wl_columnar_memory_rollback(&held));
        CHECK(reserved(g) == before);
    }
    CHECK((weighted ? wl_columnar_payload_join_create_weighted_bounded :
        wl_columnar_payload_join_create_bounded)(g, left, right, 0, 0,
        32 * 64, index_cap(window), NULL, NULL, &join) == OK);
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    uint32_t count = 0, calls = 0;
    for (;;) {
        CHECK(++calls < 10000);
        wl_columnar_continuation_status_t status =
            wl_columnar_continuation_publish(join, &sink);
        CHECK(status == WL_COLUMNAR_CONTINUATION_OK ||
            status == WL_COLUMNAR_CONTINUATION_PROGRESS ||
            status == WL_COLUMNAR_CONTINUATION_DONE);
        wl_columnar_batch_payload_t *out =
            wl_columnar_join_batch_payload_sink_take(&s.owned);
        if (out) {
            uint32_t n; wl_columnar_batch_payload_schema(out, NULL, &n);
            wl_columnar_batch_payload_reader_t *reader;
            CHECK(wl_columnar_batch_payload_acquire(out, &reader) == OK);
            CHECK(reserved(g) <= limit);
            for (uint32_t i = 0; i < n; i++) {
                CHECK(count < matches);
                uint32_t start = count / window * window;
                uint32_t end = matches - start <
                    window ? matches : start + window;
                uint32_t expected = end - 1 - count % window;
                const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                CHECK(wl_columnar_batch_payload_read(reader, i, NULL, NULL,
                    &lanes, &meta) == OK);
                CHECK(lanes[0] == 7 && lanes[1] == 0 && lanes[2] == 7 &&
                    lanes[3] == expected);
                CHECK(meta.multiplicity == (weighted
                    ? -2 * ((int64_t)(expected % 3) - 1) : 1));
                CHECK(!meta.iteration && !meta.stratum && !meta.worker);
                count++;
            }
            wl_columnar_batch_payload_reader_release(reader);
            CHECK(wl_columnar_batch_payload_release(out) == OK);
        }
        if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
    }
    CHECK(count == matches && wl_columnar_payload_join_error(join) == OK);
    wl_columnar_continuation_destroy(join); CHECK(reserved(g) == before);
    CHECK(wl_columnar_batch_payload_release(left) == OK);
    CHECK(wl_columnar_batch_payload_release(right) == OK);
    wl_columnar_spill_store_retire(store); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void streaming_unwind(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(65536);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
        &store) == OK);
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_INT64};
    input_stream_t lhs = {.rows = 1, .left = true};
    wl_columnar_batch_payload_t *left, *right;
    CHECK(wl_columnar_batch_payload_create_spilled(g, store, types, 2, 1,
        input_stream, &lhs, NULL, NULL, &left) == OK);
    uint64_t before = reserved(g), disk = wl_columnar_spill_store_bytes(store);
    for (unsigned mode = 0; mode < 5; mode++) {
        input_stream_t rhs = {.rows = 8, .fault = mode == 4 ? 0 : mode};
        short_writes = true;
        wl_columnar_spill_status_t status =
            wl_columnar_batch_payload_create_spilled(g,
                store, types, 2, 8, input_stream, &rhs, NULL, NULL, &right);
        short_writes = false; write_error = 0;
        if (!rhs.fault) {
            CHECK(status == OK && rhs.calls == 8);
            wl_columnar_batch_payload_reader_t *reader;
            CHECK(wl_columnar_batch_payload_acquire(right, &reader) == OK);
            for (uint32_t i = 0; i < 8; i++) {
                const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                CHECK(wl_columnar_batch_payload_read(reader, i, NULL, NULL,
                    &lanes, &meta) == OK);
                CHECK(lanes[0] == (i == 7 ? 99u : 7u) && lanes[1] == i &&
                    meta.multiplicity == 1);
            }
            wl_columnar_batch_payload_reader_release(reader);
            CHECK(wl_columnar_batch_payload_release(right) == OK);
        } else CHECK(status ==
                (mode == 3 ? WL_COLUMNAR_SPILL_CAPACITY : WL_COLUMNAR_SPILL_IO)
                && !right && rhs.calls == 4);
        CHECK(reserved(g) == before &&
            wl_columnar_spill_store_bytes(store) == disk);
        wl_columnar_batch_payload_reader_t *reader;
        CHECK(wl_columnar_batch_payload_acquire(left, &reader) == OK);
        const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
        CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL, &lanes,
            &meta) == OK);
        CHECK(lanes[0] == 7 && !lanes[1] && meta.multiplicity == 1);
        wl_columnar_batch_payload_reader_release(reader);
    }
    CHECK(wl_columnar_batch_payload_release(left) == OK);
    CHECK(wl_columnar_spill_store_close(store) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void window_limits(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    col_rel_t *r = source(5, 2);
    wl_columnar_batch_payload_t *p = capture(g, NULL, r, 0);
    col_rel_destroy(r);
    uint64_t before = reserved(g), fixed = 0;
    wl_columnar_continuation_t *join;
    for (uint64_t cap = 0; cap <= index_cap(5) + 1; cap++) {
        wl_columnar_spill_status_t status =
            wl_columnar_payload_join_create_bounded(g, p, p, 0, 0, 32,
                cap, NULL, NULL, &join);
        if (cap < index_cap(1)) CHECK(status == WL_COLUMNAR_SPILL_MEMORY &&
                !join);
        else {
            CHECK(status == OK);
            uint32_t rows = 1;
            while (rows < 5 && index_cap(rows + 1) <= cap) rows++;
            if (!fixed) fixed = reserved(g) - before - index_cap(rows);
            CHECK(reserved(g) - before == fixed + index_cap(rows));
            wl_columnar_continuation_destroy(join);
        }
        CHECK(reserved(g) == before);
    }
    CHECK(wl_columnar_payload_join_create_bounded(g, p, p, 0, 0, 32,
        UINT64_MAX, NULL, NULL, &join) == OK);
    CHECK(reserved(g) - before == fixed + index_cap(5));
    wl_columnar_continuation_destroy(join);
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    r = source(0, 2); p = capture(g, NULL, r, 0); col_rel_destroy(r);
    CHECK(wl_columnar_payload_join_create_bounded(g, p, p, 0, 0, 32,
        index_cap(0) - 1, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_MEMORY && !join);
    CHECK(wl_columnar_payload_join_create_bounded(g, p, p, 0, 0, 32,
        index_cap(0), NULL, NULL, &join) == OK);
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    CHECK(wl_columnar_continuation_publish(join,
        &sink) == WL_COLUMNAR_CONTINUATION_DONE);
    wl_columnar_continuation_destroy(join);
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* Two calls clear 512 buckets; the next call performs bounded build work.
 * No output is published while initialization yields. */
static void window_build_progress(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
        &store) == OK);
    col_rel_t *r = source(600, 2);
    wl_columnar_batch_payload_t *p = capture(g, store, r, 1);
    col_rel_destroy(r);
    uint64_t before = reserved(g);
    for (unsigned mode = 0; mode < 3; mode++) {
        bool stop = false; wl_columnar_continuation_t *join;
        CHECK(wl_columnar_payload_join_create_bounded(g, p, p, 0, 0, 32,
            index_cap(300), cancelled, &stop, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        /* Clearing must not read any row, or this injected fault fires early. */
        read_fail = true;
        for (unsigned i = 0; i < 2; i++) {
            CHECK(wl_columnar_continuation_publish(join, &sink) ==
                WL_COLUMNAR_CONTINUATION_PROGRESS);
            CHECK(!s.owned.ready &&
                !wl_columnar_continuation_cursor(join)->sequence);
        }
        if (mode != 0) read_fail = false;
        if (mode == 1) stop = true; /* Cancellation at CLEAR -> BUILD. */
        if (mode == 2) {
            CHECK(wl_columnar_continuation_publish(join, &sink) ==
                WL_COLUMNAR_CONTINUATION_PROGRESS);
            CHECK(!s.owned.ready);
            stop = true; /* Cancellation during a partially populated index. */
        }
        CHECK(wl_columnar_continuation_publish(join, &sink) ==
            WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(wl_columnar_payload_join_error(join) ==
            (mode ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_IO));
        read_fail = false; stop = false;
        CHECK(wl_columnar_continuation_publish(join, &sink) ==
            WL_COLUMNAR_CONTINUATION_INVALID);
        wl_columnar_continuation_cancel(join);
        wl_columnar_continuation_destroy(join);
        CHECK(reserved(g) == before);
    }
    CHECK(wl_columnar_batch_payload_release(p) == OK);
    wl_columnar_spill_store_retire(store); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void window_misses(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    col_rel_t *r = source(600, 2);
    for (uint32_t i = 0; i < r->nrows; i++) r->columns[0][i] = -1;
    wl_columnar_batch_payload_t *p = capture(g, NULL, r, 0);
    col_rel_destroy(r);
    wl_columnar_continuation_t *join;
    CHECK(wl_columnar_payload_join_create_bounded(g, p, p, 1, 0, 32,
        index_cap(300), NULL, NULL, &join) == OK);
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    unsigned progress = 0;
    for (;;) {
        wl_columnar_continuation_status_t status =
            wl_columnar_continuation_publish(join, &sink);
        CHECK(!s.owned.ready &&
            !wl_columnar_continuation_cursor(join)->sequence);
        if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
        CHECK(status == WL_COLUMNAR_CONTINUATION_PROGRESS &&
            ++progress < 10000);
    }
    CHECK(progress > 4 && wl_columnar_payload_join_error(join) == OK);
    wl_columnar_continuation_destroy(join);
    CHECK(wl_columnar_batch_payload_release(p) == OK && reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* A one-row output and one-row index put each publication in another window. */
static void window_publication(void)
{
    for (unsigned mode = 0; mode < 4; mode++) {
        wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
        wl_columnar_spill_store_t *store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 1024,
            &store) == OK);
        col_rel_t *l = source(1, 2), *r = source(3, 2);
        const int64_t lw = 2, rw[] = {1, -1, 0};
        weights(l, &lw); weights(r, rw); l->columns[0][0] = 7;
        for (uint32_t i = 0; i < 3; i++) r->columns[0][i] = 7;
        if (mode == 3) r->timestamps[1].multiplicity = INT64_MAX;
        wl_columnar_batch_payload_t *left = capture(g, store, l, 1),
            *right = capture(g, store, r, 1);
        col_rel_destroy(l); col_rel_destroy(r);
        bool stop = false; wl_columnar_continuation_t *join;
        CHECK(wl_columnar_payload_join_create_weighted_bounded(g, left, right,
            0, 0, 32, index_cap(1), cancelled, &stop, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        wl_columnar_batch_payload_t *outputs[3] = {0};
        for (unsigned i = 0; i < (mode ? 1u : 3u); i++) {
            wl_columnar_continuation_cursor_t cursor =
                *wl_columnar_continuation_cursor(join);
            for (unsigned fault = 1; fault <= 2; fault++) {
                s.failure = fault;
                CHECK(wl_columnar_continuation_publish(join, &sink) ==
                    (fault == 1 ? WL_COLUMNAR_CONTINUATION_SINK_FAILURE :
                    WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE));
                CHECK(!s.owned.ready && !s.owned.staged);
                CHECK(!memcmp(&cursor, wl_columnar_continuation_cursor(join),
                    sizeof(cursor)));
            }
            s.failure = 3;
            CHECK(wl_columnar_continuation_publish(join, &sink) ==
                WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS);
            outputs[i] = wl_columnar_join_batch_payload_sink_take(&s.owned);
            CHECK(outputs[i]);
        }
        wl_columnar_continuation_cursor_t cursor =
            *wl_columnar_continuation_cursor(join);
        s.failure = 0;
        read_fail = mode == 1; stop = mode == 2;
        CHECK(wl_columnar_continuation_publish(join, &sink) ==
            (mode ? WL_COLUMNAR_CONTINUATION_INVALID :
            WL_COLUMNAR_CONTINUATION_DONE));
        CHECK(!s.owned.ready && !s.owned.staged);
        if (mode) {
            CHECK(wl_columnar_payload_join_error(join) ==
                (mode == 1 ? WL_COLUMNAR_SPILL_IO : mode == 2
                 ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_OVERFLOW));
            CHECK(!memcmp(&cursor, wl_columnar_continuation_cursor(join),
                sizeof(cursor)));
            read_fail = false; stop = false;
            CHECK(wl_columnar_continuation_publish(join, &sink) ==
                WL_COLUMNAR_CONTINUATION_INVALID);
        }
        wl_columnar_continuation_destroy(join);
        CHECK(wl_columnar_batch_payload_release(left) == OK);
        CHECK(wl_columnar_batch_payload_release(right) == OK);
        wl_columnar_spill_store_retire(store);
        for (unsigned i = 0; i < (mode ? 1u : 3u); i++) {
            wl_columnar_batch_payload_reader_t *reader;
            CHECK(wl_columnar_batch_payload_acquire(outputs[i], &reader) == OK);
            const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
            CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL,
                &lanes, &meta) == OK);
            CHECK(lanes[3] == i && meta.multiplicity == 2 * rw[i]);
            wl_columnar_batch_payload_reader_release(reader);
            CHECK(wl_columnar_batch_payload_release(outputs[i]) == OK);
        }
        CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
    }
}

static col_rel_t *projection_source(uint32_t rows, uint64_t offset)
{
    col_rel_t *r = col_rel_new_auto("projected_source", 3); CHECK(r);
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                     WIRELOG_TYPE_UINT64, WIRELOG_TYPE_FLOAT};
    CHECK(col_rel_set_column_types(r, types, 3) == 0);
    for (uint32_t i = 0; i < rows; i++) {
        int64_t lanes[] = {key(i, rows), 0, 0};
        uint64_t value = UINT64_MAX - offset - i;
        double real = i ? (double)i + 0.25 : -0.0;
        memcpy(&lanes[1], &value, 8); memcpy(&lanes[2], &real, 8);
        CHECK(col_rel_append_row(r, lanes) == 0);
        /* Preserve raw -0 in this immutable fixture; append canonicalizes it. */
        memcpy(&r->columns[2][i], &real, 8);
    }
    return r;
}

static void projected(void)
{
    for (unsigned mode = 0; mode < 5; mode++) {
        wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
        wl_columnar_spill_store_t *store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 65536, 64,
            &store) == OK);
        col_rel_t *l = projection_source(9, 0), *r = projection_source(11, 100);
        wl_columnar_batch_payload_t *lp = capture(g, store, l, mode % 2);
        wl_columnar_batch_payload_t *rp = capture(g, store, r, 1);
        col_rel_destroy(l); col_rel_destroy(r);
        uint64_t baseline = reserved(g);
        uint32_t invalid[] = {6};
        wl_columnar_continuation_t *join = (void *)1;
        CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
            0, 0, invalid, 1, 40, index_cap(3), NULL, NULL, &join) ==
            WL_COLUMNAR_SPILL_INVALID && !join);
        CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
            0, 0, NULL, 1, 40, index_cap(3), NULL, NULL, &join) ==
            WL_COLUMNAR_SPILL_INVALID && !join);
        /* Count zero ignores the map and retains full-width semantics. */
        CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
            0, 0, invalid, 0, 40, index_cap(3), NULL, NULL, &join) ==
            WL_COLUMNAR_SPILL_MEMORY && !join);
        CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
            0, 0, invalid, 0, 48, index_cap(3), NULL, NULL, &join) == OK);
        wl_columnar_continuation_destroy(join);
        uint32_t *map = malloc(5 * sizeof(*map)); CHECK(map);
        const uint32_t expected_map[] = {5, 1, 2, 4, 5};
        memcpy(map, expected_map, sizeof(expected_map));
        for (unsigned fault = 1; fault <= 3; fault++) {
            allocation_fail = fault;
            CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
                0, 0, map, 5, 40, index_cap(3), NULL, NULL, &join) ==
                WL_COLUMNAR_SPILL_ALLOCATION && !join);
            CHECK(reserved(g) == baseline);
        }
        /* Governed map, readers, index and output all unwind on denial. */
        wl_columnar_memory_reservation_t held;
        wl_columnar_memory_reservation_init(&held);
        CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(g),
            (1 << 20) - baseline, &held));
        CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
            0, 0, map, 5, 40, index_cap(3), NULL, NULL, &join) ==
            WL_COLUMNAR_SPILL_MEMORY && !join);
        CHECK(wl_columnar_memory_rollback(&held));
        CHECK(reserved(g) == baseline);
        CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
            0, 0, map, 5, 40, index_cap(mode ? 3 : 1), NULL, NULL,
            &join) == OK);
        memset(map, 0xff, sizeof(expected_map)); free(map);
        CHECK(wl_columnar_batch_payload_release(lp) == WL_COLUMNAR_SPILL_BUSY);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        s.failure = mode;
        bool seen[9][11] = {{false}}; unsigned total = 0, calls = 0;
        for (;;) {
            CHECK(++calls < 10000);
            wl_columnar_continuation_status_t status =
                wl_columnar_continuation_publish(join, &sink);
            if (status == WL_COLUMNAR_CONTINUATION_PROGRESS) continue;
            if (status == WL_COLUMNAR_CONTINUATION_SINK_FAILURE ||
                status == WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE ||
                status == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED) {
                CHECK(mode == 1 || mode == 2 || mode == 4);
                s.failure = 0; continue;
            }
            CHECK(status == WL_COLUMNAR_CONTINUATION_OK ||
                status == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS ||
                status == WL_COLUMNAR_CONTINUATION_DONE);
            s.failure = 0;
            wl_columnar_batch_payload_t *out =
                wl_columnar_join_batch_payload_sink_take(&s.owned);
            if (out) {
                uint32_t cols, rows;
                const wirelog_column_type_t *types =
                    wl_columnar_batch_payload_schema(out, &cols, &rows);
                CHECK(cols == 5 && rows == 1 && types[0] == WIRELOG_TYPE_FLOAT
                    && types[1] == WIRELOG_TYPE_UINT64 && types[2] ==
                    WIRELOG_TYPE_FLOAT && types[3] == WIRELOG_TYPE_UINT64
                    && types[4] == WIRELOG_TYPE_FLOAT);
                wl_columnar_batch_payload_reader_t *reader;
                CHECK(wl_columnar_batch_payload_acquire(out, &reader) == OK);
                const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL,
                    &lanes, &meta) == OK);
                uint64_t i = UINT64_MAX - lanes[1],
                    j = UINT64_MAX - 100 - lanes[3];
                CHECK(i < 9 && j < 11 && !seen[i][j] && key((uint32_t)i, 9) ==
                    key((uint32_t)j, 11));
                double lv = i ? (double)i + 0.25 : -0.0;
                double rv = j ? (double)j + 0.25 : -0.0;
                CHECK(memcmp(&lanes[0], &rv, 8) == 0 &&
                    memcmp(&lanes[2], &lv, 8) == 0 && lanes[0] == lanes[4]);
                CHECK(meta.multiplicity == 1 && !meta.iteration && !meta.stratum
                    && !meta.worker);
                seen[i][j] = true; total++;
                wl_columnar_batch_payload_reader_release(reader);
                CHECK(wl_columnar_batch_payload_release(out) == OK);
            }
            if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
        }
        unsigned expected = 0;
        for (uint32_t i = 0; i < 9; i++)
            for (uint32_t j = 0; j < 11; j++)
                if (key(i, 9) == key(j, 11)) {
                    CHECK(seen[i][j]); expected++;
                }
        CHECK(total == expected);
        wl_columnar_continuation_destroy(join);
        CHECK(reserved(g) == baseline);
        CHECK(wl_columnar_batch_payload_release(lp) == OK);
        CHECK(wl_columnar_batch_payload_release(rp) == OK);
        CHECK(wl_columnar_spill_store_close(store) == OK);
        CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
    }
}

static col_rel_t *reference_source(wl_intern_t *intern, unsigned rows,
    bool string_key, bool inlined)
{
    const char *values[] = {"", "alpha", "한글"};
    int64_t ids[3];
    for (unsigned i = 0; i < 3; i++) {
        ids[i] = wl_intern_put(intern, values[i]); CHECK(ids[i] >= 0);
    }
    col_rel_t *r = col_rel_new_auto("reference", 3); CHECK(r);
    wirelog_column_type_t types[] =
    {string_key ? WIRELOG_TYPE_STRING : WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64,
     WIRELOG_TYPE_STRING};
    CHECK(col_rel_set_column_types(r, types, 3) == 0);
    for (unsigned i = 0; i < rows; i++) {
        int64_t row[] = {string_key?ids[i%3]:(int64_t)(i%3), i, ids[(i+1)%3]};
        CHECK(col_rel_append_row(r, row) == 0);
    }
    if (inlined) {
        CHECK(wl_intern_put(intern, "pair") >= 0);
        col_rel_logical_col_t layout[] = {{WIRELOG_COMPOUND_KIND_NONE, 0, 0},
                                          {WIRELOG_COMPOUND_KIND_INLINE, 2, 1}};
        CHECK(col_rel_apply_compound_schema(r, layout, 2) == 0);
    }
    return r;
}
static wl_columnar_batch_payload_t *reference_capture(
    wl_columnar_memory_governor_ref_t *g,
    wl_columnar_spill_store_t *store, col_rel_t *r, wl_intern_t *intern,
    unsigned mode)
{
    wl_columnar_batch_payload_t *p = NULL;
    wirelog_column_t columns[2] = {0};
    columns[0].type = r->column_types[0];
    columns[1].type = WIRELOG_TYPE_INT32;
    columns[1].compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    columns[1].compound_functor_id = (uint32_t)wl_intern_get(intern, "pair");
    columns[1].compound_arity = 2; columns[1].compound_inline_col_offset = 1;
    wl_columnar_batch_payload_storage_t storage = mode ==
        1 ? WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT;
    CHECK((r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE ?
        wl_columnar_batch_payload_capture_with_inline(g, store, r, r->nrows,
        storage,
        columns, 2, intern, NULL, NULL, &p) :
        wl_columnar_batch_payload_capture_with_intern(g, store, r, r->nrows,
        storage,
        intern, NULL, NULL, &p)) == OK);
    if (mode == 2)CHECK(wl_columnar_batch_payload_spill(&p, store, NULL,
            NULL) == OK);
    return p;
}
static void reference_joins(bool inlined)
{
    const uint32_t map[] = {5, 1, 4, 2, 5};
    const char *values[] = {"", "alpha", "한글"};
    for (unsigned string_key = 0; string_key < 2; string_key++) {
        for (unsigned mode = 0; mode < 5; mode++) {
            wl_columnar_memory_governor_ref_t *g = governor(1<<20);
            wl_intern_t *intern = wl_intern_create();
            CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
            wl_columnar_spill_store_t *store;
            CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 65536, 64,
                &store) == OK);
            col_rel_t *l = reference_source(intern, 9, string_key, inlined),
                *r = reference_source(intern, 11, string_key, inlined);
            wl_columnar_batch_payload_t *lp = reference_capture(g, store, l,
                    intern, mode == 4?2:mode%2);
            wl_columnar_batch_payload_t *rp = reference_capture(g, store, r,
                    intern, (mode/2)%2);
            col_rel_destroy(l); col_rel_destroy(r);
            uint64_t baseline = reserved(g);
            wl_columnar_continuation_t *join = NULL;
            CHECK(create_join(g, lp, rp, 0, 0, 48, NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            CHECK(create_weighted_join(g, lp, rp, 0, 0, 48, NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            CHECK(wl_columnar_payload_join_create_projected_bounded(g, lp, rp,
                0, 0, map, 5, 40, index_cap(1), NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            CHECK(wl_columnar_payload_join_create_reference_projected_bounded(g,
                lp, rp, 0, 0, map, 5, 40, index_cap(1), NULL, NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            wl_intern_t *foreign = wl_intern_create();
            CHECK(foreign && wl_intern_attach_memory_governor(foreign, g) == 0);
            CHECK(wl_intern_put(foreign, "different-at-zero") == 0);
            col_rel_t *f = reference_source(foreign, 3, string_key, inlined);
            wl_columnar_batch_payload_t *fp = reference_capture(g, store, f,
                    foreign, 1); col_rel_destroy(f);
            CHECK(wl_columnar_payload_join_create_reference_projected_bounded(g,
                lp, fp, 0, 0, map, 5, 40, index_cap(1), intern, NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            CHECK(wl_columnar_batch_payload_release(fp) == OK);
            wl_intern_free(foreign);
            f = reference_source(intern, 3, !string_key, inlined);
            fp = reference_capture(g, store, f, intern, 0); col_rel_destroy(f);
            CHECK(wl_columnar_payload_join_create_reference_projected_bounded(g,
                lp, fp, 0, 0, map, 5, 40, index_cap(1), intern, NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            CHECK(wl_columnar_batch_payload_release(fp) == OK &&
                reserved(g) == baseline);
            for (unsigned fault = 1; fault <= 3; fault++) {
                allocation_fail = fault;
                CHECK(
                    wl_columnar_payload_join_create_reference_projected_bounded(
                        g, lp, rp, 0, 0, map, 5, 40, index_cap(1), intern, NULL,
                        NULL, &join) == WL_COLUMNAR_SPILL_ALLOCATION && !join);
                CHECK(reserved(g) == baseline);
            }
            CHECK(wl_columnar_payload_join_create_reference_projected_bounded(g,
                lp, rp, 0, 0, map, 5, 40, index_cap(mode?3:1), intern, NULL,
                NULL, &join) == OK);
            sink_t sink_state; wl_columnar_continuation_sink_t sink;
            sink_init(&sink_state, &sink, g);
            wl_columnar_join_batch_payload_sink_init_with_intern(
                &sink_state.owned, &sink_state.inner, g, NULL,
                WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, intern);
            sink_state.failure = mode;
            bool seen[9][11] = {{false}}; unsigned count = 0, calls = 0;
            wl_columnar_batch_payload_t *retained = NULL;
            for (;;) {
                CHECK(++calls < 10000);
                wl_columnar_continuation_status_t status =
                    wl_columnar_continuation_publish(join, &sink);
                if (status == WL_COLUMNAR_CONTINUATION_PROGRESS) continue;
                if (status == WL_COLUMNAR_CONTINUATION_SINK_FAILURE ||
                    status == WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE ||
                    status == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED) {
                    CHECK(mode == 1 || mode == 2 || mode == 4);
                    CHECK(wl_columnar_batch_payload_release(lp) ==
                        WL_COLUMNAR_SPILL_BUSY);
                    sink_state.failure = 0; continue;
                }
                CHECK(status == WL_COLUMNAR_CONTINUATION_OK ||
                    status == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS ||
                    status == WL_COLUMNAR_CONTINUATION_DONE);
                sink_state.failure = 0;
                wl_columnar_batch_payload_t *out =
                    wl_columnar_join_batch_payload_sink_take(&sink_state.owned);
                if (out) {
                    uint32_t cols, rows;
                    const wirelog_column_type_t *types =
                        wl_columnar_batch_payload_schema(out, &cols, &rows);
                    CHECK(cols == 5 && rows == 1 &&
                        types[0] == WIRELOG_TYPE_STRING &&
                        types[1] == WIRELOG_TYPE_INT64 &&
                        types[2] == WIRELOG_TYPE_INT64 &&
                        types[3] == WIRELOG_TYPE_STRING &&
                        types[4] == WIRELOG_TYPE_STRING);
                    CHECK(wl_columnar_batch_payload_uses_intern(out, intern));
                    uint32_t logical_count = UINT32_MAX;
                    CHECK(!wl_columnar_batch_payload_inline_schema(out,
                        &logical_count) && logical_count == 0);
                    wl_columnar_batch_payload_reader_t *reader;
                    CHECK(wl_columnar_batch_payload_acquire(out,
                        &reader) == OK);
                    const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                    CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL,
                        &lanes, &meta) == OK);
                    unsigned i = (unsigned)lanes[1], j = (unsigned)lanes[2];
                    CHECK(i < 9 && j < 11 && i%3 == j%3 && !seen[i][j]);
                    seen[i][j] = true; count++;
                    CHECK(!strcmp(wl_intern_reverse(intern, (int64_t)lanes[0]),
                        values[(j+1)%3]) && !strcmp(wl_intern_reverse(intern,
                        (int64_t)lanes[3]),
                        values[(i+1)%3]) && lanes[0] == lanes[4]);
                    wl_columnar_batch_payload_reader_release(reader);
                    if (!retained) retained = out;
                    else CHECK(wl_columnar_batch_payload_release(out) == OK);
                }
                if (status == WL_COLUMNAR_CONTINUATION_DONE) break;
            }
            CHECK(count == 33 && retained);
            wl_columnar_continuation_destroy(join);
            CHECK(wl_columnar_batch_payload_release(lp) == OK &&
                wl_columnar_batch_payload_release(rp) == OK);
            CHECK(wl_columnar_spill_store_close(store) == OK);
            wl_intern_free(intern); /* Independently captured output retains its own pin. */
            wl_columnar_batch_payload_reader_t *reader;
            CHECK(wl_columnar_batch_payload_acquire(retained, &reader) == OK);
            const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
            CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL, &lanes,
                &meta) == OK);
            CHECK(wl_intern_reverse(intern,
                (int64_t)lanes[0]) && wl_intern_reverse(intern,
                (int64_t)lanes[3]));
            wl_columnar_batch_payload_reader_release(reader);
            CHECK(wl_columnar_batch_payload_release(retained) == OK);
            CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
        }
    }
}

/* Preserve original global IDs/keys while splitting into governed segments. */
static wl_columnar_trace_store_t *segmented(
    wl_columnar_memory_governor_ref_t *g, wl_columnar_spill_store_t *store,
    const col_rel_t *input, uint32_t chunk, unsigned mode)
{
    wl_columnar_trace_store_t *trace;
    CHECK(wl_columnar_trace_store_create(g, input->column_types, input->ncols,
        &trace) == OK);
    for (uint32_t start = 0;;) {
        uint32_t count = input->nrows - start;
        if (count > chunk) count = chunk;
        col_rel_t *part;
        CHECK(wl_columnar_relation_new_auto_governed("trace_join_fixture",
            input->ncols, count ? count : 1, input->timestamps != NULL,
            g, &part) == 0);
        CHECK(col_rel_set_column_types(part, input->column_types,
            input->ncols) == 0);
        for (uint32_t c = 0; c < input->ncols; c++)
            memcpy(part->columns[c], input->columns[c] + start,
                (size_t)count * 8);
        if (input->timestamps)
            memcpy(part->timestamps, input->timestamps + start,
                (size_t)count * sizeof(*part->timestamps));
        part->nrows = count;
        bool spill = mode == 1 || (mode == 2 && (start/chunk)%2 == 0);
        CHECK(wl_columnar_trace_store_append_capture(trace, part,
            spill ? WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
            store, NULL, NULL) == OK);
        part->nrows = 0; /* Empty boundaries exercise repeated cumulative ends. */
        CHECK(wl_columnar_trace_store_append_capture(trace, part,
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, store, NULL, NULL) == OK);
        col_rel_destroy(part);
        start += count;
        if (start == input->nrows) break;
    }
    return trace;
}
static wl_columnar_trace_store_snapshot_t *snapshot(
    wl_columnar_trace_store_t *t)
{
    wl_columnar_trace_store_snapshot_t *s;
    CHECK(wl_columnar_trace_store_snapshot_acquire(t, &s) == OK); return s;
}
static void trace_combinations(void)
{
    for (unsigned a = 0; a < 3; a++) for (unsigned b = 0; b < 3; b++)
            for (unsigned self = 0; self < 2; self++) {
                wl_columnar_memory_governor_ref_t *g = governor(1 << 22);
                wl_columnar_spill_store_t *store;
                CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22,
                    1024, &store) == OK);
                col_rel_t *l = source(17, 3), *r = source(self ? 17 : 23, 3);
                wl_columnar_trace_store_t *lt = segmented(g, store, l, 5, a);
                wl_columnar_trace_store_t *rt = self ? lt : segmented(g, store,
                        r, 7, b);
                wl_columnar_trace_store_snapshot_t *ls = snapshot(lt),
                    *rs = snapshot(rt);
                wl_columnar_continuation_t *join;
                CHECK(wl_columnar_payload_join_create_trace_bounded(g, ls, rs,
                    0, 0,
                    96, index_cap(3), NULL, NULL, &join) == OK);
                /* A later commit cannot extend this producer. Snapshot pins prevent
                 * replacing a reader that has not yet been acquired lazily. */
                col_rel_t *extra;
                CHECK(wl_columnar_relation_new_auto_governed("later", 3, 1,
                    false, g, &extra) == 0);
                CHECK(col_rel_set_column_types(extra, l->column_types, 3) == 0);
                int64_t lanes[] = {1, 10000, 0};
                CHECK(col_rel_append_row(extra, lanes) == 0);
                CHECK(wl_columnar_trace_store_append_capture(lt, extra,
                    WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, store, NULL,
                    NULL) == OK);
                CHECK(wl_columnar_trace_store_demote(lt, 1, store, NULL,
                    NULL) == WL_COLUMNAR_SPILL_BUSY);
                col_rel_destroy(extra);
                wl_columnar_trace_store_snapshot_release(ls);
                wl_columnar_trace_store_snapshot_release(rs);
                wl_columnar_trace_store_retire(lt);
                if (!self) wl_columnar_trace_store_retire(rt);
                sink_t s; wl_columnar_continuation_sink_t sink;
                sink_init(&s, &sink, g);
                bool seen[17 * 23] = {false};
                drain(join, &s, &sink, l->nrows, r->nrows, 3, 3, seen);
                wl_columnar_continuation_destroy(join);
                col_rel_destroy(l); col_rel_destroy(r);
                wl_columnar_spill_store_retire(store);
                CHECK(reserved(g) == 0);
                wl_columnar_memory_governor_ref_release(g);
            }
}
static void trace_failures(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 22);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
        &store) == OK);
    col_rel_t *r = source(17, 3);
    wl_columnar_trace_store_t *t = segmented(g, store, r, 5, 1);
    wl_columnar_trace_store_snapshot_t *ss = snapshot(t);
    uint64_t floor = reserved(g);
    for (unsigned mode = 0; mode < 7; mode++) {
        bool stop = false;
        wl_columnar_continuation_t *join = NULL;
        CHECK(wl_columnar_payload_join_create_trace_bounded(g, ss, ss, 0, 0,
            48, index_cap(3), cancelled, &stop, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        if (mode < 4) {
            s.failure = mode + 1;
            wl_columnar_continuation_status_t status;
            do {
                status = wl_columnar_continuation_publish(join, &sink);
            }while (status == WL_COLUMNAR_CONTINUATION_PROGRESS);
            CHECK(status == (mode == 0 ? WL_COLUMNAR_CONTINUATION_SINK_FAILURE :
                mode == 1 ? WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE :
                mode == 2 ? WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS :
                WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED));
            /* No input reads when retrying a frozen pending output. */
            if (mode != 2) {
                read_fail = true;
                CHECK(wl_columnar_continuation_publish(join, &sink) == status);
                read_fail = false;
            }
            s.failure = 0;
            bool seen[17 * 17] = {false};
            if (mode == 2) {
                /* Consume already committed output through the same oracle. */
                wl_columnar_batch_payload_t *out =
                    wl_columnar_join_batch_payload_sink_take(&s.owned);
                CHECK(out); CHECK(wl_columnar_batch_payload_release(out) == OK);
                wl_columnar_continuation_destroy(join);
                CHECK(reserved(g) == floor); continue;
            }
            drain(join, &s, &sink, 17, 17, 3, 3, seen);
        } else {
            if (mode == 4) read_fail_after = 9;
            if (mode == 5) stop = true;
            if (mode == 6) wl_columnar_spill_store_retire(store);
            wl_columnar_continuation_status_t status;
            do {
                status = wl_columnar_continuation_publish(join, &sink);
            }while (status == WL_COLUMNAR_CONTINUATION_PROGRESS);
            CHECK(status == WL_COLUMNAR_CONTINUATION_INVALID);
            CHECK(!s.owned.ready && wl_columnar_payload_join_error(join) ==
                (mode == 4 ? WL_COLUMNAR_SPILL_IO : mode ==
                5 ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_CLOSED));
            read_fail_after = 0; stop = false;
            CHECK(wl_columnar_continuation_publish(join,
                &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        }
        wl_columnar_continuation_destroy(join);
        if (mode != 6) CHECK(reserved(g) == floor);
    }
    wl_columnar_trace_store_snapshot_release(ss);
    wl_columnar_trace_store_retire(t);
    col_rel_destroy(r); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void trace_limits(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 22);
    col_rel_t *r = source(0, 2);
    wl_columnar_trace_store_t *empty = segmented(g, NULL, r, 5, 0);
    wl_columnar_trace_store_snapshot_t *e = snapshot(empty);
    col_rel_destroy(r); r = source(5, 2);
    wl_columnar_trace_store_t *t = segmented(g, NULL, r, 2, 0);
    wl_columnar_trace_store_snapshot_t *s = snapshot(t);
    wl_columnar_continuation_t *join;
    CHECK(wl_columnar_payload_join_create_trace_bounded(g, e, s, 0, 0, 32,
        index_cap(1), NULL, NULL, &join) == OK);
    sink_t sink_context; wl_columnar_continuation_sink_t sink;
    sink_init(&sink_context, &sink, g);
    CHECK(wl_columnar_continuation_publish(join,
        &sink) == WL_COLUMNAR_CONTINUATION_DONE);
    wl_columnar_continuation_destroy(join);
    uint64_t before = reserved(g);
    for (unsigned fail = 1; fail <= 2; fail++) {
        allocation_fail = fail;
        CHECK(wl_columnar_payload_join_create_trace_bounded(g, s, s, 0, 0, 32,
            index_cap(1), NULL, NULL, &join) == WL_COLUMNAR_SPILL_ALLOCATION);
        CHECK(!join && reserved(g) == before); allocation_fail = 0;
    }
    CHECK(wl_columnar_payload_join_create_trace_bounded(g, s, s, 0, 0, 32,
        index_cap(1)-1, NULL, NULL, &join) == WL_COLUMNAR_SPILL_MEMORY);
    CHECK(!join && reserved(g) == before);
    CHECK(wl_columnar_payload_join_create_trace_bounded(g, s, s, 2, 0, 32,
        index_cap(1), NULL, NULL, &join) == WL_COLUMNAR_SPILL_INVALID);
    wl_columnar_trace_store_snapshot_release(e);
    wl_columnar_trace_store_retire(empty);
    wl_columnar_trace_store_snapshot_release(s);
    wl_columnar_trace_store_retire(t);
    col_rel_destroy(r);
    col_rel_t *signed_source;
    CHECK(wl_columnar_relation_new_auto_governed("signed", 2, 1, true, g,
        &signed_source) == 0);
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_INT64};
    CHECK(col_rel_set_column_types(signed_source, types, 2) == 0);
    int64_t values[] = {1, 0};
    CHECK(col_rel_append_row(signed_source, values) == 0);
    signed_source->timestamps[0].multiplicity = -1;
    t = segmented(g, NULL, signed_source, 1, 0); s = snapshot(t);
    CHECK(wl_columnar_payload_join_create_trace_bounded(g, s, s, 0, 0, 32,
        index_cap(1), NULL, NULL, &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(!join);
    wl_columnar_trace_store_snapshot_release(s);
    wl_columnar_trace_store_retire(t);
    col_rel_destroy(signed_source); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static const wirelog_column_type_t trace_join_types[] = {
    WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64
};
typedef struct { int64_t key, value, weight; } trace_record_t;
static col_rel_t *trace_records(wl_columnar_memory_governor_ref_t *g,
    const trace_record_t *records, uint32_t count)
{
    col_rel_t *r;
    CHECK(wl_columnar_relation_new_auto_governed("trace_records", 2,
        count ? count : 1, true, g, &r) == 0);
    CHECK(col_rel_set_column_types(r, trace_join_types, 2) == 0);
    for (uint32_t i = 0; i < count; i++) {
        r->columns[0][i] = records[i].key; r->columns[1][i] = records[i].value;
        r->timestamps[i] =
            (col_delta_timestamp_t){.multiplicity = records[i].weight};
    }
    r->nrows = count; return r;
}
typedef struct {
    int64_t lanes[4], weight;
    bool seen;
} trace_expected_t;
typedef struct {
    wl_columnar_continuation_t *join;
    sink_t sink_context;
    wl_columnar_continuation_sink_t sink;
    trace_expected_t expected[64];
    unsigned count, received;
    int64_t sum;
    bool done;
} trace_consumer_t;
/* Small safe fixture products, independently enumerated; no production JOIN
 * result is used as the expected answer. Identical retraction tuples can repeat. */
static void trace_expected(trace_consumer_t *consumer, const trace_record_t *l,
    unsigned ln, const trace_record_t *r, unsigned rn)
{
    for (unsigned i = 0; i < ln; i++) for (unsigned j = 0; j < rn; j++)
            if (l[i].key == r[j].key) {
                CHECK(consumer->count < 64);
                consumer->expected[consumer->count++] = (trace_expected_t){
                    {l[i].key, l[i].value, r[j].key, r[j].value},
                    l[i].weight * r[j].weight, false
                };
            }
}
static void trace_step(trace_consumer_t *c)
{
    wl_columnar_continuation_status_t status =
        wl_columnar_continuation_publish(c->join, &c->sink);
    CHECK(status == WL_COLUMNAR_CONTINUATION_OK ||
        status == WL_COLUMNAR_CONTINUATION_PROGRESS ||
        status == WL_COLUMNAR_CONTINUATION_DONE);
    wl_columnar_batch_payload_t *out =
        wl_columnar_join_batch_payload_sink_take(&c->sink_context.owned);
    if (out) {
        uint32_t rows; wl_columnar_batch_payload_schema(out, NULL, &rows);
        wl_columnar_batch_payload_reader_t *reader;
        CHECK(wl_columnar_batch_payload_acquire(out, &reader) == OK);
        for (uint32_t row = 0; row < rows; row++) {
            const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
            CHECK(wl_columnar_batch_payload_read(reader, row, NULL, NULL,
                &lanes, &meta) == OK);
            CHECK(!meta.iteration && !meta.stratum && !meta.worker);
            unsigned i;
            for (i = 0; i < c->count; i++) if (!c->expected[i].seen &&
                    meta.multiplicity == c->expected[i].weight &&
                    memcmp(lanes, c->expected[i].lanes,
                    sizeof(c->expected[i].lanes)) == 0){
                    c->expected[i].seen = true; break;
                }
            CHECK(i < c->count); c->received++; c->sum += meta.multiplicity;
        }
        wl_columnar_batch_payload_reader_release(reader);
        CHECK(wl_columnar_batch_payload_release(out) == OK);
    }
    c->done = status == WL_COLUMNAR_CONTINUATION_DONE;
    if (c->done) CHECK(c->count == c->received);
}
static void trace_generations(void)
{
    const trace_record_t history[] = {{7, 10, 2}, {8, 11, 1}, {7, 10, -1},
                                      {7, 10, -1}, {8, 11, -1}, {7, 10, 3},
                                      {8, 11, 2}};
    const trace_record_t delta[] = {{7, 20, 1}, {7, 20, -2}, {8, 21, 3},
                                    {9, 22, INT64_MAX}, {7, 20, 0}};
    const unsigned ends[] = {3, 5, 7};
    const int64_t sums[] = {2, 0, 3};
    wl_columnar_memory_governor_ref_t *g = governor(1 << 22);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
        &store) == OK);
    wl_columnar_trace_store_t *trace;
    CHECK(wl_columnar_trace_store_create(g, trace_join_types, 2, &trace) == OK);
    col_rel_t *d = trace_records(g, delta, 5);
    wl_columnar_batch_payload_t *payload = capture(g, store, d, 1);
    wl_columnar_trace_store_t *dt = segmented(g, store, d, 2, 2);
    wl_columnar_trace_store_snapshot_t *ds = snapshot(dt);
    col_rel_destroy(d);
    trace_consumer_t consumers[6] = {0};
    for (unsigned generation = 0, start = 0; generation < 3; generation++) {
        col_rel_t *r = trace_records(g, history + start,
                ends[generation] - start);
        CHECK(wl_columnar_trace_store_append_capture(trace, r,
            generation ==
            1 ? WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT :
            WL_COLUMNAR_BATCH_PAYLOAD_SPILLED,
            store, NULL, NULL) == OK);
        col_rel_destroy(r); start = ends[generation];
        wl_columnar_trace_store_snapshot_t *ss = snapshot(trace);
        CHECK(wl_columnar_trace_store_snapshot_state(ss).generation ==
            generation + 1);
        for (unsigned kind = 0; kind < 2; kind++) {
            trace_consumer_t *c = &consumers[generation * 2 + kind];
            CHECK((kind ?
                wl_columnar_payload_join_create_weighted_trace_bounded(g, ds,
                ss, 0, 0, 32, index_cap(2), NULL, NULL, &c->join)
                : wl_columnar_payload_join_create_delta_trace_bounded(g,
                payload, ss, 0, 0, 32, index_cap(2), NULL, NULL,
                &c->join)) == OK);
            sink_init(&c->sink_context, &c->sink, g);
            trace_expected(c, delta, 5, history, ends[generation]);
        }
        wl_columnar_trace_store_snapshot_release(ss);
    }
    wl_columnar_trace_store_snapshot_release(ds);
    wl_columnar_trace_store_retire(dt); wl_columnar_trace_store_retire(trace);
    /* Freeze each first output across a failed commit; retry must do no I/O. */
    for (unsigned i = 0; i < 6; i++) {
        trace_consumer_t *c = &consumers[i]; c->sink_context.failure = 2;
        wl_columnar_continuation_status_t status;
        do {
            status = wl_columnar_continuation_publish(c->join, &c->sink);
        }while (status == WL_COLUMNAR_CONTINUATION_PROGRESS);
        CHECK(status == WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE);
        read_fail = true;
        CHECK(wl_columnar_continuation_publish(c->join, &c->sink) == status);
        read_fail = false; c->sink_context.failure = 0;
    }
    for (unsigned rounds = 0;; rounds++) {
        CHECK(rounds < 1000); bool done = true;
        for (unsigned i = 0; i < 6; i++) if (!consumers[i].done) {
                trace_step(&consumers[i]); done = false;
            }
        if (done) break;
    }
    for (unsigned i = 0; i < 6; i++) {
        CHECK(consumers[i].sum == sums[i/2]);
        wl_columnar_continuation_destroy(consumers[i].join);
    }
    CHECK(wl_columnar_batch_payload_release(payload) == OK);
    wl_columnar_spill_store_retire(store); CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}
static void trace_signed_errors(void)
{
    const int64_t lhs[] = {INT64_MIN, INT64_MAX, INT64_MIN, INT64_MAX, 0};
    const int64_t rhs[] = {1, 1, -1, 2, INT64_MIN};
    for (unsigned kind = 0; kind < 2; kind++) for (unsigned k = 0; k < 5; k++)
            for (unsigned mismatch = 0; mismatch < 2; mismatch++) {
                wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
                trace_record_t ldata[] = {{7, 1, lhs[k]}},
                    rdata[] = {{mismatch ? 8 : 7, 2, rhs[k]}};
                col_rel_t *l = trace_records(g, ldata, 1),
                    *r = trace_records(g, rdata, 1);
                wl_columnar_trace_store_t *lt = segmented(g, NULL, l, 1, 0),
                    *rt = segmented(g, NULL, r, 1, 0);
                wl_columnar_trace_store_snapshot_t *ls = snapshot(lt),
                    *rs = snapshot(rt);
                wl_columnar_batch_payload_t *lp = capture(g, NULL, l, 0);
                wl_columnar_continuation_t *join;
                CHECK((kind ?
                    wl_columnar_payload_join_create_weighted_trace_bounded(g,
                    ls, rs, 0, 0, 32, index_cap(1), NULL, NULL, &join)
                : wl_columnar_payload_join_create_delta_trace_bounded(g, lp, rs,
                    0, 0, 32, index_cap(1), NULL, NULL, &join)) == OK);
                sink_t s; wl_columnar_continuation_sink_t sink;
                sink_init(&s, &sink, g);
                wl_columnar_continuation_status_t status =
                    wl_columnar_continuation_publish(join, &sink);
                bool overflow = k == 2 || k == 3;
                if (mismatch) CHECK(status == WL_COLUMNAR_CONTINUATION_DONE &&
                        !s.owned.ready);
                else if (overflow) {
                    CHECK(status == WL_COLUMNAR_CONTINUATION_INVALID &&
                        !s.owned.ready);
                    CHECK(wl_columnar_payload_join_error(join) ==
                        WL_COLUMNAR_SPILL_OVERFLOW);
                    CHECK(wl_columnar_continuation_publish(join,
                        &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
                } else {
                    CHECK(status == WL_COLUMNAR_CONTINUATION_OK);
                    wl_columnar_batch_payload_t *out =
                        wl_columnar_join_batch_payload_sink_take(&s.owned);
                    wl_columnar_batch_payload_reader_t *reader;
                    CHECK(wl_columnar_batch_payload_acquire(out,
                        &reader) == OK);
                    const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                    CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL,
                        &lanes, &meta) == OK);
                    CHECK(meta.multiplicity ==
                        (k == 0 ? INT64_MIN : k == 1 ? INT64_MAX : 0));
                    wl_columnar_batch_payload_reader_release(reader);
                    CHECK(wl_columnar_batch_payload_release(out) == OK);
                }
                wl_columnar_continuation_destroy(join);
                CHECK(wl_columnar_batch_payload_release(lp) == OK);
                /* Complete delta validation refuses a nonmatching logical time. */
                l->timestamps[0].iteration = 1; lp = capture(g, NULL, l, 0);
                CHECK(wl_columnar_payload_join_create_delta_trace_bounded(g, lp,
                    rs, 0, 0, 32, index_cap(1), NULL, NULL,
                    &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
                CHECK(wl_columnar_batch_payload_release(lp) == OK);
                wl_columnar_trace_store_snapshot_release(ls);
                wl_columnar_trace_store_snapshot_release(rs);
                wl_columnar_trace_store_retire(lt);
                wl_columnar_trace_store_retire(rt);
                col_rel_destroy(l); col_rel_destroy(r); CHECK(reserved(g) == 0);
                wl_columnar_memory_governor_ref_release(g);
            }
}
/* Same physical history and allowance; only source ownership transfer differs.
 * Fixed segment count across cardinalities separates row scratch from metadata. */
static uint64_t trace_causal_budget(uint32_t chunk, bool retain)
{
    const uint64_t limit = 65536;
    wl_columnar_memory_governor_ref_t *g = governor(limit);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
        &store) == OK);
    wl_columnar_trace_store_t *t;
    CHECK(wl_columnar_trace_store_create(g, trace_join_types, 2, &t) == OK);
    col_rel_t *held[2] = {0};
    for (unsigned segment = 0; segment < 2; segment++) {
        col_rel_t *r;
        CHECK(wl_columnar_relation_new_auto_governed("history", 2, chunk, true,
            g, &r) == 0);
        CHECK(col_rel_set_column_types(r, trace_join_types, 2) == 0);
        for (uint32_t i = 0; i < chunk; i++) {
            r->columns[0][i] = 7; r->columns[1][i] = segment * chunk + i;
            r->timestamps[i] =
                (col_delta_timestamp_t){.multiplicity = (int64_t)(i%3) - 1};
        }
        r->nrows = chunk; uint64_t before = reserved(g);
        if (retain) {
            CHECK(wl_columnar_trace_store_append_capture(t, r,
                WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, store, NULL, NULL) == OK);
            held[segment] = r;
        } else {
            CHECK(wl_columnar_trace_store_append_take(t, &r, store, NULL,
                NULL) == OK && !r);
            CHECK(reserved(g) < before); /* Original storage really retired. */
        }
    }
    trace_record_t tail[] = {{7, 2 * chunk, 1}}, delta[] = {{7, 99, -2}};
    col_rel_t *r = trace_records(g, tail, 1);
    CHECK(wl_columnar_trace_store_append_capture(t, r,
        WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, store, NULL, NULL) == OK);
    col_rel_destroy(r); r = trace_records(g, delta, 1);
    wl_columnar_batch_payload_t *d = capture(g, store, r, 0);
    col_rel_destroy(r);
    wl_columnar_trace_store_snapshot_t *ss = snapshot(t);
    CHECK(wl_columnar_trace_store_snapshot_state(ss).segments == 3);
    uint64_t before = reserved(g); wl_columnar_continuation_t *join = NULL;
    wl_columnar_spill_status_t status =
        wl_columnar_payload_join_create_delta_trace_bounded(g, d, ss, 0, 0,
            32*512, index_cap(127), NULL, NULL, &join);
    uint64_t scratch = reserved(g) - before;
    if (retain) {
        CHECK(status == WL_COLUMNAR_SPILL_MEMORY && !join &&
            reserved(g) == before);
    } else {
        CHECK(status == OK && join);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        bool seen[1025] = {0}; unsigned count = 0;
        for (unsigned calls = 0;; calls++) {
            CHECK(calls < 10000);
            wl_columnar_continuation_status_t step =
                wl_columnar_continuation_publish(join, &sink);
            CHECK(step == WL_COLUMNAR_CONTINUATION_OK ||
                step == WL_COLUMNAR_CONTINUATION_PROGRESS ||
                step == WL_COLUMNAR_CONTINUATION_DONE);
            CHECK(reserved(g) <= limit);
            wl_columnar_batch_payload_t *out =
                wl_columnar_join_batch_payload_sink_take(&s.owned);
            if (out) {
                uint32_t rows;
                wl_columnar_batch_payload_schema(out, NULL, &rows);
                wl_columnar_batch_payload_reader_t *reader;
                CHECK(wl_columnar_batch_payload_acquire(out, &reader) == OK);
                for (uint32_t row = 0; row < rows; row++) {
                    const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                    CHECK(wl_columnar_batch_payload_read(reader, row, NULL,
                        NULL, &lanes, &meta) == OK);
                    uint32_t id = (uint32_t)lanes[3];
                    CHECK(id <= 2*chunk && !seen[id] && lanes[0] == 7 &&
                        lanes[1] == 99 && lanes[2] == 7);
                    CHECK(meta.multiplicity ==
                        -2 * (id == 2*chunk ? 1 : (int64_t)((id%chunk)%3) - 1));
                    seen[id] = true; count++;
                }
                wl_columnar_batch_payload_reader_release(reader);
                CHECK(wl_columnar_batch_payload_release(out) == OK);
            }
            if (step == WL_COLUMNAR_CONTINUATION_DONE) break;
        }
        CHECK(count == 2*chunk + 1); wl_columnar_continuation_destroy(join);
        CHECK(reserved(g) == before);
    }
    wl_columnar_trace_store_snapshot_release(ss);
    wl_columnar_trace_store_retire(t);
    CHECK(wl_columnar_batch_payload_release(d) == OK);
    col_rel_destroy(held[0]); col_rel_destroy(held[1]);
    wl_columnar_spill_store_retire(store);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
    return scratch;
}

static void trace_partial_overflow(void)
{
    for (unsigned batch = 1; batch <= 3; batch += 2) {
        wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
        trace_record_t lhs[] = {{7, 1, 2}},
            rhs[] = {{7, 2, 1}, {7, 3, INT64_MAX}};
        col_rel_t *l = trace_records(g, lhs, 1), *r = trace_records(g, rhs, 2);
        wl_columnar_batch_payload_t *d = capture(g, NULL, l, 0);
        wl_columnar_trace_store_t *t = segmented(g, NULL, r, 1, 0);
        wl_columnar_trace_store_snapshot_t *ss = snapshot(t);
        wl_columnar_continuation_t *join;
        CHECK(wl_columnar_payload_join_create_delta_trace_bounded(g, d, ss, 0,
            0, 32 * batch, index_cap(1), NULL, NULL, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        wl_columnar_batch_payload_t *prior = NULL;
        if (batch == 1) {
            CHECK(wl_columnar_continuation_publish(join,
                &sink) == WL_COLUMNAR_CONTINUATION_OK);
            prior = wl_columnar_join_batch_payload_sink_take(&s.owned);
            CHECK(prior);
        }
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(!s.owned.ready &&
            wl_columnar_payload_join_error(join) == WL_COLUMNAR_SPILL_OVERFLOW);
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        wl_columnar_continuation_destroy(join);
        if (prior) {
            wl_columnar_batch_payload_reader_t *reader;
            CHECK(wl_columnar_batch_payload_acquire(prior, &reader) == OK);
            const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
            CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL, &lanes,
                &meta) == OK);
            CHECK(meta.multiplicity == 2 && lanes[3] == 2);
            wl_columnar_batch_payload_reader_release(reader);
            CHECK(wl_columnar_batch_payload_release(prior) == OK);
        }
        wl_columnar_trace_store_snapshot_release(ss);
        wl_columnar_trace_store_retire(t);
        CHECK(wl_columnar_batch_payload_release(d) == OK); col_rel_destroy(l);
        col_rel_destroy(r);
        CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
    }
}
static void trace_progress_and_floor(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 22);
    wl_columnar_spill_store_t *store;
    CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 22, 1024,
        &store) == OK);
    col_rel_t *r = source(600, 2);
    wl_columnar_trace_store_t *t = segmented(g, store, r, 7, 2);
    wl_columnar_trace_store_snapshot_t *ss = snapshot(t);
    wl_columnar_trace_store_cursor_t *cursor;
    CHECK(wl_columnar_trace_store_cursor_acquire(ss, &cursor) == OK);
    const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
    CHECK(wl_columnar_trace_store_cursor_read(cursor, 0, NULL, NULL, &lanes,
        &meta) == OK);
    wl_columnar_memory_governor_t *ledger =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t limit = atomic_load_explicit(&ledger->usable_bytes,
            memory_order_acquire);
    uint64_t before = reserved(g);
    atomic_store_explicit(&ledger->usable_bytes, before, memory_order_release);
    wl_columnar_continuation_t *join;
    CHECK(wl_columnar_payload_join_create_weighted_trace_bounded(g, ss, ss, 0,
        0, 32, index_cap(300), NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_MEMORY && !join);
    CHECK(reserved(g) == before);
    CHECK(wl_columnar_trace_store_cursor_read(cursor, 0, NULL, NULL, &lanes,
        &meta) == OK && lanes[1] == 0);
    atomic_store_explicit(&ledger->usable_bytes, limit, memory_order_release);
    wl_columnar_trace_store_cursor_release(cursor);
    for (unsigned mode = 0; mode < 3; mode++) {
        bool stop = false;
        CHECK(wl_columnar_payload_join_create_weighted_trace_bounded(g, ss, ss,
            0, 0, 32, index_cap(300), cancelled, &stop, &join) == OK);
        sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
        read_fail = true;
        for (unsigned i = 0; i < 2; i++) {
            CHECK(wl_columnar_continuation_publish(join,
                &sink) == WL_COLUMNAR_CONTINUATION_PROGRESS);
            CHECK(!s.owned.ready &&
                !wl_columnar_continuation_cursor(join)->sequence);
        }
        if (mode) read_fail = false;
        if (mode == 1) stop = true;
        if (mode == 2) {
            CHECK(wl_columnar_continuation_publish(join,
                &sink) == WL_COLUMNAR_CONTINUATION_PROGRESS);
            CHECK(!s.owned.ready); stop = true;
        }
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_INVALID);
        CHECK(wl_columnar_payload_join_error(join) ==
            (mode ? WL_COLUMNAR_SPILL_CANCELLED : WL_COLUMNAR_SPILL_IO));
        read_fail = false; wl_columnar_continuation_destroy(join);
    }
    wl_columnar_trace_store_snapshot_release(ss);
    wl_columnar_trace_store_retire(t);
    col_rel_destroy(r); wl_columnar_spill_store_retire(store);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

/* Reference fixtures preserve global row identity across lazy segment changes. */
static void reference_columns(const col_rel_t *r, wl_intern_t *intern,
    wirelog_column_t columns[2])
{
    memset(columns, 0, 2 * sizeof(*columns));
    columns[0].type = r->column_types[0];
    columns[1].type = WIRELOG_TYPE_INT32;
    columns[1].compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    columns[1].compound_functor_id = (uint32_t)wl_intern_get(intern, "pair");
    columns[1].compound_arity = 2;
    columns[1].compound_inline_col_offset = 1;
}
static wl_columnar_trace_store_t *reference_segmented(
    wl_columnar_memory_governor_ref_t *g, wl_columnar_spill_store_t *store,
    const col_rel_t *r, wl_intern_t *intern, uint32_t chunk, unsigned mode)
{
    bool inlined = r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE;
    wirelog_column_t columns[2]; reference_columns(r, intern, columns);
    wl_columnar_trace_store_t *t;
    CHECK(wl_columnar_trace_store_create_reference(g, r->column_types, 3,
        inlined ? columns : NULL, inlined ? 2 : 0, intern, &t) == OK);
    for (uint32_t start = 0;;) {
        uint32_t count = r->nrows - start;
        if (count > chunk) count = chunk;
        col_rel_t *part;
        CHECK(wl_columnar_relation_new_auto_governed("reference_segment", 3,
            count ? count : 1, r->timestamps != NULL, g, &part) == 0);
        CHECK(col_rel_set_column_types(part, r->column_types, 3) == 0);
        if (inlined) {
            col_rel_logical_col_t layout[] = {{WIRELOG_COMPOUND_KIND_NONE, 0,
                                               0},
                                              {WIRELOG_COMPOUND_KIND_INLINE, 2,
                                               1}};
            CHECK(col_rel_apply_compound_schema(part, layout, 2) == 0);
        }
        for (unsigned c = 0; c < 3; c++)
            memcpy(part->columns[c], r->columns[c] + start,
                count * sizeof(int64_t));
        if (r->timestamps) memcpy(part->timestamps, r->timestamps + start,
                count * sizeof(*part->timestamps));
        part->nrows = count;
        bool spilled = mode == 1 || (mode == 2 && (start/chunk)%2 == 0);
        CHECK(wl_columnar_trace_store_append_capture_reference(t, part,
            inlined ? columns : NULL, inlined ? 2 : 0, intern,
            spilled ? WL_COLUMNAR_BATCH_PAYLOAD_SPILLED :
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
            store, NULL, NULL) == OK);
        part->nrows = 0;
        CHECK(wl_columnar_trace_store_append_capture_reference(t, part,
            inlined ? columns : NULL, inlined ? 2 : 0, intern,
            WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, store, NULL, NULL) == OK);
        col_rel_destroy(part); start += count;
        if (start == r->nrows) break;
    }
    return t;
}
static void reference_trace_joins(void)
{
    const char *values[] = {"", "alpha", "한글"};
    const uint32_t map[] = {5, 1, 4, 2, 5};
    for (unsigned inline_mode = 0; inline_mode < 2; inline_mode++)
        for (unsigned string_key = 0; string_key < 2; string_key++)
            for (unsigned mode = 0; mode < 5; mode++) {
                wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
                wl_intern_t *intern = wl_intern_create();
                CHECK(intern && wl_intern_attach_memory_governor(intern,
                    g) == 0);
                wl_columnar_spill_store_t *store;
                CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20,
                    64, &store) == OK);
                col_rel_t *l = reference_source(intern, mode == 4 ? 0 : 9,
                        string_key, inline_mode);
                col_rel_t *r = reference_source(intern, 11, string_key,
                        inline_mode);
                wl_columnar_trace_store_t *lt = reference_segmented(g, store, l,
                        intern, 2, mode%3);
                wl_columnar_trace_store_t *rt = mode ==
                    3 ? lt : reference_segmented(g, store, r, intern, 3,
                        (mode+1)%3);
                col_rel_destroy(l); col_rel_destroy(r);
                wl_columnar_trace_store_snapshot_t *ls = snapshot(lt),
                    *rs = snapshot(rt);
                wl_intern_t *foreign = wl_intern_create();
                CHECK(foreign && wl_intern_attach_memory_governor(foreign,
                    g) == 0);
                CHECK(wl_intern_put(foreign, "foreign-zero") == 0);
                uint64_t before = reserved(g);
                wl_columnar_continuation_t *join;
                CHECK(
                    wl_columnar_payload_join_create_reference_trace_projected_bounded
                        (g,
                    ls, rs, 0, 0, map, 5, 40, index_cap(1), foreign, NULL, NULL,
                    &join)
                    == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
                CHECK(reserved(g) == before); wl_intern_free(foreign);
                for (unsigned fault = 1; fault <= 3; fault++) {
                    before = reserved(g); allocation_fail = fault;
                    CHECK(
                        wl_columnar_payload_join_create_reference_trace_projected_bounded
                            (g,
                        ls, rs, 0, 0, map, 5, 40, index_cap(1), intern, NULL,
                        NULL, &join)
                        == WL_COLUMNAR_SPILL_ALLOCATION && !join);
                    CHECK(reserved(g) == before);
                }
                uint32_t copied_map[5]; memcpy(copied_map, map, sizeof(map));
                CHECK(
                    wl_columnar_payload_join_create_reference_trace_projected_bounded
                        (g,
                    ls, rs, 0, 0, copied_map, 5, 40, index_cap(3), intern, NULL,
                    NULL, &join) == OK);
                memset(copied_map, 0xff, sizeof(copied_map));
                wl_columnar_trace_store_snapshot_release(ls);
                wl_columnar_trace_store_snapshot_release(rs);
                wl_columnar_trace_store_retire(lt);
                if (rt != lt) wl_columnar_trace_store_retire(rt);
                sink_t s; wl_columnar_continuation_sink_t sink;
                sink_init(&s, &sink, g);
                wl_columnar_join_batch_payload_sink_init_with_intern(&s.owned,
                    &s.inner,
                    g, NULL, WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, intern);
                s.failure = mode == 4 ? 0 : mode;
                bool seen[9][11] = {{false}}; unsigned total = 0;
                wl_columnar_batch_payload_t *retained = NULL;
                for (unsigned calls = 0;; calls++) {
                    CHECK(calls < 10000);
                    wl_columnar_continuation_status_t step =
                        wl_columnar_continuation_publish(join, &sink);
                    if (step == WL_COLUMNAR_CONTINUATION_PROGRESS) continue;
                    if (step == WL_COLUMNAR_CONTINUATION_SINK_FAILURE ||
                        step == WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE) {
                        s.failure = 0; read_fail = true;
                        step = wl_columnar_continuation_publish(join, &sink);
                        read_fail = false; /* Frozen retry cannot reread segments. */
                    }
                    CHECK(step == WL_COLUMNAR_CONTINUATION_OK ||
                        step == WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS ||
                        step == WL_COLUMNAR_CONTINUATION_DONE);
                    s.failure = 0;
                    wl_columnar_batch_payload_t *out =
                        wl_columnar_join_batch_payload_sink_take(&s.owned);
                    if (out) {
                        uint32_t cols, rows, logical = UINT32_MAX;
                        const wirelog_column_type_t *types =
                            wl_columnar_batch_payload_schema(out, &cols, &rows);
                        CHECK(cols == 5 && rows == 1 &&
                            types[0] == WIRELOG_TYPE_STRING &&
                            types[1] == WIRELOG_TYPE_INT64 &&
                            types[2] == WIRELOG_TYPE_INT64 &&
                            types[3] == WIRELOG_TYPE_STRING &&
                            types[4] == WIRELOG_TYPE_STRING);
                        CHECK(!wl_columnar_batch_payload_inline_schema(out,
                            &logical) && !logical);
                        wl_columnar_batch_payload_reader_t *reader;
                        CHECK(wl_columnar_batch_payload_acquire(out,
                            &reader) == OK);
                        const uint64_t *lanes;
                        wl_columnar_spill_metadata_t meta;
                        CHECK(wl_columnar_batch_payload_read(reader, 0, NULL,
                            NULL, &lanes, &meta) == OK);
                        unsigned i = (unsigned)lanes[1], j = (unsigned)lanes[2];
                        CHECK(i < 9 && j < (mode == 3 ? 9u : 11u) &&
                            i%3 == j%3 && !seen[i][j]);
                        seen[i][j] = true; total++;
                        CHECK(!strcmp(wl_intern_reverse(intern,
                            (int64_t)lanes[0]),
                            values[(j+1)%3]) &&
                            !strcmp(wl_intern_reverse(intern,
                            (int64_t)lanes[3]),
                            values[(i+1)%3]) && lanes[0] == lanes[4]);
                        CHECK(meta.multiplicity == 1 && !meta.iteration &&
                            !meta.stratum && !meta.worker);
                        wl_columnar_batch_payload_reader_release(reader);
                        if (!retained) retained = out;
                        else CHECK(wl_columnar_batch_payload_release(out) ==
                                OK);
                    }
                    if (step == WL_COLUMNAR_CONTINUATION_DONE) break;
                }
                CHECK(total == (mode == 4 ? 0u : mode == 3 ? 27u : 33u));
                wl_columnar_continuation_destroy(join);
                wl_columnar_spill_store_retire(store); wl_intern_free(intern);
                if (retained) {
                    wl_columnar_batch_payload_reader_t *reader;
                    CHECK(wl_columnar_batch_payload_acquire(retained,
                        &reader) == OK);
                    const uint64_t *lanes; wl_columnar_spill_metadata_t meta;
                    CHECK(wl_columnar_batch_payload_read(reader, 0, NULL, NULL,
                        &lanes, &meta) == OK);
                    const char *text;
                    CHECK(wl_columnar_batch_payload_resolve_string(reader,
                        lanes[0], &text) == OK && text);
                    wl_columnar_batch_payload_reader_release(reader);
                    CHECK(wl_columnar_batch_payload_release(retained) == OK);
                }
                CHECK(reserved(g) == 0);
                wl_columnar_memory_governor_ref_release(g);
            }
}

typedef struct {
    sink_t sink;
    wl_intern_t *intern;
    const col_rel_t *pending;
    int64_t first[6];
    unsigned calls;
} reference_borrow_t;
static wl_columnar_continuation_status_t reference_borrow_begin(void *v,
    const wl_columnar_continuation_batch_t *batch)
{
    reference_borrow_t *b = v; const col_rel_t *r = batch->payload;
    CHECK(r && r->ncols == 6 && r->nrows == 1 && !r->compound_arity_map);
    if (b->pending) CHECK(b->pending == r);
    b->pending = r;
    for (unsigned c = 0; c < 6; c++) {
        if (b->calls) CHECK(b->first[c] == r->columns[c][0]);
        b->first[c] = r->columns[c][0];
        if (r->column_types[c] == WIRELOG_TYPE_STRING)
            CHECK(wl_intern_reverse(b->intern, r->columns[c][0]));
    }
    b->calls++; return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
}
static void reference_trace_retirement(void)
{
    for (unsigned inlined = 0; inlined < 2; inlined++) {
        wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
        wl_intern_t *intern = wl_intern_create();
        CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
        wl_columnar_spill_store_t *store;
        CHECK(wl_columnar_spill_store_open(test_tmpdir(), g, 1 << 20, 64,
            &store) == OK);
        col_rel_t *r = reference_source(intern, 7, true, inlined);
        wl_columnar_trace_store_t *t = reference_segmented(g, store, r, intern,
                2, 2);
        col_rel_destroy(r);
        wl_columnar_trace_store_snapshot_t *ss = snapshot(t);
        wl_intern_free(intern); /* Existing owner capability permits lazy construction. */
        wl_columnar_continuation_t *join;
        CHECK(wl_columnar_payload_join_create_reference_trace_projected_bounded(
                g,
                ss, ss, 0, 0, NULL, 0, 48, index_cap(3), intern, NULL, NULL,
                &join) == OK);
        wl_columnar_trace_store_snapshot_release(ss);
        wl_columnar_trace_store_retire(t);
        reference_borrow_t b = {0}; b.intern = intern;
        wl_columnar_continuation_sink_t sink; sink_init(&b.sink, &sink, g);
        sink.context = &b; sink.begin = reference_borrow_begin;
        wl_columnar_continuation_status_t step;
        do {
            step = wl_columnar_continuation_publish(join, &sink);
        }while (step == WL_COLUMNAR_CONTINUATION_PROGRESS);
        CHECK(step == WL_COLUMNAR_CONTINUATION_SINK_FAILURE && b.calls == 1);
        sink_t owned; wl_columnar_continuation_sink_t owned_sink;
        sink_init(&owned, &owned_sink, g);
        wl_columnar_join_batch_payload_sink_init_with_intern(&owned.owned,
            &owned.inner, g, NULL, WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT, intern);
        uint64_t before = reserved(g);
        read_fail = true;
        CHECK(wl_columnar_continuation_publish(join,
            &owned_sink) == WL_COLUMNAR_CONTINUATION_SINK_FAILURE);
        CHECK(owned.owned.error == WL_COLUMNAR_SPILL_CLOSED &&
            !owned.owned.ready && reserved(g) == before);
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_SINK_FAILURE && b.calls == 2);
        read_fail = false;
        wl_columnar_spill_store_retire(store);
        CHECK(wl_columnar_continuation_publish(join,
            &sink) == WL_COLUMNAR_CONTINUATION_SINK_FAILURE && b.calls == 3);
        wl_columnar_continuation_destroy(join);
        CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
    }
}
static void reference_trace_numeric_inline(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
    col_rel_t *r = reference_source(intern, 2, false, true);
    r->column_types[2] = WIRELOG_TYPE_INT64;
    wl_columnar_trace_store_t *t = reference_segmented(g, NULL, r, intern, 1,
            0);
    col_rel_destroy(r); wl_columnar_trace_store_snapshot_t *ss = snapshot(t);
    wl_columnar_continuation_t *join;
    uint64_t before = reserved(g);
    CHECK(wl_columnar_payload_join_create_reference_trace_projected_bounded(g,
        ss, ss, 0, 0, NULL, 0, 48, index_cap(1), NULL, NULL, NULL, &join)
        == WL_COLUMNAR_SPILL_UNSUPPORTED && !join && reserved(g) == before);
    CHECK(wl_columnar_payload_join_create_reference_trace_projected_bounded(g,
        ss, ss, 0, 0, NULL, 0, 48, index_cap(1), intern, NULL, NULL,
        &join) == OK);
    wl_columnar_trace_store_snapshot_release(ss);
    wl_columnar_trace_store_retire(t);
    wl_intern_free(intern);
    sink_t s; wl_columnar_continuation_sink_t sink; sink_init(&s, &sink, g);
    unsigned rows = 0;
    for (;;) {
        wl_columnar_continuation_status_t step =
            wl_columnar_continuation_publish(join, &sink);
        CHECK(step == WL_COLUMNAR_CONTINUATION_OK ||
            step == WL_COLUMNAR_CONTINUATION_PROGRESS ||
            step == WL_COLUMNAR_CONTINUATION_DONE);
        wl_columnar_batch_payload_t *out =
            wl_columnar_join_batch_payload_sink_take(&s.owned);
        if (out) {
            uint32_t n; wl_columnar_batch_payload_schema(out, NULL, &n);
            rows += n;
            CHECK(!wl_columnar_batch_payload_inline_schema(out, NULL));
            CHECK(wl_columnar_batch_payload_release(out) == OK);
        }
        if (step == WL_COLUMNAR_CONTINUATION_DONE) break;
    }
    CHECK(rows == 2); wl_columnar_continuation_destroy(join);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

static void numeric_trace_references_refused(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(1 << 20);
    wl_intern_t *intern = wl_intern_create();
    CHECK(intern && wl_intern_attach_memory_governor(intern, g) == 0);
    CHECK(wl_intern_put(intern, "unary") == 0);
    col_rel_t *plain = source(1, 2);
    wl_columnar_trace_store_t *nt = segmented(g, NULL, plain, 1, 0);
    wl_columnar_trace_store_snapshot_t *ns = snapshot(nt);
    wl_columnar_batch_payload_t *np = capture(g, NULL, plain, 0);
    col_rel_destroy(plain);
    for (unsigned inlined = 0; inlined < 2; inlined++) for (unsigned empty = 0;
            empty < 2; empty++){
            wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                             inlined ? WIRELOG_TYPE_INT64 :
                                             WIRELOG_TYPE_STRING};
            wirelog_column_t columns[] = {{.type = WIRELOG_TYPE_INT64},
                                          {.type = WIRELOG_TYPE_INT64,
                                           .compound_kind =
                                               WIRELOG_COMPOUND_KIND_INLINE,
                                           .compound_arity = 1,
                                           .compound_inline_col_offset = 1}};
            wl_columnar_trace_store_t *rt;
            CHECK(wl_columnar_trace_store_create_reference(g, types, 2,
                inlined ? columns : NULL,
                inlined ? 2 : 0, intern, &rt) == OK);
            if (!empty) {
                col_rel_t *r;
                CHECK(wl_columnar_relation_new_auto_governed("reference_guard",
                    2, 1, false, g, &r) == 0);
                CHECK(col_rel_set_column_types(r, types, 2) == 0);
                if (inlined) {
                    const col_rel_logical_col_t layout[] =
                    {{WIRELOG_COMPOUND_KIND_NONE, 0, 0},
                     {WIRELOG_COMPOUND_KIND_INLINE, 1, 1}};
                    CHECK(col_rel_apply_compound_schema(r, layout, 2) == 0);
                }
                int64_t values[] = {7, 0};
                CHECK(col_rel_append_row(r, values) == 0);
                CHECK(wl_columnar_trace_store_append_capture_reference(rt, r,
                    inlined ? columns : NULL,
                    inlined ? 2 : 0, intern, WL_COLUMNAR_BATCH_PAYLOAD_RESIDENT,
                    NULL, NULL, NULL) == OK);
                col_rel_destroy(r);
            }
            wl_columnar_trace_store_snapshot_t *rs = snapshot(rt);
            uint64_t before = reserved(g);
            wl_columnar_continuation_t *join = (void *)1;
            for (unsigned side = 0; side < 2; side++) {
                CHECK(wl_columnar_payload_join_create_trace_bounded(g,
                    side ? ns : rs, side ? rs : ns,
                    0, 0, 32, index_cap(1), NULL, NULL,
                    &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
                CHECK(wl_columnar_payload_join_create_weighted_trace_bounded(g,
                    side ? ns : rs, side ? rs : ns,
                    0, 0, 32, index_cap(1), NULL, NULL,
                    &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            }
            CHECK(wl_columnar_payload_join_create_delta_trace_bounded(g, np, rs,
                0, 0, 32,
                index_cap(1), NULL, NULL,
                &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
            CHECK(reserved(g) == before);
            wl_columnar_trace_store_snapshot_release(rs);
            wl_columnar_trace_store_retire(rt);
        }
    wl_columnar_trace_store_snapshot_release(ns);
    wl_columnar_trace_store_retire(nt);
    CHECK(wl_columnar_batch_payload_release(np) == OK); wl_intern_free(intern);
    CHECK(reserved(g) == 0); wl_columnar_memory_governor_ref_release(g);
}

int main(void)
{
    reference_trace_joins(); reference_trace_retirement();
    reference_trace_numeric_inline();
    numeric_trace_references_refused();
    trace_partial_overflow(); trace_progress_and_floor();
    trace_generations(); trace_signed_errors();
    CHECK(trace_causal_budget(128, false) == trace_causal_budget(512, false));
    trace_causal_budget(512, true);
    trace_combinations(); trace_failures(); trace_limits();
    reference_joins(false); reference_joins(true);
    projected();
    combinations(); failures(); budget(false); budget(true); misses();
    concurrent_self_join(false); concurrent_self_join(true);
    unsupported_and_empty();
    weighted_storage(); weighted_boundaries(); weighted_partial_overflow();
    weighted_coordinates(); weighted_floor(); weighted_stop();
    for (uint32_t window = 1; window <= 3; window += 2) {
        index_limit = index_cap(window);
        combinations(); weighted_storage(); failures(); unsupported_and_empty();
        weighted_boundaries(); weighted_coordinates(); weighted_floor();
        weighted_partial_overflow(); weighted_stop();
        concurrent_self_join(false); concurrent_self_join(true);
    }
    index_limit = 0;
    window_budget(false, false); window_budget(true, false);
    window_budget(false, true); window_budget(true, true); window_limits();
    streaming_unwind();
    window_build_progress(); window_publication(); window_misses();
    puts("payload_join: PASS"); return 0;
}
