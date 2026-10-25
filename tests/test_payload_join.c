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
bool wl_columnar_payload_join_test_allocation(void)
{
    return allocation_fail && --allocation_fail == 0;
}
bool wl_columnar_spill_test_allocation(void) {
    return false;
}
int wl_columnar_spill_test_io(bool writing, uint64_t offset, size_t *length)
{
    (void)offset; (void)length;
    return !writing &&
           (read_fail || (read_fail_after && --read_fail_after == 0)) ? EIO : 0;
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
    sink_t *s = v; return s->inner.reserve(s->inner.context, bytes, rows);
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
            CHECK(wl_columnar_payload_join_create(g, left, right, 0, 0,
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
    CHECK(wl_columnar_payload_join_create(other, p, p, 0, 0, 100, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_INVALID && !join);
    CHECK(wl_columnar_payload_join_create(g, p, p, 9, 0, 100, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_INVALID);
    CHECK(wl_columnar_payload_join_create(g, p, p, 2, 0, 100, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 47, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_MEMORY);
    for (unsigned n = 1; n <= 2; n++) {
        allocation_fail = n;
        CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 96, NULL, NULL,
            &join) == WL_COLUMNAR_SPILL_ALLOCATION);
        CHECK(!join && reserved(g) == before);
    }
    read_fail = true;
    CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 96, NULL, NULL,
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
        wl_columnar_spill_status_t status = wl_columnar_payload_join_create(g,
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
    CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 96, cancelled, &stop,
        &join) == WL_COLUMNAR_SPILL_CANCELLED);
    CHECK(!join && reserved(g) == before); stop = false;
    for (unsigned mode = 0; mode < 2; mode++) {
        CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 96, cancelled,
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
    CHECK(wl_columnar_payload_join_create(g, p, bad, 0, 0, 96, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_payload_join_create(g, bad, p, 0, 0, 96, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_batch_payload_release(bad) == OK); col_rel_destroy(r);
    CHECK(reserved(g) == before);
    wl_columnar_spill_store_retire(store);
    CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 96, NULL, NULL,
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
    CHECK((weighted ? wl_columnar_payload_join_create_weighted :
        wl_columnar_payload_join_create)(g, left, right, 0, 0, 67 * 8 * 2,
        NULL, NULL, &join) == OK);
    CHECK(reserved(g) < limit);
    /* Refuse a second index/scratch while all remaining budget is held. */
    wl_columnar_memory_reservation_t slack;
    wl_columnar_memory_reservation_init(&slack);
    CHECK(wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(g), limit - reserved(g),
            &slack) == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    wl_columnar_continuation_t *denied;
    CHECK((weighted ? wl_columnar_payload_join_create_weighted :
        wl_columnar_payload_join_create)(g, left, right, 0, 0, 67 * 8, NULL,
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
    CHECK(wl_columnar_payload_join_create(g, left, right, 0, 0, 32, NULL, NULL,
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
        CHECK((weighted ? wl_columnar_payload_join_create_weighted :
            wl_columnar_payload_join_create)(g, p, p, 0, 0, 96, NULL, NULL,
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
    CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 64, NULL, NULL,
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
    CHECK(wl_columnar_payload_join_create(g, p, p, 0, 0, 64, NULL, NULL,
        &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(wl_columnar_batch_payload_release(p) == OK); col_rel_destroy(r);
    wl_intern_free(intern);
    r = source(0, 2);
    wl_columnar_batch_payload_t *empty = capture(g, NULL, r, 0);
    col_rel_destroy(r);
    r = source(1, 2); p = capture(g, NULL, r, 0); col_rel_destroy(r);
    for (unsigned side = 0; side < 2; side++) {
        CHECK(wl_columnar_payload_join_create(g, side ? p : empty,
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
                CHECK(wl_columnar_payload_join_create(g, left, right, 0, 0, 80,
                    NULL, NULL, &join) == WL_COLUMNAR_SPILL_UNSUPPORTED);
                CHECK(wl_columnar_payload_join_create_weighted(g, left, right,
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
            CHECK(wl_columnar_payload_join_create_weighted(g, left, right, 0, 0,
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
    CHECK(wl_columnar_payload_join_create_weighted(g, left, right, 0, 0, 64,
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
        CHECK(wl_columnar_payload_join_create_weighted(g, plain, bad, 0, 0, 64,
            NULL, NULL, &join) == WL_COLUMNAR_SPILL_UNSUPPORTED && !join);
        CHECK(wl_columnar_payload_join_create_weighted(g, bad, plain, 0, 0, 64,
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
        (weighted ? wl_columnar_payload_join_create_weighted :
        wl_columnar_payload_join_create)(g, p, p, 0, 0, 32 * 64, NULL, NULL,
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
        CHECK(wl_columnar_payload_join_create_weighted(g, p, p, 0, 0, 64,
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

int main(void)
{
    combinations(); failures(); budget(false); budget(true); misses();
    concurrent_self_join(false); concurrent_self_join(true);
    unsupported_and_empty();
    weighted_storage(); weighted_boundaries(); weighted_partial_overflow();
    weighted_coordinates(); weighted_floor(); weighted_stop();
    puts("payload_join: PASS"); return 0;
}
