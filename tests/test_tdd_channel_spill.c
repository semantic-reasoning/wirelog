/*
 * test_tdd_channel_spill.c - relation adapters for the TDD channel
 * (Issue #1373)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Batches travel through a real channel and are spilled to a real store in
 * a private directory under WL_TDD_CHANNEL_SPILL_DIR (the build tree when
 * run by meson), never under the system temporary directory.
 *
 * Usage: test_tdd_channel_spill [test-name]   (no name runs every test)
 */
#define _POSIX_C_SOURCE 200809L
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE /* mkdtemp() under _POSIX_C_SOURCE on macOS */
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/eval_tdd_channel.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/session.h"
#include "../wirelog/thread.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

#define CHECK(x)                                                      \
        do {                                                          \
            if (!(x)) {                                               \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
                abort();                                              \
            }                                                         \
        } while (0)

#define OK WL_COLUMNAR_SPILL_OK

static char spill_dir[1024];
static int consumer_owner;
static int producer_owner;

/* ------------------------------------------------------------------ */
/* Fixtures                                                            */
/* ------------------------------------------------------------------ */

static wl_columnar_memory_governor_ref_t *
governor(uint64_t budget)
{
    wl_columnar_memory_resolution_t r;
    memset(&r, 0, sizeof(r));
    r.budget_bytes = budget;
    r.usable_bytes = budget;
    r.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    r.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    r.status = WL_COLUMNAR_MEMORY_OK;
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g);
    return g;
}

static uint64_t
reserved(wl_columnar_memory_governor_ref_t *g)
{
    return wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
}

static wl_columnar_spill_store_t *
open_store(wl_columnar_memory_governor_ref_t *g, uint64_t quota)
{
    wl_columnar_spill_store_t *store = NULL;
    CHECK(wl_columnar_spill_store_open(spill_dir, g, quota, 256, &store)
        == OK);
    return store;
}

static col_delta_timestamp_t
ts(uint32_t iteration, uint32_t stratum, uint32_t worker, int64_t mult)
{
    col_delta_timestamp_t t;
    memset(&t, 0, sizeof(t));
    t.iteration = iteration;
    t.stratum = stratum;
    t.worker = worker;
    t.multiplicity = mult;
    return t;
}

/* A governed two-column (INT64, FLOAT) relation with timestamps: row i holds
 * (values[i], values[i] / 4.0) stamped with stamps[i]. */
static col_rel_t *
make_rel(wl_columnar_memory_governor_ref_t *g, const char *name,
    const int64_t *values, const col_delta_timestamp_t *stamps, uint32_t n)
{
    col_rel_t *r = NULL;
    CHECK(wl_columnar_relation_new_auto_governed(name, 2, n ? n : 1, true, g,
        &r) == 0);
    wirelog_column_type_t types[2] = { WIRELOG_TYPE_INT64,
                                       WIRELOG_TYPE_FLOAT };
    CHECK(col_rel_set_column_types(r, types, 2) == 0);
    for (uint32_t i = 0; i < n; i++) {
        double f = (double)values[i] / 4.0;
        int64_t row[2];
        row[0] = values[i];
        memcpy(&row[1], &f, sizeof(f));
        CHECK(col_rel_append_row(r, row) == 0);
    }
    CHECK(r->timestamps && r->timestamp_capacity >= n);
    for (uint32_t i = 0; i < n; i++)
        r->timestamps[i] = stamps[i];
    return r;
}

static col_rel_t *
make_dst(wl_columnar_memory_governor_ref_t *g)
{
    col_rel_t *r = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("dst", 2, 1, true, g, &r)
        == 0);
    wirelog_column_type_t types[2] = { WIRELOG_TYPE_INT64,
                                       WIRELOG_TYPE_FLOAT };
    CHECK(col_rel_set_column_types(r, types, 2) == 0);
    return r;
}

static void
check_row(const col_rel_t *r, uint32_t i, int64_t value,
    col_delta_timestamp_t stamp)
{
    double f = (double)value / 4.0;
    int64_t bits;
    memcpy(&bits, &f, sizeof(f));
    CHECK(i < r->nrows);
    CHECK(r->columns[0][i] == value && r->columns[1][i] == bits);
    CHECK(r->timestamps);
    CHECK(r->timestamps[i].iteration == stamp.iteration);
    CHECK(r->timestamps[i].stratum == stamp.stratum);
    CHECK(r->timestamps[i].worker == stamp.worker);
    CHECK(r->timestamps[i].multiplicity == stamp.multiplicity);
}

static wl_tdd_channel_t *
open_channel(wl_columnar_memory_governor_ref_t *g, uint32_t lanes)
{
    wl_tdd_channel_t *ch = wl_tdd_channel_create(lanes, UINT64_MAX,
            wl_columnar_memory_governor_ref_get(g), &consumer_owner);
    CHECK(ch);
    CHECK(wl_tdd_channel_begin_round(ch, 4, 1) == 0);
    return ch;
}

static void
publish(wl_tdd_channel_t *ch, uint32_t lane, uint32_t rel_idx, col_rel_t *r)
{
    wl_tdd_batch_t *batch = NULL;
    CHECK(wl_tdd_channel_batch_new(ch, lane, rel_idx, &producer_owner,
        &batch) == 0);
    uint64_t charge = col_rel_transport_bytes(r);
    CHECK(wl_tdd_channel_reserve(ch, batch, charge ? charge : 1) == 0);
    wl_tdd_channel_publish(ch, batch, r, r->nrows);
}

/* Close every lane and pump to COMPLETE; the taken batches, in take order,
 * land in out[0..*n). */
static void
take_all(wl_tdd_channel_t *ch, uint32_t lanes, wl_tdd_batch_t **out,
    uint32_t *n, uint32_t max)
{
    for (uint32_t lane = 0; lane < lanes; lane++)
        wl_tdd_channel_close_lane(ch, lane, 0);
    *n = 0;
    for (;;) {
        wl_tdd_batch_t *batch = NULL;
        int err = 0;
        wl_tdd_pump_result_t r = wl_tdd_channel_pump(ch, &batch, &err);
        if (r == WL_TDD_PUMP_COMPLETE)
            break;
        CHECK(r == WL_TDD_PUMP_DATA && *n < max);
        CHECK(batch->state == WL_TDD_BATCH_CONSUMER);
        out[(*n)++] = batch;
    }
}

/* Link batches into a seq-ordered intrusive list. */
static wl_tdd_batch_t *
link_list(wl_tdd_batch_t **batches, uint32_t n)
{
    for (uint32_t i = 0; i + 1 < n; i++)
        batches[i]->next = batches[i + 1];
    if (n)
        batches[n - 1]->next = NULL;
    return n ? batches[0] : NULL;
}

typedef struct {
    wl_tdd_batch_t *batches[8];
    int rcs[8];
    uint32_t count;
} sink_log_t;

static void
record_refusal(void *ctx, wl_tdd_batch_t *batch, int rc)
{
    sink_log_t *log = ctx;
    CHECK(log->count < 8);
    log->batches[log->count] = batch;
    log->rcs[log->count] = rc;
    log->count++;
}

static uint64_t
retired_count(wl_tdd_channel_t *ch)
{
    wl_tdd_channel_stats_t s;
    wl_tdd_channel_stats(ch, &s);
    return s.retired;
}

/* A session with an injected enforcing governor and a spill store under the
 * private directory; the coalesce paths need the coordinator. */
static wl_session_t *
make_session(wl_columnar_memory_governor_ref_t *g)
{
    static wl_plan_t plan;
    wl_session_options_t options;
    wl_session_options_init(&options);
    options.memory_governor = g;
    options.spill_directory = spill_dir;
    options.spill_quota_bytes = UINT64_C(1) << 24;
    options.spill_max_row_bytes = 256;
    memset(&plan, 0, sizeof(plan));
    wl_session_t *session = NULL;
    CHECK(wl_session_create_with_options(wl_backend_columnar(), &plan, 1,
        &options, &session) == 0);
    CHECK(COL_SESSION(session)->spill_store);
    return session;
}

/* ------------------------------------------------------------------ */
/* Consumer spill and rehydrate                                        */
/* ------------------------------------------------------------------ */

static void
test_round_trip(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 26);
    wl_columnar_spill_store_t *store = open_store(g, UINT64_C(1) << 20);
    wl_tdd_channel_t *ch = open_channel(g, 2);
    const int64_t values[6] = { -3, 0, 7, 7, INT64_C(1) << 40, -1 };
    const col_delta_timestamp_t stamps[6] = {
        ts(0, 4, 1, 1), ts(1, 4, 1, -1), ts(1, 4, 1, 1),
        ts(2, 4, 1, -1), ts(5, 4, 1, 3), ts(9, 4, 1, -2),
    };
    col_rel_t *r = make_rel(g, "rt", values, stamps, 6);
    publish(ch, 1, 0, r);
    wl_tdd_batch_t *b[1];
    uint32_t n = 0;
    take_all(ch, 2, b, &n, 1);
    CHECK(n == 1 && b[0]->kind == WL_TDD_BATCH_RESIDENT && b[0]->payload == r);

    uint64_t floor_bytes = 0, reader_bytes = 0;
    CHECK(wl_columnar_batch_payload_floor(2, 6,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, &floor_bytes, &reader_bytes) == OK);
    wl_columnar_spill_status_t status = WL_COLUMNAR_SPILL_INVALID;
    CHECK(wl_columnar_eval_tdd_batch_spill(b[0], g, store, &status) == 0);
    CHECK(status == OK);
    CHECK(b[0]->kind == WL_TDD_BATCH_SPILLED && b[0]->payload);
    CHECK(b[0]->payload != (void *)r && b[0]->charge == floor_bytes);
    CHECK(!(b[0]->flags & WL_TDD_BATCH_FLAG_UNSPILLABLE));
    CHECK(wl_columnar_spill_store_bytes(store) > 0);

    col_rel_t *dst = make_dst(g);
    CHECK(wl_columnar_eval_tdd_batch_rehydrate(b[0], dst, 2) == 0);
    CHECK(dst->nrows == 6);
    for (uint32_t i = 0; i < 6; i++)
        check_row(dst, i, values[i], stamps[i]);
    /* Worker 1 is out of range for a one-worker round (#1373). */
    col_rel_t *narrow = make_dst(g);
    CHECK(wl_columnar_eval_tdd_batch_rehydrate(b[0], narrow, 1) == EPROTO);
    /* The batch survives a failed rehydrate and can be read again. */
    CHECK(b[0]->kind == WL_TDD_BATCH_SPILLED && b[0]->payload);
    CHECK(col_rel_destroy_checked(narrow) == 0);
    CHECK(col_rel_destroy_checked(dst) == 0);

    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    wl_columnar_eval_tdd_batch_discard(ch, b[0], &sink);
    CHECK(log.count == 0 && retired_count(ch) == 1);
    CHECK(wl_columnar_spill_store_bytes(store) == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_string_refused(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 26);
    wl_columnar_spill_store_t *store = open_store(g, UINT64_C(1) << 20);
    wl_tdd_channel_t *ch = open_channel(g, 1);
    const int64_t values[2] = { 0, 1 };
    const col_delta_timestamp_t stamps[2] = { ts(0, 4, 0, 1),
                                              ts(0, 4, 0, 1) };
    col_rel_t *r = make_rel(g, "str", values, stamps, 2);
    wirelog_column_type_t types[2] = { WIRELOG_TYPE_STRING,
                                       WIRELOG_TYPE_FLOAT };
    CHECK(col_rel_set_column_types(r, types, 2) == 0);
    publish(ch, 0, 0, r);
    wl_tdd_batch_t *b[1];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 1);
    CHECK(n == 1);
    uint64_t charge = b[0]->charge, before = reserved(g);
    col_relation_snapshot_t snap = wl_columnar_relation_snapshot(r);
    wl_columnar_spill_status_t status = OK;
    CHECK(wl_columnar_eval_tdd_batch_spill(b[0], g, store, &status)
        == ENOTSUP);
    CHECK(status == WL_COLUMNAR_SPILL_UNSUPPORTED);
    CHECK(b[0]->flags & WL_TDD_BATCH_FLAG_UNSPILLABLE);
    CHECK(b[0]->kind == WL_TDD_BATCH_RESIDENT && b[0]->payload == r);
    CHECK(b[0]->charge == charge && reserved(g) == before);
    CHECK(wl_columnar_relation_snapshot_equal(snap,
        wl_columnar_relation_snapshot(r)));
    CHECK(r->nrows == 2 && r->columns[0][1] == 1);
    CHECK(wl_columnar_spill_store_bytes(store) == 0);
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    wl_columnar_eval_tdd_batch_discard(ch, b[0], &sink);
    CHECK(log.count == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_quota_exhausted(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 26);
    wl_columnar_spill_store_t *tiny = open_store(g, 32);
    wl_tdd_channel_t *ch = open_channel(g, 1);
    const int64_t values[3] = { 1, 2, 3 };
    const col_delta_timestamp_t stamps[3] = {
        ts(0, 4, 0, 1), ts(0, 4, 0, 1), ts(0, 4, 0, 1),
    };
    col_rel_t *r = make_rel(g, "quota", values, stamps, 3);
    publish(ch, 0, 0, r);
    wl_tdd_batch_t *b[1];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 1);
    uint64_t charge = b[0]->charge, before = reserved(g);
    col_relation_snapshot_t snap = wl_columnar_relation_snapshot(r);
    wl_columnar_spill_status_t status = OK;
    CHECK(wl_columnar_eval_tdd_batch_spill(b[0], g, tiny, &status) == EIO);
    CHECK(status == WL_COLUMNAR_SPILL_QUOTA);
    CHECK(b[0]->flags & WL_TDD_BATCH_FLAG_UNSPILLABLE);
    CHECK(b[0]->kind == WL_TDD_BATCH_RESIDENT && b[0]->payload == r);
    CHECK(b[0]->charge == charge && reserved(g) == before);
    CHECK(wl_columnar_relation_snapshot_equal(snap,
        wl_columnar_relation_snapshot(r)));
    for (uint32_t i = 0; i < 3; i++)
        check_row(r, i, values[i], stamps[i]);
    CHECK(wl_columnar_spill_store_bytes(tiny) == 0);
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    wl_columnar_eval_tdd_batch_discard(ch, b[0], &sink);
    CHECK(log.count == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    CHECK(wl_columnar_spill_store_close(tiny) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_reader_floor(void)
{
    const uint64_t budget = UINT64_C(1) << 24;
    wl_columnar_memory_governor_ref_t *g = governor(budget);
    wl_columnar_spill_store_t *store = open_store(g, UINT64_C(1) << 20);
    wl_tdd_channel_t *ch = open_channel(g, 1);
    const int64_t values[4] = { 10, 20, 30, 40 };
    const col_delta_timestamp_t stamps[4] = {
        ts(3, 4, 0, 1), ts(3, 4, 0, 1), ts(3, 4, 0, 1), ts(3, 4, 0, 1),
    };
    publish(ch, 0, 0, make_rel(g, "floor", values, stamps, 4));
    wl_tdd_batch_t *b[1];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 1);
    CHECK(wl_columnar_eval_tdd_batch_spill(b[0], g, store, NULL) == 0);

    col_rel_t *dst = make_dst(g);
    bool denied = false;
    CHECK(col_rel_reserve_capacity_admitted(dst, 4, &denied) == 0);
    uint64_t payload_bytes = 0, reader_bytes = 0;
    CHECK(wl_columnar_batch_payload_floor(2, 4,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, &payload_bytes, &reader_bytes)
        == OK);
    wl_columnar_spill_floor_t sfloor;
    CHECK(wl_columnar_spill_floor(spill_dir, 2, &sfloor) == OK);
    uint64_t need = reader_bytes + sfloor.reader_bytes;
    CHECK(need > 0);
    uint64_t baseline = reserved(g);

    /* One byte short of the reader floor: refused before any row moves. */
    wl_columnar_memory_reservation_t hold;
    wl_columnar_memory_reservation_init(&hold);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(g),
        budget - baseline - need + 1, &hold));
    CHECK(wl_columnar_eval_tdd_batch_rehydrate(b[0], dst, 1) == ENOSPC);
    CHECK(dst->nrows == 0);
    CHECK(wl_columnar_memory_rollback(&hold));
    CHECK(reserved(g) == baseline);

    /* Exactly the reader floor of headroom is enough, and every byte of it
     * comes back once the reader is released. */
    wl_columnar_memory_reservation_init(&hold);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(g),
        budget - baseline - need, &hold));
    CHECK(wl_columnar_eval_tdd_batch_rehydrate(b[0], dst, 1) == 0);
    CHECK(wl_columnar_memory_rollback(&hold));
    CHECK(dst->nrows == 4 && reserved(g) == baseline);
    for (uint32_t i = 0; i < 4; i++)
        check_row(dst, i, values[i], stamps[i]);

    CHECK(col_rel_destroy_checked(dst) == 0);
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    wl_columnar_eval_tdd_batch_discard(ch, b[0], &sink);
    CHECK(log.count == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    CHECK(wl_columnar_spill_store_close(store) == OK);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* ------------------------------------------------------------------ */
/* Worker-style delta spilled while another lane is still open         */
/* ------------------------------------------------------------------ */

typedef struct {
    wl_tdd_channel_t *ch;
    wl_columnar_memory_governor_ref_t *g;
    uint32_t lane;
    uint32_t eff_iter;
} producer_t;

/* Build the delta as a worker does: a governed private relation with an
 * admitted timestamp image, stamped (eff_iter, stratum, worker, +1) before
 * it is published. */
static void *
producer_main(void *arg)
{
    producer_t *p = arg;
    col_rel_t *delta = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("$d", 2, 8, true, p->g,
        &delta) == 0);
    wirelog_column_type_t types[2] = { WIRELOG_TYPE_INT64,
                                       WIRELOG_TYPE_FLOAT };
    CHECK(col_rel_set_column_types(delta, types, 2) == 0);
    for (int64_t i = 0; i < 8; i++) {
        double f = (double)(100 + i) / 4.0;
        int64_t row[2];
        row[0] = 100 + i;
        memcpy(&row[1], &f, sizeof(f));
        CHECK(col_rel_append_row(delta, row) == 0);
    }
    for (uint32_t i = 0; i < delta->nrows; i++)
        delta->timestamps[i] = ts(p->eff_iter, 4, p->lane, 1);
    publish(p->ch, p->lane, 3, delta);
    wl_tdd_channel_close_lane(p->ch, p->lane, 0);
    return NULL;
}

static void
test_worker_delta_spill(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_tdd_channel_t *ch = open_channel(coord->memory_governor, 2);
    producer_t p = { ch, coord->memory_governor, 0, 6 };
    wl_thread_t thread;
    CHECK(wl_thread_create(&thread, producer_main, &p) == 0);
    wl_tdd_batch_t *batch = NULL;
    int err = 0;
    CHECK(wl_tdd_channel_pump(ch, &batch, &err) == WL_TDD_PUMP_DATA);
    /* Lane 1 is still open: the consumer spills without waiting for it. */
    wl_columnar_spill_status_t status = WL_COLUMNAR_SPILL_INVALID;
    CHECK(wl_columnar_eval_tdd_batch_spill(batch, coord->memory_governor,
        coord->spill_store, &status) == 0);
    CHECK(status == OK && batch->kind == WL_TDD_BATCH_SPILLED);
    wl_thread_join(&thread);
    wl_tdd_channel_close_lane(ch, 1, 0);
    wl_tdd_batch_t *none = NULL;
    CHECK(wl_tdd_channel_pump(ch, &none, &err) == WL_TDD_PUMP_COMPLETE);

    batch->next = NULL;
    wl_tdd_batch_t *list = batch;
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 2, "$d", &sink,
        &slot) == 0);
    CHECK(!list && slot && slot->nrows == 8 && log.count == 0);
    for (uint32_t i = 0; i < 8; i++)
        check_row(slot, i, 100 + (int64_t)i, ts(6, 4, 0, 1));
    CHECK(retired_count(ch) == 1);
    CHECK(col_rel_destroy_checked(slot) == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* ------------------------------------------------------------------ */
/* Coalesce                                                            */
/* ------------------------------------------------------------------ */

static void
test_coalesce_seq_order(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 1);
    const int64_t v0[2] = { 1, 2 }, v1[2] = { 3, 4 }, v2[1] = { 5 };
    const col_delta_timestamp_t s0[2] = { ts(0, 4, 0, 1), ts(0, 4, 0, 1) };
    const col_delta_timestamp_t s1[2] = { ts(1, 4, 0, 1), ts(1, 4, 0, 1) };
    const col_delta_timestamp_t s2[1] = { ts(2, 4, 0, 1) };
    publish(ch, 0, 2, make_rel(cg, "r", v0, s0, 2));
    publish(ch, 0, 2, make_rel(cg, "r", v1, s1, 2));
    publish(ch, 0, 2, make_rel(cg, "r", v2, s2, 1));
    wl_tdd_batch_t *b[3];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 3);
    CHECK(n == 3 && b[0]->seq < b[1]->seq && b[1]->seq < b[2]->seq);
    CHECK(wl_columnar_eval_tdd_batch_spill(b[1], cg, coord->spill_store,
        NULL) == 0);
    wl_tdd_batch_t *list = link_list(b, 3);
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == 0);
    CHECK(!list && slot && log.count == 0 && retired_count(ch) == 3);
    /* Differing iterations: no dedup, rows stay in seq order. */
    CHECK(slot->nrows == 5);
    check_row(slot, 0, 1, s0[0]);
    check_row(slot, 1, 2, s0[1]);
    check_row(slot, 2, 3, s1[0]);
    check_row(slot, 3, 4, s1[1]);
    check_row(slot, 4, 5, s2[0]);
    CHECK(strcmp(slot->name, "r") == 0);
    CHECK(wl_columnar_spill_store_bytes(coord->spill_store) == 0);
    CHECK(col_rel_destroy_checked(slot) == 0);

    /* A single resident batch is adopted without a copy. */
    CHECK(wl_tdd_channel_begin_round(ch, 4, 2) == 0);
    col_rel_t *only = make_rel(cg, "only", v0, s0, 2);
    publish(ch, 0, 2, only);
    take_all(ch, 1, b, &n, 1);
    list = link_list(b, 1);
    slot = NULL;
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == 0);
    CHECK(slot == only && !list && retired_count(ch) == 4);
    CHECK(col_rel_destroy_checked(slot) == 0);

    /* ... but only with a timestamp array. */
    CHECK(wl_tdd_channel_begin_round(ch, 4, 3) == 0);
    col_rel_t *bare = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("bare", 2, 1, false, cg,
        &bare) == 0);
    int64_t row[2] = { 1, 0 };
    CHECK(col_rel_append_row(bare, row) == 0);
    publish(ch, 0, 2, bare);
    take_all(ch, 1, b, &n, 1);
    list = link_list(b, 1);
    slot = NULL;
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == EPROTO);
    CHECK(list == b[0] && !slot && b[0]->payload == bare);
    CHECK(b[0]->state == WL_TDD_BATCH_CONSUMER);
    wl_columnar_eval_tdd_discard_list(ch, &list, &sink);
    CHECK(!list && log.count == 0 && retired_count(ch) == 5);

    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_coalesce_overlap_dedup(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 2);
    const col_delta_timestamp_t u = ts(5, 4, 1, 1);
    const int64_t v0[3] = { 3, 1, 2 }, v1[2] = { 4, 3 }, v2[2] = { 2, 9 };
    const col_delta_timestamp_t s[3] = { u, u, u };
    publish(ch, 1, 0, make_rel(cg, "ov", v0, s, 3));
    publish(ch, 1, 0, make_rel(cg, "ov", v1, s, 2));
    publish(ch, 1, 0, make_rel(cg, "ov", v2, s, 2));
    wl_tdd_batch_t *b[3];
    uint32_t n = 0;
    take_all(ch, 2, b, &n, 3);
    CHECK(n == 3);
    CHECK(wl_columnar_eval_tdd_batch_spill(b[1], cg, coord->spill_store,
        NULL) == 0);
    wl_tdd_batch_t *list = link_list(b, 3);
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 2, NULL, &sink,
        &slot) == 0);
    CHECK(!list && slot && log.count == 0);
    /* {3,1,2} + {4,3} + {2,9}: each of 1, 2, 3, 4, 9 exactly once. */
    CHECK(slot->nrows == 5);
    const int64_t expect[5] = { 1, 2, 3, 4, 9 };
    for (uint32_t i = 0; i < 5; i++) {
        uint32_t hits = 0;
        for (uint32_t row = 0; row < slot->nrows; row++)
            if (slot->columns[0][row] == expect[i]) {
                check_row(slot, row, expect[i], u);
                hits++;
            }
        CHECK(hits == 1);
    }
    CHECK(col_rel_destroy_checked(slot) == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_coalesce_signed_kept(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 1);
    const int64_t v[1] = { 7 };
    const col_delta_timestamp_t s0[1] = { ts(1, 4, 0, 1) };
    const col_delta_timestamp_t s1[1] = { ts(1, 4, 0, -1) };
    const col_delta_timestamp_t s2[1] = { ts(2, 4, 0, 1) };
    publish(ch, 0, 0, make_rel(cg, "sg", v, s0, 1));
    publish(ch, 0, 0, make_rel(cg, "sg", v, s1, 1));
    publish(ch, 0, 0, make_rel(cg, "sg", v, s2, 1));
    wl_tdd_batch_t *b[3];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 3);
    CHECK(wl_columnar_eval_tdd_batch_spill(b[1], cg, coord->spill_store,
        NULL) == 0);
    wl_tdd_batch_t *list = link_list(b, 3);
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == 0);
    CHECK(!list && slot && slot->nrows == 3);
    check_row(slot, 0, 7, s0[0]);
    check_row(slot, 1, 7, s1[0]);
    check_row(slot, 2, 7, s2[0]);
    CHECK(col_rel_destroy_checked(slot) == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void check_list_intact(wl_tdd_batch_t *list, wl_tdd_batch_t **b,
    void *const *payloads, uint32_t n);

/* Uniform batches whose rows all carry -1 are not set-unioned: a union
 * would turn two retractions of one row into one. */
static void
test_coalesce_uniform_negative_kept(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 1);
    const int64_t v[1] = { 7 };
    const col_delta_timestamp_t s[1] = { ts(1, 4, 0, -1) };
    publish(ch, 0, 0, make_rel(cg, "neg", v, s, 1));
    publish(ch, 0, 0, make_rel(cg, "neg", v, s, 1));
    wl_tdd_batch_t *b[2];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 2);
    wl_tdd_batch_t *list = link_list(b, 2);
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == 0);
    CHECK(!list && slot && slot->nrows == 2);
    check_row(slot, 0, 7, s[0]);
    check_row(slot, 1, 7, s[0]);
    CHECK(col_rel_destroy_checked(slot) == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* A list that mixes relations, or is not in increasing seq, is refused
 * with EINVAL and left as it was. */
static void
test_coalesce_invalid_list(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 1);
    const int64_t v[1] = { 3 };
    const col_delta_timestamp_t s[1] = { ts(1, 0, 0, 1) };
    publish(ch, 0, 0, make_rel(cg, "inv", v, s, 1));
    publish(ch, 0, 0, make_rel(cg, "inv", v, s, 1));
    publish(ch, 0, 1, make_rel(cg, "other", v, s, 1));
    wl_tdd_batch_t *b[3];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 3);
    void *payloads[3] = { b[0]->payload, b[1]->payload, b[2]->payload };
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    col_rel_t *slot = NULL;

    wl_tdd_batch_t *mixed[2] = { b[0], b[2] };
    void *mixed_payloads[2] = { payloads[0], payloads[2] };
    wl_tdd_batch_t *list = link_list(mixed, 2);
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == EINVAL);
    CHECK(slot == NULL);
    check_list_intact(list, mixed, mixed_payloads, 2);

    wl_tdd_batch_t *reversed[2] = { b[1], b[0] };
    void *reversed_payloads[2] = { payloads[1], payloads[0] };
    list = link_list(reversed, 2);
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == EINVAL);
    CHECK(slot == NULL);
    check_list_intact(list, reversed, reversed_payloads, 2);

    list = link_list(b, 3);
    wl_columnar_eval_tdd_discard_list(ch, &list, &sink);
    CHECK(list == NULL && log.count == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);

    /* Two lanes: lane 1's second batch follows lane 0's first in seq, so
     * only the worker check can refuse the pair. */
    ch = open_channel(cg, 2);
    publish(ch, 0, 0, make_rel(cg, "inv", v, s, 1));
    publish(ch, 1, 0, make_rel(cg, "inv", v, s, 1));
    publish(ch, 1, 0, make_rel(cg, "inv", v, s, 1));
    take_all(ch, 2, b, &n, 3);
    wl_tdd_batch_t *lane0 = NULL, *lane1_second = NULL;
    for (uint32_t i = 0; i < n; i++) {
        if (b[i]->worker == 0)
            lane0 = b[i];
        else if (b[i]->seq == 1)
            lane1_second = b[i];
    }
    CHECK(lane0 && lane1_second && lane0->seq < lane1_second->seq);
    wl_tdd_batch_t *workers[2] = { lane0, lane1_second };
    void *worker_payloads[2] = { lane0->payload, lane1_second->payload };
    list = link_list(workers, 2);
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 2, NULL, &sink,
        &slot) == EINVAL);
    CHECK(slot == NULL);
    check_list_intact(list, workers, worker_payloads, 2);
    list = link_list(b, 3);
    wl_columnar_eval_tdd_discard_list(ch, &list, &sink);
    CHECK(list == NULL && log.count == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* A relation without timestamps is not spilled: the payload would stamp
 * its rows {0,0,0,+1}. */
static void
test_spill_untimestamped_refused(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 1);
    col_rel_t *r = NULL;
    CHECK(wl_columnar_relation_new_auto_governed("plain", 1, 1, false, cg,
        &r) == 0);
    int64_t row[1] = { 5 };
    CHECK(col_rel_append_row(r, row) == 0);
    CHECK(r->timestamps == NULL);
    publish(ch, 0, 0, r);
    wl_tdd_batch_t *b[1];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 1);
    uint64_t charge = b[0]->charge;
    wl_columnar_spill_status_t status = WL_COLUMNAR_SPILL_OK;
    CHECK(wl_columnar_eval_tdd_batch_spill(b[0], cg, coord->spill_store,
        &status) == EINVAL);
    CHECK(status == WL_COLUMNAR_SPILL_INVALID);
    CHECK(b[0]->kind == WL_TDD_BATCH_RESIDENT && b[0]->payload == r
        && b[0]->charge == charge
        && !(b[0]->flags & WL_TDD_BATCH_FLAG_UNSPILLABLE));
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    wl_columnar_eval_tdd_batch_discard(ch, b[0], &sink);
    CHECK(log.count == 0);
    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* Every descriptor on a list a failed coalesce returned is still CONSUMER
 * with its payload and rows intact. */
static void
check_list_intact(wl_tdd_batch_t *list, wl_tdd_batch_t **b,
    void *const *payloads, uint32_t n)
{
    wl_tdd_batch_t *it = list;
    for (uint32_t i = 0; i < n; i++) {
        CHECK(it == b[i]);
        CHECK(it->state == WL_TDD_BATCH_CONSUMER && it->payload == payloads[i]);
        it = it->next;
    }
    CHECK(it == NULL);
}

static void
test_coalesce_failure_owned(void)
{
    const uint64_t budget = UINT64_C(1) << 26;
    wl_columnar_memory_governor_ref_t *g = governor(budget);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 2);
    const int64_t v0[2] = { 1, 2 }, v1[2] = { 3, 4 }, v2[2] = { 5, 6 };
    /* Uniform stamps, so a successful coalesce also consolidates. */
    const col_delta_timestamp_t s0[2] = { ts(0, 4, 1, 1), ts(0, 4, 1, 1) };
    publish(ch, 1, 0, make_rel(cg, "f", v0, s0, 2));
    publish(ch, 1, 0, make_rel(cg, "f", v1, s0, 2));
    publish(ch, 1, 0, make_rel(cg, "f", v2, s0, 2));
    wl_tdd_batch_t *b[3];
    uint32_t n = 0;
    take_all(ch, 2, b, &n, 3);
    CHECK(wl_columnar_eval_tdd_batch_spill(b[1], cg, coord->spill_store,
        NULL) == 0);
    void *payloads[3] = { b[0]->payload, b[1]->payload, b[2]->payload };
    wl_tdd_batch_t *list = link_list(b, 3);
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    wl_columnar_memory_governor_t *raw = wl_columnar_memory_governor_ref_get(g);
    uint64_t baseline = reserved(g);

    /* 1. The governor refuses the slot outright. */
    wl_columnar_memory_reservation_t hold;
    wl_columnar_memory_reservation_init(&hold);
    CHECK(wl_columnar_memory_reserve(raw, budget - baseline, &hold));
    coord->memory_budget_denied = false;
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 2, NULL, &sink,
        &slot) == ENOSPC);
    CHECK(coord->memory_budget_denied);
    CHECK(wl_columnar_memory_rollback(&hold));
    CHECK(!slot && reserved(g) == baseline);
    check_list_intact(list, b, payloads, 3);

    /* 2. A failure after the first batch was appended: the spilled batch
     * names worker 1 in a one-worker round. */
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == EPROTO);
    CHECK(!slot && reserved(g) == baseline);
    check_list_intact(list, b, payloads, 3);
    for (uint32_t i = 0; i < 2; i++)
        check_row((col_rel_t *)payloads[0], i, v0[i], s0[i]);

    /* 3. Grow the headroom 8 bytes at a time.  Each governor refusal on the
     * way must fail cleanly, and the first sufficient headroom succeeds. */
    unsigned refusals = 0;
    int rc = ENOSPC;
    for (uint64_t room = 0; rc != 0; room += 8) {
        CHECK(room < (UINT64_C(1) << 20));
        wl_columnar_memory_reservation_init(&hold);
        CHECK(wl_columnar_memory_reserve(raw, budget - baseline - room,
            &hold));
        rc = wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 2, NULL,
                &sink, &slot);
        CHECK(wl_columnar_memory_rollback(&hold));
        if (rc != 0) {
            CHECK(rc == ENOSPC);
            CHECK(!slot && reserved(g) == baseline);
            check_list_intact(list, b, payloads, 3);
            refusals++;
        }
    }
    CHECK(refusals > 1);
    CHECK(!list && slot && slot->nrows == 6 && log.count == 0);
    for (uint32_t i = 0; i < 6; i++)
        check_row(slot, i, (int64_t)i + 1, s0[0]);
    CHECK(col_rel_destroy_checked(slot) == 0);

    /* ... and a failed list can be discarded instead. */
    CHECK(wl_tdd_channel_begin_round(ch, 4, 2) == 0);
    publish(ch, 1, 0, make_rel(cg, "f", v0, s0, 2));
    publish(ch, 1, 0, make_rel(cg, "f", v1, s0, 2));
    take_all(ch, 2, b, &n, 2);
    CHECK(wl_columnar_eval_tdd_batch_spill(b[1], cg, coord->spill_store,
        NULL) == 0);
    list = link_list(b, 2);
    slot = NULL;
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == EPROTO);
    wl_columnar_eval_tdd_discard_list(ch, &list, &sink);
    CHECK(!list && log.count == 0);
    CHECK(wl_columnar_spill_store_bytes(coord->spill_store) == 0);

    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

static void
test_refusal_sink(void)
{
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_C(1) << 28);
    wl_session_t *session = make_session(g);
    wl_col_session_t *coord = COL_SESSION(session);
    wl_columnar_memory_governor_ref_t *cg = coord->memory_governor;
    wl_tdd_channel_t *ch = open_channel(cg, 1);
    const int64_t v0[1] = { 1 }, v1[1] = { 2 };
    const col_delta_timestamp_t s0[1] = { ts(0, 4, 0, 1) };
    const col_delta_timestamp_t s1[1] = { ts(1, 4, 0, 1) };
    col_rel_t *r0 = make_rel(cg, "rf", v0, s0, 1);
    col_rel_t *r1 = make_rel(cg, "rf", v1, s1, 1);
    publish(ch, 0, 0, r0);
    publish(ch, 0, 0, r1);
    wl_tdd_batch_t *b[2];
    uint32_t n = 0;
    take_all(ch, 1, b, &n, 2);
    /* A reader lease on r1 blocks its retirement after the copy. */
    wl_columnar_source_access_reader_t lease;
    memset(&lease, 0, sizeof(lease));
    CHECK(col_rel_source_reader_acquire(r1, &lease) == 0);
    wl_tdd_batch_t *list = link_list(b, 2);
    col_rel_t *slot = NULL;
    sink_log_t log;
    memset(&log, 0, sizeof(log));
    wl_tdd_refusal_sink_t sink = { record_refusal, &log };
    CHECK(wl_columnar_eval_tdd_coalesce_slot(coord, ch, &list, 1, NULL, &sink,
        &slot) == 0);
    CHECK(!list && slot && slot->nrows == 2);
    CHECK(log.count == 1 && log.batches[0] == b[1] && log.rcs[0] == EBUSY);
    CHECK(b[1]->state == WL_TDD_BATCH_CONSUMER && b[1]->payload == r1);
    CHECK(retired_count(ch) == 1);

    /* The discard helper routes the same refusal, then retires on retry. */
    log.count = 0;
    wl_columnar_eval_tdd_batch_discard(ch, b[1], &sink);
    CHECK(log.count == 1 && log.batches[0] == b[1] && log.rcs[0] == EBUSY);
    CHECK(retired_count(ch) == 1);
    CHECK(col_rel_source_reader_release(&lease) == 0);
    wl_columnar_eval_tdd_batch_discard(ch, b[1], &sink);
    CHECK(log.count == 1 && retired_count(ch) == 2);
    CHECK(col_rel_destroy_checked(slot) == 0);

    /* discard_all disposes queued batches through the discard sink. */
    CHECK(wl_tdd_channel_begin_round(ch, 4, 2) == 0);
    publish(ch, 0, 0, make_rel(cg, "rf", v0, s0, 1));
    publish(ch, 0, 0, make_rel(cg, "rf", v1, s1, 1));
    wl_tdd_channel_close_lane(ch, 0, 0);
    wl_tdd_discard_ctx_t dctx = { ch, { record_refusal, &log } };
    wl_tdd_channel_discard_all(ch, wl_columnar_eval_tdd_discard_sink, &dctx);
    CHECK(log.count == 1 && retired_count(ch) == 4);

    CHECK(wl_tdd_channel_destroy(ch) == 0);
    wl_session_destroy(session);
    CHECK(reserved(g) == 0);
    wl_columnar_memory_governor_ref_release(g);
}

/* ------------------------------------------------------------------ */

typedef struct {
    const char *name;
    void (*fn)(void);
} test_case_t;

static const test_case_t tests[] = {
    { "round_trip", test_round_trip },
    { "string_refused", test_string_refused },
    { "quota_exhausted", test_quota_exhausted },
    { "reader_floor", test_reader_floor },
    { "worker_delta_spill", test_worker_delta_spill },
    { "coalesce_seq_order", test_coalesce_seq_order },
    { "coalesce_overlap_dedup", test_coalesce_overlap_dedup },
    { "coalesce_signed_kept", test_coalesce_signed_kept },
    { "coalesce_uniform_negative_kept", test_coalesce_uniform_negative_kept },
    { "coalesce_invalid_list", test_coalesce_invalid_list },
    { "spill_untimestamped_refused", test_spill_untimestamped_refused },
    { "coalesce_failure_owned", test_coalesce_failure_owned },
    { "refusal_sink", test_refusal_sink },
};

static void
make_spill_dir(void)
{
    const char *base = getenv("WL_TDD_CHANNEL_SPILL_DIR");
    if (!base || !base[0])
        base = ".";
#ifdef _WIN32
    snprintf(spill_dir, sizeof(spill_dir), "%s/tdd-channel-spill-%lu", base,
        (unsigned long)GetCurrentProcessId());
    CHECK(_mkdir(spill_dir) == 0);
#else
    snprintf(spill_dir, sizeof(spill_dir), "%s/tdd-channel-spill-XXXXXX",
        base);
    CHECK(mkdtemp(spill_dir) != NULL);
#endif
}

static void
remove_spill_dir(void)
{
    /* Fails if any spill segment was left behind. */
#ifdef _WIN32
    CHECK(_rmdir(spill_dir) == 0);
#else
    CHECK(rmdir(spill_dir) == 0);
#endif
}

int
main(int argc, char **argv)
{
    const char *only = argc > 1 ? argv[1] : NULL;
    unsigned ran = 0;
    make_spill_dir();
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        if (only && strcmp(only, tests[i].name) != 0)
            continue;
        printf("  %s ... ", tests[i].name);
        fflush(stdout);
        tests[i].fn();
        printf("ok\n");
        ran++;
    }
    remove_spill_dir();
    if (ran == 0) {
        fprintf(stderr, "no test named %s\n", only ? only : "(none)");
        return 1;
    }
    printf("tdd channel spill: %u passed\n", ran);
    return 0;
}
