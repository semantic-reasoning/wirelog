/*
 * test_tdd_multiworker.c - TDD multi-worker tuple count regression tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Regression tests for Issue #404: multi-worker execution (W=4) must produce
 * the same tuple count as single-worker (W=1) for recursive strata.
 *
 * Root cause was a combination of two budget regressions in commit 6929689:
 *   1. join_output_limit divided by num_workers (per-worker divisor)
 *   2. pre-join backpressure firing too early on worker sessions
 *
 * These tests guard against silent data loss from budget/limit regressions.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "columnar/eval_tdd_channel.h"
#include "columnar/internal.h"
#include "plan_fixture.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;
static uint32_t discard_destroy_count = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)
#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)
#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

static int
test_checked_size_mul_boundaries(void)
{
    TEST("checked TDD nrels byte-size multiplication boundaries");

    size_t bytes = SIZE_MAX;
    size_t pointer_limit = SIZE_MAX / sizeof(col_rel_t *);
    bool ok = wl_columnar_eval_checked_size_mul(0, sizeof(col_rel_t *),
            &bytes) == 0
        && bytes == 0;
    ok = ok && wl_columnar_eval_checked_size_mul(3, sizeof(col_rel_t *),
            &bytes) == 0
        && bytes == 3 * sizeof(col_rel_t *);
    ok = ok && wl_columnar_eval_checked_size_mul(pointer_limit,
            sizeof(col_rel_t *), &bytes) == 0
        && bytes == pointer_limit * sizeof(col_rel_t *);
    if (pointer_limit < SIZE_MAX)
        ok = ok && wl_columnar_eval_checked_size_mul(pointer_limit + 1,
                sizeof(col_rel_t *), &bytes) == EOVERFLOW
            && bytes == pointer_limit * sizeof(col_rel_t *);
    ok = ok && wl_columnar_eval_checked_size_mul(2, SIZE_MAX, &bytes)
        == EOVERFLOW;
    size_t slots = 0;
    ok = ok && wl_columnar_eval_checked_count_inc(0, &slots) == 0
        && slots == 1;
    if ((size_t)UINT32_MAX < SIZE_MAX)
        ok = ok && wl_columnar_eval_checked_count_inc(UINT32_MAX, &slots) == 0
            && slots == (size_t)UINT32_MAX + 1;
    else
        ok = ok && wl_columnar_eval_checked_count_inc(UINT32_MAX, &slots)
            == EOVERFLOW;
    if (!ok) {
        FAIL("size multiplication boundary mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_checked_row_add_boundaries(void)
{
    TEST("checked TDD row-count addition boundaries");

    uint32_t out = 123;
    bool ok = wl_columnar_eval_checked_row_add(0, 0, &out) == 0 && out == 0;
    ok = ok && wl_columnar_eval_checked_row_add(7, 5, &out) == 0 && out == 12;
    ok = ok && wl_columnar_eval_checked_row_add(UINT32_MAX - 1u, 1u,
            &out) == 0 && out == UINT32_MAX;
    ok = ok && wl_columnar_eval_checked_row_add(UINT32_MAX, 1u,
            &out) == EOVERFLOW && out == UINT32_MAX;
    ok = ok && wl_columnar_eval_checked_row_add(UINT32_MAX - 1u, 2u,
            &out) == EOVERFLOW && out == UINT32_MAX;

    if (!ok) {
        FAIL("row-count addition boundary mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_checked_hash_capacity_boundaries(void)
{
    TEST("checked TDD dedup hash capacity boundaries");

    uint32_t capacity = UINT32_C(0xA5A5A5A5);
    bool ok = wl_columnar_eval_checked_hash_capacity(0, &capacity) == 0
        && capacity == 4u;
    ok = ok && wl_columnar_eval_checked_hash_capacity(1, &capacity) == 0
        && capacity == 4u;
    ok = ok && wl_columnar_eval_checked_hash_capacity(3, &capacity) == 0
        && capacity == 8u;
    if (SIZE_MAX >= UINT64_C(1) << 34) {
        ok = ok
            && wl_columnar_eval_checked_hash_capacity(UINT32_C(1) << 30,
                &capacity) == 0
            && capacity == (UINT32_C(1) << 31);
    }
    ok = ok
        && wl_columnar_eval_checked_hash_capacity((UINT32_C(1) << 30) + 1u,
            &capacity) == EOVERFLOW
        && capacity == (SIZE_MAX >= UINT64_C(1) << 34
            ? (UINT32_C(1) << 31) : UINT32_C(0xA5A5A5A5));
    ok = ok
        && wl_columnar_eval_checked_hash_capacity(UINT32_MAX / 2u,
            &capacity) == EOVERFLOW
        && capacity == (SIZE_MAX >= UINT64_C(1) << 34
            ? (UINT32_C(1) << 31) : UINT32_C(0xA5A5A5A5));
    ok = ok
        && wl_columnar_eval_checked_hash_capacity(UINT32_MAX, &capacity)
        == EOVERFLOW
        && capacity == (SIZE_MAX >= UINT64_C(1) << 34
            ? (UINT32_C(1) << 31) : UINT32_C(0xA5A5A5A5));
    if (!ok) {
        FAIL("hash capacity boundary mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_sorted_merge_row_overflow(void)
{
    TEST("TDD sorted merge rejects row-count overflow");

    col_rel_t *dst = col_rel_new_auto("merge-overflow-dst", 1);
    col_rel_t *src = col_rel_new_auto("merge-overflow-src", 1);
    bool ok = dst != NULL && src != NULL;
    if (ok) {
        dst->nrows = UINT32_MAX;
        dst->capacity = UINT32_MAX;
        src->nrows = 1;
        uint32_t old_rows = dst->nrows;
        uint32_t old_capacity = dst->capacity;
        ok = tdd_sorted_merge_append(dst, src) == EOVERFLOW
            && dst->nrows == old_rows
            && dst->capacity == old_capacity
            && dst->merge_columns == NULL;
    }
    col_rel_destroy(dst);
    col_rel_destroy(src);
    if (!ok) {
        FAIL("sorted merge overflow mutated destination");
        return 1;
    }
    PASS();
    return 0;
}

struct count_ctx {
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
    (void)relation;
    (void)row;
    (void)ncols;
}

/*
 * Run transitive closure on a 20-node chain graph with the given number of
 * workers.  Returns the tuple count for the tc relation, or -1 on error.
 *
 * Graph: 1->2, 2->3, ..., 19->20  (19 edges)
 * TC should produce n*(n-1)/2 = 190 tuples for a 20-node chain.
 */
static int64_t
run_tc_chain20(uint32_t num_workers)
{
    /* Build the Datalog program */
    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return -1;
    }
    plan_fixture_hold(prog);

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        return -1;
    }

    /* Insert 19 edges forming a 20-node chain */
    for (int i = 1; i <= 19; i++) {
        int64_t row[2] = { i, i + 1 };
        rc = wl_session_insert(session, "edge", row, 1, 2);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            return -1;
        }
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        return -1;
    }

    int64_t count = ctx.count;

    wl_session_destroy(session);
    wl_plan_free(plan);
    return count;
}

/* ======================================================================== */
/* Test 1: W=1 vs W=4 produce same tuple count (Issue #404 regression)     */
/* ======================================================================== */

static int
test_w1_vs_w4_tuple_count(void)
{
    TEST("TC 20-node chain: W=1 and W=4 produce same tuple count");

    int64_t count1 = run_tc_chain20(1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count4 = run_tc_chain20(4);
    if (count4 < 0) {
        FAIL("session with W=4 failed");
        return 1;
    }

    if (count1 != count4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            count1, count4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: W=1 produces the expected 190 tuples for a 20-node chain        */
/* ======================================================================== */

static int
test_w1_expected_count(void)
{
    TEST("TC 20-node chain: W=1 produces 190 tuples");

    int64_t count = run_tc_chain20(1);
    if (count < 0) {
        FAIL("session failed");
        return 1;
    }

    /* 20-node chain: n*(n-1)/2 = 20*19/2 = 190 */
    if (count != 190) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 190, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: W=8 also matches W=1                                            */
/* ======================================================================== */

static int
test_w1_vs_w8_tuple_count(void)
{
    TEST("TC 20-node chain: W=1 and W=8 produce same tuple count");

    int64_t count1 = run_tc_chain20(1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count8 = run_tc_chain20(8);
    if (count8 < 0) {
        FAIL("session with W=8 failed");
        return 1;
    }

    if (count1 != count8) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=8 produced %" PRId64,
            count1, count8);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Dual-write queue transport tests (Issue #410, Commit 3)                 */
/* ======================================================================== */

static int
test_queue_only_w1(void)
{
    TEST("queue-only transport: TC chain-20 W=1 produces 190 tuples");

    int64_t count = run_tc_chain20(1);
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_queue_only_w4(void)
{
    TEST("queue-only transport: TC chain-20 W=4 produces 190 tuples");

    int64_t count = run_tc_chain20(4);
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_coordinator_drain_w2(void)
{
    TEST("coordinator drain+adapter: TC chain-20 W=2 produces 190 tuples");

    int64_t count = run_tc_chain20(2);
    if (count < 0) {
        FAIL("session with W=2 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_coordinator_drain_w4(void)
{
    TEST("coordinator drain+adapter: TC chain-20 W=4 produces 190 tuples");

    int64_t count = run_tc_chain20(4);
    if (count < 0) {
        FAIL("session with W=4 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_dual_write_w2(void)
{
    TEST("dual-write transport: TC chain-20 W=2 produces 190 tuples");

    int64_t count = run_tc_chain20(2);
    if (count < 0) {
        FAIL("session with W=2 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

static int
test_dual_write_w3(void)
{
    TEST("dual-write transport: TC chain-20 W=3 produces 190 tuples");

    int64_t count = run_tc_chain20(3);
    if (count < 0) {
        FAIL("session with W=3 failed");
        return 1;
    }
    if (count != 190) {
        char msg[80];
        snprintf(msg, sizeof(msg),
            "expected 190 tuples, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Channel collect + coalesce into the delta matrix (Issue #410, #1373)    */
/* ======================================================================== */

/*
 * The coordinator pumps the channel after the join and coalesces every
 * (worker, relation) list of batches into ctxs[w].delta_rels[ri].  These
 * fixtures publish real relations through the producer API and run the
 * production consumer helpers against a real coordinator session.
 */
typedef struct {
    wl_session_t *session;
    wl_col_session_t *coord;
    wl_tdd_channel_t *ch;
    wl_tdd_batch_t **lists;
    uint32_t workers;
    uint32_t nrels;
    unsigned refusals;
    uint64_t governor_baseline;
} channel_fixture_t;

static void
fixture_refusal(void *ctx, wl_tdd_batch_t *batch, int rc)
{
    channel_fixture_t *f = ctx;
    (void)rc;
    f->refusals++;
    batch->payload = NULL;
    wl_tdd_channel_retire(f->ch, batch);
}

static uint64_t
fixture_governor_reserved(const channel_fixture_t *f)
{
    return f->coord->memory_governor
           ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f->coord->memory_governor))
           : 0;
}

/* Sessions borrow their plan for their whole lifetime. */
static wl_plan_t fixture_plan;

static bool
fixture_open(channel_fixture_t *f, uint32_t workers, uint32_t nrels)
{
    memset(f, 0, sizeof(*f));
    if (wl_session_create(wl_backend_columnar(), &fixture_plan, 1,
        &f->session) != 0)
        return false;
    f->coord = COL_SESSION(f->session);
    f->workers = workers;
    f->nrels = nrels;
    f->governor_baseline = fixture_governor_reserved(f);
    f->ch = wl_tdd_channel_create(workers, UINT64_MAX, NULL, f->coord);
    f->lists = calloc((size_t)workers * nrels, sizeof(*f->lists));
    return f->ch && f->lists && wl_tdd_channel_begin_round(f->ch, 0, 1) == 0;
}

/* Close every lane, collect and coalesce into @ctxs. */
static int
fixture_deliver(channel_fixture_t *f, col_eval_tdd_worker_ctx_t *ctxs,
    uint64_t *rows)
{
    for (uint32_t w = 0; w < f->workers; w++)
        wl_tdd_channel_close_lane(f->ch, w, 0);
    wl_tdd_refusal_sink_t sink = { fixture_refusal, f };
    int rc = wl_columnar_eval_tdd_channel_collect(f->ch, &f->coord->mem_ledger,
            f->lists, f->workers, f->nrels, &sink, rows);
    if (rc == 0)
        rc = wl_columnar_eval_tdd_channel_coalesce_all(f->coord, f->ch,
                f->lists, ctxs, f->workers, f->nrels, &sink);
    return rc;
}

/* Destroy the matrix and the channel; true when nothing leaked. */
static bool
fixture_close(channel_fixture_t *f, col_eval_tdd_worker_ctx_t *ctxs)
{
    bool ok = true;
    for (uint32_t w = 0; ctxs && w < f->workers; w++)
        for (uint32_t ri = 0; ri < f->nrels; ri++) {
            col_rel_destroy(ctxs[w].delta_rels[ri]);
            ctxs[w].delta_rels[ri] = NULL;
        }
    for (size_t i = 0; f->lists && i < (size_t)f->workers * f->nrels; i++)
        ok &= f->lists[i] == NULL;
    if (f->ch) {
        /* A test that failed early may leave lanes open or batches queued. */
        wl_tdd_channel_close_unstarted(f->ch, 0, f->workers);
        wl_tdd_discard_ctx_t discard = { f->ch, { fixture_refusal, f },
                                         &f->coord->mem_ledger };
        wl_tdd_channel_discard_all(f->ch, wl_columnar_eval_tdd_discard_sink,
            &discard);
        wl_columnar_eval_tdd_channel_discard_lists(f->ch, f->lists,
            (size_t)f->workers * f->nrels, &discard.refusal);
        wl_tdd_channel_stats_t stats;
        wl_tdd_channel_stats(f->ch, &stats);
        ok &= stats.published == stats.retired && stats.queued_batches == 0;
        ok &= wl_tdd_channel_destroy(f->ch) == 0;
    }
    if (f->coord) {
        wl_mem_ledger_snapshot_t ledger;
        wl_mem_ledger_snapshot(&f->coord->mem_ledger, &ledger);
        ok &= ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL] == 0;
        ok &= fixture_governor_reserved(f) == f->governor_baseline;
    }
    free((void *)f->lists);
    wl_session_destroy(f->session);
    return ok;
}

/* A governed one-column delta holding @base..@base+@rows-1, stamped as
 * worker @worker's delta of round 1. */
static col_rel_t *
make_stamped_delta(channel_fixture_t *f, int64_t base, uint32_t rows,
    uint32_t worker)
{
    col_rel_t *rel = col_rel_new_auto("$d$tc", 1);
    if (!rel || col_rel_attach_memory_governor(rel,
        f->coord->memory_governor) != 0) {
        col_rel_destroy(rel);
        return NULL;
    }
    for (uint32_t i = 0; i < rows; i++) {
        int64_t value = base + (int64_t)i;
        if (col_rel_append_row(rel, &value) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    if (col_rel_enable_timestamps(rel) != 0) {
        col_rel_destroy(rel);
        return NULL;
    }
    for (uint32_t i = 0; i < rows; i++) {
        rel->timestamps[i].iteration = 1;
        rel->timestamps[i].stratum = 0;
        rel->timestamps[i].worker = worker;
        rel->timestamps[i].multiplicity = 1;
    }
    return rel;
}

/* Publish @rel on @lane for @rel_idx as a worker producer does, CHANNEL
 * charge included.  On failure @rel is still the caller's. */
static int
fixture_publish(channel_fixture_t *f, uint32_t lane, uint32_t rel_idx,
    col_rel_t *rel)
{
    wl_tdd_batch_t *batch = NULL;
    int rc = wl_tdd_channel_batch_new(f->ch, lane, rel_idx, NULL, &batch);
    if (rc == 0) {
        rc = wl_tdd_channel_reserve(f->ch, batch,
                col_rel_transport_bytes(rel));
        if (rc != 0)
            wl_tdd_channel_batch_abandon(f->ch, batch);
    }
    if (rc != 0)
        return rc;
    wl_mem_ledger_alloc(&f->coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
        batch->charge);
    wl_tdd_channel_publish(f->ch, batch, rel, rel->nrows);
    return 0;
}

/* Publish a fresh delta of @rows rows from @base; NULL on failure. */
static col_rel_t *
fixture_publish_new(channel_fixture_t *f, uint32_t lane, uint32_t rel_idx,
    int64_t base, uint32_t rows)
{
    col_rel_t *rel = make_stamped_delta(f, base, rows, lane);
    if (rel && fixture_publish(f, lane, rel_idx, rel) != 0) {
        col_rel_destroy(rel);
        return NULL;
    }
    return rel;
}

static int
test_reconstruct_empty(void)
{
    TEST("channel delivery: an empty round leaves the matrix unchanged");

    channel_fixture_t f;
    col_eval_tdd_worker_ctx_t ctxs[2];
    col_rel_t *dr0[3], *dr1[3];
    memset(dr0, 0, sizeof(dr0));
    memset(dr1, 0, sizeof(dr1));
    memset(ctxs, 0, sizeof(ctxs));
    ctxs[0].delta_rels = dr0;
    ctxs[1].delta_rels = dr1;
    uint64_t rows = 0;
    bool ok = fixture_open(&f, 2, 3) && fixture_deliver(&f, ctxs, &rows) == 0
        && rows == 0;
    for (uint32_t w = 0; ok && w < 2; w++)
        for (uint32_t ri = 0; ri < 3; ri++)
            ok &= ctxs[w].delta_rels[ri] == NULL;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("non-NULL entry or leak after an empty round");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_reconstruct_single(void)
{
    TEST("channel delivery: one batch is adopted into one slot");

    channel_fixture_t f;
    col_eval_tdd_worker_ctx_t ctxs[4];
    col_rel_t *dr[4][5];
    memset(dr, 0, sizeof(dr));
    memset(ctxs, 0, sizeof(ctxs));
    for (uint32_t w = 0; w < 4; w++)
        ctxs[w].delta_rels = dr[w];
    uint64_t rows = 0;
    bool ok = fixture_open(&f, 4, 5);
    col_rel_t *published = ok ? fixture_publish_new(&f, 2, 3, 7, 2) : NULL;
    ok = ok && published && fixture_deliver(&f, ctxs, &rows) == 0
        && rows == 2;
    /* The single batch's relation is the slot: adopted, not copied. */
    ok = ok && ctxs[2].delta_rels[3] == published;
    for (uint32_t w = 0; ok && w < 4; w++)
        for (uint32_t ri = 0; ri < 5; ri++)
            if (!(w == 2 && ri == 3))
                ok &= ctxs[w].delta_rels[ri] == NULL;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("slot [2][3] not adopted or an adjacent slot was set");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_reconstruct_full_matrix(void)
{
    TEST("channel delivery: full W*nrels matrix filled correctly");

    channel_fixture_t f;
    col_eval_tdd_worker_ctx_t ctxs[4];
    col_rel_t *dr[4][3];
    col_rel_t *published[4][3];
    memset(dr, 0, sizeof(dr));
    memset(published, 0, sizeof(published));
    memset(ctxs, 0, sizeof(ctxs));
    for (uint32_t w = 0; w < 4; w++)
        ctxs[w].delta_rels = dr[w];
    bool ok = fixture_open(&f, 4, 3);
    for (uint32_t w = 0; ok && w < 4; w++)
        for (uint32_t ri = 0; ok && ri < 3; ri++) {
            published[w][ri] = fixture_publish_new(&f, w, ri,
                    (int64_t)(w * 100 + ri), 1);
            ok = published[w][ri] != NULL;
        }
    uint64_t rows = 0;
    ok = ok && fixture_deliver(&f, ctxs, &rows) == 0 && rows == 12;
    for (uint32_t w = 0; ok && w < 4; w++)
        for (uint32_t ri = 0; ri < 3; ri++)
            ok &= ctxs[w].delta_rels[ri] == published[w][ri];
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("matrix entry mismatch");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_reconstruct_sparse(void)
{
    TEST("channel delivery: sparse batches, rest remain NULL");

    channel_fixture_t f;
    col_eval_tdd_worker_ctx_t ctxs[4];
    col_rel_t *dr[4][3];
    memset(dr, 0, sizeof(dr));
    memset(ctxs, 0, sizeof(ctxs));
    for (uint32_t w = 0; w < 4; w++)
        ctxs[w].delta_rels = dr[w];
    /* 5 of 12 slots: {0,0}, {1,2}, {2,1}, {3,0}, {3,2} */
    const uint32_t slot[5][2] = { { 0, 0 }, { 1, 2 }, { 2, 1 }, { 3, 0 },
                                  { 3, 2 } };
    col_rel_t *published[5] = { NULL };
    bool ok = fixture_open(&f, 4, 3);
    for (uint32_t i = 0; ok && i < 5; i++) {
        published[i] = fixture_publish_new(&f, slot[i][0], slot[i][1],
                (int64_t)i, 1);
        ok = published[i] != NULL;
    }
    uint64_t rows = 0;
    ok = ok && fixture_deliver(&f, ctxs, &rows) == 0 && rows == 5;
    unsigned filled = 0;
    for (uint32_t i = 0; ok && i < 5; i++)
        ok &= ctxs[slot[i][0]].delta_rels[slot[i][1]] == published[i];
    for (uint32_t w = 0; ok && w < 4; w++)
        for (uint32_t ri = 0; ri < 3; ri++)
            filled += ctxs[w].delta_rels[ri] != NULL;
    ok = ok && filled == 5;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("sparse slot mismatch");
        return 1;
    }
    PASS();
    return 0;
}

/* Rows of a one-column relation as a bitmask over values 0..63. */
static uint64_t
row_mask(const col_rel_t *rel)
{
    uint64_t mask = 0;
    for (uint32_t i = 0; rel && i < rel->nrows; i++) {
        int64_t v = col_rel_get(rel, i, 0);
        if (v >= 0 && v < 64)
            mask |= UINT64_C(1) << v;
    }
    return mask;
}

static int
test_reconstruct_duplicate(void)
{
    TEST("channel delivery: several batches per slot are owned exactly once");

    /* Two overlapping batches of one slot coalesce into a fresh slot with
     * their union; both sources are retired, and the governor returning to
     * its baseline at close shows each was destroyed (ASan catches a
     * second destroy). */
    channel_fixture_t f;
    col_eval_tdd_worker_ctx_t ctxs[1];
    col_rel_t *dr[4];
    memset(dr, 0, sizeof(dr));
    memset(ctxs, 0, sizeof(ctxs));
    ctxs[0].delta_rels = dr;
    bool ok = fixture_open(&f, 1, 4);
    col_rel_t *first = ok ? fixture_publish_new(&f, 0, 1, 1, 2) : NULL;
    col_rel_t *second = first ? fixture_publish_new(&f, 0, 1, 2, 2) : NULL;
    uint64_t rows = 0;
    ok = ok && first && second && fixture_deliver(&f, ctxs, &rows) == 0
        && rows == 4;
    wl_tdd_channel_stats_t stats;
    wl_tdd_channel_stats(f.ch, &stats);
    ok = ok && ctxs[0].delta_rels[1] != NULL
        && ctxs[0].delta_rels[1] != first && ctxs[0].delta_rels[1] != second
        && ctxs[0].delta_rels[1]->nrows == 3
        && row_mask(ctxs[0].delta_rels[1]) == 0xEu
        && stats.published == 2 && stats.retired == 2 && f.refusals == 0;
    for (uint32_t ri = 0; ok && ri < 4; ri++)
        if (ri != 1)
            ok &= ctxs[0].delta_rels[ri] == NULL;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("duplicate slot batches were not coalesced exactly once");
        return 1;
    }

    /* A batch naming a relation outside the stratum is disposed once and
     * fails the round; the valid slot published beside it still arrives. */
    memset(dr, 0, sizeof(dr));
    ok = fixture_open(&f, 1, 4);
    col_rel_t *valid = ok ? fixture_publish_new(&f, 0, 3, 5, 1) : NULL;
    col_rel_t *invalid = valid ? fixture_publish_new(&f, 0, 4, 9, 1) : NULL;
    rows = 0;
    wl_tdd_refusal_sink_t sink = { fixture_refusal, &f };
    if (ok && valid && invalid) {
        wl_tdd_channel_close_lane(f.ch, 0, 0);
        ok = wl_columnar_eval_tdd_channel_collect(f.ch, &f.coord->mem_ledger,
                f.lists, 1, 4, &sink, &rows) == EPROTO && rows == 1;
        ok = ok && wl_columnar_eval_tdd_channel_coalesce_all(f.coord, f.ch,
                f.lists, ctxs, 1, 4, &sink) == 0;
    } else {
        ok = false;
    }
    wl_tdd_channel_stats(f.ch, &stats);
    ok = ok && ctxs[0].delta_rels[3] == valid && stats.published == 2
        && stats.retired == 2 && f.refusals == 0;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("invalid batch was not disposed exactly once");
        return 1;
    }
    PASS();
    return 0;
}

/* A completed round that reclaimed an orphan reports EPROTO, and a slot
 * that is already occupied fails coalesce with the other lists disposed and
 * the slots already built kept. */
static int
test_channel_orphan_and_occupied_slot(void)
{
    TEST("channel delivery: orphan EPROTO and an occupied slot");

    channel_fixture_t f;
    col_eval_tdd_worker_ctx_t ctxs[1];
    col_rel_t *dr[3];
    memset(dr, 0, sizeof(dr));
    memset(ctxs, 0, sizeof(ctxs));
    ctxs[0].delta_rels = dr;
    uint64_t rows = 0;
    wl_tdd_refusal_sink_t sink = { fixture_refusal, &f };

    bool ok = fixture_open(&f, 1, 3);
    col_rel_t *valid = ok ? fixture_publish_new(&f, 0, 0, 1, 2) : NULL;
    wl_tdd_batch_t *orphan = NULL;
    ok = ok && valid && wl_tdd_channel_batch_new(f.ch, 0, 1, NULL,
            &orphan) == 0;
    int collect_rc = -1;
    if (ok) {
        wl_tdd_channel_close_lane(f.ch, 0, 0);
        collect_rc = wl_columnar_eval_tdd_channel_collect(f.ch,
                &f.coord->mem_ledger, f.lists, 1, 3, &sink, &rows);
        /* The driver disposes the lists after a failed collect. */
        wl_columnar_eval_tdd_channel_discard_lists(f.ch, f.lists, 3, &sink);
    }
    ok = ok && collect_rc == EPROTO && rows == 2;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("an orphan on a clean lane did not fail the collect");
        return 1;
    }

    memset(dr, 0, sizeof(dr));
    ok = fixture_open(&f, 1, 3);
    col_rel_t *first = ok ? fixture_publish_new(&f, 0, 0, 1, 1) : NULL;
    col_rel_t *second = first ? fixture_publish_new(&f, 0, 1, 2, 1) : NULL;
    col_rel_t *third = second ? fixture_publish_new(&f, 0, 2, 3, 1) : NULL;
    col_rel_t *occupant = third ? make_stamped_delta(&f, 9, 1, 0) : NULL;
    rows = 0;
    if (ok && occupant) {
        dr[1] = occupant;
        wl_tdd_channel_close_lane(f.ch, 0, 0);
        ok = wl_columnar_eval_tdd_channel_collect(f.ch, &f.coord->mem_ledger,
                f.lists, 1, 3, &sink, &rows) == 0 && rows == 3;
        ok = ok && wl_columnar_eval_tdd_channel_coalesce_all(f.coord, f.ch,
                f.lists, ctxs, 1, 3, &sink) == EPROTO;
        ok = ok && dr[0] == first && dr[1] == occupant && dr[2] == NULL;
        for (uint32_t i = 0; ok && i < 3; i++)
            ok &= f.lists[i] == NULL;
    } else {
        ok = false;
        col_rel_destroy(occupant);
    }
    wl_tdd_channel_stats_t stats;
    wl_tdd_channel_stats(f.ch, &stats);
    ok = ok && stats.published == 3 && stats.retired == 3 && f.refusals == 0;
    ok = fixture_close(&f, ctxs) && ok;
    if (!ok) {
        FAIL("occupied slot did not fail coalesce with the rest disposed");
        return 1;
    }
    PASS();
    return 0;
}

static void
test_channel_side_sizing_boundaries(void)
{
    TEST("channel side-buffer sizing rejects multiplication overflow");

    uint64_t bytes = 0, one = 0, two = 0;
    bool ok = wl_tdd_channel_footprint(1, &one) == 0
        && wl_tdd_channel_footprint(2, &two) == 0
        && wl_columnar_eval_tdd_channel_side_bytes(1, 0, &bytes) == 0
        && bytes == one
        && wl_columnar_eval_tdd_channel_side_bytes(2, 3, &bytes) == 0
        && bytes == two + 6u * sizeof(wl_tdd_batch_t *)
        && wl_columnar_eval_tdd_channel_side_bytes(0, 3, &bytes) == EINVAL
        && wl_columnar_eval_tdd_channel_side_bytes(2, 3, NULL) == EINVAL
        && wl_columnar_eval_tdd_channel_side_bytes(UINT32_MAX, 1, &bytes)
        == 0
        && wl_columnar_eval_tdd_channel_side_bytes(UINT32_MAX, UINT32_MAX,
            &bytes) == EOVERFLOW;
    if (!ok) {
        FAIL("unexpected channel side-buffer boundary result");
        return;
    }
    PASS();
}

static void
test_channel_side_footprint_boundaries(void)
{
    TEST("channel footprint grows by one lane record per lane");
    uint64_t one = 0, two = 0, three = 0;
    bool ok = wl_tdd_channel_footprint(1, &one) == 0
        && wl_tdd_channel_footprint(2, &two) == 0
        && wl_tdd_channel_footprint(3, &three) == 0
        && two > one && three - two == two - one
        && wl_tdd_channel_footprint(0, &one) == EINVAL;
    if (ok)
        PASS();
    else
        FAIL("incorrect channel footprint or argument check");
}

static void
test_channel_four_workers_no_loss(void)
{
    TEST("four channel lanes deliver every batch exactly once, in order");
    wl_tdd_channel_t *ch = wl_tdd_channel_create(4, UINT64_MAX, NULL, NULL);
    bool seen[20] = { false };
    uint64_t next_seq[4] = { 0, 0, 0, 0 };
    bool ok = ch && wl_tdd_channel_begin_round(ch, 0, 0) == 0;
    for (uint32_t worker = 0; ok && worker < 4; worker++)
        for (uint32_t item = 0; ok && item < 5; item++) {
            uintptr_t id = (uintptr_t)worker * 5u + item + 1u;
            wl_tdd_batch_t *batch = NULL;
            ok = wl_tdd_channel_batch_new(ch, worker, item, NULL, &batch) == 0
                && wl_tdd_channel_reserve(ch, batch, 8) == 0;
            if (ok)
                wl_tdd_channel_publish(ch, batch, (void *)id, 1);
        }
    for (uint32_t worker = 0; ch && worker < 4; worker++)
        wl_tdd_channel_close_lane(ch, worker, 0);
    unsigned delivered = 0;
    for (;;) {
        wl_tdd_batch_t *batch = NULL;
        int err = 0;
        wl_tdd_pump_result_t result = ch
            ? wl_tdd_channel_pump(ch, &batch, &err) : WL_TDD_PUMP_ERROR;
        if (result != WL_TDD_PUMP_DATA) {
            ok = ok && result == WL_TDD_PUMP_COMPLETE;
            break;
        }
        uintptr_t id = (uintptr_t)batch->payload;
        if (!id || id > 20 || seen[id - 1u] || batch->worker >= 4
            || batch->seq != next_seq[batch->worker])
            ok = false;
        else
            seen[id - 1u] = true;
        if (batch->worker < 4)
            next_seq[batch->worker]++;
        batch->payload = NULL;
        wl_tdd_channel_retire(ch, batch);
        delivered++;
    }
    if (delivered != 20)
        ok = false;
    for (unsigned i = 0; i < 20; i++)
        if (!seen[i])
            ok = false;
    if (ch && wl_tdd_channel_destroy(ch) != 0)
        ok = false;
    if (ok)
        PASS();
    else
        FAIL("lost, duplicate or reordered batch");
}

typedef struct {
    wl_tdd_channel_t *ch;
} discard_count_ctx_t;

/* discard_all sink: count, free the fake payload and retire. */
static void
count_discard_sink(void *ctx, wl_tdd_batch_t *batch)
{
    discard_count_ctx_t *count = ctx;
    discard_destroy_count++;
    free(batch->payload);
    batch->payload = NULL;
    wl_tdd_channel_retire(count->ch, batch);
}

static int
test_discard_drains_all_messages(void)
{
    TEST("channel discard drains more than one W*nrels batch");

    wl_tdd_channel_t *ch = wl_tdd_channel_create(2, UINT64_MAX, NULL, NULL);
    if (!ch || wl_tdd_channel_begin_round(ch, 0, 0) != 0) {
        if (ch) {
            wl_tdd_channel_close_unstarted(ch, 0, 2);
            (void)wl_tdd_channel_destroy(ch);
        }
        FAIL("channel creation failed");
        return 1;
    }
    discard_destroy_count = 0;
    discard_count_ctx_t count = { ch };
    bool ok = true;
    for (uint32_t i = 0; ok && i < 12; i++) {
        void *delta = malloc(1);
        wl_tdd_batch_t *batch = NULL;
        ok = delta && wl_tdd_channel_batch_new(ch, i % 2, i, NULL,
                &batch) == 0;
        if (ok && wl_tdd_channel_reserve(ch, batch, 1) != 0) {
            wl_tdd_channel_batch_abandon(ch, batch);
            ok = false;
        }
        if (ok)
            wl_tdd_channel_publish(ch, batch, delta, 1);
        else
            free(delta);
    }
    wl_tdd_channel_close_unstarted(ch, 0, 2);
    wl_tdd_channel_discard_all(ch, count_discard_sink, &count);
    wl_tdd_channel_stats_t stats;
    wl_tdd_channel_stats(ch, &stats);
    ok = ok && stats.queued_batches == 0 && discard_destroy_count == 12
        && stats.retired == 12;
    ok = wl_tdd_channel_destroy(ch) == 0 && ok;
    if (!ok) {
        FAIL("discard left queued deltas");
        return 1;
    }
    PASS();
    return 0;
}

static void
test_tdd_matrix_size_boundaries(void)
{
    TEST("TDD W*nrels matrix sizing rejects wrapped counts");

    size_t count = 0;
    if (wl_columnar_eval_tdd_matrix_size(0, 0, sizeof(uint32_t), &count)
        != 0
        || count != 0
        || wl_columnar_eval_tdd_matrix_size(1, 1, sizeof(uint32_t), &count)
        != 0
        || count != 1
        || wl_columnar_eval_tdd_matrix_size(2, UINT32_MAX / 2u,
        1, &count) != 0
        || count != UINT32_MAX - 1u
        || wl_columnar_eval_tdd_matrix_size(2, UINT32_MAX / 2u + 1u,
        1, &count) != EOVERFLOW
        || wl_columnar_eval_tdd_matrix_size(UINT32_MAX, 2,
        sizeof(uint32_t), &count) != EOVERFLOW) {
        FAIL("unexpected TDD matrix-size boundary result");
        return;
    }
    PASS();
}

static void
test_tdd_queue_discard_large_dimensions(void)
{
    TEST("TDD channel disposal drains queued and listed batches");

    /* The production disposal walks the queue and the consumer lists
     * without sizing anything by W*nrels, and frees every CHANNEL charge. */
    channel_fixture_t f;
    bool ok = fixture_open(&f, 2, 2);
    col_rel_t *first = ok ? fixture_publish_new(&f, 0, 0, 1, 1) : NULL;
    col_rel_t *second = first ? fixture_publish_new(&f, 1, 1, 2, 1) : NULL;
    ok = ok && first && second;
    if (ok) {
        /* Take one batch onto a consumer list; leave the other queued. */
        wl_tdd_batch_t *batch = NULL;
        int err = 0;
        ok = wl_tdd_channel_pump(f.ch, &batch, &err) == WL_TDD_PUMP_DATA;
        if (ok) {
            wl_mem_ledger_free(&f.coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
                batch->charge);
            f.lists[batch->rel_idx * 2u + batch->worker] = batch;
        }
    }
    wl_tdd_channel_close_unstarted(f.ch, 0, 2);
    f.coord->tdd_channel = f.ch;
    f.coord->tdd_channel_lists = f.lists;
    f.coord->tdd_channel_list_count = 4;
    wl_columnar_eval_tdd_dispose_channel(f.coord);
    ok = ok && f.coord->tdd_channel == NULL
        && f.coord->tdd_channel_lists == NULL;
    if (!f.coord->tdd_channel) {
        /* Disposal freed both; the fixture must not free them again. */
        f.ch = NULL;
        f.lists = NULL;
    }
    ok = fixture_close(&f, NULL) && ok;
    if (ok)
        PASS();
    else
        FAIL("disposal left queued or listed payloads");
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_tdd_multiworker (Issue #404) ===\n");

    test_checked_size_mul_boundaries();
    test_checked_row_add_boundaries();
    test_checked_hash_capacity_boundaries();
    test_sorted_merge_row_overflow();
    test_w1_expected_count();
    test_w1_vs_w4_tuple_count();
    test_w1_vs_w8_tuple_count();

    printf(
        "\n=== dual-write queue transport tests (Issue #410, Commit 3) ===\n");
    test_dual_write_w2();
    test_dual_write_w3();

    printf(
        "\n=== coordinator drain + adapter tests (Issue #410, Commit 4) ===\n");
    test_coordinator_drain_w2();
    test_coordinator_drain_w4();

    printf("\n=== channel transport tests (Issue #410, #1373) ===\n");
    test_queue_only_w1();
    test_queue_only_w4();
    test_reconstruct_empty();
    test_reconstruct_single();
    test_reconstruct_full_matrix();
    test_reconstruct_sparse();
    test_reconstruct_duplicate();
    test_channel_orphan_and_occupied_slot();
    test_channel_side_sizing_boundaries();
    test_channel_side_footprint_boundaries();
    test_channel_four_workers_no_loss();
    test_tdd_matrix_size_boundaries();
    test_tdd_queue_discard_large_dimensions();
    test_discard_drains_all_messages();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
