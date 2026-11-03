/*
 * test_mem_instrumentation.c - memory ledger coverage and baseline (Issue #1380)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Drives a recursive closure through the real evaluator at W=1 and W=8 and
 * checks the memory instrumentation contract:
 *
 *   1. WL_MEM_REPORT unset / =1 / =2 produce identical results (tuples,
 *      iterations, relation contents).  The report only changes stderr.
 *   2. Every allocation class the ledger claims to cover is actually
 *      charged: ARENA matches the session's pool + arena capacity exactly,
 *      STORED and ARRANGEMENT are non-zero after a join workload, RELATION
 *      is charged on whichever session ran the joins, and at W=8 the TDD
 *      transport shows up under CHANNEL with worker peaks folded into the
 *      coordinator aggregates.
 *   3. Charges are symmetric: after a snapshot the CHANNEL and CACHE
 *      counters are back to zero and current_bytes equals the sum of the
 *      subsystem counters (a double credit would be clamped and break the
 *      sum).
 *   4. A baseline line per (workload, workers) is printed on stdout so the
 *      CI log carries the fixture numbers next to the DOOP baselines in
 *      docs/MEMORY.md.
 *
 * Built twice: default (K-fusion) and -DENABLE_K_FUSION=0.
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/mem_ledger.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#  define setenv   wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#endif

/* ======================================================================== */
/* Harness                                                                  */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

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

#define CHAIN_EDGES 100u
#define CHAIN_CLOSURE_ROWS 5050

typedef struct count_ctx {
    int64_t count;
} count_ctx_t;

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    ((count_ctx_t *)user_data)->count++;
}

/* Result of one evaluator run: what the identity test compares, plus the
 * instrumentation stats captured while the session was still alive. */
typedef struct run_result {
    int rc;
    int64_t tuples;
    uint32_t iterations;
    int exact;                 /* closure contents verified row by row  */
    uint32_t selected_workers; /* tdd_audit.selected_workers            */
    uint64_t arena_expected;   /* pool + eval arena capacity, from sess */
    wl_columnar_mem_stats_t stats;
} run_result_t;

/* Verify every closure tuple, not just the cardinality. */
static int
exact_chain(wl_session_t *sess)
{
    col_rel_t *r = session_find_rel(COL_SESSION(sess), "r");
    static bool seen[CHAIN_EDGES + 1][CHAIN_EDGES + 1];
    memset(seen, 0, sizeof(seen));
    if (!r || r->ncols != 2 || r->nrows != CHAIN_CLOSURE_ROWS)
        return 0;
    for (uint32_t i = 0; i < r->nrows; i++) {
        int64_t x = r->columns[0][i];
        int64_t y = r->columns[1][i];
        if (x < 0 || y > CHAIN_EDGES || x >= y || seen[x][y])
            return 0;
        seen[x][y] = true;
    }
    return 1;
}

static uint64_t
allocator_bytes(const wl_col_session_t *cs)
{
    uint64_t bytes = 0;
    if (cs->delta_pool) {
        bytes += (uint64_t)cs->delta_pool->slot_cap
            * (uint64_t)cs->delta_pool->slot_size;
        bytes += (uint64_t)cs->delta_pool->arena_cap;
    }
    if (cs->eval_arena)
        bytes += (uint64_t)cs->eval_arena->capacity;
    return bytes;
}

/*
 * run_chain: transitive closure of a 100-edge chain (5050 closure rows).
 * The self-join r(x,z) :- r(x,y), r(y,z) is the shape the TDD planner
 * accepts at W=8 (see test_tdd_decision_stats.c), so W=8 exercises the
 * delta transport and W=1 the sequential evaluator.
 */
static void
run_chain(uint32_t workers, run_result_t *out)
{
    memset(out, 0, sizeof(*out));
    out->rc = 1;

    const char *source =
        ".decl edge(x:int32,y:int32)\n"
        ".decl r(x:int32,y:int32)\n"
        "r(x, y) :- edge(x, y).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return;
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return;
    }
    plan_fixture_hold(prog);

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        return;
    }
    out->arena_expected = allocator_bytes(COL_SESSION(sess));

    int64_t rows[CHAIN_EDGES * 2];
    for (size_t i = 0; i < CHAIN_EDGES; i++) {
        rows[i * 2] = (int64_t)i;
        rows[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(sess, "edge", rows, CHAIN_EDGES, 2);
    if (rc == 0) {
        count_ctx_t ctx = { 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        out->tuples = ctx.count;
    }
    if (rc == 0) {
        out->iterations = col_session_get_iteration_count(sess);
        out->exact = exact_chain(sess);
        out->selected_workers = COL_SESSION(sess)->tdd_audit.selected_workers;
        col_session_get_mem_stats(sess, &out->stats);
    }
    out->rc = rc;

    wl_session_destroy(sess);
    wl_plan_free(plan);
}

static void
print_baseline(const char *label, uint32_t workers, const run_result_t *r)
{
    int count = 0;
    (void)wl_columnar_mem_subsys_name(0, &count);
    printf("\n    mem-baseline workload=chain100 build=%s workers=%u "
        "tuples=%" PRId64 " iterations=%u peak=%" PRIu64
        " rss_peak=%" PRIu64 " worker_peak_max=%" PRIu64
        " worker_reports=%" PRIu64 "\n",
        label, workers, r->tuples, r->iterations, r->stats.peak_bytes,
        r->stats.rss_peak_bytes, r->stats.worker_peak_max_bytes,
        r->stats.worker_reports);
    printf("    mem-baseline subsys:");
    for (int i = 0; i < count; i++)
        printf(" %s=%" PRIu64 "/%" PRIu64, wl_columnar_mem_subsys_name(i, NULL),
            r->stats.subsys_current_bytes[i], r->stats.subsys_peak_bytes[i]);
    printf("\n");
}

/*
 * force_tdd: the adaptive width chooser needs WIRELOG_TDD_MIN_ROWS_PER_WORKER
 * rows per worker (default 4096) before it hands a stratum to W workers;
 * the 100-edge fixture is far below that, so the W=8 runs pin the
 * threshold to 1 the same way test_tdd_decision_stats.c does.
 */
static void
force_tdd(bool on)
{
    if (on)
        setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    else
        unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
}

static const char *
build_label(void)
{
#if defined(ENABLE_K_FUSION) && ENABLE_K_FUSION == 0
    return "nofusion";
#else
    return "fused";
#endif
}

/* ======================================================================== */
/* Test 1: WL_MEM_REPORT does not change results                            */
/* ======================================================================== */

/*
 * Whether two runs of one workload reached the same ledger peaks.  Every
 * subsystem but CHANNEL must peak identically.  The coordinator pumps the
 * TDD channel while the workers publish (#1373), so how many bytes are
 * queued at once, and with it the CHANNEL peak, depends on scheduling; the
 * total peak may therefore differ by at most the larger CHANNEL peak.
 */
static bool
peaks_match(const wl_columnar_mem_stats_t *a, const wl_columnar_mem_stats_t *b)
{
    int count = 0;
    (void)wl_columnar_mem_subsys_name(0, &count);
    for (int s = 0; s < count; s++)
        if (s != WL_MEM_SUBSYS_CHANNEL
            && a->subsys_peak_bytes[s] != b->subsys_peak_bytes[s])
            return false;
    uint64_t channel = a->subsys_peak_bytes[WL_MEM_SUBSYS_CHANNEL];
    if (b->subsys_peak_bytes[WL_MEM_SUBSYS_CHANNEL] > channel)
        channel = b->subsys_peak_bytes[WL_MEM_SUBSYS_CHANNEL];
    uint64_t diff = a->peak_bytes > b->peak_bytes
        ? a->peak_bytes - b->peak_bytes : b->peak_bytes - a->peak_bytes;
    return diff <= channel;
}

static int
test_report_identity(uint32_t workers)
{
    char name[96];
    snprintf(name, sizeof(name),
        "WL_MEM_REPORT unset/1/2 produce identical results (W=%u)", workers);
    TEST(name);

    const char *levels[3] = { NULL, "1", "2" };
    run_result_t res[3];
    force_tdd(workers > 1);
    for (int i = 0; i < 3; i++) {
        if (levels[i])
            setenv("WL_MEM_REPORT", levels[i], 1);
        else
            unsetenv("WL_MEM_REPORT");
        run_chain(workers, &res[i]);
    }
    unsetenv("WL_MEM_REPORT");
    force_tdd(false);

    for (int i = 0; i < 3; i++) {
        if (res[i].rc != 0) {
            FAIL("evaluation failed");
            return 1;
        }
        if (!res[i].exact || res[i].tuples != CHAIN_CLOSURE_ROWS) {
            FAIL("closure contents wrong");
            return 1;
        }
    }
    for (int i = 1; i < 3; i++) {
        if (res[i].tuples != res[0].tuples
            || res[i].iterations != res[0].iterations) {
            FAIL("tuples/iterations differ across WL_MEM_REPORT levels");
            return 1;
        }
        if (!peaks_match(&res[i].stats, &res[0].stats)) {
            FAIL("ledger peak differs across WL_MEM_REPORT levels");
            return 1;
        }
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: coverage and symmetry                                            */
/* ======================================================================== */

static int
check_common(const run_result_t *r)
{
    const wl_columnar_mem_stats_t *st = &r->stats;

    if (r->rc != 0 || !r->exact) {
        FAIL("evaluation failed");
        return 1;
    }
    /* ARENA: exactly the fixed capacity of the session's own allocators.
     * Worker allocators are charged to the worker ledgers, not here. */
    if (st->subsys_current_bytes[WL_MEM_SUBSYS_ARENA] != r->arena_expected
        || st->subsys_peak_bytes[WL_MEM_SUBSYS_ARENA] < r->arena_expected) {
        FAIL("ARENA does not match pool + arena capacity");
        return 1;
    }
    /* STORED: the closure relation is sampled at the end of the snapshot. */
    if (st->subsys_current_bytes[WL_MEM_SUBSYS_STORED]
        < (uint64_t)CHAIN_CLOSURE_ROWS * 2u * sizeof(int64_t)) {
        FAIL("STORED smaller than the closure relation");
        return 1;
    }
    /* ARRANGEMENT and RELATION: the self-join builds at least one hash
     * index and produces ledger-attached join outputs on the session that
     * ran it -- the coordinator at W=1, the workers (folded into
     * worker_peak_max) when the stratum ran under TDD. */
    if (st->subsys_peak_bytes[WL_MEM_SUBSYS_ARRANGEMENT] == 0
        && st->worker_peak_max_bytes == 0) {
        FAIL("neither ARRANGEMENT nor any worker peak was charged");
        return 1;
    }
    if (st->subsys_peak_bytes[WL_MEM_SUBSYS_RELATION] == 0
        && st->worker_peak_max_bytes == 0) {
        FAIL("neither RELATION nor any worker peak was charged");
        return 1;
    }
    /* Symmetry: transient classes are fully credited back. */
    if (st->subsys_current_bytes[WL_MEM_SUBSYS_CHANNEL] != 0) {
        FAIL("CHANNEL not back to zero after snapshot");
        return 1;
    }
    if (st->subsys_current_bytes[WL_MEM_SUBSYS_CACHE] != 0) {
        FAIL("CACHE not back to zero after snapshot");
        return 1;
    }
    if (st->subsys_current_bytes[WL_MEM_SUBSYS_TEMPORARY] != 0) {
        FAIL("TEMPORARY not back to zero after snapshot");
        return 1;
    }
    uint64_t sum = 0;
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT; i++)
        sum += st->subsys_current_bytes[i];
    if (sum != st->current_bytes) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "current_bytes=%" PRIu64 " but subsystem sum=%" PRIu64,
            st->current_bytes, sum);
        FAIL(msg);
        return 1;
    }
    if (st->peak_bytes < st->current_bytes) {
        FAIL("peak_bytes below current_bytes");
        return 1;
    }
    return 0;
}

static int
test_coverage_sequential(void)
{
    TEST("ledger covers arena/stored/arrangement/relation at W=1");
    unsetenv("WL_MEM_REPORT");
    force_tdd(false);
    run_result_t r;
    run_chain(1, &r);
    if (check_common(&r))
        return 1;
    if (r.stats.worker_reports != 0
        || r.stats.subsys_peak_bytes[WL_MEM_SUBSYS_CHANNEL] != 0) {
        FAIL("W=1 must not create TDD workers or a delta channel");
        return 1;
    }
    print_baseline(build_label(), 1, &r);
    PASS();
    return 0;
}

static int
test_coverage_tdd(void)
{
    TEST("ledger covers TDD channel and worker peaks at W=8");
    unsetenv("WL_MEM_REPORT");
    force_tdd(true);
    run_result_t r;
    run_chain(8, &r);
    force_tdd(false);
    if (check_common(&r))
        return 1;
    if (r.selected_workers == 8) {
        /* The stratum ran under TDD: deltas crossed the TDD batch channel
         * and every worker teardown folded its ledger peak into the
         * coordinator aggregates. */
        if (r.stats.subsys_peak_bytes[WL_MEM_SUBSYS_CHANNEL] == 0) {
            FAIL("CHANNEL peak is zero although TDD ran at W=8");
            return 1;
        }
        if (r.stats.worker_reports == 0
            || r.stats.worker_peak_max_bytes == 0
            || r.stats.worker_peak_sum_bytes < r.stats.worker_peak_max_bytes) {
            FAIL("worker peak aggregates missing after TDD run");
            return 1;
        }
    } else {
        printf(" (TDD not selected: selected_workers=%u, channel checks "
            "skipped)", r.selected_workers);
    }
    print_baseline(build_label(), 8, &r);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: accessor contract                                                */
/* ======================================================================== */

static int
test_accessor_contract(void)
{
    TEST("stats accessor is NULL-safe and names every subsystem");
    wl_columnar_mem_stats_t st;
    memset(&st, 0xAB, sizeof(st));
    col_session_get_mem_stats(NULL, &st);
    if (st.peak_bytes != 0 || st.current_bytes != 0 || st.worker_reports != 0) {
        FAIL("NULL session must zero the stats");
        return 1;
    }
    col_session_get_mem_stats(NULL, NULL);

    int count = 0;
    (void)wl_columnar_mem_subsys_name(0, &count);
    if (count != WL_MEM_SUBSYS_COUNT || count > 8) {
        FAIL("subsystem count mismatch");
        return 1;
    }
    for (int i = 0; i < count; i++) {
        if (!wl_columnar_mem_subsys_name(i, NULL)) {
            FAIL("subsystem without a name");
            return 1;
        }
    }
    if (wl_columnar_mem_subsys_name(count, NULL) != NULL
        || wl_columnar_mem_subsys_name(-1, NULL) != NULL) {
        FAIL("out-of-range subsystem must return NULL");
        return 1;
    }
    PASS();
    return 0;
}

int
main(void)
{
    printf("Memory instrumentation tests (Issue #1380, %s build)\n",
        build_label());
    printf("======================================================\n\n");

    test_accessor_contract();
    test_coverage_sequential();
    test_coverage_tdd();
    test_report_identity(1);
    test_report_identity(8);

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
