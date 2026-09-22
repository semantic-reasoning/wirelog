/*
 * test_tdd_recursive.c - Unit tests for col_eval_stratum_tdd recursive path
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: W=1 TC baseline, W=2 and W=4 correctness vs single-worker,
 * cyclic graph convergence, deep chain (exercises EVAL_STRIDE).
 *
 * Issue #318: Distributed Stratum Evaluator Phase 2
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_TEST_ALLOC_WRAP
void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

static long allocation_fail_at = -1;
static long allocation_calls;
static size_t allocation_fail_calloc_size;
static wl_col_session_t *allocation_failure_worker;
static bool allocation_failed_after_transfer;

static bool
fail_this_allocation(void)
{
    return allocation_fail_at >= 0
           && allocation_calls++ == allocation_fail_at;
}

void *
__wrap_malloc(size_t size)
{
    return fail_this_allocation() ? NULL : __real_malloc(size);
}

void *
__wrap_calloc(size_t count, size_t size)
{
    if (allocation_fail_calloc_size && count == 1
        && size == allocation_fail_calloc_size) {
        allocation_fail_calloc_size = 0;
        allocation_failed_after_transfer = allocation_failure_worker
            && allocation_failure_worker->nrels > 0
            && allocation_failure_worker->rels != NULL;
        return NULL;
    }
    return fail_this_allocation() ? NULL : __real_calloc(count, size);
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    return fail_this_allocation() ? NULL : __real_realloc(ptr, size);
}
#endif

/* ======================================================================== */
/* Test Harness                                                             */
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

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

/*
 * Build a session with transitive closure:
 *   .decl edge(x: int32, y: int32)
 *   .decl tc(x: int32, y: int32)
 *   tc(x, y) :- edge(x, y).
 *   tc(x, z) :- tc(x, y), edge(y, z).
 *
 * tc is in a recursive stratum (depends on itself).
 */
static wl_col_session_t *
make_tc_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

static void
cleanup_session(wl_col_session_t *sess, wl_plan_t *plan,
    wirelog_program_t *prog)
{
    wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

static int
test_tdd_owner_lifetime_retry_gate(void)
{
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    if (!sess)
        return 1;

    col_rel_t *relation = col_rel_new_auto("retained_delta", 0);
    size_t bytes = sizeof(wl_columnar_eval_tdd_owner_lifetime_t)
        + sizeof(col_rel_t *);
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime
        = (wl_columnar_eval_tdd_owner_lifetime_t *)calloc(1, bytes);
    wl_columnar_source_access_reader_t reader = { 0 };
    int rc = relation && lifetime ? 0 : ENOMEM;
    bool attached = false;
    if (rc == 0)
        rc = col_rel_source_reader_acquire_transferable(relation, &reader);
    if (rc == 0) {
        lifetime->matrix_slots = 1;
        lifetime->relations[0] = relation;
        lifetime->evaluation_active = true;
        sess->tdd_owner_lifetime = lifetime;
        attached = true;
        if (wl_columnar_session_cleanup_ready(sess) != EBUSY
            || sess->tdd_owner_lifetime != lifetime
            || wl_columnar_session_ensure_tdd_worker_slots(sess, 2) != EBUSY)
            rc = EPROTO;
    }
    if (rc == 0) {
        lifetime->evaluation_active = false;
        for (unsigned attempt = 0; attempt < 2; attempt++) {
            if (wl_columnar_session_cleanup_ready(sess) != EBUSY
                || sess->tdd_owner_lifetime != lifetime
                || lifetime->relations[0] != relation) {
                rc = EPROTO;
                break;
            }
        }
    }
    if (reader.owner) {
        int release_rc = col_rel_source_reader_release(&reader);
        if (rc == 0)
            rc = release_rc;
    }
    if (rc == 0)
        rc = wl_columnar_session_cleanup_ready(sess);
    if (rc == 0 && (sess->tdd_owner_lifetime != NULL
        || wl_columnar_session_ensure_tdd_worker_slots(sess, 2) != 0))
        rc = EPROTO;
    if (attached && sess->tdd_owner_lifetime) {
        sess->tdd_owner_lifetime->evaluation_active = false;
        int cleanup_rc = wl_columnar_eval_tdd_owner_lifetime_retry(sess);
        if (rc == 0)
            rc = cleanup_rc;
        if (cleanup_rc == 0)
            lifetime = NULL;
    }
    if (!attached) {
        free(lifetime);
        col_rel_destroy(relation);
    }
    cleanup_session(sess, plan, prog);
    return rc == 0 ? 0 : 1;
}

static int
test_tdd_owner_lifetime_alias_progress_and_dedup(void)
{
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    if (!sess)
        return 1;

    col_rel_t *root = col_rel_new_auto("lifetime-root", 1);
    col_rel_t *alias = col_rel_new_auto("lifetime-alias", 1);
    size_t bytes = sizeof(wl_columnar_eval_tdd_owner_lifetime_t)
        + 3 * sizeof(col_rel_t *);
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime = calloc(1, bytes);
    wl_columnar_source_access_reader_t reader = { 0 };
    int64_t value = 19;
    int rc = root && alias && lifetime ? 0 : ENOMEM;
    bool attached = false;
    if (rc == 0)
        rc = col_rel_append_row(root, &value);
    if (rc == 0)
        rc = col_rel_install_shared_view(alias, root);
    if (rc == 0)
        rc = col_rel_source_reader_acquire_transferable(root, &reader);
    if (rc == 0) {
        lifetime->matrix_slots = 3;
        lifetime->relations[0] = root;
        lifetime->relations[1] = root;
        lifetime->relations[2] = alias;
        sess->tdd_owner_lifetime = lifetime;
        attached = true;
        if (wl_columnar_eval_tdd_owner_lifetime_retry(sess) != EBUSY
            || lifetime->relations[0] != root || lifetime->relations[1]
            || lifetime->relations[2] != NULL)
            rc = EPROTO;
    }
    if (reader.owner) {
        int release_rc = col_rel_source_reader_release(&reader);
        if (rc == 0)
            rc = release_rc;
    }
    if (rc == 0)
        rc = wl_columnar_eval_tdd_owner_lifetime_retry(sess);
    if (rc == 0 && sess->tdd_owner_lifetime)
        rc = EPROTO;
    if (!attached) {
        free(lifetime);
        col_rel_destroy(alias);
        col_rel_destroy(root);
    }
    cleanup_session(sess, plan, prog);
    return rc == 0 ? 0 : 1;
}

static int
test_tdd_owner_lifetime_mixed_backing_retention(void)
{
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_ref_t *ref = NULL;
    delta_pool_t *pool = NULL;
    wl_arena_t *arena = NULL;
    col_rel_t *heap = NULL;
    col_rel_t *pooled = NULL;
    col_rel_t *arena_rel = NULL;
    wl_columnar_source_access_reader_t readers[3] = { 0 };
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime = NULL;
    bool attached = false;
    int rc = sess ? 0 : ENOMEM;

    if (rc == 0) {
        resolution.budget_bytes = 1u << 20;
        resolution.usable_bytes = resolution.budget_bytes;
        resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
        resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
        resolution.status = WL_COLUMNAR_MEMORY_OK;
        ref = wl_columnar_memory_governor_ref_create(&resolution);
        pool = delta_pool_create(2, sizeof(col_rel_t), 4096);
        arena = wl_arena_create(4096);
        heap = col_rel_new_auto("lifetime-governed", 1);
        pooled = pool ? col_rel_pool_new_auto(pool, NULL,
                "lifetime-pooled", 1) : NULL;
        arena_rel = pool && arena ? col_rel_pool_new_auto(pool, arena,
                "lifetime-arena", 1) : NULL;
        if (!ref || !pool || !arena || !heap || !pooled || !arena_rel)
            rc = ENOMEM;
    }
    if (rc == 0)
        rc = col_rel_attach_memory_governor(heap, ref);
    int64_t value = 23;
    for (uint32_t i = 0; rc == 0 && i <= COL_REL_INIT_CAP; i++)
        rc = col_rel_append_row(heap, &value);
    if (rc == 0)
        rc = col_rel_append_row(pooled, &value);
    if (rc == 0)
        rc = col_rel_append_row(arena_rel, &value);
    if (rc == 0)
        rc = col_rel_source_reader_acquire_transferable(heap, &readers[0]);
    if (rc == 0)
        rc = col_rel_source_reader_acquire_transferable(pooled, &readers[1]);
    if (rc == 0)
        rc = col_rel_source_reader_acquire_transferable(arena_rel, &readers[2]);
    if (rc == 0) {
        size_t bytes = sizeof(*lifetime) + 3 * sizeof(col_rel_t *);
        lifetime = calloc(1, bytes);
        if (!lifetime)
            rc = ENOMEM;
        else {
            lifetime->matrix_slots = 3;
            lifetime->relations[0] = heap;
            lifetime->relations[1] = pooled;
            lifetime->relations[2] = arena_rel;
            sess->tdd_owner_lifetime = lifetime;
            attached = true;
        }
    }
    uint64_t reserved = ref ? wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref)) : 0;
    uint64_t reservation_bytes = heap ? heap->retained_reservation.bytes : 0;
    const void *reservation_identity = heap
        ? heap->retained_reservation.identity : NULL;
    for (unsigned attempt = 0; rc == 0 && attempt < 2; attempt++) {
        int retry_rc = wl_columnar_eval_tdd_owner_lifetime_retry(sess);
        if (retry_rc != EBUSY
            || lifetime->relations[0] != heap
            || lifetime->relations[1] != pooled
            || lifetime->relations[2] != arena_rel
            || !pooled->pool_owned || !arena_rel->arena_owned
            || heap->retained_reservation.identity != reservation_identity
            || heap->retained_reservation.bytes != reservation_bytes
            || wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) != reserved){
            rc = EPROTO;
        }
    }
    for (size_t i = 0; i < 3; i++) {
        if (!readers[i].owner)
            continue;
        int release_rc = col_rel_source_reader_release(&readers[i]);
        if (rc == 0)
            rc = release_rc;
    }
    if (rc == 0)
        rc = wl_columnar_eval_tdd_owner_lifetime_retry(sess);
    if (rc == 0 && sess->tdd_owner_lifetime)
        rc = EPROTO;
    if (!attached) {
        if (lifetime)
            free(lifetime);
        if (arena_rel)
            col_rel_destroy(arena_rel);
        if (pooled)
            col_rel_destroy(pooled);
        if (heap)
            col_rel_destroy(heap);
    }
    if (sess)
        cleanup_session(sess, plan, prog);
    if (pool)
        delta_pool_destroy(pool);
    if (arena)
        wl_arena_free(arena);
    if (ref) {
        if (wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) != 0)
            rc = EPROTO;
        wl_columnar_memory_governor_ref_release(ref);
    }
    return rc == 0 ? 0 : 1;
}

static int
insert_edges(wl_col_session_t *sess, const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 2);
}

static int
insert_filtered_edges(wl_col_session_t *sess, const int64_t *rows,
    uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 3);
}

static uint32_t
count_rows(wl_col_session_t *sess, const char *name)
{
    col_rel_t *r = session_find_rel(sess, name);

    return r ? r->nrows : 0;
}

static int
relation_rows_equal(wl_col_session_t *left, wl_col_session_t *right,
    const char *name)
{
    col_rel_t *lr = session_find_rel(left, name);
    col_rel_t *rr = session_find_rel(right, name);
    if (!lr || !rr)
        return lr == rr;
    if (lr->ncols != rr->ncols || lr->nrows != rr->nrows)
        return 0;

    WL_IGNORE_RESULT(col_rel_radix_sort_int64(lr));
    WL_IGNORE_RESULT(col_rel_radix_sort_int64(rr));

    for (uint32_t c = 0; c < lr->ncols; c++)
        for (uint32_t r = 0; r < lr->nrows; r++)
            if (lr->columns[c][r] != rr->columns[c][r])
                return 0;
    return 1;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * test_tc_w1_baseline:
 * W=1 transitive closure on a 4-node chain: 1->2->3->4.
 * Expected 6 tc tuples: (1,2),(2,3),(3,4),(1,3),(2,4),(1,4).
 */
static int
test_tc_w1_baseline(void)
{
    TEST("W=1 TC baseline: chain 1->2->3->4 yields 6 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 4 };
    if (insert_edges(sess, rows, 3) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (nrows != 6) {
        FAIL("expected 6 tc tuples");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w2_correctness:
 * W=2 TC on chain 1->2->3->4 must match W=1 row count.
 */
static int
test_tc_w2_correctness(void)
{
    TEST("W=2 TC correctness matches W=1 baseline (chain)");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 4 };
    insert_edges(sess1, rows, 3);
    insert_edges(sess2, rows, 3);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w4_correctness:
 * W=4 TC on 6-node chain must match W=1.
 */
static int
test_tc_w4_correctness(void)
{
    TEST("W=4 TC correctness matches W=1 baseline (6-node chain)");

    wl_plan_t *plan1 = NULL, *plan4 = NULL;
    wirelog_program_t *prog1 = NULL, *prog4 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess4 = make_tc_session(4, &plan4, &prog4);
    if (!sess4) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=4 session create");
        return 1;
    }

    /* Chain: 1->2->3->4->5->6 (5 edges, 15 TC tuples) */
    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6 };
    insert_edges(sess1, rows, 5);
    insert_edges(sess4, rows, 5);

    int rc1 = wl_session_step(&sess1->base);
    int rc4 = wl_session_step(&sess4->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt4 = count_rows(sess4, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess4, plan4, prog4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt4) {
        FAIL("W=4 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_cyclic:
 * Cyclic graph 1->2->3->1. TC must include all 9 pairs (including self-loops).
 * W=2 must match W=1.
 */
static int
test_tc_cyclic(void)
{
    TEST("W=2 TC cyclic graph convergence matches W=1 (3-cycle)");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 2, 3, 3, 1 };
    insert_edges(sess1, rows, 3);
    insert_edges(sess2, rows, 3);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 cyclic: row count differs from W=1 baseline");
        return 1;
    }
    /* W=1 should also be 9 (all 3x3 pairs from the 3-cycle) */
    if (cnt1 != 9) {
        FAIL("W=1 cyclic: expected 9 tc tuples");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_w2_empty_edb:
 * Empty edge set: TC must be empty with W=2.
 */
static int
test_tc_w2_empty_edb(void)
{
    TEST("W=2 TC with empty EDB yields empty tc");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_tc_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    int rc = wl_session_step(&sess->base);
    uint32_t nrows = count_rows(sess, "tc");
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 0) {
        FAIL("expected 0 tc tuples from empty EDB");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_deep_chain:
 * Deep chain of EVAL_STRIDE*2+3 nodes to exercise stride-based iteration.
 * W=2 row count must match W=1.
 */
static int
test_tc_deep_chain(void)
{
    TEST("W=2 TC deep chain (20 nodes) matches W=1");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    /* Chain 1->2->...->20: 19 edges, 190 TC tuples (n*(n-1)/2 = 19*20/2=190) */
    const uint32_t N = 20;
    int64_t rows[38]; /* 2*19 */
    for (uint32_t i = 0; i < N - 1; i++) {
        rows[(size_t)i * 2] = (int64_t)i + 1;
        rows[(size_t)i * 2 + 1] = (int64_t)i + 2;
    }

    insert_edges(sess1, rows, N - 1);
    insert_edges(sess2, rows, N - 1);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 deep chain: row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * test_tc_category_a_diamond:
 * Category A stratum (recursive + EXCHANGE + no IDB-IDB joins).
 * Diamond graph: 1->2, 1->3, 2->4, 3->4.
 * TC: (1,2),(1,3),(2,4),(3,4),(1,4) = 5 tuples.
 * W=2 must match W=1, exercising TDD for Category A after the #390 fix.
 */
static int
test_tc_category_a_diamond(void)
{
    TEST("Category A: W=2 TC diamond graph matches W=1 (no IDB-IDB join)");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_tc_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_tc_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = { 1, 2, 1, 3, 2, 4, 3, 4 };
    insert_edges(sess1, rows, 4);
    insert_edges(sess2, rows, 4);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "tc");
    uint32_t cnt2 = count_rows(sess2, "tc");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != 5) {
        FAIL("W=1 expected 5 tc tuples");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 tc row count differs from W=1 baseline");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * make_triple_idb_session:
 * Build a session with a 3-IDB-body-atom rule:
 *   .decl e(x: int32, y: int32)
 *   .decl r(x: int32, y: int32)
 *   r(x, y) :- e(x, y).
 *   r(x, y) :- r(x, a), r(a, b), r(b, y).
 *
 * The recursive rule has 3 IDB body atoms (all r).
 * BDX mode must NOT be enabled for this stratum.
 */
static wl_col_session_t *
make_triple_idb_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl e(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- e(x, y).\n"
        "r(x, y) :- r(x, a), r(a, b), r(b, y).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

/*
 * test_triple_idb_guard:
 * 3-IDB-body-atom rule must fall back to single-threaded (BDX guard).
 * W=2 must still produce correct results matching W=1.
 */
static int
test_triple_idb_guard(void)
{
    TEST(
        "3-IDB guard: W=2 triple-join r(x,y):-r(x,a),r(a,b),r(b,y) matches W=1");

    wl_plan_t *plan1 = NULL, *plan2 = NULL;
    wirelog_program_t *prog1 = NULL, *prog2 = NULL;
    wl_col_session_t *sess1 = make_triple_idb_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }
    wl_col_session_t *sess2 = make_triple_idb_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    /* Chain: 1->2->3->4->5 (4 edges) */
    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5 };
    if (wl_session_insert(&sess1->base, "e", rows, 4, 2) != 0
        || wl_session_insert(&sess2->base, "e", rows, 4, 2) != 0) {
        cleanup_session(sess1, plan1, prog1);
        cleanup_session(sess2, plan2, prog2);
        FAIL("insert");
        return 1;
    }

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "r");
    uint32_t cnt2 = count_rows(sess2, "r");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL(
            "W=2 row count differs from W=1 (guard should force single-threaded)");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* BDX integration tests                                                    */
/* ======================================================================== */

/*
 * Self-join TC program:
 *   .decl edge(x: int32, y: int32)
 *   .decl r(x: int32, y: int32)
 *   r(x, y) :- edge(x, y).
 *   r(x, z) :- r(x, y), r(y, z).
 *
 * The second rule is an IDB-IDB self-join (Category C) → triggers BDX mode.
 */
static wl_col_session_t *
make_selfjoin_tc_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- edge(x, y).\n"
        "r(x, z) :- r(x, y), r(y, z).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

/*
 * 5-node chain: 1→2→3→4→5   ⇒  TC has 10 tuples (4+3+2+1).
 */
static int
test_bdx_selfjoin_w2(void)
{
    TEST("BDX self-join W=2 correctness");

    wl_plan_t *p1, *p2;
    wirelog_program_t *pr1, *pr2;
    wl_col_session_t *s1 = make_selfjoin_tc_session(1, &p1, &pr1);
    wl_col_session_t *s2 = make_selfjoin_tc_session(2, &p2, &pr2);
    if (!s1 || !s2) {
        FAIL("session creation failed");
        return 1;
    }

    /* 5-node chain */
    int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5};
    insert_edges(s1, edges, 4);
    insert_edges(s2, edges, 4);

    int rc1 = wl_session_step(&s1->base);
    int rc2 = wl_session_step(&s2->base);

    int equal = relation_rows_equal(s1, s2, "r");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s2, p2, pr2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (!equal) {
        FAIL("BDX W=2 row set differs from W=1");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_bdx_selfjoin_w4(void)
{
    TEST("BDX self-join W=4 correctness");

    wl_plan_t *p1, *p4;
    wirelog_program_t *pr1, *pr4;
    wl_col_session_t *s1 = make_selfjoin_tc_session(1, &p1, &pr1);
    wl_col_session_t *s4 = make_selfjoin_tc_session(4, &p4, &pr4);
    if (!s1 || !s4) {
        FAIL("session creation failed");
        return 1;
    }

    /* 5-node chain */
    int64_t edges[] = {1, 2, 2, 3, 3, 4, 4, 5};
    insert_edges(s1, edges, 4);
    insert_edges(s4, edges, 4);

    int rc1 = wl_session_step(&s1->base);
    int rc4 = wl_session_step(&s4->base);

    int equal = relation_rows_equal(s1, s4, "r");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s4, p4, pr4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (!equal) {
        FAIL("BDX W=4 row set differs from W=1");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_bdx_selfjoin_w4_shuffled_duplicates(void)
{
    TEST("BDX self-join W=4 shuffled duplicate edges matches W=1 rows");

    wl_plan_t *p1, *p4;
    wirelog_program_t *pr1, *pr4;
    wl_col_session_t *s1 = make_selfjoin_tc_session(1, &p1, &pr1);
    wl_col_session_t *s4 = make_selfjoin_tc_session(4, &p4, &pr4);
    if (!s1 || !s4) {
        FAIL("session creation failed");
        return 1;
    }

    int64_t edges[] = {
        3, 4, 1, 2, 2, 3, 4, 5, 2, 3,
        1, 2, 5, 6, 3, 4, 6, 7, 4, 5
    };
    insert_edges(s1, edges, 10);
    insert_edges(s4, edges, 10);

    int rc1 = wl_session_step(&s1->base);
    int rc4 = wl_session_step(&s4->base);

    int equal = relation_rows_equal(s1, s4, "r");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s4, p4, pr4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (!equal) {
        FAIL("BDX W=4 shuffled duplicate row set differs from W=1");
        return 1;
    }
    PASS();
    return 0;
}

static col_rel_t *
make_seed_relation(const char *name, const int64_t *rows, uint32_t nrows,
    uint32_t ncols)
{
    col_rel_t *rel = col_rel_new_auto(name, ncols);
    if (!rel)
        return NULL;
    for (uint32_t row = 0; row < nrows; row++) {
        if (col_rel_append_row(rel, rows + (size_t)row * ncols) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    return rel;
}

typedef struct {
    uint32_t nrows;
    uint32_t ncols;
    uint32_t capacity;
    int64_t **columns;
    char **col_names;
    col_delta_timestamp_t *timestamps;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint64_t ledger_ts_bytes;
    uint64_t owned_ledger_bytes;
    uint64_t *dedup_slots;
    uint32_t dedup_cap;
    uint32_t dedup_count;
    int64_t first_row[2];
    col_delta_timestamp_t first_timestamp;
} bdx_seed_snapshot_t;

static void
capture_bdx_seed_snapshot(const col_rel_t *rel, bdx_seed_snapshot_t *snapshot)
{
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->nrows = rel->nrows;
    snapshot->ncols = rel->ncols;
    snapshot->capacity = rel->capacity;
    snapshot->columns = rel->columns;
    snapshot->col_names = rel->col_names;
    snapshot->timestamps = rel->timestamps;
    snapshot->view_generation = rel->view_generation;
    snapshot->storage_generation = rel->storage_generation;
    snapshot->ledger_ts_bytes = rel->ledger_ts_bytes;
    snapshot->owned_ledger_bytes = col_rel_owned_ledger_bytes(rel);
    snapshot->dedup_slots = rel->dedup_slots;
    snapshot->dedup_cap = rel->dedup_cap;
    snapshot->dedup_count = rel->dedup_count;
    if (rel->nrows > 0 && rel->ncols >= 2) {
        snapshot->first_row[0] = rel->columns[0][0];
        snapshot->first_row[1] = rel->columns[1][0];
    }
    if (rel->timestamps && rel->nrows > 0)
        snapshot->first_timestamp = rel->timestamps[0];
}

static bool
bdx_seed_snapshot_unchanged(const col_rel_t *rel,
    const bdx_seed_snapshot_t *snapshot)
{
    return rel->nrows == snapshot->nrows
           && rel->ncols == snapshot->ncols
           && rel->capacity == snapshot->capacity
           && rel->columns == snapshot->columns
           && rel->col_names == snapshot->col_names
           && rel->timestamps == snapshot->timestamps
           && rel->view_generation == snapshot->view_generation
           && rel->storage_generation == snapshot->storage_generation
           && rel->ledger_ts_bytes == snapshot->ledger_ts_bytes
           && col_rel_owned_ledger_bytes(rel) == snapshot->owned_ledger_bytes
           && rel->dedup_slots == snapshot->dedup_slots
           && rel->dedup_cap == snapshot->dedup_cap
           && rel->dedup_count == snapshot->dedup_count
           && (rel->nrows == 0 || (rel->columns[0][0]
           == snapshot->first_row[0]
           && rel->columns[1][0] == snapshot->first_row[1]))
           && (!rel->timestamps || rel->nrows == 0
           || memcmp(&rel->timestamps[0], &snapshot->first_timestamp,
           sizeof(snapshot->first_timestamp)) == 0);
}

static void
prepare_bdx_seed_metadata(col_rel_t *rel)
{
    rel->dedup_slots = (uint64_t *)calloc(4, sizeof(*rel->dedup_slots));
    rel->dedup_cap = 4;
    rel->dedup_count = 2;
    rel->dedup_slots[1] = 0x1111;
    rel->dedup_slots[3] = 0x3333;
    (void)col_rel_enable_timestamps(rel);
    rel->timestamps[0].iteration = 17;
    rel->timestamps[0].multiplicity = -4;
}

static int
test_bdx_seed_reader_denial_and_retry(void)
{
    TEST("BDX seed reader denial preserves cidb and retries");

    const int64_t initial[] = { 99, 100 };
    const int64_t worker_rows[] = { 2, 3, 1, 2, 2, 3 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 3, 2);
    col_rel_t *workers[] = { worker };
    wl_columnar_source_access_reader_t reader = { 0 };
    uint64_t generation;
    int rc;

    if (!cidb || !worker || col_rel_source_reader_acquire(cidb, &reader) != 0) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("seed reader setup");
        return 1;
    }
    generation = cidb->view_generation;
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != EBUSY || cidb->nrows != 1
        || cidb->columns[0][0] != 99 || cidb->columns[1][0] != 100
        || cidb->view_generation != generation) {
        col_rel_source_reader_release(&reader);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("active reader was not denied transactionally");
        return 1;
    }
    if (col_rel_source_reader_release(&reader) != 0
        || wl_columnar_eval_test_bdx_seed(cidb, workers, 1) != 0
        || cidb->nrows != 2
        || cidb->columns[0][0] != 1 || cidb->columns[1][0] != 2
        || cidb->columns[0][1] != 2 || cidb->columns[1][1] != 3) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("seed retry or dedup result");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_alias_denial_and_retry(void)
{
    TEST("BDX seed live-alias denial preserves cidb and retries");

    const int64_t initial[] = { 7, 8 };
    const int64_t worker_rows[] = { 4, 5, 4, 5 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 2, 2);
    col_rel_t *alias = col_rel_new_auto("alias", 2);
    col_rel_t *workers[] = { worker };
    int rc;

    if (!cidb || !worker || !alias
        || col_rel_install_shared_view(alias, cidb) != 0) {
        col_rel_destroy(alias);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("seed alias setup");
        return 1;
    }
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != EBUSY || cidb->nrows != 1
        || cidb->columns[0][0] != 7 || cidb->columns[1][0] != 8) {
        col_rel_storage_alias_release(alias);
        col_rel_destroy(alias);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("live alias was not denied transactionally");
        return 1;
    }
    if (col_rel_storage_alias_release(alias) != 0
        || wl_columnar_eval_test_bdx_seed(cidb, workers, 1) != 0
        || cidb->nrows != 1
        || cidb->columns[0][0] != 4 || cidb->columns[1][0] != 5) {
        col_rel_destroy(alias);
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("alias retry or dedup result");
        return 1;
    }
    col_rel_destroy(alias);
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_staging_failure_is_atomic(void)
{
    TEST("BDX seed staging failure leaves cidb unchanged");

    const int64_t initial[] = { 11, 12 };
    const int64_t bad_rows[] = { 1, 2, 3 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *bad_worker = make_seed_relation("r", bad_rows, 1, 3);
    col_rel_t *workers[] = { bad_worker };
    uint64_t generation;
    int rc;

    if (!cidb || !bad_worker) {
        col_rel_destroy(cidb);
        col_rel_destroy(bad_worker);
        FAIL("seed staging setup");
        return 1;
    }
    generation = cidb->view_generation;
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != EINVAL || cidb->nrows != 1
        || cidb->ncols != 2 || cidb->columns[0][0] != 11
        || cidb->columns[1][0] != 12
        || cidb->view_generation != generation) {
        col_rel_destroy(cidb);
        col_rel_destroy(bad_worker);
        FAIL("schema staging failure changed cidb");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(bad_worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_second_worker_failure_is_atomic(void)
{
    TEST("BDX seed second-worker staging failure is atomic");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker0_rows[] = { 1, 2, 2, 3 };
    const int64_t worker1_rows[] = { 3, 4, 3, 4 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker0 = make_seed_relation("r", worker0_rows, 2, 2);
    col_rel_t *worker1 = make_seed_relation("r", worker1_rows, 2, 2);
    col_rel_t *workers[] = { worker0, worker1 };
    bdx_seed_snapshot_t snapshot;
    int rc;

    if (!cidb || !worker0 || !worker1) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker0);
        col_rel_destroy(worker1);
        FAIL("second-worker staging setup");
        return 1;
    }
    prepare_bdx_seed_metadata(cidb);
    capture_bdx_seed_snapshot(cidb, &snapshot);
    wl_columnar_eval_test_bdx_seed_fail_worker(1);
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 2);
    if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(cidb, &snapshot)) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker0);
        col_rel_destroy(worker1);
        FAIL("second-worker failure changed cidb");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker0);
    col_rel_destroy(worker1);
    PASS();
    return 0;
}

static int
test_bdx_seed_sort_failure_is_atomic(void)
{
    TEST("BDX seed sort failure is atomic");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker_rows[] = { 2, 3, 1, 2 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 2, 2);
    col_rel_t *workers[] = { worker };
    bdx_seed_snapshot_t snapshot;
    int rc;

    if (!cidb || !worker) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("sort failure setup");
        return 1;
    }
    prepare_bdx_seed_metadata(cidb);
    capture_bdx_seed_snapshot(cidb, &snapshot);
    wl_columnar_eval_test_bdx_seed_fail_sort_once();
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(cidb, &snapshot)) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("sort failure changed cidb");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

static int
test_bdx_seed_allocation_failure_is_atomic(void)
{
#ifdef WL_TEST_ALLOC_WRAP
    TEST("BDX seed allocation failure is atomic");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker_rows[] = { 2, 3, 1, 2 };
    bool observed_failure = false;

    for (long fail_at = 0; fail_at < 64 && !observed_failure; fail_at++) {
        col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker_rows, 2, 2);
        col_rel_t *workers[] = { worker };
        bdx_seed_snapshot_t snapshot;
        int rc;
        if (!cidb || !worker) {
            col_rel_destroy(cidb);
            col_rel_destroy(worker);
            FAIL("allocation failure setup");
            return 1;
        }
        prepare_bdx_seed_metadata(cidb);
        capture_bdx_seed_snapshot(cidb, &snapshot);
        allocation_calls = 0;
        allocation_fail_at = fail_at;
        rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
        allocation_fail_at = -1;
        if (rc == ENOMEM) {
            observed_failure = true;
            if (!bdx_seed_snapshot_unchanged(cidb, &snapshot)) {
                col_rel_destroy(cidb);
                col_rel_destroy(worker);
                FAIL("allocation failure changed cidb");
                return 1;
            }
        } else if (rc != 0) {
            col_rel_destroy(cidb);
            col_rel_destroy(worker);
            FAIL("unexpected allocation failure result");
            return 1;
        }
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
    }
    allocation_fail_at = -1;
    if (!observed_failure) {
        FAIL("allocation failure seam was not exercised");
        return 1;
    }
    PASS();
    return 0;
#else
    fputs("BDX seed allocation failure coverage skipped\n", stderr);
    return 0;
#endif
}

static int
test_bdx_seed_success_preserves_timestamps(void)
{
    TEST("BDX seed success preserves timestamp values and accounting");

    const int64_t initial[] = { 11, 12 };
    const int64_t worker_rows[] = { 2, 3, 1, 2, 2, 3 };
    col_rel_t *cidb = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 3, 2);
    col_rel_t *workers[] = { worker };
    wl_mem_ledger_t ledger;
    int rc;

    if (!cidb || !worker || col_rel_enable_timestamps(cidb) != 0
        || col_rel_enable_timestamps(worker) != 0) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("timestamp success setup");
        return 1;
    }
    wl_mem_ledger_init(&ledger, 0);
    cidb->mem_ledger = &ledger;
    col_rel_ledger_reconcile(cidb, 0);
    worker->timestamps[0].iteration = 20;
    worker->timestamps[0].multiplicity = 2;
    worker->timestamps[1].iteration = 10;
    worker->timestamps[1].multiplicity = 1;
    worker->timestamps[2].iteration = 30;
    worker->timestamps[2].multiplicity = 3;
    rc = wl_columnar_eval_test_bdx_seed(cidb, workers, 1);
    if (rc != 0 || cidb->nrows != 2 || !cidb->timestamps
        || cidb->columns[0][0] != 1 || cidb->columns[1][0] != 2
        || cidb->timestamps[0].iteration != 10
        || cidb->timestamps[0].multiplicity != 1
        || cidb->columns[0][1] != 2 || cidb->columns[1][1] != 3
        || cidb->timestamps[1].iteration != 20
        || cidb->timestamps[1].multiplicity != 2
        || cidb->ledger_ts_bytes != col_rel_timestamp_ledger_bytes(cidb)
        || cidb->ledger_ts_bytes == 0) {
        col_rel_destroy(cidb);
        col_rel_destroy(worker);
        FAIL("timestamp values or accounting not preserved");
        return 1;
    }
    col_rel_destroy(cidb);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

#ifdef WL_TEST_TDD_RESET_RESTORE
static int
test_tdd_reset_restore_transaction(void)
{
    TEST("TDD coordinator reset/restore preserves state transactionally");

    const int64_t rows[] = { 7, 8, 9, 10 };
    col_rel_t *rel = make_seed_relation("r", rows, 2, 2);
    col_rel_t *alias = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_col_session_t coord = { 0 };
    wl_plan_relation_t plan_rel = { 0 };
    wl_plan_stratum_t stratum = { 0 };
    col_rel_t **saved = NULL;
    uint64_t *saved_dedup = NULL;
    int64_t first0;
    int64_t first1;
    uint64_t generation;
    int rc;

    if (!rel || col_rel_enable_timestamps(rel) != 0) {
        col_rel_destroy(rel);
        FAIL("reset/restore setup");
        return 1;
    }
    rel->timestamps[0].iteration = 12;
    rel->timestamps[0].multiplicity = -3;
    rel->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    rel->compound_count = 1;
    rel->compound_arity_map = (uint32_t *)calloc(2,
            sizeof(*rel->compound_arity_map));
    if (!rel->compound_arity_map) {
        col_rel_destroy(rel);
        FAIL("compound metadata setup");
        return 1;
    }
    rel->compound_arity_map[0] = 1;
    rel->compound_arity_map[1] = 1;
    rel->dedup_slots = (uint64_t *)calloc(4, sizeof(*rel->dedup_slots));
    if (!rel->dedup_slots) {
        col_rel_destroy(rel);
        FAIL("dedup setup");
        return 1;
    }
    rel->dedup_cap = 4;
    rel->dedup_count = 1;
    rel->dedup_slots[1] = 0xfeed;
    first0 = rel->columns[0][0];
    first1 = rel->columns[1][0];
    generation = rel->view_generation;

    rc = col_rel_source_reader_acquire(rel, &reader);
    if (rc != 0 || wl_columnar_eval_test_tdd_reset(rel) != EBUSY
        || rel->nrows != 2 || rel->columns[0][0] != first0
        || rel->columns[1][0] != first1
        || rel->view_generation != generation
        || col_rel_source_reader_release(&reader) != 0) {
        col_rel_destroy(rel);
        FAIL("reader denial changed coordinator relation");
        return 1;
    }

    alias = col_rel_new_auto("alias", 2);
    if (!alias || col_rel_install_shared_view(alias, rel) != 0
        || wl_columnar_eval_test_tdd_reset(rel) != EBUSY
        || rel->nrows != 2 || rel->columns[0][0] != first0
        || col_rel_storage_alias_release(alias) != 0) {
        if (alias)
            col_rel_storage_alias_release(alias);
        col_rel_destroy(alias);
        col_rel_destroy(rel);
        FAIL("live alias denial changed coordinator relation");
        return 1;
    }
    col_rel_destroy(alias);

    rc = wl_columnar_eval_test_tdd_reset(rel);
    if (rc != 0 || rel->nrows != 0 || rel->ncols != 2
        || rel->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        || rel->timestamps != NULL || rel->dedup_slots != NULL) {
        col_rel_destroy(rel);
        FAIL("successful reset did not preserve schema and clear rows");
        return 1;
    }

#ifdef WL_TEST_ALLOC_WRAP
    {
        bool observed_failure = false;
        for (long fail_at = 0; fail_at < 64 && !observed_failure;
            fail_at++) {
            col_rel_t *candidate = make_seed_relation("r", rows, 2, 2);
            bdx_seed_snapshot_t snapshot;
            if (!candidate) {
                FAIL("reset allocation setup");
                return 1;
            }
            capture_bdx_seed_snapshot(candidate, &snapshot);
            allocation_calls = 0;
            allocation_fail_at = fail_at;
            rc = wl_columnar_eval_test_tdd_reset(candidate);
            allocation_fail_at = -1;
            if (rc == ENOMEM) {
                observed_failure = true;
                if (!bdx_seed_snapshot_unchanged(candidate, &snapshot)) {
                    col_rel_destroy(candidate);
                    FAIL("reset allocation failure changed relation");
                    return 1;
                }
            } else if (rc != 0) {
                col_rel_destroy(candidate);
                FAIL("unexpected reset allocation result");
                return 1;
            }
            col_rel_destroy(candidate);
        }
        if (!observed_failure) {
            FAIL("reset allocation failure seam was not exercised");
            return 1;
        }
    }
#else
    /* Without --wrap the sweep above cannot run.  Say so, rather than let a
     * reduced-coverage run report the same PASS as a full one; this mirrors
     * test_bdx_seed_allocation_failure_is_atomic. */
    fputs("reset allocation failure coverage skipped\n", stderr);
#endif

    {
        col_rel_t *overflow = make_seed_relation("overflow", rows, 2, 2);
        bdx_seed_snapshot_t snapshot;
        if (!overflow) {
            FAIL("reset overflow setup");
            return 1;
        }
        overflow->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
        capture_bdx_seed_snapshot(overflow, &snapshot);
        rc = wl_columnar_eval_test_tdd_reset(overflow);
        if (rc != EOVERFLOW || !bdx_seed_snapshot_unchanged(overflow,
            &snapshot)) {
            col_rel_destroy(overflow);
            FAIL("reset overflow was not transactional");
            return 1;
        }
        col_rel_destroy(overflow);
    }

    if (session_add_rel(&coord, rel) != 0) {
        col_rel_destroy(rel);
        FAIL("restore session registration");
        return 1;
    }
    plan_rel.name = "r";
    stratum.relations = &plan_rel;
    stratum.relation_count = 1;
    saved_dedup = (uint64_t *)calloc(4, sizeof(*saved_dedup));
    if (!saved_dedup) {
        session_remove_rel(&coord, "r");
        free(coord.rels);
        FAIL("restore dedup setup");
        return 1;
    }
    /* Restore starts from a published relation with its own metadata. */
    rel = session_find_rel(&coord, "r");
    rel->nrows = 0;
    rel->dedup_slots = saved_dedup;
    rel->dedup_cap = 4;
    rel->dedup_count = 1;
    rel->dedup_slots[2] = 0xbeef;

    {
        col_rel_t *bad = col_rel_new_auto("r", 2);
        col_rel_t *bad_saved[1] = { bad };
        uint64_t before_generation = rel->view_generation;
        if (!bad) {
            session_remove_rel(&coord, "r");
            free(coord.rels);
            FAIL("restore schema setup");
            return 1;
        }
        bad->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        bad->compound_count = 1;
        bad->compound_arity_map = (uint32_t *)calloc(2,
                sizeof(*bad->compound_arity_map));
        if (!bad->compound_arity_map) {
            col_rel_destroy(bad);
            session_remove_rel(&coord, "r");
            free(coord.rels);
            FAIL("restore schema metadata setup");
            return 1;
        }
        bad->compound_arity_map[0] = 0;
        bad->compound_arity_map[1] = 1;
        if (wl_columnar_eval_test_tdd_restore(&stratum, &coord,
            bad_saved) != EINVAL
            || rel->view_generation != before_generation
            || rel->nrows != 0) {
            col_rel_destroy(bad);
            session_remove_rel(&coord, "r");
            free(coord.rels);
            FAIL("restore schema failure changed relation");
            return 1;
        }
        col_rel_destroy(bad);
    }

    if (col_rel_enable_timestamps(rel) != 0
        || wl_columnar_eval_test_tdd_save(&stratum, &coord, &saved) != 0) {
        session_remove_rel(&coord, "r");
        free(coord.rels);
        FAIL("lossless snapshot setup");
        return 1;
    }

    rel = session_find_rel(&coord, "r");
    rel->nrows = 0;
    if (col_rel_source_reader_acquire(rel, &reader) != 0
        || wl_columnar_eval_test_tdd_restore(&stratum, &coord, saved)
        != EBUSY
        || rel->nrows != 0
        || col_rel_source_reader_release(&reader) != 0
        || wl_columnar_eval_test_tdd_restore(&stratum, &coord, saved) != 0) {
        wl_columnar_eval_test_tdd_free_saved(&stratum, saved);
        session_remove_rel(&coord, "r");
        free(coord.rels);
        FAIL("restore reader denial or retry");
        return 1;
    }
    rel = session_find_rel(&coord, "r");
    if (!rel || rel->nrows != 0 || !rel->timestamps
        || rel->dedup_cap != 4 || rel->dedup_count != 1
        || rel->dedup_slots[2] != 0xbeef
        || rel->compound_kind != WIRELOG_COMPOUND_KIND_INLINE) {
        wl_columnar_eval_test_tdd_free_saved(&stratum, saved);
        session_remove_rel(&coord, "r");
        free(coord.rels);
        FAIL("restore did not preserve relation metadata");
        return 1;
    }
    wl_columnar_eval_test_tdd_free_saved(&stratum, saved);
    session_remove_rel(&coord, "r");
    session_rel_free_hash(&coord);
    free(coord.rels);

    /* Admission covers the complete relation set before publication.  A
     * reader on the second relation must not leave the first relation
     * restored while the second remains untouched. */
    {
        wl_col_session_t multi = { 0 };
        wl_plan_relation_t multi_plan[2] = { { 0 }, { 0 } };
        wl_plan_stratum_t multi_stratum = { 0 };
        col_rel_t *a = make_seed_relation("a", rows, 2, 2);
        const int64_t rows_b[] = { 11, 12, 13, 14 };
        col_rel_t *b = make_seed_relation("b", rows_b, 2, 2);
        col_rel_t **multi_saved = NULL;
        wl_columnar_source_access_reader_t b_reader = { 0 };

        if (!a || !b || session_add_rel(&multi, a) != 0
            || session_add_rel(&multi, b) != 0) {
            col_rel_destroy(a);
            col_rel_destroy(b);
            session_rel_free_hash(&multi);
            free(multi.rels);
            FAIL("multi-relation restore setup");
            return 1;
        }
        multi_plan[0].name = "a";
        multi_plan[1].name = "b";
        multi_stratum.relations = multi_plan;
        multi_stratum.relation_count = 2;
        if (wl_columnar_eval_test_tdd_save(&multi_stratum, &multi,
            &multi_saved) != 0) {
            session_remove_rel(&multi, "a");
            session_remove_rel(&multi, "b");
            session_rel_free_hash(&multi);
            free(multi.rels);
            FAIL("multi-relation snapshot setup");
            return 1;
        }
        session_find_rel(&multi, "a")->nrows = 0;
        session_find_rel(&multi, "b")->nrows = 0;
        if (col_rel_source_reader_acquire(session_find_rel(&multi, "b"),
            &b_reader) != 0
            || wl_columnar_eval_test_tdd_restore(&multi_stratum, &multi,
            multi_saved) != EBUSY
            || session_find_rel(&multi, "a")->nrows != 0
            || session_find_rel(&multi, "b")->nrows != 0
            || col_rel_source_reader_release(&b_reader) != 0
            || wl_columnar_eval_test_tdd_restore(&multi_stratum, &multi,
            multi_saved) != 0
            || session_find_rel(&multi, "a")->nrows != 2
            || session_find_rel(&multi, "b")->nrows != 2) {
            col_rel_source_reader_release(&b_reader);
            wl_columnar_eval_test_tdd_free_saved(&multi_stratum, multi_saved);
            session_remove_rel(&multi, "a");
            session_remove_rel(&multi, "b");
            session_rel_free_hash(&multi);
            free(multi.rels);
            FAIL("multi-relation reader admission or retry");
            return 1;
        }
        wl_columnar_eval_test_tdd_free_saved(&multi_stratum, multi_saved);
        session_remove_rel(&multi, "a");
        session_remove_rel(&multi, "b");
        session_rel_free_hash(&multi);
        free(multi.rels);
    }

    /* A malformed candidate for an absent relation must fail before any
     * candidate is registered, leaving the session registry unchanged. */
    {
        wl_col_session_t fresh = { 0 };
        wl_plan_relation_t fresh_plan[2] = { { 0 }, { 0 } };
        wl_plan_stratum_t fresh_stratum = { 0 };
        col_rel_t *bad = col_rel_new_auto("missing", 2);
        col_rel_t *bad_saved[2] = { bad, NULL };

        fresh_plan[0].name = "missing";
        fresh_plan[1].name = "also_missing";
        fresh_stratum.relations = fresh_plan;
        fresh_stratum.relation_count = 2;
        bad->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        bad->compound_count = 1;
        bad->compound_arity_map = (uint32_t *)calloc(2,
                sizeof(*bad->compound_arity_map));
        if (!bad->compound_arity_map) {
            col_rel_destroy(bad);
            FAIL("new-relation cleanup setup");
            return 1;
        }
        bad->compound_arity_map[0] = 0;
        bad->compound_arity_map[1] = 1;
        if (wl_columnar_eval_test_tdd_restore(&fresh_stratum, &fresh,
            bad_saved) != EINVAL || fresh.nrels != 0
            || session_find_rel(&fresh, "missing")
            || session_find_rel(&fresh, "also_missing")) {
            col_rel_destroy(bad);
            session_rel_free_hash(&fresh);
            free(fresh.rels);
            FAIL("failed restore left a new relation registered");
            return 1;
        }
        col_rel_destroy(bad);
        session_rel_free_hash(&fresh);
        free(fresh.rels);
    }
    PASS();
    return 0;
}
#endif

#ifdef WL_TEST_TDD_MERGE
static int
test_tdd_merge_transactional_publication(void)
{
    TEST("TDD merge stages worker results before relation publication");

    const int64_t initial[] = { 9, 9 };
    const int64_t worker0_rows[] = { 2, 3, 1, 2 };
    const int64_t worker1_rows[] = { 4, 5, 4, 5 };
    int rc;

    /* A live reader denies publication, then the same transaction retries. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *workers[] = { worker };
        wl_columnar_source_access_reader_t reader = { 0 };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker
            || col_rel_source_reader_acquire(target, &reader) != 0) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge reader setup");
            return 1;
        }
        capture_bdx_seed_snapshot(target, &snapshot);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EBUSY || !bdx_seed_snapshot_unchanged(target, &snapshot)
            || col_rel_source_reader_release(&reader) != 0) {
            col_rel_source_reader_release(&reader);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge reader denial or retry");
            return 1;
        }
        if (wl_columnar_eval_test_tdd_merge(&target, workers, 1) != 0
            || target->nrows != 3 || target->columns[0][0] != 1
            || target->columns[1][0] != 2) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge reader retry");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(worker);
    }

    /* A live alias is also denied without changing rows or metadata. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *alias = col_rel_new_auto("alias", 2);
        col_rel_t *workers[] = { worker };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker || !alias
            || col_rel_install_shared_view(alias, target) != 0) {
            col_rel_destroy(alias);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge alias setup");
            return 1;
        }
        capture_bdx_seed_snapshot(target, &snapshot);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EBUSY || !bdx_seed_snapshot_unchanged(target, &snapshot)
            || col_rel_storage_alias_release(alias) != 0) {
            col_rel_storage_alias_release(alias);
            col_rel_destroy(alias);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge alias denial or retry");
            return 1;
        }
        if (wl_columnar_eval_test_tdd_merge(&target, workers, 1) != 0) {
            col_rel_destroy(alias);
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge alias retry");
            return 1;
        }
        col_rel_destroy(alias);
        col_rel_destroy(target);
        col_rel_destroy(worker);
    }

    /* Failure in the second worker must not publish the first worker. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker0 = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *worker1 = make_seed_relation("r", worker1_rows, 2, 2);
        col_rel_t *workers[] = { worker0, worker1 };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker0 || !worker1) {
            col_rel_destroy(target);
            col_rel_destroy(worker0);
            col_rel_destroy(worker1);
            FAIL("merge failure setup");
            return 1;
        }
        prepare_bdx_seed_metadata(target);
        capture_bdx_seed_snapshot(target, &snapshot);
        wl_columnar_eval_test_tdd_merge_fail_worker(1);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 2);
        if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(worker0);
            col_rel_destroy(worker1);
            FAIL("second worker failure changed target");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(worker0);
        col_rel_destroy(worker1);
    }

    /* Schema, overflow, and sort errors all preserve the target. */
    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        const int64_t bad_schema_rows[] = { 1, 2, 3, 4, 5, 6 };
        col_rel_t *bad_schema = make_seed_relation("r", bad_schema_rows, 2,
                3);
        col_rel_t *valid_worker = make_seed_relation("r", worker0_rows, 2,
                2);
        col_rel_t *workers[] = { bad_schema };
        bdx_seed_snapshot_t snapshot;

        if (!target || !bad_schema || !valid_worker) {
            col_rel_destroy(target);
            col_rel_destroy(bad_schema);
            col_rel_destroy(valid_worker);
            FAIL("merge schema setup");
            return 1;
        }
        capture_bdx_seed_snapshot(target, &snapshot);
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(bad_schema);
            col_rel_destroy(valid_worker);
            FAIL("schema failure changed target");
            return 1;
        }
        workers[0] = valid_worker;
        wl_columnar_eval_test_tdd_merge_fail_overflow_once();
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != EOVERFLOW || !bdx_seed_snapshot_unchanged(target,
            &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(bad_schema);
            col_rel_destroy(valid_worker);
            FAIL("overflow failure changed target");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(bad_schema);
        col_rel_destroy(valid_worker);
    }

    {
        col_rel_t *target = make_seed_relation("r", initial, 1, 2);
        col_rel_t *worker = make_seed_relation("r", worker0_rows, 2, 2);
        col_rel_t *workers[] = { worker };
        bdx_seed_snapshot_t snapshot;

        if (!target || !worker) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("merge sort setup");
            return 1;
        }
        prepare_bdx_seed_metadata(target);
        capture_bdx_seed_snapshot(target, &snapshot);
        wl_columnar_eval_test_tdd_merge_fail_sort_once();
        rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
        if (rc != ENOMEM || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
            col_rel_destroy(target);
            col_rel_destroy(worker);
            FAIL("sort failure changed target");
            return 1;
        }
        col_rel_destroy(target);
        col_rel_destroy(worker);
    }

    PASS();
    return 0;
}

static int
test_tdd_merge_schema_mismatch_rollback(void)
{
    TEST("TDD merge rejects complete schema mismatches before staging");

    const int64_t initial[] = { 9, 9 };
    const int64_t worker_rows[] = { 2, 3 };
    col_rel_t *target = make_seed_relation("r", initial, 1, 2);
    col_rel_t *worker = make_seed_relation("r", worker_rows, 1, 2);
    col_rel_t *workers[] = { worker };
    bdx_seed_snapshot_t snapshot;
    int rc;

    if (!target || !worker) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("schema mismatch setup");
        return 1;
    }
    prepare_bdx_seed_metadata(target);
    capture_bdx_seed_snapshot(target, &snapshot);

    worker->declared_ncols = target->declared_ncols + 1;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("declared width mismatch changed target");
        return 1;
    }
    worker->declared_ncols = target->declared_ncols;

    worker->schema_ok = false;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("schema state mismatch changed target");
        return 1;
    }
    worker->schema_ok = target->schema_ok;

    free(worker->column_types);
    worker->column_types = (wirelog_column_type_t *)malloc(
        worker->ncols * sizeof(*worker->column_types));
    if (!worker->column_types) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column type mismatch setup");
        return 1;
    }
    for (uint32_t col = 0; col < worker->ncols; col++)
        worker->column_types[col] = WIRELOG_TYPE_FLOAT;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column type mismatch changed target");
        return 1;
    }
    free(worker->column_types);
    worker->column_types = NULL;

    free(worker->col_names[0]);
    worker->col_names[0] = strdup("different");
    if (!worker->col_names[0]) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column name mismatch setup");
        return 1;
    }
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("column name mismatch changed target");
        return 1;
    }
    free(worker->col_names[0]);
    worker->col_names[0] = strdup(target->col_names[0]);
    if (!worker->col_names[0]) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("compound mismatch setup");
        return 1;
    }

    worker->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    worker->compound_count = 1;
    worker->compound_arity_map = (uint32_t *)malloc(2 * sizeof(uint32_t));
    if (!worker->compound_arity_map) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("compound map mismatch setup");
        return 1;
    }
    worker->compound_arity_map[0] = 1;
    worker->compound_arity_map[1] = 1;
    rc = wl_columnar_eval_test_tdd_merge(&target, workers, 1);
    if (rc != EINVAL || !bdx_seed_snapshot_unchanged(target, &snapshot)) {
        col_rel_destroy(target);
        col_rel_destroy(worker);
        FAIL("compound schema mismatch changed target");
        return 1;
    }

    col_rel_destroy(target);
    col_rel_destroy(worker);
    PASS();
    return 0;
}

#endif

/* ======================================================================== */
/* Microbenchmark: Filtered-join arrangement cache (Issue #433)             */
/* ======================================================================== */

/*
 * make_filtered_tc_session:
 * Build a TC session with a constant filter in the right-side EDB atom:
 *   tc(x, y) :- edge(x, y, 1).
 *   tc(x, z) :- tc(x, y), edge(y, z, 1).
 *
 * Explicit comparisons such as y > 0 and z > 0 are lowered as FILTER
 * operators above the join; JPP does not move them into right_filter_expr.
 * Constants inside a right-side atom are collected as scan-local filters,
 * which triggers the filt_cache path in col_op_join.  With the filt_arr
 * optimization (Issue #433), the arrangement for the filtered edge relation
 * is cached across sub-passes rather than rebuilt each time.
 */
static wl_col_session_t *
make_filtered_tc_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32, tag: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y, 1).\n"
        "tc(x, z) :- tc(x, y), edge(y, z, 1).\n",
        &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(session);
}

/*
 * test_filt_arr_bench_w1:
 * W=1 filtered TC on a 10-node chain (9 edges, all y > 0).
 * Expected: 45 tuples (all pairs i<j in 1..10).
 *
 * Correctness regression for filt_arr arrangement cache (Issue #433):
 * verifies the cached arrangement for the filtered right-side EDB
 * produces the same results as the ephemeral-hash-table path.
 */
static int
test_filt_arr_bench_w1(void)
{
    TEST("Filt-arr bench W=1: 10-node chain filtered TC, 45 tuples");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_filtered_tc_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create failed");
        return 1;
    }

    /* 10-node chain: tag=1 edges (1,2)..(9,10). */
    int64_t edges[] = {
        1, 2, 1,  2, 3, 1,  3, 4, 1,  4, 5, 1,
        5, 6, 1,  6, 7, 1,  7, 8, 1,  8, 9, 1,
        9, 10, 1,
        /* This edge must be rejected by the atom filter. */
        10, 11, 0
    };
    if (insert_filtered_edges(sess, edges, 10) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert_edges failed");
        return 1;
    }

    uint64_t t0 = now_ns();
    int rc = wl_session_step(&sess->base);
    uint64_t elapsed_ms = (now_ns() - t0) / 1000000ULL;

    uint32_t nrows = count_rows(sess, "tc");
    uint32_t filt_cache_count = sess->filt_cache_count;
    cleanup_session(sess, plan, prog);

    if (rc != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (nrows != 45) {
        FAIL("expected 45 tc tuples");
        return 1;
    }
    if (filt_cache_count == 0) {
        FAIL("expected filtered edge cache to be populated");
        return 1;
    }
    printf(" [%llu ms]", (unsigned long long)elapsed_ms);
    PASS();
    return 0;
}

/*
 * test_filt_arr_bench_w4:
 * W=4 filtered TC must match W=1 result (45 tuples).
 * Validates that filt_arr caching is safe under K-fusion worker isolation.
 */
static int
test_filt_arr_bench_w4(void)
{
    TEST("Filt-arr bench W=4: matches W=1 filtered TC (45 tuples)");

    wl_plan_t *p1 = NULL, *p4 = NULL;
    wirelog_program_t *pr1 = NULL, *pr4 = NULL;
    wl_col_session_t *s1 = make_filtered_tc_session(1, &p1, &pr1);
    wl_col_session_t *s4 = make_filtered_tc_session(4, &p4, &pr4);
    if (!s1 || !s4) {
        if (s1) cleanup_session(s1, p1, pr1);
        if (s4) cleanup_session(s4, p4, pr4);
        FAIL("session create failed");
        return 1;
    }

    int64_t edges[] = {
        1, 2, 1,  2, 3, 1,  3, 4, 1,  4, 5, 1,
        5, 6, 1,  6, 7, 1,  7, 8, 1,  8, 9, 1,
        9, 10, 1,
        10, 11, 0
    };
    if (insert_filtered_edges(s1, edges, 10) != 0
        || insert_filtered_edges(s4, edges, 10) != 0) {
        cleanup_session(s1, p1, pr1);
        cleanup_session(s4, p4, pr4);
        FAIL("insert_edges failed");
        return 1;
    }

    int rc1 = wl_session_step(&s1->base);
    int rc4 = wl_session_step(&s4->base);

    uint32_t cnt1 = count_rows(s1, "tc");
    uint32_t cnt4 = count_rows(s4, "tc");

    cleanup_session(s1, p1, pr1);
    cleanup_session(s4, p4, pr4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != 45) {
        FAIL("W=1 expected 45 tc tuples");
        return 1;
    }
    if (cnt1 != cnt4) {
        FAIL("W=4 must match W=1");
        return 1;
    }
    PASS();
    return 0;
}

#ifdef WL_TEST_OWNER_PUBLICATION
static col_rel_t *
owner_publication_candidate(const char *name, int64_t value)
{
    col_rel_t *candidate = col_rel_new_auto(name, 1);
    if (!candidate || col_rel_append_row(candidate, &value) != 0) {
        col_rel_destroy(candidate);
        return NULL;
    }
    return candidate;
}

static void
owner_publication_session_cleanup(wl_col_session_t *session)
{
    if (!session)
        return;
    for (uint32_t i = 0; i < session->nrels; i++) {
        if (session->rels[i])
            (void)session_remove_rel(session, session->rels[i]->name);
    }
    session_rel_free_hash(session);
    free(session->rels);
    session->rels = NULL;
}

typedef struct owner_publication_snapshot {
    col_rel_t *relation;
    uint64_t identity;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint32_t nrows;
    uint32_t capacity;
    uint32_t base_nrows;
    uint32_t sorted_nrows;
    uint32_t run_count;
    uint32_t run_end;
    int64_t row0;
    wirelog_column_type_t type0;
} owner_publication_snapshot_t;

static owner_publication_snapshot_t
owner_publication_snapshot(col_rel_t *rel)
{
    return (owner_publication_snapshot_t){
               .relation = rel,
               .identity = rel->relation_identity,
               .view_generation = rel->view_generation,
               .storage_generation = rel->storage_generation,
               .nrows = rel->nrows,
               .capacity = rel->capacity,
               .base_nrows = rel->base_nrows,
               .sorted_nrows = rel->sorted_nrows,
               .run_count = rel->run_count,
               .run_end = rel->run_count ? rel->run_ends[0] : 0,
               .row0 = rel->nrows ? rel->columns[0][0] : 0,
               .type0 = rel->column_types ? rel->column_types[0]
                                   : WIRELOG_TYPE_INT64,
    };
}

static bool
owner_publication_snapshot_matches(const owner_publication_snapshot_t *snap)
{
    const col_rel_t *rel = snap ? snap->relation : NULL;
    return rel && rel->relation_identity == snap->identity
           && rel->view_generation == snap->view_generation
           && rel->storage_generation == snap->storage_generation
           && rel->nrows == snap->nrows && rel->capacity == snap->capacity
           && rel->base_nrows == snap->base_nrows
           && rel->sorted_nrows == snap->sorted_nrows
           && rel->run_count == snap->run_count
           && (!snap->run_count || rel->run_ends[0] == snap->run_end)
           && (!rel->nrows || rel->columns[0][0] == snap->row0)
           && (!rel->column_types || rel->column_types[0] == snap->type0);
}

static col_rel_t *
owner_publication_governed_target(const char *name, int64_t value,
    wl_columnar_memory_governor_ref_t *ref)
{
    col_rel_t *rel = owner_publication_candidate(name, value);
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    if (!rel || col_rel_set_column_types(rel, &type, 1) != 0)
        goto fail;
    rel->base_nrows = 1;
    rel->sorted_nrows = 1;
    rel->run_count = 1;
    rel->run_ends[0] = 1;
    if (col_rel_attach_memory_governor(rel, ref) != 0
        || col_rel_reserve_capacity_admitted(rel, rel->capacity, NULL) != 0)
        goto fail;
    return rel;
fail:
    col_rel_destroy(rel);
    return NULL;
}

static col_rel_t *
owner_publication_large_candidate(const char *name, int64_t first)
{
    col_rel_t *candidate = col_rel_new_auto(name, 1);
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    if (!candidate || col_rel_set_column_types(candidate, &type, 1) != 0)
        goto fail;
    for (uint32_t i = 0; i < COL_REL_INIT_CAP + 1u; i++) {
        int64_t value = first + i;
        if (col_rel_append_row(candidate, &value) != 0)
            goto fail;
    }
    candidate->base_nrows = 1;
    candidate->sorted_nrows = candidate->nrows;
    candidate->run_count = 1;
    candidate->run_ends[0] = candidate->nrows;
    return candidate;
fail:
    col_rel_destroy(candidate);
    return NULL;
}

static int
owner_publication_add_candidate(
    wl_columnar_eval_owner_publication_txn_t *txn,
    wl_col_session_t *session, const char *name, col_rel_t *target,
    col_rel_t **candidate)
{
    if (!candidate || !*candidate)
        return EINVAL;
    int rc = wl_columnar_eval_owner_publication_add(txn, session, name,
            target, *candidate);
    if (rc == 0)
        *candidate = NULL;
    else {
        col_rel_destroy(*candidate);
        *candidate = NULL;
    }
    return rc;
}

static int
test_owner_publication_hashless_registry(void)
{
    wl_col_session_t sessions[2] = { 0 };
    wl_columnar_eval_owner_publication_txn_t txn;
    col_rel_t *target[2] = { NULL, NULL };
    owner_publication_snapshot_t before[2] = { 0 };
    col_rel_t **registry[2] = { NULL, NULL };
    col_rel_t *registry_contents[2][4] = { { NULL } };
    uint32_t registry_counts[2] = { 0, 0 };
    const char *failure = NULL;
    int rc = EFAULT;

#define HASHLESS_CHECK(condition, message) \
        do { if (!(condition)) { failure = (message); goto cleanup; \
             } } while (0)
    wl_columnar_eval_owner_publication_init(&txn);
    for (uint32_t i = 0; i < 2; i++) {
        sessions[i].rel_cap = 4;
        sessions[i].rels = calloc(sessions[i].rel_cap,
                sizeof(*sessions[i].rels));
        HASHLESS_CHECK(sessions[i].rels, "hashless registry allocation");
        target[i] = owner_publication_candidate(i == 0
            ? "hashless-a" : "hashless-b", 10 + i);
        HASHLESS_CHECK(target[i], "hashless target allocation");
        if (session_add_rel(&sessions[i], target[i]) != 0) {
            col_rel_destroy(target[i]);
            target[i] = NULL;
            failure = "hashless target registration";
            goto cleanup;
        }
        target[i] = sessions[i].rels[0];
        before[i] = owner_publication_snapshot(target[i]);
        registry[i] = sessions[i].rels;
        registry_counts[i] = sessions[i].nrels;
        memcpy(registry_contents[i], sessions[i].rels,
            sessions[i].rel_cap * sizeof(*registry_contents[i]));
        session_rel_free_hash(&sessions[i]);
        HASHLESS_CHECK(!sessions[i].rel_hash_head
            && !sessions[i].rel_hash_next
            && sessions[i].rel_hash_nbuckets == 0
            && sessions[i].rel_hash_chain_cap == 0,
            "clear built hash for populated session");
    }

    /* Both target lookup and registration-image staging must leave the
     * populated-but-hashless sessions untouched on a later image failure. */
    {
        col_rel_t *candidate = owner_publication_candidate("hashless-a", 20);
        HASHLESS_CHECK(candidate, "hashless replacement candidate A");
        HASHLESS_CHECK(owner_publication_add_candidate(&txn, &sessions[0],
            "hashless-a", target[0], &candidate) == 0,
            "add hashless replacement A");
        candidate = owner_publication_candidate("$new-a", 30);
        HASHLESS_CHECK(candidate, "hashless registration candidate A");
        HASHLESS_CHECK(owner_publication_add_candidate(&txn, &sessions[0],
            "$new-a", NULL, &candidate) == 0,
            "add hashless registration A");
        candidate = owner_publication_candidate("hashless-b", 40);
        HASHLESS_CHECK(candidate, "hashless replacement candidate B");
        HASHLESS_CHECK(owner_publication_add_candidate(&txn, &sessions[1],
            "hashless-b", target[1], &candidate) == 0,
            "add hashless replacement B");
        candidate = owner_publication_candidate("$new-b", 50);
        HASHLESS_CHECK(candidate, "hashless registration candidate B");
        HASHLESS_CHECK(owner_publication_add_candidate(&txn, &sessions[1],
            "$new-b", NULL, &candidate) == 0,
            "add hashless registration B");
        HASHLESS_CHECK(!sessions[0].rel_hash_head
            && !sessions[1].rel_hash_head,
            "add leaves hashless registries unchanged");
        HASHLESS_CHECK(wl_columnar_eval_owner_publication_prepare(&txn) == 0
            && !sessions[0].rel_hash_head && !sessions[1].rel_hash_head,
            "prepare does not lazily publish session hashes");
        wl_columnar_eval_test_owner_publication_fail_registration(
            &sessions[1], "$new-b");
        HASHLESS_CHECK(wl_columnar_eval_owner_publication_register(&txn)
            == ENOMEM
            && wl_columnar_eval_test_owner_publication_registration_failure_hit()
            &&
            wl_columnar_eval_test_owner_publication_registration_failure_followed_image(),
            "later image preparation fault after first image");
        wl_columnar_eval_test_owner_publication_fail_registration(NULL, NULL);
        for (uint32_t i = 0; i < 2; i++) {
            HASHLESS_CHECK(sessions[i].rels == registry[i]
                && sessions[i].nrels == registry_counts[i]
                && memcmp(sessions[i].rels, registry_contents[i],
                sessions[i].rel_cap * sizeof(*registry_contents[i])) == 0
                && !sessions[i].rel_hash_head && !sessions[i].rel_hash_next
                && sessions[i].rel_hash_nbuckets == 0
                && sessions[i].rel_hash_chain_cap == 0
                && owner_publication_snapshot_matches(&before[i]),
                "failed preparation preserves hashless registry and target");
        }
        HASHLESS_CHECK(wl_columnar_eval_owner_publication_discard(&txn) == 0,
            "discard failed hashless transaction");
        for (uint32_t i = 0; i < 2; i++)
            HASHLESS_CHECK(sessions[i].rels == registry[i]
                && sessions[i].nrels == registry_counts[i]
                && memcmp(sessions[i].rels, registry_contents[i],
                sessions[i].rel_cap * sizeof(*registry_contents[i])) == 0
                && !sessions[i].rel_hash_head && !sessions[i].rel_hash_next
                && sessions[i].rel_hash_nbuckets == 0
                && sessions[i].rel_hash_chain_cap == 0,
                "discard preserves populated hashless session");
    }

    /* The same populated hashless shape succeeds, proving commit preflight
     * also avoids lookups that would mutate the old session hash. */
    {
        col_rel_t *candidate;
        wl_columnar_eval_owner_publication_init(&txn);
        candidate = owner_publication_candidate("hashless-a", 21);
        HASHLESS_CHECK(candidate && owner_publication_add_candidate(&txn,
            &sessions[0], "hashless-a", target[0], &candidate) == 0,
            "add hashless retry replacement A");
        candidate = owner_publication_candidate("$new-a", 31);
        HASHLESS_CHECK(candidate && owner_publication_add_candidate(&txn,
            &sessions[0], "$new-a", NULL, &candidate) == 0,
            "add hashless retry registration A");
        candidate = owner_publication_candidate("hashless-b", 41);
        HASHLESS_CHECK(candidate && owner_publication_add_candidate(&txn,
            &sessions[1], "hashless-b", target[1], &candidate) == 0,
            "add hashless retry replacement B");
        candidate = owner_publication_candidate("$new-b", 51);
        HASHLESS_CHECK(candidate && owner_publication_add_candidate(&txn,
            &sessions[1], "$new-b", NULL, &candidate) == 0,
            "add hashless retry registration B");
        HASHLESS_CHECK(wl_columnar_eval_owner_publication_prepare(&txn) == 0
            && !sessions[0].rel_hash_head && !sessions[1].rel_hash_head
            && wl_columnar_eval_owner_publication_register(&txn) == 0
            && wl_columnar_eval_owner_publication_commit(&txn) == 0,
            "commit hashless sessions atomically");
        HASHLESS_CHECK(target[0]->columns[0][0] == 21
            && target[1]->columns[0][0] == 41
            && sessions[0].nrels == 2 && sessions[1].nrels == 2
            && session_find_rel(&sessions[0], "$new-a")
            && session_find_rel(&sessions[1], "$new-b"),
            "hashless commit publishes relations and hash tables");
    }
    rc = 0;
cleanup:
    wl_columnar_eval_test_owner_publication_fail_registration(NULL, NULL);
    (void)wl_columnar_eval_owner_publication_discard(&txn);
    for (uint32_t i = 0; i < 2; i++)
        owner_publication_session_cleanup(&sessions[i]);
    if (failure)
        fprintf(stderr, "owner publication hashless fixture: %s\n", failure);
#undef HASHLESS_CHECK
    return rc == 0 ? 0 : 1;
}

static int
test_owner_publication_existing_targets(void)
{
    wl_col_session_t sessions[2] = { 0 };
    wl_columnar_memory_resolution_t resolution = { 0 };
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *low = NULL, *high = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    bool reader_held = false;
    wl_columnar_eval_owner_publication_txn_t txn;
    owner_publication_snapshot_t low_before = { 0 }, high_before = { 0 };
    col_rel_t **low_registry = NULL, **high_registry = NULL;
    uint32_t low_registry_count = 0, high_registry_count = 0;
    const char *failure = NULL;
    uint64_t budget_baseline = 0;
    int rc = EFAULT;

#define OWNER_CHECK(condition, message) \
        do { if (!(condition)) { failure = (message); goto cleanup; \
             } } while (0)
    wl_columnar_eval_owner_publication_init(&txn);
    resolution.budget_bytes = UINT64_C(1) << 20;
    resolution.usable_bytes = resolution.budget_bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    ref = wl_columnar_memory_governor_ref_create(&resolution);
    OWNER_CHECK(ref, "governor setup");
    for (uint32_t i = 0; i < 2; i++) {
        sessions[i].rel_cap = 4;
        sessions[i].rels = calloc(sessions[i].rel_cap,
                sizeof(*sessions[i].rels));
        OWNER_CHECK(sessions[i].rels, "session registry setup");
    }
    low = owner_publication_governed_target("owner-low", 10, ref);
    high = owner_publication_governed_target("owner-high", 20, ref);
    OWNER_CHECK(low && high && low->relation_identity < high->relation_identity
        && session_add_rel(&sessions[0], low) == 0
        && session_add_rel(&sessions[1], high) == 0,
        "existing target setup and canonical identity order");
    low_before = owner_publication_snapshot(low);
    high_before = owner_publication_snapshot(high);
    low_registry = sessions[0].rels;
    high_registry = sessions[1].rels;
    low_registry_count = sessions[0].nrels;
    high_registry_count = sessions[1].nrels;
    budget_baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));

    /* Add in reverse canonical order. A reader on the later owner must fail
     * preparation after the earlier writer was acquired, which discard must
     * release without changing either session registry or target. */
    {
        col_rel_t *candidate_high = owner_publication_candidate("owner-high",
                200);
        if (!candidate_high) {
            failure = "reader candidates";
            goto cleanup;
        }
        OWNER_CHECK(owner_publication_add_candidate(&txn, &sessions[1],
            "owner-high", high, &candidate_high) == 0,
            "add later target first");
        col_rel_t *candidate_low = owner_publication_candidate("owner-low",
                100);
        if (!candidate_low) {
            failure = "reader candidate allocation";
            goto cleanup;
        }
        OWNER_CHECK(owner_publication_add_candidate(&txn, &sessions[0],
            "owner-low", low, &candidate_low) == 0,
            "add earlier target second");
        OWNER_CHECK(col_rel_source_reader_acquire(high, &reader) == 0,
            "acquire reader on later owner");
        reader_held = true;
        OWNER_CHECK(wl_columnar_eval_owner_publication_prepare(&txn) == EBUSY
            && txn.count == 0
            && atomic_load_explicit(&low->source_access.state,
            memory_order_acquire) == 0
            && session_find_rel(&sessions[0], "owner-low") == low
            && session_find_rel(&sessions[1], "owner-high") == high
            && sessions[0].rels == low_registry
            && sessions[1].rels == high_registry
            && sessions[0].nrels == low_registry_count
            && sessions[1].nrels == high_registry_count
            && owner_publication_snapshot_matches(&low_before)
            && owner_publication_snapshot_matches(&high_before)
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == budget_baseline,
            "later reader refusal releases earlier writer without publication");
        OWNER_CHECK(col_rel_source_reader_release(&reader) == 0,
            "release later owner reader");
        reader_held = false;
        OWNER_CHECK(wl_columnar_eval_owner_publication_discard(&txn) == 0,
            "reader-failure transaction discard is idempotent");
    }

    /* Fail at the later existing-target prepare after the earlier target has
     * a private replacement and committed charge, then retry that same
     * logical two-target publication with fresh candidates. */
    {
        col_rel_t *candidate_high = owner_publication_large_candidate(
            "owner-high", 2000);
        if (!candidate_high) {
            failure = "fault candidates";
            goto cleanup;
        }
        OWNER_CHECK(owner_publication_add_candidate(&txn, &sessions[1],
            "owner-high", high, &candidate_high) == 0,
            "fault add later target first");
        col_rel_t *candidate_low = owner_publication_large_candidate(
            "owner-low", 1000);
        if (!candidate_low) {
            failure = "fault candidate allocation";
            goto cleanup;
        }
        OWNER_CHECK(owner_publication_add_candidate(&txn, &sessions[0],
            "owner-low", low, &candidate_low) == 0,
            "fault add earlier target second");
        wl_columnar_eval_test_owner_publication_fail_prepare(&sessions[1],
            "owner-high");
        OWNER_CHECK(wl_columnar_eval_owner_publication_prepare(&txn) == ENOMEM
            && wl_columnar_eval_test_owner_publication_prepare_failure_hit()
            &&
            wl_columnar_eval_test_owner_publication_prepare_failure_followed_prepared()
            && txn.count == 0
            && owner_publication_snapshot_matches(&low_before)
            && owner_publication_snapshot_matches(&high_before)
            && atomic_load_explicit(&low->source_access.state,
            memory_order_acquire) == 0
            && atomic_load_explicit(&high->source_access.state,
            memory_order_acquire) == 0
            && session_find_rel(&sessions[0], "owner-low") == low
            && session_find_rel(&sessions[1], "owner-high") == high
            && sessions[0].rels == low_registry
            && sessions[1].rels == high_registry
            && sessions[0].nrels == low_registry_count
            && sessions[1].nrels == high_registry_count
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == budget_baseline,
            "later prepare failure discards earlier replacement and charge");
        OWNER_CHECK(wl_columnar_eval_owner_publication_discard(&txn) == 0,
            "failed prepare leaves an idempotently empty transaction");
    }

    {
        col_rel_t *candidate_high = owner_publication_large_candidate(
            "owner-high", 3000);
        if (!candidate_high) {
            failure = "retry candidates";
            goto cleanup;
        }
        OWNER_CHECK(owner_publication_add_candidate(&txn, &sessions[1],
            "owner-high", high, &candidate_high) == 0,
            "retry add later target first");
        col_rel_t *candidate_low = owner_publication_large_candidate(
            "owner-low", 4000);
        if (!candidate_low) {
            failure = "retry candidate allocation";
            goto cleanup;
        }
        OWNER_CHECK(owner_publication_add_candidate(&txn, &sessions[0],
            "owner-low", low, &candidate_low) == 0,
            "retry add earlier target second");
        OWNER_CHECK(wl_columnar_eval_owner_publication_prepare(&txn) == 0
            && txn.entries[0].target == low && txn.entries[1].target == high
            && txn.entries[0].replacement_prepared
            && txn.entries[1].replacement_prepared
            && txn.entries[0].candidate == NULL
            && txn.entries[1].candidate == NULL
            && owner_publication_snapshot_matches(&low_before)
            && owner_publication_snapshot_matches(&high_before)
            && session_find_rel(&sessions[0], "owner-low") == low
            && session_find_rel(&sessions[1], "owner-high") == high
            && sessions[0].rels == low_registry
            && sessions[1].rels == high_registry
            && sessions[0].nrels == low_registry_count
            && sessions[1].nrels == high_registry_count
            && wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) > budget_baseline,
            "prepare sorts owners and keeps all publication private");
        OWNER_CHECK(wl_columnar_eval_owner_publication_register(&txn) == 0
            && wl_columnar_eval_owner_publication_commit(&txn) == 0
            && low->nrows == COL_REL_INIT_CAP + 1u
            && high->nrows == COL_REL_INIT_CAP + 1u
            && low->columns[0][0] == 4000
            && high->columns[0][0] == 3000
            && low->columns[0][low->nrows - 1] == 4000 + low->nrows - 1
            && high->columns[0][high->nrows - 1]
            == 3000 + high->nrows - 1
            && low->base_nrows == 1 && high->base_nrows == 1
            && low->sorted_nrows == low->nrows
            && high->sorted_nrows == high->nrows
            && low->run_count == 1 && high->run_count == 1
            && low->run_ends[0] == low->nrows
            && high->run_ends[0] == high->nrows
            && low->column_types[0] == WIRELOG_TYPE_INT64
            && high->column_types[0] == WIRELOG_TYPE_INT64,
            "retry commits both outputs and replacement metadata");
        for (uint32_t row = 0; row < low->nrows; row++) {
            if (low->columns[0][row] != 4000 + row
                || high->columns[0][row] != 3000 + row) {
                failure = "retry output row contents";
                goto cleanup;
            }
        }
        uint64_t live_charge = low->retained_reserved_bytes
            + high->retained_reserved_bytes;
        OWNER_CHECK(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) == live_charge
            && wl_columnar_eval_owner_publication_discard(&txn) == 0
            && wl_columnar_eval_owner_publication_discard(&txn) == 0
            && txn.entries == NULL && txn.count == 0 && txn.capacity == 0,
            "retry leaves exact live charges and repeated discard is safe");
    }
    rc = 0;
cleanup:
    wl_columnar_eval_test_owner_publication_fail_prepare(NULL, NULL);
    if (reader_held)
        (void)col_rel_source_reader_release(&reader);
    (void)wl_columnar_eval_owner_publication_discard(&txn);
    if (low && session_find_rel(&sessions[0], "owner-low") != low)
        col_rel_destroy(low);
    if (high && session_find_rel(&sessions[1], "owner-high") != high)
        col_rel_destroy(high);
    for (uint32_t i = 0; i < 2; i++)
        owner_publication_session_cleanup(&sessions[i]);
    low = high = NULL;
    if (ref) {
        if (wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(ref)) != 0) {
            failure = "target reservation teardown leaked governor charge";
            rc = EFAULT;
        }
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        fprintf(stderr, "owner publication fixture: %s\n", failure);
        return 1;
    }
    return rc == 0 ? 0 : 1;
#undef OWNER_CHECK
}

static int
test_owner_publication_transaction(void)
{
    wl_col_session_t sessions[3] = { 0 };
    wl_columnar_eval_owner_publication_txn_t txn;
    col_rel_t *target = NULL;
    col_rel_t *owner = NULL;
    col_rel_t *alias = NULL;
    int64_t initial = 1;
    int rc = EFAULT;

    wl_columnar_eval_owner_publication_init(&txn);

    for (uint32_t i = 0; i < 3; i++) {
        sessions[i].rel_cap = 8;
        sessions[i].rels = (col_rel_t **)calloc(sessions[i].rel_cap,
                sizeof(*sessions[i].rels));
        if (!sessions[i].rels)
            goto cleanup;
    }
    target = col_rel_new_auto("r", 1);
    if (!target || col_rel_append_row(target, &initial) != 0
        || session_add_rel(&sessions[0], target) != 0)
        goto cleanup;
    target = session_find_rel(&sessions[0], "r");

#ifdef WL_TEST_ALLOC_WRAP
    /* A failed growth keeps candidate ownership with the caller. */
    {
        col_rel_t *candidate = owner_publication_candidate("$d$add", 9);
        wl_columnar_eval_owner_publication_init(&txn);
        if (!candidate)
            goto cleanup;
        allocation_calls = 0;
        allocation_fail_at = 0;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$add", NULL, candidate) != ENOMEM) {
            allocation_fail_at = -1;
            col_rel_destroy(candidate);
            goto cleanup;
        }
        allocation_fail_at = -1;
        col_rel_destroy(candidate);
        wl_columnar_eval_owner_publication_discard(&txn);
    }
#endif

    /* A candidate may have only one transaction owner. */
    {
        col_rel_t *candidate = owner_publication_candidate("$d$duplicate", 11);
        wl_columnar_eval_owner_publication_init(&txn);
        if (!candidate
            || wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$duplicate", NULL, candidate) != 0) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$duplicate", NULL, candidate) != EEXIST) {
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        wl_columnar_eval_owner_publication_discard(&txn);
    }

    /* Duplicate canonical owners are rejected before either writer is held. */
    {
        col_rel_t *first = owner_publication_candidate("r", 2);
        col_rel_t *second = owner_publication_candidate("r", 3);
        wl_columnar_eval_owner_publication_init(&txn);
        if (!first || !second) {
            col_rel_destroy(first);
            col_rel_destroy(second);
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[0],
            "r", target, first) != 0) {
            col_rel_destroy(first);
            col_rel_destroy(second);
            goto cleanup;
        }
        first = NULL;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[0],
            "r", target, second) != 0) {
            col_rel_destroy(second);
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        second = NULL;
        if (wl_columnar_eval_owner_publication_prepare(&txn) != EBUSY
            || atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) != 0) {
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        wl_columnar_eval_owner_publication_discard(&txn);
    }

    /* A target must still be registered under the exact session/name pair. */
    {
        col_rel_t *candidate = owner_publication_candidate("r", 8);
        delta_pool_t *pool = NULL;
        col_rel_t *pool_candidate = NULL;

        wl_columnar_eval_owner_publication_init(&txn);
        if (!candidate) {
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "r", target, candidate) != EINVAL) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        col_rel_destroy(candidate);
        candidate = owner_publication_candidate("not-r", 9);
        if (!candidate) {
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[0],
            "not-r", target, candidate) != EINVAL) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        col_rel_destroy(candidate);

        candidate = owner_publication_candidate("$d$actual", 10);
        if (!candidate) {
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$expected", NULL, candidate) != EINVAL) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        col_rel_destroy(candidate);
        if (session_find_rel(&sessions[1], "$d$expected")
            || session_find_rel(&sessions[1], "$d$actual"))
            goto cleanup;

        candidate = col_rel_new_auto("$d$alias-candidate", 1);
        if (!candidate
            || col_rel_install_shared_view(candidate, target) != 0) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$alias-candidate", NULL, candidate) != EINVAL) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        col_rel_destroy(candidate);
        if (session_find_rel(&sessions[1], "$d$alias-candidate"))
            goto cleanup;

        pool = delta_pool_create(2, sizeof(col_rel_t), 256);
        pool_candidate = pool
            ? col_rel_pool_new_auto(pool, NULL, "$d$pool", 1) : NULL;
        int pool_rc = pool_candidate
            ? wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
                "$d$pool", NULL, pool_candidate) : EFAULT;
        if (!pool_candidate || pool_rc != EINVAL) {
            col_rel_free_contents(pool_candidate);
            delta_pool_destroy(pool);
            goto cleanup;
        }
        col_rel_free_contents(pool_candidate);
        delta_pool_destroy(pool);
        wl_columnar_eval_owner_publication_discard(&txn);
    }

    /* An alias is not a canonical publication target. */
    owner = col_rel_new_auto("owner", 1);
    alias = col_rel_new_auto("alias", 1);
    if (!owner || !alias || col_rel_install_shared_view(alias, owner) != 0
        || session_add_rel(&sessions[1], alias) != 0) {
        wl_columnar_eval_owner_publication_discard(&txn);
        goto cleanup;
    }
    {
        col_rel_t *candidate = owner_publication_candidate("alias", 4);
        wl_columnar_eval_owner_publication_init(&txn);
        if (!candidate) {
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "alias", alias, candidate) != 0) {
            col_rel_destroy(candidate);
            goto cleanup;
        }
        candidate = NULL;
        if (wl_columnar_eval_owner_publication_prepare(&txn) != EBUSY
            || atomic_load_explicit(&owner->source_access.state,
            memory_order_acquire) != 0) {
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        wl_columnar_eval_owner_publication_discard(&txn);
    }
    if (session_remove_rel(&sessions[1], "alias") != 0)
        goto cleanup;
    alias = NULL;
    col_rel_destroy(owner);
    owner = NULL;

    /* A preparation failure leaves the published target untouched. */
    {
        col_rel_t *bad = owner_publication_candidate("r", 5);
        wl_columnar_eval_owner_publication_init(&txn);
        if (!bad)
            goto cleanup;
        bad->nrows = bad->capacity + 1u;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[0], "r",
            target, bad) != 0) {
            col_rel_destroy(bad);
            goto cleanup;
        }
        bad = NULL;
        if (wl_columnar_eval_owner_publication_prepare(&txn) != EINVAL
            || target->nrows != 1 || target->columns[0][0] != initial
            || atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) != 0)
            goto cleanup;
    }

    /* A later private image failure leaves both earlier and later session
    * registries, hashes, and the existing target exactly as published. */
    {
        col_rel_t *replacement = owner_publication_candidate("r", 2);
        col_rel_t *delta0 = owner_publication_candidate("$d$r0", 6);
        col_rel_t *delta1 = owner_publication_candidate("$d$r1", 7);
        col_rel_t *keep1 = owner_publication_candidate("keep-one", 80);
        col_rel_t *hole = owner_publication_candidate("remove-hole", 81);
        col_rel_t **registry1;
        col_rel_t **registry2;
        uint32_t *hash1, *hash1_next, *hash2, *hash2_next;
        uint32_t nrels1, nrels2;
        uint32_t hash1_nbuckets, hash1_chain_cap, hash2_nbuckets,
            hash2_chain_cap;
        col_rel_t *registry1_contents[8] = { 0 };
        col_rel_t *registry2_contents[8] = { 0 };
        uint32_t hash1_heads[16] = { 0 }, hash1_next_contents[8] = { 0 };
        wl_columnar_eval_owner_publication_init(&txn);
        if (!replacement || !delta0 || !delta1 || !keep1 || !hole) {
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta1);
            col_rel_destroy(keep1);
            col_rel_destroy(hole);
            goto cleanup;
        }
        if (session_add_rel(&sessions[1], keep1) != 0) {
            col_rel_destroy(keep1);
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta1);
            col_rel_destroy(hole);
            goto cleanup;
        }
        keep1 = NULL;
        if (session_add_rel(&sessions[1], hole) != 0) {
            col_rel_destroy(hole);
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta1);
            goto cleanup;
        }
        hole = NULL;
        if (session_remove_rel(&sessions[1], "remove-hole") != 0)
            goto cleanup;
        if (session_find_rel(&sessions[1], "keep-one") == NULL
            || sessions[2].rel_hash_nbuckets != 0)
            goto cleanup;
        registry1 = sessions[1].rels;
        registry2 = sessions[2].rels;
        hash1 = sessions[1].rel_hash_head;
        hash1_next = sessions[1].rel_hash_next;
        hash2 = sessions[2].rel_hash_head;
        hash2_next = sessions[2].rel_hash_next;
        nrels1 = sessions[1].nrels;
        nrels2 = sessions[2].nrels;
        hash1_nbuckets = sessions[1].rel_hash_nbuckets;
        hash1_chain_cap = sessions[1].rel_hash_chain_cap;
        hash2_nbuckets = sessions[2].rel_hash_nbuckets;
        hash2_chain_cap = sessions[2].rel_hash_chain_cap;
        if (nrels1 > 8 || nrels2 > 8 || hash1_nbuckets > 16
            || hash1_chain_cap > 8)
            goto cleanup;
        memcpy(registry1_contents, sessions[1].rels,
            sessions[1].rel_cap * sizeof(*registry1_contents));
        memcpy(registry2_contents, sessions[2].rels,
            sessions[2].rel_cap * sizeof(*registry2_contents));
        memcpy(hash1_heads, sessions[1].rel_hash_head,
            hash1_nbuckets * sizeof(*hash1_heads));
        memcpy(hash1_next_contents, sessions[1].rel_hash_next,
            hash1_chain_cap * sizeof(*hash1_next_contents));
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[2],
            "$d$r1", NULL, delta1) != 0) {
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta1);
            goto cleanup;
        }
        delta1 = NULL;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[0],
            "r", target, replacement) != 0) {
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        replacement = NULL;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$r0", NULL, delta0) != 0) {
            col_rel_destroy(delta0);
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        delta0 = NULL;
        if (wl_columnar_eval_owner_publication_prepare(&txn) != 0)
            goto cleanup;
        wl_columnar_eval_test_owner_publication_fail_registration(
            &sessions[2], "$d$r1");
        if (wl_columnar_eval_owner_publication_register(&txn) != ENOMEM
            || !wl_columnar_eval_test_owner_publication_registration_failure_hit()
            || !
            wl_columnar_eval_test_owner_publication_registration_failure_followed_image())


            goto cleanup;
        wl_columnar_eval_test_owner_publication_fail_registration(NULL, NULL);
        if (sessions[1].rels != registry1 || sessions[2].rels != registry2
            || sessions[1].rel_hash_head != hash1
            || sessions[1].rel_hash_next != hash1_next
            || sessions[2].rel_hash_head != hash2
            || sessions[2].rel_hash_next != hash2_next
            || sessions[1].nrels != nrels1 || sessions[2].nrels != nrels2
            || sessions[1].rel_hash_nbuckets != hash1_nbuckets
            || sessions[1].rel_hash_chain_cap != hash1_chain_cap
            || sessions[2].rel_hash_nbuckets != hash2_nbuckets
            || sessions[2].rel_hash_chain_cap != hash2_chain_cap
            || memcmp(sessions[1].rels, registry1_contents,
            sessions[1].rel_cap * sizeof(*registry1_contents)) != 0
            || memcmp(sessions[2].rels, registry2_contents,
            sessions[2].rel_cap * sizeof(*registry2_contents)) != 0
            || memcmp(sessions[1].rel_hash_head, hash1_heads,
            hash1_nbuckets * sizeof(*hash1_heads)) != 0
            || memcmp(sessions[1].rel_hash_next, hash1_next_contents,
            hash1_chain_cap * sizeof(*hash1_next_contents)) != 0
            || session_find_rel(&sessions[1], "keep-one") == NULL
            || session_find_rel(&sessions[1], "$d$r0")
            || session_find_rel(&sessions[2], "$d$r1")
            || target->nrows != 1
            || atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) == 0)
            goto cleanup;
        if (wl_columnar_eval_owner_publication_discard(&txn) != 0
            || sessions[1].rels != registry1 || sessions[2].rels != registry2
            || sessions[1].rel_hash_head != hash1
            || sessions[1].rel_hash_next != hash1_next
            || sessions[2].rel_hash_head != hash2
            || sessions[2].rel_hash_next != hash2_next
            || memcmp(sessions[1].rels, registry1_contents,
            sessions[1].rel_cap * sizeof(*registry1_contents)) != 0
            || memcmp(sessions[1].rel_hash_head, hash1_heads,
            hash1_nbuckets * sizeof(*hash1_heads)) != 0
            || memcmp(sessions[1].rel_hash_next, hash1_next_contents,
            hash1_chain_cap * sizeof(*hash1_next_contents)) != 0)
            goto cleanup;
        if (session_find_rel(&sessions[1], "$d$r0")
            || session_find_rel(&sessions[2], "$d$r1")
            || target->nrows != 1
            || atomic_load_explicit(&target->source_access.state,
            memory_order_acquire) != 0)
            goto cleanup;
        wl_columnar_eval_owner_publication_discard(&txn);
    }

    /* The same shape succeeds after the failed transaction was discarded. */
    {
        col_rel_t *replacement = owner_publication_candidate("r", 2);
        col_rel_t *delta0 = owner_publication_candidate("$d$r0", 6);
        col_rel_t *delta1 = owner_publication_candidate("$d$r1", 7);
        col_rel_t *delta0_extra = owner_publication_candidate(
            "$d$r0-extra", 8);
        wl_columnar_eval_owner_publication_init(&txn);
        if (!replacement || !delta0 || !delta1 || !delta0_extra) {
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta1);
            col_rel_destroy(delta0_extra);
            goto cleanup;
        }
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[2],
            "$d$r1", NULL, delta1) != 0) {
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta1);
            col_rel_destroy(delta0_extra);
            goto cleanup;
        }
        delta1 = NULL;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[0],
            "r", target, replacement) != 0) {
            col_rel_destroy(replacement);
            col_rel_destroy(delta0);
            col_rel_destroy(delta0_extra);
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        replacement = NULL;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$r0", NULL, delta0) != 0) {
            col_rel_destroy(delta0);
            col_rel_destroy(delta0_extra);
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        delta0 = NULL;
        if (wl_columnar_eval_owner_publication_add(&txn, &sessions[1],
            "$d$r0-extra", NULL, delta0_extra) != 0) {
            col_rel_destroy(delta0_extra);
            wl_columnar_eval_owner_publication_discard(&txn);
            goto cleanup;
        }
        delta0_extra = NULL;
        if (wl_columnar_eval_owner_publication_prepare(&txn) != 0
            || wl_columnar_eval_owner_publication_register(&txn) != 0
            || wl_columnar_eval_owner_publication_commit(&txn) != 0
            || target->nrows != 1 || target->columns[0][0] != 2
            || !session_find_rel(&sessions[1], "$d$r0")
            || !session_find_rel(&sessions[2], "$d$r1")
            || !session_find_rel(&sessions[1], "keep-one")
            || !session_find_rel(&sessions[1], "$d$r0-extra")
            || sessions[1].nrels != 3 || sessions[2].nrels != 1
            || strcmp(sessions[1].rels[0]->name, "keep-one") != 0
            || strcmp(sessions[1].rels[1]->name, "$d$r0") != 0
            || strcmp(sessions[1].rels[2]->name, "$d$r0-extra") != 0)
            goto cleanup;
        if (wl_columnar_eval_owner_publication_discard(&txn) != 0
            || wl_columnar_eval_owner_publication_discard(&txn) != 0
            || txn.entries != NULL || txn.count != 0 || txn.capacity != 0)
            goto cleanup;
        rc = 0;
    }

cleanup:
    wl_columnar_eval_test_owner_publication_fail_registration(NULL, NULL);
    wl_columnar_eval_owner_publication_discard(&txn);
    if (alias)
        col_rel_destroy(alias);
    if (owner)
        col_rel_destroy(owner);
    for (uint32_t i = 0; i < 3; i++)
        owner_publication_session_cleanup(&sessions[i]);
    if (target)
        target = NULL;
    return rc;
}
#endif

#ifdef WL_TEST_BDX_SEED
extern int wl_columnar_eval_test_hybrid_init(const wl_plan_stratum_t *,
    wl_col_session_t *, uint32_t);
extern int wl_columnar_eval_test_hybrid_cleanup(wl_col_session_t *);
extern void (*wl_columnar_eval_test_hybrid_boundary)(unsigned, uint32_t);

#ifdef WL_TEST_ALLOC_WRAP
static unsigned hybrid_failure_phase;
static bool hybrid_failure_hit;
static void
hybrid_fail_later_worker(unsigned phase, uint32_t worker)
{
    if (phase == hybrid_failure_phase && worker == 1) {
        hybrid_failure_hit = true;
        allocation_calls = 0;
        allocation_fail_at = 0;
        wl_columnar_eval_test_hybrid_boundary = NULL;
    }
}
#endif

extern int wl_columnar_eval_test_initializer(unsigned, wl_col_session_t *,
    uint32_t);
extern void (*wl_columnar_eval_test_initializer_boundary)(unsigned, unsigned,
    uint32_t, wl_col_session_t *);

#ifdef WL_TEST_ALLOC_WRAP
static unsigned initializer_fault_mode, initializer_fault_phase;
static bool initializer_hold_worker, initializer_fault_hit,
    initializer_clone_fault;
static int initializer_reader_rc;
static wl_columnar_source_access_reader_t initializer_reader;
static col_rel_t *initializer_held;
static uint64_t initializer_held_identity;

static void
fail_initializer_later_worker(unsigned mode, unsigned phase, uint32_t worker,
    wl_col_session_t *coord)
{
    if (mode != initializer_fault_mode || phase != initializer_fault_phase
        || worker != 1)
        return;
    initializer_fault_hit = true;
    wl_columnar_eval_test_initializer_boundary = NULL;
    if (initializer_hold_worker) {
        initializer_held = session_find_rel(&coord->tdd_workers[0], "input");
        initializer_reader_rc = col_rel_source_reader_acquire(initializer_held,
                &initializer_reader);
        initializer_held_identity = initializer_held->relation_identity;
    }
    if (initializer_clone_fault) {
        allocation_fail_calloc_size = sizeof(col_arr_entry_t);
        allocation_failure_worker = &coord->tdd_workers[worker];
        allocation_failed_after_transfer = false;
    } else {
        allocation_calls = 0;
        allocation_fail_at = 0;
    }
}

static void
test_initializer_schema_parity(uint32_t workers, unsigned mode, bool typed)
{
    TEST("worker initializer preserves empty and populated schema");
    wl_plan_t plan = { 0 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL;
    const char *failure = NULL;
#define META_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    META_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "metadata session");
    wl_col_session_t *coord = (wl_col_session_t *)session;
    for (unsigned populated = 0; populated < 2; populated++) {
        unowned = col_rel_new_auto(populated ? "input" : "empty", 2);
        META_CHECK(unowned, "metadata input");
        wirelog_column_type_t types[] = { WIRELOG_TYPE_INT64,
                                          WIRELOG_TYPE_FLOAT };
        if (typed)
            META_CHECK(col_rel_set_column_types(unowned, types, 2) == 0,
                "metadata types");
        unowned->declared_ncols = 1;
        unowned->has_graph_column = true;
        unowned->graph_col_idx = 0;
        unowned->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
        unowned->compound_count = 1;
        unowned->compound_arity_map = malloc(sizeof(uint32_t));
        META_CHECK(unowned->compound_arity_map, "metadata map");
        unowned->compound_arity_map[0] = 2;
        META_CHECK(col_rel_enable_timestamps(unowned) == 0,
            "metadata timestamps");
        if (populated) {
            int64_t row[] = { 42, 0 };
            META_CHECK(col_rel_append_row(unowned, row) == 0, "metadata row");
            unowned->timestamps[0].iteration = 29;
            unowned->timestamps[0].multiplicity = -1;
        }
        META_CHECK(session_add_rel(coord, unowned) == 0,
            "metadata registration");
        unowned = NULL;
    }
    META_CHECK(wl_columnar_eval_test_initializer(mode, coord, workers) == 0,
        "metadata initialize");
    uint32_t total = 0;
    for (uint32_t w = 0; w < workers; w++) {
        for (unsigned populated = 0; populated < 2; populated++) {
            const char *name = populated ? "input" : "empty";
            col_rel_t *r = session_find_rel(&coord->tdd_workers[w], name);
            col_rel_t *source = session_find_rel(coord, name);
            META_CHECK(r && r->schema_ok && r->declared_ncols == 1
                && r->has_graph_column && r->graph_col_idx == 0
                && r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
                && r->compound_count == 1 && r->compound_arity_map
                && r->compound_arity_map != source->compound_arity_map
                && r->compound_arity_map[0] == 2
                && (r->column_types != NULL) == typed,
                "worker metadata differs");
            if (typed)
                META_CHECK(r->column_types[1] == WIRELOG_TYPE_FLOAT
                    && strcmp(r->schema.children[1]->format, "g") == 0,
                    "worker float schema");
            if (!populated)
                META_CHECK(r->nrows == 0 && r->timestamps,
                    "empty worker timestamp mode");
            total += r->nrows;
            for (uint32_t row = 0; row < r->nrows; row++)
                META_CHECK(r->columns[0][row] == 42 && r->timestamps
                    && r->timestamps[row].iteration == 29
                    && r->timestamps[row].multiplicity == -1,
                    "worker row provenance");
        }
    }
    META_CHECK(total == (mode == 0 ? 1 : workers), "worker exact row total");
    META_CHECK(wl_columnar_eval_test_hybrid_cleanup(coord) == 0,
        "metadata cleanup");
cleanup:
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef META_CHECK
}

static void
test_initializer_unwind(uint32_t workers, unsigned mode, unsigned phase,
    bool held, bool clone_failure)
{
    TEST(
        "initializer unwind: all caller slots, retained worker and exact retry");
    wl_plan_t plan = { 0 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL, *source = NULL, *empty = NULL;
    const char *failure = NULL;
    int64_t value = 42;
    initializer_fault_mode = mode;
    initializer_fault_phase = phase;
    initializer_hold_worker = held;
    initializer_clone_fault = clone_failure;
    initializer_fault_hit = false;
    initializer_reader_rc = EINVAL;
    initializer_held = NULL;
    memset(&initializer_reader, 0, sizeof(initializer_reader));
#define INIT_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    INIT_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *coord = (wl_col_session_t *)session;
    unowned = col_rel_new_auto("input", 1);
    INIT_CHECK(unowned, "input allocation");
    for (unsigned i = 0; i < 128; i++)
        INIT_CHECK(col_rel_append_row(unowned, &value) == 0, "input rows");
    INIT_CHECK(session_add_rel(coord, unowned) == 0, "input registration");
    source = unowned;
    unowned = NULL;
    unowned = col_rel_new_auto("empty", 1);
    INIT_CHECK(unowned && session_add_rel(coord, unowned) == 0,
        "empty relation");
    empty = unowned;
    unowned = NULL;
    if (clone_failure) {
        uint32_t key = 0;
        INIT_CHECK(col_session_get_arrangement(&coord->base, "input", &key, 1)
            && coord->arr_count == 1, "coordinator arrangement");
    }
    INIT_CHECK(wl_columnar_eval_test_initializer(mode, coord, workers) == 0
        && wl_columnar_eval_test_hybrid_cleanup(coord) == 0,
        "warm infrastructure");
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(coord->memory_governor);
    uint64_t reserved = wl_columnar_memory_reserved(governor);
    uint64_t source_view = source->view_generation;
    uint64_t source_storage = source->storage_generation;
    int64_t **source_columns = source->columns;
    wl_columnar_eval_test_initializer_boundary = fail_initializer_later_worker;
    int rc = wl_columnar_eval_test_initializer(mode, coord, workers);
    allocation_fail_at = -1;
    allocation_fail_calloc_size = 0;
    allocation_failure_worker = NULL;
    wl_columnar_eval_test_initializer_boundary = NULL;
    INIT_CHECK(initializer_fault_hit && rc == (held ? EBUSY : ENOMEM),
        "real late allocation failure not propagated");
    if (clone_failure)
        INIT_CHECK(allocation_failed_after_transfer,
            "clone failed before transfer");
    INIT_CHECK(source->nrows == 128 && source->columns == source_columns
        && source->view_generation == source_view
        && source->storage_generation == source_storage && empty->nrows == 0
        && col_rel_get(source, 127, 0) == 42,
        "coordinator source mutated");
    if (held) {
        INIT_CHECK(initializer_reader_rc == 0 && coord->tdd_workers_count == 2
            && session_find_rel(&coord->tdd_workers[0],
            "input") == initializer_held
            && initializer_held->relation_identity == initializer_held_identity,
            "refused worker ownership lost");
        INIT_CHECK(wl_columnar_eval_test_hybrid_cleanup(coord) == EBUSY,
            "repeated teardown ignored reader");
        INIT_CHECK(col_rel_source_reader_release(&initializer_reader) == 0,
            "release worker reader");
        INIT_CHECK(wl_columnar_eval_test_hybrid_cleanup(coord) == 0,
            "released cohort teardown");
    }
    INIT_CHECK(coord->tdd_workers_count == 0
        && wl_columnar_memory_reserved(governor) == reserved
        && col_rel_storage_alias_borrow_count(source) == 0
        && col_rel_storage_alias_borrow_count(empty) == 0
        && atomic_load_explicit(&source->source_access.state,
        memory_order_seq_cst) == 0
        && atomic_load_explicit(&empty->source_access.state,
        memory_order_seq_cst) == 0,
        "failure leaked allocation credit or source leases");
    INIT_CHECK(wl_columnar_eval_test_initializer(mode, coord, workers) == 0
        && coord->tdd_workers_count == workers, "initializer retry");
    uint32_t total = 0;
    for (uint32_t w = 0; w < workers; w++) {
        col_rel_t *r = session_find_rel(&coord->tdd_workers[w], "input");
        col_rel_t *e = session_find_rel(&coord->tdd_workers[w], "empty");
        INIT_CHECK(r && e && r->ncols == 1 && e->ncols == 1 && e->nrows == 0,
            "retry worker relations");
        total += r->nrows;
        for (uint32_t row = 0; row < r->nrows; row++)
            INIT_CHECK(col_rel_get(r, row, 0) == 42, "exact retry rows");
        if (mode == 2)
            INIT_CHECK(r->storage_owner == source && e->storage_owner == empty,
                "global-read retry source leases");
        else
            INIT_CHECK(r->storage_owner == r, "private retry relation");
    }
    INIT_CHECK(total == (mode == 0 ? 128u : 128u * workers),
        "exact retry count");
    INIT_CHECK(wl_columnar_eval_test_hybrid_cleanup(coord) == 0
        && wl_columnar_memory_reserved(governor) == reserved
        && col_rel_storage_alias_borrow_count(source) == 0
        && col_rel_storage_alias_borrow_count(empty) == 0,
        "retry teardown accounting");
cleanup:
    wl_columnar_eval_test_initializer_boundary = NULL;
    allocation_fail_at = -1;
    allocation_fail_calloc_size = 0;
    allocation_failure_worker = NULL;
    if (initializer_reader.owner)
        (void)col_rel_source_reader_release(&initializer_reader);
    if (session)
        (void)wl_columnar_eval_test_hybrid_cleanup((wl_col_session_t *)session);
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef INIT_CHECK
}
#endif

static void
test_hybrid_empty_idb_ownership(unsigned fault)
{
    TEST("hybrid empty IDB: private metadata, partial failure and exact retry");
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output" };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *output_rel = NULL;
    const char *failure = NULL;
#define HYBRID_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    HYBRID_CHECK(wl_session_create(wl_backend_columnar(), &plan, 2,
        &session) == 0, "create");
    wl_col_session_t *sess = (wl_col_session_t *)session;
    int64_t value = 42;
    HYBRID_CHECK(wl_session_insert(session, "input", &value, 1, 1) == 0,
        "insert");
    col_rel_t *input = session_find_rel(sess, "input");
    output_rel = col_rel_new_auto("output", 2);
    HYBRID_CHECK(output_rel, "output allocation");
    output_rel->column_types = malloc(2 * sizeof(*output_rel->column_types));
    output_rel->compound_arity_map = malloc(sizeof(uint32_t));
    HYBRID_CHECK(output_rel->column_types && output_rel->compound_arity_map,
        "metadata allocation");
    output_rel->column_types[0] = WIRELOG_TYPE_INT64;
    output_rel->column_types[1] = WIRELOG_TYPE_FLOAT;
    output_rel->compound_arity_map[0] = 2;
    output_rel->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    output_rel->compound_count = 1;
    output_rel->inline_physical_offset = 0;
    output_rel->declared_ncols = 1;
    output_rel->has_graph_column = true;
    output_rel->graph_col_idx = 1;
    HYBRID_CHECK(col_rel_enable_timestamps(output_rel) == 0, "timestamps");
    HYBRID_CHECK(session_add_rel(sess, output_rel) == 0, "register output");
    col_rel_t *source = output_rel;
    output_rel = NULL;
    /* Warm coordinator infrastructure before comparing reservation totals. */
    HYBRID_CHECK(wl_columnar_eval_test_hybrid_init(&stratum, sess, 2) == 0,
        "warm initialization");
    HYBRID_CHECK(wl_columnar_eval_test_hybrid_cleanup(sess) == 0,
        "warm cleanup");
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(sess->memory_governor);
    uint64_t baseline = wl_columnar_memory_reserved(governor);
#ifdef WL_TEST_ALLOC_WRAP
    if (fault > 0) {
        hybrid_failure_phase = fault - 1;
        hybrid_failure_hit = false;
        wl_columnar_eval_test_hybrid_boundary = hybrid_fail_later_worker;
        int rc = wl_columnar_eval_test_hybrid_init(&stratum, sess, 2);
        allocation_fail_at = -1;
        wl_columnar_eval_test_hybrid_boundary = NULL;
        HYBRID_CHECK(rc == ENOMEM && hybrid_failure_hit
            && sess->tdd_workers_count == 0,
            "later worker allocation failure");
        HYBRID_CHECK(wl_columnar_memory_reserved(governor) == baseline
            && col_rel_storage_alias_borrow_count(source) == 0
            && col_rel_storage_alias_borrow_count(input) == 0
            && atomic_load_explicit(&source->source_access.state,
            memory_order_seq_cst) == 0
            && atomic_load_explicit(&input->source_access.state,
            memory_order_seq_cst) == 0,
            "failure leaked reservations or source leases");
    }
#else
    (void)fault;
#endif
    HYBRID_CHECK(wl_columnar_eval_test_hybrid_init(&stratum, sess, 2) == 0,
        "retry initialization");
    HYBRID_CHECK(sess->tdd_workers_count == 2
        && col_rel_storage_alias_borrow_count(source) == 0
        && atomic_load_explicit(&source->source_access.state,
        memory_order_seq_cst) == 0
        && col_rel_storage_alias_borrow_count(input) == 2,
        "IDB private ownership or EDB sharing");
    for (uint32_t worker = 0; worker < 2; worker++) {
        col_rel_t *r = session_find_rel(&sess->tdd_workers[worker], "output");
        col_rel_t *root = NULL;
        HYBRID_CHECK(r && col_rel_storage_owner_resolve(r, &root) == 0
            && root == r && r != source && r->nrows == 0
            && r->columns != source->columns
            && r->memory_governor == sess->memory_governor
            && r->dedup_slots && r->timestamps
            && r->ncols == 2 && r->declared_ncols == 1
            && r->column_types[0] == WIRELOG_TYPE_INT64
            && r->column_types[1] == WIRELOG_TYPE_FLOAT
            && strcmp(r->col_names[1], source->col_names[1]) == 0
            && r->has_graph_column && r->graph_col_idx == 1
            && r->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && r->compound_count == 1 && r->inline_physical_offset == 0
            && r->compound_arity_map != source->compound_arity_map
            && r->compound_arity_map[0] == 2,
            "private worker metadata");
    }
    HYBRID_CHECK(wl_columnar_eval_test_hybrid_cleanup(sess) == 0
        && wl_columnar_memory_reserved(governor) == baseline
        && col_rel_storage_alias_borrow_count(input) == 0,
        "retry cleanup accounting");
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    allocation_fail_at = -1;
#endif
    wl_columnar_eval_test_hybrid_boundary = NULL;
    col_rel_destroy(output_rel);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef HYBRID_CHECK
}
#endif

static void
collect_empty_idb_result(const char *relation, const int64_t *row,
    uint32_t ncols, void *data)
{
    unsigned *counts = data;
    counts[0]++;
    if (strcmp(relation, "output") == 0 && ncols == 1 && row[0] == 42)
        counts[1]++;
}

static void
test_tdd_existing_empty_idb(uint32_t workers, bool held, bool public_call)
{
    TEST(
        "TDD existing empty IDB: actual workers, checked refusal and exact rows");
    uint32_t key = 0;
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = 2 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unregistered = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    int64_t *values = NULL;
    const char *failure = NULL;
    const uint32_t count = 65536;
#define EMPTY_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    EMPTY_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = (wl_col_session_t *)session;
    values = malloc((size_t)count * sizeof(*values));
    EMPTY_CHECK(values, "allocate input");
    for (uint32_t row = 0; row < count; row++)
        values[row] = 42;
    EMPTY_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "insert");
    unregistered = col_rel_new_auto("output", 1);
    EMPTY_CHECK(unregistered, "allocate empty output");
    unregistered->column_types = malloc(sizeof(*unregistered->column_types));
    EMPTY_CHECK(unregistered->column_types, "allocate type");
    unregistered->column_types[0] = WIRELOG_TYPE_INT64;
    EMPTY_CHECK(session_add_rel(sess, unregistered) == 0, "register output");
    col_rel_t *result = unregistered;
    unregistered = NULL;
    sess->tdd_audit.enabled = true;
    if (held) {
        EMPTY_CHECK(col_rel_source_reader_acquire(result, &reader) == 0,
            "reader");
        EMPTY_CHECK(col_eval_stratum_tdd(&stratum, sess, 0) == EBUSY
            && result->nrows == 0 && sess->tdd_workers_count == 0,
            "external reader did not safely refuse");
        EMPTY_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
    }
    unsigned counts[2] = { 0, 0 };
    int rc = public_call
        ? wl_session_snapshot(session, collect_empty_idb_result, counts)
        : col_eval_stratum_tdd(&stratum, sess, 0);
    EMPTY_CHECK(rc == 0, "TDD empty output evaluation");
    EMPTY_CHECK((public_call
        ? sess->tdd_executed_strata == 1 && sess->tdd_workers_cap >= workers
        && counts[0] == 1 && counts[1] == 1
        : sess->tdd_audit.selected_workers == workers
        && sess->tdd_audit.submitted_tasks >= workers)
        && sess->tdd_workers_count == 0 && result->nrows == 1
        && col_rel_get(result, 0, 0) == 42
        && col_rel_storage_alias_borrow_count(result) == 0,
        "actual worker execution or exact output");
cleanup:
    if (reader.owner)
        (void)col_rel_source_reader_release(&reader);
    free(values);
    col_rel_destroy(unregistered);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef EMPTY_CHECK
}

static void
test_tdd_worker_segments(uint32_t workers, unsigned mode, bool ordinary)
{
    TEST("TDD worker CONCAT: actual dispatch, empty result and error retry");
    uint32_t key = 0;
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, mode == 1 ? 0 : 1 };
    wl_plan_op_exchange_t exchange = { .num_workers = 1,
                                       .key_col_idxs = &key,
                                       .key_col_count = 1 };
    const char *keys[] = { "col0" };
    uint32_t projection[] = { 0 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "missing" }
    };
    if (ordinary) {
        ops[0].relation_name = "output";
        ops[1] = (wl_plan_op_t) { .op = WL_PLAN_OP_JOIN,
                                  .right_relation = "output", .left_keys = keys,
                                  .right_keys = keys,
                                  .key_count = 1, .project_indices = projection,
                                  .project_count = 1 };
    }
    wl_plan_relation_t output = { .name = "output", .delta_name = "$d$output",
                                  .ops = ops, .op_count = mode == 2 ? 7 : 6 };
    wl_plan_stratum_t stratum = { .relations = &output, .relation_count = 1,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    int64_t *values = NULL;
    col_rel_t *unregistered = NULL;
    const char *failure = NULL;
    const uint32_t count = 65536;
#define WORKER_SEG_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    WORKER_SEG_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = (wl_col_session_t *)session;
    values = malloc((size_t)count * sizeof(*values));
    WORKER_SEG_CHECK(values, "input allocation");
    for (uint32_t row = 0; row < count; row++)
        values[row] = 42;
    WORKER_SEG_CHECK(wl_session_insert(session, "input", values, count, 1) == 0,
        "insert");
    /* Union an aligned IDB self-join with filtered input to exercise ordinary
     * publication; the other fixture uses outbound owner publication. */
    if (ordinary) {
        unregistered = col_rel_new_auto("output", 1);
        WORKER_SEG_CHECK(unregistered, "empty IDB allocation");
        unregistered->column_types =
            malloc(sizeof(*unregistered->column_types));
        WORKER_SEG_CHECK(unregistered->column_types, "empty IDB type");
        unregistered->column_types[0] = WIRELOG_TYPE_INT64;
        WORKER_SEG_CHECK(session_add_rel(sess, unregistered) == 0, "empty IDB");
        unregistered = NULL;
    }
    sess->tdd_audit.enabled = true;
    if (mode == 2) {
        WORKER_SEG_CHECK(col_eval_stratum_tdd(&stratum, sess, 0) == ENOENT
            && sess->tdd_audit.selected_workers == workers
            && sess->tdd_audit.submitted_tasks >= workers
            && sess->tdd_workers_count == 0, "actual worker error");
        output.op_count = 6;
    }
    int eval_rc = col_eval_stratum_tdd(&stratum, sess, 0);
    WORKER_SEG_CHECK(eval_rc == 0, "worker evaluation/retry");
    col_rel_t *result = session_find_rel(sess, "output");
    WORKER_SEG_CHECK(sess->tdd_audit.selected_workers == workers
        && sess->tdd_audit.submitted_tasks >= workers
        && strcmp(sess->tdd_audit.strategy, ordinary ? "aligned" : "owner") == 0
        && sess->tdd_workers_count == 0
        && result && result->nrows == (mode == 1 ? 0u : 1u),
        "actual outbound workers or exact row count");
    if (mode != 1)
        WORKER_SEG_CHECK(col_rel_get(result, 0, 0) == 42, "exact row");
cleanup:
    col_rel_destroy(unregistered);
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef WORKER_SEG_CHECK
}

extern int wl_columnar_eval_test_retire_prior_deltas(const wl_plan_stratum_t *,
    wl_col_session_t *, uint32_t);
extern void (*wl_columnar_eval_test_subpass_boundary)(wl_col_session_t *,
    uint32_t,
    bool);

extern int wl_columnar_eval_test_global_exchange(const wl_plan_stratum_t *,
    wl_col_session_t *, col_eval_tdd_worker_ctx_t *, uint32_t);

static void
test_global_exchange_metadata(uint32_t workers, bool schema_less)
{
    TEST(
        "global exchange: full metadata adoption and timestamp correspondence");
    wl_plan_relation_t relation = { .name = "output",
                                    .delta_name = "$d$output" };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    wl_session_t *session = NULL;
    col_eval_tdd_worker_ctx_t *ctxs = NULL;
    col_rel_t *prototype = NULL, *unowned = NULL;
    wl_columnar_memory_governor_ref_t *governor = NULL;
    const char *failure = NULL;
#define GLOBAL_META_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    GLOBAL_META_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *coord = (wl_col_session_t *)session;
    governor = coord->memory_governor;
    wl_columnar_memory_governor_ref_retain(governor);
    prototype = col_rel_new_auto("prototype", 2);
    GLOBAL_META_CHECK(prototype, "prototype");
    GLOBAL_META_CHECK(col_rel_attach_memory_governor(prototype, governor) == 0,
        "attach governor");
    wirelog_column_type_t types[] = { WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64 };
    GLOBAL_META_CHECK(col_rel_set_column_types(prototype, types, 2) == 0,
        "types");
    prototype->compound_arity_map = malloc(sizeof(uint32_t));
    GLOBAL_META_CHECK(prototype->compound_arity_map, "compound map");
    prototype->compound_arity_map[0] = 2;
    prototype->compound_kind = WIRELOG_COMPOUND_KIND_INLINE;
    prototype->compound_count = 1;
    prototype->inline_physical_offset = 0;
    prototype->declared_ncols = 1;
    prototype->has_graph_column = true;
    prototype->graph_col_idx = 1;
    GLOBAL_META_CHECK(col_rel_enable_timestamps(prototype) == 0,
        "timestamp mode");
    if (schema_less)
        GLOBAL_META_CHECK(col_rel_alloc(&unowned, "output") == 0, "schemaless");
    else unowned = wl_columnar_relation_new_like_governed("output", prototype,
                governor);
    GLOBAL_META_CHECK(unowned && session_add_rel(coord, unowned) == 0,
        "target");
    col_rel_t *target = unowned;
    unowned = NULL;
    GLOBAL_META_CHECK(wl_columnar_eval_test_initializer(2, coord, workers) == 0,
        "global workers");
    ctxs = calloc(workers, sizeof(*ctxs));
    GLOBAL_META_CHECK(ctxs, "contexts");
    for (uint32_t w = 0; w < workers; w++) {
        ctxs[w].delta_rels = calloc(1, sizeof(col_rel_t *));
        GLOBAL_META_CHECK(ctxs[w].delta_rels, "matrix");
        col_rel_t *delta = wl_columnar_relation_new_like_governed("$d$output",
                prototype, governor);
        ctxs[w].delta_rels[0] = delta;
        GLOBAL_META_CHECK(delta, "payload");
        int64_t rows[][2] = { { 42, 42 }, { 7, 7 }, { 42, 42 } };
        for (unsigned row = 0; row < 3; row++) {
            GLOBAL_META_CHECK(col_rel_append_row(delta, rows[row]) == 0, "row");
            delta->timestamps[row] = (col_delta_timestamp_t){ .iteration = row +
                                                                  10,
                                                              .worker = w,
                                                              .stratum = 3,
                                                              .multiplicity =
                                                                  row + 1 };
        }
    }
    GLOBAL_META_CHECK(wl_columnar_eval_test_global_exchange(&stratum, coord,
        ctxs, workers) == 0, "metadata exchange");
    GLOBAL_META_CHECK(target == session_find_rel(coord, "output")
        && target->nrows == 2 && target->ncols == 2 &&
        target->declared_ncols == 1
        && target->column_types && target->column_types[0] == WIRELOG_TYPE_INT64
        && target->has_graph_column && target->graph_col_idx == 1
        && target->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
        && target->compound_count == 1 && target->compound_arity_map
        && target->compound_arity_map[0] == 2 && target->timestamps
        && col_rel_get(target, 0, 0) == 7 && col_rel_get(target, 1, 0) == 42
        && target->timestamps[0].iteration == 11
        && target->timestamps[0].multiplicity == 2
        && target->timestamps[1].iteration == 10
        && target->timestamps[1].multiplicity == 1,
        "metadata adoption or timestamp correspondence");
    for (uint32_t w = 0; w < workers; w++) {
        col_rel_t *view = session_find_rel(&coord->tdd_workers[w], "output");
        GLOBAL_META_CHECK(ctxs[w].delta_rels[0] == NULL && view &&
            view->nrows == 2
            && view->declared_ncols == 1 && view->has_graph_column
            && view->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            && view->compound_arity_map && view->compound_arity_map[0] == 2
            && view->timestamps && view->timestamps[0].iteration == 11,
            "refreshed worker metadata");
    }
cleanup:
    if (ctxs) {
        for (uint32_t w = 0; w < workers; w++) {
            if (ctxs[w].delta_rels) col_rel_destroy(ctxs[w].delta_rels[0]);
            free(ctxs[w].delta_rels);
        }
        free(ctxs);
    }
    col_rel_destroy(unowned);
    col_rel_destroy(prototype);
    wl_session_destroy(session);
    if (governor) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                governor)) != 0
            && !failure) failure = "metadata reservation leak";
        wl_columnar_memory_governor_ref_release(governor);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef GLOBAL_META_CHECK
}

static void
test_tdd_prior_delta_retirement(uint32_t workers, unsigned mode)
{
    TEST("prior TDD delta retirement: checked aliases, roots and preparation");
    bool schema_less = mode >= 7;
    if (schema_less)
        mode = mode == 7 ? 0 : mode == 8 ? 1 : 6;
    wl_plan_relation_t relation = { .name = "output",
                                    .delta_name = "$d$output" };
    wl_plan_stratum_t stratum = { .relations = &relation, .relation_count = 1 };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    const char *failure = NULL;
#define RETIRE_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    RETIRE_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(sess->memory_governor);
    RETIRE_CHECK(wl_columnar_session_ensure_tdd_worker_slots(sess,
        workers) == 0,
        "worker slots");
    uint64_t baseline = wl_columnar_memory_reserved(budget);
    for (uint32_t w = 0; w < workers; w++) {
        sess->tdd_workers_count = w + 1;
        RETIRE_CHECK(col_worker_session_create(sess, w, NULL, 0,
            &sess->tdd_workers[w]) == 0, "worker create");
    }
    int64_t value = 42;
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    unowned = col_rel_new_auto("$d$output", 1);
    RETIRE_CHECK(unowned && (schema_less || col_rel_set_column_types(unowned,
        &type, 1) == 0)
        && col_rel_append_row(unowned, &value) == 0
        && col_rel_enable_timestamps(unowned) == 0, "root");
    unowned->timestamps[0] = (col_delta_timestamp_t){ .iteration = 7,
                                                      .stratum = 2,
                                                      .multiplicity = -3 };
    col_rel_t *root = unowned;
    RETIRE_CHECK(session_add_rel(&sess->tdd_workers[0], unowned) == 0,
        "register root");
    unowned = NULL;
    for (uint32_t w = 1; w < workers; w++) {
        unowned = col_rel_new_auto("$d$output", 1);
        RETIRE_CHECK(unowned && col_rel_install_shared_view(unowned, root) == 0,
            "shared alias");
        col_rel_t *alias = unowned;
        RETIRE_CHECK(session_add_rel(&sess->tdd_workers[w], alias) == 0,
            "register alias");
        unowned = NULL;
        RETIRE_CHECK(wl_columnar_session_adopt_shared_view(
                &sess->tdd_workers[w],
                alias) == 0, "adopt lease");
    }
    col_rel_t *prior[8];
    uint64_t identities[8], generations[8];
    for (uint32_t w = 0; w < workers; w++) {
        prior[w] = session_find_rel(&sess->tdd_workers[w], "$d$output");
        /* Match legacy pool-derived registrations without an Arrow schema,
         * releasing this fixture's constructor-owned schema first. */
        if (schema_less) {
            ArrowSchemaRelease(&prior[w]->schema);
            memset(&prior[w]->schema, 0, sizeof(prior[w]->schema));
            prior[w]->schema_ok = false;
        }
        identities[w] = prior[w]->relation_identity;
        generations[w] = prior[w]->view_generation;
    }
    uint64_t prepared_baseline = wl_columnar_memory_reserved(budget);
    if (mode == 1 || mode == 2)
        RETIRE_CHECK(col_rel_source_reader_acquire(prior[mode == 1 ? 0 : 1],
            &reader) == 0, "held prior reader");
    if (mode == 3) {
        unowned = col_rel_new_auto("held", 1);
        RETIRE_CHECK(unowned && col_rel_append_row(unowned, &value) == 0
            && col_rel_source_reader_acquire(unowned, &reader) == 0
            && wl_columnar_eval_stack_cleanup_begin(&sess->tdd_workers[1],
            &frame) == 0, "pending frame");
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
        result->rel = unowned;
        result->owned = true;
        unowned = NULL;
        RETIRE_CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY,
            "retain frame");
    }
    uint64_t saved_limit = atomic_load_explicit(&budget->usable_bytes,
            memory_order_relaxed);
    wl_columnar_memory_mode_t saved_mode = budget->mode;
    if (mode == 5) {
        budget->mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
        atomic_store_explicit(&budget->usable_bytes,
            wl_columnar_memory_reserved(budget), memory_order_relaxed);
    }
#ifdef WL_TEST_ALLOC_WRAP
    if (mode == 4 || mode == 6)
        allocation_fail_at = mode == 4 ? 0 : 8;
    allocation_calls = 0;
#endif
    int rc = wl_columnar_eval_test_retire_prior_deltas(&stratum, sess, workers);
#ifdef WL_TEST_ALLOC_WRAP
    allocation_fail_at = -1;
#endif
    budget->mode = saved_mode;
    atomic_store_explicit(&budget->usable_bytes, saved_limit,
        memory_order_relaxed);
    if (mode != 0) {
        int expected = mode <= 3 ? EBUSY : mode == 5 ? ENOSPC : ENOMEM;
        RETIRE_CHECK(rc == expected, "retirement failure code");
        uint32_t stable_to = mode == 1 ? 1 : workers;
        for (uint32_t w = 0; w < stable_to; w++) {
            col_rel_t *current = session_find_rel(&sess->tdd_workers[w],
                    "$d$output");
            RETIRE_CHECK(current == prior[w] &&
                current->relation_identity == identities[w]
                && current->view_generation == generations[w] &&
                current->nrows == 1
                && col_rel_get(current, 0, 0) == value && current->timestamps
                && current->timestamps[0].multiplicity == -3,
                "refusal mutated retained registration");
        }
        if (mode == 1) {
            for (uint32_t w = 1; w < workers; w++) {
                col_rel_t *current = session_find_rel(&sess->tdd_workers[w],
                        "$d$output");
                RETIRE_CHECK(current->relation_identity != identities[w]
                    && current->nrows == 0 && !current->col_shared,
                    "alias prefix was not independently retired");
            }
        }
        if (mode >= 4)
            RETIRE_CHECK(wl_columnar_memory_reserved(budget) ==
                prepared_baseline,
                "preparation leaked admission");
        if (reader.owner) {
            RETIRE_CHECK(wl_columnar_eval_test_retire_prior_deltas(&stratum,
                sess, workers) == EBUSY, "repeated refusal");
            RETIRE_CHECK(col_rel_source_reader_release(&reader) == 0,
                "release");
        }
        if (mode == 3)
            RETIRE_CHECK(wl_columnar_eval_stack_cleanup_retry(
                    &sess->tdd_workers[1]) == 0,
                "drain frame");
        rc = wl_columnar_eval_test_retire_prior_deltas(&stratum, sess, workers);
    }
    RETIRE_CHECK(rc == 0, "retirement retry");
    for (uint32_t w = 0; w < workers; w++) {
        col_rel_t *current = session_find_rel(&sess->tdd_workers[w],
                "$d$output");
        RETIRE_CHECK(current && current->nrows == 0 && current->ncols == 1
            && (schema_less ? current->column_types == NULL
                : current->column_types &&
            current->column_types[0] == WIRELOG_TYPE_INT64)
            && !current->col_shared && current->schema_ok == !schema_less,
            "empty registration lost metadata or independence");
        unowned = col_rel_new_auto("output", 1);
        RETIRE_CHECK(unowned && col_rel_append_row(unowned, &value) == 0
            && session_add_rel(&sess->tdd_workers[w], unowned) == 0,
            "full relation for outbound AUTO");
        unowned = NULL;
        sess->tdd_workers[w].tdd_subpass_active = true;
        sess->tdd_workers[w].tdd_outbound_only_active = true;
        sess->tdd_workers[w].current_iteration = 1;
        wl_plan_op_t variable = { .op = WL_PLAN_OP_VARIABLE,
                                  .relation_name = "output",
                                  .delta_mode = WL_DELTA_AUTO };
        eval_stack_t stack;
        eval_stack_init(&stack);
        RETIRE_CHECK(col_op_variable(&variable, &stack,
            &sess->tdd_workers[w]) == 0
            && stack.top == 1 && stack.items[0].rel == current
            && stack.items[0].rel->nrows == 0 && eval_stack_drain(&stack) == 0,
            "outbound AUTO replayed full input after empty retirement");
        RETIRE_CHECK(col_worker_session_destroy(&sess->tdd_workers[w]) == 0,
            "checked worker teardown");
    }
    sess->tdd_workers_count = 0;
    RETIRE_CHECK(wl_columnar_memory_reserved(budget) == baseline,
        "retirement leaked worker reservation");
cleanup:
#ifdef WL_TEST_ALLOC_WRAP
    allocation_fail_at = -1;
#endif
    if (reader.owner)
        (void)col_rel_source_reader_release(&reader);
    if (frame)
        (void)wl_columnar_eval_stack_cleanup_finish(&frame);
    col_rel_destroy(unowned);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef RETIRE_CHECK
}

static unsigned retire_dispatch_mode;
static uint32_t retire_dispatch_workers, retire_dispatch_rounds;
static bool retire_dispatch_captured, retire_dispatch_verified;
static int retire_dispatch_rc;
static col_rel_t *retire_dispatch_root, *retire_dispatch_alias;
static uint64_t retire_dispatch_view, retire_dispatch_storage;
static col_delta_timestamp_t *retire_dispatch_timestamps;
static wl_columnar_source_access_reader_t retire_dispatch_reader;
static wl_columnar_source_access_reader_t retire_dispatch_frame_reader;

/* Both boundaries run on the coordinator, before submit or after wait_all.
 * Hook configuration remains fixed while workers run. */
static void
hold_prior_delta_across_dispatch(wl_col_session_t *coord, uint32_t iteration,
    bool before)
{
    if (!before)
        retire_dispatch_rounds++;
    if (iteration != 1)
        return;
    if (before) {
        if (retire_dispatch_captured)
            return;
        retire_dispatch_captured = true;
        retire_dispatch_workers = coord->tdd_workers_count;
        retire_dispatch_rc = EINVAL;
        retire_dispatch_root = session_find_rel(&coord->tdd_workers[0],
                "$d$relay");
        retire_dispatch_alias = session_find_rel(&coord->tdd_workers[1],
                "$d$relay");
        col_rel_t *owner = NULL;
        if (!retire_dispatch_root || !retire_dispatch_alias
            || retire_dispatch_root->nrows != 1 ||
            retire_dispatch_alias->nrows != 1
            || col_rel_storage_owner_resolve(retire_dispatch_alias, &owner) != 0
            || owner != retire_dispatch_root)
            return;
        retire_dispatch_view = retire_dispatch_root->view_generation;
        retire_dispatch_storage = retire_dispatch_root->storage_generation;
        retire_dispatch_timestamps = retire_dispatch_root->timestamps;
        retire_dispatch_rc = col_rel_source_reader_acquire_transferable(
            retire_dispatch_mode ==
            0 ? retire_dispatch_root : retire_dispatch_alias,
            &retire_dispatch_reader);
        return;
    }
    if (!retire_dispatch_captured || retire_dispatch_rc != 0)
        return;
    retire_dispatch_verified = retire_dispatch_root->nrows == 1
        && retire_dispatch_alias->nrows == 1
        && retire_dispatch_root->view_generation == retire_dispatch_view
        && retire_dispatch_root->storage_generation == retire_dispatch_storage
        && retire_dispatch_root->timestamps == retire_dispatch_timestamps
        && col_rel_get(retire_dispatch_root, 0, 0) == 42
        && col_rel_get(retire_dispatch_alias, 0, 0) == 42;
    if (retire_dispatch_mode == 2 && retire_dispatch_verified) {
        wl_col_session_t *worker = &coord->tdd_workers[1];
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        col_rel_t *held = col_rel_new_auto("held", 1);
        if (!held) {
            retire_dispatch_rc = ENOMEM; return;
        }
        retire_dispatch_rc = wl_columnar_eval_stack_cleanup_begin(worker,
                &frame);
        if (retire_dispatch_rc != 0) {
            col_rel_destroy(held); return;
        }
        eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
        result->rel = held;
        result->owned = true;
        retire_dispatch_rc =
            eval_stack_push(wl_columnar_eval_stack_cleanup_stack(frame),
                retire_dispatch_alias, false);
        if (retire_dispatch_rc == 0)
            retire_dispatch_rc =
                col_rel_source_reader_acquire_transferable(held,
                    &retire_dispatch_frame_reader);
        int finish_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
        if (retire_dispatch_rc == 0 && finish_rc != EBUSY)
            retire_dispatch_rc = EINVAL;
    }
}

typedef struct { unsigned count, seen; bool invalid; } retire_dispatch_rows_t;

static void
collect_retirement_rows(const char *relation, const int64_t *row,
    uint32_t ncols,
    void *user)
{
    retire_dispatch_rows_t *rows = user;
    rows->count++;
    unsigned bit = strcmp(relation, "output") == 0 ? 1u
        : strcmp(relation, "relay") == 0 ? 2u : 0u;
    rows->invalid |= !bit || ncols != 1 || row[0] != 42 ||
        (rows->seen & bit) != 0;
    rows->seen |= bit;
}

static void
test_tdd_retirement_actual_dispatch(uint32_t workers, unsigned mode)
{
    TEST(
        "prior TDD deltas: actual shared dispatch, barrier refusal and exact retry");
    uint32_t key = 0;
    wl_plan_op_exchange_t exchange = { .num_workers = 1, .key_col_idxs = &key,
                                       .key_col_count = 1 };
    const char *keys[] = { "col0" };
    uint32_t projection[] = { 0 };
    uint8_t predicate[] = { WL_PLAN_EXPR_BOOL, 1 };
    wl_plan_op_t output_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "output" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "output", .left_keys = keys,
          .right_keys = keys, .key_count = 1, .project_indices = projection,
          .project_count = 1 },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "relay" },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_op_t relay_ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "input" },
        { .op = WL_PLAN_OP_FILTER, .filter_expr = { predicate, 2 } },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange }
    };
    wl_plan_relation_t relations[] = {
        { .name = "output", .delta_name = "$d$output", .ops = output_ops,
          .op_count = 5 },
        { .name = "relay", .delta_name = "$d$relay", .ops = relay_ops,
          .op_count = 3 }
    };
    wl_plan_stratum_t stratum = { .relations = relations, .relation_count = 2,
                                  .is_recursive = true };
    const char *edb[] = { "input" };
    wl_plan_t plan = { .strata = &stratum, .stratum_count = 1,
                       .edb_relations = edb, .edb_count = 1 };
    wl_session_t *session = NULL;
    col_rel_t *unowned = NULL;
    int64_t *values = NULL;
    const char *failure = NULL;
    retire_dispatch_rows_t rows = { 0 };
    retire_dispatch_mode = mode;
    retire_dispatch_workers = retire_dispatch_rounds = 0;
    retire_dispatch_captured = retire_dispatch_verified = false;
    retire_dispatch_rc = EINVAL;
    memset(&retire_dispatch_reader, 0, sizeof(retire_dispatch_reader));
    memset(&retire_dispatch_frame_reader, 0,
        sizeof(retire_dispatch_frame_reader));
#define DISPATCH_CHECK(condition, message) \
        do { if (!(condition)) { failure = message; goto cleanup; } } while (0)
    DISPATCH_CHECK(wl_session_create(wl_backend_columnar(), &plan, workers,
        &session) == 0, "create");
    wl_col_session_t *sess = COL_SESSION(session);
    values = malloc(65536 * sizeof(*values));
    DISPATCH_CHECK(values, "input allocation");
    for (uint32_t i = 0; i < 65536; i++) values[i] = 42;
    DISPATCH_CHECK(wl_session_insert(session, "input", values, 65536, 1) == 0,
        "input");
    wirelog_column_type_t type = WIRELOG_TYPE_INT64;
    for (unsigned i = 0; i < 2; i++) {
        unowned = col_rel_new_auto(relations[i].name, 1);
        DISPATCH_CHECK(unowned && col_rel_set_column_types(unowned, &type,
            1) == 0
            && session_add_rel(sess, unowned) == 0, "typed empty IDB");
        unowned = NULL;
    }
    sess->tdd_audit.enabled = true;
    wl_columnar_eval_test_subpass_boundary = hold_prior_delta_across_dispatch;
    int rc = wl_session_snapshot(session, collect_retirement_rows, &rows);
    /* #1661 U4: rounds == 2, not >= 2.  The exact count pins that
     * wl_columnar_eval_retire_prior_deltas refused and aborted the sub-pass
     * before the exchange dispatch ran -- a third round would mean the
     * exchange executed and the refusal surfaced somewhere below it instead.
     * That is what makes the exchange-site session_remove_rel discards
     * correct: the refusal is pre-empted upstream, not swallowed there.
     *
     * Modes 0 and 1 hold a reader and are pinned by this.  Mode 2 also arms a
     * refusing cleanup frame, so its rc comes from the worker-state check
     * above retire and it stays at 2 either way -- it does not pin
     * retirement.  Do not read its passing as coverage. */
    DISPATCH_CHECK(rc == EBUSY && retire_dispatch_captured
        && retire_dispatch_verified && retire_dispatch_rc == 0 &&
        rows.count == 0
        && retire_dispatch_workers == workers && retire_dispatch_rounds == 2
        && sess->tdd_executed_strata == 1,
        "actual shared prior delta refusal");
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        DISPATCH_CHECK(wl_session_snapshot(session, collect_retirement_rows,
            &rows) == EBUSY
            && rows.count == 0 && retire_dispatch_root->nrows == 1
            && retire_dispatch_root->view_generation == retire_dispatch_view
            && retire_dispatch_root->storage_generation ==
            retire_dispatch_storage
            && retire_dispatch_root->timestamps == retire_dispatch_timestamps,
            "repeated refusal changed held root");
    }
    DISPATCH_CHECK(col_rel_source_reader_release(&retire_dispatch_reader) == 0,
        "release prior delta reader");
    if (retire_dispatch_frame_reader.owner)
        DISPATCH_CHECK(col_rel_source_reader_release(
                &retire_dispatch_frame_reader) == 0,
            "release retained frame");
    wl_columnar_eval_test_subpass_boundary = NULL;
    DISPATCH_CHECK(wl_session_snapshot(session, collect_retirement_rows,
        &rows) == 0
        && rows.count == 2 && rows.seen == 3 && !rows.invalid
        && sess->tdd_workers_count == 0, "exact snapshot retry");
cleanup:
    wl_columnar_eval_test_subpass_boundary = NULL;
    if (retire_dispatch_reader.owner)
        (void)col_rel_source_reader_release(&retire_dispatch_reader);
    if (retire_dispatch_frame_reader.owner)
        (void)col_rel_source_reader_release(&retire_dispatch_frame_reader);
    col_rel_destroy(unowned);
    free(values);
    wl_session_destroy(session);
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef DISPATCH_CHECK
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    for (uint32_t workers = 2; workers <= 8; workers *= 4) {
        test_global_exchange_metadata(workers, false);
        test_global_exchange_metadata(workers, true);
    }
    for (unsigned mode = 0; mode < 10; mode++) {
#ifndef WL_TEST_ALLOC_WRAP
        if (mode == 4 || mode == 6 || mode == 9)
            continue;
#endif
        test_tdd_prior_delta_retirement(2, mode);
        test_tdd_prior_delta_retirement(8, mode);
    }
    for (unsigned mode = 0; mode < 3; mode++) {
        test_tdd_retirement_actual_dispatch(2, mode);
        test_tdd_retirement_actual_dispatch(8, mode);
    }
    printf("TDD Recursive Distributed Evaluator Tests\n");
    printf("==========================================\n");

#ifdef WL_TEST_BDX_SEED
#ifdef WL_TEST_ALLOC_WRAP
    for (uint32_t workers = 2; workers <= 8; workers += 6)
        for (unsigned mode = 0; mode < 2; mode++)
            for (unsigned typed = 0; typed < 2; typed++)
                test_initializer_schema_parity(workers, mode, typed != 0);
#endif
    test_hybrid_empty_idb_ownership(0);
#ifdef WL_TEST_ALLOC_WRAP
    test_hybrid_empty_idb_ownership(1);
    test_hybrid_empty_idb_ownership(2);
    test_hybrid_empty_idb_ownership(3);
    for (uint32_t workers = 2; workers <= 8; workers += 6) {
        for (unsigned mode = 0; mode < 3; mode++) {
            test_initializer_unwind(workers, mode, 0, false, false);
            test_initializer_unwind(workers, mode, 3, false, false);
            test_initializer_unwind(workers, mode, 3, true, false);
            test_initializer_unwind(workers, mode, 3, false, true);
        }
        test_initializer_unwind(workers, 0, 1, false, false);
        test_initializer_unwind(workers, 1, 2, false, false);
    }
#endif
#endif
    test_tdd_existing_empty_idb(2, false, false);
    test_tdd_existing_empty_idb(2, false, true);
    test_tdd_existing_empty_idb(8, false, false);
    test_tdd_existing_empty_idb(8, false, true);
    test_tdd_existing_empty_idb(2, true, false);
    for (unsigned mode = 0; mode < 3; mode++) {
        test_tdd_worker_segments(2, mode, false);
        test_tdd_worker_segments(8, mode, false);
        test_tdd_worker_segments(2, mode, true);
        test_tdd_worker_segments(8, mode, true);
    }
    test_tc_w1_baseline();
    test_tc_w2_correctness();
    test_tc_w4_correctness();
    test_tc_cyclic();
    test_tc_w2_empty_edb();
    test_tc_deep_chain();
    test_tc_category_a_diamond();
    test_triple_idb_guard();
    test_bdx_selfjoin_w2();
    test_bdx_selfjoin_w4();
    test_bdx_selfjoin_w4_shuffled_duplicates();
    test_bdx_seed_reader_denial_and_retry();
    test_bdx_seed_alias_denial_and_retry();
    test_bdx_seed_staging_failure_is_atomic();
    test_bdx_seed_second_worker_failure_is_atomic();
    test_bdx_seed_sort_failure_is_atomic();
    test_bdx_seed_allocation_failure_is_atomic();
    test_bdx_seed_success_preserves_timestamps();
    TEST("owner lifetime refusal is retained and cleanup readiness retries");
    if (test_tdd_owner_lifetime_retry_gate() == 0)
        PASS();
    else
        FAIL("owner lifetime refusal/retry readiness");
    TEST("owner lifetime deduplicates slots and retries roots after aliases");
    if (test_tdd_owner_lifetime_alias_progress_and_dedup() == 0)
        PASS();
    else
        FAIL("owner lifetime alias progress/dedup");
    TEST(
        "owner lifetime retains governed heap, pool, and arena backing on refusal");
    if (test_tdd_owner_lifetime_mixed_backing_retention() == 0)
        PASS();
    else
        FAIL("owner lifetime mixed backing retention/accounting");
#ifdef WL_TEST_TDD_RESET_RESTORE
    test_tdd_reset_restore_transaction();
#endif
#ifdef WL_TEST_TDD_MERGE
    test_tdd_merge_transactional_publication();
    test_tdd_merge_schema_mismatch_rollback();
#endif
#ifdef WL_TEST_OWNER_PUBLICATION
    TEST("owner publication preserves populated hashless registries");
    if (test_owner_publication_hashless_registry() == 0)
        PASS();
    else
        FAIL("owner publication hashless registry");
    TEST("owner publication with existing targets sorts and retries");
    if (test_owner_publication_existing_targets() == 0)
        PASS();
    else
        FAIL("owner publication existing-target retry");
    TEST("owner publication transaction lifecycle");
    if (test_owner_publication_transaction() == 0)
        PASS();
    else
        FAIL("owner publication transaction");
#endif
    test_filt_arr_bench_w1();
    test_filt_arr_bench_w4();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
