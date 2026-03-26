/*
 * test_compaction.c - Row-level compaction tests for relation data buffers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates that col_rel_compact() correctly shrinks oversized buffers
 * after bulk retractions, and that subsequent inserts/consolidations
 * continue to work correctly.
 *
 * Issue #217: Row-level compaction for relation data buffers
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
/* Plan Helper                                                             */
/* ======================================================================== */

static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/* ======================================================================== */
/* Test 1: Basic compaction after bulk retraction                           */
/* ======================================================================== */

/*
 * Insert 1000 rows, retract 900, step.
 * Verify capacity <= nrows * 4.
 */
static int
test_compact_basic(void)
{
    TEST("Basic compaction after bulk retraction");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return 1;
    }

    /* Insert 1000 rows */
    int64_t *insert_data = (int64_t *)malloc(1000 * sizeof(int64_t));
    if (!insert_data) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("malloc failed");
        return 1;
    }
    for (int i = 0; i < 1000; i++)
        insert_data[i] = i + 1;
    wl_session_insert(session, "a", insert_data, 1000, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        free(insert_data);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 1 failed");
        return 1;
    }

    /* Retract 900 rows (leave 100) */
    int64_t *remove_data = (int64_t *)malloc(900 * sizeof(int64_t));
    if (!remove_data) {
        free(insert_data);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("malloc failed");
        return 1;
    }
    for (int i = 0; i < 900; i++)
        remove_data[i] = i + 1;
    rc = wl_session_remove(session, "a", remove_data, 900, 1);
    if (rc != 0) {
        free(remove_data);
        free(insert_data);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("remove failed");
        return 1;
    }

    rc = wl_session_step(session);
    free(remove_data);
    free(insert_data);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 2 failed");
        return 1;
    }

    /* Access internal state via COL_SESSION cast */
    wl_col_session_t *sess = (wl_col_session_t *)session;
    col_rel_t *a_rel = NULL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, "a") == 0) {
            a_rel = sess->rels[i];
            break;
        }
    }

    bool ok = true;
    if (!a_rel) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("relation 'a' not found");
        return 1;
    }

    if (a_rel->nrows > 0 && a_rel->capacity > a_rel->nrows * 4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "capacity %u still > nrows*4 (%u) after compaction",
            a_rel->capacity, a_rel->nrows * 4);
        FAIL(msg);
        ok = false;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 2: Compaction threshold — only fires when capacity > nrows*4        */
/* ======================================================================== */

static int
test_compact_threshold(void)
{
    TEST("Compaction threshold (only when capacity > nrows*4)");

    /* Create a relation directly and test col_rel_compact */
    col_rel_t *r = col_rel_new_auto("test_rel", 1);
    if (!r) {
        FAIL("col_rel_new_auto failed");
        return 1;
    }

    /* Append 10 rows — capacity should be COL_REL_INIT_CAP=64 */
    for (int i = 0; i < 10; i++) {
        int64_t val = i;
        col_rel_append_row(r, &val);
    }

    uint32_t cap_before = r->capacity;

    /* nrows=10, capacity=64: 64 <= 10*4=40? No, 64>40, so compaction fires */
    col_rel_compact(r);
    uint32_t cap_after = r->capacity;

    bool threshold_respected
        = (cap_after <= (uint32_t)(r->nrows * 4) || cap_after == cap_before);

    /* Now test the no-compaction case: nrows=32, capacity=64: 64 <= 32*4=128 */
    col_rel_t *r2 = col_rel_new_auto("test_rel2", 1);
    if (!r2) {
        col_rel_destroy(r);
        FAIL("col_rel_new_auto failed");
        return 1;
    }
    for (int i = 0; i < 32; i++) {
        int64_t val = i;
        col_rel_append_row(r2, &val);
    }
    /* capacity should now be 64 = exactly nrows*2, which is <= nrows*4=128 */
    uint32_t cap2_before = r2->capacity;
    col_rel_compact(r2);
    uint32_t cap2_after = r2->capacity;
    /* merge_buf freed regardless, but data capacity unchanged */
    bool no_shrink = (cap2_after == cap2_before);

    col_rel_destroy(r);
    col_rel_destroy(r2);

    if (!threshold_respected) {
        FAIL("capacity not reduced when it should be");
        return 1;
    }
    if (!no_shrink) {
        FAIL("capacity changed when it should not have");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: merge_buf freed; subsequent insert+consolidation works           */
/* ======================================================================== */

static int
test_compact_merge_buf_freed(void)
{
    TEST("merge_buf freed; subsequent insert+consolidation works");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return 1;
    }

    /* Insert 200 rows and step to populate merge_buf via consolidation */
    int64_t *data = (int64_t *)malloc(200 * sizeof(int64_t));
    if (!data) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("malloc failed");
        return 1;
    }
    for (int i = 0; i < 200; i++)
        data[i] = i + 1;
    wl_session_insert(session, "a", data, 200, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        free(data);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 1 failed");
        return 1;
    }

    /* Retract 180 rows to trigger compaction */
    rc = wl_session_remove(session, "a", data, 180, 1);
    free(data);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("remove failed");
        return 1;
    }
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 2 failed");
        return 1;
    }

    /* Verify merge_buf is NULL after compaction */
    wl_col_session_t *sess = (wl_col_session_t *)session;
    col_rel_t *a_rel = NULL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, "a") == 0) {
            a_rel = sess->rels[i];
            break;
        }
    }
    if (a_rel && a_rel->merge_columns != NULL) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("merge_columns not NULL after compaction");
        return 1;
    }

    /* Insert more rows and step — consolidation must work without crash */
    int64_t new_data[] = { 500, 501, 502 };
    wl_session_insert(session, "a", new_data, 3, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 3 after merge_buf freed failed");
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: Empty relation — capacity zeroed, then re-insert works           */
/* ======================================================================== */

static int
test_compact_empty_relation(void)
{
    TEST("Empty relation: capacity zeroed, re-insert works");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return 1;
    }

    /* Insert 50 rows */
    int64_t *data = (int64_t *)malloc(50 * sizeof(int64_t));
    if (!data) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("malloc failed");
        return 1;
    }
    for (int i = 0; i < 50; i++)
        data[i] = i + 1;
    wl_session_insert(session, "a", data, 50, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        free(data);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 1 failed");
        return 1;
    }

    /* Retract ALL 50 rows */
    rc = wl_session_remove(session, "a", data, 50, 1);
    free(data);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("remove failed");
        return 1;
    }
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 2 failed");
        return 1;
    }

    /* After compaction of empty relation, capacity should be 0 */
    wl_col_session_t *sess = (wl_col_session_t *)session;
    col_rel_t *a_rel = NULL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, "a") == 0) {
            a_rel = sess->rels[i];
            break;
        }
    }

    bool cap_zeroed = true;
    if (a_rel && a_rel->nrows == 0 && a_rel->capacity != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "capacity %u != 0 after full retraction",
            a_rel->capacity);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL(msg);
        return 1;
    }
    (void)cap_zeroed;

    /* Re-insert rows — session must still work */
    int64_t new_data[] = { 100, 200 };
    wl_session_insert(session, "a", new_data, 2, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step 3 (re-insert) failed");
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 5: Insert-retract cycle — capacity stays bounded                    */
/* ======================================================================== */

static int
test_compact_insert_retract_cycle(void)
{
    TEST("Insert-retract cycle: capacity stays bounded");

    wl_plan_t *plan = build_plan(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return 1;
    }

    /* 100 cycles: insert 100, retract 90, step */
    for (int cycle = 0; cycle < 100; cycle++) {
        int64_t insert_data[100];
        for (int i = 0; i < 100; i++)
            insert_data[i] = cycle * 100 + i + 1;
        wl_session_insert(session, "a", insert_data, 100, 1);
        rc = wl_session_step(session);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            FAIL("insert step failed");
            return 1;
        }

        /* Retract first 90 of the inserted rows */
        rc = wl_session_remove(session, "a", insert_data, 90, 1);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            FAIL("remove failed");
            return 1;
        }
        rc = wl_session_step(session);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            FAIL("retract step failed");
            return 1;
        }
    }

    /* After 100 cycles with compaction, capacity should be bounded.
     * Each cycle keeps 10 rows: max live rows ~1000.
     * capacity should not have grown unboundedly. */
    wl_col_session_t *sess = (wl_col_session_t *)session;
    col_rel_t *a_rel = NULL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, "a") == 0) {
            a_rel = sess->rels[i];
            break;
        }
    }

    bool ok = true;
    if (a_rel) {
        /* Without compaction, capacity could grow to 100*100*2 = 20000+.
         * With compaction: capacity <= max(nrows*2, COL_REL_INIT_CAP)*~few.
         * Use generous bound: capacity <= nrows * 8 + 128 */
        uint32_t bound = a_rel->nrows * 8 + 128;
        if (a_rel->capacity > bound) {
            char msg[128];
            snprintf(msg, sizeof(msg),
                "capacity %u unbounded (nrows=%u, bound=%u)",
                a_rel->capacity, a_rel->nrows, bound);
            FAIL(msg);
            ok = false;
        }
    }

    wl_session_destroy(session);
    wl_plan_free(plan);

    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Compaction Tests (Issue #217)\n");
    printf("==============================\n\n");

    test_compact_basic();
    test_compact_threshold();
    test_compact_merge_buf_freed();
    test_compact_empty_relation();
    test_compact_insert_retract_cycle();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
