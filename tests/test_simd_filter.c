/*
 * test_simd_filter.c - SIMD Fast-Path Filter Tests (Issue #214)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that the FILTER operator produces correct results for simple
 * comparison predicates via the real session API.  Validates the
 * scalar fast-path and (when compiled with AVX2/NEON) the SIMD path.
 *
 * Test cases:
 *   1. Simple EQ filter: x == 1 keeps only matching rows
 *   2. Simple GT filter: x > 5 keeps only rows where x > 5
 *   3. Empty result: filter eliminates all rows
 *   4. All pass: filter keeps all rows (GTE against minimum)
 *   5. Large relation: 1000+ rows with EQ filter
 *   6. NEQ filter: x != 2 keeps all rows except x==2
 *   7. LTE filter: x <= 3 keeps correct rows
 *   8. LT filter: x < 3 keeps correct rows
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework (matches wirelog convention)
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
    do {                                                \
        test_count++;                                   \
        printf("TEST %d: %s ... ", test_count, (name)); \
    } while (0)

#define PASS()            \
    do {                  \
        pass_count++;     \
        printf("PASS\n"); \
    } while (0)

#define FAIL(msg)                    \
    do {                             \
        fail_count++;                \
        printf("FAIL: %s\n", (msg)); \
        return;                      \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond))      \
            FAIL(msg);    \
    } while (0)

/* ----------------------------------------------------------------
 * Tuple collector
 * ---------------------------------------------------------------- */

#define MAX_ROWS 2048
#define MAX_NCOLS 16

typedef struct {
    uint32_t count;
    int64_t rows[MAX_ROWS][MAX_NCOLS];
    uint32_t ncols[MAX_ROWS];
    int oom;
    /* per-relation tracking */
    const char *tracked_rel;
    uint32_t tracked_count;
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collect_t *c = (collect_t *)user;
    if (c->tracked_rel && relation && strcmp(relation, c->tracked_rel) == 0) {
        c->tracked_count++;
        if (c->count < MAX_ROWS) {
            uint32_t idx = c->count++;
            c->ncols[idx] = ncols < MAX_NCOLS ? ncols : MAX_NCOLS;
            for (uint32_t i = 0; i < c->ncols[idx]; i++)
                c->rows[idx][i] = row[i];
        } else {
            c->oom = 1;
        }
    }
}

/* ----------------------------------------------------------------
 * Session helper
 * ---------------------------------------------------------------- */

/*
 * run_program_with_facts:
 * Parse src, apply all optimizer passes, create session, load facts,
 * collect tuples from tracked_rel, then tear down.
 *
 * Returns 0 on success.  out->tracked_count holds the tuple count for
 * tracked_rel; out->rows holds up to MAX_ROWS tuples.
 */
static int
run_program_with_facts(const char *src, const char *tracked_rel, collect_t *out)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "parse error: %d\n", (int)err);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        fprintf(stderr, "plan generation failed\n");
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        fprintf(stderr, "session create failed\n");
        return -1;
    }

    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        fprintf(stderr, "load facts failed\n");
        return -1;
    }

    memset(out, 0, sizeof(*out));
    out->tracked_rel = tracked_rel;

    int rc = wl_session_snapshot(sess, collect_cb, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ================================================================
 * Test 1: Simple EQ filter -- x == 1 keeps only matching rows.
 *
 * edge(1,10), edge(1,20), edge(2,30), edge(3,40)
 * filtered(x,y) :- edge(x,y), x == 1.
 * Expected: filtered = {(1,10), (1,20)} -- 2 tuples
 * ================================================================ */
static void
test_eq_filter_basic(void)
{
    TEST("EQ filter x==1 keeps correct rows");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 10). edge(1, 20). edge(2, 30). edge(3, 40).\n"
                      ".decl filtered(x: int32, y: int32)\n"
                      "filtered(x, y) :- edge(x, y), x = 1.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 2, "expected 2 rows (x==1)");

    /* All returned rows must have x==1 */
    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] == 1, "row x value must be 1");

    PASS();
}

/* ================================================================
 * Test 2: Simple GT filter -- x > 5 keeps only rows where x > 5.
 *
 * edge(3,1), edge(5,2), edge(6,3), edge(7,4), edge(10,5)
 * filtered(x,y) :- edge(x,y), x > 5.
 * Expected: filtered = {(6,3),(7,4),(10,5)} -- 3 tuples
 * ================================================================ */
static void
test_gt_filter_basic(void)
{
    TEST("GT filter x>5 keeps correct rows");

    const char *src
        = ".decl edge(x: int32, y: int32)\n"
          "edge(3, 1). edge(5, 2). edge(6, 3). edge(7, 4). edge(10, 5).\n"
          ".decl filtered(x: int32, y: int32)\n"
          "filtered(x, y) :- edge(x, y), x > 5.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected 3 rows (x>5)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] > 5, "row x value must be > 5");

    PASS();
}

/* ================================================================
 * Test 3: Empty result -- filter eliminates all rows.
 *
 * edge(1,10), edge(2,20), edge(3,30)
 * filtered(x,y) :- edge(x,y), x == 99.
 * Expected: filtered = {} -- 0 tuples
 * ================================================================ */
static void
test_filter_empty_result(void)
{
    TEST("EQ filter with no matching rows returns empty result");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 10). edge(2, 20). edge(3, 30).\n"
                      ".decl filtered(x: int32, y: int32)\n"
                      "filtered(x, y) :- edge(x, y), x = 99.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 0, "expected 0 rows when no match");

    PASS();
}

/* ================================================================
 * Test 4: All pass -- GTE filter keeps all rows.
 *
 * edge(1,10), edge(2,20), edge(3,30)
 * filtered(x,y) :- edge(x,y), x >= 1.
 * Expected: filtered = all 3 tuples
 * ================================================================ */
static void
test_filter_all_pass(void)
{
    TEST("GTE filter x>=1 keeps all rows");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 10). edge(2, 20). edge(3, 30).\n"
                      ".decl filtered(x: int32, y: int32)\n"
                      "filtered(x, y) :- edge(x, y), x >= 1.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected all 3 rows to pass");

    PASS();
}

/* ================================================================
 * Test 5: Large relation -- 1000+ rows with EQ filter.
 *
 * edge(i, i*2) for i in [0, 1023].
 * filtered(x,y) :- edge(x,y), x == 512.
 * Expected: filtered = {(512, 1024)} -- exactly 1 tuple
 * ================================================================ */
static void
test_filter_large_relation(void)
{
    TEST("EQ filter on 1024-row relation returns 1 tuple");

    /* Build source with 1024 inline facts */
    const int NROWS = 1024;
    const int BUFSIZE = 1024 * 24 + 512;
    char *src = (char *)malloc((size_t)BUFSIZE);
    ASSERT(src != NULL, "malloc failed");

    int pos = 0;
    pos += snprintf(src + pos, (size_t)(BUFSIZE - pos),
                    ".decl edge(x: int32, y: int32)\n");
    for (int i = 0; i < NROWS; i++) {
        pos += snprintf(src + pos, (size_t)(BUFSIZE - pos), "edge(%d, %d).\n",
                        i, i * 2);
    }
    pos += snprintf(src + pos, (size_t)(BUFSIZE - pos),
                    ".decl filtered(x: int32, y: int32)\n"
                    "filtered(x, y) :- edge(x, y), x = 512.\n");

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    free(src);

    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 1, "expected exactly 1 row (x==512)");
    ASSERT(out.count >= 1, "collect buffer has row");
    ASSERT(out.rows[0][0] == 512, "x value must be 512");
    ASSERT(out.rows[0][1] == 1024, "y value must be 1024");

    PASS();
}

/* ================================================================
 * Test 6: NEQ filter -- x != 2 keeps all rows except x==2.
 *
 * edge(1,10), edge(2,20), edge(3,30), edge(4,40)
 * filtered(x,y) :- edge(x,y), x != 2.
 * Expected: 3 tuples
 * ================================================================ */
static void
test_neq_filter(void)
{
    TEST("NEQ filter x!=2 keeps all rows except x==2");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 10). edge(2, 20). edge(3, 30). edge(4, 40).\n"
                      ".decl filtered(x: int32, y: int32)\n"
                      "filtered(x, y) :- edge(x, y), x != 2.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected 3 rows (x!=2)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] != 2, "row x value must not be 2");

    PASS();
}

/* ================================================================
 * Test 7: LTE filter -- x <= 3 keeps rows where x <= 3.
 *
 * edge(1..5, ...)
 * filtered(x,y) :- edge(x,y), x <= 3.
 * Expected: 3 tuples
 * ================================================================ */
static void
test_lte_filter(void)
{
    TEST("LTE filter x<=3 keeps correct rows");

    const char *src
        = ".decl edge(x: int32, y: int32)\n"
          "edge(1, 10). edge(2, 20). edge(3, 30). edge(4, 40). edge(5, 50).\n"
          ".decl filtered(x: int32, y: int32)\n"
          "filtered(x, y) :- edge(x, y), x <= 3.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 3, "expected 3 rows (x<=3)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] <= 3, "row x value must be <= 3");

    PASS();
}

/* ================================================================
 * Test 8: LT filter -- x < 3 keeps rows where x < 3.
 * ================================================================ */
static void
test_lt_filter(void)
{
    TEST("LT filter x<3 keeps correct rows");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 10). edge(2, 20). edge(3, 30). edge(4, 40).\n"
                      ".decl filtered(x: int32, y: int32)\n"
                      "filtered(x, y) :- edge(x, y), x < 3.\n";

    collect_t out;
    int rc = run_program_with_facts(src, "filtered", &out);
    ASSERT(rc == 0, "session execution failed");
    ASSERT(out.tracked_count == 2, "expected 2 rows (x<3)");

    for (uint32_t i = 0; i < out.count; i++)
        ASSERT(out.rows[i][0] < 3, "row x value must be < 3");

    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== SIMD Filter Fast-Path Tests (Issue #214) ===\n");

    test_eq_filter_basic();
    test_gt_filter_basic();
    test_filter_empty_result();
    test_filter_all_pass();
    test_filter_large_relation();
    test_neq_filter();
    test_lte_filter();
    test_lt_filter();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ---\n");

    return (fail_count > 0) ? 1 : 0;
}
