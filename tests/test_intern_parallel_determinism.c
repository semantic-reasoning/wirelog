/*
 * test_intern_parallel_determinism.c - shared intern table under parallel
 * evaluation (Issue #958)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Every parallel worker borrows the coordinator's wl_intern_t: the
 * per-worker session copies carry the same pointer and it is not on
 * either null-out list.  A rule that builds strings in a head position
 * therefore has several threads calling wl_intern_put()/reverse() on
 * one table at once.  Unsynchronized, that produced two ids for one
 * string (wrong joins, extra tuples) and, once the id -> string array
 * was reallocated under a concurrent reader, heap corruption.
 *
 * The reproducer is a recursive rule with four IDB body atoms and a
 * cat() in the head, which is the K-fusion dispatch shape:
 * wirelog/columnar/ops.c has no row-count gate on that path, only
 * WL_KFUSION_MIN_PARALLEL_K = 4 live body atoms.  It is therefore the
 * cheapest of the three parallel paths to reach -- 30 EDB facts are
 * enough, where the non-recursive path needs 65,536 rows.
 *
 * The assertion is that W=8 reproduces the W=1 result exactly, over
 * repetitions, comparing the full sorted string-valued output rather
 * than a tuple count: a count alone misses a run where a symbol was
 * interned twice but the row totals happened to match.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

/* *INDENT-OFF* */
#define TEST(name) \
        do { \
            tests_run++; \
            printf("  [%d] %s", tests_run, (name)); \
        } while (0)

#define PASS() \
        do { \
            tests_passed++; \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg) \
        do { \
            tests_failed++; \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)
/* *INDENT-ON* */

/* ======================================================================== */
/* Workload                                                                 */
/* ======================================================================== */

/* 3 chains x 10 edges = 30 EDB facts. */
#define CHAINS 3
#define CHAIN_LEN 10

/* Repetitions per worker count.  The defect is probabilistic: measured
 * on the unfixed table this shape mismatched in roughly a third of the
 * W=8 runs, so a handful of repetitions is enough to catch it while
 * keeping the test well under a second. */
#define REPS 12

#define MAX_ROWS 4096
#define MAX_ROW_TEXT 128

/*
 * P is seeded from the EDB and then closed under a four-way self-join,
 * with the label built by cat() in the head position -- the string that
 * each worker has to intern.
 */
static char *
build_program(void)
{
    size_t cap = 4096 + (size_t)CHAINS * CHAIN_LEN * 48;
    char *src = (char *)malloc(cap);
    if (!src)
        return NULL;

    size_t pos = 0;
    int n = snprintf(src + pos, cap - pos,
            ".decl E(x: symbol, y: symbol)\n"
            ".decl P(x: symbol, y: symbol, l: symbol)\n");
    if (n < 0)
        goto fail;
    pos += (size_t)n;

    for (int c = 0; c < CHAINS; c++) {
        for (int d = 0; d < CHAIN_LEN; d++) {
            n = snprintf(src + pos, cap - pos, "E(\"n%d_%d\", \"n%d_%d\").\n",
                    c, d, c, d + 1);
            if (n < 0 || (size_t)n >= cap - pos)
                goto fail;
            pos += (size_t)n;
        }
    }

    n = snprintf(src + pos, cap - pos,
            "P(x, y, cat(cat(x, \"->\"), y)) :- E(x, y).\n"
            "P(a, e, cat(cat(a, \"=>\"), e)) :-"
            " P(a, b, l1), P(b, c, l2), P(c, d, l3), P(d, e, l4).\n");
    if (n < 0 || (size_t)n >= cap - pos)
        goto fail;

    return src;

fail:
    free(src);
    return NULL;
}

/* ======================================================================== */
/* Result Collection                                                        */
/* ======================================================================== */

typedef struct {
    const wl_intern_t *intern;
    uint32_t count;
    int overflow;
    int unresolved; /* a column id that did not reverse-map */
    char rows[MAX_ROWS][MAX_ROW_TEXT];
} result_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    result_t *res = (result_t *)user_data;

    if (res->count >= MAX_ROWS) {
        res->overflow = 1;
        return;
    }

    char *dst = res->rows[res->count];
    size_t pos = 0;

    int n = snprintf(dst, MAX_ROW_TEXT, "%s(", relation ? relation : "?");
    if (n < 0)
        return;
    pos += (size_t)n;

    for (uint32_t i = 0; i < ncols && pos + 1 < MAX_ROW_TEXT; i++) {
        const char *val = wl_intern_reverse(res->intern, row[i]);
        if (!val)
            res->unresolved = 1;
        n = snprintf(dst + pos, MAX_ROW_TEXT - pos, "%s%s", i ? "," : "",
                val ? val : "?");
        if (n < 0)
            break;
        pos += (size_t)n;
    }
    if (pos + 1 < MAX_ROW_TEXT)
        snprintf(dst + pos, MAX_ROW_TEXT - pos, ")");

    res->count++;
}

static int
row_cmp(const void *a, const void *b)
{
    return strcmp((const char *)a, (const char *)b);
}

/*
 * Evaluate @src with @workers workers and fill @res with the sorted
 * string form of every derived tuple.  Returns 0 on success.
 */
static int
evaluate(const char *src, uint32_t workers, result_t *res)
{
    memset(res, 0, sizeof(*res));

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0 || !plan) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *session = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, workers, &session) != 0
        || !session) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    res->intern = wirelog_program_get_intern(prog);

    int rc = -1;
    if (wl_session_load_facts(session, prog) == 0
        && wl_session_snapshot(session, collect_cb, res) == 0)
        rc = 0;

    if (rc == 0 && (res->overflow || res->unresolved))
        rc = -1;

    if (rc == 0)
        qsort(res->rows, res->count, MAX_ROW_TEXT, row_cmp);

    /* The rows are copied out as text, so the intern table can go. */
    res->intern = NULL;

    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

static int
results_equal(const result_t *a, const result_t *b)
{
    if (a->count != b->count)
        return 0;
    for (uint32_t i = 0; i < a->count; i++) {
        if (strcmp(a->rows[i], b->rows[i]) != 0)
            return 0;
    }
    return 1;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_kfusion_symbol_determinism(uint32_t workers)
{
    char name[96];

    snprintf(name, sizeof(name),
        "K-fusion head cat(): W=%u matches W=1 over %d runs", workers, REPS);
    TEST(name);

    char *src = build_program();
    if (!src) {
        FAIL("program generation failed");
        return;
    }

    result_t *baseline = (result_t *)malloc(sizeof(*baseline));
    result_t *actual = (result_t *)malloc(sizeof(*actual));
    if (!baseline || !actual) {
        FAIL("allocation failed");
        goto out;
    }

    if (evaluate(src, 1, baseline) != 0) {
        FAIL("single-worker evaluation failed");
        goto out;
    }
    if (baseline->count == 0) {
        FAIL("single-worker evaluation derived nothing");
        goto out;
    }

    for (int rep = 0; rep < REPS; rep++) {
        if (evaluate(src, workers, actual) != 0) {
            FAIL("parallel evaluation failed");
            goto out;
        }
        if (!results_equal(baseline, actual)) {
            printf("\n      rep %d: %u rows vs %u at W=1\n", rep,
                actual->count, baseline->count);
            FAIL("parallel output differs from single-worker output");
            goto out;
        }
    }

    PASS();

out:
    free(baseline);
    free(actual);
    free(src);
}

int
main(void)
{
    printf("=== Shared Intern Table Under Parallel Evaluation ===\n");

    test_kfusion_symbol_determinism(8);
    test_kfusion_symbol_determinism(16);

    printf("\nResults: %d/%d passed, %d failed\n", tests_passed, tests_run,
        tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
