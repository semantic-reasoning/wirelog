/*
 * test_tdd_nonrecursive.c - Unit tests for col_eval_stratum_tdd non-recursive path
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: W=1 fast path, W=2 and W=4 correctness vs single-worker baseline,
 * empty EDB, multi-relation stratum.
 *
 * Issue #318: Distributed Stratum Evaluator Phase 1
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value ? value : "");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#  define setenv wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
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
 * Build a coordinator session with "edge(x,y) :- edge(x,y)." (copy rule).
 * This is a non-recursive stratum: copies EDB edge facts to IDB edge_t.
 */
static wl_col_session_t *
make_copy_session(uint32_t num_workers, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    /* Simple copy rule: non-recursive stratum */
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl edge_t(x: int32, y: int32)\n"
        "edge_t(x, y) :- edge(x, y).\n",
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

/* Insert edge rows (each row = {x, y}) into a session. */
static int
insert_edges(wl_col_session_t *sess, const int64_t *rows, uint32_t nrows)
{
    return wl_session_insert(&sess->base, "edge", rows, nrows, 2);
}

/* Count rows in a named relation (0 if not found). */
static uint32_t
count_rows(wl_col_session_t *sess, const char *name)
{
    col_rel_t *r = session_find_rel(sess, name);

    return r ? r->nrows : 0;
}

/* ======================================================================== */
/* Plain steps over a program at W workers (#2134)                          */
/* ======================================================================== */

/* Setting the knob to 1 lets inputs of a few rows reach the partitioned
 * path.  The previous value is restored by knob_restore(). */
#define MIN_ROWS_KNOB "WIRELOG_TDD_MIN_ROWS_PER_WORKER"

static char knob_saved[64];
static int knob_was_set;

static void
knob_set(const char *value)
{
    const char *old = getenv(MIN_ROWS_KNOB);
    knob_was_set = old != NULL;
    if (old)
        snprintf(knob_saved, sizeof(knob_saved), "%s", old);
    if (value)
        setenv(MIN_ROWS_KNOB, value, 1);
    else
        unsetenv(MIN_ROWS_KNOB);
}

static void
knob_restore(void)
{
    if (knob_was_set)
        setenv(MIN_ROWS_KNOB, knob_saved, 1);
    else
        unsetenv(MIN_ROWS_KNOB);
}

/* A session for @src built from the parsed program with no optimizer pass,
 * unless @sip applies the SIP pass (one of the passes wirelog_optimize runs
 * by default) to get SEMIJOIN operators. */
static wl_col_session_t *
program_session(const char *src, uint32_t num_workers, bool sip,
    wl_plan_t **plan_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;

    *plan_out = NULL;
    if (!prog)
        return NULL;
    if (sip)
        wl_sip_apply(prog, NULL);
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);
    if (wl_session_create(wl_backend_columnar(), plan, num_workers,
        &session) != 0 || !session) {
        wl_plan_free(plan);
        return NULL;
    }
    *plan_out = plan;
    return COL_SESSION(session);
}

typedef enum { OP_INSERT, OP_REMOVE, OP_STEP, OP_SNAPSHOT } seq_kind_t;

typedef struct {
    seq_kind_t kind;
    const char *rel;      /* OP_INSERT / OP_REMOVE */
    uint32_t ncols;
    uint32_t nrows;
    const int64_t *rows;
} seq_op_t;

#define MAX_READS 6
#define MAX_HEAD_ROWS 64

/* The head's rows after each step or snapshot, sorted. */
typedef struct {
    int rc;
    uint32_t nreads;
    uint32_t ncols[MAX_READS];
    uint32_t nrows[MAX_READS];
    int64_t rows[MAX_READS][MAX_HEAD_ROWS][2];
    uint32_t max_active_workers[MAX_READS];
    bool has_semijoin; /* the plan holds a SEMIJOIN operator */
} seq_result_t;

static void
ignore_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)user_data;
}

static int
head_row_cmp(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a;
    const int64_t *y = (const int64_t *)b;
    for (int c = 0; c < 2; c++) {
        if (x[c] != y[c])
            return x[c] < y[c] ? -1 : 1;
    }
    return 0;
}

static void
capture_head(wl_col_session_t *sess, const char *head, seq_result_t *out)
{
    uint32_t r = out->nreads++;
    col_rel_t *rel = session_find_rel(sess, head);

    if (r >= MAX_READS)
        return; /* same_reads fails a sequence with more reads */

    out->max_active_workers[r] = sess->tdd_max_active_workers;
    if (!rel) {
        out->ncols[r] = 0;
        out->nrows[r] = 0;
        return;
    }
    out->ncols[r] = rel->ncols;
    out->nrows[r] = rel->nrows;
    if (rel->ncols > 2 || rel->nrows > MAX_HEAD_ROWS) {
        out->nrows[r] = UINT32_MAX;
        return;
    }
    memset(out->rows[r], 0, sizeof(out->rows[r]));
    for (uint32_t i = 0; i < rel->nrows; i++)
        for (uint32_t c = 0; c < rel->ncols; c++)
            out->rows[r][i][c] = rel->columns[c][i];
    qsort(out->rows[r], rel->nrows, sizeof(out->rows[r][0]), head_row_cmp);
}

/* Run @ops on a fresh session for @src at @num_workers and capture @head
 * after every step and snapshot. */
static seq_result_t
run_sequence(const char *src, bool sip, const char *head,
    uint32_t num_workers, const seq_op_t *ops, uint32_t nops)
{
    seq_result_t out;
    wl_plan_t *plan = NULL;
    wl_col_session_t *sess = program_session(src, num_workers, sip, &plan);

    memset(&out, 0, sizeof(out));
    out.rc = sess ? 0 : ENOMEM;
    for (uint32_t si = 0; plan && si < plan->stratum_count; si++)
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++)
            for (uint32_t oi = 0;
                oi < plan->strata[si].relations[ri].op_count; oi++)
                if (plan->strata[si].relations[ri].ops[oi].op
                    == WL_PLAN_OP_SEMIJOIN)
                    out.has_semijoin = true;
    for (uint32_t i = 0; sess && i < nops && out.rc == 0; i++) {
        const seq_op_t *op = &ops[i];
        switch (op->kind) {
        case OP_INSERT:
            out.rc = wl_session_insert(&sess->base, op->rel, op->rows,
                    op->nrows, op->ncols);
            break;
        case OP_REMOVE:
            out.rc = wl_session_remove(&sess->base, op->rel, op->rows,
                    op->nrows, op->ncols);
            break;
        case OP_STEP:
            out.rc = wl_session_step(&sess->base);
            if (out.rc == 0)
                capture_head(sess, head, &out);
            break;
        default:
            out.rc = wl_session_snapshot(&sess->base, ignore_tuple, NULL);
            if (out.rc == 0)
                capture_head(sess, head, &out);
            break;
        }
    }
    if (sess)
        wl_session_destroy(&sess->base);
    wl_plan_free(plan);
    return out;
}

static bool
same_reads(const seq_result_t *a, const seq_result_t *b)
{
    if (a->rc != 0 || b->rc != 0 || a->nreads != b->nreads
        || a->nreads > MAX_READS)
        return false;
    for (uint32_t r = 0; r < a->nreads; r++) {
        if (a->nrows[r] == UINT32_MAX || a->nrows[r] != b->nrows[r]
            || a->ncols[r] != b->ncols[r])
            return false;
        if (memcmp(a->rows[r], b->rows[r],
            a->nrows[r] * sizeof(a->rows[r][0])) != 0)
            return false;
    }
    return true;
}

/* Run @ops at one worker and at @num_workers and compare every read. */
static bool
matches_one_worker_sip(const char *label, const char *src, bool sip,
    const char *head, uint32_t num_workers, const seq_op_t *ops,
    uint32_t nops)
{
    seq_result_t want = run_sequence(src, sip, head, 1, ops, nops);
    seq_result_t got = run_sequence(src, sip, head, num_workers, ops, nops);

    if (sip && !got.has_semijoin) {
        printf("\n    %s: the plan holds no SEMIJOIN", label);
        return false;
    }
    if (same_reads(&want, &got))
        return true;
    printf("\n    %s W=%u: rc %d/%d reads %u/%u", label, num_workers,
        want.rc, got.rc, want.nreads, got.nreads);
    for (uint32_t r = 0; r < want.nreads && r < got.nreads && r < MAX_READS;
        r++)
        printf(" [rows %u/%u]", want.nrows[r], got.nrows[r]);
    return false;
}

static bool
matches_one_worker(const char *label, const char *src, const char *head,
    uint32_t num_workers, const seq_op_t *ops, uint32_t nops)
{
    return matches_one_worker_sip(label, src, false, head, num_workers, ops,
               nops);
}

static const char cross_src[] =
    ".decl e(x: int64)\n.decl p(a: int64, b: int64)\n"
    "p(x, y) :- e(x), e(y).\n";
static const char two_heads_src[] =
    ".decl e(x: int64)\n.decl r(x: int64)\n.decl q(x: int64)\n"
    ".decl p(a: int64, b: int64)\n"
    "r(x) :- e(x).\nq(x) :- e(x), x > 7.\np(x, y) :- r(x), q(y).\n";
static const char join_src[] =
    ".decl e(x: int64, z: int64)\n.decl f(z: int64, y: int64)\n"
    ".decl p(x: int64, y: int64)\np(x, y) :- e(x, z), f(z, y).\n";
static const char neg_src[] =
    ".decl e(x: int64, y: int64)\n.decl f(y: int64, z: int64)\n"
    ".decl c(x: int64, y: int64)\nc(x, y) :- e(x, y), !f(y, _).\n";
static const char semi_src[] =
    ".decl e(x: int64, y: int64)\n.decl f(y: int64, z: int64)\n"
    ".decl c(x: int64, y: int64)\nc(x, y) :- e(x, y), f(y, _).\n";
/* Three atoms: the SIP pass guards the join with a SEMIJOIN. */
static const char chain_src[] =
    ".decl a(x: int64, y: int64)\n.decl b(y: int64, z: int64)\n"
    ".decl c(z: int64, w: int64)\n.decl p(x: int64, w: int64)\n"
    "p(x, w) :- a(x, y), b(y, z), c(z, w).\n";
static const char count_src[] =
    ".decl e(x: int64, y: int64)\n.decl c(y: int64, n: int64)\n"
    "c(y, count(x)) :- e(x, y).\n";

static const int64_t v78[] = { 7, 8 };
static const int64_t v9[] = { 9 };
static const int64_t v789[] = { 7, 8, 9 };
static const int64_t v10[] = { 10 };
static const int64_t join_e[] = { 1, 10, 2, 20, 3, 10, 4, 30 };
static const int64_t join_f[] = { 10, 100, 10, 101, 20, 200, 30, 300 };
static const int64_t join_e_gone[] = { 3, 10 };
static const int64_t exist_e[] = { 1, 10, 2, 20, 3, 30, 4, 40 };
static const int64_t exist_f[] = { 10, 0, 30, 0 };
static const int64_t count_e[] = { 1, 10, 2, 10, 3, 20, 4, 30, 5, 30 };
static const int64_t chain_a[] = { 1, 10, 2, 20, 3, 30, 4, 10 };
static const int64_t chain_b[] = { 10, 100, 20, 200, 30, 300 };
static const int64_t chain_c[] = { 100, 7, 200, 8, 300, 9, 100, 6 };

/*
 * #2134: a plain step at more than one worker returns the model one worker
 * returns for a non-recursive stratum that joins, negates, tests membership
 * or aggregates, on the first step and after an earlier step or snapshot.
 */
static void
test_multi_atom_plain_step_matches_one_worker(void)
{
    const seq_op_t step_twice[] = {
        { OP_INSERT, "e", 1, 2, v78 }, { OP_STEP, NULL, 0, 0, NULL },
        { OP_INSERT, "e", 1, 1, v9 }, { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t snapshot_then_step[] = {
        { OP_INSERT, "e", 1, 2, v78 }, { OP_SNAPSHOT, NULL, 0, 0, NULL },
        { OP_INSERT, "e", 1, 1, v9 }, { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t one_step[] = {
        { OP_INSERT, "e", 1, 3, v789 }, { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t two_heads[] = {
        { OP_INSERT, "e", 1, 3, v789 }, { OP_STEP, NULL, 0, 0, NULL },
        { OP_INSERT, "e", 1, 1, v10 }, { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t join_with_removal[] = {
        { OP_INSERT, "e", 2, 4, join_e }, { OP_INSERT, "f", 2, 4, join_f },
        { OP_STEP, NULL, 0, 0, NULL },
        { OP_REMOVE, "e", 2, 1, join_e_gone }, { OP_STEP, NULL, 0, 0, NULL },
        { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t existence[] = {
        { OP_INSERT, "e", 2, 4, exist_e }, { OP_INSERT, "f", 2, 2, exist_f },
        { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t counted[] = {
        { OP_INSERT, "e", 2, 5, count_e }, { OP_STEP, NULL, 0, 0, NULL },
    };
    const seq_op_t chained[] = {
        { OP_INSERT, "a", 2, 4, chain_a }, { OP_INSERT, "b", 2, 3, chain_b },
        { OP_INSERT, "c", 2, 4, chain_c }, { OP_STEP, NULL, 0, 0, NULL },
    };
    static const uint32_t widths[] = { 2, 4 };
    bool ok = true;

    TEST("plain step joins, negates and aggregates as one worker does");
    knob_set("1");
    for (size_t w = 0; w < sizeof(widths) / sizeof(widths[0]); w++) {
        uint32_t W = widths[w];
        ok = matches_one_worker("cross, step twice", cross_src, "p", W,
                step_twice, 4) && ok;
        ok = matches_one_worker("cross, snapshot then step", cross_src, "p",
                W, snapshot_then_step, 4) && ok;
        ok = matches_one_worker("cross, one step", cross_src, "p", W,
                one_step, 2) && ok;
        ok = matches_one_worker("two heads", two_heads_src, "p", W,
                two_heads, 4) && ok;
        ok = matches_one_worker("join with removal", join_src, "p", W,
                join_with_removal, 6) && ok;
        ok = matches_one_worker("negation", neg_src, "c", W, existence, 3)
            && ok;
        ok = matches_one_worker("membership", semi_src, "c", W, existence,
                3) && ok;
        ok = matches_one_worker("count", count_src, "c", W, counted, 2)
            && ok;
        ok = matches_one_worker_sip("semijoin chain", chain_src, true, "p",
                W, chained, 4) && ok;
    }
    knob_restore();
    if (ok)
        PASS();
    else
        FAIL("a multi-worker plain step differs from one worker");
}

/*
 * A rule that reads a nullary relation holding a row derives that row's
 * consequence at more than one worker as at one (#2134), and a nullary
 * head derived from a partitioned relation stays one row.  A nullary
 * relation without rows does not stop the stratum being partitioned.
 */
static void
test_nullary_input_plain_step_matches_one_worker(void)
{
    /* The flag row is inserted below; the session loads no facts. */
    static const char flag_src[] =
        ".decl e(x: int64)\n.decl flag()\n.decl p(x: int64)\n"
        "p(100) :- flag().\np(x) :- e(x).\n";
    static const char ok_src[] =
        ".decl e(x: int64)\n.decl ok()\nok() :- e(_).\n";
    static const int64_t v123[] = { 1, 2, 3 };
    static const int64_t no_columns[1] = { 0 };
    const seq_op_t ops[] = {
        { OP_INSERT, "flag", 0, 1, no_columns },
        { OP_INSERT, "e", 1, 3, v123 }, { OP_STEP, NULL, 0, 0, NULL },
    };
    static const uint32_t widths[] = { 2, 4 };
    bool ok = true;

    TEST("plain step reads a nonempty nullary relation as one worker does");
    knob_set("1");
    for (size_t w = 0; w < sizeof(widths) / sizeof(widths[0]); w++) {
        ok = matches_one_worker("nullary input", flag_src, "p", widths[w],
                ops, 3) && ok;
        ok = matches_one_worker("nullary head", ok_src, "ok", widths[w],
                ops + 1, 2) && ok;
    }
    /* A nullary relation without rows leaves the stratum partitioned. */
    {
        static const char empty_flag_src[] =
            ".decl e(x: int64)\n.decl flag()\n.decl p(x: int64)\n"
            "p(100) :- flag().\np(x) :- e(x).\n";
        seq_result_t got = run_sequence(empty_flag_src, false, "p", 4,
                ops + 1, 2);
        ok = matches_one_worker("empty nullary input", empty_flag_src, "p",
                4, ops + 1, 2) && ok;
        if (got.rc != 0 || got.nreads != 1 || got.max_active_workers[0] <= 1) {
            printf("\n    empty nullary input: not partitioned");
            ok = false;
        }
    }
    knob_restore();
    if (ok)
        PASS();
    else
        FAIL("a multi-worker plain step differs from one worker");
}

/*
 * A stratum whose rule reads one row per derivation stays partitioned: the
 * projection still matches one worker, including a removal whose value
 * another row still derives, and the step ran on more than one worker.
 */
static void
test_row_local_plain_step_stays_partitioned(void)
{
    static const char src[] =
        ".decl e(x: int64, y: int64)\n.decl c(y: int64)\n"
        "c(y) :- e(x, y).\n";
    static const int64_t rows[] = { 1, 5, 2, 5, 3, 6, 4, 7 };
    static const int64_t gone[] = { 1, 5 };
    const seq_op_t ops[] = {
        { OP_INSERT, "e", 2, 4, rows }, { OP_STEP, NULL, 0, 0, NULL },
        { OP_REMOVE, "e", 2, 1, gone }, { OP_STEP, NULL, 0, 0, NULL },
    };
    seq_result_t got;
    bool ok;

    TEST("row-local plain step stays partitioned and matches one worker");
    knob_set("1");
    ok = matches_one_worker("projection with removal", src, "c", 4, ops, 4);
    got = run_sequence(src, false, "c", 4, ops, 4);
    knob_restore();
    /* Read right after each step: a snapshot resets the counter. */
    ok = ok && got.rc == 0 && got.nreads == 2
        && got.max_active_workers[0] > 1;
    if (ok)
        PASS();
    else
        FAIL("row-local stratum fell back or differs from one worker");
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * test_w1_fast_path:
 * W=1 must produce the same result as a direct col_eval_stratum call.
 * Verifies the zero-overhead delegation path.
 */
static int
test_w1_fast_path(void)
{
    TEST("W=1 fast path delegates to col_eval_stratum");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_copy_session(1, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    /* Insert 6 edges */
    int64_t rows[] = { 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 1 };
    if (insert_edges(sess, rows, 6) != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("insert");
        return 1;
    }

    /* Run step: triggers col_eval_stratum_tdd with W=1 */
    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    uint32_t nrows = count_rows(sess, "edge_t");
    if (nrows != 6) {
        cleanup_session(sess, plan, prog);
        FAIL("expected 6 rows in edge_t");
        return 1;
    }

    cleanup_session(sess, plan, prog);
    PASS();
    return 0;
}

/*
 * test_w2_correctness:
 * W=2 must produce the same row count as W=1 baseline.
 */
static int
test_w2_correctness(void)
{
    TEST("W=2 non-recursive correctness matches W=1 baseline");

    /* Build W=1 baseline */
    wl_plan_t *plan1 = NULL;
    wirelog_program_t *prog1 = NULL;
    wl_col_session_t *sess1 = make_copy_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }

    /* Build W=2 session */
    wl_plan_t *plan2 = NULL;
    wirelog_program_t *prog2 = NULL;
    wl_col_session_t *sess2 = make_copy_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    int64_t rows[] = {
        1, 2, 2, 3, 3, 4, 4, 5, 5, 6,
        6, 7, 7, 8, 8, 9, 9, 10, 10, 1
    };
    uint32_t nrows = 10;

    insert_edges(sess1, rows, nrows);
    insert_edges(sess2, rows, nrows);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "edge_t");
    uint32_t cnt2 = count_rows(sess2, "edge_t");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 row count differs from W=1 baseline");
        return 1;
    }

    PASS();
    return 0;
}

/*
 * test_w4_correctness:
 * W=4 must produce the same row count as W=1 baseline.
 */
static int
test_w4_correctness(void)
{
    TEST("W=4 non-recursive correctness matches W=1 baseline");

    wl_plan_t *plan1 = NULL;
    wirelog_program_t *prog1 = NULL;
    wl_col_session_t *sess1 = make_copy_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }

    wl_plan_t *plan4 = NULL;
    wirelog_program_t *prog4 = NULL;
    wl_col_session_t *sess4 = make_copy_session(4, &plan4, &prog4);
    if (!sess4) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=4 session create");
        return 1;
    }

    /* 20 edges */
    int64_t rows[40];
    for (int i = 0; i < 20; i++) {
        rows[i * 2] = (int64_t)(i + 1);
        rows[i * 2 + 1] = (int64_t)((i + 1) % 20 + 1);
    }

    insert_edges(sess1, rows, 20);
    insert_edges(sess4, rows, 20);

    int rc1 = wl_session_step(&sess1->base);
    int rc4 = wl_session_step(&sess4->base);

    uint32_t cnt1 = count_rows(sess1, "edge_t");
    uint32_t cnt4 = count_rows(sess4, "edge_t");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess4, plan4, prog4);

    if (rc1 != 0 || rc4 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt4) {
        FAIL("W=4 row count differs from W=1 baseline");
        return 1;
    }

    PASS();
    return 0;
}

/*
 * test_empty_edb:
 * Empty EDB must produce empty output regardless of W.
 */
static int
test_empty_edb(void)
{
    TEST("empty EDB produces empty output for W=2");

    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t *sess = make_copy_session(2, &plan, &prog);
    if (!sess) {
        FAIL("session create");
        return 1;
    }

    /* No inserts: empty EDB */
    int rc = wl_session_step(&sess->base);
    if (rc != 0) {
        cleanup_session(sess, plan, prog);
        FAIL("session step failed");
        return 1;
    }

    uint32_t nrows = count_rows(sess, "edge_t");
    cleanup_session(sess, plan, prog);

    if (nrows != 0) {
        FAIL("expected 0 rows from empty EDB");
        return 1;
    }

    PASS();
    return 0;
}

/*
 * test_w2_large:
 * 100 edges with W=2: row count must match W=1 baseline.
 */
static int
test_w2_large(void)
{
    TEST("W=2 large input (100 edges) matches W=1 baseline");

    wl_plan_t *plan1 = NULL;
    wirelog_program_t *prog1 = NULL;
    wl_col_session_t *sess1 = make_copy_session(1, &plan1, &prog1);
    if (!sess1) {
        FAIL("baseline session create");
        return 1;
    }

    wl_plan_t *plan2 = NULL;
    wirelog_program_t *prog2 = NULL;
    wl_col_session_t *sess2 = make_copy_session(2, &plan2, &prog2);
    if (!sess2) {
        cleanup_session(sess1, plan1, prog1);
        FAIL("W=2 session create");
        return 1;
    }

    const uint32_t nedges = 100;
    int64_t *rows = (int64_t *)malloc(nedges * 2 * sizeof(int64_t));
    if (!rows) {
        cleanup_session(sess1, plan1, prog1);
        cleanup_session(sess2, plan2, prog2);
        FAIL("malloc");
        return 1;
    }
    for (uint32_t i = 0; i < nedges; i++) {
        rows[i * 2] = (int64_t)(i + 1);
        rows[i * 2 + 1] = (int64_t)((i * 7 + 3) % nedges + 1);
    }

    insert_edges(sess1, rows, nedges);
    insert_edges(sess2, rows, nedges);
    free(rows);

    int rc1 = wl_session_step(&sess1->base);
    int rc2 = wl_session_step(&sess2->base);

    uint32_t cnt1 = count_rows(sess1, "edge_t");
    uint32_t cnt2 = count_rows(sess2, "edge_t");

    cleanup_session(sess1, plan1, prog1);
    cleanup_session(sess2, plan2, prog2);

    if (rc1 != 0 || rc2 != 0) {
        FAIL("session step failed");
        return 1;
    }
    if (cnt1 != cnt2) {
        FAIL("W=2 large: row count differs from W=1 baseline");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("TDD Non-Recursive Distributed Evaluator Tests\n");
    printf("=============================================\n");

    test_w1_fast_path();
    test_w2_correctness();
    test_w4_correctness();
    test_empty_edb();
    test_w2_large();
    test_multi_atom_plain_step_matches_one_worker();
    test_row_local_plain_step_stays_partitioned();
    test_nullary_input_plain_step_matches_one_worker();

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf("\n");

    return tests_failed > 0 ? 1 : 0;
}
