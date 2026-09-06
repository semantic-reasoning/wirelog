/*
 * test_tdd_decision_stats.c - recursive TDD planner decision counters
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <stdint.h>
#include <errno.h>
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

extern void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *sess,
    uint32_t *out_recursive_strata, uint32_t *out_executed_strata,
    uint32_t *out_fallback_strata, uint32_t *out_snapshot_ineligible,
    uint32_t *out_no_exchange, uint32_t *out_unsafe_plan,
    uint32_t *out_adaptive_workers, const char **out_last_fallback_reason);

typedef struct decision_stats {
    uint32_t recursive;
    uint32_t executed;
    uint32_t fallback;
    uint32_t snapshot_ineligible;
    uint32_t no_exchange;
    uint32_t unsafe_plan;
    uint32_t adaptive_workers;
    const char *last_reason;
} decision_stats_t;

typedef struct count_ctx {
    int64_t count;
} count_ctx_t;

static int submission_allowance = -1;

int
wl_columnar_eval_test_submit(wl_work_queue_t *wq,
    void (*fn)(void *), void *ctx)
{
    if (submission_allowance == 0)
        return -1;
    if (submission_allowance > 0)
        submission_allowance--;
    return wl_workqueue_submit(wq, fn, ctx);
}

/* Verify every tuple and uniqueness, not just a cardinality that a wrong
* relation could accidentally satisfy. The inline chain has 100 edges. */
static int
exact_chain(wl_session_t *sess)
{
    col_rel_t *r = session_find_rel(COL_SESSION(sess), "r");
    bool seen[101][101] = { { false } };
    if (!r || r->ncols != 2 || r->nrows != 5050)
        return 0;
    for (uint32_t i = 0; i < r->nrows; i++) {
        int64_t x = r->columns[0][i];
        int64_t y = r->columns[1][i];
        if (x < 0 || y > 100 || x >= y || seen[x][y])
            return 0;
        seen[x][y] = true;
    }
    return 1;
}

static int
run_bdx_mode(decision_stats_t *stats, int use_step);
static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data);

/* Exercise real evaluator paths, without diagnostic-only fault injection.
 * mode 0: long linear owner closure triggers tiny-frontier serial replay.
 * mode 1: wrapper rejects the very first submission; mode 3 rejects fifth.
 * mode 2: an earlier BDX stratum is followed by a rejected three-IDB SCC. */
static int
run_audit_boundary(int mode)
{
    const char *source = mode == 0
        ? ".decl edge(x:int32,y:int32)\n.decl r(x:int32,y:int32)\n"
        "r(x,y) :- edge(x,y).\nr(x,z) :- r(x,y), edge(y,z).\n"
        : mode == 1 || mode == 3
        ? ".decl edge(x:int32,y:int32)\n.decl r(x:int32,y:int32)\n"
        "r(x,y) :- edge(x,y).\nr(x,z) :- r(x,y), r(y,z).\n"
        : ".decl edge(x:int32,y:int32)\n.decl r(x:int32,y:int32)\n"
        ".decl s(x:int32,y:int32)\n"
        "r(x,y) :- edge(x,y).\nr(x,z) :- r(x,y), r(y,z).\n"
        "s(x,y) :- r(x,y).\ns(x,w) :- s(x,y),s(y,z),s(z,w).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return 1;
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return 1;
    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        return 1;
    }
    int64_t rows[200];
    for (uint32_t i = 0; i < 100; i++) {
        rows[2 * i] = i;
        rows[2 * i + 1] = i + 1;
    }
    wl_col_session_t *col = COL_SESSION(sess);
    submission_allowance = mode == 1 ? 0 : mode == 3 ? 4 : -1;
    rc = wl_session_insert(sess, "edge", rows, 100, 2);
    count_ctx_t ctx = { 0 };
    if (rc == 0) {
        rc = wl_session_snapshot(sess, count_cb, &ctx);
    }
    submission_allowance = -1;
    int ok;
    if (mode == 1 || mode == 3) {
        ok = rc == ENOMEM && col->tdd_audit.selected_workers == 8
            && col->tdd_audit.submitted_tasks == (mode == 1 ? 0u : 4u)
            && col->tdd_audit.completed_rounds == 0;
    } else if (mode == 0) {
        ok = rc == 0 && exact_chain(sess)
            && col->tdd_audit.completed_rounds > 0
            && col->tdd_audit.replay
            && strcmp(col->tdd_audit.replay, "owner_tiny_frontier") == 0;
    } else {
        ok = rc == 0 && exact_chain(sess)
            && col->tdd_executed_strata == 1
            && col->tdd_last_fallback_reason
            == WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN
            && col->tdd_audit.selected_workers == 0
            && col->tdd_audit.submitted_tasks == 0
            && col->tdd_audit.strategy == NULL;
    }
    if (rc == 0) {
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        ok = ok && rc == 0 && col->tdd_audit.submitted_tasks == 0
            && col->tdd_audit.replay == NULL;
    }
    wl_session_destroy(sess);
    wl_plan_free(plan);
    return ok ? 0 : 1;
}
static int
run_bdx_repeated_snapshot(decision_stats_t *first, decision_stats_t *second,
    int64_t *first_count, int64_t *second_count);
static int
run_remove_invalidates_stable_snapshot(decision_stats_t *after_remove,
    int64_t *before_count, int64_t *after_count);

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    count_ctx_t *ctx = (count_ctx_t *)user_data;
    (void)relation;
    (void)row;
    (void)ncols;
    ctx->count++;
}

static int
run_bdx(decision_stats_t *stats)
{
    return run_bdx_mode(stats, 0);
}

static int
run_bdx_step(decision_stats_t *stats)
{
    return run_bdx_mode(stats, 1);
}

static int
run_bdx_mode(decision_stats_t *stats, int use_step)
{
    const char *source =
        ".decl edge(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- edge(x, y).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return 1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return 1;

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        return 1;
    }

    int64_t rows[200];
    for (uint32_t i = 0; i < 100; i++) {
        rows[i * 2] = (int64_t)i;
        rows[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(sess, "edge", rows, 100, 2);
    if (rc == 0 && use_step) {
        rc = wl_session_step(sess);
    } else if (rc == 0) {
        count_ctx_t ctx = { 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        if (rc == 0 && !exact_chain(sess))
            rc = 1;
        if (rc == 0) {
            wl_col_session_t *col = COL_SESSION(sess);
            bool adaptive = strcmp(getenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER"),
                    "1000000000") == 0;
            if (col->tdd_audit.selected_workers != (adaptive ? 1u : 8u)
                || (adaptive ? col->tdd_audit.submitted_tasks != 0
                    : col->tdd_audit.completed_rounds == 0))
                rc = 1;
        }
    }
    if (rc == 0) {
        wl_columnar_session_get_tdd_decision_stats(sess,
            &stats->recursive, &stats->executed, &stats->fallback,
            &stats->snapshot_ineligible, &stats->no_exchange,
            &stats->unsafe_plan, &stats->adaptive_workers,
            &stats->last_reason);
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
}

static int
run_bdx_repeated_snapshot(decision_stats_t *first, decision_stats_t *second,
    int64_t *first_count, int64_t *second_count)
{
    const char *source =
        ".decl edge(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- edge(x, y).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return 1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return 1;

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        return 1;
    }

    int64_t rows[200];
    for (uint32_t i = 0; i < 100; i++) {
        rows[i * 2] = (int64_t)i;
        rows[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(sess, "edge", rows, 100, 2);
    if (rc == 0) {
        count_ctx_t ctx = { 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        *first_count = ctx.count;
    }
    if (rc == 0) {
        wl_columnar_session_get_tdd_decision_stats(sess,
            &first->recursive, &first->executed, &first->fallback,
            &first->snapshot_ineligible, &first->no_exchange,
            &first->unsafe_plan, &first->adaptive_workers,
            &first->last_reason);
    }
    if (rc == 0) {
        count_ctx_t ctx = { 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        *second_count = ctx.count;
    }
    if (rc == 0) {
        wl_columnar_session_get_tdd_decision_stats(sess,
            &second->recursive, &second->executed, &second->fallback,
            &second->snapshot_ineligible, &second->no_exchange,
            &second->unsafe_plan, &second->adaptive_workers,
            &second->last_reason);
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
}

static int
run_remove_invalidates_stable_snapshot(decision_stats_t *after_remove,
    int64_t *before_count, int64_t *after_count)
{
    const char *source =
        ".decl edge(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- edge(x, y).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return 1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return 1;

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        return 1;
    }

    int64_t rows[200];
    for (uint32_t i = 0; i < 100; i++) {
        rows[i * 2] = (int64_t)i;
        rows[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(sess, "edge", rows, 100, 2);
    if (rc == 0) {
        count_ctx_t ctx = { 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        *before_count = ctx.count;
    }
    if (rc == 0)
        rc = wl_session_remove(sess, "edge", rows, 1, 2);
    if (rc == 0) {
        count_ctx_t ctx = { 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx);
        *after_count = ctx.count;
    }
    if (rc == 0) {
        wl_columnar_session_get_tdd_decision_stats(sess,
            &after_remove->recursive, &after_remove->executed,
            &after_remove->fallback,
            &after_remove->snapshot_ineligible,
            &after_remove->no_exchange,
            &after_remove->unsafe_plan,
            &after_remove->adaptive_workers,
            &after_remove->last_reason);
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
}

static int
expect(const char *name, int ok)
{
    if (ok) {
        printf("%s ... PASS\n", name);
        return 0;
    }
    printf("%s ... FAIL\n", name);
    return 1;
}

int
main(void)
{
    int failed = 0;
    decision_stats_t stats;
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    failed += expect("post-dispatch serial replay retains history",
            run_audit_boundary(0) == 0);
    failed += expect("first submission error is not execution",
            run_audit_boundary(1) == 0);
    failed += expect("partial submission and drain is not a complete round",
            run_audit_boundary(3) == 0);
    failed += expect("later rejected stratum has no stale width",
            run_audit_boundary(2) == 0);

    memset(&stats, 0, sizeof(stats));
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    failed += expect("TDD execution counted", run_bdx(&stats) == 0
            && stats.recursive == 1
            && stats.executed == 1
            && stats.fallback == 0
            && stats.adaptive_workers == 0
            && stats.last_reason
            && strcmp(stats.last_reason, "none") == 0);

    memset(&stats, 0, sizeof(stats));
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1000000000", 1);
    failed += expect("adaptive fallback counted", run_bdx(&stats) == 0
            && stats.recursive == 1
            && stats.executed == 0
            && stats.fallback == 1
            && stats.adaptive_workers == 1
            && stats.last_reason
            && strcmp(stats.last_reason, "adaptive_workers") == 0);

    memset(&stats, 0, sizeof(stats));
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1000000000", 1);
    failed += expect("session step leaves snapshot stats untouched",
            run_bdx_step(&stats) == 0
            && stats.recursive == 0
            && stats.executed == 0
            && stats.fallback == 0
            && stats.adaptive_workers == 0
            && stats.last_reason
            && strcmp(stats.last_reason, "none") == 0);

    decision_stats_t first;
    decision_stats_t second;
    int64_t first_count = 0;
    int64_t second_count = 0;
    memset(&first, 0, sizeof(first));
    memset(&second, 0, sizeof(second));
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    failed += expect("clean repeated snapshot reads stable rows",
            run_bdx_repeated_snapshot(&first, &second, &first_count,
            &second_count) == 0
            && first_count > 0
            && second_count == first_count
            && first.recursive == 1
            && first.executed == 1
            && second.recursive == 0
            && second.executed == 0
            && second.fallback == 0
            && second.last_reason
            && strcmp(second.last_reason, "none") == 0);

    decision_stats_t after_remove;
    int64_t before_remove_count = 0;
    int64_t after_remove_count = 0;
    memset(&after_remove, 0, sizeof(after_remove));
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    failed += expect("remove invalidates stable snapshot",
            run_remove_invalidates_stable_snapshot(&after_remove,
            &before_remove_count, &after_remove_count) == 0
            && before_remove_count == 5050
            && after_remove_count == 4950
            && after_remove.recursive == 1
            && (after_remove.executed + after_remove.fallback) == 1);

    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    return failed == 0 ? 0 : 1;
}
