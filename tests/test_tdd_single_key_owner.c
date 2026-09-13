/*
 * test_tdd_single_key_owner.c - single-column owner-partitioned TDD coverage
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
#include "plan_fixture.h"

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

extern void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *sess,
    uint32_t *out_recursive_strata, uint32_t *out_executed_strata,
    uint32_t *out_fallback_strata, uint32_t *out_snapshot_ineligible,
    uint32_t *out_no_exchange, uint32_t *out_unsafe_plan,
    uint32_t *out_adaptive_workers, const char **out_last_fallback_reason);

typedef struct count_ctx {
    int64_t count;
} count_ctx_t;

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

static wl_session_t *
make_session(uint32_t workers, wl_plan_t **plan_out)
{
    const char *source =
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return NULL;
    }
    plan_fixture_hold(prog);

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        return NULL;
    }

    *plan_out = plan;
    return sess;
}

static int
insert_chain(wl_session_t *sess, uint32_t nodes)
{
    for (uint32_t i = 1; i < nodes; i++) {
        int64_t row[2] = { (int64_t)i, (int64_t)i + 1 };
        if (wl_session_insert(sess, "edge", row, 1, 2) != 0)
            return 1;
    }
    return 0;
}

static int
relation_equal(wl_session_t *left, wl_session_t *right, const char *name)
{
    col_rel_t *lr = session_find_rel(COL_SESSION(left), name);
    col_rel_t *rr = session_find_rel(COL_SESSION(right), name);
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

static uint32_t
relation_rows(wl_session_t *sess, const char *name)
{
    col_rel_t *rel = session_find_rel(COL_SESSION(sess), name);
    return rel ? rel->nrows : 0;
}

static int
get_owner_stats_ok(wl_session_t *sess)
{
    uint32_t recursive = 0;
    uint32_t executed = 0;
    uint32_t fallback = 0;
    uint32_t snapshot_ineligible = 0;
    uint32_t no_exchange = 0;
    uint32_t unsafe_plan = 0;
    uint32_t adaptive_workers = 0;
    const char *last_reason = NULL;

    wl_columnar_session_get_tdd_decision_stats(sess, &recursive, &executed,
        &fallback, &snapshot_ineligible, &no_exchange, &unsafe_plan,
        &adaptive_workers, &last_reason);

    return recursive == 1 && executed == 1 && fallback == 0
           && snapshot_ineligible == 0 && no_exchange == 0 && unsafe_plan == 0
           && adaptive_workers == 0 && last_reason
           && strcmp(last_reason, "none") == 0;
}

static int
get_snapshot_fallback_stats_ok(wl_session_t *sess)
{
    uint32_t recursive = 0;
    uint32_t executed = 0;
    uint32_t fallback = 0;
    uint32_t snapshot_ineligible = 0;
    uint32_t no_exchange = 0;
    uint32_t unsafe_plan = 0;
    uint32_t adaptive_workers = 0;
    const char *last_reason = NULL;

    wl_columnar_session_get_tdd_decision_stats(sess, &recursive, &executed,
        &fallback, &snapshot_ineligible, &no_exchange, &unsafe_plan,
        &adaptive_workers, &last_reason);

    return recursive == 1 && executed == 0 && fallback == 1
           && snapshot_ineligible == 1 && no_exchange == 0 && unsafe_plan == 0
           && adaptive_workers == 0 && last_reason
           && strcmp(last_reason, "snapshot_ineligible") == 0;
}

static int
run_single_key_owner(void)
{
    wl_plan_t *plan1 = NULL;
    wl_plan_t *plan4 = NULL;
    wl_session_t *w1 = make_session(1, &plan1);
    wl_session_t *w4 = make_session(4, &plan4);
    if (!w1 || !w4)
        goto fail;

    if (insert_chain(w1, 7) != 0 || insert_chain(w4, 7) != 0)
        goto fail;

    count_ctx_t c1 = { 0 };
    count_ctx_t c4 = { 0 };
    if (wl_session_snapshot(w1, count_cb, &c1) != 0
        || wl_session_snapshot(w4, count_cb, &c4) != 0)
        goto fail;

    int ok = relation_rows(w1, "tc") == 21 && relation_equal(w1, w4, "tc")
        && get_owner_stats_ok(w4);

    int64_t row[2] = { 7, 8 };
    if (!ok || wl_session_insert(w1, "edge", row, 1, 2) != 0
        || wl_session_insert(w4, "edge", row, 1, 2) != 0)
        goto done;

    c1.count = 0;
    c4.count = 0;
    if (wl_session_snapshot(w1, count_cb, &c1) != 0
        || wl_session_snapshot(w4, count_cb, &c4) != 0)
        goto fail;

    ok = relation_rows(w1, "tc") == 28 && relation_equal(w1, w4, "tc")
        && get_snapshot_fallback_stats_ok(w4);

done:
    wl_session_destroy(w1);
    wl_session_destroy(w4);
    wl_plan_free(plan1);
    wl_plan_free(plan4);
    return ok ? 0 : 1;

fail:
    if (w1)
        wl_session_destroy(w1);
    if (w4)
        wl_session_destroy(w4);
    if (plan1)
        wl_plan_free(plan1);
    if (plan4)
        wl_plan_free(plan4);
    return 1;
}

int
main(void)
{
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    int rc = run_single_key_owner();
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");

    if (rc == 0) {
        printf("single-key owner TDD ... PASS\n");
        return 0;
    }
    printf("single-key owner TDD ... FAIL\n");
    return 1;
}
