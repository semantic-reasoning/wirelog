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
#include "plan_fixture.h"

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
static int hold_reader_before_worker_cleanup;
static int cleanup_reader_rc;
static wl_columnar_source_access_reader_t cleanup_reader;
static int cleanup_deferred_rc;
static col_rel_t *cleanup_deferred_rel;
static wl_columnar_source_access_reader_t cleanup_deferred_reader;

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

void
wl_columnar_eval_test_before_worker_cleanup(wl_col_session_t *coord)
{
    if (!hold_reader_before_worker_cleanup || !coord
        || coord->tdd_workers_count == 0)
        return;
    hold_reader_before_worker_cleanup = 0;
    col_rel_t *alias = session_find_rel(&coord->tdd_workers[0], "edge");
    cleanup_reader_rc = alias
        ? col_rel_source_reader_acquire(alias, &cleanup_reader) : ENOENT;
    cleanup_deferred_rel = col_rel_new_auto("deferred-tdd-result", 1);
    cleanup_deferred_rc = cleanup_deferred_rel
        ? col_rel_source_reader_acquire(cleanup_deferred_rel,
            &cleanup_deferred_reader) : ENOMEM;
    if (cleanup_deferred_rc == 0)
        cleanup_deferred_rc = wl_columnar_session_defer_relation(
            &coord->tdd_workers[0], cleanup_deferred_rel);
    if (cleanup_deferred_rc != 0) {
        if (cleanup_deferred_reader.owner)
            (void)col_rel_source_reader_release(&cleanup_deferred_reader);
        if (cleanup_deferred_rel) {
            col_rel_destroy(cleanup_deferred_rel);
            cleanup_deferred_rel = NULL;
        }
    }
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

static int
run_snapshot_frames(void)
{
    const char *source =
        ".decl a(x:int32,y:int32)\n.decl b(x:int32,y:int32)\n"
        ".decl r(x:int32,y:int32)\n.decl s(x:int32,y:int32)\n"
        ".decl t(x:int32,y:int32)\n"
        "r(x,y) :- a(x,y).\nr(x,z) :- r(x,y),r(y,z).\n"
        "s(x,y) :- b(x,y).\ns(x,z) :- s(x,y),s(y,z).\n"
        "t(x,y) :- a(x,y).\nt(x,z) :- t(x,y),t(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *prog = wirelog_parse_string(source, &error);
    if (!prog)
        return 1;
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return 1;
    }
    plan_fixture_hold(prog);
    if (plan->stratum_count != 3) {
        wl_plan_free(plan);
        return 1;
    }
    /* These independent SCCs can execute in any order. Choose r,s,t to
     * deliberately exercise a hole in the affected mask (indices 0 and 2). */
    const char *names[] = { "r", "s", "t" };
    wl_plan_stratum_t *strata = (wl_plan_stratum_t *)plan->strata;
    for (uint32_t i = 0; i < 3; i++) {
        for (uint32_t j = i; j < 3; j++) {
            if (strcmp(plan->strata[j].relations[0].name, names[i]) == 0) {
                wl_plan_stratum_t temporary = plan->strata[i];
                strata[i] = plan->strata[j];
                strata[j] = temporary;
                break;
            }
        }
    }
    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        return 1;
    }
    int64_t first[] = { 1, 2 };
    int64_t second[] = { 2, 3 };
    count_ctx_t ctx = { 0 };
    rc = wl_session_insert(sess, "a", first, 1, 2);
    if (rc == 0)
        rc = wl_session_insert(sess, "b", first, 1, 2);
    if (rc == 0)
        rc = wl_session_snapshot(sess, count_cb, &ctx);
    /* Use the incremental insert route without a delta callback, so the
     * snapshot does not fall back to a full evaluation (#1030, #2108). */
    if (rc == 0)
        rc = col_session_insert_incremental(sess, "a", second, 1, 2);
    if (rc == 0)
        rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc == 0)
        rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc == 0 && ctx.count != 17) /* three rows, then seven twice */
        rc = 1;
    wl_session_destroy(sess);
    wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
}

/* Exercise real evaluator paths, without diagnostic-only fault injection.
 * mode 0: long linear owner closure triggers tiny-frontier serial replay.
 * mode 1: wrapper rejects the very first submission; mode 3 rejects fifth.
 * mode 2: an earlier BDX stratum is followed by a rejected three-IDB SCC. */
static int
run_audit_boundary(int mode)
{
    const char *source = mode == 0 || mode == 4
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
    if (rc != 0) {
        wirelog_program_free(prog);
        return 1;
    }
    plan_fixture_hold(prog);
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
        if (mode == 4) {
            memset(&cleanup_reader, 0, sizeof(cleanup_reader));
            cleanup_reader_rc = 0;
            memset(&cleanup_deferred_reader, 0,
                sizeof(cleanup_deferred_reader));
            cleanup_deferred_rel = NULL;
            cleanup_deferred_rc = 0;
            hold_reader_before_worker_cleanup = 1;
        }
        rc = wl_session_snapshot(sess, count_cb, &ctx);
    }
    submission_allowance = -1;
    int ok;
    if (mode == 4) {
        bool retained_worker = col->tdd_workers_count == 8
            && col->tdd_workers[0].rels != NULL
            && col->tdd_workers[0].nrels > 0;
        bool refused_before_replay = rc == EBUSY && cleanup_reader_rc == 0
            && cleanup_deferred_rc == 0
            && cleanup_reader.owner != NULL && retained_worker
            && col->deferred_relation_count == 1
            && col->deferred_relations == cleanup_deferred_rel
            && col->tdd_audit.replay == NULL;
        int release_rc = cleanup_reader.owner
            ? col_rel_source_reader_release(&cleanup_reader) : EINVAL;
        int deferred_release_rc = cleanup_deferred_reader.owner
            ? col_rel_source_reader_release(&cleanup_deferred_reader) : EINVAL;
        hold_reader_before_worker_cleanup = 0;
        int retry_rc = release_rc == 0 && deferred_release_rc == 0
            ? wl_session_snapshot(sess, count_cb, &ctx) : release_rc;
        ok = refused_before_replay && release_rc == 0 && retry_rc == 0
            && deferred_release_rc == 0
            && exact_chain(sess) && col->tdd_workers_count == 0
            && col->deferred_relation_count == 0
            && col->tdd_audit.replay
            && strcmp(col->tdd_audit.replay, "owner_tiny_frontier") == 0;
    } else if (mode == 1 || mode == 3) {
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
    if (mode != 4 && rc == 0) {
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
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return 1;
    }
    plan_fixture_hold(prog);

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
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return 1;
    }
    plan_fixture_hold(prog);

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
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return 1;
    }
    plan_fixture_hold(prog);

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

/* Borrowed metadata is sufficient for this read-only planner query.  The
 * session lookup still owns a lazily allocated hash, released on every exit. */
static int
run_self_join_alignment(bool nested)
{
    char *names[] = { "alpha", "beta", "other" };
    col_rel_t relation = { .name = "r", .ncols = 3, .col_names = names };
    col_rel_t *relations[] = { &relation };
    wl_col_session_t coord = { .rels = relations, .nrels = 1 };
    const char *left[] = { "alpha", "beta" };
    const char *right[] = { "alpha", "beta" };
    uint32_t keys[] = { 0, 1 };
    wl_plan_op_exchange_t exchange = { .key_col_idxs = keys,
                                       .key_col_count = 1 };
    wl_plan_op_t ops[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "r" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "r", .left_keys = left,
          .right_keys = right, .key_count = 1 },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange },
    };
    wl_plan_op_t *children[] = { ops, ops };
    uint32_t counts[] = { 2, 2 };
    wl_plan_op_k_fusion_t fusion = { .k = 2, .k_ops = children,
                                     .k_op_counts = counts };
    wl_plan_op_t outer[] = {
        { .op = WL_PLAN_OP_K_FUSION, .opaque_data = &fusion },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange },
    };
    wl_plan_relation_t plan_rel = { .name = "r", .ops = nested ? outer : ops,
                                    .op_count = nested ? 2 : 3 };
    wl_plan_stratum_t stratum = { .relations = &plan_rel, .relation_count = 1 };
    wl_plan_op_t *exchange_op = nested ? &outer[1] : &ops[2];
    int failed = 1;

#define ALIGNMENT_CHECK(expected) do { \
            if (tdd_stratum_idb_self_join_exchange_aligned(&stratum, &coord) \
                != (expected)) { \
                fprintf(stderr, "alignment nested=%d line=%d expected=%d\n", \
                    nested, __LINE__, (expected)); \
                goto done; \
            } \
} while (0)

    if (!tdd_stratum_has_idb_self_join(&stratum))
        goto done;
    /* Positive controls prevent missing schema/metadata from masking a NULL
     * dereference. Composite index 1 retains valid keys at index 0. */
    for (uint32_t width = 1; width <= 2; width++) {
        ops[1].key_count = exchange.key_col_count = width;
        ALIGNMENT_CHECK(true);
        for (uint32_t k = 0; k < width; k++) {
            const char *saved = left[k];
            left[k] = NULL;
            ALIGNMENT_CHECK(false);
            left[k] = saved;
            saved = right[k];
            right[k] = NULL;
            ALIGNMENT_CHECK(false);
            right[k] = saved;
        }
        ALIGNMENT_CHECK(true);
    }

    left[1] = "other";
    ALIGNMENT_CHECK(false);
    left[1] = "beta";
    right[1] = "other";
    ALIGNMENT_CHECK(false);
    right[1] = "missing";
    ALIGNMENT_CHECK(false);
    right[1] = "beta";
    ops[1].left_keys = NULL;
    ALIGNMENT_CHECK(false);
    ops[1].left_keys = left;
    ops[1].right_keys = NULL;
    ALIGNMENT_CHECK(false);
    ops[1].right_keys = right;
    ops[1].key_count = 0;
    ALIGNMENT_CHECK(false);
    ops[1].key_count = 1;
    ALIGNMENT_CHECK(false);
    ops[1].key_count = 2;

    session_rel_free_hash(&coord);
    coord.nrels = 0;
    ALIGNMENT_CHECK(false);
    coord.nrels = 1;
    relation.col_names = NULL;
    ALIGNMENT_CHECK(false);
    relation.col_names = names;
    relation.ncols = 0;
    ALIGNMENT_CHECK(false);
    relation.ncols = 3;
    exchange_op->opaque_data = NULL;
    ALIGNMENT_CHECK(false);
    exchange_op->opaque_data = &exchange;
    exchange.key_col_idxs = NULL;
    ALIGNMENT_CHECK(false);
    exchange.key_col_idxs = keys;
    exchange.key_col_count = 0;
    ALIGNMENT_CHECK(false);
    exchange.key_col_count = 2;
    ALIGNMENT_CHECK(true);

    /* A duplicate name resolves its first literal match, even when only
     * the later duplicate belongs to the exchange key. */
    ops[1].key_count = exchange.key_col_count = 1;
    names[0] = names[1] = "dup";
    left[0] = right[0] = "dup";
    ALIGNMENT_CHECK(true);
    keys[0] = 1;
    ALIGNMENT_CHECK(false);
    names[0] = NULL;
    ALIGNMENT_CHECK(true);

    /* Numeric-looking keys do not synthesize positional schema names. */
    names[0] = "alpha";
    names[1] = "beta";
    left[0] = right[0] = "col0";
    ALIGNMENT_CHECK(false);
    names[1] = "col0";
    ALIGNMENT_CHECK(true);
    keys[0] = 0;
    ALIGNMENT_CHECK(false);
    failed = 0;
done:
    session_rel_free_hash(&coord);
#undef ALIGNMENT_CHECK
    return failed;
}

/* Check the public internal queries, rather than duplicating their traversal. */
static int
run_plan_traversal(void)
{
    char *names[] = { "col0", "col1" };
    col_rel_t r = { .name = "r", .ncols = 2, .col_names = names };
    col_rel_t s = { .name = "s", .ncols = 2, .col_names = names };
    col_rel_t *registered[] = { &r, &s };
    wl_col_session_t coord = { .rels = registered, .nrels = 2 };
    const char *key0[] = { "col0" }, *key1[] = { "col1" };
    uint32_t column = 0;
    wl_plan_op_exchange_t exchange = { .key_col_idxs = &column,
                                       .key_col_count = 1 };
    wl_plan_op_t seed[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "edb" },
    };
    wl_plan_op_t one[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "r" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "edb", .left_keys = key0,
          .right_keys = key0, .key_count = 1 },
    };
    wl_plan_op_t two[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "r" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "r", .left_keys = key0,
          .right_keys = key0, .key_count = 1 },
    };
    wl_plan_op_t three[] = { two[0], two[1], two[1] };
    wl_plan_op_t *children[] = { seed, one, two };
    uint32_t counts[] = { 1, 2, 2 };
    wl_plan_op_k_fusion_t fusion = { .k = 3, .k_ops = children,
                                     .k_op_counts = counts };
    wl_plan_op_t *later_children[] = { seed, three };
    uint32_t later_counts[] = { 1, 3 };
    wl_plan_op_k_fusion_t later = { .k = 2, .k_ops = later_children,
                                    .k_op_counts = later_counts };
    wl_plan_op_t root[7] = {
        seed[0],
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange },
    };
    wl_plan_op_t other[] = {
        { .op = WL_PLAN_OP_K_FUSION, .opaque_data = &later },
        { .op = WL_PLAN_OP_EXCHANGE, .opaque_data = &exchange },
    };
    wl_plan_relation_t rels[] = {
        { .name = "r", .ops = root, .op_count = 0 },
        { .name = "s", .ops = other, .op_count = 0 },
    };
    wl_plan_stratum_t sp = { .relations = rels, .relation_count = 0 };
    wl_tdd_segment_stats_t stats;
    int failed = 1;

#define TRAVERSAL_CHECK(maximum, self, single, aligned) do { \
            if (stratum_max_idb_body_atoms(&sp) != (maximum) \
                || tdd_stratum_has_idb_self_join(&sp) != (self) \
                || tdd_stratum_single_idb_join_keys_exchange_aligned(&sp) \
                != (single) \
                || tdd_stratum_idb_self_join_exchange_aligned(&sp, &coord) \
                != (aligned)) { \
                fprintf(stderr, "traversal classification line=%d\n", __LINE__); \
                goto done; \
            } \
} while (0)
#define TRAVERSAL_REQUIRE(condition) do { \
            if (!(condition)) { \
                fprintf(stderr, "traversal control line=%d\n", __LINE__); \
                goto done; \
            } \
} while (0)

    TRAVERSAL_CHECK(0, false, true, false);
    sp.relation_count = 1;
    TRAVERSAL_CHECK(0, false, true, false); /* Empty sequence. */
    rels[0].op_count = 2;
    TRAVERSAL_CHECK(0, false, true, false);

    root[0] = one[0];
    root[1] = one[1];
    root[2] = (wl_plan_op_t){ .op = WL_PLAN_OP_EXCHANGE,
                              .opaque_data = &exchange };
    rels[0].op_count = 3;
    TRAVERSAL_CHECK(1, false, true, false);
    root[1].left_keys = key1;
    TRAVERSAL_CHECK(1, false, false, false);
    root[1] = two[1];
    TRAVERSAL_CHECK(2, true, false, true);
    root[1].left_keys = key1;
    TRAVERSAL_CHECK(2, true, false, false);

    /* A later child decides; a second fusion and later relation must also
     * be visited. Child sequences each start with fresh predicate state. */
    root[0] = seed[0];
    root[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_K_FUSION,
                              .opaque_data = &fusion };
    TRAVERSAL_CHECK(2, true, false, true);
    two[1].left_keys = key1;
    TRAVERSAL_CHECK(2, true, false, false);
    two[1].left_keys = key0;
    root[2] = other[0];
    root[3] = other[1];
    rels[0].op_count = 4;
    TRAVERSAL_CHECK(3, true, false, true);
    root[2] = other[1];
    rels[0].op_count = 3;
    sp.relation_count = 2;
    rels[1].op_count = 2;
    TRAVERSAL_CHECK(3, true, false, true);
    three[2].left_keys = key1;
    TRAVERSAL_CHECK(3, true, false, false);
    three[2].left_keys = key0;
    sp.relation_count = 1;

    fusion.k = 1;
    children[0] = one;
    counts[0] = 2;
    TRAVERSAL_CHECK(1, false, true, false);
    one[1].left_keys = key1;
    TRAVERSAL_CHECK(1, false, false, false);
    one[1].left_keys = key0;
    TRAVERSAL_REQUIRE(tdd_stratum_global_read_candidate(&sp));
    tdd_stratum_segment_stats(&sp, &stats);
    TRAVERSAL_REQUIRE(stats.total_segments == 1
        && stats.global_read_segments == 1 && stats.seed_only_segments == 0);

    /* These four queries include top-level evidence even when fusion exists;
    * segment statistics intentionally use only the children in that case. */
    root[0] = three[0];
    root[1] = three[1];
    root[2] = three[2];
    root[3] = (wl_plan_op_t){ .op = WL_PLAN_OP_K_FUSION,
                              .opaque_data = &fusion };
    root[4] = other[1];
    rels[0].op_count = 5;
    children[0] = seed;
    counts[0] = 1;
    TRAVERSAL_CHECK(3, true, false, true);
    tdd_stratum_segment_stats(&sp, &stats);
    TRAVERSAL_REQUIRE(stats.total_segments == 1
        && stats.seed_only_segments == 1 && stats.max_segment_idb_atoms == 0);
    TRAVERSAL_REQUIRE(!tdd_stratum_global_read_candidate(&sp));
    fusion.k = 0;
    TRAVERSAL_CHECK(3, true, false, true);
    root[3].opaque_data = NULL;
    TRAVERSAL_CHECK(3, true, false, true);

    /* Empty children and NULL payloads do not hide a later usable child. */
    root[0] = seed[0];
    root[1] = (wl_plan_op_t){ .op = WL_PLAN_OP_K_FUSION };
    root[2] = (wl_plan_op_t){ .op = WL_PLAN_OP_K_FUSION,
                              .opaque_data = &fusion };
    root[3] = other[1];
    rels[0].op_count = 4;
    fusion.k = 2;
    children[0] = NULL;
    counts[0] = 0;
    children[1] = one;
    counts[1] = 2;
    /* The separate LFTJ precheck rejects the NULL fusion payload. */
    TRAVERSAL_CHECK(1, false, false, false);
    root[1] = seed[0];
    TRAVERSAL_CHECK(1, false, true, false);

    /* Direct children are not recursively expanded by these four queries.
    * The distinct unsupported-LFTJ checker still descends recursively. */
    wl_plan_op_t nested[] = {
        { .op = WL_PLAN_OP_K_FUSION, .opaque_data = &later },
    };
    children[1] = nested;
    counts[1] = 1;
    TRAVERSAL_CHECK(0, false, true, false);
    wl_plan_op_t invalid_lftj[] = { { .op = WL_PLAN_OP_LFTJ } };
    later_children[1] = invalid_lftj;
    later_counts[1] = 1;
    TRAVERSAL_REQUIRE(tdd_stratum_has_unsupported_lftj(&sp));
    TRAVERSAL_CHECK(0, false, false, false);
    failed = 0;
done:
    session_rel_free_hash(&coord);
#undef TRAVERSAL_REQUIRE
#undef TRAVERSAL_CHECK
    return failed;
}

int
main(int argc, char **argv)
{
    int failed = 0;
    decision_stats_t stats;
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    if (argc == 2) {
        if (strcmp(argv[1], "--audit-error") == 0)
            return run_audit_boundary(1);
        if (strcmp(argv[1], "--audit-partial") == 0)
            return run_audit_boundary(3);
        if (strcmp(argv[1], "--audit-cleanup-refusal") == 0)
            return run_audit_boundary(4);
        if (strcmp(argv[1], "--audit-unsafe") == 0)
            return run_audit_boundary(2);
        if (strcmp(argv[1], "--audit-frames-off") == 0)
            unsetenv("WIRELOG_TDD_STRATUM_PROFILE");
        return run_snapshot_frames();
    }
    failed += expect("top-level self-join alignment rejects malformed keys",
            run_self_join_alignment(false) == 0);
    failed += expect("nested self-join alignment rejects malformed keys",
            run_self_join_alignment(true) == 0);
    failed += expect("TDD plan traversal preserves query policies",
            run_plan_traversal() == 0);
    failed += expect("post-dispatch serial replay retains history",
            run_audit_boundary(0) == 0);
    failed += expect("worker cleanup refusal blocks serial replay and retries",
            run_audit_boundary(4) == 0);
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
