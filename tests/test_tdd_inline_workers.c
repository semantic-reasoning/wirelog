/* Actual multi-worker TDD dispatch over an inline compound relation. */
#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdbool.h>
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
    return _putenv_s(name, value ? value : "");
}
#define setenv wl_test_setenv_
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}
#define unsetenv wl_test_unsetenv_
#endif

extern void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *, uint32_t *,
    uint32_t *, uint32_t *, uint32_t *, uint32_t *, uint32_t *, uint32_t *,
    const char **);

/* Enable the production TDD submission path while keeping this test's
 * dispatch hook deterministic. */
int
wl_columnar_eval_test_submit(wl_work_queue_t *wq, void (*fn)(void *),
    void *ctx)
{
    return wl_workqueue_submit(wq, fn, ctx);
}

void
wl_columnar_eval_test_before_worker_cleanup(wl_col_session_t *coord)
{
    (void)coord;
}

typedef struct {
    int64_t rows;
    bool valid;
    bool first_edge_present;
    bool seen[101][101];
} oracle_t;

static void
collect_reach(const char *relation, const int64_t *row, uint32_t ncols,
    void *arg)
{
    oracle_t *oracle = (oracle_t *)arg;
    if (!relation || strcmp(relation, "reach") != 0 || ncols != 4
        || row[0] < 0 || row[0] > 100 || row[1] > 100
        || row[0] >= row[1]
        || (!oracle->first_edge_present && row[0] == 0)
        || row[2] != 7 || row[3] != 8
        || oracle->seen[row[0]][row[1]])
        oracle->valid = false;
    else
        oracle->seen[row[0]][row[1]] = true;
    oracle->rows++;
}

static int
snapshot_oracle(wl_session_t *sess, int64_t expected,
    bool first_edge_present)
{
    oracle_t oracle = { .rows = 0, .valid = true,
                        .first_edge_present = first_edge_present };
    int rc = wl_session_snapshot(sess, collect_reach, &oracle);
    if (rc != 0 || !oracle.valid || oracle.rows != expected)
        return 1;
    for (int x = 0; x <= 100; x++) {
        for (int y = x + 1; y <= 100; y++) {
            bool wanted = first_edge_present || x != 0;
            if (oracle.seen[x][y] != wanted)
                return 1;
        }
    }
    return 0;
}

static bool
relation_shape(wl_col_session_t *session, const char *name)
{
    for (uint32_t i = 0; i < session->nrels; i++) {
        col_rel_t *rel = session->rels[i];
        if (rel && rel->name && strcmp(rel->name, name) == 0) {
            return rel->ncols == 4;
        }
    }
    return false;
}

static void
count_rows(const char *relation, const int64_t *row, uint32_t ncols, void *arg)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (*(uint64_t *)arg)++;
}

/* Keep the dispatch assertion independent from compound-output eligibility;
 * the inline lifecycle below verifies the compound path itself. */
static int
run_scalar_dispatch_probe(void)
{
    const char *source = ".decl edge(x: int32, y: int32)\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(x,y) :- edge(x,y).\n"
        "reach(x,z) :- reach(x,y), edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    if (!program)
        return 1;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    int rc = wl_plan_from_program(program, &plan);
    if (rc != 0 || !plan)
        goto fail;
    plan_fixture_hold(program);
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &session);
    if (rc != 0)
        goto fail;
    int64_t rows[200];
    for (uint32_t i = 0; i < 100; i++) {
        rows[i * 2] = (int64_t)i;
        rows[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(session, "edge", rows, 100, 2);
    uint64_t count = 0;
    if (rc == 0)
        rc = wl_session_snapshot(session, count_rows, &count);
    wl_col_session_t *columnar = COL_SESSION(session);
    if (rc == 0 && (count != 5050
        || columnar->tdd_audit.selected_workers != 8
        || columnar->tdd_audit.submitted_tasks == 0
        || columnar->tdd_audit.completed_rounds == 0)) {
        fprintf(stderr,
            "probe rc=%d count=%" PRIu64 " workers=%u submitted=%" PRIu64
            " rounds=%" PRIu64 "\n",
            rc, count, columnar->tdd_audit.selected_workers,
            columnar->tdd_audit.submitted_tasks,
            columnar->tdd_audit.completed_rounds);
        rc = EIO;
    }
    wl_session_destroy(session);
    wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
fail:
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    else if (program)
        wirelog_program_free(program);
    return 1;
}

static int
run_lifecycle(uint32_t workers, bool require_dispatch)
{
    const char *source =
        ".decl edge(x: int32, y: int32)\n"
        ".decl reach(x: int32, y: int32, p: pair/2 inline)\n"
        "reach(x,y,7,8) :- edge(x,y).\n"
        "reach(x,z,a,b) :- reach(x,y,a,b), edge(y,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    int rc;

    if (!program) {
        return 1;
    }
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    rc = wl_plan_from_program(program, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(program);
        return 1;
    }
    plan_fixture_hold(program);
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &session);
    if (rc != 0 || !session) {
        goto cleanup;
    }
    int64_t facts[200];
    for (uint32_t i = 0; i < 100; i++) {
        facts[i * 2] = (int64_t)i;
        facts[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(session, "edge", facts, 100, 2);
    if (rc == 0)
        rc = snapshot_oracle(session, 5050, true);
    if (rc != 0) {
        goto cleanup;
    }

    /* The derived relation is the authoritative inline storage schema. */
    if (!relation_shape(COL_SESSION(session), "reach")) {
        rc = EIO;
        goto cleanup;
    }

    uint32_t recursive = 0, executed = 0, fallback = 0;
    uint32_t ineligible = 0, no_exchange = 0, unsafe = 0, adaptive = 0;
    const char *reason = NULL;
    wl_columnar_session_get_tdd_decision_stats(session, &recursive, &executed,
        &fallback, &ineligible, &no_exchange, &unsafe, &adaptive, &reason);
    wl_col_session_t *columnar = COL_SESSION(session);
    if (require_dispatch &&
        (recursive == 0 || executed == 0 || adaptive != 0)) {
        rc = EIO;
        goto cleanup;
    }
    if (require_dispatch
        && (columnar->tdd_audit.selected_workers != workers
        || columnar->tdd_audit.submitted_tasks == 0
        || columnar->tdd_audit.completed_rounds == 0
        || (columnar->tdd_audit.replay
        && strcmp(columnar->tdd_audit.replay,
        "owner_tiny_frontier") != 0))) {
        rc = EIO;
        goto cleanup;
    }

    rc = wl_session_remove(session, "edge", facts, 1, 2);
    if (rc == 0)
        rc = snapshot_oracle(session, 4950, false);
    if (rc == 0)
        rc = wl_session_insert(session, "edge", facts, 1, 2);
    if (rc == 0)
        rc = snapshot_oracle(session, 5050, true);
cleanup:
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
}

int
main(void)
{
    const char *old_threshold = getenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    char *saved_threshold = old_threshold ? strdup(old_threshold) : NULL;
    const char *old_profile = getenv("WIRELOG_TDD_STRATUM_PROFILE");
    char *saved_profile = old_profile ? strdup(old_profile) : NULL;
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    int rc = run_lifecycle(1, false);
    if (rc == 0)
        rc = run_lifecycle(8, false);
    if (rc == 0)
        rc = run_scalar_dispatch_probe();
    if (saved_threshold) {
        setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", saved_threshold, 1);
        free(saved_threshold);
    } else {
        unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    }
    if (saved_profile) {
        setenv("WIRELOG_TDD_STRATUM_PROFILE", saved_profile, 1);
        free(saved_profile);
    } else {
        unsetenv("WIRELOG_TDD_STRATUM_PROFILE");
    }
    printf("tdd inline workers lifecycle W1/W8: %s\n",
        rc == 0 ? "PASS" : "FAIL");
    return rc;
}
