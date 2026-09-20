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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *, uint32_t *,
    uint32_t *, uint32_t *, uint32_t *, uint32_t *, uint32_t *, uint32_t *,
    const char **);

typedef struct {
    int64_t rows;
    bool valid;
} oracle_t;

static void
collect_reach(const char *relation, const int64_t *row, uint32_t ncols,
    void *arg)
{
    oracle_t *oracle = (oracle_t *)arg;
    if (!relation || strcmp(relation, "reach") != 0 || ncols != 4
        || row[0] < 0 || row[1] > 100 || row[0] >= row[1]
        || row[2] != 7 || row[3] != 8)
        oracle->valid = false;
    oracle->rows++;
}

static int
snapshot_oracle(wl_session_t *sess, int64_t expected)
{
    oracle_t oracle = { 0, true };
    int rc = wl_session_snapshot(sess, collect_reach, &oracle);
    return rc == 0 && oracle.valid && oracle.rows == expected ? 0 : 1;
}

static int
run_lifecycle(uint32_t workers, bool require_dispatch)
{
    const char *source =
        ".decl edge(x: int32, y: int32, p: pair/2 inline)\n"
        ".decl reach(x: int32, y: int32, p: pair/2 inline)\n"
        "reach(x,y,a,b) :- edge(x,y,a,b).\n"
        "reach(x,z,a,b) :- reach(x,y,a,b), edge(y,z,a,b).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;
    wl_session_t *session = NULL;
    int rc;

    if (!program)
        return 1;
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
    if (rc != 0 || !session)
        goto cleanup;

    int64_t facts[400];
    for (uint32_t i = 0; i < 100; i++) {
        facts[i * 4] = (int64_t)i;
        facts[i * 4 + 1] = (int64_t)i + 1;
        facts[i * 4 + 2] = 7;
        facts[i * 4 + 3] = 8;
    }
    rc = wl_session_insert(session, "edge", facts, 100, 4);
    if (rc == 0)
        rc = snapshot_oracle(session, 5050);
    if (rc != 0)
        goto cleanup;

    uint32_t recursive = 0, executed = 0, fallback = 0;
    uint32_t ineligible = 0, no_exchange = 0, unsafe = 0, adaptive = 0;
    const char *reason = NULL;
    wl_columnar_session_get_tdd_decision_stats(session, &recursive, &executed,
        &fallback, &ineligible, &no_exchange, &unsafe, &adaptive, &reason);
    wl_col_session_t *columnar = COL_SESSION(session);
    if (recursive == 0 || executed == 0 || adaptive != 0)
        goto cleanup;
    if (require_dispatch
        && (columnar->tdd_audit.selected_workers != workers
        || columnar->tdd_audit.submitted_tasks == 0
        || columnar->tdd_audit.completed_rounds == 0
        || (columnar->tdd_audit.replay
        && strcmp(columnar->tdd_audit.replay,
        "owner_tiny_frontier") != 0)))
        goto cleanup;

    rc = wl_session_remove(session, "edge", facts, 1, 4);
    if (rc == 0)
        rc = snapshot_oracle(session, 4950);
    if (rc == 0)
        rc = wl_session_insert(session, "edge", facts, 1, 4);
    if (rc == 0)
        rc = snapshot_oracle(session, 5050);

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
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    int rc = run_lifecycle(1, false);
    if (rc == 0)
        rc = run_lifecycle(8, true);
    printf("tdd inline workers lifecycle W1/W8: %s\n",
        rc == 0 ? "PASS" : "FAIL");
    return rc;
}
