/* Columnar teardown must drain queued work before owned state is released. */

#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/passes/fusion.h"
#include "wirelog/passes/jpp.h"
#include "wirelog/passes/sip.h"
#include "wirelog/session.h"
#include "wirelog/wirelog.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    col_rel_t *relation;
    bool completed;
    bool relation_was_live;
} queued_check_t;

static int
expect(bool condition, const char *message)
{
    if (!condition)
        fprintf(stderr, "FAIL: %s\n", message);
    return condition ? 0 : 1;
}

static void
check_relation_before_teardown(void *opaque)
{
    queued_check_t *check = (queued_check_t *)opaque;

    check->relation_was_live = check->relation
        && check->relation->name
        && strcmp(check->relation->name, "edge") == 0
        && check->relation->nrows == 1;
    check->completed = true;
}

static wl_plan_t *
build_plan(wirelog_program_t **program_out)
{
    const char *source =
        ".decl edge(x: int32, y: int32)\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(x, y) :- edge(x, y).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    wl_plan_t *plan = NULL;

    if (!program)
        return NULL;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    if (wl_plan_from_program(program, &plan) != 0) {
        wirelog_program_free(program);
        return NULL;
    }
    *program_out = program;
    return plan;
}

int
main(void)
{
    wirelog_program_t *program = NULL;
    wl_plan_t *plan = build_plan(&program);
    wl_session_t *base = NULL;
    wl_col_session_t *session;
    wl_col_session_t worker = { 0 };
    queued_check_t check = { 0 };
    int64_t edge[] = { 1, 2 };
    int failures = 0;
    int rc;

    failures += expect(plan != NULL, "plan builds");
    if (!plan)
        return 1;
    rc = wl_session_create(wl_backend_columnar(), plan, 2, &base);
    failures += expect(rc == 0 && base != NULL, "columnar session creates");
    if (rc != 0 || !base) {
        wl_plan_free(plan);
        wirelog_program_free(program);
        return 1;
    }
    session = COL_SESSION(base);

    failures += expect(session->base.operation_admission != NULL
            && session->base.owns_operation_admission,
            "coordinator owns operation admission");
    rc = col_worker_session_create(session, 0, NULL, 0, &worker);
    failures += expect(rc == 0, "worker session creates");
    if (rc == 0) {
        failures += expect(worker.base.operation_admission == NULL
                && !worker.base.owns_operation_admission,
                "worker does not own coordinator admission");
        col_worker_session_destroy(&worker);
    }

    rc = wl_session_insert(base, "edge", edge, 1, 2);
    failures += expect(rc == 0, "edge insertion succeeds");
    check.relation = session_find_rel(session, "edge");
    failures += expect(check.relation != NULL, "edge relation exists");
    rc = wl_columnar_session_ensure_workqueue(session, 2);
    failures += expect(rc == 0 && session->wq != NULL,
            "workqueue creates");
    if (rc == 0 && session->wq)
        rc = wl_workqueue_submit(session->wq,
                check_relation_before_teardown, &check);
    failures += expect(rc == 0, "submit-only work item queues");
    failures += expect(!check.completed,
            "submit-only item remains pending before destroy");

    wl_session_destroy(base);
    failures += expect(check.completed,
            "destroy drains queued work item");
    failures += expect(check.relation_was_live,
            "queued work completes before relation teardown");

    wl_plan_free(plan);
    wirelog_program_free(program);
    return failures != 0;
}
