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
        ".decl meta(x: int32, y: int32)\n"
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
    int64_t meta[] = { 1, 3 };
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
    rc = wl_session_insert(base, "meta", meta, 1, 2);
    failures += expect(rc == 0, "meta insertion succeeds");
    check.relation = session_find_rel(session, "edge");
    failures += expect(check.relation != NULL, "edge relation exists");

    /* Qualify the operation-local bundle's nested failure and teardown
     * contract on real session-owned relations.  A dependency denial must
     * unwind the primary arrangement probe before the caller can retry. */
    col_rel_t *meta_rel = session_find_rel(session, "meta");
    uint32_t key_cols[] = { 0 };
    col_arrangement_t *edge_arr = col_session_get_arrangement(
        base, "edge", key_cols, 1);
    col_arr_entry_t *edge_entry = NULL;
    if (edge_arr) {
        for (uint32_t i = 0; i < session->arr_count; i++) {
            if (&session->arr_entries[i].arr == edge_arr) {
                edge_entry = &session->arr_entries[i];
                break;
            }
        }
    }
    failures += expect(meta_rel != NULL && edge_arr != NULL
            && edge_entry != NULL, "nested bundle setup");
    if (meta_rel && edge_arr && edge_entry) {
        col_arrangement_probe_bundle_t bundle = { 0 };
        col_arrangement_probe_t *primary = NULL;
        wl_columnar_source_access_writer_t meta_writer = { 0 };
        col_arrangement_probe_bundle_init(&bundle);
        rc = col_arrangement_probe_bundle_acquire_primary(
            &bundle, base, check.relation, key_cols, 1, &primary);
        failures += expect(rc == 0 && primary != NULL,
                "primary bundle probe acquires");
        rc = wl_columnar_source_access_writer_acquire(
            &meta_rel->source_access, &meta_writer);
        failures += expect(rc == 0, "dependency writer setup");
        if (rc == 0) {
            rc = col_arrangement_probe_bundle_acquire_dependency(
                &bundle, meta_rel);
            failures += expect(rc == EBUSY && bundle.count == 0
                    && bundle.dependency_count == 0
                    && primary != NULL && !primary->active
                    && atomic_load_explicit(&check.relation->source_access.state,
                        memory_order_acquire) == 0
                    && edge_entry->pin_count == 0,
                "nested dependency denial rolls back primary");
            failures += expect(wl_columnar_source_access_writer_release(
                &meta_writer) == 0, "dependency writer release");
        }
        failures += expect(col_arrangement_probe_bundle_release(&bundle) == 0,
                "rolled-back bundle remains releasable");

        col_arrangement_probe_bundle_init(&bundle);
        rc = col_arrangement_probe_bundle_acquire_primary(
            &bundle, base, check.relation, key_cols, 1, &primary);
        failures += expect(rc == 0, "primary retry acquires");
        rc = col_arrangement_probe_bundle_acquire_dependency(&bundle,
            meta_rel);
        failures += expect(rc == 0 && bundle.dependency_count == 1,
                "dependency retry acquires");
        failures += expect(wl_session_insert(base, "edge", edge, 1, 2)
                == EBUSY, "session mutation is excluded by active bundle");
        failures += expect(col_arrangement_probe_bundle_release(&bundle) == 0,
                "nested bundle release succeeds");
        wl_columnar_source_access_writer_t edge_writer = { 0 };
        failures += expect(wl_columnar_source_access_writer_acquire(
            &check.relation->source_access, &edge_writer) == 0,
            "edge writer succeeds after bundle release");
        failures += expect(wl_columnar_source_access_writer_release(
            &edge_writer) == 0, "edge writer release after bundle");
    }

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
