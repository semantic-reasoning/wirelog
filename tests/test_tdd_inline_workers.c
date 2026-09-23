/* Actual multi-worker TDD dispatch over an inline compound relation. */
#define _POSIX_C_SOURCE 200809L

#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"
#include "plan_fixture.h"

#include <stdbool.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* glibc's C11 <threads.h> trampoline is not a reliable ThreadSanitizer
 * runtime surface.  Keep this worker test on the POSIX TSan gate, while the
 * native C11 configuration remains compile-smoke coverage. */
#if defined(__SANITIZE_THREAD__)
#define WL_TEST_THREAD_SANITIZER 1
#elif defined(__clang__)
#if __has_feature(thread_sanitizer)
#define WL_TEST_THREAD_SANITIZER 1
#endif
#endif

#if defined(WL_HAVE_C11_THREADS) && defined(WL_TEST_THREAD_SANITIZER)
#define WL_SKIP_NATIVE_C11_TSAN 1
#endif

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

static bool owner_partial_submit_armed;
static unsigned owner_partial_submit_count;

/* Enable the production TDD submission path while keeping this test's
 * dispatch hook deterministic. */
int
wl_columnar_eval_test_submit(wl_work_queue_t *wq, void (*fn)(void *),
    void *ctx)
{
    if (owner_partial_submit_armed
        && ++owner_partial_submit_count == 2)
        return ENOMEM;
    return wl_workqueue_submit(wq, fn, ctx);
}

/* #1661 U4: stage a refusal at cohort teardown rather than at prior-delta
 * retirement.  test_tdd_recursive's retirement test covers the retire path;
 * this covers the gate below it -- col_worker_session_destroy tearing the
 * worker registry down with checked destroys -- which is what makes the
 * exchange-site session_remove_rel discards safe even on the branches that
 * register nothing after a sweep. */
static bool teardown_hold_armed;
static bool teardown_hold_fired;
static bool teardown_reader_release_safe;
static wl_columnar_source_access_reader_t teardown_reader;
static bool teardown_caller_attempted;
static bool teardown_caller_alias_selected;
static bool teardown_caller_state_ok;
static int teardown_caller_rc;
static uint32_t teardown_caller_cohort_count;
static bool owner_queue_hold_armed;
static bool owner_queue_hold_fired;
static col_rel_t *owner_queue_relation;
static wl_columnar_source_access_reader_t owner_queue_reader;

void
wl_columnar_eval_test_before_worker_cleanup(wl_col_session_t *coord)
{
    if (!teardown_hold_armed || teardown_hold_fired || !coord)
        return;
    /* Gate on a populated cohort.  Every TDD initializer calls
     * tdd_cleanup_workers on entry with tdd_workers_count == 0, so an
     * arm-once flag alone would be consumed before any worker exists and the
     * test would pass without ever holding anything. */
    if (coord->tdd_workers_count == 0)
        return;
    teardown_caller_cohort_count = coord->tdd_workers_count;
    if (teardown_caller_cohort_count != 8)
        return;
    wl_col_session_t *cohort_workers = coord->tdd_workers;
    wl_col_session_t *worker = &coord->tdd_workers[0];
    for (uint32_t i = 0; i < worker->nrels; i++) {
        col_rel_t *rel = worker->rels[i];
        if (!rel || !rel->storage_owner || rel->storage_owner == rel)
            continue;
        if (col_rel_source_reader_acquire_transferable(rel,
            &teardown_reader) != 0)
            continue;
        teardown_hold_fired = true;
        teardown_reader_release_safe = true;
        teardown_caller_alias_selected = true;

        uint32_t worker_id = worker->worker_id;
        uint32_t nrels = worker->nrels;
        col_rel_t **rels = worker->rels;
        uintptr_t held_alias_address = (uintptr_t)rel;
        uintptr_t *slot_addresses = NULL;
        if (nrels == 0 || sizeof(*slot_addresses) > SIZE_MAX / nrels)
            return;
        slot_addresses = malloc((size_t)nrels * sizeof(*slot_addresses));
        if (!slot_addresses)
            return;
        for (uint32_t slot = 0; slot < nrels; slot++)
            slot_addresses[slot] = (uintptr_t)rels[slot];

        teardown_caller_rc = col_worker_session_destroy(worker);
        teardown_caller_attempted = true;

        bool held_alias_still_registered = false;
        bool slots_stable = worker->rels == rels && worker->nrels == nrels;
        if (slots_stable) {
            for (uint32_t slot = 0; slot < nrels; slot++) {
                uintptr_t after = (uintptr_t)worker->rels[slot];
                uintptr_t before = slot_addresses[slot];
                if ((before == 0 && after != 0)
                    || (before != 0 && after != 0 && after != before))
                    slots_stable = false;
                if (slot == i && after == held_alias_address)
                    held_alias_still_registered = true;
            }
        }
        teardown_reader_release_safe = teardown_caller_rc != 0
            && worker->rels == rels && worker->nrels == nrels
            && worker->rels[i] == rel;
        teardown_caller_state_ok = teardown_caller_rc == EBUSY
            && coord->tdd_workers == cohort_workers
            && coord->tdd_workers_count == 8
            && worker == &cohort_workers[0]
            && worker->worker_id == worker_id
            && worker->coordinator == coord
            && worker->teardown_started
            && slots_stable && held_alias_still_registered;
        free(slot_addresses);
        return;
    }
}

void
wl_columnar_eval_test_tdd_worker_start(wl_col_session_t *worker)
{
    /* Worker zero invokes this hook serially across TDD rounds, and the
     * coordinator reads the result only after the workqueue barrier. */
    if (!owner_queue_hold_armed || !worker || worker->worker_id != 0
        || !worker->coordinator
        || !worker->coordinator->tdd_owner_lifetime
        || !worker->coordinator->delta_queue || owner_queue_hold_fired)
        return;
    owner_queue_hold_fired = true;
    wl_col_session_t *coord = worker->coordinator;
    col_rel_t *relation = col_rel_new_auto("queued_retained_delta", 0);
    if (!relation || col_rel_source_reader_acquire_transferable(relation,
        &owner_queue_reader) != 0) {
        col_rel_destroy(relation);
        owner_queue_hold_fired = false;
        return;
    }
    /* Invalid rel_idx forces the payload into the lifetime's reserved
     * overflow slots while preserving the real worker matrix output. */
    int rc = wl_mpsc_enqueue(coord->delta_queue, worker->worker_id,
            relation, worker->current_iteration, UINT32_MAX);
    if (rc != 0) {
        (void)col_rel_source_reader_release(&owner_queue_reader);
        col_rel_destroy(relation);
        owner_queue_hold_fired = false;
        return;
    }
    owner_queue_relation = relation;
    wl_mem_ledger_alloc(&coord->mem_ledger, WL_MEM_SUBSYS_CHANNEL,
        col_rel_transport_bytes(relation));
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

static bool
program_relation_shape(const wirelog_program_t *program, const char *name)
{
    for (uint32_t i = 0; i < program->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &program->relations[i];
        if (rel->name && strcmp(rel->name, name) == 0) {
            return rel->column_count == 3
                   && rel->columns
                   && rel->columns[0].compound_kind
                   == WIRELOG_COMPOUND_KIND_NONE
                   && rel->columns[1].compound_kind
                   == WIRELOG_COMPOUND_KIND_NONE
                   && rel->columns[2].compound_kind
                   == WIRELOG_COMPOUND_KIND_INLINE
                   && rel->columns[2].compound_arity == 2
                   && wl_ir_relation_physical_width(rel) == 4;
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
    if (!program_relation_shape(program, "reach")) {
        wirelog_program_free(program);
        return 1;
    }
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
    if (!require_dispatch
        && (columnar->tdd_audit.selected_workers != 0
        || columnar->tdd_audit.submitted_tasks != 0
        || columnar->tdd_audit.completed_rounds != 0
        || adaptive != 0)) {
        rc = EIO;
        goto cleanup;
    }
    if (require_dispatch
        && (fallback != 0 || ineligible != 0 || no_exchange != 0
        || unsafe != 0 || adaptive != 0
        || (reason && reason[0] != '\0'
        && strcmp(reason, "none") != 0
        && strcmp(reason, "owner_tiny_frontier") != 0))) {
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

/* A reader held across worker teardown must fail the enclosing snapshot, keep
 * the cohort for a retry, and clear once released.  If this ever passes with
 * the snapshot returning 0, a worker relation stayed registered while the
 * call reported success -- the contract #1661 names. */
static int
run_teardown_refusal_gate(void)
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
    wl_col_session_t *columnar = NULL;
    int rc;

    if (!program)
        return 1;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    rc = wl_plan_from_program(program, &plan);
    if (rc != 0 || !plan)
        goto fail;
    plan_fixture_hold(program);
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &session);
    if (rc != 0)
        goto fail;
    columnar = COL_SESSION(session);
    int64_t facts[200];
    for (int i = 0; i < 100; i++) {
        facts[i * 2] = (int64_t)i;
        facts[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(session, "edge", facts, 100, 2);
    if (rc != 0)
        goto cleanup;

    memset(&teardown_reader, 0, sizeof(teardown_reader));
    teardown_hold_armed = true;
    teardown_hold_fired = false;
    teardown_reader_release_safe = false;
    teardown_caller_attempted = false;
    teardown_caller_alias_selected = false;
    teardown_caller_state_ok = false;
    teardown_caller_rc = 0;
    teardown_caller_cohort_count = 0;
    rc = snapshot_oracle(session, 5050, true);
    /* The hook must have run on a populated cohort, or the assertions below
     * would hold for want of anything to refuse. */
    if (!teardown_hold_fired) {
        rc = EIO;
        goto cleanup;
    }
    /* A sanity guard, not the discriminating assertion.  The snapshot
     * reports a generic 1 here both with and without the propagation
     * under test, so this never fires on the mutation below.  It is kept
     * because a snapshot reporting success outright would invalidate
     * everything after it. */
    if (rc == 0) {
        rc = EIO;
        goto cleanup;
    }
    /* This is the assertion that carries the test.  col_worker_session_destroy
     * refuses at session.c's alias pass -- measured, relation_alias_rc == EBUSY
     * with pool_alias_rc, relation_root_rc and pool_root_rc all 0, so the
     * refusal travels out through the FIRST arm of that function's fold, not
     * the relation_root_rc arm.  Dropping that arm makes the caller read the
     * teardown as successful and retire the cohort: tdd_workers_count goes 8
     * -> 0 and the retry below has nothing to run on. */
    if (columnar->tdd_workers_count == 0) {
        /* The cohort was retired despite the refusal -- the regression this
         * gate exists to catch.  The worker relation the reader points at has
         * been freed with it, so drop the reader without touching it rather
         * than turning a clean failure into a use-after-free. */
        memset(&teardown_reader, 0, sizeof(teardown_reader));
        teardown_reader_release_safe = false;
        rc = EIO;
        goto cleanup;
    }
    if (!teardown_caller_attempted || !teardown_caller_alias_selected
        || !teardown_caller_state_ok || teardown_caller_rc != EBUSY
        || teardown_caller_cohort_count != 8
        || columnar->tdd_workers_count != 8) {
        rc = EIO;
        goto cleanup;
    }

    teardown_hold_armed = false;
    if (!teardown_reader_release_safe) {
        rc = EIO;
        goto cleanup;
    }
    int reader_release_rc = col_rel_source_reader_release(&teardown_reader);
    teardown_reader_release_safe = false;
    if (reader_release_rc != 0) {
        rc = EIO;
        goto cleanup;
    }
    wl_col_session_t *caller_retry_worker = &columnar->tdd_workers[0];
    int caller_retry_rc = col_worker_session_destroy(caller_retry_worker);
    if (caller_retry_rc != 0 || caller_retry_worker->coordinator != NULL
        || caller_retry_worker->rels != NULL
        || caller_retry_worker->nrels != 0) {
        rc = EIO;
        goto cleanup;
    }
    rc = snapshot_oracle(session, 5050, true);
    if (rc == 0 && columnar->tdd_workers_count != 0)
        rc = EIO;
cleanup:
    teardown_hold_armed = false;
    /* Release before destroy: col_session_destroy aborts when a worker
     * teardown refuses. */
    if (teardown_reader.owner) {
        if (teardown_reader_release_safe)
            (void)col_rel_source_reader_release(&teardown_reader);
        else
            memset(&teardown_reader, 0, sizeof(teardown_reader));
    }
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    return rc == 0 ? 0 : 1;
fail:
    if (plan)
        wl_plan_free(plan);
    else if (program)
        wirelog_program_free(program);
    return 1;
}

/* Inject one queue-owned payload from a real worker producer. The reader
 * forces owner cleanup to retain the captured overflow slot, then a fresh
 * quiescent retry releases it after the reader is dropped. */
static int
run_owner_queue_lifetime_retry(void)
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
    bool fixture_held = false;
    wl_mem_ledger_snapshot_t baseline_ledger;
    int rc = 0;
    if (!program)
        return 1;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);
    rc = wl_plan_from_program(program, &plan);
    if (rc != 0 || !plan)
        goto fail;
    plan_fixture_hold(program);
    fixture_held = true;
    rc = wl_session_create(wl_backend_columnar(), plan, 8, &session);
    if (rc != 0)
        goto fail;
    wl_col_session_t *columnar = COL_SESSION(session);
    int64_t facts[200];
    for (int i = 0; i < 100; i++) {
        facts[i * 2] = (int64_t)i;
        facts[i * 2 + 1] = (int64_t)i + 1;
    }
    rc = wl_session_insert(session, "edge", facts, 100, 2);
    if (rc != 0)
        goto cleanup;
    wl_mem_ledger_snapshot(&columnar->mem_ledger, &baseline_ledger);

    memset(&owner_queue_reader, 0, sizeof(owner_queue_reader));
    owner_queue_relation = NULL;
    owner_queue_hold_fired = false;
    owner_partial_submit_count = 0;
    owner_partial_submit_armed = true;
    owner_queue_hold_armed = true;
    rc = snapshot_oracle(session, 5050, true);
    owner_partial_submit_armed = false;
    owner_queue_hold_armed = false;
    if (rc == 0 || !owner_queue_hold_fired
        || !owner_queue_reader.owner || !owner_queue_relation
        || !columnar->tdd_owner_lifetime) {
        rc = EPROTO;
        goto cleanup;
    }
    wl_columnar_eval_tdd_owner_lifetime_t *lifetime
        = columnar->tdd_owner_lifetime;
    size_t total = lifetime->matrix_slots + lifetime->overflow_slots;
    bool retained = false;
    for (size_t i = lifetime->matrix_slots; i < total; i++)
        retained |= lifetime->relations[i] == owner_queue_relation;
    if (!retained) {
        rc = EPROTO;
        goto cleanup;
    }
    for (unsigned attempt = 0; attempt < 2; attempt++) {
        if (wl_columnar_session_cleanup_ready(columnar) != EBUSY
            || columnar->tdd_owner_lifetime != lifetime) {
            rc = EPROTO;
            goto cleanup;
        }
    }
    if (col_rel_source_reader_release(&owner_queue_reader) != 0) {
        memset(&owner_queue_reader, 0, sizeof(owner_queue_reader));
        rc = EIO;
        goto cleanup;
    }
    memset(&owner_queue_reader, 0, sizeof(owner_queue_reader));
    rc = wl_columnar_session_cleanup_ready_quiescent(columnar);
    if (rc != 0 || columnar->tdd_owner_lifetime || columnar->delta_queue) {
        rc = EPROTO;
        goto cleanup;
    }
    wl_mem_ledger_snapshot_t after_retry_ledger;
    wl_mem_ledger_snapshot(&columnar->mem_ledger, &after_retry_ledger);
    if (after_retry_ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL]
        != baseline_ledger.subsys_bytes[WL_MEM_SUBSYS_CHANNEL]) {
        rc = EPROTO;
        goto cleanup;
    }
    rc = snapshot_oracle(session, 5050, true);
cleanup:
    owner_partial_submit_armed = false;
    owner_queue_hold_armed = false;
    if (owner_queue_reader.owner)
        (void)col_rel_source_reader_release(&owner_queue_reader);
    if (session)
        wl_session_destroy(session);
    if (plan)
        wl_plan_free(plan);
    if (program && !fixture_held)
        wirelog_program_free(program);
    return rc == 0 ? 0 : 1;
fail:
    if (plan)
        wl_plan_free(plan);
    if (program && !fixture_held)
        wirelog_program_free(program);
    return 1;
}

int
main(void)
{
#if defined(WL_SKIP_NATIVE_C11_TSAN)
    fprintf(stderr,
        "tdd_inline_workers: SKIP: native C11 threads are not a reliable "
        "ThreadSanitizer runtime surface; use -Dthreads=posix for TSan "
        "runtime coverage\n");
    return 77;
#endif
    const char *old_threshold = getenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    char *saved_threshold = old_threshold ? strdup(old_threshold) : NULL;
    const char *old_profile = getenv("WIRELOG_TDD_STRATUM_PROFILE");
    char *saved_profile = old_profile ? strdup(old_profile) : NULL;
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    setenv("WIRELOG_TDD_STRATUM_PROFILE", "1", 1);
    int rc = run_lifecycle(1, false);
    if (rc == 0)
        rc = run_lifecycle(8, true);
    if (rc == 0)
        rc = run_scalar_dispatch_probe();
    if (rc == 0)
        rc = run_teardown_refusal_gate();
    if (rc == 0)
        rc = run_owner_queue_lifetime_retry();
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
