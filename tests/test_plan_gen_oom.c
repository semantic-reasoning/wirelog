/* Regression coverage for issue #1102: planner clone failures are atomic. */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/session_options.h"
#include "../wirelog/session.h"
#include "../wirelog/intern.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

static long fail_at = -1;
static long allocation_calls;
static bool mr1_capture_session, mr1_malloc_failed;
static wl_col_session_t *mr1_session;

void *
__wrap_malloc(size_t size)
{
    if (fail_at >= 0 && allocation_calls++ == fail_at)
        return NULL;
    if (mr1_capture_session && mr1_session) {
        wl_columnar_memory_reservation_t *token =
            &mr1_session->controlled_scalar_view_reservation;
        if (token->identity == token && token->bytes == size
            && atomic_load_explicit(&token->state, memory_order_relaxed)
            == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED) {
            mr1_malloc_failed = true; mr1_capture_session = false;
            mr1_session = NULL;
            return NULL;
        }
    }
    return __real_malloc(size);
}

void *
__wrap_calloc(size_t count, size_t size)
{
    if (fail_at >= 0 && allocation_calls++ == fail_at)
        return NULL;
    void *storage = __real_calloc(count, size);
    if (mr1_capture_session && !mr1_session && count == 1
        && size == sizeof(wl_col_session_t)) mr1_session = storage;
    return storage;
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    if (fail_at >= 0 && allocation_calls++ == fail_at)
        return NULL;
    return __real_realloc(ptr, size);
}

static void session_view_abort(bool serial, int line) {
    fprintf(stderr, "session view serial%d line%d\n", serial, line);
    abort();
}

/* Existing linker wrapping reaches the actual session arena malloc; no new
 * production hook, phase mutation or surrogate allocation failure is used. */
static bool
test_mr1_session_arena_malloc(bool serial)
{
    static const char text[] =
        ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
        "path(x,y):-edge(x,y).\n"
        "path(x,z):-path(x,y),path(y,w),path(w,z).\n";
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(serial?
            ".decl input(x:int64)\n.decl reach(x:int64)\n"
            "reach(x):-input(x). reach(x+1):-reach(x),reach(y),x<3,y<3.":
            text, &error);
    wl_plan_t *plan = NULL;
    if (!program || wl_plan_from_program(program, &plan)) return false;
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    wl_evaluation_control_t *control = NULL;
    if (!ref || wl_evaluation_control_create(0,
        &control)) session_view_abort(serial, __LINE__);
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    if (wl_columnar_memory_reserve_checked(g, 17, &peer)
        != WL_COLUMNAR_MEMORY_ADMISSION_OK
        || !wl_columnar_memory_commit(&peer, &peer)) session_view_abort(serial,
            __LINE__);
    wl_session_options_t options; wl_session_options_init(&options);
    options.evaluation_control = control; options.memory_governor = ref;
    wl_session_t *session = NULL;
    mr1_capture_session = true; mr1_malloc_failed = false;
    int rc = wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options, &session);
    mr1_capture_session = false; mr1_session = NULL;
    if (rc != ENOMEM || session || !mr1_malloc_failed
        || wl_evaluation_control_active_attempt_id(control)) session_view_abort(
            serial, __LINE__);
    wl_evaluation_control_report_t report;
    if (wl_evaluation_control_report(control,
        &report) != ENOENT) session_view_abort(serial, __LINE__);
    int owner;
    if (wl_evaluation_control_attach(control, &owner)
        || wl_evaluation_control_detach(control,
        &owner)) session_view_abort(serial, __LINE__);
    /* Retry creates a real view from the same still-live original plan. */
    if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &session) || !session
        || !((wl_col_session_t *)session)->controlled_scalar_view)
        session_view_abort(serial, __LINE__);
    wl_plan_scalar_view_t *view =
        ((wl_col_session_t *)session)->controlled_scalar_view;
    if (!view->storage_bytes || atomic_load_explicit(&g->reserved_bytes,
        memory_order_relaxed) <= 17 ||
        wl_evaluation_control_active_attempt_id(control) ||
        wl_evaluation_control_report(control,
        &report) != ENOENT) session_view_abort(serial, __LINE__);
    if (wl_session_try_destroy(&session)) session_view_abort(serial, __LINE__);
    if (serial) {
        if (wl_intern_detach_memory_governor(plan->intern,
            ref)) session_view_abort(serial, __LINE__);
        if (atomic_load_explicit(&g->reserved_bytes,
            memory_order_relaxed) != 17)
            session_view_abort(serial, __LINE__);
        atomic_store(&g->usable_bytes, 17);
        if (wl_evaluation_control_reset(control)) session_view_abort(serial,
                __LINE__);
        int denial = wl_session_create_with_options(wl_backend_columnar(), plan,
                1,
                &options, &session);
        if (denial != WL_ERR_MEMORY_BUDGET || session ||
            atomic_load_explicit(&g->reserved_bytes,
            memory_order_relaxed) != 17 ||
            wl_evaluation_control_active_attempt_id(
                control)) session_view_abort(serial, __LINE__);
        atomic_store(&g->usable_bytes, WL_COLUMNAR_MEMORY_MIN_BUDGET);
        if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
            &options,
            &session) ||
            !COL_SESSION(session)->controlled_scalar_view) session_view_abort(
                serial, __LINE__);
        view = COL_SESSION(session)->controlled_scalar_view;
        if (serial) {
            if (view->group_count != 1 || view->branch_count != 2 ||
                view->groups[0].terminal_exchange) session_view_abort(serial,
                    __LINE__);
            int64_t row = 1;
            if (wl_session_insert(session, "input", &row, 1,
                1)) session_view_abort(serial, __LINE__);
            int step = EAGAIN;
            for (unsigned turns = 0; step == EAGAIN && turns < 100000; ++turns)
                step = wl_session_step(session);
            /* Immutable metadata does not remove the current runtime gate. */
            if (step != ENOTSUP) session_view_abort(serial, __LINE__);
            wl_session_options_t raw_options;
            wl_session_options_init(&raw_options);
            raw_options.memory_governor = ref;
            wl_session_t *raw = NULL;
            if (wl_session_create_with_options(wl_backend_columnar(), plan, 1,
                &raw_options, &raw) || wl_session_insert(raw, "input", &row, 1,
                1))
                session_view_abort(serial, __LINE__);
            step = EAGAIN;
            for (unsigned turns = 0; step == EAGAIN && turns < 100000; ++turns)
                step = wl_session_step(raw);
            col_rel_t *reach = session_find_rel(COL_SESSION(raw), "reach");
            if (step || !reach || reach->nrows != 3 || reach->ncols != 1 ||
                reach->columns[0][0] != 1 || reach->columns[0][1] != 2 ||
                reach->columns[0][2] != 3 ||
                wl_session_try_destroy(&raw)) session_view_abort(serial,
                    __LINE__);
        }
        if (wl_session_try_destroy(&session)) session_view_abort(serial,
                __LINE__);
    }
    wl_evaluation_control_release(control);
    wl_plan_free(plan); wirelog_program_free(program);
    if (atomic_load_explicit(&g->reserved_bytes, memory_order_relaxed) != 17
        || !wl_columnar_memory_release(&peer)
        || atomic_load_explicit(&g->reserved_bytes,
        memory_order_relaxed)) session_view_abort(serial, __LINE__);
    wl_columnar_memory_governor_ref_release(ref);
    return true;
}

int
main(void)
{
    static const char source[]
        = ".decl a(x: int64, value: int64)\n"
        ".decl b(x: int64, value: int64)\n"
        ".decl c(x: int64, value: int64)\n"
        ".decl guard(q: int64)\n"
        ".decl inline_a(id:int64,p:pair(symbol,int64) inline)\n"
        ".decl inline_b(p:box/1 inline,x:int64,q:pair/2 inline)\n"
        ".decl out(x: int64, av: int64, bv: int64, cv: int64)\n"
        "out(x, av, bv, cv) :- a(x, av), b(x, bv), c(x, cv), guard(7).\n";
    bool saw_success = false;
    bool saw_injected_failure = false;
    bool saw_lftj = false;
    bool saw_inline = false;

    for (long attempt = 0; attempt < 512; attempt++) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(source, &error);
        if (!program) {
            fprintf(stderr, "parse failed before allocation sweep\n");
            return 1;
        }
        /* Keep the consecutive EDB JOIN chain intact so the planner's LFTJ
         * rewrite and subsequent clone path are exercised. */
        if (wl_fusion_apply(program, NULL) != 0) {
            wirelog_program_free(program);
            return 1;
        }

        fail_at = attempt;
        allocation_calls = 0;
        wl_plan_t *plan = NULL;
        int rc = wl_plan_from_program(program, &plan);
        bool injected = allocation_calls > attempt;
        if (injected)
            saw_injected_failure = true;
        if (rc == 0) {
            if (!plan) {
                fprintf(stderr, "successful generation returned no plan at "
                    "%ld\n", attempt);
                wirelog_program_free(program);
                return 1;
            }
            for (uint32_t s = 0; s < plan->stratum_count; s++) {
                for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
                    const wl_plan_relation_t *relation
                        = &plan->strata[s].relations[r];
                    for (uint32_t i = 0; i < relation->op_count; i++) {
                        if (relation->ops[i].op == WL_PLAN_OP_LFTJ)
                            saw_lftj = true;
                    }
                }
            }
            unsigned declarations = 0;
            for (uint32_t i = 0; i < plan->edb_count; i++)
                declarations += plan->edb_schemas &&
                    plan->edb_schemas[i].columns != NULL;
            if (declarations != 2) {
                fprintf(stderr, "successful plan lost compound declarations\n");
                wl_plan_free(plan); wirelog_program_free(program); return 1;
            }
            saw_inline = true;
            saw_success = true;
            wl_plan_free(plan);
        } else if (plan) {
            fprintf(stderr, "failed clone returned a partial plan at %ld\n",
                attempt);
            wl_plan_free(plan);
            wirelog_program_free(program);
            return 1;
        }
        fail_at = -1;
        wirelog_program_free(program);
    }

    if (!test_mr1_session_arena_malloc(false) ||
        !test_mr1_session_arena_malloc(true)) return 1;
    return saw_success && saw_injected_failure && saw_lftj &&
           saw_inline ? 0 : 1;
}
