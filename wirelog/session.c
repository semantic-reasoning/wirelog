/*
 * session.c - wirelog session wrapper implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#include "session.h"
#include "backend.h"
#include "thread.h"
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

static void wl_session_gate_fatal(void);

typedef enum {
    WL_SESSION_CLOSE_NONE,
    WL_SESSION_CLOSE_PREPARING,
    WL_SESSION_CLOSE_PARKED,
    WL_SESSION_CLOSE_COMMITTED,
    WL_SESSION_CLOSE_LEGACY_WAIT
} wl_session_close_phase_t;

typedef enum {
    WL_SESSION_SOURCE_TICKET_NONE,
    WL_SESSION_SOURCE_TICKET_PREPARED_ROLE,
    WL_SESSION_SOURCE_TICKET_PREPARED_BIND,
    WL_SESSION_SOURCE_TICKET_ROLE_INITIAL_STOP,
    WL_SESSION_SOURCE_TICKET_BIND_INITIAL_STOP,
    WL_SESSION_SOURCE_TICKET_ROLE_RESULT,
    WL_SESSION_SOURCE_TICKET_BIND_RESULT,
    WL_SESSION_SOURCE_TICKET_PROTOCOL_ONLY
} wl_session_source_ticket_kind_t;

typedef struct {
    wl_session_source_ticket_kind_t kind;
    const wl_session_t *session;
    const void *root;
    wl_backend_financial_actor_t actor;
    int pending_result, foreign_result;
    wl_evaluation_control_source_financial_state_t state;
} wl_session_source_ticket_t;

static wl_backend_financial_actor_t
wl_session_source_actor_current(void)
{
#if defined(WL_HAVE_C11_THREADS)
    return thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    return GetCurrentThreadId();
#else
    return pthread_self();
#endif
}

static bool
wl_session_source_actor_current_equal(wl_backend_financial_actor_t actor)
{
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(actor, wl_session_source_actor_current());
#elif defined(_WIN32) || defined(_WIN64)
    return actor == wl_session_source_actor_current();
#else
    return pthread_equal(actor, wl_session_source_actor_current());
#endif
}

struct wl_session_admission {
    wl_mutex_t mutex;
    wl_cond_t idle;
    size_t active_operations;
    bool controlled_step_pending;
    bool controlled_step_complete;
    bool closing;
    wl_session_close_phase_t close_phase;
    const void *close_owner;
    wl_session_exclusive_attempt_t *exclusive_attempt;
    wl_evaluation_control_source_bank_t *source_bank;
    wl_session_source_ticket_t source_ticket;
    wl_backend_controlled_completion_t completed_owner;
    uint64_t next_completed_owner_ticket;
};

#if defined(_MSC_VER)
#define WL_SESSION_THREAD_LOCAL __declspec(thread)
#else
#define WL_SESSION_THREAD_LOCAL _Thread_local
#endif
static WL_SESSION_THREAD_LOCAL unsigned char wl_session_thread_cookie;
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_session_test_completed_event)(wl_session_test_completed_event_t,
    const wl_session_t *, const wl_backend_controlled_completion_t *,
    wl_backend_completed_source_kind_t, int);
static WL_SESSION_THREAD_LOCAL bool wl_session_fail_next_admission_lock;
static WL_SESSION_THREAD_LOCAL bool wl_session_fail_next_admission_unlock;
static WL_SESSION_THREAD_LOCAL unsigned wl_session_refuse_source_read_after;

void
wl_session_testhook_refuse_source_read(unsigned after)
{
    wl_session_refuse_source_read_after = after;
}
#endif
/* The public wrapper produces this scope only after actual admission. It
* owns its issued context only inside this actual public stack frame.
* No BANK/context/controller/attempt-ID loan reaches Native/user code. */
typedef struct wl_session_source_scope {
    wl_session_t *session;
    wl_session_admission_t *admission;
    wl_evaluation_control_source_lane_t lane;
    struct wl_session_source_scope *previous;
    bool metadata_step;
    enum {
        WL_SESSION_SOURCE_METADATA_NONE,
        WL_SESSION_SOURCE_METADATA_REQUEST,
        WL_SESSION_SOURCE_METADATA_RECIPE,
        WL_SESSION_SOURCE_METADATA_ISSUED,
        WL_SESSION_SOURCE_METADATA_ISSUED_BIND,
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED,
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND
    } metadata_mode;
    /* Initialized only by the REQUEST publisher. No absent request aggregate
     * is zero-filled at every STEP/Snapshot/CLOSE scope construction. */
    wl_session_source_metadata_request_t metadata;
    const wl_session_source_metadata_recipe_t *metadata_recipe;
    const wl_backend_financial_offer_t *metadata_offer;
    const void *metadata_bind_root;
    /* Initialized only on actual issued OPEN, never read in other modes. */
    wl_evaluation_control_source_context_t issued_context;
    int issued_pending_result;
    wl_evaluation_control_source_retained_output_t retained_output;
    bool retained_protocol_only;
    /* Ordinary STEP only: restore admission if no backend was dispatched. */
    bool pending_before_dispatch;
    wl_session_source_snapshot_observation_t *snapshot_observation;
    wl_evaluation_control_source_context_t observation_context;
    bool observation_open;
    int observation_stop;
} wl_session_source_scope_t;

static WL_SESSION_THREAD_LOCAL wl_session_source_scope_t *
    wl_session_source_scope;

/* Published only around the backend prepare callback of an actually claimed
 * checked CLOSE. Other Source scopes cannot acquire this provenance by copying
 * a lane value. The automatic mark never survives its real CLOSE frame. */
typedef struct wl_session_checked_close_scope {
    const struct wl_session_checked_close_scope *identity;
    const wl_session_source_scope_t *source;
    const wl_session_t *session;
    const wl_session_admission_t *admission;
    const void *claim_cookie;
    wl_backend_financial_actor_t actor;
} wl_session_checked_close_scope_t;

static WL_SESSION_THREAD_LOCAL const wl_session_checked_close_scope_t *
    wl_session_checked_close_scope;
typedef struct wl_session_completed_source_event {
    const struct wl_session_completed_source_event *identity;
    const wl_session_source_scope_t *scope;
    const wl_backend_controlled_completion_t *completion;
    wl_backend_completed_source_kind_t kind;
    int result;
} wl_session_completed_source_event_t;
static WL_SESSION_THREAD_LOCAL const wl_session_completed_source_event_t *
    wl_session_completed_source_event;
static WL_SESSION_THREAD_LOCAL const wl_session_t *wl_session_completed_postcall;

/* Initialize only live scope fields. Readers must test metadata_mode before
 * following compatibility request storage, which has no value in NONE mode. */
static void
wl_session_source_scope_initialize(wl_session_source_scope_t *scope,
    wl_session_t *session, wl_session_admission_t *admission,
    wl_evaluation_control_source_lane_t lane, bool metadata_step)
{
    scope->session = session;
    scope->admission = admission;
    scope->lane = lane;
    scope->previous = wl_session_source_scope;
    scope->metadata_step = metadata_step;
    scope->metadata_mode = WL_SESSION_SOURCE_METADATA_NONE;
    scope->snapshot_observation = NULL;
    scope->observation_open = false;
    scope->observation_stop = 0;
}

/* Call only after the requested Session/current-Scope association is proved.
 * A prior owned financial stop is a no-action receipt, never another cut. */
static int
wl_session_source_observation_end(wl_session_source_scope_t *scope)
{
    if (!scope->observation_open) return scope->observation_stop;
    int result = wl_evaluation_control_source_observation_end(
        &scope->observation_context);
    if (!scope->observation_context.identity) {
        scope->observation_open = false;
        if (result) scope->observation_stop = result;
    }
    return result;
}

static int
wl_session_source_observation_poll(wl_session_source_scope_t *scope)
{
    if (scope->observation_stop) return scope->observation_stop;
    if (!scope->observation_open) {
        int entered = wl_evaluation_control_source_observation_open(
            scope->admission->source_bank, &scope->observation_context);
        if (entered) {
            int result = entered <
                0 ? -entered : WL_EVALUATION_CONTROL_CANCELLED;
            if (entered > 0) scope->observation_stop = result;
            return result;
        }
        scope->observation_open = true;
        return 0; /* Actual OPEN's real post10zero just succeeded. */
    }
    int result = wl_evaluation_control_source_observation_poll(
        &scope->observation_context);
    if (!scope->observation_context.identity) {
        scope->observation_open = false;
        if (result) scope->observation_stop = result;
    }
    return result;
}

bool
wl_session_source_step_metadata_can_defer(const wl_session_t *session)
{
    const wl_session_source_scope_t *scope = wl_session_source_scope;
    return scope && scope->session == session && scope->metadata_step
           && scope->admission == session->operation_admission
           && scope->lane == WL_EVALUATION_CONTROL_SOURCE_OPERATION
           && scope->metadata_mode == WL_SESSION_SOURCE_METADATA_NONE
           && session->backend
           && session->backend->session_controlled_financial_offer;
}

/* Selection happens before the actual BANK issuer. No stack payload or
* Native owner is copied into the transient public operation result. */
int
wl_session_source_step_metadata_select(wl_session_t *session,
    const wl_session_source_metadata_request_t *request)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !scope->metadata_step) return 0;
    if (!session || scope->session != session
        || scope->admission != session->operation_admission
        || scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        || !session->owns_operation_admission || !session->evaluation_control
        || !request || !request->result || !request->owner || !request->action
        || !request->payload || !request->busy || !*request->busy
        || !request->phase || *request->phase != request->before_phase
        || (!request->operation_id && request->kind !=
        WL_SESSION_SOURCE_METADATA_FINANCIER_COMPLETED
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_BEGIN_FAILURE
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_SNAPSHOT_PRIOR)
        || !request->operation_identity
        || !request->before_phase
        || (request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER
        && request->kind != WL_SESSION_SOURCE_METADATA_CONSTRUCTION
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_COMPLETED
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_BEGIN_FAILURE
        && request->kind !=
        WL_SESSION_SOURCE_METADATA_FINANCIER_SNAPSHOT_PRIOR))
        return -EINVAL;
    wl_session_source_metadata_result_t *result = request->result;
    if (scope->metadata_mode != WL_SESSION_SOURCE_METADATA_NONE
        || result->scheduled || result->ready
        || result->identity) return -EBUSY;
    result->owner = request->owner; result->session = session;
    result->admission = scope->admission;
    result->thread_cookie = &wl_session_thread_cookie;
    result->operation_id = request->operation_id;
    result->operation_identity = request->operation_identity;
    result->before_phase = request->before_phase; result->kind = request->kind;
    result->outcome.action_result = 0; result->outcome.action_called = false;
    result->financial_result = 0; result->ready = false;
    result->identity = result; result->scheduled = true;
    scope->metadata = *request;
    scope->metadata_mode = WL_SESSION_SOURCE_METADATA_REQUEST;
    return 1;
}

int
wl_session_source_step_metadata_take(wl_session_t *session,
    wl_session_source_metadata_result_t *result, const void *owner,
    uint64_t operation_id, uint64_t operation_identity,
    wl_session_source_metadata_kind_t kind,
    wl_evaluation_control_source_phase_result_t *out, unsigned *before_phase)
{
    if (!result || !out || !before_phase) return -EINVAL;
    if (!result->identity && !result->scheduled && !result->ready) return 0;
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || result->identity != result || result->session != session
        || result->admission != scope->admission || result->owner != owner
        || result->operation_id != operation_id
        || result->operation_identity != operation_identity ||
        result->kind != kind
        || result->thread_cookie != &wl_session_thread_cookie) return -EINVAL;
    if (result->scheduled || !result->ready) return -EBUSY;
    *out = (wl_evaluation_control_source_phase_result_t){
        result->financial_result, result->outcome.action_result,
        result->outcome.action_called
    };
    *before_phase = result->before_phase;
    result->ready = false; result->identity = NULL;
    return 1;
}

/* The caller already owns the canonical financial descriptor. The short
 * selector checks its actual stable role slot before transporting one pointer;
 * the live semantic tuple is resolved only after Core's real initial zero. */
int
wl_session_source_step_metadata_select_recipe(wl_session_t *session,
    const wl_session_source_metadata_recipe_t *recipe)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !scope->metadata_step) return 0;
    /* metadata_step is created only by the admitted OP wrapper. Its scope
     * admission/lane and issued immutable recipe cannot change before execute.
     * The provider proves exact slot addresses before any result dereference. */
    if (scope->session != session || !recipe || recipe->identity != recipe
        || recipe->session != session || !recipe->root || !recipe->resolve)
        return -EINVAL;
    if (scope->metadata_mode != WL_SESSION_SOURCE_METADATA_NONE) return -EBUSY;
    int rc = recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_SELECT, NULL);
    if (rc) return -rc;
    wl_session_source_metadata_result_t *result = recipe->result;
    if (result->scheduled || result->ready) return -EBUSY;
    result->before_phase = *recipe->phase;
    result->thread_cookie = &wl_session_thread_cookie;
    result->scheduled = true;
    scope->metadata_recipe = recipe;
    scope->metadata_mode = WL_SESSION_SOURCE_METADATA_RECIPE;
    return 1;
}

int
wl_session_source_step_metadata_take_recipe(wl_session_t *session,
    const wl_session_source_metadata_recipe_t *recipe,
    wl_evaluation_control_source_phase_result_t *out, unsigned *before_phase)
{
    if (!recipe || recipe->identity != recipe || !recipe->result
        || recipe->result->recipe != recipe || !out || !before_phase)
        return -EINVAL;
    wl_session_source_metadata_result_t *result = recipe->result;
    if (!result->scheduled && !result->ready) return 0;
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || recipe->session != session || !recipe->root || !recipe->resolve
        || result->thread_cookie != &wl_session_thread_cookie) return -EINVAL;
    if (result->scheduled || !result->ready) return -EBUSY;
    int rc = recipe->resolve(recipe, result->semantic_valid ?
            WL_SESSION_SOURCE_METADATA_TAKE :
            WL_SESSION_SOURCE_METADATA_SELECT, NULL);
    if (rc) return -rc;
    if (!result->semantic_valid && (!recipe->phase
        || *recipe->phase != result->before_phase
        || result->outcome.action_called || !result->financial_result))
        return -EINVAL;
    *out = (wl_evaluation_control_source_phase_result_t){
        result->financial_result, result->outcome.action_result,
        result->outcome.action_called
    };
    *before_phase = result->before_phase;
    result->ready = false;
    result->semantic_valid = false;
    if (session->backend->session_controlled_financial_head_complete
        && recipe->resolve(recipe,
        WL_SESSION_SOURCE_METADATA_PRESENTATION_TAKEN, NULL))
        wl_session_gate_fatal();
    return 1;
}

/* Adapter passes only the immutable recipe to its sealed role resolver. */
static int
wl_session_source_metadata_prepare(const void *opaque, unsigned group,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    const wl_session_source_metadata_recipe_t *recipe = opaque;
    return recipe->resolve(recipe, group,
               (wl_session_source_metadata_preparation_t *)prepared);
}

/* The low policy belongs to the declared canonical producer, not to an
 * arbitrary resolver that merely supplies the same accounting enum. */
static const wl_evaluation_control_source_phase_policy_t
    wl_session_source_metadata_role_policy = {10, 13, 6};

static const wl_evaluation_control_source_phase_policy_t *
wl_session_source_metadata_policy(const wl_session_t *session,
    const wl_session_source_metadata_recipe_t *recipe)
{
    static const wl_evaluation_control_source_phase_policy_t legacy = {
        208, 224, 80
    };
    return recipe->accounting_class ==
           WL_SESSION_SOURCE_METADATA_ACCOUNTING_COLUMNAR_ROLE
           && session->backend->session_source_metadata_canonical_resolver
           && recipe->resolve ==
           session->backend->session_source_metadata_canonical_resolver ?
           &wl_session_source_metadata_role_policy : &legacy;
}

wl_session_source_metadata_role_result_t
wl_session_source_metadata_role_owned(wl_session_t *session,
    const wl_session_source_metadata_recipe_t *recipe)
{
    wl_session_source_metadata_role_result_t out = {{EINVAL, 0, false}, 0};
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || !session->owns_operation_admission || !session->evaluation_control
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)) return out;
    bool close = scope->lane == WL_EVALUATION_CONTROL_SOURCE_CLOSE;
    wl_evaluation_control_source_context_t context;
    int ending = wl_session_source_observation_end(scope);
    if (ending) {
        out.phase.financial_result = ending; return out;
    }
    int entered = close ?
        wl_evaluation_control_source_metadata_enter_close_owned(
        scope->admission->source_bank, &context) :
        wl_evaluation_control_source_metadata_enter_issued(
        scope->admission->source_bank, &context);
    if (entered) {
        out.phase.financial_result = entered < 0 ? -entered :
            WL_EVALUATION_CONTROL_CANCELLED;
        return out;
    }
    /* All full slot/actor/class proof follows this invocation's real first0.
     * A failed proof still owns, and must settle, this automatic context. */
    wl_session_source_metadata_preparation_t prepared;
    prepared.source = (wl_evaluation_control_source_metadata_preparation_t){0};
    int protocol = !recipe || recipe->identity != recipe
        || recipe->session != session || !recipe->root
        || recipe->accounting_class !=
        WL_SESSION_SOURCE_METADATA_ACCOUNTING_COLUMNAR_ROLE
        || !session->backend->session_source_metadata_canonical_resolver
        || recipe->resolve !=
        session->backend->session_source_metadata_canonical_resolver ? EINVAL :
        recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_SELECT, NULL);
    if (!protocol && (recipe->result->scheduled || recipe->result->ready))
        protocol = EBUSY;
    if (!protocol) recipe->result->before_phase = *recipe->phase;
    const wl_evaluation_control_source_phase_policy_t *policy =
        protocol ? NULL :
        &wl_session_source_metadata_role_policy;
    out.phase.financial_result = close ?
        wl_evaluation_control_source_metadata_continue_close_owned(&context,
            policy, wl_session_source_metadata_prepare, recipe,
            &prepared.source) :
        wl_evaluation_control_source_metadata_continue_issued(&context,
            policy, wl_session_source_metadata_prepare, recipe,
            &prepared.source);
    if (!prepared.source.entry.settled || context.identity)
        wl_session_gate_fatal();
    out.preparation_protocol = protocol ? protocol :
        prepared.source.preparation_protocol;
    out.phase.action_result = prepared.source.outcome.action_result;
    out.phase.action_called = prepared.source.outcome.action_called;
    return out;
}

wl_evaluation_control_source_phase_result_t
wl_session_source_metadata_immediate_recipe(wl_session_t *session,
    const wl_session_source_metadata_recipe_t *recipe)
{
    wl_evaluation_control_source_phase_result_t out = {EINVAL, 0, false};
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || !session->owns_operation_admission || !session->evaluation_control
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)
        || !recipe || recipe->identity != recipe || recipe->session != session
        || !recipe->root || !recipe->resolve) return out;
    int rc = recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_SELECT, NULL);
    if (rc) {
        out.financial_result = rc; return out;
    }
    wl_session_source_metadata_result_t *result = recipe->result;
    if (result->scheduled || result->ready) return out;
    result->before_phase = *recipe->phase;
    wl_session_source_metadata_preparation_t prepared;
    prepared.source = (wl_evaluation_control_source_metadata_preparation_t){0};
    const wl_evaluation_control_source_phase_policy_t *policy =
        wl_session_source_metadata_policy(session, recipe);
    int ending = wl_session_source_observation_end(scope);
    if (ending) {
        out.financial_result = ending; return out;
    }
    out.financial_result =
        wl_evaluation_control_source_bank_run_metadata_recipe(
        scope->admission->source_bank, scope->lane,
        policy, wl_session_source_metadata_prepare, recipe, &prepared.source);
    result->preparation_protocol = prepared.source.preparation_protocol;
    out.action_result = prepared.source.outcome.action_result;
    out.action_called = prepared.source.outcome.action_called;
    return out;
}

int
wl_session_source_metadata_bind_issued(wl_session_t *session, void *root,
    wl_evaluation_control_source_metadata_bind_prepare_fn prepare,
    wl_evaluation_control_source_metadata_bind_fn bind,
    wl_evaluation_control_source_metadata_bind_outcome_t *out)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || scope->session != session
        || (scope->metadata_mode != WL_SESSION_SOURCE_METADATA_ISSUED_BIND
        && scope->metadata_mode !=
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND)
        || scope->metadata_bind_root != root) return EINVAL;
    if (out) out->settled = false;
    int rc;
    if (scope->metadata_mode ==
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND){
        rc = wl_evaluation_control_source_metadata_bind_continue_retained_owned(
            &scope->issued_context, prepare, bind, root, out,
            &scope->retained_output);
        if (!rc) rc = scope->retained_output.financial_result;
    } else rc = wl_evaluation_control_source_metadata_bind_continue_issued(
            &scope->issued_context, prepare, bind, root, out);
    if (!out || !out->settled || scope->issued_context.identity)
        wl_session_gate_fatal();
    return rc;
}

int
wl_session_source_metadata_bind_roles(wl_session_t *session, void *root,
    unsigned *cursor, wl_evaluation_control_source_metadata_bind_fn bind)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || !session->owns_operation_admission || !session->evaluation_control
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)) return EINVAL;
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 224, 80
    };
    int ending = wl_session_source_observation_end(scope);
    if (ending) return ending;
    return wl_evaluation_control_source_bank_bind_metadata_roles(
        scope->admission->source_bank, scope->lane, &policy, bind, root,
        cursor);
}

int
wl_session_source_metadata_observe_bound_roles(wl_session_t *session)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || !session->owns_operation_admission || !session->evaluation_control
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)) return EINVAL;
    wl_evaluation_control_source_metadata_entry_t entry = {0};
    int ending = wl_session_source_observation_end(scope);
    if (ending) return ending;
    return wl_evaluation_control_source_bank_observe_metadata(
        scope->admission->source_bank, scope->lane, &entry);
}

/* Immediate Snapshot/CLOSE counterpart. The actual caller has already proved
 * its canonical financial disposition; no callback/payload escapes this call. */
wl_evaluation_control_source_phase_result_t
wl_session_source_metadata_immediate(wl_session_t *session,
    const wl_session_source_metadata_request_t *request)
{
    wl_evaluation_control_source_phase_result_t out = {EINVAL, 0, false};
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session
        || scope->admission != session->operation_admission
        || !session->owns_operation_admission || !session->evaluation_control
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)
        || !request || !request->owner || !request->action || !request->payload
        || !request->busy || !*request->busy || !request->phase
        || !request->before_phase || !request->operation_identity
        || (!request->operation_id && request->kind !=
        WL_SESSION_SOURCE_METADATA_FINANCIER_COMPLETED
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_BEGIN_FAILURE
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_SNAPSHOT_PRIOR)
        || (request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER
        && request->kind != WL_SESSION_SOURCE_METADATA_CONSTRUCTION
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_COMPLETED
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_BEGIN_FAILURE
        && request->kind != WL_SESSION_SOURCE_METADATA_FINANCIER_SNAPSHOT_PRIOR)
        || *request->phase != request->before_phase) return out;
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 224, 80
    };
    wl_evaluation_control_source_metadata_outcome_t outcome = {0, false};
    int ending = wl_session_source_observation_end(scope);
    if (ending) {
        out.financial_result = ending; return out;
    }
    out.financial_result = wl_evaluation_control_source_bank_run_metadata(
        scope->admission->source_bank, scope->lane, &policy, request->action,
        request->payload, &outcome);
    out.action_result = outcome.action_result;
    out.action_called = outcome.action_called;
    return out;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_session_test_source_birth_entered)(void);
#endif

static int
wl_session_source_finance_birth(void *payload)
{
    wl_session_source_finance_t *record = payload;
    /* The actual TLS/BANK CURRENT issuer dominates this private callback.
     * Birth completes before this nonrefusing publication; no control is
     * exposed to a Native/user callback during construction. */
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_test_source_birth_entered)
        wl_session_test_source_birth_entered();
#endif
    wl_evaluation_control_lexical_finance_birth_result_t born =
        wl_evaluation_control_lexical_finance_birth_try(record->limit, record);
    if (born.protocol) return born.protocol;
    record->control = born.control; record->id = born.id;
    record->attached = true; record->active = true;
    return 0;
}

bool
wl_session_source_operation_scope_owned(const wl_session_t *session)
{
    const wl_session_source_scope_t *scope = wl_session_source_scope;
    return session && scope && scope->session == session
           && session->owns_operation_admission &&
           session->owns_evaluation_control
           && session->evaluation_control
           && session->operation_admission &&
           scope->admission == session->operation_admission
           && scope->lane == WL_EVALUATION_CONTROL_SOURCE_OPERATION;
}

bool
wl_session_source_snapshot_scope_owned(const wl_session_t *session)
{
    return wl_session_source_operation_scope_owned(session)
           && !wl_session_source_scope->metadata_step;
}

bool
wl_session_source_checked_close_scope_owned(const wl_session_t *session)
{
    const wl_session_checked_close_scope_t *claim =
        wl_session_checked_close_scope;
    const wl_session_source_scope_t *scope = wl_session_source_scope;
    /* No admission mutex is acquired here: a real claimed CLOSE excludes
     * competing operations until destroy_prepare returns. This fixed scope
     * proof is safe in the financial BANK callback and grants no permission
     * to inspect or mutate an observer, reader, token or other Native object. */
    return session && claim && claim->identity == claim && scope
           && claim->source == scope && claim->session == session
           && scope->session == session
           && claim->admission == scope->admission
           && scope->admission == session->operation_admission
           && claim->claim_cookie == &wl_session_thread_cookie
           && scope->lane == WL_EVALUATION_CONTROL_SOURCE_CLOSE
           && !scope->metadata_step && session->owns_operation_admission
           && session->owns_evaluation_control && session->evaluation_control
           && wl_session_source_actor_current_equal(claim->actor);
}

bool
wl_session_source_close_scope_owned(const wl_session_t *session)
{
    const wl_session_source_scope_t *scope = wl_session_source_scope;
    /* Financial placement only. In particular this broader CLOSE observation
     * cannot establish the checked claim needed for a foreign Native lease. */
    return session && scope && scope->session == session
           && session->owns_operation_admission
           && session->owns_evaluation_control && session->evaluation_control
           && scope->admission
           && scope->admission == session->operation_admission
           && scope->lane == WL_EVALUATION_CONTROL_SOURCE_CLOSE
           && !scope->metadata_step;
}

bool
wl_session_source_completed_endpoint_owned(const wl_session_t *session,
    const wl_backend_controlled_completion_t *completion,
    wl_backend_completed_source_kind_t kind, int result)
{
    const wl_session_completed_source_event_t *e =
        wl_session_completed_source_event;
    return e && e->identity == e && e->scope == wl_session_source_scope
           && e->scope->session == session
           && e->scope->admission == session->operation_admission
           && e->completion == completion && e->kind == kind
           && e->result == result;
}

bool
wl_session_completed_postcall_pending(const wl_session_t *session)
{
    return session && wl_session_completed_postcall == session;
}

static wl_evaluation_control_source_phase_result_t
wl_session_source_run(wl_session_t *session,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || scope->session != session || !session
        || !session->owns_operation_admission || !session->evaluation_control
        || scope->admission != session->operation_admission
        || !scope->admission
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE))
        return (wl_evaluation_control_source_phase_result_t){EINVAL, 0, false};
    int ending = wl_session_source_observation_end(scope);
    if (ending) return (wl_evaluation_control_source_phase_result_t){.
                                                                     financial_result
                                                                         =
                                                                         ending};
    return wl_evaluation_control_source_bank_run_lexical(
        scope->admission->source_bank,
        scope->lane, policy, action, payload);
}

static wl_evaluation_control_source_phase_result_t
wl_session_source_run_birth(wl_session_t *session,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || scope->session != session || !session
        || !session->owns_operation_admission || !session->evaluation_control
        || scope->admission != session->operation_admission
        || !scope->admission
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE))
        return (wl_evaluation_control_source_phase_result_t){EINVAL, 0, false};
    int ending = wl_session_source_observation_end(scope);
    if (ending) return (wl_evaluation_control_source_phase_result_t){.
                                                                     financial_result
                                                                         =
                                                                         ending};
    return wl_evaluation_control_source_bank_run_lexical_constructor(
        scope->admission->source_bank,
        scope->lane, policy, action, payload);
}

static bool
wl_session_source_snapshot_observation_scope(wl_session_t *session,
    const wl_session_source_scope_t *scope)
{
    return session && scope && scope->session == session
           && session->owns_operation_admission && session->evaluation_control
           && scope->admission &&
           scope->admission == session->operation_admission
           && scope->lane == WL_EVALUATION_CONTROL_SOURCE_OPERATION
           && !scope->metadata_step
           && scope->metadata_mode == WL_SESSION_SOURCE_METADATA_NONE;
}

static int
wl_session_source_snapshot_observation_zero(wl_session_t *session,
    wl_session_source_snapshot_observation_t *binding)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!wl_session_source_snapshot_observation_scope(session, scope)
        || !binding || scope->snapshot_observation != binding
        || binding->identity != binding || binding->scope != scope
        || binding->session != session
        || binding->thread_cookie != &wl_session_thread_cookie)
        return EINVAL;
    wl_session_source_finance_t *record = binding->record;
    if (!record || record->control != binding->control
        || !record->control || record->control == session->evaluation_control
        || !record->active || !record->attached || !record->id
        || record->limit != 240 || record->charged > record->limit)
        return EINVAL;
    wl_evaluation_control_lexical_finance_checkpoint_result_t checked =
        wl_evaluation_control_lexical_finance_checkpoint_owned(record->control,
            record, sizeof(*record), binding->control_identity, record->id,
            record->limit, record->charged);
    return checked.protocol ? checked.protocol : !checked.checked ? EINVAL
        : checked.checkpoint_result;
}

int
wl_session_source_snapshot_observation_bind(wl_session_t *session,
    wl_session_source_snapshot_observation_t *binding,
    wl_session_source_finance_t *record)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!wl_session_source_snapshot_observation_scope(session, scope)
        || scope->snapshot_observation || !binding || !record
        || binding->identity) return EINVAL;
    uintptr_t b = (uintptr_t)binding, r = (uintptr_t)record,
        s = (uintptr_t)session, q = (uintptr_t)scope;
    if (sizeof(*binding) > UINTPTR_MAX - b
        || sizeof(*record) > UINTPTR_MAX - r
        || sizeof(*session) > UINTPTR_MAX - s ||
        sizeof(*scope) > UINTPTR_MAX - q
        || (b < r + sizeof(*record) && r < b + sizeof(*binding))
        || (b < s + sizeof(*session) && s < b + sizeof(*binding))
        || (r < s + sizeof(*session) && s < r + sizeof(*record))
        || (b < q + sizeof(*scope) && q < b + sizeof(*binding))
        || (r < q + sizeof(*scope) && q < r + sizeof(*record))) return EINVAL;
    if (!record->control || record->control == session->evaluation_control
        || !record->active || !record->attached || !record->id
        || record->limit != 240 || record->charged) return EINVAL;
    uint64_t identity = wl_evaluation_control_identity(record->control);
    wl_evaluation_control_lexical_finance_checkpoint_result_t checked =
        wl_evaluation_control_lexical_finance_checkpoint_owned(record->control,
            record, sizeof(*record), identity, record->id, 240, 0);
    if (checked.protocol || !checked.checked || checked.checkpoint_result)
        return checked.protocol ? checked.protocol : checked.checkpoint_result
            ? checked.checkpoint_result : EINVAL;
    binding->session = session; binding->scope = scope;
    binding->thread_cookie = &wl_session_thread_cookie;
    binding->record = record; binding->control = record->control;
    binding->control_identity = identity; binding->outer = false;
    binding->identity = binding;
    scope->snapshot_observation = binding;
    return 0;
}

int
wl_session_source_snapshot_observation_unbind(wl_session_t *session,
    wl_session_source_snapshot_observation_t *binding)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!wl_session_source_snapshot_observation_scope(session, scope)
        || !binding || scope->snapshot_observation != binding
        || binding->identity != binding || binding->session != session
        || binding->scope != scope
        || binding->thread_cookie != &wl_session_thread_cookie) return EINVAL;
    scope->snapshot_observation = NULL;
    binding->identity = NULL; binding->scope = NULL; binding->record = NULL;
    binding->control = NULL; binding->session = NULL;
    binding->thread_cookie = NULL; binding->control_identity = 0;
    binding->outer = false;
    return 0;
}

wl_session_source_snapshot_checkpoint_result_t
wl_session_source_snapshot_checkpoint(wl_session_t *session)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!wl_session_source_snapshot_observation_scope(session, scope)
        || !scope->snapshot_observation)
        return (wl_session_source_snapshot_checkpoint_result_t){false, 0};
    return (wl_session_source_snapshot_checkpoint_result_t){true,
                                                            wl_session_source_snapshot_observation_zero
                                                                (session,
                                                                scope->
                                                                snapshot_observation)};
}

wl_session_source_step_checkpoint_result_t
wl_session_source_step_checkpoint(wl_session_t *session)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!session || !scope || scope->session != session
        || !session->source_c1_admission || !session->owns_operation_admission
        || !session->owns_evaluation_control || !session->evaluation_control
        || !scope->admission || scope->admission != session->operation_admission
        || !scope->metadata_step
        || scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        || scope->metadata_mode == WL_SESSION_SOURCE_METADATA_ISSUED
        || scope->metadata_mode == WL_SESSION_SOURCE_METADATA_ISSUED_BIND
        || scope->metadata_mode == WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED
        || scope->metadata_mode ==
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND)
        return (wl_session_source_step_checkpoint_result_t){false, 0};
    return (wl_session_source_step_checkpoint_result_t){true,
                                                        wl_session_source_observation_poll
                                                            (scope)};
}

static int
wl_session_source_footer_noop(void *payload)
{
    (void)payload;
    return 0;
}

int
wl_session_source_footer(wl_session_t *session)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || scope->session != session || !session
        || !session->owns_operation_admission || !session->evaluation_control
        || scope->admission != session->operation_admission
        || !scope->admission
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)) return EINVAL;
    if (scope->lane == WL_EVALUATION_CONTROL_SOURCE_OPERATION) {
        if (scope->metadata_step && session->source_c1_admission)
            return wl_session_source_observation_poll(scope);
        wl_session_source_snapshot_observation_t *binding =
            scope->snapshot_observation;
        if (binding && binding->outer)
            return wl_session_source_snapshot_observation_zero(session,
                       binding);
        int result = wl_evaluation_control_source_operation_observe_owned(
            scope->admission->source_bank);
        if (binding && (result == WL_EVALUATION_CONTROL_CANCELLED
            || result == WL_EVALUATION_CONTROL_WORK_LIMIT)) {
            int observed = wl_session_source_snapshot_observation_zero(session,
                    binding);
            if (!observed) binding->outer = true;
            return observed;
        }
        return result;
    }
    /* CLOSE has its own existing authenticated owner and bank lane. It is
     * not relabelled as the fixed OP observation. */
    static const wl_evaluation_control_source_phase_policy_t policy = {
        190, 54, 208
    };
    return wl_evaluation_control_source_bank_run_lexical(
        scope->admission->source_bank, scope->lane, &policy,
        wl_session_source_footer_noop, NULL).financial_result;
}

int
wl_session_source_admission_read(wl_session_t *session,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 96, 80
    };
#ifdef WL_SESSION_TEST_HOOKS
    /* Exercise the actual kernel's invalid-action rejection, not a mock
     * financial result or Native permission. Consume the fault once. */
    if (wl_session_refuse_source_read_after
        && !--wl_session_refuse_source_read_after) action = NULL;
#endif
    wl_evaluation_control_source_phase_result_t phase =
        wl_session_source_run(session, &policy, action, payload);
    int rc = phase.financial_result ? phase.financial_result
        : phase.action_called ? phase.action_result : EINVAL;
    /* Admitted read rejection still owns the public return path. A real
     * scope error remains a scope error; it never falls back to direct reads. */
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (rc && scope && scope->session == session
        && scope->admission == session->operation_admission
        && session->owns_operation_admission)
        (void)wl_session_source_footer(session);
    return rc;
}

wl_evaluation_control_source_phase_result_t
wl_session_source_snapshot_metadata(wl_session_t *session,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 144, 80
    };
    return wl_session_source_run(session, &policy, action, payload);
}

wl_evaluation_control_source_phase_result_t
wl_session_source_operator_close_metadata(wl_session_t *session,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 224, 80
    };
    return wl_session_source_run(session, &policy, action, payload);
}

int
wl_session_source_finance_start(wl_session_t *session,
    wl_session_source_finance_t *record,
    const wl_evaluation_control_source_phase_policy_t *policy, uint64_t limit)
{
    if (!record || record->control || record->id || record->active
        || record->attached || !limit) return EINVAL;
    record->limit = limit;
    wl_evaluation_control_source_phase_result_t result =
        wl_session_source_run_birth(session, policy,
            wl_session_source_finance_birth,
            record);
    int rc = result.financial_result ? result.financial_result
        : !result.action_called ? EINVAL : result.action_result;
    if (rc) {
        (void)wl_session_source_finance_dispose(record);
        return rc;
    }
    return 0;
}

int
wl_session_source_finance_charge(wl_session_source_finance_t *record,
    uint64_t work)
{
    if (!record || !record->active || !record->control || !record->id
        || !work || work > WL_EVALUATION_CONTROL_QUANTUM) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(record->control, record->id,
            work);
    if (!rc) record->charged += work;
    return rc;
}

int
wl_session_source_finance_finish(wl_session_source_finance_t *record)
{
    if (!record || !record->control || !record->active || !record->attached
        || !record->id || !record->limit || record->charged > record->limit)
        return EINVAL;
    wl_evaluation_control_lexical_finance_finish_result_t finished =
        wl_evaluation_control_lexical_finance_finish_owned(record->control,
            record, sizeof(*record), record->id, record->limit,
            record->charged);
    if (!finished.finished) return finished.protocol ? finished.protocol :
               EINVAL;
    uint64_t id = record->id;
    record->active = false; record->id = 0;
    if (finished.protocol || finished.report.attempt_id != id
        || finished.report.limit != record->limit
        || finished.report.consumed != record->charged
        || finished.report.result != finished.result
        || finished.report.execution_status ||
        finished.report.cleanup_status) abort();
    return finished.result;
}

int
wl_session_source_finance_retire_active_owned(
    wl_session_source_finance_t *record)
{
    if (!record || !record->control || !record->active || !record->attached
        || !record->id || !record->limit || record->charged > record->limit)
        return EINVAL;
    wl_evaluation_control_lexical_finance_result_t retired =
        wl_evaluation_control_lexical_finance_retire_active_owned(
        record->control,
        record, sizeof(*record), record->id, record->limit,
        record->charged);
    if (!retired.closed) return retired.protocol ? retired.protocol : EINVAL;
    record->control = NULL; record->attached = false;
    record->active = false; record->id = 0;
    return retired.result;
}

int
wl_session_source_finance_rebegin(wl_session_source_finance_t *record)
{
    if (!record || !record->control || !record->attached || record->active ||
        record->id)
        return EINVAL;
    int rc = wl_evaluation_control_begin(record->control, record);
    if (rc) return rc;
    record->id = wl_evaluation_control_active_attempt_id(record->control);
    record->charged = 0; record->active = true;
    return record->id ? 0 : EINVAL;
}

int
wl_session_source_finance_dispose(wl_session_source_finance_t *record)
{
    if (!record || !record->control) return 0;
    int rc = record->active ? wl_session_source_finance_finish(record) : 0;
    if (record->active) return rc ? rc : EINVAL;
    if (record->attached) {
        int detached = wl_evaluation_control_detach(record->control, record);
        if (detached) return detached;
        record->attached = false;
    }
    wl_evaluation_control_release(record->control);
    record->control = NULL;
    return rc;
}

static bool
wl_session_source_close_storage(wl_session_t *session,
    wl_session_source_finance_t *record,
    wl_session_source_close_scope_t *scope)
{
    if (!session || !record || !scope) return false;
    uintptr_t r = (uintptr_t)record, s = (uintptr_t)scope,
        b = (uintptr_t)session;
    if (sizeof(*record) > UINTPTR_MAX - r || sizeof(*scope) > UINTPTR_MAX - s
        || sizeof(*session) > UINTPTR_MAX - b) return false;
    uintptr_t re = r + sizeof(*record), se = s + sizeof(*scope),
        be = b + sizeof(*session);
    return !(r < se && s < re) && !(r < be && b < re) && !(s < be && b < se);
}

typedef struct {
    wl_session_t *session;
    wl_session_source_finance_t *record;
    wl_session_source_close_scope_t *scope;
    const wl_evaluation_control_source_phase_policy_t *policy;
    uint64_t limit;
    bool bound;
} wl_session_source_checked_birth_t;

static int
wl_session_source_checked_birth(void *payload)
{
    wl_session_source_checked_birth_t *p = payload;
    wl_session_source_finance_t *record = p->record;
    wl_session_source_close_scope_t *scope = p->scope;
    if (!p->session || !record || !scope || !p->policy) return EINVAL;
    uintptr_t r = (uintptr_t)record, s = (uintptr_t)scope;
    uintptr_t b = (uintptr_t)p->session, q = (uintptr_t)p->policy;
    if (sizeof(*record) > UINTPTR_MAX - r || sizeof(*scope) > UINTPTR_MAX - s
        || sizeof(*p->session) > UINTPTR_MAX - b ||
        sizeof(*p->policy) > UINTPTR_MAX - q)
        return EINVAL;
    uintptr_t re = r + sizeof(*record), se = s + sizeof(*scope);
    uintptr_t be = b + sizeof(*p->session), qe = q + sizeof(*p->policy);
    if ((r < se && s < re) || (r < be && b < re) || (s < be && b < se)
        || (r < qe && q < re) || (s < qe && q < se)) return EINVAL;
    if (scope->identity) return EINVAL;
    if (record->control || record->id || record->active || record->attached
        || record->limit || record->charged || !p->limit) return EINVAL;
    wl_evaluation_control_lexical_finance_birth_result_t born =
        wl_evaluation_control_lexical_finance_birth_try(p->limit, record);
    if (born.protocol) return born.protocol;
    record->control = born.control; record->id = born.id;
    record->limit = p->limit; record->charged = 0;
    record->attached = record->active = true;
    scope->record = record; scope->session = p->session;
    scope->thread_cookie = &wl_session_thread_cookie;
    scope->control = record->control;
    scope->control_identity = born.identity;
    scope->limit = p->limit;
    scope->birth_phase = (wl_evaluation_control_source_phase_result_t){0};
    scope->result = (wl_session_source_finance_close_result_t){0};
    scope->pair_phase = (wl_evaluation_control_source_pair_result_t){0};
    scope->pair_complete = false;
    scope->identity = scope;
    p->bound = true;
    return 0;
}

int
wl_session_source_finance_start_checked(wl_session_t *session,
    wl_session_source_finance_t *record,
    const wl_evaluation_control_source_phase_policy_t *policy, uint64_t limit,
    wl_session_source_close_scope_t *scope)
{
    /* All record/scope field validation is behind the actual BANK CURRENT.
     * No uninitialized scope field beyond its initial marker is read here. */
    if (!record || !scope || !limit) return EINVAL;
    wl_session_source_checked_birth_t payload = {session, record, scope, policy,
                                                 limit, false};
    wl_evaluation_control_source_phase_result_t phase =
        wl_session_source_run(session,
            policy, wl_session_source_checked_birth, &payload);
    if (payload.bound) scope->birth_phase = phase;
    return phase.financial_result ? phase.financial_result
        : phase.action_called ? phase.action_result : EINVAL;
}

typedef struct {
    wl_session_source_checked_birth_t checked;
    int result;
    bool attempted;
} wl_session_source_checked_birth_early_t;

static int
wl_session_source_checked_birth_early(void *payload)
{
    wl_session_source_checked_birth_early_t *envelope = payload;
    wl_session_source_checked_birth_t *p = &envelope->checked;
    envelope->attempted = true;
    int rc = EINVAL;
    wl_session_source_finance_t *record = p->record;
    wl_session_source_close_scope_t *scope = p->scope;
    if (!p->session || !record || !scope || !p->policy) goto done;
    uintptr_t r = (uintptr_t)record, s = (uintptr_t)scope;
    uintptr_t b = (uintptr_t)p->session, q = (uintptr_t)p->policy;
    if (sizeof(*record) > UINTPTR_MAX - r || sizeof(*scope) > UINTPTR_MAX - s
        || sizeof(*p->session) > UINTPTR_MAX - b ||
        sizeof(*p->policy) > UINTPTR_MAX - q)
        goto done;
    uintptr_t re = r + sizeof(*record), se = s + sizeof(*scope);
    uintptr_t be = b + sizeof(*p->session), qe = q + sizeof(*p->policy);
    if ((r < se && s < re) || (r < be && b < re) || (s < be && b < se)
        || (r < qe && q < re) || (s < qe && q < se)) goto done;
    if (scope->identity) goto done;
    if (record->control || record->id || record->active || record->attached
        || record->limit || record->charged || !p->limit) goto done;
    wl_evaluation_control_lexical_finance_birth_result_t born =
        wl_evaluation_control_lexical_finance_fresh_birth_try(p->limit, record);
    if (born.protocol) {
        rc = born.protocol; goto done;
    }
    record->control = born.control; record->id = born.id;
    record->limit = p->limit; record->charged = 0;
    record->attached = record->active = true;
    scope->record = record; scope->session = p->session;
    scope->thread_cookie = &wl_session_thread_cookie;
    scope->control = record->control;
    scope->control_identity = born.identity;
    scope->limit = p->limit;
    scope->birth_phase = (wl_evaluation_control_source_phase_result_t){0};
    scope->result = (wl_session_source_finance_close_result_t){0};
    scope->pair_phase = (wl_evaluation_control_source_pair_result_t){0};
    scope->pair_complete = false;
    scope->identity = scope;
    p->bound = true;
    rc = 0;
done:
    envelope->result = rc;
    return rc;
}

static wl_evaluation_control_source_early_result_t
wl_session_source_run_birth_early(wl_session_t *session,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || scope->session != session || !session
        || !session->owns_operation_admission || !session->evaluation_control
        || scope->admission != session->operation_admission
        || !scope->admission
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE))
        return (wl_evaluation_control_source_early_result_t){
                   .protocol = EINVAL, .financial_result = EINVAL
        };
    int ending = wl_session_source_observation_end(scope);
    if (ending) return (wl_evaluation_control_source_early_result_t){.
                                                                     financial_result
                                                                         =
                                                                         ending};
    return wl_evaluation_control_source_bank_run_lexical_constructor_early(
        scope->admission->source_bank, scope->lane, policy, action, payload);
}

wl_session_source_checked_birth_outcome_t
wl_session_source_finance_start_checked_early(wl_session_t *session,
    wl_session_source_finance_t *record,
    const wl_evaluation_control_source_phase_policy_t *policy, uint64_t limit,
    wl_session_source_close_scope_t *scope)
{
    wl_session_source_checked_birth_outcome_t result = {
        .phase = {.protocol = EINVAL, .financial_result = EINVAL}
    };
    if (!record || !scope || !limit) return result;
    wl_session_source_checked_birth_early_t payload = {
        .checked = {session, record, scope, policy, limit, false}
    };
    result.phase = wl_session_source_run_birth_early(session, policy,
            wl_session_source_checked_birth_early, &payload);
    result.birth_attempted = payload.attempted;
    result.birth_result = payload.result;
    result.bound = payload.checked.bound;
    if (result.bound) scope->birth_phase =
            (wl_evaluation_control_source_phase_result_t){
            result.phase.financial_result, result.phase.first_result,
            result.phase.first_called
        };
    return result;
}

static int
wl_session_source_close_scope_prove(wl_session_t *session,
    wl_session_source_finance_t *record, wl_session_source_close_scope_t *scope)
{
    if (!wl_session_source_close_storage(session, record, scope)) return EINVAL;
    if (scope->identity != scope || scope->record != record ||
        scope->session != session)
        return EINVAL;
    if (scope->thread_cookie != &wl_session_thread_cookie) return EBUSY;
    if (scope->result.closed)
        return !record->control && !record->id && !record->active &&
               !record->attached
               && !scope->control ? 0 : EINVAL;
    if (!record->control || record->control != scope->control ||
        !record->attached
        || record->limit != scope->limit ||
        !scope->control_identity) return EINVAL;
    return 0;
}

static wl_evaluation_control_lexical_finance_expectation_t
wl_session_source_close_expectation(wl_session_source_finance_t *record,
    wl_session_source_close_scope_t *scope)
{
    return (wl_evaluation_control_lexical_finance_expectation_t){
               .owner = record, .scope = scope,
               .owner_bytes = sizeof(*record), .scope_bytes = sizeof(*scope),
               .identity = scope->control_identity, .limit = scope->limit,
               .id = record->id, .charged = record->charged,
               .active = record->active,
    };
}

static int
wl_session_source_close_actual(wl_session_source_close_scope_t *scope)
{
    wl_session_source_finance_t *record = scope->record;
    wl_evaluation_control_lexical_finance_expectation_t e =
        wl_session_source_close_expectation(record, scope);
    wl_evaluation_control_lexical_finance_result_t closed =
        wl_evaluation_control_lexical_finance_close_checked(record->control,
            &e);
    if (closed.protocol) return closed.protocol;
    if (!closed.closed || !closed.report_valid) return EINVAL;
    /* No callback or refusing operation separates physical financial free
     * from invalidation of both lexical pointers and the applied journal. */
    record->control = NULL; record->id = 0;
    record->active = record->attached = false;
    scope->control = NULL;
    scope->result.source_result = closed.result;
    scope->result.checkpoint_result = closed.checkpoint_result;
    scope->result.applied = scope->result.closed = true;
    return closed.result;
}

static int
wl_session_source_close_action(void *payload)
{
    /* close_owned proved this original-address lexical pair immediately
     * before synchronous BANK entry. Its prefix invokes no external action
     * and cannot change this private scope. The core still revalidates all
     * actual owner/ref/generation/alias state before its financial poll. */
    return wl_session_source_close_actual(payload);
}

wl_session_source_finance_close_result_t
wl_session_source_finance_close_owned(wl_session_t *session,
    wl_session_source_finance_t *record, wl_session_source_close_scope_t *scope)
{
    wl_session_source_finance_close_result_t out = {0};
    out.protocol = wl_session_source_close_scope_prove(session, record, scope);
    if (out.protocol) return out;
    if (scope->result.closed) return scope->result;
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 144,
                                                                       80};
    wl_evaluation_control_source_phase_result_t phase =
        wl_session_source_run(session,
            &policy, wl_session_source_close_action, scope);
    if (!phase.action_called) {
        /* No BANK activation/context is retained. Authenticate the genuine
         * private pair before its saved0; arbitrary inputs cannot poll it. */
        wl_evaluation_control_lexical_finance_expectation_t e =
            wl_session_source_close_expectation(record, scope);
        out.protocol =
            wl_evaluation_control_lexical_finance_prove_checked(record->control,
                &e);
        if (out.protocol) return out;
        int checkpoint = record->active
            ? wl_evaluation_control_charge_attempt(record->control, record->id,
                0) : 0;
        int rc = wl_session_source_close_actual(scope);
        if (!scope->result.closed) {
            out.protocol = rc; return out;
        }
        if (checkpoint) scope->result.checkpoint_result = checkpoint;
    }
    if (scope->result.closed) {
        scope->result.phase = phase;
        return scope->result;
    }
    /* Strict invalid-input refusal leaves the real owner intact. Proper
     * paired private births cannot acquire a foreign owner or extra ref. */
    out.phase = phase;
    out.protocol = phase.action_called ? phase.action_result : EINVAL;
    return out;
}

static wl_evaluation_control_source_pair_result_t
wl_session_source_run_pair(wl_session_t *session,
    const wl_evaluation_control_source_pair_policy_t *policy,
    wl_evaluation_control_source_action_fn first, void *first_payload,
    wl_evaluation_control_source_action_fn second, void *second_payload)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!session || !scope || scope->session != session
        || scope->admission != session->operation_admission
        || !session->owns_operation_admission
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE))
        return (wl_evaluation_control_source_pair_result_t){EINVAL, 0, 0, false,
                                                            false};
    int ending = wl_session_source_observation_end(scope);
    if (ending) return (wl_evaluation_control_source_pair_result_t){.
                                                                    financial_result
                                                                        = ending};
    return wl_evaluation_control_source_bank_run_lexical_pair(
        scope->admission->source_bank, scope->lane, policy,
        first, first_payload, second, second_payload);
}

wl_session_source_finance_pair_result_t
wl_session_source_finance_close_then_metadata(wl_session_t *session,
    wl_session_source_finance_t *record, wl_session_source_close_scope_t *scope,
    wl_evaluation_control_source_action_fn metadata, void *payload)
{
    wl_session_source_finance_pair_result_t out = {0};
    out.protocol = wl_session_source_close_scope_prove(session, record, scope);
    if (out.protocol) return out;
    if (!metadata || payload == scope || payload == record) {
        out.protocol = EINVAL; return out;
    }
    if (scope->result.closed) {
        out.close = scope->result;
        if (!scope->pair_complete) {
            out.protocol = EINVAL; return out;
        }
        out.phase = scope->pair_phase;
        return out;
    }
    static const wl_evaluation_control_source_pair_policy_t policy = {208, 144,
                                                                      144, 80};
    wl_evaluation_control_source_pair_result_t phase =
        wl_session_source_run_pair(session,
            &policy, wl_session_source_close_action, scope, metadata, payload);
    if (!phase.first_called) {
        /* No expired pair context is resumed. The original birth provenance
        * and exact core owner authorize only mandatory financial unwind. */
        wl_evaluation_control_lexical_finance_expectation_t e =
            wl_session_source_close_expectation(record, scope);
        out.protocol =
            wl_evaluation_control_lexical_finance_prove_checked(record->control,
                &e);
        if (out.protocol) return out;
        int checkpoint = record->active
            ? wl_evaluation_control_charge_attempt(record->control, record->id,
                0) : 0;
        int rc = wl_session_source_close_actual(scope);
        if (!scope->result.closed) {
            out.protocol = rc; return out;
        }
        if (checkpoint) scope->result.checkpoint_result = checkpoint;
    }
    if (!scope->result.closed) {
        out.phase = phase;
        out.protocol = phase.first_called ? phase.first_result : EINVAL;
        return out;
    }
    scope->result.phase = (wl_evaluation_control_source_phase_result_t){
        phase.financial_result, phase.first_result, phase.first_called
    };
    scope->pair_phase = phase;
    scope->pair_complete = true;
    out.close = scope->result; out.phase = phase;
    return out;
}

static bool
wl_session_source_final_issuer(wl_session_t *session)
{
    const wl_session_source_scope_t *issuer = wl_session_source_scope;
    return session && issuer && issuer->session == session
           && issuer->admission &&
           issuer->admission == session->operation_admission
           && session->owns_operation_admission && session->evaluation_control
           && (issuer->lane == WL_EVALUATION_CONTROL_SOURCE_OPERATION
           || issuer->lane == WL_EVALUATION_CONTROL_SOURCE_CLOSE);
}

#ifdef WL_SESSION_TEST_HOOKS
static int wl_session_admission_lock(wl_session_admission_t *);
static void wl_session_checked_unlock(wl_session_admission_t *);

int
wl_session_test_source_early_report(wl_session_t *session,
    wl_evaluation_control_report_t *out)
{
    wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!scope || !session || scope->session != session ||
        !session->owns_operation_admission
        || !session->evaluation_control || !scope->admission
        || scope->admission != session->operation_admission
        || (scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        && scope->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE)) return EINVAL;
    return wl_evaluation_control_test_source_early_report(
        scope->admission->source_bank,
        scope->lane, out);
}

static int
wl_session_test_source_quiescent_reset_cancel(wl_session_t *session,
    wl_evaluation_control_report_t *report, bool require_pending, bool reset)
{
    if (!session || !report || !session->owns_operation_admission
        || !session->evaluation_control || !session->operation_admission)
        return EINVAL;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) return EIO;
    int rc = EBUSY;
    if ((!reset || !admission->source_ticket.kind) && !admission->closing &&
        !admission->active_operations
        && (!require_pending || admission->controlled_step_pending) &&
        !admission->exclusive_attempt) {
        rc = wl_evaluation_control_test_source_early_report(
            admission->source_bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, report);
        if (!rc &&
            reset) rc = wl_evaluation_control_test_source_final_reset_cancel(
                admission->source_bank,
                WL_EVALUATION_CONTROL_SOURCE_OPERATION);
    }
    wl_session_checked_unlock(admission);
    return rc;
}

int
wl_session_test_source_pending_reset_cancel(wl_session_t *session,
    wl_evaluation_control_report_t *report)
{
    return wl_session_test_source_quiescent_reset_cancel(session, report, true,
               true);
}

int
wl_session_test_source_pending_report(wl_session_t *session,
    wl_evaluation_control_report_t *report)
{
    return wl_session_test_source_quiescent_reset_cancel(session, report, true,
               false);
}

int
wl_session_test_source_unstarted_reset_cancel(wl_session_t *session,
    wl_evaluation_control_report_t *report)
{
    return wl_session_test_source_quiescent_reset_cancel(session, report, false,
               true);
}

int
wl_session_test_source_parked_close_reset_cancel(wl_session_t *session,
    wl_evaluation_control_report_t *report)
{
    if (!session || !report || !session->owns_operation_admission
        || !session->evaluation_control || !session->operation_admission)
        return EINVAL;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) return EIO;
    int rc = EBUSY;
    if (admission->closing && admission->close_phase == WL_SESSION_CLOSE_PARKED
        && !admission->close_owner && !admission->active_operations
        && !admission->exclusive_attempt && admission->source_bank) {
        rc = wl_evaluation_control_test_source_early_report(
            admission->source_bank, WL_EVALUATION_CONTROL_SOURCE_CLOSE, report);
        if (!rc) rc = wl_evaluation_control_test_source_final_reset_cancel(
                admission->source_bank, WL_EVALUATION_CONTROL_SOURCE_CLOSE);
    }
    wl_session_checked_unlock(admission);
    return rc;
}

int
wl_session_test_source_final_reset_cancel(wl_session_t *session)
{
    if (!wl_session_source_final_issuer(session)) return EINVAL;
    const wl_session_source_scope_t *issuer = wl_session_source_scope;
    return wl_evaluation_control_test_source_final_reset_cancel(
        issuer->admission->source_bank, issuer->lane);
}
#endif

static wl_session_source_final_close_value_t
wl_session_source_final_close_value(
    wl_session_source_finance_close_result_t closed)
{
    return (wl_session_source_final_close_value_t){
               closed.protocol, closed.source_result, closed.checkpoint_result,
               closed.applied, closed.closed
    };
}

wl_session_source_final_pair_result_t
wl_session_source_finance_close_then_final_metadata(wl_session_t *session,
    wl_session_source_finance_t *record, wl_session_source_close_scope_t *scope,
    wl_evaluation_control_source_action_fn metadata, void *payload,
    size_t payload_bytes,
    void *journal, size_t journal_bytes)
{
    wl_session_source_final_pair_result_t out = {0};
    if (!wl_session_source_final_issuer(session)) {
        out.protocol = EINVAL; return out;
    }
    out.protocol = wl_session_source_close_scope_prove(session, record, scope);
    if (out.protocol) return out;
    if (scope->result.closed) {
        out.protocol = EALREADY; return out;
    }
    if (!metadata || !payload || !payload_bytes || !journal || !journal_bytes) {
        out.protocol = EINVAL; return out;
    }
    static const wl_evaluation_control_source_pair_policy_t policy = {208, 144,
                                                                      144, 80};
    wl_evaluation_control_source_final_pair_geometry_t geometry = {
        .session = {session, sizeof(*session)},
        .record = {record, sizeof(*record)}, .scope = {scope, sizeof(*scope)},
        .payload = {payload, payload_bytes},
        .journal = {journal, journal_bytes},
        .source_control = record->control,
        .source_expectation = wl_session_source_close_expectation(record, scope)
    };
    wl_session_source_scope_t *issuer = wl_session_source_scope;
    int ending = wl_session_source_observation_end(issuer);
    if (ending) out.phase.financial_result = ending;
    else out.phase = wl_evaluation_control_source_bank_run_lexical_pair_final(
            issuer->admission->source_bank, issuer->lane, &policy, &geometry,
            wl_session_source_close_action, metadata);
    if (!out.phase.first_called) {
        /* This distinct event closes only the genuine original financial
         * owner. It never rewrites the original pair's call or phase bits. */
        wl_evaluation_control_lexical_finance_expectation_t expected =
            wl_session_source_close_expectation(record, scope);
        out.protocol =
            wl_evaluation_control_lexical_finance_prove_checked(record->control,
                &expected);
        if (out.protocol) return out;
        int checkpoint = record->active
            ? wl_evaluation_control_charge_attempt(record->control, record->id,
                0) : 0;
        out.mandatory_unwind_called = true;
        int closed = wl_session_source_close_actual(scope);
        if (scope->result.closed) {
            if (checkpoint) scope->result.checkpoint_result = checkpoint;
            out.mandatory_unwind =
                wl_session_source_final_close_value(scope->result);
        } else {
            out.mandatory_unwind.protocol = closed ? closed : EINVAL;
            out.protocol = out.mandatory_unwind.protocol;
            return out;
        }
        if (out.phase.first_result) out.protocol = out.phase.first_result;
    }
    if (!scope->result.closed || record->control || record->id || record->active
        || record->attached || scope->control) {
        out.protocol = out.phase.first_called && out.phase.first_result
            ? out.phase.first_result : EINVAL;
        return out;
    }
    out.close = wl_session_source_final_close_value(scope->result);
    return out;
}

wl_evaluation_control_source_final_result_t
wl_session_source_final_metadata(wl_session_t *session,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn metadata, void *payload,
    size_t payload_bytes,
    void *journal, size_t journal_bytes)
{
    if (!wl_session_source_final_issuer(session) || !policy || !metadata
        || !payload || !payload_bytes || !journal || !journal_bytes)
        return (wl_evaluation_control_source_final_result_t){.financial_result =
                                                                 EINVAL};
    const wl_evaluation_control_source_final_geometry_t geometry = {
        .session = {session, sizeof(*session)},
        .payload = {payload, payload_bytes}, .journal = {journal, journal_bytes}
    };
    wl_session_source_scope_t *issuer = wl_session_source_scope;
    int ending = wl_session_source_observation_end(issuer);
    if (ending) return (wl_evaluation_control_source_final_result_t){.
                                                                     financial_result
                                                                         =
                                                                         ending};
    return wl_evaluation_control_source_bank_run_lexical_final(
        issuer->admission->source_bank, issuer->lane, policy, &geometry,
        metadata);
}

typedef struct {
    wl_session_source_finance_t *record;
    wl_session_source_close_scope_t *scope;
    wl_evaluation_control_t *control;
    wl_evaluation_control_lexical_finance_expectation_t expected;
    int protocol;
} wl_session_source_early_close_context_t;

static int
wl_session_source_early_close_action(void *payload)
{
    wl_session_source_early_close_context_t *p = payload;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_source_close_cancel_at ==
        WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_PHYSICAL) {
        wl_evaluation_control_test_source_close_cancel_at = 0;
        wl_evaluation_control_request_cancel(p->control);
    }
#endif
    wl_evaluation_control_lexical_finance_result_t closed =
        wl_evaluation_control_lexical_finance_close_checked(p->control,
            &p->expected);
    p->protocol = closed.protocol;
    if (closed.protocol) return closed.protocol;
    if (!closed.closed || !closed.report_valid) {
        p->protocol = EINVAL; return EINVAL;
    }
    /* Physical financial free -> original-address invalidation, no refusal. */
    p->record->control = NULL; p->record->id = 0;
    p->record->active = p->record->attached = false;
    p->scope->control = NULL;
    p->scope->result.source_result = closed.result;
    p->scope->result.checkpoint_result = closed.checkpoint_result;
    p->scope->result.applied = p->scope->result.closed = true;
    return closed.result;
}

wl_session_source_finance_close_early_result_t
wl_session_source_finance_close_early_owned(wl_session_t *session,
    wl_session_source_finance_t *record, wl_session_source_close_scope_t *scope)
{
    wl_session_source_finance_close_early_result_t out = {0};
    if (!wl_session_source_final_issuer(session)) {
        out.protocol = EINVAL; return out;
    }
    out.protocol = wl_session_source_close_scope_prove(session, record, scope);
    if (out.protocol) return out;
    if (scope->result.closed) {
        out.close = scope->result; return out;
    }
    if (!record->active || !record->id ||
        record->control == session->evaluation_control){
        out.protocol = EINVAL; return out;
    }
    wl_evaluation_control_lexical_finance_checkpoint_result_t checked =
        wl_evaluation_control_lexical_finance_checkpoint_owned(record->control,
            record, sizeof(*record), scope->control_identity, record->id,
            record->limit, record->charged);
    out.protocol = checked.protocol; out.owner_checked = checked.checked;
    out.authorizer_result = checked.checkpoint_result;
    if (checked.protocol || !checked.checked) return out;
    wl_session_source_early_close_context_t context = {
        .record = record, .scope = scope, .control = record->control,
        .expected = wl_session_source_close_expectation(record, scope)
    };
    wl_evaluation_control_source_close_borrow_t borrow = {
        context.control, context.expected.id, context.expected.identity
    };
    wl_session_source_scope_t *issuer = wl_session_source_scope;
    static const wl_evaluation_control_source_phase_policy_t policy = {208, 144,
                                                                       80};
    int ending = wl_session_source_observation_end(issuer);
    if (ending) out.phase.phase.financial_result = ending;
    else out.phase = wl_evaluation_control_source_bank_run_lexical_close_early(
            issuer->admission->source_bank, issuer->lane, &policy, &borrow,
            wl_session_source_early_close_action, &context);
    out.protocol = out.phase.protocol;
    if (!out.phase.phase.first_called) {
        /* The authentic private pair was never lent or mutated. This second
         * actual completion handoff precedes strict physical retirement. */
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_close_cancel_at ==
            WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_COMPLETION) {
            wl_evaluation_control_test_source_close_cancel_at = 0;
            wl_evaluation_control_request_cancel(context.control);
        }
#endif
        out.completion_called = true;
        out.completion_result = wl_evaluation_control_charge_attempt(
            context.control, context.expected.id, 0);
        out.mandatory_unwind_called = true;
        (void)wl_session_source_early_close_action(&context);
    }
    if (!scope->result.closed) {
        out.protocol = context.protocol ? context.protocol : EINVAL;
        out.close = scope->result;
        out.close.protocol = out.protocol;
        return out;
    }
    scope->result.phase = (wl_evaluation_control_source_phase_result_t){
        out.phase.phase.financial_result, out.phase.phase.first_result,
        out.phase.phase.first_called
    };
    out.close = scope->result;
    return out;
}

#undef WL_SESSION_THREAD_LOCAL

static void
wl_session_gate_fatal(void)
{
    abort();
}

static void
wl_session_checked_unlock(wl_session_admission_t *admission)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_fail_next_admission_unlock) {
        wl_session_fail_next_admission_unlock = false;
        wl_session_gate_fatal();
    }
#endif
    if (wl_mutex_unlock(&admission->mutex) != 0)
        wl_session_gate_fatal();
}

static int
wl_session_admission_lock(wl_session_admission_t *admission)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_fail_next_admission_lock) {
        wl_session_fail_next_admission_lock = false;
        return -1;
    }
#endif
    return wl_mutex_lock(&admission->mutex);
}

#ifdef WL_SESSION_TEST_HOOKS
void
wl_session_testhook_fail_next_admission_lock(void)
{
    wl_session_fail_next_admission_lock = true;
}

void
wl_session_testhook_fail_next_admission_unlock(void)
{
    wl_session_fail_next_admission_unlock = true;
}
#endif

static wl_session_admission_t *
wl_session_admission_create(void)
{
    wl_session_admission_t *admission =
        (wl_session_admission_t *)calloc(1, sizeof(*admission));
    if (!admission)
        return NULL;
    if (wl_mutex_init(&admission->mutex) != 0) {
        free(admission);
        return NULL;
    }
    if (wl_cond_init(&admission->idle) != 0) {
        wl_mutex_destroy(&admission->mutex);
        free(admission);
        return NULL;
    }
    return admission;
}

/* Committed teardown owns no remaining financial issuer. */
static void
wl_session_admission_free(wl_session_admission_t *admission)
{
    if (!admission) return;
    if (admission->source_bank ||
        admission->source_ticket.kind) wl_session_gate_fatal();
    wl_cond_destroy(&admission->idle);
    wl_mutex_destroy(&admission->mutex);
    free(admission);
}

/* Unescaped constructor rollback, and the distinct legacy NULL-prepare
 * compatibility callback, still own an actual live BANK to retire. */
static void
wl_session_admission_destroy(wl_session_admission_t *admission)
{
    if (!admission) return;
    if (admission->source_ticket.kind) wl_session_gate_fatal();
    if (wl_evaluation_control_source_bank_destroy(&admission->source_bank))
        wl_session_gate_fatal();
    wl_session_admission_free(admission);
}

static int
wl_session_operation_begin(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return 0;

    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->source_ticket.kind || admission->closing ||
        admission->controlled_step_pending
        || admission->controlled_step_complete ||
        admission->exclusive_attempt) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    admission->active_operations++;
    wl_session_checked_unlock(admission);
    return 0;
}

static void wl_session_operation_end(wl_session_t *session);

/* Reserve the operation before querying backend mutation policy, so neither
 * STEP nor an exclusive attempt can race authorization and commit. */
static int
wl_session_operation_begin_mutation(wl_session_t *session, bool insert,
    const char *relation, uint32_t num_rows, uint32_t num_cols,
    bool *controlled_mutation)
{
    *controlled_mutation = false;
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return 0;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->source_ticket.kind || admission->closing ||
        admission->controlled_step_pending
        || admission->exclusive_attempt
        || (admission->controlled_step_complete
        && admission->active_operations != 0)) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    bool controlled_complete = admission->controlled_step_complete;
    admission->active_operations++;
    wl_session_checked_unlock(admission);
    if (!controlled_complete)
        return 0;

    wl_controlled_step_state_t state =
        session->backend && session->backend->session_controlled_step_state
        ? session->backend->session_controlled_step_state(session)
        : WL_CONTROLLED_STEP_IDLE;
    int rc = EBUSY;
    if (state == WL_CONTROLLED_STEP_COMPLETE
        || state == WL_CONTROLLED_STEP_RETRYABLE
        || state == WL_CONTROLLED_STEP_DIRTY) {
        if (insert && session->backend->session_controlled_step_authorize_insert
            && session->backend->session_controlled_step_authorize_insert(
                session, relation, num_rows, num_cols))
            return 0;
        if (session->backend->session_controlled_mutation_admit
            && session->backend->session_controlled_mutation_committed) {
            rc = session->backend->session_controlled_mutation_admit(session,
                    insert, relation, num_rows, num_cols);
            if (!rc)
                *controlled_mutation = true;
        }
    }
    if (rc)
        wl_session_operation_end(session);
    return rc;
}

/* A controlled step owns the session across calls while its framed host
 * continuation is pending. Admit the first call only from a quiescent
 * session; a resume is the only public operation allowed through the gate. */
/* Called only while actual quiescent admission is locked. No TLS scope is
 * invented before admission; this direct lease protects read-only SOURCE. */
static int
wl_session_source_gate_read_locked(wl_session_admission_t *admission,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_phase_result_t phase =
        wl_evaluation_control_source_bank_run_lexical(admission->source_bank,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, policy, action, payload);
    if (phase.financial_result) return phase.financial_result;
    return phase.action_called ? phase.action_result : EINVAL;
}

typedef struct {
    wl_session_t *session;
    wl_controlled_step_state_t state;
} wl_session_source_gate_state_t;

static int
wl_session_source_gate_state(void *payload)
{
    wl_session_source_gate_state_t *p = payload;
    p->state = p->session->backend->session_controlled_step_state(p->session);
    return 0;
}

int
wl_session_source_admission_state_read(wl_session_t *session,
    wl_controlled_step_state_t *state)
{
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 160, 80
    };
    wl_session_source_gate_state_t read = {session, WL_CONTROLLED_STEP_IDLE};
    wl_evaluation_control_source_phase_result_t phase =
        wl_session_source_run(session, &policy, wl_session_source_gate_state,
            &read);
    if (phase.financial_result) return phase.financial_result;
    if (!phase.action_called) return EINVAL;
    if (phase.action_result) return phase.action_result;
    *state = read.state;
    return 0;
}

/* Admission remains held throughout this single financial packet. Neither
 * backend presentation nor a Scope is consulted after a result was published. */
static int
wl_session_source_packet_step_locked(wl_session_t *session,
    wl_session_admission_t *admission)
{
    wl_session_source_ticket_t *ticket = &admission->source_ticket;
    if (ticket->session != session) {
        wl_session_checked_unlock(admission); return EINVAL;
    }
    if (ticket->kind != WL_SESSION_SOURCE_TICKET_PROTOCOL_ONLY
        && !wl_session_source_actor_current_equal(ticket->actor)) {
        int result = ticket->foreign_result;
        wl_session_checked_unlock(admission); return result;
    }
    wl_evaluation_control_source_retained_output_t out;
    int rc = wl_evaluation_control_source_financial_progress_owned(
        &admission->source_bank, &out);
    if (rc) {
        wl_session_checked_unlock(admission); return rc;
    }
    ticket->state = out.state;
    int result = ticket->pending_result;
    if (out.state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT)
        ticket->kind = WL_SESSION_SOURCE_TICKET_NONE;
    wl_session_checked_unlock(admission);
    return result;
}

/* Columnar Snapshot calls this before its first backend state inspection.
 * The actual Snapshot reservation is still acquired by its ordinary gate. */
bool
wl_session_source_retained_pending(const wl_session_t *session)
{
    if (!session || !session->operation_admission) return false;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) return true;
    bool pending = admission->source_ticket.kind !=
        WL_SESSION_SOURCE_TICKET_NONE;
    wl_session_checked_unlock(admission);
    return pending;
}

#ifdef WL_SESSION_TEST_HOOKS
/* No production caller creates persistent standby in this first unit. The
 * test path proves the installed canonical producer under real admission. */
#ifdef WL_SESSION_RETAINED_TEST_HOOKS
int
wl_session_test_source_retained_arm(wl_session_t *session)
{
    if (!session || !session->owns_operation_admission
        || !session->evaluation_control || !session->source_c1_admission
        || session->backend != wl_backend_columnar()
        || !session->operation_admission) return EINVAL;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) return EIO;
    if (admission->closing || admission->active_operations
        || admission->exclusive_attempt || admission->source_ticket.kind) {
        wl_session_checked_unlock(admission); return EBUSY;
    }
    wl_backend_pending_bind_t pending;
    const wl_backend_financial_offer_t *offer =
        session->backend->session_controlled_financial_offer(session, &pending);
    const void *root;
    wl_backend_financial_actor_t actor;
    int foreign_result = EBUSY, pending_result = EAGAIN;
    bool role = offer != NULL;
    if (offer) {
        const wl_session_source_metadata_recipe_t *r = offer->recipe;
        if (offer->identity != offer || offer->session != session
            || !r || r->identity != r || r->session != session
            || !r->root || !r->resolve) {
            wl_session_checked_unlock(admission); return EINVAL;
        }
        wl_session_source_metadata_preparation_t prepared;
        prepared.financial_record = (void *)offer;
        int rc = r->resolve(r, WL_SESSION_SOURCE_METADATA_OFFER_PROVE,
                &prepared);
        if (rc) {
            wl_session_checked_unlock(admission); return rc;
        }
        root = r->root;
        actor = *offer->actor;
        pending_result = offer->pending_result ==
            WL_EVALUATION_CONTROL_WORK_LIMIT
            ? WL_EVALUATION_CONTROL_WORK_LIMIT : EAGAIN;
        foreign_result = offer->foreign_result ==
            WL_EVALUATION_CONTROL_WORK_LIMIT
            ? WL_EVALUATION_CONTROL_WORK_LIMIT : EBUSY;
    } else {
        if (pending.refusal || !pending.root) {
            int rc = pending.refusal > 0 ? pending.refusal : EINVAL;
            wl_session_checked_unlock(admission); return rc;
        }
        int rc = wl_columnar_session_test_retained_bind_prove(session,
                pending.root);
        if (rc) {
            wl_session_checked_unlock(admission); return rc;
        }
        root = pending.root;
        /* This real held-admission boundary creates the financial actor. It
         * does not assert an inherited backend/native BIND lease. */
        actor = wl_session_source_actor_current();
    }
    wl_evaluation_control_source_retained_output_t out;
    int rc = wl_evaluation_control_source_standby_prepare_owned(
        &admission->source_bank, &out);
    if (rc) {
        wl_session_checked_unlock(admission); return rc;
    }
    wl_session_source_ticket_t *ticket = &admission->source_ticket;
    ticket->session = session; ticket->root = root; ticket->actor = actor;
    ticket->pending_result = pending_result;
    ticket->foreign_result = foreign_result;
    ticket->state = out.state;
    ticket->kind = role ?
        (out.state == WL_EVALUATION_CONTROL_SOURCE_STANDBY ?
        WL_SESSION_SOURCE_TICKET_PREPARED_ROLE :
        WL_SESSION_SOURCE_TICKET_ROLE_INITIAL_STOP) :
        (out.state == WL_EVALUATION_CONTROL_SOURCE_STANDBY ?
        WL_SESSION_SOURCE_TICKET_PREPARED_BIND :
        WL_SESSION_SOURCE_TICKET_BIND_INITIAL_STOP);
    rc = out.financial_result;
    wl_session_checked_unlock(admission);
    return rc;
}
#endif

int
wl_session_test_source_retained_view(wl_session_t *session,
    unsigned *kind, wl_evaluation_control_source_financial_view_t *out)
{
    if (!session || !session->owns_operation_admission || !kind || !out
        || !session->operation_admission) return EINVAL;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) return EIO;
    *kind = (unsigned)admission->source_ticket.kind;
    int rc = wl_evaluation_control_test_source_financial_view(
        &admission->source_bank, out);
    wl_session_checked_unlock(admission);
    return rc;
}
#endif

static int
wl_session_operation_begin_step(wl_session_t *session,
    wl_session_source_scope_t *scope)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission) {
        wl_session_source_scope_initialize(scope, session, admission,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, true);
        return 0;
    }
    if (wl_session_admission_lock(admission)) return EIO;
    if (admission->closing || admission->active_operations
        || admission->exclusive_attempt) {
        wl_session_checked_unlock(admission); return EBUSY;
    }
    wl_session_source_ticket_t *ticket = &admission->source_ticket;
    bool retained_role = ticket->kind == WL_SESSION_SOURCE_TICKET_PREPARED_ROLE;
    bool retained_bind = ticket->kind == WL_SESSION_SOURCE_TICKET_PREPARED_BIND;
    if (ticket->kind && !retained_role && !retained_bind)
        return wl_session_source_packet_step_locked(session, admission);
    if (ticket->kind && (ticket->session != session ||
        !wl_session_source_actor_current_equal(ticket->actor))) {
        int result = ticket->session !=
            session ? EINVAL : ticket->foreign_result;
        wl_session_checked_unlock(admission); return result;
    }
    wl_backend_pending_bind_t pending;
    const wl_backend_financial_offer_t *offer;
    if (session->backend->session_controlled_financial_offer)
        offer = session->backend->session_controlled_financial_offer(session,
                &pending);
    else {
        offer = NULL; pending.root = NULL; pending.refusal = 0;
    }
    if (offer) {
        if (retained_bind || (retained_role &&
            (!offer->recipe || offer->recipe->root != ticket->root))) {
            wl_session_checked_unlock(admission); return EINVAL;
        }
        if (offer->identity != offer || offer->session != session) {
            wl_session_checked_unlock(admission); return EINVAL;
        }
        if (*offer->busy) {
            wl_session_checked_unlock(admission); return EBUSY;
        }
        if (
#if defined(WL_HAVE_C11_THREADS)
            !thrd_equal(*offer->actor, thrd_current())
#elif defined(_WIN32) || defined(_WIN64)
            *offer->actor != GetCurrentThreadId()
#else
            !pthread_equal(*offer->actor, pthread_self())
#endif
            ) {
            int result = offer->foreign_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT
                ? WL_EVALUATION_CONTROL_WORK_LIMIT : EBUSY;
            wl_session_checked_unlock(admission); return result;
        }
        admission->controlled_step_pending = true;
        admission->active_operations = 1;
        int rc;
        if (retained_role) {
            scope->issued_context = (wl_evaluation_control_source_context_t){0};
            rc = wl_evaluation_control_source_metadata_attach_retained_owned(
                &admission->source_bank, &scope->issued_context,
                &scope->retained_output);
            if (rc || !scope->retained_output.opened) {
                if (!rc) {
                    ticket->state = scope->retained_output.state;
                    ticket->kind = WL_SESSION_SOURCE_TICKET_ROLE_INITIAL_STOP;
                }
                admission->active_operations = 0;
                int result = rc ? rc : ticket->pending_result;
                wl_session_checked_unlock(admission); return result;
            }
        } else rc = wl_evaluation_control_source_metadata_enter_issued(
                admission->source_bank, &scope->issued_context);
        if (rc) {
            /* Only this held reservation exists: no Scope/TLS was installed,
             * and non-OPEN never initialized context. Copy before unlock. */
            int result = rc < 0 ? -rc : offer->pending_result ==
                WL_EVALUATION_CONTROL_WORK_LIMIT ?
                WL_EVALUATION_CONTROL_WORK_LIMIT : EAGAIN;
            admission->active_operations = 0;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_session_fail_next_admission_unlock) {
                wl_session_fail_next_admission_unlock = false;
                wl_session_gate_fatal();
            }
#endif
            if (wl_mutex_unlock(&admission->mutex)) wl_session_gate_fatal();
            return result;
        }
        wl_session_source_scope_initialize(scope, session, admission,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, true);
        scope->metadata_mode = retained_role ?
            WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED :
            WL_SESSION_SOURCE_METADATA_ISSUED;
        scope->retained_protocol_only = false;
        scope->metadata_offer = offer;
        scope->metadata_recipe = offer->recipe;
        scope->issued_pending_result = offer->pending_result;
        wl_session_source_scope = scope;
        wl_session_checked_unlock(admission);
        return 0;
    }
    if (pending.refusal) {
        int result = pending.refusal > 0 ? pending.refusal : EINVAL;
        wl_session_checked_unlock(admission); return result;
    }
    if (pending.root) {
        if (retained_role || (retained_bind && pending.root != ticket->root)) {
            wl_session_checked_unlock(admission); return EINVAL;
        }
        if (!session->backend->session_controlled_metadata_bind_continue) {
            wl_session_checked_unlock(admission); return EINVAL;
        }
        admission->controlled_step_pending = true;
        admission->active_operations = 1;
        int rc;
        if (retained_bind) {
            scope->issued_context = (wl_evaluation_control_source_context_t){0};
            rc = wl_evaluation_control_source_metadata_attach_retained_owned(
                &admission->source_bank, &scope->issued_context,
                &scope->retained_output);
            if (rc || !scope->retained_output.opened) {
                if (!rc) {
                    ticket->state = scope->retained_output.state;
                    ticket->kind = WL_SESSION_SOURCE_TICKET_BIND_INITIAL_STOP;
                }
                admission->active_operations = 0;
                int result = rc ? rc : ticket->pending_result;
                wl_session_checked_unlock(admission); return result;
            }
        } else rc = wl_evaluation_control_source_metadata_enter_issued(
                admission->source_bank, &scope->issued_context);
        if (rc) {
            int result = rc < 0 ? -rc : EAGAIN;
            admission->active_operations = 0;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_session_fail_next_admission_unlock) {
                wl_session_fail_next_admission_unlock = false;
                wl_session_gate_fatal();
            }
#endif
            if (wl_mutex_unlock(&admission->mutex)) wl_session_gate_fatal();
            return result;
        }
        wl_session_source_scope_initialize(scope, session, admission,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION, true);
        scope->metadata_mode = retained_bind ?
            WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND :
            WL_SESSION_SOURCE_METADATA_ISSUED_BIND;
        scope->retained_protocol_only = false;
        scope->issued_pending_result = EAGAIN;
        scope->metadata_bind_root = pending.root;
        wl_session_source_scope = scope;
        wl_session_checked_unlock(admission);
        return 0;
    }
    if (ticket->kind) {
        wl_session_checked_unlock(admission); return EINVAL;
    }
    if (session->source_c1_admission) {
        int entered = wl_evaluation_control_source_observation_open(
            admission->source_bank, &scope->observation_context);
        if (entered) {
            int result = entered <
                0 ? -entered : WL_EVALUATION_CONTROL_CANCELLED;
            wl_session_checked_unlock(admission); return result;
        }
    }
    scope->pending_before_dispatch = admission->controlled_step_pending;
    admission->controlled_step_pending = true;
    admission->active_operations++;
    wl_session_source_scope_initialize(scope, session, admission,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, true);
    scope->observation_open = session->source_c1_admission;
    wl_session_checked_unlock(admission);
    return 0;
}

/* SNAPSHOT owns a synchronous operation, including internal evaluation and
 * the complete callback batch. Only its own stopped attempt can be retried;
 * a retained STEP continuation remains protected. */
static int
wl_session_operation_begin_controlled_snapshot(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission || !session->backend
        || !session->backend->session_controlled_step_state) return ENOTSUP;
    if (wl_session_admission_lock(admission) != 0) return EIO;
    int rc = EBUSY;
    if (!admission->source_ticket.kind && !admission->closing &&
        !admission->active_operations
        && !admission->controlled_step_pending &&
        !admission->exclusive_attempt) {
        wl_controlled_step_state_t state;
        if (session->source_c1_admission) {
            static const wl_evaluation_control_source_phase_policy_t policy = {
                212, 160, 96
            };
            wl_session_source_gate_state_t read = {session,
                                                   WL_CONTROLLED_STEP_IDLE};
            int read_rc = wl_session_source_gate_read_locked(admission, &policy,
                    wl_session_source_gate_state, &read);
            if (read_rc) {
                wl_session_checked_unlock(admission);
                return read_rc;
            }
            state = read.state;
        } else state = session->backend->session_controlled_step_state(session);
        if (state == WL_CONTROLLED_STEP_COMPLETE
            || (session->backend->session_controlled_snapshot_evaluates
            && (state == WL_CONTROLLED_STEP_IDLE
            || state == WL_CONTROLLED_STEP_DIRTY
            || state == WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE))) {
            admission->active_operations++;
            rc = 0;
        } else if (state == WL_CONTROLLED_STEP_IDLE) rc = ENOTSUP;
    }
    wl_session_checked_unlock(admission);
    return rc;
}

/* Observation tags are not logical states. Validate before enum conversion. */
static bool
wl_session_controlled_state_valid(int state)
{
    switch (state) {
    case WL_CONTROLLED_STEP_IDLE:
    case WL_CONTROLLED_STEP_PENDING:
    case WL_CONTROLLED_STEP_COMPLETE:
    case WL_CONTROLLED_STEP_RETRYABLE:
    case WL_CONTROLLED_STEP_INVALIDATED:
    case WL_CONTROLLED_STEP_DIRTY:
    case WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE:
    case WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING:
        return true;
    default:
        return false;
    }
}

wl_session_completed_owner_ticket_result_t
wl_session_completed_owner_ticket_reserve(wl_session_t *session,
    uint64_t operation_identity, uint64_t completed_attempt_id)
{
    wl_session_completed_owner_ticket_result_t out = {EINVAL, 0};
    const wl_session_source_scope_t *scope = wl_session_source_scope;
    if (!operation_identity || !completed_attempt_id
        || !wl_session_source_operation_scope_owned(session)
        || !scope->metadata_step
        || scope->metadata_mode != WL_SESSION_SOURCE_METADATA_NONE
        || wl_evaluation_control_active_attempt_id(
            session->evaluation_control) != completed_attempt_id) return out;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) {
        out.protocol = EIO; return out;
    }
    if (admission->active_operations != 1 || admission->closing
        || admission->exclusive_attempt || admission->completed_owner.selected)
        out.protocol = EBUSY;
    else if (admission->next_completed_owner_ticket == UINT64_MAX)
        out.protocol = EOVERFLOW;
    else {
        out.ticket = ++admission->next_completed_owner_ticket;
        out.protocol = 0;
    }
    wl_session_checked_unlock(admission);
    return out;
}

static bool
wl_session_completed_owner_shape(const wl_backend_controlled_completion_t *c)
{
    return c->selected
           && c->domain == WL_BACKEND_CONTROLLED_COMPLETION_POST_FINISH_TX
           && c->kind == WL_BACKEND_CONTROLLED_COMPLETION_STEP
           && c->operation_identity && c->ticket && c->completed_attempt_id;
}

static bool
wl_session_completed_owner_equal(const wl_backend_controlled_completion_t *a,
    const wl_backend_controlled_completion_t *b)
{
    return wl_session_completed_owner_shape(a)
           && wl_session_completed_owner_shape(b)
           && a->operation_identity == b->operation_identity
           && a->ticket == b->ticket
           && a->completed_attempt_id == b->completed_attempt_id
           && a->result == b->result;
}

/* No Native operation or backend callback runs under this lock. A foreign
 * STEP's genuine EBUSY still retains this descriptive owner. */
static void
wl_session_completed_owner_pending_end(wl_session_t *session,
    const wl_backend_controlled_completion_t *completion)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission || !wl_session_completed_owner_shape(completion)
        || completion->state != WL_CONTROLLED_STEP_PENDING)
        wl_session_gate_fatal();
    if (wl_session_admission_lock(admission)) wl_session_gate_fatal();
    if (admission->active_operations != 1
        || completion->ticket > admission->next_completed_owner_ticket
        || (admission->completed_owner.selected
        && !wl_session_completed_owner_equal(&admission->completed_owner,
        completion))) wl_session_gate_fatal();
    admission->completed_owner = *completion;
    admission->controlled_step_pending = true;
    admission->controlled_step_complete = false;
    admission->active_operations = 0;
    if (admission->closing && wl_cond_signal(&admission->idle))
        wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}

static void
wl_session_operation_end_step(wl_session_t *session,
    wl_controlled_step_state_t state, bool continuation_pending)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return;

    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    if (admission->completed_owner.selected) {
        if (state != WL_CONTROLLED_STEP_PENDING) wl_session_gate_fatal();
        continuation_pending = true;
    }
    admission->active_operations--;
    /* Only a STEP that returned EAGAIN with its live operation identity has
     * transferred a continuation to this caller. A backend EBUSY caused by a
     * separately parked frame must not poison session admission as though it
     * were a continuation created by this STEP call. */
    admission->controlled_step_pending = continuation_pending;
    if (admission->completed_owner.selected)
        admission->controlled_step_complete = false;
    if (state == WL_CONTROLLED_STEP_COMPLETE
        || state == WL_CONTROLLED_STEP_RETRYABLE
        || state == WL_CONTROLLED_STEP_INVALIDATED
        || state == WL_CONTROLLED_STEP_DIRTY
        || state == WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE)
        admission->controlled_step_complete = true;
    else if (state == WL_CONTROLLED_STEP_IDLE)
        admission->controlled_step_complete = false;
    if (admission->closing && admission->active_operations == 0)
        if (wl_cond_signal(&admission->idle) != 0)
            wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}

/* Actual admission epilogue shared by the compatibility and stable recipe
 * issuers. The closing waiter is real; no session/result access follows unlock. */
static int
wl_session_source_metadata_admission_end(wl_session_source_scope_t *scope,
    int pending_result)
{
    (void)wl_session_source_observation_end(scope);
    if (scope->observation_open) wl_session_gate_fatal();
    wl_session_source_scope = scope->previous;
    wl_session_admission_t *admission = scope->admission;
    if (wl_session_admission_lock(admission)) wl_session_gate_fatal();
    if (admission->active_operations != 1) wl_session_gate_fatal();
    if (scope->metadata_mode == WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED
        || scope->metadata_mode ==
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND) {
        wl_session_source_ticket_t *ticket = &admission->source_ticket;
        ticket->state = scope->retained_output.state;
        ticket->kind = scope->retained_protocol_only ?
            WL_SESSION_SOURCE_TICKET_PROTOCOL_ONLY :
            scope->metadata_mode == WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED ?
            WL_SESSION_SOURCE_TICKET_ROLE_RESULT :
            WL_SESSION_SOURCE_TICKET_BIND_RESULT;
        if (scope->retained_protocol_only) ticket->root = NULL;
    }
    admission->active_operations = 0;
    admission->controlled_step_pending = true;
    if (admission->closing && wl_cond_signal(&admission->idle))
        wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
    return pending_result;
}

/* Only the two pre-backend observations use this exact reservation rollback.
 * No logical state is inferred and no backend/Original operation is needed. */
static int
wl_session_source_unstarted_end(wl_session_source_scope_t *scope, int result)
{
    (void)wl_session_source_observation_end(scope);
    if (scope->observation_open) wl_session_gate_fatal();
    wl_session_source_scope = scope->previous;
    wl_session_admission_t *admission = scope->admission;
    if (wl_session_admission_lock(admission)) wl_session_gate_fatal();
    if (admission->active_operations != 1 || admission->exclusive_attempt)
        wl_session_gate_fatal();
    admission->active_operations = 0;
    if (scope->metadata_step)
        admission->controlled_step_pending = scope->pending_before_dispatch;
    if (admission->closing && wl_cond_signal(&admission->idle))
        wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
    return result;
}

#ifdef WL_SESSION_TEST_HOOKS
const wl_backend_financial_offer_t *(*wl_session_test_retained_claim_offer)(
    const wl_backend_financial_offer_t *);
void (*wl_session_test_before_source_observation)(wl_session_t *, bool);
void (*wl_session_test_metadata_observation_stop)(wl_session_t *, bool, int);
void (*wl_session_test_before_destroy_seal)(wl_session_t *);
void (*wl_session_test_after_destroy_seal)(wl_session_t *);
#endif

static int
wl_session_source_issued_continue(wl_session_source_scope_t *scope,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_prepare_fn prepare,
    const void *recipe,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    if (scope->metadata_mode == WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED
        || scope->metadata_mode ==
        WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND) {
        int rc = wl_evaluation_control_source_metadata_continue_retained_owned(
            &scope->issued_context, policy, prepare, recipe, prepared,
            &scope->retained_output);
        /* Genuine settlement initializes the output even on rejected recipe. */
        if (!prepared->entry.settled || scope->issued_context.identity)
            wl_session_gate_fatal();
        return rc ? rc : scope->retained_output.financial_result;
    }
    return wl_evaluation_control_source_metadata_continue_issued(
        &scope->issued_context, policy, prepare, recipe, prepared);
}

/* Actual OPEN belongs to this public frame and must settle on every exit. */
static int
wl_session_source_step_metadata_execute_issued(wl_session_source_scope_t *scope)
{
    if (scope->observation_open) wl_session_gate_fatal();
    const wl_session_source_metadata_recipe_t *r = scope->metadata_recipe;
    wl_session_source_metadata_preparation_t prepared;
    prepared.source = (wl_evaluation_control_source_metadata_preparation_t){0};
    /* CLAIM compares this actual returned offer with the canonical Root slot.
    * Core group0 later overwrites this temporary view with financial data. */
#ifdef WL_SESSION_TEST_HOOKS
    if (scope->metadata_mode == WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED
        && wl_session_test_retained_claim_offer)
        scope->metadata_offer =
            wl_session_test_retained_claim_offer(scope->metadata_offer);
#endif
    prepared.financial_record = (void *)scope->metadata_offer;
    int protocol = !r || r->identity != r || r->session != scope->session
        || !r->root || !r->resolve ? EINVAL :
        r->resolve(r, WL_SESSION_SOURCE_METADATA_CLAIM, &prepared);
    if (protocol) {
        scope->retained_protocol_only = true;
        (void)wl_session_source_issued_continue(
            scope, NULL, NULL, NULL, &prepared.source);
        if (!prepared.source.entry.settled || scope->issued_context.identity)
            wl_session_gate_fatal();
        /* CLAIM failed before its final busy publication. No unproved or
         * already-busy slot belongs to this rejected invocation. */
        return wl_session_source_metadata_admission_end(scope, protocol);
    }
    wl_session_source_metadata_result_t *result = r->result;
    result->before_phase = *r->phase;
    result->thread_cookie = &wl_session_thread_cookie;
    result->scheduled = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (r->resolve(r, WL_SESSION_SOURCE_METADATA_BEFORE, NULL))
        wl_session_gate_fatal();
#endif
    result->financial_result =
        wl_session_source_issued_continue(
        scope,
        wl_session_source_metadata_policy(scope->session, r),
        wl_session_source_metadata_prepare,
        r, &prepared.source);
    if (!prepared.source.entry.settled || scope->issued_context.identity)
        wl_session_gate_fatal();
    result->preparation_protocol = prepared.source.preparation_protocol;
    result->outcome = prepared.source.outcome;
    result->semantic_valid = prepared.source.result_published;
    if (r->resolve(r, WL_SESSION_SOURCE_METADATA_RESULT_STORED, NULL))
        wl_session_gate_fatal();
    result->ready = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (r->resolve(r, WL_SESSION_SOURCE_METADATA_AFTER, NULL))
        wl_session_gate_fatal();
#endif
    *r->busy = false; result->scheduled = false;
    return wl_session_source_metadata_admission_end(scope,
               scope->issued_pending_result);
}

static int
wl_session_source_step_metadata_execute_bind(wl_session_source_scope_t *scope)
{
    if (scope->observation_open) wl_session_gate_fatal();
    int rc = scope->session->backend->session_controlled_metadata_bind_continue(
        scope->session, scope->metadata_bind_root);
    if (scope->issued_context.identity) {
        scope->retained_protocol_only = true;
        wl_evaluation_control_source_metadata_preparation_t rejected;
        rejected.entry.settled = false;
        (void)wl_session_source_issued_continue(
            scope, NULL, NULL, NULL, &rejected);
        if (!rejected.entry.settled || scope->issued_context.identity)
            wl_session_gate_fatal();
    }
    return wl_session_source_metadata_admission_end(scope, rc);
}

static int
wl_session_source_step_metadata_execute_recipe(
    wl_session_source_scope_t *scope, int pending_result)
{
    const wl_session_source_metadata_recipe_t *recipe = scope->metadata_recipe;
    wl_session_source_metadata_result_t *result = recipe->result;
    /* Actual SELECT bound this immutable slot, kept busy, and returned
     * immediately through the backend. No user/diagnostic callback intervenes.
     * All mutable financial tuple/phase checks occur after real initial0. */
    if (scope != wl_session_source_scope
        || scope->metadata_mode != WL_SESSION_SOURCE_METADATA_RECIPE)
        wl_session_gate_fatal();
    wl_session_source_metadata_preparation_t prepared;
    prepared.source = (wl_evaluation_control_source_metadata_preparation_t){0};
    const wl_evaluation_control_source_phase_policy_t *policy =
        wl_session_source_metadata_policy(scope->session, recipe);
    int ending = wl_session_source_observation_end(scope);
    if (ending) {
        result->financial_result = ending;
        result->preparation_protocol = 0;
        result->outcome = (wl_evaluation_control_source_metadata_outcome_t){0};
        result->semantic_valid = false;
        if (scope->session->backend->session_controlled_financial_head_complete
            && recipe->resolve(recipe,
            WL_SESSION_SOURCE_METADATA_OBSERVATION_STOP, NULL))
            wl_session_gate_fatal();
        result->ready = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_session_test_metadata_observation_stop)
            wl_session_test_metadata_observation_stop(scope->session, true,
                ending);
#endif
    } else {
        result->financial_result =
            wl_evaluation_control_source_bank_run_metadata_recipe(
            scope->admission->source_bank, scope->lane, policy,
            wl_session_source_metadata_prepare, recipe, &prepared.source);
        result->preparation_protocol = prepared.source.preparation_protocol;
        result->outcome = prepared.source.outcome;
        result->semantic_valid = prepared.source.result_published;
        if (scope->session->backend->session_controlled_financial_head_complete
            && recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_RESULT_STORED,
            NULL))
            wl_session_gate_fatal();
        result->ready = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (recipe->resolve(recipe, WL_SESSION_SOURCE_METADATA_AFTER, NULL))
            wl_session_gate_fatal();
#endif
    }
    *recipe->busy = false;
    result->scheduled = false;
    return wl_session_source_metadata_admission_end(scope, pending_result);
}

/* Backend routing has already returned before this issuer starts. Admission
 * still excludes every public mutation/destroy/reentry throughout settlement. */
static int
wl_session_source_step_metadata_execute(wl_session_t *session,
    wl_session_source_scope_t *scope, int pending_result)
{
    const wl_session_source_metadata_request_t *request = &scope->metadata;
    wl_session_source_metadata_result_t *result = request->result;
    if (scope != wl_session_source_scope || !scope->metadata_step
        || scope->metadata_mode != WL_SESSION_SOURCE_METADATA_REQUEST
        || scope->session != session || scope->admission !=
        session->operation_admission || !result || result->identity != result
        || !result->scheduled || result->ready || result->session != session
        || result->admission != scope->admission
        || result->owner != request->owner
        || result->operation_id != request->operation_id
        || result->operation_identity != request->operation_identity
        || result->before_phase != request->before_phase
        || result->kind != request->kind
        || *request->phase != request->before_phase
        || result->thread_cookie != &wl_session_thread_cookie
        || !*request->busy) wl_session_gate_fatal();
    static const wl_evaluation_control_source_phase_policy_t policy = {
        208, 224, 80
    };
    int ending = wl_session_source_observation_end(scope);
    if (ending) {
        result->financial_result = ending;
        result->preparation_protocol = 0;
        result->outcome = (wl_evaluation_control_source_metadata_outcome_t){0};
        result->semantic_valid = false;
        result->ready = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_session_test_metadata_observation_stop)
            wl_session_test_metadata_observation_stop(session, false, ending);
#endif
    } else {
        result->financial_result =
            wl_evaluation_control_source_bank_run_metadata(
            scope->admission->source_bank, scope->lane, &policy,
            request->action,
            request->payload, &result->outcome);
        result->ready = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (request->after) request->after(request->payload);
#endif
    }
    *request->busy = false;
    result->scheduled = false;
    /* No result/Step/session access follows the final admission unlock. */
    return wl_session_source_metadata_admission_end(scope, pending_result);
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_session_test_completed_pin_view(const wl_session_t *session,
    wl_session_test_completed_pin_t *out)
{
    if (!session || !out || !session->operation_admission) return EINVAL;
    wl_session_admission_t *admission = session->operation_admission;
    if (wl_session_admission_lock(admission)) return EIO;
    const wl_backend_controlled_completion_t *c = &admission->completed_owner;
    *out = (wl_session_test_completed_pin_t){
        .selected = c->selected,
        .active_operations = admission->active_operations,
        .ticket = c->ticket, .operation_identity = c->operation_identity,
        .completed_attempt_id = c->completed_attempt_id, .result = c->result
    };
    wl_session_checked_unlock(admission);
    return 0;
}
#endif

static void
wl_session_controlled_completion_end(wl_session_t *session,
    const wl_backend_controlled_completion_t *completion)
{
    wl_session_admission_t *admission = session->operation_admission;
    wl_controlled_step_state_t state = completion->state;
    if (!admission || !completion->selected || !completion->operation_identity
        || !completion->ticket || !session->backend
        || !session->backend->session_controlled_completion_commit)
        wl_session_gate_fatal();
#ifdef WL_SESSION_TEST_HOOKS
    if (completion->domain == WL_BACKEND_CONTROLLED_COMPLETION_POST_FINISH_TX
        && wl_session_test_completed_event)
        wl_session_test_completed_event(WL_SESSION_TEST_COMPLETED_BEFORE_COMMIT,
            session, completion, WL_BACKEND_COMPLETED_SOURCE_UNKNOWN, 0);
#endif
    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    if (admission->active_operations != 1)
        wl_session_gate_fatal();
    bool completed_tx = completion->domain ==
        WL_BACKEND_CONTROLLED_COMPLETION_POST_FINISH_TX;
    if (completed_tx) {
        if (!wl_session_completed_owner_equal(&admission->completed_owner,
            completion)) wl_session_gate_fatal();
    } else if (completion->domain != WL_BACKEND_CONTROLLED_COMPLETION_RESET
        || completion->completed_attempt_id
        || admission->completed_owner.selected) wl_session_gate_fatal();
    session->backend->session_controlled_completion_commit(session, completion);
    if (completed_tx) {
        admission->completed_owner.selected = false;
        admission->completed_owner.ticket = 0;
        admission->completed_owner.operation_identity = 0;
        admission->completed_owner.completed_attempt_id = 0;
        admission->completed_owner.domain =
            WL_BACKEND_CONTROLLED_COMPLETION_RESET;
    }
    admission->active_operations--;
    admission->controlled_step_pending = false;
    if (state == WL_CONTROLLED_STEP_COMPLETE
        || state == WL_CONTROLLED_STEP_RETRYABLE
        || state == WL_CONTROLLED_STEP_INVALIDATED
        || state == WL_CONTROLLED_STEP_DIRTY
        || state == WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE)
        admission->controlled_step_complete = true;
    else if (state == WL_CONTROLLED_STEP_IDLE)
        admission->controlled_step_complete = false;
    if (admission->closing && admission->active_operations == 0)
        if (wl_cond_signal(&admission->idle) != 0)
            wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}
/* Two actual public-call domains: an owned observation was consumed, or no
 * observation was required. Neither stands in for the old finisher receipt. */
static int
wl_session_completed_owner_source_return(wl_session_t *session,
    wl_session_source_scope_t *scope,
    const wl_backend_controlled_completion_t *completion,
    bool had_observation, bool was_ready, int backend_result)
{
    if (!wl_session_completed_owner_shape(completion)
        || !session->backend->session_controlled_completed_source_end
        || !session->backend->session_controlled_completion_commit
        || !session->owns_operation_admission || !scope->admission
        || scope->session != session
        || scope->admission != session->operation_admission
        || scope->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION
        || !scope->metadata_step
        || scope->metadata_mode != WL_SESSION_SOURCE_METADATA_NONE
        || had_observation != session->source_c1_admission
        || (!was_ready && (!backend_result
        || completion->state != WL_CONTROLLED_STEP_PENDING))
        || (was_ready && (!wl_session_controlled_state_valid(completion->state)
        || completion->state == WL_CONTROLLED_STEP_PENDING
        || completion->state == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING
        || completion->state == WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE)))
        wl_session_gate_fatal();
    bool semantic_return = was_ready && completion->invocation_accepted
        && backend_result == completion->result;
    bool progress_return = !was_ready && completion->invocation_accepted
        && backend_result == EAGAIN;
    if (!backend_result && !semantic_return) wl_session_gate_fatal();
    const wl_session_t *previous_postcall = wl_session_completed_postcall;
    wl_session_completed_postcall = session;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_test_completed_event)
        wl_session_test_completed_event(
            WL_SESSION_TEST_COMPLETED_BEFORE_SOURCE_END, session, completion,
            had_observation ? WL_BACKEND_COMPLETED_SOURCE_OBSERVATION :
            WL_BACKEND_COMPLETED_SOURCE_NOT_REQUIRED, 0);
#endif
    int financial = 0;
    wl_backend_completed_source_kind_t source_kind;
    if (had_observation) {
        if (wl_session_source_scope != scope) wl_session_gate_fatal();
        wl_session_source_step_checkpoint_result_t source =
            wl_session_source_step_checkpoint(session);
        if (!source.selected) wl_session_gate_fatal();
        int ended = wl_session_source_observation_end(scope);
        if (scope->observation_open || scope->observation_context.identity)
            wl_session_gate_fatal();
        financial = source.financial_result ? source.financial_result : ended;
        source_kind = WL_BACKEND_COMPLETED_SOURCE_OBSERVATION;
    } else {
        /* The old non-C1 path already restored TLS before capture. Reinstall
         * only this actual admitted scope for the descriptive endpoint. Its
         * never-opened observation_context must not be read or initialized. */
        if (scope->observation_open
            || wl_session_source_scope != scope->previous)
            wl_session_gate_fatal();
        wl_session_source_scope = scope;
        source_kind = WL_BACKEND_COMPLETED_SOURCE_NOT_REQUIRED;
    }
    wl_session_completed_source_event_t event = {
        .identity = &event, .scope = scope, .completion = completion,
        .kind = source_kind, .result = financial
    };
    const wl_session_completed_source_event_t *previous =
        wl_session_completed_source_event;
    wl_session_completed_source_event = &event;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_test_completed_event)
        wl_session_test_completed_event(
            WL_SESSION_TEST_COMPLETED_ENDPOINT_ACTIVE,
            session, completion, source_kind, financial);
#endif
    session->backend->session_controlled_completed_source_end(session,
        completion, source_kind, financial);
    wl_session_completed_source_event = previous;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_test_completed_event)
        wl_session_test_completed_event(
            WL_SESSION_TEST_COMPLETED_ENDPOINT_REVOKED, session, completion,
            source_kind, financial);
#endif
    wl_backend_controlled_completion_t after = {0};
    int observed_after = session->backend->session_controlled_step_observe(
        session, WL_BACKEND_CONTROLLED_COMPLETION_STEP, true, &after);
    if (!wl_session_completed_owner_equal(completion, &after)
        || after.invocation_accepted != completion->invocation_accepted
        || (observed_after !=
        WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETED_OWNER_PENDING
        && observed_after !=
        WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY))
        wl_session_gate_fatal();
    if ((observed_after ==
        WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETED_OWNER_PENDING
        && after.state != WL_CONTROLLED_STEP_PENDING)
        || (observed_after ==
        WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY
        && (!wl_session_controlled_state_valid(after.state)
        || after.state == WL_CONTROLLED_STEP_PENDING
        || after.state == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING
        || after.state == WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE)))
        wl_session_gate_fatal();
    wl_session_source_scope = scope->previous;
    if (!financial && observed_after ==
        WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY
        && (semantic_return || progress_return)) {
        wl_session_controlled_completion_end(session, &after);
        wl_session_completed_postcall = previous_postcall;
        return after.result;
    }
    /* Refused Source or foreign Native execution cannot consume HANDOFF. */
    after.state = WL_CONTROLLED_STEP_PENDING;
    wl_session_completed_owner_pending_end(session, &after);
    wl_session_completed_postcall = previous_postcall;
    return semantic_return ? EAGAIN : backend_result;
}

static void
wl_session_operation_end(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return;

    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    admission->active_operations--;
    if (admission->closing && admission->active_operations == 0)
        if (wl_cond_signal(&admission->idle) != 0)
            wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}

static void
wl_session_admission_close_and_wait(wl_session_t *session,
    wl_session_admission_t *admission)
{
    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    if (!admission->closing
        || admission->close_phase != WL_SESSION_CLOSE_LEGACY_WAIT
        || admission->close_owner != &wl_session_thread_cookie)
        wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
#ifdef WL_SESSION_TEST_HOOKS
    wl_session_testhook_admission_closed(session);
#endif
    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    while (admission->active_operations != 0)
        if (wl_cond_wait(&admission->idle, &admission->mutex) != 0)
            wl_session_gate_fatal();
    if (admission->source_ticket.kind) wl_session_gate_fatal();
    admission->close_phase = WL_SESSION_CLOSE_PREPARING;
    wl_session_checked_unlock(admission);
}

static bool
wl_session_exclusive_owner(const wl_session_exclusive_attempt_t *attempt)
{
    return attempt && attempt->self == attempt
           && attempt->owner_cookie == &wl_session_thread_cookie;
}

int
wl_session_exclusive_attempt_acquire(wl_session_t *session,
    wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!session || !attempt || attempt->self || attempt->session
        || attempt->held || attempt->entered || attempt->exit_pending
        || !session->owns_operation_admission)
        return EINVAL;
    admission = session->operation_admission;
    if (!admission) return EINVAL;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->source_ticket.kind || admission->closing ||
        admission->active_operations != 0
        || admission->controlled_step_pending
        || admission->controlled_step_complete ||
        admission->exclusive_attempt) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    attempt->self = attempt;
    attempt->session = session;
    attempt->owner_cookie = &wl_session_thread_cookie;
    attempt->held = true;
    admission->exclusive_attempt = attempt;
    wl_session_checked_unlock(admission);
    return 0;
}

int
wl_session_exclusive_attempt_enter(wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || attempt->entered || attempt->exit_pending
        || !attempt->session->operation_admission)
        return EINVAL;
    admission = attempt->session->operation_admission;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->exclusive_attempt != attempt
        || admission->active_operations != 0) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    admission->active_operations = 1;
    attempt->entered = true;
    wl_session_checked_unlock(admission);
    return 0;
}

int
wl_session_exclusive_attempt_mark_exit_pending(
    wl_session_exclusive_attempt_t *attempt)
{
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || !attempt->entered || attempt->exit_pending)
        return EINVAL;
    attempt->exit_pending = true;
    return 0;
}

int
wl_session_exclusive_attempt_leave(wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || !attempt->entered || !attempt->exit_pending
        || !attempt->session->operation_admission)
        return EINVAL;
    admission = attempt->session->operation_admission;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->exclusive_attempt != attempt
        || admission->active_operations != 1)
        wl_session_gate_fatal();
    admission->active_operations = 0;
    attempt->entered = false;
    attempt->exit_pending = false;
    if (admission->closing)
        wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
    return 0;
}

int
wl_session_exclusive_attempt_release(wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || attempt->entered || attempt->exit_pending
        || !attempt->session->operation_admission)
        return EINVAL;
    admission = attempt->session->operation_admission;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->exclusive_attempt != attempt
        || admission->active_operations != 0)
        wl_session_gate_fatal();
    admission->exclusive_attempt = NULL;
    attempt->held = false;
    attempt->session = NULL;
    attempt->owner_cookie = NULL;
    attempt->self = NULL;
    wl_session_checked_unlock(admission);
    return 0;
}

void
wl_session_options_init(wl_session_options_t *options)
{
    if (!options)
        return;
    options->size = (uint32_t)sizeof(*options);
    options->version = WL_SESSION_OPTIONS_VERSION;
    options->windows_job_handle = NULL;
    options->memory_governor = NULL;
    options->evaluation_control = NULL;
    options->spill_directory = NULL;
    options->spill_quota_bytes = 0;
    options->spill_max_row_bytes = 0;
    options->spill_join_batch_bytes = 0;
    options->spill_join_index_bytes = 0;
}

#ifdef WL_SESSION_TEST_HOOKS
/* Only compiled into direct-source test executables (never libwirelog):
 * a per-thread default substituted when a creator passes no options, so
 * facade paths without an options parameter can run under an injected
 * governor. */
#if defined(_MSC_VER)
#define WL_SESSION_THREAD_LOCAL __declspec(thread)
#else
#define WL_SESSION_THREAD_LOCAL _Thread_local
#endif
static WL_SESSION_THREAD_LOCAL const wl_session_options_t *testhook_options;
static wl_session_testhook_fn testhook_admission_closed;
static wl_session_testhook_fn testhook_before_workqueue_drain;
static wl_session_testhook_fn testhook_after_worker_lease_release;
#undef WL_SESSION_THREAD_LOCAL

void
wl_session_testhook_set_default_options(const wl_session_options_t *options)
{
    testhook_options = options;
}

const wl_session_options_t *
wl_session_testhook_default_options(void)
{
    return testhook_options;
}

void
wl_session_testhook_set_admission_closed(wl_session_testhook_fn fn)
{
    testhook_admission_closed = fn;
}

void
wl_session_testhook_admission_closed(wl_session_t *session)
{
    if (testhook_admission_closed)
        testhook_admission_closed(session);
}

void
wl_session_testhook_set_before_workqueue_drain(wl_session_testhook_fn fn)
{
    testhook_before_workqueue_drain = fn;
}

void
wl_session_testhook_before_workqueue_drain(wl_session_t *session)
{
    if (testhook_before_workqueue_drain)
        testhook_before_workqueue_drain(session);
}

void
wl_session_testhook_set_after_worker_lease_release(wl_session_testhook_fn fn)
{
    testhook_after_worker_lease_release = fn;
}

void
wl_session_testhook_after_worker_lease_release(wl_session_t *session)
{
    if (testhook_after_worker_lease_release)
        testhook_after_worker_lease_release(session);
}
#endif

static int
wl_session_options_normalize(const wl_session_options_t *options,
    wl_session_options_t *normalized)
{
    /* Frozen prefixes: never access old storage through a current lvalue. */
    typedef struct {
        uint32_t size;
        uint32_t version;
        void *windows_job_handle;
        wl_columnar_memory_governor_ref_t *memory_governor;
    } wl_session_options_v2_t;
    typedef struct {
        uint32_t size;
        uint32_t version;
        void *windows_job_handle;
        wl_columnar_memory_governor_ref_t *memory_governor;
        wl_evaluation_control_t *evaluation_control;
    } wl_session_options_v3_t;
    typedef struct {
        uint32_t size, version;
        void *windows_job_handle;
        wl_columnar_memory_governor_ref_t *memory_governor;
        wl_evaluation_control_t *evaluation_control;
        const char *spill_directory;
        uint64_t spill_quota_bytes, spill_max_row_bytes;
    } wl_session_options_v4_t;
    uint32_t size;
    uint32_t version;
    wl_session_options_init(normalized);
    if (!options)
        return 1;
    memcpy(&size, options, sizeof(size));
    if (size < sizeof(size) + sizeof(version))
        return 0;
    memcpy(&version, (const char *)options + sizeof(size), sizeof(version));
    if (version == 2 && size >= sizeof(wl_session_options_v2_t)) {
        wl_session_options_v2_t legacy;
        memcpy(&legacy, options, sizeof(legacy));
        normalized->windows_job_handle = legacy.windows_job_handle;
        normalized->memory_governor = legacy.memory_governor;
        return 1;
    }
    if (version == 3 && size >= sizeof(wl_session_options_v3_t)) {
        wl_session_options_v3_t legacy;
        memcpy(&legacy, options, sizeof(legacy));
        normalized->windows_job_handle = legacy.windows_job_handle;
        normalized->memory_governor = legacy.memory_governor;
        normalized->evaluation_control = legacy.evaluation_control;
        return 1;
    }
    if (version == 4 && size >= sizeof(wl_session_options_v4_t)) {
        wl_session_options_v4_t legacy;
        memcpy(&legacy, options, sizeof(legacy));
        normalized->windows_job_handle = legacy.windows_job_handle;
        normalized->memory_governor = legacy.memory_governor;
        normalized->evaluation_control = legacy.evaluation_control;
        normalized->spill_directory = legacy.spill_directory;
        normalized->spill_quota_bytes = legacy.spill_quota_bytes;
        normalized->spill_max_row_bytes = legacy.spill_max_row_bytes;
        return 1;
    }
    if (version != WL_SESSION_OPTIONS_VERSION || size < sizeof(*normalized))
        return 0;
    memcpy(normalized, options, sizeof(*normalized));
    normalized->size = (uint32_t)sizeof(*normalized);
    return 1;
}

int
wl_session_create(const wl_compute_backend_t *backend, const wl_plan_t *plan,
    uint32_t num_workers, wl_session_t **out)
{
    return wl_session_create_with_snapshot_options(backend, plan,
               num_workers, NULL, NULL, out);
}

int
wl_session_create_with_snapshot(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot, wl_session_t **out)
{
    return wl_session_create_with_snapshot_options(backend, plan,
               num_workers, snapshot, NULL, out);
}

int
wl_session_create_with_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    return wl_session_create_with_snapshot_options(backend, plan,
               num_workers, NULL, options, out);
}

int
wl_session_create_with_snapshot_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot,
    const wl_session_options_t *options, wl_session_t **out)
{
    int rc;
    wl_session_admission_t *admission;
    wl_session_options_t normalized;
    wl_evaluation_control_t *control;
#ifdef WL_SESSION_TEST_HOOKS
    if (!options)
        options = testhook_options;
#endif
    if (!out)
        return -1;
    *out = NULL;
    if (!backend || !backend->session_create
        || !wl_session_options_normalize(options, &normalized))
        return -1;
    if (normalized.spill_directory) {
        if (!normalized.spill_directory[0] || !normalized.spill_quota_bytes
            || normalized.spill_max_row_bytes < 32)
            return EINVAL;
    } else if (normalized.spill_quota_bytes || normalized.spill_max_row_bytes) {
        return EINVAL;
    }
    control = normalized.evaluation_control;
    if ((normalized.spill_join_batch_bytes != 0)
        != (normalized.spill_join_index_bytes != 0))
        return EINVAL;
    if (normalized.spill_join_batch_bytes
        && (!normalized.spill_directory || control))
        return EINVAL;
    if ((control || normalized.spill_directory)
        && !backend->session_create_with_options)
        return ENOTSUP;

    if ((backend->session_controlled_step_observe != NULL)
        != (backend->session_controlled_completion_commit != NULL))
        return EINVAL;
    if (backend->session_controlled_metadata_bind_continue
        && !backend->session_controlled_financial_offer)
        return EINVAL;
    if (backend->session_controlled_financial_head_complete
        && (!backend->session_controlled_financial_offer
        || !backend->session_controlled_metadata_bind_continue))
        return EINVAL;

    admission = wl_session_admission_create();
    if (!admission)
        return ENOMEM;
    if (control) {
        rc = wl_evaluation_control_attach(control, admission);
        if (rc != 0) {
            wl_session_admission_destroy(admission);
            return rc;
        }
        /* Financial BANK birth precedes backend construction, publication
        * and the first evaluation BEGIN. No engine/native debt exists. */
        rc = wl_evaluation_control_source_bank_create(&admission->source_bank);
        if (rc) {
            if (wl_evaluation_control_detach(control, admission))
                wl_session_gate_fatal();
            wl_session_admission_destroy(admission);
            return rc;
        }
    }

    if (options && backend->session_create_with_options)
        rc = backend->session_create_with_options(plan, num_workers,
                &normalized, out);
    else
        rc = backend->session_create(plan, num_workers, out);
    if (rc == 0 && *out) {
        /* Ensure the backend pointer is correctly bound */
        (*out)->backend = backend;
        (*out)->input_sealed = false;
        (*out)->operation_admission = admission;
        (*out)->owns_operation_admission = true;
        (*out)->evaluation_control = control;
        (*out)->owns_evaluation_control = control != NULL;
        (*out)->source_financial_head = NULL;
        if (snapshot) {
            wirelog_extension_snapshot_retain(snapshot);
            (*out)->extension_snapshot = snapshot;
            (*out)->owns_extension_snapshot = true;
        }
    } else {
        if (control && wl_evaluation_control_detach(control, admission))
            wl_session_gate_fatal();
        wl_session_admission_destroy(admission);
        if (rc == 0)
            rc = -1;
    }
    return rc;
}

static void
wl_session_destroy_owned(wl_session_t *session,
    wl_session_admission_t *admission)
{
    const wl_compute_backend_t *backend;
    wirelog_extension_snapshot_t *snapshot;
    wl_evaluation_control_t *control;
    if (!session)
        return;
    if (admission)
        wl_session_admission_close_and_wait(session, admission);
    backend = session->backend;
    snapshot = session->owns_extension_snapshot
        ? session->extension_snapshot : NULL;
    control = session->owns_evaluation_control
        ? session->evaluation_control : NULL;
    if (backend && backend->session_destroy) {
        wl_session_source_scope_t scope;
        wl_session_source_scope_initialize(&scope, session, admission,
            WL_EVALUATION_CONTROL_SOURCE_CLOSE, false);
        if (control && admission) wl_session_source_scope = &scope;
        backend->session_destroy(session);
        if (control && admission) wl_session_source_scope = scope.previous;
    }
    wirelog_extension_snapshot_release(snapshot);
    if (control)
        (void)wl_evaluation_control_detach(control, admission);
    wl_session_admission_destroy(admission);
}

/* All callers already own one close claim. Only the prepared route may
 * return a refusal; the NULL-prepare void compatibility callback is final. */
static int
wl_session_destroy_claimed(wl_session_t *session,
    wl_session_admission_t *admission, wl_session_t **session_slot)
{
    const wl_compute_backend_t *backend = session->backend;
    wirelog_extension_snapshot_t *snapshot = session->owns_extension_snapshot
        ? session->extension_snapshot : NULL;
    wl_evaluation_control_t *control = session->owns_evaluation_control
        ? session->evaluation_control : NULL;
    bool prepared = backend && backend->session_destroy_prepare;
    wl_session_source_scope_t scope;
    bool scoped = control && admission->source_bank;
    if (scoped) {
        wl_session_source_scope_initialize(&scope, session, admission,
            WL_EVALUATION_CONTROL_SOURCE_CLOSE, false);
        wl_session_source_scope = &scope;
    }
    const bool checked = prepared && scoped && session_slot != NULL;
    wl_session_checked_close_scope_t checked_scope;
    const wl_session_checked_close_scope_t *previous_checked = NULL;
    if (checked) {
        /* Only try_destroy passes a slot, after its same-lock exclusive
         * PREPARING/current-close-owner claim. The legacy void path passes
         * NULL and cannot acquire this checked-only mark. */
        previous_checked = wl_session_checked_close_scope;
        checked_scope.identity = &checked_scope;
        checked_scope.source = &scope;
        checked_scope.session = session;
        checked_scope.admission = admission;
        checked_scope.claim_cookie = &wl_session_thread_cookie;
        checked_scope.actor = wl_session_source_actor_current();
        wl_session_checked_close_scope = &checked_scope;
    }
    unsigned char commit_owner = 0;
    int rc = 0;
    if (prepared) {
        rc = backend->session_destroy_prepare(session);
        if (checked) wl_session_checked_close_scope = previous_checked;
        if (scoped) wl_session_source_scope = scope.previous;
        if (!rc) rc = wl_evaluation_control_source_bank_destroy(
                &admission->source_bank);
        if (!rc && control) {
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_session_test_before_destroy_seal)
                wl_session_test_before_destroy_seal(session);
#endif
            rc = wl_evaluation_control_transfer(control, admission,
                    &commit_owner);
#ifdef WL_SESSION_TEST_HOOKS
            if (!rc && wl_session_test_after_destroy_seal)
                wl_session_test_after_destroy_seal(session);
#endif
        }
        if (rc) {
            if (wl_session_admission_lock(admission)) wl_session_gate_fatal();
            if (!admission->closing
                || admission->close_phase != WL_SESSION_CLOSE_PREPARING
                || admission->close_owner != &wl_session_thread_cookie)
                wl_session_gate_fatal();
            admission->close_phase = WL_SESSION_CLOSE_PARKED;
            admission->close_owner = NULL;
            wl_session_checked_unlock(admission);
            return rc == WL_EVALUATION_CONTROL_CANCELLED
                   || rc == WL_EVALUATION_CONTROL_WORK_LIMIT ? EAGAIN : rc;
        }
    }
    if (wl_session_admission_lock(admission)) wl_session_gate_fatal();
    if (admission->source_ticket.kind || !admission->closing ||
        admission->active_operations
        || admission->close_phase != WL_SESSION_CLOSE_PREPARING
        || admission->close_owner != &wl_session_thread_cookie)
        wl_session_gate_fatal();
    admission->close_phase = WL_SESSION_CLOSE_COMMITTED;
    admission->close_owner = NULL;
    wl_session_checked_unlock(admission);
    if (session_slot) *session_slot = NULL;
    if (backend && backend->session_destroy) backend->session_destroy(session);
    /* Session may already be freed. Only captured owners remain below. */
    if (!prepared) {
        if (scoped) wl_session_source_scope = scope.previous;
        if (wl_evaluation_control_source_bank_destroy(&admission->source_bank))
            wl_session_gate_fatal();
        if (control && wl_evaluation_control_transfer(control, admission,
            &commit_owner)) wl_session_gate_fatal();
    }
    wirelog_extension_snapshot_release(snapshot);
    if (control && wl_evaluation_control_detach(control, &commit_owner))
        wl_session_gate_fatal();
    wl_session_admission_free(admission);
    return 0;
}

int
wl_session_try_destroy(wl_session_t **session_slot)
{
    if (!session_slot) return EINVAL;
    wl_session_t *session = *session_slot;
    if (!session) return 0;
    if (!session->owns_operation_admission) return EINVAL;
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission) return EINVAL;
    if (wl_session_admission_lock(admission)) return EIO;
    if (admission->source_ticket.kind) {
        if (admission->active_operations || admission->exclusive_attempt
            || admission->closing ||
            admission->close_phase != WL_SESSION_CLOSE_NONE) {
            wl_session_checked_unlock(admission); return EBUSY;
        }
        return wl_session_source_packet_step_locked(session, admission);
    }
    bool initial = admission->close_phase == WL_SESSION_CLOSE_NONE;
    bool resumed = admission->close_phase == WL_SESSION_CLOSE_PARKED;
    if ((!initial && !resumed) || (initial && admission->closing)
        || admission->active_operations || admission->exclusive_attempt
        || admission->controlled_step_pending
        || (session->evaluation_control &&
        wl_evaluation_control_active_attempt_id(
            session->evaluation_control))) {
        wl_session_checked_unlock(admission); return EBUSY;
    }
    admission->closing = true;
    admission->close_phase = WL_SESSION_CLOSE_PREPARING;
    admission->close_owner = &wl_session_thread_cookie;
    wl_session_checked_unlock(admission);
#ifdef WL_SESSION_TEST_HOOKS
    wl_session_testhook_admission_closed(session);
#endif
    return wl_session_destroy_claimed(session, admission, session_slot);
}

void
wl_session_destroy(wl_session_t *session)
{
    if (!session) return;
    wl_session_admission_t *admission = session->owns_operation_admission
        ? session->operation_admission : NULL;
    if (!admission) {
        wl_session_destroy_owned(session, NULL);
        return;
    }
    if (wl_session_admission_lock(admission)) wl_session_gate_fatal();
    bool parked = admission->source_ticket.kind || admission->closing
        || admission->close_phase != WL_SESSION_CLOSE_NONE
        || admission->exclusive_attempt || admission->controlled_step_pending
        || (session->evaluation_control &&
        wl_evaluation_control_active_attempt_id(
            session->evaluation_control));
    if (parked) {
        wl_session_checked_unlock(admission);
        fputs("wl_session_destroy: live attempt; use "
            "wl_session_try_destroy after completion/ACK\n", stderr);
        wl_session_gate_fatal();
    }
    /* Check and claim are one lock interval. No old unclaimed legacy waiter
     * can inherit a failed checked request between these two operations. */
    admission->closing = true;
    admission->close_phase = WL_SESSION_CLOSE_LEGACY_WAIT;
    admission->close_owner = &wl_session_thread_cookie;
    wl_session_checked_unlock(admission);
    wl_session_admission_close_and_wait(session, admission);
    if (wl_session_destroy_claimed(session, admission, NULL))
        wl_session_gate_fatal();
}

int
wl_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    int rc;
    bool controlled_mutation = false;
    if (!session)
        return -1;
    rc = wl_session_operation_begin_mutation(session, true, relation,
            num_rows, num_cols, &controlled_mutation);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_insert
        || session->input_load_failed)
        rc = -1;
    else {
        if (session->backend->session_budget_denial_clear)
            session->backend->session_budget_denial_clear(session);
        rc = session->backend->session_insert(session, relation, data,
                num_rows, num_cols);
    }
    if (controlled_mutation && rc == 0 && num_rows != 0
        && session->backend->session_controlled_mutation_committed)
        session->backend->session_controlled_mutation_committed(session, true,
            relation, num_rows, num_cols);
    wl_session_operation_end(session);
    return rc;
}

wl_columnar_memory_governor_t *
wl_session_memory_governor(wl_session_t *session)
{
    wl_columnar_memory_governor_t *governor = NULL;
    if (!session)
        return NULL;
    if (wl_session_operation_begin(session) != 0)
        return NULL;
    if (session->backend && session->backend->session_memory_governor)
        governor = session->backend->session_memory_governor(session);
    wl_session_operation_end(session);
    return governor;
}

int
wl_session_make_compound(wl_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args,
    uint64_t *handle_out)
{
    int rc;
    if (!session)
        return EINVAL;
    rc = wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_make_compound)
        rc = EINVAL;
    else
        rc = session->backend->session_make_compound(session, functor, arity,
                args, handle_out);
    wl_session_operation_end(session);
    return rc;
}

int
wl_session_remove(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    int rc;
    bool controlled_mutation = false;
    if (!session)
        return -1;
    rc = wl_session_operation_begin_mutation(session, false, relation,
            num_rows, num_cols, &controlled_mutation);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_remove)
        rc = -1;
    else {
        if (session->backend->session_budget_denial_clear)
            session->backend->session_budget_denial_clear(session);
        rc = session->backend->session_remove(session, relation, data,
                num_rows, num_cols);
    }
    if (controlled_mutation && rc == 0 && num_rows != 0
        && session->backend->session_controlled_mutation_committed)
        session->backend->session_controlled_mutation_committed(session, false,
            relation, num_rows, num_cols);
    wl_session_operation_end(session);
    return rc;
}

int
wl_session_step(wl_session_t *session)
{
    int rc;
    if (!session)
        return -1;
    bool controlled = session->evaluation_control != NULL;
    /* A non-columnar backend must opt in to the controlled STEP contract;
     * otherwise invoking its raw callback could run unbounded work. */
    if (controlled && (!session->backend
        || !session->backend->session_controlled_step_state))
        return ENOTSUP;
    wl_session_source_scope_t scope;
    if (controlled) {
        rc = wl_session_operation_begin_step(session, &scope);
        if (rc) return rc;
        if (scope.metadata_mode == WL_SESSION_SOURCE_METADATA_ISSUED_BIND
            || scope.metadata_mode ==
            WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED_BIND)
            return wl_session_source_step_metadata_execute_bind(&scope);
        if (scope.metadata_mode == WL_SESSION_SOURCE_METADATA_ISSUED
            || scope.metadata_mode ==
            WL_SESSION_SOURCE_METADATA_RETAINED_ISSUED)
            return wl_session_source_step_metadata_execute_issued(&scope);
    } else {
        rc = wl_session_operation_begin(session);
        if (rc) return rc;
    }
    bool dispatched = false;
    if (!controlled) wl_session_source_scope_initialize(&scope, session,
            session->operation_admission,
            WL_EVALUATION_CONTROL_SOURCE_OPERATION,
            true);
    if (controlled) {
        wl_session_source_scope = &scope;
        if (session->source_c1_admission) {
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_session_test_before_source_observation)
                wl_session_test_before_source_observation(session, false);
#endif
            rc = wl_session_source_footer(session);
            if (rc) return wl_session_source_unstarted_end(&scope, rc);
        }
    }
    if (!session->backend || !session->backend->session_step
        || session->input_load_failed)
        rc = -1;
    else {
        dispatched = true;
        rc = session->backend->session_step(session);
    }
    if (controlled &&
        scope.metadata_mode == WL_SESSION_SOURCE_METADATA_RECIPE) {
        if (!dispatched || (rc != EAGAIN
            && rc != WL_EVALUATION_CONTROL_WORK_LIMIT)) wl_session_gate_fatal();
        return wl_session_source_step_metadata_execute_recipe(&scope,
                   rc);
    }
    if (controlled && scope.metadata_mode ==
        WL_SESSION_SOURCE_METADATA_REQUEST) {
        if (!dispatched || (rc != EAGAIN
            && rc != WL_EVALUATION_CONTROL_WORK_LIMIT)) wl_session_gate_fatal();
        return wl_session_source_step_metadata_execute(session, &scope, rc);
    }
    if (controlled && !dispatched && scope.admission
        && session->source_c1_admission)
        return wl_session_source_unstarted_end(&scope, rc);
    bool capture_scoped = controlled && session->source_c1_admission
        && scope.admission && session->owns_operation_admission;
    if (controlled && !capture_scoped) wl_session_source_scope = scope.previous;
    if (controlled) {
        if (capture_scoped) (void)wl_session_source_step_checkpoint(session);
        wl_controlled_step_state_t state;
        if (session->backend
            && session->backend->session_controlled_step_observe) {
            wl_backend_controlled_completion_t completion = {0};
            int observed = session->backend->session_controlled_step_observe(
                session, WL_BACKEND_CONTROLLED_COMPLETION_STEP, dispatched,
                &completion);
            if (observed ==
                WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETED_OWNER_PENDING) {
                if (!dispatched) wl_session_gate_fatal();
                return wl_session_completed_owner_source_return(session,
                           &scope, &completion, capture_scoped, false, rc);
            }
            if (observed ==
                WL_BACKEND_CONTROLLED_OBSERVATION_COMPLETION_READY) {
                if (!dispatched || !completion.selected
                    || completion.kind != WL_BACKEND_CONTROLLED_COMPLETION_STEP
                    || !completion.operation_identity || !completion.ticket
                    || !wl_session_controlled_state_valid(completion.state))
                    wl_session_gate_fatal();
                int result = completion.result;
                if (completion.domain ==
                    WL_BACKEND_CONTROLLED_COMPLETION_POST_FINISH_TX)
                    return wl_session_completed_owner_source_return(session,
                               &scope, &completion, capture_scoped, true, rc);
                if (capture_scoped) {
                    (void)wl_session_source_step_checkpoint(session);
                    (void)wl_session_source_observation_end(&scope);
                    if (scope.observation_open) wl_session_gate_fatal();
                    wl_session_source_scope = scope.previous;
                }
                wl_session_controlled_completion_end(session, &completion);
                return result;
            }
            if (!wl_session_controlled_state_valid(observed))
                wl_session_gate_fatal();
            state = (wl_controlled_step_state_t)observed;
        } else {
            state = session->backend
                && session->backend->session_controlled_step_state
                ? session->backend->session_controlled_step_state(session)
                : WL_CONTROLLED_STEP_IDLE;
        }
        /* Only an actual owned observation refusal with an authenticated
         * retained Root selects this continuation. A pre-BEGIN IDLE result
         * and completed/foreign semantic outcomes keep their real result. */
        bool observation_pending = capture_scoped && scope.observation_stop
            && rc == scope.observation_stop
            && state == WL_CONTROLLED_STEP_PENDING;
        if (observation_pending) rc = EAGAIN;
        bool pending = observation_pending
            || state == WL_CONTROLLED_STEP_NATIVE_ERROR_CLEANUP_PENDING
            || (rc == EAGAIN
            && wl_evaluation_control_active_attempt_id(
                session->evaluation_control) != 0)
            || (rc == WL_EVALUATION_CONTROL_WORK_LIMIT
            && state == WL_CONTROLLED_STEP_PENDING);
        if (capture_scoped) {
            (void)wl_session_source_step_checkpoint(session);
            (void)wl_session_source_observation_end(&scope);
            if (scope.observation_open) wl_session_gate_fatal();
            wl_session_source_scope = scope.previous;
        }
        if (rc == EAGAIN && !pending)
            rc = EIO;
        wl_session_operation_end_step(session, state, pending);
    } else {
        wl_session_operation_end(session);
    }
    return rc;
}

/* Callback replacement is legal at a quiescent completed boundary. Keep the
 * generic operation gate closed for retained attempts and other operations. */
static int
wl_session_operation_begin_callback(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission) return 0;
    if (wl_session_admission_lock(admission) != 0) return EIO;
    int rc = EBUSY;
    if (!admission->source_ticket.kind && !admission->closing &&
        !admission->active_operations
        && !admission->controlled_step_pending &&
        !admission->exclusive_attempt) {
        wl_controlled_step_state_t state = session->backend
            && session->backend->session_controlled_step_state
            ? session->backend->session_controlled_step_state(session)
            : WL_CONTROLLED_STEP_IDLE;
        if ((!admission->controlled_step_complete
            && (!session->evaluation_control ||
            state == WL_CONTROLLED_STEP_IDLE))
            || state == WL_CONTROLLED_STEP_COMPLETE) {
            admission->active_operations++;
            rc = 0;
        }
    }
    wl_session_checked_unlock(admission);
    return rc;
}

void
wl_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
    void *user_data)
{
    if (!session)
        return;
    if (wl_session_delta_callback_is_current(session)) {
        if (session->backend && session->backend->session_set_delta_cb)
            session->backend->session_set_delta_cb(session, callback,
                user_data);
        return;
    }
    if (wl_session_operation_begin_callback(session) != 0)
        return;
    if (session->backend && session->backend->session_set_delta_cb)
        session->backend->session_set_delta_cb(session, callback, user_data);
    wl_session_operation_end(session);
}

int
wl_session_snapshot(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data)
{
    int rc;
    if (!session)
        return -1;
    bool controlled = session->evaluation_control != NULL;
    rc = controlled ? wl_session_operation_begin_controlled_snapshot(session)
        : wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    wl_session_source_scope_t scope;
    wl_session_source_scope_initialize(&scope, session,
        session->operation_admission,
        WL_EVALUATION_CONTROL_SOURCE_OPERATION, false);
    if (controlled) {
        wl_session_source_scope = &scope;
        if (session->source_c1_admission) {
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_session_test_before_source_observation)
                wl_session_test_before_source_observation(session, true);
#endif
            rc = wl_session_source_footer(session);
            if (rc) return wl_session_source_unstarted_end(&scope, rc);
        }
    }
    if (!session->backend || !session->backend->session_snapshot
        || session->input_load_failed)
        rc = -1;
    else if (controlled && !callback)
        rc = EINVAL;
    else
        rc = session->backend->session_snapshot(session, callback, user_data);
    if (controlled) wl_session_source_scope = scope.previous;
    if (controlled)
        wl_session_operation_end_step(session,
            session->backend && session->backend->session_controlled_step_state
                ? session->backend->session_controlled_step_state(session)
                : WL_CONTROLLED_STEP_IDLE, false);
    else
        wl_session_operation_end(session);
    return rc;
}

bool
wl_session_budget_denied(wl_session_t *session)
{
    wl_session_admission_t *admission;
    bool denied;

    if (!session)
        return false;
    admission = session->operation_admission;
    if (admission) {
        if (wl_session_admission_lock(admission) != 0)
            return false;
        if (admission->source_ticket.kind || admission->closing ||
            admission->exclusive_attempt) {
            wl_session_checked_unlock(admission);
            return false;
        }
        /* A parked or completed controlled STEP is a quiescent boundary.
         * Read it under the admission lock without taking an operation, so
         * polling never makes the owner's resume or an admitted mutation see
         * EBUSY; a call that is running still reads false (#2351). */
        if (admission->controlled_step_pending
            || admission->controlled_step_complete) {
            denied = admission->active_operations == 0 && session->backend
                && session->backend->session_budget_denied
                && session->backend->session_budget_denied(session);
            wl_session_checked_unlock(admission);
            return denied;
        }
        admission->active_operations++;
        wl_session_checked_unlock(admission);
    }
    denied = session->backend && session->backend->session_budget_denied
        && session->backend->session_budget_denied(session);
    wl_session_operation_end(session);
    return denied;
}

wirelog_source_query_status_t
wl_session_inspection_begin(wl_session_t *session)
{
    wl_session_admission_t *admission;
    bool busy;

    if (!session)
        return WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT;
    /* Controlled sessions are outside v1.  Refuse before the lock so the
     * control, its fuel and attempt identity stay exactly as they were. */
    if (session->evaluation_control)
        return WIRELOG_SOURCE_QUERY_UNSUPPORTED;
    if (wl_session_delta_callback_is_current(session))
        return WIRELOG_SOURCE_QUERY_BUSY;
    admission = session->operation_admission;
    if (!admission)
        return WIRELOG_SOURCE_QUERY_UNSUPPORTED;
    if (wl_session_admission_lock(admission) != 0)
        return WIRELOG_SOURCE_QUERY_INTERNAL_ERROR;
    /* Unlike wl_session_operation_begin(), a nested reservation is refused:
     * any active operation means a callback may be calling back in, or
     * another call is mid-flight. */
    busy = admission->source_ticket.kind || admission->closing ||
        admission->active_operations != 0
        || admission->controlled_step_pending
        || admission->controlled_step_complete
        || admission->exclusive_attempt;
    if (!busy)
        admission->active_operations++;
    wl_session_checked_unlock(admission);
    if (busy)
        return WIRELOG_SOURCE_QUERY_BUSY;
    if (session->input_sealed || session->input_load_failed) {
        wl_session_operation_end(session);
        return WIRELOG_SOURCE_QUERY_UNSUPPORTED;
    }
    return WIRELOG_SOURCE_QUERY_OK;
}

void
wl_session_inspection_end(wl_session_t *session)
{
    if (session)
        wl_session_operation_end(session);
}

wirelog_source_query_status_t
wl_session_source_query(wl_session_t *session, const wl_source_query_t *query,
    wl_source_query_result_t *result)
{
    if (!session || !query || !result)
        return WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT;
    result->count = 0;
    result->exists = false;
    if (!session->backend || !session->backend->session_source_query)
        return WIRELOG_SOURCE_QUERY_UNSUPPORTED;
    return session->backend->session_source_query(session, query, result);
}
