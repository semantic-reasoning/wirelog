/*
 * backend/columnar_nanoarrow.c - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#define _GNU_SOURCE

#include "columnar/compound_side.h"
#include "columnar/internal.h"
#include "columnar/batch_payload.h"
#include "columnar/columnar_nanoarrow.h"
#include "columnar/join_batch.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    COL_CONTROLLED_PREFLIGHT_IDLE = 0,
    COL_CONTROLLED_PREFLIGHT_SHAPE,
    COL_CONTROLLED_PREFLIGHT_TARGET,
    COL_CONTROLLED_PREFLIGHT_VARIABLE_FULL,
    COL_CONTROLLED_PREFLIGHT_VARIABLE_DELTA,
    COL_CONTROLLED_PREFLIGHT_JOIN_FULL,
    COL_CONTROLLED_PREFLIGHT_JOIN_DELTA,
    COL_CONTROLLED_PREFLIGHT_READY
} col_controlled_preflight_phase_t;

typedef enum {
    COL_CONTROLLED_DEFAULT_PREFLIGHT_IDLE = 0,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_PLAN,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_STRATUM,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_RELATION,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_OPERATOR,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_SLOT,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_BYTE,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_EXPECTATION,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_NEXT_RELATION,
    COL_CONTROLLED_DEFAULT_PREFLIGHT_READY
} col_controlled_default_preflight_phase_t;

typedef struct session_rel_registration session_rel_registration_t;

typedef enum {
    COL_CONTROLLED_RECURSIVE_INERT = 0,
    COL_CONTROLLED_RECURSIVE_INIT,
    COL_CONTROLLED_RECURSIVE_ACQUIRE_EDGE,
    COL_CONTROLLED_RECURSIVE_BUILD_WORKING,
    COL_CONTROLLED_RECURSIVE_BUILD_INITIAL_FRONTIER,
    COL_CONTROLLED_RECURSIVE_BEGIN_BODY,
    COL_CONTROLLED_RECURSIVE_RUN_BODY,
    COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY,
    COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_ACK,
    COL_CONTROLLED_RECURSIVE_BUILD_CANDIDATE,
    COL_CONTROLLED_RECURSIVE_COPY_CANDIDATE,
    COL_CONTROLLED_RECURSIVE_COPY_FRONTIER,
    COL_CONTROLLED_RECURSIVE_RETIRE_BODY_RESULT,
    COL_CONTROLLED_RECURSIVE_BEGIN_MERGE,
    COL_CONTROLLED_RECURSIVE_RUN_MERGE,
    COL_CONTROLLED_RECURSIVE_TAKE_MERGE,
    COL_CONTROLLED_RECURSIVE_CLEANUP_MERGE,
    COL_CONTROLLED_RECURSIVE_RETIRE_OLD_WORKING,
    COL_CONTROLLED_RECURSIVE_RETIRE_CANDIDATE,
    COL_CONTROLLED_RECURSIVE_CHECK_CONVERGENCE,
    COL_CONTROLLED_RECURSIVE_DISPOSE_FRONTIER,
    COL_CONTROLLED_RECURSIVE_BUILD_PUBLICATION,
    COL_CONTROLLED_RECURSIVE_COPY_PUBLICATION,
    COL_CONTROLLED_RECURSIVE_PUBLISH,
    COL_CONTROLLED_RECURSIVE_FINALIZE_COMMITTED,
    COL_CONTROLLED_RECURSIVE_RETIRE_FINAL_WORKING,
    COL_CONTROLLED_RECURSIVE_ABORT,
    COL_CONTROLLED_RECURSIVE_DONE
} col_controlled_recursive_phase_t;

typedef struct {
    uintptr_t identity;
    uint64_t operation_id;
    const wl_plan_t *plan;
    const wl_plan_relation_t *relation;
    col_controlled_recursive_phase_t phase;
    wl_columnar_relation_reader_step_t edge_reader;
    wl_columnar_eval_private_bindings_t body_bindings;
    wl_columnar_relation_managed_binary_build_t build;
    wl_columnar_managed_candidate_merge_t merge;
    wl_columnar_relation_managed_retirement_t retirement;
    session_rel_registration_t *registration;
    wl_evaluation_control_t *maintenance_control;
    uint64_t maintenance_id;
    wl_columnar_eval_cleanup_receipt_key_t receipt_key;
    col_rel_t *edge;
    col_rel_t *working;
    col_rel_t *frontier;
    col_rel_t *body_result;
    col_rel_t *candidate;
    col_rel_t *delta;
    col_rel_t *replacement;
    col_rel_t *old_working;
    col_rel_t *publication;
    const col_rel_t *copy_source;
    col_rel_t *copy_destination;
    col_relation_snapshot_t edge_snapshot;
    uint32_t iterations;
    uint32_t copy_row;
    uint32_t retirement_next;
    uint32_t allocation_precharge_capacity;
    uint32_t allocation_metadata_phase;
    const char *allocation_name;
    bool registration_prepared;
    bool receipt_pending;
    int primary_result;
    bool body_active;
    bool merge_result_taken;
    bool maintenance_active;
    bool edge_held;
    bool committed;
    bool postcommit_yielded;
} col_controlled_recursive_step_t;

typedef enum {
    WL_SESSION_OBSERVED_COUNT = 0,
    WL_SESSION_OBSERVED_ALLOCATE = 1,
    WL_SESSION_OBSERVED_ZERO = 2,
    WL_SESSION_OBSERVED_PLAN = 3,
    WL_SESSION_OBSERVED_NAMES = 4,
    WL_SESSION_OBSERVED_LOOKUP = 5,
    WL_SESSION_OBSERVED_LEASES = 6,
    WL_SESSION_OBSERVED_SCHEMA = 7,
    WL_SESSION_OBSERVED_EVALUATE = 8,
    WL_SESSION_OBSERVED_BASELINE = 9,
    WL_SESSION_OBSERVED_EVENTS = 10,
    WL_SESSION_OBSERVED_REGISTRY = 11,
    WL_SESSION_OBSERVED_RELEASE_READERS = 12,
    WL_SESSION_OBSERVED_CLAIMS = 13,
    WL_SESSION_OBSERVED_CUTOFF = 14,
    WL_SESSION_OBSERVED_RETIRE = 15,
    WL_SESSION_OBSERVED_FINISH = 16,
    WL_SESSION_OBSERVED_DONE = 17,
    WL_SESSION_RECURSIVE_SEED = 18,
    WL_SESSION_RECURSIVE_COMPARE = 19,
    WL_SESSION_RECURSIVE_REBIND = 20,
    WL_SESSION_RECURSIVE_RETIRE = 21,
    WL_SESSION_APPEND_AUDIT = 25,
    WL_SESSION_APPEND_COPY = 26,
    WL_SESSION_APPEND_PLAN = 27,
    WL_SESSION_RECURSIVE_RANGE = 29,
    WL_SESSION_RECURSIVE_AUDIT = 30,
    WL_SESSION_RECURSIVE_PLAN = 31,
    WL_SESSION_RECURSIVE_FRONTIER = 32,
    WL_SESSION_RECURSIVE_DIFFERENCE = 33,
    WL_SESSION_RECURSIVE_GATHER = 34,
    WL_SESSION_RECURSIVE_EMIT = 35,
} wl_columnar_session_observed_phase_t;

typedef struct {
    wl_plan_relation_t plan;
    col_rel_t *candidate, *previous, *frontier, *next_frontier;
    col_relation_snapshot_t frontier_snapshot;
    uint32_t op_base;
    uint32_t stratum, width;
    bool recursive;
    uint32_t delta_reads;
} wl_columnar_session_observed_head_t;

typedef struct {
    col_rel_t *relation;
    col_relation_snapshot_t snapshot;
    wl_columnar_relation_reader_step_t reader;
    wl_columnar_relation_scalar_retirement_t retirement;
    bool retire, preserve;
} wl_columnar_session_observed_original_t;

typedef struct {
    wl_columnar_arrangement_diff_attempt_t differential;
    wl_columnar_eval_private_bindings_t bindings;
    wl_columnar_eval_cleanup_receipt_key_t receipt_key;
    wl_columnar_eval_private_dynamic_binding_t *entries;
    wl_columnar_eval_delta_binding_t *overrides;
    wl_columnar_session_observed_head_t *heads;
    wl_columnar_session_observed_original_t *originals;
    wl_plan_op_t *ops;
    uint32_t *frontier_sources;
    col_rel_t **image;
    col_rel_t **old_registry;
    wl_columnar_memory_reservation_t metadata_credit, image_credit;
    wl_columnar_memory_reservation_t old_registry_credit, old_hash_credit;
    uint32_t *old_hash_head, *old_hash_next;
    bool old_registry_held, old_hash_held, old_physical_freed;
    bool rollback;
    uint32_t rollback_index;
    int rollback_result;
    uint32_t image_cap, image_count;
    wl_evaluation_control_t *maintenance;
    uint64_t maintenance_id, generation;
    uint32_t head_count, op_count, count, old_count;
    uint32_t si, ri, index, other, op, head, filled_heads, filled_ops;
    size_t byte;
    wl_columnar_session_observed_phase_t phase;
    unsigned allocation, init_array;
    size_t init_byte;
    bool child_active, receipt_pending, receipt_ack, committed;
    bool metadata_admitted, image_admitted, observer_started;
    bool observer_ready, prepare_started, prepare_ready;
    size_t delivery;
    uint32_t cleanup_index, dispose_index, missing_count;
    wl_columnar_relation_scalar_retirement_t disposal;
    int child_result;
    uint32_t round_begin, round_end, round, round_index, column, cleanup_round,
        audit_op;
    uint64_t row;
    col_rel_t **round_owner;
    bool round_active, round_changed;
    wl_columnar_relation_scalar_retirement_t round_retirement;
    col_rel_t *append_delta;
    col_relation_snapshot_t append_snapshot;
    bool append_active;
    bool round_frontiers, frontier_started;
    uint32_t old_row, new_row, degree_depth;
    uint8_t degrees[COL_STACK_MAX];
} wl_columnar_session_observed_transaction_t;

struct wl_columnar_controlled_step {
    wl_columnar_session_observed_transaction_t *transaction;
    bool general_observed, transaction_initialized, transaction_discard;
    size_t transaction_initialized_bytes;
    wl_columnar_eval_serial_framed_host_reply_t reply;
    wl_evaluation_control_t *previous_funding;
    uint64_t operation_id, operation_identity;
    uint64_t previous_funding_id;
    uint64_t successor_input_identity, successor_input_view_generation;
    uint64_t successor_input_storage_generation;
    uint64_t successor_head_identity, successor_head_view_generation;
    uint64_t successor_head_storage_generation;
    size_t successor_rename_name_bytes_charged;
    uint32_t successor_candidate_scan_index;
    uint32_t successor_registration_scan_charged;
    uint32_t stratum_index;
    uint32_t relation_index;
    uint32_t preflight_op;
    uint32_t preflight_registry_slot;
    uint32_t preflight_name_byte;
    uint32_t preflight_registry_count;
    col_controlled_preflight_phase_t preflight_phase;
    col_controlled_default_preflight_phase_t default_preflight_phase;
    col_rel_t **preflight_registry;
    col_rel_t **default_preflight_registry;
    const wl_plan_t *default_preflight_plan;
    uint32_t default_preflight_stratum;
    uint32_t default_preflight_relation;
    uint32_t default_preflight_op;
    uint32_t default_preflight_registry_slot;
    uint32_t default_preflight_name_byte;
    uint32_t default_preflight_registry_count;
    uint32_t default_preflight_call_actions;
    uint64_t default_preflight_charged_actions;
    col_controlled_recursive_step_t *recursive;
    col_rel_t *preflight_variable_full;
    col_rel_t *preflight_variable_delta;
    col_rel_t *preflight_join_full;
    col_rel_t *preflight_join_delta;
    uint32_t preflight_variable_full_rows;
    uint32_t preflight_variable_delta_rows;
    uint32_t preflight_join_full_rows;
    uint32_t preflight_join_delta_rows;
    bool controlled_materialized_batch_fallback;
    uint32_t observed_preflight_phase;
    uint32_t observed_preflight_slot;
    size_t observed_preflight_byte;
    uint32_t observed_preflight_edb;
    col_rel_t **observed_preflight_registry;
    uint32_t observed_preflight_registry_count;
    col_rel_t *observed_input;
    uint64_t observed_input_identity;
    col_rel_t *observed_head;
    uint64_t observed_head_identity;
    uint32_t observed_result_slot;
    size_t observed_result_byte;
    bool observed_result_ready;
    size_t observed_input_name_length;
    size_t observed_head_name_length;
    bool default_preflight_target_found;
    bool default_preflight_entry_charged;
    bool cursor_advance_pending;
    char *successor_private_name;
    char successor_name_buffer[128];
    uint32_t successor_name_suffix;
    uint32_t successor_name_slot;
    uint32_t successor_name_byte;
    unsigned successor_name_phase;
    size_t successor_metadata_byte;
    col_rel_t *successor_candidate;
    wl_columnar_source_access_reader_t successor_input_reader;
    wl_plan_relation_t successor_relation;
    wl_plan_op_t successor_ops[4];
    wl_columnar_eval_serial_framed_host_reply_kind_t previous_kind;
    int previous_protocol;
    int primary_result;
    size_t delivery_precharge_index;
    bool operation_active;
    bool relation_active;
    bool previous_valid;
    bool refused;
    bool complete;
    bool recursive_route;
    bool retryable;
    bool provenance_invalid;
    bool provenance_valid;
    col_rel_t *provenance_edge;
    col_relation_snapshot_t provenance_edge_snapshot;
    col_relation_snapshot_t provenance_path_snapshot;
    bool observed;
    bool successor_mutation_seen;
    bool append_proven;
    col_rel_t *append_source;
    uint64_t append_identity;
    uint32_t append_begin;
    bool successor_attempt;
    bool successor_candidate_detached;
    bool successor_candidate_renamed;
    bool baseline_ready;
    bool observer_preparing;
    bool observer_prepare_discard;
    bool observer_prepared;
    bool observer_evaluated;
    bool published;
    bool observer_cleanup_pending;
    wl_columnar_relation_scalar_retirement_t staging_retirement;
    col_relation_snapshot_t staging_snapshot;
    wl_evaluation_control_t *staging_maintenance;
    uint64_t staging_maintenance_id;
    uint32_t staging_slot;
    size_t staging_byte;
    bool staging_ready, staging_done, staging_tail_prepaid;
};

bool
wl_columnar_session_observed_bindings_authorized(
    const wl_col_session_t *sess,
    const wl_columnar_eval_private_bindings_t *bindings)
{
    const wl_columnar_controlled_step_t *step =
        sess ? sess->controlled_step : NULL;
    const wl_columnar_session_observed_transaction_t *tx =
        step ? step->transaction : NULL;
    return tx && step->transaction_initialized
           && step->general_observed && step->operation_active
           && tx->child_active && tx->head < tx->head_count
           && bindings == &tx->bindings &&
           bindings->identity == (uintptr_t)bindings
           && bindings->dynamic_entries == tx->entries
           && bindings->count == tx->count &&
           bindings->generation == tx->generation
           && bindings->plan == &tx->heads[tx->head].plan
           && bindings->session == sess &&
           bindings->control == sess->base.evaluation_control
           && bindings->attempt_id == step->operation_id
           && wl_evaluation_control_active_attempt_id(bindings->control)
           == step->operation_id;
}

int
wl_columnar_session_diff_index(wl_col_session_t *sess,
    const wl_columnar_eval_private_bindings_t *bindings,
    const col_rel_t *source,
    const char *name, const uint32_t *keys, uint32_t count,
    col_diff_arrangement_t **out, bool *build)
{
    *out = NULL; *build = false;
    if (!sess->diff_enabled || !bindings || sess->spill_join_batch_bytes
        || sess->join_batch_bytes)
        return 0;
    if (!wl_columnar_session_observed_bindings_authorized(sess, bindings))
        return EINVAL;
    wl_columnar_controlled_step_t *step = sess->controlled_step;
    wl_columnar_session_observed_transaction_t *tx = step->transaction;
    if (tx->heads[tx->head].recursive || !source || !count
        || source->storage_owner != source || source->col_shared
        || source->timestamps ||
        source->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
        return 0;
    wl_evaluation_control_t *control = bindings->control;
    uint64_t id = bindings->attempt_id;
    bool public_edb = false;
    for (uint32_t i = 0; i < sess->plan->edb_count; ++i) {
        int rc = wl_evaluation_control_charge_attempt(control, id, 128);
        if (rc) return rc;
        if (source == tx->entries[i].public_relation
            && source == tx->entries[i].relation) public_edb = true;
    }
    if (!public_edb) return 0;
    for (uint32_t c = 0; c < source->ncols; ++c) {
        int rc = wl_evaluation_control_charge_attempt(control, id, 128);
        if (rc) return rc;
        if (source->column_types &&
            source->column_types[c] != WIRELOG_TYPE_INT64)
            return 0;
    }
    return wl_columnar_arrangement_diff_attempt_index(&tx->differential,
               sess, tx->ops, tx->op_count, source, name, keys, count, control,
               id,
               out, build);
}

static int col_controlled_step_successor_target_init(wl_col_session_t *,
    wl_columnar_controlled_step_t *, const wl_plan_relation_t *);

static bool col_controlled_step_provenance_current(
    const wl_col_session_t *sess,
    const wl_columnar_controlled_step_t *step);
static void col_controlled_step_record_edge_append(wl_col_session_t *sess,
    col_rel_t *edge);
static bool col_session_controlled_step_authorize_insert(
    const wl_session_t *session, const char *relation, uint32_t num_rows,
    uint32_t num_cols);

bool
wl_columnar_session_controlled_batch_authorized(
    const wl_col_session_t *sess, const wl_evaluation_control_t *control,
    uint64_t id, uint64_t identity)
{
    const wl_columnar_controlled_step_t *step = sess
        ? sess->controlled_step : NULL;
    return step && (uint64_t)(uintptr_t)step == identity
           && step->operation_active && step->operation_id == id
           && step->preflight_phase == COL_CONTROLLED_PREFLIGHT_READY
           && step->controlled_materialized_batch_fallback
           && control == sess->base.evaluation_control
           && wl_evaluation_control_active_attempt_id(control) == id;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_session_test_observed_preflight_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
int wl_columnar_session_test_fail_framed_funding_before_stop;
int wl_columnar_session_test_fail_framed_funding_after_stop;
int wl_columnar_session_test_exhaust_after_relation;
unsigned wl_columnar_session_test_preflight_quantum = 64;
void (*wl_columnar_session_test_recursive_step_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t);
size_t wl_columnar_session_test_observer_precharged_events;
void (*wl_columnar_session_test_before_successor_registration)(col_rel_t *);
void (*wl_columnar_session_test_staging_prepared)(wl_evaluation_control_t *);
void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);
void (*wl_columnar_session_test_successor_registry_charge)(
    wl_evaluation_control_t *, unsigned, uint32_t);
#endif

int
wl_columnar_session_delta_source(wl_col_session_t *sess,
    const wl_columnar_eval_private_bindings_t *bindings,
    const wl_plan_op_t *op, col_rel_t **out)
{
    *out = NULL;
    if (!bindings || !bindings->dynamic_entries) return 0;
    if (!wl_columnar_session_observed_bindings_authorized(sess, bindings))
        return EINVAL;
    wl_columnar_session_observed_transaction_t *tx =
        sess->controlled_step->transaction;
    const wl_columnar_session_observed_head_t *head = &tx->heads[tx->head];
    uint32_t local = 0, encoded = 0;
    if ((tx->round_active && tx->round_frontiers) || tx->append_active) {
        uintptr_t address = (uintptr_t)op, first = (uintptr_t)head->plan.ops;
        if (address < first || address - first >=
            (uint64_t)head->plan.op_count * sizeof(*op)
            || (address - first) % sizeof(*op)) return EINVAL;
        local = (uint32_t)((address - first) / sizeof(*op));
        if (head->op_base >= tx->op_count ||
            local >= tx->op_count - head->op_base
            || !tx->frontier_sources) return EINVAL;
        encoded = tx->frontier_sources[head->op_base + local];
        if (!encoded) return 0;
    }
    if (tx->round_active && tx->round_frontiers) {
        uint32_t source_head = encoded - 1;
        if (source_head < tx->round_begin || source_head >= tx->round_end)
            return EINVAL;
        const wl_columnar_session_observed_head_t *source =
            &tx->heads[source_head];
        col_rel_t *r = tx->round ? source->frontier : source->previous;
        col_relation_snapshot_t snapshot = tx->round
            ? source->frontier_snapshot
            : tx->entries[sess->plan->edb_count + source_head].snapshot;
        if (!r || !wl_columnar_relation_snapshot_equal(snapshot,
            wl_columnar_relation_snapshot(r))) return EINVAL;
        *out = r;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_session_test_observed_transaction_checkpoint) {
            wl_columnar_session_test_observed_transaction_checkpoint(
                bindings->control, 37, tx->round, bindings);
            wl_columnar_session_test_observed_transaction_checkpoint(
                bindings->control, 39, local, bindings);
            wl_columnar_session_test_observed_transaction_checkpoint(
                bindings->control, 38, source->previous->nrows, bindings);
            wl_columnar_session_test_observed_transaction_checkpoint(
                bindings->control, 36, r->nrows, bindings);
        }
#endif
    } else if (tx->append_active) {
        if (encoded != 1) return EINVAL;
        if (!tx->append_delta || !wl_columnar_relation_snapshot_equal(
                tx->append_snapshot,
                wl_columnar_relation_snapshot(tx->append_delta))) return EINVAL;
        *out = tx->append_delta;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_session_test_observed_transaction_checkpoint) {
            wl_columnar_session_test_observed_transaction_checkpoint(
                bindings->control, 44, local, bindings);
            wl_columnar_session_test_observed_transaction_checkpoint(
                bindings->control, 28, tx->append_delta->nrows, bindings);
        }
#endif
    }
    return 0;
}

static int wl_columnar_session_controlled_step_drain_for_destroy(
    wl_col_session_t *sess);

/* Platform-specific memory detection headers (Issue #221) */
#ifdef __linux__
#include <sys/sysinfo.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(_WIN32)
#include <windows.h>
#endif
/* Peak RSS sampling for the memory report (Issue #1380) */
#if !defined(_WIN32)
#include <sys/resource.h>
#endif

/* A uint64_t dependency mask cannot represent indices above 63.  Treat an
 * unrepresentable index as affected so a wide plan is evaluated conservatively
 * instead of invoking undefined behaviour or silently skipping work. */
static bool
col_affected_mask_contains(uint64_t mask, uint32_t index)
{
    return index >= 64 || (mask & (UINT64_C(1) << index)) != 0;
}

/* Relation storage and cache functions moved to columnar/relation.c and
 * columnar/cache.c; declarations in columnar/internal.h. */

/* Arrangement functions moved to columnar/arrangement.c;
 * frontier/affected-strata functions moved to columnar/frontier.c;
 * Mobius/Z-set functions moved to columnar/mobius.c;
 * declarations in columnar/internal.h. */

col_rel_t *
session_find_rel(wl_col_session_t *sess, const char *name)
{
    if (!name)
        return NULL;

    /* Fast path: try hash lookup first (O(1)) */
    if (sess->rel_hash_nbuckets > 0) {
        col_rel_t *rel = session_rel_hash_lookup(sess, name);
        if (rel)
            return rel;
    }

    /* Lazy initialization: build hash on first use */
    if (sess->rel_hash_nbuckets == 0 && sess->nrels > 0) {
        if (session_rel_build_hash(sess) == 0) {
            /* Retry lookup after rebuild */
            return session_rel_hash_lookup(sess, name);
        }
        /* On malloc failure, fall back to linear search below */
    }

    /* Fallback: linear search (used when hash not available or error) */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0)
            return sess->rels[i];
    }
    return NULL;
}

#ifdef WL_SESSION_TEST_HOOKS
/* Serial qualification policy only. Detach the same source but retain its
 * allocation until this session is destroyed; never change admission results.
 * Policy reset cannot release or overwrite the independent retained owner. */
bool wl_columnar_session_test_retain_cold_source;
static wl_col_session_t *wl_columnar_session_test_cold_owner;
static col_rel_t *wl_columnar_session_test_cold_source;
bool
wl_columnar_session_test_has_retained_cold_source(const wl_col_session_t *s)
{
    /* NULL queries whether any detached test owner remains. */
    return s ? wl_columnar_session_test_cold_owner == s
             : wl_columnar_session_test_cold_owner != NULL;
}
#endif

/* Cold bindings never enter the raw relation registry. Only an audited,
 * immutable plan may access them through bounded JOIN/projection readers. */
static bool
wl_columnar_session_cold_plan(const wl_col_session_t *sess, const char *name,
    uint32_t ncols, const char *const *names,
    const wirelog_column_type_t *types)
{
    const wl_plan_t *plan = sess->plan;
    if (!plan || !name || !ncols || !plan->strata) return false;
    bool edb = false, consumer = false;
    for (uint32_t i = 0; i < plan->edb_count; i++)
        if (plan->edb_relations && plan->edb_relations[i] &&
            strcmp(plan->edb_relations[i], name) == 0) edb = true;
    if (!edb) return false;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        if (sp->is_recursive || !sp->relations) return false;
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];
            if (!rp->name || strcmp(rp->name, name) == 0 || !rp->ops)
                return false;
            bool projection = wl_columnar_eval_plan_cold_projection(rp,
                    name, ncols, types);
            for (uint32_t oi = 0; oi < rp->op_count; oi++) {
                const wl_plan_op_t *op = &rp->ops[oi];
                /* Reject all opaque rewrites, even if top-level names hide
                 * their dependencies. No alternate body/worker route is used. */
                if (op->opaque_data || op->materialized ||
                    (op->op != WL_PLAN_OP_VARIABLE &&
                    op->op != WL_PLAN_OP_JOIN && op->op != WL_PLAN_OP_MAP &&
                    op->op != WL_PLAN_OP_CONSOLIDATE &&
                    !(projection && op->op == WL_PLAN_OP_FILTER)) ||
                    (op->delta_mode != WL_DELTA_AUTO &&
                    op->delta_mode != WL_DELTA_FORCE_FULL)) return false;
                if (op->relation_name && strcmp(op->relation_name, name) == 0 &&
                    !projection)
                    return false;
                if (!op->right_relation || strcmp(op->right_relation,
                    name) != 0)
                    continue;
                if (op->op != WL_PLAN_OP_JOIN || op->key_count != 1 ||
                    op->right_filter_expr.size ||
                    (op->project_count && !op->project_indices)) return false;
                int key = wl_columnar_relation_schema_column_index(ncols,
                        names, op->right_keys ? op->right_keys[0] : NULL);
                /* The resident JOIN also falls back to column zero. */
                if (key < 0) key = 0;
                if (types && types[key] != WIRELOG_TYPE_INT64) return false;
                consumer = true;
            }
        }
    }
    return consumer;
}

static bool
wl_columnar_session_cold_mode(const wl_col_session_t *s)
{
    return s->spill_store && s->owns_spill_store &&
           s->spill_join_batch_bytes && s->spill_join_index_bytes &&
           !s->coordinator && s->num_workers == 1 &&
           !s->base.evaluation_control && !s->join_batch_bytes &&
           !s->delta_cb && !s->diff_operators_active && !s->tdd_input_run &&
           !s->delta_seeded && !s->retraction_seeded &&
           !s->retraction_right_pass && !s->delta_observer &&
           !s->delta_observer_build && !s->delta_observer_prepare &&
           !s->delta_publish_active;
}

int
wl_columnar_session_cold_execution_check(const wl_col_session_t *s)
{
    if (!s->cold_relation) return 0;
    const wl_columnar_session_cold_relation_t *c = s->cold_relation;
    const wirelog_column_type_t *types = wl_columnar_batch_payload_schema(
        c->payload, NULL, NULL);
    return wl_columnar_session_cold_mode(s) &&
           wl_columnar_session_cold_plan(s, c->name, c->ncols,
               (const char *const *)c->col_names, types) ? 0 : ENOTSUP;
}

static void
wl_columnar_session_cold_free(wl_columnar_session_cold_relation_t *c)
{
    if (!c) return;
    if (c->payload && wl_columnar_batch_payload_release(c->payload) !=
        WL_COLUMNAR_SPILL_OK) abort(); /* No synchronous reader may escape. */
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    if (!wl_columnar_memory_reservation_move(&token, &c->reservation)) abort();
    free(c);
    if (!wl_columnar_memory_release(&token)) abort();
}

wl_columnar_spill_status_t
wl_columnar_session_seal_join_input(wl_session_t *session, const char *name,
    wl_columnar_spill_cancel_fn cancel, void *context)
{
    if (!session || session->backend != wl_backend_columnar() || !name)
        return WL_COLUMNAR_SPILL_INVALID;
    wl_col_session_t *s = COL_SESSION(session);
#ifdef WL_SESSION_TEST_HOOKS
    bool retain = wl_columnar_session_test_retain_cold_source;
    if (retain && wl_columnar_session_test_cold_owner)
        return WL_COLUMNAR_SPILL_BUSY;
#endif
    if (s->base.input_load_failed || s->cold_relation || s->cold_seal_closed ||
        s->has_evaluated ||
        s->plain_step_completion_pending || s->uncommitted_strata_done ||
        s->cleanup_active || s->cleanup_pending || s->source_leases ||
        s->deferred_relations || s->retained_eval_entries ||
        s->tdd_workers || s->tdd_owner_lifetime || s->controlled_step ||
        s->arr_count || s->darr_count || s->diff_arr_count ||
        s->diff_txn_count ||
        s->sarr_count || s->filt_arr_count || s->filt_cache_count ||
        s->mat_cache.count || s->last_removed_relation)
        return WL_COLUMNAR_SPILL_BUSY;
    if (!wl_columnar_session_cold_mode(s)) return WL_COLUMNAR_SPILL_UNSUPPORTED;
    /* Linear lookup avoids allocating a hash merely to refuse this operation. */
    uint32_t slot = 0;
    while (slot < s->nrels && (!s->rels[slot] ||
        strcmp(s->rels[slot]->name, name) != 0)) slot++;
    if (slot == s->nrels) return WL_COLUMNAR_SPILL_INVALID;
    col_rel_t *r = s->rels[slot];
    if (r->memory_governor != s->memory_governor ||
        !wl_columnar_batch_payload_can_take(r) || r->timestamps ||
        !wl_columnar_session_cold_plan(s, name, r->ncols,
        (const char *const *)r->col_names, r->column_types))
        return WL_COLUMNAR_SPILL_UNSUPPORTED;
    if (r->storage_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return WL_COLUMNAR_SPILL_OVERFLOW;
    uint64_t bytes = sizeof(wl_columnar_session_cold_relation_t) +
        (r->col_names ? (uint64_t)r->ncols * sizeof(char *) : 0);
    if (!wl_columnar_memory_size_add(bytes, strlen(r->name) + 1, &bytes))
        return WL_COLUMNAR_SPILL_OVERFLOW;
    for (uint32_t i = 0; r->col_names && i < r->ncols; i++)
        if (r->col_names[i] && !wl_columnar_memory_size_add(bytes,
            strlen(r->col_names[i]) + 1,
            &bytes)) return WL_COLUMNAR_SPILL_OVERFLOW;
    if (bytes > SIZE_MAX) return WL_COLUMNAR_SPILL_OVERFLOW;
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_admission_status_t admission =
        wl_columnar_memory_reserve_checked(wl_columnar_memory_governor_ref_get(
                s->memory_governor), bytes, &pending);
    if (admission > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        if (admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
            s->memory_budget_denied = true;
            return WL_COLUMNAR_SPILL_MEMORY;
        }
        return WL_COLUMNAR_SPILL_OVERFLOW;
    }
    wl_columnar_session_cold_relation_t *c = calloc(1, (size_t)bytes);
    if (!c) {
        if (!wl_columnar_memory_rollback(&pending)) abort();
        return WL_COLUMNAR_SPILL_ALLOCATION;
    }
    wl_columnar_memory_reservation_init(&c->reservation);
    if (!wl_columnar_memory_reservation_move(&c->reservation, &pending) ||
        !wl_columnar_memory_commit(&c->reservation, c)) abort();
    char *cursor = (char *)(c + 1);
    if (r->col_names) {
        c->col_names = (char **)cursor;
        cursor += (size_t)r->ncols * sizeof(char *);
    }
    c->name = cursor;
    memcpy(cursor, r->name, strlen(r->name) + 1);
    cursor += strlen(r->name) + 1;
    for (uint32_t i = 0; r->col_names && i < r->ncols; i++) {
        if (!r->col_names[i]) continue;
        c->col_names[i] = cursor;
        memcpy(cursor, r->col_names[i], strlen(r->col_names[i]) + 1);
        cursor += strlen(r->col_names[i]) + 1;
    }
    c->ncols = r->ncols; c->nrows = r->nrows;
    c->source_snapshot = wl_columnar_relation_snapshot(r);
    c->cold_storage_generation = r->storage_generation + 1;
    bool inserted_name = s->last_inserted_relation &&
        strcmp(s->last_inserted_relation, r->name) == 0;
    wl_columnar_spill_status_t rc = wl_columnar_batch_payload_capture(
        s->memory_governor, s->spill_store, r, r->nrows,
        WL_COLUMNAR_BATCH_PAYLOAD_SPILLED, cancel, context, &c->payload);
    if (rc == WL_COLUMNAR_SPILL_OK && cancel && cancel(context))
        rc = WL_COLUMNAR_SPILL_CANCELLED;
    if (rc == WL_COLUMNAR_SPILL_OK) {
        int retired;
#ifdef WL_SESSION_TEST_HOOKS
        if (retain) {
            wl_columnar_relation_retirement_token_t token;
            wl_columnar_relation_retirement_init(&token);
            retired = wl_columnar_relation_retirement_prepare(r, &token);
            if (!retired && wl_columnar_relation_retirement_cancel(&token))
                abort();
        } else
#endif
        retired = col_rel_destroy_checked(r);
        if (retired) rc = retired == EBUSY ? WL_COLUMNAR_SPILL_BUSY :
                WL_COLUMNAR_SPILL_INVALID;
    }
    if (rc != WL_COLUMNAR_SPILL_OK) {
        wl_columnar_session_cold_free(c);
        if (rc == WL_COLUMNAR_SPILL_MEMORY) s->memory_budget_denied = true;
        return rc;
    }
    /* Irreversible commit: no callbacks, allocations or fallible work. Cache
     * registries were empty and source leases absent at preflight. */
#ifdef WL_SESSION_TEST_HOOKS
    if (retain) {
        wl_columnar_session_test_cold_owner = s;
        wl_columnar_session_test_cold_source = r;
    }
#endif
    s->rels[slot] = NULL;
    session_rel_free_hash(s);
    if (inserted_name) s->last_inserted_relation = c->name;
    s->cold_relation = c;
    s->base.input_sealed = true;
    return WL_COLUMNAR_SPILL_OK;
}

static uint32_t
session_compound_max_epochs_from_env(void)
{
    const char *env = getenv("WIRELOG_COMPOUND_MAX_EPOCHS");
    if (!env || env[0] == '\0')
        return 0u;
    char *endp = NULL;
    errno = 0;
    unsigned long v = strtoul(env, &endp, 10);
    if (endp == env || *endp != '\0' || errno == ERANGE || v == 0)
        return 0u;
    if (v > (unsigned long)(WL_COMPOUND_EPOCH_MAX + 1u))
        return 0u;
    return (uint32_t)v;
}

static int
session_hash_result_or_fallback(int rc)
{
    if (rc == ENOMEM || rc == ENOSPC) {
        /* A failed rebuild leaves the previous index valid for its entries;
         * misses still take session_find_rel's linear path. */
        return 0;
    }
    return rc;
}

static int
session_rels_admit(wl_col_session_t *sess, uint64_t bytes,
    wl_columnar_memory_reservation_t *reservation)
{
    if (!sess->memory_governor)
        return 0;
    wl_columnar_memory_admission_status_t status
        = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(sess->memory_governor),
            bytes, reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        sess->memory_budget_denied = true;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
        : EINVAL;
}

static void
session_rels_release(wl_columnar_memory_reservation_t *reservation)
{
    if (reservation->identity == reservation
        && atomic_load_explicit(&reservation->state,
        memory_order_acquire)
        == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && !wl_columnar_memory_rollback(reservation))
        abort();
}

static int
session_ops_count_controlled_diff_join_candidates(const wl_plan_op_t *ops,
    uint32_t count, uint64_t *candidates)
{
    if (!candidates)
        return EINVAL;
    for (uint32_t i = 0; ops && i < count; i++) {
        const wl_plan_op_t *op = &ops[i];
        if (op->op == WL_PLAN_OP_JOIN && op->right_relation
            && !op->materialized && op->key_count == 1
            && op->left_keys && op->left_keys[0]
            && op->right_keys && op->right_keys[0]) {
            if (*candidates == UINT64_MAX)
                return EOVERFLOW;
            (*candidates)++;
        }
    }
    return 0;
}

static int
session_plan_controlled_diff_join_capacity(const wl_plan_t *plan,
    uint32_t *capacity)
{
    uint64_t candidates = 0;
    int rc;
    if (!capacity)
        return EINVAL;
    *capacity = 0;
    if (!plan || !plan->strata)
        return 0;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        for (uint32_t r = 0; stratum->relations
            && r < stratum->relation_count; r++) {
            const wl_plan_relation_t *relation = &stratum->relations[r];
            rc = session_ops_count_controlled_diff_join_candidates(
                relation->ops, relation->op_count, &candidates);
            if (rc != 0)
                return rc;
        }
        for (uint32_t r = 0; stratum->bodies
            && r < stratum->relation_count; r++) {
            const wl_plan_body_t *body = &stratum->bodies[r];
            rc = session_ops_count_controlled_diff_join_candidates(
                body->ops, body->op_count, &candidates);
            if (rc != 0)
                return rc;
        }
    }
    if (candidates == 0)
        return 0;
    if (candidates > UINT32_MAX)
        return EOVERFLOW;
    *capacity = candidates < WL_DIFF_ARR_REGISTRY_INITIAL_CAP
        ? WL_DIFF_ARR_REGISTRY_INITIAL_CAP : (uint32_t)candidates;
    return 0;
}

static int
session_diff_arr_registry_init(wl_col_session_t *sess)
{
    uint32_t capacity;
    uint64_t bytes;
    if (!sess || sess->diff_arr_entries || sess->diff_arr_cap != 0)
        return EINVAL;
    int rc = session_plan_controlled_diff_join_capacity(sess->plan,
            &capacity);
    if (rc != 0 || capacity == 0)
        return rc;
    if (!wl_columnar_memory_size_mul(capacity,
        sizeof(*sess->diff_arr_entries), &bytes) || bytes > SIZE_MAX)
        return EOVERFLOW;

    wl_columnar_memory_reservation_init(
        &sess->diff_arr_registry_reservation);
    rc = session_rels_admit(sess, bytes,
            &sess->diff_arr_registry_reservation);
    if (rc != 0)
        return rc;
    col_diff_arr_entry_t *entries = (col_diff_arr_entry_t *)calloc(
        capacity, sizeof(*entries));
    if (!entries) {
        session_rels_release(&sess->diff_arr_registry_reservation);
        return ENOMEM;
    }

    sess->diff_arr_entries = entries;
    sess->diff_arr_cap = capacity;
    sess->diff_arr_registry_reserved_bytes = bytes;
    if (sess->memory_governor
        && !wl_columnar_memory_commit(
            &sess->diff_arr_registry_reservation, sess)) {
        free(sess->diff_arr_entries);
        sess->diff_arr_entries = NULL;
        sess->diff_arr_cap = 0;
        sess->diff_arr_registry_reserved_bytes = 0;
        session_rels_release(&sess->diff_arr_registry_reservation);
        return EINVAL;
    }
    wl_mem_ledger_alloc(&sess->mem_ledger, WL_MEM_SUBSYS_ARRANGEMENT,
        bytes);
    return 0;
}

static void
session_invalidate_relation_caches(wl_col_session_t *sess, const char *name)
{
    col_session_invalidate_arrangements(&sess->base, name);
    col_mat_cache_release_pins(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);
    col_mat_cache_clear(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);

    if (sess->filt_cache_active_pins > 0) {
        /* Issue #1435: leases hold entry pointers, so nothing may move.
         * A pinned entry for the relation is hidden and destroyed by its
         * last release; an unpinned one is emptied in place and rebuilt
         * by the next lookup of its filter. */
        for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
            col_filt_cache_entry_t *e = &sess->filt_cache[i];
            if (!e->rel_name || strcmp(e->rel_name, name) != 0)
                continue;
            if (e->pin_count > 0) {
                e->evict_deferred = true;
                continue;
            }
            if (e->filtered)
                col_rel_destroy(e->filtered);
            e->filtered = NULL;
            e->source_nrows = 0;
            e->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
        }
        return;
    }
    uint32_t out = 0;
    for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
        col_filt_cache_entry_t *e = &sess->filt_cache[i];
        if (strcmp(e->rel_name, name) == 0) {
            free(e->rel_name);
            free(e->filter_data);
            if (e->filtered)
                col_rel_destroy(e->filtered);
            if (e->metadata_reservation) {
                (void)wl_columnar_memory_rollback(e->metadata_reservation);
                free(e->metadata_reservation);
            }
            memset(e, 0, sizeof(*e));
            continue;
        }
        if (out != i) {
            sess->filt_cache[out] = sess->filt_cache[i];
            memset(&sess->filt_cache[i], 0, sizeof(sess->filt_cache[i]));
        }
        out++;
    }
    sess->filt_cache_count = out;
}

int
col_session_get_expression_status(wl_session_t *session)
{
    return session ? COL_SESSION(session)->extension_expr_status : 0;
}

static void
session_note_inserted_input(wl_col_session_t *sess, const col_rel_t *relation,
    bool advance_epoch)
{
    const char *relation_name = relation->name;

    session_invalidate_relation_caches(sess, relation_name);
    if (advance_epoch)
        sess->outer_epoch++;
    if (sess->last_inserted_relation
        && strcmp(sess->last_inserted_relation, relation_name) != 0)
        sess->pending_full_input_eval = true;
    sess->last_inserted_relation = relation_name;
    sess->pending_input_change = true;
    sess->snapshot_stable_valid = false;
}

/*
 * Seed shadows (Issue #1994).
 *
 * A rule head can also receive input -- inline facts such as `reach(1).`, or
 * a host insert -- and then holds its input rows and its derived rows in one
 * relation.  A full re-evaluation discards the derived rows by resetting the
 * whole relation, which would discard the input too.  Every rule head
 * therefore has a private copy of its input rows in `$in$<name>`, registered
 * on the first insert into it (see session_seed_shadow_for_insert), kept in
 * step with every insert and removal, and appended back after each reset.
 * No plan operator names a `$in$` relation, so evaluation never reads it.
 */
#define WL_SEED_SHADOW_PREFIX "$in$"

static int
session_seed_shadow_name(const char *relation, char *out, size_t cap)
{
    int len = snprintf(out, cap, WL_SEED_SHADOW_PREFIX "%s", relation);
    return len < 0 || (size_t)len >= cap ? EOVERFLOW : 0;
}

static col_rel_t *
session_seed_shadow(wl_col_session_t *sess, const char *relation)
{
    char name[256];
    if (!sess || !relation
        || session_seed_shadow_name(relation, name, sizeof(name)) != 0)
        return NULL;
    return session_find_rel(sess, name);
}

static bool
session_plan_has_rule_head(const wl_plan_t *plan, const char *relation)
{
    for (uint32_t si = 0; plan && si < plan->stratum_count; si++)
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++)
            if (plan->strata[si].relations[ri].name
                && strcmp(plan->strata[si].relations[ri].name, relation)
                == 0)
                return true;
    return false;
}

/* The strata among the first 64 that list @relation as a rule head. */
static uint64_t
session_head_strata(const wl_plan_t *plan, const char *relation)
{
    uint64_t mask = 0;

    for (uint32_t si = 0; plan && si < plan->stratum_count && si < 64; si++)
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ri++)
            if (plan->strata[si].relations[ri].name
                && strcmp(plan->strata[si].relations[ri].name, relation)
                == 0)
                mask |= (uint64_t)1 << si;
    return mask;
}

/* Register an empty session-private relation called @name. */
static int
session_private_rel_create(wl_col_session_t *sess, const char *name)
{
    col_rel_t *rel = NULL;
    int rc = wl_columnar_relation_alloc_governed(&rel, name,
            sess->memory_governor);
    if (rc != 0)
        return rc;
    rc = session_add_rel(sess, rel);
    if (rc != 0) {
        rc = rc == ENOMEM && rel->memory_budget_denial_pending ? ENOSPC : rc;
        col_rel_destroy(rel);
    }
    return rc;
}

static int
session_seed_shadow_create(wl_col_session_t *sess, const char *relation)
{
    char name[256];
    int rc = session_seed_shadow_name(relation, name, sizeof(name));
    return rc != 0 ? rc : session_private_rel_create(sess, name);
}

/* The shadow a host insert into @r must also write, created on the first
 * insert into a rule head -- inline facts are loaded through this same
 * insert path.  *seed_out is NULL for a relation that is not a rule head.
 * On the common path -- inserts into plain input relations --
 * seed_none_identity answers without a lookup. */
static int
session_seed_shadow_for_insert(wl_col_session_t *sess, const col_rel_t *r,
    col_rel_t **seed_out)
{
    uint64_t *none = &sess->seed_none_identity[r->relation_identity
            % (sizeof(sess->seed_none_identity)
            / sizeof(sess->seed_none_identity[0]))];

    *seed_out = NULL;
    if (r->relation_identity != 0 && r->relation_identity == *none)
        return 0;
    *seed_out = session_seed_shadow(sess, r->name);
    if (*seed_out)
        return 0;
    if (!session_plan_has_rule_head(sess->plan, r->name)) {
        *none = r->relation_identity;
        return 0;
    }
    int rc = session_seed_shadow_create(sess, r->name);
    if (rc == 0)
        *seed_out = session_seed_shadow(sess, r->name);
    return rc;
}

/* Drop rows [nrows, rel->nrows) of a session-private relation: a seed
 * shadow's rows appended by a batch whose relation write then failed, or the
 * retired rows a commit has consumed.  No evaluation reads the relation. */
static void
session_private_rel_truncate(col_rel_t *rel, uint32_t nrows)
{
    if (!rel || rel->nrows <= nrows)
        return;
    rel->nrows = nrows;
    if (rel->base_nrows > nrows)
        rel->base_nrows = nrows;
    rel->sorted_nrows = 0;
    rel->run_count = 0;
    memset(rel->run_ends, 0, sizeof(rel->run_ends));
    wl_columnar_eval_dedup_set_clear(rel);
    wl_columnar_relation_touch_view(rel);
}

/* Per-request flags a removal collects (Issue #2091). */
#define SESSION_REMOVE_IN_RELATION 1u /* the relation held the row */
#define SESSION_REMOVE_PENDING 2u     /* it cancels an uncommitted insert */

/* Mark, without changing the shadow, one shadow row for each requested row
 * it holds, and flag in @state each request whose row was inserted since the
 * last commit (Issue #2091).  *mask_out is NULL when there is no shadow or
 * nothing matched; otherwise the caller passes it to
 * session_seed_shadow_apply_remove once the relation's own removal has
 * committed, or frees it. */
static int
session_seed_shadow_plan_remove(col_rel_t *seed, const int64_t *data,
    uint32_t num_rows, uint32_t num_cols, uint8_t **mask_out, uint8_t *state)
{
    int64_t row_stack[COL_STACK_MAX];
    int64_t *row_buf = row_stack;
    uint8_t *mask = NULL;
    bool any = false;

    *mask_out = NULL;
    if (!seed || seed->nrows == 0 || seed->ncols != num_cols)
        return 0;
    mask = (uint8_t *)calloc(seed->nrows, sizeof(*mask));
    if (!mask)
        return ENOMEM;
    if (num_cols > COL_STACK_MAX) {
        row_buf = (int64_t *)malloc((size_t)num_cols * sizeof(*row_buf));
        if (!row_buf) {
            free(mask);
            return ENOMEM;
        }
    }
    uint32_t committed = seed->base_nrows < seed->nrows
        ? seed->base_nrows : seed->nrows;
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        for (uint32_t ri = 0; ri < seed->nrows; ri++) {
            if (mask[ri])
                continue;
            col_rel_row_copy_out(seed, ri, row_buf);
            if (memcmp(row_buf, del, (size_t)num_cols * sizeof(*del)) == 0) {
                mask[ri] = 1;
                if (ri >= committed)
                    state[di] |= SESSION_REMOVE_PENDING;
                any = true;
                break;
            }
        }
    }
    if (row_buf != row_stack)
        free(row_buf);
    if (!any) {
        free(mask);
        return 0;
    }
    *mask_out = mask;
    return 0;
}

/* Apply a planned removal.  No allocation or fallible operation. */
static void
session_seed_shadow_apply_remove(col_rel_t *seed, uint8_t *mask)
{
    uint32_t out_r = 0;
    uint32_t committed = 0;

    if (!seed || !mask)
        return;
    for (uint32_t ri = 0; ri < seed->nrows; ri++) {
        if (mask[ri])
            continue;
        if (ri < seed->base_nrows)
            committed++;
        if (out_r != ri)
            col_rel_row_move_raw(seed, out_r, ri);
        out_r++;
    }
    session_private_rel_truncate(seed, out_r);
    /* Issue #2091: rows below base_nrows are the committed input. */
    seed->base_nrows = committed;
    free(mask);
}

/*
 * Committed model of a rule head (Issue #2091).
 *
 * A step with a delta callback publishes the difference between the model
 * it derives and the model the previous step or snapshot committed.  A rule
 * head written by the host since that commit no longer holds that model:
 *
 *   - its seed shadow's rows [base_nrows, nrows) were inserted since -- the
 *     step and snapshot commits set base_nrows to nrows on every relation,
 *     an insert appends, and a removal keeps that boundary -- and until the
 *     next evaluation the head holds a copy of each;
 *   - a removal that took a row out of the head without cancelling such an
 *     insert copies the row to `$out$<name>`, which the next commit empties.
 *
 * The delta observer therefore takes the committed model to be the head plus
 * its `$out$` rows less its uncommitted inserts, counting copies.  A head the
 * host has not written since the commit is unchanged by that rule.  The
 * rule holds between a commit and the next evaluation; an evaluation that
 * stops short of committing -- a failed snapshot -- can consolidate the head
 * first, and the baseline is then only approximate.  Like a seed shadow, no
 * plan operator names `$out$<name>`.
 */
#define WL_RETIRED_PREFIX "$out$"

col_rel_t *
wl_columnar_session_retired_rows(wl_col_session_t *sess, const char *relation)
{
    char name[256];
    int len = snprintf(name, sizeof(name), WL_RETIRED_PREFIX "%s", relation);
    if (len < 0 || (size_t)len >= sizeof(name))
        return NULL;
    return session_find_rel(sess, name);
}

col_rel_t *
wl_columnar_session_seed_rows(wl_col_session_t *sess, const char *relation)
{
    return session_seed_shadow(sess, relation);
}

/* Copy to rule head @r's retired rows each requested row @state marks as held
 * by @r and not as cancelling an uncommitted insert.  Call before the removal
 * changes @r, and before any relation registration is prepared, since this
 * may register the retired relation.  A failure leaves the retired rows
 * unchanged.  On success *retired_out and *prior_out let a caller whose
 * removal then fails take the copy back with session_private_rel_truncate();
 * *retired_out is NULL when nothing was copied. */
static int
session_retire_committed_rows(wl_col_session_t *sess, const col_rel_t *r,
    const int64_t *data, uint32_t num_rows, const uint8_t *state,
    col_rel_t **retired_out, uint32_t *prior_out)
{
    uint32_t count = 0;
    uint64_t cells = 0;
    uint64_t bytes = 0;
    bool denied = false;
    int64_t *rows;
    col_rel_t *retired;
    int rc;

    *retired_out = NULL;
    *prior_out = 0;
    for (uint32_t di = 0; di < num_rows; di++)
        if (state[di] == SESSION_REMOVE_IN_RELATION)
            count++;
    if (count == 0 || !session_plan_has_rule_head(sess->plan, r->name))
        return 0;
    retired = wl_columnar_session_retired_rows(sess, r->name);
    if (!retired) {
        char name[256];
        int len = snprintf(name, sizeof(name), WL_RETIRED_PREFIX "%s",
                r->name);
        if (len < 0 || (size_t)len >= sizeof(name))
            return EOVERFLOW;
        rc = session_private_rel_create(sess, name);
        if (rc != 0)
            return rc;
        retired = session_find_rel(sess, name);
    }
    if (!wl_columnar_memory_size_mul(count, r->ncols, &cells)
        || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
        || bytes > SIZE_MAX)
        return EOVERFLOW;
    rows = (int64_t *)malloc((size_t)bytes);
    if (!rows)
        return ENOMEM;
    *prior_out = retired->nrows;
    count = 0;
    for (uint32_t di = 0; di < num_rows; di++)
        if (state[di] == SESSION_REMOVE_IN_RELATION)
            memcpy(rows + (size_t)count++ * r->ncols,
                data + (size_t)di * r->ncols, (size_t)r->ncols
                * sizeof(*rows));
    rc = col_rel_append_rows_atomic(retired, rows, count, r->ncols, &denied);
    free(rows);
    if (rc == 0)
        *retired_out = retired;
    return rc == ENOMEM && denied ? ENOSPC : rc;
}

/* A commit makes the current rows the committed model again. */
static void
session_retired_rows_clear(wl_col_session_t *sess)
{
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r && strncmp(r->name, WL_RETIRED_PREFIX,
            sizeof(WL_RETIRED_PREFIX) - 1) == 0)
            session_private_rel_truncate(r, 0);
    }
}

int
wl_columnar_session_restore_seed(wl_col_session_t *sess, col_rel_t *r)
{
    col_rel_t *seed = r ? session_seed_shadow(sess, r->name) : NULL;
    int64_t *rows = NULL;
    uint64_t cells = 0;
    uint64_t bytes = 0;
    bool denied = false;
    int rc;

    if (!seed || seed->nrows == 0)
        return 0;
    if (r->ncols != 0 && r->ncols != seed->ncols)
        return EINVAL;
    if (!wl_columnar_memory_size_mul(seed->nrows, seed->ncols, &cells)
        || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
        || bytes > SIZE_MAX)
        return EOVERFLOW;
    rows = (int64_t *)malloc((size_t)bytes);
    if (!rows)
        return ENOMEM;
    for (uint32_t i = 0; i < seed->nrows; i++)
        col_rel_row_copy_out(seed, i, rows + (size_t)i * seed->ncols);
    rc = col_rel_append_rows_atomic(r, rows, seed->nrows, seed->ncols,
            &denied);
    free(rows);
    return rc == ENOMEM && denied ? ENOSPC : rc;
}

/* Shared views borrow a root relation's columns. Teardown must release the
 * aliases before their root; otherwise the root's owner metadata would point
 * at freed storage. A root owned by another session is left for that owner
 * to retry after its aliases have drained. */
static int
session_destroy_relation_array_pass(col_rel_t **relations, uint32_t count,
    bool aliases)
{
    int result = 0;

    for (uint32_t i = 0; i < count; i++) {
        col_rel_t *relation = relations[i];
        bool is_alias;
        if (!relation)
            continue;
        is_alias = relation->storage_owner
            && relation->storage_owner != relation;
        if (aliases != is_alias)
            continue;
        int rc = col_rel_destroy_checked(relation);
        if (rc == 0)
            relations[i] = NULL;
        else if (result == 0)
            result = rc;
    }
    return result;
}

static bool
wl_columnar_session_retained_eval_has_relation(
    const wl_col_session_t *sess,
    const col_rel_t *relation)
{
    for (const wl_columnar_retained_eval_entry_t *retained
            = sess ? sess->retained_eval_entries : NULL;
        retained; retained = retained->next) {
        if (retained->entry.kind == WL_COLUMNAR_EVAL_ENTRY_RELATION
            && retained->entry.rel == relation)
            return true;
    }
    return false;
}

/* During worker teardown, retained entries are the authoritative owners of
 * descriptors still waiting for an external reader.  Retire other aliases
 * while leaving every retained descriptor reachable for its next disposal
 * attempt. */
static int
wl_columnar_session_destroy_worker_relation_aliases(
    wl_col_session_t *worker,
    uint32_t *destroyed_out)
{
    int first_rc = 0;
    uint32_t destroyed = 0;

    for (uint32_t i = 0; i < worker->nrels; i++) {
        col_rel_t *relation = worker->rels ? worker->rels[i] : NULL;
        if (!relation || wl_columnar_session_retained_eval_has_relation(
                worker, relation))
            continue;
        if (!relation->storage_owner || relation->storage_owner == relation)
            continue;
        int rc = col_rel_destroy_checked(relation);
        if (rc == 0) {
            worker->rels[i] = NULL;
            destroyed++;
        } else if (first_rc == 0) {
            first_rc = rc;
        }
    }
    if (destroyed_out)
        *destroyed_out = destroyed;
    return first_rc;
}

/* Validate all arithmetic before deriving a descriptor address.  The
 * checked pool helper validates each range too; this outer check also
 * protects our retained-slot exclusions from malformed pool geometry. */
static int
wl_columnar_session_validate_worker_pool_geometry(const delta_pool_t *pool,
    uint32_t slot_limit)
{
    if (!pool)
        return slot_limit == 0 ? 0 : EINVAL;
    if (slot_limit > pool->slot_used || pool->slot_used > pool->slot_cap
        || slot_limit > pool->slot_cap)
        return EINVAL;
    if (slot_limit == 0)
        return 0;
    if (!pool->slab || pool->slot_size < sizeof(col_rel_t)
        || pool->slot_size % _Alignof(col_rel_t) != 0
        || (uintptr_t)pool->slab % _Alignof(col_rel_t) != 0)
        return EINVAL;
    if ((size_t)slot_limit > SIZE_MAX / pool->slot_size)
        return EOVERFLOW;
    size_t span = (size_t)slot_limit * pool->slot_size;
    if (span > UINTPTR_MAX - (uintptr_t)pool->slab)
        return EOVERFLOW;
    return 0;
}

/* Reuse the relation layer's checked range destroyer for each contiguous
 * non-retained alias slot.  A retained alias slot is never passed to the
 * pool sweep, so the inventory cannot tombstone the entry's descriptor. */
static int
wl_columnar_session_destroy_worker_pool_aliases(wl_col_session_t *worker,
    uint32_t slot_limit, uint32_t *destroyed_out)
{
    delta_pool_t *pool = worker->delta_pool;
    int rc = wl_columnar_session_validate_worker_pool_geometry(pool,
            slot_limit);
    int first_rc = rc;
    uint32_t destroyed = 0;

    if (rc != 0) {
        if (destroyed_out)
            *destroyed_out = 0;
        return rc;
    }
    if (!pool) {
        if (destroyed_out)
            *destroyed_out = 0;
        return 0;
    }

    for (uint32_t slot = 0; slot < slot_limit; slot++) {
        col_rel_t *relation = (col_rel_t *)(pool->slab
            + (size_t)slot * pool->slot_size);
        if (wl_columnar_session_retained_eval_has_relation(worker, relation)
            || (relation->relation_identity == 0
            && !relation->storage_owner)
            || !relation->storage_owner
            || relation->storage_owner == relation)
            continue;

        int destroy_rc = col_rel_pool_destroy_aliases_checked(pool, slot,
                slot + 1);
        if (destroy_rc == 0 && relation->relation_identity == 0)
            destroyed++;
        else if (destroy_rc != 0 && first_rc == 0)
            first_rc = destroy_rc;
    }
    if (destroyed_out)
        *destroyed_out = destroyed;
    return first_rc;
}

static wl_columnar_session_source_lease_t **
wl_columnar_session_source_lease_slot(wl_col_session_t *sess,
    const col_rel_t *borrower)
{
    wl_columnar_session_source_lease_t **slot = &sess->source_leases;

    while (*slot && (*slot)->borrower != borrower)
        slot = &(*slot)->next;
    return slot;
}

static int
wl_columnar_session_source_lease_prepare(const col_rel_t *borrower,
    const col_rel_t *source, wl_columnar_session_source_lease_t **out)
{
    col_rel_t *owner = NULL;
    wl_columnar_session_source_lease_t *lease;
    int rc;

    if (!borrower || !source || !out)
        return EINVAL;
    *out = NULL;
    lease = (wl_columnar_session_source_lease_t *)calloc(1,
            sizeof(*lease));
    if (!lease)
        return ENOMEM;
    lease->borrower = (col_rel_t *)borrower;
    rc = col_rel_source_reader_acquire_transferable(source, &lease->reader);
    if (rc != 0) {
        free(lease);
        return rc;
    }
    rc = col_rel_storage_owner_resolve(source, &owner);
    if (rc != 0) {
        (void)col_rel_source_reader_release(&lease->reader);
        free(lease);
        return rc;
    }
    if (owner->pool_owned || owner->arena_owned) {
        (void)col_rel_source_reader_release(&lease->reader);
        free(lease);
        return EINVAL;
    }
    lease->owner = owner;
    lease->owner_identity = owner->relation_identity;
    *out = lease;
    return 0;
}

static int
wl_columnar_session_source_lease_release(
    wl_columnar_session_source_lease_t *lease)
{
    int rc;

    if (!lease)
        return 0;
    rc = col_rel_source_reader_release(&lease->reader);
    if (rc == 0)
        free(lease);
    return rc;
}

static wl_columnar_session_source_lease_t *
wl_columnar_session_source_lease_take(wl_col_session_t *sess,
    const col_rel_t *borrower)
{
    wl_columnar_session_source_lease_t **slot;
    wl_columnar_session_source_lease_t *lease;

    if (!sess || !borrower)
        return NULL;
    slot = wl_columnar_session_source_lease_slot(sess, borrower);
    lease = *slot;
    if (!lease)
        return NULL;
    *slot = lease->next;
    lease->next = NULL;
    return lease;
}

static void
wl_columnar_session_source_lease_restore(wl_col_session_t *sess,
    wl_columnar_session_source_lease_t *lease)
{
    if (!sess || !lease)
        return;
    lease->next = sess->source_leases;
    sess->source_leases = lease;
}

int
wl_columnar_session_adopt_shared_view(wl_col_session_t *sess,
    col_rel_t *borrower)
{
    wl_columnar_session_source_lease_t **slot;
    wl_columnar_session_source_lease_t *lease;
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *owner = NULL;
    int rc;

    if (!sess || !borrower)
        return EINVAL;
    rc = col_rel_source_reader_acquire_transferable(borrower, &reader);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(borrower, &owner);
    if (rc != 0) {
        (void)col_rel_source_reader_release(&reader);
        return rc;
    }
    if (owner == borrower) {
        (void)col_rel_source_reader_release(&reader);
        return 0;
    }
    slot = wl_columnar_session_source_lease_slot(sess, borrower);
    if (*slot) {
        rc = ((*slot)->owner == owner
            && (*slot)->owner_identity == owner->relation_identity)
            ? 0 : EINVAL;
        (void)col_rel_source_reader_release(&reader);
        return rc;
    }
    rc = wl_columnar_session_source_lease_prepare(borrower, owner, &lease);
    if (col_rel_source_reader_release(&reader) != 0 && rc == 0)
        rc = EINVAL;
    if (rc != 0)
        return rc;
    lease->next = sess->source_leases;
    sess->source_leases = lease;
    return 0;
}

int
wl_columnar_session_retire_source_lease(wl_col_session_t *sess,
    col_rel_t *borrower)
{
    wl_columnar_session_source_lease_t *lease;
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *owner = NULL;
    int rc;

    if (!sess || !borrower)
        return EINVAL;
    rc = col_rel_source_reader_acquire_transferable(borrower, &reader);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(borrower, &owner);
    if (rc != 0) {
        (void)col_rel_source_reader_release(&reader);
        return rc;
    }
    /* A live alias still needs its lease.  Callers may retire only after a
     * successful COW/deep-copy transition made the relation self-owned. */
    if (owner != borrower) {
        (void)col_rel_source_reader_release(&reader);
        return EBUSY;
    }
    if (col_rel_source_reader_release(&reader) != 0)
        return EINVAL;
    lease = wl_columnar_session_source_lease_take(sess, borrower);
    rc = wl_columnar_session_source_lease_release(lease);
    if (rc != 0)
        wl_columnar_session_source_lease_restore(sess, lease);
    return rc;
}

static int
wl_columnar_session_source_leases_release_all(wl_col_session_t *sess)
{
    wl_columnar_session_source_lease_t *lease;
    int result = 0;

    if (!sess)
        return EINVAL;
    while ((lease = sess->source_leases) != NULL) {
        sess->source_leases = lease->next;
        /* Stable registry tokens are explicitly transferable so an
         * externally serialized worker teardown need not run on the
         * publication thread. */
        int rc = wl_columnar_session_source_lease_release(lease);
        assert(rc == 0);
        if (rc != 0) {
            lease->next = sess->source_leases;
            sess->source_leases = lease;
            result = rc;
            break;
        }
    }
    return result;
}

bool
wl_columnar_deferred_relation_eligible(const col_rel_t *rel)
{
    col_rel_t *owner = NULL;

    if (!rel || rel->pool_owned || rel->arena_owned
        || col_rel_storage_owner_resolve(rel, &owner) != 0)
        return false;
    /* A shared-view alias is borrowed storage, not an independently owned
     * relation.  Its owner may outlive the evaluator, but destroying the
     * alias itself is not a valid deferred cleanup operation. */
    return owner == rel && !owner->pool_owned && !owner->arena_owned;
}

int
wl_columnar_session_defer_relation(wl_col_session_t *sess, col_rel_t *rel)
{
    col_rel_t **tail;

    if (!sess || !rel)
        return EINVAL;
    /* Pool/arena storage can disappear with the evaluator's worker/session
     * allocator.  Refuse it here as an invariant violation rather than ever
     * making a deferred pointer outlive that allocator. */
    if (!wl_columnar_deferred_relation_eligible(rel))
        return EINVAL;
    if (rel->deferred_relation_session) {
        /* The relation is already owned by this registry, so repeated
        * cleanup attempts are idempotent.  A relation cannot safely be
        * adopted by a second session without first being unlinked. */
        return rel->deferred_relation_session == sess ? 0 : EBUSY;
    }
    rel->deferred_relation_next = NULL;
    tail = &sess->deferred_relations;
    while (*tail)
        tail = &(*tail)->deferred_relation_next;
    *tail = rel;
    rel->deferred_relation_session = sess;
    sess->deferred_relation_count++;
    return 0;
}

int
wl_columnar_session_retry_deferred(wl_col_session_t *sess)
{
    col_rel_t **slot;
    int first_rc = 0;

    if (!sess)
        return EINVAL;
    slot = &sess->deferred_relations;
    while (*slot) {
        col_rel_t *rel = *slot;
        col_rel_t *next = rel->deferred_relation_next;
        int rc;

        if (rel->deferred_relation_session != sess) {
            if (first_rc == 0)
                first_rc = EFAULT;
            slot = &rel->deferred_relation_next;
            continue;
        }
        if (!wl_columnar_deferred_relation_eligible(rel)) {
            if (first_rc == 0)
                first_rc = EINVAL;
            slot = &rel->deferred_relation_next;
            continue;
        }
        rc = col_rel_destroy_checked(rel);
        if (rc == 0) {
            *slot = next;
            assert(sess->deferred_relation_count > 0);
            sess->deferred_relation_count--;
            continue;
        }
        if (first_rc == 0)
            first_rc = rc;
        slot = &rel->deferred_relation_next;
    }
    return first_rc;
}

int
wl_columnar_session_cleanup_ready(wl_col_session_t *sess)
{
    if (!sess)
        return EINVAL;
    if (sess->delta_publish_active || sess->tdd_input_run)
        return EBUSY;
    if (sess->tdd_owner_lifetime) {
        if (sess->tdd_owner_lifetime->evaluation_active
            || sess->tdd_owner_lifetime->dispatch_active)
            return EBUSY;
        int lifetime_rc = wl_columnar_eval_tdd_owner_lifetime_retry(sess);
        if (lifetime_rc != 0)
            return lifetime_rc;
    }
    int kfusion_rc = wl_columnar_kfusion_retry_pending(sess);
    if (kfusion_rc != 0)
        return kfusion_rc;
    /* Active-only nesting is valid. Retry must never consume an active frame;
     * active plus pending cleanup is rejected by the cleanup primitive. */
    int rc = sess->cleanup_pending
        ? wl_columnar_eval_stack_cleanup_retry(sess) : 0;
    return rc != 0 ? rc : wl_columnar_eval_delta_rollback_retry(sess);
}

int
wl_columnar_session_cleanup_ready_quiescent(wl_col_session_t *sess)
{
    if (!sess)
        return EINVAL;
    if (sess->tdd_owner_lifetime) {
        int quiesce_rc
            = wl_columnar_eval_tdd_owner_lifetime_quiesce(sess);
        if (quiesce_rc != 0)
            return quiesce_rc;
    }
    if (sess->coordinator)
        return wl_columnar_session_cleanup_ready(sess);
    /* Worker frame lists are thread-confined until this barrier. Never inspect
     * them before submitted callbacks have finished, including drain failure. */
    if (sess->wq) {
        int rc = wl_workqueue_drain(sess->wq);
        if (rc != 0)
            return rc;
    }
    int result = 0;
    for (uint32_t w = 0; w < sess->tdd_workers_count; w++) {
        int rc = wl_columnar_session_cleanup_ready(&sess->tdd_workers[w]);
        if (result == 0)
            result = rc;
    }
    int rc = wl_columnar_session_cleanup_ready(sess);
    return result != 0 ? result : rc;
}

int
wl_columnar_session_transfer_deferred(wl_col_session_t *from,
    wl_col_session_t *to)
{
    if (!from || !to || from == to)
        return EINVAL;

    while (from->deferred_relations) {
        col_rel_t *rel = from->deferred_relations;
        col_rel_t *next = rel->deferred_relation_next;
        int rc;

        if (rel->deferred_relation_session != from)
            return EFAULT;
        from->deferred_relations = next;
        rel->deferred_relation_next = NULL;
        rel->deferred_relation_session = NULL;
        assert(from->deferred_relation_count > 0);
        from->deferred_relation_count--;

        rc = wl_columnar_session_defer_relation(to, rel);
        if (rc != 0) {
            /* The destination may already own this relation after a
             * previous cleanup path.  Treat that as an idempotent transfer;
             * retaining a second source link would violate single ownership.
             */
            if (rc == EBUSY && rel->deferred_relation_session == to)
                continue;
            rel->deferred_relation_next = from->deferred_relations;
            rel->deferred_relation_session = from;
            from->deferred_relations = rel;
            from->deferred_relation_count++;
            return rc;
        }
    }
    return 0;
}

/* Keep a refused evaluator entry alive in the parent session.  This registry
 * deliberately accepts pool/arena relations: those allocators belong to the
 * parent session and therefore outlive the block-local evaluator stack. */
int
wl_columnar_session_retain_eval_entry(wl_col_session_t *sess,
    eval_entry_t *entry)
{
    wl_columnar_retained_eval_entry_t *retained;

    if (!sess || !entry)
        return EINVAL;
    retained = (wl_columnar_retained_eval_entry_t *)calloc(1,
            sizeof(*retained));
    if (!retained)
        return ENOMEM;
    retained->entry = *entry;
    memset(entry, 0, sizeof(*entry));
    retained->next = sess->retained_eval_entries;
    sess->retained_eval_entries = retained;
    sess->retained_eval_entry_count++;
    return 0;
}

int
wl_columnar_session_retain_eval_stack(wl_col_session_t *sess,
    eval_stack_t *stack)
{
    int first_rc = 0;

    if (!sess || !stack)
        return EINVAL;
    while (stack->top > 0) {
        int rc = wl_columnar_session_retain_eval_entry(sess,
                &stack->items[stack->top - 1]);
        if (rc != 0) {
            first_rc = rc;
            break;
        }
        stack->top--;
    }
    return first_rc;
}

int
wl_columnar_session_retry_retained_eval_entries(wl_col_session_t *sess)
{
    wl_columnar_retained_eval_entry_t **slot;
    int first_rc = 0;

    if (!sess)
        return EINVAL;
    slot = &sess->retained_eval_entries;
    while (*slot) {
        wl_columnar_retained_eval_entry_t *retained = *slot;
        int rc = eval_entry_dispose(&retained->entry);
        if (rc != 0) {
            if (first_rc == 0)
                first_rc = rc;
            slot = &retained->next;
            continue;
        }
        *slot = retained->next;
        assert(sess->retained_eval_entry_count > 0);
        sess->retained_eval_entry_count--;
        free(retained);
    }
    return first_rc;
}

/* Row removal rewrites the canonical relation storage in place.  Admit the
 * operation before the first source read so a competing reader, writer, or
 * live storage alias cannot observe a partially compacted relation. */
static int
session_relation_writer_acquire(col_rel_t *relation, col_rel_t **owner_out,
    wl_columnar_source_access_writer_t *writer)
{
    col_rel_t *owner = NULL;
    int rc;

    if (!relation || !owner_out || !writer)
        return EINVAL;
    *owner_out = NULL;
    rc = col_rel_source_writer_acquire(relation, writer);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(relation, &owner);
    if (rc != 0) {
        (void)wl_columnar_source_access_writer_release(writer);
        return rc;
    }
    if (col_rel_storage_alias_borrow_count(owner) > 0) {
        (void)wl_columnar_source_access_writer_release(writer);
        return EBUSY;
    }
    *owner_out = owner;
    return 0;
}

int
wl_columnar_session_install_shared_view(wl_col_session_t *sess,
    col_rel_t *dst, const col_rel_t *src)
{
    wl_columnar_session_source_lease_t **slot;
    wl_columnar_session_source_lease_t *old_lease;
    wl_columnar_session_source_lease_t *new_lease = NULL;
    col_rel_t *owner = NULL;
    wl_columnar_source_access_gate_t *source_descriptor = NULL;
    bool source_descriptor_held = false;
    int rc;
    int descriptor_release_rc = 0;

    if (!sess || !dst || !src)
        return EINVAL;
    rc = col_rel_storage_owner_ensure_initialized((col_rel_t *)src);
    if (rc != 0)
        return rc;
    rc = col_rel_storage_owner_resolve(src, &owner);
    if (rc != 0)
        return rc;
    slot = wl_columnar_session_source_lease_slot(sess, dst);
    old_lease = *slot;
    /* The publication helper acquires the owner reader it needs.  This
     * wrapper only holds the source descriptor gate, avoiding a second owner
     * reader that would make a same-owner lease refresh look busy. */
    source_descriptor =
        (wl_columnar_source_access_gate_t *)&src->descriptor_access;
    rc = wl_columnar_source_access_gate_reader_acquire(source_descriptor);
    if (rc != 0)
        return rc;
    source_descriptor_held = true;
    if (!old_lease || old_lease->owner != owner
        || old_lease->owner_identity != owner->relation_identity) {
        rc = wl_columnar_session_source_lease_prepare(dst, owner,
                &new_lease);
        if (rc != 0) {
            if (source_descriptor_held)
                (void)wl_columnar_source_access_gate_reader_release(
                    source_descriptor);
            return rc;
        }
    }

    /* A refresh replaces alias descriptors under the old owner's writer.
     * Lend only this session's lifetime lease to the atomic upgrade; a new
     * lease can serve this role when adopting an alias of the same owner. */
    wl_columnar_source_access_reader_t *destination_lease = old_lease
        ? &old_lease->reader
        : (new_lease && dst->storage_owner == owner
            ? &new_lease->reader : NULL);
    rc = wl_columnar_relation_install_shared_view_with_lease(dst, src,
            destination_lease);
    if (rc != 0) {
        if (new_lease) {
            int release_rc
                = wl_columnar_session_source_lease_release(new_lease);
            assert(release_rc == 0);
        }
        if (source_descriptor_held)
            (void)wl_columnar_source_access_gate_reader_release(
                source_descriptor);
        return rc;
    }
    if (!new_lease) {
        rc = 0;
        if (source_descriptor_held
            && wl_columnar_source_access_gate_reader_release(
                source_descriptor) != 0 && rc == 0)
            rc = EINVAL;
        return rc == 0 ? 0 : EINVAL;
    }

    if (source_descriptor_held
        && wl_columnar_source_access_gate_reader_release(source_descriptor)
        != 0)
        descriptor_release_rc = EINVAL;

    if (old_lease) {
        new_lease->next = old_lease->next;
        *slot = new_lease;
        old_lease->next = NULL;
        int old_release_rc
            = wl_columnar_session_source_lease_release(old_lease);
        assert(old_release_rc == 0);
        if (old_release_rc != 0)
            return old_release_rc;
    } else {
        new_lease->next = sess->source_leases;
        sess->source_leases = new_lease;
    }
    return descriptor_release_rc;
}

static void
session_pool_rel_transfer_payload(col_rel_t *dst, col_rel_t *src)
{
    const size_t descriptor_offset
        = offsetof(col_rel_t, descriptor_reservation);
    const size_t after_descriptor_offset
        = offsetof(col_rel_t, memory_budget_denial_pending);
    const size_t retained_offset
        = offsetof(col_rel_t, retained_reservation);
    const size_t after_retained_offset
        = offsetof(col_rel_t, retained_reserved_bytes);
    const size_t alias_borrows_offset
        = offsetof(col_rel_t, storage_alias_borrows);

    memcpy(dst, src, descriptor_offset);
    memcpy((char *)dst + after_descriptor_offset,
        (const char *)src + after_descriptor_offset,
        retained_offset - after_descriptor_offset);
    memcpy((char *)dst + after_retained_offset,
        (const char *)src + after_retained_offset,
        alias_borrows_offset - after_retained_offset);
    atomic_store_explicit(&dst->storage_alias_borrows, 0,
        memory_order_relaxed);
}

/* Both descriptors are private or exclusively gated. Move the token without
 * crediting live storage, and rebind its logical owner to the new descriptor. */
static bool
wl_columnar_session_pool_rel_move_reservation(col_rel_t *dst, col_rel_t *src)
{
    if (src->retained_reserved_bytes == 0)
        return true;
    if (!wl_columnar_memory_transfer(&src->retained_reservation, dst))
        return false;
    if (!wl_columnar_memory_reservation_move(&dst->retained_reservation,
        &src->retained_reservation)) {
        if (!wl_columnar_memory_transfer(&src->retained_reservation, src))
            abort();
        return false;
    }
    return true;
}

static bool
session_pool_rel_move_metadata(col_rel_t *dst, col_rel_t *src)
{
    if (src->metadata_reserved_bytes == 0)
        return true;
    if (!src->memory_governor || dst->memory_governor != src->memory_governor
        || dst->metadata_reserved_bytes != 0
        || dst->metadata_reservation.identity != &dst->metadata_reservation
        || src->metadata_reservation.identity != &src->metadata_reservation
        || src->metadata_reservation.bytes != src->metadata_reserved_bytes
        || src->metadata_reservation.governor
        != wl_columnar_memory_governor_ref_get(src->memory_governor)
        || atomic_load_explicit(&src->metadata_reservation.state,
        memory_order_acquire) != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || atomic_load_explicit(&src->metadata_reservation.owner_bits,
        memory_order_acquire) != (uintptr_t)src)
        return false;
    if (!wl_columnar_memory_transfer(&src->metadata_reservation, dst))
        return false;
    if (!wl_columnar_memory_reservation_move(&dst->metadata_reservation,
        &src->metadata_reservation)) {
        if (!wl_columnar_memory_transfer(&src->metadata_reservation, src))
            abort();
        return false;
    }
    dst->metadata_reserved_bytes = src->metadata_reserved_bytes;
    src->metadata_reserved_bytes = 0;
    return true;
}

static bool
session_pool_rel_move_dedup(col_rel_t *dst, col_rel_t *src)
{
    if (!src->dedup_reserved_bytes)
        return true;
    if (dst->dedup_reserved_bytes
        || !wl_columnar_memory_transfer(&src->dedup_reservation, dst))
        return false;
    if (!wl_columnar_memory_reservation_move(&dst->dedup_reservation,
        &src->dedup_reservation)) {
        if (!wl_columnar_memory_transfer(&src->dedup_reservation, src))
            abort();
        return false;
    }
    dst->dedup_reserved_bytes = src->dedup_reserved_bytes;
    src->dedup_reserved_bytes = 0;
    return true;
}

static int
session_pool_rel_admit_dedup(col_rel_t *heap, uint64_t bytes)
{
    wl_columnar_memory_reservation_t pending;
    wl_columnar_memory_admission_status_t status;
    if (!bytes || !heap->memory_governor)
        return 0;
    wl_columnar_memory_reservation_init(&pending);
    status = wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(heap->memory_governor),
        bytes, &pending);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
            ? EOVERFLOW : EBUSY;
    if (!wl_columnar_memory_commit(&pending, heap)
        || !wl_columnar_memory_reservation_move(
            &heap->dedup_reservation, &pending))
        abort();
    heap->dedup_reserved_bytes = bytes;
    return 0;
}

static bool
session_pool_rel_move_name(col_rel_t *dst, col_rel_t *src,
    wl_columnar_memory_governor_ref_t *governor)
{
    if (src->pool_name_reserved_bytes == 0)
        return true;
    if (!governor || !src->name
        || strlen(src->name) + 1u != src->pool_name_reserved_bytes
        || dst->pool_name_reserved_bytes != 0
        || dst->pool_name_reservation.identity != &dst->pool_name_reservation
        || src->pool_name_reservation.identity != &src->pool_name_reservation
        || src->pool_name_reservation.bytes != src->pool_name_reserved_bytes
        || src->pool_name_reservation.governor
        != wl_columnar_memory_governor_ref_get(governor)
        || atomic_load_explicit(&src->pool_name_reservation.state,
        memory_order_acquire) != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || atomic_load_explicit(&src->pool_name_reservation.owner_bits,
        memory_order_acquire) != (uintptr_t)src)
        return false;
    if (!wl_columnar_memory_transfer(&src->pool_name_reservation, dst))
        return false;
    if (!wl_columnar_memory_reservation_move(&dst->pool_name_reservation,
        &src->pool_name_reservation)) {
        if (!wl_columnar_memory_transfer(&src->pool_name_reservation, src))
            abort();
        return false;
    }
    dst->pool_name_reserved_bytes = src->pool_name_reserved_bytes;
    src->pool_name_reserved_bytes = 0;
    return true;
}

static void
session_pool_rel_finish_promotion(col_rel_t *heap)
{
    /* The published heap descriptor now covers the transferred name. */
    if (heap->pool_name_reserved_bytes
        && !wl_columnar_memory_release(&heap->pool_name_reservation))
        abort();
    heap->pool_name_reserved_bytes = 0;
}

static int
session_pool_rel_promote(col_rel_t *src,
    wl_columnar_memory_governor_ref_t *session_governor,
    col_rel_t **heap_out)
{
    wl_columnar_source_access_writer_t descriptor_writer = { 0 };
    wl_columnar_source_access_writer_t owner_writer = { 0 };
    col_rel_t *heap;
    wl_columnar_memory_governor_ref_t *governor
        = src->memory_governor ? src->memory_governor : session_governor;
    bool source_had_governor = src->memory_governor != NULL;
    int rc;

    *heap_out = NULL;
    rc = wl_columnar_source_access_writer_acquire(&src->descriptor_access,
            &descriptor_writer);
    if (rc != 0)
        return EBUSY;
    rc = wl_columnar_source_access_writer_acquire(&src->source_access,
            &owner_writer);
    if (rc != 0) {
        (void)wl_columnar_source_access_writer_release(&descriptor_writer);
        return EBUSY;
    }
    uint64_t state = atomic_load_explicit(&src->retained_reservation.state,
            memory_order_acquire);
    uint64_t payload_bytes = 0;
    uint64_t dedup_bytes = 0;
    if (!col_rel_retained_live_bytes(src, &payload_bytes)
        || wl_columnar_eval_dedup_set_bytes(src, &dedup_bytes) != 0
        || (source_had_governor && src->ncols && !src->row_scratch)) {
        rc = EINVAL;
        goto fail;
    }
    bool empty = state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && src->retained_reservation.bytes == 0
        && src->retained_reserved_bytes == 0
        && (!source_had_governor || payload_bytes == 0);
    bool committed = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        && src->retained_reserved_bytes > 0
        && src->retained_reserved_bytes == payload_bytes
        && src->retained_reservation.bytes == src->retained_reserved_bytes
        && src->retained_reservation.replacement_bytes == 0
        && src->memory_governor
        && src->retained_reservation.governor
        == wl_columnar_memory_governor_ref_get(src->memory_governor)
        && atomic_load_explicit(&src->retained_reservation.owner_bits,
            memory_order_acquire) == (uintptr_t)src;
    if (src->storage_owner != src
        || col_rel_storage_alias_borrow_count(src) != 0
        || src->retained_reservation.identity != &src->retained_reservation
        || (!empty && !committed)) {
        rc = EBUSY;
        goto fail;
    }
    rc = governor
        ? wl_columnar_relation_alloc_governed(&heap, src->name, governor)
        : col_rel_alloc(&heap, src->name ? src->name : "");
    if (rc != 0)
        goto fail;
    /* The pool name moves with its payload; discard the allocator's copy. */
    free(heap->name);
    heap->name = NULL;
    if (src->metadata_reserved_bytes) {
        if (!session_pool_rel_move_metadata(heap, src)) {
            col_rel_destroy(heap);
            rc = EBUSY;
            goto fail;
        }
    } else if (governor) {
        rc = wl_columnar_relation_admit_existing_metadata(heap, src);
        if (rc != 0) {
            col_rel_destroy(heap);
            goto fail;
        }
    }
    if (!wl_columnar_session_pool_rel_move_reservation(heap, src)) {
        if (src->memory_governor
            && !session_pool_rel_move_metadata(src, heap))
            abort();
        col_rel_destroy(heap);
        rc = EBUSY;
        goto fail;
    }
    if (!session_pool_rel_move_name(heap, src, governor)) {
        if (!wl_columnar_session_pool_rel_move_reservation(src, heap))
            abort();
        if (source_had_governor
            && !session_pool_rel_move_metadata(src, heap))
            abort();
        col_rel_destroy(heap);
        rc = EBUSY;
        goto fail;
    }
    if (source_had_governor) {
        if (!session_pool_rel_move_dedup(heap, src))
            rc = EBUSY;
        else
            rc = 0;
    } else {
        rc = session_pool_rel_admit_dedup(heap, dedup_bytes);
    }
    if (rc != 0) {
        if (!session_pool_rel_move_name(src, heap, governor)
            || !wl_columnar_session_pool_rel_move_reservation(src, heap)
            || (source_had_governor
            && !session_pool_rel_move_metadata(src, heap)))
            abort();
        col_rel_destroy(heap);
        goto fail;
    }
    session_pool_rel_transfer_payload(heap, src);
    heap->memory_governor = governor;
    if (source_had_governor)
        wl_columnar_memory_governor_ref_release(governor);
    wl_columnar_source_access_gate_init(&heap->source_access);
    wl_columnar_source_access_gate_init(&heap->descriptor_access);
    heap->pool_owned = false;
    heap->deferred_relation_next = NULL;
    heap->deferred_relation_session = NULL;
    heap->storage_owner = heap;
    heap->storage_owner_identity = heap->relation_identity;
    heap->storage_owner_generation = heap->storage_generation;
    memset(src, 0, offsetof(col_rel_t, retained_reservation));
    memset((char *)src + offsetof(col_rel_t, retained_reserved_bytes), 0,
        offsetof(col_rel_t, storage_alias_borrows)
        - offsetof(col_rel_t, retained_reserved_bytes));
    src->pool_owned = true;
    src->relation_identity = 0;
    src->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
    src->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID;
    src->storage_owner = NULL;
    src->deferred_relation_next = NULL;
    src->deferred_relation_session = NULL;
    atomic_store_explicit(&src->storage_alias_borrows, 0,
        memory_order_relaxed);
    wl_columnar_memory_reservation_init(&src->metadata_reservation);
    wl_columnar_memory_reservation_init(&src->pool_name_reservation);
    wl_columnar_memory_reservation_init(&src->dedup_reservation);
    wl_columnar_memory_reservation_init(&src->retained_reservation);
    /* The pool slot remains permanently closed until pool reset/reuse. */
    descriptor_writer.owner = NULL;
    owner_writer.owner = NULL;
    *heap_out = heap;
    return 0;

fail:
    (void)wl_columnar_source_access_writer_release(&owner_writer);
    (void)wl_columnar_source_access_writer_release(&descriptor_writer);
    return rc;
}

static void
session_pool_rel_rollback(col_rel_t *src, col_rel_t *heap,
    bool source_had_governor, bool source_had_scratch)
{
    wl_columnar_memory_reservation_t descriptor;
    wl_columnar_memory_reservation_t metadata;
    wl_columnar_memory_governor_ref_t *governor = heap->memory_governor;

    if (source_had_governor
        && !wl_columnar_session_pool_rel_move_reservation(src, heap))
        abort();
    if (source_had_governor && !session_pool_rel_move_dedup(src, heap))
        abort();
    if (!session_pool_rel_move_name(src, heap, governor))
        abort();
    session_pool_rel_transfer_payload(src, heap);
    if (!source_had_governor && heap->dedup_reserved_bytes) {
        if (!wl_columnar_memory_release(&heap->dedup_reservation))
            abort();
        heap->dedup_reserved_bytes = 0;
    }
    if (!source_had_governor) {
        if (!source_had_scratch) {
            free(src->row_scratch);
            src->row_scratch = NULL;
        }
        src->retained_reserved_bytes = 0;
        if (heap->retained_reserved_bytes
            && !wl_columnar_memory_release(&heap->retained_reservation))
            abort();
        heap->retained_reserved_bytes = 0;
        src->memory_budget_denial_pending = false;
    }
    wl_columnar_memory_reservation_init(&metadata);
    if (source_had_governor) {
        if (!session_pool_rel_move_metadata(src, heap))
            abort();
    } else if (heap->metadata_reserved_bytes
        && !wl_columnar_memory_reservation_move(&metadata,
        &heap->metadata_reservation)) {
        abort();
    }
    if (source_had_governor)
        wl_columnar_memory_governor_ref_retain(governor);
    else
        src->memory_governor = NULL;
    src->pool_owned = true;
    src->storage_owner = src;
    src->storage_owner_identity = src->relation_identity;
    src->storage_owner_generation = src->storage_generation;
    src->deferred_relation_next = NULL;
    src->deferred_relation_session = NULL;
    wl_columnar_source_access_gate_init(&src->source_access);
    wl_columnar_source_access_gate_init(&src->descriptor_access);
    wl_columnar_memory_reservation_init(&descriptor);
    if (heap->descriptor_reserved_bytes
        && !wl_columnar_memory_reservation_move(&descriptor,
        &heap->descriptor_reservation))
        abort();
    free(heap);
    if (governor) {
        if (metadata.bytes && !wl_columnar_memory_release(&metadata))
            abort();
        if (!wl_columnar_memory_release(&descriptor))
            abort();
        wl_columnar_memory_governor_ref_release(governor);
    }
}

int
session_add_rel(wl_col_session_t *sess, col_rel_t *r)
{
    int failure_rc = ENOMEM;
    if (!r)
        return EINVAL;
    /* Promotion copies the descriptor and clears the source slot.  Do not
     * perform that destructive transition while an operation-scoped reader
     * still points at the pool/arena relation. */
    if (wl_columnar_source_access_gate_busy(&r->source_access))
        return EBUSY;
    /* Pool-owned structs must be promoted to heap before storing in the
     * session, because col_session_destroy calls free() on each entry. */
    col_rel_t *pool_src = NULL;
    bool pool_src_had_governor = false;
    bool pool_src_had_scratch = false;
    if (r->pool_owned) {
        col_rel_t *heap = NULL;
        pool_src_had_governor = r->memory_governor != NULL;
        pool_src_had_scratch = r->row_scratch != NULL;
        int promote_rc = session_pool_rel_promote(r, sess->memory_governor,
                &heap);
        if (promote_rc != 0)
            return promote_rc;
        pool_src = r;
        r = heap;
        if (!pool_src_had_governor && r->memory_governor) {
            int admit_rc = wl_columnar_relation_admit_existing_payload(r);
            if (admit_rc != 0) {
                session_pool_rel_rollback(pool_src, r, false,
                    pool_src_had_scratch);
                return admit_rc;
            }
        }
    }
    /* Arena-owned data must be promoted to heap before storing in the
     * session, because arena_reset invalidates all arena pointers. */
    if (r->arena_owned && r->columns && r->ncols > 0) {
        /* Admission and the complete heap copy are transactional.  A
         * failure leaves the pool/arena relation unchanged so the caller can
         * still destroy or retry it safely. */
        if (col_rel_promote_arena_admitted(r) != 0)
            goto oom;
    }

    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, r->name) == 0) {
            if (sess->rels[i] == r) {
                if (pool_src)
                    session_pool_rel_finish_promotion(r);
                return 0;
            }
            session_invalidate_relation_caches(sess, r->name);
            col_rel_t *old_relation = sess->rels[i];
            wl_columnar_session_source_lease_t *old_lease
                = wl_columnar_session_source_lease_take(sess,
                    old_relation);
            int destroy_rc = col_rel_destroy_checked(old_relation);
            if (destroy_rc != 0) {
                wl_columnar_session_source_lease_restore(sess, old_lease);
                goto busy;
            }
            int release_rc
                = wl_columnar_session_source_lease_release(old_lease);
            assert(release_rc == 0);
            sess->rels[i] = r;
            if (pool_src)
                session_pool_rel_finish_promotion(r);
            return 0;
        }
    }

    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (!sess->rels[i]) {
            sess->rels[i] = r;
            int hash_ret = session_hash_result_or_fallback(
                session_rel_build_hash(sess));
            if (hash_ret != 0) {
                sess->rels[i] = NULL;
                failure_rc = hash_ret;
                goto oom;
            }
            if (pool_src)
                session_pool_rel_finish_promotion(r);
            return 0;
        }
    }

    if (sess->nrels >= sess->rel_cap) {
        if (sess->rel_cap > UINT32_MAX / 2u) {
            failure_rc = EOVERFLOW;
            goto oom;
        }
        uint32_t nc = sess->rel_cap ? sess->rel_cap * 2u : 16u;
        uint64_t bytes;
        if (!wl_columnar_memory_size_mul(nc, sizeof(col_rel_t *),
            &bytes) || bytes > SIZE_MAX) {
            failure_rc = EOVERFLOW;
            goto oom;
        }
        wl_columnar_memory_reservation_t pending;
        wl_columnar_memory_reservation_init(&pending);
        failure_rc = session_rels_admit(sess, bytes, &pending);
        if (failure_rc != 0)
            goto oom;
        col_rel_t **nr = (col_rel_t **)malloc((size_t)bytes);
        if (!nr) {
            session_rels_release(&pending);
            failure_rc = ENOMEM;
            goto oom;
        }
        if (sess->nrels > 0)
            memcpy((void *)nr, (const void *)sess->rels,
                (size_t)sess->nrels * sizeof(*nr));
        free((void *)sess->rels);
        session_rels_release(&sess->rels_reservation);
        if (sess->memory_governor) {
            if (sess->rels_reservation.identity
                != &sess->rels_reservation)
                wl_columnar_memory_reservation_init(
                    &sess->rels_reservation);
            if (!wl_columnar_memory_reservation_move(
                    &sess->rels_reservation, &pending))
                abort();
        }
        sess->rels = nr;
        sess->rel_cap = nc;
    }
    uint32_t idx = sess->nrels;
    sess->rels[sess->nrels++] = r;

    /* Update hash table for O(1) lookup (Issue #281).
     * May rebuild if load factor exceeded or rebuild on first insert. */
    int hash_ret = session_rel_hash_insert(sess, idx);
    hash_ret = session_hash_result_or_fallback(hash_ret);
    if (hash_ret != 0) {
        sess->rels[--sess->nrels] = NULL;
        failure_rc = hash_ret;
        goto oom;
    }
    /* Optional hash allocation or admission failure leaves linear lookup
     * available in session_find_rel. */

    if (pool_src)
        session_pool_rel_finish_promotion(r);
    return 0;

busy:
    if (pool_src)
        session_pool_rel_rollback(pool_src, r, pool_src_had_governor,
            pool_src_had_scratch);
    return EBUSY;

oom:
    /* Undo the pool-to-heap promotion.  A failed session_add_rel leaves the
     * relation with the caller, which destroys it via col_rel_destroy(), so
     * the promoted copy has to go back into the pool slot before returning.
     * Without this the copy -- and the name, col_names[] and compound
     * metadata it is now the only pointer to -- leaks, and the caller's
     * col_rel_destroy() sees the zeroed slot with pool_owned cleared by the
     * memset above and calls free() on an interior pointer into the
     * delta_pool slab.  Both promotions run on relations from
     * col_rel_pool_new_auto(), which sets pool_owned and arena_owned
     * together. */
    if (pool_src) {
        session_pool_rel_rollback(pool_src, r, pool_src_had_governor,
            pool_src_had_scratch);
    }
    return failure_rc;
}

/* Registration for a newly staged retraction relation is prepared without
 * changing the session.  The commit path can then swap the relation pointer
 * and its row source after EDB compaction has become non-failing. */
struct session_rel_registration {
    uint32_t index;
    bool append;
    col_rel_t *old_relation;
    col_rel_t **replacement_array;
    uint32_t replacement_capacity;
    wl_columnar_memory_reservation_t replacement_reservation;
    wl_columnar_relation_retirement_token_t retirement;
};

static void
session_rel_registration_init(session_rel_registration_t *registration)
{
    memset(registration, 0, sizeof(*registration));
    wl_columnar_memory_reservation_init(&registration->replacement_reservation);
    wl_columnar_relation_retirement_init(&registration->retirement);
}

static int
session_rel_registration_prepare(wl_col_session_t *sess,
    col_rel_t *relation, session_rel_registration_t *registration)
{
    uint32_t hole = UINT32_MAX;

    if (!sess || !relation || !relation->name || !registration
        || relation->pool_owned || relation->arena_owned)
        return EINVAL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *current = sess->rels[i];
        if (!current) {
            if (hole == UINT32_MAX)
                hole = i;
            continue;
        }
        if (strcmp(current->name, relation->name) == 0) {
            registration->old_relation = current;
            registration->index = i;
            int rc = wl_columnar_relation_retirement_prepare(current,
                    &registration->retirement);
            if (rc != 0)
                return rc;
            return 0;
        }
    }

    if (hole != UINT32_MAX) {
        registration->index = hole;
        return 0;
    }
    if (sess->nrels < sess->rel_cap) {
        registration->index = sess->nrels;
        registration->append = true;
        return 0;
    }

    uint32_t capacity = sess->rel_cap ? sess->rel_cap : 16u;
    if (capacity <= sess->nrels) {
        if (capacity > UINT32_MAX / 2u)
            return EOVERFLOW;
        capacity *= 2u;
    }
    uint64_t array_bytes;
    if (capacity <= sess->nrels
        || !wl_columnar_memory_size_mul(capacity, sizeof(*sess->rels),
        &array_bytes)
        || array_bytes > SIZE_MAX)
        return EOVERFLOW;
    int admit_rc = session_rels_admit(sess, array_bytes,
            &registration->replacement_reservation);
    if (admit_rc != 0)
        return admit_rc;
    col_rel_t **replacement = (col_rel_t **)calloc(capacity,
            sizeof(*replacement));
    if (!replacement) {
        session_rels_release(&registration->replacement_reservation);
        return ENOMEM;
    }
    if (sess->nrels != 0)
        memcpy((void *)replacement, (const void *)sess->rels,
            (size_t)sess->nrels * sizeof(*replacement));
    registration->replacement_array = replacement;
    registration->replacement_capacity = capacity;
    registration->index = sess->nrels;
    registration->append = true;
    return 0;
}

static void
session_rel_registration_discard(session_rel_registration_t *registration)
{
    if (!registration)
        return;
    if (registration->retirement.relation)
        (void)wl_columnar_relation_retirement_cancel(
            &registration->retirement);
    free((void *)registration->replacement_array);
    session_rels_release(&registration->replacement_reservation);
    session_rel_registration_init(registration);
}

/* Return success only after the prepared retirement token belongs to the
 * expected public head generation.  Callers can then commit immediately. */
static int
session_rel_registration_prepare_expected(wl_col_session_t *sess,
    col_rel_t *relation, col_rel_t *expected_old, uint64_t identity,
    uint64_t view_generation, uint64_t storage_generation,
    session_rel_registration_t *registration)
{
    int rc = session_rel_registration_prepare(sess, relation, registration);
    if (rc)
        return rc;
    col_rel_t *prepared_old = registration->old_relation;
    if (prepared_old != expected_old || !prepared_old
        || prepared_old->relation_identity != identity
        || prepared_old->view_generation != view_generation
        || prepared_old->storage_generation != storage_generation) {
        session_rel_registration_discard(registration);
        return EINVAL;
    }
    return 0;
}

static void
session_rel_registration_commit(wl_col_session_t *sess, col_rel_t *relation,
    session_rel_registration_t *registration)
{
    col_rel_t **old_array = NULL;
    wl_columnar_session_source_lease_t *old_lease = NULL;
    bool replaced_array = registration->replacement_array != NULL;

    if (registration->old_relation) {
        session_invalidate_relation_caches(sess, relation->name);
        old_lease = wl_columnar_session_source_lease_take(sess,
                registration->old_relation);
    }
    if (registration->replacement_array) {
        old_array = sess->rels;
        sess->rels = registration->replacement_array;
        sess->rel_cap = registration->replacement_capacity;
        registration->replacement_array = NULL;
    }
    sess->rels[registration->index] = relation;
    if (registration->append && registration->index == sess->nrels)
        sess->nrels++;
    session_rel_free_hash(sess);
    if (registration->old_relation)
        wl_columnar_relation_retirement_commit(&registration->retirement);
    free((void *)old_array);
    if (replaced_array) {
        session_rels_release(&sess->rels_reservation);
        if (sess->memory_governor) {
            if (sess->rels_reservation.identity
                != &sess->rels_reservation)
                wl_columnar_memory_reservation_init(
                    &sess->rels_reservation);
            if (!wl_columnar_memory_reservation_move(
                    &sess->rels_reservation,
                    &registration->replacement_reservation))
                abort();
        }
    }
    if (old_lease) {
        int release_rc = wl_columnar_session_source_lease_release(old_lease);
        assert(release_rc == 0);
    }
    session_rel_registration_init(registration);
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_columnar_session_test_register_retraction(wl_col_session_t *sess,
    col_rel_t *relation, bool publish)
{
    session_rel_registration_t registration;
    session_rel_registration_init(&registration);
    int rc = session_rel_registration_prepare(sess, relation, &registration);
    if (rc != 0)
        return rc;
    if (publish)
        session_rel_registration_commit(sess, relation, &registration);
    else
        session_rel_registration_discard(&registration);
    return 0;
}

int
wl_columnar_session_test_register_expected_head(wl_col_session_t *sess,
    col_rel_t *relation, col_rel_t *expected_old, uint64_t identity,
    uint64_t view_generation, uint64_t storage_generation, bool publish)
{
    session_rel_registration_t registration;
    session_rel_registration_init(&registration);
    int rc = session_rel_registration_prepare_expected(sess, relation,
            expected_old, identity, view_generation, storage_generation,
            &registration);
    if (rc != 0)
        return rc;
    if (publish)
        session_rel_registration_commit(sess, relation, &registration);
    else
        session_rel_registration_discard(&registration);
    return 0;
}
#endif

int
session_remove_rel(wl_col_session_t *sess, const char *name)
{
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && strcmp(sess->rels[i]->name, name) == 0) {
            session_invalidate_relation_caches(sess, name);
            col_rel_t *relation = sess->rels[i];
            wl_columnar_session_source_lease_t *lease
                = wl_columnar_session_source_lease_take(sess, relation);
            if (col_rel_destroy_checked(relation) != 0) {
                wl_columnar_session_source_lease_restore(sess, lease);
                return EBUSY;
            }
            int release_rc = wl_columnar_session_source_lease_release(lease);
            assert(release_rc == 0);
            sess->rels[i] = NULL;
            session_rel_free_hash(sess);
            return 0;
        }
    }
    return ENOENT;
}

typedef struct wl_columnar_session_tdd_decision {
    bool use_tdd;
    wl_columnar_internal_tdd_fallback_reason_t fallback_reason;
} wl_columnar_session_tdd_decision_t;

static const char *
wl_columnar_session_tdd_fallback_reason_name(
    wl_columnar_internal_tdd_fallback_reason_t reason)
{
    switch (reason) {
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE:
        return "none";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NON_RECURSIVE:
        return "non_recursive";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_SNAPSHOT_INELIGIBLE:
        return "snapshot_ineligible";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NO_EXCHANGE:
        return "no_exchange";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN:
        return "unsafe_plan";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS:
        return "adaptive_workers";
    case WL_COLUMNAR_INTERNAL_TDD_FALLBACK_REASON_COUNT:
        break;
    }
    return "unknown";
}

static bool
wl_columnar_session_tdd_stratum_has_exchange(const wl_plan_stratum_t *sp)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];
        for (uint32_t oi = 0; oi < rp->op_count; oi++) {
            if (rp->ops[oi].op == WL_PLAN_OP_EXCHANGE)
                return true;
        }
    }
    return false;
}

static bool
wl_columnar_session_tdd_stratum_is_safe(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    if (tdd_stratum_has_unsupported_lftj(sp))
        return false;

    /* TDD is only beneficial for recursive strata when there is no
     * IDB self-join OR the IDB self-join is exchange-aligned.  A
     * non-aligned self-join forces replicate_mode (all W workers do
     * the full join), which is worse than single-threaded. */
    if (!tdd_stratum_has_idb_self_join(sp)) {
        /* Category A: no IDB-IDB joins.  Owner-partitioned TDD is
         * correct only when every recursive IDB probe uses the
         * relation's EXCHANGE key.  Rules that consume recursive IDB
         * rows through non-exchange keys or multiple IDB body atoms
         * require cross-owner access and must stay single-threaded for
         * now. */
        return stratum_max_idb_body_atoms(sp) <= 1
               && tdd_stratum_single_idb_join_keys_exchange_aligned(sp);
    }
    if (tdd_stratum_idb_self_join_exchange_aligned(sp, sess))
        return true;

    /* Category C: non-aligned IDB-IDB joins. BDX mode is only correct for
     * rules with at most 2 IDB body atoms. */
    return stratum_max_idb_body_atoms(sp) <= 2;
}

static void
wl_columnar_session_tdd_debug_decision(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, bool snapshot_tdd_eligible,
    wl_columnar_session_tdd_decision_t decision)
{
    const char *env = getenv("WIRELOG_TDD_DECISION_DEBUG");
    if (!env || env[0] == '\0' || env[0] == '0')
        return;

    /* Diagnostic capability only; never participates in admission. K=1 and
     * unsupported relations are reported explicitly, not silently omitted. */
    if (sp) {
        for (uint32_t r = 0; r < sp->relation_count; r++) {
            uint32_t counts[3];
            int rc = wl_columnar_eval_tdd_plan_binding_summary(sp, r, counts);
            fprintf(stderr,
                "TDD bindings rel=%s status=%d alternatives=%u slices=%u reads=%u\n",
                sp->relations[r].name ? sp->relations[r].name : "(null)", rc,
                counts[0], counts[1], counts[2]);
        }
    }

    const char *first_rel = (sp && sp->relation_count > 0)
        ? sp->relations[0].name : "(none)";
    wl_tdd_segment_stats_t segment_stats;
    tdd_stratum_segment_stats(sp, &segment_stats);
    fprintf(stderr,
        "TDD decision rel=%s recursive=%d snapshot=%d exchange=%d "
        "safe=%d global_read_candidate=%d self_join=%d idb_atoms=%u "
        "segments=%u segment_seed=%u segment_global_read=%u "
        "segment_unsafe=%u segment_max_idb=%u segment_max_joins=%u "
        "single_key_aligned=%d use_tdd=%d fallback=%s\n",
        first_rel ? first_rel : "(null)",
        sp ? (int)sp->is_recursive : 0,
        (int)snapshot_tdd_eligible,
        sp ? (int)wl_columnar_session_tdd_stratum_has_exchange(sp) : 0,
        sp ? (int)wl_columnar_session_tdd_stratum_is_safe(sp, sess) : 0,
        sp ? (int)tdd_stratum_global_read_candidate(sp) : 0,
        sp ? (int)tdd_stratum_has_idb_self_join(sp) : 0,
        sp ? stratum_max_idb_body_atoms(sp) : 0,
        segment_stats.total_segments,
        segment_stats.seed_only_segments,
        segment_stats.global_read_segments,
        segment_stats.unsafe_segments,
        segment_stats.max_segment_idb_atoms,
        segment_stats.max_segment_join_like,
        sp ? (int)tdd_stratum_single_idb_join_keys_exchange_aligned(sp) : 0,
        (int)decision.use_tdd,
        wl_columnar_session_tdd_fallback_reason_name(
            decision.fallback_reason));
}

static wl_columnar_session_tdd_decision_t
wl_columnar_session_tdd_plan_stratum(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess,
    bool snapshot_tdd_eligible)
{
    wl_columnar_session_tdd_decision_t decision = {
        .use_tdd = false,
        .fallback_reason = WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE,
    };

    if (!sp->is_recursive) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NON_RECURSIVE;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (tdd_stratum_has_unsupported_lftj(sp)) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (!snapshot_tdd_eligible) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_SNAPSHOT_INELIGIBLE;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (!wl_columnar_session_tdd_stratum_has_exchange(sp)) {
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NO_EXCHANGE;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }
    if (!wl_columnar_session_tdd_stratum_is_safe(sp, sess)) {
        const char *env = getenv("WIRELOG_TDD_GLOBAL_READ");
        if (!(env && env[0] == '0' && env[1] == '\0')
            && tdd_stratum_global_read_candidate(sp)) {
            decision.use_tdd = true;
            wl_columnar_session_tdd_debug_decision(sp, sess,
                snapshot_tdd_eligible, decision);
            return decision;
        }
        decision.fallback_reason =
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN;
        wl_columnar_session_tdd_debug_decision(sp, sess,
            snapshot_tdd_eligible, decision);
        return decision;
    }

    decision.use_tdd = true;
    wl_columnar_session_tdd_debug_decision(sp, sess, snapshot_tdd_eligible,
        decision);
    return decision;
}

/* Operator implementations moved to columnar/ops.c;
 * evaluator functions moved to columnar/eval.c;
 * declarations in columnar/internal.h. */

/* ======================================================================== */
/* Public Accessors                                                          */
/* ======================================================================== */

/*
 * col_session_get_iteration_count:
 *
 * Return the number of fixed-point iterations performed during the last
 * call to col_eval_stratum.  Returns 0 if no evaluation has occurred yet.
 *
 * @param sess  A wl_session_t* backed by the columnar backend.
 */
uint32_t
col_session_get_iteration_count(wl_session_t *sess)
{
    return COL_SESSION(sess)->total_iterations;
}

/*
 * col_session_get_cache_stats:
 *
 * Return CSE materialization cache hit and miss counts accumulated during
 * the last evaluation.  Both out-parameters are optional (NULL-safe).
 *
 * @param sess    A wl_session_t* backed by the columnar backend.
 * @param out_hits    Set to the number of cache hits (may be NULL).
 * @param out_misses  Set to the number of cache misses (may be NULL).
 */
void
col_session_get_cache_stats(wl_session_t *sess, uint64_t *out_hits,
    uint64_t *out_misses)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_hits)
        *out_hits = cs->mat_cache.hits;
    if (out_misses)
        *out_misses = cs->mat_cache.misses;
}

/*
 * col_session_get_perf_stats:
 *
 * Return accumulated profiling counters (in nanoseconds) from the last
 * wl_session_snapshot() call.  Counters are reset at the start of each
 * evaluation pass.  Both out-parameters are optional (NULL-safe).
 *
 * @param sess             A wl_session_t* backed by the columnar backend.
 * @param out_consolidation_ns  Time spent in incremental consolidation.
 * @param out_kfusion_ns        Time spent in K-fusion dispatch.
 */
void
col_session_get_perf_stats(wl_session_t *sess, uint64_t *out_consolidation_ns,
    uint64_t *out_kfusion_ns,
    uint64_t *out_kfusion_alloc_ns,
    uint64_t *out_kfusion_dispatch_ns,
    uint64_t *out_kfusion_merge_ns,
    uint64_t *out_kfusion_cleanup_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_consolidation_ns)
        *out_consolidation_ns = cs->consolidation_ns;
    if (out_kfusion_ns)
        *out_kfusion_ns = cs->kfusion_ns;
    if (out_kfusion_alloc_ns)
        *out_kfusion_alloc_ns = cs->kfusion_alloc_ns;
    if (out_kfusion_dispatch_ns)
        *out_kfusion_dispatch_ns = cs->kfusion_dispatch_ns;
    if (out_kfusion_merge_ns)
        *out_kfusion_merge_ns = cs->kfusion_merge_ns;
    if (out_kfusion_cleanup_ns)
        *out_kfusion_cleanup_ns = cs->kfusion_cleanup_ns;
}

/*
 * col_session_get_consolidation_stats:
 *
 * Return fast-path and slow-path hit counts accumulated across all
 * consolidation calls in the last wl_session_snapshot().
 * Both out-parameters are NULL-safe.
 *
 * @param sess            A wl_session_t* backed by the columnar backend.
 * @param out_fast_hits   Count of calls that took the O(D) fast path.
 * @param out_slow_hits   Count of calls that took the O(N+D) merge walk.
 */
void
col_session_get_consolidation_stats(wl_session_t *sess,
    uint64_t *out_fast_hits, uint64_t *out_slow_hits)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_fast_hits)
        *out_fast_hits = cs->consolidate_fast_hits;
    if (out_slow_hits)
        *out_slow_hits = cs->consolidate_slow_hits;
}

/*
 * col_session_get_exchange_time_ns:
 *
 * Return legacy coordinator serial time across the last wl_session_snapshot()
 * call.  This includes recursive TDD exchange scatter/gather and the
 * non-recursive TDD coordinator merge path.  out_exchange_ns is NULL-safe.
 */
void
col_session_get_exchange_time_ns(wl_session_t *sess,
    uint64_t *out_exchange_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_exchange_ns)
        *out_exchange_ns = cs->exchange_time_ns;
}

/*
 * col_session_get_tdd_perf_stats:
 *
 * Return recursive TDD evaluator profiling counters from the last
 * wl_session_snapshot() call.  The counters expose the current
 * queue-assisted after-barrier implementation without changing execution.
 * All out-parameters are NULL-safe.
 */
void
col_session_get_tdd_perf_stats(wl_session_t *sess, uint64_t *out_total_ns,
    uint64_t *out_dispatch_wait_ns, uint64_t *out_submit_loop_ns,
    uint64_t *out_wait_barrier_ns, uint64_t *out_worker_sum_ns,
    uint64_t *out_worker_max_ns, uint64_t *out_idle_estimate_ns,
    uint64_t *out_queue_drain_ns,
    uint64_t *out_convergence_ns, uint64_t *out_exchange_ns,
    uint64_t *out_final_merge_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_total_ns)
        *out_total_ns = cs->tdd_total_ns;
    if (out_dispatch_wait_ns)
        *out_dispatch_wait_ns = cs->tdd_dispatch_wait_ns;
    if (out_submit_loop_ns)
        *out_submit_loop_ns = cs->tdd_submit_loop_ns;
    if (out_wait_barrier_ns)
        *out_wait_barrier_ns = cs->tdd_wait_barrier_ns;
    if (out_worker_sum_ns)
        *out_worker_sum_ns = cs->tdd_worker_sum_ns;
    if (out_worker_max_ns)
        *out_worker_max_ns = cs->tdd_worker_max_ns;
    if (out_idle_estimate_ns)
        *out_idle_estimate_ns = cs->tdd_idle_estimate_ns;
    if (out_queue_drain_ns)
        *out_queue_drain_ns = cs->tdd_queue_drain_ns;
    if (out_convergence_ns)
        *out_convergence_ns = cs->tdd_convergence_ns;
    if (out_exchange_ns)
        *out_exchange_ns = cs->tdd_exchange_ns;
    if (out_final_merge_ns)
        *out_final_merge_ns = cs->tdd_final_merge_ns;
}

/*
 * wl_columnar_session_get_tdd_exchange_breakdown_stats:
 *
 * Return private recursive TDD exchange sub-counters from the last
 * wl_session_snapshot() call.  Kept out of the installed columnar header so
 * benchmark instrumentation can advance without changing public API shape.
 * All out-parameters are NULL-safe.
 */
#if defined(__GNUC__) && __GNUC__ >= 4
__attribute__((visibility("hidden")))
#endif
void
wl_columnar_session_get_tdd_exchange_breakdown_stats(wl_session_t *sess,
    uint64_t *out_matrix_ns, uint64_t *out_coordinator_ns,
    uint64_t *out_scatter_ns, uint64_t *out_gather_ns,
    uint64_t *out_broadcast_ns)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_matrix_ns)
        *out_matrix_ns = cs->tdd_exchange_matrix_ns;
    if (out_coordinator_ns)
        *out_coordinator_ns = cs->tdd_exchange_coordinator_ns;
    if (out_scatter_ns)
        *out_scatter_ns = cs->tdd_exchange_scatter_ns;
    if (out_gather_ns)
        *out_gather_ns = cs->tdd_exchange_gather_ns;
    if (out_broadcast_ns)
        *out_broadcast_ns = cs->tdd_exchange_broadcast_ns;
}

/*
 * wl_columnar_session_get_tdd_worker_width_stats:
 *
 * Return adaptive TDD worker-width selections from the last snapshot.
 * Hidden benchmark instrumentation hook; not part of the installed API.
 */
#if defined(__GNUC__) && __GNUC__ >= 4
__attribute__((visibility("hidden")))
#endif
void
wl_columnar_session_get_tdd_worker_width_stats(wl_session_t *sess,
    uint32_t *out_last_active_workers, uint32_t *out_max_active_workers)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_last_active_workers)
        *out_last_active_workers = cs->tdd_last_active_workers;
    if (out_max_active_workers)
        *out_max_active_workers = cs->tdd_max_active_workers;
}

/*
 * wl_columnar_session_get_tdd_decision_stats:
 *
 * Return TDD planner decisions from the last snapshot.  Recursive strata are
 * planned TDD-first; fallback counters explain why K-fusion/serial evaluation
 * was used instead.
 */
#if defined(__GNUC__) && __GNUC__ >= 4
__attribute__((visibility("hidden")))
#endif
void
wl_columnar_session_get_tdd_decision_stats(wl_session_t *sess,
    uint32_t *out_recursive_strata, uint32_t *out_executed_strata,
    uint32_t *out_fallback_strata, uint32_t *out_snapshot_ineligible,
    uint32_t *out_no_exchange, uint32_t *out_unsafe_plan,
    uint32_t *out_adaptive_workers, const char **out_last_fallback_reason)
{
    wl_col_session_t *cs = COL_SESSION(sess);
    if (out_recursive_strata)
        *out_recursive_strata = cs->tdd_recursive_strata;
    if (out_executed_strata)
        *out_executed_strata = cs->tdd_executed_strata;
    if (out_fallback_strata)
        *out_fallback_strata = cs->tdd_fallback_strata;
    if (out_snapshot_ineligible) {
        *out_snapshot_ineligible =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_SNAPSHOT_INELIGIBLE];
    }
    if (out_no_exchange) {
        *out_no_exchange =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NO_EXCHANGE];
    }
    if (out_unsafe_plan) {
        *out_unsafe_plan =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_UNSAFE_PLAN];
    }
    if (out_adaptive_workers) {
        *out_adaptive_workers =
            cs->tdd_fallback_reason_counts[
            WL_COLUMNAR_INTERNAL_TDD_FALLBACK_ADAPTIVE_WORKERS];
    }
    if (out_last_fallback_reason) {
        *out_last_fallback_reason =
            wl_columnar_session_tdd_fallback_reason_name(
            cs->tdd_last_fallback_reason);
    }
}

int
wl_columnar_session_ensure_workqueue(wl_col_session_t *sess,
    uint32_t active_workers)
{
    if (!sess)
        return EINVAL;
    if (active_workers <= 1)
        return 0;
    if (active_workers > sess->num_workers)
        return EINVAL;
    if (sess->wq && sess->wq_workers >= active_workers)
        return 0;

    wl_work_queue_t *new_wq = wl_workqueue_create(active_workers);
    if (!new_wq)
        return ENOMEM;

    wl_workqueue_destroy(sess->wq);
    sess->wq = new_wq;
    sess->wq_workers = active_workers;
    return 0;
}

int
wl_columnar_session_ensure_tdd_worker_slots(wl_col_session_t *sess,
    uint32_t active_workers)
{
    if (!sess)
        return EINVAL;
    if (sess->tdd_owner_lifetime)
        return EBUSY;
    if (active_workers == 0 || active_workers > sess->num_workers)
        return EINVAL;
    if (sess->tdd_workers_cap >= active_workers)
        return 0;

    wl_col_session_t *new_workers = (wl_col_session_t *)realloc(
        sess->tdd_workers, active_workers * sizeof(wl_col_session_t));
    if (!new_workers)
        return ENOMEM;

    memset(&new_workers[sess->tdd_workers_cap], 0,
        (active_workers - sess->tdd_workers_cap) * sizeof(wl_col_session_t));
    sess->tdd_workers = new_workers;
    sess->tdd_workers_cap = active_workers;
    return 0;
}

/* ======================================================================== */
/* Vtable Functions                                                          */
/* ======================================================================== */

/*
 * col_should_activate_diff: Determine whether differential operators should
 * be used for this evaluation pass (Issue #264).
 *
 * Differential operators are activated only when ALL conditions hold:
 *   1. diff_enabled is true (session-level master switch)
 *   2. affected_mask is not UINT64_MAX (not a non-incremental full eval)
 *   3. affected_mask is not 0 (at least one stratum affected)
 *
 * When any condition fails, epoch-based operators are used (safe fallback).
 */
static bool
col_should_activate_diff(const wl_col_session_t *sess, uint64_t affected_mask)
{
    return sess->diff_enabled
           && affected_mask != UINT64_MAX
           && affected_mask != 0;
}

/*
 * session_snapshot_delta_safe: Whether the strata in @mask may be brought
 * up to date over an inserted delta (Issue #2114).  A stratum that retracts
 * through negation must lose rows no delta names, and an aggregate over new
 * rows alone replaces nothing, so either sends the snapshot to a full
 * evaluation.  The insertion binder refuses both operators as well; this
 * reads the flags the plan recorded before its rewrites, which no fusion
 * can hide.
 */
static bool
session_snapshot_delta_safe(const wl_plan_t *plan, uint64_t mask)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (!col_affected_mask_contains(mask, si))
            continue;
        if (!plan->strata[si].is_monotone || plan->strata[si].has_aggregate)
            return false;
    }
    return true;
}

/* Reset every stratum and rule frontier to (current_epoch, UINT32_MAX), so
 * no frontier recorded before this evaluation can skip an iteration. */
static void
session_reset_frontiers(const wl_plan_t *plan, wl_col_session_t *sess)
{
    for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA; si++)
        sess->frontier_ops->reset_stratum_frontier(sess, si,
            sess->outer_epoch);
    for (uint32_t ri = 0; ri < MAX_RULES; ri++)
        sess->frontier_ops->reset_rule_frontier(sess, ri, sess->outer_epoch);
}

/* Issue #2114: may every stratum in @mask be bound by the insertion binder?
 * Asked before anything is mutated; the binder's refusals do not depend on
 * which relations changed, so none are passed. */
static bool
session_snapshot_insert_admitted(const wl_plan_t *plan, uint64_t mask)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (!col_affected_mask_contains(mask, si))
            continue;
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            for (int round = 0; round < (sp->is_recursive ? 2 : 1); round++) {
                wl_columnar_eval_tdd_plan_manifest_t m;
                int rc = wl_columnar_eval_tdd_plan_insert_bindings(sp, ri,
                        NULL, 0, round == 0, &m);
                wl_columnar_eval_tdd_plan_bindings_free(&m);
                if (rc != 0)
                    return false;
            }
        }
    }
    return true;
}

/* #1382: does a stratum in @mask hold a JOIN?  The insertion binder admits
 * JOIN, SEMIJOIN, MAP and FILTER and refuses LFTJ and K-Fusion (they carry
 * opaque_data), and only a JOIN would be a bounded-mode candidate. */
static bool
session_snapshot_insert_joins(const wl_plan_t *plan, uint64_t mask)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (!col_affected_mask_contains(mask, si))
            continue;
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; sp->bodies && ri < sp->relation_count; ri++) {
            const wl_plan_body_t *body = &sp->bodies[ri];
            for (uint32_t o = 0; body->ops && o < body->op_count; o++) {
                if (body->ops[o].op == WL_PLAN_OP_JOIN)
                    return true;
            }
        }
    }
    return false;
}

#ifdef WL_SESSION_TEST_HOOKS
/* Issue #2119: calls of session_snapshot_insert() that returned 0, counted
 * by the snapshotting thread.  A later cutoff, compaction or emit failure in
 * the same snapshot does not undo the count, and the retry counts again.
 * Plain, not atomic: the tests that read it snapshot from one thread. */
uint32_t wl_columnar_session_test_insert_route_taken;
#endif

/*
 * session_snapshot_insert: Bring the strata an incremental insert reaches up
 * to date without re-deriving what their heads hold (Issue #2114).
 *
 * The rows appended to the inserted relation since the last evaluation are
 * rows [base_nrows, nrows).  Each reached stratum, in plan order, runs the
 * insertion rounds of wl_columnar_eval_tdd_insert_stratum() with every
 * relation that has gained rows so far as a changed relation: the inserted
 * one, and the heads of the strata before it.  Returns ENOTSUP, with nothing
 * mutated, when a reached stratum negates, aggregates or holds an operator
 * the binder cannot bind; any other failure may leave heads partly updated.
 */
static int
session_snapshot_insert(wl_session_t *session, const wl_plan_t *plan)
{
    wl_col_session_t *sess = COL_SESSION(session);
    const char *inserted = sess->last_inserted_relation;
    col_rel_t *rel = session_find_rel(sess, inserted);
    uint64_t mask = col_compute_affected_strata(session, inserted);

    if (!rel || rel->base_nrows > rel->nrows
        || !session_snapshot_delta_safe(plan, mask)
        || !session_snapshot_insert_admitted(plan, mask))
        return ENOTSUP;
    /* The insertion rounds run on a worker clone with bounded join batching
     * turned off (insert_clone_prepare).  Under WIRELOG_JOIN_BATCH_BYTES
     * that is a fallback for a stratum with a JOIN: recorded, or refused in
     * strict mode before anything is mutated, so the snapshot is evaluated
     * in full instead (#1382). */
    if (sess->join_batch_bytes > 0
        && session_snapshot_insert_joins(plan, mask)) {
        if (sess->join_batch_strict)
            return ENOTSUP;
        col_join_batch_record_fallback(sess,
            COL_JOIN_BATCH_EXCLUDED_INSERT_ROUTE);
    }

    uint32_t capacity = 1;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (col_affected_mask_contains(mask, si))
            capacity += plan->strata[si].relation_count;
    }
    const char **names = (const char **)calloc(capacity, sizeof(*names));
    col_rel_t **deltas = (col_rel_t **)calloc(capacity, sizeof(*deltas));
    col_rel_t **heads = NULL;
    uint32_t heads_count = 0;
    uint32_t nchanged = 0;
    int rc = 0;

    if (!names || !deltas) {
        rc = ENOMEM;
        goto done;
    }
    rc = wl_columnar_relation_copy_rows("$inserted", rel, rel->base_nrows,
            rel->nrows, sess->memory_governor, &deltas[0]);
    if (rc != 0)
        goto done;
    names[0] = inserted;
    nchanged = 1;

    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        if (!col_affected_mask_contains(mask, si))
            continue;
        const wl_plan_stratum_t *sp = &plan->strata[si];
        uint32_t nheads = sp->relation_count;
        heads = (col_rel_t **)calloc(nheads ? nheads : 1, sizeof(*heads));
        if (!heads) {
            rc = ENOMEM;
            goto done;
        }
        heads_count = nheads;
        rc = wl_columnar_eval_tdd_insert_stratum(sess, sp, names,
                (col_rel_t *const *)deltas, nchanged, heads);
        if (rc != 0)
            goto done;
        for (uint32_t ri = 0; ri < nheads; ri++) {
            if (!heads[ri])
                continue;
            if (nchanged >= capacity) {
                rc = EOVERFLOW;
                goto done;
            }
            names[nchanged] = sp->relations[ri].name;
            deltas[nchanged++] = heads[ri];
            heads[ri] = NULL;
        }
        free((void *)heads);
        heads = NULL;
        heads_count = 0;
    }

done:
    if (heads) {
        for (uint32_t ri = 0; ri < heads_count; ri++)
            col_rel_destroy(heads[ri]);
        free((void *)heads);
    }
    for (uint32_t c = 0; deltas && c < capacity; c++)
        col_rel_destroy(deltas[c]);
    free((void *)names);
    free((void *)deltas);
    return rc;
}

#ifdef WL_TEST_MUTATION_SET_HOOK
int
wl_columnar_session_test_snapshot_insert(wl_session_t *session,
    const wl_plan_t *plan)
{
    return session_snapshot_insert(session, plan);
}
#endif

/*
 * col_detect_physical_memory: Detect total physical RAM in bytes (Issue #221).
 *
 * Uses platform-specific syscalls to query total installed RAM. Returns 0 if
 * the platform is not supported or if detection fails. No /proc/meminfo used.
 */
uint64_t
col_detect_physical_memory(void)
{
#ifdef __linux__
    struct sysinfo info;
    if (sysinfo(&info) == 0)
        return (uint64_t)info.totalram * (uint64_t)info.mem_unit;
    return 0;
#elif defined(__APPLE__)
    int mib[2] = { CTL_HW, HW_MEMSIZE };
    uint64_t memsize = 0;
    size_t len = sizeof(memsize);
    if (sysctl(mib, 2, &memsize, &len, NULL, 0) == 0)
        return memsize;
    return 0;
#elif defined(_WIN32)
    MEMORYSTATUSEX memStatus;
    memStatus.dwLength = sizeof(memStatus);
    if (GlobalMemoryStatusEx(&memStatus))
        return (uint64_t)memStatus.ullTotalPhys;
    return 0;
#else
    return 0;
#endif
}

/* ======================================================================== */
/* Memory instrumentation (Issue #1380)                                     */
/* ======================================================================== */

int
col_session_mem_report_level(void)
{
    const char *env = getenv("WL_MEM_REPORT");
    if (!env || env[0] == '\0')
        return 0;
    if (env[0] == '0' && env[1] == '\0')
        return 0;
    if (env[0] == '2')
        return 2;
    return 1;
}

uint64_t
col_session_peak_rss_bytes(void)
{
#if !defined(_WIN32)
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) != 0 || ru.ru_maxrss <= 0)
        return 0;
#if defined(__APPLE__)
    return (uint64_t)ru.ru_maxrss;          /* bytes */
#else
    return (uint64_t)ru.ru_maxrss * 1024u;  /* kilobytes */
#endif
#else
    return 0;
#endif
}

/*
 * ledger_allocator_bytes: fixed-capacity bytes held by a session's delta
 * pool and eval arena.  Charged to ARENA at creation and credited at
 * destruction; delta_pool_reset()/wl_arena_reset() do not change it (the
 * backing buffers are retained), which keeps the #598 rotation canary
 * ("rotation does not mutate the ledger") true.
 */
static uint64_t
ledger_allocator_bytes(const delta_pool_t *pool, const wl_arena_t *arena)
{
    uint64_t bytes = 0;
    if (pool) {
        bytes += (uint64_t)pool->slot_cap * (uint64_t)pool->slot_size;
        bytes += (uint64_t)pool->arena_cap;
    }
    if (arena)
        bytes += (uint64_t)arena->capacity;
    return bytes;
}

static void
ledger_charge_allocators(wl_col_session_t *sess)
{
    uint64_t bytes = ledger_allocator_bytes(sess->delta_pool,
            sess->eval_arena);
    if (bytes > 0)
        wl_mem_ledger_alloc(&sess->mem_ledger, WL_MEM_SUBSYS_ARENA, bytes);
}

static void
ledger_credit_allocators(wl_col_session_t *sess)
{
    uint64_t bytes = ledger_allocator_bytes(sess->delta_pool,
            sess->eval_arena);
    if (bytes > 0)
        wl_mem_ledger_free(&sess->mem_ledger, WL_MEM_SUBSYS_ARENA, bytes);
}

void
col_session_mem_sample(wl_col_session_t *sess)
{
    if (!sess)
        return;

    /* STORED: every session-owned relation (EDB, IDB, worker partitions).
     * These never carry a col_rel_t.mem_ledger, so this is the only place
     * their footprint is measured.  Arena-owned columns contribute 0 by
     * construction (col_rel_owned_ledger_bytes). */
    uint64_t stored = 0;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        const col_rel_t *r = sess->rels[i];
        if (!r)
            continue;
        uint64_t b = col_rel_transport_bytes(r);
        stored = (b > UINT64_MAX - stored) ? UINT64_MAX : stored + b;
    }
    wl_mem_ledger_set_gauge(&sess->mem_ledger, WL_MEM_SUBSYS_STORED, stored);

    /* TEMPORARY: pool-slot relations whose column buffers spilled to the
     * heap because the pool arena was exhausted.  Slots already released
     * through col_rel_free_contents are zeroed and contribute 0.  Pool
     * temporaries that also carry a mem_ledger (join outputs) are charged
     * to RELATION as they grow and are skipped here. */
    uint64_t temporary = sess->cleanup_reserved_bytes;
    temporary = sess->delta_rollback_reserved_bytes > UINT64_MAX - temporary
        ? UINT64_MAX : temporary + sess->delta_rollback_reserved_bytes;
    temporary = sess->delta_observer_reserved_bytes > UINT64_MAX - temporary
        ? UINT64_MAX : temporary + sess->delta_observer_reserved_bytes;
    const delta_pool_t *dp = sess->delta_pool;
    if (dp && dp->slab) {
        for (uint32_t sidx = 0; sidx < dp->slot_used; sidx++) {
            const col_rel_t *pr = (const col_rel_t *)(dp->slab
                + (size_t)sidx * dp->slot_size);
            if (pr->mem_ledger)
                continue;
            uint64_t b = col_rel_transport_bytes(pr);
            temporary = (b > UINT64_MAX - temporary) ? UINT64_MAX
                : temporary + b;
        }
    }
    wl_mem_ledger_set_gauge(&sess->mem_ledger, WL_MEM_SUBSYS_TEMPORARY,
        temporary);
}

void
col_session_mem_note_worker(wl_col_session_t *coordinator,
    const wl_col_session_t *worker)
{
    if (!coordinator || !worker)
        return;
    wl_mem_ledger_snapshot_t snap;
    wl_mem_ledger_snapshot(&worker->mem_ledger, &snap);
    coordinator->mem_worker_reports++;
    if (snap.peak_bytes > coordinator->mem_worker_peak_max)
        coordinator->mem_worker_peak_max = snap.peak_bytes;
    coordinator->mem_worker_peak_sum
        = (snap.peak_bytes > UINT64_MAX - coordinator->mem_worker_peak_sum)
        ? UINT64_MAX
        : coordinator->mem_worker_peak_sum + snap.peak_bytes;
}

void
col_session_get_mem_stats(wl_session_t *session, wl_columnar_mem_stats_t *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof(*out));
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    wl_mem_ledger_snapshot_t snap;
    wl_mem_ledger_snapshot(&sess->mem_ledger, &snap);
    out->budget_bytes = snap.total_budget;
    out->current_bytes = snap.current_bytes;
    out->peak_bytes = snap.peak_bytes;
    for (int i = 0; i < WL_MEM_SUBSYS_COUNT && i < 8; i++) {
        out->subsys_current_bytes[i] = snap.subsys_bytes[i];
        out->subsys_peak_bytes[i] = snap.subsys_peak[i];
    }
    out->rss_peak_bytes = col_session_peak_rss_bytes();
    out->worker_reports = sess->mem_worker_reports;
    out->worker_peak_max_bytes = sess->mem_worker_peak_max;
    out->worker_peak_sum_bytes = sess->mem_worker_peak_sum;
}

const char *
wl_columnar_mem_subsys_name(int subsys, int *out_count)
{
    if (out_count)
        *out_count = WL_MEM_SUBSYS_COUNT;
    if (subsys < 0 || subsys >= WL_MEM_SUBSYS_COUNT)
        return NULL;
    return wl_mem_subsys_names[subsys];
}

/*
 * mem_report_bytes: human-readable byte formatting for the coordinator
 * summary lines (binary units, same convention as wl_mem_ledger_report).
 */
static const char *
mem_report_bytes(uint64_t bytes, char *buf, size_t len)
{
    if (bytes >= (uint64_t)1024 * 1024 * 1024)
        snprintf(buf, len, "%.1fGB",
            (double)bytes / ((double)1024 * 1024 * 1024));
    else if (bytes >= (uint64_t)1024 * 1024)
        snprintf(buf, len, "%.1fMB", (double)bytes / ((double)1024 * 1024));
    else if (bytes >= 1024)
        snprintf(buf, len, "%.1fKB", (double)bytes / 1024.0);
    else
        snprintf(buf, len, "%lluB", (unsigned long long)bytes);
    return buf;
}

/*
 * col_compute_worker_cap: RAM-aware worker cap formula (Issue #409).
 * See declaration in internal.h for full rationale and examples.
 */
uint32_t
col_compute_worker_cap(uint64_t ram_bytes)
{
    if (ram_bytes == 0)
        return 16u;
    uint64_t ram_linear = ram_bytes / (40ULL * 1024 * 1024);
    uint64_t ram_sqrt = (uint64_t)sqrt((double)ram_bytes / 8.0);
    uint64_t dyn = ram_linear < ram_sqrt ? ram_linear : ram_sqrt;
    if (dyn < 16u)
        dyn = 16u;
    if (dyn > 4096u)
        dyn = 4096u;
    return (uint32_t)dyn;
}

/*
 * col_session_create: Initialize a columnar backend session
 *
 * Implements wl_compute_backend_t.session_create vtable slot.
 *
 * @param plan:        Execution plan (borrowed, must outlive session)
 * @param num_workers: Thread pool size cap for parallel K-fusion/TDD.
 *                    Workers are allocated lazily for the active width of a
 *                    given stratum/operator.
 * @param out:         (out) Receives &sess->base on success
 *
 * Memory initialization order:
 *   1. Allocate wl_col_session_t (zero-initialized via calloc)
 *   2. Set sess->plan = plan (borrowed reference)
 *   3. Allocate rels[] with initial capacity 16
 *   4. Pre-register EDB relations from plan->edb_relations (ncols lazy-inited)
 *   5. Set *out = &sess->base  (session.c:38 then sets base.backend)
 *
 * @return 0 on success, EINVAL if plan/out is NULL, ENOMEM on alloc failure
 *         or governor denial, EOVERFLOW when admitting the intern table would
 *         overflow the governor total (#1431)
 *
 * @see wl_session_create in session.c for vtable dispatch context
 * @see wl_col_session_t memory layout documentation above
 */
static int
col_session_create_internal(const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    wl_columnar_memory_governor_ref_t *memory_governor;
    const wl_columnar_memory_governor_t *governor;
    uint64_t join_output_limit = 0;
    bool intern_attached_here = false;
    int relation_create_rc = ENOMEM;

    if (!plan || !out)
        return EINVAL;
    *out = NULL;
    {
        const char *env = getenv("WIRELOG_JOIN_OUTPUT_LIMIT");
        if (env && env[0] != '\0') {
            uint64_t value = 0;
            for (const unsigned char *p = (const unsigned char *)env;
                *p != '\0'; p++) {
                if (*p < '0' || *p > '9'
                    || value > (UINT64_MAX - (*p - '0')) / 10)
                    return EINVAL;
                value = value * 10 + (*p - '0');
            }
            join_output_limit = value;
        }
        if (join_output_limit > UINT32_MAX)
            join_output_limit = UINT32_MAX;
    }
    if (options && options->memory_governor) {
        /* #1473: an injected governor replaces environment and host
         * resolution.  The session holds its own reference; the caller's
         * reference stays with the caller. */
        memory_governor = options->memory_governor;
        wl_columnar_memory_governor_ref_retain(memory_governor);
    } else {
        wl_columnar_memory_resolution_t memory_resolution;
        wl_columnar_memory_sources_t memory_sources;
        const char *memory_budget = getenv("WIRELOG_MEMORY_BUDGET");

        if (wl_columnar_memory_probe_sources(&memory_sources) != 0)
            return EINVAL;
        if (options && options->windows_job_handle)
            (void)wl_columnar_memory_probe_windows_job(
                options->windows_job_handle, &memory_sources);
        if (wl_columnar_memory_resolve(memory_budget, &memory_sources,
            &memory_resolution)
            != WL_COLUMNAR_MEMORY_OK)
            return EINVAL;
        memory_governor = wl_columnar_memory_governor_ref_create(
            &memory_resolution);
        if (!memory_governor)
            return ENOMEM;
    }
    governor = wl_columnar_memory_governor_ref_get(memory_governor);

    wl_col_session_t *sess
        = (wl_col_session_t *)calloc(1, sizeof(wl_col_session_t));
    if (!sess) {
        wl_columnar_memory_governor_ref_release(memory_governor);
        return ENOMEM;
    }
    int receipt_generation_rc
        = wl_columnar_eval_stack_cleanup_receipt_session_generation_new(
            &sess->cleanup_receipt_session_generation);
    if (receipt_generation_rc) {
        free(sess);
        wl_columnar_memory_governor_ref_release(memory_governor);
        return receipt_generation_rc;
    }
    sess->cleanup_receipt_next_id = 1;
    sess->cleanup_receipt_next_plan_epoch = 1;
    sess->memory_governor = memory_governor;
    wl_columnar_memory_reservation_init(&sess->rel_hash_reservation);
    wl_columnar_memory_reservation_init(&sess->rels_reservation);
    wl_columnar_memory_reservation_init(
        &sess->delta_observer_retirement_metadata);
    wl_columnar_memory_reservation_init(&sess->delta_queue_ring_reservation);
    wl_columnar_memory_reservation_init(
        &sess->diff_arr_registry_reservation);

    sess->frontier_ops = &col_frontier_epoch_ops;

    /* Issue #600: select rotation strategy via WIRELOG_ROTATION env var.
     * Default is STANDARD; "pinned" selects the pin-aware placeholder.
     * Run init() if the chosen vtable provides one; on failure, treat as
     * ENOMEM and fall through the oom path (init must not have allocated
     * anything else, otherwise it must clean up before returning).
     *
     * #630: the legacy "mvcc" value is no longer accepted -- the
     * strategy was renamed to "pinned" because wirelog is single-mutator
     * and the pin-aware reclamation mechanism (RCU/EBR territory) is
     * the correct mental model.  An "mvcc" env value emits a one-shot
     * stderr migration message and falls through to STANDARD so a
     * stale operator config surfaces loudly rather than silently
     * selecting the placeholder. */
    {
        const col_rotation_ops_t *chosen = &col_rotation_standard_ops;
        const char *rot_env = getenv("WIRELOG_ROTATION");
        const char *strategy_name = "standard";
        if (rot_env && strcmp(rot_env, "pinned") == 0) {
            chosen = &col_rotation_pinned_ops;
            strategy_name = "pinned";
        } else if (rot_env && strcmp(rot_env, "mvcc") == 0) {
            static bool mvcc_migrated = false;
            if (!mvcc_migrated) {
                mvcc_migrated = true;
                fprintf(stderr,
                    "[wirelog] WIRELOG_ROTATION=mvcc was renamed to "
                    "pinned in #630; treating as standard. Set "
                    "WIRELOG_ROTATION=pinned to select the pin-aware "
                    "placeholder.\n");
            }
        }
        sess->rotation_ops = chosen;
        WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
            "event=rotation_ops_select strategy=%s", strategy_name);
        if (chosen->init) {
            int rc = chosen->init(sess);
            if (rc != 0) {
                wl_columnar_memory_governor_ref_release(
                    sess->memory_governor);
                free(sess);
                return ENOMEM;
            }
        }
    }

    sess->plan = plan;
    sess->intern = plan->intern;
    if (sess->intern) {
        int intern_rc = wl_intern_attach_memory_governor(
            sess->intern, sess->memory_governor);
        /* A program-owned table may already be attached to the governor of
         * an earlier session.  While a live session, worker or result still
         * holds that governor the attach reports EBUSY and this session
         * continues with its own governor for session-owned storage; once
         * the table is the only holder, the attach rebinds it here
         * transactionally (Issue #1469) and a denied rebind fails session
         * creation like a denied first attach.  A later failure of this
         * create detaches the table again (the orphaned owner is gone by
         * then), and the next session re-attaches it from a scan. */
        intern_attached_here = intern_rc == 0;
        if (intern_rc != 0 && intern_rc != EALREADY && intern_rc != EBUSY) {
            /* Attach failed before taking ownership, so there is nothing
             * to detach: the table is either unattached or owned by an
             * earlier session's governor. */
            wl_columnar_memory_governor_ref_release(
                sess->memory_governor);
            free(sess);
            return intern_rc;
        }
    }
    sess->num_workers = num_workers > 0 ? num_workers : 1;
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO, "session created num_workers=%u",
        sess->num_workers);

    /*
     * WL_MAX_WORKERS: Per-worker and W² exchange buffer cost cap
     *
     * Rationale:
     * - K-fusion k is bounded by max IDB body atoms per recursive rule.
     *   For all known workloads (DOOP k=8-9, CSPA k=5), max k < 16.
     * - TDD distributed eval uses W×W exchange buffer matrix: W² × 8B
     *   allocated by coordinator (Issue #318). Total infra cost per W:
     *   Per-worker linear: 20MB (8MB arena + 4MB pool + 8MB thread stack
     *   + arrangement clones).
     *   Exchange matrix overhead: 8B × W × W (partition buffers sent from
     *   each worker to each other worker).
     *
     * Total memory for W workers: W × 20MB + W² × 8B
     * Examples:
     *   W=16:  320MB + 2KB ≈ 320MB
     *   W=512: 10.2GB + 2MB ≈ 10.2GB
     *   W=1024: 20.4GB + 8MB ≈ 20.4GB
     *
     * Environment variable WIRELOG_MAX_WORKERS allows override [1, 512].
     * Default 16 covers all known workloads with safety margin.
     * Set WL_MEM_REPORT=1 for clamping diagnostics.
     */
#define WL_MAX_WORKERS_HARD_LIMIT 4096u
    {
        /* Dynamic cap: reject num_workers exceeding the RAM-based safe limit
         * (Issue #409). WIRELOG_MAX_WORKERS env override is accepted up to the
         * hard limit with a warning; without override, requests above the
         * RAM-based cap are rejected so callers get a clear error (not silent
         * truncation). See col_compute_worker_cap() for the formula. */
        uint32_t ram_cap = col_compute_worker_cap(col_detect_physical_memory());

        bool has_env_override = false;
        uint32_t env_cap = 0;
        const char *env = getenv("WIRELOG_MAX_WORKERS");
        if (env && env[0] != '\0') {
            char *endp = NULL;
            unsigned long val = strtoul(env, &endp, 10);
            if (endp != env && *endp == '\0' && val > 0) {
                has_env_override = true;
                env_cap = (val <= WL_MAX_WORKERS_HARD_LIMIT)
                    ? (uint32_t)val
                    : WL_MAX_WORKERS_HARD_LIMIT;
                if (env_cap > ram_cap)
                    fprintf(stderr,
                        "[wirelog] WIRELOG_MAX_WORKERS=%u overrides "
                        "RAM-based cap %u; ensure sufficient memory\n",
                        env_cap, ram_cap);
            }
        }

        uint32_t effective_cap = has_env_override ? env_cap : ram_cap;

        if (sess->num_workers > effective_cap) {
            if (!has_env_override) {
                fprintf(stderr,
                    "[wirelog] num_workers %u exceeds RAM-based cap %u "
                    "(set WIRELOG_MAX_WORKERS to override, max %u)\n",
                    sess->num_workers, effective_cap,
                    WL_MAX_WORKERS_HARD_LIMIT);
                if (intern_attached_here)
                    (void)wl_intern_detach_memory_governor(
                        sess->intern, sess->memory_governor);
                wl_columnar_memory_governor_ref_release(
                    sess->memory_governor);
                free(sess);
                return EINVAL;
            }
            /* Env override active: clamp to the explicit cap */
            if (getenv("WL_MEM_REPORT"))
                fprintf(stderr,
                    "[wirelog] clamping num_workers %u -> %u\n",
                    sess->num_workers, effective_cap);
            sess->num_workers = effective_cap;
        }
    }
#undef WL_MAX_WORKERS_HARD_LIMIT

    /* The callback policy uses the effective cap after RAM/env clamping. */
    sess->callback_configured_workers = sess->num_workers;
    sess->callback_active_workers = 1;
    sess->callback_parallel_execution = false;
    sess->callback_session_key = sess;

    /* Zero leaves the legacy row cap disabled; memory admission governs
     * allocations independently. A configured positive cap remains optional. */
    sess->join_output_limit = join_output_limit;
    /* Bounded keyed-join sub-batches (Issue #1446): strict decimals like
     * the siblings above; anything else leaves the mode off. */
    {
        const char *batch_env = getenv("WIRELOG_JOIN_BATCH_BYTES");
        const char *strict_env = getenv("WIRELOG_JOIN_BATCH_STRICT");
        sess->join_batch_bytes = 0;
        sess->join_batch_strict = false;
        if (batch_env && batch_env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            uint64_t val = strtoull(batch_env, &endp, 10);
            if (endp != batch_env && *endp == '\0' && errno != ERANGE)
                sess->join_batch_bytes = val;
        }
        if (sess->join_batch_bytes > 0 && strict_env
            && strict_env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            uint64_t val = strtoull(strict_env, &endp, 10);
            if (endp != strict_env && *endp == '\0' && errno != ERANGE)
                sess->join_batch_strict = val != 0;
        }
    }

    sess->rel_cap = 16;
    sess->pending_input_change = true;
    int rels_admit_rc = session_rels_admit(sess,
            (uint64_t)sess->rel_cap * sizeof(col_rel_t *),
            &sess->rels_reservation);
    if (rels_admit_rc != 0) {
        if (intern_attached_here)
            (void)wl_intern_detach_memory_governor(
                sess->intern, sess->memory_governor);
        wl_columnar_memory_governor_ref_release(sess->memory_governor);
        free(sess);
        return rels_admit_rc == ENOSPC ? WL_ERR_MEMORY_BUDGET
            : rels_admit_rc;
    }
    sess->rels = (col_rel_t **)calloc(sess->rel_cap, sizeof(col_rel_t *));
    if (!sess->rels) {
        session_rels_release(&sess->rels_reservation);
        if (intern_attached_here)
            (void)wl_intern_detach_memory_governor(
                sess->intern, sess->memory_governor);
        wl_columnar_memory_governor_ref_release(sess->memory_governor);
        free(sess);
        return ENOMEM;
    }

    /* Worker resources are allocated lazily after an operator/stratum chooses
     * its active width. This preserves workers=N as a cap rather than forcing
     * N threads and N TDD worker slots at session creation. */

    /* Create delta pool for per-iteration temporaries.
     * Slab: 256 relations (cover ~20 rules x 5 ops + headroom)
     * Arena: 64MB initial (for row data buffers) */
    wl_columnar_memory_admission_status_t pool_status =
        WL_COLUMNAR_MEMORY_ADMISSION_OK;
    sess->delta_pool
        = delta_pool_create_managed_status(256, sizeof(col_rel_t),
            64UL * 1024 * 1024,
            wl_columnar_memory_governor_ref_get(sess->memory_governor),
            &pool_status);
    if (!sess->delta_pool) {
        if (pool_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            sess->memory_budget_denied = true;
        /* Non-fatal: pool allocation failed, fall back to malloc */
    }

    /* Create per-iteration arena for operator data buffers.
     * 64MB capacity covers typical operator output allocations
     * (COL_REL_INIT_CAP * ncols * sizeof(int64_t) per operator).
     * Reset at each iteration boundary alongside delta_pool_reset.
     * NULL arena is handled gracefully: operators fall back to malloc. */
    sess->eval_arena = wl_arena_create_managed(64UL * 1024 * 1024,
            wl_columnar_memory_governor_ref_get(sess->memory_governor));
    /* Non-fatal if NULL: col_rel_pool_new_auto falls back to malloc */

    /* The ledger remains attribution-only. The shared governor owns admission
     * policy; workers must not receive divided ledger budgets. */
    wl_mem_ledger_init(&sess->mem_ledger,
        governor->mode == WL_COLUMNAR_MEMORY_MODE_ENFORCING
            ? governor->budget_bytes : 0);

    /* Issue #1380: memory instrumentation wiring.  The delta pool and eval
     * arena above were created before the ledger existed, so charge their
     * fixed capacity now; the materialization cache re-parents cached join
     * results onto this ledger from here on. */
    sess->mem_report_level = col_session_mem_report_level();
    sess->mat_cache.ledger = &sess->mem_ledger;
    (void)col_mat_cache_attach_reclaimer(&sess->mat_cache);
    ledger_charge_allocators(sess);
    relation_create_rc = session_diff_arr_registry_init(sess);
    if (relation_create_rc != 0)
        goto oom;
    relation_create_rc = ENOMEM;

    /* Issue #264: Initialize differential path master switch.
     * Default: enabled (true). Users can disable via WIRELOG_DIFF_ENABLED=0
     * to force epoch-based evaluation regardless of affected_strata. */
    {
        const char *diff_env = getenv("WIRELOG_DIFF_ENABLED");
        if (diff_env && diff_env[0] == '0' && diff_env[1] == '\0')
            sess->diff_enabled = false;
        else
            sess->diff_enabled = true;
    }

    /* Issue #277: Cache debug/log env vars at session init to avoid repeated
     * getenv() calls in hot paths. Retained as dead-until-retired session
     * fields until the legacy flags are formally removed in a follow-up
     * issue; the active flag surface is now wl_log_thresholds[], seeded by
     * wl_log_init() (see Issue #287 presence-check shim). */
    sess->debug_join = (getenv("WL_DEBUG_JOIN") != NULL);
    sess->consolidation_log = (getenv("WL_CONSOLIDATION_LOG") != NULL);

    /* Issue #287: initialize the structured logger so WL_LOG() in migrated
     * call sites honors WL_LOG / WL_LOG_FILE / legacy presence flags.
     * Idempotent; cheap on re-entry. */
    wl_log_init();

    /* Issue #286: Configurable cache eviction threshold via environment variable.
     * Default: 90% of COL_MAT_CACHE_LIMIT_BYTES (cache evicts when exceeding
     * this threshold). Users can override via WL_MAT_CACHE_EVICT_THRESHOLD_PERCENT
     * environment variable. */
    const char *threshold_env = getenv("WL_MAT_CACHE_EVICT_THRESHOLD_PERCENT");
    int threshold_percent = 90; /* default */
    if (threshold_env) {
        char *endptr;
        long val = strtol(threshold_env, &endptr, 10);
        if (*endptr == '\0' && val >= 1 && val <= 100)
            threshold_percent = (int)val;
        /* Non-numeric or out-of-range: keep default 90% */
    }
    sess->cache_evict_threshold
        = (COL_MAT_CACHE_LIMIT_BYTES * (uint64_t)threshold_percent) / 100;

    /* Issue #216: Initialize arrangement cache LRU tracking.
     * arr_clock starts at 1 so unaccessed entries (lru_clock=0) sort last.
     * arr_cache_limit_bytes defaults to COL_ARR_CACHE_LIMIT_BYTES (256 MB)
     * and can be overridden via WL_ARR_CACHE_LIMIT_BYTES env var. */
    sess->arr_clock = 1;
    sess->arr_total_bytes = 0;
    {
        size_t arr_limit = COL_ARR_CACHE_LIMIT_BYTES;
        const char *arr_limit_env = getenv("WL_ARR_CACHE_LIMIT_BYTES");
        if (arr_limit_env && arr_limit_env[0] != '\0') {
            char *endp = NULL;
            errno = 0;
            unsigned long long val = strtoull(arr_limit_env, &endp, 10);
            if (endp != arr_limit_env && *endp == '\0' && errno != ERANGE
                && val > 0)
                arr_limit = (size_t)val;
        }
        sess->arr_cache_limit_bytes = arr_limit;
    }

    /* Issue #559: Side-relation compound arena.  Phase 0 gateway field.
     * Allocated after the simpler malloc/calloc paths so the dedicated
     * `oom:` label is the only cleanup site that has to free it.
     * session_seed=0x53455353u ('SESS') is a placeholder; the remap-aware
     * seed will be plumbed in when handle rotation lands (#586+).
     * default_gen_cap=4096 mirrors the smoke-test usage in
     * tests/test_compound_arena.c; max_epochs=0 selects the library
     * default (WL_COMPOUND_EPOCH_MAX + 1). */
    uint32_t compound_max_epochs = session_compound_max_epochs_from_env();
    sess->compound_arena = wl_arena_compound_arena_create_managed_ref(
        0x53455353u, 4096u, compound_max_epochs, sess->memory_governor);
    if (!sess->compound_arena)
        goto oom;
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
        "event=compound_arena_init seed=0x%08x default_gen_cap=%u "
        "max_epochs=%u",
        0x53455353u, 4096u, sess->compound_arena->max_epochs);
    /* Issue #583: lifecycle audit trail for the compound side-relation
     * subsystem.  WL_LOG=COMPOUND:5 captures create/destroy/alloc/
     * freeze/unfreeze in one section without enabling the noisier
     * SESSION INFO surface. */
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=session_create seed=0x%08x default_gen_cap=%u "
        "max_epochs=%u",
        0x53455353u, 4096u, sess->compound_arena->max_epochs);

    /* Pre-register EDB relations (ncols determined at first insert) */
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        col_rel_t *r = NULL;
        int rc = wl_columnar_relation_alloc_governed(&r,
                plan->edb_relations[i], sess->memory_governor);
        if (rc != 0) {
            relation_create_rc = rc;
            goto oom;
        }
        /* Issue #535: propagate graph-column metadata from plan to col_rel_t.
         * Guard on non-NULL array to stay compatible with callers that have
         * not populated edb_has_graph_column (defensive; wl_plan_from_program
         * always populates it). */
        if (plan->edb_has_graph_column != NULL
            && plan->edb_has_graph_column[i]) {
            r->has_graph_column = true;
            r->graph_col_idx = plan->edb_graph_col_index[i];
        }
        /* Issue #1038: carry the declared physical width so the first insert
         * is checked against the `.decl` rather than defining it.  Guarded on
         * non-NULL for callers that build a plan by hand and never populate
         * the array; they keep the old first-producer-wins behaviour. */
        if (plan->edb_declared_width != NULL
            && plan->edb_declared_width[i] != WL_PLAN_WIDTH_UNDECLARED)
            r->declared_ncols = plan->edb_declared_width[i];
        /* Preserve declared physical lane types before the first fact is
         * inserted.  In particular, float facts are binary64 bits in the
         * existing 64-bit storage lane and must not be accepted as legacy
         * integers merely because the relation starts empty. */
        bool has_float_type = false;
        if (plan->edb_column_types && plan->edb_column_type_counts
            && plan->edb_column_types[i]
            && plan->edb_column_type_counts[i] > 0) {
            uint32_t type_count = plan->edb_column_type_counts[i];
            for (uint32_t c = 0; c < type_count; c++)
                has_float_type |= plan->edb_column_types[i][c]
                    == WIRELOG_TYPE_FLOAT;
        }
        if (has_float_type) {
            uint32_t type_count = plan->edb_column_type_counts[i];
            rc = col_rel_set_schema(r, type_count, NULL);
            if (rc == 0)
                rc = col_rel_set_column_types(r,
                        plan->edb_column_types[i], type_count);
            if (rc != 0) {
                relation_create_rc = rc == ENOMEM
                    && r->memory_budget_denial_pending ? ENOSPC : rc;
                col_rel_destroy(r);
                goto oom;
            }
        }
        rc = session_add_rel(sess, r);
        if (rc != 0) {
            relation_create_rc = rc == ENOMEM
                && r->memory_budget_denial_pending ? ENOSPC : rc;
            col_rel_destroy(r);
            goto oom;
        }
    }

    /* Issue #535: Auto-create __graph_metadata when any EDB has __graph_id.
     * The relation is empty at creation; user code populates it.
     * Duplicate guard: skip if already declared explicitly. */
    {
        bool any_graph_enabled = false;
        if (plan->edb_has_graph_column != NULL) {
            for (uint32_t i = 0; i < plan->edb_count; i++) {
                if (plan->edb_has_graph_column[i]) {
                    any_graph_enabled = true;
                    break;
                }
            }
        }
        if (any_graph_enabled
            && session_find_rel(sess, "__graph_metadata") == NULL) {
            col_rel_t *meta = NULL;
            int rc = wl_columnar_relation_alloc_governed(&meta,
                    "__graph_metadata", sess->memory_governor);
            if (rc != 0) {
                relation_create_rc = rc;
                goto oom;
            }
            static const char *const meta_cols[6] = {
                "graph_id", "tenant", "timestamp", "location", "risk",
                "description"
            };
            rc = col_rel_set_schema(meta, 6, meta_cols);
            if (rc != 0) {
                relation_create_rc = rc == ENOMEM
                    && meta->memory_budget_denial_pending ? ENOSPC : rc;
                col_rel_destroy(meta);
                goto oom;
            }
            rc = session_add_rel(sess, meta);
            if (rc != 0) {
                relation_create_rc = rc == ENOMEM
                    && meta->memory_budget_denial_pending ? ENOSPC : rc;
                col_rel_destroy(meta);
                goto oom;
            }
        }
    }

    /* Issue #105: Populate stratum_is_monotone from plan.
     * Copy monotone property from each stratum in the plan.
     * Conservative default (all false from calloc) is already set,
     * so only copy if strata exist. */
    for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA; si++) {
        sess->stratum_is_monotone[si] = plan->strata[si].is_monotone;
    }

    /* Issue #103: Initialize 2D frontier epoch tracking.
     * outer_epoch is initialized to 0 by calloc (line 4673) and incremented
     * before each EDB insertion via col_session_insert_incremental. This
     * distinguishes different insertion epochs for 2D frontier (epoch, iteration)
     * pairs to prevent incorrect skip-condition evaluation across epochs. */
    /* outer_epoch = 0; */ /* Already zeroed by calloc */

    /* Last fallible initialization: no store-owning early return follows.
     * All spill allocations use the session's existing admission governor. */
    if (options && options->spill_directory) {
        wl_columnar_spill_status_t spill_rc = wl_columnar_spill_store_open(
            options->spill_directory, sess->memory_governor,
            options->spill_quota_bytes, options->spill_max_row_bytes,
            &sess->spill_store);
        if (spill_rc != WL_COLUMNAR_SPILL_OK) {
            relation_create_rc = spill_rc == WL_COLUMNAR_SPILL_MEMORY
                ? ENOSPC : spill_rc == WL_COLUMNAR_SPILL_ALLOCATION
                ? ENOMEM : spill_rc == WL_COLUMNAR_SPILL_INVALID ? EINVAL : EIO;
            goto oom;
        }
        sess->owns_spill_store = true;
        sess->spill_join_batch_bytes = options->spill_join_batch_bytes;
        sess->spill_join_index_bytes = options->spill_join_index_bytes;
    }

    /* Issue #317: Initialize per-worker frontier progress tracker.
     * Uses MAX_STRATA so the tracker is valid without knowing the plan's
     * stratum count at init time.  Failure is non-fatal: progress tracking
     * degrades gracefully (all_converged returns false, min returns UINT32_MAX).
     * The oom label does NOT free progress.entries because wl_frontier_progress_init
     * only sets entries on success; on ENOMEM entries remains NULL so destroy is safe. */
    wl_frontier_progress_init(&sess->progress, sess->num_workers, MAX_STRATA);

    *out = &sess->base;
    return 0;

oom:
    (void)col_session_free_diff_arrangements(sess);
    (void)session_destroy_relation_array_pass(sess->rels, sess->nrels, true);
    (void)session_destroy_relation_array_pass(sess->rels, sess->nrels, false);
    free((void *)sess->rels);
    session_rels_release(&sess->rels_reservation);
    session_rel_free_hash(sess);
    wl_workqueue_destroy(sess->wq);       /* NULL-safe */
    delta_pool_destroy(sess->delta_pool); /* NULL-safe */
    wl_arena_free(sess->eval_arena);      /* NULL-safe; releases admission */
    (void)wl_compound_arena_free_checked(sess->compound_arena);
    if (intern_attached_here)
        (void)wl_intern_detach_memory_governor(
            sess->intern, sess->memory_governor);
    wl_columnar_memory_governor_ref_release(sess->memory_governor);
    free(sess);
    return relation_create_rc == ENOSPC ? WL_ERR_MEMORY_BUDGET
        : relation_create_rc;
}

/*
 * col_session_destroy: Free all resources owned by a columnar session
 *
 * Implements wl_compute_backend_t.session_destroy vtable slot.
 * NULL-safe. Frees rels[], each rels[i], and the session struct itself.
 * The plan is borrowed and NOT freed here.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 */
static void
col_session_destroy(wl_session_t *session)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->controlled_step
        && wl_columnar_session_controlled_step_drain_for_destroy(sess) != 0) {
        fputs(
            "wirelog: controlled step cleanup failed during session destroy\n",
            stderr);
        abort();
    }
    if (sess->kfusion_pending_cohort
        && wl_columnar_kfusion_retry_pending(sess) != 0) {
        fputs(
            "wirelog: K-Fusion cohort cleanup refused during session destroy\n",
            stderr);
        abort();
    }
    wl_columnar_delta_events_clear(sess);

    /* Issue #1380: emit the memory baseline before teardown when explicitly
     * requested (WL_MEM_REPORT=1 or 2).  The ledger is session-owned, so
     * this is the last point at which current and peak values for every
     * subsystem are available.  Keep the report opt-in so normal runs and
     * their stderr remain unchanged. */
    if (sess->mem_report_level > 0) {
        char b1[32], b2[32], b3[32];
        col_session_mem_sample(sess);
        fprintf(stderr,
            "[wirelog mem] scope=coordinator workers=%u rss_peak=%s\n",
            sess->num_workers,
            mem_report_bytes(col_session_peak_rss_bytes(), b1, sizeof(b1)));
        wl_mem_ledger_report(&sess->mem_ledger);
        fprintf(stderr,
            "[wirelog mem] tdd_workers reports=%llu peak_max=%s peak_sum=%s\n",
            (unsigned long long)sess->mem_worker_reports,
            mem_report_bytes(sess->mem_worker_peak_max, b2, sizeof(b2)),
            mem_report_bytes(sess->mem_worker_peak_sum, b3, sizeof(b3)));
    }

    /* Issue #959: report the join-output high-water mark before teardown.
     *
     * The open question is whether dividing the row cap by W is right --
     * col_session_worker_init() gives each worker join_output_limit / W,
     * which assumes a worker's largest intermediate is 1/W of the
     * single-threaded one.  Workers are separate sessions, so each prints
     * its own line and the ratio is readable directly:
     *
     *     WL_LOG=SESSION:4 ... 2>&1 | grep join_output_peak
     *
     * A worker peak near single_threaded_peak / W means the division is
     * sound for that workload; a much larger one is the reproduction #959
     * has been missing, and also sizes the fix.  DEBUG rather than INFO so
     * it does not appear in ordinary runs.
     *
     * Known gap, stated rather than left to be discovered: only the
     * coordinator reaches here.  Worker sessions are bitwise copies torn
     * down on their own path and never call col_session_destroy, so at W=8
     * this still prints exactly one line.  What it gives today is the
     * single-threaded reference number -- the denominator of the ratio --
     * and confirmation that it does not change with W.  Capturing the
     * numerator needs the same emit on the worker teardown path, which is
     * the next step for #959 and is deliberately not guessed at here. */
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_DEBUG,
        "join_output_peak rows=%llu limit=%llu workers=%u",
        (unsigned long long)sess->join_output_peak,
        (unsigned long long)sess->join_output_limit,
        sess->num_workers);

    /* A failed owner-mode dispatch retains worker callback contexts in its
     * lifetime record. Quiesce them before the rotation hook can release any
     * backing state they may still reference. */
    if (sess->tdd_owner_lifetime) {
        int quiesce_rc
            = wl_columnar_eval_tdd_owner_lifetime_quiesce(sess);
        if (quiesce_rc != 0) {
            fputs("wirelog: owner TDD delta dispatch did not quiesce before "
                "rotation teardown\n", stderr);
            abort();
        }
    }

    /* Issue #600: tear down rotation strategy first so the destroy hook
     * still sees a fully-populated session (eval_arena, compound_arena,
     * etc.) before any of the other teardown frees them. NULL-safe. */
    if (sess->rotation_ops && sess->rotation_ops->destroy)
        sess->rotation_ops->destroy(sess);
    /* Admission is closed and all backend operations are quiescent before
     * this vtable entry is called.  Finish any submit-only or in-flight batch
     * while its worker/view/relation/allocator contexts are still alive. */
    if (sess->wq) {
#ifdef WL_SESSION_TEST_HOOKS
        wl_session_testhook_before_workqueue_drain(&sess->base);
#endif
        (void)wl_workqueue_drain(sess->wq);
    }
    if (sess->tdd_owner_lifetime) {
        int quiesce_rc
            = wl_columnar_eval_tdd_owner_lifetime_quiesce(sess);
        if (quiesce_rc != 0) {
            fputs("wirelog: owner TDD delta dispatch did not quiesce during "
                "session destroy\n", stderr);
            abort();
        }
        int lifetime_rc = wl_columnar_eval_tdd_owner_lifetime_retry(sess);
        if (lifetime_rc != 0) {
            fputs("wirelog: owner TDD delta cleanup failed during session "
                "destroy\n", stderr);
            abort();
        }
    }
    wl_columnar_eval_tdd_dispose_queue_ring(sess);
    /* Worker views may borrow coordinator relation storage.  Drain workers
     * before coordinator relations so their owner borrows are released before
     * the coordinator root is destroyed. */
    if (sess->tdd_workers) {
        for (uint32_t w = sess->tdd_workers_count; w > 0; w--) {
            int destroy_rc = col_worker_session_destroy(
                &sess->tdd_workers[w - 1]);
            if (destroy_rc != 0) {
                fprintf(stderr,
                    "wirelog: worker teardown failed during session "
                    "destroy (worker=%u, rc=%d)\n", w - 1, destroy_rc);
                abort();
            }
        }
        free(sess->tdd_workers);
        sess->tdd_workers = NULL;
        sess->tdd_workers_count = 0;
    }
    /* Public destruction has already closed admission and drained workers.
     * Refused internal readers violate the same synchronous teardown invariant
     * as refused relation destruction below; never free their allocators. */
    int build_rc;
    do {
        build_rc = wl_columnar_eval_delta_observer_build_discard(sess);
    } while (build_rc == EAGAIN);
    if (build_rc != 0) {
        fputs("wirelog: observer construction cleanup failed\n", stderr);
        abort();
    }
    do {
        build_rc = wl_columnar_eval_delta_observer_prepare_discard(sess);
    } while (build_rc == EAGAIN);
    if (build_rc != 0) {
        fputs("wirelog: observer preparation cleanup failed\n", stderr);
        abort();
    }
    int cleanup_rc = (sess->tdd_input_run || sess->cleanup_active
        || wl_columnar_eval_delta_rollback_active(sess)) ? EBUSY
        : wl_columnar_eval_stack_cleanup_retry(sess);
    if (cleanup_rc == 0)
        cleanup_rc = wl_columnar_eval_delta_rollback_discard(sess);
    if (cleanup_rc == 0)
        cleanup_rc = wl_columnar_eval_delta_observer_discard(sess);
    if (cleanup_rc != 0) {
        fputs("wirelog: evaluator cleanup failed during session destroy\n",
            stderr);
        abort();
    }
    memset(sess->cleanup_receipts, 0, sizeof(sess->cleanup_receipts));
    /* Issue #1435: differential arrangements hold registry-entry leases and
     * row links into relation positions.  Reject teardown while any lease or
     * replacement transaction is still active, before any relation storage is
     * destroyed. */
    assert(sess->diff_txn_count == 0);
    int diff_rc = col_session_free_diff_arrangements(sess);
    assert(diff_rc == 0);
    if (diff_rc != 0) {
        WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_ERROR,
            "cannot tear down differential arrangements: %d", diff_rc);
        return;
    }
    int lease_rc = wl_columnar_session_source_leases_release_all(sess);
    int deferred_rc = wl_columnar_session_retry_deferred(sess);
    int retained_eval_rc
        = wl_columnar_session_retry_retained_eval_entries(sess);
#ifdef WL_SESSION_TEST_HOOKS
    wl_session_testhook_after_worker_lease_release(&sess->base);
#endif
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_cold_owner == sess) {
        if (col_rel_destroy_checked(
                wl_columnar_session_test_cold_source)) abort();
        wl_columnar_session_test_cold_source = NULL;
        wl_columnar_session_test_cold_owner = NULL;
    }
#endif
    int relation_alias_rc = session_destroy_relation_array_pass(sess->rels,
            sess->nrels, true);
    int pool_alias_rc = col_rel_pool_destroy_aliases_checked(
        sess->delta_pool, 0,
        sess->delta_pool ? sess->delta_pool->slot_used : 0);
    int relation_root_rc = session_destroy_relation_array_pass(sess->rels,
            sess->nrels, false);
    int pool_root_rc = col_rel_pool_destroy_roots_checked(sess->delta_pool,
            0, sess->delta_pool ? sess->delta_pool->slot_used : 0);
    if (lease_rc != 0 || deferred_rc != 0 || retained_eval_rc != 0
        || relation_alias_rc != 0
        || pool_alias_rc != 0 || relation_root_rc != 0
        || pool_root_rc != 0) {
        fputs("wirelog: relation teardown failed during session destroy\n",
            stderr);
        abort();
    }
    free((void *)sess->rels);
    session_rels_release(&sess->rels_reservation);
    /* Free relation name hash table (Issue #281) */
    session_rel_free_hash(sess);
    col_mat_cache_detach_reclaimer(&sess->mat_cache);
    col_mat_cache_release_pins(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);
    col_mat_cache_clear(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);
    wl_workqueue_destroy(sess->wq);
    wl_kfusion_adaptive_destroy(sess->kfusion_adaptive);
    /* Free arrangement registry (Phase 3C) */
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        free(sess->arr_entries[i].rel_name);
        free(sess->arr_entries[i].key_cols);
        arr_free_contents(&sess->arr_entries[i].arr);
        col_arr_detach_memory_governor(&sess->arr_entries[i].arr);
    }
    free(sess->arr_entries);
    col_session_free_delta_arrangements(sess);
    assert(sess->sarr_active_pins == 0);
    col_session_free_sorted_arrangements(sess);
    col_session_free_filt_arrangements(sess);
    ledger_credit_allocators(sess); /* Issue #1380 */
    delta_pool_destroy(sess->delta_pool);
    wl_arena_free(sess->eval_arena);
    /* Free exchange buffer matrix (Issue #316): coordinator-owned W x W grid */
    if (sess->exchange_bufs) {
        for (uint32_t src = 0; src < sess->exchange_num_workers; src++) {
            if (sess->exchange_bufs[src]) {
                for (uint32_t dst = 0; dst < sess->exchange_num_workers; dst++)
                    col_rel_destroy(sess->exchange_bufs[src][dst]);
                free((void *)sess->exchange_bufs[src]);
            }
        }
        free((void *)sess->exchange_bufs);
    }
    /* Issue #317: Free per-worker frontier progress tracker.
     * Worker sessions have progress.entries == NULL (set in col_worker_session_create)
     * so this is safe to call on both coordinator and worker sessions. */
    wl_frontier_progress_destroy(&sess->progress);
    /* Issue #386: Free filtered relation cache */
    assert(sess->filt_cache_active_pins == 0);
    for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
        free(sess->filt_cache[i].rel_name);
        free(sess->filt_cache[i].filter_data);
        if (sess->filt_cache[i].metadata_reservation) {
            (void)wl_columnar_memory_rollback(
                sess->filt_cache[i].metadata_reservation);
            free(sess->filt_cache[i].metadata_reservation);
        }
        if (sess->filt_cache[i].filtered)
            col_rel_destroy(sess->filt_cache[i].filtered);
    }
    free(sess->filt_cache);
    if (sess->filt_cache_array_reservation) {
        (void)wl_columnar_memory_rollback(
            sess->filt_cache_array_reservation);
        free(sess->filt_cache_array_reservation);
        sess->filt_cache_array_reservation = NULL;
    }
    /* Issue #559: free side-relation compound arena (NULL-safe). */
    WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_INFO,
        "event=compound_arena_destroy live_handles=%llu",
        sess->compound_arena
            ? (unsigned long long)sess->compound_arena->live_handles
            : 0ULL);
    /* Issue #583: lifecycle audit trail (mirrors session_create). */
    WL_LOG(WL_LOG_SEC_COMPOUND, WL_LOG_TRACE,
        "lifecycle event=session_destroy live_handles=%llu",
        sess->compound_arena
            ? (unsigned long long)sess->compound_arena->live_handles
            : 0ULL);
    wl_arena_compound_arena_retire(sess->compound_arena);
    sess->compound_arena = NULL;
    wl_columnar_session_cold_free(sess->cold_relation);
    sess->cold_relation = NULL;
    if (sess->owns_spill_store)
        wl_columnar_spill_store_retire(sess->spill_store);
    wl_columnar_memory_governor_ref_release(sess->memory_governor);
    free(sess);
}

wl_columnar_memory_governor_ref_t *
col_session_memory_governor_ref(wl_session_t *session)
{
    wl_col_session_t *sess;

    if (!session)
        return NULL;
    sess = COL_SESSION(session);
    return sess->memory_governor;
}

static wl_columnar_memory_governor_t *
col_session_memory_governor(wl_session_t *session)
{
    wl_columnar_memory_governor_ref_t *ref
        = col_session_memory_governor_ref(session);
    return wl_columnar_memory_governor_ref_get(ref);
}

/* ======================================================================== */
/* Per-Worker Session State (Issue #315)                                    */
/* ======================================================================== */

/*
 * col_worker_session_create:
 * Create an isolated worker session from a coordinator session.
 * See internal.h for full documentation.
 */
int
col_worker_session_create(wl_col_session_t *coordinator,
    uint32_t worker_id, col_rel_t **partitions,
    uint32_t num_partitions, wl_col_session_t *out_worker)
{
    int failure_rc = ENOMEM;
    if (!coordinator || !out_worker || coordinator == out_worker
        || (!partitions && num_partitions > 0))
        return EINVAL;
    if (coordinator->base.input_sealed) return ENOTSUP;
    /* Worker storage is reusable only after a complete destroy. A refused
     * teardown leaves ownership in place and permanently closes this worker. */
    if (out_worker->teardown_started || out_worker->tdd_input_run
        || coordinator->tdd_input_run || coordinator->delta_observer_build
        || coordinator->delta_observer_prepare
        || out_worker->delta_observer_build
        || out_worker->delta_observer_prepare)
        return EBUSY;

    uint64_t receipt_session_generation = 0;
    int receipt_generation_rc
        = wl_columnar_eval_stack_cleanup_receipt_session_generation_new(
            &receipt_session_generation);
    if (receipt_generation_rc) return receipt_generation_rc;

    /* #1661: the worker registry is heap-only, and this is the one registry
     * population path that does not go through session_add_rel -- so it also
     * skips the pool->heap and arena->heap promotions that let an entry
     * outlive its backing storage.  A pool slot lives only as long as its
     * delta_pool slab, and arena columns die at the next arena reset, while
     * the worker registry must survive both.  Every partition producer today
     * heap-allocates (col_rel_partition_by_key and col_rel_merge_partitions
     * even assert it), so refusing costs nothing and makes the invariant
     * enforced at the boundary instead of conventional at the producers.
     * Rejecting here, before any field of *out_worker is written and before
     * the adopt loop runs, leaves every partition caller-owned.
     * NULL slots are holes, not storage: tdd_init_workers_global_read passes
     * a count with gaps where relations were removed. */
    for (uint32_t i = 0; i < num_partitions; i++) {
        if (partitions[i]
            && (partitions[i]->pool_owned || partitions[i]->arena_owned)) {
            WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_ERROR,
                "worker partition rejected: unpromoted storage "
                "(worker=%u, index=%u, rel=%s, pool=%d, arena=%d)",
                worker_id, i,
                partitions[i]->name ? partitions[i]->name : "(unnamed)",
                (int)partitions[i]->pool_owned,
                (int)partitions[i]->arena_owned);
            return EINVAL;
        }
    }

    /* Step 1: Bitwise copy — copies all value fields (frontiers,
     * counters, booleans, plan pointer, frontier_ops). */
    wl_columnar_session_copy_without_ledger(out_worker, coordinator);
    out_worker->cleanup_receipt_session_generation =
        receipt_session_generation;
    out_worker->cleanup_receipt_next_id = 1;
    out_worker->cleanup_receipt_next_plan_epoch = 1;
    memset(out_worker->cleanup_receipts, 0,
        sizeof(out_worker->cleanup_receipts));
    out_worker->owns_spill_store = false; /* Borrow, including failure cleanup. */
    out_worker->spill_join_selected_count = 0;
    out_worker->delta_observer_build = NULL;
    /* Independent authority exists before any failure cleanup. The worker
     * attribution budget is initialized below after its policy is resolved. */
    wl_mem_ledger_init(&out_worker->mem_ledger, 0);

    /* Step 2: Set identity fields */
    out_worker->worker_id = worker_id;
    out_worker->coordinator = coordinator;
    out_worker->teardown_started = false;
    out_worker->teardown_reported = false;
    /* A worker's bounded-join fallbacks are its own until teardown folds
     * them into the coordinator (#1382); the warned mask stays inherited
     * so a reason the coordinator already warned about is not repeated. */
    out_worker->join_batch_fallback_count = 0;
    out_worker->controlled_materialized_batch_published_count = 0;
    out_worker->join_batch_last_reason = 0;
    out_worker->memory_governor = coordinator->memory_governor;
    wl_columnar_memory_governor_ref_retain(out_worker->memory_governor);
    out_worker->extension_expr_status = 0;
    /* Recovery continuations belong only to the coordinator; workers are
     * disposable execution clones and must never inherit one. */
    out_worker->plain_step_completion_pending = false;
    out_worker->plain_step_completion_active = false;
    out_worker->plain_step_completion_step_context = false;
    out_worker->plain_step_completion_mask = UINT64_MAX;
    out_worker->plain_step_completion_stratum = 0;
    out_worker->plain_step_completion_phase =
        WL_COLUMNAR_PLAIN_STEP_COMPLETION_NONE;
    out_worker->plain_step_completion_relation = 0;
    out_worker->plain_step_completion_workers_retired = false;
    out_worker->callback_session_key = coordinator->callback_session_key;
    out_worker->delta_events = NULL;
    out_worker->delta_event_count = 0;
    out_worker->delta_event_capacity = 0;
    out_worker->delta_event_transaction = false;

    /* Prevent accidental wl_session_destroy on stack-allocated worker */
    out_worker->base.backend = NULL;
    out_worker->base.owns_extension_snapshot = false;
    out_worker->base.operation_admission = NULL;
    out_worker->base.owns_operation_admission = false;
    out_worker->base.owns_evaluation_control = false;

    /* Step 3: NULL all owned pointers (safe for cleanup on early abort) */
    out_worker->wq = NULL;
    out_worker->wq_workers = 0;
    out_worker->kfusion_adaptive = NULL;
    out_worker->eval_arena = NULL;
    out_worker->delta_pool = NULL;
    out_worker->rels = NULL;
    out_worker->nrels = 0;
    out_worker->rel_cap = 0;
    wl_columnar_memory_reservation_init(&out_worker->rels_reservation);
    out_worker->rel_hash_head = NULL;
    out_worker->rel_hash_next = NULL;
    out_worker->rel_hash_nbuckets = 0;
    out_worker->rel_hash_chain_cap = 0;
    wl_columnar_memory_reservation_init(&out_worker->rel_hash_reservation);
    out_worker->source_leases = NULL;
    out_worker->deferred_relations = NULL;
    out_worker->deferred_relation_count = 0;
    out_worker->cleanup_active = NULL;
    out_worker->tdd_input_run = NULL;
    out_worker->cleanup_pending = NULL;
    out_worker->cleanup_active_count = 0;
    out_worker->cleanup_pending_count = 0;
    out_worker->cleanup_reserved_bytes = 0;
    out_worker->delta_rollback = NULL;
    out_worker->delta_rollback_reserved_bytes = 0;
    out_worker->delta_observer = NULL;
    out_worker->delta_observer_reserved_bytes = 0;
    wl_columnar_memory_reservation_init(
        &out_worker->delta_observer_retirement_metadata);
    out_worker->delta_observer_retirement_governor = NULL;
    out_worker->delta_observer_retirement =
        WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE;
    out_worker->delta_observer_retirement_gc = false;
    out_worker->delta_publish_active = false;
    out_worker->delta_publish_cancelled = false;
    out_worker->retained_eval_entries = NULL;
    out_worker->retained_eval_entry_count = 0;
    out_worker->tdd_owner_lifetime = NULL;
    /* This struct began as a copy of the coordinator, so clear the flag
     * rather than inherit a value.  Nothing reads it before col_eval_stratum
     * writes it, so no observation depends on this -- but it is not a fold
     * point either: the worker's flag describes the worker's own stratum and
     * must never be merged upward. */
    out_worker->eval_stratum_heads_final = false;
    out_worker->arr_entries = NULL;
    out_worker->arr_count = 0;
    out_worker->arr_cap = 0;
    out_worker->arr_total_bytes = 0; /* cloned registries start without storage */
    out_worker->diff_arr_entries = NULL;
    out_worker->diff_arr_count = 0;
    out_worker->diff_arr_cap = 0;
    wl_columnar_memory_reservation_init(
        &out_worker->diff_arr_registry_reservation);
    out_worker->diff_arr_registry_reserved_bytes = 0;
    out_worker->diff_arr_registry_metadata_reserved_bytes = 0;
    out_worker->darr_entries = NULL;
    out_worker->darr_count = 0;
    out_worker->darr_cap = 0;
    out_worker->sarr_entries = NULL;
    out_worker->sarr_count = 0;
    out_worker->sarr_cap = 0;
    out_worker->sarr_active_pins = 0;
    out_worker->filt_arr_entries = NULL;
    out_worker->filt_arr_count = 0;
    out_worker->filt_arr_cap = 0;
    out_worker->filt_cache = NULL;
    out_worker->filt_cache_count = 0;
    out_worker->filt_cache_cap = 0;
    out_worker->filt_cache_array_reservation = NULL;
    out_worker->filt_cache_active_pins = 0;
    /* Issue #579 / R-5: workers BORROW the coordinator's frozen arena.
     * Worker destroy must NOT free this pointer (see
     * col_worker_session_destroy).  The K-Fusion freeze contract
     * (Issue #561) guarantees the arena is frozen for the duration
     * of worker access. */
    out_worker->compound_arena = coordinator->compound_arena;
    memset(&out_worker->compound_borrow, 0,
        sizeof(out_worker->compound_borrow));
    if (out_worker->compound_arena
        && !wl_compound_arena_borrow(out_worker->compound_arena,
        &out_worker->compound_borrow))
        goto cleanup;
    memset(&out_worker->mat_cache, 0, sizeof(col_mat_cache_t));
    /* Exchange buffers are owned by coordinator; worker inherits borrowed ptr */
    out_worker->exchange_bufs = NULL;
    out_worker->exchange_num_workers = 0;
    /* Issue #318: TDD workers array is owned by coordinator; workers have none */
    out_worker->tdd_workers = NULL;
    out_worker->tdd_workers_cap = 0;
    out_worker->tdd_workers_count = 0;
    /* Issue #317: Worker does not own the progress tracker.
     * NULL entries so col_session_destroy (if called on worker) does not
     * double-free the coordinator's entries array. */
    out_worker->progress.entries = NULL;
    out_worker->progress.num_workers = 0;
    out_worker->progress.num_strata = 0;

    /* Step 4: NULL borrowed fields that workers must not use */
    out_worker->delta_cb = NULL;
    out_worker->delta_data = NULL;
    out_worker->last_inserted_relation = NULL;
    out_worker->last_removed_relation = NULL;

    /* Step 5: Initialize an attribution-only worker ledger. The governor is
     * shared; only the legacy ledger threshold uses an equal reporting share
     * so its aggregate backpressure does not multiply by worker count. */
    uint32_t active_workers = coordinator->tdd_active_workers > 0
        ? coordinator->tdd_active_workers : coordinator->num_workers;
    if (active_workers == 0)
        active_workers = 1;
    uint64_t worker_budget = atomic_load_explicit(
        &coordinator->mem_ledger.total_budget, memory_order_relaxed);
    if (worker_budget > 0 && active_workers > 1)
        worker_budget /= active_workers;
    wl_mem_ledger_init(&out_worker->mem_ledger, worker_budget);
    /* Issue #1380: the bitwise copy carried the coordinator's cache ledger
     * link and aggregate counters; workers account against their own
     * ledger and never aggregate. */
    out_worker->mat_cache.ledger = &out_worker->mem_ledger;
    out_worker->mem_channel_ring_bytes = 0;
    wl_columnar_memory_reservation_init(
        &out_worker->delta_queue_ring_reservation);
    out_worker->mem_worker_reports = 0;
    out_worker->mem_worker_peak_max = 0;
    out_worker->mem_worker_peak_sum = 0;

    /* Issue #426: Scale join_output_limit per worker.
     * The bitwise copy above gave the worker the coordinator's full limit;
     * shrink to a fair per-worker share (0 = disabled, preserved as-is).
     * Clamp to minimum 1 so that limit < W does not accidentally disable
     * the cap (0 means unlimited). */
    if (active_workers > 0 && coordinator->join_output_limit > 0) {
        uint64_t per_worker =
            coordinator->join_output_limit / active_workers;
        out_worker->join_output_limit = per_worker > 0 ? per_worker : 1;
    }

    /* Step 6: Populate rels[] with partition relations (ownership transfer) */
    if (num_partitions > 0) {
        uint64_t bytes;
        if (!wl_columnar_memory_size_mul(num_partitions,
            sizeof(col_rel_t *), &bytes) || bytes > SIZE_MAX) {
            failure_rc = EOVERFLOW;
            goto cleanup;
        }
        failure_rc = session_rels_admit(out_worker, bytes,
                &out_worker->rels_reservation);
        if (failure_rc != 0) {
            if (failure_rc == ENOSPC)
                coordinator->memory_budget_denied = true;
            goto cleanup;
        }
        failure_rc = ENOMEM;
        out_worker->rels
            = (col_rel_t **)calloc(num_partitions, sizeof(col_rel_t *));
        if (!out_worker->rels)
            goto cleanup;
        /* Shared views are published before the worker exists.  Adopt their
         * roots while partitions[] is still caller-owned so lease allocation
         * failure leaves every partition available for caller cleanup. */
        for (uint32_t i = 0; i < num_partitions; i++) {
            if (!partitions[i])
                continue;
            int rc = wl_columnar_session_adopt_shared_view(out_worker,
                    partitions[i]);
            if (rc != 0)
                goto cleanup;
        }
        for (uint32_t i = 0; i < num_partitions; i++) {
            out_worker->rels[i] = partitions[i];
            partitions[i] = NULL; /* Mark as transferred */
        }
    }
    out_worker->nrels = num_partitions;
    out_worker->rel_cap = num_partitions;

    /* Step 7: Allocate per-worker arena (scaled by num_workers) */
    {
        uint32_t k = active_workers > 0 ? active_workers : 1;
        size_t arena_cap = coordinator->eval_arena
            ? coordinator->eval_arena->capacity / k
            : 8UL * 1024 * 1024;
        if (arena_cap < 8UL * 1024 * 1024)
            arena_cap = 8UL * 1024 * 1024;
        out_worker->eval_arena = wl_arena_create_managed(arena_cap,
                wl_columnar_memory_governor_ref_get(
                    out_worker->memory_governor));
        /* Non-fatal if NULL: operators fall back to malloc */
    }

    /* Step 8: Allocate per-worker delta_pool (scaled by num_workers) */
    {
        uint32_t k = active_workers > 0 ? active_workers : 1;
        size_t pool_arena = 32UL * 1024 * 1024 / k;
        if (pool_arena < 4UL * 1024 * 1024)
            pool_arena = 4UL * 1024 * 1024;
        uint32_t pool_slots = 128 / k;
        if (pool_slots < 16)
            pool_slots = 16;
        wl_columnar_memory_admission_status_t pool_status =
            WL_COLUMNAR_MEMORY_ADMISSION_OK;
        out_worker->delta_pool
            = delta_pool_create_managed_status(pool_slots, sizeof(col_rel_t),
                pool_arena,
                wl_columnar_memory_governor_ref_get(
                    out_worker->memory_governor), &pool_status);
        if (!out_worker->delta_pool
            && pool_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            out_worker->memory_budget_denied = true;
        /* Non-fatal if NULL: operators fall back to malloc */
    }
    ledger_charge_allocators(out_worker); /* Issue #1380: ARENA */

    /* Step 9: Deep-clone arrangement registries */
    if (coordinator->arr_count > 0) {
        int rc = col_arr_entries_clone(coordinator->arr_entries,
                coordinator->arr_count, &out_worker->arr_entries,
                &out_worker->arr_cap, out_worker->memory_governor);
        if (rc != 0)
            goto cleanup;
        out_worker->arr_count = coordinator->arr_count;
        /* Issue #1380: the clones are worker-owned copies; charge them to
         * the worker ledger (the clone path bypasses arr_build_full). */
        for (uint32_t i = 0; i < out_worker->arr_count; i++)
            col_arr_attach_ledger(&out_worker->arr_entries[i].arr,
                &out_worker->mem_ledger);
    }
    if (coordinator->diff_arr_count > 0) {
        uint64_t array_bytes;
        uint64_t metadata_bytes;
        uint64_t bytes;
        wl_columnar_memory_reservation_t pending;
        wl_columnar_memory_reservation_init(&pending);
        int rc = col_diff_arr_entries_registry_size(
            coordinator->diff_arr_entries, coordinator->diff_arr_count,
            &array_bytes, &metadata_bytes);
        if (rc != 0 || !wl_columnar_memory_size_add(array_bytes,
            metadata_bytes, &bytes)) {
            failure_rc = rc != 0 ? rc : EOVERFLOW;
            goto cleanup;
        }
        rc = session_rels_admit(out_worker, bytes, &pending);
        if (rc != 0) {
            failure_rc = rc;
            goto cleanup;
        }
        rc = col_diff_arr_entries_clone(coordinator->diff_arr_entries,
                coordinator->diff_arr_count, &out_worker->diff_arr_entries,
                &out_worker->diff_arr_cap, out_worker->memory_governor);
        if (rc != 0) {
            session_rels_release(&pending);
            if (rc == ENOSPC)
                out_worker->memory_budget_denied = true;
            failure_rc = rc;
            goto cleanup;
        }
        out_worker->diff_arr_count = coordinator->diff_arr_count;
        out_worker->diff_arr_registry_reserved_bytes = array_bytes;
        out_worker->diff_arr_registry_metadata_reserved_bytes =
            metadata_bytes;
        if (out_worker->memory_governor) {
            if (!wl_columnar_memory_reservation_move(
                    &out_worker->diff_arr_registry_reservation, &pending))
                abort();
            if (!wl_columnar_memory_commit(
                    &out_worker->diff_arr_registry_reservation, out_worker))
                abort();
        }
        wl_mem_ledger_alloc(&out_worker->mem_ledger,
            WL_MEM_SUBSYS_ARRANGEMENT, bytes);
        for (uint32_t i = 0; i < out_worker->diff_arr_count; i++)
            col_diff_arrangement_attach_ledger(
                out_worker->diff_arr_entries[i].diff_arr,
                &out_worker->mem_ledger);
    }

    return 0;

cleanup:
    {
        int destroy_rc = col_worker_session_destroy(out_worker);
        return destroy_rc != 0 ? destroy_rc : failure_rc;
    }
}

/*
 * col_worker_session_destroy:
 * Free all resources owned by a worker session.
 * See internal.h for full documentation.
 */
int
col_worker_session_destroy(wl_col_session_t *worker)
{
    if (!worker)
        return 0;
    /* Nesting is allowed during evaluation, never during destruction. */
    if (worker->tdd_input_run || worker->cleanup_active
        || wl_columnar_eval_delta_rollback_active(worker))
        return EBUSY;
    int build_rc;
    do {
        build_rc = wl_columnar_eval_delta_observer_build_discard(worker);
    } while (build_rc == EAGAIN);
    if (build_rc != 0)
        return build_rc;
    do {
        build_rc = wl_columnar_eval_delta_observer_prepare_discard(worker);
    } while (build_rc == EAGAIN);
    if (build_rc != 0)
        return build_rc;
    int cleanup_rc = wl_columnar_eval_stack_cleanup_retry(worker);
    if (cleanup_rc == 0)
        cleanup_rc = wl_columnar_eval_delta_rollback_discard(worker);
    if (cleanup_rc == 0)
        cleanup_rc = wl_columnar_eval_delta_observer_discard(worker);
    if (cleanup_rc != 0)
        return cleanup_rc;
    memset(worker->cleanup_receipts, 0, sizeof(worker->cleanup_receipts));
    worker->teardown_started = true;
    wl_columnar_delta_events_clear(worker);
    if (worker->filt_cache_active_pins != 0)
        return EBUSY;

    /* Issue #1380: worker ledgers are independent copies with their own
     * allocations and peaks.  Take a final gauge sample, fold the peak into
     * the coordinator aggregates (printed once in the coordinator report),
     * and print the full worker breakdown only at WL_MEM_REPORT=2 -- TDD
     * workers are torn down after every stratum, so per-worker lines are
     * noisy at level 1. */
    if (!worker->teardown_reported) {
        col_session_mem_sample(worker);
        col_session_mem_note_worker(worker->coordinator, worker);
        col_join_batch_fold_worker(worker->coordinator, worker);
        worker->teardown_reported = true;
    }
    if (worker->mem_report_level >= 2) {
        fprintf(stderr, "[wirelog mem] scope=worker id=%u workers=%u\n",
            worker->worker_id, worker->num_workers);
        wl_mem_ledger_report(&worker->mem_ledger);
    }

    /* Release the worker's own source leases before retiring its aliases. */
    int rc = wl_columnar_session_source_leases_release_all(worker);
    if (rc != 0) {
        fprintf(stderr,
            "wirelog: worker source-lease teardown failed "
            "(worker=%u, rc=%d)\n", worker->worker_id, rc);
        return rc;
    }
    rc = wl_columnar_session_retry_deferred(worker);
    if (rc != 0 && worker->coordinator) {
        int transfer_rc = wl_columnar_session_transfer_deferred(worker,
                worker->coordinator);
        if (transfer_rc != 0) {
            fprintf(stderr,
                "wirelog: worker deferred transfer failed: %d\n",
                transfer_rc);
            return transfer_rc;
        }
    } else if (rc != 0) {
        fprintf(stderr,
            "wirelog: worker deferred cleanup failed: %d\n", rc);
        return rc;
    }
    uint32_t pool_slot_limit = worker->delta_pool
        ? worker->delta_pool->slot_used : 0;
    int pool_geometry_rc = wl_columnar_session_validate_worker_pool_geometry(
        worker->delta_pool, pool_slot_limit);
    if (pool_geometry_rc != 0)
        return pool_geometry_rc;
    while (worker->retained_eval_entry_count != 0) {
        uint32_t retained_before = worker->retained_eval_entry_count;
        uint32_t relation_aliases_destroyed = 0;
        uint32_t pool_aliases_destroyed = 0;
        int pass_rc = 0;
        int retained_rc
            = wl_columnar_session_retry_retained_eval_entries(worker);
        if (retained_rc != 0)
            pass_rc = retained_rc;
        uint64_t progress = retained_before
            - worker->retained_eval_entry_count;

        int relation_rc
            = wl_columnar_session_destroy_worker_relation_aliases(worker,
                &relation_aliases_destroyed);
        if (pass_rc == 0 && relation_rc != 0)
            pass_rc = relation_rc;
        progress += relation_aliases_destroyed;

        int pool_rc = wl_columnar_session_destroy_worker_pool_aliases(worker,
                pool_slot_limit, &pool_aliases_destroyed);
        if (pass_rc == 0 && pool_rc != 0)
            pass_rc = pool_rc;
        progress += pool_aliases_destroyed;

        if (worker->retained_eval_entry_count == 0)
            break;
        if (progress == 0)
            return pass_rc != 0 ? pass_rc : EBUSY;
    }
    int relation_alias_rc = session_destroy_relation_array_pass(
        worker->rels, worker->nrels, true);
    int pool_alias_rc = col_rel_pool_destroy_aliases_checked(
        worker->delta_pool, 0, pool_slot_limit);
    int relation_root_rc = session_destroy_relation_array_pass(
        worker->rels, worker->nrels, false);
    int pool_root_rc = col_rel_pool_destroy_roots_checked(
        worker->delta_pool, 0, pool_slot_limit);
    if (relation_alias_rc != 0 || pool_alias_rc != 0
        || relation_root_rc != 0 || pool_root_rc != 0) {
        fprintf(stderr,
            "wirelog: worker relation teardown refused "
            "(worker=%u, aliases=%d/%d roots=%d/%d)\n",
            worker->worker_id, relation_alias_rc, pool_alias_rc,
            relation_root_rc, pool_root_rc);
        return relation_alias_rc != 0 ? relation_alias_rc
            : pool_alias_rc != 0 ? pool_alias_rc
            : relation_root_rc != 0 ? relation_root_rc : pool_root_rc;
    }

    free((void *)worker->rels);
    session_rels_release(&worker->rels_reservation);
    worker->rels = NULL;
    worker->nrels = 0;
    worker->rel_cap = 0;

    /* A filtered result can be held by an operation-scoped reader too. Keep
     * its cache entry intact when checked destruction refuses so teardown
     * can be retried after that reader releases its lease. */
    for (uint32_t i = 0; i < worker->filt_cache_count; i++) {
        if (!worker->filt_cache[i].filtered)
            continue;
        rc = col_rel_destroy_checked(worker->filt_cache[i].filtered);
        if (rc != 0)
            return rc;
        worker->filt_cache[i].filtered = NULL;
    }
    for (uint32_t i = 0; i < worker->filt_cache_count; i++) {
        free(worker->filt_cache[i].rel_name);
        worker->filt_cache[i].rel_name = NULL;
        free(worker->filt_cache[i].filter_data);
        worker->filt_cache[i].filter_data = NULL;
        if (worker->filt_cache[i].metadata_reservation) {
            (void)wl_columnar_memory_rollback(
                worker->filt_cache[i].metadata_reservation);
            free(worker->filt_cache[i].metadata_reservation);
            worker->filt_cache[i].metadata_reservation = NULL;
        }
    }
    free(worker->filt_cache);
    worker->filt_cache = NULL;
    worker->filt_cache_count = 0;
    worker->filt_cache_cap = 0;
    if (worker->filt_cache_array_reservation) {
        (void)wl_columnar_memory_rollback(
            worker->filt_cache_array_reservation);
        free(worker->filt_cache_array_reservation);
        worker->filt_cache_array_reservation = NULL;
    }

    /* Free mat_cache entries (all worker-owned since zeroed at create) */
    col_mat_cache_release_pins(&worker->mat_cache);
    assert(worker->mat_cache.active_pins == 0);
    col_mat_cache_clear(&worker->mat_cache);
    assert(worker->mat_cache.active_pins == 0);

    /* Free arrangement registries */
    for (uint32_t i = 0; i < worker->arr_count; i++) {
        free(worker->arr_entries[i].rel_name);
        free(worker->arr_entries[i].key_cols);
        arr_free_contents(&worker->arr_entries[i].arr);
        col_arr_detach_memory_governor(&worker->arr_entries[i].arr);
    }
    free(worker->arr_entries);
    assert(worker->diff_txn_count == 0);
    int diff_rc = col_session_free_diff_arrangements(worker);
    assert(diff_rc == 0);
    if (diff_rc != 0) {
        WL_LOG(WL_LOG_SEC_SESSION, WL_LOG_ERROR,
            "cannot tear down worker differential arrangements: %d", diff_rc);
        return diff_rc;
    }
    col_session_free_delta_arrangements(worker);
    assert(worker->sarr_active_pins == 0);
    col_session_free_sorted_arrangements(worker);
    col_session_free_filt_arrangements(worker);

    /* Free hash table (may have been lazily built) */
    session_rel_free_hash(worker);

    /* Free allocators (all NULL-safe) */
    wl_workqueue_destroy(worker->wq);
    ledger_credit_allocators(worker); /* Issue #1380 */
    delta_pool_destroy(worker->delta_pool);
    wl_arena_free(worker->eval_arena);

    /* compound_arena is BORROWED from coordinator (Issue #579 / R-5).
     * DO NOT call wl_compound_arena_free here — the coordinator owns it
     * and frees it in its own col_session_destroy path. */
    if (worker->compound_borrow.arena)
        (void)wl_compound_arena_borrow_release(&worker->compound_borrow);

    wl_columnar_memory_governor_ref_release(worker->memory_governor);

    /* Zero the struct to prevent dangling pointer use */
    memset(worker, 0, sizeof(*worker));
    return 0;
}

static bool
col_controlled_step_provenance_current(const wl_col_session_t *sess,
    const wl_columnar_controlled_step_t *step)
{
    if (!sess || !step || !step->provenance_valid
        || step->provenance_invalid || !step->provenance_edge
        || !step->provenance_edge->name
        || strcmp(step->provenance_edge->name, "edge") != 0
        || session_find_rel((wl_col_session_t *)sess, "edge")
        != step->provenance_edge)
        return false;
    const col_rel_t *path = session_find_rel((wl_col_session_t *)sess,
            "path");
    return path && wl_columnar_relation_snapshot_equal(
        step->provenance_edge_snapshot,
        wl_columnar_relation_snapshot(step->provenance_edge))
           && wl_columnar_relation_snapshot_equal(
        step->provenance_path_snapshot,
        wl_columnar_relation_snapshot(path));
}

static void
col_controlled_step_record_edge_append(wl_col_session_t *sess, col_rel_t *edge)
{
    wl_columnar_controlled_step_t *step = sess ? sess->controlled_step : NULL;
    /* General transactions authorize EDB mutations through their owned table;
     * the public mutation-completed callback schedules full recomputation. */
    if (step && step->general_observed) return;
    if (!step || !step->complete || !step->recursive_route
        || !step->provenance_valid || step->provenance_invalid
        || edge != step->provenance_edge
        || !edge || session_find_rel(sess, "edge") != edge
        || !wl_columnar_relation_snapshot_equal(
            step->provenance_path_snapshot,
            wl_columnar_relation_snapshot(session_find_rel(sess, "path")))) {
        if (step) step->provenance_invalid = true;
        return;
    }
    step->provenance_edge_snapshot = wl_columnar_relation_snapshot(edge);
    step->retryable = true;
}

static bool
col_session_controlled_step_authorize_insert(const wl_session_t *session,
    const char *relation, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || strcmp(relation, "edge") != 0
        || num_rows == 0 || num_cols != 2)
        return false;
    const wl_col_session_t *sess = COL_SESSION((wl_session_t *)session);
    const wl_columnar_controlled_step_t *step = sess->controlled_step;
    return step && step->complete && step->recursive_route
           && !sess->delta_cb && !sess->delta_observer
           && !sess->delta_publish_active
           && col_controlled_step_provenance_current(sess, step);
}

int
col_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;
    if (session->input_sealed) return ENOTSUP;
    if (num_rows == 0)
        return 0;
    if (COL_SESSION(session)->delta_observer
        || COL_SESSION(session)->delta_publish_active)
        return EBUSY;

    /* Issue #662: When a delta callback is installed, EDB inserts must take
     * the incremental path so arrangement caches are invalidated and the
     * outer-epoch counter advances; otherwise a subsequent col_session_step
     * with delta_cb may run differential operators against stale arrangement
     * indices and surface as -1/EINVAL.  This mirrors col_session_remove,
     * which already reroutes to col_session_remove_incremental in the same
     * condition. */
    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->plain_step_completion_pending)
        return EBUSY;
    if (sess->delta_cb != NULL) {
        int incremental_rc = col_session_insert_incremental(session, relation,
                data, num_rows, num_cols);
        if (!incremental_rc && num_rows)
            col_controlled_step_record_edge_append(sess,
                session_find_rel(sess, relation));
        return incremental_rc;
    }

    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(
        COL_SESSION(session));
    if (cleanup_rc != 0)
        return cleanup_rc;

    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;

    /* Lazy schema initialisation on first insert.  Issue #1038: the first
     * insert used to *define* the width; it is now checked against the
     * declared physical width when the program declared one, so a host
     * cannot establish a relation at a shape its own `.decl` contradicts. */
    if (r->ncols == 0 && r->declared_ncols != 0
        && num_cols != r->declared_ncols)
        return EINVAL;
    if (r->ncols != 0 && r->ncols != num_cols) {
        return EINVAL; /* column count mismatch */
    }

    /* Issue #1994: the seed shadow takes the batch first, so a failure on
     * either side leaves the two in step. */
    col_rel_t *seed = NULL;
    int rc = session_seed_shadow_for_insert(COL_SESSION(session), r, &seed);
    if (rc != 0)
        return rc;
    uint32_t seed_rows = seed ? seed->nrows : 0;
    bool denied = false;
    rc = seed ? col_rel_append_rows_atomic(seed, data, num_rows, num_cols,
            &denied) : 0;
    if (rc != 0)
        return rc == ENOMEM && denied ? ENOSPC : rc;
    rc = col_rel_append_rows_atomic(r, data, num_rows, num_cols, &denied);
    if (rc != 0) {
        session_private_rel_truncate(seed, seed_rows);
        return rc == ENOMEM && denied ? ENOSPC : rc;
    }

    session_note_inserted_input(sess, r, false);
    col_controlled_step_record_edge_append(sess, r);
    /* The non-incremental API must force a full epoch evaluation even when
     * the previous update targeted the same relation. */
    sess->pending_full_input_eval = true;

    return 0;
}

static int
col_session_create(const wl_plan_t *plan, uint32_t num_workers,
    wl_session_t **out)
{
    return col_session_create_internal(plan, num_workers, NULL, out);
}

static int
col_session_create_with_options(const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    return col_session_create_internal(plan, num_workers, options, out);
}

static bool
compound_arg_type_valid(wirelog_column_type_t type)
{
    switch (type) {
    case WIRELOG_TYPE_INT32:
    case WIRELOG_TYPE_INT64:
    case WIRELOG_TYPE_UINT32:
    case WIRELOG_TYPE_UINT64:
    case WIRELOG_TYPE_FLOAT:
    case WIRELOG_TYPE_STRING:
    case WIRELOG_TYPE_BOOL:
        return true;
    }
    return false;
}

static int
col_session_make_compound(wl_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out)
{
    if (handle_out)
        *handle_out = WIRELOG_COMPOUND_HANDLE_NULL;
    if (!session || !functor || !args || arity == 0 || !handle_out)
        return EINVAL;
    if (session->input_sealed) return ENOTSUP;
    if (arity > UINT32_MAX - 1u)
        return EOVERFLOW;
    if (arity > UINT32_MAX / (uint32_t)sizeof(int64_t))
        return EOVERFLOW;
    for (uint32_t i = 0; i < arity; i++) {
        if (!compound_arg_type_valid(args[i].type))
            return EINVAL;
    }

    wl_col_session_t *sess = COL_SESSION(session);
    if (!sess || !sess->compound_arena)
        return EINVAL;
    if (sess->plain_step_completion_pending)
        return EBUSY;
    if (sess->delta_observer || sess->delta_publish_active)
        return EBUSY;
    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(sess);
    if (cleanup_rc != 0)
        return cleanup_rc;
    if (sess->compound_arena->frozen)
        return EBUSY;
    if (sess->compound_arena->current_epoch >= sess->compound_arena->max_epochs)
        return ENOSPC;

    col_rel_t *side_rel = NULL;
    int rc = wl_compound_side_ensure(sess, functor, arity, &side_rel);
    if (rc != 0)
        return rc;
    if (!side_rel)
        return EINVAL;

    uint32_t payload_size = arity * (uint32_t)sizeof(int64_t);
    uint64_t handle = wl_compound_arena_alloc(sess->compound_arena,
            payload_size);
    if (handle == WIRELOG_COMPOUND_HANDLE_NULL) {
        if (sess->compound_arena->frozen)
            return EBUSY;
        if (sess->compound_arena->current_epoch
            >= sess->compound_arena->max_epochs)
            return ENOSPC;
        return ENOMEM;
    }

    uint32_t ncols = arity + 1u;
    int64_t stack_row[16];
    int64_t *row = stack_row;
    if (ncols > (uint32_t)(sizeof(stack_row) / sizeof(stack_row[0]))) {
        row = (int64_t *)malloc((size_t)ncols * sizeof(*row));
        if (!row) {
            wl_compound_arena_retain(sess->compound_arena, handle, -1);
            return ENOMEM;
        }
    }

    row[0] = (int64_t)handle;
    for (uint32_t i = 0; i < arity; i++)
        row[i + 1u] = args[i].value;

    rc = col_rel_append_row(side_rel, row);
    if (row != stack_row)
        free(row);
    if (rc != 0) {
        wl_compound_arena_retain(sess->compound_arena, handle, -1);
        return rc;
    }

    session_note_inserted_input(sess, side_rel, true);
    *handle_out = handle;
    return 0;
}

/*
 * col_session_insert_incremental: Append facts to a session WITHOUT resetting
 * the per-stratum frontier.
 *
 * Unlike col_session_insert(), this function preserves frontier[] state so
 * that a subsequent col_session_step() call can perform incremental
 * re-evaluation: only strata whose frontier has not yet converged past the
 * current iteration are evaluated.
 *
 * Facts are appended to the existing relation; existing rows are kept.
 * Schema is lazily initialised on the first call (same as col_session_insert).
 *
 * @param session:  Active wl_session_t created by col_session_create
 * @param relation: Name of the EDB relation to append to
 * @param data:     Row-major int64_t array, num_rows * num_cols elements
 * @param num_rows: Number of rows to append (0 is a no-op and returns 0)
 * @param num_cols: Number of columns per row
 * @return 0 on success, EINVAL on bad args, ENOENT if relation unknown,
 *         ENOMEM on allocation failure
 */
int
col_session_insert_incremental(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;
    if (session->input_sealed) return ENOTSUP;
    if (num_rows == 0)
        return 0;
    if (COL_SESSION(session)->delta_observer
        || COL_SESSION(session)->delta_publish_active)
        return EBUSY;
    if (COL_SESSION(session)->plain_step_completion_pending)
        return EBUSY;

    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(
        COL_SESSION(session));
    if (cleanup_rc != 0)
        return cleanup_rc;

    col_rel_t *r = session_find_rel(COL_SESSION(session), relation);
    if (!r)
        return ENOENT;

    /* Lazy schema initialisation on first insert.  Issue #1038: the first
     * insert used to *define* the width; it is now checked against the
     * declared physical width when the program declared one, so a host
     * cannot establish a relation at a shape its own `.decl` contradicts. */
    if (r->ncols == 0 && r->declared_ncols != 0
        && num_cols != r->declared_ncols)
        return EINVAL;
    if (r->ncols != 0 && r->ncols != num_cols) {
        return EINVAL; /* column count mismatch */
    }

    /* Append the complete batch atomically; frontier[] is intentionally NOT
     * modified by the incremental path. */
    /* Issue #1994: the seed shadow takes the batch first, so a failure on
     * either side leaves the two in step. */
    col_rel_t *seed = NULL;
    int rc = session_seed_shadow_for_insert(COL_SESSION(session), r, &seed);
    if (rc != 0)
        return rc;
    uint32_t seed_rows = seed ? seed->nrows : 0;
    bool denied = false;
    rc = seed ? col_rel_append_rows_atomic(seed, data, num_rows, num_cols,
            &denied) : 0;
    if (rc != 0)
        return rc == ENOMEM && denied ? ENOSPC : rc;
    rc = col_rel_append_rows_atomic(r, data, num_rows, num_cols, &denied);
    if (rc != 0) {
        session_private_rel_truncate(seed, seed_rows);
        return rc == ENOMEM && denied ? ENOSPC : rc;
    }

    wl_col_session_t *sess = COL_SESSION(session);
    session_note_inserted_input(sess, r, true);
    col_controlled_step_record_edge_append(sess, r);
    return 0;
}

/* Forward declaration for col_session_remove_incremental */
static int
col_session_remove_incremental(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols);

static int
col_session_remove(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;
    if (session->input_sealed) return ENOTSUP;
    if (num_rows == 0)
        return 0;
    if (COL_SESSION(session)->delta_observer
        || COL_SESSION(session)->delta_publish_active)
        return EBUSY;
    if (COL_SESSION(session)->plain_step_completion_pending)
        return EBUSY;

    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->plain_step_completion_pending)
        return EBUSY;
    if (sess->delta_cb != NULL)
        return col_session_remove_incremental(session, relation, data, num_rows,
                   num_cols);

    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(
        COL_SESSION(session));
    if (cleanup_rc != 0)
        return cleanup_rc;

    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;
    if (r->ncols == 0)
        return 0; /* uninitialized schema = nothing to remove */
    if (r->ncols != num_cols)
        return EINVAL;

    wl_columnar_source_access_writer_t writer = { 0 };
    col_rel_t *owner = NULL;
    int writer_rc = session_relation_writer_acquire(r, &owner, &writer);
    (void)owner;
    if (writer_rc != 0)
        return writer_rc;

    uint8_t *matched = NULL;
    uint8_t *seed_mask = NULL;
    uint8_t *request_state = NULL;
    col_rel_t *seed = NULL;
    int64_t row_stack[COL_STACK_MAX];
    int64_t *row_buf = row_stack;
    uint32_t remove_count = 0;
    uint64_t input_cells;
    uint64_t input_bytes;
    if (!wl_columnar_memory_size_mul(num_rows, num_cols, &input_cells)
        || !wl_columnar_memory_size_mul(input_cells, sizeof(int64_t),
        &input_bytes)
        || input_bytes > SIZE_MAX) {
        writer_rc = EOVERFLOW;
        goto remove_release;
    }

    /* Prepare the entire match plan before changing the EDB.  A request can
     * consume only one still-unmatched source row, so duplicate requests
     * remove duplicate tuples only when those tuples actually exist. */
    /* The relation writer held across this call keeps r->nrows fixed. */
    uint32_t matched_nrows = r->nrows;
    matched = matched_nrows
        ? (uint8_t *)calloc(matched_nrows, sizeof(*matched)) : NULL;
    request_state = (uint8_t *)calloc(num_rows, sizeof(*request_state));
    if ((matched_nrows != 0 && !matched) || !request_state) {
        writer_rc = ENOMEM;
        goto remove_release;
    }
    size_t row_bytes = (size_t)(input_bytes / num_rows);
    if (num_cols > COL_STACK_MAX) {
        row_buf = (int64_t *)malloc((size_t)num_cols * sizeof(*row_buf));
        if (!row_buf) {
            writer_rc = ENOMEM;
            goto remove_release;
        }
    }
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            if (matched[ri])
                continue;
            col_rel_row_copy_out(r, ri, row_buf);
            if (memcmp(row_buf, del, row_bytes) == 0) {
                matched[ri] = 1;
                request_state[di] = SESSION_REMOVE_IN_RELATION;
                remove_count++;
                break;
            }
        }
    }

    if (remove_count != 0
        && r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        writer_rc = EOVERFLOW;
        goto remove_release;
    }
    /* session_relation_writer_acquire() rejects active leases.  A residual
     * shared-column view without a lease cannot be compacted in place. */
    if (remove_count != 0 && r->col_shared) {
        writer_rc = EBUSY;
        goto remove_release;
    }

    /* Issue #1994: the seed shadow records input, so a request removes from
     * it even when the relation itself no longer holds the row. */
    seed = session_seed_shadow(sess, r->name);
    writer_rc = session_seed_shadow_plan_remove(seed, data, num_rows,
            num_cols, &seed_mask, request_state);
    if (writer_rc != 0)
        goto remove_release;
    if (remove_count != 0) {
        col_rel_t *retired = NULL;
        uint32_t retired_prior = 0;
        writer_rc = session_retire_committed_rows(sess, r, data, num_rows,
                request_state, &retired, &retired_prior);
        if (writer_rc != 0)
            goto remove_release;
    }

    /* No allocation or fallible operation follows the first row write. */
    if (remove_count != 0) {
        uint32_t out_r = 0;
        uint32_t old_nrows = matched_nrows;
        uint32_t old_base_nrows = r->base_nrows < old_nrows
            ? r->base_nrows : old_nrows;
        uint32_t new_base_nrows = 0;
        for (uint32_t ri = 0; ri < old_nrows; ri++) {
            if (!matched[ri]) {
                if (ri < old_base_nrows)
                    new_base_nrows++;
                if (out_r != ri) {
                    col_rel_row_move_raw(r, out_r, ri);
                    if (r->timestamps)
                        r->timestamps[out_r] = r->timestamps[ri];
                }
                out_r++;
            }
        }
        r->nrows = out_r;
        r->base_nrows = new_base_nrows;
        /* Compaction preserves the row sequence, but these optional indexes
         * describe old row offsets.  Let their owning paths rebuild lazily. */
        r->sorted_nrows = 0;
        r->run_count = 0;
        memset(r->run_ends, 0, sizeof(r->run_ends));
        wl_columnar_eval_dedup_set_clear(r);
        wl_columnar_relation_touch_view(r);
    }
    /* A row only the shadow still holds is already absent from the model,
     * so taking it out of the input changes nothing to re-evaluate. */
    session_seed_shadow_apply_remove(seed, seed_mask);
    seed_mask = NULL;
    if (remove_count != 0) {
        session_invalidate_relation_caches(sess, r->name);
        sess->pending_input_change = true;
        /* Issue #2114: nothing records which rows went, so a later
         * incremental insert must not narrow the evaluation to its strata. */
        sess->pending_full_input_eval = true;
        sess->snapshot_stable_valid = false;
    }

remove_release:
    if (row_buf != row_stack)
        free(row_buf);
    free(seed_mask);
    free(request_state);
    free(matched);
    if (writer.owner) {
        int release_rc = wl_columnar_source_access_writer_release(&writer);
        if (writer_rc == 0 && release_rc != 0)
            writer_rc = release_rc;
    }
    return writer_rc;
}

/*
 * col_session_remove_incremental: Remove rows and pre-seed retraction deltas
 *
 * (Issue #158) Semi-naive delta retraction for non-recursive strata.
 * When a delta callback is registered, this function:
 *   1. Creates $r$<name> relation from removed rows
 *   2. Registers it as a session relation (for VARIABLE ops to consume)
 *   3. Removes rows from the EDB using existing compact logic
 *   4. Records the removal for affected-stratum calculation
 *
 * The $r$<name> relation is used during the next session_step to seed
 * the retraction evaluation, enabling delta-only propagation.  A snapshot
 * that evaluates first discards it instead (Issue #2106).
 */
static int
col_session_remove_incremental(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols)
{
    if (!session || !relation || !data)
        return EINVAL;
    if (session->input_sealed) return ENOTSUP;
    if (num_rows == 0)
        return 0;
    if (COL_SESSION(session)->delta_observer
        || COL_SESSION(session)->delta_publish_active)
        return EBUSY;
    if (COL_SESSION(session)->plain_step_completion_pending)
        return EBUSY;

    wl_col_session_t *sess = COL_SESSION(session);

    /* Find EDB relation */
    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(
        COL_SESSION(session));
    if (cleanup_rc != 0)
        return cleanup_rc;

    col_rel_t *r = session_find_rel(sess, relation);
    if (!r)
        return ENOENT;
    if (r->ncols == 0)
        return 0; /* uninitialized schema = nothing to remove */
    if (r->ncols != num_cols)
        return EINVAL;

    wl_columnar_source_access_writer_t writer = { 0 };
    col_rel_t *owner = NULL;
    int writer_rc = session_relation_writer_acquire(r, &owner, &writer);
    (void)owner;
    if (writer_rc != 0)
        return writer_rc;

    struct {
        uint32_t request_row;
        uint32_t source_row;
    } *match_plan = NULL;
    uint8_t *matched = NULL;
    uint8_t *seed_mask = NULL;
    uint8_t *request_state = NULL;
    col_rel_t *seed = NULL;
    col_rel_t *rdelta = NULL;
    col_rel_t *previous_delta = NULL;
    col_rel_t *retired = NULL;
    uint32_t retired_prior = 0;
    wl_columnar_source_access_reader_t previous_reader = { 0 };
    session_rel_registration_t registration;
    session_rel_registration_init(&registration);
    int64_t row_stack[COL_STACK_MAX];
    int64_t *row_buf = row_stack;
    uint32_t match_count = 0;
    uint64_t input_cells;
    uint64_t input_bytes;
    size_t match_plan_count;
    uint32_t match_capacity;
    bool denied = false;
    bool budget_denied = false;
    char rname[256];
    int rc = 0;

    match_capacity = num_rows < r->nrows ? num_rows : r->nrows;
    match_plan_count = match_capacity ? (size_t)match_capacity : 1u;
    if (!wl_columnar_memory_size_mul(num_rows, num_cols, &input_cells)
        || !wl_columnar_memory_size_mul(input_cells, sizeof(int64_t),
        &input_bytes)
        || input_bytes > SIZE_MAX
        || match_plan_count > SIZE_MAX / sizeof(*match_plan)) {
        writer_rc = EOVERFLOW;
        goto incremental_release;
    }
    match_plan = calloc(match_plan_count, sizeof(*match_plan));
    /* The relation writer held across this call keeps r->nrows fixed. */
    uint32_t matched_nrows = r->nrows;
    matched = matched_nrows
        ? (uint8_t *)calloc(matched_nrows, sizeof(*matched)) : NULL;
    request_state = (uint8_t *)calloc(num_rows, sizeof(*request_state));
    if (!match_plan || !request_state
        || (matched_nrows != 0 && !matched)) {
        writer_rc = ENOMEM;
        goto incremental_release;
    }
    if (num_cols > COL_STACK_MAX) {
        row_buf = (int64_t *)malloc((size_t)num_cols * sizeof(*row_buf));
        if (!row_buf) {
            writer_rc = ENOMEM;
            goto incremental_release;
        }
    }

    size_t row_bytes = (size_t)(input_bytes / num_rows);
    for (uint32_t di = 0; di < num_rows; di++) {
        const int64_t *del = data + (size_t)di * num_cols;
        for (uint32_t ri = 0; ri < r->nrows; ri++) {
            if (matched[ri])
                continue;
            col_rel_row_copy_out(r, ri, row_buf);
            if (memcmp(row_buf, del, row_bytes) == 0) {
                if (match_count >= match_capacity) {
                    writer_rc = EOVERFLOW;
                    goto incremental_release;
                }
                matched[ri] = 1;
                request_state[di] = SESSION_REMOVE_IN_RELATION;
                match_plan[match_count].request_row = di;
                match_plan[match_count].source_row = ri;
                match_count++;
                break;
            }
        }
    }

    /* Issue #1994: the seed shadow records input, so a request removes from
     * it even when the relation itself no longer holds the row. */
    seed = session_seed_shadow(sess, r->name);
    writer_rc = session_seed_shadow_plan_remove(seed, data, num_rows,
            num_cols, &seed_mask, request_state);
    if (writer_rc != 0)
        goto incremental_release;

    /* A miss is a no-op for the model: it must not replace an earlier
     * staged retraction or disturb any session publication state.  It still
     * removes the input the seed shadow holds -- a row the model already
     * lacks -- so a later full re-evaluation does not bring it back. */
    if (match_count == 0) {
        session_seed_shadow_apply_remove(seed, seed_mask);
        seed_mask = NULL;
        writer_rc = 0;
        goto incremental_release;
    }
    if (sess->outer_epoch == UINT32_MAX
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        writer_rc = EOVERFLOW;
        goto incremental_release;
    }
    if (r->col_shared) {
        writer_rc = EBUSY;
        goto incremental_release;
    }
    if (sess->last_removed_relation
        && strcmp(sess->last_removed_relation, r->name) != 0) {
        writer_rc = EBUSY;
        goto incremental_release;
    }
    int rname_len = snprintf(rname, sizeof(rname), "$r$%s", r->name);
    if (rname_len < 0 || (size_t)rname_len >= sizeof(rname)) {
        writer_rc = EOVERFLOW;
        goto incremental_release;
    }

    if (sess->last_removed_relation) {
        previous_delta = session_find_rel(sess, rname);
        if (!previous_delta || previous_delta->ncols != num_cols) {
            writer_rc = EINVAL;
            goto incremental_release;
        }
        rc = col_rel_source_reader_acquire(previous_delta, &previous_reader);
        if (rc != 0) {
            writer_rc = rc;
            goto incremental_release;
        }
    }

    rc = col_rel_alloc(&rdelta, rname);
    if (rc != 0 || !rdelta) {
        writer_rc = rc != 0 ? rc : ENOMEM;
        goto incremental_release;
    }
    rc = col_rel_attach_memory_governor(rdelta, sess->memory_governor);
    if (rc != 0) {
        budget_denied = rdelta->memory_budget_denial_pending;
        writer_rc = budget_denied ? ENOSPC : rc;
        goto incremental_release;
    }
    /* Build the complete private replacement, carrying earlier same-relation
     * retractions forward before adding rows selected by this request. */
    uint32_t previous_rows = previous_delta ? previous_delta->nrows : 0;
    if (previous_delta) {
        for (uint32_t i = 0; i < previous_delta->nrows; i++) {
            col_rel_row_copy_out(previous_delta, i, row_buf);
            denied = false;
            rc = col_rel_append_rows_atomic(rdelta, row_buf, 1u, num_cols,
                    &denied);
            if (rc != 0) {
                budget_denied = rc == ENOMEM && denied;
                writer_rc = budget_denied ? ENOSPC : rc;
                goto incremental_release;
            }
        }
    }
    for (uint32_t i = 0; i < match_count; i++) {
        uint32_t di = match_plan[i].request_row;
        denied = false;
        rc = col_rel_append_rows_atomic(rdelta,
                data + (size_t)di * num_cols, 1u, num_cols, &denied);
        if (rc != 0) {
            budget_denied = rc == ENOMEM && denied;
            writer_rc = budget_denied ? ENOSPC : rc;
            goto incremental_release;
        }
    }
    if (r->timestamps || (previous_delta && previous_delta->timestamps)) {
        rc = col_rel_enable_timestamps(rdelta);
        if (rc != 0) {
            budget_denied = rdelta->memory_budget_denial_pending;
            writer_rc = budget_denied ? ENOSPC : rc;
            goto incremental_release;
        }
        if (previous_delta && previous_delta->timestamps) {
            for (uint32_t i = 0; i < previous_rows; i++)
                rdelta->timestamps[i] = previous_delta->timestamps[i];
        }
        for (uint32_t i = 0; i < match_count; i++) {
            uint32_t source_row = match_plan[i].source_row;
            if (r->timestamps)
                rdelta->timestamps[previous_rows + i]
                    = r->timestamps[source_row];
        }
    }

    if (previous_reader.owner) {
        rc = col_rel_source_reader_release(&previous_reader);
        if (rc != 0) {
            writer_rc = rc;
            goto incremental_release;
        }
    }
    previous_delta = NULL;
    rc = session_retire_committed_rows(sess, r, data, num_rows,
            request_state, &retired, &retired_prior);
    if (rc != 0) {
        writer_rc = rc;
        goto incremental_release;
    }
    rc = session_rel_registration_prepare(sess, rdelta, &registration);
    if (rc != 0) {
        session_private_rel_truncate(retired, retired_prior);
        writer_rc = rc;
        goto incremental_release;
    }

    /* From this point onward, EDB compaction and publication cannot fail. */
    uint32_t old_nrows = matched_nrows;
    uint32_t old_base_nrows = r->base_nrows < old_nrows
        ? r->base_nrows : old_nrows;
    uint32_t out_r = 0;
    uint32_t new_base_nrows = 0;
    for (uint32_t ri = 0; ri < old_nrows; ri++) {
        if (!matched[ri]) {
            if (ri < old_base_nrows)
                new_base_nrows++;
            if (out_r != ri) {
                col_rel_row_move_raw(r, out_r, ri);
                if (r->timestamps)
                    r->timestamps[out_r] = r->timestamps[ri];
            }
            out_r++;
        }
    }
    r->nrows = out_r;
    r->base_nrows = new_base_nrows;
    r->sorted_nrows = 0;
    r->run_count = 0;
    memset(r->run_ends, 0, sizeof(r->run_ends));
    wl_columnar_eval_dedup_set_clear(r);
    wl_columnar_relation_touch_view(r);
    session_rel_registration_commit(sess, rdelta, &registration);
    rdelta = NULL;
    session_seed_shadow_apply_remove(seed, seed_mask);
    seed_mask = NULL;
    session_invalidate_relation_caches(sess, r->name);
    sess->last_removed_relation = r->name;
    sess->outer_epoch++;
    sess->pending_input_change = true;
    sess->snapshot_stable_valid = false;
    writer_rc = 0;

incremental_release:
    if (budget_denied)
        sess->remove_staging_budget_denied = true;
    if (previous_reader.owner)
        (void)col_rel_source_reader_release(&previous_reader);
    if (registration.retirement.relation
        || registration.replacement_array)
        session_rel_registration_discard(&registration);
    if (rdelta)
        col_rel_destroy(rdelta);
    if (row_buf != row_stack)
        free(row_buf);
    free(match_plan);
    free(seed_mask);
    free(request_state);
    free(matched);
    if (writer.owner) {
        int release_rc = wl_columnar_source_access_writer_release(&writer);
        if (writer_rc == 0 && release_rc != 0)
            writer_rc = release_rc;
    }
    return writer_rc;
}

/* Issue #158: forget the removal a step or snapshot has just evaluated --
 * the pending relation, the retraction flag and every `$r$<name>` staging
 * relation.  Issue #2106: a snapshot that evaluated a removal staged with a
 * delta callback installed kept all three, so every later snapshot missed
 * the stable path and evaluated again on top of the model it had emitted. */
static void
session_retraction_cleanup(wl_col_session_t *sess)
{
    sess->last_removed_relation = NULL;
    sess->retraction_seeded = false;
    for (uint32_t i = 0; i < sess->nrels;) {
        col_rel_t *r = sess->rels[i];
        if (r && strncmp(r->name, "$r$", 3) == 0) {
            if (session_remove_rel(sess, r->name) != 0)
                i++;
            /* A successful removal leaves the same index for the next slot. */
        } else {
            i++;
        }
    }
}

/*
 * col_session_step: Advance the session by one evaluation epoch
 *
 * Implements wl_compute_backend_t.session_step vtable slot.
 *
 * Iterates all strata in plan order. For each stratum:
 *   - Fast path (no delta_cb): col_eval_stratum directly
 *   - Delta path: col_stratum_step_with_delta (snapshot + eval + set diff)
 * Arena is reset after each stratum to reclaim temporary evaluation data.
 *
 * TODO(#810): Replace set-diff delta with semi-naive ΔR propagation.
 *
 * @param session: wl_session_t* (cast to wl_col_session_t* internally)
 * @return 0 on success, non-zero on evaluation error
 */
static void col_session_reclaim_quiescent(wl_col_session_t *sess);

#ifdef WL_SESSION_TEST_HOOKS
/*
 * Fired on the stable fast path, just above its cutoff.  The hole that cutoff
 * closes there is a cancellation that lands after the wrapper's entry poll, and
 * no single-threaded test can place one between that poll and the emit without
 * a lever.  Under the same WL_SESSION_TEST_HOOKS boundary as every other lever
 * here: an unguarded definition survives in libwirelog_static.a as a writable
 * global, and the iOS leg builds the shared library with default visibility.
 */
void (*wl_columnar_session_test_before_stable_emit)(wl_col_session_t *sess);
#endif

bool
wl_columnar_session_budget_denied(const void *session)
{
    const wl_col_session_t *sess = session;

    if (!sess || sess->memory_budget_denied)
        return sess != NULL && sess->memory_budget_denied;
    for (uint32_t i = 0; i < sess->tdd_workers_count; i++) {
        if (sess->tdd_workers[i].memory_budget_denied)
            return true;
    }
    for (uint32_t i = 0; i < sess->nrels; i++) {
        if (sess->rels[i] && sess->rels[i]->memory_budget_denial_pending)
            return true;
    }
    for (uint32_t i = 0; i < sess->diff_arr_count; i++) {
        if (sess->diff_arr_entries[i].diff_arr
            && sess->diff_arr_entries[i].diff_arr
            ->memory_budget_denial_pending)
            return true;
    }
    return false;
}

void
wl_columnar_session_budget_denial_clear(wl_col_session_t *sess)
{
    if (!sess)
        return;
    sess->memory_budget_denied = false;
    for (uint32_t i = 0; i < sess->tdd_workers_count; i++)
        sess->tdd_workers[i].memory_budget_denied = false;
    for (uint32_t i = 0; i < sess->nrels; i++)
        if (sess->rels[i])
            sess->rels[i]->memory_budget_denial_pending = false;
    for (uint32_t i = 0; i < sess->diff_arr_count; i++)
        if (sess->diff_arr_entries[i].diff_arr)
            sess->diff_arr_entries[i].diff_arr
            ->memory_budget_denial_pending = false;
    for (uint32_t i = 0; i < sess->tdd_workers_count; i++)
        for (uint32_t r = 0; r < sess->tdd_workers[i].nrels; r++)
            if (sess->tdd_workers[i].rels[r])
                sess->tdd_workers[i].rels[r]
                ->memory_budget_denial_pending = false;
}

static void
col_session_budget_denial_clear(void *session)
{
    wl_columnar_session_budget_denial_clear(COL_SESSION(session));
}

/*
 * The publication cutoff (#1953).  wl_evaluation_control_finish does not poll,
 * so the integration owes a final charge(0) before it commits publication --
 * without one, a cancellation arriving after the wrapper's entry poll is seen
 * only by the NEXT attempt, and this one publishes and reports success.
 *
 * charge() returns EINVAL when no attempt is active.  That is deliberately NOT
 * softened into a no-op here: the impls run only inside their wrapper's begin,
 * so an EINVAL would mean an impl was reached outside its attempt, which is the
 * defect class #1820 shipped.  Let it surface.
 */
static int
col_session_publication_cutoff(wl_col_session_t *sess)
{
    return sess->base.evaluation_control
        ? wl_evaluation_control_charge(sess->base.evaluation_control, 0) : 0;
}

static int
col_session_clear_idb_rows(const wl_plan_t *plan, wl_col_session_t *sess,
    uint32_t strata_end);

/* Issue #2092: records that strata [0, si] completed in an evaluation that
 * has not committed yet.  The snapshot runs every stratum in order from 0.
 * A plain step does too, or resumes just after the prefix an earlier attempt
 * completed, so the completed strata form a prefix. */
static void
col_session_note_stratum_done(wl_col_session_t *sess, uint32_t si)
{
    uint32_t done = si < UINT16_MAX ? si + 1 : UINT16_MAX;
    if (done > sess->uncommitted_strata_done)
        sess->uncommitted_strata_done = (uint16_t)done;
}

static int
col_session_step_impl(wl_session_t *session)
{
    wl_col_session_t *sess = COL_SESSION(session);
    if (sess->delta_observer_retirement
        == WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_COMPLETED_STEP) {
        int retire_rc = wl_columnar_eval_delta_observer_discard(sess);
        if (retire_rc != 0)
            return retire_rc;
        col_session_reclaim_quiescent(sess);
        return 0;
    }
    if (sess->delta_observer_retirement
        != WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE) {
        int retire_rc = wl_columnar_eval_delta_observer_discard(sess);
        if (retire_rc != 0)
            return retire_rc;
    }
    wl_columnar_session_budget_denial_clear(sess);
    if (sess->plain_step_completion_pending
        && sess->plain_step_completion_active)
        return EBUSY;
    if (!sess->plain_step_completion_pending)
        sess->plain_step_completion_step_context = false;
    if (sess->delta_publish_active || sess->delta_observer_prepare
        || sess->delta_observer_build || sess->cleanup_active
        || wl_columnar_eval_delta_rollback_active(sess)
        || wl_columnar_eval_delta_observer_active(sess))
        return EBUSY;
    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(sess);
    if (cleanup_rc != 0)
        return cleanup_rc;
    int deferred_rc = wl_columnar_session_retry_deferred(sess);
    if (deferred_rc != 0)
        return deferred_rc;
    const wl_plan_t *plan = sess->plan;
    if (!sess->plain_step_completion_pending)
        sess->plain_step_completion_step_context =
            sess->delta_observer == NULL && sess->delta_cb == NULL;
    uint32_t completion_start = 0;
    bool completing_plain_step = sess->plain_step_completion_pending;
    uint64_t completion_mask = UINT64_MAX;
    if (completing_plain_step) {
        if (sess->plain_step_completion_active)
            return EBUSY;
        sess->plain_step_completion_active = true;
        if (sess->plain_step_completion_phase
            == WL_COLUMNAR_PLAIN_STEP_COMPLETION_COMPACT)
            goto compact_staged_deltas;
        if (sess->plain_step_completion_phase
            != WL_COLUMNAR_PLAIN_STEP_COMPLETION_FINALIZE_NONREC
            && sess->plain_step_completion_phase
            != WL_COLUMNAR_PLAIN_STEP_COMPLETION_EVALUATE_REMAINING)
            return EBUSY;
        completion_start = sess->plain_step_completion_stratum + 1;
        completion_mask = sess->plain_step_completion_mask;
        if (sess->plain_step_completion_phase
            == WL_COLUMNAR_PLAIN_STEP_COMPLETION_FINALIZE_NONREC) {
            int resume_rc =
                wl_columnar_eval_resume_nonrecursive_completion(sess);
            if (resume_rc != 0)
                return resume_rc;
        }
    }
    sess->extension_expr_status = 0;

    /* A step with nothing pending has nothing to derive, with or without a
     * delta callback (Issue #1994).  Every write that invalidates the stable
     * model also sets pending_input_change, and a session starts with it
     * set, so the first step still evaluates. */
    if (!completing_plain_step && !sess->delta_observer
        && !sess->pending_input_change
        && sess->last_inserted_relation == NULL
        && sess->last_removed_relation == NULL){
        col_session_reclaim_quiescent(sess);
        return 0;
    }

    /* Compute affected strata bitmask (Phase 4 incremental skip).
     * A step may carry both an insertion and a removal, and the two
     * relations need not share a stratum, so the mask is the UNION of the
     * strata reachable from each -- seeded at 0 so a single-source step
     * still narrows to just that source's strata (Issue #1031).  When
     * neither pointer is set (regular step) UINT64_MAX means all strata are
     * evaluated, and pending_full_input_eval ("this step cannot be
     * incremental") keeps that default no matter what is pending. */
    uint64_t affected_mask =
        completing_plain_step ? completion_mask : UINT64_MAX;
    if (!completing_plain_step && !sess->pending_full_input_eval
        && (sess->last_inserted_relation != NULL
        || sess->last_removed_relation != NULL)) {
        affected_mask = 0;
        /* Issue #2091: col_compute_affected_strata marks the strata that
         * read a relation.  A rule head written by the host must also be
         * re-derived by the strata that define it, or a removed row that a
         * rule still derives stays out of the model. */
        if (sess->last_inserted_relation != NULL) {
            affected_mask |= col_compute_affected_strata(
                session, sess->last_inserted_relation);
            affected_mask |= session_head_strata(plan,
                    sess->last_inserted_relation);
        }
        if (sess->last_removed_relation != NULL) {
            affected_mask |= col_compute_affected_strata(
                session, sess->last_removed_relation);
            affected_mask |= session_head_strata(plan,
                    sess->last_removed_relation);
        }
    }
    /* Issue #1994: a step with no delta callback is not incremental.  It
     * re-derives every stratum from scratch, so once the session has evaluated
     * it first discards what that evaluation derived and resets every stratum
     * frontier, under the same has_evaluated condition a snapshot uses before
     * a full re-evaluation; the rule frontiers are reset below.  Without the
     * clear every step appended another copy of each derived row and a
     * retraction never reached the rows it had derived; without the stratum
     * reset a stale frontier skips iterations a recursive stratum needs after
     * the clear.  A resumed step (completing_plain_step) keeps what its first
     * attempt already merged. */
    if (sess->plain_step_completion_step_context
        && !completing_plain_step) {
        affected_mask = UINT64_MAX;
        if (sess->has_evaluated) {
            /* Until this step commits, the derived relations are empty or
             * partial, so whatever evaluates next must clear and evaluate in
             * full; only the commit below resets this. */
            sess->pending_full_input_eval = true;
            int clear_rc = col_session_clear_idb_rows(plan, sess,
                    plan->stratum_count);
            if (clear_rc != 0)
                return clear_rc;
            for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA;
                si++)
                sess->frontier_ops->reset_stratum_frontier(sess, si,
                    sess->outer_epoch);
        } else if (sess->uncommitted_strata_done > 0) {
            /* Issue #2092: a first evaluation failed after these strata
             * completed.  Discard what they derived, keeping host input, so
             * re-deriving them does not append a second copy.  The stratum
             * that failed is re-run over its own rows, as before. */
            uint32_t done = sess->uncommitted_strata_done;
            int clear_rc = col_session_clear_idb_rows(plan, sess, done);
            if (clear_rc != 0)
                return clear_rc;
            for (uint32_t si = 0; si < plan->stratum_count && si < MAX_STRATA
                && si < done; si++)
                sess->frontier_ops->reset_stratum_frontier(sess, si,
                    sess->outer_epoch);
        }
        sess->plain_step_completion_mask = affected_mask;
    }

    if (!completing_plain_step && sess->delta_observer) {
        affected_mask = wl_columnar_eval_delta_observer_mask(sess);
    } else if (!completing_plain_step && sess->delta_cb) {
        int rc = wl_columnar_eval_delta_observer_begin(sess, affected_mask);
        if (rc != 0)
            return rc;
    }
    if (!completing_plain_step)
        wl_columnar_eval_delta_observer_set_active(sess, true);
    if (wl_columnar_eval_delta_observer_evaluated(sess))
        goto compact_staged_deltas;

    /* Issue #158: Pre-seed retraction deltas.  If last_removed_relation is
     * set (removal via col_session_remove_incremental), check if $r$<name>
     * exists for retractions, and set retraction_seeded.  This block does
     * not narrow affected_mask; the removal's contribution to it is unioned
     * in above (Issue #1031).  A plain step re-derives from scratch instead
     * (Issue #1994): seeding there would make the first iteration read the
     * removed rows in place of the relation. */
    if (sess->last_removed_relation != NULL
        && !sess->plain_step_completion_step_context) {
        char rname[256];
        if (retraction_rel_name(sess->last_removed_relation, rname,
            sizeof(rname))
            == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rname);
            if (rdelta && rdelta->nrows > 0)
                sess->retraction_seeded = true;
        }
    }

    /* Issue #264: Activate differential operators when session toggle is on
     * and only partial strata are affected (see col_should_activate_diff). */
    sess->diff_operators_active = col_should_activate_diff(sess, affected_mask);

    /* Issue #106 (US-106-004): Reset rule frontiers with stratum context awareness.
     * col_session_step is for delta callback mode (no pre-seeded deltas).
     * Always reset affected rules to force re-evaluation. */
    if (affected_mask == UINT64_MAX && !completing_plain_step) {
        /* Full evaluation (non-incremental): reset all rules to (current_epoch, UINT32_MAX)
         * sentinel. Prevents premature skip across different evaluation contexts. */
        for (uint32_t ri = 0; ri < MAX_RULES; ri++) {
            sess->frontier_ops->reset_rule_frontier(sess, ri,
                sess->outer_epoch);
        }
    } else {
        /* Incremental (delta callback mode): reset affected rules to (current_epoch, UINT32_MAX).
         * No pre-seeded deltas in this path, so reset unconditionally. */
        for (uint32_t si = 0; si < plan->stratum_count; si++) {
            if (col_affected_mask_contains(affected_mask, si)
                && (!completing_plain_step || si >= completion_start)) {
                uint32_t rule_base = 0;
                for (uint32_t j = 0; j < si; j++)
                    rule_base += plan->strata[j].relation_count;
                for (uint32_t ri = 0; ri < plan->strata[si].relation_count;
                    ri++) {
                    uint32_t rule_id = rule_base + ri;
                    if (rule_id < MAX_RULES) {
                        sess->frontier_ops->reset_rule_frontier(sess, rule_id,
                            sess->outer_epoch);
                    }
                }
            }
        }
    }

    wl_columnar_delta_events_clear(sess);
    sess->delta_event_transaction = false;
    for (uint32_t si = completion_start; si < plan->stratum_count; si++) {
        /* Skip strata not affected by the last incremental insertion */
        if (!col_affected_mask_contains(affected_mask, si))
            continue;

        const wl_plan_stratum_t *sp = &plan->strata[si];
        int rc = (!completing_plain_step && sess->delta_observer)
            ? col_stratum_step_with_delta(sp, sess, si)
            : col_eval_stratum_tdd(sp, sess, si);
        if (rc != 0) {
            sess->delta_event_transaction = false;
            wl_columnar_delta_events_clear(sess);
            wl_columnar_eval_delta_observer_set_active(sess, false);
            sess->plain_step_completion_active = false;
            col_session_reclaim_quiescent(sess);
            return rc;
        }
        if (sess->plain_step_completion_step_context) {
            col_session_note_stratum_done(sess, si);
            /* The stratum, including checked cleanup/finalization, committed.
             * Preserve that prefix if a later W1 stratum returns an ordinary
             * error before the final compaction continuation is armed. */
            if (sess->num_workers <= 1) {
                sess->plain_step_completion_pending = true;
                sess->plain_step_completion_active = true;
            }
            sess->plain_step_completion_stratum = si;
            sess->plain_step_completion_phase =
                WL_COLUMNAR_PLAIN_STEP_COMPLETION_EVALUATE_REMAINING;
        }
    }
    wl_columnar_eval_delta_observer_set_evaluated(sess);
compact_staged_deltas:
    /* Compaction is a source mutation.  Perform it before clearing the
     * pending operation state or removing temporary retraction relations so
     * EBUSY can be retried without committing session bookkeeping or
     * publishing duplicate delta events. */
    if (sess->plain_step_completion_step_context) {
        sess->plain_step_completion_pending = true;
        sess->plain_step_completion_active = true;
        sess->plain_step_completion_phase =
            WL_COLUMNAR_PLAIN_STEP_COMPLETION_COMPACT;
    }
    int compact_rc = col_rel_compact_many(sess->rels, sess->nrels);
    if (compact_rc != 0) {
        sess->plain_step_completion_active = false;
        wl_columnar_eval_delta_observer_set_active(sess, false);
        col_session_reclaim_quiescent(sess);
        return compact_rc;
    }

    /*
     * Cut here: after compaction, before the observer is prepared.
     *
     * After compaction, because both shapes take their resume token across it.
     * The observer records `evaluated` above the label; the plain step sets
     * plain_step_completion_pending with phase COMPACT just before compacting,
     * and has no observer to carry that state instead.  Cutting earlier would
     * leave a stopped plain step with no token and force a full re-evaluation.
     * Compacting twice is harmless: the batch releases every writer it took,
     * and a second pass over compacted data is idempotent.
     *
     * Before prepare, because prepare reserves for the whole staged diff and
     * allocates the event array.  Prepare publishes nothing to the host, so
     * cutting ahead of it gives up no coverage and keeps a stopped attempt from
     * holding a reservation it never uses.
     *
     * Outside the observer branch, so the callback-free shape is covered too:
     * with no observer, control falls straight from here into the commit below,
     * which is where a later snapshot starts treating this model as stable.
     *
     * The unwind clears one committed field.  plain_step_completion_active is a
     * re-entrancy latch, not progress: while it and _pending are both set, this
     * function's entry guard and the snapshot's refuse with EBUSY.  Each unwind
     * that can run with it set clears it, this one included; leaving it set is
     * what would strand the session.  set_active(false) reverts what an attempt
     * that reached here set, and is what lets the observer be discarded at
     * destroy.
     *
     * What routes the retry back to this label is the observer's `evaluated`
     * flag, or, with no observer, plain_step_completion_pending with phase
     * COMPACT, and the observer's staged baseline is what makes the retry
     * deliver the same events.  pending_input_change and has_evaluated are not
     * routers: this function writes them only in the commit block below, so
     * skipping it leaves them describing what this attempt published.  Say
     * nothing here about base_nrows -- evaluation rewrites it above this point
     * for recursive aggregates, so it is neither ours to describe nor safe to
     * reason about from this vantage.
     */
    int cutoff_rc = col_session_publication_cutoff(sess);
    if (cutoff_rc != 0) {
        sess->plain_step_completion_active = false;
        wl_columnar_eval_delta_observer_set_active(sess, false);
        col_session_reclaim_quiescent(sess);
        return cutoff_rc;
    }

    if (!completing_plain_step && sess->delta_observer) {
        int prepare_rc = wl_columnar_eval_delta_observer_prepare(sess);
        if (prepare_rc != 0) {
            wl_columnar_eval_delta_observer_set_active(sess, false);
            return prepare_rc;
        }
        wl_columnar_eval_delta_observer_publish(sess);
    }

    /* Issue #158: Cleanup retraction state and delta relations after step */
    session_retraction_cleanup(sess);

    /* Reset after successful eval so next plain session_step runs all strata */
    sess->last_inserted_relation = NULL;
    sess->pending_input_change = false;
    sess->pending_full_input_eval = false;
    sess->has_evaluated = true;
    sess->uncommitted_strata_done = 0;
    sess->plain_step_completion_pending = false;
    sess->plain_step_completion_active = false;
    sess->plain_step_completion_phase =
        WL_COLUMNAR_PLAIN_STEP_COMPLETION_NONE;
    sess->plain_step_completion_step_context = false;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            r->base_nrows = r->nrows;
    }
    session_retired_rows_clear(sess);
    sess->snapshot_stable_valid = true;
    if (sess->delta_observer) {
        int finish_rc = wl_columnar_eval_delta_observer_finish(sess);
        if (finish_rc != 0)
            return finish_rc;
    }
    col_session_reclaim_quiescent(sess);
    return 0;
}

/*
 * col_session_set_delta_cb: Register a delta callback on this session
 *
 * Implements wl_compute_backend_t.session_set_delta_cb vtable slot.
 * The callback is invoked with diff=+1 for new tuples during col_session_step.
 *
 * TODO(#810): Also fire diff=-1 for retracted tuples when semi-naive
 * delta propagation tracks removed tuples explicitly.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Function invoked per output delta tuple (NULL to disable)
 * @param user_data: Opaque pointer passed through to callback
 */
static void
col_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
    void *user_data)
{
    if (!session)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    sess->delta_cb = callback;
    sess->delta_data = user_data;
    /* Issue #2121: clearing the callback sets no routing flag.  The next
     * snapshot chooses its route from what is staged, as it does for a
     * session that never had a callback (#2114). */
    if (!callback) {
        if (sess->delta_publish_active)
            sess->delta_publish_cancelled = true;
        wl_columnar_eval_delta_observer_cancel(sess);
    }
}

#ifdef WL_SESSION_TEST_HOOKS
bool wl_columnar_session_test_enable_correctness_replay;
void (*wl_columnar_session_test_before_correctness_replay)(wl_col_session_t *);
#endif

static void
col_session_reset_snapshot_profile(wl_col_session_t *sess)
{
    memset(&sess->tdd_audit, 0, sizeof(sess->tdd_audit));
    sess->consolidation_ns = 0;
    sess->kfusion_ns = 0;
    sess->kfusion_alloc_ns = 0;
    sess->kfusion_dispatch_ns = 0;
    sess->kfusion_merge_ns = 0;
    sess->kfusion_cleanup_ns = 0;
    sess->consolidate_fast_hits = 0;
    sess->consolidate_slow_hits = 0;
    sess->exchange_time_ns = 0;
    sess->tdd_total_ns = 0;
    sess->tdd_dispatch_wait_ns = 0;
    sess->tdd_submit_loop_ns = 0;
    sess->tdd_wait_barrier_ns = 0;
    sess->tdd_worker_sum_ns = 0;
    sess->tdd_worker_max_ns = 0;
    sess->tdd_idle_estimate_ns = 0;
    sess->tdd_queue_drain_ns = 0;
    sess->tdd_convergence_ns = 0;
    sess->tdd_exchange_ns = 0;
    sess->tdd_exchange_matrix_ns = 0;
    sess->tdd_exchange_coordinator_ns = 0;
    sess->tdd_exchange_scatter_ns = 0;
    sess->tdd_exchange_gather_ns = 0;
    sess->tdd_exchange_broadcast_ns = 0;
    sess->tdd_final_merge_ns = 0;
    sess->tdd_last_active_workers = 0;
    sess->tdd_max_active_workers = 0;
    sess->tdd_recursive_strata = 0;
    sess->tdd_executed_strata = 0;
    sess->tdd_fallback_strata = 0;
    memset(sess->tdd_fallback_reason_counts, 0,
        sizeof(sess->tdd_fallback_reason_counts));
    sess->tdd_last_fallback_reason = WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE;
    sess->tdd_decision_tracking_active = false;
}

static bool
col_controlled_step_controlled_op(const wl_plan_op_t *op)
{
    switch (op->op) {
    case WL_PLAN_OP_VARIABLE:
    case WL_PLAN_OP_MAP:
    case WL_PLAN_OP_FILTER:
    case WL_PLAN_OP_JOIN:
    case WL_PLAN_OP_ANTIJOIN:
    case WL_PLAN_OP_REDUCE:
    case WL_PLAN_OP_CONCAT:
    case WL_PLAN_OP_CONSOLIDATE:
    case WL_PLAN_OP_SEMIJOIN:
        return true;
    default:
        return false;
    }
}

static bool
col_controlled_step_batch_shape(const wl_col_session_t *sess)
{
    const wl_plan_t *plan = sess->plan;
    if (!sess->join_batch_bytes || sess->join_batch_strict || !plan
        || plan->stratum_count != 1 || !plan->strata
        || plan->strata[0].is_recursive
        || plan->strata[0].relation_count != 1
        || !plan->strata[0].relations)
        return false;
    return true;
}

/* Reject whole-session and recursive routes before opening an operation
 * attempt.  This checks only immutable plan shape and session admission state;
 * relation/operator validation and all evaluation work remain charged below. */
static bool wl_columnar_session_observed_general_shape(
    const wl_col_session_t *);

static bool
wl_columnar_session_recursive_transaction_shape(const wl_col_session_t *sess)
{
    const wl_plan_t *plan = sess->plan;
    if (!plan || !plan->strata ||
        plan->stratum_count > MAX_STRATA) return false;
    /* Preserve the independently qualified binary fast path and its tariffs. */
    if (!sess->delta_cb && plan->stratum_count == 1
        && plan->strata[0].relation_count == 1 && plan->strata[0].relations
        && wl_columnar_eval_plan_numeric_recursive_path_supported(
            &plan->strata[0].relations[0])) return false;
    for (uint32_t i = 0; i < plan->stratum_count; ++i) {
        const wl_plan_stratum_t *stratum = &plan->strata[i];
        if (!stratum->is_recursive) continue;
        /* Preserve the existing before-attempt refusal of the single-head
         * typed route; general metadata is validated by charged admission. */
        if (plan->stratum_count == 1 && stratum->relation_count == 1
            && (!stratum->bodies || !stratum->bodies[0].declared_int64))
            return false;
        return true;
    }
    return false;
}

static int
col_controlled_step_static_preflight(wl_col_session_t *sess, bool successor)
{
    const wl_plan_t *plan = sess->plan;
    const wl_plan_relation_t *only_relation = NULL;
    const wl_columnar_controlled_step_t *step = sess->controlled_step;
    if (sess->delta_cb && plan && plan->stratum_count > MAX_STRATA)
        return ENOTSUP;
    if (step && step->general_observed) return 0;
    /* A stopped rule retains its own baseline for the next control attempt.
     * Do not mistake that owned, unevaluated baseline for a foreign observer. */
    bool retained_baseline = step && step->observed && step->baseline_ready
        && !step->operation_active && !step->complete && !step->published
        && !step->observer_evaluated && !step->observer_preparing
        && !step->observer_prepare_discard && !step->observer_prepared
        && !step->observer_cleanup_pending
        && !wl_columnar_eval_delta_observer_evaluated(sess);
    if (sess->cleanup_active || sess->cleanup_pending)
        return EBUSY;
    if (!plan || !plan->strata || !sess->base.evaluation_control
        || (sess->delta_observer && !retained_baseline)
        || sess->current_iteration
        || sess->retraction_seeded || sess->retraction_right_pass
        || sess->tdd_input_run || sess->tdd_subpass_active
        || sess->tdd_outbound_only_active || sess->diff_operators_active
        || (sess->join_batch_bytes && sess->join_batch_strict)
        || sess->join_output_shared_count || sess->join_output_limit
        || sess->coordinator || sess->num_workers > 1
        || sess->cleanup_active || sess->cleanup_pending
        || wl_columnar_eval_delta_rollback_active(sess)
        || (wl_columnar_eval_delta_observer_active(sess) && !retained_baseline))
        return ENOTSUP;

    if (wl_columnar_session_recursive_transaction_shape(sess)) return 0;

    /* The first controlled recursive route is intentionally exact: one
     * recursive path relation, the parser's terminal zero-worker EXCHANGE,
     * and one registered EDB source. All other recursive plans remain
     * fail-closed before opening an attempt. */
    if (plan->stratum_count == 1 && plan->strata[0].is_recursive) {
        const wl_plan_stratum_t *stratum = &plan->strata[0];
        bool successor = sess->controlled_step
            && sess->controlled_step->provenance_valid;
        if (sess->delta_cb || stratum->relation_count != 1
            || !stratum->relations
            || sess->nrels != (successor ? 2u : 1u)
            || !wl_columnar_eval_plan_numeric_recursive_path_supported(
                &stratum->relations[0]))
            return ENOTSUP;
        if (successor
            && !col_controlled_step_provenance_current(sess,
            sess->controlled_step))
            return EINVAL;
        col_rel_t *edge = sess->rels ? sess->rels[0] : NULL;
        if (!edge || !edge->name || strcmp(edge->name, "edge") != 0
            || edge->ncols != 2 || edge->timestamps
            || edge->compound_kind != WIRELOG_COMPOUND_KIND_NONE
            || (edge->column_types
            && (edge->column_types[0] != WIRELOG_TYPE_INT64
            || edge->column_types[1] != WIRELOG_TYPE_INT64)))
            return ENOTSUP;
        return 0;
    }

    if (step && step->general_observed) return 0;
    if (!sess->delta_cb) return 0;
    if (wl_columnar_session_observed_general_shape(sess) ||
        (successor && sess->nrels > 3)) return 0;

    /* This route has exactly one stratum/rule. Reject other counts before
     * traversing metadata; variable-size validation belongs to the attempt. */
    if (plan->stratum_count != 1 || plan->strata[0].is_recursive
        || plan->strata[0].relation_count != 1 || !plan->strata[0].relations)
        return ENOTSUP;
    only_relation = &plan->strata[0].relations[0];
    if (successor && sess->nrels > 3)
        return ENOTSUP;
    {
        if (!only_relation->name
            || (only_relation->op_count != 3 && only_relation->op_count != 4)
            || !only_relation->ops
            || only_relation->ops[0].op != WL_PLAN_OP_VARIABLE
            || (only_relation->op_count == 4
            && only_relation->ops[1].op != WL_PLAN_OP_FILTER)
            || only_relation->ops[only_relation->op_count - 2].op
            != WL_PLAN_OP_MAP
            || only_relation->ops[only_relation->op_count - 1].op
            != WL_PLAN_OP_CONSOLIDATE
            || !only_relation->ops[0].relation_name)
            return ENOTSUP;
        const wl_plan_op_t *map =
            &only_relation->ops[only_relation->op_count - 2];
        if (map->project_count != 1 || !map->project_indices
            || map->project_indices[0] != 0
            || (map->map_expr_count && (!map->map_exprs
            || map->map_expr_count != 1)))
            return ENOTSUP;
        if (map->map_expr_count) {
            const wl_plan_expr_buffer_t *expr = &map->map_exprs[0];
            if (!expr->data && expr->size)
                return ENOTSUP;
            if (expr->data && expr->size) {
                uint16_t len = 0;
                if (expr->size != 7 || expr->data[0] != WL_PLAN_EXPR_VAR)
                    return ENOTSUP;
                memcpy(&len, expr->data + 1, sizeof(len));
                if (len != 4 || memcmp(expr->data + 3, "col0", 4) != 0)
                    return ENOTSUP;
            }
        }
    }

    return 0;
}

/* Observed admission precedes baseline construction and operator execution.
 * All caller-sized strings/arrays are visited one charged action at a time. */
static int
col_controlled_step_observed_preflight(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    const wl_plan_t *plan = sess->plan;
    const wl_plan_relation_t *rule = &plan->strata[0].relations[0];
    const char *source = rule->ops[0].relation_name;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    if (!step->observed) return 0;
    /* A refused preflight releases session admission. A later mutation may
     * replace the registry before retry; discard its partial lookup proof. */
    if (step->observed_preflight_phase && !step->baseline_ready
        && !step->relation_active && !step->cursor_advance_pending
        && (step->observed_preflight_registry != sess->rels
        || step->observed_preflight_registry_count != sess->nrels)) {
        step->observed_preflight_phase = 0;
        step->successor_private_name = NULL;
        step->successor_name_phase = 0;
        step->successor_metadata_byte = 0;
    }
    if (step->observed_preflight_phase == 8) return 0;
    for (unsigned action = 0; action < 64; ++action) {
        int rc = wl_evaluation_control_charge_attempt(control,
                step->operation_id, 1);
        if (rc) return rc;
        switch (step->observed_preflight_phase) {
        case 0:
            step->observed_preflight_registry = sess->rels;
            step->observed_preflight_registry_count = sess->nrels;
            step->observed_preflight_slot = 0;
            step->observed_preflight_byte = 0;
            step->observed_input = NULL;
            step->observed_head = NULL;
            step->observed_preflight_phase = 1;
            break;
        case 1: /* Find the declared EDB; even absent entries cost a visit. */
            if (step->observed_preflight_slot >= plan->edb_count)
                return ENOTSUP;
            if (!plan->edb_relations
                || !plan->edb_relations[step->observed_preflight_slot]) {
                step->observed_preflight_slot++;
                break;
            }
            step->observed_preflight_phase = 2;
            break;
        case 2: {
            size_t byte = step->observed_preflight_byte;
            char actual =
                plan->edb_relations[step->observed_preflight_slot][byte];
            char wanted = source[byte];
            if (actual != wanted) {
                step->observed_preflight_slot++;
                step->observed_preflight_byte = 0;
                step->observed_preflight_phase = 1;
            } else if (!wanted) {
                uint32_t edb = step->observed_preflight_slot;
                if (!plan->edb_declared_width || !plan->edb_column_type_counts
                    || !plan->edb_column_types
                    || plan->edb_declared_width[edb] != 1
                    || plan->edb_column_type_counts[edb] != 1
                    || !plan->edb_column_types[edb]
                    || plan->edb_column_types[edb][0] != WIRELOG_TYPE_INT64)
                    return ENOTSUP;
                step->observed_preflight_edb = edb;
                step->observed_input_name_length = byte;
                step->observed_preflight_byte = 0;
                step->observed_preflight_phase = 3;
            } else {
                if (byte == SIZE_MAX) return EOVERFLOW;
                step->observed_preflight_byte++;
            }
            break;
        }
        case 3: /* Establish a finite bound for the immutable head name too. */
            if (rule->name[step->observed_preflight_byte]) {
                if (step->observed_preflight_byte == SIZE_MAX) return EOVERFLOW;
                step->observed_preflight_byte++;
            } else {
                step->observed_head_name_length = step->observed_preflight_byte;
                step->observed_preflight_slot = 0;
                step->observed_preflight_byte = 0;
                step->observed_preflight_phase = 4;
            }
            break;
        case 4: /* One registry pass, comparing each candidate with the source. */
        case 6: /* A second pass resolves the existing/absent public head. */
            if (step->observed_preflight_registry != sess->rels
                || step->observed_preflight_registry_count != sess->nrels)
                return EINVAL;
            if (step->observed_preflight_slot == sess->nrels) {
                if (step->observed_preflight_phase == 4) {
                    if (!step->observed_input) return ENOTSUP;
                    step->observed_preflight_slot = 0;
                    step->observed_preflight_phase = 6;
                } else {
                    if ((step->observed_head != NULL) !=
                        step->successor_attempt)
                        return ENOTSUP;
                    step->observed_preflight_phase = 8;
                }
                break;
            }
            if (!sess->rels || !sess->rels[step->observed_preflight_slot]
                || !sess->rels[step->observed_preflight_slot]->name) {
                step->observed_preflight_slot++;
                break;
            }
            step->observed_preflight_phase++;
            break;
        case 5:
        case 7: {
            bool input = step->observed_preflight_phase == 5;
            col_rel_t *candidate = sess->rels[step->observed_preflight_slot];
            size_t byte = step->observed_preflight_byte;
            char wanted = (input ? source : rule->name)[byte];
            char actual = candidate->name[byte];
            if (wanted == actual && wanted) {
                if (byte == SIZE_MAX) return EOVERFLOW;
                step->observed_preflight_byte++;
                break;
            }
            if (wanted == actual) {
                if (candidate->nrows > candidate->capacity
                    || !((candidate->ncols == 1
                    && (!candidate->column_types || candidate->column_types[0]
                    == WIRELOG_TYPE_INT64))
                    || (input && !candidate->nrows && !candidate->ncols
                    && candidate->declared_ncols == 1))) return ENOTSUP;
                if (input && !step->observed_input) {
                    step->observed_input = candidate;
                    step->observed_input_identity =
                        candidate->relation_identity;
                }
                if (!input &&
                    !step->observed_head) step->observed_head = candidate;
            }
            step->observed_preflight_slot++;
            step->observed_preflight_byte = 0;
            step->observed_preflight_phase--;
            break;
        }
        case 8: return wl_evaluation_control_charge_attempt(control,
                       step->operation_id, 0);
        default: return EINVAL;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_session_test_observed_preflight_checkpoint)
            wl_columnar_session_test_observed_preflight_checkpoint(control,
                step->observed_preflight_phase, step->observed_preflight_byte);
#endif
    }
    int rc = wl_evaluation_control_charge_attempt(control, step->operation_id,
            0);
    return rc ? rc : step->observed_preflight_phase == 8 ? 0 : EAGAIN;
}

static uint32_t
col_controlled_step_default_preflight_quantum(void)
{
#ifdef WL_SESSION_TEST_HOOKS
    unsigned quantum = wl_columnar_session_test_preflight_quantum;
    if (!quantum || quantum > 64) return 64;
    return quantum;
#else
    return 64;
#endif
}

static int
col_controlled_step_default_preflight_action(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    const wl_plan_t *plan = step->default_preflight_plan;
    const wl_plan_stratum_t *stratum;
    const wl_plan_relation_t *relation;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc = wl_evaluation_control_charge_attempt(control, step->operation_id,
            1);
    if (rc) return rc;
    step->default_preflight_charged_actions++;

    switch (step->default_preflight_phase) {
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_PLAN:
        if (!plan || !plan->strata || (step->default_preflight_registry_count
            && !step->default_preflight_registry))
            return ENOTSUP;
        step->default_preflight_phase = plan->stratum_count
            ? COL_CONTROLLED_DEFAULT_PREFLIGHT_STRATUM
            : COL_CONTROLLED_DEFAULT_PREFLIGHT_READY;
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_STRATUM:
        if (step->default_preflight_stratum >= plan->stratum_count) {
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_READY;
            return 0;
        }
        stratum = &plan->strata[step->default_preflight_stratum];
        if ((stratum->is_recursive && !step->recursive_route)
            || (stratum->relation_count && !stratum->relations))
            return ENOTSUP;
        step->default_preflight_relation = 0;
        step->default_preflight_phase = stratum->relation_count
            ? COL_CONTROLLED_DEFAULT_PREFLIGHT_RELATION
            : COL_CONTROLLED_DEFAULT_PREFLIGHT_NEXT_RELATION;
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_RELATION:
        stratum = &plan->strata[step->default_preflight_stratum];
        if (step->default_preflight_relation >= stratum->relation_count) {
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_NEXT_RELATION;
            return 0;
        }
        relation = &stratum->relations[step->default_preflight_relation];
        if (!relation->name || !relation->ops || !relation->op_count
            || relation->ops[relation->op_count - 1].op
            != (step->recursive_route ? WL_PLAN_OP_EXCHANGE
                                      : WL_PLAN_OP_CONSOLIDATE))
            return ENOTSUP;
        step->default_preflight_op = 0;
        step->default_preflight_phase =
            COL_CONTROLLED_DEFAULT_PREFLIGHT_OPERATOR;
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_OPERATOR:
        stratum = &plan->strata[step->default_preflight_stratum];
        relation = &stratum->relations[step->default_preflight_relation];
        if (step->default_preflight_op >= relation->op_count) {
            step->default_preflight_registry_slot = 0;
            step->default_preflight_name_byte = 0;
            step->default_preflight_target_found = false;
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_SLOT;
            return 0;
        }
        if (!col_controlled_step_controlled_op(
                &relation->ops[step->default_preflight_op])
            && !(step->recursive_route
            && relation->ops[step->default_preflight_op].op
            == WL_PLAN_OP_EXCHANGE))
            return ENOTSUP;
        step->default_preflight_op++;
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_SLOT:
        if (step->default_preflight_registry_slot
            >= step->default_preflight_registry_count) {
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_EXPECTATION;
            return 0;
        }
        if (!step->default_preflight_registry
            || !step->default_preflight_registry[
                step->default_preflight_registry_slot]
            || !step->default_preflight_registry[
                step->default_preflight_registry_slot]->name) {
            step->default_preflight_registry_slot++;
            step->default_preflight_name_byte = 0;
            return 0;
        }
        step->default_preflight_phase =
            COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_BYTE;
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_BYTE: {
        stratum = &plan->strata[step->default_preflight_stratum];
        relation = &stratum->relations[step->default_preflight_relation];
        col_rel_t *candidate = step->default_preflight_registry[
            step->default_preflight_registry_slot];
        if (step->default_preflight_name_byte == UINT32_MAX)
            return ENOTSUP;
        uint32_t offset = step->default_preflight_name_byte;
        char wanted = relation->name[offset];
        char actual = candidate->name[offset];
        if (wanted != actual) {
            step->default_preflight_registry_slot++;
            step->default_preflight_name_byte = 0;
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_SLOT;
        } else if (!wanted) {
            step->default_preflight_target_found = true;
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_EXPECTATION;
        } else {
            step->default_preflight_name_byte++;
        }
        return 0;
    }
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_EXPECTATION: {
        bool committed_prefix = step->default_preflight_stratum
            < step->stratum_index
            || (step->default_preflight_stratum == step->stratum_index
            && step->default_preflight_relation < step->relation_index)
            || (step->cursor_advance_pending
            && step->default_preflight_stratum == step->stratum_index
            && step->default_preflight_relation == step->relation_index)
            || step->complete
            || (step->recursive_route && step->provenance_valid)
            || (step->successor_attempt
            && step->default_preflight_stratum == 0
            && step->default_preflight_relation == 0);
        if (step->default_preflight_target_found != committed_prefix)
            return ENOTSUP;
        step->default_preflight_phase =
            COL_CONTROLLED_DEFAULT_PREFLIGHT_NEXT_RELATION;
        return 0;
    }
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_NEXT_RELATION:
        stratum = &plan->strata[step->default_preflight_stratum];
        if (++step->default_preflight_relation < stratum->relation_count) {
            step->default_preflight_op = 0;
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_RELATION;
        } else {
            step->default_preflight_stratum++;
            step->default_preflight_relation = 0;
            step->default_preflight_phase =
                COL_CONTROLLED_DEFAULT_PREFLIGHT_STRATUM;
        }
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_READY:
        return 0;
    case COL_CONTROLLED_DEFAULT_PREFLIGHT_IDLE:
    default:
        return EINVAL;
    }
}

static int
col_controlled_step_default_preflight(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    uint32_t actions = 0;
    uint32_t quantum = col_controlled_step_default_preflight_quantum();
    if (step->default_preflight_phase ==
        COL_CONTROLLED_DEFAULT_PREFLIGHT_IDLE) {
        int rc = wl_evaluation_control_charge_attempt(control,
                step->operation_id, 1);
        if (rc) return rc;
        step->default_preflight_charged_actions++;
        actions++;
        step->default_preflight_entry_charged = true;
        step->default_preflight_plan = sess->plan;
        step->default_preflight_registry = sess->rels;
        step->default_preflight_registry_count = sess->nrels;
        step->default_preflight_phase =
            COL_CONTROLLED_DEFAULT_PREFLIGHT_PLAN;
    } else if (step->default_preflight_plan != sess->plan
        || step->default_preflight_registry != sess->rels
        || step->default_preflight_registry_count != sess->nrels) {
        return ENOTSUP;
    }
    while (actions < quantum && step->default_preflight_phase
        != COL_CONTROLLED_DEFAULT_PREFLIGHT_READY) {
        int rc = col_controlled_step_default_preflight_action(sess, step);
        if (rc) return rc;
        actions++;
    }
    step->default_preflight_call_actions = actions;
    int poll_rc = wl_evaluation_control_charge_attempt(control,
            step->operation_id, 0);
    if (poll_rc) return poll_rc;
    if (actions == quantum) return EAGAIN;
    return step->default_preflight_phase
           == COL_CONTROLLED_DEFAULT_PREFLIGHT_READY ? 0 : EAGAIN;
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_columnar_session_test_preflight_snapshot(wl_session_t *session,
    wl_columnar_session_test_preflight_state_t *out)
{
    if (!session || !out) return EINVAL;
    const wl_col_session_t *sess = COL_SESSION(session);
    const wl_columnar_controlled_step_t *step = sess->controlled_step;
    if (!step) return ENOENT;
    *out = (wl_columnar_session_test_preflight_state_t){
        .phase = (uint32_t)step->default_preflight_phase,
        .stratum = step->default_preflight_stratum,
        .relation = step->default_preflight_relation,
        .op = step->default_preflight_op,
        .registry_slot = step->default_preflight_registry_slot,
        .name_byte = step->default_preflight_name_byte,
        .eval_stratum = step->stratum_index,
        .eval_relation = step->relation_index,
        .call_actions = step->default_preflight_call_actions,
        .charged_actions = step->default_preflight_charged_actions,
        .ready = step->default_preflight_phase
            == COL_CONTROLLED_DEFAULT_PREFLIGHT_READY,
        .cursor_advance_pending = step->cursor_advance_pending,
        .target_name_byte_active = step->default_preflight_phase
            == COL_CONTROLLED_DEFAULT_PREFLIGHT_TARGET_BYTE,
        .entry_charged = step->default_preflight_entry_charged
    };
    return 0;
}
#endif

static int
col_controlled_step_preflight(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    const wl_plan_t *plan = sess->plan;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    uint64_t id = step->operation_id;
    bool batch = sess->join_batch_bytes != 0;
    if (!batch) return col_controlled_step_default_preflight(sess, step);
    int initial_charge = wl_evaluation_control_charge_attempt(control, id, 1);
    if (initial_charge) return initial_charge;
    if (wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    if (step->preflight_phase == COL_CONTROLLED_PREFLIGHT_IDLE) {
        step->preflight_registry = sess->rels;
        step->preflight_registry_count = sess->nrels;
    } else if (step->preflight_registry != sess->rels
        || step->preflight_registry_count != sess->nrels) {
        return ENOTSUP;
    }
    const wl_plan_relation_t *relation = plan->stratum_count == 1
        && plan->strata[0].relation_count == 1
        ? &plan->strata[0].relations[0] : NULL;
    if (!relation || !col_controlled_step_batch_shape(sess)) return ENOTSUP;
    if (!relation->name || !relation->ops || relation->op_count != 5)
        return ENOTSUP;
    const char *lookup_name = NULL;
    bool delta_name = false;
    for (uint32_t action = 1; action < 64; ++action) {
        int rc = wl_evaluation_control_charge_attempt(control, id, 1);
        if (rc) return rc;
        if (wl_evaluation_control_active_attempt_id(control) != id)
            return EINVAL;
        if (step->preflight_phase == COL_CONTROLLED_PREFLIGHT_IDLE)
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_SHAPE;
        if (step->preflight_phase == COL_CONTROLLED_PREFLIGHT_SHAPE) {
            if (step->preflight_op < relation->op_count) {
                const wl_plan_op_t *op = &relation->ops[step->preflight_op];
                if (!col_controlled_step_controlled_op(op)) return ENOTSUP;
                if (step->preflight_op == 0
                    && (op->op != WL_PLAN_OP_VARIABLE
                    || (op->delta_mode != WL_DELTA_AUTO
                    && op->delta_mode != WL_DELTA_FORCE_FULL)))
                    return ENOTSUP;
                if (step->preflight_op == 1
                    && (op->op != WL_PLAN_OP_JOIN || op->materialized
                    || !op->right_relation || !op->key_count
                    || !op->left_keys || !op->right_keys)) return ENOTSUP;
                if (step->preflight_op == 1
                    && op->delta_mode != WL_DELTA_AUTO
                    && op->delta_mode != WL_DELTA_FORCE_FULL)
                    return ENOTSUP;
                if (step->preflight_op == 2
                    && (op->op != WL_PLAN_OP_FILTER
                    || !op->filter_expr.data || !op->filter_expr.size))
                    return ENOTSUP;
                if (step->preflight_op == 3
                    && op->op != WL_PLAN_OP_MAP) return ENOTSUP;
                if (step->preflight_op == 4
                    && op->op != WL_PLAN_OP_CONSOLIDATE) return ENOTSUP;
                ++step->preflight_op;
                continue;
            }
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_TARGET;
            continue;
        }
        switch (step->preflight_phase) {
        case COL_CONTROLLED_PREFLIGHT_TARGET:
            lookup_name = relation->name;
            break;
        case COL_CONTROLLED_PREFLIGHT_VARIABLE_FULL:
            lookup_name = relation->ops[0].relation_name;
            break;
        case COL_CONTROLLED_PREFLIGHT_VARIABLE_DELTA:
            lookup_name = relation->ops[0].relation_name;
            delta_name = true;
            break;
        case COL_CONTROLLED_PREFLIGHT_JOIN_FULL:
            lookup_name = relation->ops[1].right_relation;
            break;
        case COL_CONTROLLED_PREFLIGHT_JOIN_DELTA:
            lookup_name = relation->ops[1].right_relation;
            delta_name = true;
            break;
        case COL_CONTROLLED_PREFLIGHT_READY:
            if (step->preflight_registry != sess->rels
                || step->preflight_registry_count != sess->nrels
                || step->preflight_variable_full == NULL
                || step->preflight_join_full == NULL
                || step->preflight_variable_full->nrows
                != step->preflight_variable_full_rows
                || (step->preflight_variable_delta
                && step->preflight_variable_delta->nrows
                != step->preflight_variable_delta_rows)
                || step->preflight_join_full->nrows
                != step->preflight_join_full_rows
                || (step->preflight_join_delta
                && step->preflight_join_delta->nrows
                != step->preflight_join_delta_rows)) return ENOTSUP;
            return 0;
        default:
            return ENOTSUP;
        }
        if (!lookup_name || !lookup_name[0] || lookup_name[0] == '$')
            return ENOTSUP;
        col_rel_t *found = NULL;
        bool finished = false;
        while (step->preflight_registry_slot < sess->nrels) {
            col_rel_t *candidate = sess->rels
                ? sess->rels[step->preflight_registry_slot] : NULL;
            const char *candidate_name = candidate ? candidate->name : NULL;
            if (!candidate_name) {
                ++step->preflight_registry_slot;
                step->preflight_name_byte = 0;
                break;
            }
            uint32_t offset = step->preflight_name_byte;
            char wanted;
            if (delta_name && offset < 3) {
                wanted = offset == 0 ? '$' : 'd';
                if (offset == 2) wanted = '$';
            } else {
                uint32_t base_offset = delta_name ? offset - 3 : offset;
                if (base_offset > 252) return ENOTSUP;
                wanted = lookup_name[base_offset];
                if (base_offset == 252 && wanted) return ENOTSUP;
            }
            char actual = candidate_name[offset];
            if (actual != wanted) {
                ++step->preflight_registry_slot;
                step->preflight_name_byte = 0;
                break;
            }
            if (!wanted) {
                found = candidate;
                ++step->preflight_registry_slot;
                finished = true;
                break;
            }
            ++step->preflight_name_byte;
            break;
        }
        if (step->preflight_registry_slot == sess->nrels)
            finished = true;
        if (!finished) continue;
        step->preflight_registry_slot = 0;
        step->preflight_name_byte = 0;
        if (step->preflight_phase == COL_CONTROLLED_PREFLIGHT_TARGET) {
            if (found) return ENOTSUP;
            step->preflight_phase = batch
                ? COL_CONTROLLED_PREFLIGHT_VARIABLE_FULL
                : COL_CONTROLLED_PREFLIGHT_READY;
        } else if (step->preflight_phase ==
            COL_CONTROLLED_PREFLIGHT_VARIABLE_FULL) {
            step->preflight_variable_full = found;
            if (!found) return ENOTSUP;
            step->preflight_variable_full_rows = found->nrows;
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_VARIABLE_DELTA;
        } else if (step->preflight_phase ==
            COL_CONTROLLED_PREFLIGHT_VARIABLE_DELTA) {
            step->preflight_variable_delta = found;
            step->preflight_variable_delta_rows = found ? found->nrows : 0;
            wl_delta_mode_t mode = relation->ops[0].delta_mode;
            if (mode != WL_DELTA_AUTO && mode != WL_DELTA_FORCE_FULL)
                return ENOTSUP;
            if (mode == WL_DELTA_AUTO && found && found->nrows
                && found->nrows < step->preflight_variable_full->nrows)
                return ENOTSUP;
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_JOIN_FULL;
        } else if (step->preflight_phase ==
            COL_CONTROLLED_PREFLIGHT_JOIN_FULL) {
            step->preflight_join_full = found;
            if (!found) return ENOTSUP;
            step->preflight_join_full_rows = found->nrows;
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_JOIN_DELTA;
        } else if (step->preflight_phase ==
            COL_CONTROLLED_PREFLIGHT_JOIN_DELTA) {
            step->preflight_join_delta = found;
            step->preflight_join_delta_rows = found ? found->nrows : 0;
            wl_delta_mode_t mode = relation->ops[1].delta_mode;
            if (mode != WL_DELTA_AUTO && mode != WL_DELTA_FORCE_FULL)
                return ENOTSUP;
            if (mode == WL_DELTA_AUTO && found && found->nrows
                && found->nrows < step->preflight_join_full->nrows)
                return ENOTSUP;
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_READY;
            step->controlled_materialized_batch_fallback = true;
        }
        delta_name = false;
    }
    if (step->preflight_phase == COL_CONTROLLED_PREFLIGHT_READY) return 0;
    int poll_rc = wl_evaluation_control_charge_attempt(control, id, 0);
    return poll_rc ? poll_rc : EAGAIN;
}

static void
col_controlled_step_grant_release(wl_columnar_controlled_step_t *step,
    wl_evaluation_control_t **grant)
{
    if (!grant || !*grant) return;
    if (wl_evaluation_control_active_attempt_id(*grant))
        (void)wl_evaluation_control_finish(*grant, step, 0, 0);
    (void)wl_evaluation_control_detach(*grant, step);
    wl_evaluation_control_release(*grant);
    *grant = NULL;
}

static int
col_controlled_step_grant_begin(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step, wl_evaluation_control_t **out,
    uint64_t *out_id)
{
    *out = NULL; *out_id = 0;
#ifdef WL_SESSION_TEST_HOOKS
    if ((!step->primary_result
        && wl_columnar_session_test_fail_framed_funding_before_stop > 0)
        || (step->primary_result
        && wl_columnar_session_test_fail_framed_funding_after_stop > 0)) {
        if (step->primary_result)
            --wl_columnar_session_test_fail_framed_funding_after_stop;
        else
            --wl_columnar_session_test_fail_framed_funding_before_stop;
        return EAGAIN;
    }
#endif
    wl_evaluation_control_t *grant = NULL;
    /* Recursive capture adds a bound child-begin transition to the relation
     * host's finite call chain. Keep that protocol allowance explicit; all
     * evaluator work remains charged to the independent session operation. */
    uint64_t host_quanta = step->recursive_route || step->general_observed
        ? 8 : 4;
    int rc = wl_evaluation_control_create(host_quanta * UINT64_C(384),
            &grant);
    if (rc) return EAGAIN;
    rc = wl_evaluation_control_attach(grant, step);
    if (!rc) rc = wl_evaluation_control_begin(grant, step);
    if (rc) {
        if (wl_evaluation_control_active_attempt_id(grant))
            (void)wl_evaluation_control_finish(grant, step, rc, 0);
        (void)wl_evaluation_control_detach(grant, step);
        wl_evaluation_control_release(grant);
        return EAGAIN;
    }
    *out = grant;
    *out_id = wl_evaluation_control_active_attempt_id(grant);
    return *out_id ? 0 : EAGAIN;
}

static bool
col_controlled_step_receipt_consumed(wl_col_session_t *sess,
    const wl_columnar_controlled_step_t *step)
{
    if (!step->previous_valid) return false;
    if (sess->cleanup_active) {
        wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
        if (wl_columnar_eval_stack_framed_context(sess->cleanup_active, sess,
            &ctx) == 0 && ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING)
            return true;
    }
    return !sess->cleanup_active
           && step->previous_kind ==
           WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT;
}

static int
col_controlled_step_host_action(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step, const wl_plan_relation_t *relation,
    wl_columnar_eval_serial_framed_host_action_t action)
{
    wl_evaluation_control_t *grant = NULL;
    uint64_t grant_id = 0;
    int rc = col_controlled_step_grant_begin(sess, step, &grant, &grant_id);
    if (rc) return rc;
    col_controlled_recursive_step_t *recursive = step->recursive;
    bool ordinary_capture = step->general_observed && step->transaction
        && step->transaction->child_active;
    bool capture_only = ordinary_capture || (step->recursive_route && recursive
        && recursive->body_active);
    bool consume = action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE
        && col_controlled_step_receipt_consumed(sess, step);
    wl_columnar_eval_serial_framed_host_request_t request = {
        .funding = grant,
        .funding_id = grant_id,
        .operation = sess->base.evaluation_control,
        .operation_id = step->operation_id,
        .operation_identity = step->operation_identity,
        .controlled_materialized_batch_fallback =
            step->controlled_materialized_batch_fallback,
        .private_bindings = ordinary_capture ? &step->transaction->bindings
            : capture_only ? &recursive->body_bindings : NULL,
        .capture_only = capture_only,
        .capture_out = ordinary_capture
            ? &step->transaction->heads[step->transaction->head].candidate
            : capture_only ? &recursive->body_result : NULL,
        .action = action,
        .receipt_key = ordinary_capture ? step->transaction->receipt_key
            : (wl_columnar_eval_cleanup_receipt_key_t){0},
        .previous_valid = step->previous_valid,
        .previous_kind = step->previous_kind,
        .previous_protocol = step->previous_protocol,
        .previous_funding = step->previous_funding,
        .previous_funding_id = step->previous_funding_id,
        .reply = &step->reply
    };
    int protocol = wl_columnar_eval_serial_framed_relation(relation, sess,
            false, &request);
    int finish_rc = wl_evaluation_control_finish(grant, step, 0, 0);
    if (finish_rc && !protocol) protocol = finish_rc;

    if (action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS) {
        col_controlled_step_grant_release(step, &grant);
        return protocol;
    }

    if (!consume || !protocol) {
        col_controlled_step_grant_release(step,
            &step->previous_funding);
        step->previous_valid = !consume
            && (step->reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
            || step->reply.kind ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT);
        step->previous_kind = step->reply.kind;
        step->previous_protocol = protocol;
        step->previous_funding_id = grant_id;
        if (step->previous_valid) {
            step->previous_funding = grant;
            grant = NULL;
        }
    }
    col_controlled_step_grant_release(step, &grant);
    return protocol;
}

static void
col_controlled_step_advance_cursor(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    const wl_plan_t *plan = sess->plan;
    if (!sess->join_batch_bytes) {
        /* Publication is complete, but the next relation cursor is advanced
         * by the charged scheduler. Preserve this marker if that charge stops. */
        step->cursor_advance_pending = true;
    } else if (++step->relation_index
        >= plan->strata[step->stratum_index].relation_count) {
        /* The admitted batch shape has exactly one relation in one stratum. */
        step->relation_index = 0;
        step->stratum_index = plan->stratum_count;
        step->complete = true;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (!step->complete &&
        wl_columnar_session_test_exhaust_after_relation > 0) {
        --wl_columnar_session_test_exhaust_after_relation;
        /* A finite configured test limit makes this a real sticky stop at
         * the exact publication boundary, without timing or worker races. */
        (void)wl_evaluation_control_charge(sess->base.evaluation_control,
            UINT64_MAX);
    }
#endif
}

static int
col_controlled_step_successor_publish_candidate(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    const char *public_name = sess->plan->strata[0].relations[0].name;
    col_rel_t *input = step->observed_input;
    col_rel_t *old_head = step->observed_head;
    if (!step->successor_input_reader.owner || !input || !old_head
        || input->relation_identity != step->successor_input_identity
        || input->view_generation != step->successor_input_view_generation
        || input->storage_generation
        != step->successor_input_storage_generation
        || old_head->relation_identity != step->successor_head_identity
        || old_head->view_generation != step->successor_head_view_generation
        || old_head->storage_generation
        != step->successor_head_storage_generation)
        return EINVAL;
    if (!step->successor_candidate) return ENOENT;
    if (!step->successor_candidate_detached) {
        while (step->successor_candidate_scan_index < sess->nrels) {
            uint32_t i = step->successor_candidate_scan_index;
            int charge_rc = wl_evaluation_control_charge(control, 1);
            if (charge_rc)
                return charge_rc;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_session_test_successor_registry_charge)
                wl_columnar_session_test_successor_registry_charge(control,
                    1, i);
#endif
            if (sess->rels[i] != step->successor_candidate)
                step->successor_candidate_scan_index++;
            else {
                sess->rels[i] = NULL;
                session_rel_free_hash(sess);
                step->successor_candidate_detached = true;
                break;
            }
        }
        if (!step->successor_candidate_detached)
            return ENOENT;
    }
    if (!step->successor_candidate_renamed) {
        while (public_name[step->successor_rename_name_bytes_charged]) {
            int charge_rc = wl_evaluation_control_charge(control, 1);
            if (charge_rc)
                return charge_rc;
            step->successor_rename_name_bytes_charged++;
        }
        int charge_rc = wl_evaluation_control_charge(control, 1);
        if (charge_rc)
            return charge_rc;
        int rename_rc = wl_columnar_relation_rename_checked(
            step->successor_candidate, public_name);
        if (rename_rc)
            return rename_rc;
        step->successor_candidate_renamed = true;
    }
    while (step->successor_registration_scan_charged < sess->nrels) {
        int charge_rc = wl_evaluation_control_charge(control, 1);
        if (charge_rc)
            return charge_rc;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_session_test_successor_registry_charge)
            wl_columnar_session_test_successor_registry_charge(control, 2,
                step->successor_registration_scan_charged);
#endif
        step->successor_registration_scan_charged++;
    }
    int charge_rc = wl_evaluation_control_charge(control, 1);
    if (charge_rc)
        return charge_rc;
    charge_rc = wl_evaluation_control_charge(control, 0);
    if (charge_rc)
        return charge_rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_before_successor_registration)
        wl_columnar_session_test_before_successor_registration(old_head);
#endif
    session_rel_registration_t registration;
    session_rel_registration_init(&registration);
    int rc = session_rel_registration_prepare_expected(sess,
            step->successor_candidate, old_head, step->successor_head_identity,
            step->successor_head_view_generation,
            step->successor_head_storage_generation, &registration);
    if (rc) {
        step->successor_registration_scan_charged = 0;
        return rc;
    }
    session_rel_registration_commit(sess, step->successor_candidate,
        &registration);
    step->observed_head = step->successor_candidate;
    step->observed_head_identity = step->observed_head->relation_identity;
    step->successor_candidate = NULL;
    step->successor_candidate_detached = false;
    int release_rc = col_rel_source_reader_release(
        &step->successor_input_reader);
    assert(release_rc == 0);
    return 0;
}

static int
col_controlled_step_finish(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step, int result)
{
    int rc = wl_evaluation_control_finish(sess->base.evaluation_control,
            sess->base.operation_admission, result, 0);
    step->preflight_phase = COL_CONTROLLED_PREFLIGHT_IDLE;
    step->preflight_op = 0;
    step->preflight_registry_slot = 0;
    step->preflight_name_byte = 0;
    step->preflight_registry_count = 0;
    step->preflight_registry = NULL;
    step->preflight_variable_full = NULL;
    step->preflight_variable_delta = NULL;
    step->preflight_join_full = NULL;
    step->preflight_join_delta = NULL;
    step->preflight_variable_full_rows = 0;
    step->preflight_variable_delta_rows = 0;
    step->preflight_join_full_rows = 0;
    step->preflight_join_delta_rows = 0;
    step->controlled_materialized_batch_fallback = false;
    step->default_preflight_phase = COL_CONTROLLED_DEFAULT_PREFLIGHT_IDLE;
    step->default_preflight_plan = NULL;
    step->default_preflight_registry = NULL;
    step->default_preflight_stratum = 0;
    step->default_preflight_relation = 0;
    step->default_preflight_op = 0;
    step->default_preflight_registry_slot = 0;
    step->default_preflight_name_byte = 0;
    step->default_preflight_registry_count = 0;
    step->default_preflight_call_actions = 0;
    step->default_preflight_charged_actions = 0;
    step->default_preflight_target_found = false;
    step->default_preflight_entry_charged = false;
    step->operation_active = false;
    step->operation_id = 0;
    step->relation_active = false;
    step->primary_result = 0;
    step->refused = result != 0
        && result != WL_EVALUATION_CONTROL_WORK_LIMIT;
    return rc;
}

/* Resolve the committed result without triggering a lazy hash rebuild.
 * The session operation admission keeps registry and names stable. */
static int
col_controlled_step_observed_result(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    if (step->observed_result_ready) return 0;
    const char *name = step->successor_attempt ? step->successor_private_name
        : sess->plan->strata[0].relations[0].name;
    while (step->observed_result_slot < sess->nrels) {
        int rc = wl_evaluation_control_charge_attempt(
            sess->base.evaluation_control, step->operation_id, 1);
        if (rc) return rc;
        col_rel_t *r = sess->rels[step->observed_result_slot];
        if (r && r->name) {
            size_t byte = step->observed_result_byte;
            char actual = r->name[byte], wanted = name[byte];
            if (actual == wanted && wanted) {
                if (byte == SIZE_MAX) return EOVERFLOW;
                step->observed_result_byte++;
                continue;
            }
            if (actual == wanted) {
                if (step->successor_attempt) step->successor_candidate = r;
                else {
                    step->observed_head = r;
                    step->observed_head_identity = r->relation_identity;
                }
                step->observed_result_ready = true;
                return wl_evaluation_control_charge_attempt(
                    sess->base.evaluation_control, step->operation_id, 0);
            }
        }
        step->observed_result_slot++;
        step->observed_result_byte = 0;
    }
    return ENOENT;
}

/* The removal producer owns one $r$ source at a time. Resolve that exact source
 * under STEP accounting, then retain reversible writer claims until cutoff. */
static int
col_controlled_step_staging_prepare(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    if (step->staging_ready) return 0;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    if (!step->successor_attempt || !sess->last_removed_relation) {
        step->staging_ready = true;
        return 0;
    }
    if (!step->observed_input
        || sess->last_removed_relation != step->observed_input->name)
        return ENOTSUP;
    while (!step->staging_retirement.storage.identity) {
        int rc = wl_evaluation_control_charge_attempt(control,
                step->operation_id, 1);
        if (rc) return rc;
        if (step->staging_slot == sess->nrels) return EINVAL;
        col_rel_t *r = sess->rels[step->staging_slot];
        if (!r || !r->name) {
            step->staging_slot++; continue;
        }
        size_t byte = step->staging_byte;
        char expected = byte < 3 ? "$r$"[byte]
            : step->observed_input->name[byte - 3];
        char actual = r->name[byte];
        if (actual != expected) {
            step->staging_slot++; step->staging_byte = 0;
        }else if (actual) step->staging_byte++;
        else {
            step->staging_snapshot = wl_columnar_relation_snapshot(r);
            rc = wl_columnar_relation_scalar_retirement_begin(
                &step->staging_retirement, r);
            if (rc) return rc;
        }
    }
    int rc = wl_columnar_relation_scalar_retirement_prepare(
        &step->staging_retirement, control, step->operation_id);
    if (rc) return rc;
    if (!wl_columnar_relation_snapshot_equal(step->staging_snapshot,
        wl_columnar_relation_snapshot(
            step->staging_retirement.storage.relation)))
        return EINVAL;
    if (!step->staging_maintenance) {
        rc = wl_evaluation_control_create(0, &step->staging_maintenance);
        if (rc) return rc;
        rc = wl_evaluation_control_attach(step->staging_maintenance, step);
        if (!rc) rc = wl_evaluation_control_begin(step->staging_maintenance,
                    step);
        if (rc) {
            (void)wl_evaluation_control_detach(step->staging_maintenance, step);
            wl_evaluation_control_release(step->staging_maintenance);
            step->staging_maintenance = NULL;
            return rc;
        }
        step->staging_maintenance_id = wl_evaluation_control_active_attempt_id(
            step->staging_maintenance);
    }
    step->staging_ready = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_staging_prepared)
        wl_columnar_session_test_staging_prepared(control);
#endif
    return wl_evaluation_control_charge_attempt(control, step->operation_id, 0);
}

static void
col_controlled_step_staging_end(wl_columnar_controlled_step_t *step)
{
    if (!step->staging_maintenance) return;
    (void)wl_evaluation_control_finish(step->staging_maintenance, step, 0, 0);
    (void)wl_evaluation_control_detach(step->staging_maintenance, step);
    wl_evaluation_control_release(step->staging_maintenance);
    step->staging_maintenance = NULL;
}

static int
col_controlled_step_staging_finish(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    if (step->staging_done) return 0;
    if (!step->successor_attempt) {
        step->staging_done = true; return 0;
    }
    if (step->staging_retirement.storage.identity) {
        if (!step->staging_retirement.committed) {
            if (step->staging_slot >= sess->nrels
                || sess->rels[step->staging_slot]
                != step->staging_retirement.storage.relation) return EINVAL;
            int rc = wl_columnar_relation_scalar_retirement_commit(
                &step->staging_retirement);
            if (rc) return rc;
            sess->rels[step->staging_slot] = NULL;
            session_rel_free_hash(sess);
            sess->last_removed_relation = NULL;
            sess->retraction_seeded = false;
        }
        int rc = wl_columnar_relation_scalar_retirement_step(
            &step->staging_retirement, step->staging_maintenance,
            step->staging_maintenance_id);
        if (rc) return rc;
        col_controlled_step_staging_end(step);
    }
    /* The legacy observed domain has at most four slots after a removal and
     * private candidate. Visits were prepaid before publication. No live slot
     * moves, so the cached input/head proofs remain valid. */
    while (sess->nrels && !sess->rels[sess->nrels - 1]) sess->nrels--;
    session_rel_free_hash(sess);
    sess->last_inserted_relation = NULL;
    sess->pending_input_change = false;
    sess->pending_full_input_eval = false;
    sess->has_evaluated = true;
    col_rel_t *input = step->observed_input;
    if (input)
        input->base_nrows = input->nrows;
    step->staging_done = true;
    return 0;
}

static int
col_controlled_step_observer_drive(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc = 0;
    if (step->published) goto cleanup;
    int lookup_rc = col_controlled_step_observed_result(sess, step);
    if (lookup_rc) return lookup_rc;
    if (!step->observer_evaluated) {
        wl_columnar_eval_delta_observer_set_active(sess, false);
        wl_columnar_eval_delta_observer_set_evaluated(sess);
        step->observer_evaluated = true;
    }
    while (step->observer_prepare_discard) {
        int rc = wl_columnar_eval_delta_observer_prepare_discard(sess);
        if (rc == EAGAIN) continue;
        if (rc) return rc;
        step->observer_prepare_discard = false;
        step->observer_preparing = false;
    }
    if (!step->observer_prepared) {
        if (!step->observer_preparing) {
            if (step->successor_attempt && !step->successor_candidate)
                return ENOENT;
            int rc = step->successor_attempt
                ? wl_columnar_eval_delta_observer_prepare_start_with_override(
                sess, sess->plan->strata[0].relations[0].name,
                step->successor_candidate)
                : wl_columnar_eval_delta_observer_prepare_start(sess);
            if (rc) return rc;
            step->observer_preparing = true;
        }
        for (;;) {
            bool ready = false;
            int rc = wl_columnar_eval_delta_observer_prepare_advance(sess,
                    control, step->operation_id, &ready);
            if (rc == EAGAIN) continue;
            if (rc == WL_EVALUATION_CONTROL_CANCELLED) {
                step->observer_prepare_discard = true;
                return rc;
            }
            if (rc == WL_EVALUATION_CONTROL_WORK_LIMIT)
                return rc;
            if (rc) {
                step->observer_prepare_discard = true;
                return rc;
            }
            if (!ready) continue;
            step->observer_preparing = false;
            step->observer_prepared = true;
            break;
        }
    }
    size_t event_count = wl_columnar_eval_delta_observer_event_count(sess);
#ifdef WL_SESSION_TEST_HOOKS
    if (!step->delivery_precharge_index)
        wl_columnar_session_test_observer_precharged_events = 0;
#endif
    while (step->delivery_precharge_index < event_count) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        step->delivery_precharge_index++;
#ifdef WL_SESSION_TEST_HOOKS
        wl_columnar_session_test_observer_precharged_events =
            step->delivery_precharge_index;
#endif
    }
    rc = col_controlled_step_staging_prepare(sess, step);
    if (rc) return rc;
    if (!step->staging_tail_prepaid) {
        rc = wl_evaluation_control_charge(control,
                step->successor_attempt ? sess->nrels : 0);
        if (rc) return rc;
        step->staging_tail_prepaid = true;
    }
    if (!step->published) {
        if (step->successor_attempt) {
            rc = col_controlled_step_successor_publish_candidate(sess, step);
            if (rc) return rc;
        } else {
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        }
        step->published = true;
        wl_columnar_eval_delta_observer_publish(sess);
        step->observer_cleanup_pending = true;
    }
cleanup:
    if (step->observer_cleanup_pending) {
        int cleanup_rc = col_controlled_step_staging_finish(sess, step);
        if (cleanup_rc) return cleanup_rc;
        rc = wl_columnar_eval_delta_observer_finish(sess);
        if (rc) return rc;
        step->observer_cleanup_pending = false;
    }
    return 0;
}

static int
col_controlled_recursive_checkpoint(wl_col_session_t *sess,
    col_controlled_recursive_step_t *recursive, unsigned phase)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_recursive_step_checkpoint)
        wl_columnar_session_test_recursive_step_checkpoint(
            sess->base.evaluation_control, phase, recursive->iterations);
#else
    (void)phase;
#endif
    return wl_evaluation_control_charge_attempt(
        sess->base.evaluation_control, recursive->operation_id, 0);
}

static void
col_controlled_recursive_fail(col_controlled_recursive_step_t *recursive,
    int result)
{
    if (!recursive->primary_result)
        recursive->primary_result = result ? result : EINVAL;
    recursive->phase = COL_CONTROLLED_RECURSIVE_ABORT;
}

static int
col_controlled_recursive_charge(wl_col_session_t *sess,
    col_controlled_recursive_step_t *recursive)
{
    return wl_evaluation_control_charge_attempt(
        sess->base.evaluation_control, recursive->operation_id, 1);
}

static int
col_controlled_recursive_dispose(col_rel_t **relation)
{
    if (!relation || !*relation) return 0;
    int rc = col_rel_destroy_checked(*relation);
    if (!rc) *relation = NULL;
    return rc;
}

static int
col_controlled_recursive_new_named_binary(wl_col_session_t *sess,
    col_controlled_recursive_step_t *recursive, const char *name,
    uint32_t capacity, col_rel_t **out)
{
    static const char *const path_columns[] = { "x", "y" };
    int rc;

    if (!sess || !recursive || !name || !out || *out) return EINVAL;
    if (!capacity) capacity = 1;
    if (!recursive->build.identity) {
        rc = wl_columnar_relation_managed_binary_build_begin(
            &recursive->build, capacity, sess->memory_governor);
        if (rc) return rc;
        recursive->allocation_precharge_capacity = capacity;
        recursive->allocation_name = name;
    } else if (recursive->allocation_precharge_capacity != capacity
        || !recursive->allocation_name
        || strcmp(recursive->allocation_name, name) != 0) {
        return EINVAL;
    }
    if (recursive->build.phase
        != WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_READY) {
        uint64_t remaining = WL_EVALUATION_CONTROL_QUANTUM;
        bool lane_allocation = recursive->build.phase
            == WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_COLUMN_0
            || recursive->build.phase
            == WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_COLUMN_1
            || recursive->build.phase
            == WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_MERGE_COLUMN_0
            || recursive->build.phase
            == WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_MERGE_COLUMN_1;
        if (lane_allocation
            && recursive->build.allocation_control_identity
            && !recursive->build.allocation_units_remaining) {
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_session_test_recursive_step_checkpoint)
                wl_columnar_session_test_recursive_step_checkpoint(
                    sess->base.evaluation_control,
                    WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_ADMITTED,
                    recursive->build.capacity);
#endif
            remaining = 1;
        } else if (lane_allocation) {
            remaining = recursive->build.allocation_units_remaining;
            if (remaining > WL_EVALUATION_CONTROL_QUANTUM)
                remaining = WL_EVALUATION_CONTROL_QUANTUM;
        }
        rc = wl_columnar_relation_managed_binary_build_step_budgeted(
            &recursive->build, sess->base.evaluation_control,
            recursive->operation_id, &remaining);
#ifdef WL_SESSION_TEST_HOOKS
        if (rc == EAGAIN && lane_allocation
            && recursive->build.allocation_control_identity
            && wl_columnar_session_test_recursive_step_checkpoint)
            wl_columnar_session_test_recursive_step_checkpoint(
                sess->base.evaluation_control,
                WL_COLUMNAR_SESSION_TEST_RECURSIVE_ALLOCATION_PRECHARGE,
                recursive->build.capacity);
#endif
        if (rc) return rc;
        if (recursive->build.phase
            != WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_READY)
            return EAGAIN;
    }
    if (recursive->allocation_metadata_phase == 0) {
        rc = col_controlled_recursive_charge(sess, recursive);
        if (!rc)
            rc = wl_columnar_relation_managed_binary_build_adopt_schema(
                &recursive->build, path_columns);
        if (rc) return rc;
        recursive->allocation_metadata_phase = 1;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_session_test_recursive_step_checkpoint)
            wl_columnar_session_test_recursive_step_checkpoint(
                sess->base.evaluation_control,
                WL_COLUMNAR_SESSION_TEST_RECURSIVE_SCHEMA_ADOPTED,
                recursive->build.capacity);
#endif
        return EAGAIN;
    }
    if (recursive->allocation_metadata_phase == 1) {
        rc = col_controlled_recursive_charge(sess, recursive);
        if (!rc)
            rc = wl_columnar_relation_rename_checked(
                recursive->build.relation, name);
        if (rc) return rc;
        recursive->allocation_metadata_phase = 2;
    }
    if (recursive->allocation_metadata_phase != 2)
        return EINVAL;
    *out = recursive->build.relation;
    memset(&recursive->build, 0, sizeof(recursive->build));
    recursive->allocation_precharge_capacity = 0;
    recursive->allocation_metadata_phase = 0;
    recursive->allocation_name = NULL;
    return 0;
}

static int
col_controlled_recursive_maintenance_begin(
    col_controlled_recursive_step_t *recursive)
{
    if (recursive->maintenance_active) return 0;
    int rc = wl_evaluation_control_create(0,
            &recursive->maintenance_control);
    if (rc) return rc;
    rc = wl_evaluation_control_attach(recursive->maintenance_control,
            recursive);
    if (!rc) rc = wl_evaluation_control_begin(
            recursive->maintenance_control, recursive);
    if (rc) {
        (void)wl_evaluation_control_detach(recursive->maintenance_control,
            recursive);
        wl_evaluation_control_release(recursive->maintenance_control);
        recursive->maintenance_control = NULL;
        return rc;
    }
    recursive->maintenance_id = wl_evaluation_control_active_attempt_id(
        recursive->maintenance_control);
    recursive->maintenance_active = recursive->maintenance_id != 0;
    return recursive->maintenance_active ? 0 : EINVAL;
}

static void
col_controlled_recursive_maintenance_end(
    col_controlled_recursive_step_t *recursive)
{
    if (!recursive->maintenance_control) return;
    if (wl_evaluation_control_active_attempt_id(
            recursive->maintenance_control))
        (void)wl_evaluation_control_finish(recursive->maintenance_control,
            recursive, 0, 0);
    (void)wl_evaluation_control_detach(recursive->maintenance_control,
        recursive);
    wl_evaluation_control_release(recursive->maintenance_control);
    recursive->maintenance_control = NULL;
    recursive->maintenance_id = 0;
    recursive->maintenance_active = false;
}

static int
col_controlled_recursive_retire_managed(
    col_controlled_recursive_step_t *recursive, col_rel_t **relation)
{
    if (!relation) return EINVAL;
    if (!*relation) return 0;
    if (!recursive->retirement.identity) {
        int rc = wl_columnar_relation_managed_retirement_begin(
            &recursive->retirement, *relation);
        return rc == EBUSY ? EAGAIN : rc ? rc : EAGAIN;
    }
    if (recursive->retirement.relation
        && recursive->retirement.relation != *relation)
        return EINVAL;
    int rc = wl_columnar_relation_managed_retirement_step(
        &recursive->retirement, recursive->maintenance_control,
        recursive->maintenance_id);
    if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
    if (rc) return rc;
    if (recursive->retirement.phase
        != WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_DONE)
        return EAGAIN;
    *relation = NULL;
    memset(&recursive->retirement, 0, sizeof(recursive->retirement));
    return 0;
}

static int
col_controlled_recursive_body_drive(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step,
    col_controlled_recursive_step_t *recursive)
{
    int protocol = col_controlled_step_host_action(sess, step,
            recursive->relation, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE);
    if (protocol == EAGAIN) return EAGAIN;
    if (protocol && step->reply.kind
        == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED) {
        recursive->body_active = false;
        col_controlled_recursive_fail(recursive, protocol);
        return 0;
    }
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL) {
        recursive->body_active = false;
        if (step->reply.execution_result) {
            col_controlled_recursive_fail(recursive,
                step->reply.execution_result);
            return 0;
        }
        if (!recursive->body_result) {
            col_controlled_recursive_fail(recursive, EINVAL);
            return 0;
        }
        recursive->iterations++;
        int rc = col_controlled_recursive_checkpoint(sess, recursive,
                WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_BODY);
        if (rc) {
            col_controlled_recursive_fail(recursive, rc);
            return 0;
        }
        recursive->phase = COL_CONTROLLED_RECURSIVE_BUILD_CANDIDATE;
        return 0;
    }
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING) {
        recursive->body_active = false;
        recursive->receipt_key = step->reply.cleanup_receipt_key;
        recursive->receipt_pending = true;
        if (step->reply.execution_result)
            recursive->primary_result = step->reply.execution_result;
        recursive->phase = COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY;
        return EAGAIN;
    }
    if (step->reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
        || step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT) {
        int status_rc = col_controlled_step_host_action(sess, step,
                recursive->relation,
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS);
        if (status_rc == EAGAIN) return EAGAIN;
        if (status_rc) {
            recursive->body_active = false;
            col_controlled_recursive_fail(recursive, status_rc);
            return 0;
        }
        if (step->reply.status.execution_result
            && !recursive->primary_result) {
            recursive->primary_result =
                step->reply.status.execution_result;
            col_controlled_recursive_fail(recursive,
                recursive->primary_result);
            return 0;
        }
        return EAGAIN;
    }
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED) {
        recursive->body_active = false;
        col_controlled_recursive_fail(recursive,
            protocol ? protocol : ENOTSUP);
        return 0;
    }
    return EAGAIN;
}

static int
col_controlled_recursive_receipt_drive(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step,
    col_controlled_recursive_step_t *recursive)
{
    wl_columnar_eval_serial_framed_host_action_t action =
        recursive->phase == COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY
        ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
        : WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK;
    int rc = col_controlled_step_host_action(sess, step,
            recursive->relation, action);
    if (rc == EAGAIN) return EAGAIN;
    if (rc) return rc;
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING)
        return EAGAIN;
    if (step->reply.kind != WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL)
        return EAGAIN;
    if (step->reply.execution_result && !recursive->primary_result)
        recursive->primary_result = step->reply.execution_result;
    if (recursive->phase == COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY) {
        recursive->phase = COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_ACK;
        return EAGAIN;
    }
    recursive->receipt_pending = false;
    memset(&recursive->receipt_key, 0, sizeof(recursive->receipt_key));
    recursive->phase = recursive->primary_result
        ? COL_CONTROLLED_RECURSIVE_ABORT
        : COL_CONTROLLED_RECURSIVE_BEGIN_MERGE;
    return 0;
}

static int
col_controlled_recursive_abort_body(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step,
    col_controlled_recursive_step_t *recursive)
{
    if (recursive->receipt_pending) {
        recursive->phase = COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY;
        return col_controlled_recursive_receipt_drive(sess, step, recursive);
    }
    if (!recursive->body_active) return 0;
    int protocol = col_controlled_step_host_action(sess, step,
            recursive->relation, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT);
    if (protocol == EAGAIN) return EAGAIN;
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING) {
        recursive->body_active = false;
        recursive->receipt_key = step->reply.cleanup_receipt_key;
        recursive->receipt_pending = true;
        if (step->reply.execution_result && !recursive->primary_result)
            recursive->primary_result = step->reply.execution_result;
        recursive->phase = COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY;
        return EAGAIN;
    }
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL) {
        recursive->body_active = false;
        if (step->reply.execution_result && !recursive->primary_result)
            recursive->primary_result = step->reply.execution_result;
        return 0;
    }
    if (step->reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
        || step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT) {
        int status_rc = col_controlled_step_host_action(sess, step,
                recursive->relation,
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS);
        if (status_rc == EAGAIN) return EAGAIN;
        if (status_rc && status_rc != EBUSY) return status_rc;
        return EAGAIN;
    }
    if (protocol && !recursive->primary_result)
        recursive->primary_result = protocol;
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED)
        recursive->body_active = false;
    return recursive->body_active ? EAGAIN : 0;
}

static int
col_controlled_recursive_release_edge(
    col_controlled_recursive_step_t *recursive)
{
    if (!recursive->edge_held) return 0;
    int rc = wl_columnar_relation_reader_step(recursive->edge,
            &recursive->edge_reader, true, NULL, 0);
    if (!rc) recursive->edge_held = false;
    return rc;
}

static int
col_controlled_recursive_abort_cleanup(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step,
    col_controlled_recursive_step_t *recursive)
{
    int rc = col_controlled_recursive_abort_body(sess, step, recursive);
    if (rc) return rc;
    rc = col_controlled_recursive_release_edge(recursive);
    if (rc) return rc == EAGAIN ? EAGAIN : rc;
    if (recursive->registration) {
        session_rel_registration_discard(recursive->registration);
        free(recursive->registration);
        recursive->registration = NULL;
        recursive->registration_prepared = false;
    }
    if (recursive->merge.identity) {
        if (!recursive->merge_result_taken)
            (void)wl_columnar_managed_candidate_merge_abort(&recursive->merge);
        if (col_controlled_recursive_maintenance_begin(recursive))
            return EAGAIN;
        rc = wl_columnar_managed_candidate_merge_cleanup_step(
            &recursive->merge, recursive->maintenance_control,
            recursive->maintenance_id);
        if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
        if (rc) return rc;
        wl_columnar_managed_candidate_phase_t phase;
        int terminal = 0;
        rc = wl_columnar_managed_candidate_merge_status(&recursive->merge,
                &phase, &terminal);
        if (rc) return rc;
        if (phase != WL_COLUMNAR_MANAGED_CANDIDATE_DONE) return EAGAIN;
        rc = wl_columnar_managed_candidate_merge_ack(&recursive->merge);
        if (rc) return rc;
        recursive->merge_result_taken = false;
    }
    if (col_controlled_recursive_maintenance_begin(recursive))
        return EAGAIN;
    while (recursive->retirement_next < 6) {
        uint32_t slot = recursive->retirement_next;
        col_rel_t **relation = slot == 0 ? &recursive->replacement
            : slot == 1 ? &recursive->delta
            : slot == 2 ? &recursive->old_working
            : slot == 3 ? &recursive->candidate
            : slot == 4 ? &recursive->working
            : &recursive->build.relation;
        bool aliases_working = slot == 2 && *relation
            && *relation == recursive->working;
        bool aliases_copy = slot == 3 && *relation
            && *relation == recursive->copy_destination;
        rc = col_controlled_recursive_retire_managed(recursive, relation);
        if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
        if (rc) return rc;
        if (aliases_working) recursive->working = NULL;
        if (aliases_copy) recursive->copy_destination = NULL;
        recursive->retirement_next++;
        if (*relation) return EAGAIN;
    }
    if (recursive->copy_destination == recursive->publication)
        recursive->publication = NULL;
    if (recursive->copy_destination == recursive->frontier)
        recursive->frontier = NULL;
    rc = col_controlled_recursive_dispose(&recursive->copy_destination);
    if (rc) return rc;
    rc = col_controlled_recursive_dispose(&recursive->body_result);
    if (rc) return rc;
    rc = col_controlled_recursive_dispose(&recursive->publication);
    if (rc) return rc;
    rc = col_controlled_recursive_dispose(&recursive->frontier);
    if (rc) return rc;
    col_controlled_recursive_maintenance_end(recursive);
    recursive->phase = COL_CONTROLLED_RECURSIVE_DONE;
    return 0;
}

static int
col_controlled_step_recursive_drive(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step, bool teardown)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    col_controlled_recursive_step_t *recursive = step->recursive;
    if (!recursive) {
        recursive = calloc(1, sizeof(*recursive));
        if (!recursive)
            return col_controlled_step_finish(sess, step, ENOMEM);
        recursive->identity = (uintptr_t)recursive;
        recursive->operation_id = step->operation_id;
        recursive->plan = sess->plan;
        recursive->relation = &sess->plan->strata[0].relations[0];
        recursive->phase = COL_CONTROLLED_RECURSIVE_INIT;
        recursive->registration = calloc(1, sizeof(*recursive->registration));
        if (!recursive->registration) {
            free(recursive);
            return col_controlled_step_finish(sess, step, ENOMEM);
        }
        session_rel_registration_init(recursive->registration);
        step->recursive = recursive;
    }
    if (recursive->identity != (uintptr_t)recursive
        || recursive->operation_id != step->operation_id
        || recursive->plan != sess->plan
        || recursive->relation != &sess->plan->strata[0].relations[0])
        return col_controlled_step_finish(sess, step, EINVAL);

    if (teardown && !recursive->primary_result)
        recursive->primary_result = WL_EVALUATION_CONTROL_CANCELLED;
    if ((teardown || recursive->phase == COL_CONTROLLED_RECURSIVE_ABORT)
        && !recursive->committed) {
        if (!recursive->primary_result)
            recursive->primary_result = WL_EVALUATION_CONTROL_CANCELLED;
        int cleanup_rc = col_controlled_recursive_abort_cleanup(sess, step,
                recursive);
        if (cleanup_rc == EAGAIN || cleanup_rc == EBUSY) return EAGAIN;
        if (cleanup_rc && !recursive->primary_result)
            recursive->primary_result = cleanup_rc;
        if (cleanup_rc) return EAGAIN;
        int result = recursive->primary_result;
        free(recursive);
        step->recursive = NULL;
        int finish_rc = col_controlled_step_finish(sess, step, result);
        return teardown && finish_rc == WL_EVALUATION_CONTROL_CANCELLED
            ? 0 : finish_rc;
    }

    for (unsigned transitions = 0; transitions < 8; ++transitions) {
        int rc = 0;
        switch (recursive->phase) {
        case COL_CONTROLLED_RECURSIVE_INIT: {
            rc = col_controlled_recursive_charge(sess, recursive);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            bool successor = step->provenance_valid;
            if ((sess->nrels != (successor ? 2u : 1u))
                || !sess->rels || !sess->rels[0]
                || !sess->rels[0]->name
                || strcmp(sess->rels[0]->name, "edge") != 0
                || sess->rels[0]->ncols != 2 || sess->rels[0]->timestamps
                || sess->rels[0]->compound_kind != WIRELOG_COMPOUND_KIND_NONE
                || (sess->rels[0]->column_types
                && (sess->rels[0]->column_types[0] != WIRELOG_TYPE_INT64
                || sess->rels[0]->column_types[1]
                != WIRELOG_TYPE_INT64))) {
                col_controlled_recursive_fail(recursive, ENOTSUP);
                return EAGAIN;
            }
            recursive->edge = sess->rels[0];
            recursive->edge_snapshot =
                wl_columnar_relation_snapshot(recursive->edge);
            if (!wl_columnar_relation_snapshot_valid(
                    recursive->edge_snapshot)) {
                col_controlled_recursive_fail(recursive, EINVAL);
                return EAGAIN;
            }
            rc = wl_columnar_relation_managed_binary_build_begin(
                &recursive->build, 1, sess->memory_governor);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_BUILD_WORKING;
            return EAGAIN;
        }
        case COL_CONTROLLED_RECURSIVE_BUILD_WORKING: {
            rc = wl_columnar_relation_managed_binary_build_step(
                &recursive->build, control, recursive->operation_id);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (recursive->build.phase
                != WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_READY)
                return EAGAIN;
            if (recursive->phase == COL_CONTROLLED_RECURSIVE_BUILD_WORKING) {
                recursive->working = recursive->build.relation;
                recursive->build.relation = NULL;
                memset(&recursive->build, 0, sizeof(recursive->build));
                recursive->phase
                    = COL_CONTROLLED_RECURSIVE_BUILD_INITIAL_FRONTIER;
                continue;
            }
            col_controlled_recursive_fail(recursive, EINVAL);
            return EAGAIN;
        }
        case COL_CONTROLLED_RECURSIVE_BUILD_INITIAL_FRONTIER:
            rc = col_controlled_recursive_new_named_binary(sess, recursive,
                    "$controlled-path-frontier", 1, &recursive->frontier);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_ACQUIRE_EDGE;
            continue;
        case COL_CONTROLLED_RECURSIVE_ACQUIRE_EDGE:
            rc = col_controlled_recursive_charge(sess, recursive);
            if (!rc) rc = wl_columnar_relation_reader_step(recursive->edge,
                        &recursive->edge_reader, false, control,
                        recursive->operation_id);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->edge_held = true;
            if (!wl_columnar_relation_snapshot_equal(
                    recursive->edge_snapshot,
                    wl_columnar_relation_snapshot(recursive->edge))) {
                col_controlled_recursive_fail(recursive, EBUSY);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_BEGIN_BODY;
            return EAGAIN;
        case COL_CONTROLLED_RECURSIVE_BEGIN_BODY: {
            rc = col_controlled_recursive_charge(sess, recursive);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (recursive->body_bindings.identity)
                memset(&recursive->body_bindings, 0,
                    sizeof(recursive->body_bindings));
            static const char *const names[] = { "edge", "path" };
            col_rel_t *const relations[] = { recursive->edge,
                                             recursive->frontier };
            rc = wl_columnar_eval_private_bindings_begin(
                &recursive->body_bindings, sess, recursive->relation,
                control, recursive->operation_id, names, relations, 2);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->body_active = true;
            recursive->phase = COL_CONTROLLED_RECURSIVE_RUN_BODY;
            continue;
        }
        case COL_CONTROLLED_RECURSIVE_RUN_BODY:
            rc = col_controlled_recursive_body_drive(sess, step, recursive);
            if (rc == EAGAIN) return EAGAIN;
            if (recursive->phase == COL_CONTROLLED_RECURSIVE_ABORT)
                return EAGAIN;
            continue;
        case COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_RETRY:
        case COL_CONTROLLED_RECURSIVE_BODY_RECEIPT_ACK:
            rc = col_controlled_recursive_receipt_drive(sess, step,
                    recursive);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            continue;
        case COL_CONTROLLED_RECURSIVE_BUILD_CANDIDATE:
            if (!recursive->build.identity) {
                rc = wl_columnar_relation_managed_binary_build_begin(
                    &recursive->build,
                    recursive->body_result->nrows
                            ? recursive->body_result->nrows : 1u,
                    sess->memory_governor);
                if (rc) {
                    col_controlled_recursive_fail(recursive, rc);
                    return EAGAIN;
                }
            }
            rc = wl_columnar_relation_managed_binary_build_step(
                &recursive->build, control, recursive->operation_id);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (recursive->build.phase
                != WL_COLUMNAR_RELATION_MANAGED_BINARY_BUILD_READY)
                return EAGAIN;
            recursive->candidate = recursive->build.relation;
            recursive->build.relation = NULL;
            memset(&recursive->build, 0, sizeof(recursive->build));
            recursive->copy_source = recursive->body_result;
            recursive->copy_destination = recursive->candidate;
            recursive->copy_row = 0;
            recursive->phase = COL_CONTROLLED_RECURSIVE_COPY_CANDIDATE;
            continue;
        case COL_CONTROLLED_RECURSIVE_COPY_CANDIDATE:
            if (recursive->copy_row < recursive->copy_source->nrows) {
                rc = col_controlled_recursive_charge(sess, recursive);
                if (!rc) {
                    int64_t row[2] = {
                        recursive->copy_source->columns[0][recursive->copy_row],
                        recursive->copy_source->columns[1][recursive->copy_row]
                    };
                    rc = wl_columnar_relation_append_row_controlled(
                        recursive->copy_destination, row, control);
                }
                if (rc == EAGAIN) return EAGAIN;
                if (rc) {
                    col_controlled_recursive_fail(recursive, rc);
                    return EAGAIN;
                }
                recursive->copy_row++;
                return EAGAIN;
            }
            recursive->copy_source = NULL;
            recursive->copy_destination = NULL;
            recursive->phase = COL_CONTROLLED_RECURSIVE_RETIRE_BODY_RESULT;
            continue;
        case COL_CONTROLLED_RECURSIVE_RETIRE_BODY_RESULT:
            rc = col_controlled_recursive_charge(sess, recursive);
            if (!rc) rc = col_controlled_recursive_dispose(
                    &recursive->body_result);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_BEGIN_MERGE;
            continue;
        case COL_CONTROLLED_RECURSIVE_BEGIN_MERGE:
            rc = col_controlled_recursive_charge(sess, recursive);
            if (!rc) rc = wl_columnar_managed_candidate_merge_begin(
                    &recursive->merge, recursive->working,
                    recursive->candidate, control, recursive->operation_id);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_RUN_MERGE;
            return EAGAIN;
        case COL_CONTROLLED_RECURSIVE_RUN_MERGE: {
            uint64_t merge_quantum = WL_EVALUATION_CONTROL_QUANTUM;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_session_test_recursive_step_checkpoint)
                merge_quantum = 1;
#endif
            rc = wl_columnar_managed_candidate_merge_advance(
                &recursive->merge, merge_quantum);
            if (rc == EAGAIN) {
                int checkpoint_rc = col_controlled_recursive_checkpoint(sess,
                        recursive,
                        WL_COLUMNAR_SESSION_TEST_RECURSIVE_DURING_MERGE);
                if (checkpoint_rc)
                    col_controlled_recursive_fail(recursive, checkpoint_rc);
                return EAGAIN;
            }
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            rc = col_controlled_recursive_checkpoint(sess, recursive,
                    WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_MERGE);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_TAKE_MERGE;
            continue;
        }
        case COL_CONTROLLED_RECURSIVE_TAKE_MERGE:
            rc = col_controlled_recursive_charge(sess, recursive);
            if (!rc) rc = wl_columnar_managed_candidate_merge_take_result(
                    &recursive->merge, &recursive->replacement,
                    &recursive->delta);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->merge_result_taken = true;
            recursive->old_working = recursive->working;
            rc = col_controlled_recursive_maintenance_begin(recursive);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_CLEANUP_MERGE;
            continue;
        case COL_CONTROLLED_RECURSIVE_CLEANUP_MERGE: {
            rc = wl_columnar_managed_candidate_merge_cleanup_step(
                &recursive->merge, recursive->maintenance_control,
                recursive->maintenance_id);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            wl_columnar_managed_candidate_phase_t phase;
            int terminal = 0;
            rc = wl_columnar_managed_candidate_merge_status(
                &recursive->merge, &phase, &terminal);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (phase != WL_COLUMNAR_MANAGED_CANDIDATE_DONE)
                return EAGAIN;
            rc = wl_columnar_managed_candidate_merge_ack(&recursive->merge);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->merge_result_taken = false;
            recursive->phase = COL_CONTROLLED_RECURSIVE_RETIRE_OLD_WORKING;
            continue;
        }
        case COL_CONTROLLED_RECURSIVE_RETIRE_OLD_WORKING: {
            bool working_is_old = recursive->old_working
                && recursive->old_working == recursive->working;
            rc = col_controlled_recursive_retire_managed(recursive,
                    &recursive->old_working);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (working_is_old) recursive->working = NULL;
            recursive->phase = COL_CONTROLLED_RECURSIVE_RETIRE_CANDIDATE;
            return EAGAIN;
        }
        case COL_CONTROLLED_RECURSIVE_RETIRE_CANDIDATE:
            rc = col_controlled_recursive_retire_managed(recursive,
                    &recursive->candidate);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            col_controlled_recursive_maintenance_end(recursive);
            recursive->working = recursive->replacement;
            recursive->replacement = NULL;
            recursive->phase = COL_CONTROLLED_RECURSIVE_CHECK_CONVERGENCE;
            continue;
        case COL_CONTROLLED_RECURSIVE_CHECK_CONVERGENCE: {
            rc = col_controlled_recursive_charge(sess, recursive);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (!recursive->delta) {
                col_controlled_recursive_fail(recursive, EINVAL);
                return EAGAIN;
            }
            /* An empty delta can be freed before retirement reaches DONE. */
            uint32_t delta_rows = recursive->retirement.identity
                ? 0 : recursive->delta->nrows;
            rc = col_controlled_recursive_checkpoint(sess, recursive,
                    delta_rows
                    ? WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_MERGE
                    : WL_COLUMNAR_SESSION_TEST_RECURSIVE_CONVERGENCE);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (delta_rows) {
                rc = col_controlled_recursive_new_named_binary(sess,
                        recursive, "$controlled-path-frontier",
                        delta_rows,
                        &recursive->copy_destination);
                if (rc == EAGAIN) return EAGAIN;
                if (rc) {
                    col_controlled_recursive_fail(recursive, rc);
                    return EAGAIN;
                }
                recursive->copy_source = recursive->delta;
                recursive->copy_row = 0;
                recursive->phase = COL_CONTROLLED_RECURSIVE_COPY_FRONTIER;
                return EAGAIN;
            }
            rc = col_controlled_recursive_maintenance_begin(recursive);
            if (!rc) rc = col_controlled_recursive_retire_managed(recursive,
                        &recursive->delta);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            col_controlled_recursive_maintenance_end(recursive);
            recursive->phase = COL_CONTROLLED_RECURSIVE_DISPOSE_FRONTIER;
            continue;
        }
        case COL_CONTROLLED_RECURSIVE_COPY_FRONTIER:
            if (recursive->copy_source && !recursive->retirement.identity
                && recursive->copy_row < recursive->copy_source->nrows) {
                rc = col_controlled_recursive_charge(sess, recursive);
                if (!rc) {
                    int64_t row[2] = {
                        recursive->copy_source->columns[0][recursive->copy_row],
                        recursive->copy_source->columns[1][recursive->copy_row]
                    };
                    rc = wl_columnar_relation_append_row_controlled(
                        recursive->copy_destination, row, control);
                }
                if (rc == EAGAIN) return EAGAIN;
                if (rc) {
                    col_controlled_recursive_fail(recursive, rc);
                    return EAGAIN;
                }
                recursive->copy_row++;
                return EAGAIN;
            }
            /* Resumed retirement must not read the retired delta alias. */
            recursive->copy_source = NULL;
            rc = col_controlled_recursive_maintenance_begin(recursive);
            if (!rc) rc = col_controlled_recursive_retire_managed(recursive,
                        &recursive->delta);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (!rc) col_controlled_recursive_maintenance_end(recursive);
            if (!rc) rc = col_controlled_recursive_dispose(
                    &recursive->frontier);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->frontier = recursive->copy_destination;
            recursive->copy_destination = NULL;
            recursive->copy_source = NULL;
            recursive->copy_row = 0;
            recursive->phase = COL_CONTROLLED_RECURSIVE_BEGIN_BODY;
            return EAGAIN;
        case COL_CONTROLLED_RECURSIVE_DISPOSE_FRONTIER:
            rc = col_controlled_recursive_charge(sess, recursive);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            rc = col_controlled_recursive_dispose(&recursive->frontier);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->phase = COL_CONTROLLED_RECURSIVE_BUILD_PUBLICATION;
            continue;
        case COL_CONTROLLED_RECURSIVE_BUILD_PUBLICATION: {
            rc = col_controlled_recursive_charge(sess, recursive);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            rc = col_controlled_recursive_new_named_binary(sess, recursive,
                    "path", recursive->working->nrows,
                    &recursive->publication);
            if (rc == EAGAIN) return EAGAIN;
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            recursive->copy_source = recursive->working;
            recursive->copy_destination = recursive->publication;
            recursive->copy_row = 0;
            recursive->phase = COL_CONTROLLED_RECURSIVE_COPY_PUBLICATION;
            continue;
        }
        case COL_CONTROLLED_RECURSIVE_COPY_PUBLICATION:
            if (recursive->copy_row < recursive->copy_source->nrows) {
                rc = col_controlled_recursive_charge(sess, recursive);
                if (!rc) {
                    int64_t row[2] = {
                        recursive->copy_source->columns[0][recursive->copy_row],
                        recursive->copy_source->columns[1][recursive->copy_row]
                    };
                    rc = wl_columnar_relation_append_row_controlled(
                        recursive->copy_destination, row, control);
                }
                if (rc == EAGAIN) return EAGAIN;
                if (rc) {
                    col_controlled_recursive_fail(recursive, rc);
                    return EAGAIN;
                }
                recursive->copy_row++;
                return EAGAIN;
            }
            recursive->copy_source = NULL;
            recursive->copy_destination = NULL;
            recursive->phase = COL_CONTROLLED_RECURSIVE_PUBLISH;
            continue;
        case COL_CONTROLLED_RECURSIVE_PUBLISH:
            if (!recursive->registration_prepared) {
                rc = col_controlled_recursive_charge(sess, recursive);
                if (!rc && (recursive->publication->nrows
                    != recursive->working->nrows
                    || recursive->publication->ncols != 2
                    || recursive->publication->schema_ok == false))
                    rc = EINVAL;
                if (!rc) {
                    recursive->publication->sorted_nrows =
                        recursive->publication->nrows;
                    recursive->publication->run_count =
                        recursive->publication->nrows ? 1u : 0u;
                    if (recursive->publication->run_count)
                        recursive->publication->run_ends[0] =
                            recursive->publication->nrows;
                }
                if (!rc) rc = session_rel_registration_prepare(sess,
                            recursive->publication, recursive->registration);
                if (rc) {
                    col_controlled_recursive_fail(recursive, rc);
                    return EAGAIN;
                }
                recursive->registration_prepared = true;
                return EAGAIN;
            }
            rc = col_controlled_recursive_charge(sess, recursive);
            if (!rc) rc = col_controlled_recursive_checkpoint(sess,
                        recursive,
                        WL_COLUMNAR_SESSION_TEST_RECURSIVE_BEFORE_COMMIT);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            rc = wl_evaluation_control_charge_attempt(control,
                    recursive->operation_id, 0);
            if (rc) {
                col_controlled_recursive_fail(recursive, rc);
                return EAGAIN;
            }
            if (!wl_columnar_relation_snapshot_equal(
                    recursive->edge_snapshot,
                    wl_columnar_relation_snapshot(recursive->edge))) {
                col_controlled_recursive_fail(recursive, EBUSY);
                return EAGAIN;
            }
            session_rel_registration_commit(sess, recursive->publication,
                recursive->registration);
            step->provenance_edge = recursive->edge;
            step->provenance_edge_snapshot =
                wl_columnar_relation_snapshot(recursive->edge);
            col_rel_t *published_path = session_find_rel(sess, "path");
            step->provenance_path_snapshot = published_path
                ? wl_columnar_relation_snapshot(published_path)
                : (col_relation_snapshot_t){ 0, 0, 0 };
            step->provenance_valid = recursive->edge && published_path
                && wl_columnar_relation_snapshot_valid(
                step->provenance_edge_snapshot)
                && wl_columnar_relation_snapshot_valid(
                step->provenance_path_snapshot);
            step->provenance_invalid = !step->provenance_valid;
            step->retryable = false;
            free(recursive->registration);
            recursive->registration = NULL;
            recursive->registration_prepared = false;
            recursive->publication = NULL;
            sess->last_inserted_relation = NULL;
            sess->pending_input_change = false;
            sess->pending_full_input_eval = false;
            sess->has_evaluated = true;
            sess->uncommitted_strata_done = 0;
            sess->snapshot_stable_valid = true;
            for (uint32_t i = 0; i < sess->nrels; ++i) {
                col_rel_t *rel = sess->rels[i];
                if (rel)
                    rel->base_nrows = rel->nrows;
            }
            session_retired_rows_clear(sess);
            recursive->committed = true;
            recursive->phase = COL_CONTROLLED_RECURSIVE_FINALIZE_COMMITTED;
            continue;
        case COL_CONTROLLED_RECURSIVE_FINALIZE_COMMITTED:
            if (!recursive->postcommit_yielded) {
                recursive->postcommit_yielded = true;
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_session_test_recursive_step_checkpoint)
                    wl_columnar_session_test_recursive_step_checkpoint(
                        control,
                        WL_COLUMNAR_SESSION_TEST_RECURSIVE_AFTER_COMMIT,
                        recursive->iterations);
#endif
                return EAGAIN;
            }
            rc = col_controlled_recursive_release_edge(recursive);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (rc) {
                if (!recursive->primary_result)
                    recursive->primary_result = rc;
                return EAGAIN;
            }
            recursive->phase
                = COL_CONTROLLED_RECURSIVE_RETIRE_FINAL_WORKING;
            continue;
        case COL_CONTROLLED_RECURSIVE_DONE: {
            int result = recursive->primary_result;
            free(recursive->registration);
            free(recursive);
            step->recursive = NULL;
            return col_controlled_step_finish(sess, step, result);
        }
        case COL_CONTROLLED_RECURSIVE_RETIRE_FINAL_WORKING:
            if (col_controlled_recursive_maintenance_begin(recursive))
                return EAGAIN;
            rc = col_controlled_recursive_retire_managed(recursive,
                    &recursive->working);
            if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
            if (rc) {
                if (!recursive->primary_result)
                    recursive->primary_result = rc;
                return EAGAIN;
            }
            col_controlled_recursive_maintenance_end(recursive);
            step->complete = true;
            recursive->phase = COL_CONTROLLED_RECURSIVE_DONE;
            continue;
        case COL_CONTROLLED_RECURSIVE_ABORT:
            break;
        default:
            col_controlled_recursive_fail(recursive, EINVAL);
            return EAGAIN;
        }
    }
    return EAGAIN;
}

static int
col_controlled_step_schedule(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    if (sess->join_batch_bytes) return 0;
    const wl_plan_t *plan = sess->plan;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    uint32_t actions = step->default_preflight_call_actions;
    uint32_t quantum = col_controlled_step_default_preflight_quantum();

    while (actions < quantum && !step->complete) {
        if (step->cursor_advance_pending) {
            int rc = wl_evaluation_control_charge_attempt(control,
                    step->operation_id, 1);
            if (rc) return rc;
            actions++;
            step->default_preflight_call_actions = actions;
            step->relation_index++;
            step->cursor_advance_pending = false;
            if (actions == quantum) break;
        }
        if (step->stratum_index >= plan->stratum_count) {
            step->complete = true;
            break;
        }
        const wl_plan_stratum_t *stratum =
            &plan->strata[step->stratum_index];
        if (step->relation_index < stratum->relation_count) break;
        if (step->relation_index != stratum->relation_count)
            return EINVAL;
        int rc = wl_evaluation_control_charge_attempt(control,
                step->operation_id, 1);
        if (rc) return rc;
        actions++;
        step->default_preflight_call_actions = actions;
        step->stratum_index++;
        step->relation_index = 0;
    }
    int poll_rc = wl_evaluation_control_charge_attempt(control,
            step->operation_id, 0);
    if (poll_rc) return poll_rc;
    return actions == quantum && !step->complete ? EAGAIN : 0;
}

static int
col_controlled_step_current_relation(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step,
    const wl_plan_relation_t **out)
{
    *out = NULL;
    if (step->successor_attempt) {
        if (step->stratum_index == 0 && step->relation_index == 0)
            *out = &step->successor_relation;
        else
            step->complete = true;
        return 0;
    }
    if (step->stratum_index >= sess->plan->stratum_count) {
        step->complete = true;
        return 0;
    }
    const wl_plan_stratum_t *stratum =
        &sess->plan->strata[step->stratum_index];
    if (step->relation_index >= stratum->relation_count
        || !stratum->relations)
        return EINVAL;
    *out = &stratum->relations[step->relation_index];
    return 0;
}

/* General observed STEP keeps public descriptors unchanged until every head,
 * signed event and retirement claim is ready. Each metadata action is bounded
 * by one descriptor, plan op, slot or byte. */
static bool
wl_columnar_session_observed_scalar_type(wirelog_column_type_t type)
{
    return type == WIRELOG_TYPE_INT32 || type == WIRELOG_TYPE_INT64
           || type == WIRELOG_TYPE_UINT32 || type == WIRELOG_TYPE_UINT64
           || type == WIRELOG_TYPE_FLOAT || type == WIRELOG_TYPE_STRING
           || type == WIRELOG_TYPE_BOOL;
}

static int
wl_columnar_session_observed_reserve(wl_col_session_t *sess, uint64_t bytes,
    wl_columnar_memory_reservation_t *token, uint64_t id)
{
    if (!sess->memory_governor) return 0;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
        wl_columnar_memory_governor_ref_get(sess->memory_governor), bytes,
        token, &status, sess->base.evaluation_control, id);
    if (rc) return rc;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
        sess->memory_budget_denied = true;
        return ENOSPC;
    }
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

static int
wl_columnar_session_observed_old_storage_finish(
    wl_columnar_session_observed_transaction_t *tx)
{
    if (!tx->old_physical_freed) {
        free((void *)tx->old_registry);
        free(tx->old_hash_head);
        free(tx->old_hash_next);
        tx->old_registry = NULL;
        tx->old_hash_head = tx->old_hash_next = NULL;
        tx->old_physical_freed = true;
        return EAGAIN;
    }
    bool done = false;
    if (tx->old_registry_held) {
        int rc = wl_columnar_memory_reservation_finish_step(
            &tx->old_registry_credit, true, &done);
        if (rc || !done) return rc ? rc : EAGAIN;
        tx->old_registry_held = false;
    }
    if (tx->old_hash_held) {
        int rc = wl_columnar_memory_reservation_finish_step(
            &tx->old_hash_credit, true, &done);
        if (rc || !done) return rc ? rc : EAGAIN;
        tx->old_hash_held = false;
    }
    return 0;
}

static int
wl_columnar_session_observed_maintenance(
    wl_columnar_session_observed_transaction_t *tx)
{
    if (tx->maintenance) return 0;
    int rc = wl_evaluation_control_create(0, &tx->maintenance);
    if (!rc) rc = wl_evaluation_control_attach(tx->maintenance, tx);
    if (!rc) rc = wl_evaluation_control_begin(tx->maintenance, tx);
    if (rc) {
        if (tx->maintenance) {
            (void)wl_evaluation_control_detach(tx->maintenance, tx);
            wl_evaluation_control_release(tx->maintenance);
            tx->maintenance = NULL;
        }
        return rc;
    }
    tx->maintenance_id =
        wl_evaluation_control_active_attempt_id(tx->maintenance);
    return 0;
}

static int
wl_columnar_session_observed_cancel_claims(
    wl_columnar_session_observed_transaction_t *tx)
{
    if (tx->rollback_index < tx->old_count) {
        wl_columnar_relation_scalar_retirement_t *r =
            &tx->originals[tx->rollback_index].retirement;
        if (r->storage.identity && !r->committed) {
            int rc = wl_columnar_relation_scalar_retirement_cancel(r);
            if (rc) return rc;
            memset(r, 0, sizeof(*r));
        }
        tx->rollback_index++;
        return EAGAIN;
    }
    return 0;
}

static int
wl_columnar_session_observed_child(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    wl_columnar_session_observed_transaction_t *tx = step->transaction;
    wl_columnar_session_observed_head_t *head = &tx->heads[tx->head];
    if (!tx->child_active) {
        if (tx->generation == UINT64_MAX) return EOVERFLOW;
        tx->generation++;
        tx->bindings.identity = (uintptr_t)&tx->bindings;
        tx->bindings.session = sess;
        tx->bindings.plan = &head->plan;
        tx->bindings.control = sess->base.evaluation_control;
        tx->bindings.attempt_id = step->operation_id;
        tx->bindings.count = tx->count;
        tx->bindings.dynamic_entries = tx->entries;
        tx->bindings.generation = tx->generation;
        tx->child_active = true;
        tx->child_result = 0;
    }
    wl_columnar_eval_serial_framed_host_action_t action = tx->receipt_pending
        ? (tx->receipt_ack ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK
            : WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY)
        : WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE;
    int rc = col_controlled_step_host_action(sess, step, &head->plan, action);
    if (rc == EAGAIN) return rc;
    if (step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING){
        tx->receipt_pending = true;
        tx->receipt_key = step->reply.cleanup_receipt_key;
        if (step->reply.execution_result) tx->child_result =
                step->reply.execution_result;
        return EAGAIN;
    }
    if (step->reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL) {
        if (step->reply.execution_result) tx->child_result =
                step->reply.execution_result;
        if ((!tx->receipt_pending && step->reply.cleanup_receipt_key.receipt_id)
            || (tx->receipt_pending && !tx->receipt_ack)) {
            tx->receipt_key = step->reply.cleanup_receipt_key;
            tx->receipt_pending = true;
            tx->receipt_ack = true;
            return EAGAIN;
        }
        tx->receipt_pending = tx->receipt_ack = tx->child_active = false;
        memset(&tx->receipt_key, 0, sizeof(tx->receipt_key));
        col_controlled_step_grant_release(step, &step->previous_funding);
        return tx->child_result;
    }
    if (rc && step->reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED
        && !sess->cleanup_active) {
        tx->child_active = false;
        return rc;
    }
    if (sess->cleanup_active
        && (step->reply.kind == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
        || step->reply.kind ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT)){
        rc = col_controlled_step_host_action(sess, step, &head->plan,
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS);
        if (rc == EAGAIN) return rc;
        if (rc) return rc;
    }
    return EAGAIN;
}

/* The SCC bridge accepts only positive, row-local operations. Projection
 * expressions are checked one column per charged PLAN action below. */
static bool
wl_columnar_session_recursive_positive_op(const wl_plan_op_t *op)
{
    if (op->opaque_data || op->materialized) return false;
    switch (op->op) {
    case WL_PLAN_OP_VARIABLE:
    case WL_PLAN_OP_JOIN:
    case WL_PLAN_OP_CONCAT:
    case WL_PLAN_OP_CONSOLIDATE:
    case WL_PLAN_OP_FILTER: return true;
    case WL_PLAN_OP_MAP:
        return op->project_count && op->project_indices
               && (!op->map_expr_count || (op->map_exprs
               && op->map_expr_count == op->project_count));
    default: return false;
    }
}

static int
wl_columnar_session_recursive_retire(
    wl_columnar_session_observed_transaction_t *tx, col_rel_t **owner,
    wl_evaluation_control_t *control, uint64_t id)
{
    wl_columnar_relation_scalar_retirement_t *r = &tx->round_retirement;
    if (!r->storage.identity) {
        int rc = wl_columnar_relation_scalar_retirement_begin(r, *owner);
        if (rc) return rc;
        tx->round_owner = owner;
    }
    if (tx->round_owner != owner) return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    /* Exercise retained ownership after physical disposal has begun, and
     * after the descriptor is gone but managed credit retirement remains. */
    if (wl_columnar_session_test_observed_transaction_checkpoint &&
        r->committed && tx->round > 0) {
        if (r->phase == 6 && r->column > 0)
            wl_columnar_session_test_observed_transaction_checkpoint(control,
                22, 0, NULL);
        else if (r->phase == 13)
            wl_columnar_session_test_observed_transaction_checkpoint(control,
                23, 0, NULL);
    }
#endif
    if (!r->committed) {
        int rc = wl_columnar_relation_scalar_retirement_prepare(r, control, id);
        if (rc) return rc;
        rc = wl_columnar_relation_scalar_retirement_commit(r);
        if (rc) return rc;
    }
    int rc = wl_columnar_relation_scalar_retirement_step(r, control, id);
    if (rc) return rc;
    *owner = NULL;
    tx->round_owner = NULL;
    memset(r, 0, sizeof(*r));
    return 0;
}

static int
wl_columnar_session_frontier_new(wl_col_session_t *sess,
    wl_columnar_session_observed_head_t *head, uint64_t id)
{
    wl_columnar_relation_scalar_source_t source;
    bool initialized = false;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
        &source, &initialized, control, id);
    if (!rc) rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &source, head->candidate, false);
    if (!rc) rc =
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
            (
            &source, head->plan.name, sess->memory_governor,
            &head->next_frontier);
    return initialized ? wl_columnar_relation_scalar_source_end(&source, rc)
        : rc;
}

static int
wl_columnar_session_observed_action(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    wl_columnar_session_observed_transaction_t *tx = step->transaction;
    const wl_plan_t *plan = sess->plan;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_observed_transaction_checkpoint)
        wl_columnar_session_test_observed_transaction_checkpoint(control,
            tx->phase,
            tx->phase == WL_SESSION_OBSERVED_EVALUATE ? tx->head : tx->index,
            tx->child_active ? &tx->bindings : NULL);
    if (wl_columnar_session_test_observed_transaction_checkpoint
        && tx->round_active && tx->phase == WL_SESSION_OBSERVED_EVALUATE
        && tx->child_active && tx->head + 1 == tx->round_end)
        wl_columnar_session_test_observed_transaction_checkpoint(control, 24,
            tx->round, &tx->bindings);
#endif
    if ((tx->phase < WL_SESSION_OBSERVED_RETIRE
        || tx->phase >= WL_SESSION_RECURSIVE_SEED) &&
        !(tx->phase == WL_SESSION_OBSERVED_EVALUATE && tx->child_active)){
        rc = wl_evaluation_control_charge_attempt(control, step->operation_id,
                tx->phase == WL_SESSION_OBSERVED_PLAN ? 256 : tx->phase ==
                WL_SESSION_OBSERVED_ALLOCATE ||
                tx->phase == WL_SESSION_OBSERVED_ZERO ? 64 :
                tx->phase == WL_SESSION_RECURSIVE_DIFFERENCE ||
                tx->phase == WL_SESSION_RECURSIVE_GATHER ? 4 : 1);
        if (rc) return rc;
    }
    switch (tx->phase) {
    case WL_SESSION_OBSERVED_COUNT: /* Count immutable metadata before allocating stable arrays. */
        if (tx->si == plan->stratum_count) {
            if (tx->head_count > UINT32_MAX - plan->edb_count) return EOVERFLOW;
            tx->count = tx->head_count + plan->edb_count;
            if (!tx->head_count || !tx->count) return ENOTSUP;
            tx->old_count = sess->nrels;
            if (tx->old_count > UINT32_MAX - tx->count) return EOVERFLOW;
            tx->image_cap = tx->count + tx->old_count;
            tx->image_count = tx->count;
            tx->old_registry = sess->rels;
            tx->si = tx->ri = 0;
            tx->phase = WL_SESSION_OBSERVED_ALLOCATE;
            return 0;
        }
        if (!plan->strata) return ENOTSUP;
        const wl_plan_stratum_t *count_stratum = &plan->strata[tx->si];
        if (count_stratum->is_recursive && (!count_stratum->is_monotone
            || count_stratum->has_aggregate ||
            !count_stratum->bodies)) return ENOTSUP;
        if (plan->strata[tx->si].relation_count &&
            !plan->strata[tx->si].relations)
            return EINVAL;
        if (tx->ri == plan->strata[tx->si].relation_count) {
            tx->si++; tx->ri = 0; return 0;
        } else {
            const wl_plan_relation_t *r =
                &plan->strata[tx->si].relations[tx->ri++];
            const wl_plan_body_t *body = count_stratum->is_recursive
                ? &count_stratum->bodies[tx->ri - 1] : NULL;
            const wl_plan_op_t *ops = body ? body->ops : r->ops;
            uint32_t count = body ? body->op_count : r->op_count;
            if (!r->name || !ops || !count
                || ops[count - 1].op != WL_PLAN_OP_CONSOLIDATE
                || (body && (!body->declared_int64 || !body->declared_width)))
                return ENOTSUP;
            if (tx->head_count == UINT32_MAX ||
                count > UINT32_MAX - 2
                || count + 2 > UINT32_MAX - tx->op_count)
                return EOVERFLOW;
            tx->head_count++; tx->op_count += count + 2;
            return 0;
        }
    case WL_SESSION_OBSERVED_ALLOCATE: {
        uint64_t bytes = (uint64_t)tx->count * sizeof(*tx->entries)
            + (uint64_t)tx->head_count *
            (sizeof(*tx->heads) + sizeof(*tx->overrides))
            + (uint64_t)tx->old_count * sizeof(*tx->originals)
            + (uint64_t)tx->op_count *
            (sizeof(*tx->ops) + sizeof(*tx->frontier_sources));
        uint64_t image_bytes = (uint64_t)tx->image_cap * sizeof(*tx->image);
        if (bytes > SIZE_MAX || image_bytes > SIZE_MAX) return EOVERFLOW;
        switch (tx->allocation) {
        case 0:
            wl_columnar_memory_reservation_init(&tx->metadata_credit);
            rc = wl_columnar_session_observed_reserve(sess, bytes + sizeof(*tx),
                    &tx->metadata_credit, step->operation_id);
            if (rc) return rc;
            tx->metadata_admitted = true;
            break;
        case 1:
            wl_columnar_memory_reservation_init(&tx->image_credit);
            rc = wl_columnar_session_observed_reserve(sess, image_bytes,
                    &tx->image_credit, step->operation_id);
            if (rc) return rc;
            tx->image_admitted = true;
            break;
        case 2:
            tx->entries = malloc((size_t)tx->count * sizeof(*tx->entries));
            if (!tx->entries) return ENOMEM;
            break;
        case 3:
            tx->heads = malloc((size_t)tx->head_count * sizeof(*tx->heads));
            if (!tx->heads) return ENOMEM;
            break;
        case 4:
            tx->overrides = malloc((size_t)tx->head_count *
                    sizeof(*tx->overrides));
            if (!tx->overrides) return ENOMEM;
            break;
        case 5:
            if (tx->old_count) {
                tx->originals = malloc((size_t)tx->old_count *
                        sizeof(*tx->originals));
                if (!tx->originals) return ENOMEM;
            }
            break;
        case 6:
            tx->ops = malloc((size_t)tx->op_count * sizeof(*tx->ops));
            if (!tx->ops) return ENOMEM;
            break;
        case 7:
            tx->image = (col_rel_t **)malloc((size_t)image_bytes);
            if (!tx->image) return ENOMEM;
            break;
        case 8:
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_session_test_observed_transaction_checkpoint)
                wl_columnar_session_test_observed_transaction_checkpoint(
                    control,
                    42, tx->allocation, NULL);
#endif
            tx->frontier_sources = malloc((size_t)tx->op_count *
                    sizeof(*tx->frontier_sources));
            if (!tx->frontier_sources) return ENOMEM;
            break;
        default: tx->phase = WL_SESSION_OBSERVED_ZERO; return 0;
        }
        tx->allocation++;
        return 0;
    }
    case WL_SESSION_OBSERVED_ZERO: { /* No ownership-bearing record is live until zeroing completes. */
        unsigned char *data;
        size_t bytes;
        switch (tx->init_array) {
        case 0: data = (unsigned char *)tx->entries;
            bytes = (size_t)tx->count * sizeof(*tx->entries); break;
        case 1: data = (unsigned char *)tx->heads;
            bytes = (size_t)tx->head_count * sizeof(*tx->heads); break;
        case 2: data = (unsigned char *)tx->overrides;
            bytes = (size_t)tx->head_count * sizeof(*tx->overrides); break;
        case 3: data = (unsigned char *)tx->originals;
            bytes = (size_t)tx->old_count * sizeof(*tx->originals); break;
        case 4: data = (unsigned char *)tx->ops;
            bytes = (size_t)tx->op_count * sizeof(*tx->ops); break;
        case 5: data = (unsigned char *)tx->frontier_sources;
            bytes = (size_t)tx->op_count * sizeof(*tx->frontier_sources); break;
        default: tx->phase = WL_SESSION_OBSERVED_PLAN; return 0;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (tx->init_array == 5 && tx->init_byte && tx->init_byte < bytes
            && wl_columnar_session_test_observed_transaction_checkpoint)
            wl_columnar_session_test_observed_transaction_checkpoint(control,
                41, (uint32_t)tx->init_byte, NULL);
#endif
        size_t chunk = bytes - tx->init_byte;
        if (chunk > 64) chunk = 64;
        if (chunk) memset(data + tx->init_byte, 0, chunk);
        tx->init_byte += chunk;
        if (tx->init_byte == bytes) {
            tx->init_array++; tx->init_byte = 0;
        }
        return 0;
    }
    case WL_SESSION_OBSERVED_PLAN: /* Flatten head metadata without moving live records. */
        if (tx->index < plan->edb_count) {
            if (!plan->edb_relations || !plan->edb_relations[tx->index]
                || !plan->edb_declared_width || !plan->edb_column_type_counts
                || !plan->edb_column_types) return ENOTSUP;
            tx->entries[tx->index].name = plan->edb_relations[tx->index];
            tx->index++; return 0;
        }
        if (tx->si == plan->stratum_count) {
            tx->index = tx->other = 0; tx->phase = WL_SESSION_OBSERVED_NAMES;
            return 0;
        }
        if (tx->ri == plan->strata[tx->si].relation_count) {
            tx->si++; tx->ri = 0; return 0;
        } else {
            const wl_plan_relation_t *source =
                &plan->strata[tx->si].relations[tx->ri];
            wl_columnar_session_observed_head_t *head =
                &tx->heads[tx->filled_heads];
            const wl_plan_stratum_t *stratum = &plan->strata[tx->si];
            const wl_plan_body_t *body = stratum->is_recursive
                ? &stratum->bodies[tx->ri] : NULL;
            const wl_plan_op_t *source_ops = body ? body->ops : source->ops;
            uint32_t source_count = body ? body->op_count : source->op_count;
            if (!tx->op) {
                head->stratum = tx->si;
                head->recursive = stratum->is_recursive;
                head->width = body ? body->declared_width : 0;
                head->plan = *source;
                head->plan.op_count = source_count;
                head->plan.ops = tx->ops + tx->filled_ops;
                head->op_base = tx->filled_ops;
                tx->entries[plan->edb_count +
                    tx->filled_heads].name = source->name;
            }
            /* Audit every rewritten slot separately from the preserved body. */
            if (body && tx->audit_op < source->op_count) {
                const wl_plan_op_t *original = &source->ops[tx->audit_op++];
                if (original->op == WL_PLAN_OP_K_FUSION ||
                    original->op == WL_PLAN_OP_LFTJ)
                    return ENOTSUP;
                if (original->op == WL_PLAN_OP_EXCHANGE) {
                    const wl_plan_op_exchange_t *exchange =
                        original->opaque_data;
                    if (!exchange || exchange->num_workers) return ENOTSUP;
                }
                return 0;
            }
            if (!col_controlled_step_controlled_op(
                    &source_ops[tx->op])) return ENOTSUP;
            if (body && !wl_columnar_session_recursive_positive_op(
                    &source_ops[tx->op])) return ENOTSUP;
            const wl_plan_op_t *recursive_op = &source_ops[tx->op];
            if (body && recursive_op->op == WL_PLAN_OP_MAP
                && tx->column < recursive_op->map_expr_count) {
                const wl_plan_expr_buffer_t *expr =
                    &recursive_op->map_exprs[tx->column];
                char expected[32];
                int length = snprintf(expected, sizeof(expected), "col%u",
                        recursive_op->project_indices[tx->column]);
                uint16_t actual = 0;
                if (!expr->data || expr->size < 3 ||
                    expr->data[0] != WL_PLAN_EXPR_VAR)
                    return ENOTSUP;
                memcpy(&actual, expr->data + 1, sizeof(actual));
                if (length < 0 || (uint32_t)length != actual ||
                    expr->size != (uint32_t)actual + 3
                    || memcmp(expr->data + 3, expected,
                    actual) != 0) return ENOTSUP;
                tx->column++; return 0;
            }
            tx->column = 0;
            tx->ops[tx->filled_ops + tx->op] = source_ops[tx->op];
            tx->ops[tx->filled_ops + tx->op].delta_mode = WL_DELTA_FORCE_FULL;
            if (++tx->op == source_count) {
                tx->filled_ops += source_count + 2; tx->filled_heads++;
                tx->op = tx->audit_op = 0; tx->ri++;
            }
            return 0;
        }
    case WL_SESSION_OBSERVED_NAMES: /* Validate names and detect collisions, byte by byte. */
        if (tx->index == tx->count) {
            tx->index = tx->other = 0; tx->phase = WL_SESSION_OBSERVED_LOOKUP;
            return 0;
        }
        if (tx->other == tx->index) {
            char value = tx->entries[tx->index].name[tx->byte];
            if (value) {
                if (tx->byte == SIZE_MAX) {
                    return EOVERFLOW;
                }
                tx->byte++;
                return 0;
            }
            if (!tx->byte) return EINVAL;
            tx->index++; tx->other = 0; tx->byte = 0; return 0;
        } else {
            char a = tx->entries[tx->index].name[tx->byte];
            char b = tx->entries[tx->other].name[tx->byte];
            if (a != b) {
                tx->other++; tx->byte = 0; return 0;
            }
            if (!a) return EINVAL;
            if (tx->byte == SIZE_MAX) return EOVERFLOW;
            tx->byte++; return 0;
        }
    case WL_SESSION_OBSERVED_LOOKUP: /* Match original slots to public bindings or consumed $r$ inputs. */
        if (sess->rels != tx->old_registry ||
            sess->nrels != tx->old_count) return EINVAL;
        if (tx->index == tx->old_count) {
            tx->index = tx->other = tx->op = 0;
            tx->phase = WL_SESSION_OBSERVED_LEASES; return 0;
        } else {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[tx->index];
            original->relation = sess->rels[tx->index];
            col_rel_t *r = original->relation;
            if (!r) {
                tx->index++; return 0;
            }
            if (!r->name) return EINVAL;
            uint32_t limit = tx->op == 1 ? plan->edb_count : tx->count;
            if (tx->other == limit) {
                if (tx->op == 3) {
                    original->preserve = true;
                    original->snapshot = wl_columnar_relation_snapshot(r);
                    tx->index++; tx->other = tx->op = 0; tx->byte = 0;
                    return 0;
                }
                tx->op++; tx->other = tx->op > 1 ? plan->edb_count : 0;
                tx->byte = 0; return 0;
            }
            size_t prefix_length = tx->op == 1 ? 3 : tx->op == 2 ? 5 : 4;
            const char *prefix = tx->op == 1 ? "$r$" : tx->op ==
                2 ? "$out$" : "$in$";
            char wanted = (char)(!tx->op ? tx->entries[tx->other].name[tx->byte]
                : tx->byte < prefix_length ? prefix[tx->byte]
                : tx->entries[tx->other].name[tx->byte - prefix_length]);
            char actual = r->name[tx->byte];
            if (wanted != actual) {
                tx->other++; tx->byte = 0; return 0;
            }
            if (wanted) {
                if (tx->byte == SIZE_MAX) {
                    return EOVERFLOW;
                }
                tx->byte++;
                return 0;
            }
            if (!tx->op && tx->other < plan->edb_count) {
                if (tx->entries[tx->other].relation) return EINVAL;
                tx->entries[tx->other].relation = r;
                tx->entries[tx->other].public_relation = r;
            } else {
                original->retire = true;
                if (!tx->op) {
                    if (tx->entries[tx->other].public_relation) return EINVAL;
                    tx->entries[tx->other].public_relation = r;
                }
            }
            original->snapshot = wl_columnar_relation_snapshot(r);
            tx->index++; tx->other = tx->op = 0; tx->byte = 0;
            return 0;
        }
    case WL_SESSION_OBSERVED_LEASES: /* Acquire stable descriptor and source readers for the original view. */
        if (tx->index == tx->old_count) {
            tx->index = tx->op = 0; tx->phase = WL_SESSION_OBSERVED_SCHEMA;
            return 0;
        }
        if (tx->originals[tx->index].relation) {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[tx->index];
            rc = wl_columnar_relation_reader_step(original->relation,
                    &original->reader, false, control, step->operation_id);
            if (rc) return rc;
            if (!wl_columnar_relation_snapshot_equal(original->snapshot,
                wl_columnar_relation_snapshot(
                    original->relation))) return EINVAL;
        }
        tx->index++; return 0;
    case WL_SESSION_OBSERVED_SCHEMA: /* Declared scalar input schemas and private empty input owners. */
        if (tx->index == plan->edb_count) {
            tx->index = tx->op = tx->other = 0; tx->byte = 0;
            col_rel_t *input = step->append_source;
            tx->append_active = step->append_proven && sess->diff_enabled
                && sess->has_evaluated && sess->pending_input_change
                && !sess->pending_full_input_eval &&
                !sess->last_removed_relation
                && input && sess->last_inserted_relation == input->name
                && input->relation_identity == step->append_identity
                && input->base_nrows == step->append_begin
                && input->base_nrows <= input->nrows;
            tx->phase = tx->append_active ? WL_SESSION_APPEND_AUDIT
                : WL_SESSION_OBSERVED_EVALUATE;
            return 0;
        }
        {
            uint32_t i = tx->index, width = plan->edb_declared_width[i];
            col_rel_t *r = tx->entries[i].relation;
            if (!r || plan->edb_column_type_counts[i] != width
                || (width && !plan->edb_column_types[i])) return ENOTSUP;
            if (tx->op < width) {
                wirelog_column_type_t type = plan->edb_column_types[i][tx->op];
                if (!wl_columnar_session_observed_scalar_type(
                        type)) return ENOTSUP;
                if (r->ncols && (r->ncols != width
                    || (r->column_types ? r->column_types[tx->op] :
                    WIRELOG_TYPE_INT64) != type))
                    return ENOTSUP;
                tx->op++; return 0;
            }
            if ((!r->ncols && width) || !r->schema_ok) {
                if (r->nrows) return ENOTSUP;
                col_rel_t *empty = NULL;
                rc = wl_columnar_relation_new_typed_operator_attempt_controlled(
                    tx->entries[i].name, width, plan->edb_column_types[i],
                    sess->memory_governor, &empty, control, step->operation_id);
                if (rc) return rc;
                tx->entries[i].relation = empty;
            }
            tx->entries[i].snapshot =
                wl_columnar_relation_snapshot(tx->entries[i].relation);
            tx->index++; tx->op = 0; return 0;
        }
    case WL_SESSION_APPEND_AUDIT: {
        /* Complete the whole-host proof before any child runs. Each name
         * byte, source occurrence and projection expression is charged. */
        if (tx->index == tx->head_count) {
            tx->index = tx->op = tx->other = 0; tx->byte = 0;
            tx->phase = WL_SESSION_APPEND_COPY; return 0;
        }
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->index];
        const wl_plan_op_t *op = &head->plan.ops[tx->op];
        bool supported = !head->recursive
            && wl_columnar_session_recursive_positive_op(op)
            && tx->entries[plan->edb_count + tx->index].public_relation;
        /* Only pure projections: expressions may change monotonicity or
         * require a separate qualified incremental representation. */
        if (supported && op->op == WL_PLAN_OP_MAP
            && tx->column < op->map_expr_count) {
            const wl_plan_expr_buffer_t *expr = &op->map_exprs[tx->column];
            char expected[32];
            int length = snprintf(expected, sizeof(expected), "col%u",
                    op->project_indices[tx->column]);
            uint16_t actual = 0;
            if (!expr->data || expr->size < 3
                || expr->data[0] != WL_PLAN_EXPR_VAR) supported = false;
            else {
                memcpy(&actual, expr->data + 1, sizeof(actual));
                if (length < 0 || (uint32_t)length != actual
                    || expr->size != (uint32_t)actual + 3
                    || memcmp(expr->data + 3, expected, actual) != 0)
                    supported = false;
                else {
                    tx->column++; return 0;
                }
            }
        }
        const char *name = op->op == WL_PLAN_OP_VARIABLE ? op->relation_name
            : op->op == WL_PLAN_OP_JOIN ? op->right_relation : NULL;
        if (op->op == WL_PLAN_OP_VARIABLE || op->op == WL_PLAN_OP_JOIN) {
            if (!name || tx->other == plan->edb_count) supported = false;
            if (supported) {
                const wl_columnar_eval_private_dynamic_binding_t *entry =
                    &tx->entries[tx->other];
                char a = name[tx->byte], b = entry->name[tx->byte];
                if (a != b) {
                    tx->other++; tx->byte = 0; return 0;
                }
                if (a) {
                    tx->byte++; return 0;
                }
                const col_rel_t *r = entry->relation;
                if (!r || r->timestamps || r->compound_kind !=
                    WIRELOG_COMPOUND_KIND_NONE || !r->ncols)
                    supported = false;
                if (supported && tx->column < r->ncols) {
                    if (r->column_types &&
                        r->column_types[tx->column] != WIRELOG_TYPE_INT64)
                        supported = false;
                    else {
                        tx->column++; return 0;
                    }
                }
                if (supported && r == step->append_source) {
                    tx->frontier_sources[head->op_base + tx->op] = 1;
                    head->delta_reads++;
#ifdef WL_SESSION_TEST_HOOKS
                    if (wl_columnar_session_test_observed_transaction_checkpoint)


                        wl_columnar_session_test_observed_transaction_checkpoint
                        (
                            control, 43, tx->op, NULL);
#endif
                }
            }
        }
        /* Prove linearity along every evaluated path, not across the UNION
         * alternatives as a whole. Partial maps are inactive after fallback. */
        if (supported) {
            bool changed = tx->frontier_sources[head->op_base + tx->op] != 0;
            if (op->op == WL_PLAN_OP_VARIABLE) {
                if (tx->degree_depth == COL_STACK_MAX) supported = false;
                else tx->degrees[tx->degree_depth++] = changed ? 1 : 0;
            } else if (op->op == WL_PLAN_OP_CONCAT) {
                if (tx->degree_depth < 2) supported = false;
                else {
                    uint8_t right = tx->degrees[--tx->degree_depth];
                    if (right > tx->degrees[tx->degree_depth - 1])
                        tx->degrees[tx->degree_depth - 1] = right;
                }
            } else if (!tx->degree_depth) supported = false;
            else if (op->op == WL_PLAN_OP_JOIN && changed) {
                if (tx->degrees[tx->degree_depth - 1]) supported = false;
                else tx->degrees[tx->degree_depth - 1] = 1;
            }
            if (tx->op + 1 == head->plan.op_count && tx->degree_depth != 1)
                supported = false;
        }
        if (!supported) {
            tx->degree_depth = 0;
            tx->append_active = false;
            tx->index = tx->op = tx->other = tx->column = 0; tx->byte = 0;
            tx->phase = WL_SESSION_OBSERVED_EVALUATE; return 0;
        }
        tx->other = tx->column = 0; tx->byte = 0;
        if (++tx->op == head->plan.op_count) {
            tx->index++; tx->op = tx->degree_depth = 0;
        }
        return 0;
    }
    case WL_SESSION_APPEND_COPY:
        rc = wl_columnar_relation_copy_rows_controlled("$append_delta",
                step->append_source, step->append_begin,
                step->append_source->nrows, sess->memory_governor,
                &tx->append_delta, control);
        if (rc) return rc;
        tx->append_snapshot = wl_columnar_relation_snapshot(tx->append_delta);
        tx->phase = WL_SESSION_APPEND_PLAN; return 0;
    case WL_SESSION_APPEND_PLAN: {
        if (tx->index == tx->head_count) {
            tx->index = 0; tx->phase = WL_SESSION_OBSERVED_EVALUATE; return 0;
        }
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->index];
        if (head->delta_reads) {
            /* Replace terminal consolidate with old-head VARIABLE, CONCAT,
             * CONSOLIDATE. The two extra slots were counted before allocation. */
            wl_plan_op_t *tail = (wl_plan_op_t *)head->plan.ops
                + head->plan.op_count - 1;
            if (tx->byte < 3 * sizeof(*tail)) {
                size_t bytes = 3 * sizeof(*tail) - tx->byte;
                if (bytes > 128) bytes = 128;
                rc = wl_evaluation_control_charge_attempt(control,
                        step->operation_id, bytes);
                if (rc) return rc;
                memset((unsigned char *)tail + tx->byte, 0, bytes);
                tx->byte += bytes; return 0;
            }
            tail[0].op = WL_PLAN_OP_VARIABLE;
            tail[0].relation_name = head->plan.name;
            tail[0].delta_mode = WL_DELTA_FORCE_FULL;
            tail[1].op = WL_PLAN_OP_CONCAT;
            tail[2].op = WL_PLAN_OP_CONSOLIDATE;
            head->plan.op_count += 2;
            wl_columnar_eval_private_dynamic_binding_t *entry =
                &tx->entries[plan->edb_count + tx->index];
            entry->relation = entry->public_relation;
            entry->snapshot = wl_columnar_relation_snapshot(entry->relation);
        }
        tx->index++; tx->byte = 0; return 0;
    }
    case WL_SESSION_OBSERVED_EVALUATE: /* Complete each private child and ACK its cleanup before rebinding. */
        if (tx->head == tx->head_count) {
            tx->phase = WL_SESSION_OBSERVED_BASELINE; return 0;
        }
        if (tx->heads[tx->head].recursive && !tx->round_active) {
            tx->round_begin = tx->head; tx->round_end = tx->head;
            tx->round_index = tx->head; tx->round = 0;
            tx->round_active = true;
            tx->round_frontiers = sess->diff_enabled;
            tx->phase = WL_SESSION_RECURSIVE_RANGE;
            return 0;
        }
        rc = wl_columnar_session_observed_child(sess, step);
        if (rc) return rc;
        if (tx->child_active) return EAGAIN;
        if (!tx->heads[tx->head].candidate) return EINVAL;
        {
            col_rel_t *candidate = tx->heads[tx->head].candidate;
            if (tx->round_active) {
                if (candidate->ncols !=
                    tx->heads[tx->head].width) return ENOTSUP;
                if (++tx->head == tx->round_end) {
                    tx->round_index = tx->round_begin;
                    tx->row = tx->column = 0; tx->round_changed = false;
                    tx->phase = WL_SESSION_RECURSIVE_COMPARE;
                }
                return 0;
            }
            uint32_t slot = plan->edb_count + tx->head;
            tx->entries[slot].relation = candidate;
            tx->entries[slot].snapshot =
                wl_columnar_relation_snapshot(candidate);
            if (!tx->entries[slot].public_relation)
                tx->overrides[tx->missing_count++] =
                    (wl_columnar_eval_delta_binding_t){
                    tx->entries[slot].name, NULL, candidate->ncols
                };
            tx->head++;
            return 0;
        }
    case WL_SESSION_RECURSIVE_RANGE:
        if (tx->round_index < tx->head_count &&
            tx->heads[tx->round_index].stratum
            == tx->heads[tx->round_begin].stratum) {
            tx->round_end = ++tx->round_index; return 0;
        }
        tx->index = tx->round_index = tx->round_begin;
        tx->other = tx->round_begin; tx->op = tx->degree_depth = 0;
        tx->byte = 0;
        tx->phase = tx->round_frontiers ? WL_SESSION_RECURSIVE_AUDIT
            : WL_SESSION_RECURSIVE_SEED;
        if (!tx->round_frontiers) tx->index = 0;
        return 0;
    case WL_SESSION_RECURSIVE_AUDIT: {
        if (tx->index == tx->round_end) {
            tx->index = tx->round_begin;
            tx->frontier_started = true;
            tx->phase = WL_SESSION_RECURSIVE_PLAN; return 0;
        }
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->index];
        const wl_plan_op_t *op = &head->plan.ops[tx->op];
        const char *name = op->op == WL_PLAN_OP_VARIABLE ? op->relation_name
            : op->op == WL_PLAN_OP_JOIN ? op->right_relation : NULL;
        if (name && tx->other < tx->round_end) {
            char a = name[tx->byte];
            char b = tx->heads[tx->other].plan.name[tx->byte];
            if (a != b) {
                tx->other++; tx->byte = 0; return 0;
            }
            if (a) {
                tx->byte++; return 0;
            }
            if (tx->other == UINT32_MAX) return EOVERFLOW;
            tx->frontier_sources[head->op_base + tx->op] = tx->other + 1;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_session_test_observed_transaction_checkpoint)
                wl_columnar_session_test_observed_transaction_checkpoint(
                    control,
                    40, tx->op, NULL);
#endif
        }
        bool recursive = tx->frontier_sources[head->op_base + tx->op] != 0;
        if (op->op == WL_PLAN_OP_VARIABLE) {
            if (tx->degree_depth == COL_STACK_MAX) return ENOBUFS;
            tx->degrees[tx->degree_depth++] = recursive ? 1 : 0;
        } else if (op->op == WL_PLAN_OP_CONCAT) {
            if (tx->degree_depth < 2) return EINVAL;
            uint8_t right = tx->degrees[--tx->degree_depth];
            if (right > tx->degrees[tx->degree_depth - 1])
                tx->degrees[tx->degree_depth - 1] = right;
        } else {
            if (!tx->degree_depth) return EINVAL;
            if (op->op == WL_PLAN_OP_JOIN && recursive) {
                if (tx->degrees[tx->degree_depth - 1]) {
                    /* Whole-SCC fallback; partial maps are inactive and no
                     * child plan has been changed. */
                    tx->round_frontiers = false; tx->index = 0;
                    tx->phase = WL_SESSION_RECURSIVE_SEED; return 0;
                }
                tx->degrees[tx->degree_depth - 1] = 1;
            }
        }
        tx->other = tx->round_begin; tx->byte = 0;
        if (++tx->op == head->plan.op_count) {
            if (tx->degree_depth != 1) return EINVAL;
            tx->op = tx->degree_depth = 0; tx->index++;
        }
        return 0;
    }
    case WL_SESSION_RECURSIVE_PLAN: {
        if (tx->index == tx->round_end) {
            tx->index = 0;
            tx->phase = WL_SESSION_RECURSIVE_SEED; return 0;
        }
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->index];
        wl_plan_op_t *tail = (wl_plan_op_t *)head->plan.ops
            + head->plan.op_count - 1;
        if (tx->byte < 3 * sizeof(*tail)) {
            size_t bytes = 3 * sizeof(*tail) - tx->byte;
            if (bytes > 128) bytes = 128;
            rc = wl_evaluation_control_charge_attempt(control,
                    step->operation_id, bytes);
            if (rc) return rc;
            memset((unsigned char *)tail + tx->byte, 0, bytes);
            tx->byte += bytes; return 0;
        }
        tail[0].op = WL_PLAN_OP_VARIABLE;
        tail[0].relation_name = head->plan.name;
        tail[0].delta_mode = WL_DELTA_FORCE_FULL;
        tail[1].op = WL_PLAN_OP_CONCAT;
        tail[2].op = WL_PLAN_OP_CONSOLIDATE;
        head->plan.op_count += 2;
        tx->index++; tx->byte = 0; return 0;
    }
    case WL_SESSION_RECURSIVE_SEED: {
        /* Start from bottom, never an old public closure. On later rounds
         * ownership moves from candidate to the immutable round input. */
        uint32_t i = tx->round_index;
        if (i == tx->head_count || tx->heads[i].stratum
            != tx->heads[tx->round_begin].stratum) {
            tx->round_end = i;
            tx->head = tx->round_begin;
            tx->phase = WL_SESSION_OBSERVED_EVALUATE;
            return 0;
        }
        wl_columnar_session_observed_head_t *head = &tx->heads[i];
        if (tx->round) {
            head->previous = head->candidate; head->candidate = NULL;
        } else {
            rc = wl_columnar_relation_new_auto_operator_attempt_controlled(
                head->plan.name, head->width, 1, sess->memory_governor,
                &head->previous, control, step->operation_id);
            if (rc) return rc;
        }
        uint32_t slot = plan->edb_count + i;
        tx->entries[slot].relation = head->previous;
        tx->entries[slot].snapshot =
            wl_columnar_relation_snapshot(head->previous);
        tx->round_index++;
        return 0;
    }
    case WL_SESSION_RECURSIVE_COMPARE: {
        if (tx->round_index == tx->round_end) {
            tx->round_index = tx->round_begin;
            tx->phase = WL_SESSION_RECURSIVE_REBIND;
            return 0;
        }
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->round_index];
        col_rel_t *a = head->previous, *b = head->candidate;
        if (!a || !b || a->ncols != head->width || b->ncols != head->width)
            return EINVAL;
        if (tx->column < head->width && tx->row == 0) {
            if ((b->column_types &&
                b->column_types[tx->column] != WIRELOG_TYPE_INT64)
                || b->timestamps ||
                b->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
                return ENOTSUP;
        }
        /* Even unequal cardinalities visit the output schema first. */
        if (tx->row == 0 && tx->column < head->width) {
            tx->column++; return 0;
        }
        if (!tx->row) {
            tx->row = 1; tx->column = 0; return 0;
        }
        if (tx->round_frontiers) {
            tx->column = tx->old_row = tx->new_row = 0;
            tx->phase = WL_SESSION_RECURSIVE_FRONTIER; return 0;
        }
        if (a->nrows != b->nrows) tx->round_changed = true;
        else if (tx->row - 1 < b->nrows) {
            if (a->columns[tx->column][tx->row - 1]
                != b->columns[tx->column][tx->row -
                1]) tx->round_changed = true;
            if (++tx->column == head->width) {
                tx->column = 0; tx->row++;
            }
            return 0;
        }
        tx->round_index++; tx->row = tx->column = 0;
        return 0;
    }
    case WL_SESSION_RECURSIVE_FRONTIER:
        rc = wl_columnar_session_frontier_new(sess,
                &tx->heads[tx->round_index], step->operation_id);
        if (rc) return rc;
        tx->phase = WL_SESSION_RECURSIVE_DIFFERENCE; return 0;
    case WL_SESSION_RECURSIVE_DIFFERENCE: {
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->round_index];
        const col_rel_t *a = head->previous, *b = head->candidate;
        /* Ordering comes from actual terminal controlled CONSOLIDATE outputs
         * (or empty bottom), never caller-supplied sorted-prefix hints. */
        if (tx->new_row == b->nrows) {
            if (head->next_frontier->nrows) tx->round_changed = true;
            tx->round_index++; tx->row = tx->column = 0;
            tx->phase = WL_SESSION_RECURSIVE_COMPARE; return 0;
        }
        if (tx->old_row == a->nrows) {
            tx->column = 0; tx->phase = WL_SESSION_RECURSIVE_GATHER;
            return 0;
        }
        int64_t av = a->columns[tx->column][tx->old_row];
        int64_t bv = b->columns[tx->column][tx->new_row];
        if (av < bv) {
            tx->old_row++; tx->column = 0;
        }else if (av > bv) {
            tx->column = 0; tx->phase = WL_SESSION_RECURSIVE_GATHER;
        } else if (++tx->column == head->width) {
            tx->old_row++; tx->new_row++; tx->column = 0;
        }
        return 0;
    }
    case WL_SESSION_RECURSIVE_GATHER: {
        wl_columnar_session_observed_head_t *head = &tx->heads[tx->round_index];
        head->next_frontier->row_scratch[tx->column] =
            head->candidate->columns[tx->column][tx->new_row];
        if (++tx->column == head->width)
            tx->phase = WL_SESSION_RECURSIVE_EMIT;
        return 0;
    }
    case WL_SESSION_RECURSIVE_EMIT: {
        col_rel_t *r = tx->heads[tx->round_index].next_frontier;
        rc = wl_columnar_relation_append_row_controlled(r, r->row_scratch,
                control);
        if (rc) return rc;
        tx->new_row++; tx->column = 0;
        tx->phase = WL_SESSION_RECURSIVE_DIFFERENCE; return 0;
    }
    case WL_SESSION_RECURSIVE_REBIND: {
        if (tx->round_index == tx->round_end) {
            tx->round_index = tx->round_begin;
            tx->phase = WL_SESSION_RECURSIVE_RETIRE;
            return 0;
        }
        uint32_t i = tx->round_index++, slot = plan->edb_count + i;
        col_rel_t *r = tx->heads[i].candidate;
        tx->entries[slot].relation = r;
        tx->entries[slot].snapshot = wl_columnar_relation_snapshot(r);
        return 0;
    }
    case WL_SESSION_RECURSIVE_RETIRE:
        if (tx->round_index < tx->round_end) {
            if (tx->heads[tx->round_index].previous) {
                rc = wl_columnar_session_recursive_retire(tx,
                        &tx->heads[tx->round_index].previous, control,
                        step->operation_id);
                if (rc) return rc;
            }
            wl_columnar_session_observed_head_t *head =
                &tx->heads[tx->round_index];
            if (head->frontier) {
                rc = wl_columnar_session_recursive_retire(tx, &head->frontier,
                        control, step->operation_id);
                if (rc) return rc;
            }
            if (head->next_frontier) {
                if (tx->round_changed) {
                    head->frontier = head->next_frontier;
                    head->next_frontier = NULL;
                    head->frontier_snapshot =
                        wl_columnar_relation_snapshot(head->frontier);
                } else {
                    rc = wl_columnar_session_recursive_retire(tx,
                            &head->next_frontier,
                            control, step->operation_id);
                    if (rc) return rc;
                }
            }
            if (!tx->round_changed) {
                uint32_t slot = plan->edb_count + tx->round_index;
                if (!tx->entries[slot].public_relation)
                    tx->overrides[tx->missing_count++] =
                        (wl_columnar_eval_delta_binding_t){tx->entries[slot].
                                                           name,
                                                           NULL,
                                                           tx->heads[tx->
                                                               round_index].
                                                           width};
            }
            tx->round_index++; return 0;
        }
        if (tx->round_changed) {
            if (++tx->round >= MAX_ITERATIONS) return EOVERFLOW;
            tx->round_index = tx->round_begin;
            tx->phase = WL_SESSION_RECURSIVE_SEED;
        } else {
            tx->round_active = false;
            tx->head = tx->round_end;
            tx->phase = WL_SESSION_OBSERVED_EVALUATE;
        }
        return 0;
    case WL_SESSION_OBSERVED_BASELINE:
        if (!sess->delta_cb) {
            tx->index = 0; tx->phase = WL_SESSION_OBSERVED_REGISTRY; return 0;
        }
        if (!tx->observer_started) {
            rc = tx->missing_count
                ? wl_columnar_eval_delta_observer_build_start_missing_heads(
                sess,
                UINT64_MAX, tx->overrides, tx->missing_count)
                : wl_columnar_eval_delta_observer_build_start(sess, UINT64_MAX);
            if (rc) return rc;
            tx->observer_started = true;
        }
        rc = wl_columnar_eval_delta_observer_build_advance(sess, control,
                step->operation_id, &tx->observer_ready);
        if (rc) return rc;
        if (!tx->observer_ready) return EAGAIN;
        wl_columnar_eval_delta_observer_set_active(sess, false);
        wl_columnar_eval_delta_observer_set_evaluated(sess);
        tx->index = 0; tx->phase = WL_SESSION_OBSERVED_EVENTS; return 0;
    case WL_SESSION_OBSERVED_EVENTS:
        if (tx->index < tx->head_count) {
            tx->overrides[tx->index] = (wl_columnar_eval_delta_binding_t){
                tx->heads[tx->index].plan.name, tx->heads[tx->index].candidate,
                tx->heads[tx->index].candidate->ncols
            };
            tx->index++; return 0;
        }
        if (!tx->prepare_started) {
            rc =
                wl_columnar_eval_delta_observer_prepare_start_with_bindings(
                sess,
                tx->overrides, tx->head_count);
            if (rc) return rc;
            tx->prepare_started = true;
        }
        rc = wl_columnar_eval_delta_observer_prepare_advance(sess, control,
                step->operation_id, &tx->prepare_ready);
        if (rc) return rc;
        if (!tx->prepare_ready) return EAGAIN;
        tx->index = 0; tx->phase = WL_SESSION_OBSERVED_REGISTRY; return 0;
    case WL_SESSION_OBSERVED_REGISTRY: /* Coordinated complete registry image; no independent registrations. */
        if (tx->index < tx->count) {
            uint32_t i = tx->index++;
            tx->image[i] = tx->entries[i].relation;
            if (i < plan->edb_count) {
                tx->image[i] = tx->entries[i].public_relation;
            }
            return 0;
        }
        if (tx->index - tx->count < tx->old_count) {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[tx->index++ - tx->count];
            if (original->preserve) tx->image[tx->image_count++] =
                    original->relation;
            return 0;
        }
        tx->index = 0; tx->phase = WL_SESSION_OBSERVED_RELEASE_READERS;
        return 0;
    case WL_SESSION_OBSERVED_RELEASE_READERS: /* Relinquish readers only for descriptors to be exclusively retired. */
        if (tx->index == tx->old_count) {
            tx->index = 0; tx->phase = WL_SESSION_OBSERVED_CLAIMS; return 0;
        }
        if (tx->originals[tx->index].retire) {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[tx->index];
            rc = wl_columnar_relation_reader_step(original->relation,
                    &original->reader, true, control, step->operation_id);
            if (rc) return rc;
        }
        tx->index++; return 0;
    case WL_SESSION_OBSERVED_CLAIMS: /* Every claim is reversible until the single publication cutoff. */
        if (tx->index == tx->old_count) {
            rc = wl_columnar_arrangement_diff_attempt_prepare(&tx->differential,
                    sess, control, step->operation_id);
            if (rc) return rc;
            tx->phase = WL_SESSION_OBSERVED_CUTOFF; return 0;
        }
        {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[tx->index];
            if (original->retire) {
                if (!original->retirement.storage.identity) {
                    rc = wl_columnar_relation_scalar_retirement_begin(
                        &original->retirement, original->relation);
                    if (rc) return rc;
                }
                rc = wl_columnar_relation_scalar_retirement_prepare(
                    &original->retirement, control, step->operation_id);
                if (rc) return rc;
                if (!wl_columnar_relation_snapshot_equal(original->snapshot,
                    wl_columnar_relation_snapshot(
                        original->relation))) return EINVAL;
            }
            tx->index++; return 0;
        }
    case WL_SESSION_OBSERVED_CUTOFF: {
        size_t events = wl_columnar_eval_delta_observer_event_count(sess);
        if (tx->delivery < events) {
            tx->delivery++; return 0;
        }
        rc = wl_columnar_session_observed_maintenance(tx);
        if (rc) return rc;
        rc = wl_evaluation_control_charge_attempt(control, step->operation_id,
                (uint64_t)tx->old_count + tx->count + 16
                + (tx->differential.started ? 128 : 0));
        if (rc) return rc;
        rc = wl_evaluation_control_charge_attempt(control, step->operation_id,
                0);
        if (rc) return rc;
        for (uint32_t i = 0; i < tx->old_count; ++i) {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[i];
            if (original->retire) {
                rc =
                    wl_columnar_relation_scalar_retirement_commit(
                    &original->retirement);
                if (rc) abort(); /* READY claims were verified before cutoff. */
            }
        }
        tx->old_hash_head = sess->rel_hash_head;
        tx->old_hash_next = sess->rel_hash_next;
        wl_columnar_memory_reservation_init(&tx->old_registry_credit);
        wl_columnar_memory_reservation_init(&tx->old_hash_credit);
        if (sess->memory_governor) {
            if (sess->rels) {
                if (!wl_columnar_memory_reservation_move(
                        &tx->old_registry_credit,
                        &sess->rels_reservation)) abort();
                tx->old_registry_held = true;
            }
            if (sess->rel_hash_head || sess->rel_hash_next) {
                if (!wl_columnar_memory_reservation_move(&tx->old_hash_credit,
                    &sess->rel_hash_reservation)) abort();
                tx->old_hash_held = true;
            }
        }
        sess->rel_hash_head = sess->rel_hash_next = NULL;
        sess->rel_hash_nbuckets = sess->rel_hash_chain_cap = 0;
        if (sess->memory_governor && !wl_columnar_memory_reservation_move(
                &sess->rels_reservation, &tx->image_credit)) abort();
        wl_columnar_arrangement_diff_attempt_commit(&tx->differential, sess);
        sess->rels = tx->image; tx->image = NULL;
        sess->nrels = tx->image_count; sess->rel_cap = tx->image_cap;
        for (uint32_t i = 0; i < tx->count; ++i) {
            col_rel_t *published = sess->rels[i];
            published->base_nrows = published->nrows;
        }
        sess->last_inserted_relation = sess->last_removed_relation = NULL;
        sess->pending_input_change = sess->pending_full_input_eval = false;
        sess->retraction_seeded = false;
        sess->has_evaluated = true;
        sess->snapshot_stable_valid = true;
        sess->uncommitted_strata_done = 0;
        tx->committed = step->published = true;
        step->observer_cleanup_pending = tx->observer_started;
        tx->index = 0; tx->phase = WL_SESSION_OBSERVED_RETIRE;
        if (tx->observer_started) wl_columnar_eval_delta_observer_publish(sess);
        return 0;
    }
    case WL_SESSION_OBSERVED_RETIRE:
        rc = wl_columnar_arrangement_diff_attempt_drain(&tx->differential,
                sess, tx->maintenance, tx->maintenance_id);
        if (rc) return rc;
        if (tx->index == tx->old_count) {
            tx->index = 0; tx->phase = WL_SESSION_OBSERVED_FINISH; return 0;
        }
        {
            wl_columnar_session_observed_original_t *original =
                &tx->originals[tx->index];
            if (original->retire) {
                rc =
                    wl_columnar_relation_scalar_retirement_step(
                    &original->retirement,
                    tx->maintenance, tx->maintenance_id);
                if (rc) return rc;
            } else if (original->relation) {
                rc = wl_columnar_relation_reader_step(original->relation,
                        &original->reader, true, tx->maintenance,
                        tx->maintenance_id);
                if (rc) return rc;
            }
            tx->index++; return 0;
        }
    case WL_SESSION_OBSERVED_FINISH:
        if (tx->append_delta) {
            rc = wl_columnar_session_recursive_retire(tx, &tx->append_delta,
                    tx->maintenance, tx->maintenance_id);
            if (rc) return rc;
            return 0;
        }
        if (tx->index < plan->edb_count) {
            wl_columnar_eval_private_dynamic_binding_t *entry =
                &tx->entries[tx->index];
            if (entry->relation != entry->public_relation) {
                if (!tx->disposal.storage.identity) {
                    rc =
                        wl_columnar_relation_scalar_retirement_begin(
                        &tx->disposal, entry->relation);
                    if (rc) return rc;
                }
                if (!tx->disposal.committed) {
                    rc =
                        wl_columnar_relation_scalar_retirement_prepare(
                        &tx->disposal,
                        tx->maintenance, tx->maintenance_id);
                    if (rc) return rc;
                    rc =
                        wl_columnar_relation_scalar_retirement_commit(
                        &tx->disposal);
                    if (rc) return rc;
                }
                rc = wl_columnar_relation_scalar_retirement_step(&tx->disposal,
                        tx->maintenance, tx->maintenance_id);
                if (rc) return rc;
                entry->relation = entry->public_relation;
                memset(&tx->disposal, 0, sizeof(tx->disposal));
            }
            tx->index++; return 0;
        }
        rc = wl_columnar_session_observed_old_storage_finish(tx);
        if (rc) return rc;
        tx->cleanup_index = tx->old_count;
        tx->dispose_index = tx->count;
        if (tx->observer_started) {
            rc = wl_columnar_eval_delta_observer_finish(sess);
            if (rc) return rc;
        }
        step->observer_cleanup_pending = false;
        step->complete = true; tx->phase = WL_SESSION_OBSERVED_DONE;
        return 0;
    case WL_SESSION_OBSERVED_DONE: return 0;
    default: return EINVAL;
    }
}

static int wl_columnar_session_observed_dispose(wl_col_session_t *,
    wl_columnar_controlled_step_t *);

static int
wl_columnar_session_observed_drive(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    if (step->transaction_discard) {
        int rc = wl_columnar_session_observed_dispose(sess, step);
        if (rc) return EAGAIN;
        step->transaction_discard = false;
        step->transaction_initialized = false;
        step->transaction_initialized_bytes = 0;
        return col_controlled_step_finish(sess, step, step->primary_result);
    }
    if (!step->transaction) {
        int rc =
            wl_evaluation_control_charge_attempt(sess->base.evaluation_control,
                step->operation_id, 64);
        if (rc) return col_controlled_step_finish(sess, step, rc);
        step->transaction = malloc(sizeof(*step->transaction));
        step->transaction_initialized = false;
        step->transaction_initialized_bytes = 0;
        if (!step->transaction) return col_controlled_step_finish(sess, step,
                       ENOMEM);
    }
    if (!step->transaction_initialized) {
        int rc =
            wl_evaluation_control_charge_attempt(sess->base.evaluation_control,
                step->operation_id, 64);
        if (rc) {
            if (rc != WL_EVALUATION_CONTROL_WORK_LIMIT) {
                free(step->transaction); step->transaction = NULL;
                step->transaction_initialized_bytes = 0;
            }
            return col_controlled_step_finish(sess, step, rc);
        }
        size_t bytes = sizeof(*step->transaction) -
            step->transaction_initialized_bytes;
        if (bytes > 64) bytes = 64;
        memset((unsigned char *)step->transaction +
            step->transaction_initialized_bytes, 0, bytes);
        step->transaction_initialized_bytes += bytes;
        if (step->transaction_initialized_bytes == sizeof(*step->transaction))
            step->transaction_initialized = true;
        return EAGAIN;
    }
    wl_columnar_session_observed_transaction_t *tx = step->transaction;
    for (unsigned action = 0; action < 64; ++action) {
        if (tx->rollback) {
            int cancel_rc = wl_columnar_session_observed_cancel_claims(tx);
            if (cancel_rc) return EAGAIN;
            tx->rollback = false;
            tx->phase = WL_SESSION_OBSERVED_CLAIMS; tx->index = 0;
            if (tx->rollback_result != WL_EVALUATION_CONTROL_WORK_LIMIT
                || tx->differential.started || tx->append_active ||
                tx->frontier_started) {
                step->transaction_discard = true;
                step->primary_result = tx->rollback_result;
                return EAGAIN;
            }
            return col_controlled_step_finish(sess, step, tx->rollback_result);
        }
        int rc = wl_columnar_session_observed_action(sess, step);
        if (rc == EAGAIN || rc == EBUSY) return EAGAIN;
        if (rc) {
            /* An active child must drain on its original attempt before the
             * operation report is closed. The host sees the sticky stop. */
            if (tx->child_active) return EAGAIN;
            if (tx->phase >= WL_SESSION_OBSERVED_CLAIMS &&
                tx->phase <= WL_SESSION_OBSERVED_CUTOFF) {
                tx->rollback = true; tx->rollback_index = 0;
                tx->rollback_result = rc;
                return EAGAIN;
            }
            /* Synchronous JOIN/image builders may own a partial index.
             * Retire the entire private image before retrying a stopped
             * differential attempt; only the published image may survive. */
            if (!tx->committed && (rc != WL_EVALUATION_CONTROL_WORK_LIMIT
                || tx->differential.started || tx->append_active ||
                tx->frontier_started)) {
                step->transaction_discard = true;
                step->primary_result = rc;
                return EAGAIN;
            }
            return col_controlled_step_finish(sess, step, rc);
        }
        if (step->complete) return col_controlled_step_finish(sess, step, 0);
    }
    return EAGAIN;
}

static int
wl_columnar_session_observed_dispose(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step)
{
    wl_columnar_session_observed_transaction_t *tx = step->transaction;
    if (!tx) return 0;
    if (!step->transaction_initialized) {
        free(tx); step->transaction = NULL;
        return 0;
    }
    if (tx->child_active || sess->cleanup_active) return EBUSY;
    int rc = wl_columnar_session_observed_maintenance(tx);
    if (rc) return rc;
    rc = wl_columnar_arrangement_diff_attempt_drain(&tx->differential,
            sess, tx->maintenance, tx->maintenance_id);
    if (rc) return rc;
    uint32_t originals = tx->phase <
        WL_SESSION_OBSERVED_LEASES ? 0 : tx->old_count;
    if (originals > tx->old_count) originals = tx->old_count;
    if (tx->cleanup_index < originals) {
        wl_columnar_session_observed_original_t *original =
            &tx->originals[tx->cleanup_index];
        if (original->reader.descriptor_held) {
            rc = wl_columnar_relation_reader_step(original->relation,
                    &original->reader, true, tx->maintenance,
                    tx->maintenance_id);
            if (rc) return rc;
        }
        if (original->retirement.committed) {
            rc =
                wl_columnar_relation_scalar_retirement_step(
                &original->retirement,
                tx->maintenance, tx->maintenance_id);
            if (rc) return rc;
        }
        if (original->retirement.storage.identity &&
            !original->retirement.committed) {
            rc =
                wl_columnar_relation_scalar_retirement_cancel(
                &original->retirement);
            if (rc) return rc;
        }
        tx->cleanup_index++;
        return EAGAIN;
    }
    if (tx->observer_started && !tx->committed) {
        rc = wl_columnar_eval_delta_observer_build_discard(sess);
        if (rc) return rc;
        rc = wl_columnar_eval_delta_observer_prepare_discard(sess);
        if (rc) return rc;
        wl_columnar_eval_delta_observer_set_active(sess, false);
        rc = wl_columnar_eval_delta_observer_discard(sess);
        if (rc) return rc;
        tx->observer_started = false;
    }
    if (tx->phase >= WL_SESSION_OBSERVED_SCHEMA && tx->heads) {
        if (tx->round_owner) {
            rc = wl_columnar_session_recursive_retire(tx, tx->round_owner,
                    tx->maintenance, tx->maintenance_id);
            if (rc) return rc;
        }
        while (tx->cleanup_round < tx->head_count) {
            col_rel_t **owner = &tx->heads[tx->cleanup_round].previous;
            if (*owner) {
                rc = wl_columnar_session_recursive_retire(tx, owner,
                        tx->maintenance, tx->maintenance_id);
                if (rc) return rc;
            }
            wl_columnar_session_observed_head_t *head =
                &tx->heads[tx->cleanup_round];
            if (head->frontier) {
                rc = wl_columnar_session_recursive_retire(tx, &head->frontier,
                        tx->maintenance, tx->maintenance_id);
                if (rc) return rc;
            }
            if (head->next_frontier) {
                rc = wl_columnar_session_recursive_retire(tx,
                        &head->next_frontier,
                        tx->maintenance, tx->maintenance_id);
                if (rc) return rc;
            }
            tx->cleanup_round++;
            return EAGAIN;
        }
    }
    if (tx->append_delta) {
        rc = wl_columnar_session_recursive_retire(tx, &tx->append_delta,
                tx->maintenance, tx->maintenance_id);
        if (rc) return rc;
        return EAGAIN;
    }
    uint32_t entries = tx->phase < WL_SESSION_OBSERVED_SCHEMA ? 0 : tx->count;
    if (entries > tx->count) entries = tx->count;
    if (tx->dispose_index < entries) {
        if (!tx->entries || !tx->heads) return EINVAL;
        uint32_t i = tx->dispose_index;
        col_rel_t *r = i < sess->plan->edb_count
            ? tx->entries[i].relation != tx->entries[i].public_relation
                ? tx->entries[i].relation : NULL
            : tx->committed ? NULL : tx->heads[i -
                sess->plan->edb_count].candidate;
        if (r) {
            if (!tx->disposal.storage.identity) {
                rc = wl_columnar_relation_scalar_retirement_begin(&tx->disposal,
                        r);
                if (rc) return rc;
            }
            if (!tx->disposal.committed) {
                rc =
                    wl_columnar_relation_scalar_retirement_prepare(
                    &tx->disposal,
                    tx->maintenance, tx->maintenance_id);
                if (rc) return rc;
                rc =
                    wl_columnar_relation_scalar_retirement_commit(
                    &tx->disposal);
                if (rc) return rc;
            }
            rc = wl_columnar_relation_scalar_retirement_step(&tx->disposal,
                    tx->maintenance, tx->maintenance_id);
            if (rc) return rc;
            memset(&tx->disposal, 0, sizeof(tx->disposal));
        }
        tx->dispose_index++;
        return EAGAIN;
    }
    if (tx->committed) {
        rc = wl_columnar_session_observed_old_storage_finish(tx);
        if (rc) return rc;
    }
    free((void *)tx->image); tx->image = NULL;
    if (tx->image_admitted && !tx->committed && sess->memory_governor) {
        bool done = false;
        rc = wl_columnar_memory_reservation_finish_step(&tx->image_credit, true,
                &done);
        if (rc || !done) return rc ? rc : EAGAIN;
        tx->image_admitted = false;
    }
    free(tx->entries); tx->entries = NULL;
    free(tx->heads); tx->heads = NULL;
    free(tx->originals); tx->originals = NULL;
    free(tx->overrides); tx->overrides = NULL;
    free(tx->ops); tx->ops = NULL;
    free(tx->frontier_sources); tx->frontier_sources = NULL;
    if (tx->metadata_admitted && sess->memory_governor) {
        bool done = false;
        rc = wl_columnar_memory_reservation_finish_step(&tx->metadata_credit,
                true, &done);
        if (rc || !done) return rc ? rc : EAGAIN;
    }
    (void)wl_evaluation_control_finish(tx->maintenance, tx, 0, 0);
    (void)wl_evaluation_control_detach(tx->maintenance, tx);
    wl_evaluation_control_release(tx->maintenance);
    free(tx); step->transaction = NULL;
    return 0;
}

static int
col_controlled_step_drive(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step, bool teardown)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    const void *owner = sess->base.operation_admission;
    step->default_preflight_call_actions = 0;
    if (!step->operation_active) {
        step->refused = false;
        int rc = wl_evaluation_control_begin(control, owner);
        if (rc) return rc;
        step->operation_id = wl_evaluation_control_active_attempt_id(control);
        if (!step->operation_id) return EINVAL;
        step->operation_active = true;
        step->primary_result = 0;
    } else if (wl_evaluation_control_active_attempt_id(control)
        != step->operation_id) {
        return EINVAL;
    }

    if (teardown) wl_evaluation_control_request_cancel(control);

    if (step->general_observed)
        return wl_columnar_session_observed_drive(sess, step);

    if (step->observed && !step->complete) {
        int rc = col_controlled_step_observed_preflight(sess, step);
        if (rc == EAGAIN) return rc;
        if (rc == ENOTSUP && (step->observed_preflight_phase == 2
            || step->observed_preflight_phase == 5)) {
            step->general_observed = true;
            return wl_columnar_session_observed_drive(sess, step);
        }
        if (rc) return col_controlled_step_finish(sess, step, rc);
        if (step->successor_attempt) {
            rc = col_controlled_step_successor_target_init(sess, step,
                    &sess->plan->strata[0].relations[0]);
            if (rc == EAGAIN) return rc;
            if (rc) return col_controlled_step_finish(sess, step, rc);
        }
    }

    if (!step->complete && !step->relation_active
        && (sess->join_batch_bytes
            ? step->preflight_phase != COL_CONTROLLED_PREFLIGHT_READY
            : step->default_preflight_phase
        != COL_CONTROLLED_DEFAULT_PREFLIGHT_READY)) {
        int rc = col_controlled_step_preflight(sess, step);
        if (rc == EAGAIN && wl_evaluation_control_active_attempt_id(control)
            == step->operation_id) return EAGAIN;
        if (rc) {
            step->refused = rc == ENOTSUP;
            step->preflight_phase = COL_CONTROLLED_PREFLIGHT_IDLE;
            step->controlled_materialized_batch_fallback = false;
            return col_controlled_step_finish(sess, step, rc);
        }
    }

    /* The bounded recursive route owns its complete operation lifecycle,
     * including child execution, delta growth and final publication. */
    if (step->recursive_route && !step->complete)
        return col_controlled_step_recursive_drive(sess, step, teardown);
    if (step->successor_attempt && !step->published
        && !step->successor_input_reader.owner) {
        col_rel_t *source = step->observed_input;
        if (!source || source->relation_identity
            != step->successor_input_identity
            || source->view_generation
            != step->successor_input_view_generation
            || source->storage_generation
            != step->successor_input_storage_generation)
            return col_controlled_step_finish(sess, step, EINVAL);
        int lease_rc = wl_evaluation_control_charge(control, 1);
        if (lease_rc)
            return col_controlled_step_finish(sess, step, lease_rc);
        lease_rc = col_rel_source_reader_acquire(source,
                &step->successor_input_reader);
        if (lease_rc)
            return col_controlled_step_finish(sess, step, lease_rc);
        if (source->relation_identity != step->successor_input_identity
            || source->view_generation
            != step->successor_input_view_generation
            || source->storage_generation
            != step->successor_input_storage_generation) {
            int release_rc = col_rel_source_reader_release(
                &step->successor_input_reader);
            assert(release_rc == 0);
            return col_controlled_step_finish(sess, step, EINVAL);
        }
        lease_rc = wl_evaluation_control_charge(control, 0);
        if (lease_rc)
            return col_controlled_step_finish(sess, step, lease_rc);
    }

    if (step->observed) {
        col_rel_t *source = step->observed_input;
        if (source && source->ncols == 0 && source->nrows == 0) {
            int schema_rc = wl_evaluation_control_charge(control, 1);
            if (schema_rc)
                return col_controlled_step_finish(sess, step, schema_rc);
            schema_rc = col_rel_set_schema(source, 1, NULL);
            if (schema_rc)
                return col_controlled_step_finish(sess, step, schema_rc);
            schema_rc = wl_evaluation_control_charge(control, 0);
            if (schema_rc)
                return col_controlled_step_finish(sess, step, schema_rc);
        }
    }

    if (step->observed && !step->baseline_ready) {
        const wl_plan_relation_t *relation =
            &sess->plan->strata[0].relations[0];
        if (!sess->delta_observer_build) {
            int build_rc = step->successor_attempt
                ? wl_columnar_eval_delta_observer_build_start(sess,
                    UINT64_MAX)
                : wl_columnar_eval_delta_observer_build_start_missing_head(
                sess, UINT64_MAX, relation->name);
            if (build_rc)
                return col_controlled_step_finish(sess, step, build_rc);
        }
        for (;;) {
            bool ready = false;
            int build_rc = wl_columnar_eval_delta_observer_build_advance(sess,
                    control, step->operation_id, &ready);
            if (build_rc == EAGAIN) continue;
            if (build_rc)
                return col_controlled_step_finish(sess, step, build_rc);
            if (!ready) continue;
            step->baseline_ready = true;
            wl_columnar_eval_delta_observer_set_active(sess, true);
            break;
        }
    }

    if (teardown) {
        wl_evaluation_control_request_cancel(control);
        if (!step->relation_active) {
            int rc = col_controlled_step_finish(sess, step,
                    step->primary_result ? step->primary_result
                    : WL_EVALUATION_CONTROL_CANCELLED);
            return rc == WL_EVALUATION_CONTROL_CANCELLED ? 0 : rc;
        }
    }

    if (step->complete) {
        if (step->observed &&
            (!step->published || step->observer_cleanup_pending)) {
            int observer_rc = col_controlled_step_observer_drive(sess, step);
            if (observer_rc == EAGAIN) return EAGAIN;
            if (observer_rc)
                return col_controlled_step_finish(sess, step, observer_rc);
        } else if (step->observer_cleanup_pending) {
            int cleanup_rc = wl_columnar_eval_delta_observer_finish(sess);
            if (cleanup_rc)
                return col_controlled_step_finish(sess, step, cleanup_rc);
            step->observer_cleanup_pending = false;
        }
        int rc = step->observed ? 0
            : wl_evaluation_control_charge(control, 0);
        return col_controlled_step_finish(sess, step, rc);
    }

    for (;;) {
        int schedule_rc = col_controlled_step_schedule(sess, step);
        if (schedule_rc == EAGAIN &&
            wl_evaluation_control_active_attempt_id(control)
            == step->operation_id)
            return EAGAIN;
        if (schedule_rc) {
            int finish_rc = col_controlled_step_finish(sess, step,
                    schedule_rc);
            return finish_rc;
        }
        if (step->complete) {
            int observer_rc = step->observed
                ? col_controlled_step_observer_drive(sess, step) : 0;
            if (observer_rc == EAGAIN) return EAGAIN;
            return col_controlled_step_finish(sess, step, observer_rc);
        }
        const wl_plan_relation_t *relation = NULL;
        int relation_rc = col_controlled_step_current_relation(sess, step,
                &relation);
        if (relation_rc) {
            int finish_rc = col_controlled_step_finish(sess, step,
                    relation_rc);
            return finish_rc;
        }
        if (!relation) {
            if (step->observed &&
                (!step->published || step->observer_cleanup_pending)) {
                int observer_rc = col_controlled_step_observer_drive(sess,
                        step);
                if (observer_rc == EAGAIN) return EAGAIN;
                if (observer_rc)
                    return col_controlled_step_finish(sess, step,
                               observer_rc);
            } else if (step->observer_cleanup_pending) {
                int cleanup_rc =
                    wl_columnar_eval_delta_observer_finish(sess);
                if (cleanup_rc)
                    return col_controlled_step_finish(sess, step,
                               cleanup_rc);
                step->observer_cleanup_pending = false;
            }
            int rc = col_controlled_step_finish(sess, step, 0);
            return rc;
        }
        step->relation_active = true;
        for (;;) {
            int protocol = col_controlled_step_host_action(sess, step,
                    relation, WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE);
            if (protocol == EAGAIN) return EAGAIN;
            if (protocol && step->reply.kind
                == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED
                && !sess->cleanup_active) {
                step->relation_active = false;
                int result = protocol;
                int finish_rc = col_controlled_step_finish(sess, step, result);
                return finish_rc;
            }
            if (protocol && step->reply.kind
                == WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT
                && !sess->cleanup_active) {
                step->relation_active = false;
                return col_controlled_step_finish(sess, step, protocol);
            }
            if (step->reply.kind ==
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL) {
                int result = step->reply.execution_result;
                if (step->reply.committed)
                    col_controlled_step_advance_cursor(sess, step);
                step->relation_active = false;
                col_controlled_step_grant_release(step,
                    &step->previous_funding);
                if (result) {
                    step->primary_result = result;
                    int finish_rc = col_controlled_step_finish(sess, step,
                            result);
                    return finish_rc;
                }
                if (!step->reply.committed) {
                    int finish_rc = col_controlled_step_finish(sess, step,
                            EINVAL);
                    return finish_rc;
                }
                if (teardown) {
                    int finish_rc = col_controlled_step_finish(sess, step,
                            WL_EVALUATION_CONTROL_CANCELLED);
                    return finish_rc == WL_EVALUATION_CONTROL_CANCELLED
                        ? 0 : finish_rc;
                }
                int boundary_stop = wl_evaluation_control_charge(control, 0);
                if (boundary_stop) {
                    int finish_rc = col_controlled_step_finish(sess, step,
                            boundary_stop);
                    return finish_rc;
                }
                break;
            }
            if (step->reply.kind ==
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED) {
                int finish_rc = col_controlled_step_finish(sess, step,
                        protocol ? protocol : ENOTSUP);
                return finish_rc;
            }

            int status_rc = col_controlled_step_host_action(sess, step,
                    relation, WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS);
            if (status_rc == EAGAIN) return EAGAIN;
            if (status_rc) {
                int finish_rc = col_controlled_step_finish(sess, step,
                        status_rc);
                return finish_rc;
            }
            if (step->reply.status.execution_result
                && !step->primary_result)
                step->primary_result = step->reply.status.execution_result;
            if (step->reply.status.retained
                || step->reply.status.cleanup_result)
                return EAGAIN;
            if (teardown && !sess->cleanup_active) {
                int finish_rc = col_controlled_step_finish(sess, step,
                        step->primary_result ? step->primary_result
                        : WL_EVALUATION_CONTROL_CANCELLED);
                return finish_rc == WL_EVALUATION_CONTROL_CANCELLED
                    ? 0 : finish_rc;
            }
        }
    }
}

static int
col_controlled_step_successor_target_init(wl_col_session_t *sess,
    wl_columnar_controlled_step_t *step,
    const wl_plan_relation_t *source_relation)
{
    if (step->successor_private_name) return 0;
    if (!source_relation || !source_relation->ops
        || (source_relation->op_count != 3 && source_relation->op_count != 4)
        || !step->observed_input || !step->observed_head) return ENOTSUP;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    for (unsigned action = 0; action < 64; ++action) {
        int rc;
        if (step->successor_name_phase == 0) {
            if (step->successor_name_suffix == 1024) return EEXIST;
            /* Fixed buffer generation and header bookkeeping are admitted
             * before the bounded snprintf; no caller-sized string is read. */
            rc = wl_evaluation_control_charge(control, 128);
            if (rc) return rc;
            int n = snprintf(step->successor_name_buffer,
                    sizeof(step->successor_name_buffer),
                    "__wirelog_step_%" PRIxPTR "_%" PRIx64 "_%" PRIx32,
                    (uintptr_t)step, step->operation_identity,
                    step->successor_name_suffix);
            if (n < 0 || (size_t)n >= sizeof(step->successor_name_buffer))
                return EOVERFLOW;
            step->successor_name_slot = 0;
            step->successor_name_byte = 0;
            step->successor_name_phase = 1;
        } else if (step->successor_name_phase == 1) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) return rc;
            if (step->successor_name_slot >= sess->nrels) {
                step->successor_name_phase = 2;
                continue;
            }
            col_rel_t *r = sess->rels[step->successor_name_slot];
            if (!r || !r->name) {
                step->successor_name_slot++;
                step->successor_name_byte = 0;
                continue;
            }
            uint32_t byte = step->successor_name_byte;
            char wanted = step->successor_name_buffer[byte];
            char actual = r->name[byte];
            if (actual != wanted) {
                step->successor_name_slot++;
                step->successor_name_byte = 0;
            } else if (!wanted) {
                step->successor_name_suffix++;
                step->successor_name_phase = 0;
            } else step->successor_name_byte++;
        } else {
            /* Copy fixed plan metadata in admitted <=128-byte pieces. */
            size_t relation_bytes = sizeof(step->successor_relation);
            size_t op_bytes = source_relation->op_count
                * sizeof(step->successor_ops[0]);
            size_t offset = step->successor_metadata_byte;
            if (offset < relation_bytes + op_bytes) {
                size_t local = offset < relation_bytes ? offset
                    : offset - relation_bytes;
                size_t remaining = offset < relation_bytes
                    ? relation_bytes - local : op_bytes - local;
                size_t chunk = remaining < 128 ? remaining : 128;
                rc = wl_evaluation_control_charge(control, chunk);
                if (rc) return rc;
                unsigned char *dst = offset < relation_bytes
                    ? (unsigned char *)&step->successor_relation
                    : (unsigned char *)step->successor_ops;
                const unsigned char *src = offset < relation_bytes
                    ? (const unsigned char *)source_relation
                    : (const unsigned char *)source_relation->ops;
                memcpy(dst + local, src + local, chunk);
                step->successor_metadata_byte += chunk;
                continue;
            }
            rc = wl_evaluation_control_charge(control, 32);
            if (rc) return rc;
            step->successor_private_name = step->successor_name_buffer;
            col_rel_t *input = step->observed_input;
            col_rel_t *head = step->observed_head;
            step->successor_input_identity = input->relation_identity;
            step->successor_input_view_generation = input->view_generation;
            step->successor_input_storage_generation =
                input->storage_generation;
            step->successor_head_identity = head->relation_identity;
            step->successor_head_view_generation = head->view_generation;
            step->successor_head_storage_generation = head->storage_generation;
            step->successor_relation.name = step->successor_private_name;
            step->successor_ops[0].delta_mode = WL_DELTA_FORCE_FULL;
            step->successor_relation.ops = step->successor_ops;
            return wl_evaluation_control_charge(control, 0);
        }
    }
    int rc = wl_evaluation_control_charge(control, 0);
    return rc ? rc : EAGAIN;
}

static bool
wl_columnar_session_observed_general_shape(const wl_col_session_t *sess)
{
    const wl_plan_t *plan = sess->plan;
    if (wl_columnar_session_recursive_transaction_shape(sess)) return true;
    if (!sess->delta_cb) return false;
    if (!plan || !plan->strata) return true;
    if (plan->edb_count == 1 && plan->edb_column_type_counts
        && plan->edb_column_type_counts[0] == 1 && plan->edb_column_types
        && plan->edb_column_types[0]
        && plan->edb_column_types[0][0] != WIRELOG_TYPE_INT64) return true;
    if (plan->stratum_count != 1 || plan->strata[0].relation_count != 1)
        return true;
    if (!plan->strata[0].relations) return true;
    const wl_plan_relation_t *r = &plan->strata[0].relations[0];
    if (!r->ops || (r->op_count != 3 && r->op_count != 4)) return true;
    if (r->ops[0].op != WL_PLAN_OP_VARIABLE
        || (r->op_count == 4 && r->ops[1].op != WL_PLAN_OP_FILTER)
        || r->ops[r->op_count - 2].op != WL_PLAN_OP_MAP
        || r->ops[r->op_count - 1].op != WL_PLAN_OP_CONSOLIDATE) return true;
    const wl_plan_op_t *map = &r->ops[r->op_count - 2];
    return map->project_count != 1 || !map->project_indices
           || map->project_indices[0] != 0;
}

static int
col_controlled_step(wl_col_session_t *sess)
{
    wl_columnar_controlled_step_t *step = sess->controlled_step;
    bool successor_step = false;
    if (step && step->complete && step->recursive_route) {
        if (!step->retryable) {
            if (col_controlled_step_provenance_current(sess, step))
                return 0;
            step->provenance_invalid = true;
            return EINVAL;
        }
        if (!col_controlled_step_provenance_current(sess, step)) {
            step->provenance_invalid = true;
            step->retryable = false;
            return EINVAL;
        }
        int preflight_rc = col_controlled_step_static_preflight(sess, false);
        if (preflight_rc) return preflight_rc;
        step->default_preflight_phase = COL_CONTROLLED_DEFAULT_PREFLIGHT_IDLE;
        step->default_preflight_plan = NULL;
        step->default_preflight_registry = NULL;
        step->default_preflight_registry_count = 0;
        step->default_preflight_stratum = 0;
        step->default_preflight_relation = 0;
        step->default_preflight_op = 0;
        step->default_preflight_registry_slot = 0;
        step->default_preflight_name_byte = 0;
        step->default_preflight_target_found = false;
        step->complete = false;
        step->retryable = false;
        successor_step = true;
    }
    if (step && step->complete && step->successor_mutation_seen) {
        int rc = col_controlled_step_static_preflight(sess, true);
        if (rc) return rc;
        uint64_t identity = step->operation_identity;
        bool general = step->general_observed || sess->nrels > 3;
        if (step->transaction) {
            if (!step->operation_active) {
                rc = wl_evaluation_control_begin(sess->base.evaluation_control,
                        sess->base.operation_admission);
                if (rc) return rc;
                step->operation_active = true;
                step->operation_id = wl_evaluation_control_active_attempt_id(
                    sess->base.evaluation_control);
            }
            rc = wl_columnar_session_observed_dispose(sess, step);
            if (rc) return rc;
        }
        bool append_proven = step->append_proven;
        col_rel_t *append_source = step->append_source;
        uint64_t append_identity = step->append_identity;
        uint32_t append_begin = step->append_begin;
        bool observed = step->observed;
        uint64_t operation_id = step->operation_id;
        bool operation_active = step->operation_active;
        memset(step, 0, sizeof(*step));
        step->operation_id = operation_id;
        step->operation_active = operation_active;
        step->operation_identity = identity;
        step->observed = observed;
        step->general_observed = general;
        step->successor_attempt = true;
        step->append_proven = append_proven;
        step->append_source = append_source;
        step->append_identity = append_identity;
        step->append_begin = append_begin;
    }
    if (!successor_step
        && (!step || (!step->operation_active && !step->complete))) {
        int rc = col_controlled_step_static_preflight(sess,
                step && step->successor_attempt);
        if (rc) return rc;
    }
    if (!step) {
        sess->controlled_step = calloc(1, sizeof(*sess->controlled_step));
        if (!sess->controlled_step) return ENOMEM;
        /* The host identity persists while control attempt IDs may change. */
        sess->controlled_step->operation_identity =
            (uint64_t)(uintptr_t)sess->controlled_step;
        const wl_plan_t *plan = sess->plan;
        sess->controlled_step->recursive_route = plan
            && plan->stratum_count == 1 && plan->strata
            && plan->strata[0].is_recursive
            && plan->strata[0].relation_count == 1
            && plan->strata[0].relations
            && wl_columnar_eval_plan_numeric_recursive_path_supported(
            &plan->strata[0].relations[0]);
        sess->controlled_step->observed = sess->delta_cb != NULL;
        sess->controlled_step->general_observed =
            wl_columnar_session_observed_general_shape(sess);
        if (sess->controlled_step->general_observed)
            sess->controlled_step->recursive_route = false;
    }
    return col_controlled_step_drive(sess, sess->controlled_step, false);
}

static int
wl_columnar_session_controlled_step_drain_for_destroy(wl_col_session_t *sess)
{
    wl_columnar_controlled_step_t *step = sess->controlled_step;
    if (!step) return 0;
    int rc = 0;
    if (step->operation_active)
        rc = col_controlled_step_drive(sess, step, true);
    if (rc == EAGAIN || step->operation_active) return EBUSY;
    if (step->transaction) {
        int dispose_rc = EAGAIN;
        for (unsigned action = 0; action < 16 && dispose_rc == EAGAIN; ++action)
            dispose_rc = wl_columnar_session_observed_dispose(sess, step);
        if (dispose_rc) return dispose_rc == EAGAIN ? EBUSY : dispose_rc;
    }
    if (step->staging_retirement.storage.identity && !step->staging_done) {
        if (step->staging_retirement.committed) {
            int cleanup_rc;
            do {
                cleanup_rc = col_controlled_step_staging_finish(sess, step);
            }while (cleanup_rc == EAGAIN);
            if (cleanup_rc) return cleanup_rc;
        } else {
            int cancel_rc = wl_columnar_relation_scalar_retirement_cancel(
                &step->staging_retirement);
            if (cancel_rc) return cancel_rc;
        }
    }
    col_controlled_step_staging_end(step);
    if (step->successor_candidate_detached && step->successor_candidate) {
        int candidate_rc = col_rel_destroy_checked(step->successor_candidate);
        if (candidate_rc != 0)
            return candidate_rc;
        step->successor_candidate = NULL;
    }
    if (step->successor_input_reader.owner) {
        int reader_rc = col_rel_source_reader_release(
            &step->successor_input_reader);
        if (reader_rc != 0)
            return reader_rc;
    }
    /* The drained host relinquishes its retained baseline before generic
     * observer disposal. No evaluation or publication remains in flight. */
    if (step->observed && step->baseline_ready && !step->observer_evaluated
        && !step->published && !wl_columnar_eval_delta_observer_evaluated(sess))
        wl_columnar_eval_delta_observer_set_active(sess, false);
    col_controlled_step_grant_release(step, &step->previous_funding);
    free(step);
    sess->controlled_step = NULL;
    /* Destroy consumes the terminal outcome from the caller-owned control
     * report; a stopped operation is safe to tear down once its frame drains. */
    return 0;
}

/* Controlled initial STEP uses the framed relation host; raw session
 * behavior keeps the established synchronous evaluator. */
static int
col_session_step(wl_session_t *session)
{
    if (!session) return EINVAL;
    wl_col_session_t *sess = COL_SESSION(session);
    sess->cold_seal_closed = true;
    int cold_rc = wl_columnar_session_cold_execution_check(sess);
    if (cold_rc != 0) return cold_rc;
    if (session->evaluation_control)
        return col_controlled_step(COL_SESSION(session));
    return col_session_step_impl(session);
}

static wl_controlled_step_state_t
col_session_controlled_step_state(const wl_session_t *session)
{
    const wl_col_session_t *sess = COL_SESSION((wl_session_t *)session);
    const wl_columnar_controlled_step_t *step = sess->controlled_step;
    if (sess->cleanup_active || sess->cleanup_pending)
        return WL_CONTROLLED_STEP_PENDING;
    if (!step) return WL_CONTROLLED_STEP_IDLE;
    if (step->provenance_invalid
        || (step->complete && step->recursive_route
        && step->provenance_valid
        && !col_controlled_step_provenance_current(sess, step)))
        return WL_CONTROLLED_STEP_INVALIDATED;
    if (step->general_observed &&
        !step->complete) return WL_CONTROLLED_STEP_PENDING;
    if (step->complete) {
        if (step->observed && (!step->published
            || step->observer_cleanup_pending))
            return WL_CONTROLLED_STEP_PENDING;
        return (step->retryable ||
               step->successor_mutation_seen) ? WL_CONTROLLED_STEP_RETRYABLE
            : WL_CONTROLLED_STEP_COMPLETE;
    }
    return step->refused ? WL_CONTROLLED_STEP_IDLE
        : WL_CONTROLLED_STEP_PENDING;
}

static int
col_session_controlled_mutation_admit(wl_session_t *session, bool insert,
    const char *relation, uint32_t num_rows, uint32_t num_cols)
{
    wl_col_session_t *sess = COL_SESSION(session);
    wl_columnar_controlled_step_t *step = sess->controlled_step;
    if (!step || (!step->observed && !step->general_observed))
        return EBUSY;
    if (step->general_observed) {
        const wl_columnar_session_observed_transaction_t *tx =
            step->transaction;
        if (!tx || !relation || !step->complete || !step->published
            || step->observer_cleanup_pending) return EBUSY;
        for (uint32_t i = 0; i < sess->plan->edb_count; ++i) {
            col_rel_t *r = tx->entries[i].public_relation;
            if (r && num_cols == sess->plan->edb_declared_width[i]
                && strcmp(relation, tx->entries[i].name) == 0) return 0;
        }
        return ENOTSUP;
    }
    if (!relation || num_cols != 1 || num_rows > 1)
        return ENOTSUP;
    if (!step->complete || !step->published
        || step->observer_cleanup_pending)
        return ENOTSUP;
    int rc = col_controlled_step_static_preflight(sess, true);
    if (rc)
        return rc;
    const char *source = sess->plan->strata[0].relations[0]
        .ops[0].relation_name;
    /* Mutation API argument matching is outside STEP accounting. Its finite
     * bound comes from the already charged immutable-name proof, and at most
     * three registry slots are inspected. Never rebuild the registry hash or
     * rescan plan/EDB metadata here. STEP revalidates under its own allowance. */
    if (step->observed_preflight_phase != 8
        || step->observed_input_name_length == SIZE_MAX
        || step->observed_head_name_length == SIZE_MAX
        || strncmp(relation, source, step->observed_input_name_length + 1) != 0)
        return ENOTSUP;
    const char *head_name = sess->plan->strata[0].relations[0].name;
    bool input_found = false, head_found = false;
    for (uint32_t i = 0; i < sess->nrels; ++i) {
        col_rel_t *r = sess->rels ? sess->rels[i] : NULL;
        if (!r || !r->name) continue;
        if (r == step->observed_input
            && r->relation_identity == step->observed_input_identity
            && strncmp(r->name, source,
            step->observed_input_name_length + 1) == 0)
            input_found = true;
        if (r == step->observed_head
            && r->relation_identity == step->observed_head_identity
            && strncmp(r->name, head_name,
            step->observed_head_name_length + 1) == 0
            && r->ncols == 1 && r->nrows <= r->capacity
            && (!r->column_types || r->column_types[0] == WIRELOG_TYPE_INT64))
            head_found = true;
    }
    if (!input_found || !head_found) return ENOTSUP;
    (void)insert;
    return 0;
}

static void
col_session_controlled_mutation_committed(wl_session_t *session,
    bool insert, const char *relation, uint32_t num_rows, uint32_t num_cols)
{
    (void)insert;
    (void)num_cols;
    if (!session || !relation || num_rows == 0)
        return;
    wl_col_session_t *sess = COL_SESSION(session);
    wl_columnar_controlled_step_t *step = sess->controlled_step;
    if (step && step->general_observed && step->complete && step->published) {
        wl_columnar_session_observed_transaction_t *tx = step->transaction;
        col_rel_t *input = NULL;
        for (uint32_t i = 0; tx && i < sess->plan->edb_count; ++i)
            if (!strcmp(relation, tx->entries[i].name))
                input = tx->entries[i].public_relation;
        if (!step->successor_mutation_seen) {
            step->append_proven = insert && input;
            step->append_source = input;
            step->append_identity = input ? input->relation_identity : 0;
            step->append_begin = input ? input->base_nrows : 0;
        } else if (!insert || input != step->append_source)
            step->append_proven = false;
        step->successor_mutation_seen = true;
        return;
    }
    if (!step || !step->observed || !step->complete || !step->published
        || !sess->plan || sess->plan->stratum_count != 1
        || !sess->plan->strata || sess->plan->strata[0].relation_count != 1
        || !sess->plan->strata[0].relations
        || !sess->plan->strata[0].relations[0].ops
        || sess->plan->strata[0].relations[0].op_count == 0
        || !sess->plan->strata[0].relations[0].ops[0].relation_name)
        return;
    if (strncmp(relation,
        sess->plan->strata[0].relations[0].ops[0].relation_name,
        step->observed_input_name_length + 1) == 0)
        step->successor_mutation_seen = true;
}

static int
col_session_emit_snapshot(const wl_plan_t *plan, wl_col_session_t *sess,
    wirelog_on_tuple_fn callback, void *user_data)
{
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *rname = sp->relations[ri].name;
            col_rel_t *r = session_find_rel(sess, rname);
            if (!r || r->nrows == 0)
                continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                callback(rname, col_rel_row(r, row), r->ncols,
                    user_data);
            }
        }
    }
    return 0;
}

static int
col_session_clear_idb_rows(const wl_plan_t *plan, wl_col_session_t *sess,
    uint32_t strata_end)
{
    col_rel_t **targets = NULL;
    col_rel_t **owners = NULL;
    wl_columnar_source_access_writer_t *writers = NULL;
    uint32_t target_count = 0;
    uint32_t owner_count = 0;
    uint32_t reset_count = 0;
    uint32_t capacity = sess ? (sess->nrels ? sess->nrels : 1) : 0;
    int rc = 0;

    if (!plan || !sess)
        return EINVAL;
    targets = (col_rel_t **)calloc(capacity, sizeof(*targets));
    owners = (col_rel_t **)calloc(capacity, sizeof(*owners));
    writers = (wl_columnar_source_access_writer_t *)calloc(
        capacity, sizeof(*writers));
    if (!targets || !owners || !writers) {
        free((void *)targets);
        free((void *)owners);
        free(writers);
        return ENOMEM;
    }

    /* Collect each relation once and resolve all owners before mutation. */
    for (uint32_t si = 0; si < plan->stratum_count && si < strata_end; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
            if (!r)
                continue;
            bool known_target = false;
            for (uint32_t i = 0; i < target_count; i++)
                if (targets[i] == r)
                    known_target = true;
            if (known_target)
                continue;
            col_rel_t *owner = NULL;
            rc = col_rel_storage_owner_resolve(r, &owner);
            if (rc != 0) {
                rc = rc != 0 ? rc : EBUSY;
                goto cleanup;
            }
            if (target_count >= capacity) {
                rc = EOVERFLOW;
                goto cleanup;
            }
            targets[target_count] = r;
            target_count++;
            bool known_owner = false;
            for (uint32_t i = 0; i < owner_count; i++)
                if (owners[i] == owner)
                    known_owner = true;
            if (!known_owner) {
                if (owner_count >= capacity) {
                    rc = EOVERFLOW;
                    goto cleanup;
                }
                owners[owner_count] = owner;
                owner_count++;
            }
        }
    }

    for (uint32_t i = 1; i < owner_count; i++) {
        col_rel_t *key = owners[i];
        uint32_t j = i;
        while (j > 0 && (uintptr_t)owners[j - 1] > (uintptr_t)key) {
            owners[j] = owners[j - 1];
            j--;
        }
        owners[j] = key;
    }
    for (uint32_t i = 0; i < owner_count; i++) {
        rc = wl_columnar_source_access_writer_acquire(
            &owners[i]->source_access, &writers[i]);
        if (rc != 0)
            goto cleanup;
    }

    /* Reset admission must use the alias count protected by each acquired
     * owner writer, not the earlier collection-time observation. */
    for (uint32_t i = 0; i < target_count; i++) {
        col_rel_t *owner = NULL;
        rc = col_rel_storage_owner_resolve(targets[i], &owner);
        if (rc != 0)
            goto cleanup;
        if (col_rel_storage_alias_borrow_count(owner) > 0) {
            rc = EBUSY;
            goto cleanup;
        }
    }

    /* Every writer is held: all logical resets below are non-fallible. */
    for (uint32_t i = 0; i < target_count; i++) {
        col_rel_t *owner = NULL;
        rc = col_rel_storage_owner_resolve(targets[i], &owner);
        if (rc != 0)
            goto cleanup;
        wl_columnar_source_access_writer_t *writer = NULL;
        for (uint32_t j = 0; j < owner_count; j++)
            if (owners[j] == owner)
                writer = &writers[j];
        rc = col_rel_reset_rows_locked(targets[i], writer);
        if (rc != 0)
            goto cleanup;
        reset_count = i + 1u;
        col_session_invalidate_arrangements(&sess->base, targets[i]->name);
    }

    col_mat_cache_release_pins(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);
    col_mat_cache_clear(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);

cleanup:
    for (uint32_t i = owner_count; i > 0; i--)
        if (writers[i - 1].owner)
            (void)wl_columnar_source_access_writer_release(&writers[i - 1]);
    /* Issue #1994: a reset relation that also holds input gets its input
     * back.  This appends under the relation's own writer, so it runs only
     * once every writer above is released. */
    for (uint32_t i = 0; i < reset_count; i++) {
        int seed_rc = wl_columnar_session_restore_seed(sess, targets[i]);
        if (rc == 0 && seed_rc != 0)
            rc = seed_rc;
    }
    free((void *)targets);
    free((void *)owners);
    free(writers);
    return rc;
}

/* Call only after the dispatch barrier. A retained worker frame may still
 * borrow coordinator registry/cache owners even after checked worker teardown
 * refused it. Externally owned generic worker arrays are not this cohort. */
static bool
wl_columnar_session_has_retained_cleanup(const wl_col_session_t *sess)
{
    if (!sess)
        return false;
    if (sess->cleanup_active || sess->cleanup_pending || sess->delta_rollback)
        return true;
    if (!sess->coordinator) {
        for (uint32_t w = 0; w < sess->tdd_workers_count; w++) {
            const wl_col_session_t *worker = &sess->tdd_workers[w];
            if (worker->cleanup_active || worker->cleanup_pending
                || worker->delta_rollback)
                return true;
        }
    }
    return false;
}

/* Snapshot/step completion is the coordinator's quiescent boundary.  Explicit
 * pins must already have been released by join.c; only then may the ledger
 * callback reclaim owned cache entries under memory pressure. */
static void
col_session_reclaim_quiescent(wl_col_session_t *sess)
{
    if (!sess || wl_columnar_session_has_retained_cleanup(sess))
        return;
    col_mat_cache_release_pins(&sess->mat_cache);
    if (!sess->mat_cache.reclaimer_owner_alive)
        return;
    if (!wl_mem_ledger_over_budget(&sess->mem_ledger)
        && !wl_mem_ledger_subsys_over_budget(&sess->mem_ledger,
        WL_MEM_SUBSYS_CACHE))
        return;
    (void)wl_mem_ledger_reclaim(&sess->mem_ledger);
}

static void
wl_columnar_session_profile_begin(const wl_plan_t *plan, uint64_t affected_mask,
    uint32_t workers, bool stable)
{
    uint32_t expected = 0;
    if (!stable) {
        for (uint32_t si = 0; si < plan->stratum_count; si++) {
            if (col_affected_mask_contains(affected_mask, si))
                expected++;
        }
    }
    fprintf(stderr,
        "TDD snapshot begin plan_count=%u expected_count=%u scope=%s "
        "affected_mask=%016" PRIx64 " requested_workers=%u\n",
        plan->stratum_count, expected,
        stable ? "stable" : affected_mask == UINT64_MAX ? "full" : "affected",
        affected_mask, workers);
}

/*
 * col_session_snapshot: Evaluate all strata and emit current IDB tuples
 *
 * Implements wl_compute_backend_t.session_snapshot vtable slot.
 *
 * Evaluation order:
 *   1. If the session is already clean and stable, read cached IDB rows.
 *   2. Otherwise execute all strata in plan order.
 *   3. For each IDB relation in each stratum, invoke callback once per row
 *
 * Complexity: O(S * R * N) where S=strata, R=relations per stratum, N=rows
 *
 * Issue #811: clean snapshots read stable R without recomputing.
 *
 * @param session:   wl_session_t* (cast to wl_col_session_t* internally)
 * @param callback:  Invoked once per output tuple (relation, row, ncols)
 * @param user_data: Opaque pointer passed through to callback
 * @return 0 on success, EINVAL if session/callback NULL, non-zero on eval error
 */
static int
col_session_snapshot_impl(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data)
{
    if (!session || !callback)
        return EINVAL;

    wl_col_session_t *sess = COL_SESSION(session);
    wl_columnar_session_budget_denial_clear(sess);
    if (sess->plain_step_completion_pending
        && sess->plain_step_completion_active)
        return EBUSY;
    if (!sess->plain_step_completion_pending)
        sess->plain_step_completion_step_context = false;
    if (sess->delta_observer || sess->delta_publish_active)
        return EBUSY;
    int cleanup_rc = wl_columnar_session_cleanup_ready_quiescent(sess);
    if (cleanup_rc != 0)
        return cleanup_rc;
    int deferred_rc = wl_columnar_session_retry_deferred(sess);
    if (deferred_rc != 0)
        return deferred_rc;
    if (sess->plain_step_completion_pending) {
        /* Drain a refused plain STEP before snapshot callbacks are emitted. */
        int resume_rc = col_session_step_impl(session);
        if (resume_rc != 0)
            return resume_rc;
    }
    const wl_plan_t *plan = sess->plan;
    sess->extension_expr_status = 0;
    const char *tdd_profile = getenv("WIRELOG_TDD_STRATUM_PROFILE");
    bool tdd_profile_active = tdd_profile && tdd_profile[0] != '\0'
        && tdd_profile[0] != '0';
    uint32_t tdd_profile_evaluated = 0;

    /* Reset profiling counters for this evaluation pass */
    col_session_reset_snapshot_profile(sess);

    if (sess->snapshot_stable_valid
        && !sess->pending_input_change
        && !sess->pending_full_input_eval
        && sess->last_inserted_relation == NULL
        && sess->last_removed_relation == NULL
        && !sess->retraction_seeded) {
        if (tdd_profile_active)
            wl_columnar_session_profile_begin(plan, 0, sess->num_workers, true);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_session_test_before_stable_emit)
            wl_columnar_session_test_before_stable_emit(sess);
#endif
        /*
         * The fast path has its own cutoff because it has its own emit.  A
         * cutoff placed for the evaluating path below is not on this route: this
         * branch publishes the whole model without evaluating, so a
         * cancellation arriving after the wrapper's entry poll would be seen by
         * nothing -- finish() reads control->stop, and only charge() converts
         * the sticky cancelled word into one.
         *
         * "It does no work, so the entry poll is adjacent enough" does not hold.
         * A refused plain step is drained above by calling col_session_step_impl
         * inside this attempt, and that drain compacts every relation before
         * writing all six fields this branch tests -- four in its commit block,
         * and last_removed_relation and retraction_seeded in the retraction
         * cleanup just above it.  So a drain can leave control here with the
         * whole model just compacted.
         *
         * The unwind performs the quiescent boundary and nothing else.  It does
         * not sample the memory ledger because this branch's success route does
         * not either -- it emits and reclaims -- so the unwind is symmetric with
         * the path it guards.  And it must not clear snapshot_stable_valid:
         * refusing to publish does not invalidate a stable model, and clearing
         * it would push the retry into a full re-evaluation.
         */
        int stable_cutoff_rc = col_session_publication_cutoff(sess);
        if (stable_cutoff_rc != 0) {
            col_session_reclaim_quiescent(sess);
            return stable_cutoff_rc;
        }
        int rc = col_session_emit_snapshot(plan, sess, callback, user_data);
        col_session_reclaim_quiescent(sess);
        if (tdd_profile_active && rc == 0)
            fprintf(stderr, "TDD snapshot complete evaluated_count=0 rc=0\n");
        return rc;
    }

    /* Issue #2114: after an incremental insert, the strata it reaches are
     * brought up to date with occurrence-bound instances
     * (session_snapshot_insert).  A staged removal and an insert into a rule
     * head are not insertions those can evaluate, so either makes this a
     * full re-evaluation.  Issue #2121: whether a delta callback is
     * installed plays no part in this choice.  This function calls no delta
     * callback on either route -- a refused plain step drained above resumes
     * without the delta observer -- and both run the bookkeeping below
     * `evaluated`. */
    if (sess->has_evaluated && !sess->pending_full_input_eval
        && sess->last_inserted_relation != NULL
        && (sess->last_removed_relation != NULL
        || session_plan_has_rule_head(plan, sess->last_inserted_relation)))
        sess->pending_full_input_eval = true;
    if (sess->has_evaluated && !sess->pending_full_input_eval
        && sess->last_inserted_relation != NULL) {
        int insert_rc = session_snapshot_insert(session, plan);
        if (insert_rc == 0) {
#ifdef WL_SESSION_TEST_HOOKS
            wl_columnar_session_test_insert_route_taken++;
#endif
            /* The insert route bypasses the stratum evaluators, which keep
             * the frontiers; reset them as a full evaluation does. */
            sess->diff_operators_active = false;
            session_reset_frontiers(plan, sess);
            goto evaluated;
        }
        /* A refusal at admission left the session as it was; any later
         * failure may have left heads partly updated.  Either way the
         * evaluation is redone in full, and the flag stays set until a
         * snapshot or step commits, so a retry after an error does too. */
        sess->pending_full_input_eval = true;
        if (insert_rc != ENOTSUP && insert_rc != EINVAL)
            return insert_rc;
    }

    if (sess->has_evaluated
        && (sess->pending_full_input_eval
        || (sess->pending_input_change
        && sess->last_inserted_relation == NULL))) {
        int clear_rc = col_session_clear_idb_rows(plan, sess,
                plan->stratum_count);
        if (clear_rc != 0)
            return clear_rc;
    } else if (!sess->has_evaluated && sess->uncommitted_strata_done > 0) {
        /* Issue #2092: as in the plain step, a failed first evaluation left
         * these strata derived; every frontier is reset below. */
        int clear_rc = col_session_clear_idb_rows(plan, sess,
                sess->uncommitted_strata_done);
        if (clear_rc != 0)
            return clear_rc;
    }

    /* Every stratum evaluates in full from here: the incremental case left
     * above.  Differential operators serve a step that evaluates part of the
     * plan (see col_should_activate_diff), and a step may have left them on;
     * every stratum and rule frontier is reset to (current_epoch, UINT32_MAX)
     * so no stale frontier can skip iterations the evaluation needs. */
    sess->diff_operators_active = false;
    session_reset_frontiers(plan, sess);

    /* Issue #361: Use TDD parallel evaluation in snapshot when workers are
     * available and facts have been loaded (initial evaluation).
     * col_eval_stratum_tdd falls back to single-threaded when W<=1. */
    bool snapshot_tdd_eligible = sess->num_workers > 1 && !sess->has_evaluated;
    sess->tdd_decision_tracking_active = true;
    if (tdd_profile_active)
        wl_columnar_session_profile_begin(plan, UINT64_MAX,
            sess->num_workers, false);
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        wl_columnar_session_tdd_decision_t tdd_decision =
            wl_columnar_session_tdd_plan_stratum(&plan->strata[si], sess,
                snapshot_tdd_eligible);
        bool use_tdd = tdd_decision.use_tdd;
        if (plan->strata[si].is_recursive) {
            sess->tdd_recursive_strata++;
            if (use_tdd) {
                sess->tdd_executed_strata++;
            } else {
                wl_columnar_internal_tdd_fallback_reason_t reason =
                    tdd_decision.fallback_reason;
                sess->tdd_fallback_strata++;
                sess->tdd_last_fallback_reason = reason;
                if (reason > WL_COLUMNAR_INTERNAL_TDD_FALLBACK_NONE
                    && reason <
                    WL_COLUMNAR_INTERNAL_TDD_FALLBACK_REASON_COUNT) {
                    sess->tdd_fallback_reason_counts[reason]++;
                }
            }
        }
        /* Issue #390: Correctness oracle — compare TDD vs single-threaded
         * tuple counts for recursive strata.  Enabled via env var.
         * Debug-only; never runs in production.
         *
         * Save pre-stratum IDB state before TDD so the single-threaded
         * re-evaluation starts from the same base facts. */
        static int tdd_cc = -1;
        if (tdd_cc < 0) {
            const char *env = getenv("WIRELOG_TDD_CORRECTNESS_CHECK");
            tdd_cc = (env && env[0] == '1') ? 1 : 0;
        }
        bool tdd_cc_enabled = tdd_cc != 0;
#ifdef WL_SESSION_TEST_HOOKS
        tdd_cc_enabled |= wl_columnar_session_test_enable_correctness_replay;
#endif
        bool tdd_cc_active = tdd_cc_enabled
            && use_tdd && plan->strata[si].is_recursive;
        const wl_plan_stratum_t *csp = &plan->strata[si];
        uint32_t cnrels = csp->relation_count;
        col_rel_t **pre_saved = NULL;
        if (tdd_cc_active) {
            pre_saved
                = (col_rel_t **)calloc(cnrels, sizeof(col_rel_t *));
            if (pre_saved) {
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    if (r && r->nrows > 0 && r->ncols > 0) {
                        pre_saved[ri]
                            = col_rel_new_auto(r->name, r->ncols);
                        if (pre_saved[ri])
                            col_rel_append_all(
                                pre_saved[ri], r, NULL);
                    }
                }
            }
        }

        uint64_t stratum_t0 = tdd_profile_active ? now_ns() : 0;
        uint64_t kfusion_base = sess->kfusion_ns;
        uint64_t exchange_base = sess->exchange_time_ns;
        uint64_t tdd_base = sess->tdd_total_ns;
        uint64_t wait_base = sess->tdd_wait_barrier_ns;
        uint64_t merge_base = sess->tdd_final_merge_ns;
        memset(&sess->tdd_audit, 0, sizeof(sess->tdd_audit));
        sess->tdd_audit.enabled = tdd_profile_active;

        int rc = use_tdd
            ? col_eval_stratum_tdd(&plan->strata[si], sess, si)
            : col_eval_stratum(&plan->strata[si], sess, si);
        sess->tdd_audit.enabled = false;

        if (tdd_profile_active) {
            tdd_profile_evaluated++;
            const char *first_rel = plan->strata[si].relation_count > 0
                ? plan->strata[si].relations[0].name : "(none)";
            fprintf(stderr,
                "TDD stratum idx=%u rel=%s recursive=%d use_tdd=%d "
                "fallback=%s "
                "total_ms=%.3f kfusion_ms=%.3f exchange_ms=%.3f "
                "tdd_ms=%.3f requested_workers=%u strategy=%s "
                "selected_workers=%u submitted_tasks=%" PRIu64 " "
                "completed_rounds=%" PRIu64 " replay=%s replay_rc=%d rc=%d "
                "worker_min_ms=%.3f worker_max_ms=%.3f worker_sum_ms=%.3f "
                "worker_delta_rows=%" PRIu64 " wait_ms=%.3f merge_ms=%.3f\n",
                si, first_rel ? first_rel : "(null)",
                (int)plan->strata[si].is_recursive, (int)use_tdd,
                wl_columnar_session_tdd_fallback_reason_name(
                    tdd_decision.fallback_reason),
                (double)(now_ns() - stratum_t0) / 1e6,
                (double)(sess->kfusion_ns - kfusion_base) / 1e6,
                (double)(sess->exchange_time_ns - exchange_base) / 1e6,
                (double)(sess->tdd_total_ns - tdd_base) / 1e6,
                sess->num_workers,
                sess->tdd_audit.strategy ? sess->tdd_audit.strategy : "none",
                sess->tdd_audit.selected_workers,
                sess->tdd_audit.submitted_tasks,
                sess->tdd_audit.completed_rounds,
                sess->tdd_audit.replay ? sess->tdd_audit.replay : "none",
                sess->tdd_audit.replay_rc, rc,
                (double)sess->tdd_audit.worker_min_ns / 1e6,
                (double)sess->tdd_audit.worker_max_ns / 1e6,
                (double)sess->tdd_audit.worker_sum_ns / 1e6,
                sess->tdd_audit.worker_delta_rows,
                (double)(sess->tdd_wait_barrier_ns - wait_base) / 1e6,
                (double)(sess->tdd_final_merge_ns - merge_base) / 1e6);
        }

        if (tdd_cc_active && rc == 0 && pre_saved) {
            uint32_t *tdd_counts
                = (uint32_t *)calloc(cnrels, sizeof(uint32_t));
            col_rel_t **tdd_saved
                = (col_rel_t **)calloc(cnrels, sizeof(col_rel_t *));
            if (tdd_counts && tdd_saved) {
                /* Save TDD results */
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    tdd_counts[ri] = r ? r->nrows : 0;
                    if (r && r->nrows > 0 && r->ncols > 0) {
                        tdd_saved[ri]
                            = col_rel_new_auto(r->name, r->ncols);
                        if (tdd_saved[ri])
                            col_rel_append_all(
                                tdd_saved[ri], r, NULL);
                    }
                }

                /* Restore pre-stratum IDB (base facts preserved) */
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    if (r) {
                        r->nrows = 0;
                        wl_columnar_relation_touch_view(r);
                    }
                    if (pre_saved[ri] && r) {
                        if (r->ncols == 0
                            && pre_saved[ri]->ncols > 0)
                            col_rel_set_schema(r,
                                pre_saved[ri]->ncols,
                                (const char *const *)
                                pre_saved[ri]->col_names);
                        col_rel_append_all(
                            r, pre_saved[ri], NULL);
                    }
                }

                /* Re-evaluate single-threaded */
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_session_test_before_correctness_replay)
                    wl_columnar_session_test_before_correctness_replay(sess);
#endif
                int st_rc = col_eval_stratum(csp, sess, si);
                if (st_rc != 0)
                    rc = st_rc;

                if (st_rc == 0) {
                    for (uint32_t ri = 0; ri < cnrels; ri++) {
                        col_rel_t *r = session_find_rel(
                            sess, csp->relations[ri].name);
                        uint32_t st = r ? r->nrows : 0;
                        if (st != tdd_counts[ri])
                            fprintf(stderr,
                                "[TDD-CHECK] stratum %u "
                                "relation '%s': TDD=%u "
                                "single=%u\n",
                                si, csp->relations[ri].name,
                                tdd_counts[ri], st);
                    }
                }

                /* Failed replay may retain registered dependencies. Free
                 * only private backups; let the existing error path preserve
                 * pending frames and their allocators before any reset. */
                for (uint32_t ri = 0; ri < cnrels; ri++) {
                    if (st_rc != 0) {
                        col_rel_destroy(tdd_saved[ri]);
                        continue;
                    }
                    col_rel_t *r = session_find_rel(
                        sess, csp->relations[ri].name);
                    if (r) {
                        r->nrows = 0;
                        wl_columnar_relation_touch_view(r);
                    }
                    if (tdd_saved[ri] && r) {
                        if (r->ncols == 0
                            && tdd_saved[ri]->ncols > 0)
                            col_rel_set_schema(r,
                                tdd_saved[ri]->ncols,
                                (const char *const *)
                                tdd_saved[ri]->col_names);
                        col_rel_append_all(
                            r, tdd_saved[ri], NULL);
                    }
                    col_rel_destroy(tdd_saved[ri]);
                }
            }
            free(tdd_counts);
            free((void *)tdd_saved);
        }
        if (pre_saved) {
            for (uint32_t ri = 0; ri < cnrels; ri++)
                col_rel_destroy(pre_saved[ri]);
            free((void *)pre_saved);
        }

        if (rc != 0) {
            if (wl_columnar_session_has_retained_cleanup(sess)) {
                sess->tdd_decision_tracking_active = false;
                return rc;
            }
            /* A reader may still depend on a delta after evaluation fails.
             * Checked removal keeps that exact registry owner on refusal;
             * continue reclaiming other deltas without losing the busy one. */
            int cleanup_rc = 0;
            for (uint32_t i = 0; i < sess->nrels; i++) {
                col_rel_t *r = sess->rels[i];
                if (r && strncmp(r->name, "$d$", 3) == 0) {
                    int remove_rc = session_remove_rel(sess, r->name);
                    if (cleanup_rc == 0)
                        cleanup_rc = remove_rc;
                }
            }
            /* Compact rels[] to close holes */
            uint32_t out = 0;
            for (uint32_t in = 0; in < sess->nrels; in++) {
                if (sess->rels[in] != NULL)
                    sess->rels[out++] = sess->rels[in];
            }
            sess->nrels = out;
            session_rel_free_hash(sess);
            sess->tdd_decision_tracking_active = false;
            col_session_reclaim_quiescent(sess);
            return cleanup_rc != 0 ? cleanup_rc : rc;
        }
        if (sess->eval_arena)
            wl_arena_reset(sess->eval_arena);
        col_session_note_stratum_done(sess, si);
    }
    sess->tdd_decision_tracking_active = false;

evaluated:;
    /*
     * Cut here: after evaluation, before compaction, above the bookkeeping.
     *
     * Above the bookkeeping because this path commits before it delivers.
     * Nothing below rolls has_evaluated, snapshot_stable_valid or the base_nrows
     * loop back, so a cutoff under them would report cancellation while leaving
     * the session claiming this pass succeeded.
     *
     * What makes a stop here safe for the retry is narrower than "nothing is
     * committed".  Plenty is: a drained step's whole commit block, the heads
     * session_snapshot_insert brought up to date, and
     * col_session_clear_idb_rows, which wipes the IDB rows.  The wipe is the
     * sharp edge -- a retry that took the stable fast path would publish an
     * empty model as a complete success.  It cannot, because clear_idb_rows
     * is only reached when pending_full_input_eval or pending_input_change is
     * set, nothing between that guard and this cutoff writes either one, and
     * both disqualify the fast path.  The insert route leaves the same flags
     * set, so a retry runs it again from the same appended rows, and finds
     * nothing the heads do not already hold.
     *
     * Before compaction, which is where this differs from the step path.  That
     * one cuts after compaction because both its shapes carry a resume token
     * across it and cutting earlier would strand a stopped step with no token;
     * this path sets no token and re-evaluates from the top, so the reason does
     * not transfer.  What does apply is the converse: col_rel_compact_many can
     * return EBUSY, and finish() prefers a non-zero execution_status over
     * control->stop, so compacting first would report EBUSY for an attempt the
     * caller asked to cancel.  Polling first makes cancellation win.
     *
     * This covers publication at the snapshot boundary.  It is not a claim
     * about streaming, but the shape of that gap is narrower than it looks:
     * nothing in eval*.c charges, yet this path's own strata cannot stream
     * either -- wl_columnar_delta_events_publish has one caller, inside
     * col_stratum_step_with_delta, whose only production caller is
     * col_session_step_impl.  Its other caller is a retraction helper that
     * file still marks UNUSED and that only a WL_SESSION_TEST_HOOKS wrapper
     * reaches.  So the one route by which a snapshot streams is the plain-step
     * drain above, and that returns through the step path's own cutoff, whose
     * rc this function propagates.  The unpolled fixpoint itself is the
     * operator-checkpoint item of #1953, not this one.
     *
     * The unwind performs the quiescent boundary, as the compaction failure
     * below does, and is deliberately not preceded by col_session_mem_sample.
     * Not because the sample would publish something a stopped pass should not:
     * on any route where a stratum evaluated, eval_serial.c and eval.c have
     * sampled the coordinator already and the STORED/TEMPORARY high-water marks
     * are raised above this point.  (The exceptions are the insert route,
     * which evaluates through bound slice runs rather than those stratum
     * evaluators, and a plan with no strata at all: there the sample below
     * may be this pass's first.)
     * Omitting it is inert either way -- the gauges are absolute, so the next
     * sample recomputes them, and a gauge one pass old costs
     * col_session_reclaim_quiescent at most a missed eviction of recomputable
     * cache entries, or one it did not need.
     */
    int cutoff_rc = col_session_publication_cutoff(sess);
    if (cutoff_rc != 0) {
        col_session_reclaim_quiescent(sess);
        return cutoff_rc;
    }

    /*
     * Compaction is a source mutation.  Do it before publishing the stable
     * snapshot bookkeeping so EBUSY leaves the session's retry state intact;
     * the compaction pass itself is index-ordered and not transactional.
     */
    int compact_rc = col_rel_compact_many(sess->rels, sess->nrels);
    if (compact_rc != 0) {
        col_session_reclaim_quiescent(sess);
        return compact_rc;
    }

    /* Reset after successful eval so next plain snapshot runs all strata. */
    session_retraction_cleanup(sess);
    sess->last_inserted_relation = NULL;
    sess->pending_input_change = false;
    sess->pending_full_input_eval = false;
    sess->has_evaluated = true;
    sess->uncommitted_strata_done = 0;
    sess->snapshot_stable_valid = true;

    /* Issue #83: Update base_nrows for all relations after convergence.
     * This marks the current state as "stable" so the next incremental
     * insert can compute the delta as rows[base_nrows..nrows). */
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *r = sess->rels[i];
        if (r)
            r->base_nrows = r->nrows;
    }
    session_retired_rows_clear(sess);

    /* Issue #1380: final STORED/TEMPORARY gauge sample for this pass. */
    col_session_mem_sample(sess);

    int snapshot_rc = col_session_emit_snapshot(plan, sess, callback,
            user_data);
    col_session_reclaim_quiescent(sess);
    if (tdd_profile_active && snapshot_rc == 0)
        fprintf(stderr, "TDD snapshot complete evaluated_count=%u rc=0\n",
            tdd_profile_evaluated);
    return snapshot_rc;
}

/* Callback-ready snapshot entries remain at stable addresses while scalar
 * readers and memory reservation tokens are live. Plan names are immutable
 * for the session lifetime; operation admission excludes session mutation. */
typedef struct {
    const char *name;
    uint32_t width, rows;
    size_t offset;
    bool initialized;
    wl_columnar_relation_scalar_source_t source;
} wl_session_snapshot_entry_t;

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_session_test_snapshot_checkpoint)(
    wl_evaluation_control_t *, unsigned, size_t);
int wl_columnar_session_test_snapshot_allocation = -1;
#endif

static int
wl_session_snapshot_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, size_t index)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_snapshot_checkpoint)
        wl_columnar_session_test_snapshot_checkpoint(control, phase, index);
#else
    (void)phase;
    (void)index;
#endif
    return wl_evaluation_control_charge(control, 0);
}

/* Private workspace credits are RESERVED until their physical storage is
 * freed. Neither token is moved or exposed to callbacks. Fixed admission
 * includes allocation bookkeeping and its mandatory free/refund on failure. */
static int
wl_session_snapshot_allocate(wl_col_session_t *sess, uint64_t bytes,
    wl_columnar_memory_reservation_t *credit, void **out)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc = wl_evaluation_control_charge(control, 128);
    if (rc) return rc;
    if (bytes > SIZE_MAX) return EOVERFLOW;
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(sess->memory_governor), bytes,
            credit, &status, control,
            wl_evaluation_control_active_attempt_id(control));
        if (rc) return rc;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_session_test_snapshot_allocation == 0) return ENOMEM;
    if (wl_columnar_session_test_snapshot_allocation > 0)
        --wl_columnar_session_test_snapshot_allocation;
#endif
    *out = malloc((size_t)bytes);
    if (!*out) return ENOMEM;
    return wl_session_snapshot_checkpoint(control, 5, (size_t)bytes);
}

static void
wl_session_snapshot_free(void *storage,
    wl_columnar_memory_reservation_t *credit)
{
    free(storage);
    if (atomic_load_explicit(&credit->state, memory_order_acquire)
        == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && !wl_columnar_memory_rollback(credit)) abort();
}

static int
wl_session_snapshot_prepare(wl_col_session_t *sess,
    wirelog_on_tuple_fn callback, void *user_data)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    const wl_plan_t *plan = sess->plan;
    size_t count = 0, initialized_entries = 0;
    wl_session_snapshot_entry_t *entries = NULL;
    int64_t *values = NULL;
    wl_columnar_memory_reservation_t metadata_credit, values_credit;
    int rc = wl_evaluation_control_charge(control, 256);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(&metadata_credit);
    wl_columnar_memory_reservation_init(&values_credit);
    for (uint32_t si = 0; si < plan->stratum_count; ++si) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto cleanup;
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) goto cleanup;
            if (count == SIZE_MAX) {
                rc = EOVERFLOW; goto cleanup;
            }
            ++count;
            rc = wl_session_snapshot_checkpoint(control, 1, count);
            if (rc) goto cleanup;
        }
    }
    if (!count) goto cutoff;
    if (count > SIZE_MAX / sizeof(*entries)) {
        rc = EOVERFLOW; goto cleanup;
    }
    size_t metadata_bytes = count * sizeof(*entries);
    void *metadata_storage = NULL;
    rc = wl_session_snapshot_allocate(sess, metadata_bytes, &metadata_credit,
            &metadata_storage);
    entries = metadata_storage;
    if (rc) goto cleanup;
    /* Every initialized entry also prepays its cleanup traversal slot. */
    for (size_t offset = 0; offset < (size_t)metadata_bytes;) {
        size_t chunk = (size_t)metadata_bytes - offset;
        if (chunk > 128) chunk = 128;
        rc = wl_evaluation_control_charge(control, chunk);
        if (rc) goto cleanup;
        memset((unsigned char *)entries + offset, 0, chunk);
        offset += chunk;
    }
    size_t index = 0;
    uint64_t cells = 0;
    for (uint32_t si = 0; si < plan->stratum_count; ++si) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto cleanup;
        for (uint32_t ri = 0; ri < plan->strata[si].relation_count; ++ri) {
            rc = wl_evaluation_control_charge(control, 32);
            if (rc) goto cleanup;
            if (index >= count) {
                rc = EOVERFLOW; goto cleanup;
            }
            wl_session_snapshot_entry_t *entry = &entries[index++];
            initialized_entries = index;
            entry->name = plan->strata[si].relations[ri].name;
            col_rel_t *relation = NULL;
            for (uint32_t slot = 0; slot < sess->nrels; ++slot) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc) goto cleanup;
                col_rel_t *r = sess->rels[slot];
                if (!r || !r->name || !entry->name) continue;
                size_t byte = 0;
                for (;;) {
                    rc = wl_evaluation_control_charge(control, 1);
                    if (rc) goto cleanup;
                    char a = r->name[byte], b = entry->name[byte];
                    rc = wl_session_snapshot_checkpoint(control, 4, byte);
                    if (rc) goto cleanup;
                    if (a != b) break;
                    if (!a) {
                        relation = r; break;
                    }
                    if (byte == SIZE_MAX) {
                        rc = EOVERFLOW; goto cleanup;
                    }
                    ++byte;
                }
                if (relation) break;
            }
            if (!relation || !relation->nrows) continue;
            rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
                &entry->source, &entry->initialized, control, id);
            if (rc) goto cleanup;
            rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
                &entry->source, relation, false);
            if (rc) goto cleanup;
            bool timestamped;
            rc = wl_columnar_relation_scalar_source_shape_attempt_controlled(
                &entry->source, &entry->width, &entry->rows, &timestamped);
            if (rc) goto cleanup;
            uint64_t entry_cells;
            entry->offset = (size_t)cells;
            if (!wl_columnar_memory_size_mul(entry->rows,
                entry->width ? entry->width : 1, &entry_cells)
                || !wl_columnar_memory_size_add(cells, entry_cells, &cells)
                || cells > SIZE_MAX) {
                rc = EOVERFLOW; goto cleanup;
            }
        }
    }
    if (cells) {
        uint64_t value_bytes;
        if (!wl_columnar_memory_size_mul(cells, sizeof(*values),
            &value_bytes)) {
            rc = EOVERFLOW; goto cleanup;
        }
        void *value_storage = NULL;
        rc = wl_session_snapshot_allocate(sess, value_bytes, &values_credit,
                &value_storage);
        values = value_storage;
        if (rc) goto cleanup;
    }
    for (size_t i = 0; i < count; ++i) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto cleanup;
        wl_session_snapshot_entry_t *entry = &entries[i];
        if (entry->rows && !values) {
            rc = EINVAL; goto cleanup;
        }
        for (uint32_t row = 0; row < entry->rows; ++row) {
            /* Prepay the finite, noninterruptible callback dispatch tail. */
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) goto cleanup;
            int64_t *tuple = values + entry->offset
                + (size_t)row * (entry->width ? entry->width : 1);
            rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
                &entry->source, row, tuple, entry->width);
            if (rc) goto cleanup;
            rc = wl_session_snapshot_checkpoint(control, 2, row);
            if (rc) goto cleanup;
        }
        if (entry->initialized) {
            rc = wl_columnar_relation_scalar_source_end(&entry->source, 0);
            entry->initialized = false;
            if (rc) goto cleanup;
        }
    }
cutoff:
    rc = wl_session_snapshot_checkpoint(control, 3, count);
    if (!rc && values) {
        for (size_t i = 0; i < count; ++i) {
            wl_session_snapshot_entry_t *entry = &entries[i];
            for (uint32_t row = 0; row < entry->rows; ++row)
                callback(entry->name, values + entry->offset
                    + (size_t)row * (entry->width ? entry->width : 1),
                    entry->width, user_data);
        }
    }
cleanup:
    for (size_t i = 0; i < initialized_entries; ++i) {
        if (entries[i].initialized) {
            int end_rc = wl_columnar_relation_scalar_source_end(
                &entries[i].source, rc);
            if (!rc) rc = end_rc;
        }
    }
    wl_session_snapshot_free(values, &values_credit);
    wl_session_snapshot_free(entries, &metadata_credit);
    return rc;
}

static int
col_session_snapshot_controlled(wl_session_t *session,
    wirelog_on_tuple_fn callback, void *user_data)
{
    wl_col_session_t *sess = COL_SESSION(session);
    wl_evaluation_control_t *control = session->evaluation_control;
    const void *owner = session->operation_admission;
    if (!control || !owner)
        return EINVAL;

    wl_controlled_step_state_t state =
        col_session_controlled_step_state(session);
    if (state == WL_CONTROLLED_STEP_PENDING)
        return EBUSY;
    if (state != WL_CONTROLLED_STEP_COMPLETE)
        return ENOTSUP;
    if (!callback)
        return EINVAL;

    int rc = wl_evaluation_control_begin(control, owner);
    if (rc)
        return rc;

    rc = wl_session_snapshot_prepare(sess, callback, user_data);
    return wl_evaluation_control_finish(control, owner, rc, 0);
}

static int
col_session_snapshot(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data)
{
    if (!session) return EINVAL;
    wl_col_session_t *sess = COL_SESSION(session);
    sess->cold_seal_closed = true;
    int cold_rc = wl_columnar_session_cold_execution_check(sess);
    if (cold_rc != 0) return cold_rc;
    if (session->evaluation_control)
        return col_session_snapshot_controlled(session, callback, user_data);
    return col_session_snapshot_impl(session, callback, user_data);
}

/* Affected strata/rules detection moved to columnar/frontier.c;
 * Mobius/Z-set functions moved to columnar/mobius.c;
 * declarations in columnar/internal.h. */
/* ======================================================================== */
/* Vtable Singleton                                                          */
/* ======================================================================== */

static const wl_compute_backend_t col_backend = {
    .name = "columnar",
    .session_create = col_session_create,
    .session_destroy = col_session_destroy,
    .session_insert = col_session_insert,
    .session_make_compound = col_session_make_compound,
    .session_remove = col_session_remove,
    .session_step = col_session_step,
    .session_set_delta_cb = col_session_set_delta_cb,
    .session_snapshot = col_session_snapshot,
    .session_memory_governor = col_session_memory_governor,
    .session_create_with_options = col_session_create_with_options,
    .session_budget_denied = wl_columnar_session_budget_denied,
    .session_budget_denial_clear = col_session_budget_denial_clear,
    .session_controlled_step_state = col_session_controlled_step_state,
    .session_controlled_step_authorize_insert =
        col_session_controlled_step_authorize_insert,
    .session_controlled_mutation_admit =
        col_session_controlled_mutation_admit,
    .session_controlled_mutation_committed =
        col_session_controlled_mutation_committed,
};

const wl_compute_backend_t *
wl_backend_columnar(void)
{
    return &col_backend;
}
