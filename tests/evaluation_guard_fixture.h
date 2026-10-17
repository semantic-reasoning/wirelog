/* Read-only observations for the explicit whole-session controlled gate.
 * Never copy a live token or use an observed pointer as an owning handle. */
#ifndef TEST_EVALUATION_GUARD_FIXTURE_H
#define TEST_EVALUATION_GUARD_FIXTURE_H

#include "../wirelog/columnar/internal.h"
#include "../wirelog/evaluation_control.h"

static inline uint64_t
wl_test_evaluation_guard_mix(uint64_t hash, uint64_t value)
{
    return (hash ^ value) * UINT64_C(1099511628211);
}

static inline uint64_t
wl_test_evaluation_guard_state(wl_col_session_t *sess)
{
    uint64_t h = UINT64_C(14695981039346656037);
#define VALUE(x) (h = wl_test_evaluation_guard_mix(h, (uint64_t)(x)))
#define POINTER(x) VALUE((uintptr_t)(x))
    POINTER(sess->rels); VALUE(sess->nrels); VALUE(sess->rel_cap);
    POINTER(sess->rel_hash_head); POINTER(sess->rel_hash_next);
    VALUE(sess->rel_hash_nbuckets); VALUE(sess->rel_hash_chain_cap);
    for (uint32_t i = 0; sess->rel_hash_head && i < sess->rel_hash_nbuckets;
        ++i)
        VALUE(sess->rel_hash_head[i]);
    for (uint32_t i = 0; sess->rel_hash_next && i < sess->nrels
        && i < sess->rel_hash_chain_cap; ++i)
        if (sess->rels[i]) VALUE(sess->rel_hash_next[i]);
    const wl_columnar_memory_reservation_t *tokens[] = {
        &sess->rels_reservation, &sess->rel_hash_reservation
    };
    for (unsigned i = 0; i < 2; ++i) {
        const wl_columnar_memory_reservation_t *t = tokens[i];
        POINTER(t->identity); POINTER(t->governor);
        VALUE(t->bytes); VALUE(t->replacement_bytes); VALUE(t->transition_kind);
        VALUE(atomic_load_explicit(&t->owner_bits, memory_order_acquire));
        VALUE(atomic_load_explicit(&t->state, memory_order_acquire));
    }
    for (uint32_t i = 0; i < sess->nrels; ++i) {
        col_rel_t *r = sess->rels[i]; POINTER(r);
        if (r) {
            VALUE(r->nrows); VALUE(r->ncols); VALUE(r->base_nrows);
            VALUE(r->sorted_nrows); VALUE(r->run_count);
            VALUE(r->view_generation); VALUE(r->storage_generation);
        }
    }
    POINTER(sess->cleanup_active); POINTER(sess->cleanup_pending);
    VALUE(sess->cleanup_active_count); VALUE(sess->cleanup_pending_count);
    VALUE(sess->cleanup_reserved_bytes); POINTER(sess->delta_observer);
    POINTER(sess->delta_rollback); VALUE(sess->delta_publish_active);
    VALUE(sess->delta_publish_cancelled); VALUE(sess->pending_input_change);
    VALUE(sess->pending_full_input_eval);
    VALUE(sess->plain_step_completion_pending);
    VALUE(sess->plain_step_completion_phase);
    VALUE(sess->plain_step_completion_active);
    POINTER(sess->last_removed_relation); VALUE(sess->retraction_seeded);
    POINTER(sess->last_inserted_relation); VALUE(sess->has_evaluated);
    VALUE(sess->snapshot_stable_valid); VALUE(sess->eval_stratum_heads_final);
    VALUE(sess->current_iteration); VALUE(sess->outer_epoch);
    VALUE(sess->memory_budget_denied); VALUE(sess->tdd_executed_strata);
    VALUE(sess->tdd_fallback_strata); VALUE(sess->tdd_last_active_workers);
    VALUE(sess->mat_cache.pin_epoch); POINTER(sess->progress.entries);
    VALUE(sess->progress.num_workers); VALUE(sess->progress.num_strata);
    for (uint32_t i = 0; sess->progress.entries
        && i < sess->progress.num_workers * sess->progress.num_strata; ++i) {
        VALUE(sess->progress.entries[i].outer_epoch);
        VALUE(sess->progress.entries[i].iteration);
    }
    if (sess->memory_governor) VALUE(wl_columnar_memory_reserved(
                wl_columnar_memory_governor_ref_get(sess->memory_governor)));
    VALUE(wl_evaluation_control_active_attempt_id(
            sess->base.evaluation_control));
    wl_evaluation_control_report_t report = { 0 };
    int rc = wl_evaluation_control_report(sess->base.evaluation_control,
            &report);
    VALUE(rc);
    if (!rc) {
        VALUE(report.attempt_id); VALUE(report.limit); VALUE(report.consumed);
        VALUE(report.stop_reason); VALUE(report.execution_status);
        VALUE(report.cleanup_status); VALUE(report.result);
    }
#undef POINTER
#undef VALUE
    return h;
}
#endif
