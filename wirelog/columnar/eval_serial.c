/*
 * columnar/eval_serial.c - serial stratum evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Serial (single-threaded) stratum evaluation extracted from
 * columnar/eval.c.
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Distributed (TDD) stratum evaluation is implemented in columnar/eval.c. */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel);

int
wl_columnar_eval_serial_recursive_pool_reset(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_idle(sess, control); if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *r = &sess->recursive;
    if (!r->active || !r->ready || r->phase != 2 || r->reset_active
        || !r->delta_rels || sess->cleanup_pending
        || sess->mat_cache.active_pins || sess->filt_cache_active_pins)
        return EBUSY;
    uint64_t extent; uint32_t slots;
    rc = wl_columnar_session_recursive_pool_extent(sess, &extent, &slots);
    if (rc) return rc;
    delta_pool_t *pool = sess->delta_pool;
    if ((sess->arr_count && !sess->arr_entries)
        || (sess->sarr_count && !sess->sarr_entries)
        || (sess->diff_arr_count && !sess->diff_arr_entries)
        || (sess->darr_count && !sess->darr_entries)) return EINVAL;
    for (uint32_t i = 0; i < sess->arr_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        if (sess->arr_entries[i].pin_count) return EBUSY;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    for (uint32_t i = 0; i < sess->sarr_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        if (sess->sarr_entries[i].sarr.pin_count) return EBUSY;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    for (uint32_t i = 0; i < sess->diff_arr_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        if (sess->diff_arr_entries[i].pin_count
            || sess->diff_arr_entries[i].transaction_pending) return EBUSY;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    for (uint32_t i = 0; i < sess->darr_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        if (sess->darr_entries[i].pin_count) return EBUSY;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    uint64_t capacity_extent;
    if (!wl_columnar_memory_size_mul(pool->slot_cap, pool->slot_size,
        &capacity_extent)
        || capacity_extent > SIZE_MAX
        || capacity_extent > UINTPTR_MAX - (uintptr_t)pool->slab) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    for (uint32_t i = 0; i < r->relation_count; i++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        col_rel_t *delta = r->delta_rels[i];
        uintptr_t address = (uintptr_t)delta;
        bool in_slab = delta && address >= (uintptr_t)pool->slab
            && address - (uintptr_t)pool->slab < extent;
        if (in_slab && (address - (uintptr_t)pool->slab) % pool->slot_size)
            return EINVAL;
        /* Membership is checked by address, without reading a relation
         * payload through an unheld alias. Unused slab slots are not live
         * journal owners; actual heap entries have separate lifetimes. */
        if (delta && address >= (uintptr_t)pool->slab
            && address - (uintptr_t)pool->slab < capacity_extent && !in_slab)
            return EINVAL;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!in_slab) continue;
        for (uint32_t j = 0; j < i; j++) {
            rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
            if (r->delta_rels[j] == delta) return EINVAL;
            rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        }
    }
    rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    r->reset_pool = pool; r->reset_slab = pool->slab;
    r->reset_stride = pool->slot_size; r->reset_high_water = slots;
    delta_pool_reset(pool);
    r->reset_active = true;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_eval_serial_recursive_pool_barrier(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_idle(sess, control); if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *r = &sess->recursive;
    if (!r->active || !r->ready || !r->delta_rels
        || sess->cleanup_pending) return EBUSY;
    if (!r->reset_active) return wl_evaluation_control_charge(control, 0);
    uint64_t extent; uint32_t slots;
    rc = wl_columnar_session_recursive_pool_extent(sess, &extent, &slots);
    if (rc) return rc;
    for (uint32_t i = 0; i < r->relation_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        if (r->delta_rels[i]) return EBUSY;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    r->reset_active = false; r->reset_pool = NULL; r->reset_slab = NULL;
    r->reset_stride = 0; r->reset_high_water = 0;
    return wl_evaluation_control_charge(control, 0);
}

/* The ordinary subpass uses the raw cache/sample/reset/rotation/GC order.
 * A failed nonterminal subpass is abandoned through the original owners;
 * this helper never resets an allowance or replays a consumed GC record. */
int
wl_columnar_eval_serial_recursive_boundary(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!sess->recursive.active || !sess->recursive.ready
        || sess->recursive.phase != 2 || sess->recursive.reset_active
        || sess->coordinator || sess->num_workers != 1
        || sess->rotation_ops != &col_rotation_standard_ops
        || sess->delta_observer || sess->delta_rollback)
        return ENOTSUP;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    uint32_t cursor = 0;
    rc = wl_columnar_cache_mat_retire_controlled(&sess->mat_cache, &cursor,
            true, control); if (rc) return rc;
    if (sess->cache_evict_threshold) {
        rc = wl_columnar_cache_mat_evict_controlled(&sess->mat_cache,
                sess->cache_evict_threshold, control);
    } else {
        cursor = 0;
        rc = wl_columnar_cache_mat_retire_controlled(&sess->mat_cache, &cursor,
                false, control);
    }
    if (rc) return rc;
    rc = wl_columnar_session_mem_sample_controlled(sess, control);
    if (rc) return rc;
    rc = wl_columnar_eval_serial_recursive_pool_reset(sess, control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_idle(sess, control); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!sess->recursive.reset_active || sess->cleanup_pending
        || sess->rotation_ops != &col_rotation_standard_ops) return EBUSY;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    /* STANDARD is a fixed used-counter reset; backing allocation and its
     * original credit remain. Compound collection is separately controlled. */
    sess->rotation_ops->rotate_eval_arena(sess);
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (!sess->compound_arena) return 0;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_arena_compound_arena_gc_record_t *gc = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_compound_gc(sess, frame, &gc, control);
    if (!rc) rc = wl_arena_compound_arena_gc_step(gc, sess->compound_arena,
                control);
    if (gc) rc = wl_arena_compound_arena_gc_retire(gc, rc, rc ? NULL : control);
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    return cleanup && rc <= 0 ? cleanup : rc;
}

/* Persistent stratum payload uses its original session reservation. A rule
* frame may retain registered deltas, so callers drain that frame first. */
int
wl_columnar_eval_serial_recursive_retire(wl_col_session_t *sess, int primary,
    wl_evaluation_control_t *control)
{
    if (!sess) return primary > 0 ? primary : EINVAL;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    if (!record->active) {
        /* A terminal registered sweep can outlive private delta/snap owners.
         * Its original consumed removal authority is still real cleanup. */
        if (sess->serial_completion.pending
            && sess->serial_completion.phase ==
            WL_COLUMNAR_SERIAL_PHASE_RECURSIVE_TERMINAL
            && record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL)
            return wl_columnar_session_recursive_removal_drain(sess, primary);
        return primary;
    }
    int idle = wl_columnar_eval_stack_cleanup_idle(sess, NULL);
    if (idle) {
        record->cleanup_pending = true;
        return primary > 0 ? primary : idle;
    }
    int rc = primary;
    if (record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL) {
        rc = wl_columnar_session_recursive_removal_drain(sess, rc);
        if (record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL)
            return rc;
    }
    if (record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTERED) {
        if (!record->delta_rels || record->relation >= record->relation_count)
            return rc > 0 ? rc : EINVAL;
        col_rel_t *registered = record->delta_rels[record->relation];
        if (!registered || registered->pool_owned)
            return rc > 0 ? rc : EINVAL;
        /* This slot is a borrowed cleanup reference after actual registry
         * publication. Its original name credit is the only pending owner. */
        if (registered->pool_name_reserved_bytes
            && !wl_columnar_memory_release(
                &registered->pool_name_reservation)){
            record->cleanup_pending = true;
            return rc > 0 ? rc : EINVAL;
        }
        registered->pool_name_reserved_bytes = 0;
        record->delta_rels[record->relation] = NULL;
        record->phase = 2;
    }
    if (!record->delta_rels)
        record->retirement_cursor = record->relation_count;
    while (record->retirement_cursor < record->relation_count) {
        if (control && !rc) {
            rc = wl_evaluation_control_charge(control, 8);
            if (!rc) rc = wl_evaluation_control_charge(control, 0);
        }
        uint32_t index = record->retirement_cursor;
        col_rel_t *delta =
            record->delta_rels ? record->delta_rels[index] : NULL;
        if (delta) {
            /* Existing checked physical disposal exclusion. No charged
             * width-loop claim is made for relation/Arrow destruction. */
            int cleanup = col_rel_destroy_checked(delta);
            if (cleanup) {
                record->cleanup_pending = true;
                return rc > 0 ? rc : cleanup;
            }
            record->delta_rels[index] = NULL;
        }
        record->retirement_cursor++;
        if (control && !rc) rc = wl_evaluation_control_charge(control, 0);
        /* Once a work stop occurs, the rest is mandatory checked disposal;
        * it does not admit fresh work or reset/refund consumed control. */
    }
    free(record->snap); record->snap = NULL;
    free(record->delta_rels); record->delta_rels = NULL;
    if ((record->reservation.governor || record->reservation.bytes)
        && !wl_columnar_memory_rollback(&record->reservation)) {
        record->cleanup_pending = true;
        return rc > 0 ? rc : EINVAL;
    }
    /* Successful rollback retains diagnostic scalar bytes in the token.
     * Only now, after physical NULL and actual credited terminal state, may
     * the original slot be initialized for a future host invocation. */
    wl_columnar_memory_reservation_init(&record->reservation);
    sess->diff_operators_active = record->saved_diff_operators_active;
    record->active = false; record->ready = false;
    record->cleanup_pending = false; record->phase = 0;
    record->plan = NULL; record->stratum = NULL;
    record->relation_count = 0; record->retirement_cursor = 0;
    record->reset_active = false; record->reset_pool = NULL;
    record->reset_slab = NULL;
    record->reset_stride = 0; record->reset_high_water = 0;
    return rc;
}

int
wl_columnar_eval_serial_recursive_begin(wl_col_session_t *sess,
    const wl_plan_t *plan, uint32_t stratum_index,
    wl_evaluation_control_t *control)
{
    if (!sess || !plan || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    if (record->active || record->cleanup_pending || record->delta_rels
        || record->snap || record->reservation.bytes
        || record->reservation.governor || record->reservation.replacement_bytes
        || record->reset_active || record->reset_pool || record->reset_slab
        || record->reset_stride || record->reset_high_water)
        return EBUSY;
    if (stratum_index >= plan->stratum_count || !plan->strata) return EINVAL;
    const wl_plan_stratum_t *stratum = &plan->strata[stratum_index];
    if (!stratum->is_recursive || !stratum->relation_count ||
        !stratum->relations)
        return EINVAL;
    uint64_t pointers, snapshots, bytes;
    if (!wl_columnar_memory_size_mul(stratum->relation_count,
        sizeof(col_rel_t *), &pointers)
        || !wl_columnar_memory_size_mul(stratum->relation_count,
        sizeof(uint32_t), &snapshots)
        || !wl_columnar_memory_size_add(pointers, snapshots, &bytes)
        || pointers > SIZE_MAX || snapshots > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    record->plan = plan; record->stratum = stratum;
    record->stratum_index = stratum_index;
    record->relation_count = stratum->relation_count;
    record->iteration = record->subpass = record->relation = record->cursor = 0;
    record->retirement_cursor = record->rule_id_base =
        record->effective_iteration = 0;
    record->saved_diff_operators_active = sess->diff_operators_active;
    record->active = true; record->ready = false; record->phase = 1;
    record->canonical_consumed = false; record->cleanup_pending = false;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(sess->memory_governor), bytes,
            &record->reservation, &status, control);
        if (rc) goto fail;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status ==
                WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : ENOMEM;
            goto fail;
        }
    }
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto fail;
    record->delta_rels = calloc(record->relation_count, sizeof(col_rel_t *));
    if (!record->delta_rels) {
        rc = ENOMEM; goto fail;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto fail;
    record->snap = malloc((size_t)snapshots);
    if (!record->snap) {
        rc = ENOMEM; goto fail;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    record->ready = true; record->phase = 2;
    return 0;
fail:
    return wl_columnar_eval_serial_recursive_retire(sess, rc, NULL);
}

int
wl_columnar_eval_serial_recursive_clone(wl_col_session_t *sess,
    col_rel_t *source,
    const char *name, uint32_t index, wl_evaluation_control_t *control)
{
    if (!sess || !source || !name || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    if (!record->active || !record->ready || !record->delta_rels
        || index >= record->relation_count ||
        record->delta_rels[index]) return EINVAL;
    if (record->reset_active) return EBUSY;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_merge_incremental_scratch_t *scratch = NULL;
    rc = wl_columnar_eval_stack_cleanup_incremental_owner(sess, frame,
            &authority, &scratch, control); if (rc) goto done;
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    rc = wl_columnar_relation_incremental_acquire(authority, source, NULL,
            control);
    if (rc) goto done;
    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : source->memory_governor;
    rc = wl_columnar_relation_pool_new_like_held_controlled(&result->rel,
            sess->delta_pool, name, source, governor, control);
    if (result->rel) result->owned = true;
    if (rc) goto done;
    rc = wl_columnar_merge_incremental_retire(authority, scratch, 0);
    if (rc) goto done;
    if (!result->rel->capacity) {
        rc = wl_columnar_relation_incremental_acquire(authority, result->rel,
                NULL, control); if (rc) goto done;
        rc = wl_columnar_relation_incremental_reserve_rows(bulk, result->rel,
                authority, 0, COL_REL_INIT_CAP, false, control);
        if (rc) goto done;
        rc = wl_columnar_merge_incremental_retire(authority, scratch, 0);
        if (rc) goto done;
    }
    rc = wl_columnar_relation_incremental_acquire(authority, result->rel,
            NULL, control); if (rc) goto done;
    rc = wl_columnar_relation_incremental_timestamps(bulk, authority, 0,
            control);
    if (rc) goto done;
    rc = wl_columnar_merge_incremental_retire(authority, scratch, 0);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 64); if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    /* Physical owner moves once at the checked scalar cutoff. No stopped
     * frame may retain a duplicate owning alias to this original local slot. */
    record->delta_rels[index] = result->rel;
    result->rel = NULL; result->owned = false;
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

int
wl_columnar_eval_serial_recursive_snapshot(wl_col_session_t *sess,
    uint32_t index, wl_evaluation_control_t *control)
{
    if (!sess || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *r = &sess->recursive;
    if (!r->active || !r->ready || r->phase != 2 || !r->stratum
        || !r->snap || index >= r->relation_count) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    col_rel_t *head = NULL;
    rc = wl_columnar_join_controlled_lookup(sess,
            r->stratum->relations[index].name, &head, control);
    if (rc) return rc;
    if (!head) return ENOENT;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_sources(frame);
    if (!group) rc = EINVAL;
    else rc = wl_columnar_relation_source_group_acquire_controlled(group, head,
                NULL, control);
    if (!rc) {
        rc = wl_evaluation_control_charge(control, 16);
        if (!rc) {
            r->snap[index] = head->nrows;
            rc = wl_evaluation_control_charge(control, 0);
        }
    }
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    return cleanup && rc <= 0 ? cleanup : rc;
}

int
wl_columnar_eval_serial_recursive_register_head(wl_col_session_t *sess,
    const char *name, wl_evaluation_control_t *control)
{
    if (!sess || !name || !control || sess->recursive.active) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    int rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_session_promotion_scratch_t *state = NULL;
    wl_columnar_session_hash_controlled_registry_t *registry = NULL;
    rc = wl_columnar_eval_stack_cleanup_promotion_owner(sess, frame,
            &authority, &state, control); if (rc) goto done;
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    rc = wl_columnar_relation_descriptor_prepare(bulk, state, result,
            name, sess->memory_governor, control); if (rc) goto done;
    rc = wl_columnar_relation_descriptor_handoff(bulk, state, result,
            wl_columnar_eval_stack_cleanup_stack(frame), control);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_cleanup_descriptor_registry(frame, &registry,
            control); if (rc) goto done;
    rc = wl_columnar_session_hash_registry_controlled(registry, sess,
            result->rel, control);
    /* Actual publication survives a positive old-image cleanup refusal. */
    wl_columnar_eval_stack_cleanup_descriptor_published(frame);
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

int
wl_columnar_eval_serial_recursive_register_delta(wl_col_session_t *sess,
    uint32_t index, wl_evaluation_control_t *control)
{
    if (!sess || !control) return EINVAL;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!record->active || !record->ready || record->phase != 2
        || !record->delta_rels || index >= record->relation_count
        || !record->delta_rels[index] || !record->delta_rels[index]->pool_owned)
        return EINVAL;
    col_rel_t *source = record->delta_rels[index];
    record->relation = index;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_session_promotion_scratch_t *state = NULL;
    rc = wl_columnar_eval_stack_cleanup_promotion_owner(sess, frame,
            &authority, &state, control); if (rc) goto done;
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    rc = wl_columnar_relation_incremental_acquire(authority, source, NULL,
            control);
    if (rc) goto done;
    rc = wl_columnar_relation_descriptor_prepare(bulk, state, result,
            source->name, sess->memory_governor, control); if (rc) goto done;
    rc = wl_columnar_session_promotion_move_credits(authority, state, result,
            control);
    if (rc) goto done;
    rc = wl_columnar_session_promotion_stage_payload(authority, state, result,
            control);
    if (rc) goto done;
    rc = wl_columnar_session_promotion_publish_payload(sess, authority, state,
            result, control);
    if (record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_PROMOTED)
        return wl_columnar_eval_serial_recursive_register_promoted(sess, index,
                   &frame, control, rc);
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

int
wl_columnar_eval_serial_recursive_register_promoted(wl_col_session_t *sess,
    uint32_t index, wl_columnar_eval_stack_cleanup_frame_t **handle,
    wl_evaluation_control_t *control, int primary)
{
    if (!sess || !handle || !*handle || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = *handle;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    if (!record->active || !record->ready || !record->delta_rels
        || index >= record->relation_count || record->relation != index
        || !record->delta_rels[index]
        || record->phase != WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_PROMOTED
        || sess->cleanup_active != frame ||
        sess->serial_completion.mode_frame != frame
        || sess->serial_completion.mode != WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || !sess->serial_completion.payload_ready) return EINVAL;
    int rc = 0;
    wl_columnar_session_hash_controlled_registry_t *registry = NULL;
    rc = wl_columnar_eval_stack_cleanup_registry(frame, &registry, control);
    if (!rc) rc = wl_columnar_session_hash_registry_controlled(registry,
                sess, record->delta_rels[index], control);
    return wl_columnar_eval_serial_recursive_register_finish(sess, index,
               handle, registry, rc, primary);
}

int
wl_columnar_eval_serial_recursive_register_finish(wl_col_session_t *sess,
    uint32_t index, wl_columnar_eval_stack_cleanup_frame_t **handle,
    wl_columnar_session_hash_controlled_registry_t *registry, int rc,
    int primary)
{
    if (!sess || !handle || !*handle || sess->cleanup_active != *handle
        || sess->serial_completion.mode_frame != *handle
        || sess->serial_completion.mode ==
        WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || (registry &&
        sess->serial_completion.mode != WL_COLUMNAR_SERIAL_FRAME_REGISTRY))
        return EINVAL;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    if (!record->active || !record->ready || !record->delta_rels
        || index >= record->relation_count || record->relation != index
        || !record->delta_rels[index]
        || record->phase != WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_PROMOTED
        || !wl_columnar_eval_stack_cleanup_registry_matches(*handle, registry))
        return EINVAL;
    if (registry && registry->published) {
        /* Registry owns the descriptor now. Keep only a borrowed name
         * retirement reference; no retry can roll it back or insert twice. */
        record->canonical_consumed = true;
        record->phase = WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTERED;
    } else if (sess->serial_completion.mode ==
        WL_COLUMNAR_SERIAL_FRAME_REGISTRY){
        record->phase = WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTRY_FAILED;
    }
    if (primary > 0 || (!rc && primary)) rc = primary;
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(handle);
        if (cleanup && rc <= 0) rc = cleanup;
    }
    if (record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_ROLLBACK_RESTORED
        && !sess->cleanup_pending) record->phase = 2;
    if (record->phase == WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTERED) {
        col_rel_t *registered = record->delta_rels[index];
        if (registered->pool_name_reserved_bytes
            && !wl_columnar_memory_release(
                &registered->pool_name_reservation)){
            record->cleanup_pending = true;
            return rc > 0 ? rc : EINVAL;
        }
        registered->pool_name_reserved_bytes = 0;
        record->delta_rels[index] = NULL;
        record->phase = 2;
    }
    return rc;
}

/* Initial IDB sorting preserves every row and timestamp. It does not perform
 * the incremental merge's timestamp retirement or duplicate compaction. */
int
wl_columnar_eval_serial_recursive_initial_sort(wl_col_session_t *sess,
    col_rel_t *source, wl_evaluation_control_t *control)
{
    if (!sess || !source || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    int rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_merge_incremental_scratch_t *scratch = NULL;
    rc = wl_columnar_eval_stack_cleanup_incremental_owner(sess, frame,
            &authority, &scratch, control); if (rc) goto done;
    rc = wl_columnar_relation_incremental_acquire(authority, source, NULL,
            control); if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    uint32_t count = source->nrows;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    if (count <= 1u) goto done;
    rc = wl_evaluation_control_charge(control, 96); if (rc) goto done;
    scratch->lane = 1u;
    wl_columnar_memory_reservation_init(&scratch->sort.workspace.admission);
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    rc = wl_columnar_relation_incremental_radix_prepare(source, 0, count,
            &scratch->sort.workspace, &authority->leases[0], control);
    if (rc) goto done;
    rc = wl_columnar_relation_incremental_radix_execute(source, 0, count,
            &scratch->sort.workspace, authority, 0,
            wl_columnar_eval_stack_cleanup_bulk_append(frame), control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    source->sorted_nrows = count;
    authority->set.published = true;
    rc = wl_evaluation_control_charge(control, 0);
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

/* Frontier and net multiplicity scans retain the same original reader group
 * until every visited timestamp has been admitted. Raw chooses the minimum
 * timestamp within a relation; the stratum caller folds those frontiers. */
int
wl_columnar_eval_serial_recursive_frontier(wl_col_session_t *sess,
    col_rel_t *source, col_frontier_t *out, int64_t *net,
    wl_evaluation_control_t *control)
{
    if (!sess || !source || !out || !net || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    int rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_sources(frame);
    if (!group) {
        rc = EINVAL; goto done;
    }
    rc = wl_columnar_relation_source_group_acquire_controlled(group,
            source, NULL, control); if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 32); if (rc) goto done;
    uint32_t count = source->nrows;
    if (count > source->capacity || (source->timestamps
        && source->timestamp_capacity < count)) {
        rc = EINVAL; goto done;
    }
    col_frontier_t frontier = {0, 0};
    int64_t sum = 0;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    if (source->timestamps) for (uint32_t row = 0; row < count; row++) {
            rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
            const col_delta_timestamp_t *ts = &source->timestamps[row];
            if (!row || ts->iteration < frontier.iteration
                || (ts->iteration == frontier.iteration &&
                ts->stratum < frontier.stratum)){
                frontier.iteration = ts->iteration;
                frontier.stratum = ts->stratum;
            }
            if ((ts->multiplicity > 0 && sum > INT64_MAX - ts->multiplicity)
                || (ts->multiplicity < 0 &&
                sum < INT64_MIN - ts->multiplicity)){
                rc = EOVERFLOW; goto done;
            }
            sum += ts->multiplicity;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        }
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    *out = frontier; *net = sum;
    rc = wl_evaluation_control_charge(control, 0);
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

/* Canonical prefix and private delta retain separate original authorities.
 * Each storage transition retires before acquiring the next genuine epoch. */
int
wl_columnar_eval_serial_recursive_consolidate(wl_col_session_t *sess,
    col_rel_t *source, uint32_t index, uint32_t old_rows, int *fast,
    wl_evaluation_control_t *control)
{
    if (!sess || !source || !control || !fast) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    if (!record->active || !record->ready || record->phase != 2
        || !record->delta_rels || !record->stratum
        || index >= record->relation_count || old_rows > source->nrows
        || record->delta_rels[index]) return EINVAL;
    rc = wl_columnar_eval_serial_recursive_clone(sess, source,
            record->stratum->relations[index].delta_name, index, control);
    if (rc) return rc;
    col_rel_t *delta = record->delta_rels[index];
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_merge_incremental_scratch_t *scratch = NULL;
    rc = wl_columnar_eval_stack_cleanup_incremental_owner(sess, frame,
            &authority, &scratch, control); if (rc) goto done;
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    rc = wl_columnar_merge_incremental_controlled(authority, scratch, bulk,
            source, old_rows, delta, fast, control);
    if (rc) goto done;
    if (delta->nrows) {
        rc = wl_columnar_relation_incremental_acquire(authority, delta, NULL,
                control);
        if (rc) goto done;
        rc = wl_columnar_relation_incremental_stamp(authority, 0,
                record->effective_iteration, record->stratum_index, control);
        if (rc) goto done;
        rc = wl_columnar_merge_incremental_retire(authority, scratch, 0);
        if (rc) goto done;
        rc = wl_columnar_relation_incremental_acquire(authority, source, NULL,
                control);
        if (rc) goto done;
        rc = wl_columnar_relation_incremental_timestamps(bulk, authority, 0,
                control);
    }
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

static bool
col_reduce_output_group_equal(const col_rel_t *rel, uint32_t a, uint32_t b,
    uint32_t group_by_count, uint32_t aggregate_index)
{
    for (uint32_t c = 0; c < group_by_count; c++) {
        uint32_t out_col = c >= aggregate_index ? c + 1 : c;
        if (col_rel_get(rel, a, out_col)
            != col_rel_get(rel, b, out_col))
            return false;
    }
    return true;
}

typedef struct {
    uint64_t hash;
    uint32_t row;
} col_group_slot_t;

static uint64_t
col_group_hash(const col_rel_t *rel, uint32_t row, uint32_t group_by_count,
    uint32_t aggregate_index)
{
    uint64_t hash = UINT64_C(1469598103934665603);
    for (uint32_t c = 0; c < group_by_count; c++) {
        uint32_t out_col = c >= aggregate_index ? c + 1 : c;
        hash ^= (uint64_t)col_rel_get(rel, row, out_col);
        hash *= UINT64_C(1099511628211);
    }
    return hash ? hash : 1;
}

int
wl_columnar_eval_serial_aggregate_retire(
    wl_columnar_merge_incremental_authority_t *authority,
    wl_columnar_eval_serial_aggregate_scratch_t *record, int primary)
{
    if (!record || !record->ready)
        return primary > 0 ? primary : EINVAL;
    col_rel_t *rel = record->relation;
    if (record->changed && !record->metadata_set) {
        int cleanup = authority && authority->active
            && authority->source == rel
            ? col_rel_mutation_lease_validate(&authority->leases[0], rel)
            : EINVAL;
        if (cleanup) return primary > 0 ? primary : cleanup;
        if (rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
            return primary > 0 ? primary : EOVERFLOW;
        if (record->finalized) rel->nrows = record->output_rows;
        rel->sorted_nrows = record->finalized ? record->output_rows : 0;
        rel->base_nrows = record->finalized ? record->output_rows : 0;
        rel->run_count = 0;
        memset(rel->run_ends, 0, sizeof(rel->run_ends));
        wl_columnar_relation_touch_view(rel);
        authority->set.published = true;
        record->metadata_set = true;
    }
    if (record->changed) {
        int cleanup = wl_columnar_eval_dedup_retire_checked(rel,
                &record->dedup_disposed, primary);
        if (rel->dedup_reserved_bytes) return cleanup;
        if (cleanup > 0 && primary <= 0) return cleanup;
    }
    free(record->groups);
    record->groups = NULL;
    if (record->admission_active) {
        wl_columnar_memory_reservation_t *token = &record->admission;
        if (!rel || !rel->memory_governor || token->identity != token
            || token->bytes != record->bytes
            || token->governor != wl_columnar_memory_governor_ref_get(
                rel->memory_governor)
            || atomic_load_explicit(&token->state, memory_order_acquire)
            != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            || !wl_columnar_memory_rollback(token))
            return primary > 0 ? primary : EBUSY;
        record->admission_active = false;
    }
    return primary;
}

static int
wl_columnar_eval_serial_canonicalize_aggregate_locked(col_rel_t *rel,
    const wl_plan_agg_spec_t *spec, const wl_intern_t *intern)
{
    if (!rel || !spec || !spec->has_spec || rel->nrows < 2)
        return 0;

    uint32_t gc = spec->group_by_count;
    uint32_t agg_index = spec->aggregate_index < rel->ncols
        ? spec->aggregate_index : gc;
    if (rel->ncols <= gc)
        return EINVAL;

    uint32_t old_nrows = rel->nrows;
    uint32_t out = 0;
    bool logical_changed = false;
    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)rel->nrows * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired)
        return ENOMEM;
    uint64_t group_bytes = 0;
    if (!wl_columnar_memory_size_mul(map_cap, sizeof(col_group_slot_t),
        &group_bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t group_reservation;
    wl_columnar_memory_reservation_init(&group_reservation);
    if (rel->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(rel->memory_governor),
                group_bytes, &group_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC : ENOMEM;
    }
    col_group_slot_t *groups = (col_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups) {
        wl_columnar_memory_release(&group_reservation);
        return ENOMEM;
    }
    uint32_t map_mask = map_cap - 1;
    for (uint32_t row = 0; row < rel->nrows; row++) {
        uint32_t found = UINT32_MAX;
        uint64_t hash = col_group_hash(rel, row, gc, agg_index);
        uint32_t slot = (uint32_t)hash & map_mask;
        while (groups[slot].hash != 0) {
            uint32_t keep = groups[slot].row;
            if (groups[slot].hash == hash
                && col_reduce_output_group_equal(rel, keep, row, gc,
                agg_index)) {
                found = keep;
                break;
            }
            slot = (slot + 1) & map_mask;
        }

        if (found != UINT32_MAX) {
            int64_t cur = col_rel_get(rel, found, agg_index);
            int64_t val = col_rel_get(rel, row, agg_index);
            /* Same comparator as col_op_reduce(): a fixpoint that ordered
             * lexicographically within an iteration and by intern id across
             * iterations would be worse than the original bug (#965). */
            bool better = col_agg_better(spec->fn, spec->operand_type,
                    intern, val, cur);
            if (better) {
                rel->columns[agg_index][found] = val;
                if (rel->timestamps)
                    rel->timestamps[found] = rel->timestamps[row];
                logical_changed = true;
            }
            continue;
        }

        if (out != row) {
            for (uint32_t c = 0; c < rel->ncols; c++)
                rel->columns[c][out] = rel->columns[c][row];
            if (rel->timestamps)
                rel->timestamps[out] = rel->timestamps[row];
            logical_changed = true;
        }
        groups[slot].hash = hash;
        groups[slot].row = out;
        out++;
    }

    if (logical_changed || out != old_nrows) {
        rel->nrows = out;
        rel->sorted_nrows = out;
        rel->base_nrows = out;
        rel->run_count = 0;
        wl_columnar_eval_dedup_set_clear(rel);
        memset(rel->run_ends, 0, sizeof(rel->run_ends));
        wl_columnar_relation_touch_view(rel);
    }

    free(groups);
    wl_columnar_memory_release(&group_reservation);

    return 0;
}

static int
wl_columnar_eval_serial_canonicalize_aggregate(col_rel_t *rel,
    const wl_plan_agg_spec_t *spec, const wl_intern_t *intern,
    wl_col_session_t *sess)
{
    if (!rel || !spec || !spec->has_spec || rel->nrows < 2)
        return 0;
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc = col_rel_source_writer_acquire(rel, &writer);
    if (rc != 0)
        return rc;
    col_rel_t *owner = NULL;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc == 0 && (owner != rel
        || col_rel_storage_alias_borrow_count(owner) != 0))
        rc = EBUSY;
    if (rc == 0)
        rc = wl_columnar_eval_serial_canonicalize_aggregate_locked(rel,
                spec, intern);
    int release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == ENOSPC)
        sess->memory_budget_denied = true;
    return rc != 0 ? rc : release_rc;
}

int
wl_columnar_eval_serial_canonicalize_aggregate_controlled(col_rel_t *rel,
    const wl_plan_agg_spec_t *spec, wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    if (!rel || !spec || !spec->has_spec)
        return wl_evaluation_control_charge(control, 0);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_merge_incremental_authority_t *authority = NULL;
    wl_columnar_merge_incremental_scratch_t *scratch = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_incremental_owner(sess, frame,
            &authority, &scratch, control);
    if (rc) goto done;
    rc = wl_columnar_relation_incremental_acquire(authority, rel, NULL,
            control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto done;
    if (authority->leases[0].owner != rel
        || col_rel_storage_alias_borrow_count(rel)) {
        rc = EBUSY; goto done;
    }
    if (rel->nrows < 2) goto done;
    uint32_t gc = spec->group_by_count;
    uint32_t aggregate = spec->aggregate_index < rel->ncols
        ? spec->aggregate_index : gc;
    if (rel->ncols <= gc || rel->nrows > rel->capacity
        || !rel->columns || (rel->timestamps
        && rel->timestamp_capacity < rel->nrows)) {
        rc = EINVAL; goto done;
    }
    if (spec->fn != WIRELOG_AGG_MIN && spec->fn != WIRELOG_AGG_MAX) {
        rc = ENOTSUP; goto done;
    }
    if (rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        rc = EOVERFLOW; goto done;
    }
    if ((!rel->dedup_cap && (rel->dedup_slots || rel->dedup_count))
        || (rel->dedup_cap && (!rel->dedup_slots
        || (rel->dedup_cap & (rel->dedup_cap - 1u))))
        || rel->dedup_count > rel->dedup_cap) {
        rc = EINVAL; goto done;
    }
    wl_columnar_memory_reservation_t *dedup = &rel->dedup_reservation;
    uint64_t dedup_state = atomic_load_explicit(&dedup->state,
            memory_order_acquire);
    if (rel->dedup_reserved_bytes) {
        if (!rel->memory_governor || dedup->identity != dedup
            || rel->dedup_reserved_bytes
            != (uint64_t)rel->dedup_cap * sizeof(uint64_t)
            || dedup->bytes != rel->dedup_reserved_bytes
            || dedup->governor != wl_columnar_memory_governor_ref_get(
                rel->memory_governor)
            || dedup_state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            || atomic_load_explicit(&dedup->owner_bits, memory_order_acquire)
            != (uintptr_t)rel) {
            rc = EBUSY; goto done;
        }
    } else if ((rel->dedup_cap && rel->memory_governor)
        || (dedup_state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && dedup_state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)) {
        rc = EBUSY; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto done;
    for (uint32_t c = 0; c < rel->ncols; c++) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto done;
        if (!rel->columns[c]) {
            rc = EINVAL; goto done;
        }
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto done;
    }
    /* The old scratch is inert; lane2 remains safe throughout partial clear. */
    scratch->lane = 2;
    for (size_t at = 0; at < sizeof(scratch->aggregate);) {
        size_t n = sizeof(scratch->aggregate) - at;
        if (n > 128) n = 128;
        rc = wl_evaluation_control_charge(control, n);
        if (rc) goto done;
        memset((char *)&scratch->aggregate + at, 0, n);
        at += n;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 96);
    if (rc) goto done;
    wl_columnar_eval_serial_aggregate_scratch_t *record = &scratch->aggregate;
    wl_columnar_memory_reservation_init(&record->admission);
    record->relation = rel;
    record->spec = spec;
    record->old_nrows = rel->nrows;
    record->ready = true;
    scratch->lane = 5;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto done;
    record->map_cap = 1;
    uint64_t desired = (uint64_t)rel->nrows * 2u;
    while ((uint64_t)record->map_cap < desired) {
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) goto done;
        if (record->map_cap > UINT32_MAX / 2u) {
            rc = ENOMEM; goto done;
        }
        record->map_cap <<= 1;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) goto done;
    if (!wl_columnar_memory_size_mul(record->map_cap,
        sizeof(col_group_slot_t), &record->bytes)) {
        rc = EOVERFLOW; goto done;
    }
    if (record->bytes > SIZE_MAX) {
        rc = ENOMEM; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto done;
    if (rel->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(rel->memory_governor),
            record->bytes, &record->admission, &status, control);
        uint64_t state = atomic_load_explicit(&record->admission.state,
                memory_order_acquire);
        record->admission_active = record->admission.governor != NULL
            && state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED;
        if (rc) goto done;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : ENOMEM;
            if (rc == ENOSPC) sess->memory_budget_denied = true;
            goto done;
        }
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto done;
    record->groups = malloc((size_t)record->bytes);
    if (!record->groups) {
        rc = ENOMEM; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto done;
    for (size_t at = 0; at < (size_t)record->bytes;) {
        size_t n = (size_t)record->bytes - at;
        if (n > 128) n = 128;
        rc = wl_evaluation_control_charge(control, n);
        if (rc) goto done;
        memset((char *)record->groups + at, 0, n);
        at += n;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto done;
    }
    col_group_slot_t *groups = record->groups;
    uint32_t mask = record->map_cap - 1;
    uint64_t hashed = 0, probed = 0, winners = 0, copied = 0;
    for (; record->row < record->old_nrows; record->row++) {
        uint32_t row = record->row, found = UINT32_MAX;
        uint64_t hash = UINT64_C(1469598103934665603);
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto done;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto done;
        for (uint32_t c = 0; c < gc; c++) {
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) goto done;
            uint32_t col = c >= aggregate ? c + 1 : c;
            hash ^= (uint64_t)rel->columns[col][row];
            hash *= UINT64_C(1099511628211);
            wl_columnar_relation_consolidate_checkpoint(control, 140,
                ++hashed, NULL, rel);
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) goto done;
        }
        if (!hash) hash = 1;
        uint32_t slot = (uint32_t)hash & mask;
        for (;;) {
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) goto done;
            bool occupied = groups[slot].hash != 0;
            uint32_t keep = groups[slot].row;
            bool equal = occupied && groups[slot].hash == hash;
            wl_columnar_relation_consolidate_checkpoint(control, 141,
                ++probed, NULL, rel);
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) goto done;
            if (!occupied) break;
            for (uint32_t c = 0; equal && c < gc; c++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto done;
                uint32_t col = c >= aggregate ? c + 1 : c;
                equal = rel->columns[col][keep] == rel->columns[col][row];
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
            if (equal) {
                found = keep; break;
            }
            slot = (slot + 1) & mask;
        }
        if (found != UINT32_MAX) {
            bool better = false;
            rc = wl_columnar_ops_reduce_better(spec->fn, spec->operand_type,
                    sess->intern, rel->columns[aggregate][row],
                    rel->columns[aggregate][found], &better, control);
            if (rc) goto done;
            if (better) {
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto done;
                record->changed = true;
                rel->columns[aggregate][found] = rel->columns[aggregate][row];
                if (rel->timestamps)
                    rel->timestamps[found] = rel->timestamps[row];
                wl_columnar_relation_consolidate_checkpoint(control, 142,
                    ++winners, NULL, rel);
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
            continue;
        }
        if (record->output_rows != row) {
            for (uint32_t c = 0; c < rel->ncols; c++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto done;
                record->changed = true;
                rel->columns[c][record->output_rows] = rel->columns[c][row];
                wl_columnar_relation_consolidate_checkpoint(control, 143,
                    ++copied, NULL, rel);
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
            if (rel->timestamps) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto done;
                record->changed = true;
                rel->timestamps[record->output_rows] = rel->timestamps[row];
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
        }
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto done;
        groups[slot].hash = hash;
        groups[slot].row = record->output_rows++;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) goto done;
    record->changed |= record->output_rows != record->old_nrows;
    record->finalized = true;
done:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return cleanup && rc <= 0 ? cleanup : rc;
    }
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_serial_test_before_aggregate)(wl_col_session_t *,
    col_rel_t *);
#endif

int
wl_columnar_eval_serial_canonicalize_aggregates(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    if (control) {
        for (uint32_t i = 0; i < sp->relation_count; i++) {
            int rc = wl_evaluation_control_charge(control, 16);
            if (rc) return rc;
            const wl_plan_relation_t *rp = &sp->relations[i];
            bool selected = rp->recursive_agg.has_spec;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
            if (!selected) continue;
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, rp->name, &rel,
                    control);
            if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_eval_serial_test_before_aggregate)
                wl_columnar_eval_serial_test_before_aggregate(sess, rel);
#endif
            rc = wl_columnar_eval_serial_canonicalize_aggregate_controlled(
                rel, &rp->recursive_agg, sess, control);
            if (rc) return rc;
            uint32_t family = 0, cursor = 0;
            rc = wl_columnar_arrangement_invalidate_controlled(sess,
                    rp->name, &family, &cursor, control);
            if (rc) return rc;
        }
        return 0;
    }
    /*
     * Reading the recorded specification rather than looking for a REDUCE in
     * rp->ops is the whole of #975: rewrite_multiway_delta() replaces a fused
     * relation's operators with a single K_FUSION and moves the originals into
     * its opaque_data, so the operator was not there to be found and this ran
     * on every recursive aggregate relation *except* the ones fusion applies
     * to.  wl_plan_relation_t.recursive_agg is filled at IR lowering, before
     * any rewrite runs.
     *
     * Timing is unchanged: this is the fixpoint, not per-iteration
     * consolidation.  A same-stratum consumer would therefore hold rows
     * derived from labels this reduction then dominates away, output that
     * contradicts itself.  #1021 settled that by refusing the shape at plan
     * time -- a recursive min/max aggregate may not share an SCC with any
     * other relation -- rather than by changing when this runs, so nothing
     * here had to move.  Moving the reduction into consolidation is still
     * open and would readmit the intra-round half of the shape; that is
     * deferred to milestone 0.70.0, see docs/SEMANTICS.md.
     *
     * The correction that matters to a reader of the old comment here:
     * wl_plan_stratum_t.is_monotone is no longer "hardcoded false and never
     * computed".  wl_plan_from_program() computes it (negation only).  Its
     * one consumer is the snapshot's incremental-route gate (#2114), which
     * reads a false value as "evaluate in full"; nothing here acts on it.
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];
        if (!rp->recursive_agg.has_spec)
            continue;
        col_rel_t *rel = session_find_rel(sess, rp->name);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_serial_test_before_aggregate)
            wl_columnar_eval_serial_test_before_aggregate(sess, rel);
#endif
        int rc = wl_columnar_eval_serial_canonicalize_aggregate(rel,
                &rp->recursive_agg, sess->intern, sess);
        if (rc != 0)
            return rc;
        col_session_invalidate_arrangements(&sess->base, rp->name);
    }
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_serial_test_after_plan)(wl_col_session_t *sess,
    eval_stack_t *stack, eval_entry_t *result);
void (*wl_columnar_eval_serial_test_before_delta_consolidate)(
    wl_col_session_t *sess, col_rel_t *source, col_rel_t *delta);
void (*wl_columnar_eval_serial_test_before_delta_clone)(
    wl_col_session_t *sess, col_rel_t *source);
void (*wl_columnar_eval_serial_test_before_delta_timestamps)(
    wl_col_session_t *sess, col_rel_t *source, col_rel_t *delta);
void (*wl_columnar_eval_serial_test_after_delta_stamp)(
    wl_col_session_t *sess, col_rel_t *source, col_rel_t *delta);
#endif

int
wl_columnar_eval_serial_framed_relation(const wl_plan_relation_t *rp,
    wl_col_session_t *sess, bool recursive)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    col_rel_t *controlled_target = NULL;
    if (control) {
        int preflight = wl_evaluation_control_charge(control, 16);
        if (preflight) return preflight;
        if (recursive ? (!sess->recursive.active || !sess->recursive.ready
            || sess->recursive.phase != 2 || sess->diff_enabled
            || sess->diff_operators_active || sess->tdd_subpass_active)
            : (sess->eval_arena && !sess->serial_completion.pending))
            return ENOTSUP;
        preflight = wl_columnar_join_controlled_lookup(sess, rp->name,
                &controlled_target, control);
        if (preflight) return preflight;
        if (!controlled_target &&
            !sess->serial_completion.pending) return ENOTSUP;
    }
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    int rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc != 0)
        return rc;
    if (sess->serial_completion.pending) sess->serial_completion.frame = frame;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    rc = col_eval_relation_plan(rp, stack, sess);
    if (rc == 0 && stack->top != 0) {
        rc = control ? wl_evaluation_control_charge(control,
                2u * sizeof(*result) + 8u) : 0;
        if (!rc) {
            *result = eval_stack_pop(stack);
            if (control) memset(&stack->items[stack->top], 0, sizeof(*result));
        }
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_serial_test_after_plan)
        wl_columnar_eval_serial_test_after_plan(sess, stack, result);
#endif
    if (rc != 0)
        goto done;
    if (result->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION) {
        rc = ENOTSUP;
        goto done;
    }
    rc = eval_stack_drain(stack);
    if (rc != 0 || !result->rel || (recursive && result->rel->nrows == 0))
        goto done;
    col_rel_t *target = control ? controlled_target : session_find_rel(sess,
            rp->name);
    if (control) {
        wl_columnar_relation_bulk_append_t *record =
            wl_columnar_eval_stack_cleanup_bulk_append(frame);
        wl_columnar_relation_source_group_t *sources =
            wl_columnar_eval_stack_cleanup_sources(frame);
        if (!record || !sources) {
            rc = EINVAL; goto done;
        }
        if (result->owned || !target) {
            if (stack->top >= COL_STACK_MAX) {
                rc = ENOMEM; goto done;
            }
            rc = wl_evaluation_control_charge(control,
                    sizeof(eval_entry_t) + 16u);
            if (rc) goto done;
            eval_entry_t *candidate = &stack->items[stack->top];
            memset(candidate, 0, sizeof(*candidate));
            candidate->kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
            candidate->owned = true;
            stack->top++;
            rc = wl_columnar_relation_source_group_acquire_controlled(sources,
                    result->rel, NULL, control);
            if (!rc) rc =
                    wl_columnar_relation_new_like_held_controlled(
                    target ? "$publish" : rp->name,
                    result->rel, sess->memory_governor, &candidate->rel,
                    control);
            rc = wl_columnar_relation_source_group_drain(sources, rc);
            if (rc) goto done;
            rc = wl_columnar_relation_bulk_append_controlled(record,
                    candidate->rel, result->rel, control);
            if (rc) goto done;
            /* The original visible result must retire before canonical append.
             * Both owners live in distinct real frame entries on refusal. */
            rc = eval_entry_dispose(result);
            if (rc) goto done;
            rc = wl_evaluation_control_charge(control,
                    2u * sizeof(*result) + 8u);
            if (rc) goto done;
            *result = eval_stack_pop(stack);
            memset(&stack->items[stack->top], 0, sizeof(*result));
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) goto done;
        }
        if (target) {
            if (recursive) {
                rc = wl_columnar_relation_canonical_schema_controlled(record,
                        target, result->rel, control);
                if (rc) goto done;
                /* A consumed schema is not a consumed row append. Retire its
                 * publication witness before append's first admission. */
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto done;
                record->published = false;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
            rc = wl_columnar_relation_bulk_append_controlled(record, target,
                    result->rel, control);
            if (recursive && (record->published || !rc))
                sess->recursive.canonical_consumed = true;
            if (sess->serial_completion.pending && (record->published || !rc))
                sess->serial_completion.relation++;
        } else {
            wl_columnar_session_hash_controlled_registry_t *registry = NULL;
            rc = wl_columnar_eval_stack_cleanup_registry(frame, &registry,
                    control);
            if (!rc) rc = wl_columnar_session_hash_registry_controlled(registry,
                        sess, result->rel, control);
            if (registry && registry->published) {
                /* Actual registry ownership transfer and consumed cursor precede
                * any possible frame/result disposal. Staging did not consume. */
                result->rel = NULL;
                result->owned = false;
                sess->serial_completion.relation++;
            }
        }
        goto done;
    }
    if (target) {
        if (result->owned) {
            /* Dispose the externally observable result before publication.
            * Otherwise a refused destroy after append duplicates rows on
            * an initial snapshot retry. The private copy never escapes. */
            col_rel_t *copy = NULL;
            rc = wl_columnar_relation_new_like_governed_checked(&copy,
                    "$publish", result->rel, sess->memory_governor);
            if (rc != 0) {
                if (rc == ENOSPC)
                    sess->memory_budget_denied = true;
                goto done;
            }
            rc = eval_stack_push(stack, copy, true);
            if (rc != 0) {
                col_rel_destroy(copy);
                goto done;
            }
            rc = col_rel_append_all(copy, result->rel, NULL);
            if (rc == 0)
                rc = eval_entry_dispose(result);
            if (rc != 0)
                goto done;
            *result = eval_stack_pop(stack);
        }
        if (recursive && target->ncols == 0 && result->rel->ncols > 0) {
            rc = col_rel_set_schema(target, result->rel->ncols,
                    (const char *const *)result->rel->col_names);
            if (rc != 0)
                goto done;
        }
        rc = col_rel_append_all(target, result->rel, sess->eval_arena);
        goto done;
    }
    if (result->owned) {
        rc = wl_columnar_relation_rename_checked(result->rel, rp->name);
        if (rc != 0)
            goto done;
    } else {
        col_rel_t *source = result->rel;
        wl_columnar_source_access_reader_t reader = { 0 };
        rc = col_rel_source_reader_acquire(source, &reader);
        if (rc != 0)
            goto done;
        wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
            ? sess->memory_governor : source->memory_governor;
        col_rel_t *copy = NULL;
        rc = wl_columnar_relation_pool_new_like_governed_checked(&copy,
                sess->delta_pool, rp->name, source, governor);
        if (rc != 0) {
            if (rc == ENOSPC)
                sess->memory_budget_denied = true;
        } else {
            result->rel = copy;
            result->owned = true;
            rc = col_rel_append_all(copy, source, sess->eval_arena);
        }
        int release_rc = col_rel_source_reader_release(&reader);
        if (rc == 0)
            rc = release_rc;
        if (rc != 0)
            goto done;

    }
    rc = session_add_rel(sess, result->rel);
    if (rc == 0) {
        /* Pool promotion may have retired the original descriptor. */
        result->rel = NULL;
        result->owned = false;
    }
done:;
    int cleanup_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    return control && rc > 0 ? rc : cleanup_rc != 0 ? cleanup_rc : rc;
}

/* Issue #2100: a rule head that also holds input -- inline facts or host
 * rows, which its seed shadow records -- keeps that input beside what its
 * rules derive, so it can repeat a derived row, and an input row inserted
 * more than once is there once per insert.  Rules and snapshots must see
 * such a row once, as they see a derived row, so sort and deduplicate each
 * such head of @sp, as the multi-worker non-recursive path's finalization
 * does for every head.  The seed shadow keeps the copies, so a removal still
 * takes out one insert at a time. */
int
wl_columnar_eval_consolidate_input_heads(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *name = sp->relations[ri].name;
        col_rel_t *seed = wl_columnar_session_seed_rows(sess, name);
        col_rel_t *head = session_find_rel(sess, name);
        if (!seed || seed->nrows == 0 || !head || head->nrows <= 1)
            continue;
        int rc = wl_columnar_eval_delta_consolidate(head, sess);
        if (rc != 0)
            return rc;
        col_session_invalidate_arrangements(&sess->base, name);
    }
    return 0;
}

/*
 * col_eval_stratum:
 * Evaluate one stratum, writing results into session relations.
 * Non-recursive strata are evaluated once.
 * Recursive strata use semi-naive fixed-point iteration.
 *
 * Returns 0 on success, non-zero on error.
 */
static int wl_columnar_eval_serial_recursive_terminal(const wl_plan_stratum_t *,
    wl_col_session_t *, uint32_t);

/* Ordinary W1 fixed-point evaluation keeps only scalar local progress. All
 * allocation, rule, source and delta authority lives at its original durable
 * session/frame address. A nonterminal failure abandons those owners and a
 * later host attempt starts the raw baseline again. */
static int
wl_columnar_eval_serial_recursive_controlled(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, uint32_t stratum_idx)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    wl_columnar_serial_completion_t *completion = &sess->serial_completion;
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    if (!control || !completion->pending || completion->plan != sess->plan
        || completion->stratum != stratum_idx || !sp->is_recursive
        || !sp->relation_count || !sp->relations || sess->diff_enabled
        || sess->diff_operators_active || sess->delta_observer ||
        sess->delta_rollback
        || sess->delta_cb || sess->coordinator || sess->num_workers != 1
        || sess->darr_count || sess->rotation_ops != &col_rotation_standard_ops
        || sess->frontier_ops != &col_frontier_epoch_ops) return ENOTSUP;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    for (uint32_t i = 0; i < sp->relation_count; i++) {
        rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
        bool supported = false;
        rc = wl_columnar_eval_plan_recursive_validate(&sp->relations[i],
                control,
                &supported); if (rc) return rc;
        if (!supported || !sp->relations[i].name ||
            !sp->relations[i].delta_name)
            return ENOTSUP;
    }
    /* Validate every head before registering even the first absent head. */
    for (uint32_t i = 0; i < sp->relation_count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        col_rel_t *existing = NULL;
        rc = wl_columnar_join_controlled_lookup(sess, sp->relations[i].name,
                &existing, control); if (rc) return rc;
        if (!existing) {
            rc = wl_columnar_eval_serial_recursive_register_head(sess,
                    sp->relations[i].name, control); if (rc) return rc;
        }
    }
    rc = wl_columnar_eval_serial_recursive_begin(sess, sess->plan, stratum_idx,
            control);
    if (rc) return rc;
    wl_columnar_eval_serial_recursive_record_t *record = &sess->recursive;
    for (uint32_t i = 0; i < sp->relation_count; i++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto fail;
        col_rel_t *head = NULL;
        rc = wl_columnar_join_controlled_lookup(sess, sp->relations[i].name,
                &head, control); if (rc) goto fail;
        if (!head) {
            rc = ENOENT; goto fail;
        }
        rc = wl_columnar_eval_serial_recursive_initial_sort(sess, head,
                control);
        if (rc) goto fail;
    }
    uint32_t rule_base = 0;
    for (uint32_t si = 0; si < stratum_idx; si++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) goto fail;
        if (sess->plan->strata[si].relation_count > UINT32_MAX - rule_base) {
            rc = EOVERFLOW; goto fail;
        }
        rule_base += sess->plan->strata[si].relation_count;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    }
    rc = wl_evaluation_control_charge(control, 32); if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    record->rule_id_base = rule_base > MAX_RULES ? MAX_RULES : rule_base;
    sess->frontier_ops->init_stratum(sess, stratum_idx);
    uint32_t final_iteration = 0;
    col_frontier_t stratum_frontier = {0, 0};
    bool converged = false;
    for (uint32_t outer = 0; outer < MAX_ITERATIONS && !converged; outer++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto fail;
        bool any_outer = false, ran = false, next_outer = false;
        for (uint32_t sub = 0; sub < EVAL_STRIDE; sub++) {
            rc = wl_evaluation_control_charge(control, 32); if (rc) goto fail;
            uint32_t iteration = outer * EVAL_STRIDE + sub;
            record->iteration = outer; record->subpass = sub;
            record->effective_iteration = iteration;
            sess->current_iteration = iteration;
            sess->diff_operators_active = false;
            bool skip = sess->frontier_ops->should_skip_iteration(sess,
                    stratum_idx, iteration);
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
            if (skip) continue;
            ran = true;
            /* This route does not produce a delta-arrangement cache. Never
             * hide a reached raw cache-free loop behind a scalar admission. */
            if (sess->darr_count) {
                rc = ENOTSUP; goto fail;
            }
            for (uint32_t i = 0; i < sp->relation_count; i++) {
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto fail;
                if (record->delta_rels[i]) {
                    rc = wl_columnar_eval_serial_recursive_register_delta(sess,
                            i, control);
                    if (rc) goto fail;
                }
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto fail;
            }
            rc = wl_columnar_eval_serial_recursive_pool_barrier(sess, control);
            if (rc) goto fail;
            for (uint32_t i = 0; i < sp->relation_count; i++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto fail;
                rc = wl_columnar_eval_serial_recursive_snapshot(sess, i,
                        control);
                if (rc) goto fail;
            }
            if (iteration) {
                bool net_zero = true, forced_empty = true;
                for (uint32_t i = 0; i < sp->relation_count; i++) {
                    rc = wl_evaluation_control_charge(control, 16);
                    if (rc) goto fail;
                    col_rel_t *delta = NULL;
                    rc = wl_columnar_join_controlled_lookup(sess,
                            sp->relations[i].delta_name, &delta, control);
                    if (rc) goto fail;
                    if (delta) {
                        col_frontier_t frontier; int64_t net;
                        rc = wl_columnar_eval_serial_recursive_frontier(sess,
                                delta,
                                &frontier, &net, control); if (rc) goto fail;
                        if (net) net_zero = false;
                    }
                    bool empty = false;
                    rc =
                        wl_columnar_eval_plan_empty_forced_delta(
                        &sp->relations[i],
                        sess, iteration, &empty, control);
                    if (rc) goto fail;
                    if (!empty) forced_empty = false;
                }
                if (net_zero || forced_empty) {
                    next_outer = true; break;
                }
            }
            for (uint32_t i = 0; i < sp->relation_count; i++) {
                rc = wl_evaluation_control_charge(control, 32);
                if (rc) goto fail;
                bool skip_rule = record->rule_id_base <= UINT32_MAX - i
                    && record->rule_id_base + i < MAX_RULES
                    && sess->frontier_ops->should_skip_rule(sess,
                        record->rule_id_base + i, iteration);
                bool empty = false;
                rc = wl_columnar_eval_plan_empty_forced_delta(&sp->relations[i],
                        sess, iteration, &empty, control); if (rc) goto fail;
                if (skip_rule || empty) continue;
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto fail;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto fail;
                completion->relation = i;
                rc = wl_columnar_eval_serial_framed_relation(&sp->relations[i],
                        sess, true);
                if (rc) goto fail;
            }
            for (uint32_t i = 0; i < sp->relation_count; i++) {
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto fail;
                int removed = wl_columnar_session_remove_controlled(sess,
                        sp->relations[i].delta_name, control);
                if (removed && removed != ENOENT && removed != EBUSY) {
                    rc = removed; goto fail;
                }
                if (record->phase ==
                    WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL){
                    rc = removed ? removed : EBUSY; goto fail;
                }
            }
            bool any_new = false;
            for (uint32_t i = 0; i < sp->relation_count; i++) {
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto fail;
                uint32_t old_rows = record->snap[i];
                rc = wl_columnar_eval_serial_recursive_snapshot(sess, i,
                        control);
                if (rc) goto fail;
                uint32_t new_rows = record->snap[i];
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto fail;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto fail;
                record->snap[i] = old_rows;
                if (new_rows <= old_rows) continue;
                col_rel_t *head = NULL;
                rc = wl_columnar_join_controlled_lookup(sess,
                        sp->relations[i].name, &head, control);
                if (rc) goto fail;
                if (!head) {
                    rc = ENOENT; goto fail;
                }
                int fast = 0;
                rc = wl_columnar_eval_serial_recursive_consolidate(sess, head,
                        i,
                        old_rows, &fast, control);
                /* Sorting may precede a later error: preserve actual raw
                 * invalidation without hiding its entry/name work. */
                uint32_t family = 0, cursor = 0;
                if (!rc) rc =
                        wl_columnar_arrangement_invalidate_controlled(sess,
                            sp->relations[i].name, &family, &cursor, control);
                if (rc) goto fail;
                col_rel_t *delta = record->delta_rels[i];
                col_frontier_t frontier; int64_t net;
                rc = wl_columnar_eval_serial_recursive_frontier(sess, delta,
                        &frontier, &net, control); if (rc) goto fail;
                if (net) {
                    any_new = true;
                    if (frontier.iteration > stratum_frontier.iteration
                        || (frontier.iteration == stratum_frontier.iteration
                        && frontier.stratum > stratum_frontier.stratum))
                        stratum_frontier = frontier;
                } else {
                    rc = wl_columnar_eval_stack_cleanup_idle(sess, control);
                    if (rc) goto fail;
                    rc = wl_evaluation_control_charge(control, 32);
                    if (rc) goto fail;
                    rc = wl_evaluation_control_charge(control, 0);
                    if (rc) goto fail;
                    rc = col_rel_destroy_checked(delta); if (rc) goto fail;
                    record->delta_rels[i] = NULL;
                    rc = wl_evaluation_control_charge(control, 0);
                    if (rc) goto fail;
                }
            }
            rc = wl_columnar_eval_serial_recursive_boundary(sess, control);
            if (rc) goto fail;
            if (!any_new) {
                converged = true; break;
            }
            any_outer = true; final_iteration = iteration;
        }
        if (!ran || next_outer) continue;
        if (!any_outer) converged = true;
    }
    rc = wl_columnar_eval_serial_canonicalize_aggregates(sp, sess);
    if (rc) goto fail;
    for (uint32_t i = 0; i < sp->relation_count; i++) {
        rc = wl_evaluation_control_charge(control, 32); if (rc) goto fail;
        if (record->rule_id_base <= UINT32_MAX - i
            && record->rule_id_base + i < MAX_RULES)
            sess->frontier_ops->record_rule_convergence(sess,
                record->rule_id_base + i,
                sess->outer_epoch, final_iteration);
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    }
    rc = wl_evaluation_control_charge(control, 64); if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto fail;
    sess->diff_operators_active = record->saved_diff_operators_active;
    sess->total_iterations = final_iteration;
    sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
        sess->outer_epoch, stratum_frontier.iteration);
    sess->eval_stratum_heads_final = true;
    completion->phase = WL_COLUMNAR_SERIAL_PHASE_RECURSIVE_TERMINAL;
    completion->relation = completion->compact = completion->reset_stratum = 0;
    completion->reset_relation = UINT32_MAX;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    return wl_columnar_eval_serial_recursive_terminal(sp, sess, stratum_idx);
fail:
    return wl_columnar_eval_serial_recursive_retire(sess, rc, NULL);
}

/* Canonical completion is independent of this bounded cleanup lane. The
 * external stage remains valid after local delta/snapshot owners retire. */
static int
wl_columnar_eval_serial_recursive_terminal(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, uint32_t stratum_idx)
{
    wl_columnar_serial_completion_t *state = &sess->serial_completion;
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    if (!control || !sess->plan || !sess->plan->strata || !state->pending
        || state->plan != sess->plan || state->stratum != stratum_idx
        || stratum_idx >= sess->plan->stratum_count
        || sp != &sess->plan->strata[stratum_idx] || !sp->is_recursive
        || !sess->eval_stratum_heads_final || sess->diff_enabled
        || sess->coordinator || sess->rotation_ops != &col_rotation_standard_ops
        || state->relation > sp->relation_count || state->compact > 6)
        return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_session_cleanup_ready(sess); if (rc) return rc;
    if (sess->recursive.active && sess->recursive.reset_active) {
        rc = wl_columnar_eval_serial_recursive_pool_barrier(sess, control);
        if (rc) return rc;
    }
    /* A new eligible sweep resets only its prior diagnostic. Already
     * completed teardown stages are never reset when revisiting a refusal. */
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (state->reset_relation < state->relation)
        state->relation = state->reset_relation;
    state->reset_stratum = 0; state->reset_relation = UINT32_MAX;
    while (state->relation < sp->relation_count) {
        rc = wl_evaluation_control_charge(control, 32);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        uint32_t index = state->relation;
        const char *name = sp->relations[index].delta_name;
        if (!name) return EINVAL;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        /* The following scalar cursor/error disposition is prepaid before
         * actual removal, including a successful physical destruction. */
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        int removed = wl_columnar_session_remove_controlled(sess, name,
                control);
        bool consumed = sess->recursive.phase ==
            WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REMOVAL
            && sess->recursive.removal_family == 8;
        if (removed && removed != ENOENT && removed != EBUSY) {
            if (consumed) state->relation++;
            return state->reset_stratum ? (int)state->reset_stratum : removed;
        }
        if (removed == EBUSY) {
            if (!state->reset_stratum) state->reset_stratum = EBUSY;
            if (state->reset_relation ==
                UINT32_MAX) state->reset_relation = index;
        }
        state->relation++;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
    }
    if (!state->compact) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        state->compact = 1;
    }
    if (state->compact == 1) {
        /* Disposal remains checked even after a negative tail. Its success
         * does not consume the external registered-sweep cursor. */
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_columnar_eval_serial_recursive_retire(sess, 0, control);
        if (!sess->recursive.active && !sess->recursive.cleanup_pending)
            state->compact = 2;
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
    }
    if (state->compact == 2) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_columnar_session_mem_sample_controlled(sess, control);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        state->compact = 3;
    }
    if (state->compact == 3) {
        rc = wl_columnar_eval_stack_cleanup_idle(sess, control);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        if (sess->recursive.active || sess->recursive.cleanup_pending
            || sess->mat_cache.active_pins || sess->filt_cache_active_pins)
            return EBUSY;
        /* Every remaining used slot must be an actual retired tombstone
         * before the raw two-counter reset. No capacity-based authority. */
        uint32_t used = sess->delta_pool ? sess->delta_pool->slot_used : 0;
        for (uint32_t i = 0; i < used; i++) {
            col_rel_t *slot = NULL; bool dead = false; uint64_t bytes;
            rc = wl_columnar_relation_pool_sample_controlled(sess, i, &slot,
                    &dead, &bytes, control);
            if (rc) return state->reset_stratum ? (int)state->reset_stratum :
                       rc;
            if (!dead || bytes) return EBUSY;
        }
        rc = wl_evaluation_control_charge(control, 32);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        delta_pool_reset(sess->delta_pool); state->compact = 4;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
    }
    if (state->compact == 4) {
        rc = wl_columnar_eval_stack_cleanup_idle(sess, control);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 32);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        sess->rotation_ops->rotate_eval_arena(sess); state->compact = 5;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
    }
    if (state->compact == 5) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        uint32_t cursor = 0;
        rc = wl_columnar_cache_mat_retire_controlled(&sess->mat_cache, &cursor,
                true, control);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        cursor = 0;
        rc = wl_columnar_cache_mat_retire_controlled(&sess->mat_cache, &cursor,
                false, control);
        if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
        state->compact = 6;
    }
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return state->reset_stratum ? (int)state->reset_stratum : rc;
    if (state->reset_relation !=
        UINT32_MAX) state->relation = state->reset_relation;
    return (int)state->reset_stratum;
}

int
col_eval_stratum(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    if (sess->serial_completion.pending
        && sess->serial_completion.phase ==
        WL_COLUMNAR_SERIAL_PHASE_RECURSIVE_TERMINAL)
        return wl_columnar_eval_serial_recursive_terminal(sp, sess,
                   stratum_idx);
    /* Cleared here so every exit below reports "heads not final" unless one
     * of the two completion points is reached. */
    sess->eval_stratum_heads_final = false;
    if (sess->cleanup_active)
        return EBUSY;
    int readiness_rc = wl_columnar_session_cleanup_ready(sess);
    if (readiness_rc != 0)
        return readiness_rc;

    if (sp->is_recursive && sess->base.evaluation_control
        && sess->serial_completion.pending)
        return wl_columnar_eval_serial_recursive_controlled(sp, sess,
                   stratum_idx);

    if (!sp->is_recursive) {
        if (sess->serial_completion.pending) {
            wl_evaluation_control_t *control = sess->base.evaluation_control;
            sess->current_iteration = 0;
            while (sess->serial_completion.relation < sp->relation_count) {
                int rc = wl_evaluation_control_charge(control, 16);
                if (rc) return rc;
                const wl_plan_relation_t *rp =
                    &sp->relations[sess->serial_completion.relation];
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) return rc;
                uint32_t before = sess->serial_completion.relation;
                rc = wl_columnar_eval_serial_framed_relation(rp, sess, false);
                if (rc) return rc;
                if (before == sess->serial_completion.relation)
                    sess->serial_completion.relation++; /* Genuine empty success. */
            }
            /* Entry preflight proved seed/cache/pool occupancy absent. Heap
             * operators keep the ordinary arena and pool objects untouched. */
            uint32_t rule_base = 0;
            for (uint32_t si = 0; si < stratum_idx; si++) {
                int rc = wl_evaluation_control_charge(control, 8);
                if (rc) return rc;
                if (sess->plan->strata[si].relation_count >
                    UINT32_MAX - rule_base)
                    return EOVERFLOW;
                rule_base += sess->plan->strata[si].relation_count;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) return rc;
            }
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                int rc = wl_evaluation_control_charge(control, 16);
                if (rc) return rc;
                if (rule_base <= UINT32_MAX - ri && rule_base + ri < MAX_RULES)
                    sess->frontier_ops->reset_rule_frontier(sess,
                        rule_base + ri,
                        sess->outer_epoch);
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) return rc;
            }
            int rc = wl_evaluation_control_charge(control, 16);
            if (rc) return rc;
            sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
                sess->outer_epoch, UINT32_MAX);
            sess->eval_stratum_heads_final = true;
            return wl_evaluation_control_charge(control, 0);
        }
        /* Issue #914: a non-recursive stratum is always the base case. The
         * recursive-stratum loop leaves sess->current_iteration > 0 and never
         * resets it, which would wrongly trigger the base-case EDB VARIABLE
         * skip (eval.c base-case skip) and drop this stratum's head tuples.
         * Reset here so it covers both the parallel and sequential paths. */
        sess->current_iteration = 0;

        /* Non-recursive: evaluate each relation plan once */
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];
            int par_rc = wl_columnar_eval_nonrec_relation_parallel(rp, sess);
            if (par_rc == 0)
                continue;
            if (par_rc != EAGAIN) {
                if (getenv("WIRELOG_DEBUG_NONREC_TDD"))
                    fprintf(stderr, "nonrec_tdd failed rel=%s rc=%d\n",
                        rp->name ? rp->name : "(null)", par_rc);
                return par_rc;
            }

            int rc = wl_columnar_eval_serial_framed_relation(rp, sess,
                    false);
            if (rc != 0)
                return rc;
        }
        /* Issue #2100: this stratum's rows landed on top of the input. */
        int input_rc = wl_columnar_eval_consolidate_input_heads(sp, sess);
        if (input_rc != 0)
            return input_rc;
        /* Completion point for the non-recursive branch: the rule loop and
         * the input-head consolidation above are the last things that can
         * fail, so every head is final from here.  This must stay below them
         * -- setting it where the branch opens would report true on the
         * failures that return from them, and the caller would skip a
         * restore the heads still need.  Move this with the completion
         * boundary if either ever grows a new exit. */
        sess->eval_stratum_heads_final = true;
        col_mat_cache_release_pins(&sess->mat_cache);
        assert(sess->mat_cache.active_pins == 0);
        col_mat_cache_clear(&sess->mat_cache);
        assert(sess->mat_cache.active_pins == 0);
        col_session_mem_sample(sess); /* Issue #1380 */
        delta_pool_reset(sess->delta_pool);
        sess->rotation_ops->rotate_eval_arena(sess);

        /* Non-recursive stratum frontier: record convergence epoch and iteration.
        * Non-recursive strata always converge at iteration UINT32_MAX (no loop),
        * so store (outer_epoch, UINT32_MAX) to enable epoch-aware skip on next
        * incremental call. Always update both fields so same-epoch skip logic
        * fires correctly when the frontier persists across session_step calls. */
        sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
            sess->outer_epoch, UINT32_MAX);

        /* Issue #317: Report non-recursive convergence to coordinator's
         * progress tracker.  Non-recursive strata always converge at
         * UINT32_MAX (no iteration loop), matching the sentinel convention. */
        if (sess->coordinator) {
            wl_frontier_progress_record(&sess->coordinator->progress,
                sess->worker_id, stratum_idx, sess->outer_epoch, UINT32_MAX);
        }

        /* Non-recursive rule frontiers: mark each rule fully evaluated.
         * UINT32_MAX sentinel matches stratum frontier convention. */
        if (sess->plan) {
            uint32_t rule_base = 0;
            for (uint32_t si = 0; si < stratum_idx; si++)
                rule_base += sess->plan->strata[si].relation_count;
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                uint32_t rule_idx = rule_base + ri;
                if (rule_idx < MAX_RULES) {
                    /* Issue #106: Conservative approach - always reset rule frontiers
                     * for affected strata to (current_epoch, UINT32_MAX) sentinel.
                     * UINT32_MAX prevents premature skip during re-evaluation. */
                    sess->frontier_ops->reset_rule_frontier(sess, rule_idx,
                        sess->outer_epoch);
                }
            }
        }

        return 0;
    }

    /* Issue #2100: the loop below merges each iteration's rows into the
     * head's sorted prefix and drops the ones already there, so a head that
     * enters it deduplicated leaves it deduplicated. */
    int input_rc = wl_columnar_eval_consolidate_input_heads(sp, sess);
    if (input_rc != 0)
        return input_rc;

    /*
     * Recursive stratum: semi-naive fixed-point iteration.
     * Iterate until no new tuples are produced.
     *
     * Pre-register empty IDB relations so that VARIABLE ops can find
     * them on the first iteration (before any tuples are produced).
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        col_rel_t *existing = session_find_rel(sess, sp->relations[ri].name);
        if (!existing) {
            col_rel_t *empty = NULL;
            int rc = col_rel_alloc(&empty, sp->relations[ri].name);
            if (rc != 0)
                return ENOMEM;
            rc = session_add_rel(sess, empty);
            if (rc != 0) {
                col_rel_destroy(empty);
                return rc;
            }
        }
    }

    /*
     * Semi-naive fixed-point iteration with delta tracking.
     * VARIABLE ops prefer "$d$relname" delta relations (rows added in the
     * previous iteration). JOIN right-side lookups always use the full
     * relation by name, giving delta (left) x full (right) join semantics.
     */
    uint32_t nrels = sp->relation_count;
    uint64_t delta_scratch_bytes = 0;
    uint64_t bytes = 0;
    if (!wl_columnar_memory_size_mul(nrels, sizeof(col_rel_t *), &bytes)
        || !wl_columnar_memory_size_add(delta_scratch_bytes, bytes,
        &delta_scratch_bytes)
        || !wl_columnar_memory_size_mul(nrels, sizeof(uint32_t), &bytes)
        || !wl_columnar_memory_size_add(delta_scratch_bytes, bytes,
        &delta_scratch_bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t delta_reservation;
    wl_columnar_memory_reservation_init(&delta_reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                delta_scratch_bytes, &delta_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC : ENOMEM;
        }
    }
    col_rel_t **delta_rels = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
    if (!delta_rels) {
        wl_columnar_memory_release(&delta_reservation);
        return ENOMEM;
    }

    /* Allocate snap once before the iteration loop (Issue #367): hoisting
     * the allocation out of the outer loop eliminates 2 malloc/free calls
     * per iteration (14K+ iterations for CRDT). */
    uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
    if (!snap) {
        free((void *)delta_rels);
        wl_columnar_memory_release(&delta_reservation);
        return ENOMEM;
    }

    /* Sort pre-existing data in each IDB relation before iterating.
     * Handles the EDB+IDB case: when base facts are pre-loaded into a
     * relation that also appears as an IDB in a recursive rule, the loaded
     * facts may be in insertion order (unsorted).
     * col_op_consolidate_incremental_delta requires rel->data[0..snap) to be
     * sorted; an unsorted prefix causes the 2-pointer merge to miss duplicates,
     * producing spurious output rows. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (r && r->nrows > 1) {
            /* The comment above states the precondition this establishes.
             * A refused sort leaves the prefix unsorted, so continuing would
             * produce exactly the spurious rows it warns about. */
            int sort_rc = col_rel_radix_sort_int64(r);
            if (sort_rc != 0) {
                free((void *)delta_rels);
                free(snap);
                wl_columnar_memory_release(&delta_reservation);
                return sort_rc;
            }
        }
    }

    /* Initialize recursive stratum frontier to UINT32_MAX (not set sentinel)
     * only on the first evaluation (frontier==0 from calloc). If a prior
     * col_session_insert_incremental call preserved a real convergence frontier,
     * keep it so the per-iteration skip condition fires for iterations beyond
     * the prior convergence point. */
    sess->frontier_ops->init_stratum(sess, stratum_idx);

    /* Phase 4 (US-4-004): Compute the base global rule index for this stratum.
     * Rule indices are assigned by enumerating strata in order and relations
     * within each stratum, matching col_compute_affected_rules convention. */
    uint32_t rule_id_base = 0;
    if (sess->plan) {
        for (uint32_t si = 0;
            si < stratum_idx && si < sess->plan->stratum_count; si++) {
            rule_id_base += sess->plan->strata[si].relation_count;
        }
    }
    /* Clamp so rule_id_base + ri never exceeds MAX_RULES - 1 */
    if (rule_id_base >= MAX_RULES)
        rule_id_base = MAX_RULES;

    uint32_t iter;
    col_frontier_t strat_frontier = { 0, 0 };
    uint32_t final_eff_iter = 0; /* effective sub-pass index at convergence */

    /* Issue #282: Save the session-level diff_operators_active so we can
     * restore it after the recursive stratum evaluation completes.
     * Within the iteration loop, we override it per sub-pass. */
    bool saved_diff_operators_active = sess->diff_operators_active;

    /*
     * Stride-based semi-naive iteration (Issue #237).
     * Each outer iteration runs EVAL_STRIDE sub-passes, chaining the delta
     * from each sub-pass into the next.  Effective recursion depth covered:
     *   MAX_ITERATIONS * EVAL_STRIDE = 4096 * 8 = 32768
     * This allows deep-chain workloads (e.g. CRDT) to converge where they
     * previously hit the MAX_ITERATIONS cap and produced incomplete results.
     *
     * When EVAL_STRIDE == 1 the behaviour is identical to the prior single-
     * pass loop.
     */
    for (iter = 0; iter < MAX_ITERATIONS; iter++) {
        bool outer_any_new = false; /* any sub-pass produced new tuples  */
        int outer_rc = 0;           /* error propagated from inner loop  */
        bool publication_failed = false;
        bool stride_all_skipped
            = true; /* true until a sub-pass actually runs */
        bool outer_continue_next
            = false;            /* net-zero/all-empty: retry next outer */
        bool converged = false; /* fixed point reached                */

        /* snap[] is filled at the start of each sub-pass (nrows grows as new
         * tuples are appended).  The array is allocated once before this loop
         * (Issue #367) to avoid per-iteration malloc/free overhead. */

        /* ------------------------------------------------------------------ */
        /* Inner sub-pass loop: EVAL_STRIDE semi-naive passes per outer iter.  */
        /* ------------------------------------------------------------------ */
        for (uint32_t sub = 0; sub < EVAL_STRIDE; sub++) {
            uint32_t eff_iter = iter * EVAL_STRIDE + sub;

            /* Publish effective iteration so operators can distinguish base case
             * (eff_iter 0: FORCE_DELTA falls back to full) from delta case
             * (eff_iter > 0: FORCE_DELTA with absent delta → empty result). */
            sess->current_iteration = eff_iter;

            /* Issue #282: Enable differential operators for recursive iterations
             * > 0.  At eff_iter 0, we seed from full EDB relations (base case)
             * so arrangement reuse is not applicable.  From eff_iter 1 onward,
             * only the delta is processed (semi-naive), so col_op_join_diff can
             * build the hash table incrementally: O(D) instead of O(N).
             * diff_arr_entries persist across sub-passes and are invalidated by
             * col_session_invalidate_arrangements when the underlying relation
             * changes (called after consolidation each sub-pass).
             * The saved value is restored after the recursive stratum completes
             * so non-recursive strata and the outer session logic are unaffected. */
            sess->diff_operators_active
                = sess->diff_enabled && eff_iter > 0;

            /* Iteration skip based on frontier (convergence point of prior
             * evaluation).  Skip sub-pass when both hold:
             * 1. Same outer_epoch — frontier is valid for this insertion epoch
             * 2. eff_iter > frontier.iteration — already processed in this epoch
             * Once eff_iter exceeds the frontier for sub=0, it also exceeds for
             * all higher sub values, so the entire outer iteration effectively
             * continues to the next one. */
            if (sess->frontier_ops->should_skip_iteration(sess, stratum_idx,
                eff_iter)) {
                continue; /* skip this sub-pass */
            }

            stride_all_skipped = false; /* at least one sub-pass runs */

            /* Clear per-sub-pass delta arrangement cache (sequential eval path).
             * K-fusion workers manage their own darr caches independently. */
            col_session_free_delta_arrangements(sess);

            /* Register delta relations from previous sub-pass into session */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                if (!delta_rels[ri])
                    continue;
                int rc = session_add_rel(sess, delta_rels[ri]);
                if (rc != 0) {
                    /* This delta is still private. Preserve its local owner
                     * for stride_error cleanup; replacement failure leaves the
                     * previous registered delta intact. */
                    outer_rc = rc;
                    publication_failed = true;
                    goto stride_error;
                }
                delta_rels[ri] = NULL; /* session now owns it */
            }

            /* Record per-relation row counts (O(1) snapshot — no data copy).
             * Re-filled each sub-pass because nrows changes after each pass. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
                snap[ri] = r ? r->nrows : 0; /* O(1) count only, no copy */
            }

            /* Phase 3D: Frontier skip with multiplicities (US-3D-002).
             * Skip sub-pass if all delta relations have zero net multiplicity.
             * When detected, retry with the next outer iteration (same as the
             * prior single-pass behaviour). */
            if (eff_iter
                > 0) { /* only skip from effective iteration 1 onward */
                bool all_deltas_net_zero = true;
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    const char *dname = sp->relations[ri].delta_name;
                    col_rel_t *delta = session_find_rel(sess, dname);
                    if (!delta || delta->nrows == 0) {
                        /* Empty delta: net multiplicity is zero */
                        continue;
                    }
                    /* Compute net multiplicity for this delta relation */
                    int64_t net_mult = 0;
                    for (uint32_t row = 0; row < delta->nrows; row++) {
                        net_mult += delta->timestamps[row].multiplicity;
                    }
                    if (net_mult != 0) {
                        all_deltas_net_zero = false;
                        break;
                    }
                }
                if (all_deltas_net_zero) {
                    /* All deltas zero net-multiplicity: skip to next outer iter */
                    outer_continue_next = true;
                    break;
                }
            }

            /* Stratum-level early exit: if all rules have empty forced deltas,
             * the sub-pass will produce no new facts. (Issue #81) */
            if (eff_iter > 0) {
                bool all_rules_empty = true;
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    if (!has_empty_forced_delta(&sp->relations[ri], sess,
                        eff_iter)) {
                        all_rules_empty = false;
                        break;
                    }
                }
                if (all_rules_empty) {
                    outer_continue_next = true;
                    break;
                }
            }

            /* Single-pass semi-naive evaluation. VARIABLE prefers delta when it
             * is a strict subset of full (genuine new facts). JOIN propagates
             * the is_delta flag through results and applies right-delta when
             * left is full and a strictly-smaller right delta exists. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                const wl_plan_relation_t *rp = &sp->relations[ri];

                /* Issue #106 (US-106-003): Rule-level frontier skip with epoch
                 * gating.  Skip rule evaluation only when BOTH hold:
                 * 1. Same outer_epoch (prevents cross-epoch incorrect skips)
                 * 2. eff_iter > convergence point (already processed in epoch)
                 * Across epoch boundaries mismatch => always re-eval. */
                uint32_t rule_id = rule_id_base + ri;
                if (sess->frontier_ops->should_skip_rule(sess, rule_id,
                    eff_iter)) {
                    continue;
                }

                /* Pre-scan skip: if a FORCE_DELTA op references an empty or
                 * absent delta (eff_iter > 0), the plan produces 0 rows. */
                if (has_empty_forced_delta(rp, sess, eff_iter)) {
                    continue;
                }

                /* Previous-pass publication transferred every private
                 * delta before a rule may retain cleanup dependencies. */
                for (uint32_t di = 0; di < nrels; di++)
                    assert(delta_rels[di] == NULL);
                int rc = wl_columnar_eval_serial_framed_relation(rp, sess,
                        true);
                if (rc != 0) {
                    outer_rc = rc;
                    goto stride_error;
                }
            }

            /* Remove delta relations from session (evaluation is complete).
             * A successful removal is the normal case: every sub-pass after
             * the first removes the delta its predecessor published.  ENOENT
             * arises whenever a relation published no delta -- the first
             * sub-pass, a relation with no new rows, one absent from the
             * registry, or a delta that came back empty and was destroyed
             * unpublished.  A refusal keeps the registry owner, so nothing is
             * lost; it surfaces one sub-pass later if that relation publishes
             * again, and otherwise falls through to the terminal removal at
             * the end of the stratum, which does propagate.
             *
             * It is deliberately not propagated from here.  The terminal
             * removal is enough for the contract: every other exit from
             * col_eval_stratum already returns non-zero -- the two entry
             * guards, the recursive setup failures, the cleanup_pending
             * branch, stride_error, and the aggregate failure -- and the
             * non-recursive branch removes no delta at all, so the only
             * success exit that could report success with a reader-held
             * delta still registered is the terminal one.  That closes the
             * contract, though not the state: a refusal here followed by an
             * unrelated later stride_error still returns non-zero with the
             * delta registered.  What handles that depends on the caller:
             * col_session_snapshot sweeps every $d$ relation on its error
             * path, while col_session_step has no such sweep and leaves it
             * to the session's lease release at teardown.  The sweep is also
             * skipped when the session holds retained cleanup, which is the
             * case likeliest to have caused the refusal in the first place,
             * so do not read it as a general backstop.
             *
             * Propagating from here would also suppress work rather than
             * merely report it.  Between this refusal and the same-name
             * registration that surfaces it, a whole sub-pass runs and any
             * rows it derives are appended to head relations; restore_flat
             * declines a populated head, so no rollback undoes them.  The rc
             * and the completion flag would be identical either way, so the
             * only difference is head content this unit is not required to
             * change.  See #1661. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                const char *dname = sp->relations[ri].delta_name;
                (void)session_remove_rel(sess, dname);
            }

            /* Phase 4: Frontier is computed incrementally as deltas are created
             * because delta_rels[ri] may be set to NULL in the next sub-pass's
             * registration loop.  strat_frontier declared before outer loop. */

            /* Consolidate all IDB relations to remove duplicates and compute
             * delta as a byproduct.  snap[ri] marks the boundary between the
             * already-sorted prefix and unsorted new rows appended this pass. */
            bool any_new = false;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
                if (!r || snap[ri] >= r->nrows) {
                    continue; /* no new rows for this relation */
                }

                const char *dname = sp->relations[ri].delta_name;
                wl_columnar_memory_governor_ref_t *governor =
                    sess->memory_governor ? sess->memory_governor
                                          : r->memory_governor;
                wl_columnar_source_access_reader_t reader = { 0 };
                int reader_rc = col_rel_source_reader_acquire(r, &reader);
                if (reader_rc != 0) {
                    outer_rc = reader_rc;
                    goto stride_error;
                }
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_serial_test_before_delta_clone)
                    wl_columnar_eval_serial_test_before_delta_clone(sess, r);
#endif
                col_rel_t *delta = NULL;
                int clone_rc =
                    wl_columnar_relation_pool_new_like_governed_checked(
                    &delta, sess->delta_pool, dname, r, governor);
                int reader_release_rc =
                    col_rel_source_reader_release(&reader);
                if (reader_release_rc != 0 && delta) {
                    col_rel_destroy(delta);
                    delta = NULL;
                }
                if (clone_rc != 0 || !delta) {
                    outer_rc = reader_release_rc != 0 ? reader_release_rc
                                                      : clone_rc;
                    if (outer_rc == ENOSPC)
                        sess->memory_budget_denied = true;
                    goto stride_error;
                }

                /* Nullary pool clones have no initial row capacity, so make
                 * one timestamp slot admissible before consolidation can emit
                 * their first tuple. */
                if (delta->capacity == 0) {
                    int reserve_rc = col_rel_reserve_capacity_admitted(delta,
                            COL_REL_INIT_CAP, NULL);
                    if (reserve_rc != 0) {
                        col_rel_destroy(delta);
                        outer_rc = reserve_rc;
                        goto stride_error;
                    }
                }

                /* Reserve provenance storage before consolidation mutates the
                 * source.  This makes timestamp admission part of the same
                 * pre-mutation allocation contract as the delta rows. */
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_serial_test_before_delta_timestamps)
                    wl_columnar_eval_serial_test_before_delta_timestamps(
                        sess, r, delta);
#endif
                int timestamp_rc = col_rel_enable_timestamps(delta);
                if (timestamp_rc != 0) {
                    col_rel_destroy(delta);
                    outer_rc = timestamp_rc;
                    goto stride_error;
                }
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_serial_test_before_delta_consolidate)
                    wl_columnar_eval_serial_test_before_delta_consolidate(
                        sess, r, delta);
#endif

                /* Consolidate WITH delta output (no separate merge walk) */
                uint32_t cons_old = snap[ri];
                uint32_t cons_new = r->nrows - cons_old; /* delta count D */
                int fast_flag = 0;
                uint64_t cons_t0 = now_ns();
                int rc2 = col_op_consolidate_incremental_delta(r, snap[ri],
                        delta, &fast_flag);
                uint64_t cons_elapsed = now_ns() - cons_t0;
                if (rc2 != 0) {
                    /* The source may have been sorted before a later
                     * admission error; invalidate cached arrangements too. */
                    col_session_invalidate_arrangements(&sess->base,
                        sp->relations[ri].name);
                    col_rel_destroy(delta);
                    outer_rc = rc2;
                    goto stride_error;
                }

                sess->consolidation_ns += cons_elapsed;
                sess->consolidate_fast_hits += (uint64_t)fast_flag;
                sess->consolidate_slow_hits += (uint64_t)(1 - fast_flag);
                /* Invalidate arrangements for this relation only after the
                 * consolidation transaction has committed. */
                col_session_invalidate_arrangements(&sess->base,
                    sp->relations[ri].name);
                /* Per-call trace: WL_LOG=CONSOLIDATION:5 (or legacy
                 * WL_CONSOLIDATION_LOG presence shim). */
                WL_LOG(WL_LOG_SEC_CONSOLIDATION, WL_LOG_DEBUG,
                    "eff_iter=%u stratum=%u rel=%s N=%u D=%u "
                    "time_us=%.1f ratio=%.4f",
                    eff_iter, stratum_idx, sp->relations[ri].name,
                    cons_old, cons_new, (double)cons_elapsed / 1000.0,
                    cons_old > 0 ? (double)cons_new / (double)cons_old
                                     : 0.0);

                if (delta->nrows > 0) {
                    /* Stamp each new row with its provenance (eff_iter, stratum).
                     * worker=0 indicates the sequential (non-K-fusion) path.
                     *
                     * The array was allocated and admitted above by
                     * col_rel_enable_timestamps(); reallocating it here would
                     * leak that one and leave timestamp_capacity describing
                     * storage the governor never reserved.  It is calloc'd to
                     * capacity >= nrows, so the stamping loop can write
                     * straight into it. */
                    wl_columnar_relation_touch_storage(delta);
                    for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                        delta->timestamps[ti].iteration = eff_iter;
                        delta->timestamps[ti].stratum = stratum_idx;
                        /* worker left zero: sequential evaluation path */
                        delta->timestamps[ti].multiplicity = 1;
                    }
#ifdef WL_SESSION_TEST_HOOKS
                    if (wl_columnar_eval_serial_test_after_delta_stamp)
                        wl_columnar_eval_serial_test_after_delta_stamp(
                            sess, r, delta);
#endif

                    /* Phase 4: Enable timestamp tracking on target relation to
                     * preserve provenance through consolidation.  This enables
                     * frontier computation to determine convergence. */
                    timestamp_rc = col_rel_enable_timestamps(r);
                    if (timestamp_rc != 0) {
                        col_rel_destroy(delta);
                        outer_rc = timestamp_rc;
                        goto stride_error;
                    }

                    delta_rels[ri] = delta;
                    any_new = true;

                    /* Phase 4: Compute frontier from this delta immediately.
                     * Must happen before next sub-pass registration sets
                     * delta_rels[ri]=NULL.
                     * frontier = MAX eff_iter that produced facts. */
                    col_frontier_t rel_frontier = col_frontier_compute(delta);
                    if (rel_frontier.iteration > strat_frontier.iteration
                        || (rel_frontier.iteration == strat_frontier.iteration
                        && rel_frontier.stratum > strat_frontier.stratum)) {
                        strat_frontier = rel_frontier;
                    }
                } else {
                    col_rel_destroy(delta);
                }
            }

            /* Issue #176: Per-sub-pass cache eviction for recursive strata.
             * Materialized join results may live in delta_pool, so release
             * them before resetting the pool.  Resetting first clears the
             * pool-owned marker and leaves the cache holding stale pointers.
             * Use configurable eviction threshold (cache_evict_threshold):
             * - If 0: clear entire cache each sub-pass (backward compatible)
             * - If > 0: evict LRU entries when cache exceeds threshold */
            if (sess->cache_evict_threshold == 0) {
                col_mat_cache_release_pins(&sess->mat_cache);
                assert(sess->mat_cache.active_pins == 0);
                col_mat_cache_clear(&sess->mat_cache);
                assert(sess->mat_cache.active_pins == 0);
            } else {
                col_mat_cache_release_pins(&sess->mat_cache);
                col_mat_cache_evict_until(&sess->mat_cache,
                    sess->cache_evict_threshold);
            }

            col_session_mem_sample(sess); /* Issue #1380 */

            delta_pool_reset(sess->delta_pool);
            sess->rotation_ops->rotate_eval_arena(sess);
            /* Issue #560: Advance the compound-arena epoch frontier at the
             * end of each recursive sub-pass iteration so the
             * epoch-frontier GC can reclaim handles whose multiplicity
             * dropped to zero in this iteration.  NULL-guard each link in
             * the dispatch chain because tests and early lifecycle paths
             * may operate without a compound arena or rotation strategy.
             * The compound_arena is borrowed from the coordinator (Issue
             * #579 / R-5); only the coordinator may mutate it, so
             * worker-context invocations skip the dispatch. */
            if (sess->coordinator == NULL
                && sess->compound_arena && sess->rotation_ops
                && sess->rotation_ops->gc_epoch_boundary) {
                sess->rotation_ops->gc_epoch_boundary(sess);
            }
            if (!any_new) {
                /* Fixed point reached: no new tuples in this sub-pass. */
                converged = true;
                break;
            }
            outer_any_new = true;
            final_eff_iter = eff_iter;
            continue; /* next sub-pass */

stride_error:
            break; /* exit inner loop; outer_rc carries the error */
        } /* end inner sub-pass loop */

        if (outer_rc != 0) {
            /* Issue #282: Restore diff_operators_active on error path */
            sess->diff_operators_active = saved_diff_operators_active;
            if (sess->cleanup_pending) {
                /* Retained rule entries may borrow any registered delta or
                 * cache. Only local bookkeeping can be released here. */
                for (uint32_t ri = 0; ri < nrels; ri++)
                    assert(delta_rels[ri] == NULL);
                free(snap);
                free((void *)delta_rels);
                wl_columnar_memory_release(&delta_reservation);
                return outer_rc;
            }
            for (uint32_t ri = 0; ri < nrels; ri++) {
                /* Publication is atomic per relation. Preserve the old owner
                 * and any successfully published prefix even if their readers
                 * have released before this cleanup executes. */
                /* Best effort: outer_rc already carries the failure. */
                if (!publication_failed)
                    (void)session_remove_rel(sess,
                        sp->relations[ri].delta_name);
                if (delta_rels[ri])
                    col_rel_destroy(delta_rels[ri]);
            }
            free(snap);
            free((void *)delta_rels);
            wl_columnar_memory_release(&delta_reservation);
            return outer_rc;
        }

        /* Dispatch on why the inner loop exited */
        if (stride_all_skipped)
            continue; /* all sub-passes were frontier-skipped: next outer iter */
        if (outer_continue_next)
            continue; /* net-zero or all-empty delta: retry next outer iter */
        if (converged || !outer_any_new)
            break; /* true convergence */
    } /* end outer stride loop */
    /* Issue #282: Restore session-level diff_operators_active.
     * The per-iteration override above applies only within the recursive
     * fixed-point loop.  Restoring the saved value ensures subsequent
     * non-recursive strata and outer session logic see the correct state. */
    sess->diff_operators_active = saved_diff_operators_active;

    sess->total_iterations = final_eff_iter;

    /* Issue #914: current_iteration is deliberately left at final_eff_iter (> 0)
     * here. The non-recursive branch of col_eval_stratum is responsible for
     * resetting it to 0 before its base-case evaluation; do not add a second
     * reset here (consensus declined that). See the non-recursive branch above. */

    /* Issue #106 (US-106-005): Record per-rule frontier at convergence with epoch.
     * When stratum converges at effective iteration I, record (outer_epoch, I).
     * On next incremental snapshot, skip fires when eff_iter > I. */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        uint32_t rule_id = rule_id_base + ri;
        sess->frontier_ops->record_rule_convergence(sess, rule_id,
            sess->outer_epoch, final_eff_iter);
    }

    /* Phase 4: Update per-stratum frontier after recursive stratum evaluation.
     * strat_frontier was computed incrementally via delta timestamps (eff_iter),
     * so it already holds the effective iteration index at convergence. */
    if (strat_frontier.iteration != UINT32_MAX) {
        sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
            sess->outer_epoch, strat_frontier.iteration);

        /* Issue #317: Report recursive convergence to coordinator's progress
         * tracker so col_eval_stratum_multiworker can compute the global
         * minimum frontier after the workqueue barrier.
         *
         * A worker that records converged progress here can now return EBUSY
         * from the terminal removal below, leaving a stale converged slot.
         * progress.h offers only a per-stratum reset across all workers, so a
         * worker must not undo its own entry without clobbering its siblings.
         * Harmless today: the slot is read only after the barrier, which is
         * skipped when any worker rc is non-zero, and the next multiworker
         * call resets the stratum's slots first.  See #1661. */
        if (sess->coordinator) {
            wl_frontier_progress_record(&sess->coordinator->progress,
                sess->worker_id, stratum_idx, sess->outer_epoch,
                strat_frontier.iteration);
        }
    }

    int agg_rc = wl_columnar_eval_serial_canonicalize_aggregates(sp, sess);
    if (agg_rc != 0) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (delta_rels[ri])
                col_rel_destroy(delta_rels[ri]);
        }
        free(snap);
        free((void *)delta_rels);
        wl_columnar_memory_release(&delta_reservation);
        return agg_rc;
    }

    /* Completion point for the recursive branch: aggregate canonicalization
     * above is the last step here whose failure is propagated, so every
     * head's content is final from here.  The terminal delta removal below
     * can still be refused -- see the comment on it -- but that concerns a
     * $d$ delta, not a head.  Move this with that boundary if a step whose
     * failure is propagated is added below. */
    sess->eval_stratum_heads_final = true;

    /* Cleanup all delta relations after frontier has been computed.  A
     * refusal keeps the registry owner, and unlike the sub-pass removal
     * above it is returned: this is the only success exit that could
     * otherwise report success with a reader-held delta still registered,
     * which is the contract violation #1661 names.  See the enumeration at
     * the sub-pass site for why every other exit already returns non-zero.
     *
     * The refusal is captured and returned below the whole teardown rather
     * than from here, so no refusal path can skip delta_pool_reset,
     * rotate_eval_arena or the mat-cache release and leave state growing on
     * each retry.  The test pins that control flow reaches the arena
     * rotation, so every step up to it is covered against an early return;
     * the mat-cache release follows the rotation and rides on the single
     * bottom return alone, and nothing is pinned against a step being
     * skipped in place.
     *
     * It deliberately does not touch the frontier, and no reset is performed
     * here.  U1's comment claimed a propagated refusal must reset it or
     * should_skip_iteration would starve the retry.  That is false twice
     * over.  This sweep sits outside the stride loop, so the loop's skipping
     * cannot keep an evaluation from reaching it; and a fully skipped retry
     * cannot arise anyway, because both vtables skip only when eff_iter is
     * strictly greater than the recorded iteration, so effective iteration 0
     * is never skippable.
     * The refusal also happens below the completion point above, so the
     * heads are already final and a retry has nothing to re-derive -- it
     * needs this sweep, and it gets it.  The convergence record left
     * standing is true for the same reason the heads are final, and
     * col_session_get_frontier reports it more accurately for being left
     * alone. */
    int removal_rc = 0;
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (delta_rels[ri])
            col_rel_destroy(delta_rels[ri]);
        const char *dname = sp->relations[ri].delta_name;
        int rm = session_remove_rel(sess, dname);
        /* session_remove_rel returns exactly {0, EBUSY, ENOENT}, and
         * session.c launders any non-zero from col_rel_destroy_checked into a
         * literal EBUSY, so this is equivalent to "== EBUSY" today and cannot
         * be told apart from it by a test.  The negative form is a
         * forward-compatibility preference: a third code should propagate
         * rather than vanish.  ENOENT is the common case and not a failure --
         * a relation that published no delta this stratum has nothing to
         * remove.  Keep sweeping so a later relation's private delta is still
         * destroyed and its registration still cleared. */
        if (rm != 0 && rm != ENOENT && removal_rc == 0)
            removal_rc = rm;
    }
    free(snap);
    free((void *)delta_rels);
    wl_columnar_memory_release(&delta_reservation);
    col_session_mem_sample(sess); /* Issue #1380 */
    delta_pool_reset(sess->delta_pool);
    sess->rotation_ops->rotate_eval_arena(sess);
    col_mat_cache_release_pins(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);
    col_mat_cache_clear(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);

    /* One return, below the teardown, so no refusal path can skip
     * delta_pool_reset or rotate_eval_arena -- otherwise pool and arena usage
     * would grow on every retry. */
    return removal_rc;
}
static col_frontier_t
col_frontier_compute(const col_rel_t *rel)
{
    col_frontier_t f = { 0, 0 };

    /* Handle NULL or empty relation */
    if (!rel || rel->nrows == 0 || !rel->timestamps)
        return f;

    /* Initialize frontier to first row's timestamp */
    f.iteration = rel->timestamps[0].iteration;
    f.stratum = rel->timestamps[0].stratum;

    /* Find minimum (iteration, stratum) */
    for (uint32_t i = 1; i < rel->nrows; i++) {
        const col_delta_timestamp_t *ts = &rel->timestamps[i];
        if (ts->iteration < f.iteration
            || (ts->iteration == f.iteration && ts->stratum < f.stratum)) {
            f.iteration = ts->iteration;
            f.stratum = ts->stratum;
        }
    }

    return f;
}
