/* Attempt-bound intern transactions (#2299).
 * Copyright (C) CleverPlant; licensed under LGPL-3.0. */
#include "intern_private.h"
#include "columnar/internal.h"
#include <stdlib.h>
#include <stdint.h>

/* #2299: attempt-bound insertion. Existing raw writers remain unchanged.
 * Owner/pin and stable-governor borrowing are caller prerequisites. All
 * variable loops are private or read-only until the final cutoff. */
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_intern_control_test_checkpoint)(wl_evaluation_control_t *, unsigned,
    uint64_t);
int wl_intern_control_test_fail_allocation = -1;
uint32_t wl_intern_control_test_slot_capacity(const wl_intern_t *intern)
{
    return intern->slot_capacity; /* Isolated, quiescent test observation. */
}
#endif

static void *
wl_intern_control_allocate(size_t size)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_intern_control_test_fail_allocation >= 0
        && wl_intern_control_test_fail_allocation-- == 0)
        return NULL;
#endif
    return malloc(size);
}

static int
wl_intern_control_region(wl_evaluation_control_t *control, uint64_t attempt_id,
    unsigned phase, uint64_t progress)
{
    if (!control || !attempt_id
        || wl_evaluation_control_active_attempt_id(control) != attempt_id)
        return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(control, attempt_id, 224);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_intern_control_test_checkpoint)
        wl_intern_control_test_checkpoint(control, phase, progress);
#else
    (void)phase;
    (void)progress;
#endif
    return wl_evaluation_control_charge_attempt(control, attempt_id, 0);
}

static int
wl_intern_control_empty_slot(wl_intern_slot_t *slots, uint32_t capacity,
    uint32_t hash, uint32_t *slot, wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    if (!slots || capacity < INTERN_INITIAL_CAP
        || (capacity & (capacity - 1u))) return EINVAL;
    uint32_t h = hash & (capacity - 1u);
    for (;;) {
        int rc = wl_intern_control_region(control, attempt_id, 9, h);
        if (rc) return rc;
        if (slots[h].string_id == SLOT_EMPTY) {
            *slot = h;
            return 0;
        }
        h = (h + 1u) & (capacity - 1u);
    }
}

int
wl_intern_control_put_attempt(wl_intern_t *intern, const char *source,
    int64_t *out_id, wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if (!intern || !source || !out_id) return EINVAL;
    int rc = wl_intern_control_region(control, attempt_id, 1, 0);
    if (rc) return rc;
    /* First disposal is prepaid before acquiring each local resource.
     * Refund/release CAS retry and allocator/mutex bodies are mandatory
     * cleanup/host boundaries, never claimed finite by this funding. */
    rc = wl_intern_control_region(control, attempt_id, 13, 0);
    if (rc) return rc;
    size_t length = 0, copied = 0;
    rc = wl_columnar_expr_cstr_length_attempt_controlled(source, &length,
            control, attempt_id);
    if (rc) return rc;
    if (length == SIZE_MAX) return EOVERFLOW;
    rc = wl_intern_control_region(control, attempt_id, 14, 0);
    if (rc) return rc;
    char *copy = wl_intern_control_allocate(length + 1u);
    if (!copy) return ENOMEM;
    char **segment = NULL;
    wl_intern_slot_t *slots = NULL;
    bool locked = false, pending_valid = false;
    wl_columnar_memory_reservation_t pending, retirement;
    wl_columnar_memory_reservation_init(&pending);
    wl_columnar_memory_reservation_init(&retirement);
    rc = wl_columnar_expr_bytes_copy_attempt_controlled((uint8_t *)copy,
            (const uint8_t *)source, length + 1u, &copied, control, attempt_id);
    if (rc) goto fail;
    uint32_t hash = 0;
    rc = wl_columnar_expr_bytes_hash_attempt_controlled((const uint8_t *)copy,
            length, UINT32_C(2166136261), &hash, control, attempt_id);
    if (rc) goto fail;
    rc = wl_intern_control_region(control, attempt_id, 2, length);
    if (rc) goto fail;
    /* Prepay first unlock, then the caller's lock-acquisition region. */
    rc = wl_intern_control_region(control, attempt_id, 13, 1);
    if (rc) goto fail;
    rc = wl_intern_control_region(control, attempt_id, 14, 1);
    if (rc) goto fail;
    wl_mutex_lock(&intern->lock);
    locked = true;
    rc = wl_intern_control_region(control, attempt_id, 3, 0);
    if (rc) goto fail;
    uint32_t old_capacity = intern->slot_capacity;
    /* Constructors start at64 and writers only double. Capture that fixed
     * geometry while holding the writer lock; probes never accept a smaller
     * or non-power-of-two private table. The added fixed checks remain within
     * the already funded224 region and do not add a work admission. */
    if (!intern->slots || old_capacity < INTERN_INITIAL_CAP
        || (old_capacity & (old_capacity - 1u))) {
        rc = EINVAL; goto fail;
    }
    uint32_t h = hash & (old_capacity - 1u);
    for (;;) {
        rc = wl_intern_control_region(control, attempt_id, 4, h);
        if (rc) goto fail;
        uint32_t existing = intern->slots[h].string_id;
        if (existing == SLOT_EMPTY) break;
        const char *candidate = intern_string_at(intern, existing);
        size_t candidate_length = 0;
        rc = wl_columnar_expr_cstr_length_attempt_controlled(candidate,
                &candidate_length, control, attempt_id);
        if (rc) goto fail;
        if (candidate_length == length) {
            int ordering = 0;
            rc = wl_columnar_expr_bytes_compare_attempt_controlled(
                (const uint8_t *)copy, (const uint8_t *)candidate, length,
                &ordering, control, attempt_id);
            if (rc) goto fail;
            if (!ordering) {
                rc = wl_intern_control_region(control, attempt_id, 10, 0);
                if (rc) goto fail;
                wl_mutex_unlock(&intern->lock);
                free(copy);
                *out_id = existing;
                return 0;
            }
        }
        h = (h + 1u) & (old_capacity - 1u);
    }
    rc = wl_intern_control_region(control, attempt_id, 15, 0);
    if (rc) goto fail;
    uint32_t count = WL_INTERN_LOAD_RELAXED(&intern->count);
    if (count >= INTERN_MAX_STRINGS) {
        rc = EOVERFLOW; goto fail;
    }
    uint32_t seg = intern_seg_of(count);
    bool new_segment = intern->segments[seg] == NULL;
    bool resize = (uint64_t)(count + 1u) * INTERN_LOAD_FACTOR_DEN
        > (uint64_t)old_capacity * INTERN_LOAD_FACTOR_NUM;
    if (resize && old_capacity > UINT32_MAX / 2u) {
        rc = EOVERFLOW; goto fail;
    }
    uint32_t capacity = resize ? old_capacity * 2u
                              : old_capacity;
    uint64_t segment_bytes = 0, slot_bytes = 0, retired_bytes = 0;
    rc = wl_intern_control_region(control, attempt_id, 15, 1);
    if (rc) goto fail;
    if ((new_segment && !wl_columnar_memory_size_mul(intern_seg_size(seg),
        sizeof(char *), &segment_bytes))
        || (resize && (!wl_columnar_memory_size_mul(capacity,
        sizeof(wl_intern_slot_t), &slot_bytes)
        || !wl_columnar_memory_size_mul(old_capacity,
        sizeof(wl_intern_slot_t), &retired_bytes)))
        || segment_bytes > SIZE_MAX || slot_bytes > SIZE_MAX) {
        rc = EOVERFLOW; goto fail;
    }
    rc = wl_intern_control_region(control, attempt_id, 15, 2);
    if (rc) goto fail;
    uint64_t retained = 0;
    if (intern->memory_governor) {
        wl_columnar_memory_reservation_t *old = &intern->reservation;
        if (!intern->reserved_bytes || old->identity != old
            || old->governor !=
            wl_columnar_memory_governor_ref_get(intern->memory_governor)
            || old->bytes != intern->reserved_bytes
            || atomic_load_explicit(&old->state, memory_order_acquire)
            != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            || atomic_load_explicit(&old->owner_bits, memory_order_relaxed)
            != (uint64_t)(uintptr_t)intern
            || retired_bytes > intern->reserved_bytes) {
            rc = EINVAL; goto fail;
        }
        retained = intern->reserved_bytes - retired_bytes;
        if (!wl_columnar_memory_size_add(retained, length + 1u, &retained)
            || !wl_columnar_memory_size_add(retained, segment_bytes, &retained)
            || !wl_columnar_memory_size_add(retained, slot_bytes, &retained)) {
            rc = EOVERFLOW; goto fail;
        }
        rc = wl_intern_control_region(control, attempt_id, 13, 2);
        if (rc) goto fail;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            old->governor, retained, &pending, &status, control, attempt_id);
        /* A stopped reserve owns its own mandatory refund. */
        if (rc) goto fail;
        pending_valid = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        if (!pending_valid) {
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? WL_INTERN_ERR_MEMORY_BUDGET
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
            goto fail;
        }
    }
    rc = wl_intern_control_region(control, attempt_id, 5, count);
    if (rc) goto fail;
    if (new_segment) {
        rc = wl_intern_control_region(control, attempt_id, 13, 3);
        if (rc) goto fail;
        rc = wl_intern_control_region(control, attempt_id, 14, 3);
        if (rc) goto fail;
        segment = (char **)wl_intern_control_allocate((size_t)segment_bytes);
        if (!segment) {
            rc = ENOMEM; goto fail;
        }
        for (uint32_t i = 0; i < intern_seg_size(seg); ++i) {
            rc = wl_intern_control_region(control, attempt_id, 6, i);
            if (rc) goto fail;
            segment[i] = NULL;
        }
    }
    if (resize) {
        rc = wl_intern_control_region(control, attempt_id, 13, 4);
        if (rc) goto fail;
        rc = wl_intern_control_region(control, attempt_id, 14, 4);
        if (rc) goto fail;
        slots = wl_intern_control_allocate((size_t)slot_bytes);
        if (!slots) {
            rc = ENOMEM; goto fail;
        }
        for (uint32_t i = 0; i < capacity; ++i) {
            rc = wl_intern_control_region(control, attempt_id, 7, i);
            if (rc) goto fail;
            slots[i].string_id = SLOT_EMPTY;
        }
        for (uint32_t i = 0; i < count; ++i) {
            rc = wl_intern_control_region(control, attempt_id, 8, i);
            if (rc) goto fail;
            const char *existing = intern_string_at(intern, i);
            size_t n = 0;
            uint32_t existing_hash = 0, target = 0;
            rc = wl_columnar_expr_cstr_length_attempt_controlled(existing,
                    &n, control, attempt_id);
            if (rc) goto fail;
            rc = wl_columnar_expr_bytes_hash_attempt_controlled(
                (const uint8_t *)existing, n, UINT32_C(2166136261),
                &existing_hash, control, attempt_id);
            if (rc) goto fail;
            rc = wl_intern_control_empty_slot(slots, capacity, existing_hash,
                    &target, control, attempt_id);
            if (rc) goto fail;
            rc = wl_intern_control_region(control, attempt_id, 16, i);
            if (rc) goto fail;
            slots[target].string_id = i;
        }
        rc = wl_intern_control_empty_slot(slots, capacity, hash, &h,
                control, attempt_id);
        if (rc) goto fail;
    }
    /* Two prepaid fixed regions fund ownership publication and first old
    * retirement. The actual indivisible suffix is two straight-line moves
    * plus metadata/count publication (<192 logical actions, no loop).
    * Allocator/mutex bodies and mandatory refund contention remain U. */
    rc = wl_intern_control_region(control, attempt_id, 17, 0);
    if (rc) goto fail;
    rc = wl_intern_control_region(control, attempt_id, 17, 1);
    if (rc) goto fail;
    rc = wl_intern_control_region(control, attempt_id, 10, count);
    if (rc) goto fail;
    if (pending_valid) {
        bool committed = false;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(&pending,
                intern, &committed, control, attempt_id);
        if (rc) goto fail;
        if (!committed) {
            rc = ENOMEM; goto fail;
        }
        /* Exclusivity, identities, source state and owner were validated
         * above; locals are initialized EMPTY. A failed move here is an
         * internal invariant violation, never a recoverable partial return. */
        if (!wl_columnar_memory_reservation_move(&retirement,
            &intern->reservation)
            || !wl_columnar_memory_reservation_move(&intern->reservation,
            &pending)) abort();
        intern->reserved_bytes = retained;
    }
    /* Ungoverned tables use the same preceding final guarded cutoff. */
    wl_intern_slot_t *retired_slots = NULL;
    if (slots) {
        retired_slots = intern->slots;
        intern->slots = slots;
        intern->slot_capacity = capacity;
    }
    if (segment) intern->segments[seg] = segment;
    intern->segments[seg][count - intern_seg_base(seg)] = copy;
    intern->slots[h].string_id = count;
    WL_INTERN_STORE_RELEASE(&intern->count, count + 1u);
    /* Keep later writers behind the lock until the old footprint is retired,
     * as raw insertion does. Mandatory disposal itself is noncancellable. */
    free(retired_slots);
    if (retirement.governor &&
        !wl_columnar_memory_release(&retirement)) abort();
    wl_mutex_unlock(&intern->lock);
    *out_id = count;
    return 0;
fail:
    /* Cleanup never polls or invokes hooks; it cannot debit a successor. */
    free(copy);
    free((void *)segment);
    free(slots);
    if (pending_valid && !wl_columnar_memory_rollback(&pending)) abort();
    if (locked) wl_mutex_unlock(&intern->lock);
    return rc;
}
