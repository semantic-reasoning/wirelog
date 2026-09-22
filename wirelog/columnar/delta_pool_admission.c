/* columnar/delta_pool_admission.c - shared-governor glue for delta pools */

#include "columnar/delta_pool.h"

#include <stdint.h>
#include <stdlib.h>

#define WL_DELTA_POOL_ALIGN 8u

typedef struct {
    wl_columnar_memory_reservation_t reservation;
} wl_delta_pool_admission_t;

static void
release_delta_pool_admission(void *context)
{
    wl_delta_pool_admission_t *admission = context;

    if (!admission)
        return;
    (void)wl_columnar_memory_release(&admission->reservation);
    free(admission);
}

static bool
delta_pool_footprint(uint32_t max_slots, size_t slot_size,
    size_t arena_bytes, uint64_t *out)
{
    uint64_t aligned_slot;
    uint64_t slab_bytes;
    uint64_t total;

    if (!out || max_slots == 0 || slot_size == 0 || arena_bytes == 0
        || slot_size > SIZE_MAX - (WL_DELTA_POOL_ALIGN - 1))
        return false;
    aligned_slot = (uint64_t)((slot_size + (WL_DELTA_POOL_ALIGN - 1))
        & ~(size_t)(WL_DELTA_POOL_ALIGN - 1));
    if (!wl_columnar_memory_size_mul((uint64_t)max_slots, aligned_slot,
        &slab_bytes)
        || !wl_columnar_memory_size_add(slab_bytes, (uint64_t)arena_bytes,
        &total))
        return false;
    *out = total;
    return true;
}

delta_pool_t *
delta_pool_create_managed(uint32_t max_slots, size_t slot_size,
    size_t arena_bytes, wl_columnar_memory_governor_t *governor)
{
    return delta_pool_create_managed_status(max_slots, slot_size, arena_bytes,
               governor, NULL);
}

delta_pool_t *
delta_pool_create_managed_status(uint32_t max_slots, size_t slot_size,
    size_t arena_bytes, wl_columnar_memory_governor_t *governor,
    wl_columnar_memory_admission_status_t *status_out)
{
    wl_delta_pool_admission_t *admission;
    wl_columnar_memory_admission_status_t status;
    delta_pool_t *pool;
    uint64_t footprint;

    if (status_out)
        *status_out = WL_COLUMNAR_MEMORY_ADMISSION_OK;
    if (!governor)
        return delta_pool_create(max_slots, slot_size, arena_bytes);
    if (!delta_pool_footprint(max_slots, slot_size, arena_bytes, &footprint)) {
        if (status_out)
            *status_out = WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW;
        return NULL;
    }
    admission = (wl_delta_pool_admission_t *)malloc(sizeof(*admission));
    if (!admission)
        return NULL;
    wl_columnar_memory_reservation_init(&admission->reservation);
    status = wl_columnar_memory_reserve_checked(governor, footprint,
            &admission->reservation);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        if (status_out)
            *status_out = status;
        free(admission);
        return NULL;
    }
    pool = delta_pool_create_with_admission(max_slots, slot_size, arena_bytes,
            admission, release_delta_pool_admission);
    if (!pool) {
        (void)wl_columnar_memory_rollback(&admission->reservation);
        free(admission);
        return NULL;
    }
    if (!wl_columnar_memory_commit(&admission->reservation, pool)) {
        delta_pool_destroy(pool);
        return NULL;
    }
    return pool;
}
