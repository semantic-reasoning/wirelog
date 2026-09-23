/* compound_arena_admission.c - shared-governor glue for fixed metadata */

#include "compound_arena.h"

#include "columnar/memory_governor.h"

#include <errno.h>
#include <stdlib.h>

typedef struct {
    wl_columnar_memory_governor_t *governor;
    wl_columnar_memory_reservation_t reservation;
} wl_compound_arena_admission_t;

static void
release_compound_arena_admission(void *context)
{
    wl_compound_arena_admission_t *admission = context;

    if (!admission)
        return;
    (void)wl_columnar_memory_release(&admission->reservation);
    free(admission);
}

static int
prepare_compound_growth(void *context, uint64_t old_bytes,
    uint64_t new_bytes, wl_columnar_memory_reservation_t *reservation)
{
    wl_compound_arena_admission_t *admission = context;
    wl_columnar_memory_admission_status_t status;

    wl_columnar_memory_reservation_init(reservation);
    if (old_bytes == 0)
        status = wl_columnar_memory_reserve_checked(admission->governor,
                new_bytes, reservation);
    else
        status = wl_columnar_memory_reserve_growth(admission->governor,
                old_bytes, new_bytes, reservation);
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OK
           || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY ? 0 : -1;
}

static int
publish_compound_growth(void *context,
    wl_columnar_memory_reservation_t *old_reservation,
    wl_columnar_memory_reservation_t *new_reservation, const void *owner)
{
    (void)context;
    (void)old_reservation;
    if (!wl_columnar_memory_commit(new_reservation, owner))
        return -1;
    return 0;
}

static void
abort_compound_growth(void *context,
    wl_columnar_memory_reservation_t *reservation)
{
    (void)context;
    (void)wl_columnar_memory_rollback(reservation);
}

static int
release_compound_growth(void *context,
    wl_columnar_memory_reservation_t *reservation)
{
    (void)context;
    /* A committed token is owned exclusively by this generation. A false
     * result means it was already terminal, so no bytes remain to release;
     * treating that state as idempotent keeps destruction leak-free. */
    (void)wl_columnar_memory_release(reservation);
    return 0;
}

static uint32_t
normalize_max_epochs(uint32_t max_epochs)
{
    if (max_epochs == 0)
        return WL_COMPOUND_DEFAULT_MAX_EPOCHS;
    if (max_epochs > WL_COMPOUND_DEFAULT_MAX_EPOCHS)
        return WL_COMPOUND_DEFAULT_MAX_EPOCHS;
    return max_epochs;
}

int
wl_compound_arena_create_managed_checked(uint32_t session_seed,
    uint32_t default_gen_cap, uint32_t max_epochs,
    wl_columnar_memory_governor_t *governor, wl_compound_arena_t **out)
{
    wl_compound_arena_admission_t *admission;
    wl_columnar_memory_admission_status_t status;
    wl_compound_arena_t *arena;
    uint64_t gens_bytes;
    uint64_t fixed_bytes;

    if (!out)
        return EINVAL;
    *out = NULL;
    if (default_gen_cap == 0)
        return EINVAL;
    if (!governor) {
        *out = wl_compound_arena_create(session_seed, default_gen_cap,
                max_epochs);
        return *out ? 0 : ENOMEM;
    }
    max_epochs = normalize_max_epochs(max_epochs);
    if (!wl_columnar_memory_size_mul(max_epochs,
        sizeof(wl_compound_gen_t), &gens_bytes)
        || !wl_columnar_memory_size_add(sizeof(wl_compound_arena_t),
        gens_bytes, &fixed_bytes))
        return EOVERFLOW;

    admission = (wl_compound_arena_admission_t *)malloc(sizeof(*admission));
    if (!admission)
        return ENOMEM;
    admission->governor = governor;
    wl_columnar_memory_reservation_init(&admission->reservation);
    status = wl_columnar_memory_reserve_checked(governor, fixed_bytes,
            &admission->reservation);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        free(admission);
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
            : EINVAL;
    }

    arena = wl_compound_arena_create_with_admission(session_seed,
            default_gen_cap, max_epochs, admission,
            release_compound_arena_admission, prepare_compound_growth,
            publish_compound_growth, abort_compound_growth,
            release_compound_growth);
    if (!arena) {
        (void)wl_columnar_memory_rollback(&admission->reservation);
        free(admission);
        return ENOMEM;
    }
    if (!wl_columnar_memory_commit(&admission->reservation, arena)) {
        wl_compound_arena_free(arena);
        return EINVAL;
    }
    *out = arena;
    return 0;
}

wl_compound_arena_t *
wl_compound_arena_create_managed(uint32_t session_seed,
    uint32_t default_gen_cap, uint32_t max_epochs,
    wl_columnar_memory_governor_t *governor)
{
    wl_compound_arena_t *arena = NULL;
    (void)wl_compound_arena_create_managed_checked(session_seed,
        default_gen_cap, max_epochs, governor, &arena);
    return arena;
}
