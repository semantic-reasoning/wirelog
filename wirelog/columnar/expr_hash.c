/* Bounded XXH3 cooperation boundary (#2299).
 * Copyright (C) CleverPlant; licensed under LGPL-3.0.
 * The pinned vendor implementation and its stack-state layout must share a
 * header identity; exposed states must not cross the shared xxHash ABI.
 * Vendor reset/update/digest bodies remain narrow synchronous library
 * boundaries even under compiler inlining/LTO. Updates admit at most64 input
 * bytes; the vendor can also process its fixed256-byte buffer/retained stripes.
 * Engine tariffs cover caller work, not a SIMD instruction-count claim. */
#define XXH_INLINE_ALL
#include <xxhash.h>
#include "internal.h"
#include <errno.h>

typedef char wl_columnar_expr_hash_tariff_fits_quantum[
        (UINT64_C(179) <= UINT64_C(224) &&
        UINT64_C(224) <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

#ifdef WL_SESSION_TEST_HOOKS
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
#endif

static int
wl_columnar_expr_hash_poll(wl_evaluation_control_t *control, uint64_t id,
    unsigned phase, size_t progress)
{
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_test_checkpoint)
        wl_columnar_expr_test_checkpoint(control, phase, progress, false);
#else
    (void)phase; (void)progress;
#endif
    return wl_evaluation_control_charge_attempt(control, id, 0);
}

static int
wl_columnar_expr_hash_region(wl_evaluation_control_t *control, uint64_t id,
    unsigned phase, size_t progress)
{
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(control, id, 224);
    return rc ? rc : wl_columnar_expr_hash_poll(control, id, phase, progress);
}

int
wl_columnar_expr_hash_bytes_attempt_controlled(const uint8_t *bytes,
    size_t length, int64_t *result, wl_evaluation_control_t *control,
    uint64_t id)
{
    if ((!bytes && length) || !result) return EINVAL;
    int rc = wl_columnar_expr_hash_region(control, id, 120, length);
    if (rc) return rc;
    /* Vendor-declared alignment and initialization, with no heap state. */
    XXH3_state_t state;
    rc = wl_columnar_expr_hash_region(control, id, 121, 0);
    if (rc) return rc;
    XXH3_INITSTATE(&state);
    XXH_errorcode status = XXH3_64bits_reset(&state);
    rc = wl_columnar_expr_hash_poll(control, id, 122, 0);
    if (rc) return rc;
    if (status != XXH_OK) return EIO;
    size_t offset = 0;
    while (offset < length) {
        size_t chunk = length - offset;
        if (chunk > 64) chunk = 64;
        rc = wl_columnar_expr_hash_region(control, id, 123, offset);
        if (rc) return rc;
        status = XXH3_64bits_update(&state, bytes + offset, chunk);
        rc = wl_columnar_expr_hash_poll(control, id, 124, offset);
        if (rc) return rc;
        if (status != XXH_OK) return EIO;
        offset += chunk;
    }
    rc = wl_columnar_expr_hash_region(control, id, 125, offset);
    if (rc) return rc;
    XXH64_hash_t value = XXH3_64bits_digest(&state);
    rc = wl_columnar_expr_hash_poll(control, id, 126, offset);
    if (rc) return rc;
    rc = wl_columnar_expr_hash_region(control, id, 127, offset);
    if (rc) return rc;
    *result = (int64_t)value;
    return 0;
}
