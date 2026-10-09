/* Bounded private XXH3 streaming for ordinary controlled expressions.
 * Copyright (C) CleverPlant. Licensed under LGPL-3.0. */
#define XXH_INLINE_ALL
#include <xxhash.h>
#include "internal.h"
#include <errno.h>
#include <stdint.h>

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_expr_hash_test_controlled_checkpoint)(
    wl_evaluation_control_t *, int, uint64_t, bool);
#endif
static void
wl_columnar_expr_hash_checkpoint(wl_evaluation_control_t *control, int stage,
    uint64_t progress, bool failed)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_hash_test_controlled_checkpoint)
        wl_columnar_expr_hash_test_controlled_checkpoint(control, stage,
            progress, failed);
#else
    (void)control; (void)stage; (void)progress; (void)failed;
#endif
}
/* All streaming calls use this TU's pinned inline implementation, never the
 * external shared API with an exposed state. Upstream permits uninitialized
 * automatic state when FIRST reset is default; no full-state clear is needed.
 * Reset128/update128+bytes<=64/digest128 are the reviewed bounded library
 * exception: update may consume carried256/four stripes plus one scramble;
 * digest has fixed short<=240 or buffered<=256/last-stripe work, not CPU counts. */
int
wl_columnar_expr_hash_run(const uint8_t *bytes, size_t length, int64_t *value,
    bool *failed, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    XXH_ALIGN(64) XXH3_state_t state;
    uintptr_t address = (uintptr_t)&state;
    bool aligned = address % 64 == 0;
    wl_columnar_expr_hash_checkpoint(control, 26, (uint64_t)address, !aligned);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (!aligned) return EINVAL;
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) return rc;
    bool bad = XXH3_64bits_reset(&state) != XXH_OK;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (bad) {
        *failed = true; return 0;
    }
    size_t offset = 0;
    while (offset < length) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        size_t count = length - offset;
        if (count > 64) count = 64;
        const uint8_t *part = bytes + offset;
        wl_columnar_expr_hash_checkpoint(control, 27, offset + count, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 128 + count);
        if (rc) return rc;
        bad = XXH3_64bits_update(&state, part, count) != XXH_OK;
        wl_columnar_expr_hash_checkpoint(control, 28, offset + count, bad);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (bad) {
            *failed = true; return 0;
        }
        rc = wl_evaluation_control_charge(control, 4);
        if (rc) return rc;
        offset += count;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) return rc;
    int64_t pending = (int64_t)XXH3_64bits_digest(&state);
    wl_columnar_expr_hash_checkpoint(control, 29, length, false);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    *value = pending; *failed = false;
    return 0;
}
