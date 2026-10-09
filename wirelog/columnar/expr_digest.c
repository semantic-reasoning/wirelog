/* Scoped controlled unary PSA digests. Copyright (C) CleverPlant.
 * Licensed under LGPL-3.0. */
#include "internal.h"
#include <errno.h>
#include <string.h>
#ifdef WL_MBEDTLS_ENABLED
#include <psa/crypto.h>
#endif
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_expr_digest_test_checkpoint)(wl_evaluation_control_t *,
    int, uint64_t, bool);
int64_t (*wl_columnar_expr_digest_test_provider_result)(int, int64_t, size_t *);
#endif
#ifdef WL_MBEDTLS_ENABLED
static void
wl_columnar_expr_digest_checkpoint(wl_evaluation_control_t *c, int stage,
    uint64_t progress, bool failed)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_digest_test_checkpoint)
        wl_columnar_expr_digest_test_checkpoint(c, stage, progress, failed);
#else
    (void)c; (void)stage; (void)progress; (void)failed;
#endif
}
/* Test report overrides occur ONLY after the genuine provider call. Actual
 * finish success terminates even if a fixture changes reported status/length. */
static psa_status_t
wl_columnar_expr_digest_report(int stage, psa_status_t actual, size_t *length)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_digest_test_provider_result)
        return (psa_status_t)wl_columnar_expr_digest_test_provider_result(stage,
                   actual, length);
#else
    (void)stage; (void)length;
#endif
    return actual;
}
#endif
int
wl_columnar_expr_digest_run(uint8_t op, const uint8_t *bytes, size_t length,
    int64_t *value, bool *failed, wl_evaluation_control_t *control)
{
#ifndef WL_MBEDTLS_ENABLED
    (void)op; (void)bytes; (void)length; (void)value; (void)control;
    *failed = true; return 0;
#else
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    psa_algorithm_t alg; size_t expected;
    switch (op) {
    case WL_PLAN_EXPR_ARITH_MD5: case WL_PLAN_EXPR_ARITH_MD5_S:
        alg = PSA_ALG_MD5; expected = 16; break;
    case WL_PLAN_EXPR_ARITH_SHA1: case WL_PLAN_EXPR_ARITH_SHA1_S:
        alg = PSA_ALG_SHA_1; expected = 20; break;
    case WL_PLAN_EXPR_ARITH_SHA256: case WL_PLAN_EXPR_ARITH_SHA256_S:
        alg = PSA_ALG_SHA_256; expected = 32; break;
    default: alg = PSA_ALG_SHA_512; expected = 64; break;
    }
    bool attempted = false, terminated = false, semantic = false;
    psa_hash_operation_t operation;
    unsigned char digest[64]; size_t actual_length = 0;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    /* Public all-bits-zero contract, before provider operation ownership.
     * Never access private fields or copy/reinitialize an active operation. */
    for (size_t offset = 0; offset < sizeof(operation);) {
        size_t count = sizeof(operation) - offset;
        if (count > 128) count = 128;
        rc = wl_evaluation_control_charge(control, count + 8);
        if (rc) return rc;
        memset((unsigned char *)&operation + offset, 0, count);
        offset += count;
        wl_columnar_expr_digest_checkpoint(control, 35, offset, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    /* Narrow external resource/time exceptions: init/setup128, update128+
     * incoming<=64, finish128+capacity<=64. Provider hidden workspace/global
     * runtime is not session-G memory. Successful init is retained globally. */
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) return rc;
    psa_status_t status = wl_columnar_expr_digest_report(0, psa_crypto_init(),
            NULL);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (status != PSA_SUCCESS) {
        *failed = true; return 0;
    }
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) return rc;
    attempted = true;
    status = wl_columnar_expr_digest_report(1,
            psa_hash_setup(&operation, alg), NULL);
    semantic = status != PSA_SUCCESS;
    wl_columnar_expr_digest_checkpoint(control, 31, 0, semantic);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc || semantic) goto cleanup;
    for (size_t offset = 0; offset < length;) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto cleanup;
        size_t count = length - offset;
        if (count > 64) count = 64;
        const uint8_t *part = bytes + offset;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto cleanup;
        rc = wl_evaluation_control_charge(control, 128 + count);
        if (rc) goto cleanup;
        status = wl_columnar_expr_digest_report(2,
                psa_hash_update(&operation, part, count), NULL);
        semantic = status != PSA_SUCCESS;
        wl_columnar_expr_digest_checkpoint(control, 32, offset + count,
            semantic);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc || semantic) goto cleanup;
        rc = wl_evaluation_control_charge(control, 4);
        if (rc) goto cleanup;
        offset += count;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto cleanup;
    }
    rc = wl_evaluation_control_charge(control, 128 + expected);
    if (rc) goto cleanup;
    status = psa_hash_finish(&operation, digest, expected, &actual_length);
    terminated = status == PSA_SUCCESS;
    status = wl_columnar_expr_digest_report(3, status, &actual_length);
    semantic = status != PSA_SUCCESS || actual_length != expected;
    wl_columnar_expr_digest_checkpoint(control, 33, actual_length, semantic);
    rc = wl_evaluation_control_charge(control, 0);
cleanup:
    if (attempted && !terminated) {
        /* Mandatory terminal external cleanup cannot be denied by control.
         * Public abort contract, not proof faulty drivers reclaim hidden heap. */
        status = wl_columnar_expr_digest_report(4,
                psa_hash_abort(&operation), NULL);
        if (status != PSA_SUCCESS && rc <= 0) rc = EIO;
    }
    if (rc) return rc;
    if (semantic) {
        *failed = true; return 0;
    }
    int64_t pending; bool fold_failed;
    rc = wl_columnar_expr_hash_run(digest, expected, &pending, &fold_failed,
            control);
    if (rc) return rc;
    wl_columnar_expr_digest_checkpoint(control, 34, expected, fold_failed);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    *failed = fold_failed;
    if (!fold_failed) *value = pending;
    return 0;
#endif
}
