/* Scoped controlled PSA digests and UUID5 pairs. Copyright (C) CleverPlant.
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
static int
wl_columnar_expr_digest_segments(uint8_t op, const uint8_t *const *parts,
    const size_t *lengths, size_t part_count, bool uuid, char *uuid_text,
    int64_t *value, bool *failed, wl_evaluation_control_t *control)
{
#ifndef WL_MBEDTLS_ENABLED
    (void)op; (void)parts; (void)lengths; (void)part_count; (void)uuid;
    (void)uuid_text;
    (void)value; (void)control;
    *failed = true; return 0;
#else
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    psa_algorithm_t alg; size_t expected;
    switch (op) {
    case WL_PLAN_EXPR_ARITH_UUID5: alg = PSA_ALG_SHA_1; expected = 20; break;
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
    for (size_t segment = 0; segment < part_count; segment++) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto cleanup;
        const uint8_t *bytes = parts[segment]; size_t length = lengths[segment];
        wl_columnar_expr_digest_checkpoint(control, 36, segment, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto cleanup;
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
            if (uuid) wl_columnar_expr_digest_checkpoint(control,
                    40 + (int)segment, offset + count, semantic);
            rc = wl_evaluation_control_charge(control, 0);
            if (rc || semantic) goto cleanup;
            rc = wl_evaluation_control_charge(control, 4);
            if (rc) goto cleanup;
            offset += count;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) goto cleanup;
        }
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
    if (uuid) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        digest[6] = (digest[6] & 0x0f) | 0x50;
        digest[8] = (digest[8] & 0x3f) | 0x80;
        wl_columnar_expr_digest_checkpoint(control, 45, 20, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (uuid_text) {
            static const char hex[] = "0123456789abcdef";
            size_t out = 0;
            for (size_t i = 0; i < 16; i++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) return rc;
                uuid_text[out++] = hex[digest[i] >> 4];
                uuid_text[out++] = hex[digest[i] & 0x0f];
                if (i == 3 || i == 5 || i == 7 || i == 9)
                    uuid_text[out++] = '-';
                wl_columnar_expr_digest_checkpoint(control, 47, i + 1, false);
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) return rc;
            uuid_text[out] = 0;
            wl_columnar_expr_digest_checkpoint(control, 48, out, false);
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
            *failed = false; return 0;
        }
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        int64_t pending;
        memcpy(&pending, digest, 8);
        wl_columnar_expr_digest_checkpoint(control, 46, 8, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        *value = pending; *failed = false; return 0;
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

int
wl_columnar_expr_digest_run(uint8_t op, const uint8_t *bytes, size_t length,
    int64_t *value, bool *failed, wl_evaluation_control_t *control)
{
    return wl_columnar_expr_digest_segments(op, &bytes, &length, 1, false, NULL,
               value, failed, control);
}
int
wl_columnar_expr_uuid_pair_run(const uint8_t *ns, size_t ns_length,
    bool ns_symbol, const uint8_t *name, size_t name_length, bool name_symbol,
    bool framed, int64_t *value, bool *failed, wl_evaluation_control_t *control)
{
    uint8_t headers[2][9];
    const uint8_t *parts[4]; size_t lengths[4]; size_t count = 0;
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    const uint8_t *bodies[2] = {ns, name};
    const size_t sizes[2] = {ns_length, name_length};
    const bool domains[2] = {ns_symbol, name_symbol};
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    for (size_t i = 0; i < 2; i++) {
        rc = wl_evaluation_control_charge(control, 24);
        if (rc) return rc;
        if (framed) {
            headers[i][0] = domains[i] ? 'S' : 'I';
            uint64_t length = (uint64_t)sizes[i];
            for (unsigned j = 0; j < 8; j++)
                headers[i][j + 1] = (uint8_t)(length >> (8 * j));
            parts[count] = headers[i]; lengths[count++] = 9;
        }
        parts[count] = bodies[i]; lengths[count++] = sizes[i];
#ifdef WL_MBEDTLS_ENABLED
        wl_columnar_expr_digest_checkpoint(control, 37, i, false);
#endif
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    return wl_columnar_expr_digest_segments(WL_PLAN_EXPR_ARITH_UUID5, parts,
               lengths, count, true, NULL, value, failed, control);
}

int
wl_columnar_expr_uuid_rfc_run(const uint8_t namespace_uuid[16],
    const uint8_t *name, size_t name_length, char text[37], bool *failed,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    const uint8_t *parts[2] = {namespace_uuid, name};
    const size_t lengths[2] = {16, name_length};
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    return wl_columnar_expr_digest_segments(WL_PLAN_EXPR_ARITH_UUID5, parts,
               lengths, 2, true, text, NULL, failed, control);
}
