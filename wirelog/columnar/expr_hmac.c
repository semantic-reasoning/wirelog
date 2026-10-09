/* Scoped controlled HMAC-SHA256. Copyright (C) CleverPlant.
 * Licensed under LGPL-3.0. */
#include "internal.h"
#include <errno.h>
#include <string.h>
#ifdef WL_MBEDTLS_ENABLED
#include <psa/crypto.h>
#endif
#ifdef WL_COLUMNAR_HMAC_MBED_VERSION_GETTER
#include <mbedtls/version.h>
#endif
#ifdef WL_COLUMNAR_HMAC_TF_VERSION_GETTER
#include <tf-psa-crypto/version.h>
#endif
#ifdef WL_SESSION_TEST_HOOKS
bool (*wl_columnar_expr_hmac_test_eligible)(void);
void (*wl_columnar_expr_hmac_test_checkpoint)(wl_evaluation_control_t *, int,
    uint64_t, bool);
int64_t (*wl_columnar_expr_hmac_test_provider_result)(int, int64_t, size_t *);
void (*wl_columnar_expr_hmac_test_normalized_key)(const uint8_t *, size_t);
void (*wl_columnar_expr_hmac_test_mac)(const uint8_t *, size_t);
#endif
int
wl_columnar_expr_hmac_eligible(wl_evaluation_control_t *control)
{
#ifndef WL_MBEDTLS_ENABLED
    (void)control; return 0;
#else
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_hmac_test_eligible &&
        !wl_columnar_expr_hmac_test_eligible()) return ENOTSUP;
#endif
#ifndef WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED
    (void)control; return ENOTSUP;
#else
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
#ifdef WL_COLUMNAR_HMAC_MBED_VERSION_GETTER
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    unsigned int mbed_version = mbedtls_version_get_number();
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (mbed_version != MBEDTLS_VERSION_NUMBER) return ENOTSUP;
#endif
#ifdef WL_COLUMNAR_HMAC_TF_VERSION_GETTER
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    unsigned int tf_version = tf_psa_crypto_version_get_number();
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (tf_version != TF_PSA_CRYPTO_VERSION_NUMBER) return ENOTSUP;
#endif
    return 0;
#endif
#endif
}
#if defined(WL_MBEDTLS_ENABLED) && \
    defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
static void
wl_columnar_expr_hmac_checkpoint(wl_evaluation_control_t *control, int stage,
    uint64_t progress, bool failed)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_hmac_test_checkpoint)
        wl_columnar_expr_hmac_test_checkpoint(control, stage, progress, failed);
#else
    (void)control; (void)stage; (void)progress; (void)failed;
#endif
}
static psa_status_t
wl_columnar_expr_hmac_report(int stage, psa_status_t actual, size_t *length)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_hmac_test_provider_result) {
        psa_status_t reported = (psa_status_t)
            wl_columnar_expr_hmac_test_provider_result(stage, actual, length);
        return actual != PSA_SUCCESS &&
               reported == PSA_SUCCESS ? actual : reported;
    }
#else
    (void)stage; (void)length;
#endif
    return actual;
}
#endif
int
wl_columnar_expr_hmac_run(const uint8_t *message, size_t message_length,
    const uint8_t *key, size_t key_length, int64_t *value, bool *failed,
    wl_evaluation_control_t *control)
{
#ifndef WL_MBEDTLS_ENABLED
    (void)message; (void)message_length; (void)key; (void)key_length;
    (void)value; (void)control; *failed = true; return 0;
#elif !defined(WL_COLUMNAR_HMAC_SOFTWARE_PROFILE_SUPPORTED)
    (void)message; (void)message_length; (void)key; (void)key_length;
    (void)value; (void)failed; (void)control;
    return ENOTSUP;
#else
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    uint8_t normalized[32], mac[32]; uint8_t empty = 0;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (key_length > 64) {
        bool semantic;
        wl_columnar_expr_hmac_checkpoint(control, 70, key_length, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        rc = wl_columnar_expr_digest_sha256_full(key, key_length, normalized,
                &semantic, control);
        if (rc) return rc;
        if (semantic) {
            *failed = true; return 0;
        }
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        key = normalized; key_length = 32;
        wl_columnar_expr_hmac_checkpoint(control, 71, 32, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    } else if (!key_length) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        key = &empty; key_length = 1;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_hmac_test_normalized_key)
        wl_columnar_expr_hmac_test_normalized_key(key, key_length);
#endif
    psa_mac_operation_t operation;
    for (size_t offset = 0; offset < sizeof(operation);) {
        size_t count = sizeof(operation) - offset;
        if (count > 128) count = 128;
        rc = wl_evaluation_control_charge(control, count + 8);
        if (rc) return rc;
        memset((unsigned char *)&operation + offset, 0, count);
        offset += count;
        wl_columnar_expr_hmac_checkpoint(control, 79, offset, false);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    /* Public default attributes require bounded metadata only; no key owner
     * exists until actual import succeeds. Operation all-zero is public idle. */
    _Static_assert(sizeof(psa_key_attributes_t) <= 128,
        "audited PSA key attributes require a bounded initializer");
    rc = wl_evaluation_control_charge(control,
            sizeof(psa_key_attributes_t) + 16);
    if (rc) return rc;
    psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
    psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
    psa_set_key_usage_flags(&attributes, PSA_KEY_USAGE_SIGN_MESSAGE);
    psa_set_key_algorithm(&attributes, PSA_ALG_HMAC(PSA_ALG_SHA_256));
    psa_key_id_t actual_key = 0;
    bool imported = false, attempted = false, terminated = false,
        semantic = false;
    size_t actual_length = 0;
    wl_columnar_expr_hmac_checkpoint(control, 80, key_length, false);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) {
        psa_reset_key_attributes(&attributes); return rc;
    }
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) {
        psa_reset_key_attributes(&attributes); return rc;
    }
    psa_status_t status = wl_columnar_expr_hmac_report(0, psa_crypto_init(),
            NULL);
    wl_columnar_expr_hmac_checkpoint(control, 72, 0, status != PSA_SUCCESS);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc || status != PSA_SUCCESS) {
        psa_reset_key_attributes(&attributes);
        if (!rc) *failed = true;
        return rc;
    }
    /* Fixed-input external primitive allowances, not provider hidden heap/time
     * measurements. All key imports here are normalized to at most64 bytes. */
    rc = wl_evaluation_control_charge(control, 128 + key_length);
    if (rc) {
        psa_reset_key_attributes(&attributes); return rc;
    }
    psa_status_t actual = psa_import_key(&attributes, key, key_length,
            &actual_key);
    imported = actual == PSA_SUCCESS;
    psa_reset_key_attributes(&attributes);
    status = wl_columnar_expr_hmac_report(1, actual, NULL);
    semantic = status != PSA_SUCCESS;
    if (!imported && actual_key != 0) rc = EIO;
    wl_columnar_expr_hmac_checkpoint(control, 73, key_length, semantic);
    if (!rc) rc = wl_evaluation_control_charge(control, 0);
    if (rc || semantic) goto cleanup;
    rc = wl_evaluation_control_charge(control, 128);
    if (rc) goto cleanup;
    attempted = true;
    status = wl_columnar_expr_hmac_report(2,
            psa_mac_sign_setup(&operation, actual_key,
            PSA_ALG_HMAC(PSA_ALG_SHA_256)), NULL);
    semantic = status != PSA_SUCCESS;
    wl_columnar_expr_hmac_checkpoint(control, 74, 0, semantic);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc || semantic) goto cleanup;
    for (size_t offset = 0; offset < message_length;) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) goto cleanup;
        size_t count = message_length - offset;
        if (count > 64) count = 64;
        const uint8_t *part = message + offset;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto cleanup;
        rc = wl_evaluation_control_charge(control, 128 + count);
        if (rc) goto cleanup;
        status = wl_columnar_expr_hmac_report(3,
                psa_mac_update(&operation, part, count), NULL);
        semantic = status != PSA_SUCCESS;
        wl_columnar_expr_hmac_checkpoint(control, 75, offset + count, semantic);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc || semantic) goto cleanup;
        rc = wl_evaluation_control_charge(control, 4);
        if (rc) goto cleanup;
        offset += count;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) goto cleanup;
    }
    rc = wl_evaluation_control_charge(control, 160);
    if (rc) goto cleanup;
    actual = psa_mac_sign_finish(&operation, mac, 32, &actual_length);
    terminated = actual == PSA_SUCCESS;
    status = wl_columnar_expr_hmac_report(4, actual, &actual_length);
    semantic = status != PSA_SUCCESS || actual_length != 32;
    wl_columnar_expr_hmac_checkpoint(control, 76, actual_length, semantic);
    rc = wl_evaluation_control_charge(control, 0);
cleanup:
    if (attempted && !terminated) {
        status = wl_columnar_expr_hmac_report(5, psa_mac_abort(&operation),
                NULL);
        if (status != PSA_SUCCESS && rc <= 0) rc = EIO;
    }
    if (imported) {
        /* Exactly one mandatory attempt AFTER MAC termination, even after an
         * abort error. Genuine failed destroy admits unknown external state:
         * no retained-ID retry/quarantine or provider-baseline claim. */
        status = wl_columnar_expr_hmac_report(6, psa_destroy_key(actual_key),
                NULL);
        if (status != PSA_SUCCESS && rc <= 0) rc = EIO;
    }
    if (rc) return rc;
    if (semantic) {
        *failed = true; return 0;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_hmac_test_mac) wl_columnar_expr_hmac_test_mac(mac, 32);
#endif
    wl_columnar_expr_hmac_checkpoint(control, 77, 32, false);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    int64_t pending; bool fold_failed;
    rc = wl_columnar_expr_hash_run(mac, 32, &pending, &fold_failed, control);
    if (rc) return rc;
    wl_columnar_expr_hmac_checkpoint(control, 78, 32, fold_failed);
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    *failed = fold_failed;
    if (!fold_failed) *value = pending;
    return 0;
#endif
}
