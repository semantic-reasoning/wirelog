/* Original PSA digest cooperation boundary (#2299).
 * Copyright (C) CleverPlant; licensed under LGPL-3.0.
 * Preserve compute-provider acceptance/refusal exactly. Provider init/compute
 * bodies and provider-owned allocation are narrow synchronous boundaries;
 * compute may traverse its complete variable-size input without cancellation.
 * Engine traversal is admitted separately by the caller. No raw evaluator,
 * multipart fallback, provider normalization or operation state is used.
 * Fixed engine regions reuse G9+C13+T29 and at most157 caller/branch actions:
 * 208 <=224 <=quantum256. Bootstrap selection/validation is at most64 fixed
 * actions. Hooks/test shims are instrumentation outside the production bound. */
#include "internal.h"
#include <errno.h>
#ifdef WL_MBEDTLS_ENABLED
#include <psa/crypto.h>
#endif

typedef char wl_columnar_expr_crypto_tariff_fits_quantum[
        (UINT64_C(208) <= UINT64_C(224) &&
        UINT64_C(224) <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

#ifdef WL_SESSION_TEST_HOOKS
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
#ifdef WL_MBEDTLS_ENABLED
psa_status_t (*wl_columnar_expr_crypto_test_init)(void);
psa_status_t (*wl_columnar_expr_crypto_test_compute)(psa_algorithm_t,
    const uint8_t *, size_t, uint8_t *, size_t, size_t *);
#endif
#endif

static int
wl_columnar_expr_crypto_poll(wl_evaluation_control_t *control, uint64_t id,
    unsigned phase, size_t progress)
{
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_test_checkpoint)
        wl_columnar_expr_test_checkpoint(control, phase, progress, false);
#else
    (void)phase; (void)progress;
#endif
    return wl_evaluation_control_charge_attempt(control, id, 0);
}

static int
wl_columnar_expr_crypto_region(wl_evaluation_control_t *control, uint64_t id,
    unsigned phase, size_t progress)
{
    if (!control || !id ||
        wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(control, id, 224);
    return rc ? rc : wl_columnar_expr_crypto_poll(control, id, phase, progress);
}

int
wl_columnar_expr_crypto_digest_attempt_controlled(uint8_t tag,
    const uint8_t *bytes, size_t length, int64_t *value, bool *failed,
    wl_evaluation_control_t *control, uint64_t id)
{
    if ((!bytes && length) || !value || !failed) return EINVAL;
#ifdef WL_MBEDTLS_ENABLED
    psa_algorithm_t algorithm;
    size_t expected;
    switch ((wl_plan_expr_tag_t)tag) {
    case WL_PLAN_EXPR_ARITH_MD5: case WL_PLAN_EXPR_ARITH_MD5_S:
        algorithm = PSA_ALG_MD5; expected = 16;
        break;
    case WL_PLAN_EXPR_ARITH_SHA1: case WL_PLAN_EXPR_ARITH_SHA1_S:
        algorithm = PSA_ALG_SHA_1; expected = 20;
        break;
    case WL_PLAN_EXPR_ARITH_SHA256: case WL_PLAN_EXPR_ARITH_SHA256_S:
        algorithm = PSA_ALG_SHA_256; expected = 32;
        break;
    case WL_PLAN_EXPR_ARITH_SHA512: case WL_PLAN_EXPR_ARITH_SHA512_S:
        algorithm = PSA_ALG_SHA_512; expected = 64;
        break;
    default: return EINVAL;
    }
#else
    /* Disabled tags share validation only; no provider selection occurs. */
    switch ((wl_plan_expr_tag_t)tag) {
    case WL_PLAN_EXPR_ARITH_MD5: case WL_PLAN_EXPR_ARITH_MD5_S:
    case WL_PLAN_EXPR_ARITH_SHA1: case WL_PLAN_EXPR_ARITH_SHA1_S:
    case WL_PLAN_EXPR_ARITH_SHA256: case WL_PLAN_EXPR_ARITH_SHA256_S:
    case WL_PLAN_EXPR_ARITH_SHA512: case WL_PLAN_EXPR_ARITH_SHA512_S:
        break;
    default: return EINVAL;
    }
#endif
    int rc = wl_columnar_expr_crypto_region(control, id, 200, length);
    if (rc) return rc;
    int64_t result = 0;
    bool semantic = true;
#ifdef WL_MBEDTLS_ENABLED
    uint8_t digest[64];
    size_t actual = 0;
    rc = wl_columnar_expr_crypto_region(control, id, 201, 0);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    psa_status_t status = wl_columnar_expr_crypto_test_init
        ? wl_columnar_expr_crypto_test_init() : psa_crypto_init();
#else
    psa_status_t status = psa_crypto_init();
#endif
    rc = wl_columnar_expr_crypto_poll(control, id, 202, 0);
    if (rc) return rc;
    if (status != PSA_SUCCESS) goto done;
    rc = wl_columnar_expr_crypto_region(control, id, 203, length);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    status = wl_columnar_expr_crypto_test_compute
        ? wl_columnar_expr_crypto_test_compute(algorithm, bytes, length,
            digest, expected, &actual)
        : psa_hash_compute(algorithm, bytes, length, digest, expected, &actual);
#else
    status = psa_hash_compute(algorithm, bytes, length, digest, expected,
            &actual);
#endif
    rc = wl_columnar_expr_crypto_poll(control, id, 204, actual);
    if (rc) return rc;
    if (status != PSA_SUCCESS || actual != expected) goto done;
    rc = wl_columnar_expr_hash_bytes_attempt_controlled(digest, expected,
            &result, control, id);
    if (rc) return rc;
    semantic = false;
done:
#endif
    rc = wl_columnar_expr_crypto_region(control, id, 205, length);
    if (rc) return rc;
    *value = result; *failed = semantic;
    return 0;
}

#ifdef WL_MBEDTLS_ENABLED
#ifdef WL_SESSION_TEST_HOOKS
psa_status_t (*wl_columnar_expr_crypto_hmac_test_init)(void);
psa_status_t (*wl_columnar_expr_crypto_hmac_test_import)(
    const psa_key_attributes_t *, const uint8_t *, size_t, psa_key_id_t *);
void (*wl_columnar_expr_crypto_hmac_test_reset)(psa_key_attributes_t *);
psa_status_t (*wl_columnar_expr_crypto_hmac_test_mac)(psa_key_id_t,
    psa_algorithm_t, const uint8_t *, size_t, uint8_t *, size_t, size_t *);
psa_status_t (*wl_columnar_expr_crypto_hmac_test_destroy)(psa_key_id_t);
#endif

static void
wl_columnar_expr_crypto_hmac_reset(psa_key_attributes_t *attributes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_crypto_hmac_test_reset) {
        wl_columnar_expr_crypto_hmac_test_reset(attributes);
        return;
    }
#endif
    psa_reset_key_attributes(attributes);
}

/* Consume real local authority before the original single destroy attempt.
 * Failed destroy is not evidence that another attempt is safe. This fixed
 * caller is prepaid before attributes/import; provider interiors are opaque.
 * Provider fault wrappers are instrumentation, never control checkpoints. */
static psa_status_t
wl_columnar_expr_crypto_hmac_destroy(psa_key_id_t key, bool *owned,
    bool *attempted)
{
    if (!*owned || *attempted) return PSA_ERROR_BAD_STATE;
    *owned = false; *attempted = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_crypto_hmac_test_destroy)
        return wl_columnar_expr_crypto_hmac_test_destroy(key);
#endif
    return psa_destroy_key(key);
}
#endif

/* Eight positive fixed packets plus the existing32-byte hash fold:13T.
 * Init/import/MAC/reset/destroy provider bodies are explicit synchronous
 * boundaries. Fixed G9+C13+T29+caller<=157 gives208<=T224<=quantum256.
 * FIRST_CLEANUP_PREPAY precedes attributes, so an interrupted live key/reset
 * never borrows a future forward admission or the successor's controller. */
int
wl_columnar_expr_crypto_hmac_attempt_controlled(const uint8_t *message,
    size_t message_length, const uint8_t *key, size_t key_length,
    int64_t *value, bool *failed, wl_evaluation_control_t *control, uint64_t id)
{
    if ((!message && message_length) || (!key && key_length)
        || !value || !failed) return EINVAL;
    int rc = wl_columnar_expr_crypto_region(control, id, 230, message_length);
    if (rc) return rc;
    int64_t result = 0;
    bool semantic = true;
#ifdef WL_MBEDTLS_ENABLED
    rc = wl_columnar_expr_crypto_region(control, id, 231, 0);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    psa_status_t status = wl_columnar_expr_crypto_hmac_test_init
        ? wl_columnar_expr_crypto_hmac_test_init() : psa_crypto_init();
#else
    psa_status_t status = psa_crypto_init();
#endif
    rc = wl_columnar_expr_crypto_poll(control, id, 232, 0);
    if (rc) return rc;
    if (status != PSA_SUCCESS) goto done;
    rc = wl_columnar_expr_crypto_region(control, id, 233, 0);
    if (rc) return rc;
    rc = wl_columnar_expr_crypto_region(control, id, 234, key_length);
    if (rc) return rc;
    psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
    bool attributes_live = true, key_owned = false, destroy_attempted = false;
    psa_key_id_t key_id = 0;
    psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
    psa_set_key_usage_flags(&attributes, PSA_KEY_USAGE_SIGN_MESSAGE);
    psa_set_key_algorithm(&attributes, PSA_ALG_HMAC(PSA_ALG_SHA_256));
    static const uint8_t empty_key[1] = {0};
    if (!key_length) {
        key = empty_key; key_length = sizeof(empty_key);
    }
    rc = wl_columnar_expr_crypto_region(control, id, 235, key_length);
    if (rc) goto cleanup;
#ifdef WL_SESSION_TEST_HOOKS
    status = wl_columnar_expr_crypto_hmac_test_import
        ? wl_columnar_expr_crypto_hmac_test_import(&attributes, key,
            key_length, &key_id)
        : psa_import_key(&attributes, key, key_length, &key_id);
#else
    status = psa_import_key(&attributes, key, key_length, &key_id);
#endif
    key_owned = status == PSA_SUCCESS;
    wl_columnar_expr_crypto_hmac_reset(&attributes);
    attributes_live = false;
    rc = wl_columnar_expr_crypto_poll(control, id, 236, 0);
    if (rc) goto cleanup;
    if (status != PSA_SUCCESS) goto done;
    rc = wl_columnar_expr_crypto_region(control, id, 237, message_length);
    if (rc) goto cleanup;
    uint8_t digest[32]; size_t actual = 0;
#ifdef WL_SESSION_TEST_HOOKS
    status = wl_columnar_expr_crypto_hmac_test_mac
        ? wl_columnar_expr_crypto_hmac_test_mac(key_id,
            PSA_ALG_HMAC(PSA_ALG_SHA_256), message, message_length,
            digest, sizeof(digest), &actual)
        : psa_mac_compute(key_id, PSA_ALG_HMAC(PSA_ALG_SHA_256),
            message, message_length, digest, sizeof(digest), &actual);
#else
    status = psa_mac_compute(key_id, PSA_ALG_HMAC(PSA_ALG_SHA_256),
            message, message_length, digest, sizeof(digest), &actual);
#endif
    rc = wl_columnar_expr_crypto_poll(control, id, 238, actual);
    if (rc) goto cleanup;
    bool mac_valid = status == PSA_SUCCESS && actual == sizeof(digest);
    rc = wl_columnar_expr_crypto_region(control, id, 239, 0);
    if (rc) goto cleanup;
    status = wl_columnar_expr_crypto_hmac_destroy(key_id, &key_owned,
            &destroy_attempted);
    rc = wl_columnar_expr_crypto_poll(control, id, 240, 0);
    if (rc) goto cleanup;
    if (!mac_valid || status != PSA_SUCCESS) goto done;
    rc = wl_columnar_expr_hash_bytes_attempt_controlled(digest, sizeof(digest),
            &result, control, id);
    if (rc) goto cleanup;
    semantic = false;
    goto done;
cleanup:
    if (attributes_live) wl_columnar_expr_crypto_hmac_reset(&attributes);
    if (key_owned)
        (void)wl_columnar_expr_crypto_hmac_destroy(key_id, &key_owned,
            &destroy_attempted);
    return rc;
done:
#else
    (void)key_length;
#endif
    rc = wl_columnar_expr_crypto_region(control, id, 241, message_length);
    if (rc) return rc;
    *value = result; *failed = semantic;
    return 0;
}
