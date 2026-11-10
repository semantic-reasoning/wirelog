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
