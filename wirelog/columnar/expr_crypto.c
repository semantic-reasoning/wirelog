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

#ifdef WL_MBEDTLS_ENABLED
#ifdef WL_SESSION_TEST_HOOKS
psa_status_t (*wl_columnar_expr_crypto_uuid_test_init)(void);
psa_status_t (*wl_columnar_expr_crypto_uuid_test_random)(uint8_t *, size_t);
psa_status_t (*wl_columnar_expr_crypto_uuid_test_setup)(psa_hash_operation_t *,
    psa_algorithm_t);
psa_status_t (*wl_columnar_expr_crypto_uuid_test_update)(psa_hash_operation_t *,
    const uint8_t *, size_t);
psa_status_t (*wl_columnar_expr_crypto_uuid_test_finish)(psa_hash_operation_t *,
    uint8_t *, size_t, size_t *);
psa_status_t (*wl_columnar_expr_crypto_uuid_test_abort)(psa_hash_operation_t *);
#endif
static psa_status_t
wl_columnar_expr_crypto_uuid_init(void)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_crypto_uuid_test_init)
        return wl_columnar_expr_crypto_uuid_test_init();
#endif
    return psa_crypto_init();
}
static psa_status_t
wl_columnar_expr_crypto_uuid_update(psa_hash_operation_t *operation,
    const uint8_t *data, size_t length)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_crypto_uuid_test_update)
        return wl_columnar_expr_crypto_uuid_test_update(operation, data,
                   length);
#endif
    return psa_hash_update(operation, data, length);
}
static void
wl_columnar_expr_crypto_uuid_abort(psa_hash_operation_t *operation,
    bool *pending, bool *attempted)
{
    if (!*pending || *attempted) return;
    *pending = false; *attempted = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_crypto_uuid_test_abort) {
        (void)wl_columnar_expr_crypto_uuid_test_abort(operation); return;
    }
#endif
    (void)psa_hash_abort(operation);
}
/* Exactly the original9-byte header and one whole header update. Each
 * fixed store has its own packet; no provider capability-changing chunking. */
static int
wl_columnar_expr_crypto_uuid_frame(psa_hash_operation_t *operation,
    uint8_t tag, size_t length, unsigned which, bool *valid,
    wl_evaluation_control_t *control, uint64_t id)
{
    uint8_t header[9]; uint64_t n = (uint64_t)length;
    for (unsigned i = 0; i < 9; ++i) {
        int rc = wl_columnar_expr_crypto_region(control, id, 306, which*16+i);
        if (rc) return rc;
        header[i] = i ? (uint8_t)(n >> (8*(i-1))) : tag;
    }
    int rc = wl_columnar_expr_crypto_region(control, id, 307, which);
    if (rc) return rc;
    psa_status_t status = wl_columnar_expr_crypto_uuid_update(operation,
            header, sizeof(header));
    rc = wl_columnar_expr_crypto_poll(control, id, 308, which);
    if (rc) return rc;
    *valid = status == PSA_SUCCESS; return 0;
}
#endif

int
wl_columnar_expr_crypto_uuid4_attempt_controlled(int64_t *value, bool *failed,
    wl_evaluation_control_t *control, uint64_t id)
{
    if (!value || !failed) return EINVAL;
    int rc = wl_columnar_expr_crypto_region(control, id, 320, 0);
    if (rc) return rc;
    int64_t result = 0; bool semantic = true;
#ifdef WL_MBEDTLS_ENABLED
    uint8_t uuid[16];
    rc = wl_columnar_expr_crypto_region(control, id, 321, 0);
    if (rc) return rc;
    psa_status_t status = wl_columnar_expr_crypto_uuid_init();
    rc = wl_columnar_expr_crypto_poll(control, id, 322, 0);
    if (rc) return rc;
    if (status == PSA_SUCCESS) {
        rc = wl_columnar_expr_crypto_region(control, id, 323, 0);
        if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
        status = wl_columnar_expr_crypto_uuid_test_random
            ? wl_columnar_expr_crypto_uuid_test_random(uuid, sizeof(uuid))
            : psa_generate_random(uuid, sizeof(uuid));
#else
        status = psa_generate_random(uuid, sizeof(uuid));
#endif
        rc = wl_columnar_expr_crypto_poll(control, id, 324, 0);
        if (rc) return rc;
        semantic = status != PSA_SUCCESS;
    }
#endif
    rc = wl_columnar_expr_crypto_region(control, id, 325, 0);
    if (rc) return rc;
#ifdef WL_MBEDTLS_ENABLED
    if (!semantic) {
        uuid[6] = (uint8_t)((uuid[6] & 0x0f) | 0x40);
        uuid[8] = (uint8_t)((uuid[8] & 0x3f) | 0x80);
        memcpy(&result, uuid, sizeof(result));
    }
#endif
    *value = result; *failed = semantic; return 0;
}

/* Original SHA1 pair route. With I=ceil(sizeof(operation)/8), native(9+I)T
 * and framed(29+I)T; first abort caller is prepaid before setup. Opaque init/setup/update/finish/abort bodies remain
 * synchronous boundaries. Fixed caller regions fit G9+C13+T29+157=208<=224;
 * separately metered state-copy regions have the explicit211 bound below.
 * A correct finish consumes cleanup authority BEFORE the postcall hook;
 * wrong-length success follows raw's idempotent abort path. */
int
wl_columnar_expr_crypto_uuid_pair_attempt_controlled(bool framed,
    uint8_t first_tag, const uint8_t *first, size_t first_length,
    uint8_t second_tag, const uint8_t *second, size_t second_length,
    uint8_t *output, bool *failed, wl_evaluation_control_t *control,
    uint64_t id)
{
    if ((!first && first_length) || (!second && second_length)
        || !output || !failed) return EINVAL;
    int rc = wl_columnar_expr_crypto_region(control, id, 300, 0);
    if (rc) return rc;
    bool semantic = true;
#ifdef WL_MBEDTLS_ENABLED
    rc = wl_columnar_expr_crypto_region(control, id, 301, 0);
    if (rc) return rc;
    static const psa_hash_operation_t initial_operation =
        PSA_HASH_OPERATION_INIT;
    psa_hash_operation_t operation;
    bool pending = false, abort_attempted = false, frame_valid = false;
    uint8_t digest[20] = {0}; size_t actual = 0;
    const unsigned char *initial_bytes =
        (const unsigned char *)&initial_operation;
    unsigned char *operation_bytes = (unsigned char *)&operation;
    size_t offset = 0;
    /* Exact inactive provider representation, never an active-operation copy.
     * At most8 bytes:12 actions/byte96 + guard51 + full fixed suffix64=211.
     * No API sees a partially initialized object, so early stops own no abort. */
    while (offset < sizeof(operation)) {
        rc = wl_columnar_expr_crypto_region(control, id, 318, offset);
        if (rc) return rc;
        size_t count = sizeof(operation) - offset;
        if (count > 8) count = 8;
        for (size_t byte = 0; byte < count; ++byte)
            operation_bytes[offset + byte] = initial_bytes[offset + byte];
        offset += count;
    }
    rc = wl_columnar_expr_crypto_region(control, id, 302, 0);
    if (rc) return rc;
    rc = wl_columnar_expr_crypto_region(control, id, 303, 0);
    if (rc) return rc;
    psa_status_t status = wl_columnar_expr_crypto_uuid_init();
    rc = wl_columnar_expr_crypto_poll(control, id, 304, 0);
    if (rc) goto control_failure;
    if (status != PSA_SUCCESS) goto semantic_failure;
    rc = wl_columnar_expr_crypto_region(control, id, 305, 0);
    if (rc) goto control_failure;
    pending = true; /* Failed setup also owns raw's abort attempt. */
#ifdef WL_SESSION_TEST_HOOKS
    status = wl_columnar_expr_crypto_uuid_test_setup
        ? wl_columnar_expr_crypto_uuid_test_setup(&operation, PSA_ALG_SHA_1)
        : psa_hash_setup(&operation, PSA_ALG_SHA_1);
#else
    status = psa_hash_setup(&operation, PSA_ALG_SHA_1);
#endif
    rc = wl_columnar_expr_crypto_poll(control, id, 317, 0);
    if (rc) goto control_failure;
    if (status != PSA_SUCCESS) goto semantic_failure;
    if (framed) {
        rc = wl_columnar_expr_crypto_uuid_frame(&operation, first_tag,
                first_length, 0, &frame_valid, control, id);
        if (rc) goto control_failure;
        if (!frame_valid) goto semantic_failure;
    }
    rc = wl_columnar_expr_crypto_region(control, id, 309, first_length);
    if (rc) goto control_failure;
    status = wl_columnar_expr_crypto_uuid_update(&operation, first,
            first_length);
    rc = wl_columnar_expr_crypto_poll(control, id, 310, first_length);
    if (rc) goto control_failure;
    if (status != PSA_SUCCESS) goto semantic_failure;
    if (framed) {
        rc = wl_columnar_expr_crypto_uuid_frame(&operation, second_tag,
                second_length, 1, &frame_valid, control, id);
        if (rc) goto control_failure;
        if (!frame_valid) goto semantic_failure;
    }
    rc = wl_columnar_expr_crypto_region(control, id, 311, second_length);
    if (rc) goto control_failure;
    status = wl_columnar_expr_crypto_uuid_update(&operation, second,
            second_length);
    rc = wl_columnar_expr_crypto_poll(control, id, 312, second_length);
    if (rc) goto control_failure;
    if (status != PSA_SUCCESS) goto semantic_failure;
    rc = wl_columnar_expr_crypto_region(control, id, 313, 0);
    if (rc) goto control_failure;
#ifdef WL_SESSION_TEST_HOOKS
    status = wl_columnar_expr_crypto_uuid_test_finish
        ? wl_columnar_expr_crypto_uuid_test_finish(&operation, digest,
            sizeof(digest), &actual)
        : psa_hash_finish(&operation, digest, sizeof(digest), &actual);
#else
    status = psa_hash_finish(&operation, digest, sizeof(digest), &actual);
#endif
    if (status == PSA_SUCCESS && actual == sizeof(digest)) pending = false;
    rc = wl_columnar_expr_crypto_poll(control, id, 314, actual);
    if (rc) goto control_failure;
    if (status != PSA_SUCCESS ||
        actual != sizeof(digest)) goto semantic_failure;
    semantic = false; goto complete;
semantic_failure:
    wl_columnar_expr_crypto_uuid_abort(&operation, &pending, &abort_attempted);
    goto complete;
control_failure:
    wl_columnar_expr_crypto_uuid_abort(&operation, &pending, &abort_attempted);
    return rc;
complete:
#else
    (void)framed; (void)first_tag; (void)second_tag;
#endif
    rc = wl_columnar_expr_crypto_region(control, id, 315, 0);
    if (rc) return rc;
#ifdef WL_MBEDTLS_ENABLED
    if (!semantic) {
        memcpy(output, digest, 20);
    } else {
        memset(output, 0, 20);
    }
#else
    memset(output, 0, 20);
#endif
    *failed = semantic; return 0;
}
