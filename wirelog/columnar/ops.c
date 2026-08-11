/*
 * columnar/ops.c - wirelog Columnar Backend Operator Implementations
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * All col_op_* operator functions and supporting helpers extracted from
 * backend/columnar_nanoarrow.c for modular compilation.
 */

#define _GNU_SOURCE

/* Minimum K to use parallel K-fusion dispatch.  For K below this threshold,
 * thread-dispatch + per-worker setup overhead (arena alloc, delta pool,
 * synchronization) exceeds the parallelisation benefit.
 * Measured: DDISASM K=3 is 14% slower with 8-worker parallel than sequential.
 * K < WL_KFUSION_MIN_PARALLEL_K falls back to sequential execution. */
#define WL_KFUSION_MIN_PARALLEL_K 4

/* Best-effort match-pair cache for parallel keyed diff joins.  The cache is
 * scratch memory outside the final output relation, so keep it bounded and
 * fall back to the old fill traversal when a worker reaches the cap. */
#define WL_JOIN_PAIR_CACHE_MAX_BYTES (256ULL * 1024ULL * 1024ULL)
#define WL_JOIN_PAIR_CACHE_MIN_LEFT_ROWS 100000u

#if defined(_MSC_VER)
#define WL_OPS_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define WL_OPS_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define WL_OPS_ALWAYS_INLINE inline
#endif

#include "columnar/internal.h"
#include "columnar/lftj.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"
#include "../crc32.h"
#include "../intern.h"
#include "../string_ops.h"

#include <xxhash.h>

#ifdef WL_MBEDTLS_ENABLED
#include <psa/crypto.h>
#endif

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/* ======================================================================== */
/* Postfix Filter Expression Evaluator                                       */
/* ======================================================================== */

typedef struct {
    int64_t vals[COL_FILTER_STACK];
    uint32_t top;
} filt_stack_t;

#ifdef WL_MBEDTLS_ENABLED
static int
wl_columnar_ops_psa_init(void)
{
    return psa_crypto_init() == PSA_SUCCESS ? 0 : -1;
}

static int
wl_columnar_ops_psa_hash_bytes(psa_algorithm_t alg, const void *data,
    size_t len, unsigned char *digest, size_t digest_len)
{
    size_t actual_len = 0;
    if (wl_columnar_ops_psa_init() != 0)
        return -1;
    if (psa_hash_compute(alg, (const unsigned char *)data, len,
        digest, digest_len, &actual_len) != PSA_SUCCESS)
        return -1;
    return actual_len == digest_len ? 0 : -1;
}

/*
 * Domain tags for framed digest operands (Issue #968).  One byte, chosen to
 * be legible in a hexdump of the digest input.
 */
#define WL_DIGEST_TAG_SYMBOL ((uint8_t)'S')
#define WL_DIGEST_TAG_INT64  ((uint8_t)'I')

/**
 * Emit one framed operand's header: its domain tag, then its length as a
 * little-endian uint64.
 */
static int
wl_columnar_ops_psa_hash_frame(psa_hash_operation_t *op, uint8_t tag,
    size_t len)
{
    uint8_t hdr[1 + 8];
    uint64_t n = (uint64_t)len;

    hdr[0] = tag;
    for (unsigned i = 0; i < 8; i++)
        hdr[1 + i] = (uint8_t)(n >> (8 * i));
    return psa_hash_update(op, hdr, sizeof(hdr)) == PSA_SUCCESS ? 0 : -1;
}

/**
 * Digest @first followed by @second.  uuid5()'s only caller.
 *
 * When @framed, each operand is preceded by a one-byte domain tag and its
 * length as a little-endian uint64, so the encoding is injective over
 * (type, bytes) pairs: the tag separates a symbol from an int64 whose
 * little-endian bytes happen to spell it, and the length fixes the split
 * point.  A bare concatenation promises neither once the operands are
 * variable-length: uuid5("ab", "c") and uuid5("a", "bc") would hash
 * identical bytes, and so would uuid5("abcdefgh", x) and uuid5(<the int64
 * with those bytes>, x).  @first_tag and @second_tag must describe the bytes
 * actually passed, not the opcode's declared operand types -- see
 * filt_bytes_t::is_symbol.
 *
 * Injective is not collision-free.  uuid5() returns digest[0..7] with a
 * version nibble forced, so it has at most 2^60 distinct outputs however
 * unambiguous its input encoding is; see docs/SECURITY_MODEL.md.
 *
 * @framed is false only for WL_PLAN_EXPR_ARITH_UUID5, the int64-only opcode,
 * whose two operands are 8 bytes each.  The split point there is fixed and
 * both operands are int64s, so there is nothing to disambiguate -- and
 * leaving its bytes alone is what lets an out-of-tree decoder that already
 * implements 0x2A keep computing the same answer.  Its input is always
 * exactly 16 bytes, so the framed encoding must never be 16 bytes: the two
 * headers alone are 18, which is why uuid5("", "") no longer collides with
 * uuid5(0, 0) the way a bare 8-byte length prefix left it doing.
 *
 * Reproducible outside wirelog:
 *
 *     frame = lambda t, b: t + pack('<Q', len(b)) + b
 *     buf   = frame(b'S', ns) + frame(b'S', name)
 *     d     = bytearray(sha1(buf).digest())
 */
static int
wl_columnar_ops_psa_hash_pair(psa_algorithm_t alg, bool framed,
    uint8_t first_tag, const void *first, size_t first_len,
    uint8_t second_tag, const void *second, size_t second_len,
    unsigned char *digest, size_t digest_len)
{
    psa_hash_operation_t op = PSA_HASH_OPERATION_INIT;
    size_t actual_len = 0;
    int ret = -1;

    if (wl_columnar_ops_psa_init() != 0)
        return -1;
    if (psa_hash_setup(&op, alg) != PSA_SUCCESS)
        goto out;
    if (framed
        && wl_columnar_ops_psa_hash_frame(&op, first_tag, first_len) != 0)
        goto out;
    if (psa_hash_update(&op, (const unsigned char *)first, first_len)
        != PSA_SUCCESS)
        goto out;
    if (framed
        && wl_columnar_ops_psa_hash_frame(&op, second_tag, second_len) != 0)
        goto out;
    if (psa_hash_update(&op, (const unsigned char *)second, second_len)
        != PSA_SUCCESS)
        goto out;
    if (psa_hash_finish(&op, digest, digest_len, &actual_len) != PSA_SUCCESS)
        goto out;
    ret = actual_len == digest_len ? 0 : -1;
out:
    if (ret != 0)
        psa_hash_abort(&op);
    return ret;
}

#ifdef WL_MBEDTLS_ENABLED
static int
wl_columnar_ops_parse_uuid(const char *text, unsigned char uuid[16])
{
    static const unsigned char hyphen[4] = { 8, 13, 18, 23 };
    size_t pos = 0;

    if (!text || strlen(text) != 36)
        return -1;
    for (size_t i = 0; i < 36; i++) {
        if (i == hyphen[0] || i == hyphen[1]
            || i == hyphen[2] || i == hyphen[3]) {
            if (text[i] != '-')
                return -1;
            continue;
        }
        unsigned char c = (unsigned char)text[i];
        unsigned char nibble;
        if (c >= '0' && c <= '9')
            nibble = (unsigned char)(c - '0');
        else if (c >= 'a' && c <= 'f')
            nibble = (unsigned char)(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F')
            nibble = (unsigned char)(c - 'A' + 10);
        else
            return -1;
        if ((pos & 1u) == 0)
            uuid[pos / 2] = (unsigned char)(nibble << 4);
        else
            uuid[pos / 2] |= nibble;
        pos++;
    }
    return pos == 32 ? 0 : -1;
}

static int64_t
wl_columnar_ops_format_uuid(const unsigned char uuid[16], wl_intern_t *intern)
{
    static const char hex[] = "0123456789abcdef";
    char text[37];
    size_t out = 0;
    for (size_t i = 0; i < 16; i++) {
        text[out++] = hex[uuid[i] >> 4];
        text[out++] = hex[uuid[i] & 0x0f];
        if (i == 3 || i == 5 || i == 7 || i == 9)
            text[out++] = '-';
    }
    text[out] = '\0';
    return wl_intern_put(intern, text);
}
#endif

static int
wl_columnar_ops_psa_hmac_sha256(const void *msg, size_t msg_len,
    const void *key, size_t key_len, unsigned char *digest, size_t digest_len)
{
    psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
    psa_key_id_t key_id = 0;
    size_t actual_len = 0;
    psa_status_t status;
    int ret = -1;

    if (wl_columnar_ops_psa_init() != 0)
        return -1;

    psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
    psa_set_key_usage_flags(&attributes, PSA_KEY_USAGE_SIGN_MESSAGE);
    psa_set_key_algorithm(&attributes, PSA_ALG_HMAC(PSA_ALG_SHA_256));

    /*
     * A symbol key can be the empty string, which PSA refuses to import.
     * RFC 2104 zero-pads the key to the block size, so an empty key and a
     * one-byte zero key produce the same MAC -- verified against
     * hmac.new(b"", ...) == hmac.new(b"\0", ...).  Substituting it keeps
     * hmac_sha256(x, "") externally reproducible instead of failing the
     * query.  (Before #963 both operands were 8 bytes, so this could not
     * arise.)
     */
    static const unsigned char empty_key[1] = { 0 };
    if (key_len == 0) {
        key = empty_key;
        key_len = sizeof(empty_key);
    }

    status = psa_import_key(&attributes, (const unsigned char *)key,
            key_len, &key_id);
    psa_reset_key_attributes(&attributes);
    if (status != PSA_SUCCESS)
        return -1;

    status = psa_mac_compute(key_id, PSA_ALG_HMAC(PSA_ALG_SHA_256),
            (const unsigned char *)msg, msg_len, digest, digest_len,
            &actual_len);
    if (status == PSA_SUCCESS && actual_len == digest_len)
        ret = 0;

    if (psa_destroy_key(key_id) != PSA_SUCCESS)
        ret = -1;
    return ret;
}
#endif

/* ======================================================================== */
/* Digest operand bytes (Issue #963)                                        */
/* ======================================================================== */

/**
 * filt_bytes_t: the byte range a digest opcode consumes for one operand.
 *
 * @ptr either aliases the interned string or points into @inl, so the value
 * must outlive its use and must not be copied after being filled.
 *
 * @is_symbol reports which of those two happened.  It is not the caller's
 * @as_symbol: a symbol-typed operand whose reverse lookup fails falls back
 * to int64 bytes, and @is_symbol is then false.  Framed digests tag the
 * operand from this field, so that fallback is tagged for the bytes it
 * really emitted; tagging it from the opcode's declared type instead would
 * label an int64 a symbol and let it collide with the symbol spelling those
 * same eight bytes -- the very collision the tag exists to prevent.
 */
typedef struct {
    const uint8_t *ptr;
    size_t len;
    bool is_symbol;
    uint8_t inl[sizeof(int64_t)];
} filt_bytes_t;

/**
 * Fill @out with the bytes @v contributes to a digest.
 *
 * @as_symbol is set by the caller for the WL_PLAN_EXPR_ARITH_*_S opcode
 * family, which the plan generator emits when the operand's declared type
 * is `symbol`/`string`.  Then @v is an intern id and the digest covers the
 * string's own bytes -- strlen(@v) of them, with no NUL terminator, which
 * is what `xxhsum`, `crc32` and `sha256sum` see for the same input.
 * Otherwise @v is a value and the digest covers its 8-byte int64
 * representation, unchanged from before #963.
 *
 * The reverse lookup can fail even under an _S opcode, because `.decl`
 * types are not enforced: a column declared `symbol` may hold raw integers
 * that were never interned.  This falls back to the int64 representation
 * rather than failing the row.  The alternative -- failing -- is not a
 * dropped row in MAP position but an ERANGE that aborts the whole PROJECT
 * operator (col_op_project()), turning a query that runs today into
 * `error: execution failed` with no output.  The fallback instead gives
 * exactly what the integer opcode would have given for that value, which
 * for data that is genuinely integral is the right answer; nothing that is
 * genuinely a symbol reaches it.  The guarantee #963 establishes is
 * therefore only as strong as the `.decl`, and docs/SEMANTICS.md says so.
 */
static void
filt_digest_bytes(filt_bytes_t *out, int64_t v, bool as_symbol,
    wl_intern_t *intern)
{
    if (as_symbol) {
        const char *str = intern ? wl_intern_reverse(intern, v) : NULL;
        if (str) {
            out->ptr = (const uint8_t *)str;
            out->len = strlen(str);
            out->is_symbol = true;
            return;
        }
        WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_DEBUG,
            "digest operand %" PRId64 " is declared symbol but names no "
            "interned string: digesting its int64 representation", v);
    }
    memcpy(out->inl, &v, sizeof(v));
    out->ptr = out->inl;
    out->len = sizeof(v);
    out->is_symbol = false;
}

static inline void
filt_push(filt_stack_t *s, int64_t v)
{
    if (s->top < COL_FILTER_STACK)
        s->vals[s->top++] = v;
}

static inline int64_t
filt_pop(filt_stack_t *s)
{
    return s->top != 0 ? s->vals[--s->top] : 0;
}

static inline uint64_t
wl_columnar_ops_abs_u64(int64_t v)
{
    return v < 0 ? (uint64_t)0 - (uint64_t)v : (uint64_t)v;
}

/* Use compiler builtins where available; otherwise use explicit range checks. */
#if defined(__GNUC__) || defined(__clang__)
#define WL_COLUMNAR_OPS_HAVE_INT64_OVERFLOW_BUILTIN 1
#elif defined(__has_builtin)
#if __has_builtin(__builtin_add_overflow) \
    && __has_builtin(__builtin_sub_overflow) \
    && __has_builtin(__builtin_mul_overflow)
#define WL_COLUMNAR_OPS_HAVE_INT64_OVERFLOW_BUILTIN 1
#endif
#endif

/* Checked int64 arithmetic helpers shared by both slow and compiled evaluators. */
static int
wl_columnar_ops_checked_add_int64(int64_t a, int64_t b, int64_t *out)
{
#if defined(WL_COLUMNAR_OPS_HAVE_INT64_OVERFLOW_BUILTIN)
    return __builtin_add_overflow(a, b, out) ? ERANGE : 0;
#else
    if (b > 0) {
        if (a > INT64_MAX - b)
            return ERANGE;
    } else {
        if (a < INT64_MIN - b)
            return ERANGE;
    }
    *out = a + b;
    return 0;
#endif
}

static int
wl_columnar_ops_checked_sub_int64(int64_t a, int64_t b, int64_t *out)
{
#if defined(WL_COLUMNAR_OPS_HAVE_INT64_OVERFLOW_BUILTIN)
    return __builtin_sub_overflow(a, b, out) ? ERANGE : 0;
#else
    if (b > 0) {
        if (a < INT64_MIN + b)
            return ERANGE;
    } else {
        if (a > INT64_MAX + b)
            return ERANGE;
    }
    *out = a - b;
    return 0;
#endif
}

static int
wl_columnar_ops_checked_mul_int64(int64_t a, int64_t b, int64_t *out)
{
#if defined(WL_COLUMNAR_OPS_HAVE_INT64_OVERFLOW_BUILTIN)
    return __builtin_mul_overflow(a, b, out) ? ERANGE : 0;
#else
    if (a == 0 || b == 0) {
        *out = 0;
        return 0;
    }
    if ((a == -1 && b == INT64_MIN) || (b == -1 && a == INT64_MIN))
        return ERANGE;

    uint64_t ua = wl_columnar_ops_abs_u64(a);
    uint64_t ub = wl_columnar_ops_abs_u64(b);
    if ((a < 0) == (b < 0)) {
        if (ua > (uint64_t)INT64_MAX / ub)
            return ERANGE;
    } else {
        if (ua > ((uint64_t)INT64_MAX + 1ULL) / ub)
            return ERANGE;
    }
    *out = a * b;
    return 0;
#endif
}

static int
wl_columnar_ops_checked_div_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b == 0 || (a == INT64_MIN && b == -1))
        return ERANGE;
    *out = a / b;
    return 0;
}

static int
wl_columnar_ops_checked_mod_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b == 0 || (a == INT64_MIN && b == -1))
        return ERANGE;
    *out = a % b;
    return 0;
}

static int
wl_columnar_ops_checked_shl_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b < 0 || b >= 64)
        return ERANGE;
    *out = (int64_t)((uint64_t)a << (uint32_t)b);
    return 0;
}

static int
wl_columnar_ops_checked_shr_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b < 0 || b >= 64)
        return ERANGE;
    *out = a >> (uint32_t)b;
    return 0;
}

/*
 * col_eval_expr_run:
 * Core postfix expression evaluator. Runs the bytecode against a row and
 * stores the top-of-stack value in *out_val.
 * Returns 0 on success, non-zero on malformed bytecode.
 */
static int
col_eval_expr_run(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, int64_t *out_val, wl_intern_t *intern)
{
    filt_stack_t s;
    s.top = 0;

    uint32_t i = 0;
    while (i < size) {
        uint8_t tag = buf[i++];
        switch ((wl_plan_expr_tag_t)tag) {

        case WL_PLAN_EXPR_VAR: {
            if (i + 2 > size)
                goto bad;
            uint16_t nlen;
            memcpy(&nlen, buf + i, 2);
            i += 2;
            if (i + nlen > size)
                goto bad;
            /* variable name is "colN" */
            long col = 0;
            if (nlen > 3 && buf[i] == 'c' && buf[i + 1] == 'o'
                && buf[i + 2] == 'l') {
                char tmp[16] = { 0 };
                uint32_t cplen = (nlen - 3 < 15) ? nlen - 3 : 15;
                memcpy(tmp, buf + i + 3, cplen);
                col = strtol(tmp, NULL, 10);
            }
            i += nlen;
            filt_push(&s, (col >= 0 && (uint32_t)col < ncols) ? row[col] : 0);
            break;
        }

        case WL_PLAN_EXPR_CONST_INT: {
            if (i + 8 > size)
                goto bad;
            int64_t v;
            memcpy(&v, buf + i, 8);
            i += 8;
            filt_push(&s, v);
            break;
        }

        case WL_PLAN_EXPR_BOOL: {
            if (i + 1 > size)
                goto bad;
            filt_push(&s, buf[i++] ? 1 : 0);
            break;
        }

        case WL_PLAN_EXPR_CONST_STR: {
            if (i + 2 > size)
                goto bad;
            uint16_t slen;
            memcpy(&slen, buf + i, 2);
            i += 2;
            if (i + slen > size)
                goto bad;
            if (intern) {
                char *tmp = (char *)malloc((size_t)slen + 1);
                if (!tmp)
                    goto bad;
                memcpy(tmp, buf + i, slen);
                tmp[slen] = '\0';
                int64_t id = wl_intern_put(intern, tmp);
                free(tmp);
                filt_push(&s, id);
            } else {
                filt_push(&s, 0);
            }
            i += slen;
            break;
        }

        /* Arithmetic */
        case WL_PLAN_EXPR_ARITH_ADD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_add_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_sub_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_mul_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_div_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_mod_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BAND: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a & b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BOR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a | b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BXOR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a ^ b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BNOT: {
            int64_t a = filt_pop(&s);
            filt_push(&s, ~a);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_shl_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_shr_int64(a, b, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        /*
         * Digest opcodes.  Each integer opcode is paired with the _S
         * variant the plan generator emits when the operand is
         * symbol-typed (Issue #963); filt_digest_bytes() is the only
         * place the two differ.
         */
        case WL_PLAN_EXPR_ARITH_HASH:
        case WL_PLAN_EXPR_ARITH_HASH_S: {
            int64_t a = filt_pop(&s);
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_HASH_S, intern);
            filt_push(&s, (int64_t)XXH3_64bits(m.ptr, m.len));
            break;
        }

        case WL_PLAN_EXPR_ARITH_CRC32_ETH:
        case WL_PLAN_EXPR_ARITH_CRC32_ETH_S: {
            int64_t a = filt_pop(&s);
            filt_bytes_t m;
            filt_digest_bytes(&m, a,
                tag == WL_PLAN_EXPR_ARITH_CRC32_ETH_S, intern);
            filt_push(&s, (int64_t)ethernet_crc32(m.ptr, m.len));
            break;
        }

        case WL_PLAN_EXPR_ARITH_CRC32_CAST:
        case WL_PLAN_EXPR_ARITH_CRC32_CAST_S: {
            int64_t a = filt_pop(&s);
            filt_bytes_t m;
            filt_digest_bytes(&m, a,
                tag == WL_PLAN_EXPR_ARITH_CRC32_CAST_S, intern);
            filt_push(&s, (int64_t)castagnoli_crc32(m.ptr, m.len));
            break;
        }

        case WL_PLAN_EXPR_ARITH_MD5:
        case WL_PLAN_EXPR_ARITH_MD5_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[16];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_MD5_S, intern);
            if (wl_columnar_ops_psa_hash_bytes(PSA_ALG_MD5, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* md5 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA1:
        case WL_PLAN_EXPR_ARITH_SHA1_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[20];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_SHA1_S, intern);
            if (wl_columnar_ops_psa_hash_bytes(PSA_ALG_SHA_1, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* sha1 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA256:
        case WL_PLAN_EXPR_ARITH_SHA256_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[32];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_SHA256_S,
                intern);
            if (wl_columnar_ops_psa_hash_bytes(PSA_ALG_SHA_256, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* sha256 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA512:
        case WL_PLAN_EXPR_ARITH_SHA512_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[64];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_SHA512_S,
                intern);
            if (wl_columnar_ops_psa_hash_bytes(PSA_ALG_SHA_512, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* sha512 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_HMAC_SHA256:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t key_val = filt_pop(&s);
            int64_t msg_val = filt_pop(&s);
            unsigned char digest[32];
            bool msg_sym = (tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS
                || tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI);
            bool key_sym = (tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS
                || tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS);
            filt_bytes_t m, k;
            filt_digest_bytes(&m, msg_val, msg_sym, intern);
            filt_digest_bytes(&k, key_val, key_sym, intern);
            if (wl_columnar_ops_psa_hmac_sha256(m.ptr, m.len, k.ptr, k.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            (void)filt_pop(&s);
            goto bad; /* hmac_sha256 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_UUID4: {
#ifdef WL_MBEDTLS_ENABLED
            unsigned char uuid[16];
            if (wl_columnar_ops_psa_init() != 0)
                goto bad;
            if (psa_generate_random(uuid, sizeof(uuid)) != PSA_SUCCESS)
                goto bad;
            /* RFC 4122 v4: set version=4, variant=0b10 */
            uuid[6] = (uuid[6] & 0x0F) | 0x40;
            uuid[8] = (uuid[8] & 0x3F) | 0x80;
            /* Return upper 64 bits as int64_t */
            int64_t result;
            memcpy(&result, uuid, sizeof(result));
            filt_push(&s, result);
#else
            goto bad; /* uuid4 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        /*
         * uuid5(ns, name) over symbols prefixes each operand with a one-byte
         * domain tag and its length before hashing, so distinct typed
         * (namespace, name) pairs always hash distinct bytes.  A bare
         * concatenation -- what the int64-only opcode does, and what it can
         * safely keep doing with two fixed-width operands of one type --
         * would make uuid5("ab", "c") and uuid5("a", "bc") the same value
         * the moment the operands became variable-length; a length prefix
         * without the tag would still equate uuid5("abcdefgh", x) with
         * uuid5(<the int64 spelling those bytes>, x).
         *
         * "Distinct bytes" is a claim about the digest input only.  The
         * digest is then truncated to 8 bytes with a version nibble forced,
         * so distinct inputs can still return the same int64.
         */
        case WL_PLAN_EXPR_ARITH_UUID5:
        case WL_PLAN_EXPR_ARITH_UUID5_SS:
        case WL_PLAN_EXPR_ARITH_UUID5_SI:
        case WL_PLAN_EXPR_ARITH_UUID5_IS: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t name = filt_pop(&s);
            int64_t ns = filt_pop(&s);
            unsigned char digest[20]; /* SHA-1 output */
            bool ns_sym = (tag == WL_PLAN_EXPR_ARITH_UUID5_SS
                || tag == WL_PLAN_EXPR_ARITH_UUID5_SI);
            bool name_sym = (tag == WL_PLAN_EXPR_ARITH_UUID5_SS
                || tag == WL_PLAN_EXPR_ARITH_UUID5_IS);
            filt_bytes_t nsb, nmb;
            filt_digest_bytes(&nsb, ns, ns_sym, intern);
            filt_digest_bytes(&nmb, name, name_sym, intern);
            if (wl_columnar_ops_psa_hash_pair(PSA_ALG_SHA_1,
                tag != WL_PLAN_EXPR_ARITH_UUID5,
                nsb.is_symbol ? WL_DIGEST_TAG_SYMBOL : WL_DIGEST_TAG_INT64,
                nsb.ptr, nsb.len,
                nmb.is_symbol ? WL_DIGEST_TAG_SYMBOL : WL_DIGEST_TAG_INT64,
                nmb.ptr, nmb.len, digest, sizeof(digest)) != 0)
                goto bad;
            /* RFC 4122 v5: set version=5, variant=0b10 */
            digest[6] = (digest[6] & 0x0F) | 0x50;
            digest[8] = (digest[8] & 0x3F) | 0x80;
            /* Return upper 64 bits as int64_t */
            int64_t result;
            memcpy(&result, digest, sizeof(result));
            filt_push(&s, result);
#else
            (void)filt_pop(&s);
            (void)filt_pop(&s);
            goto bad; /* uuid5 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        /* Comparisons */
        case WL_PLAN_EXPR_CMP_EQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a == b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_NEQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a != b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a < b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a > b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a <= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a >= b ? 1 : 0);
            break;
        }

        /* Aggregates: not valid in row-level evaluation, skip */
        case WL_PLAN_EXPR_AGG_COUNT:
        case WL_PLAN_EXPR_AGG_SUM:
        case WL_PLAN_EXPR_AGG_MIN:
        case WL_PLAN_EXPR_AGG_MAX:
            break;

        /* String functions: operands are intern IDs (int64_t on stack) */
        case WL_PLAN_EXPR_STR_FN_STRLEN: {
            int64_t a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_strlen(a, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_CAT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_cat(a, b, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_SUBSTR: {
            int64_t c = filt_pop(&s), b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_substr(a, b, c, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_CONTAINS: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s,
                intern ? (string_ops_contains(a, b, intern) ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_PREFIX: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s,
                intern ? (string_ops_str_prefix(a, b, intern) ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_SUFFIX: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s,
                intern ? (string_ops_str_suffix(a, b, intern) ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_ORD: {
            int64_t a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_str_ord(a, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_UPPER: {
            int64_t a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_to_upper(a, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_LOWER: {
            int64_t a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_to_lower(a, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_REPLACE: {
            int64_t c = filt_pop(&s), b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_str_replace(a, b, c, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TRIM: {
            int64_t a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_trim(a, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_STRING: {
            int64_t a = filt_pop(&s);
            filt_push(&s, intern ? string_ops_to_string(a, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_NUMBER: {
            int64_t a = filt_pop(&s);
            int64_t v;
            if (!intern || wl_string_ops_to_number_checked(a, intern, &v) != 0)
                goto bad;
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_UUID5_RFC: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t name_id = filt_pop(&s);
            int64_t namespace_id = filt_pop(&s);
            const char *namespace_text = intern
                ? wl_intern_reverse(intern, namespace_id) : NULL;
            const char *name = intern
                ? wl_intern_reverse(intern, name_id) : NULL;
            unsigned char namespace_uuid[16];
            unsigned char digest[20];

            /* RFC 4122 requires the namespace operand to be a canonical
             * UUID string, but the name is an arbitrary string. */
            if (!intern || !name
                || wl_columnar_ops_parse_uuid(namespace_text,
                namespace_uuid) != 0
                || wl_columnar_ops_psa_hash_pair(PSA_ALG_SHA_1, false,
                0, namespace_uuid, sizeof(namespace_uuid), 0,
                name, strlen(name), digest, sizeof(digest)) != 0)
                goto bad;
            /* RFC 4122 v5: version 5 and the RFC variant. */
            digest[6] = (digest[6] & 0x0F) | 0x50;
            digest[8] = (digest[8] & 0x3F) | 0x80;
            int64_t result = wl_columnar_ops_format_uuid(digest, intern);
            if (result < 0)
                goto bad;
            filt_push(&s, result);
#else
            (void)filt_pop(&s);
            (void)filt_pop(&s);
            goto bad; /* uuid5_rfc requires mbedTLS */
#endif
            break;
        }

        /* String comparisons: intern IDs → strcmp-based bool */
        case WL_PLAN_EXPR_CMP_STR_EQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            filt_push(&s,
                (sa && sb) ? (strcmp(sa, sb) == 0 ? 1 : 0) : (a == b ? 1 : 0));
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_NEQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            filt_push(&s,
                (sa && sb) ? (strcmp(sa, sb) != 0 ? 1 : 0) : (a != b ? 1 : 0));
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_LT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            filt_push(&s, (sa && sb) ? (strcmp(sa, sb) < 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_GT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            filt_push(&s, (sa && sb) ? (strcmp(sa, sb) > 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_LTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            filt_push(&s, (sa && sb) ? (strcmp(sa, sb) <= 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_GTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            filt_push(&s, (sa && sb) ? (strcmp(sa, sb) >= 0 ? 1 : 0) : 0);
            break;
        }

        default:
            goto bad;
        }
    }
    *out_val = s.top > 0 ? s.vals[s.top - 1] : 0;
    return 0;

bad:
    *out_val = 0;
    return 1;
}

/*
 * col_eval_filter_row:
 * Evaluate the postfix expression buffer against a single row.
 * Variable names are assumed to be "col<N>" (rewritten by plan compiler).
 * Returns non-zero if the row passes the predicate, 0 if filtered out.
 */
static int
col_eval_filter_row(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, wl_intern_t *intern)
{
    int64_t val;
    int err = col_eval_expr_run(buf, size, row, ncols, &val, intern);
    return err ? 0 : (val != 0 ? 1 : 0);
}

/*
 * col_eval_expr_i64:
 * Evaluate the postfix expression buffer and write the computed value to out_val.
 * Used by MAP operations to compute head argument expressions.
 * Returns 0 on success, non-zero on empty expression or evaluation error.
 */
static int
col_eval_expr_i64(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, int64_t *out_val, wl_intern_t *intern)
{
    return col_eval_expr_run(buf, size, row, ncols, out_val, intern);
}

/* ======================================================================== */
/* Pre-compiled expression evaluator                                        */
/* ======================================================================== */

/* Forward declaration: parse_var_col is defined in the FILTER section. */
static bool parse_var_col(const uint8_t *buf, uint32_t size, uint32_t *pos,
    uint32_t *col_out);

/*
 * col_expr_instr_t: single decoded instruction.
 *
 * op   — WL_PLAN_EXPR_* opcode.
 * iarg — for WL_PLAN_EXPR_VAR: column index pre-parsed from "colN".
 * larg — for WL_PLAN_EXPR_CONST_INT / BOOL: immediate int64 value.
 * Operator opcodes (arithmetic, comparison, aggregate) leave iarg/larg at 0.
 */
typedef struct {
    uint8_t op;
    uint32_t iarg;
    int64_t larg;
} col_expr_instr_t;

typedef struct {
    col_expr_instr_t *instrs;
    uint32_t ninstr;
} col_expr_compiled_t;

static void
col_expr_compiled_free(col_expr_compiled_t *c)
{
    if (c) {
        free(c->instrs);
        free(c);
    }
}

/*
 * col_expr_compile:
 * Walk the bytecode buffer once and produce a pre-compiled instruction array.
 * Variable names ("colN") are resolved to column indices here so that the
 * per-row evaluator (col_eval_expr_compiled) never calls strtol.
 *
 * Returns NULL if the expression contains unsupported opcodes (CONST_STR,
 * hash/crypto functions) or if allocation fails.  Callers fall back to
 * col_eval_expr_run in that case.
 */
static col_expr_compiled_t *
col_expr_compile(const uint8_t *buf, uint32_t size)
{
    if (!buf || size == 0)
        return NULL;

    /* Pass 1: validate bytecode and count instructions. */
    uint32_t ninstr = 0;
    uint32_t i = 0;
    while (i < size) {
        uint8_t tag = buf[i];
        switch ((wl_plan_expr_tag_t)tag) {
        case WL_PLAN_EXPR_VAR: {
            uint32_t pos = i;
            uint32_t col = 0;
            if (!parse_var_col(buf, size, &pos, &col))
                return NULL;
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_CONST_INT:
            i++;
            if (i + 8 > size)
                return NULL;
            i += 8;
            break;
        case WL_PLAN_EXPR_BOOL:
            i++;
            if (i + 1 > size)
                return NULL;
            i++;
            break;
        case WL_PLAN_EXPR_CONST_STR:
            return NULL; /* unsupported: fall back to slow path */
        /* Arithmetic operators (no payload) */
        case WL_PLAN_EXPR_ARITH_ADD:
        case WL_PLAN_EXPR_ARITH_SUB:
        case WL_PLAN_EXPR_ARITH_MUL:
        case WL_PLAN_EXPR_ARITH_DIV:
        case WL_PLAN_EXPR_ARITH_MOD:
        case WL_PLAN_EXPR_ARITH_BAND:
        case WL_PLAN_EXPR_ARITH_BOR:
        case WL_PLAN_EXPR_ARITH_BXOR:
        case WL_PLAN_EXPR_ARITH_BNOT:
        case WL_PLAN_EXPR_ARITH_SHL:
        case WL_PLAN_EXPR_ARITH_SHR:
        /* Comparison operators (no payload) */
        case WL_PLAN_EXPR_CMP_EQ:
        case WL_PLAN_EXPR_CMP_NEQ:
        case WL_PLAN_EXPR_CMP_LT:
        case WL_PLAN_EXPR_CMP_GT:
        case WL_PLAN_EXPR_CMP_LTE:
        case WL_PLAN_EXPR_CMP_GTE:
        /* Aggregate operators (no payload) */
        case WL_PLAN_EXPR_AGG_COUNT:
        case WL_PLAN_EXPR_AGG_SUM:
        case WL_PLAN_EXPR_AGG_MIN:
        case WL_PLAN_EXPR_AGG_MAX:
            i++;
            break;
        default:
            return NULL; /* hash/UUID/crypto: unsupported, use slow path */
        }
        ninstr++;
    }

    if (ninstr == 0)
        return NULL;

    col_expr_compiled_t *c =
        (col_expr_compiled_t *)malloc(sizeof(col_expr_compiled_t));
    if (!c)
        return NULL;
    c->instrs =
        (col_expr_instr_t *)malloc(ninstr * sizeof(col_expr_instr_t));
    if (!c->instrs) {
        free(c);
        return NULL;
    }
    c->ninstr = ninstr;

    /* Pass 2: fill instruction array. */
    uint32_t j = 0;
    i = 0;
    while (i < size && j < ninstr) {
        col_expr_instr_t *instr = &c->instrs[j++];
        instr->op = buf[i];
        instr->iarg = 0;
        instr->larg = 0;
        switch ((wl_plan_expr_tag_t)buf[i]) {
        case WL_PLAN_EXPR_VAR: {
            uint32_t pos = i;
            parse_var_col(buf, size, &pos, &instr->iarg);
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_CONST_INT:
            i++; /* skip opcode */
            memcpy(&instr->larg, buf + i, 8);
            i += 8;
            break;
        case WL_PLAN_EXPR_BOOL:
            i++; /* skip opcode */
            instr->larg = buf[i++] ? 1 : 0;
            break;
        default:
            i++; /* operator: consume opcode byte only */
            break;
        }
    }
    return c;
}

/*
 * col_eval_expr_compiled:
 * Fast postfix evaluator using a pre-compiled instruction array.
 * VAR instructions use pre-parsed column indices — no strtol per row.
 * Returns 0 on success with result in *out_val, non-zero on error.
 */
static int
col_eval_expr_compiled(const col_expr_compiled_t *c, const int64_t *row,
    uint32_t ncols, int64_t *out_val)
{
    filt_stack_t s;
    s.top = 0;
    for (uint32_t k = 0; k < c->ninstr; k++) {
        const col_expr_instr_t *in = &c->instrs[k];
        switch ((wl_plan_expr_tag_t)in->op) {
        case WL_PLAN_EXPR_VAR:
            filt_push(&s, (in->iarg < ncols) ? row[in->iarg] : 0);
            break;
        case WL_PLAN_EXPR_CONST_INT:
        case WL_PLAN_EXPR_BOOL:
            filt_push(&s, in->larg);
            break;
        case WL_PLAN_EXPR_ARITH_ADD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_add_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_sub_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_mul_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_div_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_mod_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BAND: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a & b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BOR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a | b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BXOR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a ^ b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BNOT: {
            int64_t a = filt_pop(&s);
            filt_push(&s, ~a);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_shl_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            int64_t v;
            if (wl_columnar_ops_checked_shr_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            filt_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_CMP_EQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a == b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_NEQ: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a != b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a < b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GT: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a > b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a <= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GTE: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a >= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_AGG_COUNT:
        case WL_PLAN_EXPR_AGG_SUM:
        case WL_PLAN_EXPR_AGG_MIN:
        case WL_PLAN_EXPR_AGG_MAX:
            break;
        default:
            *out_val = 0;
            return 1;
        }
    }
    *out_val = s.top > 0 ? s.vals[s.top - 1] : 0;
    return 0;
}

/* ======================================================================== */
/* Eval Stack                                                                */
/* ======================================================================== */

void
eval_stack_init(eval_stack_t *s)
{
    memset(s, 0, sizeof(*s));
}

int
eval_stack_push(eval_stack_t *s, col_rel_t *r, bool owned)
{
    if (s->top >= COL_STACK_MAX)
        return ENOBUFS;
    s->items[s->top].rel = r;
    s->items[s->top].owned = owned;
    s->items[s->top].is_delta = false;
    s->items[s->top].seg_boundaries = NULL;
    s->items[s->top].seg_count = 0;
    s->top++;
    return 0;
}

/* Push with explicit delta flag (used by VARIABLE and JOIN to tag delta results). */
int
eval_stack_push_delta(eval_stack_t *s, col_rel_t *r, bool owned, bool is_delta)
{
    int rc = eval_stack_push(s, r, owned);
    if (rc == 0)
        s->items[s->top - 1].is_delta = is_delta;
    return rc;
}

eval_entry_t
eval_stack_pop(eval_stack_t *s)
{
    eval_entry_t e = { NULL, false, false, NULL, 0 };
    if (s->top > 0)
        e = s->items[--s->top];
    return e;
}

void
eval_stack_drain(eval_stack_t *s)
{
    while (s->top > 0) {
        eval_entry_t e = eval_stack_pop(s);
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        if (e.owned)
            col_rel_destroy(e.rel);
    }
}

/* ======================================================================== */
/* Operator Implementations                                                  */
/* ======================================================================== */

/* Cross-module function declarations are in columnar/internal.h */

/*
 * col_op_resolve_key:
 *   Resolve a plan-supplied "colN" join key against @rel.  A key that does
 *   not resolve means the plan carries a column layout that disagrees with
 *   the relation the operator actually sees; the operator then degrades to
 *   column 0 and joins on the wrong column, which under-derives silently.
 *   Log it under JOIN so the condition is observable (issue #955).
 */
static uint32_t
col_op_resolve_key(const col_rel_t *rel, const char *name, const char *side,
    const char *op_name, const char *right_relation)
{
    int idx = col_rel_col_idx(rel, name);
    if (idx >= 0)
        return (uint32_t)idx;
    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR,
        "%s: unresolved %s key '%s' (right=%s, ncols=%u) -- falling back to "
        "column 0",
        op_name, side, name ? name : "(null)",
        right_relation ? right_relation : "-", rel->ncols);
    return 0;
}

/* --- VARIABLE ------------------------------------------------------------ */

int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op->relation_name)
        return ENOENT;
    col_rel_t *full_rel = session_find_rel(sess, op->relation_name);
    if (!full_rel)
        return ENOENT;

    /* Delta mode controls whether we use delta or full relation.
     * FORCE_FULL:  always use the full relation (no delta substitution).
     * FORCE_DELTA: always use the delta relation; if no delta exists or
     *              it is empty, push an empty relation so that the rule
     *              copy produces no output (correct semi-naive behavior).
     * AUTO:        heuristic -- prefer delta only when it is a genuine
     *              strict subset of the full relation (nrows < full).
     *
     * Issue #158 extension: When retraction_seeded and iteration == 0,
     * look for $r$<name> (retraction delta) instead of $d$<name> */
    char dname[256];
    col_rel_t *delta = NULL;

    if (sess->retraction_seeded && sess->current_iteration == 0
        && !sess->retraction_right_pass) {
        /* Retraction mode (left pass): look for $r$<name> retraction delta.
         * Issue #472: Skip during right pass — VARIABLE loads full relation
         * so JOIN/SEMIJOIN can use $r$ on the right side instead. */
        if (retraction_rel_name(op->relation_name, dname, sizeof(dname)) == 0)
            delta = session_find_rel(sess, dname);
    } else {
        /* Normal mode: look for $d$<name> insertion delta */
        snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
        delta = session_find_rel(sess, dname);
    }

    if (op->delta_mode == WL_DELTA_FORCE_EMPTY
        || (op->delta_mode == WL_DELTA_FORCE_EMPTY_AFTER_SEED
        && sess->tdd_outbound_only_active
        && sess->current_iteration > 0)) {
        /* Issue #370: segment has no FORCE_DELTA — push empty to skip. */
        col_rel_t *empty = col_rel_pool_new_like(
            sess->delta_pool, "$empty_skip", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, false);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }
    if (op->delta_mode == WL_DELTA_FORCE_FULL) {
        return eval_stack_push_delta(stack, full_rel, false, false);
    }
    if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows > 0) {
            return eval_stack_push_delta(stack, delta, false, true);
        }
        if (sess->current_iteration == 0) {
            if (sess->delta_seeded || sess->retraction_seeded) {
                /* Issue #83 (delta-seeded) or #158 (retraction-seeded):
                 * No pre-seeded delta means this relation has no new/removed facts.
                 * Push empty so only rules with actual deltas produce output. */
                col_rel_t *empty = col_rel_pool_new_like(
                    sess->delta_pool, "$empty_delta", full_rel);
                if (!empty)
                    return ENOMEM;
                int push_rc = eval_stack_push_delta(stack, empty, true, true);
                if (push_rc != 0)
                    col_rel_destroy(empty);
                return push_rc;
            }
            /* Base-case iteration: no deltas exist yet, fall back to full
             * relation so EDB-grounded rules can still fire on iter 0. */
            return eval_stack_push_delta(stack, full_rel, false, false);
        }
        /* Iteration > 0: delta absent or empty means the relation has
         * converged.  Push an empty relation so this rule copy produces
         * no output (correct semi-naive semantics, issue #85). */
        col_rel_t *empty
            = col_rel_pool_new_like(sess->delta_pool, "$empty_delta", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, true);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }

    /* WL_DELTA_AUTO: use delta if strictly smaller than full relation.
     * Exception: inside a TDD worker sub-pass the broadcast $d$<rel> may be
     * >= the local partition, so we must use it whenever it is non-empty. */
    bool use_delta = delta && (((delta->nrows > 0
        && delta->nrows < full_rel->nrows) || (delta->nrows > 0
        && sess->tdd_subpass_active)) || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0));
    col_rel_t *rel = use_delta ? delta : full_rel;
    /* push borrowed reference - session owns the relation */
    return eval_stack_push_delta(stack, rel, false, use_delta);
}

/* --- MAP ----------------------------------------------------------------- */

int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    uint32_t pc = op->project_count;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$map", pc);
    if (!out) {
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * pc);
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    /* Pre-compile map expressions once to avoid per-row strtol. */
    col_expr_compiled_t **ce_map = NULL;
    uint32_t ce_map_count = 0;
    if (op->map_exprs && op->map_expr_count > 0) {
        ce_map = (col_expr_compiled_t **)calloc(pc,
                sizeof(col_expr_compiled_t *));
        if (ce_map) {
            ce_map_count = (op->map_expr_count < pc) ? op->map_expr_count : pc;
            for (uint32_t c = 0; c < ce_map_count; c++) {
                if (op->map_exprs[c].data && op->map_exprs[c].size > 0)
                    ce_map[c] = col_expr_compile(op->map_exprs[c].data,
                            op->map_exprs[c].size);
            }
        }
    }

    /* Row scratch, hoisted out of the loop: initialising it per row would
     * malloc once per row for relations wider than COL_STACK_MAX (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, e.rel->ncols)) {
        if (ce_map) {
            for (uint32_t c = 0; c < ce_map_count; c++)
                col_expr_compiled_free(ce_map[c]);
            free(ce_map);
        }
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *const row = row_rb.ptr;
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        col_rel_row_copy_out(e.rel, r, row);
        for (uint32_t c = 0; c < pc; c++) {
            if (op->map_exprs && c < op->map_expr_count && op->map_exprs[c].data
                && op->map_exprs[c].size > 0) {
                if (ce_map && c < ce_map_count && ce_map[c]) {
                    int64_t val = 0;
                    if (col_eval_expr_compiled(ce_map[c], row, e.rel->ncols,
                        &val) != 0) {
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                col_expr_compiled_free(ce_map[i]);
                            free(ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (e.owned)
                            col_rel_destroy(e.rel);
                        return ERANGE;
                    }
                    tmp[c] = val;
                } else {
                    int64_t val = 0;
                    if (col_eval_expr_i64(op->map_exprs[c].data,
                        op->map_exprs[c].size, row, e.rel->ncols,
                        &val, sess->intern) != 0) {
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                col_expr_compiled_free(ce_map[i]);
                            free(ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (e.owned)
                            col_rel_destroy(e.rel);
                        return ERANGE;
                    }
                    tmp[c] = val;
                }
            } else {
                uint32_t src = op->project_indices ? op->project_indices[c] : c;
                tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
            }
        }
        int rc = col_rel_append_row(out, tmp);
        if (rc != 0) {
            if (ce_map) {
                for (uint32_t c = 0; c < ce_map_count; c++)
                    col_expr_compiled_free(ce_map[c]);
                free(ce_map);
            }
            col_row_buf_release(&row_rb);
            free(tmp);
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(e.rel);
            return rc;
        }
    }

    if (ce_map) {
        for (uint32_t c = 0; c < ce_map_count; c++)
            col_expr_compiled_free(ce_map[c]);
        free(ce_map);
    }
    col_row_buf_release(&row_rb);
    free(tmp);

    if (e.owned)
        col_rel_destroy(e.rel);
    return eval_stack_push(stack, out, true);
}

/* --- FILTER -------------------------------------------------------------- */

/*
 * simple_filter_cmp_t:
 * Decoded simple comparison predicate of the form:
 *   colA CMP CONST   (b_is_const == true)
 *   colA CMP colB    (b_is_const == false)
 *
 * Populated by filter_is_simple_cmp() when the bytecode matches one of
 * these two patterns.  Used to bypass the full postfix interpreter.
 */
typedef struct {
    uint32_t col_a;  /* first operand column index */
    bool b_is_const; /* true: b is a constant; false: b is colB */
    uint32_t col_b;  /* second operand column index (when !b_is_const) */
    int64_t const_b; /* constant value (when b_is_const) */
    wl_plan_expr_tag_t cmp_op; /* comparison opcode (EQ/NEQ/LT/LTE/GT/GTE) */
} simple_filter_cmp_t;

/*
 * parse_var_col:
 * Parse a WL_PLAN_EXPR_VAR token at buf[i] (not yet consumed; i points at
 * the opcode byte itself).  If successful, advance *pos past the full token
 * and store the extracted column index in *col_out.
 * Returns true on success, false on malformed bytecode.
 *
 * VAR encoding: [0x01][name_len:u16 LE][name:u8*name_len]
 * where name is "colN" (N is the decimal column index).
 */
static bool
parse_var_col(const uint8_t *buf, uint32_t size, uint32_t *pos,
    uint32_t *col_out)
{
    uint32_t i = *pos;
    if (i >= size || buf[i] != (uint8_t)WL_PLAN_EXPR_VAR)
        return false;
    i++; /* consume opcode */
    if (i + 2 > size)
        return false;
    uint16_t nlen;
    memcpy(&nlen, buf + i, 2);
    i += 2;
    if (i + nlen > size)
        return false;
    /* name must start with "col" followed by decimal digits */
    if (nlen < 4 || buf[i] != 'c' || buf[i + 1] != 'o' || buf[i + 2] != 'l')
        return false;
    /* parse digits */
    uint32_t col = 0;
    uint32_t digits = nlen - 3;
    if (digits == 0 || digits > 9) /* no digits or overflow guard */
        return false;
    for (uint32_t d = 0; d < digits; d++) {
        uint8_t ch = buf[i + 3 + d];
        if (ch < '0' || ch > '9')
            return false;
        col = col * 10 + (uint32_t)(ch - '0');
    }
    i += nlen;
    *pos = i;
    *col_out = col;
    return true;
}

/*
 * filter_is_simple_cmp:
 * Inspect the bytecode buffer and return true if it encodes exactly one
 * of:
 *   Pattern A: VAR("colA")  CONST_INT(k)  CMP_OP
 *   Pattern B: VAR("colA")  VAR("colB")   CMP_OP
 *
 * On success, fill *out and return true.
 * If the bytecode does not match (complex expression, string constants,
 * arithmetic, etc.) return false so the caller falls back to the full
 * interpreter.
 */
static bool
filter_is_simple_cmp(const uint8_t *buf, uint32_t size,
    simple_filter_cmp_t *out)
{
    if (!buf || size == 0)
        return false;

    uint32_t pos = 0;

    /* --- First operand: must be VAR("colA") --- */
    uint32_t col_a = 0;
    if (!parse_var_col(buf, size, &pos, &col_a))
        return false;

    /* --- Second operand: CONST_INT or VAR("colB") --- */
    bool b_is_const = false;
    int64_t const_b = 0;
    uint32_t col_b = 0;

    if (pos < size && buf[pos] == (uint8_t)WL_PLAN_EXPR_CONST_INT) {
        pos++; /* consume opcode */
        if (pos + 8 > size)
            return false;
        memcpy(&const_b, buf + pos, 8);
        pos += 8;
        b_is_const = true;
    } else if (pos < size && buf[pos] == (uint8_t)WL_PLAN_EXPR_VAR) {
        if (!parse_var_col(buf, size, &pos, &col_b))
            return false;
        b_is_const = false;
    } else {
        return false;
    }

    /* --- Third token: CMP opcode (no payload) --- */
    if (pos >= size)
        return false;
    uint8_t cmp_tag = buf[pos++];
    switch ((wl_plan_expr_tag_t)cmp_tag) {
    case WL_PLAN_EXPR_CMP_EQ:
    case WL_PLAN_EXPR_CMP_NEQ:
    case WL_PLAN_EXPR_CMP_LT:
    case WL_PLAN_EXPR_CMP_GT:
    case WL_PLAN_EXPR_CMP_LTE:
    case WL_PLAN_EXPR_CMP_GTE:
        break;
    default:
        return false;
    }

    /* --- No remaining bytes --- */
    if (pos != size)
        return false;

    out->col_a = col_a;
    out->b_is_const = b_is_const;
    out->col_b = col_b;
    out->const_b = const_b;
    out->cmp_op = (wl_plan_expr_tag_t)cmp_tag;
    return true;
}

/*
 * col_filter_cmp_row:
 * Evaluate a simple_filter_cmp_t predicate against a single row.
 * Inlined helper shared by the scalar fast-path and SIMD tail loops.
 */
static inline bool
col_filter_cmp_row(const int64_t *row, uint32_t ncols,
    const simple_filter_cmp_t *cmp)
{
    if (cmp->col_a >= ncols)
        return false;
    int64_t a_val = row[cmp->col_a];
    int64_t b_val = cmp->b_is_const
                        ? cmp->const_b
                        : (cmp->col_b < ncols ? row[cmp->col_b] : 0);
    switch (cmp->cmp_op) {
    case WL_PLAN_EXPR_CMP_EQ:
        return a_val == b_val;
    case WL_PLAN_EXPR_CMP_NEQ:
        return a_val != b_val;
    case WL_PLAN_EXPR_CMP_LT:
        return a_val < b_val;
    case WL_PLAN_EXPR_CMP_LTE:
        return a_val <= b_val;
    case WL_PLAN_EXPR_CMP_GT:
        return a_val > b_val;
    case WL_PLAN_EXPR_CMP_GTE:
        return a_val >= b_val;
    default:
        return false;
    }
}

/* --- Column-native filter selection kernel -------------------------------
 *
 * col_filter_select_rows() scans one or two contiguous int64_t columns and
 * writes the indices of the passing rows into a selection vector.  It is the
 * scan half of col_op_filter()'s fast path; materialization is separate so
 * the scan can run branchless over contiguous memory.
 *
 * This replaces the row-major SIMD kernels removed earlier: those gathered
 * data[r * ncols + col_a], touching one cache line per row.  Column-native
 * access is contiguous, so a whole vector loads in one instruction.
 */

#ifdef __AVX2__
/*
 * Left-pack table: for selection mask m, byte j holds the lane index of the
 * j-th set bit of m; trailing bytes are 0 and are never consumed because the
 * caller advances by popcount(m).  256 * 8 = 2048 bytes, and it lands in
 * .rodata rather than .text.
 *
 * _mm256_cvtepu8_epi32 widens the 8 bytes to 8 uint32 lanes, which is the
 * control operand _mm256_permutevar8x32_epi32 needs.
 */
static const uint8_t col_filter_leftpack_lut[256][8] = {
    { 0, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 0, 0, 0, 0, 0, 0, 0 },
    { 1, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 0, 0, 0, 0, 0, 0 },
    { 2, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 2, 0, 0, 0, 0, 0, 0 },
    { 1, 2, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 2, 0, 0, 0, 0, 0 },
    { 3, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 3, 0, 0, 0, 0, 0, 0 },
    { 1, 3, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 3, 0, 0, 0, 0, 0 },
    { 2, 3, 0, 0, 0, 0, 0, 0 },
    { 0, 2, 3, 0, 0, 0, 0, 0 },
    { 1, 2, 3, 0, 0, 0, 0, 0 },
    { 0, 1, 2, 3, 0, 0, 0, 0 },
    { 4, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 4, 0, 0, 0, 0, 0, 0 },
    { 1, 4, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 4, 0, 0, 0, 0, 0 },
    { 2, 4, 0, 0, 0, 0, 0, 0 },
    { 0, 2, 4, 0, 0, 0, 0, 0 },
    { 1, 2, 4, 0, 0, 0, 0, 0 },
    { 0, 1, 2, 4, 0, 0, 0, 0 },
    { 3, 4, 0, 0, 0, 0, 0, 0 },
    { 0, 3, 4, 0, 0, 0, 0, 0 },
    { 1, 3, 4, 0, 0, 0, 0, 0 },
    { 0, 1, 3, 4, 0, 0, 0, 0 },
    { 2, 3, 4, 0, 0, 0, 0, 0 },
    { 0, 2, 3, 4, 0, 0, 0, 0 },
    { 1, 2, 3, 4, 0, 0, 0, 0 },
    { 0, 1, 2, 3, 4, 0, 0, 0 },
    { 5, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 5, 0, 0, 0, 0, 0, 0 },
    { 1, 5, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 5, 0, 0, 0, 0, 0 },
    { 2, 5, 0, 0, 0, 0, 0, 0 },
    { 0, 2, 5, 0, 0, 0, 0, 0 },
    { 1, 2, 5, 0, 0, 0, 0, 0 },
    { 0, 1, 2, 5, 0, 0, 0, 0 },
    { 3, 5, 0, 0, 0, 0, 0, 0 },
    { 0, 3, 5, 0, 0, 0, 0, 0 },
    { 1, 3, 5, 0, 0, 0, 0, 0 },
    { 0, 1, 3, 5, 0, 0, 0, 0 },
    { 2, 3, 5, 0, 0, 0, 0, 0 },
    { 0, 2, 3, 5, 0, 0, 0, 0 },
    { 1, 2, 3, 5, 0, 0, 0, 0 },
    { 0, 1, 2, 3, 5, 0, 0, 0 },
    { 4, 5, 0, 0, 0, 0, 0, 0 },
    { 0, 4, 5, 0, 0, 0, 0, 0 },
    { 1, 4, 5, 0, 0, 0, 0, 0 },
    { 0, 1, 4, 5, 0, 0, 0, 0 },
    { 2, 4, 5, 0, 0, 0, 0, 0 },
    { 0, 2, 4, 5, 0, 0, 0, 0 },
    { 1, 2, 4, 5, 0, 0, 0, 0 },
    { 0, 1, 2, 4, 5, 0, 0, 0 },
    { 3, 4, 5, 0, 0, 0, 0, 0 },
    { 0, 3, 4, 5, 0, 0, 0, 0 },
    { 1, 3, 4, 5, 0, 0, 0, 0 },
    { 0, 1, 3, 4, 5, 0, 0, 0 },
    { 2, 3, 4, 5, 0, 0, 0, 0 },
    { 0, 2, 3, 4, 5, 0, 0, 0 },
    { 1, 2, 3, 4, 5, 0, 0, 0 },
    { 0, 1, 2, 3, 4, 5, 0, 0 },
    { 6, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 6, 0, 0, 0, 0, 0, 0 },
    { 1, 6, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 6, 0, 0, 0, 0, 0 },
    { 2, 6, 0, 0, 0, 0, 0, 0 },
    { 0, 2, 6, 0, 0, 0, 0, 0 },
    { 1, 2, 6, 0, 0, 0, 0, 0 },
    { 0, 1, 2, 6, 0, 0, 0, 0 },
    { 3, 6, 0, 0, 0, 0, 0, 0 },
    { 0, 3, 6, 0, 0, 0, 0, 0 },
    { 1, 3, 6, 0, 0, 0, 0, 0 },
    { 0, 1, 3, 6, 0, 0, 0, 0 },
    { 2, 3, 6, 0, 0, 0, 0, 0 },
    { 0, 2, 3, 6, 0, 0, 0, 0 },
    { 1, 2, 3, 6, 0, 0, 0, 0 },
    { 0, 1, 2, 3, 6, 0, 0, 0 },
    { 4, 6, 0, 0, 0, 0, 0, 0 },
    { 0, 4, 6, 0, 0, 0, 0, 0 },
    { 1, 4, 6, 0, 0, 0, 0, 0 },
    { 0, 1, 4, 6, 0, 0, 0, 0 },
    { 2, 4, 6, 0, 0, 0, 0, 0 },
    { 0, 2, 4, 6, 0, 0, 0, 0 },
    { 1, 2, 4, 6, 0, 0, 0, 0 },
    { 0, 1, 2, 4, 6, 0, 0, 0 },
    { 3, 4, 6, 0, 0, 0, 0, 0 },
    { 0, 3, 4, 6, 0, 0, 0, 0 },
    { 1, 3, 4, 6, 0, 0, 0, 0 },
    { 0, 1, 3, 4, 6, 0, 0, 0 },
    { 2, 3, 4, 6, 0, 0, 0, 0 },
    { 0, 2, 3, 4, 6, 0, 0, 0 },
    { 1, 2, 3, 4, 6, 0, 0, 0 },
    { 0, 1, 2, 3, 4, 6, 0, 0 },
    { 5, 6, 0, 0, 0, 0, 0, 0 },
    { 0, 5, 6, 0, 0, 0, 0, 0 },
    { 1, 5, 6, 0, 0, 0, 0, 0 },
    { 0, 1, 5, 6, 0, 0, 0, 0 },
    { 2, 5, 6, 0, 0, 0, 0, 0 },
    { 0, 2, 5, 6, 0, 0, 0, 0 },
    { 1, 2, 5, 6, 0, 0, 0, 0 },
    { 0, 1, 2, 5, 6, 0, 0, 0 },
    { 3, 5, 6, 0, 0, 0, 0, 0 },
    { 0, 3, 5, 6, 0, 0, 0, 0 },
    { 1, 3, 5, 6, 0, 0, 0, 0 },
    { 0, 1, 3, 5, 6, 0, 0, 0 },
    { 2, 3, 5, 6, 0, 0, 0, 0 },
    { 0, 2, 3, 5, 6, 0, 0, 0 },
    { 1, 2, 3, 5, 6, 0, 0, 0 },
    { 0, 1, 2, 3, 5, 6, 0, 0 },
    { 4, 5, 6, 0, 0, 0, 0, 0 },
    { 0, 4, 5, 6, 0, 0, 0, 0 },
    { 1, 4, 5, 6, 0, 0, 0, 0 },
    { 0, 1, 4, 5, 6, 0, 0, 0 },
    { 2, 4, 5, 6, 0, 0, 0, 0 },
    { 0, 2, 4, 5, 6, 0, 0, 0 },
    { 1, 2, 4, 5, 6, 0, 0, 0 },
    { 0, 1, 2, 4, 5, 6, 0, 0 },
    { 3, 4, 5, 6, 0, 0, 0, 0 },
    { 0, 3, 4, 5, 6, 0, 0, 0 },
    { 1, 3, 4, 5, 6, 0, 0, 0 },
    { 0, 1, 3, 4, 5, 6, 0, 0 },
    { 2, 3, 4, 5, 6, 0, 0, 0 },
    { 0, 2, 3, 4, 5, 6, 0, 0 },
    { 1, 2, 3, 4, 5, 6, 0, 0 },
    { 0, 1, 2, 3, 4, 5, 6, 0 },
    { 7, 0, 0, 0, 0, 0, 0, 0 },
    { 0, 7, 0, 0, 0, 0, 0, 0 },
    { 1, 7, 0, 0, 0, 0, 0, 0 },
    { 0, 1, 7, 0, 0, 0, 0, 0 },
    { 2, 7, 0, 0, 0, 0, 0, 0 },
    { 0, 2, 7, 0, 0, 0, 0, 0 },
    { 1, 2, 7, 0, 0, 0, 0, 0 },
    { 0, 1, 2, 7, 0, 0, 0, 0 },
    { 3, 7, 0, 0, 0, 0, 0, 0 },
    { 0, 3, 7, 0, 0, 0, 0, 0 },
    { 1, 3, 7, 0, 0, 0, 0, 0 },
    { 0, 1, 3, 7, 0, 0, 0, 0 },
    { 2, 3, 7, 0, 0, 0, 0, 0 },
    { 0, 2, 3, 7, 0, 0, 0, 0 },
    { 1, 2, 3, 7, 0, 0, 0, 0 },
    { 0, 1, 2, 3, 7, 0, 0, 0 },
    { 4, 7, 0, 0, 0, 0, 0, 0 },
    { 0, 4, 7, 0, 0, 0, 0, 0 },
    { 1, 4, 7, 0, 0, 0, 0, 0 },
    { 0, 1, 4, 7, 0, 0, 0, 0 },
    { 2, 4, 7, 0, 0, 0, 0, 0 },
    { 0, 2, 4, 7, 0, 0, 0, 0 },
    { 1, 2, 4, 7, 0, 0, 0, 0 },
    { 0, 1, 2, 4, 7, 0, 0, 0 },
    { 3, 4, 7, 0, 0, 0, 0, 0 },
    { 0, 3, 4, 7, 0, 0, 0, 0 },
    { 1, 3, 4, 7, 0, 0, 0, 0 },
    { 0, 1, 3, 4, 7, 0, 0, 0 },
    { 2, 3, 4, 7, 0, 0, 0, 0 },
    { 0, 2, 3, 4, 7, 0, 0, 0 },
    { 1, 2, 3, 4, 7, 0, 0, 0 },
    { 0, 1, 2, 3, 4, 7, 0, 0 },
    { 5, 7, 0, 0, 0, 0, 0, 0 },
    { 0, 5, 7, 0, 0, 0, 0, 0 },
    { 1, 5, 7, 0, 0, 0, 0, 0 },
    { 0, 1, 5, 7, 0, 0, 0, 0 },
    { 2, 5, 7, 0, 0, 0, 0, 0 },
    { 0, 2, 5, 7, 0, 0, 0, 0 },
    { 1, 2, 5, 7, 0, 0, 0, 0 },
    { 0, 1, 2, 5, 7, 0, 0, 0 },
    { 3, 5, 7, 0, 0, 0, 0, 0 },
    { 0, 3, 5, 7, 0, 0, 0, 0 },
    { 1, 3, 5, 7, 0, 0, 0, 0 },
    { 0, 1, 3, 5, 7, 0, 0, 0 },
    { 2, 3, 5, 7, 0, 0, 0, 0 },
    { 0, 2, 3, 5, 7, 0, 0, 0 },
    { 1, 2, 3, 5, 7, 0, 0, 0 },
    { 0, 1, 2, 3, 5, 7, 0, 0 },
    { 4, 5, 7, 0, 0, 0, 0, 0 },
    { 0, 4, 5, 7, 0, 0, 0, 0 },
    { 1, 4, 5, 7, 0, 0, 0, 0 },
    { 0, 1, 4, 5, 7, 0, 0, 0 },
    { 2, 4, 5, 7, 0, 0, 0, 0 },
    { 0, 2, 4, 5, 7, 0, 0, 0 },
    { 1, 2, 4, 5, 7, 0, 0, 0 },
    { 0, 1, 2, 4, 5, 7, 0, 0 },
    { 3, 4, 5, 7, 0, 0, 0, 0 },
    { 0, 3, 4, 5, 7, 0, 0, 0 },
    { 1, 3, 4, 5, 7, 0, 0, 0 },
    { 0, 1, 3, 4, 5, 7, 0, 0 },
    { 2, 3, 4, 5, 7, 0, 0, 0 },
    { 0, 2, 3, 4, 5, 7, 0, 0 },
    { 1, 2, 3, 4, 5, 7, 0, 0 },
    { 0, 1, 2, 3, 4, 5, 7, 0 },
    { 6, 7, 0, 0, 0, 0, 0, 0 },
    { 0, 6, 7, 0, 0, 0, 0, 0 },
    { 1, 6, 7, 0, 0, 0, 0, 0 },
    { 0, 1, 6, 7, 0, 0, 0, 0 },
    { 2, 6, 7, 0, 0, 0, 0, 0 },
    { 0, 2, 6, 7, 0, 0, 0, 0 },
    { 1, 2, 6, 7, 0, 0, 0, 0 },
    { 0, 1, 2, 6, 7, 0, 0, 0 },
    { 3, 6, 7, 0, 0, 0, 0, 0 },
    { 0, 3, 6, 7, 0, 0, 0, 0 },
    { 1, 3, 6, 7, 0, 0, 0, 0 },
    { 0, 1, 3, 6, 7, 0, 0, 0 },
    { 2, 3, 6, 7, 0, 0, 0, 0 },
    { 0, 2, 3, 6, 7, 0, 0, 0 },
    { 1, 2, 3, 6, 7, 0, 0, 0 },
    { 0, 1, 2, 3, 6, 7, 0, 0 },
    { 4, 6, 7, 0, 0, 0, 0, 0 },
    { 0, 4, 6, 7, 0, 0, 0, 0 },
    { 1, 4, 6, 7, 0, 0, 0, 0 },
    { 0, 1, 4, 6, 7, 0, 0, 0 },
    { 2, 4, 6, 7, 0, 0, 0, 0 },
    { 0, 2, 4, 6, 7, 0, 0, 0 },
    { 1, 2, 4, 6, 7, 0, 0, 0 },
    { 0, 1, 2, 4, 6, 7, 0, 0 },
    { 3, 4, 6, 7, 0, 0, 0, 0 },
    { 0, 3, 4, 6, 7, 0, 0, 0 },
    { 1, 3, 4, 6, 7, 0, 0, 0 },
    { 0, 1, 3, 4, 6, 7, 0, 0 },
    { 2, 3, 4, 6, 7, 0, 0, 0 },
    { 0, 2, 3, 4, 6, 7, 0, 0 },
    { 1, 2, 3, 4, 6, 7, 0, 0 },
    { 0, 1, 2, 3, 4, 6, 7, 0 },
    { 5, 6, 7, 0, 0, 0, 0, 0 },
    { 0, 5, 6, 7, 0, 0, 0, 0 },
    { 1, 5, 6, 7, 0, 0, 0, 0 },
    { 0, 1, 5, 6, 7, 0, 0, 0 },
    { 2, 5, 6, 7, 0, 0, 0, 0 },
    { 0, 2, 5, 6, 7, 0, 0, 0 },
    { 1, 2, 5, 6, 7, 0, 0, 0 },
    { 0, 1, 2, 5, 6, 7, 0, 0 },
    { 3, 5, 6, 7, 0, 0, 0, 0 },
    { 0, 3, 5, 6, 7, 0, 0, 0 },
    { 1, 3, 5, 6, 7, 0, 0, 0 },
    { 0, 1, 3, 5, 6, 7, 0, 0 },
    { 2, 3, 5, 6, 7, 0, 0, 0 },
    { 0, 2, 3, 5, 6, 7, 0, 0 },
    { 1, 2, 3, 5, 6, 7, 0, 0 },
    { 0, 1, 2, 3, 5, 6, 7, 0 },
    { 4, 5, 6, 7, 0, 0, 0, 0 },
    { 0, 4, 5, 6, 7, 0, 0, 0 },
    { 1, 4, 5, 6, 7, 0, 0, 0 },
    { 0, 1, 4, 5, 6, 7, 0, 0 },
    { 2, 4, 5, 6, 7, 0, 0, 0 },
    { 0, 2, 4, 5, 6, 7, 0, 0 },
    { 1, 2, 4, 5, 6, 7, 0, 0 },
    { 0, 1, 2, 4, 5, 6, 7, 0 },
    { 3, 4, 5, 6, 7, 0, 0, 0 },
    { 0, 3, 4, 5, 6, 7, 0, 0 },
    { 1, 3, 4, 5, 6, 7, 0, 0 },
    { 0, 1, 3, 4, 5, 6, 7, 0 },
    { 2, 3, 4, 5, 6, 7, 0, 0 },
    { 0, 2, 3, 4, 5, 6, 7, 0 },
    { 1, 2, 3, 4, 5, 6, 7, 0 },
    { 0, 1, 2, 3, 4, 5, 6, 7 },
};
#endif /* __AVX2__ */

/*
 * col_filter_cmp_scalar:
 * Scalar comparison shared by the tail loop and the pure-scalar build, so
 * both paths decide identically.  An unrecognized opcode rejects the row.
 */
static inline bool
col_filter_cmp_scalar(int64_t a, int64_t b, wl_plan_expr_tag_t cmp_op)
{
    switch (cmp_op) {
    case WL_PLAN_EXPR_CMP_EQ:
        return a == b;
    case WL_PLAN_EXPR_CMP_NEQ:
        return a != b;
    case WL_PLAN_EXPR_CMP_LT:
        return a < b;
    case WL_PLAN_EXPR_CMP_LTE:
        return a <= b;
    case WL_PLAN_EXPR_CMP_GT:
        return a > b;
    case WL_PLAN_EXPR_CMP_GTE:
        return a >= b;
    default:
        return false;
    }
}

uint32_t
col_filter_select_rows(const int64_t *col_a, const int64_t *col_b,
    int64_t const_b, uint32_t nrows, wl_plan_expr_tag_t cmp_op,
    uint32_t *out_sel)
{
    uint32_t out = 0;
    uint32_t r = 0;

    if (nrows == 0)
        return 0;

    /* Reject unknown opcodes up front; the scalar helper rejects every row
     * for these, so selecting nothing is the same answer. */
    switch (cmp_op) {
    case WL_PLAN_EXPR_CMP_EQ:
    case WL_PLAN_EXPR_CMP_NEQ:
    case WL_PLAN_EXPR_CMP_LT:
    case WL_PLAN_EXPR_CMP_LTE:
    case WL_PLAN_EXPR_CMP_GT:
    case WL_PLAN_EXPR_CMP_GTE:
        break;
    default:
        return 0;
    }

#ifdef __AVX2__
    /*
     * All six opcodes reduce to cmpeq/cmpgt plus an optional operand swap and
     * an optional bitwise NOT, so one loop body covers them all:
     *
     *   EQ  : cmpeq(a, b)          NEQ : ~cmpeq(a, b)
     *   GT  : cmpgt(a, b)          LTE : ~cmpgt(a, b)
     *   LT  : cmpgt(b, a)          GTE : ~cmpgt(b, a)
     *
     * The three selectors are loop-invariant, so this stays a single compact
     * body instead of six specialized copies competing for the .text budget.
     */
    const bool use_eq = (cmp_op == WL_PLAN_EXPR_CMP_EQ
        || cmp_op == WL_PLAN_EXPR_CMP_NEQ);
    const bool swap = (cmp_op == WL_PLAN_EXPR_CMP_LT
        || cmp_op == WL_PLAN_EXPR_CMP_GTE);
    const bool invert = (cmp_op == WL_PLAN_EXPR_CMP_NEQ
        || cmp_op == WL_PLAN_EXPR_CMP_GTE
        || cmp_op == WL_PLAN_EXPR_CMP_LTE);

    const __m256i lane_id = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
    const __m256i all_ones = _mm256_set1_epi64x(-1LL);
    const __m256i const_v = _mm256_set1_epi64x(const_b);

    /* Equivalent to r + 8 <= nrows, but written so the bound cannot wrap:
     * columns are allocated to exactly `capacity` int64_t and capacity ==
     * nrows is reachable, so reading past nrows would be a heap over-read.
     * The remainder runs in the scalar tail. */
    for (; nrows >= 8 && r <= nrows - 8; r += 8) {
        __m256i a0 = _mm256_loadu_si256((const __m256i *)(col_a + r));
        __m256i a1 = _mm256_loadu_si256((const __m256i *)(col_a + r + 4));
        __m256i b0 = const_v;
        __m256i b1 = const_v;

        if (col_b) {
            b0 = _mm256_loadu_si256((const __m256i *)(col_b + r));
            b1 = _mm256_loadu_si256((const __m256i *)(col_b + r + 4));
        }

        __m256i x0 = swap ? b0 : a0;
        __m256i y0 = swap ? a0 : b0;
        __m256i x1 = swap ? b1 : a1;
        __m256i y1 = swap ? a1 : b1;

        __m256i m0 = use_eq ? _mm256_cmpeq_epi64(x0, y0)
                            : _mm256_cmpgt_epi64(x0, y0);
        __m256i m1 = use_eq ? _mm256_cmpeq_epi64(x1, y1)
                            : _mm256_cmpgt_epi64(x1, y1);

        if (invert) {
            m0 = _mm256_xor_si256(m0, all_ones);
            m1 = _mm256_xor_si256(m1, all_ones);
        }

        /* One sign bit per 64-bit lane: 4 bits per vector, 8 bits total. */
        uint32_t mask =
            (uint32_t)_mm256_movemask_pd(_mm256_castsi256_pd(m0))
            | ((uint32_t)_mm256_movemask_pd(_mm256_castsi256_pd(m1)) << 4);

        if (mask == 0)
            continue;

        __m256i idx = _mm256_add_epi32(_mm256_set1_epi32((int)r), lane_id);
        __m256i perm = _mm256_cvtepu8_epi32(
            _mm_loadl_epi64((const __m128i *)col_filter_leftpack_lut[mask]));
        __m256i packed = _mm256_permutevar8x32_epi32(idx, perm);

        /* Always stores 8 lanes; only the first popcount(mask) are valid.
         * out_sel must therefore carry COL_FILTER_SEL_SLACK spare slots. */
        _mm256_storeu_si256((__m256i *)(out_sel + out), packed);
        out += (uint32_t)__builtin_popcount(mask);
    }
#endif /* __AVX2__ */

    for (; r < nrows; r++) {
        int64_t b_val = col_b ? col_b[r] : const_b;
        if (col_filter_cmp_scalar(col_a[r], b_val, cmp_op))
            out_sel[out++] = r;
    }
    return out;
}

int
col_op_filter(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$filter", e.rel);
    if (!out) {
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    const uint8_t *buf = op->filter_expr.data;
    uint32_t bsz = op->filter_expr.size;

    /* Fast path: simple colA CMP CONST or colA CMP colB predicate.
     * Bypasses per-row bytecode dispatch, strtol, and stack init. */
    simple_filter_cmp_t cmp;
    if (buf && bsz > 0 && e.rel->nrows > 0
        && filter_is_simple_cmp(buf, bsz, &cmp) && cmp.col_a < e.rel->ncols
        && (cmp.b_is_const || cmp.col_b < e.rel->ncols)) {
        /* Column-native filter: read directly from contiguous columns[col_a]
         * instead of gathering rows into a flat buffer (6D optimization). */
        const uint32_t ncols = e.rel->ncols;
        const uint32_t nrows = e.rel->nrows;
        int64_t *const *columns = e.rel->columns;
        const int64_t *col_a = columns[cmp.col_a];
        const int64_t *col_b_ptr = (!cmp.b_is_const && cmp.col_b < ncols)
            ? columns[cmp.col_b] : NULL;

        /* Pre-allocate output buffer sized for worst-case (all rows pass) */
        size_t cap = (size_t)nrows * ncols;
        int64_t *tmp = (int64_t *)malloc(cap * sizeof(int64_t));
        if (!tmp) {
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(e.rel);
            return ENOMEM;
        }

        uint32_t nout = 0;
        /*
         * The selection vector only pays for itself when the scan is
         * vectorized.  With the scalar kernel it adds a store, a load and a
         * second traversal per row and measured 0.70x of the fused loop on a
         * 1M-row 8-column relation, so non-AVX2 targets keep the fused form.
         */
#ifdef __AVX2__
        bool use_selection = true;
#else
        bool use_selection = false;
#endif

        if (use_selection) {
            /* COL_FILTER_SEL_SLACK spare slots let the AVX2 left-pack store a
            * full 8-lane vector on its last iteration without overrunning. */
            uint32_t *sel = (uint32_t *)malloc(
                ((size_t)COL_FILTER_TILE + COL_FILTER_SEL_SLACK)
                * sizeof(uint32_t));
            if (!sel) {
                free(tmp);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return ENOMEM;
            }

            /*
             * Scan and materialize a tile at a time.  Scanning the whole
             * column first, then materializing, measured slower than the
             * fused loop on a 1M-row 8-column relation.  Tiling recovers
             * that and bounds the selection vector.
             *
             * Wide relations stay materialize-bound whatever the tile size:
             * at 1M rows, 8 columns and 25% selectivity the scan is under a
             * millisecond of a ~6ms total, so this path still runs at about
             * 0.96x of the fused loop there.  That corner is accepted -- the
             * cost is the sel[] indirection during materialize, which is
             * exactly what the fused loop avoids, and no scan-side change
             * can reach it.
             */
            for (uint32_t base = 0; base < nrows;) {
                uint32_t chunk = nrows - base;
                if (chunk > COL_FILTER_TILE)
                    chunk = COL_FILTER_TILE;

                uint32_t nsel = col_filter_select_rows(col_a + base,
                        col_b_ptr ? col_b_ptr + base : NULL, cmp.const_b,
                        chunk, cmp.cmp_op, sel);

                /*
                 * A predicate that keeps almost every row is materialized
                 * faster by the fused loop: there the row index is the loop
                 * counter, while here every row costs an extra load from
                 * sel[].  Decide once, from the first tile.
                 *
                 * One tile is a sample, so sorted or clustered input whose
                 * first tile is unrepresentative can pick the fused loop and
                 * forgo the vectorized win.  That costs nothing against the
                 * pre-kernel baseline -- it lands at ~1.00x, not below -- so
                 * it is a missed optimization rather than a regression.
                 */
                if (base == 0 && nsel > chunk - (chunk / 8)) {
                    use_selection = false;
                    break;
                }

                for (uint32_t i = 0; i < nsel; i++) {
                    uint32_t src_row = base + sel[i];
                    for (uint32_t c = 0; c < ncols; c++)
                        tmp[(size_t)nout * ncols + c] = columns[c][src_row];
                    nout++;
                }

                /* Advance by the clamped chunk, never by the tile size:
                 * base + COL_FILTER_TILE could wrap past UINT32_MAX. */
                base += chunk;
            }
            free(sel);
        }

        if (!use_selection) {
            nout = 0;
            for (uint32_t r = 0; r < nrows; r++) {
                int64_t b_val = col_b_ptr ? col_b_ptr[r] : cmp.const_b;
                if (col_filter_cmp_scalar(col_a[r], b_val, cmp.cmp_op)) {
                    for (uint32_t c = 0; c < ncols; c++)
                        tmp[(size_t)nout * ncols + c] = columns[c][r];
                    nout++;
                }
            }
        }

        /* Bulk-copy the passing rows into the output relation */
        for (uint32_t r = 0; r < nout; r++) {
            int rc = col_rel_append_row(out, tmp + (size_t)r * ncols);
            if (rc != 0) {
                free(tmp);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return rc;
            }
        }
        free(tmp);
        if (e.owned)
            col_rel_destroy(e.rel);
        return eval_stack_push(stack, out, true);
    }

    /* Slow path: pre-compile expression once, then evaluate per row. */
    col_expr_compiled_t *ce =
        (buf && bsz > 0) ? col_expr_compile(buf, bsz) : NULL;

    /* Row scratch, hoisted out of the loop (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, e.rel->ncols)) {
        col_expr_compiled_free(ce);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *const row = row_rb.ptr;
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        col_rel_row_copy_out(e.rel, r, row);
        int pass;
        if (!buf || bsz == 0) {
            pass = 1;
        } else if (ce) {
            int64_t val = 0;
            pass = (col_eval_expr_compiled(ce, row, e.rel->ncols, &val) == 0)
                       ? (val != 0 ? 1 : 0)
                       : 0; /* on error: reject row */
        } else {
            pass = col_eval_filter_row(buf, bsz, row, e.rel->ncols,
                    sess->intern);
        }
        if (pass) {
            int rc = col_rel_append_row(out, row);
            if (rc != 0) {
                col_row_buf_release(&row_rb);
                col_expr_compiled_free(ce);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return rc;
            }
        }
    }
    col_row_buf_release(&row_rb);
    col_expr_compiled_free(ce);

    if (e.owned)
        col_rel_destroy(e.rel);
    return eval_stack_push(stack, out, true);
}

/* --- Hash join helpers --------------------------------------------------- */

static uint32_t
next_pow2(uint32_t n)
{
    if (n < 16)
        return 16;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

/* --- Right-child filter helper ------------------------------------------- */

/**
 * fill_filtered_rel: apply a serialized filter expression to @rel, appending
 * passing rows into the already-allocated (empty) relation @out.
 *
 * @buf  raw filter expression bytes
 * @bsz  length of @buf
 * @rel  source relation (read-only)
 * @out  destination relation (caller-allocated, must be empty on entry)
 *
 * Returns 0 on success, non-zero (ENOMEM) on allocation failure.
 * On failure @out may be partially filled; caller should destroy it.
 */
static int
fill_filtered_rel(const uint8_t *buf, uint32_t bsz, col_rel_t *rel,
    col_rel_t *out, wl_intern_t *intern)
{
    /* Row scratch, hoisted out of both loops below (#1000). */
    col_row_buf_t rb;
    if (!col_row_buf_init(&rb, rel->ncols))
        return ENOMEM;
    int64_t *row_buf = rb.ptr;

    /* Fast path: simple colA CMP CONST or colA CMP colB predicate */
    simple_filter_cmp_t cmp;
    if (filter_is_simple_cmp(buf, bsz, &cmp)) {
        for (uint32_t r = 0; r < rel->nrows; r++) {
            col_rel_row_copy_out(rel, r, row_buf);
            if (col_filter_cmp_row(row_buf, rel->ncols, &cmp)) {
                if (col_rel_append_row(out, row_buf) != 0) {
                    col_row_buf_release(&rb);
                    return ENOMEM;
                }
            }
        }
        col_row_buf_release(&rb);
        return 0;
    }

    /* Slow path: compile once, evaluate per row */
    col_expr_compiled_t *ce = col_expr_compile(buf, bsz);
    for (uint32_t r = 0; r < rel->nrows; r++) {
        col_rel_row_copy_out(rel, r, row_buf);
        int pass;
        if (ce) {
            int64_t val = 0;
            pass = (col_eval_expr_compiled(ce, row_buf, rel->ncols, &val) == 0)
                       ? (val != 0 ? 1 : 0)
                       : 0; /* fail-closed: reject row on eval error */
        } else {
            int64_t val = 0;
            int err = col_eval_expr_run(buf, bsz, row_buf, rel->ncols, &val,
                    intern);
            pass = (err == 0) ? (val != 0 ? 1 : 0) : 0; /* fail-closed */
        }
        if (pass && col_rel_append_row(out, row_buf) != 0) {
            col_row_buf_release(&rb);
            col_expr_compiled_free(ce);
            return ENOMEM;
        }
    }
    col_row_buf_release(&rb);
    col_expr_compiled_free(ce);
    return 0;
}

/**
 * Apply a serialized filter expression to a relation, returning a new
 * pool-allocated relation containing only the passing rows.
 * The returned relation is owned by pool and freed when the pool resets.
 * Returns NULL on allocation failure.
 */
static col_rel_t *
apply_right_filter(const wl_plan_expr_buffer_t *fexpr, col_rel_t *rel,
    delta_pool_t *pool, wl_intern_t *intern)
{
    col_rel_t *out = col_rel_pool_new_like(pool, "$rfilter", rel);
    if (!out)
        return NULL;

    if (fill_filtered_rel(fexpr->data, fexpr->size, rel, out, intern) != 0) {
        col_rel_destroy(out);
        return NULL;
    }
    return out;
}

/**
 * FNV-1a hash over a byte buffer.  Used to key the filtered-relation cache
 * by filter expression content (Issue #386).
 */
static uint64_t
fnv1a_hash(const uint8_t *buf, uint32_t len)
{
    uint64_t h = UINT64_C(14695981039346656037); /* FNV offset basis */
    for (uint32_t i = 0; i < len; i++) {
        h ^= (uint64_t)buf[i];
        h *= UINT64_C(1099511628211); /* FNV prime */
    }
    return h;
}

/**
 * apply_right_filter_cached: session-level cached variant of apply_right_filter.
 *
 * Looks up (rel_name, filter_hash) in sess->filt_cache.  On hash match a
 * full memcmp of filter bytes is performed to guard against hash collisions.
 * If found and the source relation has not grown since the entry was built,
 * returns the cached filtered relation (owned by the cache; caller must NOT
 * destroy it).
 *
 * If the source grew, the stale entry is rebuilt in-place.  If no entry
 * exists, one is created.  On any allocation failure the function returns
 * NULL (caller handles ENOMEM).
 *
 * The returned pointer is valid until the cache entry is evicted (i.e., until
 * source_nrows changes or the session is destroyed).  Callers that hold it
 * across iterations should re-call each iteration (cheap: cache hit = O(N)
 * linear scan over filt_cache, typically 1-2 entries per session).
 */
static col_rel_t *
apply_right_filter_cached(wl_col_session_t *sess,
    const wl_plan_expr_buffer_t *fexpr, const char *rel_name,
    col_rel_t *rel)
{
    uint64_t fhash = fnv1a_hash(fexpr->data, fexpr->size);

    /* Linear scan: filt_cache is tiny (one entry per unique filter predicate) */
    for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
        if (sess->filt_cache[i].filter_hash != fhash)
            continue;
        if (strcmp(sess->filt_cache[i].rel_name, rel_name) != 0)
            continue;
        /* Full content comparison to guard against hash collisions */
        if (sess->filt_cache[i].filter_size != fexpr->size
            || memcmp(sess->filt_cache[i].filter_data, fexpr->data,
            fexpr->size) != 0)
            continue;
        /* Cache hit */
        if (sess->filt_cache[i].source_nrows == rel->nrows)
            return sess->filt_cache[i].filtered; /* still valid */
        /* Source grew — rebuild in-place */
        if (sess->filt_cache[i].filtered)
            col_rel_destroy(sess->filt_cache[i].filtered);
        sess->filt_cache[i].filtered = col_rel_new_like("$rfilter_cache", rel);
        if (!sess->filt_cache[i].filtered)
            return NULL;
        if (fill_filtered_rel(fexpr->data, fexpr->size, rel,
            sess->filt_cache[i].filtered, sess->intern) != 0) {
            col_rel_destroy(sess->filt_cache[i].filtered);
            sess->filt_cache[i].filtered = NULL;
            return NULL;
        }
        sess->filt_cache[i].source_nrows = rel->nrows;
        return sess->filt_cache[i].filtered;
    }

    /* Cache miss — build a new entry */
    if (sess->filt_cache_count == sess->filt_cache_cap) {
        uint32_t new_cap = sess->filt_cache_cap == 0 ? 4
                                                      : sess->filt_cache_cap *
            2;
        void *tmp = realloc(sess->filt_cache,
                new_cap * sizeof(*sess->filt_cache));
        if (!tmp)
            return NULL;
        sess->filt_cache = tmp;
        sess->filt_cache_cap = new_cap;
    }

    uint32_t idx = sess->filt_cache_count;
    sess->filt_cache[idx].rel_name = strdup(rel_name);
    if (!sess->filt_cache[idx].rel_name)
        return NULL;
    /* Store an owned copy of the filter expression bytes for full key compare */
    sess->filt_cache[idx].filter_data = (uint8_t *)malloc(fexpr->size);
    if (!sess->filt_cache[idx].filter_data) {
        free(sess->filt_cache[idx].rel_name);
        return NULL;
    }
    memcpy(sess->filt_cache[idx].filter_data, fexpr->data, fexpr->size);
    sess->filt_cache[idx].filter_size = fexpr->size;
    sess->filt_cache[idx].filter_hash = fhash;
    sess->filt_cache[idx].source_nrows = 0; /* will be set after fill */
    sess->filt_cache[idx].filtered = col_rel_new_like("$rfilter_cache", rel);
    if (!sess->filt_cache[idx].filtered) {
        free(sess->filt_cache[idx].filter_data);
        free(sess->filt_cache[idx].rel_name);
        return NULL;
    }
    sess->filt_cache_count++;

    /* Fill the new entry */
    col_rel_t *out = sess->filt_cache[idx].filtered;
    if (fill_filtered_rel(fexpr->data, fexpr->size, rel, out,
        sess->intern) != 0) {
        col_rel_destroy(out);
        sess->filt_cache[idx].filtered = NULL;
        /* Leave the entry in cache with NULL filtered; harmless on next lookup */
        return NULL;
    }
    sess->filt_cache[idx].source_nrows = rel->nrows;
    return out;
}

/* --- JOIN ---------------------------------------------------------------- */

#define WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT 4096u

static uint32_t
col_join_parallel_min_left_rows(void)
{
    const char *env = getenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    if (!env || env[0] == '\0')
        return WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT;

    char *endp = NULL;
    errno = 0;
    unsigned long v = strtoul(env, &endp, 10);
    if (endp == env || *endp != '\0' || errno == ERANGE || v > UINT32_MAX)
        return WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT;
    return (uint32_t)v;
}

static bool
col_join_should_parallelize_rows(const wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right)
{
    if (!sess || !left || !right)
        return false;
    if (sess->coordinator || sess->num_workers <= 1)
        return false;
    uint32_t min_left = col_join_parallel_min_left_rows();
    if (min_left == 0)
        min_left = 1;
    return left->nrows >= min_left
           && left->nrows >= sess->num_workers * min_left;
}

static bool
col_join_should_parallelize_cross(const wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right)
{
    if (!col_join_should_parallelize_rows(sess, left, right))
        return false;
    if (right->nrows != 0 && left->nrows > UINT64_MAX / right->nrows)
        return false;
    uint64_t total = (uint64_t)left->nrows * right->nrows;
    return sess->join_output_limit == 0 || total < sess->join_output_limit;
}

static void
col_join_attach_ledger(wl_col_session_t *sess, col_rel_t *rel)
{
    if (!sess || !rel || rel->mem_ledger)
        return;
    rel->mem_ledger = &sess->mem_ledger;
    if (rel->capacity > 0 && rel->ncols > 0)
        wl_mem_ledger_alloc(rel->mem_ledger, WL_MEM_SUBSYS_RELATION,
            (uint64_t)rel->capacity * rel->ncols * sizeof(int64_t));
}

static bool
col_join_output_limit_reached(wl_col_session_t *sess, const col_rel_t *out)
{
    if (!sess)
        return false;
    uint64_t limit = sess->join_output_shared_count
        ? sess->join_output_shared_limit : sess->join_output_limit;
    if (limit == 0)
        return false;
    if (sess->join_output_shared_count) {
        uint64_t n = atomic_fetch_add_explicit(
            sess->join_output_shared_count, 1, memory_order_relaxed) + 1;
        return n >= limit;
    }
    return out && out->nrows >= limit;
}

/*
 * col_join_inloop_backpressure: in-loop soft backpressure check.
 *
 * The RELATION-subsystem 80% threshold is a SOFT signal intended for the
 * multi-worker TDD path, where col_eval_stratum_tdd recovers via EAGAIN
 * (retry with fewer workers).  Coordinator and single-session evaluators
 * have no fallback — propagating EOVERFLOW from this signal turns a soft
 * advisory into an unrecoverable, silent abort (Issue #791: DOOP fails at
 * stratum 54 SubtypeOf eff_iter=1 when RELATION usage crosses 4.7 GB on a
 * 16 GB host even though absolute memory is fine and the cardinality cap
 * is far away).  Restrict this check to worker sessions; the hard
 * cardinality cap (col_join_output_limit_reached) remains the universal
 * safety net.  The pre-join coordinator-side BP check still applies and
 * degrades gracefully to an empty result (Issue #404 design).
 */
static inline bool
col_join_inloop_backpressure(wl_col_session_t *sess, const col_rel_t *out)
{
    if (!sess || !sess->coordinator)
        return false;
    return out && out->nrows > 0 && out->nrows % 10000 == 0
           && wl_mem_ledger_should_backpressure(
        &sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 80);
}

static int
col_join_reserve_exact(col_rel_t *rel, uint32_t nrows)
{
    if (!rel)
        return EINVAL;
    if (nrows <= rel->capacity)
        return 0;
    uint32_t old_cap = rel->capacity;
    if (rel->arena_owned) {
        int64_t **new_cols = col_columns_alloc(rel->ncols, nrows);
        if (!new_cols)
            return ENOMEM;
        for (uint32_t c = 0; c < rel->ncols; c++)
            memcpy(new_cols[c], rel->columns[c],
                sizeof(int64_t) * rel->nrows);
        free(rel->columns);
        rel->columns = new_cols;
        rel->arena_owned = false;
    } else if (rel->columns) {
        if (col_columns_realloc(rel->columns, rel->ncols, nrows) != 0)
            return ENOMEM;
    } else {
        rel->columns = col_columns_alloc(rel->ncols, nrows);
        if (!rel->columns)
            return ENOMEM;
    }
    rel->capacity = nrows;
    if (rel->mem_ledger && rel->ncols > 0)
        wl_mem_ledger_alloc(rel->mem_ledger, WL_MEM_SUBSYS_RELATION,
            (uint64_t)(nrows - old_cap) * rel->ncols * sizeof(int64_t));
    return 0;
}

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    col_rel_t *out;
    const uint32_t *project_indices;
    uint32_t project_count;
    uint64_t begin;
    uint64_t end;
} col_join_cross_ctx_t;

typedef struct {
    uint32_t lr;
    uint32_t rr;
} col_join_pair_ref_t;

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    const col_diff_arrangement_t *darr;
    const uint32_t *lk;
    const uint32_t *rk;
    uint32_t kc;
    const wl_plan_op_t *op;
    col_rel_t *out;
    uint32_t begin;
    uint32_t end;
    uint64_t out_begin;
    uint64_t count;
    uint64_t limit;
    uint32_t *left_hashes;
    col_join_pair_ref_t *pairs;
    uint32_t pair_count;
    uint32_t pair_cap;
    uint32_t pair_cap_limit;
    bool pairs_complete;
    atomic_bool *stop;
    atomic_uint_fast64_t *shared_count;
    int rc;
} col_join_keyed_ctx_t;

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    const uint32_t *lk;
    const uint32_t *rk;
    uint32_t kc;
    const wl_plan_op_t *op;
    const uint32_t *ht_head;
    const uint32_t *ht_next;
    uint32_t nbuckets;
    col_rel_t *out;
    uint32_t begin;
    uint32_t end;
    uint64_t out_begin;
    uint64_t count;
    uint32_t *left_hashes;
} col_semijoin_ctx_t;

static int64_t
col_join_pair_value(const col_rel_t *left, uint32_t lr, const col_rel_t *right,
    uint32_t rr, uint32_t idx);

static WL_OPS_ALWAYS_INLINE uint32_t
col_join_hash_rel_keys(const col_rel_t *rel, uint32_t row,
    const uint32_t *key_cols, uint32_t kc);

static WL_OPS_ALWAYS_INLINE bool
col_join_keys_match_rel(const col_rel_t *left, uint32_t lr,
    const uint32_t *lk, const col_rel_t *right, uint32_t rr,
    const uint32_t *rk, uint32_t kc);

static uint32_t
col_join_output_width(const col_rel_t *left, const col_rel_t *right,
    const wl_plan_op_t *op)
{
    return (op && op->project_count > 0 && op->project_indices)
        ? op->project_count : left->ncols + right->ncols;
}

static void
col_join_write_pair_at(col_rel_t *out, uint64_t out_row,
    const col_rel_t *left, uint32_t lr, const col_rel_t *right, uint32_t rr,
    const uint32_t *project_indices, uint32_t project_count)
{
    if (project_count > 0 && project_indices) {
        for (uint32_t c = 0; c < project_count; c++)
            out->columns[c][out_row] = col_join_pair_value(left, lr, right,
                    rr, project_indices[c]);
    } else {
        for (uint32_t c = 0; c < left->ncols; c++)
            out->columns[c][out_row] = left->columns[c][lr];
        for (uint32_t c = 0; c < right->ncols; c++)
            out->columns[left->ncols + c][out_row] = right->columns[c][rr];
    }
}

static bool
col_join_pair_cache_append(col_join_keyed_ctx_t *ctx, uint32_t lr,
    uint32_t rr)
{
    if (!ctx->pairs_complete)
        return false;
    if (ctx->pair_cap_limit == 0) {
        ctx->pairs_complete = false;
        return false;
    }
    if (ctx->pair_count == ctx->pair_cap) {
        uint32_t new_cap = ctx->pair_cap ? ctx->pair_cap * 2u : 1024u;
        if (new_cap <= ctx->pair_cap) {
            ctx->pairs_complete = false;
            return false;
        }
        if (ctx->pair_cap_limit > 0 && new_cap > ctx->pair_cap_limit)
            new_cap = ctx->pair_cap_limit;
        size_t max_pairs = SIZE_MAX / sizeof(col_join_pair_ref_t);
        if (new_cap <= ctx->pair_cap || (size_t)new_cap > max_pairs) {
            free(ctx->pairs);
            ctx->pairs = NULL;
            ctx->pair_count = 0;
            ctx->pair_cap = 0;
            ctx->pairs_complete = false;
            return false;
        }
        col_join_pair_ref_t *new_pairs = (col_join_pair_ref_t *)realloc(
            ctx->pairs, (size_t)new_cap * sizeof(col_join_pair_ref_t));
        if (!new_pairs) {
            free(ctx->pairs);
            ctx->pairs = NULL;
            ctx->pair_count = 0;
            ctx->pair_cap = 0;
            ctx->pairs_complete = false;
            return false;
        }
        ctx->pairs = new_pairs;
        ctx->pair_cap = new_cap;
    }
    ctx->pairs[ctx->pair_count].lr = lr;
    ctx->pairs[ctx->pair_count].rr = rr;
    ctx->pair_count++;
    return true;
}

static void
col_join_keyed_count_worker_fn(void *arg)
{
    col_join_keyed_ctx_t *ctx = (col_join_keyed_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    const col_diff_arrangement_t *darr = ctx->darr;
    uint64_t count = 0;
    uint64_t reported = 0;

    for (uint32_t lr = ctx->begin; lr < ctx->end
        && (!ctx->stop || !atomic_load_explicit(ctx->stop,
        memory_order_relaxed)); lr++) {
        uint32_t h = col_join_hash_rel_keys(left, lr, ctx->lk, ctx->kc)
            & (darr->nbuckets - 1);
        if (ctx->left_hashes)
            ctx->left_hashes[lr] = h;
        for (uint32_t e = darr->ht_head[h]; e != 0;
            e = darr->ht_next[e - 1]) {
            if (ctx->stop && atomic_load_explicit(ctx->stop,
                memory_order_relaxed))
                break;
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, ctx->lk, right, rr, ctx->rk,
                ctx->kc)) {
                (void)col_join_pair_cache_append(ctx, lr, rr);
                count++;
                if (ctx->limit > 0 && ctx->shared_count
                    && (count - reported) >= 1024u) {
                    uint64_t seen = atomic_fetch_add_explicit(
                        ctx->shared_count, 1024u, memory_order_relaxed)
                        + 1024u;
                    reported += 1024u;
                    if (seen >= ctx->limit && ctx->stop) {
                        atomic_store_explicit(ctx->stop, true,
                            memory_order_relaxed);
                        break;
                    }
                }
            }
        }
    }
    if (count > reported && ctx->shared_count) {
        uint64_t delta = count - reported;
        uint64_t seen = atomic_fetch_add_explicit(ctx->shared_count, delta,
                memory_order_relaxed) + delta;
        if (ctx->limit > 0 && seen >= ctx->limit && ctx->stop)
            atomic_store_explicit(ctx->stop, true, memory_order_relaxed);
    }
    ctx->count = count;
}

static void
col_join_keyed_fill_worker_fn(void *arg)
{
    col_join_keyed_ctx_t *ctx = (col_join_keyed_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    const col_diff_arrangement_t *darr = ctx->darr;
    uint64_t out_row = ctx->out_begin;

    if (ctx->pairs_complete && (uint64_t)ctx->pair_count == ctx->count) {
        for (uint32_t i = 0; i < ctx->pair_count; i++) {
            col_join_write_pair_at(ctx->out, out_row++, left,
                ctx->pairs[i].lr, right, ctx->pairs[i].rr,
                ctx->op->project_indices, ctx->op->project_count);
        }
        return;
    }

    for (uint32_t lr = ctx->begin; lr < ctx->end; lr++) {
        uint32_t h = ctx->left_hashes ? ctx->left_hashes[lr]
            : (col_join_hash_rel_keys(left, lr, ctx->lk, ctx->kc)
            & (darr->nbuckets - 1));
        for (uint32_t e = darr->ht_head[h]; e != 0;
            e = darr->ht_next[e - 1]) {
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, ctx->lk, right, rr, ctx->rk,
                ctx->kc)) {
                col_join_write_pair_at(ctx->out, out_row++, left, lr, right,
                    rr, ctx->op->project_indices, ctx->op->project_count);
            }
        }
    }
}

static bool
col_semijoin_row_found(const col_semijoin_ctx_t *ctx, uint32_t lr)
{
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    uint32_t h = ctx->left_hashes ? ctx->left_hashes[lr]
        : (col_join_hash_rel_keys(left, lr, ctx->lk, ctx->kc)
        & (ctx->nbuckets - 1));

    for (uint32_t e = ctx->ht_head[h]; e != 0; e = ctx->ht_next[e - 1]) {
        uint32_t rr = e - 1;
        if (col_join_keys_match_rel(left, lr, ctx->lk, right, rr, ctx->rk,
            ctx->kc))
            return true;
    }
    return false;
}

static void
col_semijoin_count_worker_fn(void *arg)
{
    col_semijoin_ctx_t *ctx = (col_semijoin_ctx_t *)arg;
    uint64_t count = 0;

    for (uint32_t lr = ctx->begin; lr < ctx->end; lr++) {
        if (ctx->left_hashes)
            ctx->left_hashes[lr] = col_join_hash_rel_keys(ctx->left, lr,
                    ctx->lk, ctx->kc) & (ctx->nbuckets - 1);
        if (col_semijoin_row_found(ctx, lr))
            count++;
    }
    ctx->count = count;
}

static void
col_semijoin_fill_worker_fn(void *arg)
{
    col_semijoin_ctx_t *ctx = (col_semijoin_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    col_rel_t *out = ctx->out;
    uint64_t out_row = ctx->out_begin;
    uint32_t ocols = ctx->op->project_count ? ctx->op->project_count
        : left->ncols;

    for (uint32_t lr = ctx->begin; lr < ctx->end; lr++) {
        if (!col_semijoin_row_found(ctx, lr))
            continue;
        if (ctx->op->project_count > 0 && ctx->op->project_indices) {
            for (uint32_t c = 0; c < ocols; c++) {
                uint32_t si = ctx->op->project_indices[c];
                out->columns[c][out_row] = (si < left->ncols)
                    ? left->columns[si][lr] : 0;
            }
        } else {
            for (uint32_t c = 0; c < left->ncols; c++)
                out->columns[c][out_row] = left->columns[c][lr];
        }
        out_row++;
    }
}

static int64_t
col_join_pair_value(const col_rel_t *left, uint32_t lr, const col_rel_t *right,
    uint32_t rr, uint32_t idx)
{
    if (idx < left->ncols)
        return left->columns[idx][lr];
    idx -= left->ncols;
    return idx < right->ncols ? right->columns[idx][rr] : 0;
}

static void
col_join_cross_fill_worker_fn(void *arg)
{
    col_join_cross_ctx_t *ctx = (col_join_cross_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    col_rel_t *out = ctx->out;
    uint64_t right_rows = right->nrows;

    uint64_t oi = ctx->begin;
    uint32_t lr = (uint32_t)(ctx->begin / right_rows);
    uint32_t rpos = (uint32_t)(ctx->begin % right_rows);
    while (oi < ctx->end) {
        uint32_t rr = right->nrows - 1u - rpos;
        if (ctx->project_count > 0 && ctx->project_indices) {
            for (uint32_t c = 0; c < ctx->project_count; c++)
                out->columns[c][oi] = col_join_pair_value(left, lr, right, rr,
                        ctx->project_indices[c]);
        } else {
            for (uint32_t c = 0; c < left->ncols; c++)
                out->columns[c][oi] = left->columns[c][lr];
            for (uint32_t c = 0; c < right->ncols; c++)
                out->columns[left->ncols + c][oi] = right->columns[c][rr];
        }
        oi++;
        rpos++;
        if (rpos == right->nrows) {
            rpos = 0;
            lr++;
        }
    }
}

static int
col_join_parallel_cross(wl_col_session_t *sess, const col_rel_t *left,
    const col_rel_t *right, const wl_plan_op_t *op, col_rel_t **outp,
    int *out_overflow)
{
    if (!sess || !left || !right || !outp || !*outp)
        return EINVAL;
    if (sess->num_workers <= 1 || right->nrows == 0)
        return EINVAL;

    uint64_t total = (uint64_t)left->nrows * (uint64_t)right->nrows;
    uint64_t emit_total = total;
    if (sess->join_output_limit > 0 && emit_total >= sess->join_output_limit) {
        emit_total = sess->join_output_limit;
        *out_overflow = 1;
    }
    if (emit_total > UINT32_MAX)
        return ENOMEM;

    uint32_t nrows = (uint32_t)emit_total;
    uint32_t active_workers = sess->num_workers > emit_total
        ? (uint32_t)emit_total
        : sess->num_workers;
    if (active_workers <= 1)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(sess, active_workers);
    if (ensure_rc != 0)
        return ensure_rc;

    uint32_t W = active_workers;
    col_rel_t *out = col_rel_new_auto("$join",
            col_join_output_width(left, right, op));
    if (!out)
        return ENOMEM;
    col_join_attach_ledger(sess, out);
    if (col_join_reserve_exact(out, nrows) != 0) {
        col_rel_destroy(out);
        return ENOMEM;
    }
    out->nrows = nrows;

    col_join_cross_ctx_t *ctxs = (col_join_cross_ctx_t *)calloc(
        W, sizeof(col_join_cross_ctx_t));
    if (!ctxs) {
        col_rel_destroy(out);
        return ENOMEM;
    }

    uint64_t chunk = (emit_total + W - 1u) / W;
    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        uint64_t begin = (uint64_t)w * chunk;
        uint64_t end = begin + chunk;
        if (begin > emit_total)
            begin = emit_total;
        if (end > emit_total)
            end = emit_total;
        ctxs[w].left = left;
        ctxs[w].right = right;
        ctxs[w].out = out;
        ctxs[w].project_indices = op ? op->project_indices : NULL;
        ctxs[w].project_count = op ? op->project_count : 0;
        ctxs[w].begin = begin;
        ctxs[w].end = end;
        if (wl_workqueue_submit(sess->wq, col_join_cross_fill_worker_fn,
            &ctxs[w]) != 0) {
            rc = ENOMEM;
            break;
        }
    }
    wl_workqueue_wait_all(sess->wq);
    free(ctxs);
    if (rc != 0) {
        col_rel_destroy(out);
        return rc;
    }

    /* The placeholder $join relation was allocated before this fast path was
     * selected and its initial capacity was never charged to the ledger. */
    (*outp)->mem_ledger = NULL;
    col_rel_destroy(*outp);
    *outp = out;
    return 0;
}

static WL_OPS_ALWAYS_INLINE uint32_t
col_join_hash_rel_keys(const col_rel_t *rel, uint32_t row,
    const uint32_t *key_cols, uint32_t kc)
{
    uint32_t h = 2166136261u;
    if (kc == 1) {
        uint64_t v = (uint64_t)rel->columns[key_cols[0]][row];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
        return h;
    }
    if (kc == 2) {
        uint64_t v = (uint64_t)rel->columns[key_cols[0]][row];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
        v = (uint64_t)rel->columns[key_cols[1]][row];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
        return h;
    }
    for (uint32_t i = 0; i < kc; i++) {
        uint64_t v = (uint64_t)rel->columns[key_cols[i]][row];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
    }
    return h;
}

static WL_OPS_ALWAYS_INLINE bool
col_join_keys_match_rel(const col_rel_t *left, uint32_t lr,
    const uint32_t *lk, const col_rel_t *right, uint32_t rr,
    const uint32_t *rk, uint32_t kc)
{
    if (kc == 1)
        return left->columns[lk[0]][lr] == right->columns[rk[0]][rr];
    if (kc == 2)
        return left->columns[lk[0]][lr] == right->columns[rk[0]][rr]
               && left->columns[lk[1]][lr] == right->columns[rk[1]][rr];
    for (uint32_t k = 0; k < kc; k++)
        if (left->columns[lk[k]][lr] != right->columns[rk[k]][rr])
            return false;
    return true;
}

static int
col_join_append_pair(col_rel_t *out, const col_rel_t *left, uint32_t lr,
    const col_rel_t *right, uint32_t rr, const uint32_t *project_indices,
    uint32_t project_count, int64_t *fallback_row)
{
    if (out->nrows < out->capacity) {
        uint32_t out_row = out->nrows;
        if (out->timestamps)
            memset(&out->timestamps[out_row], 0,
                sizeof(col_delta_timestamp_t));
        if (project_count > 0 && project_indices) {
            for (uint32_t c = 0; c < project_count; c++)
                out->columns[c][out_row] = col_join_pair_value(left, lr, right,
                        rr, project_indices[c]);
        } else {
            for (uint32_t c = 0; c < left->ncols; c++)
                out->columns[c][out_row] = left->columns[c][lr];
            for (uint32_t c = 0; c < right->ncols; c++)
                out->columns[left->ncols + c][out_row]
                    = right->columns[c][rr];
        }
        out->nrows++;
        return 0;
    }

    if (project_count > 0 && project_indices) {
        for (uint32_t c = 0; c < project_count; c++)
            fallback_row[c] = col_join_pair_value(left, lr, right, rr,
                    project_indices[c]);
    } else {
        for (uint32_t c = 0; c < left->ncols; c++)
            fallback_row[c] = left->columns[c][lr];
        for (uint32_t c = 0; c < right->ncols; c++)
            fallback_row[left->ncols + c] = right->columns[c][rr];
    }
    return col_rel_append_row(out, fallback_row);
}

int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    /* right_filtered: non-NULL only when right was pool-allocated by
     * apply_right_filter (non-cached path: antijoin/semijoin callers).
     * For col_op_join we use apply_right_filter_cached; the cache owns
     * the filtered relation and we must NOT destroy it here. */
    col_rel_t *right_filtered = NULL;
    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, join produces empty result (cross-product with nothing).
         * Similar to ANTIJOIN logic (which keeps all left rows on missing right).
         * This can occur in generated plans where optional relations may not exist. */
        uint32_t empty_cols = (op->project_count > 0 && op->project_indices)
            ? op->project_count : left_e.rel->ncols;
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$join_empty", empty_cols);
        if (!out) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        if (left_e.owned)
            col_rel_destroy(left_e.rel);
        return eval_stack_push_delta(stack, out, true, false);
    }

#ifdef WL_PROFILE
    uint64_t _t0_join = now_ns();
    sess->profile.join_calls++;
#endif

    /* Right-side delta substitution controlled by delta_mode:
     * FORCE_DELTA: always substitute delta of right if available; if no
     *              delta exists, short-circuit with an empty result (this
     *              rule copy produces no tuples from this permutation).
     * FORCE_FULL:  never substitute delta; always use full right.
     * AUTO:        heuristic -- substitute delta when left is not already
     *              a delta and right-delta is strictly smaller than full. */
    bool used_right_delta = false;
    if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        /* Issue #472: mirror VARIABLE op retraction-aware pattern —
         * fall back to $r$<name> when retraction_seeded at iteration 0. */
        if (!rdelta && sess->retraction_seeded
            && sess->current_iteration == 0) {
            if (retraction_rel_name(op->right_relation, rdname,
                sizeof(rdname)) == 0)
                rdelta = session_find_rel(sess, rdname);
        }
        if (rdelta && rdelta->nrows > 0) {
            right = rdelta;
            used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded
            || sess->retraction_seeded) {
            /* Iteration > 0, delta-seeded iter 0 (issue #83), or
             * retraction-seeded iter 0 (issue #472):
             * FORCE_DELTA required but delta absent/empty. Short-circuit to
             * empty result — this rule copy produces no tuples from this
             * permutation (correct semi-naive, issue #85). */
            uint32_t ocols = col_join_output_width(left_e.rel, right, op);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            col_rel_t *empty = col_rel_new_auto("$join_empty", ocols);
            if (!empty)
                return ENOMEM;
            int push_rc = eval_stack_push(stack, empty, true);
            if (push_rc != 0)
                col_rel_destroy(empty);
            return push_rc;
        }
        /* else: iteration 0 — no deltas yet, fall through to full right */
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && op->right_relation
        && (!left_e.is_delta || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0))) {
        /* AUTO: original heuristic */
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && (((rdelta->nrows > 0
            && rdelta->nrows < right->nrows) || (rdelta->nrows > 0
            && sess->tdd_subpass_active))
            || (sess->tdd_outbound_only_active
            && sess->current_iteration > 0))) {
            right = rdelta;
            used_right_delta = true;
        }
    }
    /* Issue #472: Retraction right-pass — when retraction_right_pass is set,
     * use the $r$ retraction delta on the right side so that self-join rules
     * derive retractions from full(left) x $r$(right). */
    if (!used_right_delta && sess->retraction_right_pass
        && sess->current_iteration == 0 && op->right_relation) {
        char rdname[256];
        if (retraction_rel_name(op->right_relation, rdname,
            sizeof(rdname)) == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rdname);
            if (rdelta && rdelta->nrows > 0) {
                right = rdelta;
                used_right_delta = true;
            }
        }
    }

    /* Apply constant filter on right child (from FILTER wrappers collected
     * during plan generation).  Use session-level cache (Issue #386): the
     * filtered relation is owned by sess->filt_cache and must NOT be
     * destroyed here.  right_filtered remains NULL for the cached path. */
    if (op->right_filter_expr.size > 0 && op->right_relation
        && !used_right_delta) {
        col_rel_t *filtered = apply_right_filter_cached(sess,
                &op->right_filter_expr, op->right_relation, right);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        /* right_filtered stays NULL: cache owns the relation */
    } else if (op->right_filter_expr.size > 0) {
        /* Delta path or no relation name: fall back to pool-allocated filter */
        col_rel_t *filtered = apply_right_filter(&op->right_filter_expr, right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    /* Materialization cache: reuse previous join result when available.
     * Works with both stable (borrowed) and worker-owned relations since
     * the cache key is based on content hash, not ownership. This enables
     * cache reuse in K-fusion worker sessions, eliminating redundant joins. */
    bool projected_join = op->project_count > 0 && op->project_indices;
    if (op->materialized && !projected_join) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached) {
#ifdef WL_PROFILE
            sess->profile.join_cache_hit_ns += now_ns() - _t0_join;
#endif
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return eval_stack_push_delta(stack, cached, false,
                       left_e.is_delta || used_right_delta);
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;

    /* Resolve key column positions */
    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "JOIN",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right", "JOIN",
                op->right_relation);
    }

    uint32_t ocols = col_join_output_width(left, right, op);
    /* Materialized results outlive the current delta-pool reset while they
     * remain in mat_cache, so cache-owned joins must be heap allocated. */
    col_rel_t *out = (op->materialized && !projected_join)
        ? col_rel_new_auto("$join", ocols)
        : col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$join", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    /* Attach ledger so row growth is tracked under RELATION subsystem */
    col_join_attach_ledger(sess, out);

    /* Backpressure check (Issue #224): when RELATION subsystem reaches >= 80%
     * of its budget, skip row generation and push an empty result instead of
     * risking EOVERFLOW (rc=84).  Evaluation continues with gracefully
     * degraded (incomplete) results rather than failing entirely.
     *
     * TDD workers (coordinator != NULL) skip this pre-join check (Issue #404):
     * returning empty results before row generation causes silent correctness
     * bugs — zero join output leads to premature fixed-point convergence.
     * Workers still have in-loop backpressure + join_output_limit as hard
     * safety nets.  Coordinator sessions retain full pre-join protection. */
    if (wl_mem_ledger_should_backpressure(&sess->mem_ledger,
        WL_MEM_SUBSYS_RELATION, 80)
        && !sess->coordinator) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return eval_stack_push(stack, out, true);
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* BOOLEAN SPECIALIZATION (Issue #62): Fast-path for unary relations.
     * When right relation is unary (ncols == 1) and single join key,
     * use O(1) hash-set membership test instead of merge-join.
     * Profiling shows 37.7% of DOOP-class joins are unary; this path
     * provides ~30-40% speedup for such workloads. */
    bool right_is_unary = (right->ncols == 1);
    bool left_is_unary = (left->ncols == 1);
#ifdef WL_PROFILE
    if ((right_is_unary || left_is_unary) && kc == 1)
        sess->profile.join_unary++;
#endif
    if ((right_is_unary || left_is_unary) && kc == 1) {
        /* build: unary side as hash set; probe: non-unary side iterated.
         * When both are unary, right is preferred as build side. */
        col_rel_t *build = right_is_unary ? right : left;
        col_rel_t *probe = right_is_unary ? left : right;
        uint32_t build_kcol = right_is_unary ? rk[0] : lk[0];
        uint32_t probe_kcol = right_is_unary ? lk[0] : rk[0];

        /* Build hash set from the unary relation's single column. */
        uint32_t nbuckets = next_pow2(build->nrows > 0 ? build->nrows * 2 : 1);
        uint32_t *ht_head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
        uint32_t *ht_next = (uint32_t *)malloc(
            (build->nrows > 0 ? build->nrows : 1) * sizeof(uint32_t));
        if (!ht_head || !ht_next) {
            free(ht_head);
            free(ht_next);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        for (uint32_t bi = 0; bi < build->nrows; bi++) {
            int64_t key = col_rel_get(build, bi, build_kcol);
            /* Inline FNV-1a hash for single int64 value */
            uint32_t h = 2166136261u;
            uint64_t v = (uint64_t)key;
            h ^= (uint32_t)(v & 0xffffffff);
            h *= 16777619u;
            h ^= (uint32_t)(v >> 32);
            h *= 16777619u;
            h &= (nbuckets - 1);
            ht_next[bi] = ht_head[h];
            ht_head[h] = bi + 1; /* 1-based; 0 = end of chain */
        }

        /* Probe: iterate non-unary side, test membership in hash set. */
        int join_rc = 0;
        for (uint32_t pr = 0; pr < probe->nrows && join_rc == 0; pr++) {
            int64_t pkey = probe->columns[probe_kcol][pr];
            /* Inline FNV-1a hash for single int64 value */
            uint32_t h = 2166136261u;
            uint64_t v = (uint64_t)pkey;
            h ^= (uint32_t)(v & 0xffffffff);
            h *= 16777619u;
            h ^= (uint32_t)(v >> 32);
            h *= 16777619u;
            h &= (nbuckets - 1);

            for (uint32_t e = ht_head[h]; e != 0; e = ht_next[e - 1]) {
                uint32_t bi = e - 1;
                int64_t bkey
                    = col_rel_get(build, bi, build_kcol);
                if (pkey != bkey)
                    continue;
                uint32_t lr = right_is_unary ? pr : bi;
                uint32_t rr = right_is_unary ? bi : pr;
                join_rc = col_join_append_pair(out, left, lr, right, rr,
                        op->project_indices, op->project_count, tmp);
                if (join_rc != 0) {
                    fprintf(stderr,
                        "ERROR: col_rel_append_row failed with rc=%d at "
                        "unary join\n",
                        join_rc);
                    break;
                }
                if (col_join_output_limit_reached(sess, out)
                    || col_join_inloop_backpressure(sess, out)) {
                    fprintf(stderr,
                        "join output limit reached: %u rows "
                        "(limit=%llu)\n",
                        out->nrows,
                        (unsigned long long)sess->join_output_limit);
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }

        free(ht_head);
        free(ht_next);
        if (join_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Unary join completed, out->nrows=%u",
            out->nrows);
    } else {
        /* Standard merge-join for non-unary relations. */
        /* Hash join: use persistent arrangement for the full right relation;
         * fall back to an ephemeral hash table for delta substitution or when
         * the arrangement cannot be allocated. */
        col_arrangement_t *arr = NULL;
        uint32_t nbuckets_ep = 0;
        uint32_t *ht_head_ep = NULL;
        uint32_t *ht_next_ep = NULL;

        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Standard merge-join starting - left=%u rows, right=%u rows, kc=%u",
            left->nrows, right->nrows, kc);

        if (!used_right_delta && op->right_relation && kc > 0) {
            if (op->right_filter_expr.size == 0) {
                arr = col_session_get_arrangement(&sess->base,
                        op->right_relation, rk, kc);
            } else {
                /* Issue #433: filtered right arrangement cache.
                 * `right` is the cached filtered relation from filt_cache;
                 * filt_arr persists across sub-passes to avoid ephemeral
                 * hash table rebuild on every semi-naive iteration. */
                if (!sess->coordinator) {
                    uint64_t fhash = fnv1a_hash(op->right_filter_expr.data,
                            op->right_filter_expr.size);
                    arr = col_session_get_filt_arrangement(sess,
                            op->right_relation, fhash, right, rk, kc);
                }
            }
        } else if (used_right_delta && op->right_relation && kc > 0)
            arr = col_session_get_delta_arrangement(sess, op->right_relation,
                    right, rk, kc);

        if (arr)
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Using persistent arrangement");
        else
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "No arrangement available, will use ephemeral hash table");

        if (!arr) {
            /* Ephemeral hash table (delta path or arrangement unavailable). */
            nbuckets_ep = next_pow2(right->nrows > 0 ? right->nrows * 2 : 1);
            ht_head_ep = (uint32_t *)calloc(nbuckets_ep, sizeof(uint32_t));
            ht_next_ep = (uint32_t *)malloc(
                (right->nrows > 0 ? right->nrows : 1) * sizeof(uint32_t));
            if (!ht_head_ep || !ht_next_ep) {
                fprintf(stderr,
                    "ERROR: Ephemeral hash table allocation failed "
                    "(nbuckets=%u)\n",
                    nbuckets_ep);
                free(ht_head_ep);
                free(ht_next_ep);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Ephemeral hash table created - nbuckets=%u",
                nbuckets_ep);
            for (uint32_t rr = 0; rr < right->nrows; rr++) {
                uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
                    & (nbuckets_ep - 1);
                ht_next_ep[rr] = ht_head_ep[h];
                ht_head_ep[h] = rr + 1; /* 1-based; 0 = end of chain */
            }
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Ephemeral hash table built successfully");
        }

        /* key_row scratch buffer for arrangement probe: values placed at rk[]
         * positions so col_arrangement_find_first() matches correctly. */
        int64_t *key_row = NULL;
        if (arr) {
            key_row = (int64_t *)malloc(
                sizeof(int64_t) * (right->ncols > 0 ? right->ncols : 1));
            if (!key_row) {
                free(ht_head_ep);
                free(ht_next_ep);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
        }

        int join_rc = 0;
        int join_overflow = 0;
        if (kc == 0 && col_join_should_parallelize_cross(sess, left, right)) {
            join_rc = col_join_parallel_cross(sess, left, right, op, &out,
                    &join_overflow);
            if (join_rc == EINVAL)
                join_rc = 0;
            else if (join_rc == 0 && join_overflow)
                join_rc = EOVERFLOW;
        } else for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
                if (arr) {
                    /* Arrangement probe: fill key_row at right-side positions. */
                    for (uint32_t k = 0; k < kc; k++)
                        key_row[rk[k]] = left->columns[lk[k]][lr];
                    uint32_t rr = col_arrangement_find_first(arr,
                            right->columns,
                            right->ncols, key_row);
                    while (rr != UINT32_MAX && join_rc == 0) {
                        /* Verify key match: find_next may return collision rows. */
                        if (col_join_keys_match_rel(left, lr, lk, right, rr, rk,
                            kc)) {
                            join_rc = col_join_append_pair(out, left, lr, right,
                                    rr, op->project_indices, op->project_count,
                                    tmp);
                            if (join_rc != 0) {
                                fprintf(stderr,
                                    "ERROR: col_rel_append_row failed in "
                                    "arrangement probe with rc=%d\n",
                                    join_rc);
                                break;
                            }
                            if (col_join_output_limit_reached(sess, out)
                                || col_join_inloop_backpressure(sess, out)) {
                                fprintf(
                                    stderr,
                                    "join output limit reached: %u rows "
                                    "(limit=%llu)\n",
                                    out->nrows,
                                    (unsigned long long)sess->join_output_limit);
                                join_rc = EOVERFLOW;
                            }
                        }
                        rr = col_arrangement_find_next(arr, rr);
                    }
                } else {
                    /* Ephemeral hash probe. */
                    uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
                        & (nbuckets_ep - 1);
                    for (uint32_t e = ht_head_ep[h]; e != 0;
                        e = ht_next_ep[e - 1]) {
                        uint32_t rr = e - 1;
                        if (!col_join_keys_match_rel(left, lr, lk, right, rr,
                            rk,
                            kc))
                            continue;
                        join_rc = col_join_append_pair(out, left, lr, right, rr,
                                op->project_indices, op->project_count, tmp);
                        if (join_rc != 0) {
                            fprintf(stderr,
                                "ERROR: col_rel_append_row failed in ephemeral "
                                "hash probe with rc=%d\n",
                                join_rc);
                            break;
                        }
                        if (col_join_output_limit_reached(sess, out)
                            || col_join_inloop_backpressure(sess, out)) {
                            fprintf(stderr,
                                "join output limit reached: %u rows "
                                "(limit=%llu)\n",
                                out->nrows,
                                (unsigned long long)sess->join_output_limit);
                            join_rc = EOVERFLOW;
                            break;
                        }
                    }
                }
            }

        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Merge-join loop completed, out->nrows=%u, rc=%d",
            out->nrows, join_rc);

        free(key_row);
        free(ht_head_ep);
        free(ht_next_ep);
        if (join_rc != 0) {
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Merge-join failed with rc=%d, out->nrows=%u",
                join_rc, out->nrows);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG, "Merge-join succeeded");
    }

    free(tmp);
    free(lk);
    free(rk);
    /* Propagate delta flag: result is a delta if left was delta OR we used
     * right-delta. This ensures subsequent JOINs in the same rule plan know
     * whether to apply right-delta (they should NOT if we already used one). */
    bool result_is_delta = projected_join ? false
        : (left_e.is_delta || used_right_delta);

    /* Populate materialization cache when hint is set.
     * Works with both stable and worker-owned relations.
     * Cache takes ownership of out; we push a borrowed reference.
     * This enables K-fusion workers to cache and reuse intermediate joins,
     * reducing redundant computation across the K worker copies. */
    if (op->materialized && !projected_join) {
        col_mat_cache_insert(&sess->mat_cache, left, right, out);
#ifdef WL_PROFILE
        if (out->nrows == 0)
            sess->profile.join_empty_out++;
        sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
        if (right_filtered)
            col_rel_destroy(right_filtered);
        if (left_e.owned)
            col_rel_destroy(left);
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
    if (left_e.owned)
        col_rel_destroy(left);
#ifdef WL_PROFILE
    if (out->nrows == 0)
        sess->profile.join_empty_out++;
    sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push_delta(stack, out, true, result_is_delta);
}

/* --- ANTIJOIN ------------------------------------------------------------ */

int
col_op_antijoin(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right_filtered = NULL;
    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, antijoin keeps all left rows */
        return eval_stack_push(stack, left_e.rel, left_e.owned);
    }

    /* Issue #386: antijoin filter caching is not yet implemented.
     * Antijoin always uses an ephemeral pool-allocated filtered relation, so
     * the per-iteration filter cost is O(N) — acceptable for current workloads
     * but a candidate for follow-up optimization. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered
            = apply_right_filter(&op->right_filter_expr, right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "ANTIJOIN",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right", "ANTIJOIN",
                op->right_relation);
    }

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$antijoin", left);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* Hash antijoin: build hash set from right, iterate left. */
    uint32_t aj_nbuckets = next_pow2(right->nrows > 0 ? right->nrows * 2 : 1);
    uint32_t *aj_head = (uint32_t *)calloc(aj_nbuckets, sizeof(uint32_t));
    uint32_t *aj_next
        = (uint32_t *)malloc((right->nrows + 1) * sizeof(uint32_t));
    if (!aj_head || !aj_next) {
        free(aj_head);
        free(aj_next);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
            & (aj_nbuckets - 1);
        aj_next[rr] = aj_head[h];
        aj_head[h] = rr + 1;
    }
    int aj_rc = 0;
    int64_t *lrow_buf = (int64_t *)malloc(
        sizeof(int64_t) * (left->ncols ? left->ncols : 1));
    if (!lrow_buf) {
        aj_rc = ENOMEM;
        goto antijoin_done;
    }
    for (uint32_t lr = 0; lr < left->nrows && aj_rc == 0; lr++) {
        uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
            & (aj_nbuckets - 1);
        bool found = false;
        for (uint32_t e = aj_head[h]; e != 0 && !found; e = aj_next[e - 1]) {
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                found = true;
        }
        if (!found) {
            for (uint32_t c = 0; c < left->ncols; c++)
                lrow_buf[c] = left->columns[c][lr];
            aj_rc = col_rel_append_row(out, lrow_buf);
        }
    }
    free(lrow_buf);
antijoin_done:
    free(aj_head);
    free(aj_next);
    if (aj_rc != 0) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return aj_rc;
    }
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push(stack, out, true);
}

/* --- CONCAT -------------------------------------------------------------- */

int
col_op_concat(eval_stack_t *stack, wl_col_session_t *sess)
{
    if (stack->top < 2)
        return 0; /* single-item passthrough for K-copy boundary marker */

    eval_entry_t b_e = eval_stack_pop(stack);
    eval_entry_t a_e = eval_stack_pop(stack);
    col_rel_t *a = a_e.rel;
    col_rel_t *b = b_e.rel;

    if (!a || !b || a->ncols != b->ncols) {
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        return EINVAL;
    }

#ifdef WL_PROFILE
    uint64_t _t0_concat = now_ns();
    sess->profile.concat_calls++;
#endif

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$concat", a);
    if (!out) {
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        return ENOMEM;
    }

    int rc = col_rel_append_all(out, a, NULL);
    if (rc == 0)
        rc = col_rel_append_all(out, b, NULL);

    if (rc != 0) {
        if (a_e.seg_boundaries)
            free(a_e.seg_boundaries);
        if (b_e.seg_boundaries)
            free(b_e.seg_boundaries);
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        col_rel_destroy(out);
        return rc;
    }

    /* Track segment boundaries for K-way merge optimization. */
    uint32_t left_segs = a_e.seg_count > 0 ? a_e.seg_count : 1;
    uint32_t right_segs = b_e.seg_count > 0 ? b_e.seg_count : 1;
    uint32_t total_segs = left_segs + right_segs;

    uint32_t *out_boundaries
        = (uint32_t *)malloc((total_segs + 1) * sizeof(uint32_t));
    if (!out_boundaries) {
        if (a_e.seg_boundaries)
            free(a_e.seg_boundaries);
        if (b_e.seg_boundaries)
            free(b_e.seg_boundaries);
        if (a_e.owned)
            col_rel_destroy(a);
        if (b_e.owned)
            col_rel_destroy(b);
        col_rel_destroy(out);
        return ENOMEM;
    }

    /* Copy left boundaries */
    if (a_e.seg_boundaries != NULL) {
        memcpy(out_boundaries, a_e.seg_boundaries,
            (left_segs + 1) * sizeof(uint32_t));
    } else {
        out_boundaries[0] = 0;
        out_boundaries[1] = a->nrows;
    }

    /* Adjust and append right boundaries */
    uint32_t right_offset = a->nrows;
    if (b_e.seg_boundaries != NULL) {
        for (uint32_t i = 0; i <= right_segs; i++)
            out_boundaries[left_segs + i]
                = b_e.seg_boundaries[i] + right_offset;
    } else {
        out_boundaries[left_segs] = right_offset;
        out_boundaries[left_segs + 1] = out->nrows;
    }

    /* Clean up input boundaries */
    if (a_e.seg_boundaries)
        free(a_e.seg_boundaries);
    if (b_e.seg_boundaries)
        free(b_e.seg_boundaries);

    if (a_e.owned)
        col_rel_destroy(a);
    if (b_e.owned)
        col_rel_destroy(b);

#ifdef WL_PROFILE
    if (out->nrows == 0)
        sess->profile.concat_empty_out++;
    sess->profile.concat_ns += now_ns() - _t0_concat;
#endif

    rc = eval_stack_push(stack, out, true);
    if (rc != 0) {
        free(out_boundaries);
        col_rel_destroy(out);
        return rc;
    }

    /* Attach boundary metadata to the pushed entry */
    stack->items[stack->top - 1].seg_boundaries = out_boundaries;
    stack->items[stack->top - 1].seg_count = total_segs;
    return 0;
}

/* --- CONSOLIDATE --------------------------------------------------------- */

/* Issue #197: SIMD row comparison functions moved here so kway_row_cmp and
 * all callers in the consolidate/merge paths use the optimized dispatcher. */

/* Helper: lexicographic int64_t row comparison (-1/0/+1).
 * Compares rows a and b with ncols columns using int64_t values (not bytes).
 * Required for correct little-endian int64_t comparisons.
 */
static int UNUSED
row_cmp_lex(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        if (a[c] < b[c])
            return -1;
        if (a[c] > b[c])
            return 1;
    }
    return 0;
}

#ifdef __AVX2__
/* row_cmp_simd_avx2 - AVX2-accelerated lexicographic int64_t row comparison.
 *
 * Compares rows a and b (each ncols int64_t values) and returns -1, 0, or +1,
 * identical in semantics to row_cmp_lex().  Processes 4 elements per SIMD
 * iteration then falls back to scalar for the remainder.
 *
 * No alignment assumptions: unaligned loads (_mm256_loadu_si256) are used.
 */
static inline int
row_cmp_simd_avx2(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    uint32_t i = 0;

    /* Process 4 int64_t elements per iteration (256-bit vectors). */
    for (; i + 4 <= ncols; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i *)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i *)(b + i));

        /* eq_mask: 0xFFFFFFFFFFFFFFFF for equal lanes, 0 otherwise. */
        __m256i eq_mask = _mm256_cmpeq_epi64(va, vb);

        /* Collapse equality mask to 4-bit scalar (one bit per byte-group of 8).
         * movemask gives one bit per byte; equal lane -> 8 bits set -> 0xFF.
         * We check for a fully-equal lane by looking at 8-bit groups. */
        int eq_bits = _mm256_movemask_epi8(eq_mask); /* 32 bits, 8 per lane */

        if (eq_bits == (int)0xFFFFFFFF) {
            /* All 4 lanes are equal; continue to next chunk. */
            continue;
        }

        /* At least one lane differs.  Find the lowest-index differing lane.
         * eq_bits has 8 consecutive bits set for an equal lane.
         * Lane k occupies bits [8k .. 8k+7].  A differing lane has at least
         * one of those bits clear, so (~eq_bits) has a set bit in that range.
         */
        int neq = ~eq_bits;
        /* ctz gives the position of the first differing byte; divide by 8
         * gives the lane index within this 4-element chunk. */
        int lane = __builtin_ctz((unsigned int)neq) / 8;
        int64_t av = a[i + (uint32_t)lane];
        int64_t bv = b[i + (uint32_t)lane];
        return (av < bv) ? -1 : 1;
    }

    /* Scalar fallback for the remaining ncols % 4 elements. */
    for (; i < ncols; i++) {
        if (a[i] < b[i])
            return -1;
        if (a[i] > b[i])
            return 1;
    }
    return 0;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
/* row_cmp_simd_neon - NEON-accelerated lexicographic int64_t row comparison.
 *
 * Compares rows a and b (each ncols int64_t values) and returns -1, 0, or +1,
 * identical in semantics to row_cmp_lex().  Processes 2 elements per SIMD
 * iteration then falls back to scalar for the remainder.
 *
 * No alignment assumptions: unaligned loads (vld1q_s64) are used.
 */
static inline int
row_cmp_simd_neon(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    uint32_t i = 0;

    /* Process 2 int64_t elements per iteration (128-bit vectors). */
    for (; i + 2 <= ncols; i += 2) {
        int64x2_t va = vld1q_s64(a + i);
        int64x2_t vb = vld1q_s64(b + i);

        /* eq_mask: all-ones (0xFFFFFFFFFFFFFFFF) for equal lanes, 0 otherwise. */
        uint64x2_t eq_mask = vceqq_s64(va, vb);

        /* Optimized lane extraction: check lane 0 first, avoid ternary operator.
         * This improves instruction scheduling and reduces branch prediction stalls. */
        uint64_t eq0 = vgetq_lane_u64(eq_mask, 0);
        if (!eq0) {
            /* Lane 0 differs; extract and compare. */
            int64_t av = vgetq_lane_s64(va, 0);
            int64_t bv = vgetq_lane_s64(vb, 0);
            return (av < bv) ? -1 : 1;
        }

        /* Lane 0 is equal; check lane 1. */
        uint64_t eq1 = vgetq_lane_u64(eq_mask, 1);
        if (eq1) {
            /* Both lanes equal; continue to next pair. */
            continue;
        }

        /* Lane 1 differs; extract and compare. */
        int64_t av = vgetq_lane_s64(va, 1);
        int64_t bv = vgetq_lane_s64(vb, 1);
        return (av < bv) ? -1 : 1;
    }

    /* Scalar fallback for the remaining ncols % 2 element. */
    if (i < ncols) {
        if (a[i] < b[i])
            return -1;
        if (a[i] > b[i])
            return 1;
    }
    return 0;
}
#endif /* __ARM_NEON__ */

/* Dispatcher: Select best row comparison at compile time.
 * Automatically chooses AVX2, NEON, or scalar fallback.
 */
#ifdef __AVX2__
#define row_cmp_optimized row_cmp_simd_avx2
#elif defined(__ARM_NEON__)
#define row_cmp_optimized row_cmp_simd_neon
#else
#define row_cmp_optimized row_cmp_lex
#endif

/* Issue #279: Fully-unrolled specializations for the two most common widths. */
static inline int
row_cmp_ncols2(const int64_t *a, const int64_t *b)
{
    if (a[0] != b[0])
        return (a[0] < b[0]) ? -1 : 1;
    if (a[1] != b[1])
        return (a[1] < b[1]) ? -1 : 1;
    return 0;
}

static inline int
row_cmp_ncols4(const int64_t *a, const int64_t *b)
{
    if (a[0] != b[0])
        return (a[0] < b[0]) ? -1 : 1;
    if (a[1] != b[1])
        return (a[1] < b[1]) ? -1 : 1;
    if (a[2] != b[2])
        return (a[2] < b[2]) ? -1 : 1;
    if (a[3] != b[3])
        return (a[3] < b[3]) ? -1 : 1;
    return 0;
}

static inline int
row_cmp_dispatch(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    if (ncols == 2)
        return row_cmp_ncols2(a, b);
    if (ncols == 4)
        return row_cmp_ncols4(a, b);
    /* Fallback: call the compile-time SIMD selection directly. */
#ifdef __AVX2__
    return row_cmp_simd_avx2(a, b, ncols);
#elif defined(__ARM_NEON__)
    return row_cmp_simd_neon(a, b, ncols);
#else
    return row_cmp_lex(a, b, ncols);
#endif
}

/* Issue #197: kway_row_cmp now delegates to row_cmp_dispatch so all 10+
 * call sites in the consolidate/merge hot paths use the SIMD dispatcher.
 * Issue #279: row_cmp_dispatch adds loop-free fast paths for ncols=2/4. */
static inline int
kway_row_cmp(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    return row_cmp_dispatch(a, b, ncols);
}

/* Compare a relation row against a raw row buffer (for merge operations
 * where one operand is in a temp buffer). Phase B, Issue #330.
 * Direct column access for cache efficiency (Issue #334). */
static inline int
col_rel_row_cmp_raw(const col_rel_t *r, uint32_t row_idx,
    const int64_t *raw_row)
{
    uint32_t ncols = r->ncols;
    for (uint32_t c = 0; c < ncols; c++) {
        int64_t va = r->columns[c][row_idx];
        int64_t vb = raw_row[c];
        if (va < vb)
            return -1;
        if (va > vb)
            return 1;
    }
    return 0;
}

/*
 * col_op_consolidate_hash_dedup - Hash-based deduplication for consolidation.
 *
 * When the total row count greatly exceeds the unique count (common in
 * recursive Datalog joins), hash-based dedup is O(N) vs O(N * passes)
 * for radix sort + O(N) merge.  After dedup, the small unique set is
 * sorted with radix sort.
 *
 * Returns 0 on success, -1 to signal fallback to sort+merge (too many
 * uniques or allocation failure).
 */
static int
col_op_consolidate_hash_dedup(col_rel_t *rel)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Hash table: open addressing, power-of-2 capacity */
    uint32_t ht_cap = 8192;
    uint32_t ht_mask = ht_cap - 1;
    int64_t *ht_vals = (int64_t *)malloc((size_t)ht_cap * row_bytes);
    uint8_t *ht_used = (uint8_t *)calloc(ht_cap, 1);
    if (!ht_vals || !ht_used) {
        free(ht_vals);
        free(ht_used);
        return -1;
    }

    /* Unique row buffer (row-major flat array) */
    uint32_t uniq_cap = 4096;
    uint32_t uniq_count = 0;
    int64_t *uniq_buf = (int64_t *)malloc((size_t)uniq_cap * row_bytes);
    if (!uniq_buf) {
        free(ht_vals);
        free(ht_used);
        return -1;
    }

    int64_t _rb[COL_STACK_MAX];
    int64_t *rb = nc <= COL_STACK_MAX ? _rb
        : (int64_t *)malloc((size_t)nc * sizeof(int64_t));
    if (!rb) {
        free(ht_vals);
        free(ht_used);
        free(uniq_buf);
        return -1;
    }

    for (uint32_t r = 0; r < nr; r++) {
        /* Read row from column-major storage */
        for (uint32_t c = 0; c < nc; c++)
            rb[c] = rel->columns[c][r];

        /* FNV-1a hash */
        uint64_t h = 14695981039346656037ULL;
        for (uint32_t c = 0; c < nc; c++) {
            h ^= (uint64_t)rb[c];
            h *= 1099511628211ULL;
        }

        uint32_t slot = (uint32_t)(h & ht_mask);
        bool found = false;
        while (ht_used[slot]) {
            int64_t *sv = ht_vals + (size_t)slot * nc;
            if (memcmp(sv, rb, row_bytes) == 0) {
                found = true;
                break;
            }
            slot = (slot + 1) & ht_mask;
        }

        if (!found) {
            /* Check if rehash needed (load > 50%) */
            if (uniq_count * 2 >= ht_cap) {
                /* If unique count already > nr/4, hash dedup not worth it */
                if (uniq_count > nr / 4) {
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    return -1;
                }

                /* Rehash to 2x capacity */
                uint32_t new_cap = ht_cap * 2;
                uint32_t new_mask = new_cap - 1;
                int64_t *new_vals
                    = (int64_t *)malloc((size_t)new_cap * row_bytes);
                uint8_t *new_used
                    = (uint8_t *)calloc(new_cap, 1);
                if (!new_vals || !new_used) {
                    free(new_vals);
                    free(new_used);
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    return -1;
                }

                for (uint32_t s = 0; s < ht_cap; s++) {
                    if (ht_used[s]) {
                        int64_t *sv = ht_vals + (size_t)s * nc;
                        uint64_t rh = 14695981039346656037ULL;
                        for (uint32_t c2 = 0; c2 < nc; c2++) {
                            rh ^= (uint64_t)sv[c2];
                            rh *= 1099511628211ULL;
                        }
                        uint32_t ns = (uint32_t)(rh & new_mask);
                        while (new_used[ns])
                            ns = (ns + 1) & new_mask;
                        memcpy(new_vals + (size_t)ns * nc, sv,
                            row_bytes);
                        new_used[ns] = 1;
                    }
                }

                free(ht_vals);
                free(ht_used);
                ht_vals = new_vals;
                ht_used = new_used;
                ht_cap = new_cap;
                ht_mask = new_mask;

                /* Re-probe for current row in new table */
                slot = (uint32_t)(h & ht_mask);
                while (ht_used[slot])
                    slot = (slot + 1) & ht_mask;
            }

            /* Insert into hash table */
            memcpy(ht_vals + (size_t)slot * nc, rb, row_bytes);
            ht_used[slot] = 1;

            /* Grow unique buffer if needed */
            if (uniq_count >= uniq_cap) {
                uniq_cap *= 2;
                int64_t *nb = (int64_t *)realloc(uniq_buf,
                        (size_t)uniq_cap * row_bytes);
                if (!nb) {
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    return -1;
                }
                uniq_buf = nb;
            }

            memcpy(uniq_buf + (size_t)uniq_count * nc, rb, row_bytes);
            uniq_count++;
        }
    }

    if (rb != _rb) free(rb);
    free(ht_vals);
    free(ht_used);

    /* Write unique rows back to relation */
    for (uint32_t r = 0; r < uniq_count; r++)
        col_rel_row_copy_in(rel, r, uniq_buf + (size_t)r * nc);
    rel->nrows = uniq_count;
    free(uniq_buf);

    /* Sort the small unique set */
    if (uniq_count > 1) {
        col_rel_radix_sort(rel, 0, uniq_count);

        /* Final dedup pass (hash guarantees value-uniqueness, but ensure
         * sorted order has no adjacent duplicates for determinism) */
        uint32_t out = 1;
        for (uint32_t i = 1; i < uniq_count; i++) {
            if (col_rel_row_cmp(rel, i - 1, i) != 0) {
                if (out != i)
                    col_rel_row_move(rel, out, i);
                out++;
            }
        }
        rel->nrows = out;
    }

    return 0;
}

/*
 * col_op_consolidate_kway_merge - K-way merge with per-segment sort and dedup.
 *
 * Sorts each segment in-place, then merges K sorted segments using a min-heap.
 * Deduplicates on-the-fly during merge. Writes merged result back into rel.
 *
 * For K=1: just sort + dedup in-place.
 * For K=2: optimized 2-way merge (no heap overhead).
 * For K>=3: min-heap merge with O(M log K) comparisons.
 *
 * @rel            Relation containing K concatenated segments.
 * @seg_boundaries Array of (seg_count+1) offsets [s0, s1, ..., sK].
 * @seg_count      Number of segments K.
 * @return         0 on success, ENOMEM on allocation failure.
 */
int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
    uint32_t seg_count)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1)
        return 0;

    /* Hash-based dedup for large datasets (#369): O(N) scan + O(U log U) sort
     * where U is the unique count.  When U << N (common in recursive Datalog
     * joins), this is much faster than sorting all N rows. */
    if (nr > 10000) {
        int rc = col_op_consolidate_hash_dedup(rel);
        if (rc == 0)
            return 0;
        /* rc == -1: too many uniques or alloc failure, fall through */
    }

    /* Sort each segment in-place using radix sort.
     * Optimization (#369): skip sort for already-sorted segments (e.g.,
     * from consolidated IDB reads). Also dedup within each segment after
     * sort to reduce merge input. Track per-segment unique counts. */
    /* MSVC does not support VLAs; use heap allocation for portability. */
    uint32_t *seg_starts = (uint32_t *)malloc(seg_count * sizeof(uint32_t));
    uint32_t *seg_ends = (uint32_t *)malloc(seg_count * sizeof(uint32_t));
    if (!seg_starts || !seg_ends) {
        free(seg_starts);
        free(seg_ends);
        return ENOMEM;
    }

    for (uint32_t s = 0; s < seg_count; s++) {
        uint32_t start = seg_boundaries[s];
        uint32_t end = seg_boundaries[s + 1];
        uint32_t count = end - start;
        seg_starts[s] = start;

        if (count > 1) {
            /* Quick sorted-check: bail on first out-of-order pair */
            bool already_sorted = true;
            for (uint32_t r = start + 1; r < end; r++) {
                if (col_rel_row_cmp(rel, r - 1, r) > 0) {
                    already_sorted = false;
                    break;
                }
            }
            if (!already_sorted)
                col_rel_radix_sort(rel, start, count);

            /* Intra-segment dedup: compact unique rows to reduce merge */
            uint32_t out_r = start + 1;
            for (uint32_t r = start + 1; r < end; r++) {
                if (col_rel_row_cmp(rel, out_r - 1, r) != 0) {
                    if (out_r != r)
                        col_rel_row_move(rel, out_r, r);
                    out_r++;
                }
            }
            seg_ends[s] = out_r;
        } else {
            seg_ends[s] = end;
        }
    }

    /* K=1: already sorted+deduped by the loop above */
    if (seg_count == 1) {
        rel->nrows = seg_ends[0];
        free(seg_starts);
        free(seg_ends);
        return 0;
    }

    /* Allocate merge output buffer */
    int64_t *merged = (int64_t *)malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged) {
        free(seg_starts);
        free(seg_ends);
        return ENOMEM;
    }

    if (seg_count == 2) {
        /* Optimized 2-way merge (no heap) */
        uint32_t i = seg_starts[0], j = seg_starts[1];
        uint32_t i_end = seg_ends[0], j_end = seg_ends[1];
        uint32_t out = 0;
        int64_t *last_row = NULL;

        while (i < i_end && j < j_end) {
            int cmp = col_rel_row_cmp(rel, i, j);
            uint32_t row_to_add_idx;

            if (cmp <= 0) {
                row_to_add_idx = i;
                i++;
                if (cmp == 0)
                    j++; /* skip duplicate */
            } else {
                row_to_add_idx = j;
                j++;
            }

            if (last_row == NULL
                || col_rel_row_cmp_raw(rel, row_to_add_idx, last_row)
                != 0) {
                col_rel_row_copy_out(rel, row_to_add_idx,
                    merged + (size_t)out * nc);
                last_row = merged + (size_t)out * nc;
                out++;
            }
        }

        while (i < i_end) {
            if (last_row == NULL
                || col_rel_row_cmp_raw(rel, i, last_row) != 0) {
                col_rel_row_copy_out(rel, i, merged + (size_t)out * nc);
                last_row = merged + (size_t)out * nc;
                out++;
            }
            i++;
        }

        while (j < j_end) {
            if (last_row == NULL
                || col_rel_row_cmp_raw(rel, j, last_row) != 0) {
                col_rel_row_copy_out(rel, j, merged + (size_t)out * nc);
                last_row = merged + (size_t)out * nc;
                out++;
            }
            j++;
        }

        /* Scatter flat merged buffer back into column-major */
        for (uint32_t r = 0; r < out; r++)
            col_rel_row_copy_in(rel, r, merged + (size_t)r * nc);
        rel->nrows = out;
        free(merged);
        free(seg_starts);
        free(seg_ends);
        return 0;
    }

    /* General K-way merge (K >= 3) using min-heap.
     *
     * Heap entries: (segment_index, current_row_pointer).
     * Heap property: parent row <= child rows (lexicographic).
     */
    typedef struct {
        uint32_t seg;    /* segment index */
        uint32_t cursor; /* current row index within rel->data */
        uint32_t end;    /* one-past-end row index for this segment */
    } heap_entry_t;

    /* Build initial heap from non-empty segments */
    heap_entry_t *heap
        = (heap_entry_t *)malloc(seg_count * sizeof(heap_entry_t));
    if (!heap) {
        free(merged);
        free(seg_starts);
        free(seg_ends);
        return ENOMEM;
    }

    uint32_t heap_size = 0;
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_starts[s] < seg_ends[s]) {
            heap[heap_size].seg = s;
            heap[heap_size].cursor = seg_starts[s];
            heap[heap_size].end = seg_ends[s];
            heap_size++;
        }
    }

    /* Sift-down helper using col_rel_row_cmp (Phase B, Issue #330) */
#define HEAP_SIFT_DOWN(start, size)                                          \
        do {                                                                 \
            uint32_t _p = (start);                                           \
            while (2 * _p + 1 < (size)) {                                    \
                uint32_t _c = 2 * _p + 1;                                    \
                if (_c + 1 < (size)                                          \
                    && col_rel_row_cmp(rel, heap[_c + 1].cursor,             \
                    heap[_c].cursor)                                  \
                    < 0)                                              \
                _c++;                                                    \
                if (col_rel_row_cmp(rel, heap[_p].cursor,                    \
                    heap[_c].cursor)                                     \
                    <= 0)                                                    \
                break;                                                   \
                heap_entry_t _tmp = heap[_p];                                \
                heap[_p] = heap[_c];                                         \
                heap[_c] = _tmp;                                             \
                _p = _c;                                                     \
            }                                                                \
        } while (0)

    /* Build min-heap (heapify) */
    if (heap_size > 1) {
        for (int32_t i = (int32_t)(heap_size / 2) - 1; i >= 0; i--)
            HEAP_SIFT_DOWN((uint32_t)i, heap_size);
    }

    /* Extract-min loop with dedup */
    uint32_t out = 0;
    int64_t *last_row = NULL;

    while (heap_size > 0) {
        /* Dedup: skip if same as last emitted row */
        if (last_row == NULL
            || col_rel_row_cmp_raw(rel, heap[0].cursor, last_row) != 0) {
            col_rel_row_copy_out(rel, heap[0].cursor,
                merged + (size_t)out * nc);
            last_row = merged + (size_t)out * nc;
            out++;
        }

        /* Advance cursor of min segment */
        heap[0].cursor++;
        if (heap[0].cursor >= heap[0].end) {
            /* Segment exhausted: replace root with last element */
            heap[0] = heap[heap_size - 1];
            heap_size--;
        }
        if (heap_size > 0)
            HEAP_SIFT_DOWN(0, heap_size);
    }

#undef HEAP_SIFT_DOWN

    /* Scatter flat merged buffer back into column-major */
    for (uint32_t r = 0; r < out; r++)
        col_rel_row_copy_in(rel, r, merged + (size_t)r * nc);
    rel->nrows = out;
    free(merged);
    free(heap);
    free(seg_starts);
    free(seg_ends);
    return 0;
}

int
col_op_consolidate(eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        /* Nothing to deduplicate */
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        in->sorted_nrows = nr;
        in->run_count = 1;
        in->run_ends[0] = nr;
        return eval_stack_push(stack, in, e.owned);
    }

    /* Sort in-place if we own the relation, otherwise copy first */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_pool_new_like(sess->delta_pool, "$consol", in);
        if (!work) {
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        if (col_rel_append_all(work, in, NULL) != 0) {
            col_rel_destroy(work);
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        work_owned = true;
    }

    /* Dispatch: K-way merge when segment boundaries are available */
    uint32_t k = e.seg_count > 0 ? e.seg_count : 1;
    if (k >= 2 && e.seg_boundaries != NULL) {
        int rc = col_op_consolidate_kway_merge(work, e.seg_boundaries, k);
        free(e.seg_boundaries);
        if (rc != 0) {
            if (work_owned)
                col_rel_destroy(work);
            return rc;
        }
        work->sorted_nrows = work->nrows;
        work->run_count = 1;
        work->run_ends[0] = work->nrows;
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);

    /* Issue #94: Incremental merge when a sorted prefix exists.
     * data[0..sorted_nrows) is already sorted+unique from a prior
     * consolidation.  Sort only the unsorted suffix and merge. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;

        /* Phase 1: sort only the unsorted suffix using radix sort */
        col_rel_radix_sort(work, sn, delta_count);

        /* Phase 1b: dedup within suffix */
        uint32_t d_unique = 1;
        for (uint32_t i = 1; i < delta_count; i++) {
            if (col_rel_row_cmp(work, sn + i - 1, sn + i) != 0) {
                col_rel_row_move(work, sn + d_unique, sn + i);
                d_unique++;
            }
        }

        /* Phase 2: merge sorted prefix with sorted suffix */
        uint32_t max_rows = sn + d_unique;

        /* Reuse persistent merge buffer when possible (column-major) */
        int64_t **merged_cols;
        bool used_merge_buf = false;
        if (work->merge_columns && work->merge_buf_cap >= max_rows) {
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        } else {
            /* Grow persistent buffer */
            uint32_t new_cap = max_rows > work->merge_buf_cap * 2
                                   ? max_rows
                                   : work->merge_buf_cap * 2;
            if (new_cap < max_rows)
                new_cap = max_rows;
            if (work->merge_columns) {
                if (col_columns_realloc(work->merge_columns, nc,
                    new_cap) != 0) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            } else {
                work->merge_columns = col_columns_alloc(nc, new_cap);
                if (!work->merge_columns) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            }
            work->merge_buf_cap = new_cap;
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        }

        uint32_t oi = 0, di = 0, out = 0;
        while (oi < sn && di < d_unique) {
            int cmp = col_rel_row_cmp(work, oi, sn + di);
            if (cmp < 0) {
                col_columns_copy_row(merged_cols, out, work->columns, oi, nc);
                oi++;
            } else if (cmp == 0) {
                col_columns_copy_row(merged_cols, out, work->columns, oi, nc);
                oi++;
                di++;
            } else {
                col_columns_copy_row(merged_cols, out, work->columns,
                    sn + di, nc);
                di++;
            }
            out++;
        }
        while (oi < sn) {
            col_columns_copy_row(merged_cols, out, work->columns, oi, nc);
            oi++;
            out++;
        }
        while (di < d_unique) {
            col_columns_copy_row(merged_cols, out, work->columns,
                sn + di, nc);
            di++;
            out++;
        }

        /* Swap merge_columns and columns to avoid O(N) memcpy (issue #218). */
        if (used_merge_buf) {
            int64_t **old_cols = work->columns;
            uint32_t old_cap = work->capacity;
            work->columns = work->merge_columns;
            work->capacity = work->merge_buf_cap;
            work->merge_columns = old_cols;
            work->merge_buf_cap = old_cap;
        }
        work->nrows = out;
        work->sorted_nrows = out;
        work->run_count = 1;
        work->run_ends[0] = out;

        /* Right-size columns after dedup (issue #218). */
        if (out > 0 && work->capacity > out + out / 4) {
            uint32_t tight = out + out / 4;
            if (tight < COL_REL_INIT_CAP)
                tight = COL_REL_INIT_CAP;
            if (col_columns_realloc(work->columns, nc, tight) == 0)
                work->capacity = tight;
        }

        return eval_stack_push(stack, work, work_owned);
    }

    /* Fallback: radix sort + dedup (sorted_nrows == 0 or full re-sort) */
    col_rel_radix_sort_int64(work);

    /* Compact: keep only unique rows */
    uint32_t out_r = 1; /* first row always kept */
    for (uint32_t r = 1; r < nr; r++) {
        if (col_rel_row_cmp(work, r - 1, r) != 0) {
            col_rel_row_move(work, out_r, r);
            out_r++;
        }
    }
    work->nrows = out_r;
    work->sorted_nrows = out_r;
    work->run_count = 1;
    work->run_ends[0] = out_r;

    return eval_stack_push(stack, work, work_owned);
}

/*
 * col_op_consolidate_incremental:
 * Incremental sort+dedup for semi-naive evaluation.
 *
 * Precondition: rel->data[0..old_nrows) is already sorted+unique from
 * the previous iteration's consolidation. New rows appended during this
 * iteration live in [old_nrows..rel->nrows).
 *
 * Algorithm:
 *   1. Sort only the delta rows: O(D log D)
 *   2. Dedup within delta: O(D)
 *   3. Merge sorted old with sorted delta, skipping duplicates: O(N + D)
 *
 * Total: O(D log D + N) vs O(N log N) for full re-sort.
 */
static int UNUSED
col_op_consolidate_incremental(col_rel_t *rel, uint32_t old_nrows)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1 || old_nrows >= nr)
        return 0; /* nothing new or trivially sorted */

    uint32_t delta_count = nr - old_nrows;

    /* Phase 1: sort only the new delta rows using radix sort */
    col_rel_radix_sort(rel, old_nrows, delta_count);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (col_rel_row_cmp(rel, old_nrows + i - 1, old_nrows + i) != 0) {
            col_rel_row_move(rel, old_nrows + d_unique, old_nrows + i);
            d_unique++;
        }
    }

    /* Phase 2: merge sorted old [0..old_nrows) with sorted+unique delta.
     * Allocate temporary buffer for merge output. */
    size_t max_rows = (size_t)old_nrows + d_unique;
    int64_t *merged = (int64_t *)malloc(max_rows * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    uint32_t oi = 0, di = 0, out = 0;
    while (oi < old_nrows && di < d_unique) {
        int cmp = col_rel_row_cmp(rel, oi, old_nrows + di);
        if (cmp < 0) {
            col_rel_row_copy_out(rel, oi, merged + (size_t)out * nc);
            oi++;
            out++;
        } else if (cmp == 0) {
            col_rel_row_copy_out(rel, oi, merged + (size_t)out * nc);
            oi++;
            di++;
            out++; /* skip duplicate from delta */
        } else {
            col_rel_row_copy_out(rel, old_nrows + di,
                merged + (size_t)out * nc);
            di++;
            out++;
        }
    }
    /* Copy remaining from old */
    while (oi < old_nrows) {
        col_rel_row_copy_out(rel, oi, merged + (size_t)out * nc);
        oi++;
        out++;
    }
    /* Copy remaining from delta */
    while (di < d_unique) {
        col_rel_row_copy_out(rel, old_nrows + di,
            merged + (size_t)out * nc);
        di++;
        out++;
    }

    /* Scatter flat merged buffer back into column-major */
    for (uint32_t r = 0; r < out; r++)
        col_rel_row_copy_in(rel, r, merged + (size_t)r * nc);
    free(merged);
    rel->nrows = out;
    return 0;
}

/*
 * col_rel_compact_runs - K-way merge of tiered sorted runs (#369).
 *
 * Merges all independently sorted+unique runs into a single sorted run.
 * Uses min-heap merge (runs are already sorted, no per-segment sort needed).
 * Writes merged result back into rel using flat buffer + scatter.
 *
 * @return 0 on success, ENOMEM on allocation failure.
 */
static int
col_rel_compact_runs(col_rel_t *rel)
{
    if (rel->run_count <= 1)
        return 0;

    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    uint32_t K = rel->run_count;

    /* Build segment boundaries from run_ends */
    uint32_t seg_bounds[COL_MAX_RUNS + 1];
    seg_bounds[0] = 0;
    for (uint32_t i = 0; i < K; i++)
        seg_bounds[i + 1] = rel->run_ends[i];

    /* Allocate merge output buffer (flat row-major) */
    int64_t *merged = (int64_t *)malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    /* Min-heap entries (stack-allocated, K <= COL_MAX_RUNS = 8).
     *
     * lead_key[i] shadows columns[0][heap[i].cursor] so the inner
     * comparator can short-circuit on the leading column without
     * indirecting through col_rel_row_cmp() and its column-array
     * dereference.  When the leading keys differ (the common case for
     * sorted runs whose interleaving is determined by the leading
     * sort key), the heap relation falls out of a register-resident
     * scalar compare; only equal-leading rows pay the full row_cmp
     * cost.  lead_key is kept consistent with heap[].cursor at every
     * mutation: cursor advance, end-of-run replacement, and sift-down
     * swap. */
    typedef struct {
        uint32_t cursor;
        uint32_t end;
    } compact_he_t;

    compact_he_t heap[COL_MAX_RUNS];
    int64_t lead_key[COL_MAX_RUNS];
    uint32_t heap_size = 0;
    const int64_t *lead_col = rel->columns[0];

    for (uint32_t s = 0; s < K; s++) {
        if (seg_bounds[s] < seg_bounds[s + 1]) {
            heap[heap_size].cursor = seg_bounds[s];
            heap[heap_size].end = seg_bounds[s + 1];
            lead_key[heap_size] = lead_col[seg_bounds[s]];
            heap_size++;
        }
    }

#define COMPACT_LT_(_a, _b) \
        (lead_key[_a] < lead_key[_b]                                             \
        || (lead_key[_a] == lead_key[_b]                                        \
        && col_rel_row_cmp(rel, heap[_a].cursor, heap[_b].cursor) < 0))

#define COMPACT_LE_(_a, _b) \
        (lead_key[_a] < lead_key[_b]                                             \
        || (lead_key[_a] == lead_key[_b]                                        \
        && col_rel_row_cmp(rel, heap[_a].cursor, heap[_b].cursor) <= 0))

#define COMPACT_SIFT_DOWN(start, size)                                       \
        do {                                                                     \
            uint32_t _p = (start);                                               \
            while (2 * _p + 1 < (size)) {                                        \
                uint32_t _c = 2 * _p + 1;                                        \
                if (_c + 1 < (size) && COMPACT_LT_(_c + 1, _c))                  \
                _c++;                                                            \
                if (COMPACT_LE_(_p, _c))                                         \
                break;                                                           \
                compact_he_t _tmp = heap[_p];                                    \
                heap[_p] = heap[_c];                                             \
                heap[_c] = _tmp;                                                 \
                int64_t _kt = lead_key[_p];                                      \
                lead_key[_p] = lead_key[_c];                                     \
                lead_key[_c] = _kt;                                              \
                _p = _c;                                                         \
            }                                                                    \
        } while (0)

    /* Build min-heap */
    if (heap_size > 1) {
        for (int32_t i = (int32_t)(heap_size / 2) - 1; i >= 0; i--)
            COMPACT_SIFT_DOWN((uint32_t)i, heap_size);
    }

    /* Extract-min loop (cross-run uniqueness is guaranteed, but dedup
     * defensively in case of edge cases) */
    uint32_t out = 0;
    int64_t *last_row = NULL;

    while (heap_size > 0) {
        if (last_row == NULL
            || col_rel_row_cmp_raw(rel, heap[0].cursor, last_row) != 0) {
            col_rel_row_copy_out(rel, heap[0].cursor,
                merged + (size_t)out * nc);
            last_row = merged + (size_t)out * nc;
            out++;
        }
        heap[0].cursor++;
        if (heap[0].cursor >= heap[0].end) {
            heap[0] = heap[heap_size - 1];
            lead_key[0] = lead_key[heap_size - 1];
            heap_size--;
        } else {
            lead_key[0] = lead_col[heap[0].cursor];
        }
        if (heap_size > 0)
            COMPACT_SIFT_DOWN(0, heap_size);
    }

#undef COMPACT_SIFT_DOWN
#undef COMPACT_LT_
#undef COMPACT_LE_

    /* Scatter flat merged buffer back into column-major */
    for (uint32_t r = 0; r < out; r++)
        col_rel_row_copy_in(rel, r, merged + (size_t)r * nc);
    free(merged);

    rel->nrows = out;
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;
    return 0;
}

/*
 * col_op_consolidate_incremental_delta - Incremental consolidation with delta output
 *
 * PURPOSE:
 *   Merge pre-sorted old data with newly appended delta rows, while simultaneously
 *   emitting the set of truly-new rows (R_new - R_old) as a byproduct.
 *   This eliminates separate post-iteration merge walk needed for delta computation.
 *
 * PRECONDITIONS:
 *   - rel->data[0..old_nrows) is already sorted and unique (invariant)
 *   - rel->data[old_nrows..rel->nrows) contains newly appended delta rows (unsorted)
 *   - old_nrows <= rel->nrows
 *
 * POSTCONDITIONS:
 *   - rel->data[0..rel->nrows) is sorted and unique (new invariant)
 *   - delta_out->data contains exactly R_new - R_old (truly new rows)
 *   - delta_out->data is sorted in same order as rel->data
 *   - rel->nrows reflects final merged count
 *
 * ALGORITHM (#369):
 *   Binary-search dedup with tiered sorted runs when D << N.
 *   Falls back to 2-pointer merge when D is large relative to N.
 *   Compacts all runs via K-way merge when run_count >= COL_MAX_RUNS.
 *
 * ALGORITHM COMPLEXITY:
 *   - Time: O(D log D + D) fast-path (all delta > all old, common for CRDT)
 *           O(D log D + D*K*log(N/K)) binary-search path (D << N)
 *           O(D log D + N + D) fallback merge walk (D ~ N)
 *   - Space: O(N + D) for merge buffer (fallback) or O(D) (binary-search path)
 */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, int *out_fast_path)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr == 0 || old_nrows >= nr) {
        if (out_fast_path)
            *out_fast_path = 1; /* trivially fast: no data to process */
        return 0;              /* nothing new */
    }

    uint32_t delta_count = nr - old_nrows;

    /* Phase 1: sort only the new delta rows using radix sort */
    col_rel_radix_sort(rel, old_nrows, delta_count);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (col_rel_row_cmp(rel, old_nrows + i - 1, old_nrows + i) != 0) {
            col_rel_row_move(rel, old_nrows + d_unique, old_nrows + i);
            d_unique++;
        }
    }

    /* Initialize or repair run tracking (#369, #376).
     * After retraction/re-eval the relation may be cleared (nrows reduced)
     * without resetting run_count/run_ends, leaving stale metadata.
     * Normalize to a single run covering [0, old_nrows) when inconsistent. */
    if (rel->run_count == 0
        || rel->run_ends[rel->run_count - 1] != old_nrows) {
        rel->run_count = (old_nrows > 0) ? 1 : 0;
        if (old_nrows > 0)
            rel->run_ends[0] = old_nrows;
    }

    /* Fast-path (Issue #239): if all delta rows sort after max of all runs,
     * skip the merge/search and directly append as new run. */
    int fast_path = 0;
    if (old_nrows == 0) {
        fast_path = 1;
    } else {
        /* Check against max (last row) of ALL runs (#369 C1) */
        bool all_less = true;
        for (uint32_t i = 0; i < rel->run_count && all_less; i++) {
            uint32_t end = rel->run_ends[i];
            if (end > 0
                && col_rel_row_cmp(rel, end - 1, old_nrows) >= 0)
                all_less = false;
        }
        if (all_less)
            fast_path = 1;
    }

    if (fast_path) {
        /* All d_unique rows are novel. Emit to delta_out and append as run. */
        if (delta_out) {
            col_row_buf_t drb;
            int64_t *const dr = col_row_buf_init(&drb, nc);
            if (!dr)
                return ENOMEM;
            for (uint32_t k = 0; k < d_unique; k++) {
                for (uint32_t c = 0; c < nc; c++)
                    dr[c] = rel->columns[c][old_nrows + k];
                col_rel_append_row(delta_out, dr);
            }
            col_row_buf_release(&drb);
        }
        rel->nrows = old_nrows + d_unique;
        rel->sorted_nrows = rel->nrows;

        /* Register as new run */
        if (rel->run_count < COL_MAX_RUNS) {
            rel->run_ends[rel->run_count] = rel->nrows;
            rel->run_count++;
        } else {
            /* Temporarily extend last run to include new data, then compact */
            rel->run_ends[rel->run_count - 1] = rel->nrows;
            int rc = col_rel_compact_runs(rel);
            if (rc != 0)
                return rc;
        }

        if (rel->timestamps) {
            free(rel->timestamps);
            rel->timestamps = NULL;
        }
        if (out_fast_path)
            *out_fast_path = 1;
        return 0;
    }

    /* Adaptive dispatch (#369): use binary-search dedup when D << N,
     * fall back to 2-pointer merge when D is large (first iterations). */
    if (d_unique <= old_nrows / 16 && rel->run_count > 0) {
        /* Binary-search dedup path: O(D * K * log(N/K)) */
        uint32_t novel_count = 0;

        for (uint32_t i = 0; i < d_unique; i++) {
            uint32_t row_idx = old_nrows + i;
            bool found = false;

            /* Search each existing run */
            for (uint32_t r = 0; r < rel->run_count && !found; r++) {
                uint32_t run_start = (r == 0) ? 0 : rel->run_ends[r - 1];
                uint32_t run_end = rel->run_ends[r];
                if (col_rel_binary_search_row(rel, run_start, run_end,
                    row_idx))
                    found = true;
            }

            if (!found) {
                /* Novel row: compact to front of delta region */
                if (novel_count != i)
                    col_rel_row_move(rel, old_nrows + novel_count, row_idx);
                if (delta_out) {
                    /* Issue #1000: the one converted site that is NOT
                     * covered by a test, and the one that inits inside a
                     * per-row loop rather than above it.
                     *
                     * Reaching it needs a >32-column relation *and*
                     * d_unique <= old_nrows/16 && rel->run_count > 0, i.e.
                     * the binary-search dedup branch on a large existing
                     * relation with few novel rows.  Mutating this guard to
                     * the old fixed width survives the whole suite, so the
                     * bound here rests on inspection, not on a test.
                     *
                     * The per-row init is the pre-existing shape and is not
                     * a regression -- the base code allocated here too --
                     * but it does contradict the hoisting rule this helper
                     * documents.  Both are tracked in #1003. */
                    col_row_buf_t drb;
                    int64_t *const dr = col_row_buf_init(&drb, nc);
                    if (!dr)
                        return ENOMEM;
                    for (uint32_t c = 0; c < nc; c++)
                        dr[c] = rel->columns[c][old_nrows + novel_count];
                    col_rel_append_row(delta_out, dr);
                    col_row_buf_release(&drb);
                }
                novel_count++;
            }
        }

        if (novel_count > 0) {
            rel->nrows = old_nrows + novel_count;
            /* Register novel rows as new run */
            if (rel->run_count < COL_MAX_RUNS) {
                rel->run_ends[rel->run_count] = rel->nrows;
                rel->run_count++;
            } else {
                /* Compact existing runs, preserving novel rows (#376).
                 * Novel rows at [old_nrows..old_nrows+novel_count) are not
                 * in any run yet.  compact_runs only merges run-bounded
                 * data so novel rows are physically untouched.  Relocate
                 * them adjacent to the compacted prefix afterwards. */
                int rc = col_rel_compact_runs(rel);
                if (rc != 0)
                    return rc;
                uint32_t compacted = rel->nrows;
                for (uint32_t j = 0; j < novel_count; j++)
                    col_rel_row_move(rel, compacted + j, old_nrows + j);
                rel->nrows = compacted + novel_count;
                rel->run_ends[rel->run_count] = rel->nrows;
                rel->run_count++;
            }
        } else {
            rel->nrows = old_nrows; /* no new rows */
        }
        rel->sorted_nrows = rel->nrows;

        if (rel->timestamps) {
            free(rel->timestamps);
            rel->timestamps = NULL;
        }
        if (out_fast_path)
            *out_fast_path = 0;
        return 0;
    }

    /* Fallback: 2-pointer merge when D is large relative to N.
     * After merge, reset to single run. */
    uint32_t max_rows = old_nrows + d_unique;

    if (rel->merge_buf_cap < max_rows) {
        uint32_t new_cap = max_rows > rel->merge_buf_cap * 2
                               ? max_rows
                               : rel->merge_buf_cap * 2;
        if (new_cap < max_rows)
            new_cap = max_rows;
        if (rel->merge_columns) {
            if (col_columns_realloc(rel->merge_columns, nc, new_cap) != 0)
                return ENOMEM;
        } else {
            rel->merge_columns = col_columns_alloc(nc, new_cap);
            if (!rel->merge_columns)
                return ENOMEM;
        }
        rel->merge_buf_cap = new_cap;
    }
    int64_t **merged_cols = rel->merge_columns;

    col_row_buf_t delta_rb;
    if (!col_row_buf_init(&delta_rb, nc))
        return ENOMEM;
    int64_t *delta_row = delta_rb.ptr;

    /* For fallback merge, we need a single sorted prefix.
     * If multiple runs exist, compact first (#377 fix).
     * compact_runs only merges run-bounded data; delta rows at
     * [old_nrows..old_nrows+d_unique) are physically untouched.
     * Relocate them adjacent to the compacted prefix afterwards. */
    if (rel->run_count > 1) {
        uint32_t delta_phys = old_nrows; /* physical location of delta */
        int rc = col_rel_compact_runs(rel);
        if (rc != 0) {
            col_row_buf_release(&delta_rb);
            return rc;
        }
        uint32_t compacted = rel->nrows;
        for (uint32_t j = 0; j < d_unique; j++)
            col_rel_row_move(rel, compacted + j, delta_phys + j);
        old_nrows = compacted;
        rel->nrows = compacted + d_unique;
        max_rows = old_nrows + d_unique;
    }

    uint32_t oi = 0, di = 0, out = 0;

    while (oi < old_nrows && di < d_unique) {
        int cmp = col_rel_row_cmp(rel, oi, old_nrows + di);

        if (cmp == 0) {
            col_columns_copy_row(merged_cols, out, rel->columns, oi, nc);
            oi++;
            di++;
        } else if (cmp < 0) {
            col_columns_copy_row(merged_cols, out, rel->columns, oi, nc);
            oi++;
        } else {
            col_columns_copy_row(merged_cols, out, rel->columns,
                old_nrows + di, nc);
            if (delta_out) {
                for (uint32_t c = 0; c < nc; c++)
                    delta_row[c] = merged_cols[c][out];
                col_rel_append_row(delta_out, delta_row);
            }
            di++;
        }
        out++;
    }
    while (oi < old_nrows) {
        col_columns_copy_row(merged_cols, out, rel->columns, oi, nc);
        oi++;
        out++;
    }
    while (di < d_unique) {
        col_columns_copy_row(merged_cols, out, rel->columns,
            old_nrows + di, nc);
        if (delta_out) {
            for (uint32_t c = 0; c < nc; c++)
                delta_row[c] = merged_cols[c][out];
            col_rel_append_row(delta_out, delta_row);
        }
        di++;
        out++;
    }

    col_row_buf_release(&delta_rb);

    /* Swap merge_columns and columns to avoid O(N) memcpy (issue #218). */
    {
        int64_t **old_cols = rel->columns;
        uint32_t old_cap = rel->capacity;
        rel->columns = rel->merge_columns;
        rel->capacity = rel->merge_buf_cap;
        rel->merge_columns = old_cols;
        rel->merge_buf_cap = old_cap;
    }
    rel->nrows = out;
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;

    /* Phase 3b: Right-size columns after dedup (issue #218). */
    if (out > 0 && rel->capacity > out + out / 4) {
        uint32_t tight = out + out / 4;
        if (tight < COL_REL_INIT_CAP)
            tight = COL_REL_INIT_CAP;
        if (col_columns_realloc(rel->columns, nc, tight) == 0)
            rel->capacity = tight;
    }

    if (rel->timestamps) {
        free(rel->timestamps);
        rel->timestamps = NULL;
    }
    if (out_fast_path)
        *out_fast_path = 0;
    return 0;
}

/* --- K-FUSION ------------------------------------------------------------ */

/**
 * col_rel_merge_k:
 * Merge K sorted relations into a single deduplicated relation.
 * Uses the same min-heap merging strategy as col_op_consolidate_kway_merge.
 *
 * @relations: Array of K col_rel_t pointers (caller-owned, each sorted)
 * @k:         Number of relations to merge
 *
 * Returns: Newly allocated merged relation (caller must free).
 *          Returns NULL on allocation failure.
 *
 * The output relation name is "<merged-k>" and contains all rows from
 * the K input relations with duplicates removed.
 */
/*
 * col_rel_merge_k - Deterministic K-way sorted merge with deduplication.
 *
 * Determinism guarantee (Issue #260):
 *   - k=1: sequential copy, input order preserved
 *   - k=2: two-pointer merge on sorted inputs, left-before-right tie-break
 *   - k>=3: left-fold over pairs: merge(merge(r[0],r[1]),r[2]),...
 *     Fixed input order + sorted inputs => identical output across runs.
 *
 * Precondition: each input relation is already sorted+deduped
 *   (WL_PLAN_OP_CONSOLIDATE is the last K-fusion worker op).
 */
static col_rel_t *
col_rel_merge_k(col_rel_t **relations, uint32_t k)
{
    if (k == 0)
        return NULL;

    /* All K relations must have the same schema */
    uint32_t nc = relations[0]->ncols;
    uint32_t total_rows = 0;
    for (uint32_t i = 0; i < k; i++) {
        if (relations[i]->ncols != nc)
            return NULL; /* Schema mismatch */
        total_rows += relations[i]->nrows;
    }

    if (total_rows == 0) {
        /* Create empty result with correct schema */
        return col_rel_new_like("<merged-k>", relations[0]);
    }

    /* Create output relation with capacity for all rows */
    col_rel_t *out = col_rel_new_like("<merged-k>", relations[0]);
    if (!out)
        return NULL;

    /* Per-block scratch (#1000): both the staging row and the dedup key must
     * be nc wide, not COL_STACK_MAX wide.  MERGE_K_SETUP declares and
     * allocates them once per merge block -- allocating inside
     * MERGE_K_APPEND would malloc once per row for wide relations. */
#define MERGE_K_SETUP()                                                      \
        col_row_buf_t _rowbuf, _lastbuf;                                         \
        int64_t *_rb, *last_row_buf;                                             \
        const int64_t *last_row = NULL;                                          \
        _rb = col_row_buf_init(&_rowbuf, nc);                                    \
        last_row_buf = col_row_buf_init(&_lastbuf, nc);                          \
        if (!_rb || !last_row_buf) {                                             \
            col_row_buf_release(&_rowbuf);                                       \
            col_row_buf_release(&_lastbuf);                                      \
            col_rel_destroy(out);                                                \
            return NULL;                                                         \
        }

#define MERGE_K_RELEASE()                                                    \
        do {                                                                     \
            col_row_buf_release(&_rowbuf);                                       \
            col_row_buf_release(&_lastbuf);                                      \
        } while (0)

    /* Helper: copy row from relation into temp buf, append to out, dedup
     * against last_row in out.  Bails out of the enclosing function on
     * failure (after releasing the block scratch). */
#define MERGE_K_APPEND(rel_ptr, row_idx)                                     \
        do {                                                                     \
            col_rel_row_copy_out((rel_ptr), (row_idx), _rb);                     \
            if (last_row == NULL                                                 \
                || row_cmp_dispatch(last_row, _rb, nc) != 0) {                   \
                if (col_rel_append_row(out, _rb) != 0) {                         \
                    MERGE_K_RELEASE();                                           \
                    col_rel_destroy(out);                                        \
                    return NULL;                                                 \
                }                                                                \
                col_rel_row_copy_out(out, out->nrows - 1, last_row_buf);         \
                last_row = last_row_buf;                                         \
            }                                                                    \
        } while (0)

    /* K=1: Copy with dedup using append (handles dynamic growth) */
    if (k == 1) {
        col_rel_t *src = relations[0];
        MERGE_K_SETUP();
        for (uint32_t r = 0; r < src->nrows; r++) {
            MERGE_K_APPEND(src, r);
        }
        MERGE_K_RELEASE();
        return out;
    }

    /* K=2: Optimized 2-pointer merge using append */
    if (k == 2) {
        col_rel_t *left = relations[0];
        col_rel_t *right = relations[1];
        uint32_t li = 0, ri = 0;
        MERGE_K_SETUP();

        while (li < left->nrows && ri < right->nrows) {
            int cmp = col_rel_row_cmp2(left, li, right, ri);

            if (cmp < 0) {
                MERGE_K_APPEND(left, li);
                li++;
            } else if (cmp > 0) {
                MERGE_K_APPEND(right, ri);
                ri++;
            } else {
                /* Equal rows: add once, skip both */
                MERGE_K_APPEND(left, li);
                li++;
                ri++;
            }
        }

        /* Drain remaining rows from left */
        while (li < left->nrows) {
            MERGE_K_APPEND(left, li);
            li++;
        }

        /* Drain remaining rows from right */
        while (ri < right->nrows) {
            MERGE_K_APPEND(right, ri);
            ri++;
        }

        MERGE_K_RELEASE();
        return out;
    }

    /* K >= 3: Pairwise merge fallback */
    col_rel_t *temp = relations[0];
    for (uint32_t i = 1; i < k; i++) {
        col_rel_t *pair[2] = { temp, relations[i] };
        col_rel_t *merged = col_rel_merge_k(pair, 2);
        if (!merged) {
            col_rel_destroy(out);
            if (i > 1)
                col_rel_destroy(temp);
            return NULL;
        }
        if (i > 1)
            col_rel_destroy(temp);
        temp = merged;
    }

    /* Move final result into output using append */
    {
        MERGE_K_SETUP();
        for (uint32_t r = 0; r < temp->nrows; r++) {
            MERGE_K_APPEND(temp, r);
        }
        MERGE_K_RELEASE();
        col_rel_destroy(temp);
    }

#undef MERGE_K_APPEND
#undef MERGE_K_RELEASE
#undef MERGE_K_SETUP
    return out;
}

/**
 * col_arr_entry_clone - Deep-copy one arrangement registry entry (#260).
 *
 * All owned memory (rel_name, key_cols, ht_head, ht_next) is freshly
 * allocated.  arr.key_cols is set to the new entry's key_cols (shared alias,
 * not a separate allocation — matches arrangement.c creation convention).
 * Returns 0 on success; dst is memset-zeroed before returning on failure.
 */
static int
col_arr_entry_clone(const col_arr_entry_t *src, col_arr_entry_t *dst)
{
    memset(dst, 0, sizeof(*dst));

    dst->rel_name = wl_strdup(src->rel_name);
    if (!dst->rel_name)
        return ENOMEM;

    if (src->key_count > 0) {
        dst->key_cols = (uint32_t *)malloc(src->key_count * sizeof(uint32_t));
        if (!dst->key_cols) {
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->key_cols, src->key_cols, src->key_count * sizeof(uint32_t));
    }
    dst->key_count = src->key_count;

    /* arr.key_cols is a shared alias of entry.key_cols (not separately owned).
    * Mirrors the convention in col_session_get_arrangement (arrangement.c). */
    dst->arr.key_cols = dst->key_cols;
    dst->arr.key_count = src->arr.key_count;
    dst->arr.indexed_rows = src->arr.indexed_rows;
    dst->arr.content_hash = src->arr.content_hash;
    dst->arr.nbuckets = src->arr.nbuckets;
    dst->arr.ht_cap = src->arr.ht_cap;
    /* Issue #216: copy LRU metadata so worker clones inherit access state. */
    dst->lru_clock = src->lru_clock;
    dst->mem_bytes = src->mem_bytes;

    if (src->arr.nbuckets > 0 && src->arr.ht_head) {
        dst->arr.ht_head
            = (uint32_t *)malloc(src->arr.nbuckets * sizeof(uint32_t));
        if (!dst->arr.ht_head) {
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->arr.ht_head, src->arr.ht_head,
            src->arr.nbuckets * sizeof(uint32_t));
    }

    if (src->arr.ht_cap > 0 && src->arr.ht_next) {
        dst->arr.ht_next
            = (uint32_t *)malloc(src->arr.ht_cap * sizeof(uint32_t));
        if (!dst->arr.ht_next) {
            free(dst->arr.ht_head);
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->arr.ht_next, src->arr.ht_next,
            src->arr.ht_cap * sizeof(uint32_t));
    }

    return 0;
}

/**
 * col_arr_entries_clone - Deep-copy an arrangement registry array (#260).
 *
 * Creates an independent copy of `count` entries for a K-fusion worker.
 * On success, *out_entries owns all allocations and *out_cap equals count.
 * On failure, *out_entries is NULL.
 */
int
col_arr_entries_clone(const col_arr_entry_t *src, uint32_t count,
    col_arr_entry_t **out_entries, uint32_t *out_cap)
{
    *out_entries = NULL;
    *out_cap = 0;

    if (count == 0)
        return 0;

    col_arr_entry_t *cloned
        = (col_arr_entry_t *)calloc(count, sizeof(col_arr_entry_t));
    if (!cloned)
        return ENOMEM;

    for (uint32_t i = 0; i < count; i++) {
        int clone_rc = col_arr_entry_clone(&src[i], &cloned[i]);
        if (clone_rc != 0) {
            for (uint32_t j = 0; j < i; j++) {
                free(cloned[j].rel_name);
                free(cloned[j].key_cols);
                arr_free_contents(&cloned[j].arr);
            }
            free(cloned);
            return clone_rc;
        }
    }

    *out_entries = cloned;
    *out_cap = count;
    return 0;
}

/**
 * Worker task context for K-fusion evaluation.
 * plan_data is embedded (not a pointer) so its lifetime matches the worker array.
 * sess points to an isolated session wrapper with a per-worker mat_cache so
 * concurrent col_op_join calls do not share the non-thread-safe cache.
 */
typedef struct {
    wl_plan_relation_t plan_data; /* Embedded plan (stable lifetime) */
    eval_stack_t stack;           /* Output stack (initialized by worker) */
    wl_col_session_t
    *sess;        /* Per-worker session wrapper (isolated mat_cache) */
    int rc;       /* Return code from evaluation */
} col_op_k_fusion_worker_t;

/**
 * Worker thread function for K-fusion parallel evaluation.
 * Evaluates a single relation plan and collects result in context.
 */
static void
col_op_k_fusion_worker(void *ctx)
{
    col_op_k_fusion_worker_t *wc = (col_op_k_fusion_worker_t *)ctx;
    eval_stack_init(&wc->stack);
    wc->rc = col_eval_relation_plan(&wc->plan_data, &wc->stack, wc->sess);
}

/**
 * K-Fusion W=1 serial fast-path (Issue #549).
 *
 * Why this is safe:
 *   At num_workers <= 1 there is only one thread, so the per-worker session
 *   clone / arena / delta_pool machinery exists purely to isolate concurrent
 *   workers that no longer exist. Arrangements and mat_cache are stateful
 *   caches that the engine rebuilds on demand, so executing K branches
 *   sequentially against the parent sess cannot race and cannot change the
 *   computed result. Skipping the 375-per-iter arrangement clones is pure
 *   profit for workloads like DOOP at W=1.
 *
 * The parallel path deliberately drops any mat_cache entries its workers
 * produced during a dispatch (workers start with count=0, cleanup frees
 * from index 0). Mirror that invariant here by snapshotting sess->mat_cache
 * count on entry and trimming any branch-added entries on return, so outside
 * code sees identical K-Fusion side-effects regardless of worker count.
 */
static int
col_op_k_fusion_serial(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    uint32_t k = meta->k;

    uint64_t _phase_t0 = now_ns();
    col_rel_t **results = (col_rel_t **)calloc(k, sizeof(col_rel_t *));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results)
        return ENOMEM;

    /* Snapshot mat_cache so branch-added entries don't leak past K-Fusion
     * (parity with parallel path which discards all worker additions). */
    uint32_t mat_base = sess->mat_cache.count;

    /* Snapshot delta_pool->slot_used so any intermediate pool-allocated
     * relations produced by branch evaluation (VARIABLE FORCE_EMPTY,
     * JOIN/SEMIJOIN outputs, etc.) can have their heap-allocated fields
     * (name, columns, col_names) freed in cleanup.  The parallel path gets
     * this for free by owning a per-worker delta_pool that is fully
     * destroyed at teardown; the serial path shares the parent pool and
     * must sweep the range itself (#549 ASAN fix). */
    uint32_t pool_slot_base
        = sess->delta_pool ? sess->delta_pool->slot_used : 0;

    int rc = 0;
    uint32_t n_results = 0;

    /* Evaluate each K branch sequentially against the parent session. */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < k; d++) {
        wl_plan_relation_t plan_data;
        memset(&plan_data, 0, sizeof(plan_data));
        plan_data.name = "<k_fusion_copy>";
        plan_data.delta_name = NULL;
        plan_data.ops = meta->k_ops[d];
        plan_data.op_count = meta->k_op_counts[d];

        /* Per-copy empty-delta skip (issue #85): if this copy references an
         * empty/absent delta on iteration > 0, skip — produces 0 rows. */
        if (has_empty_forced_delta(&plan_data, sess, sess->current_iteration))
            continue;

        eval_stack_t s;
        eval_stack_init(&s);
        int branch_rc = col_eval_relation_plan(&plan_data, &s, sess);
        if (branch_rc != 0) {
            rc = branch_rc;
            eval_stack_drain(&s);
            goto cleanup;
        }

        eval_entry_t e = eval_stack_pop(&s);
        if (!e.rel) {
            rc = EINVAL;
            eval_stack_drain(&s);
            goto cleanup;
        }

        /* If not owned, share columns zero-copy (parity with parallel path). */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(sess->delta_pool,
                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                eval_stack_drain(&s);
                goto cleanup;
            }
            copy->col_shared = (bool *)calloc(e.rel->ncols, sizeof(bool));
            if (copy->col_shared) {
                for (uint32_t c = 0; c < e.rel->ncols; c++) {
                    free(copy->columns[c]); /* free pool-allocated column */
                    copy->columns[c] = e.rel->columns[c];
                    copy->col_shared[c] = true;
                }
            } else {
                /* Fallback: deep copy on alloc failure */
                for (uint32_t c = 0; c < e.rel->ncols; c++)
                    memcpy(copy->columns[c], e.rel->columns[c],
                        (size_t)e.rel->nrows * sizeof(int64_t));
            }
            copy->nrows = e.rel->nrows;
            results[n_results++] = copy;
        } else {
            results[n_results++] = e.rel;
        }
        eval_stack_drain(&s);
    }
    COL_SESSION(sess)->kfusion_dispatch_ns += now_ns() - _phase_t0;

    /* Merge: inputs are sorted+deduped (CONSOLIDATE is each branch's last op). */
    _phase_t0 = now_ns();
    {
        col_rel_t *merged;
        if (n_results == 0) {
            /* All copies skipped: empty output with target relation schema. */
            uint32_t ncols = 0;
            if (op->relation_name) {
                col_rel_t *target = session_find_rel(sess, op->relation_name);
                if (target)
                    ncols = target->ncols;
            }
            merged = col_rel_new_auto("$kfusion_empty", ncols);
        } else {
            merged = col_rel_merge_k(results, n_results);
        }
        if (!merged) {
            rc = ENOMEM;
            goto cleanup;
        }
        rc = eval_stack_push(stack, merged, true);
        if (rc != 0)
            col_rel_destroy(merged);
    }
    COL_SESSION(sess)->kfusion_merge_ns += now_ns() - _phase_t0;

cleanup:
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < n_results; d++) {
        if (results[d])
            col_rel_destroy(results[d]);
    }
    /* Trim mat_cache back to pre-dispatch baseline. Entries added by branches
     * are owned by the cache and must be freed the same way the parallel path
     * frees its worker caches. */
    {
        col_mat_cache_t *mc = &sess->mat_cache;
        for (uint32_t i = mat_base; i < mc->count; i++) {
            col_rel_destroy(mc->entries[i].result);
            mc->total_bytes -= mc->entries[i].mem_bytes;
        }
        mc->count = mat_base;
    }
    /* Sweep any pool slots allocated during branch eval (#549 ASAN fix).
     * Slots whose relations were already col_rel_destroy'd upstream are
     * zeroed and this walk is a safe no-op for them (free(NULL) chains).
     * Slots still holding heap pointers (e.g. intermediate $empty_skip
     * names, JOIN $join_out names that the engine left dangling in the
     * parent pool) get their name/columns/col_names/etc. freed here.
     * After the sweep we reclaim slot_used so the pool can reuse the
     * range for the next K-Fusion dispatch. */
    if (sess->delta_pool) {
        delta_pool_t *dp = sess->delta_pool;
        for (uint32_t s = pool_slot_base; s < dp->slot_used; s++) {
            col_rel_t *pr = (col_rel_t *)(dp->slab
                + (size_t)s * dp->slot_size);
            col_rel_free_contents(pr);
        }
        dp->slot_used = pool_slot_base;
    }
    free(results);
    COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
    return rc;
}

/**
 * K-Fusion operator: evaluate K copies of a relation plan via workqueue,
 * merge results with deduplication, and push result onto stack.
 *
 * Each of the K operator sequences in opaque_data is submitted as a
 * separate worker task to the workqueue. The K workers evaluate in
 * parallel (or sequentially on single-threaded systems).
 * Results are merged via col_rel_merge_k() after all workers complete.
 */
int
col_op_k_fusion(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op->opaque_data)
        return EINVAL;

    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k == 0)
        return EINVAL;

    /* Issue #549: W=1 fast-path. Skip per-worker clone/arena/delta_pool
     * machinery when there's only one thread — pure overhead otherwise.
     * TDD workers already run under the distributed stratum workqueue; running
     * nested K-fusion workers inside them oversubscribes execution and divides
     * join_output_limit a second time. */
    if (sess->tdd_subpass_active || sess->coordinator
        || (sess->wq == NULL && sess->num_workers <= 1))
        return col_op_k_fusion_serial(op, stack, sess);

    /* Issue #560: Advance the compound-arena epoch frontier before
     * evaluating K-Fusion branch liveness so the parallel path preserves the
     * same coordinator epoch boundary ordering as worker dispatch. The
     * compound_arena is borrowed from the coordinator (Issue #579 / R-5);
     * only the coordinator may mutate it. */
    if (sess->coordinator == NULL
        && sess->compound_arena && sess->rotation_ops
        && sess->rotation_ops->gc_epoch_boundary) {
        sess->rotation_ops->gc_epoch_boundary(sess);
    }

    uint64_t _phase_t0 = now_ns();
    uint32_t *live_indices = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!live_indices)
        return ENOMEM;
    uint32_t live_count = 0;
    for (uint32_t d = 0; d < k; d++) {
        wl_plan_relation_t plan_data;
        memset(&plan_data, 0, sizeof(plan_data));
        plan_data.name = "<k_fusion_copy>";
        plan_data.delta_name = NULL;
        plan_data.ops = meta->k_ops[d];
        plan_data.op_count = meta->k_op_counts[d];
        if (!has_empty_forced_delta(&plan_data, sess, sess->current_iteration))
            live_indices[live_count++] = d;
    }
    if (live_count == 0) {
        COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
        uint32_t ncols = 0;
        if (op->relation_name) {
            col_rel_t *target = session_find_rel(sess, op->relation_name);
            if (target)
                ncols = target->ncols;
        }
        col_rel_t *empty = col_rel_new_auto("$kfusion_empty", ncols);
        free(live_indices);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push(stack, empty, true);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }

    /* Use session-level workqueue created at col_session_create (issue #99).
     * If this invocation cannot or should not dispatch parallel branch work,
     * use the existing serial K-fusion evaluator before allocating worker
     * sessions. */
    uint32_t active_workers = live_count < sess->num_workers
        ? live_count : sess->num_workers;
    wl_work_queue_t *wq = NULL; /* NULL when serial or in workers */
    if (active_workers > 1 && live_count >= WL_KFUSION_MIN_PARALLEL_K) {
        int ensure_rc = wl_columnar_session_ensure_workqueue(sess,
                active_workers);
        if (ensure_rc != 0) {
            free(live_indices);
            return ensure_rc;
        }
        wq = sess->wq;
    }
    if (!wq || live_count < WL_KFUSION_MIN_PARALLEL_K) {
        free(live_indices);
        return col_op_k_fusion_serial(op, stack, sess);
    }

    col_rel_t **results = (col_rel_t **)calloc(live_count, sizeof(col_rel_t *));
    col_op_k_fusion_worker_t *workers = (col_op_k_fusion_worker_t *)calloc(
        live_count, sizeof(col_op_k_fusion_worker_t));
    /* Per-worker session wrappers: shallow copy of sess with isolated mutable
     * caches so concurrent branch evaluation does not race on cache state. */
    wl_col_session_t *worker_sess
        = (wl_col_session_t *)calloc(live_count, sizeof(wl_col_session_t));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results || !workers || !worker_sess) {
        free(live_indices);
        free(results);
        free(workers);
        free(worker_sess);
        return ENOMEM;
    }

    int rc = 0;

    /* Issue #196: Workers start with zeroed mat_cache (no shared entries).
     * All worker cache entries are worker-owned; cleanup frees all of them
     * starting from index 0, so no base_count snapshot is needed. */

    /* Initialise per-worker session wrappers and submit only live tasks in one
     * batch so W acts as a cap instead of forcing allocation for skipped
     * delta-copy branches. */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < live_count; d++) {
        uint32_t branch_idx = live_indices[d];
        /* Shallow copy shares rels[], plan, etc. (read-only during K-fusion).
         * mat_cache is zeroed below (Issue #196): workers start fresh.
         * arrangement caches are zeroed below, so workers rebuild private
         * entries on demand instead of cloning coordinator state. */
        worker_sess[d] = *sess;
        worker_sess[d].wq = NULL; /* prevent nested K-fusion from workers */
        worker_sess[d].wq_workers = 0;
        worker_sess[d].num_workers = 1;
        worker_sess[d].tdd_workers = NULL;
        worker_sess[d].tdd_workers_cap = 0;
        worker_sess[d].tdd_workers_count = 0;
        if (worker_sess[d].join_output_limit > 0 && live_count > 1) {
            uint64_t per_branch =
                worker_sess[d].join_output_limit / live_count;
            worker_sess[d].join_output_limit =
                per_branch > 0 ? per_branch : 1;
        }
        /* NULL out owned resources before allocation so cleanup_wq is safe
         * even if we abort early (e.g. clone failure).  Each owned pointer
         * is replaced below; the parent session retains its own copies. */
        worker_sess[d].eval_arena = NULL;
        worker_sess[d].delta_pool = NULL;
        worker_sess[d].arr_entries = NULL;
        worker_sess[d].arr_count = 0;
        worker_sess[d].arr_cap = 0;
        worker_sess[d].arr_clock = sess->arr_clock;
        worker_sess[d].arr_total_bytes = 0;
        worker_sess[d].arr_cache_limit_bytes = sess->arr_cache_limit_bytes;
        worker_sess[d].diff_arr_entries = NULL;
        worker_sess[d].diff_arr_count = 0;
        worker_sess[d].diff_arr_cap = 0;
        worker_sess[d].darr_entries = NULL;
        worker_sess[d].darr_count = 0;
        worker_sess[d].darr_cap = 0;
        /* Issue #433: workers start with empty filt_arr (isolation safety).
         * Workers rebuild filt_arr from filt_cache if needed per dispatch. */
        worker_sess[d].filt_arr_entries = NULL;
        worker_sess[d].filt_arr_count = 0;
        worker_sess[d].filt_arr_cap = 0;
        worker_sess[d].filt_cache = NULL;
        worker_sess[d].filt_cache_count = 0;
        worker_sess[d].filt_cache_cap = 0;
        /* Issue #196: Workers start with empty mat_cache.  Divergent rule
         * copies have ~0% cache hit rate, so inheriting parent entries
         * wastes memory without benefit. */
        memset(&worker_sess[d].mat_cache, 0, sizeof(col_mat_cache_t));
        /* Issue #196: Per-worker arena isolation (arena.h contract: NOT
         * thread-safe, each worker must own its arena). */
        {
            size_t parent_cap
                = sess->eval_arena ? sess->eval_arena->capacity : 0;
            size_t worker_cap = parent_cap / live_count;
            if (worker_cap < 8 * 1024 * 1024)
                worker_cap = 8 * 1024 * 1024; /* 8MB minimum */
            worker_sess[d].eval_arena = wl_arena_create(worker_cap);
            /* NULL arena is handled gracefully: operators check before use */
        }
        /* Issue #196: Scale per-worker delta_pool inversely with active
         * branch count to keep aggregate memory ~constant. */
        {
            size_t pool_arena = 32 * 1024 * 1024 / live_count;
            if (pool_arena < 4 * 1024 * 1024)
                pool_arena = 4 * 1024 * 1024; /* 4MB minimum */
            uint32_t pool_slots = 128 / live_count;
            if (pool_slots < 16)
                pool_slots = 16;
            worker_sess[d].delta_pool
                = delta_pool_create(pool_slots, sizeof(col_rel_t), pool_arena);
        }

        workers[d].plan_data.name = "<k_fusion_copy>";
        workers[d].plan_data.ops = meta->k_ops[branch_idx];
        workers[d].plan_data.op_count = meta->k_op_counts[branch_idx];
        workers[d].sess = &worker_sess[d];
        workers[d].rc = 0;

        if (wq) {
            /* Parallel path: submit to session workqueue (issue #99) */
            if (wl_workqueue_submit(wq, col_op_k_fusion_worker, &workers[d])
                != 0) {
                rc = ENOMEM;
                wl_workqueue_drain(wq);
                goto cleanup_wq;
            }
        } else {
            /* Sequential fallback: execute directly (num_workers=1) */
            col_op_k_fusion_worker(&workers[d]);
        }
    }

    /* Barrier: wait for all parallel workers to complete.
     * Skipped when wq is NULL (sequential path already finished). */
    if (wq && wl_workqueue_wait_all(wq) != 0) {
        rc = -1;
        goto cleanup_wq;
    }
    COL_SESSION(sess)->kfusion_dispatch_ns += now_ns() - _phase_t0;

    /* Issue #177: Merge worker profile counters back to session.
     * K-fusion workers accumulate profiling stats (join_calls, join_unary,
     * etc.) during parallel evaluation. Aggregate these counters to the
     * session profile for comprehensive profiling. */
#ifdef WL_PROFILE
    {
        wl_profile_t base_profile = sess->profile;
        for (uint32_t d = 0; d < live_count; d++) {
            /* Merge counters: sum increments from baseline */
            sess->profile.join_calls
                += worker_sess[d].profile.join_calls - base_profile.join_calls;
            sess->profile.join_unary
                += worker_sess[d].profile.join_unary - base_profile.join_unary;
            sess->profile.join_binary += worker_sess[d].profile.join_binary
                - base_profile.join_binary;
            sess->profile.seminaive_ops += worker_sess[d].profile.seminaive_ops
                - base_profile.seminaive_ops;
        }
    }
#endif

    /* Collect results from each worker's eval_stack */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < live_count; d++) {
        if (workers[d].rc != 0) {
            rc = workers[d].rc;
            goto cleanup_results;
        }

        eval_entry_t e = eval_stack_pop(&workers[d].stack);
        if (!e.rel) {
            rc = EINVAL;
            eval_stack_drain(&workers[d].stack);
            goto cleanup_results;
        }

        /* If not owned, share columns zero-copy (6B optimization).
         * The source relation outlives the merge, so borrowing is safe. */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(worker_sess[d].delta_pool,
                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                eval_stack_drain(&workers[d].stack);
                goto cleanup_results;
            }
            copy->col_shared = (bool *)calloc(e.rel->ncols, sizeof(bool));
            if (copy->col_shared) {
                for (uint32_t c = 0; c < e.rel->ncols; c++) {
                    free(copy->columns[c]); /* free pool-allocated column */
                    copy->columns[c] = e.rel->columns[c];
                    copy->col_shared[c] = true;
                }
            } else {
                /* Fallback: deep copy on alloc failure */
                for (uint32_t c = 0; c < e.rel->ncols; c++)
                    memcpy(copy->columns[c], e.rel->columns[c],
                        (size_t)e.rel->nrows * sizeof(int64_t));
            }
            copy->nrows = e.rel->nrows;
            results[d] = copy;
        } else {
            results[d] = e.rel;
        }
        eval_stack_drain(&workers[d].stack);
    }

    /* Merge live branch results with deduplication.
     * Workers ran WL_PLAN_OP_CONSOLIDATE as the last plan op, so each
     * result is already sorted+deduped — no qsort needed here. */
    {
        /* Compact non-NULL results. Use the
         * existing results array as backing — we build compact in-place. */
        col_rel_t **compact
            = (col_rel_t **)malloc(live_count * sizeof(col_rel_t *));
        if (!compact) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        uint32_t n_results = 0;
        for (uint32_t d = 0; d < live_count; d++) {
            if (results[d])
                compact[n_results++] = results[d];
        }

        col_rel_t *merged;
        if (n_results == 0) {
            /* Defensive fallback: produce empty output with the target
             * relation schema if no worker produced a relation. */
            uint32_t ncols = 0;
            if (op->relation_name) {
                col_rel_t *target = session_find_rel(sess, op->relation_name);
                if (target)
                    ncols = target->ncols;
            }
            merged = col_rel_new_auto("$kfusion_empty", ncols);
        } else {
            merged = col_rel_merge_k(compact, n_results);
        }
        free(compact);
        if (!merged) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        rc = eval_stack_push(stack, merged, true);
        if (rc != 0)
            col_rel_destroy(merged);
    }
    COL_SESSION(sess)->kfusion_merge_ns += now_ns() - _phase_t0;

cleanup_results:
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < live_count; d++) {
        if (results[d])
            col_rel_destroy(results[d]);
        eval_stack_drain(&workers[d].stack);
    }

cleanup_wq:
    /* On early-exit paths (submit failure, wait failure) _phase_t0 may hold
    * a stale dispatch value; reset it here so cleanup timing is correct. */
    _phase_t0 = now_ns();
    /* wq is session-owned and reused across iterations — do not destroy here.
     * Workers start with empty mat_cache (Issue #196), so all entries are
     * worker-owned and freed from index 0.
     * Free each worker's private arrangement caches (arr_* and darr_*).
     * Lock-free design: no synchronization needed because each worker owns
     * its isolated cache — no races at cleanup time. */
    for (uint32_t d = 0; d < live_count; d++) {
        eval_stack_drain(&workers[d].stack);
        col_mat_cache_t *wc = &worker_sess[d].mat_cache;
        /* Issue #196: worker mat_cache starts empty (zeroed above), so ALL
         * entries were created by this worker — free from index 0. */
        for (uint32_t i = 0; i < wc->count; i++)
            col_rel_destroy(wc->entries[i].result);
        /* Issue #216: merge worker lru_clocks back into coordinator so
         * arrangements accessed by any worker are counted as recently used.
         * Worker entries were cloned in the same order as coordinator entries,
         * so index-matched comparison is valid. */
        {
            wl_col_session_t *cs = COL_SESSION(sess);
            uint32_t shared = worker_sess[d].arr_count < cs->arr_count
                ? worker_sess[d].arr_count
                : cs->arr_count;
            for (uint32_t i = 0; i < shared; i++) {
                col_arr_entry_t *wk = &worker_sess[d].arr_entries[i];
                col_arr_entry_t *co = &cs->arr_entries[i];
                if (wk->lru_clock > co->lru_clock)
                    co->lru_clock = wk->lru_clock;
            }
            /* Advance coordinator clock once outside the loop. */
            if (worker_sess[d].arr_clock > cs->arr_clock)
                cs->arr_clock = worker_sess[d].arr_clock;
        }
        /* Free worker's private full-arrangement cache (arr_*). */
        for (uint32_t i = 0; i < worker_sess[d].arr_count; i++) {
            col_arr_entry_t *e = &worker_sess[d].arr_entries[i];
            free(e->rel_name);
            free(e->key_cols);
            arr_free_contents(&e->arr);
        }
        free(worker_sess[d].arr_entries);
        /* Free worker's private delta-arrangement cache (darr_*). */
        col_session_free_delta_arrangements(&worker_sess[d]);
        /* Free worker's private diff-arrangement cache (diff_arr_*). */
        col_session_free_diff_arrangements(&worker_sess[d]);
        /* Free worker's private filtered arrangement cache (filt_arr_*). */
        col_session_free_filt_arrangements(&worker_sess[d]);
        for (uint32_t i = 0; i < worker_sess[d].filt_cache_count; i++) {
            free(worker_sess[d].filt_cache[i].rel_name);
            free(worker_sess[d].filt_cache[i].filter_data);
            if (worker_sess[d].filt_cache[i].filtered)
                col_rel_destroy(worker_sess[d].filt_cache[i].filtered);
        }
        free(worker_sess[d].filt_cache);
        /* Free contents of pool-allocated relations before bulk destroy.
         * delta_pool_destroy frees the slab/arena but skips individually
         * malloc'd members (name, columns, col_names) -- leaks under ASAN.
         * col_rel_free_contents zeroes each slot, so already-destroyed
         * relations (via mat_cache or results cleanup) are safe no-ops. */
        {
            delta_pool_t *dp = worker_sess[d].delta_pool;
            if (dp) {
                for (uint32_t s = 0; s < dp->slot_used; s++) {
                    col_rel_t *pr = (col_rel_t *)(dp->slab
                        + (size_t)s * dp->slot_size);
                    col_rel_free_contents(pr);
                }
            }
        }
        delta_pool_destroy(worker_sess[d].delta_pool);
        wl_arena_free(worker_sess[d].eval_arena);
    }
    free(worker_sess);
    free(results);
    free(workers);
    free(live_indices);
    COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
    return rc;
}

/* --- SEMIJOIN ------------------------------------------------------------ */

int
col_op_semijoin(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    col_rel_t *right_filtered = NULL;
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right)
        return eval_stack_push(stack, left_e.rel, left_e.owned);

    /* Issue #386: semijoin filter caching is not yet implemented.
     * Semijoin always uses an ephemeral pool-allocated filtered relation, so
     * the per-iteration filter cost is O(N) — acceptable for current workloads
     * but a candidate for follow-up optimization. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered
            = apply_right_filter(&op->right_filter_expr, right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "SEMIJOIN",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right", "SEMIJOIN",
                op->right_relation);
    }

    /* Output: project_indices selects output columns from left */
    uint32_t ocols = op->project_count ? op->project_count : left->ncols;
    col_rel_t *out
        = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena, "$semijoin",
            ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* Build hash set from right relation join keys: O(|R|) */
    uint32_t nbuckets = next_pow2(right->nrows > 0 ? right->nrows * 2 : 1);
    uint32_t *ht_head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
    uint32_t *ht_next = (uint32_t *)malloc((right->nrows > 0 ? right->nrows : 1)
            * sizeof(uint32_t));
    if (!ht_head || !ht_next) {
        free(ht_head);
        free(ht_next);
        free(tmp);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
            & (nbuckets - 1);
        ht_next[rr] = ht_head[h];
        ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
    }

    int sj_rc = 0;
    uint32_t min_left = col_join_parallel_min_left_rows();
    if (min_left == 0)
        min_left = 1;
    uint32_t W = left->nrows / min_left;
    if (W > sess->num_workers)
        W = sess->num_workers;
    if (!sess->coordinator && W > 1 && right->nrows > 0) {
        int ensure_rc = wl_columnar_session_ensure_workqueue(sess, W);
        if (ensure_rc == 0) {
            col_semijoin_ctx_t *ctxs = (col_semijoin_ctx_t *)calloc(
                W, sizeof(col_semijoin_ctx_t));
            uint64_t *offsets = (uint64_t *)calloc(W + 1,
                    sizeof(uint64_t));
            uint32_t *left_hashes = (uint32_t *)malloc(
                sizeof(uint32_t) * (size_t)(left->nrows ? left->nrows : 1));
            if (ctxs && offsets) {
                uint32_t chunk = (left->nrows + W - 1u) / W;
                int prc = 0;
                for (uint32_t w = 0; w < W; w++) {
                    uint32_t begin = w * chunk;
                    uint32_t end = begin + chunk;
                    if (begin > left->nrows)
                        begin = left->nrows;
                    if (end > left->nrows)
                        end = left->nrows;
                    ctxs[w].left = left;
                    ctxs[w].right = right;
                    ctxs[w].lk = lk;
                    ctxs[w].rk = rk;
                    ctxs[w].kc = kc;
                    ctxs[w].op = op;
                    ctxs[w].ht_head = ht_head;
                    ctxs[w].ht_next = ht_next;
                    ctxs[w].nbuckets = nbuckets;
                    ctxs[w].begin = begin;
                    ctxs[w].end = end;
                    ctxs[w].left_hashes = left_hashes;
                    if (wl_workqueue_submit(sess->wq,
                        col_semijoin_count_worker_fn, &ctxs[w]) != 0)
                        prc = ENOMEM;
                }
                wl_workqueue_wait_all(sess->wq);
                uint64_t total = 0;
                for (uint32_t w = 0; w < W; w++) {
                    offsets[w] = total;
                    total += ctxs[w].count;
                }
                offsets[W] = total;
                if (prc == 0 && total > UINT32_MAX)
                    prc = ENOMEM;
                if (prc == 0 && col_join_reserve_exact(out,
                    (uint32_t)total) != 0)
                    prc = ENOMEM;
                if (prc == 0) {
                    out->nrows = (uint32_t)total;
                    if (out->timestamps && total > 0)
                        memset(out->timestamps, 0,
                            (size_t)total * sizeof(col_delta_timestamp_t));
                    for (uint32_t w = 0; w < W; w++) {
                        ctxs[w].out = out;
                        ctxs[w].out_begin = offsets[w];
                        if (wl_workqueue_submit(sess->wq,
                            col_semijoin_fill_worker_fn, &ctxs[w]) != 0)
                            prc = ENOMEM;
                    }
                    wl_workqueue_wait_all(sess->wq);
                }
                if (prc != 0)
                    out->nrows = 0;
                free(left_hashes);
                free(offsets);
                free(ctxs);
                if (prc == 0)
                    goto semijoin_done;
            } else {
                free(left_hashes);
                free(offsets);
                free(ctxs);
            }
        }
    }

    /* Probe: for each left row test membership, emit if found: O(|L|) */
    for (uint32_t lr = 0; lr < left->nrows && sj_rc == 0; lr++) {
        uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
            & (nbuckets - 1);
        bool found = false;
        for (uint32_t e = ht_head[h]; e != 0 && !found; e = ht_next[e - 1]) {
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                found = true;
        }
        if (found) {
            if (op->project_count > 0 && op->project_indices) {
                for (uint32_t c = 0; c < ocols; c++) {
                    uint32_t si = op->project_indices[c];
                    tmp[c] = (si < left->ncols) ? left->columns[si][lr] : 0;
                }
            } else {
                for (uint32_t c = 0; c < left->ncols; c++)
                    tmp[c] = left->columns[c][lr];
            }
            sj_rc = col_rel_append_row(out, tmp);
        }
    }

semijoin_done:
    free(ht_head);
    free(ht_next);
    if (sj_rc != 0) {
        free(tmp);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return sj_rc;
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push(stack, out, true);
}

/* --- REDUCE (aggregate) -------------------------------------------------- */

int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t gc = op->group_by_count;

    /* Output: group_by columns + 1 aggregate column */
    uint32_t ocols = gc + 1;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$reduce", ocols);
    if (!out) {
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    col_expr_compiled_t *agg_ce = NULL;
    if (op->agg_expr.data && op->agg_expr.size > 0)
        agg_ce = col_expr_compile(op->agg_expr.data, op->agg_expr.size);

    /* Row scratch, hoisted out of the loop (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, in->ncols)) {
        col_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    typedef struct {
        uint64_t hash;
        uint32_t row;
    } reduce_group_slot_t;
    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)(in->nrows ? in->nrows : 1) * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired) {
        col_row_buf_release(&row_rb);
        col_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }
    reduce_group_slot_t *groups = (reduce_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups) {
        col_row_buf_release(&row_rb);
        col_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }
    uint32_t map_mask = map_cap - 1;

    /* Index groups by their key so reduction remains linear in the number of
     * input rows rather than scanning every output group. */
    int64_t *const row = row_rb.ptr;
    for (uint32_t r = 0; r < in->nrows; r++) {
        col_rel_row_copy_out(in, r, row);
        int64_t agg_val = (in->ncols > gc) ? row[gc] : 1;
        if (op->agg_fn != WIRELOG_AGG_COUNT
            && op->agg_expr.data && op->agg_expr.size > 0) {
            if (agg_ce) {
                int64_t val = 0;
                if (col_eval_expr_compiled(agg_ce, row, in->ncols, &val) == 0)
                    agg_val = val;
                else {
                    col_row_buf_release(&row_rb);
                    col_expr_compiled_free(agg_ce);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
            } else {
                int64_t val = 0;
                if (col_eval_expr_i64(op->agg_expr.data, op->agg_expr.size,
                    row, in->ncols, &val, sess->intern) == 0) {
                    agg_val = val;
                } else {
                    col_row_buf_release(&row_rb);
                    col_expr_compiled_free(agg_ce);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
            }
        }

        /* Use an open-addressed key index instead of scanning all output
         * groups for every input row. */
        uint64_t hash = UINT64_C(1469598103934665603);
        for (uint32_t k = 0; k < gc; k++) {
            uint32_t gi = op->group_by_indices ? op->group_by_indices[k] : k;
            hash ^= (uint64_t)row[gi < in->ncols ? gi : 0];
            hash *= UINT64_C(1099511628211);
        }
        if (!hash)
            hash = 1;
        uint32_t slot = (uint32_t)hash & map_mask;
        bool found = false;
        uint32_t group_row = UINT32_MAX;
        while (groups[slot].hash != 0) {
            bool match = groups[slot].hash == hash;
            for (uint32_t k = 0; k < gc && match; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                match = row[gi < in->ncols ? gi : 0]
                    == col_rel_get(out, groups[slot].row, k);
            }
            if (match) {
                found = true;
                group_row = groups[slot].row;
                break;
            }
            slot = (slot + 1) & map_mask;
        }
        if (found) {
            /* Update aggregate */
            int64_t cur = col_rel_get(out, group_row, gc);
            switch (op->agg_fn) {
            case WIRELOG_AGG_COUNT:
                col_rel_set(out, group_row, gc, cur + 1);
                break;
            case WIRELOG_AGG_SUM:
            {
                int64_t next;
                if (wl_columnar_ops_checked_add_int64(cur, agg_val,
                    &next) != 0) {
                    col_row_buf_release(&row_rb);
                    col_expr_compiled_free(agg_ce);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
                col_rel_set(out, group_row, gc, next);
            }
            break;
            case WIRELOG_AGG_MIN:
            case WIRELOG_AGG_MAX:
                /* Ordered by the operand's declared domain, not by the
                 * raw int64 -- for a symbol column that int64 is an
                 * intern id (Issue #965). */
                if (col_agg_better(op->agg_fn, op->agg_operand_type,
                    sess->intern, agg_val, cur))
                    col_rel_set(out, group_row, gc, agg_val);
                break;
            default:
                break;
            }
        }
        if (!found) {
            for (uint32_t k = 0; k < gc; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                tmp[k] = row[gi < in->ncols ? gi : 0];
            }
            tmp[gc] = (op->agg_fn == WIRELOG_AGG_COUNT) ? 1 : agg_val;
            int rc = col_rel_append_row(out, tmp);
            if (rc != 0) {
                col_row_buf_release(&row_rb);
                col_expr_compiled_free(agg_ce);
                free(groups);
                free(tmp);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(in);
                return rc;
            }
            groups[slot].hash = hash;
            groups[slot].row = out->nrows - 1;
        }
    }

    col_row_buf_release(&row_rb);
    col_expr_compiled_free(agg_ce);
    free(groups);
    free(tmp);
    if (e.owned)
        col_rel_destroy(in);
    return eval_stack_push(stack, out, true);
}

/* --- REDUCE WEIGHTED (Z-set / Mobius COUNT) ------------------------------ */

/*
 * col_op_reduce_weighted:
 * Global COUNT aggregation using Z-set (signed multiplicity) semantics.
 * Output: one row whose data value = sum of input multiplicities, and whose
 * timestamp.multiplicity = the same sum.
 *
 * src - input relation; src->timestamps[i].multiplicity carries each row's
 *       signed weight.
 * dst - output relation (caller-allocated, empty on entry, ncols >= 1).
 *
 * Returns 0 on success, EINVAL / ENOMEM on error.
 */
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst)
{
    if (!src || !dst)
        return EINVAL;

    /* Sum all input multiplicities. */
    int64_t total = 0;
    if (src->timestamps) {
        for (uint32_t i = 0; i < src->nrows; i++)
            total += src->timestamps[i].multiplicity;
    } else {
        /* No timestamp tracking: treat each row as multiplicity 1. */
        total = (int64_t)src->nrows;
    }

    /* Allocate timestamp tracking on dst if not already present. */
    if (!dst->timestamps) {
        dst->timestamps
            = (col_delta_timestamp_t *)calloc(1, sizeof(col_delta_timestamp_t));
        if (!dst->timestamps)
            return ENOMEM;
        dst->capacity = (dst->capacity == 0) ? 1 : dst->capacity;
    }

    /* Allocate column buffers for one output row if not already present. */
    if (!dst->columns) {
        uint32_t ncols = dst->ncols ? dst->ncols : 1;
        dst->columns = col_columns_alloc(ncols, 1);
        if (!dst->columns)
            return ENOMEM;
        /* Zero-initialize the single row */
        for (uint32_t c = 0; c < ncols; c++)
            dst->columns[c][0] = 0;
        dst->capacity = 1;
    }

    /* Write the single aggregate row. */
    col_rel_set(dst, 0, 0, total);
    dst->nrows = 1;

    /* Set output row multiplicity. */
    memset(&dst->timestamps[0], 0, sizeof(col_delta_timestamp_t));
    dst->timestamps[0].multiplicity = total;

    return 0;
}

/* ======================================================================== */
/* LFTJ Operator (Issue #195)                                               */
/* ======================================================================== */

/*
 * lftj_binary_ctx_t: callback context for col_op_lftj.
 *
 * wl_lftj_join delivers rows in compact format:
 *   [key, non_key_rel0..., non_key_rel1..., ...]
 *
 * This context reconstructs binary-join-compatible rows:
 *   [all_rel0_cols, all_rel1_cols, ...]  (key duplicated per relation)
 *
 * The downstream WL_PLAN_OP_MAP project_indices are unchanged because the
 * output column layout matches what a cascade of WL_PLAN_OP_JOIN produces.
 */
typedef struct {
    uint32_t k;
    uint32_t *ncols;          /* per-relation column count (k entries)    */
    uint32_t *key_cols;       /* per-relation join key column (k entries) */
    uint32_t *lftj_offsets;   /* start of Ri's non-key cols in LFTJ row  */
    uint32_t *binary_offsets; /* start of Ri's cols in binary output     */
    uint32_t total_binary_ncols;
    int64_t *tmp;   /* scratch row buffer                       */
    col_rel_t *out; /* destination relation                     */
    int rc;         /* first error code encountered; 0 = ok    */
} lftj_binary_ctx_t;

/*
 * lftj_binary_cb: output callback for col_op_lftj.
 *
 * Converts compact LFTJ output to binary-join-compatible format and appends
 * the result to ctx->out.  Sets ctx->rc on allocation failure (subsequent
 * calls are no-ops).
 */
static void
lftj_binary_cb(const int64_t *row, uint32_t lftj_ncols, void *user)
{
    (void)lftj_ncols;
    lftj_binary_ctx_t *ctx = (lftj_binary_ctx_t *)user;
    if (ctx->rc)
        return; /* already OOM; skip remaining rows */

    const int64_t key = row[0];
    for (uint32_t i = 0; i < ctx->k; i++) {
        uint32_t nc = ctx->ncols[i];
        uint32_t kc = ctx->key_cols[i];
        uint32_t lo = ctx->lftj_offsets[i];
        uint32_t bo = ctx->binary_offsets[i];
        for (uint32_t c = 0; c < nc; c++) {
            int64_t val;
            if (c == kc)
                val = key;
            else if (c < kc)
                val = row[lo + c];
            else
                val = row[lo + c - 1u];
            ctx->tmp[bo + c] = val;
        }
    }
    int rc = col_rel_append_row(ctx->out, ctx->tmp);
    if (rc)
        ctx->rc = rc;
}

/*
 * col_op_lftj: execute a WL_PLAN_OP_LFTJ operator.
 *
 * Performs a multi-way leapfrog triejoin over the k EDB relations named in
 * op->opaque_data.  Uses the sorted arrangement cache to avoid re-sorting
 * on repeated calls (the sort inside wl_lftj_join degrades to O(N) when the
 * input is already sorted).  Pushes binary-join-compatible result onto stack.
 */
int
col_op_lftj(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    if (!op->opaque_data)
        return EINVAL;
    const wl_plan_op_lftj_t *meta = (const wl_plan_op_lftj_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k < 2u || !meta->rel_names || !meta->key_cols)
        return EINVAL;

    /* Allocate per-relation working arrays. */
    wl_lftj_input_t *inputs
        = (wl_lftj_input_t *)calloc(k, sizeof(wl_lftj_input_t));
    uint32_t *ncols = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *lftj_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *binary_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!inputs || !ncols || !lftj_offsets || !binary_offsets) {
        free(inputs);
        free(ncols);
        free(lftj_offsets);
        free(binary_offsets);
        return ENOMEM;
    }

    /* Resolve each relation and populate LFTJ input descriptors. */
    uint32_t total_binary_ncols = 0u;
    uint32_t lftj_nk_total = 0u;
    int rc = 0;
    for (uint32_t i = 0; i < k; i++) {
        col_rel_t *rel = session_find_rel(sess, meta->rel_names[i]);
        if (!rel) {
            rc = ENOENT;
            goto cleanup_arrays;
        }
        uint32_t kc = meta->key_cols[i];
        if (kc >= rel->ncols) {
            rc = EINVAL;
            goto cleanup_arrays;
        }

        /* Use the pre-sorted arrangement when available: wl_lftj_join still
         * copies and sorts internally, but starting from a sorted copy
         * reduces its qsort from O(N log N) to O(N). */
        col_sorted_arr_t *sarr
            = col_session_get_sorted_arrangement(sess, meta->rel_names[i], kc);
        if (sarr && sarr->indexed_rows == rel->nrows && sarr->nrows > 0) {
            inputs[i].data = sarr->sorted;
            inputs[i].nrows = sarr->nrows;
        } else {
            /* Gather column-major into flat buffer for LFTJ */
            int64_t *flat = (int64_t *)malloc(
                (size_t)rel->nrows * rel->ncols * sizeof(int64_t));
            if (!flat) {
                /* Free previously allocated flat buffers */
                for (uint32_t j = 0; j < i; j++) {
                    if (inputs[j].data != NULL) {
                        col_sorted_arr_t *prev_sarr
                            = col_session_get_sorted_arrangement(sess,
                                meta->rel_names[j], meta->key_cols[j]);
                        if (!(prev_sarr
                            && prev_sarr->indexed_rows
                            == inputs[j].nrows
                            && prev_sarr->nrows > 0))
                            free((void *)inputs[j].data);
                    }
                }
                rc = ENOMEM;
                goto cleanup_arrays;
            }
            for (uint32_t r = 0; r < rel->nrows; r++)
                col_rel_row_copy_out(rel, r,
                    flat + (size_t)r * rel->ncols);
            inputs[i].data = flat;
            inputs[i].nrows = rel->nrows;
        }
        inputs[i].ncols = rel->ncols;
        inputs[i].key_col = kc;

        ncols[i] = rel->ncols;
        binary_offsets[i] = total_binary_ncols;
        lftj_offsets[i] = 1u + lftj_nk_total; /* 1: shared key lives at [0] */
        total_binary_ncols += rel->ncols;
        lftj_nk_total += rel->ncols - 1u;
    }

    {
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$lftj",
                total_binary_ncols);
        int64_t *tmp = (int64_t *)malloc(
            (total_binary_ncols ? total_binary_ncols : 1u) * sizeof(int64_t));
        if (!out || !tmp) {
            free(tmp);
            if (out)
                col_rel_destroy(out);
            rc = ENOMEM;
            goto cleanup_arrays;
        }

        lftj_binary_ctx_t ctx = { k,
                                  ncols,
                                  meta->key_cols,
                                  lftj_offsets,
                                  binary_offsets,
                                  total_binary_ncols,
                                  tmp,
                                  out,
                                  0 };

        rc = wl_lftj_join(inputs, k, lftj_binary_cb, &ctx);
        if (rc == 0)
            rc = ctx.rc;

        free(tmp);
        if (rc != 0) {
            col_rel_destroy(out);
            goto cleanup_arrays;
        }
        rc = eval_stack_push(stack, out, true);
    }

cleanup_arrays:
    /* Free flat buffers allocated for non-sarr LFTJ inputs */
    if (inputs) {
        for (uint32_t i = 0; i < k; i++) {
            if (inputs[i].data) {
                col_sorted_arr_t *sarr2
                    = col_session_get_sorted_arrangement(sess,
                        meta->rel_names[i], meta->key_cols[i]);
                if (!(sarr2 && sarr2->sorted == inputs[i].data))
                    free((void *)inputs[i].data);
            }
        }
    }
    free(inputs);
    free(ncols);
    free(lftj_offsets);
    free(binary_offsets);
    return rc;
}

/* --- DIFFERENTIAL JOIN --------------------------------------------------- */

/*
 * col_op_join_diff - Differential join with arrangement reuse (Issue #263).
 *
 * Key optimization over col_op_join:
 *   - Uses col_diff_arrangement_t as a persistent hash index
 *   - Only hashes NEW rows (delta) since last iteration: O(D) vs O(N)
 *   - Hash table persists across iterations within an epoch
 *
 * Guard: activated when sess->diff_operators_active is true
 *        (affected_strata < full_mask, i.e., partial insertion)
 *
 * Falls back to ephemeral hash table when:
 *   - No key columns (kc == 0)
 *   - Delta-substituted right relation (no persistent arrangement)
 *   - Diff arrangement creation/resize fails
 */
int
col_op_join_diff(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    col_rel_t *right_filtered = NULL;
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        uint32_t empty_cols = (op->project_count > 0 && op->project_indices)
            ? op->project_count : left_e.rel->ncols;
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$join_diff_empty", empty_cols);
        if (!out) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        if (left_e.owned)
            col_rel_destroy(left_e.rel);
        return eval_stack_push_delta(stack, out, true, false);
    }

    /* Right-side delta substitution (same logic as col_op_join) */
    bool used_right_delta = false;
    if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        /* Issue #472: mirror VARIABLE op retraction-aware pattern —
         * fall back to $r$<name> when retraction_seeded at iteration 0. */
        if (!rdelta && sess->retraction_seeded
            && sess->current_iteration == 0) {
            if (retraction_rel_name(op->right_relation, rdname,
                sizeof(rdname)) == 0)
                rdelta = session_find_rel(sess, rdname);
        }
        if (rdelta && rdelta->nrows > 0) {
            right = rdelta;
            used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded
            || sess->retraction_seeded) {
            uint32_t ocols = col_join_output_width(left_e.rel, right, op);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            col_rel_t *empty = col_rel_new_auto("$join_diff_empty", ocols);
            if (!empty)
                return ENOMEM;
            int push_rc = eval_stack_push(stack, empty, true);
            if (push_rc != 0)
                col_rel_destroy(empty);
            return push_rc;
        }
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && op->right_relation
        && (!left_e.is_delta || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0))) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && (((rdelta->nrows > 0
            && rdelta->nrows < right->nrows) || (rdelta->nrows > 0
            && sess->tdd_subpass_active))
            || (sess->tdd_outbound_only_active
            && sess->current_iteration > 0))) {
            right = rdelta;
            used_right_delta = true;
        }
    }
    /* Issue #472: Retraction right-pass (same as col_op_join). */
    if (!used_right_delta && sess->retraction_right_pass
        && sess->current_iteration == 0 && op->right_relation) {
        char rdname[256];
        if (retraction_rel_name(op->right_relation, rdname,
            sizeof(rdname)) == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rdname);
            if (rdelta && rdelta->nrows > 0) {
                right = rdelta;
                used_right_delta = true;
            }
        }
    }

    /* Apply constant filter on right child (from FILTER wrappers collected
     * during plan generation).  Use session-level cache (Issue #386): the
     * filtered relation is owned by sess->filt_cache and must NOT be
     * destroyed here.  right_filtered remains NULL for the cached path. */
    if (op->right_filter_expr.size > 0 && op->right_relation
        && !used_right_delta) {
        col_rel_t *filtered = apply_right_filter_cached(sess,
                &op->right_filter_expr, op->right_relation, right);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        /* right_filtered stays NULL: cache owns the relation */
    } else if (op->right_filter_expr.size > 0) {
        /* Delta path or no relation name: fall back to pool-allocated filter */
        col_rel_t *filtered = apply_right_filter(&op->right_filter_expr, right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    /* Materialization cache check */
    bool projected_join = op->project_count > 0 && op->project_indices;
    if (op->materialized && !projected_join) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached) {
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return eval_stack_push_delta(stack, cached, false,
                       left_e.is_delta || used_right_delta);
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;

    /* Resolve key column positions */
    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "JOIN(diff)",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right",
                "JOIN(diff)",
                op->right_relation);
    }

    uint32_t ocols = col_join_output_width(left, right, op);
    /* Materialized results outlive the current delta-pool reset while they
     * remain in mat_cache, so cache-owned joins must be heap allocated. */
    col_rel_t *out = (op->materialized && !projected_join)
        ? col_rel_new_auto("$join_diff", ocols)
        : col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$join_diff", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    col_join_attach_ledger(sess, out);

    /* Backpressure check (Issue #224) */
    if (wl_mem_ledger_should_backpressure(&sess->mem_ledger,
        WL_MEM_SUBSYS_RELATION, 80)) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return eval_stack_push(stack, out, true);
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    int join_rc = 0;

    /* DIFFERENTIAL PATH: persistent diff_arrangement for non-delta right.
     * The arrangement persists across iterations, only indexing new rows. */
    col_diff_arrangement_t *darr = NULL;
    if (kc > 0 && op->right_relation && !used_right_delta
        && op->right_filter_expr.size == 0)
        darr = col_session_get_diff_arrangement(sess, op->right_relation, rk,
                kc);

    if (darr
        && col_diff_arrangement_ensure_ht_capacity(darr, right->nrows) != 0)
        darr = NULL; /* capacity grow failed; fall through to ephemeral */

    if (darr) {
        /* Incrementally add new rows [indexed_rows, right->nrows) to hash */
        uint32_t indexed = darr->indexed_rows;
        uint32_t nbk = darr->nbuckets;
        for (uint32_t rr = indexed; rr < right->nrows; rr++) {
            uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc) & (nbk - 1);
            darr->ht_next[rr] = darr->ht_head[h];
            darr->ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
        }
        darr->indexed_rows = right->nrows;
        darr->current_nrows = right->nrows;

        uint32_t min_left = col_join_parallel_min_left_rows();
        if (min_left == 0)
            min_left = 1;
        uint32_t W = left->nrows / min_left;
        if (W > sess->num_workers)
            W = sess->num_workers;
        if (!sess->coordinator && W > 1) {
            int ensure_rc = wl_columnar_session_ensure_workqueue(sess, W);
            if (ensure_rc != 0) {
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ensure_rc;
            }
            col_join_keyed_ctx_t *ctxs = (col_join_keyed_ctx_t *)calloc(
                W, sizeof(col_join_keyed_ctx_t));
            uint64_t *offsets = (uint64_t *)calloc(W + 1, sizeof(uint64_t));
            uint32_t *left_hashes = (uint32_t *)malloc(
                sizeof(uint32_t)
                * (size_t)(left->nrows ? left->nrows : 1));
            if (!ctxs || !offsets) {
                free(ctxs);
                free(offsets);
                free(left_hashes);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
            uint32_t chunk = (left->nrows + W - 1u) / W;
            uint64_t pair_budget = wl_mem_ledger_bytes_remaining(
                &sess->mem_ledger) / 8u;
            if (left->nrows < WL_JOIN_PAIR_CACHE_MIN_LEFT_ROWS)
                pair_budget = 0;
            if (pair_budget > WL_JOIN_PAIR_CACHE_MAX_BYTES)
                pair_budget = WL_JOIN_PAIR_CACHE_MAX_BYTES;
            uint64_t pair_budget_per_worker = W > 0 ? pair_budget / W : 0;
            uint32_t pair_cap_limit = pair_budget_per_worker
                / sizeof(col_join_pair_ref_t) > UINT32_MAX
                ? UINT32_MAX
                : (uint32_t)(pair_budget_per_worker
                / sizeof(col_join_pair_ref_t));
            atomic_bool stop = ATOMIC_VAR_INIT(false);
            atomic_uint_fast64_t shared_count = ATOMIC_VAR_INIT(0);
            int prc = 0;
            for (uint32_t w = 0; w < W; w++) {
                uint32_t begin = w * chunk;
                uint32_t end = begin + chunk;
                if (begin > left->nrows)
                    begin = left->nrows;
                if (end > left->nrows)
                    end = left->nrows;
                ctxs[w].left = left;
                ctxs[w].right = right;
                ctxs[w].darr = darr;
                ctxs[w].lk = lk;
                ctxs[w].rk = rk;
                ctxs[w].kc = kc;
                ctxs[w].op = op;
                ctxs[w].begin = begin;
                ctxs[w].end = end;
                ctxs[w].limit = sess->join_output_limit;
                ctxs[w].left_hashes = left_hashes;
                ctxs[w].pair_cap_limit = pair_cap_limit;
                ctxs[w].pairs_complete = true;
                ctxs[w].stop = &stop;
                ctxs[w].shared_count = &shared_count;
                if (wl_workqueue_submit(sess->wq,
                    col_join_keyed_count_worker_fn, &ctxs[w]) != 0)
                    prc = ENOMEM;
            }
            wl_workqueue_wait_all(sess->wq);
            if (atomic_load_explicit(&stop, memory_order_relaxed)
                && prc == 0)
                prc = EOVERFLOW;
            uint64_t total = 0;
            for (uint32_t w = 0; w < W; w++) {
                if (ctxs[w].rc != 0 && prc == 0)
                    prc = ctxs[w].rc;
                offsets[w] = total;
                total += ctxs[w].count;
            }
            offsets[W] = total;
            if (prc == 0 && sess->join_output_limit > 0
                && total >= sess->join_output_limit)
                prc = EOVERFLOW;
            if (prc == 0 && total > UINT32_MAX)
                prc = ENOMEM;
            if (prc == 0 && total > out->capacity && ocols > 0) {
                uint64_t add_rows = total - out->capacity;
                uint64_t row_bytes = (uint64_t)ocols * sizeof(int64_t);
                uint64_t add_bytes = add_rows > UINT64_MAX / row_bytes
                    ? UINT64_MAX : add_rows * row_bytes;
                uint64_t budget = atomic_load_explicit(
                    &sess->mem_ledger.total_budget, memory_order_relaxed);
                uint64_t current = atomic_load_explicit(
                    &sess->mem_ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
                    memory_order_relaxed);
                uint64_t cap = (budget
                    * wl_mem_subsys_pct[WL_MEM_SUBSYS_RELATION]) / 100u;
                uint64_t threshold = (cap * 80u) / 100u;
                if (budget > 0 && cap > 0
                    && (add_bytes > UINT64_MAX - current
                    || current + add_bytes >= threshold))
                    prc = EOVERFLOW;
            }
            if (prc == 0) {
                if (col_join_reserve_exact(out, (uint32_t)total) != 0) {
                    prc = ENOMEM;
                } else {
                    out->nrows = (uint32_t)total;
                    for (uint32_t w = 0; w < W; w++) {
                        ctxs[w].out = out;
                        ctxs[w].out_begin = offsets[w];
                        ctxs[w].rc = 0;
                        if (wl_workqueue_submit(sess->wq,
                            col_join_keyed_fill_worker_fn, &ctxs[w]) != 0)
                            prc = ENOMEM;
                    }
                    wl_workqueue_wait_all(sess->wq);
                    for (uint32_t w = 0; w < W; w++)
                        if (ctxs[w].rc != 0 && prc == 0)
                            prc = ctxs[w].rc;
                }
            }
            for (uint32_t w = 0; w < W; w++)
                free(ctxs[w].pairs);
            free(offsets);
            free(ctxs);
            free(left_hashes);
            if (prc != 0) {
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return prc;
            }
            goto join_success;
        }

        /* Probe left against the persistent diff arrangement hash table */
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc) & (nbk - 1);
            for (uint32_t e = darr->ht_head[h]; e != 0;
                e = darr->ht_next[e - 1]) {
                uint32_t rr = e - 1;
                if (!col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                    continue;
                join_rc = col_join_append_pair(out, left, lr, right, rr,
                        op->project_indices, op->project_count, tmp);
                if (join_rc != 0)
                    break;
                if (col_join_output_limit_reached(sess, out)
                    || col_join_inloop_backpressure(sess, out)) {
                    fprintf(stderr,
                        "join output limit reached (diff): %u rows "
                        "(limit=%llu)\n",
                        out->nrows,
                        (unsigned long long)sess->join_output_limit);
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }

        if (join_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
    } else {
        /* Ephemeral hash table fallback (same as col_op_join) */
        uint32_t nbuckets_ep = next_pow2(right->nrows >
                0 ? right->nrows * 2 : 1);
        uint32_t *ht_head_ep = (uint32_t *)calloc(nbuckets_ep,
                sizeof(uint32_t));
        uint32_t *ht_next_ep = (uint32_t *)malloc(
            (right->nrows > 0 ? right->nrows : 1) * sizeof(uint32_t));
        if (!ht_head_ep || !ht_next_ep) {
            free(ht_head_ep);
            free(ht_next_ep);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        for (uint32_t rr = 0; rr < right->nrows; rr++) {
            uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
                & (nbuckets_ep - 1);
            ht_next_ep[rr] = ht_head_ep[h];
            ht_head_ep[h] = rr + 1;
        }
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
                & (nbuckets_ep - 1);
            for (uint32_t e = ht_head_ep[h]; e != 0;
                e = ht_next_ep[e - 1]) {
                uint32_t rr = e - 1;
                if (!col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                    continue;
                join_rc = col_join_append_pair(out, left, lr, right, rr,
                        op->project_indices, op->project_count, tmp);
                if (join_rc != 0)
                    break;
                if (col_join_output_limit_reached(sess, out)
                    || col_join_inloop_backpressure(sess, out)) {
                    fprintf(stderr,
                        "join output limit reached (diff): %u rows "
                        "(limit=%llu)\n",
                        out->nrows,
                        (unsigned long long)sess->join_output_limit);
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }
        free(ht_head_ep);
        free(ht_next_ep);
        if (join_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
    }

join_success:
    free(tmp);
    free(lk);
    free(rk);
    bool result_is_delta = projected_join ? false
        : (left_e.is_delta || used_right_delta);

    /* Materialization cache: insert BEFORE destroying left, because
     * col_mat_cache_key_content dereferences left to compute content hash. */
    if (op->materialized && !projected_join) {
        col_mat_cache_insert(&sess->mat_cache, left, right, out);
        if (left_e.owned)
            col_rel_destroy(left);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
    if (left_e.owned)
        col_rel_destroy(left);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push_delta(stack, out, true, result_is_delta);
}

/* --- DIFFERENTIAL CONSOLIDATE -------------------------------------------- */

/*
 * col_op_consolidate_diff - Differential consolidate with trace-based
 * incremental compaction (Issue #263).
 *
 * Key optimization over col_op_consolidate:
 *   - Uses sorted prefix tracking for incremental merge: O(D log D + N)
 *   - Creates trace checkpoint for frontier persistence across iterations
 *   - Preserves arrangement validity by using incremental merge path
 *
 * Algorithm:
 *   1. If sorted prefix exists [0..sorted_nrows): sort only suffix (delta)
 *   2. Dedup within delta
 *   3. Merge sorted prefix + sorted delta, emitting unique rows
 *   4. Record trace timestamp for convergence tracking
 *
 * Guard: activated when sess->diff_operators_active is true
 */
int
col_op_consolidate_diff(eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        in->sorted_nrows = nr;
        in->run_count = 1;
        in->run_ends[0] = nr;
        return eval_stack_push(stack, in, e.owned);
    }

    /* Own the relation for in-place sort */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_pool_new_like(sess->delta_pool, "$consol_diff", in);
        if (!work) {
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        if (col_rel_append_all(work, in, NULL) != 0) {
            col_rel_destroy(work);
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        work_owned = true;
    }

    /* K-way merge dispatch (same as col_op_consolidate) */
    uint32_t k = e.seg_count > 0 ? e.seg_count : 1;
    if (k >= 2 && e.seg_boundaries != NULL) {
        int rc = col_op_consolidate_kway_merge(work, e.seg_boundaries, k);
        free(e.seg_boundaries);
        if (rc != 0) {
            if (work_owned)
                col_rel_destroy(work);
            return rc;
        }
        work->sorted_nrows = work->nrows;
        work->run_count = 1;
        work->run_ends[0] = work->nrows;
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);

    /* Trace-based incremental compaction:
     * When a sorted prefix exists, use incremental merge (O(D log D + N))
     * instead of full sort (O(N log N)). Record trace for frontier tracking. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;

        /* Phase 1: sort only the unsorted suffix using radix sort */
        col_rel_radix_sort(work, sn, delta_count);

        /* Phase 1b: dedup within suffix */
        uint32_t d_unique = 1;
        for (uint32_t i = 1; i < delta_count; i++) {
            if (col_rel_row_cmp(work, sn + i - 1, sn + i) != 0) {
                col_rel_row_move(work, sn + d_unique, sn + i);
                d_unique++;
            }
        }

        /* Phase 2: merge sorted prefix with sorted suffix */
        uint32_t max_rows = sn + d_unique;

        /* Reuse persistent merge buffer when possible (column-major) */
        int64_t **merged_cols;
        bool used_merge_buf = false;
        if (work->merge_columns && work->merge_buf_cap >= max_rows) {
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        } else {
            uint32_t new_cap = max_rows > work->merge_buf_cap * 2
                                   ? max_rows
                                   : work->merge_buf_cap * 2;
            if (new_cap < max_rows)
                new_cap = max_rows;
            if (work->merge_columns) {
                if (col_columns_realloc(work->merge_columns, nc,
                    new_cap) != 0) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            } else {
                work->merge_columns = col_columns_alloc(nc, new_cap);
                if (!work->merge_columns) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            }
            work->merge_buf_cap = new_cap;
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        }

        uint32_t oi = 0, di = 0, out_idx = 0;
        while (oi < sn && di < d_unique) {
            int cmp = col_rel_row_cmp(work, oi, sn + di);
            if (cmp < 0) {
                col_columns_copy_row(merged_cols, out_idx,
                    work->columns, oi, nc);
                oi++;
            } else if (cmp == 0) {
                col_columns_copy_row(merged_cols, out_idx,
                    work->columns, oi, nc);
                oi++;
                di++;
            } else {
                col_columns_copy_row(merged_cols, out_idx,
                    work->columns, sn + di, nc);
                di++;
            }
            out_idx++;
        }
        while (oi < sn) {
            col_columns_copy_row(merged_cols, out_idx,
                work->columns, oi, nc);
            oi++;
            out_idx++;
        }
        while (di < d_unique) {
            col_columns_copy_row(merged_cols, out_idx,
                work->columns, sn + di, nc);
            di++;
            out_idx++;
        }

        /* Swap merge_columns and columns (issue #218) */
        if (used_merge_buf) {
            int64_t **old_cols = work->columns;
            uint32_t old_cap = work->capacity;
            work->columns = work->merge_columns;
            work->capacity = work->merge_buf_cap;
            work->merge_columns = old_cols;
            work->merge_buf_cap = old_cap;
        }
        work->nrows = out_idx;
        work->sorted_nrows = out_idx;
        work->run_count = 1;
        work->run_ends[0] = out_idx;

        /* Right-size columns after dedup (issue #218) */
        if (out_idx > 0 && work->capacity > out_idx + out_idx / 4) {
            uint32_t tight = out_idx + out_idx / 4;
            if (tight < COL_REL_INIT_CAP)
                tight = COL_REL_INIT_CAP;
            if (col_columns_realloc(work->columns, nc, tight) == 0)
                work->capacity = tight;
        }

        return eval_stack_push(stack, work, work_owned);
    }

    /* Fallback: radix sort + dedup */
    col_rel_radix_sort_int64(work);

    uint32_t out_r = 1;
    for (uint32_t r = 1; r < nr; r++) {
        if (col_rel_row_cmp(work, r - 1, r) != 0) {
            col_rel_row_move(work, out_r, r);
            out_r++;
        }
    }
    work->nrows = out_r;
    work->sorted_nrows = out_r;
    work->run_count = 1;
    work->run_ends[0] = out_r;

    return eval_stack_push(stack, work, work_owned);
}

/* ======================================================================== */
/* Exchange Operator (Issue #316)                                           */
/* ======================================================================== */

/*
 * col_op_exchange:
 * Redistribute tuples by hash(key_columns) % W across workers.
 *
 * Single-worker (W=1): no-op, leave stack unchanged.
 *
 * Multi-worker: pops input from eval stack, partitions it into W
 * sub-relations stored in coord->exchange_bufs[my_worker_id][0..W-1].
 * Does NOT push a result -- the coordinator gathers exchange_bufs[*][w]
 * for each worker w after the barrier.
 *
 * Precondition: coord->exchange_bufs must be allocated by the caller
 * (coordinator) before submitting workers to the workqueue.
 */
int
col_op_exchange(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op->opaque_data)
        return EINVAL;

    const wl_plan_op_exchange_t *meta
        = (const wl_plan_op_exchange_t *)op->opaque_data;

    /* Single-worker no-op: leave stack unchanged */
    if (meta->num_workers <= 1)
        return 0;

    /* Pop input from eval stack */
    if (stack->top == 0)
        return EINVAL;
    eval_entry_t input_entry = eval_stack_pop(stack);
    col_rel_t *input = input_entry.rel;

    /* NULL or empty input is a no-op for exchange */
    if (!input || input->ncols == 0) {
        if (input_entry.owned && input)
            col_rel_destroy(input);
        return 0;
    }

    /* Validate key column indices against input schema */
    if (input->ncols > 0) {
        for (uint32_t i = 0; i < meta->key_col_count; i++) {
            if (meta->key_col_idxs[i] >= input->ncols) {
                if (input_entry.owned)
                    col_rel_destroy(input);
                return EINVAL;
            }
        }
    }

    /* Locate coordinator and determine this worker's id */
    wl_col_session_t *coord = sess->coordinator ? sess->coordinator : sess;
    uint32_t my_id = sess->coordinator ? sess->worker_id : 0;

    if (!coord->exchange_bufs || my_id >= coord->exchange_num_workers) {
        if (input_entry.owned)
            col_rel_destroy(input);
        return EINVAL;
    }

    /* Scatter: partition input into exchange_bufs[my_id][0..W-1] */
    int rc = col_rel_exchange_partition(input, meta->key_col_idxs,
            meta->key_col_count, meta->num_workers,
            coord->exchange_bufs[my_id]);

    if (input_entry.owned)
        col_rel_destroy(input);

    return rc;
}
