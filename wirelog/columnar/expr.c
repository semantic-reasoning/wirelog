/*
 * columnar/expr.c - Compiled columnar expression evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/internal.h"
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
#include <math.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int64_t vals[COL_FILTER_STACK];
    uint32_t top;
} expr_stack_t;

static inline void
expr_push(expr_stack_t *s, int64_t v)
{
    if (s->top < COL_FILTER_STACK)
        s->vals[s->top++] = v;
}
static inline int64_t
expr_pop(expr_stack_t *s)
{
    return s->top != 0 ? s->vals[--s->top] : 0;
}

static bool
expr_float_load(int64_t lane, double *out)
{
    memcpy(out, &lane, sizeof(*out));
    return isfinite(*out);
}

static bool
expr_float_store(double value, int64_t *out)
{
    if (!isfinite(value))
        return false;
    if (value == 0.0)
        value = 0.0;
    memcpy(out, &value, sizeof(*out));
    return true;
}

#ifdef WL_MBEDTLS_ENABLED
static int
wl_columnar_expr_psa_init(void)
{
    return psa_crypto_init() == PSA_SUCCESS ? 0 : -1;
}

static int
wl_columnar_expr_psa_hash_bytes(psa_algorithm_t alg, const void *data,
    size_t len, unsigned char *digest, size_t digest_len)
{
    size_t actual_len = 0;
    if (wl_columnar_expr_psa_init() != 0)
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
wl_columnar_expr_psa_hash_frame(psa_hash_operation_t *op, uint8_t tag,
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
wl_columnar_expr_psa_hash_pair(psa_algorithm_t alg, bool framed,
    uint8_t first_tag, const void *first, size_t first_len,
    uint8_t second_tag, const void *second, size_t second_len,
    unsigned char *digest, size_t digest_len)
{
    psa_hash_operation_t op = PSA_HASH_OPERATION_INIT;
    size_t actual_len = 0;
    int ret = -1;

    if (wl_columnar_expr_psa_init() != 0)
        return -1;
    if (psa_hash_setup(&op, alg) != PSA_SUCCESS)
        goto out;
    if (framed
        && wl_columnar_expr_psa_hash_frame(&op, first_tag, first_len) != 0)
        goto out;
    if (psa_hash_update(&op, (const unsigned char *)first, first_len)
        != PSA_SUCCESS)
        goto out;
    if (framed
        && wl_columnar_expr_psa_hash_frame(&op, second_tag, second_len) != 0)
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
wl_columnar_expr_parse_uuid(const char *text, unsigned char uuid[16])
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
wl_columnar_expr_format_uuid(const unsigned char uuid[16], wl_intern_t *intern)
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
wl_columnar_expr_psa_hmac_sha256(const void *msg, size_t msg_len,
    const void *key, size_t key_len, unsigned char *digest, size_t digest_len)
{
    psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
    psa_key_id_t key_id = 0;
    size_t actual_len = 0;
    psa_status_t status;
    int ret = -1;

    if (wl_columnar_expr_psa_init() != 0)
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

/*
 * wl_columnar_expr_eval_run:
 * Core postfix expression evaluator. Runs the bytecode against a row and
 * stores the top-of-stack value in *out_val.
 * Returns 0 on success, non-zero on malformed bytecode.
 */
int
wl_columnar_expr_eval_run(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, int64_t *out_val, wl_intern_t *intern)
{
    expr_stack_t s;
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
            expr_push(&s, (col >= 0 && (uint32_t)col < ncols) ? row[col] : 0);
            break;
        }

        case WL_PLAN_EXPR_VAR_FLOAT: {
            if (i + 2 > size)
                goto bad;
            uint16_t nlen;
            memcpy(&nlen, buf + i, 2);
            i += 2;
            if (i + nlen > size)
                goto bad;
            long col = 0;
            if (nlen > 3 && buf[i] == 'c' && buf[i + 1] == 'o'
                && buf[i + 2] == 'l') {
                char tmp[16] = { 0 };
                uint32_t cplen = (nlen - 3 < 15) ? nlen - 3 : 15;
                memcpy(tmp, buf + i + 3, cplen);
                col = strtol(tmp, NULL, 10);
            }
            i += nlen;
            double value;
            if (col < 0 || (uint32_t)col >= ncols
                || !expr_float_load(row[col], &value))
                goto bad;
            expr_push(&s, row[col]);
            break;
        }

        case WL_PLAN_EXPR_CONST_INT: {
            if (i + 8 > size)
                goto bad;
            int64_t v;
            memcpy(&v, buf + i, 8);
            i += 8;
            expr_push(&s, v);
            break;
        }

        case WL_PLAN_EXPR_CONST_FLOAT: {
            if (i + 8 > size)
                goto bad;
            int64_t bits;
            memcpy(&bits, buf + i, 8);
            i += 8;
            double value;
            if (!expr_float_load(bits, &value))
                goto bad;
            expr_push(&s, bits);
            break;
        }

        case WL_PLAN_EXPR_BOOL: {
            if (i + 1 > size)
                goto bad;
            expr_push(&s, buf[i++] ? 1 : 0);
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
                expr_push(&s, id);
            } else {
                expr_push(&s, 0);
            }
            i += slen;
            break;
        }

        /* Arithmetic */
        case WL_PLAN_EXPR_ARITH_ADD: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_add_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_sub_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_mul_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_div_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_mod_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
        case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
        case WL_PLAN_EXPR_ARITH_FLOAT_DIV: {
            int64_t bbits = expr_pop(&s), abits = expr_pop(&s), result;
            double a, b, value;
            if (!expr_float_load(abits, &a) || !expr_float_load(bbits, &b))
                goto bad;
            switch ((wl_plan_expr_tag_t)tag) {
            case WL_PLAN_EXPR_ARITH_FLOAT_ADD: value = a + b; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_SUB: value = a - b; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_MUL: value = a * b; break;
            default: value = a / b; break;
            }
            if (!expr_float_store(value, &result))
                goto bad;
            expr_push(&s, result);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BAND: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a & b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BOR: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a | b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BXOR: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a ^ b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BNOT: {
            int64_t a = expr_pop(&s);
            expr_push(&s, ~a);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHL: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_shl_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_shr_int64(a, b, &v) != 0)
                goto bad;
            expr_push(&s, v);
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
            int64_t a = expr_pop(&s);
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_HASH_S, intern);
            expr_push(&s, (int64_t)XXH3_64bits(m.ptr, m.len));
            break;
        }

        case WL_PLAN_EXPR_ARITH_CRC32_ETH:
        case WL_PLAN_EXPR_ARITH_CRC32_ETH_S: {
            int64_t a = expr_pop(&s);
            filt_bytes_t m;
            filt_digest_bytes(&m, a,
                tag == WL_PLAN_EXPR_ARITH_CRC32_ETH_S, intern);
            expr_push(&s, (int64_t)ethernet_crc32(m.ptr, m.len));
            break;
        }

        case WL_PLAN_EXPR_ARITH_CRC32_CAST:
        case WL_PLAN_EXPR_ARITH_CRC32_CAST_S: {
            int64_t a = expr_pop(&s);
            filt_bytes_t m;
            filt_digest_bytes(&m, a,
                tag == WL_PLAN_EXPR_ARITH_CRC32_CAST_S, intern);
            expr_push(&s, (int64_t)castagnoli_crc32(m.ptr, m.len));
            break;
        }

        case WL_PLAN_EXPR_ARITH_MD5:
        case WL_PLAN_EXPR_ARITH_MD5_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = expr_pop(&s);
            unsigned char digest[16];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_MD5_S, intern);
            if (wl_columnar_expr_psa_hash_bytes(PSA_ALG_MD5, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            expr_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)expr_pop(&s);
            goto bad; /* md5 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA1:
        case WL_PLAN_EXPR_ARITH_SHA1_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = expr_pop(&s);
            unsigned char digest[20];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_SHA1_S, intern);
            if (wl_columnar_expr_psa_hash_bytes(PSA_ALG_SHA_1, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            expr_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)expr_pop(&s);
            goto bad; /* sha1 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA256:
        case WL_PLAN_EXPR_ARITH_SHA256_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = expr_pop(&s);
            unsigned char digest[32];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_SHA256_S,
                intern);
            if (wl_columnar_expr_psa_hash_bytes(PSA_ALG_SHA_256, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            expr_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)expr_pop(&s);
            goto bad; /* sha256 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA512:
        case WL_PLAN_EXPR_ARITH_SHA512_S: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = expr_pop(&s);
            unsigned char digest[64];
            filt_bytes_t m;
            filt_digest_bytes(&m, a, tag == WL_PLAN_EXPR_ARITH_SHA512_S,
                intern);
            if (wl_columnar_expr_psa_hash_bytes(PSA_ALG_SHA_512, m.ptr, m.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            expr_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)expr_pop(&s);
            goto bad; /* sha512 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_HMAC_SHA256:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t key_val = expr_pop(&s);
            int64_t msg_val = expr_pop(&s);
            unsigned char digest[32];
            bool msg_sym = (tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS
                || tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI);
            bool key_sym = (tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS
                || tag == WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS);
            filt_bytes_t m, k;
            filt_digest_bytes(&m, msg_val, msg_sym, intern);
            filt_digest_bytes(&k, key_val, key_sym, intern);
            if (wl_columnar_expr_psa_hmac_sha256(m.ptr, m.len, k.ptr, k.len,
                digest, sizeof(digest)) != 0)
                goto bad;
            expr_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)expr_pop(&s);
            (void)expr_pop(&s);
            goto bad; /* hmac_sha256 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_UUID4: {
#ifdef WL_MBEDTLS_ENABLED
            unsigned char uuid[16];
            if (wl_columnar_expr_psa_init() != 0)
                goto bad;
            if (psa_generate_random(uuid, sizeof(uuid)) != PSA_SUCCESS)
                goto bad;
            /* RFC 4122 v4: set version=4, variant=0b10 */
            uuid[6] = (uuid[6] & 0x0F) | 0x40;
            uuid[8] = (uuid[8] & 0x3F) | 0x80;
            /* Return upper 64 bits as int64_t */
            int64_t result;
            memcpy(&result, uuid, sizeof(result));
            expr_push(&s, result);
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
            int64_t name = expr_pop(&s);
            int64_t ns = expr_pop(&s);
            unsigned char digest[20]; /* SHA-1 output */
            bool ns_sym = (tag == WL_PLAN_EXPR_ARITH_UUID5_SS
                || tag == WL_PLAN_EXPR_ARITH_UUID5_SI);
            bool name_sym = (tag == WL_PLAN_EXPR_ARITH_UUID5_SS
                || tag == WL_PLAN_EXPR_ARITH_UUID5_IS);
            filt_bytes_t nsb, nmb;
            filt_digest_bytes(&nsb, ns, ns_sym, intern);
            filt_digest_bytes(&nmb, name, name_sym, intern);
            if (wl_columnar_expr_psa_hash_pair(PSA_ALG_SHA_1,
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
            expr_push(&s, result);
#else
            (void)expr_pop(&s);
            (void)expr_pop(&s);
            goto bad; /* uuid5 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        /* Comparisons */
        case WL_PLAN_EXPR_CMP_EQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a == b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_NEQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a != b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a < b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a > b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a <= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a >= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_FLOAT_EQ:
        case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
        case WL_PLAN_EXPR_CMP_FLOAT_LT:
        case WL_PLAN_EXPR_CMP_FLOAT_GT:
        case WL_PLAN_EXPR_CMP_FLOAT_LTE:
        case WL_PLAN_EXPR_CMP_FLOAT_GTE: {
            int64_t bbits = expr_pop(&s), abits = expr_pop(&s);
            double a, b;
            if (!expr_float_load(abits, &a) || !expr_float_load(bbits, &b))
                goto bad;
            bool result;
            switch ((wl_plan_expr_tag_t)tag) {
            case WL_PLAN_EXPR_CMP_FLOAT_EQ: result = a == b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_NEQ: result = a != b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_LT: result = a < b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_GT: result = a > b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_LTE: result = a <= b; break;
            default: result = a >= b; break;
            }
            expr_push(&s, result ? 1 : 0);
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
            int64_t a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_strlen(a, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_CAT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_cat(a, b, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_SUBSTR: {
            int64_t c = expr_pop(&s), b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_substr(a, b, c, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_CONTAINS: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s,
                intern ? (string_ops_contains(a, b, intern) ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_PREFIX: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s,
                intern ? (string_ops_str_prefix(a, b, intern) ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_SUFFIX: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s,
                intern ? (string_ops_str_suffix(a, b, intern) ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_ORD: {
            int64_t a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_str_ord(a, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_UPPER: {
            int64_t a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_to_upper(a, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_LOWER: {
            int64_t a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_to_lower(a, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_REPLACE: {
            int64_t c = expr_pop(&s), b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_str_replace(a, b, c, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TRIM: {
            int64_t a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_trim(a, intern) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_STRING: {
            int64_t a = expr_pop(&s);
            expr_push(&s, intern ? string_ops_to_string(a, intern) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_NUMBER: {
            int64_t a = expr_pop(&s);
            int64_t v;
            if (!intern || wl_string_ops_to_number_checked(a, intern, &v) != 0)
                goto bad;
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_UUID5_RFC: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t name_id = expr_pop(&s);
            int64_t namespace_id = expr_pop(&s);
            const char *namespace_text = intern
                ? wl_intern_reverse(intern, namespace_id) : NULL;
            const char *name = intern
                ? wl_intern_reverse(intern, name_id) : NULL;
            unsigned char namespace_uuid[16];
            unsigned char digest[20];

            /* RFC 4122 requires the namespace operand to be a canonical
             * UUID string, but the name is an arbitrary string. */
            if (!intern || !name
                || wl_columnar_expr_parse_uuid(namespace_text,
                namespace_uuid) != 0
                || wl_columnar_expr_psa_hash_pair(PSA_ALG_SHA_1, false,
                0, namespace_uuid, sizeof(namespace_uuid), 0,
                name, strlen(name), digest, sizeof(digest)) != 0)
                goto bad;
            /* RFC 4122 v5: version 5 and the RFC variant. */
            digest[6] = (digest[6] & 0x0F) | 0x50;
            digest[8] = (digest[8] & 0x3F) | 0x80;
            int64_t result = wl_columnar_expr_format_uuid(digest, intern);
            if (result < 0)
                goto bad;
            expr_push(&s, result);
#else
            (void)expr_pop(&s);
            (void)expr_pop(&s);
            goto bad; /* uuid5_rfc requires mbedTLS */
#endif
            break;
        }

        /* String comparisons: intern IDs → strcmp-based bool */
        case WL_PLAN_EXPR_CMP_STR_EQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s,
                (sa && sb) ? (strcmp(sa, sb) == 0 ? 1 : 0) : (a == b ? 1 : 0));
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_NEQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s,
                (sa && sb) ? (strcmp(sa, sb) != 0 ? 1 : 0) : (a != b ? 1 : 0));
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_LT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) < 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_GT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) > 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_LTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) <= 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_GTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) >= 0 ? 1 : 0) : 0);
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
 * wl_columnar_expr_filter_row:
 * Evaluate the postfix expression buffer against a single row.
 * Variable names are assumed to be "col<N>" (rewritten by plan compiler).
 * Returns non-zero if the row passes the predicate, 0 if filtered out.
 */
int
wl_columnar_expr_filter_row(const uint8_t *buf, uint32_t size,
    const int64_t *row,
    uint32_t ncols, wl_intern_t *intern)
{
    int64_t val;
    int err = wl_columnar_expr_eval_run(buf, size, row, ncols, &val, intern);
    return err ? 0 : (val != 0 ? 1 : 0);
}

/*
 * wl_columnar_expr_eval_i64:
 * Evaluate the postfix expression buffer and write the computed value to out_val.
 * Used by MAP operations to compute head argument expressions.
 * Returns 0 on success, non-zero on empty expression or evaluation error.
 */
int
wl_columnar_expr_eval_i64(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, int64_t *out_val, wl_intern_t *intern)
{
    return wl_columnar_expr_eval_run(buf, size, row, ncols, out_val, intern);
}

/*
 * wl_columnar_expr_parse_var_col:
 * Parse a WL_PLAN_EXPR_VAR token at buf[i] (not yet consumed; i points at
 * the opcode byte itself).  If successful, advance *pos past the full token
 * and store the extracted column index in *col_out.
 * Returns true on success, false on malformed bytecode.
 *
 * VAR encoding: [0x01][name_len:u16 LE][name:u8*name_len]
 * where name is "colN" (N is the decimal column index).
 */
bool
wl_columnar_expr_parse_var_col(const uint8_t *buf, uint32_t size, uint32_t *pos,
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

/* ======================================================================== */
/* Pre-compiled expression evaluator                                        */
/* ======================================================================== */

/*
 * expr_instr_t: single decoded instruction.
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
} expr_instr_t;

struct wl_columnar_expr_compiled {
    expr_instr_t *instrs;
    uint32_t ninstr;
    /* Issue #966: fixed for the whole expression, so it is resolved once here
     * rather than passed to the per-row evaluator.  Borrowed -- wl_plan_t
     * documents its intern as outliving the plan, which outlives any compiled
     * expression.  NULL is legal and matches the interpreter's own NULL-intern
     * fallbacks. */
    const wl_intern_t *intern;
};

void
wl_columnar_expr_compiled_free(wl_columnar_expr_compiled_t *c)
{
    if (c) {
        free(c->instrs);
        free(c);
    }
}

/*
 * wl_columnar_expr_compile:
 * Walk the bytecode buffer once and produce a pre-compiled instruction array.
 * Variable names ("colN") are resolved to column indices here so that the
 * per-row evaluator (wl_columnar_expr_eval_compiled) never calls strtol.
 *
 * Returns NULL if the expression contains unsupported opcodes (CONST_STR,
 * hash/crypto functions) or if allocation fails.  Callers fall back to
 * col_eval_expr_run in that case.
 */
wl_columnar_expr_compiled_t *
wl_columnar_expr_compile(const uint8_t *buf, uint32_t size,
    const wl_intern_t *intern)
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
            if (!wl_columnar_expr_parse_var_col(buf, size, &pos, &col))
                return NULL;
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_VAR_FLOAT: {
            uint32_t pos = i;
            uint32_t col = 0;
            if (!wl_columnar_expr_parse_var_col(buf, size, &pos, &col))
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
        case WL_PLAN_EXPR_CONST_FLOAT:
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
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
        case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
        case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
        /* Comparison operators (no payload) */
        case WL_PLAN_EXPR_CMP_EQ:
        case WL_PLAN_EXPR_CMP_NEQ:
        case WL_PLAN_EXPR_CMP_LT:
        case WL_PLAN_EXPR_CMP_GT:
        case WL_PLAN_EXPR_CMP_LTE:
        case WL_PLAN_EXPR_CMP_GTE:
        case WL_PLAN_EXPR_CMP_FLOAT_EQ:
        case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
        case WL_PLAN_EXPR_CMP_FLOAT_LT:
        case WL_PLAN_EXPR_CMP_FLOAT_GT:
        case WL_PLAN_EXPR_CMP_FLOAT_LTE:
        case WL_PLAN_EXPR_CMP_FLOAT_GTE:
        /* Issue #966: string-ordering comparisons (no payload).  #962 made
         * these correct by emitting them; they were absent here, so any
         * predicate containing one fell to `default: return NULL` and the
         * whole expression was demoted to the interpreter.  Measured at
         * ~66 ns/cmp of the ~77 ns regression -- the demotion, not the
         * strcmp. */
        case WL_PLAN_EXPR_CMP_STR_EQ:
        case WL_PLAN_EXPR_CMP_STR_NEQ:
        case WL_PLAN_EXPR_CMP_STR_LT:
        case WL_PLAN_EXPR_CMP_STR_GT:
        case WL_PLAN_EXPR_CMP_STR_LTE:
        case WL_PLAN_EXPR_CMP_STR_GTE:
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

    wl_columnar_expr_compiled_t *c =
        (wl_columnar_expr_compiled_t *)malloc(
        sizeof(wl_columnar_expr_compiled_t));
    if (!c)
        return NULL;
    c->instrs =
        (expr_instr_t *)malloc(ninstr * sizeof(expr_instr_t));
    if (!c->instrs) {
        free(c);
        return NULL;
    }
    c->ninstr = ninstr;
    c->intern = intern;

    /* Pass 2: fill instruction array. */
    uint32_t j = 0;
    i = 0;
    while (i < size && j < ninstr) {
        expr_instr_t *instr = &c->instrs[j++];
        instr->op = buf[i];
        instr->iarg = 0;
        instr->larg = 0;
        switch ((wl_plan_expr_tag_t)buf[i]) {
        case WL_PLAN_EXPR_VAR: {
            uint32_t pos = i;
            wl_columnar_expr_parse_var_col(buf, size, &pos, &instr->iarg);
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_VAR_FLOAT: {
            uint32_t pos = i;
            wl_columnar_expr_parse_var_col(buf, size, &pos, &instr->iarg);
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_CONST_INT:
            i++; /* skip opcode */
            memcpy(&instr->larg, buf + i, 8);
            i += 8;
            break;
        case WL_PLAN_EXPR_CONST_FLOAT:
            i++;
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
 * wl_columnar_expr_eval_compiled:
 * Fast postfix evaluator using a pre-compiled instruction array.
 * VAR instructions use pre-parsed column indices — no strtol per row.
 * Returns 0 on success with result in *out_val, non-zero on error.
 */
/*
 * Issue #966: string comparison on the compiled path.
 *
 * The six CMP_STR arms below are transcribed from the interpreter's block in
 * col_eval_expr rather than paraphrased, because their fallbacks are NOT
 * symmetric and the asymmetry is load-bearing: when either reverse lookup
 * fails, EQ/NEQ fall back to comparing the raw ids, while LT/GT/LTE/GTE
 * yield false.  Collapsing the two families into one shape would be a silent
 * wrong-answer change, and #962's sweep found no program in this tree that
 * compares symbols -- nothing existing would catch it.
 */
int
wl_columnar_expr_eval_compiled(const wl_columnar_expr_compiled_t *c,
    const int64_t *row,
    uint32_t ncols, int64_t *out_val)
{
    const wl_intern_t *intern = c->intern;
    expr_stack_t s;
    s.top = 0;
    for (uint32_t k = 0; k < c->ninstr; k++) {
        const expr_instr_t *in = &c->instrs[k];
        switch ((wl_plan_expr_tag_t)in->op) {
        case WL_PLAN_EXPR_VAR:
            expr_push(&s, (in->iarg < ncols) ? row[in->iarg] : 0);
            break;
        case WL_PLAN_EXPR_VAR_FLOAT: {
            int64_t value = (in->iarg < ncols) ? row[in->iarg] : 0;
            double decoded;
            if (in->iarg >= ncols || !expr_float_load(value, &decoded)) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, value);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_EQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s,
                (sa && sb) ? (strcmp(sa, sb) == 0 ? 1 : 0) : (a == b ? 1 : 0));
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_NEQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s,
                (sa && sb) ? (strcmp(sa, sb) != 0 ? 1 : 0) : (a != b ? 1 : 0));
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_LT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) < 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_GT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) > 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_LTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) <= 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_STR_GTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
            const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
            expr_push(&s, (sa && sb) ? (strcmp(sa, sb) >= 0 ? 1 : 0) : 0);
            break;
        }
        case WL_PLAN_EXPR_CONST_INT:
        case WL_PLAN_EXPR_BOOL:
            expr_push(&s, in->larg);
            break;
        case WL_PLAN_EXPR_CONST_FLOAT: {
            double decoded;
            if (!expr_float_load(in->larg, &decoded)) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, in->larg);
            break;
        }
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
        case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
        case WL_PLAN_EXPR_ARITH_FLOAT_DIV: {
            int64_t bbits = expr_pop(&s), abits = expr_pop(&s), result;
            double a, b, value;
            if (!expr_float_load(abits, &a) || !expr_float_load(bbits, &b)) {
                *out_val = 0;
                return 1;
            }
            switch ((wl_plan_expr_tag_t)in->op) {
            case WL_PLAN_EXPR_ARITH_FLOAT_ADD: value = a + b; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_SUB: value = a - b; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_MUL: value = a * b; break;
            default: value = a / b; break;
            }
            if (!expr_float_store(value, &result)) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, result);
            break;
        }
        case WL_PLAN_EXPR_ARITH_ADD: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_add_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_sub_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_mul_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_div_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_mod_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BAND: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a & b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BOR: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a | b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BXOR: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a ^ b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_BNOT: {
            int64_t a = expr_pop(&s);
            expr_push(&s, ~a);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHL: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_shl_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            int64_t v;
            if (wl_columnar_arithmetic_checked_shr_int64(a, b, &v) != 0) {
                *out_val = 0;
                return 1;
            }
            expr_push(&s, v);
            break;
        }
        case WL_PLAN_EXPR_CMP_EQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a == b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_NEQ: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a != b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a < b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GT: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a > b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_LTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a <= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_GTE: {
            int64_t b = expr_pop(&s), a = expr_pop(&s);
            expr_push(&s, a >= b ? 1 : 0);
            break;
        }
        case WL_PLAN_EXPR_CMP_FLOAT_EQ:
        case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
        case WL_PLAN_EXPR_CMP_FLOAT_LT:
        case WL_PLAN_EXPR_CMP_FLOAT_GT:
        case WL_PLAN_EXPR_CMP_FLOAT_LTE:
        case WL_PLAN_EXPR_CMP_FLOAT_GTE: {
            int64_t bbits = expr_pop(&s), abits = expr_pop(&s);
            double a, b;
            if (!expr_float_load(abits, &a) || !expr_float_load(bbits, &b)) {
                *out_val = 0;
                return 1;
            }
            bool result;
            switch ((wl_plan_expr_tag_t)in->op) {
            case WL_PLAN_EXPR_CMP_FLOAT_EQ: result = a == b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_NEQ: result = a != b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_LT: result = a < b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_GT: result = a > b; break;
            case WL_PLAN_EXPR_CMP_FLOAT_LTE: result = a <= b; break;
            default: result = a >= b; break;
            }
            expr_push(&s, result ? 1 : 0);
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
