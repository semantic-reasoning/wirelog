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
#include <ctype.h>
#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define WL_CALLBACK_SESSION_STACK_MAX 64

#if defined(_MSC_VER)
static __declspec(thread) const void *wl_callback_session_stack[
    WL_CALLBACK_SESSION_STACK_MAX];
static __declspec(thread) uint32_t wl_callback_session_depth;
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
static _Thread_local const void *wl_callback_session_stack[
    WL_CALLBACK_SESSION_STACK_MAX];
static _Thread_local uint32_t wl_callback_session_depth;
#else
static __thread const void *wl_callback_session_stack[
    WL_CALLBACK_SESSION_STACK_MAX];
static __thread uint32_t wl_callback_session_depth;
#endif

typedef struct {
    int64_t vals[COL_FILTER_STACK];
    uint32_t types[COL_FILTER_STACK];
    const uint8_t *string_data[COL_FILTER_STACK];
    uint32_t string_lengths[COL_FILTER_STACK];
    uint32_t top;
} expr_stack_t;

static inline void
expr_push(expr_stack_t *s, int64_t v)
{
    if (s->top < COL_FILTER_STACK) {
        s->vals[s->top++] = v;
        s->types[s->top - 1] = WIRELOG_EXTENSION_VALUE_INT64;
        s->string_data[s->top - 1] = NULL;
        s->string_lengths[s->top - 1] = 0;
    }
}
static inline void
expr_push_typed(expr_stack_t *s, int64_t v, uint32_t type)
{
    if (s->top < COL_FILTER_STACK) {
        s->vals[s->top] = v;
        s->types[s->top++] = type;
        s->string_data[s->top - 1] = NULL;
        s->string_lengths[s->top - 1] = 0;
    }
}

static inline void
expr_push_string(expr_stack_t *s, int64_t value, const uint8_t *data,
    uint32_t length)
{
    if (s->top < COL_FILTER_STACK) {
        s->vals[s->top] = value;
        s->types[s->top] = WIRELOG_EXTENSION_VALUE_STRING;
        s->string_data[s->top] = data;
        s->string_lengths[s->top++] = length;
    }
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
wl_columnar_expr_eval_run_ctx(const uint8_t *buf, uint32_t size,
    const int64_t *row, uint32_t ncols, int64_t *out_val,
    const wl_columnar_expr_context_t *ctx,
    wl_columnar_expr_status_t *status)
{
    expr_stack_t s;
    s.top = 0;
    wl_intern_t *intern = ctx ? ctx->intern : NULL;
    if (status)
        *status = WL_COLUMNAR_EXPR_OK;

    uint32_t i = 0;
    while (i < size) {
        uint8_t tag = buf[i++];
        switch ((wl_plan_expr_tag_t)tag) {

        case WL_PLAN_EXPR_VAR: {
            if (i > size || size - i < 2)
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

        case WL_PLAN_EXPR_VAR_STRING: {
            if (i > size || size - i < 2)
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
            if (!intern || col < 0 || (uint32_t)col >= ncols)
                goto bad;
            int64_t id = row[col];
            const char *value = wl_intern_reverse(intern, id);
            if (!value)
                goto bad;
            expr_push_string(&s, id, (const uint8_t *)value,
                (uint32_t)strlen(value));
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
            expr_push_typed(&s, buf[i++] ? 1 : 0,
                WIRELOG_EXTENSION_VALUE_BOOL);
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
            int64_t value = 0;
            if (intern) {
                char *tmp = (char *)malloc((size_t)slen + 1);
                if (!tmp) {
                    if (status)
                        *status = WL_COLUMNAR_EXPR_ALLOCATION_FAILURE;
                    goto bad;
                }
                memcpy(tmp, buf + i, slen);
                tmp[slen] = '\0';
                value = wl_intern_put(intern, tmp);
                free(tmp);
                /* Literals are pre-interned by the planner, so a failed
                 * put here is a refused growth (Issue #1470). */
                if (value < 0)
                    goto denied;
            }
            expr_push_string(&s, value, buf + i, slen);
            i += slen;
            break;
        }

        case WL_PLAN_EXPR_EXTENSION_CALL:
        case WL_PLAN_EXPR_EXTENSION_CALL_ABI: {
            uint16_t name_len;
            uint32_t nargs;
            uint64_t addon_abi_identity = 0;
            uint32_t addon_abi_version = 0;
            char name[256];
            const wirelog_extension_descriptor_t *descriptor;
            wirelog_extension_value_t args[COL_FILTER_STACK];
            wirelog_extension_value_t result = { 0 };

            if (i > size || size - i < 2) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_EXTENSION_MALFORMED;
                goto bad;
            }
            name_len = (uint16_t)buf[i] | (uint16_t)buf[i + 1] << 8;
            i += 2;
            if (name_len == 0 || name_len >= sizeof(name)
                || i > size || size - i < name_len
                || size - i - name_len < 4) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_EXTENSION_MALFORMED;
                goto bad;
            }
            memcpy(name, buf + i, name_len);
            name[name_len] = '\0';
            i += name_len;
            nargs = (uint32_t)buf[i] | (uint32_t)buf[i + 1] << 8
                | (uint32_t)buf[i + 2] << 16 | (uint32_t)buf[i + 3] << 24;
            i += 4;
            if ((wl_plan_expr_tag_t)tag == WL_PLAN_EXPR_EXTENSION_CALL_ABI) {
                if (size - i < 12) {
                    if (status)
                        *status = WL_COLUMNAR_EXPR_EXTENSION_MALFORMED;
                    goto bad;
                }
                for (uint32_t shift = 0; shift < 8; shift++)
                    addon_abi_identity |=
                        (uint64_t)buf[i + shift] << (shift * 8);
                i += 8;
                addon_abi_version = (uint32_t)buf[i]
                    | (uint32_t)buf[i + 1] << 8
                    | (uint32_t)buf[i + 2] << 16
                    | (uint32_t)buf[i + 3] << 24;
                i += 4;
            }
            if (!ctx || !ctx->extensions) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
                goto bad;
            }
            descriptor = wirelog_extension_snapshot_find(ctx->extensions,
                    name);
            if (!descriptor) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
                goto bad;
            }
            if ((wl_plan_expr_tag_t)tag == WL_PLAN_EXPR_EXTENSION_CALL_ABI
                && (addon_abi_identity != descriptor->addon_abi_identity
                || addon_abi_version != descriptor->addon_abi_version)) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH;
                goto bad;
            }
            if (ctx->parallel_execution && ctx->active_worker_count > 1
                && !(descriptor->callback_policy
                & WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE)) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_CALLBACK_POLICY;
                goto bad;
            }
            if (ctx->session_key
                && !(descriptor->callback_policy
                & WIRELOG_EXTENSION_CALLBACK_REENTRANT)) {
                for (uint32_t depth = 0;
                    depth < wl_callback_session_depth; depth++) {
                    if (wl_callback_session_stack[depth]
                        == ctx->session_key) {
                        if (status)
                            *status = WL_COLUMNAR_EXPR_CALLBACK_REENTRANT;
                        goto bad;
                    }
                }
            }
            if (nargs != descriptor->arity || nargs > s.top
                || nargs > COL_FILTER_STACK) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_ARITY_MISMATCH;
                goto bad;
            }
            if (descriptor->result_type != WIRELOG_EXTENSION_VALUE_BOOL
                && (!ctx->allow_extension_scalar_result
                || descriptor->result_type
                != WIRELOG_EXTENSION_VALUE_INT64)) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_INVALID_RESULT;
                goto bad;
            }
            for (uint32_t arg = 0; arg < nargs; arg++) {
                uint32_t index = s.top - nargs + arg;
                uint32_t expected = descriptor->argument_types
                    ? descriptor->argument_types[arg] : 0;
                uint32_t actual = s.types[index];
                if (expected != 0 && actual != expected) {
                    if (status)
                        *status = WL_COLUMNAR_EXPR_TYPE_MISMATCH;
                    goto bad;
                }
                args[arg].type = actual;
                args[arg].size = actual == WIRELOG_EXTENSION_VALUE_BOOL
                    ? sizeof(uint8_t)
                    : actual == WIRELOG_EXTENSION_VALUE_STRING
                    ? s.string_lengths[index] : sizeof(int64_t);
                if (actual == WIRELOG_EXTENSION_VALUE_BOOL)
                    args[arg].as.bool_value = s.vals[index] != 0;
                else if (actual == WIRELOG_EXTENSION_VALUE_STRING) {
                    args[arg].as.string_value.data =
                        (const char *)s.string_data[index];
                    args[arg].as.string_value.length =
                        s.string_lengths[index];
                }else
                    args[arg].as.int64_value = s.vals[index];
            }
            s.top -= nargs;
            if (ctx->session_key
                && wl_callback_session_depth
                >= WL_CALLBACK_SESSION_STACK_MAX) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_CALLBACK_REENTRANT;
                goto bad;
            }
            if (ctx->session_key)
                wl_callback_session_stack[wl_callback_session_depth++]
                    = ctx->session_key;
            void *callback_lease = wl_extension_callback_lease_acquire(
                ctx->extensions, descriptor);
            if (!callback_lease) {
                if (ctx->session_key)
                    wl_callback_session_depth--;
                if (status)
                    *status = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
                goto bad;
            }
            int callback_rc = descriptor->invoke
                ? descriptor->invoke(args, nargs, &result,
                    descriptor->user_data) : -1;
            wl_extension_callback_lease_release(callback_lease);
            if (ctx->session_key)
                wl_callback_session_depth--;
            if (callback_rc != 0) {
                if (status)
                    *status = WL_COLUMNAR_EXPR_CALLBACK_FAILURE;
                goto bad;
            }
            if (result.type == WIRELOG_EXTENSION_VALUE_BOOL) {
                if (result.size != sizeof(uint8_t)) {
                    if (status)
                        *status = WL_COLUMNAR_EXPR_INVALID_RESULT;
                    goto bad;
                }
                expr_push_typed(&s, result.as.bool_value != 0,
                    WIRELOG_EXTENSION_VALUE_BOOL);
            } else if (ctx->allow_extension_scalar_result
                && result.type == WIRELOG_EXTENSION_VALUE_INT64
                && result.size == sizeof(int64_t)) {
                expr_push_typed(&s, result.as.int64_value,
                    WIRELOG_EXTENSION_VALUE_INT64);
            } else {
                if (status)
                    *status = WL_COLUMNAR_EXPR_INVALID_RESULT;
                goto bad;
            }
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
            int64_t v = 0;
            int rc = intern ? wl_string_ops_cat_checked(a, b, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : 0);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_SUBSTR: {
            int64_t c = expr_pop(&s), b = expr_pop(&s), a = expr_pop(&s);
            int64_t v = 0;
            int rc = intern ? wl_string_ops_substr_checked(a, b, c, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : 0);
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
            int64_t v = 0;
            int rc = intern ? wl_string_ops_to_upper_checked(a, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_LOWER: {
            int64_t a = expr_pop(&s);
            int64_t v = 0;
            int rc = intern ? wl_string_ops_to_lower_checked(a, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_STR_REPLACE: {
            int64_t c = expr_pop(&s), b = expr_pop(&s), a = expr_pop(&s);
            int64_t v = 0;
            int rc = intern ? wl_string_ops_str_replace_checked(a, b, c, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TRIM: {
            int64_t a = expr_pop(&s);
            int64_t v = 0;
            int rc = intern ? wl_string_ops_trim_checked(a, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : a);
            break;
        }
        case WL_PLAN_EXPR_STR_FN_TO_STRING: {
            int64_t a = expr_pop(&s);
            int64_t v = 0;
            int rc = intern ? wl_string_ops_to_string_checked(a, intern,
                    &v) : EINVAL;
            if (intern && rc == ENOMEM)
                goto denied;
            expr_push(&s, intern ? (rc == 0 ? v : -1) : 0);
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
                goto denied; /* only interning can fail here */
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

denied:
    /* A string result could not be interned (growth refused by the memory
     * governor or allocation failed): the row must not carry a bad id, so
     * the evaluation fails with an allocation status (Issue #1470). */
    if (status)
        *status = WL_COLUMNAR_EXPR_ALLOCATION_FAILURE;
bad:
    *out_val = 0;
    if (status && *status == WL_COLUMNAR_EXPR_OK)
        *status = WL_COLUMNAR_EXPR_MALFORMED;
    return status ? *status : WL_COLUMNAR_EXPR_MALFORMED;
}

int
wl_columnar_expr_eval_run(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, int64_t *out_val, wl_intern_t *intern)
{
    wl_columnar_expr_context_t ctx = {
        .intern = intern,
        .extensions = NULL,
        .configured_worker_count = 1,
        .active_worker_count = 1,
        .parallel_execution = false,
        .session_key = NULL
    };
    wl_columnar_expr_status_t status;
    return wl_columnar_expr_eval_run_ctx(buf, size, row, ncols, out_val,
               &ctx, &status) == WL_COLUMNAR_EXPR_OK ? 0 : 1;
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

typedef struct {
    char name[256];
    uint32_t nargs;
    uint64_t identity;
    uint32_t version;
    bool abi;
} wl_columnar_expr_extension_call_t;

struct wl_columnar_expr_compiled {
    expr_instr_t *instrs;
    uint32_t ninstr;
    /* Issue #966: fixed for the whole expression, so it is resolved once here
     * rather than passed to the per-row evaluator.  Borrowed -- wl_plan_t
     * documents its intern as outliving the plan, which outlives any compiled
     * expression.  NULL is legal and matches the interpreter's own NULL-intern
     * fallbacks. */
    const wl_intern_t *intern;
    wl_columnar_memory_reservation_t *reservation;
};

typedef struct {
    wl_columnar_expr_compiled_t compiled;
    wl_columnar_memory_reservation_t token;
} wl_columnar_expr_controlled_owner_t;
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_expr_test_controlled_checkpoint)(wl_evaluation_control_t *,
    int, uint64_t, bool);
int wl_columnar_expr_test_fail_allocation = -1;
bool wl_columnar_expr_test_fail_commit;
#endif
static void
wl_columnar_expr_controlled_checkpoint(wl_evaluation_control_t *control,
    int stage, uint64_t progress, bool failed)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_test_controlled_checkpoint)
        wl_columnar_expr_test_controlled_checkpoint(control, stage, progress,
            failed);
#else
    (void)control; (void)stage; (void)progress; (void)failed;
#endif
}
static int
wl_columnar_expr_controlled_tail(wl_evaluation_control_t *control)
{
    return wl_evaluation_control_charge(control, 0);
}
static void *
wl_columnar_expr_controlled_malloc(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_test_fail_allocation >= 0
        && wl_columnar_expr_test_fail_allocation-- == 0) {
        wl_columnar_expr_test_fail_allocation = -1;
        return NULL;
    }
#endif
    return malloc(bytes);
}
/* Read and copy each consumed byte under separate admission. */
static int
wl_columnar_expr_controlled_bytes(const uint8_t *buf, uint32_t size,
    uint32_t *pos, uint8_t *out, uint32_t count, int pass,
    wl_evaluation_control_t *control)
{
    if (*pos > size || count > size - *pos) return ENOTSUP;
    for (uint32_t i = 0; i < count; i++) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        out[i] = buf[(*pos)++];
        wl_columnar_expr_controlled_checkpoint(control, pass, *pos, false);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
    }
    return 0;
}
static int
wl_columnar_expr_controlled_decode(const uint8_t *buf, uint32_t size,
    uint32_t *pos, expr_instr_t *out, int pass,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    *out = (expr_instr_t){0};
    rc = wl_columnar_expr_controlled_bytes(buf, size, pos, &out->op, 1, pass,
            control);
    if (rc) return rc;
    switch ((wl_plan_expr_tag_t)out->op) {
    case WL_PLAN_EXPR_VAR: {
        uint8_t encoded[2], name[12]; uint16_t length;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 2,
                pass, control);
        if (rc) return rc;
        memcpy(&length, encoded, sizeof(length));
        if (length < 4 || length > 12) return ENOTSUP;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, name, length,
                pass, control);
        if (rc) return rc;
        if (name[0] != 'c' || name[1] != 'o' || name[2] != 'l') return ENOTSUP;
        for (uint32_t d = 3; d < length; d++) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) return rc;
            if (name[d] < '0' || name[d] > '9') return ENOTSUP;
            out->iarg = out->iarg * 10u + (uint32_t)(name[d] - '0');
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) return rc;
        }
        break;
    }
    case WL_PLAN_EXPR_CONST_INT:
    case WL_PLAN_EXPR_CONST_FLOAT: {
        uint8_t encoded[8];
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 8,
                pass, control);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        memcpy(&out->larg, encoded, sizeof(out->larg));
        break;
    }
    case WL_PLAN_EXPR_BOOL: {
        uint8_t value;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, &value, 1,
                pass, control);
        if (rc) return rc;
        out->larg = value ? 1 : 0;
        break;
    }
    case WL_PLAN_EXPR_ARITH_ADD: case WL_PLAN_EXPR_ARITH_SUB:
    case WL_PLAN_EXPR_ARITH_MUL: case WL_PLAN_EXPR_ARITH_DIV:
    case WL_PLAN_EXPR_ARITH_MOD: case WL_PLAN_EXPR_ARITH_SHL:
    case WL_PLAN_EXPR_ARITH_SHR: case WL_PLAN_EXPR_ARITH_BAND:
    case WL_PLAN_EXPR_ARITH_BOR: case WL_PLAN_EXPR_ARITH_BXOR:
    case WL_PLAN_EXPR_ARITH_BNOT:
    case WL_PLAN_EXPR_CMP_EQ: case WL_PLAN_EXPR_CMP_NEQ:
    case WL_PLAN_EXPR_CMP_LT: case WL_PLAN_EXPR_CMP_LTE:
    case WL_PLAN_EXPR_CMP_GT: case WL_PLAN_EXPR_CMP_GTE:
    case WL_PLAN_EXPR_ARITH_FLOAT_ADD: case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
    case WL_PLAN_EXPR_ARITH_FLOAT_MUL: case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
    case WL_PLAN_EXPR_CMP_FLOAT_EQ: case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
    case WL_PLAN_EXPR_CMP_FLOAT_LT: case WL_PLAN_EXPR_CMP_FLOAT_LTE:
    case WL_PLAN_EXPR_CMP_FLOAT_GT: case WL_PLAN_EXPR_CMP_FLOAT_GTE:
    case WL_PLAN_EXPR_CMP_STR_EQ: case WL_PLAN_EXPR_CMP_STR_NEQ:
    case WL_PLAN_EXPR_CMP_STR_LT: case WL_PLAN_EXPR_CMP_STR_LTE:
    case WL_PLAN_EXPR_CMP_STR_GT: case WL_PLAN_EXPR_CMP_STR_GTE:
    case WL_PLAN_EXPR_AGG_COUNT: case WL_PLAN_EXPR_AGG_SUM:
    case WL_PLAN_EXPR_AGG_MIN: case WL_PLAN_EXPR_AGG_MAX:
        break;
    default: return ENOTSUP;
    }
    return wl_columnar_expr_controlled_tail(control);
}
void
wl_columnar_expr_compiled_free_controlled(wl_columnar_expr_compiled_t *compiled,
    wl_evaluation_control_t *control)
{
    if (!control) {
        wl_columnar_expr_compiled_free(compiled); return;
    }
    if (!compiled) return;
    wl_columnar_expr_controlled_owner_t *owner =
        (wl_columnar_expr_controlled_owner_t *)compiled;
    wl_columnar_memory_reservation_t token;
    wl_columnar_memory_reservation_init(&token);
    if (owner->token.bytes
        && !wl_columnar_memory_reservation_move(&token, &owner->token)) abort();
    free(compiled->instrs);
    free(owner);
    if (token.bytes) {
        uint64_t state = atomic_load_explicit(&token.state,
                memory_order_acquire);
        bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            ? wl_columnar_memory_release(&token) :
            wl_columnar_memory_rollback(&token);
        if (!retired) abort();
    }
}
int
wl_columnar_expr_compile_controlled(const uint8_t *buf, uint32_t size,
    wl_columnar_memory_governor_ref_t *governor, wl_col_session_t *sess,
    wl_columnar_expr_compiled_t **out, bool *supported,
    wl_evaluation_control_t *control)
{
    if (!out || !supported) return EINVAL;
    if (!control) {
        int error;
        *out = wl_columnar_expr_compile_governed(buf, size,
                sess ? sess->intern : NULL, governor, sess, &error);
        *supported = error == 0;
        return error == ENOTSUP ? 0 : error;
    }
    *out = NULL;
    if (!buf || !size) {
        *supported = false; return 0;
    }
    uint32_t count = 0, pos = 0;
    expr_instr_t instruction;
    int rc;
    while (pos < size) {
        rc = wl_columnar_expr_controlled_decode(buf, size, &pos, &instruction,
                1, control);
        if (rc == ENOTSUP) {
            *supported = false; return 0;
        }
        if (rc) return rc;
        if (count == UINT32_MAX) return EOVERFLOW;
        count++;
    }
    uint64_t instr_bytes, total;
    if (!wl_columnar_memory_size_mul(count, sizeof(expr_instr_t), &instr_bytes)
        || !wl_columnar_memory_size_add(
            sizeof(wl_columnar_expr_controlled_owner_t),
            instr_bytes, &total) || total > SIZE_MAX) return EOVERFLOW;
    wl_columnar_memory_reservation_t admission;
    wl_columnar_memory_reservation_init(&admission);
    wl_columnar_expr_controlled_owner_t *owner = NULL;
    bool admitted = false;
    rc = wl_evaluation_control_charge(control, 48);
    if (rc) return rc;
    wl_columnar_expr_controlled_checkpoint(control, 5, count, false);
    if (governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(governor), total, &admission,
            &status, control);
        if (rc) return rc;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status ==
                   WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : ENOMEM;
        }
        admitted = true;
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto fail;
    owner = wl_columnar_expr_controlled_malloc(sizeof(*owner));
    if (!owner) {
        rc = ENOMEM; goto fail;
    }
    owner->compiled = (wl_columnar_expr_compiled_t){.ninstr = count,
                                                    .intern =
                                                        sess ? sess->intern :
                                                        NULL};
    wl_columnar_memory_reservation_init(&owner->token);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto fail;
    owner->compiled.instrs =
        wl_columnar_expr_controlled_malloc((size_t)instr_bytes);
    if (!owner->compiled.instrs) {
        rc = ENOMEM; goto fail;
    }
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto fail;
    pos = 0;
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_columnar_expr_controlled_decode(buf, size, &pos, &instruction,
                2, control);
        if (rc) goto fail;
        rc = wl_evaluation_control_charge(control, 3);
        if (rc) goto fail;
        owner->compiled.instrs[i].op = instruction.op;
        owner->compiled.instrs[i].iarg = instruction.iarg;
        owner->compiled.instrs[i].larg = instruction.larg;
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) goto fail;
    }
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto fail;
    if (admitted) {
        if (!wl_columnar_memory_reservation_move(&owner->token, &admission)) {
            rc = EINVAL; goto fail;
        }
        admitted = false;
        owner->compiled.reservation = &owner->token;
        bool committed;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_expr_test_fail_commit) {
            wl_columnar_expr_test_fail_commit = false;
            rc = ENOMEM; goto fail;
        }
#endif
        rc = wl_columnar_memory_governor_commit_controlled(&owner->token,
                owner, &committed, control);
        if (rc) goto fail;
        if (!committed) {
            rc = ENOMEM; goto fail;
        }
    }
    wl_columnar_expr_controlled_checkpoint(control, 4, count, false);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto fail;
    *supported = true;
    *out = &owner->compiled;
    return 0;
fail:
    if (owner) wl_columnar_expr_compiled_free_controlled(&owner->compiled,
            control);
    if (admitted && !wl_columnar_memory_rollback(&admission)) abort();
    return rc;
}
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_expr_test_retirement_checkpoint)(
    wl_columnar_expr_operator_preparation_t *, int);
#endif
static void
wl_columnar_expr_retirement_checkpoint(
    wl_columnar_expr_operator_preparation_t *r,
    int phase)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_test_retirement_checkpoint)
        wl_columnar_expr_test_retirement_checkpoint(r, phase);
#else
    (void)r; (void)phase;
#endif
}
static int
wl_columnar_expr_checked_credit_retire(wl_columnar_memory_reservation_t *token)
{
    if (!token->bytes) return 0;
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) return 0;
    bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        ? wl_columnar_memory_release(token)
        : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && wl_columnar_memory_rollback(token);
    return retired ? 0 : EINVAL;
}
static int
wl_columnar_expr_checked_owner_retire(
    wl_columnar_expr_operator_preparation_t *r,
    wl_columnar_expr_compiled_t **slot)
{
    if (*slot) {
        /* Only owners allocated by compile_checked enter this record. */
        wl_columnar_expr_controlled_owner_t *owner =
            (wl_columnar_expr_controlled_owner_t *)*slot;
        uint64_t retired_state = atomic_load_explicit(
            &r->retired_expression.state, memory_order_acquire);
        if (retired_state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
            && retired_state !=
            WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) return EBUSY;
        wl_columnar_expr_retirement_checkpoint(r, 1);
        if (owner->token.bytes && !wl_columnar_memory_reservation_move(
                &r->retired_expression, &owner->token)) return EINVAL;
        free(owner->compiled.instrs); owner->compiled.instrs = NULL;
        free(owner); *slot = NULL;
        wl_columnar_expr_retirement_checkpoint(r, 2);
    }
    return wl_columnar_expr_checked_credit_retire(&r->retired_expression);
}
int
wl_columnar_expr_operator_retire(wl_columnar_expr_operator_preparation_t *r,
    int rc, bool mandatory, wl_evaluation_control_t *control)
{
    if (!r || !r->ready) return rc > 0 ? rc : EINVAL;
    /* Mandatory drain disables work admission, never identity/state checks. */
    if (!mandatory) {
        int stop = wl_evaluation_control_charge(control, 96);
        if (stop) {
            if (rc <= 0) rc = stop; mandatory = true;
        }
    }
    int cleanup = wl_columnar_expr_checked_owner_retire(r, &r->current);
    if (!cleanup) cleanup =
            wl_columnar_expr_checked_credit_retire(&r->compile_admission);
    if (!cleanup) cleanup = wl_columnar_expr_checked_owner_retire(r,
                &r->single);
    if (cleanup) return rc > 0 ? rc : cleanup;
    if (!mandatory) {
        int stop = wl_evaluation_control_charge(control, 0);
        if (stop) {
            if (rc <= 0) rc = stop; mandatory = true;
        }
    }
    while (r->retiring < r->initialized) {
        if (!mandatory) {
            int stop = wl_evaluation_control_charge(control, 64);
            if (stop) {
                if (rc <= 0) rc = stop; mandatory = true;
            }
        }
        cleanup = wl_columnar_expr_checked_owner_retire(r,
                &r->expressions[r->retiring]);
        if (cleanup) return rc > 0 ? rc : cleanup;
        r->retiring++;
        if (!mandatory) {
            wl_columnar_expr_retirement_checkpoint(r, 3);
            int stop = wl_evaluation_control_charge(control, 0);
            if (stop) {
                if (rc <= 0) rc = stop; mandatory = true;
            }
        }
    }
    if (!mandatory) {
        int stop = wl_evaluation_control_charge(control, 64);
        if (stop) {
            if (rc <= 0) rc = stop; mandatory = true;
        }
    }
    free(r->scratch_payload); r->scratch_payload = NULL;
    r->expressions = NULL;
    wl_columnar_expr_retirement_checkpoint(r, 4);
    cleanup = wl_columnar_expr_checked_credit_retire(&r->scratch);
    if (cleanup) return rc > 0 ? rc : cleanup;
    r->initialized = r->retiring = 0;
    if (!mandatory) {
        int stop = wl_evaluation_control_charge(control, 0);
        if (stop && rc <= 0) rc = stop;
    }
    return rc;
}
int
wl_columnar_expr_compile_checked(const uint8_t *buf, uint32_t size,
    wl_columnar_memory_governor_ref_t *governor, wl_col_session_t *sess,
    wl_columnar_expr_compiled_t **out, bool *supported,
    wl_columnar_expr_operator_preparation_t *record,
    wl_evaluation_control_t *control)
{
    if (!out || !supported || !record || !record->ready || !control
        || record->current || record->compile_admission.bytes) return EINVAL;
    *out = NULL;
    if (!buf || !size) {
        *supported = false; return 0;
    }
    uint32_t count = 0, pos = 0;
    expr_instr_t instruction;
    int rc;
    while (pos < size) {
        rc = wl_columnar_expr_controlled_decode(buf, size, &pos, &instruction,
                1, control);
        if (rc == ENOTSUP) {
            *supported = false; return 0;
        }
        if (rc) return rc;
        if (count == UINT32_MAX) return EOVERFLOW;
        count++;
    }
    uint64_t instr_bytes, total;
    if (!wl_columnar_memory_size_mul(count, sizeof(expr_instr_t), &instr_bytes)
        || !wl_columnar_memory_size_add(
            sizeof(wl_columnar_expr_controlled_owner_t),
            instr_bytes, &total) || total > SIZE_MAX) return EOVERFLOW;
    wl_columnar_memory_reservation_t *admission = &record->compile_admission;
    wl_columnar_expr_controlled_owner_t *owner = NULL;

    rc = wl_evaluation_control_charge(control, 48);
    if (rc) return rc;
    wl_columnar_expr_controlled_checkpoint(control, 5, count, false);
    if (governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(governor), total, admission,
            &status, control);
        if (rc) return rc;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status ==
                   WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : ENOMEM;
        }

    }
    wl_columnar_expr_retirement_checkpoint(record, 5);
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto fail;
    owner = wl_columnar_expr_controlled_malloc(sizeof(*owner));
    if (!owner) {
        rc = ENOMEM; goto fail;
    }
    record->current = &owner->compiled;
    owner->compiled = (wl_columnar_expr_compiled_t){.ninstr = count,
                                                    .intern =
                                                        sess ? sess->intern :
                                                        NULL};
    wl_columnar_memory_reservation_init(&owner->token);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto fail;
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto fail;
    owner->compiled.instrs =
        wl_columnar_expr_controlled_malloc((size_t)instr_bytes);
    if (!owner->compiled.instrs) {
        rc = ENOMEM; goto fail;
    }
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto fail;
    wl_columnar_expr_retirement_checkpoint(record, 6);
    pos = 0;
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_columnar_expr_controlled_decode(buf, size, &pos, &instruction,
                2, control);
        if (rc) goto fail;
        rc = wl_evaluation_control_charge(control, 3);
        if (rc) goto fail;
        owner->compiled.instrs[i].op = instruction.op;
        owner->compiled.instrs[i].iarg = instruction.iarg;
        owner->compiled.instrs[i].larg = instruction.larg;
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) goto fail;
    }
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) goto fail;
    if (admission->bytes) {
        if (!wl_columnar_memory_reservation_move(&owner->token, admission)) {
            rc = EINVAL; goto fail;
        }

        owner->compiled.reservation = &owner->token;
        bool committed;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_expr_test_fail_commit) {
            wl_columnar_expr_test_fail_commit = false;
            rc = ENOMEM; goto fail;
        }
#endif
        rc = wl_columnar_memory_governor_commit_controlled(&owner->token,
                owner, &committed, control);
        if (rc) goto fail;
        if (!committed) {
            rc = ENOMEM; goto fail;
        }
    }
    wl_columnar_expr_controlled_checkpoint(control, 4, count, false);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto fail;
    *supported = true;
    *out = &owner->compiled;
    record->current = NULL;
    return 0;
fail:
    return rc; /* Original current owner/admission remain in the frame. */
}
/* Published reverse strings are immutable under the caller's table lifetime.
 * Byte operations poll individually; no unbounded libc scan is hidden here. */
static int
wl_columnar_expr_controlled_string_length(const char *text, size_t *out,
    wl_evaluation_control_t *control)
{
    size_t n = 0;
    for (;;) {
        int rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        unsigned char byte = (unsigned char)text[n];
        wl_columnar_expr_controlled_checkpoint(control, 14, n + 1, false);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (!byte) {
            *out = n; return 0;
        }
        if (n == SIZE_MAX - 1) return EOVERFLOW;
        n++;
    }
}
static int
wl_columnar_expr_controlled_string_compare(expr_stack_t *stack, uint8_t op,
    const wl_intern_t *intern, wl_evaluation_control_t *control)
{
    int64_t b = expr_pop(stack), a = expr_pop(stack);
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    const char *sa = intern ? wl_intern_reverse(intern, a) : NULL;
    const char *sb = intern ? wl_intern_reverse(intern, b) : NULL;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    int ordering = 0; bool found = sa && sb;
    if (found) for (size_t i = 0;; i++) {
            rc = wl_evaluation_control_charge(control, 3);
            if (rc) return rc;
            unsigned char av = (unsigned char)sa[i], bv = (unsigned char)sb[i];
            ordering = av < bv ? -1 : av > bv ? 1 : 0;
            wl_columnar_expr_controlled_checkpoint(control, 15, i + 1, false);
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) return rc;
            if (ordering || !av) break;
        }
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    bool value;
    switch ((wl_plan_expr_tag_t)op) {
    case WL_PLAN_EXPR_CMP_STR_EQ: value = found ? ordering == 0 : a == b; break;
    case WL_PLAN_EXPR_CMP_STR_NEQ: value = found ? ordering != 0 : a != b;
        break;
    case WL_PLAN_EXPR_CMP_STR_LT: value = found && ordering < 0; break;
    case WL_PLAN_EXPR_CMP_STR_LTE: value = found && ordering <= 0; break;
    case WL_PLAN_EXPR_CMP_STR_GT: value = found && ordering > 0; break;
    default: value = found && ordering >= 0; break;
    }
    expr_push(stack, value);
    return 0;
}
/* Expression temporary always belongs to S, separate from intern's T. */
static int
wl_columnar_expr_controlled_string_temporary(size_t bytes, char **out,
    wl_columnar_memory_reservation_t *token, bool *held,
    wl_col_session_t *sess, bool *allocation, wl_evaluation_control_t *control)
{
    *out = NULL; *held = false;
    wl_columnar_memory_reservation_init(token);
    if (sess && sess->memory_governor) {
        wl_columnar_memory_admission_status_t admission;
        int rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(sess->memory_governor),
            bytes, token, &admission, control);
        if (rc) return rc;
        if (admission != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && admission != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (admission == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW)
                return EOVERFLOW;
            if (admission == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            *allocation = true; return 0;
        }
        *held = true;
    }
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    *out = wl_columnar_expr_controlled_malloc(bytes);
    if (!*out) *allocation = true;
    return wl_columnar_expr_controlled_tail(control);
}
static int
wl_columnar_expr_controlled_string_copy(char *out, const char *source,
    size_t length, wl_evaluation_control_t *control)
{
    for (size_t i = 0; i < length; i++) {
        int rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        out[i] = source[i];
        wl_columnar_expr_controlled_checkpoint(control, 16, i + 1, false);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
    }
    return 0;
}
/* Raw UTF8 next is permissive: invalid lead advances1, invalid continuation
 * advances to that byte. It checks at most4 bytes, including any terminator. */
static int
wl_columnar_expr_controlled_utf8_next(const char *text, const char **out,
    int64_t *value, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 12);
    if (rc) return rc;
    unsigned char c = (unsigned char)*text;
    unsigned length = c < 0x80 ? 1 : (c & 0xe0) == 0xc0 ? 2
        : (c & 0xf0) == 0xe0 ? 3 : (c & 0xf8) == 0xf0 ? 4 : 1;
    *out = !c ? NULL : text + 1;
    int64_t cp = c < 0x80 ? c : length == 2 ? c & 0x1f : length == 3
        ? c & 0x0f : length == 4 ? c & 7 : -1;
    if (!c) cp = -1;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    if (!c) {
        if (value) {
            *value = cp;
        }
        return 0;
    }
    for (unsigned i = 1; i < length; i++) {
        rc = wl_evaluation_control_charge(control, 4);
        if (rc) return rc;
        unsigned char byte = (unsigned char)text[i];
        *out = text + ((byte & 0xc0) == 0x80 ? i + 1 : i);
        cp = (byte & 0xc0) == 0x80 ? (cp << 6) | (byte & 0x3f) : -1;
        wl_columnar_expr_controlled_checkpoint(control, 17, i, false);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if ((byte & 0xc0) != 0x80) break;
    }
    if (value) *value = cp;
    return 0;
}
static int
wl_columnar_expr_controlled_string_produce(expr_stack_t *stack,
    const expr_instr_t *in, const uint8_t *bytecode, wl_col_session_t *sess,
    bool *allocation, wl_evaluation_control_t *control)
{
    wl_intern_t *intern = sess ? sess->intern : NULL;
    const char *a = NULL, *b = NULL;
    size_t first = 0, second = 0;
    int64_t result = 0;
    int rc;
    if (in->op == WL_PLAN_EXPR_CONST_STR) {
        if (!intern) {
            expr_push_string(stack, 0, bytecode + in->larg, in->iarg);
            return 0;
        }
        a = (const char *)(bytecode + in->larg); first = in->iarg;
    } else {
        int64_t count = 0, start = 0, idb = 0;
        if (in->op == WL_PLAN_EXPR_STR_FN_SUBSTR) {
            count = expr_pop(stack); start = expr_pop(stack);
        } else idb = expr_pop(stack);
        int64_t ida = expr_pop(stack);
        if (!intern) {
            expr_push(stack, 0); return 0;
        }
        rc = wl_evaluation_control_charge(control, 32);
        if (rc) return rc;
        a = wl_intern_reverse(intern, ida);
        if (in->op == WL_PLAN_EXPR_STR_FN_CAT) b = wl_intern_reverse(intern,
                    idb);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (!a || (in->op == WL_PLAN_EXPR_STR_FN_CAT && !b)
            || start < 0 || count < 0) {
            expr_push(stack, -1); return 0;
        }
        if (b) {
            rc = wl_columnar_expr_controlled_string_length(a, &first, control);
            if (rc) return rc;
            rc = wl_columnar_expr_controlled_string_length(b, &second, control);
            if (rc) return rc;
        } else {
            const char *p = a;
            int64_t i = 0;
            while (i < start) {
                const char *next;
                rc = wl_columnar_expr_controlled_utf8_next(p, &next, NULL,
                        control);
                if (rc) return rc;
                if (!next) {
                    expr_push(stack, -1); return 0;
                }
                p = next; i++;
            }
            a = p;
            for (i = 0; i < count; i++) {
                const char *next;
                rc = wl_columnar_expr_controlled_utf8_next(p, &next, NULL,
                        control);
                if (rc) return rc;
                if (!next) break;
                p = next;
            }
            first = (size_t)(p - a);
        }
    }
    if (second > SIZE_MAX - first ||
        first + second == SIZE_MAX) return EOVERFLOW;
    char *temporary = NULL;
    wl_columnar_memory_reservation_t token; bool held;
    rc = wl_columnar_expr_controlled_string_temporary(first + second + 1,
            &temporary, &token, &held, sess, allocation, control);
    if (rc || *allocation) goto done;
    rc = wl_columnar_expr_controlled_string_copy(temporary, a, first, control);
    if (rc) goto done;
    if (second) {
        rc = wl_columnar_expr_controlled_string_copy(temporary + first, b,
                second, control);
        if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto done;
    temporary[first + second] = '\0';
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto done;
    int semantic; bool denied;
    rc = wl_intern_put_controlled(intern, temporary, &result, &semantic,
            &denied, control);
    if (rc) goto done;
    if (denied && sess) sess->memory_budget_denied = true;
    if (semantic) {
        *allocation = true; goto done;
    }
    if (in->op == WL_PLAN_EXPR_CONST_STR)
        expr_push_string(stack, result, bytecode + in->larg, in->iarg);
    else expr_push(stack, result);
done:
    free(temporary);
    if (held && !wl_columnar_memory_rollback(&token)) abort();
    return rc;
}
/* Local reflected CRC has no raw lazy table or hardware/cache initialization.
 * Metadata16, native copy8, init8, byte17 (visit1 + bits8 + state8),
 * and finalize8 each end at a real cutoff. Borrowed symbol lifetime is
 * supplied by the ordinary caller; native storage stays in this frame. */
static int
wl_columnar_expr_controlled_crc(expr_stack_t *stack, uint8_t op,
    wl_col_session_t *sess, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    int64_t operand = expr_pop(stack);
    bool symbol = op == WL_PLAN_EXPR_ARITH_CRC32_ETH_S
        || op == WL_PLAN_EXPR_ARITH_CRC32_CAST_S;
    const char *text = symbol && sess && sess->intern
        ? wl_intern_reverse(sess->intern, operand) : NULL;
    uint32_t polynomial = op == WL_PLAN_EXPR_ARITH_CRC32_ETH
        || op == WL_PLAN_EXPR_ARITH_CRC32_ETH_S
        ? UINT32_C(0xedb88320) : UINT32_C(0x82f63b78);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    uint8_t native[8]; const uint8_t *bytes; size_t length;
    if (text) {
        rc = wl_columnar_expr_controlled_string_length(text, &length, control);
        if (rc) return rc;
        bytes = (const uint8_t *)text;
    } else {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        memcpy(native, &operand, sizeof(native));
        bytes = native; length = sizeof(native);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    uint32_t crc = UINT32_MAX;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    for (size_t i = 0; i < length; i++) {
        rc = wl_evaluation_control_charge(control, 17);
        if (rc) return rc;
        crc ^= bytes[i];
        for (unsigned bit = 0; bit < 8; bit++)
            crc = (crc >> 1) ^ ((crc & 1) ? polynomial : 0);
        wl_columnar_expr_controlled_checkpoint(control, 25, i + 1, false);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    int64_t result = (int64_t)(crc ^ UINT32_MAX);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    expr_push(stack, result);
    return 0;
}
static int
wl_columnar_expr_controlled_hash(expr_stack_t *stack, uint8_t op,
    wl_col_session_t *sess, bool *failed, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    int64_t operand = expr_pop(stack);
    const char *text = op == WL_PLAN_EXPR_ARITH_HASH_S && sess && sess->intern
        ? wl_intern_reverse(sess->intern, operand) : NULL;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    uint8_t native[8]; const uint8_t *bytes; size_t length;
    if (text) {
        rc = wl_columnar_expr_controlled_string_length(text, &length, control);
        if (rc) return rc;
        bytes = (const uint8_t *)text;
    } else {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        memcpy(native, &operand, sizeof(native));
        bytes = native; length = sizeof(native);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
    }
    int64_t value;
    rc = wl_columnar_expr_hash_run(bytes, length, &value, failed, control);
    if (!rc && !*failed) expr_push(stack, value);
    return rc;
}
static int
wl_columnar_expr_controlled_digest(expr_stack_t *stack, uint8_t op,
    wl_col_session_t *sess, bool *failed, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    int64_t operand = expr_pop(stack);
#ifndef WL_MBEDTLS_ENABLED
    (void)op; (void)sess; (void)operand; *failed = true; return 0;
#else
    const char *text = (op >= WL_PLAN_EXPR_ARITH_MD5_S &&
        op <= WL_PLAN_EXPR_ARITH_SHA512_S) && sess && sess->intern
        ? wl_intern_reverse(sess->intern, operand) : NULL;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    uint8_t native[8]; const uint8_t *bytes; size_t length;
    if (text) {
        rc = wl_columnar_expr_controlled_string_length(text, &length, control);
        if (rc) return rc;
        bytes = (const uint8_t *)text;
    } else {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        memcpy(native, &operand, sizeof(native));
        bytes = native; length = sizeof(native);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
    }
    int64_t value;
    rc = wl_columnar_expr_digest_run(op, bytes, length, &value, failed,
            control);
    if (!rc && !*failed) expr_push(stack, value);
    return rc;
#endif
}
static int
wl_columnar_expr_controlled_hmac(expr_stack_t *stack, uint8_t op,
    wl_col_session_t *sess, bool *failed, wl_evaluation_control_t *control)
{
    int rc = wl_columnar_expr_hmac_eligible(control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    int64_t key = expr_pop(stack), message = expr_pop(stack);
#ifndef WL_MBEDTLS_ENABLED
    (void)op; (void)sess; (void)key; (void)message; *failed = true; return 0;
#else
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    uint8_t native[2][8]; const uint8_t *bytes[2]; size_t lengths[2];
    const int64_t values[2] = {message, key};
    const bool requested[2] = {
        op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS ||
        op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI,
        op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS ||
        op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS
    };
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    for (unsigned i = 0; i < 2; i++) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return rc;
        const char *text = requested[i] && sess && sess->intern
            ? wl_intern_reverse(sess->intern, values[i]) : NULL;
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (text) {
            rc = wl_columnar_expr_controlled_string_length(text, &lengths[i],
                    control);
            if (rc) return rc;
            bytes[i] = (const uint8_t *)text;
        } else {
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) return rc;
            memcpy(native[i], &values[i], 8);
            bytes[i] = native[i]; lengths[i] = 8;
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) return rc;
        }
    }
    int64_t value;
    rc = wl_columnar_expr_hmac_run(bytes[0], lengths[0], bytes[1], lengths[1],
            &value, failed, control);
    if (!rc && !*failed) expr_push(stack, value);
    return rc;
#endif
}
static int
wl_columnar_expr_controlled_uuid_pair(expr_stack_t *stack, uint8_t op,
    wl_col_session_t *sess, bool *failed, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    int64_t name = expr_pop(stack), ns = expr_pop(stack);
#ifndef WL_MBEDTLS_ENABLED
    (void)op; (void)sess; (void)name; (void)ns; *failed = true; return 0;
#else
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    uint8_t native[2][8]; const uint8_t *bytes[2]; size_t lengths[2];
    bool symbols[2];
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    const int64_t values[2] = {ns, name};
    const bool requested[2] = {
        op == WL_PLAN_EXPR_ARITH_UUID5_SS || op == WL_PLAN_EXPR_ARITH_UUID5_SI,
        op == WL_PLAN_EXPR_ARITH_UUID5_SS || op == WL_PLAN_EXPR_ARITH_UUID5_IS
    };
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    for (unsigned i = 0; i < 2; i++) {
        rc = wl_evaluation_control_charge(control, 16);
        if (rc) return rc;
        const char *text = requested[i] && sess && sess->intern
            ? wl_intern_reverse(sess->intern, values[i]) : NULL;
        symbols[i] = text != NULL;
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (text) {
            rc = wl_columnar_expr_controlled_string_length(text, &lengths[i],
                    control);
            if (rc) return rc;
            bytes[i] = (const uint8_t *)text;
        } else {
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) return rc;
            memcpy(native[i], &values[i], 8);
            bytes[i] = native[i]; lengths[i] = 8;
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) return rc;
        }
    }
    int64_t value;
    rc = wl_columnar_expr_uuid_pair_run(bytes[0], lengths[0], symbols[0],
            bytes[1], lengths[1], symbols[1], op != WL_PLAN_EXPR_ARITH_UUID5,
            &value, failed, control);
    if (!rc && !*failed) expr_push(stack, value);
    return rc;
#endif
}
static int
wl_columnar_expr_controlled_uuid_rfc(expr_stack_t *stack,
    wl_col_session_t *sess, bool *failed, bool *allocation,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    int64_t name_id = expr_pop(stack), ns_id = expr_pop(stack);
#ifndef WL_MBEDTLS_ENABLED
    (void)sess; (void)name_id; (void)ns_id; (void)allocation;
    *failed = true; return 0;
#else
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    wl_intern_t *intern = sess ? sess->intern : NULL;
    const char *ns = intern ? wl_intern_reverse(intern, ns_id) : NULL;
    const char *name = intern ? wl_intern_reverse(intern, name_id) : NULL;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    if (!ns || !name) {
        *failed = true; return 0;
    }
    size_t length;
    rc = wl_columnar_expr_controlled_string_length(ns, &length, control);
    if (rc) return rc;
    if (length != 36) {
        *failed = true; return 0;
    }
    uint8_t namespace_uuid[16]; size_t pos = 0;
    for (size_t i = 0; i < 36; i++) {
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        unsigned char c = (unsigned char)ns[i]; bool invalid = false;
        if (i == 8 || i == 13 || i == 18 || i == 23) invalid = c != '-';
        else {
            unsigned char nibble = 0;
            if (c >= '0' && c <= '9') nibble = c - '0';
            else if (c >= 'a' && c <= 'f') nibble = c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') nibble = c - 'A' + 10;
            else invalid = true;
            if (!invalid) {
                if (!(pos & 1)) namespace_uuid[pos / 2] = nibble << 4;
                else namespace_uuid[pos / 2] |= nibble;
                pos++;
            }
        }
        wl_columnar_expr_controlled_checkpoint(control, 49, i + 1, invalid);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (invalid) {
            *failed = true; return 0;
        }
    }
    rc = wl_columnar_expr_controlled_string_length(name, &length, control);
    if (rc) return rc;
    char text[37];
    rc = wl_columnar_expr_uuid_rfc_run(namespace_uuid, (const uint8_t *)name,
            length, text, failed, control);
    if (rc || *failed) return rc;
    int64_t result; int semantic; bool denied;
    wl_columnar_expr_controlled_checkpoint(control, 50, 36, false);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    rc = wl_intern_put_controlled(intern, text, &result, &semantic, &denied,
            control);
    if (rc) return rc;
    if (denied) sess->memory_budget_denied = true;
    if (semantic) {
        *allocation = true; return 0;
    }
    expr_push(stack, result);
    return 0;
#endif
}
static int
wl_columnar_expr_controlled_string_match(const char *hay, size_t available,
    const char *needle, size_t length, bool *matches,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    *matches = false;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc || length > available) return rc;
    for (size_t i = 0; i < length; i++) {
        rc = wl_evaluation_control_charge(control, 3);
        if (rc) return rc;
        bool same = (unsigned char)hay[i] == (unsigned char)needle[i];
        wl_columnar_expr_controlled_checkpoint(control, 18, i + 1, false);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (!same) return 0;
    }
    *matches = true;
    return 0;
}
static int
wl_columnar_expr_controlled_string_put(const char *text, int64_t *value,
    wl_col_session_t *sess, bool *allocation, wl_evaluation_control_t *control)
{
    int status; bool denied;
    int rc = wl_intern_put_controlled(sess->intern, text, value, &status,
            &denied, control);
    if (rc) return rc;
    if (denied) sess->memory_budget_denied = true;
    if (status) *allocation = true;
    return 0;
}
static int
wl_columnar_expr_controlled_number_byte(const char *text, size_t offset,
    unsigned char *out, int stage, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 2);
    if (rc) return rc;
    unsigned char byte = (unsigned char)text[offset];
    wl_columnar_expr_controlled_checkpoint(control, stage, offset + 1, false);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    *out = byte; return 0;
}
static int
wl_columnar_expr_controlled_number(const char *text, int64_t *value,
    bool *failed, wl_evaluation_control_t *control)
{
    if (!text) {
        *value = 0; return 0;
    }
    size_t offset = 0; unsigned char byte; int rc;
    for (;;) {
        rc = wl_columnar_expr_controlled_number_byte(text, offset, &byte, 19,
                control);
        if (rc) return rc;
        if (!isspace(byte)) break;
        if (offset == SIZE_MAX - 1) return EOVERFLOW;
        offset++;
    }
    bool negative = byte == '-';
    if (negative || byte == '+') {
        if (offset == SIZE_MAX - 1) return EOVERFLOW;
        offset++;
        rc = wl_columnar_expr_controlled_number_byte(text, offset, &byte, 20,
                control);
        if (rc) return rc;
    }
    uint64_t magnitude = 0, threshold = (uint64_t)INT64_MAX + negative;
    bool overflow = false; uint64_t digits = 0;
    while (byte >= '0' && byte <= '9') {
        rc = wl_evaluation_control_charge(control, 12);
        if (rc) return rc;
        unsigned digit = byte - '0';
        if (!overflow) {
            overflow = magnitude > threshold / 10
                || (magnitude == threshold / 10 && digit > threshold % 10);
            if (!overflow) magnitude = magnitude * 10 + digit;
        }
        wl_columnar_expr_controlled_checkpoint(control, 21, ++digits, overflow);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (offset == SIZE_MAX - 1) return EOVERFLOW;
        offset++;
        rc = wl_columnar_expr_controlled_number_byte(text, offset, &byte, 20,
                control);
        if (rc) return rc;
    }
    if (overflow) *failed = true;
    else *value = negative ? magnitude == (uint64_t)INT64_MAX + 1 ? INT64_MIN
        : -(int64_t)magnitude : (int64_t)magnitude;
    return 0;
}
static int
wl_columnar_expr_controlled_builtin(expr_stack_t *stack, uint8_t op,
    wl_col_session_t *sess, bool *failed, bool *allocation,
    wl_evaluation_control_t *control)
{
    int64_t c = 0, b = 0;
    if (op == WL_PLAN_EXPR_STR_FN_STR_REPLACE) c = expr_pop(stack);
    if (op == WL_PLAN_EXPR_STR_FN_STR_REPLACE
        || op == WL_PLAN_EXPR_STR_FN_CONTAINS
        || op == WL_PLAN_EXPR_STR_FN_STR_PREFIX
        || op == WL_PLAN_EXPR_STR_FN_STR_SUFFIX) b = expr_pop(stack);
    int64_t a = expr_pop(stack), result = 0;
    wl_intern_t *intern = sess ? sess->intern : NULL;
    if (!intern) {
        if (op == WL_PLAN_EXPR_STR_FN_TO_NUMBER) *failed = true;
        else expr_push(stack, op == WL_PLAN_EXPR_STR_FN_TO_UPPER
                || op == WL_PLAN_EXPR_STR_FN_TO_LOWER
                || op == WL_PLAN_EXPR_STR_FN_TRIM
                || op == WL_PLAN_EXPR_STR_FN_STR_REPLACE ? a : 0);
        return 0;
    }
    if (op == WL_PLAN_EXPR_STR_FN_TO_STRING) {
        int rc = wl_evaluation_control_charge(control, 64);
        if (rc) return rc;
        char text[24];
        int written = snprintf(text, sizeof(text), "%" PRId64, a);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (written < 0 || (size_t)written >= sizeof(text)) {
            expr_push(stack, -1); return 0;
        }
        rc = wl_columnar_expr_controlled_string_put(text, &result, sess,
                allocation, control);
        if (rc) return rc;
        if (!*allocation) expr_push(stack, result);
        return 0;
    }
    int rc = wl_evaluation_control_charge(control, 48);
    if (rc) return rc;
    const char *source = wl_intern_reverse(intern, a);
    const char *pattern = NULL, *replacement = NULL;
    bool binary = op == WL_PLAN_EXPR_STR_FN_CONTAINS
        || op == WL_PLAN_EXPR_STR_FN_STR_PREFIX
        || op == WL_PLAN_EXPR_STR_FN_STR_SUFFIX
        || op == WL_PLAN_EXPR_STR_FN_STR_REPLACE;
    if (binary) pattern = wl_intern_reverse(intern, b);
    if (op == WL_PLAN_EXPR_STR_FN_STR_REPLACE)
        replacement = wl_intern_reverse(intern, c);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    if (op == WL_PLAN_EXPR_STR_FN_TO_NUMBER) {
        rc = wl_columnar_expr_controlled_number(source, &result, failed,
                control);
        if (rc) return rc;
        if (!*failed) expr_push(stack, result);
        return 0;
    }
    if (!source || (binary && !pattern)
        || (op == WL_PLAN_EXPR_STR_FN_STR_REPLACE && !replacement)) {
        expr_push(stack, op == WL_PLAN_EXPR_STR_FN_CONTAINS
            || op == WL_PLAN_EXPR_STR_FN_STR_PREFIX
            || op == WL_PLAN_EXPR_STR_FN_STR_SUFFIX ? 0 : -1);
        return 0;
    }
    if (op == WL_PLAN_EXPR_STR_FN_STRLEN || op == WL_PLAN_EXPR_STR_FN_STR_ORD) {
        const char *p = source;
        if (op == WL_PLAN_EXPR_STR_FN_STR_ORD) {
            rc = wl_columnar_expr_controlled_utf8_next(p, &p, &result, control);
            if (rc) return rc;
        } else {
            for (;;) {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc) return rc;
                const char *next;
                rc = wl_columnar_expr_controlled_utf8_next(p, &next, NULL,
                        control);
                if (rc) return rc;
                if (!next) break;
                if (result == INT64_MAX) return EOVERFLOW;
                p = next; result++;
                wl_columnar_expr_controlled_checkpoint(control, 22,
                    (uint64_t)result, false);
                rc = wl_columnar_expr_controlled_tail(control);
                if (rc) return rc;
            }
        }
        expr_push(stack, result); return 0;
    }
    size_t length, pattern_length = 0, replacement_length = 0;
    rc = wl_columnar_expr_controlled_string_length(source, &length, control);
    if (rc) return rc;
    if (binary) {
        rc = wl_columnar_expr_controlled_string_length(pattern, &pattern_length,
                control);
        if (rc) return rc;
    }
    if (op == WL_PLAN_EXPR_STR_FN_CONTAINS ||
        op == WL_PLAN_EXPR_STR_FN_STR_PREFIX
        || op == WL_PLAN_EXPR_STR_FN_STR_SUFFIX) {
        bool matches = false;
        if (op == WL_PLAN_EXPR_STR_FN_CONTAINS) {
            for (size_t i = 0;; i++) {
                rc = wl_columnar_expr_controlled_string_match(source + i,
                        length - i, pattern, pattern_length, &matches, control);
                if (rc) return rc;
                if (matches || i == length) break;
            }
        } else {
            size_t offset = op == WL_PLAN_EXPR_STR_FN_STR_SUFFIX
                && pattern_length <= length ? length - pattern_length : 0;
            rc = wl_columnar_expr_controlled_string_match(source + offset,
                    length - offset, pattern, pattern_length, &matches,
                    control);
            if (rc) return rc;
        }
        expr_push(stack, matches); return 0;
    }
    size_t begin = 0, end = length, output_length = length, matches = 0;
    if (op == WL_PLAN_EXPR_STR_FN_TRIM) {
        while (begin < length) {
            rc = wl_evaluation_control_charge(control, 3);
            if (rc) return rc;
            bool space = isspace((unsigned char)source[begin]);
            wl_columnar_expr_controlled_checkpoint(control, 23, begin + 1,
                false);
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) return rc;
            if (!space) break;
            begin++;
        }
        while (end > begin) {
            rc = wl_evaluation_control_charge(control, 3);
            if (rc) return rc;
            bool space = isspace((unsigned char)source[end - 1]);
            wl_columnar_expr_controlled_checkpoint(control, 24,
                length - end + 1, false);
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) return rc;
            if (!space) break;
            end--;
        }
        output_length = end - begin;
    } else if (op == WL_PLAN_EXPR_STR_FN_STR_REPLACE) {
        if (!pattern_length) {
            expr_push(stack, a); return 0;
        }
        rc = wl_columnar_expr_controlled_string_length(replacement,
                &replacement_length, control);
        if (rc) return rc;
        for (size_t i = 0; i < length;) {
            bool same;
            rc = wl_columnar_expr_controlled_string_match(source + i,
                    length - i,
                    pattern, pattern_length, &same, control);
            if (rc) return rc;
            if (same) {
                matches++; i += pattern_length;
            } else i++;
        }
        if (!matches) {
            expr_push(stack, a); return 0;
        }
        uint64_t difference, size;
        size_t delta = replacement_length >= pattern_length
            ? replacement_length - pattern_length : pattern_length -
            replacement_length;
        if (!wl_columnar_memory_size_mul(matches, delta,
            &difference)) return EOVERFLOW;
        if (replacement_length >= pattern_length) {
            if (!wl_columnar_memory_size_add(length, difference,
                &size)) return EOVERFLOW;
        } else {
            if (difference > length) return EOVERFLOW;
            size = length - difference;
        }
        if (size >= SIZE_MAX) return EOVERFLOW;
        output_length = (size_t)size;
    }
    if (output_length == SIZE_MAX) return EOVERFLOW;
    char *buffer = NULL; wl_columnar_memory_reservation_t token; bool held;
    rc = wl_columnar_expr_controlled_string_temporary(output_length + 1,
            &buffer, &token, &held, sess, allocation, control);
    if (rc || *allocation) goto done;
    if (op == WL_PLAN_EXPR_STR_FN_STR_REPLACE) {
        size_t output = 0;
        for (size_t i = 0; i < length;) {
            bool same;
            rc = wl_columnar_expr_controlled_string_match(source + i,
                    length - i,
                    pattern, pattern_length, &same, control);
            if (rc) goto done;
            size_t count = same ? replacement_length : 1;
            rc = wl_columnar_expr_controlled_string_copy(buffer + output,
                    same ? replacement : source + i, count, control);
            if (rc) goto done;
            output += count; i += same ? pattern_length : 1;
        }
    } else {
        for (size_t i = 0; i < output_length; i++) {
            rc = wl_evaluation_control_charge(control, 4);
            if (rc) goto done;
            unsigned char byte = (unsigned char)source[begin + i];
            if (op == WL_PLAN_EXPR_STR_FN_TO_UPPER && byte >= 'a' &&
                byte <= 'z') byte -= 32;
            if (op == WL_PLAN_EXPR_STR_FN_TO_LOWER && byte >= 'A' &&
                byte <= 'Z') byte += 32;
            buffer[i] = (char)byte;
            wl_columnar_expr_controlled_checkpoint(control, 16, i + 1, false);
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) goto done;
        }
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) goto done;
    buffer[output_length] = '\0';
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto done;
    rc = wl_columnar_expr_controlled_string_put(buffer, &result, sess,
            allocation, control);
    if (!rc && !*allocation) expr_push(stack, result);
done:
    free(buffer);
    if (held && !wl_columnar_memory_rollback(&token)) abort();
    return rc;
}
static int
wl_columnar_expr_controlled_numeric_step(expr_stack_t *stack,
    const expr_instr_t *in, const int64_t *row, uint32_t ncols,
    bool interpreted, bool *failed, const wl_intern_t *intern,
    wl_evaluation_control_t *control)
{
    int rc;
    *failed = false;
    if (in->op >= WL_PLAN_EXPR_CMP_STR_EQ && in->op <= WL_PLAN_EXPR_CMP_STR_GTE)
        return wl_columnar_expr_controlled_string_compare(stack, in->op,
                   intern, control);
    if (in->op == WL_PLAN_EXPR_VAR) {
        if (in->iarg < ncols) {
            rc = wl_evaluation_control_charge(control, 1);
            if (rc) return rc;
            expr_push(stack, row[in->iarg]);
        } else expr_push(stack, 0);
    } else if (in->op == WL_PLAN_EXPR_CONST_FLOAT) {
        double decoded;
        if (!expr_float_load(in->larg, &decoded)) *failed = true;
        else expr_push(stack, in->larg);
    } else if (in->op == WL_PLAN_EXPR_CONST_INT ||
        in->op == WL_PLAN_EXPR_BOOL) {
        if (interpreted && in->op == WL_PLAN_EXPR_BOOL)
            expr_push_typed(stack, in->larg, WIRELOG_EXTENSION_VALUE_BOOL);
        else expr_push(stack, in->larg);
    } else if (in->op == WL_PLAN_EXPR_ARITH_BNOT) {
        int64_t a = expr_pop(stack); expr_push(stack, ~a);
    } else if (in->op == WL_PLAN_EXPR_AGG_COUNT ||
        in->op == WL_PLAN_EXPR_AGG_SUM
        || in->op == WL_PLAN_EXPR_AGG_MIN ||
        in->op == WL_PLAN_EXPR_AGG_MAX) {
        /* Actual compiled aggregates are no-ops. */
    } else {
        int64_t b = expr_pop(stack), a = expr_pop(stack), result = 0;
        int semantic = 0;
        switch ((wl_plan_expr_tag_t)in->op) {
        case WL_PLAN_EXPR_ARITH_ADD: semantic =
                wl_columnar_arithmetic_checked_add_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_SUB: semantic =
                wl_columnar_arithmetic_checked_sub_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_MUL: semantic =
                wl_columnar_arithmetic_checked_mul_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_DIV: semantic =
                wl_columnar_arithmetic_checked_div_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_MOD: semantic =
                wl_columnar_arithmetic_checked_mod_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_SHL: semantic =
                wl_columnar_arithmetic_checked_shl_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_SHR: semantic =
                wl_columnar_arithmetic_checked_shr_int64(a, b, &result);
            break;
        case WL_PLAN_EXPR_ARITH_BAND: result = a & b; break;
        case WL_PLAN_EXPR_ARITH_BOR: result = a | b; break;
        case WL_PLAN_EXPR_ARITH_BXOR: result = a ^ b; break;
        case WL_PLAN_EXPR_CMP_EQ: result = a == b; break;
        case WL_PLAN_EXPR_CMP_NEQ: result = a != b; break;
        case WL_PLAN_EXPR_CMP_LT: result = a < b; break;
        case WL_PLAN_EXPR_CMP_LTE: result = a <= b; break;
        case WL_PLAN_EXPR_CMP_GT: result = a > b; break;
        case WL_PLAN_EXPR_CMP_GTE: result = a >= b; break;
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD: case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL: case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
        case WL_PLAN_EXPR_CMP_FLOAT_EQ: case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
        case WL_PLAN_EXPR_CMP_FLOAT_LT: case WL_PLAN_EXPR_CMP_FLOAT_GT:
        case WL_PLAN_EXPR_CMP_FLOAT_LTE: case WL_PLAN_EXPR_CMP_FLOAT_GTE: {
            double da, db, dv = 0;
            if (!expr_float_load(a, &da) || !expr_float_load(b, &db)) {
                semantic = 1; break;
            }
            switch ((wl_plan_expr_tag_t)in->op) {
            case WL_PLAN_EXPR_ARITH_FLOAT_ADD: dv = da + db; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_SUB: dv = da - db; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_MUL: dv = da * db; break;
            case WL_PLAN_EXPR_ARITH_FLOAT_DIV: dv = da / db; break;
            case WL_PLAN_EXPR_CMP_FLOAT_EQ: result = da == db; break;
            case WL_PLAN_EXPR_CMP_FLOAT_NEQ: result = da != db; break;
            case WL_PLAN_EXPR_CMP_FLOAT_LT: result = da < db; break;
            case WL_PLAN_EXPR_CMP_FLOAT_GT: result = da > db; break;
            case WL_PLAN_EXPR_CMP_FLOAT_LTE: result = da <= db; break;
            default: result = da >= db; break;
            }
            if (in->op <= WL_PLAN_EXPR_ARITH_FLOAT_DIV && !expr_float_store(dv,
                &result)) semantic = 1;
            break;
        }
        default: semantic = 1; break;
        }
        *failed = semantic != 0;
        if (!*failed) expr_push(stack, result);
    }
    return 0;
}
int
wl_columnar_expr_eval_compiled_controlled(const wl_columnar_expr_compiled_t *c,
    const int64_t *row, uint32_t ncols, int64_t *value, bool *failed,
    wl_evaluation_control_t *control)
{
    if (!c || !value || !failed || (ncols && !row)) return EINVAL;
    if (!control) {
        *failed = wl_columnar_expr_eval_compiled(c, row, ncols, value) != 0;
        return 0;
    }
    expr_stack_t stack;
    stack.top = 0;
    for (uint32_t k = 0; k < c->ninstr; k++) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 32);
        if (rc) return rc;
        const expr_instr_t *in = &c->instrs[k];
        bool pending_failed;
        rc = wl_columnar_expr_controlled_numeric_step(&stack, in, row, ncols,
                false, &pending_failed, c->intern, control);
        if (rc) return rc;
        wl_columnar_expr_controlled_checkpoint(control, 3, k + 1u,
            pending_failed);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (pending_failed) {
            *failed = true; *value = 0; return 0;
        }
    }
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    int64_t pending = stack.top ? stack.vals[stack.top - 1] : 0;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    *value = pending; *failed = false;
    return 0;
}

/* The lease protects the descriptor only for this invocation. The session or
 * direct caller must keep the snapshot alive across the complete attempt. */
static int
wl_columnar_expr_controlled_extension(expr_stack_t *stack,
    const wl_columnar_expr_extension_call_t *call, wl_col_session_t *sess,
    bool allow_scalar, wl_columnar_expr_status_t *pending_status,
    wl_evaluation_control_t *control)
{
    const wirelog_extension_descriptor_t *descriptor = NULL;
    void *lease = NULL;
    bool tls_pushed = false;
    int64_t scalar = 0;
    uint32_t scalar_type = WIRELOG_EXTENSION_VALUE_BOOL;
    const void *session_key = sess ? sess->callback_session_key : NULL;
    *pending_status = WL_COLUMNAR_EXPR_OK;
    if (!sess || !sess->base.extension_snapshot) {
        *pending_status = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
        return 0;
    }
    int rc = wl_extension_callback_lookup_controlled(
        sess->base.extension_snapshot, call->name, &descriptor, &lease,
        control);
    if (rc) return rc;
    if (!descriptor) {
        *pending_status = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
        return 0;
    }
    rc = wl_evaluation_control_charge(control, 48);
    if (rc) goto cleanup;
    /* Copy the metadata needed after invocation before any host effects. */
    uint32_t policy = descriptor->callback_policy;
    uint32_t arity = descriptor->arity;
    uint32_t declared_result = descriptor->result_type;
    uint64_t identity = descriptor->addon_abi_identity;
    uint32_t version = descriptor->addon_abi_version;
    const uint32_t *expected_types = descriptor->argument_types;
    wirelog_extension_scalar_fn invoke = descriptor->invoke;
    void *user_data = descriptor->user_data;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto cleanup;
    if (call->abi && (call->identity != identity || call->version != version)) {
        *pending_status = WL_COLUMNAR_EXPR_EXTENSION_ABI_MISMATCH;
        goto cleanup;
    }
    if (sess->callback_parallel_execution && sess->callback_active_workers > 1
        && !(policy & WIRELOG_EXTENSION_CALLBACK_THREAD_SAFE)) {
        *pending_status = WL_COLUMNAR_EXPR_CALLBACK_POLICY;
        goto cleanup;
    }
    if (session_key && !(policy & WIRELOG_EXTENSION_CALLBACK_REENTRANT)) {
        for (uint32_t depth = 0; depth < wl_callback_session_depth; depth++) {
            rc = wl_evaluation_control_charge(control, 4);
            if (rc) goto cleanup;
            bool same = wl_callback_session_stack[depth] == session_key;
            wl_columnar_expr_controlled_checkpoint(control, 85, depth + 1,
                same);
            rc = wl_columnar_expr_controlled_tail(control);
            if (rc) goto cleanup;
            if (same) {
                *pending_status = WL_COLUMNAR_EXPR_CALLBACK_REENTRANT;
                goto cleanup;
            }
        }
    }
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) goto cleanup;
    if (call->nargs != arity || call->nargs > stack->top
        || call->nargs > COL_FILTER_STACK) {
        *pending_status = WL_COLUMNAR_EXPR_ARITY_MISMATCH;
        goto cleanup;
    }
    if (declared_result != WIRELOG_EXTENSION_VALUE_BOOL
        && (!allow_scalar ||
        declared_result != WIRELOG_EXTENSION_VALUE_INT64)){
        *pending_status = WL_COLUMNAR_EXPR_INVALID_RESULT;
        goto cleanup;
    }
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto cleanup;
    wirelog_extension_value_t args[COL_FILTER_STACK];
    for (uint32_t arg = 0; arg < call->nargs; arg++) {
        rc = wl_evaluation_control_charge(control, 32);
        if (rc) goto cleanup;
        uint32_t index = stack->top - call->nargs + arg;
        uint32_t actual = stack->types[index];
        uint32_t expected = expected_types ? expected_types[arg] : 0;
        bool mismatch = expected != 0 && actual != expected;
        if (!mismatch) {
            args[arg].type = actual;
            args[arg].size = actual == WIRELOG_EXTENSION_VALUE_BOOL
                ? sizeof(uint8_t) : actual == WIRELOG_EXTENSION_VALUE_STRING
                ? stack->string_lengths[index] : sizeof(int64_t);
            if (actual == WIRELOG_EXTENSION_VALUE_BOOL)
                args[arg].as.bool_value = stack->vals[index] != 0;
            else if (actual == WIRELOG_EXTENSION_VALUE_STRING) {
                args[arg].as.string_value.data =
                    (const char *)stack->string_data[index];
                args[arg].as.string_value.length = stack->string_lengths[index];
            } else args[arg].as.int64_value = stack->vals[index];
        }
        wl_columnar_expr_controlled_checkpoint(control, 86, arg + 1, mismatch);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) goto cleanup;
        if (mismatch) {
            *pending_status = WL_COLUMNAR_EXPR_TYPE_MISMATCH;
            goto cleanup;
        }
    }
    /* Admit pop/TLS/result initialization (24), scalar validation/copy (24)
    * and final typed push (16) before the non-preemptible host callback. */
    rc = wl_evaluation_control_charge(control, 64);
    if (rc) goto cleanup;
    stack->top -= call->nargs;
    if (session_key &&
        wl_callback_session_depth >= WL_CALLBACK_SESSION_STACK_MAX){
        *pending_status = WL_COLUMNAR_EXPR_CALLBACK_REENTRANT;
        goto cleanup;
    }
    if (session_key) {
        wl_callback_session_stack[wl_callback_session_depth++] = session_key;
        tls_pushed = true;
    }
    wirelog_extension_value_t result;
    result.type = 0;
    result.size = 0;
    result.as.int64_value = 0;
    wl_columnar_expr_controlled_checkpoint(control, 87, call->nargs, false);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) goto cleanup;
    int callback_rc = invoke ? invoke(args, call->nargs, &result,
            user_data) : -1;
    if (callback_rc != 0)
        *pending_status = WL_COLUMNAR_EXPR_CALLBACK_FAILURE;
    else if (result.type == WIRELOG_EXTENSION_VALUE_BOOL
        && result.size == sizeof(uint8_t)) {
        scalar = result.as.bool_value != 0;
    } else if (allow_scalar && result.type == WIRELOG_EXTENSION_VALUE_INT64
        && result.size == sizeof(int64_t)) {
        scalar = result.as.int64_value;
        scalar_type = WIRELOG_EXTENSION_VALUE_INT64;
    } else *pending_status = WL_COLUMNAR_EXPR_INVALID_RESULT;
cleanup:
    /* A final entry release may invoke destroy_userdata. Keep raw TLS ordering
     * so same-session reentry from that destructor still sees the active call. */
    wl_extension_callback_lease_release(lease);
    if (tls_pushed) wl_callback_session_depth--;
    if (rc) return rc;
    wl_columnar_expr_controlled_checkpoint(control, 88, call->nargs,
        *pending_status != WL_COLUMNAR_EXPR_OK);
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    if (*pending_status == WL_COLUMNAR_EXPR_OK)
        expr_push_typed(stack, scalar, scalar_type);
    return 0;
}

/* Admitted fallback decodes real instruction boundaries. Classification never
 * loads a row or treats bytes after an unknown/truncated instruction as tags. */
static int
wl_columnar_expr_controlled_interpreter_decode(const uint8_t *buf,
    uint32_t size, uint32_t *pos, expr_instr_t *in, long *column,
    bool classify, bool *malformed, bool *excluded,
    wl_columnar_expr_extension_call_t *extension,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 33);
    if (rc) return rc;
    *in = (expr_instr_t){0}; *column = 0;
    *malformed = false; *excluded = false;
    rc = wl_columnar_expr_controlled_bytes(buf, size, pos, &in->op, 1,
            classify ? 6 : 7, control);
    if (rc) {
        if (rc == ENOTSUP) {
            *malformed = true; return 0;
        }
        return rc;
    }
    switch ((wl_plan_expr_tag_t)in->op) {
    case WL_PLAN_EXPR_VAR: case WL_PLAN_EXPR_VAR_FLOAT:
    case WL_PLAN_EXPR_VAR_STRING: {
        uint8_t encoded[2]; uint16_t length;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 2,
                classify ? 6 : 7, control);
        if (rc) break;
        memcpy(&length, encoded, 2);
        if (length > size - *pos) {
            *malformed = true; return 0;
        }
        uint32_t begin = *pos;
        if (!classify && length > 3) {
            uint8_t prefix[3]; uint32_t probe = begin;
            rc = wl_columnar_expr_controlled_bytes(buf, size, &probe, prefix, 3,
                    7, control);
            if (rc) return rc;
            if (prefix[0] == 'c' && prefix[1] == 'o' && prefix[2] == 'l') {
                char suffix[16];
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) return rc;
                memset(suffix, 0, sizeof(suffix));
                rc = wl_columnar_expr_controlled_tail(control);
                if (rc) return rc;
                uint32_t count = length - 3 < 15 ? length - 3 : 15;
                rc = wl_columnar_expr_controlled_bytes(buf, size, &probe,
                        (uint8_t *)suffix, count, 7, control);
                if (rc) return rc;
                rc = wl_evaluation_control_charge(control, 32);
                if (rc) return rc;
                *column = strtol(suffix, NULL, 10);
                rc = wl_columnar_expr_controlled_tail(control);
                if (rc) return rc;
            }
        }
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        *pos = begin + length;
        break;
    }
    case WL_PLAN_EXPR_CONST_STR: {
        uint8_t encoded[2]; uint16_t length;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 2,
                classify ? 6 : 7, control);
        if (rc) break;
        memcpy(&length, encoded, 2);
        if (length > size - *pos) {
            *malformed = true; return 0;
        }
        rc = wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        in->iarg = length; in->larg = *pos; *pos += length;
        break;
    }
    case WL_PLAN_EXPR_EXTENSION_CALL: case WL_PLAN_EXPR_EXTENSION_CALL_ABI: {
        uint8_t encoded[12];
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 2,
                classify ? 6 : 7, control);
        if (rc) break;
        uint16_t length = (uint16_t)encoded[0] | (uint16_t)encoded[1] << 8;
        if (!length || length >= sizeof(extension->name) ||
            length > size - *pos || size - *pos - length < 4) {
            *malformed = true; return 0;
        }
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos,
                (uint8_t *)extension->name, length, classify ? 6 : 7, control);
        if (rc) break;
        rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
        extension->name[length] = 0;
        rc = wl_columnar_expr_controlled_tail(control); if (rc) return rc;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 4,
                classify ? 6 : 7, control);
        if (rc) break;
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        extension->nargs = (uint32_t)encoded[0] | (uint32_t)encoded[1] << 8 |
            (uint32_t)encoded[2] << 16 | (uint32_t)encoded[3] << 24;
        extension->identity = 0; extension->version = 0;
        extension->abi = in->op == WL_PLAN_EXPR_EXTENSION_CALL_ABI;
        rc = wl_columnar_expr_controlled_tail(control); if (rc) return rc;
        if (extension->abi) {
            rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 12,
                    classify ? 6 : 7, control);
            if (rc) break;
            for (uint32_t j = 0; j < 8; j++) {
                rc = wl_evaluation_control_charge(control, 2);
                if (rc) return rc;
                extension->identity |= (uint64_t)encoded[j] << (j * 8);
                rc = wl_columnar_expr_controlled_tail(control);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
            extension->version = (uint32_t)encoded[8] | (uint32_t)encoded[9] <<
                8 |
                (uint32_t)encoded[10] << 16 | (uint32_t)encoded[11] << 24;
            rc = wl_columnar_expr_controlled_tail(control); if (rc) return rc;
        }
        break;
    }
    case WL_PLAN_EXPR_ARITH_HMAC_SHA256: case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS:
    case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI: case
        WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS:
        if (classify) {
            rc = wl_columnar_expr_hmac_eligible(control);
            if (rc) return rc;
        }
        break;
    case WL_PLAN_EXPR_ARITH_UUID4:
    case WL_PLAN_EXPR_STR_FN_UUID5_RFC:
    case WL_PLAN_EXPR_ARITH_UUID5: case WL_PLAN_EXPR_ARITH_UUID5_SS:
    case WL_PLAN_EXPR_ARITH_UUID5_SI: case WL_PLAN_EXPR_ARITH_UUID5_IS:
    case WL_PLAN_EXPR_ARITH_MD5: case WL_PLAN_EXPR_ARITH_MD5_S:
    case WL_PLAN_EXPR_ARITH_SHA1: case WL_PLAN_EXPR_ARITH_SHA1_S:
    case WL_PLAN_EXPR_ARITH_SHA256: case WL_PLAN_EXPR_ARITH_SHA256_S:
    case WL_PLAN_EXPR_ARITH_SHA512: case WL_PLAN_EXPR_ARITH_SHA512_S:
    case WL_PLAN_EXPR_ARITH_HASH: case WL_PLAN_EXPR_ARITH_HASH_S:
    case WL_PLAN_EXPR_ARITH_CRC32_ETH: case WL_PLAN_EXPR_ARITH_CRC32_ETH_S:
    case WL_PLAN_EXPR_ARITH_CRC32_CAST: case WL_PLAN_EXPR_ARITH_CRC32_CAST_S:
    case WL_PLAN_EXPR_STR_FN_CAT: case WL_PLAN_EXPR_STR_FN_SUBSTR:
    case WL_PLAN_EXPR_STR_FN_STRLEN: case WL_PLAN_EXPR_STR_FN_CONTAINS:
    case WL_PLAN_EXPR_STR_FN_STR_PREFIX: case WL_PLAN_EXPR_STR_FN_STR_SUFFIX:
    case WL_PLAN_EXPR_STR_FN_STR_ORD: case WL_PLAN_EXPR_STR_FN_TO_UPPER:
    case WL_PLAN_EXPR_STR_FN_TO_LOWER: case WL_PLAN_EXPR_STR_FN_STR_REPLACE:
    case WL_PLAN_EXPR_STR_FN_TRIM: case WL_PLAN_EXPR_STR_FN_TO_STRING:
    case WL_PLAN_EXPR_STR_FN_TO_NUMBER:
        break;
    case WL_PLAN_EXPR_CONST_INT: case WL_PLAN_EXPR_CONST_FLOAT: {
        uint8_t encoded[8];
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, encoded, 8,
                classify ? 6 : 7, control);
        if (rc) break;
        rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        memcpy(&in->larg, encoded, 8);
        break;
    }
    case WL_PLAN_EXPR_BOOL: {
        uint8_t v;
        rc = wl_columnar_expr_controlled_bytes(buf, size, pos, &v, 1,
                classify ? 6 : 7, control);
        if (!rc) in->larg = v ? 1 : 0;
        break;
    }
    case WL_PLAN_EXPR_ARITH_ADD: case WL_PLAN_EXPR_ARITH_SUB:
    case WL_PLAN_EXPR_ARITH_MUL: case WL_PLAN_EXPR_ARITH_DIV:
    case WL_PLAN_EXPR_ARITH_MOD: case WL_PLAN_EXPR_ARITH_SHL:
    case WL_PLAN_EXPR_ARITH_SHR: case WL_PLAN_EXPR_ARITH_BAND:
    case WL_PLAN_EXPR_ARITH_BOR: case WL_PLAN_EXPR_ARITH_BXOR:
    case WL_PLAN_EXPR_ARITH_BNOT:
    case WL_PLAN_EXPR_CMP_EQ: case WL_PLAN_EXPR_CMP_NEQ:
    case WL_PLAN_EXPR_CMP_LT: case WL_PLAN_EXPR_CMP_LTE:
    case WL_PLAN_EXPR_CMP_GT: case WL_PLAN_EXPR_CMP_GTE:
    case WL_PLAN_EXPR_ARITH_FLOAT_ADD: case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
    case WL_PLAN_EXPR_ARITH_FLOAT_MUL: case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
    case WL_PLAN_EXPR_CMP_FLOAT_EQ: case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
    case WL_PLAN_EXPR_CMP_FLOAT_LT: case WL_PLAN_EXPR_CMP_FLOAT_LTE:
    case WL_PLAN_EXPR_CMP_FLOAT_GT: case WL_PLAN_EXPR_CMP_FLOAT_GTE:
    case WL_PLAN_EXPR_CMP_STR_EQ: case WL_PLAN_EXPR_CMP_STR_NEQ:
    case WL_PLAN_EXPR_CMP_STR_LT: case WL_PLAN_EXPR_CMP_STR_LTE:
    case WL_PLAN_EXPR_CMP_STR_GT: case WL_PLAN_EXPR_CMP_STR_GTE:
    case WL_PLAN_EXPR_AGG_COUNT: case WL_PLAN_EXPR_AGG_SUM:
    case WL_PLAN_EXPR_AGG_MIN: case WL_PLAN_EXPR_AGG_MAX:
        break;
    default:
        *excluded = in->op == WL_PLAN_EXPR_CONST_STR
            || in->op == WL_PLAN_EXPR_VAR_STRING
            || (in->op >= WL_PLAN_EXPR_ARITH_HASH &&
            in->op <= WL_PLAN_EXPR_ARITH_SHA512)
            || (in->op >= WL_PLAN_EXPR_ARITH_HMAC_SHA256 &&
            in->op <= WL_PLAN_EXPR_ARITH_UUID5)
            || (in->op >= WL_PLAN_EXPR_STR_FN_STRLEN &&
            in->op <= WL_PLAN_EXPR_STR_FN_UUID5_RFC)
            || (in->op >= WL_PLAN_EXPR_CMP_STR_EQ &&
            in->op <= WL_PLAN_EXPR_CMP_STR_GTE)
            || (in->op >= WL_PLAN_EXPR_ARITH_HASH_S &&
            in->op <= WL_PLAN_EXPR_ARITH_UUID5_IS)
            || in->op == WL_PLAN_EXPR_EXTENSION_CALL ||
            in->op == WL_PLAN_EXPR_EXTENSION_CALL_ABI;
        *malformed = !*excluded;
        break;
    }
    if (rc == ENOTSUP) {
        *malformed = true; rc = 0;
    }
    if (rc) return rc;
    return wl_columnar_expr_controlled_tail(control);
}
int
wl_columnar_expr_interpreter_supported_controlled(const uint8_t *buf,
    uint32_t size, bool *supported, wl_evaluation_control_t *control)
{
    if (!supported || (size && !buf) || !control) return EINVAL;
    uint32_t pos = 0;
    while (pos < size) {
        expr_instr_t in; long column; bool malformed, excluded;
        wl_columnar_expr_extension_call_t extension;
        int rc = wl_columnar_expr_controlled_interpreter_decode(buf, size,
                &pos, &in, &column, true, &malformed, &excluded, &extension,
                control);
        if (rc) return rc;
        if (excluded) {
            *supported = false; return 0;
        }
        if (malformed) break;
    }
    *supported = true;
    return 0;
}
int
wl_columnar_expr_eval_interpreter_controlled(const uint8_t *buf,
    uint32_t size, const int64_t *row, uint32_t ncols, int64_t *value,
    bool *failed, wl_col_session_t *sess, wl_columnar_expr_status_t *status,
    bool allow_extension_scalar_result, bool allow_extension_calls,
    wl_evaluation_control_t *control)
{
    if (!value || !failed || (size && !buf) || (ncols && !row)) return EINVAL;
    if (!control) {
        wl_columnar_expr_context_t ctx = {
            .intern = sess ? sess->intern : NULL,
            .extensions = allow_extension_calls &&
                sess ? sess->base.extension_snapshot : NULL,
            .allow_extension_scalar_result = allow_extension_calls &&
                allow_extension_scalar_result,
            .configured_worker_count =
                allow_extension_calls &&
                sess ? sess->callback_configured_workers : 1,
            .active_worker_count = allow_extension_calls &&
                sess ? sess->callback_active_workers : 1,
            .parallel_execution =
                allow_extension_calls &&
                sess ? sess->callback_parallel_execution : false,
            .session_key = allow_extension_calls &&
                sess ? sess->callback_session_key : NULL
        };
        *failed = wl_columnar_expr_eval_run_ctx(buf, size, row, ncols, value,
                &ctx, status) != 0;
        return 0;
    }
    expr_stack_t stack; stack.top = 0;
    uint32_t pos = 0, instruction = 0;
    while (pos < size) {
        expr_instr_t in; long column; bool pending, excluded;
        wl_columnar_expr_extension_call_t extension;
        bool allocation = false;
        wl_columnar_expr_status_t pending_status = WL_COLUMNAR_EXPR_MALFORMED;
        int rc = wl_columnar_expr_controlled_interpreter_decode(buf, size,
                &pos, &in, &column, false, &pending, &excluded, &extension,
                control);
        if (rc) return rc;
        if (excluded) return ENOTSUP;
        if (!pending) {
            rc = wl_evaluation_control_charge(control, 32);
            if (rc) return rc;
        }
        if (in.op == WL_PLAN_EXPR_EXTENSION_CALL
            || in.op == WL_PLAN_EXPR_EXTENSION_CALL_ABI) {
            pending_status = WL_COLUMNAR_EXPR_EXTENSION_MALFORMED;
            if (!pending && !allow_extension_calls) {
                pending_status = WL_COLUMNAR_EXPR_MISSING_EXTENSION;
                pending = true;
            }
            if (!pending) {
                rc = wl_columnar_expr_controlled_extension(&stack, &extension,
                        sess, allow_extension_scalar_result, &pending_status,
                        control);
                if (rc) return rc;
                pending = pending_status != WL_COLUMNAR_EXPR_OK;
            }
        } else if (!pending &&
            (in.op == WL_PLAN_EXPR_VAR || in.op == WL_PLAN_EXPR_VAR_FLOAT
            || in.op == WL_PLAN_EXPR_VAR_STRING)){
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) return rc;
            bool ordinary_out = column < 0 || (uint32_t)column >= ncols;
            if (ordinary_out) {
                if (in.op != WL_PLAN_EXPR_VAR) pending = true;
                else expr_push(&stack, 0);
            } else if ((unsigned long)column >= ncols) pending = true;
            else {
                rc = wl_evaluation_control_charge(control, 1);
                if (rc) return rc;
                int64_t bits = row[column]; double decoded;
                if (in.op == WL_PLAN_EXPR_VAR_FLOAT && !expr_float_load(bits,
                    &decoded)) pending = true;
                else if (in.op == WL_PLAN_EXPR_VAR_STRING) {
                    rc = wl_evaluation_control_charge(control, 16);
                    if (rc) return rc;
                    const char *text = sess && sess->intern
                        ? wl_intern_reverse(sess->intern, bits) : NULL;
                    rc = wl_columnar_expr_controlled_tail(control);
                    if (rc) return rc;
                    if (!text) pending = true;
                    else {
                        size_t length;
                        rc = wl_columnar_expr_controlled_string_length(text,
                                &length, control);
                        if (rc) return rc;
                        expr_push_string(&stack, bits, (const uint8_t *)text,
                            (uint32_t)length);
                    }
                } else expr_push(&stack, bits);
            }
        } else if (!pending && (in.op == WL_PLAN_EXPR_CONST_STR
            || in.op == WL_PLAN_EXPR_STR_FN_CAT
            || in.op == WL_PLAN_EXPR_STR_FN_SUBSTR)){
            rc = wl_columnar_expr_controlled_string_produce(&stack, &in, buf,
                    sess, &allocation, control);
            if (rc) return rc;
            pending = allocation;
        } else if (!pending && (in.op == WL_PLAN_EXPR_ARITH_HMAC_SHA256
            || in.op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS
            || in.op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI
            || in.op == WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS)) {
            rc = wl_columnar_expr_controlled_hmac(&stack, in.op, sess, &pending,
                    control);
            if (rc) return rc;
        } else if (!pending && in.op == WL_PLAN_EXPR_ARITH_UUID4) {
            int64_t value;
            rc = wl_columnar_expr_uuid4_run(&value, &pending, control);
            if (rc) return rc;
            if (!pending) expr_push(&stack, value);
        } else if (!pending && in.op == WL_PLAN_EXPR_STR_FN_UUID5_RFC) {
            rc = wl_columnar_expr_controlled_uuid_rfc(&stack, sess, &pending,
                    &allocation, control);
            if (rc) return rc;
            pending = pending || allocation;
        } else if (!pending && (in.op == WL_PLAN_EXPR_ARITH_UUID5
            || in.op == WL_PLAN_EXPR_ARITH_UUID5_SS
            || in.op == WL_PLAN_EXPR_ARITH_UUID5_SI
            || in.op == WL_PLAN_EXPR_ARITH_UUID5_IS)) {
            rc = wl_columnar_expr_controlled_uuid_pair(&stack, in.op, sess,
                    &pending, control);
            if (rc) return rc;
        } else if (!pending && ((in.op >= WL_PLAN_EXPR_ARITH_MD5
            && in.op <= WL_PLAN_EXPR_ARITH_SHA512)
            || (in.op >= WL_PLAN_EXPR_ARITH_MD5_S
            && in.op <= WL_PLAN_EXPR_ARITH_SHA512_S))) {
            rc = wl_columnar_expr_controlled_digest(&stack, in.op, sess,
                    &pending, control);
            if (rc) return rc;
        } else if (!pending && (in.op == WL_PLAN_EXPR_ARITH_HASH
            || in.op == WL_PLAN_EXPR_ARITH_HASH_S)) {
            rc = wl_columnar_expr_controlled_hash(&stack, in.op, sess, &pending,
                    control);
            if (rc) return rc;
        } else if (!pending && (in.op == WL_PLAN_EXPR_ARITH_CRC32_ETH
            || in.op == WL_PLAN_EXPR_ARITH_CRC32_ETH_S
            || in.op == WL_PLAN_EXPR_ARITH_CRC32_CAST
            || in.op == WL_PLAN_EXPR_ARITH_CRC32_CAST_S)) {
            rc = wl_columnar_expr_controlled_crc(&stack, in.op, sess, control);
            if (rc) return rc;
        } else if (!pending && in.op >= WL_PLAN_EXPR_STR_FN_STRLEN
            && in.op <= WL_PLAN_EXPR_STR_FN_TO_NUMBER) {
            rc = wl_columnar_expr_controlled_builtin(&stack, in.op, sess,
                    &pending, &allocation, control);
            if (rc) return rc;
            pending = pending || allocation;
        } else if (!pending) {
            rc = wl_columnar_expr_controlled_numeric_step(&stack, &in, row,
                    ncols, true, &pending, sess ? sess->intern : NULL, control);
            if (rc) return rc;
        }
        wl_columnar_expr_controlled_checkpoint(control, 3, ++instruction,
            pending);
        rc = wl_columnar_expr_controlled_tail(control);
        if (rc) return rc;
        if (pending) {
            *failed = true; *value = 0;
            if (status) *status =
                    allocation ? WL_COLUMNAR_EXPR_ALLOCATION_FAILURE
                : pending_status;
            return 0;
        }
    }
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    int64_t pending = stack.top ? stack.vals[stack.top - 1] : 0;
    rc = wl_columnar_expr_controlled_tail(control);
    if (rc) return rc;
    *failed = false; *value = pending;
    if (status) *status = WL_COLUMNAR_EXPR_OK;
    return 0;
}

void
wl_columnar_expr_compiled_free(wl_columnar_expr_compiled_t *c)
{
    if (c) {
        if (c->reservation) {
            (void)wl_columnar_memory_rollback(c->reservation);
            free(c->reservation);
        }
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
wl_columnar_expr_compile_governed(const uint8_t *buf, uint32_t size,
    const wl_intern_t *intern, wl_columnar_memory_governor_ref_t *governor,
    wl_col_session_t *sess, int *error_out)
{
    if (error_out)
        *error_out = ENOTSUP;
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

    uint64_t instr_bytes = 0;
    uint64_t compiled_bytes = 0;
    if (!wl_columnar_memory_size_mul(ninstr, sizeof(expr_instr_t),
        &instr_bytes)
        || !wl_columnar_memory_size_add(sizeof(wl_columnar_expr_compiled_t),
        instr_bytes, &compiled_bytes)) {
        if (error_out)
            *error_out = EOVERFLOW;
        return NULL;
    }
    wl_columnar_memory_reservation_t *reservation = NULL;
    if (governor && compiled_bytes > 0) {
        wl_columnar_memory_admission_status_t status;
        reservation = (wl_columnar_memory_reservation_t *)malloc(
            sizeof(*reservation));
        if (!reservation) {
            if (error_out)
                *error_out = ENOMEM;
            return NULL;
        }
        wl_columnar_memory_reservation_init(reservation);
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(governor), compiled_bytes,
            reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
                sess->memory_budget_denied = true;
            free(reservation);
            if (error_out)
                *error_out = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                    ? ENOSPC
                    : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                    ? EOVERFLOW : ENOMEM;
            return NULL;
        }
    }
    wl_columnar_expr_compiled_t *c =
        (wl_columnar_expr_compiled_t *)malloc(
        sizeof(wl_columnar_expr_compiled_t));
    if (!c) {
        if (reservation) {
            (void)wl_columnar_memory_rollback(reservation);
            free(reservation);
        }
        if (error_out)
            *error_out = ENOMEM;
        return NULL;
    }
    c->instrs =
        (expr_instr_t *)malloc((size_t)instr_bytes);
    if (!c->instrs) {
        free(c);
        if (reservation) {
            (void)wl_columnar_memory_rollback(reservation);
            free(reservation);
        }
        if (error_out)
            *error_out = ENOMEM;
        return NULL;
    }
    c->ninstr = ninstr;
    c->intern = intern;
    c->reservation = reservation;
    if (error_out)
        *error_out = 0;

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

wl_columnar_expr_compiled_t *
wl_columnar_expr_compile(const uint8_t *buf, uint32_t size,
    const wl_intern_t *intern)
{
    return wl_columnar_expr_compile_governed(buf, size, intern, NULL, NULL,
               NULL);
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
