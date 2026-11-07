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
static bool
wl_columnar_expr_parse_var_col_tag(const uint8_t *buf, uint32_t size,
    uint32_t *pos, uint32_t *col_out, uint8_t tag)
{
    uint32_t i = *pos;
    if (i >= size || buf[i] != tag)
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

/* The raw FILTER integer matcher relies on this VAR-only contract. Numeric
 * compilation explicitly selects VAR_FLOAT in the private parser instead. */
bool
wl_columnar_expr_parse_var_col(const uint8_t *buf, uint32_t size, uint32_t *pos,
    uint32_t *col_out)
{
    return wl_columnar_expr_parse_var_col_tag(buf, size, pos, col_out,
               WL_PLAN_EXPR_VAR);
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

/* The evaluators cap pushes, zero-fill underflow, and ignore aggregate tags.
 * Track the instruction that really supplies the returned top, rather than
 * assuming the final decoded instruction supplies it. All other instructions
 * accepted by these compilers are binary pop/pop/push operations. Keep this
 * grammar in sync with both compilers when adding an opcode. No payload is
 * evaluated, and no stack-sized scratch or second traversal is required. */
static void
wl_columnar_expr_result_producer(uint8_t op, uint32_t index, uint32_t *depth,
    uint32_t *producer)
{
    switch ((wl_plan_expr_tag_t)op) {
    case WL_PLAN_EXPR_AGG_COUNT:
    case WL_PLAN_EXPR_AGG_SUM:
    case WL_PLAN_EXPR_AGG_MIN:
    case WL_PLAN_EXPR_AGG_MAX:
        return;
    case WL_PLAN_EXPR_VAR:
    case WL_PLAN_EXPR_VAR_FLOAT:
    case WL_PLAN_EXPR_CONST_INT:
    case WL_PLAN_EXPR_CONST_FLOAT:
    case WL_PLAN_EXPR_BOOL:
        break;
    case WL_PLAN_EXPR_ARITH_BNOT:
    case WL_PLAN_EXPR_ARITH_HASH:
        if (*depth) --*depth;
        break;
    default:
        *depth = *depth > 1 ? *depth - 2 : 0;
        break;
    }
    if (*depth < COL_FILTER_STACK) {
        ++*depth;
        *producer = index;
    }
}

static bool
wl_columnar_expr_result_type(const expr_instr_t *instructions,
    uint32_t producer, const wirelog_column_type_t *source_types,
    uint32_t ncols, wirelog_column_type_t *type)
{
    wirelog_column_type_t result = WIRELOG_TYPE_INT64;
    if (producer != UINT32_MAX) {
        const expr_instr_t *in = &instructions[producer];
        switch ((wl_plan_expr_tag_t)in->op) {
        case WL_PLAN_EXPR_VAR:
            if (source_types && in->iarg < ncols)
                result = source_types[in->iarg];
            break;
        case WL_PLAN_EXPR_VAR_FLOAT:
        case WL_PLAN_EXPR_CONST_FLOAT:
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
        case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
        case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
            result = WIRELOG_TYPE_FLOAT;
            break;
        case WL_PLAN_EXPR_BOOL:
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
        case WL_PLAN_EXPR_CMP_STR_EQ:
        case WL_PLAN_EXPR_CMP_STR_NEQ:
        case WL_PLAN_EXPR_CMP_STR_LT:
        case WL_PLAN_EXPR_CMP_STR_GT:
        case WL_PLAN_EXPR_CMP_STR_LTE:
        case WL_PLAN_EXPR_CMP_STR_GTE:
            result = WIRELOG_TYPE_BOOL;
            break;
        case WL_PLAN_EXPR_CONST_INT:
        case WL_PLAN_EXPR_ARITH_ADD:
        case WL_PLAN_EXPR_ARITH_SUB:
        case WL_PLAN_EXPR_ARITH_MUL:
        case WL_PLAN_EXPR_ARITH_DIV:
        case WL_PLAN_EXPR_ARITH_MOD:
        case WL_PLAN_EXPR_ARITH_BAND:
        case WL_PLAN_EXPR_ARITH_BOR:
        case WL_PLAN_EXPR_ARITH_BXOR:
        case WL_PLAN_EXPR_ARITH_SHL:
        case WL_PLAN_EXPR_ARITH_SHR:
        case WL_PLAN_EXPR_ARITH_BNOT:
        case WL_PLAN_EXPR_ARITH_CRC32_ETH:
        case WL_PLAN_EXPR_ARITH_CRC32_CAST:
        case WL_PLAN_EXPR_ARITH_HASH:
            break;
        default:
            return false;
        }
    }
    if (result < WIRELOG_TYPE_INT32 || result > WIRELOG_TYPE_BOOL)
        return false;
    *type = result;
    return true;
}

struct wl_columnar_expr_compiled {
    expr_instr_t *instrs;
    uint32_t ninstr;
    uint32_t result_producer;
    /* Issue #966: fixed for the whole expression, so it is resolved once here
     * rather than passed to the per-row evaluator.  Borrowed -- wl_plan_t
     * documents its intern as outliving the plan, which outlives any compiled
     * expression.  NULL is legal and matches the interpreter's own NULL-intern
     * fallbacks. */
    const wl_intern_t *intern;
    wl_columnar_memory_reservation_t *reservation;
};

/* Controlled numeric programs have distinct ownership from raw compiled
 * expressions. No raw destructor or evaluator accepts this representation. */
struct wl_columnar_expr_controlled {
    expr_instr_t *instrs;
    uint32_t ninstr;
    bool committed;
    wl_columnar_memory_reservation_t token;
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *, unsigned,
    uint64_t, bool);
int wl_columnar_expr_test_fail_allocation = -1;
#endif

static void
wl_columnar_expr_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_expr_test_checkpoint)
        wl_columnar_expr_test_checkpoint(control, phase, progress, failed);
#else
    (void)control;
    (void)phase;
    (void)progress;
    (void)failed;
#endif
}

static int
wl_columnar_expr_charge(wl_evaluation_control_t *control, uint64_t work)
{
    return control ? wl_evaluation_control_charge(control, work) : 0;
}

static void *
wl_columnar_expr_allocate(size_t bytes)
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

/* Work denotes admitted logical source regions, not CPU instructions/time.
 * All variable traversals are separately admitted; charge/tail plumbing and
 * mandatory physical disposal/credit retirement are excluded. Fixed bounds:
 *
 * decode64: descriptor fields3, tag/length dispatch8, bounded length copy2,
 *   prefix/length checks8, constant copy8 or BOOL normalization2, loop/return
 *   bookkeeping8 = at most45. Each byte transfer (read/write/cursor) costs1;
 *   each col digit validation+decimal accumulation costs1 separately. Neither
 *   allowance hides a variable traversal. Both passes pay the same tariff.
 * scaffold64: count/cursor/local initialization8, checked mul/add/size paths12,
 *   original token initialization10, owner fields3+embedded token init10,
 *   admission verdict/authority bookkeeping8, second-pass setup2 =53.
 *   Token init counts seven zeroed fields, identity and state stores, guard;
 *   it has fixed sizeof and no input-dependent traversal. Alloc attempts1
 *   each and second-pass instruction field stores3 are additional.
 * move64: move helper identity checks6, state checks6, payload/atomic loads
 *   and stores16, authority/commit dispatch6, final capability/output stores2
 *   =36. Governor reserve15 (plus CAS retries) and commit4 are additional.
 * eval64: instruction dispatch8, two pops6, result locals4, maximum arithmetic
 *   branch30, push sidefields10, outcome bookkeeping4 =62. The arithmetic
 *   maximum covers two FLOAT loads (8-byte copies+finite checks each), one
 *   operation and FLOAT store (finite/zero checks+8-byte copy): <=30; portable
 *   checked integer multiply incl abs/range guards is <=24. Row reads1 and
 *   instruction fetch1 are additional. No string/library traversal admitted.
 * final8: stack/result selection3, pending failure1, output stores2 =6.
 * Each finite region <=256; every region and byte/digit iteration tails.
 */
static int
wl_columnar_expr_bytes(const uint8_t *buf, uint32_t size, uint32_t *pos,
    uint8_t *out, uint32_t count, unsigned pass,
    wl_evaluation_control_t *control)
{
    if (*pos > size || count > size - *pos)
        return ENOTSUP;
    for (uint32_t i = 0; i < count; i++) {
        int rc = wl_columnar_expr_charge(control, 1);
        if (rc)
            return rc;
        out[i] = buf[(*pos)++];
        wl_columnar_expr_checkpoint(control, pass, *pos, false);
        rc = wl_columnar_expr_charge(control, 0);
        if (rc)
            return rc;
    }
    return 0;
}

static int
wl_columnar_expr_decode(const uint8_t *buf, uint32_t size, uint32_t *pos,
    expr_instr_t *out, unsigned pass, wl_evaluation_control_t *control)
{
    int rc = wl_columnar_expr_charge(control, 64);
    if (rc)
        return rc;
    *out = (expr_instr_t){0};
    rc = wl_columnar_expr_bytes(buf, size, pos, &out->op, 1, pass, control);
    if (rc)
        return rc;
    switch ((wl_plan_expr_tag_t)out->op) {
    case WL_PLAN_EXPR_VAR:
    case WL_PLAN_EXPR_VAR_FLOAT: {
        uint8_t encoded[2], name[12];
        uint16_t length;
        rc = wl_columnar_expr_bytes(buf, size, pos, encoded, 2, pass, control);
        if (rc)
            return rc;
        memcpy(&length, encoded, sizeof(length));
        if (length < 4 || length > sizeof(name))
            return ENOTSUP;
        rc = wl_columnar_expr_bytes(buf, size, pos, name, length, pass,
                control);
        if (rc)
            return rc;
        if (name[0] != 'c' || name[1] != 'o' || name[2] != 'l')
            return ENOTSUP;
        /* Exactly1..9 digits, matching the checked raw VAR parser. */
        for (uint32_t d = 3; d < length; d++) {
            rc = wl_columnar_expr_charge(control, 1);
            if (rc)
                return rc;
            bool digit = name[d] >= '0' && name[d] <= '9';
            if (digit)
                out->iarg = out->iarg * 10u + (uint32_t)(name[d] - '0');
            rc = wl_columnar_expr_charge(control, 0);
            if (rc)
                return rc;
            if (!digit)
                return ENOTSUP;
        }
        break;
    }
    case WL_PLAN_EXPR_CONST_INT:
    case WL_PLAN_EXPR_CONST_FLOAT: {
        uint8_t encoded[8];
        rc = wl_columnar_expr_bytes(buf, size, pos, encoded, 8, pass, control);
        if (rc)
            return rc;
        memcpy(&out->larg, encoded, sizeof(out->larg));
        break;
    }
    case WL_PLAN_EXPR_BOOL: {
        uint8_t value;
        rc = wl_columnar_expr_bytes(buf, size, pos, &value, 1, pass, control);
        if (rc)
            return rc;
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
    case WL_PLAN_EXPR_AGG_COUNT: case WL_PLAN_EXPR_AGG_SUM:
    case WL_PLAN_EXPR_AGG_MIN: case WL_PLAN_EXPR_AGG_MAX:
        break;
    default:
        return ENOTSUP;
    }
    return wl_columnar_expr_charge(control, 0);
}

void
wl_columnar_expr_controlled_free(wl_columnar_expr_controlled_t *owner)
{
    if (!owner)
        return;
    wl_columnar_memory_reservation_t retirement;
    wl_columnar_memory_reservation_init(&retirement);
    bool held = owner->token.bytes != 0;
    bool committed = owner->committed;
    if (held && !wl_columnar_memory_reservation_move(&retirement,
        &owner->token))
        abort();
    free(owner->instrs);
    free(owner);
    /* Mandatory cleanup: physical storage dies before its own credit. */
    wl_columnar_expr_checkpoint(NULL, 8, 0, false);
    if (held) {
        bool retired = committed ? wl_columnar_memory_release(&retirement)
            : wl_columnar_memory_rollback(&retirement);
        if (!retired)
            abort();
    }
}

int
wl_columnar_expr_compile_controlled(const uint8_t *buf, uint32_t size,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_expr_controlled_t **out, bool *supported,
    wl_evaluation_control_t *control)
{
    if (!out || !supported)
        return EINVAL;
    *out = NULL;
    *supported = false;
    if (!buf && size)
        return EINVAL;
    int rc = wl_columnar_expr_charge(control, 0);
    if (rc || !size)
        return rc;
    rc = wl_columnar_expr_charge(control, 64);
    if (rc)
        return rc;
    uint32_t count = 0, pos = 0;
    expr_instr_t instruction;
    while (pos < size) {
        rc = wl_columnar_expr_decode(buf, size, &pos, &instruction, 1, control);
        if (rc)
            return rc == ENOTSUP ? 0 : rc;
        count++;
    }
    uint64_t instr_bytes, total;
    if (!wl_columnar_memory_size_mul(count, sizeof(expr_instr_t), &instr_bytes)
        || !wl_columnar_memory_size_add(sizeof(wl_columnar_expr_controlled_t),
        instr_bytes, &total) || total > SIZE_MAX)
        return EOVERFLOW;
    wl_columnar_memory_reservation_t admission;
    wl_columnar_memory_reservation_init(&admission);
    wl_columnar_expr_controlled_t *owner = NULL;
    bool admitted = false;
    if (governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(governor), total,
            &admission, &status, control);
        if (rc)
            return rc;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        admitted = true;
    }
    wl_columnar_expr_checkpoint(control, 4, count, false);
    rc = wl_columnar_expr_charge(control, 0);
    if (rc)
        goto fail;
    rc = wl_columnar_expr_charge(control, 1);
    if (rc)
        goto fail;
    owner = wl_columnar_expr_allocate(sizeof(*owner));
    if (!owner) {
        rc = ENOMEM;
        goto fail;
    }
    owner->instrs = NULL;
    owner->ninstr = count;
    owner->committed = false;
    wl_columnar_memory_reservation_init(&owner->token);
    rc = wl_columnar_expr_charge(control, 1);
    if (rc)
        goto fail;
    owner->instrs = wl_columnar_expr_allocate((size_t)instr_bytes);
    if (!owner->instrs) {
        rc = ENOMEM;
        goto fail;
    }
    rc = wl_columnar_expr_charge(control, 0);
    if (rc)
        goto fail;
    pos = 0;
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_columnar_expr_decode(buf, size, &pos, &instruction, 2, control);
        if (rc)
            goto fail;
        rc = wl_columnar_expr_charge(control, 3);
        if (rc)
            goto fail;
        owner->instrs[i].op = instruction.op;
        owner->instrs[i].iarg = instruction.iarg;
        owner->instrs[i].larg = instruction.larg;
        rc = wl_columnar_expr_charge(control, 0);
        if (rc)
            goto fail;
    }
    rc = wl_columnar_expr_charge(control, 64);
    if (rc)
        goto fail;
    if (admitted) {
        if (!wl_columnar_memory_reservation_move(&owner->token, &admission))
            abort();
        admitted = false;
    }
    wl_columnar_expr_checkpoint(control, 5, count, false);
    rc = wl_columnar_expr_charge(control, 0);
    if (rc)
        goto fail;
    if (governor) {
        rc = wl_columnar_memory_governor_commit_controlled(&owner->token,
                owner, &owner->committed, control);
        if (rc)
            goto fail;
        if (!owner->committed) {
            rc = EINVAL;
            goto fail;
        }
    }
    wl_columnar_expr_checkpoint(control, 6, count, false);
    rc = wl_columnar_expr_charge(control, 0);
    if (rc)
        goto fail;
    *supported = true;
    *out = owner;
    return 0;
fail:
    wl_columnar_expr_controlled_free(owner);
    if (admitted && !wl_columnar_memory_rollback(&admission))
        abort();
    return rc;
}

/* Caller-attempt numeric owners deliberately have a separate representation. */
struct wl_columnar_expr_attempt_numeric {
    expr_instr_t *instrs;
    uint32_t ninstr;
    uint32_t result_producer;
    wl_columnar_memory_governor_ref_t *governor_ref;
    bool credit_held;
    bool committed;
    wl_columnar_memory_reservation_t token;
};

#ifdef WL_SESSION_TEST_HOOKS
size_t
wl_columnar_expr_test_attempt_numeric_owner_size(void)
{
    return sizeof(wl_columnar_expr_attempt_numeric_t);
}

size_t
wl_columnar_expr_test_attempt_numeric_instruction_size(void)
{
    return sizeof(expr_instr_t);
}
#endif

/* Logical source funding, including ordinary outward refusal paths.
 * G9 saved-ID guard; C13 guarded debit prefix; T26 tail/T29 with caller.
 * Reservation init10; original move40; fixed ref retain4/get3.
 * New free69 rounded72: snapshots, held/ref decisions, init/move and returns;
 * original physical/credit/ref retirement and instrumentation are excluded,
 * not the new destructor as a whole. F96 includes free72, local admission/ref
 * decisions, unsupported branch and its guarded zero-poll/return. F100 adds
 * compiler result/branch/goto; F104 adds decoder leaf result/branch/return.
 * B24 byte entry includes bounds/subtraction/loop init and next C13 prefix.
 * S32 bounds decoder last-byte length/prefix/constant/dispatch continuation;
 * mutually exclusive fixed memcpy2/8 alternatives, no hidden traversal.
 * Complete funding envelopes (source slots, not instructions or time):
 * ENTRY86; DECODE3+S32+B24+C13+F104=176;
 * BYTE3+T29+loop3+S32+B24+F104=195;
 * DIGIT9+T29+invalid3+loop3+C13+F104=161;
 * PASS T29+C13+child4+F104=150;
 * SETUP locals5+init10+math23+pin7+T29+route8+C13+F96=191;
 * RESERVE T29+child12+get3+route3+status4+marker1+verdict8+T29+
 * route8+C13+F100=210; ALLOC2+branch2+fields5+init10+verdict3+
 * T29+route8+nextsetup6+C13+F100+cache-init1=179;
 * STORE3+producer20+T29+loop3+C13+F100=168;
 * producer20 bounds switch/routing, capped depth/saturating pops, accepted
 * push/cache stores and complete helper return. No instruction scan is hidden
 * in the result query. The cached uint32 follows ninstr in both opaque owners;
 * allocation and reservation continue to use the actual sizeof on each ABI.
 * MOVE branch2+move40+failure2+markers4+T29+held2+C13+F100=192;
 * COMMIT T29+child12+route3+check6+C13+F100=163;
 * EXTERNAL T29+F100+outputs2+return1=132 (alternative outcomes).
 * Ancestor funding pays a child-return suffix once, never each child loop or
 * a refused debit. Children retain their own admitted loop work. AB-G internal
 * 168 is separated by real control polls from returned-stage84; reserve
 * continuation84+status16+tail-prefix12+F100=212. Commit final4+route6+C13+
 * F100=123; other returned errors84+4+F100=188. Conservative224 dominates all
 * these actual forward intervals; no blanket168+whole-caller sum. Root output
 * initialization/initial guard/refused-first-debit are bounded bootstrap
 * exceptions and may run with consumed0. Mandatory disposal has no control
 * poll and is not a promised latency bound.
 * Raw zero-row FILTER setup adds fixed mode/tag routing (at most12 scalar
 * steps): DECODE176+12=188 and BYTE195+12=207 remain within224. Existing
 * numeric wrappers select the unchanged opcode domain and tariffs. SIMPLE
 * recognition reuses these paid byte/digit regions for at most3 instructions;
 * its separate SETUP224 pays the fixed grammar/position decisions. Raw-only
 * comparison programs are cleanup owners, never numeric evaluator inputs. */
enum {
    WL_COLUMNAR_EXPR_ATTEMPT_ENTRY_WORK = 128,
    WL_COLUMNAR_EXPR_ATTEMPT_DECODE_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_BYTE_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_DIGIT_WORK = 192,
    WL_COLUMNAR_EXPR_ATTEMPT_PASS_WORK = 192,
    WL_COLUMNAR_EXPR_ATTEMPT_SETUP_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_RESERVE_CALL_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_ALLOC_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_STORE_WORK = 192,
    WL_COLUMNAR_EXPR_ATTEMPT_MOVE_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_COMMIT_CALL_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_EXTERNAL_WORK = 160,
};

typedef char wl_columnar_expr_attempt_all_tariffs_fit_t[
        (WL_COLUMNAR_EXPR_ATTEMPT_ENTRY_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_DECODE_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_BYTE_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_DIGIT_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_PASS_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_SETUP_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_RESERVE_CALL_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_ALLOC_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_STORE_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_MOVE_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_COMMIT_CALL_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_EXTERNAL_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

typedef char wl_columnar_expr_attempt_full_regions_funded_t[
        (UINT64_C(86) <= WL_COLUMNAR_EXPR_ATTEMPT_ENTRY_WORK
        && UINT64_C(176) <= WL_COLUMNAR_EXPR_ATTEMPT_DECODE_WORK
        && UINT64_C(195) <= WL_COLUMNAR_EXPR_ATTEMPT_BYTE_WORK
        && UINT64_C(161) <= WL_COLUMNAR_EXPR_ATTEMPT_DIGIT_WORK
        && UINT64_C(150) <= WL_COLUMNAR_EXPR_ATTEMPT_PASS_WORK
        && UINT64_C(191) <= WL_COLUMNAR_EXPR_ATTEMPT_SETUP_WORK
        && UINT64_C(210) <= WL_COLUMNAR_EXPR_ATTEMPT_RESERVE_CALL_WORK
        && UINT64_C(179) <= WL_COLUMNAR_EXPR_ATTEMPT_ALLOC_WORK
        && UINT64_C(168) <= WL_COLUMNAR_EXPR_ATTEMPT_STORE_WORK
        && UINT64_C(192) <= WL_COLUMNAR_EXPR_ATTEMPT_MOVE_WORK
        && UINT64_C(163) <= WL_COLUMNAR_EXPR_ATTEMPT_COMMIT_CALL_WORK
        && UINT64_C(132) <= WL_COLUMNAR_EXPR_ATTEMPT_EXTERNAL_WORK) ? 1 : -1];

typedef char wl_columnar_expr_attempt_contiguous_fits_t[
        (UINT64_C(224) <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

static int
wl_columnar_expr_attempt_guard(wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    return !control || !attempt_id
           || wl_evaluation_control_active_attempt_id(control) != attempt_id
        ? EINVAL : 0;
}

static int
wl_columnar_expr_attempt_charge(wl_evaluation_control_t *control,
    uint64_t attempt_id, uint64_t work)
{
    int rc = wl_columnar_expr_attempt_guard(control, attempt_id);
    return rc ? rc : wl_evaluation_control_charge_attempt(control, attempt_id,
               work);
}

static int
wl_columnar_expr_attempt_tail(wl_evaluation_control_t *control,
    uint64_t attempt_id, unsigned phase, uint64_t progress)
{
    int rc = wl_columnar_expr_attempt_guard(control, attempt_id);
    if (rc)
        return rc;
    wl_columnar_expr_checkpoint(control, phase, progress, false);
    return wl_columnar_expr_attempt_charge(control, attempt_id, 0);
}

/* #2299 immutable byte operations. Each admitted region is prepaid by224.
 * Reuse the attempt guard (G9), guarded charge prefix (C13) and guarded
 * tail (T26/T29). At most128 further source actions cover validation,
 * local loads/arithmetic, loop/caller transitions, refusal and publication:
 * 9+13+29+128=179 <=224 <=quantum256. A LOAD and STORE/hash-update have
 * separate regions; nested search charges each candidate and pair.
 * Hooks are test instrumentation, never part of the production interval.
 * Invalid bootstrap touches no source/output and has a fixed bound. */
#define WL_COLUMNAR_EXPR_BYTE_WORK UINT64_C(224)
typedef char wl_columnar_expr_byte_tariff_fits_t[
        WL_COLUMNAR_EXPR_BYTE_WORK <= WL_EVALUATION_CONTROL_QUANTUM ? 1 : -1];

static int
wl_columnar_expr_byte_region(wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    return wl_columnar_expr_attempt_charge(control, attempt_id,
               WL_COLUMNAR_EXPR_BYTE_WORK);
}

static int
wl_columnar_expr_byte_cutoff(wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    int rc = wl_columnar_expr_byte_region(control, attempt_id);
    return rc ? rc : wl_columnar_expr_attempt_tail(control, attempt_id, 48, 0);
}

int
wl_columnar_expr_cstr_length_attempt_controlled(const char *source,
    size_t *length, wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if (!source || !length)
        return EINVAL;
    int rc = wl_columnar_expr_byte_region(control, attempt_id);
    if (rc) return rc;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 40, 0);
    if (rc) return rc;
    size_t n = 0;
    for (;;) {
        rc = wl_columnar_expr_byte_region(control, attempt_id);
        if (rc) return rc;
        unsigned char byte = (unsigned char)source[n];
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 41, n);
        if (rc) return rc;
        if (!byte) break;
        if (n == SIZE_MAX) return EOVERFLOW;
        ++n;
    }
    rc = wl_columnar_expr_byte_cutoff(control, attempt_id);
    if (rc) return rc;
    *length = n;
    return 0;
}

int
wl_columnar_expr_bytes_compare_attempt_controlled(const uint8_t *left,
    const uint8_t *right, size_t length, int *ordering,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if ((!left && length) || (!right && length) || !ordering)
        return EINVAL;
    int rc = wl_columnar_expr_byte_region(control, attempt_id);
    if (rc) return rc;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 40, 0);
    if (rc) return rc;
    int result = 0;
    for (size_t i = 0; i < length; ++i) {
        rc = wl_columnar_expr_byte_region(control, attempt_id);
        if (rc) return rc;
        unsigned a = left[i], b = right[i];
        result = (a > b) - (a < b);
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 42, i);
        if (rc) return rc;
        if (result) break;
    }
    rc = wl_columnar_expr_byte_cutoff(control, attempt_id);
    if (rc) return rc;
    *ordering = result;
    return 0;
}

int
wl_columnar_expr_bytes_copy_attempt_controlled(uint8_t *destination,
    const uint8_t *source, size_t length, size_t *copied,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if ((!destination && length) || (!source && length) || !copied)
        return EINVAL;
    int rc = wl_columnar_expr_byte_region(control, attempt_id);
    if (rc) return rc;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 40, 0);
    if (rc) return rc;
    *copied = 0;
    for (size_t i = 0; i < length; ++i) {
        rc = wl_columnar_expr_byte_region(control, attempt_id);
        if (rc) return rc;
        uint8_t byte = source[i];
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 43, i);
        if (rc) return rc;
        rc = wl_columnar_expr_byte_region(control, attempt_id);
        if (rc) return rc;
        destination[i] = byte;
        *copied = i + 1;
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 44, i);
        if (rc) return rc;
    }
    return wl_columnar_expr_byte_cutoff(control, attempt_id);
}

int
wl_columnar_expr_bytes_hash_attempt_controlled(const uint8_t *source,
    size_t length, uint32_t initial, uint32_t *hash,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if ((!source && length) || !hash)
        return EINVAL;
    int rc = wl_columnar_expr_byte_region(control, attempt_id);
    if (rc) return rc;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 40, 0);
    if (rc) return rc;
    uint32_t result = initial;
    for (size_t i = 0; i < length; ++i) {
        rc = wl_columnar_expr_byte_region(control, attempt_id);
        if (rc) return rc;
        uint8_t byte = source[i];
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 43, i);
        if (rc) return rc;
        rc = wl_columnar_expr_byte_region(control, attempt_id);
        if (rc) return rc;
        result = (result ^ byte) * UINT32_C(16777619);
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 45, i);
        if (rc) return rc;
    }
    rc = wl_columnar_expr_byte_cutoff(control, attempt_id);
    if (rc) return rc;
    *hash = result;
    return 0;
}

int
wl_columnar_expr_bytes_find_attempt_controlled(const uint8_t *haystack,
    size_t haystack_length, const uint8_t *needle, size_t needle_length,
    size_t *position, bool *found, wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    if ((!haystack && haystack_length) || (!needle && needle_length)
        || !position || !found)
        return EINVAL;
    int rc = wl_columnar_expr_byte_region(control, attempt_id);
    if (rc) return rc;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 40, 0);
    if (rc) return rc;
    size_t result = 0;
    bool matched = needle_length == 0;
    if (needle_length && needle_length <= haystack_length) {
        size_t last = haystack_length - needle_length;
        for (size_t i = 0; i <= last; ++i) {
            rc = wl_columnar_expr_byte_region(control, attempt_id);
            if (rc) return rc;
            rc = wl_columnar_expr_attempt_tail(control, attempt_id, 46, i);
            if (rc) return rc;
            bool candidate = true;
            for (size_t j = 0; j < needle_length; ++j) {
                rc = wl_columnar_expr_byte_region(control, attempt_id);
                if (rc) return rc;
                bool equal = haystack[i + j] == needle[j];
                rc = wl_columnar_expr_attempt_tail(control, attempt_id, 47, j);
                if (rc) return rc;
                if (!equal) {
                    candidate = false; break;
                }
            }
            if (candidate) {
                result = i; matched = true; break;
            }
        }
    }
    rc = wl_columnar_expr_byte_cutoff(control, attempt_id);
    if (rc) return rc;
    *position = result;
    *found = matched;
    return 0;
}

static int
wl_columnar_expr_attempt_bytes(const uint8_t *buf, uint32_t size,
    uint32_t *pos, uint8_t *out, uint32_t count, unsigned pass,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if (*pos > size || count > size - *pos)
        return ENOTSUP;
    for (uint32_t i = 0; i < count; i++) {
        int rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_BYTE_WORK);
        if (rc)
            return rc;
        out[i] = buf[(*pos)++];
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, pass, *pos);
        if (rc)
            return rc;
    }
    return 0;
}

static int
wl_columnar_expr_attempt_decode(const uint8_t *buf, uint32_t size,
    uint32_t *pos,
    expr_instr_t *out, unsigned pass, wl_evaluation_control_t *control,
    uint64_t attempt_id, bool raw_empty_setup)
{
    int rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_DECODE_WORK);
    if (rc)
        return rc;
    out->op = 0; out->iarg = 0; out->larg = 0;
    rc = wl_columnar_expr_attempt_bytes(buf, size, pos, &out->op, 1, pass,
            control, attempt_id);
    if (rc)
        return rc;
    if (raw_empty_setup && (out->op == WL_PLAN_EXPR_ARITH_HASH
        || out->op == WL_PLAN_EXPR_ARITH_CRC32_ETH
        || out->op == WL_PLAN_EXPR_ARITH_CRC32_CAST)) return ENOTSUP;
    switch ((wl_plan_expr_tag_t)out->op) {
    case WL_PLAN_EXPR_VAR:
    case WL_PLAN_EXPR_VAR_FLOAT: {
        uint8_t encoded[2], name[12];
        uint16_t length;
        rc = wl_columnar_expr_attempt_bytes(buf, size, pos, encoded, 2, pass,
                control, attempt_id);
        if (rc)
            return rc;
        memcpy(&length, encoded, sizeof(length));
        if (length < 4 || length > sizeof(name))
            return ENOTSUP;
        rc = wl_columnar_expr_attempt_bytes(buf, size, pos, name, length, pass,
                control, attempt_id);
        if (rc)
            return rc;
        if (name[0] != 'c' || name[1] != 'o' || name[2] != 'l')
            return ENOTSUP;
        /* Exactly1..9 digits, matching the checked raw VAR parser. */
        for (uint32_t d = 3; d < length; d++) {
            rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_EXPR_ATTEMPT_DIGIT_WORK);
            if (rc)
                return rc;
            bool digit = name[d] >= '0' && name[d] <= '9';
            if (digit)
                out->iarg = out->iarg * 10u + (uint32_t)(name[d] - '0');
            rc = wl_columnar_expr_attempt_tail(control, attempt_id, 19, d);
            if (rc)
                return rc;
            if (!digit)
                return ENOTSUP;
        }
        break;
    }
    case WL_PLAN_EXPR_CONST_INT:
    case WL_PLAN_EXPR_CONST_FLOAT: {
        uint8_t encoded[8];
        rc = wl_columnar_expr_attempt_bytes(buf, size, pos, encoded, 8, pass,
                control, attempt_id);
        if (rc)
            return rc;
        memcpy(&out->larg, encoded, sizeof(out->larg));
        break;
    }
    case WL_PLAN_EXPR_BOOL: {
        uint8_t value;
        rc = wl_columnar_expr_attempt_bytes(buf, size, pos, &value, 1, pass,
                control, attempt_id);
        if (rc)
            return rc;
        out->larg = value ? 1 : 0;
        break;
    }
    case WL_PLAN_EXPR_CMP_STR_EQ: case WL_PLAN_EXPR_CMP_STR_NEQ:
    case WL_PLAN_EXPR_CMP_STR_LT: case WL_PLAN_EXPR_CMP_STR_GT:
    case WL_PLAN_EXPR_CMP_STR_LTE: case WL_PLAN_EXPR_CMP_STR_GTE:
        if (!raw_empty_setup) return ENOTSUP;
        break;
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
    case WL_PLAN_EXPR_ARITH_CRC32_ETH:
    case WL_PLAN_EXPR_ARITH_CRC32_CAST:
    case WL_PLAN_EXPR_ARITH_HASH:
    case WL_PLAN_EXPR_AGG_COUNT: case WL_PLAN_EXPR_AGG_SUM:
    case WL_PLAN_EXPR_AGG_MIN: case WL_PLAN_EXPR_AGG_MAX:
        break;
    default:
        return ENOTSUP;
    }
    return wl_columnar_expr_attempt_charge(control, attempt_id, 0);
}

void
wl_columnar_expr_attempt_numeric_free(wl_columnar_expr_attempt_numeric_t *owner)
{
    if (!owner)
        return;
    bool held = owner->credit_held;
    bool committed = owner->committed;
    wl_columnar_memory_governor_ref_t *saved_ref = owner->governor_ref;
    expr_instr_t *instructions = owner->instrs;
    wl_columnar_memory_reservation_t retirement;
    if (held) {
        wl_columnar_memory_reservation_init(&retirement);
        if (!wl_columnar_memory_reservation_move(&retirement, &owner->token))
            abort();
    }
    free(instructions);
    free(owner);
    wl_columnar_expr_checkpoint(NULL, 8, 0, false);
    if (held) {
        bool retired = committed ? wl_columnar_memory_release(&retirement)
            : wl_columnar_memory_rollback(&retirement);
        if (!retired)
            abort();
    }
    if (saved_ref)
        wl_columnar_memory_governor_ref_release(saved_ref);
}

static int
wl_columnar_expr_compile_attempt_mode(const uint8_t *buf, uint32_t size,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_expr_attempt_numeric_t **out, bool *supported,
    wl_evaluation_control_t *control, uint64_t attempt_id, bool raw_empty_setup)
{
    if (!out || !supported)
        return EINVAL;
    *out = NULL;
    *supported = false;
    int rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_ENTRY_WORK);
    if (rc)
        return rc;
    int verdict = !buf && size ? EINVAL : 0;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 10, size);
    if (rc > 0)
        return rc;
    if (verdict || rc || !size)
        return verdict ? verdict : rc;

    uint32_t count = 0, pos = 0;
    expr_instr_t instruction;
    while (pos < size) {
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_PASS_WORK);
        if (rc)
            return rc;
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 11, count);
        if (rc)
            return rc;
        rc = wl_columnar_expr_attempt_decode(buf, size, &pos, &instruction,
                1, control, attempt_id, raw_empty_setup);
        if (rc) {
            if (rc != ENOTSUP)
                return rc;
            rc = wl_columnar_expr_attempt_charge(control, attempt_id, 0);
            return rc;
        }
        count++;
    }
    rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_SETUP_WORK);
    if (rc)
        return rc;
    uint64_t instr_bytes = 0, total = 0;
    wl_columnar_memory_reservation_t admission;
    wl_columnar_memory_reservation_init(&admission);
    wl_columnar_expr_attempt_numeric_t *owner = NULL;
    wl_columnar_memory_governor_ref_t *retained_ref = NULL;
    bool admitted = false;
    verdict = !wl_columnar_memory_size_mul(count, sizeof(expr_instr_t),
            &instr_bytes)
        || !wl_columnar_memory_size_add(
        sizeof(wl_columnar_expr_attempt_numeric_t), instr_bytes,
        &total) || total > SIZE_MAX ? EOVERFLOW : 0;
    if (governor) {
        wl_columnar_memory_governor_ref_retain(governor);
        retained_ref = governor;
    }
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 12, count);
    if (rc > 0 || !verdict) {
        if (rc)
            goto fail;
    }
    if (verdict) {
        rc = verdict;
        goto fail;
    }
    if (governor) {
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_RESERVE_CALL_WORK);
        if (rc)
            goto fail;
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 13, count);
        if (rc)
            goto fail;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(retained_ref), total,
            &admission, &status, control, attempt_id);
        if (rc)
            goto fail;
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            admitted = true;
        verdict = admitted ? 0
            : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
            ? EOVERFLOW : EINVAL;
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 4, count);
        if (rc > 0 || !verdict) {
            if (rc)
                goto fail;
        }
        if (verdict) {
            rc = verdict;
            goto fail;
        }
    }
    rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_ALLOC_WORK);
    if (rc)
        goto fail;
    owner = wl_columnar_expr_allocate(sizeof(*owner));
    if (owner) {
        owner->instrs = NULL;
        owner->ninstr = count;
        owner->result_producer = UINT32_MAX;
        owner->governor_ref = NULL;
        owner->credit_held = false;
        owner->committed = false;
        wl_columnar_memory_reservation_init(&owner->token);
    }
    verdict = owner ? 0 : ENOMEM;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 14, count);
    if (rc > 0 || !verdict) {
        if (rc)
            goto fail;
    }
    if (verdict) {
        rc = verdict;
        goto fail;
    }
    rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_ALLOC_WORK);
    if (rc)
        goto fail;
    owner->instrs = wl_columnar_expr_allocate((size_t)instr_bytes);
    verdict = owner->instrs ? 0 : ENOMEM;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 15, count);
    if (rc > 0 || !verdict) {
        if (rc)
            goto fail;
    }
    if (verdict) {
        rc = verdict;
        goto fail;
    }
    uint32_t result_depth = 0;
    pos = 0;
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_PASS_WORK);
        if (rc)
            goto fail;
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 16, i);
        if (rc)
            goto fail;
        rc = wl_columnar_expr_attempt_decode(buf, size, &pos, &instruction,
                2, control, attempt_id, raw_empty_setup);
        if (rc)
            goto fail;
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_STORE_WORK);
        if (rc)
            goto fail;
        owner->instrs[i].op = instruction.op;
        owner->instrs[i].iarg = instruction.iarg;
        owner->instrs[i].larg = instruction.larg;
        wl_columnar_expr_result_producer(instruction.op, i, &result_depth,
            &owner->result_producer);
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 17, i);
        if (rc)
            goto fail;
    }
    rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_MOVE_WORK);
    if (rc)
        goto fail;
    if (admitted) {
        if (!wl_columnar_memory_reservation_move(&owner->token, &admission))
            abort();
        owner->credit_held = true;
        admitted = false;
        owner->governor_ref = retained_ref;
        retained_ref = NULL;
    }
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 5, count);
    if (rc)
        goto fail;
    if (owner->credit_held) {
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_COMMIT_CALL_WORK);
        if (rc)
            goto fail;
        rc = wl_columnar_expr_attempt_tail(control, attempt_id, 18, count);
        if (rc)
            goto fail;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &owner->token, owner, &owner->committed, control, attempt_id);
        if (rc)
            goto fail;
        if (!owner->committed) {
            rc = EINVAL;
            goto fail;
        }
    }
    rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_EXTERNAL_WORK);
    if (rc)
        goto fail;
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 6, count);
    if (rc)
        goto fail;
    *supported = true;
    *out = owner;
    return 0;
fail:
    wl_columnar_expr_attempt_numeric_free(owner);
    if (admitted && !wl_columnar_memory_rollback(&admission))
        abort();
    if (retained_ref)
        wl_columnar_memory_governor_ref_release(retained_ref);
    if (rc == ENOTSUP)
        return wl_columnar_expr_attempt_charge(control, attempt_id, 0);
    return rc;
}

int
wl_columnar_expr_compile_attempt_controlled(const uint8_t *buf, uint32_t size,
    wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_expr_attempt_numeric_t **out, bool *supported,
    wl_evaluation_control_t *control, uint64_t id)
{
    return wl_columnar_expr_compile_attempt_mode(buf, size, governor, out,
               supported, control, id, false);
}

/* Raw FILTER setup only, never numeric evaluation. STRING comparisons need
 * real compilation even without rows; HASH/CRC use raw interpreter fallback
 * and must not allocate a compiled program in this mode. */
int
wl_columnar_expr_compile_empty_filter_attempt_controlled(const uint8_t *buf,
    uint32_t size, wl_columnar_memory_governor_ref_t *governor,
    wl_columnar_expr_attempt_numeric_t **out, bool *supported,
    wl_evaluation_control_t *control, uint64_t id)
{
    return wl_columnar_expr_compile_attempt_mode(buf, size, governor, out,
               supported, control, id, true);
}

/* Same grammar as filter_is_simple_cmp, without allocation or row/schema
 * checks. Each of at most three decoded instructions uses the existing paid
 * byte chunks and digit regions, including malformed/non-simple inputs. */
int
wl_columnar_expr_simple_filter_attempt_controlled(const uint8_t *buf,
    uint32_t size, bool *simple, wl_evaluation_control_t *control, uint64_t id)
{
    if (!simple) return EINVAL;
    *simple = false;
    int rc = wl_columnar_expr_attempt_charge(control, id,
            WL_COLUMNAR_EXPR_ATTEMPT_ENTRY_WORK);
    if (rc) return rc;
    if (!buf || !size) return !buf && size ? EINVAL : 0;
    uint32_t pos = 0;
    expr_instr_t instruction;
    for (unsigned part = 0; part < 3; ++part) {
        rc = wl_columnar_expr_attempt_decode(buf, size, &pos, &instruction,
                1, control, id, false);
        if (rc) return rc == ENOTSUP
                ? wl_columnar_expr_attempt_charge(control, id, 0) : rc;
        rc = wl_columnar_expr_attempt_charge(control, id,
                WL_COLUMNAR_EXPR_ATTEMPT_SETUP_WORK);
        if (rc) return rc;
        bool accepted = part == 0 ? instruction.op == WL_PLAN_EXPR_VAR
            : part == 1 ? instruction.op == WL_PLAN_EXPR_VAR
            || instruction.op == WL_PLAN_EXPR_CONST_INT
            : instruction.op == WL_PLAN_EXPR_CMP_EQ
            || instruction.op == WL_PLAN_EXPR_CMP_NEQ
            || instruction.op == WL_PLAN_EXPR_CMP_LT
            || instruction.op == WL_PLAN_EXPR_CMP_GT
            || instruction.op == WL_PLAN_EXPR_CMP_LTE
            || instruction.op == WL_PLAN_EXPR_CMP_GTE;
        rc = wl_columnar_expr_attempt_tail(control, id, 24, part);
        if (rc) return rc;
        if (!accepted) return 0;
    }
    *simple = pos == size;
    return wl_columnar_expr_attempt_charge(control, id, 0);
}

/* Fixed schema lookup: the producer was resolved during paid compilation.
 * Guard/debit precedes every borrowed program/schema read; publication waits
 * for the original-ID cutoff. No values, callbacks or ownership are touched. */
int
wl_columnar_expr_result_type_attempt_controlled(
    const wl_columnar_expr_attempt_numeric_t *program,
    const wirelog_column_type_t *source_types, uint32_t ncols,
    wirelog_column_type_t *type, bool *supported,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    int rc = wl_columnar_expr_attempt_charge(control, attempt_id, 128);
    if (rc) return rc;
    if (!program || !type || !supported) return EINVAL;
    wirelog_column_type_t result = WIRELOG_TYPE_INT64;
    bool known = wl_columnar_expr_result_type(program->instrs,
            program->result_producer, source_types, ncols, &result);
    rc = wl_columnar_expr_attempt_tail(control, attempt_id, 23, ncols);
    if (rc) return rc;
    if (known) *type = result;
    *supported = known;
    return 0;
}

/* Caller-attempt numeric evaluation: conservative logical source accounting.
 * G9 and C13 are the unchanged original-ID guard/charge prefix. Failed-aware
 * tail26 + caller dispatch3 = T29; instrumentation is not a polling boundary.
 * push14 covers value/top/type/string sidefield stores; pop7 includes its
 * caller store. FLOAT load10/store16 include fixed8-byte copies, finite tests
 * and zero normalization. Portable add/sub12, mul40 (two abs5 and full signed
 * guards/unsigned threshold), div/mod12 and shift10 dominate builtin paths.
 * Routing24 + reset1 + two pop7 + init2 + switch2 + worst FLOAT arm56 +
 * failed stores2 + push guard2 + push14 + return1 = step118.
 * Returned rc3 + executed2 + T29 + failed/break2 + loop3 + C13 + refused
 * next FETCH/FINAL caller3 = suffix55. No refused region funds its return.
 * Complete regions including return/refusal alternatives:
 * ENTRY <= pointer12+C13+rc3+init4+loop1+C13+refusal3 =49 <=64,
 * FETCH <= rc3+instruction address3+C13+refusal3 =22 <=32,
 * Ordinary non-CRC STEP <= postcharge3+step118+suffix55 =176 <=192,
 * ROW <= rc3+read/store2+rowT29+FLOAT/route/push30+return1+suffix55
 *       =120 <=144 (BOTH row and later step tails included),
 * FINAL <= rc3+pending select8+T29+two outputs2+return1 =43 <=48.
 * Native-integer CRC uses a separate fixed-width lane: charge16 before the
 * pop/native copy, charge8 before initialization, eight repetitions of
 * charge17 plus one byte-table update and a saved-ID checkpoint, then final8. It
 * reads exactly sizeof(int64_t) bytes and polls between bytes; no symbol
 * lookup or input-sized fallback is admitted. Native HASH is one separate
 * fixed 8-byte XXH3 lane: charge192 before pop/read/hash. In pinned xxHash
 * 0.8.4, XXH3_64bits with len==8 follows XXH3_64bits_internal,
 * XXH3_len_0to16_64b and XXH3_len_4to8_64b on every platform: two 4-byte
 * loads, fixed secret loads, one 64x64 mix and fixed avalanche. It has no
 * input-dependent loop, allocation, CPU-specific long-input dispatch or
 * platform-specific variable traversal. The entry, precharge, pop/copy,
 * fixed hash call, result push and guarded post-hash tail fit the 192-unit
 * logical region. No HASH_S lookup or fallback is admitted.
 * STEP pays pre-ROW/refused-ROW caller work; admitted ROW pays its actual
 * lane and remaining step/eval continuation once. Initial invalid/first
 * refused read-only bootstrap <=28 can execute at consumed0. Only top and
 * used stack fields are written; no whole-array copy/initialization.
 * Conservative224 bounds actual contiguous logical forward intervals, not
 * nominal fee sums, machine instructions or elapsed time. Control internals
 * and instrumentation are excluded. This evaluator allocates and retires no
 * resources; ordinary returns/branches are included without cleanup waiver.
 */
enum {
    WL_COLUMNAR_EXPR_ATTEMPT_EVAL_ENTRY_WORK = 96,
    WL_COLUMNAR_EXPR_ATTEMPT_FETCH_WORK = 64,
    WL_COLUMNAR_EXPR_ATTEMPT_STEP_WORK = 224,
    WL_COLUMNAR_EXPR_ATTEMPT_ROW_READ_WORK = 160,
    WL_COLUMNAR_EXPR_ATTEMPT_FINAL_WORK = 96,
    WL_COLUMNAR_EXPR_ATTEMPT_HASH_WORK = 192,
    WL_COLUMNAR_EXPR_ATTEMPT_STEP_PHASE = 20,
    WL_COLUMNAR_EXPR_ATTEMPT_ROW_READ_PHASE = 21,
    WL_COLUMNAR_EXPR_ATTEMPT_FINAL_PHASE = 22,
};
typedef char wl_columnar_expr_attempt_eval_tariffs_fit_quantum[
        (WL_COLUMNAR_EXPR_ATTEMPT_EVAL_ENTRY_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_FETCH_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_STEP_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_ROW_READ_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_FINAL_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EXPR_ATTEMPT_HASH_WORK <= WL_EVALUATION_CONTROL_QUANTUM)
    ? 1 : -1];
typedef char wl_columnar_expr_attempt_eval_regions_fit_tariffs[
        (UINT64_C(64) <= WL_COLUMNAR_EXPR_ATTEMPT_EVAL_ENTRY_WORK
        && UINT64_C(32) <= WL_COLUMNAR_EXPR_ATTEMPT_FETCH_WORK
        && UINT64_C(192) <= WL_COLUMNAR_EXPR_ATTEMPT_STEP_WORK
        && UINT64_C(144) <= WL_COLUMNAR_EXPR_ATTEMPT_ROW_READ_WORK
        && UINT64_C(48) <= WL_COLUMNAR_EXPR_ATTEMPT_FINAL_WORK) ? 1 : -1];
typedef char wl_columnar_expr_attempt_eval_forward_bound[
        UINT64_C(224) <= WL_EVALUATION_CONTROL_QUANTUM ? 1 : -1];

static int
wl_columnar_expr_attempt_eval_tail(wl_evaluation_control_t *control,
    uint64_t attempt_id, unsigned phase, uint64_t progress, bool failed)
{
    int rc = wl_columnar_expr_attempt_guard(control, attempt_id);
    if (rc)
        return rc;
    wl_columnar_expr_checkpoint(control, phase, progress, failed);
    return wl_columnar_expr_attempt_charge(control, attempt_id, 0);
}

/* Eight reflected bit transitions for each possible byte. */
static const uint32_t wl_columnar_expr_crc_eth_bytes[256] = {
    UINT32_C(0x00000000), UINT32_C(0x77073096), UINT32_C(0xee0e612c),
    UINT32_C(0x990951ba), UINT32_C(0x076dc419), UINT32_C(0x706af48f),
    UINT32_C(0xe963a535), UINT32_C(0x9e6495a3), UINT32_C(0x0edb8832),
    UINT32_C(0x79dcb8a4), UINT32_C(0xe0d5e91e), UINT32_C(0x97d2d988),
    UINT32_C(0x09b64c2b), UINT32_C(0x7eb17cbd), UINT32_C(0xe7b82d07),
    UINT32_C(0x90bf1d91), UINT32_C(0x1db71064), UINT32_C(0x6ab020f2),
    UINT32_C(0xf3b97148), UINT32_C(0x84be41de), UINT32_C(0x1adad47d),
    UINT32_C(0x6ddde4eb), UINT32_C(0xf4d4b551), UINT32_C(0x83d385c7),
    UINT32_C(0x136c9856), UINT32_C(0x646ba8c0), UINT32_C(0xfd62f97a),
    UINT32_C(0x8a65c9ec), UINT32_C(0x14015c4f), UINT32_C(0x63066cd9),
    UINT32_C(0xfa0f3d63), UINT32_C(0x8d080df5), UINT32_C(0x3b6e20c8),
    UINT32_C(0x4c69105e), UINT32_C(0xd56041e4), UINT32_C(0xa2677172),
    UINT32_C(0x3c03e4d1), UINT32_C(0x4b04d447), UINT32_C(0xd20d85fd),
    UINT32_C(0xa50ab56b), UINT32_C(0x35b5a8fa), UINT32_C(0x42b2986c),
    UINT32_C(0xdbbbc9d6), UINT32_C(0xacbcf940), UINT32_C(0x32d86ce3),
    UINT32_C(0x45df5c75), UINT32_C(0xdcd60dcf), UINT32_C(0xabd13d59),
    UINT32_C(0x26d930ac), UINT32_C(0x51de003a), UINT32_C(0xc8d75180),
    UINT32_C(0xbfd06116), UINT32_C(0x21b4f4b5), UINT32_C(0x56b3c423),
    UINT32_C(0xcfba9599), UINT32_C(0xb8bda50f), UINT32_C(0x2802b89e),
    UINT32_C(0x5f058808), UINT32_C(0xc60cd9b2), UINT32_C(0xb10be924),
    UINT32_C(0x2f6f7c87), UINT32_C(0x58684c11), UINT32_C(0xc1611dab),
    UINT32_C(0xb6662d3d), UINT32_C(0x76dc4190), UINT32_C(0x01db7106),
    UINT32_C(0x98d220bc), UINT32_C(0xefd5102a), UINT32_C(0x71b18589),
    UINT32_C(0x06b6b51f), UINT32_C(0x9fbfe4a5), UINT32_C(0xe8b8d433),
    UINT32_C(0x7807c9a2), UINT32_C(0x0f00f934), UINT32_C(0x9609a88e),
    UINT32_C(0xe10e9818), UINT32_C(0x7f6a0dbb), UINT32_C(0x086d3d2d),
    UINT32_C(0x91646c97), UINT32_C(0xe6635c01), UINT32_C(0x6b6b51f4),
    UINT32_C(0x1c6c6162), UINT32_C(0x856530d8), UINT32_C(0xf262004e),
    UINT32_C(0x6c0695ed), UINT32_C(0x1b01a57b), UINT32_C(0x8208f4c1),
    UINT32_C(0xf50fc457), UINT32_C(0x65b0d9c6), UINT32_C(0x12b7e950),
    UINT32_C(0x8bbeb8ea), UINT32_C(0xfcb9887c), UINT32_C(0x62dd1ddf),
    UINT32_C(0x15da2d49), UINT32_C(0x8cd37cf3), UINT32_C(0xfbd44c65),
    UINT32_C(0x4db26158), UINT32_C(0x3ab551ce), UINT32_C(0xa3bc0074),
    UINT32_C(0xd4bb30e2), UINT32_C(0x4adfa541), UINT32_C(0x3dd895d7),
    UINT32_C(0xa4d1c46d), UINT32_C(0xd3d6f4fb), UINT32_C(0x4369e96a),
    UINT32_C(0x346ed9fc), UINT32_C(0xad678846), UINT32_C(0xda60b8d0),
    UINT32_C(0x44042d73), UINT32_C(0x33031de5), UINT32_C(0xaa0a4c5f),
    UINT32_C(0xdd0d7cc9), UINT32_C(0x5005713c), UINT32_C(0x270241aa),
    UINT32_C(0xbe0b1010), UINT32_C(0xc90c2086), UINT32_C(0x5768b525),
    UINT32_C(0x206f85b3), UINT32_C(0xb966d409), UINT32_C(0xce61e49f),
    UINT32_C(0x5edef90e), UINT32_C(0x29d9c998), UINT32_C(0xb0d09822),
    UINT32_C(0xc7d7a8b4), UINT32_C(0x59b33d17), UINT32_C(0x2eb40d81),
    UINT32_C(0xb7bd5c3b), UINT32_C(0xc0ba6cad), UINT32_C(0xedb88320),
    UINT32_C(0x9abfb3b6), UINT32_C(0x03b6e20c), UINT32_C(0x74b1d29a),
    UINT32_C(0xead54739), UINT32_C(0x9dd277af), UINT32_C(0x04db2615),
    UINT32_C(0x73dc1683), UINT32_C(0xe3630b12), UINT32_C(0x94643b84),
    UINT32_C(0x0d6d6a3e), UINT32_C(0x7a6a5aa8), UINT32_C(0xe40ecf0b),
    UINT32_C(0x9309ff9d), UINT32_C(0x0a00ae27), UINT32_C(0x7d079eb1),
    UINT32_C(0xf00f9344), UINT32_C(0x8708a3d2), UINT32_C(0x1e01f268),
    UINT32_C(0x6906c2fe), UINT32_C(0xf762575d), UINT32_C(0x806567cb),
    UINT32_C(0x196c3671), UINT32_C(0x6e6b06e7), UINT32_C(0xfed41b76),
    UINT32_C(0x89d32be0), UINT32_C(0x10da7a5a), UINT32_C(0x67dd4acc),
    UINT32_C(0xf9b9df6f), UINT32_C(0x8ebeeff9), UINT32_C(0x17b7be43),
    UINT32_C(0x60b08ed5), UINT32_C(0xd6d6a3e8), UINT32_C(0xa1d1937e),
    UINT32_C(0x38d8c2c4), UINT32_C(0x4fdff252), UINT32_C(0xd1bb67f1),
    UINT32_C(0xa6bc5767), UINT32_C(0x3fb506dd), UINT32_C(0x48b2364b),
    UINT32_C(0xd80d2bda), UINT32_C(0xaf0a1b4c), UINT32_C(0x36034af6),
    UINT32_C(0x41047a60), UINT32_C(0xdf60efc3), UINT32_C(0xa867df55),
    UINT32_C(0x316e8eef), UINT32_C(0x4669be79), UINT32_C(0xcb61b38c),
    UINT32_C(0xbc66831a), UINT32_C(0x256fd2a0), UINT32_C(0x5268e236),
    UINT32_C(0xcc0c7795), UINT32_C(0xbb0b4703), UINT32_C(0x220216b9),
    UINT32_C(0x5505262f), UINT32_C(0xc5ba3bbe), UINT32_C(0xb2bd0b28),
    UINT32_C(0x2bb45a92), UINT32_C(0x5cb36a04), UINT32_C(0xc2d7ffa7),
    UINT32_C(0xb5d0cf31), UINT32_C(0x2cd99e8b), UINT32_C(0x5bdeae1d),
    UINT32_C(0x9b64c2b0), UINT32_C(0xec63f226), UINT32_C(0x756aa39c),
    UINT32_C(0x026d930a), UINT32_C(0x9c0906a9), UINT32_C(0xeb0e363f),
    UINT32_C(0x72076785), UINT32_C(0x05005713), UINT32_C(0x95bf4a82),
    UINT32_C(0xe2b87a14), UINT32_C(0x7bb12bae), UINT32_C(0x0cb61b38),
    UINT32_C(0x92d28e9b), UINT32_C(0xe5d5be0d), UINT32_C(0x7cdcefb7),
    UINT32_C(0x0bdbdf21), UINT32_C(0x86d3d2d4), UINT32_C(0xf1d4e242),
    UINT32_C(0x68ddb3f8), UINT32_C(0x1fda836e), UINT32_C(0x81be16cd),
    UINT32_C(0xf6b9265b), UINT32_C(0x6fb077e1), UINT32_C(0x18b74777),
    UINT32_C(0x88085ae6), UINT32_C(0xff0f6a70), UINT32_C(0x66063bca),
    UINT32_C(0x11010b5c), UINT32_C(0x8f659eff), UINT32_C(0xf862ae69),
    UINT32_C(0x616bffd3), UINT32_C(0x166ccf45), UINT32_C(0xa00ae278),
    UINT32_C(0xd70dd2ee), UINT32_C(0x4e048354), UINT32_C(0x3903b3c2),
    UINT32_C(0xa7672661), UINT32_C(0xd06016f7), UINT32_C(0x4969474d),
    UINT32_C(0x3e6e77db), UINT32_C(0xaed16a4a), UINT32_C(0xd9d65adc),
    UINT32_C(0x40df0b66), UINT32_C(0x37d83bf0), UINT32_C(0xa9bcae53),
    UINT32_C(0xdebb9ec5), UINT32_C(0x47b2cf7f), UINT32_C(0x30b5ffe9),
    UINT32_C(0xbdbdf21c), UINT32_C(0xcabac28a), UINT32_C(0x53b39330),
    UINT32_C(0x24b4a3a6), UINT32_C(0xbad03605), UINT32_C(0xcdd70693),
    UINT32_C(0x54de5729), UINT32_C(0x23d967bf), UINT32_C(0xb3667a2e),
    UINT32_C(0xc4614ab8), UINT32_C(0x5d681b02), UINT32_C(0x2a6f2b94),
    UINT32_C(0xb40bbe37), UINT32_C(0xc30c8ea1), UINT32_C(0x5a05df1b),
    UINT32_C(0x2d02ef8d)
};
static const uint32_t wl_columnar_expr_crc_cast_bytes[256] = {
    UINT32_C(0x00000000), UINT32_C(0xf26b8303), UINT32_C(0xe13b70f7),
    UINT32_C(0x1350f3f4), UINT32_C(0xc79a971f), UINT32_C(0x35f1141c),
    UINT32_C(0x26a1e7e8), UINT32_C(0xd4ca64eb), UINT32_C(0x8ad958cf),
    UINT32_C(0x78b2dbcc), UINT32_C(0x6be22838), UINT32_C(0x9989ab3b),
    UINT32_C(0x4d43cfd0), UINT32_C(0xbf284cd3), UINT32_C(0xac78bf27),
    UINT32_C(0x5e133c24), UINT32_C(0x105ec76f), UINT32_C(0xe235446c),
    UINT32_C(0xf165b798), UINT32_C(0x030e349b), UINT32_C(0xd7c45070),
    UINT32_C(0x25afd373), UINT32_C(0x36ff2087), UINT32_C(0xc494a384),
    UINT32_C(0x9a879fa0), UINT32_C(0x68ec1ca3), UINT32_C(0x7bbcef57),
    UINT32_C(0x89d76c54), UINT32_C(0x5d1d08bf), UINT32_C(0xaf768bbc),
    UINT32_C(0xbc267848), UINT32_C(0x4e4dfb4b), UINT32_C(0x20bd8ede),
    UINT32_C(0xd2d60ddd), UINT32_C(0xc186fe29), UINT32_C(0x33ed7d2a),
    UINT32_C(0xe72719c1), UINT32_C(0x154c9ac2), UINT32_C(0x061c6936),
    UINT32_C(0xf477ea35), UINT32_C(0xaa64d611), UINT32_C(0x580f5512),
    UINT32_C(0x4b5fa6e6), UINT32_C(0xb93425e5), UINT32_C(0x6dfe410e),
    UINT32_C(0x9f95c20d), UINT32_C(0x8cc531f9), UINT32_C(0x7eaeb2fa),
    UINT32_C(0x30e349b1), UINT32_C(0xc288cab2), UINT32_C(0xd1d83946),
    UINT32_C(0x23b3ba45), UINT32_C(0xf779deae), UINT32_C(0x05125dad),
    UINT32_C(0x1642ae59), UINT32_C(0xe4292d5a), UINT32_C(0xba3a117e),
    UINT32_C(0x4851927d), UINT32_C(0x5b016189), UINT32_C(0xa96ae28a),
    UINT32_C(0x7da08661), UINT32_C(0x8fcb0562), UINT32_C(0x9c9bf696),
    UINT32_C(0x6ef07595), UINT32_C(0x417b1dbc), UINT32_C(0xb3109ebf),
    UINT32_C(0xa0406d4b), UINT32_C(0x522bee48), UINT32_C(0x86e18aa3),
    UINT32_C(0x748a09a0), UINT32_C(0x67dafa54), UINT32_C(0x95b17957),
    UINT32_C(0xcba24573), UINT32_C(0x39c9c670), UINT32_C(0x2a993584),
    UINT32_C(0xd8f2b687), UINT32_C(0x0c38d26c), UINT32_C(0xfe53516f),
    UINT32_C(0xed03a29b), UINT32_C(0x1f682198), UINT32_C(0x5125dad3),
    UINT32_C(0xa34e59d0), UINT32_C(0xb01eaa24), UINT32_C(0x42752927),
    UINT32_C(0x96bf4dcc), UINT32_C(0x64d4cecf), UINT32_C(0x77843d3b),
    UINT32_C(0x85efbe38), UINT32_C(0xdbfc821c), UINT32_C(0x2997011f),
    UINT32_C(0x3ac7f2eb), UINT32_C(0xc8ac71e8), UINT32_C(0x1c661503),
    UINT32_C(0xee0d9600), UINT32_C(0xfd5d65f4), UINT32_C(0x0f36e6f7),
    UINT32_C(0x61c69362), UINT32_C(0x93ad1061), UINT32_C(0x80fde395),
    UINT32_C(0x72966096), UINT32_C(0xa65c047d), UINT32_C(0x5437877e),
    UINT32_C(0x4767748a), UINT32_C(0xb50cf789), UINT32_C(0xeb1fcbad),
    UINT32_C(0x197448ae), UINT32_C(0x0a24bb5a), UINT32_C(0xf84f3859),
    UINT32_C(0x2c855cb2), UINT32_C(0xdeeedfb1), UINT32_C(0xcdbe2c45),
    UINT32_C(0x3fd5af46), UINT32_C(0x7198540d), UINT32_C(0x83f3d70e),
    UINT32_C(0x90a324fa), UINT32_C(0x62c8a7f9), UINT32_C(0xb602c312),
    UINT32_C(0x44694011), UINT32_C(0x5739b3e5), UINT32_C(0xa55230e6),
    UINT32_C(0xfb410cc2), UINT32_C(0x092a8fc1), UINT32_C(0x1a7a7c35),
    UINT32_C(0xe811ff36), UINT32_C(0x3cdb9bdd), UINT32_C(0xceb018de),
    UINT32_C(0xdde0eb2a), UINT32_C(0x2f8b6829), UINT32_C(0x82f63b78),
    UINT32_C(0x709db87b), UINT32_C(0x63cd4b8f), UINT32_C(0x91a6c88c),
    UINT32_C(0x456cac67), UINT32_C(0xb7072f64), UINT32_C(0xa457dc90),
    UINT32_C(0x563c5f93), UINT32_C(0x082f63b7), UINT32_C(0xfa44e0b4),
    UINT32_C(0xe9141340), UINT32_C(0x1b7f9043), UINT32_C(0xcfb5f4a8),
    UINT32_C(0x3dde77ab), UINT32_C(0x2e8e845f), UINT32_C(0xdce5075c),
    UINT32_C(0x92a8fc17), UINT32_C(0x60c37f14), UINT32_C(0x73938ce0),
    UINT32_C(0x81f80fe3), UINT32_C(0x55326b08), UINT32_C(0xa759e80b),
    UINT32_C(0xb4091bff), UINT32_C(0x466298fc), UINT32_C(0x1871a4d8),
    UINT32_C(0xea1a27db), UINT32_C(0xf94ad42f), UINT32_C(0x0b21572c),
    UINT32_C(0xdfeb33c7), UINT32_C(0x2d80b0c4), UINT32_C(0x3ed04330),
    UINT32_C(0xccbbc033), UINT32_C(0xa24bb5a6), UINT32_C(0x502036a5),
    UINT32_C(0x4370c551), UINT32_C(0xb11b4652), UINT32_C(0x65d122b9),
    UINT32_C(0x97baa1ba), UINT32_C(0x84ea524e), UINT32_C(0x7681d14d),
    UINT32_C(0x2892ed69), UINT32_C(0xdaf96e6a), UINT32_C(0xc9a99d9e),
    UINT32_C(0x3bc21e9d), UINT32_C(0xef087a76), UINT32_C(0x1d63f975),
    UINT32_C(0x0e330a81), UINT32_C(0xfc588982), UINT32_C(0xb21572c9),
    UINT32_C(0x407ef1ca), UINT32_C(0x532e023e), UINT32_C(0xa145813d),
    UINT32_C(0x758fe5d6), UINT32_C(0x87e466d5), UINT32_C(0x94b49521),
    UINT32_C(0x66df1622), UINT32_C(0x38cc2a06), UINT32_C(0xcaa7a905),
    UINT32_C(0xd9f75af1), UINT32_C(0x2b9cd9f2), UINT32_C(0xff56bd19),
    UINT32_C(0x0d3d3e1a), UINT32_C(0x1e6dcdee), UINT32_C(0xec064eed),
    UINT32_C(0xc38d26c4), UINT32_C(0x31e6a5c7), UINT32_C(0x22b65633),
    UINT32_C(0xd0ddd530), UINT32_C(0x0417b1db), UINT32_C(0xf67c32d8),
    UINT32_C(0xe52cc12c), UINT32_C(0x1747422f), UINT32_C(0x49547e0b),
    UINT32_C(0xbb3ffd08), UINT32_C(0xa86f0efc), UINT32_C(0x5a048dff),
    UINT32_C(0x8ecee914), UINT32_C(0x7ca56a17), UINT32_C(0x6ff599e3),
    UINT32_C(0x9d9e1ae0), UINT32_C(0xd3d3e1ab), UINT32_C(0x21b862a8),
    UINT32_C(0x32e8915c), UINT32_C(0xc083125f), UINT32_C(0x144976b4),
    UINT32_C(0xe622f5b7), UINT32_C(0xf5720643), UINT32_C(0x07198540),
    UINT32_C(0x590ab964), UINT32_C(0xab613a67), UINT32_C(0xb831c993),
    UINT32_C(0x4a5a4a90), UINT32_C(0x9e902e7b), UINT32_C(0x6cfbad78),
    UINT32_C(0x7fab5e8c), UINT32_C(0x8dc0dd8f), UINT32_C(0xe330a81a),
    UINT32_C(0x115b2b19), UINT32_C(0x020bd8ed), UINT32_C(0xf0605bee),
    UINT32_C(0x24aa3f05), UINT32_C(0xd6c1bc06), UINT32_C(0xc5914ff2),
    UINT32_C(0x37faccf1), UINT32_C(0x69e9f0d5), UINT32_C(0x9b8273d6),
    UINT32_C(0x88d28022), UINT32_C(0x7ab90321), UINT32_C(0xae7367ca),
    UINT32_C(0x5c18e4c9), UINT32_C(0x4f48173d), UINT32_C(0xbd23943e),
    UINT32_C(0xf36e6f75), UINT32_C(0x0105ec76), UINT32_C(0x12551f82),
    UINT32_C(0xe03e9c81), UINT32_C(0x34f4f86a), UINT32_C(0xc69f7b69),
    UINT32_C(0xd5cf889d), UINT32_C(0x27a40b9e), UINT32_C(0x79b737ba),
    UINT32_C(0x8bdcb4b9), UINT32_C(0x988c474d), UINT32_C(0x6ae7c44e),
    UINT32_C(0xbe2da0a5), UINT32_C(0x4c4623a6), UINT32_C(0x5f16d052),
    UINT32_C(0xad7d5351)
};
typedef char wl_columnar_expr_crc_native_bytes[
        sizeof(int64_t) == 8 ? 1 : -1];

static int
wl_columnar_expr_attempt_numeric_step(expr_stack_t *stack,
    const expr_instr_t *in, const int64_t *row, uint32_t ncols,
    bool *failed,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    int rc;
    *failed = false;
    if (in->op == WL_PLAN_EXPR_VAR || in->op == WL_PLAN_EXPR_VAR_FLOAT) {
        if (in->iarg < ncols) {
            rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_EXPR_ATTEMPT_ROW_READ_WORK);
            if (rc) return rc;
            int64_t value = row[in->iarg];
            rc = wl_columnar_expr_attempt_eval_tail(control, attempt_id,
                    WL_COLUMNAR_EXPR_ATTEMPT_ROW_READ_PHASE, in->iarg, false);
            if (rc) return rc;
            double decoded;
            if (in->op == WL_PLAN_EXPR_VAR_FLOAT && !expr_float_load(value,
                &decoded))
                *failed = true;
            else expr_push(stack, value);
        } else if (in->op == WL_PLAN_EXPR_VAR_FLOAT) *failed = true;
        else expr_push(stack, 0);
    } else if (in->op == WL_PLAN_EXPR_CONST_FLOAT) {
        double decoded;
        if (!expr_float_load(in->larg, &decoded)) *failed = true;
        else expr_push(stack, in->larg);
    } else if (in->op == WL_PLAN_EXPR_CONST_INT ||
        in->op == WL_PLAN_EXPR_BOOL) {
        expr_push(stack, in->larg);
    } else if (in->op == WL_PLAN_EXPR_ARITH_CRC32_ETH
        || in->op == WL_PLAN_EXPR_ARITH_CRC32_CAST) {
        /* Exact positive admission precedes callback-free local arithmetic.
         * The post-checkpoint exact-ID poll never bills a successor. */
#ifdef WL_SESSION_TEST_HOOKS
#define WL_COLUMNAR_EXPR_CRC_CHECKPOINT(PHASE, PROGRESS) { \
            if (wl_columnar_expr_test_checkpoint) \
            wl_columnar_expr_test_checkpoint(control, (PHASE), (PROGRESS), \
                false); \
}
#else
#define WL_COLUMNAR_EXPR_CRC_CHECKPOINT(PHASE, PROGRESS) ((void)0)
#endif
        rc = wl_evaluation_control_charge_attempt(control, attempt_id, 16);
        if (rc) return rc;
        int64_t operand = expr_pop(stack);
        uint8_t native[sizeof(operand)];
        memcpy(native, &operand, sizeof(native));
        const uint32_t *table = in->op == WL_PLAN_EXPR_ARITH_CRC32_ETH
            ? wl_columnar_expr_crc_eth_bytes
            : wl_columnar_expr_crc_cast_bytes;
        WL_COLUMNAR_EXPR_CRC_CHECKPOINT(24, 0);
        rc = wl_evaluation_control_charge_attempt(control, attempt_id, 0);
        if (rc) return rc;
        rc = wl_evaluation_control_charge_attempt(control, attempt_id, 8);
        if (rc) return rc;
        uint32_t crc = UINT32_MAX;
        WL_COLUMNAR_EXPR_CRC_CHECKPOINT(24, 0);
        rc = wl_evaluation_control_charge_attempt(control, attempt_id, 0);
        if (rc) return rc;
#define WL_COLUMNAR_EXPR_CRC_BYTE_AT(N, PROGRESS) { \
            rc = wl_evaluation_control_charge_attempt(control, attempt_id, 17); \
            if (rc) return rc; \
            crc = (crc >> 8) ^ table[(crc ^ native[(N)]) & (UINT32_C(0xff))]; \
            WL_COLUMNAR_EXPR_CRC_CHECKPOINT(25, PROGRESS); \
            rc = wl_evaluation_control_charge_attempt(control, attempt_id, 0); \
            if (rc) return rc; \
}
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(0, 1);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(1, 2);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(2, 3);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(3, 4);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(4, 5);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(5, 6);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(6, 7);
        WL_COLUMNAR_EXPR_CRC_BYTE_AT(7, 8);
#undef WL_COLUMNAR_EXPR_CRC_BYTE_AT
        rc = wl_evaluation_control_charge_attempt(control, attempt_id, 8);
        if (rc) return rc;
        int64_t result = (int64_t)(crc ^ UINT32_MAX);
        WL_COLUMNAR_EXPR_CRC_CHECKPOINT(24, 1);
        rc = wl_evaluation_control_charge_attempt(control, attempt_id, 0);
        if (rc) return rc;
#undef WL_COLUMNAR_EXPR_CRC_CHECKPOINT
        expr_push(stack, result);
    } else if (in->op == WL_PLAN_EXPR_ARITH_HASH) {
        /* XXH3's fixed eight-byte path is noninterruptible; reserve its full
         * source tariff before touching the operand or calling the library. */
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_HASH_WORK);
        if (rc) return rc;
        int64_t operand = expr_pop(stack);
        uint8_t native[sizeof(operand)];
        memcpy(native, &operand, sizeof(native));
        XXH64_hash_t hash = XXH3_64bits(native, sizeof(native));
        rc = wl_columnar_expr_attempt_eval_tail(control, attempt_id, 27, 1,
                false);
        if (rc) return rc;
        expr_push(stack, (int64_t)hash);
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
wl_columnar_expr_eval_attempt_controlled(
    const wl_columnar_expr_attempt_numeric_t *program,
    const int64_t *row, uint32_t ncols, int64_t *value, bool *failed,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    /* Fixed read-only invalid/first-refused admission exception. */
    if (!program || !value || !failed || (ncols && !row))
        return EINVAL;
    int rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_EVAL_ENTRY_WORK);
    if (rc)
        return rc;
    expr_stack_t stack;
    stack.top = 0;
    bool pending_failed = false;
    uint32_t executed = 0;
    for (uint32_t k = 0; k < program->ninstr; k++) {
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_FETCH_WORK);
        if (rc)
            return rc;
        const expr_instr_t *in = &program->instrs[k];
        rc = wl_columnar_expr_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_STEP_WORK);
        if (rc)
            return rc;
        rc = wl_columnar_expr_attempt_numeric_step(&stack, in, row, ncols,
                &pending_failed, control, attempt_id);
        if (rc)
            return rc;
        executed = k + 1u;
        rc = wl_columnar_expr_attempt_eval_tail(control, attempt_id,
                WL_COLUMNAR_EXPR_ATTEMPT_STEP_PHASE, executed, pending_failed);
        if (rc)
            return rc;
        if (pending_failed)
            break;
    }
    rc = wl_columnar_expr_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_FINAL_WORK);
    if (rc)
        return rc;
    int64_t pending = !pending_failed && stack.top
        ? stack.vals[stack.top - 1] : 0;
    rc = wl_columnar_expr_attempt_eval_tail(control, attempt_id,
            WL_COLUMNAR_EXPR_ATTEMPT_FINAL_PHASE, executed, pending_failed);
    if (rc)
        return rc;
    *value = pending;
    *failed = pending_failed;
    return 0;
}

static int
wl_columnar_expr_numeric_step(expr_stack_t *stack,
    const expr_instr_t *in, const int64_t *row, uint32_t ncols,
    bool *failed,
    wl_evaluation_control_t *control)
{
    int rc;
    *failed = false;
    if (in->op == WL_PLAN_EXPR_VAR || in->op == WL_PLAN_EXPR_VAR_FLOAT) {
        if (in->iarg < ncols) {
            rc = wl_columnar_expr_charge(control, 1);
            if (rc) return rc;
            int64_t value = row[in->iarg];
            double decoded;
            if (in->op == WL_PLAN_EXPR_VAR_FLOAT && !expr_float_load(value,
                &decoded))
                *failed = true;
            else expr_push(stack, value);
        } else if (in->op == WL_PLAN_EXPR_VAR_FLOAT) *failed = true;
        else expr_push(stack, 0);
    } else if (in->op == WL_PLAN_EXPR_CONST_FLOAT) {
        double decoded;
        if (!expr_float_load(in->larg, &decoded)) *failed = true;
        else expr_push(stack, in->larg);
    } else if (in->op == WL_PLAN_EXPR_CONST_INT ||
        in->op == WL_PLAN_EXPR_BOOL) {
        expr_push(stack, in->larg);
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
wl_columnar_expr_eval_controlled(const wl_columnar_expr_controlled_t *c,
    const int64_t *row, uint32_t ncols, int64_t *value, bool *failed,
    wl_evaluation_control_t *control)
{
    if (!c || !value || !failed || (ncols && !row))
        return EINVAL;
    expr_stack_t stack;
    stack.top = 0;
    bool pending_failed = false;
    for (uint32_t k = 0; k < c->ninstr; k++) {
        int rc = wl_columnar_expr_charge(control, 1);
        if (rc)
            return rc;
        const expr_instr_t *in = &c->instrs[k];
        rc = wl_columnar_expr_charge(control, 64);
        if (rc)
            return rc;
        rc = wl_columnar_expr_numeric_step(&stack, in, row, ncols,
                &pending_failed, control);
        if (rc)
            return rc;
        wl_columnar_expr_checkpoint(control, 3, k + 1u, pending_failed);
        rc = wl_columnar_expr_charge(control, 0);
        if (rc)
            return rc;
        if (pending_failed)
            break;
    }
    int rc = wl_columnar_expr_charge(control, 8);
    if (rc)
        return rc;
    int64_t pending = !pending_failed && stack.top
        ? stack.vals[stack.top - 1] : 0;
    wl_columnar_expr_checkpoint(control, 7, c->ninstr, pending_failed);
    rc = wl_columnar_expr_charge(control, 0);
    if (rc)
        return rc;
    *value = pending;
    *failed = pending_failed;
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
            if (!wl_columnar_expr_parse_var_col_tag(buf, size, &pos, &col,
                tag))
                return NULL;
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_VAR_FLOAT: {
            uint32_t pos = i;
            uint32_t col = 0;
            if (!wl_columnar_expr_parse_var_col_tag(buf, size, &pos, &col,
                tag))
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
    c->result_producer = UINT32_MAX;
    c->intern = intern;
    c->reservation = reservation;
    if (error_out)
        *error_out = 0;

    /* Pass 2: fill instruction array. */
    uint32_t j = 0;
    uint32_t result_depth = 0;
    i = 0;
    while (i < size && j < ninstr) {
        expr_instr_t *instr = &c->instrs[j++];
        instr->op = buf[i];
        instr->iarg = 0;
        instr->larg = 0;
        switch ((wl_plan_expr_tag_t)buf[i]) {
        case WL_PLAN_EXPR_VAR: {
            uint32_t pos = i;
            wl_columnar_expr_parse_var_col_tag(buf, size, &pos, &instr->iarg,
                instr->op);
            i = pos;
            break;
        }
        case WL_PLAN_EXPR_VAR_FLOAT: {
            uint32_t pos = i;
            wl_columnar_expr_parse_var_col_tag(buf, size, &pos, &instr->iarg,
                instr->op);
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
        wl_columnar_expr_result_producer(instr->op, j - 1, &result_depth,
            &c->result_producer);
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

/* Static result contracts only: bounded depth and a complete single-result
 * buffer identify the final producer without evaluation or operand storage. */
bool
wl_columnar_expr_interpreted_result_type(const uint8_t *buf, uint32_t size,
    const wirelog_column_type_t *types, uint32_t ncols,
    wirelog_column_type_t *type)
{
    if (!buf || !size || !type) return false;
    uint32_t pos = 0, depth = 0;
    wirelog_column_type_t result = WIRELOG_TYPE_INT64;
    while (pos < size) {
        uint8_t tag = buf[pos++];
        uint32_t arity = 0;
        switch ((wl_plan_expr_tag_t)tag) {
        case WL_PLAN_EXPR_VAR:
        case WL_PLAN_EXPR_VAR_FLOAT:
        case WL_PLAN_EXPR_VAR_STRING:
        case WL_PLAN_EXPR_CONST_STR: {
            if (size - pos < sizeof(uint16_t)) return false;
            uint16_t length;
            memcpy(&length, buf + pos, sizeof(length));
            pos += sizeof(length);
            if (length > size - pos) return false;
            result = WIRELOG_TYPE_STRING;
            if (tag != WL_PLAN_EXPR_CONST_STR) {
                if (length < 4 || length > 12 || memcmp(buf + pos, "col", 3))
                    return false;
                uint32_t col = 0;
                for (uint32_t j = 3; j < length; j++) {
                    unsigned digit = (unsigned)buf[pos + j] - '0';
                    if (digit > 9 || col > (UINT32_MAX - digit) / 10)
                        return false;
                    col = col * 10 + digit;
                }
                if (col >= ncols) return false;
                result = types ? types[col] : WIRELOG_TYPE_INT64;
                if (result < WIRELOG_TYPE_INT32 || result > WIRELOG_TYPE_BOOL ||
                    (tag == WL_PLAN_EXPR_VAR_STRING &&
                    result != WIRELOG_TYPE_STRING) ||
                    (tag == WL_PLAN_EXPR_VAR_FLOAT &&
                    result != WIRELOG_TYPE_FLOAT))
                    return false;
            }
            pos += length;
            break;
        }
        case WL_PLAN_EXPR_CONST_INT:
        case WL_PLAN_EXPR_CONST_FLOAT:
            if (size - pos < 8) return false;
            pos += 8;
            result = tag ==
                WL_PLAN_EXPR_CONST_FLOAT ? WIRELOG_TYPE_FLOAT :
                WIRELOG_TYPE_INT64;
            break;
        case WL_PLAN_EXPR_BOOL:
            if (pos == size) return false;
            ++pos; result = WIRELOG_TYPE_BOOL;
            break;
        case WL_PLAN_EXPR_ARITH_ADD:
        case WL_PLAN_EXPR_ARITH_SUB:
        case WL_PLAN_EXPR_ARITH_MUL:
        case WL_PLAN_EXPR_ARITH_DIV:
        case WL_PLAN_EXPR_ARITH_MOD:
        case WL_PLAN_EXPR_ARITH_BAND:
        case WL_PLAN_EXPR_ARITH_BOR:
        case WL_PLAN_EXPR_ARITH_BXOR:
        case WL_PLAN_EXPR_ARITH_SHL:
        case WL_PLAN_EXPR_ARITH_SHR:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256:
        case WL_PLAN_EXPR_ARITH_UUID5:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI:
        case WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS:
        case WL_PLAN_EXPR_ARITH_UUID5_SS:
        case WL_PLAN_EXPR_ARITH_UUID5_SI:
        case WL_PLAN_EXPR_ARITH_UUID5_IS:
            arity = 2; result = WIRELOG_TYPE_INT64; break;
        case WL_PLAN_EXPR_ARITH_BNOT:
        case WL_PLAN_EXPR_ARITH_HASH:
        case WL_PLAN_EXPR_ARITH_CRC32_ETH:
        case WL_PLAN_EXPR_ARITH_CRC32_CAST:
        case WL_PLAN_EXPR_ARITH_MD5:
        case WL_PLAN_EXPR_ARITH_SHA1:
        case WL_PLAN_EXPR_ARITH_SHA256:
        case WL_PLAN_EXPR_ARITH_SHA512:
        case WL_PLAN_EXPR_ARITH_HASH_S:
        case WL_PLAN_EXPR_ARITH_CRC32_ETH_S:
        case WL_PLAN_EXPR_ARITH_CRC32_CAST_S:
        case WL_PLAN_EXPR_ARITH_MD5_S:
        case WL_PLAN_EXPR_ARITH_SHA1_S:
        case WL_PLAN_EXPR_ARITH_SHA256_S:
        case WL_PLAN_EXPR_ARITH_SHA512_S:
        case WL_PLAN_EXPR_STR_FN_STRLEN:
        case WL_PLAN_EXPR_STR_FN_STR_ORD:
        case WL_PLAN_EXPR_STR_FN_TO_NUMBER:
            arity = 1; result = WIRELOG_TYPE_INT64; break;
        case WL_PLAN_EXPR_ARITH_UUID4:
            arity = 0; result = WIRELOG_TYPE_INT64; break;
        case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
        case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
        case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
        case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
            arity = 2; result = WIRELOG_TYPE_FLOAT; break;
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
        case WL_PLAN_EXPR_CMP_STR_EQ:
        case WL_PLAN_EXPR_CMP_STR_NEQ:
        case WL_PLAN_EXPR_CMP_STR_LT:
        case WL_PLAN_EXPR_CMP_STR_GT:
        case WL_PLAN_EXPR_CMP_STR_LTE:
        case WL_PLAN_EXPR_CMP_STR_GTE:
        case WL_PLAN_EXPR_STR_FN_CONTAINS:
        case WL_PLAN_EXPR_STR_FN_STR_PREFIX:
        case WL_PLAN_EXPR_STR_FN_STR_SUFFIX:
            arity = 2; result = WIRELOG_TYPE_BOOL; break;
        case WL_PLAN_EXPR_STR_FN_TO_UPPER:
        case WL_PLAN_EXPR_STR_FN_TO_LOWER:
        case WL_PLAN_EXPR_STR_FN_TRIM:
        case WL_PLAN_EXPR_STR_FN_TO_STRING:
            arity = 1; result = WIRELOG_TYPE_STRING; break;
        case WL_PLAN_EXPR_STR_FN_CAT:
        case WL_PLAN_EXPR_STR_FN_UUID5_RFC:
            arity = 2; result = WIRELOG_TYPE_STRING; break;
        case WL_PLAN_EXPR_STR_FN_SUBSTR:
        case WL_PLAN_EXPR_STR_FN_STR_REPLACE:
            arity = 3; result = WIRELOG_TYPE_STRING; break;
        default: return false; /* No opaque, extension or aggregate contract. */
        }
        if (depth < arity) return false;
        depth -= arity;
        if (depth == COL_FILTER_STACK) return false;
        ++depth;
    }
    if (depth != 1) return false;
    *type = result;
    return true;
}

bool
wl_columnar_expr_compiled_result_type(const wl_columnar_expr_compiled_t *c,
    const wirelog_column_type_t *source_types, uint32_t ncols,
    wirelog_column_type_t *type)
{
    return c && type && wl_columnar_expr_result_type(c->instrs,
               c->result_producer, source_types, ncols, type);
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
