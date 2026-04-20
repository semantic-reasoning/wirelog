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

#include "columnar/internal.h"
#include "columnar/lftj.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"
#include "../intern.h"
#include "../string_ops.h"

#include <xxhash.h>

#ifdef WL_MBEDTLS_ENABLED
/* Issue #162: mbedTLS 4.0+ moved hash functions to private/ */
/* Must define MBEDTLS_ALLOW_PRIVATE_ACCESS before including private headers */
#define MBEDTLS_ALLOW_PRIVATE_ACCESS
#include <mbedtls/private/md5.h>
#include <mbedtls/private/sha1.h>
#include <mbedtls/private/sha256.h>
#include <mbedtls/private/sha512.h>
#include <mbedtls/md.h>
#include <mbedtls/private/entropy.h>
#include <mbedtls/private/ctr_drbg.h>
#endif

#include <errno.h>
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
            filt_push(&s, a + b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a - b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a * b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, b != 0 ? a / b : 0);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, b != 0 ? a % b : 0);
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
            filt_push(&s, a << b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a >> b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_HASH: {
            int64_t a = filt_pop(&s);
            filt_push(&s, (int64_t)XXH3_64bits(&a, sizeof(a)));
            break;
        }

        case WL_PLAN_EXPR_ARITH_MD5: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[16];
            mbedtls_md5_context ctx;
            mbedtls_md5_init(&ctx);
            mbedtls_md5_starts(&ctx);
            mbedtls_md5_update(&ctx, (const unsigned char *)&a, sizeof(a));
            mbedtls_md5_finish(&ctx, digest);
            mbedtls_md5_free(&ctx);
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* md5 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA1: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[20];
            mbedtls_sha1_context sha1_ctx;
            mbedtls_sha1_init(&sha1_ctx);
            int ret = mbedtls_sha1_starts(&sha1_ctx);
            if (ret != 0) {
                mbedtls_sha1_free(&sha1_ctx);
                goto bad;
            }
            ret = mbedtls_sha1_update(&sha1_ctx, (const unsigned char *)&a,
                    sizeof(a));
            if (ret != 0) {
                mbedtls_sha1_free(&sha1_ctx);
                goto bad;
            }
            ret = mbedtls_sha1_finish(&sha1_ctx, digest);
            if (ret != 0) {
                mbedtls_sha1_free(&sha1_ctx);
                goto bad;
            }
            mbedtls_sha1_free(&sha1_ctx);
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* sha1 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA256: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[32];
            mbedtls_sha256_context sha256_ctx;
            mbedtls_sha256_init(&sha256_ctx);
            int ret = mbedtls_sha256_starts(&sha256_ctx, 0);
            if (ret != 0) {
                mbedtls_sha256_free(&sha256_ctx);
                goto bad;
            }
            ret = mbedtls_sha256_update(&sha256_ctx, (const unsigned char *)&a,
                    sizeof(a));
            if (ret != 0) {
                mbedtls_sha256_free(&sha256_ctx);
                goto bad;
            }
            ret = mbedtls_sha256_finish(&sha256_ctx, digest);
            if (ret != 0) {
                mbedtls_sha256_free(&sha256_ctx);
                goto bad;
            }
            mbedtls_sha256_free(&sha256_ctx);
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* sha256 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_SHA512: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t a = filt_pop(&s);
            unsigned char digest[64];
            mbedtls_sha512_context sha512_ctx;
            mbedtls_sha512_init(&sha512_ctx);
            int ret = mbedtls_sha512_starts(&sha512_ctx, 0);
            if (ret != 0) {
                mbedtls_sha512_free(&sha512_ctx);
                goto bad;
            }
            ret = mbedtls_sha512_update(&sha512_ctx, (const unsigned char *)&a,
                    sizeof(a));
            if (ret != 0) {
                mbedtls_sha512_free(&sha512_ctx);
                goto bad;
            }
            ret = mbedtls_sha512_finish(&sha512_ctx, digest);
            if (ret != 0) {
                mbedtls_sha512_free(&sha512_ctx);
                goto bad;
            }
            mbedtls_sha512_free(&sha512_ctx);
            filt_push(&s, (int64_t)XXH3_64bits(digest, sizeof(digest)));
#else
            (void)filt_pop(&s);
            goto bad; /* sha512 requires mbedTLS (-DmbedTLS=enabled or auto) */
#endif
            break;
        }

        case WL_PLAN_EXPR_ARITH_HMAC_SHA256: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t key_val = filt_pop(&s);
            int64_t msg_val = filt_pop(&s);
            unsigned char digest[32];
            mbedtls_md_hmac(mbedtls_md_info_from_type(MBEDTLS_MD_SHA256),
                (const unsigned char *)&key_val, sizeof(key_val),
                (const unsigned char *)&msg_val, sizeof(msg_val),
                digest);
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
            mbedtls_entropy_context entropy;
            mbedtls_ctr_drbg_context ctr_drbg;
            mbedtls_entropy_init(&entropy);
            mbedtls_ctr_drbg_init(&ctr_drbg);
            int ret = mbedtls_ctr_drbg_seed(&ctr_drbg, mbedtls_entropy_func,
                    &entropy, NULL, 0);
            if (ret != 0) {
                mbedtls_ctr_drbg_free(&ctr_drbg);
                mbedtls_entropy_free(&entropy);
                goto bad;
            }
            ret = mbedtls_ctr_drbg_random(&ctr_drbg, uuid, 16);
            mbedtls_ctr_drbg_free(&ctr_drbg);
            mbedtls_entropy_free(&entropy);
            if (ret != 0)
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

        case WL_PLAN_EXPR_ARITH_UUID5: {
#ifdef WL_MBEDTLS_ENABLED
            int64_t name = filt_pop(&s);
            int64_t ns = filt_pop(&s);
            unsigned char digest[20]; /* SHA-1 output */
            mbedtls_sha1_context sha1_ctx;
            mbedtls_sha1_init(&sha1_ctx);
            int ret = mbedtls_sha1_starts(&sha1_ctx);
            if (ret != 0) {
                mbedtls_sha1_free(&sha1_ctx);
                goto bad;
            }
            ret = mbedtls_sha1_update(&sha1_ctx, (const unsigned char *)&ns,
                    sizeof(ns));
            if (ret != 0) {
                mbedtls_sha1_free(&sha1_ctx);
                goto bad;
            }
            ret = mbedtls_sha1_update(&sha1_ctx, (const unsigned char *)&name,
                    sizeof(name));
            if (ret != 0) {
                mbedtls_sha1_free(&sha1_ctx);
                goto bad;
            }
            ret = mbedtls_sha1_finish(&sha1_ctx, digest);
            mbedtls_sha1_free(&sha1_ctx);
            if (ret != 0)
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
            filt_push(&s, intern ? string_ops_to_number(a, intern) : 0);
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
    return err ? 1 : (val != 0 ? 1 : 0); /* on error: pass row through */
}

/*
 * col_eval_expr_i64:
 * Evaluate the postfix expression buffer and return the computed int64 value.
 * Used by MAP operations to compute head argument expressions.
 * Returns 0 on empty expression or evaluation error.
 */
static int64_t
col_eval_expr_i64(const uint8_t *buf, uint32_t size, const int64_t *row,
    uint32_t ncols, wl_intern_t *intern)
{
    int64_t val;
    col_eval_expr_run(buf, size, row, ncols, &val, intern);
    return val;
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
            filt_push(&s, a + b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SUB: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a - b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MUL: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a * b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_DIV: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, b != 0 ? a / b : 0);
            break;
        }
        case WL_PLAN_EXPR_ARITH_MOD: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, b != 0 ? a % b : 0);
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
            filt_push(&s, a << b);
            break;
        }
        case WL_PLAN_EXPR_ARITH_SHR: {
            int64_t b = filt_pop(&s), a = filt_pop(&s);
            filt_push(&s, a >> b);
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

    if (op->delta_mode == WL_DELTA_FORCE_EMPTY) {
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
    bool use_delta = (delta && delta->nrows > 0
        && (delta->nrows < full_rel->nrows || sess->tdd_subpass_active));
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

    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        int64_t row_buf_e[COL_STACK_MAX];
        col_rel_row_copy_out(e.rel, r, row_buf_e);
        const int64_t *row = row_buf_e;
        for (uint32_t c = 0; c < pc; c++) {
            if (op->map_exprs && c < op->map_expr_count && op->map_exprs[c].data
                && op->map_exprs[c].size > 0) {
                if (ce_map && c < ce_map_count && ce_map[c]) {
                    int64_t val = 0;
                    col_eval_expr_compiled(ce_map[c], row, e.rel->ncols,
                        &val);
                    tmp[c] = val;
                } else {
                    tmp[c] = col_eval_expr_i64(op->map_exprs[c].data,
                            op->map_exprs[c].size, row,
                            e.rel->ncols, sess->intern);
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

/*
 * col_filter_simple_scalar:
 * Scalar fast-path filter: iterate rows and copy matching ones directly
 * without per-row bytecode dispatch, strtol, or stack initialization.
 *
 * Returns the number of rows written to out_data.
 * out_data must have capacity for nrows * ncols int64_t values.
 */
static uint32_t
col_filter_simple_scalar(const int64_t *data, uint32_t nrows, uint32_t ncols,
    const simple_filter_cmp_t *cmp, int64_t *out_data)
{
    size_t row_bytes = (size_t)ncols * sizeof(int64_t);
    uint32_t out_idx = 0;
    for (uint32_t r = 0; r < nrows; r++) {
        const int64_t *row = data + (size_t)r * ncols;
        if (col_filter_cmp_row(row, ncols, cmp)) {
            memcpy(out_data + (size_t)out_idx * ncols, row, row_bytes);
            out_idx++;
        }
    }
    return out_idx;
}

#ifdef __AVX2__
/*
 * col_filter_simd_avx2:
 * AVX2 fast-path filter for const-compare predicates (b_is_const == true).
 * Processes 4 rows per SIMD iteration using 256-bit integer vectors, then
 * falls back to scalar for the tail.
 *
 * For col-col comparisons (b_is_const == false) this function delegates
 * entirely to col_filter_simple_scalar.
 *
 * Returns the number of rows written to out_data.
 */
static uint32_t
col_filter_simd_avx2(const int64_t *data, uint32_t nrows, uint32_t ncols,
    const simple_filter_cmp_t *cmp, int64_t *out_data)
{
    /* Only the const-compare branch benefits from SIMD gather+compare.
     * Col-col compare requires two non-contiguous gathers; fall back. */
    if (!cmp->b_is_const)
        return col_filter_simple_scalar(data, nrows, ncols, cmp, out_data);

    __m256i const_v = _mm256_set1_epi64x(cmp->const_b);
    __m256i all_ones = _mm256_set1_epi64x(-1LL);
    uint32_t out_idx = 0;
    size_t row_bytes = (size_t)ncols * sizeof(int64_t);
    uint32_t r = 0;

    for (; r + 4 <= nrows; r += 4) {
        /* Scalar gather: load col_a value from 4 consecutive rows */
        int64_t v0 = data[(size_t)(r + 0) * ncols + cmp->col_a];
        int64_t v1 = data[(size_t)(r + 1) * ncols + cmp->col_a];
        int64_t v2 = data[(size_t)(r + 2) * ncols + cmp->col_a];
        int64_t v3 = data[(size_t)(r + 3) * ncols + cmp->col_a];
        /* _mm256_set_epi64x fills lanes in reverse: lane3,lane2,lane1,lane0 */
        __m256i vals = _mm256_set_epi64x(v3, v2, v1, v0);

        __m256i mask;
        __m256i eq = _mm256_cmpeq_epi64(vals, const_v);
        switch (cmp->cmp_op) {
        case WL_PLAN_EXPR_CMP_EQ:
            mask = eq;
            break;
        case WL_PLAN_EXPR_CMP_NEQ:
            mask = _mm256_xor_si256(eq, all_ones);
            break;
        case WL_PLAN_EXPR_CMP_GT:
            mask = _mm256_cmpgt_epi64(vals, const_v);
            break;
        case WL_PLAN_EXPR_CMP_LT:
            mask = _mm256_cmpgt_epi64(const_v, vals);
            break;
        case WL_PLAN_EXPR_CMP_GTE:
            mask = _mm256_or_si256(_mm256_cmpgt_epi64(vals, const_v), eq);
            break;
        case WL_PLAN_EXPR_CMP_LTE:
            mask = _mm256_or_si256(_mm256_cmpgt_epi64(const_v, vals), eq);
            break;
        default:
            continue;
        }

        /* movemask_epi8: 1 bit per byte, 8 bits per 64-bit lane.
         * A lane fully set means all 8 bits for that lane are 1. */
        int bits = _mm256_movemask_epi8(mask);
        for (int lane = 0; lane < 4; lane++) {
            if (((bits >> (lane * 8)) & 0xFF) == 0xFF) {
                memcpy(out_data + (size_t)out_idx * ncols,
                    data + (size_t)(r + (uint32_t)lane) * ncols, row_bytes);
                out_idx++;
            }
        }
    }

    /* Scalar tail for remaining rows */
    for (; r < nrows; r++) {
        const int64_t *row = data + (size_t)r * ncols;
        if (col_filter_cmp_row(row, ncols, cmp)) {
            memcpy(out_data + (size_t)out_idx * ncols, row, row_bytes);
            out_idx++;
        }
    }
    return out_idx;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
/*
 * col_filter_simd_neon:
 * NEON fast-path filter for const-compare predicates (b_is_const == true).
 * Processes 2 rows per SIMD iteration using 128-bit vectors, then falls
 * back to scalar for the tail.
 *
 * For col-col comparisons (b_is_const == false) this function delegates
 * to col_filter_simple_scalar.
 *
 * Returns the number of rows written to out_data.
 */
static uint32_t
col_filter_simd_neon(const int64_t *data, uint32_t nrows, uint32_t ncols,
    const simple_filter_cmp_t *cmp, int64_t *out_data)
{
    if (!cmp->b_is_const)
        return col_filter_simple_scalar(data, nrows, ncols, cmp, out_data);

    int64x2_t const_v = vdupq_n_s64(cmp->const_b);
    uint32_t out_idx = 0;
    size_t row_bytes = (size_t)ncols * sizeof(int64_t);
    uint32_t r = 0;

    for (; r + 2 <= nrows; r += 2) {
        int64_t v0 = data[(size_t)(r + 0) * ncols + cmp->col_a];
        int64_t v1 = data[(size_t)(r + 1) * ncols + cmp->col_a];
        int64x2_t vals = vcombine_s64(vcreate_s64((uint64_t)v0),
                vcreate_s64((uint64_t)v1));

        uint64x2_t pass_mask;
        switch (cmp->cmp_op) {
        case WL_PLAN_EXPR_CMP_EQ:
            pass_mask = vceqq_s64(vals, const_v);
            break;
        case WL_PLAN_EXPR_CMP_NEQ:
            pass_mask
                = (uint64x2_t)vmvnq_u8((uint8x16_t)vceqq_s64(vals, const_v));
            break;
        case WL_PLAN_EXPR_CMP_GT:
            pass_mask = (uint64x2_t)vcgtq_s64(vals, const_v);
            break;
        case WL_PLAN_EXPR_CMP_LT:
            pass_mask = (uint64x2_t)vcltq_s64(vals, const_v);
            break;
        case WL_PLAN_EXPR_CMP_GTE:
            pass_mask = (uint64x2_t)vcgeq_s64(vals, const_v);
            break;
        case WL_PLAN_EXPR_CMP_LTE:
            pass_mask = (uint64x2_t)vcleq_s64(vals, const_v);
            break;
        default:
            continue;
        }

        /* Lane 0 */
        if (vgetq_lane_u64(pass_mask, 0) != 0) {
            memcpy(out_data + (size_t)out_idx * ncols,
                data + (size_t)(r + 0) * ncols, row_bytes);
            out_idx++;
        }
        /* Lane 1 */
        if (vgetq_lane_u64(pass_mask, 1) != 0) {
            memcpy(out_data + (size_t)out_idx * ncols,
                data + (size_t)(r + 1) * ncols, row_bytes);
            out_idx++;
        }
    }

    /* Scalar tail */
    for (; r < nrows; r++) {
        const int64_t *row = data + (size_t)r * ncols;
        if (col_filter_cmp_row(row, ncols, cmp)) {
            memcpy(out_data + (size_t)out_idx * ncols, row, row_bytes);
            out_idx++;
        }
    }
    return out_idx;
}
#endif /* __ARM_NEON__ */

/* Dispatcher: Select best filter implementation at compile time. */
#ifdef __AVX2__
#define col_filter_fast col_filter_simd_avx2
#elif defined(__ARM_NEON__)
#define col_filter_fast col_filter_simd_neon
#else
#define col_filter_fast col_filter_simple_scalar
#endif

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

        /* Scan col_a (contiguous), gather passing rows into flat output */
        uint32_t nout = 0;
        for (uint32_t r = 0; r < nrows; r++) {
            int64_t a_val = col_a[r];
            int64_t b_val = cmp.b_is_const ? cmp.const_b
                : (col_b_ptr ? col_b_ptr[r] : 0);
            bool pass = false;
            switch (cmp.cmp_op) {
            case WL_PLAN_EXPR_CMP_EQ:  pass = (a_val == b_val); break;
            case WL_PLAN_EXPR_CMP_NEQ: pass = (a_val != b_val); break;
            case WL_PLAN_EXPR_CMP_LT:  pass = (a_val < b_val); break;
            case WL_PLAN_EXPR_CMP_LTE: pass = (a_val <= b_val); break;
            case WL_PLAN_EXPR_CMP_GT:  pass = (a_val > b_val); break;
            case WL_PLAN_EXPR_CMP_GTE: pass = (a_val >= b_val); break;
            default: break;
            }
            if (pass) {
                for (uint32_t c = 0; c < ncols; c++)
                    tmp[nout * ncols + c] = columns[c][r];
                nout++;
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
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        int64_t row_buf_e[COL_STACK_MAX];
        col_rel_row_copy_out(e.rel, r, row_buf_e);
        const int64_t *row = row_buf_e;
        int pass;
        if (!buf || bsz == 0) {
            pass = 1;
        } else if (ce) {
            int64_t val = 0;
            pass = (col_eval_expr_compiled(ce, row, e.rel->ncols, &val) == 0)
                       ? (val != 0 ? 1 : 0)
                       : 1; /* on error: pass row through */
        } else {
            pass = col_eval_filter_row(buf, bsz, row, e.rel->ncols,
                    sess->intern);
        }
        if (pass) {
            int rc = col_rel_append_row(out, row);
            if (rc != 0) {
                col_expr_compiled_free(ce);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return rc;
            }
        }
    }
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

static uint32_t
hash_int64_keys(const int64_t *row, const uint32_t *key_cols, uint32_t kc)
{
    uint32_t h = 2166136261u; /* FNV-1a offset basis */
    for (uint32_t i = 0; i < kc; i++) {
        uint64_t v = (uint64_t)row[key_cols[i]];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
    }
    return h;
}

#ifdef __ARM_NEON__
/* Inline scalar hash for kc < 2: avoids function-call overhead on the hot
 * single-key path (kc=1 accounts for ~98% of DOOP joins).  Identical
 * algorithm to hash_int64_keys(); kept separate so the compiler can
 * eliminate the loop entirely for kc=1 without affecting the non-inline
 * path used on non-SIMD builds. */
static inline uint32_t
hash_int64_keys_scalar_inline(const int64_t *row, const uint32_t *key_cols,
    uint32_t kc)
{
    uint32_t h = 2166136261u;
    for (uint32_t i = 0; i < kc; i++) {
        uint64_t v = (uint64_t)row[key_cols[i]];
        h ^= (uint32_t)(v & 0xffffffff);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
    }
    return h;
}

/* Inline scalar key-match for kc < 2: avoids function-call overhead and
 * correctly handles kc=0 (returns true -- cross product) via the loop. */
static inline bool
keys_match_scalar_inline(const int64_t *lrow, const uint32_t *lk,
    const int64_t *rrow, const uint32_t *rk, uint32_t kc)
{
    for (uint32_t k = 0; k < kc; k++) {
        if (lrow[lk[k]] != rrow[rk[k]])
            return false;
    }
    return true;
}
#endif /* __ARM_NEON__ */

#if !defined(__AVX2__) && !defined(__ARM_NEON__)
/* keys_match_scalar: scalar key equality check for JOIN probe.
 * Only compiled on non-AVX2, non-NEON builds where it is aliased by
 * keys_match_fast.  On AVX2/NEON builds this function is replaced entirely
 * by keys_match_avx2/keys_match_neon, so it is excluded here to avoid
 * unused-function warnings and MSVC __attribute__ compatibility issues. */
static inline bool
keys_match_scalar(const int64_t *lrow, const uint32_t *lk, const int64_t *rrow,
    const uint32_t *rk, uint32_t kc)
{
    for (uint32_t k = 0; k < kc; k++) {
        if (lrow[lk[k]] != rrow[rk[k]])
            return false;
    }
    return true;
}
#endif /* !__AVX2__ && !__ARM_NEON__ */

#ifdef __AVX2__
/*
 * hash_int64_keys_avx2:
 * AVX2-accelerated hash for int64 key columns.
 *
 * For kc >= 4, gathers key values four at a time with _mm256_set_epi64x and
 * accumulates a XOR reduction across lanes, then applies FNV-1a as a final
 * 64->32-bit mix.  Falls back to scalar FNV-1a for kc < 4 and for the tail.
 *
 * Both build and probe paths use the same dispatcher (hash_int64_keys_fast),
 * so correctness is preserved even though output values differ from the
 * scalar path for kc >= 4.
 */
static uint32_t
hash_int64_keys_avx2(const int64_t *row, const uint32_t *key_cols, uint32_t kc)
{
    if (kc < 4)
        return hash_int64_keys(row, key_cols, kc);

    __m256i acc = _mm256_setzero_si256();
    uint32_t k = 0;

    for (; k + 4 <= kc; k += 4) {
        __m256i v
            = _mm256_set_epi64x(row[key_cols[k + 3]], row[key_cols[k + 2]],
                row[key_cols[k + 1]], row[key_cols[k + 0]]);
        acc = _mm256_xor_si256(acc, v);
    }

    /* Horizontal XOR reduction: 4 x int64 lanes -> 1 uint64 */
    __m128i lo128 = _mm256_castsi256_si128(acc);
    __m128i hi128 = _mm256_extracti128_si256(acc, 1);
    __m128i xor128 = _mm_xor_si128(lo128, hi128);
    int64_t v0 = _mm_cvtsi128_si64(xor128);
    int64_t v1 = _mm_extract_epi64(xor128, 1);
    uint64_t combined = (uint64_t)v0 ^ (uint64_t)v1;

    /* Scalar tail */
    for (; k < kc; k++)
        combined ^= (uint64_t)row[key_cols[k]];

    /* FNV-1a final mix */
    uint32_t h = 2166136261u;
    h ^= (uint32_t)(combined & 0xffffffff);
    h *= 16777619u;
    h ^= (uint32_t)(combined >> 32);
    h *= 16777619u;
    return h;
}

/*
 * keys_match_avx2:
 * AVX2-accelerated key equality check for JOIN probe.
 *
 * Processes 4 key columns per SIMD iteration using _mm256_cmpeq_epi64,
 * with scalar fallback for the tail.  Returns true iff all kc keys match.
 */
static inline bool
keys_match_avx2(const int64_t *lrow, const uint32_t *lk, const int64_t *rrow,
    const uint32_t *rk, uint32_t kc)
{
    uint32_t k = 0;

    for (; k + 4 <= kc; k += 4) {
        __m256i lv = _mm256_set_epi64x(lrow[lk[k + 3]], lrow[lk[k + 2]],
                lrow[lk[k + 1]], lrow[lk[k + 0]]);
        __m256i rv = _mm256_set_epi64x(rrow[rk[k + 3]], rrow[rk[k + 2]],
                rrow[rk[k + 1]], rrow[rk[k + 0]]);
        __m256i eq = _mm256_cmpeq_epi64(lv, rv);
        /* All 4 lanes equal iff all 32 movemask bits are set (== -1 as int) */
        if (_mm256_movemask_epi8(eq) != -1)
            return false;
    }

    /* Scalar tail */
    for (; k < kc; k++) {
        if (lrow[lk[k]] != rrow[rk[k]])
            return false;
    }
    return true;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
/*
 * hash_int64_keys_neon:
 * NEON-accelerated hash for int64 key columns.
 *
 * For kc >= 2, loads key values two at a time into a 128-bit accumulator and
 * XOR-reduces across lanes, then applies FNV-1a as a final 64->32-bit mix.
 * Falls back to scalar FNV-1a for kc < 2 and for the tail.
 *
 * Both build and probe paths use the same dispatcher (hash_int64_keys_fast),
 * so correctness is preserved even though output values differ from the
 * scalar path for kc >= 2.
 */
static inline uint32_t
hash_int64_keys_neon(const int64_t *row, const uint32_t *key_cols, uint32_t kc)
{
    if (kc < 2)
        return hash_int64_keys_scalar_inline(row, key_cols, kc);

    int64x2_t acc = vdupq_n_s64(0);
    uint32_t k = 0;

    for (; k + 2 <= kc; k += 2) {
        int64x2_t v = vcombine_s64(vcreate_s64((uint64_t)row[key_cols[k]]),
                vcreate_s64((uint64_t)row[key_cols[k + 1]]));
        acc = veorq_s64(acc, v);
    }

    /* Horizontal XOR reduction: 2 x int64 lanes -> 1 uint64 */
    uint64_t combined
        = (uint64_t)vgetq_lane_s64(acc, 0) ^ (uint64_t)vgetq_lane_s64(acc, 1);

    /* Scalar tail */
    for (; k < kc; k++)
        combined ^= (uint64_t)row[key_cols[k]];

    /* FNV-1a final mix */
    uint32_t h = 2166136261u;
    h ^= (uint32_t)(combined & 0xffffffff);
    h *= 16777619u;
    h ^= (uint32_t)(combined >> 32);
    h *= 16777619u;
    return h;
}

/*
 * keys_match_neon:
 * NEON-accelerated key equality check for JOIN probe.
 *
 * Processes 2 key columns per SIMD iteration using vceqq_s64, with scalar
 * fallback for the tail.  Returns true iff all kc keys match.
 */
static inline bool
keys_match_neon(const int64_t *lrow, const uint32_t *lk, const int64_t *rrow,
    const uint32_t *rk, uint32_t kc)
{
    if (kc < 2)
        return keys_match_scalar_inline(lrow, lk, rrow, rk, kc);

    uint32_t k = 0;

    for (; k + 2 <= kc; k += 2) {
        int64x2_t lv = vcombine_s64(vcreate_s64((uint64_t)lrow[lk[k]]),
                vcreate_s64((uint64_t)lrow[lk[k + 1]]));
        int64x2_t rv = vcombine_s64(vcreate_s64((uint64_t)rrow[rk[k]]),
                vcreate_s64((uint64_t)rrow[rk[k + 1]]));
        /* vceqq_s64 returns all-ones lanes for equal, zero for unequal */
        uint64x2_t eq = vceqq_s64(lv, rv);
        /* Both lanes must be all-ones (UINT64_MAX) for equality */
        if (vgetq_lane_u64(eq, 0) != UINT64_MAX
            || vgetq_lane_u64(eq, 1) != UINT64_MAX)
            return false;
    }

    /* Scalar tail */
    for (; k < kc; k++) {
        if (lrow[lk[k]] != rrow[rk[k]])
            return false;
    }
    return true;
}
#endif /* __ARM_NEON__ */

/* Dispatcher: select best hash and key-match implementations at compile time.
 * Automatically uses AVX2 when available, NEON on ARM, otherwise scalar. */
#ifdef __AVX2__
#define hash_int64_keys_fast hash_int64_keys_avx2
#define keys_match_fast keys_match_avx2
#elif defined(__ARM_NEON__)
/* NEON functions now handle kc < 2 internally via scalar_inline fallback,
 * eliminating per-call-site ternary dispatch overhead (Issue #234). */
#define hash_int64_keys_fast hash_int64_keys_neon
#define keys_match_fast keys_match_neon
#else
#define hash_int64_keys_fast hash_int64_keys
#define keys_match_fast keys_match_scalar
#endif

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
    /* Fast path: simple colA CMP CONST or colA CMP colB predicate */
    simple_filter_cmp_t cmp;
    if (filter_is_simple_cmp(buf, bsz, &cmp)) {
        for (uint32_t r = 0; r < rel->nrows; r++) {
            int64_t row_buf[COL_STACK_MAX];
            col_rel_row_copy_out(rel, r, row_buf);
            if (col_filter_cmp_row(row_buf, rel->ncols, &cmp)) {
                if (col_rel_append_row(out, row_buf) != 0)
                    return ENOMEM;
            }
        }
        return 0;
    }

    /* Slow path: compile once, evaluate per row */
    col_expr_compiled_t *ce = col_expr_compile(buf, bsz);
    for (uint32_t r = 0; r < rel->nrows; r++) {
        int64_t row_buf[COL_STACK_MAX];
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
            col_expr_compiled_free(ce);
            return ENOMEM;
        }
    }
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
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$join_empty",
                left_e.rel->ncols);
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
            uint32_t ocols = left_e.rel->ncols + right->ncols;
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
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && !left_e.is_delta
        && op->right_relation) {
        /* AUTO: original heuristic */
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && rdelta->nrows > 0 && rdelta->nrows < right->nrows) {
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
    if (op->materialized) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached) {
#ifdef WL_PROFILE
            sess->profile.join_cache_hit_ns += now_ns() - _t0_join;
#endif
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
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
    }

    /* Output: all left cols + all right cols (including key duplication).
     * Downstream MAP will project the desired output columns. */
    uint32_t ocols = left->ncols + right->ncols;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$join", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    /* Attach ledger so row growth is tracked under RELATION subsystem */
    out->mem_ledger = &sess->mem_ledger;

    /* Backpressure check (Issue #224): when RELATION subsystem reaches >= 80%
     * of its budget, skip row generation and push an empty result instead of
     * risking EOVERFLOW (rc=84).  Evaluation continues with gracefully
     * degraded (incomplete) results rather than failing entirely.
     *
     * TDD workers (coordinator != NULL) skip this pre-join check (Issue #404):
     * returning empty results before row generation causes silent correctness
     * bugs — zero join output leads to premature fixed-point convergence.
     * Workers still have in-loop backpressure + join_output_limit as safety
     * nets.  Coordinator sessions retain full pre-join protection. */
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
            int64_t prow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(probe, pr, prow_buf);
            const int64_t *prow = prow_buf;
            int64_t pkey = prow[probe_kcol];
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
                /* Match: emit output in [left cols | right cols] order. */
                if (right_is_unary) {
                    /* probe=left, build=right: [prow | bkey] */
                    memcpy(tmp, prow, sizeof(int64_t) * probe->ncols);
                    tmp[probe->ncols] = bkey;
                } else {
                    /* probe=right, build=left: [bkey | prow] */
                    tmp[0] = bkey;
                    memcpy(tmp + 1, prow, sizeof(int64_t) * probe->ncols);
                }
                join_rc = col_rel_append_row(out, tmp);
                if (join_rc != 0) {
                    fprintf(stderr,
                        "ERROR: col_rel_append_row failed with rc=%d at "
                        "unary join\n",
                        join_rc);
                    break;
                }
                if ((sess->join_output_limit > 0
                    && out->nrows >= sess->join_output_limit)
                    || (out->nrows % 10000 == 0 && out->nrows > 0
                    && wl_mem_ledger_should_backpressure(
                        &sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 80))) {
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
            if (join_rc != EOVERFLOW) {
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return join_rc;
            }
            /* Warn when a TDD worker truncates: silent truncation causes
             * premature convergence and incorrect results (Issue #404). */
            if (sess->coordinator) {
                fprintf(stderr,
                    "[wirelog] WARNING: join truncated on worker %u "
                    "(limit=%llu, nrows=%u) — results may be incomplete\n",
                    sess->worker_id,
                    (unsigned long long)sess->join_output_limit,
                    out->nrows);
            }
            join_rc = 0; /* soft truncation: push partial result below */
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
                uint64_t fhash = fnv1a_hash(op->right_filter_expr.data,
                        op->right_filter_expr.size);
                arr = col_session_get_filt_arrangement(sess,
                        op->right_relation, fhash, right, rk, kc);
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
                int64_t rrow_buf[COL_STACK_MAX];
                col_rel_row_copy_out(right, rr, rrow_buf);
                const int64_t *rrow = rrow_buf;
                uint32_t h
                    = hash_int64_keys_fast(rrow, rk, kc) & (nbuckets_ep - 1);
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
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            int64_t lrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(left, lr, lrow_buf);
            const int64_t *lrow = lrow_buf;

            if (arr) {
                /* Arrangement probe: fill key_row at right-side positions. */
                for (uint32_t k = 0; k < kc; k++)
                    key_row[rk[k]] = lrow[lk[k]];
                uint32_t rr = col_arrangement_find_first(arr, right->columns,
                        right->ncols, key_row);
                while (rr != UINT32_MAX && join_rc == 0) {
                    int64_t rrow_buf[COL_STACK_MAX];
                    col_rel_row_copy_out(right, rr, rrow_buf);
                    const int64_t *rrow = rrow_buf;
                    /* Verify key match: find_next may return collision rows. */
                    if (keys_match_fast(lrow, lk, rrow, rk, kc)) {
                        memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
                        memcpy(tmp + left->ncols, rrow,
                            sizeof(int64_t) * right->ncols);
                        join_rc = col_rel_append_row(out, tmp);
                        if (join_rc != 0) {
                            fprintf(stderr,
                                "ERROR: col_rel_append_row failed in "
                                "arrangement probe with rc=%d\n",
                                join_rc);
                            break;
                        }
                        if ((sess->join_output_limit > 0
                            && out->nrows >= sess->join_output_limit)
                            || (out->nrows % 10000 == 0 && out->nrows > 0
                            && wl_mem_ledger_should_backpressure(
                                &sess->mem_ledger, WL_MEM_SUBSYS_RELATION,
                                80))) {
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
                uint32_t h
                    = hash_int64_keys_fast(lrow, lk, kc) & (nbuckets_ep - 1);
                for (uint32_t e = ht_head_ep[h]; e != 0;
                    e = ht_next_ep[e - 1]) {
                    uint32_t rr = e - 1;
                    int64_t rrow_buf[COL_STACK_MAX];
                    col_rel_row_copy_out(right, rr, rrow_buf);
                    const int64_t *rrow = rrow_buf;
                    if (!keys_match_fast(lrow, lk, rrow, rk, kc))
                        continue;
                    memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
                    memcpy(tmp + left->ncols, rrow,
                        sizeof(int64_t) * right->ncols);
                    join_rc = col_rel_append_row(out, tmp);
                    if (join_rc != 0) {
                        fprintf(stderr,
                            "ERROR: col_rel_append_row failed in ephemeral "
                            "hash probe with rc=%d\n",
                            join_rc);
                        break;
                    }
                    if ((sess->join_output_limit > 0
                        && out->nrows >= sess->join_output_limit)
                        || (out->nrows % 10000 == 0 && out->nrows > 0
                        && wl_mem_ledger_should_backpressure(
                            &sess->mem_ledger, WL_MEM_SUBSYS_RELATION,
                            80))) {
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
            if (join_rc != EOVERFLOW) {
                WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                    "Merge-join failed with rc=%d, out->nrows=%u",
                    join_rc, out->nrows);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return join_rc;
            }
            /* Warn when a TDD worker truncates: silent truncation causes
             * premature convergence and incorrect results (Issue #404). */
            if (sess->coordinator) {
                fprintf(stderr,
                    "[wirelog] WARNING: join truncated on worker %u "
                    "(limit=%llu, nrows=%u) — results may be incomplete\n",
                    sess->worker_id,
                    (unsigned long long)sess->join_output_limit,
                    out->nrows);
            }
            join_rc = 0; /* soft truncation: push partial result below */
        }
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG, "Merge-join succeeded");
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    /* Propagate delta flag: result is a delta if left was delta OR we used
     * right-delta. This ensures subsequent JOINs in the same rule plan know
     * whether to apply right-delta (they should NOT if we already used one). */
    bool result_is_delta = left_e.is_delta || used_right_delta;

    /* Populate materialization cache when hint is set.
     * Works with both stable and worker-owned relations.
     * Cache takes ownership of out; we push a borrowed reference.
     * This enables K-fusion workers to cache and reuse intermediate joins,
     * reducing redundant computation across the K worker copies. */
    if (op->materialized) {
        col_mat_cache_insert(&sess->mat_cache, left, right, out);
#ifdef WL_PROFILE
        if (out->nrows == 0)
            sess->profile.join_empty_out++;
        sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
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
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
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
        int64_t rrow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(right, rr, rrow_buf);
        const int64_t *rrow = rrow_buf;
        uint32_t h = hash_int64_keys_fast(rrow, rk, kc) & (aj_nbuckets - 1);
        aj_next[rr] = aj_head[h];
        aj_head[h] = rr + 1;
    }
    int aj_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && aj_rc == 0; lr++) {
        int64_t lrow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(left, lr, lrow_buf);
        const int64_t *lrow = lrow_buf;
        uint32_t h = hash_int64_keys_fast(lrow, lk, kc) & (aj_nbuckets - 1);
        bool found = false;
        for (uint32_t e = aj_head[h]; e != 0 && !found; e = aj_next[e - 1]) {
            uint32_t rr = e - 1;
            int64_t rrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(right, rr, rrow_buf);
            const int64_t *rrow = rrow_buf;
            if (keys_match_fast(lrow, lk, rrow, rk, kc))
                found = true;
        }
        if (!found)
            aj_rc = col_rel_append_row(out, lrow);
    }
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

/* row_cmp_fn and QSORT_R_CALL are defined in columnar/internal.h */

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

    /* Min-heap entries (stack-allocated, K <= COL_MAX_RUNS = 8) */
    typedef struct {
        uint32_t cursor;
        uint32_t end;
    } compact_he_t;

    compact_he_t heap[COL_MAX_RUNS];
    uint32_t heap_size = 0;

    for (uint32_t s = 0; s < K; s++) {
        if (seg_bounds[s] < seg_bounds[s + 1]) {
            heap[heap_size].cursor = seg_bounds[s];
            heap[heap_size].end = seg_bounds[s + 1];
            heap_size++;
        }
    }

#define COMPACT_SIFT_DOWN(start, size)                                       \
        do {                                                                     \
            uint32_t _p = (start);                                               \
            while (2 * _p + 1 < (size)) {                                        \
                uint32_t _c = 2 * _p + 1;                                        \
                if (_c + 1 < (size)                                               \
                    && col_rel_row_cmp(rel, heap[_c + 1].cursor,                 \
                    heap[_c].cursor) < 0)                                     \
                _c++;                                                         \
                if (col_rel_row_cmp(rel, heap[_p].cursor,                         \
                    heap[_c].cursor) <= 0)                                    \
                break;                                                        \
                compact_he_t _tmp = heap[_p];                                     \
                heap[_p] = heap[_c];                                              \
                heap[_c] = _tmp;                                                  \
                _p = _c;                                                          \
            }                                                                     \
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
            heap_size--;
        }
        if (heap_size > 0)
            COMPACT_SIFT_DOWN(0, heap_size);
    }

#undef COMPACT_SIFT_DOWN

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
            int64_t _drb[COL_STACK_MAX];
            int64_t *dr = nc <= COL_STACK_MAX ? _drb
                : (int64_t *)malloc((size_t)nc * sizeof(int64_t));
            for (uint32_t k = 0; k < d_unique; k++) {
                for (uint32_t c = 0; c < nc; c++)
                    dr[c] = rel->columns[c][old_nrows + k];
                col_rel_append_row(delta_out, dr);
            }
            if (dr != _drb)
                free(dr);
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
                    int64_t _drb[COL_STACK_MAX];
                    int64_t *dr = nc <= COL_STACK_MAX ? _drb
                        : (int64_t *)malloc((size_t)nc * sizeof(int64_t));
                    for (uint32_t c = 0; c < nc; c++)
                        dr[c] = rel->columns[c][old_nrows + novel_count];
                    col_rel_append_row(delta_out, dr);
                    if (dr != _drb)
                        free(dr);
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

    int64_t _delta_row_buf[COL_STACK_MAX];
    int64_t *delta_row = nc <= COL_STACK_MAX ? _delta_row_buf
        : (int64_t *)malloc((size_t)nc * sizeof(int64_t));

    /* For fallback merge, we need a single sorted prefix.
     * If multiple runs exist, compact first (#377 fix).
     * compact_runs only merges run-bounded data; delta rows at
     * [old_nrows..old_nrows+d_unique) are physically untouched.
     * Relocate them adjacent to the compacted prefix afterwards. */
    if (rel->run_count > 1) {
        uint32_t delta_phys = old_nrows; /* physical location of delta */
        int rc = col_rel_compact_runs(rel);
        if (rc != 0) {
            if (delta_row != _delta_row_buf)
                free(delta_row);
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

    if (delta_row != _delta_row_buf)
        free(delta_row);

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

    /* Helper: copy row from relation into temp buf, append to out, dedup
     * against last_row in out. Returns 0 on success, -1 on failure. */
#define MERGE_K_APPEND(rel_ptr, row_idx)                                     \
        do {                                                                     \
            int64_t _rbuf[COL_STACK_MAX];                                        \
            int64_t *_rb = nc <= COL_STACK_MAX ? _rbuf                           \
            : (int64_t *)malloc((size_t)nc * sizeof(int64_t));               \
            if (!_rb) {                                                          \
                col_rel_destroy(out);                                            \
                return NULL;                                                     \
            }                                                                    \
            col_rel_row_copy_out((rel_ptr), (row_idx), _rb);                     \
            if (last_row == NULL                                                 \
                || row_cmp_dispatch(last_row, _rb, nc) != 0) {                   \
                if (col_rel_append_row(out, _rb) != 0) {                         \
                    if (_rb != _rbuf) free(_rb);                                 \
                    col_rel_destroy(out);                                        \
                    return NULL;                                                 \
                }                                                                \
                col_rel_row_copy_out(out, out->nrows - 1, last_row_buf);         \
                last_row = last_row_buf;                                         \
            }                                                                    \
            if (_rb != _rbuf) free(_rb);                                         \
        } while (0)

    /* K=1: Copy with dedup using append (handles dynamic growth) */
    if (k == 1) {
        col_rel_t *src = relations[0];
        int64_t last_row_buf[COL_STACK_MAX];
        const int64_t *last_row = NULL;
        for (uint32_t r = 0; r < src->nrows; r++) {
            MERGE_K_APPEND(src, r);
        }
        return out;
    }

    /* K=2: Optimized 2-pointer merge using append */
    if (k == 2) {
        col_rel_t *left = relations[0];
        col_rel_t *right = relations[1];
        uint32_t li = 0, ri = 0;
        int64_t last_row_buf[COL_STACK_MAX];
        const int64_t *last_row = NULL;

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
        int64_t last_row_buf[COL_STACK_MAX];
        const int64_t *last_row = NULL;
        for (uint32_t r = 0; r < temp->nrows; r++) {
            MERGE_K_APPEND(temp, r);
        }
        col_rel_destroy(temp);
    }

#undef MERGE_K_APPEND
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
    bool skipped; /* true if skipped due to empty forced delta (#85) */
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

    uint64_t _phase_t0 = now_ns();
    col_rel_t **results = (col_rel_t **)calloc(k, sizeof(col_rel_t *));
    col_op_k_fusion_worker_t *workers = (col_op_k_fusion_worker_t *)calloc(
        k, sizeof(col_op_k_fusion_worker_t));
    /* Per-worker session wrappers: shallow copy of sess with an isolated
     * mat_cache so concurrent col_op_join calls do not race on the cache. */
    wl_col_session_t *worker_sess
        = (wl_col_session_t *)calloc(k, sizeof(wl_col_session_t));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results || !workers || !worker_sess) {
        free(results);
        free(workers);
        free(worker_sess);
        return ENOMEM;
    }

    /* Use session-level workqueue created at col_session_create (issue #99).
     * When num_workers=1 (wq==NULL), K copies are evaluated sequentially
     * below with no thread overhead. */
    wl_work_queue_t *wq = sess->wq; /* NULL when num_workers=1 */
    /* Threshold: for small K, parallel dispatch costs more than it saves.
     * Force sequential execution when K < WL_KFUSION_MIN_PARALLEL_K. */
    if (wq && k < WL_KFUSION_MIN_PARALLEL_K)
        wq = NULL;

    int rc = 0;

    /* Issue #196: Workers start with zeroed mat_cache (no shared entries).
     * All worker cache entries are worker-owned; cleanup frees all of them
     * starting from index 0, so no base_count snapshot is needed. */

    /* Initialise per-worker session wrappers and submit all K tasks in one
     * batch so workers execute in parallel. */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < k; d++) {
        /* Shallow copy shares rels[], plan, etc. (read-only during K-fusion).
         * mat_cache is zeroed below (Issue #196): workers start fresh.
         * arr_* are deep-copied (#260): each worker gets an independent
         * arrangement cache to probe without races (no shared mutable state).
         * darr_* are zeroed: workers rebuild delta arrangements per-iteration. */
        worker_sess[d] = *sess;
        worker_sess[d].wq = NULL; /* prevent nested K-fusion from workers */
        /* NULL out owned resources before allocation so cleanup_wq is safe
         * even if we abort early (e.g. clone failure).  Each owned pointer
         * is replaced below; the parent session retains its own copies. */
        worker_sess[d].eval_arena = NULL;
        worker_sess[d].delta_pool = NULL;
        /* Issue #260: Deep-copy parent's full-arrangement cache so each worker
         * has an independent copy.  arr.key_cols is a shared alias of
         * entry.key_cols inside each clone (see col_arr_entry_clone). */
        worker_sess[d].arr_entries = NULL;
        worker_sess[d].arr_count = 0;
        worker_sess[d].arr_cap = 0;
        {
            uint32_t clone_cap = 0;
            int clone_rc = col_arr_entries_clone(
                sess->arr_entries, sess->arr_count,
                &worker_sess[d].arr_entries, &clone_cap);
            if (clone_rc != 0) {
                rc = ENOMEM;
                if (wq)
                    wl_workqueue_drain(wq);
                goto cleanup_wq;
            }
            worker_sess[d].arr_count = sess->arr_count;
            worker_sess[d].arr_cap = clone_cap;
            /* Issue #216: inherit LRU tracking state so worker clock is
             * consistent with coordinator; worker computes its own totals. */
            worker_sess[d].arr_clock = sess->arr_clock;
            worker_sess[d].arr_total_bytes = sess->arr_total_bytes;
            worker_sess[d].arr_cache_limit_bytes = sess->arr_cache_limit_bytes;
        }
        /* Issue #274: Deep-copy differential arrangement cache so each worker
         * has an independent copy. This prevents concurrent realloc() races
         * when col_session_get_diff_arrangement() grows the registry. */
        worker_sess[d].diff_arr_entries = NULL;
        worker_sess[d].diff_arr_count = 0;
        worker_sess[d].diff_arr_cap = 0;
        {
            uint32_t diff_clone_cap = 0;
            int diff_clone_rc = col_diff_arr_entries_clone(
                sess->diff_arr_entries, sess->diff_arr_count,
                &worker_sess[d].diff_arr_entries, &diff_clone_cap);
            if (diff_clone_rc != 0) {
                rc = diff_clone_rc;
                if (wq)
                    wl_workqueue_drain(wq);
                goto cleanup_wq;
            }
            worker_sess[d].diff_arr_count = sess->diff_arr_count;
            worker_sess[d].diff_arr_cap = diff_clone_cap;
        }
        worker_sess[d].darr_entries = NULL;
        worker_sess[d].darr_count = 0;
        worker_sess[d].darr_cap = 0;
        /* Issue #433: workers start with empty filt_arr (isolation safety).
         * Workers rebuild filt_arr from filt_cache if needed per dispatch. */
        worker_sess[d].filt_arr_entries = NULL;
        worker_sess[d].filt_arr_count = 0;
        worker_sess[d].filt_arr_cap = 0;
        /* Issue #196: Workers start with empty mat_cache.  Divergent rule
         * copies have ~0% cache hit rate, so inheriting parent entries
         * wastes memory without benefit. */
        memset(&worker_sess[d].mat_cache, 0, sizeof(col_mat_cache_t));
        /* Issue #196: Per-worker arena isolation (arena.h contract: NOT
         * thread-safe, each worker must own its arena). */
        {
            size_t parent_cap
                = sess->eval_arena ? sess->eval_arena->capacity : 0;
            size_t worker_cap = parent_cap / k;
            if (worker_cap < 8 * 1024 * 1024)
                worker_cap = 8 * 1024 * 1024; /* 8MB minimum */
            worker_sess[d].eval_arena = wl_arena_create(worker_cap);
            /* NULL arena is handled gracefully: operators check before use */
        }
        /* Issue #196: Scale per-worker delta_pool inversely with K to
         * keep aggregate memory ~constant (32MB total vs K×32MB). */
        {
            size_t pool_arena = 32 * 1024 * 1024 / k;
            if (pool_arena < 4 * 1024 * 1024)
                pool_arena = 4 * 1024 * 1024; /* 4MB minimum */
            uint32_t pool_slots = 128 / k;
            if (pool_slots < 16)
                pool_slots = 16;
            worker_sess[d].delta_pool
                = delta_pool_create(pool_slots, sizeof(col_rel_t), pool_arena);
        }

        workers[d].plan_data.name = "<k_fusion_copy>";
        workers[d].plan_data.ops = meta->k_ops[d];
        workers[d].plan_data.op_count = meta->k_op_counts[d];
        workers[d].sess = &worker_sess[d];
        workers[d].rc = 0;

        /* Per-copy empty-delta skip (issue #85): if this copy's sub-plan
         * has a FORCE_DELTA op referencing an empty/absent delta on
         * iteration > 0, skip dispatching — the copy would produce 0 rows. */
        if (has_empty_forced_delta(&workers[d].plan_data, sess,
            sess->current_iteration)) {
            workers[d].rc = 0; /* mark as succeeded with no output */
            workers[d].skipped = true;
            continue;
        }

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
        for (uint32_t d = 0; d < k; d++) {
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
    for (uint32_t d = 0; d < k; d++) {
        /* Skipped workers (empty forced delta) contribute an empty result */
        if (workers[d].skipped) {
            results[d] = NULL; /* NULL = no rows from this copy */
            continue;
        }

        if (workers[d].rc != 0) {
            rc = workers[d].rc;
            eval_stack_drain(&workers[d].stack);
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

    /* Merge K results with deduplication.
     * Workers ran WL_PLAN_OP_CONSOLIDATE as the last plan op, so each
     * result is already sorted+deduped — no qsort needed here.
     * Skipped copies (empty forced delta) have NULL results — compact
     * them out before merging. */
    {
        /* Compact non-NULL results (skipped copies have NULL). Use the
         * existing results array as backing — we build compact in-place. */
        col_rel_t **compact = (col_rel_t **)malloc(k * sizeof(col_rel_t *));
        if (!compact) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        uint32_t n_results = 0;
        for (uint32_t d = 0; d < k; d++) {
            if (results[d])
                compact[n_results++] = results[d];
        }

        col_rel_t *merged;
        if (n_results == 0) {
            /* All copies skipped: produce empty output.  Derive column
             * count from the K-fusion target relation (op->relation_name)
             * so the empty result has a matching schema. */
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
    for (uint32_t d = 0; d < k; d++) {
        if (results[d])
            col_rel_destroy(results[d]);
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
    for (uint32_t d = 0; d < k; d++) {
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
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
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
        int64_t rrow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(right, rr, rrow_buf);
        const int64_t *rrow = rrow_buf;
        uint32_t h = hash_int64_keys_fast(rrow, rk, kc) & (nbuckets - 1);
        ht_next[rr] = ht_head[h];
        ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
    }

    /* Probe: for each left row test membership, emit if found: O(|L|) */
    int sj_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && sj_rc == 0; lr++) {
        int64_t lrow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(left, lr, lrow_buf);
        const int64_t *lrow = lrow_buf;
        uint32_t h = hash_int64_keys_fast(lrow, lk, kc) & (nbuckets - 1);
        bool found = false;
        for (uint32_t e = ht_head[h]; e != 0 && !found; e = ht_next[e - 1]) {
            uint32_t rr = e - 1;
            int64_t rrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(right, rr, rrow_buf);
            const int64_t *rrow = rrow_buf;
            if (keys_match_fast(lrow, lk, rrow, rk, kc))
                found = true;
        }
        if (found) {
            if (op->project_count > 0 && op->project_indices) {
                for (uint32_t c = 0; c < ocols; c++) {
                    uint32_t si = op->project_indices[c];
                    tmp[c] = (si < left->ncols) ? lrow[si] : 0;
                }
            } else {
                memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
            }
            sj_rc = col_rel_append_row(out, tmp);
        }
    }

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

    /* Sort by group key for group-by */
    /* (Simple O(n^2) implementation; sufficient for Phase 2A) */
    for (uint32_t r = 0; r < in->nrows; r++) {
        int64_t row_buf[COL_STACK_MAX]; col_rel_row_copy_out(in, r, row_buf);
        const int64_t *row = row_buf;

        /* Check if this group key already exists in output */
        bool found = false;
        for (uint32_t o = 0; o < out->nrows; o++) {
            bool match = true;
            for (uint32_t k = 0; k < gc && match; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                match = (row[gi < in->ncols ? gi : 0]
                    == col_rel_get(out, o, k));
            }
            if (match) {
                /* Update aggregate */
                int64_t val = (in->ncols > gc) ? row[gc] : 1;
                int64_t cur = col_rel_get(out, o, gc);
                switch (op->agg_fn) {
                case WIRELOG_AGG_COUNT:
                    col_rel_set(out, o, gc, cur + 1);
                    break;
                case WIRELOG_AGG_SUM:
                    col_rel_set(out, o, gc, cur + val);
                    break;
                case WIRELOG_AGG_MIN:
                    if (val < cur)
                        col_rel_set(out, o, gc, val);
                    break;
                case WIRELOG_AGG_MAX:
                    if (val > cur)
                        col_rel_set(out, o, gc, val);
                    break;
                default:
                    break;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            for (uint32_t k = 0; k < gc; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                tmp[k] = row[gi < in->ncols ? gi : 0];
            }
            int64_t init_val = (in->ncols > gc) ? row[gc] : 1;
            tmp[gc] = (op->agg_fn == WIRELOG_AGG_COUNT) ? 1 : init_val;
            int rc = col_rel_append_row(out, tmp);
            if (rc != 0) {
                free(tmp);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(in);
                return rc;
            }
        }
    }

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
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena,
                "$join_diff_empty", left_e.rel->ncols);
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
            uint32_t ocols = left_e.rel->ncols + right->ncols;
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
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && !left_e.is_delta
        && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && rdelta->nrows > 0 && rdelta->nrows < right->nrows) {
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
    if (op->materialized) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached)
            return eval_stack_push_delta(stack, cached, false,
                       left_e.is_delta || used_right_delta);
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
        int li = col_rel_col_idx(left, op->left_keys ? op->left_keys[k] : NULL);
        int ri
            = col_rel_col_idx(right, op->right_keys ? op->right_keys[k] : NULL);
        lk[k] = (li >= 0) ? (uint32_t)li : 0;
        rk[k] = (ri >= 0) ? (uint32_t)ri : 0;
    }

    uint32_t ocols = left->ncols + right->ncols;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$join_diff", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    out->mem_ledger = &sess->mem_ledger;

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
            int64_t rrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(right, rr, rrow_buf);
            const int64_t *rrow = rrow_buf;
            uint32_t h = hash_int64_keys_fast(rrow, rk, kc) & (nbk - 1);
            darr->ht_next[rr] = darr->ht_head[h];
            darr->ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
        }
        darr->indexed_rows = right->nrows;
        darr->current_nrows = right->nrows;

        /* Probe left against the persistent diff arrangement hash table */
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            int64_t lrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(left, lr, lrow_buf);
            const int64_t *lrow = lrow_buf;
            uint32_t h = hash_int64_keys_fast(lrow, lk, kc) & (nbk - 1);
            for (uint32_t e = darr->ht_head[h]; e != 0;
                e = darr->ht_next[e - 1]) {
                uint32_t rr = e - 1;
                int64_t rrow_buf[COL_STACK_MAX];
                col_rel_row_copy_out(right, rr, rrow_buf);
                const int64_t *rrow = rrow_buf;
                if (!keys_match_fast(lrow, lk, rrow, rk, kc))
                    continue;
                memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
                memcpy(tmp + left->ncols, rrow,
                    sizeof(int64_t) * right->ncols);
                join_rc = col_rel_append_row(out, tmp);
                if (join_rc != 0)
                    break;
                if ((sess->join_output_limit > 0
                    && out->nrows >= sess->join_output_limit)
                    || (out->nrows % 10000 == 0 && out->nrows > 0
                    && wl_mem_ledger_should_backpressure(
                        &sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 80))) {
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }

        if (join_rc != 0 && join_rc != EOVERFLOW) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
        if (join_rc == EOVERFLOW)
            join_rc = 0; /* soft truncation */
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
            int64_t rrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(right, rr, rrow_buf);
            const int64_t *rrow = rrow_buf;
            uint32_t h
                = hash_int64_keys_fast(rrow, rk, kc) & (nbuckets_ep - 1);
            ht_next_ep[rr] = ht_head_ep[h];
            ht_head_ep[h] = rr + 1;
        }
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            int64_t lrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(left, lr, lrow_buf);
            const int64_t *lrow = lrow_buf;
            uint32_t h
                = hash_int64_keys_fast(lrow, lk, kc) & (nbuckets_ep - 1);
            for (uint32_t e = ht_head_ep[h]; e != 0;
                e = ht_next_ep[e - 1]) {
                uint32_t rr = e - 1;
                int64_t rrow_buf[COL_STACK_MAX];
                col_rel_row_copy_out(right, rr, rrow_buf);
                const int64_t *rrow = rrow_buf;
                if (!keys_match_fast(lrow, lk, rrow, rk, kc))
                    continue;
                memcpy(tmp, lrow, sizeof(int64_t) * left->ncols);
                memcpy(tmp + left->ncols, rrow,
                    sizeof(int64_t) * right->ncols);
                join_rc = col_rel_append_row(out, tmp);
                if (join_rc != 0)
                    break;
                if ((sess->join_output_limit > 0
                    && out->nrows >= sess->join_output_limit)
                    || (out->nrows % 10000 == 0 && out->nrows > 0
                    && wl_mem_ledger_should_backpressure(
                        &sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 80))) {
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }
        free(ht_head_ep);
        free(ht_next_ep);
        if (join_rc != 0 && join_rc != EOVERFLOW) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
        if (join_rc == EOVERFLOW)
            join_rc = 0;
    }

    free(tmp);
    free(lk);
    free(rk);
    bool result_is_delta = left_e.is_delta || used_right_delta;

    /* Materialization cache: insert BEFORE destroying left, because
     * col_mat_cache_key_content dereferences left to compute content hash. */
    if (op->materialized) {
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
    int rc = col_rel_partition_by_key(input, meta->key_col_idxs,
            meta->key_col_count, meta->num_workers,
            coord->exchange_bufs[my_id]);

    if (input_entry.owned)
        col_rel_destroy(input);

    return rc;
}
