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

#include "../wirelog-internal.h"

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
                  uint32_t ncols, int64_t *out_val)
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
            i += slen; /* skip string data, push 0 placeholder */
            filt_push(&s, 0);
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
                    uint32_t ncols)
{
    int64_t val;
    int err = col_eval_expr_run(buf, size, row, ncols, &val);
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
                  uint32_t ncols)
{
    int64_t val;
    col_eval_expr_run(buf, size, row, ncols, &val);
    return val;
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

    if (sess->retraction_seeded && sess->current_iteration == 0) {
        /* Retraction mode: look for $r$<name> retraction delta */
        if (retraction_rel_name(op->relation_name, dname, sizeof(dname)) == 0)
            delta = session_find_rel(sess, dname);
    } else {
        /* Normal mode: look for $d$<name> insertion delta */
        snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
        delta = session_find_rel(sess, dname);
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

    /* WL_DELTA_AUTO: original heuristic */
    bool use_delta
        = (delta && delta->nrows > 0 && delta->nrows < full_rel->nrows);
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
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$map", pc);
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

    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        const int64_t *row = e.rel->data + (size_t)r * e.rel->ncols;
        for (uint32_t c = 0; c < pc; c++) {
            if (op->map_exprs && c < op->map_expr_count && op->map_exprs[c].data
                && op->map_exprs[c].size > 0) {
                tmp[c] = col_eval_expr_i64(op->map_exprs[c].data,
                                           op->map_exprs[c].size, row,
                                           e.rel->ncols);
            } else {
                uint32_t src = op->project_indices ? op->project_indices[c] : c;
                tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
            }
        }
        int rc = col_rel_append_row(out, tmp);
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
        /* Pre-allocate output buffer sized for worst-case (all rows pass) */
        size_t cap = (size_t)e.rel->nrows * e.rel->ncols;
        int64_t *tmp = (int64_t *)malloc(cap * sizeof(int64_t));
        if (!tmp) {
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(e.rel);
            return ENOMEM;
        }
        uint32_t nout = col_filter_fast(e.rel->data, e.rel->nrows, e.rel->ncols,
                                        &cmp, tmp);
        /* Bulk-copy the passing rows into the output relation */
        for (uint32_t r = 0; r < nout; r++) {
            int rc = col_rel_append_row(out, tmp + (size_t)r * e.rel->ncols);
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

    /* Slow path: full postfix bytecode interpreter */
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        const int64_t *row = e.rel->data + (size_t)r * e.rel->ncols;
        int pass = (!buf || bsz == 0)
                       ? 1
                       : col_eval_filter_row(buf, bsz, row, e.rel->ncols);
        if (pass) {
            int rc = col_rel_append_row(out, row);
            if (rc != 0) {
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return rc;
            }
        }
    }

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
static uint32_t
hash_int64_keys_neon(const int64_t *row, const uint32_t *key_cols, uint32_t kc)
{
    if (kc < 2)
        return hash_int64_keys(row, key_cols, kc);

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
/* For kc < 2 the NEON functions fall back to scalar anyway (no full SIMD
 * lane to fill).  Bypass them via inline scalar helpers to eliminate the
 * double function-call overhead on the hot kc=1 path (~98% of DOOP joins).
 * keys_match_scalar_inline uses a loop so kc=0 (cross product) returns true
 * correctly without out-of-bounds access. */
#define hash_int64_keys_fast(row, key_cols, kc)                        \
    ((kc) < 2 ? hash_int64_keys_scalar_inline((row), (key_cols), (kc)) \
              : hash_int64_keys_neon((row), (key_cols), (kc)))
#define keys_match_fast(lrow, lk, rrow, rk, kc)                            \
    ((kc) < 2 ? keys_match_scalar_inline((lrow), (lk), (rrow), (rk), (kc)) \
              : keys_match_neon((lrow), (lk), (rrow), (rk), (kc)))
#else
#define hash_int64_keys_fast hash_int64_keys
#define keys_match_fast keys_match_scalar
#endif

/* --- JOIN ---------------------------------------------------------------- */

int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, join produces empty result (cross-product with nothing).
         * Similar to ANTIJOIN logic (which keeps all left rows on missing right).
         * This can occur in generated plans where optional relations may not exist. */
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$join_empty",
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
        if (rdelta && rdelta->nrows > 0) {
            right = rdelta;
            used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded) {
            /* Iteration > 0 or delta-seeded iter 0 (issue #83):
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
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$join", ocols);
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
     * degraded (incomplete) results rather than failing entirely. */
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
            int64_t key = build->data[(size_t)bi * build->ncols + build_kcol];
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
            const int64_t *prow = probe->data + (size_t)pr * probe->ncols;
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
                    = build->data[(size_t)bi * build->ncols + build_kcol];
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
            join_rc = 0; /* soft truncation: push partial result below */
        }
        fprintf(stderr, "DEBUG[JOIN]: Unary join completed, out->nrows=%u\n",
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

        fprintf(stderr,
                "DEBUG[JOIN]: Standard merge-join starting - left=%u rows, "
                "right=%u rows, kc=%u\n",
                left->nrows, right->nrows, kc);

        if (!used_right_delta && op->right_relation && kc > 0)
            arr = col_session_get_arrangement(&sess->base, op->right_relation,
                                              rk, kc);
        else if (used_right_delta && op->right_relation && kc > 0)
            arr = col_session_get_delta_arrangement(sess, op->right_relation,
                                                    right, rk, kc);

        if (arr)
            fprintf(stderr, "DEBUG[JOIN]: Using persistent arrangement\n");
        else
            fprintf(stderr, "DEBUG[JOIN]: No arrangement available, will use "
                            "ephemeral hash table\n");

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
            fprintf(stderr,
                    "DEBUG[JOIN]: Ephemeral hash table created - nbuckets=%u\n",
                    nbuckets_ep);
            for (uint32_t rr = 0; rr < right->nrows; rr++) {
                const int64_t *rrow = right->data + (size_t)rr * right->ncols;
                uint32_t h
                    = hash_int64_keys_fast(rrow, rk, kc) & (nbuckets_ep - 1);
                ht_next_ep[rr] = ht_head_ep[h];
                ht_head_ep[h] = rr + 1; /* 1-based; 0 = end of chain */
            }
            fprintf(stderr,
                    "DEBUG[JOIN]: Ephemeral hash table built successfully\n");
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
            const int64_t *lrow = left->data + (size_t)lr * left->ncols;

            if (arr) {
                /* Arrangement probe: fill key_row at right-side positions. */
                for (uint32_t k = 0; k < kc; k++)
                    key_row[rk[k]] = lrow[lk[k]];
                uint32_t rr = col_arrangement_find_first(arr, right->data,
                                                         right->ncols, key_row);
                while (rr != UINT32_MAX && join_rc == 0) {
                    const int64_t *rrow
                        = right->data + (size_t)rr * right->ncols;
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
                    const int64_t *rrow
                        = right->data + (size_t)rr * right->ncols;
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

        fprintf(
            stderr,
            "DEBUG[JOIN]: Merge-join loop completed, out->nrows=%u, rc=%d\n",
            out->nrows, join_rc);

        free(key_row);
        free(ht_head_ep);
        free(ht_next_ep);
        if (join_rc != 0) {
            if (join_rc != EOVERFLOW) {
                fprintf(stderr,
                        "DEBUG[JOIN]: Merge-join failed with rc=%d, "
                        "out->nrows=%u\n",
                        join_rc, out->nrows);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return join_rc;
            }
            join_rc = 0; /* soft truncation: push partial result below */
        }
        fprintf(stderr, "DEBUG[JOIN]: Merge-join succeeded\n");
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
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
#ifdef WL_PROFILE
    if (out->nrows == 0)
        sess->profile.join_empty_out++;
    sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
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

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, antijoin keeps all left rows */
        return eval_stack_push(stack, left_e.rel, left_e.owned);
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
        const int64_t *rrow = right->data + (size_t)rr * right->ncols;
        uint32_t h = hash_int64_keys_fast(rrow, rk, kc) & (aj_nbuckets - 1);
        aj_next[rr] = aj_head[h];
        aj_head[h] = rr + 1;
    }
    int aj_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && aj_rc == 0; lr++) {
        const int64_t *lrow = left->data + (size_t)lr * left->ncols;
        uint32_t h = hash_int64_keys_fast(lrow, lk, kc) & (aj_nbuckets - 1);
        bool found = false;
        for (uint32_t e = aj_head[h]; e != 0 && !found; e = aj_next[e - 1]) {
            uint32_t rr = e - 1;
            const int64_t *rrow = right->data + (size_t)rr * right->ncols;
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

    int rc = col_rel_append_all(out, a);
    if (rc == 0)
        rc = col_rel_append_all(out, b);

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

/* Issue #197: kway_row_cmp now delegates to row_cmp_optimized so all 10+
 * call sites in the consolidate/merge hot paths use the SIMD dispatcher. */
static inline int
kway_row_cmp(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    return row_cmp_optimized(a, b, ncols);
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
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    if (nr <= 1)
        return 0;

    /* Sort each segment in-place */
    for (uint32_t s = 0; s < seg_count; s++) {
        uint32_t start = seg_boundaries[s];
        uint32_t end = seg_boundaries[s + 1];
        if (end > start + 1) {
            QSORT_R_CALL(rel->data + (size_t)start * nc, end - start, row_bytes,
                         &nc, row_cmp_fn);
        }
    }

    /* K=1: segments already sorted, just dedup in-place */
    if (seg_count == 1) {
        uint32_t out_r = 1;
        for (uint32_t r = 1; r < nr; r++) {
            const int64_t *prev = rel->data + (size_t)(r - 1) * nc;
            const int64_t *cur = rel->data + (size_t)r * nc;
            if (row_cmp_optimized(prev, cur, nc) != 0) {
                if (out_r != r)
                    memcpy(rel->data + (size_t)out_r * nc, cur, row_bytes);
                out_r++;
            }
        }
        rel->nrows = out_r;
        return 0;
    }

    /* Allocate merge output buffer */
    int64_t *merged = (int64_t *)malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    if (seg_count == 2) {
        /* Optimized 2-way merge (no heap) */
        uint32_t mid = seg_boundaries[1];
        uint32_t i = seg_boundaries[0], j = mid;
        uint32_t i_end = mid, j_end = seg_boundaries[2];
        uint32_t out = 0;
        int64_t *last_row = NULL;

        while (i < i_end && j < j_end) {
            int64_t *row_i = rel->data + (size_t)i * nc;
            int64_t *row_j = rel->data + (size_t)j * nc;
            int cmp = kway_row_cmp(row_i, row_j, nc);
            int64_t *row_to_add;

            if (cmp <= 0) {
                row_to_add = row_i;
                i++;
                if (cmp == 0)
                    j++; /* skip duplicate */
            } else {
                row_to_add = row_j;
                j++;
            }

            if (last_row == NULL
                || row_cmp_optimized(last_row, row_to_add, nc) != 0) {
                memcpy(merged + (size_t)out * nc, row_to_add, row_bytes);
                last_row = merged + (size_t)out * nc;
                out++;
            }
        }

        while (i < i_end) {
            int64_t *row = rel->data + (size_t)i * nc;
            if (last_row == NULL || row_cmp_optimized(last_row, row, nc) != 0) {
                memcpy(merged + (size_t)out * nc, row, row_bytes);
                last_row = merged + (size_t)out * nc;
                out++;
            }
            i++;
        }

        while (j < j_end) {
            int64_t *row = rel->data + (size_t)j * nc;
            if (last_row == NULL || row_cmp_optimized(last_row, row, nc) != 0) {
                memcpy(merged + (size_t)out * nc, row, row_bytes);
                last_row = merged + (size_t)out * nc;
                out++;
            }
            j++;
        }

        memcpy(rel->data, merged, (size_t)out * nc * sizeof(int64_t));
        rel->nrows = out;
        free(merged);
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
        return ENOMEM;
    }

    uint32_t heap_size = 0;
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_boundaries[s] < seg_boundaries[s + 1]) {
            heap[heap_size].seg = s;
            heap[heap_size].cursor = seg_boundaries[s];
            heap[heap_size].end = seg_boundaries[s + 1];
            heap_size++;
        }
    }

    /* Sift-down helper (inline macro for performance) */
#define HEAP_ROW(idx) (rel->data + (size_t)heap[(idx)].cursor * nc)
#define HEAP_SIFT_DOWN(start, size)                                      \
    do {                                                                 \
        uint32_t _p = (start);                                           \
        while (2 * _p + 1 < (size)) {                                    \
            uint32_t _c = 2 * _p + 1;                                    \
            if (_c + 1 < (size)                                          \
                && kway_row_cmp(HEAP_ROW(_c + 1), HEAP_ROW(_c), nc) < 0) \
                _c++;                                                    \
            if (kway_row_cmp(HEAP_ROW(_p), HEAP_ROW(_c), nc) <= 0)       \
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
        int64_t *min_row = HEAP_ROW(0);

        /* Dedup: skip if same as last emitted row */
        if (last_row == NULL || row_cmp_optimized(last_row, min_row, nc) != 0) {
            memcpy(merged + (size_t)out * nc, min_row, row_bytes);
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

#undef HEAP_ROW
#undef HEAP_SIFT_DOWN

    memcpy(rel->data, merged, (size_t)out * nc * sizeof(int64_t));
    rel->nrows = out;
    free(merged);
    free(heap);
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
        if (col_rel_append_all(work, in) != 0) {
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
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);

    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Issue #94: Incremental merge when a sorted prefix exists.
     * data[0..sorted_nrows) is already sorted+unique from a prior
     * consolidation.  Sort only the unsorted suffix and merge. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;
        int64_t *delta_start = work->data + (size_t)sn * nc;

        /* Phase 1: sort only the unsorted suffix */
        QSORT_R_CALL(delta_start, delta_count, row_bytes, &nc, row_cmp_fn);

        /* Phase 1b: dedup within suffix */
        uint32_t d_unique = 1;
        for (uint32_t i = 1; i < delta_count; i++) {
            if (row_cmp_optimized(delta_start + (size_t)(i - 1) * nc,
                                  delta_start + (size_t)i * nc, nc)
                != 0) {
                if (d_unique != i)
                    memcpy(delta_start + (size_t)d_unique * nc,
                           delta_start + (size_t)i * nc, row_bytes);
                d_unique++;
            }
        }

        /* Phase 2: merge sorted prefix with sorted suffix */
        uint32_t max_rows = sn + d_unique;

        /* Reuse persistent merge buffer when possible */
        int64_t *merged;
        bool used_merge_buf = false;
        if (work->merge_buf && work->merge_buf_cap >= max_rows) {
            merged = work->merge_buf;
            used_merge_buf = true;
        } else {
            /* Grow persistent buffer */
            uint32_t new_cap = max_rows > work->merge_buf_cap * 2
                                   ? max_rows
                                   : work->merge_buf_cap * 2;
            if (new_cap < max_rows)
                new_cap = max_rows;
            int64_t *nb = (int64_t *)realloc(
                work->merge_buf, (size_t)new_cap * nc * sizeof(int64_t));
            if (!nb) {
                if (work_owned && work != in)
                    col_rel_destroy(work);
                return ENOMEM;
            }
            work->merge_buf = nb;
            work->merge_buf_cap = new_cap;
            merged = nb;
            used_merge_buf = true;
        }

        uint32_t oi = 0, di = 0, out = 0;
        while (oi < sn && di < d_unique) {
            const int64_t *orow = work->data + (size_t)oi * nc;
            const int64_t *drow = delta_start + (size_t)di * nc;
            int cmp = row_cmp_optimized(orow, drow, nc);
            if (cmp < 0) {
                memcpy(merged + (size_t)out * nc, orow, row_bytes);
                oi++;
            } else if (cmp == 0) {
                memcpy(merged + (size_t)out * nc, orow, row_bytes);
                oi++;
                di++;
            } else {
                memcpy(merged + (size_t)out * nc, drow, row_bytes);
                di++;
            }
            out++;
        }
        while (oi < sn) {
            memcpy(merged + (size_t)out * nc, work->data + (size_t)oi * nc,
                   row_bytes);
            oi++;
            out++;
        }
        while (di < d_unique) {
            memcpy(merged + (size_t)out * nc, delta_start + (size_t)di * nc,
                   row_bytes);
            di++;
            out++;
        }

        /* Swap merge_buf and data pointers to avoid O(N) memcpy (issue #218). */
        if (used_merge_buf) {
            int64_t *old_data = work->data;
            uint32_t old_cap = work->capacity;
            work->data = work->merge_buf;
            work->capacity = work->merge_buf_cap;
            work->merge_buf = old_data;
            work->merge_buf_cap = old_cap;
        }
        work->nrows = out;
        work->sorted_nrows = out;

        /* Right-size data buffer after dedup (issue #218). */
        if (out > 0 && work->capacity > out + out / 4) {
            uint32_t tight = out + out / 4;
            if (tight < COL_REL_INIT_CAP)
                tight = COL_REL_INIT_CAP;
            int64_t *shrunk = (int64_t *)realloc(
                work->data, (size_t)tight * nc * sizeof(int64_t));
            if (shrunk) {
                work->data = shrunk;
                work->capacity = tight;
            }
        }

        return eval_stack_push(stack, work, work_owned);
    }

    /* Fallback: standard qsort + dedup (sorted_nrows == 0 or full re-sort) */
    QSORT_R_CALL(work->data, nr, row_bytes, &nc, row_cmp_fn);

    /* Compact: keep only unique rows */
    uint32_t out_r = 1; /* first row always kept */
    for (uint32_t r = 1; r < nr; r++) {
        const int64_t *prev = work->data + (size_t)(r - 1) * nc;
        const int64_t *cur = work->data + (size_t)r * nc;
        if (row_cmp_optimized(prev, cur, nc) != 0) {
            if (out_r != r)
                memcpy(work->data + (size_t)out_r * nc, cur, row_bytes);
            out_r++;
        }
    }
    work->nrows = out_r;
    work->sorted_nrows = out_r;

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
    int64_t *delta_start = rel->data + (size_t)old_nrows * nc;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Phase 1: sort only the new delta rows */
    QSORT_R_CALL(delta_start, delta_count, row_bytes, &nc, row_cmp_fn);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (row_cmp_optimized(delta_start + (size_t)(i - 1) * nc,
                              delta_start + (size_t)i * nc, nc)
            != 0) {
            if (d_unique != i)
                memcpy(delta_start + (size_t)d_unique * nc,
                       delta_start + (size_t)i * nc, row_bytes);
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
        const int64_t *orow = rel->data + (size_t)oi * nc;
        const int64_t *drow = delta_start + (size_t)di * nc;
        int cmp = row_cmp_optimized(orow, drow, nc);
        if (cmp < 0) {
            memcpy(merged + (size_t)out * nc, orow, row_bytes);
            oi++;
            out++;
        } else if (cmp == 0) {
            memcpy(merged + (size_t)out * nc, orow, row_bytes);
            oi++;
            di++;
            out++; /* skip duplicate from delta */
        } else {
            memcpy(merged + (size_t)out * nc, drow, row_bytes);
            di++;
            out++;
        }
    }
    /* Copy remaining from old */
    if (oi < old_nrows) {
        uint32_t remaining = old_nrows - oi;
        memcpy(merged + (size_t)out * nc, rel->data + (size_t)oi * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }
    /* Copy remaining from delta */
    if (di < d_unique) {
        uint32_t remaining = d_unique - di;
        memcpy(merged + (size_t)out * nc, delta_start + (size_t)di * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }

    /* Swap buffer */
    free(rel->data);
    rel->data = merged;
    rel->nrows = out;
    rel->capacity = (uint32_t)max_rows;
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
 * MEMORY OWNERSHIP:
 *   - Caller allocates col_rel_t *delta_out (structure only)
 *   - Function allocates and owns delta_out->data (int64_t array)
 *   - Caller responsible for freeing delta_out->data via col_rel_free_contents()
 *   - If delta_out == NULL, new rows not collected (merge only)
 *
 * ERROR HANDLING:
 *   - Returns 0 on success
 *   - Returns ENOMEM if malloc fails
 *   - On error, rel and delta_out states are undefined; caller should not use
 *
 * ALGORITHM COMPLEXITY:
 *   - Time: O(D log D + N + D) where D = new delta rows, N = old rows
 *   - Space: O(N + D) for merge buffer + delta_out buffer
 *   - Dominant term: O(N) when D << N (typical in late iterations)
 */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
                                     col_rel_t *delta_out)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1 || old_nrows >= nr)
        return 0; /* nothing new */

    uint32_t delta_count = nr - old_nrows;
    int64_t *delta_start = rel->data + (size_t)old_nrows * nc;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Phase 1: sort only the new delta rows */
    QSORT_R_CALL(delta_start, delta_count, row_bytes, &nc, row_cmp_fn);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (row_cmp_optimized(delta_start + (size_t)(i - 1) * nc,
                              delta_start + (size_t)i * nc, nc)
            != 0) {
            if (d_unique != i)
                memcpy(delta_start + (size_t)d_unique * nc,
                       delta_start + (size_t)i * nc, row_bytes);
            d_unique++;
        }
    }

    /* Phase 2: merge sorted old [0..old_nrows) with sorted+unique delta.
     * Rows present in delta but not in old are emitted into delta_out.
     *
     * Issue #94: Reuse persistent merge buffer to avoid per-call malloc/free.
     * The merge buffer lives in rel->merge_buf and grows via realloc. */
    uint32_t max_rows = old_nrows + d_unique;

    if (rel->merge_buf_cap < max_rows) {
        uint32_t new_cap = max_rows > rel->merge_buf_cap * 2
                               ? max_rows
                               : rel->merge_buf_cap * 2;
        if (new_cap < max_rows)
            new_cap = max_rows;
        int64_t *nb = (int64_t *)realloc(rel->merge_buf, (size_t)new_cap * nc
                                                             * sizeof(int64_t));
        if (!nb)
            return ENOMEM;
        rel->merge_buf = nb;
        rel->merge_buf_cap = new_cap;
    }
    int64_t *merged = rel->merge_buf;

    uint32_t oi = 0, di = 0, out = 0;
    const int64_t *o_ptr = rel->data;
    const int64_t *d_ptr = delta_start;
    int64_t *merged_ptr = merged;
    while (oi < old_nrows && di < d_unique) {
        int cmp = row_cmp_optimized(o_ptr, d_ptr, nc);
        const int64_t *row_to_copy = (cmp < 0) ? o_ptr : d_ptr;
        memcpy(merged_ptr, row_to_copy, row_bytes);

        if (cmp == 0) {
            /* duplicate: skip delta row */
            d_ptr += nc;
            di++;
        }
        if (cmp <= 0) {
            o_ptr += nc;
            oi++;
        } else {
            /* delta row not in old: new fact */
            if (delta_out)
                col_rel_append_row(delta_out, d_ptr);
            d_ptr += nc;
            di++;
        }
        merged_ptr += nc;
        out++;
    }
    /* Remaining old rows */
    if (oi < old_nrows) {
        uint32_t remaining = old_nrows - oi;
        memcpy(merged + (size_t)out * nc, rel->data + (size_t)oi * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }
    /* Remaining delta rows: all new */
    if (di < d_unique) {
        if (delta_out) {
            for (uint32_t k = di; k < d_unique; k++)
                col_rel_append_row(delta_out, delta_start + (size_t)k * nc);
        }
        uint32_t remaining = d_unique - di;
        memcpy(merged + (size_t)out * nc, delta_start + (size_t)di * nc,
               (size_t)remaining * row_bytes);
        out += remaining;
    }

    /* Swap merge_buf and data pointers to avoid O(N) memcpy (issue #218).
     * The old data buffer becomes the new merge_buf for reuse next call. */
    {
        int64_t *old_data = rel->data;
        uint32_t old_cap = rel->capacity;
        rel->data = rel->merge_buf;
        rel->capacity = rel->merge_buf_cap;
        rel->merge_buf = old_data;
        rel->merge_buf_cap = old_cap;
    }
    rel->nrows = out;
    rel->sorted_nrows = out;

    /* Phase 3b: Right-size data buffer after dedup (issue #218).
     * After merge+dedup, nrows may be much smaller than capacity (e.g.,
     * 2x-doubled capacity of 8M but only 3M unique rows). Shrink to
     * nrows * 1.25 to reclaim wasted memory while keeping headroom. */
    if (out > 0 && rel->capacity > out + out / 4) {
        uint32_t tight = out + out / 4;
        if (tight < COL_REL_INIT_CAP)
            tight = COL_REL_INIT_CAP;
        int64_t *shrunk = (int64_t *)realloc(rel->data, (size_t)tight * nc
                                                            * sizeof(int64_t));
        if (shrunk) {
            rel->data = shrunk;
            rel->capacity = tight;
        }
        /* realloc shrink failure is non-fatal; keep oversized buffer. */
    }

    /* Phase 4: Update timestamp array to match consolidated data.
     * After merge, timestamps for old rows are still valid, but new rows
     * from delta have no timestamp information. Mark timestamps as invalid
     * by deallocating (frontier computation will see NULL and return (0,0)). */
    if (rel->timestamps) {
        free(rel->timestamps);
        rel->timestamps = NULL;
    }
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
static col_rel_t *UNUSED
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

    /* K=1: Copy with dedup using append (handles dynamic growth) */
    if (k == 1) {
        col_rel_t *src = relations[0];
        const int64_t *last_row = NULL;
        for (uint32_t r = 0; r < src->nrows; r++) {
            const int64_t *row = src->data + (size_t)r * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
        }
        return out;
    }

    /* K=2: Optimized 2-pointer merge using append */
    if (k == 2) {
        col_rel_t *left = relations[0];
        col_rel_t *right = relations[1];
        uint32_t li = 0, ri = 0;
        const int64_t *last_row = NULL;

        while (li < left->nrows && ri < right->nrows) {
            const int64_t *lrow = left->data + (size_t)li * nc;
            const int64_t *rrow = right->data + (size_t)ri * nc;
            int cmp = kway_row_cmp(lrow, rrow, nc);

            const int64_t *row_to_add = NULL;
            if (cmp < 0) {
                row_to_add = lrow;
                li++;
            } else if (cmp > 0) {
                row_to_add = rrow;
                ri++;
            } else {
                /* Equal rows: add once, skip both */
                row_to_add = lrow;
                li++;
                ri++;
            }

            if (last_row == NULL
                || kway_row_cmp(last_row, row_to_add, nc) != 0) {
                if (col_rel_append_row(out, row_to_add) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
        }

        /* Drain remaining rows from left */
        while (li < left->nrows) {
            const int64_t *row = left->data + (size_t)li * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
            li++;
        }

        /* Drain remaining rows from right */
        while (ri < right->nrows) {
            const int64_t *row = right->data + (size_t)ri * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
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
        const int64_t *last_row = NULL;
        for (uint32_t r = 0; r < temp->nrows; r++) {
            const int64_t *row = temp->data + (size_t)r * nc;
            if (last_row == NULL || kway_row_cmp(last_row, row, nc) != 0) {
                if (col_rel_append_row(out, row) != 0) {
                    col_rel_destroy(out);
                    col_rel_destroy(temp);
                    return NULL;
                }
                last_row = out->data + (size_t)(out->nrows - 1) * nc;
            }
        }
        col_rel_destroy(temp);
    }

    return out;
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
        *sess;    /* Per-worker session wrapper (isolated mat_cache) */
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
         * arr_* and darr_* are zeroed: each worker builds its own arrangement
         * cache independently (no sharing, no races). Lock-free: no mutex needed
         * because each worker owns its isolated cache. */
        worker_sess[d] = *sess;
        worker_sess[d].wq = NULL; /* prevent nested K-fusion from workers */
        worker_sess[d].arr_entries = NULL;
        worker_sess[d].arr_count = 0;
        worker_sess[d].arr_cap = 0;
        worker_sess[d].darr_entries = NULL;
        worker_sess[d].darr_count = 0;
        worker_sess[d].darr_cap = 0;
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

        /* If not owned, make a copy we can hand to merge */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(worker_sess[d].delta_pool,
                                                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                eval_stack_drain(&workers[d].stack);
                goto cleanup_results;
            }
            size_t row_bytes = (size_t)e.rel->ncols * sizeof(int64_t);
            memcpy(copy->data, e.rel->data, (size_t)e.rel->nrows * row_bytes);
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
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right)
        return eval_stack_push(stack, left_e.rel, left_e.owned);

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
        = col_rel_pool_new_auto(sess->delta_pool, "$semijoin", ocols);
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
        const int64_t *rrow = right->data + (size_t)rr * right->ncols;
        uint32_t h = hash_int64_keys_fast(rrow, rk, kc) & (nbuckets - 1);
        ht_next[rr] = ht_head[h];
        ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
    }

    /* Probe: for each left row test membership, emit if found: O(|L|) */
    int sj_rc = 0;
    for (uint32_t lr = 0; lr < left->nrows && sj_rc == 0; lr++) {
        const int64_t *lrow = left->data + (size_t)lr * left->ncols;
        uint32_t h = hash_int64_keys_fast(lrow, lk, kc) & (nbuckets - 1);
        bool found = false;
        for (uint32_t e = ht_head[h]; e != 0 && !found; e = ht_next[e - 1]) {
            uint32_t rr = e - 1;
            const int64_t *rrow = right->data + (size_t)rr * right->ncols;
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
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$reduce", ocols);
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
        const int64_t *row = in->data + (size_t)r * in->ncols;

        /* Check if this group key already exists in output */
        bool found = false;
        for (uint32_t o = 0; o < out->nrows; o++) {
            int64_t *orow = out->data + (size_t)o * ocols;
            bool match = true;
            for (uint32_t k = 0; k < gc && match; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                match = (row[gi < in->ncols ? gi : 0] == orow[k]);
            }
            if (match) {
                /* Update aggregate */
                int64_t val = (in->ncols > gc) ? row[gc] : 1;
                switch (op->agg_fn) {
                case WIRELOG_AGG_COUNT:
                    orow[gc]++;
                    break;
                case WIRELOG_AGG_SUM:
                    orow[gc] += val;
                    break;
                case WIRELOG_AGG_MIN:
                    if (val < orow[gc])
                        orow[gc] = val;
                    break;
                case WIRELOG_AGG_MAX:
                    if (val > orow[gc])
                        orow[gc] = val;
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

    /* Allocate data buffer for one output row if not already present. */
    if (!dst->data) {
        uint32_t ncols = dst->ncols ? dst->ncols : 1;
        dst->data = (int64_t *)calloc(ncols, sizeof(int64_t));
        if (!dst->data)
            return ENOMEM;
        dst->capacity = 1;
    }

    /* Write the single aggregate row. */
    dst->data[0] = total;
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
            inputs[i].data = rel->data;
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
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, "$lftj",
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
    free(inputs);
    free(ncols);
    free(lftj_offsets);
    free(binary_offsets);
    return rc;
}
