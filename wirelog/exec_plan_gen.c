/*
 * exec_plan_gen.c - wirelog Plan Generator from Parsed Program
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Converts a parsed+stratified wirelog_program_t into a wl_plan_t
 * execution plan.  Replaces the deleted DD plan generation path.
 */

#include "exec_plan_gen.h"

#include "columnar/columnar_nanoarrow.h"
#include "intern.h"
#include "ir/ir.h"
#include "ir/program.h"
#include "util/log.h"
#include "wirelog-ir.h"
#include "wirelog-types.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* K-Fusion Feature Flag                                                    */
/* ======================================================================== */

/*
 * ENABLE_K_FUSION:
 *
 * When set to 1, rewrite_multiway_delta() emits a single WL_PLAN_OP_K_FUSION
 * operator (containing K independent operator sequences) instead of K
 * sequential copies joined by CONCAT+CONSOLIDATE.  The K_FUSION path enables
 * parallel workqueue execution in the columnar backend.
 *
 * Set to 1 (the default) for parallel K-fusion execution.
 * Set to 0 for the proven sequential expansion path; bench_flowlog_seq
 * builds that way via -DENABLE_K_FUSION=0.
 */
#ifndef ENABLE_K_FUSION
#define ENABLE_K_FUSION 1
#endif

/* ======================================================================== */
/* Internal helpers                                                         */
/* ======================================================================== */

static void
set_plan_error(struct wirelog_program *prog, const char *fmt, ...)
{
    if (!prog || !fmt)
        return;

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(prog->plan_error, sizeof(prog->plan_error), fmt, ap);
    va_end(ap);
}

static char *
dup_str(const char *s)
{
    if (!s)
        return NULL;
    size_t len = strlen(s);
    char *d = (char *)malloc(len + 1);
    if (!d)
        return NULL;
    memcpy(d, s, len + 1);
    return d;
}

/* Allocate "$d$<name>" for delta relation name caching (Issue #285). */
static char *
make_delta_name(const char *name)
{
    size_t len = strlen(name);
    char *d = (char *)malloc(len + 4); /* "$d$" (3) + name + NUL */
    if (!d)
        return NULL;
    /* Copy the literal's NUL too: after this memcpy the buffer must be
     * NUL-terminated (bugprone-not-null-terminated-result).  Do not write
     * sizeof("$d$") - 1 -- that silences the check without terminating.
     * d[3] is overwritten by the next memcpy. */
    memcpy(d, "$d$", sizeof("$d$"));
    memcpy(d + 3, name, len + 1);
    return d;
}

/*
 * Intern every static string literal that the plan evaluator may encounter
 * before a session starts evaluating expressions.  Otherwise evaluation
 * order can assign different symbol IDs to otherwise identical sessions.
 */
static int
wl_exec_plan_gen_preintern_expr(
    wl_intern_t *intern, const wl_ir_expr_t *expr)
{
    if (!expr)
        return 0;

    for (uint32_t i = 0; i < expr->child_count; i++) {
        if (wl_exec_plan_gen_preintern_expr(intern, expr->children[i]) != 0)
            return -1;
    }

    if (expr->type == WL_IR_EXPR_CONST_STR
        && (!expr->str_value || wl_intern_put(intern, expr->str_value) < 0))
        return -1;

    return 0;
}

static int
wl_exec_plan_gen_preintern_node(
    wl_intern_t *intern, const wirelog_ir_node_t *node)
{
    if (!node)
        return 0;

    for (uint32_t i = 0; i < node->child_count; i++) {
        if (wl_exec_plan_gen_preintern_node(intern, node->children[i]) != 0)
            return -1;
    }

    if (wl_exec_plan_gen_preintern_expr(intern, node->filter_expr) != 0)
        return -1;
    for (uint32_t i = 0; node->project_exprs && i < node->project_count; i++) {
        if (wl_exec_plan_gen_preintern_expr(intern, node->project_exprs[i])
            != 0)
            return -1;
    }
    if (wl_exec_plan_gen_preintern_expr(intern, node->agg_expr) != 0)
        return -1;

    /*
     * Compound-inline arguments and compound-side handles are excluded:
     * translate_ir_node() does not serialize those expression fields for the
     * current plan evaluator.
     */
    return 0;
}

static int
wl_exec_plan_gen_preintern_static_strings(
    const struct wirelog_program *prog)
{
    if (!prog->intern)
        return -1;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (wl_exec_plan_gen_preintern_node(
                prog->intern, prog->relation_irs ? prog->relation_irs[i] : NULL)
            != 0)
            return -1;
    }
    return 0;
}

/* ======================================================================== */
/* Expression serialization (IR expr tree -> postfix byte buffer)           */
/* ======================================================================== */

/* Dynamic byte buffer for building expression bytes */
typedef struct {
    uint8_t *data;
    uint32_t size;
    uint32_t capacity;
} expr_buf_t;

static int
expr_buf_init(expr_buf_t *buf)
{
    buf->capacity = 64;
    buf->size = 0;
    buf->data = (uint8_t *)malloc(buf->capacity);
    return buf->data ? 0 : -1;
}

static int
expr_buf_ensure(expr_buf_t *buf, uint32_t extra)
{
    uint32_t need = buf->size + extra;
    if (need <= buf->capacity)
        return 0;
    uint32_t cap = buf->capacity;
    while (cap < need)
        cap *= 2;
    uint8_t *p = (uint8_t *)realloc(buf->data, cap);
    if (!p)
        return -1;
    buf->data = p;
    buf->capacity = cap;
    return 0;
}

static int
expr_buf_push_u8(expr_buf_t *buf, uint8_t v)
{
    if (expr_buf_ensure(buf, 1) != 0)
        return -1;
    buf->data[buf->size++] = v;
    return 0;
}

static int
expr_buf_push_u16(expr_buf_t *buf, uint16_t v)
{
    if (expr_buf_ensure(buf, 2) != 0)
        return -1;
    /* little-endian */
    buf->data[buf->size++] = (uint8_t)(v & 0xFF);
    buf->data[buf->size++] = (uint8_t)((v >> 8) & 0xFF);
    return 0;
}

static int
expr_buf_push_i64(expr_buf_t *buf, int64_t v)
{
    if (expr_buf_ensure(buf, 8) != 0)
        return -1;
    /* little-endian */
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; i++) {
        buf->data[buf->size++] = (uint8_t)(u & 0xFF);
        u >>= 8;
    }
    return 0;
}

static int
expr_buf_push_bytes(expr_buf_t *buf, const uint8_t *data, uint32_t len)
{
    if (expr_buf_ensure(buf, len) != 0)
        return -1;
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return 0;
}

/* ======================================================================== */
/* Column layout context                                                    */
/* ======================================================================== */

/**
 * col_ctx_t:
 *
 * The column layout an expression is resolved against: one name and one
 * value domain per column.  @types is allocated whenever @names is, so a
 * type is never simply missing -- it is WL_IR_COLTYPE_UNKNOWN, which the
 * comparison emitter can see and report.  @types is NULL only for an empty
 * context; col_ctx_type_at() treats that as all-UNKNOWN.
 */
typedef struct {
    char **names;           /* owned; @count entries, each possibly NULL */
    wl_ir_coltype_t *types; /* owned; @count entries */
    uint32_t count;
} col_ctx_t;

static void
col_ctx_free(col_ctx_t *ctx)
{
    if (!ctx)
        return;
    if (ctx->names) {
        for (uint32_t i = 0; i < ctx->count; i++)
            free(ctx->names[i]);
        free((void *)ctx->names);
    }
    free(ctx->types);
    ctx->names = NULL;
    ctx->types = NULL;
    ctx->count = 0;
}

/* Allocate @count names and types.  Returns 0 on success. */
static int
col_ctx_alloc(col_ctx_t *ctx, uint32_t count)
{
    ctx->count = count;
    ctx->names = (char **)calloc(count ? count : 1, sizeof(char *));
    ctx->types
        = (wl_ir_coltype_t *)calloc(count ? count : 1, sizeof(wl_ir_coltype_t));
    if (!ctx->names || !ctx->types) {
        col_ctx_free(ctx);
        return -1;
    }
    return 0;
}

static wl_ir_coltype_t
col_ctx_type_at(const col_ctx_t *ctx, uint32_t idx)
{
    if (!ctx || !ctx->types || idx >= ctx->count)
        return WL_IR_COLTYPE_UNKNOWN;
    return ctx->types[idx];
}

/**
 * Resolve a variable name to its column's value domain.
 *
 * Names are matched the same way serialize_expr() matches them for emission,
 * so typing and emission cannot disagree about which column a variable
 * refers to: first by declared name, then -- for expressions whose variables
 * were already rewritten to positional form by
 * rewrite_expr_vars_to_columns() -- by parsing "colN".
 */
static wl_ir_coltype_t
col_ctx_lookup_type(const col_ctx_t *ctx, const char *var_name)
{
    if (!ctx || !var_name)
        return WL_IR_COLTYPE_UNKNOWN;

    for (uint32_t c = 0; c < ctx->count; c++) {
        if (ctx->names && ctx->names[c]
            && strcmp(ctx->names[c], var_name) == 0)
            return col_ctx_type_at(ctx, c);
    }

    if (strncmp(var_name, "col", 3) == 0
        && isdigit((unsigned char)var_name[3])) {
        char *end = NULL;
        unsigned long idx = strtoul(var_name + 3, &end, 10);
        if (end && *end == '\0' && idx < ctx->count)
            return col_ctx_type_at(ctx, (uint32_t)idx);
    }

    return WL_IR_COLTYPE_UNKNOWN;
}

static const char *
arith_op_name(wirelog_arith_op_t op)
{
    switch (op) {
    case WIRELOG_ARITH_HASH:        return "hash";
    case WIRELOG_ARITH_CRC32_ETH:   return "crc32_ethernet";
    case WIRELOG_ARITH_CRC32_CAST:  return "crc32_castagnoli";
    case WIRELOG_ARITH_MD5:         return "md5";
    case WIRELOG_ARITH_SHA1:        return "sha1";
    case WIRELOG_ARITH_SHA256:      return "sha256";
    case WIRELOG_ARITH_SHA512:      return "sha512";
    case WIRELOG_ARITH_HMAC_SHA256: return "hmac_sha256";
    case WIRELOG_ARITH_UUID5:       return "uuid5";
    default:                        return "?";
    }
}

/**
 * Map an IR arith op to a plan expr tag, given the value domains of its
 * operands (Issue #963).
 *
 * @t0 and @t1 are the domains of the first and second child; @t1 is
 * WL_IR_COLTYPE_UNKNOWN for unary operators.
 *
 * Symbols are stored as intern ids.  The 0x1B..0x2A digest opcodes digest
 * that id, which makes hash("abc") a fact about the intern table rather
 * than about "abc": it matches no external tool, and the same string
 * digests differently depending on what was interned first.  When an
 * operand is string-typed the emitter picks the shadow opcode that reverses
 * the id and digests the string's own bytes instead.
 *
 * Two points where this diverges from cmp_to_tag()'s rules, deliberately:
 *
 *   - #962 keeps the integer opcode when only one operand of a comparison
 *     is a string, because CMP_STR_* needs *both* sides reversed and a
 *     one-sided match would drop every row.  hmac_sha256() and uuid5()
 *     have no such coupling -- each operand contributes its own bytes --
 *     so applying that rule here would leave the mixed case digesting an
 *     id, i.e. would preserve the defect.  Each therefore gets three
 *     opcodes (_SS/_SI/_IS) and only the all-integer case keeps 0x28/0x2A.
 *
 *   - EQ/NEQ are exempt in #962 because interning is canonical.  Nothing
 *     analogous applies to a digest: the digest of an id is not the digest
 *     of the string under any convention.
 *
 * WL_IR_EXPR_ARITH itself reports SCALAR from expr_result_type(), which is
 * correct -- no arith op produces a string -- so hash(hash(x)) digests the
 * inner result as the integer it is.
 */
static uint8_t
arith_digest_tag(wirelog_arith_op_t op, wl_ir_coltype_t t0, wl_ir_coltype_t t1)
{
    bool s0 = (t0 == WL_IR_COLTYPE_STRING);
    bool s1 = (t1 == WL_IR_COLTYPE_STRING);

    switch (op) {
    case WIRELOG_ARITH_HASH:
        return s0 ? WL_PLAN_EXPR_ARITH_HASH_S : WL_PLAN_EXPR_ARITH_HASH;
    case WIRELOG_ARITH_CRC32_ETH:
        return s0 ? WL_PLAN_EXPR_ARITH_CRC32_ETH_S
                  : WL_PLAN_EXPR_ARITH_CRC32_ETH;
    case WIRELOG_ARITH_CRC32_CAST:
        return s0 ? WL_PLAN_EXPR_ARITH_CRC32_CAST_S
                  : WL_PLAN_EXPR_ARITH_CRC32_CAST;
    case WIRELOG_ARITH_MD5:
        return s0 ? WL_PLAN_EXPR_ARITH_MD5_S : WL_PLAN_EXPR_ARITH_MD5;
    case WIRELOG_ARITH_SHA1:
        return s0 ? WL_PLAN_EXPR_ARITH_SHA1_S : WL_PLAN_EXPR_ARITH_SHA1;
    case WIRELOG_ARITH_SHA256:
        return s0 ? WL_PLAN_EXPR_ARITH_SHA256_S : WL_PLAN_EXPR_ARITH_SHA256;
    case WIRELOG_ARITH_SHA512:
        return s0 ? WL_PLAN_EXPR_ARITH_SHA512_S : WL_PLAN_EXPR_ARITH_SHA512;
    case WIRELOG_ARITH_HMAC_SHA256:
        if (s0 && s1)
            return WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS;
        if (s0)
            return WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI;
        if (s1)
            return WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS;
        return WL_PLAN_EXPR_ARITH_HMAC_SHA256;
    case WIRELOG_ARITH_UUID5:
        if (s0 && s1)
            return WL_PLAN_EXPR_ARITH_UUID5_SS;
        if (s0)
            return WL_PLAN_EXPR_ARITH_UUID5_SI;
        if (s1)
            return WL_PLAN_EXPR_ARITH_UUID5_IS;
        return WL_PLAN_EXPR_ARITH_UUID5;
    default:
        break;
    }
    return WL_PLAN_EXPR_ARITH_ADD; /* unreachable: caller filters */
}

/* True for the ops whose operand domain selects between opcode families. */
static bool
arith_op_is_digest(wirelog_arith_op_t op)
{
    switch (op) {
    case WIRELOG_ARITH_HASH:
    case WIRELOG_ARITH_CRC32_ETH:
    case WIRELOG_ARITH_CRC32_CAST:
    case WIRELOG_ARITH_MD5:
    case WIRELOG_ARITH_SHA1:
    case WIRELOG_ARITH_SHA256:
    case WIRELOG_ARITH_SHA512:
    case WIRELOG_ARITH_HMAC_SHA256:
    case WIRELOG_ARITH_UUID5:
        return true;
    default:
        return false;
    }
}

/* Map IR arith op -> plan expr tag.
 *
 * @t0/@t1 are the operand value domains (Issue #963); pass UNKNOWN for
 * operands that do not exist.  Only the digest family reads them. */
static uint8_t
arith_to_tag(wirelog_arith_op_t op, wl_ir_coltype_t t0, wl_ir_coltype_t t1)
{
    if (arith_op_is_digest(op)) {
        /*
         * An operand with no declared type keeps the integer opcode, so it
         * digests an id.  Rejecting it would break programs that work
         * today -- head and intermediate relations need no .decl -- but the
         * result is as unstable as the id assignment, so say so.  The
         * UNKNOWN sentinel is what makes "never declared" distinguishable
         * from "declared numeric", which is a legitimate integer digest.
         */
        bool binary = (op == WIRELOG_ARITH_HMAC_SHA256
            || op == WIRELOG_ARITH_UUID5);
        if (t0 == WL_IR_COLTYPE_UNKNOWN
            || (binary && t1 == WL_IR_COLTYPE_UNKNOWN)) {
            WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_WARN,
                "'%s' applied to a column with no declared type: digesting "
                "the interned id, not the string bytes. Declare the relation "
                "(.decl R(c: symbol)) if the column holds symbols",
                arith_op_name(op));
        }
        return arith_digest_tag(op, t0, t1);
    }

    switch (op) {
    case WIRELOG_ARITH_ADD:
        return WL_PLAN_EXPR_ARITH_ADD;
    case WIRELOG_ARITH_SUB:
        return WL_PLAN_EXPR_ARITH_SUB;
    case WIRELOG_ARITH_MUL:
        return WL_PLAN_EXPR_ARITH_MUL;
    case WIRELOG_ARITH_DIV:
        return WL_PLAN_EXPR_ARITH_DIV;
    case WIRELOG_ARITH_MOD:
        return WL_PLAN_EXPR_ARITH_MOD;
    case WIRELOG_ARITH_BAND:
        return WL_PLAN_EXPR_ARITH_BAND;
    case WIRELOG_ARITH_BOR:
        return WL_PLAN_EXPR_ARITH_BOR;
    case WIRELOG_ARITH_BXOR:
        return WL_PLAN_EXPR_ARITH_BXOR;
    case WIRELOG_ARITH_BNOT:
        return WL_PLAN_EXPR_ARITH_BNOT;
    case WIRELOG_ARITH_SHL:
        return WL_PLAN_EXPR_ARITH_SHL;
    case WIRELOG_ARITH_SHR:
        return WL_PLAN_EXPR_ARITH_SHR;
    case WIRELOG_ARITH_HASH:
        return WL_PLAN_EXPR_ARITH_HASH;
    case WIRELOG_ARITH_CRC32_ETH:
        return WL_PLAN_EXPR_ARITH_CRC32_ETH;
    case WIRELOG_ARITH_CRC32_CAST:
        return WL_PLAN_EXPR_ARITH_CRC32_CAST;
    case WIRELOG_ARITH_MD5:
        return WL_PLAN_EXPR_ARITH_MD5;
    case WIRELOG_ARITH_SHA1:
        return WL_PLAN_EXPR_ARITH_SHA1;
    case WIRELOG_ARITH_SHA256:
        return WL_PLAN_EXPR_ARITH_SHA256;
    case WIRELOG_ARITH_SHA512:
        return WL_PLAN_EXPR_ARITH_SHA512;
    case WIRELOG_ARITH_HMAC_SHA256:
        return WL_PLAN_EXPR_ARITH_HMAC_SHA256;
    case WIRELOG_ARITH_UUID4:
        return WL_PLAN_EXPR_ARITH_UUID4;
    case WIRELOG_ARITH_UUID5:
        return WL_PLAN_EXPR_ARITH_UUID5;
    }
    return WL_PLAN_EXPR_ARITH_ADD; /* fallback */
}

/* Map IR string function -> plan expr tag */
static uint8_t
str_fn_to_tag(wirelog_str_fn_t fn)
{
    switch (fn) {
    case WIRELOG_STR_FN_STRLEN:      return WL_PLAN_EXPR_STR_FN_STRLEN;
    case WIRELOG_STR_FN_CAT:         return WL_PLAN_EXPR_STR_FN_CAT;
    case WIRELOG_STR_FN_SUBSTR:      return WL_PLAN_EXPR_STR_FN_SUBSTR;
    case WIRELOG_STR_FN_CONTAINS:    return WL_PLAN_EXPR_STR_FN_CONTAINS;
    case WIRELOG_STR_FN_STR_PREFIX:  return WL_PLAN_EXPR_STR_FN_STR_PREFIX;
    case WIRELOG_STR_FN_STR_SUFFIX:  return WL_PLAN_EXPR_STR_FN_STR_SUFFIX;
    case WIRELOG_STR_FN_STR_ORD:     return WL_PLAN_EXPR_STR_FN_STR_ORD;
    case WIRELOG_STR_FN_TO_UPPER:    return WL_PLAN_EXPR_STR_FN_TO_UPPER;
    case WIRELOG_STR_FN_TO_LOWER:    return WL_PLAN_EXPR_STR_FN_TO_LOWER;
    case WIRELOG_STR_FN_STR_REPLACE: return WL_PLAN_EXPR_STR_FN_STR_REPLACE;
    case WIRELOG_STR_FN_TRIM:        return WL_PLAN_EXPR_STR_FN_TRIM;
    case WIRELOG_STR_FN_TO_STRING:   return WL_PLAN_EXPR_STR_FN_TO_STRING;
    case WIRELOG_STR_FN_TO_NUMBER:   return WL_PLAN_EXPR_STR_FN_TO_NUMBER;
    case WIRELOG_STR_FN_UUID5_RFC:   return WL_PLAN_EXPR_STR_FN_UUID5_RFC;
    }
    return WL_PLAN_EXPR_STR_FN_STRLEN; /* fallback */
}

static const char *
cmp_op_name(wirelog_cmp_op_t op)
{
    switch (op) {
    case WIRELOG_CMP_EQ:  return "=";
    case WIRELOG_CMP_NEQ: return "!=";
    case WIRELOG_CMP_LT:  return "<";
    case WIRELOG_CMP_GT:  return ">";
    case WIRELOG_CMP_LTE: return "<=";
    case WIRELOG_CMP_GTE: return ">=";
    }
    return "?";
}

/**
 * Map an IR comparison to a plan expr tag, given the value domains of its
 * two operands (Issue #962).
 *
 * Symbols are stored as intern ids, and ids are assigned in first-appearance
 * order, so the integer opcodes order symbols by when they were first seen.
 * WL_PLAN_EXPR_CMP_STR_* reverse the ids through wl_intern_reverse() and
 * strcmp() the strings instead, which is the ordering the language means.
 *
 * Only LT/GT/LTE/GTE are converted:
 *
 *   - EQ and NEQ are already correct.  Interning is canonical -- one id per
 *     distinct string -- so id equality IS string equality.  Converting them
 *     would buy nothing and cost the demotion described below on every
 *     equality filter in every program.
 *
 *   - The conversion is only sound when BOTH operands are strings.
 *     col_eval_expr() returns false as soon as wl_intern_reverse() yields
 *     NULL for either side, so a string opcode applied to an integer operand
 *     does not misorder rows, it drops all of them.  A one-sided type match
 *     therefore keeps the integer opcode.
 *
 * Emitting a CMP_STR_* opcode also demotes the filter from the compiled and
 * column-native SIMD paths to the bytecode interpreter: col_expr_compile()
 * and filter_is_simple_cmp() both reject unrecognised opcodes.  That is the
 * bulk of the measured cost (~66 ns of ~77 ns per comparison) and is why the
 * conversion is kept as narrow as it is.  Integer filters keep the fast
 * paths untouched.
 */
static uint8_t
cmp_to_tag(wirelog_cmp_op_t op, wl_ir_coltype_t lhs, wl_ir_coltype_t rhs)
{
    switch (op) {
    case WIRELOG_CMP_EQ:
        return WL_PLAN_EXPR_CMP_EQ;
    case WIRELOG_CMP_NEQ:
        return WL_PLAN_EXPR_CMP_NEQ;
    case WIRELOG_CMP_LT:
    case WIRELOG_CMP_GT:
    case WIRELOG_CMP_LTE:
    case WIRELOG_CMP_GTE:
        break;
    default:
        return WL_PLAN_EXPR_CMP_EQ; /* fallback */
    }

    if (lhs == WL_IR_COLTYPE_STRING && rhs == WL_IR_COLTYPE_STRING) {
        switch (op) {
        case WIRELOG_CMP_LT:  return WL_PLAN_EXPR_CMP_STR_LT;
        case WIRELOG_CMP_GT:  return WL_PLAN_EXPR_CMP_STR_GT;
        case WIRELOG_CMP_LTE: return WL_PLAN_EXPR_CMP_STR_LTE;
        default:              return WL_PLAN_EXPR_CMP_STR_GTE;
        }
    }

    /*
     * Anything else keeps the integer opcode, but says so.  Rejecting these
     * at lowering time would break programs that work today: head and
     * intermediate relations need no .decl, undeclared relations have no
     * column types at all, and a rule over an undeclared integer relation
     * evaluates perfectly well.  A warning is the most that can be done
     * without breaking them, and the UNKNOWN sentinel is what makes the two
     * cases distinguishable -- a conflict between two known types is a
     * different report from a type that was never established.
     */
    if (lhs == WL_IR_COLTYPE_UNKNOWN || rhs == WL_IR_COLTYPE_UNKNOWN) {
        WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_WARN,
            "ordering comparison '%s' on a column with no declared type: "
            "comparing interned ids, not strings. Declare the relation "
            "(.decl R(c: string)) if the column holds symbols",
            cmp_op_name(op));
    } else if (lhs != rhs) {
        WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_WARN,
            "ordering comparison '%s' mixes a string operand with a numeric "
            "one: comparing interned ids, not strings",
            cmp_op_name(op));
    }

    switch (op) {
    case WIRELOG_CMP_LT:  return WL_PLAN_EXPR_CMP_LT;
    case WIRELOG_CMP_GT:  return WL_PLAN_EXPR_CMP_GT;
    case WIRELOG_CMP_LTE: return WL_PLAN_EXPR_CMP_LTE;
    default:              return WL_PLAN_EXPR_CMP_GTE;
    }
}

/*
 * Map IR agg fn -> plan expr tag, or -1 when the aggregate has no tag.
 *
 * WIRELOG_AGG_AVG used to map to WL_PLAN_EXPR_AGG_SUM, commented
 * "approximate: no AVG tag": the serialized plan said SUM where the program
 * said average, so a backend reading the plan could not tell the two apart
 * and no diagnostic was emitted either way.  average() is now refused at the
 * WIRELOG_IR_AGGREGATE arm below, which is the only route the parser can
 * take here -- an AGGREGATE node is always a top-level head argument, so
 * min(average(v)) and the like are parse errors and this expression form
 * never carries AVG.  Failing rather than substituting keeps that true for
 * any future caller that builds IR directly (Issue #978).
 */
static int
agg_to_tag(wirelog_agg_fn_t fn)
{
    switch (fn) {
    case WIRELOG_AGG_COUNT:
        return WL_PLAN_EXPR_AGG_COUNT;
    case WIRELOG_AGG_SUM:
        return WL_PLAN_EXPR_AGG_SUM;
    case WIRELOG_AGG_MIN:
        return WL_PLAN_EXPR_AGG_MIN;
    case WIRELOG_AGG_MAX:
        return WL_PLAN_EXPR_AGG_MAX;
    case WIRELOG_AGG_AVG:
        return -1;
    }
    return -1; /* not an aggregate this plan format can encode */
}

/**
 * The value domain an expression evaluates to (Issue #962).
 *
 * Everything the postfix evaluator pushes is an int64_t; what differs is
 * whether that int64_t is a value or an interned symbol id.  String
 * functions return runtime-interned ids, which is why to_upper(a) < to_upper(b)
 * is a string comparison even though neither operand is a bare column.
 * Comparisons and aggregates push counts and booleans, which are values.
 */
static wl_ir_coltype_t
expr_result_type(const wl_ir_expr_t *expr, const col_ctx_t *ctx)
{
    if (!expr)
        return WL_IR_COLTYPE_UNKNOWN;

    switch (expr->type) {
    case WL_IR_EXPR_VAR:
        return col_ctx_lookup_type(ctx, expr->var_name);

    case WL_IR_EXPR_CONST_STR:
        return WL_IR_COLTYPE_STRING;

    case WL_IR_EXPR_CONST_INT:
    case WL_IR_EXPR_BOOL:
    case WL_IR_EXPR_ARITH:
    case WL_IR_EXPR_CMP:
    case WL_IR_EXPR_AGG:
        return WL_IR_COLTYPE_SCALAR;

    case WL_IR_EXPR_STR_FN:
        switch (expr->str_fn) {
        /* Produce a new interned string. */
        case WIRELOG_STR_FN_CAT:
        case WIRELOG_STR_FN_SUBSTR:
        case WIRELOG_STR_FN_TO_UPPER:
        case WIRELOG_STR_FN_TO_LOWER:
        case WIRELOG_STR_FN_STR_REPLACE:
        case WIRELOG_STR_FN_TRIM:
        case WIRELOG_STR_FN_TO_STRING:
        case WIRELOG_STR_FN_UUID5_RFC:
            return WL_IR_COLTYPE_STRING;
        /* Produce a length, a code point, a boolean or a number. */
        case WIRELOG_STR_FN_STRLEN:
        case WIRELOG_STR_FN_CONTAINS:
        case WIRELOG_STR_FN_STR_PREFIX:
        case WIRELOG_STR_FN_STR_SUFFIX:
        case WIRELOG_STR_FN_STR_ORD:
        case WIRELOG_STR_FN_TO_NUMBER:
            return WL_IR_COLTYPE_SCALAR;
        }
        return WL_IR_COLTYPE_UNKNOWN;
    }

    return WL_IR_COLTYPE_UNKNOWN;
}

/**
 * Carry an IR column type across into the plan's aggregate operand domain
 * (Issue #965).
 *
 * The two enumerations agree numerically today, and they are deliberately
 * not cast into one another: wl_ir_coltype_t belongs to the IR (ir/ir.h)
 * and wl_plan_agg_operand_t to the plan (exec_plan.h), which no IR header
 * reaches, so a _Static_assert tying them together is not even expressible
 * at either definition site.  An explicit switch costs nothing, and the
 * compiler's -Wswitch flags a new IR column type here rather than letting
 * it arrive in the backend as a silent SCALAR.
 */
static wl_plan_agg_operand_t
agg_operand_from_coltype(wl_ir_coltype_t t)
{
    switch (t) {
    case WL_IR_COLTYPE_SCALAR:
        return WL_PLAN_AGG_OPERAND_SCALAR;
    case WL_IR_COLTYPE_STRING:
        return WL_PLAN_AGG_OPERAND_STRING;
    case WL_IR_COLTYPE_UNKNOWN:
        break;
    }
    return WL_PLAN_AGG_OPERAND_UNKNOWN;
}

/**
 * Serialize an IR expression tree into postfix (RPN) byte encoding.
 * Walks the tree recursively: children first (postfix), then operator.
 */
static int
serialize_expr(expr_buf_t *buf, const wl_ir_expr_t *expr,
    const col_ctx_t *ctx)
{
    if (!expr)
        return -1;

    switch (expr->type) {
    case WL_IR_EXPR_VAR: {
        if (!expr->var_name)
            return -1;
        /* Resolve variable name to "colN" using column name context */
        char resolved[32];
        const char *emit_name = expr->var_name;
        if (ctx && ctx->names && ctx->count > 0) {
            for (uint32_t c = 0; c < ctx->count; c++) {
                if (ctx->names[c]
                    && strcmp(ctx->names[c], expr->var_name) == 0) {
                    snprintf(resolved, sizeof(resolved), "col%u", c);
                    emit_name = resolved;
                    break;
                }
            }
        }
        uint16_t len = (uint16_t)strlen(emit_name);
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_VAR) != 0)
            return -1;
        if (expr_buf_push_u16(buf, len) != 0)
            return -1;
        if (expr_buf_push_bytes(buf, (const uint8_t *)emit_name, len) != 0)
            return -1;
        return 0;
    }
    case WL_IR_EXPR_CONST_INT:
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_CONST_INT) != 0)
            return -1;
        return expr_buf_push_i64(buf, expr->int_value);

    case WL_IR_EXPR_CONST_STR: {
        if (!expr->str_value)
            return -1;
        uint16_t len = (uint16_t)strlen(expr->str_value);
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_CONST_STR) != 0)
            return -1;
        if (expr_buf_push_u16(buf, len) != 0)
            return -1;
        if (expr_buf_push_bytes(buf, (const uint8_t *)expr->str_value, len)
            != 0)
            return -1;
        return 0;
    }
    case WL_IR_EXPR_BOOL:
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_BOOL) != 0)
            return -1;
        return expr_buf_push_u8(buf, expr->bool_value ? 1 : 0);

    case WL_IR_EXPR_ARITH: {
        /* Serialize children first (postfix) */
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], ctx) != 0)
                return -1;
        }
        /* Operand domains select between the integer and the string digest
         * opcode families (Issue #963). */
        wl_ir_coltype_t t0 = (expr->child_count > 0)
            ? expr_result_type(expr->children[0], ctx)
            : WL_IR_COLTYPE_UNKNOWN;
        wl_ir_coltype_t t1 = (expr->child_count > 1)
            ? expr_result_type(expr->children[1], ctx)
            : WL_IR_COLTYPE_UNKNOWN;
        return expr_buf_push_u8(buf, arith_to_tag(expr->arith_op, t0, t1));
    }

    case WL_IR_EXPR_CMP: {
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], ctx) != 0)
                return -1;
        }
        wl_ir_coltype_t lhs = (expr->child_count > 0)
            ? expr_result_type(expr->children[0], ctx)
            : WL_IR_COLTYPE_UNKNOWN;
        wl_ir_coltype_t rhs = (expr->child_count > 1)
            ? expr_result_type(expr->children[1], ctx)
            : WL_IR_COLTYPE_UNKNOWN;
        return expr_buf_push_u8(buf, cmp_to_tag(expr->cmp_op, lhs, rhs));
    }

    case WL_IR_EXPR_AGG: {
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], ctx) != 0)
                return -1;
        }
        int agg_tag = agg_to_tag(expr->agg_fn);
        if (agg_tag < 0)
            return -1;
        return expr_buf_push_u8(buf, (uint8_t)agg_tag);
    }

    case WL_IR_EXPR_STR_FN:
        /* Serialize arguments first (postfix), then emit the function opcode */
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], ctx) != 0)
                return -1;
        }
        return expr_buf_push_u8(buf, str_fn_to_tag(expr->str_fn));
    }

    return -1; /* unknown type */
}

/**
 * Serialize an IR expression into a wl_plan_expr_buffer_t.
 * @ctx provides variable name -> column index resolution and the column
 * types the comparison emitter needs.
 * Returns 0 on success. On NULL expr, produces empty buffer (valid no-op).
 */
static int
serialize_expr_to_buffer_ctx(const wl_ir_expr_t *expr, const col_ctx_t *ctx,
    wl_plan_expr_buffer_t *out_buf)
{
    out_buf->data = NULL;
    out_buf->size = 0;

    if (!expr)
        return 0; /* empty = no expression */

    expr_buf_t buf;
    if (expr_buf_init(&buf) != 0)
        return -1;

    if (serialize_expr(&buf, expr, ctx) != 0) {
        free(buf.data);
        return -1;
    }

    out_buf->data = buf.data;
    out_buf->size = buf.size;
    return 0;
}

/* ======================================================================== */
/* Operator list builder                                                    */
/* ======================================================================== */

typedef struct {
    wl_plan_op_t *ops;
    uint32_t count;
    uint32_t capacity;

    /* The whole-relation reduction this operator list admits, accumulated as
     * the REDUCEs are emitted (Issue #975).  See agg_spec_observe(). */
    wl_plan_agg_spec_t agg;
    bool agg_vetoed;
} op_list_t;

static int
op_list_init(op_list_t *list)
{
    list->capacity = 8;
    list->count = 0;
    memset(&list->agg, 0, sizeof(list->agg));
    list->agg_vetoed = false;
    list->ops = (wl_plan_op_t *)calloc(list->capacity, sizeof(wl_plan_op_t));
    return list->ops ? 0 : -1;
}

/*
 * Fold one emitted REDUCE into the relation's whole-relation reduction
 * specification (Issue #975).
 *
 * This reproduces, exactly, the admission rules the columnar backend used to
 * rediscover by scanning the finished operator list -- the scan #975 deleted,
 * because a fused relation has no REDUCE left to find.  The rules are:
 *
 *   - an aggregate other than MIN or MAX has no domination order, so the
 *     relation is refused outright;
 *   - two rules of one head that disagree on the aggregate, or on the number
 *     of grouping columns, leave no single order to reduce under, so the
 *     relation is refused as well.
 *
 * Both refusals are *sticky*: agg_vetoed latches, so a later agreeing REDUCE
 * cannot resurrect a specification an earlier conflict rejected.  Overwriting
 * per REDUCE instead of vetoing would be a behaviour change in the wrong
 * direction -- it would start collapsing relations that are correctly left
 * alone today, under an order only one of their rules asked for.
 *
 * The operand's domain is reconciled rather than overwritten (Issue #1024).
 * It used to be the one field not compared -- the last REDUCE's domain won,
 * reproducing what the scan #975 deleted had done -- so for a head whose
 * rules agreed on the aggregate and the group width but disagreed on the
 * domain, *rule order* decided whether min() ordered a symbol column
 * lexicographically or by interned id.  That is a latent variant of #965,
 * whose whole point was that ordering symbols by id makes the answer depend
 * on which unrelated facts were parsed first.
 *
 * Two disagreements are not the same thing and are not treated the same:
 *
 *   UNKNOWN against a known domain is not a conflict.  UNKNOWN means no
 *   producer typed the operand -- an undeclared relation -- not that it is
 *   numeric.  The rule that does know wins, in either direction, so the
 *   answer no longer depends on which rule came last.
 *
 *   SCALAR against STRING is a real conflict: one rule's `.decl` says the
 *   column holds numbers and another's says it holds symbols.  There is no
 *   widening that is right for both -- ordering the numeric rule's values
 *   lexicographically would reverse-intern values that are not ids -- so the
 *   relation is vetoed, exactly as it already is when the rules disagree on
 *   the aggregate function or the grouping width.  This is the issue's
 *   option 1 for the case that genuinely needs it and its option 2 for the
 *   case that does not; a blanket "any STRING wins" would silently reorder a
 *   numeric column.
 *
 * The veto is sticky like the others.
 */
static void
agg_spec_observe(op_list_t *list, const wl_plan_op_t *reduce)
{
    if (list->agg_vetoed)
        return;

    if (reduce->agg_fn != WIRELOG_AGG_MIN
        && reduce->agg_fn != WIRELOG_AGG_MAX) {
        list->agg_vetoed = true;
        memset(&list->agg, 0, sizeof(list->agg));
        return;
    }

    if (list->agg.has_spec
        && (list->agg.fn != reduce->agg_fn
        || list->agg.group_by_count != reduce->group_by_count
        || list->agg.aggregate_index != reduce->aggregate_index)) {
        list->agg_vetoed = true;
        memset(&list->agg, 0, sizeof(list->agg));
        return;
    }

    /* Issue #1024: reconcile the domain instead of letting the last rule win. */
    if (list->agg.has_spec
        && list->agg.operand_type != reduce->agg_operand_type
        && list->agg.operand_type != WL_PLAN_AGG_OPERAND_UNKNOWN
        && reduce->agg_operand_type != WL_PLAN_AGG_OPERAND_UNKNOWN) {
        list->agg_vetoed = true;
        memset(&list->agg, 0, sizeof(list->agg));
        return;
    }

    wl_plan_agg_operand_t domain = reduce->agg_operand_type;
    if (domain == WL_PLAN_AGG_OPERAND_UNKNOWN && list->agg.has_spec)
        domain = list->agg.operand_type;

    list->agg.has_spec = true;
    list->agg.fn = reduce->agg_fn;
    list->agg.group_by_count = reduce->group_by_count;
    list->agg.aggregate_index = reduce->aggregate_index;
    list->agg.operand_type = domain;
}

static wl_plan_op_t *
op_list_push(op_list_t *list)
{
    if (list->count >= list->capacity) {
        uint32_t cap = list->capacity * 2;
        wl_plan_op_t *p
            = (wl_plan_op_t *)realloc(list->ops, cap * sizeof(wl_plan_op_t));
        if (!p)
            return NULL;
        list->ops = p;
        list->capacity = cap;
    }
    wl_plan_op_t *op = &list->ops[list->count];
    memset(op, 0, sizeof(*op));
    list->count++;
    return op;
}

/* Duplicate a uint32_t array */
static uint32_t *
dup_indices(const uint32_t *src, uint32_t count)
{
    if (!src || count == 0)
        return NULL;
    uint32_t *dst = (uint32_t *)malloc(count * sizeof(uint32_t));
    if (!dst)
        return NULL;
    memcpy(dst, src, count * sizeof(uint32_t));
    return dst;
}

/* ======================================================================== */
/* Column layout resolution                                                 */
/* ======================================================================== */

/**
 * Collect the output column layout produced by an IR node.
 * For SCAN: returns the declared column_names and column_types.
 * For JOIN: concatenates left + right child columns.
 * For SEMIJOIN/ANTIJOIN: the left child's columns only -- these operators
 * filter rows and never widen their input.
 * For PROJECT/FILTER/FLATMAP/UNION: delegates to child[0].
 *
 * Types ride the same recursion as names, position for position, so a layout
 * that resolves a variable to column N also reports column N's type.  Any
 * column whose type could not be established is WL_IR_COLTYPE_UNKNOWN
 * (Issue #962).
 *
 * @out is filled on success and left empty on failure; the caller frees it
 * with col_ctx_free() either way.  Returns 0 on success, -1 on failure.
 */
static int
collect_output_columns(const wirelog_ir_node_t *node, col_ctx_t *out)
{
    memset(out, 0, sizeof(*out));

    if (!node)
        return -1;

    switch (node->type) {
    case WIRELOG_IR_SCAN:
    case WIRELOG_IR_COMPOUND_INLINE:
    case WIRELOG_IR_COMPOUND_SIDE: {
        if (col_ctx_alloc(out, node->column_count) != 0)
            return -1;
        for (uint32_t i = 0; i < out->count; i++) {
            const char *name
                = node->column_names ? node->column_names[i] : NULL;
            if (name) {
                out->names[i] = dup_str(name);
                if (!out->names[i]) {
                    col_ctx_free(out);
                    return -1;
                }
            }
            out->types[i] = node->column_types ? node->column_types[i]
                                               : WL_IR_COLTYPE_UNKNOWN;
        }
        return 0;
    }

    case WIRELOG_IR_ANTIJOIN:
    case WIRELOG_IR_SEMIJOIN: {
        /* SEMIJOIN/ANTIJOIN filter their left input: col_op_semijoin and
         * col_op_antijoin emit the left columns only (optionally narrowed
         * by the node's own projection).  Concatenating the right child's
         * columns here would shift every column index resolved above this
         * node -- join keys and fused projections alike.  Out-of-range
         * "colN" names then silently resolve to column 0 in the evaluator,
         * so the rule joins on the wrong column and under-derives (#955). */
        col_ctx_t left;
        memset(&left, 0, sizeof(left));
        if (node->child_count == 0
            || collect_output_columns(node->children[0], &left) != 0) {
            col_ctx_free(&left);
            return -1;
        }
        /* Only SEMIJOIN carries its projection into the plan operator
         * (see translate_ir_node); ANTIJOIN always emits all left columns. */
        if (node->type == WIRELOG_IR_SEMIJOIN && node->project_count > 0
            && node->project_indices) {
            col_ctx_t proj;
            if (col_ctx_alloc(&proj, node->project_count) != 0) {
                col_ctx_free(&left);
                return -1;
            }
            for (uint32_t i = 0; i < proj.count; i++) {
                uint32_t idx = node->project_indices[i];
                if (idx < left.count) {
                    if (left.names[idx]) {
                        proj.names[i] = dup_str(left.names[idx]);
                        if (!proj.names[i]) {
                            col_ctx_free(&proj);
                            col_ctx_free(&left);
                            return -1;
                        }
                    }
                    proj.types[i] = col_ctx_type_at(&left, idx);
                }
            }
            col_ctx_free(&left);
            *out = proj;
            return 0;
        }
        *out = left;
        return 0;
    }

    case WIRELOG_IR_JOIN: {
        /* Concatenate left child columns + right child columns */
        col_ctx_t left, right;
        memset(&left, 0, sizeof(left));
        memset(&right, 0, sizeof(right));
        if (node->child_count == 0
            || collect_output_columns(node->children[0], &left) != 0
            || (node->child_count > 1
            && collect_output_columns(node->children[1], &right) != 0)) {
            col_ctx_free(&left);
            col_ctx_free(&right);
            return -1;
        }

        if (col_ctx_alloc(out, left.count + right.count) != 0) {
            col_ctx_free(&left);
            col_ctx_free(&right);
            return -1;
        }
        for (uint32_t i = 0; i < left.count; i++) {
            out->names[i] = left.names[i]; /* transfer ownership */
            left.names[i] = NULL;
            out->types[i] = col_ctx_type_at(&left, i);
        }
        for (uint32_t i = 0; i < right.count; i++) {
            out->names[left.count + i] = right.names[i]; /* transfer */
            right.names[i] = NULL;
            out->types[left.count + i] = col_ctx_type_at(&right, i);
        }
        col_ctx_free(&left);
        col_ctx_free(&right);
        return 0;
    }

    case WIRELOG_IR_PROJECT:
        /* Intermediate PROJECT nodes (inserted by jpp passes) have
         * column_names set but no project_exprs. Return their projected
         * output column layout so that downstream join key resolution
         * sees the correct (narrowed) column set. */
        if (!node->project_exprs && node->column_names
            && node->column_count > 0) {
            /* The node records the projected names but not their types, so
             * the types are recovered from the child through
             * project_indices -- the same mapping the MAP operator applies
             * at run time.  Without this the whole layout above any jpp
             * projection would read as untyped. */
            col_ctx_t child;
            memset(&child, 0, sizeof(child));
            if (node->child_count == 0
                || collect_output_columns(node->children[0], &child) != 0) {
                col_ctx_free(&child);
                return -1;
            }

            if (col_ctx_alloc(out, node->column_count) != 0) {
                col_ctx_free(&child);
                return -1;
            }
            for (uint32_t i = 0; i < out->count; i++) {
                if (node->column_names[i]) {
                    out->names[i] = dup_str(node->column_names[i]);
                    if (!out->names[i]) {
                        col_ctx_free(out);
                        col_ctx_free(&child);
                        return -1;
                    }
                }
                if (node->project_indices && i < node->project_count)
                    out->types[i]
                        = col_ctx_type_at(&child, node->project_indices[i]);
                else
                    out->types[i]
                        = col_ctx_lookup_type(&child, node->column_names[i]);
            }
            col_ctx_free(&child);
            return 0;
        }
        /* Head PROJECT nodes have project_exprs; delegate to child so
         * resolve_project_indices can map expression variables to child
         * column positions. */
        if (node->child_count > 0)
            return collect_output_columns(node->children[0], out);
        return -1;

    case WIRELOG_IR_FILTER:
    case WIRELOG_IR_FLATMAP:
    case WIRELOG_IR_AGGREGATE:
    case WIRELOG_IR_UNION:
        /* Delegate to first child */
        if (node->child_count > 0)
            return collect_output_columns(node->children[0], out);
        return -1;
    }

    return -1;
}

/**
 * Resolve PROJECT expression variable names to column indices.
 * Uses the child node's output column layout to map var names to positions.
 * Returns allocated uint32_t array of indices, or NULL on failure.
 */
static uint32_t *
resolve_project_indices(const wirelog_ir_node_t *project_node)
{
    if (!project_node || project_node->project_count == 0
        || !project_node->project_exprs)
        return NULL;

    /* Get child's column layout */
    const wirelog_ir_node_t *child = NULL;
    if (project_node->child_count > 0)
        child = project_node->children[0];

    col_ctx_t ctx;
    if (collect_output_columns(child, &ctx) != 0) {
        col_ctx_free(&ctx);
        return NULL;
    }

    uint32_t pc = project_node->project_count;
    uint32_t *indices = (uint32_t *)malloc(pc * sizeof(uint32_t));
    if (!indices) {
        col_ctx_free(&ctx);
        return NULL;
    }

    for (uint32_t i = 0; i < pc; i++) {
        indices[i] = i; /* fallback: identity */
        const wl_ir_expr_t *expr = project_node->project_exprs[i];
        if (expr && expr->type == WL_IR_EXPR_VAR && expr->var_name) {
            for (uint32_t c = 0; c < ctx.count; c++) {
                if (ctx.names[c]
                    && strcmp(ctx.names[c], expr->var_name) == 0) {
                    indices[i] = c;
                    break;
                }
            }
        }
    }

    col_ctx_free(&ctx);
    return indices;
}

/**
 * Resolve a join key variable name to "colN" format using the child's
 * column layout.  An explicit, in-range colN is also accepted because some
 * optimizer passes have already rewritten names to positional form.  An
 * unknown name is an error; silently selecting col0 changes query semantics.
 */
static char *
resolve_key_to_colN(const char *key_name, const wirelog_ir_node_t *child)
{
    char buf[32];
    if (!key_name || !child)
        return NULL;

    col_ctx_t ctx = { 0 };
    if (collect_output_columns(child, &ctx) != 0)
        return NULL;

    uint32_t idx = UINT32_MAX;
    for (uint32_t c = 0; c < ctx.count; c++) {
        if (ctx.names[c] && strcmp(ctx.names[c], key_name) == 0) {
            idx = c;
            break;
        }
    }

    if (idx == UINT32_MAX) {
        const char *digits = key_name;
        if (strncmp(digits, "col", 3) == 0 && digits[3] != '\0') {
            char *end = NULL;
            errno = 0;
            unsigned long explicit_idx = strtoul(digits + 3, &end, 10);
            if (errno == 0 && end != digits + 3 && *end == '\0'
                && explicit_idx <= UINT32_MAX
                && explicit_idx < ctx.count)
                idx = (uint32_t)explicit_idx;
        }
    }
    col_ctx_free(&ctx);
    if (idx == UINT32_MAX)
        return NULL;

    snprintf(buf, sizeof(buf), "col%u", idx);
    return dup_str(buf);
}

/**
 * Resolve an array of join key variable names to "colN" format.
 * Returns a newly allocated array of strings.
 */
static char **
resolve_keys_to_colN(char **keys, uint32_t count,
    const wirelog_ir_node_t *child)
{
    if (!keys || count == 0)
        return NULL;
    char **out = (char **)malloc(count * sizeof(char *));
    if (!out)
        return NULL;
    for (uint32_t i = 0; i < count; i++) {
        out[i] = resolve_key_to_colN(keys[i], child);
        if (!out[i]) {
            for (uint32_t j = 0; j < i; j++)
                free(out[j]);
            free((void *)out);
            return NULL;
        }
    }
    return out;
}

/**
 * Like unwrap_filters(), but also collects FILTER predicates encountered
 * during traversal and serializes them into out_buf.  Multiple FILTER
 * predicates are combined using bitwise-AND (BAND), which is correct for
 * boolean (0/1) comparison results.
 *
 * Used to capture constant-argument filters on the right child of
 * JOIN/ANTIJOIN/SEMIJOIN operators.  out_buf->data is NULL and size 0
 * when no FILTER predicates are present.
 */
static const wirelog_ir_node_t *
unwrap_filters_collect(const wirelog_ir_node_t *node,
    wl_plan_expr_buffer_t *out_buf)
{
    /* Collect up to 32 filter_expr pointers while traversing FILTER chain */
    const wl_ir_expr_t *filt_exprs[32];
    uint32_t nfilts = 0;

    while (node && node->type == WIRELOG_IR_FILTER && node->child_count > 0) {
        if (node->filter_expr) {
            if (nfilts < 32) {
                filt_exprs[nfilts++] = node->filter_expr;
            } else {
                fprintf(stderr,
                    "warning: right_filter: >32 nested FILTERs, "
                    "truncating\n");
            }
        }
        node = node->children[0];
    }

    out_buf->data = NULL;
    out_buf->size = 0;

    if (nfilts == 0)
        return node;

    /* Resolve column names from the base SCAN for variable resolution */
    col_ctx_t ctx;
    collect_output_columns(node, &ctx);

    expr_buf_t combined;
    if (expr_buf_init(&combined) != 0)
        goto cleanup;

    bool ok = true;
    for (uint32_t i = 0; i < nfilts && ok; i++) {
        expr_buf_t tmp;
        if (expr_buf_init(&tmp) != 0) {
            ok = false;
            break;
        }
        if (serialize_expr(&tmp, filt_exprs[i], &ctx) != 0) {
            free(tmp.data);
            ok = false;
            break;
        }
        if (i == 0) {
            /* First predicate: replace empty combined with serialized expr */
            free(combined.data);
            combined = tmp;
        } else {
            /* Subsequent predicate: append bytes then BAND to form conjunction.
             * BAND combines predicates: correct because comparisons return {0,1} */
            if (expr_buf_push_bytes(&combined, tmp.data, tmp.size) != 0
                || expr_buf_push_u8(
                    &combined, (uint8_t)WL_PLAN_EXPR_ARITH_BAND)
                != 0) {
                free(tmp.data);
                ok = false;
                break;
            }
            free(tmp.data);
        }
    }

    if (ok) {
        out_buf->data = combined.data;
        out_buf->size = combined.size;
    } else {
        free(combined.data);
    }

cleanup:
    col_ctx_free(&ctx);
    return node;
}

/**
 * Recursively translate an IR node tree into plan operators.
 * Operators are emitted in post-order (children first) to form a
 * stack-machine sequence.
 */
static int
translate_ir_node(const wirelog_ir_node_t *node, op_list_t *ops)
{
    if (!node)
        return -1;

    switch (node->type) {
    case WIRELOG_IR_SCAN:
    case WIRELOG_IR_COMPOUND_INLINE:
    case WIRELOG_IR_COMPOUND_SIDE: {
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_VARIABLE;
        op->relation_name = dup_str(node->relation_name);
        if (node->relation_name && !op->relation_name)
            return -1;
        return 0;
    }

    case WIRELOG_IR_PROJECT: {
        /* Translate child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_MAP;
        op->project_count = node->project_count;
        if (node->project_indices) {
            op->project_indices
                = dup_indices(node->project_indices, node->project_count);
            if (node->project_count > 0 && !op->project_indices)
                return -1;
        } else if (node->project_count > 0) {
            /* Resolve variable names to column indices from child layout */
            uint32_t *resolved = resolve_project_indices(node);
            if (resolved) {
                op->project_indices = resolved;
            } else {
                /* Fallback: synthesize identity [0..n-1] */
                uint32_t *ids = (uint32_t *)malloc(node->project_count
                        * sizeof(uint32_t));
                if (!ids)
                    return -1;
                for (uint32_t pi = 0; pi < node->project_count; pi++)
                    ids[pi] = pi;
                op->project_indices = ids;
            }
        }
        /* Serialize project expressions if present */
        if (node->project_exprs && node->project_count > 0) {
            op->map_exprs = (wl_plan_expr_buffer_t *)calloc(
                node->project_count, sizeof(wl_plan_expr_buffer_t));
            if (!op->map_exprs)
                return -1;
            op->map_expr_count = node->project_count;
            /* Collect the child column layout for name and type resolution */
            col_ctx_t child_ctx;
            const wirelog_ir_node_t *child0
                = (node->child_count > 0) ? node->children[0] : NULL;
            collect_output_columns(child0, &child_ctx);
            for (uint32_t i = 0; i < node->project_count; i++) {
                if (node->project_exprs[i]) {
                    if (serialize_expr_to_buffer_ctx(
                            node->project_exprs[i], &child_ctx,
                            &op->map_exprs[i])
                        != 0) {
                        col_ctx_free(&child_ctx);
                        return -1;
                    }
                }
            }
            col_ctx_free(&child_ctx);
        }
        return 0;
    }

    case WIRELOG_IR_FILTER: {
        /* Translate child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_FILTER;
        {
            col_ctx_t filt_ctx;
            const wirelog_ir_node_t *fchild
                = (node->child_count > 0) ? node->children[0] : NULL;
            collect_output_columns(fchild, &filt_ctx);
            int rc = serialize_expr_to_buffer_ctx(
                node->filter_expr, &filt_ctx, &op->filter_expr);
            col_ctx_free(&filt_ctx);
            if (rc != 0)
                return -1;
        }
        return 0;
    }

    case WIRELOG_IR_JOIN: {
        /* Left child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_JOIN;
        if (node->child_count > 1 && node->children[1]) {
            const wirelog_ir_node_t *rn
                = unwrap_filters_collect(node->children[1],
                    &op->right_filter_expr);
            /* right_relation is a relation *name*, so a composite right
             * child (JOIN/ANTIJOIN/SEMIJOIN/...) cannot be represented at
             * all.  Emitting NULL here yields an operator that matches
             * nothing and a silently empty result -- fail the plan instead
             * (Issue #989/#993). */
            if (!rn || !rn->relation_name) {
                WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                    "JOIN right child (IR node type %d) carries no relation "
                    "name: the plan cannot represent a right-deep join",
                    (int)(rn ? rn->type : node->children[1]->type));
                return -1;
            }
            op->right_relation = dup_str(rn->relation_name);
        }
        /* Resolve join key variable names to "colN" positional format */
        op->left_keys = (const char *const *)resolve_keys_to_colN(
            node->join_left_keys, node->join_key_count,
            node->child_count > 0 ? node->children[0] : NULL);
        op->right_keys = (const char *const *)resolve_keys_to_colN(
            node->join_right_keys, node->join_key_count,
            node->child_count > 1 ? node->children[1] : NULL);
        op->key_count = node->join_key_count;
        if (op->key_count > 0 && (!op->left_keys || !op->right_keys))
            return -1;
        return 0;
    }

    case WIRELOG_IR_ANTIJOIN: {
        /* Left child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_ANTIJOIN;
        if (node->child_count > 1 && node->children[1]) {
            const wirelog_ir_node_t *rn
                = unwrap_filters_collect(node->children[1],
                    &op->right_filter_expr);
            if (!rn || !rn->relation_name) {
                WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                    "ANTIJOIN right child (IR node type %d) carries no "
                    "relation name: the plan cannot represent a "
                    "right-deep join",
                    (int)(rn ? rn->type : node->children[1]->type));
                return -1;
            }
            op->right_relation = dup_str(rn ? rn->relation_name : NULL);
        } else {
            WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                "ANTIJOIN right child is missing: the plan cannot "
                "represent a right-deep join");
            return -1;
        }
        op->left_keys = (const char *const *)resolve_keys_to_colN(
            node->join_left_keys, node->join_key_count,
            node->child_count > 0 ? node->children[0] : NULL);
        op->right_keys = (const char *const *)resolve_keys_to_colN(
            node->join_right_keys, node->join_key_count,
            node->child_count > 1 ? node->children[1] : NULL);
        op->key_count = node->join_key_count;
        if (op->key_count > 0 && (!op->left_keys || !op->right_keys))
            return -1;
        return 0;
    }

    case WIRELOG_IR_SEMIJOIN: {
        /* Left child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_SEMIJOIN;
        if (node->child_count > 1 && node->children[1]) {
            const wirelog_ir_node_t *rn
                = unwrap_filters_collect(node->children[1],
                    &op->right_filter_expr);
            if (!rn || !rn->relation_name) {
                WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                    "SEMIJOIN right child (IR node type %d) carries no "
                    "relation name: the plan cannot represent a "
                    "right-deep join",
                    (int)(rn ? rn->type : node->children[1]->type));
                return -1;
            }
            op->right_relation = dup_str(rn ? rn->relation_name : NULL);
        } else {
            WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                "SEMIJOIN right child is missing: the plan cannot "
                "represent a right-deep join");
            return -1;
        }
        op->left_keys = (const char *const *)resolve_keys_to_colN(
            node->join_left_keys, node->join_key_count,
            node->child_count > 0 ? node->children[0] : NULL);
        op->right_keys = (const char *const *)resolve_keys_to_colN(
            node->join_right_keys, node->join_key_count,
            node->child_count > 1 ? node->children[1] : NULL);
        op->key_count = node->join_key_count;
        if (op->key_count > 0 && (!op->left_keys || !op->right_keys))
            return -1;
        op->project_indices
            = dup_indices(node->project_indices, node->project_count);
        op->project_count = node->project_count;
        return 0;
    }

    case WIRELOG_IR_AGGREGATE: {
        /*
         * average() is not implemented and is refused here rather than
         * answered wrongly (Issue #978).
         *
         * col_op_reduce() seeds each group with the group's first operand
         * and its update switch has arms for COUNT/SUM/MIN/MAX only, so
         * WIRELOG_AGG_AVG fell through `default: break;` and the seed was
         * returned untouched.  The answer followed scan order rather than
         * the data -- val(1,9). val(1,5). val(1,2). gave 9, and reordering
         * the same three facts gave 1 -- with exit status 0 and no
         * diagnostic.
         *
         * Implementing it needs a return type this engine does not have.
         * Every value is an int64_t (columnar/internal.h), and while
         * WIRELOG_TYPE_FLOAT exists in the public enum it is vestigial: the
         * lexer has type keywords for int32/int64/string/symbol only and no
         * decimal literal, so neither `.decl v(x: float)` nor `1.5` parses.
         * The only implementable semantics today is truncating integer
         * division, and that is the one choice that cannot be corrected
         * later without silently changing existing programs' numbers --
         * whereas widening a rejection to a real mean accepts strictly more
         * programs and rewrites none.  Precedent agrees: Soufflé refuses
         * integer operands to mean() outright, and PostgreSQL, SQLite,
         * MySQL and cozo all widen the result type rather than truncate.
         * This follows the sequence adopted for #973: reject first, support
         * later.
         *
         * Placed at lowering, not in the lexer: `average`/`AVG` keep
         * tokenizing, the AST keeps its AGGREGATE node and the public
         * WIRELOG_AGG_AVG stays as it is, so nothing about the surface
         * syntax has to be un-done when float arrives.
         */
        if (node->agg_fn == WIRELOG_AGG_AVG) {
            WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                "'average' is not supported: every value is a 64-bit "
                "integer, so there is no type to return a mean in. Compute "
                "it from sum and count instead: "
                "s(g, sum(v)) :- val(g, v). "
                "c(g, count(v)) :- val(g, v). "
                "t(g, x / y) :- s(g, x), c(g, y).");
            return -1;
        }

        /* Translate child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_REDUCE;
        op->agg_fn = node->agg_fn;
        op->group_by_indices
            = dup_indices(node->group_by_indices, node->group_by_count);
        op->group_by_count = node->group_by_count;
        op->aggregate_index = node->aggregate_index;
        if (node->agg_expr) {
            const wirelog_ir_node_t *child0
                = (node->child_count > 0) ? node->children[0] : NULL;
            col_ctx_t agg_ctx;
            if (node->column_names && node->column_count > 0) {
                /*
                 * The AGGREGATE node records the body's variable names but
                 * no types, and it is the one serialize_expr() entry point
                 * that does not take its layout from collect_output_columns()
                 * -- so without this the aggregated expression would be the
                 * only untyped expression in the plan.  The names here are
                 * the body variables, which is exactly what the child's
                 * layout is keyed by, so the types come across by name.
                 */
                col_ctx_t child_ctx;
                collect_output_columns(child0, &child_ctx);
                if (col_ctx_alloc(&agg_ctx, node->column_count) != 0) {
                    col_ctx_free(&child_ctx);
                    return -1;
                }
                for (uint32_t i = 0; i < agg_ctx.count; i++) {
                    agg_ctx.names[i] = dup_str(node->column_names[i]);
                    /* Positional first: the AGGREGATE's column list is the
                     * body layout verbatim, and agg_expr's variables were
                     * rewritten to "colN" against that same list.  Fall back
                     * to the name only where the child is narrower. */
                    agg_ctx.types[i] = (i < child_ctx.count)
                        ? col_ctx_type_at(&child_ctx, i)
                        : col_ctx_lookup_type(&child_ctx,
                            node->column_names[i]);
                }
                col_ctx_free(&child_ctx);
            } else {
                collect_output_columns(child0, &agg_ctx);
            }
            int agg_rc = serialize_expr_to_buffer_ctx(node->agg_expr,
                    &agg_ctx, &op->agg_expr);
            /*
             * The operand's domain, which is what tells REDUCE whether
             * MIN/MAX order by number or by string (Issue #965).  It has to
             * come from expr_result_type() rather than from a column lookup:
             * min(to_upper(v)) parses and runs, and its operand is a
             * runtime-interned id belonging to no column at all.
             */
            op->agg_operand_type = agg_operand_from_coltype(
                expr_result_type(node->agg_expr, &agg_ctx));
            col_ctx_free(&agg_ctx);
            if (agg_rc != 0)
                return -1;
        }

        /*
         * Say so once, here, when an ordering aggregate cannot be typed --
         * matching the ordering-comparison diagnostic of #962 rather than
         * logging per row.  As there, refusing to lower these would break
         * programs that work today: undeclared relations have no column
         * types at all, and min() over an undeclared integer relation
         * evaluates perfectly well.
         */
        if ((op->agg_fn == WIRELOG_AGG_MIN || op->agg_fn == WIRELOG_AGG_MAX)
            && op->agg_operand_type == WL_PLAN_AGG_OPERAND_UNKNOWN) {
            WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_WARN,
                "'%s' over an operand with no established type: reducing by "
                "interned id, not by string. Declare the relation "
                "(.decl R(c: symbol)) if the column holds symbols",
                op->agg_fn == WIRELOG_AGG_MIN ? "min" : "max");
        }

        /* Record whether the relation as a whole may be reduced under this
         * aggregate, while the operator is still an operator (Issue #975).
         * Last statement of the arm: nothing below pushes, so @op is still
         * the operator just emitted. */
        agg_spec_observe(ops, op);
        return 0;
    }

    case WIRELOG_IR_UNION: {
        /* Each child is translated, then CONCAT + CONSOLIDATE */
        for (uint32_t i = 0; i < node->child_count; i++) {
            int crc = translate_ir_node(node->children[i], ops);
            if (crc != 0)
                return -1;
            /* After the second+ child, emit CONCAT to merge with previous */
            if (i > 0) {
                wl_plan_op_t *concat = op_list_push(ops);
                if (!concat)
                    return -1;
                concat->op = WL_PLAN_OP_CONCAT;
            }
        }
        /* Emit CONSOLIDATE to deduplicate */
        if (node->child_count > 0) {
            wl_plan_op_t *consol = op_list_push(ops);
            if (!consol)
                return -1;
            consol->op = WL_PLAN_OP_CONSOLIDATE;
        }
        return 0;
    }

    case WIRELOG_IR_FLATMAP: {
        /* Decompose into FILTER + MAP per ARCHITECTURE.md:267 */
        /* Translate child (source) first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }

        /* FILTER op (from filter_expr) */
        if (node->filter_expr) {
            wl_plan_op_t *filt = op_list_push(ops);
            if (!filt)
                return -1;
            filt->op = WL_PLAN_OP_FILTER;
            col_ctx_t filt2_ctx;
            const wirelog_ir_node_t *fchild2
                = (node->child_count > 0) ? node->children[0] : NULL;
            collect_output_columns(fchild2, &filt2_ctx);
            int frc = serialize_expr_to_buffer_ctx(node->filter_expr,
                    &filt2_ctx, &filt->filter_expr);
            col_ctx_free(&filt2_ctx);
            if (frc != 0)
                return -1;
        }

        /* MAP op (from project_indices/project_count) */
        if (node->project_count > 0) {
            wl_plan_op_t *map = op_list_push(ops);
            if (!map)
                return -1;
            map->op = WL_PLAN_OP_MAP;
            map->project_count = node->project_count;
            if (node->project_indices) {
                map->project_indices
                    = dup_indices(node->project_indices, node->project_count);
                if (!map->project_indices)
                    return -1;
            } else {
                /* Synthesize identity indices */
                uint32_t *ids = (uint32_t *)malloc(node->project_count
                        * sizeof(uint32_t));
                if (!ids)
                    return -1;
                for (uint32_t pi = 0; pi < node->project_count; pi++)
                    ids[pi] = pi;
                map->project_indices = ids;
            }
            /* Serialize project expressions if present */
            if (node->project_exprs) {
                map->map_exprs = (wl_plan_expr_buffer_t *)calloc(
                    node->project_count, sizeof(wl_plan_expr_buffer_t));
                if (!map->map_exprs)
                    return -1;
                map->map_expr_count = node->project_count;
                /* Collect the child column layout for name/type resolution */
                col_ctx_t child_ctx2;
                const wirelog_ir_node_t *child2
                    = (node->child_count > 0) ? node->children[0] : NULL;
                collect_output_columns(child2, &child_ctx2);
                for (uint32_t i = 0; i < node->project_count; i++) {
                    if (node->project_exprs[i]) {
                        if (serialize_expr_to_buffer_ctx(
                                node->project_exprs[i], &child_ctx2,
                                &map->map_exprs[i])
                            != 0) {
                            col_ctx_free(&child_ctx2);
                            return -1;
                        }
                    }
                }
                col_ctx_free(&child_ctx2);
            }
        }
        return 0;
    }
    }

    return -1; /* unknown node type */
}

/* ======================================================================== */
/* Free helpers                                                             */
/* ======================================================================== */

#if ENABLE_K_FUSION
static void
free_k_fusion_opaque(wl_plan_op_t *op); /* forward declaration */
#endif
static void
free_lftj_opaque(wl_plan_op_t *op); /* forward declaration */
static void
free_exchange_opaque(wl_plan_op_t *op); /* forward declaration */

static void
free_op(wl_plan_op_t *op)
{
    free((void *)op->relation_name);
    free((void *)op->right_relation);

    if (op->left_keys) {
        for (uint32_t i = 0; i < op->key_count; i++)
            free((void *)op->left_keys[i]);
        free((void *)op->left_keys);
    }
    if (op->right_keys) {
        for (uint32_t i = 0; i < op->key_count; i++)
            free((void *)op->right_keys[i]);
        free((void *)op->right_keys);
    }

    free((void *)op->project_indices);
    free(op->filter_expr.data);
    free(op->right_filter_expr.data);
    free(op->agg_expr.data);
    free((void *)op->group_by_indices);

    if (op->map_exprs) {
        for (uint32_t i = 0; i < op->map_expr_count; i++)
            free(op->map_exprs[i].data);
        free(op->map_exprs);
    }

    if (op->opaque_data) {
#if ENABLE_K_FUSION
        if (op->op == WL_PLAN_OP_K_FUSION) {
            free_k_fusion_opaque(op);
        } else
#endif
        if (op->op == WL_PLAN_OP_LFTJ) {
            free_lftj_opaque(op);
        } else if (op->op == WL_PLAN_OP_EXCHANGE) {
            free_exchange_opaque(op);
        }
    }
}

static bool
map_op_is_pure_projection(const wl_plan_op_t *op)
{
    if (!op || op->op != WL_PLAN_OP_MAP || op->project_count == 0
        || !op->project_indices)
        return false;
    if (!op->map_exprs || op->map_expr_count == 0)
        return true;

    if (op->map_expr_count != op->project_count)
        return false;
    for (uint32_t i = 0; i < op->map_expr_count; i++) {
        const wl_plan_expr_buffer_t *expr = &op->map_exprs[i];
        if (!expr->data || expr->size == 0)
            continue;
        if (expr->size < 4 || expr->data[0] != WL_PLAN_EXPR_VAR)
            return false;
        uint16_t len;
        memcpy(&len, expr->data + 1, sizeof(len));
        if ((uint32_t)len + 3u != expr->size || len <= 3)
            return false;
        if (expr->data[3] != 'c' || expr->data[4] != 'o'
            || expr->data[5] != 'l')
            return false;
        uint32_t col = 0;
        for (uint32_t p = 6; p < expr->size; p++) {
            if (!isdigit((unsigned char)expr->data[p]))
                return false;
            col = col * 10u + (uint32_t)(expr->data[p] - '0');
        }
        if (col != op->project_indices[i])
            return false;
    }
    return true;
}

static int
compose_join_projection(wl_plan_op_t *join, const wl_plan_op_t *map)
{
    uint32_t *indices = (uint32_t *)malloc(map->project_count
            * sizeof(uint32_t));
    if (!indices)
        return -1;

    for (uint32_t i = 0; i < map->project_count; i++) {
        uint32_t idx = map->project_indices[i];
        if (join->project_count > 0 && join->project_indices) {
            if (idx >= join->project_count) {
                free(indices);
                return -1;
            }
            indices[i] = join->project_indices[idx];
        } else {
            indices[i] = idx;
        }
    }

    free((void *)join->project_indices);
    join->project_indices = indices;
    join->project_count = map->project_count;
    join->materialized = false;
    return 0;
}

static int
rewrite_join_project_fusion_ops(wl_plan_op_t *ops, uint32_t *op_count)
{
    if (!ops || !op_count)
        return 0;

    uint32_t i = 0;
    while (i + 1 < *op_count) {
        wl_plan_op_t *join = &ops[i];
        wl_plan_op_t *map = &ops[i + 1];
        if (join->op != WL_PLAN_OP_JOIN || !map_op_is_pure_projection(map)) {
            i++;
            continue;
        }

        if (compose_join_projection(join, map) != 0)
            return -1;

        free_op(map);
        for (uint32_t j = i + 1; j + 1 < *op_count; j++)
            ops[j] = ops[j + 1];
        memset(&ops[*op_count - 1], 0, sizeof(wl_plan_op_t));
        (*op_count)--;
    }
    return 0;
}

static int
rewrite_join_project_fusion(wl_plan_t *plan)
{
    if (!plan)
        return 0;

    wl_plan_stratum_t *strata = (wl_plan_stratum_t *)plan->strata;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_plan_stratum_t *stratum = &strata[s];
        wl_plan_relation_t *relations
            = (wl_plan_relation_t *)stratum->relations;
        for (uint32_t r = 0; r < stratum->relation_count; r++) {
            wl_plan_relation_t *rel = &relations[r];
            wl_plan_op_t *ops = (wl_plan_op_t *)rel->ops;
            if (rewrite_join_project_fusion_ops(ops, &rel->op_count) != 0)
                return -1;
#if ENABLE_K_FUSION
            for (uint32_t i = 0; i < rel->op_count; i++) {
                if (ops[i].op != WL_PLAN_OP_K_FUSION || !ops[i].opaque_data)
                    continue;
                wl_plan_op_k_fusion_t *meta
                    = (wl_plan_op_k_fusion_t *)ops[i].opaque_data;
                for (uint32_t d = 0; d < meta->k; d++) {
                    if (rewrite_join_project_fusion_ops(meta->k_ops[d],
                        &meta->k_op_counts[d]) != 0)
                        return -1;
                }
            }
#endif
        }
    }
    return 0;
}

/* ======================================================================== */
/* Multi-Way Delta Expansion (Semi-Naive K-Atom Rewriting with CSE Hints)   */
/* ======================================================================== */

/**
 * Count "delta positions" in a relation plan: the initial VARIABLE op
 * (position 0) plus each subsequent JOIN op whose right_relation is an
 * IDB relation within this stratum.  Returns the count K, and fills
 * delta_pos[] with the op indices of the delta-eligible operators.
 * delta_pos must have room for at least op_count entries.
 */
static uint32_t
count_delta_positions(const wl_plan_op_t *ops, uint32_t op_count,
    const char *const *idb_names, uint32_t idb_count,
    uint32_t *delta_pos)
{
    uint32_t k = 0;
    for (uint32_t i = 0; i < op_count; i++) {
        if (ops[i].op == WL_PLAN_OP_VARIABLE) {
            bool is_idb = false;
            for (uint32_t r = 0; r < idb_count; r++) {
                if (ops[i].relation_name && idb_names[r]
                    && strcmp(ops[i].relation_name, idb_names[r]) == 0) {
                    is_idb = true;
                    break;
                }
            }
            if (is_idb)
                delta_pos[k++] = i;
        } else if (ops[i].op == WL_PLAN_OP_JOIN) {
            bool is_idb = false;
            for (uint32_t r = 0; r < idb_count; r++) {
                if (ops[i].right_relation && idb_names[r]
                    && strcmp(ops[i].right_relation, idb_names[r]) == 0) {
                    is_idb = true;
                    break;
                }
            }
            if (is_idb)
                delta_pos[k++] = i;
        }
    }
    return k;
}

/**
 * Deep-copy LFTJ opaque metadata from src to dst->opaque_data.
 * Returns 0 on success, -1 on allocation failure.
 */
static int
clone_lftj_opaque(const wl_plan_op_t *src, wl_plan_op_t *dst)
{
    const wl_plan_op_lftj_t *sm = (const wl_plan_op_lftj_t *)src->opaque_data;
    if (!sm) {
        dst->opaque_data = NULL;
        return 0;
    }
    wl_plan_op_lftj_t *dm
        = (wl_plan_op_lftj_t *)calloc(1, sizeof(wl_plan_op_lftj_t));
    if (!dm)
        return -1;
    dm->k = sm->k;
    dm->rel_names = (char **)malloc(sm->k * sizeof(char *));
    dm->key_cols = (uint32_t *)malloc(sm->k * sizeof(uint32_t));
    if (!dm->rel_names || !dm->key_cols) {
        free((void *)dm->rel_names);
        free(dm->key_cols);
        free(dm);
        return -1;
    }
    memcpy(dm->key_cols, sm->key_cols, sm->k * sizeof(uint32_t));
    for (uint32_t i = 0; i < sm->k; i++) {
        dm->rel_names[i] = dup_str(sm->rel_names[i]);
        if (!dm->rel_names[i]) {
            for (uint32_t j = 0; j < i; j++)
                free(dm->rel_names[j]);
            free((void *)dm->rel_names);
            free(dm->key_cols);
            free(dm);
            return -1;
        }
    }
    dst->opaque_data = dm;
    return 0;
}

/**
 * Deep-copy EXCHANGE opaque_data (wl_plan_op_exchange_t + key_col_idxs).
 */
static int
clone_exchange_opaque(const wl_plan_op_t *src, wl_plan_op_t *dst)
{
    const wl_plan_op_exchange_t *sm
        = (const wl_plan_op_exchange_t *)src->opaque_data;
    if (!sm) {
        dst->opaque_data = NULL;
        return 0;
    }
    wl_plan_op_exchange_t *dm
        = (wl_plan_op_exchange_t *)calloc(1, sizeof(wl_plan_op_exchange_t));
    if (!dm)
        return -1;
    dm->num_workers = sm->num_workers;
    dm->key_col_count = sm->key_col_count;
    if (sm->key_col_idxs && sm->key_col_count > 0) {
        dm->key_col_idxs
            = (uint32_t *)malloc(sm->key_col_count * sizeof(uint32_t));
        if (!dm->key_col_idxs) {
            free(dm);
            return -1;
        }
        memcpy(dm->key_col_idxs, sm->key_col_idxs,
            sm->key_col_count * sizeof(uint32_t));
    }
    if (sm->edb_key_col_idxs && sm->edb_key_col_count > 0) {
        dm->edb_key_col_count = sm->edb_key_col_count;
        dm->edb_key_col_idxs
            = (uint32_t *)malloc(sm->edb_key_col_count * sizeof(uint32_t));
        if (dm->edb_key_col_idxs) {
            memcpy(dm->edb_key_col_idxs, sm->edb_key_col_idxs,
                sm->edb_key_col_count * sizeof(uint32_t));
        } else {
            dm->edb_key_col_count = 0; /* non-fatal: replicate fallback */
        }
    }
    if (sm->edb_rel_name) {
        dm->edb_rel_name = strdup_safe(sm->edb_rel_name);
        /* non-fatal: replicate fallback if strdup fails */
    }
    dst->opaque_data = dm;
    return 0;
}

/**
 * Deep-copy a single plan op (duplicates all owned strings/buffers).
 */
static int
clone_plan_op(const wl_plan_op_t *src, wl_plan_op_t *dst)
{
    memset(dst, 0, sizeof(*dst));
    dst->op = src->op;
    dst->delta_mode = src->delta_mode;
    dst->materialized = src->materialized;
    dst->agg_fn = src->agg_fn;
    /* Travels with agg_fn.  The join-chain (LFTJ), K-fusion and multiway
     * rewrites all rebuild a relation's ops through here, so a REDUCE that
     * loses its operand domain reverts to id ordering for every rule those
     * passes touch -- in a default build the join-chain rewrite is the one
     * that shows it: detect_lftj_chain() needs a VARIABLE plus two
     * consecutive JOINs, so a three-atom body already triggers it
     * (Issue #965).  The lookup itself is skipped entirely under
     * K-fusion -- see #975, which is a separate defect. */
    dst->agg_operand_type = src->agg_operand_type;
    dst->key_count = src->key_count;
    dst->project_count = src->project_count;
    dst->group_by_count = src->group_by_count;
    dst->aggregate_index = src->aggregate_index;
    dst->map_expr_count = src->map_expr_count;

    if (src->relation_name) {
        dst->relation_name = dup_str(src->relation_name);
        if (!dst->relation_name)
            return -1;
    }
    if (src->right_relation) {
        dst->right_relation = dup_str(src->right_relation);
        if (!dst->right_relation)
            return -1;
    }
    if (src->left_keys && src->key_count > 0) {
        char **lk = (char **)malloc(src->key_count * sizeof(char *));
        if (!lk)
            return -1;
        for (uint32_t i = 0; i < src->key_count; i++) {
            lk[i] = dup_str(src->left_keys[i]);
            if (!lk[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(lk[j]);
                free((void *)lk);
                return -1;
            }
        }
        dst->left_keys = (const char *const *)lk;
    }
    if (src->right_keys && src->key_count > 0) {
        char **rk = (char **)malloc(src->key_count * sizeof(char *));
        if (!rk)
            return -1;
        for (uint32_t i = 0; i < src->key_count; i++) {
            rk[i] = dup_str(src->right_keys[i]);
            if (!rk[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(rk[j]);
                free((void *)rk);
                return -1;
            }
        }
        dst->right_keys = (const char *const *)rk;
    }
    dst->project_indices
        = dup_indices(src->project_indices, src->project_count);
    dst->group_by_indices
        = dup_indices(src->group_by_indices, src->group_by_count);

    if (src->filter_expr.data && src->filter_expr.size > 0) {
        dst->filter_expr.data = (uint8_t *)malloc(src->filter_expr.size);
        if (!dst->filter_expr.data)
            return -1;
        memcpy(dst->filter_expr.data, src->filter_expr.data,
            src->filter_expr.size);
        dst->filter_expr.size = src->filter_expr.size;
    }

    if (src->right_filter_expr.data && src->right_filter_expr.size > 0) {
        dst->right_filter_expr.data
            = (uint8_t *)malloc(src->right_filter_expr.size);
        if (!dst->right_filter_expr.data)
            return -1;
        memcpy(dst->right_filter_expr.data, src->right_filter_expr.data,
            src->right_filter_expr.size);
        dst->right_filter_expr.size = src->right_filter_expr.size;
    }

    if (src->agg_expr.data && src->agg_expr.size > 0) {
        dst->agg_expr.data = (uint8_t *)malloc(src->agg_expr.size);
        if (!dst->agg_expr.data)
            return -1;
        memcpy(dst->agg_expr.data, src->agg_expr.data, src->agg_expr.size);
        dst->agg_expr.size = src->agg_expr.size;
    }

    if (src->map_exprs && src->map_expr_count > 0) {
        dst->map_exprs = (wl_plan_expr_buffer_t *)calloc(
            src->map_expr_count, sizeof(wl_plan_expr_buffer_t));
        if (!dst->map_exprs)
            return -1;
        for (uint32_t i = 0; i < src->map_expr_count; i++) {
            if (src->map_exprs[i].data && src->map_exprs[i].size > 0) {
                dst->map_exprs[i].data
                    = (uint8_t *)malloc(src->map_exprs[i].size);
                if (!dst->map_exprs[i].data)
                    return -1;
                memcpy(dst->map_exprs[i].data, src->map_exprs[i].data,
                    src->map_exprs[i].size);
                dst->map_exprs[i].size = src->map_exprs[i].size;
            }
        }
    }
    if (src->op == WL_PLAN_OP_LFTJ)
        return clone_lftj_opaque(src, dst);
    if (src->op == WL_PLAN_OP_EXCHANGE)
        return clone_exchange_opaque(src, dst);
    return 0;
}

/* Only the non-K-Fusion build calls this: the multi-way expansion site
 * below selects it from the #else of its #if ENABLE_K_FUSION.  Compiling it
 * unconditionally leaves an unused static in every default build. */
#if !ENABLE_K_FUSION
/**
 * Rewrite a single relation plan for multi-way delta expansion with
 * CSE materialization hints.
 *
 * Given original ops [0..op_count-1] with K delta positions, produce
 * K copies of the entire op sequence.  In copy d, delta_pos[d] gets
 * FORCE_DELTA and all other delta positions get FORCE_FULL.
 *
 * Materialization hints:  For the first K-2 delta positions (the
 * "shared prefix"), mark the corresponding JOIN ops as materialized
 * so the evaluator can cache and reuse intermediate join results
 * across copies.  This reduces effective work from K full passes to
 * ~2 passes for large K (e.g., 8-way DOOP CallGraphEdge).
 *
 * Returns new ops array (caller owns) and sets *out_count.
 * Returns NULL on failure.
 */
static wl_plan_op_t *
expand_multiway_delta(const wl_plan_op_t *ops, uint32_t op_count,
    const uint32_t *delta_pos, uint32_t k,
    uint32_t *out_count)
{
    /* Total ops: K copies of original + K CONCATs + 1 CONSOLIDATE
     * (each copy followed by a CONCAT, then CONSOLIDATE at the end) */
    uint32_t total = k * op_count + k + 1;
    wl_plan_op_t *new_ops = (wl_plan_op_t *)calloc(total, sizeof(wl_plan_op_t));
    if (!new_ops)
        return NULL;

    uint32_t wi = 0;

    for (uint32_t d = 0; d < k; d++) {
        for (uint32_t i = 0; i < op_count; i++) {
            if (clone_plan_op(&ops[i], &new_ops[wi]) != 0) {
                for (uint32_t j = 0; j < wi; j++)
                    free_op(&new_ops[j]);
                free(new_ops);
                return NULL;
            }

            /* Set delta_mode: position d gets FORCE_DELTA, all other
            * IDB positions get FORCE_FULL, non-IDB ops stay AUTO. */
            bool is_delta_pos = false;
            for (uint32_t p = 0; p < k; p++) {
                if (delta_pos[p] == i) {
                    new_ops[wi].delta_mode
                        = (p == d) ? WL_DELTA_FORCE_DELTA : WL_DELTA_FORCE_FULL;
                    is_delta_pos = true;
                    break;
                }
            }
            if (!is_delta_pos)
                new_ops[wi].delta_mode = WL_DELTA_AUTO;

            /* Materialization hint: mark the first K-2 JOIN operators
             * (among all delta positions) as materializable. These represent
             * the shared join prefix that the evaluator can cache and reuse
             * across all K copies. */
            if (is_delta_pos && new_ops[wi].op == WL_PLAN_OP_JOIN) {
                /* Count how many JOINs appear before this one in delta_pos */
                uint32_t join_idx = 0;
                for (uint32_t p = 0; p < k; p++) {
                    if (ops[delta_pos[p]].op == WL_PLAN_OP_JOIN) {
                        if (delta_pos[p] == i)
                            break;
                        join_idx++;
                    }
                }
                /* Materialize if this is one of the first K-2 JOINs */
                if (join_idx < k - 2)
                    new_ops[wi].materialized = true;
            }

            wi++;
        }

        /* Add CONCAT after each copy to mark boundaries for the evaluator.
         * The evaluator concatenates K copies before consolidation. */
        memset(&new_ops[wi], 0, sizeof(wl_plan_op_t));
        new_ops[wi].op = WL_PLAN_OP_CONCAT;
        wi++;
    }

    memset(&new_ops[wi], 0, sizeof(wl_plan_op_t));
    new_ops[wi].op = WL_PLAN_OP_CONSOLIDATE;
    wi++;

    /* Per-copy segment skip (issue #370): same logic as K-fusion path.
     * Within each copy's op_count ops, neuter UNION child segments that
     * have FORCE_FULL (IDB ops from wrong rule) but no FORCE_DELTA.
     * EDB-only segments (all AUTO) are preserved for base-case seeding and
     * skipped after the seed pass in outbound TDD evaluation. */
    for (uint32_t d = 0; d < k; d++) {
        uint32_t base = d * (op_count + 1); /* +1 for CONCAT after copy */
        uint32_t depth = 0;
        uint32_t seg_var_idx = UINT32_MAX;
        bool seg_has_delta = false;
        bool seg_has_full = false;

        for (uint32_t i = 0; i < op_count; i++) {
            wl_plan_op_t *op = &new_ops[base + i];

            if (op->op == WL_PLAN_OP_VARIABLE && depth >= 1) {
                if (seg_var_idx != UINT32_MAX && !seg_has_delta) {
                    new_ops[seg_var_idx].delta_mode = seg_has_full
                        ? WL_DELTA_FORCE_EMPTY
                        : WL_DELTA_FORCE_EMPTY_AFTER_SEED;
                }
                seg_var_idx = base + i;
                seg_has_delta = false;
                seg_has_full = false;
            }

            if (op->delta_mode == WL_DELTA_FORCE_DELTA)
                seg_has_delta = true;
            if (op->delta_mode == WL_DELTA_FORCE_FULL)
                seg_has_full = true;

            if (op->op == WL_PLAN_OP_VARIABLE) {
                if (seg_var_idx == UINT32_MAX)
                    seg_var_idx = base + i;
                depth++;
            } else if (op->op == WL_PLAN_OP_CONCAT) {
                depth--;
            }
        }
        if (seg_var_idx != UINT32_MAX && !seg_has_delta) {
            new_ops[seg_var_idx].delta_mode = seg_has_full
                ? WL_DELTA_FORCE_EMPTY
                : WL_DELTA_FORCE_EMPTY_AFTER_SEED;
        }
    }

    *out_count = wi;
    return new_ops;
}
#endif /* !ENABLE_K_FUSION */

#if ENABLE_K_FUSION
/**
 * Free K-fusion metadata stored in op->opaque_data.
 * Releases the K operator sequences and the metadata struct itself.
 * The caller is responsible for the op itself.
 */
static void
free_k_fusion_opaque(wl_plan_op_t *op)
{
    if (!op->opaque_data)
        return;
    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    if (meta->k_ops) {
        for (uint32_t d = 0; d < meta->k; d++) {
            if (meta->k_ops[d]) {
                uint32_t cnt = meta->k_op_counts ? meta->k_op_counts[d] : 0;
                for (uint32_t i = 0; i < cnt; i++)
                    free_op(&meta->k_ops[d][i]);
                free(meta->k_ops[d]);
            }
        }
        free((void *)meta->k_ops);
    }
    free(meta->k_op_counts);
    free(meta);
    op->opaque_data = NULL;
}
#endif /* ENABLE_K_FUSION */

/**
 * Free LFTJ metadata stored in op->opaque_data.
 */
static void
free_lftj_opaque(wl_plan_op_t *op)
{
    if (!op->opaque_data)
        return;
    wl_plan_op_lftj_t *meta = (wl_plan_op_lftj_t *)op->opaque_data;
    if (meta->rel_names) {
        for (uint32_t i = 0; i < meta->k; i++)
            free(meta->rel_names[i]);
        free((void *)meta->rel_names);
    }
    free(meta->key_cols);
    free(meta);
    op->opaque_data = NULL;
}

/**
 * Free Exchange metadata stored in op->opaque_data.
 */
static void
free_exchange_opaque(wl_plan_op_t *op)
{
    if (!op->opaque_data)
        return;
    wl_plan_op_exchange_t *meta = (wl_plan_op_exchange_t *)op->opaque_data;
    free(meta->key_col_idxs);
    free(meta->edb_key_col_idxs);
    free(meta->edb_rel_name);
    free(meta);
    op->opaque_data = NULL;
}

#if ENABLE_K_FUSION
/**
 * Rewrite a single relation plan for K-fusion parallel execution.
 *
 * Creates a single WL_PLAN_OP_K_FUSION operator whose opaque_data
 * points to a wl_plan_op_k_fusion_t containing K independent operator
 * sequences.  In sequence d, delta_pos[d] gets FORCE_DELTA and all
 * other delta positions get FORCE_FULL.  Non-delta ops stay AUTO.
 *
 * Materialization hints are applied identically to expand_multiway_delta():
 * the first K-2 JOIN delta positions are marked materialized for CSE reuse.
 *
 * Returns a 1-element ops array (caller owns) with *out_count = 1.
 * Returns NULL on allocation failure.
 */
static wl_plan_op_t *
expand_multiway_k_fusion(const wl_plan_op_t *ops, uint32_t op_count,
    const uint32_t *delta_pos, uint32_t k,
    uint32_t *out_count)
{
    wl_plan_op_t *result = (wl_plan_op_t *)calloc(1, sizeof(wl_plan_op_t));
    if (!result)
        return NULL;

    wl_plan_op_k_fusion_t *meta
        = (wl_plan_op_k_fusion_t *)calloc(1, sizeof(wl_plan_op_k_fusion_t));
    if (!meta) {
        free(result);
        return NULL;
    }

    meta->k = k;
    meta->k_ops = (wl_plan_op_t **)calloc(k, sizeof(wl_plan_op_t *));
    meta->k_op_counts = (uint32_t *)calloc(k, sizeof(uint32_t));
    if (!meta->k_ops || !meta->k_op_counts) {
        free((void *)meta->k_ops);
        free(meta->k_op_counts);
        free(meta);
        free(result);
        return NULL;
    }

    for (uint32_t d = 0; d < k; d++) {
        /* Allocate op_count + 1: the extra slot holds a CONSOLIDATE op
        * so each worker produces sorted+deduped output, enabling the
        * K-fusion merge to skip an expensive post-completion qsort. */
        wl_plan_op_t *seq
            = (wl_plan_op_t *)calloc(op_count + 1, sizeof(wl_plan_op_t));
        if (!seq)
            goto fail;

        meta->k_ops[d] = seq;
        meta->k_op_counts[d] = op_count + 1;

        for (uint32_t i = 0; i < op_count; i++) {
            if (clone_plan_op(&ops[i], &seq[i]) != 0) {
                /* free successfully cloned ops in this sequence */
                for (uint32_t j = 0; j < i; j++)
                    free_op(&seq[j]);
                free(seq);
                meta->k_ops[d] = NULL;
                meta->k_op_counts[d] = 0;
                goto fail;
            }

            /* Apply delta_mode: position d gets FORCE_DELTA,
             * other IDB positions get FORCE_FULL, rest stay AUTO. */
            bool is_delta_pos = false;
            for (uint32_t p = 0; p < k; p++) {
                if (delta_pos[p] == i) {
                    seq[i].delta_mode
                        = (p == d) ? WL_DELTA_FORCE_DELTA : WL_DELTA_FORCE_FULL;
                    is_delta_pos = true;
                    break;
                }
            }
            if (!is_delta_pos)
                seq[i].delta_mode = WL_DELTA_AUTO;

            /* Materialization hint: first K-2 JOIN delta positions. */

            if (is_delta_pos && seq[i].op == WL_PLAN_OP_JOIN) {
                uint32_t join_idx = 0;
                for (uint32_t p = 0; p < k; p++) {
                    if (ops[delta_pos[p]].op == WL_PLAN_OP_JOIN) {
                        if (delta_pos[p] == i)
                            break;
                        join_idx++;
                    }
                }
                if (join_idx < k - 2)
                    seq[i].materialized = true;
            }
        }
        /* Append CONSOLIDATE so each worker produces sorted+deduped output.
         * This allows col_op_k_fusion to skip the post-completion qsort. */
        seq[op_count].op = WL_PLAN_OP_CONSOLIDATE;
    }

    /* Per-worker segment skip (issue #370): in UNION'd relation plans,
     * each K-fusion worker's ops contain interleaved rule segments
     * delimited by CONCAT/CONSOLIDATE.  When a segment contains no
     * FORCE_DELTA op, its evaluation produces only redundant tuples
     * (full join instead of delta join).  Neuter such segments by
     * setting their leading VARIABLE to FORCE_EMPTY, which pushes
     * an empty relation and causes downstream JOINs to produce nothing.
     *
     * Segment boundaries are detected via stack-depth tracking:
     * each UNION child starts with a VARIABLE that increases depth. */
    for (uint32_t d = 0; d < k; d++) {
        wl_plan_op_t *seq = meta->k_ops[d];
        uint32_t depth = 0;
        uint32_t seg_var_idx = UINT32_MAX;
        bool seg_has_delta = false;
        bool seg_has_full = false;

        for (uint32_t i = 0; i < op_count; i++) {
            /* Detect UNION child boundary: a VARIABLE at depth >= 1
             * means a new child is starting (the previous child already
             * pushed one result onto the stack). */
            if (seq[i].op == WL_PLAN_OP_VARIABLE && depth >= 1) {
                /* Finalize previous segment: neuter only if the segment
                 * has IDB ops (FORCE_FULL) but none are delta-active.
                 * EDB-only segments (all AUTO) are preserved for
                 * base-case seeding and skipped after the seed pass in
                 * outbound TDD evaluation. */
                if (seg_var_idx != UINT32_MAX && !seg_has_delta) {
                    seq[seg_var_idx].delta_mode = seg_has_full
                        ? WL_DELTA_FORCE_EMPTY
                        : WL_DELTA_FORCE_EMPTY_AFTER_SEED;
                }
                seg_var_idx = i;
                seg_has_delta = false;
                seg_has_full = false;
            }

            if (seq[i].delta_mode == WL_DELTA_FORCE_DELTA)
                seg_has_delta = true;
            if (seq[i].delta_mode == WL_DELTA_FORCE_FULL)
                seg_has_full = true;

            if (seq[i].op == WL_PLAN_OP_VARIABLE) {
                if (seg_var_idx == UINT32_MAX)
                    seg_var_idx = i; /* first child's leading VARIABLE */
                depth++;
            } else if (seq[i].op == WL_PLAN_OP_CONCAT) {
                depth--;
            }
        }
        /* Finalize last segment */
        if (seg_var_idx != UINT32_MAX && !seg_has_delta) {
            seq[seg_var_idx].delta_mode = seg_has_full
                ? WL_DELTA_FORCE_EMPTY
                : WL_DELTA_FORCE_EMPTY_AFTER_SEED;
        }
    }

    result->op = WL_PLAN_OP_K_FUSION;
    result->opaque_data = (void *)meta;

    *out_count = 1;
    return result;

fail:
    /* Free any sequences allocated so far */
    for (uint32_t d = 0; d < k; d++) {
        if (meta->k_ops[d]) {
            for (uint32_t i = 0; i < meta->k_op_counts[d]; i++)
                free_op(&meta->k_ops[d][i]);
            free(meta->k_ops[d]);
        }
    }
    free((void *)meta->k_ops);
    free(meta->k_op_counts);
    free(meta);
    free(result);
    return NULL;
}
#endif /* ENABLE_K_FUSION */

/**
 * Apply multi-way delta expansion to all recursive strata in the plan.
 * For each relation in a recursive stratum with K >= 2 IDB body atoms,
 * replaces the relation's ops with K expanded copies annotated with
 * delta_mode and materialization hints.
 */
static void
rewrite_multiway_delta(wl_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_plan_stratum_t *st = (wl_plan_stratum_t *)&plan->strata[s];
        if (!st->is_recursive || !st->relations)
            continue;

        const char **idb_names
            = (const char **)malloc(st->relation_count * sizeof(char *));
        if (!idb_names)
            continue;
        for (uint32_t r = 0; r < st->relation_count; r++)
            idb_names[r] = st->relations[r].name;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            wl_plan_relation_t *rel = (wl_plan_relation_t *)&st->relations[r];
            if (!rel->ops || rel->op_count == 0)
                continue;

            uint32_t *dpos
                = (uint32_t *)malloc(rel->op_count * sizeof(uint32_t));
            if (!dpos)
                continue;
            uint32_t k = count_delta_positions(
                rel->ops, rel->op_count, idb_names, st->relation_count, dpos);

            if (k >= 2) {
                uint32_t new_count = 0;
                wl_plan_op_t *new_ops;
#if ENABLE_K_FUSION
                new_ops = expand_multiway_k_fusion(rel->ops, rel->op_count,
                        dpos, k, &new_count);
#else
                new_ops = expand_multiway_delta(rel->ops, rel->op_count, dpos,
                        k, &new_count);
#endif
                if (new_ops) {
                    for (uint32_t o = 0; o < rel->op_count; o++)
                        free_op((wl_plan_op_t *)&rel->ops[o]);
                    free((void *)rel->ops);
                    rel->ops = new_ops;
                    rel->op_count = new_count;
                }
            }
            free(dpos);
        }
        free((void *)idb_names);
    }
}

/* ======================================================================== */
/* LFTJ Chain Detection and Rewriting                                       */
/* ======================================================================== */

/* Check whether a relation name appears in the plan's EDB list. */
static bool
is_edb_rel(const char *name, const wl_plan_t *plan)
{
    if (!name)
        return false;
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        if (plan->edb_relations[i] && strcmp(plan->edb_relations[i], name) == 0)
            return true;
    }
    return false;
}

/*
 * Detect the length of an LFTJ-eligible chain starting at ops[start]:
 *   ops[start]         : WL_PLAN_OP_VARIABLE, EDB relation
 *   ops[start+1..j-1]  : WL_PLAN_OP_JOIN, key_count==1, EDB right_relation,
 *                        consistent left_keys[0] and right_keys[0].
 * Returns chain length (>= 3) if eligible, else 0.
 * Sets *out_lk and *out_rk to the shared key column name strings.
 */
static uint32_t
lftj_chain_len(const wl_plan_op_t *ops, uint32_t op_count, uint32_t start,
    const wl_plan_t *plan, const char **out_lk, const char **out_rk)
{
    if (start >= op_count || ops[start].op != WL_PLAN_OP_VARIABLE)
        return 0;
    if (!is_edb_rel(ops[start].relation_name, plan))
        return 0;

    const char *lk0 = NULL;
    const char *rk0 = NULL;
    uint32_t j = start + 1;
    while (j < op_count && ops[j].op == WL_PLAN_OP_JOIN && ops[j].key_count == 1
        && ops[j].left_keys && ops[j].right_keys && ops[j].left_keys[0]
        && ops[j].right_keys[0] && ops[j].right_relation
        && is_edb_rel(ops[j].right_relation, plan)
        && ops[j].right_filter_expr.size == 0) {
        if (!lk0) {
            lk0 = ops[j].left_keys[0];
            rk0 = ops[j].right_keys[0];
        } else if (strcmp(ops[j].left_keys[0], lk0) != 0
            || strcmp(ops[j].right_keys[0], rk0) != 0) {
            break;
        }
        j++;
    }

    uint32_t len = j - start;
    if (len < 3 || !lk0)
        return 0;
    *out_lk = lk0;
    *out_rk = rk0;
    return len;
}

/*
 * Build a WL_PLAN_OP_LFTJ operator for the chain ops[start..start+len-1].
 * Returns 0 and fills new_op on success, -1 on allocation failure.
 */
static int
build_lftj_op(const wl_plan_op_t *ops, uint32_t start, uint32_t len,
    const char *lk0, const char *rk0, wl_plan_op_t *new_op)
{
    uint32_t k = len;
    wl_plan_op_lftj_t *meta
        = (wl_plan_op_lftj_t *)calloc(1, sizeof(wl_plan_op_lftj_t));
    if (!meta)
        return -1;

    meta->k = k;
    meta->rel_names = (char **)malloc(k * sizeof(char *));
    meta->key_cols = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!meta->rel_names || !meta->key_cols) {
        free((void *)meta->rel_names);
        free(meta->key_cols);
        free(meta);
        return -1;
    }

    /* Validate "colN" format before parsing */
    if (strncmp(lk0, "col", 3) != 0 || !isdigit((unsigned char)lk0[3])
        || strncmp(rk0, "col", 3) != 0 || !isdigit((unsigned char)rk0[3])) {
        free((void *)meta->rel_names);
        free(meta->key_cols);
        free(meta);
        return -1;
    }

    /* R0 comes from the VARIABLE op; key is left_keys[0] of the first JOIN */
    meta->rel_names[0] = dup_str(ops[start].relation_name);
    meta->key_cols[0] = (uint32_t)strtoul(lk0 + 3, NULL, 10);

    /* R1..Rk-1 from JOIN right_relations; key from shared right_keys[0] */
    uint32_t rk_col = (uint32_t)strtoul(rk0 + 3, NULL, 10);
    for (uint32_t q = 1; q < k; q++) {
        meta->rel_names[q] = dup_str(ops[start + q].right_relation);
        meta->key_cols[q] = rk_col;
    }

    for (uint32_t q = 0; q < k; q++) {
        if (!meta->rel_names[q]) {
            for (uint32_t p = 0; p < k; p++)
                free(meta->rel_names[p]);
            free((void *)meta->rel_names);
            free(meta->key_cols);
            free(meta);
            return -1;
        }
    }

    memset(new_op, 0, sizeof(wl_plan_op_t));
    new_op->op = WL_PLAN_OP_LFTJ;
    new_op->opaque_data = meta;
    return 0;
}

/*
 * Parse a "colN" key string to a physical column index.
 * Returns UINT32_MAX on malformed input (caller must check).
 */
static uint32_t
parse_col_index(const char *key)
{
    if (!key || strncmp(key, "col", 3) != 0 || key[3] == '\0')
        return UINT32_MAX;
    char *end = NULL;
    unsigned long val = strtoul(key + 3, &end, 10);
    if (end == key + 3 || *end != '\0')
        return UINT32_MAX;
    return (uint32_t)val;
}

/*
 * Post-pass: append a top-level WL_PLAN_OP_EXCHANGE at the end of each
 * recursive relation's ops array.  The EXCHANGE carries key_col_idxs
 * derived from the last JOIN's left_keys, enabling tdd_exchange_deltas
 * to hash-partition deltas instead of broadcasting.
 *
 * Must run AFTER rewrite_multiway_delta / K-fusion so that the EXCHANGE
 * op ends up as a top-level op visible to tdd_exchange_deltas's linear
 * scan.  When K-fusion is active, JOINs are buried inside the K_FUSION
 * op's internal sequences — we peek into k_ops[0] to extract metadata.
 *
 * Issue #319 / #361: conservative strategy (one EXCHANGE per relation).
 */
static void
rewrite_insert_exchanges(wl_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_plan_stratum_t *st = (wl_plan_stratum_t *)&plan->strata[s];
        if (!st->is_recursive || !st->relations)
            continue;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            wl_plan_relation_t *rel = (wl_plan_relation_t *)&st->relations[r];
            if (!rel->ops || rel->op_count == 0)
                continue;

            /* Find the best JOIN for EXCHANGE key: prefer a JOIN whose
             * right_relation is IDB in this stratum (correct partition key
             * for 3-body rules like SG); fall back to last JOIN.
             *
             * Track whether IDB is the right operand (idb_on_right):
             *   - IDB on RIGHT (e.g. nsa :- insert × nsa): use right_keys
             *   - IDB on LEFT  (e.g. vbs :- vbs × blankStep): use left_keys
             * The EXCHANGE key must match the IDB columns used in the join,
             * since those are the probe columns in the next iteration. */
            const wl_plan_op_t *best_join = NULL;
            bool idb_on_right = false;

            /* 1) Scan top-level ops backward for JOINs. */
            for (int i = (int)rel->op_count - 1; i >= 0; i--) {
                if (rel->ops[i].op != WL_PLAN_OP_JOIN)
                    continue;
                if (!best_join)
                    best_join = &rel->ops[i]; /* fallback: last JOIN */
                /* Prefer JOIN whose right_relation is IDB in stratum */
                if (rel->ops[i].right_relation) {
                    bool is_idb = false;
                    for (uint32_t rj = 0; rj < st->relation_count; rj++) {
                        if (strcmp(rel->ops[i].right_relation,
                            st->relations[rj].name) == 0) {
                            is_idb = true;
                            break;
                        }
                    }
                    if (is_idb) {
                        best_join = &rel->ops[i];
                        idb_on_right = true;
                        break;
                    }
                }
            }

            /* 2) If no top-level JOIN, peek inside K_FUSION sequence 0. */
            if (!best_join
                && rel->op_count >= 1
                && rel->ops[0].op == WL_PLAN_OP_K_FUSION) {
                const wl_plan_op_k_fusion_t *kf
                    = (const wl_plan_op_k_fusion_t *)rel->ops[0].opaque_data;
                if (kf && kf->k > 0 && kf->k_ops && kf->k_ops[0]) {
                    for (int i = (int)kf->k_op_counts[0] - 1; i >= 0; i--) {
                        if (kf->k_ops[0][i].op != WL_PLAN_OP_JOIN)
                            continue;
                        if (!best_join)
                            best_join = &kf->k_ops[0][i];
                        if (kf->k_ops[0][i].right_relation) {
                            bool is_idb = false;
                            for (uint32_t rj = 0;
                                rj < st->relation_count; rj++) {
                                if (strcmp(kf->k_ops[0][i].right_relation,
                                    st->relations[rj].name) == 0) {
                                    is_idb = true;
                                    break;
                                }
                            }
                            if (is_idb) {
                                best_join = &kf->k_ops[0][i];
                                idb_on_right = true;
                                break;
                            }
                        }
                    }
                }
            }

            if (!best_join || best_join->key_count == 0)
                continue;

            /* Select the correct key source based on IDB position:
             * IDB on right → right_keys (IDB join columns)
             * IDB on left  → left_keys  (IDB join columns) */
            const char *const *exch_keys = idb_on_right
                ? best_join->right_keys : best_join->left_keys;
            if (!exch_keys)
                continue;

            /* Build EXCHANGE metadata from the selected join keys.
             * The keys are columns of the IDB operand that participate
             * in the join.  Since the EXCHANGE partitions the output
             * relation (same schema as IDB), this ensures the partition
             * key matches the recursive join probe key, giving each
             * worker a complete local join. */
            wl_plan_op_exchange_t *meta
                = (wl_plan_op_exchange_t *)calloc(
                    1, sizeof(wl_plan_op_exchange_t));
            if (!meta)
                continue;
            meta->num_workers = 0; /* plan is W-agnostic; eval uses actual W */
            meta->key_col_count = best_join->key_count;
            meta->key_col_idxs = (uint32_t *)malloc(
                meta->key_col_count * sizeof(uint32_t));
            if (!meta->key_col_idxs) {
                free(meta);
                continue;
            }
            bool ok = true;
            for (uint32_t k = 0; k < meta->key_col_count; k++) {
                uint32_t idx = parse_col_index(exch_keys[k]);
                if (idx == UINT32_MAX) {
                    ok = false;
                    break;
                }
                meta->key_col_idxs[k] = idx;
            }
            if (!ok) {
                free(meta->key_col_idxs);
                free(meta);
                continue;
            }

            /* Store EDB-side join columns (the OTHER side from exch_keys).
             * When IDB is on right, EDB keys = left_keys.
             * When IDB is on left, EDB keys = right_keys.
             * tdd_init_workers_hybrid uses these to partition EDB by the
             * join key so each worker scans only 1/W of the EDB. */
            const char *const *edb_keys = idb_on_right
                ? best_join->left_keys : best_join->right_keys;
            if (edb_keys && best_join->key_count > 0) {
                meta->edb_key_col_idxs = (uint32_t *)malloc(
                    best_join->key_count * sizeof(uint32_t));
                if (meta->edb_key_col_idxs) {
                    meta->edb_key_col_count = best_join->key_count;
                    bool edb_ok = true;
                    for (uint32_t k = 0; k < meta->edb_key_col_count; k++) {
                        uint32_t idx = parse_col_index(edb_keys[k]);
                        if (idx == UINT32_MAX) {
                            edb_ok = false;
                            break;
                        }
                        meta->edb_key_col_idxs[k] = idx;
                    }
                    if (!edb_ok) {
                        free(meta->edb_key_col_idxs);
                        meta->edb_key_col_idxs = NULL;
                        meta->edb_key_col_count = 0;
                    }
                }
            }

            /* Identify the EDB relation to partition.
             * IDB on right → EDB is the left operand (find first non-IDB
             *   VARIABLE in the ops feeding the JOIN).
             * IDB on left  → EDB is best_join->right_relation. */
            if (meta->edb_key_col_idxs) {
                const char *edb_name = NULL;
                if (!idb_on_right) {
                    edb_name = best_join->right_relation;
                } else {
                    /* Walk backward from best_join to find the nearest
                     * non-IDB VARIABLE that feeds the JOIN's left side.
                     * Forward scanning picks up base-case VARIABLEs that
                     * feed CONCAT, not the recursive JOIN. */
                    const wl_plan_op_t *scan = rel->ops;
                    uint32_t scan_n = rel->op_count;
                    if (rel->op_count >= 1
                        && rel->ops[0].op == WL_PLAN_OP_K_FUSION) {
                        const wl_plan_op_k_fusion_t *kf =
                            (const wl_plan_op_k_fusion_t *)
                            rel->ops[0].opaque_data;
                        if (kf && kf->k > 0 && kf->k_ops && kf->k_ops[0]) {
                            scan = kf->k_ops[0];
                            scan_n = kf->k_op_counts[0];
                        }
                    }
                    /* Find best_join position in the scan array. */
                    int join_pos = -1;
                    for (uint32_t j = 0; j < scan_n; j++) {
                        if (&scan[j] == best_join) {
                            join_pos = (int)j;
                            break;
                        }
                    }
                    if (join_pos < 0)
                        join_pos = (int)scan_n; /* fallback */
                    /* Walk backward from JOIN to find nearest non-IDB
                     * VARIABLE (the EDB that feeds the JOIN). */
                    for (int j = join_pos - 1; j >= 0; j--) {
                        if (scan[j].op != WL_PLAN_OP_VARIABLE
                            || !scan[j].relation_name)
                            continue;
                        bool is_stratum_idb = false;
                        for (uint32_t rj = 0; rj < st->relation_count; rj++) {
                            if (strcmp(scan[j].relation_name,
                                st->relations[rj].name) == 0) {
                                is_stratum_idb = true;
                                break;
                            }
                        }
                        if (!is_stratum_idb) {
                            edb_name = scan[j].relation_name;
                            break;
                        }
                    }
                }
                /* Only partition if EDB appears exactly once in the
                 * rule body.  Multi-use EDB (e.g. parent in SG 3-body
                 * rules) must remain replicated for correctness. */
                if (edb_name) {
                    uint32_t edb_refs = 0;
                    const wl_plan_op_t *scan2 = rel->ops;
                    uint32_t scan2_n = rel->op_count;
                    if (rel->op_count >= 1
                        && rel->ops[0].op == WL_PLAN_OP_K_FUSION) {
                        const wl_plan_op_k_fusion_t *kf2 =
                            (const wl_plan_op_k_fusion_t *)
                            rel->ops[0].opaque_data;
                        if (kf2 && kf2->k > 0
                            && kf2->k_ops && kf2->k_ops[0]) {
                            scan2 = kf2->k_ops[0];
                            scan2_n = kf2->k_op_counts[0];
                        }
                    }
                    for (uint32_t j = 0; j < scan2_n; j++) {
                        if (scan2[j].relation_name
                            && strcmp(scan2[j].relation_name,
                            edb_name) == 0)
                            edb_refs++;
                        if (scan2[j].right_relation
                            && strcmp(scan2[j].right_relation,
                            edb_name) == 0)
                            edb_refs++;
                    }
                    if (edb_refs <= 1)
                        meta->edb_rel_name = strdup_safe(edb_name);
                }
            }

            /* Append EXCHANGE as new top-level op at end of ops array. */
            wl_plan_op_t *new_ops = (wl_plan_op_t *)realloc(
                (void *)rel->ops,
                (rel->op_count + 1) * sizeof(wl_plan_op_t));
            if (!new_ops) {
                free(meta->key_col_idxs);
                free(meta);
                continue;
            }
            memset(&new_ops[rel->op_count], 0, sizeof(wl_plan_op_t));
            new_ops[rel->op_count].op = WL_PLAN_OP_EXCHANGE;
            new_ops[rel->op_count].opaque_data = meta;
            rel->ops = new_ops;
            rel->op_count++;
        }
    }
}

/*
 * Post-pass: replace EDB-only VARIABLE + (k-1) JOIN chains (k >= 3) with
 * a single WL_PLAN_OP_LFTJ operator.  Run before rewrite_multiway_delta()
 * so no K_FUSION ops are present during cloning.
 */
static void
rewrite_lftj_chains(wl_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_plan_stratum_t *st = (wl_plan_stratum_t *)&plan->strata[s];
        if (!st->relations)
            continue;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            wl_plan_relation_t *rel = (wl_plan_relation_t *)&st->relations[r];
            if (!rel->ops || rel->op_count < 3)
                continue;

            /* Quick scan: is there at least one eligible chain? */
            bool has_chain = false;
            for (uint32_t i = 0; i < rel->op_count && !has_chain; i++) {
                const char *lk0 = NULL, *rk0 = NULL;
                if (lftj_chain_len(rel->ops, rel->op_count, i, plan, &lk0, &rk0)
                    >= 3)
                    has_chain = true;
            }
            if (!has_chain)
                continue;

            /* Build replacement op list (at most rel->op_count entries) */
            wl_plan_op_t *new_ops
                = (wl_plan_op_t *)calloc(rel->op_count, sizeof(wl_plan_op_t));
            if (!new_ops)
                continue;

            uint32_t ni = 0;
            bool ok = true;
            for (uint32_t i = 0; i < rel->op_count && ok;) {
                const char *lk0 = NULL, *rk0 = NULL;
                uint32_t clen = lftj_chain_len(rel->ops, rel->op_count, i, plan,
                        &lk0, &rk0);
                if (clen >= 3) {
                    if (build_lftj_op(rel->ops, i, clen, lk0, rk0, &new_ops[ni])
                        != 0) {
                        ok = false;
                        break;
                    }
                    /* Issue #1032: the only externally observable evidence
                     * that an LFTJ was produced.  build_lftj_op() frees the
                     * chain's operators and keeps only rel_names[], so any
                     * consumer that does not descend into it sees a rule that
                     * reads nothing -- and without this line "the rewrite fired
                     * and the consumer coped" is indistinguishable from "the
                     * rewrite never fired", which is what made the blindness
                     * untestable.  TRACE, so release builds strip it. */
                    WL_LOG(WL_LOG_SEC_ARRANGEMENT, WL_LOG_TRACE,
                        "lftj rewrite relation=%s chain_at=%u k=%u "
                        "left_key=%s right_key=%s",
                        rel->name ? rel->name : "?", i, clen,
                        lk0 ? lk0 : "-", rk0 ? rk0 : "-");
                    ni++;
                    i += clen;
                } else {
                    if (clone_plan_op(&rel->ops[i], &new_ops[ni]) != 0) {
                        ok = false;
                        break;
                    }
                    ni++;
                    i++;
                }
            }

            if (!ok) {
                for (uint32_t o = 0; o < ni; o++)
                    free_op(&new_ops[o]);
                free(new_ops);
                continue;
            }

            /* Swap in the rewritten op list */
            for (uint32_t o = 0; o < rel->op_count; o++)
                free_op((wl_plan_op_t *)&rel->ops[o]);
            free((void *)rel->ops);
            rel->ops = new_ops;
            rel->op_count = ni;
        }
    }
}

/* ======================================================================== */
/* Referenced-relation sets (Issue #1019)                                   */
/* ======================================================================== */

/*
 * plan_refsets_free - Release a wl_plan_stratum_t.rule_refs array of @count
 * entries.  NULL-safe; entries with a NULL names array are skipped.
 */
static void
plan_refsets_free(wl_plan_refset_t *refs, uint32_t count)
{
    if (!refs)
        return;
    for (uint32_t r = 0; r < count; r++) {
        for (uint32_t i = 0; i < refs[r].count; i++)
            free(refs[r].names[i]);
        free((void *)refs[r].names);
    }
    free(refs);
}

/*
 * plan_op_referenced_relation - Name of the relation read by @op, using
 * exactly the predicate the frontier scan in columnar/frontier.c applies:
 * WL_PLAN_OP_VARIABLE contributes relation_name, JOIN/ANTIJOIN/SEMIJOIN
 * contribute right_relation.  Every other operator contributes nothing.
 * Returns NULL when the operator names no relation.
 */
static const char *
plan_op_referenced_relation(const wl_plan_op_t *op)
{
    if (op->op == WL_PLAN_OP_VARIABLE)
        return op->relation_name;
    if (op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_ANTIJOIN
        || op->op == WL_PLAN_OP_SEMIJOIN)
        return op->right_relation;
    return NULL;
}

/*
 * plan_stratum_record_refs - Record, per relation of @st, the de-duplicated
 * set of relation names its operators read (Issue #1019).
 *
 * Must be called while @st->relations still holds the operator list produced
 * by IR lowering.  Two rewrites destroy that list, by different means:
 * rewrite_multiway_delta() replaces it with a K_FUSION operator that holds the
 * real operators in opaque_data (wl_plan_op_k_fusion_t.k_ops[]), whereas
 * rewrite_lftj_chains() replaces the chain with an LFTJ operator whose
 * opaque_data holds only wl_plan_op_lftj_t.rel_names[]/key_cols[] -- the
 * original operators are free_op()d, not carried.  After either rewrite no
 * scan of @st->relations[i].ops can recover the names, and
 * col_compute_affected_strata() concludes that the stratum reads nothing.
 *
 * Returns 0 on success, -1 on allocation failure with @st->rule_refs left NULL
 * (consumers then fall back to the operator scan).
 */
static int
plan_stratum_record_refs(wl_plan_stratum_t *st)
{
    if (st->relation_count == 0 || !st->relations)
        return 0;

    wl_plan_refset_t *refs = (wl_plan_refset_t *)calloc(
        st->relation_count, sizeof(wl_plan_refset_t));
    if (!refs)
        return -1;

    for (uint32_t r = 0; r < st->relation_count; r++) {
        const wl_plan_relation_t *rel = &st->relations[r];
        if (rel->op_count == 0 || !rel->ops)
            continue;

        /* Each operator contributes at most one name, so op_count is a
         * sufficient upper bound before de-duplication. */
        char **names = (char **)calloc(rel->op_count, sizeof(char *));
        if (!names) {
            plan_refsets_free(refs, st->relation_count);
            return -1;
        }
        refs[r].names = names;

        for (uint32_t o = 0; o < rel->op_count; o++) {
            const char *name = plan_op_referenced_relation(&rel->ops[o]);
            if (!name)
                continue;

            bool seen = false;
            for (uint32_t i = 0; i < refs[r].count && !seen; i++) {
                if (strcmp(names[i], name) == 0)
                    seen = true;
            }
            if (seen)
                continue;

            names[refs[r].count] = dup_str(name);
            if (!names[refs[r].count]) {
                plan_refsets_free(refs, st->relation_count);
                return -1;
            }
            refs[r].count++;
        }
    }

    st->rule_refs = refs;
    return 0;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

/*
 * plan_relation_has_reduce - True when @relation still carries a REDUCE
 * operator of its own.
 *
 * Keyed on the operator rather than on wl_plan_relation_t.recursive_agg
 * deliberately.  recursive_agg records only the reductions that can be
 * canonicalised -- a head whose rules disagree on the aggregate function
 * leaves has_spec false -- yet such a relation still reduces per rule, still
 * has its per-iteration content read by a same-stratum consumer, and still
 * answers differently in different builds.  The operator is the thing that
 * exists in every one of those cases.
 */
static bool
plan_relation_has_reduce(const wl_plan_relation_t *relation)
{
    for (uint32_t o = 0; o < relation->op_count; o++) {
        if (relation->ops[o].op == WL_PLAN_OP_REDUCE)
            return true;
    }
    return false;
}

/*
 * plan_relation_reads - True when an operator of @relation names @name,
 * under exactly the predicate plan_stratum_record_refs() records with.
 */
static bool
plan_relation_reads(const wl_plan_relation_t *relation, const char *name)
{
    for (uint32_t o = 0; o < relation->op_count; o++) {
        const char *read = plan_op_referenced_relation(&relation->ops[o]);
        if (read && strcmp(read, name) == 0)
            return true;
    }
    return false;
}

/*
 * validate_recursive_aggregate_consumers - Issue #1021.  A recursive MIN/MAX
 * aggregate may not share an SCC with any other relation.
 *
 * The rule as coded is "no other relation of a recursive stratum may read a
 * relation that carries a REDUCE".  That is the same rule: wl_ir_stratify
 * assigns one stratum per SCC (ir/stratify.c, stratum_id[i] = scc_id[i]), and
 * an SCC with two or more relations is strongly connected, so some other
 * relation of it necessarily reads the aggregate directly.  State it as the
 * SCC rule when explaining it -- "no other relation may reference R"
 * understates how much surface this removes.
 *
 * Why the whole shape and not some narrower predicate.  A same-stratum
 * consumer sees each round's per-rule REDUCE output, before that round's
 * cross-rule domination has run; what it therefore observes is, in general,
 * decided by the evaluation strategy rather than by the program.  "In
 * general" is doing real work in that sentence -- see the last paragraph
 * below.  The issue's repro
 * makes that visible twice over -- the default build answers Big(3) Big(4)
 * over labels that are all 1, and an ENABLE_K_FUSION=0 build corrupts the
 * aggregate itself as well, so two builds give two different wrong answers.
 * It is not merely that the consumer reads the aggregate column
 * non-monotonically: a consumer with no predicate on that column at all
 * (Seen(x) :- Label(x, l)) is configuration-dependent for the same reason.
 *
 * The rule is coarse and knowingly refuses programs that answer correctly
 * today; the CHANGELOG entry for #1021 names five.  The obvious leniency --
 * readmit a consumer that carries its own MIN/MAX -- is unsound, and was
 * measured to be.  Over a propagating a:
 *
 *     a(x, min(v)) :- a(y, v), Edge(y, x).
 *     b(x, min(v)) :- a(x, v), v > 2.
 *     a(x, min(9)) :- b(x, v).
 *
 * yields b(3,3) b(4,3) in both build configurations.  The third rule is
 * load-bearing: it is what puts b in a's SCC.  Drop it and b is a higher
 * stratum, a settles to all-1, and b comes out empty in both builds -- so
 * the shorter spelling of this example proves nothing.  Of the two rows,
 * b(3,3) is the one no final a justifies in either configuration; b(4,3) is
 * unjustified only in the default build, the unfused one leaving a(4,3)
 * behind.
 *
 * There is one class where the outcome *is* predictable from the program,
 * and it is the reason to expect this rule to be narrowed rather than kept.
 * When the aggregated value is functionally determined by the group key --
 * `M(x, min(x)) :- Edge(x, _).` with `Root(x) :- M(x, v).` feeding back --
 * no value is ever dominated away, so a round's content is the fixpoint's
 * content and configuration-dependence is impossible by construction, not
 * merely absent from the samples anyone happened to run.  That program is
 * refused here all the same, because this check reasons about shape and not
 * about functional dependencies.  Recognising the class is #1135's best
 * candidate for a narrowing predicate.
 *
 * Returns 0 when no stratum has the shape, -1 after logging the first that
 * does.
 */
static int
validate_recursive_aggregate_consumers(const wl_plan_t *plan,
    struct wirelog_program *prog)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        if (!stratum->is_recursive || !stratum->relations
            || stratum->relation_count < 2)
            continue;

        for (uint32_t r = 0; r < stratum->relation_count; r++) {
            const wl_plan_relation_t *agg = &stratum->relations[r];
            if (!plan_relation_has_reduce(agg))
                continue;

            for (uint32_t c = 0; c < stratum->relation_count; c++) {
                const wl_plan_relation_t *consumer = &stratum->relations[c];
                if (c == r || !plan_relation_reads(consumer, agg->name))
                    continue;

                /* One remedy, not two.  An earlier draft also offered
                 * "give the consumer its own min/max and no non-monotone
                 * read of the aggregate column".  That is vacuous for every
                 * program that can reach this message: the check inspects
                 * neither the consumer's operators nor its filters, so a
                 * same-SCC consumer is refused whatever it carries -- the
                 * advice would have told the reader to do what they already
                 * do.  It is also wrong on its own terms, because such a
                 * program is configuration-dependent too: over a
                 * propagating a, `b(x, min(v)) :- a(x, v).` with the
                 * feedback rule intact answers b all-1 by default and
                 * b(3,2) b(4,3) under ENABLE_K_FUSION=0.  Whether that
                 * class can ever be readmitted is #1135's question, not a
                 * workaround to hand out here. */
                WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                    "relation '%s' of stratum %u reads recursive aggregate "
                    "relation '%s' from the same stratum: a recursive "
                    "min/max aggregate may not share an SCC with any other "
                    "relation, because a same-SCC consumer observes the "
                    "aggregate's per-iteration content and what it observes "
                    "is configuration-dependent.  Break the feedback edge "
                    "into '%s' so that '%s' lands in a later stratum",
                    consumer->name, stratum->stratum_id, agg->name,
                    agg->name, consumer->name);
                set_plan_error(prog,
                    "relation '%s' of stratum %u reads recursive aggregate "
                    "relation '%s' from the same stratum: a recursive "
                    "min/max aggregate may not share an SCC with any "
                    "other relation; break the feedback edge into '%s'",
                    consumer->name, stratum->stratum_id, agg->name,
                    consumer->name);
                return -1;
            }
        }
    }
    return 0;
}

/* COUNT and SUM are not monotone reducers across a recursive fixpoint.  The
 * plan must reject them before any rewrite can hide the REDUCE operator and
 * otherwise expose a relation whose rows violate its declared functional
 * dependency.  AVG is included defensively; parsed AVG expressions are
 * rejected earlier in translate_ir_node().
 *
 * The second pass, validate_recursive_aggregate_consumers(), rejects a
 * further shape for a related reason (#1021); see the comment on it. */
static int
validate_recursive_aggregates(const wl_plan_t *plan,
    struct wirelog_program *prog)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *stratum = &plan->strata[s];
        if (!stratum->is_recursive || !stratum->relations)
            continue;

        for (uint32_t r = 0; r < stratum->relation_count; r++) {
            const wl_plan_relation_t *relation = &stratum->relations[r];
            for (uint32_t o = 0; o < relation->op_count; o++) {
                const wl_plan_op_t *op = &relation->ops[o];
                if (op->op != WL_PLAN_OP_REDUCE
                    || (op->agg_fn != WIRELOG_AGG_COUNT
                    && op->agg_fn != WIRELOG_AGG_SUM
                    && op->agg_fn != WIRELOG_AGG_AVG))
                    continue;

                WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_ERROR,
                    "recursive aggregate '%s' is not supported in "
                    "relation '%s' of stratum %u: count, sum and average "
                    "are non-monotone across fixpoint iteration",
                    wirelog_agg_fn_str(op->agg_fn), relation->name,
                    stratum->stratum_id);
                set_plan_error(prog,
                    "recursive aggregate '%s' is not supported in relation "
                    "'%s' of stratum %u: count, sum and average are "
                    "non-monotone across fixpoint iteration",
                    wirelog_agg_fn_str(op->agg_fn), relation->name,
                    stratum->stratum_id);
                return -1;
            }
        }
    }

    return validate_recursive_aggregate_consumers(plan, prog);
}

void
wl_plan_free(wl_plan_t *plan)
{
    if (!plan)
        return;

    if (plan->strata) {
        for (uint32_t s = 0; s < plan->stratum_count; s++) {
            wl_plan_stratum_t *st = (wl_plan_stratum_t *)&plan->strata[s];
            if (st->relations) {
                for (uint32_t r = 0; r < st->relation_count; r++) {
                    wl_plan_relation_t *rel
                        = (wl_plan_relation_t *)&st->relations[r];
                    free((void *)rel->name);
                    free(rel->delta_name);
                    if (rel->ops) {
                        for (uint32_t o = 0; o < rel->op_count; o++)
                            free_op((wl_plan_op_t *)&rel->ops[o]);
                        free((void *)rel->ops);
                    }
                }
                free((void *)st->relations);
            }
            /* Issue #1019: parallel to st->relations, so freed with the same
             * count.  relation_count is 0 whenever relations is NULL. */
            plan_refsets_free(st->rule_refs, st->relation_count);
        }
        free((void *)plan->strata);
    }

    if (plan->edb_relations) {
        for (uint32_t i = 0; i < plan->edb_count; i++)
            free((void *)plan->edb_relations[i]);
        free((void *)plan->edb_relations);
    }

    /* Issue #535: free per-EDB graph-column metadata arrays. */
    free((void *)plan->edb_has_graph_column);
    free((void *)plan->edb_graph_col_index);
    free((void *)plan->edb_declared_width);

    free(plan);
}

int
wl_plan_from_program(struct wirelog_program *prog, wl_plan_t **out)
{
    if (!prog)
        return -1;

    prog->plan_error[0] = '\0';
    /* Every failure path has at least a bounded generic diagnostic; the
     * validation failures below replace it with their actionable reason. */
    set_plan_error(prog, "execution plan generation failed");

    if (!out)
        return -1;

    *out = NULL;

    /* Issue #287/#962: plan generation emits WL_LOG diagnostics -- cmp_to_tag()
     * reports ordering comparisons that fall back to intern-id order -- and it
     * runs before any session exists, so the logger would still be at its
     * all-silent default.  wl_log_init() is idempotent on re-entry;
     * col_session_create() calls it for the same reason.  It is not free,
     * though: with WL_LOG_FILE set it reopens the sink each time, so a caller
     * in a tight parse/plan loop pays an fopen+fclose per iteration.
     *
     * Correction (#973): this was never the first stage to *emit*.  Rule
     * lowering (wl_ir_program_convert_rules, ir/program.c) rejects programs
     * and explains why through WL_LOG(WL_LOG_SEC_PARSER, WL_LOG_ERROR, ...)
     * well before this point, so every such message was written into an
     * uninitialized logger and dropped -- WL_LOG=PARSER:1 could not surface
     * them, because parsing was over before anything read the variable.
     * wirelog_parse_string() (ir/api.c) now calls wl_log_init() first, which
     * is why this call is merely idempotent re-entry rather than the one that
     * matters.  Getting a message to a user who sets nothing at all is a
     * further step: the gate is LVL <= threshold and the default
     * WL_LOG_NONE (0) still suppresses WL_LOG_ERROR (1), so that needs the
     * errbuf wirelog_parse_string() builds and discards -- issue #979.
     *
     * Two caveats, both inherited rather than introduced.  It rewrites the
     * sink and thresholds without a lock, so calling it while another
     * thread logs is unsafe -- the same hazard col_session_create() already
     * carries.  And at the release ceiling -Dwirelog_log_max_level=error the
     * WARN sites below compile out entirely, so this call does nothing
     * there. */
    wl_log_init();

    if (wl_exec_plan_gen_preintern_static_strings(prog) != 0)
        return -1;

    wl_plan_t *plan = (wl_plan_t *)calloc(1, sizeof(wl_plan_t));
    if (!plan)
        return -1;

    plan->intern = prog->intern;

    /* ----------------------------------------------------------------
     * Build EDB relation list (relations with no rules / only facts)
     * ---------------------------------------------------------------- */
    uint32_t edb_cap = 8;
    char **edb_rels = (char **)malloc(edb_cap * sizeof(char *));
    if (!edb_rels) {
        free(plan);
        return -1;
    }
    /* Issue #535: parallel arrays for per-EDB graph-column metadata. */
    bool *edb_has_graph = (bool *)calloc(edb_cap, sizeof(bool));
    uint32_t *edb_graph_idx = (uint32_t *)calloc(edb_cap, sizeof(uint32_t));
    /* Issue #1038: the declared physical width, so the session can reject a
     * first host insert that disagrees with the `.decl` instead of letting
     * it define the relation.  WL_PLAN_WIDTH_UNDECLARED for a relation with
     * no `.decl` -- those are legitimate and stay caller-defined. */
    uint32_t *edb_decl_w = (uint32_t *)malloc(edb_cap * sizeof(uint32_t));
    if (!edb_has_graph || !edb_graph_idx || !edb_decl_w) {
        free(edb_decl_w);
        free(edb_graph_idx);
        free(edb_has_graph);
        free((void *)edb_rels);
        free(plan);
        return -1;
    }
    uint32_t edb_count = 0;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name)
            continue;

        /* Check if this relation is a pure IDB (has rules but no facts).
         * Relations with both rules and inline facts (e.g., reach(1).)
         * must still be pre-registered so that fact loading can insert
         * seed tuples before evaluation begins. */
        bool is_idb = false;
        bool has_facts = (rel->fact_count > 0 && rel->fact_data != NULL);
        for (uint32_t r = 0; r < prog->rule_count; r++) {
            if (prog->rules[r].head_relation
                && strcmp(prog->rules[r].head_relation, rel->name) == 0) {
                is_idb = true;
                break;
            }
        }
        if (!is_idb || has_facts) {
            if (edb_count >= edb_cap) {
                edb_cap *= 2;
                char **tmp = (char **)realloc((void *)edb_rels,
                        edb_cap * sizeof(char *));
                if (!tmp) {
                    for (uint32_t j = 0; j < edb_count; j++)
                        free(edb_rels[j]);
                    free((void *)edb_rels);
                    free(edb_has_graph);
                    free(edb_graph_idx);
                    free(edb_decl_w);
                    free(plan);
                    return -1;
                }
                edb_rels = tmp;
                bool *htmp = (bool *)realloc(edb_has_graph,
                        edb_cap * sizeof(bool));
                if (!htmp) {
                    for (uint32_t j = 0; j < edb_count; j++)
                        free(edb_rels[j]);
                    free((void *)edb_rels);
                    free(edb_has_graph);
                    free(edb_graph_idx);
                    free(edb_decl_w);
                    free(plan);
                    return -1;
                }
                edb_has_graph = htmp;
                uint32_t *itmp = (uint32_t *)realloc(edb_graph_idx,
                        edb_cap * sizeof(uint32_t));
                if (!itmp) {
                    for (uint32_t j = 0; j < edb_count; j++)
                        free(edb_rels[j]);
                    free((void *)edb_rels);
                    free(edb_has_graph);
                    free(edb_graph_idx);
                    free(edb_decl_w);
                    free(plan);
                    return -1;
                }
                edb_graph_idx = itmp;
                uint32_t *wtmp = (uint32_t *)realloc(edb_decl_w,
                        edb_cap * sizeof(uint32_t));
                if (!wtmp) {
                    for (uint32_t j = 0; j < edb_count; j++)
                        free(edb_rels[j]);
                    free((void *)edb_rels);
                    free(edb_has_graph);
                    free(edb_graph_idx);
                    free(edb_decl_w);
                    free(plan);
                    return -1;
                }
                edb_decl_w = wtmp;
            }
            edb_rels[edb_count] = dup_str(rel->name);
            if (!edb_rels[edb_count]) {
                for (uint32_t j = 0; j < edb_count; j++)
                    free(edb_rels[j]);
                free((void *)edb_rels);
                free(edb_has_graph);
                free(edb_graph_idx);
                free(edb_decl_w);
                free(plan);
                return -1;
            }
            /* Issue #535: propagate graph-column metadata from IR. */
            edb_has_graph[edb_count] = rel->has_graph_column;
            edb_graph_idx[edb_count] = rel->graph_column_index;
            /* Issue #1038: physical, not logical -- an inline compound
             * column occupies compound_arity slots, and the physical count
             * is what a host insert has to match. */
            edb_decl_w[edb_count] = rel->has_decl
                ? wl_ir_relation_physical_width(rel)
                : WL_PLAN_WIDTH_UNDECLARED;
            edb_count++;
        }
    }

    plan->edb_relations = (const char *const *)edb_rels;
    plan->edb_count = edb_count;
    plan->edb_has_graph_column = (const bool *)edb_has_graph;
    plan->edb_graph_col_index = (const uint32_t *)edb_graph_idx;
    plan->edb_declared_width = (const uint32_t *)edb_decl_w;

    /* ----------------------------------------------------------------
     * Build strata
     * ---------------------------------------------------------------- */
    uint32_t stratum_count = prog->stratum_count;
    if (stratum_count == 0)
        stratum_count = 1; /* guarantee at least 1 stratum */

    wl_plan_stratum_t *strata
        = (wl_plan_stratum_t *)calloc(stratum_count, sizeof(wl_plan_stratum_t));
    if (!strata) {
        wl_plan_free(plan);
        return -1;
    }
    /* Transfer ownership before building the first stratum so every failure
     * path can release already-completed nested allocations through
     * wl_plan_free(). */
    plan->strata = strata;
    plan->stratum_count = stratum_count;

    for (uint32_t s = 0; s < prog->stratum_count; s++) {
        const wirelog_stratum_t *src = &prog->strata[s];
        wl_plan_stratum_t *dst = &strata[s];

        dst->stratum_id = src->stratum_id;
        dst->is_recursive = src->is_recursive;

        /* Issue #105 / #1021: computed below, once this stratum's relations
         * exist.  Seeded false so every early-exit path leaves the
         * conservative value. */
        dst->is_monotone = false;

        /* Count unique relations in this stratum */
        /* Build per-relation plan from relation_irs[] */
        if (!src->rule_names || src->rule_count == 0) {
            dst->relations = NULL;
            dst->relation_count = 0;
            continue;
        }

        /* Collect unique relation names in this stratum */
        uint32_t max_rels = src->rule_count;
        char **unique_names = (char **)calloc(max_rels, sizeof(char *));
        if (!unique_names) {
            wl_plan_free(plan);
            return -1;
        }
        uint32_t unique_count = 0;

        for (uint32_t r = 0; r < src->rule_count; r++) {
            const char *name = src->rule_names[r];
            if (!name)
                continue;
            bool found = false;
            for (uint32_t u = 0; u < unique_count; u++) {
                if (strcmp(unique_names[u], name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found)
                unique_names[unique_count++] = (char *)name;
        }

        wl_plan_relation_t *rels = (wl_plan_relation_t *)calloc(
            unique_count, sizeof(wl_plan_relation_t));
        if (!rels) {
            free((void *)unique_names);
            wl_plan_free(plan);
            return -1;
        }

        for (uint32_t u = 0; u < unique_count; u++) {
            const char *rel_name = unique_names[u];

            /* Find the relation_irs[] index for this relation */
            wirelog_ir_node_t *ir_root = NULL;
            for (uint32_t ri = 0; ri < prog->relation_count; ri++) {
                if (prog->relations[ri].name
                    && strcmp(prog->relations[ri].name, rel_name) == 0
                    && prog->relation_irs && prog->relation_irs[ri]) {
                    ir_root = prog->relation_irs[ri];
                    break;
                }
            }

            rels[u].name = dup_str(rel_name);
            rels[u].delta_name = make_delta_name(rel_name);
            if (!rels[u].name || !rels[u].delta_name) {
                free((void *)unique_names);
                for (uint32_t v = 0; v < u; v++) {
                    free((void *)rels[v].name);
                    free(rels[v].delta_name);
                    if (rels[v].ops) {
                        for (uint32_t o = 0; o < rels[v].op_count; o++)
                            free_op((wl_plan_op_t *)&rels[v].ops[o]);
                        free((void *)rels[v].ops);
                    }
                }
                free((void *)rels[u].name);
                free(rels[u].delta_name);
                free(rels);
                wl_plan_free(plan);
                return -1;
            }

            if (ir_root) {
                op_list_t ol;
                if (op_list_init(&ol) != 0) {
                    free((void *)unique_names);
                    /* Clean up already-built relations */
                    for (uint32_t v = 0; v < u; v++) {
                        free((void *)rels[v].name);
                        free(rels[v].delta_name);
                        if (rels[v].ops) {
                            for (uint32_t o = 0; o < rels[v].op_count; o++)
                                free_op((wl_plan_op_t *)&rels[v].ops[o]);
                            free((void *)rels[v].ops);
                        }
                    }
                    free((void *)rels[u].name);
                    free(rels[u].delta_name);
                    free(rels);
                    wl_plan_free(plan);
                    return -1;
                }

                int rc = translate_ir_node(ir_root, &ol);
                if (rc != 0) {
                    /* Clean up ops */
                    for (uint32_t o = 0; o < ol.count; o++)
                        free_op(&ol.ops[o]);
                    free(ol.ops);
                    free((void *)unique_names);
                    for (uint32_t v = 0; v < u; v++) {
                        free((void *)rels[v].name);
                        free(rels[v].delta_name);
                        if (rels[v].ops) {
                            for (uint32_t o = 0; o < rels[v].op_count; o++)
                                free_op((wl_plan_op_t *)&rels[v].ops[o]);
                            free((void *)rels[v].ops);
                        }
                    }
                    free((void *)rels[u].name);
                    free(rels[u].delta_name);
                    free(rels);
                    wl_plan_free(plan);
                    return -1;
                }

                /* Every IDB relation is set-valued.  A relation with one
                 * defining rule has no IR UNION node, so it otherwise skips
                 * the CONSOLIDATE emitted by the union lowering.  Leaving
                 * that path unconsolidated makes duplicate EDB rows visible
                 * in snapshots and lets them multiply downstream joins. */
                if (ol.count == 0
                    || ol.ops[ol.count - 1].op != WL_PLAN_OP_CONSOLIDATE) {
                    wl_plan_op_t *consol = op_list_push(&ol);
                    if (!consol) {
                        for (uint32_t o = 0; o < ol.count; o++)
                            free_op(&ol.ops[o]);
                        free(ol.ops);
                        free((void *)unique_names);
                        for (uint32_t v = 0; v < u; v++) {
                            free((void *)rels[v].name);
                            free(rels[v].delta_name);
                            if (rels[v].ops) {
                                for (uint32_t o = 0; o < rels[v].op_count; o++)
                                    free_op((wl_plan_op_t *)&rels[v].ops[o]);
                                free((void *)rels[v].ops);
                            }
                        }
                        free((void *)rels[u].name);
                        free(rels[u].delta_name);
                        free(rels);
                        wl_plan_free(plan);
                        return -1;
                    }
                    consol->op = WL_PLAN_OP_CONSOLIDATE;
                }

                rels[u].ops = ol.ops;
                rels[u].op_count = ol.count;
                /* Issue #975: recorded here, before rewrite_lftj_chains(),
                 * rewrite_multiway_delta(), rewrite_join_project_fusion() and
                 * rewrite_insert_exchanges() run below.  Those reshape and in
                 * the fusion case hide @ops entirely; none of them can reach a
                 * field that is not an operator. */
                rels[u].recursive_agg = ol.agg;
            } else {
                rels[u].ops = NULL;
                rels[u].op_count = 0;
            }
        }

        free((void *)unique_names);
        dst->relations = rels;
        dst->relation_count = unique_count;

        /* Issue #1021: compute is_monotone, which #105 declared and left
         * hardcoded false.
         *
         * Scanned here deliberately, not later: the rewrites run at the end
         * of this function, and rewrite_multiway_delta() moves a fused
         * relation's operators into K_FUSION's opaque_data while
         * rewrite_lftj_chains() frees them outright, keeping only
         * rel_names[].  A scan after either one sees a rule that appears to
         * contain no antijoin.  That is the same blinding as #1019, and the
         * fix is the same: look before the rewrites, where the operators are
         * still all present.
         *
         * Scope, stated because the field's name promises more than this
         * computes.  What is decided here is exactly "does this stratum
         * retract through negation".  The other half of the monotonicity
         * question -- whether a same-stratum rule reads an aggregate column
         * non-monotonically, which is what gates eager aggregate domination
         * -- is NOT decided, and a true value here must not be read as
         * clearance for that.  #1021's remaining work is that analysis plus
         * moving domination into consolidation across all three call sites.
         *
         * Nothing reads this today: stratum_is_monotone[] is written in
         * session.c and has no consumer, so the "deletion phase skip
         * optimization" its comment describes does not exist.  Computing it
         * therefore cannot change any answer; it is the prerequisite the
         * issue asks for, made observable so the next step can be tested. */
        bool has_negation = false;
        for (uint32_t v = 0; v < unique_count && !has_negation; v++) {
            for (uint32_t o = 0; o < rels[v].op_count; o++) {
                if (rels[v].ops[o].op == WL_PLAN_OP_ANTIJOIN) {
                    has_negation = true;
                    break;
                }
            }
        }
        dst->is_monotone = !has_negation;

        WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_DEBUG,
            "stratum %u is_monotone=%d (negation=%d, relations=%u); "
            "negation only -- aggregate reads not analysed (#1021)",
            dst->stratum_id, (int)dst->is_monotone, (int)has_negation,
            unique_count);

        /* Issue #1019: record what each relation reads while @ops is still the
         * lowered operator list.  Deliberately placed after the ownership
         * transfer above, not next to the rels[u].ops assignment: from here on
         * wl_plan_free() reaches every allocation made so far, so the failure
         * path does not have to unwind rels[] by hand.  Still ahead of all four
         * rewrites below, which is what the record is for. */
        if (plan_stratum_record_refs(dst) != 0) {
            wl_plan_free(plan);
            return -1;
        }
    }

    /* Reject non-monotone recursive aggregates while the original REDUCE
     * operators are still visible.  Rewrites below may move them into opaque
     * backend metadata or discard their shape entirely.
     *
     * Do not move this call below rewrite_lftj_chains() or
     * rewrite_multiway_delta().  Both passes of the validation key on
     * WL_PLAN_OP_REDUCE, and after those two rewrites there is no REDUCE left
     * to key on: the #1021 repro's Label relation is exactly {K_FUSION,
     * EXCHANGE} at that point, its operators having been moved into
     * wl_plan_op_k_fusion_t.k_ops[].  A check placed lower down would
     * silently never fire on precisely the shapes fusion applies to, while
     * still passing its own tests on the unfused spellings -- which is how
     * the same mistake got made once in #975 and again in #1019.  It has to
     * sit here, after plan_stratum_record_refs() above and before the four
     * rewrites below. */
    if (validate_recursive_aggregates(plan, prog) != 0) {
        wl_plan_free(plan);
        return -1;
    }

    /* Rewrite K-atom recursive rules for complete semi-naive evaluation.
     * For rules with K >= 2 IDB body atoms, emit K copies with CSE
     * materialization hints to avoid the regression seen without CSE. */
    rewrite_lftj_chains(plan);
    rewrite_multiway_delta(plan);
    if (rewrite_join_project_fusion(plan) != 0) {
        wl_plan_free(plan);
        return -1;
    }
    rewrite_insert_exchanges(plan);

    prog->plan_error[0] = '\0';
    *out = plan;
    return 0;
}

/* ======================================================================== */
/* Plan Segment Split (Issue #316)                                          */
/* ======================================================================== */

/*
 * col_plan_split_at_exchange:
 * Split a relation plan at WL_PLAN_OP_EXCHANGE boundaries.
 *
 * N exchanges produce N+1 segments.  Each EXCHANGE op is consumed as a
 * boundary marker and does NOT appear in any segment.  The returned
 * segments are non-owning views: their ops and name pointers reference
 * the caller's original data and must not be freed via free_op().
 *
 * Returns {0, NULL} on allocation failure.
 */
wl_plan_segment_array_t
col_plan_split_at_exchange(const wl_plan_relation_t *rplan)
{
    wl_plan_segment_array_t result = { 0, NULL };

    if (!rplan)
        return result;

    /* Count exchange boundaries to determine segment count. */
    uint32_t nseg = 1;

    for (uint32_t i = 0; i < rplan->op_count; i++) {
        if (rplan->ops[i].op == WL_PLAN_OP_EXCHANGE)
            nseg++;
    }

    result.segments = (wl_plan_relation_t *)calloc(
        nseg, sizeof(wl_plan_relation_t));
    if (!result.segments)
        return result;

    uint32_t seg = 0;
    uint32_t seg_start = 0;

    for (uint32_t i = 0; i <= rplan->op_count; i++) {
        bool at_boundary = (i == rplan->op_count)
            || (rplan->ops[i].op == WL_PLAN_OP_EXCHANGE);

        if (at_boundary) {
            result.segments[seg].name = rplan->name;
            result.segments[seg].ops = rplan->ops + seg_start;
            result.segments[seg].op_count = i - seg_start;
            /* Carried, not recomputed: it is a property of the relation, and
             * every segment is a view of the same relation (Issue #975). */
            result.segments[seg].recursive_agg = rplan->recursive_agg;
            seg++;
            seg_start = i + 1; /* skip the EXCHANGE op */
        }
    }

    result.num_segments = nseg;
    return result;
}

void
wl_plan_segment_array_free(wl_plan_segment_array_t *sa)
{
    if (sa)
        free(sa->segments);
}
