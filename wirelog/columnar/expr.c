/*
 * columnar/expr.c - Compiled columnar expression evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/internal.h"

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
