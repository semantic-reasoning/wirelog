/*
 * fpga_backend.c - Naive row-store "FPGA" backend for vtable validation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Test-only backend that validates the wl_compute_backend_t vtable
 * is truly pluggable.  Correctness matters, performance does not.
 *
 * ========================================================================
 * Interface Gap Findings (Issue #496)
 * ========================================================================
 *
 * 1. Plan-gen coupling: exec_plan_gen.c unconditionally emits K_FUSION,
 *    LFTJ, and EXCHANGE operators.  Non-columnar backends must skip these
 *    (ops >= WL_PLAN_OP__BACKEND_START).
 *
 * 2. wl_easy.h hardcodes the columnar backend via wl_backend_columnar().
 *    Alternative backends require manual wl_session_create() calls.
 *
 * 3. Expression opcode coverage: only arithmetic and comparison opcodes
 *    are validated by this backend.  String, hash, and cryptographic
 *    opcodes are treated as pass-through (permissive).
 *
 * 4. Positive findings: intern sharing via plan->intern works cleanly.
 *    Session embedding (wl_session_t as first field) and the wrapper
 *    in session.c correctly sets base.backend after session_create.
 *    Delta naming convention ($d$<name>) works across backends.
 * ========================================================================
 */

#include "fpga_backend.h"
#include "../wirelog/session.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Internal Row-Store Types                                                 */
/* ======================================================================== */

/**
 * fpga_rowset_t:
 *
 * Dynamic array of rows stored as flat row-major int64_t values.
 */
typedef struct {
    int64_t *data;   /* row-major flat array */
    uint32_t nrows;
    uint32_t ncols;
    uint32_t cap;    /* capacity in rows */
} fpga_rowset_t;

/**
 * fpga_rel_t:
 *
 * Named relation holding a dynamic row set.
 */
typedef struct {
    char name[128];
    fpga_rowset_t rows;
} fpga_rel_t;

/**
 * wl_fpga_session_t:
 *
 * FPGA backend session.  The wl_session_t base MUST be the first field
 * so that C11 struct casting works (session.c sets base.backend).
 */
typedef struct {
    wl_session_t base;          /* MUST be first */
    const wl_plan_t *plan;
    fpga_rel_t *relations;
    uint32_t rel_count;
    uint32_t rel_cap;
    wl_on_delta_fn delta_cb;
    void *delta_user_data;
} wl_fpga_session_t;

/* ======================================================================== */
/* Rowset Helpers                                                           */
/* ======================================================================== */

static void
fpga_rowset_init(fpga_rowset_t *rs, uint32_t ncols)
{
    rs->data = NULL;
    rs->nrows = 0;
    rs->ncols = ncols;
    rs->cap = 0;
}

static void
fpga_rowset_free(fpga_rowset_t *rs)
{
    free(rs->data);
    rs->data = NULL;
    rs->nrows = 0;
    rs->cap = 0;
}

static int
fpga_rowset_append(fpga_rowset_t *rs, const int64_t *row, uint32_t ncols)
{
    if (ncols != rs->ncols && rs->ncols != 0)
        return -1;
    if (rs->ncols == 0)
        rs->ncols = ncols;
    if (rs->nrows >= rs->cap) {
        uint32_t newcap = rs->cap == 0 ? 16 : rs->cap * 2;
        int64_t *nd = realloc(rs->data,
                (size_t)newcap * rs->ncols * sizeof(int64_t));
        if (!nd)
            return -1;
        rs->data = nd;
        rs->cap = newcap;
    }
    memcpy(rs->data + (size_t)rs->nrows * rs->ncols,
        row, (size_t)ncols * sizeof(int64_t));
    rs->nrows++;
    return 0;
}

static int
fpga_rowset_append_rows(fpga_rowset_t *rs, const int64_t *data,
    uint32_t num_rows, uint32_t ncols)
{
    for (uint32_t i = 0; i < num_rows; i++) {
        int rc = fpga_rowset_append(rs, data + (size_t)i * ncols, ncols);
        if (rc != 0)
            return rc;
    }
    return 0;
}

static const int64_t *
fpga_rowset_row(const fpga_rowset_t *rs, uint32_t idx)
{
    return rs->data + (size_t)idx * rs->ncols;
}

static fpga_rowset_t
fpga_rowset_clone(const fpga_rowset_t *src)
{
    fpga_rowset_t dst;
    fpga_rowset_init(&dst, src->ncols);
    if (src->nrows > 0 && src->data) {
        dst.data = malloc((size_t)src->nrows * src->ncols * sizeof(int64_t));
        if (dst.data) {
            memcpy(dst.data, src->data,
                (size_t)src->nrows * src->ncols * sizeof(int64_t));
            dst.nrows = src->nrows;
            dst.cap = src->nrows;
        }
    }
    return dst;
}

/* ======================================================================== */
/* Relation Lookup                                                          */
/* ======================================================================== */

static fpga_rel_t *
fpga_find_rel(wl_fpga_session_t *s, const char *name)
{
    for (uint32_t i = 0; i < s->rel_count; i++) {
        if (strcmp(s->relations[i].name, name) == 0)
            return &s->relations[i];
    }
    return NULL;
}

static fpga_rel_t *
fpga_ensure_rel(wl_fpga_session_t *s, const char *name, uint32_t ncols)
{
    fpga_rel_t *r = fpga_find_rel(s, name);
    if (r)
        return r;
    if (s->rel_count >= s->rel_cap) {
        uint32_t newcap = s->rel_cap == 0 ? 16 : s->rel_cap * 2;
        fpga_rel_t *nr = realloc(s->relations,
                (size_t)newcap * sizeof(fpga_rel_t));
        if (!nr)
            return NULL;
        s->relations = nr;
        s->rel_cap = newcap;
    }
    r = &s->relations[s->rel_count++];
    strncpy(r->name, name, 127);
    r->name[127] = '\0';
    fpga_rowset_init(&r->rows, ncols);
    return r;
}

/* ======================================================================== */
/* Column Name Parser                                                       */
/* ======================================================================== */

/**
 * Parse a column name string to a column index.
 * Supports formats: "col0", "col1", ..., "0", "1", ...
 * Returns the parsed index, or 0 on parse failure.
 */
static uint32_t
fpga_parse_col_name(const char *name, uint32_t name_len)
{
    uint32_t idx = 0;
    uint32_t start = 0;
    /* Skip "col" prefix if present */
    if (name_len >= 4 && name[0] == 'c' && name[1] == 'o' && name[2] == 'l')
        start = 3;
    for (uint32_t k = start; k < name_len; k++) {
        char ch = name[k];
        if (ch >= '0' && ch <= '9')
            idx = idx * 10 + (uint32_t)(ch - '0');
    }
    return idx;
}

/**
 * Parse a null-terminated column name string to a column index.
 */
static uint32_t
fpga_parse_col_name_z(const char *name)
{
    if (!name)
        return 0;
    return fpga_parse_col_name(name, (uint32_t)strlen(name));
}

/* ======================================================================== */
/* Expression Evaluator (postfix stack machine)                             */
/* ======================================================================== */

#define EXPR_STACK_MAX 64

static int64_t
fpga_eval_expr(const wl_plan_expr_buffer_t *expr, const int64_t *row,
    uint32_t ncols)
{
    if (!expr || !expr->data || expr->size == 0)
        return 1; /* no filter = pass */

    int64_t stack[EXPR_STACK_MAX];
    int sp = 0;
    uint32_t pos = 0;

    while (pos < expr->size) {
        uint8_t tag = expr->data[pos++];
        switch (tag) {
        case WL_PLAN_EXPR_VAR: { /* 0x01 */
            if (pos + 2 > expr->size)
                return 1;
            uint16_t name_len = (uint16_t)(expr->data[pos]
                | (expr->data[pos + 1] << 8));
            pos += 2;
            /* Variable name encodes column index as single char digit
             * or as a multi-char name.  For the FPGA backend, we use
             * the column index directly: variable names are typically
             * single-char column references like "0", "1", etc.
             * The plan generator encodes column indices as decimal strings. */
            if (pos + name_len > expr->size)
                return 1;
            /* Parse the variable name as a column index (col0, col1, ...) */
            uint32_t col_idx = fpga_parse_col_name(
                (const char *)(expr->data + pos), name_len);
            pos += name_len;
            if (col_idx < ncols && sp < EXPR_STACK_MAX)
                stack[sp++] = row[col_idx];
            else if (sp < EXPR_STACK_MAX)
                stack[sp++] = 0;
            break;
        }
        case WL_PLAN_EXPR_CONST_INT: { /* 0x02 */
            if (pos + 8 > expr->size)
                return 1;
            int64_t val = 0;
            memcpy(&val, expr->data + pos, 8);
            pos += 8;
            if (sp < EXPR_STACK_MAX)
                stack[sp++] = val;
            break;
        }
        case WL_PLAN_EXPR_CONST_STR: { /* 0x03 */
            if (pos + 2 > expr->size)
                return 1;
            uint16_t len = (uint16_t)(expr->data[pos]
                | (expr->data[pos + 1] << 8));
            pos += 2 + len;
            if (sp < EXPR_STACK_MAX)
                stack[sp++] = 0; /* strings not supported as values */
            break;
        }
        case WL_PLAN_EXPR_BOOL: { /* 0x04 */
            if (pos + 1 > expr->size)
                return 1;
            if (sp < EXPR_STACK_MAX)
                stack[sp++] = expr->data[pos] ? 1 : 0;
            pos += 1;
            break;
        }
        /* Arithmetic */
        case WL_PLAN_EXPR_ARITH_ADD: /* 0x10 */
            if (sp >= 2) {
                sp--; stack[sp - 1] += stack[sp];
            }
            break;
        case WL_PLAN_EXPR_ARITH_SUB: /* 0x11 */
            if (sp >= 2) {
                sp--; stack[sp - 1] -= stack[sp];
            }
            break;
        case WL_PLAN_EXPR_ARITH_MUL: /* 0x12 */
            if (sp >= 2) {
                sp--; stack[sp - 1] *= stack[sp];
            }
            break;
        case WL_PLAN_EXPR_ARITH_DIV: /* 0x13 */
            if (sp >= 2) {
                sp--;
                stack[sp - 1] = stack[sp] != 0
                    ? stack[sp - 1] / stack[sp] : 0;
            }
            break;
        case WL_PLAN_EXPR_ARITH_MOD: /* 0x14 */
            if (sp >= 2) {
                sp--;
                stack[sp - 1] = stack[sp] != 0
                    ? stack[sp - 1] % stack[sp] : 0;
            }
            break;
        /* Bitwise */
        case WL_PLAN_EXPR_ARITH_BAND: /* 0x15 */
            if (sp >= 2) {
                sp--; stack[sp - 1] &= stack[sp];
            }
            break;
        case WL_PLAN_EXPR_ARITH_BOR: /* 0x16 */
            if (sp >= 2) {
                sp--; stack[sp - 1] |= stack[sp];
            }
            break;
        case WL_PLAN_EXPR_ARITH_BXOR: /* 0x17 */
            if (sp >= 2) {
                sp--; stack[sp - 1] ^= stack[sp];
            }
            break;
        case WL_PLAN_EXPR_ARITH_SHL: /* 0x18 */
            if (sp >= 2) {
                sp--;
                stack[sp - 1] = (int64_t)((uint64_t)stack[sp - 1]
                    << ((uint64_t)stack[sp] & 63));
            }
            break;
        case WL_PLAN_EXPR_ARITH_SHR: /* 0x19 */
            if (sp >= 2) {
                sp--;
                stack[sp - 1] = (int64_t)((uint64_t)stack[sp - 1]
                    >> (uint64_t)stack[sp]);
            }
            break;
        case WL_PLAN_EXPR_ARITH_BNOT: /* 0x1A */
            if (sp >= 1) stack[sp - 1] = ~stack[sp - 1];
            break;
        case WL_PLAN_EXPR_ARITH_HASH: /* 0x1B */
            /* Hash: pass through value (no xxhash dependency) */
            break;
        /* Comparisons */
        case WL_PLAN_EXPR_CMP_EQ: /* 0x22 */
            if (sp >= 2) {
                sp--; stack[sp - 1] = stack[sp - 1] == stack[sp] ? 1 : 0;
            }
            break;
        case WL_PLAN_EXPR_CMP_NEQ: /* 0x23 */
            if (sp >= 2) {
                sp--; stack[sp - 1] = stack[sp - 1] != stack[sp] ? 1 : 0;
            }
            break;
        case WL_PLAN_EXPR_CMP_LT: /* 0x24 */
            if (sp >= 2) {
                sp--; stack[sp - 1] = stack[sp - 1] < stack[sp] ? 1 : 0;
            }
            break;
        case WL_PLAN_EXPR_CMP_GT: /* 0x25 */
            if (sp >= 2) {
                sp--; stack[sp - 1] = stack[sp - 1] > stack[sp] ? 1 : 0;
            }
            break;
        case WL_PLAN_EXPR_CMP_LTE: /* 0x26 */
            if (sp >= 2) {
                sp--; stack[sp - 1] = stack[sp - 1] <= stack[sp] ? 1 : 0;
            }
            break;
        case WL_PLAN_EXPR_CMP_GTE: /* 0x27 */
            if (sp >= 2) {
                sp--; stack[sp - 1] = stack[sp - 1] >= stack[sp] ? 1 : 0;
            }
            break;
        /* Aggregates (used in REDUCE, not in filter context) */
        case WL_PLAN_EXPR_AGG_COUNT: /* 0x30 */
        case WL_PLAN_EXPR_AGG_SUM:   /* 0x31 */
        case WL_PLAN_EXPR_AGG_MIN:   /* 0x32 */
        case WL_PLAN_EXPR_AGG_MAX:   /* 0x33 */
            /* In filter context, pass through */
            break;
        default:
            /* Unknown/unsupported opcode: permissive, return true */
            return 1;
        }
    }

    return sp > 0 ? stack[sp - 1] : 1;
}

/* ======================================================================== */
/* Operator Evaluator                                                       */
/* ======================================================================== */

#define OP_STACK_MAX 32

static fpga_rowset_t
fpga_eval_ops(wl_fpga_session_t *s, const wl_plan_op_t *ops, uint32_t op_count)
{
    fpga_rowset_t stack[OP_STACK_MAX];
    int sp = 0;

    for (uint32_t i = 0; i < op_count; i++) {
        const wl_plan_op_t *op = &ops[i];

        /* Skip backend-specific operators */
        if (wl_plan_op_is_backend_specific(op->op)) {
            continue;
        }

        switch (op->op) {

        case WL_PLAN_OP_VARIABLE: {
            fpga_rowset_t rs;
            if (op->delta_mode == WL_DELTA_FORCE_EMPTY) {
                fpga_rowset_init(&rs, 0);
            } else {
                fpga_rel_t *rel = NULL;
                if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
                    /* Look up delta relation $d$<name> */
                    char dname[140];
                    snprintf(dname, sizeof(dname), "$d$%s",
                        op->relation_name);
                    rel = fpga_find_rel(s, dname);
                }
                if (!rel)
                    rel = fpga_find_rel(s, op->relation_name);
                if (rel) {
                    rs = fpga_rowset_clone(&rel->rows);
                } else {
                    fpga_rowset_init(&rs, 0);
                }
            }
            if (sp < OP_STACK_MAX)
                stack[sp++] = rs;
            else
                fpga_rowset_free(&rs);
            break;
        }

        case WL_PLAN_OP_MAP: {
            if (sp < 1)
                break;
            fpga_rowset_t *top = &stack[sp - 1];
            if (op->project_count == 0 || !op->project_indices)
                break;
            fpga_rowset_t result;
            fpga_rowset_init(&result, op->project_count);
            for (uint32_t r = 0; r < top->nrows; r++) {
                const int64_t *row = fpga_rowset_row(top, r);
                int64_t projected[64];
                for (uint32_t c = 0; c < op->project_count && c < 64; c++) {
                    uint32_t idx = op->project_indices[c];
                    projected[c] = (idx < top->ncols) ? row[idx] : 0;
                }
                fpga_rowset_append(&result, projected, op->project_count);
            }
            fpga_rowset_free(top);
            *top = result;
            break;
        }

        case WL_PLAN_OP_FILTER: {
            if (sp < 1)
                break;
            fpga_rowset_t *top = &stack[sp - 1];
            if (!op->filter_expr.data || op->filter_expr.size == 0)
                break;
            fpga_rowset_t result;
            fpga_rowset_init(&result, top->ncols);
            for (uint32_t r = 0; r < top->nrows; r++) {
                const int64_t *row = fpga_rowset_row(top, r);
                if (fpga_eval_expr(&op->filter_expr, row, top->ncols))
                    fpga_rowset_append(&result, row, top->ncols);
            }
            fpga_rowset_free(top);
            *top = result;
            break;
        }

        case WL_PLAN_OP_JOIN: {
            if (sp < 1)
                break;
            fpga_rowset_t left = stack[--sp];
            /* Look up right relation */
            fpga_rel_t *rrel = NULL;
            if (op->right_relation)
                rrel = fpga_find_rel(s, op->right_relation);
            fpga_rowset_t result;
            uint32_t out_cols = left.ncols
                + (rrel ? rrel->rows.ncols : 0);
            fpga_rowset_init(&result, out_cols);
            if (rrel && op->key_count > 0) {
                for (uint32_t li = 0; li < left.nrows; li++) {
                    const int64_t *lrow = fpga_rowset_row(&left, li);
                    for (uint32_t ri = 0; ri < rrel->rows.nrows; ri++) {
                        const int64_t *rrow =
                            fpga_rowset_row(&rrel->rows, ri);
                        /* Check join keys */
                        bool match = true;
                        for (uint32_t k = 0; k < op->key_count; k++) {
                            /* Keys are column name strings; parse as
                             * column index (same convention as VARIABLE) */
                            uint32_t lk = 0, rk = 0;
                            if (op->left_keys && op->left_keys[k])
                                lk = fpga_parse_col_name_z(op->left_keys[k]);
                            if (op->right_keys && op->right_keys[k])
                                rk = fpga_parse_col_name_z(op->right_keys[k]);
                            int64_t lv = lk < left.ncols ? lrow[lk] : 0;
                            int64_t rv = rk < rrel->rows.ncols
                                         ? rrow[rk] : 0;
                            if (lv != rv) {
                                match = false;
                                break;
                            }
                        }
                        if (!match)
                            continue;
                        /* Apply right filter if present */
                        if (op->right_filter_expr.data
                            && op->right_filter_expr.size > 0) {
                            if (!fpga_eval_expr(&op->right_filter_expr,
                                rrow, rrel->rows.ncols))
                                continue;
                        }
                        /* Concatenate left + right */
                        int64_t combined[128];
                        uint32_t ci = 0;
                        for (uint32_t c = 0; c < left.ncols && ci < 128;
                            c++)
                            combined[ci++] = lrow[c];
                        for (uint32_t c = 0;
                            c < rrel->rows.ncols && ci < 128; c++)
                            combined[ci++] = rrow[c];
                        fpga_rowset_append(&result, combined, ci);
                    }
                }
            }
            fpga_rowset_free(&left);
            if (sp < OP_STACK_MAX)
                stack[sp++] = result;
            else
                fpga_rowset_free(&result);
            break;
        }

        case WL_PLAN_OP_ANTIJOIN: {
            if (sp < 1)
                break;
            fpga_rowset_t left = stack[--sp];
            fpga_rel_t *rrel = NULL;
            if (op->right_relation)
                rrel = fpga_find_rel(s, op->right_relation);
            fpga_rowset_t result;
            fpga_rowset_init(&result, left.ncols);
            for (uint32_t li = 0; li < left.nrows; li++) {
                const int64_t *lrow = fpga_rowset_row(&left, li);
                bool found = false;
                if (rrel && op->key_count > 0) {
                    for (uint32_t ri = 0; ri < rrel->rows.nrows; ri++) {
                        const int64_t *rrow =
                            fpga_rowset_row(&rrel->rows, ri);
                        bool match = true;
                        for (uint32_t k = 0; k < op->key_count; k++) {
                            uint32_t lk = 0, rk = 0;
                            if (op->left_keys && op->left_keys[k])
                                lk = fpga_parse_col_name_z(op->left_keys[k]);
                            if (op->right_keys && op->right_keys[k])
                                rk = fpga_parse_col_name_z(op->right_keys[k]);
                            int64_t lv = lk < left.ncols ? lrow[lk] : 0;
                            int64_t rv = rk < rrel->rows.ncols
                                         ? rrow[rk] : 0;
                            if (lv != rv) {
                                match = false;
                                break;
                            }
                        }
                        if (match) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found)
                    fpga_rowset_append(&result, lrow, left.ncols);
            }
            fpga_rowset_free(&left);
            if (sp < OP_STACK_MAX)
                stack[sp++] = result;
            else
                fpga_rowset_free(&result);
            break;
        }

        case WL_PLAN_OP_REDUCE: {
            if (sp < 1)
                break;
            fpga_rowset_t *top = &stack[sp - 1];
            if (top->nrows == 0)
                break;

            /* Simple grouping: collect unique group keys, then aggregate */
            uint32_t gcount = op->group_by_count;
            uint32_t out_ncols = gcount + 1; /* group keys + agg result */
            fpga_rowset_t result;
            fpga_rowset_init(&result, out_ncols);

            /* Brute-force: for each row, check if group already seen */
            for (uint32_t r = 0; r < top->nrows; r++) {
                const int64_t *row = fpga_rowset_row(top, r);

                /* Extract group key */
                int64_t gkey[64];
                for (uint32_t g = 0; g < gcount && g < 64; g++) {
                    uint32_t idx = op->group_by_indices[g];
                    gkey[g] = idx < top->ncols ? row[idx] : 0;
                }

                /* Check if this group is already in result */
                bool found = false;
                for (uint32_t ri = 0; ri < result.nrows; ri++) {
                    const int64_t *rrow = fpga_rowset_row(&result, ri);
                    bool match = true;
                    for (uint32_t g = 0; g < gcount; g++) {
                        if (rrow[g] != gkey[g]) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        found = true;
                        break;
                    }
                }
                if (found)
                    continue;

                /* New group: compute aggregate over all matching rows */
                int64_t agg_val = 0;
                bool first = true;
                int64_t count = 0;
                for (uint32_t r2 = 0; r2 < top->nrows; r2++) {
                    const int64_t *row2 = fpga_rowset_row(top, r2);
                    bool match = true;
                    for (uint32_t g = 0; g < gcount; g++) {
                        uint32_t idx = op->group_by_indices[g];
                        int64_t v = idx < top->ncols ? row2[idx] : 0;
                        if (v != gkey[g]) {
                            match = false;
                            break;
                        }
                    }
                    if (!match)
                        continue;
                    /* Use last column as the aggregate input */
                    int64_t v = top->ncols > 0
                        ? row2[top->ncols - 1] : 0;
                    count++;
                    if (first) {
                        agg_val = v;
                        first = false;
                    } else {
                        switch (op->agg_fn) {
                        case WIRELOG_AGG_COUNT: break;
                        case WIRELOG_AGG_SUM:   agg_val += v; break;
                        case WIRELOG_AGG_MIN:
                            if (v < agg_val) agg_val = v;
                            break;
                        case WIRELOG_AGG_MAX:
                            if (v > agg_val) agg_val = v;
                            break;
                        default: break;
                        }
                    }
                }
                if (op->agg_fn == WIRELOG_AGG_COUNT)
                    agg_val = count;

                int64_t out_row[65];
                for (uint32_t g = 0; g < gcount; g++)
                    out_row[g] = gkey[g];
                out_row[gcount] = agg_val;
                fpga_rowset_append(&result, out_row, out_ncols);
            }
            fpga_rowset_free(top);
            *top = result;
            break;
        }

        case WL_PLAN_OP_CONCAT: {
            if (sp < 2)
                break;
            fpga_rowset_t b = stack[--sp];
            fpga_rowset_t *a = &stack[sp - 1];
            /* Concatenate b into a */
            for (uint32_t r = 0; r < b.nrows; r++) {
                const int64_t *row = fpga_rowset_row(&b, r);
                fpga_rowset_append(a, row, b.ncols);
            }
            fpga_rowset_free(&b);
            break;
        }

        case WL_PLAN_OP_CONSOLIDATE: {
            if (sp < 1)
                break;
            fpga_rowset_t *top = &stack[sp - 1];
            if (top->nrows <= 1)
                break;
            /* Deduplicate: brute-force O(n^2) */
            fpga_rowset_t result;
            fpga_rowset_init(&result, top->ncols);
            for (uint32_t r = 0; r < top->nrows; r++) {
                const int64_t *row = fpga_rowset_row(top, r);
                bool dup = false;
                for (uint32_t j = 0; j < result.nrows; j++) {
                    const int64_t *erow = fpga_rowset_row(&result, j);
                    if (top->ncols == result.ncols
                        && memcmp(row, erow,
                        (size_t)top->ncols * sizeof(int64_t))
                        == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup)
                    fpga_rowset_append(&result, row, top->ncols);
            }
            fpga_rowset_free(top);
            *top = result;
            break;
        }

        case WL_PLAN_OP_SEMIJOIN: {
            if (sp < 1)
                break;
            fpga_rowset_t left = stack[--sp];
            fpga_rel_t *rrel = NULL;
            if (op->right_relation)
                rrel = fpga_find_rel(s, op->right_relation);
            fpga_rowset_t result;
            fpga_rowset_init(&result, left.ncols);
            for (uint32_t li = 0; li < left.nrows; li++) {
                const int64_t *lrow = fpga_rowset_row(&left, li);
                bool found = false;
                if (rrel && op->key_count > 0) {
                    for (uint32_t ri = 0; ri < rrel->rows.nrows; ri++) {
                        const int64_t *rrow =
                            fpga_rowset_row(&rrel->rows, ri);
                        bool match = true;
                        for (uint32_t k = 0; k < op->key_count; k++) {
                            uint32_t lk = 0, rk = 0;
                            if (op->left_keys && op->left_keys[k])
                                lk = fpga_parse_col_name_z(op->left_keys[k]);
                            if (op->right_keys && op->right_keys[k])
                                rk = fpga_parse_col_name_z(op->right_keys[k]);
                            int64_t lv = lk < left.ncols ? lrow[lk] : 0;
                            int64_t rv = rk < rrel->rows.ncols
                                         ? rrow[rk] : 0;
                            if (lv != rv) {
                                match = false;
                                break;
                            }
                        }
                        if (match) {
                            found = true;
                            break;
                        }
                    }
                }
                if (found)
                    fpga_rowset_append(&result, lrow, left.ncols);
            }
            fpga_rowset_free(&left);
            if (sp < OP_STACK_MAX)
                stack[sp++] = result;
            else
                fpga_rowset_free(&result);
            break;
        }

        default:
            break;
        } /* switch */
    } /* for ops */

    fpga_rowset_t out;
    if (sp > 0) {
        out = stack[--sp];
    } else {
        fpga_rowset_init(&out, 0);
    }
    /* Free any remaining stack items */
    while (sp > 0)
        fpga_rowset_free(&stack[--sp]);
    return out;
}

/* ======================================================================== */
/* Row Diff Helper                                                          */
/* ======================================================================== */

/**
 * Check if a row exists in a rowset.
 */
static bool
fpga_rowset_contains(const fpga_rowset_t *rs, const int64_t *row,
    uint32_t ncols)
{
    for (uint32_t r = 0; r < rs->nrows; r++) {
        const int64_t *erow = fpga_rowset_row(rs, r);
        if (rs->ncols == ncols
            && memcmp(row, erow, (size_t)ncols * sizeof(int64_t)) == 0)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Session Vtable Implementation                                            */
/* ======================================================================== */

static int
fpga_session_create(const wl_plan_t *plan, uint32_t num_workers,
    wl_session_t **out)
{
    (void)num_workers;
    if (!plan || !out)
        return -1;

    wl_fpga_session_t *s = calloc(1, sizeof(wl_fpga_session_t));
    if (!s)
        return -1;

    s->plan = plan;
    s->relations = NULL;
    s->rel_count = 0;
    s->rel_cap = 0;
    s->delta_cb = NULL;
    s->delta_user_data = NULL;

    /* Pre-create relations for each EDB */
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        fpga_ensure_rel(s, plan->edb_relations[i], 0);
    }

    *out = (wl_session_t *)s;
    return 0;
}

static void
fpga_session_destroy(wl_session_t *session)
{
    wl_fpga_session_t *s = (wl_fpga_session_t *)session;
    if (!s)
        return;
    for (uint32_t i = 0; i < s->rel_count; i++)
        fpga_rowset_free(&s->relations[i].rows);
    free(s->relations);
    free(s);
}

static int
fpga_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols)
{
    wl_fpga_session_t *s = (wl_fpga_session_t *)session;
    if (!s || !relation || !data)
        return -1;
    fpga_rel_t *rel = fpga_ensure_rel(s, relation, num_cols);
    if (!rel)
        return -1;
    return fpga_rowset_append_rows(&rel->rows, data, num_rows, num_cols);
}

static int
fpga_session_remove(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows,
    uint32_t num_cols)
{
    (void)session;
    (void)relation;
    (void)data;
    (void)num_rows;
    (void)num_cols;
    return -1; /* unsupported */
}

static void
fpga_session_set_delta_cb(wl_session_t *session, wl_on_delta_fn callback,
    void *user_data)
{
    wl_fpga_session_t *s = (wl_fpga_session_t *)session;
    if (!s)
        return;
    s->delta_cb = callback;
    s->delta_user_data = user_data;
}

/**
 * Evaluate all strata and store results into session relations.
 * For recursive strata, iterate until fixed point.
 * If fire_deltas is true, fire delta callback for new tuples.
 */
static int
fpga_eval_all_strata(wl_fpga_session_t *s, bool fire_deltas)
{
    const wl_plan_t *plan = s->plan;

    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *stratum = &plan->strata[si];

        if (!stratum->is_recursive) {
            /* Non-recursive: evaluate each relation once */
            for (uint32_t ri = 0; ri < stratum->relation_count; ri++) {
                const wl_plan_relation_t *rplan = &stratum->relations[ri];
                fpga_rowset_t result = fpga_eval_ops(
                    s, rplan->ops, rplan->op_count);
                fpga_rel_t *rel = fpga_ensure_rel(
                    s, rplan->name, result.ncols);
                if (!rel) {
                    fpga_rowset_free(&result);
                    return -1;
                }

                /* Fire delta callbacks for new rows */
                if (fire_deltas && s->delta_cb) {
                    for (uint32_t r = 0; r < result.nrows; r++) {
                        const int64_t *row =
                            fpga_rowset_row(&result, r);
                        if (!fpga_rowset_contains(&rel->rows, row,
                            result.ncols)) {
                            s->delta_cb(rplan->name, row, result.ncols,
                                +1, s->delta_user_data);
                        }
                    }
                }

                /* Replace relation contents */
                fpga_rowset_free(&rel->rows);
                rel->rows = result;
            }
        } else {
            /* Recursive: iterate until fixed point */
            /* First pass: initial evaluation */
            for (uint32_t ri = 0; ri < stratum->relation_count; ri++) {
                const wl_plan_relation_t *rplan = &stratum->relations[ri];
                fpga_rowset_t result = fpga_eval_ops(
                    s, rplan->ops, rplan->op_count);
                fpga_rel_t *rel = fpga_ensure_rel(
                    s, rplan->name, result.ncols);
                if (!rel) {
                    fpga_rowset_free(&result);
                    return -1;
                }
                /* Set delta = full result for first iteration */
                char dname[140];
                snprintf(dname, sizeof(dname), "$d$%s", rplan->name);
                fpga_rel_t *drel = fpga_ensure_rel(
                    s, dname, result.ncols);
                if (drel) {
                    fpga_rowset_free(&drel->rows);
                    drel->rows = fpga_rowset_clone(&result);
                }

                /* Fire delta callbacks for initial rows */
                if (fire_deltas && s->delta_cb) {
                    for (uint32_t r = 0; r < result.nrows; r++) {
                        const int64_t *row =
                            fpga_rowset_row(&result, r);
                        if (!fpga_rowset_contains(&rel->rows, row,
                            result.ncols)) {
                            s->delta_cb(rplan->name, row, result.ncols,
                                +1, s->delta_user_data);
                        }
                    }
                }

                fpga_rowset_free(&rel->rows);
                rel->rows = result;
            }

            /* Subsequent iterations until fixed point */
            for (int iter = 0; iter < 1000; iter++) {
                bool changed = false;
                for (uint32_t ri = 0; ri < stratum->relation_count;
                    ri++) {
                    const wl_plan_relation_t *rplan =
                        &stratum->relations[ri];
                    fpga_rowset_t result = fpga_eval_ops(
                        s, rplan->ops, rplan->op_count);

                    fpga_rel_t *rel = fpga_find_rel(s, rplan->name);
                    if (!rel) {
                        fpga_rowset_free(&result);
                        continue;
                    }

                    /* Compute delta: new rows not in previous */
                    fpga_rowset_t delta;
                    fpga_rowset_init(&delta, result.ncols);
                    for (uint32_t r = 0; r < result.nrows; r++) {
                        const int64_t *row =
                            fpga_rowset_row(&result, r);
                        if (!fpga_rowset_contains(&rel->rows, row,
                            result.ncols)) {
                            fpga_rowset_append(&delta, row, result.ncols);
                            changed = true;
                        }
                    }

                    /* Fire delta callbacks */
                    if (fire_deltas && s->delta_cb) {
                        for (uint32_t r = 0; r < delta.nrows; r++) {
                            const int64_t *row =
                                fpga_rowset_row(&delta, r);
                            s->delta_cb(rplan->name, row, delta.ncols,
                                +1, s->delta_user_data);
                        }
                    }

                    /* Update delta relation */
                    char dname[140];
                    snprintf(dname, sizeof(dname), "$d$%s", rplan->name);
                    fpga_rel_t *drel = fpga_find_rel(s, dname);
                    if (drel) {
                        fpga_rowset_free(&drel->rows);
                        drel->rows = delta;
                    } else {
                        fpga_rowset_free(&delta);
                    }

                    /* Merge new rows into full relation */
                    fpga_rowset_free(&rel->rows);
                    rel->rows = result;
                }
                if (!changed)
                    break;
            }
        }
    }
    return 0;
}

static int
fpga_session_step(wl_session_t *session)
{
    wl_fpga_session_t *s = (wl_fpga_session_t *)session;
    if (!s)
        return -1;
    return fpga_eval_all_strata(s, true);
}

static int
fpga_session_snapshot(wl_session_t *session, wl_on_tuple_fn callback,
    void *user_data)
{
    wl_fpga_session_t *s = (wl_fpga_session_t *)session;
    if (!s || !callback)
        return -1;

    /* Evaluate all strata (no delta firing in snapshot mode) */
    int rc = fpga_eval_all_strata(s, false);
    if (rc != 0)
        return rc;

    /* Emit all IDB relation tuples */
    const wl_plan_t *plan = s->plan;
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *stratum = &plan->strata[si];
        for (uint32_t ri = 0; ri < stratum->relation_count; ri++) {
            const wl_plan_relation_t *rplan = &stratum->relations[ri];
            fpga_rel_t *rel = fpga_find_rel(s, rplan->name);
            if (!rel)
                continue;
            for (uint32_t r = 0; r < rel->rows.nrows; r++) {
                const int64_t *row = fpga_rowset_row(&rel->rows, r);
                callback(rplan->name, row, rel->rows.ncols, user_data);
            }
        }
    }
    return 0;
}

/* ======================================================================== */
/* Static Vtable                                                            */
/* ======================================================================== */

static const wl_compute_backend_t fpga_backend = {
    .name = "fpga",
    .session_create = fpga_session_create,
    .session_destroy = fpga_session_destroy,
    .session_insert = fpga_session_insert,
    .session_remove = fpga_session_remove,
    .session_step = fpga_session_step,
    .session_set_delta_cb = fpga_session_set_delta_cb,
    .session_snapshot = fpga_session_snapshot,
};

const wl_compute_backend_t *
wl_backend_fpga(void)
{
    return &fpga_backend;
}
