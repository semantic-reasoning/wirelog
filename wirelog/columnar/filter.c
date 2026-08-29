/*
 * columnar/filter.c - Columnar filter operator and scan kernels
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#if defined(_MSC_VER)
#define WL_OPS_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define WL_OPS_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define WL_OPS_ALWAYS_INLINE inline
#endif

#include "columnar/internal.h"
#include "wirelog/util/log.h"
#include "../wirelog-internal.h"

#include <errno.h>
#include <stdint.h>
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
    if (!wl_columnar_expr_parse_var_col(buf, size, &pos, &col_a))
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
        if (!wl_columnar_expr_parse_var_col(buf, size, &pos, &col_b))
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
 * scan half of wl_columnar_filter_op()'s fast path; materialization is separate so
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
wl_columnar_filter_op(const wl_plan_op_t *op, eval_stack_t *stack,
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
    wl_columnar_expr_compiled_t *ce =
        (buf && bsz > 0)
        ? wl_columnar_expr_compile(buf, bsz, sess ? sess->intern : NULL)
        : NULL;

    /* Row scratch, hoisted out of the loop (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, e.rel->ncols)) {
        wl_columnar_expr_compiled_free(ce);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *const row = row_rb.ptr;
    wl_columnar_expr_context_t expr_ctx = {
        .intern = sess ? sess->intern : NULL,
        .extensions = sess ? sess->base.extension_snapshot : NULL,
        .configured_worker_count = sess ? sess->callback_configured_workers : 1,
        .active_worker_count = sess ? sess->callback_active_workers : 1,
        .parallel_execution = sess ? sess->callback_parallel_execution : false,
        .session_key = sess ? sess->callback_session_key : NULL
    };
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        col_rel_row_copy_out(e.rel, r, row);
        int pass;
        if (!buf || bsz == 0) {
            pass = 1;
        } else if (ce) {
            int64_t val = 0;
            pass = (wl_columnar_expr_eval_compiled(ce, row, e.rel->ncols,
                &val) == 0)
                       ? (val != 0 ? 1 : 0)
                       : 0; /* on error: reject row */
        } else {
            int64_t val = 0;
            wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
            int err = wl_columnar_expr_eval_run_ctx(buf, bsz, row,
                    e.rel->ncols, &val, &expr_ctx, &status);
            if (err != WL_COLUMNAR_EXPR_OK
                && status >= WL_COLUMNAR_EXPR_EXTENSION_MALFORMED) {
                if (sess)
                    sess->extension_expr_status = status;
                col_row_buf_release(&row_rb);
                wl_columnar_expr_compiled_free(ce);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return err;
            }
            pass = err == WL_COLUMNAR_EXPR_OK && val != 0;
        }
        if (pass) {
            int rc = col_rel_append_row(out, row);
            if (rc != 0) {
                col_row_buf_release(&row_rb);
                wl_columnar_expr_compiled_free(ce);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(e.rel);
                return rc;
            }
        }
    }
    col_row_buf_release(&row_rb);
    wl_columnar_expr_compiled_free(ce);

    if (e.owned)
        col_rel_destroy(e.rel);
    return eval_stack_push(stack, out, true);
}

/* --- Hash join helpers --------------------------------------------------- */

/* Returns 0 when the requested power of two cannot be represented. */

uint32_t
wl_columnar_filter_next_pow2(uint32_t n)
{
    if (n < 16)
        return 16;
    if (n > UINT32_MAX / 2u + 1u)
        return 0;
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
    wl_columnar_expr_compiled_t *ce = wl_columnar_expr_compile(buf, bsz,
            intern);
    for (uint32_t r = 0; r < rel->nrows; r++) {
        col_rel_row_copy_out(rel, r, row_buf);
        int pass;
        if (ce) {
            int64_t val = 0;
            pass = (wl_columnar_expr_eval_compiled(ce, row_buf, rel->ncols,
                &val) == 0)
                       ? (val != 0 ? 1 : 0)
                       : 0; /* fail-closed: reject row on eval error */
        } else {
            int64_t val = 0;
            int err = wl_columnar_expr_eval_run(buf, bsz, row_buf, rel->ncols,
                    &val,
                    intern);
            pass = (err == 0) ? (val != 0 ? 1 : 0) : 0; /* fail-closed */
        }
        if (pass && col_rel_append_row(out, row_buf) != 0) {
            col_row_buf_release(&rb);
            wl_columnar_expr_compiled_free(ce);
            return ENOMEM;
        }
    }
    col_row_buf_release(&rb);
    wl_columnar_expr_compiled_free(ce);
    return 0;
}

/**
 * Apply a serialized filter expression to a relation, returning a new
 * pool-allocated relation containing only the passing rows.
 * The returned relation is owned by pool and freed when the pool resets.
 * Returns NULL on allocation failure, and (Issue #1140) also when rel is
 * NULL, since col_rel_pool_new_like() now rejects a NULL template instead
 * of dereferencing it.
 */
col_rel_t *
wl_columnar_filter_apply_right_filter(const wl_plan_expr_buffer_t *fexpr,
    col_rel_t *rel,
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
uint64_t
wl_columnar_filter_fnv1a_hash(const uint8_t *buf, uint32_t len)
{
    uint64_t h = UINT64_C(14695981039346656037); /* FNV offset basis */
    for (uint32_t i = 0; i < len; i++) {
        h ^= (uint64_t)buf[i];
        h *= UINT64_C(1099511628211); /* FNV prime */
    }
    return h;
}

/**
 * wl_columnar_filter_apply_right_filter_cached: session-level cached variant of wl_columnar_filter_apply_right_filter.
 *
 * Looks up (rel_name, filter_hash) in sess->filt_cache.  On hash match a
 * full memcmp of filter bytes is performed to guard against hash collisions.
 * If found and the source relation has not grown since the entry was built,
 * returns the cached filtered relation (owned by the cache; caller must NOT
 * destroy it).
 *
 * If the source grew, the stale entry is rebuilt in-place.  If no entry
 * exists, one is created.  On any allocation failure the function returns
 * NULL (caller handles ENOMEM).  Unlike the uncached variant, @rel must be
 * non-NULL: it is dereferenced here before any constructor sees it, so the
 * Issue #1140 rejection in col_rel_pool_new_like() does not cover it.
 *
 * The returned pointer is valid until the cache entry is evicted (i.e., until
 * source_nrows changes or the session is destroyed).  Callers that hold it
 * across iterations should re-call each iteration (cheap: cache hit = O(N)
 * linear scan over filt_cache, typically 1-2 entries per session).
 */
col_rel_t *
wl_columnar_filter_apply_right_filter_cached(wl_col_session_t *sess,
    const wl_plan_expr_buffer_t *fexpr, const char *rel_name,
    col_rel_t *rel)
{
    uint64_t fhash = wl_columnar_filter_fnv1a_hash(fexpr->data, fexpr->size);

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
