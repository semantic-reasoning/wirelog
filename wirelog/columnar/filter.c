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
#include <math.h>
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

/* Append a selected row and retain its complete provenance record. */
static int
wl_columnar_filter_prepare_timestamps(col_rel_t *out, const col_rel_t *src)
{
    if (!src->timestamps)
        return 0;
    int rc = col_rel_enable_timestamps(out);
    if (rc != 0)
        return rc;
    /* col_rel_enable_timestamps() is allowed to be a no-op for a
     * zero-capacity relation.  A timestamped source still requires a real
     * destination array before the first selected row is appended. */
    return out->timestamps ? 0 : ENOMEM;
}

static int
wl_columnar_filter_status(wl_col_session_t *sess, col_rel_t *relation,
    int rc)
{
    if (rc == ENOMEM && relation
        && relation->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC && sess)
        sess->memory_budget_denied = true;
    return rc;
}

/* Appends through @batch, which holds out's mutation lease for the whole
 * fill: a lease per appended row made the filter operator's cost dominated
 * by lease acquisition (main CI timeouts after #2037).  The timestamp store
 * below therefore also happens under the held writer gates. */
static int
wl_columnar_filter_append_selected(col_rel_append_batch_t *batch,
    col_rel_t *out, const col_rel_t *src, uint32_t src_row,
    const int64_t *row, wl_col_session_t *sess)
{
    if (!batch || !out || !src || !row || src_row >= src->nrows
        || (src->timestamps && !out->timestamps))
        return EINVAL;
    uint32_t dst_row = out->nrows;
    int rc = wl_columnar_filter_status(sess, out,
            col_rel_append_batch_row(batch, row));
    if (rc == 0 && src->timestamps)
        out->timestamps[dst_row] = src->timestamps[src_row];
    return rc;
}

/* Every exit from a fill region ends its batch before out is destroyed or
 * published: destroying a relation whose writer gates are still held fails,
 * and the destroy result is ignored on these paths. */
static void
wl_columnar_filter_discard_batch_out(col_rel_append_batch_t *batch,
    col_rel_t *out)
{
    (void)col_rel_append_batch_end(batch, false);
    (void)col_rel_destroy_checked(out);
}

static int
wl_columnar_filter_dispose_input(eval_stack_t *stack, eval_entry_t *entry,
    int primary_rc)
{
    int cleanup_rc = eval_stack_dispose_entry(stack, entry);
    return cleanup_rc != 0 ? cleanup_rc : primary_rc;
}

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    bool active;
} wl_columnar_filter_scratch_t;

static int
wl_columnar_filter_scratch_reserve(wl_columnar_filter_scratch_t *scratch,
    wl_columnar_memory_governor_ref_t *governor, uint64_t bytes,
    wl_col_session_t *sess)
{
    wl_columnar_memory_admission_status_t status;
    if (!scratch)
        return EINVAL;
    memset(scratch, 0, sizeof(*scratch));
    wl_columnar_memory_reservation_init(&scratch->reservation);
    if (!governor || bytes == 0)
        return 0;
    status = wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(governor), bytes,
        &scratch->reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        scratch->active = true;
        return 0;
    }
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
        sess->memory_budget_denied = true;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
        : ENOMEM;
}

static void
wl_columnar_filter_scratch_release(wl_columnar_filter_scratch_t *scratch)
{
    if (scratch && scratch->active) {
        (void)wl_columnar_memory_rollback(&scratch->reservation);
        scratch->active = false;
    }
}

static bool
wl_columnar_filter_scratch_add(uint64_t count, uint64_t size,
    uint64_t *total)
{
    uint64_t bytes;
    return total && wl_columnar_memory_size_mul(count, size, &bytes)
           && wl_columnar_memory_size_add(*total, bytes, total);
}

#ifdef WL_SESSION_TEST_HOOKS
/* Test-only observer, called with each FILTER input after it is popped. */
void (*wl_columnar_filter_test_before_filter_dispose)(eval_stack_t *,
    eval_entry_t *);
#endif

int
wl_columnar_filter_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t e;
    int pop_rc = eval_stack_pop_relation(stack, &e);
    if (pop_rc != 0)
        return pop_rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_filter_test_before_filter_dispose)
        wl_columnar_filter_test_before_filter_dispose(stack, &e);
#endif

    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : e.rel->memory_governor;
    col_rel_t *out = NULL;
    int constructor_rc =
        wl_columnar_relation_pool_new_like_governed_checked(&out,
            sess->delta_pool, "$filter", e.rel, governor);
    if (constructor_rc != 0) {
        if (constructor_rc == ENOSPC)
            sess->memory_budget_denied = true;
        return wl_columnar_filter_dispose_input(stack, &e, constructor_rc);
    }

    bool timestamped = e.rel->timestamps != NULL;
    int timestamp_rc = timestamped
        ? wl_columnar_filter_prepare_timestamps(out, e.rel) : 0;
    if (timestamp_rc != 0) {
        timestamp_rc = wl_columnar_filter_status(sess, out, timestamp_rc);
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, timestamp_rc);
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

        if (timestamped) {
            uint64_t scratch_bytes = 0;
            wl_columnar_filter_scratch_t scratch;
            if (!wl_columnar_filter_scratch_add(ncols, sizeof(int64_t),
                &scratch_bytes)) {
                (void)col_rel_destroy_checked(out);
                return wl_columnar_filter_dispose_input(stack, &e,
                           EOVERFLOW);
            }
#ifdef __AVX2__
            if (!wl_columnar_filter_scratch_add(
                    (uint64_t)COL_FILTER_TILE + COL_FILTER_SEL_SLACK,
                    sizeof(uint32_t), &scratch_bytes)) {
                (void)col_rel_destroy_checked(out);
                return wl_columnar_filter_dispose_input(stack, &e,
                           EOVERFLOW);
            }
#endif
            int scratch_rc = wl_columnar_filter_scratch_reserve(&scratch,
                    governor, scratch_bytes, sess);
            if (scratch_rc != 0) {
                (void)col_rel_destroy_checked(out);
                return wl_columnar_filter_dispose_input(stack, &e,
                           scratch_rc);
            }
            int64_t *row = (int64_t *)malloc((size_t)ncols * sizeof(*row));
            if (!row) {
                wl_columnar_filter_scratch_release(&scratch);
                (void)col_rel_destroy_checked(out);
                return wl_columnar_filter_dispose_input(stack, &e, ENOMEM);
            }
            bool use_selection = false;
#ifdef __AVX2__
            use_selection = true;
#endif
            col_rel_append_batch_t batch = { 0 };
            int batch_rc = wl_columnar_filter_status(sess, out,
                    col_rel_append_batch_begin(&batch, out));
            if (batch_rc != 0) {
                free(row);
                wl_columnar_filter_scratch_release(&scratch);
                (void)col_rel_destroy_checked(out);
                return wl_columnar_filter_dispose_input(stack, &e, batch_rc);
            }
            if (use_selection) {
                uint32_t *sel = (uint32_t *)malloc(
                    ((size_t)COL_FILTER_TILE + COL_FILTER_SEL_SLACK)
                    * sizeof(*sel));
                if (!sel) {
                    free(row);
                    wl_columnar_filter_scratch_release(&scratch);
                    wl_columnar_filter_discard_batch_out(&batch, out);
                    return wl_columnar_filter_dispose_input(stack, &e,
                               ENOMEM);
                }
                for (uint32_t base = 0; base < nrows;) {
                    uint32_t chunk = nrows - base;
                    if (chunk > COL_FILTER_TILE)
                        chunk = COL_FILTER_TILE;
                    uint32_t nsel = col_filter_select_rows(col_a + base,
                            col_b_ptr ? col_b_ptr + base : NULL, cmp.const_b,
                            chunk, cmp.cmp_op, sel);
                    if (base == 0 && nsel > chunk - (chunk / 8)) {
                        use_selection = false;
                        break;
                    }
                    for (uint32_t i = 0; i < nsel; i++) {
                        uint32_t src_row = base + sel[i];
                        for (uint32_t c = 0; c < ncols; c++)
                            row[c] = columns[c][src_row];
                        int rc = wl_columnar_filter_append_selected(&batch,
                                out, e.rel, src_row, row, sess);
                        if (rc != 0) {
                            free(sel);
                            free(row);
                            wl_columnar_filter_scratch_release(&scratch);
                            wl_columnar_filter_discard_batch_out(&batch, out);
                            return wl_columnar_filter_dispose_input(stack,
                                       &e, rc);
                        }
                    }
                    base += chunk;
                }
                free(sel);
            }
            if (!use_selection) {
                out->nrows = 0;
                for (uint32_t r = 0; r < nrows; r++) {
                    int64_t b_val = col_b_ptr ? col_b_ptr[r] : cmp.const_b;
                    if (!col_filter_cmp_scalar(col_a[r], b_val, cmp.cmp_op))
                        continue;
                    for (uint32_t c = 0; c < ncols; c++)
                        row[c] = columns[c][r];
                    int rc = wl_columnar_filter_append_selected(&batch, out,
                            e.rel, r, row, sess);
                    if (rc != 0) {
                        free(row);
                        wl_columnar_filter_scratch_release(&scratch);
                        wl_columnar_filter_discard_batch_out(&batch, out);
                        return wl_columnar_filter_dispose_input(stack, &e, rc);
                    }
                }
            }
            free(row);
            wl_columnar_filter_scratch_release(&scratch);
            batch_rc = col_rel_append_batch_end(&batch, true);
            if (batch_rc != 0) {
                (void)col_rel_destroy_checked(out);
                return wl_columnar_filter_dispose_input(stack, &e, batch_rc);
            }
            int cleanup_rc = wl_columnar_filter_dispose_input(stack, &e, 0);
            if (cleanup_rc != 0) {
                (void)col_rel_destroy_checked(out);
                return cleanup_rc;
            }
            return eval_stack_push(stack, out, true);
        }

        /* Pre-allocate output buffer sized for worst-case (all rows pass). */
        uint64_t scratch_bytes = 0;
        uint64_t cells = 0;
        wl_columnar_filter_scratch_t scratch;
        if (!wl_columnar_memory_size_mul(nrows, ncols, &cells)
            || !wl_columnar_filter_scratch_add(cells, sizeof(int64_t),
            &scratch_bytes)) {
            col_rel_destroy(out);
            return wl_columnar_filter_dispose_input(stack, &e, EOVERFLOW);
        }
#ifdef __AVX2__
        if (!wl_columnar_filter_scratch_add(
                (uint64_t)COL_FILTER_TILE + COL_FILTER_SEL_SLACK,
                sizeof(uint32_t), &scratch_bytes)) {
            col_rel_destroy(out);
            return wl_columnar_filter_dispose_input(stack, &e, EOVERFLOW);
        }
#endif
        int scratch_rc = wl_columnar_filter_scratch_reserve(&scratch,
                governor, scratch_bytes, sess);
        if (scratch_rc != 0) {
            col_rel_destroy(out);
            return wl_columnar_filter_dispose_input(stack, &e, scratch_rc);
        }
        int64_t *tmp = (int64_t *)malloc((size_t)cells * sizeof(int64_t));
        if (!tmp) {
            wl_columnar_filter_scratch_release(&scratch);
            col_rel_destroy(out);
            return wl_columnar_filter_dispose_input(stack, &e, ENOMEM);
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
                wl_columnar_filter_scratch_release(&scratch);
                col_rel_destroy(out);
                return wl_columnar_filter_dispose_input(stack, &e, ENOMEM);
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

        /* Bulk-copy the passing rows into the output relation, under one
         * lease per storage epoch rather than one per row. */
        col_rel_append_batch_t batch = { 0 };
        int batch_rc = col_rel_append_batch_begin(&batch, out);
        for (uint32_t r = 0; batch_rc == 0 && r < nout; r++)
            batch_rc = col_rel_append_batch_row(&batch,
                    tmp + (size_t)r * ncols);
        if (batch_rc == 0)
            batch_rc = col_rel_append_batch_end(&batch, true);
        if (batch_rc != 0) {
            free(tmp);
            wl_columnar_filter_scratch_release(&scratch);
            (void)col_rel_append_batch_end(&batch, false);
            batch_rc = wl_columnar_filter_status(sess, out, batch_rc);
            col_rel_destroy(out);
            return wl_columnar_filter_dispose_input(stack, &e, batch_rc);
        }
        free(tmp);
        wl_columnar_filter_scratch_release(&scratch);
        int cleanup_rc = wl_columnar_filter_dispose_input(stack, &e, 0);
        if (cleanup_rc != 0) {
            (void)col_rel_destroy_checked(out);
            return cleanup_rc;
        }
        return eval_stack_push(stack, out, true);
    }

    /* Slow path: pre-compile expression once, then evaluate per row. */
    int compile_rc = ENOTSUP;
    wl_columnar_expr_compiled_t *ce =
        (buf && bsz > 0)
        ? wl_columnar_expr_compile_governed(buf, bsz,
            sess ? sess->intern : NULL, governor, sess, &compile_rc)
        : NULL;
    if (compile_rc != ENOTSUP && compile_rc != 0) {
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, compile_rc);
    }

    /* Row scratch, hoisted out of the loop (#1000). */
    wl_columnar_filter_scratch_t row_scratch;
    uint64_t row_scratch_bytes = 0;
    if (e.rel->ncols > COL_STACK_MAX
        && !wl_columnar_filter_scratch_add(e.rel->ncols, sizeof(int64_t),
        &row_scratch_bytes)) {
        wl_columnar_expr_compiled_free(ce);
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, EOVERFLOW);
    }
    int row_scratch_rc = wl_columnar_filter_scratch_reserve(&row_scratch,
            governor, row_scratch_bytes, sess);
    if (row_scratch_rc != 0) {
        wl_columnar_expr_compiled_free(ce);
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, row_scratch_rc);
    }
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, e.rel->ncols)) {
        wl_columnar_filter_scratch_release(&row_scratch);
        wl_columnar_expr_compiled_free(ce);
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, ENOMEM);
    }

    int64_t *const row = row_rb.ptr;
    col_rel_append_batch_t batch = { 0 };
    int batch_rc = wl_columnar_filter_status(sess, out,
            col_rel_append_batch_begin(&batch, out));
    if (batch_rc != 0) {
        col_row_buf_release(&row_rb);
        wl_columnar_filter_scratch_release(&row_scratch);
        wl_columnar_expr_compiled_free(ce);
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, batch_rc);
    }
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
                if (sess && status != WL_COLUMNAR_EXPR_ALLOCATION_FAILURE)
                    sess->extension_expr_status = status;
                col_row_buf_release(&row_rb);
                wl_columnar_filter_scratch_release(&row_scratch);
                wl_columnar_expr_compiled_free(ce);
                wl_columnar_filter_discard_batch_out(&batch, out);
                /* A refused string allocation fails the step as a memory
                 * error (Issue #1470). */
                return wl_columnar_filter_dispose_input(stack, &e,
                           status == WL_COLUMNAR_EXPR_ALLOCATION_FAILURE
                    ? ENOMEM : err);
            }
            pass = err == WL_COLUMNAR_EXPR_OK && val != 0;
        }
        if (pass) {
            int rc = wl_columnar_filter_append_selected(&batch, out, e.rel,
                    r, row, sess);
            if (rc != 0) {
                col_row_buf_release(&row_rb);
                wl_columnar_filter_scratch_release(&row_scratch);
                wl_columnar_expr_compiled_free(ce);
                wl_columnar_filter_discard_batch_out(&batch, out);
                return wl_columnar_filter_dispose_input(stack, &e, rc);
            }
        }
    }
    col_row_buf_release(&row_rb);
    wl_columnar_filter_scratch_release(&row_scratch);
    wl_columnar_expr_compiled_free(ce);
    batch_rc = col_rel_append_batch_end(&batch, true);
    if (batch_rc != 0) {
        (void)col_rel_destroy_checked(out);
        return wl_columnar_filter_dispose_input(stack, &e, batch_rc);
    }

    int cleanup_rc = wl_columnar_filter_dispose_input(stack, &e, 0);
    if (cleanup_rc != 0) {
        (void)col_rel_destroy_checked(out);
        return cleanup_rc;
    }
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
    col_rel_t *out, wl_intern_t *intern,
    wl_columnar_memory_governor_ref_t *governor, wl_col_session_t *sess)
{
    /* Row scratch, hoisted out of both loops below (#1000). */
    wl_columnar_filter_scratch_t scratch;
    uint64_t scratch_bytes = 0;
    if (rel->ncols > COL_STACK_MAX
        && !wl_columnar_filter_scratch_add(rel->ncols, sizeof(int64_t),
        &scratch_bytes))
        return EOVERFLOW;
    int scratch_rc = wl_columnar_filter_scratch_reserve(&scratch, governor,
            scratch_bytes, NULL);
    if (scratch_rc != 0)
        return scratch_rc;
    col_row_buf_t rb;
    if (!col_row_buf_init(&rb, rel->ncols)) {
        wl_columnar_filter_scratch_release(&scratch);
        return ENOMEM;
    }
    int64_t *row_buf = rb.ptr;

    /* Fast path: simple colA CMP CONST or colA CMP colB predicate */
    simple_filter_cmp_t cmp;
    col_rel_append_batch_t batch = { 0 };
    if (filter_is_simple_cmp(buf, bsz, &cmp)) {
        int rc = wl_columnar_filter_status(sess, out,
                col_rel_append_batch_begin(&batch, out));
        for (uint32_t r = 0; rc == 0 && r < rel->nrows; r++) {
            col_rel_row_copy_out(rel, r, row_buf);
            if (col_filter_cmp_row(row_buf, rel->ncols, &cmp))
                rc = wl_columnar_filter_append_selected(&batch, out, rel, r,
                        row_buf, sess);
        }
        int end_rc = col_rel_append_batch_end(&batch, rc == 0);
        col_row_buf_release(&rb);
        wl_columnar_filter_scratch_release(&scratch);
        return rc != 0 ? rc : end_rc;
    }

    /* Slow path: compile once, evaluate per row */
    int compile_rc = ENOTSUP;
    wl_columnar_expr_compiled_t *ce = wl_columnar_expr_compile_governed(buf,
            bsz, intern, governor, sess, &compile_rc);
    if (compile_rc != ENOTSUP && compile_rc != 0) {
        col_row_buf_release(&rb);
        return compile_rc;
    }
    int batch_rc = wl_columnar_filter_status(sess, out,
            col_rel_append_batch_begin(&batch, out));
    if (batch_rc != 0) {
        col_row_buf_release(&rb);
        wl_columnar_filter_scratch_release(&scratch);
        wl_columnar_expr_compiled_free(ce);
        return batch_rc;
    }
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
        if (pass) {
            int rc = wl_columnar_filter_append_selected(&batch, out, rel, r,
                    row_buf, sess);
            if (rc != 0) {
                (void)col_rel_append_batch_end(&batch, false);
                col_row_buf_release(&rb);
                wl_columnar_filter_scratch_release(&scratch);
                wl_columnar_expr_compiled_free(ce);
                return rc;
            }
        }
    }
    batch_rc = col_rel_append_batch_end(&batch, true);
    col_row_buf_release(&rb);
    wl_columnar_filter_scratch_release(&scratch);
    wl_columnar_expr_compiled_free(ce);
    return batch_rc;
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
wl_columnar_filter_apply_right_filter_governed(
    const wl_plan_expr_buffer_t *fexpr,
    col_rel_t *rel, delta_pool_t *pool, wl_intern_t *intern,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *out = NULL;
    return wl_columnar_filter_apply_right_filter_governed_checked(fexpr,
               rel, pool, intern, governor, NULL, &out) == 0 ? out : NULL;
}

int
wl_columnar_filter_apply_right_filter_governed_checked(
    const wl_plan_expr_buffer_t *fexpr, col_rel_t *rel, delta_pool_t *pool,
    wl_intern_t *intern, wl_columnar_memory_governor_ref_t *governor,
    wl_col_session_t *sess, col_rel_t **out)
{
    int rc;
    if (!fexpr || !rel || (fexpr->size != 0 && !fexpr->data) || !out)
        return EINVAL;
    *out = NULL;
    rc = wl_columnar_relation_pool_new_like_governed_checked(out, pool,
            "$rfilter", rel, governor);
    if (rc != 0) {
        if (rc == ENOSPC && sess)
            sess->memory_budget_denied = true;
        return rc;
    }
    if (rel->timestamps) {
        rc = wl_columnar_filter_prepare_timestamps(*out, rel);
        if (rc != 0)
            goto failed;
    }
    rc = fill_filtered_rel(fexpr->data, fexpr->size, rel, *out, intern,
            governor, sess);
    if (rc != 0)
        goto failed;
    return 0;
failed:
    rc = wl_columnar_filter_status(sess, *out, rc);
    (void)col_rel_destroy_checked(*out);
    *out = NULL;
    return rc;
}

col_rel_t *
wl_columnar_filter_apply_right_filter(const wl_plan_expr_buffer_t *fexpr,
    col_rel_t *rel, delta_pool_t *pool, wl_intern_t *intern)
{
    return wl_columnar_filter_apply_right_filter_governed(fexpr, rel,
               pool, intern, NULL);
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
 * Session-level cached variants of wl_columnar_filter_apply_right_filter.
 *
 * Both look up (rel_name, filter_hash) in sess->filt_cache; on a hash match
 * the filter bytes are compared in full to guard against collisions.  A
 * fresh entry (source token and row count unchanged since it was built)
 * yields the cached filtered relation, owned by the cache; the caller must
 * NOT destroy it.  A stale entry is rebuilt in place unless a lease holds
 * it (Issue #1435): then it is marked deferred, hidden until its last
 * release destroys it, and the lookup reports the cache unavailable.  A
 * miss creates an entry, except that the entry array is not grown while
 * any lease is active.  Allocation failure also yields NULL; callers use
 * an owned filtered relation for that op.  Unlike the uncached variant,
 * @rel must be non-NULL: it is dereferenced here before any constructor
 * sees it, so the Issue #1140 rejection in col_rel_pool_new_like() does not
 * cover it.
 *
 * The _pin variant returns the relation under a lease that keeps it valid
 * until col_filt_cache_pin_release().  The unleased variant takes and
 * releases a lease around the lookup, so its pointer is valid only until
 * the next lookup, invalidation or release; callers that hold it across
 * iterations re-call each iteration (cheap: a linear scan over filt_cache,
 * typically 1-2 entries per session).
 */
static void
filt_cache_pin_take(wl_col_session_t *sess, col_filt_cache_entry_t *e,
    col_filt_cache_pin_t *pin)
{
    e->pin_count++;
    sess->filt_cache_active_pins++;
    pin->session = sess;
    pin->entry = e;
    pin->rel = e->filtered;
    pin->active = true;
}

static int
filt_cache_reserve(wl_col_session_t *sess, uint64_t bytes,
    wl_columnar_memory_reservation_t **out)
{
    wl_columnar_memory_reservation_t *reservation;
    wl_columnar_memory_admission_status_t status;
    if (!out)
        return EINVAL;
    *out = NULL;
    if (!sess || !sess->memory_governor || bytes == 0)
        return 0;
    reservation = (wl_columnar_memory_reservation_t *)malloc(
        sizeof(*reservation));
    if (!reservation)
        return ENOMEM;
    wl_columnar_memory_reservation_init(reservation);
    status = wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(sess->memory_governor), bytes,
        reservation);
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            sess->memory_budget_denied = true;
        free(reservation);
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
            : ENOMEM;
    }
    *out = reservation;
    return 0;
}

static void
filt_cache_release_reservation(wl_columnar_memory_reservation_t **slot)
{
    if (!slot || !*slot)
        return;
    (void)wl_columnar_memory_rollback(*slot);
    free(*slot);
    *slot = NULL;
}

static int
filt_cache_metadata_bytes(const char *rel_name, uint32_t filter_size,
    uint64_t *bytes)
{
    uint64_t name_bytes;
    if (!rel_name || !bytes)
        return EINVAL;
    if (!wl_columnar_memory_size_add((uint64_t)strlen(rel_name), 1,
        &name_bytes)
        || !wl_columnar_memory_size_add(name_bytes, filter_size, bytes))
        return EOVERFLOW;
    return 0;
}

static col_rel_t *
wl_columnar_filter_apply_right_filter_cached_pin_impl(wl_col_session_t *sess,
    const wl_plan_expr_buffer_t *fexpr, const char *rel_name, col_rel_t *rel,
    col_filt_cache_pin_t *pin, int *status_out, bool *cache_unavailable)
{
    if (status_out)
        *status_out = 0;
    if (cache_unavailable)
        *cache_unavailable = false;
    if (!pin || !status_out || !cache_unavailable || !sess || !fexpr
        || !rel_name || !rel || (fexpr->size && !fexpr->data)) {
        if (status_out)
            *status_out = EINVAL;
        return NULL;
    }
    memset(pin, 0, sizeof(*pin));
    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : rel->memory_governor;
    uint64_t fhash = wl_columnar_filter_fnv1a_hash(fexpr->data, fexpr->size);

    /* Linear scan: filt_cache is tiny (one entry per unique filter predicate) */
    for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
        col_filt_cache_entry_t *e = &sess->filt_cache[i];
        if (e->filter_hash != fhash)
            continue;
        if (!e->rel_name || strcmp(e->rel_name, rel_name) != 0)
            continue;
        /* Full content comparison to guard against hash collisions */
        if (e->filter_size != fexpr->size
            || memcmp(e->filter_data, fexpr->data, fexpr->size) != 0)
            continue;
        /* A deferred entry is hidden until its last reader releases it;
         * the caller uses an owned filtered relation meanwhile (#1435). */
        if (e->evict_deferred) {
            *cache_unavailable = true;
            return NULL;
        }
        /* Cache hit: the freshness token is the contract (Issue #1438);
         * the row count is kept as a cheap second guard. */
        bool fresh = e->filtered
            && e->filtered->memory_governor == governor
            && wl_columnar_relation_snapshot_equal(e->source_snapshot,
                wl_columnar_relation_snapshot(rel))
            && e->source_nrows == rel->nrows;
        if (!fresh) {
            /* Never replace a filtered relation under its readers: defer
             * until the last release and report it unavailable. */
            if (e->pin_count > 0) {
                e->evict_deferred = true;
                *cache_unavailable = true;
                return NULL;
            }
            /* Source changed and nobody reads the old copy: rebuild in place */
            if (e->filtered)
                col_rel_destroy(e->filtered);
            int rebuild_rc =
                wl_columnar_relation_pool_new_like_governed_checked(
                &e->filtered, NULL, "$rfilter_cache", rel, governor);
            if (rebuild_rc != 0) {
                *status_out = rebuild_rc;
                return NULL;
            }
            int timestamp_rc = rel->timestamps
                ? wl_columnar_filter_prepare_timestamps(e->filtered, rel) : 0;
            if (timestamp_rc != 0) {
                timestamp_rc = wl_columnar_filter_status(sess, e->filtered,
                        timestamp_rc);
                col_rel_destroy(e->filtered);
                e->filtered = NULL;
                *status_out = timestamp_rc;
                return NULL;
            }
            int fill_rc = fill_filtered_rel(fexpr->data, fexpr->size, rel,
                    e->filtered, sess->intern, governor, sess);
            if (fill_rc != 0) {
                fill_rc = wl_columnar_filter_status(sess, e->filtered,
                        fill_rc);
                col_rel_destroy(e->filtered);
                e->filtered = NULL;
                *status_out = fill_rc;
                return NULL;
            }
            e->source_nrows = rel->nrows;
            e->source_snapshot = wl_columnar_relation_snapshot(rel);
        }
        filt_cache_pin_take(sess, e, pin);
        return e->filtered;
    }

    /* Cache miss — build a new entry */
    if (sess->filt_cache_count == sess->filt_cache_cap) {
        /* Growth moves every entry; refuse it while a lease points into
         * the array (Issue #1435).  The caller falls back to an owned
         * filtered relation for this op. */
        if (sess->filt_cache_active_pins > 0) {
            *cache_unavailable = true;
            return NULL;
        }
        uint32_t new_cap;
        if (sess->filt_cache_cap == 0)
            new_cap = 4;
        else {
            if (sess->filt_cache_cap > UINT32_MAX / 2u) {
                *status_out = EOVERFLOW;
                return NULL;
            }
            new_cap = sess->filt_cache_cap * 2;
        }
        uint64_t array_bytes = 0;
        wl_columnar_memory_reservation_t *array_reservation = NULL;
        if (!wl_columnar_memory_size_mul(new_cap,
            sizeof(*sess->filt_cache), &array_bytes)) {
            *status_out = EOVERFLOW;
            return NULL;
        }
        int array_rc = filt_cache_reserve(sess, array_bytes,
                &array_reservation);
        if (array_rc != 0) {
            *status_out = array_rc;
            return NULL;
        }
        void *tmp = realloc(sess->filt_cache,
                new_cap * sizeof(*sess->filt_cache));
        if (!tmp) {
            filt_cache_release_reservation(&array_reservation);
            *status_out = ENOMEM;
            return NULL;
        }
        filt_cache_release_reservation(&sess->filt_cache_array_reservation);
        sess->filt_cache = tmp;
        sess->filt_cache_cap = new_cap;
        sess->filt_cache_array_reservation = array_reservation;
    }
    uint32_t idx = sess->filt_cache_count;
    memset(&sess->filt_cache[idx], 0, sizeof(sess->filt_cache[idx]));
    uint64_t metadata_bytes = 0;
    int metadata_rc = filt_cache_metadata_bytes(rel_name, fexpr->size,
            &metadata_bytes);
    if (metadata_rc != 0) {
        *status_out = metadata_rc;
        return NULL;
    }
    metadata_rc = filt_cache_reserve(sess, metadata_bytes,
            &sess->filt_cache[idx].metadata_reservation);
    if (metadata_rc != 0) {
        *status_out = metadata_rc;
        return NULL;
    }
    sess->filt_cache[idx].rel_name = strdup(rel_name);
    if (!sess->filt_cache[idx].rel_name) {
        filt_cache_release_reservation(
            &sess->filt_cache[idx].metadata_reservation);
        *status_out = ENOMEM;
        return NULL;
    }
    /* Store an owned copy of the filter expression bytes for full key compare */
    sess->filt_cache[idx].filter_data = (uint8_t *)malloc(fexpr->size);
    if (!sess->filt_cache[idx].filter_data) {
        free(sess->filt_cache[idx].rel_name);
        filt_cache_release_reservation(
            &sess->filt_cache[idx].metadata_reservation);
        *status_out = ENOMEM;
        return NULL;
    }
    memcpy(sess->filt_cache[idx].filter_data, fexpr->data, fexpr->size);
    sess->filt_cache[idx].filter_size = fexpr->size;
    sess->filt_cache[idx].filter_hash = fhash;
    sess->filt_cache[idx].source_nrows = 0; /* will be set after fill */
    sess->filt_cache[idx].source_snapshot = (col_relation_snapshot_t){ 0, 0,
                                                                       0 };
    int constructor_rc = wl_columnar_relation_pool_new_like_governed_checked(
        &sess->filt_cache[idx].filtered, NULL, "$rfilter_cache", rel,
        governor);
    if (constructor_rc != 0) {
        free(sess->filt_cache[idx].filter_data);
        free(sess->filt_cache[idx].rel_name);
        filt_cache_release_reservation(
            &sess->filt_cache[idx].metadata_reservation);
        *status_out = constructor_rc;
        return NULL;
    }
    int timestamp_rc = rel->timestamps
        ? wl_columnar_filter_prepare_timestamps(
        sess->filt_cache[idx].filtered, rel) : 0;
    if (timestamp_rc != 0) {
        timestamp_rc = wl_columnar_filter_status(sess,
                sess->filt_cache[idx].filtered, timestamp_rc);
        col_rel_destroy(sess->filt_cache[idx].filtered);
        sess->filt_cache[idx].filtered = NULL;
        free(sess->filt_cache[idx].filter_data);
        free(sess->filt_cache[idx].rel_name);
        filt_cache_release_reservation(
            &sess->filt_cache[idx].metadata_reservation);
        *status_out = timestamp_rc;
        return NULL;
    }
    sess->filt_cache_count++;

    /* Fill the new entry */
    col_rel_t *out = sess->filt_cache[idx].filtered;
    int fill_rc = fill_filtered_rel(fexpr->data, fexpr->size, rel, out,
            sess->intern, governor, sess);
    if (fill_rc != 0) {
        fill_rc = wl_columnar_filter_status(sess, out, fill_rc);
        col_rel_destroy(out);
        sess->filt_cache[idx].filtered = NULL;
        free(sess->filt_cache[idx].filter_data);
        free(sess->filt_cache[idx].rel_name);
        filt_cache_release_reservation(
            &sess->filt_cache[idx].metadata_reservation);
        memset(&sess->filt_cache[idx], 0, sizeof(sess->filt_cache[idx]));
        /* The entry was never published: roll the count back so a failed
         * build cannot strand a cache slot or make later growth skip it. */
        sess->filt_cache_count--;
        *status_out = fill_rc;
        return NULL;
    }
    sess->filt_cache[idx].source_nrows = rel->nrows;
    sess->filt_cache[idx].source_snapshot
        = wl_columnar_relation_snapshot(rel);
    filt_cache_pin_take(sess, &sess->filt_cache[idx], pin);
    return out;
}

int
wl_columnar_filter_apply_right_filter_cached_pin_checked(
    wl_col_session_t *sess, const wl_plan_expr_buffer_t *fexpr,
    const char *rel_name, col_rel_t *rel, col_filt_cache_pin_t *pin,
    col_rel_t **out, bool *cache_unavailable)
{
    int status = 0;
    if (!out || !cache_unavailable)
        return EINVAL;
    *out = NULL;
    *cache_unavailable = false;
    *out = wl_columnar_filter_apply_right_filter_cached_pin_impl(sess,
            fexpr, rel_name, rel, pin, &status, cache_unavailable);
    if (status == ENOSPC && sess)
        sess->memory_budget_denied = true;
    return *out ? 0 : status;
}

col_rel_t *
wl_columnar_filter_apply_right_filter_cached_pin(wl_col_session_t *sess,
    const wl_plan_expr_buffer_t *fexpr, const char *rel_name, col_rel_t *rel,
    col_filt_cache_pin_t *pin)
{
    col_rel_t *out = NULL;
    bool cache_unavailable = false;
    (void)wl_columnar_filter_apply_right_filter_cached_pin_checked(sess,
        fexpr, rel_name, rel, pin, &out, &cache_unavailable);
    return out;
}

/* Unleased lookup: take and immediately release a lease.  Pin and release
 * are adjacent, so no growth, compaction or deferral can observe the
 * lease; the result is exactly the pre-#1435 behaviour. */
col_rel_t *
wl_columnar_filter_apply_right_filter_cached(wl_col_session_t *sess,
    const wl_plan_expr_buffer_t *fexpr, const char *rel_name,
    col_rel_t *rel)
{
    col_filt_cache_pin_t pin = { 0 };
    col_rel_t *out = wl_columnar_filter_apply_right_filter_cached_pin(sess,
            fexpr, rel_name, rel, &pin);
    col_filt_cache_pin_release(&pin);
    return out;
}

void
col_filt_cache_pin_release(col_filt_cache_pin_t *pin)
{
    col_filt_cache_entry_t *e;
    wl_col_session_t *sess;

    if (!pin || !pin->active || !pin->entry)
        return;
    e = pin->entry;
    sess = pin->session;
    if (e->pin_count > 0)
        e->pin_count--;
    if (sess && sess->filt_cache_active_pins > 0)
        sess->filt_cache_active_pins--;
    if (e->pin_count == 0 && e->evict_deferred) {
        /* Last reader gone: destroy the stale copy now and let the next
         * lookup rebuild it against whatever source it is given (the
         * source may have been replaced by session_add_rel meanwhile). */
        if (e->filtered)
            col_rel_destroy(e->filtered);
        e->filtered = NULL;
        e->source_nrows = 0;
        e->source_snapshot = (col_relation_snapshot_t){ 0, 0, 0 };
        e->evict_deferred = false;
    }
    memset(pin, 0, sizeof(*pin));
}

/* Numeric transactional FILTER source draft. Tariffs below are provisional
 * until the independent expanded-source gate; no measured bound is claimed. */
enum {
    WL_COLUMNAR_FILTER_ATTEMPT_ENTRY_WORK = 224,
    WL_COLUMNAR_FILTER_ATTEMPT_INIT_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_UNARY_BEGIN_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_INPUT_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_GOVERNOR_PIN_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_SIZE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RESERVE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_ALLOCATE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_COMMIT_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_PROGRAM_PREDICATE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_BATCH_BEGIN_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_NUMERIC_EVAL_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SELECTION_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_INIT_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_BEGIN_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_SHAPE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_NEWLIKE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_PROGRAM_COMPILE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_BATCH_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_GATHER_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SELECTED_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_BATCH_INIT_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_ROW_WORK = 128,
    /* Provisional named first-retirement admissions, not count certificates. */
    WL_COLUMNAR_FILTER_ATTEMPT_LIFECYCLE_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_UNARY_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_REFERENCE_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RETIRE_WORK = 192,
    WL_COLUMNAR_FILTER_ATTEMPT_PROGRAM_RETIRE_WORK = 192,
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_filter_test_attempt_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t);
int wl_columnar_filter_test_attempt_fail_allocation = -1;
#endif

static int
wl_columnar_filter_attempt_guard(wl_evaluation_control_t *control, uint64_t id)
{
    return !control || !id
           || wl_evaluation_control_active_attempt_id(control) != id
        ? EINVAL : 0;
}

static int
wl_columnar_filter_attempt_charge(wl_evaluation_control_t *control,
    uint64_t id, uint64_t work)
{
    int rc = wl_columnar_filter_attempt_guard(control, id);
    return rc ? rc : wl_evaluation_control_charge(control, work);
}

static int
wl_columnar_filter_attempt_tail(wl_evaluation_control_t *control,
    uint64_t id, unsigned phase, uint64_t progress)
{
    int rc = wl_columnar_filter_attempt_guard(control, id);
    if (rc)
        return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_filter_test_attempt_checkpoint)
        wl_columnar_filter_test_attempt_checkpoint(control, phase, progress);
#else
    (void)phase;
    (void)progress;
#endif
    return wl_columnar_filter_attempt_charge(control, id, 0);
}

/* A child can finish/restart the attempt in a hook. Check immutable locals
 * before any further callback/debit, and arbitrate positive control errors. */
static int
wl_columnar_filter_attempt_return(wl_evaluation_control_t *control,
    uint64_t id, int child_result)
{
    int tail = wl_columnar_filter_attempt_charge(control, id, 0);
    return tail > 0 ? tail : child_result ? child_result : tail;
}

static void *
wl_columnar_filter_attempt_allocate(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_filter_test_attempt_fail_allocation >= 0) {
        if (wl_columnar_filter_test_attempt_fail_allocation == 0)
            return NULL;
        --wl_columnar_filter_test_attempt_fail_allocation;
    }
#endif
    return malloc(bytes);
}

static void
wl_columnar_filter_attempt_single_init(
    wl_columnar_relation_mutation_single_t *single)
{
    single->set.identity = 0;
    single->set.acquisition_nonce = 0;
    single->set.roles = 0;
    single->set.descriptors = 0;
    single->set.owners = 0;
    single->set.leases = 0;
    single->set.initializations = 0;
    single->set.descriptor_count = 0;
    single->set.owner_count = 0;
    single->set.lease_count = 0;
    single->set.initialization_count = 0;
    single->set.descriptors_acquired = 0;
    single->set.owners_acquired = 0;
    single->set.terminal_sequence = 0;
    single->set.terminal_expected_events = 0;
    single->set.terminal_observed_events = 0;
    single->set.terminal_sequence_consumed = 0;
    single->set.published = 0;
    single->role.relation = 0;
    single->role.role_flags = 0;
    single->descriptor.relation = 0;
    single->owners[0].owner = 0;
    single->owners[1].owner = 0;
    single->lease.identity = 0;
    single->lease.acquisition_nonce = 0;
    single->lease.set = 0;
    single->lease.relation = 0;
    single->lease.owner = 0;
    single->lease.descriptor_slot = 0;
    single->lease.owner_slot = 0;
    single->lease.self_owner_slot = 0;
    single->lease.relation_identity = 0;
    single->lease.relation_generation = 0;
    single->lease.owner_identity = 0;
    single->lease.owner_generation = 0;
    single->lease.role_flags = 0;
    single->lease.detached = 0;
    single->lease.storage_transitioned = 0;
    single->initialization.relation = 0;
    single->initialization.owner_identity = 0;
    single->initialization.owner_generation = 0;
    single->initialization.borrows = 0;
    single->descriptor.writer.owner = 0;
    single->descriptor.writer.secondary_owner = 0;
    single->descriptor.writer.identity = 0;
    single->descriptor.writer.thread_valid = 0;
    single->owners[0].writer.owner = 0;
    single->owners[0].writer.secondary_owner = 0;
    single->owners[0].writer.identity = 0;
    single->owners[0].writer.thread_valid = 0;
    single->owners[1].writer.owner = 0;
    single->owners[1].writer.secondary_owner = 0;
    single->owners[1].writer.identity = 0;
    single->owners[1].writer.thread_valid = 0;
}

void
wl_columnar_filter_attempt_batch_init(
    wl_columnar_relation_append_batch_t *batch)
{
    batch->identity = NULL; batch->control = NULL; batch->attempt_id = 0;
    batch->phase = WL_COLUMNAR_RELATION_BATCH_EMPTY;
    batch->raw_mode = false; batch->result_saved = false;
    batch->saved_result = 0;
    batch->raw.relation = NULL; batch->raw.active = false;
    batch->raw.verified_generation = 0;
    wl_columnar_filter_attempt_single_init(&batch->raw.single);
}

static bool
wl_columnar_filter_attempt_thread(const wl_columnar_filter_context_t *ctx)
{
    if (!ctx->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(ctx->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return ctx->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(ctx->owner_thread, pthread_self()) != 0;
#endif
}

static void
wl_columnar_filter_attempt_status_init(wl_columnar_filter_status_t *status)
{
    status->progress = WL_COLUMNAR_FILTER_PROGRESS_NOT_STARTED;
    status->execution_result = 0; status->cleanup_result = 0;
    status->semantic_result = 0; status->committed = false;
}

static void
wl_columnar_filter_attempt_status(const wl_columnar_filter_context_t *ctx,
    wl_columnar_filter_status_t *status, wl_columnar_filter_progress_t progress)
{
    status->progress = progress;
    status->execution_result = ctx->execution_result;
    status->cleanup_result = ctx->cleanup_result;
    status->semantic_result = ctx->semantic_result;
    status->committed = ctx->committed;
}

int
wl_columnar_filter_op_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_filter_op_attempt_start_owned(op, frame, sess, control,
               attempt_id, status,
               NULL);
}

int
wl_columnar_filter_op_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!status || !op) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, sess, &stack,
            owner);
    if (rc) return rc;
    wl_columnar_filter_attempt_status_init(status);
    if (!control) {
        int result = wl_columnar_filter_op(op, stack, sess);
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        return 0;
    }
    wl_columnar_filter_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    ctx->program = NULL; ctx->output = NULL; ctx->private_output = NULL;
    ctx->governor = NULL;
    ctx->values = ctx->inline_values;
    ctx->source_initialized = false; ctx->batch_initialized = false;
    ctx->drain_initialized = false;
    ctx->heap = false; ctx->admitted = false; ctx->credit_committed = false;
    ctx->ref_held = false; ctx->unary_started = false;
    ctx->committed = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0; ctx->control = NULL; ctx->pin_owned = false;
    ctx->thread_valid = false; ctx->busy = false;
    ctx->frame = frame; ctx->session = sess; ctx->attempt_id = attempt_id;
    /* Existing control/reference interior has no finite CAS or time claim. */
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    ctx->control = control; ctx->pin_owned = true; ctx->identity = ctx;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->busy = true;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_ENTRY_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 1, 0);
    if (rc)
        goto done;
    /* First-retirement ledger (all numbers provisional until source recount):
     * 10 LIFECYCLE slots cover BATCH/PROGRAM/PHYSICAL/CREDIT/REFERENCE/SOURCE/
     * DECIDE/CONTROL/FINISH/RESET, each including status/FIFO host suffix;
     * RESET also includes same-turn ACK. Each is paid before any child owner.
     * Owner supplements below pay that stage's actual nonempty child routing;
     * child prepaid retirement is counted once, never also spent on our host.
     * Partial prepay refusal owns only fixed context/control activation. Its
     * ten no-op/release turns + ACK are the explicitly unpaid bounded lifecycle
     * bootstrap exception, including first ENTRY refusal with consumed0.
     * No live child or indefinite retry is covered by that exception. */
    for (unsigned stage = 0; stage < 10; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_LIFECYCLE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 17, stage);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_INIT_WORK);
    if (rc)
        goto done;
    ctx->credit.governor = NULL; ctx->credit.bytes = 0;
    ctx->credit.replacement_bytes = 0;
    ctx->credit.transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_init(&ctx->credit.owner_bits, 0);
    atomic_init(&ctx->credit.state, WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
    ctx->credit.identity = &ctx->credit;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 2, 0);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 2; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_UNARY_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 18, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_UNARY_BEGIN_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 3, 0);
    if (rc)
        goto done;
    rc = wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
            control, attempt_id, owner);
    if (rc)
        goto done;
    ctx->unary_started = true;
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_INPUT_WORK);
    if (rc)
        goto done;
    const col_rel_t *input = NULL;
    col_rel_t **out_slot = NULL;
    rc = wl_columnar_eval_stack_unary_input_owned(frame, &input, owner);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_eval_stack_unary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 19, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_INIT_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_init_attempt_controlled(&ctx->source,
            &ctx->source_initialized, control, attempt_id);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_BEGIN_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->source,
        input, true);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    uint32_t width = 0, rows = 0;
    bool timestamped = false;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_SHAPE_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_shape_attempt_controlled(
        &ctx->source,
        &width, &rows, &timestamped);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    ctx->width = width; ctx->nrows = rows;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_REFERENCE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 20, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_GOVERNOR_PIN_WORK);
    if (rc)
        goto done;
    ctx->governor = sess->memory_governor
        ? sess->memory_governor : input->memory_governor;
    wl_columnar_memory_governor_ref_retain(ctx->governor);
    ctx->ref_held = ctx->governor != NULL;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 4, width);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 21, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_NEWLIKE_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled(
        &ctx->source, "$filter", ctx->governor, out_slot);
    /* Original frame slot ownership bookkeeping precedes stale-ID arbitration. */
    ctx->output = *out_slot;
    ctx->private_output = *out_slot;
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 2; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 22, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_SIZE_WORK);
    if (rc)
        goto done;
    uint64_t bytes = 0;
    bool sized = wl_columnar_memory_size_mul(width, sizeof(int64_t), &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 5, width);
    if (rc > 0 || sized) {
        if (rc)
            goto done;
    } else {
        rc = EOVERFLOW;
        goto done;
    }
    if (width > 16) {
        if (ctx->governor) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RESERVE_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 6, width);
            if (rc)
                goto done;
            wl_columnar_memory_admission_status_t status;
            rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
                wl_columnar_memory_governor_ref_get(ctx->governor), bytes,
                &ctx->credit, &status, control, attempt_id);
            if (rc)
                goto done;
            ctx->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
                || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
            int admission = ctx->admitted ? 0
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
            rc = wl_columnar_filter_attempt_return(control, attempt_id,
                    admission);
            if (rc)
                goto done;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_ALLOCATE_WORK);
        if (rc)
            goto done;
        ctx->values = wl_columnar_filter_attempt_allocate((size_t)bytes);
        ctx->heap = ctx->values != NULL;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 7, width);
        if (rc > 0 || ctx->heap) {
            if (rc)
                goto done;
        } else {
            rc = ENOMEM;
            goto done;
        }
        if (ctx->admitted) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_COMMIT_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 8, width);
            if (rc)
                goto done;
            rc = wl_columnar_memory_governor_commit_attempt_controlled(
                &ctx->credit, ctx->values, &ctx->credit_committed, control,
                attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc)
                goto done;
        }
    }
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_PROGRAM_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 23, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_PROGRAM_PREDICATE_WORK);
    if (rc)
        goto done;
    bool has_expression = op->filter_expr.data && op->filter_expr.size;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 9, 0);
    if (rc)
        goto done;
    if (has_expression) {
        bool supported = false;
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_PROGRAM_COMPILE_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_expr_compile_attempt_controlled(op->filter_expr.data,
                op->filter_expr.size, ctx->governor, &ctx->program, &supported,
                control, attempt_id);
        rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
        if (rc)
            goto done;
        if (!supported) {
            rc = ENOTSUP;
            goto done;
        }
    }
    /* Eighteen future steps get two128 halves each from36 payments. Each
     * payment reserves64 for current loop/caller scaffolding,128 for future
     * first cleanup; common BATCH is separate margin, never spent twice.
     * First refusal uses prior PROGRAM caller funding; later refusal uses
     * its latest unused128 instead of any nonexistent future batch step.
     * This provisional capacity requires actual per-step/source recount. */
    for (unsigned stage = 0; stage < 36; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_BATCH_RETIRE_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 16, stage);
        if (rc)
            goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_BATCH_INIT_WORK);
    if (rc)
        goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    ctx->batch_initialized = true;
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 10, 0);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_BATCH_BEGIN_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 11, 0);
    if (rc)
        goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            *out_slot, control);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    for (uint32_t row = 0; row < rows; ++row) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_ROW_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 12, row);
        if (rc)
            goto done;
        ctx->row = row;
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_GATHER_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
            &ctx->source, row, ctx->values, width);
        rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
        if (rc)
            goto done;
        bool selected = true;
        if (ctx->program) {
            int64_t value = 0;
            bool failed = false;
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_NUMERIC_EVAL_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_expr_eval_attempt_controlled(ctx->program,
                    ctx->values, width, &value, &failed, control, attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc)
                goto done;
            selected = !failed && value != 0;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SELECTION_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 13, row);
        if (rc)
            goto done;
        if (selected) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SELECTED_WORK);
            if (rc)
                goto done;
            rc =
                wl_columnar_relation_append_batch_selected_source_row_operator_controlled
                (
                &ctx->batch, &ctx->source, row, ctx->values, width);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc)
                goto done;
        }
    }

done:
    /* Every live owner is embedded; cleanup returns through real host turns. */
    ctx->execution_result = wl_columnar_filter_status(sess, ctx->output, rc);
    ctx->semantic_result = ctx->execution_result;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_BATCH;
    ctx->busy = false;
    wl_columnar_filter_attempt_status(ctx, status,
        WL_COLUMNAR_FILTER_PROGRESS_YIELD);
    return 0;
}

/* Scalar lexicographic comparison over the original pinned source. Fees
 * are provisional until the expanded-source gate, never telemetry-derived. */
static int
wl_columnar_filter_consolidate_compare(wl_columnar_filter_context_t *ctx,
    uint32_t left, uint32_t right, int *order,
    wl_evaluation_control_t *control, uint64_t id)
{
    int rc = wl_columnar_filter_attempt_charge(control, id, 128);
    if (rc) return rc;
    *order = 0;
    for (uint32_t column = 0; column < ctx->width; ++column) {
        rc = wl_columnar_filter_attempt_charge(control, id, 128);
        if (rc) return rc;
        int64_t a = ctx->source.columns[column][left];
        int64_t b = ctx->source.columns[column][right];
        if (ctx->source.types &&
            ctx->source.types[column] == WIRELOG_TYPE_FLOAT) {
            double fa, fb;
            memcpy(&fa, &a, sizeof(fa)); memcpy(&fb, &b, sizeof(fb));
            *order = (fa > fb) - (fa < fb);
        } else {
            *order = (a > b) - (a < b);
        }
        rc = wl_columnar_filter_attempt_tail(control, id, 200, column);
        if (rc) return rc;
        if (*order) break;
    }
    return wl_columnar_filter_attempt_return(control, id, 0);
}

int
wl_columnar_filter_consolidate_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_filter_consolidate_attempt_start_owned(op, frame, sess,
               control, attempt_id, status, NULL);
}

int
wl_columnar_filter_consolidate_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!status || !op) return EINVAL;
    if (owner && !control) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, sess, &stack,
            owner);
    if (rc) return rc;
    if (owner) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (record->control != control || record->attempt_id != attempt_id
            || wl_evaluation_control_active_attempt_id(control) != attempt_id)
            return EINVAL;
    }
    wl_columnar_filter_attempt_status_init(status);
    if (!control) {
        int result = col_op_consolidate(stack, sess);
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        return 0;
    }
    wl_columnar_filter_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    if (ctx->stage != WL_COLUMNAR_FILTER_STAGE_INERT) return EBUSY;
    /* The first refusal owns no context or child. Host action funding pays
     * this finite bootstrap; it does not grant successor attempt authority. */
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_ENTRY_WORK);
    if (rc) {
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = rc; status->semantic_result = rc;
        return 0;
    }
    ctx->program = NULL; ctx->output = NULL; ctx->private_output = NULL;
    ctx->governor = NULL;
    ctx->values = ctx->inline_values;
    ctx->source_initialized = false; ctx->batch_initialized = false;
    ctx->drain_initialized = false;
    ctx->heap = false; ctx->admitted = false; ctx->credit_committed = false;
    ctx->ref_held = false; ctx->unary_started = false;
    ctx->committed = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0; ctx->control = NULL; ctx->pin_owned = false;
    ctx->thread_valid = false; ctx->busy = false;
    ctx->frame = frame; ctx->session = sess; ctx->attempt_id = attempt_id;
    /* Existing control/reference interior has no finite CAS or time claim. */
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    ctx->control = control; ctx->pin_owned = true; ctx->identity = ctx;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->busy = true;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 1, 0);
    if (rc)
        goto done;
    bool controlled_batch_fallback =
        wl_columnar_eval_plan_numeric_batch_fallback_authorized(owner, frame,
            sess, control, attempt_id);
    if (op->op != WL_PLAN_OP_CONSOLIDATE || !stack->top
        || stack->items[stack->top - 1].is_delta
        || sess->current_iteration || sess->diff_operators_active
        || sess->retraction_seeded || sess->retraction_right_pass
        || sess->tdd_input_run || sess->tdd_subpass_active
        || sess->tdd_outbound_only_active
        || (sess->join_batch_bytes && !controlled_batch_fallback)
        || sess->join_output_shared_count || sess->coordinator
        || sess->num_workers > 1) {
        rc = ENOTSUP; goto done;
    }
    /* First-retirement ledger (all numbers provisional until source recount):
     * 10 LIFECYCLE slots cover BATCH/PROGRAM/PHYSICAL/CREDIT/REFERENCE/SOURCE/
     * DECIDE/CONTROL/FINISH/RESET, each including status/FIFO host suffix;
     * RESET also includes same-turn ACK. Each is paid before any child owner.
     * Owner supplements below pay that stage's actual nonempty child routing;
     * child prepaid retirement is counted once, never also spent on our host.
     * Partial prepay refusal owns only fixed context/control activation. Its
     * ten no-op/release turns + ACK are the explicitly unpaid bounded lifecycle
     * bootstrap exception, including first ENTRY refusal with consumed0.
     * No live child or indefinite retry is covered by that exception. */
    for (unsigned stage = 0; stage < 10; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_LIFECYCLE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 17, stage);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_INIT_WORK);
    if (rc)
        goto done;
    ctx->credit.governor = NULL; ctx->credit.bytes = 0;
    ctx->credit.replacement_bytes = 0;
    ctx->credit.transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_init(&ctx->credit.owner_bits, 0);
    atomic_init(&ctx->credit.state, WL_COLUMNAR_MEMORY_RESERVATION_EMPTY);
    ctx->credit.identity = &ctx->credit;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 2, 0);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 2; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_UNARY_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 18, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_UNARY_BEGIN_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 3, 0);
    if (rc)
        goto done;
    rc = wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
            control, attempt_id, owner);
    if (rc)
        goto done;
    ctx->unary_started = true;
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_INPUT_WORK);
    if (rc)
        goto done;
    const col_rel_t *input = NULL;
    col_rel_t **out_slot = NULL;
    rc = wl_columnar_eval_stack_unary_input_owned(frame, &input, owner);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_eval_stack_unary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 19, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_INIT_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_init_attempt_controlled(&ctx->source,
            &ctx->source_initialized, control, attempt_id);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_BEGIN_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->source,
        input, false);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    uint32_t width = 0, rows = 0;
    bool timestamped = false;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_SHAPE_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_shape_attempt_controlled(
        &ctx->source,
        &width, &rows, &timestamped);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    ctx->width = width; ctx->nrows = rows;
    if (input->timestamps) {
        rc = ENOTSUP; goto done;
    }
    /* Even trivial inputs must reject nonfinite FLOAT values before sorting.
     * Each row/column is separately paid, including non-FLOAT columns. */
    for (uint32_t row = 0; row < rows; ++row) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
        if (rc) goto done;
        for (uint32_t column = 0; column < width; ++column) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
            if (rc) goto done;
            if (ctx->source.types
                && ctx->source.types[column] == WIRELOG_TYPE_FLOAT) {
                double value;
                memcpy(&value, &ctx->source.columns[column][row],
                    sizeof(value));
                if (!isfinite(value)) {
                    rc = EINVAL; goto done;
                }
            }
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 201,
                    column);
            if (rc) goto done;
        }
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 202, row);
        if (rc) goto done;
    }

    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_REFERENCE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 20, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_GOVERNOR_PIN_WORK);
    if (rc)
        goto done;
    ctx->governor = sess->memory_governor
        ? sess->memory_governor : input->memory_governor;
    wl_columnar_memory_governor_ref_retain(ctx->governor);
    ctx->ref_held = ctx->governor != NULL;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 4, width);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 21, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_NEWLIKE_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled(
        &ctx->source, "$consol", ctx->governor, out_slot);
    /* Original frame slot ownership bookkeeping precedes stale-ID arbitration. */
    ctx->output = *out_slot;
    ctx->private_output = *out_slot;
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 2; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 22, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_SIZE_WORK);
    if (rc)
        goto done;
    /* SCRATCH_SIZE192 reserves64 exclusively for first BATCH prepay refusal;
     * current sizing and its forward skip scaffolding use at most128. */
    uint64_t bytes = 0;
    uint64_t cells = (uint64_t)(width ? width : 1) + rows;
    bool sized = wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 5, width);
    if (rc > 0 || sized) {
        if (rc)
            goto done;
    } else {
        rc = EOVERFLOW;
        goto done;
    }
    {
        if (ctx->governor) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RESERVE_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 6, width);
            if (rc)
                goto done;
            wl_columnar_memory_admission_status_t status;
            rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
                wl_columnar_memory_governor_ref_get(ctx->governor), bytes,
                &ctx->credit, &status, control, attempt_id);
            if (rc)
                goto done;
            ctx->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
                || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
            int admission = ctx->admitted ? 0
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
            rc = wl_columnar_filter_attempt_return(control, attempt_id,
                    admission);
            if (rc)
                goto done;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_ALLOCATE_WORK);
        if (rc)
            goto done;
        ctx->values = wl_columnar_filter_attempt_allocate((size_t)bytes);
        ctx->heap = ctx->values != NULL;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 7, width);
        if (rc > 0 || ctx->heap) {
            if (rc)
                goto done;
        } else {
            rc = ENOMEM;
            goto done;
        }
        if (ctx->admitted) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_COMMIT_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 8, width);
            if (rc)
                goto done;
            rc = wl_columnar_memory_governor_commit_attempt_controlled(
                &ctx->credit, ctx->values, &ctx->credit_committed, control,
                attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc)
                goto done;
        }
    }
    /* Eighteen future steps get two128 halves each from36 payments. Each
     * payment reserves64 for current loop/caller scaffolding,128 for future
     * first cleanup; common BATCH is separate margin, never spent twice.
     * First refusal uses reserved64 from SCRATCH_SIZE192; later refusal uses
     * its latest unused128 instead of any nonexistent future batch step.
     * This provisional capacity requires actual per-step/source recount. */
    for (unsigned stage = 0; stage < 36; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_BATCH_RETIRE_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 16, stage);
        if (rc)
            goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_BATCH_INIT_WORK);
    if (rc)
        goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    ctx->batch_initialized = true;
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 10, 0);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_BATCH_BEGIN_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 11, 0);
    if (rc)
        goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            *out_slot, control);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    uint32_t *indices = (uint32_t *)(ctx->values + (width ? width : 1));
    uint32_t *temporary = indices + rows;
    for (uint32_t row = 0; row < rows; ++row) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
        if (rc) goto done;
        indices[row] = row;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 203, row);
        if (rc) goto done;
    }
    /* Discover from leased values, not cache metadata: MAP may have changed
     * ordering. Nonfinite FLOATs were rejected above. Strict ordering makes
     * the prefix unique; stable suffix sort/merge preserves first-row ties.
     * These locals survive framed suspension with the original source lease. */
    uint32_t prefix = 0;
    if (sess->diff_enabled && rows) {
        prefix = 1;
        while (prefix < rows) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
            if (rc) goto done;
            int order = 0;
            rc = wl_columnar_filter_consolidate_compare(ctx, prefix - 1,
                    prefix, &order, control, attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) goto done;
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
            if (rc) goto done;
            bool increasing = order < 0;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 208,
                    prefix);
            if (rc) goto done;
            if (!increasing) break;
            ++prefix;
        }
    }
    for (uint64_t span = 1; span < rows - prefix; span *= 2) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
        if (rc) goto done;
        for (uint64_t base = prefix; base < rows; base += span * 2) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
            if (rc) goto done;
            uint64_t middle = base + span < rows ? base + span : rows;
            uint64_t end = base + 2 * span < rows ? base + 2 * span : rows;
            uint64_t left = base, right = middle;
            for (uint64_t out = base; out < end; ++out) {
                rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                        128);
                if (rc) goto done;
                int order = 0;
                if (left < middle && right < end) {
                    rc = wl_columnar_filter_consolidate_compare(ctx,
                            indices[left], indices[right], &order,
                            control, attempt_id);
                    rc = wl_columnar_filter_attempt_return(control, attempt_id,
                            rc);
                    if (rc) goto done;
                }
                rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                        128);
                if (rc) goto done;
                temporary[out] = left < middle && (right == end || order <= 0)
                    ? indices[left++] : indices[right++];
                rc = wl_columnar_filter_attempt_tail(control, attempt_id, 204,
                        out);
                if (rc) goto done;
            }
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 205,
                    base);
            if (rc) goto done;
        }
        uint32_t *swap = indices; indices = temporary; temporary = swap;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 206, span);
        if (rc) goto done;
    }
    if (prefix && prefix < rows) {
        /* Prefix indices are implicit: buffer swaps initialize only suffix
         * slots. Merge into the other array before the existing dedup pass. */
        uint32_t left = 0, right = prefix;
        for (uint32_t out = 0; out < rows; ++out) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
            if (rc) goto done;
            int order = 0;
            if (left < prefix && right < rows) {
                rc = wl_columnar_filter_consolidate_compare(ctx, left,
                        indices[right], &order, control, attempt_id);
                rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
                if (rc) goto done;
            }
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
            if (rc) goto done;
            temporary[out] = left < prefix && (right == rows || order <= 0)
                ? left++ : indices[right++];
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 209,
                    out);
            if (rc) goto done;
        }
        indices = temporary;
    }
    uint32_t previous = 0;
    for (uint32_t position = 0; position < rows; ++position) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
        if (rc) goto done;
        uint32_t row = indices[position];
        int order = 1;
        if (position) {
            rc = wl_columnar_filter_consolidate_compare(ctx, previous, row,
                    &order, control, attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) goto done;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
        if (rc) goto done;
        if (order) {
            ctx->row = row;
            rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
                &ctx->source, row, ctx->values, width);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) goto done;
            rc = wl_columnar_filter_attempt_charge(control, attempt_id, 192);
            if (rc) goto done;
            rc =
                wl_columnar_relation_append_batch_selected_source_row_operator_controlled
                (
                &ctx->batch, &ctx->source, row, ctx->values, width);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) goto done;
            previous = row;
        }
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 207,
                position);
        if (rc) goto done;
    }
    /* Complete sorting AND dedup precede the derived cache seal. No append
     * follows. Separate caller funding covers its continuation to real poll. */
    rc = wl_columnar_filter_attempt_charge(control, attempt_id, 128);
    if (rc) goto done;
    rc = wl_columnar_relation_append_batch_mark_sorted_operator_controlled(
        &ctx->batch, control, attempt_id);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);

done:
    /* Every live owner is embedded; cleanup returns through real host turns. */
    ctx->execution_result = wl_columnar_filter_status(sess, ctx->output, rc);
    ctx->semantic_result = ctx->execution_result;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_BATCH;
    ctx->busy = false;
    wl_columnar_filter_attempt_status(ctx, status,
        WL_COLUMNAR_FILTER_PROGRESS_YIELD);
    return 0;
}

enum {
    WL_COLUMNAR_FILTER_MAP_FORWARD_WORK = 192,
    WL_COLUMNAR_FILTER_MAP_TABLE_RETIRE_WORK = 256,
};

/* Preserve a discoverable initialized prefix on every partial exit. Fixed
* first-retirement grants precede all table resources; each slot has its own
* future grant. Forward work does not borrow another slot's retirement. */
static int
wl_columnar_filter_map_prepare_table(wl_columnar_filter_context_t *ctx,
    const wl_plan_op_t *op, uint32_t count, wl_evaluation_control_t *control,
    uint64_t attempt_id)
{
    int rc;
    for (unsigned stage = 0; stage < 6; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_TABLE_RETIRE_WORK);
        if (rc) return rc;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 100, stage);
        if (rc) return rc;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
    if (rc) return rc;
    wl_columnar_filter_program_table_t *table = &ctx->table;
    wl_columnar_memory_reservation_init(&table->credit);
    table->programs = NULL;
    table->initialized_slots = 0; table->retirement_cursor = 0;
    table->allocated = false; table->admitted = false;
    table->committed = false; table->ref_held = false;
    table->governor = NULL;
    table->initialized = true;
    uint64_t bytes = 0;
    bool sized = wl_columnar_memory_size_mul(count, sizeof(*table->programs),
            &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 101, count);
    if (rc > 0 || sized) {
        if (rc) return rc;
    } else return EOVERFLOW;
    if (count) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
        if (rc) return rc;
        table->governor = ctx->governor;
        wl_columnar_memory_governor_ref_retain(table->governor);
        table->ref_held = table->governor != NULL;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 102, count);
        if (rc) return rc;
        if (table->governor) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
            if (rc) return rc;
            wl_columnar_memory_admission_status_t status;
            rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
                wl_columnar_memory_governor_ref_get(table->governor), bytes,
                &table->credit, &status, control, attempt_id);
            if (rc) return rc;
            table->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
                || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
            int admission = table->admitted ? 0
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status ==
                WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
            rc = wl_columnar_filter_attempt_return(control, attempt_id,
                    admission);
            if (rc) return rc;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
        if (rc) return rc;
        table->programs = (wl_columnar_expr_attempt_numeric_t **)
            wl_columnar_filter_attempt_allocate((size_t)bytes);
        table->allocated = table->programs != NULL;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 103, count);
        if (rc > 0 || table->allocated) {
            if (rc) return rc;
        } else return ENOMEM;
        if (table->admitted) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
            if (rc) return rc;
            rc = wl_columnar_memory_governor_commit_attempt_controlled(
                &table->credit, (const void *)table->programs,
                &table->committed,
                control, attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) return rc;
        }
    }
    for (uint32_t column = 0; column < count; ++column) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_TABLE_RETIRE_WORK);
        if (rc) return rc;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 104, column);
        if (rc) return rc;
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
        if (rc) return rc;
        table->programs[column] = NULL;
        ++table->initialized_slots;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 105, column);
        if (rc) return rc;
        bool expression = op->map_exprs && column < op->map_expr_count
            && op->map_exprs[column].data && op->map_exprs[column].size;
        if (expression) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
            if (rc) return rc;
            bool supported = false;
            rc = wl_columnar_expr_compile_attempt_controlled(
                op->map_exprs[column].data, op->map_exprs[column].size,
                ctx->governor, &table->programs[column], &supported,
                control, attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) return rc;
            if (!supported) return ENOTSUP;
        }
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 106, column);
        if (rc) return rc;
    }
    return 0;
}

int
wl_columnar_filter_map_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_filter_map_attempt_start_owned(op, frame, sess, control,
               attempt_id, status,
               NULL);
}

int
wl_columnar_filter_map_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t attempt_id,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!status || !op) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, sess, &stack,
            owner);
    if (rc) return rc;
    wl_columnar_filter_attempt_status_init(status);
    if (!control) {
        int result = col_op_map(op, stack, sess);
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        return 0;
    }
    wl_columnar_filter_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    ctx->program = NULL; ctx->output = NULL; ctx->private_output = NULL;
    ctx->governor = NULL;
    ctx->values = ctx->inline_values;
    ctx->source_initialized = false; ctx->batch_initialized = false;
    ctx->drain_initialized = false;
    ctx->heap = false; ctx->admitted = false; ctx->credit_committed = false;
    ctx->ref_held = false; ctx->unary_started = false;
    ctx->committed = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0; ctx->control = NULL; ctx->pin_owned = false;
    ctx->thread_valid = false; ctx->busy = false;
    ctx->frame = frame; ctx->session = sess; ctx->attempt_id = attempt_id;
    /* Existing control/reference interior has no finite CAS or time claim. */
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    ctx->control = control; ctx->pin_owned = true; ctx->identity = ctx;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->busy = true;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_ENTRY_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 1, 0);
    if (rc)
        goto done;
    /* First-retirement ledger (all numbers provisional until source recount):
     * 10 LIFECYCLE slots cover BATCH/PROGRAM/PHYSICAL/CREDIT/REFERENCE/SOURCE/
     * DECIDE/CONTROL/FINISH/RESET, each including status/FIFO host suffix;
     * RESET also includes same-turn ACK. Each is paid before any child owner.
     * Owner supplements below pay that stage's actual nonempty child routing;
     * child prepaid retirement is counted once, never also spent on our host.
     * Partial prepay refusal owns only fixed context/control activation. Its
     * ten no-op/release turns + ACK are the explicitly unpaid bounded lifecycle
     * bootstrap exception, including first ENTRY refusal with consumed0.
     * No live child or indefinite retry is covered by that exception. */
    for (unsigned stage = 0; stage < 10; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_LIFECYCLE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 17, stage);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_INIT_WORK);
    if (rc)
        goto done;
    wl_columnar_memory_reservation_init(&ctx->credit);
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 2, 0);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 2; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_UNARY_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 18, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_UNARY_BEGIN_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 3, 0);
    if (rc)
        goto done;
    rc = wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
            control, attempt_id, owner);
    if (rc)
        goto done;
    ctx->unary_started = true;
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_INPUT_WORK);
    if (rc)
        goto done;
    const col_rel_t *input = NULL;
    col_rel_t **out_slot = NULL;
    rc = wl_columnar_eval_stack_unary_input_owned(frame, &input, owner);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_eval_stack_unary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 19, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_INIT_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_init_attempt_controlled(&ctx->source,
            &ctx->source_initialized, control, attempt_id);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_BEGIN_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->source,
        input, false);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    uint32_t width = 0, rows = 0;
    bool timestamped = false;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SOURCE_SHAPE_WORK);
    if (rc)
        goto done;
    rc =
        wl_columnar_relation_scalar_source_shape_attempt_controlled(
        &ctx->source,
        &width, &rows, &timestamped);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    ctx->width = width; ctx->nrows = rows;
    uint32_t output_width = op->project_count;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_REFERENCE_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 20, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_GOVERNOR_PIN_WORK);
    if (rc)
        goto done;
    ctx->governor = sess->memory_governor
        ? sess->memory_governor : input->memory_governor;
    wl_columnar_memory_governor_ref_retain(ctx->governor);
    ctx->ref_held = ctx->governor != NULL;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 4, width);
    if (rc)
        goto done;
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 2; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 22, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_SIZE_WORK);
    if (rc)
        goto done;
    uint64_t bytes = 0, value_count = 0;
    bool sized = wl_columnar_memory_size_add(width, output_width, &value_count)
        && wl_columnar_memory_size_mul(value_count, sizeof(int64_t), &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 5, width);
    if (rc > 0 || sized) {
        if (rc)
            goto done;
    } else {
        rc = EOVERFLOW;
        goto done;
    }
    if (value_count > 16) {
        if (ctx->governor) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_RESERVE_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 6, width);
            if (rc)
                goto done;
            wl_columnar_memory_admission_status_t status;
            rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
                wl_columnar_memory_governor_ref_get(ctx->governor), bytes,
                &ctx->credit, &status, control, attempt_id);
            if (rc)
                goto done;
            ctx->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
                || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
            int admission = ctx->admitted ? 0
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
            rc = wl_columnar_filter_attempt_return(control, attempt_id,
                    admission);
            if (rc)
                goto done;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_ALLOCATE_WORK);
        if (rc)
            goto done;
        ctx->values = wl_columnar_filter_attempt_allocate((size_t)bytes);
        ctx->heap = ctx->values != NULL;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 7, width);
        if (rc > 0 || ctx->heap) {
            if (rc)
                goto done;
        } else {
            rc = ENOMEM;
            goto done;
        }
        if (ctx->admitted) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_ATTEMPT_SCRATCH_COMMIT_WORK);
            if (rc)
                goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 8, width);
            if (rc)
                goto done;
            rc = wl_columnar_memory_governor_commit_attempt_controlled(
                &ctx->credit, ctx->values, &ctx->credit_committed, control,
                attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc)
                goto done;
        }
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
    if (rc) goto done;
    rc = wl_columnar_filter_map_prepare_table(ctx, op, output_width,
            control, attempt_id);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc) goto done;
    /* Eighteen future steps get two128 halves each from36 payments. Each
     * payment reserves64 for current loop/caller scaffolding,128 for future
     * first cleanup; common BATCH is separate margin, never spent twice.
     * First refusal uses prior PROGRAM caller funding; later refusal uses
     * its latest unused128 instead of any nonexistent future batch step.
     * This provisional capacity requires actual per-step/source recount. */
    for (unsigned stage = 0; stage < 36; ++stage) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_BATCH_RETIRE_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 16, stage);
        if (rc)
            goto done;
    }
    /* The output region of already-owned int64 scratch temporarily holds
     * type codes. The constructor consumes them synchronously before any row
     * overwrites this region. Source metadata stays under its original lease. */
    for (uint32_t column = 0; column < output_width; ++column) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 110, column);
        if (rc) goto done;
        wirelog_column_type_t type = WIRELOG_TYPE_INT64;
        wl_columnar_expr_attempt_numeric_t *program =
            ctx->table.programs[column];
        if (program) {
            bool supported = false;
            rc = wl_columnar_expr_result_type_attempt_controlled(program,
                    ctx->source.types, width, &type, &supported, control,
                    attempt_id);
            rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
            if (rc) goto done;
            if (!supported) {
                rc = ENOTSUP; goto done;
            }
        } else {
            uint32_t source_column = op->project_indices
                ? op->project_indices[column] : column;
            if (ctx->source.types && source_column < width)
                type = ctx->source.types[source_column];
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
        if (rc) goto done;
        ctx->values[(size_t)width + column] = type;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 111, column);
        if (rc) goto done;
    }
    /* Pre-admit this owner's first retirement before its live marker.
     * Skipped/failed owner acquisition leaves this named paid slack unused. */
    for (unsigned retire = 0; retire < 1; ++retire) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_OUTPUT_RETIRE_WORK);
        if (rc) goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 21, retire);
        if (rc) goto done;
    }
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_NEWLIKE_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
        "$map", output_width, ctx->governor ? rows : COL_REL_INIT_CAP,
        output_width ? ctx->values + width : NULL,
        ctx->governor, out_slot, control, attempt_id);
    /* Original frame slot ownership bookkeeping precedes stale-ID arbitration. */
    ctx->output = *out_slot;
    ctx->private_output = *out_slot;
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_BATCH_INIT_WORK);
    if (rc)
        goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    ctx->batch_initialized = true;
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 10, 0);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_charge(control, attempt_id,
            WL_COLUMNAR_FILTER_ATTEMPT_BATCH_BEGIN_WORK);
    if (rc)
        goto done;
    rc = wl_columnar_filter_attempt_tail(control, attempt_id, 11, 0);
    if (rc)
        goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            *out_slot, control);
    rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
    if (rc)
        goto done;
    for (uint32_t row = 0; row < rows; ++row) {
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_ROW_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_filter_attempt_tail(control, attempt_id, 12, row);
        if (rc)
            goto done;
        ctx->row = row;
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_GATHER_WORK);
        if (rc)
            goto done;
        rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
            &ctx->source, row, ctx->values, width);
        rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
        if (rc)
            goto done;
        for (uint32_t column = 0; column < output_width; ++column) {
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
            if (rc) goto done;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 108,
                    column);
            if (rc) goto done;
            int64_t value = 0;
            wl_columnar_expr_attempt_numeric_t *program =
                ctx->table.programs[column];
            if (program) {
                bool failed = false;
                rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                        WL_COLUMNAR_FILTER_ATTEMPT_NUMERIC_EVAL_WORK);
                if (rc) goto done;
                rc = wl_columnar_expr_eval_attempt_controlled(program,
                        ctx->values, width, &value, &failed, control,
                        attempt_id);
                rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
                if (rc) goto done;
                if (failed) {
                    rc = ERANGE; goto done;
                }
            } else {
                uint32_t source_column =
                    op->project_indices ? op->project_indices[column] : column;
                value = source_column < width ? ctx->values[source_column] : 0;
            }
            rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                    WL_COLUMNAR_FILTER_MAP_FORWARD_WORK);
            if (rc) goto done;
            ctx->values[(size_t)width + column] = value;
            rc = wl_columnar_filter_attempt_tail(control, attempt_id, 109,
                    column);
            if (rc) goto done;
        }
        rc = wl_columnar_filter_attempt_charge(control, attempt_id,
                WL_COLUMNAR_FILTER_ATTEMPT_SELECTED_WORK);
        if (rc) goto done;
        rc = wl_columnar_relation_append_batch_numeric_row_operator_controlled(
            &ctx->batch, ctx->values + width, control, attempt_id);
        rc = wl_columnar_filter_attempt_return(control, attempt_id, rc);
        if (rc) goto done;
    }

done:
    /* Every live owner is embedded; cleanup returns through real host turns. */
    ctx->execution_result = wl_columnar_filter_status(sess, ctx->output, rc);
    ctx->semantic_result = ctx->execution_result;
    ctx->stage = ctx->table.initialized
        ? WL_COLUMNAR_FILTER_STAGE_TABLE_PROGRAM :
        WL_COLUMNAR_FILTER_STAGE_BATCH;
    ctx->busy = false;
    wl_columnar_filter_attempt_status(ctx, status,
        WL_COLUMNAR_FILTER_PROGRESS_YIELD);
    return 0;
}

static int
wl_columnar_filter_attempt_context_valid(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_filter_context_t *ctx)
{
    if (ctx->identity != ctx || ctx->frame != frame || !ctx->session
        || ctx->session->cleanup_active != frame
        || ctx->stage <= WL_COLUMNAR_FILTER_STAGE_INERT
        || ctx->stage > WL_COLUMNAR_FILTER_STAGE_DONE
        || !wl_columnar_filter_attempt_thread(ctx))
        return EINVAL;
    return ctx->busy ? EBUSY : 0;
}

int
wl_columnar_filter_op_attempt_step(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_filter_op_attempt_step_owned(frame, status, NULL);
}

int
wl_columnar_filter_op_attempt_step_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!status) return EINVAL;
    wl_columnar_filter_context_t *ctx = NULL;
    int rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    rc = wl_columnar_filter_attempt_context_valid(frame, ctx);
    if (rc) {
        wl_columnar_filter_attempt_status_init(status);
        return rc;
    }
    if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_RUNNING) {
        wl_columnar_filter_attempt_status_init(status);
        return EBUSY;
    }
    if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_DONE) {
        wl_columnar_filter_attempt_status(ctx, status,
            WL_COLUMNAR_FILTER_PROGRESS_DONE);
        return 0;
    }
    ctx->busy = true;
    switch (ctx->stage) {
    case WL_COLUMNAR_FILTER_STAGE_TABLE_PROGRAM:
        if (ctx->table.retirement_cursor < ctx->table.initialized_slots) {
            uint32_t slot = ctx->table.retirement_cursor;
            wl_columnar_expr_attempt_numeric_t *program =
                ctx->table.programs[slot];
            ctx->table.programs[slot] = NULL;
            ++ctx->table.retirement_cursor;
            wl_columnar_expr_attempt_numeric_free(program);
            break;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_TABLE_PHYSICAL;
        break;
    case WL_COLUMNAR_FILTER_STAGE_TABLE_PHYSICAL:
        if (ctx->table.allocated) {
            void *physical = (void *)ctx->table.programs;
            ctx->table.programs = NULL;
            ctx->table.allocated = false;
            free(physical);
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_TABLE_CREDIT;
        break;
    case WL_COLUMNAR_FILTER_STAGE_TABLE_CREDIT:
        if (ctx->table.admitted) {
            bool released = ctx->table.committed
                ? wl_columnar_memory_release(&ctx->table.credit)
                : wl_columnar_memory_rollback(&ctx->table.credit);
            if (!released) {
                ctx->cleanup_result = EINVAL;
                goto retained;
            }
            ctx->table.admitted = false;
        }
        ctx->cleanup_result = 0;
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_TABLE_REFERENCE;
        break;
    case WL_COLUMNAR_FILTER_STAGE_TABLE_REFERENCE:
        if (ctx->table.ref_held) {
            wl_columnar_memory_governor_ref_t *ref = ctx->table.governor;
            ctx->table.governor = NULL;
            ctx->table.ref_held = false;
            wl_columnar_memory_governor_ref_release(ref);
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_TABLE_CLEAR;
        break;
    case WL_COLUMNAR_FILTER_STAGE_TABLE_CLEAR:
        ctx->table.initialized_slots = 0;
        ctx->table.retirement_cursor = 0;
        ctx->table.committed = false;
        ctx->table.initialized = false;
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_BATCH;
        break;
    case WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH:
        if (ctx->batch_initialized) {
            if (!ctx->drain_initialized
                || ctx->batch_drain.identity != &ctx->batch_drain) {
                rc = EINVAL;
                goto protocol_error;
            }
            bool done;
            rc = wl_columnar_relation_private_batch_drain_step(
                &ctx->batch_drain, &ctx->batch, ctx->execution_result, &done);
            if (rc) {
                if (ctx->batch_drain.release_error != rc)
                    goto protocol_error;
                ctx->cleanup_result = rc;
                goto retained;
            }
            ctx->cleanup_result = 0;
            if (!done) break;
            ctx->batch_initialized = false;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE;
        break;
    case WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE:
        if (ctx->join_right_initialized) {
            (void)wl_columnar_relation_scalar_source_end(&ctx->join_right,
                ctx->execution_result);
            if (ctx->join_right.phase !=
                WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED)
                abort();
            ctx->join_right_initialized = false;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_PHYSICAL;
        break;
    case WL_COLUMNAR_FILTER_STAGE_JOIN_PHYSICAL: {
        if (!ctx->join_initialized
            || ctx->join_retirement_cursor >= WL_COLUMNAR_JOIN_SCRATCH_COUNT) {
            rc = EINVAL; goto protocol_error;
        }
        wl_columnar_join_scratch_t *scratch =
            &ctx->join_scratch[ctx->join_retirement_cursor];
        if (scratch->initialized && scratch->allocated) {
            void *physical = scratch->physical;
            scratch->physical = NULL; scratch->allocated = false;
            free(physical);
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT;
        break;
    }
    case WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT: {
        if (!ctx->join_initialized
            || ctx->join_retirement_cursor >= WL_COLUMNAR_JOIN_SCRATCH_COUNT) {
            rc = EINVAL; goto protocol_error;
        }
        wl_columnar_join_scratch_t *scratch =
            &ctx->join_scratch[ctx->join_retirement_cursor];
        if (scratch->initialized && scratch->admitted) {
            bool released = scratch->committed
                ? wl_columnar_memory_release(&scratch->credit)
                : wl_columnar_memory_rollback(&scratch->credit);
            if (!released) {
                ctx->cleanup_result = EINVAL; goto retained;
            }
            scratch->admitted = false;
        }
        ctx->cleanup_result = 0;
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_REFERENCE;
        break;
    }
    case WL_COLUMNAR_FILTER_STAGE_JOIN_REFERENCE: {
        if (!ctx->join_initialized
            || ctx->join_retirement_cursor >= WL_COLUMNAR_JOIN_SCRATCH_COUNT) {
            rc = EINVAL; goto protocol_error;
        }
        wl_columnar_join_scratch_t *scratch =
            &ctx->join_scratch[ctx->join_retirement_cursor];
        if (scratch->initialized && scratch->ref_held) {
            wl_columnar_memory_governor_ref_t *ref = scratch->governor;
            scratch->governor = NULL; scratch->ref_held = false;
            wl_columnar_memory_governor_ref_release(ref);
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_CLEAR;
        break;
    }
    case WL_COLUMNAR_FILTER_STAGE_JOIN_CLEAR: {
        if (!ctx->join_initialized
            || ctx->join_retirement_cursor >= WL_COLUMNAR_JOIN_SCRATCH_COUNT) {
            rc = EINVAL; goto protocol_error;
        }
        wl_columnar_join_scratch_t *scratch =
            &ctx->join_scratch[ctx->join_retirement_cursor];
        scratch->initialized = false; scratch->committed = false;
        ++ctx->join_retirement_cursor;
        if (ctx->join_retirement_cursor == WL_COLUMNAR_JOIN_SCRATCH_COUNT) {
            ctx->join_retirement_cursor = 0;
            ctx->join_initialized = false;
            ctx->stage = WL_COLUMNAR_FILTER_STAGE_PROGRAM;
        } else ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_PHYSICAL;
        break;
    }
    case WL_COLUMNAR_FILTER_STAGE_BATCH:
        if (ctx->batch_initialized) {
            if (!ctx->drain_initialized
                || ctx->batch_drain.identity != &ctx->batch_drain) {
                rc = EINVAL;
                goto protocol_error;
            }
            bool done;
            rc = wl_columnar_relation_private_batch_drain_step(
                &ctx->batch_drain, &ctx->batch, ctx->execution_result, &done);
            if (rc) {
                if (ctx->batch_drain.release_error != rc)
                    goto protocol_error;
                ctx->cleanup_result = rc;
                goto retained;
            }
            ctx->cleanup_result = 0;
            if (!done) break;
            ctx->batch_initialized = false;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_PROGRAM;
        break;
    case WL_COLUMNAR_FILTER_STAGE_PROGRAM: {
        wl_columnar_expr_attempt_numeric_t *program = ctx->program;
        ctx->program = NULL;
        wl_columnar_expr_attempt_numeric_free(program);
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_PHYSICAL;
        break;
    }
    case WL_COLUMNAR_FILTER_STAGE_PHYSICAL:
        if (ctx->heap) {
            int64_t *physical = ctx->values;
            ctx->values = ctx->inline_values;
            ctx->heap = false;
            free(physical);
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_CREDIT;
        break;
    case WL_COLUMNAR_FILTER_STAGE_CREDIT:
        if (ctx->admitted) {
            bool released = ctx->credit_committed
                ? wl_columnar_memory_release(&ctx->credit)
                : wl_columnar_memory_rollback(&ctx->credit);
            if (!released) {
                ctx->cleanup_result = EINVAL;
                goto retained;
            }
            ctx->admitted = false;
        }
        ctx->cleanup_result = 0;
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_REFERENCE;
        break;
    case WL_COLUMNAR_FILTER_STAGE_REFERENCE:
        if (ctx->ref_held) {
            wl_columnar_memory_governor_ref_t *ref = ctx->governor;
            ctx->governor = NULL; ctx->ref_held = false;
            wl_columnar_memory_governor_ref_release(ref);
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_SOURCE;
        break;
    case WL_COLUMNAR_FILTER_STAGE_SOURCE:
        if (ctx->source_initialized) {
            (void)wl_columnar_relation_scalar_source_end(&ctx->source,
                ctx->execution_result);
            if (ctx->source.phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED)
                abort();
            ctx->source_initialized = false;
        }
        ctx->stage = ctx->join_decision_pending
            ? WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE :
            WL_COLUMNAR_FILTER_STAGE_DECIDE;
        break;
    case WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE:
        if (!ctx->pin_owned || !ctx->join_decision_pending) {
            rc = EINVAL; goto protocol_error;
        }
        if (!ctx->execution_result && ctx->unary_started) {
            rc = wl_columnar_filter_attempt_charge(ctx->control,
                    ctx->attempt_id, 0);
            if (!rc)
                rc =
                    wl_columnar_eval_stack_unary_commit_join_attempt_context_owned
                    (
                    frame, ctx, &ctx->committed, owner);
            ctx->execution_result = rc;
            ctx->semantic_result = rc;
        }
        ctx->join_result_is_delta = false;
        ctx->join_decision_pending = false;
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_CONTROL;
        break;
    case WL_COLUMNAR_FILTER_STAGE_DECIDE:
        if (!ctx->pin_owned) {
            rc = EINVAL;
            goto protocol_error;
        }
        if (!ctx->execution_result && ctx->unary_started) {
            rc = wl_columnar_filter_attempt_charge(ctx->control,
                    ctx->attempt_id, 0);
            if (!rc)
                rc = wl_columnar_eval_stack_unary_commit_attempt_owned(
                    frame, &ctx->committed, owner);
            ctx->execution_result = rc;
            ctx->semantic_result = rc;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_CONTROL;
        break;
    case WL_COLUMNAR_FILTER_STAGE_CONTROL: {
        if (!ctx->pin_owned) {
            rc = EINVAL;
            goto protocol_error;
        }
        /* After this stage no helper may evaluate dormant old control values. */
        wl_evaluation_control_t *control = ctx->control;
        ctx->control = NULL; ctx->pin_owned = false;
        ctx->output = NULL;
        wl_evaluation_control_release(control);
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_FINISH;
        break;
    }
    case WL_COLUMNAR_FILTER_STAGE_FINISH:
        if (ctx->unary_started) {
            bool finish_retained = false;
            rc = wl_columnar_eval_stack_filter_finish_owned(frame,
                    ctx->execution_result, &ctx->semantic_result,
                    &finish_retained, owner);
            if (rc) goto protocol_error;
            if (finish_retained) goto retained;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_RESET;
        break;
    case WL_COLUMNAR_FILTER_STAGE_RESET:
        if (ctx->unary_started) {
            rc = wl_columnar_eval_stack_unary_reset_owned(frame, owner);
            if (rc) {
                if (rc != EBUSY) goto protocol_error;
                goto retained;
            }
            ctx->unary_started = false;
        }
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_DONE;
        break;
    default:
        rc = EINVAL;
        goto protocol_error;
    }
    ctx->busy = false;
    wl_columnar_filter_attempt_status(ctx, status,
        ctx->stage == WL_COLUMNAR_FILTER_STAGE_DONE
        ? WL_COLUMNAR_FILTER_PROGRESS_DONE : WL_COLUMNAR_FILTER_PROGRESS_YIELD);
    return 0;
retained:
    ctx->busy = false;
    wl_columnar_filter_attempt_status(ctx, status,
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    return 0;
protocol_error:
    ctx->busy = false;
    wl_columnar_filter_attempt_status_init(status);
    return rc;
}

int
wl_columnar_filter_op_attempt_ack(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return wl_columnar_filter_op_attempt_ack_owned(frame, NULL);
}

int
wl_columnar_filter_op_attempt_ack_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    wl_columnar_filter_context_t *ctx = NULL;
    int rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_INERT)
        return 0;
    rc = wl_columnar_filter_attempt_context_valid(frame, ctx);
    if (rc) return rc;
    if (ctx->stage != WL_COLUMNAR_FILTER_STAGE_DONE || ctx->pin_owned
        || ctx->unary_started || ctx->source_initialized ||
        ctx->batch_initialized || ctx->join_initialized
        || ctx->join_decision_pending || ctx->join_result_is_delta
        || ctx->heap || ctx->admitted || ctx->ref_held || ctx->program ||
        ctx->output || ctx->private_output || ctx->table.programs
        || ctx->table.initialized_slots || ctx->table.retirement_cursor
        || ctx->table.allocated || ctx->table.admitted || ctx->table.ref_held
        || ctx->table.governor || ctx->table.committed ||
        ctx->table.initialized)
        return EBUSY;
    ctx->thread_valid = false; ctx->frame = NULL; ctx->session = NULL;
    ctx->values = ctx->inline_values; ctx->governor = NULL;
    ctx->attempt_id = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0; ctx->committed = false;
    ctx->credit_committed = false; ctx->drain_initialized = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_INERT;
    return 0;
}
