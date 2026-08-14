/*
 * columnar/arithmetic.c - Checked integer arithmetic for columnar expressions
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>

static inline uint64_t
wl_columnar_arithmetic_abs_u64(int64_t v)
{
    return v < 0 ? (uint64_t)0 - (uint64_t)v : (uint64_t)v;
}

/* Use compiler builtins where available; otherwise use explicit range checks. */
#if defined(__GNUC__) || defined(__clang__)
#define WL_COLUMNAR_ARITHMETIC_HAVE_INT64_OVERFLOW_BUILTIN 1
#elif defined(__has_builtin)
#if __has_builtin(__builtin_add_overflow) \
    && __has_builtin(__builtin_sub_overflow) \
    && __has_builtin(__builtin_mul_overflow)
#define WL_COLUMNAR_ARITHMETIC_HAVE_INT64_OVERFLOW_BUILTIN 1
#endif
#endif

int
wl_columnar_arithmetic_checked_add_int64(int64_t a, int64_t b, int64_t *out)
{
#if defined(WL_COLUMNAR_ARITHMETIC_HAVE_INT64_OVERFLOW_BUILTIN)
    return __builtin_add_overflow(a, b, out) ? ERANGE : 0;
#else
    if (b > 0) {
        if (a > INT64_MAX - b)
            return ERANGE;
    } else {
        if (a < INT64_MIN - b)
            return ERANGE;
    }
    *out = a + b;
    return 0;
#endif
}

int
wl_columnar_arithmetic_checked_sub_int64(int64_t a, int64_t b, int64_t *out)
{
#if defined(WL_COLUMNAR_ARITHMETIC_HAVE_INT64_OVERFLOW_BUILTIN)
    return __builtin_sub_overflow(a, b, out) ? ERANGE : 0;
#else
    if (b > 0) {
        if (a < INT64_MIN + b)
            return ERANGE;
    } else {
        if (a > INT64_MAX + b)
            return ERANGE;
    }
    *out = a - b;
    return 0;
#endif
}

int
wl_columnar_arithmetic_checked_mul_int64(int64_t a, int64_t b, int64_t *out)
{
#if defined(WL_COLUMNAR_ARITHMETIC_HAVE_INT64_OVERFLOW_BUILTIN)
    return __builtin_mul_overflow(a, b, out) ? ERANGE : 0;
#else
    if (a == 0 || b == 0) {
        *out = 0;
        return 0;
    }
    if ((a == -1 && b == INT64_MIN) || (b == -1 && a == INT64_MIN))
        return ERANGE;

    uint64_t ua = wl_columnar_arithmetic_abs_u64(a);
    uint64_t ub = wl_columnar_arithmetic_abs_u64(b);
    if ((a < 0) == (b < 0)) {
        if (ua > (uint64_t)INT64_MAX / ub)
            return ERANGE;
    } else {
        if (ua > ((uint64_t)INT64_MAX + 1ULL) / ub)
            return ERANGE;
    }
    *out = a * b;
    return 0;
#endif
}

int
wl_columnar_arithmetic_checked_div_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b == 0 || (a == INT64_MIN && b == -1))
        return ERANGE;
    *out = a / b;
    return 0;
}

int
wl_columnar_arithmetic_checked_mod_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b == 0 || (a == INT64_MIN && b == -1))
        return ERANGE;
    *out = a % b;
    return 0;
}

int
wl_columnar_arithmetic_checked_shl_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b < 0 || b >= 64)
        return ERANGE;
    *out = (int64_t)((uint64_t)a << (uint32_t)b);
    return 0;
}

int
wl_columnar_arithmetic_checked_shr_int64(int64_t a, int64_t b, int64_t *out)
{
    if (b < 0 || b >= 64)
        return ERANGE;
    *out = a >> (uint32_t)b;
    return 0;
}
