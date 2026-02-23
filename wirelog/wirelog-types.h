/*
 * wirelog-types.h - wirelog Type Definitions
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#ifndef WIRELOG_TYPES_H
#define WIRELOG_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ======================================================================== */
/* Comparison Operators                                                     */
/* ======================================================================== */

typedef enum {
    WL_CMP_EQ,  /* = */
    WL_CMP_NEQ, /* != */
    WL_CMP_LT,  /* < */
    WL_CMP_GT,  /* > */
    WL_CMP_LTE, /* <= */
    WL_CMP_GTE, /* >= */
} wl_cmp_op_t;

/* ======================================================================== */
/* Arithmetic Operators                                                     */
/* ======================================================================== */

typedef enum {
    WL_ARITH_ADD, /* + */
    WL_ARITH_SUB, /* - */
    WL_ARITH_MUL, /* * */
    WL_ARITH_DIV, /* / */
    WL_ARITH_MOD, /* % */
} wl_arith_op_t;

/* ======================================================================== */
/* Aggregate Functions                                                      */
/* ======================================================================== */

typedef enum {
    WL_AGG_COUNT, /* count / COUNT */
    WL_AGG_SUM,   /* sum / SUM */
    WL_AGG_MIN,   /* min / MIN */
    WL_AGG_MAX,   /* max / MAX */
    WL_AGG_AVG,   /* average / AVG */
} wl_agg_fn_t;

/* ======================================================================== */
/* Operator String Conversion                                               */
/* ======================================================================== */

const char *
wl_cmp_op_str(wl_cmp_op_t op);
const char *
wl_arith_op_str(wl_arith_op_t op);
const char *
wl_agg_fn_str(wl_agg_fn_t fn);

/* ======================================================================== */
/* Basic Types                                                              */
/* ======================================================================== */

typedef int32_t wirelog_int_t;
typedef uint64_t wirelog_uint_t;
typedef double wirelog_float_t;
typedef bool wirelog_bool_t;

/* ======================================================================== */
/* Relation Types                                                           */
/* ======================================================================== */

/**
 * wirelog_column_type_t:
 *
 * Column data types supported by wirelog
 */
typedef enum {
    WIRELOG_TYPE_INT32 = 0,  /* 32-bit signed integer */
    WIRELOG_TYPE_INT64 = 1,  /* 64-bit signed integer */
    WIRELOG_TYPE_UINT32 = 2, /* 32-bit unsigned integer */
    WIRELOG_TYPE_UINT64 = 3, /* 64-bit unsigned integer */
    WIRELOG_TYPE_FLOAT = 4,  /* 64-bit floating point */
    WIRELOG_TYPE_STRING = 5, /* UTF-8 string */
    WIRELOG_TYPE_BOOL = 6,   /* Boolean */
} wirelog_column_type_t;

/**
 * wirelog_column_t:
 *
 * Column metadata
 */
typedef struct {
    const char *name;
    wirelog_column_type_t type;
} wirelog_column_t;

/**
 * wirelog_schema_t:
 *
 * Relation schema (column definitions)
 */
typedef struct {
    const char *relation_name;
    wirelog_column_t *columns;
    uint32_t column_count;
} wirelog_schema_t;

/* ======================================================================== */
/* Stratification                                                           */
/* ======================================================================== */

/**
 * wirelog_stratum_t:
 *
 * A stratum in the stratified program.
 *
 * @stratum_id:    Stratum index (0 = computed first).
 * @rule_names:    Head relation names of rules in this stratum.
 * @rule_count:    Number of rules.
 * @is_recursive:  True if stratum contains a recursive SCC
 *                 (self-loop or mutual recursion cycle).
 */
typedef struct {
    uint32_t stratum_id;
    const char **rule_names;
    uint32_t rule_count;
    bool is_recursive;
} wirelog_stratum_t;

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_TYPES_H */
