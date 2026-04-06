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
    WIRELOG_CMP_EQ,  /* = */
    WIRELOG_CMP_NEQ, /* != */
    WIRELOG_CMP_LT,  /* < */
    WIRELOG_CMP_GT,  /* > */
    WIRELOG_CMP_LTE, /* <= */
    WIRELOG_CMP_GTE, /* >= */
} wirelog_cmp_op_t;

/* ======================================================================== */
/* Arithmetic Operators                                                     */
/* ======================================================================== */

typedef enum {
    WIRELOG_ARITH_ADD,       /* + */
    WIRELOG_ARITH_SUB,       /* - */
    WIRELOG_ARITH_MUL,       /* * */
    WIRELOG_ARITH_DIV,       /* / */
    WIRELOG_ARITH_MOD,       /* % */
    WIRELOG_ARITH_BAND,      /* band - bitwise AND */
    WIRELOG_ARITH_BOR,       /* bor - bitwise OR */
    WIRELOG_ARITH_BXOR,      /* bxor - bitwise XOR */
    WIRELOG_ARITH_BNOT,      /* bnot - bitwise NOT (unary) */
    WIRELOG_ARITH_SHL,       /* bshl - left shift */
    WIRELOG_ARITH_SHR,       /* bshr - right shift */
    WIRELOG_ARITH_HASH,      /* hash - xxHash3 64-bit (unary) */
    WIRELOG_ARITH_CRC32_ETH, /* crc32_ethernet() - CRC-32 Ethernet/ISO (unary) */
    WIRELOG_ARITH_CRC32_CAST, /* crc32_castagnoli() - CRC-32C iSCSI (unary) */
    WIRELOG_ARITH_MD5,        /* md5() - MD5 hex digest (unary, mbedTLS) */
    WIRELOG_ARITH_SHA1,       /* sha1() - SHA-1 hex digest (unary, mbedTLS) */
    WIRELOG_ARITH_SHA256, /* sha256() - SHA-256 hex digest (unary, mbedTLS) */
    WIRELOG_ARITH_SHA512, /* sha512() - SHA-512 hex digest (unary, mbedTLS) */
    WIRELOG_ARITH_HMAC_SHA256, /* hmac_sha256(msg, key) - HMAC-SHA-256 hex digest (binary, mbedTLS) */
    WIRELOG_ARITH_UUID4, /* uuid4() - random UUID v4 (mbedTLS CTR-DRBG) */
    WIRELOG_ARITH_UUID5, /* uuid5(ns, name) - SHA-1 name-based UUID v5 (mbedTLS) */
} wirelog_arith_op_t;

/* ======================================================================== */
/* Aggregate Functions                                                      */
/* ======================================================================== */

typedef enum {
    WIRELOG_AGG_COUNT, /* count / COUNT */
    WIRELOG_AGG_SUM,   /* sum / SUM */
    WIRELOG_AGG_MIN,   /* min / MIN */
    WIRELOG_AGG_MAX,   /* max / MAX */
    WIRELOG_AGG_AVG,   /* average / AVG */
} wirelog_agg_fn_t;

/* ======================================================================== */
/* String Functions                                                         */
/* ======================================================================== */

typedef enum {
    WL_STR_FN_STRLEN,      /* strlen(s) - byte length */
    WL_STR_FN_CAT,         /* cat(s1, s2) - concatenation */
    WL_STR_FN_SUBSTR,      /* substr(s, start, len) - substring */
    WL_STR_FN_CONTAINS,    /* contains(s, needle) - substring test */
    WL_STR_FN_STR_PREFIX,  /* str_prefix(s, prefix) - prefix test */
    WL_STR_FN_STR_SUFFIX,  /* str_suffix(s, suffix) - suffix test */
    WL_STR_FN_STR_ORD,     /* str_ord(s) - first code point ordinal */
    WL_STR_FN_TO_UPPER,    /* to_upper(s) - ASCII upper-case */
    WL_STR_FN_TO_LOWER,    /* to_lower(s) - ASCII lower-case */
    WL_STR_FN_STR_REPLACE, /* str_replace(s, from, to) - replace all */
    WL_STR_FN_TRIM,        /* trim(s) - strip leading/trailing whitespace */
    WL_STR_FN_TO_STRING,   /* to_string(x) - numeric to string */
    WL_STR_FN_TO_NUMBER,   /* to_number(s) - string to numeric */
} wirelog_str_fn_t;

/* ======================================================================== */
/* Operator String Conversion                                               */
/* ======================================================================== */

const char *
wirelog_cmp_op_str(wirelog_cmp_op_t op);
const char *
wirelog_arith_op_str(wirelog_arith_op_t op);
const char *
wirelog_agg_fn_str(wirelog_agg_fn_t fn);

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
