/*
 * wirelog-types.h - wirelog Type Definitions
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Licensed under LGPL-3.0-or-later
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

/**
 * @file wirelog-types.h
 * @brief Common type definitions and enums for the wirelog public API.
 */

#ifndef WIRELOG_TYPES_H
#define WIRELOG_TYPES_H

#include "wirelog/wirelog-export.h"

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
    WIRELOG_ARITH_MD5,        /* md5() - MD5 digest folded to int64 (unary, mbedTLS) */
    WIRELOG_ARITH_SHA1,       /* sha1() - SHA-1 digest folded to int64 (unary, mbedTLS) */
    WIRELOG_ARITH_SHA256, /* sha256() - SHA-256 digest folded to int64 (unary, mbedTLS) */
    WIRELOG_ARITH_SHA512, /* sha512() - SHA-512 digest folded to int64 (unary, mbedTLS) */
    WIRELOG_ARITH_HMAC_SHA256, /* hmac_sha256(msg, key) - HMAC-SHA-256 folded to int64 (binary, mbedTLS) */
    WIRELOG_ARITH_UUID4, /* uuid4() - random UUID v4 prefix as int64 (mbedTLS) */
    WIRELOG_ARITH_UUID5, /* uuid5(ns, name) - name-based UUID v5 prefix as int64 (mbedTLS) */
} wirelog_arith_op_t;

/* ======================================================================== */
/* Aggregate Functions                                                      */
/* ======================================================================== */

typedef enum {
    WIRELOG_AGG_COUNT, /* count / COUNT */
    WIRELOG_AGG_SUM,   /* sum / SUM */
    WIRELOG_AGG_MIN,   /* min / MIN */
    WIRELOG_AGG_MAX,   /* max / MAX */
    /*
     * average / AVG.  Requires a float operand and returns a binary64 mean.
     */
    WIRELOG_AGG_AVG,
} wirelog_agg_fn_t;

/* ======================================================================== */
/* String Functions                                                         */
/* ======================================================================== */

typedef enum {
    WIRELOG_STR_FN_STRLEN,      /* strlen(s) - byte length */
    WIRELOG_STR_FN_CAT,         /* cat(s1, s2) - concatenation */
    WIRELOG_STR_FN_SUBSTR,      /* substr(s, start, len) - substring */
    WIRELOG_STR_FN_CONTAINS,    /* contains(s, needle) - substring test */
    WIRELOG_STR_FN_STR_PREFIX,  /* str_prefix(s, prefix) - prefix test */
    WIRELOG_STR_FN_STR_SUFFIX,  /* str_suffix(s, suffix) - suffix test */
    WIRELOG_STR_FN_STR_ORD,     /* str_ord(s) - first code point ordinal */
    WIRELOG_STR_FN_TO_UPPER,    /* to_upper(s) - ASCII upper-case */
    WIRELOG_STR_FN_TO_LOWER,    /* to_lower(s) - ASCII lower-case */
    WIRELOG_STR_FN_STR_REPLACE, /* str_replace(s, from, to) - replace all */
    WIRELOG_STR_FN_TRIM,        /* trim(s) - strip leading/trailing whitespace */
    WIRELOG_STR_FN_TO_STRING,   /* to_string(x) - numeric to string */
    WIRELOG_STR_FN_TO_NUMBER,   /* to_number(s) - string to numeric */
    WIRELOG_STR_FN_UUID5_RFC,   /* uuid5_rfc(namespace_uuid, name) */
} wirelog_str_fn_t;

/* ======================================================================== */
/* Operator String Conversion                                               */
/* ======================================================================== */

WIRELOG_API const char *
wirelog_cmp_op_str(wirelog_cmp_op_t op);
WIRELOG_API const char *
wirelog_arith_op_str(wirelog_arith_op_t op);
/*
 * Returns the aggregate's source spelling, i.e. a name the lexer accepts.
 * WIRELOG_AGG_AVG returns "average", not "avg" -- `avg` is not a keyword
 * (see docs/SYNTAX.md), so the old spelling did not parse back.  Intended
 * for diagnostics and program dumps; the returned string is static.
 */
WIRELOG_API const char *
wirelog_agg_fn_str(wirelog_agg_fn_t fn);

/* ======================================================================== */
/* Basic Types                                                              */
/* ======================================================================== */

typedef int32_t wirelog_int_t;
typedef uint64_t wirelog_uint_t;
typedef double wirelog_float_t;
typedef bool wirelog_bool_t;

/* ======================================================================== */
/* Callbacks                                                                */
/* ======================================================================== */

/**
 * wirelog_on_tuple_fn:
 *
 * Callback invoked once per computed output tuple.
 *
 * @relation:  Null-terminated output relation name.
 * @row:       Array of int64_t values (length = @ncols).
 * @ncols:     Number of columns in the row.
 * @user_data: Opaque pointer passed through from the caller.
 */
typedef void (*wirelog_on_tuple_fn)(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data);

/**
 * wirelog_on_delta_fn:
 *
 * Callback invoked when a tuple is inserted/removed in an incremental session.
 *
 * @relation:  Null-terminated output relation name.
 * @row:       Array of int64_t values (length = @ncols).
 * @ncols:     Number of columns in the row.
 * @diff:      +1 for insertion, -1 for removal.
 * @user_data: Opaque pointer passed through from the caller.
 */
typedef void (*wirelog_on_delta_fn)(const char *relation, const int64_t *row,
    uint32_t ncols, int32_t diff, void *user_data);

/* Versioned typed row descriptors.  Float lanes contain host-order
 * IEEE-754 binary64 bits and are copied with memcpy at the API boundary. */
typedef struct wirelog_typed_row_v1 {
    uint32_t struct_size;
    uint16_t abi_version;
    uint16_t reserved;
    uint32_t logical_ncols;
    uint32_t physical_nlanes;
    uint32_t physical_stride;
    const uint32_t *types;
    const uint32_t *lane_offsets;
    const uint32_t *physical_types;
    const uint64_t *lanes;
} wirelog_typed_row_v1_t;

typedef enum {
    WIRELOG_TYPED_ERROR_NONE = 0,
    WIRELOG_TYPED_ERROR_DESCRIPTOR = 1,
    WIRELOG_TYPED_ERROR_SCHEMA = 2,
    WIRELOG_TYPED_ERROR_VALUE = 3,
} wirelog_typed_error_code_t;

typedef struct wirelog_typed_error_v1 {
    uint32_t struct_size;
    uint32_t code;
    uint32_t row_index;
    uint32_t logical_col;
    char *message;
    uint32_t message_capacity;
} wirelog_typed_error_v1_t;

typedef struct wirelog_typed_compound_arg_v1 {
    uint32_t type;
    uint64_t bits;
} wirelog_typed_compound_arg_v1_t;

typedef void (*wirelog_on_typed_tuple_fn)(const char *relation,
    const wirelog_typed_row_v1_t *row, int32_t diff, void *user_data);

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
 * wirelog_compound_kind_t:
 *
 * Compound column kind (Issue #531: IR Lowering)
 */
typedef enum {
    WIRELOG_COMPOUND_KIND_NONE = 0,   /* Regular column, not compound */
    WIRELOG_COMPOUND_KIND_INLINE = 1, /* f/N inline compound */
    WIRELOG_COMPOUND_KIND_SIDE = 2,   /* f/N side-relation compound (default) */
} wirelog_compound_kind_t;

#define WIRELOG_COMPOUND_HANDLE_NULL ((uint64_t)0)

typedef struct {
    wirelog_column_type_t type;
    int64_t value;
} wirelog_compound_arg_t;

/**
 * wirelog_column_t:
 *
 * Column metadata. Supports both regular and compound columns.
 * Compound metadata fields are populated for compound_kind != NONE.
 */
typedef struct {
    const char *name;
    wirelog_column_type_t type;
    wirelog_compound_kind_t compound_kind;      /* NONE if not compound */
    uint32_t compound_functor_id;               /* functor ID (intern'd) if compound */
    uint32_t compound_arity;                    /* functor arity if compound */
    uint32_t compound_inline_col_offset;        /* physical column offset if inline */
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
