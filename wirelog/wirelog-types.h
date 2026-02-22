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
    WIRELOG_TYPE_INT32 = 0,      /* 32-bit signed integer */
    WIRELOG_TYPE_INT64 = 1,      /* 64-bit signed integer */
    WIRELOG_TYPE_UINT32 = 2,     /* 32-bit unsigned integer */
    WIRELOG_TYPE_UINT64 = 3,     /* 64-bit unsigned integer */
    WIRELOG_TYPE_FLOAT = 4,      /* 64-bit floating point */
    WIRELOG_TYPE_STRING = 5,     /* UTF-8 string */
    WIRELOG_TYPE_BOOL = 6,       /* Boolean */
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
/* IR Node Types                                                            */
/* ======================================================================== */

/**
 * wirelog_ir_op_type_t:
 *
 * Intermediate representation operator types
 */
typedef enum {
    WIRELOG_OP_SCAN = 0,         /* Scan a base relation */
    WIRELOG_OP_PROJECT = 1,      /* Project columns */
    WIRELOG_OP_FILTER = 2,       /* Apply predicate filter */
    WIRELOG_OP_JOIN = 3,         /* Join two relations */
    WIRELOG_OP_FLATMAP = 4,      /* Fused join+map+filter */
    WIRELOG_OP_AGGREGATE = 5,    /* Aggregation */
    WIRELOG_OP_ANTIJOIN = 6,     /* Antijoin (negation) */
    WIRELOG_OP_UNION = 7,        /* Union (append) */
} wirelog_ir_op_type_t;

/* ======================================================================== */
/* Stratification                                                           */
/* ======================================================================== */

/**
 * wirelog_stratum_t:
 *
 * A stratum in the stratified program
 */
typedef struct {
    uint32_t stratum_id;
    const char **rule_names;
    uint32_t rule_count;
} wirelog_stratum_t;

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_TYPES_H */
