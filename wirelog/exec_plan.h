/*
 * exec_plan.h - wirelog Backend-Agnostic Execution Plan Types
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Defines FFI-safe (C ABI compatible) types for passing execution plans
 * to any backend (Differential Dataflow, columnar C11, etc.).
 *
 * These types are shared between:
 *   - The DD backend (ffi/dd_ffi.h wraps these for Rust FFI transport)
 *   - The columnar C11 backend (backend/columnar.c consumes these directly)
 *
 * Split from ffi/dd_ffi.h during Phase 2A (Issue #80) to enable
 * backend-agnostic plan consumption without Rust FFI dependencies.
 *
 * ========================================================================
 * Filter Expression Serialization
 * ========================================================================
 *
 * Filter predicates are serialized into a flat byte buffer using
 * postfix (Reverse Polish Notation) encoding for FFI transport.
 *
 * Encoding format (all values little-endian):
 *   Each instruction is: [tag:u8] [payload...]
 *
 *   Tag                   Payload
 *   ---                   -------
 *   EXPR_VAR              [name_len:u16] [name:u8*name_len]  (no NUL)
 *   EXPR_CONST_INT        [value:i64]
 *   EXPR_CONST_STR        [len:u16] [data:u8*len]            (no NUL)
 *   EXPR_BOOL             [value:u8]   (0 = false, 1 = true)
 *   EXPR_ARITH_ADD..MOD   (no payload; pops 2 operands from stack)
 *   EXPR_CMP_EQ..GTE      (no payload; pops 2 operands from stack)
 *   EXPR_AGG_COUNT..MAX   (no payload; pops 1 operand from stack)
 *
 * Evaluation: walk the buffer left-to-right, push values onto a stack.
 * Operators pop their operands and push the result.  A well-formed
 * expression leaves exactly one value on the stack.
 *
 * Example:  X > 5  encodes as:
 *   [EXPR_VAR][1][X] [EXPR_CONST_INT][5,0,0,0,0,0,0,0] [EXPR_CMP_GT]
 */

#ifndef WL_EXEC_PLAN_H
#define WL_EXEC_PLAN_H

#include "wirelog-types.h"

#include <stdbool.h>
#include <stdint.h>

/* ======================================================================== */
/* Serialized Expression Buffer                                             */
/* ======================================================================== */

/**
 * wl_ffi_expr_tag_t:
 *
 * Opcodes for the serialized postfix expression encoding.
 * Each opcode is stored as a single uint8_t byte in the expression
 * buffer, followed by its payload (if any).
 *
 * Value-producing tags (push one value onto the evaluation stack):
 *   WL_FFI_EXPR_VAR:        Variable reference.
 *                            Payload: [name_len:u16] [name:u8*name_len]
 *   WL_FFI_EXPR_CONST_INT:  64-bit signed integer literal.
 *                            Payload: [value:i64] (8 bytes, little-endian)
 *   WL_FFI_EXPR_CONST_STR:  String literal.
 *                            Payload: [len:u16] [data:u8*len]
 *   WL_FFI_EXPR_BOOL:       Boolean literal.
 *                            Payload: [value:u8] (0=false, 1=true)
 *
 * Binary operator tags (pop 2, push 1):
 *   WL_FFI_EXPR_ARITH_ADD .. WL_FFI_EXPR_ARITH_MOD
 *   WL_FFI_EXPR_CMP_EQ     .. WL_FFI_EXPR_CMP_GTE
 *
 * Unary aggregate tags (pop 1, push 1):
 *   WL_FFI_EXPR_AGG_COUNT .. WL_FFI_EXPR_AGG_MAX
 */
typedef enum {
    /* Value producers */
    WL_FFI_EXPR_VAR = 0x01,
    WL_FFI_EXPR_CONST_INT = 0x02,
    WL_FFI_EXPR_CONST_STR = 0x03,
    WL_FFI_EXPR_BOOL = 0x04,

    /* Arithmetic operators (binary, pop 2 push 1) */
    WL_FFI_EXPR_ARITH_ADD = 0x10,
    WL_FFI_EXPR_ARITH_SUB = 0x11,
    WL_FFI_EXPR_ARITH_MUL = 0x12,
    WL_FFI_EXPR_ARITH_DIV = 0x13,
    WL_FFI_EXPR_ARITH_MOD = 0x14,

    /* Comparison operators (binary, pop 2 push 1) */
    WL_FFI_EXPR_CMP_EQ = 0x20,
    WL_FFI_EXPR_CMP_NEQ = 0x21,
    WL_FFI_EXPR_CMP_LT = 0x22,
    WL_FFI_EXPR_CMP_GT = 0x23,
    WL_FFI_EXPR_CMP_LTE = 0x24,
    WL_FFI_EXPR_CMP_GTE = 0x25,

    /* Aggregate operators (unary, pop 1 push 1) */
    WL_FFI_EXPR_AGG_COUNT = 0x30,
    WL_FFI_EXPR_AGG_SUM = 0x31,
    WL_FFI_EXPR_AGG_MIN = 0x32,
    WL_FFI_EXPR_AGG_MAX = 0x33,
} wl_ffi_expr_tag_t;

/**
 * wl_ffi_expr_buffer_t:
 *
 * Flat byte buffer containing a serialized expression in postfix
 * (RPN) encoding.  Replaces pointer-based expression trees for FFI.
 *
 * @data:  Byte buffer (owned by caller).  NULL if no expression.
 * @size:  Number of bytes in the buffer.  0 if no expression.
 */
typedef struct {
    uint8_t *data;
    uint32_t size;
} wl_ffi_expr_buffer_t;

/* ======================================================================== */
/* Operator Types                                                           */
/* ======================================================================== */

/**
 * wl_ffi_op_type_t:
 *
 * Operator types in a backend execution plan.
 * Explicit integer values for stable ABI across backends.
 *
 * WL_FFI_OP_VARIABLE:    Reference to an input collection (EDB or IDB).
 * WL_FFI_OP_MAP:         Column projection / rename.
 * WL_FFI_OP_FILTER:      Predicate filter (expr in serialized buffer).
 * WL_FFI_OP_JOIN:        Equijoin on key columns.
 * WL_FFI_OP_ANTIJOIN:    Negation (antijoin).
 * WL_FFI_OP_REDUCE:      Aggregation (group-by + aggregate function).
 * WL_FFI_OP_CONCAT:      Union of multiple collections.
 * WL_FFI_OP_CONSOLIDATE: Deduplication / consolidation.
 * WL_FFI_OP_SEMIJOIN:    Semijoin (SIP pre-filter).
 */
typedef enum {
    WL_FFI_OP_VARIABLE = 0,
    WL_FFI_OP_MAP = 1,
    WL_FFI_OP_FILTER = 2,
    WL_FFI_OP_JOIN = 3,
    WL_FFI_OP_ANTIJOIN = 4,
    WL_FFI_OP_REDUCE = 5,
    WL_FFI_OP_CONCAT = 6,
    WL_FFI_OP_CONSOLIDATE = 7,
    WL_FFI_OP_SEMIJOIN = 8,
} wl_ffi_op_type_t;

/* ======================================================================== */
/* Operator Node                                                            */
/* ======================================================================== */

/**
 * wl_ffi_op_t:
 *
 * Flat operator descriptor.  All pointer fields point to caller-owned
 * memory.  The backend must NOT free or retain these pointers beyond
 * the duration of plan execution.
 *
 * Field usage by operator type:
 *
 *   VARIABLE:    relation_name
 *   MAP:         project_indices, project_count  (and/or map_exprs)
 *   FILTER:      filter_expr
 *   JOIN:        right_relation, left_keys, right_keys, key_count
 *   ANTIJOIN:    right_relation, left_keys, right_keys, key_count
 *   REDUCE:      agg_fn, group_by_indices, group_by_count
 *   CONCAT:      (no fields used)
 *   CONSOLIDATE: (no fields used)
 *   SEMIJOIN:    right_relation, left_keys, right_keys, key_count,
 *                project_indices, project_count
 */
typedef struct {
    wl_ffi_op_type_t op;

    const char *relation_name;
    const char *right_relation;

    const char *const *left_keys;
    const char *const *right_keys;
    uint32_t key_count;

    const uint32_t *project_indices;
    uint32_t project_count;

    wl_ffi_expr_buffer_t filter_expr;

    wirelog_agg_fn_t agg_fn;
    const uint32_t *group_by_indices;
    uint32_t group_by_count;

    wl_ffi_expr_buffer_t *map_exprs;
    uint32_t map_expr_count;
} wl_ffi_op_t;

/* ======================================================================== */
/* Per-Relation Plan                                                        */
/* ======================================================================== */

/**
 * wl_ffi_relation_plan_t:
 *
 * Operator sequence for a single IDB relation within a stratum.
 *
 * @name:      Null-terminated relation name (caller-owned).
 * @ops:       Array of operator descriptors (caller-owned).
 * @op_count:  Number of operators in the sequence.
 */
typedef struct {
    const char *name;
    const wl_ffi_op_t *ops;
    uint32_t op_count;
} wl_ffi_relation_plan_t;

/* ======================================================================== */
/* Stratum Plan                                                             */
/* ======================================================================== */

/**
 * wl_ffi_stratum_plan_t:
 *
 * Execution plan for a single stratum.
 *
 * @stratum_id:     Stratum index (0 = executed first).
 * @is_recursive:   True if this stratum requires fixed-point iteration.
 * @relations:      Array of per-relation plans (caller-owned).
 * @relation_count: Number of relations in this stratum.
 */
typedef struct {
    uint32_t stratum_id;
    bool is_recursive;
    const wl_ffi_relation_plan_t *relations;
    uint32_t relation_count;
} wl_ffi_stratum_plan_t;

/* ======================================================================== */
/* Full Execution Plan                                                      */
/* ======================================================================== */

/**
 * wl_ffi_plan_t:
 *
 * Complete execution plan for a stratified Datalog program.
 * Passed to any backend (DD via FFI, or columnar directly).
 *
 * @strata:        Array of stratum plans ordered by execution sequence.
 *                 Stratum 0 executes first (caller-owned).
 * @stratum_count: Number of strata.
 * @edb_relations: Array of null-terminated EDB (input) relation names
 *                 (caller-owned array and strings).
 * @edb_count:     Number of EDB relations.
 */
typedef struct {
    const wl_ffi_stratum_plan_t *strata;
    uint32_t stratum_count;
    const char *const *edb_relations;
    uint32_t edb_count;
} wl_ffi_plan_t;

#endif /* WL_EXEC_PLAN_H */
