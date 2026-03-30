/*
 * exec_plan.h - wirelog Backend-Agnostic Execution Plan Types
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Defines stable, C ABI compatible types for passing execution plans
 * to any backend (Differential Dataflow, columnar C11, etc.).
 *
 * These types are shared between:
 *   - The DD backend (backend/dd/dd_ffi.h wraps these for plan transport)
 *   - The columnar C11 backend (backend/columnar.c consumes these directly)
 *
 * Split from backend/dd/dd_ffi.h during Phase 2A (Issue #80) to enable
 * backend-agnostic plan consumption without FFI-specific dependencies.
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
 * wl_plan_expr_tag_t:
 *
 * Opcodes for the serialized postfix expression encoding.
 * Each opcode is stored as a single uint8_t byte in the expression
 * buffer, followed by its payload (if any).
 *
 * Value-producing tags (push one value onto the evaluation stack):
 *   WL_PLAN_EXPR_VAR:        Variable reference.
 *                            Payload: [name_len:u16] [name:u8*name_len]
 *   WL_PLAN_EXPR_CONST_INT:  64-bit signed integer literal.
 *                            Payload: [value:i64] (8 bytes, little-endian)
 *   WL_PLAN_EXPR_CONST_STR:  String literal.
 *                            Payload: [len:u16] [data:u8*len]
 *   WL_PLAN_EXPR_BOOL:       Boolean literal.
 *                            Payload: [value:u8] (0=false, 1=true)
 *
 * Binary operator tags (pop 2, push 1):
 *   WL_PLAN_EXPR_ARITH_ADD .. WL_PLAN_EXPR_ARITH_MOD
 *   WL_PLAN_EXPR_CMP_EQ     .. WL_PLAN_EXPR_CMP_GTE
 *
 * Unary aggregate tags (pop 1, push 1):
 *   WL_PLAN_EXPR_AGG_COUNT .. WL_PLAN_EXPR_AGG_MAX
 */
typedef enum {
    /* Value producers */
    WL_PLAN_EXPR_VAR = 0x01,
    WL_PLAN_EXPR_CONST_INT = 0x02,
    WL_PLAN_EXPR_CONST_STR = 0x03,
    WL_PLAN_EXPR_BOOL = 0x04,

    /* Arithmetic operators (binary, pop 2 push 1) */
    WL_PLAN_EXPR_ARITH_ADD = 0x10,
    WL_PLAN_EXPR_ARITH_SUB = 0x11,
    WL_PLAN_EXPR_ARITH_MUL = 0x12,
    WL_PLAN_EXPR_ARITH_DIV = 0x13,
    WL_PLAN_EXPR_ARITH_MOD = 0x14,

    /* Bitwise operators (binary, pop 2 push 1) */
    WL_PLAN_EXPR_ARITH_BAND = 0x15,
    WL_PLAN_EXPR_ARITH_BOR = 0x16,
    WL_PLAN_EXPR_ARITH_BXOR = 0x17,
    WL_PLAN_EXPR_ARITH_SHL = 0x18,
    WL_PLAN_EXPR_ARITH_SHR = 0x19,

    /* Unary bitwise NOT */
    WL_PLAN_EXPR_ARITH_BNOT = 0x1A,

    /* Unary hash function (xxHash3 64-bit, pop 1 push 1) */
    WL_PLAN_EXPR_ARITH_HASH = 0x1B,

    /* Unary CRC-32 functions (pop 1 push 1) */
    WL_PLAN_EXPR_ARITH_CRC32_ETH
        = 0x1C, /* CRC-32 Ethernet/ISO (poly 0x04C11DB7) */
    WL_PLAN_EXPR_ARITH_CRC32_CAST
        = 0x1D, /* CRC-32C Castagnoli (poly 0x1EDC6F41) */

    /* Unary cryptographic hash functions (pop 1 push 1, requires mbedTLS) */
    WL_PLAN_EXPR_ARITH_MD5 = 0x1E,  /* md5() - MD5 32-char hex digest */
    WL_PLAN_EXPR_ARITH_SHA1 = 0x1F, /* sha1() - SHA-1 40-char hex digest */
    WL_PLAN_EXPR_ARITH_SHA256
        = 0x20, /* sha256() - SHA-256 64-char hex digest */
    WL_PLAN_EXPR_ARITH_SHA512
        = 0x21, /* sha512() - SHA-512 128-char hex digest */

    /* Binary cryptographic hash functions (pop 2 push 1, requires mbedTLS) */
    WL_PLAN_EXPR_ARITH_HMAC_SHA256
        = 0x28, /* hmac_sha256(msg, key) - HMAC-SHA-256 64-char hex digest */

    /* UUID functions (requires mbedTLS) */
    WL_PLAN_EXPR_ARITH_UUID4 = 0x29, /* uuid4() - nullary, push 1 */
    WL_PLAN_EXPR_ARITH_UUID5 = 0x2A, /* uuid5(ns, name) - pop 2 push 1 */

    /* Comparison operators (binary, pop 2 push 1) */
    WL_PLAN_EXPR_CMP_EQ = 0x22,
    WL_PLAN_EXPR_CMP_NEQ = 0x23,
    WL_PLAN_EXPR_CMP_LT = 0x24,
    WL_PLAN_EXPR_CMP_GT = 0x25,
    WL_PLAN_EXPR_CMP_LTE = 0x26,
    WL_PLAN_EXPR_CMP_GTE = 0x27,

    /* Aggregate operators (unary, pop 1 push 1) */
    WL_PLAN_EXPR_AGG_COUNT = 0x30,
    WL_PLAN_EXPR_AGG_SUM = 0x31,
    WL_PLAN_EXPR_AGG_MIN = 0x32,
    WL_PLAN_EXPR_AGG_MAX = 0x33,
} wl_plan_expr_tag_t;

/**
 * wl_plan_expr_buffer_t:
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
} wl_plan_expr_buffer_t;

/* ======================================================================== */
/* Delta Mode (Semi-Naive Multi-Way Join Expansion)                         */
/* ======================================================================== */

/**
 * wl_delta_mode_t:
 *
 * Controls delta/full relation selection in VARIABLE and JOIN operators
 * for multi-way semi-naive evaluation.
 *
 * For a K-atom recursive rule (K >= 2), the plan generator emits K
 * copies of the rule plan, each with exactly one body atom forced to
 * use its delta relation and the rest forced to use full relations.
 * This ensures every ΔR_i × R_1_full × ... × R_K_full permutation
 * is covered, which is required for correct semi-naive evaluation.
 *
 * WL_DELTA_AUTO:        Heuristic selection (current default behavior).
 *                       VARIABLE picks delta when it is a strict subset
 *                       of full; JOIN applies right-delta heuristically.
 * WL_DELTA_FORCE_DELTA: Force delta version of the relation.
 *                       VARIABLE selects "$d$<name>" if it exists and
 *                       has rows; otherwise produces empty result.
 *                       JOIN forces right-delta substitution.
 * WL_DELTA_FORCE_FULL:  Force full version of the relation.
 *                       VARIABLE always selects the full relation.
 *                       JOIN skips right-delta substitution.
 * WL_DELTA_FORCE_EMPTY: Force empty result unconditionally (#370).
 *                       Used to neuter UNION child segments in
 *                       multiway delta expansion when the segment
 *                       contains no FORCE_DELTA op, preventing
 *                       wasteful full-join computation.
 */
typedef enum {
    WL_DELTA_AUTO = 0,
    WL_DELTA_FORCE_DELTA = 1,
    WL_DELTA_FORCE_FULL = 2,
    WL_DELTA_FORCE_EMPTY = 3,
} wl_delta_mode_t;

/* ======================================================================== */
/* Operator Types                                                           */
/* ======================================================================== */

/**
 * wl_plan_op_type_t:
 *
 * Operator types in a backend execution plan.
 * Explicit integer values for stable ABI across backends.
 *
 * WL_PLAN_OP_VARIABLE:    Reference to an input collection (EDB or IDB).
 * WL_PLAN_OP_MAP:         Column projection / rename.
 * WL_PLAN_OP_FILTER:      Predicate filter (expr in serialized buffer).
 * WL_PLAN_OP_JOIN:        Equijoin on key columns.
 * WL_PLAN_OP_ANTIJOIN:    Negation (antijoin).
 * WL_PLAN_OP_REDUCE:      Aggregation (group-by + aggregate function).
 * WL_PLAN_OP_CONCAT:      Union of multiple collections.
 * WL_PLAN_OP_CONSOLIDATE: Deduplication / consolidation.
 * WL_PLAN_OP_SEMIJOIN:    Semijoin (SIP pre-filter).
 * WL_PLAN_OP_LFTJ:        Multi-way leapfrog triejoin on a single shared key
 *                          column across k >= 3 EDB relations (Issue #195).
 * WL_PLAN_OP_EXCHANGE:    Redistribute tuples by hash(key_columns) % W across
 *                          workers for partition-correct parallel evaluation
 *                          (Issue #316).  opaque_data points to
 *                          wl_plan_op_exchange_t.
 */
typedef enum {
    WL_PLAN_OP_VARIABLE = 0,
    WL_PLAN_OP_MAP = 1,
    WL_PLAN_OP_FILTER = 2,
    WL_PLAN_OP_JOIN = 3,
    WL_PLAN_OP_ANTIJOIN = 4,
    WL_PLAN_OP_REDUCE = 5,
    WL_PLAN_OP_CONCAT = 6,
    WL_PLAN_OP_CONSOLIDATE = 7,
    WL_PLAN_OP_SEMIJOIN = 8,
    WL_PLAN_OP_K_FUSION = 9,
    WL_PLAN_OP_LFTJ = 10,
    WL_PLAN_OP_EXCHANGE = 11, /* Redistribute tuples by hash(key) % W */
} wl_plan_op_type_t;

/* ======================================================================== */
/* Operator Node                                                            */
/* ======================================================================== */

/**
 * wl_plan_op_t:
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
 *   JOIN:        right_relation, right_filter_expr, left_keys, right_keys,
 *                key_count
 *   ANTIJOIN:    right_relation, right_filter_expr, left_keys, right_keys,
 *                key_count
 *   REDUCE:      agg_fn, group_by_indices, group_by_count
 *   CONCAT:      (no fields used)
 *   CONSOLIDATE: (no fields used)
 *   SEMIJOIN:    right_relation, right_filter_expr, left_keys, right_keys,
 *                key_count, project_indices, project_count
 *   K_FUSION:    opaque_data (points to wl_plan_op_k_fusion_t in columnar backend)
 *   LFTJ:        opaque_data (points to wl_plan_op_lftj_t in columnar backend)
 *   EXCHANGE:    opaque_data (points to wl_plan_op_exchange_t in columnar backend)
 */
typedef struct {
    wl_plan_op_type_t op;

    const char *relation_name;
    const char *right_relation;

    const char *const *left_keys;
    const char *const *right_keys;
    uint32_t key_count;

    const uint32_t *project_indices;
    uint32_t project_count;

    wl_plan_expr_buffer_t filter_expr;

    wirelog_agg_fn_t agg_fn;
    const uint32_t *group_by_indices;
    uint32_t group_by_count;

    wl_plan_expr_buffer_t *map_exprs;
    uint32_t map_expr_count;

    wl_delta_mode_t delta_mode; /* semi-naive delta/full selection control */
    bool materialized; /* hint: cache this intermediate result for CSE reuse */

    /* Backend-specific metadata.  NULL for all ops except K_FUSION.
     * For K_FUSION: points to a wl_plan_op_k_fusion_t (defined in
     * backend/columnar_nanoarrow.h) containing K operator sequences
     * for parallel semi-naive evaluation.  Owned by the plan; freed
     * via wl_plan_free() -> free_op() path. */
    void *opaque_data;

    /* Filter predicate applied to right-child tuples before join probe.
    * Placed after opaque_data to avoid shifting hot fields (delta_mode,
    * opaque_data) that were present in the original 136-byte layout. */
    wl_plan_expr_buffer_t right_filter_expr;
} wl_plan_op_t;

/* ======================================================================== */
/* Per-Relation Plan                                                        */
/* ======================================================================== */

/**
 * wl_plan_relation_t:
 *
 * Operator sequence for a single IDB relation within a stratum.
 *
 * @name:      Null-terminated relation name (caller-owned).
 * @ops:       Array of operator descriptors (caller-owned).
 * @op_count:  Number of operators in the sequence.
 */
typedef struct {
    const char *name;
    const wl_plan_op_t *ops;
    uint32_t op_count;
} wl_plan_relation_t;

/* ======================================================================== */
/* Stratum Plan                                                             */
/* ======================================================================== */

/**
 * wl_plan_stratum_t:
 *
 * Execution plan for a single stratum.
 *
 * @stratum_id:     Stratum index (0 = executed first).
 * @is_recursive:   True if this stratum requires fixed-point iteration.
 * @is_monotone:    True if all rules in this stratum only derive facts
 *                  (no deletion via negation/antijoin/subtraction).
 *                  Used for DRedL-style deletion phase optimization.
 * @relations:      Array of per-relation plans (caller-owned).
 * @relation_count: Number of relations in this stratum.
 */
typedef struct {
    uint32_t stratum_id;
    bool is_recursive;
    bool is_monotone;
    const wl_plan_relation_t *relations;
    uint32_t relation_count;
} wl_plan_stratum_t;

/* ======================================================================== */
/* Full Execution Plan                                                      */
/* ======================================================================== */

/**
 * wl_plan_t:
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
    const wl_plan_stratum_t *strata;
    uint32_t stratum_count;
    const char *const *edb_relations;
    uint32_t edb_count;
} wl_plan_t;

#endif /* WL_EXEC_PLAN_H */
