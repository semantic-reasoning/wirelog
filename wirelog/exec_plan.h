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
 *   WL_PLAN_EXPR_CONST_FLOAT: finite binary64 literal.
 *                            Payload: [bits:u64] (8 bytes, little-endian)
 *   WL_PLAN_EXPR_CONST_STR:  String literal.
 *                            Payload: [len:u16] [data:u8*len]
 *   WL_PLAN_EXPR_BOOL:       Boolean literal.
 *                            Payload: [value:u8] (0=false, 1=true)
 *
 * Binary operator tags (pop 2, push 1):
 *   WL_PLAN_EXPR_ARITH_ADD .. WL_PLAN_EXPR_ARITH_MOD
 *   WL_PLAN_EXPR_ARITH_FLOAT_ADD .. WL_PLAN_EXPR_ARITH_FLOAT_DIV
 *   WL_PLAN_EXPR_CMP_EQ     .. WL_PLAN_EXPR_CMP_GTE
 *   WL_PLAN_EXPR_CMP_FLOAT_EQ .. WL_PLAN_EXPR_CMP_FLOAT_GTE
 *
 * Unary aggregate tags (pop 1, push 1):
 *   WL_PLAN_EXPR_AGG_COUNT .. WL_PLAN_EXPR_AGG_AVG
 */
typedef enum {
    /* Value producers */
    WL_PLAN_EXPR_VAR = 0x01,
    WL_PLAN_EXPR_CONST_INT = 0x02,
    WL_PLAN_EXPR_CONST_STR = 0x03,
    WL_PLAN_EXPR_BOOL = 0x04,
    WL_PLAN_EXPR_VAR_FLOAT = 0x05, /* variable containing a binary64 lane */
    WL_PLAN_EXPR_CONST_FLOAT = 0x06, /* payload: binary64 bits, little-endian */

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

    /* Floating-point arithmetic (binary64, finite operands/results). */
    WL_PLAN_EXPR_ARITH_FLOAT_ADD = 0x70,
    WL_PLAN_EXPR_ARITH_FLOAT_SUB = 0x71,
    WL_PLAN_EXPR_ARITH_FLOAT_MUL = 0x72,
    WL_PLAN_EXPR_ARITH_FLOAT_DIV = 0x73,

    /* Unary bitwise NOT */
    WL_PLAN_EXPR_ARITH_BNOT = 0x1A,

    /* Unary hash function (xxHash3 64-bit, pop 1 push 1) */
    WL_PLAN_EXPR_ARITH_HASH = 0x1B,

    /* Unary CRC-32 functions (pop 1 push 1) */
    WL_PLAN_EXPR_ARITH_CRC32_ETH
        = 0x1C, /* CRC-32 Ethernet/ISO (poly 0x04C11DB7) */
    WL_PLAN_EXPR_ARITH_CRC32_CAST
        = 0x1D, /* CRC-32C Castagnoli (poly 0x1EDC6F41) */

    /* Unary mbedTLS digest functions (pop 1, push folded int64) */
    WL_PLAN_EXPR_ARITH_MD5 = 0x1E,  /* md5() - MD5 digest */
    WL_PLAN_EXPR_ARITH_SHA1 = 0x1F, /* sha1() - SHA-1 digest */
    WL_PLAN_EXPR_ARITH_SHA256
        = 0x20, /* sha256() - SHA-256 digest */
    WL_PLAN_EXPR_ARITH_SHA512
        = 0x21, /* sha512() - SHA-512 digest */

    /* Binary mbedTLS HMAC function (pop 2, push folded int64) */
    WL_PLAN_EXPR_ARITH_HMAC_SHA256
        = 0x28, /* hmac_sha256(msg, key) - HMAC-SHA-256 */

    /* UUID functions (requires mbedTLS, push UUID prefix as int64) */
    WL_PLAN_EXPR_ARITH_UUID4 = 0x29, /* uuid4() - nullary */
    WL_PLAN_EXPR_ARITH_UUID5 = 0x2A, /* uuid5(ns, name) - pop 2 */

    /* Comparison operators (binary, pop 2 push 1) */
    WL_PLAN_EXPR_CMP_EQ = 0x22,
    WL_PLAN_EXPR_CMP_NEQ = 0x23,
    WL_PLAN_EXPR_CMP_LT = 0x24,
    WL_PLAN_EXPR_CMP_GT = 0x25,
    WL_PLAN_EXPR_CMP_LTE = 0x26,
    WL_PLAN_EXPR_CMP_GTE = 0x27,

    /* Floating-point comparisons (binary64, finite operands). */
    WL_PLAN_EXPR_CMP_FLOAT_EQ = 0x74,
    WL_PLAN_EXPR_CMP_FLOAT_NEQ = 0x75,
    WL_PLAN_EXPR_CMP_FLOAT_LT = 0x76,
    WL_PLAN_EXPR_CMP_FLOAT_GT = 0x77,
    WL_PLAN_EXPR_CMP_FLOAT_LTE = 0x78,
    WL_PLAN_EXPR_CMP_FLOAT_GTE = 0x79,

    /* Aggregate operators (unary, pop 1 push 1) */
    WL_PLAN_EXPR_AGG_COUNT = 0x30,
    WL_PLAN_EXPR_AGG_SUM = 0x31,
    WL_PLAN_EXPR_AGG_MIN = 0x32,
    WL_PLAN_EXPR_AGG_MAX = 0x33,
    WL_PLAN_EXPR_AGG_AVG = 0x34,

    /* String function operators (operands are intern IDs as int64_t) */
    WL_PLAN_EXPR_STR_FN_STRLEN     = 0x40, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_CAT        = 0x41, /* binary:  pop 2 push 1 */
    WL_PLAN_EXPR_STR_FN_SUBSTR     = 0x42, /* ternary: pop 3 push 1 */
    WL_PLAN_EXPR_STR_FN_CONTAINS   = 0x43, /* binary:  pop 2 push bool */
    WL_PLAN_EXPR_STR_FN_STR_PREFIX = 0x44, /* binary:  pop 2 push bool */
    WL_PLAN_EXPR_STR_FN_STR_SUFFIX = 0x45, /* binary:  pop 2 push bool */
    WL_PLAN_EXPR_STR_FN_STR_ORD    = 0x46, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_TO_UPPER   = 0x47, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_TO_LOWER   = 0x48, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_STR_REPLACE= 0x49, /* ternary: pop 3 push 1 */
    WL_PLAN_EXPR_STR_FN_TRIM       = 0x4A, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_TO_STRING  = 0x4B, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_TO_NUMBER  = 0x4C, /* unary:   pop 1 push 1 */
    WL_PLAN_EXPR_STR_FN_UUID5_RFC  = 0x4D, /* binary: pop 2 push 1 */

    /* String comparison operators (intern IDs → bool, via strcmp) */
    WL_PLAN_EXPR_CMP_STR_EQ  = 0x50, /* binary: pop 2 push bool */
    WL_PLAN_EXPR_CMP_STR_NEQ = 0x51,
    WL_PLAN_EXPR_CMP_STR_LT  = 0x52,
    WL_PLAN_EXPR_CMP_STR_GT  = 0x53,
    WL_PLAN_EXPR_CMP_STR_LTE = 0x54,
    WL_PLAN_EXPR_CMP_STR_GTE = 0x55,

    /*
     * Digest opcodes whose operands are symbols (Issue #963).
     *
     * The 0x1B..0x2A family digests the int64_t on the stack.  When that
     * int64_t is an interned symbol id, digesting it answers a question
     * about the intern table rather than about the string: hash("abc")
     * matches no external tool, and the same string digests differently
     * depending on what was interned first.  These opcodes reverse the id
     * and digest the string's own bytes instead.
     *
     * Byte convention: strlen(s) bytes, no NUL terminator.  That is what
     * `printf '%s' abc | xxhsum -H3` and `crc32(payload)` see, so the
     * values are reproducible outside wirelog.
     *
     * All are payload-free, exactly like the integer opcodes they shadow,
     * so a decoder that does not know them still advances by one byte and
     * stays in sync with the rest of the stream.
     *
     * hmac_sha256() and uuid5() take two operands that type independently,
     * so each gets three opcodes; the II case keeps the existing opcode.
     * _SS = both symbols, _SI = first symbol, _IS = second symbol.
     *
     * The UUID5_* opcodes additionally frame each operand -- a one-byte
     * domain tag ('S' for a symbol's bytes, 'I' for an int64's) followed by
     * its length as a little-endian uint64 -- which the 0x2A opcode does
     * not.  Without the length, two variable-length operands could be split
     * at more than one point: uuid5("ab", "c") and uuid5("a", "bc") would
     * hash the same bytes.  Without the tag, an operand's type would not be
     * recoverable either: uuid5("abcdefgh", x) and uuid5(<the int64 whose
     * little-endian bytes are "abcdefgh">, x) would hash the same bytes.
     * 0x2A's two operands are 8 bytes each and both int64, so neither
     * ambiguity exists there and its bytes are unchanged.
     *
     * The framing makes the digest *input* injective over typed operand
     * pairs.  It does not make uuid5() injective: the return is the first 8
     * bytes of the digest with a version nibble forced, so at most 2^60
     * distinct values exist regardless.  See docs/SECURITY_MODEL.md.
     */
    WL_PLAN_EXPR_ARITH_HASH_S        = 0x60, /* unary:  pop 1 push 1 */
    WL_PLAN_EXPR_ARITH_CRC32_ETH_S   = 0x61,
    WL_PLAN_EXPR_ARITH_CRC32_CAST_S  = 0x62,
    WL_PLAN_EXPR_ARITH_MD5_S         = 0x63,
    WL_PLAN_EXPR_ARITH_SHA1_S        = 0x64,
    WL_PLAN_EXPR_ARITH_SHA256_S      = 0x65,
    WL_PLAN_EXPR_ARITH_SHA512_S      = 0x66,
    WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS = 0x67, /* binary: pop 2 push 1 */
    WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI = 0x68,
    WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS = 0x69,
    WL_PLAN_EXPR_ARITH_UUID5_SS      = 0x6A, /* binary: pop 2 push 1 */
    WL_PLAN_EXPR_ARITH_UUID5_SI      = 0x6B,
    WL_PLAN_EXPR_ARITH_UUID5_IS      = 0x6C,
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
 * WL_DELTA_FORCE_EMPTY_AFTER_SEED:
 *                       Use normal AUTO selection for the seed pass, then
 *                       force empty in outbound TDD sub-passes.  Used for
 *                       EDB-only UNION child segments that seed a recursive
 *                       relation once but do not need repeated evaluation.
 */
typedef enum {
    WL_DELTA_AUTO = 0,
    WL_DELTA_FORCE_DELTA = 1,
    WL_DELTA_FORCE_FULL = 2,
    WL_DELTA_FORCE_EMPTY = 3,
    WL_DELTA_FORCE_EMPTY_AFTER_SEED = 4,
} wl_delta_mode_t;

/**
 * wl_plan_agg_operand_t:
 *
 * The value domain of a REDUCE operator's aggregated operand (Issue #965).
 *
 * Every value a plan moves is an int64_t.  What differs is whether MIN/MAX
 * should order it as a number or as the string whose interned id it is --
 * ids are assigned in first-appearance order, so ordering symbols by id
 * makes the answer depend on which unrelated facts happened to be parsed
 * first.  Only the plan generator knows which of the two a column holds,
 * because only it has the `.decl` types.
 *
 * UNKNOWN is the zero value on purpose.  op_list_push() and clone_plan_op()
 * both memset() their op before filling it in, so an op that no producer
 * typed is structurally distinguishable from one deliberately typed
 * SCALAR, rather than silently claiming to be numeric.
 */
typedef enum {
    WL_PLAN_AGG_OPERAND_UNKNOWN = 0,
    WL_PLAN_AGG_OPERAND_SCALAR = 1,
    WL_PLAN_AGG_OPERAND_STRING = 2,
    WL_PLAN_AGG_OPERAND_FLOAT = 3,
} wl_plan_agg_operand_t;

/**
 * wl_plan_agg_spec_t:
 *
 * The order a recursive relation's rows may be reduced under as a whole
 * (Issue #975).
 *
 * A relation whose every rule reduces by the same ordering aggregate over
 * the same grouping columns has one row per group at the fixpoint, not one
 * row per group per rule: each rule computes a partial minimum (or maximum)
 * and the union of those partials still has to be reduced once more.  This
 * records that the relation admits that final reduction, and under which
 * order.
 *
 * It is a property of the relation, established at IR lowering, and NOT
 * something to be recovered by scanning the operator list.  The scan is what
 * #975 was: rewrite_multiway_delta() (exec_plan_gen.c) replaces a fused
 * relation's operators with a single K_FUSION and moves the originals into
 * its opaque_data, so a REDUCE lookup over ops found nothing and the final
 * reduction was skipped -- silently, on exactly the relations fusion exists
 * to accelerate.  Recorded before any rewrite runs, a rewrite cannot delete
 * a field that is not an operator.
 *
 * has_spec == false is the zero value, matching the discipline documented
 * for wl_plan_agg_operand_t above: a relation nobody typed is structurally
 * distinguishable from one deliberately admitted.  It is also the answer for
 * every relation the final reduction must refuse -- see the accumulator in
 * exec_plan_gen.c for the conflicts that clear it.
 *
 * @has_spec:       True when the relation admits whole-relation reduction.
 * @fn:             The aggregate every rule of the relation agreed on.
 *                  Only WIRELOG_AGG_MIN and WIRELOG_AGG_MAX are admitted.
 * @operand_type:   The aggregated operand's domain, which decides whether
 *                  the order is numeric or lexicographic (Issue #965).
 * @group_by_count: Number of grouping columns.
 * @aggregate_index: Output column containing the aggregated value.
 */
typedef struct {
    bool has_spec;
    wirelog_agg_fn_t fn;
    wl_plan_agg_operand_t operand_type;
    uint32_t group_by_count;
    uint32_t aggregate_index;
} wl_plan_agg_spec_t;

/* ======================================================================== */
/* Operator Types                                                           */
/* ======================================================================== */

/**
 * wl_plan_op_type_t:
 *
 * Operator types in a backend execution plan.
 * Explicit integer values for stable ABI across backends.
 *
 * Range partitioning (Issue #495):
 *   Universal operators  (0-8):  Backend-agnostic relational algebra ops.
 *                                Use flat fields in wl_plan_op_t only.
 *   Backend-specific ops (9+):   Columnar backend optimizations.
 *                                Use opaque_data for backend-defined metadata.
 *                                See WL_PLAN_OP__BACKEND_START below.
 *
 * --- Universal operators (0-8) ---
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
 *
 * --- Columnar backend operators (9-11) ---
 *
 * WL_PLAN_OP_K_FUSION:    Parallel semi-naive delta expansion (Issue #370).
 *                          opaque_data -> wl_plan_op_k_fusion_t.
 * WL_PLAN_OP_LFTJ:        Multi-way leapfrog triejoin on a single shared key
 *                          column across k >= 3 EDB relations (Issue #195).
 *                          opaque_data -> wl_plan_op_lftj_t.
 * WL_PLAN_OP_EXCHANGE:    Redistribute tuples by hash(key_columns) % W across
 *                          workers for partition-correct parallel evaluation
 *                          (Issue #316).  opaque_data -> wl_plan_op_exchange_t.
 */
typedef enum {
    /* Universal operators (0-8): backend-agnostic */
    WL_PLAN_OP_VARIABLE = 0,
    WL_PLAN_OP_MAP = 1,
    WL_PLAN_OP_FILTER = 2,
    WL_PLAN_OP_JOIN = 3,
    WL_PLAN_OP_ANTIJOIN = 4,
    WL_PLAN_OP_REDUCE = 5,
    WL_PLAN_OP_CONCAT = 6,
    WL_PLAN_OP_CONSOLIDATE = 7,
    WL_PLAN_OP_SEMIJOIN = 8,

    /* Columnar backend operators (9+): use opaque_data */
    WL_PLAN_OP_K_FUSION = 9,
    WL_PLAN_OP_LFTJ = 10,
    WL_PLAN_OP_EXCHANGE = 11,
} wl_plan_op_type_t;

/* First backend-specific operator value.  All ops with numeric value
 * >= WL_PLAN_OP__BACKEND_START carry their metadata in opaque_data
 * (defined by the active backend, e.g. columnar/columnar_nanoarrow.h).
 * Universal ops (< WL_PLAN_OP__BACKEND_START) use flat wl_plan_op_t fields. */
#define WL_PLAN_OP__BACKEND_START 9

/** Returns true if @op is a backend-specific operator (>= BACKEND_START). */
static inline bool
wl_plan_op_is_backend_specific(wl_plan_op_type_t op)
{
    return (int)op >= WL_PLAN_OP__BACKEND_START;
}

/** Returns true if @op is a universal (backend-agnostic) operator. */
static inline bool
wl_plan_op_is_universal(wl_plan_op_type_t op)
{
    return (int)op < WL_PLAN_OP__BACKEND_START;
}

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
 * Universal operators (use flat fields only, opaque_data is NULL):
 *   VARIABLE:    relation_name
 *   MAP:         project_indices, project_count  (and/or map_exprs)
 *   FILTER:      filter_expr
 *   JOIN:        right_relation, right_filter_expr, left_keys, right_keys,
 *                key_count, optional project_indices/project_count for
 *                projection-only JOIN->MAP fusion
 *   ANTIJOIN:    right_relation, right_filter_expr, left_keys, right_keys,
 *                key_count
 *   REDUCE:      agg_fn, group_by_indices, group_by_count
 *   CONCAT:      (no fields used)
 *   CONSOLIDATE: (no fields used)
 *   SEMIJOIN:    right_relation, right_filter_expr, left_keys, right_keys,
 *                key_count, project_indices, project_count
 *
 * Backend-specific operators (>= WL_PLAN_OP__BACKEND_START, use opaque_data):
 *   K_FUSION:    opaque_data -> wl_plan_op_k_fusion_t (columnar)
 *   LFTJ:        opaque_data -> wl_plan_op_lftj_t (columnar)
 *   EXCHANGE:    opaque_data -> wl_plan_op_exchange_t (columnar)
 */
typedef struct {
    wl_plan_op_type_t op;

    const char *relation_name;
    /* A relation *name*, not a subplan.  The plan therefore cannot represent
     * a JOIN/ANTIJOIN/SEMIJOIN whose right child is itself composite: any
     * such tree must be built left-deep, with the composite side on the left.
     * translate_ir_node() rejects a JOIN/ANTIJOIN/SEMIJOIN that violates this
     * rather than
     * emitting NULL here, which used to yield a plan that silently computed
     * nothing (#989). */
    const char *right_relation;

    const char *const *left_keys;
    const char *const *right_keys;
    uint32_t key_count;

    const uint32_t *project_indices;
    uint32_t project_count;

    wl_plan_expr_buffer_t filter_expr;

    wirelog_agg_fn_t agg_fn;
    wl_plan_expr_buffer_t agg_expr;
    const uint32_t *group_by_indices;
    uint32_t group_by_count;
    uint32_t aggregate_index;

    wl_plan_expr_buffer_t *map_exprs;
    uint32_t map_expr_count;

    wl_delta_mode_t delta_mode; /* semi-naive delta/full selection control */
    bool materialized; /* hint: cache this intermediate result for CSE reuse */

    /* Backend-specific metadata.  NULL for all universal ops (< BACKEND_START).
     * For backend-specific ops (K_FUSION, LFTJ, EXCHANGE): points to a
     * backend-defined struct in columnar/columnar_nanoarrow.h.
     * Owned by the plan; freed via wl_plan_free() -> free_op() path. */
    void *opaque_data;

    /* Filter predicate applied to right-child tuples before join probe.
    * Placed after opaque_data to avoid shifting hot fields (delta_mode,
    * opaque_data) that were present in the original 136-byte layout. */
    wl_plan_expr_buffer_t right_filter_expr;

    /* REDUCE only: the value domain of the aggregated operand, which is what
     * decides whether MIN/MAX order by number or by string (Issue #965).
     * Appended at the end so no existing field shifts.  UNKNOWN on every
     * other operator, and on a REDUCE whose operand type could not be
     * established. */
    wl_plan_agg_operand_t agg_operand_type;
    /* REDUCE only: result domain.  FLOAT is binary64; other numeric
     * aggregates retain the legacy int64 lane. */
    wl_plan_agg_operand_t agg_result_type;
} wl_plan_op_t;

/* ======================================================================== */
/* Per-Relation Plan                                                        */
/* ======================================================================== */

/**
 * wl_plan_relation_t:
 *
 * Operator sequence for a single IDB relation within a stratum.
 *
 * @name:       Null-terminated relation name (caller-owned).
 * @delta_name: Pre-computed "$d$<name>" string (caller-owned).
 *              Caches the delta relation name to avoid repeated snprintf
 *              in eval hot paths (Issue #285).
 * @ops:        Array of operator descriptors (caller-owned).
 * @op_count:   Number of operators in the sequence.
 */
typedef struct {
    const char *name;
    char *delta_name;
    const wl_plan_op_t *ops;
    uint32_t op_count;

    /* Whether this relation's rows may be reduced as a whole at the
     * fixpoint, and under which order (Issue #975).  Established at IR
     * lowering, before any plan rewrite runs, precisely so that a rewrite
     * which reshapes @ops cannot take it away.  Appended at the end so no
     * existing field shifts, matching the discipline wl_plan_op_t follows
     * above.  Zeroed (has_spec == false) on every relation that is not a
     * recursive ordering aggregate. */
    wl_plan_agg_spec_t recursive_agg;
} wl_plan_relation_t;

/* ======================================================================== */
/* Stratum Plan                                                             */
/* ======================================================================== */

/**
 * wl_plan_refset_t:
 *
 * De-duplicated set of relation names read by one relation's operator
 * sequence (Issue #1019).
 *
 * @names: Owned array of @count owned, null-terminated names.  NULL when
 *         @count is 0 and the relation had no operators.
 * @count: Number of names.
 */
typedef struct {
    char **names;
    uint32_t count;
} wl_plan_refset_t;

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
 * @rule_refs:      Issue #1019.  @relation_count entries, owned, parallel to
 *                  @relations: the relations each rule reads, recorded at IR
 *                  lowering before any plan rewrite runs.  Two rewrites erase
 *                  those names from @relations[i].ops, by different means:
 *                  rewrite_multiway_delta() really does move the naming
 *                  operators out of reach, into
 *                  wl_plan_op_k_fusion_t.k_ops[] behind
 *                  wl_plan_op_t.opaque_data; rewrite_lftj_chains() moves no
 *                  operators at all -- build_lftj_op() copies only
 *                  rel_names[]/key_cols[] into wl_plan_op_lftj_t and then
 *                  free_op()s the originals, so the operators are gone
 *                  rather than hidden.  Either way a consumer scanning
 *                  @relations[i].ops no longer sees the names.  NULL
 *                  means "not recorded" -- consumers must fall back to scanning
 *                  the operator list.  Held on the stratum rather than on
 *                  wl_plan_relation_t because relation structs get rebuilt
 *                  field by field in several places -- col_plan_split_at_exchange()
 *                  and the two designated initialisers in columnar/eval.c that
 *                  wrap a child op list -- so a new relation field is silently
 *                  dropped rather than carried.
 *
 *                  Invariant to preserve when adding a construction site:
 *                  every site must zero-initialise the stratum before
 *                  filling it, so an unrecorded @rule_refs reads as NULL and
 *                  consumers take the fallback path instead of dereferencing
 *                  garbage.  wl_plan_from_program() gets that from calloc();
 *                  the stack-declared wl_plan_stratum_t arrays in
 *                  tests/test_affected_strata.c, tests/test_affected_rules.c
 *                  and tests/test_rule_level_frontier.c (18 arrays at the
 *                  time of writing) each memset() the struct first.  This
 *                  type is deliberately NOT private to wl_plan_t: hand-built
 *                  strata passed straight to the columnar frontier helpers
 *                  are an established pattern in those three files.
 */
typedef struct {
    uint32_t stratum_id;
    bool is_recursive;
    bool is_monotone;
    const wl_plan_relation_t *relations;
    uint32_t relation_count;
    wl_plan_refset_t *rule_refs;
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
 * @strata:                Array of stratum plans ordered by execution sequence.
 *                         Stratum 0 executes first (caller-owned).
 * @stratum_count:         Number of strata.
 * @edb_relations:         Array of null-terminated EDB (input) relation names
 *                         (caller-owned array and strings).
 * @edb_count:             Number of EDB relations.
 * @edb_has_graph_column:  Parallel array (length edb_count); true when the
 *                         corresponding EDB relation has an __graph_id column.
 *                         Issue #535: RDF named-graph support.
 * @edb_graph_col_index:   Parallel array (length edb_count); column index of
 *                         __graph_id (valid only when edb_has_graph_column[i]).
 *                         Issue #535: RDF named-graph support.
 * @edb_declared_width:    Parallel array (length edb_count); the relation's
 *                         declared *physical* width, or
 *                         WL_PLAN_WIDTH_UNDECLARED when it has no `.decl`.
 *                         Issue #1038: without this the first host insert
 *                         defines the width and a later destructuring rule
 *                         can read a slot nobody wrote.
 */
/* Issue #1038: sentinel for edb_declared_width[i] when the relation carries
 * no `.decl`.  Distinct from 0, which is the honest physical width of a
 * zero-arity declaration such as `.decl p()`. */
#define WL_PLAN_WIDTH_UNDECLARED UINT32_MAX

typedef struct {
    const wl_plan_stratum_t *strata;
    uint32_t stratum_count;
    const char *const *edb_relations;
    uint32_t edb_count;
    /* Issue #535: per-EDB graph-column metadata (parallel arrays; length = edb_count).
     * edb_has_graph_column[i] == true  => edb_relations[i] has an __graph_id column.
     * edb_graph_col_index[i] == column index of __graph_id (valid only when flag true).
     * Allocated with edb_relations; freed in wl_plan_free. */
    const bool *edb_has_graph_column;
    const uint32_t *edb_graph_col_index;
    /* Issue #1038: declared physical width per EDB relation, or
     * WL_PLAN_WIDTH_UNDECLARED.  Allocated with edb_relations; freed in
     * wl_plan_free. */
    const uint32_t *edb_declared_width;
    /* Parallel per-EDB physical type arrays.  These are additive metadata
     * used by typed columnar backends; NULL preserves hand-built legacy
     * plans' int64 default. */
    const wirelog_column_type_t *const *edb_column_types;
    const uint32_t *edb_column_type_counts;
    struct wl_intern *intern; /* borrowed from program; lifetime >= plan */
} wl_plan_t;

#endif /* WL_EXEC_PLAN_H */
