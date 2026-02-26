/*
 * dd_ffi.h - wirelog DD FFI Boundary Types
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Defines FFI-safe (C ABI compatible) types for passing DD execution
 * plans across the C-to-Rust boundary.  These types mirror the internal
 * dd_plan.h structures but are flattened for safe interop: no nested
 * owned pointers, no expression trees, no internal-only types.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * The wirelog pipeline produces DD execution plans (wl_dd_plan_t) on
 * the C side.  These plans must cross into Rust where Differential
 * Dataflow executes them.  The internal plan types (dd_plan.h) contain
 * owned pointer trees (wl_ir_expr*), string arrays, and other shapes
 * that are not safe to pass across an FFI boundary.
 *
 * This header defines a parallel set of FFI-safe types:
 *
 *   Internal (dd_plan.h)         FFI (dd_ffi.h)
 *   -------------------------    ---------------------------
 *   wl_dd_op_t                   wl_ffi_op_t
 *   wl_dd_relation_plan_t        wl_ffi_relation_plan_t
 *   wl_dd_stratum_plan_t         wl_ffi_stratum_plan_t
 *   wl_dd_plan_t                 wl_ffi_plan_t
 *   wl_ir_expr (tree)            wl_ffi_expr_buffer_t (flat)
 *
 * A marshalling step (wl_dd_marshal_plan) converts the internal plan
 * to the FFI representation.  The Rust side receives a const pointer
 * to the FFI plan, reads its contents, and returns.  The C side then
 * frees the FFI plan with wl_ffi_plan_free().
 *
 * Pipeline position:
 *   Parse -> IR -> Stratify -> DD Plan -> [Marshal] -> FFI -> Rust DD
 *
 * ========================================================================
 * Memory Ownership
 * ========================================================================
 *
 * Ownership model: C allocates, C frees.  Rust borrows.
 *
 * All memory within wl_ffi_plan_t is owned by the C side.  The Rust
 * FFI entry point receives a *const* pointer and must NOT free, modify,
 * or retain any pointer from the FFI plan beyond the duration of the
 * call.  If Rust needs data after the FFI call returns, it must copy
 * the data into Rust-owned memory.
 *
 * Ownership chain:
 *   wl_ffi_plan_t                      (C allocates, C frees)
 *     -> edb_relations[]               (owned array of const char*)
 *     -> strata[]                      (owned array)
 *       -> relations[]                 (owned array)
 *         -> name                      (owned string, null-terminated)
 *         -> ops[]                     (owned array)
 *           -> relation_name           (owned string, VARIABLE only)
 *           -> right_relation          (owned string, JOIN/ANTIJOIN)
 *           -> left_keys[]             (owned string array, JOIN)
 *           -> right_keys[]            (owned string array, JOIN)
 *           -> project_indices[]       (owned array, MAP only)
 *           -> group_by_indices[]      (owned array, REDUCE only)
 *           -> filter_expr.data        (owned byte buffer, FILTER only)
 *
 * Lifecycle:
 *   1. C calls wl_dd_marshal_plan() to convert internal -> FFI plan
 *   2. C passes const wl_ffi_plan_t* to Rust via wl_dd_execute()
 *   3. Rust reads the plan, builds DD dataflow, executes, returns
 *   4. C calls wl_ffi_plan_free() to release all FFI memory
 *
 * ========================================================================
 * Filter Expression Serialization
 * ========================================================================
 *
 * The internal plan uses wl_ir_expr trees for FILTER predicates.
 * Pointer-based trees cannot cross FFI.  Instead, the marshaller
 * serializes the expression tree into a flat byte buffer using
 * postfix (Reverse Polish Notation) encoding.
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
 * Evaluation: the Rust side walks the buffer left-to-right, pushing
 * values onto an evaluation stack.  Operators pop their operands and
 * push the result.  A well-formed expression leaves exactly one value
 * on the stack.
 *
 * Example:  X > 5  encodes as:
 *   [EXPR_VAR][1][X] [EXPR_CONST_INT][5,0,0,0,0,0,0,0] [EXPR_CMP_GT]
 *
 * ========================================================================
 * Error Handling
 * ========================================================================
 *
 * wl_dd_marshal_plan() returns:
 *    0  success, *out is set to the marshalled FFI plan
 *   -1  memory allocation failure (partial state cleaned up)
 *   -2  invalid input (NULL plan pointer)
 *   -3  expression serialization error (malformed filter tree)
 *
 * wl_dd_execute() returns:
 *    0  success
 *   -1  execution error (DD runtime failure)
 *   -2  invalid plan (NULL or malformed)
 *
 * wl_dd_worker_create() returns:
 *   non-NULL  success (opaque worker handle)
 *   NULL      creation failure (invalid num_workers or resource error)
 *
 * ========================================================================
 * Usage (internal)
 * ========================================================================
 *
 *   wl_dd_plan_t *plan = NULL;
 *   int rc = wl_dd_plan_generate(program, &plan);
 *   if (rc != 0) { handle error }
 *
 *   wl_ffi_plan_t *ffi_plan = NULL;
 *   rc = wl_dd_marshal_plan(plan, &ffi_plan);
 *   if (rc != 0) { handle error }
 *
 *   wl_dd_worker_t *worker = wl_dd_worker_create(4);
 *   rc = wl_dd_execute(ffi_plan, worker);
 *
 *   wl_dd_worker_destroy(worker);
 *   wl_ffi_plan_free(ffi_plan);
 *   wl_dd_plan_free(plan);
 */

#ifndef WIRELOG_DD_FFI_H
#define WIRELOG_DD_FFI_H

#include "../wirelog-types.h"

#include <stdbool.h>
#include <stdint.h>

/*
 * dd_plan.h provides wl_dd_plan_t (the internal DD plan type).
 * Both headers are internal (not installed), so this include is safe.
 * The marshalling functions below convert wl_dd_plan_t -> wl_ffi_plan_t.
 */
#include "dd_plan.h"

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
 *   WL_FFI_EXPR_ARITH_ADD:  Addition (+)
 *   WL_FFI_EXPR_ARITH_SUB:  Subtraction (-)
 *   WL_FFI_EXPR_ARITH_MUL:  Multiplication (*)
 *   WL_FFI_EXPR_ARITH_DIV:  Division (/)
 *   WL_FFI_EXPR_ARITH_MOD:  Modulo (%)
 *   WL_FFI_EXPR_CMP_EQ:     Equal (=)
 *   WL_FFI_EXPR_CMP_NEQ:    Not equal (!=)
 *   WL_FFI_EXPR_CMP_LT:     Less than (<)
 *   WL_FFI_EXPR_CMP_GT:     Greater than (>)
 *   WL_FFI_EXPR_CMP_LTE:    Less than or equal (<=)
 *   WL_FFI_EXPR_CMP_GTE:    Greater than or equal (>=)
 *
 * Unary aggregate tags (pop 1, push 1):
 *   WL_FFI_EXPR_AGG_COUNT:  count()
 *   WL_FFI_EXPR_AGG_SUM:    sum()
 *   WL_FFI_EXPR_AGG_MIN:    min()
 *   WL_FFI_EXPR_AGG_MAX:    max()
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
 * (RPN) encoding.  Replaces the wl_ir_expr pointer tree for FFI.
 *
 * The buffer is a sequence of [tag][payload] instructions.
 * See wl_ffi_expr_tag_t for the encoding format.
 *
 * @data:  Byte buffer (owned by C side).  NULL if no expression.
 * @size:  Number of bytes in the buffer.  0 if no expression.
 */
typedef struct {
    uint8_t *data;
    uint32_t size;
} wl_ffi_expr_buffer_t;

/* ======================================================================== */
/* FFI Operator Types                                                       */
/* ======================================================================== */

/**
 * wl_ffi_op_type_t:
 *
 * FFI-safe operator type enum.  Mirrors wl_dd_op_type_t but uses
 * explicit integer values for stable ABI across C and Rust.
 *
 * WL_FFI_OP_VARIABLE:    Reference to an input collection (EDB or IDB).
 * WL_FFI_OP_MAP:         Column projection / rename.
 * WL_FFI_OP_FILTER:      Predicate filter (expr in serialized buffer).
 * WL_FFI_OP_JOIN:        Equijoin on key columns.
 * WL_FFI_OP_ANTIJOIN:    Negation (antijoin).
 * WL_FFI_OP_REDUCE:      Aggregation (group-by + aggregate function).
 * WL_FFI_OP_CONCAT:      Union of multiple collections.
 * WL_FFI_OP_CONSOLIDATE: Deduplication / consolidation.
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
} wl_ffi_op_type_t;

/* ======================================================================== */
/* FFI Operator Node                                                        */
/* ======================================================================== */

/**
 * wl_ffi_op_t:
 *
 * FFI-safe flat operator descriptor.  All pointer fields point to
 * C-owned memory.  Rust must NOT free or retain these pointers.
 *
 * This is the FFI counterpart of wl_dd_op_t.  Key differences:
 *   - filter_expr (pointer tree) is replaced by filter_expr (flat buffer)
 *   - All enum values have explicit integer backing
 *   - No internal-only types are referenced
 *
 * Field usage by operator type:
 *
 *   VARIABLE:    relation_name
 *   MAP:         project_indices, project_count
 *   FILTER:      filter_expr
 *   JOIN:        right_relation, left_keys, right_keys, key_count
 *   ANTIJOIN:    right_relation, left_keys, right_keys, key_count
 *   REDUCE:      agg_fn, group_by_indices, group_by_count
 *   CONCAT:      (no fields used)
 *   CONSOLIDATE: (no fields used)
 *
 * @op:               Operator type (explicit integer values for ABI stability).
 *
 * @relation_name:    VARIABLE: null-terminated source relation name.
 *                    Owned by C; Rust borrows.
 *
 * @right_relation:   JOIN/ANTIJOIN: null-terminated right-side relation name.
 *                    Owned by C; Rust borrows.
 *
 * @left_keys:        JOIN/ANTIJOIN: array of null-terminated left key column
 *                    names.  Array length is @key_count.
 *                    Owned by C (array and strings); Rust borrows.
 *
 * @right_keys:       JOIN/ANTIJOIN: array of null-terminated right key column
 *                    names.  Array length is @key_count.
 *                    Owned by C (array and strings); Rust borrows.
 *
 * @key_count:        JOIN/ANTIJOIN: number of join key pairs.
 *
 * @project_indices:  MAP: array of output column indices.
 *                    Array length is @project_count.
 *                    Owned by C; Rust borrows.
 *                    NULL if the projection used expressions (not yet
 *                    supported in FFI; will be extended in a future phase).
 *
 * @project_count:    MAP: number of projected columns.
 *
 * @filter_expr:      FILTER: serialized postfix expression buffer.
 *                    See wl_ffi_expr_buffer_t and wl_ffi_expr_tag_t.
 *                    Buffer data is owned by C; Rust borrows.
 *                    data=NULL and size=0 if no filter expression.
 *
 * @agg_fn:           REDUCE: aggregation function (uses wl_agg_fn_t from
 *                    wirelog-types.h, which has stable integer values).
 *
 * @group_by_indices: REDUCE: array of grouping column indices.
 *                    Array length is @group_by_count.
 *                    Owned by C; Rust borrows.
 *
 * @group_by_count:   REDUCE: number of grouping columns.
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

    wl_agg_fn_t agg_fn;
    const uint32_t *group_by_indices;
    uint32_t group_by_count;
} wl_ffi_op_t;

/* ======================================================================== */
/* FFI Per-Relation Plan                                                    */
/* ======================================================================== */

/**
 * wl_ffi_relation_plan_t:
 *
 * FFI-safe operator sequence for a single IDB relation within a stratum.
 *
 * @name:      Null-terminated relation name.
 *             Owned by C; Rust borrows.
 * @ops:       Array of FFI operator descriptors.
 *             Array length is @op_count.
 *             Owned by C; Rust borrows.
 * @op_count:  Number of operators in the sequence.
 */
typedef struct {
    const char *name;
    const wl_ffi_op_t *ops;
    uint32_t op_count;
} wl_ffi_relation_plan_t;

/* ======================================================================== */
/* FFI Stratum Plan                                                         */
/* ======================================================================== */

/**
 * wl_ffi_stratum_plan_t:
 *
 * FFI-safe execution plan for a single stratum.
 *
 * @stratum_id:     Stratum index (0 = executed first).
 * @is_recursive:   True if this stratum requires fixed-point iteration.
 *                  The Rust side should wrap the stratum's dataflow
 *                  in a DD iterate() scope when this flag is set.
 * @relations:      Array of per-relation plans.
 *                  Array length is @relation_count.
 *                  Owned by C; Rust borrows.
 * @relation_count: Number of relations in this stratum.
 */
typedef struct {
    uint32_t stratum_id;
    bool is_recursive;
    const wl_ffi_relation_plan_t *relations;
    uint32_t relation_count;
} wl_ffi_stratum_plan_t;

/* ======================================================================== */
/* FFI Full Plan                                                            */
/* ======================================================================== */

/**
 * wl_ffi_plan_t:
 *
 * FFI-safe complete DD execution plan for a stratified program.
 * This is the top-level structure passed across the C-to-Rust boundary.
 *
 * @strata:        Array of stratum plans, ordered by execution sequence.
 *                 Stratum 0 executes first.
 *                 Array length is @stratum_count.
 *                 Owned by C; Rust borrows.
 * @stratum_count: Number of strata.
 * @edb_relations: Array of null-terminated EDB (input) relation names.
 *                 Array length is @edb_count.
 *                 Owned by C (array and strings); Rust borrows.
 * @edb_count:     Number of EDB relations.
 */
typedef struct {
    const wl_ffi_stratum_plan_t *strata;
    uint32_t stratum_count;
    const char *const *edb_relations;
    uint32_t edb_count;
} wl_ffi_plan_t;

/* ======================================================================== */
/* DD Worker Handle (opaque)                                                */
/* ======================================================================== */

/**
 * wl_dd_worker_t:
 *
 * Opaque handle to a Differential Dataflow worker pool.
 * Created by Rust, passed to C as an opaque pointer.
 * C must not inspect or modify the contents; it only passes
 * the handle back to Rust entry points.
 */
typedef struct wl_dd_worker wl_dd_worker_t;

/* ======================================================================== */
/* Rust-exported FFI Entry Points                                           */
/* ======================================================================== */

/*
 * These functions are implemented in Rust and exported with C linkage.
 * The C side calls them through the FFI boundary.  All pointer arguments
 * are borrowed: Rust reads them during the call and does not retain them.
 */

/**
 * wl_dd_worker_create:
 * @num_workers: Number of DD worker threads to spawn.
 *               Must be >= 1.
 *
 * Create a Differential Dataflow worker pool with the specified number
 * of threads.  The worker pool is reusable across multiple plan
 * executions.
 *
 * Returns:
 *   non-NULL: Opaque worker handle (transfer full to C; C must call
 *             wl_dd_worker_destroy to release).
 *   NULL:     Creation failed (invalid num_workers or resource error).
 */
wl_dd_worker_t *
wl_dd_worker_create(uint32_t num_workers);

/**
 * wl_dd_worker_destroy:
 * @worker: (transfer full): Worker handle to destroy (NULL-safe).
 *
 * Destroy a DD worker pool and release all associated resources.
 * Blocks until all worker threads have shut down.
 */
void
wl_dd_worker_destroy(wl_dd_worker_t *worker);

/**
 * wl_dd_execute:
 * @plan:   (borrow): FFI plan to execute.  Must remain valid for the
 *          duration of the call.  Must not be NULL.
 * @worker: (borrow): Worker pool to execute on.  Must not be NULL.
 *
 * Execute a DD plan on the given worker pool.  The Rust side reads the
 * plan, constructs the DD dataflow graph, and runs it to completion.
 *
 * The plan pointer and all data it references must remain valid and
 * unmodified for the duration of this call.
 *
 * Returns:
 *    0: Success.
 *   -1: Execution error (DD runtime failure).
 *   -2: Invalid arguments (NULL plan or worker).
 */
int
wl_dd_execute(const wl_ffi_plan_t *plan, wl_dd_worker_t *worker);

/* ======================================================================== */
/* EDB Data Loading (Rust-side)                                             */
/* ======================================================================== */

/**
 * wl_dd_load_edb:
 * @worker:   (borrow): Worker handle.  Must not be NULL.
 * @relation: (borrow): Null-terminated EDB relation name.
 * @data:     (borrow): Flat array of int64_t values, row-major.
 *            Array length is @num_rows * @num_cols.
 *            May be NULL if @num_rows is 0.
 * @num_rows: Number of rows to load.  May be 0.
 * @num_cols: Number of columns per row.  Must be >= 1.
 *
 * Load EDB (input) data into the worker before plan execution.
 * Call once per relation, or multiple times to append rows.
 * Data must be loaded before calling wl_dd_execute or wl_dd_execute_cb.
 *
 * Returns:
 *    0: Success.
 *   -2: Invalid arguments (NULL worker/relation, or zero columns).
 */
int
wl_dd_load_edb(wl_dd_worker_t *worker, const char *relation,
               const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/* ======================================================================== */
/* Result Callback Execution (Rust-side)                                    */
/* ======================================================================== */

/**
 * wl_dd_on_tuple_fn:
 *
 * Callback invoked once per computed output tuple during execution.
 *
 * @relation:  Null-terminated output relation name.
 * @row:       Array of int64_t values (length = @ncols).
 * @ncols:     Number of columns in the row.
 * @user_data: Opaque pointer passed through from the caller.
 */
typedef void (*wl_dd_on_tuple_fn)(const char *relation, const int64_t *row,
                                  uint32_t ncols, void *user_data);

/**
 * wl_dd_execute_cb:
 * @plan:      (borrow): FFI plan to execute.  Must not be NULL.
 * @worker:    (borrow): Worker pool with EDB data loaded.  Must not be NULL.
 * @on_tuple:  Callback for result delivery.  May be NULL (results discarded).
 * @user_data: Opaque pointer passed through to @on_tuple.
 *
 * Execute a DD plan and deliver computed IDB tuples via callback.
 *
 * Returns:
 *    0: Success.
 *   -1: Execution error (DD runtime failure).
 *   -2: Invalid arguments (NULL plan or worker).
 */
int
wl_dd_execute_cb(const wl_ffi_plan_t *plan, wl_dd_worker_t *worker,
                 wl_dd_on_tuple_fn on_tuple, void *user_data);

/* ======================================================================== */
/* Marshalling: Internal Plan -> FFI Plan (C-side)                          */
/* ======================================================================== */

/*
 * These functions are implemented in C.  They convert the internal
 * dd_plan.h structures into the flat FFI-safe types defined above.
 */

/**
 * wl_dd_marshal_plan:
 * @plan: (borrow): Internal DD plan to convert.  Must not be NULL.
 * @out:  (out): On success, receives the marshalled FFI plan.
 *        The caller must free the result with wl_ffi_plan_free().
 *
 * Convert an internal DD execution plan into an FFI-safe representation.
 * All strings are copied (the FFI plan does not alias the internal plan).
 * Expression trees are serialized into flat byte buffers.
 *
 * Returns:
 *    0: Success.  *out is set to the FFI plan.
 *   -1: Memory allocation failure (partial state cleaned up).
 *   -2: Invalid input (NULL plan pointer).
 *   -3: Expression serialization error (malformed filter tree).
 *
 * On error, *out is unchanged (remains NULL if initialized to NULL).
 */
int
wl_dd_marshal_plan(const wl_dd_plan_t *plan, wl_ffi_plan_t **out);

/**
 * wl_ffi_plan_free:
 * @plan: (transfer full): FFI plan to free (NULL-safe).
 *
 * Free a marshalled FFI plan and all owned memory, including all
 * copied strings, operator arrays, expression buffers, and the
 * plan structure itself.
 */
void
wl_ffi_plan_free(wl_ffi_plan_t *plan);

/**
 * wl_ffi_expr_serialize:
 * @expr: (borrow): Expression tree to serialize.  May be NULL.
 * @out:  (out): On success, receives the serialized buffer.
 *        If @expr is NULL, out->data is set to NULL and out->size to 0.
 *
 * Serialize an IR expression tree into a flat postfix byte buffer
 * suitable for FFI transport.  The caller owns the buffer and must
 * free out->data with free() when done (or let wl_ffi_plan_free()
 * handle it as part of the plan).
 *
 * Returns:
 *    0: Success.
 *   -1: Memory allocation failure.
 *   -3: Malformed expression tree (unknown node type).
 */
int
wl_ffi_expr_serialize(const struct wl_ir_expr *expr, wl_ffi_expr_buffer_t *out);

#endif /* WIRELOG_DD_FFI_H */
