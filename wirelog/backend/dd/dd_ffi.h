/*
 * dd_ffi.h - wirelog DD Backend Declarations
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Declares flat, C ABI compatible types for DD backend execution plans.
 * These types are used by the DD execution engine and marshaling code.
 * Provides stable naming and structure definitions for the DD subsystem.
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
 *   wl_dd_op_t                   wl_plan_op_t
 *   wl_dd_relation_plan_t        wl_plan_relation_t
 *   wl_dd_stratum_plan_t         wl_plan_stratum_t
 *   wl_dd_plan_t                 wl_plan_t
 *   wl_ir_expr (tree)            wl_plan_expr_buffer_t (flat)
 *
 * A marshalling step (wl_dd_marshal_plan) converts the internal plan
 * to the FFI representation.  The Rust side receives a const pointer
 * to the FFI plan, reads its contents, and returns.  The C side then
 * frees the FFI plan with wl_plan_free().
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
 * All memory within wl_plan_t is owned by the C side.  The Rust
 * FFI entry point receives a *const* pointer and must NOT free, modify,
 * or retain any pointer from the FFI plan beyond the duration of the
 * call.  If Rust needs data after the FFI call returns, it must copy
 * the data into Rust-owned memory.
 *
 * Ownership chain:
 *   wl_plan_t                      (C allocates, C frees)
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
 *   2. C passes const wl_plan_t* to Rust via wl_dd_execute()
 *   3. Rust reads the plan, builds DD dataflow, executes, returns
 *   4. C calls wl_plan_free() to release all FFI memory
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
 *   wl_plan_t *ffi_plan = NULL;
 *   rc = wl_dd_marshal_plan(plan, &ffi_plan);
 *   if (rc != 0) { handle error }
 *
 *   wl_dd_worker_t *worker = wl_dd_worker_create(4);
 *   rc = wl_dd_execute(ffi_plan, worker);
 *
 *   wl_dd_worker_destroy(worker);
 *   wl_plan_free(ffi_plan);
 *   wl_dd_plan_free(plan);
 */

#ifndef WIRELOG_DD_FFI_H
#define WIRELOG_DD_FFI_H

/*
 * exec_plan.h provides the backend-agnostic plan types:
 *   wl_plan_expr_tag_t, wl_plan_expr_buffer_t, wl_plan_op_type_t,
 *   wl_plan_op_t, wl_plan_relation_t, wl_plan_stratum_t,
 *   wl_plan_t
 *
 * These types are shared with the columnar C11 backend (Phase 2A).
 */
#include "../../exec_plan.h"

#include <stdbool.h>
#include <stdint.h>

/*
 * dd_plan.h provides wl_dd_plan_t (the internal DD plan type).
 * Both headers are internal (not installed), so this include is safe.
 * The marshalling functions below convert wl_dd_plan_t -> wl_plan_t.
 */
#include "dd_plan.h"

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
wl_dd_execute(const wl_plan_t *plan, wl_dd_worker_t *worker);

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
wl_dd_execute_cb(const wl_plan_t *plan, wl_dd_worker_t *worker,
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
 *        The caller must free the result with wl_plan_free().
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
wl_dd_marshal_plan(const wl_dd_plan_t *plan, wl_plan_t **out);

/**
 * wl_plan_free:
 * @plan: (transfer full): FFI plan to free (NULL-safe).
 *
 * Free a marshalled FFI plan and all owned memory, including all
 * copied strings, operator arrays, expression buffers, and the
 * plan structure itself.
 */
void
wl_plan_free(wl_plan_t *plan);

/**
 * wl_plan_expr_serialize:
 * @expr: (borrow): Expression tree to serialize.  May be NULL.
 * @out:  (out): On success, receives the serialized buffer.
 *        If @expr is NULL, out->data is set to NULL and out->size to 0.
 *
 * Serialize an IR expression tree into a flat postfix byte buffer
 * suitable for FFI transport.  The caller owns the buffer and must
 * free out->data with free() when done (or let wl_plan_free()
 * handle it as part of the plan).
 *
 * Returns:
 *    0: Success.
 *   -1: Memory allocation failure.
 *   -3: Malformed expression tree (unknown node type).
 */
int
wl_plan_expr_serialize(const struct wl_ir_expr *expr,
                       wl_plan_expr_buffer_t *out);

/* ======================================================================== */
/* Persistent Session Handle (opaque)                                       */
/* ======================================================================== */

/**
 * wl_dd_persistent_session_t:
 *
 * Opaque handle to a persistent Differential Dataflow session.
 * Created by Rust, passed to C as an opaque pointer.
 *
 * A persistent session keeps the DD dataflow alive across multiple
 * step() calls, supporting insert-only incremental updates on
 * non-recursive programs.
 */
typedef struct wl_dd_persistent_session wl_dd_persistent_session_t;

/* ======================================================================== */
/* Delta Callback                                                           */
/* ======================================================================== */

/**
 * wl_dd_on_delta_fn:
 *
 * Callback invoked when a tuple is inserted or retracted in a
 * persistent session step.
 *
 * @relation:  Null-terminated output relation name.
 * @row:       Array of int64_t values (length = @ncols).
 * @ncols:     Number of columns in the row.
 * @diff:      +1 for insertion, -1 for retraction.
 * @user_data: Opaque pointer passed through from the caller.
 */
typedef void (*wl_dd_on_delta_fn)(const char *relation, const int64_t *row,
                                  uint32_t ncols, int32_t diff,
                                  void *user_data);

/* ======================================================================== */
/* Persistent Session FFI Entry Points (Rust-exported)                      */
/* ======================================================================== */

/**
 * wl_dd_session_create:
 * @plan:        (borrow): FFI plan for the dataflow.  Must not be NULL.
 * @num_workers: Number of worker threads.  Must be 1 (MVP restriction).
 * @out:         (out): On success, receives the persistent session handle.
 *
 * Create a persistent DD session with a background worker thread.
 * The session keeps the dataflow alive for incremental step() calls.
 *
 * Returns:
 *    0: Success.  *out is set to the session handle.
 *   -1: Execution error or num_workers > 1.
 *   -2: Invalid arguments (NULL pointers).
 */
int
wl_dd_session_create(const wl_plan_t *plan, uint32_t num_workers,
                     wl_dd_persistent_session_t **out);

/**
 * wl_dd_session_destroy:
 * @session: (transfer full): Session handle to destroy (NULL-safe).
 *
 * Destroy a persistent DD session and shut down its background thread.
 */
void
wl_dd_session_destroy(wl_dd_persistent_session_t *session);

/**
 * wl_dd_session_insert:
 * @session:   (borrow): Active session handle.
 * @relation:  (borrow): Null-terminated EDB relation name.
 * @data:      (borrow): Flat row-major array of int64_t values.
 * @num_rows:  Number of rows.
 * @num_cols:  Number of columns per row.
 *
 * Insert facts into the session.  Facts are buffered until the next
 * wl_dd_session_step() call.
 *
 * Returns:
 *    0: Success.
 *   -1: Insert failed.
 *   -2: Invalid arguments.
 */
int
wl_dd_session_insert(wl_dd_persistent_session_t *session, const char *relation,
                     const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wl_dd_session_step:
 * @session: (borrow): Active session handle.
 *
 * Advance the session by one epoch, processing all pending inserts
 * and firing the delta callback for new derivations.
 *
 * Returns:
 *    0: Success.
 *   -1: Step failed.
 *   -2: Invalid arguments.
 */
int
wl_dd_session_step(wl_dd_persistent_session_t *session);

/**
 * wl_dd_session_set_delta_cb:
 * @session:   (borrow): Active session handle.
 * @on_delta:  Delta callback function (NULL to clear).
 * @user_data: Opaque pointer passed through to @on_delta.
 *
 * Register a callback for receiving incremental updates on output
 * relations as the session advances via wl_dd_session_step.
 */
void
wl_dd_session_set_delta_cb(wl_dd_persistent_session_t *session,
                           wl_dd_on_delta_fn on_delta, void *user_data);

/**
 * wl_dd_session_remove:
 * @session:   (borrow): Active session handle.
 * @relation:  (borrow): Null-terminated EDB relation name.
 * @data:      (borrow): Flat row-major array of int64_t values.
 * @num_rows:  Number of rows to retract.
 * @num_cols:  Number of columns per row.
 *
 * Retract facts from the session.  Facts are buffered until the next
 * wl_dd_session_step() call.
 *
 * Returns:
 *    0: Success.
 *   -1: Remove failed.
 *   -2: Invalid arguments.
 */
int
wl_dd_session_remove(wl_dd_persistent_session_t *session, const char *relation,
                     const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wl_dd_session_snapshot:
 * @session:   (borrow): Active session handle.
 * @on_tuple:  Callback for result delivery.  May be NULL (results discarded).
 * @user_data: Opaque pointer passed through to @on_tuple.
 *
 * Emit the current complete state of all IDB output relations via callback.
 * Unlike wl_dd_session_step(), this does not advance the epoch.
 *
 * Returns:
 *    0: Success.
 *   -1: Snapshot failed.
 *   -2: Invalid arguments.
 */
int
wl_dd_session_snapshot(wl_dd_persistent_session_t *session,
                       wl_dd_on_tuple_fn on_tuple, void *user_data);

#endif /* WIRELOG_DD_FFI_H */
