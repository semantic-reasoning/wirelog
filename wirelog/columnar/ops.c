/*
 * columnar/ops.c - wirelog Columnar Backend Operator Implementations
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * All col_op_* operator functions and supporting helpers extracted from
 * backend/columnar_nanoarrow.c for modular compilation.
 */

#define _GNU_SOURCE

/* Minimum K to use parallel K-fusion dispatch.  For K below this threshold,
 * thread-dispatch + per-worker setup overhead (arena alloc, delta pool,
 * synchronization) exceeds the parallelisation benefit.
 * Measured: DDISASM K=3 is 14% slower with 8-worker parallel than sequential.
 * K < WL_KFUSION_MIN_PARALLEL_K falls back to sequential execution. */
#define WL_KFUSION_MIN_PARALLEL_K 4

/* Best-effort match-pair cache for parallel keyed diff joins.  The cache is
 * scratch memory outside the final output relation, so keep it bounded and
 * fall back to the old fill traversal when a worker reaches the cap. */
#define WL_JOIN_PAIR_CACHE_MAX_BYTES (256ULL * 1024ULL * 1024ULL)
#define WL_JOIN_PAIR_CACHE_MIN_LEFT_ROWS 100000u

#if defined(_MSC_VER)
#define WL_OPS_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define WL_OPS_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define WL_OPS_ALWAYS_INLINE inline
#endif

#include "columnar/internal.h"
#include "columnar/lftj.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/* ======================================================================== */
/* Postfix Filter Expression Evaluator                                       */
/* ======================================================================== */

/* ======================================================================== */
/* Operator Implementations                                                  */
/* ======================================================================== */

/* Cross-module function declarations are in columnar/internal.h */

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    bool active;
} wl_ops_scratch_t;

static int
wl_ops_scratch_reserve(wl_ops_scratch_t *scratch,
    wl_columnar_memory_governor_ref_t *governor, uint64_t bytes,
    wl_col_session_t *sess);
static void
wl_ops_scratch_release(wl_ops_scratch_t *scratch);

/* --- VARIABLE ------------------------------------------------------------ */

/* Empty rule results still own names, schema and initial column storage.
 * The pool/heap checked constructor retains the existing timestamp policy. */
static int
wl_columnar_ops_push_empty(eval_stack_t *stack, wl_col_session_t *sess,
    const col_rel_t *like, const char *name, bool is_delta)
{
    col_rel_t *empty = NULL;
    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : like->memory_governor;
    int rc = wl_columnar_relation_pool_new_like_governed_checked(&empty,
            sess->delta_pool, name, like, governor);
    if (rc != 0) {
        if (rc == ENOSPC)
            sess->memory_budget_denied = true;
        return rc;
    }
    rc = eval_stack_push_delta(stack, empty, true, is_delta);
    if (rc != 0)
        col_rel_destroy(empty);
    return rc;
}

int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (sess->tdd_input_run) {
        col_rel_t *bound = NULL;
        bool delta = false;
        int rc = wl_columnar_eval_tdd_plan_resolve_input(sess, op, false,
                &bound, &delta);
        return rc ? rc : eval_stack_push_delta(stack, bound, false, delta);
    }
    if (!op->relation_name)
        return ENOENT;
    col_rel_t *full_rel = session_find_rel(sess, op->relation_name);
    if (!full_rel)
        return ENOENT;

    /* Delta mode controls whether we use delta or full relation.
     * FORCE_FULL:  always use the full relation (no delta substitution).
     * FORCE_DELTA: always use the delta relation; if no delta exists or
     *              it is empty, push an empty relation so that the rule
     *              copy produces no output (correct semi-naive behavior).
     * AUTO:        heuristic -- prefer delta only when it is a genuine
     *              strict subset of the full relation (nrows < full).
     *
     * Issue #158 extension: When retraction_seeded and iteration == 0,
     * look for $r$<name> (retraction delta) instead of $d$<name> */
    char dname[256];
    col_rel_t *delta = NULL;

    if (sess->retraction_seeded && sess->current_iteration == 0
        && !sess->retraction_right_pass) {
        /* Retraction mode (left pass): look for $r$<name> retraction delta.
         * Issue #472: Skip during right pass — VARIABLE loads full relation
         * so JOIN/SEMIJOIN can use $r$ on the right side instead. */
        if (retraction_rel_name(op->relation_name, dname, sizeof(dname)) == 0)
            delta = session_find_rel(sess, dname);
    } else {
        /* Normal mode: look for $d$<name> insertion delta */
        snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
        delta = session_find_rel(sess, dname);
    }

    if (op->delta_mode == WL_DELTA_FORCE_EMPTY
        || (op->delta_mode == WL_DELTA_FORCE_EMPTY_AFTER_SEED
        && sess->tdd_outbound_only_active
        && sess->current_iteration > 0)) {
        /* Issue #370: segment has no FORCE_DELTA — push empty to skip. */
        return wl_columnar_ops_push_empty(stack, sess, full_rel,
                   "$empty_skip", false);
    }
    if (op->delta_mode == WL_DELTA_FORCE_FULL) {
        return eval_stack_push_delta(stack, full_rel, false, false);
    }
    if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows > 0) {
            return eval_stack_push_delta(stack, delta, false, true);
        }
        if (sess->current_iteration == 0) {
            if (sess->delta_seeded || sess->retraction_seeded) {
                /* Issue #83 (delta-seeded) or #158 (retraction-seeded):
                 * No pre-seeded delta means this relation has no new/removed facts.
                 * Push empty so only rules with actual deltas produce output. */
                return wl_columnar_ops_push_empty(stack, sess, full_rel,
                           "$empty_delta", true);
            }
            /* Base-case iteration: no deltas exist yet, fall back to full
             * relation so EDB-grounded rules can still fire on iter 0. */
            return eval_stack_push_delta(stack, full_rel, false, false);
        }
        /* Iteration > 0: delta absent or empty means the relation has
         * converged.  Push an empty relation so this rule copy produces
         * no output (correct semi-naive semantics, issue #85). */
        return wl_columnar_ops_push_empty(stack, sess, full_rel,
                   "$empty_delta", true);
    }

    /* WL_DELTA_AUTO: use delta if strictly smaller than full relation.
     * Exception: inside a TDD worker sub-pass the broadcast $d$<rel> may be
     * >= the local partition, so we must use it whenever it is non-empty. */
    bool use_delta = delta && (((delta->nrows > 0
        && delta->nrows < full_rel->nrows) || (delta->nrows > 0
        && sess->tdd_subpass_active)) || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0));
    col_rel_t *rel = use_delta ? delta : full_rel;
    /* push borrowed reference - session owns the relation */
    return eval_stack_push_delta(stack, rel, false, use_delta);
}

/* --- MAP ----------------------------------------------------------------- */

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_ops_test_before_map_dispose)(eval_stack_t *,
    eval_entry_t *);
bool wl_columnar_ops_test_map_fail_output_alloc;
void (*wl_columnar_ops_test_before_reduce_dispose)(eval_stack_t *,
    eval_entry_t *);
bool wl_columnar_ops_test_reduce_fail_output_alloc;
#endif

/* Preserve the complete input entry when cleanup is refused. */
static int
wl_columnar_ops_dispose_entry(eval_stack_t *stack, eval_entry_t *entry,
    int result)
{
    int rc = eval_stack_dispose_entry(stack, entry);
    return rc != 0 ? rc : result;
}

/* Callers may restore only before effectful evaluation, or after proving
 * evaluation was pure. Ordinary failures retain checked-disposal precedence. */
static int
wl_columnar_ops_admission_failure(eval_stack_t *stack, eval_entry_t *entry,
    wl_col_session_t *sess, int rc)
{
    if (rc == ENOSPC) {
        sess->memory_budget_denied = true;
        int restore_rc = eval_stack_repush_entry(stack, entry);
        return restore_rc != 0 ? restore_rc : rc;
    }
    return wl_columnar_ops_dispose_entry(stack, entry, rc);
}

/* Relation-only operators must reject and destroy a pending continuation. */
static int
wl_columnar_ops_reject_null_session(eval_stack_t *stack)
{
    if (stack->top == 0 || stack->top > COL_STACK_MAX
        || stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
        return EINVAL;
    eval_entry_t discarded;
    return eval_stack_pop_relation(stack, &discarded);
}

/* Complete schema only: an interpreter-only lane retains the legacy whole
 * untyped output. Resolve without running any expression/callback, then install
 * on the still-empty private output using normal governed metadata ownership.
 * The temporary native vector is covered by the caller's scratch admission. */
static int
wl_columnar_ops_map_set_types(const wl_plan_op_t *op, const col_rel_t *input,
    col_rel_t *out, wl_columnar_expr_compiled_t *const *programs,
    uint32_t program_count)
{
    uint32_t count = op->project_count;
    if (!count) return 0;
    wirelog_column_type_t *types = malloc((size_t)count * sizeof(*types));
    if (!types) return ENOMEM;
    bool known = true;
    for (uint32_t c = 0; c < count; ++c) {
        if (op->map_exprs && c < op->map_expr_count &&
            op->map_exprs[c].data && op->map_exprs[c].size) {
            known = c < program_count && programs &&
                wl_columnar_expr_compiled_result_type(programs[c],
                    input->column_types, input->ncols, &types[c]);
        } else {
            uint32_t column = op->project_indices ? op->project_indices[c] : c;
            types[c] = input->column_types && column < input->ncols
                ? input->column_types[column] : WIRELOG_TYPE_INT64;
            known = types[c] >= WIRELOG_TYPE_INT32 &&
                types[c] <= WIRELOG_TYPE_BOOL;
        }
        if (!known) break;
    }
    int rc = known ? col_rel_set_column_types(out, types, count) : 0;
    free(types);
    return rc;
}

int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    if (!op || !stack)
        return EINVAL;
    if (!sess)
        return wl_columnar_ops_reject_null_session(stack);
    eval_entry_t e;
    int pop_rc = eval_stack_pop_relation(stack, &e);
    if (pop_rc != 0)
        return pop_rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_ops_test_before_map_dispose)
        wl_columnar_ops_test_before_map_dispose(stack, &e);
#endif

    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : e.rel->memory_governor;
    uint32_t pc = op->project_count;
    uint64_t map_scratch_bytes = 0;
    uint64_t map_bytes = 0;
    bool map_sized = wl_columnar_memory_size_mul(pc ? pc : 1, sizeof(int64_t),
            &map_bytes)
        && wl_columnar_memory_size_add(map_scratch_bytes, map_bytes,
            &map_scratch_bytes);
    map_sized = map_sized
        && wl_columnar_memory_size_mul(pc, sizeof(wirelog_column_type_t),
            &map_bytes)
        && wl_columnar_memory_size_add(map_scratch_bytes, map_bytes,
            &map_scratch_bytes);
    if (op->map_exprs && op->map_expr_count > 0)
        map_sized = map_sized
            && wl_columnar_memory_size_mul(pc,
                sizeof(wl_columnar_expr_compiled_t *), &map_bytes)
            && wl_columnar_memory_size_add(map_scratch_bytes, map_bytes,
                &map_scratch_bytes);
    if (e.rel->ncols > COL_STACK_MAX)
        map_sized = map_sized
            && wl_columnar_memory_size_mul(e.rel->ncols, sizeof(int64_t),
                &map_bytes)
            && wl_columnar_memory_size_add(map_scratch_bytes, map_bytes,
                &map_scratch_bytes);
    if (!map_sized || map_scratch_bytes > SIZE_MAX)
        return wl_columnar_ops_dispose_entry(stack, &e, EOVERFLOW);
    wl_ops_scratch_t map_scratch;
    int map_scratch_rc = wl_ops_scratch_reserve(&map_scratch,
            governor, map_scratch_bytes, sess);
    if (map_scratch_rc != 0)
        return wl_columnar_ops_admission_failure(stack, &e, sess,
                   map_scratch_rc);
#define WL_MAP_RELEASE_SCRATCH() wl_ops_scratch_release(&map_scratch)
    col_rel_t *out = NULL;
    int output_rc = ENOMEM;
#ifdef WL_SESSION_TEST_HOOKS
    if (!wl_columnar_ops_test_map_fail_output_alloc)
#endif
    {
        /* MAP emits exactly one row per input. Admit the complete payload
         * before invoking any extension callback; no later growth is needed.
         * Governed outputs own their heap storage independently of pool epochs. */
        if (governor) {
            output_rc = wl_columnar_relation_new_auto_governed("$map", pc,
                    e.rel->nrows, false, governor, &out);
        } else {
            out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
                    "$map", pc);
            output_rc = out ? 0 : ENOMEM;
        }
    }
    if (output_rc != 0) {
        WL_MAP_RELEASE_SCRATCH();
        return wl_columnar_ops_admission_failure(stack, &e, sess,
                   output_rc);
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (pc ? pc : 1));
    if (!tmp) {
        WL_MAP_RELEASE_SCRATCH();
        col_rel_destroy(out);
        return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
    }

    /* Pre-compile map expressions once to avoid per-row strtol. */
    wl_columnar_expr_compiled_t **ce_map = NULL;
    uint32_t ce_map_count = 0;
    if (op->map_exprs && op->map_expr_count > 0) {
        ce_map = (wl_columnar_expr_compiled_t **)calloc(pc,
                sizeof(wl_columnar_expr_compiled_t *));
        if (!ce_map && pc > 0) {
            free(tmp);
            col_rel_destroy(out);
            WL_MAP_RELEASE_SCRATCH();
            return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
        }
        ce_map_count = (op->map_expr_count < pc) ? op->map_expr_count : pc;
        for (uint32_t c = 0; c < ce_map_count; c++) {
            if (op->map_exprs[c].data && op->map_exprs[c].size > 0) {
                int expr_rc = ENOTSUP;
                ce_map[c] = wl_columnar_expr_compile_governed(
                    op->map_exprs[c].data, op->map_exprs[c].size,
                    sess ? sess->intern : NULL,
                    governor, sess, &expr_rc);
                if (expr_rc != ENOTSUP && expr_rc != 0) {
                    for (uint32_t j = 0; j < c; j++)
                        wl_columnar_expr_compiled_free(ce_map[j]);
                    free((void *)ce_map);
                    free(tmp);
                    col_rel_destroy(out);
                    WL_MAP_RELEASE_SCRATCH();
                    return wl_columnar_ops_admission_failure(stack, &e,
                               sess,
                               expr_rc);
                }
            }
        }
    }

    int schema_rc = wl_columnar_ops_map_set_types(op, e.rel, out, ce_map,
            ce_map_count);
    if (schema_rc) {
        for (uint32_t c = 0; c < ce_map_count; ++c)
            wl_columnar_expr_compiled_free(ce_map[c]);
        free((void *)ce_map);
        free(tmp);
        col_rel_destroy(out);
        WL_MAP_RELEASE_SCRATCH();
        return wl_columnar_ops_admission_failure(stack, &e, sess, schema_rc);
    }

    /* Row scratch, hoisted out of the loop: initialising it per row would
     * malloc once per row for relations wider than COL_STACK_MAX (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, e.rel->ncols)) {
        if (ce_map) {
            for (uint32_t c = 0; c < ce_map_count; c++)
                wl_columnar_expr_compiled_free(ce_map[c]);
            free((void *)ce_map);
        }
        free(tmp);
        col_rel_destroy(out);
        WL_MAP_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
    }

    int64_t *const row = row_rb.ptr;
    wl_columnar_expr_context_t expr_ctx = {
        .intern = sess ? sess->intern : NULL,
        .extensions = sess ? sess->base.extension_snapshot : NULL,
        .allow_extension_scalar_result = true,
        .configured_worker_count = sess ? sess->callback_configured_workers : 1,
        .active_worker_count = sess ? sess->callback_active_workers : 1,
        .parallel_execution = sess ? sess->callback_parallel_execution : false,
        .session_key = sess ? sess->callback_session_key : NULL
    };
    /* Issue #2122: append under one batch lease rather than one mutation
     * set per row.  Every exit below ends the batch before destroying out. */
    col_rel_append_batch_t batch = { 0 };
    int batch_rc = col_rel_append_batch_begin(&batch, out);
    if (batch_rc != 0) {
        if (ce_map) {
            for (uint32_t c = 0; c < ce_map_count; c++)
                wl_columnar_expr_compiled_free(ce_map[c]);
            free((void *)ce_map);
        }
        col_row_buf_release(&row_rb);
        free(tmp);
        col_rel_destroy(out);
        WL_MAP_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, batch_rc);
    }
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        col_rel_row_copy_out(e.rel, r, row);
        for (uint32_t c = 0; c < pc; c++) {
            if (op->map_exprs && c < op->map_expr_count && op->map_exprs[c].data
                && op->map_exprs[c].size > 0) {
                if (ce_map && c < ce_map_count && ce_map[c]) {
                    int64_t val = 0;
                    if (wl_columnar_expr_eval_compiled(ce_map[c], row,
                        e.rel->ncols,
                        &val) != 0) {
                        (void)col_rel_append_batch_end(&batch, false);
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                wl_columnar_expr_compiled_free(ce_map[i]);
                            free((void *)ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        WL_MAP_RELEASE_SCRATCH();
                        return wl_columnar_ops_dispose_entry(stack, &e,
                                   ERANGE);
                    }
                    tmp[c] = val;
                } else {
                    int64_t val = 0;
                    wl_columnar_expr_status_t expr_status
                        = WL_COLUMNAR_EXPR_OK;
                    if (wl_columnar_expr_eval_run_ctx(
                            op->map_exprs[c].data, op->map_exprs[c].size, row,
                            e.rel->ncols, &val, &expr_ctx, &expr_status) != 0) {
                        (void)col_rel_append_batch_end(&batch, false);
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                wl_columnar_expr_compiled_free(ce_map[i]);
                            free((void *)ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (sess && expr_status
                            >= WL_COLUMNAR_EXPR_EXTENSION_MALFORMED
                            && expr_status
                            != WL_COLUMNAR_EXPR_ALLOCATION_FAILURE)
                            sess->extension_expr_status = expr_status;
                        /* A refused string allocation fails the step as a
                         * memory error (Issue #1470); the partial output
                         * was destroyed above. */
                        if (expr_status
                            == WL_COLUMNAR_EXPR_ALLOCATION_FAILURE) {
                            WL_MAP_RELEASE_SCRATCH();
                            return wl_columnar_ops_dispose_entry(stack, &e,
                                       ENOMEM);
                        }
                        WL_MAP_RELEASE_SCRATCH();
                        return wl_columnar_ops_dispose_entry(stack, &e,
                                   expr_status >=
                                   WL_COLUMNAR_EXPR_EXTENSION_MALFORMED
                            ? expr_status : ERANGE);
                    }
                    tmp[c] = val;
                }
            } else {
                uint32_t src = op->project_indices ? op->project_indices[c] : c;
                tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
            }
        }
        int rc = col_rel_append_batch_row(&batch, tmp);
        if (rc != 0) {
            (void)col_rel_append_batch_end(&batch, false);
            if (ce_map) {
                for (uint32_t c = 0; c < ce_map_count; c++)
                    wl_columnar_expr_compiled_free(ce_map[c]);
                free((void *)ce_map);
            }
            col_row_buf_release(&row_rb);
            free(tmp);
            col_rel_destroy(out);
            WL_MAP_RELEASE_SCRATCH();
            return wl_columnar_ops_dispose_entry(stack, &e, rc);
        }
    }
    batch_rc = col_rel_append_batch_end(&batch, true);

    if (ce_map) {
        for (uint32_t c = 0; c < ce_map_count; c++)
            wl_columnar_expr_compiled_free(ce_map[c]);
        free((void *)ce_map);
    }
    col_row_buf_release(&row_rb);
    free(tmp);
    WL_MAP_RELEASE_SCRATCH();

    if (batch_rc != 0) {
        col_rel_destroy(out);
        return wl_columnar_ops_dispose_entry(stack, &e, batch_rc);
    }
    int input_rc = wl_columnar_ops_dispose_entry(stack, &e, 0);
    if (input_rc != 0) {
        col_rel_destroy(out);
        return input_rc;
    }
    int push_rc = eval_stack_push(stack, out, true);
    if (push_rc != 0)
        col_rel_destroy(out);
#undef WL_MAP_RELEASE_SCRATCH
    return push_rc;
}

/* --- CONCAT and CONSOLIDATE are implemented in columnar/merge.c. */
/* --- K-FUSION is implemented in columnar/kfusion.c. */
/* --- REDUCE (aggregate) -------------------------------------------------- */

typedef struct {
    uint64_t hash;
    uint32_t row;
} reduce_group_slot_t;

static int
wl_ops_scratch_reserve(wl_ops_scratch_t *scratch,
    wl_columnar_memory_governor_ref_t *governor, uint64_t bytes,
    wl_col_session_t *sess)
{
    wl_columnar_memory_admission_status_t status;
    if (!scratch)
        return EINVAL;
    memset(scratch, 0, sizeof(*scratch));
    wl_columnar_memory_reservation_init(&scratch->reservation);
    if (!governor || bytes == 0)
        return 0;
    status = wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(governor), bytes,
        &scratch->reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        scratch->active = true;
        return 0;
    }
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
        sess->memory_budget_denied = true;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
        : ENOMEM;
}

static void
wl_ops_scratch_release(wl_ops_scratch_t *scratch)
{
    if (scratch && scratch->active) {
        (void)wl_columnar_memory_rollback(&scratch->reservation);
        scratch->active = false;
    }
}

int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op || !stack)
        return EINVAL;
    if (!sess)
        return wl_columnar_ops_reject_null_session(stack);
    eval_entry_t e;
    int pop_rc = eval_stack_pop_relation(stack, &e);
    if (pop_rc != 0)
        return pop_rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_ops_test_before_reduce_dispose)
        wl_columnar_ops_test_before_reduce_dispose(stack, &e);
#endif

    col_rel_t *in = e.rel;
    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : in->memory_governor;
    uint32_t gc = op->group_by_count;
    if (gc == UINT32_MAX)
        return wl_columnar_ops_dispose_entry(stack, &e, EOVERFLOW);

    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)(in->nrows ? in->nrows : 1) * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired)
        return wl_columnar_ops_dispose_entry(stack, &e, EOVERFLOW);
    uint64_t scratch_bytes = 0;
    uint64_t bytes = 0;
    uint32_t ocols = gc + 1;
    bool sized = wl_columnar_memory_size_mul(ocols,
            sizeof(wirelog_column_type_t), &bytes)
        && wl_columnar_memory_size_add(scratch_bytes, bytes,
            &scratch_bytes)
        && wl_columnar_memory_size_mul(ocols ? ocols : 1,
            sizeof(int64_t), &bytes)
        && wl_columnar_memory_size_add(scratch_bytes, bytes,
            &scratch_bytes)
        && wl_columnar_memory_size_mul(map_cap,
            sizeof(reduce_group_slot_t), &bytes)
        && wl_columnar_memory_size_add(scratch_bytes, bytes,
            &scratch_bytes)
        && wl_columnar_memory_size_mul(map_cap, sizeof(double), &bytes)
        && wl_columnar_memory_size_add(scratch_bytes, bytes,
            &scratch_bytes)
        && wl_columnar_memory_size_mul(map_cap, sizeof(uint64_t), &bytes)
        && wl_columnar_memory_size_add(scratch_bytes, bytes,
            &scratch_bytes);
    if (in->ncols > COL_STACK_MAX)
        sized = sized
            && wl_columnar_memory_size_mul(in->ncols, sizeof(int64_t),
                &bytes)
            && wl_columnar_memory_size_add(scratch_bytes, bytes,
                &scratch_bytes);
    if (!sized)
        return wl_columnar_ops_dispose_entry(stack, &e, EOVERFLOW);
    wl_ops_scratch_t scratch;
    int scratch_rc = wl_ops_scratch_reserve(&scratch,
            governor, scratch_bytes, sess);
    if (scratch_rc != 0)
        return wl_columnar_ops_admission_failure(stack, &e, sess, scratch_rc);
#define WL_REDUCE_RELEASE_SCRATCH() wl_ops_scratch_release(&scratch)

    /* Output: the group columns and aggregate retain the rule-head order. */
    uint32_t agg_index = op->aggregate_index < ocols
        ? op->aggregate_index : gc;
    col_rel_t *out = NULL;
    int output_rc = ENOMEM;
#ifdef WL_SESSION_TEST_HOOKS
    if (!wl_columnar_ops_test_reduce_fail_output_alloc)
#endif
    {
        if (governor) {
            uint32_t capacity = in->nrows < COL_REL_INIT_CAP
                ? in->nrows : COL_REL_INIT_CAP;
            output_rc = wl_columnar_relation_new_auto_governed("$reduce",
                    ocols, capacity, false, governor, &out);
        } else {
            out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
                    "$reduce", ocols);
            output_rc = out ? 0 : ENOMEM;
        }
    }
    if (output_rc != 0) {
        WL_REDUCE_RELEASE_SCRATCH();
        return wl_columnar_ops_admission_failure(stack, &e, sess, output_rc);
    }

    /* COUNT's operand is ignored; cardinalities are always integer lanes. */
    bool float_agg = op->agg_fn != WIRELOG_AGG_COUNT
        && op->agg_operand_type == WL_PLAN_AGG_OPERAND_FLOAT;
    if (ocols > 0) {
        wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
            (size_t)ocols * sizeof(*types));
        if (!types) {
            col_rel_destroy(out);
            WL_REDUCE_RELEASE_SCRATCH();
            return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
        }
        for (uint32_t c = 0; c < gc; c++) {
            uint32_t src = op->group_by_indices ? op->group_by_indices[c] : c;
            uint32_t out_col = c >= agg_index ? c + 1 : c;
            types[out_col] = (in->column_types && src < in->ncols)
                ? in->column_types[src] : WIRELOG_TYPE_INT64;
        }
        types[agg_index] = float_agg ? WIRELOG_TYPE_FLOAT
                                     : WIRELOG_TYPE_INT64;
        int type_rc = col_rel_set_column_types(out, types, ocols);
        free(types);
        if (type_rc != 0) {
            col_rel_destroy(out);
            WL_REDUCE_RELEASE_SCRATCH();
            return wl_columnar_ops_admission_failure(stack, &e, sess, type_rc);
        }
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        WL_REDUCE_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
    }

    wl_columnar_expr_compiled_t *agg_ce = NULL;
    if (op->agg_expr.data && op->agg_expr.size > 0) {
        int expr_rc = ENOTSUP;
        agg_ce = wl_columnar_expr_compile_governed(op->agg_expr.data,
                op->agg_expr.size, sess ? sess->intern : NULL,
                governor, sess, &expr_rc);
        if (expr_rc != ENOTSUP && expr_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            WL_REDUCE_RELEASE_SCRATCH();
            return wl_columnar_ops_admission_failure(stack, &e, sess, expr_rc);
        }
    }

    /* Row scratch, hoisted out of the loop (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, in->ncols)) {
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        WL_REDUCE_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
    }

    reduce_group_slot_t *groups = (reduce_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups) {
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        WL_REDUCE_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
    }
    double *sums = (double *)calloc(map_cap, sizeof(*sums));
    uint64_t *counts = (uint64_t *)calloc(map_cap, sizeof(*counts));
    if (!sums || !counts) {
        free(sums);
        free(counts);
        free(groups);
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        WL_REDUCE_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, ENOMEM);
    }
    /* Interpreted aggregates can intern strings. Do not replay those effects
     * after a late refusal; COUNT never evaluates its aggregate expression. */
    bool retry_safe = op->agg_fn == WIRELOG_AGG_COUNT
        || !op->agg_expr.data || op->agg_expr.size == 0 || agg_ce != NULL;
    uint32_t map_mask = map_cap - 1;
    int set_rc = 0;
    /* One held lease covers the group appends and the aggregate updates
     * (#2072).  Every exit below that destroys out ends it first. */
    col_rel_append_batch_t agg_batch = { 0 };
    int begin_rc = col_rel_append_batch_begin(&agg_batch, out);
    if (begin_rc != 0) {
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(sums);
        free(counts);
        free(groups);
        free(tmp);
        col_rel_destroy(out);
        WL_REDUCE_RELEASE_SCRATCH();
        return wl_columnar_ops_dispose_entry(stack, &e, begin_rc);
    }

    /* Index groups by their key so reduction remains linear in the number of
     * input rows rather than scanning every output group. */
    int64_t *const row = row_rb.ptr;
    for (uint32_t r = 0; r < in->nrows; r++) {
        col_rel_row_copy_out(in, r, row);
        int64_t agg_val = (in->ncols > gc) ? row[gc] : 1;
        if (op->agg_fn != WIRELOG_AGG_COUNT
            && op->agg_expr.data && op->agg_expr.size > 0) {
            if (agg_ce) {
                int64_t val = 0;
                if (wl_columnar_expr_eval_compiled(agg_ce, row, in->ncols,
                    &val) == 0)
                    agg_val = val;
                else {
                    set_rc = ERANGE;
                    goto set_failure;
                }
            } else {
                int64_t val = 0;
                if (wl_columnar_expr_eval_i64(op->agg_expr.data,
                    op->agg_expr.size,
                    row, in->ncols, &val, sess->intern) == 0) {
                    agg_val = val;
                } else {
                    set_rc = ERANGE;
                    goto set_failure;
                }
            }
        }
        if (float_agg && !wl_columnar_float_bits_valid(agg_val)) {
            set_rc = EINVAL;
            goto set_failure;
        }

        /* Use an open-addressed key index instead of scanning all output
         * groups for every input row. */
        uint64_t hash = UINT64_C(1469598103934665603);
        for (uint32_t k = 0; k < gc; k++) {
            uint32_t gi = op->group_by_indices ? op->group_by_indices[k] : k;
            int64_t key = row[gi < in->ncols ? gi : 0];
            if (in->column_types && gi < in->ncols
                && in->column_types[gi] == WIRELOG_TYPE_FLOAT)
                key = (int64_t)wl_columnar_float_canonical_bits(key);
            hash ^= (uint64_t)key;
            hash *= UINT64_C(1099511628211);
        }
        if (!hash)
            hash = 1;
        uint32_t slot = (uint32_t)hash & map_mask;
        bool found = false;
        uint32_t group_row = UINT32_MAX;
        while (groups[slot].hash != 0) {
            bool match = groups[slot].hash == hash;
            for (uint32_t k = 0; k < gc && match; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                uint32_t out_col = k >= agg_index ? k + 1 : k;
                int64_t left = row[gi < in->ncols ? gi : 0];
                int64_t right = col_rel_get(out, groups[slot].row, out_col);
                if (in->column_types && gi < in->ncols
                    && in->column_types[gi] == WIRELOG_TYPE_FLOAT)
                    left = (int64_t)wl_columnar_float_canonical_bits(left);
                match = left == right;
            }
            if (match) {
                found = true;
                group_row = groups[slot].row;
                break;
            }
            slot = (slot + 1) & map_mask;
        }
        if (found) {
            /* Update aggregate */
            int64_t cur = col_rel_get(out, group_row, agg_index);
            switch (op->agg_fn) {
            case WIRELOG_AGG_COUNT:
                set_rc = col_rel_append_batch_set(&agg_batch, group_row,
                        agg_index, cur + 1);
                if (set_rc != 0)
                    goto set_failure;
                break;
            case WIRELOG_AGG_SUM:
            {
                if (float_agg) {
                    double value = wl_columnar_float_from_bits(agg_val);
                    double next_value = sums[slot] + value;
                    if (!isfinite(next_value)) {
                        set_rc = ERANGE;
                        goto set_failure;
                    }
                    sums[slot] = next_value;
                    set_rc = col_rel_append_batch_set(&agg_batch, group_row,
                            agg_index,
                            (int64_t)wl_columnar_float_canonical_bits(
                                wl_columnar_float_to_bits(next_value)));
                    if (set_rc != 0)
                        goto set_failure;
                    break;
                }
                int64_t next;
                if (wl_columnar_arithmetic_checked_add_int64(cur, agg_val,
                    &next) != 0) {
                    set_rc = ERANGE;
                    goto set_failure;
                }
                set_rc = col_rel_append_batch_set(&agg_batch, group_row,
                        agg_index, next);
                if (set_rc != 0)
                    goto set_failure;
            }
            break;
            case WIRELOG_AGG_MIN:
            case WIRELOG_AGG_MAX:
                /* Ordered by the operand's declared domain, not by the
                 * raw int64 -- for a symbol column that int64 is an
                 * intern id (Issue #965). */
                if (float_agg) {
                    int cmp = wl_columnar_float_compare_bits(agg_val, cur);
                    if ((op->agg_fn == WIRELOG_AGG_MIN && cmp < 0)
                        || (op->agg_fn == WIRELOG_AGG_MAX && cmp > 0))
                        set_rc = col_rel_append_batch_set(&agg_batch,
                                group_row, agg_index,
                                (int64_t)wl_columnar_float_canonical_bits(
                                    agg_val));
                    if (set_rc != 0)
                        goto set_failure;
                } else if (col_agg_better(op->agg_fn, op->agg_operand_type,
                    sess->intern, agg_val, cur)) {
                    set_rc = col_rel_append_batch_set(&agg_batch, group_row,
                            agg_index, agg_val);
                    if (set_rc != 0)
                        goto set_failure;
                }
                break;
            case WIRELOG_AGG_AVG:
                if (!float_agg)
                    break;
                sums[slot] += wl_columnar_float_from_bits(agg_val);
                counts[slot]++;
                if (!isfinite(sums[slot])) {
                    set_rc = ERANGE;
                    goto set_failure;
                }
                set_rc = col_rel_append_batch_set(&agg_batch, group_row,
                        agg_index,
                        (int64_t)wl_columnar_float_canonical_bits(
                            wl_columnar_float_to_bits(sums[slot]
                            / (double)counts[slot])));
                if (set_rc != 0)
                    goto set_failure;
                break;
            default:
                break;
            }
        }
        if (!found) {
            for (uint32_t k = 0; k < gc; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                uint32_t out_col = k >= agg_index ? k + 1 : k;
                tmp[out_col] = row[gi < in->ncols ? gi : 0];
            }
            tmp[agg_index] = (op->agg_fn == WIRELOG_AGG_COUNT) ? 1 : agg_val;
            if (float_agg) {
                sums[slot] = wl_columnar_float_from_bits(agg_val);
                counts[slot] = 1;
                if (op->agg_fn == WIRELOG_AGG_AVG)
                    tmp[agg_index] = (int64_t)wl_columnar_float_to_bits(
                        sums[slot]);
                else
                    tmp[agg_index] = (int64_t)
                        wl_columnar_float_canonical_bits(agg_val);
            }
            int rc = col_rel_append_batch_row(&agg_batch, tmp);
            if (rc != 0) {
                if (rc == ENOMEM && out->memory_budget_denial_pending)
                    rc = ENOSPC;
                if (rc == ENOSPC)
                    sess->memory_budget_denied = true;
                (void)col_rel_append_batch_end(&agg_batch, false);
                col_row_buf_release(&row_rb);
                wl_columnar_expr_compiled_free(agg_ce);
                free(sums);
                free(counts);
                free(groups);
                free(tmp);
                col_rel_destroy(out);
                WL_REDUCE_RELEASE_SCRATCH();
                if (retry_safe)
                    return wl_columnar_ops_admission_failure(stack, &e,
                               sess, rc);
                return wl_columnar_ops_dispose_entry(stack, &e, rc);
            }
            groups[slot].hash = hash;
            groups[slot].row = out->nrows - 1;
        }
    }
    set_rc = col_rel_append_batch_end(&agg_batch, true);
    if (set_rc != 0)
        goto set_failure;

    col_row_buf_release(&row_rb);
    wl_columnar_expr_compiled_free(agg_ce);
    free(groups);
    free(sums);
    free(counts);
    free(tmp);
    WL_REDUCE_RELEASE_SCRATCH();
    int input_rc = wl_columnar_ops_dispose_entry(stack, &e, 0);
    if (input_rc != 0) {
        col_rel_destroy(out);
        return input_rc;
    }
    int push_rc = eval_stack_push(stack, out, true);
    if (push_rc != 0)
        col_rel_destroy(out);
    return push_rc;

set_failure:
    (void)col_rel_append_batch_end(&agg_batch, false);
    col_row_buf_release(&row_rb);
    wl_columnar_expr_compiled_free(agg_ce);
    free(groups);
    free(sums);
    free(counts);
    free(tmp);
    col_rel_destroy(out);
    WL_REDUCE_RELEASE_SCRATCH();
    return wl_columnar_ops_dispose_entry(stack, &e, set_rc);
}
#undef WL_REDUCE_RELEASE_SCRATCH

/* --- REDUCE WEIGHTED (Z-set / Mobius COUNT) ------------------------------ */

/*
 * col_op_reduce_weighted:
 * Global COUNT aggregation using Z-set (signed multiplicity) semantics.
 * Output: one row whose data value = sum of input multiplicities, and whose
 * timestamp.multiplicity = the same sum.
 *
 * src - input relation; src->timestamps[i].multiplicity carries each row's
 *       signed weight.
 * dst - output relation (caller-allocated, empty on entry, ncols >= 1).
 *
 * Returns 0 on success, EINVAL / ENOMEM on error.
 */
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst)
{
    if (!src || !dst)
        return EINVAL;

    /* Sum all input multiplicities. */
    int64_t total = 0;
    if (src->timestamps) {
        for (uint32_t i = 0; i < src->nrows; i++)
            total += src->timestamps[i].multiplicity;
    } else {
        /* No timestamp tracking: treat each row as multiplicity 1. */
        total = (int64_t)src->nrows;
    }

    /* Prepare the complete replacement off to the side.  In particular,
     * never install timestamps before the column allocation has succeeded:
     * callers rely on a failed reduce leaving dst byte-for-byte usable. */
    uint32_t ncols = dst->ncols ? dst->ncols : 1;
    if (ncols == 0)
        return EINVAL;
    int64_t **new_columns = col_columns_alloc(ncols, 1);
    if (!new_columns)
        return ENOMEM;
    col_delta_timestamp_t *new_timestamps
        = (col_delta_timestamp_t *)calloc(1, sizeof(*new_timestamps));
    if (!new_timestamps) {
        col_columns_free(new_columns, ncols);
        return ENOMEM;
    }
    for (uint32_t c = 0; c < ncols; c++)
        new_columns[c][0] = 0;
    new_columns[0][0] = total;
    new_timestamps[0].multiplicity = total;

    uint64_t ledger_before = col_rel_owned_ledger_bytes(dst);
    if (dst->columns) {
        if (dst->col_shared) {
            for (uint32_t c = 0; c < dst->ncols; c++)
                if (!dst->col_shared[c])
                    free(dst->columns[c]);
            free((void *)dst->columns);
        } else if (!dst->arena_owned) {
            col_columns_free(dst->columns, dst->ncols);
        } else {
            free((void *)dst->columns);
        }
    }
    free(dst->col_shared);
    free(dst->timestamps);
    dst->columns = new_columns;
    dst->col_shared = NULL;
    dst->timestamps = new_timestamps;
    dst->timestamp_capacity = 1;
    dst->arena_owned = false;
    dst->ncols = ncols;
    dst->capacity = 1;
    dst->nrows = 1;
    col_rel_ledger_reconcile(dst, ledger_before);
    wl_columnar_relation_touch_storage(dst);
    wl_columnar_relation_touch_view(dst);

    return 0;
}

/* ======================================================================== */
/* LFTJ Operator (Issue #195)                                               */
/* ======================================================================== */

#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
#if defined(_MSC_VER)
#define WL_COLUMNAR_LFTJ_OUTPUT_TEST_TLS __declspec(thread)
#else
#define WL_COLUMNAR_LFTJ_OUTPUT_TEST_TLS _Thread_local
#endif
static WL_COLUMNAR_LFTJ_OUTPUT_TEST_TLS
wl_columnar_lftj_output_test_hook_state_t lftj_output_test_hook_state;

void
wl_columnar_lftj_test_deny_next_output_growth(void)
{
    lftj_output_test_hook_state.deny_output_growth_pending = true;
    lftj_output_test_hook_state.deny_output_growth_consumed = false;
}

void
wl_columnar_lftj_test_deny_next_output_construction(void)
{
    lftj_output_test_hook_state.deny_output_construction_pending = true;
    lftj_output_test_hook_state.deny_output_construction_consumed = false;
}

void
wl_columnar_lftj_test_fail_next_output_growth(void)
{
    lftj_output_test_hook_state.fail_output_growth_pending = true;
    lftj_output_test_hook_state.fail_output_growth_consumed = false;
}

void
wl_columnar_lftj_test_get_output_hook_state(
    wl_columnar_lftj_output_test_hook_state_t *out)
{
    if (out)
        *out = lftj_output_test_hook_state;
}

void
wl_columnar_lftj_test_clear_output_hooks(void)
{
    memset(&lftj_output_test_hook_state, 0,
        sizeof(lftj_output_test_hook_state));
}

static void
lftj_test_before_output_construction(
    wl_columnar_memory_governor_ref_t *governor)
{
    wl_columnar_lftj_output_test_hook_state_t *state
        = &lftj_output_test_hook_state;
    if (!state->deny_output_construction_pending || !governor)
        return;
    wl_columnar_memory_governor_t *memory_governor
        = wl_columnar_memory_governor_ref_get(governor);
    if (!memory_governor)
        return;
    state->previous_constructor_usable_bytes = atomic_load_explicit(
        &memory_governor->usable_bytes, memory_order_relaxed);
    state->reserved_before_construction
        = wl_columnar_memory_reserved(memory_governor);
    atomic_store_explicit(&memory_governor->usable_bytes,
        state->reserved_before_construction, memory_order_relaxed);
    state->deny_output_construction_pending = false;
    state->deny_output_construction_consumed = true;
    state->deny_output_construction_restore_pending = true;
}

static void
lftj_test_after_output_construction(
    wl_columnar_memory_governor_ref_t *governor)
{
    wl_columnar_lftj_output_test_hook_state_t *state
        = &lftj_output_test_hook_state;
    if (!state->deny_output_construction_restore_pending || !governor)
        return;
    wl_columnar_memory_governor_t *memory_governor
        = wl_columnar_memory_governor_ref_get(governor);
    if (memory_governor)
        atomic_store_explicit(&memory_governor->usable_bytes,
            state->previous_constructor_usable_bytes, memory_order_relaxed);
    state->deny_output_construction_restore_pending = false;
}

static bool
lftj_test_before_output_growth(col_rel_t *out,
    wl_columnar_memory_governor_ref_t *governor)
{
    wl_columnar_lftj_output_test_hook_state_t *state
        = &lftj_output_test_hook_state;
    if (!out || out->nrows == 0 || out->nrows < out->capacity)
        return false;
    if (!state->fail_output_growth_pending
        && !state->deny_output_growth_pending)
        return false;
    state->output_rows_at_growth = out->nrows;
    state->output_capacity_at_growth = out->capacity;
    if (state->fail_output_growth_pending) {
        state->fail_output_growth_pending = false;
        state->fail_output_growth_consumed = true;
        return true;
    }
    if (state->deny_output_growth_pending && governor) {
        wl_columnar_memory_governor_t *memory_governor
            = wl_columnar_memory_governor_ref_get(governor);
        if (memory_governor) {
            state->previous_usable_bytes = atomic_load_explicit(
                &memory_governor->usable_bytes, memory_order_relaxed);
            state->reserved_before_growth
                = wl_columnar_memory_reserved(memory_governor);
            atomic_store_explicit(&memory_governor->usable_bytes,
                state->reserved_before_growth, memory_order_relaxed);
            state->deny_output_growth_pending = false;
            state->deny_output_growth_consumed = true;
            state->deny_output_growth_restore_pending = true;
        }
    }
    return false;
}

static void
lftj_test_after_output_append(wl_columnar_memory_governor_ref_t *governor)
{
    wl_columnar_lftj_output_test_hook_state_t *state
        = &lftj_output_test_hook_state;
    if (!state->deny_output_growth_restore_pending || !governor)
        return;
    wl_columnar_memory_governor_t *memory_governor
        = wl_columnar_memory_governor_ref_get(governor);
    if (memory_governor)
        atomic_store_explicit(&memory_governor->usable_bytes,
            state->previous_usable_bytes, memory_order_relaxed);
    state->deny_output_growth_restore_pending = false;
}
#endif

/*
 * lftj_binary_ctx_t: callback context for col_op_lftj.
 *
 * wl_lftj_join delivers rows in compact format:
 *   [key, non_key_rel0..., non_key_rel1..., ...]
 *
 * This context reconstructs binary-join-compatible rows:
 *   [all_rel0_cols, all_rel1_cols, ...]  (key duplicated per relation)
 *
 * The downstream WL_PLAN_OP_MAP project_indices are unchanged because the
 * output column layout matches what a cascade of WL_PLAN_OP_JOIN produces.
 */
typedef struct {
    uint32_t k;
    uint32_t *ncols;          /* per-relation column count (k entries)    */
    uint32_t *key_cols;       /* per-relation join key column (k entries) */
    uint32_t *lftj_offsets;   /* start of Ri's non-key cols in LFTJ row  */
    uint32_t *binary_offsets; /* start of Ri's cols in binary output     */
    uint32_t total_binary_ncols;
    int64_t *tmp;   /* scratch row buffer                       */
    col_rel_t *out; /* destination relation                     */
    int rc;         /* first error code encountered; 0 = ok    */
    wl_columnar_memory_governor_ref_t *governor;
    col_rel_append_batch_t *batch; /* held lease on out (#2072)       */
} lftj_binary_ctx_t;

/*
 * lftj_binary_cb: output callback for col_op_lftj.
 *
 * Converts compact LFTJ output to binary-join-compatible format and appends
 * the result to ctx->out through ctx->batch.  Sets ctx->rc on allocation
 * failure (subsequent calls are no-ops).
 */
static void
lftj_binary_cb(const int64_t *row, uint32_t lftj_ncols, void *user)
{
    (void)lftj_ncols;
    lftj_binary_ctx_t *ctx = (lftj_binary_ctx_t *)user;
    if (ctx->rc)
        return; /* already OOM; skip remaining rows */

    const int64_t key = row[0];
    for (uint32_t i = 0; i < ctx->k; i++) {
        uint32_t nc = ctx->ncols[i];
        uint32_t kc = ctx->key_cols[i];
        uint32_t lo = ctx->lftj_offsets[i];
        uint32_t bo = ctx->binary_offsets[i];
        for (uint32_t c = 0; c < nc; c++) {
            int64_t val;
            if (c == kc)
                val = key;
            else if (c < kc)
                val = row[lo + c];
            else
                val = row[lo + c - 1u];
            ctx->tmp[bo + c] = val;
        }
    }
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
    if (lftj_test_before_output_growth(ctx->out, ctx->governor)) {
        lftj_output_test_hook_state.failure_pending_flag
            = ctx->out->memory_budget_denial_pending;
        ctx->rc = ENOMEM;
        return;
    }
#endif
    int rc = col_rel_append_batch_row(ctx->batch, ctx->tmp);
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
    lftj_test_after_output_append(ctx->governor);
#endif
    if (rc == ENOMEM && ctx->out->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc)
        ctx->rc = rc;
}

/*
 * col_op_lftj: execute a WL_PLAN_OP_LFTJ operator.
 *
 * Performs a multi-way leapfrog triejoin over the k EDB relations named in
 * op->opaque_data.  Uses the sorted arrangement cache to avoid re-sorting
 * on repeated calls (the sort inside wl_lftj_join degrades to O(N) when the
 * input is already sorted).  Pushes binary-join-compatible result onto stack.
 */
int
col_op_lftj(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    if (!op || !stack || !sess)
        return EINVAL;
    if (!op->opaque_data)
        return EINVAL;
    const wl_plan_op_lftj_t *meta = (const wl_plan_op_lftj_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k < 2u || !meta->rel_names || !meta->key_cols)
        return EINVAL;

    uint64_t lftj_scratch_bytes = 0;
    uint64_t bytes = 0;
    if (!wl_columnar_memory_size_mul(k,
        sizeof(wl_lftj_input_t) + sizeof(col_sorted_arrangement_probe_t)
        + sizeof(col_rel_t *)
        + sizeof(wl_columnar_source_access_reader_t)
        + sizeof(bool) + 3u * sizeof(uint32_t), &bytes)
        || !wl_columnar_memory_size_add(lftj_scratch_bytes, bytes,
        &lftj_scratch_bytes))
        return EOVERFLOW;
    for (uint32_t i = 0; i < k; i++) {
        col_rel_t *rel = session_find_rel(sess, meta->rel_names[i]);
        if (!rel || meta->key_cols[i] >= rel->ncols)
            return rel ? EINVAL : ENOENT;
        if (!wl_columnar_memory_size_mul(rel->nrows, rel->ncols,
            &bytes)
            || !wl_columnar_memory_size_mul(bytes, sizeof(int64_t), &bytes)
            || !wl_columnar_memory_size_add(lftj_scratch_bytes, bytes,
            &lftj_scratch_bytes))
            return EOVERFLOW;
        if (!wl_columnar_memory_size_mul(rel->ncols, sizeof(int64_t),
            &bytes)
            || !wl_columnar_memory_size_add(lftj_scratch_bytes, bytes,
            &lftj_scratch_bytes))
            return EOVERFLOW;
    }
    wl_ops_scratch_t lftj_scratch;
    int lftj_scratch_rc = wl_ops_scratch_reserve(&lftj_scratch,
            sess ? sess->memory_governor : NULL, lftj_scratch_bytes, sess);
    if (lftj_scratch_rc != 0)
        return lftj_scratch_rc;
#define WL_LFTJ_RELEASE_SCRATCH() wl_ops_scratch_release(&lftj_scratch)

    /* Allocate per-relation working arrays. */
    wl_lftj_input_t *inputs
        = (wl_lftj_input_t *)calloc(k, sizeof(wl_lftj_input_t));
    col_sorted_arrangement_probe_t *sorted_probes
        = (col_sorted_arrangement_probe_t *)calloc(
            k, sizeof(col_sorted_arrangement_probe_t));
    col_rel_t **relations
        = (col_rel_t **)calloc(k, sizeof(col_rel_t *));
    wl_columnar_source_access_reader_t *source_readers
        = (wl_columnar_source_access_reader_t *)calloc(
            k, sizeof(wl_columnar_source_access_reader_t));
    bool *cache_backed = (bool *)calloc(k, sizeof(bool));
    uint32_t *ncols = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *lftj_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *binary_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!inputs || !sorted_probes || !relations || !source_readers
        || !cache_backed || !ncols || !lftj_offsets || !binary_offsets) {
        free(inputs);
        free(sorted_probes);
        free((void *)relations);
        free(source_readers);
        free(cache_backed);
        free(ncols);
        free(lftj_offsets);
        free(binary_offsets);
        WL_LFTJ_RELEASE_SCRATCH();
        return ENOMEM;
    }

    /* Resolve every relation and acquire every source reader before building
    * any sorted cache.  This prevents a later input from observing a
    * different source generation while earlier inputs are being prepared. */
    uint32_t total_binary_ncols = 0u;
    uint32_t lftj_nk_total = 0u;
    wirelog_column_type_t key_type = WIRELOG_TYPE_INT64;
    int rc = 0;
    for (uint32_t i = 0; i < k; i++) {
        col_rel_t *rel = session_find_rel(sess, meta->rel_names[i]);
        if (!rel) {
            rc = ENOENT;
            goto cleanup_arrays;
        }
        uint32_t kc = meta->key_cols[i];
        if (kc >= rel->ncols) {
            rc = EINVAL;
            goto cleanup_arrays;
        }
        wirelog_column_type_t relation_key_type = rel->column_types
            ? rel->column_types[kc] : WIRELOG_TYPE_INT64;
        if (i == 0)
            key_type = relation_key_type;
        else if (relation_key_type != key_type) {
            rc = EINVAL;
            goto cleanup_arrays;
        }
        relations[i] = rel;
        rc = col_rel_source_reader_acquire(rel, &source_readers[i]);
        if (rc != 0)
            goto cleanup_arrays;
    }

    /* Populate LFTJ input descriptors while all source generations remain
     * stable under the readers acquired above. */
    for (uint32_t i = 0; i < k; i++) {
        col_rel_t *rel = relations[i];
        uint32_t kc = meta->key_cols[i];

        /* Use a leased pre-sorted arrangement when available.  The lease
         * covers the source generation, sorted buffer, and registry entry
         * until the join and all callbacks have stopped reading it. */
        int pin_rc
            = col_session_acquire_sorted_arrangement_probe_with_source_reader(
                (wl_session_t *)sess, rel, kc, &sorted_probes[i],
                &source_readers[i]);
        if (pin_rc == 0) {
            col_sorted_arr_t *sarr = sorted_probes[i].arr;
            inputs[i].data = sarr->sorted;
            inputs[i].nrows = sarr->nrows;
            cache_backed[i] = true;
        } else if (pin_rc != ENOMEM && pin_rc != EBUSY) {
            rc = pin_rc;
            goto cleanup_arrays;
        } else {
            /* A cache build may be unavailable, but the operator can still
             * make an owned input copy.  The source reader acquired in the
             * first pass keeps it stable while LFTJ consumes it. */
            /* Gather column-major into flat buffer for LFTJ.  The source
             * reader acquired in the first pass remains held through join. */
            int64_t *flat = (int64_t *)malloc(
                (size_t)rel->nrows * rel->ncols * sizeof(int64_t));
            if (!flat) {
                rc = ENOMEM;
                goto cleanup_arrays;
            }
            for (uint32_t r = 0; r < rel->nrows; r++)
                col_rel_row_copy_out(rel, r,
                    flat + (size_t)r * rel->ncols);
            inputs[i].data = flat;
            inputs[i].nrows = rel->nrows;
        }
        inputs[i].ncols = rel->ncols;
        inputs[i].key_col = kc;
        ncols[i] = rel->ncols;
        binary_offsets[i] = total_binary_ncols;
        lftj_offsets[i] = 1u + lftj_nk_total; /* 1: shared key lives at [0] */
        total_binary_ncols += rel->ncols;
        lftj_nk_total += rel->ncols - 1u;
    }

    {
        col_rel_t *out = NULL;
        if (sess->memory_governor) {
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
            lftj_test_before_output_construction(sess->memory_governor);
#endif
            rc = wl_columnar_relation_new_auto_governed("$lftj",
                    total_binary_ncols, 0, false, sess->memory_governor, &out);
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
            lftj_test_after_output_construction(sess->memory_governor);
#endif
        } else {
            out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
                    "$lftj", total_binary_ncols);
            rc = out ? 0 : ENOMEM;
        }
        int64_t *tmp = (int64_t *)malloc(
            (total_binary_ncols ? total_binary_ncols : 1u) * sizeof(int64_t));
        if (!out || !tmp) {
            free(tmp);
            if (out)
                col_rel_destroy(out);
            if (rc == 0)
                rc = ENOMEM;
            goto cleanup_arrays;
        }
        /* The callback runs on this thread and appends every output row
        * under one held lease.  out is not one of the join's inputs. */
        col_rel_append_batch_t lftj_batch = { 0 };
        rc = col_rel_append_batch_begin(&lftj_batch, out);
        if (rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            goto cleanup_arrays;
        }

        lftj_binary_ctx_t ctx = { k,
                                  ncols,
                                  meta->key_cols,
                                  lftj_offsets,
                                  binary_offsets,
                                  total_binary_ncols,
                                  tmp,
                                  out,
                                  0,
                                  sess->memory_governor,
                                  &lftj_batch };

        rc = wl_columnar_lftj_join_typed_governed(inputs, key_type, k,
                lftj_binary_cb, &ctx,
                sess ? sess->memory_governor : NULL);
        if (rc == 0)
            rc = ctx.rc;
        int end_rc = col_rel_append_batch_end(&lftj_batch, rc == 0);
        if (rc == 0)
            rc = end_rc;

        free(tmp);
        if (rc != 0) {
            col_rel_destroy(out);
            goto cleanup_arrays;
        }
        rc = eval_stack_push(stack, out, true);
        if (rc != 0)
            col_rel_destroy(out);
    }

cleanup_arrays:
    /* Release leases/readers before freeing any owned fallback buffers. */
    if (inputs) {
        for (uint32_t i = 0; i < k; i++) {
            if (sorted_probes && sorted_probes[i].active) {
                int release_rc = col_sorted_arrangement_probe_release(
                    &sorted_probes[i]);
                if (release_rc != 0 && sorted_probes[i].active)
                    release_rc = col_sorted_arrangement_probe_release(
                        &sorted_probes[i]);
                if (release_rc != 0) {
                    /* The operation-local token is the only recoverable
                     * owner of the pin.  Returning after losing it would
                     * leave the session permanently pinned, so fail closed
                     * rather than free the token storage. */
                    fprintf(stderr,
                        "wirelog: fatal sorted-arrangement probe release "
                        "failure (%d)\n", release_rc);
                    abort();
                }
            }
            /* Acquisition can fail after taking the source reader but before
             * activating the probe.  Keep that token recoverable as well. */
            if (sorted_probes && !sorted_probes[i].active
                && sorted_probes[i].source_reader.owner) {
                int release_rc = col_rel_source_reader_release(
                    &sorted_probes[i].source_reader);
                if (release_rc != 0) {
                    fprintf(stderr,
                        "wirelog: fatal sorted-arrangement reader release "
                        "failure (%d)\n", release_rc);
                    abort();
                }
            }
            if (source_readers && source_readers[i].owner) {
                int release_rc = col_rel_source_reader_release(
                    &source_readers[i]);
                if (release_rc != 0) {
                    fprintf(stderr,
                        "wirelog: fatal LFTJ source reader release "
                        "failure (%d)\n", release_rc);
                    abort();
                }
            }
            if (inputs[i].data && (!cache_backed || !cache_backed[i]))
                free((void *)inputs[i].data);
        }
    }
    free(inputs);
    free(sorted_probes);
    free((void *)relations);
    free(source_readers);
    free(cache_backed);
    free(ncols);
    free(lftj_offsets);
    free(binary_offsets);
    WL_LFTJ_RELEASE_SCRATCH();
#undef WL_LFTJ_RELEASE_SCRATCH
    if (rc == ENOSPC && sess)
        sess->memory_budget_denied = true;
    return rc;
}

/* --- DIFFERENTIAL CONSOLIDATE is implemented in columnar/diff.c. */
/* --- JOIN and ANTIJOIN are implemented in columnar/join.c. */

/* --- Private initial numeric REDUCE (#1821) --- */

enum {
    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK = 192,
    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK = 128,
    /* Two payments per first cleanup turn:64 current +128 future per
     * payment, as in existing B2 retirement. Named capacity: common10,
     * batch18, fixed scratch12, right source1, JOIN decision1. Common10 is
     * PROGRAM,BATCH,PHYSICAL,CREDIT,REFERENCE,SOURCE,CONTROL,FINISH,RESET,ACK;
     * JOIN bypasses common BATCH and ordinary DECIDE; BATCH shares are unused.
     * Child prepaid
     * interiors are distinct; unused named capacity is never refunded. */
    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_RETIRE_PAYMENTS = 84
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_ops_test_reduce_attempt_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
int wl_columnar_ops_test_reduce_attempt_fail_allocation = -1;
#endif

static int
wl_columnar_ops_reduce_attempt_guard(wl_evaluation_control_t *control,
    uint64_t id)
{
    return !control || !id
           || wl_evaluation_control_active_attempt_id(control) != id
        ? EINVAL : 0;
}

static int
wl_columnar_ops_reduce_attempt_charge(wl_evaluation_control_t *control,
    uint64_t id, uint64_t work)
{
    int rc = wl_columnar_ops_reduce_attempt_guard(control, id);
    return rc ? rc : wl_evaluation_control_charge(control, work);
}

static int
wl_columnar_ops_reduce_attempt_tail(wl_evaluation_control_t *control,
    uint64_t id, unsigned phase, uint64_t progress)
{
    int rc = wl_columnar_ops_reduce_attempt_guard(control, id);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_ops_test_reduce_attempt_checkpoint)
        wl_columnar_ops_test_reduce_attempt_checkpoint(control, phase,
            progress);
#else
    (void)phase; (void)progress;
#endif
    return wl_columnar_ops_reduce_attempt_charge(control, id, 0);
}

static int
wl_columnar_ops_reduce_attempt_return(wl_evaluation_control_t *control,
    uint64_t id, int child_result)
{
    int tail = wl_columnar_ops_reduce_attempt_charge(control, id, 0);
    return tail > 0 ? tail : child_result ? child_result : tail;
}

static void *
wl_columnar_ops_reduce_attempt_allocate(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_ops_test_reduce_attempt_fail_allocation >= 0) {
        if (!wl_columnar_ops_test_reduce_attempt_fail_allocation) return NULL;
        --wl_columnar_ops_test_reduce_attempt_fail_allocation;
    }
#endif
    return malloc(bytes);
}

static int
wl_columnar_ops_reduce_attempt_scratch(wl_columnar_join_scratch_t *scratch,
    uint64_t bytes, wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control, uint64_t id)
{
    int rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(&scratch->credit);
    scratch->physical = NULL; scratch->governor = NULL;
    scratch->allocated = false; scratch->admitted = false;
    scratch->committed = false; scratch->ref_held = false;
    scratch->initialized = true;
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 9, bytes);
    if (rc) return rc;
    if (!bytes) return 0;
    if (governor) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
        if (rc) return rc;
        scratch->governor = governor;
        wl_columnar_memory_governor_ref_retain(governor);
        scratch->ref_held = true;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 10, bytes);
        if (rc) return rc;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(governor), bytes,
            &scratch->credit, &status, control, id);
        if (rc) return rc;
        scratch->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        int admission = scratch->admitted ? 0
            : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
        rc = wl_columnar_ops_reduce_attempt_return(control, id, admission);
        if (rc) return rc;
    }
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) return rc;
    scratch->physical = wl_columnar_ops_reduce_attempt_allocate((size_t)bytes);
    scratch->allocated = scratch->physical != NULL;
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 11, bytes);
    if (rc > 0 || scratch->allocated) {
        if (rc) return rc;
    } else return ENOMEM;
    if (scratch->admitted) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
        if (rc) return rc;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 12, bytes);
        if (rc) return rc;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &scratch->credit, scratch->physical, &scratch->committed, control,
            id);
        rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    }
    return rc;
}

static void
wl_columnar_ops_reduce_attempt_status(
    const wl_columnar_filter_context_t *context,
    wl_columnar_filter_status_t *status)
{
    status->progress = WL_COLUMNAR_FILTER_PROGRESS_YIELD;
    status->execution_result = context->execution_result;
    status->cleanup_result = context->cleanup_result;
    status->semantic_result = context->semantic_result;
    status->committed = context->committed;
}

/* Dense records keep raw first-seen order independently of hash slots. */
typedef struct {
    uint32_t representative;
    int64_t value;
    double sum;
    uint64_t count;
} wl_columnar_ops_reduce_group_t;

int
wl_columnar_ops_reduce_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_ops_reduce_attempt_start_owned(op, frame, session,
               control, id, status, NULL);
}

int
wl_columnar_ops_reduce_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!op || !status) return EINVAL;
    if (owner && !control) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, session, &stack,
            owner);
    if (rc) return rc;
    if (owner) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (record->control != control || record->attempt_id != id)
            return EINVAL;
    }
    if (!control) {
        int result = op->op == WL_PLAN_OP_REDUCE
            ? col_op_reduce(op, stack, session) : EINVAL;
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        status->cleanup_result = 0; status->committed = result == 0;
        return 0;
    }
    rc = wl_columnar_ops_reduce_attempt_guard(control, id);
    if (rc) return rc;
    wl_columnar_filter_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    /* Refused first admission has no live context or resource ownership. */
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) {
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = rc; status->semantic_result = rc;
        status->cleanup_result = 0; status->committed = false;
        return 0;
    }
    ctx->program = NULL; ctx->output = NULL; ctx->private_output = NULL;
    ctx->governor = NULL; ctx->values = ctx->inline_values;
    ctx->source_initialized = false; ctx->batch_initialized = false;
    ctx->drain_initialized = false; ctx->heap = false; ctx->admitted = false;
    ctx->credit_committed = false; ctx->ref_held = false;
    ctx->unary_started = false; ctx->committed = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0;
    ctx->control = NULL; ctx->pin_owned = false; ctx->thread_valid = false;
    ctx->busy = false; ctx->frame = frame; ctx->session = session;
    ctx->attempt_id = id; ctx->join_right_initialized = false;
    ctx->join_retirement_cursor = 0; ctx->join_result_is_delta = false;
    ctx->join_decision_pending = false;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[i];
        scratch->initialized = false; scratch->allocated = false;
        scratch->admitted = false; scratch->committed = false;
        scratch->ref_held = false; scratch->physical = NULL;
        scratch->governor = NULL;
    }
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    ctx->control = control; ctx->pin_owned = true; ctx->identity = ctx;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->busy = true;
    ctx->join_initialized = true; ctx->join_decision_pending = true;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    bool count = op->agg_fn == WIRELOG_AGG_COUNT;
    bool floating = !count && op->agg_operand_type == WL_PLAN_AGG_OPERAND_FLOAT;
    bool domain = op->agg_operand_type >= WL_PLAN_AGG_OPERAND_UNKNOWN
        && op->agg_operand_type <= WL_PLAN_AGG_OPERAND_FLOAT
        && (count || op->agg_operand_type != WL_PLAN_AGG_OPERAND_STRING);
    bool supported = op->op == WL_PLAN_OP_REDUCE && !op->materialized
        && domain && (count || op->agg_fn == WIRELOG_AGG_SUM
        || op->agg_fn == WIRELOG_AGG_MIN || op->agg_fn == WIRELOG_AGG_MAX
        || (op->agg_fn == WIRELOG_AGG_AVG && floating))
        && !session->current_iteration && !session->diff_operators_active
        && !session->retraction_seeded && !session->retraction_right_pass
        && !session->tdd_input_run && !session->tdd_subpass_active
        && !session->tdd_outbound_only_active && !session->join_batch_bytes
        && !session->join_output_shared_count && !session->coordinator
        && session->num_workers <= 1;
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 1, 0);
    if (rc > 0 || supported) {
        if (rc) goto done;
    } else {
        rc = ENOTSUP; goto done;
    }
    for (unsigned payment = 0;
        payment < WL_COLUMNAR_OPS_REDUCE_ATTEMPT_RETIRE_PAYMENTS; ++payment){
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 2, payment);
        if (rc) goto done;
    }
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 3, 0);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
            control, id, owner);
    if (rc) goto done;
    ctx->unary_started = true;
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    const col_rel_t *left = NULL;
    col_rel_t **out_slot = NULL;
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_input_owned(frame, &left, owner);
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_init_attempt_controlled(&ctx->source,
            &ctx->source_initialized, control, id);
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->source, left, false);
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    if (ctx->source.timestamps) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    uint32_t gc = op->group_by_count;
    bool shaped = gc != UINT32_MAX && (!gc || ctx->source.width);
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 30, gc);
    if (rc > 0 || shaped) {
        if (rc) goto done;
    } else {
        rc = gc == UINT32_MAX ? EOVERFLOW : EINVAL; goto done;
    }
    uint32_t width = gc + 1;
    uint32_t ai = op->aggregate_index < width ? op->aggregate_index : gc;
    bool expression = !count && op->agg_expr.data && op->agg_expr.size;
    wl_columnar_memory_governor_ref_t *governor = session->memory_governor
        ? session->memory_governor : left->memory_governor;
    uint32_t capacity = 1;
    uint64_t desired = (uint64_t)(ctx->source.nrows ? ctx->source.nrows : 1) *
        2u;
    while ((uint64_t)capacity < desired) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        bool fits = capacity <= UINT32_MAX / 2u;
        if (fits) capacity *= 2u;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 31, capacity);
        if (rc > 0 || fits) {
            if (rc) goto done;
        } else {
            rc = EOVERFLOW; goto done;
        }
    }
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    uint64_t slot_bytes = 0, group_bytes = 0, lanes = 0, workspace = 0;
    bool sized = wl_columnar_memory_size_mul(capacity,
            sizeof(reduce_group_slot_t), &slot_bytes)
        && wl_columnar_memory_size_mul(ctx->source.nrows,
            sizeof(wl_columnar_ops_reduce_group_t), &group_bytes)
        && wl_columnar_memory_size_mul(width, 2u, &lanes)
        && wl_columnar_memory_size_add(lanes,
            expression ? ctx->source.width : 0u, &lanes)
        && wl_columnar_memory_size_mul(lanes, sizeof(int64_t), &workspace)
        && slot_bytes <= SIZE_MAX && group_bytes <= SIZE_MAX
        && workspace <= SIZE_MAX;
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 32, workspace);
    if (rc > 0 || sized) {
        if (rc) goto done;
    } else {
        rc = EOVERFLOW; goto done;
    }
    uint64_t sizes[WL_COLUMNAR_JOIN_SCRATCH_COUNT] = {
        slot_bytes, group_bytes, workspace
    };
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_ops_reduce_attempt_scratch(&ctx->join_scratch[i],
                sizes[i], governor, control, id);
        rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
        if (rc) goto done;
    }
    reduce_group_slot_t *slots = ctx->join_scratch[0].physical;
    wl_columnar_ops_reduce_group_t *groups = ctx->join_scratch[1].physical;
    int64_t *types = ctx->join_scratch[2].physical;
    int64_t *values = types + width;
    int64_t *row_values = values + width;
    for (uint32_t c = 0; c < width; ++c) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        uint32_t key = c > ai ? c - 1u : c;
        uint32_t source = op->group_by_indices && c != ai
            ? op->group_by_indices[key] : key;
        types[c] = c ==
            ai ? (floating ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64)
            : source < ctx->source.width && ctx->source.types
            ? ctx->source.types[source] : WIRELOG_TYPE_INT64;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 33, c);
        if (rc) goto done;
    }
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    uint32_t initial = governor && ctx->source.nrows < COL_REL_INIT_CAP
        ? ctx->source.nrows : COL_REL_INIT_CAP;
    ctx->width = width; ctx->nrows = ctx->source.nrows;
    rc = wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
        "$reduce", width, initial, types, governor, out_slot, control, id);
    ctx->output = *out_slot; ctx->private_output = *out_slot;
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    if (expression) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
        if (rc) goto done;
        bool compiled = false;
        rc = wl_columnar_expr_compile_attempt_controlled(op->agg_expr.data,
                op->agg_expr.size, governor, &ctx->program, &compiled, control,
                id);
        rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
        if (rc) goto done;
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 34, compiled);
        if (rc > 0 || compiled) {
            if (rc) goto done;
        } else {
            rc = ENOTSUP; goto done;
        }
    }
    for (uint32_t slot = 0; slot < capacity; ++slot) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        slots[slot].hash = 0; slots[slot].row = 0;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 35, slot);
        if (rc) goto done;
    }
    uint32_t group_count = 0;
    for (uint32_t r = 0; r < ctx->source.nrows; ++r) {
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        ctx->row = r;
        int64_t operand = count ? 1 : ctx->source.width > gc
            ? ctx->source.columns[gc][r] : 1;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 36, r);
        if (rc) goto done;
        if (expression) {
            for (uint32_t c = 0; c < ctx->source.width; ++c) {
                rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                        WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
                if (rc) goto done;
                row_values[c] = ctx->source.columns[c][r];
                rc = wl_columnar_ops_reduce_attempt_tail(control, id, 37, c);
                if (rc) goto done;
            }
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
            if (rc) goto done;
            bool failed = false;
            rc = wl_columnar_expr_eval_attempt_controlled(ctx->program,
                    row_values, ctx->source.width, &operand, &failed, control,
                    id);
            rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 38, failed);
            if (rc > 0 || !failed) {
                if (rc) goto done;
            } else {
                rc = ERANGE; goto done;
            }
        }
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        bool valid = !floating || wl_columnar_float_bits_valid(operand);
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 39, r);
        if (rc > 0 || valid) {
            if (rc) goto done;
        } else {
            rc = EINVAL; goto done;
        }
        uint64_t hash = UINT64_C(1469598103934665603);
        for (uint32_t k = 0; k < gc; ++k) {
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint32_t c = op->group_by_indices ? op->group_by_indices[k] : k;
            int64_t value = ctx->source.columns[c <
                    ctx->source.width ? c : 0][r];
            if (ctx->source.types && c < ctx->source.width
                && ctx->source.types[c] == WIRELOG_TYPE_FLOAT)
                value = (int64_t)wl_columnar_float_canonical_bits(value);
            hash = (hash ^ (uint64_t)value) * UINT64_C(1099511628211);
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 40, k);
            if (rc) goto done;
        }
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        if (!hash) hash = 1;
        uint32_t slot = (uint32_t)hash & (capacity - 1u);
        bool found = false;
        rc = wl_columnar_ops_reduce_attempt_tail(control, id, 41, slot);
        if (rc) goto done;
        for (;;) {
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint64_t occupied = slots[slot].hash;
            bool match = occupied == hash;
            uint32_t gr = slots[slot].row;
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 42, slot);
            if (rc) goto done;
            if (!occupied) break;
            for (uint32_t k = 0; k < gc && match; ++k) {
                rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                        WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
                if (rc) goto done;
                uint32_t c = op->group_by_indices ? op->group_by_indices[k] : k;
                uint32_t src = c < ctx->source.width ? c : 0;
                int64_t a = ctx->source.columns[src][r];
                int64_t b = ctx->source.columns[src][groups[gr].representative];
                if (ctx->source.types && c < ctx->source.width
                    && ctx->source.types[c] == WIRELOG_TYPE_FLOAT) {
                    a = (int64_t)wl_columnar_float_canonical_bits(a);
                    b = (int64_t)wl_columnar_float_canonical_bits(b);
                }
                match = a == b;
                rc = wl_columnar_ops_reduce_attempt_tail(control, id, 43, k);
                if (rc) goto done;
            }
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            found = match;
            if (!found) slot = (slot + 1u) & (capacity - 1u);
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 44, slot);
            if (rc) goto done;
            if (found) break;
        }
        if (!found) {
            /* Raw validates each new group before inspecting any later row.
             * Keep operand errors before this same-row typed validation. */
            for (uint32_t c = 0; c < width; ++c) {
                rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                        WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
                if (rc) goto done;
                uint32_t k = c > ai ? c - 1u : c;
                uint32_t src = op->group_by_indices && c != ai
                    ? op->group_by_indices[k] : k;
                int64_t value = c == ai ? operand
                    : ctx->source.columns[src < ctx->source.width ? src : 0][r];
                valid = types[c] != WIRELOG_TYPE_FLOAT
                    || wl_columnar_float_bits_valid(value);
                if (valid && types[c] == WIRELOG_TYPE_FLOAT)
                    value = (int64_t)wl_columnar_float_canonical_bits(value);
                values[c] = value;
                rc = wl_columnar_ops_reduce_attempt_tail(control, id, 45, c);
                if (rc > 0 || valid) {
                    if (rc) goto done;
                } else {
                    rc = EINVAL; goto done;
                }
            }
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            wl_columnar_ops_reduce_group_t *group = &groups[group_count];
            group->representative = r;
            group->value = values[ai]; group->count = 1;
            group->sum = floating ? wl_columnar_float_from_bits(operand) : 0.0;
            slots[slot].row = group_count++; slots[slot].hash = hash;
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 46, r);
            if (rc) goto done;
        } else {
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
            if (rc) goto done;
            wl_columnar_ops_reduce_group_t *group = &groups[slots[slot].row];
            int64_t next = group->value;
            bool overflow = false;
            if (count) ++next;
            else if (op->agg_fn == WIRELOG_AGG_SUM && !floating)
                overflow = wl_columnar_arithmetic_checked_add_int64(
                    next, operand, &next) != 0;
            else if (op->agg_fn == WIRELOG_AGG_SUM
                || op->agg_fn == WIRELOG_AGG_AVG) {
                group->sum += wl_columnar_float_from_bits(operand);
                ++group->count;
                overflow = !isfinite(group->sum);
                next = (int64_t)wl_columnar_float_canonical_bits(
                    wl_columnar_float_to_bits(op->agg_fn == WIRELOG_AGG_AVG
                    ? group->sum / (double)group->count : group->sum));
            } else {
                int comparison = floating
                    ? wl_columnar_float_compare_bits(operand, next)
                    : operand < next ? -1 : operand > next ? 1 : 0;
                if ((op->agg_fn == WIRELOG_AGG_MIN && comparison < 0)
                    || (op->agg_fn == WIRELOG_AGG_MAX && comparison > 0))
                    next = floating ? (int64_t)
                        wl_columnar_float_canonical_bits(operand) : operand;
            }
            group->value = next;
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 47, r);
            if (rc > 0 || !overflow) {
                if (rc) goto done;
            } else {
                rc = ERANGE; goto done;
            }
        }
    }
    if (!group_count) goto done;
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    ctx->batch_initialized = true;
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true;
    rc = wl_columnar_ops_reduce_attempt_tail(control, id, 15, 0);
    if (rc) goto done;
    rc = wl_columnar_ops_reduce_attempt_charge(control, id,
            WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            ctx->output, control);
    rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
    if (rc) goto done;
    for (uint32_t gr = 0; gr < group_count; ++gr) {
        for (uint32_t c = 0; c < width; ++c) {
            rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                    WL_COLUMNAR_OPS_REDUCE_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint32_t k = c > ai ? c - 1u : c;
            uint32_t src = op->group_by_indices && c != ai
                ? op->group_by_indices[k] : k;
            int64_t value = c == ai ? groups[gr].value
                : ctx->source.columns[src < ctx->source.width ? src : 0]
                [groups[gr].representative];
            values[c] = types[c] == WIRELOG_TYPE_FLOAT
                ? (int64_t)wl_columnar_float_canonical_bits(value) : value;
            rc = wl_columnar_ops_reduce_attempt_tail(control, id, 48, c);
            if (rc) goto done;
        }
        rc = wl_columnar_ops_reduce_attempt_charge(control, id,
                WL_COLUMNAR_OPS_REDUCE_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_relation_append_batch_numeric_row_operator_controlled(
            &ctx->batch, values, control, id);
        rc = wl_columnar_ops_reduce_attempt_return(control, id, rc);
        if (rc) goto done;
    }
done:
    if (rc == ENOMEM && ctx->output &&
        ctx->output->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC) session->memory_budget_denied = true;
    ctx->execution_result = rc; ctx->semantic_result = rc;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH;
    ctx->busy = false;
    wl_columnar_ops_reduce_attempt_status(ctx, status);
    return 0;
}
