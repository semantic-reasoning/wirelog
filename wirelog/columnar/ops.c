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

/* --- VARIABLE ------------------------------------------------------------ */

int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
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
        col_rel_t *empty = col_rel_pool_new_like(
            sess->delta_pool, "$empty_skip", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, false);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
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
                col_rel_t *empty = col_rel_pool_new_like(
                    sess->delta_pool, "$empty_delta", full_rel);
                if (!empty)
                    return ENOMEM;
                int push_rc = eval_stack_push_delta(stack, empty, true, true);
                if (push_rc != 0)
                    col_rel_destroy(empty);
                return push_rc;
            }
            /* Base-case iteration: no deltas exist yet, fall back to full
             * relation so EDB-grounded rules can still fire on iter 0. */
            return eval_stack_push_delta(stack, full_rel, false, false);
        }
        /* Iteration > 0: delta absent or empty means the relation has
         * converged.  Push an empty relation so this rule copy produces
         * no output (correct semi-naive semantics, issue #85). */
        col_rel_t *empty
            = col_rel_pool_new_like(sess->delta_pool, "$empty_delta", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, true);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
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

int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    uint32_t pc = op->project_count;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$map", pc);
    if (!out) {
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * pc);
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    /* Pre-compile map expressions once to avoid per-row strtol. */
    wl_columnar_expr_compiled_t **ce_map = NULL;
    uint32_t ce_map_count = 0;
    if (op->map_exprs && op->map_expr_count > 0) {
        ce_map = (wl_columnar_expr_compiled_t **)calloc(pc,
                sizeof(wl_columnar_expr_compiled_t *));
        if (ce_map) {
            ce_map_count = (op->map_expr_count < pc) ? op->map_expr_count : pc;
            for (uint32_t c = 0; c < ce_map_count; c++) {
                if (op->map_exprs[c].data && op->map_exprs[c].size > 0)
                    ce_map[c] = wl_columnar_expr_compile(op->map_exprs[c].data,
                            op->map_exprs[c].size,
                            sess ? sess->intern : NULL);
            }
        }
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
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *const row = row_rb.ptr;
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
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                wl_columnar_expr_compiled_free(ce_map[i]);
                            free((void *)ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (e.owned)
                            col_rel_destroy(e.rel);
                        return ERANGE;
                    }
                    tmp[c] = val;
                } else {
                    int64_t val = 0;
                    if (wl_columnar_expr_eval_i64(op->map_exprs[c].data,
                        op->map_exprs[c].size, row, e.rel->ncols,
                        &val, sess->intern) != 0) {
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                wl_columnar_expr_compiled_free(ce_map[i]);
                            free((void *)ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (e.owned)
                            col_rel_destroy(e.rel);
                        return ERANGE;
                    }
                    tmp[c] = val;
                }
            } else {
                uint32_t src = op->project_indices ? op->project_indices[c] : c;
                tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
            }
        }
        int rc = col_rel_append_row(out, tmp);
        if (rc != 0) {
            if (ce_map) {
                for (uint32_t c = 0; c < ce_map_count; c++)
                    wl_columnar_expr_compiled_free(ce_map[c]);
                free((void *)ce_map);
            }
            col_row_buf_release(&row_rb);
            free(tmp);
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(e.rel);
            return rc;
        }
    }

    if (ce_map) {
        for (uint32_t c = 0; c < ce_map_count; c++)
            wl_columnar_expr_compiled_free(ce_map[c]);
        free((void *)ce_map);
    }
    col_row_buf_release(&row_rb);
    free(tmp);

    if (e.owned)
        col_rel_destroy(e.rel);
    return eval_stack_push(stack, out, true);
}

/* --- CONCAT and CONSOLIDATE are implemented in columnar/merge.c. */
/* --- K-FUSION is implemented in columnar/kfusion.c. */
/* --- REDUCE (aggregate) -------------------------------------------------- */

int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t gc = op->group_by_count;

    /* Output: the group columns and aggregate retain the rule-head order. */
    uint32_t ocols = gc + 1;
    uint32_t agg_index = op->aggregate_index < ocols
        ? op->aggregate_index : gc;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$reduce", ocols);
    if (!out) {
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    bool float_agg = op->agg_operand_type == WL_PLAN_AGG_OPERAND_FLOAT;
    if (ocols > 0) {
        wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
            (size_t)ocols * sizeof(*types));
        if (!types) {
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(in);
            return ENOMEM;
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
            if (e.owned)
                col_rel_destroy(in);
            return type_rc;
        }
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    wl_columnar_expr_compiled_t *agg_ce = NULL;
    if (op->agg_expr.data && op->agg_expr.size > 0)
        agg_ce = wl_columnar_expr_compile(op->agg_expr.data, op->agg_expr.size,
                sess ? sess->intern : NULL);

    /* Row scratch, hoisted out of the loop (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, in->ncols)) {
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    typedef struct {
        uint64_t hash;
        uint32_t row;
    } reduce_group_slot_t;
    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)(in->nrows ? in->nrows : 1) * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired) {
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }
    reduce_group_slot_t *groups = (reduce_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups) {
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
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
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }
    uint32_t map_mask = map_cap - 1;

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
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce);
                    free(sums);
                    free(counts);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
            } else {
                int64_t val = 0;
                if (wl_columnar_expr_eval_i64(op->agg_expr.data,
                    op->agg_expr.size,
                    row, in->ncols, &val, sess->intern) == 0) {
                    agg_val = val;
                } else {
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce);
                    free(sums);
                    free(counts);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
            }
        }
        if (float_agg && !wl_columnar_float_bits_valid(agg_val)) {
            free(sums); free(counts); free(groups);
            col_row_buf_release(&row_rb);
            wl_columnar_expr_compiled_free(agg_ce); free(tmp);
            col_rel_destroy(out);
            if (e.owned) col_rel_destroy(in);
            return EINVAL;
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
                col_rel_set(out, group_row, agg_index, cur + 1);
                break;
            case WIRELOG_AGG_SUM:
            {
                if (float_agg) {
                    double value = wl_columnar_float_from_bits(agg_val);
                    double next_value = sums[slot] + value;
                    if (!isfinite(next_value)) {
                        free(sums); free(counts); free(groups);
                        col_row_buf_release(&row_rb);
                        wl_columnar_expr_compiled_free(agg_ce); free(tmp);
                        col_rel_destroy(out);
                        if (e.owned) col_rel_destroy(in);
                        return ERANGE;
                    }
                    sums[slot] = next_value;
                    (void)col_rel_set(out, group_row, agg_index,
                        (int64_t)wl_columnar_float_canonical_bits(
                            wl_columnar_float_to_bits(next_value)));
                    break;
                }
                int64_t next;
                if (wl_columnar_arithmetic_checked_add_int64(cur, agg_val,
                    &next) != 0) {
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce);
                    free(sums);
                    free(counts);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
                col_rel_set(out, group_row, agg_index, next);
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
                        (void)col_rel_set(out, group_row, agg_index,
                            (int64_t)wl_columnar_float_canonical_bits(agg_val));
                } else if (col_agg_better(op->agg_fn, op->agg_operand_type,
                    sess->intern, agg_val, cur))
                    col_rel_set(out, group_row, agg_index, agg_val);
                break;
            case WIRELOG_AGG_AVG:
                if (!float_agg)
                    break;
                sums[slot] += wl_columnar_float_from_bits(agg_val);
                counts[slot]++;
                if (!isfinite(sums[slot])) {
                    free(sums); free(counts); free(groups);
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce); free(tmp);
                    col_rel_destroy(out);
                    if (e.owned) col_rel_destroy(in);
                    return ERANGE;
                }
                (void)col_rel_set(out, group_row, agg_index,
                    (int64_t)wl_columnar_float_canonical_bits(
                        wl_columnar_float_to_bits(sums[slot]
                        / (double)counts[slot])));
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
            int rc = col_rel_append_row(out, tmp);
            if (rc != 0) {
                col_row_buf_release(&row_rb);
                wl_columnar_expr_compiled_free(agg_ce);
                free(sums);
                free(counts);
                free(groups);
                free(tmp);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(in);
                return rc;
            }
            groups[slot].hash = hash;
            groups[slot].row = out->nrows - 1;
        }
    }

    col_row_buf_release(&row_rb);
    wl_columnar_expr_compiled_free(agg_ce);
    free(groups);
    free(sums);
    free(counts);
    free(tmp);
    if (e.owned)
        col_rel_destroy(in);
    return eval_stack_push(stack, out, true);
}

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

    /* Allocate timestamp tracking on dst if not already present. */
    if (!dst->timestamps) {
        dst->timestamps
            = (col_delta_timestamp_t *)calloc(1, sizeof(col_delta_timestamp_t));
        if (!dst->timestamps)
            return ENOMEM;
        dst->capacity = (dst->capacity == 0) ? 1 : dst->capacity;
    }

    /* Allocate column buffers for one output row if not already present. */
    if (!dst->columns) {
        uint32_t ncols = dst->ncols ? dst->ncols : 1;
        dst->columns = col_columns_alloc(ncols, 1);
        if (!dst->columns)
            return ENOMEM;
        /* Zero-initialize the single row */
        for (uint32_t c = 0; c < ncols; c++)
            dst->columns[c][0] = 0;
        dst->capacity = 1;
    }

    /* Write the single aggregate row. */
    col_rel_set(dst, 0, 0, total);
    dst->nrows = 1;

    /* Set output row multiplicity. */
    memset(&dst->timestamps[0], 0, sizeof(col_delta_timestamp_t));
    dst->timestamps[0].multiplicity = total;

    return 0;
}

/* ======================================================================== */
/* LFTJ Operator (Issue #195)                                               */
/* ======================================================================== */

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
} lftj_binary_ctx_t;

/*
 * lftj_binary_cb: output callback for col_op_lftj.
 *
 * Converts compact LFTJ output to binary-join-compatible format and appends
 * the result to ctx->out.  Sets ctx->rc on allocation failure (subsequent
 * calls are no-ops).
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
    int rc = col_rel_append_row(ctx->out, ctx->tmp);
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
    if (!op->opaque_data)
        return EINVAL;
    const wl_plan_op_lftj_t *meta = (const wl_plan_op_lftj_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k < 2u || !meta->rel_names || !meta->key_cols)
        return EINVAL;

    /* Allocate per-relation working arrays. */
    wl_lftj_input_t *inputs
        = (wl_lftj_input_t *)calloc(k, sizeof(wl_lftj_input_t));
    uint32_t *ncols = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *lftj_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *binary_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!inputs || !ncols || !lftj_offsets || !binary_offsets) {
        free(inputs);
        free(ncols);
        free(lftj_offsets);
        free(binary_offsets);
        return ENOMEM;
    }

    /* Resolve each relation and populate LFTJ input descriptors. */
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

        /* Use the pre-sorted arrangement when available: wl_lftj_join still
         * copies and sorts internally, but starting from a sorted copy
         * reduces its qsort from O(N log N) to O(N). */
        col_sorted_arr_t *sarr
            = col_session_get_sorted_arrangement(sess, meta->rel_names[i], kc);
        if (sarr && sarr->indexed_rows == rel->nrows && sarr->nrows > 0) {
            inputs[i].data = sarr->sorted;
            inputs[i].nrows = sarr->nrows;
        } else {
            /* Gather column-major into flat buffer for LFTJ */
            int64_t *flat = (int64_t *)malloc(
                (size_t)rel->nrows * rel->ncols * sizeof(int64_t));
            if (!flat) {
                /* Free previously allocated flat buffers */
                for (uint32_t j = 0; j < i; j++) {
                    if (inputs[j].data != NULL) {
                        col_sorted_arr_t *prev_sarr
                            = col_session_get_sorted_arrangement(sess,
                                meta->rel_names[j], meta->key_cols[j]);
                        if (!(prev_sarr
                            && prev_sarr->indexed_rows
                            == inputs[j].nrows
                            && prev_sarr->nrows > 0)) {
                            free((void *)inputs[j].data);
                            inputs[j].data = NULL;
                        }
                    }
                }
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
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$lftj",
                total_binary_ncols);
        int64_t *tmp = (int64_t *)malloc(
            (total_binary_ncols ? total_binary_ncols : 1u) * sizeof(int64_t));
        if (!out || !tmp) {
            free(tmp);
            if (out)
                col_rel_destroy(out);
            rc = ENOMEM;
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
                                  0 };

        rc = wl_columnar_lftj_join_typed(inputs, key_type, k, lftj_binary_cb,
                &ctx);
        if (rc == 0)
            rc = ctx.rc;

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
    /* Free flat buffers allocated for non-sarr LFTJ inputs */
    if (inputs) {
        for (uint32_t i = 0; i < k; i++) {
            if (inputs[i].data) {
                col_sorted_arr_t *sarr2
                    = col_session_get_sorted_arrangement(sess,
                        meta->rel_names[i], meta->key_cols[i]);
                if (!(sarr2 && sarr2->sorted == inputs[i].data))
                    free((void *)inputs[i].data);
            }
        }
    }
    free(inputs);
    free(ncols);
    free(lftj_offsets);
    free(binary_offsets);
    return rc;
}

/* --- DIFFERENTIAL CONSOLIDATE is implemented in columnar/diff.c. */
/* --- JOIN and ANTIJOIN are implemented in columnar/join.c. */
