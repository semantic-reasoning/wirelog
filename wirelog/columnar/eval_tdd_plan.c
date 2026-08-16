/*
 * columnar/eval_tdd_plan.c - TDD plan analysis helpers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD eligibility and plan-shape analysis extracted from columnar/eval.c.
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * is_stratum_idb:
 * Returns true if the given relation name is an IDB in this stratum.
 */
static bool
is_stratum_idb(const wl_plan_stratum_t *sp, const char *name)
{
    if (!name)
        return false;
    for (uint32_t rj = 0; rj < sp->relation_count; rj++) {
        if (strcmp(name, sp->relations[rj].name) == 0)
            return true;
    }
    return false;
}

/*
 * LFTJ plans currently contain only EDB operands.  Keep that invariant
 * explicit at the TDD boundary: a hand-built or future plan with an IDB
 * operand must take the conservative path rather than being treated as an
 * EDB-only operator by the linear TDD scans.
 */
static bool
tdd_lftj_meta_valid(const wl_plan_op_lftj_t *meta)
{
    if (!meta || meta->k < 3 || !meta->rel_names || !meta->key_cols)
        return false;
    for (uint32_t i = 0; i < meta->k; i++) {
        if (!meta->rel_names[i])
            return false;
    }
    return true;
}

static uint32_t
tdd_lftj_idb_count(const wl_plan_op_lftj_t *meta,
    const wl_plan_stratum_t *sp)
{
    if (!tdd_lftj_meta_valid(meta))
        return UINT32_MAX;
    uint32_t count = 0;
    for (uint32_t i = 0; i < meta->k; i++) {
        if (is_stratum_idb(sp, meta->rel_names[i]))
            count++;
    }
    return count;
}

static bool
tdd_ops_have_unsupported_lftj(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp)
{
    if (!ops)
        return false;
    for (uint32_t i = 0; i < op_count; i++) {
        const wl_plan_op_t *op = &ops[i];
        if (op->op == WL_PLAN_OP_LFTJ) {
            const wl_plan_op_lftj_t *meta =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            if (tdd_lftj_idb_count(meta, sp) != 0)
                return true;
        } else if (op->op == WL_PLAN_OP_K_FUSION) {
            if (!op->opaque_data)
                return true;
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)op->opaque_data;
            if (kf->k == 0 || !kf->k_ops || !kf->k_op_counts)
                return true;
            for (uint32_t k = 0; k < kf->k; k++) {
                if (kf->k_op_counts[k] > 0 && !kf->k_ops[k])
                    return true;
                if (tdd_ops_have_unsupported_lftj(kf->k_ops[k],
                    kf->k_op_counts[k], sp))
                    return true;
            }
        }
    }
    return false;
}

bool
tdd_stratum_has_unsupported_lftj(const wl_plan_stratum_t *sp)
{
    if (!sp)
        return true;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        if (tdd_ops_have_unsupported_lftj(sp->relations[ri].ops,
            sp->relations[ri].op_count, sp))
            return true;
    }
    return false;
}

/*
 * ops_have_idb_idb_join:
 * Walk an op sequence tracking whether the eval stack top derives from IDB.
 * Returns true if any JOIN has BOTH IDB-derived left input AND IDB right.
 * VARIABLE resets the tracker; JOIN with IDB right propagates it.
 */
static bool
ops_have_idb_idb_join(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp)
{
    if (!ops)
        return op_count != 0;

    bool stack_has_idb = false;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE) {
            stack_has_idb = is_stratum_idb(sp, op->relation_name);
        } else if (op->op == WL_PLAN_OP_JOIN && op->right_relation) {
            bool right_idb = is_stratum_idb(sp, op->right_relation);
            if (right_idb && stack_has_idb)
                return true;
            if (right_idb)
                stack_has_idb = true;
        } else if (op->op == WL_PLAN_OP_LFTJ) {
            const wl_plan_op_lftj_t *meta =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            uint32_t idb_count = tdd_lftj_idb_count(meta, sp);
            if (idb_count == UINT32_MAX || idb_count >= 2)
                return true;
            if (idb_count == 1)
                stack_has_idb = true;
        }
    }
    return false;
}

static bool
op_references_stratum_idb(const wl_plan_op_t *op,
    const wl_plan_stratum_t *sp);

static uint32_t
ops_max_idb_segment_body_atoms(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp)
{
    uint32_t max_count = 0;
    uint32_t segment_count = 0;
    uint32_t depth = 0;

    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE && segment_count > 0) {
            if (segment_count > max_count)
                max_count = segment_count;
            segment_count = 0;
        }

        if (op->op == WL_PLAN_OP_LFTJ) {
            const wl_plan_op_lftj_t *meta =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            uint32_t idb_count = tdd_lftj_idb_count(meta, sp);
            if (idb_count == UINT32_MAX
                || UINT32_MAX - segment_count < idb_count)
                segment_count = UINT32_MAX;
            else
                segment_count += idb_count;
        } else if (op_references_stratum_idb(op, sp)) {
            segment_count++;
        }

        if (op->op == WL_PLAN_OP_VARIABLE) {
            depth++;
        } else if (op->op == WL_PLAN_OP_CONCAT && depth > 0) {
            depth--;
        }
    }

    if (segment_count > max_count)
        max_count = segment_count;
    return max_count;
}

/*
 * stratum_max_idb_body_atoms:
 * Walk all relations in a stratum (including K_FUSION children) and
 * return the maximum number of IDB body atoms across all rules.
 * Used as a static guard: BDX mode is only correct for rules with
 * at most 2 IDB body atoms.
 */
uint32_t
stratum_max_idb_body_atoms(const wl_plan_stratum_t *sp)
{
    uint32_t max_count = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        /* Check top-level ops */
        uint32_t c = ops_max_idb_segment_body_atoms(rel->ops,
                rel->op_count, sp);
        if (c > max_count)
            max_count = c;

        /* Check ops inside K_FUSION */
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    c = ops_max_idb_segment_body_atoms(kf->k_ops[ki],
                            kf->k_op_counts[ki], sp);
                    if (c > max_count)
                        max_count = c;
                }
            }
        }
    }
    return max_count;
}

static bool
op_references_stratum_idb(const wl_plan_op_t *op,
    const wl_plan_stratum_t *sp)
{
    if (op->op == WL_PLAN_OP_VARIABLE)
        return is_stratum_idb(sp, op->relation_name);
    if ((op->op == WL_PLAN_OP_JOIN
        || op->op == WL_PLAN_OP_SEMIJOIN
        || op->op == WL_PLAN_OP_ANTIJOIN)
        && op->right_relation)
        return is_stratum_idb(sp, op->right_relation);
    if (op->op == WL_PLAN_OP_LFTJ) {
        const wl_plan_op_lftj_t *meta =
            (const wl_plan_op_lftj_t *)op->opaque_data;
        return tdd_lftj_idb_count(meta, sp) != 0;
    }
    return false;
}

static uint32_t
ops_max_idb_segment_join_like(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp)
{
    uint32_t max_count = 0;
    uint32_t segment_count = 0;
    uint32_t depth = 0;
    bool segment_has_idb = false;

    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE
            && (segment_count > 0 || segment_has_idb)) {
            if (segment_has_idb && segment_count > max_count)
                max_count = segment_count;
            segment_count = 0;
            segment_has_idb = false;
        }

        if (op_references_stratum_idb(op, sp))
            segment_has_idb = true;

        switch (op->op) {
        case WL_PLAN_OP_JOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            segment_count++;
            break;
        case WL_PLAN_OP_LFTJ: {
            const wl_plan_op_lftj_t *meta =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            uint32_t idb_count = tdd_lftj_idb_count(meta, sp);
            if (idb_count == UINT32_MAX)
                return UINT32_MAX;
            if (idb_count > 0 && meta->k > 1)
                segment_count += meta->k - 1;
            break;
        }
        default:
            break;
        }

        if (op->op == WL_PLAN_OP_VARIABLE) {
            depth++;
        } else if (op->op == WL_PLAN_OP_CONCAT && depth > 0) {
            depth--;
        }
    }

    if (segment_has_idb && segment_count > max_count)
        max_count = segment_count;
    return max_count;
}

static uint32_t
tdd_stratum_global_read_max_idb_atoms(const wl_plan_stratum_t *sp)
{
    uint32_t max_count = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];
        bool has_kfusion = false;
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op != WL_PLAN_OP_K_FUSION
                || !rel->ops[oi].opaque_data)
                continue;
            has_kfusion = true;
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
            for (uint32_t ki = 0; ki < kf->k; ki++) {
                uint32_t c = ops_max_idb_segment_body_atoms(kf->k_ops[ki],
                        kf->k_op_counts[ki], sp);
                if (c > max_count)
                    max_count = c;
            }
        }
        if (!has_kfusion) {
            uint32_t c = ops_max_idb_segment_body_atoms(rel->ops,
                    rel->op_count, sp);
            if (c > max_count)
                max_count = c;
        }
    }
    return max_count;
}

static bool
tdd_relation_has_exchange_key(const wl_plan_relation_t *rel)
{
    for (uint32_t oi = 0; oi < rel->op_count; oi++) {
        if (rel->ops[oi].op != WL_PLAN_OP_EXCHANGE)
            continue;
        const wl_plan_op_exchange_t *meta =
            (const wl_plan_op_exchange_t *)rel->ops[oi].opaque_data;
        return meta && meta->key_col_idxs && meta->key_col_count > 0;
    }
    return false;
}

static void
tdd_record_segment_stats(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp, bool relation_has_exchange,
    wl_tdd_segment_stats_t *stats)
{
    if (!ops || op_count == 0 || !stats)
        return;

    uint32_t idb_atoms = 0;
    uint32_t join_like = 0;
    bool has_idb = false;
    bool has_antijoin = false;

    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op_references_stratum_idb(op, sp)) {
            has_idb = true;
            idb_atoms++;
        }
        switch (op->op) {
        case WL_PLAN_OP_JOIN:
        case WL_PLAN_OP_SEMIJOIN:
            join_like++;
            break;
        case WL_PLAN_OP_ANTIJOIN:
            join_like++;
            has_antijoin = true;
            break;
        case WL_PLAN_OP_LFTJ: {
            const wl_plan_op_lftj_t *meta =
                (const wl_plan_op_lftj_t *)op->opaque_data;
            uint32_t idb_count = tdd_lftj_idb_count(meta, sp);
            if (idb_count == UINT32_MAX) {
                stats->unsafe_segments++;
                return;
            }
            if (meta->k > 1)
                join_like += meta->k - 1;
            break;
        }
        default:
            break;
        }
    }

    stats->total_segments++;
    if (idb_atoms > stats->max_segment_idb_atoms)
        stats->max_segment_idb_atoms = idb_atoms;
    if (join_like > stats->max_segment_join_like)
        stats->max_segment_join_like = join_like;

    if (!has_idb) {
        stats->seed_only_segments++;
        return;
    }

    if (relation_has_exchange
        && idb_atoms <= 2
        && join_like <= 4
        && !has_antijoin
        && !ops_have_idb_idb_join(ops, op_count, sp)) {
        stats->global_read_segments++;
    } else {
        stats->unsafe_segments++;
    }
}

static void
tdd_visit_rule_segments(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp, bool relation_has_exchange,
    wl_tdd_segment_stats_t *stats)
{
    if (!ops || op_count == 0)
        return;

    for (uint32_t oi = 0; oi < op_count;) {
        while (oi < op_count && ops[oi].op != WL_PLAN_OP_VARIABLE)
            oi++;
        if (oi >= op_count)
            break;

        uint32_t seg_start = oi++;
        while (oi < op_count
            && ops[oi].op != WL_PLAN_OP_VARIABLE
            && ops[oi].op != WL_PLAN_OP_CONCAT
            && ops[oi].op != WL_PLAN_OP_CONSOLIDATE) {
            oi++;
        }
        if (oi > seg_start) {
            tdd_record_segment_stats(ops + seg_start, oi - seg_start, sp,
                relation_has_exchange, stats);
        }
        if (oi < op_count && ops[oi].op == WL_PLAN_OP_CONCAT) {
            oi++;
            continue;
        }
    }
}

static bool
tdd_rule_slice_global_read_safe(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp, bool relation_has_exchange)
{
    uint32_t idb_atoms = 0;
    uint32_t join_like = 0;
    bool has_antijoin = false;
    bool recursive_semijoin = false;

    if (!relation_has_exchange || !ops || op_count == 0)
        return false;

    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->delta_mode == WL_DELTA_FORCE_EMPTY)
            continue;
        if (op_references_stratum_idb(op, sp))
            idb_atoms++;
        switch (op->op) {
        case WL_PLAN_OP_JOIN:
            join_like++;
            break;
        case WL_PLAN_OP_SEMIJOIN:
            join_like++;
            if (op->right_relation
                && is_stratum_idb(sp, op->right_relation))
                recursive_semijoin = true;
            break;
        case WL_PLAN_OP_ANTIJOIN:
            join_like++;
            has_antijoin = true;
            break;
        default:
            break;
        }
    }

    return idb_atoms == 1
           && join_like <= 4
           && !has_antijoin
           && !recursive_semijoin
           && !ops_have_idb_idb_join(ops, op_count, sp);
}

static bool
tdd_child_plan_global_read_safe(const wl_plan_op_t *ops, uint32_t op_count,
    const wl_plan_stratum_t *sp, bool relation_has_exchange)
{
    if (!ops || op_count == 0)
        return false;

    uint32_t idb_segments = 0;
    for (uint32_t oi = 0; oi < op_count;) {
        while (oi < op_count && ops[oi].op != WL_PLAN_OP_VARIABLE)
            oi++;
        if (oi >= op_count)
            break;

        uint32_t seg_start = oi++;
        while (oi < op_count
            && ops[oi].op != WL_PLAN_OP_VARIABLE
            && ops[oi].op != WL_PLAN_OP_CONCAT
            && ops[oi].op != WL_PLAN_OP_CONSOLIDATE) {
            oi++;
        }

        if (ops[seg_start].delta_mode == WL_DELTA_FORCE_EMPTY) {
            if (oi < op_count && ops[oi].op == WL_PLAN_OP_CONCAT)
                oi++;
            continue;
        }

        bool has_idb = false;
        for (uint32_t si = seg_start; si < oi; si++) {
            if (ops[si].delta_mode == WL_DELTA_FORCE_EMPTY)
                continue;
            if (op_references_stratum_idb(&ops[si], sp)) {
                has_idb = true;
                break;
            }
        }
        if (has_idb) {
            if (!tdd_rule_slice_global_read_safe(ops + seg_start,
                oi - seg_start, sp, relation_has_exchange))
                return false;
            idb_segments++;
        }

        if (oi < op_count && ops[oi].op == WL_PLAN_OP_CONCAT)
            oi++;
    }

    return idb_segments == 1;
}

static int
tdd_rule_slices_append(wl_tdd_rule_slice_t **slices, uint32_t *count,
    uint32_t *cap, uint32_t relation_index, const wl_plan_op_t *ops,
    uint32_t op_count, bool tdd_safe)
{
    if (op_count == 0)
        return 0;
    if (*count == *cap) {
        uint32_t new_cap = *cap == 0 ? 32 : *cap * 2;
        wl_tdd_rule_slice_t *new_slices =
            (wl_tdd_rule_slice_t *)realloc(*slices,
                (size_t)new_cap * sizeof(wl_tdd_rule_slice_t));
        if (!new_slices)
            return ENOMEM;
        *slices = new_slices;
        *cap = new_cap;
    }

    (*slices)[*count].relation_index = relation_index;
    (*slices)[*count].ops = ops;
    (*slices)[*count].op_count = op_count;
    (*slices)[*count].tdd_safe = tdd_safe;
    (*count)++;
    return 0;
}

static int
tdd_build_rule_slices(const wl_plan_stratum_t *sp,
    wl_tdd_rule_slice_t **out_slices, uint32_t *out_count,
    uint32_t *out_safe_count)
{
    wl_tdd_rule_slice_t *slices = NULL;
    uint32_t count = 0;
    uint32_t cap = 0;
    uint32_t safe_count = 0;

    *out_slices = NULL;
    *out_count = 0;
    *out_safe_count = 0;
    if (!sp)
        return 0;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];
        bool has_kfusion = false;
        bool has_exchange = tdd_relation_has_exchange_key(rel);

        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op != WL_PLAN_OP_K_FUSION
                || !rel->ops[oi].opaque_data)
                continue;
            has_kfusion = true;
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
            for (uint32_t ki = 0; ki < kf->k; ki++) {
                bool safe = tdd_child_plan_global_read_safe(kf->k_ops[ki],
                        kf->k_op_counts[ki], sp, has_exchange);
                int rc = tdd_rule_slices_append(&slices, &count, &cap, ri,
                        kf->k_ops[ki], kf->k_op_counts[ki], safe);
                if (rc != 0) {
                    free(slices);
                    return rc;
                }
            }
        }

        if (!has_kfusion) {
            bool safe = tdd_child_plan_global_read_safe(rel->ops,
                    rel->op_count, sp, has_exchange);
            int rc = tdd_rule_slices_append(&slices, &count, &cap, ri,
                    rel->ops, rel->op_count, safe);
            if (rc != 0) {
                free(slices);
                return rc;
            }
        }
    }

    for (uint32_t i = 0; i < count; i++) {
        if (slices[i].tdd_safe)
            safe_count++;
    }
    *out_slices = slices;
    *out_count = count;
    *out_safe_count = safe_count;
    return 0;
}

bool
tdd_stratum_mixed_slice_candidate(const wl_plan_stratum_t *sp)
{
    if (tdd_stratum_has_unsupported_lftj(sp))
        return false;
    wl_tdd_rule_slice_t *slices = NULL;
    uint32_t count = 0;
    uint32_t safe_count = 0;
    int rc = tdd_build_rule_slices(sp, &slices, &count, &safe_count);
    free(slices);
    return rc == 0 && safe_count > 0 && safe_count < count;
}

void
tdd_stratum_segment_stats(const wl_plan_stratum_t *sp,
    wl_tdd_segment_stats_t *stats)
{
    if (!stats)
        return;
    memset(stats, 0, sizeof(*stats));
    if (!sp)
        return;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];
        bool has_kfusion = false;
        bool has_exchange = tdd_relation_has_exchange_key(rel);

        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op != WL_PLAN_OP_K_FUSION
                || !rel->ops[oi].opaque_data)
                continue;
            has_kfusion = true;
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
            for (uint32_t ki = 0; ki < kf->k; ki++) {
                tdd_visit_rule_segments(kf->k_ops[ki],
                    kf->k_op_counts[ki], sp, has_exchange, stats);
            }
        }

        if (!has_kfusion) {
            tdd_visit_rule_segments(rel->ops, rel->op_count, sp,
                has_exchange, stats);
        }
    }
}

bool
tdd_stratum_global_read_candidate(const wl_plan_stratum_t *sp)
{
    if (!sp)
        return false;
    if (tdd_stratum_has_unsupported_lftj(sp))
        return false;
    if (tdd_stratum_has_idb_self_join(sp))
        return false;
    uint32_t max_idb_atoms = tdd_stratum_global_read_max_idb_atoms(sp);
    if (max_idb_atoms > 2)
        return false;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];
        if (!tdd_relation_has_exchange_key(rel))
            return false;
        uint32_t top_joins = ops_max_idb_segment_join_like(rel->ops,
                rel->op_count, sp);
        if (top_joins > 4)
            return false;

        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op != WL_PLAN_OP_K_FUSION
                || !rel->ops[oi].opaque_data)
                continue;
            const wl_plan_op_k_fusion_t *kf =
                (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
            for (uint32_t ki = 0; ki < kf->k; ki++) {
                uint32_t child_joins = ops_max_idb_segment_join_like(
                    kf->k_ops[ki], kf->k_op_counts[ki], sp);
                if (child_joins > 4)
                    return false;
            }
        }
    }
    return true;
}

static uint32_t
tdd_parse_col_index(const char *key)
{
    if (!key || key[0] != 'c' || key[1] != 'o' || key[2] != 'l')
        return UINT32_MAX;
    char *end = NULL;
    unsigned long v = strtoul(key + 3, &end, 10);
    if (end == key + 3 || *end != '\0' || v > UINT32_MAX)
        return UINT32_MAX;
    return (uint32_t)v;
}

static bool
tdd_find_relation_exchange_key(const wl_plan_stratum_t *sp, const char *name,
    const uint32_t **key_cols, uint32_t *key_count)
{
    *key_cols = NULL;
    *key_count = 0;
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        if (strcmp(sp->relations[ri].name, name) != 0)
            continue;
        for (uint32_t oi = 0; oi < sp->relations[ri].op_count; oi++) {
            if (sp->relations[ri].ops[oi].op != WL_PLAN_OP_EXCHANGE)
                continue;
            const wl_plan_op_exchange_t *meta =
                (const wl_plan_op_exchange_t *)
                sp->relations[ri].ops[oi].opaque_data;
            if (!meta || !meta->key_col_idxs || meta->key_col_count == 0)
                return false;
            *key_cols = meta->key_col_idxs;
            *key_count = meta->key_col_count;
            return true;
        }
        return false;
    }
    return false;
}

static bool
tdd_key_names_match_exchange(const char *const *keys, uint32_t key_count,
    const uint32_t *exchange_cols, uint32_t exchange_count)
{
    if (!keys || !exchange_cols || key_count == 0
        || key_count != exchange_count)
        return false;
    for (uint32_t k = 0; k < key_count; k++) {
        uint32_t idx = tdd_parse_col_index(keys[k]);
        if (idx == UINT32_MAX)
            return false;
        bool found = false;
        for (uint32_t x = 0; x < exchange_count; x++) {
            if (exchange_cols[x] == idx) {
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }
    return true;
}

static bool
tdd_ops_single_idb_keys_exchange_aligned(const wl_plan_op_t *ops,
    uint32_t op_count, const wl_plan_stratum_t *sp)
{
    bool stack_has_idb = false;
    const char *stack_idb_name = NULL;

    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE) {
            if (is_stratum_idb(sp, op->relation_name)) {
                stack_has_idb = true;
                stack_idb_name = op->relation_name;
            } else {
                stack_has_idb = false;
                stack_idb_name = NULL;
            }
        } else if (op->op == WL_PLAN_OP_JOIN && op->right_relation) {
            bool right_idb = is_stratum_idb(sp, op->right_relation);
            if (stack_has_idb && stack_idb_name) {
                const uint32_t *xkey = NULL;
                uint32_t xkey_count = 0;
                if (!tdd_find_relation_exchange_key(sp, stack_idb_name,
                    &xkey, &xkey_count))
                    return false;
                if (!tdd_key_names_match_exchange(op->left_keys,
                    op->key_count, xkey, xkey_count))
                    return false;
            }
            if (right_idb) {
                const uint32_t *xkey = NULL;
                uint32_t xkey_count = 0;
                if (!tdd_find_relation_exchange_key(sp, op->right_relation,
                    &xkey, &xkey_count))
                    return false;
                if (!tdd_key_names_match_exchange(op->right_keys,
                    op->key_count, xkey, xkey_count))
                    return false;
                stack_has_idb = true;
                stack_idb_name = op->right_relation;
            }
        }
    }
    return true;
}

bool
tdd_stratum_single_idb_join_keys_exchange_aligned(
    const wl_plan_stratum_t *sp)
{
    if (tdd_stratum_has_unsupported_lftj(sp))
        return false;
    if (tdd_stratum_has_idb_self_join(sp))
        return false;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        if (!tdd_ops_single_idb_keys_exchange_aligned(
                rel->ops, rel->op_count, sp))
            return false;

        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    if (!tdd_ops_single_idb_keys_exchange_aligned(
                            kf->k_ops[ki], kf->k_op_counts[ki], sp))
                        return false;
                }
            }
        }
    }
    return true;
}

/*
 * tdd_stratum_has_idb_self_join:
 * Returns true if any rule in the stratum has a JOIN where BOTH the left
 * input (from VARIABLE or previous JOIN) AND the right_relation are IDB.
 * Only these true IDB-IDB joins (e.g. CSPA's valueFlow join valueFlow)
 * require full replication.  EDB-IDB joins (e.g. CRDT's insert join
 * nextSiblingAnc) work correctly with data partitioning (partition IDB,
 * replicate EDB).
 */
bool
tdd_stratum_has_idb_self_join(const wl_plan_stratum_t *sp)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        /* Check top-level ops */
        if (ops_have_idb_idb_join(rel->ops, rel->op_count, sp))
            return true;

        /* Check ops inside K_FUSION */
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    if (ops_have_idb_idb_join(kf->k_ops[ki],
                        kf->k_op_counts[ki], sp))
                        return true;
                }
            }
        }
    }
    return false;
}

/*
 * idb_idb_join_right_keys_match_exchange:
 * Walk an op sequence.  For each IDB-IDB JOIN found, verify that every
 * right_key column name resolves to a column index that is listed in the
 * EXCHANGE key_col_idxs of the right relation.
 *
 * Returns false as soon as any IDB-IDB JOIN is found whose right_keys do
 * NOT match the EXCHANGE partition key — meaning cross-partition joins would
 * occur and asymmetric init is unsafe.
 *
 * Returns true if every IDB-IDB JOIN in this op sequence is exchange-aligned
 * (or if there are no IDB-IDB JOINs at all).
 */
static bool
idb_idb_join_right_keys_match_exchange(const wl_plan_op_t *ops,
    uint32_t op_count, const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    bool stack_has_idb = false;
    for (uint32_t oi = 0; oi < op_count; oi++) {
        const wl_plan_op_t *op = &ops[oi];
        if (op->op == WL_PLAN_OP_VARIABLE) {
            stack_has_idb = is_stratum_idb(sp, op->relation_name);
        } else if (op->op == WL_PLAN_OP_JOIN && op->right_relation) {
            bool right_idb = is_stratum_idb(sp, op->right_relation);
            if (right_idb && stack_has_idb) {
                /* Found an IDB-IDB join.  For asymmetric partition-replicate to
                 * be correct, BOTH the left join key AND the right join key
                 * must equal the EXCHANGE partition key.  If left_keys and
                 * right_keys are both "col0" (e.g. vA:-vF(z,x),vF(z,y)),
                 * each worker's partition is self-contained.  If left_key
                 * is "col1" and right_key is "col0" (e.g. TC r:-r(x,y),r(y,z)),
                 * cross-partition joins are needed and replication is required.
                 */
                if (!op->right_keys || !op->left_keys || op->key_count == 0)
                    return false;

                /* Find the EXCHANGE key for the right relation. */
                const uint32_t *xkey = NULL;
                uint32_t xkey_count = 0;
                for (uint32_t rj = 0; rj < sp->relation_count; rj++) {
                    if (strcmp(sp->relations[rj].name, op->right_relation) != 0)
                        continue;
                    for (uint32_t oj = 0; oj < sp->relations[rj].op_count;
                        oj++) {
                        if (sp->relations[rj].ops[oj].op ==
                            WL_PLAN_OP_EXCHANGE) {
                            const wl_plan_op_exchange_t *meta =
                                (const wl_plan_op_exchange_t *)
                                sp->relations[rj].ops[oj].opaque_data;
                            if (meta && meta->key_col_count > 0) {
                                xkey = meta->key_col_idxs;
                                xkey_count = meta->key_col_count;
                            }
                            break;
                        }
                    }
                    break;
                }

                if (!xkey || xkey_count == 0)
                    return false; /* No EXCHANGE key: cannot verify alignment */
                if (xkey_count != op->key_count)
                    return false; /* Key count mismatch */

                /* Resolve right_keys (column names) to indices via the
                 * coordinator's relation schema, then compare to xkey.
                 * Also verify left_keys against the same xkey: for the join
                 * to be fully local, BOTH left and right join keys must match
                 * the EXCHANGE partition key.  Example: for TC r:-r(x,y),r(y,z)
                 * right_key="col0" matches but left_key="col1" does not, so
                 * workers would need cross-partition data. */
                col_rel_t *rrel = session_find_rel(coord, op->right_relation);
                if (!rrel || !rrel->col_names || rrel->ncols == 0)
                    return false; /* No schema: cannot verify */

                for (uint32_t k = 0; k < op->key_count; k++) {
                    /* Check right_key against EXCHANGE key */
                    const char *rkname = op->right_keys[k];
                    uint32_t rcidx = UINT32_MAX;
                    for (uint32_t c = 0; c < rrel->ncols; c++) {
                        if (rrel->col_names[c]
                            && strcmp(rrel->col_names[c], rkname) == 0) {
                            rcidx = c;
                            break;
                        }
                    }
                    if (rcidx == UINT32_MAX)
                        return false; /* Right column not found */
                    bool rfound = false;
                    for (uint32_t xk = 0; xk < xkey_count; xk++) {
                        if (xkey[xk] == rcidx) {
                            rfound = true;
                            break;
                        }
                    }
                    if (!rfound)
                        return false; /* Right join key not in EXCHANGE key */

                    /* Check left_key against xkey (using same column index
                     * space — left relation is the same IDB, same schema). */
                    const char *lkname = op->left_keys[k];
                    uint32_t lcidx = UINT32_MAX;
                    for (uint32_t c = 0; c < rrel->ncols; c++) {
                        if (rrel->col_names[c]
                            && strcmp(rrel->col_names[c], lkname) == 0) {
                            lcidx = c;
                            break;
                        }
                    }
                    if (lcidx == UINT32_MAX)
                        return false; /* Left column not found */
                    bool lfound = false;
                    for (uint32_t xk = 0; xk < xkey_count; xk++) {
                        if (xkey[xk] == lcidx) {
                            lfound = true;
                            break;
                        }
                    }
                    if (!lfound)
                        return false; /* Left join key not in EXCHANGE key */
                }
            }
            if (right_idb)
                stack_has_idb = true;
        }
    }
    return true;
}

/*
 * tdd_stratum_idb_self_join_exchange_aligned:
 * Returns true if the stratum has IDB self-joins AND all of them are
 * exchange-aligned (right_keys match the EXCHANGE partition key).
 *
 * When true, the stratum can use asymmetric partition-replicate:
 * each worker holds 1/W of the IDB (partitioned by EXCHANGE key),
 * and the delta is broadcast.  Joins are fully local because the join
 * key == partition key on both sides.
 *
 * When false (join key differs from partition key, e.g. transitive closure
 * r(x,z):-r(x,y),r(y,z) where join is on col1=col0), cross-partition joins
 * would occur with partitioned IDB, so replicate_mode must be used instead.
 */
bool
tdd_stratum_idb_self_join_exchange_aligned(const wl_plan_stratum_t *sp,
    wl_col_session_t *coord)
{
    if (!tdd_stratum_has_idb_self_join(sp))
        return false;

    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rel = &sp->relations[ri];

        if (!idb_idb_join_right_keys_match_exchange(
                rel->ops, rel->op_count, sp, coord))
            return false;

        /* Check inside K_FUSION */
        for (uint32_t oi = 0; oi < rel->op_count; oi++) {
            if (rel->ops[oi].op == WL_PLAN_OP_K_FUSION
                && rel->ops[oi].opaque_data) {
                const wl_plan_op_k_fusion_t *kf =
                    (const wl_plan_op_k_fusion_t *)rel->ops[oi].opaque_data;
                for (uint32_t ki = 0; ki < kf->k; ki++) {
                    if (!idb_idb_join_right_keys_match_exchange(
                            kf->k_ops[ki], kf->k_op_counts[ki], sp, coord))
                        return false;
                }
            }
        }
    }
    return true;
}
