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

/* Enumerate each relation's top-level sequence followed by its direct fusion
 * children. Predicates retain their own state and can stop before later
 * metadata is inspected. Other traversal policies deliberately stay separate. */
typedef struct {
    uint32_t relation;
    uint32_t op;
    uint32_t child;
    bool top_yielded;
    const wl_plan_op_t *ops;
    uint32_t count;
} wl_columnar_eval_tdd_plan_cursor_t;

static bool
wl_columnar_eval_tdd_plan_next(const wl_plan_stratum_t *sp,
    wl_columnar_eval_tdd_plan_cursor_t *cursor)
{
    while (cursor->relation < sp->relation_count) {
        const wl_plan_relation_t *rel = &sp->relations[cursor->relation];
        if (!cursor->top_yielded) {
            cursor->top_yielded = true;
            cursor->ops = rel->ops;
            cursor->count = rel->op_count;
            return true;
        }
        while (cursor->op < rel->op_count) {
            const wl_plan_op_t *op = &rel->ops[cursor->op];
            if (op->op == WL_PLAN_OP_K_FUSION && op->opaque_data) {
                const wl_plan_op_k_fusion_t *kf = op->opaque_data;
                if (cursor->child < kf->k) {
                    cursor->ops = kf->k_ops[cursor->child];
                    cursor->count = kf->k_op_counts[cursor->child];
                    cursor->child++;
                    return true;
                }
            }
            cursor->op++;
            cursor->child = 0;
        }
        cursor->relation++;
        cursor->op = 0;
        cursor->top_yielded = false;
    }
    return false;
}

/* Compare semantic payloads, not cloned pointers or structure padding. */
static bool
wl_columnar_eval_tdd_plan_string_equal(const char *a, const char *b)
{
    return a && b ? strcmp(a, b) == 0 : a == b;
}

static bool
wl_columnar_eval_tdd_plan_bytes_equal(const void *a, const void *b,
    size_t size)
{
    return size == 0 || (a && b && memcmp(a, b, size) == 0);
}

static bool
wl_columnar_eval_tdd_plan_expr_equal(wl_plan_expr_buffer_t a,
    wl_plan_expr_buffer_t b)
{
    return a.size == b.size
           && wl_columnar_eval_tdd_plan_bytes_equal(a.data, b.data, a.size);
}

static bool
wl_columnar_eval_tdd_plan_keys_equal(const wl_plan_op_t *a,
    const wl_plan_op_t *b)
{
    if (a->key_count != b->key_count)
        return false;
    for (uint32_t i = 0; i < a->key_count; i++) {
        if (!a->left_keys || !a->right_keys || !b->left_keys || !b->right_keys
            || !a->left_keys[i] || !a->right_keys[i]
            || !wl_columnar_eval_tdd_plan_string_equal(a->left_keys[i],
            b->left_keys[i])
            || !wl_columnar_eval_tdd_plan_string_equal(a->right_keys[i],
            b->right_keys[i]))
            return false;
    }
    return true;
}

static bool
wl_columnar_eval_tdd_plan_op_equal(const wl_plan_op_t *a,
    const wl_plan_op_t *b)
{
    if (a->op != b->op || a->opaque_data || b->opaque_data
        || !wl_columnar_eval_tdd_plan_string_equal(a->relation_name,
        b->relation_name)
        || !wl_columnar_eval_tdd_plan_string_equal(a->right_relation,
        b->right_relation)
        || !wl_columnar_eval_tdd_plan_keys_equal(a, b)
        || a->project_count != b->project_count
        || !wl_columnar_eval_tdd_plan_bytes_equal(a->project_indices,
        b->project_indices, (size_t)a->project_count * sizeof(uint32_t))
        || !wl_columnar_eval_tdd_plan_expr_equal(a->filter_expr, b->filter_expr)
        || !wl_columnar_eval_tdd_plan_expr_equal(a->right_filter_expr,
        b->right_filter_expr)
        || a->agg_fn != b->agg_fn || a->aggregate_index != b->aggregate_index
        || a->agg_operand_type != b->agg_operand_type ||
        a->agg_result_type != b->agg_result_type
        || !wl_columnar_eval_tdd_plan_expr_equal(a->agg_expr, b->agg_expr)
        || a->group_by_count != b->group_by_count
        || !wl_columnar_eval_tdd_plan_bytes_equal(a->group_by_indices,
        b->group_by_indices, (size_t)a->group_by_count * sizeof(uint32_t))
        || a->map_expr_count != b->map_expr_count)
        return false;
    for (uint32_t i = 0; i < a->map_expr_count; i++) {
        if (!a->map_exprs || !b->map_exprs
            || !wl_columnar_eval_tdd_plan_expr_equal(a->map_exprs[i],
            b->map_exprs[i]))
            return false;
    }
    return true;
}

static const char *
wl_columnar_eval_tdd_plan_operand(const wl_plan_op_t *op)
{
    if (op->op == WL_PLAN_OP_VARIABLE)
        return op->relation_name;
    if (op->op == WL_PLAN_OP_JOIN || op->op == WL_PLAN_OP_SEMIJOIN)
        return op->right_relation;
    return NULL;
}

static bool
wl_columnar_eval_tdd_plan_source(const wl_plan_op_t *op,
    const wl_plan_stratum_t *sp)
{
    return (op->op == WL_PLAN_OP_VARIABLE || op->op == WL_PLAN_OP_JOIN)
           && is_stratum_idb(sp, wl_columnar_eval_tdd_plan_operand(op));
}

/* Only the redundant, non-projecting prefilter immediately before the same
 * keyed JOIN is recognized. Other recursive SEMIJOIN shapes stay unsupported.
 * Its registry lookup reads FULL even when the following JOIN drives DELTA. */
static bool
wl_columnar_eval_tdd_plan_prefilter(const wl_plan_op_t *ops,
    uint32_t i, uint32_t end)
{
    const wl_plan_op_t *op = &ops[i];
    return i + 1 < end && ops[i + 1].op == WL_PLAN_OP_JOIN
           && op->key_count == 1 && op->project_count == 0
           && op->right_filter_expr.size == 0
           && ops[i + 1].right_filter_expr.size == 0
           && wl_columnar_eval_tdd_plan_string_equal(op->right_relation,
               ops[i + 1].right_relation)
           && wl_columnar_eval_tdd_plan_keys_equal(op, &ops[i + 1]);
}

static int
wl_columnar_eval_tdd_plan_slice(const wl_plan_stratum_t *sp,
    const wl_plan_op_t *ops, uint32_t base, uint32_t start, uint32_t end,
    uint32_t alternative, uint32_t ordinal,
    wl_columnar_eval_tdd_plan_manifest_t *manifest)
{
    uint32_t sources = 0, drivers = 0, driver = UINT32_MAX;
    for (uint32_t i = start; i < end; i++) {
        const wl_plan_op_t *op = &ops[base + i];
        if (wl_columnar_eval_tdd_plan_source(op, sp)) {
            sources++;
            if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
                drivers++;
                driver = base + i;
            }
        }
        if (op->op == WL_PLAN_OP_SEMIJOIN
            && is_stratum_idb(sp, op->right_relation)
            && !wl_columnar_eval_tdd_plan_prefilter(ops, base + i, base + end))
            return ENOTSUP;
    }
    if (drivers > 1)
        return EINVAL;
    bool inactive = sources != 0 && drivers == 0;
    bool seed = sources == 0;
    uint32_t read_start = manifest->read_count;
    for (uint32_t i = start; i < end; i++) {
        const wl_plan_op_t *op = &ops[base + i];
        wl_delta_mode_t expected = WL_DELTA_AUTO;
        if (wl_columnar_eval_tdd_plan_source(op, sp))
            expected = base + i ==
                driver ? WL_DELTA_FORCE_DELTA : WL_DELTA_FORCE_FULL;
        if (i == start && inactive)
            expected = WL_DELTA_FORCE_EMPTY;
        if (i == start && seed)
            expected = WL_DELTA_FORCE_EMPTY_AFTER_SEED;
        if (op->delta_mode != expected)
            return EINVAL;
        const char *name = wl_columnar_eval_tdd_plan_operand(op);
        if (!name || inactive)
            continue;
        if (manifest->reads) {
            wl_columnar_eval_tdd_plan_read_t *read =
                &manifest->reads[manifest->read_count];
            *read = (wl_columnar_eval_tdd_plan_read_t){
                .op_index = base + i, .source_index = i,
                .relation_name = name,
                .right_operand = op->op != WL_PLAN_OP_VARIABLE,
                .kind = base + i == driver ? WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                    : op->op ==
                    WL_PLAN_OP_SEMIJOIN ? WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER
                    : WL_COLUMNAR_EVAL_TDD_PLAN_FULL,
            };
        }
        manifest->read_count++;
    }
    if (seed) {
        for (uint32_t i = start; i < end; i++) {
            if (is_stratum_idb(sp,
                wl_columnar_eval_tdd_plan_operand(&ops[base + i])))
                return ENOTSUP; /* A recursive prefilter alone is not a seed. */
        }
    }
    if (manifest->slices) {
        manifest->slices[manifest->slice_count] =
            (wl_columnar_eval_tdd_plan_slice_t){
            .alternative = alternative, .ordinal = ordinal,
            .start = base + start, .count = end - start, .driver = driver,
            .read_start = read_start,
            .read_count = manifest->read_count - read_start,
            .seed = seed, .inactive = inactive,
        };
    }
    manifest->slice_count++;
    return 0;
}

static int
wl_columnar_eval_tdd_plan_walk(const wl_plan_stratum_t *sp,
    const wl_plan_relation_t *rel,
    wl_columnar_eval_tdd_plan_manifest_t *manifest)
{
    uint32_t width = manifest->block_size;
    for (uint32_t b = 0; b < manifest->alternative_count; b++) {
        uint32_t base = b * width, depth = 0, ordinal = 0, start = UINT32_MAX;
        uint32_t drivers = 0;
        for (uint32_t i = 0; i < width; i++) {
            const wl_plan_op_t *op = &rel->ops[base + i];
            if (!wl_columnar_eval_tdd_plan_op_equal(&rel->ops[i], op))
                return EINVAL;
            if (op->delta_mode == WL_DELTA_FORCE_DELTA)
                drivers++;
            bool boundary = op->op == WL_PLAN_OP_VARIABLE
                || op->op == WL_PLAN_OP_CONCAT ||
                op->op == WL_PLAN_OP_CONSOLIDATE;
            if (boundary && start != UINT32_MAX) {
                int rc = wl_columnar_eval_tdd_plan_slice(sp, rel->ops, base,
                        start, i, b, ordinal++, manifest);
                if (rc != 0)
                    return rc;
                start = UINT32_MAX;
            }
            switch (op->op) {
            case WL_PLAN_OP_VARIABLE:
                if (!op->relation_name || i == width - 1)
                    return EINVAL;
                depth++;
                start = i;
                break;
            case WL_PLAN_OP_CONCAT:
                if (op->delta_mode != WL_DELTA_AUTO
                    || (i == width - 1 ? depth != 1 : depth < 2))
                    return EINVAL;
                depth--;
                break;
            case WL_PLAN_OP_CONSOLIDATE:
                if (depth != 1 || op->delta_mode != WL_DELTA_AUTO)
                    return EINVAL;
                break;
            case WL_PLAN_OP_JOIN:
            case WL_PLAN_OP_SEMIJOIN:
                if (!op->right_relation)
                    return EINVAL;
            /* fall through */
            case WL_PLAN_OP_MAP:
            case WL_PLAN_OP_FILTER:
                if (start == UINT32_MAX || depth == 0)
                    return EINVAL;
                break;
            default:
                return ENOTSUP;
            }
        }
        if (depth != 0 || start != UINT32_MAX || drivers != 1)
            return EINVAL;
    }
    /* Independently enumerate source slots. Counting deltas alone could
     * incorrectly accept a truncated or duplicated set of alternatives. */
    uint32_t sources = 0;
    for (uint32_t i = 0; i < width; i++) {
        uint32_t drives = 0;
        for (uint32_t b = 0; b < manifest->alternative_count; b++)
            drives += rel->ops[b * width + i].delta_mode ==
                WL_DELTA_FORCE_DELTA;
        bool source = wl_columnar_eval_tdd_plan_source(&rel->ops[i], sp);
        if (drives != (source ? 1u : 0u))
            return EINVAL;
        sources += source;
    }
    return sources == manifest->alternative_count ? 0 : EINVAL;
}

void
wl_columnar_eval_tdd_plan_bindings_free(
    wl_columnar_eval_tdd_plan_manifest_t *manifest)
{
    if (!manifest)
        return;
    free(manifest->reads);
    free(manifest->slices);
    memset(manifest, 0, sizeof(*manifest));
}

/* Deliberately narrow grammar, rechecked at execution. K1 has no expanded
 * FORCE_DELTA markers: only this proved right operand may bind AUTO to DELTA.
 * The two seed bodies and outer CONCAT/CONS/EXCHANGE are not executed here. */
static bool
wl_columnar_eval_tdd_plan_relation_names_valid(const wl_plan_stratum_t *sp)
{
    for (uint32_t i = 0; i < sp->relation_count; i++) {
        if (!sp->relations[i].name)
            return false;
        for (uint32_t j = 0; j < i; j++)
            if (strcmp(sp->relations[i].name, sp->relations[j].name) == 0)
                return false;
    }
    return true;
}

/* The K1 exception admits only the generated payload shape. The shared
 * semantic comparator intentionally ignores pointers with zero lengths. */
static bool
wl_columnar_eval_tdd_plan_k1_ptr_shape(const wl_plan_op_t *op,
    const wl_plan_op_t *expected)
{
    if (!!op->left_keys != !!expected->left_keys
        || !!op->right_keys != !!expected->right_keys
        || !!op->project_indices != !!expected->project_indices
        || !!op->group_by_indices != !!expected->group_by_indices
        || !!op->map_exprs != !!expected->map_exprs
        || !!op->filter_expr.data != !!expected->filter_expr.data
        || !!op->right_filter_expr.data != !!expected->right_filter_expr.data
        || !!op->agg_expr.data != !!expected->agg_expr.data)
        return false;
    for (uint32_t i = 0; i < op->map_expr_count; i++)
        if (!!op->map_exprs[i].data != !!expected->map_exprs[i].data)
            return false;
    return true;
}

/* Callers validate the current stratum's relation names before recognition. */
static bool
wl_columnar_eval_tdd_plan_cspa_k1(const wl_plan_stratum_t *sp,
    const wl_plan_relation_t *rel)
{
    static const char *const col0[] = { "col0" }, *const col1[] = { "col1" };
    static const uint32_t p11[] = { 1, 1 }, p00[] = { 0, 0 },
        p13[] = { 1, 3 }, p03[] = { 0, 3 };
    /* Borrowed comparison constants: never mutated or passed to a plan owner.
     * Buffer fields are mutable pointer types, so no const-discard is needed. */
    static uint8_t variables[2][7] = {
        { WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0' },
        { WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1' },
    };
    static wl_plan_expr_buffer_t expressions[2][2] = {
        { { .data = variables[0], .size = 7 },
          { .data = variables[0], .size = 7 } },
        { { .data = variables[1], .size = 7 },
          { .data = variables[1], .size = 7 } },
    };
    static const wl_plan_op_t expected[] = {
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "assign" },
        { .op = WL_PLAN_OP_MAP, .project_count = 2, .project_indices = p11,
          .map_expr_count = 2, .map_exprs = expressions[1] },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "assign" },
        { .op = WL_PLAN_OP_MAP, .project_count = 2, .project_indices = p00,
          .map_expr_count = 2, .map_exprs = expressions[0] },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_VARIABLE, .relation_name = "dereference" },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "valueAlias",
          .key_count = 1, .left_keys = col0, .right_keys = col0,
          .project_count = 2, .project_indices = p13 },
        { .op = WL_PLAN_OP_SEMIJOIN, .right_relation = "dereference",
          .key_count = 1, .left_keys = col1, .right_keys = col0 },
        { .op = WL_PLAN_OP_JOIN, .right_relation = "dereference",
          .key_count = 1, .left_keys = col1, .right_keys = col0,
          .project_count = 2, .project_indices = p03 },
        { .op = WL_PLAN_OP_CONCAT },
        { .op = WL_PLAN_OP_CONSOLIDATE },
        { .op = WL_PLAN_OP_EXCHANGE },
    };
    if (!sp->is_recursive || !rel->ops || rel->op_count != 12
        || !wl_columnar_eval_tdd_plan_string_equal(rel->name, "memoryAlias")
        || rel->recursive_agg.has_spec)
        return false;
    if (!is_stratum_idb(sp, "valueAlias")
        || is_stratum_idb(sp, "dereference") || is_stratum_idb(sp, "assign"))
        return false;
    for (uint32_t i = 0; i < 12; i++) {
        wl_plan_op_t op = rel->ops[i];
        if (i == 11)
            op.opaque_data = NULL;
        if (op.materialized || op.delta_mode != WL_DELTA_AUTO
            || !wl_columnar_eval_tdd_plan_op_equal(&op, &expected[i])
            || !wl_columnar_eval_tdd_plan_k1_ptr_shape(&op, &expected[i]))
            return false;
    }
    const wl_plan_op_exchange_t *meta = rel->ops[11].opaque_data;
    return meta && meta->num_workers == 0 && meta->key_col_count == 1
           && meta->key_col_idxs && meta->key_col_idxs[0] == 0
           && meta->edb_key_col_count == 1 && meta->edb_key_col_idxs
           && meta->edb_key_col_idxs[0] == 0
           && !meta->edb_rel_name;
}

static int
wl_columnar_eval_tdd_plan_k1_manifest(const wl_plan_stratum_t *sp,
    uint32_t relation_index, wl_columnar_eval_tdd_plan_manifest_t *out)
{
    const wl_plan_relation_t *rel = &sp->relations[relation_index];
    *out = (wl_columnar_eval_tdd_plan_manifest_t){
        .form = WL_COLUMNAR_EVAL_TDD_PLAN_CSPA_K1,
        .relation_index = relation_index, .alternative_count = 1,
        .block_size = 4, .slice_count = 1, .read_count = 4,
        .owner_stratum = sp, .owner_relation = rel,
        .owner_ops = rel->ops, .owner_op_count = rel->op_count,
    };
    out->slices = calloc(1, sizeof(*out->slices));
    out->reads = calloc(4, sizeof(*out->reads));
    if (!out->slices || !out->reads) {
        wl_columnar_eval_tdd_plan_bindings_free(out);
        return ENOMEM;
    }
    *out->slices = (wl_columnar_eval_tdd_plan_slice_t){
        .start = 5, .count = 4, .driver = 6, .read_count = 4,
    };
    for (uint32_t i = 0; i < 4; i++)
        out->reads[i] = (wl_columnar_eval_tdd_plan_read_t){
            .op_index = 5 + i, .source_index = i,
            .relation_name = wl_columnar_eval_tdd_plan_operand(&rel->ops[5+i]),
            .right_operand = i != 0,
            .kind = i == 1 ? WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                : i == 2 ? WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER
                : WL_COLUMNAR_EVAL_TDD_PLAN_FULL,
        };
    return 0;
}

static int
wl_columnar_eval_tdd_plan_collect(const wl_plan_stratum_t *sp,
    uint32_t relation_index, wl_columnar_eval_tdd_plan_manifest_t *out,
    uint32_t counts[3])
{
    if (!out && !counts)
        return EINVAL;
    if (out)
        memset(out, 0, sizeof(*out));
    if (counts)
        memset(counts, 0, 3 * sizeof(*counts));
    if (!sp || !sp->relations || relation_index >= sp->relation_count)
        return EINVAL;
    if (!wl_columnar_eval_tdd_plan_relation_names_valid(sp))
        return EINVAL;
    const wl_plan_relation_t *rel = &sp->relations[relation_index];
    if (!sp->is_recursive || !rel->ops || rel->op_count < 4)
        return ENOTSUP;
    uint32_t count = rel->op_count;
    const wl_plan_op_t *exchange = &rel->ops[count - 1];
    const wl_plan_op_exchange_t *meta = exchange->opaque_data;
    if (exchange->op != WL_PLAN_OP_EXCHANGE || !meta
        || !meta->key_col_idxs || !meta->key_col_count)
        return ENOTSUP;
    if (rel->ops[count - 2].op != WL_PLAN_OP_CONSOLIDATE
        || rel->ops[count - 2].delta_mode != WL_DELTA_AUTO
        || exchange->delta_mode != WL_DELTA_AUTO)
        return EINVAL;
    uint32_t k = 0;
    for (uint32_t i = 0; i < count - 2; i++)
        k += rel->ops[i].delta_mode == WL_DELTA_FORCE_DELTA;
    if (k < 2) {
        if (!wl_columnar_eval_tdd_plan_cspa_k1(sp, rel))
            return ENOTSUP;
        if (counts) {
            counts[0] = 1;
            counts[1] = 1;
            counts[2] = 4;
            return 0;
        }
        return wl_columnar_eval_tdd_plan_k1_manifest(sp, relation_index, out);
    }
    if ((count - 2) % k != 0)
        return EINVAL;
    uint32_t width = (count - 2) / k;
    if (width < 2 || rel->ops[width - 1].op != WL_PLAN_OP_CONCAT)
        return EINVAL;
    wl_columnar_eval_tdd_plan_manifest_t result = {
        .relation_index = relation_index, .alternative_count = k,
        .block_size = width,
    };
    int rc = wl_columnar_eval_tdd_plan_walk(sp, rel, &result);
    if (rc != 0)
        return rc;
    size_t slice_bytes, read_bytes;
    if (wl_columnar_eval_checked_size_mul(result.slice_count,
        sizeof(*result.slices), &slice_bytes) != 0
        || wl_columnar_eval_checked_size_mul(result.read_count,
        sizeof(*result.reads), &read_bytes) != 0)
        return EOVERFLOW;
    if (counts) {
        counts[0] = result.alternative_count;
        counts[1] = result.slice_count;
        counts[2] = result.read_count;
        return 0;
    }
    result.slices = calloc(1, slice_bytes);
    result.reads = calloc(1, read_bytes);
    if (!result.slices || !result.reads) {
        wl_columnar_eval_tdd_plan_bindings_free(&result);
        return ENOMEM;
    }
    result.slice_count = 0;
    result.read_count = 0;
    rc = wl_columnar_eval_tdd_plan_walk(sp, rel, &result);
    if (rc != 0) {
        wl_columnar_eval_tdd_plan_bindings_free(&result);
        return rc;
    }
    result.owner_stratum = sp;
    result.owner_relation = rel;
    result.owner_ops = rel->ops;
    result.owner_op_count = rel->op_count;
    *out = result;
    return 0;
}

int
wl_columnar_eval_tdd_plan_bindings(const wl_plan_stratum_t *sp,
    uint32_t relation_index, wl_columnar_eval_tdd_plan_manifest_t *out)
{
    return wl_columnar_eval_tdd_plan_collect(sp, relation_index, out, NULL);
}

/* Diagnostics need validated counts, not owned occurrence arrays. The full
 * manifest path retains its allocation and typed ENOMEM behavior. */
int
wl_columnar_eval_tdd_plan_binding_summary(const wl_plan_stratum_t *sp,
    uint32_t relation_index, uint32_t counts[3])
{
    return wl_columnar_eval_tdd_plan_collect(sp, relation_index, NULL, counts);
}

/* Issue #2114: is @name one of the relations an insertion instance may be
 * driven from in this round. */
static bool
wl_columnar_eval_tdd_plan_insert_driver(const wl_plan_stratum_t *sp,
    const char *name, const char *const *changed, uint32_t changed_count,
    bool round0)
{
    if (!name)
        return false;
    if (!round0)
        return is_stratum_idb(sp, name);
    for (uint32_t c = 0; c < changed_count; c++) {
        if (strcmp(name, changed[c]) == 0)
            return true;
    }
    return false;
}

/* Emit, or with NULL arrays count, the instances of the rule slice
 * ops[start, end): one per driving occurrence, each followed by every read
 * of the slice. */
static int
wl_columnar_eval_tdd_plan_insert_slice(const wl_plan_stratum_t *sp,
    const wl_plan_op_t *ops, uint32_t start, uint32_t end, uint32_t ordinal,
    const char *const *changed, uint32_t changed_count, bool round0,
    wl_columnar_eval_tdd_plan_manifest_t *m)
{
    for (uint32_t i = start; i < end; i++) {
        if (ops[i].op == WL_PLAN_OP_SEMIJOIN
            && !wl_columnar_eval_tdd_plan_prefilter(ops, i, end))
            return ENOTSUP;
    }
    for (uint32_t d = start; d < end; d++) {
        const wl_plan_op_t *driver = &ops[d];
        if ((driver->op != WL_PLAN_OP_VARIABLE
            && driver->op != WL_PLAN_OP_JOIN)
            || !wl_columnar_eval_tdd_plan_insert_driver(sp,
            wl_columnar_eval_tdd_plan_operand(driver), changed,
            changed_count, round0))
            continue;
        uint32_t read_start = m->read_count;
        for (uint32_t i = start; i < end; i++) {
            const char *name = wl_columnar_eval_tdd_plan_operand(&ops[i]);
            if (!name)
                continue;
            if (m->reads) {
                m->reads[m->read_count] = (wl_columnar_eval_tdd_plan_read_t){
                    .op_index = i, .source_index = i - start,
                    .relation_name = name,
                    .right_operand = ops[i].op != WL_PLAN_OP_VARIABLE,
                    .kind = i == d ? WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
                        : ops[i].op == WL_PLAN_OP_SEMIJOIN
                        ? WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER
                        : WL_COLUMNAR_EVAL_TDD_PLAN_FULL,
                };
            }
            if (m->read_count == UINT32_MAX)
                return EOVERFLOW;
            m->read_count++;
        }
        if (m->slices) {
            m->slices[m->slice_count] = (wl_columnar_eval_tdd_plan_slice_t){
                .alternative = 0, .ordinal = ordinal, .start = start,
                .count = end - start, .driver = d, .read_start = read_start,
                .read_count = m->read_count - read_start,
            };
        }
        if (m->slice_count == UINT32_MAX)
            return EOVERFLOW;
        m->slice_count++;
    }
    return 0;
}

/* Walk the body as a union of rule slices: each starts at a VARIABLE and
 * runs through JOIN, SEMIJOIN, MAP and FILTER operators; CONCAT and
 * CONSOLIDATE only close and combine them.  An operator after a CONCAT or
 * CONSOLIDATE that is not one itself would apply to the union rather than to
 * one rule, so it is refused with every other operator. */
static int
wl_columnar_eval_tdd_plan_insert_walk(const wl_plan_stratum_t *sp,
    const wl_plan_op_t *ops, uint32_t op_count, const char *const *changed,
    uint32_t changed_count, bool round0,
    wl_columnar_eval_tdd_plan_manifest_t *m)
{
    uint32_t depth = 0, ordinal = 0, start = UINT32_MAX;
    for (uint32_t i = 0; i <= op_count; i++) {
        bool closes = i == op_count || ops[i].op == WL_PLAN_OP_VARIABLE
            || ops[i].op == WL_PLAN_OP_CONCAT
            || ops[i].op == WL_PLAN_OP_CONSOLIDATE;
        if (closes && start != UINT32_MAX) {
            int rc = wl_columnar_eval_tdd_plan_insert_slice(sp, ops, start, i,
                    ordinal++, changed, changed_count, round0, m);
            if (rc != 0)
                return rc;
            start = UINT32_MAX;
        }
        if (i == op_count)
            break;
        const wl_plan_op_t *op = &ops[i];
        if (op->opaque_data)
            return ENOTSUP;
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
            if (!op->relation_name)
                return EINVAL;
            depth++;
            start = i;
            break;
        case WL_PLAN_OP_CONCAT:
            if (depth < 2)
                return EINVAL;
            depth--;
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            if (depth != 1)
                return EINVAL;
            break;
        case WL_PLAN_OP_JOIN:
        case WL_PLAN_OP_SEMIJOIN:
            if (!op->right_relation)
                return EINVAL;
        /* fall through */
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_FILTER:
            if (start == UINT32_MAX)
                return ENOTSUP;
            break;
        default:
            return ENOTSUP;
        }
    }
    return depth == 1 ? 0 : EINVAL;
}

int
wl_columnar_eval_tdd_plan_insert_bindings(const wl_plan_stratum_t *sp,
    uint32_t relation_index, const char *const *changed,
    uint32_t changed_count, bool round0,
    wl_columnar_eval_tdd_plan_manifest_t *out)
{
    if (!out)
        return EINVAL;
    memset(out, 0, sizeof(*out));
    if (!sp || !sp->relations || relation_index >= sp->relation_count
        || (changed_count && !changed))
        return EINVAL;
    for (uint32_t c = 0; round0 && c < changed_count; c++) {
        if (!changed[c] || is_stratum_idb(sp, changed[c]))
            return EINVAL;
    }
    if (!sp->bodies)
        return ENOTSUP;
    const wl_plan_body_t *body = &sp->bodies[relation_index];
    if (!body->ops || body->op_count == 0)
        return ENOTSUP;
    wl_columnar_eval_tdd_plan_manifest_t result = {
        .form = WL_COLUMNAR_EVAL_TDD_PLAN_INSERT,
        .relation_index = relation_index, .alternative_count = 1,
        .block_size = body->op_count,
    };
    int rc = wl_columnar_eval_tdd_plan_insert_walk(sp, body->ops,
            body->op_count, changed, changed_count, round0, &result);
    if (rc != 0)
        return rc;
    if (result.slice_count) {
        size_t slice_bytes, read_bytes;
        if (wl_columnar_eval_checked_size_mul(result.slice_count,
            sizeof(*result.slices), &slice_bytes) != 0
            || wl_columnar_eval_checked_size_mul(result.read_count,
            sizeof(*result.reads), &read_bytes) != 0)
            return EOVERFLOW;
        result.slices = calloc(1, slice_bytes);
        result.reads = calloc(1, read_bytes);
        if (!result.slices || !result.reads) {
            wl_columnar_eval_tdd_plan_bindings_free(&result);
            return ENOMEM;
        }
        result.slice_count = 0;
        result.read_count = 0;
        rc = wl_columnar_eval_tdd_plan_insert_walk(sp, body->ops,
                body->op_count, changed, changed_count, round0, &result);
        if (rc != 0) {
            wl_columnar_eval_tdd_plan_bindings_free(&result);
            return rc;
        }
    }
    result.owner_stratum = sp;
    result.owner_relation = &sp->relations[relation_index];
    result.owner_ops = body->ops;
    result.owner_op_count = body->op_count;
    *out = result;
    return 0;
}

/* Validate the captured schema without consulting mutable registry aliases. */
static bool
wl_columnar_eval_tdd_plan_input_matches(const wl_columnar_eval_tdd_input_t *in)
{
    const col_rel_t *rel = in->relation;
    if (!rel || rel->ncols != in->ncols
        || (rel->col_names == NULL) != (in->column_names == NULL))
        return false;
    for (uint32_t c = 0; rel->col_names && c < rel->ncols; c++) {
        if (!wl_columnar_eval_tdd_plan_string_equal(rel->col_names[c],
            in->column_names[c]))
            return false;
    }
    return rel->name && in->name && strcmp(rel->name, in->name) == 0
           && rel->view_generation == in->view_generation
           && rel->storage_generation == in->storage_generation
           && rel->declared_ncols == in->declared_ncols
           && rel->schema_ok == in->schema_ok && !rel->timestamps
           && (rel->column_types == NULL) == (in->column_types == NULL)
           && (!rel->column_types || memcmp(rel->column_types, in->column_types,
           rel->ncols * sizeof(*rel->column_types)) == 0)
           && rel->compound_kind == in->compound_kind
           && rel->compound_count == in->compound_count
           && rel->compound_arity_len == in->compound_arity_len
           && rel->inline_physical_offset == in->inline_physical_offset
           && (rel->compound_arity_len == 0 || (rel->compound_arity_map
           && in->compound_arity_map && memcmp(rel->compound_arity_map,
           in->compound_arity_map,
           rel->compound_arity_len * sizeof(uint32_t)) == 0))
           && rel->has_graph_column == in->has_graph_column
           && (!rel->has_graph_column ||
           rel->graph_col_idx == in->graph_col_idx);
}

/* These are the three unary EDB seed bodies in the unfused CSPA plan.
 * Compare complete payloads, including expression bytes, before allowing the
 * bound VARIABLE path to bypass ordinary seed/delta selection. */
static bool
wl_columnar_eval_tdd_plan_seed_supported(const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *manifest,
    const wl_columnar_eval_tdd_plan_slice_t *slice)
{
    static const uint32_t starts[] = {0, 2, 5};
    static const uint32_t projections[][2] = {{0, 1}, {0, 0}, {1, 1}};
    const wl_plan_relation_t *rel = manifest->owner_relation;
    if (!rel->name || strcmp(rel->name, "valueFlow") != 0
        || slice->ordinal >= 3 || slice->start != starts[slice->ordinal]
        || slice->count != 2 || slice->read_count != 1
        || slice->start + slice->count >= manifest->block_size)
        return false;
    for (uint32_t r = 0; r < sp->relation_count; r++) {
        if (!sp->relations[r].name
            || strcmp(sp->relations[r].name, "assign") == 0)
            return false;
    }
    uint8_t variables[2][7] = {
        {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'},
        {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'},
    };
    const uint32_t *projection = projections[slice->ordinal];
    wl_plan_expr_buffer_t expressions[2] = {
        {.data = variables[projection[0]], .size = 7},
        {.data = variables[projection[1]], .size = 7},
    };
    const wl_plan_op_t expected_variable = {
        .op = WL_PLAN_OP_VARIABLE, .relation_name = "assign",
    };
    const wl_plan_op_t expected_map = {
        .op = WL_PLAN_OP_MAP, .project_count = 2,
        .project_indices = projection,
        .map_expr_count = 2, .map_exprs = expressions,
    };
    const wl_plan_op_t *ops = rel->ops + slice->start;
    /* op_equal compares semantic payload bytes, but intentionally ignores
     * pointers whose count/size is zero. Keep this seed allowlist exact. */
    if (ops[0].left_keys || ops[0].right_keys || ops[0].project_indices
        || ops[0].group_by_indices || ops[0].map_exprs
        || ops[1].left_keys || ops[1].right_keys
        || ops[1].group_by_indices
        || ops[0].filter_expr.data || ops[0].right_filter_expr.data
        || ops[0].agg_expr.data || ops[1].filter_expr.data
        || ops[1].right_filter_expr.data || ops[1].agg_expr.data)
        return false;
    return ops[0].delta_mode == WL_DELTA_FORCE_EMPTY_AFTER_SEED
           && ops[1].delta_mode == WL_DELTA_AUTO
           && !ops[0].materialized && !ops[1].materialized
           && wl_columnar_eval_tdd_plan_op_equal(&ops[0], &expected_variable)
           && wl_columnar_eval_tdd_plan_op_equal(&ops[1], &expected_map);
}

int
wl_columnar_eval_tdd_plan_prepare_inputs(wl_col_session_t *worker,
    const wl_plan_stratum_t *sp,
    const wl_columnar_eval_tdd_plan_manifest_t *manifest, uint32_t slice_index,
    const void *snapshot, const void *partition,
    uint32_t worker_index, uint32_t worker_count,
    const wl_columnar_eval_tdd_input_t *inputs, uint32_t input_count,
    wl_columnar_eval_tdd_run_t *run)
{
    if (!worker || !sp || !manifest || !run || !inputs || !snapshot
        || !worker_count || worker_index >= worker_count)
        return EINVAL;
    if (run->worker || run->slots || run->cleanup || run->output
        || worker->teardown_started || worker->tdd_input_run ||
        worker->cleanup_active
        || worker->cleanup_pending)
        return EBUSY;
    if (manifest->owner_stratum != sp || !sp->relations
        || manifest->relation_index >= sp->relation_count)
        return EINVAL;
    const wl_plan_relation_t *rel = &sp->relations[manifest->relation_index];
    if (manifest->owner_relation != rel || manifest->owner_ops != rel->ops
        || manifest->owner_op_count != rel->op_count || !rel->ops
        || !manifest->slices || !manifest->reads
        || slice_index >= manifest->slice_count)
        return EINVAL;
    const wl_columnar_eval_tdd_plan_slice_t *slice =
        &manifest->slices[slice_index];
    bool k1 = manifest->form == WL_COLUMNAR_EVAL_TDD_PLAN_CSPA_K1;
    if (k1) {
        if (!wl_columnar_eval_tdd_plan_relation_names_valid(sp)
            || !wl_columnar_eval_tdd_plan_cspa_k1(sp, rel)
            || manifest->alternative_count != 1 || manifest->block_size != 4
            || manifest->slice_count != 1 || manifest->read_count != 4
            || slice_index != 0 || slice->start != 5 || slice->count != 4
            || slice->driver != 6 || slice->read_start != 0
            || slice->read_count != 4 || slice->alternative || slice->ordinal
            || slice->seed || slice->inactive)
            return EINVAL;
    } else if (manifest->form != WL_COLUMNAR_EVAL_TDD_PLAN_EXPANDED)
        return EINVAL;
    if ((!k1 && manifest->alternative_count < 2) || slice->inactive
        || worker->diff_operators_active || worker->retraction_seeded
        || worker->retraction_right_pass || worker->delta_seeded
        || worker->join_batch_bytes > 0 || worker->join_batch_strict)
        return ENOTSUP;
    if (!slice->count || slice->start >= rel->op_count
        || slice->count > rel->op_count - slice->start
        || slice->read_start > manifest->read_count
        || slice->read_count > manifest->read_count - slice->read_start
        || input_count != slice->read_count || !input_count)
        return EINVAL;
    if (k1 && (inputs[0].relation != inputs[2].relation
        || inputs[0].relation != inputs[3].relation))
        return EINVAL;
    bool seed = slice->seed;
    if (seed) {
        if (slice->alternative != 0 || worker->current_iteration != 0
            || worker_count != 1 || worker_index != 0 || partition)
            return ENOTSUP;
        if (slice->driver != UINT32_MAX || input_count != 1
            || inputs[0].partition || inputs[0].worker_index != 0
            || inputs[0].worker_count != 1 || inputs[0].ncols != 2
            || !inputs[0].name || strcmp(inputs[0].name, "assign") != 0)
            return EINVAL;
        if (!wl_columnar_eval_tdd_plan_seed_supported(sp, manifest, slice))
            return ENOTSUP;
    } else if (!partition) {
        return EINVAL;
    }
    uint32_t reads = 0, drivers = 0;
    for (uint32_t i = slice->start; i < slice->start + slice->count; i++) {
        const wl_plan_op_t *op = &rel->ops[i];
        if (op->opaque_data || op->right_filter_expr.size ||
            op->filter_expr.size || op->agg_expr.size
            || (!seed && op->map_expr_count))
            return ENOTSUP;
        if (op->op == WL_PLAN_OP_MAP)
            continue;
        if (op->op != WL_PLAN_OP_VARIABLE && op->op != WL_PLAN_OP_JOIN
            && op->op != WL_PLAN_OP_SEMIJOIN)
            return ENOTSUP;
        if ((i == slice->start) != (op->op == WL_PLAN_OP_VARIABLE)
            || reads >= input_count)
            return EINVAL;
        const wl_columnar_eval_tdd_plan_read_t *read =
            &manifest->reads[slice->read_start + reads];
        const wl_columnar_eval_tdd_input_t *in = &inputs[reads++];
        const char *name = wl_columnar_eval_tdd_plan_operand(op);
        bool delta = i == slice->driver;
        wl_columnar_eval_tdd_plan_read_kind_t kind = delta
            ? WL_COLUMNAR_EVAL_TDD_PLAN_DELTA
            : op->op ==
            WL_PLAN_OP_SEMIJOIN ? WL_COLUMNAR_EVAL_TDD_PLAN_PREFILTER
            : WL_COLUMNAR_EVAL_TDD_PLAN_FULL;
        if (read->op_index != i || in->read != read || !name
            || (k1 && in->ncols != 2)
            || (k1 && read->source_index != i - 5)
            || !read->relation_name || strcmp(name, read->relation_name) != 0
            || read->right_operand != (op->op != WL_PLAN_OP_VARIABLE)
            || read->kind != kind || in->snapshot != snapshot
            || !wl_columnar_eval_tdd_plan_input_matches(in))
            return EINVAL;
        if (delta) {
            if (op->delta_mode != (k1 ? WL_DELTA_AUTO : WL_DELTA_FORCE_DELTA)
                || op->op == WL_PLAN_OP_SEMIJOIN
                || in->partition != partition ||
                in->worker_index != worker_index
                || in->worker_count != worker_count)
                return EINVAL;
            drivers++;
        } else if ((!seed && op->delta_mode != WL_DELTA_FORCE_FULL
            && op->delta_mode != WL_DELTA_AUTO)
            || in->partition)
            return EINVAL;
        if (op->op == WL_PLAN_OP_SEMIJOIN
            && !wl_columnar_eval_tdd_plan_prefilter(rel->ops, i,
            slice->start + slice->count))
            return ENOTSUP;
    }
    if (reads != input_count || drivers != (seed ? 0u : 1u))
        return EINVAL;
    size_t bytes;
    if (wl_columnar_eval_checked_size_mul(input_count,
        sizeof(*run->slots), &bytes) != 0)
        return EOVERFLOW;
    /* Reserve in the stable caller handle: even failed setup has a durable
     * token if releasing credit refuses. No live token is abandoned locally. */
    wl_columnar_memory_reservation_init(&run->reservation);
    if (worker->memory_governor) {
        wl_columnar_memory_admission_status_t status =
            wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(worker->memory_governor),
            bytes, &run->reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                worker->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
        run->governor = worker->memory_governor;
        wl_columnar_memory_governor_ref_retain(run->governor);
    }
    run->worker = worker;
    run->manifest = manifest;
    run->slice = slice;
    worker->tdd_input_run = run;
    run->slots = calloc(input_count, sizeof(*run->slots));
    if (!run->slots)
        return ENOMEM;
    run->input_count = input_count;
    if (run->governor && !wl_columnar_memory_commit(&run->reservation,
        run->slots))
        return EINVAL;
    wl_columnar_eval_tdd_input_slot_t *slots = run->slots;
    for (uint32_t i = 0; i < input_count; i++) {
        slots[i].input = inputs[i];
        bool duplicate = false;
        for (uint32_t j = 0; j < i; j++)
            duplicate |= slots[j].input.relation == inputs[i].relation;
        if (!duplicate) {
            int rc = col_rel_source_reader_acquire(inputs[i].relation,
                    &slots[i].reader);
            if (rc != 0)
                return rc;
        }
        if (!wl_columnar_eval_tdd_plan_input_matches(&inputs[i]))
            return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_tdd_plan_resolve_input(wl_col_session_t *worker,
    const wl_plan_op_t *op, bool right, col_rel_t **relation, bool *delta)
{
    if (!worker || !op || !relation || !delta)
        return EINVAL;
    wl_columnar_eval_tdd_run_t *run = worker->tdd_input_run;
    if (!run || !run->evaluating || run->current_op != op)
        return EBUSY;
    for (uint32_t i = 0; i < run->input_count; i++) {
        const wl_columnar_eval_tdd_input_t *in = &run->slots[i].input;
        const wl_columnar_eval_tdd_plan_read_t *read = in->read;
        if (read->op_index >= run->manifest->owner_op_count
            || op != &run->manifest->owner_ops[read->op_index]
            || read->right_operand != right)
            continue;
        *relation = in->relation;
        *delta = read->kind == WL_COLUMNAR_EVAL_TDD_PLAN_DELTA;
        return 0;
    }
    return EINVAL;
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
    wl_columnar_eval_tdd_plan_cursor_t cursor = { 0 };
    while (wl_columnar_eval_tdd_plan_next(sp, &cursor)) {
        uint32_t c = ops_max_idb_segment_body_atoms(cursor.ops, cursor.count,
                sp);
        if (c > max_count)
            max_count = c;
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

    wl_columnar_eval_tdd_plan_cursor_t cursor = { 0 };
    while (wl_columnar_eval_tdd_plan_next(sp, &cursor)) {
        if (!tdd_ops_single_idb_keys_exchange_aligned(
                cursor.ops, cursor.count, sp))
            return false;
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
    wl_columnar_eval_tdd_plan_cursor_t cursor = { 0 };
    while (wl_columnar_eval_tdd_plan_next(sp, &cursor)) {
        if (ops_have_idb_idb_join(cursor.ops, cursor.count, sp))
            return true;
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
                    if (!op->right_keys[k] || !op->left_keys[k])
                        return false;
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

    wl_columnar_eval_tdd_plan_cursor_t cursor = { 0 };
    while (wl_columnar_eval_tdd_plan_next(sp, &cursor)) {
        if (!idb_idb_join_right_keys_match_exchange(
                cursor.ops, cursor.count, sp, coord))
            return false;
    }
    return true;
}
