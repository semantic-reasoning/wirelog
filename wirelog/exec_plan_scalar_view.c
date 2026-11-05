/*
 * exec_plan_scalar_view.c - Immutable structured scalar plan views
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "exec_plan.h"
#include "columnar/columnar_nanoarrow.h"

#include <errno.h>

/* The scalar view deliberately copies descriptors, not their plan-owned
 * payloads. This preparation is synchronous session creation, never a free
 * per-operation scan or a replacement for checked scalar capture. */
typedef struct {
    uint32_t relations, groups, branches, ops;
    size_t offsets, relation_offset, group_offset, branch_offset, op_offset;
    size_t bytes, alignment;
} wl_plan_scalar_view_layout_t;

static int
wl_plan_scalar_view_count_add(uint32_t *count, uint32_t amount)
{
    if (amount > UINT32_MAX - *count) return EOVERFLOW;
    *count += amount;
    return 0;
}

static bool
wl_plan_scalar_view_extent(uint32_t count, size_t width)
{
    return width && count <= SIZE_MAX / width;
}

/* Validate all branch counts before reading any branch operator descriptor.
 * Counts/pointers are backed by the existing immutable compiled-plan API;
 * arithmetic checks do not make forged C objects safe to dereference. */
static int
wl_plan_scalar_view_group_measure(const wl_plan_stratum_t *stratum,
    const wl_plan_relation_t *relation, uint32_t *branches, uint32_t *ops)
{
    *branches = 0; *ops = 0;
    if (!stratum->is_recursive || stratum->has_aggregate
        || relation->recursive_agg.has_spec || relation->op_count != 2
        || !relation->ops || relation->ops[0].op != WL_PLAN_OP_K_FUSION
        || relation->ops[1].op != WL_PLAN_OP_EXCHANGE) return 0;
    const wl_plan_op_k_fusion_t *meta = relation->ops[0].opaque_data;
    const wl_plan_op_exchange_t *exchange = relation->ops[1].opaque_data;
    if (!meta || !exchange) return EINVAL;
    if (exchange->num_workers) return 0;
    if (meta->k < 2 || !meta->k_ops || !meta->k_op_counts) return EINVAL;
    if (!wl_plan_scalar_view_extent(meta->k, sizeof(*meta->k_ops))
        || !wl_plan_scalar_view_extent(meta->k, sizeof(*meta->k_op_counts)))
        return EOVERFLOW;
    uint32_t total = 0;
    for (uint32_t d = 0; d < meta->k; ++d) {
        uint32_t count = meta->k_op_counts[d];
        if (!count || !meta->k_ops[d]) return EINVAL;
        if (!wl_plan_scalar_view_extent(count, sizeof(wl_plan_op_t)))
            return EOVERFLOW;
        int rc = wl_plan_scalar_view_count_add(&total, count);
        if (rc) return rc;
    }
    bool materialized = false;
    for (uint32_t d = 0; d < meta->k; ++d) {
        for (uint32_t i = 0; i < meta->k_op_counts[d]; ++i) {
            const wl_plan_op_t *op = &meta->k_ops[d][i];
            if (op->opaque_data) return 0;
            switch (op->op) {
            case WL_PLAN_OP_VARIABLE:
            case WL_PLAN_OP_SEMIJOIN:
            case WL_PLAN_OP_ANTIJOIN:
            case WL_PLAN_OP_FILTER:
            case WL_PLAN_OP_MAP:
            case WL_PLAN_OP_CONCAT:
            case WL_PLAN_OP_CONSOLIDATE: break;
            case WL_PLAN_OP_JOIN:
                materialized |= op->materialized;
                break;
            default: return 0;
            }
        }
    }
    if (materialized) {
        *branches = meta->k; *ops = total;
    }
    return 0;
}

/* Checked padding, multiplication and addition before pointer formation. */
static int
wl_plan_scalar_view_region(wl_plan_scalar_view_layout_t *layout,
    uint32_t count, size_t width, size_t alignment, size_t *offset)
{
    size_t remainder = layout->bytes % alignment;
    size_t padding = remainder ? alignment - remainder : 0;
    if (padding > SIZE_MAX - layout->bytes
        || !wl_plan_scalar_view_extent(count, width)) return EOVERFLOW;
    size_t begin = layout->bytes + padding;
    size_t bytes = (size_t)count * width;
    if (bytes > SIZE_MAX - begin) return EOVERFLOW;
    *offset = begin;
    layout->bytes = begin + bytes;
    if (alignment > layout->alignment) layout->alignment = alignment;
    return 0;
}

static int
wl_plan_scalar_view_measure(const wl_plan_t *origin,
    wl_plan_scalar_view_layout_t *layout)
{
    if (origin->stratum_count && !origin->strata) return EINVAL;
    if (!wl_plan_scalar_view_extent(origin->stratum_count,
        sizeof(*origin->strata))) return EOVERFLOW;
    uint64_t relations = 0;
    for (uint32_t s = 0; s < origin->stratum_count; ++s) {
        const wl_plan_stratum_t *stratum = &origin->strata[s];
        if (stratum->relation_count && !stratum->relations) return EINVAL;
        if (!wl_plan_scalar_view_extent(stratum->relation_count,
            sizeof(*stratum->relations))) return EOVERFLOW;
        if (stratum->relation_count > UINT64_MAX - relations)
            return EOVERFLOW;
        relations += stratum->relation_count;
        for (uint32_t r = 0; r < stratum->relation_count; ++r) {
            uint32_t branches, ops;
            int rc = wl_plan_scalar_view_group_measure(stratum,
                    &stratum->relations[r], &branches, &ops);
            if (rc) return rc;
            if (!branches) continue;
            rc = wl_plan_scalar_view_count_add(&layout->groups, 1);
            if (!rc) rc = wl_plan_scalar_view_count_add(&layout->branches,
                        branches);
            if (!rc) rc = wl_plan_scalar_view_count_add(&layout->ops, ops);
            if (rc) return rc;
        }
    }
    /* A known unselected plan acquires no arena and needs no dense map. */
    if (!layout->groups) return 0;
    if (relations > UINT32_MAX || origin->stratum_count == UINT32_MAX)
        return EOVERFLOW;
    layout->relations = (uint32_t)relations;
    layout->bytes = sizeof(wl_plan_scalar_view_t);
    layout->alignment = _Alignof(wl_plan_scalar_view_t);
    int rc = wl_plan_scalar_view_region(layout, origin->stratum_count + 1u,
            sizeof(uint32_t), _Alignof(uint32_t), &layout->offsets);
    if (!rc) rc = wl_plan_scalar_view_region(layout, layout->relations,
                sizeof(wl_plan_scalar_relation_t),
                _Alignof(wl_plan_scalar_relation_t), &layout->relation_offset);
    if (!rc) rc = wl_plan_scalar_view_region(layout, layout->groups,
                sizeof(wl_plan_scalar_group_t),
                _Alignof(wl_plan_scalar_group_t),
                &layout->group_offset);
    if (!rc) rc = wl_plan_scalar_view_region(layout, layout->branches,
                sizeof(wl_plan_scalar_branch_t),
                _Alignof(wl_plan_scalar_branch_t),
                &layout->branch_offset);
    if (!rc) rc = wl_plan_scalar_view_region(layout, layout->ops,
                sizeof(wl_plan_op_t), _Alignof(wl_plan_op_t),
                &layout->op_offset);
    return rc;
}

static int
wl_plan_scalar_view_build(const wl_plan_t *origin,
    const wl_plan_scalar_view_layout_t *layout,
    const wl_plan_scalar_view_allocator_t *allocator, void *storage)
{
    unsigned char *arena = storage;
    uint32_t *offsets = (uint32_t *)(arena + layout->offsets);
    wl_plan_scalar_relation_t *relations =
        (wl_plan_scalar_relation_t *)(arena + layout->relation_offset);
    wl_plan_scalar_group_t *groups =
        (wl_plan_scalar_group_t *)(arena + layout->group_offset);
    wl_plan_scalar_branch_t *branches =
        (wl_plan_scalar_branch_t *)(arena + layout->branch_offset);
    wl_plan_op_t *ops = (wl_plan_op_t *)(arena + layout->op_offset);
    uint32_t relation_index = 0, group_index = 0, branch_index = 0,
        op_index = 0;
    for (uint32_t s = 0; s < origin->stratum_count; ++s) {
        offsets[s] = relation_index;
        const wl_plan_stratum_t *stratum = &origin->strata[s];
        for (uint32_t r = 0; r < stratum->relation_count; ++r) {
            if (relation_index >= layout->relations) return EINVAL;
            const wl_plan_relation_t *relation = &stratum->relations[r];
            relations[relation_index++] = (wl_plan_scalar_relation_t){
                relation, UINT32_MAX
            };
            uint32_t group_branches, group_ops;
            int rc = wl_plan_scalar_view_group_measure(stratum, relation,
                    &group_branches, &group_ops);
            if (rc) return rc;
            if (!group_branches) continue;
            if (group_index >= layout->groups || branch_index > layout->branches
                || group_branches > layout->branches - branch_index
                || op_index > layout->ops || group_ops > layout->ops - op_index)
                return EINVAL;
            const wl_plan_op_k_fusion_t *meta = relation->ops[0].opaque_data;
            relations[relation_index - 1].group_index = group_index;
            groups[group_index] = (wl_plan_scalar_group_t){
                s, r, 0, branch_index, group_branches,
                &relation->ops[0], &relation->ops[1],
                WL_PLAN_SCALAR_MERGE_SORTED_UNION_AFTER_ALL
            };
            for (uint32_t d = 0; d < group_branches; ++d) {
                uint32_t count = meta->k_op_counts[d];
                if (branch_index >= layout->branches || op_index > layout->ops
                    || count > layout->ops - op_index) return EINVAL;
                branches[branch_index++] = (wl_plan_scalar_branch_t){
                    .group_index = group_index, .branch_index = d,
                    .first_op = op_index, .op_count = count,
                    .origin_ops = meta->k_ops[d],
                    .scalar_relation = {
                        .name = "<k_fusion_copy>", .delta_name = NULL,
                        .ops = &ops[op_index], .op_count = count
                    }
                };
                for (uint32_t i = 0; i < count; ++i) {
                    if (op_index >= layout->ops) return EINVAL;
                    ops[op_index++] = meta->k_ops[d][i];
                }
            }
            ++group_index;
        }
    }
    if (relation_index != layout->relations || group_index != layout->groups
        || branch_index != layout->branches || op_index != layout->ops)
        return EINVAL;
    offsets[origin->stratum_count] = relation_index;
    wl_plan_scalar_view_t *view = storage;
    *view = (wl_plan_scalar_view_t){
        .origin = origin, .allocator = *allocator,
        .storage_bytes = layout->bytes,
        .stratum_count = origin->stratum_count,
        .relation_count = layout->relations, .group_count = layout->groups,
        .branch_count = layout->branches, .op_count = layout->ops,
        .stratum_offsets = offsets, .relations = relations, .groups = groups,
        .branches = branches, .ops = ops
    };
    return 0;
}

int
wl_plan_scalar_view_create(const wl_plan_t *origin,
    const wl_plan_scalar_view_allocator_t *allocator,
    wl_plan_scalar_view_t **out)
{
    if (!out) return EINVAL;
    *out = NULL;
    if (!origin || !allocator || !allocator->allocate || !allocator->release)
        return EINVAL;
    wl_plan_scalar_view_allocator_t owner = *allocator;
    wl_plan_scalar_view_layout_t layout = {0};
    int rc = wl_plan_scalar_view_measure(origin, &layout);
    if (rc || !layout.groups) return rc;
    void *storage = NULL;
    rc = owner.allocate(owner.context, layout.bytes, &storage);
    if (rc) return rc;
    if (!storage) return ENOMEM;
    if ((uintptr_t)storage % layout.alignment) {
        owner.release(owner.context, storage, layout.bytes);
        return EINVAL;
    }
    rc = wl_plan_scalar_view_build(origin, &layout, &owner, storage);
    if (rc) {
        owner.release(owner.context, storage, layout.bytes);
        return rc;
    }
    *out = storage;
    return 0;
}

void
wl_plan_scalar_view_destroy(wl_plan_scalar_view_t *view)
{
    if (!view) return;
    wl_plan_scalar_view_allocator_t allocator = view->allocator;
    size_t bytes = view->storage_bytes;
    allocator.release(allocator.context, view, bytes);
}
