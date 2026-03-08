# Plan Generation Strategy for K-Fusion Parallelism

**Date:** 2026-03-08
**Status:** Design Documentation (Plan Generation Not Yet Implemented)
**Next Steps:** Create K_FUSION node instantiation in exec_plan_gen.c

---

## Current State

### Phase 2B Implementation (Complete)
The current plan generation (`expand_multiway_delta()` in exec_plan_gen.c) handles multi-way delta expansion by:

1. **Creating K copies** of the relation's operator sequence
2. **Annotating with delta_mode** for semi-naive iteration (FORCE_DELTA, FORCE_FULL)
3. **Adding CONCAT operators** between copies for boundary marking
4. **Adding CONSOLIDATE operator** at the end for deduplication

**Result:** Sequential K-copy evaluation with CONSOLIDATE-based merging

### Example:
```
Original:      [OP1, OP2, OP3]
Expanded:      [OP1, OP2, OP3, CONCAT,
                OP1, OP2, OP3, CONCAT,
                OP1, OP2, OP3, CONCAT,
                CONSOLIDATE]
```

---

## What's Needed for K-Fusion Parallelism

### Plan Generation Changes Required

To enable K-FUSION operator dispatch and parallel evaluation, we need:

#### 1. **Alternative K-Fusion Expansion Path**
Create a new expansion function `expand_multiway_k_fusion()` that:
- Detects K-copy relations (K >= 2)
- Creates a single `WL_PLAN_OP_K_FUSION` operator instead of K separate copies
- Stores metadata about K count and operator arrays

#### 2. **K-Fusion Metadata Structure**
Add support for opaque K-fusion metadata in the plan operator:

```c
/* In exec_plan.h */
typedef struct {
    uint32_t k;                    /* Number of copies */
    wl_plan_op_t **ops;            /* Array of K operator sequences */
    const uint32_t *delta_pos;     /* Delta positions for each copy */
} wl_plan_op_k_fusion_t;
```

#### 3. **Integration with Plan Generation**
Modify `rewrite_multiway_delta()` to:
- Check a feature flag or configuration: use K-fusion vs. sequential expansion
- For K-fusion: call `expand_multiway_k_fusion()` instead of sequential expansion
- Preserve backward compatibility for non-K-fusion paths

#### 4. **Plan Structure Changes**
Extend `wl_plan_op_t` to include:
```c
void *opaque_data;  /* Backend-specific metadata (K-fusion uses this) */
```

---

## Architecture Decision: Sequential Now, Parallel Later

### Rationale for Current Sequential Approach

1. **Risk Mitigation**: K-fusion infrastructure is complete and tested
   - Merge algorithm: ✅ Correct and optimized
   - Operator dispatch: ✅ Infrastructure in place
   - Unit tests: ✅ 7 comprehensive tests passing
   - Regression validation: ✅ All 20 tests pass

2. **Minimal Backward Compatibility Impact**:
   - Current `expand_multiway_delta()` works correctly
   - Plan structure doesn't require immediate changes
   - K-fusion can be added as an optional path

3. **Phased Delivery**:
   - Phase 2B (Current): Sequential K-copy evaluation with proven CONSOLIDATE merging
   - Phase 2C+ (Future): K-fusion parallel execution via modified plan generation

### Performance Impact

**Current Implementation:**
- Uses sequential CONCAT + CONSOLIDATE
- Benefits from CONSOLIDATE optimization (qsort + dedup)
- Baseline: CSPA 28.7s (established in Phase 2B)

**Future K-Fusion Path (Phase 2C+):**
- Would use parallel workqueue execution
- Target: 30-40% improvement (17-20s)
- Requires plan generation changes + performance validation

---

## Detailed Plan: How to Implement K-Fusion Plan Generation

### Step 1: Define K-Fusion Metadata Structure
```c
/* In columnar_nanoarrow.h or new k_fusion.h */
typedef struct {
    uint32_t k;
    wl_plan_op_t **k_ops;        /* K separate operator sequences */
    const uint32_t **k_delta_pos; /* Delta positions for each copy */
} wl_plan_op_k_fusion_t;
```

### Step 2: Implement expand_multiway_k_fusion()
```c
static wl_plan_op_t *
expand_multiway_k_fusion(const wl_plan_op_t *ops, uint32_t op_count,
                         const uint32_t *delta_pos, uint32_t k,
                         uint32_t *out_count)
{
    /* Create single K_FUSION operator */
    wl_plan_op_t *result = (wl_plan_op_t *)calloc(1, sizeof(wl_plan_op_t));

    /* Allocate and populate K-fusion metadata */
    wl_plan_op_k_fusion_t *meta = calloc(1, sizeof(wl_plan_op_k_fusion_t));
    meta->k = k;
    meta->k_ops = (wl_plan_op_t **)malloc(k * sizeof(wl_plan_op_t *));

    /* Clone operator sequences for each copy */
    for (uint32_t d = 0; d < k; d++) {
        meta->k_ops[d] = (wl_plan_op_t *)malloc(op_count * sizeof(wl_plan_op_t));
        for (uint32_t i = 0; i < op_count; i++) {
            clone_plan_op(&ops[i], &meta->k_ops[d][i]);
            /* Set delta_mode for copy d */
            /* ... */
        }
    }

    /* Populate K-fusion operator */
    result->op = WL_PLAN_OP_K_FUSION;
    result->opaque_data = (void *)meta;

    *out_count = 1;
    return result;
}
```

### Step 3: Add Configuration Flag
```c
/* In plan generation context */
struct plan_gen_config {
    bool use_k_fusion;  /* Enable K-fusion parallelism */
    /* ... */
};
```

### Step 4: Modify rewrite_multiway_delta()
```c
if (is_multiway && (config.use_k_fusion || k >= 8)) {
    rel->ops = expand_multiway_k_fusion(...);
} else {
    rel->ops = expand_multiway_delta(...);  /* Current path */
}
```

---

## Testing Strategy for K-Fusion Plan Generation

1. **Plan Structure Validation**:
   - Verify K-FUSION operator created with correct K count
   - Verify metadata contains correct operator sequences
   - Verify backward compatibility (sequential path still works)

2. **Functional Correctness**:
   - Execute K-FUSION plans for CSPA (K=2)
   - Verify output matches baseline (20,381 tuples)
   - Verify iteration count stays at 6

3. **Performance Validation**:
   - Measure wall-time with K-FUSION vs. CONSOLIDATE
   - Target: 30-40% improvement
   - Measure workqueue overhead < 5%

4. **Regression Testing**:
   - All 15 workloads must pass
   - DOOP breakthrough validation (< 5 minutes)

---

## Dependencies and Blockers

### Current Blockers:
- ⚠️ `wl_plan_op_t` doesn't have `opaque_data` field (would require struct change)
- ⚠️ Plan serialization may not support opaque data (requires investigation)
- ⚠️ Backward compatibility with existing plan structures

### Workarounds:
1. Reuse existing operator fields (left_keys, right_keys) for K-fusion metadata
2. Create a separate K_FUSION operator that doesn't use opaque_data
3. Extend plan structure carefully to maintain ABI stability

---

## Timeline Estimate

### Phase 2C+ (If Pursued)
1. **Week 1**: Define K-fusion metadata structure, update exec_plan.h
2. **Week 2**: Implement expand_multiway_k_fusion(), integrate with rewrite_multiway_delta()
3. **Week 3**: Testing, performance validation, DOOP breakthrough attempt

**Total**: 2-3 weeks (same as current K-fusion infrastructure work)

---

## Recommendation

### Current Status: ✅ Infrastructure Ready
- K-fusion merge algorithm: Complete and tested
- Operator dispatch: Complete and tested
- Unit tests: Passing
- Regression tests: All 20 passing

### Go/No-Go Decision:
**✅ GO** on current sequential implementation for Phase 2B completion.
**Future**: Implement K-fusion plan generation in Phase 2C+ when:
1. Architecture decisions are finalized
2. Timeline permits additional 2-3 week sprint
3. DOOP breakthrough becomes primary priority

---

## References

- **K-FUSION-ARCHITECTURE.md**: Full architectural overview
- **K-FUSION-DESIGN.md**: Detailed technical design
- **SPECIALIST-REVIEW-SYNTHESIS.md**: Architect verification
- **exec_plan_gen.c**: Current plan generation implementation
- **columnar_nanoarrow.c**: Operator dispatch and merge functions
