# Phase 3D-Ext: Incremental Evaluation Roadmap

## Overview

Phase 3D-Ext implemented frontier-based filtering infrastructure for skipping redundant stratum evaluation. However, the current implementation is **dormant code** unreachable in the single-call evaluation model. This document designs semantic fixes required before activation in Phase 4+ incremental re-evaluation.

## The Problem: Cross-Stratum Iteration Comparison

### Current Semantics

In `col_eval_stratum` (line 3243-3244):
```c
if (sess->frontier.iteration > 0 && iter > sess->frontier.iteration &&
    sess->frontier.stratum < stratum_idx) {
    continue;  // Skip redundant stratum
}
```

### The Bug

The condition compares semantically unrelated iteration counters:

- **`iter`**: Local fixed-point counter for the **current stratum**
  - Ranges 0..N for stratum K's convergence
  - Resets to 0 when moving to next stratum
  - Example: stratum 0 converges in 5 iterations, stratum 1 converges in 10 iterations

- **`sess->frontier.iteration`**: From the **previous stratum's** convergence
  - Remembers where stratum K-1 finished
  - Does not reflect stratum K's progress
  - Example: if stratum 0 finished at iter=5, frontier.iteration=5

### Why This Breaks

In multi-recursive-stratum programs:

```
Stratum 0: converges at iteration 5
Stratum 1: needs iterations 0..9 to converge
Stratum 2: needs iterations 0..3 to converge

Frontier after stratum 0: (5, 0)

At stratum 2, iteration 4:
  - iter = 4 (stratum 2's local counter)
  - frontier.iteration = 5 (stratum 0's final iteration)
  - frontier.stratum = 0

Current condition: iter(4) > frontier.iteration(5)? NO -> Don't skip ✓

BUT: This comparison is meaningless. Stratum 2's iteration 4 has no
relationship to stratum 0's iteration 5. We're comparing apples (stratum 2)
to oranges (stratum 0).
```

In incremental re-evaluation where frontier persists across calls, the bug manifests:

```
After first session_step call:
  - Stratum 0 processes iterations 0..5
  - Stratum 1 processes iterations 0..9
  - Frontier set to (9, 1) at end

In second session_step call (incremental):
  - Stratum 0 skipped (already done)
  - Stratum 1, iteration 10:
    - iter = 10 (continuing from iteration 9)
    - frontier.iteration = 9 (from previous stratum? or end of prior call?)
    - frontier.stratum = 1

This becomes ambiguous: which stratum's frontier are we comparing to?
```

## Design Options

### Option 1: Per-Stratum Frontier Array

**Approach**: Store frontier per stratum instead of single session-wide value.

```c
// In wl_col_session_t:
col_frontier_t frontiers[MAX_STRATA];  // One frontier per stratum
```

**Semantics**:
- `frontiers[0]` = max iteration reached in stratum 0
- `frontiers[1]` = max iteration reached in stratum 1
- etc.

**Skip condition** (fixed):
```c
if (iter > sess->frontiers[stratum_idx].iteration) {
    continue;  // Skip: this stratum already converged at iter <= previous
}
```

**Pros**:
- ✅ Correct semantics: compare stratum against itself
- ✅ Enables true incremental evaluation: know convergence point per stratum
- ✅ Supports multi-recursive-stratum programs correctly
- ✅ Clean, explicit design

**Cons**:
- ❌ More memory (1 frontier per stratum vs 1 global)
- ❌ Requires initialization per stratum
- ❌ Requires update at end of each stratum (vs once per session)

**Recommended for**: Long-term incremental evaluation (Phase 4+)

### Option 2: Same-Stratum Comparison Only

**Approach**: Only skip if we're re-evaluating the **same stratum** that previously converged.

```c
// Only skip if both:
// 1. This is a stratum re-evaluation (we've seen this stratum before)
// 2. We've already converged in this stratum
if (sess->frontier.stratum == stratum_idx &&  // Same stratum
    iter > sess->frontier.iteration) {         // Already converged
    continue;
}
```

**Semantics**:
- Skip only when re-evaluating a stratum that already converged
- Prevents cross-stratum iteration comparison
- First evaluation of each stratum always runs (no skip)

**Pros**:
- ✅ Minimal code change
- ✅ Correct semantics for same-stratum reuse
- ✅ No new memory allocation
- ✅ Low complexity

**Cons**:
- ❌ Limited benefit: only skips re-evaluation of same stratum
- ❌ Doesn't enable true multi-iteration incremental eval
- ❌ Assumes strata are processed sequentially (not parallelized)

**Recommended for**: Short-term, conservative optimization (Phase 3D-Ext+)

### Option 3: Break Instead of Continue

**Approach**: Use `break` to exit the stratum loop entirely once frontier is reached.

```c
if (iter > sess->frontier.iteration &&
    sess->frontier.stratum < stratum_idx) {
    break;  // This stratum has converged, don't evaluate further
}
```

**Semantics**:
- Assumes frontier.iteration is the **convergence point for all prior strata**
- Once `iter` exceeds this point, the current stratum doesn't need further iterations
- Exits iteration loop, moves to next stratum

**Pros**:
- ✅ Simple semantic: frontier is convergence checkpoint
- ✅ Works for sequential stratum evaluation
- ✅ Minimal code change

**Cons**:
- ❌ Still has cross-stratum comparison issue
- ❌ Break vs continue creates different behavior (could mask bugs)
- ❌ Doesn't handle multi-recursive-stratum programs correctly

**Not recommended**: Cross-stratum semantics still problematic

## Recommendation: Option 1 (Per-Stratum Frontiers)

For Phase 4+ incremental re-evaluation, implement **Option 1: Per-Stratum Frontier Array**.

### Why

1. **Correctness first**: Eliminates semantic ambiguity completely
2. **Future-proof**: Enables true incremental evaluation with multiple strata
3. **Clear semantics**: Each stratum tracks its own convergence point
4. **Scalable**: Works for any number of recursive strata

### Implementation Path

```c
// Phase 4: Incremental Re-evaluation
// 1. Expand wl_col_session_t with frontiers[MAX_STRATA]
// 2. Initialize frontiers in col_session_create()
// 3. Update frontier[stratum_idx] after each stratum evaluation
// 4. Replace line 3243 skip condition with per-stratum check
// 5. Add tests for multi-stratum incremental scenarios
```

### Activation Trigger

Activate Option 1 when:
- Phase 4 incremental re-evaluation is implemented
- Session frontier persists across multiple `session_step` calls
- Benefit demonstrated: >2x speedup on join-heavy workloads with repeated queries

## Current Status

- **Phase 3D-Ext**: Infrastructure in place, dormant code, documented
- **Phase 3D-Ext-Followup**: Design roadmap complete (this document)
- **Phase 4**: Implement Option 1 per-stratum frontiers
- **Phase 4+**: Incremental re-evaluation with persistent frontiers

## References

- **Architect Review**: a2a42d1fa88a8d650 (conditional approval, dormant status verified)
- **Documentation**: progress.txt Phase 3D-Ext section
- **Code**: wirelog/backend/columnar_nanoarrow.c lines 3238-3246
- **Tests**: tests/test_frontier_skip_integration.c (dormancy verification)

## Impact Assessment

| Aspect | Impact |
|--------|--------|
| **Correctness** | ✅ Bug fixed by Option 1 |
| **Performance** | ✅ 2-4x speedup potential (Phase 4) |
| **Complexity** | ⚠️ Moderate (per-stratum tracking) |
| **Backward Compat** | ✅ No impact (dormant code) |
| **Timeline** | Phase 4+ (incremental eval) |

---

**Document Status**: Complete
**Date**: March 8, 2026
**Author**: Claude Code (architecture design)
**For**: Phase 4+ Incremental Evaluation Planning
