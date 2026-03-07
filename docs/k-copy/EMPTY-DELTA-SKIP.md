# Empty-Delta Skip Optimization (Option B)

**Date:** 2026-03-08
**Status:** Design Document
**Target:** Skip K-copy passes when their designated FORCE_DELTA input produces zero rows

---

## Problem Statement

In multi-way delta expansion (K-copy), each copy forces one IDB body atom to use its delta relation while others use the full relation. In later semi-naive iterations, most deltas become empty (no new tuples for that relation), but the evaluator still executes all K copies — performing joins, filters, and maps that produce zero output.

### How K-Copy Execution Works

For a rule with K=3 IDB body atoms, the plan has 3 copies:

```
Copy 0: VARIABLE(R, FORCE_DELTA) → JOIN(S, FORCE_FULL) → JOIN(T, FORCE_FULL) → CONCAT
Copy 1: VARIABLE(R, FORCE_FULL) → JOIN(S, FORCE_DELTA) → JOIN(T, FORCE_FULL) → CONCAT
Copy 2: VARIABLE(R, FORCE_FULL) → JOIN(S, FORCE_FULL) → JOIN(T, FORCE_DELTA) → CONCAT
CONSOLIDATE
```

If `$d$S` (delta of S) is empty in some iteration:
- Copy 1 still loads R (full), attempts JOIN with empty S-delta → produces 0 rows
- The JOIN, subsequent ops, and CONCAT all execute on empty data
- This wastes time on the empty join + memcpy for CONCAT

### Wasted Work Per Empty Copy

For each empty-delta copy, the evaluator executes:

| Operation | Cost | Result |
|-----------|------|--------|
| VARIABLE (FORCE_DELTA, empty) | O(1) lookup | Empty relation pushed |
| JOIN (empty left × full right) | O(old_nrows * ncols) setup | Zero output rows |
| Additional JOINs, MAPs, FILTERs | Various | Zero output (propagated) |
| CONCAT | O(nrows * ncols) memcpy | Copies non-empty side unchanged |

The JOIN setup cost is non-trivial: even with an empty left input, the right relation is looked up and prepared. For CSPA with nrows ~20K and ncols=2, this is ~160KB of wasted memory access per empty copy per iteration.

---

## Current Code Analysis

### FORCE_DELTA in col_op_variable (lines 795-831)

```c
if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
    if (delta && delta->nrows > 0) {
        return eval_stack_push_delta(stack, delta, false, true);
    }
    /* No delta available: fall back to full relation */
    return eval_stack_push_delta(stack, full_rel, false, false);
}
```

**Key observation:** When `delta_mode == WL_DELTA_FORCE_DELTA` and no delta exists (or delta has 0 rows), the code falls back to the full relation. This means:
- On iteration 0 (no deltas yet): all copies use full relation → correct base case
- On iteration N where delta is empty: falls back to full → **produces duplicate work** that CONSOLIDATE must later remove

This fallback behavior is correct for the base case (iteration 0) but wasteful in later iterations where an empty delta genuinely means "no new facts for this relation."

### FORCE_DELTA in col_op_join right-side (lines 985-1010)

```c
if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
    col_rel_t *rdelta = session_find_rel(sess, rdname);
    if (rdelta && rdelta->nrows > 0) {
        right = rdelta;
        used_right_delta = true;
    }
    /* else: no delta available - fall through using full right relation */
}
```

Same pattern: falls back to full when delta is empty.

---

## Proposed Design

### Approach: Skip at Relation Plan Level

The cleanest skip point is in `col_eval_relation_plan` (lines 1971-2013), detecting when a FORCE_DELTA VARIABLE will produce zero rows and short-circuiting the entire copy.

However, `col_eval_relation_plan` evaluates a flat op array — it doesn't know copy boundaries. The skip must happen at a level that understands K-copy structure.

### Recommended: Skip in col_op_variable with empty-result propagation

**Phase 1: Mark empty-delta results**

When `FORCE_DELTA` encounters an empty or missing delta (after iteration 0), push an empty relation with a special marker:

```c
if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
    if (delta && delta->nrows > 0) {
        return eval_stack_push_delta(stack, delta, false, true);
    }
    /* After base case: delta is genuinely empty — push empty result
     * to signal downstream ops can short-circuit */
    col_rel_t *empty = col_rel_new_auto("$empty", full_rel->ncols);
    if (!empty) return ENOMEM;
    /* Copy schema but no data */
    col_rel_set_schema(empty, full_rel->ncols,
                       (const char *const *)full_rel->col_names);
    return eval_stack_push_delta(stack, empty, true, true);
}
```

**Phase 2: Short-circuit JOIN on empty input**

In `col_op_join`, detect empty left input early:

```c
static int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack,
            wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    col_rel_t *left = left_e.rel;

    /* Short-circuit: if left is empty, skip the join entirely */
    if (left && left->nrows == 0) {
        col_rel_t *empty_out = col_rel_new_auto("$join", /* output ncols */);
        if (left_e.owned) { col_rel_free_contents(left); free(left); }
        return eval_stack_push(stack, empty_out, true);
    }

    // ... existing join logic ...
}
```

Similarly for `col_op_antijoin`, `col_op_semijoin`, `col_op_filter`, `col_op_map`.

**Phase 3: CONCAT handles empty copies gracefully**

CONCAT already works correctly with empty relations — it just copies the non-empty side. No changes needed, but the boundary tracking (from Option A) naturally records fewer segments.

### Alternative: Pre-scan at evaluator level

Before evaluating a K-copy block, scan which deltas exist and skip entire copies:

```c
/* In col_eval_relation_plan or a wrapper */
for (uint32_t copy = 0; copy < k; copy++) {
    /* Check if this copy's designated delta is empty */
    const char *delta_rel = get_copy_delta_name(rplan, copy);
    col_rel_t *delta = session_find_rel(sess, delta_rel);
    if (delta && delta->nrows == 0) {
        /* Skip this copy entirely — push empty for CONCAT */
        col_rel_t *empty = col_rel_new_auto("$skip", nc);
        eval_stack_push(stack, empty, true);
        advance_ops_to_next_concat(rplan, &op_idx);
        continue;
    }
    /* Execute copy normally */
    // ...
}
```

This is more complex because it requires the evaluator to understand K-copy boundaries in the flat op array. The per-op short-circuit approach (Phase 1-2) is simpler and achieves the same result.

---

## Pseudocode: Per-Op Short-Circuit

### Modified col_op_variable

```c
static int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess)
{
    col_rel_t *full_rel = session_find_rel(sess, op->relation_name);
    if (!full_rel) return ENOENT;

    char dname[256];
    snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
    col_rel_t *delta = session_find_rel(sess, dname);

    if (op->delta_mode == WL_DELTA_FORCE_FULL) {
        return eval_stack_push_delta(stack, full_rel, false, false);
    }

    if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows > 0) {
            return eval_stack_push_delta(stack, delta, false, true);
        }

        /* Delta is empty or missing.
         * If delta relations have been registered (not iteration 0),
         * this copy will produce no new tuples → push empty. */
        if (delta) {
            /* delta exists but is empty → genuinely no new facts */
            col_rel_t *empty = col_rel_new_auto("$empty", full_rel->ncols);
            if (!empty) return ENOMEM;
            return eval_stack_push_delta(stack, empty, true, true);
        }

        /* No delta registered yet (iteration 0): fall back to full */
        return eval_stack_push_delta(stack, full_rel, false, false);
    }

    /* WL_DELTA_AUTO */
    bool use_delta = (delta && delta->nrows > 0 && delta->nrows < full_rel->nrows);
    col_rel_t *rel = use_delta ? delta : full_rel;
    return eval_stack_push_delta(stack, rel, false, use_delta);
}
```

### Modified col_op_join (early exit on empty left)

```c
static int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack,
            wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel) return EINVAL;

    /* Early exit: empty left input → empty join output */
    if (left_e.rel->nrows == 0) {
        /* Determine output column count */
        uint32_t out_ncols = op->project_count;
        col_rel_t *empty_out = col_rel_new_auto("$join", out_ncols);
        if (!empty_out) {
            if (left_e.owned) {
                col_rel_free_contents(left_e.rel);
                free(left_e.rel);
            }
            return ENOMEM;
        }
        if (left_e.owned) {
            col_rel_free_contents(left_e.rel);
            free(left_e.rel);
        }
        return eval_stack_push(stack, empty_out, true);
    }

    // ... existing join logic (unchanged) ...
}
```

The same pattern applies to `col_op_antijoin`, `col_op_semijoin`, `col_op_filter`, `col_op_map`, and `col_op_reduce` — check for `nrows == 0` on input and short-circuit with an appropriately-shaped empty output.

---

## Code Changes Required

### File: `wirelog/backend/columnar_nanoarrow.c`

| Line Range | Change | Description |
|------------|--------|-------------|
| 795-831 (col_op_variable) | **Modify** | Push empty relation when FORCE_DELTA has empty/missing delta (post-iteration 0) |
| 835-980 (col_op_join) | **Add early exit** | Check `left->nrows == 0` → return empty output |
| 985-1010 (join right-delta) | **Add early exit** | Check right delta `nrows == 0` → return empty output |
| ~1050-1150 (col_op_antijoin) | **Add early exit** | Same pattern for empty left |
| ~1150-1250 (col_op_semijoin) | **Add early exit** | Same pattern for empty left |

### No changes needed to:
- `exec_plan_gen.c` (plan structure unchanged)
- `col_op_concat` (already handles empty relations correctly)
- `col_op_consolidate` (fewer rows = faster, no special handling needed)

---

## Iteration-Level Impact Analysis

For CSPA with K=3 (3 IDB relations: path, reach, same_generation):

| Iteration | Non-empty deltas | Active copies | Skipped copies | Savings |
|-----------|-----------------|---------------|----------------|---------|
| 0 | All 3 (base case) | 3 | 0 | 0% |
| 1-10 | Typically 2-3 | 2-3 | 0-1 | 0-33% |
| 10-50 | Typically 1-2 | 1-2 | 1-2 | 33-67% |
| 50-100 | Typically 1 | 1 | 2 | 67% |
| 100+ | 0-1 | 0-1 | 2-3 | 67-100% |

**Weighted average:** In later iterations (which dominate total time due to larger relation sizes), typically only 1-2 of 3 relations have new facts. Average skip rate: ~40-50% of copies.

---

## Performance Estimates

### Cost saved per skipped copy

Per skipped copy, we avoid:
- JOIN lookup + setup: ~10-50us (depends on right relation size)
- Join execution on empty left: O(1) but with function call overhead
- CONCAT memcpy: O(nrows_other * ncols) — this is the biggest per-row cost

### Total estimated savings

For CSPA (100+ iterations, K=3):
- Average copies skipped per iteration: ~1.2 (conservative)
- Average cost per skipped copy: ~50us (join setup) + variable CONCAT savings
- JOIN setup savings: 100 iterations * 1.2 * 50us = 6ms (negligible)
- **Real savings come from reduced CONSOLIDATE input**: fewer rows → faster sort/merge

If 40% of copies are skipped → M is reduced by ~40% on average:
- Consolidate with K-way merge (Option A): O(M * (log(M/K) + log K))
- With 40% reduction in M: ~40% reduction in consolidate time
- Consolidate is 52-63% of wall time → 0.4 * 0.55 * 27.3s = **6.0s savings**

### Expected CSPA Results

| Metric | Baseline | Option B Only | With Option A | Change |
|--------|----------|---------------|---------------|--------|
| Wall time | 27,361 ms | 24,000-26,000 ms | 13,000-17,000 ms | -5-15% alone, -38-52% combined |
| Peak RSS | 5.3 GB | ~5.3 GB | ~5.3 GB | No change |
| Tuples | 20,381 | 20,381 | 20,381 | Must match |

---

## Correctness Considerations

### Iteration 0 (Base Case)

On iteration 0, no delta relations are registered in the session. The modified `col_op_variable` detects this (delta == NULL) and falls back to the full relation, preserving the existing base-case behavior.

### Distinguishing "no delta yet" from "delta is empty"

- **No delta registered** (`delta == NULL`): Iteration 0 or first evaluation. Fall back to full relation.
- **Delta registered but empty** (`delta->nrows == 0`): A genuine empty delta. Push empty result to skip.
- **Delta registered and non-empty** (`delta->nrows > 0`): Normal delta evaluation.

The session's delta relation lifecycle handles this correctly:
1. Iteration 0: No `$d$X` relations exist → NULL → fall back to full
2. Iteration 1+: `$d$X` created by `col_op_consolidate_incremental_delta` → may be empty or non-empty

### Fixed-Point Convergence

Skipping copies with empty deltas does NOT affect convergence:
- A copy with FORCE_DELTA on relation R and empty `$d$R` would produce no new tuples anyway
- The full relation fallback was producing duplicates that CONSOLIDATE removed
- Skipping eliminates both the redundant computation AND the redundant consolidation work

---

## Integration with Option A (K-Way Merge)

The two optimizations are fully complementary:

1. **Option B reduces input volume:** Fewer active copies → fewer segments in K-way merge
2. **Option A handles remaining segments efficiently:** K'-way merge where K' = active copies

Combined flow:
```
Iteration N:
  Copy 0 (FORCE_DELTA R): delta_R is non-empty → execute → produces M0 rows
  Copy 1 (FORCE_DELTA S): delta_S is EMPTY → SKIP (push empty)
  Copy 2 (FORCE_DELTA T): delta_T is non-empty → execute → produces M2 rows

  CONCAT: merges [M0 rows] + [0 rows] + [M2 rows] with 2 segment boundaries
  CONSOLIDATE: 2-way merge (not 3-way) on M0+M2 rows (not M0+M1+M2)
```

The segment boundary tracking from Option A naturally adapts: empty copies contribute zero-length segments that the merge skips.

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Incorrect iteration-0 behavior | Low | Explicit NULL check distinguishes "no delta" from "empty delta" |
| Breaking convergence | Very Low | Empty delta copies produce no new tuples regardless |
| Output column count mismatch | Medium | Must correctly compute output ncols for empty join result |
| Memory leak on early exit | Medium | Must free owned inputs on all exit paths |
| Interaction with CSE cache | Low | Materialized results are per-iteration; skip doesn't affect cache |

---

## Implementation Order

1. **Modify `col_op_variable`** — push empty when FORCE_DELTA has empty delta (not NULL)
2. **Add early exits to `col_op_join`** — short-circuit on empty left input
3. **Add early exits to `col_op_antijoin`, `col_op_semijoin`** — same pattern
4. **Verify:** All 15 workloads pass, CSPA produces 20,381 tuples
5. **Measure:** 3 CSPA runs, compare to baseline

---

## Summary

Skip K-copy passes with empty FORCE_DELTA input by:
1. Pushing empty relations instead of falling back to full (post-iteration 0)
2. Short-circuiting JOIN/ANTIJOIN/SEMIJOIN on empty input
3. Letting CONCAT and CONSOLIDATE naturally handle fewer rows

**No plan structure changes.** The optimization is purely in the evaluator's handling of empty inputs, with savings propagating through reduced CONSOLIDATE input volume.
