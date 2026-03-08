# Empty-Delta Skip Optimization Design (US-005)

## 1. Executive Summary

**Problem**: In the semi-naive fixed-point loop, when a FORCE_DELTA position has an empty or absent
delta on iteration N>0, the evaluator falls back to the full relation. This produces a JOIN result
that contains only duplicates of rows already in the accumulated output. The JOIN computation, and
the subsequent CONSOLIDATE to remove those duplicates, are entirely wasted.

**Solution**: Skip relation plan evaluation when it has empty forced-delta positions and iter > 0.

**Expected Improvement**: 5-15% wall-time reduction in later iterations (smaller impact than K-way
merge CONSOLIDATE, which is the dominant optimization). Effect is more pronounced in recursive
queries with many strata that converge at different rates.

**Approach**: Option B (pre-scan skip) as the primary optimization, plus Option C (post-eval nrows
check) as a safety net for edge cases.

---

## 2. Problem Statement

### Current Flow

The semi-naive fixed-point loop (`columnar_nanoarrow.c:2497-2668`) iterates until no new rows are
produced. Each iteration evaluates every relation plan and accumulates results. The loop tracks
iteration number (`iter`) but the per-operator logic does not use it.

### Root Cause

`col_op_variable()` at line 862-868 contains a FORCE_DELTA fallback:

```c
// columnar_nanoarrow.c:862-868
if (delta_rel == NULL || delta_rel->nrows == 0) {
    // Fall back to full relation
    result = full_rel;
}
```

This fallback exists to handle iteration 0, where no delta relations exist yet and the full
relation is needed to seed the computation. However, the same fallback also triggers on iteration
N>0 when the delta exists but is empty (zero rows). In that case, the fallback is incorrect.

### What Goes Wrong on Iteration N>0

1. Delta exists but has zero rows (produced last iteration but accumulated nothing new)
2. Fallback fires: full relation used instead
3. JOIN with full relation executes (potentially expensive)
4. Result contains rows already in the accumulated output (all of them, in the worst case)
5. CONSOLIDATE removes the duplicates
6. Net delta is zero rows — same as if we had skipped

The JOIN computation and the CONSOLIDATE work are entirely wasted.

### Where Empty Deltas Come From

`columnar_nanoarrow.c:2650-2656`: after each iteration, the delta for the next iteration is
computed as the difference between the new result and the previous accumulated relation. If no new
rows were produced, the delta is zero rows and is not stored (or stored as an empty relation) for
the next iteration.

---

## 3. Current Implementation Details

### FORCE_DELTA Fallback (line 862-868)

```c
if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
    col_rel_t *delta_rel = session_find_rel(sess, delta_name);
    if (delta_rel == NULL || delta_rel->nrows == 0) {
        // Fallback: use full relation
        result = full_rel;
    } else {
        result = delta_rel;
    }
}
```

- **Why it exists**: Iteration 0 has no delta relations yet. Using the full relation seeds the
  computation correctly.
- **The problem**: The same code path runs when delta_rel exists but is empty (iter > 0). In that
  case the full relation is a wrong input: all rows it contains are already accumulated.

### FORCE_DELTA Annotation (exec_plan_gen.c:1008-1009)

The plan generator marks recursive positions with `WL_DELTA_FORCE_DELTA`. Non-recursive positions
use `WL_DELTA_AUTO` and are not affected by this optimization.

### Semi-naive Loop Structure (line 2497-2668)

```
iter = 0
while changed:
    for each relation plan rp:
        result = col_eval_relation_plan(rp)   // line 2534
        append result to accumulated
        compute delta for next iter
    iter++
```

The `iter` variable is available at the loop level but not passed into `col_eval_relation_plan`.

---

## 4. Proposed Solutions

### Option B: Pre-scan Skip (Recommended Primary)

**When**: Before `col_eval_relation_plan()` is called (insertion point: line 2528-2534).

**Check**: For any `WL_DELTA_FORCE_DELTA` op in the plan, does its delta relation exist and have
nrows > 0?

**Action**: If any FORCE_DELTA position has an empty or absent delta (and iter > 0), skip the
entire relation plan and continue to the next one.

**Pseudocode**:
```
if iter > 0:
  for each op in relation_plan.ops:
    if op.delta_mode == WL_DELTA_FORCE_DELTA:
      delta_rel = session_find_rel(delta_name)
      if delta_rel == NULL OR delta_rel.nrows == 0:
        skip this relation plan  // continue to next relation
```

**Benefit**: Avoids all wasted computation — JOIN, MAP, FILTER, CONSOLIDATE — for the skipped plan.

**Complexity**: O(op_count) scan per relation plan per iteration. op_count is typically small
(single digits), so overhead is negligible.

### Option C: Post-eval Nrows Check (Safety Net)

**When**: After `col_eval_relation_plan()` returns (insertion point: line 2542-2546).

**Check**: Is `result.rel->nrows == 0`?

**Action**: Free the result and continue to the next relation plan, skipping the append and
consolidate.

**Benefit**: Trivial to implement; catches any remaining zero-row results regardless of cause.

**Limitation**: Does not avoid the JOIN computation (already executed). Serves as a safety net only.

### Why NOT Option A: Changing FORCE_DELTA Fallback

Option A would fix the fallback by threading the iteration number into the operator-level logic so
that the full-relation fallback only fires when iter == 0.

**Rejected because**:
- Requires adding `iter` to the operator call signature (widening the API surface)
- Risk of breaking EDB-grounded recursive rules on iteration 0 (subtle correctness risk)
- The loop level (Options B/C) is the correct place to express iteration-level decisions
- Operator-level changes are harder to reason about and test in isolation

---

## 5. Correctness Proof

**Claim**: Skipping a relation plan that contains at least one empty FORCE_DELTA position (on
iter > 0) produces the same result as executing the plan and discarding the output.

**Proof**:

1. Let P be a relation plan with FORCE_DELTA position F.
2. On iter > 0, delta(F) is empty (0 rows).
3. The current fallback substitutes full_rel(F) for delta(F).
4. In a semi-naive JOIN, at least one argument must be a delta relation. If all delta positions are
   empty, the correct delta-JOIN result is 0 rows.
5. Substituting full_rel for an empty delta produces rows, but those rows are already in the
   accumulated output (by definition: accumulated = union of all prior results; iter > 0 means at
   least one prior iteration contributed those rows).
6. The append step adds those rows, and CONSOLIDATE removes them as duplicates.
7. Net contribution: 0 rows. Net change to accumulated output: none.
8. Skipping the plan is therefore equivalent.

**Cartesian product property**: If plan P has multiple FORCE_DELTA positions and any one of them
is empty, the JOIN result is 0 rows (JOIN with a 0-row relation produces 0 rows). Therefore any
empty FORCE_DELTA position is sufficient to skip the entire plan.

**Iteration 0 base case**: The `iter > 0` guard means iteration 0 always executes. At iter 0, no
delta relations exist, so the full-relation fallback is correct for seeding the computation.

---

## 6. Integration Points

### Option B Insertion (Primary)

Insertion after the `ri` loop header and before the `col_eval_relation_plan` call:

```c
// columnar_nanoarrow.c — within semi-naive loop, after line 2528
for (uint32_t ri = 0; ri < sess->rel_count; ri++) {
    wl_relation_plan_t *rp = &sess->rel_plans[ri];

    // NEW: Pre-scan for empty forced deltas (skip wasted JOIN computation)
    if (iter > 0) {
        bool has_empty_forced_delta = false;
        for (uint32_t oi = 0; oi < rp->op_count && !has_empty_forced_delta; oi++) {
            const wl_plan_op_t *op = &rp->ops[oi];
            if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
                const char *rname = get_relation_name(op);
                if (rname) {
                    char dname[256];
                    snprintf(dname, sizeof(dname), "$d$%s", rname);
                    col_rel_t *d = session_find_rel(sess, dname);
                    if (!d || d->nrows == 0) {
                        has_empty_forced_delta = true;
                    }
                }
            }
        }
        if (has_empty_forced_delta)
            continue;  // Skip this relation plan entirely
    }

    // Existing col_eval_relation_plan call (line ~2534)
    col_eval_result_t result = col_eval_relation_plan(sess, rp, scratch);
```

### Option C Insertion (Safety Net)

Insertion after the result is obtained and before the append:

```c
// columnar_nanoarrow.c — after col_eval_relation_plan returns (~line 2542)
col_eval_result_t result = col_eval_relation_plan(sess, rp, scratch);

if (stack.top == 0) {
    // ... existing error handling ...
}

// NEW: Skip zero-row results (safety net for any remaining empty-delta cases)
if (result.rel && result.rel->nrows == 0) {
    if (result.owned) {
        col_rel_free_contents(result.rel);
        free(result.rel);
    }
    continue;
}

// Existing append logic
```

---

## 7. Edge Cases

| Case | Handling |
|------|----------|
| Iteration 0 | `iter > 0` guard preserves full-relation fallback for seeding; pre-scan does not run |
| Non-recursive strata | Not evaluated inside semi-naive loop; optimization does not apply |
| `WL_DELTA_AUTO` ops | Pre-scan only checks `WL_DELTA_FORCE_DELTA`; AUTO positions are unaffected |
| EDB relation with FORCE_DELTA | EDB never has delta relations; skip is still correct (EDB rows already accumulated) |
| Only some FORCE_DELTA positions empty | Any empty FORCE_DELTA makes the full plan produce 0 rows (JOIN property); skip is correct |
| Multiple recursive IDB relations | Each relation plan is checked independently; plans with non-empty deltas still run |
| delta_name buffer overflow | Use `snprintf` with bounded buffer; relation names are bounded by parser limits |

---

## 8. Performance Expectations

| Dimension | Expected Change |
|-----------|----------------|
| Iteration count to fixpoint | No change (convergence criterion unchanged) |
| Per-iteration work (later iters) | Reduced — wasted JOIN computations avoided |
| Wall-time reduction | 5-15% on recursive queries; negligible on non-recursive |
| Cache locality | No change (different mechanism from K-way merge CONSOLIDATE) |
| Memory allocation | No change (skipped plans never allocate result relations) |
| Pre-scan overhead | Negligible — O(op_count) comparisons, no allocation |

The optimization has the largest effect in benchmarks with many recursive relations that converge
at different rates. In those cases, later iterations have many empty deltas, and the skip avoids
substantial redundant JOIN work.

---

## 9. Interaction with K-way Merge CONSOLIDATE

The K-way merge CONSOLIDATE optimization (`docs/consolidate-optimization/K-WAY-MERGE-DESIGN.md`)
operates at the in-plan CONSOLIDATE level: it merges pre-sorted runs to avoid quadratic row
comparison during deduplication.

Empty-delta skip operates at the relation plan level: it prevents the plan from executing at all.

**No conflicts**: The two optimizations are fully orthogonal.

**Combined behavior**:
- If Option B fires (empty delta detected), the plan is skipped entirely; K-way merge is never
  reached for that plan in that iteration.
- If Option B does not fire (delta is non-empty), the plan runs normally; K-way merge applies
  within CONSOLIDATE as usual.

The optimizations can be enabled independently or together. Combined, they target different sources
of wasted work: K-way merge reduces CONSOLIDATE cost; empty-delta skip eliminates entire plan
evaluations.

---

## 10. Testing Strategy

### Unit Tests (US-006): `test_empty_delta_skip.c`

| Test | Description |
|------|-------------|
| Test 1: Empty delta detection | iter > 0, delta NULL or nrows == 0 → pre-scan returns true |
| Test 2: Full relation fallback at iter 0 | iter == 0 → pre-scan does not run → plan executes normally |
| Test 3: Skip correctness | Single-step recursive query; verify result identical with and without skip |
| Test 4: Iteration count | Verify that enabling skip does not change fixpoint iteration count |
| Test 5: Cartesian product property | Plan with two FORCE_DELTA positions, one empty → skip fires |

### Integration Tests (US-008): Full 15-workload Benchmark

- All 15 workloads produce identical output rows vs. baseline (no correctness regression)
- Iteration counts match baseline on all workloads (convergence unchanged)
- Wall-time measured on recursive workloads; target: 5-15% reduction vs. baseline with empty-delta
  skip disabled

---

## 11. References

| Location | Description |
|----------|-------------|
| `columnar_nanoarrow.c:838-877` | `col_op_variable()` with FORCE_DELTA fallback |
| `columnar_nanoarrow.c:862-868` | Fallback logic (root cause) |
| `columnar_nanoarrow.c:2497-2668` | Semi-naive fixed-point loop |
| `columnar_nanoarrow.c:2528-2534` | Insertion point for Option B (pre-scan) |
| `columnar_nanoarrow.c:2542-2546` | Insertion point for Option C (post-eval check) |
| `columnar_nanoarrow.c:2650-2656` | Where empty deltas originate |
| `exec_plan_gen.c:1008-1009` | FORCE_DELTA annotation in plan generator |
| `docs/consolidate-optimization/K-WAY-MERGE-DESIGN.md` | K-way merge design (orthogonal, no conflicts) |
