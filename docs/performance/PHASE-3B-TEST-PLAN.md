# Phase 3B Test Plan: Timestamped Delta Tracking & Incremental Consolidation

**Date:** 2026-03-08
**Author:** Gemini (validation specialist)
**Phase:** 3B (Weeks 4-8)
**Status:** DRAFT — prepared ahead of Phase 3B start

---

## Context

Phase 3B introduces per-record timestamp tracking to replace relation-level snapshot
diffing. This is the highest-ROI change for closing the 91x DOOP gap (~40-50x factor).

**Key invariant:** With `WL_FEATURE_TIMESTAMPS` ON, all programs must produce
**identical tuple counts** to the Phase 2D baseline. Timestamps are metadata only —
they must never affect set semantics or row equality.

---

## Testing Pyramid

```
10% e2e         (CSPA/DOOP correctness gates, Phase 2D parity)
30% integration (timestamp propagation through operators, K-fusion interaction)
60% unit        (col_rel_t struct, each operator's ts propagation, incremental consolidate)
```

---

## Task 3B-1: Timestamp Infrastructure Unit Tests

**File:** `tests/test_timestamp_infrastructure.c`

### T1-1: col_rel_t timestamp allocation

Verify `timestamps` array is allocated alongside `data` in `col_rel_alloc()`.
- `col_rel_new("r", 2, 10)` → `rel->timestamps != NULL`
- `rel->timestamps` has capacity `>= 10`
- All initial timestamp values = 0

### T1-2: Timestamp freed with contents

Verify `col_rel_free_contents()` frees the `timestamps` array without double-free.
- Allocate relation, free contents → ASAN clean

### T1-3: Feature flag OFF = zero behavioral change

With `WL_FEATURE_TIMESTAMPS=0` (compile-time flag):
- All 21 tests pass without modification
- No timestamp fields allocated (struct unchanged)
- Binary footprint unchanged

### T1-4: Timestamp array bounds safety

Write `timestamps[nrows-1]`, verify no out-of-bounds.
- ASAN clean on append operations up to capacity

### T1-5: col_rel_append_row propagates caller-supplied timestamp

`col_rel_append_row(rel, row, ts=3)` → `rel->timestamps[rel->nrows-1] == 3`

---

## Task 3B-2: Timestamp Propagation Unit Tests

**File:** `tests/test_timestamp_propagation.c`

### Propagation Rules Under Test

| Operator | Rule | Test |
|----------|------|------|
| VARIABLE | EDB→0, IDB delta→current_iter | T2-1 |
| MAP | preserve input ts | T2-2 |
| FILTER | preserve input ts | T2-3 |
| JOIN | output ts = max(left_ts, right_ts) | T2-4 |
| ANTIJOIN | preserve left ts | T2-5 |
| REDUCE | output ts = max(group input ts) | T2-6 |
| CONCAT | preserve each input ts | T2-7 |
| CONSOLIDATE | keep first occurrence ts | T2-8 |
| SEMIJOIN | preserve left ts | T2-9 |

### T2-4: JOIN timestamp = max(left, right)

```
left:  row(1,2) ts=1, row(3,4) ts=3
right: row(2,5) ts=2, row(4,6) ts=1

JOIN on col[1]=col[0]:
  (1,2)∘(2,5): output ts = max(1,2) = 2  → row(1,2,5) ts=2
  (3,4)∘(4,6): output ts = max(3,1) = 3  → row(3,4,6) ts=3
```

### T2-8: CONSOLIDATE preserves first-occurrence timestamp

After dedup, the surviving row must carry the timestamp of its FIRST occurrence
(not any duplicate's timestamp). This ensures monotonicity.

### T2-10: Timestamps excluded from row equality

Rows with identical data but different timestamps must be considered equal by
dedup and comparison functions (`kway_row_cmp`, `memcmp` on data only).

---

## Task 3B-3: Incremental CONSOLIDATE Tests

**File:** `tests/test_consolidate_incremental_ts.c`

### T3-1: Partition correctness

Given relation with `N` old rows (ts < current_iter) and `D` new rows (ts == current_iter):
- Old prefix is already sorted and must NOT be re-sorted
- Only new rows are sorted: O(D log D)
- Result is fully sorted and deduplicated

### T3-2: Already-sorted prefix stability

Insert 100 old rows (ts=1) sorted, then 5 new rows (ts=2). Verify:
- Old prefix unchanged in memory
- New rows merged correctly
- Total = sorted, deduplicated union

### T3-3: Correctness against full-sort baseline

For each CSPA iteration input, compare incremental-ts output against qsort baseline:
- Identical tuple sets (order may differ; verify by sorting both and comparing)
- Tuple counts match exactly

### T3-4: Performance regression guard

Incremental CONSOLIDATE on CSPA iter 3 data (mostly old rows):
- Must be faster than full sort (>5x improvement target)
- Measured as: D ≪ N case where D/N < 0.01

### T3-5: Dedup timestamp resolution

When new row (ts=2) duplicates old row (ts=1), old row's timestamp is preserved:
- Output has old row with ts=1 (first occurrence wins)
- Duplicate new row discarded

---

## Task 3B-4: Timestamp-Based Delta Tests

**File:** `tests/test_delta_timestamp.c`

### T4-1: Delta = rows with ts == current_iteration

After iteration `N`, delta extraction must return exactly the rows whose
timestamp equals `N`. Previously-derived rows (ts < N) must not appear in delta.

### T4-2: Fixed-point detection via empty delta

When no new rows are produced (delta is empty), convergence is detected.
- Iteration count matches Phase 2D baseline for TC, CSPA

### T4-3: Parity with snapshot-diff delta

For 3 iterations of TC program:
- Snapshot-diff delta (current) vs timestamp-based delta (new)
- Must produce identical row sets at each iteration

### T4-4: Delta correctness for K-fusion

After K-fusion parallel evaluation with timestamp propagation:
- Combined result has correct timestamps (max of K-copy timestamps)
- Delta extraction gives correct new rows

---

## Task 3B-5: K-Fusion + Timestamps Integration Tests

**Extends:** `tests/test_k_fusion_e2e.c` (new test cases added)

### T5-1: K-copy skips evaluation when delta is empty (timestamp-aware)

A K-fusion worker whose forced-delta input has NO rows with `ts == current_iter`
must skip `wl_workqueue_submit()` entirely. Verify via workqueue task count.

### T5-2: K-fusion with timestamp propagation preserves correctness

Run K=2 TC program with timestamps enabled:
- 6-tuple result (same as Phase 3A test T1)
- All timestamps correctly assigned

### T5-3: Mixed empty/non-empty K-copies

In a K=4 fusion where K=2 copies have empty deltas and K=2 have non-empty:
- Only 2 tasks submitted to workqueue
- Result is correct (same tuple count)

---

## Phase 3B Acceptance Gate (Week 8)

### Correctness Gate (MANDATORY)
- [ ] All tests pass with `WL_FEATURE_TIMESTAMPS=0` (zero regression)
- [ ] All tests pass with `WL_FEATURE_TIMESTAMPS=1`
- [ ] CSPA: 20,381 tuples, 6 iterations (Phase 2D baseline preserved)
- [ ] DOOP: tuple count matches Phase 2D baseline (captured from background run)
- [ ] ASAN clean: all tests

### Performance Gate
- [ ] CSPA median < 2.0s (3-run median, release build)
- [ ] DOOP < 5 minutes (from 71m50s baseline)
- [ ] Incremental CONSOLIDATE >5x faster on late iterations (D/N < 0.01)

### Architecture Gate
- [ ] `timestamps` array is out-of-band (no changes to data layout)
- [ ] Row comparison functions unchanged (`kway_row_cmp`, `memcmp`)
- [ ] TSan clean for all K-fusion paths
- [ ] Feature flag works: OFF = identical binary to Phase 2D

---

## Known Risks for Phase 3B

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Timestamp propagation bug in JOIN (wrong max selection) | Medium | High | T2-4 unit test with explicit values |
| Incremental merge leaves unsorted prefix | Low | High | T3-1 partition correctness test |
| CONSOLIDATE dedup breaks timestamp invariant | Medium | High | T2-8 first-occurrence ts preservation test |
| K-fusion empty-skip incorrectly skips non-empty delta | Medium | Critical | T5-1 task count verification |
| Memory overhead of timestamps array per relation | Low | Medium | Profile RSS before/after |

---

## Test File Registry (Phase 3B)

| File | Tests | Priority |
|------|-------|----------|
| `tests/test_timestamp_infrastructure.c` | T1-1 to T1-5 | HIGH |
| `tests/test_timestamp_propagation.c` | T2-1 to T2-10 | HIGH |
| `tests/test_consolidate_incremental_ts.c` | T3-1 to T3-5 | HIGH |
| `tests/test_delta_timestamp.c` | T4-1 to T4-4 | MEDIUM |
| `tests/test_k_fusion_e2e.c` (extended) | T5-1 to T5-3 | MEDIUM |

Estimated new test lines: ~800-1000 (following existing patterns)

---

## Dependency on Phase 3A

Phase 3B tests REQUIRE Phase 3A gate to pass first:
- KI-1 (dedup bug) must be fixed → correct K-fusion results under timestamps
- True parallel dispatch → TSan validation meaningful

**Do NOT start Phase 3B implementation until Phase 3A gate passes.**

---

## References

- `docs/timely/TIMELY-PHASE-3-PLAN.md:213-362` — Phase 3B task specifications
- `wirelog/backend/columnar_nanoarrow.c:2032-2120` — `col_op_consolidate_incremental_delta()` (reference implementation)
- `docs/performance/PHASE-3A-TEST-PLAN.md` — Phase 3A gate criteria
