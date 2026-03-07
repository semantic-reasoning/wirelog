# CONSOLIDATE Function Design: col_op_consolidate_incremental_delta

**Date:** 2026-03-07
**Status:** Complete - Ready for TDD Implementation
**Purpose:** Eliminate O(N × iterations) snapshot allocations in semi-naive evaluation loop

---

## 1. Function Specification

### Signature
```c
/*
 * col_op_consolidate_incremental_delta - Incremental consolidation with delta output
 *
 * PURPOSE:
 *   Merge pre-sorted old data with newly appended delta rows, while simultaneously
 *   emitting the set of truly-new rows (R_new - R_old) as a byproduct.
 *   This eliminates separate post-iteration merge walk needed for delta computation.
 *
 * PRECONDITIONS:
 *   - rel->data[0..old_nrows) is already sorted and unique (invariant)
 *   - rel->data[old_nrows..rel->nrows) contains newly appended delta rows (unsorted)
 *   - old_nrows <= rel->nrows
 *
 * POSTCONDITIONS:
 *   - rel->data[0..rel->nrows) is sorted and unique (new invariant)
 *   - delta_out->data contains exactly R_new - R_old (truly new rows)
 *   - delta_out->data is sorted in same order as rel->data
 *   - rel->nrows reflects final merged count
 *
 * MEMORY OWNERSHIP:
 *   - Caller allocates col_rel_t *delta_out (structure only)
 *   - Function allocates and owns delta_out->data (int64_t array)
 *   - Caller responsible for freeing delta_out->data via col_rel_free_contents()
 *   - If delta_out == NULL, new rows not collected (merge only)
 *
 * ERROR HANDLING:
 *   - Returns 0 on success
 *   - Returns ENOMEM if malloc fails
 *   - On error, rel and delta_out states are undefined; caller should not use
 *
 * THREAD SAFETY:
 *   - Uses qsort_r() with context parameter (no global state)
 *   - Safe for multi-threaded use (per-relation isolation)
 *
 * ALGORITHM COMPLEXITY:
 *   - Time: O(D log D + N + D) where D = new delta rows, N = old rows
 *   - Space: O(N + D) for merge buffer + delta_out buffer
 *   - Dominant term: O(N) when D << N (typical in late iterations)
 */
int col_op_consolidate_incremental_delta(
    col_rel_t *rel,           // IN/OUT: relation with sorted old + unsorted delta
    uint32_t old_nrows,       // IN: boundary - rows [0..old_nrows) already sorted+unique
    col_rel_t *delta_out      // OUT: newly discovered rows (R_new - R_old)
);
```

---

## 2. Algorithm: 3-Phase Approach

### Phase 1: Sort Delta Rows Only (O(D log D))
```
Input: rel->data = [sorted_old | unsorted_delta]
       rel->nrows = old_nrows + delta_count
       delta_count = rel->nrows - old_nrows

delta_start = rel->data + old_nrows * ncols
qsort_r(delta_start, delta_count, row_bytes, context, row_cmp_fn)
Output: rel->data = [sorted_old | sorted_delta]
```

**Rationale:** Only new rows are unsorted. Old data already sorted + unique.

### Phase 1b: Dedup Within Delta (O(D))
```
d_unique = 1
for i = 1 to delta_count - 1:
    if delta[i] != delta[i-1]:
        delta[d_unique++] = delta[i]
Output: delta_start now contains d_unique rows (duplicates removed)
```

**Rationale:** Eliminate duplicates before merge walk.

### Phase 2: Merge + Emit New Rows (O(N + D))
```
merged_buffer = malloc((old_nrows + d_unique) * ncols * sizeof(int64_t))
new_rows_buffer = malloc(d_unique * ncols * sizeof(int64_t))

oi = 0, di = 0, out = 0, new_out = 0
while oi < old_nrows AND di < d_unique:
    orow = rel->data[oi]
    drow = delta_start[di]

    if orow < drow:
        merged[out++] = orow; oi++          // Old row, not new
    elif orow == drow:
        merged[out++] = orow; oi++; di++    // Duplicate, skip from delta
    else:  // drow < orow (NEW)
        merged[out++] = drow                // Add to merged
        new_rows[new_out++] = drow          // EMIT: this row is new
        di++

// Handle remaining old rows (all go to merged, none are new)
while oi < old_nrows:
    merged[out++] = rel->data[oi++]

// Handle remaining delta rows (all are new)
while di < d_unique:
    merged[out++] = delta_start[di]
    new_rows[new_out++] = delta_start[di++]

Output: merged_buffer has final sorted+unique, new_rows has R_new - R_old
```

**Rationale:** Single merge pass simultaneously produces merged result and delta set.

### Phase 3: Swap Buffers (O(1))
```
free(rel->data)
rel->data = merged_buffer
rel->nrows = out
rel->capacity = old_nrows + d_unique

delta_out->data = new_rows_buffer
delta_out->nrows = new_out
delta_out->capacity = d_unique
```

---

## 3. Edge Cases

### Case 1: First Iteration (old_nrows == 0)
- All rows in rel are "new"
- Phase 1: qsort entire rel->data
- Phase 1b: Dedup entire rel->data
- Phase 2: Merge walk exits immediately (old loop condition false)
- Remaining delta loop copies all to merged and new_rows
- Result: delta_out contains all rows

### Case 2: No New Rows (delta_count == 0)
- Return immediately with delta_out->nrows = 0
- No work needed

### Case 3: All Delta Rows Duplicate in Old
- Phase 2: Merge walk keeps all old rows
- All delta rows matched (oi < orow), so none go to new_rows
- Result: delta_out->nrows = 0, merged unchanged

### Case 4: NULL delta_out
- Skip new_rows allocation and collection
- Merge still completes correctly
- Caller gets merged result, no delta information

---

## 4. Integration Points

### Current col_eval_stratum Pattern (REMOVE)
```c
// Lines 1883-1900: OLD_DATA snapshot (EXPENSIVE)
int64_t **old_data = malloc(nrels * sizeof(int64_t *));
for (uint32_t ri = 0; ri < nrels; ri++) {
    size_t bytes = snap[ri] * ncols * sizeof(int64_t);
    old_data[ri] = malloc(bytes);
    memcpy(old_data[ri], rel->data, bytes);  // O(N) per relation per iteration
}

// ... rule evaluation appends new rows ...

// Lines 2004-2069: Delta merge walk (EXPENSIVE)
for (uint32_t ri = 0; ri < nrels; ri++) {
    // Scan old_data[ri] vs new rel->data to find R_new - R_old
    // This is O(N + D) per relation per iteration
}

// Lines 2064-2069: Cleanup
for (uint32_t ri = 0; ri < nrels; ri++) {
    free(old_data[ri]);  // O(1)
}
free(old_data);
```

### New col_eval_stratum Pattern (REPLACE)
```c
// Lines 1883-1900 replaced with O(1) snapshot:
uint32_t *snap = malloc(nrels * sizeof(uint32_t));
for (uint32_t ri = 0; ri < nrels; ri++) {
    snap[ri] = rel->nrows;  // O(1) - just record row count
}

// ... rule evaluation appends new rows ...

// Lines 2004-2069 replaced with delta-integrated consolidation:
for (uint32_t ri = 0; ri < nrels; ri++) {
    col_rel_t *delta = col_rel_new_like(dname, rel);
    int rc = col_op_consolidate_incremental_delta(rel, snap[ri], delta);
    if (rc != 0) return rc;

    if (delta->nrows > 0) {
        delta_rels[ri] = delta;  // Use as next iteration's delta
        any_new = true;
    } else {
        col_rel_free_contents(delta);
        free(delta);
    }
}

// Cleanup is handled inside col_op_consolidate_incremental_delta
// No separate delta merge walk needed
```

---

## 5. Memory Efficiency

### Old Pattern Cost (per iteration per relation)
```
old_data snapshot:     malloc(N) + memcpy(N)        = O(N) time + O(N) memory
delta merge walk:      scan old_data + rel->data    = O(N+D) time
cleanup:               free(old_data)                = O(1)
Total per iteration:   O(N) + O(N+D)                ≈ O(N) dominated
Total for I iterations: I × O(N × N_avg)            = O(I × N²)
```

### New Pattern Cost (per iteration per relation)
```
col_op_consolidate_incremental_delta:
  Phase 1: qsort(delta)   = O(D log D)
  Phase 1b: dedup(delta)  = O(D)
  Phase 2: merge          = O(N + D)
  Phase 3: swap           = O(1)
  Total per iteration:     O(D log D + N)           ≈ O(N) when D << N
  Total for I iterations:  I × O(N_avg)            = O(I × N)
```

### Memory Savings
- **Old:** O(N) snapshot + O(N+D) merge buffer = O(N) peak per iteration
- **New:** O(N+D) merge buffer only = O(N) peak (no snapshot)
- **Net:** Eliminates O(N × iterations × relations) allocation overhead
- **For CSPA:** ~100 iterations × 3 relations × 20K rows = 6M allocations → 0 snapshots

---

## 6. Test Cases (for US-003 TDD RED)

### Test Case 1: Empty Old + Small Delta
```
Input:  old_nrows = 0
        delta = [row(1,2), row(3,4)]
        rel->nrows = 2
Expected: merged = [row(1,2), row(3,4)]
          delta_out = [row(1,2), row(3,4)]
Assertion: delta_out->nrows == 2, all rows present
```

### Test Case 2: Old + All-Duplicate Delta
```
Input:  old_nrows = 2, old = [row(1,2), row(3,4)]
        delta = [row(1,2), row(3,4)]
        rel->nrows = 4
Expected: merged = [row(1,2), row(3,4)]
          delta_out empty
Assertion: delta_out->nrows == 0, merged unchanged
```

### Test Case 3: Old + Unique Delta
```
Input:  old_nrows = 2, old = [row(1,2), row(3,4)]
        delta = [row(2,3), row(5,6)]
        rel->nrows = 4
Expected: merged = [row(1,2), row(2,3), row(3,4), row(5,6)] (sorted)
          delta_out = [row(2,3), row(5,6)]
Assertion: merged->nrows == 4, delta_out->nrows == 2, both sorted
```

### Test Case 4: First Iteration (old_nrows == 0)
```
Input:  old_nrows = 0
        delta = [many rows]
Expected: All rows are "new"
Assertion: delta_out->nrows == total_rows
```

### Test Case 5: Large Random Dataset
```
Input:  old = [1000 random sorted rows]
        delta = [500 new + 200 duplicates of old]
Expected: merged = union (1200 unique rows, sorted)
          delta_out = [500 new rows only]
Assertion: Brute-force verification: for each delta row, check membership in merged and in delta_out iff new
```

---

## 7. References

- Current implementation: `wirelog/backend/columnar_nanoarrow.c:1864–2070` (col_eval_stratum)
- Related function: `col_op_consolidate_incremental()` (lines 1405–1482) - base consolidation
- qsort_r integration: Phase A (US-001) - context parameter passing
- Semi-naive loop: lines 1516–2070 (major consumer of snapshots)

---

## 8. Acceptance Criteria

✓ Function signature specified
✓ Pseudocode covers 3 phases + edge cases
✓ Algorithm complexity documented: O(D log D + N)
✓ Memory ownership clear
✓ Integration points identified (remove old_data, remove merge walk)
✓ 5 test cases designed (ready for US-003)
✓ Edge cases covered

**Status:** READY FOR TDD IMPLEMENTATION (US-003 → US-004)
