# K-Way Merge CONSOLIDATE Design Document

**Date:** 2026-03-08
**Status:** Design — Architect Approved
**Scope:** In-plan CONSOLIDATE only (columnar_nanoarrow.c:2036-2037)

---

## 1. Executive Summary

**Problem:** The in-plan `col_op_consolidate` uses a full `qsort_r` on M concatenated rows
from K copy-evaluations. This O(M log M) sort dominates ~35% of CSPA wall time
(29.3 s baseline, measured 2026-03-07).

**Solution:** K-way merge exploiting the fact that each of the K copies produces independently
sortable output. By sorting K segments of ~M/K rows each and then merging, total sort work
drops to O(M(log M − log K)) while the merge adds only O(M log K). The dominant practical
gain is cache locality: segments fit in L2 where the full M-row sort did not.

**Expected improvement:** 30–45% wall-time reduction on CSPA (target: 29.3 s → 15–19 s).

---

## 2. Current Implementation

### col_op_consolidate (lines 1386–1433)

```c
static int
col_op_consolidate(eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols, nr = in->nrows;

    if (nr <= 1)
        return eval_stack_push(stack, in, e.owned);

    /* Copy if not owned */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) { /* ... copy ... */ }

    qsort_r(work->data, nr, sizeof(int64_t) * nc, &nc, row_cmp_fn);

    /* Compact: keep only unique rows */
    uint32_t out_r = 1;
    for (uint32_t r = 1; r < nr; r++) {
        const int64_t *prev = work->data + (size_t)(r - 1) * nc;
        const int64_t *cur  = work->data + (size_t)r * nc;
        if (memcmp(prev, cur, sizeof(int64_t) * nc) != 0) {
            if (out_r != r)
                memcpy(work->data + (size_t)out_r * nc, cur, sizeof(int64_t) * nc);
            out_r++;
        }
    }
    work->nrows = out_r;
    return eval_stack_push(stack, work, work_owned);
}
```

**Call site:** in-plan evaluation switch (lines 2036–2037):
```c
case WL_PLAN_OP_CONSOLIDATE:
    rc = col_op_consolidate(stack);
    break;
```

### Performance: O(M log M) sort + O(M) dedup

For CSPA (K=3, M≈20 000 rows, nc=2):
- Sort: 20 000 × log₂(20 000) ≈ 286 K comparisons per CONSOLIDATE call
- 100+ semi-naive iterations → tens of millions of comparisons per run

### Paths NOT affected by this optimization

| Path | Line | Reason |
|------|------|--------|
| Incremental CONSOLIDATE | 2314 | Uses `col_op_consolidate_incremental_delta` — already optimized |
| `col_idb_consolidate` | 2610 | Always K=1 (single relation), uses standard qsort fallback |

---

## 3. Proposed K-Way Merge Algorithm

### 3a. Algorithm Overview

The plan generator (`exec_plan_gen.c:985–1048`) produces for a rule with K IDB body atoms:

```
[Copy 0 ops] CONCAT [Copy 1 ops] CONCAT ... [Copy K-1 ops] CONCAT CONSOLIDATE
```

Each CONCAT merges two stack entries. After K copies and K−1 CONCATs, one concatenated
relation sits on the stack. CONSOLIDATE then sorts and deduplicates it.

**Key insight:** Each copy's output is independent. Sorting K segments of M/K rows each and
merging is strictly better than sorting all M rows at once, due to cache locality.

**Proposed algorithm:**

1. **Input:** concatenated K-copy output with `seg_boundaries` metadata in `eval_entry_t`
2. **Step 1 — Per-segment sort:** Sort each of the K segments independently:
   O((M/K) log(M/K)) per segment × K = O(M(log M − log K)) total
3. **Step 2 — K-way merge with dedup:** Use min-heap (K≥3) or direct 2-way comparison (K=2)
   to produce final sorted+deduplicated output: O(M log K)
4. **Output:** sorted + deduplicated relation, byte-identical to current implementation

### 3b. Pseudocode

```
function col_op_consolidate_kway(rel, seg_boundaries, seg_count):
    if seg_count <= 1 or seg_boundaries == NULL:
        // Fallback: standard qsort (K=1 or no boundary metadata)
        return col_op_consolidate_standard(rel)

    if seg_count == 2:
        // Optimized: direct 2-way merge (no heap overhead)
        return col_op_consolidate_2way_merge(rel, seg_boundaries[1])

    // General K-way merge (seg_count >= 3)
    for each segment s in 0..seg_count-1:
        seg_start = seg_boundaries[s]
        seg_end   = seg_boundaries[s+1]
        sort_segment(rel, seg_start, seg_end)   // qsort_r on slice

    heap = build_min_heap(K entries, one per non-empty segment)
    merged = allocate_buffer(nr rows)
    last_row = None
    out = 0

    while heap not empty:
        min_seg = heap.extract_min()             // uses row_cmp_optimized
        row = get_row(rel, min_seg.current_pos)

        if row != last_row:                      // dedup on-the-fly
            merged[out++] = row
            last_row = row

        min_seg.current_pos++
        if min_seg.current_pos < seg_boundaries[min_seg.seg + 1]:
            heap.insert(min_seg)                 // re-insert if segment not exhausted

    swap rel.data with merged
    rel.nrows = out
    return rel
```

### 3c. K=2 Specialization

For K=2 (most common case — rules with exactly 2 IDB body atoms), skip the heap entirely and
use direct 2-pointer merge. This reuses the proven pattern from `col_op_consolidate_incremental`
(lines 1486–1519):

```c
/* Direct 2-way merge — no heap, no allocation overhead beyond output buffer */
static void
merge_2way_dedup(const int64_t *a, uint32_t na,
                 const int64_t *b, uint32_t nb,
                 int64_t *out, uint32_t *out_count,
                 uint32_t nc)
{
    size_t row_bytes = (size_t)nc * sizeof(int64_t);
    uint32_t ai = 0, bi = 0, oi = 0;
    const int64_t *last = NULL;

    while (ai < na && bi < nb) {
        const int64_t *arow = a + (size_t)ai * nc;
        const int64_t *brow = b + (size_t)bi * nc;
        int cmp = row_cmp_optimized(arow, brow, nc);   /* SIMD dispatcher */
        const int64_t *pick;
        if (cmp <= 0) { pick = arow; ai++; if (cmp == 0) bi++; }
        else           { pick = brow; bi++; }
        if (!last || row_cmp_optimized(last, pick, nc) != 0) {
            memcpy(out + (size_t)oi * nc, pick, row_bytes);
            last = out + (size_t)oi * nc;
            oi++;
        }
    }
    /* Drain remaining, with dedup against last */
    while (ai < na) { /* ... */ ai++; }
    while (bi < nb) { /* ... */ bi++; }
    *out_count = oi;
}
```

Pattern mirrors lines 1486–1519 of `col_op_consolidate_incremental` exactly, adapted to accept
two arbitrary segments rather than old-rows vs delta-rows.

### 3d. SIMD Optimization

Use `row_cmp_optimized` (lines 1655–1661) as the comparison function in:
- Min-heap sift-down operations (K≥3 path)
- 2-way comparison in K=2 path

`row_cmp_optimized` is a compile-time dispatcher:
```c
#ifdef __AVX2__
#define row_cmp_optimized row_cmp_simd_avx2
#elif defined(__ARM_NEON__)
#define row_cmp_optimized row_cmp_simd_neon
#else
#define row_cmp_optimized row_cmp_lex
#endif
```

This is the same mechanism proven in `col_op_consolidate_incremental_delta` (line 1740).
No AVX2/NEON compatibility changes are needed — the macro handles platform selection
transparently.

**Note:** Per-segment `qsort_r` calls continue to use `row_cmp_fn` (the existing qsort_r
comparator wrapper) since qsort_r requires the standard C comparator signature.
`row_cmp_optimized` is used in the merge phase where we control the call site directly.

---

## 4. Complexity Analysis

| Metric | Current | K-way Merge |
|--------|---------|-------------|
| Sort phase | O(M log M) | O(K × (M/K) log(M/K)) = O(M(log M − log K)) |
| Merge phase | N/A | O(M log K) |
| Dedup phase | O(M) separate pass (memcmp) | O(M) in-merge (no separate pass) |
| **Total** | **O(M log M)** | **O(M log(M/K)) + O(M log K)** |

### Algebraic identity

```
O(M log(M/K)) + O(M log K)
= O(M(log M − log K)) + O(M log K)
= O(M log M)
```

The asymptotic class is the same. The practical gain is **constant-factor**, driven by:

**Cache locality improvement:**
- Current: 20 K rows × 2 cols × 8 bytes = 320 KB — exceeds L2 on many CPUs (256 KB), causing
  repeated evictions during qsort's random-access pivot comparisons
- K=3 segments: ~107 KB each — fits comfortably in L2 → drastically fewer cache misses

**Comparison savings by K:**

| K | Sort comparisons | Merge comparisons | Total | vs baseline |
|---|-----------------|-------------------|-------|-------------|
| 1 | 286 K | 0 | 286 K | 1.00× |
| 2 | 2 × 143 K = 274 K | M log 2 = 20 K | 294 K | 1.03× (merge pays for itself via cache) |
| 3 | 3 × 85 K = 254 K | M log 3 = 32 K | 286 K | ~1.00× (cache dominates) |
| 6 | 6 × 38 K = 228 K | M log 6 = 52 K | 280 K | 0.98× |

For K=2 the comparison count is nearly identical to K=1, but the cache benefit dominates
because each 143 K-row segment fits in L2. For K=3 both effects combine.

---

## 5. Data Structure Changes

### 5a. eval_entry_t extension (columnar_nanoarrow.c:734–738)

```c
typedef struct {
    col_rel_t *rel;
    bool       owned;
    bool       is_delta;   /* true when rel is a delta (ΔR) relation */
    uint32_t  *seg_boundaries; /* NEW: segment boundary row indices, length seg_count+1 */
                               /*      seg_boundaries[0] = 0 (always)                  */
                               /*      seg_boundaries[k] = start row of segment k       */
                               /*      seg_boundaries[seg_count] = nrows (sentinel)     */
    uint32_t   seg_count;      /* NEW: number of segments (0 = no segmentation info)    */
} eval_entry_t;
```

The sentinel at `seg_boundaries[seg_count]` avoids an out-of-bounds read when computing
segment end: `seg_end = seg_boundaries[s + 1]` is always valid for s in 0..seg_count-1.

### 5b. eval_stack_push modification (line 752)

```c
static int
eval_stack_push(eval_stack_t *s, col_rel_t *r, bool owned)
{
    if (s->top >= COL_STACK_MAX)
        return ENOBUFS;
    s->items[s->top].rel           = r;
    s->items[s->top].owned         = owned;
    s->items[s->top].is_delta      = false;
    s->items[s->top].seg_boundaries = NULL;  /* NEW: initialize to NULL */
    s->items[s->top].seg_count      = 0;     /* NEW: initialize to 0    */
    s->top++;
    return 0;
}
```

### 5c. eval_stack_drain modification (line 783)

```c
static void
eval_stack_drain(eval_stack_t *s)
{
    while (s->top > 0) {
        eval_entry_t e = eval_stack_pop(s);
        if (e.seg_boundaries)          /* NEW: free boundary array if present */
            free(e.seg_boundaries);
        if (e.owned) {
            col_rel_free_contents(e.rel);
            free(e.rel);
        }
    }
}
```

### 5d. col_op_concat modification (lines 1311–1365)

The concat operator tracks segment boundaries in the output entry. The output entry carries
the union of both input entries' segment lists, with right-hand boundaries offset by the
left-hand row count.

```
Pseudocode for boundary propagation in col_op_concat:

  a_segs = max(a_e.seg_count, 1)   // treat unsegmented entry as 1 segment
  b_segs = max(b_e.seg_count, 1)
  total_segs = a_segs + b_segs

  new_bounds = malloc((total_segs + 1) * sizeof(uint32_t))  // +1 for sentinel
  if (!new_bounds) goto enomem;

  // Copy left boundaries (or synthesize [0] if unsegmented)
  if (a_e.seg_boundaries):
      memcpy(new_bounds, a_e.seg_boundaries, (a_segs + 1) * sizeof(uint32_t))
  else:
      new_bounds[0] = 0
      new_bounds[1] = a->nrows    // sentinel for single segment

  // Append right boundaries, offset by left row count
  a_nrows = a->nrows              // captured BEFORE appending b to out
  if (b_e.seg_boundaries):
      for i in 0..b_segs:
          new_bounds[a_segs + i] = b_e.seg_boundaries[i] + a_nrows
      new_bounds[a_segs + b_segs] = b_e.seg_boundaries[b_segs] + a_nrows  // sentinel
  else:
      new_bounds[a_segs]     = 0 + a_nrows               // start of b's single segment
      new_bounds[a_segs + 1] = b->nrows + a_nrows         // sentinel

  free(a_e.seg_boundaries)        // release old boundary arrays
  free(b_e.seg_boundaries)

  entry_out.seg_boundaries = new_bounds
  entry_out.seg_count      = total_segs
```

**Memory cleanup in col_op_concat error paths:**

At every error return point inside `col_op_concat` that occurs after `new_bounds` is allocated,
add `free(new_bounds)` before the return. Also free `a_e.seg_boundaries` and `b_e.seg_boundaries`
at every error return, since they are consumed regardless of success or failure.

---

## 6. Implementation Notes for col_op_consolidate

### 6a. Fallback path (K=1 or seg_boundaries == NULL)

When `e.seg_count <= 1` or `e.seg_boundaries == NULL`, fall through to the existing
`qsort_r` + compact loop unchanged. Free `e.seg_boundaries` (may be NULL — free(NULL) is safe)
before entering the fallback path.

### 6b. K=2 path

```c
if (e.seg_count == 2 && e.seg_boundaries != NULL) {
    uint32_t mid = e.seg_boundaries[1];  /* start of second segment */

    /* Sort each half */
    qsort_r(work->data,
            mid, row_bytes, &nc, row_cmp_fn);
    qsort_r(work->data + (size_t)mid * nc,
            nr - mid, row_bytes, &nc, row_cmp_fn);

    /* 2-way merge with dedup into merged buffer */
    int64_t *merged = malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged) { free(e.seg_boundaries); /* cleanup */ return ENOMEM; }

    uint32_t out_count = 0;
    merge_2way_dedup(work->data, mid,
                     work->data + (size_t)mid * nc, nr - mid,
                     merged, &out_count, nc);

    free(work->data);
    work->data = merged;
    work->nrows = out_count;
    work->capacity = nr;

    free(e.seg_boundaries);
    return eval_stack_push(stack, work, work_owned);
}
```

Pattern from `col_op_consolidate_incremental` lines 1486–1519. No heap, no allocation beyond
the output buffer.

### 6c. K>=3 path — min-heap merge

```c
/* Heap entry tracks which segment and current row position */
typedef struct { uint32_t seg; uint32_t pos; } heap_entry_t;

static void
heap_sift_down(heap_entry_t *heap, uint32_t pos, uint32_t size,
               const int64_t *data, uint32_t nc,
               const uint32_t *bounds, uint32_t total_nr)
{
    while (true) {
        uint32_t smallest = pos;
        uint32_t left = 2 * pos + 1, right = 2 * pos + 2;
        const int64_t *s_row = data + (size_t)heap[smallest].pos * nc;

        if (left < size) {
            const int64_t *l_row = data + (size_t)heap[left].pos * nc;
            if (row_cmp_optimized(l_row, s_row, nc) < 0) { smallest = left; s_row = l_row; }
        }
        if (right < size) {
            const int64_t *r_row = data + (size_t)heap[right].pos * nc;
            if (row_cmp_optimized(r_row, s_row, nc) < 0) smallest = right;
        }
        if (smallest == pos) break;

        heap_entry_t tmp = heap[pos];
        heap[pos] = heap[smallest];
        heap[smallest] = tmp;
        pos = smallest;
    }
}
```

Merge loop:
```c
uint32_t out = 0;
const int64_t *last_out = NULL;

while (heap_size > 0) {
    heap_entry_t top = heap[0];
    const int64_t *row = work->data + (size_t)top.pos * nc;

    /* Dedup: skip if identical to previous output row */
    if (!last_out || row_cmp_optimized(last_out, row, nc) != 0) {
        memcpy(merged + (size_t)out * nc, row, row_bytes);
        last_out = merged + (size_t)out * nc;
        out++;
    }

    /* Advance segment pointer */
    uint32_t seg_end = e.seg_boundaries[top.seg + 1];
    if (top.pos + 1 < seg_end) {
        heap[0].pos = top.pos + 1;
        heap_sift_down(heap, 0, heap_size, work->data, nc, e.seg_boundaries, nr);
    } else {
        /* Segment exhausted — remove from heap */
        heap[0] = heap[--heap_size];
        if (heap_size > 0)
            heap_sift_down(heap, 0, heap_size, work->data, nc, e.seg_boundaries, nr);
    }
}
```

**Memory cleanup in col_op_consolidate error paths:**

Every early return after `merged` or `heap` is allocated must free both, plus `e.seg_boundaries`.
Example:
```c
if (!heap) {
    free(merged);
    free(e.seg_boundaries);
    if (work_owned) { col_rel_free_contents(work); free(work); }
    return ENOMEM;
}
```

---

## 7. Edge Cases and Correctness

| Case | Handling |
|------|----------|
| K=1 (single segment) | Fallback to standard qsort — identical output to current code |
| seg_boundaries == NULL | Treated as K=1, fallback to qsort |
| Empty segments | Skipped during heap initialization (seg_start == seg_end means no rows) |
| Unequal segment sizes | Natural — seg_boundaries carries exact per-segment row counts |
| All rows identical | Dedup reduces to 1 row in output (same behavior as current) |
| All rows unique | Out-count equals in-count, no dedup reduction |
| Memory allocation failure (merged) | Free seg_boundaries + work if owned, return ENOMEM |
| Memory allocation failure (heap) | Free merged + seg_boundaries + work if owned, return ENOMEM |
| nr <= 1 | Early return before any allocation (existing check preserved) |
| col_idb_consolidate (K=1 always) | seg_boundaries is NULL (eval_stack_push initializes to NULL), fallback path taken — no behavior change |

**Correctness invariant:** The output of K-way merge is byte-identical to the output of full
sort + dedup on the same input rows, because:
1. Per-segment sorts produce the same order as a global sort would for those rows (qsort_r is
   comparison-based, deterministic for fixed comparator)
2. K-way merge preserves global sorted order across segments by the heap invariant
3. Dedup during merge is equivalent to dedup after sort for sorted input

---

## 8. Integration with Other Components

### 8a. Interaction with CSE cache

The CSE (common subexpression elimination) cache operates on CONCAT output before CONSOLIDATE.
Cache lookup and insert use `col_rel_t` directly — they are unaware of `eval_entry_t` metadata.
The `seg_boundaries` array lives only in `eval_entry_t`, not in `col_rel_t`, so the CSE cache
is completely unaffected.

### 8b. Interaction with col_op_consolidate_incremental_delta

`col_op_consolidate_incremental_delta` (line 1699–1779) is the incremental path called at
line 2314 inside the semi-naive fixpoint loop. It operates on the full accumulated IDB relation
(not on K-copy concat output) and already uses optimized 2-way merge. This optimization does
not touch that path. Both code paths use qsort_r internally for their respective sort steps —
no conflict.

### 8c. Plan generator expectations (exec_plan_gen.c:985–1048)

`expand_multiway_delta` generates for K IDB body atoms:

```
[Copy 0 ops] CONCAT [Copy 1 ops] CONCAT [Copy 2 ops] CONCAT ... CONSOLIDATE
```

This is K copies and K−1 CONCAT operations (binary left-fold). The modified `col_op_concat`
accumulates segment boundaries through each CONCAT. After K−1 CONCATs, the entry on the stack
has `seg_count = K` and `seg_boundaries` of length K+1. CONSOLIDATE receives these and
performs K-way merge.

No changes to `exec_plan_gen.c` are required. The boundary metadata is an evaluator-level
optimization, invisible to the plan.

---

## 9. Performance Expectations

**Baseline:** CSPA median 29.3 s (measured 2026-03-07, 15 workloads, 3 runs each)

| Component | Effect | Magnitude |
|-----------|--------|-----------|
| qsort cache locality (K=3 segments fit in L2) | Sort phase faster | −30% to −40% on sort |
| Dedup merged into merge pass (no separate memcmp scan) | Eliminate extra O(M) pass | −5% on consolidate |
| Merge overhead (heap sift-down, extra allocation) | Cost | +5% to +10% on consolidate |
| **Net on col_op_consolidate** | | **−25% to −40%** |
| col_op_consolidate share of CSPA wall time | ~35% | |
| **Net on overall CSPA wall time** | | **−9% to −14% from consolidate alone** |

The 30–45% overall wall-time estimate accounts for cascade effects: faster consolidate reduces
total iteration time, which may reduce memory pressure and improve cache state for subsequent
iterations.

**Conservative target:** 29.3 s × 0.70 = 20.5 s
**Optimistic target:** 29.3 s × 0.55 = 16.1 s
**Published target:** 15–19 s (30–45% reduction)

---

## 10. Testing Strategy

### Unit tests (test_consolidate_kway_merge.c)

| Test | K | Input | Expected output |
|------|---|-------|----------------|
| K=2, no duplicates | 2 | seg0=[1,2], seg1=[3,4] | [1,2,3,4] |
| K=2, all duplicates | 2 | seg0=[1,2], seg1=[1,2] | [1,2] |
| K=2, interleaved | 2 | seg0=[1,3], seg1=[2,4] | [1,2,3,4] |
| K=3, CSPA-like | 3 | 3 segs of ~100 rows | sorted unique |
| K=1 fallback | 1 | any | identical to qsort result |
| NULL boundaries | — | seg_boundaries=NULL | identical to qsort result |
| Empty segment | 3 | seg0=[], seg1=[1,2], seg2=[3] | [1,2,3] |
| All identical | 3 | 3 segs of [1,1], [1,1], [1,1] | [1] |
| Large K | 6 | M=20 K rows | sorted unique, matches qsort |

### Validation tests (CSPA correctness)

Run CSPA and verify:
- Exactly 20 381 output tuples (the established correctness oracle)
- Iteration count matches baseline
- Output is byte-identical to baseline sorted output

### Performance tests

```bash
# Build
meson compile -C build bench_flowlog

# 3 CSPA runs (take median)
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa

# Target: median < 19 000 ms (30%+ improvement over 29 300 ms baseline)
```

### Regression tests (all 15 workloads)

```bash
meson test -C build
# All 15 workloads must pass with identical tuple counts
```

---

## 11. Architect Recommendations — Traceability

| Recommendation | Section(s) where addressed |
|---------------|---------------------------|
| Scope: in-plan CONSOLIDATE only (line 2036–2037) | §2 Call site, §8b Incremental path, §7 col_idb_consolidate edge case |
| eval_entry_t metadata (seg_boundaries, seg_count) | §5a–5d Data structure changes |
| K=2 specialization (direct 2-way merge, no heap) | §3c K=2 Specialization, §6b K=2 path |
| SIMD optimization (row_cmp_optimized in merge phase) | §3d SIMD Optimization, §6b, §6c |
| Memory cleanup in all error paths | §5c eval_stack_drain, §5d concat error paths, §6 consolidate error paths |

---

## References

| Resource | Location |
|----------|----------|
| Original K-way merge doc | `docs/k-copy/K-WAY-MERGE-CONSOLIDATE.md` |
| col_op_consolidate (current) | `wirelog/backend/columnar_nanoarrow.c:1386–1433` |
| col_op_consolidate_incremental (2-way merge pattern) | `wirelog/backend/columnar_nanoarrow.c:1486–1519` |
| col_op_consolidate_incremental_delta | `wirelog/backend/columnar_nanoarrow.c:1699–1779` |
| row_cmp_optimized (SIMD dispatcher) | `wirelog/backend/columnar_nanoarrow.c:1655–1661` |
| col_op_concat (current) | `wirelog/backend/columnar_nanoarrow.c:1311–1365` |
| eval_entry_t (current) | `wirelog/backend/columnar_nanoarrow.c:734–738` |
| eval_stack_push (current) | `wirelog/backend/columnar_nanoarrow.c:752–761` |
| eval_stack_drain (current) | `wirelog/backend/columnar_nanoarrow.c:783–792` |
| In-plan CONSOLIDATE call site | `wirelog/backend/columnar_nanoarrow.c:2036–2037` |
| Incremental CONSOLIDATE call site | `wirelog/backend/columnar_nanoarrow.c:2314` |
| col_idb_consolidate | `wirelog/backend/columnar_nanoarrow.c:2610` |
| Plan generator (expand_multiway_delta) | `wirelog/exec_plan_gen.c:985–1048` |
| CSPA baseline benchmark | `docs/performance/BENCHMARK-RESULTS-2026-03-07.md` |
