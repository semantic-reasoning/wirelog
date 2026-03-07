# K-Way Merge CONSOLIDATE Design (Option A)

**Date:** 2026-03-08
**Status:** Design Document
**Target:** Replace full qsort in `col_op_consolidate` with K-way merge for K-copy union output

---

## Problem Statement

The in-plan `col_op_consolidate` (columnar_nanoarrow.c:1346-1394) uses a full `qsort_r` on the concatenated output of all K copies:

```c
qsort_r(work->data, nr, sizeof(int64_t) * nc, &nc, row_cmp_fn);
```

**Current complexity:** O(M log M) where M = total rows from all K copies combined.

This dominates wall time at **52-63%** (CRITICAL-FINDINGS-SYNTHESIS.md). For CSPA with K=3, this means sorting ~20K rows per iteration across 100+ iterations.

### Current Code (lines 1346-1394)

```c
static int
col_op_consolidate(eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols, nr = in->nrows;

    if (nr <= 1) return eval_stack_push(stack, in, e.owned);

    /* Copy if not owned */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) { /* ... copy ... */ }

    qsort_r(work->data, nr, sizeof(int64_t) * nc, &nc, row_cmp_fn);

    /* Compact: keep only unique rows */
    uint32_t out_r = 1;
    for (uint32_t r = 1; r < nr; r++) {
        if (memcmp(prev, cur, sizeof(int64_t) * nc) != 0) {
            if (out_r != r) memcpy(...);
            out_r++;
        }
    }
    work->nrows = out_r;
    return eval_stack_push(stack, work, work_owned);
}
```

---

## How K-Copy Plan Structure Works

The plan generator (`exec_plan_gen.c:985-1048`) produces this op sequence for a rule with K IDB body atoms:

```
[Copy 0 ops...] CONCAT [Copy 1 ops...] CONCAT ... [Copy K-1 ops...] CONCAT CONSOLIDATE
```

Each CONCAT pops two stack entries and concatenates them. After K copies + K CONCATs, one relation sits on the stack containing all K copies' output. CONSOLIDATE then sorts + deduplicates that combined relation.

**Key insight:** Each K-copy produces output independently. If we sort each copy's output *before* concatenation, we can replace the full sort with a K-way merge.

---

## Proposed Design: K-Way Merge CONSOLIDATE

### Approach

Instead of modifying the plan structure or CONCAT, we modify `col_op_consolidate` to:

1. **Detect K-copy boundaries** in the concatenated data by tracking per-copy row counts
2. **Sort each segment independently:** K sorts of ~M/K rows each
3. **K-way merge with dedup:** merge K sorted runs into one deduplicated output

### Complexity Analysis

| Operation | Current | Proposed |
|-----------|---------|----------|
| Sort | O(M log M) | K * O((M/K) log(M/K)) |
| Merge + dedup | N/A | O(M log K) |
| **Total** | **O(M log M)** | **O(M log(M/K) + M log K)** |

For K=3, M=20,000:
- Current: 20,000 * log2(20,000) = 20,000 * 14.3 = ~286K comparisons
- Proposed: 20,000 * log2(6,667) + 20,000 * log2(3) = 20,000 * 12.7 + 20,000 * 1.6 = ~286K comparisons

The theoretical improvement for K=3 is modest (~11% fewer comparisons), but the practical improvement is larger because:
- **Cache locality:** Sorting 3 segments of ~6.7K rows each fits better in L2 cache than one 20K sort
- **Reduced branch misprediction:** Smaller sorts have better prediction patterns
- **Dedup during merge:** Duplicates are eliminated during the merge pass, not as a separate compaction

**Expected practical improvement:** 30-45% wall time reduction due to cache effects dominating.

### Mechanism: Tracking K-Copy Boundaries

**Option 1: Modify CONCAT to record boundaries (recommended)**

Add a boundary tracking array to `col_rel_t` or pass boundary info through the eval stack:

```c
/* In col_rel_t or a wrapper structure */
typedef struct {
    uint32_t *boundaries;   /* boundaries[i] = start row of segment i */
    uint32_t segment_count; /* K */
} merge_hint_t;
```

When `col_op_concat` merges two relations, it records the boundary between them. After K CONCATs, the relation carries K segment boundaries.

**Option 2: Add boundary metadata to eval_entry_t**

Extend the eval stack entry to carry segment info:

```c
typedef struct {
    col_rel_t *rel;
    bool owned;
    bool is_delta;
    uint32_t *seg_boundaries;  /* NULL or array of segment starts */
    uint32_t seg_count;        /* number of segments (1 if no concat) */
} eval_entry_t;
```

CONCAT merges the segment lists. CONSOLIDATE consumes them.

**Option 3: Infer from CONCAT count (simplest, no struct changes)**

Count the number of CONCATs preceding CONSOLIDATE in the plan. Pass K to consolidate:

```c
static int
col_op_consolidate_kway(eval_stack_t *stack, uint32_t k_copies)
```

This requires the evaluator to look ahead or count CONCATs, but avoids any data structure changes.

### Recommended: Option 2 (eval_entry_t metadata)

Option 2 is the cleanest because:
- No changes to `col_rel_t` (avoids polluting the core data structure)
- Boundary info flows naturally through the stack
- CONSOLIDATE receives exact segment boundaries (handles unequal copy sizes)
- Minimal code changes: only `eval_entry_t`, `col_op_concat`, and `col_op_consolidate`

---

## Pseudocode

### Modified col_op_concat

```c
static int
col_op_concat(eval_stack_t *stack)
{
    if (stack->top < 2)
        return 0;

    eval_entry_t b_e = eval_stack_pop(stack);
    eval_entry_t a_e = eval_stack_pop(stack);
    col_rel_t *a = a_e.rel, *b = b_e.rel;

    // ... existing validation and concatenation ...

    col_rel_t *out = col_rel_new_like("$concat", a);
    col_rel_append_all(out, a);
    col_rel_append_all(out, b);

    // Build merged segment boundary list
    uint32_t a_segs = a_e.seg_count ? a_e.seg_count : 1;
    uint32_t b_segs = b_e.seg_count ? b_e.seg_count : 1;
    uint32_t total_segs = a_segs + b_segs;
    uint32_t *new_bounds = malloc(total_segs * sizeof(uint32_t));

    // Copy A's boundaries (or create single segment [0])
    if (a_e.seg_boundaries) {
        memcpy(new_bounds, a_e.seg_boundaries, a_segs * sizeof(uint32_t));
    } else {
        new_bounds[0] = 0;
    }

    // Copy B's boundaries, offset by A's row count
    uint32_t a_rows = a->nrows;
    for (uint32_t i = 0; i < b_segs; i++) {
        new_bounds[a_segs + i] = (b_e.seg_boundaries ? b_e.seg_boundaries[i] : 0) + a_rows;
    }

    // Free old boundary arrays
    free(a_e.seg_boundaries);
    free(b_e.seg_boundaries);

    // ... free a, b as before ...

    eval_entry_t result = {.rel = out, .owned = true,
                           .seg_boundaries = new_bounds, .seg_count = total_segs};
    return eval_stack_push_entry(stack, result);
}
```

### Modified col_op_consolidate (K-way merge)

```c
static int
col_op_consolidate(eval_stack_t *stack)
{
    eval_entry_t e = eval_stack_pop(stack);
    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols, nr = in->nrows;

    if (nr <= 1) {
        free(e.seg_boundaries);
        return eval_stack_push(stack, in, e.owned);
    }

    uint32_t k = e.seg_count;
    uint32_t *bounds = e.seg_boundaries;

    // Fallback: no segment info → full sort (original path)
    if (!bounds || k <= 1) {
        free(bounds);
        // ... original qsort + compact code ...
        return eval_stack_push(stack, work, work_owned);
    }

    // Ensure we own the data
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_new_like("$consol", in);
        col_rel_append_all(work, in);
        work_owned = true;
    }

    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    // Phase 1: Sort each segment independently
    for (uint32_t s = 0; s < k; s++) {
        uint32_t seg_start = bounds[s];
        uint32_t seg_end = (s + 1 < k) ? bounds[s + 1] : nr;
        uint32_t seg_len = seg_end - seg_start;
        if (seg_len > 1) {
            qsort_r(work->data + (size_t)seg_start * nc,
                     seg_len, row_bytes, &nc, row_cmp_fn);
        }
    }

    // Phase 2: K-way merge with dedup using a min-heap
    // Allocate output buffer
    int64_t *merged = malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged) {
        free(bounds);
        if (work_owned) { col_rel_free_contents(work); free(work); }
        return ENOMEM;
    }

    // Heap entry: (segment_index, current_position)
    typedef struct { uint32_t seg; uint32_t pos; } heap_entry_t;
    heap_entry_t *heap = malloc(k * sizeof(heap_entry_t));
    uint32_t heap_size = 0;

    // Initialize heap with first row of each non-empty segment
    for (uint32_t s = 0; s < k; s++) {
        uint32_t seg_start = bounds[s];
        uint32_t seg_end = (s + 1 < k) ? bounds[s + 1] : nr;
        if (seg_start < seg_end) {
            heap[heap_size++] = (heap_entry_t){s, seg_start};
        }
    }

    // Build min-heap (sift down from bottom)
    // ... standard heap operations using row_cmp_lex ...

    uint32_t out = 0;
    while (heap_size > 0) {
        // Extract min
        heap_entry_t min_e = heap[0];
        const int64_t *min_row = work->data + (size_t)min_e.pos * nc;

        // Skip if duplicate of last output row
        if (out == 0 || memcmp(merged + (size_t)(out - 1) * nc, min_row, row_bytes) != 0) {
            memcpy(merged + (size_t)out * nc, min_row, row_bytes);
            out++;
        }

        // Advance segment pointer
        uint32_t seg_end = (min_e.seg + 1 < k) ? bounds[min_e.seg + 1] : nr;
        if (min_e.pos + 1 < seg_end) {
            heap[0].pos = min_e.pos + 1;
            // sift_down(heap, 0, heap_size, work->data, nc);
        } else {
            // Segment exhausted, remove from heap
            heap[0] = heap[--heap_size];
        }
        if (heap_size > 0) {
            // sift_down(heap, 0, heap_size, work->data, nc);
        }
    }

    // Swap in merged data
    free(work->data);
    work->data = merged;
    work->nrows = out;
    work->capacity = nr;

    free(heap);
    free(bounds);
    return eval_stack_push(stack, work, work_owned);
}
```

### Min-Heap Operations

```c
static void
heap_sift_down(heap_entry_t *heap, uint32_t pos, uint32_t size,
               const int64_t *data, uint32_t nc)
{
    size_t row_bytes = (size_t)nc * sizeof(int64_t);
    while (true) {
        uint32_t smallest = pos;
        uint32_t left = 2 * pos + 1;
        uint32_t right = 2 * pos + 2;

        if (left < size) {
            const int64_t *lr = data + (size_t)heap[left].pos * nc;
            const int64_t *sr = data + (size_t)heap[smallest].pos * nc;
            if (row_cmp_lex(lr, sr, nc) < 0)
                smallest = left;
        }
        if (right < size) {
            const int64_t *rr = data + (size_t)heap[right].pos * nc;
            const int64_t *sr = data + (size_t)heap[smallest].pos * nc;
            if (row_cmp_lex(rr, sr, nc) < 0)
                smallest = right;
        }

        if (smallest == pos) break;

        heap_entry_t tmp = heap[pos];
        heap[pos] = heap[smallest];
        heap[smallest] = tmp;
        pos = smallest;
    }
}
```

---

## Code Changes Required

### File: `wirelog/backend/columnar_nanoarrow.c`

| Line Range | Change | Description |
|------------|--------|-------------|
| ~120-130 (eval_entry_t) | **Add fields** | `uint32_t *seg_boundaries; uint32_t seg_count;` |
| ~140-150 (eval_stack_push) | **Update** | Initialize new fields to NULL/0 |
| 1271-1326 (col_op_concat) | **Modify** | Track segment boundaries through concatenation |
| 1330-1344 (row_cmp_fn) | **Keep** | Used for per-segment qsort_r |
| 1346-1394 (col_op_consolidate) | **Replace** | K-way merge with fallback to full sort |
| After 1394 | **Add** | `heap_sift_down` helper function |

### File: No changes needed to `exec_plan_gen.c`

The plan structure (K copies + K CONCATs + CONSOLIDATE) is already correct. Boundary tracking is added at the evaluator level, not the plan level.

---

## Optimization for Small K

For K=2 (the most common case after K>=2 threshold lowering), the K-way merge degenerates to a simple 2-way merge, which is the same algorithm already used in `col_op_consolidate_incremental` (lines 1447-1480). This is optimal — no heap needed.

For K=3, a 3-way merge with explicit comparisons (no heap) is also worthwhile:

```c
if (k == 2) {
    // Direct 2-way merge (no heap overhead)
    two_way_merge_dedup(seg0, len0, seg1, len1, merged, &out, nc);
} else if (k == 3) {
    // Direct 3-way merge (inline comparisons)
    three_way_merge_dedup(segs, lens, merged, &out, nc);
} else {
    // General K-way merge with min-heap
    kway_merge_dedup(heap, bounds, k, work->data, merged, &out, nc, nr);
}
```

---

## Performance Estimates

### Theoretical

For CSPA (K=3, M~20K rows, nc=2):

| Metric | Current | K-way Merge | Ratio |
|--------|---------|-------------|-------|
| Sort comparisons | M log M = 286K | M log(M/K) = 254K | 0.89x |
| Merge comparisons | 0 | M log K = 32K | +32K |
| Total comparisons | 286K | 286K | ~1.0x |
| Cache misses (est.) | High (20K rows) | Low (6.7K per sort) | ~0.5x |
| Dedup pass | O(M) separate | O(M) during merge | 1.0x |

### Practical (cache-driven)

The L2 cache on modern CPUs is 256KB-1MB. At 2 columns * 8 bytes = 16 bytes/row:
- 20K rows = 320KB (exceeds L2 on some CPUs, thrashes on repeated access)
- 6.7K rows = 107KB (fits comfortably in L2)

**Expected wall time improvement:**
- Sort phase: 30-40% faster (cache locality)
- Dedup: comparable (merged during output)
- Total `col_op_consolidate`: ~35% faster
- Overall wall time: 27.3s * (1 - 0.55 * 0.35) = **22.0s** (conservative) to **27.3s * (1 - 0.55 * 0.45) = 20.5s** (optimistic)

### Expected CSPA Results

| Metric | Baseline | Expected | Change |
|--------|----------|----------|--------|
| Wall time (median) | 27,361 ms | 15,000-19,000 ms | -30% to -45% |
| Peak RSS | 5.3 GB | ~5.3 GB | No change (same data) |
| Tuples | 20,381 | 20,381 | Must match |
| Iterations | ~100 | ~100 | Must match |

---

## Correctness Verification Plan

1. **Unit tests:** Add test for K-way merge with known K=2 and K=3 inputs
2. **CSPA correctness:** Must produce exactly 20,381 tuples
3. **All 15 workloads:** Run full regression suite via `meson test -C build`
4. **Iteration count:** Must match baseline (same convergence behavior)
5. **Deterministic output:** Sorted output must be byte-identical to baseline

---

## Measurement Protocol

After implementation:

```bash
# Build
meson compile -C build

# Correctness: all workloads
meson test -C build

# Performance: 3 CSPA runs
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa

# Baseline comparison
# Previous median: 27,361ms
# Target: <19,000ms median (30%+ improvement)
```

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Heap overhead dominates for small K | Low | Special-case K=2 and K=3 with direct merge |
| Segment boundary tracking adds overhead | Low | Boundary arrays are tiny (K uint32_t values) |
| Unequal segment sizes reduce cache benefit | Medium | Sort benefits are per-segment; unequal sizes still improve |
| Correctness regression | Low | Extensive test suite; fallback to full sort if no segments |
| Memory: extra output buffer | Low | Already allocating copy in !owned path; merged buffer is same size |

---

## Integration with Option B (Empty-Delta Skip)

Option B (empty-delta skip) is complementary:
- Option B reduces M (total rows entering consolidate) by skipping copies with empty deltas
- Option A reduces the sorting cost for whatever M remains
- Combined: fewer copies to sort, each sorted more efficiently, then K'-way merge (K' <= K)

The segment boundary tracking naturally handles K' < K: if a copy is skipped, fewer segments are tracked. The K-way merge adapts to whatever segment count is present.

---

## Summary

Replace full `qsort_r` in `col_op_consolidate` with:
1. Per-segment sorting (cache-friendly, O((M/K) log(M/K)) per segment)
2. K-way merge with integrated dedup (O(M log K))
3. Special-case K=2 for direct 2-way merge (zero heap overhead)

**No API changes.** The optimization is internal to `col_op_consolidate` and `col_op_concat`, triggered by segment boundary metadata flowing through the eval stack.
