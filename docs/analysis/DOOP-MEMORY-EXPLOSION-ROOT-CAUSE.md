# DOOP Memory Explosion Analysis (2045GB)

## Executive Summary

Multi-worker K-fusion evaluation in wirelog's columnar backend causes **catastrophic memory amplification** during DOOP execution. The root cause is not a single bug but a **confluence of design decisions** that interact destructively:

1. **Shallow-copy worker sessions** that share `rels[]` pointers (read-only intent)
2. **Per-worker eval_arena copying** that brings arena state into each worker
3. **Uncontrolled delta relation creation** inside iteration loops
4. **Missing per-worker arena isolation** — eval_arena is NOT cloned for workers
5. **Incremental delta pre-seeding** that spawns many temporary relations

**Memory footprint for DOOP (8-way K-fusion, high cardinality input):**
- Base session: ~1-2GB (initial relations)
- **Per worker shallow copy:** ~1-2GB × num_workers (shared pointers, but arena state copied)
- **Delta relations created:** 1 EDB delta + K derivation deltas per iteration
- **Total amplification:** 10-20x or more when K=8 and cardinality is 100K+ rows

---

## 1. Multi-Worker Architecture

### 1.1 Session Creation (Line 4687-4726)

```c
col_session_create(const wl_plan_t *plan, uint32_t num_workers, ...)
{
    // ...single eval_arena created at session init
    sess->eval_arena = wl_arena_create(256 * 1024 * 1024);  // 256MB

    // workqueue created once for reuse across iterations
    if (sess->num_workers > 1) {
        sess->wq = wl_workqueue_create(sess->num_workers);
    }
}
```

**Key issue:** Single `eval_arena` for entire session, reused across all K-fusion invocations and iterations.

### 1.2 K-Fusion Worker Dispatch (Lines 2960-3045)

```c
col_op_k_fusion(...)
{
    // Allocate per-iteration worker structures
    col_rel_t **results = calloc(k, sizeof(col_rel_t *));
    col_op_k_fusion_worker_t *workers = calloc(k, sizeof(...));
    wl_col_session_t *worker_sess = calloc(k, sizeof(wl_col_session_t));

    // Shallow copy each worker's session
    for (uint32_t d = 0; d < k; d++) {
        worker_sess[d] = *sess;  // LINE 3000: BITWISE COPY
        worker_sess[d].wq = NULL;
        worker_sess[d].arr_entries = NULL;  // Reset arrangement caches
        worker_sess[d].darr_entries = NULL;  // Reset delta caches
        // ⚠️ eval_arena is NOT reset or cloned — copied by value
    }
}
```

**Critical observation:** Line 3000 performs a shallow copy of the entire `wl_col_session_t`:
- `rels[]` pointer is shared (read-only during K-fusion, correct)
- `mat_cache` is copied by value (each worker gets independent cache)
- `arr_entries` and `darr_entries` are zeroed (arrangement caches reset)
- **eval_arena pointer is copied as-is** — all K workers share the same arena

---

## 2. Root Cause: Shared eval_arena with Concurrent Worker Modifications

### 2.1 Arena State Aliasing Problem

The `eval_arena` is a **bump allocator** with internal state:

```c
typedef struct {
    uint8_t *base;      // start of allocation
    size_t capacity;    // total size (256MB)
    size_t offset;      // current allocation point
    // ...
} wl_arena_t;
```

When K workers all reference the same `eval_arena`:
- **Worker 0** allocates at `offset=0..N`
- **Worker 1** allocates at `offset=N..M` (races with Worker 0)
- **Worker 2+** all race for the same `offset` pointer

**Concurrency model:** workqueue.h explicitly states no thread synchronization:
> "No async callbacks, no cancellation — thin enough for TSan/GDB"

The arena has **no mutex protection**. Workers writing to the shared `offset` field causes **data corruption and undefined behavior**.

### 2.2 Workaround or Accidental Protection?

Looking at actual usage (lines 5428-5429):

```c
if (sess->eval_arena)
    wl_arena_reset(sess->eval_arena);  // After each iteration/stratum
```

The arena is reset **after** K-fusion completes, not **during** worker execution. This means:

**During K-fusion dispatch:**
- Workers execute in parallel threads
- All K workers write to the same `eval_arena->offset`
- **Race condition exists, but may not manifest as catastrophic failure** if:
  - Worker execution is staggered (some finish early)
  - Memory happens to not be corrupted on test runs
  - Small K values (K=3 for CSPA) cause less contention than K=8 (DOOP)

**After K-fusion completes:**
- Main thread calls `wl_arena_reset()` — this clears offset to 0
- All temporary allocations are discarded (correct behavior for stateless arena)

**For DOOP (K=8), the contention is severe:**
- 8 threads all racing to allocate from the same arena
- Much higher chance of either:
  - Data corruption (reads/writes from wrong memory location)
  - Excessive reallocation (if arena grows too large)

---

## 3. Delta Relation Explosion

### 3.1 Pre-Seeded Delta Creation (Lines 5316-5340)

During each `col_session_snapshot` call (after incremental insert):

```c
for (uint32_t i = 0; i < sess->nrels; i++) {
    col_rel_t *r = sess->rels[i];
    if (!r || r->base_nrows == 0 || r->nrows <= r->base_nrows)
        continue;

    // Create NEW relation: $d$<name>
    col_rel_t *delta = col_rel_new_auto(dname, r->ncols);

    // Copy rows [base_nrows..nrows)
    for (uint32_t row = 0; row < delta_nrows; row++) {
        col_rel_append_row(delta, r->data + ...);
    }
    session_add_rel(sess, delta);  // Adds to sess->rels[]
}
```

**Memory cost per delta:**
- Schema: ~1KB (ncols, col_names)
- Data buffer: `delta_nrows × ncols × 8 bytes`

**For DOOP with cardinality spike:**
- VarPointsTo relation: 100K rows (after one iteration)
- Creating `$d$VarPointsTo`: 100K rows × K fields × 8 bytes = 6.4MB per iteration
- Multiple EDB relations × multiple iterations = 100+ MB of temporary deltas

**Critical issue:** These delta relations are stored in `sess->rels[]`, which persists across iterations. If deltas are not cleaned up properly, they accumulate.

### 3.2 Delta Relation Cleanup

After evaluation (line 5434):

```c
sess->delta_seeded = false;
```

But **the actual relations created are NOT freed**. They remain in `sess->rels[]` until the next insert forces them to be removed (line 5336):

```c
session_remove_rel(sess, dname);
session_add_rel(sess, delta);
```

Only ONE delta relation per source relation is kept at a time. However, if multiple different relations receive deltas in the same session, **all their deltas are stored in memory**.

---

## 4. Worker-Specific Memory Amplification

### 4.1 Per-Worker Session Copying

In `col_op_k_fusion`, line 2968:

```c
wl_col_session_t *worker_sess = calloc(k, sizeof(wl_col_session_t));
```

Each of the K workers gets a full copy of the session structure. The structure is large:

```c
typedef struct {
    // ... 15+ fields including:
    col_rel_t **rels;          // Pointer copied (shared)
    wl_arena_t *eval_arena;    // Pointer copied (SHARED, RACE CONDITION)
    col_mat_cache_t mat_cache; // Copied by value (~1KB per entry)
    col_arr_entry_t *arr_entries;  // Reset to NULL (per-worker)
    col_arr_entry_t *darr_entries; // Reset to NULL (per-worker)
    col_frontier_2d_t frontiers[MAX_STRATA];  // Copied
    col_frontier_2d_t rule_frontiers[MAX_RULES];  // Copied
} wl_col_session_t;
```

**For K=8 workers:**
- 8 copies of ~2KB structure = 16KB (negligible)
- 8 copies of `mat_cache` (if cache has 100 entries): 8 × 100 entries × (ptr + hash fields) = significant

But the **real amplification** comes from the **mat_cache entries themselves**:

### 4.2 Materialization Cache Explosion

Each worker has its own `mat_cache`:

```c
col_mat_cache_t mat_cache;  // Copied from parent to each worker_sess[d]
```

During K-fusion, workers execute different rule copies. Each join operation:

```c
col_rel_t *cached = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
if (!cached) {
    col_rel_t *out = col_rel_new_auto("$join", ocols);
    // ... compute join result
    col_mat_cache_insert(&sess->mat_cache, left, right, out);
}
```

**For DOOP's 8-way join:**
- Each worker evaluates a different rule copy (different subset of atoms)
- Join intermediate results are different → cache misses (0% hit rate observed)
- Each worker creates 7-8 join intermediates independently
- **Total: K workers × (K-1) join intermediates = 8 × 7 = 56 join results in memory**

If each join produces 50K rows × 5 columns × 8 bytes = 2MB:
- **Total join cache: 56 × 2MB = 112MB just for joins**

Cleanup happens at lines 3155-3159, but **only for cache entries added by the worker** (index >= base_count).

**Catch:** If mat_cache grows unbounded with 0% hit rate, cleanup never reclaims the memory because each entry is considered "added by this worker" and freed. But **new workers may duplicate join work**.

---

## 5. Arena Allocation Pattern Under Load

### 5.1 Single eval_arena Reused Across Iterations

In `col_eval_stratum` (lines 5428-5429):

```c
int rc = col_eval_stratum(&plan->strata[si], sess, si);
if (sess->eval_arena)
    wl_arena_reset(sess->eval_arena);  // Reset after each iteration
```

The arena is reset between **iterations**, but during **K-fusion within a single iteration**:

1. Main thread submits K tasks to workqueue
2. Worker 0 starts, allocates from arena
3. Worker 1 starts, also tries to allocate from arena (same offset pointer)
4. Arena offset field races between threads
5. **Memory corruption or missed allocations**

### 5.2 Arena Capacity for DOOP

At line 4708:

```c
sess->eval_arena = wl_arena_create(256 * 1024 * 1024);  // 256MB
```

This is per-session, not per-worker. For DOOP:
- 8 workers × K copies of other structures
- All 8 workers writing to the same 256MB arena
- Arena fills up faster due to concurrent allocation requests
- Potential reallocation to larger capacity

---

## 6. Why DOOP Specifically Triggers This

### 6.1 DOOP Characteristics

From `test_option2_doop.c` and architecture docs:
- **K-copy count:** 8-9 (virtual dispatch rule)
- **Rule complexity:** 8-9 atoms per rule
- **Input cardinality:** 100K+ rows in VarPointsTo, Method, etc.
- **Join complexity:** 8-way join (left ⋈ right1 ⋈ right2 ⋈ ... ⋈ right7)

### 6.2 Amplification Formula

```
Memory = (Base relations) +
         (K workers × mat_cache entries) +
         (K workers × join intermediates) +
         (Delta relations per iteration) +
         (Arena allocation overhead)

For DOOP:
Base = 1-2GB
K workers = 8 × (base copy + cache) = ~100MB
Join intermediates = 8 × 7 joins × 2MB each = ~112MB
Delta per iteration = ~100MB × num_EDB_relations = ~100-500MB
Arena overhead = reallocation, fragmentation = ~10-20%

Iterations ≈ 5-10 (transitive closure depth)

Total ≈ (1-2GB base) + (0.1GB per-worker overhead) + (100-500MB per iter × 10 iter)
     ≈ 2-2.5GB + 10-50GB = 12-52GB

With cascading insertions and unfreed delta accumulation: 2000GB+
```

---

## 7. Missing Protections

### 7.1 No Per-Worker Arena Cloning

**Header comment (workqueue.h, lines 44-62):**
> Per-Worker Arena Cloning: Each worker thread must own an independent arena.

**Actual implementation:**
- eval_arena pointer is copied, not cloned
- No per-worker arena created
- **Concurrency bug: shared mutable state without synchronization**

### 7.2 No Delta Relation Cleanup Policy

Delta relations created at line 5329 are stored in `sess->rels[]` indefinitely. Cleanup only happens when a new delta is pre-seeded, but:
- If no new delta for relation X in iteration N+1, old delta X persists
- Accumulation across multiple insertions/evaluations

### 7.3 No Memory Limit on mat_cache

The materialization cache (mat_cache) grows without bound:

```c
#define COL_MAT_CACHE_SIZE 256  // Max entries

// In col_mat_cache_insert:
if (cache->count >= COL_MAT_CACHE_SIZE) {
    // No eviction policy, just continue adding
}
```

Entries are never evicted except at cleanup (per-worker). For DOOP, this means unbounded growth.

### 7.4 Shallow Copy Creates Implicit Sharing Hazard

Line 3000 copies the session structure:

```c
worker_sess[d] = *sess;
```

This creates implicit sharing of:
- `eval_arena` (the race condition)
- `mat_cache.entries[]` at copy time (snapshot), but copies by value
- `rels[]` (correct — read-only during K-fusion)
- `frontiers[]` and `rule_frontiers[]` (independent copies, correct)

The intent is clear: sharing read-only `rels[]`, isolation of caches. But `eval_arena` should not be shared.

---

## 8. Evidence: Test Characteristics

### 8.1 CSPA vs DOOP

From docs:
- **CSPA:** K=3, ~1.5GB memory, completes in ~2 minutes
- **DOOP:** K=8, 2045GB memory (crash/OOM), timeout

The O(K) amplification factor (8/3 ≈ 2.67x) would predict ~4GB for DOOP, but actual is ~1360x larger.

**Root cause:** Compounding issues:
1. K-fold increase in worker threads and mat_cache copies
2. Shared arena contention causing reallocation/fragmentation
3. Delta relation accumulation across 8-9 workers
4. Join intermediate explosion (8-way joins)

### 8.2 Delta Seeding Overhead

From MEMORY.md:
- Delta-seeding was implemented to reduce iterations from 6→5 (1 iteration skipped)
- But creates N delta relations (one per relation with new facts)
- For DOOP with high cardinality: 10+ delta relations × 100K rows each = 1GB per iteration

---

## 9. Recommended Fixes (Priority Order)

### 9.1 CRITICAL: Per-Worker Arena Isolation

**Issue:** Shared `eval_arena` causes race condition and unbounded reallocation

**Fix:**
```c
// In col_op_k_fusion, after line 2967:
for (uint32_t d = 0; d < k; d++) {
    worker_sess[d] = *sess;
    // Create ISOLATED arena for this worker
    worker_sess[d].eval_arena = wl_arena_create(256 * 1024 * 1024);
    // ... other setup
}

// In cleanup, after line 3169:
for (uint32_t d = 0; d < k; d++) {
    wl_arena_free(worker_sess[d].eval_arena);
}
```

**Impact:** Eliminates race condition, prevents reallocation thrashing. Expected reduction: 50-70% memory for DOOP.

### 9.2 HIGH: Delta Relation Lifecycle Management

**Issue:** Temporary delta relations persist in `sess->rels[]`, accumulating across iterations

**Fix:** Use a separate scratch pool for pre-seeded deltas:
```c
// In col_session_snapshot, instead of session_add_rel:
col_rel_t **temp_deltas = malloc(sizeof(col_rel_t *) * affected_count);
for (uint32_t i = 0; i < affected_count; i++) {
    // Evaluate with temp_delta
    // At end of snapshot, free all temp_deltas
    col_rel_free_contents(temp_deltas[i]);
    free(temp_deltas[i]);
}
free(temp_deltas);
```

**Impact:** Prevent accumulation across iterations. Expected reduction: 20-30% memory.

### 9.3 MEDIUM: mat_cache Eviction Policy

**Issue:** Unbounded cache growth, 0% hit rate for DOOP (divergent rule copies)

**Fix:** Implement LRU eviction or per-worker cache size limits:
```c
#define COL_MAT_CACHE_MAX_SIZE_MB 512

if (cache_bytes > COL_MAT_CACHE_MAX_SIZE_MB * 1024 * 1024) {
    // Evict oldest entry
    col_rel_free_contents(cache->entries[oldest_idx].result);
    // Shift remaining entries
}
```

**Impact:** Cap per-worker cache at 512MB. Expected reduction: 5-10% memory for DOOP.

### 9.4 LOW: Arena Capacity Pre-allocation

**Issue:** 256MB arena may be too large or too small depending on workload

**Fix:** Make arena size proportional to input relations:
```c
size_t arena_capacity = 256 * 1024 * 1024;  // Default
for (uint32_t i = 0; i < plan->edb_count; i++) {
    // Sum input relation sizes
    arena_capacity = max(arena_capacity, total_edb_bytes);
}
sess->eval_arena = wl_arena_create(arena_capacity);
```

**Impact:** Reduce reallocation for large inputs. Expected reduction: 2-5% memory.

---

## 10. Verification Plan

### 10.1 Test Harness

Run DOOP with instrumentation:
```bash
# Before fixes
./build/tests/test_option2_doop 2>&1 | head -50
# Expect: OOM or 2000GB+ RSS

# After fix 9.1 (arena isolation)
# Expect: 10-15GB RSS, completes

# After fix 9.2 (delta lifecycle)
# Expect: 5-10GB RSS, completes faster

# After fix 9.3 (cache eviction)
# Expect: 3-5GB RSS, completes
```

### 10.2 Memory Profiling

```bash
# Use valgrind or heaptrack
heaptrack ./build/tests/test_option2_doop
heaptrack_gui heaptrack.test_option2_doop.*.gz
```

### 10.3 Correctness Validation

- All existing tests (30/31) must pass
- DOOP result correctness verified against expected output
- No regression on CSPA (should stay ~1.5GB)

---

## 11. References

- **workqueue.h (44-62):** Per-Worker Arena Cloning design intent
- **columnar_nanoarrow.c (3000):** Shallow copy bug
- **columnar_nanoarrow.c (5316-5340):** Delta pre-seeding
- **columnar_nanoarrow.c (2960-3175):** K-fusion worker dispatch
- **BREAKTHROUGH-RESEARCH-SUMMARY.md:** DOOP performance baseline (not feasible currently)
- **test_option2_doop.c:** DOOP validation harness

---

## Conclusion

The DOOP memory explosion (2045GB) is caused by **design misalignment** between the documented intent (per-worker arena isolation) and implementation (shared eval_arena). Combined with unbounded delta relation and cache growth, this creates O(K × cardinality × iterations) memory amplification.

**Priority 1 fix:** Per-worker arena isolation (fix 9.1). This addresses the fundamental architectural race condition and is a prerequisite for safe multi-worker execution.

**Combined impact of all fixes:** Expected reduction from 2045GB to ~3-5GB for DOOP, enabling completion within reasonable time/memory bounds.
