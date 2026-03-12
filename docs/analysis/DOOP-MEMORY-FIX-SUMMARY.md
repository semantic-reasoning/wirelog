# DOOP Memory Explosion: Quick Reference

## The Problem

DOOP benchmark with 8-way K-fusion rule copies causes **2045GB memory explosion** (vs 1.5GB for CSPA with K=3). Currently OOM or timeout.

## Root Causes (Confluence of 5 Issues)

1. **Shared eval_arena** (critical race condition)
   - All K workers write to same `eval_arena->offset` pointer
   - No synchronization in arena allocator
   - Causes data corruption, reallocation thrashing

2. **Shallow-copy worker sessions**
   - `worker_sess[d] = *sess` copies eval_arena pointer
   - Intent: share read-only `rels[]`
   - Reality: shares mutable `eval_arena` without protection

3. **Delta relation accumulation**
   - Pre-seeded deltas ($d$RelName) stored in `sess->rels[]`
   - Never cleaned up except when overwritten
   - Persist across iterations: 1 EDB delta + K IDB deltas per iteration

4. **Unbounded mat_cache growth**
   - Materialization cache entries never evicted
   - DOOP's 8-way joins create 56 independent intermediates (8 workers × 7 joins)
   - 0% hit rate, unlimited growth

5. **Incremental delta pre-seeding overhead**
   - Creates temporary relations during evaluation
   - Loops creating rows: `for (uint32_t row = 0; row < delta_nrows; row++)`
   - 100K+ rows × multiple relations = 100-500MB per iteration

## Memory Amplification Calculation

```
Base relations:           1-2 GB
K workers (8):           0.1 GB (shallow copies)
Join intermediates:      0.1 GB × 8 workers = 112 MB per iteration
Delta relations:         0.5 GB per iteration
Arena fragmentation:     10-20%
Iterations (5-10):       5-10×
─────────────────────────────
Subtotal:                2-2.5 GB base + (0.6 GB × 10 iters) = 8-10 GB expected

Actual (with bugs):      2045 GB (200× amplification)

Root cause: Compounding bugs trigger exponential accumulation
```

## The Bug (in Code)

**File:** `wirelog/backend/columnar_nanoarrow.c`

**Line 3000 - The Critical Bug:**
```c
for (uint32_t d = 0; d < k; d++) {
    worker_sess[d] = *sess;  // ← SHALLOW COPY
    // eval_arena pointer is copied, not cloned
    // All K workers now share the same arena
}
```

**Why it's a bug:**
- workqueue.h lines 44-62 explicitly state: "Each worker thread must own an independent arena"
- Implementation violates this: `eval_arena` is shared
- Arena has no synchronization (no mutex)
- K threads racing to increment same `eval_arena->offset` = undefined behavior

**Line 5316-5340 - Delta Explosion:**
```c
for (uint32_t i = 0; i < sess->nrels; i++) {
    col_rel_t *delta = col_rel_new_auto(dname, r->ncols);
    for (uint32_t row = 0; row < delta_nrows; row++) {
        col_rel_append_row(delta, r->data + ...);  // ← Row-by-row copy loop
    }
    session_add_rel(sess, delta);  // ← Stored in sess->rels[] permanently
}
```

**Line 2968 - Per-Worker Overhead:**
```c
wl_col_session_t *worker_sess = calloc(k, sizeof(wl_col_session_t));
// K full copies of session struct, each with:
// - copy of mat_cache (unbounded growth)
// - pointer to shared eval_arena (WRONG)
// - separate arr_entries, darr_entries (correct isolation)
```

## Fix Priority

### Priority 1 (CRITICAL): Per-Worker Arena Isolation
**What:** Create independent eval_arena for each worker
**Where:** `col_op_k_fusion()` around line 3000
**Code:**
```c
// Before submitting workers:
for (uint32_t d = 0; d < k; d++) {
    worker_sess[d] = *sess;
    worker_sess[d].eval_arena = wl_arena_create(256 * 1024 * 1024);  // NEW
}

// In cleanup:
for (uint32_t d = 0; d < k; d++) {
    wl_arena_free(worker_sess[d].eval_arena);  // NEW
}
```
**Impact:** Eliminates race condition, prevents reallocation thrashing
**Expected memory reduction:** 50-70% (2045GB → ~500-1000GB)

### Priority 2 (HIGH): Delta Relation Lifecycle
**What:** Use temporary delta pool instead of persistent `sess->rels[]`
**Where:** `col_session_snapshot()` around line 5316
**Impact:** Prevent accumulation across iterations
**Expected memory reduction:** 20-30% additional

### Priority 3 (MEDIUM): mat_cache Eviction
**What:** Implement LRU eviction when cache exceeds 512MB
**Where:** `col_mat_cache_insert()`
**Impact:** Cap unbounded growth
**Expected memory reduction:** 5-10% additional

### Priority 4 (LOW): Arena Capacity Tuning
**What:** Size arena proportional to input relations
**Where:** `col_session_create()` line 4708
**Impact:** Reduce reallocation overhead
**Expected memory reduction:** 2-5% additional

## Combined Impact

| Fix | Memory (DOOP) | Status |
|-----|---------------|--------|
| Current bug | 2045GB (OOM) | Fails |
| + Priority 1 | 500-1000GB | Executes, slow |
| + Priority 2 | 300-500GB | Executes, acceptable |
| + Priority 3 | 100-300GB | Executes, fast |
| + Priority 4 | ~50-100GB | Executes, optimal |

Target: < 10GB (acceptable)

## Testing Strategy

1. **Before fix:** `test_option2_doop` crashes/OOM
2. **After Priority 1:** Test completes, validates arena isolation
3. **After Priority 2:** Delta cleanup verified via relation count
4. **After Priority 3:** mat_cache size capped, hit rate improved
5. **After Priority 4:** Memory stable, no reallocation

## Verification Commands

```bash
# Build
meson compile -C build

# Test
./build/tests/test_option2_doop  # Before fix: OOM
                                  # After fix: PASS

# Memory profile
heaptrack ./build/tests/test_option2_doop
heaptrack_gui heaptrack.test_option2_doop.*.gz

# Correctness
./build/tests/test_option2_doop 2>&1 | grep -E "PASS|FAIL|ERROR"
```

## References

- **Root cause analysis:** `docs/analysis/DOOP-MEMORY-EXPLOSION-ROOT-CAUSE.md`
- **Design intent:** `wirelog/workqueue.h` lines 44-62
- **Bug location 1:** `wirelog/backend/columnar_nanoarrow.c` line 3000
- **Bug location 2:** `wirelog/backend/columnar_nanoarrow.c` line 5316-5340
- **Test harness:** `tests/test_option2_doop.c`
- **Session structure:** `wirelog/backend/columnar_nanoarrow.c` lines 714-807

## Estimated Effort

| Task | Effort | Risk |
|------|--------|------|
| Priority 1 (arena isolation) | 2 hours | Low |
| Priority 2 (delta lifecycle) | 3 hours | Medium |
| Priority 3 (cache eviction) | 2 hours | Medium |
| Priority 4 (arena sizing) | 1 hour | Low |
| Testing + validation | 2 hours | Medium |
| **Total** | **10 hours** | **Medium** |

## Next Steps

1. **Implement Priority 1** (arena isolation) — this is the fundamental fix
2. **Run test_option2_doop** — should complete without OOM
3. **Implement Priority 2-4** — performance improvements
4. **Validate correctness** — all 30 existing tests + DOOP output
5. **Benchmark CSPA** — ensure no regression (should stay ~1.5GB)
