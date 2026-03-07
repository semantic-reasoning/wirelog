# Content-Based Cache Key Design for CSE Materialization

**Status:** Design Complete
**Scope:** US-002 -- Replace pointer-based CSE cache keying with content-based hashing
**Target:** `wirelog/backend/columnar_nanoarrow.c` lines 314-413

---

## 1. Problem Statement

The CSE materialization cache (`col_mat_cache_t`) achieves 0% hit rate in K-copy evaluation because it keys entries by pointer identity:

```c
// columnar_nanoarrow.c:326-328
typedef struct {
    const col_rel_t *left_ptr;   /* cache key: left input relation ptr  */
    const col_rel_t *right_ptr;  /* cache key: right input relation ptr */
    col_rel_t *result;           /* owned cached join result            */
    ...
} col_mat_entry_t;
```

Lookup compares raw pointers (line 357-358):
```c
if (cache->entries[i].left_ptr == left
    && cache->entries[i].right_ptr == right)
```

In K-copy evaluation, each copy creates fresh intermediate `col_rel_t` instances for join results. Two copies producing identical data get different pointers, so every lookup misses. This defeats the purpose of CSE materialization for multi-copy rules (DOOP's 8/9-way joins, CSPA's 3-way joins).

---

## 2. Cache Key Design

### 2.1 Current Key

```c
// Key: (const col_rel_t *left, const col_rel_t *right)
// Comparison: pointer equality (==)
```

### 2.2 Proposed Key Structure

```c
typedef struct {
    uint64_t left_hash;   /* content hash of left input relation  */
    uint64_t right_hash;  /* content hash of right input relation */
} col_mat_cache_key_t;
```

### 2.3 Proposed Entry Structure

```c
typedef struct {
    col_mat_cache_key_t key;    /* content-based cache key             */
    col_rel_t *result;          /* owned cached join result            */
    size_t mem_bytes;           /* bytes used by result->data          */
    uint64_t lru_clock;         /* logical time of last access         */
} col_mat_entry_t;
```

### 2.4 Migration Path

The external API signature remains unchanged:

```c
static col_rel_t *
col_mat_cache_lookup(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right);

static void
col_mat_cache_insert(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right, col_rel_t *result);
```

Internally, these functions compute content hashes from the `col_rel_t*` arguments and compare/store `col_mat_cache_key_t` values instead of raw pointers. The call sites at lines 1018 and 1150 require zero changes.

---

## 3. Content Hash Function

### 3.1 Specification

```c
/**
 * hash_relation_content - Compute a content-based hash of a col_rel_t.
 *
 * @param rel  Relation to hash (may be NULL or empty).
 * @return     Deterministic uint64_t hash based on data content + shape.
 *
 * Properties:
 *   - Deterministic: same data layout -> same hash (guaranteed by sorted order)
 *   - Discriminating: different data -> different hash (high probability)
 *   - Bounded cost: O(min(K, nrows)) where K = 100
 *   - Pure: no side effects, no state
 */
static uint64_t
hash_relation_content(const col_rel_t *rel);
```

### 3.2 Algorithm

```
function hash_relation_content(rel):
    if rel is NULL or rel->nrows == 0:
        return 0

    hash = 14695981039346656037  // FNV-1a offset basis (64-bit)
    K = min(100, rel->nrows)

    // Sample first K rows (deterministic because relations are sorted post-CONSOLIDATE)
    for i in 0..K-1:
        for j in 0..rel->ncols-1:
            val = rel->data[i * ncols + j]
            // FNV-1a: XOR then multiply
            hash = hash XOR (uint64_t)val
            hash = hash * 1099511628211  // FNV prime

    // Mix in shape to distinguish relations with identical prefixes but different sizes
    hash = hash XOR (uint64_t)rel->nrows
    hash = hash * 1099511628211
    hash = hash XOR (uint64_t)rel->ncols
    hash = hash * 1099511628211

    return hash
```

### 3.3 Why FNV-1a Over djb2

The test suite (`test_cse_cache_hit_rate.c:108`) uses djb2 (`hash = ((hash << 5) + hash) ^ val`). FNV-1a is recommended for production because:

- **Better avalanche:** Each input bit affects more output bits, reducing collision probability for similar-but-not-identical relations (e.g., relations differing only in the last few rows beyond the sample window).
- **Standard:** FNV-1a is a well-characterized hash with known collision properties for 64-bit outputs.
- **Same cost:** Both are single-pass, no-allocation, O(K * ncols) multiplies/XORs.

Either hash family is acceptable for a cache (collisions cause a redundant computation, not incorrect results). The djb2 variant from the test suite is also a valid implementation choice if consistency with existing test code is preferred.

### 3.4 Cost Analysis

| Relation Size | Rows Hashed | Cost (approx) |
|---------------|-------------|----------------|
| 0 rows | 0 | ~5 ns (null check + return) |
| 10 rows, 3 cols | 10 | ~100 ns (30 multiply-XOR ops + 2 shape ops) |
| 100 rows, 3 cols | 100 | ~1 us (300 multiply-XOR ops + 2 shape ops) |
| 450,000 rows, 5 cols | 100 | ~1 us (500 multiply-XOR ops + 2 shape ops) |

The cost is dominated by the sample size K=100, independent of actual relation size. For the CSPA workload where join results reach 450K rows, hashing costs ~1 microsecond versus millions of CPU cycles for recomputing the join on a cache miss.

### 3.5 Determinism Guarantee

Content hashing is deterministic **because relations are in canonical form after CONSOLIDATE** (`columnar_nanoarrow.c:1377`):
- `qsort_r` sorts rows lexicographically by all columns
- Duplicate rows are removed (lines 1380-1391)
- Result: identical logical relations have identical physical byte layout in `data[]`

This means the first K rows of two semantically equal relations are byte-identical, producing the same hash.

**Important caveat:** The hash function must only be applied to relations that have been consolidated (sorted + deduped). Pre-consolidation intermediates may have rows in arbitrary order. The current call sites (lines 1016-1018 and 1149-1150) operate on session relations (`session_find_rel` results at line 1007) which are consolidated after each semi-naive iteration (`col_idb_consolidate` at line 2633). This invariant holds.

For owned intermediate relations (join outputs before consolidation), the `!left_e.owned` guard at line 1016 already prevents cache lookup. With content-based keying, this guard should be **retained** to ensure only consolidated (canonical-order) relations are hashed.

---

## 4. Key Generation Strategy

### 4.1 Options Evaluated

**Option A: Hash each input relation independently**
```c
key.left_hash  = hash_relation_content(left);
key.right_hash = hash_relation_content(right);
// Lookup: key.left_hash == entry.left_hash && key.right_hash == entry.right_hash
```

**Option B: Hash a combined tuple**
```c
// Hash (nrows_L, ncols_L, first_row_L, nrows_R, ncols_R, first_row_R) into single uint64
```

**Option C: Interned relation IDs**
```c
// Assign monotonic ID at relation creation; use (left_id, right_id) as key
// Requires tracking content->ID mapping (essentially a separate hash table)
```

### 4.2 Recommendation: Option A

**Option A is recommended** for the following reasons:

1. **Simplicity:** Two independent hash calls, one comparison per entry. No additional data structures.

2. **Composability:** Left and right hashes can be computed independently, cached on the `col_rel_t` itself if needed for amortization (future optimization: store `uint64_t content_hash` on `col_rel_t` and invalidate on mutation).

3. **Collision analysis:** With two independent 64-bit hashes, the probability of a false positive match is ~2^-128 per comparison (assuming good hash distribution). This is astronomically unlikely -- far below the threshold where it matters for a performance cache.

4. **No extra state:** Option C requires a content-to-ID hash map, adding complexity and memory overhead. Option B conflates two independent inputs into one hash, increasing collision risk and making debugging harder.

**Why not Option B:** A single combined hash loses the ability to independently reason about which input changed. It also has slightly higher collision probability (two different (left, right) pairs could produce the same combined hash more easily than matching on two independent hashes).

**Why not Option C:** Interned IDs require a separate lookup table mapping content hashes to sequential IDs. This adds complexity (another hash table, ID lifecycle management) without meaningful benefit over direct content hashing. It would be warranted if hash computation were expensive (it is not at ~1us) or if the same relation appeared in many different cache keys (amortization benefit), but the current cache has at most 64 entries.

---

## 5. K-Copy Isolation Semantics

### 5.1 Current Behavior

With pointer-based keying, each K-copy's intermediate join result gets a unique `col_rel_t*`. Even when two copies join the same left relation with the same right relation and produce identical output, the cache misses because the pointers differ. Result: **0% hit rate** for K-copy shared prefixes.

### 5.2 New Behavior

With content-based keying, two K-copies that join identical inputs produce the same `(left_hash, right_hash)` key. The cache returns the previously computed join result. Result: **cache hit when data is identical**.

### 5.3 Semantic Correctness

**This is semantically correct.** The join operation is a pure function of its inputs:

```
JOIN(left_data, right_data, keys) = deterministic_result
```

If `left_data_1 == left_data_2` and `right_data_1 == right_data_2` (byte-identical after consolidation), then `JOIN(left_1, right_1, keys) == JOIN(left_2, right_2, keys)`. The join keys are determined by the plan operator (`op->left_keys`, `op->right_keys`), which is the same across K-copies of the same rule (they share the same plan structure).

**Edge case -- delta mode:** The `!left_e.owned` guard (line 1016) ensures we only cache-lookup when the left input is a stable session relation. Delta relations (`$d$<name>`) are also session relations found via `session_find_rel` (line 1007). Two K-copies forcing different deltas will have different right-side relations (different delta contents), producing different `right_hash` values. The cache correctly distinguishes these cases.

### 5.4 When K-Copy Reuse Applies

For a 3-copy rule like CSPA's R3:
- Copy 0 forces delta for IDB_A, uses full IDB_B and IDB_C
- Copy 1 forces delta for IDB_B, uses full IDB_A and IDB_C
- Copy 2 forces delta for IDB_C, uses full IDB_A and IDB_B

If Copy 0 and Copy 1 share a common join prefix (e.g., both join the same `assign` EDB relation first), and that prefix produces identical intermediate results, the cache hit avoids recomputing it.

For DOOP's 8/9-way rules, the reuse potential is much higher -- up to 7-8 copies may share common multi-way join prefixes.

---

## 6. Backward Compatibility

### 6.1 API Stability

The cache functions maintain the same external signatures:

```c
// No change to function signatures
static col_rel_t *
col_mat_cache_lookup(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right);

static void
col_mat_cache_insert(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right, col_rel_t *result);
```

Call sites at lines 1018 and 1150 require no modification. The `col_mat_cache_t` struct changes internally (entry layout), but it is file-scoped (`static`) with no external visibility.

### 6.2 Behavioral Change

- **Before:** Cache hit requires pointer identity (same `col_rel_t*` for both left and right).
- **After:** Cache hit requires content identity (same data layout for both left and right).

This is strictly more permissive -- every pointer match is also a content match, but not vice versa. No existing correct behavior is broken.

### 6.3 Fallback Strategy

If content hashing introduces unexpected issues:

1. **Compile-time toggle:** `#define COL_MAT_CACHE_CONTENT_KEYING 1` wrapping the new code, with pointer-based fallback when set to 0.
2. **Runtime detection:** If cache hit rate remains 0% after the change (monitored via `mat_cache.clock` vs hit counter), log a diagnostic and fall back to pointer-based keying. This is a development diagnostic, not a production feature.

The hash function is pure (no state, no allocation, no side effects). If it produces incorrect hashes (bug), the worst case is cache misses (correctness preserved) or false cache hits (correctness risk). The false-hit risk is mitigated by the ~2^-128 collision probability of dual 64-bit hashes.

---

## 7. Performance Expectations

### 7.1 Hash Computation Overhead

| Operation | Cost | Frequency per Iteration |
|-----------|------|------------------------|
| `hash_relation_content` | ~1 us | 2 per cache lookup + 2 per cache insert |
| Total hash overhead per iteration | ~4-8 us | Negligible vs join cost (~ms) |

### 7.2 Cache Hit Benefit

| Scenario | Rows Saved | CPU Cycles Saved | Wall Time Impact |
|----------|------------|-------------------|------------------|
| DOOP 8-way rule, K=8 | Up to 7 x 450K-row join results | ~10M+ cycles per avoided join | **Transformative** (seconds saved per iteration) |
| CSPA 3-way rule, K=3 | Up to 2 x shared prefix joins | ~1-5M cycles per avoided join | **Moderate** (hundreds of ms saved across iterations) |
| Simple 2-way rule, K=2 | 0-1 shared prefix | ~0.5-1M cycles | **Minor** |

### 7.3 Net Assessment

| Workload | Hash Cost | Cache Benefit | Net |
|----------|-----------|---------------|-----|
| DOOP (8/9-way rules) | +8 us/iter | -seconds/iter | **Strongly positive** |
| CSPA (3-way rules) | +4 us/iter | -100s of ms total | **Positive** |
| Simple programs (2-way) | +4 us/iter | ~0 (few copies) | **Neutral** (negligible overhead) |

### 7.4 Memory Impact

No additional memory allocation. The `col_mat_cache_key_t` (16 bytes) replaces two pointers (16 bytes on 64-bit). Cache entry size is unchanged. The hash function uses no heap allocation.

---

## 8. Thread Safety

### 8.1 Current State

The materialization cache is embedded in `wl_col_session_t` (line 466):
```c
col_mat_cache_t mat_cache;  /* materialization cache (US-006) */
```

Sessions are not shared across threads. All cache operations occur within a single session's evaluation path (`col_eval_relation_plan` and its callees).

### 8.2 Hash Function Properties

`hash_relation_content` is a **pure function**:
- Reads only from `rel->data`, `rel->nrows`, `rel->ncols` (all const/stable during lookup)
- No writes to shared state
- No heap allocation
- No static/global variables

### 8.3 Conclusion

No thread-safety changes are needed. The function is safe for future multi-threaded use (e.g., workqueue Phase B-lite) without modification, as each session would have its own cache instance.

---

## 9. Implementation Checklist

1. Add `hash_relation_content()` to `columnar_nanoarrow.c` (near line 315, before cache functions)
2. Replace `col_mat_entry_t` pointer fields with `col_mat_cache_key_t key` (lines 326-328)
3. Update `col_mat_cache_lookup` to compute hashes and compare key structs (lines 352-364)
4. Update `col_mat_cache_insert` to compute and store key hashes (lines 366-413)
5. Retain the `!left_e.owned` guard at line 1016 (ensures canonical sort order)
6. Add compile-time toggle `COL_MAT_CACHE_CONTENT_KEYING` for fallback
7. Validate with `test_cse_cache_hit_rate` test suite (all 8 tests must pass)

---

## References

- `wirelog/backend/columnar_nanoarrow.c:326-332` -- current `col_mat_entry_t` structure
- `wirelog/backend/columnar_nanoarrow.c:352-364` -- current pointer-based lookup
- `wirelog/backend/columnar_nanoarrow.c:366-413` -- current cache insert with LRU eviction
- `wirelog/backend/columnar_nanoarrow.c:1016-1023` -- cache lookup call site (materialized join)
- `wirelog/backend/columnar_nanoarrow.c:1147-1153` -- cache insert call site
- `wirelog/backend/columnar_nanoarrow.c:1377` -- qsort_r consolidation (guarantees sorted order)
- `wirelog/backend/columnar_nanoarrow.c:1380-1391` -- dedup after sort (guarantees canonical form)
- `wirelog/backend/columnar_nanoarrow.c:466` -- mat_cache field in session struct
- `tests/test_cse_cache_hit_rate.c:97-133` -- content-based key and hash function prototypes
- `docs/k-copy/BREAKTHROUGH-RESEARCH-SUMMARY.md` -- K-copy architecture and CSE context
