# Handle Remapping Data Structure Design
## Issue #557 — Open-Addressing Hash Table for Compound-Handle Remap

**Status**: Design (Phase 3C)
**Audience**: Implementers (#562, #563), reviewers, architects
**Depends on**: Compound arena (`wirelog/arena/compound_arena.h`), rotation strategy (#600)
**Consumed by**: #562 (lookup-table implementation), #563 (EDB remap pass)

---

## 1. Problem Statement

### 1.1 Why we need a remap table

Compound handles are 64-bit packed values:

```
  bit 63 ............................. bit 0
  +-----------------+--------+----------------+
  | session_seed:20 | epoch:12 |   offset:32   |
  +-----------------+--------+----------------+
```

Source: `wirelog/arena/compound_arena.h:55-109`.

The 12-bit `epoch` field saturates at **4095 generations**. Once the
compound arena's `current_epoch` reaches `max_epochs`, the arena refuses
new allocations (#550). The STANDARD rotation strategy (#600) currently
treats this as a hard wall.

To extend the lifetime of a session past 4095 generations — and to
support evacuating live handles to a fresh arena during compaction —
the system must rewrite every persisted handle from `(old_epoch,
old_offset)` to `(new_epoch, new_offset)`. The relation between old and
new handle is one-to-one, computed once during evacuation, and consulted
many times while EDB columns are walked.

That table is what this issue defines.

### 1.2 Access pattern

| Phase            | Operation                               | Frequency                    |
|------------------|-----------------------------------------|------------------------------|
| Build (evacuate) | `insert(old, new)` — once per live row  | up to ~live_handle_count     |
| Apply (rewrite)  | `lookup(old) -> new` — once per cell    | live_handle_count × handle-cols |
| Teardown         | `free()`                                | once per remap pass          |

The table is **build-then-query**: all inserts happen before any
lookup, and once lookups start the table is read-only. There is **no
per-entry deletion**. The whole table is freed in one shot.

### 1.3 Sizing target

| Workload            | Live handles | Notes                              |
|---------------------|--------------|------------------------------------|
| Typical (DOOP)      | ~30K–60K     | One epoch boundary                 |
| Stress (#596)       | ~100K        | Daemon-style; #557 design target   |
| Worst-case (in spec)| ~1M          | Bound by compound-arena live count |

The design must keep ~100K entries comfortably in L2/L3 cache and stay
under a few MB.

---

## 2. Solution: Open-Addressing Hash with Linear Probing

### 2.1 Choice summary

| Decision                   | Choice                              | §   |
|----------------------------|-------------------------------------|-----|
| Layout                     | Open addressing (single allocation) | 2.2 |
| Probe sequence             | Linear probing                      | 2.3 |
| Load factor (rehash trigger)| 0.75                                | 2.4 |
| Empty-slot sentinel        | `key == 0` (= `WL_COMPOUND_HANDLE_NULL`) | 2.5 |
| Deletion                   | Not supported (build-then-query)    | 2.6 |
| Capacity                   | Power of two, mask-based modulo     | 2.7 |

### 2.2 Why open addressing (vs chaining)

- **Single allocation**: one `calloc` of an array of `(int64_t key,
  int64_t value)` pairs. Chaining requires a node pool plus the bucket
  array — two allocations and a pointer chase per probe.
- **Cache efficiency**: linear probing walks contiguous memory; in the
  common case (load factor 0.5–0.75) a probe touches at most one or two
  64-byte cache lines. Chained buckets land in scattered nodes.
- **No per-entry malloc churn**: chaining would require either an arena
  for nodes or `malloc`/`free` per insert, both more code than the
  remap pass deserves.
- **Predictable layout**: the table is freed in one `free()` call — no
  walk-and-free traversal.

The classical downside of open addressing — bad behavior under high
load factor and clustering — is mitigated by (a) fixing the load
factor at 0.75, and (b) the access pattern being build-then-query, so
the table never sees mixed insert/erase that grows tombstone clutter.

### 2.3 Why linear probing (vs quadratic / double hashing)

- **Cache locality**: linear probing keeps probes within the same cache
  line for short runs, which dominates real-world performance below
  load factor 0.8.
- **Simpler termination**: a probe either finds the key, finds an
  empty slot, or runs back to its origin. No need to track a separate
  step size.
- **Empirically sufficient for our load**: handles are derived from a
  pseudo-random session seed XOR offset, so the input distribution is
  already well-mixed; we do not need quadratic probing's clustering
  resistance.
- **Tradeoff acknowledged**: linear probing's primary clustering can
  degrade once load factor exceeds ~0.8. We rehash at 0.75 so the
  observed factor stays in the linear-probing sweet spot.

If profiling later shows clustering pathologies (probe lengths
> 8), the implementation can swap to quadratic probing without an API
change — the probe sequence is encapsulated inside `insert` and
`lookup`.

### 2.4 Load-factor threshold (0.75)

- **Below 0.5**: wastes memory; mean probe length ~1.0.
- **0.5–0.75**: mean probe length 1.5–2.5; sweet spot for linear
  probing.
- **0.75–0.85**: mean probe length 3–6; acceptable, but variance
  rises.
- **Above 0.85**: linear probing degenerates (chains of 20+ slots).

We rehash when `count > capacity * 3 / 4` (avoiding floats; equivalent
to load factor 0.75 within rounding). Rehash doubles capacity, so the
amortized insert cost stays O(1).

### 2.5 Sentinel: `key == 0` marks empty

`WL_COMPOUND_HANDLE_NULL` is defined as `(uint64_t)0` and is documented
as a value the arena never returns (`compound_arena.h:55-72`). That
makes `0` a free sentinel for "empty slot" without burning an extra
byte per entry for a state flag.

We treat the key field as `int64_t` to match how handles are stored in
relation columns (`int64_t *col[]`). The bit pattern is identical to
`uint64_t`; sign is irrelevant for hashing and equality.

### 2.6 Why no per-entry deletion

The remap pass is **build-then-query**: insert phase finishes before
lookup phase begins, and the entire table is freed when the remap
pass ends. Supporting per-entry deletion would require either:

- **Tombstones**: extra state per slot (or a reserved sentinel value)
  plus tombstone counting; lookups must skip tombstones; eventually
  rebuild to reclaim them.
- **Backshift deletion**: walk the cluster after the deleted slot and
  shift entries back; correct but O(cluster length) per delete and
  fragile.

Both add complexity for zero benefit at this layer. If a future caller
needs erase semantics, it can build a thin wrapper that marks entries
externally; the underlying table remains insert-only.

This decision is documented in the header: callers must not call
`insert` with a key already present except to update its value (the
implementation overwrites in place).

### 2.7 Capacity rounding

The implementation rounds the requested capacity up to the next power
of two so the modulo operation reduces to `index & (capacity - 1)`.
The minimum capacity is 16 (small enough not to matter; large enough
that the rounding doesn't dominate). This is an implementation detail
and not exposed through the API.

---

## 3. Data Structure

### 3.1 Public type (opaque)

```c
typedef struct wl_handle_remap wl_handle_remap_t;
```

The struct definition lives in `handle_remap.c` only; callers see only
the typedef. This keeps the layout free to change (e.g., switch probe
sequences, add stats counters) without ABI churn.

### 3.2 Internal layout (informative)

```c
struct wl_handle_remap {
    int64_t  *keys;       /* capacity entries; 0 = empty                    */
    int64_t  *values;     /* capacity entries; only valid when keys[i] != 0 */
    size_t    capacity;   /* power of two; index mask = capacity - 1        */
    size_t    count;      /* live entries (excluding empties)               */
    size_t    rehash_at;  /* count threshold that triggers grow             */
};
```

Splitting `keys` and `values` into separate arrays (struct-of-arrays)
keeps the lookup hot loop scanning only keys, doubling the keys per
cache line (8 keys per 64-byte line vs. 4 key/value pairs). Values are
only fetched after a key match.

### 3.3 Hash function

```c
/* SplitMix64 finalizer — 4 mul/xor/shift, no branches, well-mixed. */
static inline uint64_t
wl_handle_remap_hash(int64_t key) {
    uint64_t x = (uint64_t)key;
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27; x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x;
}
```

SplitMix64's finalizer is well-known to produce a high-quality bit
mix from sequential or weakly-distributed input. Compound handles
have considerable structure (session_seed in the high bits, offset in
the low bits), so we cannot just mask the raw key — we need a real
mix to break correlations.

This is the same finalizer used in `xxhash`, Java's `SplittableRandom`,
and Go's hash/maphash. It is freely usable.

---

## 4. API Operations

### 4.1 `wl_handle_remap_create(capacity, out)`

- Rounds `capacity / 0.75` up to the next power of two (minimum 16).
- Allocates `keys` (zeroed) and `values` arrays.
- Stores `rehash_at = capacity * 3 / 4`.
- Returns `0` on success, `ENOMEM` on allocation failure, `EINVAL`
  if `out == NULL`.

### 4.2 `wl_handle_remap_insert(remap, old, new)`

- Asserts `old != 0` (NULL handle is the empty sentinel; cannot be a
  key). Returns `EINVAL` for `old == 0`.
- Hashes `old` and probes linearly until finding either a matching key
  (overwrite value) or an empty slot (insert).
- If `count + 1 > rehash_at`, allocates a new keys/values array of
  double capacity, reinserts every live entry, and frees the old
  arrays. Reinsertion uses the same `insert` path so probe ordering
  is consistent.
- Returns `0` on success, `ENOMEM` if rehash allocation fails (the
  table remains usable at its previous size).

### 4.3 `wl_handle_remap_lookup(remap, old)`

- Returns `0` (`WL_COMPOUND_HANDLE_NULL`) if `remap == NULL` or
  `old == 0`.
- Hashes `old` and probes linearly until finding either a matching key
  (return value) or an empty slot (key not present).
- On not-found, returns `0`. The caller distinguishes "not present"
  from "remapped to NULL" by the structural invariant that no
  handle ever maps to NULL (NULL is reserved).

### 4.4 `wl_handle_remap_free(remap)`

- NULL-safe.
- Frees `keys`, `values`, and the struct itself.

### 4.5 Error codes

The header maps the OMC names to **positive errno** values to match
the existing `wirelog/columnar/` convention. Other files in this
directory (`session.c`, `eval.c`, `internal.h`) return `ENOMEM` /
`EINVAL` / `ENOENT` directly with no sign flip; this header follows
the same pattern so callers can compare against `errno.h` constants
without translation:

| OMC name                  | Constant            |
|---------------------------|---------------------|
| `WL_ERROR_INVALID_ARGS`   | `EINVAL`            |
| `WL_ERROR_OOM`            | `ENOMEM`            |
| `WL_ERROR_NOT_FOUND`      | `ENOENT` (reserved) |

`WL_ERROR_NOT_FOUND` is not currently returned by any operation — the
lookup contract is "return 0 on miss" — but the constant is reserved
in the header for future operations (e.g., explicit `find_or_fail`).

---

## 5. Memory Estimate

| Live handles | Rounded cap | keys+values | Overhead |
|--------------|-------------|-------------|----------|
| 1K           | 2048        | 32 KB       | ~0       |
| 10K          | 16384       | 256 KB      | ~0       |
| 50K          | 65536       | 1 MB        | ~0       |
| **100K**     | **131072**  | **2 MB**    | **~0**   |
| 500K         | 1048576     | 16 MB       | ~0       |
| 1M           | 2097152     | 32 MB       | ~0       |

(Each slot = 16 bytes: 8 keys + 8 values, struct-of-arrays.
"Overhead" is the few dozen bytes of struct fields.)

For the design target (100K live handles), the table fits in 2 MB —
well within typical L2 cache pressure budgets. At 1M handles (worst
case in #596 stress), 32 MB is still a single contiguous allocation
that the kernel can back with huge pages on Linux.

---

## 6. Concurrency

The remap table is **single-threaded**. The intended caller (#563 EDB
remap pass) runs on the main thread between epochs while K-Fusion
workers are paused (compound arena frozen, see #600 §1). No locks are
needed and none are provided.

If a future caller wants concurrent reads after build, it can wrap the
table in a `pthread_rwlock` externally — the data structure has no
internal mutability after the build phase ends, so reader-only
concurrency is safe.

---

## 7. Testing Strategy (consumed by #562)

#562 will add unit tests against this header:

1. **Smoke**: create / insert one / lookup one / free.
2. **Capacity round-up**: request 100; expect lookups still O(1).
3. **Rehash trigger**: insert past the 0.75 threshold; verify all
   prior keys still resolve to their original values.
4. **Collision stress**: insert 100K random handles; assert mean probe
   length < 3.
5. **Lookup miss**: lookup a never-inserted handle; expect 0.
6. **Insert overwrite**: insert (k, v1) then (k, v2); lookup(k) ==
   v2; count unchanged.
7. **Reject NULL key**: insert with `old == 0` returns `EINVAL`.
8. **NULL-safe free**: `wl_handle_remap_free(NULL)` is a no-op.

Stress and ASan/TSan coverage piggyback on #563's full EDB-remap
pass, which exercises the table at production scale.

---

## 8. References

- `wirelog/arena/compound_arena.h:55-109` — handle bit layout and
  accessors (`wl_compound_handle_session/_epoch/_offset`).
- `wirelog/columnar/internal.h` — `col_rel_t` structure (handle
  columns are `int64_t *`).
- `ROTATION_STRATEGY_DESIGN.md` (Issue #600) — when the remap pass
  fires (epoch boundary, arena frozen).
- Issue #550 — compound arena saturation (the wall this design helps
  push past).
- Issue #562 — lookup-table implementation (consumes this header).
- Issue #563 — EDB remap pass (the first user).

---

**End of design document.**
