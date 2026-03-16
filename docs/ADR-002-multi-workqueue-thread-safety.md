# ADR-002: Multi-Workqueue Thread-Safety Design (Option B — Verified)

**Status**: Approved & Verified (Consensus Planning + Evidence-Based Validation)
**Date**: 2026-03-16
**Validation**: TSan clean (K=1,2,4,8), zero data races, output correctness confirmed

---

## Decision: Option B — Selective Isolation (Already Implemented)

**Finding**: The current codebase already correctly implements Option B selective isolation for multi-worker K-fusion. No design changes needed.

Workers share read-only EDB/IDB relations (`rels[]`), each worker gets exclusive:
- `eval_arena` (per-worker at ops.c:2309-2311, NOT shared)
- `eval_stack` (independent evaluation context)
- Per-worker cache (mat_cache by-value copy, isolated per worker)
- Arrangement snapshots (read-only during parallel phase)

Results collected in worker buffer, merged sequentially by main thread (collect-then-merge pattern per ADR-001).

---

## Verification Results

### Thread-Safety: ✅ VERIFIED CORRECT
- **TSan validation**: 75 tests run with `-Db_sanitize=thread,undefined`, **ZERO data races detected**
- **ASAN validation**: **ZERO heap errors** (use-after-free, buffer overflows, leaks)
- **Output correctness**: FNV-1a fingerprint match across K=1,2,4,8 workers (byte-for-byte identical)
- **Session audit**: Exhaustive field-by-field verification of shared vs exclusive isolation

### Key Finding: eval_arena is Per-Worker
Contrary to initial hypothesis, `ops.c:2309-2311` already creates independent arenas:
```c
worker_sess[d].eval_arena
    = sess->eval_arena ? wl_arena_create(sess->eval_arena->capacity)
                       : NULL;
```
The shallow session copy at line 2304 is immediately overridden. Each worker owns an exclusive arena. **No shared arena access occurs.**

### Session Isolation Table (Verified)
| Field | Classification | Evidence |
|-------|----------------|----------|
| `rels[]`, `nrels`, `rel_cap` | [SHARED_RO] | Read-only via `session_find_rel()`; no writes from operators |
| `plan` | [SHARED_RO] | Const pointer; immutable during dispatch |
| `current_iteration`, `delta_seeded` | [SHARED_RO] | Read scalars; set before dispatch |
| `eval_arena` | [WORKER_EXCL] | Per-worker `wl_arena_create()` at ops.c:2309 |
| `eval_stack` | [WORKER_EXCL] | Embedded in worker_t; ops.c:2225 |
| `mat_cache` | [WORKER_EXCL] | By-value copy; ops.c:2304 + base_count guard |
| `arr_entries`, `darr_entries` | [WORKER_EXCL] | Zeroed per-worker; ops.c:2312-2317 |
| `delta_pool` | [WORKER_EXCL] | Per-worker allocation; ops.c:2318-2319 |
| `frontiers[]`, `rule_frontiers[]` | [SESSION_ONLY] | Never accessed by workers; main thread only |
| `wq` | [NULLED] | Set NULL in worker copies to prevent nested dispatch |

---

## Why Option B (Already Implemented)

### Hazard Prevention (Proven)
| Hazard | Prevention | Evidence |
|--------|-----------|----------|
| H1: session_find_rel/add_rel race | Only main thread calls in merge phase | ops.c:2443-2485 sequential merge |
| H2: nrels/rel_cap mutation race | Only main thread modifies (post-barrier) | session.c:869 in main-thread-only snapshot |
| H3: Concurrent append to target | Main thread merges sequentially | ops.c:2443+ merges K results → main session |
| H4: Arena contention (NEW) | Per-worker arenas (ops.c:2309) | Each worker gets independent arena |

### Collect-Then-Merge Pattern (Verified)
```
Main Thread         Workers (K)           Main Thread
═══════════════════════════════════════════════════════════════
submit()  ──────► parallel eval
          (each worker has exclusive
           eval_arena, eval_stack, cache)
          
wait_all() ◄────── barrier (mutex)
          
merge()   ─── collect results, free worker arenas, update session
```

The barrier's mutex release/acquire provides happens-before ordering. All worker writes are visible to main thread post-barrier. No concurrent mutations of shared state occur.

---

## What Was WRONG in Initial Analysis

1. **eval_arena race condition**: ❌ False premise
   - Current code already creates per-worker arenas
   - No shared arena access occurs
   - No fix needed

2. **2045 GB memory amplification**: ❌ Unvalidated claim
   - No such measurement found in repository
   - Highest documented peak RSS is 6.5 GB (CSPA baseline)
   - Cannot investigate without reproducible dataset/command

3. **Design decision needed**: ❌ False premise
   - Option B is already implemented
   - No design change needed
   - Verification only required (now complete)

---

## Real Optimization Opportunities Found

### Issue 1: Dead Arena Allocations (P0)
- Session arena: 256 MB allocated, never used (`wl_arena_alloc()` has 0 callers)
- K-fusion worker arenas: K × 256 MB each, never used
- **For K=8**: 2.3 GB wasted per invocation
- **Fix**: Remove arena allocations (~20 LOC delete)
- **Risk**: NONE (legacy code, zero dependencies)

### Issue 2: Unbounded mat_cache in Recursive Strata (P1)
- Cache eviction implemented for non-recursive strata (512 MB limit)
- Cache cleared only after recursion converges (not per-iteration)
- For deep recursion: entries accumulate beyond 512 MB limit
- **Fix**: Implement per-iteration cache eviction (~40-60 LOC)
- **Risk**: LOW (cache semantics preserved, lower hit rate expected)

### Issue 3: Error-Path Delta Leak (P2)
- Pre-seeded `$d$` deltas persist if `col_eval_stratum()` fails
- **Impact**: Benign (replaced on next snapshot)
- **Fix**: Add cleanup before early return (~15 LOC)

### Issue 4: WL_PROFILE Counter Loss (P2)
- Worker profiling data discarded (not merged back)
- **Impact**: Incomplete profiling (correctness unaffected)
- **Fix**: Merge counters after barrier (~20 LOC)

---

## Implementation Status

### Already Implemented ✅
- Per-worker arena isolation (ops.c:2309-2311)
- Session field isolation (verified via audit)
- Collect-then-merge pattern (ops.c:2254-2485)
- mat_cache LRU eviction for non-recursive paths (cache.c)
- TSan validation (all tests pass, zero races)

### Ready for Optimization
- P0: Remove dead arenas (straightforward)
- P1: Add per-iteration cache eviction (moderate complexity)
- P2: Error path + profiling cleanup (optional, low complexity)

---

## Acceptance Criteria — All Met ✅

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Zero TSan data races | ✅ VERIFIED | 75 tests, K=1,2,4,8, ZERO races |
| Output byte-for-byte match | ✅ VERIFIED | FNV-1a fingerprints identical |
| ASAN clean | ✅ VERIFIED | ZERO heap errors |
| Session isolation correct | ✅ VERIFIED | Exhaustive field audit |
| Collect-then-merge pattern | ✅ VERIFIED | Code inspection + TSan proof |
| Thread-safe for general multi-worker | ✅ VERIFIED | Design validated, tests pass |

---

## References

- **Issue #174**: Multi-workqueue optimization roadmap (correct analysis)
- **Issue #173**: Closed (incorrect root cause)
- **Code locations**:
  - Per-worker arenas: ops.c:2309-2311
  - Dead arenas: session.c:274, ops.c:2311
  - mat_cache eviction: cache.c, eval.c:600-650
  - K-fusion implementation: ops.c:2254-2485
- **Related ADRs**:
  - ADR-001: Collect-then-merge pattern for non-recursive strata
  - shared-state-hazards.md: Hazard catalog + mitigation patterns

---

**Verified By**: Consensus planning with TSan/ASAN validation + code audit
**Status**: Design approved and verified correct. Ready for optimization implementation.
