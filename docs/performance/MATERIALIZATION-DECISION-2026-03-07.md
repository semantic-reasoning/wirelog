# Materialization Decision - Keep US-006 Evaluator Integration

**Date:** 2026-03-07
**Decision:** **KEEP** CSE Materialization (US-006) in codebase
**Trade-off:** +1.7% performance cost vs -11% memory efficiency gain + infrastructure preservation
**Status:** FINAL

---

## Decision Summary

**Chosen:** Keep `col_mat_cache_t` and evaluator integration despite performance regression.

### Rationale

1. **Memory Efficiency Gain (-11%)**
   - Peak RSS: 3.5GB → 3.1GB (saves 400MB)
   - Important for embedded/resource-constrained deployment scenarios

2. **Infrastructure Preservation**
   - CSE cache is foundation for future parallelization (Phase B-lite)
   - Materialization hints already annotated in plan generation
   - Removing now would require re-implementing later

3. **Performance Cost is Minimal**
   - +1.7% overhead (28.3s → 28.7s)
   - Acceptable for feature trade-off
   - Small enough that future optimizations could recover

4. **Correctness is Perfect**
   - CSPA produces identical results (20,381 tuples)
   - All 13 tests pass
   - Zero correctness regression

---

## Measurement Data

### CSPA Performance Before/After Materialization

```
                    Without US-006    With US-006    Delta
─────────────────────────────────────────────────────────
Wall Time           28.3s             28.7s         +0.4s (+1.7%)
Peak RSS            3.5 GB            3.1 GB        -400MB (-11%)
Tuples              20,381            20,381        ✓ (identical)
Correctness         PASS              PASS          ✓ (identical)
```

### What US-006 Added
- `col_mat_cache_t`: LRU cache (64 entries, 100MB limit)
- Cache lookup/insert in `col_op_join`
- Materialization hints in plan expansion (first K-2 JOINs)
- Delta mode enum (FORCE_DELTA, FORCE_FULL, AUTO)

### Why Cache Isn't Delivering Speedup
- Hit rate too low for CSPA workload (3-way joins too diverse)
- OR: Misses + cache management overhead dominate
- Net result: cache overhead > cache benefit

---

## Impact on Future Work

### Phase 3 (Parallelization)
✅ **Enabled**: Cache can be shared across worker threads
✅ **Infrastructure ready**: Materialization hints already in plan
✅ **Expected benefit**: Cache hit rate improves with static scheduling

### DOOP Optimization
⚠️ **No immediate help**: Still DNF (does not finish)
✅ **Prerequisite for future work**: Static group materialization will build on this

---

## Open Questions / Future Work

1. **Cache hit rate analysis** (Phase 1 of profiling)
   - How often are cached joins reused?
   - Do cache misses dominate?

2. **Adaptive cache sizing** (Future optimization)
   - Should cache size adapt per workload?
   - Hash-based caching instead of LRU?

3. **Performance profiling** (Phase 1, Measure)
   - Where is the 6.9× slowdown coming from?
   - Is it qsort, evaluator loop, or something else?

---

## Commits Related to This Decision

| Commit | Description | Status |
|--------|-------------|--------|
| e68995f | Final benchmark validation with US-006 | ✅ In tree |
| 0288254 | CSE materialization infrastructure | ✅ In tree |
| 275d8c6 | Plan rewriting with materialization hints | ✅ In tree |
| e7c181e | Delta mode enum integration | ✅ In tree |
| 89f21b3 | Performance investigation report | ✅ In tree |

---

## Decision Approval

**User Decision:** Keep Materialization
**Approved:** 2026-03-07
**Rationale:** Accept 1.7% performance cost for infrastructure + memory savings
**Next Phase:** Profile to identify true bottleneck (6.9× regression)

---

**Generated:** 2026-03-07
**Status:** FINAL - Implementation in place, ready for Phase 3 (Parallelization)
