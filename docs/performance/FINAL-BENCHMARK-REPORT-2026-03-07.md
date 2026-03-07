# Option 2 + CSE Final Benchmark Report

**Date:** 2026-03-07 17:35–18:00 JST
**Branch:** `next/pure-c11 @ e35be4b`
**Status:** Final validation with US-006 (Evaluator Integration) completed

---

## Executive Summary

✅ **Correctness**: CSPA verified (20,381 tuples = baseline)
⚠️ **Performance**: 6× slower than baseline (28.7s vs 4.6s)
🔄 **Evaluator Integration**: US-006 adds <2% overhead (minimal impact)
⏳ **DOOP Status**: Re-running with evaluator integration active

---

## CSPA Benchmark Comparison

### Previous Run (Without US-006 Evaluator Integration)
- Build: Release (-O2)
- Time: 28.3 seconds
- Memory: 3.5 GB
- Status: ✅ Correct (20,381 tuples)

### Current Run (With US-006 Evaluator Integration)
- Build: Release (-O2)
- Time: 28.7 seconds
- Memory: 3.1 GB
- Status: ✅ Correct (20,381 tuples)

### Delta
```
Δ Time: +489ms (+1.7%)
Δ Memory: -400 MB (-11%)
Evaluator overhead: <2%
```

**Finding:** Evaluator integration (CSE cache, lookups, inserts) adds <2% overhead. This is **acceptable** for production.

---

## What US-006 (Evaluator Integration) Added

1. **col_mat_cache_t** (LRU, 64 entries, 100MB limit)
   - Materialized join caching
   - Per-session instance

2. **Integrated into col_op_join**
   - Cache lookup before computation
   - Cache insert after computation (when `op->materialized`)

3. **Fixed evaluator behaviors**
   - col_op_concat boundary markers
   - col_op_variable FORCE_DELTA fallback (full relation, not empty)
   - col_op_join FORCE_DELTA fallback

4. **Test Results**
   - All 13 core tests pass
   - option2_cse: 10/10 tests pass
   - Expected overhead: <2% ✓ Confirmed

---

## DOOP Re-Run Status

⏳ **In Progress** (5-minute execution window)

Expected outcomes:
- ✅ **PASS:** DOOP completes < 5 minutes (CSE transformation successful)
- ⏱️ **TIMEOUT:** DOOP > 5 minutes (CSE helps but still slow)
- ❌ **DNF:** DOOP does not finish (CSE insufficient for 8-way joins)

---

## Root Cause of CSPA 6× Slowdown

**Status:** Partially identified

### Ruled Out ✗
1. ❌ Plan expansion (K-way delta) — disabling made it worse (-42%)
2. ❌ Evaluator integration (US-006) — adds <2% overhead only
3. ❌ Compiler flags — Release builds are optimized

### Still Under Investigation 🔍
1. 📊 **Possible:** O(N log N) consolidate sort dominates
2. 📊 **Possible:** Memory allocation patterns
3. 📊 **Possible:** Cache locality with new fields (delta_mode, materialized)
4. 📊 **Possible:** Baseline measurement environment (different system/flags)

**Recommendation:**
Profile with Instruments/perf to identify CPU/cache/memory bottleneck.

---

## Performance Optimization Options

### Short Term (1-2 days)
- [ ] Profile CSPA with perf/Instruments
- [ ] Identify top 3 bottleneck functions
- [ ] Compare to baseline profiler output

### Medium Term (3-5 days)
- [ ] Optimize identified bottleneck (e.g., sort algorithm, memory allocation)
- [ ] Re-run benchmarks to validate improvement
- [ ] Target: <10s CSPA (50% improvement)

### Long Term (1-2 weeks)
- [ ] If DOOP passes with CSE: ship current implementation
- [ ] If DOOP fails: redesign static group materialization for 8-way joins

---

## Conclusion

| Component | Status | Verdict |
|-----------|--------|---------|
| **Option 2 Implementation** | ✅ | Correct, tests pass |
| **Plan Rewriting (K-way)** | ✅ | K=3 produces 3 copies correctly |
| **CSE Materialization** | ✅ | Infrastructure + evaluator integration complete |
| **Evaluator Integration** | ✅ | <2% overhead (acceptable) |
| **Correctness** | ✅ | CSPA verif ied, 20,381 tuples correct |
| **Performance** | ⚠️ | 6× slower (root cause TBD) |
| **DOOP Completion** | ⏳ | Re-running (results pending) |

**Overall:** Option 2 + CSE is **architecturally sound** and **functionally correct**. Performance regression requires profiling to identify root cause. Evaluator integration is efficient (<2% overhead).

---

**Next Milestone:**
- [ ] DOOP benchmark result (target: <5 min completion)
- [ ] If successful: prepare for production
- [ ] If timeout: plan DOOP-specific optimizations

**Status:** Awaiting DOOP results...
