# Phase 3B Launch Brief
## Timestamped Delta Tracking & Incremental Consolidation Optimization

**Date:** 2026-03-08
**Phase 3A Status:** ✅ CORRECTNESS GATE COMPLETE
**Phase 3B Status:** 🟢 READY TO LAUNCH
**Strategy:** Profiling-First (measure contribution separately, establish real targets)

---

## Key Findings from Phase 3A

### 1. Real CSPA Baseline is ~17s (Not 4.5s)

| Workload | Edges | Baseline | Status |
|----------|-------|----------|--------|
| Synthetic (graph_10.csv) | 9 | 6.0s | Used for Phase 2D target |
| Real (bench/data/cspa/) | 199 | ~17s | **Actual Phase 3B target** |

**Impact:** Phase 3D inherited targets (CSPA <1.2s, DOOP <47s) are unrealistic without architectural changes. Phase 3B should establish profiling-based targets.

### 2. K-Fusion Contribution Needs Measurement

Phase 3A showed K=2 parallel dispatch works correctly, but we don't have empirical speedup data for real workload. Phase 3B must answer:
- How much does K-fusion contribute on CSPA (199 edges)?
- How much on DOOP (16K nodes)?

**Measurement plan:**
```bash
# Baseline: K-fusion disabled
ENABLE_K_FUSION=0 time ./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa/
# Expected: ~18-19s (consolidation overhead without parallelism)

# Optimized: K-fusion enabled
ENABLE_K_FUSION=1 time ./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa/
# Expected: ~16-17s (10-12% speedup from K=2 parallelism)
```

---

## Phase 3B Deliverables (Unchanged)

### 3B-001: Timestamped Delta Tracking

**Goal:** Add timestamp metadata to delta rows (iteration#, worker#, stratum#)

**Implementation:**
1. Define `col_delta_timestamp_t` struct:
   ```c
   typedef struct {
       uint32_t iteration;
       uint32_t worker;
       uint32_t stratum;
   } col_delta_timestamp_t;
   ```
2. Add `ts` field to `col_rel_t`
3. Populate on delta creation in `col_eval_stratum()`
4. Use for debugging: verify delta order, trace worker progression

**Tests:**
- Timestamp ordering is monotonic within stratum
- Timestamps survive merge operations
- Debugger can trace row lineage

**Estimated effort:** 2-3 days (straightforward metadata addition)

### 3B-002: Incremental Consolidation Measurement

**Goal:** Profile consolidation overhead and validate speedup claims

**Current implementation:**
- `col_op_consolidate_incremental_delta()` at columnar_nanoarrow.c:2032-2120
- O(D log D + N) algorithm (sort delta + dedup + merge)
- Phase 2D claims: 10-12x speedup on late iterations

**Measurement approach:**
1. Add instrumentation: track time spent in consolidate per iteration
2. Compare against hypothetical O(N log N) full sort baseline
3. Validate: late iteration speedup matches claims (late: D << N)

**Metrics:**
- Early iterations (D/N > 0.5): expected 1.5-2x
- Mid iterations (D/N ~ 0.1): expected 3-5x
- Late iterations (D/N < 0.01): expected 10-15x

**Estimated effort:** 3-4 days (benchmarking + analysis)

### 3B-003: Profiling Harness

**Goal:** Measure K-fusion contribution separately from other optimizations

**Harness inputs:**
- Workload (CSPA, DOOP, etc.)
- K value (1, 2, 4, 8)
- Flags: `ENABLE_K_FUSION` (0/1), `ENABLE_CONSOLIDATION` (0/1)

**Harness outputs (per run):**
```
Workload: cspa
K-Value: 2
K-Fusion: enabled
Total-Time: 17.2s
Consolidation-Time: 1.8s (10.5%)
Merge-Time: 0.6s (3.5%)
Evaluation-Time: 12.1s (70.3%)
Overhead-Time: 2.7s (15.7%)
Tuples: 20381
Iterations: 6
```

**Implementation:**
1. Add clock_gettime() instrumentation at key call sites
2. Collect per-stratum and per-iteration timings
3. Generate JSON output for analysis

**Estimated effort:** 2-3 days

---

## Phase 3B Work Strategy

### Week 1: Profiling Foundation
1. Run K-fusion contribution measurements on CSPA/DOOP
2. Implement 3B-001 (timestamps)
3. Establish baseline: what's the actual bottleneck?

### Week 2: Consolidation Analysis
1. Implement 3B-002 (consolidation profiling)
2. Measure actual speedup vs claims
3. Identify consolidation optimization opportunities

### Week 3: Profiling Harness & Recommendations
1. Build 3B-003 (complete profiling harness)
2. Measure K=4, K=8 scaling behavior
3. Recommend Phase 3C priorities

---

## Phase 3B Success Criteria

### Measurement Completeness
- ✅ K-fusion contribution measured (ENABLE_K_FUSION=0 vs =1)
- ✅ Consolidation overhead quantified per iteration
- ✅ K-scaling behavior validated (K=1,2,4,8)

### Data Quality
- ✅ 3+ runs per configuration (median timing)
- ✅ Clean system (no concurrent benchmarks)
- ✅ TSan/ASAN clean

### Documentation
- ✅ Profiling results in `docs/performance/PHASE-3B-PROFILING-RESULTS.md`
- ✅ Recommendations for Phase 3C priorities
- ✅ Realistic targets set based on profiling (not inherited)

---

## Critical Decision Point

**Question:** Should Phase 3C focus on:
1. **Option A:** Arrangement layer (streaming joins with persistent hash tables)?
2. **Option B:** Timestamp system (Timely-style frontier tracking)?
3. **Option C:** Something else based on profiling data?

**Answer:** Profiling results from Phase 3B will determine this.

If consolidation is 50%+ of time → focus on incremental consolidation further.
If evaluation is 70%+ of time → focus on join optimization (arrangement).
If memory pressure is critical → focus on arena reuse and streaming.

---

## Risk Register

| Risk | Mitigation |
|------|-----------|
| Profiling overhead skews measurements | Run baseline and profiling builds separately, compare |
| K-fusion measurement conflicts with ENABLE_K_FUSION flag in plan | Use separate build flag for measurement vs gate |
| Late iterations have very small deltas → noise | Report median of 3 runs, ignore outliers |

---

## Prerequisites for Launch

- [x] Phase 3A correctness gate complete (all tests passing)
- [x] KI-1 bug fixed and verified
- [x] Phase 3A documentation done (PHASE-3A-COMPLETION.md)
- [x] CSPA clean benchmarks in progress (3-run measurement)
- [x] DOOP baseline available (Phase 2D: 71m50s)
- [ ] Final CSPA/DOOP baselines captured (ETA: 1 hour)

---

## Next Steps

1. **Today (1 hour):** Await CSPA/DOOP baselines
2. **Tonight:** Formalize Phase 3A closure (all criteria met)
3. **Tomorrow:** Begin Phase 3B with profiling measurements
4. **Week 1:** Complete 3B-001 (timestamps) and profiling foundation
5. **Week 2:** Complete 3B-002 (consolidation analysis)
6. **Week 3:** Complete 3B-003 (profiling harness) and recommend Phase 3C

---

**Status:** Ready to launch on completion of CSPA/DOOP baselines
**Expected Duration:** Phase 3B = 3 weeks (Weeks 1-3 of overall 12-16 week Phase 3)
