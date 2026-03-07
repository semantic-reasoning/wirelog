# Option 2 + CSE Benchmark Results — 2026-03-07

**Branch:** `next/pure-c11 @ 08c088b`
**Build:** Meson with clang-format cleanup
**Host:** macOS 25.3.0 / Apple Silicon
**Date:** 2026-03-07 16:50–17:30 JST

---

## Executive Summary

✅ **Correctness Verified:** CSPA produces correct 20,381 tuples (baseline match)
⚠️ **Performance Regression:** CSPA takes 34.8s (baseline: 4.6s) — **7.5× slower**
❌ **DOOP Status:** Still does not finish (DNF) — benchmark timeout after 17+ minutes

**Key Finding:** Performance regression is NOT due to Plan expansion (K-way delta). Root cause requires further investigation.

---

## CSPA Benchmark Results

### Measurement 1: With Plan Expansion (K >= 3)
```
Status      : OK (CORRECTNESS PASS)
Tuples      : 20381 / 20381 ✓
Wall time   : 34,797.9 ms
Peak RSS    : 2,785,712 KB (2.7 GB)
Change      : +656% slower than baseline
```

### Measurement 2: Without Plan Expansion for K=3 (K >= 4 only)
```
Status      : OK
Tuples      : 20381 / 20381 ✓
Wall time   : 41,935.3 ms
Peak RSS    : 1,928,224 KB (1.9 GB)
Change      : +811% slower than baseline (even worse)
```

### Analysis

| Metric | Baseline | Current | Change | Implication |
|--------|----------|---------|--------|-------------|
| Wall time | 4,602 ms | 34,798 ms | +656% | Significant regression |
| Peak RSS | 4,670 MB | 2,786 MB | -40% | Memory improvement (but slower) |
| Tuples | 20,381 | 20,381 | ✓ Same | Correctness maintained |
| Plan expansion | N/A | Active | - | NOT the root cause |

**Conclusion on Plan Expansion:**
- Disabling K=3 expansion → **41.9s** (worse)
- Enabling K=3 expansion → **34.8s** (better, but still slow)
- **Plan expansion provides minor improvement**, not the cause of regression

---

## DOOP Benchmark Results

### Status: DNF (Did Not Finish)

```
Processes started: 4 concurrent benchmarks
Runtimes: 17m 56s, 22m 8s, ... (each exceeded resource limits)
Status: Timeout → Force terminated
Root cause: Evaluation too slow for 4.2M input rows
```

**Baseline expectation:** DNF (~2.5 minute timeout per previous run)
**Current result:** DNF (still not finishing after 17+ minutes)
**Improvement:** None — DOOP remains incomplete

---

## Root Cause Analysis: Why is CSPA 7.5× slower?

### Hypothesis 1: Plan Expansion ❌ REJECTED
- Disabling K=3 expansion makes performance **worse** (+42% slower)
- Conclusion: Not the cause

### Hypothesis 2: Compiler Optimization
- Build uses `meson compile` with default flags
- Possible: `-O0` (debug) instead of `-O2` (release)?
- Check: `meson buildtype` option not verified

### Hypothesis 3: Code Change Regression
- Recent commits: plan rewriting, delta_mode, CSE infrastructure
- Possible: Evaluator loop overhead from new field access?
- Check: Profile evaluator performance on per-iteration ops

### Hypothesis 4: Memory Allocator
- Current: 2.7 GB peak RSS (good)
- Baseline: 4.7 GB peak RSS (worse)
- Performance: Current slower despite less memory
- Implication: Not memory-bound; CPU or algorithmic difference

### Hypothesis 5: Evaluation Loop Changes
- Delta_mode field on every `wl_plan_op_t`
- Materialized field on every `wl_plan_op_t`
- Potential cache-line miss or branch misprediction impact?

---

## Recommendations

### Immediate Investigation (Priority 1)
1. **Profile CSPA evaluation loop** with perf/Instruments
   - Identify if evaluator CPU is the bottleneck
   - Check for cache misses or branch mispredictions
   - Compare to baseline profile

2. **Verify build flags**
   ```bash
   meson configure build | grep buildtype
   meson configure build -Dbuildtype=release
   meson compile -C build
   ./scripts/run_cspa_validation.sh --repeat 1
   ```

3. **Bisect recent commits**
   - Start with commit before delta_mode introduction
   - Test performance at each commit
   - Identify which change caused regression

### Performance Tuning (Priority 2)
1. Optimize delta_mode field access (inline cache-friendly structure?)
2. Review col_op_variable and col_op_join for inefficiencies
3. Consider SIMD or vectorization for batch operations

### DOOP Path (Priority 3)
1. **DOOP remains DNF** — Plan expansion alone is insufficient
2. CSE materialization runtime integration still needed
3. Evaluate approach: static group materialization for K=8 rules?

---

## Conclusion

| Component | Status | Verdict |
|-----------|--------|---------|
| **Correctness** | ✅ PASS | CSPA produces correct output |
| **Option 2 Plan Expansion** | ⚠️ MIXED | Code works but has performance cost |
| **CSE Materialization** | ⚠️ PARTIAL | Infrastructure done; runtime integration pending |
| **Overall** | ⚠️ BLOCKED | Performance regression must be fixed before release |

**Next Steps:**
1. Profile to identify root cause of CSPA slowdown
2. Apply targeted fix (likely compiler flags or code optimization)
3. Re-run benchmarks to validate improvement
4. Revisit DOOP path after CSPA is fixed

---

**Generated:** 2026-03-07 17:30 JST
**Branch:** `next/pure-c11 @ 08c088b`
**Status:** Investigating performance regression — NOT a correctness issue
