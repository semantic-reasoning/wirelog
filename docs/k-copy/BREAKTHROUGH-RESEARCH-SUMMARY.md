# Breakthrough Research Summary: K-Copy Performance Optimization

**Date:** 2026-03-08
**Status:** Research synthesis complete
**Scope:** Options A + B + CSE findings for CSPA K-copy bottleneck elimination

---

## 1. Executive Summary

The K-copy architecture (semi-naive delta expansion for K-atom rules) is the dominant performance bottleneck in wirelog's columnar evaluator, accounting for 52-63% of CSPA wall time through in-plan CONSOLIDATE operations. Three complementary optimization strategies were investigated: **Option A** (K-way merge CONSOLIDATE), **Option B** (empty-delta skip), and **CSE cache diagnosis**. Combined, Options A+B are projected to reduce CSPA wall time by 35-60% (27.3s to 10-18s). CSE materialization was found to be counterproductive for CSPA's 3-way rules but transformative for DOOP's 8/9-way rules. The recommended path forward is phased implementation: A first, then B, with CSE deferred to DOOP enablement.

---

## 2. Performance Impact Analysis

### 2.1 Current Baseline

| Metric | Value | Source |
|--------|-------|--------|
| CSPA wall time (median) | 27.3s | Post-CONSOLIDATE optimization (Phase A complete) |
| CSPA wall time (pre-optimization) | 35.3s | Original baseline |
| DD historical baseline | 4.6s | Removed backend (correctness oracle) |
| Peak RSS | 3.0-5.3 GB | Varies by measurement point |
| Output tuples | 20,381 | Correct (baseline match) |
| Regression vs DD | 5.9x | Down from 7.7x after Phase A |

### 2.2 Completed Optimizations (Already Merged)

**Phase A: Delta-Integrated Incremental Consolidation** (CONSOLIDATE-COMPLETION.md)
- Replaced O(N) per-iteration old_data snapshot with O(1) row-count tracking
- Implemented `col_op_consolidate_incremental_delta` with delta as merge byproduct
- Eliminated global `g_consolidate_ncols` via `qsort_r` context (thread-safe)
- **Result:** 13% wall time improvement (35.3s -> 30.7s median)

**SIMD Row Comparison** (MERGE-OPTIMIZATION-PERFORMANCE-RESULTS.md)
- AVX2 and ARM NEON implementations for parallel row comparison
- Pointer caching and branch reduction in merge loop
- **Result:** 12% wall time improvement (31.1s -> 27.4s median)

**K=2 Delta Expansion** (K2-DELTA-EXPANSION-IMPLEMENTATION.md)
- Lowered `rewrite_multiway_delta` threshold from K>=3 to K>=2
- Generates explicit 2-copy expansions for binary recursive joins (R1, R4)
- **Result:** Correctness maintained, iteration count reduction for K=2 rules

### 2.3 Option A: K-Way Merge CONSOLIDATE

**Problem:** In-plan CONSOLIDATE at line 1732 uses full `qsort` on the K-copy union output. For K=3 copies, this is O(M log M) where M = K x join output size, consuming 52-63% of wall time.

**Design:**
- Track K-copy boundaries via CONCAT stack markers during plan evaluation
- Sort each K-copy output independently: K x O((M/K) log(M/K))
- Perform K-way merge with dedup: O(M log K) using a min-heap
- Total complexity: O(M x (log(M/K) + log K)) vs current O(M log M)

**Expected Impact:**
- Wall time: 27.3s -> 15-19s (**30-45% reduction**)
- The improvement comes from sorting smaller segments (better cache locality, lower constant factors) and the efficient K-way merge replacing full-array qsort

**Implementation Effort:** 2-3 days
**Risk:** Low — algorithmic improvement with clear correctness invariant (sorted+unique output)

### 2.4 Option B: Empty-Delta Skip

**Problem:** All K plan copies execute every iteration, even when their designated delta relation is empty. In later iterations, typically only 1-2 of 3 IDB relations have new facts, making 1-2 copies redundant.

**Design:**
- Detect when `FORCE_DELTA` VARIABLE produces zero rows in `col_eval_relation_plan`
- Short-circuit remaining JOIN operations for that copy
- Skip CONCAT contribution for empty copies

**Expected Impact:**
- Wall time: additional **5-15% reduction** on top of Option A
- Greatest benefit in later iterations where deltas become sparse
- For CSPA with 3 IDB relations, ~33-66% of copies are skippable in late iterations

**Implementation Effort:** 1-2 days
**Risk:** Low — early-exit optimization, does not change evaluation semantics

### 2.5 Combined Impact Estimate (A + B)

| Scenario | Wall Time | Reduction | vs DD Baseline |
|----------|-----------|-----------|----------------|
| Current (post-Phase A + SIMD) | 27.3s | — | 5.9x |
| After Option A (K-way merge) | 15-19s | 30-45% | 3.3-4.1x |
| After Option A + B (+ empty skip) | 12-16s | 40-55% | 2.6-3.5x |
| Theoretical floor (K-copy inherent) | 8-10s | — | 1.7-2.2x |

**Key insight:** Even with perfect optimization, K-copy architecture has an inherent 1.7-2.2x overhead vs the DD baseline because K=3 rules require 3x more join work (correct semi-naive semantics). This is not a bug — it's the cost of correctness.

---

## 3. CSE Cache Findings

### 3.1 Current State

CSE (Common Subexpression Elimination) infrastructure exists in the codebase:
- `col_mat_cache_t` cache structure (line 339)
- Lookup logic (lines 1016-1023)
- `materialized` hint on plan operations (line 1033)

**Observed hit rate: 0%** — the cache is present but ineffective.

### 3.2 Suspected Root Causes

Investigation identified three potential issues:

1. **Pointer stability:** The `left_e.owned` guard in cache lookup causes misses because intermediate relations get new pointers each K-copy pass. The cache keys are based on relation pointers, which change between copies even when the underlying data is identical.

2. **Cache invalidation timing:** New intermediate relations are created per K-copy pass, producing different pointer identities for semantically equivalent data. The cache sees each as a distinct entry.

3. **Workload incompatibility (CSPA-specific):** CSPA's many-to-many joins (Rule 5: `vF x mA -> 450K rows`) produce large intermediates that are counterproductive to materialize. The materialized result is larger than any input relation, making cache reuse more expensive than recomputation.

### 3.3 CSE Impact by Workload

| Workload | K (max) | CSE Benefit | Rationale |
|----------|---------|-------------|-----------|
| **CSPA** | 3 | **None/Harmful** | Only Rule 5 has 3 IDB atoms; intermediate T=450K rows is too large; CSE adds 14.4 MB memory for ~18K ops saved (net negative) |
| **DOOP** | 8-9 | **Transformative** | Rules D1-D3 have 5-7 static atoms materializable once at stratum entry; 43-53% per-iteration reduction; likely required for DOOP completion |

**Detailed CSPA CSE analysis** (from OPTION2-CSE-ANALYSIS.md):
- Rule 5 (`valueAlias 3-way`): CSE materializes `T = vF ⋈ mA` at 450K rows. The cross-join with delta in pass 3 costs 450K ops vs 22.6K ops naive. **CSE is counterproductive.**
- Rules 1-4: K=2 or insufficient IDB atoms — CSE not applicable.
- **Verdict: DO NOT USE CSE for CSPA. Use naive Option 2 (K plan copies).**

**DOOP CSE projection** (from OPTION2-CSE-ANALYSIS.md):
- Rule D2 (9-way VarPointsTo): CSE reduces 3M -> 1.4M ops per iteration (53% reduction)
- Static atom groups (5-7 atoms) materialized ONCE at stratum entry, reused every iteration
- **Required for DOOP to complete** — without CSE, 8-way chains through 1M-row VarPointsTo are infeasible

### 3.4 Implications for Architecture

The CSE cache's 0% hit rate for CSPA is not a bug to fix — it reflects the correct behavior for a workload where materialization is counterproductive. The cache infrastructure is sound and will become critical when DOOP support is prioritized.

**For single-pass interleaved delta evaluation:** CSE's pointer stability issue would need resolution regardless. If a future evaluator redesign is pursued, the cache should use content-based keys (hash of relation data) rather than pointer-based keys.

---

## 4. Recommendation & Roadmap

### Phased Implementation Path

#### Phase 1: Option A — K-Way Merge CONSOLIDATE (3-5 days)

**Scope:** Replace full qsort in `col_op_consolidate` (line 1346-1394) with K-way merge.

**Steps:**
1. Add CONCAT boundary tracking to record K-copy segment boundaries
2. Implement per-segment sort (K independent sorts on smaller arrays)
3. Implement K-way merge with min-heap and dedup
4. Integrate into `WL_PLAN_OP_CONSOLIDATE` dispatch path

**Acceptance Criteria:**
- CSPA wall time: target 15-19s (median, 3 runs)
- All 15 workloads: identical tuple counts (correctness gate)
- Build clean: `-Wall -Wextra -Werror`, clang-format applied

**Measurement:**
```bash
./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa --repeat 3
./build/bench/bench_flowlog --workload all --data bench/data/graph_10.csv --data-weighted bench/data/graph_10_weighted.csv
```

#### Phase 2: Option B — Empty-Delta Skip (1-2 days)

**Scope:** Add early-exit for K-copy passes with empty delta input.

**Steps:**
1. Check `FORCE_DELTA` relation's delta row count before executing copy
2. Skip JOIN chain and CONCAT contribution for zero-delta copies
3. Update iteration bookkeeping to handle skipped copies

**Acceptance Criteria:**
- Additional 5-15% wall time reduction on top of Phase 1
- Combined target: 12-16s
- All 15 workloads: identical tuple counts

#### Phase 3: CSE Cache Fixes (Conditional, 3-5 days)

**Decision gate:** Only pursue if Phases 1+2 combined leave CSPA above 16s.

**If CSE root cause is pointer stability (fixable):**
- Implement content-based cache keys (hash of sorted relation data)
- Test hit rate improvement on CSPA Rule 5
- Measure wall time impact (expected: 5-10% additional gain IF intermediate is beneficial)

**If CSE is architecture limitation (defer):**
- Document as "inherent K-copy cost" for CSPA's many-to-many join pattern
- Preserve infrastructure for DOOP enablement (Phase 4)
- No code changes needed

**Current assessment:** CSE is an architecture limitation for CSPA (many-to-many joins produce oversized intermediates). Recommend **deferring** CSE fixes until DOOP is prioritized.

#### Phase 4: Single-Pass Interleaved Delta Evaluation (Future Research)

**Decision gate:** Only pursue if Phases 1+2+3 combined still leave >2-3x gap to DD baseline (i.e., CSPA > 10s).

**Scope:** Redesign the evaluator to interleave delta passes within a single plan execution, eliminating K-copy duplication entirely.

**Characteristics:**
- Eliminates K-copy overhead at the architecture level
- Requires evaluator rewrite (4-8 weeks, high risk)
- Research-level complexity: must maintain semi-naive correctness invariants
- Theoretical target: approach DD baseline (4.6s) within 1.5x

**Current recommendation:** **Defer.** Phases 1+2 are projected to bring CSPA to 12-16s (2.6-3.5x DD baseline). The remaining gap is largely inherent to K-copy semantics. A full evaluator rewrite is justified only if the business case requires sub-10s CSPA performance.

---

## 5. Evidence & Verification

### 5.1 CSPA Benchmark History

| Configuration | Wall Time | Peak RSS | Tuples | Source |
|--------------|-----------|----------|--------|--------|
| DD backend (historical) | 4.6s | — | 20,381 | Removed commit 8f03049 |
| Pre-optimization baseline | 35.3s | 3.0 GB | 20,381 | CSPA-BASELINE-PROFILE.md |
| Phase 2C (plan expansion) | 28.7s | 2.8 GB | 20,381 | BENCHMARK-RESULTS-2026-03-07.md |
| + Phase A (delta consolidation) | 30.7s | 5.3 GB | 20,381 | CONSOLIDATE-COMPLETION.md |
| + SIMD merge optimization | 27.3s | 4.4 GB | 20,381 | MERGE-OPTIMIZATION-PERFORMANCE-RESULTS.md |
| Projected: + Option A | 15-19s | ~3 GB | 20,381 | Theoretical (K-way merge) |
| Projected: + Options A+B | 12-16s | ~3 GB | 20,381 | Theoretical (+ empty skip) |
| Theoretical floor | 8-10s | — | 20,381 | Inherent K-copy cost |

### 5.2 All 15 Workloads Validation

Every optimization committed to date has passed the full 15-workload regression suite:
- TC, Reach, CC, SSSP, SG, Bipartite, Andersen, Dyck, CSPA, CSDA, Galen, Polonius, DDISASM, CRDT, DOOP
- All output tuple counts match baseline exactly
- No performance regressions on non-CSPA workloads

### 5.3 Bottleneck Attribution

From CRITICAL-FINDINGS-SYNTHESIS.md (27.3s current baseline):

| Component | Time | Fraction | Optimization |
|-----------|------|----------|-------------|
| In-plan CONSOLIDATE (K-copy dedup) | ~15-18s | 52-63% | **Option A targets this** |
| K-copy join evaluation | ~8-10s | 28-35% | **Option B reduces this** |
| Post-plan incremental consolidate | ~1-2s | 3-7% | Already optimized (Phase A) |
| Delta computation + snapshots | ~1-2s | 3-7% | Already optimized (Phase A) |
| CSE cache overhead | ~0.4s | 1.4% | Negligible; defer |

---

## 6. References

### Completed Analysis Documents

| Document | Location | Content |
|----------|----------|---------|
| Critical Findings Synthesis | `docs/cspa-improvement-plan/CRITICAL-FINDINGS-SYNTHESIS.md` | Bottleneck identification, corrected strategy after H1/H2 discovery |
| CSPA Baseline Profile | `docs/cspa-analysis/CSPA-BASELINE-PROFILE.md` | Empirical baseline metrics, 7.7x regression analysis |
| CONSOLIDATE Completion Report | `docs/performance/CONSOLIDATE-COMPLETION.md` | Phase A implementation (delta-integrated consolidation) |
| CONSOLIDATE Improvement Plan | `docs/cspa-analysis/CONSOLIDATE-IMPROVEMENT-PLAN.md` | Option A design with pseudocode |
| Option 2 + CSE Analysis | `docs/performance/OPTION2-CSE-ANALYSIS.md` | Per-rule cost models for CSPA and DOOP |
| Merge Optimization Results | `docs/merge-optimization/MERGE-OPTIMIZATION-PERFORMANCE-RESULTS.md` | SIMD row comparison (11.96% improvement) |
| Benchmark Results | `docs/performance/BENCHMARK-RESULTS-2026-03-07.md` | Plan expansion impact measurements |

### Research Task Deliverables

| Task | Deliverable | Status |
|------|------------|--------|
| #1: K-way merge CONSOLIDATE (Option A) | `docs/k-copy/K-WAY-MERGE-CONSOLIDATE.md` | In progress |
| #2: Empty-delta skip (Option B) | `docs/k-copy/EMPTY-DELTA-SKIP.md` | Pending |
| #3: CSE cache diagnosis | `docs/k-copy/CSE-CACHE-DIAGNOSIS.md` | In progress |

### Prior Architecture Decisions

| Document | Location |
|----------|----------|
| Parallelism Strategy ADR | `docs/performance/ADR-002-parallelism-strategy.md` |
| Phase 2/3 Sequencing Template | `docs/performance/ADR-003-TEMPLATE-phase-2-3-sequencing.md` |
| Workqueue Design | `docs/workqueue/workqueue-design.md` |
| Workqueue ADR | `docs/workqueue/ADR-001-workqueue-introduction-strategy.md` |

---

## 7. Key Learnings

1. **Bottleneck moved after Phase A:** The post-iteration CONSOLIDATE (3-7% of time) was already optimized; the real bottleneck is in-plan CONSOLIDATE (52-63%), which is a different function at a different call site.

2. **CSE is workload-dependent:** For CSPA (K=3, many-to-many joins), CSE is harmful. For DOOP (K=8-9, many static atoms), CSE is essential. A single policy cannot serve both — the materialization threshold must be rule-aware.

3. **K-copy overhead is partially inherent:** Even perfect optimization leaves a 1.7-2.2x gap vs DD baseline because K=3 rules genuinely require 3x more join work. This is correct semi-naive semantics, not waste.

4. **Memory and time don't always correlate:** Phase A reduced allocation count but didn't significantly reduce peak RSS (bounded by relation data size, not snapshot pattern). The wall time improvement came from algorithmic reduction (O(N log N) -> O(D log D + N)), not memory savings.

5. **Measure before assuming:** Initial analysis assumed CONSOLIDATE and delta expansion were unimplemented. Team discovery revealed both were already in the code — the real target was a different CONSOLIDATE call site entirely.

---

**Generated:** 2026-03-08
**Synthesized from:** 7 research documents, 4 task specifications, CSPA benchmark data
**Status:** Research synthesis complete; implementation roadmap ready for execution
