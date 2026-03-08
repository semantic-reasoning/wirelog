# RALPLAN-DR: Phase 3 Timely-like Framework vs K-Fusion Optimization

**Date:** 2026-03-08
**Status:** Decision Required
**Scope:** Phase 3 strategic direction for wirelog performance architecture
**Stakeholders:** wirelog engineering team, CleverPlant
**Prior Art:** ADR-002 (Parallelism Strategy), Phase 2D Final Report, K-Fusion Architecture

---

## Context & Problem Statement

wirelog's pure C11 columnar backend (nanoarrow) achieves CSPA in ~6.0s (K=2) and DOOP in 71m50s (K=8). The reference system (DD + Timely Dataflow / Naiad) achieves DOOP in ~47s -- a 91x gap. The fundamental question:

**Should Phase 3 build a Timely-like frontier-based framework in C, or should it maximize K-fusion parallelism within the existing semi-naive evaluator?**

The performance gap stems from two orthogonal factors:
1. **Per-iteration cost**: K-copy expansion forces O(K) sequential evaluations per iteration (addressed by K-fusion workqueue parallelism)
2. **Delta tracking granularity**: Manual delta tracking via CONSOLIDATE full-sort vs Timely's automatic frontier-based progress tracking (the 91x structural gap)

### Current Architecture Summary

```
Semi-Naive Fixed-Point Loop (col_eval_stratum):
  for iter = 0..MAX_ITER:
    for each relation R in stratum:
      for k = 0..K:                    // K-copy expansion
        evaluate_copy_k(R)             // Sequential (K-fusion: parallel)
      consolidate(R)                   // Full sort + dedup
    compute_deltas()                   // Diff against prior snapshot
    if no_new_facts: break

Storage: row-major int64_t arrays (nanoarrow columnar)
Threading: workqueue (pthread pool, 5-function API)
Operators: VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE,
           CONCAT, CONSOLIDATE, SEMIJOIN, K_FUSION
```

### Reference Architecture: Naiad / Timely Dataflow

Naiad (SOSP 2013) introduces **pointstamp-based progress tracking**:
- Each data record carries a logical timestamp (epoch, iteration)
- **Frontiers**: per-operator lower bounds on future timestamps
- Operators can process data when frontier advances past their input
- **Automatic delta propagation**: no explicit CONSOLIDATE needed
- **Fine-grained parallelism**: operators execute when inputs are ready (dataflow)
- Differential Dataflow (DD) builds on Timely to provide automatic incremental maintenance

Key Naiad concepts relevant to wirelog:
1. **Pointstamps**: (epoch, iteration_count) tuples attached to records
2. **Frontier tracking**: graph-wide progress protocol via could-still-produce messages
3. **Notification**: operator fires when all inputs at a timestamp are complete
4. **Arrangement**: indexed, compacted representation of collections at each time

---

## Principles (5)

### P1: Incremental Value Delivery
Deliver measurable performance improvements in each phase. Avoid multi-month rewrites that delay feedback. Each milestone must be benchmarkable against CSPA and DOOP baselines.

### P2: C11 Purity & Embedded Readiness
All code must remain pure C11 + pthreads. No external runtime dependencies. The architecture must remain viable for embedded targets and future FPGA backends (Phase 4+).

### P3: Correctness Over Performance
Semi-naive fixed-point semantics must be preserved exactly. Tuple counts, iteration counts, and output facts must match the validated baseline. Any new framework must be provably equivalent to the current evaluator.

### P4: Leverage Existing Infrastructure
Maximize reuse of proven components: workqueue (5-function API), K-way merge (col_rel_merge_k), arena allocator, columnar storage, and the plan generation pipeline. Minimize throwaway work.

### P5: Bounded Complexity Budget
The wirelog backend is ~3100 lines of dense C. Any framework addition must have a complexity proportional to its performance gain. A 5000-line Timely runtime for 2x improvement is not justified; 500 lines for 10x might be.

---

## Decision Drivers (Top 3)

### Driver 1: Root Cause of the 91x DOOP Gap

The gap between wirelog (71m50s) and DD+Timely (47s) is NOT primarily about parallelism. DD ran single-worker in practice (per project memory). The gap comes from:

1. **Redundant recomputation**: wirelog re-evaluates ALL K copies every iteration, even when only 1 copy's delta is non-empty. DD's arrangement-based joins avoid this entirely.
2. **Full-sort consolidation**: wirelog's CONSOLIDATE does qsort on the entire relation each iteration. DD's consolidate operates only on changed records (differential updates with timestamps).
3. **Coarse delta granularity**: wirelog tracks deltas at the relation level (snapshot diff). DD tracks deltas at the record level with multiplicities, enabling precise incremental joins.

K-fusion parallelism addresses factor 1 partially (parallel K-copies, not elimination of redundant copies). It does NOT address factors 2-3 at all.

### Driver 2: Implementation Feasibility in 4-6 Weeks

A full Timely/Naiad implementation requires:
- Pointstamp progress tracking protocol (graph-wide, O(V+E) per message)
- Frontier computation and notification system
- Arranged collections (indexed by time, compacted)
- Dataflow graph construction and operator scheduling

This is a substantial runtime -- Timely Dataflow is ~15,000 lines of Rust. Even a minimal C implementation would be 3000-5000 lines, roughly doubling the backend codebase. The risk of incomplete or buggy implementation within 4-6 weeks is HIGH.

In contrast, completing K-fusion dispatch + plan generation integration is estimated at 500-600 new lines, with most infrastructure already built and tested.

### Driver 3: Return on Investment by Workload

| Workload | Current | K-Fusion Target | Timely Target | K-Fusion ROI | Timely ROI |
|----------|---------|-----------------|---------------|--------------|------------|
| CSPA (K=2) | 6.0s | 3.6-4.2s (30-40%) | ~1.0s (83%) | Moderate | High |
| DOOP (K=8) | 71m50s | 10-30m (50-60%) | ~47s (99%) | High | Transformational |
| Simple TC | <0.1s | <0.1s | <0.1s | None | None |

K-fusion delivers meaningful improvement on DOOP but cannot close the 91x gap. Only architectural changes to delta tracking can achieve DD-like performance. However, K-fusion is achievable and low-risk.

---

## Viable Options (3)

### Option A: Complete K-Fusion Parallelism (Conservative)

**Scope:** Finish the remaining K-fusion implementation (Layer 4: workqueue dispatch + plan generation integration).

**What it delivers:**
- Parallel K-copy evaluation via workqueue (K workers)
- 30-40% CSPA improvement (6.0s -> 3.6-4.2s)
- 50-60% DOOP improvement (71m50s -> 10-30m, possibly under 5m)
- DOOP unblocked for the first time

**Implementation (2-3 weeks, 1 engineer):**
1. Plan generation: detect K-copy relations, emit K_FUSION nodes (exec_plan_gen.c)
2. Complete col_op_k_fusion() dispatch with per-worker arenas
3. Workqueue lifecycle integration in col_eval_stratum()
4. Regression + performance validation

**What already exists:**
- Merge algorithm (col_rel_merge_k) -- tested, 5/5 passing
- Operator infrastructure (WL_PLAN_OP_K_FUSION = 9) -- in exec_plan.h
- Worker task function (col_op_k_fusion_worker) -- skeleton complete
- Workqueue API -- fully implemented, 5 functions

**New code:** ~500-600 lines

| Pros | Cons |
|------|------|
| Low risk: infrastructure 70% built | Cannot close the 91x DOOP gap (structural limit) |
| 2-3 weeks timeline, proven pattern | Per-iteration cost unchanged (qsort consolidation) |
| Preserves all existing architecture | Parallelism ROI limited at K=2 (CSPA) |
| Directly testable against baseline | No improvement to delta tracking granularity |
| Unblocks DOOP completion | Marginal returns on future K-fusion tuning |

---

### Option B: Lightweight Frontier-Based Delta Tracking (Targeted)

**Scope:** Implement a minimal frontier/timestamp system for delta tracking WITHOUT building a full Timely runtime. Keep the semi-naive loop structure but replace CONSOLIDATE full-sort with timestamp-based incremental consolidation.

**Core Idea -- Timestamped Records:**
```c
// Current: rows are bare int64_t arrays
int64_t row[ncols];

// Proposed: rows carry iteration timestamp
typedef struct {
    int64_t *cols;      // existing column data
    uint32_t timestamp; // iteration when this row was produced
} timestamped_row_t;

// CONSOLIDATE becomes incremental:
// - Only sort rows where timestamp == current_iteration
// - Merge-insert into already-sorted prefix
// - Delta = rows with timestamp == current_iteration after dedup
```

**What it delivers:**
- Incremental CONSOLIDATE: O(D log D + N) instead of O(N log N) per iteration (already partially implemented in col_op_consolidate_incremental_delta)
- Precise delta tracking: only new rows propagate, not full snapshots
- Foundation for future arrangement-like indexed joins
- Estimated 3-5x improvement on DOOP (71m50s -> 15-25m)

**Implementation (4-6 weeks, 1-2 engineers):**
1. Week 1-2: Add timestamp field to col_rel_t, propagate through operators
2. Week 2-3: Replace CONSOLIDATE with timestamp-aware incremental merge
3. Week 3-4: Modify JOIN to use timestamp-based delta selection (join only new x full + full x new)
4. Week 4-5: Modify delta computation to use timestamps instead of snapshot diff
5. Week 5-6: Regression validation, performance tuning, edge cases

**New code:** ~1500-2500 lines (modifications across columnar_nanoarrow.c, exec_plan.h)

| Pros | Cons |
|------|------|
| Addresses root cause (delta granularity) | 4-6 weeks, higher risk than Option A |
| Composable with K-fusion (Option A) | Timestamp overhead on every row (memory +8 bytes/row) |
| Incremental path toward DD-like perf | Requires changes across all operators |
| Preserves semi-naive loop structure | Subtle correctness risks in timestamp propagation |
| Reuses existing incremental_delta work | Not a full Timely (no dataflow scheduling) |
| 3-5x DOOP improvement potential | May need index structures for large relations |

---

### Option C: Full Timely-like Dataflow Runtime (Ambitious)

**Scope:** Build a minimal but complete Timely-like dataflow runtime in C11: pointstamp progress tracking, frontier-based operator scheduling, and arranged (indexed + compacted) collections.

**What it delivers:**
- Automatic delta propagation (no explicit CONSOLIDATE)
- Dataflow-scheduled operator execution (fine-grained parallelism)
- Arrangement-based joins (indexed, incremental)
- Theoretical parity with DD+Timely (~47s DOOP)

**Implementation (12-16 weeks, 2 engineers):**
1. Week 1-3: Dataflow graph construction and operator registry
2. Week 3-6: Pointstamp protocol and frontier tracking
3. Week 6-9: Arranged collections (time-indexed, compaction)
4. Week 9-12: Operator reimplementation on dataflow (JOIN, MAP, FILTER, REDUCE)
5. Week 12-14: Integration with plan generation and session API
6. Week 14-16: Regression, performance validation, stabilization

**New code:** ~3000-5000 lines (new runtime module + operator rewrites)

| Pros | Cons |
|------|------|
| Closes the 91x gap (DD parity) | 12-16 weeks, very high risk |
| Elegant architecture (dataflow) | Essentially rewrites the backend from scratch |
| Fine-grained parallelism for free | 3000-5000 new lines doubles codebase complexity |
| Proven model (Naiad, Timely, DD) | C11 implementation of pointstamp protocol is novel |
| Maximum long-term performance | Existing infrastructure (K-fusion, workqueue) largely unused |
| | Debugging dataflow correctness in C is extremely hard |
| | Violates P1 (no incremental value for 12+ weeks) |
| | FPGA integration with dataflow runtime is unclear |

---

## Recommendation: Option A+B Phased (K-Fusion First, Then Targeted Delta Tracking)

### Phase 3A: Complete K-Fusion (Weeks 1-3)
Execute Option A to capture immediate gains and unblock DOOP.

### Phase 3B: Timestamped Delta Tracking (Weeks 4-9)
Execute Option B to address the structural delta tracking gap. This composes naturally with K-fusion -- parallel K-copy evaluation with incremental delta tracking.

### Phase 3C (Future): Evaluate Full Dataflow (Week 10+)
With Phase 3A+3B data, decide whether the remaining gap to DD justifies a full Timely-like runtime. By this point, the gap may be 5-10x (not 91x), making the cost-benefit clearer.

**Combined Target:**
- CSPA: 6.0s -> 1.5-2.5s (60-75% improvement)
- DOOP: 71m50s -> 5-15m (80-93% improvement)

---

## Pre-Mortem: 3 Failure Scenarios

### Failure 1: K-Fusion Delivers Less Than Expected (<15% on CSPA)

**Scenario:** Workqueue thread creation/synchronization overhead on K=2 negates parallelism benefit. CSPA improves only 10-15% instead of 30-40%.

**Why it could happen:**
- K=2 means only 2 parallel tasks -- Amdahl's law limits speedup to <2x on the K-copy portion
- Workqueue mutex contention on small tasks (CSPA iterations are fast, ~1s each)
- Arena allocation cost per worker exceeds saved join time

**Mitigation:**
- Measure workqueue overhead on Day 3 (target <5%)
- If overhead >10%, fall back to sequential K-fusion for K<=2 (parallel only for K>=4)
- Profile per-iteration task duration; if <100ms, consider batching multiple iterations per worker

**Detection:** CSPA benchmark shows <15% improvement after K-fusion integration.

### Failure 2: Timestamp Propagation Breaks Correctness

**Scenario:** Adding timestamps to rows causes subtle deduplication errors. Two rows with different timestamps but same data are incorrectly treated as distinct, inflating fact counts and preventing fixed-point convergence.

**Why it could happen:**
- Timestamp comparison accidentally included in equality checks (row dedup must ignore timestamps)
- JOIN output timestamp assignment is ambiguous (max of inputs? current iteration?)
- ANTIJOIN with timestamped rows may miss negation at wrong timestamp
- REDUCE aggregation over timestamped rows may double-count

**Mitigation:**
- Timestamps stored OUT-OF-BAND (separate array, not interleaved with row data)
- All existing row comparison functions (kway_row_cmp, memcmp) remain timestamp-unaware
- Extensive unit tests: validate fact counts match baseline for all 20 test suites
- Implement behind a feature flag; fall back to snapshot-based deltas if correctness fails

**Detection:** Any test suite fact count differs from baseline. Iteration count increases beyond baseline.

### Failure 3: DOOP Remains Intractably Slow Despite Both Optimizations

**Scenario:** After K-fusion + timestamped deltas, DOOP improves to 15-20m but plateaus. The remaining gap to DD's 47s is due to arrangement-based indexed joins, which neither Option A nor B provides.

**Why it could happen:**
- DOOP's 136 rules create complex join graphs where hash-join rebuild per iteration dominates
- DD's arrangements maintain pre-indexed collections across iterations; wirelog rebuilds indexes each time
- K=8 parallel evaluation helps but 8 workers still each do full hash-join builds
- The bottleneck shifts from consolidation to join index construction

**Mitigation:**
- Profile DOOP after Phase 3A to identify whether join or consolidate dominates
- If join-dominated: implement persistent hash indexes (incremental index update, not rebuild)
- Persistent indexes are a step toward arrangements without full Timely runtime
- Accept 15-20m as Phase 3 target; full DD parity requires Phase 4 (arrangement layer)

**Detection:** DOOP wall-time stabilizes at 15-20m despite K-fusion + timestamp optimizations. Profiling shows >60% time in JOIN operations.

---

## Test Plan

### Unit Tests

**K-Fusion Dispatch (Option A):**
- `test_k_fusion_dispatch_k2`: K=2 parallel evaluation produces correct merged output
- `test_k_fusion_dispatch_k4`: K=4 with varying delta sizes
- `test_k_fusion_dispatch_k8`: K=8 (DOOP-like) stress test
- `test_k_fusion_empty_delta`: K-fusion with empty delta inputs (skip optimization)
- `test_k_fusion_arena_isolation`: Per-worker arena does not leak across workers
- `test_k_fusion_error_propagation`: Worker failure propagates to main thread

**Timestamped Delta Tracking (Option B):**
- `test_timestamp_propagation_variable`: VARIABLE operator assigns correct timestamp
- `test_timestamp_propagation_join`: JOIN output carries max(left_ts, right_ts)
- `test_timestamp_propagation_filter`: FILTER preserves input timestamp
- `test_timestamp_dedup_ignores_ts`: CONSOLIDATE dedup compares data only, not timestamps
- `test_incremental_consolidate_ts`: Only timestamp==current rows are sorted and merged
- `test_delta_from_timestamps`: Delta extraction uses timestamps, not snapshot diff

### Integration Tests

**Semi-Naive Loop Correctness:**
- `test_seminaive_kfusion_cspa`: CSPA produces 20,381 tuples in 6 iterations with K-fusion
- `test_seminaive_kfusion_tc`: Transitive closure matches baseline fact count
- `test_seminaive_timestamp_cspa`: CSPA with timestamp-based deltas matches baseline
- `test_seminaive_combined`: K-fusion + timestamps together produce correct results
- `test_fixed_point_convergence`: Iteration count does not increase with any optimization

**Thread Safety:**
- `test_kfusion_concurrent_k8`: 8 workers, no data races (run with TSan)
- `test_kfusion_arena_stress`: 100 iterations with arena create/destroy cycles
- `test_kfusion_workqueue_reuse`: Workqueue reused across iterations (not create/destroy per iter)

### End-to-End Tests

**Regression Suite (All 20 test suites):**
- Every optimization must pass all 20 existing tests (19 OK + 1 EXPECTEDFAIL for DOOP)
- Fact counts must match baseline exactly
- Iteration counts must not increase

**Performance Benchmarks:**
- CSPA 3-run median wall-time (target: Phase 3A <4.2s, Phase 3A+B <2.5s)
- DOOP completion time (target: Phase 3A <30m, Phase 3A+B <15m)
- Peak RSS measurement (must not exceed 4GB for CSPA, 2GB for DOOP)
- Workqueue overhead measurement (target: <5% on K=2)

### Observability

**Performance Instrumentation:**
- Per-iteration wall-time logging (identify iteration-level regression)
- Per-operator time breakdown (CONSOLIDATE, JOIN, VARIABLE percentages)
- Delta size per iteration (track delta convergence rate)
- Workqueue task duration histogram (identify overhead vs useful work)
- Arena allocation high-water mark per worker

**Correctness Instrumentation:**
- Fact count assertion after each iteration (catch mid-evaluation errors)
- Timestamp monotonicity check (timestamps must not decrease within a relation)
- Delta subset assertion (delta facts must be subset of full relation)

---

## ADR: Architectural Decision Record

### Decision

Pursue **Option A+B Phased**: complete K-fusion parallelism first (2-3 weeks), then implement targeted timestamp-based delta tracking (4-6 weeks). Defer full Timely-like dataflow runtime (Option C) pending Phase 3A+B performance data.

### Decision Drivers

1. **Root cause**: The 91x DOOP gap is caused by coarse delta tracking, not lack of parallelism. K-fusion addresses symptom (parallel K-copies); timestamps address cause (incremental deltas).
2. **Feasibility**: K-fusion is 70% built; timestamps are a bounded extension. Full Timely is 12-16 weeks and high-risk.
3. **Incremental delivery**: Phase 3A delivers value in 2-3 weeks; Phase 3B adds compounding value in 4-6 more weeks. Full Timely delivers nothing for 12+ weeks.

### Alternatives Considered

| Option | Effort | DOOP Target | Risk | Verdict |
|--------|--------|-------------|------|---------|
| A: K-Fusion only | 2-3 weeks | 10-30m | Low | Necessary but insufficient |
| B: Timestamped deltas only | 4-6 weeks | 15-25m | Medium | Addresses root cause but misses easy K-fusion wins |
| C: Full Timely runtime | 12-16 weeks | ~47s | Very High | Optimal outcome but unacceptable timeline/risk |
| **A+B: Phased** | **6-9 weeks** | **5-15m** | **Medium** | **Best risk-adjusted ROI** |

### Why A+B Phased Was Chosen

1. **P1 (Incremental Value)**: K-fusion delivers measurable gains by week 3; timestamps add more by week 9
2. **P2 (C11 Purity)**: Both approaches stay pure C11 + pthreads; no runtime framework needed
3. **P3 (Correctness)**: K-fusion preserves exact semi-naive semantics; timestamps add metadata without changing evaluation logic
4. **P4 (Leverage Infrastructure)**: K-fusion reuses workqueue, merge, operator infrastructure; timestamps extend existing incremental_delta work
5. **P5 (Bounded Complexity)**: ~500 lines (A) + ~2000 lines (B) = ~2500 lines total, well within complexity budget

### Consequences

**Positive:**
- DOOP drops from 71m50s to estimated 5-15m (80-93% improvement)
- CSPA drops from 6.0s to estimated 1.5-2.5s (60-75% improvement)
- Architecture becomes incrementally closer to DD-like performance
- Foundation laid for future arrangement/index layer (Phase 4)

**Negative:**
- Does not achieve DD parity on DOOP (~47s); gap remains ~6-19x
- Timestamp overhead adds ~8 bytes/row memory cost
- Two optimization layers increase debugging surface
- Option C (full Timely) may still be needed for DD parity; decision deferred, not eliminated

**Neutral:**
- Workqueue infrastructure proven for K-fusion becomes reusable for other parallel patterns
- Performance measurement infrastructure established during Phase 2D carries forward

### Follow-ups

1. **After Phase 3A (Week 3)**: Benchmark DOOP completion time. If <5m, re-evaluate whether Phase 3B is needed.
2. **After Phase 3B (Week 9)**: Profile remaining DOOP gap. If >10x from DD, evaluate arrangement layer or Option C.
3. **Phase 4 Planning**: Based on Phase 3 data, decide between:
   - Arrangement layer (persistent indexes for incremental joins) -- moderate effort, high ROI
   - Full Timely runtime -- high effort, maximum ROI
   - FPGA backend via workqueue interface -- orthogonal, can parallelize with either

---

## Appendix A: Naiad/Timely Concept Mapping to wirelog

| Naiad Concept | wirelog Equivalent | Gap |
|---------------|-------------------|-----|
| Pointstamp (epoch, iter) | iteration counter in col_eval_stratum | wirelog has no per-record timestamps |
| Frontier | snap[] array (per-relation row count) | Coarse: relation-level, not operator-level |
| Notification | fixed-point check (any_new) | Coarse: checks all relations, not per-operator |
| Arrangement | col_rel_t (flat sorted array) | No time-indexing, no compaction, rebuilt each iter |
| Dataflow graph | wl_plan_t (operator sequence) | No scheduling; strict sequential execution |
| Progress protocol | N/A | Does not exist; would require graph-wide messaging |
| Worker scheduling | workqueue (submit/wait_all) | Barrier-only; no fine-grained operator scheduling |

## Appendix B: Complexity Estimates

| Component | Option A | Option B | Option C |
|-----------|----------|----------|----------|
| New C code (lines) | 500-600 | 1500-2500 | 3000-5000 |
| Files modified | 3-4 | 5-8 | 10-15 |
| New test files | 1 | 2-3 | 5-8 |
| Risk of correctness bugs | Low | Medium | High |
| Calendar time (1 eng) | 2-3 weeks | 4-6 weeks | 12-16 weeks |
| Reuse of existing code | 70% | 30% | 10% |

---

**Document Version:** 1.0
**Generated:** 2026-03-08
**Format:** RALPLAN-DR (Principles, Decision Drivers, Options, Pre-Mortem, Test Plan, ADR)
**Review Required:** Architect + Critic consensus before execution
