# Breakthrough Performance Strategy: 30/60/90 Day Roadmap

**Date:** 2026-03-08
**Status:** Strategic recommendation for 50%+ wall-time improvement
**Audience:** Engineering leadership, project planning
**Outcome:** Clear path to unlock DOOP and achieve baseline performance recovery

---

## Executive Recommendation

### STOP: Incremental Fixes, START: Evaluator Rewrite

**Current State:** CSPA at 28.7s (6× slower than 4.6s baseline)
**Target:** 12-16 seconds (50%+ improvement)
**Path:** Evaluator redesign with K-fusion and workqueue parallelism
**Timeline:** 30 days (4-week sprint with 2 engineers)
**DOOP Unblock:** Single-pass evaluator enables 8-way joins (currently timeout)

---

## Why Evaluator Rewrite Over Incremental Fixes

### The Fundamental Insight

**K-copy redundancy is the architecture's Achilles' heel.**

Current semi-naive loop forces this pattern:
```
for (6 iterations) {
    for (each rule) {
        if rule expands to K copies {
            for (K=1 to 2 copies) {
                clone, join, consolidate  // 12 total operations
            }
        }
    }
}
```

No amount of optimization (K-way merge, incremental sort, CSE cache) can save us from the **fundamental inefficiency**: evaluating the same rule K times independently.

**Evaluator rewrite fixes this by:**
1. Evaluating K copies **once** with parallelism
2. Merging results **inline** (no separate consolidate step)
3. Reducing from 12 copies → 2-4 copies across all iterations
4. **Savings: 66-83% of K-copy overhead**

### Comparative Analysis

| Approach | Wall-Time Gain | Correctness Risk | Architectural Impact | DOOP Impact |
|----------|---|---|---|---|
| **Incremental Consolidate** | 15-20% | High (subtle dedup bugs) | Threads incremental state through eval stack | No unblock |
| **K-way Merge + Empty-Delta** | 20-30% | Medium (already implemented) | No change, additive | Partial unblock |
| **Evaluator Rewrite** | 50-60% | Medium (well-scoped refactor) | Fundamental redesign, cleaner | ✅ Full unblock |
| **Incremental + Evaluator** | 60-70% | Medium (phased approach) | Incremental becomes unused | ✅ Full unblock |

**Verdict:** Evaluator rewrite is the only approach that:
1. Hits the 50%+ target
2. Unblocks DOOP (prerequisite for Phase 3 multi-way optimizations)
3. Has lower correctness risk than incremental consolidate
4. Produces cleaner, more maintainable architecture

---

## ARCHITECT-VERIFIED ROADMAP: 2-3 Week Acceleration Plan

⚠️ **CRITICAL CORRECTION FROM ARCHITECT REVIEW:**
- **Workqueue already exists** (workqueue.c, 269 lines, fully implemented)
- **K-way merge already exists** (col_op_consolidate_kway_merge, lines 1498-1684)
- **Timeline reduced from 4 weeks → 2-3 weeks** (1 engineer sufficient)
- **Expectations revised: 30-40% CSPA, 50-60% DOOP** (not 50-60% on CSPA)
- **Fixed-point iterations stay constant** (data-dependent, semi-naive correctness)

### Week 1 (Days 1-5): Design & Integration (ACCELERATED)

**Goal:** Refactor semi-naive loop for K-fusion, leverage existing infrastructure

**Milestones (ACCELERATED - workqueue & merge already exist):**

- [ ] **Day 1:** Design K-fusion plan node + refactoring strategy
  - Define COL_OP_K_FUSION node type for plan generation
  - Identify integration points in col_eval_relation_plan() (lines 2540-2730)
  - Plan arena cloning per worker (per-worker pattern at workqueue.h:49-59)
  - **Deliverable:** Design doc with code pointers

- [ ] **Day 2-3:** Refactor col_eval_relation_plan() for K-fusion dispatch
  - Add K-fusion conditional branch (COL_OP_K_FUSION handling)
  - Replace K-copy loop with workqueue dispatch
  - Adapt col_rel_merge_k() from consolidate_kway_merge (lines 1498-1684)
  - **Deliverable:** col_eval_relation_plan() updated with K-fusion path

- [ ] **Day 4:** Integration testing + validation
  - Unit tests for K-fusion dispatch (K=2, K=3)
  - Validate arena cloning thread-safety
  - Integration test with semi-naive loop
  - **Deliverable:** test_k_fusion_integration.c (10+ tests passing)

- [ ] **Day 5:** Regression testing + performance profiling
  - Run all 15 workloads (output correctness gate)
  - Measure iteration counts (should stay at 6 for CSPA)
  - Profile wall-time improvement (30-40% target for CSPA, 50-60% for DOOP)
  - **Deliverable:** REGRESSION-RESULTS.txt + PERFORMANCE-PROFILE.txt

**Critical Validation (Days 2-5):**
- ⚠️ Confirm iteration count does NOT change (architect emphasis)
- ⚠️ Profile K-copy overhead dominance (60-70% claimed, must validate with perf)
- ⚠️ Measure workqueue overhead on K=2 (should be < 5%)
- ⚠️ Run DOOP first validation (K=8 parallelism benefit is higher than CSPA K=2)

**Risk Mitigation:**
- If iterations increase: Rollback and investigate fixed-point convergence (possible dedup timing bug)
- If wall-time < 20% improvement: Profile to identify hidden bottleneck (may not be K-copy)
- If workqueue overhead > 5% on K=2: Consider sequential K path for CSPA, keep parallel for DOOP (K=8)

---

### Week 2 (Days 6-10): Integration & Validation

**Goal:** Refactor semi-naive loop and validate correctness

**Milestones:**
- [ ] **Day 6:** Refactor semi-naive loop for K-fusion
  - Integrate K-fusion evaluator path
  - Replace K-copy loop with single K-fusion evaluation
  - Remove redundant FORCE_DELTA fallback for K-fusion case
  - **Deliverable:** Updated col_eval_relation_plan() with K-fusion conditional

- [ ] **Day 7:** Full integration testing
  - Unit tests for K-fusion evaluator (single test, K=2, K=3)
  - Integration tests for semi-naive loop with K-fusion
  - Iteration count validation (should stay same or decrease)
  - **Deliverable:** test_k_fusion_evaluator.c (10+ test cases)

- [ ] **Day 8:** Regression testing (Phase 1)
  - Run all 15 workloads
  - Validate output correctness (fact counts must match)
  - Measure iteration counts (should be ≤ baseline)
  - **Deliverable:** REGRESSION-RESULTS.txt with all 15 workloads passing

- [ ] **Day 9:** Performance validation
  - Profile CSPA: wall-time breakdown
  - Compare to baseline: consolidate %, join %, memory allocation
  - Measure workqueue overhead
  - **Deliverable:** PERFORMANCE-RESULTS.txt with detailed metrics

- [ ] **Day 10:** DOOP validation
  - Run DOOP benchmark
  - Measure completion time (target: < 5 minutes)
  - If successful: celebrate, move to Phase 3 planning
  - If timeout: investigate and escalate
  - **Deliverable:** DOOP-RESULTS.txt with status

**Parallel Track (Days 6-10):**
- Prepare comprehensive test suite for parallel correctness
- Monitor for race conditions in arena cloning
- Stress-test workqueue with varying worker counts

**Success Criteria:**
- ✅ All 15 workloads produce identical output
- ✅ CSPA wall-time < 16 seconds (50% improvement)
- ✅ Iteration counts unchanged or decreased
- ✅ Memory usage < 4GB (same as baseline)
- ✅ DOOP completes < 5 minutes (enables Phase 3)

**If Goals Not Met:**
- If CSPA > 16 seconds but > 20 seconds: revisit merge_k complexity, optimize hotpath
- If DOOP still times out: fall back to sequential K-fusion (lose parallelism), investigate other blockers
- If iteration count increases: debug fixed-point condition, may indicate correctness issue

---

### Week 3 (Days 11-15): Optimization & Polish

**Goal:** Optimize hot paths and prepare production merge

**Milestones:**
- [ ] **Day 11:** Profile and optimize hot paths
  - Identify functions consuming > 10% wall time
  - Profile merge_k with large K values
  - Optimize memory allocation in merge (preallocate output, avoid copies)
  - **Deliverable:** PERFORMANCE-TUNING.txt with optimization results

- [ ] **Day 12:** Code quality & style
  - Apply clang-format (llvm@18)
  - Add comments to merge_k and workqueue integration points
  - Code review checklist (no global state, thread-safe, correct error handling)
  - **Deliverable:** Formatted code + review checklist

- [ ] **Day 13:** Stress testing
  - Run workloads with varying numbers of worker threads (2, 4, 8)
  - Validate performance scaling (should improve with more workers on K-fusion)
  - Test memory pressure (OOM scenarios)
  - **Deliverable:** STRESS-TEST-RESULTS.txt

- [ ] **Day 14:** Documentation
  - Update ARCHITECTURE.md with K-fusion section
  - Document workqueue API and usage patterns
  - Write design rationale for future maintainers
  - **Deliverable:** Updated docs/ARCHITECTURE.md

- [ ] **Day 15:** Final validation & cleanup
  - Final regression test (all 15 workloads)
  - Ensure no memory leaks (valgrind check)
  - Prepare atomic commits (one per major feature)
  - **Deliverable:** Ready for production merge

**Atomic Commits (prepared for merge):**
1. feat: implement workqueue backend with thread-safe arena cloning
2. feat: implement inline K-way merge with dedup (col_rel_merge_k)
3. feat: add K-fusion tree builder to plan generation
4. refactor: integrate K-fusion evaluator into semi-naive loop
5. test: comprehensive K-fusion correctness and regression tests

---

## Post-Delivery: Phase 3 Unblock (Days 16+)

### Immediate Wins

With evaluator rewrite complete:
1. **DOOP enablement:** 8-way joins now feasible (<5 min expected)
2. **Workqueue infrastructure ready:** Can tackle Phase B-lite scheduling
3. **Parallelism unlocked:** Multi-core scaling available for future optimizations

### Phase 3 Planning (Days 16-30)

**Leverage evaluator rewrite for:**
1. DOOP-specific optimizations (multi-way join acceleration)
2. Workqueue-based evaluation for non-recursive strata
3. Further incremental consolidate (now safe to add on top of evaluator rewrite)

---

## Team Composition (ARCHITECT-VERIFIED)

### PRIMARY (RECOMMENDED): 1 Engineer, 2-3 Week Sprint ⭐

**Engineer 1 (Lead): Evaluator Integration**
- **Responsibility:** K-fusion dispatch + col_eval_relation_plan() refactor + merge integration
- **Skills:** C11 + threaded programming + algorithm understanding
- **Time commitment:** 100% (weeks 1-3)
- **Critical path:** Days 2-3 refactor (400 lines of evaluator logic)
- **Blockers to watch:**
  - Arena cloning thread-safety (but design already validated)
  - Iteration count drift (catch by day 4)
  - Workqueue overhead on K=2 (micro-benchmark on day 3)

**Optional: 1/2-Time Code Reviewer**
- Review col_eval_relation_plan() refactor for thread-safety
- Validate performance profiling methodology
- Monitor for subtle race conditions in arena cloning

### ALTERNATIVE: 2 Engineers, 2-3 Weeks (Parallel)

- **Engineer 1:** K-fusion refactor (days 1-3) → then regression testing (days 4-5)
- **Engineer 2:** Parallel setup + test infrastructure (days 1-3) → then profiling (days 4-5)
- **Advantage:** Faster turnaround, split workload
- **Disadvantage:** Overkill for scope (workqueue + merge already exist)

---

## Budget & Resource Allocation (ACCELERATED)

### PRIMARY: 1-Engineer, 2-3 Week Sprint ⭐

| Resource | Cost | Notes |
|----------|------|-------|
| **Engineering** | 80-120 hours (1 eng × 2-3 weeks × 40-60 h/week) | Core refactor + integration + testing |
| **Code review** | 10-15 hours | Thread-safety validation of col_eval_relation_plan refactor |
| **Performance validation** | 5-10 hours (included) | Profiling with perf to validate improvements |
| **Documentation** | 5 hours (included) | ARCHITECTURE.md updates |
| **TOTAL** | **100-150 hours** | ~0.5-0.75 engineer-weeks of actual effort |

**Cost savings:** Workqueue (4h) + K-way merge (4h) already exist = 8 hours saved vs naive estimate

### Milestones & Checkpoints (2-3 WEEK SPRINT)

**Day 3 End:** col_eval_relation_plan refactor complete + unit tests passing
- **Gate:** K-fusion dispatch operational, arena cloning thread-safe

**Day 4 End:** All 15 workloads passing regression tests
- **Gate:** No correctness regressions, iteration count validated

**Day 5 End:** Performance profiling + DOOP validation
- **Gate:** 30-40% CSPA improvement confirmed (or investigate blockers)

**Days 6-10 (optional):** Optimization + stress testing
- **Gate:** Hot-path tuning (if needed), workqueue overhead quantified

---

## Success Metrics (ARCHITECT-VERIFIED EXPECTATIONS)

### Hard Targets (Must Hit)

| Metric | Target | Current | Success Criteria | Notes |
|--------|--------|---------|---|---|
| **CSPA Wall Time** | 18-20 seconds | 28.7s | 30-40% improvement ✓ | K=2 parallelism has limited scalability |
| **Iteration Count (CSPA)** | 6 iterations (same) | 6 iterations | ⚠️ MUST NOT INCREASE | Data-dependent, not addressable by K-fusion |
| **Fact Count (CSPA)** | 20,381 tuples | 20,381 tuples | Correctness gate ✓ | No data loss |
| **DOOP Completion** | < 5 minutes | Timeout | PRIMARY breakthrough ✓ | K=8 parallelism unlocks 8-way joins |
| **All 15 Workloads** | Pass regression | 15/15 passing | No output changes | Verify architectural soundness |
| **Memory Peak RSS** | < 4GB | ~3.1GB | Performance baseline | Should not degrade with K-fusion |

### Soft Targets (Validation Milestones)

| Metric | Target | Expected | Purpose |
|--------|--------|----------|---------|
| **DOOP Wall Time** | 2-3 minutes | 3-5 minutes | K=8 parallelism benefit measurement |
| **Workqueue Overhead (K=2)** | < 5% | 2-3% expected | Confirms parallelism doesn't hurt CSPA |
| **CSPA Hot-Path (K-copy)** | 60-70% of wall time | Must validate | Confirms root cause diagnosis |

### ⚠️ Critical Caveats

1. **Iteration count will stay at 6** (not reduce to 1-2 as initially claimed)
   - Fixed-point convergence is data-dependent semi-naive property
   - K-fusion improves per-iteration cost, not iteration count
   - Architect verification: CRITICAL, do not proceed without validating this assumption

2. **CSPA targets 30-40%, not 50-60%**
   - K=2 parallelism on 2+ cores has limited scalability advantage
   - Real breakthrough is on DOOP (K=8) where parallelism scales better
   - Architect recommendation: Validate DOOP first (higher ROI)

3. **All 15 workloads must produce identical output**
   - Output must match exactly (same fact count, same order after consolidation)
   - If any workload differs, rollback and investigate (likely a dedup bug in K-way merge)
   - Iteration count may decrease if empty-delta skip improves, but must stay constant or decrease only

---

## Risk Management

### High-Risk Areas

| Risk | Probability | Impact | Mitigation |
|------|---|---|---|
| **Workqueue thread-safety bugs** | Medium | High | Arena cloning reviewed early, stress testing |
| **K-way merge dedup correctness** | Low | High | Unit tests first, comprehensive test coverage |
| **Fixed-point iteration count increase** | Low | High | Iteration count validation on day 8 |
| **DOOP still times out** | Medium | High | Escalate to Phase 3 planning, investigate other blockers |

### Escalation Triggers

| Trigger | Action |
|---------|--------|
| If CSPA > 18s by day 10 | Investigate merge_k complexity, consider fallback to sequential K |
| If regression test fails | Rollback to checkpoint, debug correctness issue |
| If DOOP times out on day 10 | Confirm other factors (join complexity, plan rewriting) aren't blocking |
| If >2 days delay on week 1 | Add resources or extend timeline to week 5 |

---

## Alternative Plan B (If Evaluator Rewrite Blocked)

### Fallback: Incremental Consolidate + Targeted Optimizations (3 Weeks)

**If critical blocker emerges:**
1. Implement incremental consolidate (days 1-8)
2. Complete K-way merge CONSOLIDATE validation on DOOP (days 8-12)
3. Add targeted join optimizations (days 12-18)
4. Expected gain: 30-35% (falls short of 50% target)

**Decision point:** If day 10 validation shows < 20% gain, pivot to Plan B and extend timeline.

---

## Success Narrative (2-3 WEEK ACCELERATED SPRINT)

**By day 3 (Mid-week 1):**
- ✅ col_eval_relation_plan() refactored for K-fusion dispatch
- ✅ Arena cloning validated thread-safe
- ✅ Unit tests for K-fusion path passing

**By day 4 (End of week 1):**
- ✅ All 15 workloads pass regression tests (output correctness confirmed)
- ✅ Iteration count validated at 6 (did not increase - architect critical check)
- ✅ Memory RSS baseline established

**By day 5 (Early week 2):**
- ✅ CSPA wall-time improved 30-40% (17-20 seconds)
- ✅ DOOP completes < 5 minutes (PRIMARY breakthrough)
- ✅ Performance profiling complete, bottleneck analysis validated
- ✅ Production readiness confirmed

**Optional Days 6-10 (Week 2-3):**
- ✅ Hot-path optimization (if wall-time still > 20 seconds)
- ✅ Stress testing with varying worker counts
- ✅ Final documentation + cleanup

**Outcome:** Evaluator rewrite complete in 2-3 weeks, DOOP unblocked, Phase 3 ready to start immediately.

---

## Next Steps

### Immediate (Today)

1. [ ] Approve evaluator rewrite as primary path
2. [ ] Allocate 2 engineers for 4-week sprint
3. [ ] Schedule kickoff meeting (architecture review with team)

### This Week

1. [ ] Finalize workqueue design with team
2. [ ] Create detailed week-by-week task breakdown
3. [ ] Set up performance profiling infrastructure
4. [ ] Begin workqueue + merge_k implementation

### Success Criteria for Approval

- [ ] Team agrees evaluator rewrite is viable path
- [ ] Resources allocated and committed
- [ ] Workqueue design reviewed and approved
- [ ] Testing infrastructure in place

---

## Summary (ARCHITECT-APPROVED)

**The evaluator rewrite is architecturally sound and feasible.** K-fusion + workqueue parallelism leverages existing infrastructure (workqueue + K-way merge already built) to deliver:
- **30-40% wall-time improvement on CSPA** (K=2, limited parallelism benefit)
- **50-60% improvement on DOOP** (K=8, parallelism scales better) — PRIMARY breakthrough
- **Unblock Phase 3** (DOOP < 5 minutes enables multi-way join optimization)
- **Cleaner architecture** than incremental consolidate (no state threading)

**CRITICAL CORRECTION:** Fixed-point iterations stay constant (semi-naive correctness). The initial claim that iterations reduce from 6 to 1-2 was incorrect. Architect has validated this is a non-addressable data-dependent property.

**Timeline:** 2-3 weeks with 1 engineer (or 2-3 weeks with 2 parallel engineers).

**Recommendation:** START IMMEDIATELY with architect corrections applied. Validate iteration count and workqueue overhead on day 3. Prioritize DOOP validation (higher ROI than CSPA micro-optimization on K=2).

---

**Document Version:** 2.0 (Architect-Verified)
**Last Updated:** 2026-03-08
**Architect Review:** APPROVED with critical corrections applied
**Owner:** Performance Working Group
