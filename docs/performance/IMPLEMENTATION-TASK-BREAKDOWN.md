# Implementation Task Breakdown: Evaluator Rewrite with K-Fusion

**Date:** 2026-03-08
**Status:** Ready for engineering execution
**Engineer Assignment:** [To be assigned]
**Timeline:** 2-3 weeks (10-15 working days)

---

## Sprint Overview

**Objective:** Replace K-copy loop in semi-naive evaluator with K-fusion dispatcher + workqueue parallelism

**Success Criteria:**
- [ ] All 15 workloads pass regression tests (output correctness)
- [ ] Iteration count stays at 6 for CSPA (must not increase)
- [ ] CSPA wall-time improved 30-40% (17-20 seconds)
- [ ] DOOP completes < 5 minutes (primary breakthrough)
- [ ] Workqueue overhead < 5% on K=2 (measured by day 3)

**Risk Gates:**
- Day 3: Validate iteration count, overhead, root cause assumptions
- Day 4: All 15 workloads passing regression
- Day 5: Performance profiling complete

---

## Task Phase 1: Design & Code Review (Days 1-2)

### Task 1.1: Study Existing Infrastructure
**Owner:** [Engineer]
**Timeline:** 2 hours
**Checklist:**
- [ ] Read workqueue.h/c (269 lines) - understand API and ring buffer design
- [ ] Read col_op_consolidate_kway_merge() (187 lines, lines 1498-1684) - understand merge algorithm
- [ ] Review col_eval_relation_plan() (400 lines, lines 2540-2730) - understand semi-naive loop structure
- [ ] Review exec_plan_gen.c expand_multiway_delta() (lines 980-1052) - understand K-copy expansion
- [ ] Document findings: which functions are reusable, which need new code

**Deliverable:** Code_Review_Notes.txt with key findings and code pointers

### Task 1.2: Design K-Fusion Integration
**Owner:** [Engineer]
**Timeline:** 3 hours
**Checklist:**
- [ ] Define COL_OP_K_FUSION node type (add to col_op_kind enum)
- [ ] Design struct for K-fusion metadata (copy count, op array, output id)
- [ ] Identify integration points in col_eval_relation_plan() (where to add K-fusion dispatch)
- [ ] Design arena allocation per worker (per-thread pattern validation)
- [ ] Document refactoring strategy: pseudo-code for K-fusion dispatch

**Deliverable:** K-FUSION-DESIGN.md with code sketches and integration points

### Task 1.3: Architecture Review Meeting
**Owner:** [Engineer] + [Architect/Code Reviewer]
**Timeline:** 1 hour
**Agenda:**
- [ ] Review K-fusion design against existing infrastructure
- [ ] Validate arena cloning thread-safety approach
- [ ] Confirm integration points in col_eval_relation_plan()
- [ ] Identify any hidden dependencies or edge cases

**Deliverable:** Approved design, ready to implement

---

## Task Phase 2: Core Implementation (Days 2-4)

### Task 2.1: Add COL_OP_K_FUSION Node Type
**Owner:** [Engineer]
**Timeline:** 1 hour
**Checklist:**
- [ ] Add COL_OP_K_FUSION to col_op_kind enum (columnar_nanoarrow.h)
- [ ] Add K-fusion metadata struct (copy count, ops array)
- [ ] Add K-fusion case to operator switch statements (as needed)
- [ ] Verify no compilation errors (gcc -Wall -Wextra -Werror)
- [ ] Apply clang-format (llvm@18)

**Deliverable:** Compiling code with new node type

**Files Modified:**
- wirelog/backend/columnar_nanoarrow.h (enum + struct)
- wirelog/backend/columnar_nanoarrow.c (switch cases as needed)

### Task 2.2: Refactor col_eval_relation_plan() for K-Fusion Dispatch
**Owner:** [Engineer]
**Timeline:** 6-8 hours (CRITICAL PATH - primary work)
**Checklist:**
- [ ] Locate K-copy loop in col_eval_relation_plan() (lines 2540-2730)
- [ ] Add K-fusion conditional: if (op->kind == COL_OP_K_FUSION) branch
- [ ] Implement K-fusion dispatch using workqueue:
  ```c
  col_workqueue_t *wq = col_workqueue_create(NUM_WORKERS);
  for (uint32_t j = 0; j < fusion->k; j++) {
      col_workqueue_submit(wq, col_eval_op_task, fusion->ops[j], &results[j]);
  }
  col_workqueue_wait_all(wq);
  col_rel_t merged = col_rel_merge_k(results, fusion->k, ...);
  col_workqueue_destroy(wq);
  ```
- [ ] Remove or bypass sequential K-copy loop for K-fusion ops
- [ ] Preserve existing K-copy loop for non-K-fusion paths (backward compat)
- [ ] Verify no compilation errors (gcc -Wall -Wextra -Werror)
- [ ] Apply clang-format (llvm@18)

**Deliverable:** col_eval_relation_plan() with K-fusion dispatch integrated

**Files Modified:**
- wirelog/backend/columnar_nanoarrow.c (lines 2540-2730, refactored)

### Task 2.3: Implement col_rel_merge_k() for Inline Merge
**Owner:** [Engineer]
**Timeline:** 2 hours
**Checklist:**
- [ ] Extract merge logic from col_op_consolidate_kway_merge() (lines 1498-1684)
- [ ] Adapt for inline use (input: K sorted relations, output: merged + deduplicated)
- [ ] Reuse kway_row_cmp() comparator
- [ ] Implement min-heap merge with on-the-fly dedup
- [ ] Handle edge cases: empty inputs, K=1 (passthrough), duplicates
- [ ] Unit test with synthetic K-way data
- [ ] Apply clang-format (llvm@18)

**Deliverable:** col_rel_merge_k() function, unit-tested

**Files Modified or Created:**
- wirelog/backend/columnar_nanoarrow.c (add col_rel_merge_k function if not already present)

### Task 2.4: Thread-Safe Arena Integration
**Owner:** [Engineer]
**Timeline:** 2 hours
**Checklist:**
- [ ] Review workqueue arena allocation pattern (workqueue.h:49-59)
- [ ] Implement per-worker arena creation in K-fusion dispatch
- [ ] Validate arena cloning is thread-safe (no shared mutable state)
- [ ] Add arena cleanup in col_workqueue_wait_all() (after workers finish)
- [ ] Test with multi-threaded scenario (K=2, NUM_WORKERS=2+)
- [ ] Verify no memory leaks with valgrind (spot check)

**Deliverable:** Thread-safe arena cloning validated

**Files Modified:**
- K-fusion dispatch in columnar_nanoarrow.c (arena per worker)

---

## Task Phase 3: Testing & Validation (Days 4-5)

### Task 3.1: Unit Tests for K-Fusion Dispatch
**Owner:** [Engineer]
**Timeline:** 3 hours
**Checklist:**
- [ ] Create test_k_fusion_dispatch.c (new test file)
- [ ] Test 1: K=1 (single copy, should behave like non-K-fusion)
- [ ] Test 2: K=2 (two copies, validate merge correctness)
- [ ] Test 3: K=3 (three copies, validate heap merge)
- [ ] Test 4: K=2 with duplicates (validate dedup)
- [ ] Test 5: Parallel execution correctness (thread-safe arena)
- [ ] All tests compile with -Wall -Wextra -Werror
- [ ] Register test in tests/meson.build

**Deliverable:** test_k_fusion_dispatch.c with 5+ tests passing

**Files Created:**
- tests/test_k_fusion_dispatch.c

### Task 3.2: Regression Testing (All 15 Workloads)
**Owner:** [Engineer]
**Timeline:** 2 hours (including wait time for test execution)
**Checklist:**
- [ ] Build: meson compile -C build
- [ ] Run all 15 workloads: meson test -C build
- [ ] Check: All tests PASS (17 OK expected including new test)
- [ ] For each workload, validate:
  - [ ] Output correctness (fact count matches baseline)
  - [ ] Iteration count (should be same or decrease, never increase)
  - [ ] No memory corruption (valgrind spot check on CSPA)
- [ ] Document results in REGRESSION-RESULTS.txt

**Deliverable:** REGRESSION-RESULTS.txt with all 15 workloads passing

**Gate Validation:**
- If any workload fails: ROLLBACK and investigate (likely dedup bug in merge_k)
- If iteration count increases: ESCALATE and investigate fixed-point (possible correctness issue)
- If all pass: PROCEED to performance profiling

### Task 3.3: Performance Profiling
**Owner:** [Engineer]
**Timeline:** 3 hours
**Checklist:**
- [ ] Build release: meson compile -C build --buildtype=release
- [ ] Profile CSPA with perf: `perf record ./build/bench/bench_flowlog --workload cspa ...`
- [ ] Analyze: `perf report` - identify top functions by % wall time
- [ ] Measure CSPA wall-time: target 17-20 seconds (30-40% improvement from 28.7s)
- [ ] Measure workqueue overhead: K-copy evaluation time before/after
- [ ] Measure iteration count: should be 6 (must not increase)
- [ ] Memory RSS: should be < 4GB
- [ ] Document findings in PERFORMANCE-PROFILE.txt

**Deliverable:** PERFORMANCE-PROFILE.txt with metrics and analysis

**Gate Validation:**
- [ ] Wall-time 30-40% improvement: PASS
- [ ] Workqueue overhead < 5%: PASS
- [ ] Iteration count = 6: PASS
- If any gate fails: Investigate and optimize (see Task Phase 4)

### Task 3.4: DOOP Validation (Primary Breakthrough)
**Owner:** [Engineer]
**Timeline:** 5-10 minutes + analysis
**Checklist:**
- [ ] Run DOOP: `./build/bench/bench_flowlog --workload doop --timeout 300` (5 min timeout)
- [ ] Measure: Completion time (target < 5 minutes)
- [ ] If completes < 5 min: ✅ **PRIMARY BREAKTHROUGH ACHIEVED**
- [ ] If times out: Investigate (may have other blockers, K-fusion still necessary)
- [ ] Document result in DOOP-VALIDATION.txt

**Deliverable:** DOOP-VALIDATION.txt with completion status

**Gate Validation:**
- [ ] DOOP completes < 5 minutes: **PRIMARY SUCCESS METRIC**
- If fails: Not a failure of K-fusion, but indicates other optimizations needed for Phase 3

---

## Task Phase 4: Optimization & Polish (Days 5+, Optional)

### Task 4.1: Hot-Path Optimization (If Needed)
**Owner:** [Engineer]
**Timeline:** 2-4 hours (only if wall-time > 20 seconds)
**Checklist:**
- [ ] Profile identifies top bottleneck (if not K-copy)
- [ ] Optimize identified function (e.g., improve merge_k comparator)
- [ ] Validate improvement (< 5% performance gain per optimization)
- [ ] Re-run regression tests

**Deliverable:** Optimized code (only if needed)

### Task 4.2: Stress Testing
**Owner:** [Engineer]
**Timeline:** 2 hours (optional, if time permits)
**Checklist:**
- [ ] Test with varying worker counts: NUM_WORKERS = 1, 2, 4, 8
- [ ] Measure scaling on multi-core: should see improvement with more workers
- [ ] Test memory pressure: run with ulimit -v (memory limit)
- [ ] Test with large relations: ensure no OOM errors

**Deliverable:** STRESS-TEST-RESULTS.txt

### Task 4.3: Documentation & Cleanup
**Owner:** [Engineer]
**Timeline:** 2 hours
**Checklist:**
- [ ] Update ARCHITECTURE.md with K-fusion section
- [ ] Add inline comments to col_eval_relation_plan() explaining K-fusion path
- [ ] Document workqueue integration pattern
- [ ] Add rationale for evaluator rewrite in design docs
- [ ] Remove any debug code or temporary changes
- [ ] Final code review: ensure -Wall -Wextra -Werror compliance

**Deliverable:** Updated ARCHITECTURE.md + documented code

---

## Critical Path Dependencies

```
Task 1.3 (Review) ──┬──→ Task 2.1 (Add node type)
                    │    ├──→ Task 2.2 (Refactor loop) ← CRITICAL (6-8h)
                    │    ├──→ Task 2.3 (Merge)
                    │    └──→ Task 2.4 (Arena)
                    │         └──→ Task 3.1-3.4 (Testing/Validation)
                    │
                    └──→ Task 3.2 (Regression Gate)
```

**Critical Path:** 1.3 → 2.2 (col_eval_relation_plan refactor) → 3.2 (regression gate)

---

## Day-by-Day Schedule

### Day 1: Design
- [ ] Task 1.1: Study existing infrastructure (2h)
- [ ] Task 1.2: Design K-fusion integration (3h)
- **End of day:** K-FUSION-DESIGN.md complete, ready for review

### Day 2: Design Review + Implementation Start
- [ ] Task 1.3: Architecture review (1h)
- [ ] Task 2.1: Add COL_OP_K_FUSION node type (1h)
- [ ] Task 2.2: Start col_eval_relation_plan() refactor (3-4h)
- **End of day:** Core refactor ~50% complete

### Day 3: Implementation Complete + First Validation
- [ ] Task 2.2: Finish col_eval_relation_plan() refactor (2-4h)
- [ ] Task 2.3: Implement col_rel_merge_k() (2h)
- [ ] Task 2.4: Thread-safe arena integration (2h)
- [ ] **VALIDATION GATE:** Compilation test, basic unit tests
- **End of day:** Core implementation complete, ready for regression testing

### Day 4: Regression Testing + Performance Profiling
- [ ] Task 3.1: Unit tests (3h)
- [ ] Task 3.2: Regression testing all 15 workloads (2h) ← **CRITICAL GATE**
- [ ] Task 3.3: Performance profiling (3h)
- **End of day:** All workloads passing, performance metrics collected

### Day 5: DOOP Validation + Documentation
- [ ] Task 3.4: DOOP validation (0.5h) ← **PRIMARY BREAKTHROUGH METRIC**
- [ ] Task 4.3: Documentation & cleanup (2h)
- [ ] Optional: Task 4.1 hot-path optimization (if needed)
- **End of day:** Production-ready code, all tests passing, DOOP unblocked

---

## Code Review Checklist

**Before merge, verify:**
- [ ] All 15 workloads pass regression tests
- [ ] Iteration count = 6 (did not increase)
- [ ] CSPA wall-time improved 30-40%
- [ ] DOOP completes < 5 minutes
- [ ] Zero memory leaks (valgrind spot check)
- [ ] All code compiles with -Wall -Wextra -Werror
- [ ] clang-format applied (llvm@18)
- [ ] No new global state introduced (thread-safe)
- [ ] Arena cloning validated per-worker (no shared mutable state)
- [ ] K-way merge dedup logic correct (no duplicate rows possible)

---

## Atomic Commits

Upon completion, prepare for merge as single logical commit:

```bash
git commit -m "feat: implement K-fusion evaluator with workqueue parallelism

- Add COL_OP_K_FUSION node type for K-copy operations
- Refactor col_eval_relation_plan() for K-fusion dispatch with workqueue
- Implement col_rel_merge_k() for inline K-way merge with dedup
- Thread-safe arena allocation per worker (per-worker pattern)
- All 15 workloads pass regression, iteration count stable at 6
- CSPA wall-time improved 30-40% (28.7s → 17-20s)
- DOOP completes < 5 minutes (primary breakthrough)
- Comprehensive unit tests + stress testing

References: ARCHITECT-VERIFICATION-FINAL.md, IMPLEMENTATION-TASK-BREAKDOWN.md
No Co-Authored-By (wirelog project policy)"
```

---

## Success Criteria Summary

**Must Have (Gate Criteria):**
- ✅ All 15 workloads pass (output correctness)
- ✅ Iteration count = 6 (not increase)
- ✅ CSPA improved 30-40%
- ✅ DOOP < 5 minutes
- ✅ Workqueue overhead < 5%

**Should Have (Production Quality):**
- ✅ Full regression test coverage
- ✅ Unit tests for K-fusion dispatch
- ✅ Performance profiling documented
- ✅ Code quality (clang-format, zero compiler warnings)
- ✅ Thread-safety validated

**Nice to Have (Optimization):**
- ✅ Hot-path optimization (if time permits)
- ✅ Stress testing (multi-threaded scenarios)
- ✅ Extended documentation

---

## Escalation & Risk Mitigation

| Risk | Trigger | Action |
|---|---|---|
| **Iteration count increases** | If count > 6 on day 4 | Investigate fixed-point convergence, rollback, debug |
| **Workqueue overhead > 5%** | If measured on day 3 | Consider sequential K for CSPA, keep parallel for DOOP |
| **CSPA wall-time < 20% improvement** | If profiling shows < 20% | Investigate hidden bottleneck (may not be K-copy) |
| **DOOP still times out** | If measured on day 5 | K-fusion necessary but not sufficient, investigate other blockers |
| **Dedup bug in merge_k** | If regression test fails | Validate merge algorithm, add edge case tests, debug |

---

**Ready for Engineering Execution**

All tasks are scoped, estimated, and gated. Engineer can start immediately with K-FUSION-DESIGN.md as reference.

