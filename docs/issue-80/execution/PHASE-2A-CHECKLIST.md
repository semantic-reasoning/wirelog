# Phase 2A Pre-Execution Checklist

**Phase**: 2A (Columnar Backend Implementation + Rust Removal)
**Timeline**: 35 days with 2 C engineers
**Status**: Ready for execution (pending engineer assignment)

---

## Pre-Kickoff (Before Day 0)

- [ ] **Assign Engineers**: Confirm 2 C engineers available for full 35 days
- [ ] **Architecture Review**: Both engineers read and sign off on:
  - `docs/ARCHITECTURE.md` (system design overview)
  - `docs/PHASE-2A-EXECUTION.md` (detailed task breakdown)
  - `docs/delta-query.md` (decision rationale)
- [ ] **Environment Setup**: Both engineers have:
  - Meson 1.0+ (`meson --version`)
  - Ninja (`ninja --version`)
  - Clang-format 18 (`clang-format --version | grep "18."`)
  - Valgrind + ASan tools (`valgrind --version`, `clang -fsanitize=address`)
- [ ] **Repository Access**: Both engineers can push to feature branches
- [ ] **Create GitHub Issue #X (Phase 2A)**: Link to this checklist + execution roadmap
- [ ] **Baseline Verification**: `meson test -C build` passes (20/20 tests)

---

## Day 0 Morning (Design Phase Start)

### Engineer 1 Tasks

- [ ] Create feature branch: `feature/phase2a-columnar-backend`
- [ ] Create task: header refactoring design (4 hours)
  - [ ] Sketch `wirelog/exec_plan.h` types to extract from `dd_ffi.h`
  - [ ] List Rust FFI declarations to delete from `dd_ffi.h`
  - [ ] Write pseudo-code for `backend.h` vtable
  - [ ] Document in `.omc/plans/header-refactoring.md`
- [ ] Create task: memory architecture spec (3 hours)
  - [ ] Arena allocator design (pseudo-code)
  - [ ] Document in `.omc/plans/memory-architecture.md`
  - [ ] Get E2 sign-off

### Engineer 2 Tasks

- [ ] Create feature branch: (same as E1, or diverge for parallel work)
- [ ] Create task: evaluator loop design (4 hours)
  - [ ] Write semi-naive evaluation algorithm (pseudo-code)
  - [ ] Sketch main loop structure
  - [ ] Document in `.omc/plans/evaluator-design.md`
  - [ ] Get E1 sign-off
- [ ] Create task: operator interface design (3 hours)
  - [ ] Define operator function signatures (SCAN, FILTER, MAP, JOIN, ANTIJOIN)
  - [ ] Document in `.omc/plans/operator-interface.md`
  - [ ] Get E1 sign-off

### Both Engineers

- [ ] **Sync (30 min)**: Review design docs, resolve any impedance mismatches
- [ ] **Commit Design Phase Output**: Create PR with design docs in `.omc/plans/`
- [ ] **Get Architect Sign-Off**: Circulate design PR; architect reviews for correctness
- [ ] **Proceed to Days 2-10** (pending design approval)

---

## Days 2-10: Core Infrastructure

### E1: Backend & Memory Layer

- [ ] **Days 2-3**: Header Refactoring
  - [ ] Create `wirelog/exec_plan.h` with extracted types
  - [ ] Delete Rust declarations from `dd_ffi.h`
  - [ ] Update `backend.h` includes
  - [ ] Verify: `meson compile -C build` still passes
  - [ ] Commit: "refactor: split dd_ffi.h → exec_plan.h"

- [ ] **Days 4-5**: Arena Allocator
  - [ ] Implement `wl_arena_t` + allocation/free functions
  - [ ] Create `backend/memory.c` with arena lifecycle
  - [ ] Write unit test: `tests/test_arena.c`
  - [ ] Run: `meson test -C build` (new test passes)
  - [ ] Commit: "feat: arena allocator for memory management"

- [ ] **Days 6-8**: Evaluator Loop Skeleton
  - [ ] Create `backend/columnar.c` with main iteration loop
  - [ ] Implement convergence check (no deltas produced)
  - [ ] Stub out operator calls (call SCAN, pass to FILTER, etc.)
  - [ ] Test on trivial 1-rule program
  - [ ] Commit: "feat: evaluator loop skeleton for columnar backend"

- [ ] **Days 9-10**: Session Wiring
  - [ ] Implement `wl_compute_backend_columnar` vtable
  - [ ] Wire session API to columnar backend
  - [ ] Integration test: load program via CLI, execute
  - [ ] Commit: "feat: columnar backend vtable integration"

### E2: Operators

- [ ] **Days 2-4**: SCAN Operator
  - [ ] Implement `wl_op_scan_t` (load base facts into relation)
  - [ ] Create `backend/operators.c`
  - [ ] Unit test: `tests/test_scan_operator.c`
  - [ ] Verify facts load correctly
  - [ ] Commit: "feat: SCAN operator (base fact loading)"

- [ ] **Days 5-6**: FILTER & MAP Operators
  - [ ] Implement RPN expression evaluation
  - [ ] FILTER operator (predicate filtering)
  - [ ] MAP operator (projection + arithmetic)
  - [ ] Unit tests: `tests/test_filter_operator.c`, `tests/test_map_operator.c`
  - [ ] Commit: "feat: FILTER and MAP operators"

- [ ] **Days 7-8**: UNION & Deduplication
  - [ ] Implement UNION operator (concatenate relations)
  - [ ] Implement deduplication (merge duplicate rows)
  - [ ] Unit test: `tests/test_union_dedup.c`
  - [ ] Commit: "feat: UNION and deduplication operators"

- [ ] **Days 9-10**: Non-Recursive Aggregation
  - [ ] Implement COUNT, SUM, MIN, MAX operators (non-recursive only)
  - [ ] Unit tests: `tests/test_aggregate_nonrec.c`
  - [ ] Test: aggregation semantics correct
  - [ ] Commit: "feat: non-recursive aggregation operators"

### Checkpoint: End of Day 10
- [ ] Simple TC (4 rules, 5 facts) executes end-to-end
- [ ] Both E1 and E2 work integrates without conflicts
- [ ] CI passes (20/20 baseline tests + 8 new operator unit tests)

---

## Days 11-16: CRITICAL PATH — Recursion

### E1: Delta Propagation

- [ ] **Days 11-12**: Semi-Naive Delta Tracking
  - [ ] Implement delta data structures (delta_R per relation)
  - [ ] Implement delta producer (output from SCAN/JOIN)
  - [ ] Unit test: `tests/test_delta_propagation.c`
  - [ ] Verify delta tracking correct on trivial recursion
  - [ ] Commit: "feat: semi-naive delta tracking"

- [ ] **Days 13-14**: Multi-Stratum Execution
  - [ ] Implement stratum loop (execute strata in order)
  - [ ] Delta marshaling between strata
  - [ ] Test: 2-stratum program with negation
  - [ ] Commit: "feat: multi-stratum execution with deltas"

- [ ] **Day 15**: Monotone Aggregation (Recursion)
  - [ ] Implement MIN/MAX operators for recursive strata
  - [ ] Unit test: `tests/test_monotone_agg_recursive.c`
  - [ ] Test: MIN/MAX propagate correctly across iterations
  - [ ] Commit: "feat: monotone aggregation in recursive strata"

- [ ] **Day 16**: Debug/Validation
  - [ ] Run Polonius first 50 iterations
  - [ ] Debug any divergence from DD oracle
  - [ ] Commit: "test: Polonius convergence validation (first 50 iterations)"

### E2: Complex Joins

- [ ] **Days 11-13**: Hash Join (2-way, 3-way)
  - [ ] Implement hash join operator
  - [ ] Hash table construction + probing
  - [ ] Unit tests: `tests/test_hash_join.c`
  - [ ] Verify cardinality correctness on 2-way and 3-way joins
  - [ ] Commit: "feat: hash join operator"

- [ ] **Days 14-15**: ANTIJOIN (Negation)
  - [ ] Implement ANTIJOIN operator (set difference)
  - [ ] Stratified execution support
  - [ ] Unit test: `tests/test_antijoin.c`
  - [ ] Test: negation-stratified program correct
  - [ ] Commit: "feat: ANTIJOIN operator for negation"

- [ ] **Day 16**: Multi-Way Joins Prep
  - [ ] Test 4-way, 6-way, 8-way joins (DOOP preparation)
  - [ ] Profile hash table performance
  - [ ] Commit: "test: multi-way join cardinality validation"

### Checkpoint: End of Day 16
- [ ] TC + Reach benchmarks pass oracle (identical to DD)
- [ ] Polonius first 100 iterations converge correctly
- [ ] 15 new unit tests in CI
- [ ] Critical path on track

---

## Days 17-22: Features

### E1: Sessions & Snapshots

- [ ] **Days 17-19**: Session API (multi-operation)
  - [ ] Implement `session_insert(facts)` → adds to EDB
  - [ ] Implement `session_step()` → execute strata to convergence
  - [ ] State persistence across operations
  - [ ] Integration test: `tests/test_session_incremental.c`
  - [ ] Commit: "feat: multi-operation session API"

- [ ] **Days 20-22**: Snapshots
  - [ ] Implement `session_snapshot()` → extract results at stratum boundary
  - [ ] Per-stratum snapshots
  - [ ] Unit test: `tests/test_session_snapshot.c`
  - [ ] Verify snapshot correctness
  - [ ] Commit: "feat: session snapshot at stratum boundaries"

### E2: Remaining Operators

- [ ] **Days 17-19**: SEMIJOIN & REDUCE
  - [ ] Implement SEMIJOIN (projection with join condition)
  - [ ] Implement REDUCE (custom aggregation)
  - [ ] Unit tests
  - [ ] Commit: "feat: SEMIJOIN and REDUCE operators"

- [ ] **Days 20-22**: Buffer Management & Encoding
  - [ ] Efficient row encoding (pack i64 columns)
  - [ ] Buffer pool management
  - [ ] Memory profiling
  - [ ] Commit: "perf: buffer management and tuple encoding"

### Checkpoint: End of Day 22
- [ ] CC, SSSP, SG benchmarks pass oracle
- [ ] Session API working (insert/step/snapshot)
- [ ] 8 new feature tests in CI

---

## Days 23-30: Validation & Polish

### Both Engineers: Oracle Validation (SHARED)

- [ ] **Days 23-25**: Run all 15 benchmarks vs DD oracle
  - [ ] Columnar backend: execute all 15 benchmarks
  - [ ] Diff with DD output (tuple-by-tuple)
  - [ ] Create `.omc/research/oracle-comparison.md` (results + any divergences)
  - [ ] Commit: "test: oracle validation all 15 benchmarks"

- [ ] **Day 25 CHECKPOINT** ⚠️ CRITICAL DECISION POINT
  - [ ] **If GREEN**: All 15 pass oracle → proceed to Days 26-30 (Polonius + DOOP + Rust removal)
  - [ ] **If YELLOW**: 5 non-recursive pass, Polonius/DOOP fail → decide scope reduction
    - [ ] Document decision in `.omc/plans/day25-decision.md`
    - [ ] Replan Phase 2A scope (ship non-recursive backend; defer recursion to Phase 2B)
  - [ ] **If RED**: <5 benchmarks pass → escalate to team lead

### If GREEN PATH (Days 26-30)

- [ ] **Days 27-28**: Polonius + DOOP Correctness
  - [ ] Run Polonius (1,487 iterations) to completion
  - [ ] Run DOOP (136 rules, 8-way joins) to completion
  - [ ] Oracle validation: identical to DD
  - [ ] Commit: "test: Polonius and DOOP oracle validation complete"

- [ ] **Days 29-30**: Memory Profiling & ASan
  - [ ] Run Valgrind on Polonius (detect memory leaks)
  - [ ] Run ASan on all benchmarks
  - [ ] Profile peak heap usage (target <20GB)
  - [ ] Fix any leaks detected
  - [ ] Commit: "test: memory safety validation (ASan + Valgrind clean)"

### If YELLOW PATH (Days 26-30)

- [ ] Document Phase 2A as "non-recursive columnar backend"
- [ ] Create Phase 2B plan: "recursive support + multi-way joins" (10 days)
- [ ] Polish non-recursive backend
- [ ] Commit: "feat: Phase 2A non-recursive backend (recursive deferred to Phase 2B)"

### Checkpoint: End of Day 30
- [ ] All benchmarks (or non-recursive subset) oracle-validated
- [ ] Zero memory leaks (Valgrind + ASan clean)
- [ ] Ready for Rust removal

---

## Days 31-35: Rust Removal & Final Validation

### Both Engineers: Rust Elimination (SHARED)

- [ ] **Day 31**: Delete Rust Code
  - [ ] Remove entire `rust/wirelog-dd/` directory
  - [ ] Verify: `du -sh rust/wirelog-dd/` → gone
  - [ ] Update `.gitignore` if needed
  - [ ] Commit: "refactor: delete Rust DD executor (5045 LOC removed)"

- [ ] **Day 32**: Update Build System
  - [ ] Remove Rust from `meson.build` (dependencies, rules)
  - [ ] Remove `Cargo.toml`, `Cargo.lock`
  - [ ] Verify: `meson compile -C build` succeeds (C only)
  - [ ] Check build time <5 seconds
  - [ ] Commit: "refactor: remove Rust from build system"

- [ ] **Days 33-34**: Update FFI Layer
  - [ ] Delete Rust foreign function stubs from `ffi/dd_ffi.c`
  - [ ] Update `backend.h` to remove Rust backend references
  - [ ] Ensure CLI still works (defaults to columnar backend)
  - [ ] Run: `meson test -C build` (all 20+ tests pass)
  - [ ] Commit: "refactor: remove Rust FFI layer"

- [ ] **Day 35**: Final Integration & Docs
  - [ ] Final integration test: CLI loads program, executes, outputs results
  - [ ] Update README.md (if needed) to reflect Rust removal
  - [ ] Update CHANGELOG.md with Phase 2A summary
  - [ ] Create release notes for Phase 2A
  - [ ] Commit: "docs: Phase 2A completion notes and CHANGELOG update"

### Final Checkpoint: End of Day 35 ✅

- [ ] **Correctness**: All 15 benchmarks (or non-recursive subset) identical to Phase 1 DD oracle
- [ ] **Rust Removal**: Zero Rust LOC; `grep -r "fn " rust/` returns nothing
- [ ] **Build**: Meson-only; compile time <5 sec
- [ ] **Memory Safety**: Zero ASan/Valgrind errors
- [ ] **Performance**: Execution time <3x DD baseline
- [ ] **Tests**: 20+ unit tests + all benchmarks pass
- [ ] **Documentation**: ARCHITECTURE.md, delta-query.md, PHASE-2A-EXECUTION.md all updated
- [ ] **Commits**: Atomic, logical commits with clear messages
- [ ] **CI**: GitHub Actions passes all checks

---

## Success Metrics

### Daily Progress Tracking

| Milestone | Target Date | Status | Notes |
|-----------|-------------|--------|-------|
| Design phase approved | Day 0 EOD | ⬜ | E1 + E2 sign-off |
| Baseline operators working | Day 10 EOD | ⬜ | TC runs end-to-end |
| Recursion on 4-node TC | Day 16 EOD | ⬜ | TC + Reach pass oracle |
| Sessions & snapshots | Day 22 EOD | ⬜ | Multi-op sessions work |
| Day 25 checkpoint decision | Day 25 EOD | ⬜ | Green/Yellow/Red path |
| All benchmarks validated | Day 30 EOD | ⬜ | Oracle match complete |
| Rust removed | Day 35 EOD | ⬜ | Zero Rust LOC |

### Quality Gates

- [ ] Zero compiler warnings (C11 strict)
- [ ] Zero Valgrind/ASan errors
- [ ] All unit tests pass
- [ ] All benchmarks pass oracle
- [ ] Commit messages follow convention
- [ ] Code style passes clang-format

---

## Communication & Escalation

### Daily Standup (15 min)

**Schedule**: 9:30 AM (or async Slack if time zones differ)

**Format**:
- E1: What I did yesterday, what I'm doing today, blockers
- E2: What I did yesterday, what I'm doing today, blockers
- Both: Clarify any shared work, adjust plan if needed

### Blocker Escalation

**If blocked**:
1. Describe blocker in Slack (with context)
2. Tag architect or team lead
3. Expect response within 2 hours
4. Switch to unblocked task if needed

**Examples**:
- "SCAN operator interface undefined" → E2 waits for E1 design doc
- "Hash join test fails; diverges from DD on 8-way" → Escalate to architect
- "Memory arena hits peak >20GB on Polonius" → Escalate for memory arch review

### Weekly Checkpoints

- **Monday**: Week plan confirmation
- **Wednesday**: Mid-week progress review
- **Friday**: Weekly review + next week preview

---

## References

- **Decision Document**: `docs/delta-query.md` (RALPLAN consensus)
- **Execution Roadmap**: `docs/PHASE-2A-EXECUTION.md` (detailed task breakdown)
- **Architecture**: `docs/ARCHITECTURE.md` (system design)
- **GitHub Issue**: #80 (Full Recursive Delta Support)
- **Previous Work**: `MONOTONE_AGGREGATION.md` (Phase 1 innovation)

---

## Notes for Engineers

### Commit Message Convention

```
feat: [component] brief description

Detailed explanation (if needed). Reference issue #80.

[Commit per task; keep commits atomic and logical]
```

### Testing Checklist (Per Commit)

Before pushing:
1. [ ] `meson compile -C build` passes
2. [ ] `meson test -C build` passes (all tests)
3. [ ] New test added (if new feature)
4. [ ] `clang-format --style=file -i <modified_files>`
5. [ ] No compiler warnings

### Code Review

- E1 reviews E2's code (and vice versa) before merge
- Architect reviews larger architectural changes
- Merge only with sign-off

---

**Prepared by**: wirelog architecture team
**Date**: 2026-03-04
**Status**: Ready for execution upon engineer assignment
