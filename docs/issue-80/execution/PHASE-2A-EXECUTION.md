# Phase 2A Execution Roadmap

**Issue**: #80 (Full Recursive Delta Support)
**Architecture Decision**: Option B (Complete Columnar Backend in C11)
**Status**: Ready for execution (all 5 clarifications resolved)
**Timeline**: 35 days with 2 C engineers
**Committed Outcome**: Rust-free columnar backend with full recursive query support

---

## Clarifications Resolved ✅

### 1. Header Refactoring: `dd_ffi.h` Split

**Action**:
1. Create `wirelog/exec_plan.h` containing:
   - `wl_ffi_plan_t` (renamed to `wl_exec_plan_t` OR kept as-is for backward compat)
   - `wl_ffi_stratum_plan_t`
   - `wl_ffi_relation_plan_t`
   - `wl_ffi_op_t`
   - `wl_ffi_expr_buffer_t`

2. Update `wirelog/backend.h`:
   ```c
   // OLD: #include "ffi/dd_ffi.h"
   // NEW: #include "exec_plan.h"
   ```

3. Delete from `dd_ffi.h`:
   - All Rust FFI declarations (`wl_dd_worker_create`, `wl_dd_execute`, etc.)
   - Rust opaque struct definitions

**Owner**: Engineer 1 (Day 0)
**Deliverable**: `exec_plan.h` created, includes updated, DD compilation still passes
**Verification**: `meson compile -C build` succeeds; both DD and backward-compat paths work

---

### 2. Memory Management: Arena per Epoch

**Decision**: Allocate arena per fixed-point iteration; free entire arena after convergence.

**Implementation**:
```c
// In evaluator loop (pseudo-code)
for (iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
    wl_arena_t *arena = wl_arena_create(256 * 1024 * 1024); // 256MB per iteration

    // All relation delta allocations use arena
    wl_relation_t *delta_R = wl_relation_create_in_arena(arena, ...);
    // ... propagate deltas ...

    // Check convergence
    if (no_deltas_produced) break;

    // Free entire arena
    wl_arena_free(arena);
}
```

**Why**:
- Simpler than reference-counted relations
- Predictable GC (no fragmentation)
- Polonius peak: ~15GB total (1,487 iterations × ~10MB) manageable

**Owner**: Engineer 1 (Days 0-1 design, Days 2-5 implementation)
**Test**: Valgrind on Polonius; verify peak heap ~15GB, no leaks

---

### 3. `dd_marshal.c` Fate: Keep and Refactor

**Decision**: Rename to `exec_marshal.c`; retain generic plan marshaling logic; delete Rust-specific parts.

**Keep**:
- `wl_ffi_plan_unmarshal()`
- `wl_ffi_op_unmarshal()`
- `wl_ffi_expr_buffer_apply()`

**Delete**:
- Rust worker creation helpers
- DD-specific execution wrappers

**Owner**: Engineer 1 (Days 1-3)
**Verification**: Columnar backend can unmarshal `wl_ffi_plan_t` without DD dependency

---

### 4. Hand-Computed Oracle Tests

**3 Test Programs** (added to `tests/` directory):

#### Test 1: `test_oracle_aggregation.c`
```c
// Datalog:
// count(R) :- base(R).
// select(A, count(X)) :- base(X, A), count(R).

// Input facts:
// base(1, a).
// base(1, b).
// base(2, c).

// Expected output (manually verified):
// count: 3
// select(1, 2).
// select(2, 1).
```

**Owner**: Engineer 2 (Days 9-10)
**Verification**: Both DD and columnar produce identical output

#### Test 2: `test_oracle_transitive_closure.c`
```c
// Datalog:
// path(X, Y) :- edge(X, Y).
// path(X, Z) :- path(X, Y), edge(Y, Z).

// Input edges: (1,2), (2,3), (3,4)

// Expected output (6 tuples):
// path(1,2). path(2,3). path(3,4).
// path(1,3). path(2,4).
// path(1,4).
```

**Owner**: Engineer 2 (Days 9-10)
**Verification**: Columnar backend matches manual enumeration

#### Test 3: `test_oracle_mutual_recursion.c`
```c
// Datalog:
// a(X) :- b(X).
// b(X) :- base(X), !a(X).

// Input: base(1). base(2).

// Expected output (manual fixed-point):
// a(X): {} (empty)
// b(X): {1, 2}
```

**Owner**: Engineer 2 (Days 9-10)
**Verification**: Negation-stratified execution correct

---

### 5. Day 25 Scope Decision Protocol

**GREEN PATH** (High confidence):
- TC, Reach, CC, SSSP, SG, Bipartite all pass oracle
- Polonius first 100 iterations converge correctly
- **Action**: Proceed to Days 26-35 for final validation; aim for complete Phase 2A

**YELLOW PATH** (Reduced scope):
- 5 non-recursive benchmarks pass oracle
- Polonius/DOOP show issues in multi-relation recursion
- **Action**: Release Phase 2A with non-recursive columnar backend only
- **Timeline impact**: Phase 2A ships on schedule; recursive support deferred to Phase 2B (est. 10 days)

**RED PATH** (Escalate):
- Fewer than 5 benchmarks pass
- Correctness diverges from DD
- **Action**: Escalate to team lead; consider fallback to Option A (DD backend for Phase 2A)

**Decision Authority**: Project lead + architect (Day 25 joint decision)

---

## Phase 2A Detailed Task Breakdown

### Days 0-1: Design Phase (SHARED)

| Task | Owner | Subtasks | Est. Hours |
|------|-------|----------|-----------|
| Header refactoring design | E1 | Split `dd_ffi.h`, update includes, verify DD still builds | 4h |
| Memory architecture spec | E1 | Arena allocator design, lifecycle documentation, pseudo-code | 3h |
| Evaluator loop design | E2 | Semi-naive algorithm, delta propagation, convergence checks | 4h |
| Operator interface design | E2 | SCAN, FILTER, MAP, JOIN, ANTIJOIN signatures | 3h |
| **Deliverable** | Both | Design docs + header stubs | **14h total** |

---

### Days 2-10: Core Infrastructure

#### Engineer 1 Track: Backend & Memory
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 2-3 | Create `exec_plan.h`, split `dd_ffi.h` | Header files, DD builds | `meson compile -C build` passes |
| 4-5 | Implement arena allocator | `wl_arena_t` + allocation/free | Unit tests pass; Valgrind clean |
| 6-8 | Build evaluator loop skeleton | Main iteration loop, convergence check | Compiles; trivial program runs |
| 9-10 | Session wiring + backend vtable | Columnar backend implements `wl_compute_backend_t` | Integration test loads program |

#### Engineer 2 Track: Operators
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 2-4 | SCAN operator (load base facts) | `wl_op_scan_t`, execute function | Unit test: loads facts correctly |
| 5-6 | FILTER + MAP operators | Predicate evaluation (RPN), transformations | Unit tests: correct filtering/mapping |
| 7-8 | UNION + deduplication | Relation consolidation | Unit tests: dedup removes duplicates |
| 9-10 | Non-recursive aggregation (COUNT, SUM, MIN, MAX) | Aggregation ops for non-recursive strata | Unit tests: correct aggregation |

**Checkpoint (End of Day 10)**: Simple TC (4 rules, 5 facts) executes end-to-end via columnar backend.

---

### Days 11-16: CRITICAL PATH — Recursion

#### Engineer 1 Track: Delta Propagation
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 11-12 | Semi-naive delta tracking | Delta data structures, delta producer API | Unit tests: delta propagation correct |
| 13-14 | Multi-stratum execution with deltas | Stratum ordering, delta marshaling between strata | Test: 2-stratum program runs |
| 15 | Monotone aggregation in recursion | MIN/MAX operators on recursive strata | Test: MIN/MAX recursion correct |
| 16 | Debug Polonius issues (if needed) | Debugging, correctness validation | Polonius first 50 iterations pass |

#### Engineer 2 Track: Complex Joins
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 11-13 | Hash join (2-way, 3-way) | Join operator with hash table | Unit tests: join cardinality correct |
| 14-15 | ANTIJOIN (negation operator) | Anti-join + stratified execution | Test: negation-stratified program correct |
| 16 | Multi-way joins (8-way DOOP prep) | Chained joins, predicate pushdown | Test: 8-way join cardinality |

**Checkpoint (End of Day 16)**: TC + Reach benchmarks pass oracle. Polonius passes first 100 iterations.

---

### Days 17-22: Features

#### Engineer 1 Track: Sessions & Snapshots
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 17-19 | Session API (insert, step, snapshot) | Multi-operation sessions, state persistence | Integration test: session persistence |
| 20-22 | Snapshot correctness at stratum boundaries | Per-stratum snapshots, delta capture | Test: snapshot matches actual relations |

#### Engineer 2 Track: Remaining Operators & Optimization
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 17-19 | SEMIJOIN, REDUCE operators | Projection, aggregation variants | Unit tests: operators correct |
| 20-22 | Buffer management + tuple encoding | Efficient row storage, memory packing | Profile: memory usage baseline |

**Checkpoint (End of Day 22)**: CC, SSSP, SG benchmarks pass oracle.

---

### Days 23-30: Validation & Polish

#### SHARED: Oracle Validation
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 23-25 | Run all 15 benchmarks against DD oracle | Tuple-by-tuple comparison, diff logs | All 15 benchmarks: identical output |
| 24-26 | **DAY 25 CHECKPOINT** | Scope decision (Green/Yellow/Red path) | Team decision: proceed or defer recursion |

**If GREEN PATH**:
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 27-28 | Polonius + DOOP correctness (1,487 iterations) | Full recursion on complex programs | Both benchmarks: oracle match |
| 29-30 | Memory profiling + ASan cleanup | Leak detection, peak heap tracking | Valgrind: zero leaks; peak <20GB |

**If YELLOW PATH**:
- Days 27-30: Polish non-recursive backend; document Phase 2B plan
- Phase 2A ships with non-recursive support only (recursive deferred to Phase 2B)

---

### Days 31-35: Rust Removal & Final Validation

#### SHARED: Rust Elimination
| Days | Task | Deliverable | Verification |
|------|------|-------------|--------------|
| 31 | Delete Rust source code (5,045 LOC) | `rm -rf rust/wirelog-dd/` | Rust directory gone |
| 32 | Update Cargo.toml + build system | Remove Rust dependencies from Meson | `meson compile` works; zero Rust LOC |
| 33-34 | Update FFI layer (remove Rust stubs) | Delete Rust foreign functions, update C declarations | Build succeeds; CLI still works |
| 35 | Final integration testing + docs | All 15 benchmarks still pass; update README | Release notes for Phase 2A |

**Final Checkpoint (End of Day 35)**:
- ✅ Zero Rust LOC remaining
- ✅ All 15 benchmarks pass oracle (or non-recursive subset if Yellow path)
- ✅ Build time <5 seconds
- ✅ Zero ASan/Valgrind errors
- ✅ Performance within 3x DD baseline
- ✅ Phase 2A ready to ship

---

## Risk Mitigation by Phase

### Days 0-10: Setup & Operators
| Risk | Mitigation |
|------|-----------|
| Header refactoring delays integration | Pre-design header structure; E1 owns Day 0 atomically |
| Arena allocator bugs | Implement + unit test before Day 6 core integration |
| Operator interface mismatch | Joint E1/E2 sync (daily 30min) to align vtable signatures |

### Days 11-16: CRITICAL RECURSION
| Risk | Mitigation |
|------|-----------|
| Semi-naive delta propagation broken | Implement hand-computed TC test (day 9); validate delta on trivial recursion first |
| Multi-relation mutual recursion (Polonius) | Run Polonius in stages: 10 → 50 → 100 → 1487 iterations |
| Hash join bugs on 8-way DOOP | Fuzz testing with random join cardinalities (day 14-15) |

### Days 17-30: Validation
| Risk | Mitigation |
|------|-----------|
| Memory leak on Polonius's 1,487 iterations | Valgrind after Day 28; if leak found, add arena boundary tracing |
| Oracle disagreement | Run DD and columnar in lockstep; capture first divergence with provenance |
| Day 25 checkpoint: recursion broken | Decide Yellow path early; don't force recursion past Day 25 |

### Days 31-35: Rust Removal
| Risk | Mitigation |
|------|-----------|
| Rust code lingering in dependencies | Grep for `dd_`, `wl_dd_` after deletion; ensure Cargo.lock updated |
| Build system still references Rust | Meson + CI validates zero Rust in final build |

---

## Success Criteria (Phase 2A Gates)

### MUST HAVE
- ✅ All 15 benchmarks (or non-recursive subset) produce identical tuple output as DD
- ✅ Zero Rust LOC remaining (complete source deletion + build verification)
- ✅ Meson-only build, <5 second compile time
- ✅ Zero ASan/Valgrind errors on largest benchmark (100 iterations)
- ✅ Single-worker execution time within 3x DD baseline

### SHOULD HAVE
- ✅ All 15 benchmarks including Polonius + DOOP (if Green path)
- ✅ Multi-operation sessions (insert/step/insert/step) maintain oracle correctness
- ✅ Snapshot support at stratum boundaries

### NICE TO HAVE
- ✅ Performance within 1.5x DD (targets Phase 3)
- ✅ Memory efficiency comparable to DD (targets Phase 3)

---

## Phase 2B (Deferred if Yellow Path)

If Day 25 checkpoint shows recursion issues:

| Item | Plan |
|------|------|
| Scope | Add recursive query support to non-recursive columnar backend |
| Timeline | 10 additional days (est.) |
| Deliverable | Polonius + DOOP pass oracle; ship updated Phase 2A |
| Trigger | Yellow path decision on Day 25 |

---

## Execution Checklist

**Before Day 0 (NOW)**:
- [ ] Resolve 5 clarifications (DONE ✅)
- [ ] Assign 2 C engineers (confirm 35-day availability)
- [ ] Review this roadmap; architect sign-off
- [ ] Create GitHub issue tracking Phase 2A tasks (link to this doc)

**Day 0 Morning**:
- [ ] Team sync: review design phase tasks
- [ ] E1: Start header refactoring design
- [ ] E2: Start evaluator loop design

**Days 2-35**:
- [ ] Daily 30-min sync (async written updates if needed)
- [ ] Day 25: Joint checkpoint decision
- [ ] Day 35: Release checklist

---

## References

- **Decision Doc**: `docs/delta-query.md` (Version 2)
- **Issue #80**: Full Recursive Delta Support
- **Issue #69**: Recursive aggregation (completed Phase 1)
- **ARCHITECTURE.md**: Phase roadmap
- **CLAUDE.md**: C11-first principle

---

**Prepared by**: wirelog architecture team
**Date**: 2026-03-04
**Status**: Ready for execution
**Next Milestone**: Assign engineers and begin Day 0
