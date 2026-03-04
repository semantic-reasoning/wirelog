# Phase 2A: 5 Required Clarifications

**Issue**: #80 (Full Recursive Delta Support)
**Decision**: Option B (Complete Columnar Backend in C11)
**Status**: All 5 clarifications resolved and documented below
**Date**: 2026-03-04

---

## Overview

The RALPLAN deliberate consensus identified 5 clarifications required before Phase 2A execution begins. All clarifications have been resolved with concrete decisions and implementation guidance below.

---

## Clarification #1: Header Split for `dd_ffi.h`

**Status**: ✅ RESOLVED

### Problem
- `backend.h` includes `dd_ffi.h` (line 30)
- `dd_ffi.h` contains both plan types (needed by columnar) and Rust FFI declarations (to be deleted)
- Cannot delete `dd_ffi.h` entirely; must split it

### Resolution

**Create** `wirelog/exec_plan.h`:
```c
#ifndef WL_EXEC_PLAN_H
#define WL_EXEC_PLAN_H

#include <stdint.h>
#include <stdbool.h>

// Backend-agnostic plan representation
// Used by both DD and columnar backends

typedef enum {
    WL_FFI_OP_SCAN,
    WL_FFI_OP_FILTER,
    WL_FFI_OP_MAP,
    WL_FFI_OP_JOIN,
    WL_FFI_OP_ANTIJOIN,
    WL_FFI_OP_AGGREGATE,
    WL_FFI_OP_UNION,
    WL_FFI_OP_SEMIJOIN,
} wl_ffi_op_type_t;

typedef struct {
    // Operator type and parameters
    wl_ffi_op_type_t type;
    uint32_t input_count;
    uint32_t output_width;
    // ... operator-specific fields
} wl_ffi_op_t;

typedef struct {
    // Relation metadata
    uint32_t id;
    const char *name;
    uint32_t column_count;
    // ... relation fields
} wl_ffi_relation_plan_t;

typedef struct {
    // Stratum execution plan
    uint32_t stratum_id;
    uint32_t rule_count;
    const wl_ffi_op_t *ops;
    // ... stratum fields
} wl_ffi_stratum_plan_t;

typedef struct {
    // Expression buffer (RPN evaluation)
    const uint8_t *bytecode;
    uint32_t bytecode_len;
    // ... expr buffer fields
} wl_ffi_expr_buffer_t;

typedef struct {
    // Top-level query plan
    uint32_t relation_count;
    const wl_ffi_relation_plan_t *relations;
    uint32_t stratum_count;
    const wl_ffi_stratum_plan_t *strata;
    // ... plan fields
} wl_ffi_plan_t;

#endif // WL_EXEC_PLAN_H
```

**Update** `wirelog/backend.h`:
```c
// OLD:
// #include "ffi/dd_ffi.h"

// NEW:
#include "exec_plan.h"
```

**Delete from** `wirelog/ffi/dd_ffi.h`:
```c
// REMOVE these Rust FFI declarations:
// - wl_dd_worker_create()
// - wl_dd_execute()
// - wl_dd_free_worker()
// - wl_dd_worker_t opaque struct
// - All Rust-specific function prototypes
// - All Rust-specific type definitions

// KEEP: (now in exec_plan.h)
// - wl_ffi_plan_t
// - wl_ffi_op_t
// - wl_ffi_stratum_plan_t
// - wl_ffi_relation_plan_t
// - wl_ffi_expr_buffer_t
// - wl_ffi_op_type_t enum
```

### Implementation Notes

1. **Timing**: Days 2-3 of Phase 2A (E1 responsibility)
2. **Testing**: After split, verify:
   - DD backend still compiles with `backend.h` → `exec_plan.h` include
   - Columnar backend can include `exec_plan.h` without DD dependencies
   - All existing tests pass
3. **Backward Compatibility**: None required (internal headers only)
4. **Documentation**: Update ARCHITECTURE.md to reflect new exec_plan.h location

---

## Clarification #2: Memory Management Architecture

**Status**: ✅ RESOLVED

### Problem
Two viable approaches for managing relation memory during recursive evaluation:
- Option A: Arena per epoch (simple, proven)
- Option B: Reference-counted relations (flexible, complex)

### Resolution

**DECISION**: Arena per Epoch

**Rationale**:
- Simpler than reference-counting
- Proven pattern (Timely, Differential Dataflow, traditional dataflow systems)
- Polonius's 1,487 iterations with ~10MB per iteration = ~15GB total (manageable)
- Predictable GC behavior (no fragmentation)

### Implementation Design

```c
// wirelog/backend/memory.h
typedef struct {
    void *base;
    size_t capacity;
    size_t used;
} wl_arena_t;

wl_arena_t *wl_arena_create(size_t capacity);
void *wl_arena_alloc(wl_arena_t *arena, size_t size);
void wl_arena_reset(wl_arena_t *arena);
void wl_arena_free(wl_arena_t *arena);
```

```c
// wirelog/backend/columnar.c (pseudo-code)
for (uint32_t iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
    // Allocate arena for this iteration
    wl_arena_t *arena = wl_arena_create(256 * 1024 * 1024); // 256MB per iter

    // Execute all strata; all allocations use arena
    bool has_deltas = false;
    for (uint32_t stratum = 0; stratum < stratum_count; stratum++) {
        bool stratum_has_deltas = execute_stratum(strata[stratum], arena);
        has_deltas = has_deltas || stratum_has_deltas;
    }

    // Check convergence
    if (!has_deltas) {
        wl_arena_free(arena);
        break;
    }

    // Free entire arena for next iteration
    wl_arena_free(arena);
}
```

### Memory Profiling Goals

| Benchmark | Peak Heap | Target |
|-----------|-----------|--------|
| TC (4 nodes) | <10MB | Pass |
| Reach (graph) | <50MB | Pass |
| CC (connected components) | <100MB | Pass |
| SSSP (shortest path) | <200MB | Pass |
| SG (same generation) | <100MB | Pass |
| Polonius (1,487 iterations) | <20GB | Pass (includes intermediate) |
| DOOP (8-way joins) | <15GB | Pass |

### Testing

1. **Unit Test** (Days 4-5, E1):
   - `tests/test_arena.c`: allocation, reset, free operations
   - Stress test: allocate/reset 100x to verify no leaks

2. **Integration Test** (Days 27-30):
   - Run Valgrind on Polonius (1,487 iterations)
   - Verify peak heap <20GB
   - Zero memory leaks

### Decision Authority

E1 owns memory architecture; E2 consulted during operator design to ensure allocations fit arena model.

---

## Clarification #3: `dd_marshal.c` Fate

**Status**: ✅ RESOLVED

### Problem
- `dd_marshal.c` contains both generic plan marshaling logic AND Rust-specific helpers
- Cannot simply delete it (columnar backend needs marshaling)
- Must determine what to keep vs delete

### Resolution

**DECISION**: Keep and Refactor as `exec_marshal.c`

**Keep** (Generic, Rust-independent):
```c
// Plan type unmarshaling
int wl_ffi_plan_unmarshal(const uint8_t *bytes, size_t len, wl_ffi_plan_t *out);

// Operator unmarshaling
int wl_ffi_op_unmarshal(const uint8_t *bytes, wl_ffi_op_t *out);

// Expression evaluation (RPN buffer)
int wl_ffi_expr_buffer_apply(const wl_ffi_expr_buffer_t *expr,
                             const int64_t *row_data,
                             int64_t *result);

// Relation metadata unmarshaling
int wl_ffi_relation_plan_unmarshal(const uint8_t *bytes,
                                   wl_ffi_relation_plan_t *out);
```

**Delete** (Rust-specific):
```c
// Rust worker creation helpers
// - wl_ffi_create_rust_worker()
// - wl_ffi_dd_plan_to_rust()
// - wl_ffi_rust_execute_wrapper()
// - Any Rust FFI bridging code
```

### File Organization

**Before**:
```
wirelog/ffi/dd_marshal.c (785 LOC)
  ├── Generic marshaling (440 LOC) → keep
  ├── Rust-specific wrappers (345 LOC) → delete
```

**After**:
```
wirelog/ffi/exec_marshal.c (440 LOC)
  ├── Generic plan unmarshaling ✅
  ├── Operator unmarshaling ✅
  ├── Expression evaluation ✅
  └── (Rust helpers deleted) ✅
```

### Implementation

1. **Days 1-3, E1**: Rename `dd_marshal.c` → `exec_marshal.c`
2. Remove Rust-specific functions
3. Update meson.build to reference new filename
4. Verify: DD backend still works (uses unmarshal functions)
5. Verify: Columnar backend can unmarshal plans without DD

### Testing

Verify that columnar backend can:
```c
// Load a marshaled plan without Rust dependency
const uint8_t *plan_bytes = load_marshaled_plan(...);
wl_ffi_plan_t plan;
int rc = wl_ffi_plan_unmarshal(plan_bytes, plan_len, &plan);
assert(rc == 0);
// Plan is now usable by columnar backend
```

---

## Clarification #4: Hand-Computed Oracle Tests

**Status**: ✅ RESOLVED

### Problem
- DD and columnar backends could both have bugs in shared components (e.g., `dd_plan.c`)
- Oracle (comparing DD vs columnar) won't detect shared bugs
- Need independent correctness validation

### Resolution

**Create 3 hand-computed test programs** with manually-derived expected outputs:

### Test 1: Simple Aggregation

**File**: `tests/test_oracle_aggregation.c`

**Datalog Program**:
```datalog
count(R) :- base(R).
result(A, count(X)) :- base(X, A), count(R).
```

**Input Facts**:
```
base(1, a).
base(1, b).
base(2, c).
```

**Manual Derivation** (fixed-point):
1. count(R) ← count base facts = 3
   - count: {3}
2. result(A, count(X)) ← group base by A, count per group
   - For A=1: base facts are (1,a), (1,b) → count = 2
   - For A=2: base facts are (2,c) → count = 1
   - result: {(1, 2), (2, 1)}

**Expected Output**:
```
count: {3}
result: {(1, 2), (2, 1)}
```

**Verification**:
- Both DD and columnar must produce identical results
- Manual count confirms correctness

**Implementation**: Days 9-10, E2
```c
TEST_AGGREGATION {
    // Load program
    // Load input facts
    // Execute both DD and columnar backends
    // Compare output to expected manually-computed result
    // Assert equality
}
```

### Test 2: Linear Transitive Closure

**File**: `tests/test_oracle_transitive_closure.c`

**Datalog Program**:
```datalog
path(X, Y) :- edge(X, Y).
path(X, Z) :- path(X, Y), edge(Y, Z).
```

**Input Facts**:
```
edge(1, 2).
edge(2, 3).
edge(3, 4).
```

**Manual Derivation** (fixed-point, iteration by iteration):

Iteration 1:
- path(X,Y) ← edge facts
- path: {(1,2), (2,3), (3,4)}

Iteration 2:
- path(X,Z) ← path(X,Y), edge(Y,Z)
  - (1,2) + edge(2,Y) = (1,3) ✓
  - (2,3) + edge(3,Y) = (2,4) ✓
  - (3,4) + edge(4,Y) = ∅
- path accumulates: {(1,2), (2,3), (3,4), (1,3), (2,4)}

Iteration 3:
- (1,3) + edge(3,Y) = (1,4) ✓
- path: {(1,2), (2,3), (3,4), (1,3), (2,4), (1,4)}

Iteration 4:
- No new tuples produced
- Convergence ✓

**Expected Output** (6 tuples total):
```
path: {(1,2), (2,3), (3,4), (1,3), (2,4), (1,4)}
```

**Verification**:
- Manual enumeration confirms 6 distinct paths
- No spurious paths

### Test 3: Mutual Recursion with Negation

**File**: `tests/test_oracle_mutual_recursion.c`

**Datalog Program** (stratified):
```datalog
a(X) :- b(X).
b(X) :- base(X), !a(X).
```

**Input Facts**:
```
base(1).
base(2).
base(3).
```

**Manual Derivation** (stratified fixed-point):

Stratum 0: No rules depend on negation
- (empty)

Stratum 1: b(X) :- base(X), !a(X)
- Since a is empty (stratum 0), !a = {1, 2, 3}
- b(X) ← base(X) ∧ ¬a(X)
- b(X) ← base(X) (because a is empty)
- b: {1, 2, 3}

Stratum 2: a(X) :- b(X)
- a(X) ← b(X)
- a: {1, 2, 3}

Stratum 1 (recomputed with updated a):
- b(X) :- base(X), !a(X)
- b(X) ← base(X) ∧ ¬{1, 2, 3}
- b: {} (empty, because all base facts are in a)

Stratum 2 (recomputed):
- a(X) :- b(X)
- a: {} (empty)

Iteration 2 Stratum 1:
- b(X) :- base(X), !a(X)
- Since a is now empty: b: {1, 2, 3}

Stabilization issue detected! Let's recalculate using stratum-local fixed-point:

**Correct Stratification** (from stratify.h logic):
- a and b form mutual recursion → same SCC
- a negatively depends on itself (indirect via b) → NOT STRATIFIABLE per dd_plan.c validation

**Wait**: Re-check the program:
```
a(X) :- b(X).          // a depends on b (positive)
b(X) :- base(X), !a(X). // b depends on !a (negation)
```

Dependency graph: a → b, b → ¬a (negation edge within SCC)
→ NOT STRATIFIABLE (negation cycle)

**Alternative Test 3** (use stratifiable program):
```datalog
a(X) :- b(X).
b(X) :- base(X), !c(X).
c(X) :- fact(X).
```

Strata:
1. c(X) ← fact(X)
2. b(X) ← base(X) ∧ ¬c(X)
3. a(X) ← b(X)

**Input Facts**:
```
fact(2).
base(1).
base(2).
base(3).
```

**Manual Derivation**:
- Stratum 1: c: {2}
- Stratum 2: b ← {1, 3} (base minus c)
- Stratum 3: a ← {1, 3}

**Expected Output**:
```
a: {1, 3}
b: {1, 3}
c: {2}
```

### Implementation Schedule

| Test | Schedule | Duration | Owner |
|------|----------|----------|-------|
| test_oracle_aggregation.c | Days 9-10 | 2h | E2 |
| test_oracle_transitive_closure.c | Days 9-10 | 2h | E2 |
| test_oracle_stratified_negation.c | Days 9-10 | 2h | E2 |

**Acceptance Criteria**:
- All 3 tests compile and run
- Both DD and columnar backends produce expected output
- Results match manual derivations exactly
- CI passes with 20+ tests (including 3 oracle tests)

---

## Clarification #5: Day 25 Scope Decision Protocol

**Status**: ✅ RESOLVED

### Problem
- Day 25 is critical checkpoint
- Recursion (Polonius, DOOP) may not converge correctly
- Need clear decision protocol: proceed, reduce scope, or escalate

### Resolution

**Three Paths** (decided jointly by E1, E2, Project Lead, Architect):

### GREEN PATH (All 15 Benchmarks Pass)

**Criteria**:
- ✅ TC, Reach, CC, SSSP, SG, Bipartite (6 non-recursive) pass oracle
- ✅ Polonius first 100 iterations converge correctly
- ✅ DOOP 8-way joins produce correct cardinality
- ✅ No ASan/Valgrind errors in runs

**Decision**: Continue to Days 26-35
- Proceed with full Polonius (1,487 iterations)
- Proceed with full DOOP (136 rules)
- Proceed with full Rust removal
- **Timeline**: Phase 2A ships on schedule (Day 35)

**Action Items** (E1 + E2):
1. Continue Days 26-27: Full Polonius convergence
2. Continue Days 28-29: Full DOOP convergence
3. Days 30-35: Rust removal + final validation

### YELLOW PATH (Recursion Fails)

**Criteria**:
- ✅ TC, Reach, CC, SSSP, SG, Bipartite (6 non-recursive) pass oracle
- ⚠️ Polonius iterations >100 show divergence or memory issues
- ⚠️ DOOP 8-way join has bugs or cardinality mismatches

**Decision**: Reduce Phase 2A Scope
- Ship Phase 2A with **non-recursive columnar backend only**
- Defer recursive support to **Phase 2B** (10 additional days)
- Still achieve Rust removal goal ✅

**Rationale**:
- Recursion is complex (semi-naive evaluation, mutual recursion, deltas)
- Rushing to ship broken recursion is worse than shipping incomplete on schedule
- Phase 2B can focus entirely on recursion correctness

**Action Items** (E1 + E2):
1. Document decision in `.omc/plans/day25-decision.md`
2. Days 26-30: Polish non-recursive backend
   - Fix any unit test failures
   - Memory profiling on 6 non-recursive benchmarks
   - ASan/Valgrind validation
3. Days 31-35: Rust removal
4. **Phase 2A Deliverable**: Non-recursive columnar backend, Rust removed
5. **Phase 2B Plan**: 10 additional days for recursive support
   - Scheduled after Phase 2A ships

### RED PATH (Critical Failure)

**Criteria**:
- ❌ Fewer than 5 benchmarks pass oracle
- ❌ Correctness diverges from DD on basic queries (non-recursive)
- ❌ Fundamental algorithmic issue (e.g., hash join broken across all cases)

**Decision**: Escalate to Team Lead

**Action Items** (E1 + E2 + Project Lead + Architect):
1. Immediate escalation (Day 25 morning)
2. Root cause analysis (2-4 hours)
3. Options:
   - **Option A**: Continue Phase 2A but replan Days 26-35 for debugging (likely extends past Day 35)
   - **Option B**: Fallback to Option A from delta-query.md (defer columnar to Phase 3; use DD backend for Phase 2A)
   - **Option C**: Investigate whether core plan generation bug (shared by DD and columnar)
4. Team decision within 24 hours

---

## Day 25 Execution Protocol

### Pre-Checkpoint (Days 23-25)

**E1 + E2 (Shared)**:
- Execute all 15 benchmarks under both DD and columnar
- Generate diff output (oracle comparison)
- Document findings in `.omc/research/day25-oracle-comparison.md`:
  - Which benchmarks pass oracle (tuple match)
  - Which benchmarks fail oracle (list divergences)
  - Any ASan/Valgrind errors detected
  - Memory usage profiles

### Checkpoint Meeting (Day 25, 2 PM)

**Attendees**: E1, E2, Project Lead, Architect

**Agenda** (60 min):
1. **Oracle Comparison Review** (15 min)
   - Read `.omc/research/day25-oracle-comparison.md`
   - Discuss pass/fail categorization

2. **Path Assessment** (15 min)
   - Green: "All 15 pass, proceed full speed"
   - Yellow: "Recursion broken, reduce scope"
   - Red: "Fundamental issue, escalate"

3. **Decision** (15 min)
   - Team agrees on chosen path
   - Document in `.omc/plans/day25-decision.md`

4. **Next Steps** (15 min)
   - If Green: Days 26-30 plan (Polonius + DOOP + final validation)
   - If Yellow: Days 26-30 plan (polish non-recursive, prepare Phase 2B)
   - If Red: Escalation plan + timeline adjustment

### Day 25 Decision Document

**File**: `.omc/plans/day25-decision.md`

**Template**:
```markdown
# Day 25 Checkpoint Decision

**Date**: 2026-03-XX (Day 25)
**Path Chosen**: [Green | Yellow | Red]

## Benchmarks Status

| Benchmark | Status | Oracle Match | Notes |
|-----------|--------|--------------|-------|
| TC | ✅ | Yes | Passes baseline |
| Reach | ✅ | Yes | Passes baseline |
| CC | ✅ | Yes | Passes baseline |
| SSSP | ✅ | Yes | Passes baseline |
| SG | ✅ | Yes | Passes baseline |
| Bipartite | ✅ | Yes | Passes baseline |
| Polonius | [✅/❌] | [Yes/No] | [Details] |
| DOOP | [✅/❌] | [Yes/No] | [Details] |

## Decision Rationale

[1-2 paragraph explanation of why Green/Yellow/Red chosen]

## Next Steps (Days 26-35)

[Bulleted plan for chosen path]

## Blockers (if any)

[List any identified issues and mitigation]
```

### Communication (Day 25+)

- **If Green**: Announce to team "Proceeding to full Phase 2A"
- **If Yellow**: Announce "Phase 2A ships non-recursive; Phase 2B scheduled for recursion"
- **If Red**: Announce "Escalating; investigating root cause"

---

## Summary Table

| # | Clarification | Status | Decision | Implementation |
|---|----------------|--------|----------|-----------------|
| 1 | Header split `dd_ffi.h` | ✅ | Create `exec_plan.h`; split headers | Days 2-3, E1 |
| 2 | Memory management | ✅ | Arena per epoch | Days 4-5, E1 |
| 3 | `dd_marshal.c` fate | ✅ | Refactor as `exec_marshal.c` | Days 1-3, E1 |
| 4 | Oracle tests (3 programs) | ✅ | Aggregation, TC, Negation | Days 9-10, E2 |
| 5 | Day 25 decision protocol | ✅ | Green/Yellow/Red paths | Day 25 checkpoint |

---

## Pre-Phase 2A Architecture Review

Before Phase 2A begins (Day 0), the architect should review and sign off on:

1. **Header Refactoring** (Clarification #1)
   - `exec_plan.h` types are sufficient for columnar backend
   - No circular dependencies with `backend.h`

2. **Memory Architecture** (Clarification #2)
   - Arena allocator is appropriate for Polonius
   - No hidden memory management issues

3. **Marshal Layer** (Clarification #3)
   - Generic unmarshal functions work for both DD and columnar
   - No impedance mismatch in plan representation

4. **Oracle Tests** (Clarification #4)
   - 3 test programs capture essential correctness properties
   - No false positives or false negatives

5. **Decision Protocol** (Clarification #5)
   - Green/Yellow/Red criteria are clear
   - Decision authority is understood

**Architect Sign-Off**: Approval of design documents before Day 1 begins.

---

## References

- **Main Decision Document**: `docs/delta-query.md` (RALPLAN consensus)
- **Execution Roadmap**: `docs/PHASE-2A-EXECUTION.md` (35-day task breakdown)
- **Daily Checklist**: `docs/PHASE-2A-CHECKLIST.md` (engineer-focused reference)
- **Architecture**: `docs/ARCHITECTURE.md` (system design)
- **Issue #80**: Full Recursive Delta Support specification

---

**Status**: All clarifications resolved and ready for implementation
**Date**: 2026-03-04
**Ready for**: Day 0 kickoff upon architect sign-off
