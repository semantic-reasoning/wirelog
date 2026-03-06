# RALPLAN-DR: Phase 2A Revised -- Columnar Backend with Rust Removal

**Date**: 2026-03-04
**Mode**: DELIBERATE (strategic constraint shift: Rust removal is primary goal)
**Status**: DRAFT -- awaiting Architect and Critic review
**Supersedes**: `phase2a-dd-vs-columnar-consensus.md` (Option A recommendation)

---

## Constraint Shift Acknowledgment

The previous RALPLAN-DR recommended Option A (extend Rust DD) based on Principle P2
"Minimize Rust Surface Area" -- which treated Rust as tolerable in the short term.

**The constraint has changed.** Rust removal is now the primary strategic goal, not a
deferred preference. This fundamentally invalidates the previous recommendation because:

1. Option A (DD-based Phase 2A) adds ~500 LOC Rust that must be torn out later. Every
   line of Rust added is now classified as waste.
2. Option C (Tiered Hybrid) has Tier 1 building 300 LOC Rust as a "stepping stone" --
   but if the destination is "no Rust," the stepping stone leads nowhere.
3. The question is no longer "what ships Phase 2A fastest?" but "what is the fastest
   path to Phase 2A features AND zero Rust?"

This changes the decision calculus entirely. Time-to-market for Phase 2A features must
be measured against the total cost including eventual Rust removal, not just initial
feature delivery.

---

## Revised Principles

### P1: Rust Removal is the Primary Goal (NEW -- TOP PRIORITY)
The team has decided that Rust is not part of wirelog's long-term architecture.
Every engineering hour spent deepening the Rust codebase is waste. The correct
investment is to build the C11 replacement, even if it takes longer upfront.

### P2: Incremental Delivery Within the Columnar Path
Big-bang rewrites are still dangerous. But "incremental" now means incrementally
building the columnar backend -- NOT incrementally adding Rust features that will
be thrown away. Milestone-based delivery within the columnar path preserves the
ship-and-iterate cadence.

### P3: Correctness Through Oracle Testing
DD remains available as a correctness oracle during the transition. The columnar
backend is validated by running both backends on all 15 benchmark workloads and
diffing results tuple-by-tuple. DD is not removed until the oracle passes.

### P4: Backend Abstraction Enables Safe Transition
`wl_compute_backend_t` vtable and the full session API are already implemented.
The columnar backend slots in behind the same vtable. The DD backend can coexist
until the columnar backend passes all correctness checks. Zero API changes needed.

### P5: Resource Acceleration Shortens the Critical Path
With 2 C engineers working in parallel (one on core evaluator, one on operators
and aggregation), the 40-50 day single-engineer estimate compresses to ~25-30
days. Parallelization is possible because the evaluator core and operator library
have a clean interface boundary.

---

## Decision Drivers

### D1: Total Cost of Ownership (REVISED -- TOP PRIORITY)
Previously measured as "days to Phase 2A features." Now measured as "days to
Phase 2A features with zero Rust."

**Option A total cost**: ~25 days (Phase 2A) + ~40 days (Phase 3 columnar) +
~5 days (Rust removal/migration) = **~70 days** to Rust-free Phase 2A+3.

**Option B total cost**: ~25-30 days (with 2 engineers) to Phase 2A features
AND zero Rust simultaneously = **~25-30 days** to same end state.

Option B reaches the "Rust-free with all features" state in less than half the
time of the Option A path.

### D2: Wasted Engineering Effort
Every line of Rust written under Option A must be:
1. Written
2. Tested
3. Debugged
4. Maintained during the transition period
5. Eventually removed and replaced

This is the definition of engineering waste. The +500 LOC Rust in Option A
is not "temporary" -- it is negative-value work that slows down the eventual
Rust removal by creating additional code that must be understood and replaced.

### D3: Maintenance Burden During Transition
Option A creates a period where BOTH backends must be maintained: the enhanced
DD backend (with multi-worker, aggregation, GC) AND the developing columnar
backend. With Option B, there is only ONE transition: DD (read-only oracle)
to columnar (active development).

---

## Option A: DD-based Phase 2A -- DISQUALIFIED

**Disqualification reason**: Adding ~500 LOC Rust directly contradicts the
primary principle (P1: Rust Removal). Every feature added to the Rust DD
executor is technical debt with negative ROI.

**Specific violations**:
- Multi-worker sessions require reworking `session.rs` (currently 348 LOC,
  would grow to ~600+ LOC) with `timely::execute` Process config, per-worker
  InputSession management, and worker-indexed command routing.
- Non-monotone aggregation requires new DD `Reduce` patterns in `dataflow.rs`
  (currently 1675 LOC), adding lattice-based aggregation operators.
- Both changes deepen the FFI boundary complexity (`ffi.rs` 686 LOC) with
  new entry points that must later be removed.

**Total Rust impact**: 5045 LOC -> ~5700 LOC (+13% growth in code that will
be deleted).

**Verdict**: Option A optimizes for the wrong metric. It minimizes time-to-
Phase-2A while maximizing total cost to the Rust-free target state.

---

## Option C: Tiered Hybrid -- DISQUALIFIED

**Disqualification reason**: Tier 1 (300 LOC Rust for non-recursive COUNT/SUM)
is waste under the Rust-removal constraint.

**The "stepping stone" argument is invalid**: The previous plan argued that
"Option C doesn't preclude Option B, so Tier 1 is lower risk." This reasoning
assumed Rust was acceptable long-term. Under the Rust-removal constraint:
- Tier 1 Rust work is not a stepping stone. It is a detour.
- The time spent on Tier 1 (estimated 10 days) delays the columnar backend
  by 10 days with zero reusable output.
- Tier 2 (columnar) is the only part of Option C that matters -- and Tier 2
  alone IS Option B.

**Verdict**: Strip Option C down to its useful component (Tier 2 = columnar)
and you get Option B. The Tier 1 Rust layer is pure waste.

---

## Option B Revised: Accelerated Columnar Backend (2 Engineers)

### Strategy: Parallel Development with DD Oracle

Build the C11 columnar semi-naive evaluator as a new backend behind
`wl_compute_backend_t`. Use DD as a correctness oracle during development.
Remove Rust only after the columnar backend passes all 15 benchmark workloads.

### Resource Model: 2 C Engineers, Parallel Tracks

**Engineer 1 (Core Track)**: Semi-naive evaluator engine, hash join, delta
propagation, stratified execution loop, recursion (iterate to fixed-point).

**Engineer 2 (Operator Track)**: Columnar storage layer, operator library
(MAP, FILTER, JOIN, ANTIJOIN, REDUCE/aggregation, SEMIJOIN), operator-level
unit tests.

**Interface contract between tracks**: Engineer 2 produces operators that
conform to a C function signature:

```c
/* Each operator transforms one or more relation(s) into a result relation */
typedef struct wl_relation wl_relation_t;

wl_relation_t *wl_op_scan(wl_relation_t *input);
wl_relation_t *wl_op_filter(wl_relation_t *input, wl_filter_fn fn, void *ctx);
wl_relation_t *wl_op_project(wl_relation_t *input, const uint32_t *cols, uint32_t ncols);
wl_relation_t *wl_op_join(wl_relation_t *left, wl_relation_t *right,
                           const uint32_t *left_keys, const uint32_t *right_keys,
                           uint32_t nkeys);
wl_relation_t *wl_op_antijoin(wl_relation_t *left, wl_relation_t *right,
                                const uint32_t *left_keys, const uint32_t *right_keys,
                                uint32_t nkeys);
wl_relation_t *wl_op_reduce(wl_relation_t *input, wl_agg_fn agg,
                              const uint32_t *group_by, uint32_t ngroup,
                              uint32_t agg_col);
wl_relation_t *wl_op_union(wl_relation_t **inputs, uint32_t ninputs);
```

Engineer 1 calls these operators from the evaluation loop. The interface is
stable by end of Week 1 (design doc + header file).

### Timeline: 30 Calendar Days (6 Weeks), 2 Engineers

**Week 1-2 (Days 1-10): Foundation**

| Day | Engineer 1 (Core) | Engineer 2 (Operators) |
|-----|-------------------|----------------------|
| 1-2 | Design evaluator architecture: relation storage, delta tracking, epoch model | Design columnar storage: column types, arena allocation, relation lifecycle |
| 3-4 | Implement relation store (hash table of relations, tuple dedup) | Implement `wl_relation_t` with column-major storage, append/lookup/sort |
| 5-6 | Implement non-recursive stratum evaluator: iterate operators in plan order | Implement SCAN, MAP (project), FILTER operators with unit tests |
| 7-8 | Wire evaluator to `wl_compute_backend_t` vtable (session_create, session_step) | Implement JOIN (hash join on key columns) with unit tests |
| 9-10 | Test non-recursive programs against DD oracle (TC base case, reach) | Implement ANTIJOIN, UNION (CONCAT) with unit tests |

**Milestone 1 (Day 10)**: Non-recursive programs execute correctly via columnar
backend. Backend selectable via `wl_backend_columnar()`. DD oracle confirms
identical results on `reach.dl`, `bipartite.dl`.

**Week 3-4 (Days 11-20): Recursion + Aggregation**

| Day | Engineer 1 (Core) | Engineer 2 (Operators) |
|-----|-------------------|----------------------|
| 11-12 | Implement semi-naive delta loop: `delta_R = eval(rules, delta_inputs) - R` | Implement REDUCE for COUNT, SUM (non-recursive strata) |
| 13-14 | Implement recursive stratum evaluator: iterate until delta is empty | Implement REDUCE for AVG, MIN, MAX |
| 15-16 | Test TC (transitive closure) recursive evaluation against DD oracle | Implement SEMIJOIN operator |
| 17-18 | Test SG (same-generation), CC (connected components) against DD oracle | Implement non-monotone aggregation in non-recursive strata |
| 19-20 | Test mutual recursion (multiple relations in same SCC) | Integration: all operators tested end-to-end through evaluator |

**Milestone 2 (Day 20)**: Recursive programs with basic aggregation execute
correctly. DD oracle confirms identical results on `tc.dl`, `sg.dl`, `cc.dl`,
`sssp.dl`. Non-monotone COUNT/SUM work in non-recursive strata.

**Week 5-6 (Days 21-30): Complex Workloads + Session API + Cleanup**

| Day | Engineer 1 (Core) | Engineer 2 (Operators) |
|-----|-------------------|----------------------|
| 21-22 | Session insert/remove/step for incremental updates | Optimize hash join for multi-column keys (8-way joins for DOOP) |
| 23-24 | Delta callback wiring (session_set_delta_cb, session_snapshot) | Memory management: arena compaction, relation GC |
| 25-26 | Test all 15 benchmark workloads against DD oracle | Performance profiling: identify bottlenecks vs DD |
| 27-28 | Fix any benchmark failures, debug complex workloads (Polonius, DOOP) | Monotone aggregation (MIN/MAX) in recursive strata |
| 29-30 | Final oracle validation, remove `#include "ffi/dd_ffi.h"` from backend.h | Binary size validation (<2MB without Rust) |

**Milestone 3 (Day 30)**: All 15 benchmarks produce identical results to DD.
Session API fully functional. Binary compiles without Rust toolchain.

**Post-Milestone: Rust Removal (Days 31-33)**

| Day | Task |
|-----|------|
| 31 | Remove `rust/wirelog-dd/` directory, `backend_dd.c`, DD FFI layer |
| 32 | Remove Meson Cargo integration, DD-related meson options |
| 33 | Update CI, documentation, verify clean build with zero Rust |

### Minimum Viable Feature Set for Phase 2A

The columnar backend ships Phase 2A with these features:

1. **Non-monotone aggregation (COUNT/SUM/AVG) in non-recursive strata** --
   Direct C11 implementation, no lattice complexity needed for non-recursive.

2. **Monotone aggregation (MIN/MAX) in recursive strata** -- Already proven
   correct by the MONOTONE_AGGREGATION.md formal semantics. C implementation
   follows the same lattice approach.

3. **Incremental session API** -- Already defined in `session.h`. The columnar
   backend implements the same vtable ops (insert, remove, step, delta_cb,
   snapshot).

4. **Correctness on all 15 benchmarks** -- Validated by DD oracle comparison.

**Deferred to Phase 2B** (explicitly not in scope):
- Multi-worker (multi-threaded evaluation) -- Can be added later to C11
  evaluator without Rust dependency. Single-threaded is the correct starting
  point (matches `num_workers=1` restriction that DD already has).
- FPGA/Arrow IPC -- Phase 4 concern.
- nanoarrow columnar memory optimization -- Can be layered on top of the
  initial hash-table-based relation storage.

### Why Multi-Worker is Deferred (Not Cut)

The previous plan's Phase 2A included multi-worker sessions. With Rust removal
as the constraint, multi-worker in C11 is a separate, parallelizable concern:

- DD's multi-worker (`timely::execute` with N workers) is fundamentally tied
  to timely's progress-tracking protocol. This is NOT something that can be
  trivially reimplemented in C11.
- Single-threaded semi-naive evaluation is sufficient for correctness and for
  all current benchmark workloads.
- Multi-threaded evaluation can be added to the C11 evaluator in Phase 2B
  using pthreads + work-stealing, which is a well-understood C pattern.
- Deferring multi-worker removes the highest-risk sub-task (concurrent
  evaluation correctness) from the critical path.

---

## Pre-mortem Scenarios (Deliberate Mode)

### Scenario 1: Semi-Naive Evaluator Fails on Complex Benchmarks (HIGH RISK)

**What goes wrong**: The C11 evaluator passes simple tests (TC, reach) but
produces incorrect results on Polonius (37 rules, 1487 iterations, borrow
checking) or DOOP (136 rules, 8-way joins, class hierarchy analysis). Delta
propagation has subtle bugs where tuples are duplicated or missed in multi-rule
strata with mutual recursion.

**Impact**: Week 5-6 becomes a debugging marathon. The 30-day estimate slips
to 40-45 days.

**Mitigation**:
1. **Progressive oracle testing**: Do not wait until Week 5 to test complex
   workloads. Run DD oracle comparison on EVERY intermediate milestone. By
   Day 10, test non-recursive DOOP rules. By Day 20, test Polonius.
2. **Delta verification invariant**: After every `session_step()`, assert that
   `current_state = previous_state + delta`. Implement this as a debug-mode
   check that runs in all tests.
3. **Tuple-level diff tool**: Build a test utility that loads DD oracle results
   and columnar results, sorts both, and reports the first differing tuple with
   its provenance (which rule produced it, which delta iteration).
4. **Budget 3 extra days** (Days 28-30) as explicit buffer for complex
   benchmark debugging.

### Scenario 2: Hash Join Performance Gap on 8-Way Joins (MEDIUM RISK)

**What goes wrong**: DOOP workload has 8-way joins. A naive sequence of binary
hash joins creates large intermediate relations. The evaluator runs correctly
but 10-50x slower than DD, which uses optimized multi-way join via
`timely`'s exchange-based partitioning.

**Impact**: Benchmark performance is unacceptable. Not a correctness issue
but a credibility issue -- the replacement backend is dramatically slower.

**Mitigation**:
1. **Accept initial slowdown**: Phase 2A's goal is correctness + Rust removal,
   not performance parity. Document expected performance gap.
2. **Join ordering optimization**: The IR optimizer can reorder joins to
   minimize intermediate relation size. This is a C-side optimization that
   does not require Rust.
3. **Worst-case bound**: If any benchmark takes >60 seconds (DD finishes in
   <5 seconds), investigate that specific join pattern and optimize.
4. **Phase 2B performance work**: After correctness is established, add index
   structures, join ordering, and optional columnar storage.

### Scenario 3: Session API Impedance Mismatch (LOW RISK)

**What goes wrong**: The DD session model (`persistent timely execution with
InputSession advance`) has different epoch semantics than the C11 evaluator's
natural "run rules to fixed-point" model. The session_step() behavior differs
subtly: DD reports deltas per-epoch, C11 reports deltas per-fixed-point.

**Impact**: Integration tests fail because delta callback ordering/granularity
differs between backends.

**Mitigation**:
1. **Define session_step() contract clearly**: One call to `session_step()`
   runs ALL rules to fixed-point for the current epoch. Delta callbacks report
   the net change from the previous epoch to the new epoch.
2. **Decouple epoch from iteration**: Internal semi-naive iterations are not
   visible to the caller. Only the final converged state is reported.
3. **Test delta semantics explicitly**: Create test cases where the expected
   delta output is specified exactly (not just "matches DD"), to catch
   semantic differences early.

---

## Expanded Test Plan (Deliberate Mode)

### Unit Tests (Engineer 2 responsibility, continuous)

- `test_relation.c`: Column append, lookup, sort, dedup for 1/2/4/8 column relations
- `test_op_filter.c`: Filter with constant predicates, variable comparisons, compound expressions
- `test_op_project.c`: Projection selecting subset of columns, reordering columns
- `test_op_join.c`: Hash join correctness for 1/2/4 key columns, empty inputs, self-join
- `test_op_antijoin.c`: Antijoin correctness, empty right side, complete negation
- `test_op_reduce.c`: COUNT, SUM, AVG, MIN, MAX on grouped and ungrouped inputs
- `test_op_union.c`: Union of 2, 3, N relations with dedup

### Integration Tests (Both engineers, after each milestone)

- **Non-recursive oracle**: For each of 15 benchmarks, extract non-recursive strata,
  run both DD and columnar backends, assert identical result sets.
- **Recursive oracle**: For TC, SG, CC, SSSP, run both backends on same input data,
  assert identical IDB output.
- **Session lifecycle**: Create session, insert EDB, step, verify output. Update EDB
  (insert + remove), step again, verify delta output matches expected.
- **Delta correctness**: Insert edges incrementally (one edge per step), verify TC
  delta output matches full-recomputation diff at each step.
- **Snapshot consistency**: After N insert/remove/step cycles, `session_snapshot()`
  returns the same result as running the program from scratch with the current EDB.

### E2E Tests (Week 5-6, full benchmark suite)

- **All 15 benchmarks**: Each benchmark produces identical output tuples to DD backend.
  Tested by loading same EDB data, running both backends, sorting output, comparing.
- **Incremental session E2E**: For TC, CC, SSSP: start with 50% of edges, step, add
  remaining 25%, step, remove 10%, step. Verify final state matches.
- **Stress test**: 100 epochs of insert/remove/step cycles on CC workload, verify no
  memory growth beyond O(|current_state|).
- **Binary size**: Build without Rust. Verify binary < 2MB.

### Observability

- **Memory profiling**: Track peak RSS during each benchmark. Compare against DD.
  Acceptable threshold: <2x DD memory usage.
- **Per-step timing**: Instrument `session_step()` to report wall-clock time per
  call. Log as structured output: `{relation, step_number, duration_us, tuples_added,
  tuples_removed}`.
- **Delta verification mode**: Compile-time flag (`-DWIRELOG_DELTA_VERIFY=1`) that
  enables `current = previous + delta` invariant checking after every step.
- **Tuple provenance** (debug mode): Track which rule and which delta iteration
  produced each tuple. Essential for debugging complex benchmark failures.

---

## Feasibility Analysis: 30 Days with 2 Engineers

### Why 40-50 Days (Single Engineer) Compresses to 25-30 Days (2 Engineers)

The original 40-50 day estimate was for ONE engineer doing sequential work:
1. Design + columnar storage (5 days)
2. Operators (8 days)
3. Non-recursive evaluator (5 days)
4. Recursive evaluator (7 days)
5. Session API (5 days)
6. Aggregation (5 days)
7. Testing + debugging (10 days)

With 2 engineers, the parallelizable work is:
- **Operators (Engineer 2)** runs in parallel with **Evaluator (Engineer 1)**
- **Unit tests (Engineer 2)** run in parallel with **Integration (Engineer 1)**
- **Aggregation operators (Engineer 2)** run in parallel with **Session wiring (Engineer 1)**

The NON-parallelizable work (serial dependencies):
- Evaluator design must precede evaluator implementation
- Recursive evaluation must follow non-recursive
- Session API depends on working evaluator
- Benchmark testing depends on working evaluator + operators

**Critical path with 2 engineers**:
```
Days 1-2:  Design (both engineers, shared)
Days 3-10: Evaluator core (Eng 1) || Operators (Eng 2)
Days 11-16: Recursive eval (Eng 1) || Aggregation ops (Eng 2)
Days 17-22: Session API (Eng 1) || Performance + memory (Eng 2)
Days 23-30: Full benchmark testing + debugging (both)
```

Critical path: 30 days. Versus single-engineer serial: 45 days.

### Risk Budget

The 30-day estimate includes 5 days of explicit buffer (Days 26-30) for
complex benchmark debugging (Scenario 1). If no major issues arise, the
project completes in 25 days.

### What If Only 1 Engineer?

With 1 engineer, the timeline extends to ~40 days (not 50, because the
existing vtable infrastructure eliminates the backend abstraction work that
was previously estimated at 5-7 days). The scope remains the same.

---

## Decision Framework

### The Total Cost Argument

If the team commits to Rust removal, the question is not "what ships Phase 2A
fastest?" but "what reaches Rust-free Phase 2A fastest?"

| Path | Phase 2A delivery | Rust-free delivery | Total cost |
|------|------------------|--------------------|------------|
| Option A then B | Day 25 | Day 70+ | 70+ engineer-days |
| Option B direct | Day 30 | Day 33 | 60 engineer-days (2 eng x 30 days) |

Option B direct is ~15% slower to first feature delivery but reaches the
strategic goal 50% faster.

### Break-Even Analysis

**Investment**: 2 engineers x 30 days = 60 engineer-days.

**Savings from Rust removal**:
- Eliminated: 5045 LOC Rust maintenance (7 files)
- Eliminated: Rust toolchain in CI (cargo build, cross-compilation)
- Eliminated: FFI boundary debugging (unsafe blocks, ABI coordination)
- Eliminated: Dependency management (DD 0.19.1, timely 0.26, columnar 0.11)
- Eliminated: Developer onboarding cost (every contributor must know both C and Rust)

**Conservative maintenance cost of Rust code**: 0.5 engineer-days/week
(bug fixes, dependency updates, CI maintenance).

**Break-even**: 60 days / 0.5 days/week = 120 weeks = ~2.3 years.

But this is overly conservative. The real break-even is much sooner because:
- Option A would ADD 500 LOC Rust, increasing maintenance cost
- Future features (Phase 2B multi-worker, Phase 4 FPGA) are simpler in
  pure C11 than in hybrid C/Rust
- Developer hiring is easier (C11 only vs C11+Rust)

**Realistic break-even**: 6-12 months.

### The 5700 LOC Question

Is one month of upfront investment worth eliminating 5700 LOC of Rust
maintenance forever?

Yes, if:
1. Rust removal is truly the strategic direction (stated: yes).
2. The columnar backend is correct (mitigated: DD oracle testing).
3. Performance is acceptable (mitigated: deferred to Phase 2B optimization).
4. The timeline is achievable (mitigated: 2-engineer parallel development).

---

## ADR: Architectural Decision Record

### Decision
**Option B: Accelerated Columnar Backend** -- Build a C11-native semi-naive
evaluator implementing all Phase 2A features. Remove Rust upon passing all
15 benchmark oracle tests. Target: 30 calendar days with 2 C engineers.

### Drivers
1. **Rust removal is the primary strategic goal** (P1, non-negotiable)
2. **Total cost of ownership** favors building once in C11 over building in
   Rust then rebuilding in C11 (D1)
3. **Backend vtable already exists** -- zero infrastructure work needed (P4)
4. **DD serves as correctness oracle** -- mitigates the biggest risk (P3)

### Alternatives Considered

**Option A (DD-based Phase 2A)**: Extend Rust DD with multi-worker and
non-monotone aggregation. ~25 days to Phase 2A, but adds ~500 LOC Rust
that contradicts the Rust-removal goal. Disqualified: total cost to
Rust-free state is ~70 days vs ~30 days for Option B.

**Option C (Tiered Hybrid)**: Tier 1 adds 300 LOC Rust for quick wins,
Tier 2 builds columnar. Disqualified: Tier 1 Rust work is wasted if the
goal is Rust removal. Tier 2 alone IS Option B.

### Why Option B is Chosen

1. **Aligns with primary constraint**: Zero Rust in the final state.
2. **Lower total cost**: 60 engineer-days to Rust-free Phase 2A, vs 70+
   for Option A-then-B path.
3. **Eliminates transition overhead**: No period of dual-backend maintenance
   for enhanced-DD + developing-columnar.
4. **Existing infrastructure**: Backend vtable, session API, IR pipeline,
   FFI plan structure all remain unchanged. Only the execution engine is
   replaced.
5. **Oracle testing**: DD runs alongside during development, providing a
   formally-correct reference for every test.

### Consequences

**Positive**:
- Rust toolchain eliminated from build/CI/development
- 5045 LOC of Rust code removed
- Single-language codebase (C11 only)
- Embedded target viable immediately (<2MB binary)
- Full debuggability (no FFI boundary, no opaque Rust allocations)
- Simpler contributor onboarding

**Negative**:
- Phase 2A delivery delayed by ~5 days vs Option A (30 vs 25 days)
- Semi-naive evaluator correctness must be proven against complex benchmarks
- Initial performance may lag DD on multi-way join workloads (DOOP)
- Multi-worker deferred to Phase 2B

**Neutral**:
- Session API unchanged (same `wl_compute_backend_t` vtable)
- Plan structure unchanged (same `wl_ffi_plan_t` / `wl_dd_plan_t`)
- Parser/IR/optimizer/stratifier unchanged (pure C11 already)

### Follow-ups

1. **Phase 2B (multi-worker)**: Add pthreads-based parallel evaluation to
   the columnar backend. Estimate: 10-15 days, 1 engineer. Can begin
   immediately after Phase 2A ships.
2. **Performance optimization**: After correctness is established, profile
   against DD on all benchmarks. Optimize join ordering, add index
   structures, investigate columnar (nanoarrow) memory layout.
3. **Rename plan types**: Once Rust is removed, rename `wl_dd_plan_t` to
   `wl_exec_plan_t` and `wl_ffi_plan_t` to `wl_plan_t`. Low priority but
   improves clarity.
4. **CI simplification**: Remove Cargo from CI pipeline, remove Rust
   toolchain from Docker images, update build documentation.
5. **Non-monotone aggregation in recursive strata**: If needed, implement
   using stratified-negation-style re-stratification (move aggregation to
   non-recursive stratum). This is a correctness research question that
   does not block Phase 2A.

---

## Appendix A: What Stays, What Goes

### Stays (Zero Changes)
| Component | LOC | Reason |
|-----------|-----|--------|
| Parser (`wirelog/parser/`) | ~2,500 | Pure C11 |
| IR (`wirelog/ir/`) | 3,000 | Pure C11 |
| Optimizer (`wirelog/optimizer/`) | ~1,500 | Pure C11 |
| Session API (`wirelog/session.h/c`) | ~250 | Backend-agnostic dispatcher |
| Backend vtable (`wirelog/backend.h`) | 125 | Already supports multiple backends |
| Intern (`wirelog/intern.c/h`) | ~400 | Pure C11 |
| Tests (19,284 LOC) | 19,284 | Reused with columnar backend |

### Goes (Removed After Oracle Passes)
| Component | LOC | Reason |
|-----------|-----|--------|
| `rust/wirelog-dd/src/*.rs` | 5,045 | Rust DD executor |
| `wirelog/backend_dd.c` | 169 | DD backend vtable impl |
| `wirelog/backend/dd/dd_ffi.h` | 785 | Rust FFI declarations |
| `wirelog/backend/dd/dd_marshal.c` | 686 | Plan marshaling for Rust FFI |
| Cargo.toml, Cargo.lock | ~50 | Rust build config |
| Meson Cargo integration | ~30 | Meson-to-Cargo bridge |
| **Total removed** | **~6,765** | |

### New (Columnar Backend)
| Component | Est. LOC | Engineer |
|-----------|----------|----------|
| `wirelog/backend_columnar.c` | ~200 | Eng 1 |
| `wirelog/eval/evaluator.c` | ~800 | Eng 1 |
| `wirelog/eval/evaluator.h` | ~100 | Eng 1 |
| `wirelog/eval/relation.c` | ~400 | Eng 2 |
| `wirelog/eval/relation.h` | ~80 | Eng 2 |
| `wirelog/eval/operators.c` | ~600 | Eng 2 |
| `wirelog/eval/operators.h` | ~100 | Eng 2 |
| `wirelog/eval/aggregate.c` | ~300 | Eng 2 |
| `wirelog/eval/aggregate.h` | ~50 | Eng 2 |
| `tests/test_columnar_*.c` | ~2,000 | Both |
| **Total new** | **~4,630** | |

**Net LOC change**: -6,765 (removed) + 4,630 (new) = **-2,135 LOC** net reduction.

---

## Appendix B: Codebase Metrics

| Metric | Current | After Phase 2A |
|--------|---------|---------------|
| C source (wirelog/) | 12,003 LOC | ~16,633 LOC (+4,630 new - 1,670 DD/FFI removed) |
| C tests (tests/) | 19,284 LOC | ~21,284 LOC (+2,000 new) |
| Rust source | 5,045 LOC | 0 LOC |
| Total source (excl. tests) | 17,048 LOC | ~16,633 LOC |
| Languages | C11 + Rust | C11 only |
| Build systems | Meson + Cargo | Meson only |
| External dependencies | DD 0.19.1, timely 0.26, columnar 0.11 | None (or optional nanoarrow) |
| Binary size (release) | ~8MB (with Rust static lib) | <2MB (C11 only) |
| Benchmark workloads | 15 | 15 (identical) |
| Backend vtable ops | 8 | 8 (unchanged) |
