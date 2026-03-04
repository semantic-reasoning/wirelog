# RALPLAN-DR: DD-based Phase 2A vs Columnar Backend Decision

**Date**: 2026-03-04
**Mode**: DELIBERATE (architecture-level decision with multi-month impact)
**Status**: DRAFT -- awaiting Architect and Critic review

---

## Principles

These are the values that should govern this decision, derived from wirelog's
actual codebase constraints, stated project goals, and architecture document.

### P1: Incremental Delivery over Big-Bang Rewrites
wirelog has a working, tested pipeline (410 tests, 15 benchmarks, 5045 LOC Rust,
12003 LOC C source, 19284 LOC tests). Any path that risks extended periods of
broken-main or parallel-maintenance of two backends contradicts the project's
demonstrated ship-and-iterate cadence (Phase 0 in 2 weeks, Phase 1 in 3 weeks).

### P2: Minimize Rust Surface Area (C11-First)
`CLAUDE.md` explicitly states "Minimize Rust changes." The architecture document
positions C11 as the long-term foundation. Rust/DD is a means, not an end. Any
path that deepens Rust investment should be weighed against the eventual C11-only
embedded target.

### P3: Correctness Before Performance
The existing DD backend is proven correct on complex workloads (Polonius 37 rules
1487 iterations, DOOP 136 rules 8-way joins). A columnar rewrite must match this
correctness bar. Getting semi-naive evaluation right for recursive Datalog with
negation and aggregation is a research-grade problem.

### P4: Backend Abstraction is Already in Place
`wl_compute_backend_t` vtable (commit `56c26f8`) and the full session API
(`session_create/insert/remove/step/snapshot/set_delta_cb/destroy`) are already
implemented and tested. The architecture explicitly supports backend swapability.
Neither option is blocked by missing abstractions.

### P5: Embedded Target Is a Future Commitment, Not a Current Requirement
The architecture document places nanoarrow/embedded at "Phase 3+/Phase 4+."
There is no stated customer deadline for embedded. Pulling Phase 3 forward
should require explicit justification, not assumed urgency.

---

## Decision Drivers

### D1: Time-to-Market for Phase 2A Features (TOP PRIORITY)
Issue #80 defines concrete deliverables: multi-worker sessions, non-monotone
aggregation in recursive strata (COUNT/SUM/AVG), incremental GC, delta-driven
optimization. These features have identifiable users. The faster path to
shipping them matters.

**Measurement**: Days from decision to "all 15 benchmarks pass with Phase 2A
features enabled."

### D2: Rust Code Maintenance Burden
Current Rust surface: 5045 LOC across 7 files. The session model already runs a
persistent `timely::execute_directly` with a background thread, mpsc command
channel, and `Arc<Mutex<>>` state. Adding multi-worker and non-monotone
aggregation extends this complexity. Each Rust change requires coordinating C FFI
types (`wl_ffi_*`), Rust FFI entry points, and Rust internals.

**Measurement**: LOC added to Rust, number of new `unsafe` blocks, new FFI
entry points required.

### D3: Long-Term Architecture Alignment
The architecture document's roadmap is: Phase 2 (docs) -> Phase 3 (nanoarrow
backend) -> FPGA. Option B collapses Phase 3 into Phase 2A, which changes the
roadmap sequence. Option A preserves the roadmap.

**Measurement**: Does the chosen path leave the codebase closer to or further
from the stated Phase 3+ architecture?

---

## Option A: DD-based Phase 2A (Extend Current Architecture)

Implement all Phase 2A features (multi-worker, non-monotone aggregation,
incremental GC, delta optimization) within the existing Rust DD executor,
keeping the C11 frontend and backend vtable as-is.

### Pros

1. **Fastest path to working features (~25 days).** DD already provides
   multi-worker (`timely::execute` with N workers), `Reduce` for aggregation,
   and `consolidate()` for GC. The infrastructure exists; the work is wiring
   and testing.

2. **Proven correctness foundation.** DD's `iterate()` with `distinct()` is
   formally correct for recursive fixed-point computation. Non-monotone
   aggregation in recursive strata is well-studied in DD (lattice-based
   approach). We are extending a correct system, not reimplementing one.

3. **Zero risk to existing 410 tests.** All changes are additive. The one-shot
   execution path (`wl_dd_execute_cb`) remains untouched. The session path
   (`DdSession`) is already working for single-worker, non-recursive programs.

4. **Backend vtable stays intact for future Phase 3.** A nanoarrow backend
   can be added later behind `wl_compute_backend_t` without any changes to
   the DD path.

### Cons

1. **Deepens Rust investment contrary to P2.** Multi-worker sessions require
   reworking `session.rs` (currently `execute_directly`, needs
   `timely::execute` with worker config). Non-monotone aggregation in
   recursive strata requires new DD operator patterns. Estimated +400-600
   LOC Rust.

2. **Multi-worker DD is complex.** `timely::execute` with N>1 workers uses
   `timely::communication::initialize::Configuration::Process(N)`. The
   `InputSession` must be managed per-worker. The session command channel
   must handle worker-indexed operations. This is the hardest sub-task.

3. **DD's memory model is opaque from C.** Incremental GC in DD means
   `consolidate()` calls and arrangement compaction, but C has no visibility
   into DD's internal memory. Reporting memory usage or enforcing limits
   requires Rust-side instrumentation.

4. **Phase 3 nanoarrow will still be a separate large effort.** Option A
   does not reduce the eventual cost of a C11-native backend. The columnar
   work is deferred, not eliminated.

### Trade-offs

- **Speed vs. Rust debt**: Ship Phase 2A in ~25 days but add ~500 LOC Rust
  that will eventually be replaced (or kept as the enterprise path).
- **Correctness confidence vs. opacity**: DD's formal guarantees are strong
  but the system becomes harder to debug when things go wrong across the
  FFI boundary.
- **Incremental progress vs. technical convergence**: Each DD feature adds
  value immediately but moves the codebase further from the C11-only vision.

---

## Option B: Complete Columnar Backend Replacement

Implement a C11-native semi-naive evaluator with columnar storage. Implement
all Phase 2A features directly in the new backend. Remove Rust dependency.

### Pros

1. **Eliminates Rust/FFI complexity permanently.** No more `unsafe` blocks,
   no more RPN expression serialization, no more `Arc<Mutex<>>` session
   management across thread boundaries. The entire execution layer becomes
   C11 with direct debuggability.

2. **Directly addresses the long-term architecture.** The architecture
   document envisions a C11-native backend (Phase 3). Option B delivers
   this immediately, making Phase 3 a no-op and Phase 4 (FPGA via Arrow
   IPC) the natural next step.

3. **Full control over memory management.** A C11 evaluator can use arena
   allocation (per Discussion #58), track memory precisely, and implement
   GC with full visibility. No opaque Rust allocations.

4. **Embedded target becomes viable immediately.** A C11-only backend
   produces a standalone binary without the Rust toolchain. Binary size
   target (<2MB) becomes achievable.

### Cons

1. **~40-50 days estimated, 1.6-2x longer than Option A.** Building a
   correct semi-naive evaluator with negation, recursion, and aggregation
   from scratch is substantial. The existing plan (`delta-query-support.md`)
   estimated the semi-naive evaluator alone at 5-7 days (XL), and that was
   for a simple version without non-monotone aggregation or multi-threading.

2. **High correctness risk.** Semi-naive evaluation with stratified negation
   and aggregation is subtle. DD has been battle-tested over years;
   a new C11 evaluator has not. The 15 benchmark workloads (especially
   Polonius with 1487 iterations and DOOP with 136 rules) are aggressive
   correctness tests.

3. **Multi-threading in C11 is harder than in Rust/DD.** DD's multi-worker
   model (`timely`) handles data partitioning, progress tracking, and
   worker synchronization. Reimplementing this in C11 with pthreads is
   a significant engineering effort with concurrency-bug risk.

4. **Parallel maintenance during transition.** Until the new backend passes
   all 15 benchmarks, both the DD and columnar backends must coexist. This
   is a testing and maintenance burden that contradicts P1.

### Trade-offs

- **Long-term simplicity vs. short-term risk**: The end state is cleaner
  but the path is longer and riskier.
- **Full control vs. proven correctness**: C11 gives visibility but loses
  DD's formally-verified fixed-point semantics.
- **Single codebase vs. transition period**: Eventually simpler, but the
  transition requires maintaining two backends.

---

## Pre-mortem Scenarios (Deliberate Mode)

### Scenario 1 (Option A): Multi-Worker Session Deadlocks

**What goes wrong**: `timely::execute` with N>1 workers requires careful
coordination of `InputSession` advancement across workers. If worker 0's
input advances to epoch 5 while worker 1 is still at epoch 3, the dataflow
stalls waiting for progress. The single-threaded `DdSession` command loop
(lines 239-287 of `session.rs`) becomes a bottleneck because it serializes
all commands through one mpsc channel.

**Impact**: Multi-worker sessions hang or produce incorrect results under
concurrent load. Single-worker continues to work, but the key Phase 2A
feature (multi-worker) is broken.

**Mitigation**: Prototype multi-worker session in an isolated Rust test
*before* wiring FFI. Use `timely::execute` with `Process(2)` config and
verify that `InputSession` advancement is synchronized across workers.
Add a watchdog timer that detects stalls (no probe advancement for >N
seconds) and reports error rather than hanging.

### Scenario 2 (Option A): Non-Monotone Aggregation Breaks Recursive Semantics

**What goes wrong**: Adding COUNT/SUM/AVG inside `iterate()` is currently
rejected (lines 791-821 of `dd_plan.c`). Enabling it requires a
lattice-based approach where aggregation results form a monotone lattice
(e.g., multiset semantics). If the lattice encoding is wrong, the
fixed-point either diverges (infinite loop) or converges to wrong results.

**Impact**: Recursive programs with COUNT/SUM produce silently wrong results
that pass simple tests but fail on real workloads. This is the worst failure
mode because it erodes trust in the engine.

**Mitigation**: Implement non-monotone aggregation in non-recursive strata
first (safe, no lattice needed). For recursive strata, require explicit
user opt-in (`PRAGMA recursive_aggregation`) and document that results are
under eventual-consistency semantics. Cross-validate against a reference
Datalog engine (Souffle or Crepe) on the benchmark suite.

### Scenario 3 (Option B): Semi-Naive Evaluator Fails on Complex Benchmarks

**What goes wrong**: The C11 semi-naive evaluator passes simple tests (TC,
reach) but produces incorrect results on Polonius (37 rules, 1487
iterations, borrow checking) or DOOP (136 rules, 8-way joins, class
hierarchy analysis). Debugging requires understanding both the Datalog
semantics and the evaluator's delta propagation logic across 28+ rules.

**Impact**: The project is stuck for weeks debugging subtle correctness
issues in the new evaluator while DD continues to work correctly. The
40-50 day estimate balloons to 60-80 days. Meanwhile, Phase 2A features
remain undelivered.

**Mitigation**: Use DD as a reference oracle. For every benchmark, run both
DD and columnar backends and diff results tuple-by-tuple. Start with the
simplest benchmarks (TC, Reach) and work up to complex ones (Polonius,
DOOP). Do not remove DD until the columnar backend matches on all 15
workloads.

---

## Expanded Test Plan (Deliberate Mode)

### Unit Tests

**Option A additions**:
- `session.rs`: Multi-worker session create/step/destroy lifecycle
- `session.rs`: Multi-worker insert + step with 2/4 workers, verify
  results match single-worker
- `dataflow.rs`: Non-monotone aggregation (COUNT/SUM) in non-recursive
  stratum via DD `Reduce`
- `dataflow.rs`: Non-monotone aggregation in recursive stratum with
  lattice encoding (if implemented)
- `session.rs`: Incremental GC (`consolidate()`) reduces arrangement size
  after retraction
- `ffi.rs`: `wl_dd_session_create` with `num_workers > 1` succeeds
  (currently returns -1)

**Option B additions**:
- `eval.c`: Hash join correctness for 2-column, 3-column, N-column keys
- `eval.c`: Semi-naive delta propagation for linear recursion (TC)
- `eval.c`: Semi-naive delta propagation for mutual recursion (SG)
- `eval.c`: Stratified negation (ANTIJOIN) across strata boundaries
- `eval.c`: COUNT/SUM/AVG aggregation in non-recursive strata
- `eval.c`: MIN/MAX aggregation in recursive strata (monotone)
- `columnar.c`: Column append, lookup, sort operations

### Integration Tests

- **Session lifecycle**: `test_session.c` extended with multi-worker tests
  (insert on worker 0, step, verify all workers see consistent output)
- **Delta correctness**: Insert edges incrementally, verify TC delta
  output matches full-recomputation diff at each step
- **Backend equivalence**: For each of 15 benchmarks, run both DD and
  columnar (or DD multi-worker) backends, assert identical result sets
- **Mixed operations**: Insert, step, remove, step, insert, step -- verify
  cumulative state matches expected
- **Session snapshot after retractions**: Remove facts, step, snapshot --
  verify snapshot reflects retractions

### E2E Tests

- **Multi-worker TC**: 10K-node graph, 4 workers, verify TC equals
  single-worker TC
- **Long-running session**: 100 epochs of insert/remove/step cycles on CC
  workload, verify no memory growth beyond expected
- **Non-monotone aggregation E2E**: Program with COUNT in non-recursive
  rule, verify count updates correctly after insert/remove cycles
- **CLI integration**: `wirelog --workers 4 tc.dl` produces correct output
- **Benchmark regression**: All 15 benchmarks produce identical results
  to the current main branch

### Observability

- **Memory profiling**: Track peak RSS during benchmark execution. For
  Option A, add Rust-side `jemalloc_stats` or `System::alloc` tracking.
  For Option B, add C-side `malloc_usable_size` / arena watermark.
- **Epoch timing**: Instrument `session_step()` to report wall-clock time
  per epoch. Log as structured output (relation, epoch, duration_us,
  tuples_changed).
- **GC effectiveness**: Before/after metrics for `consolidate()` (Option A)
  or compaction (Option B): arrangement size, tuple count, memory freed.
- **Worker utilization**: For multi-worker, report per-worker tuple counts
  and idle time. Detect worker skew (one worker processing 90% of tuples).

---

## ADR: Architectural Decision Record

### Decision
**PENDING** -- To be finalized after Architect and Critic review.

### Recommendation: Option A (DD-based Phase 2A)

### Drivers
1. **D1 (time-to-market)** strongly favors Option A: ~25 days vs ~40-50 days.
2. **D2 (Rust burden)** weakly favors Option B, but the current Rust surface
   (5045 LOC) is manageable and the session architecture is already sound.
3. **D3 (architecture alignment)** weakly favors Option B, but the backend
   vtable already enables future replacement without disruption.

### Alternatives Considered

**Option A: DD-based Phase 2A** -- Extend the current Rust DD executor with
multi-worker support, non-monotone aggregation, incremental GC, and
delta-driven optimization. ~25 days.

**Option B: Columnar Backend Replacement** -- Build a C11-native semi-naive
evaluator with columnar storage, implementing all Phase 2A features directly.
Remove Rust dependency. ~40-50 days.

### Why Option A is Recommended

1. **P1 (Incremental Delivery)**: Option A ships Phase 2A features in ~25
   days without risking the existing working system. Option B has a 40-50 day
   estimate with significant correctness risk that could push it to 60-80
   days.

2. **P3 (Correctness Before Performance)**: DD's `iterate()` and `Reduce`
   are formally correct. A new semi-naive evaluator must be validated against
   the entire benchmark suite. The risk of subtle correctness bugs in
   Polonius/DOOP-class programs is high.

3. **P4 (Backend Abstraction in Place)**: The vtable makes Option A
   non-blocking for Option B. After shipping Phase 2A with DD, a columnar
   backend can be developed in parallel behind the same vtable, validated
   against DD results, and swapped in when ready. This is the lowest-risk
   sequencing.

4. **P5 (Embedded is Future)**: There is no stated customer deadline for
   the embedded target. Pulling Phase 3 forward into Phase 2A introduces
   urgency where none exists.

The key insight is that **Option A does not preclude Option B**. After
shipping Phase 2A with DD, the team can pursue the columnar backend as
Phase 3 (as originally planned) with a working reference implementation
to validate against.

### Consequences

**Positive**:
- Phase 2A features delivered faster
- Existing test infrastructure reused
- Backend vtable validates its design with a second set of session features

**Negative**:
- Rust codebase grows by ~500 LOC (temporary if Phase 3 replaces DD)
- Multi-worker session adds complexity to `session.rs`
- Incremental GC depends on DD internals (`consolidate()` timing)

**Neutral**:
- Phase 3 (columnar backend) timeline unchanged
- Architecture document does not need revision

### Follow-ups

1. After Phase 2A ships, evaluate whether Phase 3 (columnar backend) should
   target embedded-only or full replacement.
2. Multi-worker session implementation should be prototyped in isolated Rust
   tests before FFI wiring.
3. Non-monotone aggregation in recursive strata should be treated as a
   separate sub-task with its own correctness validation.
4. Explore `timely::communication::initialize::Configuration::Process` vs
   `Thread` for multi-worker sessions.
5. Document the Rust code growth budget: if Phase 2A would push Rust beyond
   ~6000 LOC, reconsider scope.

---

## Appendix: Codebase Metrics (as of 2026-03-04)

| Metric | Value |
|--------|-------|
| C source (wirelog/) | 12,003 LOC |
| C tests (tests/) | 19,284 LOC |
| Rust source (src/) | 5,045 LOC |
| Test suites | 20 C suites + 85 Rust tests |
| Benchmark workloads | 15 |
| Backend vtable ops | 8 (create/destroy/insert/remove/step/set_delta_cb/snapshot) |
| Session FFI entry points | 7 (create/destroy/insert/remove/step/set_delta_cb/snapshot) |
| DD dependencies | differential-dataflow dogs3 v0.19.1, timely 0.26 |
| Current session restriction | Single worker only (`num_workers > 1` returns -1) |
| Non-monotone agg restriction | COUNT/SUM rejected in recursive strata (dd_plan.c:791-821) |
