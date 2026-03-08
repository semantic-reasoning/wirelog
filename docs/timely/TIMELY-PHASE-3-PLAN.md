# Phase 3: Timely Dataflow C11 Implementation - RALPLAN-DR Execution Plan

**Date:** 2026-03-08
**Version:** 1.0
**Status:** READY FOR REVIEW
**Scope:** Phase 3 (Weeks 0-16, 2026-03-15 ~ 2026-06-15)
**Team:** Codex (implementation), Gemini (design/docs), Claude (coordination)
**Prior Art:** RALPLAN-TIMELY-DESIGN.md, ADR-002, Phase 2D Final Report, K-Fusion Architecture

---

## Context

### Current State (Phase 2D Complete)

wirelog's pure C11 columnar backend achieves:
- **CSPA (K=2):** 6.0s median, 20,381 tuples, 6 iterations
- **DOOP (K=8):** 71m50s, K-fusion + workqueue parallel execution
- **Test Suite:** 20/20 passing (19 OK + 1 EXPECTEDFAIL)
- **Architecture:** Semi-naive evaluator with K-fusion parallelism, workqueue (5-function API), arena allocator, nanoarrow columnar storage

### Target (Phase 3 Complete)

- **DOOP:** 47s (DD/Timely parity)
- **CSPA:** <2s (significant improvement from 6.0s)
- **All benchmarks:** tuple-exact correctness against DD baseline
- **Architecture:** Timely-like frontier-based dataflow runtime in pure C11

### The 91x DOOP Gap Analysis

The gap between wirelog (71m50s) and DD+Timely (47s) stems from three root causes (per RALPLAN-TIMELY-DESIGN.md Driver 1):

1. **Redundant recomputation:** wirelog re-evaluates ALL K copies every iteration, even when only 1 copy's delta is non-empty. DD's arrangement-based joins avoid this entirely.
2. **Full-sort consolidation:** wirelog's CONSOLIDATE does qsort on the entire relation each iteration. DD's consolidate operates only on changed records.
3. **Coarse delta granularity:** wirelog tracks deltas at the relation level (snapshot diff). DD tracks deltas at the record level with multiplicities.

K-fusion parallelism addresses factor 1 partially. It does NOT address factors 2-3. Closing the gap requires architectural changes to delta tracking and join execution.

---

## Principles (5)

### P1: Phased Value Delivery with Measurable Gates
Each phase (3A/3B/3C/3D) must deliver benchmarkable improvement. No phase exceeds 4 weeks without measurable results. Gate criteria are quantitative (wall-time, tuple count, memory).

### P2: C11 Purity and Embedded Compatibility
All code remains pure C11 + pthreads. No external runtime dependencies. Architecture must remain viable for embedded targets and future FPGA backends. No C++ or Rust dependencies.

### P3: Correctness is Non-Negotiable
Semi-naive fixed-point semantics must be preserved exactly. Tuple counts and iteration counts must match the validated Phase 2D baseline. DD historical output (extractable from git history at commit 8f03049) serves as the correctness oracle for DOOP.

### P4: Leverage, Then Extend
Maximize reuse of proven components (workqueue, K-way merge, arena, columnar storage, plan generation). New Timely concepts are layered ON TOP of existing infrastructure, not replacing it. The existing semi-naive evaluator remains the fallback path.

### P5: Race Condition Prevention by Design
Phase 2D's ASAN-discovered race condition taught a clear lesson: shared mutable state between workers is the primary risk. All new concurrent paths must follow the collect-then-merge pattern. TSan validation is mandatory for every parallel code path.

---

## Decision Drivers (Top 3)

### Driver 1: Root Cause Hierarchy of the 91x Gap

Profiling and analysis reveal a clear priority order:

| Factor | Contribution to Gap | Addressed By |
|--------|-------------------|--------------|
| Coarse delta tracking (relation-level snapshot diff) | ~40-50x | Phase 3B (timestamped records) |
| Full-sort CONSOLIDATE per iteration | ~10-20x | Phase 3B (incremental merge) |
| Redundant K-copy evaluation | ~2-4x | Phase 3A (K-fusion completion) |
| No persistent join indexes | ~2-5x | Phase 3C (arrangements) |
| Sequential operator scheduling | ~1-2x | Phase 3D (dataflow graph) |

Addressing factors in order of ROI: K-fusion (quick win, 70% built) -> timestamped deltas (root cause) -> arrangements (join efficiency) -> dataflow scheduling (fine-grained parallelism).

### Driver 2: Implementation Feasibility Within 16 Weeks

Full Timely/Naiad is ~15,000 lines of Rust. A phased approach builds incrementally:
- Phase 3A: ~500 lines (completing existing infrastructure)
- Phase 3B: ~2,000 lines (timestamped delta tracking)
- Phase 3C: ~2,500 lines (arrangement layer)
- Phase 3D: ~1,500 lines (dataflow scheduling)
- **Total: ~6,500 lines** over 16 weeks (manageable for 3-person team)

### Driver 3: DD Correctness Oracle Availability

DD backend was removed at commit 8f03049 but its output is recoverable from git history. This provides a tuple-exact correctness oracle for DOOP that eliminates ambiguity in validation. The oracle is available for all 15 workloads.

---

## Work Objectives

### Immutable Goals
- Pure C11 Timely Dataflow complete implementation
- Columnar nanoarrow backend full integration
- DOOP 47s achievement (DD parity)
- All benchmarks tuple-exact correctness

### Phase Targets

| Phase | Weeks | CSPA Target | DOOP Target | Key Deliverable |
|-------|-------|-------------|-------------|-----------------|
| 3A | 1-3 | <4.5s | <30m | K-fusion plan generation + dispatch |
| 3B | 4-8 | <2.0s | <5m | Timestamped delta tracking + incremental consolidate |
| 3C | 9-13 | <1.5s | <90s | Arrangement layer (persistent indexed joins) |
| 3D | 14-16 | **<1.2s** | **<47s** | Dataflow graph scheduling + fine-grained parallelism |

**UPDATED TARGETS (Per User Directive 2026-03-08):**
- **CSPA Hard Target:** <1.2s (99% improvement from 6.0s baseline)
- **DOOP Hard Target:** <47s (91x improvement from 71m50s baseline, DD parity OR BETTER)
- **No compromise on performance goals** - both targets must be achieved

---

## Guardrails

### Must Have
- All 20 existing tests pass at every phase gate
- Tuple counts match Phase 2D baseline exactly
- ASAN clean build at every phase gate
- TSan clean for all concurrent code paths
- Fallback to semi-naive evaluator if any Timely component fails
- Feature flags for each new subsystem (enable/disable independently)

### Must NOT Have
- C++ or Rust code
- External runtime dependencies beyond pthreads
- Changes to the public API (session.h, backend.h vtable)
- Global mutable state (Phase A lesson)
- Shared mutable state between workers (Phase 2D lesson)
- Plan generation changes that break non-Timely operator paths

---

## Task Flow: Phased Execution Plan

### Phase 3A: Complete K-Fusion (Weeks 1-3)

**Goal:** Finish remaining K-fusion infrastructure, capture quick parallelism wins.

**Owner:** Codex (implementation), Claude (coordination)

#### Week 1: Plan Generation Integration

**Task 3A-1: K-Fusion Node Detection in Plan Generator**

Modify `exec_plan_gen.c` to detect K-copy relation patterns and emit `WL_PLAN_OP_K_FUSION` nodes instead of K separate copies.

Files:
- `wirelog/exec_plan_gen.c` (modify: K-copy detection logic)
- `wirelog/exec_plan.h` (no changes needed, K_FUSION=9 already exists)
- `wirelog/backend/columnar_nanoarrow.h` (no changes, `wl_plan_op_k_fusion_t` already defined)

Acceptance Criteria:
- [ ] Plan generator emits K_FUSION nodes for relations with K>=2 IDB body atoms
- [ ] K_FUSION node metadata (`wl_plan_op_k_fusion_t`) correctly populated with K operator sequences
- [ ] Each K-copy sequence has correct delta_mode annotations (position d = FORCE_DELTA, others = FORCE_FULL)
- [ ] Non-K-fusion relations generate identical plans to Phase 2D (backward compatibility)
- [ ] `meson test -C build` passes 20/20

**Task 3A-2: Complete col_op_k_fusion() Dispatch**

Replace the EINVAL placeholder in `col_op_k_fusion()` (columnar_nanoarrow.c:2301) with full workqueue orchestration.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_op_k_fusion, ~150 lines)

Acceptance Criteria:
- [ ] Per-worker arena allocation (fresh arena per K-copy)
- [ ] Workqueue create/submit/wait_all/destroy lifecycle
- [ ] Work context setup with per-worker eval_stack isolation (Phase 2D fix pattern)
- [ ] Error propagation from workers to main thread
- [ ] Result collection into caller-owned buffers
- [ ] col_rel_merge_k() called after barrier for deduplication
- [ ] Merged result registered in session
- [ ] TSan clean with K=2 and K=8

#### Week 2: Integration Testing

**Task 3A-3: K-Fusion End-to-End Validation**

Files:
- `tests/test_k_fusion_e2e.c` (new: end-to-end K-fusion tests)
- `tests/meson.build` (modify: register new test)

Acceptance Criteria:
- [ ] CSPA produces 20,381 tuples in 6 iterations with K-fusion dispatch
- [ ] TC workload matches baseline fact count
- [ ] K=2, K=4, K=8 all produce correct results
- [ ] Empty delta skip optimization works with K-fusion
- [ ] ASAN clean build passes all tests
- [ ] TSan clean with K=2, K=4, K=8

#### Week 3: Performance Validation + Gate

**Task 3A-4: Performance Benchmarking**

Acceptance Criteria:
- [ ] CSPA median wall-time < 4.5s (3-run median, release build -O3)
- [ ] DOOP completes in < 30 minutes
- [ ] Workqueue overhead < 5% (measured: K=2 parallel vs K=2 sequential)
- [ ] Peak RSS: CSPA < 2.5GB, DOOP < 4GB
- [ ] Performance results documented in `docs/performance/PHASE-3A-BENCHMARK.md`

**Phase 3A Gate:**
- [ ] All 20 tests pass
- [ ] CSPA < 4.5s, DOOP < 30m
- [ ] ASAN + TSan clean
- [ ] Performance documented

---

### Phase 3B: Timestamped Delta Tracking (Weeks 4-8)

**Goal:** Replace relation-level snapshot diff with per-record timestamp-based delta tracking. This is the single highest-ROI change for closing the 91x gap.

**Owner:** Codex (implementation), Gemini (design docs), Claude (coordination)

#### Week 4: Timestamp Infrastructure

**Task 3B-1: Add Timestamp Field to col_rel_t**

Add an out-of-band timestamp array to the columnar relation structure. Timestamps are stored separately from row data to preserve all existing row comparison functions (kway_row_cmp, memcmp) without modification.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_rel_t definition, ~30 lines)
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_rel_alloc, col_rel_free, ~20 lines)

Design:
```
// Current col_rel_t (simplified):
typedef struct {
    int64_t *data;     // row-major: data[row * ncols + col]
    uint32_t nrows;
    uint32_t ncols;
    uint32_t capacity;
    // ... column names, etc.
} col_rel_t;

// Extended with timestamps (out-of-band):
typedef struct {
    int64_t *data;
    uint32_t *timestamps;  // NEW: timestamps[row] = iteration when row was produced
    uint32_t nrows;
    uint32_t ncols;
    uint32_t capacity;
    // ... column names, etc.
} col_rel_t;
```

Acceptance Criteria:
- [ ] `timestamps` array allocated alongside `data` in col_rel_alloc()
- [ ] `timestamps` freed in col_rel_free_contents()
- [ ] All existing row comparison functions unchanged (compare data only, not timestamps)
- [ ] col_rel_append_row() propagates timestamp from source
- [ ] Feature flag: `WL_FEATURE_TIMESTAMPS` (compile-time, default OFF initially)
- [ ] All 20 tests pass with flag OFF (zero behavioral change)

**Task 3B-2: Timestamp Propagation Through Operators**

Propagate timestamps through all operators in the evaluation stack.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: each col_op_* function)

Propagation Rules:
- `VARIABLE`: timestamp = 0 for EDB rows, current_iteration for IDB delta rows
- `MAP`: preserve input row timestamp
- `FILTER`: preserve input row timestamp
- `JOIN`: output timestamp = max(left_ts, right_ts)
- `ANTIJOIN`: preserve left row timestamp
- `REDUCE`: output timestamp = max(input group timestamps)
- `CONCAT`: preserve each input row's timestamp
- `CONSOLIDATE`: preserve timestamp during dedup (keep first occurrence's timestamp)
- `SEMIJOIN`: preserve left row timestamp

Acceptance Criteria:
- [ ] Each operator correctly propagates timestamps per rules above
- [ ] Unit tests for each operator's timestamp propagation
- [ ] Timestamps do NOT affect row equality comparisons (dedup ignores timestamps)
- [ ] Feature flag ON: all 20 tests pass with identical tuple counts

#### Week 5-6: Incremental Consolidation with Timestamps

**Task 3B-3: Timestamp-Based Incremental CONSOLIDATE**

Replace the current full-sort CONSOLIDATE with timestamp-aware incremental merge. Only rows with `timestamp == current_iteration` need sorting; they merge-insert into the already-sorted prefix.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_op_consolidate, ~100 lines)

Algorithm:
```
// Current: O(N log N) full sort every iteration
qsort(rel->data, rel->nrows, row_size, row_cmp);

// New: O(D log D + N) incremental merge
// 1. Partition: rows with ts < current_iter are already sorted (prefix)
// 2. Sort only new rows (ts == current_iter): O(D log D)
// 3. Merge-insert new rows into sorted prefix: O(D + N)
// 4. Dedup during merge (compare data only, not timestamps)
```

Note: `col_op_consolidate_incremental_delta()` (lines 2032-2120) already implements a similar O(D log D + N) algorithm. This task extends it to use timestamps for partitioning instead of row-count-based old/new boundary tracking.

Acceptance Criteria:
- [ ] Incremental CONSOLIDATE uses timestamps to identify new rows
- [ ] Already-sorted prefix is NOT re-sorted
- [ ] Deduplication compares data only (timestamps excluded from equality)
- [ ] Output relation remains sorted after incremental merge
- [ ] All 20 tests pass with identical tuple counts
- [ ] Late-iteration speedup: >5x vs full-sort on CSPA iterations 3-6

**Task 3B-4: Timestamp-Based Delta Computation**

Replace snapshot-diff-based delta computation with timestamp-based extraction. Currently, deltas are computed by diffing full relation snapshots. With timestamps, delta = rows where `timestamp == current_iteration` after consolidation.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: delta computation in col_eval_stratum, ~80 lines)

Current Flow:
```
// Snapshot before iteration
snap[ri] = rel->nrows;
// ... evaluate iteration ...
// Delta = rows added since snapshot
delta_nrows = rel->nrows - snap[ri];
```

New Flow:
```
// No snapshot needed
// ... evaluate iteration ...
// Delta = rows where timestamp == current_iteration (after consolidate dedup)
delta = extract_rows_by_timestamp(rel, current_iteration);
```

Acceptance Criteria:
- [ ] Delta extraction uses timestamps, not snapshot diff
- [ ] Delta contains exactly the new unique rows from current iteration
- [ ] Fixed-point detection: `delta.nrows == 0` means convergence
- [ ] Iteration count matches Phase 2D baseline for all workloads
- [ ] Tuple counts match Phase 2D baseline exactly

#### Week 7: K-Fusion + Timestamps Integration

**Task 3B-5: K-Fusion with Timestamp-Aware Deltas**

Integrate timestamp-based deltas with K-fusion parallel dispatch. Each K-copy worker uses timestamp-based delta selection instead of snapshot-based.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_op_k_fusion, K-fusion worker)

Key Change: K-fusion workers can now skip evaluation entirely when their forced-delta input has no rows with `timestamp == current_iteration`. This eliminates the "redundant recomputation" factor from the gap analysis.

Acceptance Criteria:
- [ ] K-fusion workers check delta timestamp before evaluation
- [ ] Empty-delta K-copies skip evaluation (no workqueue submission)
- [ ] Non-empty K-copies evaluate normally with timestamp propagation
- [ ] All 20 tests pass
- [ ] CSPA iteration count unchanged (6 iterations)

#### Week 8: Phase 3B Validation + Gate

**Task 3B-6: Performance Benchmarking and Correctness Validation**

Acceptance Criteria:
- [ ] CSPA median wall-time < 2.0s (3-run median, release build -O3)
- [ ] DOOP completes in < 5 minutes
- [ ] All 20 tests pass with identical tuple counts
- [ ] ASAN clean build
- [ ] TSan clean for all parallel paths
- [ ] Performance results documented in `docs/performance/PHASE-3B-BENCHMARK.md`
- [ ] DD oracle comparison: extract DD DOOP output from git history (commit 8f03049), compare tuple-by-tuple

**Phase 3B Gate:**
- [ ] All 20 tests pass
- [ ] CSPA < 2.0s, DOOP < 5m
- [ ] ASAN + TSan clean
- [ ] DD oracle tuple-exact match (DOOP)
- [ ] Performance documented

---

### Phase 3C: Arrangement Layer (Weeks 9-13)

**Goal:** Implement persistent indexed collections (arrangements) that maintain sorted, deduplicated, time-indexed state across iterations. This enables incremental joins without rebuilding hash indexes each iteration.

**Owner:** Codex (implementation), Gemini (design/docs), Claude (coordination)

#### Week 9-10: Arrangement Data Structure

**Task 3C-1: Define Arrangement Type**

An arrangement is a persistent, time-indexed collection of rows. It maintains:
- A sorted, deduplicated "compacted" prefix (rows from iterations 0..t-1)
- An unsorted "pending" buffer (rows from iteration t)
- A key index for efficient lookup during joins

Files:
- `wirelog/backend/columnar_nanoarrow.c` (add: arrangement types and functions, ~400 lines)

Design:
```c
typedef struct {
    col_rel_t compacted;       // Sorted, deduped rows from prior iterations
    col_rel_t pending;         // New rows from current iteration (unsorted)
    uint32_t *key_cols;        // Key column indices for this arrangement
    uint32_t key_count;        // Number of key columns
    // Hash index over compacted rows for O(1) probe during join
    struct {
        uint64_t *hashes;     // Hash array (parallel to compacted.data rows)
        uint32_t *chain;      // Hash chain for collision resolution
        uint32_t *buckets;    // Bucket heads
        uint32_t bucket_count;
    } index;
    uint32_t last_compacted_iter;  // Last iteration compaction ran
} col_arrangement_t;
```

Acceptance Criteria:
- [ ] `col_arrangement_create()` initializes empty arrangement with key columns
- [ ] `col_arrangement_insert()` adds rows to pending buffer with timestamps
- [ ] `col_arrangement_compact()` merges pending into compacted, rebuilds index
- [ ] `col_arrangement_probe()` returns matching rows for given key values
- [ ] `col_arrangement_free()` cleans up all resources
- [ ] Unit tests for create/insert/compact/probe lifecycle

**Task 3C-2: Hash Index Implementation**

Implement a simple open-addressing or chained hash index over arrangement key columns for O(1) average-case probe during joins.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (add: hash index functions, ~200 lines)

Acceptance Criteria:
- [ ] Hash function: FNV-1a over key column int64_t values
- [ ] Collision resolution: chaining (simpler, no resize needed for fixed-size compacted)
- [ ] Probe returns iterator over matching rows
- [ ] Index rebuild is O(N) where N = compacted row count
- [ ] Index probe is O(1) average case
- [ ] Unit tests: correctness with duplicates, collisions, empty index

#### Week 11-12: Arrangement-Based Join

**Task 3C-3: Incremental Join via Arrangements**

Replace the current per-iteration hash-join rebuild with arrangement-based incremental join. The key insight: when only delta rows change, the join only needs to process `delta x arrangement_full` and `arrangement_full x delta`, not `full x full`.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_op_join, ~200 lines)

Current Join:
```
// Every iteration: rebuild hash index on right relation, probe with left
hash_index = build_hash_index(right_full);       // O(|R|) per iteration
result = probe(left_full_or_delta, hash_index);  // O(|L|) per iteration
```

Arrangement Join:
```
// Arrangement maintains persistent index (rebuild only when compacted)
if (right_arrangement->last_compacted_iter < current_iter) {
    compact_and_reindex(right_arrangement);  // O(|delta|) amortized
}
// Probe arrangement with delta only
result = probe(left_delta, right_arrangement);   // O(|delta_L|) per iteration
// Plus: probe left arrangement with right delta
result2 = probe(right_delta, left_arrangement);  // O(|delta_R|) per iteration
```

Acceptance Criteria:
- [ ] JOIN operator uses arrangement when available (feature flag)
- [ ] Arrangement index persists across iterations (not rebuilt from scratch)
- [ ] Compaction is incremental: only pending rows merged into compacted
- [ ] Join processes only delta x full and full x delta (not full x full)
- [ ] All 20 tests pass with identical tuple counts
- [ ] ASAN + TSan clean

**Task 3C-4: Arrangement Lifecycle in Session**

Integrate arrangements into the session lifecycle. Each IDB relation that participates in joins gets an arrangement automatically.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: session management, ~100 lines)

Acceptance Criteria:
- [ ] Arrangements created for IDB relations during plan execution
- [ ] Arrangements updated each iteration (pending insert + compaction)
- [ ] Arrangements freed in session cleanup
- [ ] Non-arrangement path preserved for relations without joins (fallback)

#### Week 13: Phase 3C Validation + Gate

**Task 3C-5: Performance Benchmarking and Correctness Validation**

Acceptance Criteria:
- [ ] CSPA median wall-time < 1.5s
- [ ] DOOP completes in < 90 seconds
- [ ] All 20 tests pass with identical tuple counts
- [ ] ASAN + TSan clean
- [ ] DD oracle tuple-exact match (all workloads)
- [ ] Memory: arrangement overhead < 2x baseline RSS
- [ ] Performance results documented in `docs/performance/PHASE-3C-BENCHMARK.md`

**Phase 3C Gate:**
- [ ] All 20 tests pass
- [ ] CSPA < 1.5s, DOOP < 90s
- [ ] ASAN + TSan clean
- [ ] DD oracle match
- [ ] Performance documented

---

### Phase 3D: Dataflow Graph Scheduling (Weeks 14-16)

**Goal:** Implement a lightweight dataflow graph with frontier-based operator scheduling. Operators fire when their input frontiers advance, enabling fine-grained parallelism without explicit iteration-level synchronization.

**Owner:** Codex (implementation), Gemini (design/docs), Claude (coordination)

#### Week 14: Dataflow Graph Construction

**Task 3D-1: Dataflow Graph Type and Builder**

Build a dataflow graph from the execution plan. Each operator becomes a node; edges represent data flow between operators.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (add: dataflow graph types, ~300 lines)

Design:
```c
typedef struct {
    uint32_t id;
    wl_plan_op_type_t op_type;
    const wl_plan_op_t *plan_op;     // Original plan operator
    uint32_t *input_ids;              // IDs of upstream nodes
    uint32_t input_count;
    uint32_t *output_ids;             // IDs of downstream nodes
    uint32_t output_count;
    uint32_t frontier;                // Minimum timestamp that can still arrive
    bool ready;                       // True if all inputs have advanced past frontier
} wl_df_node_t;

typedef struct {
    wl_df_node_t *nodes;
    uint32_t node_count;
    uint32_t *topo_order;             // Topological sort for scheduling
} wl_df_graph_t;
```

Acceptance Criteria:
- [ ] `wl_df_graph_build()` constructs dataflow graph from wl_plan_t
- [ ] Graph correctly represents operator dependencies
- [ ] Topological sort computed at build time
- [ ] Frontier initialized to 0 for all nodes
- [ ] Unit tests: graph construction for TC, CSPA, DOOP plans

**Task 3D-2: Frontier Tracking Protocol**

Implement the pointstamp-based progress tracking protocol. Each node tracks its frontier (minimum timestamp that can still arrive on any input). When all inputs have advanced past a timestamp, the node can process data at that timestamp.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (add: frontier tracking, ~200 lines)

Simplified Naiad Protocol (adapted for wirelog):
```
// When node N produces output at timestamp t:
//   1. Send data downstream
//   2. Update N's output frontier to t+1
//   3. Notify downstream nodes

// When node N receives frontier advance from input I:
//   1. Update N's input frontier for I
//   2. If all inputs have frontier > t: N can process timestamp t
//   3. Mark N as ready

// Fixed-point: when all nodes have frontier == MAX, evaluation is complete
```

Acceptance Criteria:
- [ ] Frontier advance correctly propagates through the graph
- [ ] Nodes marked ready only when ALL inputs have advanced
- [ ] Fixed-point detection: all nodes frontier == MAX
- [ ] No deadlock: progress guaranteed if any node has pending data
- [ ] Unit tests: frontier propagation for linear, diamond, and cyclic graphs

#### Week 15: Dataflow-Scheduled Evaluation

**Task 3D-3: Replace Semi-Naive Loop with Dataflow Scheduler**

Replace the outer fixed-point iteration loop with a frontier-driven scheduler that fires operators when their inputs are ready.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: col_eval_stratum, ~200 lines)

Current:
```c
for (iter = 0; iter < MAX_ITER; iter++) {
    for (ri = 0; ri < nrels; ri++) {
        evaluate_relation(ri);
    }
    consolidate_all();
    if (no_new_facts) break;
}
```

Dataflow:
```c
wl_df_graph_t *graph = wl_df_graph_build(plan);
while (!wl_df_all_done(graph)) {
    wl_df_node_t *ready = wl_df_next_ready(graph);
    if (!ready) break;  // deadlock detection

    // Fire operator: process data at ready node's frontier timestamp
    wl_df_fire_node(ready, session);

    // Advance frontiers downstream
    wl_df_advance_frontiers(graph, ready);
}
```

Acceptance Criteria:
- [ ] Dataflow scheduler produces identical results to semi-naive loop
- [ ] Operators fire in correct dependency order
- [ ] Fixed-point convergence in same number of logical iterations
- [ ] Fallback to semi-naive loop via feature flag
- [ ] All 20 tests pass with identical tuple counts

**Task 3D-4: Fine-Grained Parallelism via Workqueue**

Submit independent ready nodes to workqueue for parallel execution. Nodes with no data dependencies can fire simultaneously.

Files:
- `wirelog/backend/columnar_nanoarrow.c` (modify: scheduler, ~100 lines)

Acceptance Criteria:
- [ ] Independent ready nodes submitted to workqueue in parallel
- [ ] Dependent nodes wait for upstream completion
- [ ] Frontier advances synchronized via barrier
- [ ] TSan clean with parallel node execution
- [ ] Workqueue overhead < 5%

#### Week 16: Final Validation + Gate

**Task 3D-5: Final Performance Benchmarking and DD Oracle Validation**

Acceptance Criteria:
- [ ] CSPA median wall-time < 1.2s
- [ ] DOOP completes in < 47 seconds (DD parity)
- [ ] All 20 tests pass with identical tuple counts
- [ ] ASAN + TSan clean
- [ ] DD oracle tuple-exact match for ALL workloads (15 benchmarks + DOOP)
- [ ] Peak RSS: CSPA < 3GB, DOOP < 4GB
- [ ] Performance results documented in `docs/performance/PHASE-3D-BENCHMARK.md`
- [ ] Full Phase 3 completion report in `docs/performance/PHASE-3-FINAL-REPORT.md`

**Phase 3D Gate (Final):**
- [ ] All 20 tests pass
- [ ] DOOP <= 47s
- [ ] ASAN + TSan clean
- [ ] DD oracle tuple-exact match (all workloads)
- [ ] Performance documented
- [ ] Architecture documented in updated ARCHITECTURE.md

---

## Integration Strategy: K-Fusion to Timely

### Layer 1: K-Fusion Foundation (Phase 3A)

K-fusion remains the parallelism primitive. The workqueue + per-worker arena + collect-then-merge pattern is proven and reused throughout.

```
Existing:  K-copy → sequential evaluate → CONSOLIDATE full-sort
Phase 3A:  K-copy → parallel workqueue  → col_rel_merge_k (dedup)
```

### Layer 2: Timestamps on K-Fusion (Phase 3B)

Timestamps compose naturally with K-fusion. Each K-copy worker produces timestamped rows. The merge step preserves timestamps. Delta extraction uses timestamps.

```
Phase 3B:  K-copy → parallel workqueue → merge_k (dedup, preserve timestamps)
           Delta = rows where timestamp == current_iteration
           CONSOLIDATE = incremental merge of new rows into sorted prefix
```

### Layer 3: Arrangements Replace Raw Relations (Phase 3C)

IDB relations that participate in joins transition from raw `col_rel_t` to `col_arrangement_t`. The arrangement maintains a persistent hash index that is incrementally updated (not rebuilt) each iteration.

```
Phase 3C:  K-copy → parallel workqueue → merge into arrangement
           JOIN reads from arrangement index (O(1) probe)
           Compaction happens once per iteration (amortized)
```

### Layer 4: Dataflow Replaces Iteration Loop (Phase 3D)

The semi-naive loop is replaced by a dataflow scheduler. Operators fire when frontiers advance. K-fusion becomes one scheduling strategy among several.

```
Phase 3D:  Dataflow graph built from plan
           Scheduler fires ready operators via workqueue
           Frontiers track progress (replaces iteration counter)
           K-fusion operators are graph nodes with parallel sub-dispatch
```

### Backward Compatibility

Each layer is gated by a feature flag:
- `WL_FEATURE_K_FUSION_DISPATCH` (Phase 3A) - default ON after gate
- `WL_FEATURE_TIMESTAMPS` (Phase 3B) - default ON after gate
- `WL_FEATURE_ARRANGEMENTS` (Phase 3C) - default ON after gate
- `WL_FEATURE_DATAFLOW` (Phase 3D) - default ON after gate

All flags can be disabled independently. With all flags OFF, the evaluator behaves identically to Phase 2D.

---

## Pre-Mortem: 3 Failure Scenarios

### Failure 1: Timestamp Propagation Corrupts Deduplication

**Scenario:** Adding timestamps to rows causes CONSOLIDATE to treat rows with different timestamps but identical data as distinct. Fact counts inflate, fixed-point never converges, DOOP produces incorrect results.

**Why it could happen:**
- A code path accidentally includes timestamps in row equality comparison
- `kway_row_cmp` or `memcmp` is called on a range that includes the timestamp field
- JOIN output timestamp assignment creates duplicate-looking rows with different timestamps
- REDUCE aggregation double-counts rows with different timestamps

**Probability:** MEDIUM (this is the most likely failure mode)

**Mitigation:**
1. **Out-of-band storage:** Timestamps are stored in a separate `uint32_t[]` array, NOT interleaved with row data. This structurally prevents `memcmp(row_a, row_b, ncols * sizeof(int64_t))` from seeing timestamps.
2. **Zero code changes to comparators:** `kway_row_cmp`, `row_cmp_r`, and all dedup memcmp calls operate on `data[]` only. They are never passed timestamp memory.
3. **Invariant assertion:** After every CONSOLIDATE, assert: `no two rows in output have identical data columns` (regardless of timestamp).
4. **Incremental validation:** After each operator modification, run all 20 tests and compare tuple counts before proceeding to the next operator.
5. **Feature flag escape:** If timestamps cause correctness issues, disable `WL_FEATURE_TIMESTAMPS` and fall back to snapshot-based deltas. The system continues to work correctly (just slower).

**Detection:** Any test suite tuple count differs from Phase 2D baseline. Iteration count increases beyond baseline. ASAN reports buffer overread in comparison functions.

### Failure 2: Arrangement Index Corrupts Join Results

**Scenario:** The persistent hash index over arrangements produces incorrect join results due to stale entries, hash collisions, or incorrect compaction. DOOP output diverges from DD oracle.

**Why it could happen:**
- Hash function has poor distribution for int64_t interned symbol IDs (many collisions)
- Compaction fails to remove deleted/superseded rows from the index
- Index probe returns rows from wrong timestamps (stale data)
- Concurrent arrangement access during K-fusion violates thread safety

**Probability:** MEDIUM-HIGH (hash index correctness under incremental update is notoriously tricky)

**Mitigation:**
1. **Dual-path validation:** Run every workload with both arrangement-based join AND traditional hash-join. Assert output equality. Keep traditional join as fallback.
2. **Index rebuild verification:** After each compaction, verify index integrity by probing every row in compacted and checking that the index returns it.
3. **Conservative compaction:** Compact only when pending buffer exceeds 20% of compacted size. This limits compaction frequency and reduces bug surface.
4. **DD oracle comparison:** For DOOP, extract DD output from commit 8f03049 and compare tuple-by-tuple after each Phase 3C milestone.
5. **Thread safety:** Arrangements are NOT shared between K-fusion workers. Each worker has its own read-only snapshot of the arrangement (copy-on-read for pending buffer, shared-immutable for compacted).

**Detection:** DD oracle comparison fails. Join output row count differs between arrangement and traditional paths. ASAN reports use-after-free in index probe.

### Failure 3: Dataflow Scheduler Deadlocks on Cyclic Graphs

**Scenario:** The dataflow scheduler enters a state where no nodes are ready but the graph is not complete. Recursive strata with cyclic dependencies cause the scheduler to wait indefinitely.

**Why it could happen:**
- Frontier advancement logic does not account for cyclic dependencies (recursive rules)
- The Naiad pointstamp protocol requires careful handling of cycles via "loop contexts" which are not implemented
- A node waits for its own output (self-referential rule like `Path(x,y) :- Edge(x,z), Path(z,y)`)
- Frontier computation is O(V+E) per message; with large DOOP graphs (136 rules), this becomes the bottleneck itself

**Probability:** HIGH (this is the most architecturally challenging part)

**Mitigation:**
1. **Hybrid scheduling:** For recursive strata, use the proven semi-naive loop as the outer driver. Dataflow scheduling applies WITHIN each iteration (operator ordering within one delta round). This avoids the cyclic frontier problem entirely.
2. **Iteration-scoped frontiers:** Instead of global timestamps spanning all iterations, use iteration-local frontiers. Each iteration resets frontiers. This is simpler than full Naiad and sufficient for semi-naive evaluation.
3. **Deadlock detection:** If no nodes become ready for 100ms (configurable), log the graph state and fall back to sequential evaluation for that stratum.
4. **Incremental adoption:** Start with dataflow scheduling for non-recursive strata only (already known to work with workqueue parallelism). Add recursive strata support incrementally.
5. **Feature flag escape:** `WL_FEATURE_DATAFLOW=OFF` falls back to the proven semi-naive loop. Dataflow is an optimization, not a correctness requirement.

**Detection:** Benchmark hangs indefinitely. Deadlock detection fires. Iteration count exceeds `MAX_ITERATIONS` (currently 1000).

---

## Test Plan

### Unit Tests

#### Phase 3A: K-Fusion Dispatch
| Test | Description | Validates |
|------|-------------|-----------|
| `test_kfusion_plan_gen_k2` | Plan generator emits K_FUSION for K=2 relation | Task 3A-1 |
| `test_kfusion_plan_gen_k8` | Plan generator emits K_FUSION for K=8 relation | Task 3A-1 |
| `test_kfusion_plan_gen_fallback` | Non-K-copy relations get normal plans | Task 3A-1 |
| `test_kfusion_dispatch_k2` | K=2 parallel dispatch produces correct output | Task 3A-2 |
| `test_kfusion_dispatch_k8` | K=8 stress test | Task 3A-2 |
| `test_kfusion_empty_delta_skip` | Empty delta K-copies not submitted to workqueue | Task 3A-2 |
| `test_kfusion_arena_isolation` | Per-worker arenas do not leak across workers | Task 3A-2 |
| `test_kfusion_error_propagation` | Worker failure propagates to main thread | Task 3A-2 |

#### Phase 3B: Timestamped Deltas
| Test | Description | Validates |
|------|-------------|-----------|
| `test_ts_variable_edb` | VARIABLE assigns timestamp=0 for EDB rows | Task 3B-2 |
| `test_ts_variable_idb` | VARIABLE assigns current_iteration for IDB deltas | Task 3B-2 |
| `test_ts_join_max` | JOIN output carries max(left_ts, right_ts) | Task 3B-2 |
| `test_ts_filter_preserve` | FILTER preserves input timestamp | Task 3B-2 |
| `test_ts_consolidate_dedup_ignores_ts` | CONSOLIDATE dedup compares data only | Task 3B-3 |
| `test_ts_incremental_merge` | Incremental merge only sorts new rows | Task 3B-3 |
| `test_ts_delta_extraction` | Delta = rows with timestamp == current_iter | Task 3B-4 |
| `test_ts_fixed_point_convergence` | Iteration count matches baseline | Task 3B-4 |

#### Phase 3C: Arrangements
| Test | Description | Validates |
|------|-------------|-----------|
| `test_arr_create_insert` | Create arrangement, insert rows | Task 3C-1 |
| `test_arr_compact_sort` | Compaction sorts and deduplicates pending | Task 3C-1 |
| `test_arr_probe_exact` | Probe returns matching rows for key | Task 3C-1 |
| `test_arr_probe_missing` | Probe returns empty for non-existent key | Task 3C-1 |
| `test_arr_hash_collision` | Hash index handles collisions correctly | Task 3C-2 |
| `test_arr_incremental_compact` | Incremental compaction merges pending correctly | Task 3C-2 |
| `test_arr_join_k2` | Arrangement-based join for K=2 matches traditional | Task 3C-3 |
| `test_arr_join_k8` | Arrangement-based join for K=8 matches traditional | Task 3C-3 |

#### Phase 3D: Dataflow
| Test | Description | Validates |
|------|-------------|-----------|
| `test_df_graph_build_tc` | Graph construction for TC plan | Task 3D-1 |
| `test_df_graph_build_cspa` | Graph construction for CSPA plan | Task 3D-1 |
| `test_df_frontier_linear` | Frontier propagation in linear graph | Task 3D-2 |
| `test_df_frontier_diamond` | Frontier propagation in diamond graph | Task 3D-2 |
| `test_df_frontier_cycle` | Frontier handling for recursive strata | Task 3D-2 |
| `test_df_scheduler_tc` | Dataflow scheduler produces correct TC output | Task 3D-3 |
| `test_df_scheduler_cspa` | Dataflow scheduler produces correct CSPA output | Task 3D-3 |
| `test_df_parallel_independent` | Independent nodes fire in parallel | Task 3D-4 |

### Integration Tests

| Test | Validates | Phase |
|------|-----------|-------|
| `test_seminaive_kfusion_cspa` | CSPA: 20,381 tuples, 6 iterations with K-fusion | 3A |
| `test_seminaive_kfusion_doop` | DOOP completes with correct output via K-fusion | 3A |
| `test_seminaive_ts_cspa` | CSPA with timestamp deltas matches baseline | 3B |
| `test_seminaive_ts_doop` | DOOP with timestamp deltas matches DD oracle | 3B |
| `test_seminaive_arr_cspa` | CSPA with arrangements matches baseline | 3C |
| `test_seminaive_arr_doop` | DOOP with arrangements matches DD oracle | 3C |
| `test_dataflow_all_workloads` | All 15 workloads produce correct results via dataflow | 3D |
| `test_combined_all_features` | All features ON: K-fusion + timestamps + arrangements + dataflow | 3D |

### End-to-End Regression

- **All 20 existing test suites** must pass at every phase gate (19 OK + 1 for DOOP which transitions from EXPECTEDFAIL to PASS)
- **Fact counts** must match Phase 2D baseline exactly
- **Iteration counts** must not increase

### Performance Benchmarks

| Benchmark | Phase 3A Target | Phase 3B Target | Phase 3C Target | Phase 3D Target |
|-----------|----------------|----------------|----------------|----------------|
| CSPA wall-time | <4.5s | <2.0s | <1.5s | <1.2s |
| DOOP wall-time | <30m | <5m | <90s | <47s |
| CSPA peak RSS | <2.5GB | <2.5GB | <3.0GB | <3.0GB |
| DOOP peak RSS | <4.0GB | <4.0GB | <4.0GB | <4.0GB |
| Workqueue overhead | <5% | <5% | <5% | <5% |

### Observability

**Performance Instrumentation (all phases):**
- Per-iteration wall-time logging
- Per-operator time breakdown (CONSOLIDATE, JOIN, VARIABLE percentages)
- Delta size per iteration (track convergence rate)
- Workqueue task duration histogram
- Arena allocation high-water mark per worker

**Timestamp Instrumentation (Phase 3B+):**
- Timestamp distribution histogram per relation per iteration
- Incremental sort ratio: new_rows / total_rows per CONSOLIDATE
- Delta extraction accuracy: verify delta subset of full relation

**Arrangement Instrumentation (Phase 3C+):**
- Hash index load factor per arrangement
- Compaction frequency and duration
- Index probe hit/miss ratio
- Arrangement memory overhead vs raw relation

**Dataflow Instrumentation (Phase 3D):**
- Node fire count per operator type
- Frontier advance latency
- Parallel node execution ratio (concurrent/sequential)
- Deadlock detection trigger count (should be 0)

---

## Team Division of Labor

### Codex: Implementation (Primary)

| Phase | Tasks | Estimated Lines |
|-------|-------|----------------|
| 3A | Plan gen K-fusion nodes, col_op_k_fusion dispatch | ~500 |
| 3B | Timestamps in col_rel_t, operator propagation, incremental CONSOLIDATE | ~2,000 |
| 3C | Arrangement type, hash index, arrangement-based JOIN | ~2,500 |
| 3D | Dataflow graph, frontier tracking, scheduler | ~1,500 |

### Gemini: Design & Documentation

| Phase | Deliverables |
|-------|-------------|
| 3A | K-fusion dispatch design review, test plan documentation |
| 3B | Timestamp propagation rules specification, operator semantics doc |
| 3C | Arrangement data structure design, hash index analysis, Naiad concept mapping |
| 3D | Dataflow graph specification, frontier protocol formal description, ARCHITECTURE.md update |

### Claude: Coordination & Verification

| Phase | Responsibilities |
|-------|-----------------|
| All | Phase gate evaluation, benchmark execution, DD oracle comparison |
| All | ASAN/TSan validation runs, regression suite execution |
| All | Cross-phase integration review, feature flag management |
| All | Progress tracking, risk assessment, blocker escalation |

### Parallel Work Opportunities

| Weeks | Codex | Gemini | Claude |
|-------|-------|--------|--------|
| 1-2 | Task 3A-1, 3A-2 | Design doc for Phase 3B timestamps | DD oracle extraction from git history |
| 3 | Task 3A-3, 3A-4 | Timestamp propagation rules spec | Phase 3A benchmarks |
| 4-5 | Task 3B-1, 3B-2 | Arrangement design doc | Phase 3B unit test validation |
| 6-7 | Task 3B-3, 3B-4 | Hash index analysis | Incremental CONSOLIDATE benchmarks |
| 8 | Task 3B-5, 3B-6 | Phase 3C design review | Phase 3B gate evaluation |
| 9-10 | Task 3C-1, 3C-2 | Naiad concept mapping doc | Arrangement unit tests |
| 11-12 | Task 3C-3, 3C-4 | Dataflow graph spec | Arrangement integration tests |
| 13 | Phase 3C gate work | Frontier protocol spec | Phase 3C gate evaluation |
| 14 | Task 3D-1, 3D-2 | ARCHITECTURE.md update | Dataflow unit tests |
| 15-16 | Task 3D-3, 3D-4, 3D-5 | Final documentation | Phase 3D gate + final report |

---

## DD Correctness Oracle Strategy

### Step 1: Extract DD Output from Git History

DD backend was removed at commit 8f03049. To extract the oracle:

```bash
# Checkout DD backend commit
git stash
git checkout 8f03049

# Build with DD backend
# (requires Rust toolchain, cargo)
cargo build --release

# Run DOOP with DD backend, capture output
./target/release/wirelog-dd --workload doop --data bench/data/doop > /tmp/dd-doop-oracle.csv

# Capture all 15 workloads
for workload in tc reach cspa ...; do
    ./target/release/wirelog-dd --workload $workload > /tmp/dd-${workload}-oracle.csv
done

# Return to main
git checkout main
git stash pop
```

### Step 2: Normalize Output Format

Both DD and columnar backends produce relations as sorted tuples. Normalize:
- Sort all output rows lexicographically
- Remove any header/metadata lines
- Store as sorted CSV: one row per line, columns comma-separated

### Step 3: Tuple-Exact Comparison

```bash
# Compare wirelog output against DD oracle
diff <(sort wirelog-doop-output.csv) <(sort /tmp/dd-doop-oracle.csv)
# Zero diff = tuple-exact match
```

### Step 4: Automated Regression

Add oracle comparison to the test suite:
```c
// test_dd_oracle.c
// Load DD oracle from file
// Run wirelog with same input
// Assert output matches oracle tuple-by-tuple
```

---

## Risk Register

| Risk | Probability | Impact | Mitigation | Phase |
|------|------------|--------|------------|-------|
| Timestamp corrupts dedup | Medium | Critical | Out-of-band storage, invariant assertions | 3B |
| Arrangement index bugs | Medium-High | High | Dual-path validation, DD oracle | 3C |
| Dataflow deadlock on cycles | High | High | Hybrid scheduling, iteration-scoped frontiers | 3D |
| DOOP 47s not achievable | Medium | High | Accept 90s as Phase 3 target, defer to Phase 4 | 3D |
| Memory explosion with arrangements | Low | Medium | RSS monitoring, compaction limits | 3C |
| K-fusion overhead > 5% on K=2 | Low | Low | Sequential fallback for K<=2 | 3A |
| Race condition in arrangement access | Medium | Critical | Copy-on-read, no shared mutable state | 3C |
| DD oracle not extractable | Low | Medium | Use Phase 2D baseline as fallback oracle | Setup |

---

## ADR: Architectural Decision Record

### Decision

Implement Timely-like dataflow in 4 phases (K-Fusion -> Timestamps -> Arrangements -> Dataflow) over 16 weeks, targeting DOOP 47s parity with DD+Timely.

### Decision Drivers

1. **Root cause hierarchy:** The 91x gap is caused by coarse delta tracking (~40-50x), full-sort consolidation (~10-20x), redundant K-copy evaluation (~2-4x), and no persistent indexes (~2-5x). Phases address these in order of ROI.
2. **Incremental delivery:** Each phase delivers measurable improvement with explicit gates. No phase exceeds 4 weeks without results.
3. **Feasibility:** ~6,500 total new lines over 16 weeks with 3-person team. Each phase builds on proven infrastructure.

### Alternatives Considered

| Option | Effort | DOOP Target | Risk | Verdict |
|--------|--------|-------------|------|---------|
| K-Fusion only (Option A) | 2-3 weeks | 10-30m | Low | Necessary but insufficient (addresses 1 of 4 root causes) |
| Timestamps only (Option B) | 4-6 weeks | 5-15m | Medium | Addresses root cause but misses parallelism and index gains |
| Full Timely from scratch (Option C) | 12-16 weeks | ~47s | Very High | High risk monolithic rewrite, no incremental value |
| **Phased A+B+C+D** | **16 weeks** | **47s** | **Medium** | **Incremental, gated, leverages existing infrastructure** |

### Why Phased Was Chosen

1. **P1 (Phased Value):** Each phase delivers measurable improvement. Phase 3A by week 3, 3B by week 8, 3C by week 13, 3D by week 16.
2. **P2 (C11 Purity):** All phases remain pure C11 + pthreads. No new dependencies.
3. **P3 (Correctness):** Feature flags enable instant fallback. DD oracle validates every phase.
4. **P4 (Leverage):** K-fusion (70% built), workqueue (complete), arena (complete), incremental consolidate (complete) all reused.
5. **P5 (Race Prevention):** Collect-then-merge pattern enforced at every parallel boundary.

### Consequences

**Positive:**
- DOOP drops from 71m50s to target 47s (99.9% improvement if achieved)
- CSPA drops from 6.0s to target <1.2s (80% improvement)
- Architecture becomes a genuine Timely-like runtime in C11
- Foundation for FPGA integration (arrangements as DMA transfer units)
- DD oracle comparison validates every step

**Negative:**
- ~6,500 new lines roughly doubles backend complexity
- Arrangement memory overhead (~2x for indexed relations)
- 4 feature flags add configuration surface
- Dataflow scheduler may not fully handle all recursive patterns (hybrid approach)
- 16 weeks is a significant investment

**Neutral:**
- Workqueue infrastructure proven in Phase 2D carries forward unchanged
- Plan generation changes (Phase 3A) are localized to exec_plan_gen.c
- Public API (session.h, backend.h) unchanged throughout

### Follow-ups

1. **After Phase 3A (Week 3):** If DOOP < 5m with K-fusion alone, re-evaluate whether Phase 3B-3D are needed.
2. **After Phase 3C (Week 13):** If DOOP < 60s with arrangements, evaluate whether Phase 3D dataflow is worth the complexity.
3. **Phase 4 Planning:** Based on Phase 3 data, decide between:
   - FPGA backend via arrangement-based DMA transfer
   - Distributed execution via network-aware arrangements
   - Magic set optimization for query-specific performance

---

## Success Criteria (Final)

Phase 3 is COMPLETE when ALL of the following are met:

- [ ] DOOP wall-time <= 47 seconds (DD parity)
- [ ] CSPA wall-time < 1.2 seconds
- [ ] All 20 test suites pass (DOOP transitions from EXPECTEDFAIL to PASS)
- [ ] Tuple-exact match with DD oracle for all workloads
- [ ] ASAN clean build
- [ ] TSan clean for all concurrent code paths
- [ ] Peak RSS: CSPA < 3GB, DOOP < 4GB
- [ ] Updated ARCHITECTURE.md documenting Timely dataflow runtime
- [ ] Performance report in `docs/performance/PHASE-3-FINAL-REPORT.md`

---

**Document Version:** 1.0
**Generated:** 2026-03-08
**Format:** RALPLAN-DR (Principles, Decision Drivers, Options, Pre-Mortem, Test Plan, ADR)
**Review Required:** Architect + Critic consensus before execution begins
**Saved To:** `docs/timely/TIMELY-PHASE-3-PLAN.md`
