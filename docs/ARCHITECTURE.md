# Wirelog Architecture

**Last Updated:** 2026-03-08
**Version:** 0.11.0

---

## Table of Contents

1. [Overview](#overview)
2. [Core Components](#core-components)
3. [Evaluator Architecture](#evaluator-architecture)
4. [K-Fusion Optimization](#k-fusion-optimization)
5. [Performance Characteristics](#performance-characteristics)
6. [Extensibility](#extensibility)

---

## Overview

Wirelog is a datalog engine with a pure C11 columnar backend. It evaluates Datalog programs via semi-naive fixed-point iteration, tracking deltas between iterations to enable incremental updates.

**Key Characteristics:**
- **Execution Model**: Semi-naive evaluation with delta tracking
- **Backend**: Pure C11 columnar storage (nanoarrow format)
- **Parallelism**: Workqueue-based task parallelism (Phase 2B+)
- **Memory**: Arena-based allocation for efficient multi-threaded execution
- **Optimization**: K-fusion parallelism for multi-copy relations

---

## Core Components

### Parser & IR (`wirelog/parser/`, `wirelog/ir/`)
- **Lexer/Parser**: Parse Datalog source into Abstract Syntax Tree
- **IR (Intermediate Representation)**: Normalized AST with validation
- **Stratification**: Dependency analysis and stratum ordering
- **Interning**: Symbol table for efficient string/identifier management

### Optimizer (`wirelog/passes/`)
- **Fusion**: Combine adjacent operators into compound operations
- **JPP (Predicate Push-down)**: Push filters early in evaluation
- **SIP (Semi-Join Push-down)**: Optimize joins via semi-joins

### Plan Generation (`wirelog/exec_plan_gen.c`)
- **Plan Structure**: Compile Datalog programs to operator sequences
- **Multi-way Delta Expansion**: Create K-copy evaluation for relations with K ≥ 2 IDB body atoms
- **Materialization Hints**: Mark shared join prefixes for CSE reuse

### Backend Execution (`wirelog/backend/columnar_nanoarrow.c`)
- **Operator Dispatch**: Execute operators on columnar data
- **Columnar Storage**: Row-major int64_t arrays for efficient SIMD
- **Session Management**: Maintain relations across iterations
- **Operator Implementations**: VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE, CONCAT, CONSOLIDATE, SEMIJOIN, K_FUSION

---

## Evaluator Architecture

### Semi-Naive Evaluation

The semi-naive evaluator executes relations via fixed-point iteration:

```
Iteration 0: Evaluate base case (E₀) using full relations
Iteration 1-∞: Evaluate delta case (Δ) using deltas from prior iteration
Loop while: Iteration produces new facts (Δ is non-empty)
```

**Fixed-Point Property**: For acyclic Datalog, convergence is guaranteed in polynomial iterations (typically 6-10 for most workloads).

### Delta Tracking

Relations are stored in two forms:
- **Full relation** (`rel_name`): Complete set of facts
- **Delta relation** (`$d$rel_name`): New facts from last iteration

On each iteration:
1. **Iteration 0**: Uses full relations as base case
2. **Iteration 1+**: Uses deltas for incremental computation
3. **Consolidation**: Merge iteration results back into full relation

### K-Copy Relations

When a recursive relation has K ≥ 2 occurrences in rule bodies (IDB atoms), the evaluator creates K separate evaluation paths:

```
Original Rule: `Path(x,y) :- Edge(x,z), Path(z,y)`
               (Path is self-referential, K=1)

Multi-way Rule: `TCReach(x,y) :- Path1(x,z), Path2(z,y)`
                (TCReach depends on 2 Path instances, K=2)
                Expansion: Path1_copy1, Path2_copy1, Path1_copy2, Path2_copy2
```

**Current Implementation**: K-copy relations use sequential evaluation with CONSOLIDATE merging.
**Future K-Fusion Parallelism**: Would use parallel workqueue dispatch for faster evaluation.

---

## K-Fusion Optimization

### Motivation

Multi-way recursive relations generate K separate evaluation paths (K-copies). Current implementation evaluates them sequentially:

```
Sequential: Evaluate_K1 -> Evaluate_K2 -> ... -> Consolidate
Wall-time: O(K × iteration_time)
```

K-Fusion optimization enables parallel evaluation:

```
Parallel:  Evaluate_K1 --------\
           Evaluate_K2 -------- Consolidate
           ...                 /
           Evaluate_KN ------/
Wall-time: O(max(iteration_time) + merge_time)
```

### Implementation Layers

#### Layer 1: Merge Algorithm (Complete ✅)

**Function**: `col_rel_merge_k()` in `columnar_nanoarrow.c:2138`

- **Purpose**: Merge K sorted relations with on-the-fly deduplication
- **Algorithms**:
  - K=1: Passthrough with in-place dedup
  - K=2: Optimized 2-pointer merge
  - K≥3: Pairwise recursive merge
- **Correctness**: Uses lexicographic int64_t comparison (kway_row_cmp)
- **Thread-Safety**: Callable from main thread after workqueue barrier

#### Layer 2: Operator Infrastructure (Complete ✅)

**Enum**: `WL_PLAN_OP_K_FUSION = 9` in `exec_plan.h`

**Operator Handler**: `col_op_k_fusion()` in `columnar_nanoarrow.c:2301`

- **Status**: Infrastructure complete, awaits plan generation for actual dispatch
- **Integration**: Case statement in `col_eval_relation_plan()` (line 2571-2573)
- **Backward Compatibility**: Non-K-fusion operators unchanged

#### Layer 3: Worker Task (Complete ✅)

**Function**: `col_op_k_fusion_worker()` in `columnar_nanoarrow.c:2285`

- **Purpose**: Worker thread entry point for parallel evaluation
- **Thread-Safety Pattern**:
  - Per-worker eval_stack: No sharing between workers
  - Per-worker arena: Independent memory allocation
  - Read-only session reference: Safe concurrent access

#### Layer 4: Workqueue Integration (Ready for Implementation)

**Requires**:
1. Per-worker arena allocation
2. Workqueue create/submit/wait_all/destroy lifecycle
3. Result collection and in-memory merge
4. Session registration of merged result

**Interfaces Available**:
- `wl_workqueue_create(num_workers)`
- `wl_workqueue_submit(wq, worker_fn, ctx)`
- `wl_workqueue_wait_all(wq)`
- `wl_workqueue_destroy(wq)`

### Plan Generation Integration (Future Phase)

**Current Status**: Sequential evaluation via CONSOLIDATE
**Required for Parallelism**: Plan generation to create K_FUSION nodes with metadata

See `docs/performance/PLAN-GENERATION-STRATEGY.md` for detailed roadmap.

### Performance Characteristics

**Target Improvements** (with full K-Fusion implementation):
- CSPA (K=2): 30-40% improvement (28.7s → 17-20s)
- DOOP (K=8): 50-60% improvement (enables 8-way joins)

**Current Sequential Implementation**:
- Baseline: CSPA 28.7s (Phase 2B)
- Uses proven CONSOLIDATE merging
- Correct and efficient without parallelization overhead

---

## Performance Characteristics

### Bottleneck Analysis

Profiling reveals the primary bottleneck (Phase 2B):

1. **K-Copy Evaluation**: ~60-70% of wall-time
   - Sequential evaluation of K copies
   - Dominated by merge-sort in CONSOLIDATE

2. **Other Operations**: ~30-40% of wall-time
   - JOIN execution
   - Delta tracking
   - Operator dispatch overhead

**See**: `docs/performance/BOTTLENECK-PROFILING-ANALYSIS.md`

### Memory Usage

- **Baseline**: CSPA ~1.5GB, DOOP not feasible (relation explosion with K=8)
- **With K-Fusion**: Expected <4GB for K=8 parallelism

### Iteration Counts

- **CSPA**: 6 iterations (fixed-point achieved)
- **DOOP**: Currently times out at K=8; K-Fusion should enable completion

---

## Extensibility

### Adding New Operators

1. **Define operator type** in `exec_plan.h` (`wl_plan_op_type_t`)
2. **Implement handler** in `columnar_nanoarrow.c` (e.g., `col_op_newop()`)
3. **Add case statement** in `col_eval_relation_plan()` switch
4. **Write unit tests** in `tests/` with meson registration
5. **Document** in ARCHITECTURE.md and relevant design docs

### Parallelization Patterns

The workqueue + per-worker arena pattern is reusable for future optimizations:

- **Pattern**: Create workers → submit tasks → collect results → barrier → merge
- **Thread-Safety**: Per-worker exclusive resources avoid contention
- **Example**: K-Fusion dispatch (future implementation)

### Configuration

Arena allocation strategy can be tuned via:
- `ARENA_SIZE` macro in `arena/arena.c`
- Worker count in workqueue creation
- Delta relation materialization hints

---

## Testing & Validation

### Unit Tests
- `tests/test_k_fusion_merge.c`: Merge algorithm correctness (5 tests)
- `tests/test_k_fusion_dispatch.c`: Dispatch functionality (7 tests)
- `tests/test_consolidate_kway_merge.c`: CONSOLIDATE operator (multiple cases)

### Regression Tests
- 15 workloads in `bench/` directory
- Full suite: `meson test -C build`
- Result validation: fact counts match baseline

### Performance Validation
- Baseline profiling: `perf record -e cycles,instructions`
- CSPA wall-time measurement: 3-run median
- DOOP breakthrough validation: < 5 minute target

---

## References

- **K-Fusion Design**: `docs/performance/K-FUSION-DESIGN.md`
- **K-Fusion Architecture**: `docs/performance/K-FUSION-ARCHITECTURE.md`
- **Plan Generation Strategy**: `docs/performance/PLAN-GENERATION-STRATEGY.md`
- **Bottleneck Analysis**: `docs/performance/BOTTLENECK-PROFILING-ANALYSIS.md`
- **Specialist Review**: `docs/performance/SPECIALIST-REVIEW-SYNTHESIS.md`

---

**Maintained by**: Wirelog development team
**Last Review**: 2026-03-08 (Architect-verified K-fusion infrastructure)
