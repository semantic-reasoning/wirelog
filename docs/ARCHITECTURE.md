# wirelog Architecture Document

**Project**: wirelog - Embedded-to-Enterprise Datalog Engine
**Copyright**: Copyright (C) CleverPlant
**Date**: 2026-03-01
**Status**: Phase 0 Complete, Phase 1 Complete

---

## Core Requirements

1. **Multi-Target (Unified Start)**: Both embedded and enterprise targets **start with DD integration**
2. **FPGA Acceleration Ready**: Lightweight design enabling future FPGA acceleration without heavy libraries
3. **Strict Layering**: Layer separation for future optimization flexibility
4. **nanoarrow Deferred**: Not needed initially; added during embedded optimization phase
5. **C11 Foundation**: C11 for broad compatibility and modern features (_Static_assert, stdatomic)

---

## 1. Core Design Principles

### 1.1 Multi-Target Architecture (Embedded ↔ Enterprise)

**Phase 0-1 (Current): All DD-based**
```
wirelog core (C11)
├─ Parser (Datalog → IR)
├─ Optimizer (Fusion, JPP, SIP)
└─ DD Executor (Rust FFI)
    │
    ├─ [Embedded Target]
    │   ├─ ARM/RISC-V CPU targets
    │   ├─ Single worker or local multi-threading
    │   └─ Memory constrained (<256MB)
    │
    └─ [Enterprise Target]
        ├─ x86-64 servers
        ├─ Multi-worker, distributed processing
        └─ Memory abundant (GB scale)
```

**Mid-term (Phase 4+): Selective Optimization**
```
wirelog core (C11)
    └─ Backend Abstraction (optional)
        │
        ├─ [Embedded Path]
        │   ├─ nanoarrow memory (columnar, optional)
        │   ├─ Semi-naive executor (C11)
        │   └─ 500KB-2MB standalone binary
        │
        ├─ [Enterprise Path]
        │   └─ DD retained (no changes)
        │
        └─ [FPGA Path] (future)
            ├─ Abstracted compute kernels
            ├─ Hardware offload
            └─ Arrow IPC data transfer
```

### 1.2 FPGA Acceleration Principles

**Why avoid heavy libraries**:
- LLVM (30M LOC) → Increased FFI cost, complex FPGA integration
- CUDA/OpenCL → Hardware-specific dependencies
- MPI → Distributed processing delegated to DD

**Lightweight design instead**:
- Abstracted compute interface (ComputeBackend)
- Data transfer via Arrow IPC
- Backend implementations are optional (CPU, FPGA, GPU)

### 1.3 Strict Layering

```
[Application Layer]
  wirelog public API (.h)
    │
[Logic Layer]
  Parser → IR → Optimizer
    │
[Execution Interface]
  Backend abstraction (backend.h)
    │
    ├─ [DD Backend]       ├─ [CPU Backend]    ├─ [FPGA Backend]
    │  Rust FFI           │  nanoarrow        │  Arrow IPC
    │  (initial)          │  (mid-term)       │  (future)
    │
[Memory Layer]
  ArrowBuffer / malloc / custom allocator
    │
[I/O Layer]
  CSV, Arrow IPC, network sockets
```

### 1.4 Differential Dataflow Integration

**Phase 0-1: DD-based Implementation**
```
wirelog (C11 parser/optimizer)
    ↓ (IR → DD operator graph conversion)
Differential Dataflow (Rust executor, standalone)
    ↓
Result
```

**Advantages**:
- Proven performance (Differential Dataflow's incremental computation)
- Immediate access to DD's multi-worker, distributed processing
- wirelog implements only parser/optimizer in C11
- Embedded + enterprise start from the same foundation
- Embedded can selectively migrate to nanoarrow later

**Execution Path** (all environments):
```
wirelog (C11 parser/optimizer)
    ↓
IR → Fusion → JPP → SIP → DD operator graph
    ↓
Differential Dataflow (Rust executor)
    ↓
Result

• Embedded: DD single-worker mode, local memory
• Enterprise: DD multi-worker, distributed processing
• Same codebase, only build configuration differs per target
```

**Selective Optimization Path** (Phase 4+):
```
Embedded only (optional):
  wirelog (C11 parser/optimizer)
      ↓
  nanoarrow executor (C11, fully standalone)
      ↓
  Result (500KB-2MB binary)

Enterprise:
  (DD path retained, no changes)

FPGA acceleration (future):
  wirelog (C11 parser/optimizer)
      ↓
  ComputeBackend abstraction
      ↓
  [CPU executor] or [FPGA via Arrow IPC]
```

---

## 2. Architecture Layer Design

### 2.1 Layer Structure (Phase 0-1: All DD-based)

```
┌─────────────────────────────────────────────────────┐
│ Application API (wirelog.h)                         │
│ - wirelog_parse()                                   │
│ - wirelog_optimize()                                │
│ - wirelog_evaluate()                                │
│ - wirelog_get_result()                              │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ Logic Layer (wirelog core) - C11                    │
│ - Parser (hand-written RDP, Datalog → AST)         │
│ - IR Representation (backend-agnostic structs)      │
│ - Optimizer (Fusion, JPP, SIP)                     │
│ - Stratifier (SCC detection, topological sort)     │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ DD Translator (C11 ↔ Rust FFI)                      │
│ - IR → DD operator graph conversion                 │
│ - Result collection from DD runtime                 │
│ - Data marshalling                                  │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ Differential Dataflow (Rust, Timely)                │
│ - Multi-worker execution                            │
│ - Incremental computation                           │
│ - Distributed processing (enterprise)               │
│ - Single-worker mode (embedded)                     │
└──────────────────────────────────────────────────────┘

[I/O Layer]
  CSV, JSON, Arrow IPC (later)
```

### 2.1b Layer Structure (Phase 3+: Selective Embedded Optimization)

```
wirelog core (C11)
    ├─ [Enterprise: DD retained]
    │   └─ Differential Dataflow (no changes)
    │
    └─ [Embedded: Selective migration]
        └─ ComputeBackend abstraction
            ├─ nanoarrow executor (C11)
            └─ (future) FPGA backend via Arrow IPC
```

### 2.2 Layer Responsibilities

#### Logic Layer (wirelog core, C11)

**Responsibilities**:
- Parse Datalog programs to generate AST
- AST → IR conversion (backend-agnostic)
- IR-level optimization passes (Fusion, JPP, SIP)
- Stratification via Tarjan's iterative SCC detection
- DD-independent design

**Status**:
- ✅ Parser (hand-written RDP, FlowLog-compatible grammar, 96 tests)
- ✅ IR representation (9 node types incl. FLATMAP and SEMIJOIN, AST-to-IR conversion, UNION merge, 61 tests)
- ✅ Stratification & SCC detection (Tarjan's iterative, negation validation, 20 tests)
- ✅ Symbol interning (`wl_intern_t`, string → sequential i64 IDs, 9 tests)
- ✅ CSV input support (`.input` directive, delimiter configuration, 17 tests)
- ✅ Logic Fusion: FILTER+PROJECT → FLATMAP (in-place mutation, 14 tests)
- ✅ JPP: Join-Project Plan (greedy join reorder for 3+ atom chains, 13 tests)
- ✅ SIP: Semijoin Information Passing (pre-filter insertion in join chains, 9 tests)
- ✅ CLI driver (`wirelog` executable, .dl file execution, `--workers` flag, 15 tests)

#### DD Translator & FFI Layer (C11 ↔ Rust FFI)

**Responsibilities**:
- IR → DD execution plan translation (all 9 IR node types)
- Plan marshalling to FFI-safe flat structs
- RPN expression serialization (IR expr tree → byte buffer)
- Bulk EDB fact loading via Rust FFI
- Memory ownership: C allocates/frees, Rust borrows via const pointers

**Status**:
- ✅ DD execution plan data structures (`wl_dd_plan_t`, `wl_dd_stratum_plan_t`, `wl_dd_relation_plan_t`, `wl_dd_op_t`)
- ✅ 9 DD operator types: VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE, CONCAT, CONSOLIDATE, SEMIJOIN
- ✅ Stratum-aware plan generation with recursive stratum detection
- ✅ FFI-safe type definitions with RPN expression serialization
- ✅ ANTIJOIN with constants support (right-side filter, key indices)
- ✅ 22 DD plan tests + 31 FFI marshalling tests

#### Rust DD Executor (`wirelog-dd` crate)

**Status**:
- ✅ `staticlib` crate with `#[no_mangle] extern "C"` FFI entry points
- ✅ `repr(C)` type mirrors matching `dd_ffi.h` layout
- ✅ FFI entry points: `wl_dd_worker_create/destroy`, `wl_dd_load_edb`, `wl_dd_execute_cb`
- ✅ RPN expression deserializer + stack evaluator (i64-only)
- ✅ FFI plan reader: unsafe C pointers → safe owned Rust types
- ✅ Non-recursive stratum execution with `timely::execute()`
- ✅ Recursive stratum execution with DD `iterate()` + `distinct()`
- ✅ All 9 operator types: Variable, Map, Filter, Join, Antijoin, Reduce, Concat, Consolidate, Semijoin
- ✅ Meson-Cargo build integration (`-Ddd=true`, clippy/fmt/test targets)
- ✅ 85 Rust tests passing (clippy clean, rustfmt clean)

**Translation Rules** (IR node → DD operator):
```
SCAN      → WL_DD_VARIABLE   (reference to input collection)
PROJECT   → WL_DD_MAP        (column projection)
FILTER    → WL_DD_FILTER     (predicate filter, deep-copied expr)
JOIN      → WL_DD_JOIN       (equijoin with key columns)
ANTIJOIN  → WL_DD_ANTIJOIN   (negation with right relation + optional filter)
SEMIJOIN  → WL_DD_SEMIJOIN   (semijoin pre-filter)
AGGREGATE → WL_DD_REDUCE     (group-by + aggregation function)
UNION     → WL_DD_CONCAT + WL_DD_CONSOLIDATE (union + dedup)
FLATMAP   → WL_DD_FILTER + WL_DD_MAP  (fused filter+project)
```

**Design Decisions**:
- All pointer fields in DD ops are owned (deep copies), freed by `wl_dd_plan_free()`
- Error return via `int` (0 = success, -1 = memory, -2 = invalid input) + out-parameter
- FFI boundary: copy-based marshalling, C owns all memory, Rust borrows via const pointers
- Expression trees serialized to RPN byte buffers (avoids pointer trees across FFI)
- FFI types use fixed-width integers and explicit enum values for ABI stability

#### I/O Layer

**Responsibilities**:
- Read .dl files → parse → optimize → execute through full pipeline
- CSV input via `.input` directive (comma/tab delimiters)
- `.output` directive filtering for selective result output
- Output results as tuples (e.g. `tc(1, 2)`, `tc(2, 3)`)
- Built as `wirelog-cli` (avoids build dir collision), installed as `wirelog`

---

### 2.3 Future Layer Structure (Phase 3+: Selective Embedded Optimization)

**Layers to be added** (planned):

#### ComputeBackend Abstraction (C11)

```c
typedef struct {
    void (*join)(...);
    void (*project)(...);
    void (*filter)(...);
    void (*union_rel)(...);
    void (*dedup)(...);
    // ...
} ComputeBackend;
```

#### nanoarrow Executor (C11, optional)

- Sort-merge join on columnar data
- Semi-naive delta propagation
- Memory optimization
- Note: nanoarrow migration would reopen C-level optimization passes (Subplan Sharing, Boolean Specialization) that are currently wontfix for DD — see [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63)

#### FPGA Backend (future)

- Data transfer via Arrow IPC
- Hardware compute offload

---

## 3. Development Roadmap

### Phase 0: Foundation — Complete ✅

**Goal**: C11 parser/optimizer + DD translator + end-to-end execution

**Key deliverables**:
- Hand-written RDP parser (FlowLog-compatible grammar)
- Tree-based IR (9 node types) with AST-to-IR conversion
- Stratification via Tarjan's iterative SCC detection
- IR → DD operator graph translation (9 op types)
- FFI marshalling layer (RPN expression serialization)
- Rust DD executor crate (Differential Dataflow dogs3 v0.19.1)
- CLI driver with `.dl` file execution and `--workers` flag
- CSV input support for `.input` directive

### Phase 1: Optimization — Complete ✅

**Goal**: IR-level optimization passes + comprehensive benchmark suite

**Optimization Passes**:
- ✅ Logic Fusion (FILTER+PROJECT → FLATMAP, in-place mutation)
- ✅ JPP — Join-Project Plan (greedy join reorder for 3+ atom chains)
- ✅ SIP — Semijoin Information Passing (pre-filter insertion in join chains)
- ❌ Subplan Sharing — closed as wontfix ([#61](https://github.com/justinjoy/wirelog/issues/61)): profiling showed +1.9% slower on DOOP; DD already shares collections via lightweight Variable handles
- ❌ Boolean Specialization — closed as wontfix ([#62](https://github.com/justinjoy/wirelog/issues/62)): DD's `join_map` and `semijoin` use identical `join_core` for unary relations; near-zero cost difference

**Benchmark Suite** (15 workloads):

| Category | Workloads |
|----------|-----------|
| Graph | TC, Reach, CC, SSSP, SG, Bipartite |
| Pointer Analysis | Andersen, CSPA, CSDA, Dyck-2 |
| Advanced | Galen (8 rules), Polonius (37 rules, 1487 iterations), CRDT (23 rules), DDISASM (28 rules), DOOP (136 rules, 8-way joins) |

**Test counts**: 325 C tests (14 suites) + 85 Rust tests = **410 total tests passing**

### Phase 2: Performance Baseline (Planned)

**Goal**: Embedded vs enterprise performance and memory comparison

- Comprehensive benchmarking (all environments)
- Memory profiling (embedded vs enterprise)
- Bottleneck analysis
- Assess nanoarrow migration necessity

### Phase 3: Selective Embedded Optimization (Planned)

**Goal**: Embedded environment only nanoarrow migration (optional)

- Backend abstraction interface design
- nanoarrow executor implementation
- Binary minimization (LTO, -Os, strip)

**Decision Point**: Determine necessity after Phase 2 benchmark results

### Phase 4: FPGA Support (Planned)

**Goal**: Offload heavy computation to FPGA (optional)

- Extend ComputeBackend to FPGA
- Arrow IPC FPGA communication
- Task scheduling & offload

---

## 4. Technology Stack

| Layer | Choice | Status | Rationale |
|-------|--------|--------|-----------|
| **Language** | C11 | ✅ Confirmed | Minimal dependencies, embedded-friendly, compatibility |
| **Build** | Meson + Ninja | ✅ Confirmed | Excellent cross-compile, lightweight |
| **Parser** | Hand-written RDP | ✅ Implemented | Zero deps, FlowLog-compatible grammar |
| **IR** | Tree-based (9 node types) | ✅ Implemented | AST-to-IR, UNION merge, FLATMAP, SEMIJOIN |
| **Stratification** | Tarjan's SCC | ✅ Implemented | O(V+E), iterative |
| **Optimizer** | Fusion + JPP + SIP | ✅ Implemented | 3 passes, in-place IR mutation |
| **DD Plan** | IR → DD op graph | ✅ Implemented | 9 op types, stratum-aware |
| **FFI Marshalling** | DD plan → FFI-safe types | ✅ Implemented | RPN expr serialization |
| **Rust DD Executor** | wirelog-dd crate | ✅ Implemented | DD dogs3 v0.19.1, 85 Rust tests |
| **Build Integration** | Meson + Cargo | ✅ Implemented | `-Ddd=true`, clippy/fmt/test targets |
| **CLI Driver** | wirelog-cli binary | ✅ Implemented | .dl execution, `--workers` flag |
| **Benchmarks** | 15 workloads | ✅ Implemented | Graph, pointer analysis, program analysis |
| **Memory** | nanoarrow (mid-term) | Planned | Columnar, Arrow interop |
| **Allocator** | Region/Arena + system malloc | Planned | See [Discussion #58](https://github.com/justinjoy/wirelog/discussions/58) |
| **I/O** | CSV + Arrow IPC | CSV ✅, Arrow planned | Standard formats |

---

## 5. Open Design Items

### IR and Optimization
- [x] Optimization pass ordering (Fusion → JPP → SIP)
- [x] Join ordering strategy (greedy heuristic, maximize shared variables)
- [ ] Cost model accuracy vs performance trade-off
- [ ] IR representation format exploration (tree vs DAG vs SSA)

### Memory Management
- [ ] Region/Arena allocator design (after allocation patterns stabilize)
- [ ] Allocation category separation: `WL_ALLOC_INTERNAL` (AST/IR) vs `WL_ALLOC_FFI_TRANSFER` (DD boundary)
- [ ] Memory leak detection strategy

### Backend Abstraction
- [ ] RelationBuffer and Arrow schema relationship
- [ ] Backend data conversion costs
- [ ] Error handling approach

### Performance Goals
- [ ] Per-target performance goals (embedded vs enterprise)
- [ ] Memory usage constraints
- [ ] Deployment binary size targets

### FPGA Integration
- [ ] Hardware/Software boundary definition
- [ ] Arrow IPC communication protocol details
- [ ] Task scheduling strategy

### nanoarrow Migration Considerations
- [ ] Revisit Subplan Sharing (#61) and Boolean Specialization (#62) — see [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63)
- [ ] Arrow columnar cost model differs from DD: CSE and set-membership filters become valuable

---

## 6. References

**wirelog Project Documentation**:
- Project URL: https://github.com/justinjoy/wirelog
- FlowLog paper (reference): PVLDB 2025, "FlowLog: Efficient and Extensible Datalog via Incrementality"
- nanoarrow migration analysis: [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63)
- Allocator ADR: [Discussion #58](https://github.com/justinjoy/wirelog/discussions/58)

**External Projects**:
- Differential Dataflow: https://github.com/TimelyDataflow/differential-dataflow
- nanoarrow: https://github.com/apache/arrow-nanoarrow (used later)
- Arrow format: https://arrow.apache.org/docs/format/ (used later)

---

## 7. Document Update History

| Date | Version | Changes |
|------|---------|---------|
| 2026-02-22 | 0.1 | Initial draft, layering definition |
| 2026-02-22 | 0.2 | Phase 0 parser implementation status update |
| 2026-02-23 | 0.3 | Allocator ADR moved to Discussion #58 |
| 2026-02-24 | 0.4 | IR representation complete; Stratification & SCC complete |
| 2026-02-24 | 0.5 | DD Plan Translator complete (19 tests) |
| 2026-02-24 | 0.6 | Phase 1 Logic Fusion complete (14 tests) |
| 2026-02-26 | 0.7 | FFI marshalling layer complete (27 tests) |
| 2026-02-26 | 0.8 | Rust DD executor crate complete |
| 2026-02-27 | 0.9 | Inline fact extraction; CLI driver; end-to-end pipeline complete |
| 2026-02-27 | 0.10 | Actual DD integration (dogs3 v0.19.1) |
| 2026-02-28 | 0.11 | Rust code minimization; CSV input support; benchmark suite started |
| 2026-03-01 | 0.12 | **Phase 1 complete.** JPP, SIP optimization passes; 15 benchmarks (TC through DOOP); ANTIJOIN with constants fix; Subplan Sharing (#61) and Boolean Specialization (#62) closed as wontfix after profiling. 410 total tests (325 C + 85 Rust). Removed directory structure listings. |
