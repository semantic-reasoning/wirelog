# wirelog Architecture Document

**Project**: wirelog - Embedded-to-Enterprise Datalog Engine
**Copyright**: Copyright (C) CleverPlant
**Date**: 2026-02-22
**Status**: Phase 0 Complete, Phase 1 In Progress

⚠️ **This document is a draft.** It will be continuously updated.

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

**Initial Phase (0-3): All DD-based**
```
wirelog core (C11)
├─ Parser (Datalog → IR)
├─ Optimizer (Logic Fusion, JPP, SIP, etc.)
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

### 1.4 Initial Start: Differential Dataflow Integration

**Phase 0-3: DD-based Implementation**
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
Initial (Phase 0-3, Months 1-5):
  wirelog (C11 parser/optimizer)
      ↓
  IR → DD operator graph (conversion)
      ↓
  Differential Dataflow (Rust executor, FlowLog-based)
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

### 2.1 Layer Structure (Phase 0-3: All DD-based)

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
│ - Optimizer (Logic Fusion, JPP, SIP, Subplan)      │
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

### 2.2 Layer Responsibilities (Phase 0-3)

#### Logic Layer (wirelog core, C11)

**File Structure**:
```
wirelog/
  parser/
    lexer.c         # Tokenization
    parser.c        # Datalog → AST (hand-written RDP)
    ast.c           # AST node management
  ir/
    ir.c            # IR node construction, expression clone
    program.c       # Program metadata, AST-to-IR conversion, UNION merge
    stratify.c      # Stratification, dependency graph, Tarjan's SCC
    api.c           # Public API implementation
  ffi/
    dd_plan.h       # DD execution plan types and internal API
    dd_plan.c       # IR → DD operator graph translation
    dd_ffi.h        # FFI-safe type definitions (C ↔ Rust boundary)
    dd_marshal.c    # DD plan marshalling (internal → FFI-safe)
    facts_loader.c  # Bulk EDB fact loading via Rust FFI
  optimizer.c       # Optimizer orchestrator (planned)
  passes/
    fusion.h        # Logic Fusion header (internal API)
    fusion.c        # Logic Fusion (FILTER+PROJECT → FLATMAP)
    jpp.c           # Join-Project Plan (planned)
    sip.c           # Semijoin Information Passing (planned)
    sharing.c       # Subplan Sharing (planned)
  cli/
    driver.h        # CLI driver public interface
    driver.c        # wl_read_file(), wl_print_tuple(), wl_run_pipeline()
    main.c          # CLI entry point (--workers N, --help flags)
```

**Responsibilities**:
- Parse Datalog programs to generate AST
- AST → IR conversion (backend-agnostic)
- IR-level optimization (algorithms)
- DD-independent design

**Phase 0 Implementation Status**:
- ✅ Parser implemented (hand-written RDP, C11)
- ✅ Parser tests: 91/91 passing (47 lexer + 44 parser)
- ✅ Grammar: FlowLog-compatible (declarations, rules, negation, aggregation, arithmetic, comparisons, booleans, .plan marker)
- ✅ IR representation (8 node types, AST-to-IR conversion, UNION merge)
- ✅ IR tests: 60/60 passing (19 IR + 41 program)
- ✅ Stratification & SCC detection (Tarjan's iterative, negation validation)
- ✅ Stratification tests: 20/20 passing
- ✅ DD Plan Translator (IR → DD operator graph, all 8 IR node types)
- ✅ DD Plan tests: 19/19 passing
- 🔄 Optimization passes (Phase 1 in progress)
- ✅ Logic Fusion: FILTER+PROJECT → FLATMAP (in-place mutation, 14/14 tests)
- ✅ Inline fact extraction (row-major int64_t arrays, `wirelog_program_get_facts` API, +4 tests)
- ✅ End-to-end C → Rust execution (11/11 tests, passthrough, TC, join, filter, aggregation, inline facts)
- ✅ CLI driver (`wirelog` executable, .dl file execution, `--workers` flag, 8/8 tests)

#### DD Translator & FFI Layer (C11 ↔ Rust FFI)

**Files**:
```
wirelog/ffi/
  dd_plan.h         # DD execution plan types and internal API
  dd_plan.c         # IR → DD operator graph translation
  dd_ffi.h          # FFI-safe type definitions (C ↔ Rust boundary)
  dd_marshal.c      # DD plan marshalling (internal → FFI-safe)
  facts_loader.c    # Bulk EDB fact loading via Rust FFI (wirelog_load_all_facts)
```

**Phase 0 Status** (DD Plan — C-side complete):
- ✅ DD execution plan data structures (`wl_dd_plan_t`, `wl_dd_stratum_plan_t`, `wl_dd_relation_plan_t`, `wl_dd_op_t`)
- ✅ 8 DD operator types: VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE, CONCAT, CONSOLIDATE
- ✅ IR → DD translation for all 8 IR node types (SCAN, PROJECT, FILTER, JOIN, ANTIJOIN, AGGREGATE, UNION, FLATMAP deferred)
- ✅ Stratum-aware plan generation (EDB collection, per-stratum relation plans)
- ✅ Recursive stratum detection (`is_recursive` flag for DD `iterate()` wrapping)
- ✅ Deep-copy ownership semantics (`wl_ir_expr_clone()` for filter expressions)
- ✅ 19/19 tests passing

**FFI Marshalling Layer** (C-side complete):
- ✅ FFI-safe type definitions (`wl_ffi_plan_t`, `wl_ffi_stratum_plan_t`, `wl_ffi_relation_plan_t`, `wl_ffi_op_t`)
- ✅ RPN expression serialization (`wl_ffi_expr_serialize()` — IR expr tree → flat byte buffer)
- ✅ Plan marshalling (`wl_dd_marshal_plan()` — `wl_dd_plan_t` → `wl_ffi_plan_t`)
- ✅ Memory ownership: C allocates/frees, Rust borrows via const pointers
- ✅ Opaque worker handle (`wl_dd_worker_t`) for future Rust executor integration
- ✅ 27/27 tests passing (expression serialization, operator translation, key fidelity, memory cleanup)

#### Rust DD Executor (`wirelog-dd` crate)

**Files**:
```
rust/wirelog-dd/
  Cargo.toml          # Crate config (staticlib, DD/timely deps)
  src/
    lib.rs            # Module declarations, FFI re-exports
    ffi_types.rs      # repr(C) Rust mirrors of dd_ffi.h types
    ffi.rs            # C FFI entry points (worker lifecycle, EDB loading)
    expr.rs           # RPN expression deserializer + stack evaluator (i64-only)
    plan_reader.rs    # Unsafe FFI plan → safe Rust owned types
    dataflow.rs       # DD-native plan execution
```

**Rust-side Status** (Phase 0 — DD-native executor complete):
- ✅ Crate scaffold: `staticlib` with `#[no_mangle] extern "C"` FFI entry points
- ✅ `repr(C)` type mirrors matching `dd_ffi.h` layout (16 layout tests)
- ✅ FFI entry points: `wl_dd_worker_create/destroy`, `wl_dd_load_edb`, `wl_dd_execute`, `wl_dd_execute_cb`
- ✅ EDB loading: flat i64 array → HashMap<String, Vec<Vec<i64>>> with append semantics
- ✅ RPN expression deserializer: byte buffer → `Vec<ExprOp>` (12 active tag types: Var, ConstInt, 5 arith, 6 cmp; 6 dead tags rejected)
- ✅ Stack-based expression evaluator: `eval_filter()` with `Value::Int` only (comparisons return `Value::Int(0/1)`)
- ✅ FFI plan reader: unsafe C pointers → safe owned Rust types (`SafePlan`, `SafeOp`, etc.)
- ✅ Non-recursive stratum execution: single DD dataflow scope with `timely::execute()`
- ✅ Recursive stratum execution: DD `iterate()` with `distinct()` for set semantics and fixed-point convergence
- ✅ `consolidate()` before `inspect()` for antijoin correctness
- ✅ Data model: all tuples are `Vec<i64>`
- ✅ All 8 operator types: Variable, Map, Filter, Join, Antijoin, Reduce, Concat, Consolidate
- ✅ Meson-Cargo build integration (`-Ddd=true`, `ninja rust-clippy`, `ninja rust-fmt-check`, `ninja rust-test`)
- ✅ 78/78 Rust tests passing (clippy clean, rustfmt clean)

**Translation Rules** (IR node → DD operator):
```
SCAN      → WL_DD_VARIABLE   (reference to input collection)
PROJECT   → WL_DD_MAP        (column projection)
FILTER    → WL_DD_FILTER     (predicate filter, deep-copied expr)
JOIN      → WL_DD_JOIN       (equijoin with key columns)
ANTIJOIN  → WL_DD_ANTIJOIN   (negation with right relation)
AGGREGATE → WL_DD_REDUCE     (group-by + aggregation function)
UNION     → WL_DD_CONCAT + WL_DD_CONSOLIDATE (union + dedup)
FLATMAP   → WL_DD_FILTER + WL_DD_MAP  (fused filter+project)
```

**Responsibilities**:
- ✅ Convert wirelog IR to DD operator graph (C-side plan)
- ✅ C → Rust data marshalling (FFI-safe flat structs, RPN expression serialization)
- ✅ FFI boundary defined (memory ownership: C allocates, Rust borrows)
- ✅ DD worker management (Rust-side: worker create/destroy, EDB loading)
- ✅ Plan execution (Phase 0: DD-native execution, non-recursive + recursive)
- ✅ Result callback integration (`wl_dd_execute_cb` fully wired, not a stub)

**Design Decisions**:
- ✅ All pointer fields in DD ops are owned (deep copies), freed by `wl_dd_plan_free()`
- ✅ Error return via `int` (0 = success, -1 = memory, -2 = invalid input) + out-parameter
- ✅ FLATMAP deferred: current IR generates separate FILTER/PROJECT/JOIN nodes
- ✅ FFI boundary: copy-based marshalling, C owns all memory, Rust borrows via const pointers
- ✅ Expression trees serialized to RPN byte buffers (avoids pointer trees across FFI)
- ✅ FFI types use fixed-width integers and explicit enum values for ABI stability
- ✅ Context passing mechanism (worker handle → execution context via `WlDdWorker`)
- ✅ `wl_dd_execute_cb` wired to dataflow executor (FFI → plan_reader → DD dataflow, result callback fully connected)

#### I/O Layer (Phase 0: CLI Driver Implemented)

**Files**:
```
wirelog/cli/
  driver.h        # CLI driver public interface
  driver.c        # wl_read_file(), wl_print_tuple(), wl_run_pipeline()
  main.c          # CLI entry point (--workers N, --help flags)
```

**Responsibilities**:
- Read .dl files → parse → compile → execute through full pipeline
- Output results as tuples (e.g. `tc(1, 2)`, `tc(2, 3)`)
- Built as `wirelog-cli` (avoids build dir collision), installed as `wirelog`
- (CSV input, Arrow IPC output added later)

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

#### FPGA Backend (future)

- Data transfer via Arrow IPC
- Hardware compute offload

---

## 3. Development Roadmap

### Phase 0: Foundation (Weeks 1-4) - All environments DD-based

**Goal**: Initial version with C11 parser/optimizer + DD translator

**Implementation Items**:
- ✅ C11 parser (Datalog → AST, hand-written RDP)
- ✅ Parser tests (91/91 passing)
- ✅ FlowLog-compatible grammar implementation
- ✅ Build system (Meson, C11)
- ✅ IR representation (8 node types, AST-to-IR, UNION merge)
- ✅ IR tests (60/60 passing: 19 IR node + 41 program)
- ✅ Stratification & SCC detection (iterative Tarjan's, O(V+E))
- ✅ Stratification tests (20/20 passing)
- ✅ IR → DD operator graph translator (all 8 IR node types, 19/19 tests)
- ✅ Rust FFI marshalling layer (FFI-safe types, RPN serialization, plan marshalling, 27/27 tests)
- ✅ Rust DD executor crate (`wirelog-dd`, DD-native executor, 78/78 Rust tests)
- ✅ Actual DD integration (Differential Dataflow dogs3 v0.19.1, DD iterate() for recursive strata)
- ✅ CSV input support for `.input` directive (Issue #18)
- ✅ Meson-Cargo build integration (`-Ddd=true`, lint targets)
- ✅ End-to-end integration tests (C → FFI → Rust → results, 11/11 tests)
- ✅ Inline fact extraction (`wl_relation_info_t` fact storage, `wirelog_program_get_facts` API, 41/41 program tests)
- ✅ Bulk fact loading (`wirelog_load_all_facts`, facts_loader.c, Rust FFI-dependent)
- ✅ CLI driver (`wirelog` executable, .dl file execution, `--workers` flag, 8/8 tests)

**Validation**:
- [ ] Embedded target (ARM cross-compile) build success
- [ ] Enterprise target (x86-64) build success
- ✅ Basic Datalog program execution verification (TC, join, filter, aggregation via CLI)

**Current Status**: Parser (91/91), IR (60/60), Stratification (20/20), DD Plan (19/19), Logic Fusion (14/14), FFI Marshalling (27/27), DD Execute (11/11), CLI (8/8), Rust DD Executor (78/78) complete — 328 tests passing (250 C + 78 Rust). Actual DD integration complete (dogs3 v0.19.1). CSV input support complete (Issue #18). Phase 1 Optimization in progress.

### Phase 1: Optimization (Weeks 5-10) - All environments common

**Goal**: Implement paper-based optimization techniques at IR level (FlowLog/Soufflé reference)

**Implementation Items** (planned):
- ✅ Logic Fusion (FILTER+PROJECT → FLATMAP, in-place mutation, 14/14 tests)
- [ ] Join-Project Plan (structural cost model, JST enumeration)
- [ ] SIP (Semijoin Information Passing)
- [ ] Subplan Sharing (hash-based CTE detection)
- [ ] Boolean Specialization (diff encoding)

**Validation**:
- [ ] Optimization comparison (on vs off)
- [ ] Create benchmarks: Reach, CC, SSSP, TC, etc.
- [ ] Performance measurement

**Estimate**: 2500-3000 LOC, 6 weeks

### Phase 2: Performance Baseline (Weeks 11-14)

**Goal**: Embedded vs enterprise performance and memory comparison

**Implementation Items** (planned):
- [ ] Comprehensive benchmarking (all environments)
- [ ] Memory profiling (embedded vs enterprise)
- [ ] Bottleneck analysis
- [ ] Assess nanoarrow migration necessity
- [ ] Documentation

**Estimate**: 4 weeks

### Phase 3: Selective Embedded Optimization (Month 4+)

**Goal**: Embedded environment only nanoarrow migration (optional)

**Implementation Items** (planned):
- [ ] Backend abstraction interface design
- [ ] nanoarrow executor implementation
- [ ] ComputeBackend interface adaptation
- [ ] Refactoring & testing
- [ ] Binary minimization (LTO, -Os, strip)

**Estimate**: 1500-2000 LOC + refactoring, 4-6 weeks

**Decision Point**: Determine necessity after Phase 2 benchmark results

### Phase 4: FPGA Support (Month 6+)

**Goal**: Offload heavy computation to FPGA (optional)

**Implementation Items** (planned):
- [ ] Extend ComputeBackend to FPGA
- [ ] Arrow IPC FPGA communication
- [ ] Task scheduling & offload
- [ ] Result collection

**Estimate**: TBD (depends on FPGA hardware availability)

---

## 4. Technology Stack

| Layer | Choice | Status | Rationale |
|-------|--------|--------|-----------|
| **Language** | C11 | ✅ Confirmed | Minimal dependencies, embedded-friendly, compatibility |
| **Build** | Meson | ✅ Confirmed | Excellent cross-compile, lightweight |
| **Parser** | Hand-written RDP | ✅ Implemented | Zero deps, 91/91 tests passing |
| **IR** | Tree-based (8 node types) | ✅ Implemented | AST-to-IR, UNION merge, 56/56 tests |
| **Stratification** | Tarjan's SCC | ✅ Implemented | O(V+E), iterative, 20/20 tests |
| **DD Plan** | IR → DD op graph | ✅ Implemented | 8 op types, stratum-aware, 19/19 tests |
| **FFI Marshalling** | DD plan → FFI-safe types | ✅ Implemented | RPN expr serialization, 27/27 tests |
| **Rust DD Executor** | wirelog-dd crate | ✅ Implemented | DD-native executor, 78/78 Rust tests |
| **Build Integration** | Meson + Cargo | ✅ Implemented | `-Ddd=true`, clippy/fmt/test targets |
| **CLI Driver** | wirelog-cli binary | ✅ Implemented | .dl file execution, `--workers` flag, 8/8 tests |
| **Memory** | nanoarrow (mid-term) | Planned | Columnar, Arrow interop |
| **Allocator** | Region/Arena + system malloc | Planned (Phase 2) | jemalloc evaluated and deferred; see §4.1 ADR |
| **Threading** | Optional pthreads | Planned | Single-threaded default |
| **I/O** | CSV + Arrow IPC | Planned | Standard formats |

---

## 5. Open Design Items (TODO)

### Parser & Preprocessing
- [x] Datalog extension feature scope (negation, aggregation, constraints, etc.) - FlowLog grammar implemented
- [ ] Error message strategy
- [ ] Incremental parsing necessity

### IR and Optimization
- [ ] IR representation format (tree vs DAG vs SSA)
- [ ] Optimization pass ordering
- [ ] Cost model accuracy vs performance trade-off
- [ ] Join ordering search space size limit

### Memory Management
- [ ] Region/Arena allocator design (Phase 1 late ~ Phase 2, after allocation patterns stabilize)
- [ ] Allocation category separation: `WL_ALLOC_INTERNAL` (AST/IR) vs `WL_ALLOC_FFI_TRANSFER` (DD boundary)
- [ ] Dynamic allocation vs fixed allocation
- [ ] Memory leak detection strategy
- [ ] jemalloc re-evaluation condition: Phase 2 benchmark shows system malloc as bottleneck in enterprise path

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

---

### 4.1 Allocator Decision Record (ADR): jemalloc Evaluation

**Date**: 2026-02-23
**Status**: Decided — jemalloc not adopted for Phase 0-1
**Participants**: Planner, Architect, Critic (consensus planning)

**Context**:
wirelog targets both embedded (ARM/RISC-V, <256MB) and enterprise (x86-64, GB-scale) environments.
Current C11 codebase has ~35 allocation calls (malloc/calloc/realloc) across 5 files (parser, AST, IR, program).
Memory-intensive execution is delegated to Differential Dataflow (Rust) via FFI.

**Decision**: Do not adopt jemalloc in Phase 0-1. Design Region/Arena allocator after Phase 2 benchmarks.

**Rationale**:

1. **C11 side handles only query-scale allocations**: wirelog C11 manages parser/optimizer memory only.
   Data-scale (GB) memory is managed by DD's Rust allocator. jemalloc provides no practical benefit
   for the C11 layer.

2. **Embedded target conflict**: jemalloc's ~2MB metadata overhead directly conflicts with the
   500KB-2MB standalone binary target for embedded deployments.

3. **Arena/Region is a better fit**: AST and IR follow a clear "create → use → bulk-free" lifecycle
   (3 distinct phases: parsing, IR conversion, program metadata). This pattern is ideal for
   Region-based allocation, not general-purpose allocator replacement.

4. **Premature optimization**: 35 allocation calls in Phase 0 are not a bottleneck. Optimizer passes
   (Phase 1) will introduce new allocation patterns that must stabilize before designing the allocator.

**Alternatives Considered**:

| Alternative | Verdict | Reason |
|-------------|---------|--------|
| jemalloc | Deferred | ~2MB overhead, no benefit for query-scale allocations |
| mimalloc | Deferred | Smaller than jemalloc but same fundamental mismatch |
| Self-built Arena | **Preferred** (Phase 2) | Matches AST/IR lifecycle; simplifies error-path cleanup |
| Region-based allocator | **Preferred** (Phase 2) | Hierarchical regions map to parsing/IR/program phases |
| System malloc (current) | **Retain** (Phase 0-1) | Sufficient for current scale; no bottleneck evidence |
| `wl_allocator_t` interface | Phase 1 late | Define after optimizer allocation patterns stabilize |
| Meson build-time selection | Phase 2+ | `option('allocator', ...)` following existing `embedded`/`threads` pattern |

**Re-evaluation Trigger**: If Phase 2 benchmarks show system malloc as a measurable bottleneck
in the enterprise path, reconsider jemalloc or mimalloc for that target only.

**Open Items from This Review**:
- DD FFI memory ownership (copy vs transfer vs shared buffer) affects allocator category design
- `strdup_safe` exists as 3 independent static copies — consolidate into shared internal utility
- `WIRELOG_EMBEDDED` build macro is defined but not yet used in C source `#ifdef` guards

---

## 6. References

**wirelog Project Documentation**:
- Project URL: https://github.com/justinjoy/wirelog
- FlowLog paper (reference): `discussion/papers/2511.00865v4.pdf`
- Previous analysis: `discussion/FlowLog_C_Implementation_Analysis.md`
- Build system analysis: `discussion/build_system_analysis.md`

**External Projects**:
- Differential Dataflow: https://github.com/TimelyDataflow/differential-dataflow
- nanoarrow: https://github.com/apache/arrow-nanoarrow (used later)
- Arrow format: https://arrow.apache.org/docs/format/ (used later)

---

## 7. Document Update History

| Date | Version | Changes |
|------|---------|---------|
| 2026-02-22 | 0.1 | Initial draft, layering definition |
| 2026-02-22 | 0.2 | Phase 0 parser implementation status update (91/91 tests passing) |
| 2026-02-23 | 0.3 | Add Allocator Decision Record (§4.1): jemalloc evaluated and deferred |
| 2026-02-24 | 0.4 | IR representation complete (56 tests); Stratification & SCC complete (20 tests); 167 total |
| 2026-02-24 | 0.5 | DD Plan Translator complete (19 tests); all 8 IR→DD translations; 186 total |
| 2026-02-24 | 0.6 | Phase 1 Logic Fusion complete (14 tests); in-place FILTER+PROJECT→FLATMAP; 200 total |
| 2026-02-26 | 0.7 | FFI marshalling layer complete (27 tests); dd_plan moved to ffi/; 227 total |
| 2026-02-26 | 0.8 | Rust DD executor crate complete (90 tests); Meson-Cargo integration; 317 total (227 C + 90 Rust) |
| 2026-02-27 | 0.9 | Inline fact extraction (Issue #14, +4 program tests); CLI driver (Issue #11, 8 tests); end-to-end pipeline complete (11 DD execute tests); 340 total (250 C + 90 Rust) |
| 2026-02-27 | 0.10 | Actual DD integration (PR #24): interpreter replaced with Differential Dataflow dogs3 v0.19.1; DD iterate() for recursive strata; consolidate() for antijoin correctness |
| 2026-02-28 | 0.11 | Rust code minimization (PR #25): removed ~460 lines of dead code; stripped expr.rs to i64-only; removed unused deps (serde, columnar); 328 total tests (250 C + 78 Rust) |

---

**Next Steps**:
1. [x] Parser implementation complete (91/91 tests)
2. [x] IR representation and implementation (60/60 tests)
3. [x] Stratification & SCC detection (20/20 tests)
4. [x] DD Plan Translator (IR → DD operator graph, 19/19 tests)
5. [x] Logic Fusion optimization pass (FILTER+PROJECT → FLATMAP, 14/14 tests)
6. [ ] Remaining Phase 1 optimization passes (JPP, SIP, Subplan Sharing)
7. [x] FFI marshalling layer (C-side, FFI-safe types, RPN serialization, 27/27 tests)
8. [x] Rust DD executor crate (FFI stubs, type mirrors, expr evaluator, plan reader, DD-native dataflow, 78/78 Rust tests)
9. [x] Meson-Cargo build integration (`-Ddd=true`, `ninja rust-clippy/rust-fmt-check/rust-test`)
10. [x] Wire `wl_dd_execute_cb` to dataflow executor (fully connected, result callback working)
11. [x] End-to-end integration tests (11/11 passing: passthrough, TC, join, filter, aggregation, inline facts)
12. [x] Inline fact extraction (`wirelog_program_get_facts` API, `wirelog_load_all_facts` bulk loader, Issue #14)
13. [x] CLI driver (`wirelog` executable, .dl file execution, `--workers` flag, 8/8 tests, Issue #11)
14. [ ] Remaining Phase 1 optimization passes (JPP, SIP, Subplan Sharing)
15. [ ] Embedded target (ARM cross-compile) build validation
16. [x] Actual DD integration (Differential Dataflow dogs3 v0.19.1, PR #24)
17. [x] Rust code minimization (i64-only expr, removed dead code, PR #25)
18. [x] CSV input support for .input directive (Issue #18)
