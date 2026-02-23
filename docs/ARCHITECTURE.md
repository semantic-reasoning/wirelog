# wirelog Architecture Document

**Project**: wirelog - Embedded-to-Enterprise Datalog Engine
**Copyright**: Copyright (C) CleverPlant
**Date**: 2026-02-22
**Status**: 🔄 Design in Progress (Phase 0 Implementation)

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
    ir.c            # IR node construction and management
    program.c       # Program metadata, AST-to-IR conversion, UNION merge
    stratify.c      # Stratification, dependency graph, Tarjan's SCC
    api.c           # Public API implementation
  optimizer.c       # Optimizer orchestrator (planned)
  passes/
    fusion.c        # Logic Fusion (planned)
    jpp.c           # Join-Project Plan (planned)
    sip.c           # Semijoin Information Passing (planned)
    sharing.c       # Subplan Sharing (planned)
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
- ✅ IR tests: 56/56 passing (19 IR + 37 program)
- ✅ Stratification & SCC detection (Tarjan's iterative, negation validation)
- ✅ Stratification tests: 20/20 passing
- 🔄 Optimization passes (planned)

#### DD Translator (C11 ↔ Rust FFI)

**Files** (planned):
```
src/
  dd/
    translator.c    # IR → DD operator graph
    ffi.h           # FFI definitions
    data_marshal.c  # Data conversion C ↔ Rust
```

**Responsibilities**:
- Convert wirelog IR to DD operator graph
- C ↔ Rust data marshalling
- DD worker management (single vs multi)
- Result collection and conversion

**Design Decisions** (TODO):
- [ ] Clarify FFI boundary (memory ownership)
- [ ] Data marshalling strategy (zero-copy vs copy)
- [ ] Error handling approach
- [ ] Context passing mechanism

#### I/O Layer (Phase 0: Basic)

**Files** (planned):
```
src/
  io/
    csv.c           # CSV input → DD collection
    output.c        # DD results → output (stdout, file)
```

**Responsibilities**:
- Read CSV files → Datalog facts
- Output results after program execution
- (Arrow IPC added later)

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
- ✅ IR tests (56/56 passing: 19 IR node + 37 program)
- ✅ Stratification & SCC detection (iterative Tarjan's, O(V+E))
- ✅ Stratification tests (20/20 passing)
- 🔄 IR → DD operator graph translator
- 🔄 Basic integration tests

**Validation**:
- [ ] Embedded target (ARM cross-compile) build success
- [ ] Enterprise target (x86-64) build success
- [ ] Basic Datalog program execution verification

**Current Status**: Parser (91/91), IR (56/56), Stratification (20/20) complete — 167 tests passing. DD translator next.

### Phase 1: Optimization (Weeks 5-10) - All environments common

**Goal**: Implement paper-based optimization techniques at IR level (FlowLog/Soufflé reference)

**Implementation Items** (planned):
- [ ] Logic Fusion (Join+Map+Filter → FlatMap)
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

---

**Next Steps**:
1. [x] Parser implementation complete (91/91 tests)
2. [x] IR representation and implementation (56/56 tests)
3. [x] Stratification & SCC detection (20/20 tests)
4. [ ] DD Translator FFI design
5. [ ] Integration test creation
