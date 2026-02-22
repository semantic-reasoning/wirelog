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
5. **C99 Foundation**: C99 instead of C11 for broader compatibility

---

## 1. Core Design Principles

### 1.1 Multi-Target Architecture (Embedded ↔ Enterprise)

**Initial Phase (0-3): All DD-based**
```
wirelog core (C99)
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
wirelog core (C99)
    └─ Backend Abstraction (optional)
        │
        ├─ [Embedded Path]
        │   ├─ nanoarrow memory (columnar, optional)
        │   ├─ Semi-naive executor (C99)
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
wirelog (C99 parser/optimizer)
    ↓ (IR → DD operator graph conversion)
Differential Dataflow (Rust executor, standalone)
    ↓
Result
```

**Advantages**:
- Proven performance (Differential Dataflow's incremental computation)
- Immediate access to DD's multi-worker, distributed processing
- wirelog implements only parser/optimizer in C99
- Embedded + enterprise start from the same foundation
- Embedded can selectively migrate to nanoarrow later

**Execution Path** (all environments):
```
Initial (Phase 0-3, Months 1-5):
  wirelog (C99 parser/optimizer)
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
  wirelog (C99 parser/optimizer)
      ↓
  nanoarrow executor (C99, fully standalone)
      ↓
  Result (500KB-2MB binary)

Enterprise:
  (DD path retained, no changes)

FPGA acceleration (future):
  wirelog (C99 parser/optimizer)
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
│ Logic Layer (wirelog core) - C99                    │
│ - Parser (hand-written RDP, Datalog → AST)         │
│ - IR Representation (backend-agnostic structs)      │
│ - Optimizer (Logic Fusion, JPP, SIP, Subplan)      │
│ - Stratifier (SCC detection, topological sort)     │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ DD Translator (C99 ↔ Rust FFI)                      │
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
wirelog core (C99)
    ├─ [Enterprise: DD retained]
    │   └─ Differential Dataflow (no changes)
    │
    └─ [Embedded: Selective migration]
        └─ ComputeBackend abstraction
            ├─ nanoarrow executor (C99)
            └─ (future) FPGA backend via Arrow IPC
```

### 2.2 Layer Responsibilities (Phase 0-3)

#### Logic Layer (wirelog core, C99)

**File Structure**:
```
wirelog/
  lexer.c         # Tokenization
  parser.c        # Datalog → AST (hand-written RDP)
  ast.c           # AST node management
  ir.c            # IR node management
  stratify.c      # Stratification, SCC detection
  optimizer.c     # Optimizer orchestrator
  passes/
    fusion.c      # Logic Fusion
    jpp.c         # Join-Project Plan
    sip.c         # Semijoin Information Passing
    sharing.c     # Subplan Sharing
```

**Responsibilities**:
- Parse Datalog programs to generate AST
- AST → IR conversion (backend-agnostic)
- IR-level optimization (algorithms)
- DD-independent design

**Phase 0 Implementation Status**:
- ✅ Parser implemented (hand-written RDP, C99)
- ✅ Parser tests: 91/91 passing (47 lexer + 44 parser)
- ✅ Grammar: FlowLog-compatible (declarations, rules, negation, aggregation, arithmetic, comparisons, booleans, .plan marker)
- 🔄 IR representation definition (in progress)
- 🔄 Stratification & SCC detection (planned)
- 🔄 Optimization passes (planned)

#### DD Translator (C99 ↔ Rust FFI)

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

#### ComputeBackend Abstraction (C99)

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

#### nanoarrow Executor (C99, optional)

- Sort-merge join on columnar data
- Semi-naive delta propagation
- Memory optimization

#### FPGA Backend (future)

- Data transfer via Arrow IPC
- Hardware compute offload

---

## 3. Development Roadmap

### Phase 0: Foundation (Weeks 1-4) - All environments DD-based

**Goal**: Initial version with C99 parser/optimizer + DD translator

**Implementation Items**:
- ✅ C99 parser (Datalog → AST, hand-written RDP)
- ✅ Parser tests (91/91 passing)
- ✅ FlowLog-compatible grammar implementation
- ✅ Build system (Meson, C99)
- 🔄 IR representation definition (backend-agnostic)
- 🔄 Stratification & SCC detection
- 🔄 IR → DD operator graph translator
- 🔄 Basic integration tests

**Validation**:
- [ ] Embedded target (ARM cross-compile) build success
- [ ] Enterprise target (x86-64) build success
- [ ] Basic Datalog program execution verification

**Current Status**: Parser complete (91/91 tests passing), IR/DD translator in progress

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
| **Language** | C99 | ✅ Confirmed | Minimal dependencies, embedded-friendly, compatibility |
| **Build** | Meson | ✅ Confirmed | Excellent cross-compile, lightweight |
| **Parser** | Hand-written RDP | ✅ Implemented | Zero deps, 91/91 tests passing |
| **Memory** | nanoarrow (mid-term) | Planned | Columnar, Arrow interop |
| **Allocator** | Arena + malloc | Planned | Detailed design needed |
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
- [ ] Arena allocator detailed design
- [ ] Dynamic allocation vs fixed allocation
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

---

**Next Steps**:
1. [x] Parser implementation complete (91/91 tests)
2. [ ] IR representation definition and implementation
3. [ ] DD Translator FFI design
4. [ ] Stratification implementation
5. [ ] Integration test creation
