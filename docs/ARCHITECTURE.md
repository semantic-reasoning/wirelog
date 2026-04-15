# Wirelog Architecture - Persistent Design

**Last Updated:** 2026-04-13  
**Verification:** Architect & Critic review complete

---

## Overview

Wirelog is a Datalog engine built on **Timely-Differential concepts** implemented in **Pure C11**, using **Columnar storage** with **K-Fusion parallelism** and a **pluggable Backend abstraction**.

This document describes the **invariant architectural principles** that must be preserved across all phases and implementations. It is intentionally separated from phase-specific implementation details (which may be in separate roadmap documents).

---

## Core Design Philosophy

1. **Embedded-to-Enterprise Duality**: Single codebase targets both resource-constrained embedded systems and high-performance enterprise servers
2. **FPGA-Ready Lightweight Design**: No heavy dependencies (LLVM, CUDA, MPI); Arrow IPC enables future hardware acceleration
3. **Backend Pluggability**: Compute backends are swappable via abstraction; new backends (FPGA, GPU) can be added without modifying core logic
4. **Pure C11 Foundation**: No runtime, no GC, minimal external dependencies

---

## Five Invariant Principles

### 1. Timely-Differential Foundation

**Principle**: Wirelog implements core concepts from Timely-Differential dataflow natively in C11.

**What this means**:
- **Lattice timestamps**: Each iteration/epoch is represented as a partially-ordered lattice `(outer_epoch, iteration, worker)` with semilattice join operation
- **Z-set semantics**: Facts carry signed multiplicities (+1 insert, -1 retract) enabling correct incremental computation
- **Differential arrangements**: Relations are indexed with delta tracking for incremental skip optimization
- **Frontier tracking**: Per-stratum and per-rule progress frontiers enable incremental evaluation (skip unnecessary iterations)
- **Mobius delta formula**: Weighted joins use multiplicity multiplication and Mobius inversion for correct semantics

**Why**: Timely-Differential provides proven correctness guarantees for incremental datalog evaluation. Reimplementing these concepts in C ensures embedded compatibility while retaining the computational model.

**Code locations**:
- Lattice timestamps: `wirelog/columnar/diff_trace.h` (col_diff_trace_t)
- Z-set multiplicity: `wirelog/columnar/columnar_nanoarrow.h:162-174` (col_delta_timestamp_t)
- Differential arrangements: `wirelog/columnar/diff_arrangement.h`
- Frontier tracking: `wirelog/columnar/frontier.h`, `wirelog/columnar/progress.h`
- Mobius formula: `wirelog/columnar/mobius.c`

---

### 2. Pure C11 Language

**Principle**: All implementation must be in standard C11 (ISO/IEC 9899:2011).

**What this means**:
- No Rust, C++, Python, or other languages in the core codebase
- No FFI boundaries (foreign function interfaces)
- C11 standard features allowed: `_Static_assert`, `stdatomic.h`, designated initializers, etc.

**Current State (2026-04-13)**:
- ✅ Core engine: Pure C11
- ✅ Sorting: LSD radix sort (`col_rel_radix_sort_int64()`), fallback to `qsort_r` on allocation failure
- ⚠️ POSIX dependencies: `pthread` (not C11 `<threads.h>`)
- ⚠️ Platform intrinsics: `__builtin_ctzll`, `__builtin_prefetch` (GCC/Clang specific)

**Planned Removal**:
- POSIX pthread → migrate to C11 `<threads.h>` when compiler support matures
- `__builtin_*` → fallback to portable C11 equivalents

**Code locations**:
- Build enforcement: `meson.build:49` (`-std=c11`)
- POSIX abstraction: `wirelog/thread.h`, `wirelog/thread_posix.c`, `wirelog/thread_msvc.c`
- Radix sort: `wirelog/columnar/relation.c:1449-1638` (col_rel_radix_sort, col_rel_radix_sort_int64)
- qsort compatibility (fallback): `wirelog/columnar/internal.h:999-1115`

---

### 3. Columnar Storage Foundation

**Principle**: All relation data is stored in column-major (columnar) format.

**What this means**:
- Relations are represented as `col_rel_t`: array of columns, each column is an `int64_t[]` array
- Layout: `columns[col_index][row_index]` — column-major access pattern
- Arrow schema metadata: Each relation carries an `ArrowSchema` for type information (via nanoarrow)
- No row-major storage in hot paths (row-major structures exist only for auxiliary indices like sorted copies or join caches)

**Why**:
- **SIMD-friendly**: Column-major enables vectorized operations on homogeneous data types
- **Cache-efficient**: Sequential column access improves CPU cache locality
- **Arrow interop**: Direct mapping to Apache Arrow columnar format enables future IPC integration (FPGA, external systems)

**Code locations**:
- Relation struct: `wirelog/columnar/internal.h:183-266` (col_rel_t with columns[col][row])
- ArrowSchema integration: `wirelog/columnar/relation.c:213-227`
- Operator implementations: `wirelog/columnar/ops.c` (VARIABLE/MAP/FILTER/JOIN work on columnar buffers)

---

### 4. K-Fusion Parallelism

**Principle**: Multi-way semi-naive evaluation is parallelized via K-Fusion: when a recursive relation appears K ≥ 2 times in rule bodies, the backend creates K independent parallel evaluation paths.

**What this means**:
- For rule `Path(x,y) :- Edge(x,z), Path(z,y)` (K=1 self-loop): standard semi-naive
- For rule `Result(x,y) :- Path1(x,z), Path2(z,y)` (K=2 multi-way): creates 2 independent operator sequences, dispatches to workqueue
- Each worker has isolated copies of differential arrangements (no cross-worker synchronization)
- Results are merged back after all K workers complete

**Why**:
- Exposes parallelism at the stratum level without global synchronization
- Deep-copy isolation simplifies correctness (no locks needed for K-fusion workers)
- Natural fit for workqueue-based parallel execution

**Code locations**:
- Plan-level: `wirelog/exec_plan.h` (WL_PLAN_OP_K_FUSION operator type)
- Plan generation: `wirelog/exec_plan_gen.c:1574-1746` (expand_multiway_k_fusion)
- Execution: `wirelog/columnar/internal.h:1257` (col_op_k_fusion)
- Worker isolation: `wirelog/columnar/diff_arrangement.h:88-98` (col_diff_arrangement_deep_copy)

---

### 5. Backend Abstraction (Pluggable)

**Principle**: Compute backends are swappable via a clean vtable abstraction. The core engine (parser, optimizer, plan generation) is backend-agnostic.

**What this means**:
- `wl_compute_backend_t` vtable in `wirelog/backend.h:85-107` defines 7 operations: `session_create`, `session_destroy`, `session_insert`, `session_remove`, `session_step`, `session_set_delta_cb`, `session_snapshot`
- Each backend provides an implementation of this interface
- `wl_session_t` contains only a pointer to the backend vtable; all dispatch is polymorphic
- Future backends (FPGA, GPU, distributed) can be added without modifying core engine

**Current State**:
- ✅ Columnar backend implemented (`wl_backend_columnar()`)
- ⚠️ Only one backend exists; abstraction untested with multiple backends
- ⚠️ Backend-specific operators (K_FUSION, LFTJ, EXCHANGE) leak into shared `exec_plan.h` enum

**Planned Improvements**:
- Separate universal operators (0-8) from backend-specific operators (9+)
- Add FPGA backend using Arrow IPC for data transfer

**Code locations**:
- Vtable: `wirelog/backend.h:85-107` (wl_compute_backend_t)
- Dispatch layer: `wirelog/session.c` (pure vtable delegation)
- Embedding contract: `wirelog/columnar/columnar_nanoarrow.h:140-149` (C11 section 6.7.2.1 casting)
- Singleton: `wirelog/session.c:1921-1936` (wl_backend_columnar)

---

## Architecture Layers

```
┌─────────────────────────────────────────┐
│ Application API (wirelog.h)             │
│ - wirelog_program_load()                │
│ - wirelog_program_eval()                │
│ - wirelog_get_facts()                   │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Logic Layer (C11)                       │
│ - Parser (hand-written RDP)             │
│ - IR (Intermediate Representation)      │
│ - Stratification (SCC detection)        │
│ - Symbol interning (string dedup)       │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Optimizer (C11)                         │
│ - Fusion (FILTER+PROJECT → FLATMAP)    │
│ - JPP (join reordering)                │
│ - SIP (semijoin pre-filtering)         │
│ - Magic Sets, Subsumption              │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Plan Generation (C11)                   │
│ - IR → Execution plan translation      │
│ - K-Fusion expansion                   │
│ - Multi-way delta expansion            │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Backend Abstraction Vtable              │
│ - wl_compute_backend_t interface       │
└──────────────┬──────────────────────────┘
               │
      ┌────────┴────────────┐
      │                     │
┌─────▼──────────┐  ┌──────▼──────────┐
│ Columnar       │  │ FPGA Backend    │
│ Backend (C11)  │  │ (Planned)       │
│ + nanoarrow    │  │ Arrow IPC       │
└────────────────┘  └─────────────────┘
```

---

## Design Decisions

### Why Timely-Differential (not just semi-naive)?

Timely-Differential provides:
- Correct incremental evaluation with arbitrary precedence of derivations
- Frontier-based skip optimization (avoid redundant iterations)
- Multiplicity tracking for negation and aggregation correctness
- Natural extension to distributed evaluation

### Why Pure C11 (not Rust)?

- **Embedded compatibility**: C11 runs on bare metal, RTOS, microcontrollers
- **No runtime**: No garbage collector, predictable memory behavior
- **Single toolchain**: GCC, Clang, MSVC all supported without FFI
- **Minimal dependencies**: nanoarrow and xxHash are both C

### Why Columnar (not row-major)?

- **SIMD acceleration**: Vectorized comparison, hash, join operations
- **Cache efficiency**: Sequential column access improves L1/L2 hit rate
- **Arrow ecosystem**: Direct mapping to Apache Arrow format

### Why K-Fusion?

- **Exposes parallelism**: Multi-way joins create multiple independent execution paths
- **Avoids synchronization**: Deep-copy isolation eliminates cross-worker locking
- **Scalable**: Works for any K (fan-in) without algorithmic changes

### Why Backend Abstraction?

- **Extensibility**: FPGA, GPU, distributed backends can be added later
- **Separation of concerns**: Engine doesn't know about compute target
- **Testing**: Mock backends can be added for unit tests

---

## References

- **Verification Report**: Architect & Critic analysis (2026-04-13)
- **Timely-Differential Paper**: McCLeland et al., SIGMOD 2013
- **Apache Arrow Format**: https://arrow.apache.org/docs/format/
- **nanoarrow**: https://github.com/apache/arrow-nanoarrow
