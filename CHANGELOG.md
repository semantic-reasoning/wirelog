# Changelog

All notable changes to wirelog are documented in this file.

## [Unreleased]

### Added

- **I/O Adapter Framework** (#446): User-defined I/O adapters via runtime registry (`wl_io_register_adapter`). Public header `wirelog/io/io_adapter.h` with opaque context, ABI versioning (`WL_IO_ABI_VERSION=1`), and thread-safe registration API
- **Built-in CSV Adapter** (#455): CSV loading refactored into the adapter framework; backward-compatible `.input(filename=...)` dispatch
- **wl_easy Facade** (#445): Simplified high-level API (`wl_easy.h`) for common session workflows
- **String Operations** (#444): String-typed column functions (`strlen`, `cat`, `substr`, `contains`, `to_upper`, `to_lower`, `trim`, `str_replace`, `to_string`, `to_number`)
- **Path A Example** (#462): Standalone pcap adapter skeleton with CI compile-check against installed headers
- **Binary Size Gate** (#460): CI regression gate for `.text` section growth (5KB budget)
- **I/O Adapters User Guide** (#463): `docs/io-adapters.md` with Path A/B workflows, ABI policy, ownership rules, and thread-safety notes
- **Retraction Support** (#443): Fact retraction with recursive re-evaluation
- **Delta Query Examples**: Examples 08-12 demonstrating retraction, recursive update, time evolution, and snapshot-vs-delta patterns

## [0.21.0] - 2026-03-19

### Added

- **ARM NEON SIMD Optimization** (#231): Full SIMD vectorization for hash and key-match operations on ARM64 architectures with correctness tests (#234)
- **Memory Backpressure System** (#224): Thread-safe memory ledger tracking with JOIN budget enforcement and graceful backpressure mechanisms
- **Intra-join Backpressure** (#5): Soft EOVERFLOW truncation with memory-aware output limiting to prevent cardinality explosion

### Changed

- **Performance**: AVX2 SIMD hash/key-match now paired with ARM NEON equivalents for complete x86-64/ARM64 coverage (#231)
- **Stride-based Evaluation** (#237): Implemented in wirelog engine for improved iteration efficiency
- **Consolidation Fast-path** (#239): Optimized append for pre-sorted delta relations
- **Join Dispatch**: Inline scalar hash for kc<2 to eliminate function call overhead

### Fixed

- Handle missing right relation in JOIN by returning empty result
- Guard direct stdatomic.h includes for MSVC compatibility
- Propagate ENOMEM from col_rel_append_row at consolidation and delta-seeding sites
- Fixed col_rel_compact() right-sizing after deduplication

### Performance

- K-fusion parallel threshold to avoid small-K overhead
- Optimized row comparison via SIMD dispatcher (kway_merge)
- Per-worker arena isolation and delta_pool right-sizing for K-copy reduction

## [0.20.0] - 2026-02-28

### Added

- **CRC-32 Checksumming** (#145): Hardware-accelerated CRC-32 with Ethernet and Castagnoli variants via TDD
- **Hash Function** (#144): Built-in `hash()` function using xxHash3 with high-throughput performance
- **Bitwise Operators** (#72): Complete bitwise AND, OR, XOR, NOT support in parser and evaluator
- **Symbol Type** (#137): String column type support via symbol interning
- **CSV Output Directives** (#137): `.output(filename="...")` directive support for query result export
- **wirelog-cli** (#136): Restored CLI driver executable with enhanced CSV loading and directives integration

### Fixed

- Variable name resolution in expression serializer
- MSVC compilation compatibility (getcwd, atomics, C11 support)
- Cross-platform CRLF line ending normalization in CLI tests
- CSV loading for symbol/string columns
- LTO linker compatibility for CLI executable

## [0.11.0] - 2026-02-28 — Phase 1 Entry

### Added

- **ARM NEON SIMD Optimization** (#231): Full SIMD vectorization for hash and key-match operations on ARM64 architectures with correctness tests (#234)
- **Memory Backpressure System** (#224): Thread-safe memory ledger tracking with JOIN budget enforcement and graceful backpressure mechanisms
- **Intra-join Backpressure** (#5): Soft EOVERFLOW truncation with memory-aware output limiting to prevent cardinality explosion

### Changed

- **Performance**: AVX2 SIMD hash/key-match now paired with ARM NEON equivalents for complete x86-64/ARM64 coverage (#231)
- **Stride-based Evaluation** (#237): Implemented in wirelog engine for improved iteration efficiency
- **Consolidation Fast-path** (#239): Optimized append for pre-sorted delta relations
- **Join Dispatch**: Inline scalar hash for kc<2 to eliminate function call overhead

### Fixed

- Handle missing right relation in JOIN by returning empty result
- Guard direct stdatomic.h includes for MSVC compatibility
- Propagate ENOMEM from col_rel_append_row at consolidation and delta-seeding sites
- Fixed col_rel_compact() right-sizing after deduplication

### Performance

- K-fusion parallel threshold to avoid small-K overhead
- Optimized row comparison via SIMD dispatcher (kway_merge)
- Per-worker arena isolation and delta_pool right-sizing for K-copy reduction

## [0.11.0] - 2026-02-28 — Phase 1 Entry

Phase 1 begins. wirelog now supports string-typed columns via symbol interning,
external CSV data loading, and head arithmetic — the foundational capabilities
needed for real-world Datalog applications (DOOP, Polonius, network policy).

## [0.10.1] - 2026-02-28

### Added

- **Symbol interning** (`wl_intern_t`): bidirectional string-to-integer mapping
  for string-typed columns. The DD executor continues to operate on `Vec<i64>`;
  strings are interned at parse/load time and reverse-mapped on output. (#42)
- **`.input` CSV loading** (`wirelog_load_input_files()`): relations with
  `.input(filename="...", delimiter="...")` directives now load CSV data
  automatically during pipeline execution. (#18)
- **Head arithmetic**: `project_exprs` / `map_exprs` in the DD plan enable
  arithmetic expressions (e.g., `cost(x, c+1)`) in rule heads. (#20)
- **CC and SSSP benchmark workloads**: Connected Components and Single-Source
  Shortest Path programs added to the benchmark suite. (#27)
- **Benchmark framework**: timing utilities, graph data generator, FlowLog
  benchmark driver, and `meson -Dbench=true` build option.
- **CodeQL CI**: GitHub Advanced Security workflow with security-and-quality
  query suite.

### Fixed

- Recursive aggregation (`min`/`max`) not propagating across DD iterations. (#21)
- FFI null-guard checks flagged by CodeQL.
- Version macros in `wirelog.h` now match the project version
  (`WIRELOG_VERSION_MINOR` corrected from 1 to 10).

### Changed

- Rust executor minimized to DD-essential surface only; non-critical Rust code
  removed.
- Architecture docs updated for DD integration and version numbering.

## [0.10.0] - 2026-01-15

Initial Phase 0 release: parser, IR, optimizer, Differential Dataflow executor
via Rust FFI, CLI driver with inline-fact evaluation pipeline.
