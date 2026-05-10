# Changelog

All notable changes to wirelog are documented in this file.

## [Unreleased]

### Changed

- **String-fn enum constants renamed for v1.0 prefix conformance**
  (#757): the 12 `WL_STR_FN_*` enum constants in
  `wirelog/wirelog-types.h` (declaring the string-op kinds shipped
  in #444) are renamed to `WIRELOG_STR_FN_*`.  Source-incompatible
  for downstream consumers passing the constants to public string-
  op APIs; migrate via a textual rename.  Enum values and behaviour
  are unchanged.  Part of public-API prefix audit epic #755.
- **Public-API typedef renamed for v1.0 prefix conformance** (#760):
  the `wirelog/wirelog.h` umbrella header no longer exposes the
  internal-style `wl_intern_t` typedef name.  The new public name is
  `wirelog_intern_t`; `wirelog_program_get_intern()` returns
  `const wirelog_intern_t *`.  The internal header
  `wirelog/intern.h` keeps `typedef struct wl_intern wl_intern_t;`
  for in-tree callers (both names alias the same struct).  Two
  docstrings that previously named the internal `wl_dd_load_edb()`
  helper are rephrased to refer to it as the project's internal
  EDB-load helper.  Source-incompatible for downstream consumers
  using `wl_intern_t` after including only `wirelog/wirelog.h`;
  migrate via a textual rename to `wirelog_intern_t`.  Part of
  public-API prefix audit epic #755.
- **Callback typedefs renamed for v1.0 prefix conformance** (#758): the
  public-API callback typedefs `wl_on_delta_fn` and `wl_on_tuple_fn`
  in `wirelog/wirelog-types.h` are renamed to `wirelog_on_delta_fn` and
  `wirelog_on_tuple_fn` respectively, matching the `AGENTS.md:17-20`
  rule that public typedefs use the `wirelog_` prefix.  Source-
  incompatible for downstream consumers of the
  `wirelog_session_set_delta_cb` / `wirelog_session_snapshot` /
  `wl_easy_set_delta_cb` / `wl_easy_snapshot` parameter types.  Migrate
  via a textual rename; no signature change.  Tracked under
  `docs/MIGRATION.md` 0.30 -> 1.0 section.  Part of public-API prefix
  audit epic #755.

### Performance

- **CRDT W=8 -6.3% via leading-key cache in compact_runs heap** (PR #731): the K-way merge in `col_rel_compact_runs` now shadows the leading column with a stack-resident `int64_t lead_key[]` array parallel to the heap entries; the inner sift-down comparator short-circuits on column 0 instead of indirecting through `col_rel_row_cmp` for every comparison. CRDT W=8 5-rep median moves from 19.43s to 18.20s (-6.3%); first time `W=8 < W=1` on the dev box. No row-layout change, no sort-algorithm change, no public-header surface impact. Cross-workload (DOOP / CSPA / Polonius / Galen / DDISASM) tuple counts and gold relations preserved.

### Added

- **CRDT median-time perf gate** (PR #731): `tests/test_crdt_perf_gate.c` registered under `meson test --suite perf`. Drives the same Datalog source as `bench_flowlog --workload crdt` through the public `wirelog_*` API, asserts tuple count == 1,301,914 before any timing assertion, asserts coefficient of variation <= 3% (else SKIP), asserts median wall <= `WL_CRDT_PERF_GATE_TARGET_MS` (19,840 ms = baseline 18,890 ms x 1.05). Three-mode SKIP/SKIP/FAIL behaviour: SKIP by default; FAIL-loud under `WIRELOG_PERF_REQUIRE=1` when the cpufreq governor is not `performance` OR when the build is not `-Dwirelog_log_max_level=error` (per #731 follow-up commit). The escalator pattern lets dev hosts run `meson test --suite perf` cleanly while merge runners that opt into REQUIRE mode catch misconfiguration loud.
- **`bench/bench_crdt_workload.h`** (PR #731): the CRDT verification template is moved out of `bench_flowlog.c` into a shared header so the bench driver and the perf gate cannot drift on rule structure.
- **`wirelog/wirelog-advanced.h`** (#717, #703): New public header exposing the fine-grained `wirelog_session_*` API as the stable peer of `wl_easy`. Eight thin wrappers (`create` / `destroy` / `insert` / `remove` / `step` / `set_delta_cb` / `snapshot` / `make_compound`) over the internal session primitives. Backend selection through the `wirelog_backend_kind_t` enum (`DEFAULT` / `COLUMNAR`) — no vtable exposure. Inline `.dl` facts are seeded eagerly at `wirelog_session_create()` time, matching the wl_easy contract from #718. Internal `wl_session_*` and `wl_compute_backend_t` remain private.
- **CI guard** (#717): `scripts/ci/check-advanced-header.sh` (suite `abi`) fails when `wirelog/wirelog-advanced.h` includes any internal header.

### Fixed

- **wl_easy inline-fact materialization** (#718): `wl_easy_open` now seeds inline `.dl` facts into the columnar session at first lazy build, matching the CLI driver's order-of-operations. Previously the wl_easy facade dropped every static fact silently, so snapshots and IDB derivations re-evaluated against an empty EDB and returned no rows.

### Added

- **CLA-bot automation** (#702): `.github/workflows/cla.yml` runs the `contributor-assistant/github-action` on every PR. The bot blocks merging until every contributor has signed the wirelog CLA (recorded in `signatures/version1/cla.json`), protecting the LGPL-3.0-or-later + commercial dual-license model. Operational prerequisite documented inside the workflow file: a `CLA_SIGN_TOKEN` PAT secret with `contents:write` + `pull_requests:write` must be registered after merge.

### Documentation

- **README.md benchmark table refresh** (PR #731): full 16-workload portfolio re-measured at `--repeat 5` (5-trial medians) on the same dev host (cpufreq governor `schedutil`), replacing the previous `--repeat 1` snapshot. CRDT row reflects the new W=8 win (18.20s median, down from 19.43s). DOOP W=8 reflects unrelated post-baseline main-branch work (-51% vs prior table). Recipe block updated to `--repeat 5`. Numbers are descriptive; the regression gate in `meson test --suite perf` is the gated path.
- **`docs/SECURITY_MODEL.md`** (#701): new document recording the threat model, the mbedTLS-enabled build's license stack (Apache-2.0 + Apache-2.0 sub-dependencies on top of LGPL-3.0-or-later wirelog), and a good-faith export-control self-classification (ECCN 5D002.c.1 + License Exception ENC for `mbedTLS=enabled`; EAR99 for the default `disabled` build). Linked from README.md and from the `mbedTLS` option description in `meson_options.txt`.
- **README.md** (#717): replace the now-misleading `wl_session_*` / `wirelog/session.h` advisory with `wirelog_session_*` / `wirelog/wirelog-advanced.h`. The internal session header is explicitly called out as private.
- **`docs/SEMANTICS.md`** (#718): new document recording the engine's observable semantic-model decisions and the path toward 1.0 stabilization. First entry: inline `.dl` fact loading rules and the z-set host insert/remove model (status: Current).
- **`docs/SEMANTICS.md`** (#717): promote the cross-facade parity section from Future to Current now that the advanced surface ships.
- **`wirelog/wirelog.h`** (#717): expand the `wirelog_executor_t` docstring to clarify that it is the batch facade and to point at `wirelog_session_t` / `wl_easy_session_t` for incremental delta-callback workflows.

## [0.30.0] - 2026-05-07

### Added

- **I/O Adapter Framework** (#446): User-defined I/O adapters via runtime registry (`wl_io_register_adapter`). Public header `wirelog/io/io_adapter.h` with opaque context, ABI versioning (`WL_IO_ABI_VERSION=1`), and thread-safe registration API
- **Built-in CSV Adapter** (#455): CSV loading refactored into the adapter framework; backward-compatible `.input(filename=...)` dispatch
- **wl_easy Facade** (#445): Simplified high-level API (`wl_easy.h`) for common session workflows
- **String Operations** (#444): String-typed column functions (`strlen`, `cat`, `substr`, `contains`, `to_upper`, `to_lower`, `trim`, `str_replace`, `to_string`, `to_number`)
- **Path A Example** (#462): Standalone pcap adapter skeleton with CI compile-check against installed headers
- **C11 Threading Backend** (#494): Add C11 `<threads.h>` backend with auto-detection; POSIX/MSVC fallback preserved. `call_once` pattern for adapter registry initialization
- **Binary Size Gate** (#460): CI regression gate for `.text` section growth (5KB budget)
- **I/O Adapters User Guide** (#463): `docs/io-adapters.md` with Path A/B workflows, ABI policy, ownership rules, and thread-safety notes
- **Retraction Support** (#443): Fact retraction with recursive re-evaluation
- **Delta Query Examples**: Examples 08-12 demonstrating retraction, recursive update, time evolution, and snapshot-vs-delta patterns
- **Compound Terms**: Compound declaration parsing, inline compound declaration patterns, public side compound allocation, compound side support in flowlog benches, and daemon-style rotation examples
- **TDD Diagnostics**: Planner decision diagnostics, fallback decision stats, recursive TDD profiling counters, branch eligibility reports, and opt-in stratum profiling
- **Global-read TDD Infrastructure**: Guarded global-read recursive TDD path, candidate classification, rollback support, incremental shared-view refresh, and opt-in mixed child-plan execution

### Changed

- **Adaptive Worker Semantics**: Treat requested workers as an adaptive upper bound for K-fusion and TDD rather than a fixed allocation target
- **DOOP Benchmark Validation**: Validate DOOP benchmark output and refresh benchmark snapshots with W=N behavior
- **Recursive Evaluation**: Enable global-read TDD candidates by default while preserving guarded fallback paths
- **K-Fusion Scheduling**: Keep branch workers single-threaded, skip inactive branches, and use serial K-fusion below the dispatch threshold
- **CI Quality Gates**: Run CodeQL on main pushes and PRs, update artifact upload actions, and strengthen lint/code-quality checks

### Fixed

- **DOOP Worker Scaling** (#659): Current `main` no longer reproduces the workers-created-but-single-core DOOP path; W=8/W=16 runs preserve tuple/iteration parity and show active worker CPU
- **Worker Session Isolation**: Isolate filtered caches in worker sessions and add shared-view cleanup regression coverage
- **TDD Exchange Correctness**: Fix owner exchange delta registration and improve owner-mode fallback behavior for recursive workloads
- **MSVC Portability**: Add atomics shims, environment helper shims for tests, and portable atomic counter initialization
- **Join Robustness**: Fail closed on join output overflow and harden materialized join ownership

### Performance

- Parallelize eligible non-recursive relation plans, differential keyed joins, semijoin probing, and selected columnar join paths
- Optimize CRDT keyed probes and cap TDD width adaptively
- Reuse diff join left hash buckets and cache diff join match pairs
- Inline and specialize join, semijoin, arrangement, and owner-exchange key hashing helpers

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
