# Changelog

All notable changes to wirelog are documented in this file.

## [Unreleased]

### Added

- **File-level Doxygen markers on every public header + CI gate**
  (#780, closes #680 exit condition): every entry in
  `wirelog_public_headers` (plus the standalone
  `install_headers('wirelog/io/io_adapter.h', ...)` call) now
  carries `@file` + `@brief` inside a `/** ... */` JavaDoc block
  placed between the SPDX C-comment and the include guard.  Eight
  headers gained both markers; `wirelog/wirelog-advanced.h` gained
  `@brief` (it already had `@file`).  A new gate
  `scripts/ci/check-public-doxygen-headers.py` (registered as
  `meson test --suite abi:public_doxygen_headers`) sources its
  header list from `parse_meson_sot` in
  `scripts/ci/check-public-header-surface.py` (the single SoT) and
  fails when any installed public header is missing either marker.
  Detection is regex-strict (`^\s*\*\s*@file\b`, `^\s*\*\s*@brief\b`)
  so per-parameter `@filename:` annotations in GTK-Doc-style function
  blocks do not satisfy the file-level check.
- **Release-process documentation + release-template CI gate** (#772):
  `docs/RELEASE_PROCESS.md` defines the canonical procedure for cutting
  a release tag, including the 9-section release-note template and the
  publication procedure (release PR, tag, GitHub Releases body, signed
  artefacts).  Two helper scripts land alongside:
  `scripts/release/extract-changelog-section.sh` (emits one CHANGELOG
  versioned section to stdout, used by `gh release create
  --notes-file`), and `scripts/ci/check-release-template.sh`
  (registered as `meson test --suite abi:release_template`) which
  diffs the published GitHub Releases body for the current tag against
  the corresponding CHANGELOG section.  The gate SKIPs outside
  tag-triggered CI context (PR builds, main branch) and enforces
  inside the release-tag.yml workflow (#749 B19).
- **CHANGELOG format CI gate** (#471):
  `scripts/ci/check-changelog-format.py` (registered as
  `meson test --suite abi:changelog_format`) asserts the
  `[Unreleased]` section follows the Keep-a-Changelog conventions
  documented in `CONTRIBUTING.md`: every bullet sits under one of
  the allowed categories (`Added`, `Changed`, `Deprecated`,
  `Removed`, `Fixed`, `Performance`, `Security`, `Documentation`)
  and carries at least one `#N` PR or issue reference.  Versioned
  section headers must use `## [X.Y.Z] - YYYY-MM-DD`.  CONTRIBUTING.md
  gains a "Changelog Conventions" section documenting the format,
  cutover procedure, and the freeze rule for `release-1.x`
  (cross-link with #747 B18).
- **ABI symbol allowlist gate (#733 K2)**: `meson test --suite abi:abi_symbols`
  diffs `nm -D --defined-only build/libwirelog.so | awk '$2=="T"'`
  against `abi/libwirelog-1.0.symbols`.  53 entries seeded from the
  current export set after `gnu_symbol_visibility: 'hidden'` lands.
  Any new public symbol must update the allowlist in the same PR;
  any accidental loss of an exported symbol fails the gate.  SKIPs
  cleanly on platforms without `libwirelog.so` (Windows / static-
  only).  Cross-link with #690 B3 (libabigail-based richer ABI
  manifest will sit alongside).

### Changed

- **libwirelog symbol-visibility default = hidden, SOVERSION=1**
  (#733 K1): the shared library now exports only `WIRELOG_PUBLIC`-
  annotated symbols (53 in v0.40 baseline).  Internal `wl_*` /
  `col_*` / `arr_*` / `eval_stack_*` symbols are no longer
  reachable through `libwirelog.so`'s dynamic-symbol table; the
  total exported `T`-class count drops from 348 to 53.  SONAME
  bumps from `libwirelog.so.0` to `libwirelog.so.1` via explicit
  `soversion: '1'`, decoupled from the pre-1.0 `project_version`
  so the 1.0 ABI commitment is fixed ahead of `1.0.0` release.
  macOS gains explicit `darwin_versions: ['1', '1.0.0']`.
  Source-incompatible for downstream code that relied on
  resolving internal symbols through `libwirelog.so` -- those
  consumers must rebuild against the public surface (or against
  the non-installed `libwirelog_static.a` which retains all
  symbols).
- **Public-API prototype annotation sweep** (#733 K0): the 7
  installed public headers (`wirelog.h`, `wirelog-types.h`,
  `wirelog-parser.h`, `wirelog-ir.h`, `wirelog-optimizer.h`,
  `wirelog-easy.h`, `wirelog-advanced.h`) plus
  `wirelog/io/io_adapter.h` ctx accessors gain `WIRELOG_PUBLIC`
  on every function prototype (72 prototypes total).  No
  source-level behaviour change before K1; load-bearing for the
  visibility flip in K1 (without it, every public function
  becomes hidden).

### Added

- **Standalone-include compile matrix for public headers** (#689):
  9 small `tests/standalone/test_standalone_<HEADER>.c` stubs, each
  including exactly one public installed header and a trivial
  `main()`, registered as `meson test --suite abi:standalone_include_*`.
  Failure = a public header has a hidden dependency on another
  header being included first (broken self-containment).  The
  GCC/Clang/MSVC compiler matrix is realised at the GitHub Actions
  level: each platform compiles every stub via its native CC.  Closes
  Blocker B2 of the v0.40 API audit and supplements the existing
  3-way SSoT verification at `scripts/ci/check-public-header-surface.py`.
- **CI lint backstop for public-API prefix conformance** (#761):
  `scripts/ci/check-public-prefix.py` (registered as
  `meson test --suite abi:public_prefix`) scans the 9 public
  installed headers for any `wl_*` / `WL_*` token that might
  leak through after the v0.40 API audit closes its sibling
  renames (#762 / #756 / #757 / #758 / #759 / #760).  An
  inline `ALLOW_LIST` documents the single intentional
  exception today (`struct wl_intern` tag forward-declared in
  `wirelog/wirelog.h` for the public `wirelog_intern_t`
  typedef per #760's design).  Closes the public-API prefix
  audit epic #755 by making the rule mechanically enforced
  rather than human-only.

### Changed

- **I/O adapter framework renamed and ABI bumped to 2u for v1.0
  prefix conformance** (#762): the entire `wirelog/io/io_adapter.h`
  surface (20 identifiers including `wl_io_*` symbols, `WL_IO_*`
  macros, the `"wl_io_plugin_entry"` dlsym key, and the
  `WL_IO_ABI_VERSION` macro) is renamed to `wirelog_io_*` /
  `WIRELOG_IO_*`, and `WIRELOG_IO_ABI_VERSION` is bumped from
  `1u` to `2u`.  The registration-time
  `adapter->abi_version != WIRELOG_IO_ABI_VERSION` check rejects
  v0.30.0 plugins loud (abi_version == 1u) with a clear
  `wirelog_io_last_error()` diagnostic.  Path-B plugins must
  rebuild and rename the exported entry symbol from
  `wl_io_plugin_entry` to `wirelog_io_plugin_entry`; otherwise
  the loader's `dlsym` lookup returns NULL.  Source- and binary-
  incompatible across the entire I/O adapter surface; pre-1.0
  ABI break per the v0.40 API audit decision.  Part of public-
  API prefix audit epic #755.
- **wl_easy facade renamed and moved for v1.0 prefix conformance**
  (#756): the `wl_easy` convenience facade is renamed across its
  entire surface from `wl_easy_*` / `WL_EASY_*` to
  `wirelog_easy_*` / `WIRELOG_EASY_*`.  The header and source
  files move from `wirelog/wl_easy.h` / `wirelog/wl_easy.c` to
  `wirelog/wirelog-easy.h` / `wirelog/wirelog-easy.c`; test files
  move accordingly.  `AGENTS.md` public-headers list and
  `meson.build` SSoT entries update with the move.  Source-
  incompatible across the entire facade; downstream consumers
  update `#include` paths plus a textual symbol rename.  Part of
  public-API prefix audit epic #755.
- **Export-attribute macro renamed for v1.0 prefix conformance**
  (#759): `wirelog/wirelog-export.h` no longer defines
  `WL_PUBLIC`; the new public name is `WIRELOG_PUBLIC`.
  `WIRELOG_API` continues as the alias and remains the recommended
  attribute name for new public-API declarations.  Source-
  incompatible for downstream code that referenced `WL_PUBLIC`
  directly; migrate via a textual rename to `WIRELOG_PUBLIC` (or
  switch to `WIRELOG_API`).  Part of public-API prefix audit
  epic #755.
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
  `wirelog_easy_set_delta_cb` / `wirelog_easy_snapshot` parameter types.  Migrate
  via a textual rename; no signature change.  Tracked under
  `docs/MIGRATION.md` 0.30 -> 1.0 section.  Part of public-API prefix
  audit epic #755.

### Performance

- **CRDT W=8 -6.3% via leading-key cache in compact_runs heap** (PR #731): the K-way merge in `col_rel_compact_runs` now shadows the leading column with a stack-resident `int64_t lead_key[]` array parallel to the heap entries; the inner sift-down comparator short-circuits on column 0 instead of indirecting through `col_rel_row_cmp` for every comparison. CRDT W=8 5-rep median moves from 19.43s to 18.20s (-6.3%); first time `W=8 < W=1` on the dev box. No row-layout change, no sort-algorithm change, no public-header surface impact. Cross-workload (DOOP / CSPA / Polonius / Galen / DDISASM) tuple counts and gold relations preserved.

### Added

- **CRDT median-time perf gate** (PR #731): `tests/test_crdt_perf_gate.c` registered under `meson test --suite perf`. Drives the same Datalog source as `bench_flowlog --workload crdt` through the public `wirelog_*` API, asserts tuple count == 1,301,914 before any timing assertion, asserts coefficient of variation <= 3% (else SKIP), asserts median wall <= `WL_CRDT_PERF_GATE_TARGET_MS` (19,840 ms = baseline 18,890 ms x 1.05). Three-mode SKIP/SKIP/FAIL behaviour: SKIP by default; FAIL-loud under `WIRELOG_PERF_REQUIRE=1` when the cpufreq governor is not `performance` OR when the build is not `-Dwirelog_log_max_level=error` (per #731 follow-up commit). The escalator pattern lets dev hosts run `meson test --suite perf` cleanly while merge runners that opt into REQUIRE mode catch misconfiguration loud.
- **`bench/bench_crdt_workload.h`** (PR #731): the CRDT verification template is moved out of `bench_flowlog.c` into a shared header so the bench driver and the perf gate cannot drift on rule structure.
- **`wirelog/wirelog-advanced.h`** (#717, #703): New public header exposing the fine-grained `wirelog_session_*` API as the stable peer of `wirelog_easy`. Eight thin wrappers (`create` / `destroy` / `insert` / `remove` / `step` / `set_delta_cb` / `snapshot` / `make_compound`) over the internal session primitives. Backend selection through the `wirelog_backend_kind_t` enum (`DEFAULT` / `COLUMNAR`) — no vtable exposure. Inline `.dl` facts are seeded eagerly at `wirelog_session_create()` time, matching the wirelog_easy contract from #718. Internal `wl_session_*` and `wl_compute_backend_t` remain private.
- **CI guard** (#717): `scripts/ci/check-advanced-header.sh` (suite `abi`) fails when `wirelog/wirelog-advanced.h` includes any internal header.

### Fixed

- **wirelog_easy inline-fact materialization** (#718): `wirelog_easy_open` now seeds inline `.dl` facts into the columnar session at first lazy build, matching the CLI driver's order-of-operations. Previously the wirelog_easy facade dropped every static fact silently, so snapshots and IDB derivations re-evaluated against an empty EDB and returned no rows.

### Added

- **CLA-bot automation** (#702): `.github/workflows/cla.yml` runs the `contributor-assistant/github-action` on every PR. The bot blocks merging until every contributor has signed the wirelog CLA (recorded in `signatures/version1/cla.json`), protecting the LGPL-3.0-or-later + commercial dual-license model. Operational prerequisite documented inside the workflow file: a `CLA_SIGN_TOKEN` PAT secret with `contents:write` + `pull_requests:write` must be registered after merge.

### Documentation

- **README.md benchmark table refresh** (PR #731): full 16-workload portfolio re-measured at `--repeat 5` (5-trial medians) on the same dev host (cpufreq governor `schedutil`), replacing the previous `--repeat 1` snapshot. CRDT row reflects the new W=8 win (18.20s median, down from 19.43s). DOOP W=8 reflects unrelated post-baseline main-branch work (-51% vs prior table). Recipe block updated to `--repeat 5`. Numbers are descriptive; the regression gate in `meson test --suite perf` is the gated path.
- **`docs/SECURITY_MODEL.md`** (#701): new document recording the threat model, the mbedTLS-enabled build's license stack (Apache-2.0 + Apache-2.0 sub-dependencies on top of LGPL-3.0-or-later wirelog), and a good-faith export-control self-classification (ECCN 5D002.c.1 + License Exception ENC for `mbedTLS=enabled`; EAR99 for the default `disabled` build). Linked from README.md and from the `mbedTLS` option description in `meson_options.txt`.
- **README.md** (#717): replace the now-misleading `wl_session_*` / `wirelog/session.h` advisory with `wirelog_session_*` / `wirelog/wirelog-advanced.h`. The internal session header is explicitly called out as private.
- **`docs/SEMANTICS.md`** (#718): new document recording the engine's observable semantic-model decisions and the path toward 1.0 stabilization. First entry: inline `.dl` fact loading rules and the z-set host insert/remove model (status: Current).
- **`docs/SEMANTICS.md`** (#717): promote the cross-facade parity section from Future to Current now that the advanced surface ships.
- **`wirelog/wirelog.h`** (#717): expand the `wirelog_executor_t` docstring to clarify that it is the batch facade and to point at `wirelog_session_t` / `wirelog_easy_session_t` for incremental delta-callback workflows.

## [0.30.0] - 2026-05-07

### Added

- **I/O Adapter Framework** (#446): User-defined I/O adapters via runtime registry (`wirelog_io_register_adapter`). Public header `wirelog/io/io_adapter.h` with opaque context, ABI versioning (`WIRELOG_IO_ABI_VERSION=1`), and thread-safe registration API
- **Built-in CSV Adapter** (#455): CSV loading refactored into the adapter framework; backward-compatible `.input(filename=...)` dispatch
- **wirelog_easy Facade** (#445): Simplified high-level API (`wirelog-easy.h`) for common session workflows
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
