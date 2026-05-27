# Changelog

All notable changes to wirelog are documented in this file.

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

- Clarified #746 release-branch readiness/cutover sequencing:
  preparatory repo-side CI targeting now lands before branch creation,
  while final `release-1.x` branch-protection acceptance is deferred to
  the last-moment RC1 cutover when `project_version` is `1.0.0-rc1`,
  including synthetic PR verification expectations.

## [0.44.0] - 2026-05-25

### Added

- **CRC-32 checksum expressions** (#884): added the `crc32_ethernet(x)`
  and `crc32_castagnoli(x)` columnar expression built-ins. Each accepts a
  single arithmetic argument and returns the CRC-32 of its 8-byte `int64`
  representation as a non-negative `int64` (CRC-32/ISO-HDLC and CRC-32C
  respectively). Unlike the mbedTLS-backed digest/HMAC built-ins, the
  CRC-32 functions are always available regardless of the `mbedTLS` build
  option. See `docs/SYNTAX.md` and `examples/05-crc32-checksum/`.

### Changed

### Deprecated

### Removed

### Fixed

### Performance

### Security

### Documentation

## [0.43.0] - 2026-05-24

### Added

- Added a dedicated `mbedtls-enabled / ubuntu-latest / gcc` CI leg
  that configures with `-DmbedTLS=enabled`, verifies
  `WL_MBEDTLS_ENABLED=1`, and runs `cryptographic_hashes` so optional
  crypto paths are covered without changing the default disabled
  artifact posture (#843).
- Added `wirelog_program_get_relation_ir()`, a relation-scoped public
  accessor that returns the borrowed merged IR root for a derived
  relation and exposes multi-rule relations as `WIRELOG_IR_UNION`
  without inventing a program-level super-root (#860).

### Changed

- **Numerical safety fail-closed policy** (#822): columnar arithmetic
  now rejects unrepresentable `int64` results instead of wrapping,
  saturating, or leaking undefined behavior. Filters fail closed by
  rejecting rows; MAP/head and REDUCE expression contexts propagate
  `ERANGE`. The policy covers checked `+`, `-`, `*`, `/`, `%`,
  `bshl`, `bshr`, checked `to_number()` range parsing, and `sum()`
  accumulation, with the audit recorded in `docs/SEMANTICS.md`.

### Deprecated

### Removed

### Fixed

- **Recursive aggregation conformance for columnar MIN/MAX** (#692):
  REDUCE plan ops now carry aggregate expressions, so recursive
  aggregates evaluate `min(l)` / `max(d + w)` instead of falling back to
  a positional input column. This release includes the #852 recursive
  aggregation fix recorded under #859 while preserving #692 context.
  Recursive MIN/MAX IDBs are canonicalized after sequential
  fixed-point convergence and TDD final merge so dominated aggregate
  rows are removed from snapshots. Adds `recursive_agg_conformance`
  coverage for CC-min, SSSP-max, and stratified COUNT at workers 1,
  4, 8, and 16.

### Performance

- **Stable snapshot fast path** (#811): clean sessions now read cached
  materialized IDB rows directly on repeated snapshots instead of
  re-running TDD evaluation when no input or retraction state is pending.
- **CSPA W=1 perf gate** (#818): added `test_cspa_perf_gate` as a
  dedicated static `cspa-fast` regression guard for `W=1` using 9-trial
  median timing, 20,381-tuple/6-iteration correctness sentinels, and
  the shared `WIRELOG_PERF_GATE` / `WIRELOG_PERF_REQUIRE` control
  flow.
- **v0.43 benchmark speedup notes draft** (#794, #512, #791): added
  the portfolio speedup draft with #512 timing deltas, changed-commit
  provenance, and memory-trade-off context:
  [docs/release-notes/v0.43.md](docs/release-notes/v0.43.md).

### Security

### Documentation

- Documented the required-check policy for mbedTLS-enabled validation,
  including the stable `mbedtls-enabled / ubuntu-latest / gcc` check
  name and its PR, main-monitoring, `release-1.x`, and release-tag
  roles without changing the default `mbedTLS=disabled` artifact
  posture (#849).
- Added `docs/PLATFORM_SUPPORT.md` to classify Android/iOS release
  artifacts as Tier-2 for 1.x, documenting that Android AAR/Prefab and
  iOS XCFramework publication are deferred until explicit future
  promotion work is completed (#697).
- Added `docs/ios.md` with iOS integration guidance for
  `wirelog.xcframework` consumption, Swift callback registration with
  trailing `user_data`, simulator architecture handling, and
  App Store/tooling constraints for #470.
- Added `docs/android.md` with Android integration guidance for
  AAR/Prefab consumption patterns, JNI thread attachment patterns,
  `Context.getFilesDir()` path handling, and Android CI/alignment
  requirements for #466.

## [0.41.0] - 2026-05-20

### Added

- **Android CI smoke + 16 KB alignment hard gate** (#465):
  new `.github/workflows/android.yml` with three jobs.  `alignment-arm64`
  cross-compiles `libwirelog.so` via `cross/android-arm64.ini` and runs
  `scripts/ci/check-android-alignment.sh` against the result; the script
  uses `readelf -lW` to enumerate PT_LOAD segments and asserts each
  carries `Align >= 0x4000` (16 KB).  Hard-fails the PR on any
  insufficiently-aligned segment -- Play Store rejects new 4 KB-only
  arm64 binaries on API 35+ uploads (Nov 2025 enforcement).
  `negative-host` is the load-bearing self-test: builds the host Linux
  `.so` (which has 4 KB PT_LOAD alignment) and asserts the SAME script
  exits non-zero, proving the parser works in both directions and
  protecting against the fake-closure mode where a regex bug always
  passes.  `smoke-x86_64` cross-compiles the second ABI from #464
  without an alignment assertion (the 16 KB link flag is gated on
  `cpu_family == 'aarch64'`).  NDK r27c is installed via
  `nttld/setup-ndk@v1` and symlinked to `/opt/android-ndk-r27c` so the
  committed cross-files resolve their `ndk` constant without local
  edits.  All three jobs use the default `continue-on-error: false`
  (the deliberate opposite of #708's tsan-native advisory leg).
  Issue body's earlier `readelf -l | grep LOAD` recipe was superseded
  by the wide-mode parser; issue body's `r26d` NDK pin was reconciled
  up to `r27c` to match the cross-files shipped in #464.
- **Android meson option + NDK cross-files** (#464):
  new `meson_options.txt` boolean `android` (default false).  When
  enabled, `meson.build` excludes `wirelog_cli` from the build (no
  useful CLI entry on JNI / NDK apps), appends
  `-Wl,-z,max-page-size=16384` to `libwirelog.so`'s `link_args` on
  `aarch64` for Android 14+ 16 KB page compatibility (the flag is
  meaningless on x86_64 emulator builds and is omitted there), and
  cascades the CLI guard to the four CLI-dependent baseline tests in
  `tests/meson.build` (`baseline_int_edges`, `baseline_sym_family`,
  `baseline_tab_nodes`, `cli_version`).  Symbol-visibility policy is
  unchanged -- `gnu_symbol_visibility: 'hidden'` plus `WIRELOG_API`
  annotations on the 9 installed headers already cover all 53 Android
  exports, so the v1.0 ABI manifest gate at
  `scripts/ci/check-abi-symbols.sh` remains intact across platforms.
  Two NDK cross-files ship: `cross/android-arm64.ini` and
  `cross/android-x86_64.ini`, both pinned to NDK r27c (LTS), with
  `[constants]`-driven path composition and a documented override path
  for non-standard NDK install prefixes.  `armeabi-v7a` is intentionally
  not supported (Play Store has required 64-bit since 2019; armv7
  would carry a third toolchain for zero modern delivery value).  The
  recipe lives at `docs/cross-compile-android.md`; CI matrix coverage
  is a separate follow-up (out of scope for #464).
- **libabigail `.abi.json` ABI manifest + abidiff CI gate** (#786):
  the v1.0 ABI golden file gains its libabigail half.  Where the
  53-entry `abi/libwirelog-1.0.symbols` allowlist (#733 K2) only
  checks the dynamic-symbol *set*, the new
  `abi/libwirelog-1.0.abi.json` baseline (16 KB / 127 lines,
  generated reproducibly via
  `scripts/release/regenerate-abi-manifest.sh`) pins struct member
  offsets and padding, function-signature shapes, visibility
  attributes, and typedef targets.  `scripts/ci/check-abi-manifest.sh`
  (registered as `meson test --suite abi:abi_manifest`) runs
  `abidiff --suppr abi/libwirelog-1.0.suppr` (suppression file
  optional) against the just-built `build/libwirelog.so` and fails
  the gate when libabigail reports incompatible-change bits (rc & 4).
  Both Linux GCC and Clang legs of `.github/workflows/ci-pr.yml`
  and `.github/workflows/ci-main.yml` now `apt-get install
  abigail-tools` (the Ubuntu 24.04 package name; `libabigail-tools`
  is not a valid candidate on that runner image) so `abidiff` is
  on PATH in CI.  On non-x86_64 hosts (e.g. the new
  `ubuntu-24.04-arm` PR leg) the gate SKIPs cleanly with a clear
  message -- abidiff treats ELF architecture as an ABI-breaking
  change (`architecture changed from 'elf-amd-x86_64' to
  'elf-arm-aarch64'`, rc=12), so the committed x86_64 baseline
  cannot directly diff against an arm64 build.  Issue #824 explicitly
  scopes v1.0 Linux arm64 ABI coverage to the arch-agnostic
  `abi_symbols` allowlist on every PR; per-arch libabigail baselines
  are out of scope for #681 / v1.0 unless a later policy issue adds
  an arm64 baseline file and regeneration workflow.  The gate's other
  SKIP paths remain for platforms where libabigail is unavailable
  (Windows, macOS, cross-builds).  Demonstrated firing on a
  synthetic break: adding a new `WIRELOG_API` function trips the
  gate with `abidiff rc=4` and a clear remediation paragraph
  pointing at the regeneration script.  Closes the libabigail half
  of original ABI-Infrastructure scope from epic #690 (now
  decomposed under #786).
- **Cross-facade test-parity audit + CI gate** (#785):
  `tests/test_wirelog_advanced.c` grows from 7 to 21 test
  functions, mirroring `tests/test_wirelog_easy.c` invariants
  through the public `wirelog_session_*` surface.  14 new
  advanced tests cover parse errors, num_workers explicit
  values, intern stability (twice -- before and after first
  step), snapshot relation filtering, repeated open/use/close
  ordering, recursive multi-round delta callbacks, and the full
  inline-compound + side-compound parity sweep (5 inline-compound
  binding patterns + 1 side-compound saturation).  Every
  easy-side test is either paired with a same-named advanced
  test or carries a `/* PARITY: ... */` block-comment on its
  declaration line naming the structural reason no advanced
  analogue exists (12 facade-only annotations covering opts
  struct, `*_sym` variadics, and `wirelog_easy_print_delta`;
  the #665 partial-conjunction regression tests are now paired on
  the advanced side via #825).
  New gate `scripts/ci/check-test-parity.py` (registered as
  `meson test --suite abi:test_parity`) enforces the rule
  per-test, not as a numeric ratio, so future additions on
  either side cannot silently regress parity.  Result at this
  commit: 15 paired + 14 annotated = 29 easy tests covered.
- **`scripts/ci/check-threading-doc.sh`** (#734): static gate
  registered as `meson test --suite abi:threading_doc` that counts
  `atomic_*` call sites in `wirelog/` production sources and asserts
  the count matches the number of audit-table rows in
  `docs/THREADING.md` §5.  Drift in either direction (atomic_* added
  in code without a doc row, or doc row removed without code change)
  fails the gate with a clear diagnostic pointing at the section to
  update.
- **Compile-only smoke test for `WIRELOG_DEPRECATED_SINCE`** (#782):
  `tests/standalone/test_standalone_wirelog_deprecated_macro.c`
  annotates a static probe function with
  `WIRELOG_DEPRECATED_SINCE(99, 99)` and references it from `main`,
  so the macro's cross-compiler expansion path
  (GCC/Clang `__attribute__((deprecated))`, MSVC
  `__declspec(deprecated)`, no-op for unknown compilers) is
  exercised by the build before any real public-API deprecation
  ships.  Registered as `meson test --suite abi:standalone_include_wirelog_deprecated_macro`;
  the call-site deprecation diagnostic is suppressed per-target via
  `cc.get_supported_arguments(['-Wno-deprecated-declarations',
  '/wd4996'])` so the test does not break `-Werror` CI.
- **Public-API attribute lint backstop** (#782):
  `scripts/ci/check-public-api-macro.py` (registered as
  `meson test --suite abi:public_api_macro`) bans
  `WIRELOG_PUBLIC` on the 8 prototype headers after the v0.40-cycle
  adoption sweep to `WIRELOG_API`.  `WIRELOG_PUBLIC` remains valid
  only inside `wirelog/wirelog-export.h` where the platform `#if`
  ladder defines it.  Sources its header list by importing
  `PUBLIC_HEADERS` from the sibling `scripts/ci/check-public-prefix.py`
  so the surface stays in sync without a second hand-maintained list.

### Changed

- **CI: enroll Linux arm64 (`ubuntu-24.04-arm`) GCC build in the PR
  gate matrix** (#787): mirrors the arm64 leg already present in
  `.github/workflows/ci-main.yml` so the abi suite (`abi_symbols`,
  `public_header_surface`, `public_prefix`,
  `public_doxygen_headers`, `public_api_macro`, `test_parity`,
  `threading_doc`, `version_sync`, `changelog_format`,
  `release_template`, and the standalone-include compile tests)
  exercises the second supported architecture on every PR before
  merge.  Same-source-of-truth allowlist
  (`abi/libwirelog-1.0.symbols`) is intentionally arch-agnostic --
  `gnu_symbol_visibility: 'hidden'` plus `WIRELOG_API`-only export
  keeps SIMD/NEON wrappers internal so the 53-symbol set is
  identical across x86_64 and arm64.  Also fixes a stale matrix
  comparison in `ci-main.yml:84-95` that compared against the
  unused literal `'arm64'` instead of the actual runner label
  `'ubuntu-24.04-arm'`, so the SIMD-capability verification block
  now runs on the arm64 leg as intended.  Required-check
  promotion is a follow-up repo-admin action (branch protection),
  not part of this PR; per #824, Linux arm64 v1.0 ABI coverage is the
  arch-agnostic `abi_symbols` gate while libabigail `.abi.json`
  enforcement remains Linux x86_64-only.
- **Adopt `WIRELOG_API` on every installed public prototype** (#782):
  76 occurrences of `WIRELOG_PUBLIC` across the 8 prototype headers
  (`wirelog.h`, `wirelog-types.h`, `wirelog-ir.h`, `wirelog-parser.h`,
  `wirelog-optimizer.h`, `wirelog-easy.h`, `wirelog-advanced.h`,
  `wirelog/io/io_adapter.h`) are renamed to `WIRELOG_API`.  The rename
  is binary-identical: `#define WIRELOG_API WIRELOG_PUBLIC` at
  `wirelog/wirelog-export.h:34` expands to the same platform-specific
  attribute (`__attribute__((visibility("default")))` on GCC/Clang,
  `__declspec(dllexport)` / `__declspec(dllimport)` on Windows,
  no-op on `WIRELOG_STATIC` or unknown compilers).  The 53-entry
  allowlist at `abi/libwirelog-1.0.symbols` continues to pass the
  `abi_symbols` gate unchanged.  `WIRELOG_PUBLIC` is retained as a
  backward-compat alias for downstream code that referenced it
  directly during the v0.40 cycle.  Closes the adoption tail of
  v0.40 epic #680 exit condition 6 (macros were "introduced" at the
  definition level in v0.40; this commit puts them in use).
- **Forward-looking documentation of the visibility attribute renamed
  to `WIRELOG_API`** (#782): `docs/SEMANTICS.md` visibility table,
  `meson.build` library() comment, `scripts/ci/check-abi-symbols.sh`
  docstring, and the previously-stale "adoption sweep tracked
  separately" note in `wirelog/wirelog-export.h:31-33` all updated to
  reflect the new canonical attribute name.  `AGENTS.md` gains a
  "Visibility Attribute" subsection mandating `WIRELOG_API` for new
  public-API declarations.  History in `CHANGELOG.md` and
  `docs/MIGRATION.md` is left untouched -- those sections describe
  v0.40 state and should not retroactively change.

### Deprecated

### Removed

### Fixed

- **Advanced-side #665 partial-conjunction parity** (#825):
  `tests/test_wirelog_advanced.c` now mirrors the two easy-facade
  #665 partial-conjunction regression tests under both default and
  multi-worker execution.  The easy-side parity annotations now point
  at the live paired advanced tests instead of a closed #785 follow-up,
  and `scripts/ci/check-test-parity.py` continues to pass.
- **Optimizer-equivalence conformance matrix** (#700):
  `tests/test_optimizer_equivalence.c` now exercises the 16 combinations
  of Magic Sets, SIP, Logic Fusion, and JPP over join, recursive, and
  aggregate programs, comparing result Z-sets against the all-enabled
  baseline.  Magic Sets remains outside the public `wirelog_opt_pass_t`
  enum for v0.43; the matrix names the internal `wl_*_apply` symbols
  directly.
- **Config-aware optimizer pipeline wiring** (#700):
  the CLI and easy facade now call the shared `wirelog_optimize()` path
  instead of invoking optimizer passes directly, so public optimizer config
  behavior is exercised by user-facing pipelines.
- **Observable aggregate-skip counters** (#700):
  Magic Sets, SIP, Logic Fusion, and JPP stats now expose
  `skipped_aggregate`, and aggregate IR trees are skipped instead of being
  rewritten by passes that cannot safely transform them yet.
- **Platform ABI advisory test registration** (#788, #681):
  the macOS advisory export check is registered only on Darwin hosts
  and the Windows advisory export check only on Windows hosts.  This
  keeps Linux arm64 runners that happen to have `pwsh` installed from
  spending the Meson test timeout launching a Windows-only PowerShell
  check that should never run on that platform.

### Performance

### Security

### Documentation

- **`docs/SEMANTICS.md` optimizer-equivalence conformance section** (#700):
  now records the implemented v0.43 conformance state: matrix harness
  present, CLI/easy facade wired through the config-aware optimizer facade,
  Subsumption treated as canonicalization outside the toggle axis, Magic Sets
  kept off the public pass enum, and aggregate-skip behavior observable
  across all four matrix passes.

- **README Performance section: clarify `--repeat 5` methodology** (#736):
  fixes the stale `repeat=1` label on line 63 (now reads `--repeat 5`
  medians) to match the correct description already present on lines
  88-90.  Adds a short note explaining that historic single-trial numbers
  from pre-`1e6af00` README revisions are not directly comparable to
  current 5-trial medians; the +26% CSPA W=1 delta (1.55s -> 1.95s) is
  a measurement-methodology change, not a runtime regression, and 1.95s
  is the honest baseline.  Closes #736.
- **`docs/SEMANTICS.md` recursive aggregation residue definition + v0.43
  slip** (#692): adds a new "Recursive aggregation residue (Status:
  Future)" section defining "residue" operationally as the count of
  `'not yet implemented'` markers and disabled conformance tests in
  `tests/test_recursive_agg*.c` that block CC-min, SSSP-max, and
  count-stratified programs from producing correct output at workers in
  {1, 4, 8, 16}.  Records the current state (harness disabled at
  `tests/meson.build:184-198, 2190-2194`; `col_op_reduce` IS wired
  (`WL_PLAN_OP_REDUCE` case at `eval.c:267-269`) but conformance cannot
  run because the harness is disabled; `col_op_reduce_weighted` built
  but NOT dispatched (no `WL_PLAN_OP_REDUCE_WEIGHTED:` case) in the
  recursive dispatch switch at `wirelog/columnar/eval.c:241-288`;
  count-stratified
  scope asymmetry) and the path to residue = 0: Phase 2B prerequisite
  (#735, #809/#810/#811) then v0.43 harness re-port.  Narrows the v0.42
  exit criterion to "non-agg recursion residue = 0" via Phase 2B;
  recursive aggregation residue = 0 slips to v0.43 per architect+critic
  synthesis on 2026-05-18.
- **Advisory TSan compile smoke for `-Dthreads=native`** (#708, #826):
  `.github/workflows/ci-pr.yml` gains a `tsan-native` job mirroring the
  existing `tsan` (posix) configuration through configure/compile only,
  but it no longer runs the full native C11 runtime suite under TSan.
  Issue #826 showed that instrumented workers created through
  `thrd_create` can crash GCC/libtsan itself with
  `ThreadSanitizer:DEADLYSIGNAL` / SEGV `0x18`, before wirelog
  synchronization can be diagnosed; suppressions and per-test skips would
  therefore be misleading.  `docs/THREADING.md` section 11 now makes
  `-Dthreads=posix` the only gating race-detection surface, records
  native/glibc as compile-only advisory coverage, and leaves runtime C11
  backend coverage to the ordinary non-TSan matrix.  Closes #826; refs
  #708.
- **`docs/SEMANTICS.md` cross-facade parity audit subsection**
  (#785, under epic #681): the existing "Cross-facade parity
  (Status: Current)" block gains a new sub-block recording the
  per-test paired-or-annotated rule, the lint backstop, and the
  intentional reverse-parity asymmetry (backend selection is
  advanced-only by design).  Future maintainers reading the
  semantic model now see why some `test_create_*` tests are
  one-sided.
- **`docs/THREADING.md`** (#734, under epic #681): new canonical
  document covering wirelog's threading model -- backend selection
  (C11 `<threads.h>` > Win32 > POSIX, with `-Dthreads=posix` forcing
  pthreads as required for TSan), the three-layer atomics surface
  (direct `<stdatomic.h>` on GCC/Clang, MSVC shim in
  `mem_ledger.h:24-86`, MSVC shim in `lockfree_queue.c:22-37`), a
  40-row atomics audit table (every `atomic_*` call site in
  `wirelog/` with file:line + memory order + per-row justification),
  the lock-free SPSC delta queue ordering contract, K-fusion's two
  thresholds (K≥2 plan emission via `WL_PLAN_OP_K_FUSION`, K≥4
  parallel runtime via `WL_KFUSION_MIN_PARALLEL_K`), the
  compound-arena epoch boundary contract anchored by the
  `sess->coordinator == NULL` gate (#579), and the signal-safety
  stance (WL_LOG NOT async-signal-safe; do not call wirelog from
  signal handlers).  `README.md` and `docs/SEMANTICS.md` gain
  cross-links.

## [0.40.0] - 2026-05-12

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
