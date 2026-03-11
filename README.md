# wirelog

[![PR Lint](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/lint-pr.yml?label=PR%20Lint)](https://github.com/justinjoy/wirelog/actions/workflows/lint-pr.yml)
[![PR CI](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci-pr.yml?label=PR%20CI)](https://github.com/justinjoy/wirelog/actions/workflows/ci-pr.yml)
[![Main Lint](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/lint-main.yml?branch=main&label=Main%20Lint)](https://github.com/justinjoy/wirelog/actions/workflows/lint-main.yml)
[![Main CI](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci-main.yml?branch=main&label=Main%20CI)](https://github.com/justinjoy/wirelog/actions/workflows/ci-main.yml)

**Embedded-to-Enterprise Datalog Engine**

wirelog is a pure C11 Datalog engine with incremental evaluation. It compiles Datalog programs into an optimized columnar execution plan and evaluates them with delta-seeded semi-naive iteration, enabling efficient incremental updates over large datasets.

## Quick Facts

| Property | Value |
|----------|-------|
| Language | C11 (strict compliance) |
| Build | Meson + Ninja |
| Backend | Pure columnar (nanoarrow) |
| Phase | 4 — Incremental evaluation with delta-seeded propagation |
| Tests | 56+ passing |
| CSPA benchmark | 2.82x speedup over baseline |

## Features

- **Pure C11 codebase**: No Rust or DD dependency in the current build
- **Columnar execution**: nanoarrow-based columnar memory layout for cache efficiency
- **Delta-seeded incremental evaluation**: Re-evaluation after EDB insertion propagates only new (delta) tuples instead of re-deriving the full IDB from scratch
- **Frontier skip optimization**: Per-stratum frontier tracking skips iterations that cannot produce new results
- **Rule frontier with pre-seeded delta**: Selective per-rule frontier reset combined with pre-seeded delta injection (Issue #107)
- **Optimization passes**: Logic Fusion, Join-Project Plan (JPP), Semijoin Information Passing (SIP)
- **Benchmark suite**: 15+ workloads across graph analysis, pointer analysis, and program analysis

## Phase Status (March 2026)

| Phase | Description | Status |
|-------|-------------|--------|
| 0: Foundation | Parser, IR, stratification, CLI | Complete |
| 1: Optimization | Fusion, JPP, SIP, 15 benchmarks | Complete |
| 2: Columnar backend | DD removed, nanoarrow columnar executor | Complete |
| 3A: Symbol interning | C-only `wl_intern_t`, string-to-i64 mapping | Complete |
| 3B: Multi-worker dispatch | Columnar worker queue (Issue #99) | Complete |
| 3C: Incremental evaluation | Delta tracking, frontier array (Issue #83) | Complete |
| 3D: 2D frontier data structure | Per-rule frontier (Issue #104) | Complete |
| 3E: Rule frontier reset | Multi-stratum rule frontier (Issue #106) | Complete |
| 3F: Selective rule frontier | Pre-seeded delta + selective reset (Issue #107) | Complete |
| 3G: PR CI workflow | 3-phase blocking validation (Issue #117) | Complete |
| 3H: Main branch monitoring | Non-blocking comprehensive monitoring (Issue #116) | Complete |

## Getting Started

```bash
# Clone the repository
git clone https://github.com/justinjoy/wirelog.git
cd wirelog

# Build (requires Meson + C11 compiler)
meson setup build
meson compile -C build

# Run all tests
meson test -C build --print-errorlogs

# Run the CSPA benchmark
./build/bench/bench_flowlog \
  --workload cspa \
  --data bench/data/graph_10.csv \
  --data-weighted bench/data/graph_10_weighted.csv
```

### Benchmark Results

| Workload | Baseline | Incremental | Speedup |
|----------|----------|-------------|---------|
| CSPA | 10.4s | 3.7s | 2.82x |

## Architecture

### End-to-End Pipeline

```
.dl file
    |
    v  wl_read_file()
Parser (C11, hand-written recursive descent)
    |
    v
IR -> Fusion -> JPP -> SIP
    |
    v
Columnar Plan
    |
    v
Columnar Executor (nanoarrow)
    |   |
    |   +-- Incremental path: delta-seeded re-evaluation,
    |       frontier tracking, per-stratum skip
    v
Result callback / output
```

### Incremental Evaluation

After an initial `session_step()` call, wirelog tracks the frontier (convergence point) for each stratum. On subsequent calls with new EDB facts:

1. **Affected strata detection**: Compute which strata depend on the inserted relations.
2. **Delta pre-seeding**: Populate `$d$<name>` delta relations from `rows[base_nrows..nrows)`.
3. **Selective frontier reset**: Reset only affected strata; unaffected strata retain their frontier and skip unnecessary iterations.
4. **Semi-naive re-evaluation**: Only delta tuples propagate; no full IDB re-derivation.

This is what produces the 2.82x CSPA speedup: the delta path avoids re-computing tuples that did not change.

### Optimization Passes

| Pass | Description |
|------|-------------|
| Fusion | Merge adjacent FILTER+PROJECT into FLATMAP |
| JPP | Greedy join reorder for 3+ atom chains to minimize intermediate sizes |
| SIP | Insert semijoin pre-filters in join chains to reduce intermediate cardinality |

### Benchmark Workloads

| Category | Workloads |
|----------|-----------|
| Graph | TC, Reach, CC, SSSP, SG, Bipartite |
| Pointer Analysis | Andersen, CSPA, CSDA, Dyck-2 |
| Program Analysis | Galen, Polonius, CRDT, DDISASM, DOOP |

## CI/CD Strategy

wirelog uses a two-track CI strategy: strict blocking gates on pull requests, and comprehensive non-blocking monitoring on the main branch (Issues #116, #117).

### Pull Request Workflows (Blocking)

Every PR targeting `main` runs three sequential phases. Failure in an earlier phase stops later phases.

```
Phase 1: Lint  (lint-pr.yml)
  editorconfig-check
      |
  clang-format-18   [blocks if formatting differs from .clang-format]
      |
  clang-tidy-18     [blocks if static analysis warnings or errors found]
      |
Phase 2: Build and Test  (ci-pr.yml)
  Linux / GCC  -+
  Linux / Clang +-- fail-fast: first failure cancels remaining matrix jobs
  macOS / Clang-+
      |
Phase 3: Sanitizers  (ci-pr.yml)
  Linux / GCC   (ASan + UBSan) -+
  Linux / Clang (ASan + UBSan) -+  fail-fast
```

A PR cannot be merged unless all three phases pass.

### Main Branch Workflows (Non-Blocking Monitoring)

Pushes to `main` trigger broader coverage that runs to completion regardless of individual failures.

```
All phases run in parallel, continue-on-error: true

Lint monitor       (lint-main.yml)
  editorconfig-check
  clang-format-18
  clang-tidy-18

Build monitor      (ci-main.yml)
  Linux / GCC
  Linux / Clang
  macOS / Clang
  Windows / MSVC     <-- additional platform vs PR workflow

Sanitizers monitor (ci-main.yml)
  Linux / GCC   (ASan + UBSan)
  Linux / Clang (ASan + UBSan)
  macOS / Clang (ASan + UBSan)  <-- additional platform vs PR workflow
```

### Workflow File Reference

| File | Trigger | Mode | Purpose |
|------|---------|------|---------|
| `lint-pr.yml` | PR to `main` | Blocking | Sequential lint gates |
| `ci-pr.yml` | PR to `main` | Blocking | Build + sanitizer gates |
| `lint-main.yml` | Push to `main` | Non-blocking | Lint health monitoring |
| `ci-main.yml` | Push to `main` | Non-blocking | Comprehensive build + sanitizer monitoring |

### Local Development

Before opening a PR, run these checks locally:

```bash
# 1. Format all modified C files (required -- CI hard-gates on this)
/opt/homebrew/opt/llvm@18/bin/clang-format --style=file -i wirelog/*.c wirelog/*.h

# 2. Verify no formatting diff remains
git diff wirelog/ tests/

# 3. Run the full test suite
meson test -C build --print-errorlogs

# 4. Optional: run with sanitizers locally
meson setup build-san \
  -Db_sanitize=address,undefined \
  -Db_lundef=false \
  -Dtests=true \
  --buildtype=debug
meson test -C build-san --print-errorlogs
```

**Pre-commit hook (recommended)**: Add this to `.git/hooks/pre-commit`:

```bash
#!/bin/sh
CLANG_FORMAT=/opt/homebrew/opt/llvm@18/bin/clang-format
changed=$(git diff --cached --name-only --diff-filter=ACM | grep -E '\.(c|h)$')
if [ -n "$changed" ]; then
    echo "$changed" | xargs "$CLANG_FORMAT" --style=file -i
    echo "$changed" | xargs git add
fi
```

**PR merge requirements:**
1. All three CI phases must be green (lint, build, sanitizers)
2. clang-format 18 must report zero violations
3. clang-tidy 18 must report zero warnings or errors
4. All tests must pass on Linux GCC, Linux Clang, and macOS Clang

**Interpreting main branch CI failures:**
- Failures in `CI Main` and `Lint Main` are informational and do not block subsequent commits
- They should be investigated and addressed in a follow-up PR
- The Windows MSVC job and macOS sanitizer job exist only in the main monitor

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** -- Internal system design (developer reference)
- **[LICENSE.md](LICENSE.md)** -- Licensing information
- **[CONTRIBUTING.md](CONTRIBUTING.md)** -- Contribution guidelines
- **[SECURITY.md](SECURITY.md)** -- Security policy

## License

wirelog is available under **dual licensing**:

### Open Source: LGPL-3.0

wirelog is distributed under the GNU Lesser General Public License v3.0 (LGPL-3.0). This allows you to:

- Use wirelog as a library in proprietary applications
- Modify and distribute modified versions
- Link wirelog with proprietary software

For details, see [LICENSE.md](LICENSE.md).

### Commercial License

For proprietary or commercial use cases that require different terms:

**Contact**: inquiry@cleverplant.com

Use cases include closed-source commercial applications, OEM licensing, custom support agreements, and enterprise deployment with proprietary extensions.

## Contributing

wirelog is open source under LGPL-3.0. Contributions are welcome.

Please review [CONTRIBUTING.md](CONTRIBUTING.md), [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md), and [SECURITY.md](SECURITY.md) before submitting pull requests or reporting security issues.

By submitting a contribution, you agree to the [Contributor License Agreement (CLA)](CLA.md). This is required because wirelog uses dual licensing (LGPL-3.0 + Commercial).

## References

- **FlowLog Paper**: "[FlowLog: Efficient and Extensible Datalog via Incrementality](https://arxiv.org/pdf/2511.00865)" (PVLDB 2025)
- **Apache Arrow / nanoarrow**: [apache/arrow-nanoarrow](https://github.com/apache/arrow-nanoarrow)

---

wirelog -- precise incremental Datalog for embedded and enterprise environments.
