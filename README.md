# wirelog

[![PR Lint](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/lint-pr.yml?label=PR%20Lint)](https://github.com/justinjoy/wirelog/actions/workflows/lint-pr.yml)
[![PR CI](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci-pr.yml?label=PR%20CI)](https://github.com/justinjoy/wirelog/actions/workflows/ci-pr.yml)
[![Main Lint](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/lint-main.yml?branch=main&label=Main%20Lint)](https://github.com/justinjoy/wirelog/actions/workflows/lint-main.yml)
[![Main CI](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci-main.yml?branch=main&label=Main%20CI)](https://github.com/justinjoy/wirelog/actions/workflows/ci-main.yml)

**Precise Incremental Datalog for Embedded and Enterprise Environments**

wirelog is a pure C11 Datalog engine designed for **high-performance incremental evaluation** across embedded systems and enterprise applications. It compiles Datalog programs into optimized columnar execution plans and evaluates them using delta-seeded semi-naive iteration, delivering **2.8x+ speedup** on incremental updates while maintaining strict memory safety and portability.

## What is Wirelog?

Wirelog is a **declarative logic programming engine** that brings the power of Datalog—a language for expressing complex queries and analyses—to performance-critical applications. Unlike traditional Datalog implementations that re-compute entire results on every update, wirelog uses **incremental evaluation** to propagate only new facts, delivering orders-of-magnitude speedups on real-world workloads.

**Use cases include:**
- **Pointer analysis** (C/C++ static analysis, vulnerability detection)
- **Program analysis** (data-flow analysis, security policies, reachability)
- **Graph algorithms** (transitive closure, strongly connected components, shortest paths)
- **Configuration analysis** (policy compliance, access control, dependency resolution)
- **Network analysis** (reachability, routing, audit log correlation)

## Why Wirelog?

### Performance-First Design
- **2.8x faster incremental updates** via delta-seeded semi-naive evaluation
- **Minimal memory footprint** — columnar layout (nanoarrow) with on-demand materialization
- **Cache-efficient execution** — columnar batching reduces memory bandwidth
- **Frontier-driven optimization** — skips unnecessary iterations on unchanged data

### Production-Ready
- **Strict C11 compliance** — no Rust, no virtual machines, no GC pauses
- **Zero dependencies** (nanoarrow vendored) — single binary, deployable anywhere
- **Cross-platform** — Unix/Linux/macOS/Windows with identical semantics
- **Memory-safe** — AddressSanitizer + UndefinedBehaviorSanitizer validated
- **CI/CD hardened** — three-phase PR gates (lint, build, sanitizers) + main branch monitoring

### Developer-Friendly
- **Declarative syntax** — express logic once, optimize automatically
- **Session API** — incremental snapshot/update/query workflow
- **Symbol interning** — efficient string-to-integer mapping for large vocabularies
- **Optimization passes** — Logic Fusion, Join-Project Planning, Semijoin Information Passing

## Quick Facts

| Property | Value |
|----------|-------|
| Language | C11 (strict compliance) |
| Build | Meson + Ninja |
| Backend | Pure columnar (nanoarrow) |
| Threading | Cross-platform abstraction (POSIX pthreads / MSVC) |
| Platforms | Unix/Linux/macOS (primary), Windows (MSVC) |
| Phase | 4 — Incremental evaluation with delta-seeded propagation |
| Tests | 56+ passing, ASan/UBSan validated |
| CSPA benchmark | **2.82x speedup** (baseline 10.4s → incremental 3.7s) |

## Core Features

### Incremental Evaluation Engine
- **Delta-seeded propagation**: Only new facts propagate through the evaluation pipeline, avoiding redundant re-computation
- **Per-stratum frontier tracking**: Each rule layer remembers its convergence point; unchanged strata skip unnecessary iterations
- **Selective rule frontier reset**: Insert decisions based on data dependency graph; only affected rules re-evaluate
- **Semi-naive iteration**: Optimized delta computation strategy from Datalog theory

### Columnar Architecture
- **Pure nanoarrow backend**: Apache Arrow columnar format for memory efficiency and SIMD friendliness
- **Cache-efficient batching**: Columnar memory layout reduces cache misses and memory bandwidth
- **No intermediate materialization**: Results computed on-demand without storing full intermediate relations

### Optimization Pipeline
- **Logic Fusion**: Merge adjacent FILTER+PROJECT operations into efficient FLATMAP instructions
- **Join-Project Planning (JPP)**: Greedy join reordering to minimize intermediate cardinalities
- **Semijoin Information Passing (SIP)**: Pre-filter joins with semijoin keys to reduce data flowing through the pipeline

### Reliability & Safety
- **Memory safety guarantees**: AddressSanitizer + UndefinedBehaviorSanitizer pass on all platforms
- **Portable C11**: No Rust, no vendored bytecode; single portable C codebase
- **Comprehensive test suite**: 56+ unit tests, regression suite, and 15+ benchmarks

### Production Features
- **Symbol interning**: Efficient string→i64 mapping for large vocabularies (Issue #56)
- **Worker queue support**: Cross-platform threading (POSIX pthreads / MSVC)
- **Stratification analysis**: Automatic rule ordering for negation handling
- **CLI tools**: wirelog-cli for standalone execution; examples for programmatic use

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

### Performance: Incremental Speedup

Wirelog's incremental evaluation shines when processing updates to derived relations. The **CSPA (Demand-Driven Context-Sensitive Pointer Analysis)** benchmark demonstrates real-world gains:

| Metric | Baseline | Incremental | Gain |
|--------|----------|-------------|------|
| Evaluation time | 10.4s | 3.7s | **2.82x faster** |
| Peak memory | 13.5GB | 6.2GB | **54% reduction** |
| Iterations evaluated | 6 | 5 | **1 iteration skipped** |

**Why the speedup?** The CSPA workload inserts derived facts incrementally. Wirelog:
1. Identifies affected strata (rules depending on inserted facts)
2. Pre-seeds delta relations with only new tuples
3. Re-evaluates only affected strata; unaffected rules skip to their frontier
4. Result: Only the "new information" propagates, avoiding redundant computation

**Workload portfolio**: 15+ benchmarks across graph analysis (TC, Reach, CC, SSSP), pointer analysis (Andersen, CSPA, CSDA), and program analysis (DOOP, Polonius, DDISASM)

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

## Licensing

Wirelog uses **dual licensing** to serve both open-source and enterprise needs.

### Open Source: LGPL-3.0

Wirelog is distributed under the **GNU Lesser General Public License v3.0 (LGPL-3.0)**:

**You can:**
- ✓ Use wirelog as a library in proprietary applications (no source disclosure required)
- ✓ Modify and distribute modified versions (under LGPL-3.0)
- ✓ Deploy in closed-source products
- ✓ Link with proprietary code

**You must:**
- Document use of wirelog and provide a copy of LGPL-3.0
- Allow recipients to relink against modified versions of wirelog
- Disclose modifications to wirelog itself (not your application)

For full details, see [LICENSE.md](LICENSE.md).

### Commercial License

For use cases requiring different terms or proprietary extensions:

**Contact**: inquiry@cleverplant.com

**Commercial licensing covers:**
- Closed-source OEM embedding
- Custom feature development
- Priority support agreements
- Proprietary extensions (no LGPL obligations)
- Volume licensing discounts

## Security

Wirelog is designed for security-critical applications. We take security seriously.

**Security Commitment:**
- All pull requests are validated with AddressSanitizer (ASan) and UndefinedBehaviorSanitizer (UBSan)
- Memory safety is guaranteed by strict C11 + sanitizer passes
- Zero-copy columnar architecture eliminates many attack surfaces

**Report Security Issues:**
Please review [SECURITY.md](SECURITY.md) for vulnerability disclosure procedures.
- Do **NOT** open public GitHub issues for security vulnerabilities
- Email security concerns to the maintainers (see SECURITY.md)
- Wirelog follows responsible disclosure practices

## Contributing

Wirelog is open source under LGPL-3.0. Contributions are welcome and essential to the project's growth.

### Before You Contribute

1. **Read the guidelines**: Review [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow
2. **Understand the code of conduct**: See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
3. **Review security policy**: Check [SECURITY.md](SECURITY.md) for vulnerability handling
4. **Sign the CLA**: By submitting a PR, you agree to the [Contributor License Agreement (CLA)](CLA.md)
   - Required due to dual licensing (LGPL-3.0 + Commercial)
   - Protects both contributors and users

### Contribution Types

- **Bug reports**: File issues with reproduction steps and test case
- **Performance improvements**: Profile, optimize, and benchmark (CSPA workload preferred)
- **New optimizations**: Propose fusion rules, join orders, or query transformations
- **Platform support**: Add Windows/ARM/exotic platform handling
- **Documentation**: Improve guides, API docs, architecture notes
- **Test coverage**: Add regression tests, property-based tests, or benchmarks

### Development Workflow

```bash
# 1. Fork and clone
git clone https://github.com/YOUR_USERNAME/wirelog.git
cd wirelog

# 2. Create a feature branch
git checkout -b feat/your-feature

# 3. Make changes; ensure linting passes
/opt/homebrew/opt/llvm@18/bin/clang-format --style=file -i wirelog/*.c wirelog/*.h

# 4. Run tests locally
meson setup build
meson compile -C build
meson test -C build --print-errorlogs

# 5. Push and open a PR
git push origin feat/your-feature
```

**PR Requirements:**
- All three CI phases pass (lint, build+test, sanitizers)
- clang-format 18 validation ✓
- clang-tidy 18 clean ✓
- Tests pass on Linux (GCC + Clang) and macOS (Clang)

## Documentation & Resources

| Resource | Purpose |
|----------|---------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Internal design, optimizer pipeline, execution model |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development guidelines, PR workflow, CI expectations |
| [SECURITY.md](SECURITY.md) | Vulnerability disclosure, security policy |
| [LICENSE.md](LICENSE.md) | Full LGPL-3.0 text and licensing details |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Community standards and expected behavior |
| [CLA.md](CLA.md) | Contributor License Agreement for dual licensing |

## Research & Citations

- **FlowLog Paper**: "[FlowLog: Efficient and Extensible Datalog via Incrementality](https://arxiv.org/pdf/2511.00865)" — PVLDB 2025 — The foundational research behind wirelog's incremental evaluation strategy
- **Apache Arrow / nanoarrow**: [apache/arrow-nanoarrow](https://github.com/apache/arrow-nanoarrow) — Columnar memory format powering wirelog's backend

## Getting Help

- **Issues**: File bugs, request features, or ask questions on [GitHub Issues](https://github.com/justinjoy/wirelog/issues)
- **Discussions**: Join the community on [GitHub Discussions](https://github.com/justinjoy/wirelog/discussions)
- **Commercial Support**: Contact inquiry@cleverplant.com for enterprise support and consulting

---

**Wirelog** — Precise incremental Datalog for embedded and enterprise environments.

Built with performance, safety, and portability in mind.
