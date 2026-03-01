# wirelog

[![Sanitizers](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci.yml?branch=main&label=Sanitizers)](https://github.com/justinjoy/wirelog/actions/workflows/ci.yml)
[![Linux GCC](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci.yml?branch=main&label=Linux%20GCC)](https://github.com/justinjoy/wirelog/actions/workflows/ci.yml)
[![Linux Clang](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci.yml?branch=main&label=Linux%20Clang)](https://github.com/justinjoy/wirelog/actions/workflows/ci.yml)
[![Linux Embedded](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci.yml?branch=main&label=Linux%20Embedded)](https://github.com/justinjoy/wirelog/actions/workflows/ci.yml)
[![macOS](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci.yml?branch=main&label=macOS)](https://github.com/justinjoy/wirelog/actions/workflows/ci.yml)
[![Windows MSVC](https://img.shields.io/github/actions/workflow/status/justinjoy/wirelog/ci.yml?branch=main&label=Windows%20MSVC)](https://github.com/justinjoy/wirelog/actions/workflows/ci.yml)

**Embedded-to-Enterprise Datalog Engine**

wirelog is a C11-based Datalog engine designed to work seamlessly across embedded systems and enterprise environments. It uses Differential Dataflow for execution and can optionally be optimized with nanoarrow columnar memory for embedded deployments.

## Features

- **Unified Codebase**: Single implementation for embedded and enterprise
- **Differential Dataflow Integration**: Proven incremental processing via Rust FFI
- **Optimization Pipeline**: Logic Fusion, Join-Project Plan (JPP), Semijoin Information Passing (SIP)
- **Benchmark Suite**: 15 workloads from graph analysis to Java points-to analysis (DOOP, 136 rules)
- **Layered Architecture**: Clean separation of Logic, Execution, and I/O
- **FPGA-Ready**: Designed for future hardware acceleration via Arrow IPC
- **Minimal Dependencies**: C11 + Meson build system

## Status

**Phase 0: Foundation** complete. **Phase 1: Optimization** complete.

| Component | Tests | Status |
|-----------|-------|--------|
| Parser (lexer + parser) | 96 | Complete |
| IR (ir + program) | 61 | Complete |
| Stratification | 20 | Complete |
| DD Plan Translator | 22 | Complete |
| FFI Marshalling | 31 | Complete |
| Optimization (Fusion + JPP + SIP) | 36 | Complete |
| DD Execute (end-to-end) | 18 | Complete |
| CLI Driver | 15 | Complete |
| Symbol Interning | 9 | Complete |
| CSV Input | 17 | Complete |
| **C Total** | **325** | **14 suites, all passing** |
| Rust DD Executor | 85 | Complete |
| **Grand Total** | **410** | **All passing** |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for design details.

## Quick Start

```bash
# Clone repository
git clone https://github.com/justinjoy/wirelog.git
cd wirelog

# Build (requires Meson + C11 compiler)
meson setup builddir
meson compile -C builddir

# Run tests
meson test -C builddir
```

## Usage

```bash
# Build with DD executor (requires Rust toolchain)
meson setup builddir -Ddd=true
meson compile -C builddir

# Run a Datalog program
./builddir/wirelog-cli tc.dl

# With multiple workers
./builddir/wirelog-cli --workers 4 tc.dl
```

Example output for a transitive closure program:
```
tc(1, 2)
tc(1, 3)
tc(2, 3)
```

## Architecture

### End-to-End Pipeline

```
.dl file
    ↓ wl_read_file()
Parser (C11, hand-written RDP)
    ↓
IR → Fusion → JPP → SIP
    ↓
DD Plan → FFI Marshal
    ↓
Differential Dataflow (Rust)
    ↓ result callback
Output
```

- **Embedded**: Single-worker DD, memory-constrained
- **Enterprise**: Multi-worker DD (`--workers N`), distributed processing

### Optimization Passes

| Pass | Description |
|------|-------------|
| **Fusion** | Merge adjacent FILTER+PROJECT into FLATMAP |
| **JPP** | Greedy join reorder for 3+ atom chains to minimize intermediate sizes |
| **SIP** | Insert semijoin pre-filters in join chains to reduce intermediate cardinality |

### Benchmark Suite

15 workloads covering graph analysis, pointer analysis, and program analysis:

| Category | Workloads |
|----------|-----------|
| Graph | TC, Reach, CC, SSSP, SG, Bipartite |
| Pointer Analysis | Andersen, CSPA, CSDA, Dyck-2 |
| Advanced | Galen, Polonius, CRDT, DDISASM, DOOP |

### Phase 3+: Optional Embedded Optimization

```
wirelog (C11 parser/optimizer)
    ├─ Enterprise: Differential Dataflow (unchanged)
    └─ Embedded: nanoarrow executor (optional)
```

For details, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Technology Stack

- **Language**: C11
- **Build**: Meson + Ninja
- **Execution**: Differential Dataflow (Rust, dogs3 v0.19.1)
- **Memory** (future): nanoarrow (optional)
- **Hardware Acceleration**: Arrow IPC for FPGA/GPU offload (future)

## Development Roadmap

| Phase | Status | Deliverable |
|-------|--------|-------------|
| 0: Foundation | ✅ Complete | Parser, IR, DD translator, CLI |
| 1: Optimization | ✅ Complete | Fusion, JPP, SIP, 15 benchmarks |
| 2: Documentation | Planned | Language reference, tutorial, examples, CLI docs |
| 3: nanoarrow | Planned | C11-native executor, DD vs nanoarrow comparison |

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Detailed system design
- **[LICENSE.md](LICENSE.md)** - Licensing information

## License

wirelog is available under **dual licensing**:

### 1. Open Source: LGPL-3.0

wirelog is distributed under the GNU Lesser General Public License v3.0 (LGPL-3.0).

This allows you to:
- Use wirelog as a library in proprietary applications
- Modify and distribute modified versions
- Link wirelog with proprietary software

For details, see [LICENSE.md](LICENSE.md).

### 2. Commercial License

For proprietary or commercial use cases that require different terms:

**Contact**: inquiry@cleverplant.com

**Use cases include**:
- Closed-source commercial applications
- OEM licensing
- Custom support agreements
- Enterprise deployment with proprietary extensions

See [LICENSE.md](LICENSE.md) for full details.

## Contributing

wirelog is open source under LGPL-3.0. Contributions are welcome!

Please review our [Contributing Guidelines](CONTRIBUTING.md), [Code of Conduct](CODE_OF_CONDUCT.md), and [Security Policy](SECURITY.md) prior to submitting pull requests or reporting security issues.

By submitting a contribution, you agree to the [Contributor License Agreement (CLA)](CLA.md). This is required because wirelog uses dual licensing (LGPL-3.0 + Commercial).

## Support

- **Documentation**: [docs/](docs/)
- **Issues & Discussions**: GitHub Issues
- **Commercial Support**: inquiry@cleverplant.com

## References

- **FlowLog Paper**: "[FlowLog: Efficient and Extensible Datalog via Incrementality](https://arxiv.org/pdf/2511.00865)" (PVLDB 2025)
- **Differential Dataflow**: [TimelyDataflow/differential-dataflow](https://github.com/TimelyDataflow/differential-dataflow)
- **Apache Arrow**: [Apache/Arrow](https://arrow.apache.org/)

---

**wirelog** - Building bridges between embedded and enterprise data processing.
