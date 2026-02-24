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
- **Differential Dataflow Integration**: Leverages proven incremental processing
- **Datalog Optimization**: Logic Fusion, Join-Project-Plan, Subplan Sharing, Boolean Specialization
- **Layered Architecture**: Clean separation of Logic, Execution, and I/O
- **FPGA-Ready**: Designed for future hardware acceleration via Arrow IPC
- **Minimal Dependencies**: C11 + Meson build system

## Status

**Phase 0: Foundation** - Parser, IR, Stratification, and DD Plan Translator implemented.

| Component | Tests | Status |
|-----------|-------|--------|
| Parser | 91 | Complete |
| IR | 56 | Complete |
| Stratification | 20 | Complete |
| DD Plan Translator | 19 | Complete |
| **Total** | **186** | **All passing** |

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

## Project Structure

```
wirelog/
├── wirelog/              # Source code
│   ├── parser/          # Datalog parser (lexer, parser, AST)
│   │   ├── lexer.c/h
│   │   ├── parser.c/h
│   │   └── ast.c/h
│   └── ir/              # IR, stratification, DD plan translator
│       ├── ir.c/h
│       ├── program.c/h
│       ├── stratify.c/h
│       └── dd_plan.c/h
├── tests/               # Test suite (186 tests)
├── docs/                # Documentation
├── discussion/          # Design discussions and analysis
└── third_party/         # External libraries (nanoarrow, etc.)
```

## Architecture Highlights

### Phase 0-3: All Environments on Differential Dataflow

```
wirelog (C11 parser/optimizer)
    ↓
IR → DD operator graph
    ↓
Differential Dataflow executor
    ↓
Results (all environments)
```

- **Embedded**: Single-worker DD, memory-constrained
- **Enterprise**: Multi-worker DD, distributed processing

### Phase 3+: Optional Embedded Optimization

```
wirelog (C11 parser/optimizer)
    ├─ Enterprise: Differential Dataflow (unchanged)
    └─ Embedded: nanoarrow executor (optional)
```

For details, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Technology Stack

- **Language**: C11
- **Build**: Meson
- **Execution** (Phase 0-3): Differential Dataflow (Rust)
- **Memory** (Phase 3+): nanoarrow (optional)
- **Hardware Acceleration**: Arrow IPC for FPGA/GPU offload (future)

## Development Roadmap

| Phase | Timeline | Deliverable |
|-------|----------|-------------|
| 0: Foundation | Weeks 1-4 | Parser, IR, DD translator |
| 1: Optimization | Weeks 5-10 | Logic Fusion, JPP, SIP, Subplan Sharing |
| 2: Baseline | Weeks 11-14 | Performance benchmarking |
| 3: Embedded Opt. | Month 4+ | nanoarrow backend (optional) |
| 4: FPGA Support | Month 6+ | Hardware acceleration (optional) |

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Detailed system design (English)
- **[ARCHITECTURE.ko.md](docs/ARCHITECTURE.ko.md)** - Detailed system design (Korean)
- **[docs/README.md](docs/README.md)** - Documentation guide
- **[LICENSE.md](LICENSE.md)** - Licensing information
- **[discussion/](discussion/)** - Design discussions and analysis

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

