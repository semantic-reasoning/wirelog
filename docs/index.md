---
title: Home
layout: home
nav_order: 1
---

# wirelog

**Embedded-to-Enterprise Datalog Engine**

wirelog is a C11-based Datalog engine that compiles Datalog programs and executes them via [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow). It supports recursive queries, stratified negation, aggregation, and CSV data loading.

## Quick Start

```bash
# Build (requires Meson + C11 compiler + Rust toolchain)
meson setup builddir -Ddd=true
meson compile -C builddir

# Run a program
./builddir/wirelog-cli program.dl
```

**Hello World** -- transitive closure in 4 lines:

```
edge(1, 2). edge(2, 3). edge(3, 4).
tc(x, y) :- edge(x, y).
tc(x, z) :- tc(x, y), edge(y, z).
```

```
$ wirelog-cli tc.dl
tc(1, 2)
tc(1, 3)
tc(1, 4)
tc(2, 3)
tc(2, 4)
tc(3, 4)
```

## Documentation

| Document | Description |
|----------|-------------|
| [Tutorial](tutorial) | Step-by-step guide from first program to advanced features |
| [Language Reference](reference) | Grammar, types, operators, directives |
| [Examples](examples) | Learning-oriented example programs |
| [CLI Usage](cli) | Command-line interface reference |

## Links

- [GitHub Repository](https://github.com/justinjoy/wirelog)
- [Architecture](https://github.com/justinjoy/wirelog/blob/main/ARCHITECTURE.md) (developer reference)
- [Contributing](https://github.com/justinjoy/wirelog/blob/main/CONTRIBUTING.md)
