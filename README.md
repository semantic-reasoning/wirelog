# wirelog

Precise incremental Datalog engine in pure C11. Compiles Datalog programs into columnar execution plans and evaluates them using delta-seeded semi-naive iteration.

## Quick Start

A Datalog program that computes transitive closure:

```datalog
.decl edge(a: symbol, b: symbol)
.decl path(a: symbol, b: symbol)

path(X, Y) :- edge(X, Y).
path(X, Z) :- path(X, Y), edge(Y, Z).
```

Run it from C using the `wl_easy` facade:

```c
#include "wirelog/wl_easy.h"

int main(void) {
    wl_easy_session_t *s = NULL;
    if (wl_easy_open(
            ".decl edge(a:symbol,b:symbol)\n"
            ".decl path(a:symbol,b:symbol)\n"
            "path(X,Y) :- edge(X,Y).\n"
            "path(X,Z) :- path(X,Y), edge(Y,Z).\n", &s) != WIRELOG_OK)
        return 1;

    wl_easy_set_delta_cb(s, wl_easy_print_delta, s);
    wl_easy_insert_sym(s, "edge", "a", "b", NULL);
    wl_easy_insert_sym(s, "edge", "b", "c", NULL);
    wl_easy_step(s);   /* prints: + path("a","b"), + path("b","c"), + path("a","c") */
    wl_easy_close(s);
    return 0;
}
```

Build and run:

```bash
git clone https://github.com/justinjoy/wirelog.git
cd wirelog
meson setup build && meson compile -C build
meson test -C build
```

For fine-grained control over plans, backends, or worker counts, use the `wl_session_*` primitives in `wirelog/session.h`.

## Features

- **Incremental evaluation** -- delta-seeded semi-naive iteration propagates only new facts, not full re-derivation
- **Columnar backend** -- [nanoarrow](https://github.com/apache/arrow-nanoarrow) (minimal Apache Arrow C implementation) memory layout for cache-efficient execution
- **SIMD acceleration** -- AVX2 (x86-64) and ARM NEON (ARM64) for hash, filter, and join operations
- **Optimizer pipeline** -- Logic Fusion, Join-Project Planning, Semijoin Information Passing, Magic Sets
- **Memory backpressure** -- thread-safe ledger with JOIN budget enforcement and graceful truncation
- **Pure C11** -- no runtime, no GC; strict AddressSanitizer + UndefinedBehaviorSanitizer validation

## Performance

15-workload benchmark portfolio (2026-04-10, Release -O2, single worker):

**Environment**: AMD Ryzen 5 5600G (6C/12T), 28GB RAM, Linux 6.19.10 (Arch), GCC 15.2.1

| Category | Workload | Median | Tuples | Iterations | Peak RSS |
|----------|----------|--------|--------|------------|----------|
| Graph | TC (Transitive Closure) | 8.7ms | 4,950 | 98 | 3MB |
| Graph | Reach | 0.5ms | 100 | 98 | 3MB |
| Graph | CC (Connected Components) | 0.3ms | 100 | 0 | 3MB |
| Graph | SSSP (Shortest Path) | 0.1ms | 1 | 0 | 3MB |
| Graph | SG (Subgraph) | 0.1ms | 0 | 0 | 3MB |
| Graph | Bipartite | 1.4ms | 100 | 73 | 3MB |
| Pointer Analysis | Andersen | 2.4ms | 155 | 8 | 3MB |
| Pointer Analysis | Dyck-2 | 16.1ms | 2,120 | 8 | 6MB |
| Pointer Analysis | CSPA | 2.43s | 20,381 | 6 | 893MB |
| Data Flow | CSDA | 3.7ms | 2,986 | 29 | ~1MB |
| Ontology | Galen | 36.7ms | 5,568 | 23 | ~1MB |
| Borrow Check | Polonius | 4.2ms | 1,807 | 23 | ~1MB |
| Disassembly | DDISASM | 4.6ms | 531 | 0 | ~1MB |
| CRDT | CRDT | 17.3s | 1,301,914 | 0 | ~1GB |
| Program Analysis | DOOP (zxing) | 155.3s | 6,276,338 | 28 | 15GB |

**Incremental evaluation** (CSPA, delta-seeded): baseline 2.45s → incremental re-eval 29.9ms (**82x faster**, 1 fact inserted, 5 iterations vs 6).

## Examples

| Directory | Topic |
|-----------|-------|
| `01-simple` | Ancestor computation (facts + recursive rules) |
| `02-graph-reachability` | Flight route reachability |
| `03-bitwise-operations` | Bitwise permission analysis |
| `04-hash-functions` | Hash-based deduplication |
| `05-crc32-checksum` | CRC32 checksum validation |
| `06-timestamp-lww` | Last-write-wins timestamp resolution |
| `07-multi-source-analysis` | Set operations across data sources |
| `08-delta-queries` | Delta callbacks with `wl_easy` |
| `09-retraction-basics` | Fact retraction with `-1` deltas |
| `10-recursive-under-update` | Transitive closure under insert/remove |
| `11-time-evolution` | Per-epoch delta isolation |
| `12-snapshot-vs-delta` | Snapshot vs streaming API comparison |

## Build & Test

```bash
meson setup build
meson compile -C build
meson test -C build --print-errorlogs    # 126 tests

# Sanitizer build (optional)
meson setup build-san -Db_sanitize=address,undefined
meson test -C build-san --print-errorlogs
```

Platforms: Linux (GCC/Clang), macOS (Clang), Windows (MSVC).

## Documentation

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) -- system design, optimizer pipeline, execution model
- [CONTRIBUTING.md](CONTRIBUTING.md) -- development workflow, CI/CD, PR requirements
- [SECURITY.md](SECURITY.md) -- vulnerability disclosure
- API: [`wirelog/wl_easy.h`](wirelog/wl_easy.h) (simple) | [`wirelog/session.h`](wirelog/session.h) (advanced)

## License

Wirelog is **dual-licensed** to serve both open-source and enterprise needs.

**LGPL-3.0** (default): Use wirelog as a library in your application -- open-source or proprietary -- without disclosing your own source code. Modifications to wirelog itself must be shared under LGPL-3.0. See [LICENSE.md](LICENSE.md) for full terms.

**Commercial license**: For use cases that require no LGPL obligations -- closed-source OEM embedding, proprietary extensions, or custom feature development -- a commercial license is available.

| | LGPL-3.0 | Commercial |
|---|---|---|
| Use in proprietary apps | Yes (as a library) | Yes |
| Modify wirelog | Must share modifications | No obligation |
| OEM / embedded redistribution | Must allow relinking | Unrestricted |
| Priority support | Community only | Included |

**Contact**: inquiry@cleverplant.com
