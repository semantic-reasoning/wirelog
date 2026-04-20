# wirelog

Precise incremental Datalog engine in pure C11. Compiles Datalog programs into columnar execution plans and evaluates them using timely-differential dataflow evaluation.

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
git clone https://github.com/semantic-reasoning/wirelog.git
cd wirelog
meson setup build && meson compile -C build
meson test -C build
```

For fine-grained control over plans, backends, or worker counts, use the `wl_session_*` primitives in `wirelog/session.h`.

## Features

- **Incremental evaluation** -- timely-differential dataflow evaluation propagates only new facts, not full re-derivation
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

## Logging (`WL_LOG`)

Runtime, section-filtered, level-gated diagnostics — GStreamer `GST_DEBUG`
style. Zero overhead when disabled: release builds strip disabled levels
at compile time (`&&` short-circuit over a compile-time constant guard);
runtime-disabled sites are a single cacheline byte load plus a
predicted-not-taken branch.

### Syntax

```
WL_LOG = entry ( ',' entry )*
entry  = name ':' level
name   = ident | '*'
level  = 0..5        # NONE=0  ERROR=1  WARN=2  INFO=3  DEBUG=4  TRACE=5
```

Whitespace is tolerated. The wildcard `*` sets all sections; subsequent
entries override per-section (last-wins). Unknown section names are
silently skipped. Malformed tokens zero the output and emit a one-time
`wirelog: malformed WL_LOG spec: <value>` on stderr.

Sections (closed enum in v1; extensions are a recompile):
`GENERAL`, `JOIN`, `CONSOLIDATION`, `ARRANGEMENT`, `EVAL`, `SESSION`,
`IO`, `PARSER`, `PLUGIN`.

### Examples

```bash
WL_LOG=JOIN:4 ./build/wirelog_cli run file.wl        # DEBUG on JOIN only
WL_LOG=CONSOLIDATION:3 ./build/bench/bench_flowlog   # INFO+ on CONSOLIDATION
WL_LOG=*:2,JOIN:5 ./build/wirelog_cli                # WARN+ everywhere, TRACE on JOIN
WL_LOG_FILE=/tmp/wl.log WL_LOG=JOIN:5 ./build/wirelog_cli
```

Output shape: `[LEVEL][SECTION] file:line: <message>`. Timestamps and
thread IDs are deferred to a follow-up. If `WL_LOG_FILE` fopen fails,
the logger falls back to `stderr` with a one-time notice.

### Release builds

Pass `-Dwirelog_log_max_level=error` to strip all levels above `ERROR`
at compile time — disabled sites contribute zero `.text` bytes and do
not evaluate their arguments. Meson emits a warning if you request
`--buildtype=release` without lowering the ceiling.

```bash
meson setup build-release --buildtype=release -Dwirelog_log_max_level=error
meson compile -C build-release
```

`meson test -C build --suite abi` includes a compile-erasure check that
rebuilds libwirelog with the ceiling at `error` and asserts TRACE-level
sentinel strings are absent from `.rodata`.

### Performance gate

A release-mode microbenchmark lives under `meson test --suite perf`.
Requires a `performance` CPU governor and the `trace` ceiling so the
runtime guard is exercised; skips with Meson code 77 otherwise rather
than silently passing on noisy hardware.

```bash
meson setup build-release --buildtype=release -Dwirelog_log_max_level=trace
meson compile -C build-release
taskset -c 0 meson test -C build-release --suite perf
```

Fail criteria: wall-clock delta > 1% OR per-iteration delta > 1 ns
against a no-log baseline (100M iters, 1M warmup, 5 trials, median).

### Safety caveats

- `WL_LOG` is **not** async-signal-safe. Do not call from signal
  handlers.
- After `fork()` in a child that changes the sink, call `wl_log_init()`
  again. No `pthread_atfork` handler is installed.
- Threshold writes happen only at init; reads are lock-free byte loads
  on a cacheline-aligned, padded table. Single-writer / many-reader at
  runtime is safe without TSan noise.

### Migrating from `WL_DEBUG_JOIN` / `WL_CONSOLIDATION_LOG` (#277)

The legacy presence-check flags continue to work: any value (including
`0`, matching their original semantics) seeds the matching section to
`TRACE` at init. `WL_LOG` overrides the shim, including explicit
silence via `WL_LOG=JOIN:0`.

| Legacy invocation | Canonical replacement |
|---|---|
| `WL_DEBUG_JOIN=1 ./app` | `WL_LOG=JOIN:5 ./app` |
| `WL_CONSOLIDATION_LOG=1 ./app` | `WL_LOG=CONSOLIDATION:5 ./app` |

A separate issue will retire the legacy env vars after an
external-consumer audit.

## Documentation

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) -- system design, optimizer pipeline, execution model
- [CONTRIBUTING.md](CONTRIBUTING.md) -- development workflow, CI/CD, PR requirements
- [SECURITY.md](SECURITY.md) -- vulnerability disclosure
- [CLA.md](CLA.md) -- Contributor License Agreement (required for dual licensing)
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
