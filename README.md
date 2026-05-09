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
#include <wirelog/wirelog.h>  /* umbrella: pulls in wl_easy and the rest */

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

For fine-grained control over plans, backends, or worker counts, use the `wirelog_session_*` API in [`wirelog/wirelog-advanced.h`](wirelog/wirelog-advanced.h). The internal `wl_session_*` primitives in `wirelog/session.h` are not part of the installed surface and may change without notice.

## Features

- **Incremental evaluation** -- timely-differential dataflow evaluation propagates only new facts, not full re-derivation
- **Columnar backend** -- [nanoarrow](https://github.com/apache/arrow-nanoarrow) (minimal Apache Arrow C implementation) memory layout for cache-efficient execution
- **SIMD acceleration** -- AVX2 (x86-64) and ARM NEON (ARM64) for hash, filter, and join operations
- **Optimizer pipeline** -- Logic Fusion, Join-Project Planning, Semijoin Information Passing, Magic Sets
- **Memory backpressure** -- thread-safe ledger with JOIN budget enforcement and graceful truncation
- **Pure C11** -- no runtime, no GC; strict AddressSanitizer + UndefinedBehaviorSanitizer validation

## Performance

15-workload benchmark portfolio (2026-05-06, `main` at `8240f97`,
release/LTO build, GCC 16.1.1, `repeat=1`):

**Test environment**: Intel Xeon E5-2696 v4 (22C/44T), Linux 6.19.14
(Arch), 44 logical CPUs, single NUMA node. Measurements were collected
from `./build/bench/bench_flowlog`; wall-clock results vary with CPU
governor, thermal state, and memory pressure.

| Category | Workload | W=1 median | W=8 median | Tuples | Iterations | Peak RSS (W=1 / W=8) |
|----------|----------|------------|------------|--------|------------|----------------------|
| Graph | TC (Transitive Closure) | 11.6ms | 6.7ms | 4,950 | 98 | 3.0MB / 3.2MB |
| Graph | Reach | 1.0ms | 0.6ms | 100 | 98 | 2.8MB / 2.8MB |
| Graph | CC (Connected Components) | 0.9ms | 0.6ms | 100 | 0 | 2.9MB / 2.9MB |
| Graph | SSSP (Shortest Path) | 0.7ms | 0.5ms | 1 | 0 | 2.8MB / 2.8MB |
| Graph | SG (Subgraph) | 0.7ms | 0.5ms | 0 | 0 | 2.8MB / 2.8MB |
| Graph | Bipartite | 1.5ms | 0.9ms | 100 | 73 | 3.0MB / 3.0MB |
| Pointer Analysis | Andersen | 3.9ms | 2.7ms | 155 | 8 | 3.0MB / 3.5MB |
| Pointer Analysis | Dyck-2 | 21.4ms | 8.5ms | 2,120 | 8 | 3.8MB / 6.9MB |
| Pointer Analysis | CSPA | 1.95s | 0.79s | 20,381 | 6 | 333MB / 514MB |
| Data Flow | CSDA | 2.5ms | 2.5ms | 2,986 | 29 | 3.1MB / 3.3MB |
| Ontology | Galen | 30.2ms | 22.6ms | 5,568 | 23 | 4.3MB / 8.0MB |
| Borrow Check | Polonius | 3.9ms | 4.0ms | 1,807 | 23 | 3.4MB / 3.5MB |
| Disassembly | DDISASM | 2.9ms | 3.3ms | 531 | 0 | 3.4MB / 3.6MB |
| CRDT | CRDT | 19.10s | 18.20s | 1,301,914 | 0 / 7,603 | 73MB / 271MB |
| Program Analysis | DOOP (zxing) | 55.00s | 26.22s | 6,276,657 | 28 | 12.5GB / 13.4GB |

Numbers are 5-trial medians (`--repeat 5`) on a single dev host with
cpufreq governor `schedutil`; treat them as descriptive, not gated.
The `meson test --suite perf` regression gate is separate and runs
under `-Dwirelog_log_max_level=error` plus a `performance` governor
(see `tests/test_crdt_perf_gate.c`).

**Incremental evaluation** (CSPA, delta-seeded): W=1 baseline 1.85s
-> incremental re-eval 21.7ms (**85.3x faster**); W=8 baseline 822.1ms
-> incremental re-eval 21.1ms (**38.9x faster**). Each run inserted one
fact and changed the result from 20,381 to 21,063 tuples.

`--workers N` means "use up to N workers", not "force exactly N workers for
every stratum". The evaluator selects an active width per eligible TDD or
K-Fusion path, falls back to narrower execution when a plan is not safely
partitionable, and caps some paths to avoid spending memory on idle worker
state. This keeps `W=N` adaptive: increasing N gives the runtime permission to
use more parallelism where it is semantically safe and profitable, while
single-threaded or narrow strata remain valid.

Re-run the large-workload snapshot with:

```bash
meson setup build --buildtype=release
meson compile -C build bench/bench_flowlog
for w in 1 8; do
  ./build/bench/bench_flowlog --workload tc --data bench/data/graph_100.csv --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload reach --data bench/data/graph_100.csv --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload cc --data bench/data/graph_100.csv --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload sssp --data bench/data/graph_100.csv --data-weighted bench/data/graph_100_weighted.csv --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload sg --data bench/data/graph_100.csv --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload bipartite --data bench/data/graph_100.csv --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload andersen --data-andersen bench/data/andersen --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload dyck --data-dyck bench/data/dyck --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload cspa-fast --data-cspa bench/data/cspa --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload csda --data-csda bench/data/csda --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload galen --data-galen bench/data/galen --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload polonius --data-polonius bench/data/polonius --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload ddisasm --data-ddisasm bench/data/ddisasm --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload crdt --data-crdt bench/data/crdt --workers "$w" --repeat 5
  ./build/bench/bench_flowlog --workload doop --data-doop bench/data/doop --workers "$w" --repeat 5
done
```

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
| `13-daemon-style` | Long-running daemon rotation pattern |

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
- [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md) -- threat model, mbedTLS license stack, and export-control self-classification
- [CLA.md](CLA.md) -- Contributor License Agreement (required for dual licensing)
- API: [`wirelog/wl_easy.h`](wirelog/wl_easy.h) (simple) | [`wirelog/wirelog-advanced.h`](wirelog/wirelog-advanced.h) (advanced)

## License

Wirelog is **dual-licensed** to serve both open-source and enterprise needs.

**LGPL-3.0-or-later** (default): Use wirelog as a library in your application -- open-source or proprietary -- without disclosing your own source code. Modifications to wirelog itself must be shared under LGPL-3.0-or-later. See [LICENSE.md](LICENSE.md) for full terms.

**Commercial license**: For use cases that require no LGPL obligations -- closed-source OEM embedding, proprietary extensions, or custom feature development -- a commercial license is available.

| | LGPL-3.0-or-later | Commercial |
|---|---|---|
| Use in proprietary apps | Yes (as a library) | Yes |
| Modify wirelog | Must share modifications | No obligation |
| OEM / embedded redistribution | Must allow relinking | Unrestricted |
| Priority support | Community only | Included |

**Contact**: inquiry@cleverplant.com
