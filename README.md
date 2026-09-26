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

Run it from C using the `wirelog_easy` facade:

```c
#include <wirelog/wirelog.h>  /* umbrella: pulls in wirelog_easy and the rest */

int main(void) {
    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(
            ".decl edge(a:symbol,b:symbol)\n"
            ".decl path(a:symbol,b:symbol)\n"
            "path(X,Y) :- edge(X,Y).\n"
            "path(X,Z) :- path(X,Y), edge(Y,Z).\n", &s) != WIRELOG_OK)
        return 1;

    wirelog_easy_set_delta_cb(s, wirelog_easy_print_delta, s);
    wirelog_easy_insert_sym(s, "edge", "a", "b", NULL);
    wirelog_easy_insert_sym(s, "edge", "b", "c", NULL);
    wirelog_easy_step(s);   /* prints: + path("a","b"), + path("b","c"), + path("a","c") */
    wirelog_easy_close(s);
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

The future weighted/probabilistic evaluation design is documented in
[`docs/design/weighted-semiring-addon.md`](docs/design/weighted-semiring-addon.md).
The future scalar-function addon design is documented in
[`docs/design/scalar-function-addon.md`](docs/design/scalar-function-addon.md).
It is a design proposal, not a currently available API.

## Features

- **Incremental evaluation** -- timely-differential dataflow evaluation propagates only new facts, not full re-derivation
- **Columnar backend** -- [nanoarrow](https://github.com/apache/arrow-nanoarrow) (minimal Apache Arrow C implementation) memory layout for cache-efficient execution
- **SIMD acceleration** -- AVX2 (x86-64) and ARM NEON (ARM64) for hash, filter, and join operations
- **Optimizer pipeline** -- Logic Fusion, Join-Project Planning, Semijoin Information Passing, Magic Sets
- **Memory backpressure** -- thread-safe ledger with JOIN budget enforcement and graceful truncation
- **Pure C11** -- no runtime, no GC; strict AddressSanitizer + UndefinedBehaviorSanitizer validation

## Performance

The complete benchmark evidence is recorded in [docs/benchmarks/2026-09-26-flowlog](docs/benchmarks/2026-09-26-flowlog/). It contains two immutable 48-record matrices: the historical revision `7e498e782a4cca96175d34f86bf5b958d042552d` and the current benchmark head `afcdd8a3f05f67daaeb003c7acd25e96e2433419`, whose product base is main merge commit `30b19ba3` from #1975. Both use release `-Os`, LTO, GCC 16.2.1, `wirelog_log_max_level=error`, CPUs 0-15, and the same 82-file data inventory. Non-DOOP workloads use five-trial medians; DOOP uses one authoritative trial per width because each run is very long.

**Test environment:** Intel Xeon E5-2696 v4 (2 sockets, 44 physical cores total, 88 logical CPUs), Linux 7.2.6, 125 GiB RAM. Wall-clock values are descriptive and affected by governor, thermal state, memory pressure, and the campaigns' separate execution times. Peak RSS is shown in MiB.

| Category | Workload | W=1 median | W=8 median | W=16 median | Tuples | Iterations | Peak RSS (W=1 / W=8 / W=16 MiB) |
|---|---|---:|---:|---:|---:|---:|---:|
| Graph | TC | 10.4ms | 10.1ms | 14.0ms | 4,950 | 98 | 3.9 / 3.3 / 3.8 |
| Graph | Reach | 1.9ms | 1.9ms | 1.9ms | 100 | 98 | 3.1 / 3.4 / 3.4 |
| Graph | CC | 13.8ms | 13.6ms | 14.0ms | 100 | 99 | 4.1 / 3.8 / 4.1 |
| Graph | SSSP | 2.3ms | 2.0ms | 2.6ms | 100 | 98 | 3.4 / 3.4 / 3.4 |
| Graph | SG | 0.8ms | 0.8ms | 0.8ms | 0 | 0 | 3.4 / 3.4 / 3.4 |
| Graph | Bipartite | 3.0ms | 2.5ms | 2.5ms | 100 | 73 | 3.4 / 3.2 / 3.3 |
| Pointer Analysis | Andersen | 4.5ms | 4.8ms | 5.2ms | 155 | 8 | 3.5 / 4.1 / 4.1 |
| Pointer Analysis | Dyck-2 | 29.1ms | 29.6ms | 27.6ms | 2,120 | 8 | 5.8 / 21.8 / 7.2 |
| Pointer Analysis | CSPA (static) | 4.80s | 2.43s | 2.41s | 20,381 | 6 | 320.3 / 408.8 / 406.0 |
| Data Flow | CSDA | 4.9ms | 4.9ms | 4.9ms | 2,986 | 29 | 3.5 / 3.9 / 3.5 |
| Ontology | Galen | 74.1ms | 115.7ms | 105.1ms | 5,568 | 23 | 6.5 / 15.9 / 13.0 |
| Borrow Check | Polonius | 8.3ms | 9.4ms | 12.4ms | 1,983 | 23 / 25 / 25 | 4.0 / 4.4 / 4.4 |
| Disassembly | DDISASM | 7.5ms | 10.6ms | 11.0ms | 704 | 0 / 19 / 19 | 3.9 / 4.5 / 16.8 |
| CRDT | CRDT | 44.87s | 45.54s | 46.81s | 2,152,328 | 14,148 | 123.4 / 488.1 / 770.7 |
| Program Analysis | DOOP (zxing) | 4,677.9s | 2,871.6s | 2,847.9s | 13,828,835 | 153 | 53,369.6 / 55,158.3 / 55,231.1 |

The CSPA static row is `cspa-fast`. The separate incremental CSPA check (`cspa`) inserted 35 synthetic `Assign` facts (20% of the 179 input rows) and produced 21,063 tuples: baseline/re-evaluation medians were 4,733.6/101.4ms (W=1), 2,530.5/168.7ms (W=8), and 2,425.9/161.6ms (W=16). These are five-trial medians from the machine-verifiable current matrix.

DOOP uses the pinned 35-file extracted manifest (SHA-256 `215ddcc50bca70c0089e0ced9298274aec4edb6741736d03cc3c4386b96f831c`; declared archive SHA-256 `154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7`). It consumed up to about 54 GiB RSS in this campaign and caused substantial swap pressure; the DOOP rows are authoritative completed runs but should be treated as descriptive. The historical campaign produced different outputs (CRDT 2,156,530 and DOOP 6,069,774 tuples) because commits #955/#956/#957 changed benchmark semantics and duplicate handling. Those historical values are preserved for attribution and are not claimed as equivalent-workload timing baselines.

Pre-fix CRDT diagnostics are excluded from this table; the complete current-main matrix above is the README calibration.

Reproduce the matrix with the reviewed runner and pinned data manifest:

```bash
meson setup build-portfolio --buildtype=release -Doptimization=s -Db_lto=true -Dwirelog_log_max_level=error
meson compile -C build-portfolio -j4 bench/bench_flowlog
python scripts/perf/run-flowlog-portfolio.py --repo-root "$PWD" \
  --bench build-portfolio/bench/bench_flowlog \
  --out-dir benchmark-output --workers 1,8,16 --repeat 5 \
  --workload tc --workload reach --workload cc --workload sssp \
  --workload sg --workload bipartite --workload andersen --workload dyck \
  --workload cspa-fast --workload cspa --workload csda --workload galen \
  --workload polonius --workload ddisasm --workload crdt
```

Run the DOOP rows separately with the same setup after verifying the pinned data manifest:

```bash
python scripts/perf/run-flowlog-portfolio.py --repo-root "$PWD" \
  --bench build-portfolio/bench/bench_flowlog \
  --out-dir benchmark-doop --workers 1,8,16 --workload doop --repeat 1
```

The runner records source/tree cleanliness, binary and data hashes, commands, JSON/TSV output, host samples, and a durable manifest. The perf suite remains separately gated; no performance threshold or baseline was raised.

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
| `08-delta-queries` | Delta callbacks with `wirelog_easy` |
| `09-retraction-basics` | Fact retraction with `-1` deltas |
| `10-recursive-under-update` | Transitive closure under insert/remove |
| `11-time-evolution` | Per-epoch delta isolation |
| `12-snapshot-vs-delta` | Snapshot vs streaming API comparison |
| `13-daemon-style` | Long-running daemon rotation pattern |
| `14-arithmetic-operations` | Arithmetic expressions and aggregate functions |

## Build & Test

```bash
meson setup build
meson compile -C build
meson test -C build --print-errorlogs    # 284 tests

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
- [docs/THREADING.md](docs/THREADING.md) -- threading backends, atomics audit, K-fusion / TDD concurrency contracts
- [docs/TDD_MEMORY_BASELINE.md](docs/TDD_MEMORY_BASELINE.md) -- TDD memory-report semantics, coverage, and baseline evidence limits
- [docs/CRASH_RESTART.md](docs/CRASH_RESTART.md) -- crash/restart durability responsibilities for embedded hosts
- [docs/EMBEDDED.md](docs/EMBEDDED.md) -- embedded integration posture, build options, and host responsibilities
- [docs/INTERNALS.md](docs/INTERNALS.md) -- maintainer map of internal subsystems and public/private boundaries
- [docs/ERROR_MODEL.md](docs/ERROR_MODEL.md) -- error reporting, logging safety, fork/signal constraints, and restart handoff
- [docs/MEMORY.md](docs/MEMORY.md) -- memory ledger subsystems, `WL_MEM_REPORT`, measurement overhead, and DOOP/fixture baselines
- [docs/CI_TELEMETRY.md](docs/CI_TELEMETRY.md) -- PR CI queue delay, per-phase attribution, critical-path measurement, and the measured baseline
- [CONTRIBUTING.md](CONTRIBUTING.md) -- development workflow, CI/CD, PR requirements
- [SECURITY.md](SECURITY.md) -- vulnerability disclosure
- [docs/SIGNING.md](docs/SIGNING.md) -- verifying a release: checksums, Sigstore signatures, and provenance
- [docs/SUPPORT_POLICY.md](docs/SUPPORT_POLICY.md) -- supported-version lifecycle and 1.x support window
- [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md) -- threat model, mbedTLS license stack, and export-control self-classification
- [CLA.md](CLA.md) -- Contributor License Agreement (required for dual licensing)
- API: [`wirelog/wirelog-easy.h`](wirelog/wirelog-easy.h) (simple) | [`wirelog/wirelog-advanced.h`](wirelog/wirelog-advanced.h) (advanced)

### Supported versions

| Version line | Status |
|---|---|
| Pre-1.0 `main` | Security fixes on `main` during development |
| Maintained `1.x` | Supported through the lifecycle in [`docs/SUPPORT_POLICY.md`](docs/SUPPORT_POLICY.md) |
| Older `1.x` after EOL | Upgrade recommended; no routine backport promise |
| 0.x tags and unsupported branches | Not supported |

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
