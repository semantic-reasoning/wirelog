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

## Features

- **Incremental evaluation** -- timely-differential dataflow evaluation propagates only new facts, not full re-derivation
- **Columnar backend** -- [nanoarrow](https://github.com/apache/arrow-nanoarrow) (minimal Apache Arrow C implementation) memory layout for cache-efficient execution
- **SIMD acceleration** -- AVX2 (x86-64) and ARM NEON (ARM64) for hash, filter, and join operations
- **Optimizer pipeline** -- Logic Fusion, Join-Project Planning, Semijoin Information Passing, Magic Sets
- **Memory backpressure** -- thread-safe ledger with JOIN budget enforcement and graceful truncation
- **Pure C11** -- no runtime, no GC; strict AddressSanitizer + UndefinedBehaviorSanitizer validation

## Performance

15-workload static benchmark portfolio plus CSPA incremental check
(2026-08-04, `main` at `7e498e7`, release/LTO build, GCC 16.1.1,
`--repeat 5` medians):

**Test environment**: Intel Xeon E5-2696 v4 (2 sockets, 44C/88T), Linux 7.1.5
(Arch), 88 logical CPUs across two NUMA nodes, 125 GB RAM. Measurements were
collected from `./build-readme-bench/bench/bench_flowlog`; wall-clock results
vary with CPU governor, thermal state, and memory pressure.

Each workload was run in its own process. Peak RSS is a process-wide
high-water mark, so a single `--workload all` invocation reports the largest
workload's footprint for every workload that follows it -- the reproduction
block below runs them separately for the same reason.

| Category | Workload | W=1 median | W=8 median | W=16 median | Tuples | Iterations | Peak RSS (W=1 / W=8 / W=16) |
|----------|----------|------------|------------|-------------|--------|------------|-------------------------------|
| Graph | TC (Transitive Closure) | 6.1ms | 6.0ms | 7.6ms | 4,950 | 98 | 3.2MB / 3.5MB / 3.4MB |
| Graph | Reach | 0.6ms | 0.6ms | 0.4ms | 100 | 98 | 2.9MB / 2.9MB / 2.9MB |
| Graph | CC (Connected Components) | 8.7ms | 8.4ms | 12.7ms | 100 | 99 | 3.2MB / 3.1MB / 3.2MB |
| Graph | SSSP (Shortest Path) | 0.8ms | 0.6ms | 0.8ms | 100 | 98 | 2.8MB / 2.8MB / 2.8MB |
| Graph | SG (Subgraph) | 0.5ms | 0.4ms | 0.5ms | 0 | 0 | 2.8MB / 2.9MB / 2.8MB |
| Graph | Bipartite | 0.9ms | 0.7ms | 0.8ms | 100 | 73 | 3.0MB / 2.9MB / 2.9MB |
| Pointer Analysis | Andersen | 2.4ms | 3.1ms | 3.2ms | 155 | 8 | 3.1MB / 3.4MB / 3.5MB |
| Pointer Analysis | Dyck-2 | 10.2ms | 10.3ms | 9.5ms | 2,120 | 8 | 3.5MB / 7.2MB / 7.3MB |
| Pointer Analysis | CSPA | 1.32s | 716.9ms | 695.9ms | 20,381 | 6 | 325MB / 459MB / 453MB |
| Data Flow | CSDA | 2.7ms | 2.3ms | 2.3ms | 2,986 | 29 | 3.2MB / 3.4MB / 3.3MB |
| Ontology | Galen | 21.1ms | 31.0ms | 33.9ms | 5,568 | 23 | 4.2MB / 8.8MB / 9.0MB |
| Borrow Check | Polonius | 4.4ms | 3.8ms | 3.6ms | 1,999 | 23 / 25 / 25 | 3.5MB / 3.4MB / 3.5MB |
| Disassembly | DDISASM | 2.4ms | 3.8ms | 3.3ms | 900 | 0 / 19 / 19 | 3.5MB / 3.7MB / 3.7MB |
| CRDT | CRDT | 23.36s | 24.01s | 23.65s | 2,156,530 | 14,148 | 108MB / 345MB / 462MB |
| Program Analysis | DOOP (zxing) | 1368.4s | FAIL | FAIL | 14,096,448 [^doop] | 153 | 39.1GB / -- / -- |

[^doop]: DOOP is the one row measured with `--repeat 1`, not a 5-trial
    median: at 23 minutes a run, three widths at `--repeat 5` is six hours.
    **W=8 and W=16 do not complete** -- they hit the per-worker join-output
    cap, which is the session cap divided by the worker count (#959), so
    parallelism currently lowers the capacity of the run. W=1 is
    deterministic; the W>1 tuple counts recorded in an earlier revision of
    this table came from runs that varied between invocations (#958).
    Older, pre-#957 measurements counted duplicate rows rather than distinct
    tuples. The 14,096,448 total shown here is from the post-#957 set-semantic
    evaluator; no separate distinct-tuple benchmark number is claimed here.

Numbers are 5-trial medians (`--repeat 5`) on a single dev host with
cpufreq governor `schedutil`; treat them as descriptive, not gated.
The `meson test --suite perf` regression gate is separate and runs
under `-Dwirelog_log_max_level=error` plus a `performance` governor
(see `tests/test_crdt_perf_gate.c`).

The CSPA table row is the static `cspa-fast` workload. The separate
incremental CSPA check exercises `--workload cspa`, which runs *only* the
incremental variant -- the two are different entry points, not two
reports from one run.

Historic single-trial numbers from pre-2026-05-09 README revisions
(before commit `1e6af00`) used `--repeat 1` and are not directly
comparable to the current 5-trial medians. Wall-clock deltas between
successive refreshes on this host are descriptive and should not be read
as isolated algorithmic speedups or regressions.

**Output changed for four workloads since the 2026-05-24 revision.** These
are result-set changes, not timing noise, and they have separate causes:

- **CRDT** (1,301,914 -> 2,156,530 tuples), **DDISASM** (531 -> 900), and
  **Polonius** (1,807 -> 1,999) changed under `61e2530` (#914), which reset
  the iteration context for non-recursive strata; the previous table was
  never re-measured afterwards. Confirmed by building both sides of that
  commit: at W=1, `61e2530^` reproduces the previous table's three values
  exactly and `61e2530` reproduces the current ones exactly. CRDT's
  iteration count moves with it, 0 -> 14,148.
- **DOOP** changed under #950/#951 (reading the archive's string-valued
  `.facts` as shipped, and deriving two relations it does not ship), #956
  (`Method_Descriptor` was projecting the parameter list instead of
  `returnType(params)`), and above all **#955**, a plan-generation defect
  that made the evaluator drop most of its own derivations. The total is
  not comparable to the previous row in any tuple-level sense; see below.

**The DOOP row describes a specific artifact**: the FlowLog zxing archive
with sha256
`154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7`
(35 `.facts` files, ~740 MB unpacked), fetched by
`bench/data/doop/download.sh`. **It needs roughly 40 GB of RAM and takes
about 23 minutes** at W=1 -- the 2026-05-24 row's 12 GB and 33 s do not
describe what running this costs. That row was measured before #955, when
the evaluator was dropping most of its own derivations: `VarPointsTo`
measured 5,266 against a true value near 4.1 million. The old speed was
under-derivation, not performance.

**The difference from the 2026-05-24 row is accounted for.** Upstream
replaced the archive at that URL on 2026-05-24, 51 minutes after the old
row was measured, with a regenerated Soot fact dump rather than a
re-encoding. The HuggingFace mirror is git-backed, so the original is still
served at revision `e9d2e0e` (117,156,975 bytes, sha256 `dcd842b8...`) --
which is what the "~112 MB" in `download.sh` had been describing all along.
Building the benchmark at `70f4d84` and running it against that recovered
archive reproduces the old row exactly: **6,276,657 tuples, 28 iterations,
11.8 GB**.

Running today's engine against both archives separates the two causes:

| | tuples | iterations |
|---|---|---|
| `70f4d84` engine, old archive (the old row) | 6,276,657 | 28 |
| today's engine, old archive | 14,470,301 | 160 |
| today's engine, current archive | 14,096,448 | 153 |

**The engine change dominates.** #955 -- a plan-generation defect that
shifted join keys past a semijoin and silently resolved them to column 0 --
had the evaluator dropping most of its own derivations, so the old row was
not a smaller-but-correct answer, it was a different and wrong one. No
tuple-level decomposition against it is meaningful.

The archive change is the smaller term and still real: upstream replaced
the artifact 51 minutes after the old row was measured, with a regenerated
Soot fact dump rather than a re-encoding. The dominant single input change
is `AssignLocal`, 306,227 -> 144,124 rows: it is the main source of
`Assign`, and `VarPointsTo` closure over `Assign` dominates the total. 32
of the 34 shared relations changed; only `MainClass` and `ApplicationClass`
did not. See #952.

The recovered archive also ships full-DOOP reference outputs, which is the
first external check this benchmark has had. Post-#955, on the current
archive, against the reference (a different dataset, so exact agreement is
not expected): `Reachable` 6,127 vs 6,167, `ArrayIndexPointsTo` 32,859 vs
33,018, `InstanceFieldPointsTo` 3,792,166 vs 3,837,529, `VarPointsTo`
4,121,488 vs 4,455,314. Before #955 those measured 498, 0, and 5,266.

**Incremental evaluation** (CSPA, delta-seeded): W=1 baseline 1.31s
-> incremental re-eval 15.3ms (**86.0x faster**); W=8 baseline 691.3ms
-> incremental re-eval 27.5ms (**25.1x faster**); W=16 baseline 673.9ms
-> incremental re-eval 27.1ms (**24.9x faster**). Each run inserted one
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
for w in 1 8 16; do
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
| `08-delta-queries` | Delta callbacks with `wirelog_easy` |
| `09-retraction-basics` | Fact retraction with `-1` deltas |
| `10-recursive-under-update` | Transitive closure under insert/remove |
| `11-time-evolution` | Per-epoch delta isolation |
| `12-snapshot-vs-delta` | Snapshot vs streaming API comparison |
| `13-daemon-style` | Long-running daemon rotation pattern |

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
- [docs/CRASH_RESTART.md](docs/CRASH_RESTART.md) -- crash/restart durability responsibilities for embedded hosts
- [docs/EMBEDDED.md](docs/EMBEDDED.md) -- embedded integration posture, build options, and host responsibilities
- [docs/INTERNALS.md](docs/INTERNALS.md) -- maintainer map of internal subsystems and public/private boundaries
- [docs/ERROR_MODEL.md](docs/ERROR_MODEL.md) -- error reporting, logging safety, fork/signal constraints, and restart handoff
- [CONTRIBUTING.md](CONTRIBUTING.md) -- development workflow, CI/CD, PR requirements
- [SECURITY.md](SECURITY.md) -- vulnerability disclosure
- [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md) -- threat model, mbedTLS license stack, and export-control self-classification
- [CLA.md](CLA.md) -- Contributor License Agreement (required for dual licensing)
- API: [`wirelog/wirelog-easy.h`](wirelog/wirelog-easy.h) (simple) | [`wirelog/wirelog-advanced.h`](wirelog/wirelog-advanced.h) (advanced)

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
