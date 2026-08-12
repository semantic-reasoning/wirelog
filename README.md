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
| Graph | TC (Transitive Closure) | 6.3ms | 8.0ms | 8.0ms | 4,950 | 98 | 3.0MB / 3.3MB / 3.3MB |
| Graph | Reach | 0.6ms | 0.6ms | 0.4ms | 100 | 98 | 2.8MB / 2.9MB / 2.9MB |
| Graph | CC (Connected Components) | 7.7ms | 7.4ms | 9.9ms | 100 | 99 | 3.0MB / 3.2MB / 3.4MB |
| Graph | SSSP (Shortest Path) | 0.8ms | 0.8ms | 0.8ms | 100 | 98 | 2.9MB / 2.9MB / 2.9MB |
| Graph | SG (Subgraph) | 0.4ms | 0.5ms | 0.5ms | 0 | 0 | 2.9MB / 2.9MB / 2.9MB |
| Graph | Bipartite | 1.0ms | 1.0ms | 1.0ms | 100 | 73 | 3.0MB / 3.0MB / 2.9MB |
| Pointer Analysis | Andersen | 2.2ms | 3.8ms | 3.3ms | 155 | 8 | 3.1MB / 3.5MB / 3.6MB |
| Pointer Analysis | Dyck-2 | 11.9ms | 13.7ms | 13.4ms | 2,120 | 8 | 3.5MB / 7.2MB / 7.0MB |
| Pointer Analysis | CSPA | 1.59s | 1.41s | 1.43s | 20,381 | 6 | 330.4MB / 442.4MB / 419.1MB |
| Data Flow | CSDA | 2.8ms | 2.8ms | 2.4ms | 2,986 | 29 | 3.2MB / 3.2MB / 3.1MB |
| Ontology | Galen | 23.1ms | 29.2ms | 32.1ms | 5,568 | 23 | 4.2MB / 8.6MB / 8.1MB |
| Borrow Check | Polonius | 4.7ms | 3.4ms | 4.6ms | 1,983 | 23 / 25 / 25 | 3.5MB / 3.5MB / 3.5MB |
| Disassembly | DDISASM | 2.9ms | 3.4ms | 3.4ms | 704 | 0 / 19 / 19 | 3.5MB / 3.6MB / 3.7MB |
| CRDT | CRDT | 24.95s | 24.90s | 24.06s | 2,152,328 | 14,148 | 113.6MB / 377.7MB / 451.1MB |
| Program Analysis | DOOP (zxing) | 1399.2s | 1169.2s | 1134.7s | 13,828,835 [^doop] | 153 | 39.1GB / 40.9GB / 41.0GB |

[^doop]: DOOP is the one row measured with `--repeat 1`, not a 5-trial
    median: at 23 minutes a run, three widths at `--repeat 5` is six hours.
    All three widths complete and agree: 13,828,835 tuples and 153
    iterations at W=1, W=8 and W=16 alike. Until #959 they did not -- W=8
    and W=16 died on a join-output cap of 74,026,332. The earlier revision
    of this footnote attributed that to the session cap being divided by the
    worker count; that was wrong. The divisor was the K-fusion *branch*
    count (19 for this workload), applied to the whole session budget, so
    the cap shrank as the fan grew rather than as the width grew. W>1
    depends on the branch dispatch having a work queue, which is why W=1 was
    never affected.

    Parallel scaling is weak -- 1.20x at W=8 and 1.23x at W=16 -- and
    nothing here claims otherwise; #959 removed a hard failure, not a
    bottleneck. The W>1 tuple counts recorded two revisions ago came from
    runs that varied between invocations (#958); the agreement across widths
    above is the check that this is no longer true.

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

**Tuple counts fell for four workloads in 0.54.0, and the new numbers are
the correct ones.** DDISASM 900 -> 704, Polonius 1,999 -> 1,983, CRDT
2,156,530 -> 2,152,328, DOOP 14,096,448 -> 13,828,835. Iteration counts are
unchanged in every case.

The cause is `059e410` (#957): a relation defined by a single rule was not
deduplicated, so a head that projected away a body variable derived one row
per derivation rather than one per distinct tuple. Datalog is set-valued,
so the earlier totals were counting duplicates. Confirmed by building both
sides of that commit on this host: `059e410^` reproduces the previous
table's DDISASM 900, Polonius 1,999 and CRDT 2,156,530 exactly, and
`059e410` reproduces the three values above exactly.

DOOP was not bisected the same way -- 23 minutes a run makes a two-sided
build expensive -- so its attribution to #957 is by consistency with the
other three rather than by direct measurement. Note that the previous
footnote claimed 14,096,448 was already a post-#957 figure; that claim does
not survive re-measurement and was wrong.

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
| today's engine, current archive | 13,828,835 | 153 |

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
