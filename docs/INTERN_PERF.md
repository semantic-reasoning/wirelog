# Intern Write-Path Performance

**Last Updated:** 2026-09-10
**Baseline SHA:** `4e4690cc` (main at the time of measurement)
**Issue:** #1472 (benchmark and baseline); measures the write path from #958,
#961 and #1431

This document records the reference baseline for `wl_intern_put()`, the
writer side of the shared symbol table. It exists so that a change to the
scope of `intern->lock`, to the speculative copy prepared outside it (#961),
or to the memory-governor admission performed inside it (#1431) has a number
to compare against. **No CI gate reads these numbers.** They are a reference
for a manual before/after run, not a threshold.

## What is measured

`bench_intern` (source: `bench/bench_intern.c`) drives `wl_intern_put()` on
a fresh table per scenario:

| Mode     | What every put does                                                                 |
|----------|-------------------------------------------------------------------------------------|
| `unique` | inserts a new string: speculative copy, writer lock, lookup, governor reservation (governed runs), segment or slot growth when due, publish |
| `dup`    | hits one of 1024 pre-interned strings: speculative copy, writer lock, lookup, free |

Each mode runs with 1, 4 and 8 writer threads and with the governor off or
attached in enforcing mode with a 1 GiB budget that never denies. `--iters`
is the total number of puts per scenario and is split evenly across the
writers, so every scenario walks the same table growth schedule and the
writer axis measures lock contention only. Writers start together behind a
condition-variable barrier; each writer records its own start and end time.

Per scenario the bench reports:

- `agg_ns_put`: wall time from the first writer start to the last writer end,
  divided by the total number of puts. This is the throughput-derived cost
  and the number to compare across lock-scope changes.
- `ns_put`: the mean of the per-put latencies measured around each call. It
  includes the time a writer waits for the lock and the cost of the two
  clock reads that bracket the call (`clock_ns`, printed once per run), so it
  grows with the writer count even when throughput does not.
- `p50` and `p99` of the per-put latencies, in nanoseconds.
- `grow_events` and `grow_share`: the number of puts whose latency exceeds
  `grow_thr_ns = 16 x p50`, and the share of the summed per-put latency they
  account for (`sum(lat > thr) / sum(lat)`). With one writer these outliers
  are the slot-array resizes, the id-segment opens and allocator page faults;
  with several writers lock waits dominate the tail, so the fields are
  printed only for `writers=1`. The threshold is floored at sixteen times
  the clock cost, and the fields are only meaningful when the clock
  resolution is well below p50 (true for the Linux and macOS clocks; a
  coarse Windows counter makes them noise). Compare `grow_share` across
  runs rather than `grow_events`.
- `gov_reserved_delta` (governed `dup` runs): governor bytes reserved after
  the timed loop minus before it. A duplicate put never reserves, so the
  value must be 0; the bench exits non-zero otherwise.

The bench also exits non-zero when any put returns -1 or the final table
count differs from the expected number of strings, so a denied run can never
be mistaken for a fast one. Timing values are never asserted.

## Run parameters

| Parameter          | Value                                                        |
|--------------------|--------------------------------------------------------------|
| `--iters`          | 200000 (default; total puts per scenario)                    |
| Writers            | 1, 4, 8                                                      |
| Runs per scenario  | 3; the tables show the median of the three                   |
| Build              | default configuration: `buildtype=release`, `optimization=s`, `b_lto=true` (`meson setup build`) |
| Compiler           | GCC 16.2.1                                                   |
| CPU                | Intel Xeon E5-2696 v4 @ 2.20 GHz, 88 logical cores           |
| CPU governor       | `schedutil` (not `performance`; `wl_perf_stability_env_ok` would report the host as unstable) |
| Host SHA           | `4e4690cc`                                                   |

The host was not pinned or isolated; the numbers carry run-to-run noise of
a few percent on the single-writer rows and more on the contended rows. The
bench deliberately does not call `bench_stability_prep()`: on Windows that
helper pins the process to one CPU, which would serialize the writers.

## Results: default configuration

| Mode | Writers | Governor | agg_ns_put | ns_put | p50 ns | p99 ns | grow_events | grow_share |
|------|---------|----------|-----------:|-------:|-------:|-------:|------------:|-----------:|
| unique | 1 | off | 300 | 273 | 149 | 1493 | 1377 | 31.9% |
| unique | 1 | on | 346 | 319 | 216 | 841 | 217 | 21.6% |
| unique | 4 | off | 811 | 2964 | 1420 | 17790 | n/a | n/a |
| unique | 4 | on | 1102 | 4160 | 2207 | 21066 | n/a | n/a |
| unique | 8 | off | 1036 | 8122 | 5024 | 37881 | n/a | n/a |
| unique | 8 | on | 1242 | 9718 | 5896 | 43663 | n/a | n/a |
| dup | 1 | off | 140 | 110 | 103 | 196 | 31 | 1.1% |
| dup | 1 | on | 139 | 112 | 103 | 247 | 28 | 1.1% |
| dup | 4 | off | 302 | 1145 | 326 | 10145 | n/a | n/a |
| dup | 4 | on | 318 | 1158 | 324 | 10369 | n/a | n/a |
| dup | 8 | off | 347 | 2668 | 424 | 19250 | n/a | n/a |
| dup | 8 | on | 333 | 2585 | 405 | 19308 | n/a | n/a |

Clock pair cost (median of the three runs): 25 ns per bracketed put.

## Results: release with the log ceiling at error

Same host, `meson setup build-rel --buildtype=release
-Dwirelog_log_max_level=error`. The intern write path contains no log sites,
so the two configurations agree within noise.

| Mode | Writers | Governor | agg_ns_put | ns_put | p50 ns | p99 ns | grow_events | grow_share |
|------|---------|----------|-----------:|-------:|-------:|-------:|------------:|-----------:|
| unique | 1 | off | 314 | 287 | 154 | 1673 | 1409 | 31.9% |
| unique | 1 | on | 348 | 323 | 215 | 847 | 225 | 22.6% |
| unique | 4 | off | 786 | 2902 | 1363 | 16328 | n/a | n/a |
| unique | 4 | on | 1083 | 4065 | 2127 | 20064 | n/a | n/a |
| unique | 8 | off | 1019 | 7694 | 4803 | 37583 | n/a | n/a |
| unique | 8 | on | 1460 | 11003 | 6743 | 49230 | n/a | n/a |
| dup | 1 | off | 138 | 108 | 101 | 198 | 28 | 1.1% |
| dup | 1 | on | 135 | 109 | 102 | 196 | 33 | 1.2% |
| dup | 4 | off | 316 | 1175 | 352 | 10093 | n/a | n/a |
| dup | 4 | on | 296 | 1095 | 344 | 10029 | n/a | n/a |
| dup | 8 | off | 364 | 2802 | 398 | 20595 | n/a | n/a |
| dup | 8 | on | 370 | 2830 | 408 | 20182 | n/a | n/a |

Clock pair cost (median of the three runs): 25 ns per bracketed put.

## Reading the table

- **Governor cost per unique put.** With one writer the enforcing governor
  adds about 30 to 50 ns per unique put to `agg_ns_put` (300 vs 346 in the
  first table, 314 vs 348 in the second; the range depends on scenario
  position, see below): the size arithmetic, the reserve CAS and the commit
  inside the lock. Under contention the gap widens to a few
  hundred nanoseconds per put because the reservation lengthens the locked
  region every waiter queues behind.
- **Duplicates carry no governor cost.** The governed and ungoverned `dup`
  rows agree within noise at every writer count, and `gov_reserved_delta` is
  0 on every governed `dup` run: a duplicate is recognised under the lock
  before any governor code runs and only frees its speculative copy.
- **Contention.** `agg_ns_put` for unique puts rises from about 300 ns at one
  writer to about 800 ns at four and about 1000 ns at eight: the locked region
  serialises the puts, and the extra cost is lock hand-off. `ns_put` at eight
  writers is roughly eight times `agg_ns_put`, as expected when seven writers
  wait behind one.
- **Growth share at one writer.** At 200000 unique puts the table performs
  13 slot-array resizes (capacity doubles from 64 to 524288 at the 75 percent
  load factor) and opens 12 id segments (64 << s entries each; ids below
  200000 touch segments 0 to 11). `grow_events` is far larger than 25 because
  the 16 x p50 threshold also catches allocator and page-fault outliers on
  ordinary puts; `grow_share` is the honest figure: about a third of the
  single-writer time on this host goes to those outliers, dominated by the
  last two resizes (the `max` sample, several milliseconds, is the final
  rehash). Small early resizes and segment opens stay below the threshold.
  The share is reported as a share of summed per-put latency, not of wall
  time.
- **Scenario order.** The grid runs its scenarios in one process, and the
  first scenario pays the first-touch page faults of the heap the table
  grows into (the tenth-size warm-up table does not pre-fault it). The p99
  and `grow_events` of the first row (`unique`, one writer, governor off)
  are therefore higher than the same scenario shows when it runs later in
  the grid, and a scenario run alone is itself in first position and pays
  the same cost. Tail metrics are only comparable between rows measured in
  the same position: run each side of a comparison alone (`--mode`,
  `--writers`, `--governor`) so both sit in first position. `grow_share`
  is stable across positions; `agg_ns_put` moves by under ten percent.

## Reproducing these results

```
meson setup build
meson compile -C build bench/bench_intern
build/bench/bench_intern                              # full grid, defaults
build/bench/bench_intern --mode unique --writers 8 --governor on --iters 200000
meson test -C build bench_intern_smoke                # every scenario, 2000 puts
```

Run each scenario three times and take the median. Record the build
configuration, compiler, CPU governor and SHA with the numbers. The bench
prints `error=<reason>` and exits 1 when a put is denied, the final count is
wrong or a duplicate put reserved governor bytes.
