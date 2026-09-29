# Tagged perf-nightly calibration evidence

The Linux perf-nightly job uploads `perf-suite-linux-<run>-<attempt>` even when
the perf suite fails. Its `testlog.txt` is the authoritative record of the
CRDT and CSPA gates' nine raw trials, medians, CoV, results, and correctness
output. Other perf gates also write their results to this log.

The other files establish whether runs are comparable:

- `host-before.txt` and `host.txt`: snapshots immediately before and after
  the perf suite. Both record runner identity, kernel, CPU model, boot ID,
  requested and available affinity, governor, CPU frequency, load, CPU
  pressure, and cumulative cgroup throttle counters. Their difference helps
  identify throttling during the suite. The after file also records the source
  commit and tree.
- `meson-config.txt` and `compilers.json`: the resolved build profile and
  compiler details. The workflow uses release mode with the TRACE log ceiling.
- `sha256.txt`: hashes of both gate executables, Meson's resolved build options,
  and the CRDT/CSPA input files. A missing file is recorded explicitly.

Issue #2022 calibrates the `wirelog-perf` tagged Linux runner class, not a
particular machine. Run five jobs sequentially at one immutable source commit.
Keep every run, including failures, skips, and outliers. Calibration is eligible
only if all five jobs use matching source/build/data, each gate reports the
expected correctness result and its existing CoV check passes, and each gate's
largest median divided by its smallest median is at most 1.05. Otherwise,
investigate runner stability and start a new complete campaign after the
underlying cause changes; do not discard selected runs.

## Paired regression diagnosis

The manual `Paired Perf Diagnostic` workflow gathers non-gating CRDT and CSPA
evidence when the current nightly medians and historical targets disagree.
Dispatch it on the candidate branch with a full 40-character `base_sha` from
`main`. The candidate is the dispatch ref's exact SHA. Run one campaign with
`first=base` and another with `first=candidate`; wait for each run to finish
before starting the next. The workflow serializes its own campaigns on the
shared `wirelog-perf` runner. Other tagged jobs can still create contention,
so inspect every attempt's host observations before drawing conclusions.

The workflow builds both revisions with the same resolved release/TRACE profile,
verifies benchmark source and fixture hashes, and runs each revision's full
CRDT and CSPA correctness fixture before timing. Each campaign records nine
samples per side per workload, alternating which revision runs first. Its
`perf-paired-<run>-<attempt>` artifact contains `preflight.json`, correctness
logs, build logs, `samples/metadata.json`, every warmup and trial in
`samples/attempts.jsonl`, and `samples/summary.json`. A failed campaign still
uploads the evidence. Treat missing trials, correctness failures, timeouts,
runner pressure, and profile mismatch as ineligible evidence. The diagnostic
summary has no pass/fail timing threshold and does not change nightly gates.

For the #2022 diagnostic campaign, eligibility was declared before the first
artifact was available in the [issue comment](https://github.com/semantic-reasoning/wirelog/issues/2022#issuecomment-5891544673).
Both orders at identical source SHAs must complete all nine samples per side,
full-fixture correctness, and exact source/data/compiler/build checks. Each
side's CoV must be at most 3%. Every warmup and sample must have CPU PSI
`some avg10` at most 10%, one-minute load at most the logical CPU count,
and no increase in cgroup throttling, both before and after. Across the two
attempts, each side's median and the candidate/base median ratio must have
maximum/minimum at most 1.05. These checks decide whether the observations
are usable; **1.05 is not a permitted performance regression**. Preserve all
attempts and report ineligible campaigns as inconclusive.

### Kernel-independent hotspot attribution

If `perf` has no matching binary for the tagged runner kernel, use GNU `gprof`
as a separate function-level diagnostic. Build each revision from the same
source/data with profiling enabled for both compilation and linking:

```sh
meson setup build-gprof --buildtype=release -Dwirelog_log_max_level=trace \
  -Dtests=true -DmbedTLS=disabled -Db_lto=false -Ddebug=true \
  -Dc_args=-pg -Dc_link_args=-pg
meson compile -C build-gprof bench_flowlog
taskset -c 0 build-gprof/bench/bench_flowlog --workload cspa-fast \
  --data-cspa bench/data/cspa --workers 1 --repeat 1
gprof build-gprof/bench/bench_flowlog gmon.out > cspa-gprof.txt
```

Run each revision in its own output directory so `gmon.out` is not overwritten,
and verify the expected 20,381 tuples and 6 iterations. For CRDT, use
`--workload crdt --data-crdt bench/data/crdt` and verify 2,152,328 aggregate
tuples and 14,148 iterations. We verified this path on both revisions with
CSPA; it generated call graphs with matching correctness. `-pg` changes the
build and runtime, so use its function counts and call graph only to investigate
hotspots, never as the timing gate or a measured before/after speedup. See the
[GNU gprof manual](https://sourceware.org/binutils/docs/gprof.html) for the
profiling format and limitations.
