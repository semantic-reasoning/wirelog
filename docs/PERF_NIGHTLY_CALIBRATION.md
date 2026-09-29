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
