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
After the workflow reaches the default branch, dispatch it on the candidate
branch with a full 40-character `base_sha` from `main`. The candidate is the
dispatch ref's exact SHA. Run one campaign with `first=base` and another with
`first=candidate`; wait for each run to finish before starting the next.
During the #2022 pre-merge campaign, a temporary push trigger on the trusted
`codex/2022-relative-perf` branch uses baseline
`8d91c2da2b188b94af5b9f1da21c569d80ccb387`: the first attempt starts
with the baseline, and rerunning the same workflow reverses the first side.
Remove this temporary trigger after the evidence is collected. The workflow
serializes its own campaigns on the
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
