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

## Paired campaign shadow diagnosis

`Paired Campaign V1 Shadow` collects descriptive CRDT and CSPA evidence on the
shared `wirelog-perf` runner. Dispatch only on protected `main` with
`campaign_mode=comparison` and a full lowercase 40-character `base_sha` from
trusted main ancestry, different from current main. Candidate is always the
freshly fetched main tip. Both the checkout and dispatch SHA must equal that
tip, so an intervening main update rejects the queued run. Both measured SHAs
must be at or after #2029 revision
`c6e263d492828d1208fa11005c18d19b05342233`, ancestors of current main, and
contain `WIRELOG_CRDT_PROBE`. This compatible CRDT timer is
`crdt_perf_gate_single_run`; historical `bench_flowlog` CRDT timing is not a
campaign-v1 baseline.

Dispatch `campaign_mode=aa_control` with an empty `base_sha` for two independent
builds of current main. The weekly Monday schedule selects this same A/A mode.
It measures runner/control stability and cannot establish candidate speedup
or regression. Campaigns share serialized concurrency; other tagged jobs can
still cause contention. The runner job excludes other refs and repositories
before allocation. There is no PR execution, secrets, performance threshold,
or required-check status.

The workflow uses separate worktrees and builds with GCC and the release/TRACE
profile. Both sides compile all three binaries required by preflight hashing;
the v1 collector executes only `bench_flowlog` for CSPA and
`test_crdt_perf_gate` for CRDT. It selects and records an available affinity
CPU, runs fixed AB then BA blocks with two warmups and nine adjacent pairs per
workload/block (80 serialized launches), and evaluates evidence offline.
See [campaign v1](../scripts/perf/paired-campaign-v1.md) for the exact contract.

The `perf-paired-v1-<run>-<attempt>` artifact retains the entire evidence root
for 35 days: requested and trusted-revision provenance, affinity, separate
configure/build logs and complete side logs, plus `campaign/preflight.json`,
copied logs, `campaign/raw-attempts.jsonl`, atomic `campaign/campaign-v1.json`,
`campaign/evaluation-report.json`, and atomic `campaign/collection-status.json`
when produced. Failed and interrupted runs upload partial evidence. Statuses
are `COMPLETE_VALID`, `CORRECTNESS_FAILURE`, `INVALID_EVIDENCE`, or `INCOMPLETE`;
invalid collection can fail the diagnostic job without judging timing.
Upload and summary precede always-run guarded worktree/build cleanup.
The 360-minute budget allows 80 launch timeouts of 180 seconds plus overhead.
All timing statistics are descriptive and do not change nightly or required
perf gates. Integration dispatch waits for the #2035 collector merge and
post-merge validation; a protected-main A/A smoke run verifies hosted behavior.

### Legacy 2026 diagnostic evidence

The historical diagnostic used `first=base` and `first=candidate`, nine samples
per side, and `perf-paired-<run>-<attempt>` artifacts with
`samples/metadata.json`, `samples/attempts.jsonl`, `samples/summary.json`,
preflight and correctness logs. These are legacy evidence, not campaign-v1
output. The historical eligibility requirements below apply to those runs.

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
