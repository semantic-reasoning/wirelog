# Dedicated Linux perf-nightly calibration evidence

## Current execution contract

Issue [#2022](https://github.com/semantic-reasoning/wirelog/issues/2022)
uses the latest canonical `main` source on the dedicated Linux runner labels
`[self-hosted, Linux, X64, perf]`, allocated for scheduled nightly or manual
main measurements. PR checks do not request this limited runner. The former
shared-runner, TRACE, five-jobs-at-one-frozen-SHA calibration instruction is
superseded for #2022. Keep every observation, including misses, failures,
skips and partial artifacts; do not repeat until green or require old-source
comparisons. Separate paired diagnostic workflows retain their own contracts.

The current implementation is
[perf-nightly.yml](../.github/workflows/perf-nightly.yml) and
[run-perf-nightly-linux.sh](../scripts/ci/run-perf-nightly-linux.sh):

- Inspect the runner-local `semantic-reasoning:ubuntu26` image and run its
  immutable image ID with `--pull=never --network=host`.
- Use HOME temporary storage (`/home/perf/tmp` inside the container) and a
  `uv` virtual environment with Meson `1.12.0`. Cap Ninja build parallelism at
  `min(available CPUs, 8)`. Prebuild both gates before measurement, then use
  `--no-rebuild --num-processes 1`.
- Use the strict release ERROR profile. The measured resolved options below
  are `optimization=s` (`-Os`), `b_lto=true`, `b_ndebug=false`, `tests=true`
  and `mbedTLS=disabled`; retain the actual options with each observation.
- Run Meson and its benchmark children with `taskset -c 0`, one Wirelog worker,
  `WIRELOG_PERF_GATE=1` and `WIRELOG_PERF_REQUIRE=1`. Each gate records nine
  timed samples; CoV must be at most 3%. Existing median targets remain
  **38,120 ms CRDT** and **2,050 ms CSPA**.
- CRDT enforces **104,851 result tuples**. Its observed 14,148 iterations and
  aggregate count are recorded, with the aggregate diagnostic only. CSPA
  enforces **20,381 tuples and six iterations**.

Compiler versions and image IDs below are observed provenance, not newly
pinned version requirements. Measurements from different source revisions,
kernels or build environments do not establish source-caused regressions.

### Latest-main acquisition example

When another measurement is needed, dispatch the existing strict acquisition
on then-current `main`:

```sh
gh workflow run perf-nightly.yml --repo semantic-reasoning/wirelog \
  --ref main -f strict_smoke=true
```

This selects the latest main revision for that dispatch and measures only the
strict CRDT/CSPA scope. Retain the artifact and inspect the completed Linux
job and its raw statuses even if the overall workflow still shows a pending
Windows job. This example does not request an additional run for the recorded
observation below or satisfy broader nightly coverage.

## Artifact map and eligibility

The Linux artifact is named `perf-suite-linux-<run>-<attempt>` and is uploaded
on failure too. Preserve the complete artifact:

- `testlog.txt` is the strict ERROR CRDT/CSPA record: actual commands,
  correctness, nine raw samples, medians, CoV and target verdicts.
  `meson-test.stdout`, `.stderr` and `.exit` preserve runner outcomes.
- `gate-before.txt` and `gate-after.txt` bracket strict timed execution inside
  the container. They record capture-parent available affinity, effective
  cpuset, kernel/CPU, governor/frequencies, load, PSI and cgroup counters.
  Their affinity is not a measurement of the benchmark child's affinity.
- `host-before.txt` and `host.txt` are broader outer-host snapshots, with
  source commit/tree in `host.txt`; they do not replace gate telemetry.
- `image-inspect.json`, `image-identity.txt`, compiler/tool versions,
  `meson-config.txt`, `compilers.json`, configure/build logs and `sha256.txt`
  retain image, resolved options, executable and captured input identities.
- `qualification-status.txt`, `telemetry-status.txt` and
  `campaign-status.txt` keep acquisition, host eligibility and performance
  verdict separate. A complete, eligible target miss is a measured failure.
  Incomplete or ineligible data cannot calibrate a target.
- Full nightly mode separately retains `trace-testlog.txt`, TRACE build/test
  logs and portfolio evidence. `legacy-coverage-status.txt` describes that
  coverage. `strict_smoke=true` covers CRDT/CSPA only and reports
  `coverage_status=partial_smoke`; it proves no TRACE, portfolio, DOOP or
  Windows result.

[check-nightly-host-telemetry.sh](../scripts/ci/check-nightly-host-telemetry.sh)
requires both endpoint snapshots to provide the recorded fields, valid
numeric frequencies and CPU statistics, syntactically valid affinity/cpuset,
and governor `performance`. At each endpoint CPU PSI `some avg10` must be
at most **1.0%**, and one-minute load must not exceed the effective cpuset CPU
count. `throttled_usec` must be equal before and after. `nr_throttled` is
recorded and validated numeric, but its delta is not compared. The checker
does not enforce equality of the two affinities, kernels or CPU identities;
review their provenance separately. Endpoint frequency/governor/load/PSI
observations do not prove continuous stability during all nine samples.

## Latest measured observation: 2026-10-07

[Run 37577651576, attempt 1](https://github.com/semantic-reasoning/wirelog/actions/runs/37577651576)
retains [artifact perf-suite-linux-37577651576-1](https://github.com/semantic-reasoning/wirelog/actions/runs/37577651576/artifacts/11463174732).
It measured then-latest main after the #2101 acquisition repair:

- Source `e6315d1a4a4ea92ab4cad7690e06e4ced326aa7a`;
  tree `03388e153ab3b077ff1db33fad9cf64314b00994`.
- Runner `atlas-perf`, Intel Core i5-14600, Linux `7.2.8-arch1-2`, x86-64.
- Local image ID
  `sha256:c04b38dee72397a66dfe79ccc0ee5b3ef3f7c2d3181c03fdc3e358d7791da51e`;
  host networking. GCC `(Ubuntu 15.2.0-16ubuntu1) 15.2.0`, Meson `1.12.0`.
- Resolved release/ERROR/`s`/LTO/assertions-enabled options as listed above.
  Meson's raw commands recorded `MALLOC_PERTURB_=145` for CRDT and `134`
  for CSPA. These are invocation observations, not enforced constants or
  demonstrated causes of the miss.
- Gate endpoint cpuset and capture-parent affinity `0-19`; execution pinned
  separately to CPU 0 by the wrapper. Governor `performance` at both
  endpoints; current/min/max frequencies `800000/800000/5200000` kHz at both.
  PSI `some avg10` was `0.54%` before and `0.00%` after; load1 `2.59` and
  `1.12` versus 20 allowed CPUs; `throttled_usec=0` at both endpoints.

All times below are milliseconds, in original sample order:

| Gate | Nine raw samples | Median | Reported CoV | Recomputed CoV |
| --- | --- | ---: | ---: | ---: |
| CRDT | 23319.2, 23222.3, 23209.5, 23188.9, 23281.8, 23254.9, 23233.5, 23224.2, 23256.4 | 23233.5 | 0.161% | 0.160675% |
| CSPA | 2075.9, 2072.2, 2081.2, 2072.0, 2072.8, 2077.8, 2080.1, 2078.3, 2073.7 | 2075.9 | 0.159% | 0.159243% |

Recomputation uses population standard deviation divided by mean, from the
published one-decimal samples. Differences below 0.001 percentage points
from reported CoV are rounding, not evidence of a different acquisition.
CRDT warmup was 23,248.5 ms; correctness was 104,851 result tuples,
14,148 iterations and diagnostic aggregate 2,152,328. CSPA correctness was
20,381 tuples and six iterations.

The recorded status is **complete acquisition, eligible telemetry, failed
performance verdict**, with Meson exit 1 and partial smoke coverage. CRDT
passed; CSPA's **2,075.9 ms exceeds 2,050 ms by 25.9 ms (1.2634%)**.
This is an eligible target miss, not a discarded outlier or acquisition
failure. No target or CoV change is made here.

### Captured SHA-256 identities

These are the files actually hashed by this acquisition, not a claim that
every possible workload file was hashed:

```text
5c5f87c798fc52ce990b934982a01808d1dba85396a0015c5474d199e6056422  test_crdt_perf_gate
c38033ce007f4a17641cb8a00cdda4298a4e661af920d88fe71e32724c325ece  test_cspa_perf_gate
55c4d27cc258aff6e23fdde1e7bd4d0d966c81dab421edc941428e692e5766cc  intro-buildoptions.json
9147f2b44d4cb291336da9aa6c39c27feac8630361f2b36875474e5c30602b05  crdt/Insert_input.csv
b83fba0709b37259cd6ad00bfe3434abc3df3fec3dbccd3f5eedf273e9c921dc  crdt/Remove_input.csv
10ee3e1a6388d6596cf4d437336c6215062554761156b9be22767dba86bb2032  cspa/assign.csv
c37127cbcf42cebb04296869cf16b029d60992706c98377e4536d3c9e5b06185  cspa/dereference.csv
```

### Prior observations and unresolved decision

These independent main observations are retained as context, not frozen-source
comparisons or evidence assigning a hot path's ownership. Both prior runs used
Linux `7.2.7-arch1-1`, unlike the latest `7.2.8-arch1-2` observation.

| Run | Source | CRDT median / reported CoV | CSPA median / reported CoV | Strict result |
| --- | --- | --- | --- | --- |
| [37430466986](https://github.com/semantic-reasoning/wirelog/actions/runs/37430466986) | `54e0eca4e05425274c5f22f09c9694bf32680af7` | 23395.8 ms / 0.206% | 2033.4 ms / 0.341% | Eligible pass |
| [37452454644](https://github.com/semantic-reasoning/wirelog/actions/runs/37452454644) | `cad2c89828affa635ac3df274fce266501b07515` | 23273.8 ms / 0.248% | 2056.2 ms / 0.324% | Eligible CSPA target miss |
| [37575664019](https://github.com/semantic-reasoning/wirelog/actions/runs/37575664019) | Pre-repair attempt | No samples | No samples | Docker bridge/veth failure, exit 125 before measurement |

[#2101](https://github.com/semantic-reasoning/wirelog/pull/2101) repaired that
network acquisition prerequisite. The completed latest run confirms acquisition
works; it does not resolve the performance miss. Historical high-drift
shared-runner samples are excluded from deriving current limits; their drift
has not been experimentally eliminated by this observation.

**#2022 remains open.** Its provisioned-profile versus separately reviewed
measured-target decision is unresolved because the latest eligible acquisition
misses CSPA. This documentation establishes the current execution and evidence
contract; it does not claim all issue acceptance criteria passed or close the
issue. [#1913](https://github.com/semantic-reasoning/wirelog/issues/1913)
retains its throughput acceptance, and optimization candidates in
[#2098](https://github.com/semantic-reasoning/wirelog/issues/2098) remain
explicitly deferred. This change implements no optimization or relaxed gate.

## Separate paired campaign shadow diagnosis

This separate diagnostic workflow still exists; this document does not change
or disable its paired-v1 collection or eligibility rules. Those rules do not
require frozen-source comparisons for the current #2022 calibration above.

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
perf gates. The original integration plan depended on the #2035 collector merge and
post-merge validation, with a protected-main A/A smoke to verify hosted behavior.

### Legacy 2026 diagnostic evidence

The historical diagnostic used `first=base` and `first=candidate`, nine samples
per side, and `perf-paired-<run>-<attempt>` artifacts with
`samples/metadata.json`, `samples/attempts.jsonl`, `samples/summary.json`,
preflight and correctness logs. These are legacy evidence, not campaign-v1
output. The historical eligibility requirements below apply only to those runs.
Their frozen-source repeat rules are superseded for current #2022 calibration
by the latest-source dedicated-runner direction; the paired-v1 workflow retains
its own separate diagnostic contract.

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
export TMPDIR="$HOME/.tmp/wirelog-gprof"
mkdir -p "$TMPDIR"
uv venv "$TMPDIR/venv"
uv pip install --python "$TMPDIR/venv/bin/python" meson ninja
export PATH="$TMPDIR/venv/bin:$PATH"
meson setup build-gprof --buildtype=release -Dwirelog_log_max_level=trace \
  -Dtests=true -DmbedTLS=disabled -Db_lto=false -Ddebug=true \
  -Dc_args=-pg -Dc_link_args=-pg
meson compile -C build-gprof -j 8 bench_flowlog
taskset -c 0 build-gprof/bench/bench_flowlog --workload cspa-fast \
  --data-cspa bench/data/cspa --workers 1 --repeat 1
gprof build-gprof/bench/bench_flowlog gmon.out > cspa-gprof.txt
```

Run each revision in its own output directory so `gmon.out` is not overwritten,
and verify the expected 20,381 tuples and 6 iterations. For CRDT, use
`--workload crdt --data-crdt bench/data/crdt` and record the observed aggregate (2,152,328 in the later diagnostic)
and 14,148 iterations. The aggregate is diagnostic, not a new correctness
sentinel; the strict CRDT gate enforces 104,851 result tuples. We verified this path on both revisions with
CSPA; it generated call graphs with matching correctness. `-pg` changes the
build and runtime, so use its function counts and call graph only to investigate
hotspots, never as the timing gate or a measured before/after speedup. See the
[GNU gprof manual](https://sourceware.org/binutils/docs/gprof.html) for the
profiling format and limitations.
