# Issue #2022: paired tagged-runner diagnostic, 2026-09-29

The [paired run, attempts 1 and 2](https://github.com/semantic-reasoning/wirelog/actions/runs/36575995318)
compared base `8d91c2da2b188b94af5b9f1da21c569d80ccb387` with candidate
`2b0ea083c124a8721c86c922af52be33aafbaf83`. Attempt 1 led with the
base; attempt 2 reran the same workflow SHA and led with the candidate. The
artifacts `perf-paired-36575995318-1` and `perf-paired-36575995318-2` retain
every warmup and trial, host snapshots, correctness log, build log, hash, and
resolved profile. Both jobs succeeded on different members of the tagged
`wirelog-perf` runner class: `semantic-reasoning-i401-6` and
`semantic-reasoning-i401-6-3`. A temporary branch-limited `push` trigger
enabled this pre-merge campaign and was removed after evidence collection.

Both preflights reported `comparable`. The benchmark source and all four input
files matched between revisions and attempts. The compiler was GCC 13.3.0
(Ubuntu 13.3.0-6ubuntu2~24.04.1); both builds used release, `-Os`, LTO,
TRACE log ceiling, native threads, tests enabled, and mbedTLS disabled. Both
runners reported Linux `7.2.4-arch1-2`, Intel Xeon E5-2696 v4, 44 logical CPUs,
CPU 0 affinity, and `schedutil`. The full source trees, compiler records,
effective Meson options, fixture SHA-256 hashes, and binary SHA-256 hashes are
in each `preflight.json` and `samples/metadata.json`. The CSPA correctness-gate
source differs between revisions because the candidate emits raw trial output;
the benchmark source and inputs are identical. Both revisions' separate full
correctness checks returned CRDT 104,851 results/14,148 iterations and CSPA
20,381 tuples/6 iterations.

| Workload | Attempt | Base median / CoV | Candidate median / CoV | Candidate ÷ base |
| --- | ---: | ---: | ---: | ---: |
| CRDT W=1 | 1 | 37,703.6 ms / 0.086% | 37,820.3 ms / 0.132% | 1.00310 |
| CRDT W=1 | 2 | 37,665.1 ms / 0.108% | 37,786.4 ms / 0.099% | 1.00322 |
| CSPA W=1 | 1 | 4,039.1 ms / 0.220% | 4,177.7 ms / 0.294% | 1.03431 |
| CSPA W=1 | 2 | 4,034.8 ms / 0.102% | 4,163.7 ms / 0.256% | 1.03195 |

Nine raw trial times in milliseconds per row, in execution order within that
side, are preserved in the artifacts and transcribed here for review:

| Attempt | Workload | Side | Trial times (ms) |
| --- | --- | --- | --- |
| 1 | CRDT | base | 37650.7, 37715.9, 37689.1, 37689.1, 37661.8, 37728.1, 37726.6, 37703.6, 37760.0 |
| 1 | CRDT | candidate | 37762.7, 37857.2, 37740.0, 37735.0, 37820.3, 37770.8, 37828.6, 37865.5, 37859.8 |
| 1 | CSPA | base | 4039.1, 4050.8, 4030.4, 4057.7, 4031.2, 4033.0, 4040.7, 4035.1, 4046.4 |
| 1 | CSPA | candidate | 4192.7, 4184.8, 4184.6, 4161.6, 4176.2, 4171.2, 4177.7, 4154.1, 4191.0 |
| 2 | CRDT | base | 37755.7, 37719.2, 37644.2, 37632.6, 37699.0, 37697.1, 37637.7, 37642.8, 37665.1 |
| 2 | CRDT | candidate | 37833.5, 37817.1, 37806.3, 37775.9, 37786.4, 37794.1, 37743.8, 37707.1, 37753.0 |
| 2 | CSPA | base | 4035.3, 4040.2, 4029.8, 4028.7, 4028.3, 4036.5, 4031.8, 4034.8, 4038.7 |
| 2 | CSPA | candidate | 4171.1, 4171.4, 4143.0, 4159.4, 4182.5, 4153.8, 4163.7, 4166.6, 4163.6 |

Each attempt contains 40 successful parsed executions: one warmup and nine
samples for each side/workload. Under the [predeclared diagnostic eligibility
rule](https://github.com/semantic-reasoning/wirelog/issues/2022#issuecomment-5891544673),
both attempts are eligible. The maximum before/after one-minute load was 1.59
and 1.39 against 44 logical CPUs; maximum CPU PSI `some avg10` was 0.00% in
both; cgroup throttling increased by zero. All eight nine-sample CoVs were
below 3%. Between attempts, the largest same-side median change was 0.34%,
and the largest candidate/base ratio change was 0.23%, below the predeclared
5% stability limit.

The candidate's CSPA median was 3.2–3.4% higher on both runner instances;
CRDT was about 0.3% higher. This is a reproducible *paired observation*, not
proof that memory admission caused the CSPA difference: many commits separate
the revisions. The `bench_flowlog` CRDT timer is also not the existing
`test_crdt_perf_gate` timer, so its 37.8-second median cannot be compared
directly with that gate's 38,120 ms target. Neither the existing absolute
targets nor their CoV checks were changed. A timing contract and any #1913
acceptance revision still require separate review and independent confirmation
before either issue can close.

As a kernel-independent profiling fallback, both revisions were separately
built locally with `-pg` at compile and link time, `-Ddebug=true`, and LTO
disabled. CSPA returned 20,381 tuples/6 iterations in each build and GNU
`gprof` produced function-level call graphs. This verifies the documented
procedure in [PERF_NIGHTLY_CALIBRATION.md](PERF_NIGHTLY_CALIBRATION.md), but
instrumented timings are not gate or speedup evidence; no hotspot owner is
assigned from them.
