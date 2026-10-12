# Retained strict perf evidence

This directory preserves the five consecutive scheduled acquisitions after the
October 7 strict smoke observation. Each run retains a byte-identical subset of
its uploaded artifact, including eligible target misses. `manifest.json` records
artifact URLs and SHA-256 hashes of every retained file. Broader TRACE, portfolio
and DOOP coverage remains in the complete upstream artifacts.

These are independent latest-main acquisitions, not controlled same-source
comparisons. The governing decision and limitations are in
[the calibration contract](../../PERF_NIGHTLY_CALIBRATION.md).

| Run / UTC date | Source / tree | CRDT median / CoV | CSPA median / CoV | Strict verdict |
| --- | --- | --- | --- | --- |
| [37608652791](https://github.com/semantic-reasoning/wirelog/actions/runs/37608652791/artifacts/11477610026) / 2026-10-07 | `d648c35e53a8f167ae23a3b78f0a84f612e79b4d` / `89a7b6de921074a46d81400ffe36afe368d2450d` | 23677.9 ms / 0.223% | 2087.4 ms / 0.290% | fail |
| [37767055232](https://github.com/semantic-reasoning/wirelog/actions/runs/37767055232/artifacts/11547180997) / 2026-10-08 | `1fa011f2ba20b4cebba76d1159fe723f7ddfee10` / `d83082b517343671e311b55ffe2b1e1cab8fa2b3` | 23293.7 ms / 0.180% | 2072.2 ms / 0.323% | fail |
| [37920791985](https://github.com/semantic-reasoning/wirelog/actions/runs/37920791985/artifacts/11611918729) / 2026-10-09 | `280252e7ba6877a344311c28a6862e69200e7872` / `d99cc764d9bfd3937662b2aa2b067906c0d133ab` | 23718.2 ms / 0.205% | 2091.6 ms / 0.405% | fail |
| [38043972940](https://github.com/semantic-reasoning/wirelog/actions/runs/38043972940/artifacts/11667147046) / 2026-10-10 | `b975b8f8292e319b6e32589005c91ff2b86fec12` / `3d1b19bea14472dade18d01a1c61a91df699afbc` | 23340.0 ms / 0.156% | 1990.0 ms / 0.148% | pass |
| [38131710534](https://github.com/semantic-reasoning/wirelog/actions/runs/38131710534/artifacts/11699642268) / 2026-10-11 | `e08eca8c27a120acf97940098fd296d84146694e` / `2b51cf0bb41007d013be5672e3b1465d5c988aa5` | 22651.4 ms / 0.219% | 1941.9 ms / 0.136% | pass |

All five acquisitions are complete and telemetry-eligible. Both gates enforce
nine W=1 samples and the existing correctness sentinels. Exact sample order,
commands, correctness, and reported CoV are in each `raw/<run>/testlog.txt`.
Resolved build options and compiler identities are in `meson-config.txt` and
`compilers.json`; `configure.log` preserves configuration and `sha256.txt`
preserves captured executable/build-options/input identities. `host.txt` records
source/tree, runner, CPU and kernel. `gate-before.txt` and `gate-after.txt` record
governor, frequency, load, CPU pressure, cpuset and cgroup counters. The wrapper
pins execution to CPU 0; capture-parent affinity is a separate observation.

Population CoV recomputed from published one-decimal samples:

| Run | CRDT | CSPA |
| --- | ---: | ---: |
| 37608652791 | 0.222774% | 0.289092% |
| 37767055232 | 0.179684% | 0.322299% |
| 37920791985 | 0.205280% | 0.404032% |
| 38043972940 | 0.156450% | 0.148439% |
| 38131710534 | 0.218790% | 0.136975% |

The rounded samples and the internally computed full-precision statistics can
differ by roughly 0.001 percentage points. All recomputed medians match the logs.
These acquisitions validate the provisioned profile without deriving a new
threshold. Their differing sources and kernels prevent assigning timing changes
to source alone. Strict passes do not imply the complete nightly workflow passed.
