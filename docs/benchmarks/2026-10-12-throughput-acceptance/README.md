# #1913 measured admission optimization and throughput acceptance

Current source passes both historical gates on the dedicated host chosen by
[#2022](../../PERF_NIGHTLY_CALIBRATION.md). A fresh CSPA profile nevertheless
identifies avoidable reservation initialization on every row append. This unit
moves that initialization to the governed wide-overlap branch that uses it,
with focused denial/rollback coverage and controlled before/after measurements.
Memory admission remains enabled; no target, correctness sentinel, noise
ceiling, fixture, worker count or required gate is changed.

## Source and measurement identity

The baseline is merged main `691116500ac45407b73244850641c646877fdb43`, tree
`7dcfdbf2f6604c20a9ae73191d06e1dfd50291af`. The measured candidate code tree is
`924c5e8e921344b97ee18c4f5681548d6e58e669`: that baseline plus exactly
`wirelog/columnar/relation.c` and `tests/test_memory_admission_relation.c` from
[candidate-code.patch](candidate-code.patch), SHA-256
`f8050974061b60d54914cc003681c0dad2f9d19b6fcbb95930625367f61b31f8`.

The candidate was measured before the harness allowed a commit. Both isolated
clones therefore have the baseline HEAD. Raw wrapper `host.txt` records that
HEAD tree, **not the dirty candidate's compiled tree**. Each controlled block's
`source-identity.json`, `source-dirty.txt`, `measured-source.sha256` and post-run
integrity check record the actual code. The final reviewed commit additionally
contains this evidence and documentation, so its full tree differs from the
measured two-path code tree; the measured source bytes remain unchanged.

Two serial strict GitHub Actions acquisitions independently establish current
main acceptance at this same source: [38195651787](https://github.com/semantic-reasoning/wirelog/actions/runs/38195651787)
and [38195709101](https://github.com/semantic-reasoning/wirelog/actions/runs/38195709101).
Their selected raw artifacts and upstream artifact links are retained in
[manifest.json](manifest.json). They are contextual current-main passes,
not additional candidate measurements.

## Controlled strict measurements

Before measuring, [predeclared-plan.json](predeclared-plan.json) fixed four
serial blocks: baseline, candidate, candidate, baseline. Both order pairs use
nine timed samples per workload, plus their normal warmups, on `atlas-perf`,
Intel Core i5-14600, Linux `7.2.9-arch1-1`, immutable local image
`sha256:c04b38dee72397a66dfe79ccc0ee5b3ef3f7c2d3181c03fdc3e358d7791da51e`,
GCC Ubuntu 15.2.0, Meson 1.12.0, release/ERROR, `-Os`, LTO, assertions enabled,
CPU 0, W=1, unchanged inputs and `--no-rebuild --num-processes 1`.
No diagnostic profiler or unrelated build ran during these blocks.

| Block | CRDT median / CoV | CSPA median / CoV | Verdict |
| --- | ---: | ---: | --- |
| base-1 | 22876.7 ms / 0.295% | 1954.0 ms / 0.183% | Eligible strict pass |
| candidate-1 | 22800.1 ms / 0.164% | 1783.6 ms / 0.846% | Eligible strict pass |
| candidate-2 | 22857.6 ms / 0.362% | 1788.2 ms / 0.136% | Eligible strict pass |
| base-2 | 22806.8 ms / 0.233% | 1945.0 ms / 0.165% | Eligible strict pass |

CSPA improves **8.72%** in the forward pair
(1,954.0 → 1,783.6 ms) and **8.06%** in the reverse
pair (1,945.0 → 1,788.2 ms). CRDT remains around 22.8 seconds and passes in
all blocks; no CRDT speedup is claimed. Candidate CSPA CoV is 0.846% / 0.136%,
and every workload/block has CoV below 1%. The pairs are reported separately,
without pooling samples across blocks or attributing the difference to a
historical regression owner. Meson base-option output ordering differs between
builds, but all resolved option values, compiler and input hashes agree.

All four predeclared outcomes are retained. Each block must separately report
complete acquisition, eligible host telemetry and a performance pass. Existing
targets remain **38,120 ms CRDT / 2,050 ms CSPA**, with **CoV <=3%**. Correctness
remains **104,851 CRDT result tuples** and **20,381 CSPA tuples / six iterations**.
CRDT's observed 14,148 iterations and aggregate 2,152,328 are retained; the
aggregate is diagnostic only. Raw logs contain every sample. Manifest statistics
recompute population CoV from one-decimal samples, allowing their rounding
precision when checking the more precise reported CoV.

Endpoint telemetry confirms the existing eligibility checks, not continuous
frequency stability. These controlled measurements establish this current-base
change's effect under the adopted environment. They do not assign responsibility
for the historical #1903/shared-runner regression. Broader nightly TRACE,
portfolio, Windows and DOOP scopes are outside these strict blocks.

## Measured diagnosis and implementation

The baseline CSPA native `perf` profile contains 754 cycle samples with zero
lost samples; reservation initialization has **9.04% self attribution**. Its
callgraph reaches that function from `col_rel_append_row_impl`. The candidate
CSPA profile has 686 samples, zero lost, and no sampled reservation-init symbol.
This is diagnostic attribution, not a predicted 9% speedup or acceptance timing.
CRDT profiles contain 6,841 baseline / 6,863 candidate samples, zero lost;
its major join, copy-in and hash paths are unchanged. Unresolved libc hexadecimal
symbols are not assigned a cause.

Native profiles use GCC 16.2.1 / Meson 1.12.0 with ERROR, `-Os`, LTO and debug
symbols, `perf` 7.2.9, CPU 0, `cycles:u` at 399 Hz and DWARF callgraphs. They
remain separate from Docker/GCC15.2 strict timings. CSPA used correctness-only
mode; CRDT used its single-run probe. Both preserve full fixture correctness.
The native diagnostic configurations, commands, reports, stdout/stderr, exits
and before/after host snapshots are retained under `raw/native-*`.

The first two diagnostic launches failed because `taskset -c0` is invalid on
this host. Their exit codes, stderr and host snapshots are retained under
`raw/native-failed-launch-attempt1/`. Corrected `taskset -c 0` launches produced
the profiles above. No failed profile is used as a measurement. The commands
retained here reflect the corrected invocation. Full native binary `perf.data`
and event streams remain at the local paths and hashes in each
`external-raw-manifest.json`; the repository retains their complete text reports
rather than committing those large raw binaries. Reprofiling requires the
listed native build and commands.

The implementation keeps the token declaration, `row_credit_held` cleanup guard,
checked reserve/commit, accounting classes, typed errors and ownership/rollback
unchanged. Initialization occurs immediately before the only branch that uses
the row-staging token. Inline alias staging, nonoverlapping appends and
ungoverned wide alias staging avoid unused initialization. No public API changes
or other #2098 optimization is included.

An independent Architect selected this exact measured unit and an independent
Critic approved its lifetime proof and denial/measurement conditions before
editing. Independent raw-diff review and final design/risk validation remain
required for the final candidate by the implementation harness.

## Safety validation and reproduction

The new test supplies 16/17-column aliases from live contiguous column-zero
values. It verifies exact-budget staging success, one-byte-short staging denial,
growth denial after successful staging, unchanged original rows/storage/
generations/accounting on denial, successful retry and zero leaked credit.
Nonoverlapping setup, inline overlap and ungoverned wide overlap are covered.

Retained [validation](validation/) results:

- Eight focused admission/governor and relation-append tests passed.
- Twelve relation generation/mutation and join tests passed.
- Five relevant ASan/UBSan relation/admission tests passed.
- All controlled strict blocks validate the full CRDT and CSPA fixtures.

The exact acquisition commands are retained in [commands](commands/). They
record this checkout's local paths and expect its prepared isolated directories;
they are execution records, not a generic installer. To reproduce timing,
create separate clones at the baseline SHA, apply `candidate-code.patch` only to
the candidate clone, then run the existing strict wrapper serially in the
predeclared order on the dedicated host. For each checkout:

```sh
PERF_ARTIFACT_DIR="$artifact_dir" PERF_STRICT_SMOKE=true \
  RUNNER_NAME=atlas-perf RUNNER_OS=Linux RUNNER_ARCH=X64 \
  scripts/ci/run-perf-nightly-linux.sh
```

The wrapper creates a `uv` environment inside the inspected local image and
preserves resolved build/compiler options, executable/input hashes, host
snapshots and strict qualification statuses. Do not compare different compilers,
images or profiling modes as before/after acceptance, discard misses/noisy
blocks, rerun until green, or derive a looser threshold from these samples.
