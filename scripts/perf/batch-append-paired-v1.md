# Batch append paired comparison v1

The campaign collector in `batch-append-campaign-collection-v1.md` is pinned to
the #2033 pre/post revision manifest. To compare any two prebuilt
`bench_batch_append` binaries, for example main against an optimization branch
for #2036, use `run-batch-append-paired.py`. It reuses that collector's host
probe, eligibility rules, process launcher, plan-v3 hash-sort schedule and
strict benchmark-v2 output parser.

Each side names a binary and the clean, non-shallow checkout it was built
from. The runner records that checkout's commit and tree, and rejects
uncommitted or untracked files. Iteration counts come from a `calibrated`
batch-append-calibration v1 artifact and are identical for both sides. The
evidence directory must be new, under HOME, outside /tmp, /dev/shm and both
sources. Campaigns are never resumed.

The runner copies both binaries into the evidence directory and executes only
those copies. Before the first launch it fsyncs the manifest and the complete
108-command schedule: nine AB and nine BA adjacent pairs per case, with two
warmups and one sample per process. Before each launch it rechecks the copy's
hash. A changed binary, a spawn error or a signal aborts with a recorded
reason; nothing is retried. Every process runs pinned to `--cpu`. For each
process the runner journals:

- the started record, before the process is spawned;
- raw stdout and stderr with their hashes;
- exit and timeout data;
- the parsed output;
- host snapshots taken before and after.

Eligibility uses the calibration rules. CPU PSI some.total over wall time must
be at most 1%. Cgroup throttle counters must not increase. The SMT sibling must
be at most 1% busy. The child affinity must be exactly the selected CPU. An
ineligible process is recorded and the schedule continues. The runner computes
no ratio or verdict.

`evaluate-batch-append-paired.py` evaluates one collection offline. First it
checks that every journaled command matches the schedule regenerated from the
manifest seed, the manifest iteration counts and the side's binary hash. A pair
is used only when both processes succeeded and were eligible. For each case it
reports:

- the raw pairs;
- per-side medians, population CoV and reset-only medians;
- paired candidate/baseline ratios and ns/call savings;
- the AB-minus-BA order effect in percentage points;
- 95% intervals from a stratified bootstrap of 10,000 complete-pair resamples
  within the AB/BA strata, seeded with 2036.

Campaigns are never pooled.

`--mode aa` qualifies a case when both sides are the same binary, the capture
is complete, and both strata have nine eligible pairs. Each stratum's median
|ratio - 1| must also be at most 2%, and the order effect at most 2 pp.
`--mode comparison --aa <collection>` applies the #2036 thresholds:

- 1x1 needs at least 10% median improvement, at least 25 ns/call saved, and an
  upper ratio bound below 1.
- 1x256 and 32x256 need an upper ratio bound of at most 1.03.

A case is `meets` or `does_not_meet` only when it is complete and the A/A
collection qualified the same case. That A/A must use the comparison's base
binary, calibration and CPU. Every other case is `diagnostic`.

The #2036 evidence predates the check-in. An earlier issue-local version of
this runner captured it. That version wrote the same journal records under the
manifest schema `wirelog.issue-2036.paired-run.v1`, without the source
provenance or schedule file. The evaluator accepts that schema as well.
