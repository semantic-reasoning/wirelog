# Tagged runner B/M diagnostic eligibility v1

This is a versioned offline eligibility interpretation for two independent,
full `comparison` campaigns collected by `paired-collector-v1.py`. It does not
change a benchmark gate, timing target, nightly threshold, or evaluator v1's
descriptive meaning. `ELIGIBLE` means the records meet the predeclared
conditions for interpretation; it is never a performance `PASS`.

## Freeze before measuring

The policy bytes are pinned by SHA-256
`523029143376f1765e920d6932181cc049831dcebddd7531ffa70cc713848e55`.
Select exact distinct full source SHAs, generate a plan, and publish the
resulting plan (including the Git tree ID resolved for each commit) in a
reviewed commit before either campaign starts:

```sh
uv run python scripts/perf/freeze-paired-bm-plan.py \
  --base-sha FULL_BASE_SHA --candidate-sha FULL_CANDIDATE_SHA \
  --output scripts/perf/paired-bm-plan-v1.json
```

The collector rejects a plan whose policy digest, workload set, mode, or source
pair differs. Its build preflight also requires the CRDT single-run probe and
matching benchmark/timer contract sources on both revisions. Run the collector
once for each plan attempt with `--mode comparison --qualify-bm --bm-plan
scripts/perf/paired-bm-plan-v1.json`; retain each complete output directory.
Do not start collection until the plan commit is public. Historical artifacts
from before that commit are never inputs to this qualifier.

## Evidence and eligibility

Each attempt must contain all 80 raw launches and typed records: two AB/BA
blocks, two warmups and nine timed adjacent pairs per workload and block. Both
workloads are CRDT (`crdt_perf_gate_single_run`) and CSPA
(`bench_flowlog_repeat_1`). The companion raw host record captures logical CPU
count, one-minute load, CPU PSI `some avg10`, and effective cgroup hierarchy
throttle counters before and after every launch. Raw-to-link hashes, raw-to-
typed values, campaign/manifest identity, evaluation report, copied build logs,
source and fixture digests, resolved profiles, compiler identity, timers, and
binary hashes are checked offline.

Both attempts must be complete and correct, use the frozen source pair, and
have matching compiler, resolved profile, fixture, and timer contracts. For
each workload and side, each nine-sample AB and BA block must have CoV at most
3%. Every host boundary must have CPU PSI `some avg10` at most 10%, one-minute
load no greater than logical CPU count, and no effective-cgroup
`nr_throttled` increase within an execution. For each workload and side, the
attempt medians' maximum/minimum ratio must be at most 1.05. The two
candidate/base median ratios must also have maximum/minimum at most 1.05.

Missing required files or telemetry produce `INCONCLUSIVE`; malformed or
internally inconsistent evidence produces `INVALID_EVIDENCE`; observed
correctness/process failure produces `CORRECTNESS_FAILURE`. Failed, noisy,
and incomplete campaigns are retained and are not replaced by selected runs.
The qualifier reports eligibility and descriptive medians only. It does not
compare CSPA's `bench_flowlog_repeat_1` timer with the absolute gate in
`run_cspa_validation.sh`; that gate remains separately enforced by the
existing workflow. It makes no target recommendation or #2022 closure claim.

Evaluate only after both plan attempts are present:

```sh
uv run python scripts/perf/qualify-paired-bm.py \
  --plan scripts/perf/paired-bm-plan-v1.json \
  --attempt-one artifacts/attempt-1 \
  --attempt-two artifacts/attempt-2 \
  --output paired-bm-eligibility.json
```
