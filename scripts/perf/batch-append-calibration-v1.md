# Batch append baseline calibration v1

`calibrate-batch-append.py` consumes only a plan-v3 manifest, its exact overlay
patch, and a profile-v2 `not_executable` artifact. The base product must be the
fixed pre tree `4c10e21ab4ba034dbc50abd92a59d22e8b975130`; A/A input is accepted
only when it explicitly selects `pre`. The collector revalidates plan, profile,
manifest, and original baseline binary hashes before and after every process.

It copies the profiled baseline binary into a fresh mode-0700 evidence directory
under HOME, fsyncs the copy, checks its digest, and executes that copy. It never
launches the candidate. The evidence path must be new, under HOME, and separate
from source and build roots. Each attempt gets a durable started JSON record
before spawn, then exclusive raw stdout/stderr files, hashes, argv/cwd/env,
monotonic clocks, exit/timeout data, and host snapshots. The process runs in a
new process group; timeout or signal handling sends TERM, then KILL if needed,
and reaps it. Existing evidence is never resumed.

For each case the collector starts at the plan's default iteration count and
uses two warmups and one sample per process. The strict shared benchmark-v2
parser requires exactly one header, sample, summary, and case record; checks
case dimensions, capacity, iteration options, denied count, row/capacity/value
checks, and distinct-input probe; and rejects malformed or extra records.
Eligible samples below 250,000,000 ns scale by
`min(100000000, max(N + 1, ceil(N * 300000000 / append_total_ns)))`. Each case
is limited to six retained attempts; the first eligible sample at or above
250,000,000 ns is accepted.

Every attempt records online CPUs and affinity, selected pinned CPU, governor,
frequency, model, microcode, kernel, CPU PSI some.total, cgroup-v2 CPU throttle
counters, and SMT sibling counters when measurable. PSI some.total increase
must be at most 1% of full process monotonic wall time. Cgroup throttle counters
must not change. A measurable SMT sibling must be at most 1% busy; absent or
unmeasurable siblings are recorded explicitly as `none`. Governor/frequency are
recorded without making stability judgments from changes. Missing required
telemetry, ineligible host data, timeout, bad output, or failed correctness
checks leave durable diagnostics and prevent calibration publication. Timing
from an ineligible attempt is never used to scale N.

Only after all three cases have accepted counts does the collector atomically
publish `calibration.json`. It binds the plan/profile/manifest hashes, baseline
binary digest, host data, every attempt, accepted counts, and
`candidate_launches=0`. It contains no paired-runner argv schedule and no
performance verdict. Freezing the paired 108-launch argv schedule belongs to
the separate Unit 3B contract.
