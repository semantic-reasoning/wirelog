# PR CI Timing Telemetry (Issue #1570)

`scripts/ci/ci-run-telemetry.py` separates the time a PR CI job spends waiting
for a runner from the time it spends running, attributes the running part to
named phases, and walks the run's critical path two different ways so that
scheduling delay can be told apart from work the dependency graph forces.

It is an offline analyzer.  It reads saved GitHub REST responses from disk, is
wired into no workflow, and needs no build directory.  Only its self-test runs
in CI, registered in `tests/meson.build` under the `abi` suite.

## What it does not do

It does not change `ci-pr.yml`.  The changes this measurement informs belong
to #1574, #1571, #1572 and #1573, the publication path to #1577, and runner
placement to #1581, which exists because this measurement found the effect.
Two of those sequence behind this issue: #1574 names it an explicit
prerequisite, and #1577 ships the publication path second by design.  #1571
and #1573 say they can start independently, #1572 says only that timing
telemetry supplies it performance evidence, and #1581's one prerequisite is
item 17 of #1583 rather than this issue -- or, failing that, naming the single
table it regenerates -- though it is the issue here that this measurement
caused.  So "measure first" is the ordering this issue chose rather than one
all six are waiting on.

It does not read job logs, so it reports no per-test durations and no
compiler-cache hit rate.  Issue #1570's scope line asks for slow tests and
cold/warm cache context; that part is **not** delivered here and is carried
by #1580.  See "Known gaps".

It never substitutes zero for something it could not measure.  Most of the
design below follows from that one rule.

## Running it

Three subcommands, in order.  The first is the only one that touches the
network, and it does so through `gh api` with a hard request budget -- plus one
`gh repo view` outside that budget when it has to work out which repository it
is in.

```
scripts/ci/ci-run-telemetry.py fetch  --out DIR --run-id ID [--run-id ID ...]
scripts/ci/ci-run-telemetry.py report --out DIR --run-id ID [--run-id ID ...]
scripts/ci/ci-run-telemetry.py aggregate --out DIR --label NAME
```

`fetch` saves the raw `runs/{id}` and `runs/{id}/jobs` responses under
`DIR/raw/`.  `report` turns each saved pair into `DIR/run-{id}.json` and a
readable `DIR/run-{id}.md`.  `aggregate` pools the reports already in `DIR`
into `DIR/{label}.json` and `DIR/{label}.md`.

`report` and `aggregate` are offline, and every figure they derive is
deterministic, so a saved capture can be re-analyzed after the tool changes
without spending API requests.  The files are not byte-identical between runs:
each carries a `generated_at` stamp.

`fetch` stops at `--max-requests` (default 40) including retries, and retries
only a rate-limit answer or an HTTP 5xx, each retry charged to the same budget.

## Metric definitions

Every metric is an object, never a bare number:

```json
{"seconds": 863, "status": "measured", "reason": null, "evidence": {...}}
```

| status | meaning |
|---|---|
| `measured` | a real duration, with evidence naming what it was derived from |
| `partial` | a real but incomplete duration; `reason` says what is missing from it |
| `unavailable` | not measurable; `seconds` is `null` and `reason` says why |
| `anomalous` | the arithmetic produced an impossible value; `seconds` is `null` and the raw figure is kept under `raw_seconds` |

There is no separate status for a run still in progress.  `run_wall_clock` and
`pr_feedback`, taken before the run is known to have ended, are downgraded to
`partial`, so a lower bound is never read as a final figure.
`first_failure_latency` is deliberately not downgraded -- a job still running
can only complete later, so it cannot produce an earlier first failure -- and
`total_required_check_completion` is a fixed `unavailable` here.  Do not
generalize the rule to all four.

A `measured` metric cannot be constructed without evidence and cannot be
negative.  Both rules are enforced in the constructor rather than by
convention, so a zero can never be published as a measurement of something that
was not observed.

Each metric under `metrics` carries an `additive` flag saying whether it may be
summed along a path: `queue_delay`, `job_wall` and `scheduler_release_latency`
are additive, `upstream_elapsed` is not.  The flag is set in every state, so a
missing key never has to be interpreted.  (Two docstrings in the source call
the additive one singular.  The flag is right and they are stale; #1583 carries
the fix.)

### Per job

| metric | definition |
|---|---|
| `queue_delay` | job `created_at` to `started_at`: waiting for a runner. Summable along a path. |
| `job_wall` | job `started_at` to `completed_at`: executing. |
| `upstream_elapsed` | run start to job `created_at`: everything upstream, including upstream queueing. Useful for ordering jobs, **not** summable along a path. |
| `scheduler_release_latency` | the last dependency finishing to this job being created: how long the scheduler took to release the job once it was eligible. |
| `unaccounted` | `job_wall` minus the phases that were attributed. Closes the job's wall time. |

The markdown report prints six metrics per job -- `queue_delay`, `job_wall` and
`unaccounted` from the table above, and the `configure`, `compile` and `tests`
phases from the phase table below -- in a table that carries the job name and
its runner population beside them, eight columns in all.

`upstream_elapsed` and `scheduler_release_latency` have no column in either
report, so read those from `run-{id}.json`, where `unaccounted` is likewise a
sibling of `metrics` rather than a key inside it.  `upstream_elapsed` is also
pooled into every aggregate cell without being displayed there; #1583 covers
giving it a column.

Under the per-job table the report lists why any number it printed is not
final: the status and the reason.  More than three entries sharing one reason
*and* one column collapse into a single line carrying the count, because one
sentence repeated once per job buries every line that is not repeated.  Entries
sharing a reason across different columns stay itemized, since a count there
would name neither the job nor the number.

### Did the job run

A job that did not run has no durations, and reporting a zero for one would
read as a job that ran instantly.  Deciding which jobs those are is awkward
because GitHub's stamps do not answer it.  A job that never ran is stamped as
starting the moment it was created, with a completion equal to that or one
second earlier, so neither a start stamp nor a zero span distinguishes it from
a job cancelled the instant it was picked up.

The rule is therefore closed on the other side.  Two kinds of conclusion skip
the evidence:

- `skipped`, which never dispatches whatever it is stamped with;
- an outcome only execution reaches -- `success`, `failure`, `timed_out` --
  where asking the stamps could only erase real seconds.  A job still going
  carries no conclusion at all and is treated the same way.

Every other conclusion is weighed, including one GitHub adds after this was
written, so a new value cannot arrive meaning "ran" by default.  The evidence
is asked in order: a step of its own that ran is conclusive; the span between
the job's own stamps settles it next, and only a positive span is work; failing
that, with no completion to judge by, a start later than the creation is the
only trace of a dispatch.  With none of the three, the job never ran.

The two possible mistakes are not symmetric, which is why the rule points this
way.  An unrecognised conclusion read as executed fabricates a zero that looks
like a measurement in the artefact, drags an aggregate's sample size with it,
and is indistinguishable from a real duration.  Read as never run, a job that
really ran and lost every stamp and every step fabricates no per-job number:
it drops out of the count of jobs that ran, and its row reports `unavailable`
in all six metric columns.

That second mistake is not free, and the document should not pretend it is.
`run_wall_clock` and `pr_feedback` are computed over the jobs that ran, and a
job excluded from that set does not downgrade them.  If the wrongly-excluded
job was the last to finish, both figures come out short and still `measured`.
So the rule buys a bounded, visible per-job absence at the price of a possible
silent shortfall in two run-level totals, rather than buying nothing at all.
It is still the better direction, because the other one fabricates a number
where none exists and does so on every job it mistakes; but "this tool may
under-claim" is the honest summary, not "the worst case is an absence".

What is silent is not the job but its effect on the two run-level totals
above.

A job that did not run carries `ran: false` and is counted in the run summary
under its own conclusion -- as a count, not by name: the line reads `jobs: 15
(10 executed, 5 skipped)`.  It still gets a per-job table row, with
`unavailable` in all six metric columns, and `none` for its population when it
never reached a runner -- a job weighed as never-run that did reach one still
reports the pool it landed on.  What it is excused from is the explanation
block under the table, which would otherwise repeat one sentence six times for
a job nobody expected a number from.  The table row is where such a job is
visible by name.

One metric can survive such a job: `scheduler_release_latency` ends at the
job's *creation*, which happens whether or not the job then dispatches.  It is
*computed* for a job cancelled, stale or failed to start rather than suppressed
with the rest, and that is why the per-job table's caption is scoped to the
table rather than claiming the job has no durations at all.  Computed is not
measured: it still comes back `unavailable` for a root job (`root job; nothing
released it` -- 21 of the baseline's 126 values), for a job absent from the
supplied graph, where no graph was supplied at all, where a named dependency is
absent from the run, and where either end of the interval has no timestamp.

Two skip rules sit on top of that, and they are separate.  A job that is itself
skipped gets `unavailable: job skipped; no execution`, suppressed alongside its
other metrics.  A job that *ran* but whose dependency was skipped gets
`unavailable: released by skipped dependency; creation batched`.  The reason
for the second is the reason both exist: GitHub materializes a whole skip
cascade in one second, so creation stamps inside it carry no ordering.  It is
also the route by which a graph walk's chain ends up weighted by nodes alone.

### Phases

Step names are mapped to a fixed phase table: `runner_setup`, `checkout`,
`cache_setup`, `deps_install`, `configure`, `compile`, `tests`, `tidy`, `sbom`,
`binary_size`, `lint`, `policy`, `post_cleanup`, `other`.  A step whose name
matches no entry lands in `other`.  It is also named in the run's
`unmapped_steps`, which is how a step rename is detected rather than absorbed
-- except for the handful in `KNOWN_UNMAPPED` (`Record bash version`, `Verify
tool version`, `Publish results`), which are deliberately suppressed because
they are known to belong nowhere.  An empty `unmapped_steps` therefore means
"nothing unexpected", not "nothing in `other`".

Two things about that table are load-bearing for anyone editing it.  It has two
tiers -- an exact-name map consulted first, then a pattern list where the first
match wins.  The tiers are what keep the specific `Install ...` names out of
the generic `^Install ` pattern, and that precedence is structural rather than
positional: they are exact entries, so their order among themselves does not
matter.

Order inside the pattern list is inert *today*: all six patterns are
`^`-anchored on prefixes no step name can match twice, so the list could be
permuted without changing any classification.  What is load-bearing is that
`^Post ` exists -- delete it and a `Post <name>` step matches none of the rest
and lands in `other`, so the cleanup time disappears from `post_cleanup` rather
than merging into the phase it follows.  Order becomes load-bearing the moment
a pattern is added that can match another pattern's input, which is why the
list is written as an ordered one.

The table also keys on the step names the **API** reports, which are not the `-
name:` values in the workflow: the runner injects `Set up job` and `Complete
job`, an unnamed `- uses:` step becomes `Run <action>`, and an action with a
post phase adds `Post <name>`.

A step that the workflow's `if:` condition skipped contributes nothing and is
recorded under `skipped_steps`.  A phase whose every step was skipped reports
`unavailable`, not `measured 0`.

### Per run

| metric | definition |
|---|---|
| `run_wall_clock` | run start to the last job that ran completing |
| `pr_feedback` | run start to the last *pinned check* completing: what a reviewer waits for |
| `first_failure_latency` | run start to the completion of the earliest-completing job that concluded `failure` or `timed_out`; its evidence names the job, its conclusion, and the steps that failed |
| `total_required_check_completion` | run start to the last *required* check completing, where the required set is knowable |

`total_required_check_completion` is the one metric that is not computed.  This
repository publishes no `required_status_checks` rule -- rulesets 13129249 and
13670289 carry none, `branches/main/protection` answers 404, and a third
ruleset, 19420631, is disabled and carries only a Copilot review rule -- so the
tool emits a fixed `unavailable` naming the first two rulesets and the 404, and
`required_checks` declares `pr_feedback` as the surrogate.  Adding a
required-checks rule will not change the string: the answer is written into the
source, and making it react to the repository is #1582.

`first_failure_latency`'s `failed_steps` list is what tells #1573 whether the
binary-size gate fired late or early: the job name alone does not say which
step inside it failed.  A job that timed out stamps its steps `cancelled`
rather than `failure`, so an empty `failed_steps` on a `timed_out` job is
expected, and the evidence carries the conclusion beside the list so an empty
one is not read as a job that failed with no failing step.

`job_counts` carries a `total` -- the number the summary line prints as `jobs:
N` -- and divides the run into `executed`, `skipped` and `never_ran`, plus
`did_not_run_by_conclusion`, a map from GitHub's conclusion to a count.  That
map is open-ended: expect keys this document does not list.  `skipped` is a
second spelling of `did_not_run_by_conclusion["skipped"]`, kept so a JSON
consumer written against the earlier shape still works.  In that rendered
summary line, every term naming a job that did not run comes from one label
table, and a conclusion the table has not met names itself; `executed` is the
one literal.  The per-job reasons come from a second table, which is why adding
a conclusion means touching both.

### Critical paths

Two, reported side by side.

- **`observed`** walks back from the last job to finish, hop by hop,
  through that job's actual declared dependencies, and sums `queue_delay +
  job_wall` per hop.  It follows the graph rather than taking the nearest
  earlier finisher, which on real runs picks a macOS matrix build as the
  predecessor of a TSan job that does not depend on it.
- **`graph`** takes the longest path over the declared graph, weighting each
  node by `job_wall` and each edge by `scheduler_release_latency`.  It
  excludes queueing.

Their difference is the part of the wait the scheduler added rather than the
part the dependency graph forces -- exactly so only when both walks pick the
same chain, which they need not.  Where they diverge, the difference also
absorbs the chain choice, and subtracts release latencies the observed walk
never counted.  On the baseline below the two walks selected the identical
chain on all seven runs, and the graph path's edge weights summed to between 1
and 3 s, so there the difference is queueing to within those few seconds.

The dependency graph is read out of `.github/workflows/ci-pr.yml` at analysis
time, not hardcoded, so a job rename shows up as drift in
`critical_path.graph_drift` instead of a silently stale answer.  The `lint` job
is a reusable workflow; its caller edges are wired to the callee's roots and
the caller's dependents to the callee's sinks.  A `needs:` form this parser
does not read is refused outright rather than read as "this job is a root".

The two walks also treat a job that *did not run* differently, which matters to
anyone reading these paths to decide what to reorder.  The observed walk
refuses to step into such a job -- walking through it at zero weight would
erase the gap it represents -- so the chain **stops there**, and the path is
`partial` with the reason `walk stopped: X's dependencies did not run`.  The
graph walk instead routes **through** it at a zero node weight, keeping the
chain intact and qualifying the total.  So a short `observed` chain and a short
`graph` chain mean opposite things, and the reason string is the only way to
tell a path that ended from a path that was cut off.  Read the status, not the
length.

Both walkers refuse to sum a hop they could not time.  Beyond that, if any job
that ran, or may still run, went untimed -- on the reported chain or not -- the
path is `partial` and names those jobs.  That is deliberately over-inclusive,
because either walk can exclude such a job without leaving a trace on the chain
it reports: the graph walk weighs an untimed node at zero and routes the
longest path around it, and the observed walk starts at the last job it could
time and steps over an untimed later finisher.

Whether a job may still move depends on the run as well as the job, and the run
has three states rather than two.  A run whose status this tool does not
recognise is neither known to be going nor known to be over, so nothing about a
job's future in it is settled and the report says that rather than guessing.
The run-level totals take the opposite default and become lower bounds, because
an unreadable status might mean the run is unfinished.  Each errs in the
direction that cannot overstate.

## Provenance and caveats

- Job-level timestamps come from GitHub's server clock; step timestamps
  come from the runner's clock.  Two of the figures the tool reports subtract
  across the two.  `clock_offset_estimate` subtracts a server-clock job start
  from a runner-clock first step, which is how the offset is measured at all.
  It is on every job in `run-{id}.json`, and it has no column in the markdown,
  which mentions it only inside a `runner_clock_skew` anomaly line.
  **`unaccounted` is the one that is printed**, as a column in the per-job
  table, and it subtracts a sum of runner-clock step spans from a server-clock
  job window.  Every other reported duration stays inside one clock.
  `unaccounted` is why `steps_exceed_job_wall` exists as an anomaly kind, and
  on the baseline its exposure cancels -- see the clock skew paragraph there --
  but it is the one printed number a reader should not assume is clock-safe on
  an arbitrary run.  Where a runner's clock differs from the server's, the
  offset is estimated per job and reported as a `runner_clock_skew` anomaly.
- `pr_feedback` equals `run_wall_clock` by construction on a run where
  every job that ran is a pinned check.  They are not two agreeing
  measurements.
- `critical_path.observed` is a lower bound on run wall clock: it omits
  the first job's upstream wait and every scheduler release latency.
- Percentiles are nearest-rank, so a median is a sample value rather
  than the midpoint of two.  Where n is under 20, the p95 column is the sample
  maximum, not a percentile estimate.
- Aggregation never pools across runner populations, and a pooled cell
  that still spans more than one self-hosted machine is flagged, because those
  machines are not interchangeable.
- The aggregate carries no per-cell reason, so feed it completed, first-attempt
  runs.  Note what that does and does not guard against.  A per-job metric is
  never `partial`: only `run_wall_clock`, `pr_feedback` and the two critical
  paths are ever downgraded, and none of those is pooled.  Exclusion is per
  metric rather than per job, which is what makes a live run dangerous to pool.
  A job that has started but not finished yields `unavailable` for `job_wall`
  only: its `queue_delay` and `upstream_elapsed` are `measured` and pool
  normally.  So a half-done run contributes wall times for its finished jobs
  alone -- biased fast -- while also contributing the queue delays of the jobs
  still running, so the two metrics end up pooled over different sets of jobs.
  Which way that biases the queue figures depends on where the capture falls,
  and this baseline cannot say: it contains no half-done run.  Each metric's
  `n` says how many values it actually pooled -- in the JSON for all three, in
  the markdown for `queue_delay` and `job_wall`, which are the two with
  columns.  That gives a partial check: a cell whose `queue n` exceeds its
  `wall n` pooled queue delays from jobs whose wall times it did not, so the
  two columns are not describing the same jobs.  It is neither necessary nor
  sufficient for "a live run got in".  A completed run trips it too when a job
  lost its completion stamp, and it stays silent on a live run whose unfinished
  jobs have not started, since those have no `queue_delay` either.
- Half of "completed, first-attempt" the tool enforces itself: a report
  whose `run_attempt` is not 1, or is missing, is dropped into the aggregate's
  `excluded` list with the reason.  Cells are keyed by job name, runner
  population **and** run conclusion, so a failed run's jobs never pool with a
  successful run's.  Nothing enforces completeness, which is the half left to
  the operator.

## Known limits of the rule above

None of them affects the baseline, which was taken from completed successful
runs, but they are not all alike in whether anything is filed to change them.
Four have an owner: limits 1, 3 and 6 are items 7, 8 and 9 of #1583, and limit
5 names #1582 item 15.  Limits 2 and 4 have none, and follow from the design
rather than from missing work.

1. A job cancelled while it was still queued is reported with no durations at
   all, including the `created_at` to `started_at` wait it genuinely accrued.
   One flag gates all six metrics.  Splitting "ran" from "was queued" would
   recover a real queue measurement, which is the thing #1570 exists to
   measure.
2. `failure` is treated as proof of execution, and a self-hosted job that
   exhausts the queue limit, or whose `runs-on` matches no runner, can reach it
   without dispatching.  Routing `failure` through the evidence instead would
   be worse: a job that ran, failed and lost its stamps would become never-run,
   dropping out of `executed`, out of the wall clock and out of the failure
   scan, so `first_failure_latency` would report no failure on a run that
   failed.  That metric is read precisely when things are broken.
3. An in-flight job counts as executed in `job_counts`, so a run still
   going reports every job that has not yet concluded without running as
   executed.  A job that has already concluded `cancelled`, `stale`,
   `startup_failure` or `action_required` mid-run is weighed by the evidence
   like any other, so it *can* drop out -- but one cancelled after its own
   steps ran stays in `executed`, which is correct.
4. A *required* check that never ran leaves `pr_feedback` measuring less
   than the feedback a reviewer actually waits for.  The reader's signal is
   that job's per-job row, which is present and reads `unavailable`; the run
   summary gives a count by conclusion but no name.
5. `pr_feedback` -- the headline run-level number, and the declared surrogate
   for a required set this repository does not publish -- is scoped by
   `PINNED_CHECK_PREFIXES`, a hand-maintained tuple matched by prefix against
   job names.  It is not checked against the run and not covered by
   `graph_drift`.  Today it matches every job in `ci-pr.yml`, which is why
   `pr_feedback` equals `run_wall_clock` on this baseline.  A rename changes
   what the number covers, and so does an *added* job that matches no prefix --
   the case #1582 item 15 records and no issue has promised against.  Two
   traces exist and both are weak.  `non_pinned_executed` in the JSON evidence
   counts the jobs that fell outside the list, but the report mentions it only
   when it is zero, so a lapse shows up as a caveat disappearing rather than a
   warning appearing.  And `pr_feedback` diverges from `run_wall_clock` in the
   run-level table only when the unmatched job is the last to finish.
6. Adding a conclusion to the tool means touching its reason table, its
   label table and, if it proves execution, the ran list.  Nothing forces an
   author to find all three; the reason and label tables are pinned against
   each other, and both fall back to naming the conclusion itself, which is
   always true.

## Changed wording

Strings the reports print changed during development.  A reader grepping saved
CI artefacts for the old ones will miss silently.

| old | new |
|---|---|
| `N finished job(s) ... were not timed` | `N job(s) ... reported no duration` |
| `job has not completed`, on a job the API called completed | `job reported no completion time` |
| `job never started`, on a job whose steps ran | `job reported no start stamp; its first step ran at ...` |

## Measured baseline

**Sample.**  Seven `CI PR` runs, all `conclusion: success`, all `run_attempt:
1`, all from 2026-09-10, 126 jobs in total.  Failed runs were excluded on
purpose: a failure truncates the graph, so its critical path measures a
different thing.  These seven are not that day's successful runs; by the same
first-attempt rule the sample itself applies, they are the last seven of
twenty-eight, and `CI PR` ran sixty-five times in all, sixty-three of them
first attempts.  The selection is recency, not a random draw, so the sample is
the day's tail -- which is also where the three clustered runs below sit.
Unless a figure says otherwise, anything below that counts *other runs* counts
only these seven.

| start (UTC) | run | head | branch | self-hosted jobs |
|---|---|---|---|---|
| 12:36:36 | 34477686557 | 8b893e3f | `issue-1471-program-free-guard` | 3 |
| 13:41:32 | 34484224868 | b0bc5f1f | `issue-1473-public-denial-mapping` | 2 |
| 14:08:34 | 34487103833 | d41b4349 | `issue-1515-tombstone-reuse` | 6 |
| 17:34:10 | 34509006108 | 3544a3c6 | `issue-1528-consolidate-fix` | 0 |
| 18:23:45 | 34514058197 | f5667e80 | `issue-1473-public-denial-mapping` | 6 |
| 18:26:50 | 34514377419 | d9e09c80 | `issue-1469-intern-governor-rebind` | 7 |
| 18:27:11 | 34514413925 | d6b217c2 | `issue-1500-empty-index-lease` | 6 |

The start times are in the table because the queueing figures below cannot be
read without them.  **The last three runs began within 3 minutes 26 seconds of
each other**, and those three are exactly the three that queue heavily.
Nothing here separates "this pipeline queues" from "three PRs were pushed at
the same minute".

Nor is there a clean control to compare against, and the sample cannot see most
of the contention.  Taking each run's window as its start plus its wall clock,
every one of the seven overlaps two or three *of the other six*: the first
three overlap each other, and the fourth overlaps the cluster it precedes.
Against the day's sixty-five `CI PR` runs the test gives five to sixteen
overlapping runs each -- the same range whether a retried run is taken as one
window or attempt by attempt -- so the in-sample count understates real
concurrency several-fold, and it counts no other workflow at all.  That is also
a loose test: an overlapping run may have nothing executing.  Sampling each
run's window at one-minute resolution, two of the seven spend 45 percent of it
with no job from the other six running, and the mean number of those running
ranges from 3.2 to 6.0.  Read both as within-sample quantities.  So the runs
are not uniformly contended -- and none is isolated, in the sample or outside
it.

What the sample does show is where the long waits sit.  Twelve jobs queued 1000
s or more; the next longest wait in the sample is 892 s.  Here is every one of
them, with the run it belongs to and the number of *sampled* runs in flight
when it was created -- by the same start-plus-wall-clock window as above, but
counting the job's own run.  This is an instantaneous count at one moment, so
it does not line up with the whole-window overlaps quoted in the paragraph
above:

| created | queue | sampled runs | run | job |
|---|---|---|---|---|
| 18:59:50 | 1145 s | 4 | 34514413925 | `Sanitizers / macos-latest / clang` |
| 18:59:50 | 1202 s | 4 | 34514413925 | `Sanitizers / ubuntu-latest / gcc` |
| 18:59:50 | 1295 s | 4 | 34514413925 | `Sanitizers / ubuntu-latest / clang` |
| 18:59:50 | 1346 s | 4 | 34514413925 | `Build / ubuntu-latest / gcc` |
| 19:22:00 | 1142 s | 4 | 34514058197 | `TSan-native / ubuntu-latest / gcc` |
| 19:22:00 | 1408 s | 4 | 34514058197 | `TSan / ubuntu-latest / gcc` |
| 19:22:00 | 1599 s | 4 | 34514058197 | `TSan / ubuntu-latest / clang` |
| 19:54:17 | 1081 s | 3 | 34514377419 | `TSan / ubuntu-latest / gcc` |
| 19:59:58 | 1868 s | 3 | 34514413925 | `Build / ubuntu-latest / clang` |
| 20:25:47 | 1629 s | 2 | 34514413925 | `TSan / ubuntu-latest / gcc` |
| 20:25:47 | 1645 s | 2 | 34514413925 | `TSan / ubuntu-latest / clang` |
| 20:25:47 | 1979 s | 2 | 34514413925 | `TSan-native / ubuntu-latest / gcc` |

They fall in an eighty-six minute span.  The `sampled runs` column counts only
the sampled seven, and over this span it does not merely undercount, it points
the wrong way: across all sixty-five `CI PR` runs of the day the count at those
five creation stamps is 7, 9, 11, 11, 12 against a sampled 4, 4, 3, 3, 2.  A
run counts while any of its attempts is in flight, each attempt taking its own
start and end, so a retried run counts during its attempts and not in the gaps
between them.  That distinction is not academic here: one of the sixty-five was
retried twice, and treating it as one window opening at its last attempt's
start loses the two earlier attempts and undercounts the 19:22:00 stamp by one.
Read the column as a property of the sample, never as concurrency.  The table
locates the long waits; it does not explain them, and it cannot be pushed into
explaining them.  Little of it is independent -- twelve rows, but five creation
stamps and three runs, eight rows from one run -- and it is a threshold
selection, so the groups it appears to form are shaped by the threshold as much
as by anything in the pipeline.  Comparing those groups against each other
supports no conclusion: on the full 126 jobs the same comparison points the
other way -- nearest-rank median queue delay is 384, 13, 10 and 3 s at four,
three, two and one sampled runs in flight -- a count that sees six other runs
at most, where the day had sixty-four.  The two ends of that gradient are drawn
from disjoint sets of runs -- four in flight is only the three simultaneous
runs, one in flight only the two that spend 45 percent of their window with no
job from the other six running -- while the middle two groups draw on four and
five runs each.  Within a run the direction is not even consistent: of the six
runs that span more than one level, three follow it, one is flat, one inverts,
and one runs 834, 415 and 1645 s (n = 10, 5 and 3) as its own concurrency
falls.  So the comparison is confounded with run identity, since branch, head
commit, self-hosted placement and the time of day all travel with the run.  A
sample that could apportion any of this would space the runs deliberately and
record the queue depth at each job's creation; this one does neither.

Two of the seven sit on the same branch.  That matters for the cache argument
below rather than for the timings: the GitHub Actions cache is branch-scoped,
so those two are not independent observations of cache warmth.

n = 7 is a small sample.  The two p95 figures below are over 126 jobs, so they
are ordinary nearest-rank percentiles; the aggregate's per-cell columns are
sample maxima instead, since every cell is far under the n = 20 threshold (1 to
7 across all cells, 1 to 6 in the placement table below).

The sample is not on hosted runners only, which #1570's acceptance asks for,
and #1570 should not be read as having delivered that.  #1581 carries it:
controlling the split is its first scope item, and its re-measurement pins one
arm of each pair to the hosted pool, which is a hosted-only baseline.  It is
not the same shape as the one published here: #1581 pairs on a single head
commit where these medians and p95s spread over seven, it covers the eight job
classes of its own table rather than all eighteen jobs a run carries, and its
acceptance asks for the variance *after* the change where #1570 asks for a
before-change baseline.  It answers the placement question rather than
reproducing this distribution on hosted runners alone.  96 of the 126 jobs ran
on GitHub-hosted runners and 30 on four self-hosted machines.  The labels in
use do not control that split -- both pools answer to `ubuntu-latest` and
`windows-latest` -- though the workflow could: self-hosted runners always carry
an implicit `self-hosted` label, so `runs-on: [self-hosted, ubuntu-latest]`
would pin jobs to that pool today.  Pinning the other way needs either a label
the self-hosted machines do not answer, or for those machines to stop answering
the bare labels.  No label in this sample is a candidate: the two that only
ever reached hosted runners, `ubuntu-24.04-arm` and `macos-latest` at 14 of 14
jobs each, select a different OS or architecture, so they cannot stand in for
`ubuntu-latest` on these jobs.  The capture could not settle it anyway -- it
records the labels each job requested, not what each machine offers.  The split
moves a single job's duration by as much as 3022 s of median wall time.
Nothing in this baseline ranks that against the other effects below.  No
measurement compares them; they are not even the same kind of quantity -- a
per-job median difference, a per-run path range, a share of summed execution --
and the placement gap's cause is not established at all.  Read what follows as
four separate measurements, not as a league table.

**How long a PR waits.**

| metric | n | median | max |
|---|---|---|---|
| `run_wall_clock` | 7 | 7414 s | 10366 s |
| `pr_feedback` | 7 | 7414 s | 10366 s |
| `first_failure_latency` | 0 | unavailable | unavailable |
| `total_required_check_completion` | 0 | unavailable | unavailable |

Both `unavailable` rows are unavailable rather than zero, for different
reasons.  `total_required_check_completion`: this repository has no
`required_status_checks` rule in any of its three rulesets -- 13129249 and
13670289 are active and carry none, 19420631 is disabled and carries only a
Copilot review rule -- and `branches/main/protection` answers 404.  The report
records `pr_feedback` as the surrogate and says so.  Any later comparison
against a repository that does pin required checks must not treat these two as
the same measurement.

`first_failure_latency`: all seven runs succeeded, so there is no first failure
to time.  #1570's acceptance asks for this figure, and #1570 should not be read
as having delivered it; this sample cannot supply it -- the runs were chosen
successful on purpose, because a failure truncates the graph and its critical
path measures a different thing.  Getting it needs a second sample of failed
runs, read for this metric alone.  Both #1573 and #1574 need this figure for
their own before-and-after comparisons, so both will produce one.  Neither
covers publishing it as part of *this* baseline, which is what #1583 item 23
carries.

**Where the time goes.**  Across all 126 jobs:

| | n | median | p95 | sum |
|---|---|---|---|---|
| `queue_delay` | 126 | 29 s | 1346 s | 32582 s |
| `job_wall` | 126 | 1083 s | 3605 s | 150623 s |

35 of 126 jobs waited longer than they ran.  Queueing is 18 percent of summed
job lifetime, counting queue and execution together, but it is concentrated:
the median job waits half a minute and the worst waits 33 minutes.

**The critical path.**

| | median | min | max |
|---|---|---|---|
| observed | 7412 s | 6659 s | 10362 s |
| graph | 6822 s | 5001 s | 7987 s |
| queueing on the observed path | 996 s | 12 s | 4853 s |

The path terminates at `Build / windows-latest / msvc` on five of the seven
runs and at `TSan / ubuntu-latest / clang` on the other two, over five or six
hops.

Queueing is 11.1 percent of the critical path, taking the median of the seven
per-run shares.  Dividing the two medians in the table instead, 996/7412, gives
13.4 percent, but those medians come from different runs and no run had both
values; the per-run shares are 0.2, 1.0, 1.9, 11.1, 21.4, 26.4 and 46.8
percent.  The spread is what matters, not either central figure: 12 s of
queueing on the best run and 4853 s on the worst.

The three runs at the top of that spread are the three that started within
three and a half minutes of each other, so the spread is at least partly a
property of this sample rather than of the pipeline.  It is still the right
shape to plan against -- a queueing figure quoted as a median will understate
what a PR meets when it arrives alongside others -- but it does not establish
how often that happens.

The two runs whose path terminates at `TSan / ubuntu-latest / clang` rather
than the Windows build are the two where the graph path covers least of the
observed one, at 74 and 53 percent.  Both queue heavily, at 1797 s and 4853 s,
but they are not simply the two worst: a run that still terminated at the
Windows build queued 1855 s.  Heavy queueing does not by itself move where the
path ends.

**Execution time dominates, and compilation dominates execution.**  Phase
totals summed over all 126 jobs:

| phase | seconds | share of `job_wall` |
|---|---|---|
| `compile` | 131602 | 87% |
| `tests` | 13840 | 9% |
| `configure` | 1582 | 1% |
| `deps_install` | 1532 | 1% |
| `tidy` | 674 | <1% |
| everything else | 819 | <1% |
| `unaccounted` | 574 | <1% |

Attribution closes to within 0.4 percent of summed job wall time, and the
`other` catch-all holds 4 seconds across all 126 jobs, so the compile share is
not an artifact of steps falling into it.  `unmapped_steps` was empty on every
run, but on its own that would prove less than it looks: the three
`KNOWN_UNMAPPED` names are suppressed from it while still landing in `other`.
The 4 seconds is the claim that carries.

**The same job's duration differs by 2.4x to 4.5x between the two runner
pools.**  Both the GitHub-hosted pool and the self-hosted pool of four machines
answer the plain `ubuntu-latest` and `windows-latest` labels, so the same job
lands on either pool from run to run and nothing in the workflow chooses.
Three of the four machines served only `ubuntu-latest` jobs here and one served
only `windows-latest`, and none served both -- but that is what they were sent,
not what they offer, and the `windows-latest` machine rests on n = 2.  The
eight jobs below are the compile-heavy ones; four shorter jobs go the other way
and are named after the table.

| job | hosted n/median | self-hosted n/median | ratio |
|---|---|---|---|
| `Sanitizers / ubuntu-latest / clang` | 3 / 3861 s | 4 / 849 s | 4.5x |
| `mbedtls-enabled / ubuntu-latest / gcc` | 5 / 2268 s | 2 / 610 s | 3.7x |
| `Build / windows-latest / msvc` | 5 / 4399 s | 2 / 1377 s | 3.2x |
| `TSan-native / ubuntu-latest / gcc` | 4 / 1320 s | 3 / 433 s | 3.05x |
| `Sanitizers / ubuntu-latest / gcc` | 4 / 3108 s | 3 / 1027 s | 3.03x |
| `TSan / ubuntu-latest / gcc` | 6 / 1502 s | 1 / 544 s | 2.8x |
| `Build / ubuntu-latest / clang` | 5 / 1160 s | 2 / 476 s | 2.44x |
| `Build / ubuntu-latest / gcc` | 4 / 1215 s | 3 / 502 s | 2.42x |

The gap sits in the `compile` phase, in the same direction and of the same
order: compile ratios for those eight jobs run 2.9x to 4.9x against job ratios
of 2.4x to 4.5x, close but not equal -- `Build / ubuntu-latest / clang` is 2.4x
by job and 3.2x by compile.  So it is not a difference in what the job does.
Four jobs invert, all of them short: `lint / EditorConfig check`, `lint /
uncrustify check`, `RC changelog freeze gate` and `Detect build-triggering
changes` are slower self-hosted, which is fixed startup overhead on a job that
does almost no work.

Five things bound how hard these ratios can be pushed.  Sample sizes per cell
are 1 to 6 here, though the aggregate's other cells reach 7.  The two sides of
a cell come from different head commits, and two of the seven runs from one
branch, since each run contributed whichever placement it happened to get.
Self-hosted placement is far from uniform: the seven runs took 0, 2, 3, 6, 6,
6 and 7 of their eighteen jobs self-hosted.  And six of these eight
self-hosted cells pool more than one machine -- the aggregate flags them for
exactly this reason, `yes` in its `mixed machines` column and
`"mixed_machines": true` in its JSON, since those machines are not
interchangeable.  Two rows are worse than flagged: `Build / ubuntu-latest /
clang` and `mbedtls-enabled / ubuntu-latest / gcc` each have n = 2 spanning
two machines, so each self-hosted median is one machine's single sample.  And
every one of the 126 jobs is from a single day, so hosted throughput -- the
one side of every ratio outside this repository's control -- varies however it
varied that day, unmeasured and uncontrolled.  #1581's paired same-commit
design does not remove that bound; it holds it deliberately instead of by
accident.  Treat the ratios as an order of magnitude, not a calibration.

**The cause is not established, and the obvious mechanism for it is not.**  A
warm compiler cache would land exactly where this gap is, and nothing here
rules that out -- what is ruled out is the reflex explanation of *why* one pool
would be warmer, machine persistence: `ci-pr.yml` sets `SCCACHE_GHA_ENABLED` at
workflow level and each compiling job runs a `Setup sccache` step, so both
pools route through the same GitHub Actions cache backend rather than through
anything local to a machine.  Nothing in this measurement bounds how much of
the gap is cache behaviour and how much is machine throughput -- it could be
all of one.  The per-run cache hit rate would bound it, and it lives in the
job log this tool does not read -- that is gap 1 below, filed as #1580, and it
is the same signal #1571 is chasing.  It is not the only route: running one
commit through both pools would separate them without any log, which is
what #1581 proposes.

**Clock skew.**  22 `runner_clock_skew` anomalies across the seven runs.  The
split is by machine rather than by pool: 20 of the 22 are the two
`semantic-reasoning-i401-6*` machines, consistently 8 to 9 seconds behind
GitHub's clock, and the other two are one hosted runner and one self-hosted
machine at 3 seconds.  The partition is not clean: the self-hosted machine that
raised one of those two, `B2A1`, sits at 2 to 3 seconds, inside the hosted
range of 0 to 3, and so does the one remaining machine, which never raised an
anomaly at all.  Only the two `i401-6*` machines are separated from the hosted
pool by their offsets.

It changes no figure above, and the reason is arithmetic rather than avoidance:
`unaccounted` does cross the two clocks, but a constant offset cancels between
the head gap and the tail gap of the same job.  On the 20 jobs from those two
machines the median head gap is -8 s and the median tail gap +17 s, summing to
a skew-free 9 s, and no job in the sample raised a `steps_exceed_job_wall`
anomaly -- `runner_clock_skew` is the only anomaly kind the baseline produced
at all.  The skew is recorded because a future step-level comparison would not
have that cancellation.

## What this says about the planned work

First, one distinction the numbers above will not survive without.  The 87
percent compile share is a share of **summed job wall time across 126 jobs** --
a throughput figure, counting parallel jobs separately.  What a PR waits for is
the critical path, where the denominator is different: on the observed chain,
compile is 50, 68, 68, 72, 74, 77 and 84 percent of the path across the seven
runs, median 72.  Both figures are real and they answer different questions.  A
lever sized against the wrong one is mis-sized.

- **#1574 (job graph).**  The graph-path share of the observed path --
  100, 99, 98, 89, 79, 74 and 53 percent -- does *not* measure what reordering
  can recover, and it is easy to read as though it did.  It measures how much
  of the wait was queueing rather than chain execution, so it bounds capacity
  work, not graph work.  A share near 100 percent means the wait *is* the
  dependency chain, which is an argument for restructuring it, not against.

  What reordering could move is the work sitting on the chain ahead of the
  terminal job.  Its execution alone is 1675, 2727, 2875, 2987, 3293, 3566 and
  3904 s across the seven runs, median 2987; counting the queueing those hops
  would carry off the path with them, the figures are 2738, 3010, 3051, 3336,
  3491, 4560 and 7112 s, median 3336.  The first series is the conservative
  reading and the second the optimistic one, and neither is a promise: both
  assume the terminal job stays terminal.

  Three different chains produce those numbers, as `ci-pr.yml` stands today.
  Five runs end at `Build / windows-latest / msvc`, which `build-matrix`
  reaches only after `build-arm` and `build-mbedtls`, both of which need
  `build-primary`, which needs `lint`, which needs `rc-changelog-freeze` --
  four of those runs enter through `build-mbedtls` and one through `build-arm`.
  The other two end at `TSan / ubuntu-latest / clang`, where `tsan` needs
  `sanitizer-matrix` and `sanitizer-matrix` needs `lint`.  Every build and
  sanitizer job named here also needs `build-scope`; `lint` needs only
  `rc-changelog-freeze`.  Every observed chain in the baseline starts at
  `rc-changelog-freeze` because the walk enters through `lint`, and
  `rc-changelog-freeze` behind it is a root -- `build-scope` is a root too, so
  rootness alone does not pick the branch.

  The chains do not sort the figures cleanly.  The two TSan runs rank fifth and
  seventh of seven in both series -- a `build-mbedtls` run sits between them in
  each, at a conservative 3566 s and an optimistic 4560 s -- and in the
  conservative series a single Sanitizers job is almost the whole figure for
  each: 3252 s of 3293, and 3861 s of 3904.  The two went through different
  legs of the matrix, the gcc and the clang sanitizer job respectively.  The
  conservative minimum, 1675 s, is the one run that entered through
  `build-arm`, on a 681 s hop where the four `build-mbedtls` runs carry 1483,
  2268, 2272 and 2434 s.

  That run is worth following, because it shows the two effects are not
  separable.  Its `mbedtls-enabled / ubuntu-latest / gcc` landed self-hosted
  and took 619 s against the arm hop's 681 s, so the graph walk, which weighs
  nodes by wall time, took `build-arm`.  The observed walk agreed by a narrower
  margin: mbedtls also queued 39 s longer, so the two finished 23 s apart
  rather than 62.  Either way the longest path went through `build-arm`
  *because* the mbedtls branch happened to be fast, and that is the same
  placement effect the table above sizes at 3.7x for that exact job.  For this
  run's figure, chain choice and machine speed are not competing explanations;
  they are one.  (This is the low end of the conservative series only -- in the
  optimistic series the same run is fourth of seven.)  Attacking only the
  Windows chain would still leave the two TSan runs untouched.  Whether any of
  these edges is necessary is a question about the build that this measurement
  does not answer, and it is #1574's actual subject.

  Reordering does not reduce total compile seconds.  It changes how many of
  them are serialized on the path, which is the 50 to 84 percent above, not the
  87.
- **Runner placement**, which no open issue owned before this measurement and
  which #1581 owns now: 2.4x to 4.5x on the same job, up to 3022 s of median
  difference on `Build / windows-latest / msvc`.  Its cause is not established
  -- see the cache confound above -- so this is a variance worth removing
  before other measurements are taken, ahead of being a lever worth pulling.
- **Queueing** is a tail, not a median: 12 s on a good run, 4853 s on a
  bad one.  Whether the tail is the pipeline or the three concurrent runs that
  produced it is exactly what this sample cannot say.
- **#1571 (compiler cache) and #1572 (duplicate test compilation)** act on
  compile time, which dominates by either denominator: 87 percent of summed
  execution, a median 72 percent of the observed path.  Neither settles the
  placement question.  #1580 would supply the per-run hit rate and #1571 a
  cache-disabled control, but #1571's own controls compare "equivalent
  SHA/compiler/runner/configuration", holding the runner constant -- the one
  comparison that cannot separate the two pools.  #1581 owns that question and
  settles it by running the same commit through both pools, which needs
  neither of them.

  That is a direction for #1572, not a size.  This tool attributes compile
  seconds per job; nothing in the job and step API says *which* translation
  units a job compiled, so the share of those seconds that is duplicated --
  which for #1572 is repeated work inside one build, not work repeated across
  jobs -- cannot be derived from this capture at all.  Sizing #1572 needs a
  build-level instrument.  See gap 3 below.
- **#1573 (early size failure, and specialized builds scoped to their
  contracts).**  The measurement speaks to both halves, in different terms: the
  first it sizes only in part, the second it only locates.  `Check binary size`
  ran once per run, on `Build / ubuntu-latest / gcc`, and took under a second
  every time, so the gate costs nothing to run on a run where everything before
  it passed -- which all seven were.  What it costs where an earlier gate
  fails, the case #1573's first half exists for, has no observations here.  Its
  cost is where it sits: step 12 of 16, entered 6 to 21 minutes into its job
  (391 to 1251 s, median 948 -- a job stamp subtracted from a step stamp, so it
  crosses the two clocks, immaterial at this size), which is 126 to 277 s after
  the `Build` step it guards has finished (median 158).  Five steps separate
  them -- `Test`, then a clang-tidy install and the tidy ratchet gate, then a
  syft install and the SBOM gate -- and the tidy pair, which the size check has
  nothing to do with, is the larger contributor in four of the seven runs.
  Moving the gate up past those five alone would cut about two to four and a
  half minutes from the wait for a size verdict.  #1573 proposes more than
  that: building the production shared-library target first puts the gate ahead
  of the compile as well, and the compile is 195 to 1042 s of the same offset
  -- 55 to 84 percent of it in six of the seven runs, and just under half in
  the seventh.  The sample brackets that larger saving without locating it: the
  shared-library build is somewhere between nothing and the whole compile, so
  the saving is at least the gap and at most the gap plus the compile -- 126 to
  277 s at the low end, 347 to 1222 s at the high end, per run.  Narrowing it
  needs an observation this sample does not contain, because `Build` here is an
  unrestricted `meson compile`, the whole default target set, never the
  shared-library target alone.  On a passing run that buys nothing: the gate
  takes under a second either way.  What it buys is failure-signal latency on a
  run where the gate fails, which is the first half of #1573.
  `first_failure_latency` does not measure this interval: it runs from the
  run's start to the failing *job's* completion.  `failed_steps` is what names
  the step.

  The second half -- scoping the mbedTLS and native-TSan builds to their
  documented contracts -- is what the evidence below speaks to.  That evidence
  appears under #1574 above as well, only because that is where the critical
  path is discussed.  `mbedtls-enabled / ubuntu-latest / gcc` sits on the
  observed chain in four of the seven runs, carrying 1483, 2268, 2272 and 2434
  s, and it is the 3.7x row of the placement table.  A job that runs the full
  default build to validate a narrower contract is paying that on the critical
  path.  `TSan-native / ubuntu-latest / gcc` sits on no observed chain in this
  baseline, so what the measurement lacks for the native-TSan half is
  critical-path presence rather than cost: it has a measured wall, median 1320
  s hosted over n = 4 against 433 s self-hosted over n = 3, and a share of the
  87 percent.  And it does not show how much of the mbedTLS cost the
  contract actually needs -- that is a build-level question, the same one gap 3
  raises for #1572.

## Known gaps

The first two entries cover three #1570 scope lines this branch does not meet:
one is a measurement the job and step API cannot yield at all, the others are
delivery rather than measurement -- the numbers exist, but not where the issue
says they should appear.  The third entry is not a #1570 criterion at all; it
is the question #1572 will ask, which this capture cannot answer either.  One
of #1570's acceptance bullets also goes unmet in two of its three asks, and
they are recorded where their numbers are, under "Measured baseline": the
hosted-only sample and first-failure latency.  Its remaining ask, median and
p95 with sample-size limitations, is delivered.  A second bullet is short
rather than absent -- "Reproducing the baseline" below records that six of the
seven baseline tables are derived by hand -- from the per-run JSON, except the
long-wait table's `sampled runs` column, which is not in those files either --
and that the seventh is only partly machine-generated, which #1583 item 17
carries.  Gap 4 below is where a third bullet, "Failed runs preserve useful
timing evidence; missing telemetry never silently represents successful
validation", is half unmet: the first clause holds, but on a run that triggers
gap 4 the tool publishes a wrong number as final, with nothing in the artefact
saying so.  It does not fire on `ci-pr.yml` as it stands, because no two jobs
in it share a name, and it is listed rather than fixed here so that the fix is
reviewed on its own.  Gaps 5 to 9 are limits that would mislead on some run
other than the ones the baseline above was taken from.  #1582 carries the
defect and the two limits that sit in the same walkers, #1583 the remaining
three.  Internal items this list omits go to both: #1582 takes the two named at
the `pr_feedback` limit and the required-checks metric, #1583 the rest.  This
accounting is not claimed to be exhaustive; of everything in this document it
is the part most likely to be incomplete.

1. **Slow tests and cold/warm cache context are not captured.**  #1570's
   scope asks for them.  Both need job logs, and this tool deliberately reads
   only the job and step API, which bounds its request count and keeps it
   offline after `fetch`.  Filed as #1580; #1570 should not be read as having
   delivered this.
2. **The report reaches an operator, not the pull request author.**  #1570
   asks for two things this tool produces but does not deliver:
   machine-readable timing published with a concise job summary, and reports
   preserved after failures.  Both exist for someone who runs the script by
   hand.  Neither reaches a pull request author, because the tool is wired
   into no workflow.  #1577 carries the `workflow_run` publication path;
   until it closes, #1570 should not be read as having delivered those two
   criteria either.
3. **Duplicated compilation is not visible.**  Compile seconds are attributed
   per job, but the API names no translation units, so nothing here separates
   work done twice from work done once.  #1572 gets a direction from this
   baseline and no size, and no capture this tool can take will supply one.
4. **Defect.**  Two jobs with the same name in one run collapse in the
   per-name index and produce no drift entry.  This also defeats the run-wide
   rule in "Critical paths" above: an untimed job whose name is shared with a
   timed one is filtered out as though it were on the chain, and the total is
   published as final.
5. An untimed node weighs zero while the graph walk is *choosing* a
   chain, so the reported chain can be the wrong one.  The tool says so rather
   than fixing it.  The same is true of an unusable edge weight, and only
   on-chain unusable edges are reported.
6. A per-edge release latency uses the dependent's single value for
   every incoming edge, which understates when the path enters through an
   earlier-finishing dependency.
7. An unreadable `--workflow` leaves the critical path reporting "no
   dependency graph was supplied", which is true of what reached the walker but
   not of what the operator passed.  The distinction survives only on stderr.
   Relatedly, a graph that *was* supplied and resolved to no jobs is reported
   correctly by the critical path and incorrectly by each job's release
   latency, because the two derive the fact differently.
8. `upstream_elapsed` is pooled into every aggregate cell and printed by
   neither report.  It also influences which aggregate rows are suppressed.
9. The explanation block groups by status and reason across the whole run,
   not per job, so how it scales depends on the reason.  One confined to a
   single column collapses to a single counted line once more than three
   entries share it, and is itemized below that; one spanning several columns
   is itemized however many there are, because a count there would name
   neither the job nor the number.  A run whose reasons span several columns
   is therefore several lines per job -- bounded and linear, but long.
   Reasons confined to one column, which is the common case, collapse
   instead: the baseline's `run-34477686557.md` reports `configure` on five
   jobs in a single line.

## Reproducing the baseline

```
scripts/ci/ci-run-telemetry.py fetch --out ci-telemetry \
  --run-id 34477686557 --run-id 34484224868 --run-id 34487103833 \
  --run-id 34509006108 --run-id 34514058197 --run-id 34514377419 \
  --run-id 34514413925
scripts/ci/ci-run-telemetry.py report --out ci-telemetry \
  --run-id 34477686557 --run-id 34484224868 --run-id 34487103833 \
  --run-id 34509006108 --run-id 34514058197 --run-id 34514377419 \
  --run-id 34514413925
scripts/ci/ci-run-telemetry.py aggregate --out ci-telemetry --label success-7
```

Fourteen budgeted API requests, two per run.  As printed the sequence also
spends one unbudgeted `gh repo view` to learn the repository, since it passes
no `--repo` and assumes no `GITHUB_REPOSITORY`; set either to keep it at
fourteen.  `ci-telemetry/` is gitignored.  Re-running `report` and `aggregate`
over the saved `raw/` directory needs no network, so the figures above can be
recomputed after the tool changes.

Two things that sequence does not do for you.

`aggregate` pools per-job cells only, over `queue_delay`, `job_wall` and
`upstream_elapsed`.  It emits no run-level statistics, no phase pooling and no
`sum` column anywhere.  So of the seven baseline tables only the runner
placement one falls out of `success-7.md`, and only its `n` and median columns
do; the `ratio` column beside them and the compile ratios below it are derived
by hand.  The other six -- sample, long-wait, run-level, job-lifetime,
critical-path and phase -- are derived from the seven `run-{id}.json` files by
hand, and the long-wait table's `sampled runs` column is not in those files
either: it is cross-run window arithmetic the tool never emits.

`report` reads the dependency graph from the **working tree's** `ci-pr.yml`,
not from each run's head commit, and the seven runs sit at seven different
commits.  The numbers above were produced against `ci-pr.yml` as of this
commit.  Re-running `report` over the same `raw/` if #1574 changes the graph
will yield different critical-path figures for the same runs, signalled only by
`graph_drift` and only where a name actually moved.  Pass `--workflow` with the
file as of the run's commit to compare like with like.

## Self-test

```
meson test -C build --suite abi ci_run_telemetry_selftest
python3 scripts/ci/test-ci-run-telemetry.py          # same tests, directly
```

The fixtures under `scripts/ci/ci-telemetry-fixtures/` are trimmed captures of
real runs -- a success, a skip cascade, which is also the failed-run case, and
a clock-skewed run -- plus hand-written queued, missing-data and re-run cases.
The self-test uses no network, no `gh` and no build directory.
