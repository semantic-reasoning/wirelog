# Offline paired campaign v1

`evaluate-paired-campaign.py campaign.json` reads one immutable JSON document
and prints a deterministic descriptive report. It performs no builds, benchmark
execution, runner access, workflow operations, or GitHub API calls. Existing
`metadata.json`/`attempts.jsonl` collector output is not this schema and is
rejected. No performance threshold or performance pass/fail is defined.

The executable validator is the versioned schema. Unknown fields are rejected
on every fixed record; the variable maps below are explicitly identified.
`test-evaluate-paired-campaign.py:fixture()` is a complete executable example.
All integers exclude booleans, and all numbers must be finite. JSON duplicate
keys and NaN/Infinity constants are rejected by the CLI.

## Document

Required fields: `schema_version` (integer 1), nonempty `campaign_id` string,
`campaign_mode`, `manifest`, `blocks`, and `attempts`.
`campaign_mode` is exactly `comparison` or `aa_control`. Comparison requires
different source SHAs; A/A requires equal source SHAs. Both require distinct
nonempty build instance IDs. Equal binary hashes are allowed. Provenance is
recorded evidence, not proof that builds were independently executed; the next
collector unit must retain separate build logs.

`manifest` has exactly these fields:

| Field | Contract |
|---|---|
| `sources` | `base` and `candidate`: lowercase full 40 hex source commit IDs |
| `profiles` | Each side: nonempty variable map of resolved setting names to nonempty strings; side maps equal |
| `build_provenance` | Each side: `build_instance_id`, `source_sha`, `profile`, `build_log_sha256`; source/profile equal corresponding manifest values, log digest lowercase 64 hex |
| `host_id` | Nonempty immutable host identity string |
| `cpu` | Nonnegative integer pinned CPU |
| `workloads` | Nonempty variable map of workload names to workload specifications |

Each workload has exactly `binary_sha256` (side map of lowercase 64 hex hashes),
`fixture_sha256` (side map of nonempty variable fixture-name/hash maps, equal
between sides), `timer` (`id`, `unit`, `scope`, all nonempty strings, unit `ms`),
and `expected` (nonempty variable correctness metric/nonnegative integer map).
The collector must resolve the full build profile and exact timed region before
collection. Different workload timers may describe different benchmark probes.

`blocks` contains exactly two `{block_id, order}` objects: distinct nonempty IDs
and one `AB` plus one `BA`. A means base and B means candidate. Each block uses
that fixed order in all nine adjacent pairs. Campaign/manifest identity never
changes across blocks.

Each attempt has exactly:

| Field | Contract |
|---|---|
| `campaign_id`, `manifest_sha256` | Exact campaign ID and SHA256 of manifest JSON serialized with sorted keys, compact separators, default Python ASCII escaping, no nonfinite numbers |
| `block_id`, `workload` | Existing block/workload names |
| `sequence` | Integer 0 through 19, unique per block/workload |
| `phase`, `pair_index` | Sequence 0/1: `warmup`, null; sequence 2..19: `sample`, integer `(sequence-2)//2` |
| `side` | For even/odd sequences, base/candidate in AB or candidate/base in BA |
| `elapsed_ms` | Positive finite number, including warmups |
| `exit_code`, `timed_out` | Integer process exit code, boolean timeout indicator |
| `correctness` | `{status, observed}`; status `OK`/`FAIL`; observed has exactly expected metric keys and nonnegative integer values |
| `host_before`, `host_after` | Structural telemetry described below |

Input array order does not define execution order: immutable `sequence` does.
The collector must assign sequence at actual execution and serialize runs. A
missing sequence produces `INCOMPLETE`; duplicate/out-of-range/extra indices,
wrong side/pair/phase or identity drift produce `INVALID_EVIDENCE`. Each workload
and block requires both warmups and exactly nine timed pairs.

Telemetry has exactly `load_one`, `cpu_pressure_some_total_us`,
`cgroup_throttled_us`, `frequency_khz`, `governor`. Every metric is an object with
exactly `value` and `unavailable_reason`. Available values require null reason;
numeric values are nonnegative finite (`load_one` accepts integers/floats, other
numeric metrics require integers), governor is a nonempty string. Missing data
requires null value and a nonempty reason. No string to number coercion occurs.
Missing telemetry remains in the report and does not invalidate timing data.

## Results and exits

| Status | CLI exit | Meaning |
|---|---|---|
| `COMPLETE_VALID` | 0 | All structural, process and correctness checks valid, complete paired data |
| `CORRECTNESS_FAILURE` | 1 | Structurally valid observed failure, timeout, nonzero exit or unexpected correctness result; suppress timing statistics |
| `INVALID_EVIDENCE` | 2 | Schema/identity/data validation failure; suppress timing statistics |
| `INCOMPLETE` | 3 | Structurally valid attempts have missing required sequence entries; suppress timing statistics |

Read/JSON parse failures also exit 2 with a diagnostic on stderr. Validation
precedes correctness classification; observed correctness failures precede
incompleteness. Reports identify schema/evaluator versions, original byte input
SHA256, manifest digest/provenance, and available telemetry. There are no current
timestamps. Invalid input cannot be used to claim performance success.

For each side/block/workload: median elapsed milliseconds and population CoV
percentage (`100*pstdev/mean`) are descriptive. For each adjacent pair, the
oriented log ratio is `log(candidate_ms)-log(base_ms)` for either order. Reports
include all nine ratios, their median and mean, and the order effect
`mean_log_ratio_AB - mean_log_ratio_BA`. Negative ratios mean candidate faster.
Synthetic equal-time A/A produces exactly zero ratios and order effect. The
report does not estimate confidence intervals or validate a speedup claim.

## Collector migration for the next unit

| Existing collector evidence | Campaign v1 |
|---|---|
| `metadata.source_sha` | `manifest.sources` |
| `metadata.binary_sha256` | Per workload `binary_sha256` |
| `metadata.fixture_sha256` | Per workload fixture maps; select applicable fixture files |
| `metadata.cpu` | `manifest.cpu` |
| `metadata.first`, sample alternating order | Replace with campaign block descriptors and fixed AB/BA block order |
| Attempt `index`, `phase`, `side`, `workload` | Explicit `sequence`, `pair_index`, `phase`, `side`, `workload` |
| Attempt `parsed.elapsed_ms` | `elapsed_ms` with exact probe timer identity |
| Attempt parsed tuple/iteration/status | `correctness.observed/status`, workload `expected` |
| Attempt `exit_code` or `timeout_seconds` | Explicit integer exit code and boolean timeout; failed attempts still need typed evidence |
| Attempt raw `host_before/after` strings | Parse structural telemetry; null plus explicit unavailable reason |

New collector fields: campaign mode/ID, block IDs, immutable manifest digest on
every attempt, full resolved profile for both sides, host identity, timer ID and
scope, distinct build instance IDs and separate build log digests/provenance.
Old raw stdout/stderr, commands, binary paths, data roots and host timestamps can
remain in companion raw artifacts; they are not accepted as extra v1 fields.
Malformed/failed process outputs that cannot supply valid timings/correctness
must remain raw evidence and produce invalid/incomplete v1 evidence rather than
invented numeric values. The collector migration and workflow integration are
separate work; this unit changes neither existing collection nor any gate.

## Explicit v1 collection

The legacy `paired-benchmark.py --base-sha ...` invocation retains its existing
CLI and evidence format. Select the v1 adapter explicitly:

```sh
python scripts/perf/paired-benchmark.py v1 \
  --mode comparison --base-sha FULL_SHA --candidate-sha FULL_SHA \
  --base-source /checkout/base --candidate-source /checkout/candidate \
  --base-build /build/base --candidate-build /build/candidate \
  --base-build-log /logs/base.log --candidate-build-log /logs/candidate.log \
  --cpu 2 --timeout 180 --out-dir /evidence/new-campaign
```

Use `--mode aa_control` for equal source SHAs and independent builds. Separate
build log paths and different log hashes are required; originals are copied
into the evidence directory. Builds must expose Meson introspection files.
Preflight resolves all build options, checks compiler/profile/fixture and
benchmark timer source equality, and records exact commit/tree IDs and artifact
hashes in `preflight.json`. Bool, list, string and integer options use compact
JSON strings in the v1 profile. Build IDs identify distinct collection-side
instances; logs and recorded provenance remain evidence, not proof of an
independent build execution.

The entire fixed AB then BA schedule is durable before the first launch. Each
block and workload has two warmup launches followed by nine adjacent pairs:
80 serialized launches total. `--timeout` is 1..3600 seconds per launch; the
preflight records the theoretical sum of launch timeout budgets (80 times the
selected timeout), excluding collector/telemetry overhead. Timeout terminates
the whole process group. Benchmark failures never shorten the schedule.

`raw-attempts.jsonl` is append-only and fsynced after every launch, with raw
commands, output and timestamped host readings. Interrupted active launches
are killed and journaled before propagating the interruption. Positive typed
timings with complete correctness metrics become v1 attempts, including
nonzero exits and incorrect results. Unparseable output leaves a sequence
absent. No reruns, replacements, trimming, or performance thresholds exist.
Unavailable host metrics carry explicit reasons.

After collection, all source/build/fixture/log artifacts are checked for drift;
drift writes `collection-status.json` and prevents campaign publication.
`campaign-v1.json` is published atomically, then the existing offline evaluator
writes `evaluation-report.json` and `collection-status.json`. Its exit status
is returned. Existing output directories are refused, including interrupted
campaign directories. The checked-in runner workflow still uses the legacy
collector; workflow migration is a separate unit.

The v1 adapter requires a single-threaded Linux collector process. During
collection, main-thread library calls temporarily handle SIGTERM and SIGHUP
and restore the caller's handlers in `finally`. Signals are blocked only across
`Popen` and process assignment; the child restores the inherited prior mask
before exec. Process group cleanup targets only the launched PID as PGID,
sends TERM with a 0.5-second grace, then KILL, and bounds subsequent reaping to one second. Each launch redirects stdout and
stderr into binary temporary files opened before spawning. After cleanup the
files are flushed, fsynced and read once with UTF-8 replacement decoding; an
asynchronous signal cannot discard process output held in a pipe reader's
local buffer. Secondary cleanup failures stay in raw
evidence without replacing the original interruption. SIGTERM/SIGHUP records
include numeric/name signal identity, partial output and host readings; the
journal is fsynced once before unwinding. Library calls propagate the
termination request. The CLI restores the default handler and signals itself,
so supervisors receive the original signal termination status. Interrupted
collection does not publish campaign or evaluation JSON.

Cleanup records ordinary wait/reap errors as diagnostics. Termination requests
and keyboard interrupts propagate through cleanup into the active-launch
journal path. If the first termination signal arrives during recovery from
another interruption, the signal remains the effective control interruption;
the original primary cause and its type are retained alongside it. Cleanup is
retried idempotently before capture and journal fsync, and KILL is attempted even
if an interruption ends the TERM grace period.
