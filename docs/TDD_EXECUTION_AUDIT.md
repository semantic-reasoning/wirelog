# TDD execution audit (#1378)

TDD here means **Tuple-at-a-time Differential Dataflow**, not test-driven
development. This is an execution-path audit, **not a controlled scalability
benchmark**. No safety predicates, worker-selection policy, or tuple oracles
were relaxed.

## Evidence and reproduction

The machine-readable [inventory](tdd-execution-audit/inventory.json) includes
full stdout/stderr, their hashes, each invocation, per-stratum records, dataset
file manifests, compiler/build options, binary hashes, CPU/environment, and
physical/cgroup memory observations. Each measurement is a fresh process with
`--repeat 1`; no last-trial counters are paired with a multi-trial median.
The instrumented runtime uses commit
`57c36827eb084bd5f008737219ca0e5ee7281531` plus the snapshot-framing patch
embedded in `runtime_patch` (with its own hash), built on the #1376 fixes in
`b47725a9a6141bd52642c5fe14d8ad804f1aea79` (PR #1377).
The recorded host is Linux x86-64, two Xeon E5-2696 v4 sockets (88 logical CPUs),
GCC 16.2.1, release with size optimization and LTO. Twenty portfolio invocations
and two synthetic controls passed; DOOP remains explicitly unmeasured.

```sh
uv venv .venv
. .venv/bin/activate
meson setup build-audit --buildtype=release -Dtests=true -DmbedTLS=disabled
meson compile -C build-audit bench_flowlog bench_flowlog_seq
python scripts/perf/audit-tdd-execution.py --build-dir build-audit \
  --output /tmp/tdd-audit-new --timeout 3600 \
  --defer-doop 'Four long-running DOOP invocations deferred pending runtime budget'
```

The output directory must not already exist. The collector resets inherited
`WIRELOG_*` overrides and enables only the two diagnostic switches for the
portfolio. Separate synthetic controls set `WIRELOG_TDD_MIN_ROWS_PER_WORKER=1`;
that override is **not used for portfolio measurements**. Both binaries compile
their planner/evaluator sources under their respective fusion configuration.
The inventory records actual binary identities and any dirty checkout paths;
the collector checks binary hashes again after measurement.

All measured inputs match
[`downstream-matrix-oracles.tsv`](../scripts/release/downstream-matrix-oracles.tsv).
The collector validates tuple counts and W=1 iteration pins, then compares
iterations between configurations at the **same** worker count. W=8 iteration
counts are not forced to equal W=1. JSON and the legacy TSV output are handled:
`cspa-fast` emits `cspa`, but the separate incremental `--workload cspa` is not
interchangeable. Failed processes, timeouts, missing/duplicate/truncated logs,
wrong aliases, and incorrect results are not classified as TDD rejection.
Snapshot BEGIN records independently count the expected evaluation scope;
successful COMPLETE records confirm the actual count after snapshot completion.
The collector requires ordered decision/profile pairs and validates both counts,
so removing complete trailing pairs cannot leave a falsely verified prefix.
All admission-analysis fields and their boolean/count/fallback domains are
required. Stable zero-work frames and noncontiguous affected-scope frames are
tested, but neither can satisfy this audit's fresh full-evaluation requirement.

## CSPA-fast: exact rejection chain

The unfused lowered SCC contains `valueFlow`, `memoryAlias`, and `valueAlias`.
At its first snapshot, TDD admission sees an eligible snapshot and EXCHANGE
operators, with no unsupported LFTJ. The first alignment failure occurs inside
`idb_idb_join_right_keys_match_exchange()` in
[`eval_tdd_plan.c`](../wirelog/columnar/eval_tdd_plan.c): the coordinator has only
the two loaded EDB relations, `assign` and `dereference`. `valueFlow` has not
been registered, so `session_find_rel(coord, "valueFlow")` returns NULL and the
helper's **missing-schema guard** rejects alignment before comparing key names.
This is a conservative analysis/lifecycle limitation, not evidence that every
join is intrinsically unpartitionable.

There are also independent structural constraints. The following actual
unfused plan excerpt was inspected with GDB against `b47725a9`:

| SCC relation | Lowered operation | Relevant operands |
|---|---|---|
| `valueFlow` | op 8 VARIABLE; op 9 JOIN | `valueFlow`, right `valueFlow`, left `col1`, right `col0` |
| `valueFlow` | op 49 EXCHANGE | `key_col_count=1`, `key_col_idxs[0]=0` |
| `valueAlias` | ops 2–5 | VARIABLE `valueFlow`; JOIN `memoryAlias`; SEMIJOIN `valueFlow`; JOIN `valueFlow` |

The first row implements
`valueFlow(x,y) :- valueFlow(x,z), valueFlow(z,y).`
Even with a schema available, the left access key `col1` does not match the
`col0` ownership key. Matching tuples can belong to different workers; the
aligned asymmetric strategy cannot simply assume the join is local.

The source rule
`valueAlias(x,y) :- valueFlow(z,x), memoryAlias(z,w), valueFlow(w,y).`
has **three source IDB atoms**. Lowering inserts a SEMIJOIN prefilter, making
the maximum counter **four lowered IDB references**. Do not call this four
source atoms. Both counts exceed the current non-aligned BDX limit of two.
The broader safety chain therefore remains:

1. Aligned strategy rejected initially for missing coordinator schema.
2. Non-aligned BDX rejected because `stratum_max_idb_body_atoms(sp)=4 > 2`.
3. Global-read alternative rejected because the SCC has IDB–IDB joins.
4. Snapshot runs the existing recursive evaluator with `unsafe_plan`.

Here `has_idb_self_join` also covers IDB-derived-left/IDB-right combinations
across predicates, not only two occurrences of the same predicate.
The observed decision has `safe=0`, `global_read_candidate=0`, `self_join=1`,
and `idb_atoms=4`. Both configurations produce 20,381 tuples / 6 iterations.
Fixing the schema lookup or discounting the SEMIJOIN alone would not establish
a safe CSPA distribution; this issue does not widen admission.

To inspect the initial state in a symbol-bearing unfused build:

```text
gdb --args build-debug/bench/bench_flowlog_seq --workload cspa-fast \
  --data-cspa bench/data/cspa --workers 8 --repeat 1 --format json
break wl_columnar_session_tdd_plan_stratum
run
print sp->relation_count
print sp->relations[0].ops[9]
print *(wl_plan_op_exchange_t *)sp->relations[0].ops[49].opaque_data
```

At the alignment helper, inspect `coord->nrels` and `coord->rels`: the observed
count is two, with only `assign` and `dereference`. Operator offsets are specific
to the stated commit/configuration, not a stable plan API.

## Inventory summary

Full per-recursive-stratum identities, selected strategies, requested widths,
completed dispatch widths, admission reasons, and replay status are in the
inventory. At W=1 every recursive stratum is snapshot-ineligible by policy;
this says nothing about whether its shape could be distributed at W>1.

| Workload | Recursive strata | W=8 result in both configurations | Tuple count | Iterations W=1 / W=8 |
|---|---:|---|---:|---:|
| CSPA-fast | 1 | `valueFlow`: rejected, `unsafe_plan`; no TDD dispatch | 20,381 | 6 / 6 |
| Galen | 1 | `outP`: rejected, `unsafe_plan`; no TDD dispatch | 5,568 | 23 / 23 |
| Polonius | 12 | 11 admitted then narrowed to W=1; `subset` rejected, `unsafe_plan` | 1,983 | 23 / 25 |
| DDISASM | 3 | `code` rejected; `reachable` and `in_function` owner strategy narrowed to W=1 | 704 | 0 / 19 |
| CRDT | 2 | Owner TDD dispatch at W=8, followed by tiny-frontier serial replay | 2,152,328 | 14,148 / 14,148 |
| DOOP | Not measured | Four long-running invocations deferred pending runtime-budget decision | Not measured | Not measured |

Polonius uses BDX for `known_placeholder_subset`, global-read for
`var_drop_live_on_entry` and `origin_contains_loan_on_entry`, and owner strategy
for the other eight admitted strata. All eleven select only one worker on this
input. DDISASM's W=1 zero is its existing iteration-reporting convention, not a
claim that no computation occurred. Same-width configuration comparisons are
recorded separately rather than normalizing these differences away.

DOOP is **not classified as unsafe, OOM, or measured**. Available physical RAM
was not established as insufficient. Repository notes describe roughly 40 GB
RAM for the full input; this is not a guarantee of peak capacity. A full run
requires acquiring/verifying the pinned zxing facts, sufficient effective
memory (including cgroup limits and intermediates), and time for four separate
invocations. Use the existing `bench/data/doop/download.sh` acquisition workflow
and `--include-doop` in place of `--defer-doop` when those conditions are arranged.
This report does **not** claim that all six workloads were fully measured.

## Interpreting execution and costs

`use_tdd` and `fallback` describe **initial admission**. `strategy` and
`selected_workers` describe later selection. `submitted_tasks` counts successful
submissions; `completed_rounds` counts barriers reached after all selected tasks
were submitted. `dispatch_width` is that logical width when at least one barrier
completed, otherwise zero. These fields do not prove simultaneous execution on
eight distinct OS threads. Partial submission followed by caller-side drain
must not become a completed parallel round; the regression test forces that
path. Final `rc`, `replay`, and triggering `replay_rc` remain separate.

CRDT demonstrates why admission counters alone were insufficient: its
`nextSiblingAnc` and `value_blank_star` strata dispatch owner-mode work, then
the existing policy detects a small accepted frontier after at least 31
iterations and restarts each stratum in the sequential evaluator. The final
stratum wall time includes the attempted TDD work and serial completion. It
cannot be reported as an entirely parallel evaluation or a useful speedup.

Concrete unfused W=8 CRDT observations (milliseconds, except row/round counts):

| Stratum | Completed barriers | Attempted TDD | Total including replay | Exchange | Wait | Worker-local delta rows |
|---|---:|---:|---:|---:|---:|---:|
| `nextSiblingAnc` | 45 | 139.845 | 25,858.282 | 35.095 | 89.134 | 69,486 |
| `value_blank_star` | 32 | 198.994 | 16,396.673 | 95.761 | 95.092 | 856,359 |

The observed totals are dominated by completion after replay, not by the measured
initial TDD exchange alone. Worker subpass min/max/sum were 0.951/2.702/579.673 ms
and 0.796/55.708/676.236 ms, respectively. Those ranges warrant finer per-worker
investigation, but are not enough to attribute skew to a particular partition.
Unfused process peak RSS was 127,184 KiB at W=1 versus 326,024 KiB at W=8:
increased intermediate/worker state was observed, not an assumed eightfold copy.
Default-configuration observations and all raw values are retained in the inventory.

The two separate `tdd-bdx` controls show successful width-eight dispatch in
both configurations with 4,950 tuples. C regression tests additionally check
the complete, duplicate-free 5,050-tuple closure for the 100-edge inline chain.
This disproves the claim that TDD inherently requires K-Fusion, but is not
evidence that forcing width eight helps the six portfolio inputs.

Measurement boundaries:

- Worker min/max are **individual worker-subpass extrema**, not accumulated
  per-worker lifetime distributions. Worker sum includes all completed-barrier
  subpasses. Unequal extrema alone do not prove persistent worker skew.
- `worker_delta_rows` counts worker-local deduplicated deltas after queue
  reconstruction and before exchange. It is neither raw JOIN candidates nor
  globally accepted tuples nor transmitted bytes; repeated proposals can occur.
- `wait_ms` includes computation while waiting. It is not pure removable
  synchronization overhead. Worker/exchange/parent timers overlap and must not
  be added together. Synthetic JSON controls retain existing exchange phase
  breakdowns; legacy portfolio TSV does not expose all those sub-counters.
- RSS is process peak memory. Arrangement build time/bytes, raw JOIN candidates,
  globally accepted row totals, and byte-level exchange volume are not separately
  exposed by this audit. They are **unavailable**, not zero. Shared relation
  views preclude assuming memory always grows by W.
- Single diagnostic runs on a shared host do not separate scheduling noise from
  useful scaling. No README timing claims or performance baselines are changed.

## Follow-up boundaries

The evidence supports separating three investigations, not a general removal
of barriers or safety guards:

- **Analysis/support:** consider plan-metadata-based alignment checks, with
  aligned-schema-absent controls, separately from supporting more-than-two-IDB
  non-aligned joins. CSPA still needs a distribution that proves all delta
  combinations, ownership, deduplication, and termination.
- **Adaptive policy:** Polonius/DDISASM mainly need representative input sizes
  before changing the default worker threshold. CRDT needs tiny-frontier and
  replay-cost evidence before revising the existing owner fallback.
- **Runtime/memory:** arrangement and accepted-row/byte telemetry plus full DOOP
  measurements remain gaps. Coordinate memory work with #1367/#1373 and K-Fusion
  retirement with #1375; README scalability claims belong to #1361.

These are explicitly bounded follow-up needs, not optimizations implemented or
performance improvements claimed by #1378.
