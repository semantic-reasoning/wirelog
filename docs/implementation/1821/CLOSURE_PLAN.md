# Closing #1821: serial evaluation enforcement

This plan tracks closure of #1821; it does not declare the issue complete.
The initial filtered-observer audit used `9773f94d`. The dispatch-family and
configuration inventory is now audited at `0d91d02a` (2026-10-26) in
[SERIAL_COVERAGE.md](SERIAL_COVERAGE.md). Historical #2144 checkpoint claims are
not evidence that a path is reachable on this base.

Issues: [serial enforcement #1821](https://github.com/semantic-reasoning/wirelog/issues/1821)
and [parent #1817](https://github.com/semantic-reasoning/wirelog/issues/1817).

## Ownership and counting

The parent assigns attempt recovery to #1820, serial operator enforcement to
#1821, parallel/global worker enforcement to #1822, installed public APIs to
#1823, and product qualification to #1824. This plan does not move those other
issues into #1821. Serial retry/publication tests must still demonstrate the
#1820 contract; an unavailable recovery prerequisite remains a recorded blocker.
Closing #1821 does not close #1817 or enable the public feature by itself.

There are **five work packages below, not five promised PRs**. PR #2271 started
the coverage contract and advanced scalar observed composition. This follow-up
completes the dispatch-family inventory and groups its open obligations into
seven implementation/qualification groups. These are not a promised PR count:
differential recovery and representation work still need design sizing. Each
later PR must close named matrix rows, not merely add another isolated plan spelling.
Changes in package scope and any split into additional PRs must be recorded
here with their reason before the additional implementation begins.

## Acceptance map

Status terms: **implemented evidence** means specific implementation and tests
exist, not whole-issue acceptance; **qualification gap** means implementation
exists but the complete route/configuration claim is unproven; **unsupported**
means the current controlled admission deliberately refuses it. Source review
and fresh test results are recorded separately.

| #1821 acceptance item | Evidence on the inspected base | Remaining work / exit condition |
|---|---|---|
| Large product and keyed JOIN, including rejected/no-output candidates | Implemented evidence: `join.c`; `evaluation_join`, `evaluation_work`; `test_entry_generated_right_filter_collisions_and_stop` and `test_entry_generated_right_filter_product_work_boundary` in `evaluation_serial` | Qualify required dispatch/configuration edges and preserve distinctions between work, memory, overflow, and output-limit errors. |
| FILTER/scan, recursive, differential, batch/pipeline, LFTJ paths actually interrupted | Mixed: generated FILTER/LFTJ tests and controlled batch-producer fixtures exist; recursive STEP admits exact one/two-JOIN binary path manifests. `col_controlled_step_static_preflight` and controlled `eval_plan.c` refuse active differential/retraction state | Connect the required serial host routes and prove each real path using deterministic checkpoints; raw correctness tests such as `diff_join` alone are insufficient. |
| Cancellation within the declared quantum | Implemented checkpoint helpers and phase-specific stop fixtures; the [work table](../../SEMANTICS.md#required-downstream-work-metric-and-checkpoint-placement) specifies maximum quantum 256 | Complete per-route checkpoint and finite-region inventory, including engine-owned preflight, allocation, cleanup, and fallback work. Record host/library exclusions explicitly. |
| Exact and one-short limits, reproducible work | Implemented recursive source-fixed ABI-aware frame/descriptor formulas and primitive boundary tests | Identify missing independent boundaries per required route. Measured budgets in observer retry tests are empirical allowances, not independent cost oracles. |
| Reset/retry, canonical output, signed deltas and retained resources | Restricted recursive append and unary observed insert/remove tests exist; source-access and cleanup receipt tests cover ownership primitives | Complete required serial composition/differential recovery and resource-baseline checks. Primitive success does not establish whole-session recovery. |
| Complete path/checkpoint table | Dispatch-family/configuration inventory is complete in SERIAL_COVERAGE.md; whole-route polling and fallback qualification remain incomplete | Resolve every required row below with named actual-path tests, polling bound, retry ownership, and a justified disposition. Temporary ENOTSUP is not completion for a required supported configuration. |
| Normal + ASan/UBSan + disabled-control references | Registered `evaluation_*`, `join_batch_resume`, `join_pipeline`, `diff_join`, and LFTJ suites exist | Run final-commit serial qualification and compare disabled-control results. Old passing runs cannot qualify unsupported routes or changed code. |

Source anchors: [session dispatch](../../../wirelog/columnar/session.c),
[plan dispatch](../../../wirelog/columnar/eval_plan.c),
[framed serial entry](../../../wirelog/columnar/eval_serial.c),
[registered suites](../../../tests/meson.build),
[serial fixtures](../../../tests/test_evaluation_serial.c).

## Coverage contract completed for the audited dispatch families

[SERIAL_COVERAGE.md](SERIAL_COVERAGE.md) enumerates 13 entry/scheduling edges,
all 12 plan operator kinds, 10 fallback/representation edges and seven
configuration axes. Each has required/sibling/callback ownership, current
reachability, local checkpoint or explicit unknown bound, evidence or a
missing-evidence obligation, and an implementation group. This completes the
inventory deliverable; it does not resolve the recorded enforcement gaps or
prove every internal branch combination.

| Original family | Concrete matrix rows |
|---|---|
| S01 source/materialization | O01, F07, C05 |
| S02 scalar expressions/aggregates | O02/O03/O06, C01/C02 |
| S03 JOIN | O04, F01/F05 |
| S04 SEMIJOIN/ANTIJOIN | O05/O09, C07 |
| S05 consolidation/union | O07/O08, F07/F08 |
| S06 LFTJ | O11, F06 |
| S07 recursion | E05/E06, O10/O12, F08 |
| S08 differential/retraction | E07, F04 |
| S09 batch/pipeline | E08/E09, F01–F03, C04 |
| S10 observed signed publication | E03/E04, F10 |
| S11 preflight/resource lifecycle | E01/E02/E03/E11/E13, F09 |
| S12 snapshot | E10, F10 |
| S13 configurations | E12, C01–C07 |

**G1: bounded session admission and completed-snapshot engine work** is
implemented in this PR bundle as two atomic commits. Observed admission and
successor-name setup use charged cursors. Completed snapshots stage their
callback-ready tuples under work and memory admission, then deliver a finite
prepaid batch after the cutoff. Callback cooperation does not exclude engine
preparation. **G2: signed observed composition** is next. G2–G7 retain the
recursive/differential, host batch/LFTJ, scalar/representation, resource/config
and final qualification obligations and their #1820 dependencies. These are
six remaining work groups, not a promise of six remaining PRs.

## Work packages and exit criteria

1. **Coverage contract (inventory complete at `0d91d02a`).** Every audited
   dispatch family/configuration axis has a matrix disposition and an open-work
   group where needed. Nine new admission cases distinguish refusal from real
   execution. Implementation and unknown-bound obligations remain open in
   G2–G7; further scope splits must be recorded before implementation.
2. **Scalar observed composition.** Batch related required scalar/typed,
   multi-column and multihead host gaps, reusing metered operators and signed
   baseline/current preparation. Exit: every required S01–S05/S10 combination
   has actual-path stops, no premature/replayed publication, canonical retry
   and resource evidence. The optional unary FILTER and its retained-baseline
   stop/retry repair were delivered by PR #2271. Repeated removals still require
   bounded `$r$input` staging retirement/reuse coordinated with #1820.
3. **Serial recursion/differential.** Complete required S07/S08 SCC/frontier,
   trace and retraction paths while retaining one allowance and the #1820
   baseline contract. Exit: real differential/retraction and recursive paths
   interrupt/retry correctly. This is the largest scope uncertainty; record a
   split if recovery and enforcement cannot form one reviewable PR.
4. **Batch/pipeline/configuration closure.** Connect required S06/S09/S13
   producers, sinks and fallbacks without resetting/refunding the allowance on
   resume/replay. Exit: every required configuration has actual-path bounded
   work, correct error distinctions and resource ownership. Do not import the
   separate #1822 worker-global implementation.
5. **Final serial qualification.** Resolve remaining S11/S12 serial obligations,
   reconcile historical scope prose, and run native/sanitizer/reference checks
   on the exact final commit. Exit: all seven acceptance rows and all required
   matrix edges have evidence, with no temporary unsupported required path.
   Then close #1821; parent/product qualification remains separately owned.

## Historical evidence: PR #2271 (2026-10-25)

- [x] Source-audit baseline, seven acceptance rows and ownership boundaries.
- [x] Initial S01–S13 inventory and five packages with explicit exit criteria.
- [x] Exact four-operation filtered observer admission and bounded successor copy.
- [x] Parsed-path, canonical/reference and signed successor tests.
- [x] All-rejected scan cancellation/work exhaustion, retained-baseline retry,
  foreign-baseline refusal, and stopped-attempt teardown tests.
- [x] Native and ASan/UBSan validation of this increment; formatting and LLVM 22
  clang-tidy ratchet checks.

`test_controlled_observed_filter_successors` compares each single mutation
with a disabled-control session and checks signed events and the staging-cap
refusal. `test_controlled_observed_filter_stops` checkpoints phase 13 in the
rejected scan, verifies baseline identity during reset/retry, and covers
checked teardown after cancellation or work-limit cancellation acknowledgment.
The preflight rejection fixture also rejects an unrelated existing observer.

Publication requires independent raw-diff review and final design/risk approval
for the exact candidate. Record those approvals and the committed tree in the
PR/workflow report; the checklist above is implementation evidence, not a
claim that this increment closes #1821.

Local validation (2026-10-25): native `evaluation_serial`,
`evaluation_observer_build`, `session_admission`, `session`, `evaluation_retry`,
and `evaluation_snapshot_cutoff`; ASan/UBSan runs of the first four;
`state_map_anchors`, `state_map_anchors_selftest`, `threading_doc`, and
`threading_doc_selftest`; LLVM 22 clang-tidy ratchet and uncrustify checks.
These qualify this increment only. The complete suite, MSVC, and remaining
serial matrix configurations are not claimed by this local run.

## Coverage-contract follow-up (2026-10-26)

- Completed the current dispatch-family/configuration-axis ledger with explicit
  unsupported required routes and unknown engine-work bounds.
- Added `test_controlled_session_admission_matrix`: nine static/charged host
  refusals, no frame execution or side effects, then successful execution and
  snapshot after restoring the supported configuration.
- Preserved existing positive recursive, observed, batch and snapshot fixtures;
  private LFTJ tests are explicitly not reported as session reachability.
- Selected G1 as the next implementation milestone and recorded seven remaining
  groups with exit tests rather than inventing a fixed remaining PR count.

Local validation: native `evaluation_serial`, `evaluation_observer_build`,
`session_admission`, `session`, `evaluation_retry` and
`evaluation_snapshot_cutoff` passed; ASan/UBSan passed the first three.
Documentation gates `state_map_anchors`, `state_map_anchors_selftest`,
`threading_doc` and `threading_doc_selftest`, cited fixture/link checks,
uncrustify and whitespace checks passed. Other matrix tests were inspected,
not freshly run. This changes documentation and admission tests only; no
production dispatch or policy changed. Full-suite, MSVC and optional builds
remain unclaimed.

## G2 implementation bundle (2026-10-26)

The next bundle groups four atomic units: bounded observer preparation,
owned reusable mutation staging, coordinated multihead publication, and
integrated qualification. It targets ordinary nonrecursive compositions of
already controlled scalar operators, including multiple inputs and heads.
Scalar row identity remains the raw observer's native-byte identity; FLOAT
signed zero and NaN payloads do not acquire new equality rules. Intended scalar
types are INT32, INT64, UINT32, UINT64, FLOAT, STRING and BOOL. Compound,
timestamped and spill representations and new expression families remain G5;
recursive and differential execution remains G3. These are planned obligations,
not implemented coverage claims.

A transaction must prepare one coordinated registry image and all old-head
retirement claims before its publication cutoff. Independent registrations
cannot reserve slots independently against the same registry. Failed attempts
retain the original public heads and notification baseline; cleanup after
publication must not replay events. Callback replacement/clearing retains its
existing semantics. G2 closes only when repeated mutation cycles, typed/wide
rows and dependent/independent multiple heads pass together through session
entry points with deterministic stops, reference results and resource checks.
