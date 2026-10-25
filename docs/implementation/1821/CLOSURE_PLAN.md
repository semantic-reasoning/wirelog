# Closing #1821: serial evaluation enforcement

This plan is part of the filtered-observer implementation, not a declaration
that #1821 is complete. Source inspection baseline: `9773f94d`. Historical
#2144 checkpoint claims are not evidence that a path is reachable on this base.

Issues: [serial enforcement #1821](https://github.com/semantic-reasoning/wirelog/issues/1821)
and [parent #1817](https://github.com/semantic-reasoning/wirelog/issues/1817).

## Ownership and counting

The parent assigns attempt recovery to #1820, serial operator enforcement to
#1821, parallel/global worker enforcement to #1822, installed public APIs to
#1823, and product qualification to #1824. This plan does not move those other
issues into #1821. Serial retry/publication tests must still demonstrate the
#1820 contract; an unavailable recovery prerequisite remains a recorded blocker.
Closing #1821 does not close #1817 or enable the public feature by itself.

There are **five work packages below, not five promised PRs**. This PR starts
the coverage contract and advances scalar observed composition. There is no
evidence-based remaining PR count yet: the dispatch/configuration inventory
must first resolve the required unsupported edges. Each later PR must close a
named group from that inventory, not merely add another isolated plan spelling.
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
| Complete path/checkpoint table | Qualification gap: prose and local tariffs exist, but no completed issue-wide dispatch/fallback table | Resolve every required row below with named actual-path tests, polling bound, retry ownership, and a justified disposition. Temporary ENOTSUP is not completion for a required supported configuration. |
| Normal + ASan/UBSan + disabled-control references | Registered `evaluation_*`, `join_batch_resume`, `join_pipeline`, `diff_join`, and LFTJ suites exist | Run final-commit serial qualification and compare disabled-control results. Old passing runs cannot qualify unsupported routes or changed code. |

Source anchors: [session dispatch](../../../wirelog/columnar/session.c),
[plan dispatch](../../../wirelog/columnar/eval_plan.c),
[framed serial entry](../../../wirelog/columnar/eval_serial.c),
[registered suites](../../../tests/meson.build),
[serial fixtures](../../../tests/test_evaluation_serial.c).

## Finite inventory to complete

This is the initial required inventory, not a claim that every internal edge
has already been audited. For each row, package 1 must enumerate concrete
subpaths/configurations, then record checkpoint, maximum unpolled work,
independent or empirical budget evidence, owner on stop, retry test, and final
required/excluded disposition. Add discovered edges to the owning row rather
than silently enlarging the implementation scope.

| ID | Serial route family | Current disposition and evidence entry point |
|---|---|---|
| S01 | Source/VARIABLE scans and relation materialization | Implemented primitives; `evaluation_copy`, `evaluation_relation_creation`; qualify each host entry and representation. |
| S02 | Numeric/typed FILTER, MAP, expression and REDUCE | Implemented restricted numeric paths; `evaluation_numeric_operators`, `evaluation_numeric_plan`, generated FILTER/MAP/REDUCE fixtures; remaining type/expression configurations need disposition. |
| S03 | Ordinary/keyed JOIN and right predicates | Implemented evidence in `evaluation_join` and generated right-filter fixtures; cover rejected and colliding candidates and error policies. |
| S04 | SEMIJOIN/ANTIJOIN and membership fallbacks | Restricted private scalar coverage in `eval_plan.c` and serial fixtures; required predicate/fallback combinations need qualification. |
| S05 | CONSOLIDATE, sort/dedup, CONCAT, UNION | Metered private composition exists; `evaluation_consolidate`, `evaluation_serial`; verify required retained/owned/segmented and typed routes. |
| S06 | LFTJ, cache/build and selected fallback | Generated LFTJ fixtures prove bounded child/reader/allocation work; enumerate remaining fallback branches and host admission. |
| S07 | Serial recursive SCC/frontier scheduling | Restricted one/two-JOIN path manifests only; general required SCC/multihead/frontier routes remain unsupported or unqualified. |
| S08 | Serial differential JOIN, trace and retraction | Active differential/retraction admission is refused; raw `diff_join` tests do not prove controlled interruption. Requires implementation and #1820 recovery coordination. |
| S09 | Batch producer, pipeline, suspension/replay | Controlled producer primitives exist; session materialized fallback is exact VARIABLE/JOIN/FILTER/MAP/CONSOLIDATE and strict batching is refused. Qualify required shared-attempt producer/sink and fallback configurations. |
| S10 | Observed evaluation and signed publication | Unary identity route exists; this PR adds one optional FILTER. Required typed/multi-column/multihead composition remains unqualified. Removal staging can consume registry slots and cause the next mutation to hit the existing three-slot refusal; bounded retirement/reuse remains a recovery gap. |
| S11 | Session preflight, registry and cleanup | Bounded nonobserver preflight and cleanup helpers exist. Audit observed static scans/name lookups and engine-owned allocation/cleanup regions; do not infer boundedness from a restricted operator list. |
| S12 | SNAPSHOT serial handoff and publication | `col_session_snapshot_controlled` delivers an already completed STEP after a final poll. It does not evaluate fresh/dirty state. Audit engine-owned delivery/precharge separately from cooperative host callbacks; coordinate evaluating lifecycle with #1820. |
| S13 | Optional serial builds/configurations | Enumerate supported threading, batch, expression/library and fallback configurations; validate actual dispatch, or document why an exclusion is allowed by #1817. Worker execution belongs to #1822. |

## Work packages and exit criteria

1. **Coverage contract (starts in this PR).** Expand S01–S13 into the complete
   finite dispatch/configuration/fallback matrix, with evidence and explicit
   exclusions. Exit: no unclassified required edge; a defensible remaining PR
   grouping/count and recovery dependencies can then be stated. This PR only
   starts this inventory and does not mark that exit condition satisfied.
2. **Scalar observed composition.** Batch related required scalar/typed,
   multi-column and multihead host gaps, reusing metered operators and signed
   baseline/current preparation. Exit: every required S01–S05/S10 combination
   has actual-path stops, no premature/replayed publication, canonical retry
   and resource evidence. The optional unary FILTER and its retained-baseline
   stop/retry repair are this PR's subitems. Repeated removals still require
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

## This PR's checklist and evidence

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
