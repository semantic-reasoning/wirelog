# Closing #1821: serial evaluation enforcement

This plan tracks closure of #1821; it does not declare the issue complete.
The initial filtered-observer audit used `9773f94d`. The dispatch-family and
configuration inventory is now audited at `0d91d02a` (2026-10-26) in
[SERIAL_COVERAGE.md](SERIAL_COVERAGE.md). Historical #2144 checkpoint claims are
not evidence that a path is reachable on this base. G2's implementation audit
below uses `a76b8ff5`; the inventory's other groups retain their recorded gaps.

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
| Reset/retry, canonical output, signed deltas and retained resources | Restricted recursive append and general ordinary observed insert/remove tests exist; multihead reference, retry and governor-baseline tests accompany source-access and cleanup receipts | Complete required serial composition/differential recovery and resource-baseline checks. Primitive success does not establish whole-session recovery. |
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
merged in PR #2272 as two atomic commits. Observed admission and
successor-name setup use charged cursors. Completed snapshots stage their
callback-ready tuples under work and memory admission, then deliver a finite
prepaid batch after the cutoff. Callback cooperation does not exclude engine
preparation. **G2: signed observed composition** is implemented in this bundle
for ordinary nonrecursive compositions of already controlled scalar operators.
**G3a: ordinary recursive SCC transactions** follows G2 in this bundle;
**G3b: differential trace/arrangement and retraction execution** remains next. G3–G7 retain the host
batch/LFTJ, new scalar/expression/representation, resource/configuration and
final qualification obligations. These are five remaining work groups, not a
promise of five remaining PRs; #1821 remains open.

## Work packages and exit criteria

1. **Coverage contract (inventory complete at `0d91d02a`).** Every audited
   dispatch family/configuration axis has a matrix disposition and an open-work
   group where needed. Nine new admission cases distinguish refusal from real
   execution. Implementation and unknown-bound obligations remain open in
   G3–G7; further scope splits must be recorded before implementation.
2. **Scalar observed composition.** Batch related required scalar/typed,
   multi-column and multihead host gaps, reusing metered operators and signed
   baseline/current preparation. Exit: every required S01–S05/S10 combination
   has actual-path stops, no premature/replayed publication, canonical retry
   and resource evidence. The optional unary FILTER and its retained-baseline
   stop/retry repair were delivered by PR #2271. This G2 bundle connects ordinary
   multiinput/multihead composition and retires consumed staging. New expression
   families and representation variants remain G5; cross-policy qualification
   remains G6. Host admission does not broaden a child operator's supported domain.
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
with a disabled-control session. The historical increment checked signed
events and the then-existing staging-cap refusal; G2 extends it to repeated cycles. `test_controlled_observed_filter_stops` checkpoints phase 13 in the
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

Four atomic units remain in one PR: bounded observer preparation, owned reusable
mutation staging, coordinated multihead publication, and integrated qualification.
Production code is audited at `a76b8ff5`. The bundle closes the ordinary observed
host-composition obligations in E03/E04/F10; it does not close #1821 or certify
all child expression/representation and configuration combinations.

The observer copies, normalizes, subtracts pending rows and compares native
scalar rows through charged cursors. Its scalar schema domain is INT32, INT64,
UINT32, UINT64, FLOAT, STRING and BOOL. Native-byte identity is unchanged,
including FLOAT signed-zero and NaN payloads. Child operator validation remains
authoritative. New expressions, compound/timestamped/spill representations,
recursion, differential execution and batch/LFTJ hosting remain G3–G6.

The ordinary session transaction uses dynamically sized authenticated bindings,
without changing the fixed recursive overlay. It captures every candidate head
privately, ACKs each child cleanup receipt, prepares baseline and signed events,
and builds one registry image. Missing private heads cannot read stale public
heads. Original readers and reversible retirement claims protect publication;
unrelated registry entries are retained. Input/head counts, widths and names have
no new small shape cap. The existing columnar `MAX_STRATA` bound of 32 applies.

The final poll precedes the prepaid registry installation and callback batch.
No event escapes a precommit stop. Work exhaustion retains the transaction across
attempt IDs; cancellation/nonbudget failures drain private ownership before
closing the attempt. Retrying cancellation recomputes private candidates against
the unchanged public model. Late callback cancellation remains sticky for the
next controlled operation. Postcommit cleanup uses retained maintenance ownership
and cannot replay events. Consumed `$r$` input and observer staging, old heads,
registry/hash storage and their credits are retired; repeated cycles do not
accumulate registry holes. Callback replacement/clearing uses the existing
observer publisher.

Metadata initialization uses 64-byte slices; plan-copy actions admit 256 units.
Variable-size lookup and validation use charged byte/slot/column cursors, with
at most 64 transaction actions per host turn. The finite prepaid publication
suffix is after the cutoff and has no new cancellation claim. Remaining nested
resource/configuration audits stay explicitly in G6.

Integrated evidence in `evaluation_serial`:

- `test_controlled_observed_multi`: 24 alternating mutations across two inputs,
  dependent/independent heads and private JOIN, more than four bindings,
  cancellation between heads and after earlier retirement claims, late callback
  cancellation, signed/reference parity, stable cycle resources and zero governor
  balance after session/program destruction.
- `test_controlled_observed_wide_public`: two inputs and dependent/independent heads with 320-byte names,
  widths 0/2/257, FLOAT and empty initial inputs, repeated signed mutations and
  disabled-control row/event comparisons through session entry points.
- `test_controlled_observed_transaction_budget`: calibrated exact/one-short
  publication budgets, retained early-metadata and later-child work-limit resume,
  reference events/no replay, and cancellation acknowledgment plus checked
  destruction after partial initialization. Calibration is empirical evidence,
  not an independent total-cost formula.
- `test_controlled_observed_missing_private_head`: an unresolved private head
  cannot use its older public descriptor; public rows survive the rejected attempt
  and retry succeeds. The multihead checkpoint also rejects forged/stale bindings.
- Existing filtered-successor tests now perform repeated reusable staging cycles;
  scalar-retirement tests cover wide/nullary, contention and ledger ownership.
  `evaluation_observer_build` compares mixed scalar and native FLOAT-bit rows,
  wide pending subtraction and schema-budget boundaries against the raw observer.

Validation is recorded with the final qualification result below. Independent
review and final design/risk gates apply to each exact atomic candidate. G3a follows this merged G2 bundle (PR #2275); no additional
prerequisite-only PR was split out.

Local qualification (2026-10-26): the complete native build and 487-test suite
completed with 472 passed, 15 skipped and zero failures. The first full run
exposed missing state-map documentation anchors; those were corrected in the
publication commit, including its negative-test fixtures, before the passing
rerun. The final qualification tests then passed `evaluation_serial`,
`state_map_anchors` and `state_map_anchors_selftest` (3/3), plus
`evaluation_serial` and `evaluation_observer_build` under ASAN/UBSAN (2/2).
LLVM 22's full clang-tidy ratchet passed for the production candidate. Formatting
and whitespace checks passed. MSVC validation requires the remote Windows job;
these Linux results do not claim a local MSVC build.

## G3 bundle split (2026-10-27, before implementation)

G3a implements ordinary monotone recursive fixed-point transactions with frozen
round inputs, positive-arity declared INT64 heads, multiple heads/strata and
private recomputation after input mutation. Three atomic units cover executable
admission/scheduling, successor/publication integration, and qualification.
G3b retains actual differential JOIN/CONSOLIDATE, trace/arrangement transactions
and incremental retraction propagation: arrangement registry growth currently
refuses active controlled attempts, and ordinary recomputation cannot qualify
those distinct operators. Optimized K_FUSION/LFTJ routes, recursive aggregates,
negation, other recursive representations and mixed fact/rule IDBs remain open.
This split follows recovery/ownership boundaries; it does not close G3 or #1821.

## G3a implementation and qualification (2026-10-27)

Production implementation is audited at `a9ec399d` after two atomic commits;
the third commit qualifies the assembled bundle and records its remaining scope.
It advances E06 and ordinary recursive set comparison in F08. It does not close
G3, #1821, actual differential execution, or every required recursive route.

Declared, positive-arity scalar INT64 recursive heads use metadata recorded
with the preserved lowered bodies. Unknown declarations, nullary/typed/compound
recursive schemas, arithmetic recursive MAP, aggregates, negation and mixed
fact/rule IDBs are not admitted by this bridge. VARIABLE, ordinary JOIN,
projection MAP, supported row-local FILTER, CONCAT and CONSOLIDATE execute
through the existing controlled children. Rewritten K_FUSION/LFTJ and nonzero
worker EXCHANGE are refused. A zero-worker terminal EXCHANGE has no payload in
this selected ordinary body route; its preserved body ends with CONSOLIDATE.

Every recursive stratum starts with governed empty private heads. All its heads
read the same frozen round, including every seed-producing rule, and complete
and ACK their children before rebinding the next round. For this positive
program, synchronous iteration from bottom is ascending. Canonical consolidated
sets are compared one INT64 cell at a time; no binary-only merge is generalized.
The host enforces `MAX_ITERATIONS` (4096 full rounds), returning EOVERFLOW before
publication if it does not converge. No new total-runtime claim is made.
Completed private predecessor strata feed later strata. The existing exact
unobserved `edge`/`path` fast path and its tariffs remain unchanged, including
its existing append-only successor restriction; this increment does not add
removal successors to that legacy route.

The general route supports observed and unobserved execution, multiple EDBs,
and reusable insert/remove recomputation. It starts recursive heads from empty
after mutation, so cycles do not retain unsupported old derivations. All public
heads survive a precommit stop. A single coordinated registry image publishes
after the final poll; optional signed differences use the existing observer.
Old heads, consumed staging, private rounds and managed credits drain without
replay. Callback replacement/clearing inside publication and late cancellation
retain their existing contracts. External callback mutation after a completed
controlled STEP is not newly enabled. Fresh/dirty snapshots remain outside this
bundle; completed snapshot behavior is exercised.

Qualification fixtures in `evaluation_serial`:

- `test_controlled_recursive_transaction`: mutual recursion, several seed rules,
  more than four bindings, cycles/duplicates, empty inputs, actual comparison and
  retirement stops, and raw/reference parity with zero final governor credits.
- `test_controlled_recursive_transaction_successors`: four callback modes, each
  with 12 alternating mutations, alternate supporting derivations, loss of a
  cycle's final seed, unchanged registry size and stable cycle resources;
  completed snapshots, in-callback clear/replacement and late cancellation.
- `test_controlled_recursive_transaction_stops`: admission, empty-head setup,
  a later round's last active child, comparison, partial physical retirement,
  descriptor-free/credit-retained retirement, later claims and final cutoff.
  Each point checks work-limit resume, cancellation retry and cancellation
  acknowledgment before checked destruction; old public descriptors/snapshots,
  no early/replayed events and final zero credits are asserted.
- `test_controlled_recursive_transaction_shapes`: unary and ternary heads,
  two recursive strata with an acyclic consumer, and exact/one-short budgets.
  Whole-session budgets are empirically calibrated, not independent cost oracles;
  unchanged primitive suites retain the source-derived boundary checks.
- `test_controlled_recursive_transaction_nonlinear_view`: an explicitly
  unoptimized borrowed internal plan view executes nonlinear recursion with
  later-round stop/resume against an independent unchanged raw plan. The same
  fixture verifies the default K_FUSION plan is refused. This proves ordinary
  scheduler semantics, not default optimizer reachability.
- `test_controlled_recursive_transaction_refusals`: typed/nullary recursion,
  expanding arithmetic MAP and mixed fact/rule IDBs fail without publication.
  `test_controlled_recursive_step_general_join_chain` executes the formerly
  refused renamed three-JOIN body through the general unobserved route.

Next is G3b's actual trace/arrangement/retraction ownership and enforcement.
K_FUSION/LFTJ integration, other recursive schema/expression families, legacy
removal admission and remaining configuration/resource qualification stay open.
G3b and G4–G7 are five remaining work groups, not a fixed remaining PR count.

## G3b subdivision recorded before implementation (2026-10-27)

The next bundle delivers controlled incremental CONSOLIDATE through ordinary
session STEP, using validated prefix discovery. It supersedes the proposed
combined differential JOIN/CONSOLIDATE bundle: persistent multi-arrangement
publication needs a separate host ownership protocol, while consolidation can
reuse its existing leased source, private output and resumable cleanup.

This bundle discovers a strictly increasing prefix from actual leased values,
sorts only the suffix and merges the two runs. It does not propagate or trust
sorted-prefix metadata across MAP. The disabled optimization retains full sort.
Existing nonfinite FLOAT rejection precedes discovery, so the comparator domain
is finite numeric scalars; equal signed zeros retain stable source order.

Atomic units: (1) executing bounded algorithm and operator qualification;
(2) ordinary STEP reachability, stop/retry qualification and coverage ledger.
Persistent differential JOIN/registry transactions, active differential dispatch
(F04), persistent prefix reuse, and recursive frontier/retraction scheduling
remain open. These are work obligations, not a promised remaining PR count.

The first consolidation bundle's session qualification is
`test_controlled_incremental_consolidate_step`: generated identity/permuted MAP
plans, discovery/suffix-sort/merge/output checkpoints, cancellation and retained
work-limit resume, repeated insert/remove successors, raw signed-event parity,
no premature publication and governor restoration. Operator tests additionally
cover stale metadata and independent per-pair discovery costs. This advances
O08; it leaves E07/F04/F04a and the remaining G3b obligations above open.


## G3b persistent arrangement-backed JOIN bundle (2026-10-27)

After merged PR #2278, this bundle adds a stable private differential registry
image, native one-based persistent JOIN probes, and coordinated session
publication. Ownership, bridge and host integration form one executable atomic
unit; integrated qualification and ledger updates form the second. They ship
in one PR, never as a standalone ownership prerequisite.

The initial domain is full-right public EDB, contiguous INT64, keyed ordinary
serial JOIN with supported projection. Unchanged sources reuse native indexes;
changed generations rebuild privately. Private-head, recursive, selected-delta,
filtered/materialized/batched/spill alternatives retain their controlled
fallback/disposition. No signed differential/frontier/retraction claim follows.


Qualification includes generated shared/distinct-key heads, reuse without row
indexing, rebuild after insert/remove, unrelated-index preservation and raw
registry/pin compatibility. The 18-point cancellation/work-limit matrix proves
old heads and indexes survive every precommit stop. Refusal/invalid-chain,
ENOSPC, claimed-trim, committed-credit and postcutoff cleanup fixtures complete
the bundle. Admission-denial qualification also fixes diagnostic RELEASED token
payload being mistaken for owned cleanup credit; final teardown restores aggregate reservation credit.

The next G3b bundle must integrate the actual differential dispatcher and its
signed trace/frontier transitions, using this ownership boundary. Persistent
prefix metadata and recursive incremental retraction propagation still need
their own implementation and qualification. G4 pipeline/batch/LFTJ, G5
representation/fallback coverage, G6 policy/resource composition and G7 final
acceptance remain unchanged. No fixed remaining PR count or #1821 closure is
claimed by this ordinary JOIN index bundle.

## G3b increment: acyclic append differential execution

This executable G3b increment adds a controlled observed append successor for positive
nonrecursive heads reading EDBs directly. One proven changed occurrence reads
the appended suffix; metered union/consolidation retains the old head. All
heads and arrangement changes publish at the existing common cutoff. Mixed
mutations, repeated changed reads and transitive dependencies retain full
recomputation. Recursive frontier scheduling, weighted traces and incremental
retractions remain separate obligations; the recursive scheduler remains the next distinct G3b obligation.

## G3b increment: controlled linear SCC row frontiers

The general host now executes actual previous-round row-frontier reads for
positive declared INT64 SCCs with at most one same-SCC source occurrence across
each entire preserved head body. Exact occurrence authorization, existing
controlled union/consolidation, bounded sorted-set difference and retained
private owner retirement form one executable atomic unit in one PR bundle.
Multiple mutual heads, seeds, filters, projections and preceding/downstream
strata remain composable. Repeated same-SCC reads retain full rounds where
admitted; default optimized-path refusals are unchanged.

All input successors restart from bottom, then execute genuine frontier rounds.
This closes the selected attempt-local linear SCC scheduler domain; it does not
close cross-attempt incremental append/retraction, nonlinear/default optimized
variants, weighted traces or persistent raw epoch/rule-frontier publication.
Those remain G3b work alongside persistent prefix metadata. G4 through G7 remain
as recorded; #1821 is not closed by this increment.

Generated route tests witness smaller-than-accumulation and empty frontiers,
left/right recursive JOIN inputs, single/mutual recursion, 17-column rows,
observed/unobserved parity and insert/remove restarts. A borrowed unrewritten
compiler-body view qualifies repeated-occurrence fallback without claiming the
refused K_FUSION route. Thirteen cancellation/work-limit checkpoints cover
admission, plan/constructor preparation, difference/gather/append, later-round
selection, retirement, downstream evaluation and cutoff, with retry/event and
governor restoration checks.
