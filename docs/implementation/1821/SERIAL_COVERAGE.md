# Serial enforcement coverage contract

Audit baseline: `0d91d02a` (2026-10-26). This is the dispatch-family inventory
for package 1 of the [closure plan](CLOSURE_PLAN.md). It records implementation
obligations, not a declaration that #1821 is implemented. A refusal proves
admission behavior only; a private operator test does not prove session reachability.

## Reading the matrix

**R** means required serial work; **P** means parallel ownership in #1822;
**H** means the cooperative host callback boundary. R rows that are currently
refused remain required gaps. **Session**, **frame**, and **primitive** distinguish
entry levels. Test names below are in `test_evaluation_serial.c` unless a suite
is named. Existing fixtures were inspected; fresh execution is listed at the end.

Polling notation concerns logical engine work, never elapsed time:

- **A**: a charge/tail poll surrounds each billed action in the named helper.
  This establishes the local action boundary, not a bound for all nested work.
- **Q256**: the named copy/allocation/merge slice caps admitted units at 256.
- **Q64**: session preflight/scheduler caps a turn at 64 charged transitions.
- **U**: no complete finite polling proof for the whole named region. This is
  an open enforcement/qualification obligation, not an exclusion.
- **Refused**: no operator execution is admitted; any preflight work still needs
  its own bound. Static refusal and charged refusal have different report contracts.

The global maximum remains `WL_EVALUATION_CONTROL_QUANTUM` (256). A and Q labels
must be combined with nested-call, allocation and retirement audits before a
whole-route claim is made. Independent exact-work formulas and empirically
chosen retry allowances are different evidence; the latter cannot certify cost.

Stop owners: **S** = `controlled_step` and its attempt; **F** = framed relation,
child context and cleanup receipts; **B** = bound producer/runner; **L** = LFTJ
attempt and source readers; **O** = observer baseline/preparation/publication.
Ownership on cancellation/work exhaustion remains governed by #1820. EAGAIN
within a live attempt does not reset its allowance.

## Entry and scheduling edges

Source: [session.c](../../../wirelog/columnar/session.c),
[session admission](../../../wirelog/session.c),
[eval_serial.c](../../../wirelog/columnar/eval_serial.c).

| ID / family | Predicate and destination | Disposition, checkpoint / bound, stop owner | Evidence and remaining obligation |
|---|---|---|---|
| E01 / S11 | Control absent: ordinary `col_session_step`; control attached: `col_controlled_step` | R; raw execution is the disabled-control reference, not an enforcement route | `test_controlled_session_step_resume`; preserve raw semantics in every later group |
| E02 / S11 | Default acyclic, no observer: static checks → `col_controlled_step_default_preflight` → framed relation host | R, implemented restricted route; Q64 for operator/registry/name traversal and scheduler; S/F | `test_controlled_step_preflight_byte_cursor`, `test_controlled_step_preflight_work_limit_boundary`, `test_controlled_step_scheduler_empty_strata`; nested frame/cleanup regions remain U (G1/G6) |
| E03 / S10,S11 | Delta callback: legacy unary route or ordinary acyclic transaction → private captured heads → baseline/events → coordinated publication | R, ordinary scalar host implemented; charged byte/slot/schema/metadata cursors, A/Q64; 32 inherited strata maximum, child domains remain authoritative; S/F/O | `test_controlled_observed_multi`, `test_controlled_observed_wide_public`, `test_controlled_observed_transaction_budget`; typed/wide/nullary/dependent-head and empty-input execution, stops and reference parity. New expressions/representations and configurations remain G5/G6 |
| E04 / S10 | Completed observed or initially callback-free acyclic STEP plus declared-input insert/remove → private full candidates → signed comparison and atomic registry image | R, reusable ordinary route; no three-slot/four-binding cap; charged preparation, reversible old-head/staging claims, prepaid cutoff, retained cleanup; S/F/O | `test_controlled_observed_multi`, `test_controlled_observed_filter_successors`; repeated signed cycles, later-claim cancellation, no replay, reference output and stable/zero governor balances. #1820 recovery contract retained |
| E05 / S07 | Exact binary INT64 path manifest with one/two JOINs and terminal zero-worker EXCHANGE → `col_controlled_step_recursive_drive` | R, implemented restricted recursion; charged progress/private merge; S/F; general SCC bound U | `test_controlled_recursive_two_join_step`, `test_controlled_recursive_two_join_successor_stop`, `test_controlled_recursive_successor_work_boundaries`; G3 |
| E06 / S07 | Ordinary positive recursive multihead/SCC bodies | R, declared positive-arity INT64 route implemented with private synchronous rounds; charged comparison and retirement; S/F/O | `test_controlled_recursive_step_general_join_chain`, `test_controlled_recursive_transaction`, `test_controlled_recursive_transaction_successors`; observed/unobserved and insert/remove recomputation. `test_controlled_recursive_transaction_stops` and `test_controlled_recursive_transaction_shapes` qualify phase stops, multiple strata and empirical budget boundaries; differential and optimized routes remain G3b/G4 |
| E07 / S08 | Foreign active raw iteration/retraction/differential state at admission versus legal completed-host removal | Foreign states retain their refusal. Qualified scalar mutations and binary recursive removal use private bounded reconstruction; see the #2296 qualification below | `test_controlled_session_admission_matrix` remains refusal evidence. `evaluation_differential` independently qualifies natural scheduler selection, scalar selectors, legal mutation, stop/retry and signed events; physical scalar intersections are qualified in PHYSICAL_METADATA_AUDIT.md |
| E08 / S09 | Nonzero `join_batch_bytes`, non-strict unobserved supported scalar plans → prepared producer, controlled per-batch FILTER/MAP/FULL-probe consumers and private relation sink | R, restricted; Q64 preflight, A producer/consumer/sink, same F; intrinsic declines use bounded ordinary JOIN; S/F/B | `evaluation_join_hosts` domains/recovery/failures/funding and existing selector tests; broader policies remain G4 |
| E09 / S09 | Remaining strict/cap/shared/observed batch policies and unsupported operator/source dispositions | R, guarded per destination; general non-strict scalar shapes use E08's actual producer/consumer owners | `test_controlled_session_admission_matrix` retains foreign/strict exclusions and positively qualifies scalar batch admission; broader policy/source intersections still required, G4 |
| E10 / S12 | Fresh/dirty controlled SNAPSHOT versus completed STEP | R; qualifying fresh/dirty → private transaction and staging on one attempt; unrelated pending STEP EBUSY; completed → `col_session_snapshot_controlled` → charged private staging → finite prepaid dispatch; S | `evaluation_lifecycle` covers generated fresh/dirty acyclic and numeric recursive evaluation, shared attempt identity, stop/retry and live-state refusal; `test_controlled_session_snapshot_publication` covers cutoff/reference. `test_controlled_snapshot_preparation` covers charged preparation, no-output stops and retry; finite prepaid dispatch preserves the cutoff |
| E11 / S11 | Frame bootstrap, funding transfer, retained continuation, ABORT/ACK and destruction | R, implemented receipt protocol; per-transition charging, nested allocation/cleanup U; F | `test_cleanup_receipt_transfer_retry`, `test_entry_abort_and_unsupported_abort`, `test_controlled_session_step_destroy_refusal`; G6 must audit all ownership regions |
| E12 / S13 | Coordinator, >1 worker, TDD input/subpass/outbound state, shared output counter | P for worker scheduling/global allowance; serial payload still R | Static/composed refusal; #1822 owns worker integration. Serial payload obligations stay in operator rows, not excluded wholesale |
| E13 / S11 | JOIN output limit, active rollback, foreign observer, cleanup pending | R policies/admission; preserve error distinction and owner; no unrestricted fallback | `test_controlled_observed_preflight_rejections`, `test_controlled_batch_child_domain_rejection`; cross-policy matrix and post-stop resource baselines still G6 |

## Operator dispatch ledger

All twelve `wl_plan_op_type_t` values in
[exec_plan.h](../../../wirelog/exec_plan.h) are accounted for here.
[eval_plan.c](../../../wirelog/columnar/eval_plan.c) has distinct raw,
numeric-owned and composed-owned dispatches. Session admission is the first
filter; accepting an enum does not certify its metadata, representations or
expressions. The numeric/composed child validation remains authoritative.

| ID / family / operator | Controlled destination and variants | Entry level, bound, owner | Evidence / gap |
|---|---|---|---|
| O01 / S01 / VARIABLE | `wl_columnar_eval_plan_numeric_source_step`; full versus delta selection, missing/empty source, registry/name lookup, scalar materialization | Session/frame restricted; A lookup and Q256 relation lanes, nested representation setup U; F | `evaluation_numeric_plan`: `test_source_delta_modes`, `test_source_long_names`, `test_source_scan_stops`; representation/config qualification G5/G6 |
| O02 / S02 / FILTER | `wl_columnar_filter_op_attempt_start_owned`; predicate decode, accepted/rejected rows, empty input | Session/frame; A decode bytes/instructions/row tails; allocation/cleanup U; F | `test_entry_generated_filter_work_boundary`, `test_controlled_observed_filter_stops`; general expression domains G5 |
| O03 / S02 / MAP | `wl_columnar_filter_map_attempt_start_owned`; projection and numeric expression output, INT64/FLOAT, integer CRC/hash | Session/frame; A decode/evaluation/copy, nested allocation U; F | `test_entry_generated_map_work_limit`, `test_entry_generated_integer_crc`, `test_entry_generated_integer_hash`; unsupported expression families G5 |
| O04 / S03 / JOIN | `wl_columnar_join_op_attempt_start_owned`; product/keyed, collisions, right predicate, projection; non-materialized ordinary route | Session/frame; A hash/probe/candidate/right-filter tails, nested source/scratch U; F | `test_controlled_session_join_step`, `test_entry_generated_right_filter_collisions_and_stop`, `test_entry_generated_right_filter_product_work_boundary`; materialized/differential/batch alternatives F01–F05 |
| O05 / S04 / ANTIJOIN | `wl_columnar_join_probe_op_attempt_start_owned`; membership/nonmembership child | Session enum admitted, private scalar child restrictions; A child charges, whole host/config bound U; F | `test_entry_generated_operators`; complete predicate/type/fallback session matrix missing, G5 |
| O06 / S02 / REDUCE | `wl_columnar_ops_reduce_attempt_start_owned`; grouped/ungrouped COUNT and numeric aggregates | Session/frame restricted; charged input/group/sort/projection; whole region U; F | `test_entry_generated_reduce_work_limit`, `test_entry_generated_grouped_count_work_limit`, `test_entry_generated_grouped_numeric_reduce_work_limit`; remaining aggregate/type/error matrix G5 |
| O07 / S05 / CONCAT | Composed dispatcher → `wl_columnar_merge_concat_attempt_start_owned`; stack union inputs | Session/frame; charged source/copy/handoff; Q256 managed copies, complete ownership combinations U; F | `test_generated_union`, `test_entry_generated_operators`; owned/borrowed/segmented/typed combinations G5 |
| O08 / S05 / CONSOLIDATE | `wl_columnar_filter_consolidate_attempt_start_owned`; sort/dedup, ordinary versus differential selection | Session/frame ordinary; charged comparison/copy, cleanup U; F | `evaluation_consolidate`, `evaluation_numeric_plan`: `test_owned_consolidate_successor`; differential branch F04, remaining representations G5/G6 |
| O09 / S04 / SEMIJOIN | Same owned probe child as ANTIJOIN, membership output | Session enum admitted, restricted scalar child; A child charges, whole host/config bound U; F | `test_entry_generated_operators`; SIP/predicate/fallback session qualification G5 |
| O10 / S07,S13 / K_FUSION | Raw `col_op_k_fusion`; rejected by controlled session operator preflight | P scheduling, R serial payload; Refused; no controlled session owner | Admission matrix proves refusal before frame; #1822 for coordination, serial fallback G3/G4 |
| O11 / S06 / LFTJ | Private composed entry → `wl_columnar_ops_lftj_attempt_step`; source resolution/readers, allocation, copy, merge-sort, seek/join, release | Frame/primitive implemented; session enum refused; A per traversal/copy action, sliced allocation precharge; L | Generated LFTJ fixtures cover empty/float/nonleading/duplicate keys, source lookup, child stops, allocation and abort cleanup. Session integration and raw cache/fallback parity G4 |
| O12 / S07 / EXCHANGE | Exact recursive manifest terminal zero-worker exchange; other session uses refused | R serial terminal implemented; P redistribution; transition charge, no multiworker proof; S/F | Recursive manifest/publication fixtures and admission matrix; serial nonrecursive handling G3, worker behavior #1822 |

## Fallback and representation edges

Raw destinations must not be reached from controlled dispatch merely because an
optimized path declines. Their existence is a required integration gap unless
explicitly assigned elsewhere. No unlimited raw execution is an acceptable
controlled fallback.

| ID / family | Branch and destination | Disposition / bound / owner | Evidence and exit obligation |
|---|---|---|---|
| F01 / S03,S09 | Raw JOIN: non-differential + batch bytes → `wl_columnar_join_pipeline_try_eval`; successful consumer pipeline advances operator index | R, general controlled host unavailable; U; raw pipeline ownership is not a controlled owner | `join_pipeline` is raw correctness evidence. Need actual controlled sink/filter/MAP suspension/replay and shared allowance, G4 |
| F02 / S09 | Pipeline ORDINARY or non-strict DELTA_RIGHT decline, or relation-sink successor → ordinary JOIN; other decline/error records pipeline fallback; strict refuses | R, raw decision branches; controlled session uses E08 instead; U | `eval_plan.c` raw JOIN switch; `join_pipeline`/`join_batch_resume` references do not prove controlled fallback. Qualify every decline destination without budget reset, G4 |
| F03 / S09 | `join_batch.c` prepared producer bound to control/attempt → controlled producer/runner; capacity pause, cancellation, stale provenance | R, primitive implemented; `controlled_charge` polls each billed action and enforces caller turn allowance; B; end-to-end host U | `join_batch_resume`: `test_controlled_producer_work_limit`, `test_controlled_producer_cancel_and_stale_input`, `test_controlled_join_runner_cancel`; G4 |
| F04 / S08 | `diff_operators_active` selects `wl_columnar_join_diff_op` and `col_op_consolidate_diff`; trace/arrangement updates and `diff_join_batch.c` | R, refused at controlled host/numeric/composed entry; U; no integrated stop owner | `diff_join` is raw reference only. `arrangement.c:diff_registry_grow` also refuses an active evaluation attempt; G3 must integrate trace/registry/retraction ownership with #1820 |
| F05 / S03 | Materialized JOIN versus named ordinary right source | R, owned ordinary child explicitly excludes `op->materialized`; unsupported private variants must not fall through raw execution; U for required materialized path | Composed abort-support validation distinguishes materialized JOIN; `test_entry_abort_and_unsupported_abort`; execution/refusal coverage by representation still G5 |
| F06 / S06 | Raw `col_op_lftj` cache/sorted inputs versus private controlled per-attempt source copy/sort/seek | R; primitive has its own charged path, cache reuse/fallback host behavior unqualified; L / U for raw alternatives | `test_entry_generated_lftj_stops_inside_child`, `test_entry_generated_lftj_allocation_precharge`, reader/abort fixtures; G4 |
| F07 / S01,S05 | Scalar source/owned output versus retained, segmented, compound, timestamped/weighted or spill-backed relation representations | R; each child/source admission must have a disposition; no blanket representation support claimed; F / U | `evaluation_copy`, `evaluation_relation_creation`, `evaluation_output_materialization`, retained-input fixtures; qualify source rejection/materialization and signed metadata, G5. PR #2270 owns its spill-weight feature, not this audit |
| F08 / S05,S07 | Legacy recursive merge/dedup versus managed incremental candidate/union | R; `wl_columnar_merge_incremental_delta_attempt_advance` caps a call at Q256; generic legacy recursive path remains U; S/F | Restricted recursive session tests and `evaluation_work`/`evaluation_consolidate` primitives; general frontier/retraction integration G3 |
| F09 / S11 | Governor admission, allocator/Arrow release, CAS/control references, receipt retirement and registry replacement | R engine setup/cleanup; external allocator body is noninterruptible, engine copies/scans are not thereby excluded; U unless separately precharged/bounded | Allocation/copy/receipt tests establish local tariffs, not all nested bounds; G6 must name each finite region, charge before allocation, retain resources on every failure |
| F10 / S10,S12 | Observer/snapshot preparation → final poll → prepaid callback batch | H for callback duration; R for charged engine preparation/assembly/traversal/lookup, retained mandatory cleanup; O, A/Q64 before cutoff | `test_controlled_snapshot_preparation` and the G2 multihead/wide/budget fixtures cover preparation stops, late cancellation and finite publication. Nested policy/configuration obligations remain G6 |

### Concrete fallback predicates within F01–F07

The following subdivisions keep the fallback ledger tied to source decisions,
not only algorithm names. All are R with a U whole-region bound until G3/G4/G5
qualification; worker execution itself is P. The existing raw suites are
references, not interruption evidence.

| Subedge | Source decision / destination | Current controlled disposition and owner |
|---|---|---|
| F02a | Pipeline decline NONE/success, NOT_ELIGIBLE, ORDINARY, DELTA_RIGHT in `join_pipeline.h` and raw `eval_plan.c` switch | Success consumes the pipeline; ORDINARY and non-strict DELTA_RIGHT go to ordinary JOIN; relation-sink decline also uses ordinary JOIN; other declines record PIPELINE, strict refuses. No general controlled pipeline owner; G4 |
| F02b | `join_batch.h` eligibility OFF, CROSS, DELTA_RIGHT, FILTERED_RIGHT, WORKER | Raw non-strict exclusion chooses one-shot JOIN, strict refuses when batching requested. Controlled host has E08 only; product/right-filter ordinary controlled children remain O04, not proof of batch fallback; G4 |
| F02c | Eligible keyed JOIN lacks persistent arrangement, or producer creation cannot fit one row | Raw NO_ARRANGEMENT selects ephemeral hash; ROW_TOO_LARGE selects one-shot output, strict refuses. Controlled producer B exists only for admitted prepared inputs; host fallback ownership missing, G4 |
| F02d | MATERIALIZED, LFTJ, INSERT_ROUTE exclusions in batch eligibility/reporting | Differential/materialized selection belongs to F04/F05; raw LFTJ strict refusal/non-strict one-shot is F06; insertion route does not activate the batch knob. These are not controlled escape hatches; G3/G4/G5 |
| F04a | Differential arrangement/trace persistent transaction versus ephemeral hash fallback | Raw fallback must leave no transaction state; controlled entry refuses active differential mode and active-attempt registry growth. Integrated stop owner absent; G3, #1820 |
| F05a | Ordinary keyed persistent arrangement/probe bundle versus ephemeral hash; cached right filter versus operation-owned filtered relation | General serial STEP owns a private native index registry for full-right public EDB INT64 JOIN, with bounded creation/reuse/probes and a common head/index cutoff. Existing pins/pending/deferred entries refuse replacement. Filtered/private-head/batch representations retain their previous controlled disposition; this does not qualify raw cache alternatives, G5/G6 |
| F06a | `ops.c:col_op_lftj` sorted-arrangement probe/cache versus owned per-source materialization; raw binary output callback | Controlled private LFTJ uses its per-attempt copy/sort/traversal L owner. Raw whole-output append and cache alternatives have no session control proof; G4 |
| F07a | Scalar-source compound kind NONE versus INLINE/SIDE, declared schema/type validity, timestamps and row weights | Admission predicates differ by child; no cross-operator representation support inferred. Managed copies/readers can be bounded while a surrounding child refuses the representation; G5 |
| F07b | Retained/borrowed/owned, contiguous/segmented/spill-backed storage and empty/nullary/wide rows | Source reader/descriptor and copy helpers must preserve representation and governor state; capacity/overflow errors remain errors. Enumerated qualification combinations, not an invitation to raw fallback; G5/G6 |

## Configuration predicates

These axes apply to the rows above; they are not a claim that every Cartesian
combination is supported. An unsupported combination stays a required gap
until the product contract justifies exclusion.

| ID / family | Axis and audited dispatch consequence | Required disposition and qualification |
|---|---|---|
| C01 / S02,S13 | Numeric expression decoder accepts column numeric variables, INT/FLOAT/BOOL literals, integer/float arithmetic/comparison, CRC/hash and numeric aggregate tags; default returns ENOTSUP | R. `expr.c:wl_columnar_expr_attempt_decode` polls bytes/digits and evaluation instructions. String/symbol/extension/library expressions require explicit metered admission or refusal, G5; no unlimited expression fallback |
| C02 / S13 | `crc32_variant` ethernet/castagnoli; `mbedTLS` disabled/enabled/auto | R. Both integer CRC fixtures exist; library crypto is not certified by numeric tests. Enabled-library paths need finite admission/error semantics or refusal before invocation, G5/G6 |
| C03 / S13 | Native C11/POSIX/Windows threading, forced `threads=posix`; serial worker count versus coordinator/multiworker | R serial ABI/thread-affinity checks; P global worker allowance. Run the same serial contracts on supported backends; existing ABI-aware work formulas must not assume LP64, G6 |
| C04 / S09,S13 | Runtime batch bytes/strict, delta selectors AUTO/FORCE_FULL/FORCE_DELTA, right-delta availability, output limit/shared counter | R. E08/E09 and F01–F04 distinguish current routes; producer versus whole pipeline qualification G4, policy precedence G6 |
| C05 / S01,S13 | `embedded`, `ipc`, IO/plugin loader, Arrow/import/export and extension callbacks | IO/transport outside serial evaluation; R for resulting representations and expressions when consumed by an operator, H only for callback bodies. Qualification G5/G6; enabled options cannot bypass source/child admission |
| C06 / S13 | Android/iOS cross builds, MSVC/Clang/GCC, debug/release/sanitizers; logging ceiling, fuzz/docs switches | R serial behavior/ABI qualification; build-only tooling is not a new evaluator route. Local tests below do not certify all targets; G6 and final #1824 qualification |
| C07 / S07,S13 | Optimizer selects LFTJ, SIP, K_FUSION, EXCHANGE or materialized JOIN | R serial work follows O05/O09–O12/F05/F06; P redistribution. Refusal today is not permission to omit a default/supported optimizer result from final coverage, G3–G5 |

## Concrete remaining implementation groups

Each row above has a disposition and a group below. Package 1's inventory is
complete at the dispatch-family/configuration-axis level of this baseline;
unknown bounds and missing tests are explicitly open. It is not a proof of
all internal branch combinations. A changed dispatcher/configuration adds or
amends a row before implementation, rather than silently enlarging the scope.

| Group | Owned rows / work | Exit tests and dependencies |
|---|---|---|
| G1 — bounded session admission and snapshot engine work | E02/E03/E10/F10; S11/S12 | Long names, many strata/EDBs/registry slots and large snapshot sources stop at deterministic checkpoints before callbacks; exact/one-short admission boundaries, unchanged output on stop, retry. Preserve #1820 publication cutoff. Merged in PR #2272 for the restricted admitted session domain; fresh/dirty evaluation stays with #1820/G3 |
| G2 — reusable signed observer composition | E03/E04/F10; S10 plus already controlled ordinary scalar composition | Implemented and qualified in this bundle: repeated signed cycles, typed/wide/nullary/dependent heads, stops/retry, no replay and resource baselines. New child domains/configurations retain G5/G6 ownership; #1820 cutoff and recovery preserved |
| G3 — general serial recursive/differential execution | E05–E07/O10/O12/F04/F08 | Actual SCC/frontier, trace, retraction and serial fallback interruption; signed reference parity and reset/retry. Integrate #1820 safe state ownership, leave parallel scheduler to #1822 |
| G4 — host LFTJ and batch/pipeline/fallback integration | E08/E09/O11/F01–F03/F06/C04 | Actual chosen pipeline/LFTJ/decline destination must run and stop; one allowance across suspension/replay, no-output candidates, error distinctions, reference parity; reuse existing producers |
| G5 — scalar/expression/representation closure | O01–O09/F05/F07/C01/C02/C05/C07 | Each required type/expression/materialized/retained/segmented/weighted representation has actual-path limits, cancellation, canonical reference and ownership tests; unsupported external bodies get explicit finite admission/cooperative disposition |
| G6 — resource/policy/configuration qualification | E11/E13/F09 and C02–C06 | Audit nested setup/cleanup bounds, allocation slack, governor/overflow/output-policy precedence; named native/sanitizer/MSVC/POSIX/configuration evidence and no leaked ownership after stop |
| G7 — final serial acceptance | All R rows | Close all U and missing required execution evidence; complete quantum/boundary/reference table on final SHA, then close #1821. #1822–#1824 remain separately owned |

These are seven implementation/qualification groups, not seven guaranteed PRs.
The differential recovery and representation groups still require design
sizing; a fixed remaining PR count is not supported. Each PR must name the
rows it closes and update this ledger. G1 was merged in PR #2272. G2 is delivered
as one four-commit bundle. G3 is next; G3–G7 are five work groups, not five
promised PRs. The G2 production audit uses `a76b8ff5`; other rows retain their
original audit and explicit gaps.

## Historical coverage-contract evidence

`test_controlled_session_admission_matrix` covers nine host restrictions:
iteration, two retraction flags, differential mode, strict batching, an
unsupported batch shape, LFTJ, EXCHANGE and K_FUSION. It verifies no frame
execution, no published head, unchanged input/registry, no cleanup residue,
and static-versus-charged refusal reports. Removing the injected restriction
then executes the same session and delivers its canonical completed snapshot.
Fresh snapshot refusal produces no callback or control report.

This test does not claim that the refused algorithms executed. Existing
recursive/observer/batch/snapshot positive fixtures provide the corresponding
admitted-route evidence. Execution results for this increment are recorded in
the closure plan; other suite references above are inspected evidence only.

### G1 admission implementation (merged PR #2272)

Observed admission now charges declared-EDB visits, registry slots and name
bytes inside the existing STEP attempt, with at most 64 cursor actions per
host return. The historical restricted route rejected unsupported stratum/rule counts
without walking the arrays; G2 adds charged ordinary composition traversal. Initial-result lookup and successor-name collision scans also
charge before reading names. Successor metadata copies use at most 128-byte
chunks; the fixed generated name lives in the STEP record. Retired hash
credits become canonical empty tokens without relying on a lazy lookup.

Completed observed mutation authorization checks at most three registry
pointers and compares names only through the previously validated immutable
name lengths. This API argument validation is O(validated name length),
outside the STEP attempt; it is not a constant-time or 256-action polling
claim. Successor STEP revalidates the actual relations under its attempt.
G2 below replaces the restricted composition and staging limits; the legacy
route retains its existing admission tariffs.

### G1 completed-snapshot implementation

E10 now prepares stable metadata and row-major tuples under one control
attempt. Plan/registry/name traversal, metadata initialization, allocation,
source gathering and finite dispatch admission are charged. No callbacks
escape a preparation stop; every initialized source drains before cutoff.
The full prepared batch is delivered after cutoff, preserving late sticky
cancellation and raw ordering. That finite tail has no new polling claim.
G1 is complete for the currently admitted serial host routes. Recursive/differential
observed composition, new child expressions/representations, fresh/dirty snapshot
evaluation and nested resource/configuration qualification remain in G3–G7 and
their existing #1820 dependencies.

### G2 ordinary observed composition

The [closure plan](CLOSURE_PLAN.md#g2-implementation-bundle-2026-10-26) records
scope, ownership, named tests and local validation. General observed STEP now
uses authenticated dynamic bindings and capture-only ordinary frames. Every
child terminal receipt is acknowledged before a binding changes. Baseline/current
preparation and one coordinated registry image precede the final poll; old-head
and consumed-staging claims remain reversible until publication. Mandatory
cleanup retains its owners after the cutoff.

E03/E04/F10 have integrated session evidence for multiinput and dependent heads,
private JOIN, typed/wide/nullary rows, empty inputs, long names, repeated signed
cycles, work exhaustion, cancellation and resource baselines. The calibrated
whole-transaction exact/one-short test is empirical; it does not replace the
independent cost-oracle obligations of G7. Callback execution and finite prepaid
publication tails retain their existing cutoff semantics. No generic child
expression, optional representation, recursive/differential or batching claim
is inferred from host admission.

### G3a/G3b implementation boundary

G3a implements the ordinary recursive fixed-point transaction bundle, with
declaration-derived positive-arity INT64 schema, synchronous full-body rounds
and transactional publication. G3b retains real differential trace/arrangement
and incremental retraction execution (E07/F04). Full recomputation is not
evidence for those paths. K_FUSION/LFTJ, other recursive types and mixed
fact/rule IDBs remain separate gaps. See CLOSURE_PLAN.md for the recorded split.


G3a source audit: `a9ec399d`. The exact binary unobserved fast path (E05)
retains its old tariff and append-only successor domain. E06 uses declared
positive-arity INT64 heads, frozen full-body rounds and one-cell canonical
comparisons; at most 64 host actions run per turn. Child and receipt helpers
retain their own established poll bounds. Allocation/cleanup exclusions and
G6 nested audits are not closed by this host integration.

`test_controlled_recursive_transaction_nonlinear_view` uses an explicitly
unoptimized internal plan view and a separate unchanged raw reference. Default
K_FUSION remains refused, so this is not optimized dispatch coverage. Actual
trace/arrangement and differential retraction (E07/F04), mixed fact/rule IDBs,
recursive negation/aggregates, non-INT64/nullary recursive heads and default
optimizer integration remain open. The complete G3a evidence and next G3b work
are recorded in [the closure plan](CLOSURE_PLAN.md#g3a-implementation-and-qualification-2026-10-27).

### G3b first bundle: controlled prefix-discovery consolidation

Audited on `567a5570` plus this bundle. O08 now has an ordinary controlled
incremental algorithm selected by the session's existing `diff_enabled` option:
`wl_columnar_filter_consolidate_attempt_start_owned` discovers a strictly
increasing prefix under its scalar source lease, sorts only the suffix and
stably merges the runs before deduplication. Discovery uses actual values, not
`sorted_nrows` provenance. Disabling the option preserves the previous full sort.

`evaluation_consolidate` tests `incremental_prefixes` and
`incremental_prefix_boundary` qualify wide/nullary/finite FLOAT inputs, equal
boundaries, descending suffixes, invalid cache metadata, signed zeros, nonfinite
rejection, cancellation at discovery/sort/merge/output, retry and exact/one-short
allowances. The discovery oracle uses source fees (512 per one-column pair)
after an empirical setup anchor; whole-call budgets are empirical.

`test_controlled_incremental_consolidate_step` in `evaluation_serial` executes
unmodified generated identity and column-permuting MAP plans through ordinary
STEP, verifies actual suffix checkpoints, stops at all four algorithm phases,
and exercises both cancellation/retry and retained work-limit reset/resume.
Insert/remove successors preserve old public heads and callbacks until success;
canonical rows and signed events match a disabled-control/full-sort session.
Repeated cycles restore the governor baseline; checked destruction followed by
control and plan release drain the final retained credits.

This is O08 algorithm evidence, not F04 dispatch closure. Active differential
JOIN/CONSOLIDATE dispatch still refuses controlled execution. The subsequent ordinary JOIN bundle below supplies persistent index ownership
and host-atomic publication for its stated domain. Persistent sorted-prefix
reuse, recursive delta/frontier transitions, and actual retraction scheduling
remain open under G3b. No trace persistence is inferred from `diff.c` comments.


### G3b second bundle: persistent ordinary JOIN index ownership

`test_controlled_persistent_join` executes a generated three-head STEP plan.
Two heads share one right-key index and another uses a different key vector.
Native indexing/probe witnesses prove the route ran. A left-only successor
reuses both indexes without indexing any row; right insertion/removal rebuilds
privately, with signed observer parity against disabled-control/full-recompute
execution. A raw-created unrelated index survives the next controlled image,
and raw registry growth/pin consumers work after controlled publication.

`test_controlled_persistent_stops` injects cancellation and work exhaustion at
18 checkpoints spanning registry discovery/allocation/copy, index allocation,
key and chain initialization, request lookup/reuse, indexing/probing and host
prepare. Public heads, registry identity and entry generation remain unchanged;
reset/retry reaches reference parity and final destruction restores governor
credit. Work-limit recovery after image preparation is abort-and-retry, not
retained cursor continuation.

`test_controlled_persistent_refusals` qualifies held pins, pending transactions,
deferred invalidation, malformed native chains, admission denial for registry,
ownership slots and index storage, interrupted claimed RESERVED trims and
committed image credit before cutoff, and cancellation during postcommit
retirement. Checked destruction refuses a live partial index until cancellation
cleanup drains it. Refusals preserve public state;
invalid cached chains rebuild privately; committed completion delivers events
once. Admission denial remains ENOSPC; peer pressure is released before further
host cleanup. Denied reservations retain diagnostic payload but own no credit;
the wrapper restores their fresh token before generic cleanup. The focused governor fixture independently checks trim contention,
competing operations, wrong-thread/copied receipts and exactly-once credit.

Registry actions are admitted in 128-unit increments. Native row hashing and
candidate probes use the existing controlled JOIN key/cell charges even when
no row is emitted. Array/metadata initialization and retirement are separately
charged; allocator/free bodies retain the existing cooperative boundary. E07,
F04 and F04a remain open for actual differential dispatcher/trace/frontier
integration. This evidence advances only ordinary JOIN's F05a ownership domain.

## G3b acyclic append-delta host increment

The general **observed** controlled STEP host now selects the actual appended
suffix at an authenticated VARIABLE or JOIN-right occurrence. Direct-EDB
positive INT64 heads with at most one changed occurrence across their entire
body qualify after a charged whole-host audit. Other shapes and mutations use
existing controlled full recomputation. This is an acyclic insertion route;
raw `diff_operators_active` dispatch, recursive frontier scheduling, weighted
traces and incremental retraction remain unqualified G3b work.

`test_controlled_persistent_join` witnesses three exact delta selections of one
row for both left-input and right-input successors. Unchanged full-right native
indexes reuse their identity; delta-right joins use ephemeral indexes.
`test_controlled_append_successors` compares generated multihead results and
signed events with a control-disabled reference for repeated appends, duplicate
and nonmatching rows, filters, pure projections, unchanged union branches,
removal, mixed-input mutation, repeated-read and transitive-dependency fallback.
Eight host checkpoints exercise cancellation and work limits during eligibility,
delta preparation, plan preparation, exact selection, later-head execution,
claims, cutoff and committed retirement. Retry preserves parity and event
uniqueness; checked destruction restores governor credit. Existing primitive
copy/JOIN/CONCAT/consolidation boundaries remain their independent qualification;
these session checkpoints are integration evidence, not exact-cost formulas.

## G3b controlled recursive row-frontier increment

`test_controlled_recursive_frontiers` exercises generated single/mutual SCC
plans with recursive VARIABLE/left and JOIN-right occurrences, cycles, duplicate
seeds, filters, projections, acyclic consumers, observed/unobserved hosts and
17-column rows. Exact source hooks verify authenticated frontier dispatch,
later source row counts smaller than accumulated sets, and real zero-row inputs
while another head still changes. Initial/append/remove results and signed
events match a control-disabled reference. Repeated-occurrence fallback uses an
explicit borrowed view of compiler-preserved unrewritten bodies; it does not
qualify the default K_FUSION rewrite.

`test_controlled_recursive_frontier_stops` coordinates cancellation/work limits
at thirteen host/primitive points, including an admitted frontier constructor,
inside controlled row append, later-round source selection, physical/credit
retirement, a downstream consumer and the common cutoff. Public heads/snapshots
and pending inputs survive every precommit stop; reset/retry restores reference
parity, and checked destruction restores aggregate governor credit. Existing
recursive tests retain empirical exact/one-short session-budget coverage.
These integrated checkpoints are not new primitive exact-cost formulas.

Each comparison/gather action has a four-unit admission and handles at most one
INT64 cell plus bounded cursor bookkeeping. Constructors and row append reuse
qualified bounded metadata/row helpers; allocator/free bodies and callbacks
retain their existing cooperative exclusions. All children ACK before set
difference, all differences finish before rebinding, and all frontier ownership
transitions finish before the next round. Any precommit limit after frontier
preparation uses whole-host abort/retry, including later strata.

This advances E06's general positive INT64 linear SCC domain with attempt-local
row-frontier execution. E07/F04/F04a raw differential dispatch, weighted traces,
persistent epoch/rule-frontier publication, cross-attempt incremental retraction
and optimized/nonlinear variants remain open. No global diff flag is relaxed.

## G3b default generated linear UNION qualification

`test_controlled_recursive_union_heads` uses unmodified generated plans with
separate recursive branches referencing distinct heads and JOIN keys. Observed
and unobserved initial, append and remove attempts match raw serial and two-worker
fallback results, including exact head sets and signed event delivery. Source
checkpoints witness each branch, referenced-head selection, smaller and real
empty frontiers. `test_recursive_union_workers` separately requires a generated
EXCHANGE plan and nonzero TDD execution statistics before comparing exact sets.

The frontier stop matrix now covers seventeen checkpoints, adding map allocation,
partial 64-byte initialization, later audit entries and later-branch selection.
Authentication tests reject copied bindings and copied, misaligned and out-of-range
operator addresses; reserved old-head union slots remain unmapped. These are
integration checkpoints, not new primitive exact-cost formulas.

`test_linear_union_degree` rejects malformed/opaque/unsupported stack expressions
and `(recursive UNION seed) JOIN recursive`. `test_default_linear_union_plan`
requires natural linear alternatives without K_FUSION; the existing nonlinear
fixture still requires K expansion. Nonlinear borrowed-body fallback remains an
explicitly separate case. Persistent traces/frontiers and incremental retraction
remain unqualified and open.

## Positive linear frontier bundle: authority and recovery

The recursive UNION and acyclic append UNION execution units share the governed
operator-source map, while retaining mode-specific decoding. Their integrated
qualification is a separate atomic commit in the same PR.

`frontier_map_authority` runs at actual recursive and append selections. It
rejects copied stale records, live generation/attempt mismatches, copied,
misaligned, one-past and foreign-head operator slots. Live fields are restored
before evaluation continues. The added old-head VARIABLE is verified to remain
an unmapped full-source read. No freed transaction storage is dereferenced.

`test_controlled_append_successors` exercises twelve UNION checkpoints with
both cancellation and work limits: map allocation, partial zeroing, later source
audit, eligibility, suffix copy, plan preparation, later JOIN-right branch,
selection, later head, claims, cutoff and retirement. Existing publication,
pending-input, retry/event and governor checks apply. Additional generated cases
verify differential opt-out, callback clear/re-enable after completed operations,
nonmatching/duplicate contributions and zero-row mutation no-ops. These are
integrated host checks, not primitive exact-work formulas. They do not qualify
initially unobserved acyclic append or an empty selected append suffix; real
empty selected frontiers remain covered by the recursive fixtures.

`test_controlled_frontier_map_failures` independently exercises acyclic and
recursive hosts. It distinguishes combined-metadata governor denial (`ENOSPC`)
from test-injected map malloc failure (`ENOMEM`). The no-JOIN fixture excludes
index reclamation from hiding denial. Competing credit is released before
cleanup; failures preserve public snapshots, pending input and zero events.
Reset/retry matches raw head sets and signed events, repeated STEP does not
replay delivery, and checked destruction restores aggregate governor credit.


### Callback-free successor integration (#2294)

Completed callback-free acyclic STEP admits declared public EDB mutations and
promotes the next evaluation to the general private-head transaction. Initial
legacy admission costs remain unchanged. Failed and zero-row mutations do not
promote. General successors preserve public heads and input staging until the
coordinated cutoff, and retained attempts continue to reject input/callback
changes. Callback replacement is admitted at quiescent completed boundaries;
newly enabled notifications compare against the preceding completed heads.
`evaluation_lifecycle` qualifies generated unary and dependent/multiple heads,
duplicate and nonmatching inputs, callback changes, repeated signed cycles,
setup/later-child stops, reset/retry and governed destruction. Existing
`evaluation_serial` primitive and initial-host boundary assertions remain
independent. This adds host coverage, not new operator/representation support.


### Evaluating SNAPSHOT integration (#2295)

Fresh and normally dirty controlled snapshots use the general private-head
transaction. The complete staged registry supplies snapshot rows before
retirement claims; copied tuples and observer events are admitted before one
publication cutoff. Evaluation, preparation and the completed report share one
attempt ID and allowance. After cutoff the complete tuple batch is delivered
before fallible retirement; callback cancellation remains sticky for the next
attempt. Stable snapshots keep their independent charged staging path.

The synchronous wrapper drains internal continuations. Its payload and callback
context never escape the call. Any failed precommit snapshot discards its
private transaction, including a failure at CLAIMS/CUTOFF; retry reconstructs
it without skipping preparation. Internal DIRTY and SNAPSHOT_RETRYABLE states
separate legal evaluation from an unrelated retained STEP. A completed numeric
recursive host with current authorized append provenance and no active frame
is normal DIRTY input too; its snapshot rebuilds through preserved recursive
bodies and the same private transaction. Other backends must
explicitly opt in before controlled snapshots may evaluate.

`evaluation_lifecycle` proves fresh/dirty observed and callback-free reference
parity, shared attempt identity, evaluation/preparation/claim/cutoff stops,
allocation failure, unchanged public registry/heads/input generations, protected
mutations/STEP during snapshot retry, different retry callback contexts and
postcutoff full delta/tuple delivery. Generated numeric recursive
STEP/append/SNAPSHOT, preparation stops/retry and future STEP parity are also
qualified. Its measured whole-host one-short test is
empirical and does not replace independent primitive cost oracles. Previously
unsupported operator and representation domains retain their separate owners.


### Mixed lifecycle qualification (#2294 / #2295)

`evaluation_lifecycle` additionally stages mutations across multiple EDBs before
one successor, alternates STEP and evaluating SNAPSHOT, changes callbacks at
completed boundaries and compares relation-qualified tuples and signed events
with raw sessions. A source compaction refusal caused by live storage ownership
must still match the raw contract; admission does not erase that protection.
Actual in-evaluation hooks reject nested STEP/SNAPSHOT, input mutation,
destruction and control reset/configure. A separate live STEP and an externally
active control attempt remain protected.

Supported FLOAT scalar schemas/physical values, nullary output, empty input and
rejected input are exercised through actual generated snapshot hosts. Empty and
no-output cases have nonempty positive controls. Metadata/value reservation
denial remains ENOSPC with denial metadata, distinct from value allocation
ENOMEM; failed fresh/dirty snapshots preserve heads/events and retry, and
checked destruction returns all governor credits after program ownership ends.
The installed typed facade has no evaluation-control attachment API in this
scope (#1823); these are typed engine/source tests, not a claim of end-to-end
controlled installed typed-facade coverage. Existing raw typed facade regressions
remain part of the broader suite. New scalar/operator domains retain #2299–#2301.

## Differential selector and removal qualification (#2296 / #1953)

The `evaluation_differential` suite adds the following current dispositions to
E07/F04/F04a and the selector intersection of E08/C04. It does not enable the raw
synchronous differential dispatcher inside a controlled attempt.

| Predicate | Controlled destination and evidence | Remaining boundary |
|---|---|---|
| Non-strict scalar five-op batch fallback, iteration zero | VARIABLE AUTO/FULL/DELTA/EMPTY/EMPTY_AFTER_SEED and JOIN-right AUTO/FULL/DELTA use existing charged composed reads and JOIN. 375 internal-plan/source combinations compare exact tuples to an independent selector oracle and the same raw plan. Smaller matching deltas distinguish selection from a full read; right AUTO stays full when left is delta. Stops immediately after right selection retry without publication. | Internal-plan selector contracts are labeled as such; no compiler-generated claim for edited modes. Strict/output/worker/foreign-state guards stay unchanged. |
| Completed callback-free binary INT64 `edge`/`path` host with preserved declared body | Read-only admission proves quiescence and current old closure/input provenance. Only a successful nonzero removal promotes to the existing general transaction; old public heads survive until cutoff. Generated default differential-enabled tests cover STEP/SNAPSHOT, append-before-remove, repeated same-source removals, callback changes at completed boundaries and later mutations. | A failed or zero request does not promote. Real source readers still refuse destructive removal. General recursive domains remain #2302. |
| Fixed versus dynamic private bindings at ordinary controlled JOIN | Fixed bindings decline general-only persistent index reuse and keep the authenticated bounded local index. Dynamic bindings still require the exact live general transaction identity; copied/forged dynamic bindings are rejected. | No new index owner or raw fallback is introduced. |
| Generated scalar JOIN/dependent/stratified-negative mutations | Public insert/remove operations drive observed/callback-free STEP and SNAPSHOT. Nine cycles include mixed mutations, duplicates, missing removals, left/right deletion, restoration and genuinely empty results with nonempty controls. A raw observed STEP naturally selects differential mode; controlled hooks independently witness private evaluation and the absence of append overrides on removal. Exact relation-qualified tuples and signed events match references. | The raw incremental removal producer still stages one input relation at a time. A conflicting second input removal remains EBUSY; same-input accumulation and later operations remain usable. |

`test_recursive_removal_recovery` supplies route-local #1953 evidence with actual
session admission: pre-cancel/entry exhaustion, metadata/lease/child/claim/cutoff
stops, private JOIN index building and no-output probing, and late post-cutoff
stops. EAGAIN retains one attempt ID. Precommit returns preserve the old head
identity/content, changed EDB generation and pending input; released claims
permit a new reader. Reset/raised-limit retry starts a fresh report and emits
exactly the removed tuples once. Post-cutoff delivery completes before retained
retirement; a late work stop can be reported without rolling back delivery.
Frozen reports, subsequent insert/evaluation, checked destruction and zero
governor credit after full ownership teardown are asserted.

The extended bundle qualifies physical scalar timestamp/signed-weight
transport, value-projection dispositions, stable consolidation representatives,
actual session/removal recovery and timestamp ownership retirement; see
[PHYSICAL_METADATA_AUDIT.md](PHYSICAL_METADATA_AUDIT.md). The complete iteration-cap
inventory and convergence/stop/retry fixes are in
[ITERATION_CAP_AUDIT.md](ITERATION_CAP_AUDIT.md). Raw foreign flag admission,
persistent weighted traces, compound/spill representations, other recursive
domains and #2293's platform/final-SHA acceptance are not inferred from these
results. These remaining predicates retain their existing issue owners;
completion of this bundle alone does not close every #2296/#1953 obligation.


## Controlled per-batch session consumers (#2297, restricted domain)

`evaluation_join_hosts` witnesses actual prepared producer generations,
per-batch FILTER/MAP, transformed sink rows and serialized extent/ACK in the
original unobserved non-strict five-op STEP fixture. It covers64 candidate pairs,
32 two-row batches,257-lane rows, narrowed projections, arithmetic MAP, actual
physical source stamps, left-delta and owned forced-empty inputs. Independent
literal tuples and a control-disabled reference check results and source
images. Reject-all traversal and terminal no-batch schema are separate cases.
No output generation is forged for an empty producer.

The original owning entry moves to a frame-owned parked slot while the batch
is borrowed. Exact host/generation/op scope admits only the current consumer;
ordinary stack access, owner release, parent publication and cleanup refuse
while the entry is parked. Actual consumer reset/ACK remain legal. Restore
precedes producer ACK, and each independently owned transformed relation is
retired through an actual funded scalar receipt. The final original input is
validated reversibly under its original target before replacement.

Deterministic cancellation/work-limit cases include consumer source-end,
private commit, control release, finish, reset and ACK for FILTER and MAP,
restoration, producer ACK and final private replacement. Empty-schema aborts,
allocator/admission failures and a late arithmetic ERANGE leave public heads
unchanged; reset/retry and checked destruction retain their resource contracts.
The tests assert the original frame/control identity and reject copied owners,
wrong operations/generations and premature parent access.

Direct composed384/383 and real classifier-authorized session1152/1151 funding
cases preserve actual previous receipts. The latter's finished reports show
768 consumed on refusal and1152 on progress, with no consumer action on the
refused grant. They establish the facade chain and its nested root's remaining
768 capacity by decomposition of the unchanged384 source fees, not by sampling
active accounting or inventing standalone root authority. Final original work
is compared empirically with normal-funding runs; primitive literal-cost
oracles remain independent. An unauthorized manual batch request remains an
ENOTSUP test and consumes its genuine bootstrap receipt.

This unit does not qualify other batch plan shapes, SEMI/ANTI hosting, strict
mode, local/shared output caps, observed/SNAPSHOT wrappers or their successors.
Those remain mandatory bundle work; E09/C04 and #2297 are not closed here.


### General batch admission and FULL probe prefixes (#2297)

`evaluation_join_probes` extends the actual session host beyond that original
fixture: a compiler-generated negation body, an explicit SEMI projection into
an ANTI stage, no-FILTER/no-MAP tails, missing-right truncation with and without
an earlier FILTER, NULL keys, unresolved-name column-zero fallback (including overflowing and
non-ASCII numeric suffixes), platform-specific `strtol` index conversion, a large
FULL right source and a distinct `$d$gate` canary, and a multi-head plan.
Literal relation-qualified tuples and the same control-disabled plans are
independent result oracles. These internal edited-plan cases are not claimed
as compiler-generated selection; the negation and multi-head bodies are.

The schema-only factory and owned template retirement run before producer
creation. Hooks assert no probe-index work during schema preparation and
positive index/consumer work after producer preparation. Exact stage FULL
source identity is checked at consumer START; delta substitution and writes
through the retained source reader are rejected. The effective prefix length
leaves excluded stages on the original numeric cursor, so a missing-right
standalone probe preserves the entire entry and later MAP executes once.

The new tests stop at every schema construction call, FULL/key qualification,
partial stage initialization, schema END/retirement, producer/consumer/ACK and
private replacement. Allocation faults and enforcing-budget denial drain
all temporary resources; reset/retry matches the canonical reference. Actual
classifier-authorized facade grants1151/1152 exercise probe START/STEP/ACK
with finished receipts768/1152 and unchanged final original consumption
compared to an ordinary funded host run. Those host totals are empirical;
the existing primitive exact and one-short contracts remain separate.

Strict/cache/local-cap ordering, shared-count operation rollback and observed
STEP/SNAPSHOT/successor policy qualifications remain required before claiming
all E08/E09/F01–F03/F02a–d/C04 predicates closed. The empty probe fixture
retains an initialized scalar schema after removing all rows; an unspecialized
empty EDB descriptor still needs its own bounded source disposition.


### Authentic unspecialized empty-source reader (#2297, primitive)

`relation_empty_source` exercises the real no-insert INT64 EDB constructor
state, separately from initialized empty FLOAT/STRING tables. A distinct
empty-source record acquires both descriptor and canonical-owner gates before
checking the fixed no-payload image. It preserves declared width/graph
metadata without manufacturing physical columns or scalar HELD authority.
First insert, direct schema mutation and checked destruction refuse while
that lease is held, and succeed after its actual gate releases complete.

Original-attempt acquisition and validation remain separate from independently
funded END. Tests cover partial acquisition, pre-validation and late stops,
wrong-thread/copy/stale-ID calls, malformed payload/Arrow/alias states, frozen
original reports, and normal insertion/scalar reading after drain. This is a
source primitive foundation; VARIABLE/JOIN/probe/LFTJ destination integration
for unspecialized empty EDBs remains outstanding until exercised through the
real session hosts.


### Borrowed no-insert VARIABLE and ordinary JOIN (#2297)

Normal FULL/AUTO VARIABLE now acquires the authentic empty-source reader and
installs the exact borrowed descriptor after actual END. Direct numeric and
composed fixtures assert physical width0, `schema_ok=false`, unchanged Arrow
state and future graph metadata; declaration width never becomes a physical
column. Forced-empty constructors retain their existing admission boundary.

Ordinary owned JOIN has a separate authenticated metadata view for these
sources. Left-empty, right-empty and both-empty fixtures compare raw results,
projection/default types, output ownership/delta and FLOAT mismatch errors.
The producer host uses a zero-width routing hint before normal scalar HOLD;
the ordinary destination independently proves the empty constructor image.
`evaluation_join_hosts_domains` also runs genuine compiler/session no-insert
inputs with batch0 and configured64. It exercises first insertion into those
same descriptors after completion and an admitted ordinary dirty successor.
Configured dirty batch admission is still separate policy work; that successor
witness deliberately disables batching and does not qualify that policy.

Numeric and composed source END and owned non-stream JOIN reader END receive
the actual independent funding before the generic HOST384 debit. They pay
dispatch1 plus the reader's real release/final-clear charges; pending receipt
consumption and normal cleanup retain HOST384. Zero-success intermediate
release and CAS retry do not authorize install/commit/ACK. Tests cover a
refused dispatcher, dispatched funding refusal, partial descriptor acquisition,
cancellation/work stop, malformed source shape, protected schema/destruction,
completed-report preservation and first insertion after drain. The new query
refuses a real active stream frame; no stream cleanup authority is introduced.

A stale standalone missing-right probe rejection is corrected to the existing
exact-repush semantics and checks that the original entry is unchanged. This
unit does not qualify forced-empty bare selectors, present-empty standalone
SEMI/ANTI, LFTJ physical-zero keys, strict/cache/output-cap/shared-count policy
or observed STEP/SNAPSHOT and configured batch successors. Those remain
required in this grouped PR before either issue can be closed.

Zero-row right FILTERs still validate size/data and the real new-like graph
recipe before key/type/output work. SIMPLE recognition shares the metered
instruction decoder and exact raw grammar. NON-simple setup uses the raw
compiler opcode domain: STRING comparisons really compile, HASH/CRC and
malformed/unsupported programs take the unevaluated fallback. Its compiled
owner is stored only for real PROGRAM cleanup and is never passed to numeric
evaluation. Existing nonempty numeric compilation keeps its prior domain.
Tests distinguish these paths with actual allocation/checkpoint witnesses,
including maximal VAR digits, invalid/truncated forms, NULL data, graph
recipe errors, FLOAT mismatch, missing/forced-empty bypass, original stops
and actual compiler ENOMEM/ENOSPC with source/program credit recovery.
The bounded path omits optional empty filtered-table/cache materialization;
it governs every allocation it actually attempts and does not claim identical
cache-specific memory thresholds to raw execution.

The same session-host empty-input matrix additionally runs an explicitly
modified right-FILTER plan with batch0/configured64, then first insertion and
ordinary dirty execution. This edited-plan witness is separate from its
unchanged compiler-generated base plan.


### Forced empty VARIABLE materialization (#2297)

The genuine empty-source record now also reaches the existing forced-selector
SLOT/NEW/END protocol. Its new-like sibling revalidates actual HELD authority,
constructs an owned initialized physical-width0 relation and preserves the raw
declared metadata without fabricating a scalar HELD record or physical lane.
A future graph column at width0 produces the raw clone EINVAL; normal FULL/AUTO
borrowing still preserves that original descriptor. Existing initialized
physical graph cloning remains on the unchanged scalar recipe/fee path.

Direct numeric and composed fixtures compare raw VARIABLE metadata, exact
borrow-versus-owned identity and delta flags for FORCE_EMPTY, seeded missing
FORCE_DELTA, and the actually admitted iteration0 FORCE_EMPTY_AFTER_SEED AUTO
behavior. The latter is not claimed as the outbound-only recursive skip.
Tests cover genuine bare versus initialized graph sources, private output
allocation/admission failure, stops after real constructor credit commit and
before install, actual independently funded source drainage, zero publication
on failure and complete governor recovery after ACK. Primitive copy/native
thread checks also reject unauthorized calls to the new constructor sibling.

These are internal selector fixtures, not a claim that the compiler emits every
selector on the current public default route. Present-empty SEMI/ANTI, LFTJ
qualification and the remaining strict/cache/cap/shared/observed/successor
policies still follow in the same grouped PR.

### Present-empty standalone probes and ANTI identity (#2297)

Standalone SEMI/ANTI resolve the actual FULL right relation before source or
projection checks. A missing right preserves the entire original entry,
including delta and segment metadata. A present bare descriptor instead uses
its genuine empty-source reader and funded END. SEMI creates the raw projected
empty schema; ANTI copies when required by empty/unnamed/timestamped input.
For ordinary nonempty named input ANTI preserves the original relation and
ownership, graph and sort metadata, and clears delta/segments as raw does.

That identity disposition has a dedicated authenticated numeric cutoff. It
checks the original entry and captured source generation after all readers and
temporaries drain, then transfers only the segment allocation into the inert
normal retirement slot. Existing finish frees that allocation once and never
retires the live relation. Fixed original work regions are CONTEXT224,
STATE192, three SCRATCH128 regions and CUTOFF256; the existing FIRST_FINISH256
pays normal segment disposal. These are logical work envelopes, not latency
bounds. The call-local guard blocks recursive output/commit/reset and duplicate
cutoffs before old-target access or output mutation, and clears on every exit.

Tests compare actual raw missing-right and present-empty dispositions; cover
owned/borrowed sources, bare/initialized right, valid graph/names/sort metadata,
timestamp and unnamed copy branches, NULL/non-NULL segment pointers with zero
and nonzero counts, stale snapshots and wrong context/op/disposition, original
cancellation and an empirical exact/one-short host admission pair. Real
pre-cutoff and post-cutoff recursive calls leave checkpoint and original work
counts unchanged. Cancellation requested after cutoff does not undo success.

A real configured64 pipeline fixture truncates before a bare FULL SEMI right,
restores the entire owned 28-row prefix, then independently executes that probe
once. Its distinct nonempty delta canary prevents accidental delta lookup from
passing. The physical-width hint selects this fallback only; the standalone
reader supplies authority. LFTJ first-pass error order and the remaining
strict/cache/cap/shared/observed/successor policies remain separate units in
this grouped PR.

### LFTJ source-by-source sizing and error order (#2298)

LFTJ now completes each actual source lookup, incremental combined reader
acquisition and raw key/checked-size preflight before looking up the next
source. The raw fixed per-input sizing terms and row/width byte additions use
an independent semantic accumulator; they are not a fictitious reservation for
raw workspace. Type/shape compatibility waits until every input passes that
first pass. Genuine HELD readers are reused, with native token, descriptor and
canonical-owner gates and captured source generation revalidated; acquisition
is never repeated as a held-state query. Cleanup high-water includes a slot
before its first descriptor acquisition, and HELD clears only on actual final
funded release. Partial descriptor owners remain discoverable on every stop.

Original reader acquisition retains ACTION72. Each subsequent fixed protected
validation plus one arithmetic step, or one type/shape postcheck, uses a named
SOURCE_CHECK192 region; the raw fixed sizing prefix uses8. No new dynamic scan
or width/row loop is hidden under these fixed regions. Signature/name lookup
and existing copy/sort/seek/output work remain independently metered. The
source-order correction does not alter raw LFTJ strict/local/shared policies.

An actual pre-fix internal composed fixture returned raw EINVAL versus
controlled ENOENT for bare input0 followed by missing input1. New comparisons
cover both missing/key orders and earlier type mismatch versus later missing
or invalid key, with a postcheck witness distinguishing equal EINVAL paths.
INT64 empty output comparisons use effective NULL=>INT64 type semantics;
existing tests that require physical type-array presence remain unchanged.
FLOAT/STRING empty and graph cases additionally prove actual private source
preparation, separately from natural typed final-head session controls.
Checked arithmetic edge tests use real helpers, not forged enormous buffers.

Ten deterministic stops cover descriptor-only acquisition, genuine combined
HELD, every raw arithmetic stage, the next input and postcheck. Tests check
exact accumulator progression, cleanup high-water, unchanged source snapshots,
mutation EBUSY while held, short independent dispatcher refusal, fresh funded
drainage and successful insertion after ACK. Existing copy/sort/seek/output,
private-shadow and natural nonempty typed controls continue to run.

A separate schema-context defect was exposed during this qualification: raw
LFTJ constructs default-INT64 intermediate output while the private controlled
output carries captured FLOAT/STRING types. A real admitted internal composed
FLOAT-empty LFTJ followed by FLOAT JOIN returns raw EINVAL versus controlled
success. This is not normalized away as nullable INT64 representation. The
following separate causal schema correction records its resolution. The strict,
cache, cap/shared accounting and observed/successor policies also remain open.

Framed reader-contention and partial-abort fixtures now wait on the actual
RESOLVE/SOURCE_READER phase. They retain real descriptor/storage writer denial,
combined-reader transition and pending cleanup assertions. Duplicate input0
still rejects with its writer held before acquisition; a later duplicate now
explicitly observes the earlier combined reader and checks full drainage on
ENOTSUP, rather than holding an artificial writer forever before that lookup.
The raw reference's actual sorted-arrangement cache is released through its
normal API before referenced sources are destroyed. The first failed expanded
validation (that test wait assumption and a raw-fixture sorted-cache leak) is
preserved separately and does not approve this corrected candidate.

### LFTJ default output schema and continuation parity (#2298)

Controlled LFTJ now uses the existing auto operator constructor for its private
output, matching raw NULL column_types, default INT64 Arrow fields, generated
names, declared/graph metadata and no timestamps. Input key types still govern
actual FLOAT ordering/canonicalization and STRING-code comparisons. A later
plain projection inherits raw default types; a typed expression derives its
result type from its actual opcode. Captured source types no longer leak into
following JOIN/SEMI/ANTI compatibility decisions. No raw engine or compiler
behavior changed. The already charged source-type scratch pass remains in
place as private work and does not determine published output schema.

The constructor retains its actual ENTRY128, PREPARE128, BUILD_CALL128 and
EXTERNAL128 envelopes; builder byte/column/allocation regions still charge
individually. OUTPUT_CREATE's fixed suffix fits the established external caller
room. Total work is not asserted identical to the formerly typed constructor,
and primitive fees remain unchanged. Initial capacity uses actual pre-counted
rows as a bounded allocation alternative, not a promise of identical raw
cache/resource thresholds. Lexical partial outputs are destroyed on error;
only successful construction installs the owned LFTJ output slot.

The original admitted FLOAT-empty LFTJ→FLOAT JOIN red fixture now matches raw
EINVAL; nonempty FLOAT JOIN/SEMI/ANTI also match raw errors and preserve source
images with no public output. Internal complete-plan comparisons include empty
and duplicate nonempty INT64/FLOAT/STRING-code inputs, raw direct output schema
and physical row bits, ordinary/probe successors, plain projected MAP and
nonempty opcode-directed FLOAT addition followed by consolidation. Only
NULL=>INT64 representation is normalized in generic continuation comparison;
FLOAT/STRING are never declared equivalent to raw default metadata. The direct
LFTJ oracle additionally requires actual NULL types and matching Arrow format.

Natural compiler-selected wide/nonleading/duplicate session fixtures now check
final head names, effective types, Arrow formats, declared/graph metadata and
content in actually admitted plain/callback STEP and SNAPSHOT cases. Their
FLOAT head obtains its type through the real generated continuation, rather
than leaked LFTJ source metadata. They do not qualify still-guarded modes.

Existing output allocation and admission failure/retry tests now also stop
after actual constructor credit commit and immediately after successful
untyped output construction. Original cancellation/work exhaustion produces
no event or head, preserves source snapshots, drains real owners and recovers
governor credit; retained prepared host images keep mutation protected until
reset/retry or checked destruction. Type-sensitive consumer errors likewise
retain only private cleanup-owned prefix state. Strict/cache/local/shared and
general observed/SNAPSHOT/configured-successor policy completion remains
required later in the same grouped PR.

STRING-code continuation controls distinguish exact type policy from bit
identity: default-INT64 LFTJ output does not equal a STRING-typed right key,
while an INT64-typed right carrying the same codes is a positive match. Both
ordinary/probe results and independent literal cardinalities cover these
negative/positive pairs; no output is not treated as proof of code loss.

### Pin-free standard cache-original copy and owned-output abort

The internal cache-scalar-copy attempt clones the standard named canonical
heap/no-timestamp schema produced by actual ordinary and differential
materialized JOIN. It acquires genuine incremental descriptor/storage readers;
canonical sources with live alias borrowers remain readable. Source lifetime
is still the caller's responsibility: these readers cannot substitute for an
unpinned cache result's missing owner. No cache lookup, pin, statistic, victim
transfer, insertion or public policy admission is added by this primitive.

The held recipe drives a private metered constructor with retained cleanup
ownership at the final copier address, using the source's actual capacity,
names/types and declared/graph fields. The shared ordinary constructor remains
unchanged. The private output has constructor-issued exclusive ownership until
funded TAKE. Each
physical 64-bit cell is copied directly, including FLOAT bit patterns and
STRING codes; typed append is not used. Base rows and valid sorted/run
metadata match raw governed deep copy, including unused run slots; invalid
run metadata is normalized to zero. Fresh descriptor identity is independent.
Only copied status/shape is queryable. A separate real128 grant transfers the
completed output after source END. ACK rejects an untaken owned output.

Late forward failure retains the initialized constructor prefix and its real
reservations, or the installed output, at the original copier address. Fresh
CONSTRUCTOR_END192 turns dispose one actual child/name/column/table/descriptor
or release the saved governor reference. Each admitted token adds a real121
native refund admission:312 refuses after consuming192 with its token, flags,
credit and phase unchanged; fresh313 progresses exactly one refund. Complete
containing source is at most240 with real intervals at most133. No synchronous
constructor destructor is hidden in the failing forward return. Original END
and a live successor with zero consumption are covered alongside a genuine
17-byte committed peer that survives all cleanup. Genuine first-allocation
failure exercises a RESERVED token; cancellation after the last commit
exercises all three COMMITTED tokens, without seeding native phases or flags.

The selected reserve SOURCE caller160 owns current81 and one-use78 (159 total),
including the ordinary three-cell `ref_get` body. Typed OVERFLOW has a separate
current83/terminal28 history. Private saved-ID phase/tail/verdict checkpoints
do not exempt ordinary `charge` or its mutex/locked bodies. ADMITTED preserves
the hook/guard/zero-check order in its owning reserve body. The original-only
known64 credit helper preserves counter/CAS semantics and counts the first
native iteration; complete credited restoration including its caller fits64.
Arbitrary ROUND164 retries fund themselves. Actual unprocessed source at the
dominant cut is235-3 evaluated fronts+3 caller error cells+18 constructor/copier
ancestor cells=253/256. BUILD192 pays its complete120, including those18 cells.
Commit caller128 owns80 current+11 bootstrap+36 terminal=127. All original
prices, callback work labels and report amounts remain unchanged. Generic
capture/refund callers retain their separate source obligations.

Independently funded END drains source readers. Separate256 ZERO and BEGIN
turns clear the scalar and managed records respectively. BEGIN establishes
native identity and four final-address EMPTY tokens in that same activation.
PREPARE takes192 wrapper plus192 native-source work, with exact383/384 refusal
and progress tests; its existing finite phases validate one token or one
column/pointer pair per turn. COMMIT uses128. STEP uses256 plus its reached
native1 or64 debit, with255/256 outer boundaries and actual320 credit turns.
TAKE and ACK each require128. All these controls are independent of the
completed/stopped original; the shared scalar/managed API prices stay intact.

The private issuer creates flat, no-timestamp, no-ledger output. Retained
payload phase21 code is outside that positive provenance; a future genuine
producer reaching it requires576 and a fresh source/caller/driver proof.
The current512 cleanup driver does not qualify that future route. This unit
qualifies only the selected issued copier and its reached native dependencies;
other SOURCE callers, generic ownership and full materialized session policy
remain separate obligations.

Tests compare raw deep copy and literal metadata/cells for empty/nullary,
wide257 and grown-capacity sources, native types, FLOAT raw bits, run metadata
and live canonical aliases. Actual raw ordinary/differential materialized
originals remain standard even when inputs carry timestamps, graph or SIDE
compound metadata. Direct TS/compound inputs are explicitly outside this leaf,
not evidence of completed natural cache policy. Cancellation/work exhaustion
at forward phases, constructor allocation/admission/credit stops, partial
reader drainage, insufficient cleanup grants, reentry/copy/thread misuse,
output transfer and ACKed reuse exercise complete ownership recovery.

Separate direct scalar-retirement controls keep caller-owned external CACHE
debt through physical DONE, or exercise retained RELATION debt with no cache
ledger. Unrelated CACHE/RELATION/TIMESTAMP canaries prove subsystem separation;
this is physical-owner evidence and does not implement cache debt transfer.
The later registered pin/exclusion/insertion owner and strict/cap/shared and
general observed/configured-successor qualification are still required for
the grouped PR's completion.

### Selected JOIN/LFTJ scalar-retirement initialization

Actual frame-owned unary retirement, JOIN shape/transformed/sink retirement,
prepared batch abort and failed LFTJ output retirement use an additive
parent-bound initializer. The old raw scalar/managed begin, prepare, cancel
and step APIs keep their source and literal primitive tariffs. The previous
host formula187 did not cover scalar984B plus nested managed928B clearing;
these controlled callers no longer invoke that duplicate-clear sequence.

The actual parent selects its exact relation, scalar address and ownership
role before the first hook. BOOT128 records final-address/native identity;
ZERO256 clears scalar storage once; BIND192 establishes managed identity,
relation/address/phase and invokes all four genuine reservation initializers.
BIND latches the parent's begun flag immediately. RESET128 acknowledges only
the small initializer; its refusal retains the genuine begun receipt. No
prepare, commit or physical retirement is appended to these four actions.
The ZERO inventory includes123 eight-byte clear units and at most128 fixed
caller/helper units. BIND includes four token clears/initializers60, loop10,
managed fields10 and fixed caller/helper96. Compile-time layout guards keep
both within their declared regions. These are source inventories, distinct
from the runtime grants used to demonstrate composition.

Cancellation of old input preserves its relation/debt. Before BIND no writer
claim exists; after BIND a separately paid256 validates/releases the actual
two-writer round, invalidates ready and latches cancel-clear before return.
A later256 clears scalar storage. Private output remains with its parent
until genuine scalar commit transfers disposal responsibility. Physical DONE
latches a drained state and returns; another256 clears terminal storage
without dereferencing the freed target or repeating physical retirement.
Parent busy/selection flags prevent reentrant mutation, retarget or cleanup.

Failed composed LFTJ output retirement has an authenticated narrow funding
class: direct wrapper128 plus one actual initializer/retirement action.
ZERO256 and terminal clear256 therefore progress with384. Entering RELEASE
is a separate128 transition, and BOOT/BIND/RESET consume their own real fees.
The class checks actual final-address/native frame/session/numeric owner,
original control/ID, failed disposition and retained output/receipt state.
Successful RELEASE and input-only cleanup retain their old384 wrapper and
up to386 whole-child cleanup costs. A fresh explicit cleanup request still
requires a genuine sticky original stop; a retained authorized failed
continuation survives original finish without polling that ended attempt.
The framed root keeps its separate384; root plus ZERO is768, never384.

Tests compare genuine initialized fields/token identities with raw begin;
exercise poisoned inert bytes and BOOT127/128, ZERO255/256, BIND191/192,
RESET127/128, actual READY cancellation and physical-DONE-before-clear;
reject copy/thread/target/reentry misuse; and preserve source snapshots,
ledger canaries, governor balances and finished-original reports. Actual
prepared batch abort retries every initialization phase with short grants.
The real composed path proves383 consumes wrapper128 while refusing ZERO;
the real framed path proves767 consumes512 while refusing ZERO and768
executes it. Successful RELEASE remains outside the new cleanup class.

This caller set alone does not cover containing session staging, recursive-round,
original CLAIMS or transaction-disposal initialization. The session protocol
below supplies those distinct ownership and rollback transitions.
The raw LFTJ metadata preflight remains a compatibility helper; the actual
controlled host uses the separate metadata cursor documented below rather than
charging its whole scan inside a fixed constructor envelope. Prepared unary
publication, registered cache
ownership/exclusion/insertion, strict/cap/shared policy and observed/configured
successor qualification remain prerequisites for the completed grouped PR.

The selector matrix retains all375 cases and two distinct stop routes. The
AUTO full-right case witnesses actual prepare self/control/session/op and
left-delta/full-right pointers, then checks payload/snapshot images only after
both genuine combined readers and valid snapshots exist. Cancellation fires
at actual host PREPARED. The forced-right-delta case keeps ordinary JOIN phase7
and its actual used-delta progress1 work stop. The obsolete common phase7
witness deterministically failed on the pre-initializer HEAD as well, because
the full-right host never calls it. Corrected instrumentation retains exact
stop codes, no publication/output before retry, source snapshot parity and
exactly one later publication with unchanged literal/raw output comparisons.

### Containing session retirement and native cancellation

Staging removal, recursive-round temporaries, per-original reversible CLAIMS
and failed/successful private candidate disposal use final-address session
progress alongside each existing scalar receipt. Selection stores the actual
parent, exact receipt/target/slot/replacement, role/index and native owner
before any initializer hook. Staging saves name-scan completion and its found
registry slot separately from scalar identity. Original/round/disposal indices
cannot advance until the selected action genuinely completes. Expanded
transaction/original sizes remain in actual admission and incremental zeroing.
No session caller invokes raw scalar begin or raw scalar cancel.

Each BOOT/ZERO/BIND/RESET is one actual shared-initializer action. BIND hands
a genuinely begun receipt to its parent before RESET may fail. Reversible
rollback obtains real maintenance first and uses distinct release256 then
scalar/progress clear256. Scalar clear includes123 eight-byte receipt units,
actual progress bytes and fixed caller/native/slot suffix under a layout
bound; it does not repeat raw cancel. Scalar commit remains the existing
single publication/disposal boundary. Physical DONE latches stable progress
and changes the exact owner slot callback-free, then returns. The next
clear256 never rereads or steps a freed relation. Per-original completed
retirement prevents RETIRE/destruction double stepping. Private EDB disposal
restores its public binding; failed head disposal nulls only its private slot.
A restored/NULL slot cannot make an outstanding terminal clear disappear.

The adjacent binary recursive managed path has a separate genuine SELECT128,
then one raw managed BEGIN256, existing managed leaves, and a later CLEAR256.
BEGIN inventories a single928B clear116, four token initializers60, loop10,
raw fixed12 and caller/native/funding/return at most58; it does not call scalar
begin or duplicate the managed clear. SELECT captures exact owner/native and
working/copy aliases in its own paid action. During destruction and token
retirement the selected continuation runs before any payload/NULL predicate
or independent abort disposal. Descriptor free can precede final credits;
no path rereads its target during that interval. At actual DONE the exact
owner and captured matching working/copy/typed source aliases become NULL
without another callback; later clear leaves unrelated/public objects intact.
Convergence/frontier code recognizes selected retirement before source reads.
Existing original per-leaf convergence work is preserved; new selection,
BEGIN, clear and cleanup funding remain actual maintenance work.

An unpublished legacy successor may own a native input reader before any
staging selection. Real private maintenance therefore exists before that
reader is acquired. A terminal error with either genuine selected staging
or this reader latches its first result and keeps the original STEP active.
STEP returns EAGAIN while its actual native owner performs one cancellation
or END action. The input reader END pays128 for one real release_step/CAS;
a combined token needs two successful decrements (contention may need more
attempts), storage first then descriptor. Stage1 does not revalidate an
already released storage hold. Owner-NULL is terminal only when every active
token field is inert. The saved native parent stays authoritative after
scalar clear until reader END and a distinct FINALIZE128 complete.

Foreign STEP during these selected/pending states returns EAGAIN without
polling/debiting the original or mutating the continuation. The real active
ID keeps generic pending admission closed, including after a foreign STEP;
checked destroy rejects before closing or clearing its caller slot. Only
after all native owners are inert does FINALIZE report the saved original
result once. Idle destruction can then run on another externally synchronized
thread. No native release, mandatory allocation or retry loop is delegated
to an already-entered void backend destructor. Committed publication uses
its existing protected physical cleanup and cannot be canceled by this
uncommitted path. Existing WORK_LIMIT logical retry admission remains intact.

A refused cleanup action keeps its actual owner and copied protocol diagnostic.
Recovered refusal history remains queryable internally until the next real
operation. Once every obligation completes, final cleanup outcome is success;
a historical validator refusal is not passed as an unresolved cleanup error
that would override the saved original cancellation/work-limit result.
Original allowance/report ownership is never reset or replayed for cleanup.

Actual tests stop scalar initialization/READY/physical-clear stages in legacy
removal, multihead original CLAIMS, general positive recursive rounds and
binary managed retirement. They compare original public/source snapshots,
literal/raw results, callbacks and final resource owners. Real second-thread
STEP plus checked destroy attempts run while selected, during cancellation,
at input release_stage1 and after reader-inert before FINALIZE. They preserve
live IDs, exact gate counts and copied status, then verify legal foreign idle
destruction or a genuine retry that reacquires the saved source and publishes
once. A pre-staging observer allocation failure exercises reader-only cleanup.
The actual owner STEP also receives genuine127/128 test grants at END:127
leaves both gates/stage untouched with zero consumed;128 performs exactly one
decrement. This conditional test funding seam lends no token or disposal
authority; production cleanup uses its actual private unlimited maintenance.
Final zero-governor assertions occur after sessions AND the program-owned
intern table are released, with the external governor held for inspection.

This unit changes retirement initialization, rollback and ownership only.
Prepared unary publication, registered cache exclusion/pins/insertion, strict
arrangement/cap/shared operation policies and complete observed/configured
successor qualification remain required before completing the grouped PR.

Every actual selected scalar/managed continuation also registers with its real
containing STEP. The count is exclusion provenance; it never supplies READY
or source authority. The final genuine clear unregisters exactly once.
Foreign entry is rejected before cold sealing, phase/index changes, original
polling or cleanup. With an active original it returns same-ID EAGAIN. If a
real completed WORK_LIMIT retained a selected round, it returns that same
verified completed report/result while backend admission remains PENDING.
Legitimate reset/configure changes future allowance without changing this
completed proof. Only a successful real next begin retires that proof. Once
all owners drain, idle destruction has no creation-thread restriction.

The actual max-attempt boundary is qualified separately. A retained general
transaction round can finish real attempt UINT64_MAX with WORK_LIMIT; its
next real begin then fails EOVERFLOW before creating another attempt. This
failure retains the captured native origin independently of the selected
count while actual transaction disposal drains readers and metadata. Each
native call advances the existing maintenance cleanup only; foreign calls
repeat the same authenticated past WORK result without advancing it. No
new original attempt, allowance, callback or completed report is manufactured.
Each cleanup wrapper pays128; a separate genuine128 finalization prepayment
occurs before disposal can release its maintenance handle. After count0 AND
transaction disposal finish, that paid fixed suffix returns the saved actual
EOVERFLOW. No finish is invoked for the nonexistent new attempt. The original
MAX-ID report remains intact, and later owner-free begin still fails without
wrapping. Tests seed only the supported inactive attempt generator toMAX-1,
then reach these boundaries by genuine execution. They cover count0 with
metadata still pending, peer rejection, no publication/callback and final
foreign idle destruction and zero governor balance. The nonmax fixture also
proves normal new-attempt resume and literal/raw output parity.

Begin-failure disposal also holds a distinct lexical guard across its entire
maintenance/disposal/finalization call. It remains set while real scalar
children use their separate per-leaf guard, and while metadata reservation
FINISH_PREPARING/FINISH_RELEASING callbacks run. Backend STEP and SNAPSHOT
reject reentry before proof, funding, cold sealing or parent changes. The
actual MAX fixture reaches both exact metadata-token callbacks after count0,
then checks direct backend reentry leaves parent indices, token ownership,
public source, original report, governor observations and actual current
maintenance consumed unchanged. The test-only counter query copies the live
counter under its existing mutex; the completed-report API cannot supply that
observation. No maintenance handle or writable context is returned.

These source regions remain distinct: root read-only exclusion is at most98
fixed logical operations, BF turn wrapper including its lexical guards at
most124 within its own128 payment, and the future final fixed suffix at
most40 within its genuine prepaid128. Existing disposal leaves and allocator,
control/native runtime and registered cleanup boundaries retain their own
contracts; none of these inventories claims the entire disposal/STEP is128
or256. Test-only fixed counter observation has no production tariff or ABI.


### Bounded LFTJ metadata validation and stopped-parent cleanup

The actual numeric host validates LFTJ metadata before acquiring child resources.
HEADER, one input-name check per NAMES action, and VALIDATED each pay128.
All names, including input63 at the maximum k64, are checked before source lookup,
preserving raw error precedence. After whole-plan preflight, BIND_HEADER,
one BIND_ENTRY, and BIND_FINAL pay192; each prior-name pair pays128.
The dynamic session binding path uses its actual registered generation and a
separate192 validation. These actions preserve the existing outer host proof
and its separate384 charge; that charge does not subsidize metadata work.

The checked constructor authenticates the exact live numeric/frame/native owner,
operation, captured metadata header and binding generation without scanning
names again. PREFIX pays256 for its fixed proof and1128-byte prefix clear;
INSTALL pays192 for binding the initialized child, with identity installed last.
The module sets a dispatch marker only around the callback-free constructor,
after the metadata checkpoint. Reentry from that checkpoint, copied parents,
wrong targets and changed headers cannot acquire the child or spend original
work. Legacy direct constructors retain their compatibility preflight.

A genuine completed CANCEL/WORK original can leave validated metadata before
any child exists. The dedicated stop-only cleanup preserves the actual completed
report and requires distinct live funding. It pays192, rechecks original report
and funding generation after its last hook, excludes output overlapping the
whole parent or actual frame, verifies actual empty frame state, then clears
only the fixed metadata proof. A separately funded384 ACK performs normal
frame release before dropping the parent's final original-control reference.
Neither action revives the original attempt or relaxes normal private-binding
validation. Active successors or replaced completed reports refuse cleanup.

The numeric suite exercises all64 names, null tails and source-error priority,
all four static bindings/six pairs, exact/one-short action grants,11 genuine
metadata stop phases under CANCEL and WORK, real copied/native/reentrant
refusal, sole-reference ACK, occupied frame and hook-injected payload refusal.
Actual stack.top and result.seg_count output loans reject before debit and
remain usable for valid retry. A funding hook genuinely finishes/resets/begins
a new attempt; the old generation rejects without debiting its successor.
The host domains group additionally executes a k64 owned-plan fixture and a
real observed dynamic binding registry larger than the static four-entry limit.
The k64 fixture deliberately edits owned plan metadata before session binding;
it is not evidence that the compiler naturally emits a64-input operator.

This closes the metadata caller-scan gap only. Prepared unary publication,
registered cache ownership/insertion, strict/cap/shared policy and complete
observed/configured successor qualification remain separate bundle obligations.


### Copied framed authentication and status foundation

The framed host now matches active requests through a copied inspection view.
It contains only const comparison tokens, IDs, flags, cleanup key and status;
it returns no mutable context, frame, stack or relation loan. The error-only
frame authenticator compares actual active lineage before inspecting the frame
and verifies exact embedded context and native ownership. Ordinary mutation
still acquires its existing mutable context; this foundation introduces no
prepared-publication marker or replacement capability.

Direct framed status retains its384 independent-funding fee. Host STATUS keeps
its outer384 plus inner384, including the old one-short failure metadata: after
the first charge, a refused inner charge still reports STATUS kind and the real
cleanup key, while status stays initialized. Observation consumes no pending
grant/previous tuple or original work and does not require an active original
attempt. Direct invalid-native observation returns EINVAL; host inspection
retains EBUSY. Existing ordinary mutation and child tariffs remain unchanged.

Both status and key outputs must be external to the complete actual frame and
mutually disjoint. Before debit, status captures the original control identity,
attempt/operation IDs and all five cleanup-key fields authenticated against the
actual frame/session. After its test-only checkpoint, it first checks live
session.cleanup_active before any saved frame/context access, then revalidates
native ownership, complete receipt generation, control/operation identity and
funding generation. Thus genuine frame retirement or address reuse cannot turn
a copied status request into a stale read. The control charge itself has no
production callback; the explicit test hook exercises this defensive boundary.

Serial tests reach actual PENDING, READY and DONE, compare all copied fields
and untouched context/pending tuples, exercise direct383/384 and host767/768,
reject actual frame and mutual output overlap before debit, and verify native
refusal. A genuine finished/reset original successor receives no status debit.
The retirement hook actually closes the DONE frame, acknowledges its receipt,
and optionally begins a real new receipted framed owner; direct wrapper,
frame-based and host STATUS all refuse stale observation without touching the
freed context. The new owner is normally aborted/drained and governors balance.
No address reuse, READY state, receipt generation or prepared marker is seeded.

The fixed expanded logical source inventories are separate from the payments:
authenticate29, common validation31 (previous upper34), copied inspection65,
frame-status180 and old context wrapper215. The host outer139 and inner180
are separate existing protocol regions; no whole STATUS-call256 claim is made.
These observations supply a prerequisite for later prepared unary exclusion;
cache/strict/cap/shared and general transaction acceptance remain outstanding.


### Prepared unary replacement: Mode A ownership precursor

The internal prepared receipt now owns a real exclusion marker on the actual
numeric frame. This extends the preceding observation foundation; it does not
yet select prepared publication from JOIN_DECIDE, cache insertion or the session
transaction dispatcher. The first constructor admits borrowed old relations or
owned old relations with an already prepared scalar retirement. Owned inputs
without that real retirement refuse before marker installation or original
debit. The caller first drives the existing scalar initializer and preparation;
paid validation later checks the actual final-address, native, two-writer
retirement, rather than treating eligibility flags as authority.

The caller surrenders existing mutable loans before beginning. While marked,
new stack/result/context loans, generic commit, scalar mutation, numeric parent
progress/ACK, constructors and checked frame cleanup refuse. Copied observation
remains available. This is internal serialized ownership, not a new public
session-destruction admission gate: the caller must retain the actual
session/frame/parent through receipt ACK.

Preparation captures the seven-field old/output entries, compares fixed aliases
and one real stack entry per action, then acquires actual descriptor/storage
readers in separate steps. A partially acquired descriptor is retained and
released through its genuine token. Owned old input uses the existing scalar
writers instead of acquiring conflicting readers. Validation checks entry,
snapshot, canonical storage, generation, token and native ownership again before
sealing. The final latch pays its own128 and an additional real future128.
The callback-free no-fail leaf moves all seven old fields to retired, installs
the candidate, clears its former slot and records committed success; it does no
allocation, polling, charging or cleanup.

Distinct live funding drives cancellation, one reader-release action at a time,
fixed work clearing and ACK after the original ends. Each action checks funding
before its checkpoint and charges the saved generation afterwards. ACK clears
the marker and all frame/parent/control/token authority before dropping its own
original-control reference. ACKED history queries use local receipt bytes only,
including after frame/control destruction. The actual numeric parent retains
another original reference, so the receipt cannot legitimately be its sole
reference while that parent remains alive.

A zero-result cancellation is RESUME only from genuinely prepared/sealed state
without a saved error. It removes the receipt's leases/marker without publishing
or changing either entry. ABORT preserves the first error for the trusted caller,
which must pass it to the existing owned funded scalar finish; generic commit
does not provide a universal post-ACK error guard. A late cancellation after
the no-fail commit preserves committed success. Borrowed retired segmented
entries release only their owned segment array in one paid128 action, then clear
the borrowed entry in a following action; relation ownership and scalar READY
authority do not change during that array-only step.

Copied inspection/status/key/query outputs cannot overlap known active frame,
prepared receipt or numeric parent storage. Host request storage is checked
before reading its reply pointer; reply storage is checked before initialization.
The typed private-binding getter likewise refuses protected output before its
ordinary NULL initialization. Status rechecks current owner ranges after its
checkpoint: a genuine newly created receipt at the former external output
address cannot be overwritten. Unmarked in-frame cleanup-key bootstrap remains
valid. These checks compare known owner ranges, not arbitrary opaque allocation
sizes, and return no mutation capability.

Numeric tests cover real success, ABORT and RESUME for borrowed/owned READY
inputs, original/lower-stack/result aliases, partial descriptor cleanup,
exact/one-short preparation and funded cleanup, copied/native/reentrant
refusal, active funding/original generation turnover and ACKED reuse. Serial
tests reach a genuine generated composed parent, prepare its actual unary
replacement, retain STATUS767/768 behavior, reject parent mutation, then finish
the real parent and compare output. This fixture manually exercises Mode A at
that real boundary; it does not prove automatic Mode B or cache selection.

These fixed source regions remain separate from nominal debits: INIT is at most
252 within256, reader acquisition125 within128, old/output validation174/159
within192, no-fail replacement35 plus a conservative caller suffix16 within its
prepaid128, reader drain117 within128 and receipt ACK125 within192. Existing
ended-metadata cleanup is exactly192 within192, so these helpers do not add
shared validation to it. The observation-only range helper expands its first
frame comparison directly; constructor geometry remains separate. With the
complete four-binding/six-pair validation, numeric STATUS is at most376 and
composed STATUS exactly384 within their unchanged384 fees. The latter has no
spare capacity. Existing native/control/allocator boundaries remain qualified;
these inventories do not establish a whole session turn bound. Legacy
private-binding setup callers still require their own containing-route audit,
rather than inheriting a fictional whole-call384 proof from STATUS.

The binding getter also checks the live record's stored session when the supplied
session differs, before normalizing output. A wrong supplied session combined
with a wrong frame, control, ID or native thread cannot hide that actual
session's protected receipt. This is read-only range exclusion, not mutation
authority: full numeric/private-binding validation still runs once afterward.
NULL/nonself owners intentionally return EINVAL without changing output because
their output association is unproven; genuine self owners with safe external
output retain NULL normalization on ordinary argument errors. Caller-owned
record/session storage must remain live; this is not arbitrary pointer validation.

### Prepared unary replacement: Mode B ordinary JOIN primitive

A separate typed constructor now binds the actual quiescent ordinary
JOIN_DECIDE context. It preserves the final-address receipt layout ceiling
of392 bytes and work ceiling of256 bytes using target-compiled size guards;
these are portable upper bounds, not measurements of the Windows ABI.
The constructor leaves confirmation false. Paid header proof authenticates
the actual JOIN operation and REPLACE disposition, pending parent binding,
native ownership, original control identity and both borrowed output pointers.
Separate state and three scratch proofs establish cleanup completion, and repeat
before sealing. The context's output remains a genuine borrow through
publication; CONTROL clears it and FINISH clears the private borrow.

Publication moves all seven entry fields and uses the genuine JOIN delta result.
It records success, clears the JOIN decision history and advances the actual
context to CONTROL while retaining its original pin for normal release.
A zero-result RESUME leaves the genuine JOIN decision for the existing driver.
ABORT hands the first error into the authenticated context before releasing
exclusion. An unconfirmed receipt requires a distinct funded192 cancellation
action to authenticate that context without polling the original generation.
This preserves an actual first cancellation or work limit even after the
original control finishes, resets and begins a successor. An unsupported
ANTI_KEEP_ALL or MISSING_EXACT context receives no context stores from this
cleanup; only the attempted receipt's resources are drained.

Numeric tests drive real CHILD and PENDING boundaries, retain the actual
outstanding child tuple unchanged through receipt ACK, consume that tuple first
and then resume normal parent progress. Real preceding JOINs produce owned old
inputs; borrowed inputs and both segmented variants retain exact ownership
through publication or abort. Chained delta selection compares actual output
against the raw route, including six versus24 downstream rows. Genuine missing,
forced-empty, zero-row and bare inputs exercise empty replacement. Every new
header/state/scratch phase and typed constructor has an independent literal
exact/one-short budget oracle. First-stop tests verify successor debit remains
zero. Unsupported-context tests compare the untouched context and real fallback
against an unwrapped reference, including whether cancellation was actually
polled.

The fixed expanded source inventory is separate from nominal payments:
typed INIT252/256, header113/192, state83/192, each scratch65/128,
reader acquisition123/128, old validation at most172/192 and output157/192.
Normal cancellation is81/128 and unconfirmed cancellation142/192.
No-fail publication plus conservative caller allowance is64/128; drain117/128,
sanitize143/256, clear92/128 and ACK125/192 retain their existing bounds.
Existing ended metadata192/192, composed STATUS384/384, ZERO and FINISH254/256
and prefix249/256 retain their separately qualified inventories.

These fixtures manually select the primitive at an actual engine boundary.
Normal context-owned FINISH is the legal post-ACK disposal route for this
precursor; its generic owned disposal is not certified as bounded scalar cleanup.
Automatic receipt storage, selection and funding, a bounded context FINISH
adapter, actual cache policy integration and the containing-route audit of
legacy binding callers remain separate required work for the grouped issues.

### Context-owned bounded ordinary JOIN FINISH

A separate typed FINISH lease now reuses the genuine ACKED prepared receipt at
its original address. Actual CONTROL first releases the context's original pin
and reaches quiescent FINISH. The new constructor authenticates that frame and
numeric owner, retains the original allocation, and installs the existing
prepared marker using distinct live cleanup funding. It never polls or charges
the original attempt or its successor. Header proof authenticates ordinary JOIN,
the cleared decision history and actual private output before changing cleanup
state. Different SEMI/ANTI contexts receive no context writes and can drain only
the attempted lease.

The lease preserves original unary control/ID binding until the existing parent
RESET. It clears the context's private output borrow, uses the real staged scalar
initializer, acquires actual descriptor/storage writers, validates columns and
prior-pointer pairs, and earns genuine READY. A finite preparation sibling checks
one managed token per phase16..19, then the pool/accounting header in20. Governed
timestamp or recorded timestamp debt takes a separate pending phase21. That phase
charges a real additional192 before constructing the exact checked payload shape.
It restores phase2/column0/READY only after the original overflow and retained
capacity checks succeed. No caller phase or READY flag supplies this authority.

Scalar retirement commit transfers actual destruction authority, then clears the
selected entry, its shadow, the saved private borrow and historical unary snapshot
before any later descriptor free. Disposal advances one column, schema child,
pointer, managed credit or terminal claim action at a time. The bridge routes
ledger phase12 through an additive one-CAS acquisition sibling. Its local
publication is immediately applied/released or cancelled before return; neither
a held gate nor funding/stack publication borrower survives the call. The actual
governor CREDIT checkpoint now rechecks the saved cleanup ID after its hook and
before credit CAS, retaining the claim if the cleanup generation changed.

Segment disposal is an irreversible legal cleanup prefix. The selected entry and
shadow lose that array, and only a matching historical snapshot loses its segment
fields, before free in one callback-free action. A different live original ABORT
array remains intact. Pre-retirement decline resets actual initializer state,
releases genuine writer claims, clears scalar state and restores the remaining
legal FINISH owner/private relation borrow. It never recreates a freed array.
After relation retirement commit or borrowed-entry terminal clear, decline
refuses; cleanup retains its debt through completion. A declined receipt preserves
its causal cleanup error separately from the original first semantic result.
Supported scalar cleanup reaches physical DONE, clears actual scalar authority
and advances context RESET before marker/ref ACK. ACKED history uses local bytes.

Each real action has its own source ledger. Constructor204/256, header149/192,
segment81/128, selection94/128, scalar commit189/192, non-ledger disposal160/256,
ledger wrapper120/128, scalar clear at most210/256, claim release188/256,
decline restoration70/128, work clear98/128 and existing ACK125/192 fit their
admissions. Complete step scaffolding is at most64, including fee selection,
error/disposition handling and busy/return tails. Payload body including its
actual caller tail is191/192, with no room for uncounted additions. Its unchanged
generic formula expands all three grids, timestamp products, scratch and checked
sums; the local zero backup fields justify the separately documented bound.

Initializer calls make genuine metadata192/128 plus BOOT128, ZERO256, BIND192
or RESET128 admissions. Payload makes metadata128 + entry1 + payload192.
Ledger makes metadata128 + prepare109 + future166 + one round28; the166 is
initial26 + apply117 + release23. Scalar debt/phase suffix belongs to wrapper
metadata, not that same ledger credit. These are bounded multiple-admission calls,
not a claim that each whole call costs at most256. Existing raw scalar and looping
ledger APIs retain their separate qualifications; they are not recertified here.

Numeric fixtures use actual preceding JOIN-owned inputs, CHILD and genuine
FINISH-producing PENDING, success/ABORT/RESUME, original finish/reset/successor,
and65-column owned retirement. Per-call fresh cleanup controls are destroyed on
return. Literal exact/one-short schedules cover initialization, preparation,
payload, ledger, managed credit, decline and ACK, with actual pending tuples
unchanged until consume-first. Timestamp storage for the bridge boundary fixture
is constructed by the checked timestamp API on a real owned intermediate before
the next unary claims it; this does not claim ordinary JOIN naturally emits
timestamped output. Separate scalar-leaf constructors cover mixed signed/unsigned,
float/bool, narrow/wide and governed/timestamp combinations with peer ledger debt.

Real pool construction tests unsupported segmented decline through genuine
writer refusal and legal generic remaining FINISH. Ordinary JOIN naturally has
segmentless heap output: the distinct ABORT arrays test is explicitly a manual
internal fixture using checked compound-schema construction and a hooks-only
ownership transfer before marker installation. It seeds no phase, native owner,
READY, relation ownership or pool flag. Copied observations verify snapshot lifetime;
foreign/copy/output geometry, reentry and actual outer/initializer/PRECAS/CREDIT
funding turnover tests verify authority and first-result preservation.

This is still manually selected primitive integration. Automatic receipt storage,
selection and funding through the actual session driver, cache/policy acceptance,
shared/cap rollback and the reachable containing-route audit remain required.
Unsupported decline's legal generic disposer is not a bounded fallback acceptance
claim, and no broader issue or coverage row closes from this lease alone.

### One lexical composed-step validation

The public composed step still performs its complete native/frame/private-binding
owner proof on every entry. It sets the existing busy guard and immediately calls
a sole static validated body without a callback or yield. That body no longer
repeats the same proof. No token, field, bypass API or cross-return authority is
introduced; route branches, funding order, pending results and busy clear remain
unchanged. The checked private JOIN shadow fixture now uses four real static
bindings, including permitted aliases, while retaining distinguishable output.
Existing dynamic observed-transaction, copy/native, stale-generation and reentry
fixtures exercise the same retained public boundary.

Removing the duplicate body proof saves 54 logical source operations for no
bindings, 89 for dynamic bindings and 338 for four static bindings. The local
wrapper-through-body-entry envelopes are 64, 99 and 348 respectively, excluding
unchanged route classification, child setup and later result tails. These local
deltas do not certify a complete host allowance of 384. Ordinary JOIN stack plus getter with four static bindings
still costs 466 before remaining setup; the probe counterpart is at least
494. Reachable unbound pipeline bootstrap/final setup still needs its own actual
payment boundaries. Those containing-route repairs, automatic prepared publication
and FINISH selection, cache/policy/shared-cap and general transaction acceptance
remain required before the grouped issues can close.


### Separate ordinary READY setup actions

The selected composed READY path now dispatches ordinary JOIN and nonstream
SEMIJOIN/ANTIJOIN directly after the retained full public proof. It bypasses
the unrelated early empty/LFTJ/pipeline classifiers. A refusal-only frame query
remains inside eval_stack.c; no opaque frame member or mutable loan crosses a
module boundary. The module-owned handoff checks the exact native owner and
operation tuple before original forward128 and actual PENDING.

The parent384, DISPATCH128, STACK192, HEADER128, each ENTRY64 and PAIR32 (or
DYNAMIC64), FINISH64 and PRECHILD128 are separate admissions with actual proof
work and saved-generation polls between them. The legacy full getter and old
child APIs retain their full checks. Only private post-proof initializers in
join.c share the existing first-child192 and subsequent body. No context store
or resource acquisition precedes that admission.

NONE binding consumes192 for HEADER/FINISH; dynamic or one static consumes256;
four static consumes640 across ten entry/pair actions and HEADER/FINISH.
Including parent/dispatch/stack/prechild gives1024,1088 or1472 respectively.
The ordinary READY maximum alone requires facade384 + root384 + largest1472
=2240. The pipeline bootstrap allowance below raises actual production ADVANCE
capacity to2496 normal; recursive/general remains3072.
A no-binding READY call actually consumes1792 through the full chain;704 capacity
remains unadmitted. A short grant consumes only its completed action prefix.
READY/context/stack/output and original positive work remain unchanged until
handoff; retry repeats real proof and pays it again.

Tests exercise each actual static-four and NONE refusal boundary and exact
success on constructor-created owners, original consumed-work preservation,
real stream nonselection at PENDING/JOIN_BATCH, protected pointer output and
actual no-override normal session progress with1792 consumed/2496 limit.
The complete emitted source bounds, including expanded helpers, refusal paths,
loop transitions and actual first-child admission prefix, are:

| Paid region | Source upper bound | Funding fee |
|---|---:|---:|
| Parent public proof and caller/result tails | 380 | 384 |
| DISPATCH | 98 | 128 |
| STACK (nonstream probe worst case) | 161 | 192 |
| Binding HEADER (static worst case; NONE116, dynamic123) | 128 | 128 |
| Each ENTRY | 58 | 64 |
| Each PAIR, including terminal outer-loop transition | 31 | 32 |
| DYNAMIC authorization | 53 | 64 |
| Binding FINISH and actual caller | 61 | 64 |
| PRECHILD and typed handoff | 125 | 128 |

The STACK body remains161/192. Dormant native output protection requires
re-expanding the following HEADER geometry: its read-only entry is at most56
between the existing zero-tail checkpoints; the complete HEADER region is
bounded by128/128. The earlier whole-positive STACK-to-HEADER189 certificate
is retired. Checkpoint bounds are not additional summable financing. The private
post-HEADER head omits only immutable funding pointer/nonzero comparisons;
every saved-generation charge and every real entry/pair check remains.
HEADER proves the actual module/native owner before any original-control
probe. Both full typed entries reject original-as-funding role confusion
before a debit. Handoff distinguishes invalid proof from an actual original
forward failure. The old complete getter and child APIs retain their checks.

Actual session tests additionally cover whole-chain NONE1791/1792 and genuinely
authorized observed-dynamic1855/1856, including unchanged original work on the
short grant and consuming its real pending root result before retry. A normal
noncapture facade deliberately refuses supplied private bindings; this refusal
is not a fabricated static-four capture capability. A separate genuine
seven-op recursive capture constructor uses four checked alias bindings and
proves2239/2240 through the real facade/root, including2112 short-prefix fees,
unchanged original work, consume-first and exact three-row captured output.
It leaves the public registry unmodified; the production recursive driver
continues to construct two bindings. No callback is inserted inside the
metadata loop: generation tests use real API-entry turnover and existing
post-child callbacks, separately from the source proof of each poll.

Nonselected special classifiers, direct numeric/compatibility callers,
initialized pipeline containing calls, automatic prepared publication/FINISH,
cache/policy/shared-cap and general transaction acceptance remain OPEN.

### Controlled batch bootstrap and final initialization

The actual composed JOIN_BATCH fallback constructor has no private bindings.
Its unready path now pays parent384 before PENDING, then separately admits
STACK192, optional NONE HEADER128/FINISH64, full and optional delta LOOKUP128
each, SELECT128, RESERVE256, ALLOC128 and COMMIT192. These are real actions
with work and exact original/funding-generation checks between them. The
maximum1728 sizes capacity; it is not a bulk debit. Actual normal ADVANCE
capacity is max(2240, facade384 + root384 +1728)=2496. The session-governor
bootstrap consumes2048 through that chain; the right-governor worst path fits
2496. Unexecuted optional actions remain uncharged.

Bootstrap lookup locals live only during one activation. A late refusal can
repeat the read-only lookup and its actual original128 per registry slot/name
byte; this work is honestly charged to the same cumulative original attempt.
The governor ref, admitted credit, allocated host and committed credit are
recorded before fallible outer checks, so retry acquires each owner once. Both
lookup returns reauthenticate native/module/frame lineage after the existing
phase5/6 callback seams, before SELECT retains or stores a governor. An earlier
lookup semantic error keeps precedence over a later lineage error.

Descriptor reserve/commit use the existing attempt-bound governor APIs, with
their actual original ENTRY/CLAIM/ROUND/publication regions. Outputs are
authoritative only with execution rc0. Failure restores canonical token state;
commit has no negative poll or callback after irreversible COMMITTED. The
new wrapper fees cover their caller work, not these already admitted interiors.

Each ZERO128 funds metadata and admits the actual original slice of at most256
bytes. Even the last slice returns before HEADER. HEADER192 pays the fixed
final original16 once and initializes the three private tokens. A small private
cursor then admits CLASSIFY192 and the same original1 per tail descriptor,
MAP column or bytecode instruction. It preserves the raw numeric whitelist,
empty/default MAP columns, longest prefix and MAP termination. Only immutable
plan progress persists; no relation pointer or reader proof is retained across
classification yields. Full funded NONE binding verification precedes a fresh
PUBLISH192 stack/input/header/token proof. Final exact control polls precede
the no-fail stores, and sizeof(host)+1 is the last ready-marker store.

On prepublication error, separate FREE128, finite CREDIT192 steps and REF128
drain actual owned resources in physical-free-before-credit order. They use
maintenance funding only, preserving the first semantic error even if the
original ended or changed generation. Done/ownership flags precede fallible
funding tails. The record busy flag protects reentry before host allocation or
full zeroing; it is not a destruction lease or transferable proof.

| Paid setup region | Complete source upper bound | Fee |
|---|---:|---:|
| Parent proof/dispatch/result | 213 | 384 |
| STACK own body | 161 | 192 |
| Optional NONE HEADER / FINISH core | 116 / 56 | 128 / 64 |
| Full or delta LOOKUP wrapper, including posthook lineage | 101 | 128 |
| SELECT | 71 | 128 |
| RESERVE wrapper / COMMIT wrapper | 104 / 82 | 256 / 192 |
| ALLOC | 92 | 128 |
| ZERO metadata and slice | 88 | 128 |
| HEADER, including three actual token initializers | 110 | 192 |
| One classifier primitive | 110 | 192 |
| PUBLISH with fresh stack and token checks | 162 | 192 |
| FREE / finite CREDIT / REF | 24 / 112 / 32 | 128 / 192 / 128 |

Compound-result conversions, caller transitions and refusal/tail branches are
expanded. The nine NONE getter conversion/caller slots belong to this paid
parent, separately from the getter FINISH core. The lookup original128 regions
and attempt-governor interiors keep their own admissions; allocator, control
and mandatory library cleanup retain the existing cooperative boundaries.
The following read-only HEADER entry is at most56 between existing zero-tail
checkpoints. The old composite189 window is not reused after the full dormant
output guard; the complete HEADER bound is recaptured above.

Actual native C11 layout is host5392 versus5376: the20-byte private cursor uses
four old padding bytes. Zeroing therefore adds16 original bytes and one final
slice. Record3048, framed4872, frame11440, prepared392 and scalar retirement984
are unchanged. Fees and allocation/zero/marker use actual sizeof, with portable
cursor/quantum guards; these measurements are not Windows size claims. Existing
STATUS384, metadata192, PREFIX249 and ordinary READY source bounds remain intact.

The funding family checks each bootstrap short/exact boundary, canonical partial
resource state, physical-pointer reuse, every zero/final phase refusal, one
HEADER16, and marker-last. A predicate extended before session construction by
2000 genuine numeric instructions preserves all28 literal rows and raw parity;
classification takes more than2000 turns and adds exactly2000 original units.
Legacy child383/384 remains independently exercised after actual metadata;
the complete facade boundary is updated by the initialized containing repair
below. Existing governor seams
finish/reset/begin either original or funding at reserve/commit: successor work
stays zero, canonical state is drained or resumed, and both module entries
reject actual reentry. A constructor-created local no-session/no-left-governor
fixture reaches full/delta right-governor lookup; genuine nested-frame API
pushes at phase5/6 prove no later retain/allocation, semantic cancellation
precedence and complete cleanup. Successful full/delta cases also match raw
and literal output.

This qualifies bootstrap/unready setup only. Ready consumer/forward/cleanup
containing proofs, direct compatibility paths, nonselected special dispatch,
automatic prepared publication/FINISH, cache/policy/shared-cap and general
transaction acceptance remain required for the grouped issues.

### Initialized pipeline entry metadata

Every actual JOIN_BATCH call now pays parent384 after the retained full public
proof and exact funding-role/generation guard, before the readiness split.
This is the existing unready charge hoisted once, not a second charge. The
public composed wrapper and its selected ordinary READY path are unchanged.
The existing RELEASE-with-retained-resources reroute still records its first
semantic result and enters JOIN_BATCH before this admission; it cannot lose
parent funding by missing a wrapper-only phase selector. A refusal preserves
that existing reroute ordering rather than pretending to restore RELEASE.

The initialized destination admits one module256 action, then proves its
actual native/module owner, active frame/session, constructor fallback/NONE
domain, ready-size marker, bounded current JOIN and private host identity.
Only then does a private pure predicate return the legacy child1/384 fee into
an automatic scalar. No proof token, host loan or state persists across return.
No callback/yield occurs before the exact child admission. The old standalone
quote keeps its complete checks/default output behavior; the old checked
step and body guards remain. There is no additional synthetic validation stage.

On child refusal, parent/metadata fees remain paid, but record, stack/context,
host bytes, pending tuple and original consumed work are unchanged. Once the
child fee succeeds, the saved-ID conditional cancellation and actual PENDING
stores precede the original checked step. Its consumer receipt reconciliation,
first-error disposition and busy claim/release retain their order. The adapter
does not read the host after that call can free it. Metadata proofs do not poll
the original early or turn a funding error into a semantic stop.

| Initialized entry boundary | Local total | Full facade/root total |
|---|---:|---:|
| Parent plus actual module metadata | 640 | 1408 |
| Nonconsumer legacy child1 | 641 | 1409 |
| Consumer legacy child384 | 1024 | 1792 |

These are entry thresholds before any additional nested funding. Consumer
383/384 is isolated by giving640 completed metadata plus383/384, through a real
parent call and receipt. Complete facade1791/1792 tests replace its old
1151/1152 entry oracle; a short call consumes1408 and performs no original work.
The actual normal capacity2496 and recursive/general3072 remain unchanged.

The independently expanded fixed module bound is231/256: preadmission
self/busy/role checks14, actual module owner27, complete tuple28, phase
predicate15, child-admission/cancel/PENDING12, old wrapper31/body guard27,
fixed routing20, pending reconciliation13, semantic latch9, dispatch/call
tails27 and return/busy-clear8. Adjacent prefunding windows are conservative
latency bounds, not double-spendable source credit. The parent envelope remains
380/384. Public wrapper and old checked step/body remain unchanged. The later
automatic adapter extends dormant output protection; its changed binding
HEADER and checkpoint bounds are recaptured in the setup tables above. This
initialized-entry bookkeeping introduces no field, reader or allowance change.

Constructor-driven tests apply383/639/1023 short funding before every actual
consumer START/STEP/ACK, followed by exact1024 and consume-first. Byte snapshots
are only compared, never operated as ownership tokens. A protected stream stack
remains inaccessible through the unowned stack getter on refusal. Nonconsumer
640/641 proves actual entry; source work starts only after successful admission.
Real callbacks exercise public/old/new checked reentry; copied owners, actual
original-as-funding, stale funding, a lawful nested frame and a foreign native
thread preserve original work and ownership. Cancellation, sticky exhaustion
and original finish/reset/new generation retain first errors, zero successor
work and complete independent cleanup. Probe full-chain witnesses retain their
literal/raw content. The funding family is also selected for TSan because it
now includes the real foreign-thread test.

This completes fixed containing entry bookkeeping only. The actual nested
forward/consumer/cleanup helpers, their callback/source/governor/retirement
interiors, direct compatibility and nonselected routes still need their own
complete source qualification. Automatic publication/FINISH, cache/strict/local
and shared-cap/general transaction acceptance remain OPEN. Neither256 nor a
passing entry threshold certifies the unchanged helper interiors.

### Cleanup construction during consumer dispatch

Plain, controlled and receipted cleanup constructors reject an active stream dispatch with `EBUSY` after their existing argument validation. The refusal precedes allocation, work debit and receipt output normalization, preserving the active frame and receipt counters. Between dispatches, a parked stream still permits nested cleanup; the caller must finish the nested frame before resuming its parent.

The additional entry predicate changes the controlled source count from 103 to 104 within the existing 112 envelope and 128 work fee; the read-only bootstrap bound is 29. Successful receipted delegation executes both the wrapper and delegated entry predicates. The receipt reservation generation CAS retry remains outside any claimed fixed whole-wrapper bound. Complete consumer/forward/cleanup containing qualification remains open.

### Stream START first-boundary qualification

The callback-free NONE probe proof now omits only a redundant private-binding
getter after the real stack/native/frame proof; explicit session/control/ID
checks and the full non-NULL getter remain. There is no new fee or grant.
See SEMANTICS, "Stream START first-admission and ownerless completion", for the
unique source/payment allocation: FILTER/MAP344, fixed activation64, mandatory
stopped suffix192; probe NONE358 and stopped suffix168; empty host259/384,
empty child80 within a separate prepaid128 if present, and separate ACK363/384.
The proposed empty-host256 ceiling was insufficient for ABORT/DONE branches;
259 fits the unchanged384 fee. No actual fee excess is waived.

Genuine host tests calibrate the first nonempty FILTER and MAP START, exercise
first224 one-short/exact and lifecycle k0..10 exact/next-one-short boundaries,
then verify initialized ownership flags, ten empty child steps and one ACK,
source snapshots, cancellation, saved-generation turnover with zero successor
work, and complete resource cleanup. Probe session tests exercise SEMI/ANTI
first192 one-short/exact and cancellation, followed by real reset/retry and
literal/raw parity; ownerless refusal legitimately restores with zero ACK.
A real bound READY compatibility call exercises the changed legacy wrapper's
non-NULL path (the normal funded adapter bypasses that wrapper), with private
RHS/public-shadow canaries, actual native/copy/session/ID refusals and stale
bindings rejected by a fresh constructor. No phase, ownership or generation is
seeded to reach these states.

This qualifies the first boundary and fixed ownerless completion only.
Successful nonempty START/STEP and owner retirement, forwarding/cleanup/direct
routes, automatic publication/FINISH, configured policy/caps and general
transaction intersections remain OPEN.

### Automatic ordinary JOIN publication and typed FINISH

The framed CHILD adapter selects actual nonstream ordinary JOIN contexts at
JOIN_DECIDE/REPLACE. It invokes the genuine prepared publication protocol;
SEMI/ANTI, LFTJ, active stream consumers and unsupported dispositions continue
their existing paths. The automatic receipt lives at its final address in a
private allocation tail, outside the logical frame and numeric child. Manual
external receipt APIs retain their own checked geometry and ownership rules.

Preparation charges the original cumulative allowance. The final saved-ID poll
immediately precedes nofail publication without a callback. Distinct maintenance
grants own native routing, cleanup and typed FINISH. The completed launch grant
remains held through marker ACK and the subsequent successful checked parent
consume. PREPARED_MORE is maintenance progress; it cannot replace the old MORE
tuple or authorize resetting its grant. Rejected short admissions and failed
consume retain that tuple. FINISH uses the same genuinely ACKED receipt after
the real CONTROL stage releases its context reference.

Actual support admissions are SELECT_ENTRY128, BIND192, SELECT128 and NATIVE128;
maintenance uses ENTRY128, BIND192, PREVIOUS192, ACTION128 and a RETURN128
admitted before the primitive. Consume and native close readiness each admit
128 separately. No installation payment or larger default grant is introduced.
Actual allocation bytes determine the existing original constructor ZERO tariff;
logical-frame size does not stand in for the larger allocation. These payment
values alone are not a complete emitted-source certificate.

`evaluation_join_prepared_auto_domains` exercises real generated two-JOIN plans
through STEP/SNAPSHOT, plain/observed, empty inputs and dirty deletion/repeat.
Literal and raw oracles accompany actual publication/FINISH/ACK witnesses. The
entry fixture exercises a three-slot grant pool across 24 refused maintenance
turns, retained old reports, ACK followed by consume895/896, and actual
FINISH1599/1600 refusal/retry. Live and dormant protected-output geometry is
checked before request-field reads or reply writes. First SEALED poll failures
survive a short cancel grant and original finish/reset/successor, without billing
the successor. A genuine original256 constructor refusal with255 remaining
also preserves its saved first error across finish/reset/successor without an
explicit ABORT or counterfeit ACKED receipt. Existing exact recursive tariffs include source-derived3328 per
selected prepared JOIN, preserving their exact/one-short/reset-retry oracles.

The entry fixture also creates a real nested cleanup frame while the outer
native owner is dormant. Plain and funded binding getters refuse before changing
the parked owner's output; HEADER127/128 retains its exact debit and refusal
classification. Native-started key queries exclude the complete known outer
allocation, while a safe external key copy still succeeds. Whole outer and inner
allocations, pending tuples, original consumption and held launch reports remain
unchanged, without restoring receipt bytes. Normal inner close then permits the
outer consume and typed FINISH. These checks include original-begin refusal,
publication ACK, WAIT_FINISH and refused FINISH launch. The existing physical
publication observer runs only after the callback-free nofail commit returns.

Expanded native bounds include geometry63/64, live/dormant facade entry215/248
and271/304, complete facade347/384 and consume128/128 including actual caller
and reference release. Manual JOIN constructor factoring is256/256; FINISH is
207/256. EXTERNAL registration is139/144 within its existing160 fee. Raw NONE
getter97 replaces the earlier79 bound. Session-only receipt reads exclude the
current active allocation; they do not authenticate a parked frame from a key.
Known-owner binding getter refusals preserve parked native storage, and native
key queries retain the pre-native embedded-key initialization path. Exact bounds require recapture
after any evaluated source change.

Dynamic ended-generation outer cleanup still needs qualification before complete integration. Consumer helper
and retained retry accounting, configured strict/cache/local/shared policies and
general transaction intersections remain separate required bundle work. This
section does not close #2297/#2298 or declare the PR Ready.

### Bound native JOIN error cleanup

The actual framed numeric child retains one private binding pointer/generation
snapshot across its JOINs. The first paid BIND validates either the complete
STATIC header and current original generation or the full actual dynamic
transaction registration. Later BIND calls compare the immutable pair without
polling a new original generation. Actual child ACK clears the snapshot before
the parent consumes its completed grant. The private allocation grows by the
two actual fields; constructor ZERO and governor accounting use its complete
`sizeof`, independently of logical-frame geometry.

The native cleanup entry authenticates the actual active frame and embedded
context once, admits the existing numeric HOST384, and checks current native
error history, numeric owner, immutable binding and cleanup phase. Each action
reauthenticates; no proof token or permission boolean survives a return. Generic
numeric/private-binding validation and normal observed authorization retain
their original generation checks. Cleanup cannot dispatch sources, READY work,
new evaluation or successor control operations.

Actual precommit error JOIN_DECIDE invokes the existing nonzero-error branch,
which skips publication and original polling, then advances to CONTROL. A genuine
current ACKED receipt retains typed FINISH. A consumed failed original begin
with SKIPPED history uses the remaining generic disposer, whether receipt storage
is initially INERT or still ACKED from the previous completed JOIN. Historical
`receipt.committed` is not the current JOIN's cutoff.

Real ACK validates work and reader ownership before clearing fields and assigning
ACKED. Fresh INERT storage and failed pre-activation constructors preserve that
terminal invariant; all legal activations install nonterminal phases without a
callback. The native entry checks current identity/history and excludes marker,
launch and busy states, instead of repeating the complete inert-field scan.
Manual constructor, query, ACK and final frame-close checks retain their existing
proofs. Geometry applies to the associated known allocation; arbitrary foreign
output storage is not a receipt ownership capability.

The shared normal ACK still clears the complete LFTJ record. Native ACK omits
that redundant clear only for a genuinely never-used framed LFTJ state, proven
by constructor/transition history plus absent identity and inert metadata. Both
destinations retain actual source-release proof, meaningful numeric field clears,
phase reset and the original owned-reference drop. A native preference excludes
live or completed LFTJ identity before dispatch; other cases retain the existing
fully validated generic composed path.

The emitted source ledger assigns the canonical admission's pre-debit55 to the
actual enclosing framed HOST384, which also owns pending tuples, snapshot clear
and host return. Numeric HOST384 owns post-debit proof and its action. Complete
admission205 includes post-proof150; these are overlapping envelopes, not two
payments or205+150 work. BIND172 fits its separate192 admission. Framed own work
is347/384, including the additional mixed-LFTJ eligibility predicates.

| Numeric cleanup action | Own source bound | Actual admission |
| --- | ---: | ---: |
| Error JOIN_DECIDE | 237 | 384 |
| CONTROL | 230 | 384 |
| FINISH | 354 | 384 |
| RESET | 303 | 384 |
| Child DONE status | 217 | 384 |
| Filter ACK | 242 | 384 |
| UNARY_ADVANCE | 161 | 384 |
| RELEASE | 141 | 384 |
| Pending result consumption | 186 | 384 |
| Numeric ACK | 283 | 384 |

A uniform conservative envelope is361/384. FINISH includes199 fixed source
outside the existing exact `col_rel_destroy_checked` cooperative terminal
boundary, including native/frame/current-unary proof, entry selection, refusal,
retained state and status. That named destructor interior remains synchronous
mandatory cleanup with its existing lifetime/gate qualifications; no finite CAS
or whole-destructor quantum is asserted. The fixed wrapper work is freshly
counted on every retry. No earlier one-time or unused grant credit pays retries.

`evaluation_serial` uses the actual compiler-generated supported eight-op STATIC
two-JOIN capture plan, retaining the separate exact seven-op fixture. Its literal
baseline has four tuples. Tests exercise initial-INERT constructor failure,
second-JOIN constructor failure with prior ACKED history, and genuine current
ACKED cancellation/work exhaustion. Each ends/resets the original, begins a live
successor, drains the retained old owner and proves successor consumption/result
zero. Actual typed FINISH counts distinguish current ACKED from SKIPPED cleanup.
Binding snapshots persist across the changed JOIN operation and clear at genuine
numeric ACK. Parent ACK consumption at383/767 refuses without replacing the
tuple;768 succeeds after the numeric record has become inert. Never-used LFTJ
bytes remain zero before/after native ACK; numeric-plan tests separately prove
that a genuinely completed LFTJ still receives the full generic ACK clear.

`evaluation_join_prepared_auto_domains` uses actual registered observed a/b/c
two-JOIN transactions for first/second constructor refusal and precommit
cancellation/work exhaustion while the original ID remains current. It proves
no public output/events, unchanged source images, actual error reports and full
normal cleanup followed by literal reset/retry. A compiler-generated LFTJ then
ordinary-JOIN plan exercises its genuine completed LFTJ identity, failed ordinary
constructor, generic cleanup and successful retry; no phase or generation is
seeded to manufacture that route.

`eval_cleanup` derives the physical ZERO extent and reserved bytes from the
allocation-size observation. Its independent literal tariff schedule, every
prefix's exact/one-short assertion and failure rollback checks remain intact.
Using logical-frame size for this physical allocation caused the reproducible
CI assertion failure; increasing the allowance or relaxing the assertion is not
the correction.

These tests do not establish ended dynamic outer transaction/admission cleanup.
The existing outer generation guard and public continuation rules require that
separate mandatory destination and genuine public leak-free tests before grouped
readiness. Consumer accounting, policy/cap/cache/shared and general transaction
acceptance remain open.

### Repeated native integer CRC source accounting (#2297, #2298)

The caller-attempt evaluator now uses two immutable 256-entry byte tables
(2048 bytes of static data) for Ethernet and Castagnoli CRC. It keeps native
int64 byte order, zero-extended uint32 results, the existing positive fees and
all saved-attempt-ID checkpoints. The raw CRC implementations are unchanged.

Each CRC instruction admits FETCH64, STEP224 and its own 16+8+8*17+8 lane,
for456 units. Complete expanded CRC success source is bounded by390, or411
including the next first-refused admission. A disjoint allocation uses
FETCH64, CRC168 and STEP158, with21 further STEP units reserved for that
refusal; STEP therefore needs at most179 of its224. No compiler, ENTRY,
FINAL, prior instruction or containing host credit finances this bound.
Each repeated instruction has fresh payments; unused source slack is not
accumulated. The maximum expanded forward interval is76, below224/256.
These are conservative source bounds under the existing logical metric,
including engine hook fronts, rather than measured CPU costs or new fees.

`test_evaluation_numeric_operators` compiles actual immutable programs under
separate setup attempts and evaluates1,2,17 and65 repeated unary CRCs, both
variants and alternating variants. Independent raw CRC calls on copied native
bytes supply the result oracle, including zero, all-one, signed extrema and
alternating-bit operands. Exact evaluation budgets are1056+456*N for the
three ordinary instructions plus CRC chain; one-short refusal leaves outputs
untouched. Every byte of first/interior/last CRC has genuine positive-budget
boundary, cancellation, sticky work-limit and finish/reset/successor tests.
A successor receives no old-attempt charge; real reset/rebegin can reuse the
same immutable program without leaking cancellation.

Containing FILTER and MAP fixtures witness CRC byte checkpoints during actual
START, before the host adapter returns. Three-row successes match raw values
and metadata. Every byte in the first/interior/last CRC also reaches genuine
cancellation/work exhaustion before publication, with the original stack entry
preserved. Full ACK/report/frame destruction drains source/program credit and
independent host funding; fresh source/frame/control retries then succeed.
This closes the CRC lane only; remaining consumer, automatic lifecycle and
policy/cap obligations retain their separate coverage requirements.

### Pipeline KEY scanner admissions (#2297, #2298)

The controlled pipeline key lookup retains name-first resolution, the first
matching column, NULL-name-table fallback, and the host's existing `strtol`
saturation/narrowing behavior. Five existing positive admission sites now own
the actual source/native validation, column and byte work under the same saved
original attempt. There is no width/name-length cap or budget restart on retry.

| Actual action | Expanded source upper bound | Work admission |
|---|---:|---:|
| Entry, full HELD/native/source validation and local call interface |80|96|
| Column slot, including NULL-name continuation |21|32|
| Name byte, including mismatch/NUL and next refusal |24|32|
| Prefix byte, including final parser initialization |24|32|
| Decimal byte, including overflow/termination and next refusal |42|64|

The complete scalar-source check and validation bounds are29 and39. Every
current/next refusal front and engine argument/address/return edge is included
in its owning region; a refused positive admission adds no credit. Before the
first entry admission, the read-only local refusal and selected call edge are
bounded by23 and do not traverse source metadata. The maximum expanded forward
window is80, below256. The unchanged owned-probe per-key front admits192,
with its own86-slot interface bound; the following type-cell admission is128.
These local interfaces do not certify the remaining containing coordinator,
consumer, type/metadata or forward source interiors.

Actual admitted work is96+32*C+32*B+32*P+64*D: C counts visited column slots,
B compared name bytes including mismatch/NUL, P the reached prefix bytes and
D decimal bytes including termination. Each arbitrary scan/retry pays its own
units cumulatively. Name priority, duplicate names, partial/absent name tables,
whitespace/sign/no-digit/trailing/non-ASCII suffixes and LONG_MAX/LP64 wrapping
retain raw signed-index/fallback parity. No new runtime callback or API is added.

`evaluation_join_probes` now tests a real HELD257-column source. Looking up
`col256` visits257 columns and1105 name bytes, so its exact scanner work is43680;
43679 refuses the last32-unit action after43648 admitted units. The independently
specified source construction adds9024. Tests cover first/interior/last column
and name-byte boundaries, every prefix and decimal boundary of `col+1`, entry95
and96, parser saturation/narrowing, sticky stops and original finish/reset/live
successor histories. A refused entry leaves its output sentinel untouched;
accepted entry normalizes output before later refusal. Source leases are ended
through their real cleanup protocol, including after original generation end.

A parsed public program declares257-column `a` and `p`, joins `a` with `b(x0,t)`
and applies a257-key ANTIJOIN. Its head retains all258 fields. The actual generated
ANTI key includes `col256`; existing PROBE_KEYS callbacks witness all257 keys and
the real terminal stage turn. Two input rows differ at column256, and `p` contains
only the first: controlled and raw results are exactly the second row plus t99.
Cancellation/work exhaustion at the129th stage turn publishes no head; real
reset/retry traverses the full stage and returns the same literal result. Batch
settings are configured before session creation and restored after teardown;
full governor ownership returns to zero.

This closes the KEY leaf and its stated local call interfaces. Remaining writer,
constructor, forward/cleanup, ordinary publication/FINISH, generic receipt and
strict/cache/local/shared/general policy source obligations remain required
before the grouped PR is ready.

### Constructor INIT failure completion

The selected operator constructor retains its INIT192 fee. Attempt-scoped
charge/poll primitives preserve the saved ID before and after the existing
checkpoint hook; fixed three-token initialization and retirement keep their
ascending order without a runtime index loop. The actual first-NAME-refusal
source is bounded by127 current slots,56 common fixed disposal slots and8
immediate caller slots:191 within the unique192 payment. No refused NAME debit,
object allocation, selected token or ancestor pays this interval. Generic
relation guard/tail and governor implementations remain unchanged.

`evaluation_relation_creation` covers actual typed widths0 and1 with NULL and
pinned governors, a live17-byte peer, hooks present/absent, INIT exact/one-short
and the following NAME96 boundary. Its source-derived INIT tail totals are576
and704. Cancellation, accounting overflow and real finish/reset/new-attempt
turnover at those tails preserve nonpublication, one physical retirement and
peer credit; the successor consumes zero. Real reset/retry constructs the same
schema as raw and accepts the same literal row. Existing allocation/commit
failure sweeps remain active. This qualifies INIT/common fixed completion only;
ALLOC112, RESERVE160, their item/token shares, installation, complete constructor
callers and the remaining consumer/policy paths still require their own audits.

### Constructor reservation completion

The shared reservation helper expands its two governed attempt tails locally,
preserving the same saved-ID checks, hook positions, work160/zero values and
error precedence. Its current source bound falls from94 to84. The same
successful160 admission owns76 additional slots for that token's later lexical
refund, including the supported memory/resize instrumentation:84+76=160.
No token credit transfers to the installed relation destructor.

The descriptor SMALL96 header owns31 current slots and one outgoing refused
admission: either initial RESERVE160 (24 slots), or, after its success, first
descriptor ALLOC112 (11 slots). Its correlated bound is31+max(24,11)=55;
the bridge expires on that allocation's successful admission or builder exit.
The later SMALL96 header owns53 current slots,6 fixed loop-control slots and
one outgoing refusal bounded by26:85 total. Neither bridge pays another
token's cleanup or arbitrary subsequent work. The unchanged ALLOC112 action
has76 current plus8 same-item retirement slots; INIT remains191/192.

Real constructor tests interrupt all three reservations immediately before
and after governor admission, using cancellation, accounting overflow and
finish/reset/new-attempt turnover. They verify the actual RESERVED token,
one context retirement, a live17-byte peer, zero successor work and successful
reset/retry. First RESERVE/descriptor allocation one-short boundaries preserve
the allocation-failure canary; exact allocation admission reaches its real
failure. Full nested governor windows, COMMIT/INSTALL, installed cleanup,
multi-token and outer caller windows remain separate required qualifications.

### Nonconsumer sink ACK source admissions

The materializing host retains its original1, phase1, module256 and containing
parent payments. Its nonconsumer ACK now admits three separate source actions:
proof242 before the existing BEFORE_ACK hook, precommit227 after full canonical
sink/native/source/extent/gate validation, and commit115 after the producer's
read-only precommit. The last admission precedes callback-free extent and ACK
publication. No persistent stage, permission, layout or new hook is introduced;
the generic sink ACK and producer primitives remain unchanged.

Actual local source bounds are229/242,221/227 and87/115. Complete changed
windows through the real containing grant finish are37 for initial refusal,
252 for second refusal,243 for third refusal and107 for success; the existing
original-error remap suffix is bounded by48. Module success/refusal/error bounds
are245/253/253 within its separate256. Refused admissions supply no credit;
their fronts belong once to the preceding admitted action, or to module256 for
the first action. The largest newly interruptible admission is242, within256.

The ordinary production grant remains2496. Facade384, root384, composed384,
module256 and phase1 total1409; the three ACK actions bring genuine successful
consumption to1993. The checked quote585 observes phase1 plus584 source work;
it is not another debit. The existing capture capacity3072 does not itself
authorize a previously unsupported capture route.

Public session tests reach the actual compiler-selected nonempty ACK and apply
a one-shot grant at1650/1651,1877/1878 and1992/1993. They check exact admitted
consumption, original1, precommit nonpublication, BEFORE/AFTER counts and genuine
outer/inner PENDING states. A further BEFORE_ACK case cancels the actual live
grant through the control API; only proof242 was admitted. Normal fresh grants
consume both real pending results, retry without duplicate ACK generations and
produce the same literal content as raw execution. The finished grant report
stays intact through actual outer consumption; that checked outer consume
authenticates its report and carries the protocol to the later inner consume.
The numeric protocol field is assigned by inner consume, not by the preceding
ACK return. No report reset, private phase or token state is fabricated.

This qualifies the changed ACK action and its stated containing windows only.
Unchanged ordinary STATUS/consume/factory/coordinator envelopes, constructor,
writer/compiler, generic receipt and strict/cache/local/shared/general policy
obligations remain required before the grouped PR is Ready.


### Framed abort-capability scan

The controlled classifier now charges the original saved attempt before each
operator read. Its complete source bounds are ENTRY33, OP64 and FINAL17;
OP64 includes all grammar branches, indexed materialized inspection and one
following refused OP/FINAL frontier. ENTRY owns only one refused first OP.
The existing raw classifier remains unchanged, and each genuine recapture
repeats the scan without carrying a previous result as permission.

The sole framed call follows the same existing funding384 and precedes the
bootstrap, retention and context activation. The exact incremental framing
seam is35/384: its own15 plus either the first ENTRY refusal/public return19
or post-scan bootstrap first-refusal/public return20. This does not certify
unused framing capacity for OPEN fixed preflight/native work. Existing
bootstrap actions retain their own current/future shares; the final context
activation remains separately charged256. No field, allocation or layout
changes are introduced.

Parsed unions with3,17 and65 unique inputs exercise growing preserved compiler
bodies at stack depth2, literal nonempty results against raw execution, and
two real captures on one original attempt. Exact begin budgets include the
independently derived scan plus392*192+256. Numeric tests cover each visited
OP one-short, FINAL16/17, repeated cumulative scans, all grammar results and
real finished/stale/cancelled generations with zero successor consumption.
Framed tests cover child383/384, initial and partial original refusal and the
explicit funding-before-variable-error order. Refusal checks identity, pin and
phase freshness rather than byte equality of a context whose receipt key may
already be written. Existing recursive exact-work formulas add the compiled
body's scan once per independently specified capture round.

Whole fixed constructor/public return, static recursive recognizer, O0/O2,
retirement-parent, owner predicate, cutoff and general policy source obligations
remain separate required work.

### Constructor installation completion

The selected operator constructor uses one private checked reservation move at
its three existing INSTALL sites. It retains the NULL/distinct/native identity
and EMPTY/RELEASED destination, RESERVED/COMMITTED source checks, every payload
and atomic transfer, and the original memory orders. Its actual constructor
history has EMPTY destinations and COMMITTED sources:41 body slots plus9 call
slots per selected token. The INSTALL tail expands locally with the same
saved-ID query, work220 hook and original-ID zero charge. No callback or poll
splits the moves and installed field/reference/output transfer. Shared governor
move and legacy constructor behavior remain unchanged.

Actual INSTALL source is198 current plus20 for its one outgoing first EXTERNAL
refusal:218/220. A successful EXTERNAL admission ends that bridge; installed
checked destruction uses its existing named terminal qualification and receives
no expired private-context token credit. The correlated post-INSTALL window
through the immediate MAP/JOIN saved-ID zero cut is229/256. Direct typed
MAP/JOIN caller source fits98/192. Full scalar-source native/HELD checks add37
explicit prefix slots, yielding the corrected135/192 bound against that same
already named parent payment; the original98 projection did not include that
prefix. Cache BUILD uses its actual VALIDATE192 payment. Schema's original1
semantic action does not establish a general fixed-source payment.

Constructor tests observe genuine reservation and commit events, all three
installed token self-identities/owners, canonical storage owner and governor
reference, typed schema and literal-row parity. NULL-governor and width0 cases
exercise skipped moves with a real17-byte peer on governed routes. INSTALL220
and first EXTERNAL128 exact/one-short boundaries are witnessed from the actual
operator construction schedule; these measured boundary positions are not an
independent fee or source oracle. Both existing hooks exercise cancellation,
accounting overflow and legal finish/reset/new-attempt turnover, with zero
successor work and real reset/retry. Private failure retires one context;
post-install failure uses checked installed disposal and never retires that
context again. Peer credit and sole-reference checks verify final refund.
Physical-free-before-credit ordering is established by the emitted source
proof; these tests introduce no release hook or between-move failure point.

These bounds qualify the selected INSTALL and immediate caller seams. Full
nested governor/source stages, first-COMMIT predecessor, failed-context outer
and multi-token windows, and remaining consumer/coordinator/compiler/receipt
and policy obligations remain separate required work. Integrated adoption
requires fresh source/header/native carry; this is no whole-constructor or
PR Ready qualification.

### Compiler-owned scalar branch view (MR1)

The view builder lives in `exec_plan_scalar_view.c`, with its declarations and
allocator/lifetime contract beside the representation in `exec_plan.h`.
Production, archive, direct backend and benchmark bundles each include that
leaf once; backend-only session users need no parser/IR generator linkage.
The original compiler generator retains its own IDB schema ownership unchanged.

A controlled session whose resolved worker count is one automatically prepares
an immutable scalar branch view for the qualified default-generated recursive,
nonaggregate K_FUSION group. One governed arena owns the maps, typed group and
branch spans, and copied operator descriptors. Strings, keys, projections and
expression buffers remain borrowed from the original immutable plan/program,
which must outlive the session. The original plan and its K_FUSION/EXCHANGE
operators stay unchanged. Each branch records its independent stack boundary;
the typed group records sorted union after all alternatives, without executing
that merge or granting scalar/native authority.

This is synchronous creation work, with checked counts, alignment and allocation
sizes, rather than a free STEP scan or an evaluation-control payment. The arena
is freed before its exact governor credit, after all session consumers are
quiescent and before the governor reference is released. Worker clones exclude
the view pointer and noncopyable reservation even transiently and initialize
an independent EMPTY token at the worker's final address.

`plan_gen` verifies the actual default parsed triple-selfJOIN, three branches
and24 ordered scalar descriptors, every descriptor field/borrowed payload,
original descriptor and payload-byte preservation, mixed untouched relations,
no-view selection, allocation refusal, misalignment cleanup and count overflow
before branch reads. `evaluation_join_prepared_auto_domains` verifies automatic
constructor selection, a raw literal result of {(1,2),(1,4),(2,3),(3,4)}, unchanged
controlled ENOTSUP, checked destruction/control detach and original-plan reuse.
It also exercises a real one-short arena governor admission with a live17-byte
peer, a later actual spill-open failure, selected-coordinator worker cloning,
TDD slot growth/reinitialization and raw parallel K_FUSION worker clones.
The existing `plan_gen_oom` linker wrapper refuses the actual arena malloc only
after its real RESERVED token exists; cleanup preserves peer credit and a real
retry creates and destroys a view. The focused worker/K_FUSION/TDD suites pass.

MR1 has no evaluator, cache, producer or support-predicate consumer. It does not
claim materialized controlled execution, a controlled cache hit, scalar group
execution equivalence, or strict/local/shared/general policy acceptance. P2/MR3
cache ownership and source qualification must precede MR2 runtime enablement;
MR4 and the remaining consumer, ordinary coordinator and receipt obligations
remain required before the grouped PR is ready.


### Generic receipt target cleanup

The standalone no-active generic receipt sibling owns its complete checked
entry. Actual emitted source is189 for the rejected/failed-funding window
within the accepted192 ceiling and383 for the successful terminal path against
its unchanged384 payment, including both supported release-hook configurations.
The former185/359 projections are not certificates. The seven-site private
checked terminal move retains all actual native/state/atomic checks and omits
only two proven redundant plain identity stores.

Genuine issued keys at slots0,15 and31 cover32 active frames before any transfer,
a held result plus maximal32 stack entries, lower-entry disposal past retained
readers, read-only pending STATUS, exact383/384 and mixed raw/funded target
unlinking. Additional raw/unreceipted middle removal and normal worker cloning
exercise owner-cell isolation. Actual producer publishing defers destruction
while finish consumes the frame; real continuation reader release then permits
normal retry. Cleanup after original finish/reset leaves successor consumption
zero, fully drains frame credit against a live17-byte peer and permits a fresh
ordinary execution with literal/raw parity. Error and stale-funding tests use
real control lifecycles and copied external request keys, never seeded engine
receipt, link, owner, native or phase state.

Physical frame ZERO work derives from
`ceil(wl_columnar_eval_stack_test_allocation_size()/16)` at224 per actual chunk,
not measured budget padding or a fixed POSIX size. The source gate's inspected
allocation11928/token88 layout implies746 chunks locally; other supported ABIs
and future integrated MR1 copy exclusions require fresh checks. The atomic
inventory in this isolated base is458; integration must reconcile the actual
current inventory, including separately committed sites, rather than overwrite
its count with this historical value.

This is the exact accepted cooperative target-owner boundary, not a finite
terminal-CAS/time guarantee or broad helper waiver. Complete ordinary/O0/O2,
retirement-parent, policy and whole Ready obligations remain separate.

### Raw cache pin native identity (P2-H)

An explicitly issued materialization-cache pin owns debt only at its final
address. Successful insert/lookup publishes `self = pin`; copying the structure
creates no release authority. Raw release rejects a copy before touching its
cache, and genuine release clears all four fields. Invalid lookup arguments
still precede output normalization; a valid miss and non-NULL insertion output
normalize the entire new pin layout before publication. The stale-pin test
helper is only a nonnative output sentinel and is never released as authority.
Cache storage must outlive genuine pins, with existing serialized,
non-reentrant raw access requirements.

`wl_columnar_cache_pin_move` checks NULL and full-range disjointness before
reading either object's fields, requires an initialized inert destination and a
native active source, then transfers authority to the destination address and
clears the source. Failures leave both objects unchanged. It performs no cache
lookup, callback, allocation, control admission or pin-count change. The
independent emitted G1/G2 core audit bounds its complete successful callee at31
source cells; the specifically modeled pointer/field/index callers yield37,
39 or41 including their caller fronts. These are finite source bounds for
those expressions, not a new fee or a qualification of arbitrary future
callers. Raw release may run existing destruction and is not newly bounded.

The lifetime regression uses the same existing insert/lookup/copy/clear/release
APIs on the old layout and this layout. The old layout fails the deferred-debt
assertion before any potentially freed result read; the new layout preserves
that debt until genuine release. Tests cover both real issuers, normalization,
NULL/same/partial-overlap rejection, exact adjacency, copied/inert sources,
live-destination EBUSY and unchanged failed bytes. Actual moved pins survive
clear before/after transfer and first/middle/last compaction. A real governed
result and attached ledger/reclaimer preserve unrelated governor and17-byte
ledger peers, then refund only their own result on last release. Existing
64-pin admission, generation, reclaimer and ordinary/differential JOIN tests
exercise the appended handle layout in current consumers.

This is the raw native-handle prerequisite only. Registered receiver/root
cleanup (P2-R), deferred last-pin retirement, bounded insertion (MR3-I), the
public controlled driver (MR2), actual containing payments/cutoffs, strict
warm-cache behavior and the policy/cap matrix remain mandatory separate work.
No controlled materialized route is enabled or qualified by these tests, and
integration requires a fresh source/layout/caller carry before adoption.

The recursive terminal metadata classifier now shares one pure predicate between
its raw plan observation and the real guarded composed path. The raw path no
longer constructs dummy attempt/binding objects. Parsed seven- and eight-op
fixtures check raw classification; real private bindings and bound execution
check literal rows and terminal EXCHANGE cursor/completion while preserving the
same stack relation and ownership. Backed metadata negatives retain native
preflight rejection, and existing capture, cancellation, successor and exact
original-budget oracles remain unchanged.

This factoring adds no tariff or authority. Its source delta is not a payment
certificate: real composed successful guards add three scalar source cells for
the forwarding edge. Full O2 fixed/scanner, ordinary owner/public32, all11,
cutoff, consumer and policy accounting remain open.

### Private batch writer and capture source profile (#2297, #2298)

Controlled batch BEGIN and renewal use a private original-single constructor
with15 actual metadata/native actions. The independent action prices are
`123,33,57,84,99,60,100,33,32,28,46,18,38,39,63`: one uncontended
constructor consumes853, BEGIN adds170 (1023 total), and renewal adds its
original128, authority128 and terminal65 (1174 total). Each real extra nonce
round consumes57. Legacy hook labels, including ENTRY94, remain labels; the
source ENTRY debit is123. Generic/manual single APIs retain their own prices.

The constructor forms one typed address of its actual embedded owner only
after paid FRAME and before every failure edge. It rereads the real native
fields after callbacks. Both remaining writers are validated before either
release; identity, gate, secondary owner, thread and state checks remain, and
the24 meaningful inert stores execute after actual releases. Actual fixed
cleanup for zero/descriptor/both-held prefixes is30/71/113 within separately
owned32/78/124. FRAME47+next-refusal5+prefix32=84; descriptor and owner each
need97 within99/100. The genuine owner-success then BORROW refusal needs
46+5+113=164 within its178 same-original source. No native/error permission,
copied writer, whole-cleanup exception or source from another object is used.

Governed wide capture uses a separate source reservation profile
`100,124,164,78` and commit profile `98,81,63`. One real reserve round consumes
466 and commit242; each additional real reserve round adds164. Preparation36
and scratch allocation37 precede their actual protected work. Old generic
reservation/growth/constructor prices remain independent. Each actual
governed wide capture adds351 to its former schedule; narrow/nullary and
ungoverned paths do not invent a reservation or saved-reference refund.
Physical scratch free precedes credit release and saved-reference release.

The writer source map includes fixed native/hook/helper/caller and first-refusal
work. Its historical capture255 contour used native refund bounds that require
fresh full first-iteration/ref-setup reconciliation. The selected private
scalar-copy correction above does not establish that shared capture contour.
Only the previously qualified repeated native weak-CAS and terminal credit/claim
iterations retain their exact boundaries; the first iteration counts. A
successful constructor handoff forfeits its failure share; it cannot finance
subsequent batch END or a different activation.

`evaluation_work` checks independent exact/minus-one stage schedules, real
zero/descriptor/both writer cleanup and owner-success/BORROW refusal with
physical peer credit and release-hook failures. `memory_governor` checks the
new source APIs' independent boundaries, genuine peer-induced CAS retries,
cumulative reserve/commit on the same original allowance, and original
cancellation/accounting-error/END-reset at every real seam. Existing generic
primitive oracles remain separate. FILTER/MAP host literals retain the
actual source-profile schedules across wide capture and renewal.
The legacy two-JOIN recursive fixture begins4/6/6 writers across its three
rounds; the single-JOIN fixture begins4/5/5/5 across four rounds. Their exact
independent formulas add16/19 times `(853-498)`, preserving the existing ABI
and cutoff terms. Width2 fits the original capacities, so neither case adds
wide-capture or renewal work.
Five-round successor recomputation similarly begins4/5/5/5/5 writers and adds
24 times the same source-derived difference.

The selected activation-local writer constructor is qualified. Shared capture
first-native/ref-setup accounting and returned native-error prefixes still need
their complete source and checked drain. Complete generic drain/ROW/growth/compiler,
ordinary coordinator/owner/public recovery,
materialized-cache driver, strict/local/shared/general cap intersections and
whole-bundle acceptance remain mandatory; no PR Ready claim follows here.


#### Private SOURCE financial lifetime prerequisite

The admission-owned sealed BANK now has genuine fixed pre-session birth,
partial-lane failure rollback, owning-slot destruction and private phase/report
lifetime. Destruction uses actual256/24/160 admissions, source intervals
235/20/20/141 and correlated slack<=251 with corrected processed lower189.
Generic original/BODY IDs remain monotone and no native controller/ID escapes.
The class alone supplies no engine permission or blanket callback qualification.
The focused test covers a real parsed public session, real second-lane identity
exhaustion, original attachment/report preservation, live successor0, noncopy
and thread/reentry/owning-slot guards. Synthetic kernel policies test financial
mechanics and are not production source tariffs.

Complete public query/HOST/return/Snapshot/ordinary owning recovery, all native
consumer source contours, compiler materialized execution and strict/local/shared
cap intersections remain open. This prerequisite does not close2297/2298 or
establish their complete acceptance scope.

#### Generation-bound financial completion prerequisite

Internal atomic BEGIN now returns its actual new ID under the same mutex.
Expected-ID FINISH distinguishes own completion, matching already-ended report
and different generation without touching or exposing the latter. Genuine
three-outcome thread histories, live-successor consumed/report preservation,
late cancellation, complete seven-field generic parity, execution/cleanup/stop
precedence and actual full-width exhaustion have a separate regression target.
The two added release exchanges extend the strict inventory to522.

The new bodies are fully ordinary source (local ceilings65/114), not new
exact-nine interiors or production caller tariffs. They do not migrate public
STEP/Snapshot or consumers. Together with the private BANK lifetime, these are
prerequisites for the still-open ownership/source/materialized/policy/cap
integration;2297/2298 acceptance remains incomplete.
