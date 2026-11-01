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

The held recipe reaches the existing metered operator constructor with the
source's actual capacity, names/types and declared/graph fields. The private
output has constructor-issued exclusive ownership until funded TAKE. Each
physical 64-bit cell is copied directly, including FLOAT bit patterns and
STRING codes; typed append is not used. Base rows and valid sorted/run
metadata match raw governed deep copy, including unused run slots; invalid
run metadata is normalized to zero. Fresh descriptor identity is independent.
Only copied status/shape is queryable. A separate real128 grant transfers the
completed output after source END. ACK rejects an untaken owned output.

Late forward failure retains any created output at the original attempt
address. Independently funded END releases actual source tokens, then abort
lazily initializes the existing scalar-retirement owner with separate real256
ZERO and BEGIN grants. ZERO clears the scalar receipt; BEGIN runs the actual
managed initializer including its nested clear and all four token initializers.
Their source inventories include both real bulk clears and caller work;
existing primitive tariffs remain unchanged. Prepare, commit, physical and token/ledger
retirement use actual cleanup funding, preserving the first semantic result.
The wrapper pays192 in addition to inherited prepare/step charges. TAKE and
ACK each require their own128, independent of the completed/stopped original.
No raw deep-copy or raw destroy is hidden under a nominal fixed copy fee.

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
The existing raw LFTJ metadata preflight's bounded-count loop is also retained
for the final source audit; this initializer change does not certify that
loop's full caller envelope. Prepared unary publication, registered cache
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
