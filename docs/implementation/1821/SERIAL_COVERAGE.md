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
| E04 / S10 | Completed observed STEP plus declared-input insert/remove → private full candidates → signed comparison and atomic registry image | R, reusable ordinary route; no three-slot/four-binding cap; charged preparation, reversible old-head/staging claims, prepaid cutoff, retained cleanup; S/F/O | `test_controlled_observed_multi`, `test_controlled_observed_filter_successors`; repeated signed cycles, later-claim cancellation, no replay, reference output and stable/zero governor balances. #1820 recovery contract retained |
| E05 / S07 | Exact binary INT64 path manifest with one/two JOINs and terminal zero-worker EXCHANGE → `col_controlled_step_recursive_drive` | R, implemented restricted recursion; charged progress/private merge; S/F; general SCC bound U | `test_controlled_recursive_two_join_step`, `test_controlled_recursive_two_join_successor_stop`, `test_controlled_recursive_successor_work_boundaries`; G3 |
| E06 / S07 | Ordinary positive recursive multihead/SCC bodies | R, declared positive-arity INT64 route implemented with private synchronous rounds; charged comparison and retirement; S/F/O | `test_controlled_recursive_step_general_join_chain`, `test_controlled_recursive_transaction`, `test_controlled_recursive_transaction_successors`; observed/unobserved and insert/remove recomputation. `test_controlled_recursive_transaction_stops` and `test_controlled_recursive_transaction_shapes` qualify phase stops, multiple strata and empirical budget boundaries; differential and optimized routes remain G3b/G4 |
| E07 / S08 | `current_iteration`, `retraction_seeded`, `retraction_right_pass`, or `diff_operators_active` at initial host admission | R, static refusal before attempt/operator; no new owner | `test_controlled_session_admission_matrix`; this is injected-state refusal, not differential execution; G3 |
| E08 / S09 | Nonzero `join_batch_bytes`, non-strict, exact VARIABLE/keyed JOIN/FILTER/MAP/CONSOLIDATE → authorized materialized fallback | R, restricted; Q64 charged preflight then F, not raw pipeline; S/F | `test_controlled_batch_join_filter_map_step`, `test_controlled_batch_delta_selectors`, `test_controlled_batch_child_domain_rejection`; G4 |
| E09 / S09 | Strict batching or another batch shape | R, refused; strict mode static, other shape charged; no operator owner | `test_controlled_session_admission_matrix`; positive counterpart E08; required host producer/pipeline route absent, G4 |
| E10 / S12 | Fresh/dirty controlled SNAPSHOT versus completed STEP | R; fresh ENOTSUP / pending EBUSY; completed → `col_session_snapshot_controlled` → charged private staging → finite prepaid dispatch; S | New admission matrix covers fresh and restored/completed state; observed successor tests cover dirty EBUSY; `test_controlled_session_snapshot_publication` covers cutoff/reference. `test_controlled_snapshot_preparation` covers charged preparation, no-output stops and retry; finite prepaid dispatch preserves the cutoff |
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
| F05a | Ordinary keyed persistent arrangement/probe bundle versus ephemeral hash; cached right filter versus operation-owned filtered relation | Controlled ordinary JOIN uses its owned charged child/scratch, not proof of all raw cache paths. Persistent pins, ephemeral buffers and filtered source identity need separate parity/stop evidence; F or missing raw-route owner, G5/G6 |
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
