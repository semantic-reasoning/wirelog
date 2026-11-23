# JOIN domain qualification for #2371

This is an evidence inventory for ordinary and materialized JOIN. It records
what the implementation bundle actually exercises; it does not close #2371,
#2300 or any lifecycle/accounting sibling. The earlier unconfigured INLINE increment started from main
`3bdf3c458bc85b91345f10d900c3fc0d94de7eea`, including merged #2377 and the
latest #2301 source changes. That increment changed ordinary JOIN source admission through
an existing bounded adapter; no price, quantum, public ABI or layout changes.
The evaluation quantum remains256.

The previous public-history/materialized-memory bundle used baseline
`8266d893`. Its original test commits `d9f7775a`/`03a863a3` were merged as
`bc32ba90`/`84ae1dcf`, followed by ledger `3bdf3c45`. That earlier bundle did
not change production C. The new ordinary INLINE admission is `8c5a28dc`, with
consuming/retirement tests `f7e234dc`. Use the reviewed PR HEAD/configuration
for subsequent certificates; earlier green runs and experimental Native/Source
snapshots are not current route certificates.

## Configured no-tail INLINE policy increment

This historical #2380 increment starts from merged #2378, main `803381f1`. That merge contains
ordinary INLINE admission `6a895a0e`, physical-lane/recovery tests `d0d794ff`
and its ledger `803381f1`. The configured dispatch repair is `14f07675`;
expanded qualification is `cd4e4d99`. All36 earlier public INLINE histories
have an RHS predicate and bypass the otherwise eligible configured host.

Before this repair, six unfiltered keyed cases (left/right/both INLINE,
strict off/on, bytes256) returned controlled ENOTSUP while raw succeeded
with literal `(7,8)`. Nonstreaming host HOLD now authenticates INLINE through
the existing bounded reader. It retains RHS selection, key/type work and
strict successor/cache/row-fit policy before draining through ABORT and
handing off to ordinary controlled JOIN. It does not enter the compound-
excluding batch producer. At that checkpoint, streaming candidates retained zero admission flags;
the next increment below supersedes that refusal.

`evaluation_join_inline_policy` runs89 finite scenarios:

- 40 original matrix cases: left/right/both INLINE plus scalar control,
  strict off/on, fitting/undersized bytes, empty left, materializing successor
  and selective streaming tail. Scalar controls witness PRODUCE; successful
  INLINE controls witness actual ordinary gather with HELD descriptors.
- 18 numeric cap cases test limits1/2/3 against literal two-row output in
  permissive and strict modes. Equality refuses with EOVERFLOW.
- 12 authentic canonical arrangement-pin cases test usable versus stale/
  rebuild-deferred pins and bytes256/1. Stale-pin EBUSY precedes row-fit refusal.
- 18 internal descriptor cases test malformed-map EINVAL, SIDE ENOTSUP and
  unknown-kind EINVAL before row-fit/successor policy. Only the controlled
  descriptor is corrupted; raw is an uncorrupted semantic reference.
- One generated two-head successor consumes same-name transaction-private
  RHS while an actual stale public RHS pin stays held. No canonical-cache
  walk occurs. Pin pressure applies only to controlled execution; raw has no
  equivalent transaction-private publication and supplies literal results.

Refusals preserve pending input and no output, with positive work and cleanup0.
For no-tail nonempty refusals, restoring metadata/policy and releasing pins
permits reset/retry on the same session/input. Policy knobs and descriptor
corruption are explicitly internal fixtures, not public setter coverage.

The three `evaluation_join_inline_configured-*` modes add27 public histories
(observed STEP, plain STEP, evaluating SNAPSHOT; three INLINE shapes; healthy/
CANCEL/WORK), bringing the total with previous modes to63. They use unfiltered
keyed compiler-selected plans and strict off, including STRING/FLOAT physical
lanes and STRING output. Initial empty, RHS/left append, RHS removal and
unchanged repeats match independent literal images and signed differences.
Stops occur after real matching gather with INLINE readers HELD; old public
head/content, source snapshots/maps, pending input and absence of early
callbacks/tuples survive. A real reset retries successfully; final governor
reservations are zero.

Clang23 host suite108/108, GCC16 focused7/7 and ASan/UBSan focused7/7 passed
on the integrated code/test tree before this ledger-only unit. Strict numeric
policy, numeric cap boundaries and typed strict-off histories are separate
certificates, not a complete typed-cap/strict-recovery cross-product. No new
INLINE ENOSPC pressure certificate was supplied. At that checkpoint, full
selector/cache reuse, streamed INLINE producer/ACK and selective-tail caps,
materialized INLINE, signed timestamps/weights and containing Source/carry
remained OPEN. The streamed increment below adds finite evidence. #2371
and #2300 remain OPEN. Final broader results belong to the published PR HEAD;
MSVC and LLVM22-specific tidy evidence remain outstanding locally.

## Streamed INLINE producer and consuming increment

This historical #2384 increment starts from merged #2380, main `675d8441`. Before the repair,
six fitting strict-off/on left/right/both INLINE cases returned controlled
ENOTSUP while raw produced literal `(7,8),(9,10)`. There are three matching
JOIN candidates, including rejected `(7,6)`, but only two post-FILTER survivors;
cap3 distinguishes genuine streaming from ordinary intermediate materialization.

The configured host now admits INLINE for a legal streamed keyed JOIN. Direct
prepare independently authenticates the actual dual reader and validates the
compound map in resumable chunks of at most eight logical entries. Existing
phase values1..14 stay unchanged; LEFT_MAP15/RIGHT_MAP16 carry cursor/physical
width across prepare actions. NONE keeps its existing debit/phase sequence.
Physical typed keys feed the existing producer, whose scratch batches are scalar.
INLINE SEMI/ANTI probe RHS remains unsupported. No-tail INLINE still takes the
previous strict-preflight/ordinary-consumer handoff.

At ENTER, the parked INLINE Original must be the ready prepare's exact left
source, bound to the actual owner/host/control/attempt and both held gates.
The by-value serialized move and restore protocol remains intact. Restore does
not rerun forward Original admission after CANCEL/WORK. Source readers outlive
restore and are then released by the existing finite prepare retirement.

`join_batch_controlled_setup` adds18 direct prepare cases and separate authority
checks. Left/right/both INLINE with nine logical map entries complete as typed
scalar batches with four literal duplicate pairs and four unique ACK generations.
A max_work1 map action returns at cursor8/physical-width9; the next action can
observe genuine CANCEL/WORK before the intentionally malformed ninth entry.
Footer refusal leaves scratch unpublished. Copied, partial, wrong-source,
wrong-thread, wrong-id/inactive-control readers cannot confer authority. Real
writers remain excluded while readers are held and succeed after genuine drain;
reset/fresh preparation on the same sources returns to the saved live-source
reservation baseline after prepare drain. Final source teardown reaches zero.
Malformed NONE/INLINE maps, unknown kind and SIDE retain explicit protocol errors.
Here max_work counts prepare actions: one map action can debit1+256+88 logical
work. It is not a monetary upper for the whole nested call.

`evaluation_join_inline_policy` now runs141 finite scenarios (the previous89,
nine ENTER/restore cases and43 additional streamed-policy cases):

- The earlier selective shape2 now succeeds for fitting INLINE and witnesses
  PRODUCE, scalar consumers, unique before/after ACK and held prepared sources.
  Additional cap1/2/3 cases cover all three INLINE shapes, strict off/on, bytes64:
  equality with the two surviving rows refuses; cap3 succeeds despite three
  matching candidates. Six all-rejected cases succeed at cap1 across real batches.
- Six undersized-byte cases distinguish strict refusal from permissive ordinary
  fallback. Twelve fitting/undersized valid/stale canonical-pin cases check actual
  cache policy. Controlled strict stale pins return EBUSY before row-fit. The
  legacy raw streamed arrangement pointer API instead reports ENOMEM for the
  same stale pin, including undersized bytes. This observed errno difference is
  explicit; these cases do not certify raw/controlled error parity or fix that
  legacy pointer-API diagnostic.
- A second generated two-head case consumes a transaction-private streamed RHS
  despite a held stale same-name public pin. Pin pressure applies to controlled
  execution only; unpressured raw supplies the independent semantic comparison.
  No canonical cache walk occurs. The original no-tail case remains intact.
- Exact parked/restored pointer/ownership/delta state, malformed helper authority,
  terminal zero-row schema exchange and CANCEL/WORK after exchange are checked.
  The source's Original control remains stopped while separately funded restore
  succeeds. Reports retain the first cause with positive work and cleanup0.

The18 `evaluation_join_inline_streamed_*` test registrations add156 public
histories: observed STEP/plain STEP/evaluating SNAPSHOT, strict off/on, left/right/
both INLINE and the finite fault partitions below. Together with the unchanged
36 filtered ordinary and27 configured no-tail histories, the file runs219.
The streamed plans include a real accepting/rejecting row-local numeric FILTER,
a MAP `x+1` composition, STRING/FLOAT prefix lanes, FLOAT keys and STRING output.
The typed predicate is numeric `x<flag`; STRING function expressions retain their
existing materializing policy. Literal images and signed differences are derived
independently of both raw and production expression evaluation.

All streamed histories cover initial empty, mixed accepted/rejected matches,
RHS/left append, removal and unchanged repeat. Duplicate input remains in the
ordinary controls; streamed cap histories use distinct input facts so the cap
boundary measures pre-DISTINCT post-consumer extent rather than final set size.
Each streamed shape exercises healthy execution, CANCEL/WORK at genuine consumer
rejection and before/after generation2 ACK, and successor cap overflow/retry.
Only the FLOAT-key GENERAL producer adds CANCEL/WORK after the first private
cell write: the INT-key producer has no equivalent cell checkpoint. These are
156 selected histories, not a full fault/site/type cross-product.

Successor failures retain exact public head/content, pending input, source maps/
snapshots and zero early callbacks/tuples. Reset retries the same pending input
without duplicate eventual events. Successor cap3 refuses at three survivors;
cap4 retry succeeds despite more intermediate candidates. That live policy knob
is an internal controlled-only pressure fixture; raw stays unpressured. Both
sessions ultimately agree with literal output and every final governor is0.

Clang23 host tests126/126, GCC16 changed tests25/25 and ASan/UBSan changed tests
25/25 passed on the integrated code/test tree. U1 also passed six affected
producer/source components. These results do not replace MSVC CI or LLVM22 tidy.
Local poll/interval derivations are recorded in
[OPERATOR_SOURCE_ACCOUNTING.md](OPERATOR_SOURCE_ACCOUNTING.md#streamed-inline-join-local-intervals).
They distinguish tariffs from SOURCE distance and do not certify containing
caller/return or inherited correlated carry. At that checkpoint, full cache/
selector intersections, materialized INLINE, INLINE probe RHS, timestamp/weight
propagation, new INLINE ENOSPC pressure, full platform and Native/Source/carry
obligations remained OPEN. The next increment supplies finite memory-pressure
and matching-cap evidence. No row or issue is closed by either increment.

## INLINE pressure and matching-cap increment

This increment starts from merged #2384, main `37928b5f`. It changes the
existing generated history fixture and its registrations, with no production
algorithm, admission, ownership, quantum or tariff change. The five actual
failure sites and matching-cap partition below add144 finite histories to the
previous219, for363 in `test_evaluation_join_inline.c`. The policy executable
retains its141 scenarios. There are61 registered INLINE test selections.

The18 `evaluation_join_inline_pressure_*` selections run90 histories: three
INLINE source sides, observed STEP/plain STEP/evaluating SNAPSHOT, strict0/1,
and five successor failure sites. Initial empty and the old two-row public
image are verified before adding the pending input that produces three surviving
rows. Both-INLINE retains FLOAT keys, STRING/FLOAT prefix lanes and STRING
output; the right-INLINE numeric control retains its generated x+1 MAP.

| Site | Actual reached failure witness |
|---|---|
| Prepare SCRATCH ENOSPC | Phase11 after committed arrays credit, owned keys/hash-head/types/key-row and completed RHS indexing; a later real governed scratch admission refuses |
| Sink ENOSPC | One-shot SINK_SCHEMA pressure before actual row-buffer/output admission, after scalar consumer work |
| Positive-output ACK then ENOSPC | A current-successor ACK has positive output extent and genuine generation/work; a later real admission refuses with private output already acknowledged |
| Owned-prefix prepare ENOMEM | Phase8/cursor2 after committed arrays credit and allocated keys/hash-head; the next prepare allocation consumes the existing failure hook |
| Sink ENOMEM | One-shot constructor failure survives the first SINK_SCHEMA row-buffer turn and is consumed by the actual output constructor |

The budget competitor is a genuine same-address RESERVED token on the enforcing
controlled governor. It reserves actual available capacity minus one byte;
there is no budget mutation or fabricated ENOSPC. First-error observation
requires a real increase in the isolated governor's denial counter, then checks
rollback success, RELEASED state and the exact reserved-byte decrement for that
peer. Capacity returns only after the host selects the actual denial and before
mandatory cleanup. This qualifies cleanup with restored capacity, not permanently
exhausted-memory cleanup. Allocation failures instead require ENOMEM, consumed
one-shot hooks and unchanged denial count. Session denial metadata distinguishes
the two causes. Memory pressure is an internal controlled-only fixture; raw is
an unpressured semantic reference.

Every failure retains the exact controlled old head and literal image, pending
input and original source snapshots/maps, with no premature callbacks/SNAPSHOT
tuples. Reports preserve the initiating result with positive work and cleanup0.
Actual writer acquisition/release on both public input sources succeeds after
cleanup. Genuine reset retries the same pending input without reinsertion, then
independent literal images/signed differences, deletion and unchanged repeats
verify future behavior. Per-attempt ACK generations remain consecutive.
The exact peer-refund delta is a local resource assertion; no unexplained
pre-call/live-session reservation equality is claimed. Final zero is checked
after all sessions/source-owned state are destroyed.

The18 `evaluation_join_inline_cap_*` selections run54 histories under equal
internal cap pressure on controlled and raw: the same three source sides,
three output routes, strict0/1 and successor caps2/3/4 against three survivors.
Limits2 and3 return EOVERFLOW in both sessions; cap4 succeeds despite four/five
intermediate JOIN candidates. The latter witnesses actual producer/consumer/ACK
and discriminates post-consumer extent from ordinary intermediate materialization.
The FLOAT-key/STRING-output case supplies the typed boundary alongside numeric
and MAP controls. These are finite partitions, not every type-by-side product.

After controlled refusal the exact old head, input/source retention and report
assertions remain active. Both caps are raised to4. The genuine controlled
control is reset and retries its same pending input before the subsequent
removal/unchanged history; raw is called again and matches eventual literal
content. Raw status under the same cap and eventual content are qualified;
controlled transactional head rollback is a separate assertion and is not
inferred for raw. Direct live field changes qualify an internal policy boundary,
not a public setter API.

Clang23 JOIN hosts162/162, affected prepare/materialized-memory/threading
components6/6, GCC16 INLINE61/61 and ASan/UBSan INLINE61/61 passed on the
integrated code/test tree. The threading audit still covers621 exact source
sites with its unchanged negative selftests. These functional/resource results
add no new four-platform, whole-caller or Native/Source/carry certificate.
Remaining tight-memory/type/selector/cache intersections, INLINE probe RHS,
materialized/recursive representation, signed timestamps/weights, overflow and
containing lifecycle/accounting/platform obligations remain OPEN. No issue or
matrix row is closed by this increment.

## Finite scenario inventory

`evaluation_join_domains_plain` and `evaluation_join_domains_batch` each run
20 independent histories: four predicate shapes, keyed/product JOIN, and
healthy/CANCEL/WORK variants for each filtered shape (the unfiltered shape has
only its healthy variant). These are public parser/compiler-selected plans and
real `wl_session_step` calls, with no rewritten plan or synthetic owner.

| Shape | Actual generated condition | Positive destination evidence | Reference and limit |
|---|---|---|---|
| kind0 | Numeric constant argument `b(k,y,1)` or product `b(j,y,1)` | RHS evaluation phase44; private hash insertion phase28 for keyed nonempty selected rows | Literal accepted RHS values2/4, later6; rejected flag0; numeric RHS failure expressions are covered separately |
| kind1 | STRING constant argument `b(k,y,"keep")` or product equivalent | Actual scalar STRING RHS equality with session intern table, phase44 and keyed private hash | Literal accepted strings `keep`/`also`, later `next`; rejected flag `reject` |
| kind2 | Numeric constant RHS flag plus post-JOIN `strlen(y)>2` | Positive composed scalar FILTER on the real generated projected output | Only length4 values reach the post-FILTER; the short `x` row already fails its RHS flag. This is not independent post-FILTER rejection coverage |
| kind3 | Unfiltered RHS over the accepted data only | Plain keyed phase47 proves persistent row insertion; configured keyed batch requires real pipeline PRODUCE | Equivalent result control with different input data; no new persistent-reuse or identical-input cache-equivalence claim |

Every history checks a relation-qualified independent fixture reference,
projected output width and nonempty physical INT64/STRING column types, exact
signed callback differences, no duplicate events and an unchanged repeated
public call. The sequence is initial empty output, filtered all-rejected RHS
(where applicable), mixed accepted/rejected RHS, RHS append, RHS deletion and
left append. Duplicate RHS input is deleted with its actual multiplicity;
publication is checked as a set at each stage. Both controlled and uncontrolled
successful results must independently match the fixture reference; the raw
result is not used to compute the expected answer.

The initial registry assertion deliberately permits missing OR empty. After the
first evaluation the actual RHS registry relation must be present-empty. This
does not certify a separately selected naturally absent registry route.
Phase7 records only the engine's `used_delta` boolean. It is not a source
identity, FORCE_DELTA/forced-empty or full selector-matrix certificate. The
phase47 diagnostic is also used by empty-filter recipe validation; filtered
cases' printed counts must not be interpreted as persistent indexing. Only the
unfiltered keyed/plain assertion excludes that alternate producer.

CANCEL/WORK histories stop at RHS row1 after two genuine evaluations, following
an earlier nonempty public output. They assert the exact stop result, old
public-head identity and content, retained pending input, no early signed
events, nonzero performed work and zero cleanup error. A genuine control reset
retries the same session and pending input. The WORK injection asserts a real
`charge_attempt(..., UINT64_MAX)` refusal; it is not an independently derived
primitive exact/one-short boundary. Final governor reservations must be zero
after both sessions, control, plan and program-owned intern bindings are freed.

## Actual materialized selection and memory pressure

The original twelve scalar `evaluation_materialized_body` modes retain the natural compiler K-fusion lineage,
whole-SCC selection and authorized preserved-body identity. Its owned child
JOINs have no primitive materialized flags. This accepted equivalent route is
not a controlled materialization-cache hit.

The existing plain/batch, strict/output-policy and CANCEL/WORK/ended-generation
modes remain registered. Two new modes add actual enforcing-governor admission:

| Mode | Reached boundary | Required result and recovery |
|---|---|---|
| `batch-memory-first` | Selected body's JOIN preparation has committed arrays, before initial publication | Genuine peer reservation consumes available governed capacity; producer must select ENOSPC and increment its denial counter; no published image/events |
| `batch-memory-successor` | Same authentic preparation after the first9-row public image and new pending input | Same denial while previous `path`9 and `plain`5 contents remain visible, with no new events and pending input intact |

Peer bytes are derived from the actual governor's usable and reserved totals.
The token stays owned until the first producer error hook observes the actual
ENOSPC/denial. That hook releases peer capacity before mandatory cleanup, and
the token must reach RELEASED. The completed report preserves ENOSPC, positive
work and zero cleanup error. Reset/retry uses the same session/input and then
checks the existing independent literal `path`9→12→6 and `plain`5→6→5 histories,
exact signed events, unchanged repeated calls and final zero governor bytes.

The raw session shares the successful reference fixture but does not receive
the same injected pressure. These modes prove controlled admission/recovery;
they do not claim equal-pressure raw denial or an allocator ENOMEM result.
Existing `batch-error-ended` separately qualifies the injected allocation error
and first-error-versus-ended-Original behavior.

## Preserved K3 bodies consuming INLINE EDBs

This increment starts from merged #2387, main `6b95b661`. It adds66 finite
INLINE-EDB histories to the twelve original scalar materialized-body modes,
for78 `evaluation_materialized_body_*` selections. The separate ordinary INLINE
history file remains363 histories with its141-scenario policy executable in61
selections. No runtime algorithm, policy, ownership protocol, tariff or quantum
changes in this increment.

The natural programs declare `edge(p:pair(symbol,float) inline,x:int64,y:int64)`
and scalar INT64 `path`/`plain`. Three recursive path occurrences and one static
INLINE edge occur in left, middle or right body position. Compiler-generated
K3 lineage and the exact plan-owned opaque identity are required; expanded K
sequences contain real materialized JOIN hints. The actual controlled private
preserved body instead has three ordinary, opaque-free, materialized=false
JOINs. This qualifies materialized-equivalent preserved-body consumption of
INLINE EDBs, not a controlled literal-materialized/cache-hit primitive or an
INLINE recursive head.

Independent literal chain references give `path`7->9->5 and `plain`5->6->5:
five initial edges plus `(1,5),(2,6)`, then appended6->7 and `(3,7)`, then
removing3->4 leaves five base edges with no four-edge-derived path. These sets
are independent of raw and the evaluator. Observed STEP checks exact signed
relation-qualified differences; plain STEP installs no observer; evaluating
SNAPSHOT omits delta observers and checks full path/plain tuple images after
actual mutations. Unchanged repeats are separate checks. Heads remain scalar
width2 INT64; STRING/FLOAT values belong to the EDB prefix, with physical x/y
lanes2/3. No FLOAT recursive key or signed timestamp/weight product is implied.

The18 healthy histories cover three placements, three public routes and
plain/configured batch execution. Actual selected-body bindings and whole-SCC
witnesses exclude scalar companion work from the INLINE counters. Ordinary
consumption observes actual HELD sources, descriptor/storage readers, map and
typed prefix, displaced keys and matching gathers. Configured late-INLINE
consumption observes the real selected op's INLINE RHS, dual readers, owned
arrays and complete nonempty RHS index, then that op's producer/ACK/completion.
Stages0/1 require positive gathered rows or positive-output ACK. Deletion may
have no derived output: actual INLINE RHS hash work or nonempty-left/RHS
preparation and positive producer completion qualify that reached no-output
work. Legal empty SCC visits cannot alone satisfy the separate positive stage
witnesses.

The48 additional policy/recovery histories are finite partitions:

| Partition | Cases | Actual boundary |
|---|---:|---|
| Strict or cap1, all placements/routes | 18 | Both sessions independently return ENOTSUP or EOVERFLOW; clear strict/raise cap128 and retry the same facts. A scalar companion may refuse before INLINE consumption, so this is mixed-body policy evidence rather than an INLINE-specific refusal or cap equality with the final recursive head |
| Successor CANCEL/WORK, all placements/routes | 18 | After publishing path7/plain5, append6->7. Stop actual HELD INLINE matching gather for left/middle or completed owned/indexed INLINE preparation for configured right; no INT-key private-cell stop is invented |
| Right INLINE prepare ENOSPC/ENOMEM, initial/successor and all routes | 12 | Match the actual selected-body op and nonempty INLINE RHS/left. Deny governed SCRATCH after owned arrays/index, or consume a one-shot allocation failure after keys/hash-head prefix and committed arrays credit |

Controlled failures preserve exact old path/plain heads and literal images (or
the genuine initial-empty state), pending input and original edge snapshot/map,
with zero early signed/tuple delivery. Reports retain the selected cause with
positive work and cleanup0. Raw policy refusal status and eventual literal/signed
history are checked, without imposing controlled pre-cutoff rollback guarantees
on raw. Policy fields are internal; raising them is not public setter coverage.
Faults are disarmed before genuine control reset and same-pending-input retry;
subsequent removal and unchanged calls still match the independent references.

The six observed/plain STEP WORK cases deliberately retain the genuine parent
Origin/transaction and admitted metadata while child/receipt/native cleanup is
drained. Read-only retained-owner observations require the actual binding,
positive metadata bytes, EVALUATE/PENDING and closed generation, without a
platform-size constant or assumed active private financier. The real source
writer and valid public insert/remove, Snapshot and try_destroy remain EBUSY;
the latter retains its caller pointer and emits no tuples. Every public report
field and source/head snapshot stays unchanged across these protected probes.
Plain STEP has no observer and supplies the unambiguous pending-parent Snapshot
exclusion case. After real reset/retry, a freshly looked-up live input accepts
a writer. Snapshot WORK, CANCEL and the tested positive-error dispositions
instead permit immediate writer acquisition. Cleanup0 is therefore not a claim
that every parent lease/metadata owner disappeared at every stopped return.

Both sessions share the enforcing governor, but only controlled preparation
receives injected memory pressure; raw runs before that injection. A real
same-address RESERVED competitor consumes actual available capacity minus one.
A genuine selected ENOSPC/denial-count increase precedes checked rollback,
RELEASED state and exact peer-byte decrement, restoring capacity before mandatory
cleanup. ENOMEM instead requires the intended consumed allocator knob and no
denial-counter increase. This qualifies restored-capacity cleanup, not permanent
exhaustion; final governor zero follows destruction of both sessions and all
governed source-owned state. No unexplained live-source total-zero or complete
intermediate financial-baseline equality is asserted.

Clang23 JOIN hosts228/228, affected prepare/source-shape/threading checks5/5,
GCC16 materialized-body78/78 and ASan/UBSan materialized-body78/78 passed on the
integrated code/test tree. Exact621-site threading audit and its negative
selftests remain unchanged. These results supply no new MSVC/TSan runtime,
complete platform or Native/Source/carry certificate. Controlled literal-
materialized cache hit/pin/reuse, full/delta/empty selector products, cold/wide/
nullary and other compound products, recursive metadata and containing lifecycle/
accounting obligations remain OPEN. No matrix row or issue is closed.

## Ordinary JOIN consuming INLINE sources

The previous direct `a(i,pair(x,y)),b(x,z)` controlled route returned ENOTSUP
although uncontrolled execution returned `(1,7)` and `(2,8)`. Ordinary JOIN
now calls the existing owned INLINE-admission reader for both input sides.
The shared helper admits INLINE only for opcode JOIN; SEMIJOIN/ANTIJOIN retain
zero admission flags. The reader validates the arity map in charged chunks
of at most eight entries under its original descriptor/owner readers. Existing
retained BEGIN/END, physical metadata and gather/retirement paths are reused.
No raw fallback, new representation or widened support gate is introduced.

`evaluation_compound_sources` replaces that exact ordinary refusal oracle with
literal success on observed STEP, unobserved STEP and evaluating SNAPSHOT. It
checks actual fused JOIN keys `col1`/`col0`, projection0/4, direct INLINE source
publication, source identity/map retention and final governor zero.

`evaluation_join_inline_observed`, `_plain` (unobserved STEP) and `_snapshot`
each run four generated programs with healthy/CANCEL/WORK variants:36 histories.
Batch/output environment overrides are cleared for these ordinary routes.

| Program | Actual INLINE input and physical mapping | Predicate/result evidence |
|---|---|---|
| kind0 | Left compound occupies lanes0/1, key is col2, value col3; scalar RHS key col0 | Numeric RHS constant flag plus a genuine joined `y=0` rejected by `x<y` |
| kind1 | Right compound occupies lanes0/1, key col2, value col3, flag col4; scalar left key col0 | RHS flag after compound lanes, accepted/rejected rows and post-JOIN comparison |
| kind2 | Both sources INLINE; left STRING/FLOAT prefix before key col2; right STRING value inside its compound | Typed reader snapshots; accepted flag and matching short string reaches JOIN gather, then fails `strlen(y)>2` |
| kind3 | Same typed sources, actual zero-key product | Independent product reference and non-vacuous STRING post-FILTER rejection |

The STRING/FLOAT prefix's physical lane types are observed. This is not FLOAT
result-arithmetic or timestamp qualification. Expected values come from explicit
logical fixture facts and key selection, independent of raw output and the
production expression evaluator. Source kinds/readers and current physical
columns are read from the authentic owned JOIN context. Phase17 decodes its
actual row; phase18 witnesses matching row gather. The predicate-false row has
an accepted RHS flag and is witnessed at gather before its absence is checked
in the final literal result. This resolves rejection on these new INLINE
compositions; the old kind2 positive-only scalar history above remains limited.

Each history goes from empty RHS to populated output, then appends input,
removes an actual single RHS fact and repeats unchanged calls. Left duplicate
input is intentional; this is not duplicate-RHS deletion coverage. Observed
STEP checks exact signed set differences; SNAPSHOT checks complete tuple images.
CANCEL/WORK is injected at row gather with both source records currently HELD
and the selected side's current descriptor INLINE. Accumulated history bits
cannot satisfy that stop condition. The prior nonempty head/image, pending
input and source snapshots/arity maps must remain unchanged, with no early
events/tuples, exact report cause and zero cleanup error. Genuine control reset
retries the same session/input; successful destruction leaves governor zero.

Eight separately labelled constructed internal `evaluation_join` cases exercise
both sides: valid ordinary INLINE admission, malformed-map EINVAL, and unchanged
SEMI/ANTI ENOTSUP. They are admission-boundary evidence, not generated selectors.
The existing SIDE refusals remain. This increment does not qualify configured
strict/batch or output/memory-policy INLINE products, materialized INLINE,
complete selectors, timestamp propagation, cache pins/reuse or full Source/carry.

## Persistent JOIN indexes and selected-source histories

This increment starts from merged #2389 on main `51216670`. The focused
`evaluation_join_cache_*` fixture adds six healthy histories and36 selected
CANCEL/WORK histories. Every history uses the same logical input script in
three sessions: the actual default controlled profile, deliberately
`diff_enabled=false` controlled comparison, and a diff-disabled raw reference.
The default is asserted as returned by Session creation and left unchanged.
The disabled comparison also disables append optimization, so this is a
whole-evaluation/private-index semantic comparison, not an isolated cache-only
work or resource comparison. Each session has its own enforcing governor and
each controlled session its own control.

Natural programs use five scalar INT64 output heads. `first` and `mirror`
share canonical scalar RHS key0; `reverse` uses key1; `exact` uses key0+1;
`filtered` has a genuine RHS flag1 expression and private selected-row index.
The left input is either scalar or has a real STRING/FLOAT INLINE prefix,
with physical join keys displaced to lanes2/3. The RHS remains canonical
scalar INT64. Persistent INLINE/non-INT64 RHS and literal-materialized result
cache execution are not activated by this increment.

An independent logical fact-set enumerator specifies each head without using
raw output, production hashing or expression evaluation. Initial left facts
are(1,1),(2,2). Flag1 RHS endpoint facts are(1,1),(2,2),(1,2),(4,4);
a rejected duplicate-valued(1,2,0) and a
unique rejected(2,7,0) distinguish predicate work from observable rejection.
The latter is present in unfiltered output and absent from `filtered`.
Duplicate left insertion requires real input/index work but no new set output
or signed events; it is not FORCE_EMPTY selector evidence.

An empty public bootstrap establishes the session before BOTH sides are
populated. This reaches the general public transaction for the first nonempty
call without forcing an internal route; it does not claim that the first-ever
dense plain STEP selects this cache path. The mutation script then appends
left(4,4), appends/removes RHS(4,2,1), repeats left(4,4), changes both sides with
left(9,9)/RHS(9,9,1), and removes that RHS row. All five images and exact
relation-qualified signed differences on observed STEP are independent of
another execution. Plain STEP has no observer; evaluating Snapshot collects
full tuple images after mutations. Unchanged calls are separate checks.

Actual authorized dynamic bindings, capture-only framed child/current JOIN
and HELD source reader/gate identities bind observations. Typed INLINE prefix
map/types/values and the physical displaced keys are checked. Full RHS is the
actual canonical public EDB binding with its current snapshot. Default scalar
observed STEP selects the exact one-row appended RHS descriptor/content on the
RHS-only append, and the exact VARIABLE frontier on qualified left-only
appends. Plain STEP, Snapshot and typed INLINE-left histories select FULL RHS
in these cases. The read-only selected-source observation neither creates a
source nor forces selection. Phase7's used_delta flag alone is not the oracle.

The default full path builds three key-signature indexes and shares a sibling
request; warm left-only calls reuse the actual index objects and initialized
bucket/link contents. Successful flat registry relocation is permitted.
RHS mutation may already invalidate published index generations/extents before
evaluation, so comparisons capture the post-mutation/pre-call image. The
scalar observed RHS-delta call leaves that image untouched and uses an ephemeral
private index; it does not key the full-source registry with a delta descriptor.
Removal and both-input changes rebuild against current canonical snapshots and
indexed extents. Real build phase47 is restricted to the actual nonempty
unfiltered current build op, excluding its overloaded empty-filter recipe
meaning. Phase48 proves persistent candidate/probe work, not necessarily a
matching gathered result. The filtered private sibling separately reaches the
unique false RHS fact and its required literal output.

The36 recovery histories cover scalar/INLINE left, three routes, CANCEL/WORK
and three distinct actual sites: warm validation/reuse, insertion of two real
RHS rows into a new private index, and a warm persistent candidate probe. Every
fault preserves the exact published registry container/count/signatures/index
payload and old five-head pointers/images, pending input and both input
snapshots/maps/types/cells, with zero early events/tuples. Capture occurs after
public mutation; an invalidated prior index is not called usable for new facts.
Genuine control reset retries the same facts without reinsertion. Fresh retry
work witnesses and future updates/removals/unchanged calls remain mandatory.

At these sites private differential transactions/readers are disposed for
WORK as well as CANCEL, unlike the prior preserved-recursive WORK case. Both
actual input writers reacquire immediately. STEP retains a logical PENDING
retry state and a fixed descriptive RESERVED completion slot; it is not an
active WAIT_SOURCE owner or retained transaction/metadata credit. The public
Original report remains separate from that slot's inert report. Snapshot
instead has SNAPSHOT_RETRYABLE and an INERT completion slot. Reached STEP
insert/remove/Snapshot protection and frozen report/input/index observations
are finite assertions; no universal destruction refusal is implied. Final
governor zero follows complete teardown, not a claim of an intermediate
financial baseline or containing Source/carry certificate.

Validation and reproduction for the final code/test tree are recorded below.
Other selector/empty/pin/materialization/type/representation and signed metadata
products, current platform validation and containing Native/Source/carry remain
OPEN. No issue or matrix row is closed.

## Persistent cache policy, admission and pin increment

This bundle starts from merged #2391, main `719db75f`. It adds32 selected
histories to the earlier42 cache selections, for74 registered
`evaluation_join_cache_*` histories. The executable commits are `a13cc9c2`
(caps), `1a09dd1f` (resources) and `a63e469e` (pins). Production algorithms,
supported domains, quantum, tariffs, public ABI and memory prices are unchanged.
Every new history retains the independent five-head fact-set reference and
future append/removal/rebuild/unchanged-call checks of the preceding increment.

| New modes | Selected histories | Actual predicate |
|---|---:|---|
| `cap4`, `cap5`, `cap6` | 12 | Scalar observed/plain/Snapshot and INLINE-left observed, three initial caps each |
| `image-denied`, `build-denied`, `key-nomem` | 15 | Scalar observed/plain/Snapshot and INLINE-left observed/Snapshot, three distinct resource sites each |
| `pin-held` | 5 | The same five resource representation/route pairs, genuine canonical RHS/key0 pin |

The cold cap partition sets the same internal numeric policy in default
controlled, diff-disabled controlled and raw sessions after empty bootstrap.
Independent logical facts yield five first/mirror JOIN pairs before DISTINCT,
including the two flag-distinct RHS facts projecting to `(1,2)`, and four set
rows. Actual authorized first/mirror phase20 output extent proves caps4/5
refuse with EOVERFLOW; cap6 succeeds at extent5. Both controlled refusals retain
empty old heads, exact published registry and inputs, pending facts and no early
delivery. Raw supplies equal-cap status only, without a transactional rollback
claim. Caps are cleared and genuine controls reset for same-pending-input retry;
cap6 is also cleared after the first stage before future calls. This is an
internal cold numeric cap certificate, not a public setter or warm/complete
policy cross-product.

Resource faults target only the default controlled profile. Warm stage1 image
admission is denied before the image/slot reserve; stage5 replacement-build
admission is denied after actual image/slot initialization and all three prior
entries copied, before build credit. Both bind the current authorized first/
mirror JOIN, canonical RHS and actual transaction progress. A same-address
RESERVED peer consumes current available governor capacity minus one byte;
real denial-counter increase, ENOSPC and session denial metadata are required.
The peer stays RESERVED through completed public failure, preserved initiating
report and cleanup0, then checked rollback returns exactly its bytes and reaches
RELEASED. Other cleanup refunds may increase available capacity before that
rollback; no fixed global intermediate balance is asserted. This qualifies
cleanup with the competitor still held, unlike earlier restored-capacity cases.

`key-nomem` instead binds the actual admitted key scratch0 before index selection,
with scratch1 still uninitialized, exact key-byte extent and a witnessed failed
physical allocation. ENOMEM preserves the existing published warm registry and
does not increase governor denials. The countdown hook stays at0 until explicitly
reset to-1 before retry; teardown also requires it cleared. This is pre-index
JOIN-buffer failure, not persistent-image/index malloc coverage. At that
checkpoint those malloc sites remained unqualified by an allocation hook; the
allocation increment below adds finite evidence at eight controlled boundaries.
Both resource and pin histories
retain exact old heads/content, pending facts, input snapshots/maps/types/cells,
published index payload and zero early delivery, then reset and retry without
reinsertion. The other controlled profile and raw are unpressured whole-history
semantic comparisons, not equal-pressure resource/error certificates.

`pin-held` acquires a genuine differential pin on the published canonical RHS/
key0 index after stage0. Exact entry/index/generation/snapshots, the authentic
source reader and one-pin count are asserted. Stage1 mutates LEFT only; RHS
writer acquisition returns EBUSY. At that checkpoint the controlled image-entry check rejected
that held pin with ENOTSUP. The runtime increment below supersedes that refusal. Public
failure preserves the pin, registry location/payload, heads and inputs. Registry
snapshots compare pins, pending and deferred flags; the sole one-pin exception
requires the actual live pin's exact session/entry, and all older modes retain
zero-flag invariants. Release restores the exact pre-pin registry image and
permits writers before same-input retry, warm reuse and later RHS rebuilds.
This is internal authentic-pin setup consumed by public evaluation, not a new
public primary-cache producer, deferred invalidation or eviction certificate.

The integrated executable tree passed Clang23 JOIN hosts302/302 and five related
serial/arrangement/cleanup checks. All74 cache selections passed GCC16 and
ASan/UBSan. Formatting and diff checks passed. These are local code/test-tree
results; the following ledger commit changes documentation only. MSVC/hosted CI
on the published HEAD remains separate evidence. Final governor zero follows
complete teardown, not an intermediate financial certificate. Persistent typed/
compound RHS, materialization-result cache, remaining expression/selector/
metadata/policy products, deferred/eviction behavior, reached lifecycle and
containing Native/Source/carry obligations remain OPEN. No row or issue closes.

## Present-empty, rejected and refill cycles

This bundle starts from merged #2396 on main `79a5b502`. Three executable
commits (`31ba92dc`, `cfe1d82a`, `26746e8e`) add30 selected histories to74
previous cache selections, for104 `evaluation_join_cache_*` registrations.
No production algorithm, supported domain, quantum, tariff or public ABI changes.

Six healthy cycles cover scalar and STRING/FLOAT INLINE LEFT over observed STEP,
plain STEP and evaluating Snapshot. A separate duplicate-free public input script
keeps the Boolean logical fact reference independent of evaluator output and
avoids the earlier script's duplicated LEFT insertion. The natural five heads,
default controlled profile, deliberately diff-disabled controlled comparison and
raw reference are reused. At every stage all five literal images/types, exact
relation-qualified signed observed differences or Snapshot tuples, once-only
events and unchanged repeats are checked independently.

The script populates LEFT(1,1),(2,2) and RHS(1,1,1),(2,2,1),(2,7,0), then removes
the accepted RHS facts: `first` retains(2,7) while `filtered` is genuinely
all-rejected. Removing the last RHS fact leaves an existing typed canonical
zero-row descriptor, then RHS(1,1,1) refills it. Removing both LEFT facts leaves
a scalar or mapped typed INLINE zero-row input. RHS(2,2,1) is appended while LEFT
stays empty; LEFT is then refilled, followed by unmatched LEFT(4,4), matching
RHS(4,4,1), its removal and separate unchanged repeats.

Current authorized JOIN/occurrence observations bind HELD self/control/attempt,
descriptor/owner readers, identity/generations/extents, physical types, INLINE
maps and displaced keys. The reached typed zero-row descriptors use real scalar
HELD readers; bare flags are excluded before those assertions. Actual
`delta_source` output under the recursion guard distinguishes canonical FULL
(including present-empty RHS) from exact nonempty append descriptors and LEFT
frontiers. Default scalar observed RHS appends use the exact one-row override,
including when LEFT is empty; LEFT refill/unmatched append binds its actual
VARIABLE frontier. This does not certify a nonNULL empty override, missing
relation or FORCE_EMPTY. Empty output alone supplies none of those predicates.

Every head must reach the final schema field. Empty inputs bypass later
probe/gather/private/persistent row work and preserve the post-mutation registry;
all-rejected nonempty RHS instead requires real false-row predicate work for
`filtered`, without that head's accepted/private rows. Refill rebuilds all three
key signatures against canonical current RHS extents/snapshots; later warm work
requires four actual reuses and unchanged index identity/payload. Input mutation
may legitimately invalidate the previously published cache before evaluation.

The24 interruption selections use scalar observed/plain/Snapshot and INLINE-left
observed:16 no-output cases and eight refill cases, each with CANCEL/WORK.

| Reached stop | Actual work witness | Fixed stopped-return disposition |
|---|---|---|
| Present-empty RHS | Unfiltered first/mirror final schema field, nonempty LEFT and canonical RHS0, before output construction | CANCEL drains; WORK on selected STEP routes retains the authentic phase8 Original/transaction, admitted metadata and both input leases; Snapshot WORK drains |
| Nonempty all-rejected RHS | Actual filtered false row selection completed, final schema field, no accepted/private rows for this head; earlier siblings performed real index work | Both causes drain the private transaction/readers |
| LEFT refill after RHS growth | Actual key0 phase14 build occurrence followed by consecutive phase47 rows0/1, after both hash-link writes | Both causes drain the private index image/readers |

The refill identity is the authenticated session/control/attempt/generation,
plan/op/occurrence, canonical source snapshot, key and private-image-slot tuple.
Phase14 follows new arrangement installation; no claim inspects transaction
source fields assigned after that hook. Filtered empty-recipe phase47 cannot
satisfy this unfiltered nonempty build tuple. Pending observations are invalidated
on watch/plan transition/reuse and sequence completion; retry must perform a new
two-row build under a different active attempt. This is not opaque uncommitted
allocation-address/payload qualification or every-signature interruption.

All failures preserve exact public old heads/content, pending facts, input
snapshots/maps/types/cells, post-mutation registry and no early delivery. Reports
retain the initiating cause, positive performed work and cleanup0. Retained STEP
WORK requires the authentic Original, real admitted metadata, closed generation,
drained child state, PENDING and both writers EBUSY; disposed cases require
completed-state invariants and writers that acquire. Reports and all returned
owner observations stay frozen across protected probes. The financier active bit
is observed/frozen, not an all-platform financial certificate. Genuine reset
retries the same pending facts without reinsertion, verifies new successful
site/build work and writer release, then completes future changes/repeats.
WORK injection is an actual oversized charge refusal, not an independent
primitive exact/one-short boundary. Final governor zero follows full teardown.

The integrated executable tree passed Clang JOIN hosts332/332 and seven related
serial/source/cleanup checks, with GCC and ASan/UBSan cache selections104/104 each.
Formatting/diff checks passed. These local executable results precede the ledger
commit and do not replace hosted/MSVC validation. The selected failure matrix
is not the entire healthy representation/route product. Bare/missing/empty-
override/forced selector, wider/nullary/typed RHS and expression/policy products,
materialization-result cache, signed timestamps/weights, deferred/eviction and
containing lifecycle/Native/Source/carry obligations remain OPEN. Main's cold
LEFT JOIN support is reused context under #2301, not a new cold certificate here.
No matrix row or issue closes.

## Controlled persistent-index allocation failures

This bundle starts from main `13218aca`, after the empty/refill increment and
merged cold JOIN-aggregate source support. It adds40 selected allocation-failure
histories to104, for144 `evaluation_join_cache_*` selections. The three
executable units cover image/slots, copied metadata and replacement build
prefixes; the ledger is a separate fourth commit. No production cleanup defect
was exposed by these injections.

A narrow `WL_SESSION_TEST_HOOKS` callback receives only the actual active
control/attempt, explicit allocation-site tag and requested bytes. Returning true
requests NULL from the allocation wrapper at the selected controlled diff-attempt
malloc boundary; there are eight distinct sites.
The existing ENOMEM returns and reservation/pay/checkpoint/commit/ledger/drain
sequence remain unchanged. Without test hooks the expressions expand directly
to the original malloc byte counts; the production library contains no new
callback/helper symbol. This is deterministic allocator-boundary fault injection,
not an OS allocator-exhaustion campaign, raw allocator interception, new public
fault API or opaque transaction/ownership inspection.

All40 histories use the natural five-head programs and existing independent
logical fact reference, selected over scalar observed STEP/plain STEP/evaluating
Snapshot and typed INLINE-left observed STEP. Faults affect only default
controlled execution. Diff-disabled controlled and raw profiles remain
unpressured semantic references; they do not certify equal-pressure errno or
resource parity. All104 earlier registrations and source/site/oracle assertions
remain. Each new registration retains unit/tsan/JOIN-host labels and a120-second
watchdog; the suite label is not evidence of a TSan runtime execution.

| Selected allocation boundary | Histories | Actual partial prefix at refusal |
|---|---:|---|
| Image / slots | 8 | Warm stage1 with three published signatures; image credit admitted before image NULL, or full image zero-initialization and slot admission before slots NULL |
| First / second copied name / key array | 16 | Warm existing-entry copy; exact phase2 slot and site, complete image/slot initialization, prior name-byte/key-cell copies. Second-entry faults add one complete preceding metadata pair; keys faults add the current copied name |
| Replacement header / keys / hash heads / next links | 16 | Stage5 both-input mutation requires a stale key0 replacement. Header NULL follows build admission; keys NULL follows initialized header, retained governor and moved/committed credit. Heads/next NULL add successful preceding array allocations |

Allocation extents come from actual native structures and independently derived
fixture counts: the image includes all five heads' operations plus two reserved
suffix slots each; names and key extents bind actual copied entries; build
bucket count is the power-of-two bound for twice the current canonical RHS row
count. Build sites require no phase14 index installation, no row insertion or
bucket initialization and exactly the expected allocation/copy prefix. Keys
and heads are allocated, not yet populated, before next-link allocation. A
pre-malloc callback alone proves no allocation success; reaching the next
unchanged checked operation/copy establishes the preceding successful prefix.

Each failure authenticates the current JOIN/op/source under authorized dynamic
bindings and canonical nonempty FULL RHS. Exactly one selected NULL must yield
initiating ENOMEM with positive work, cleanup0, unchanged governor denials and
false budget-denied metadata. Fixed disposed/completed state, no live private
transaction/cleanup/retained owner and actual available input writers are
required. Exact old public heads/content, input descriptors/snapshots/maps/types/
cells, post-mutation registry container/signatures/generations/initialized index
payload, pending facts and zero early callbacks/tuples survive the failure and
protected probes; reports remain frozen. Mutation may already invalidate the
old index before the baseline is taken.

The callback clears before genuine reset. Retry uses a different active attempt,
permits all allocations, completes real copy/build work and checks the same
pending facts without reinsertion against every independent head/delivery image.
Build-failure modes alone append a post-recovery unmatched LEFT(8,8) stage after
RHS removal: scalar observed binds its exact VARIABLE frontier; all four real
warm reuses, no indexed writes and unchanged index identity/payload are required.
Later changes and unchanged repeats preserve signed once-only or Snapshot
results. Full teardown leaves the callback clear and governor reservations zero;
no unexplained intermediate total-resource equality is claimed.

The final executable tree passed Clang JOIN hosts372/372 and20 affected serial/
source/arrangement/cache/pin/drain components, plus five ABI/header/threading
contracts. GCC and ASan/UBSan each passed144/144 cache selections; formatting and
registration checks passed. These results precede the ledger-only commit and
supply no local MSVC/TSan-runtime or hosted published-HEAD certificate.

These are finite one-worker serialized, controlled-only, current-width existing-
entry/key0 fault partitions. New-signature metadata, wider counts/keys/types,
concurrent fault injection, competing first-error histories, all selector/policy/
representation products, deferred invalidation/eviction, materialization-result
cache, signed timestamp/weight and containing Native/Source/carry/platform
obligations remain OPEN. No opaque private allocation-address/payload, full
financial certificate or complete malloc-failure product is inferred. F05a,
#2371 and #2300 remain OPEN.

## Ordinary JOIN with a live differential pin

Ordinary controlled JOIN previously returned ENOTSUP when a valid published
index pin prevented relocation of its registry. It now treats that cache as
unavailable before starting an image and evaluates through the existing charged,
governed private hash index. This is a runtime success-path change, not a new
raw fallback or a test-only qualification increment. Configured batch/strict,
started-image and conflicting-transaction protocols retain their own paths.

The read-only preflight keeps pending or unpinned-deferred ENOTSUP and malformed/duplicate/
name/generation/overflow errors as errors. Every registry/name/pairwise action
has its existing128 charge and a stop footer; it allocates no image and changes
no pin or transaction state. With N entries, M plan operations, J included JOIN
names and B name bytes including NUL, additional actions are N+1 normally, or
3N+M+J+B+N(N-1)/2+3 while pinned. This is extra bounded work at unchanged unit
prices, not an unchanged whole-call total or containing Source/carry certificate.

The five existing pin-held histories now require committed independent literal/
signed/Snapshot output with real private indexing and zero persistent image
activity, retaining exact pinned registry payload/location/source/generation
and writer exclusion. Deliberate release restores the registry and writer access;
future unpinned calls use normal persistent work. Two scalar observed private-
insertion CANCEL/WORK cases retain the external pin through internal disposal,
genuine reset and same-input retry. Internal malformed-state probes ensure an
earlier pin cannot mask later protocol errors. The public query changes from
ENOTSUP to success; no public API, quantum or price constant changes. Wider
policy/deferred/eviction and platform/Native/Source/carry obligations stay OPEN.

## Genuine deferred differential invalidation

A live pinned index can legitimately receive an invalidation request. Previously
that deferred flag still caused ordinary controlled JOIN to return ENOTSUP.
The same read-only availability preflight now admits a deferred entry only while
that entry is pinned, leaving its index and deferred reset untouched and using
the existing governed private JOIN. Pending transactions and unpinned deferred
flags remain errors; all original setup/metadata/overflow priority checks and
charged actions/footers remain intact. The action count and unit prices do not
change in this increment.

Five `evaluation_join_runtime_cache_deferred*` selections use the actual
invalidation API under two genuine pins: INT64/STRING/FLOAT keys through observed
STEP/plain STEP/Snapshot, plus actual private-insertion CANCEL and WORK. The
post-invalidation registry, payload, generation and exact external reader counts
survive success and unchanged repeat. First pin release preserves deferral;
last release resets exactly once, zeroes the index and restores writer access.
A LEFT-only change then requires a persistent rebuild against the unchanged RHS;
future mutation, deletion and stable-source reuse retain independent literal
content and delivery semantics. The deliberately private profile is separately
checked against the same logical oracle.

CANCEL disposes its internal owner. This particular acyclic STEP WORK instead
retains the authenticated EVALUATE parent, metadata credit/financier and one
additional EDB lease. The test requires that fixed PENDING disposition, frozen
report and owner across protected mutation/Snapshot refusals, then genuine reset/fresh retry
returns readers to the two external pins before deliberate release. This is not
a reader leak or a universal disposed-WORK claim. Existing fourteen malformed,
pending, unpinned-deferred and mixed-priority negative probes remain unchanged.

The ordinary/cache local completion inventory is tracked in
[ORDINARY_JOIN_CACHE_ACCEPTANCE.md](ORDINARY_JOIN_CACHE_ACCEPTANCE.md). Other
ordinary items remain pending until their integration units finish; no global
#2371/#2300 or containing lifecycle/accounting/platform closure is claimed here.

## Typed canonical RHS index reuse

Controlled ordinary JOIN now permits canonical scalar RHS INT64, FLOAT and
STRING columns in its existing charged eligibility scan. A STRING/FLOAT payload
lane no longer forces private indexing either. Other enum values and the
existing noncanonical, compound, shared, timestamp, filtered, recursive,
delta, batch and spill exclusions retain their prior destinations. The
per-column charging and existing surrounding stop checks remain unchanged;
no hash namespace or installed API changes.

The natural two-head fixture in `test_evaluation_join_runtime_cache.c` uses
real intern-backed strings and finite FLOAT keys, including public +0/-0
canonicalization, negative and positive keys. Before the change the supported
query produced correct results but indexed six private RHS rows per stable
source call. It now builds one persistent index, reuses it for the second head,
and reuses it for both heads after a LEFT-only append with zero RHS insertion.
RHS append/removal rebuild against the current canonical snapshot; subsequent
LEFT changes reuse the initialized index. The independent numeric-ID oracle
checks signed callbacks, plain STEP, Snapshot, mutations and unchanged repeats
in both default and deliberately diff-disabled whole-evaluation profiles.

Nine healthy selections cover three key/payload domains and three public
routes. Three internal interoperability selections seed an index through the
actual raw differential JOIN (not the ordinary-arrangement hash namespace),
then require the controlled public call to reuse that exact index without
rebuilding. One FLOAT CANCEL at actual persistent insertion, one real NEXT
allocation ENOMEM and one FLOAT live-pin history verify finite recovery and
composition with bounded private fallback. Stops preserve pre-call public
heads, inputs, registry object/payload/metadata and denial/report semantics,
then reset and retry the same pending facts with a fresh attempt. Public NaN
and infinity rejection is checked without changing an existing warm cache.

These15 new selections accompany the two runtime changes, not a standalone
qualification increment. The146 existing cache selections remain registered.
Broader typed/predicate/representation/policy and interruption products,
containing Native/Source/carry, deferred eviction and current MSVC/TSan runtime
qualification remain open. This does not close F05a, #2371 or #2300.

## Reused evidence and remaining acceptance

| Obligation | Existing evidence reused | Still outstanding for complete #2371 acceptance |
|---|---|---|
| Ordinary key/product/projection/collision and accepted RHS expressions | `evaluation_join`, generated large/filter/collision cases in `evaluation_serial`, scalar recovery and the new public histories | Complete accepted expression/type/error/representation crossproducts beyond the specified scalar and INLINE predicates |
| Full/delta/empty selectors and signed future changes | Existing direct selector fixtures, serial append dispatches and literal public histories;42 cache histories bind actual canonical RHS and default scalar-observed append descriptors | Remaining selector products beyond the new canonical present-empty FULL/nonempty append cycle; historical phase7-only counters remain insufficient |
| Persistent full unfiltered index and filtered/private fallback | `test_controlled_persistent_join`, persistent stop/refusal matrices in `test_evaluation_serial.c`;42 natural cache/source histories across scalar/INLINE LEFT and three public routes, actual key0/key1/composite reuse/rebuild, private filtered/delta work and exact rollback/retry;32 further cap/resource/pin histories and30 empty/rejected/refill histories described above;40 further allocator-boundary histories qualify image/slots, first/second copied metadata and key0 replacement prefixes with fixed disposed ENOMEM and fresh retry/future reuse | Remaining new-entry/all-width allocator, predicate/representation/policy/pin/selector intersections; same-input default/private whole-evaluation comparison is not an isolated cache-only or resource certificate |
| Pins and deferred rebuild/eviction | `join_right_filter_cleanup` managed cache/pinned fallback/admission tests; existing strict internal-cache fixtures; five genuine differential live-pin private-success/release/future-reuse histories plus two stopped private-insertion retries | Remaining stale/deferred/eviction and other consuming intersections; new pin setup is internal, not a public primary-cache producer |
| Materialized policy/memory and future calls | All78 `evaluation_materialized_body_*` selections:12 original scalar and66 typed INLINE-EDB preserved-body histories; host strict/policy/lifecycle suites | Remaining empty/wide/metadata/selector/cache-equivalence and containing bounds, not a primitive controlled-cache-hit claim |
| Batch/pipeline, recursive/frontier and representations | Existing JOIN host suites and natural recursive materialized histories | Each accepted operator-by-representation and RUNNING/FINISH intersection, including required signed timestamps/weights; new callback sign checks are not full timestamp qualification |
| Ordinary INLINE representation | Direct public fused-projection regression;363 generated histories including streamed producer/consumer/ACK,90 controlled memory failures and54 matching-cap histories;141 finite policy cases and constructed admission controls; new persistent-index histories consume typed INLINE LEFT with canonical scalar INT64 RHS | Remaining memory/type/policy/selector/cache/materialized/metadata and other operator representation intersections |
| Reached prepared ownership and disposal | Merged #2364, existing recovery/lifecycle fixtures | Remaining #2367/#2368 local funding/caller/ACK and MSVC-C11 layout qualification on the actual routes |
| RUNNING/FINISH and containing accounting | Existing components and local interval notes | Reached #2369 adapters and #2370 full entry/return, Original/SOURCE identity and correlated Native/Source carry certificates |

#2299 expression machinery is reused. #2301 owns representation access, while
this issue still owns the consuming JOIN intersections it enables. These are
precise handoffs, not a requirement to wait for unrelated sibling domains.
Required unavailable platform/Source evidence remains outstanding. There is no
new raw fallback, supported-domain refusal, language/API semantics, quantum
increase, cap increase or replacement index project in this bundle.

## Reproduction and validation boundary

Use a uv virtual environment as required by AGENTS.md. For an environment with
Meson/Ninja already available to that environment:

```sh
uv venv .venv
. .venv/bin/activate
CC=clang python -m mesonbuild.mesonmain setup build -DmbedTLS=disabled -Db_lto=false
python -m mesonbuild.mesonmain compile -C build
python -m mesonbuild.mesonmain test -C build --no-rebuild --print-errorlogs \
  --suite evaluation_join_hosts
CC=clang python -m mesonbuild.mesonmain setup build-asan -DmbedTLS=disabled \
  -Dexamples=false -Db_lto=false -Db_sanitize=address,undefined
python -m mesonbuild.mesonmain compile -C build-asan \
  test_evaluation_join_inline test_evaluation_join_inline_policy \
  test_evaluation_join test_evaluation_compound_sources \
  test_evaluation_join_domains test_evaluation_materialized_body \
  test_relation_scalar_source_end test_evaluation_facts_sources \
  test_evaluation_empty_edb_sources
python -m mesonbuild.mesonmain test -C build-asan --no-rebuild --print-errorlogs \
  'evaluation_join_inline_*' evaluation_join evaluation_compound_sources \
  'evaluation_join_domains_*' 'evaluation_materialized_body_*' \
  relation_scalar_source_end evaluation_facts_sources evaluation_empty_edb_sources
```

The preceding #2377 snapshot had Clang23 JOIN-host101/101, ASan/UBSan16/16 and
GCC16 focused15/15. The INLINE increment (native LTO disabled) has Clang23 JOIN-host104/104 plus5
facts/compound/empty/scalar-END/serial tests, ASan/UBSan22/22 and GCC16 focused19/19,
plus formatting/diff checks.
These are the completed focused results before the documentation-only ledger
commit. The final PR body records the exact published HEAD and final broader
run; do not infer a final-SHA full-suite result from an earlier in-flight run.
MSVC and its conditional C11 actor profile have not been run in this Linux
workspace. Passing Linux tests do not close those acceptance items or #2371.

For the configured, streamed, memory-pressure and matching-cap increments,
after the same uv activation and Meson availability, the sanitizer setup above
can be followed by:

```sh
python -m mesonbuild.mesonmain compile -C build-asan \
  test_evaluation_join_inline test_evaluation_join_inline_policy
python -m mesonbuild.mesonmain test -C build-asan --no-rebuild --print-errorlogs \
  'evaluation_join_inline_*'
CC=gcc python -m mesonbuild.mesonmain setup build-gcc -DmbedTLS=disabled \
  -Dexamples=false -Db_lto=false
python -m mesonbuild.mesonmain compile -C build-gcc \
  test_evaluation_join_inline test_evaluation_join_inline_policy
python -m mesonbuild.mesonmain test -C build-gcc --no-rebuild --print-errorlogs \
  'evaluation_join_inline_*'
```

The wildcard now selects61 tests: the policy executable, six ordinary/configured
registrations, and18 each for streamed, pressure and matching-cap histories.
It selected seven at the historical #2380 checkpoint and25 after #2384.
Both executables must be compiled before using `--no-rebuild`.

For the preserved K3 INLINE-EDB increment, after the same setup and activation:

```sh
python -m mesonbuild.mesonmain compile -C build-asan test_evaluation_materialized_body
python -m mesonbuild.mesonmain test -C build-asan --no-rebuild --print-errorlogs \
  'evaluation_materialized_body_*'
python -m mesonbuild.mesonmain compile -C build-gcc test_evaluation_materialized_body
python -m mesonbuild.mesonmain test -C build-gcc --no-rebuild --print-errorlogs \
  'evaluation_materialized_body_*'
```

This wildcard selects78 tests:12 original scalar modes and66 INLINE-EDB
histories (18 healthy,18 policy,18 successor stops and12 memory failures).
The integrated Clang23 JOIN-host result is228/228, related components5/5,
GCC16 focused78/78 and ASan/UBSan78/78. These code/test results precede the
documentation-only ledger commit; no additional platform certificate is implied.

For the persistent-index/source histories, use the same uv activation and
native/GCC/ASan setups above, then compile the focused executable before each
`--no-rebuild` selection:

```sh
python -m mesonbuild.mesonmain compile -C build test_evaluation_join_cache
python -m mesonbuild.mesonmain test -C build --no-rebuild --print-errorlogs \
  'evaluation_join_cache_*'
python -m mesonbuild.mesonmain compile -C build-gcc test_evaluation_join_cache
python -m mesonbuild.mesonmain test -C build-gcc --no-rebuild --print-errorlogs \
  'evaluation_join_cache_*'
python -m mesonbuild.mesonmain compile -C build-asan test_evaluation_join_cache
python -m mesonbuild.mesonmain test -C build-asan --no-rebuild --print-errorlogs \
  'evaluation_join_cache_*'
```

The wildcard selected42 cases at the previous checkpoint: six healthy and36
recovery histories. On that integrated code/test tree, Clang23 JOIN hosts270/270, six related checks
(including serial persistent/refusal/recovery and the threading audit/selftest),
GCC16 cache cases42/42 and ASan/UBSan42/42 passed. Formatting/whitespace/diff
checks passed. Ordinary INLINE61 and materialized-body78 selections remain
unchanged. These runtime results precede the documentation-only ledger commit;
no additional MSVC/TSan runtime, platform or Source/carry certificate follows.

The previous wildcard checkpoint selected74 cases: those42 plus12 cold-cap,15
resource and five pin histories. That executable tree passed Clang JOIN hosts302/302,
related serial/arrangement/cleanup checks5/5, and GCC/ASan-UBSan cache cases74/74
each. The ordinary INLINE61 and materialized-body78 selections remain unchanged.

The preceding wildcard checkpoint selected104 cases: the previous74 plus six
healthy empty/refill cycles,16 no-output stops and eight actual key0 refill stops. Reuse the same
compile-before-no-rebuild commands above. Clang JOIN hosts332/332 and related
checks7/7, GCC104/104 and ASan/UBSan104/104 passed on the executable tree before
the ledger-only commit; no hosted/final-platform or containing Source proof follows.

The previous wildcard checkpoint selected144 cases: those104 plus eight image/slot,16 copied-
metadata and16 replacement-build allocation-failure histories. Reuse the same
compile-before-no-rebuild commands above. Clang JOIN hosts372/372, affected
serial/source/arrangement/cache/pin/drain checks20/20 and ABI/header/threading
contracts5/5, GCC144/144 and ASan/UBSan144/144 passed on the final executable
tree before the ledger-only commit. The normal production library built without
the new callback/helper symbols. Hosted published-HEAD/MSVC and containing
Native/Source/carry qualification remain outstanding.

The current cache wildcard selects146 cases: the five pin-held cases now require
success through bounded private indexing and two new pin-held insertion stops
verify CANCEL/WORK recovery. Other139 original modes retain their assertions.
