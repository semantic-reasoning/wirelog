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

## Reused evidence and remaining acceptance

| Obligation | Existing evidence reused | Still outstanding for complete #2371 acceptance |
|---|---|---|
| Ordinary key/product/projection/collision and accepted RHS expressions | `evaluation_join`, generated large/filter/collision cases in `evaluation_serial`, scalar recovery and the new public histories | Complete accepted expression/type/error/representation crossproducts beyond the specified scalar and INLINE predicates |
| Full/delta/empty selectors and signed future changes | Existing direct selector fixtures, serial append dispatches and the new literal public histories | Bind each newly enabled route to actual source identity and all required selector partitions; phase7 alone is insufficient |
| Persistent full unfiltered index and filtered/private fallback | `test_controlled_persistent_join`, persistent stop/refusal matrices in `test_evaluation_serial.c`; new literal public build/private witnesses | Complete consuming predicate/representation/policy and same-input cache equivalence; new tests do not claim reuse from output equality |
| Pins and deferred rebuild/eviction | `join_right_filter_cleanup` managed cache/pinned fallback/admission tests; existing strict internal-cache fixtures | Missing authentic consuming intersections, with internal pin setup labelled as such; no new public primary-cache producer is invented |
| Materialized policy/memory and future calls | All78 `evaluation_materialized_body_*` selections:12 original scalar and66 typed INLINE-EDB preserved-body histories; host strict/policy/lifecycle suites | Remaining empty/wide/metadata/selector/cache-equivalence and containing bounds, not a primitive controlled-cache-hit claim |
| Batch/pipeline, recursive/frontier and representations | Existing JOIN host suites and natural recursive materialized histories | Each accepted operator-by-representation and RUNNING/FINISH intersection, including required signed timestamps/weights; new callback sign checks are not full timestamp qualification |
| Ordinary INLINE representation | Direct public fused-projection regression;363 generated histories including streamed producer/consumer/ACK,90 controlled memory failures and54 matching-cap histories;141 finite policy cases and constructed admission controls | Remaining memory/type/policy/selector/cache/materialized/metadata and other operator representation intersections |
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
