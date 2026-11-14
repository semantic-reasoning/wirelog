# Operator domain accounting (#2300)

The #2300 target covers operator dispatch and algorithm domains
O04/O05/O06/O09, F05/F05a. The first atomic foundation below is an internal component. Its
production integration and the remaining operator domains are still open;
this document does not assert that #2300 can close yet.

## Nonstream private batch retirement component

`wl_columnar_filter_batch_end_step_owned_funded` performs one retirement
action at an authentic numeric-owned FILTER/MAP `BATCH` or ordinary
JOIN/probe/REDUCE `JOIN_BATCH` context. `filter_context_nonstream_owned` proves the
actual frame and numeric owner and excludes stream hosts. Context validation
proves current session, thread, lifecycle and absence of reentry. The selected
stage additionally requires the live pin and exact session, Original control
and saved attempt tuple. Other stages return `EBUSY`; a mismatched tuple or
drain is a protocol error. No financial check authorizes resource access.

The unchanged general `private_batch_drain_step_funded` proves the real
batch, nonce and writer before any release. It returns a nonzero protocol
separately from its `rc == 0` physical `release_error`. The wrapper preserves
that distinction, clears busy before its final real check, and advances the
context only after observing actual completion. A footer refusal may follow
real release progress; the next call observes that progress rather than
repeating release or acquired-count changes. Execution and semantic results
remain the initiating results. Only a successful footer publishes status.

The component accepts ended Original generations and does not charge or alter
their active successors. It has no numeric PENDING/consume behavior and makes
no claim about framed/pipeline integration. Existing production callers,
legacy step and ACK bodies, native latch and relation helper are unchanged.

### Financial contract

The **caller** supplies a distinct real controller with a fresh active saved
generation, assigns no incoming SOURCE promise to it, invokes this component
once, and genuinely finishes/reports/releases that generation. A retry uses
a new generation. Freshness and exclusive use are internal caller
preconditions; this helper neither verifies them with budget peeks nor creates
a permission flag. A production driver must separately qualify its handoff
and all simultaneously retained obligations before using this component.

There is one positive admission: the existing named
`WL_COLUMNAR_FILTER_ATTEMPT_BATCH_RETIRE_WORK`, **192**, at entry. Every
subsequent check uses the saved financier generation and price zero. Limit191
refuses without consuming work; exact192 admits the action. A post-admission
invalid proof, cancellation or native retention may leave all192 unused.
With incoming assigned promise0 and performed lower0, local outstanding is
therefore at most192, below quantum256. No prior Original reservation is
reissued, borrowed, refunded or counted as new payment here.

This argument is about logical admission. Scalar SOURCE distance is a separate
metric: an unchanged native authority interval can have upper224 even though
the logical action costs192. Likewise, HOST384 in legacy composition is BODY
callback financing, not automatically a newly issued SOURCE tariff. The
lexical SOURCE record is finished by `eval_serial_source_body` before BODY.
This component certificate does not inherit or certify a legacy HOST ledger.

### Expanded scalar source intervals

Count evaluated predicates, scalar stores, arithmetic/address fronts, actual
arguments/calls/results/returns and native first fronts. Controller/native
interiors retain only their existing narrow qualification. No engine grid
scan, allocator wrapper loop or generic destructor is excluded as a library
interior. The direct component has no pipeline, framed gateway or LATE
ancestor; later integration must count its actual ancestors.

| Complete selected interval | Upper | Basis |
|---|---:|---|
| Direct entry through192 admission/refusal and return | 32 | Record local, parameter/separate-controller leaves, real charge arguments and result/return |
| Successful entry192 through nonstream getter/context to real0, including invalid returns | 128 | Getter48 + nonstream wrapper10 + caller6 + context32 + caller5 + local1 + zero front5 =107, reserve21 |
| Postproof0 through pin/tuple/stage/drain guards to relation entry0 | 48 | Fixed selected predicates, busy/done stores and six real callee arguments/front; invalid exits included |
| Relation outer0 through valid authority to its next real0 | 224 | Unchanged structural36 + writer validators82 + inert/init/return12 + outer authority22 + root/call/check40 =192, reserve32 |
| Invalid relation proof through busy clear and wrapper footer0 | 208 | Prior complete254 minus old LFTJ ancestor68 + new return/busy/footer12 =198, reserve10 |
| Native release plus acquired-count/phase through wrapper footer0 | 128 | Prior176 minus old ancestor68 + new footer12 =120, reserve8 |
| CLEAR/FINAL through wrapper footer0 | 144 | Prior192 minus ancestor68 + footer12 =136, reserve8 |
| NEW postauthority0 through binding/result/phase publication and footer0 | 64 | At most12 relevant selectors/stores plus actual call/result/return and footer fronts |
| Wrapper final0 through status/error path and direct return | 32 | Longest YIELD suffix19, including five status fields and call/return; reserve13 |

The relation body matches base blob
`69481f4b3dea3cdfa3f79b763255f2f1f6398786`. Reused local expansions are the
funded drain rows in [the LFTJ accounting map](../2298/LFTJ_SOURCE_ACCOUNTING.md).
Old LFTJ ancestors are explicitly removed; their ownership or caller proof
is not reused. Physical release and acquired-count bookkeeping remain one
uninterrupted native action. No price is substituted for a source upper.

### Evidence and integration limits

`evaluation_numeric_plan` registers `test_owned_batch_retirement_primitive`.
It uses compiler-lowered MAP/FILTER/JOIN/ANTI/COUNT operations and authentic
initializers and ownership, visits NEW/OWNER/DESCRIPTOR/CLEAR/FINAL, and uses
one actual new financier per invocation. It asserts literal typed output and
cloned raw parity,191/192, stopped/stale financiers, wrong owner/frame/thread/
session/control/generation/nonce, physical retention, successful and failed
release followed by footer cancellation, ended Original and actual successor
with unchanged seven-field frozen report and zero successor consumption.
Peer37 governor credit remains through cleanup and returns to zero after its
own rollback. A genuine early-cancelled MAP proves the no-batch transition
preserves its initiating error. The existing generated composed JOIN pipeline
also attempts this helper during actual consumer dispatch and verifies its
nonstream refusal before continuing to the original raw-parity result.
These are component assertions, not public production wiring.

The next foundation must own generic numeric ROW capture/growth and displaced
or partial storage, owned output/input FINISH retirement, scalar source END,
registered native/latch/ACK callers and stream state/close tails. Broad
prototype A1/A2 has no approval for activation. U1 SOURCE family admission
remains blocked until those real paths and their full caller/refusal/carry
certificates are complete. The final closing PR must contain a qualified
production consumer of this primitive and complete the remaining #2300 work.

## Owned private-row component

`wl_columnar_filter_row_component_append_owned` appends to a genuine quiet
numeric-owned `BATCH` or `JOIN_BATCH` context. The external, caller-owned
`wl_columnar_relation_row_debt_t` stays at its final address from header-only
initialization through terminal unbind. It owns the actual capture, ROW,
prepared governor/ledger, refund and downsize records; none is copied after
acquisition. The cleanup frame holds one nonowning anchor and the context
keeps its real busy borrow until the receipt reaches `DONE`. Ordinary STEP,
batch END, ACK and frame destruction cannot consume that borrow.

This component does not change production ROW routing. Production operators
append while their initializers are `RUNNING`; this API accepts their later
quiet batch stage. Its supplied test row is explicitly a component input,
not a claim about a natural program result. Production wiring, owned FINISH,
stream/native callers and their complete financial ledger remain separate
activation requirements.

The return integer reports protocol/authority/funding refusal. The five-field
status separately reports the initiating execution error, physical cleanup
error, whether the row was published, pending cleanup and completion. Before
binding, refusal preserves the receipt and caller status. After binding,
semantic failure records its first cause and retains exact ownership; it
never returns a successful pending row that an outer loop could replay.
Success requires synchronous retirement and unbind. A stopped financier
leaves status unchanged even if physical progress preceded the footer;
`status_owned` observes that canonical progress. Terminal query accepts NULL
frame/owner after they have been freed, and reads only the receipt header.

Before publication, staged arrays belong to the ROW record. A bounded handoff
records their initialized extent before any returning check. After the
private cutoff, the new row/image/generations remain installed and displaced
old arrays belong to the receipt. The existing prepared governor finish,
relation/lease stores and ledger apply/release stay contiguous. No shared
ledger gate remains held across a check. Retirement frees one recorded lane,
then its pointer table, then timestamps; typed refund/downsize and capture
buffer/credit/reference release follow in separate actions. Failed native
CAS preserves its actual receipt and reports `EAGAIN`; a retry gets new
funding, without repeating an already completed free or refund.

The selected ROW-record tail and complete ROW/capture handoffs end at real
saved Original checks. This includes early capture/renewal errors: cleanup
with no ROW record uses the capture-only proof, accepts authentic
`HELD`/`DRAINING`/`ENDED` batches, and does not demand released writers be
reacquired. ROW-resource cleanup instead validates both held writers and
their exact saved nonce. Capture credit distinguishes actual RESERVED
owner-zero, COMMITTED self-owner and the recorded origin of RELEASING; it
binds the actual held governor reference before releasing credit.

### Row source intervals

The same scalar metric and narrow native/allocator boundaries above apply.
Each row includes its actual argument, callback-dispatch and returned error
front. Geometry uses genuine checks between fixed groups of at most two
range comparisons. Full native batch authority has its own initial/footer
checks, separate from context and geometry proof.

| Complete selected interval | Upper | Basis |
|---|---:|---|
| Fresh header-only setup / DONE reset | 64 / 128 | 34 meaningful header stores, portable thread front; reset additionally proves inert unbound state |
| Forward bootstrap through first actual Original check, including invalid return | 160 | Existing context/getter107 + explicit tuple/pin/batch/semantic32 + caller16 + reserve5 |
| Dedicated cleanup bootstrap through distinct-financier check/admission | 192 | Fixed header48 + actual anchored context128 + exclusion/check/return16 |
| Full mutation tuple through actual check/refusal | 96 | Self/thread24 + 11 field leaves + nested fields-call8 + caller24, reserve29 |
| Geometry setup / one span / two-pair group | 96 / 64 / 96 | Fixed setup; one overflow span; at most two subtraction-based range helpers |
| Last geometry check through header and bind's internal check | 160 | Return12 + fixed header48 + frame/context/target/nonce64 + portable thread/check24 + reserve12 |
| Bind check through finite anchor/busy publication and ROW128 refusal | 96 | Owner publication24 + returned call/check32 + refusal/status32 + reserve8 |
| Full batch/native authority through common footer | 224 | Existing structural/native192 + strict held/count8 + financial/refusal16 + reserve8 |
| ROW target/extent after authority / capture-only lineage | 64 / 96 | No width scan; capture-only forbids image/ROW/refund/downsize debt |
| One lane/table/timestamp/capture-buffer action through footer | 128 | Dispatch24 + single free/clear/cursor20 + returned bookkeeping/observer/footer48 + reserve36 |
| Prepared ledger / governor cancellation through footer | 128 / 128 | Exact final-address owner/Original tuple before native precheck; no retained HELD gate |
| Refund/downsize inert setup through native precheck | 128 | Eight meaningful inert fields plus exact record/target proof, no structure-sized clear |
| One refund/downsize begin or advance through footer | 224 | Native128 + typed result/readiness32 + complete returned wrapper48 + reserve16 |
| Capture credit role proof / one finish step through footer | 128 / 224 | RESERVED/COMMITTED/RELEASING owner proof precedes separate native check |
| Capture reference release through footer | 128 | First ref/native/free front46 + clear/progress12 + caller48 + reserve22 |
| Final check through complete unbind and returned status | 128 | Fixed owner checks24 + cause/anchor/pointer/busy handoff24 + status/return32 + reserve48 |
| DONE query / active query without a poll | 72 / 248 | Active: header53 + actual live112 + fields-only tuple23 + output geometry43 + status13 + reserve4 |
| Selected RECORD first admission / initialized record tail | 96 / 128 | Existing first80 plus pointer selection/caller; meaningful records39 and safe selected error handoff |
| SHAPE / one scalar cell / operator byte chunk | 160 / 96 / 192 | Actual old packets plus selected error handoff; operator copy stays eight bytes per183, legacy generic stays sixteen |
| Ordinary SIZE/credit/allocation/recipe/validate packet | 192 | Largest151 + selected handoff24 + caller16, reserve1; nested preparation remains separate |
| Governed ledger cutoff through completed publication and ROW footer | 252 | Old complete225 + debt stores7 + routing3 + result/phase3 + real cut8 + caller reserve6 |
| Prepublication initialized-record error handoff through footer | 128 | First native/restore64 + mandatory gate alternative40 + fixed owner/phase/check24 |
| Selected append footer through next live-context check | 160 | Cause/return16 + observer7 + actual live112 + returned refusal24 + reserve1 |
| Capture initialized header / wide ref-token setup | 96 / 128 | Readiness after meaningful header; native ref first front and complete token setup before check |
| Last capture cell/error return through selected append footer | 128 | Cell36 + retained dispatch8 + safe cause/phase/footer24 + own restore48 + reserve12 |
| Actual batch check through selected capture admission / renewal packet | 240 / 240 | Old postbatch223 + current call/progress16; unchanged renewal packets plus returned phase/footer16 |

The zero-final-payload branch uses the existing seven-field meaningful inert
reservation initializer only after actual native release completion. It has
no `sizeof(token)` clear and stays inside the native action224 envelope.
Capture's source reserve stages have independent uppers112/64/144/64 and
commit80/64/128, distinct from their logical prices100/124/164/78 and98/81/63.
The selected caller adds at most48; the own-credit restore/error/footer
contour is bounded224. No unrelated reservation or general engine retry loop
is hidden in the inherited native own-credit restoration boundary.

### Row admission and inherited obligations

The selected192 and ROW128 admissions replace the original numeric prefix.
Before128, distinct already performed leaves total64: structural20, two real
writer validations10, outer authority11, context8, frame/record9 and six
bootstrap pin/session/control/id/output/private-output leaves. Thus local
incoming assigned promise0 gives192+128-64 <=256. Geometry, thread fronts,
binding stores and future native success are not borrowed as this lower.

Normal retirement adds one named logical action price1. Each iteration first
records a new selected phase/cursor and intent; the action verifies that
snapshot. At least one distinct meaningful store has already performed before
each new1, including native retries, so its added charge minus new lower is
at most zero. Earlier intent does not fund a later retry. This does not pay
native source224 or erase old obligations: governor finish44, ledger
initial26/apply117/release23, ROW terminal48 and each actually compatible
prior unfinished region retain their own identities. Their real terminal or
cancellation work executes; zero checks alone do not settle them.

One cleanup invocation uses a fresh distinct dedicated financier generation,
incoming assigned promise0, one price192, conservative lower0 and unused at
most192. It never polls Original, including an ended generation or its
genuine successor. Exclusion uses actual frame-owned context before calling
any API on the financier; an untrusted capsule control cannot select it.
Full production parent/stream/HOST correlated carry remains unqualified by
this component contract.

### Shared callers and component evidence

Legacy capture now has a small adapter to the shared algorithm. Its
first-admission contour is255: old248 plus at most seven argument/call/return
cells. The ready predicate occurs after that check and the retained-error
branch at its own exit. The historically unqualified generic terminal
capture255 contour remains explicitly unqualified; this first-admission
proof does not certify its old synchronous disposer. Selected retained mode
avoids that disposer and has its own genuine footer.

The shared ROW selector adds at most four entry cells before RECORD and one
false debt leaf after record initialization for legacy/LFTJ callers. The
legacy growth cutoff upper becomes228 from225; its old mandatory retirement
boundary remains. Owned LFTJ nongrowth SHAPE128/CELL64 stay covered; its
inert terminal128 gains one false leaf (129). The scalar initializer BIND254
and numeric PREFIX249 bodies and record sizes remain unchanged. The cleanup
frame gains only one pointer; its dynamic accounted constructor may execute
another already bounded sixteen-byte zero iteration. Direct continuation,
dispose, free and free-target paths add one anchor guard before consuming an
entry. Generic legacy teardown needs its own later caller qualification.

The existing registered `evaluation_numeric_plan` target contains the actual
component fixtures. Generated MAP/FILTER/JOIN/ANTI/COUNT contexts prove both
accepted stages. Tests cover widths1/16/17/257, actual64→65 and128→129 growth,
spare capacity, aliased input capture, FLOAT negative zero/invalid NaN,
timestamped FILTER with signed old metadata and default appended metadata,
capture/column/timestamp allocation failure and enforcing-cap provenance.
Independent literal content checks accompany genuine READY governor/ledger
cancellation and postpublication cleanup. Original cancellation, initialized
record-tail refusal, renewal ENDED gap and partial-release DRAINING retain
exact ownership. Actual precommit RESERVED capture proceeds through
RELEASING under fresh cleanup grants. A peer reservation forces real refund
and downsize credit CAS loss; peer37 and ledger peers11/13 detect duplicate
credit/free accounting.

The retire1 oracle separately labels the observed setup allowance: allowance0
cannot free the first displaced lane, allowance1 frees exactly that lane,
and the next admission refuses. The action tariff is independently1; no
total-budget search supplies its proof. Dedicated cleanup191/192, stale and
cancelled generations, real progress followed by footer cancellation,
copied receipts, thread/session/control/status-alias refusal, reentry and
legacy STEP/END/ACK refusal are asserted. Ended Original reports retain all
seven fields and genuine successors spend zero. DONE queries work after
frame/control destruction before a fresh header-only reset.


## U0b.2: externally owned FINISH retirement component

The generic operator FINISH component reuses the existing finite scalar
retirement machine. It admits actual FILTER, MAP, JOIN, SEMIJOIN, ANTIJOIN,
REDUCE and CONSOLIDATE contexts only after CONTROL has released the output
writer/pin and reached quiet nonstream FINISH. It does not activate a new
production route. An external direct or composed numeric owner, including an
actual outstanding CHILD result, remains frozen under the prepared marker.
The embedded framed child is excluded by its actual address after canonical
owner proof and before any financier API. This excludes ordinary framed and
registered native/error parents; it does not rely on a native-cleanup flag.
Their existing production behavior is unchanged. Coherent production ROW,
stream, framed/native and SOURCE integration remains a later required unit.

The caller supplies fresh zero or genuinely ACKED final-address prepared
storage, distinct from frame, owner and selected descriptors. The constructor
never reads a dormant owner-thread payload. Fixed bounded proof packets check
actual operation, context, range, scalar state and scratch ownership; captured
plan/cursor/phase and actual context/control lineage are rechecked before
retaining Original and publishing the marker last. Original is retained for
identity/lifetime, never polled during cleanup. An ended Original and an
active successor therefore authorize neither work nor a debit here.

HEAD distinguishes committed replacement, uncommitted provisional output,
present-empty ANTI metadata-only keep-all, and an inert selected owner.
Missing right is MISSING_EXACT: it preserves the exact original entry without
a replacement commit. ANTI keep-all retains the live relation and exact owned/borrowed bit,
clears live delta/segment metadata and retires only the actual historical
segment array. HEAD never selects that live relation for destruction. The
segment action is irreversible; decline cannot resurrect a freed array.
Before scalar commit, decline performs real partial initializer reset and
claim release, restores only the legal retained entry, then clears and ACKs.
After irreversible scalar commit, work must drain through the finite machine.
There is no generic destructor or unlimited fallback.

### Source contours and financier schedules

The emitted-source support counts actual helper/caller paths separately from
logical admission totals. All new financiers have incoming assigned promise0.
The constructor has one positive256 admission with conservative lower0;
HEAD has one positive192 admission with lower0. Each proof packet executes a
real zero check. A zero check does not discharge an existing promise and a
phase with several positive admissions does not claim a whole-call256 source
loan. HOST384 is not imported as a SOURCE tariff. Correlated production carry
and containing serial/native caller bounds must be qualified at integration.

| Actual contour | Conservative source upper |
| --- | ---: |
| context / operator state / completed-resource state | 48 / 80 / 64 |
| each scratch group / terminal-inert receipt / prepared+funding proof | 32 / 128 / 96 |
| begin initial proof / geometry / operator-state packet | 160 / 112 / 112 |
| begin completed-resource / scratch / descriptor-range packet | 112 / 80 / 80 |
| begin inert receipt / final lineage / positive binding packet | 176 / 144 / 160 |
| generic step or decline initial proof | 128 |
| HEAD observer and renewed proof / context and state | 144 / 192 |
| HEAD completed-resource or scratch / disposition / fixed handoff | 112 / 112 / 96 |
| generic non-HEAD delegation prefix / decline prefix | 144 / 144 |
| segments / selection / commit / ordinary dispose | 82 / 95 / 190 / 161 |
| ledger wrapper / scalar clear / claim release / decline restore | 121 / 211 / 189 / 71 |
| work clear / ACK / error or footer | 99 / 125 / 48 |
| scalar INIT BOOT first prefix / post128 / ZERO / BIND / RESET | 176 / 144 / 208 / 224 / 192 |
| finite timestamp payload including the generic return | 192 |

The new generic non-HEAD dispatcher returns the existing helper directly:
there is exactly one extra returned cell after its actual final check. In
particular timestamp payload191 becomes192, rather than hiding a new result
or status-copy suffix. The initializer BIND224 is counted along the real
staged path, not formed by adding a wrapper to the separate legacy BIND254.
The old INERT authority, BIND254 and numeric PREFIX249 stay literal. Shared
read-only JOIN-state and scratch extraction adds six and seven cells to those
old helpers respectively: their uppers become89 and72.

| Actual phase | Logical admissions in order |
| --- | --- |
| begin / HEAD | 256 / 192 |
| segments / select | 128 / 128 |
| INIT BOOT / ZERO / BIND / RESET | 192+128 / 192+256 / 192+192 / 128+128 |
| PREPARE phase0 / phase1 or2 / ordinary token or pair | 256+1 / 192+1 / 128+1 |
| timestamp payload21 | 128+1+192 |
| COMMIT | 192 |
| ordinary DISPOSE / native credit | 256+1 / 256+64 |
| ledger DISPOSE12 with actual debt | 128+109+166+28 |
| scalar clear / work clear / ACK | 256 / 128 / 192 |
| decline / partial initializer reset if needed | 128 / 128 |
| decline claims / clear / restore / work clear | 256 / 256 / 128 / 128 |

The inherited native charge/lower identities remain distinct; in particular
BOOT's real overlap lower64 precedes its128. No earlier setup intent pays for
a future retry. Each exact/one-short phase oracle states cumulative logical
admissions independently of production fee selection. These tables qualify
the component and its real native subpaths; they do not certify the sum of
unused loans for a future containing serial SOURCE turn.

### Actual component evidence

`evaluation_numeric_plan` drives real generated MAP/FILTER/JOIN/ANTI/COUNT
operations after an actual preceding owned output. SEMI and CONSOLIDATE
fixtures are explicitly IR components, not claims of compiler selection.
Direct CHILD/PENDING and composed UNARY_CHILD/COMPOSE_PENDING witnesses retain
the old result tuple and completed host financier allocation/report through
component ACK, then consume the old tuple first. STEP, consume and ACK are
refused without mutation while the marker owns the frame. All seven fields
of ended Original reports remain equal; genuine successors consume zero.

The absent-right and present-empty ANTI fixtures separately cover actual
borrowed VARIABLE input and owned MAP output, with and without historical
segment/delta metadata, plus decline after irreversible segment disposal.
They preserve the input snapshot, exact ownership and literal values. The
borrowed keep-all regression fails on the old HEAD ownership predicate and
passes only when equality of owned bits permits the real false/false case.

Independent literal content, actual ownership and peer37/governor and11/13
ledger baselines cover successful and cancelled families. A real peer reserve1
between the governor total read and credit CAS forces native EAGAIN; a fresh
step settles it exactly once. Funding-generation turnover is injected at the
outer HEAD observer, each of four initializer APIs, ledger READY before CAS,
and governor credit before CAS. The old generation is rejected, successor
consumption remains zero, irreversible progress is retained and fresh grants
finish cleanup. Ledger READY refusal leaves its actual writer gate clear.

Explicit existing constant-true FILTER bytecode isolates actual retirement
for nullary,1,65,257 columns, empty/nonempty and mixed INT64/FLOAT/STRING/
UINT64/BOOL storage. Values use real interned symbols and typed constructors;
timestamps carry literal stratum/worker/iteration and signed multiplicity.
Owned input comes from executing the preceding FILTER. Every scalar-init
phase is declined through actual reset/claim release, then reentered and
fully drained. Critical timestamp payload321 logical admission is exercised.
These are component intersections; natural session/recursive coverage remains
required before issue closure.

`evaluation_serial` independently produces an ordinary framed quiet FINISH
and proves generic begin returns EBUSY without financier consumption or frame
mutation, then resumes the legitimate driver. The existing host lifecycle
`native-end` fixture proves actual session registration with its read-only
API for both ended Original and active successor. It rejects generic begin
on the real embedded child before funding, preserves full context/report,
then completes the existing native cleanup and verifies literal retry and
resource baselines. Neither fixture manufactures native permission flags.

## Numeric right-filtered membership probes

The controlled SEMIJOIN/ANTIJOIN route now evaluates a present right filter
before key, nullary, projection and output work. It reuses the ordinary JOIN
numeric selection and empty-filter helpers. Missing right still repushes the
original input before inspecting the expression. A failed numeric expression
rejects that row; only selected rows undergo whole-row FLOAT validation.
String-expression, materialized, differential, recursive and parallel probe
expansion is not established by this change.

The selection vector retains original row numbers in input order. Hash heads
and `next` contain compact selected ordinals; both hash construction and probe
comparison validate and translate an ordinal before reading the original
right relation. The hash allocation and all membership loops use selected
count. The output tuple remains in scratch slot 1, separate from the selection
vector. Sources and their snapshots remain unchanged. The prior ANTI keep-all
optimization still requires a physically empty right input; a filter-empty
right may use the ordinary value-output path.

### Observation intervals and logical work

Each selected-row translation adds one actual Original-control CELL128
admission, followed by the existing saved-generation tail observation. Build
uses checkpoint50 and probe uses51. These are Original WORK consumption:
`wl_columnar_join_attempt_charge` validates the saved generation and calls
`wl_evaluation_control_charge_attempt`. They create no Source BANK, lexical
lane, independent financier or assigned SOURCE promise. The neighboring
WORK192 and CELL128 prices therefore must not be added as though this change
issued a new SOURCE promise. They also provide no proof of a future containing
SOURCE carry bound. Existing private-finance activation requirements remain.

The local source census counts scalar stores, comparisons, address/arithmetic
fronts and actual argument/call/result/return fronts. Controller and unchanged
native/helper interiors retain only their existing qualification. These are
conservative observation upper bounds, not performed lower bounds, prices,
whole-route certification, or permission to widen an allocator boundary.

| Changed containing interval | Upper | Expansion |
|---|---:|---|
| Preselection WORK check through selected helper's first real charge | 96 | Result/refusal, selected count/flag, empty choice, arguments/front and helper charge wrapper |
| Last selection check through helper return and caller return check | 96 | Count/return or empty helper suffix, caller result and return-check arguments/arbitration |
| Caller return check through projection/governor/key setup to tail24 | 128 | Optional projection refusal, governor selection, fixed key locals and tail wrappers |
| Build row admission through mapping admission | 64 | Result/refusal, identity row initialization, filtered branch and charge arguments/wrapper |
| Mapping admission through checked load and tail50/51 | 96 | Indexed map load, signed/range guards, conversion/store and tail arguments/wrappers |
| tail50 through hashing's first real key charge | 96 | Return/refusal, hash local, actual helper arguments, fixed key setup and charge |
| tail51 through tail17 and next comparison admission | 96 | Return/refusal, row store, multiplied progress expression, tail and keyed dispatch |
| Mapping refusal/invalid result through status and numeric caller return | 224 | Fixed refusal front48 + existing status suffix64 + numeric caller73 + reserve39 |

No additional lower-bound credit or incoming promise is claimed. Complete
Source/carry and resource-bearing retirement qualification is still open;
this partial domain expansion does not close #2300 or activate the experimental
Root/BANK integration.

### Evidence

`evaluation_join` covers both probe kinds, keyed/unkeyed, widths0/1/17/65/257,
all/none/sparse selections, a noncontiguous high-row collision chain, rejected
duplicates, expression failure, missing/empty/nullary right, accepted/rejected
invalid FLOATs, and cancellation or genuine generation turnover at selection
and both mapping sites. It checks literal content and independent raw parity.
Materialized probe rejection remains covered.

`evaluation_join_probes_filtered-numeric` exercises public sessions using a
compiler-selected filtered ANTI plan and a separately identified explicit-IR
SEMI variant. It asserts actual emitted plan fields and selection/build/probe
witnesses. Its36 histories cross both kinds with ordinary/batched policy and
healthy, cancellation/WORK refusal at each of three phases, enforcing-cap
refusal and allocation failure. Failures expose no output head or delta event;
reset/raised-work retry delivers each expected row once. A subsequent empty
step preserves event count and ledger bytes. Actual destruction releases the
retained governor to zero after the sessions, plan and program are destroyed.
These fixtures qualify their stated ordinary intersections; they do not claim
all supported representation, deletion, materialized or recursive combinations.

## Full-issue continuation: scalar right predicates

The complete required outcome and unresolved rows are tracked in
[CLOSURE_MATRIX.md](CLOSURE_MATRIX.md). Numeric filtered probes are an initial
unit of that work, not an issue-closing delivery.

Present nonempty right selection now retains the numeric compiled owner when
supported and otherwise compiles through the current context-bearing scalar
owner. Both JOIN and probe initializers explicitly clear both program slots;
the common PROGRAM retirement frees the actual numeric/scalar owner before
physical selection storage and its credit/reference tails. The scalar call
uses the real session intern, extension snapshot, configured/active worker
counts, parallel status and callback session key, with predicate rather than
MAP extension-result permission. No raw expression fallback is introduced.
The empty-right helper retains its existing mandatory-setup/no-evaluation
contract; missing right still bypasses expression processing.

The additional scalar compile is bracketed by actual saved-Original charges
and return checks. Row evaluation enters the existing bounded scalar evaluator
with the authenticated source row and complete context after the existing
WORK admission. Semantic failure remains FILTER false; allocation, admission,
stop and protocol errors retain their own channels. Original prices and these
local observations do not close the containing Source/carry row.

The scalar public fixture repeats the36 numeric-policy/fault histories with
an actual compiler-selected string literal predicate. Its phase44 witness
reads the authentic framed child and confirms a live scalar owner with no
numeric owner on every evaluated right row. Explicit component JOIN/SEMI/ANTI
predicates additionally run a long-string CONTAINS expression with literal
content and raw parity, then cancel or create a genuine successor after
program creation. The parser does not accept a computed function directly in
a negated atom argument; that invalid syntax is not presented as a supported
natural route. Accepted scalar function bytecode is exercised separately.
No materialized, recursive, representation or financial row is closed merely
by this predicate implementation; those remain explicit in the full matrix.

## Full-issue continuation: scalar and STRING REDUCE

REDUCE retains numeric compilation first and falls back to the current scalar
owner for accepted computed operands. The real session expression context and
common PROGRAM lifetime are the same context-bearing contracts used by the
other scalar consumers. Operand semantic failure becomes ERANGE, whereas a
right predicate's failure rejects its row. Missing/invalid protocol, stop,
allocation and cap results are not rewritten as operand semantics.

STRING MIN/MAX now compares authentic intern rank and lexical content through
the existing bounded length/byte comparison kernels. A valid intern value
outranks an unresolved ID for both MIN and MAX. Two valid strings compare
lexically, while two unresolved IDs retain signed integer ordering. Unknown
operand domain remains ordinary ID ordering. STRING SUM retains checked signed
ID addition and its existing ERANGE behavior; COUNT and FLOAT arithmetic keep
their prior paths. The aggregate cell retains the raw operator's existing
INT64/FLOAT storage metadata rather than inventing a new result type.

The comparator reads each real intern binding, performs bounded byte scans,
and publishes its outputs only after its last saved-generation check. Its
WORK admissions are not a new SOURCE issuer or a containing carry certificate.
Current parsed AVG requires FLOAT; no integer AVG semantics are introduced.

`evaluation_numeric_operators` now includes generated grouped/ungrouped
MIN/MAX/SUM with shifted intern IDs and reversed inputs, computed CAT and
STRLEN operands, literal values, raw type/content parity and actual public
controlled/raw sessions. Public delivery occurs once, a later empty step
preserves events and ledger bytes, and destruction of session/plan/program
releases the retained governor to zero. Invalid/unresolved IDs, NULL intern,
UNKNOWN domain, overflow, long lexical comparisons and interrupted comparison
or storage have independent component oracles. A natural computed division
failure separately proves ERANGE with no output/events and clean destruction.

A genuine scalar-program-created checkpoint proves cleanup after cancellation,
WORK refusal, subsequent temporary allocation failure, enforcing-cap refusal,
and actual Original FINISH followed by a live successor. It observes the real
context and program instead of seeding owner fields. The old report's seven
fields remain unchanged, the successor consumes zero, and actual cleanup
releases all retained governor credit. Remaining representation, recursive,
materialized, lifecycle and full source/carry rows remain open in the closure
matrix; these tests alone do not complete #2300.


## Canonical observed-child recovery

The ordinary observed transaction now retains the actual current child's full
receipt key before financier callbacks. This key is an identity candidate;
real frame/receipt, operation, head, bindings, generation and native-thread
proofs supply authority. The pending and ACK protocol flags remain separate.
The cached receipt index is validated against all key fields. Receipt scans
poll at each slot inside the independently admitted fixed owner envelope.

The typed observed recovery route consumes genuine pending launch tuples,
redirects neverborn numeric/filter work, and maintains authentic prepared
publication receipts without an ordinary ABORT or any Original/successor
charge. Actual receipt errors take priority over the provisional EINVAL used
for an ended-successful but incomplete operation. Prepared mutable getters
remain EBUSY; their read-only protection is retained through real ACK.

Actual ACK publishes a result journal before financier callbacks. A completed
ACK can return its actual result again only after full key, slot, session
receipt generation and next-ID/epoch validation. Arbitrary FREE/ENOENT never
means completed cleanup. Metadata financial close distinguishes an initial
admission refusal with zero consumption from a stop after admitted work.

`evaluation_observed_owner_recovery` exercises seven authentic recursive
checkpoints: numeric preflight, unary child, framed lookup, pending result,
FREED before parent consume, ACK after slot clear, and prepared publication.
Each has healthy, ended0, live-successor and one-short metadata
histories. Refused host funding is exercised at the six checkpoints with a
remaining host action; post-ACK journal consumption needs no new host grant. Wrong full keys and wrong native threads are rejected before and
after registration. Repeated genuine ACK results, absent STATUS, frozen report
fields, zero successor consumption, nonpublication, exact future events and
zero governor retention are checked using actual sessions and receipts.

This repairs the canonical owner/receipt handoff. It does not certify finite
observer/transaction disposal, RUNNING ROW/resource END/FINISH adapters or the
containing SOURCE/carry proof. Those completion rows remain open in this PR.


## Finite nonstream scalar BEGIN and END

`scalar_source_begin_retained_attempt_controlled` preserves genuine acquired
reader claims when BEGIN fails. Both descriptor and owner acquisition use the
retained kernel; a successful CAS publishes the actual token before a later
refusal can return. Failure saves the actual first cause and enters ENDING,
including failure after HELD or snapshot publication. Shape, gather, clone and
repeated BEGIN then refuse forward access. No owner or acquisition is inferred
from a phase label: initialized token records identify the remaining claims.

The existing BEGIN wrapper keeps its legacy automatic drain. The frame adapter
selects retained BEGIN only for an authenticated nonstream numeric owner with
an actual PENDING READY launch and the current plan operation. Independent
owned primitives may execute a different operation while their numeric owner
is parked; their existing contract remains. A stream branch requires the real
START lease, host, downstream operation, input, generation and schema proof.
Its finite stream BEGIN/END integration remains separately required.

The new scalar END sibling returns protocol independently from the saved
initiating result. It validates source identity, native thread and the saved
Original allocation identity, rejects Original as financier before a charge,
and performs one actual reader-release step or one fixed metadata packet.
ENDING forbids forward access before any decrement. Owner and descriptor
readers retain their real release stages across contention and refused
financial returns. Descriptor data is not dereferenced after its protection
is gone. The first result, including zero, remains frozen. Terminal progress
survives a refused footer and permits a later observation without repeating a
release. A drained query does not dereference the old controller or relation.

The typed filter selector proves NONE, empty left/right or scalar left/right
from actual context, child phase, current operation, saved tuple and original
source record. It performs a genuine saved-financier observation after its
common context proof; failure returns protocol, never a false NONE result.
The action repeats proof. Scalar source initialization remains set until a
successful DRAINED observation. Busy is cleared on every return, and a failed
call preserves the returned status and enclosing cleanup stage.

Dispatch wiring covers normal numeric, composed, registered
JOIN/UNARY/OBSERVED and AUTO cleanup while retaining each owner's existing
admission restrictions. The tests below name actual paths; wiring alone is
not activation evidence. Empty sources call their existing finite relation
leaf after the new complete proof; the old empty adapter is not recursively
reproved. Source END consume checks the child protocol before DRAINED. A
financier failure after terminal physical progress therefore schedules a new
terminal observation; it does not become an Original semantic error or an
implicit success. Ordinary incomplete progress schedules another END instead
of inventing EBUSY. The existing synchronous API remains for its explicit
legacy callers, including stream paths awaiting their actual adapter.

The scalar leaf admits128 per unfinished action; an already DRAINED query
requires no new leaf admission. The selected numeric plan dispatch
admits1 separately; a filter wrapper admits its own128 before the leaf. These
are real separate checks and logical prices, not source-observation upper
bounds. Existing Original INIT256 is unchanged. Its saved allocation/attempt
identity, ending cursor and complete inert reader stages are published before
initialized=true. Actual frame allocation and governor accounting use the
expanded record size. Old unused INIT/END slack is neither refunded nor used
as a new SOURCE promise. Containing source/private-binding profiles and
correlated carry still require separate qualification.

`relation_scalar_source_end` has317 direct histories: real INERT/HELD and
interrupted descriptor/owner/validation/snapshot states; typed, timestamped,
shared-owner and widths0/1/17/257; exact128/one-short127; stopped/stale/copied/
wrong-thread/Original-aliased financiers; actual CAS contention; refused
post-release and terminal footers; Original turnover during BEGIN; legacy
controls and genuine unsupported-descriptor ENOTSUP. Literal values, type and
signed timestamp metadata, snapshots, gate counts and governor baselines are
preserved. `evaluation_numeric_plan` separately asserts the wrapper's255/256
boundary and five actual END actions without Original consumption, plus the
plan-source128/129 dispatch boundary and pending financial refusal.

Actual recursive sessions add12 retained-BEGIN and21 END histories. They prove
that claims remain after interrupted BEGIN, then retire through actual finite
consumers. Numeric, left and right END checkpoints cover healthy, ended0,
live successor, metadata/host refusal and actual CANCEL/WORK after terminal
physical progress. The seven-field old report, successor consumption0,
pre-cutoff heads, future event uniqueness and governor release are checked.
The existing34 canonical-owner histories remain independent regressions.

This unit does not close finite observer/parent disposal, RUNNING ROW/batch,
generic FINISH, stream source adapters, materialized domains or complete
SOURCE/carry and platform qualification. Their closure matrix rows stay OPEN
within the same Draft PR.


### Local scalar-source observation bounds

These are manually expanded written-source upper envelopes: evaluated scalar
predicates, address/arithmetic fronts, stores, actual arguments/calls/results/
returns and native first fronts. They are neither prices, byte counts,
measurements nor strict executed lower witnesses. Controller/thread/native
interiors retain only their existing narrow qualifications. The MSVC wide CAS
and neutral active-ID read C shims are expanded; only the underlying
Interlocked native operation retains its platform boundary.

A **simple parent** below contributes exactly the existing73-cell numeric
return envelope. The no-private-bindings entry uses the existing61-cell
validation profile. Neither profile includes arbitrary static binding loops,
dynamic observed authorization, stream/LATE/public ancestors or SourceRoot.
They need their own complete joins; the tables cannot retrospectively qualify
an earlier long prefix. A stopped zero is not a fresh observation: its full
refused return remains attached to the preceding successful observation.

| Complete local packet | C11/POSIX upper | MSVC upper | Expanded basis |
|---|---:|---:|---|
| INIT entry/admission refusal |32 |32 | Output bootstrap, null guards, real charge/front and refused return |
| Admitted INIT through actual tail observation/local return |160 |176 |46 scalar target/store pairs92 + identity7 + two reader-clear call chains14 + thread2 + continuation1 + tail front6 + expanded tail20/36 =142/158, reserve18 |
| Common source-END owner/context/alias proof to first actual zero |144 |144 | One nonstream getter58 + context/thread32 + arguments/bootstrap/alias/refusal38, reserve16 |
| No-binding numeric entry through that common zero |240 |240 | Existing validation61 + common144 + direct caller fronts28, reserve7 |
| Common zero through actual family proof and next dispatch observation/refusal |224 |240 | Source/child/tuple/op proof160 + selector12 + numeric16 + financier/active-ID28, reserve8; MSVC shim16 |
| Common zero through refused second zero and simple-parent return |248 |248 | Family160 + result/NONE/zero/refusal12 + parent73, reserve3 |
| Scalar END entry through its128 observation/refusal |112 |112 | Header/output geometry32 + thread/identity32 + financing/phase/cursor32 + real charge/refusal16 |
| Scalar END admission to first-cause/ENDING publication and entry zero |48 |48 | Phase/result8 + at most4 meaningful target/store fronts16 + real zero/refusal16, reserve8 |
| Scalar entry zero through one actual reader action to footer zero |160 |176 | Dispatch24 + expanded reader96/112 + result/cursor16 + footer16, reserve8 |
| Scalar already-empty reader to footer |96 |96 | Real empty/stage guards32 + dispatch24 + cursor/footer24, reserve16 |
| Scalar pointer/extent or identity/schema packet to footer |80 |80 | At most11 real metadata targets plus branch/cursor and actual footer fronts |
| Scalar terminal packet to footer |64 |64 | Dispatch10 + four target/store fronts16 + footer16, reserve22 |
| Scalar footer through wrapper publication and simple-parent return |160 |176 | Leaf12 + wrapper55 + parent73, reserve20; conservative MSVC allowance16 |
| New bare wrapper second zero to direct empty leaf charge1 |128 |144 | Source/busy/done/arguments24 + header16 + thread16 + identity/active-ID/phase40 + charge16, reserve16; MSVC shim16 |
| Empty charge1 through one reader CAS and simple-parent return |240 |256 | Leaf dispatch16 + reader96/112 + busy/status/return48 + parent73, reserve7 |
| Empty already-empty reader to terminal admission |64 |64 | Ending/reader10 + inert-token20 + next real admission16, reserve18 |
| Empty terminal admission through nonfinal control release and simple-parent return |192 |208 | Control/clear24 + nonfinal native release16/32 + done/return8 + wrapper48 + parent73, reserve23 |
| Source-END consumption after its funding observation |80 |80 | Protocol-before-DRAINED, partial/terminal phase publication and simple return |

The alias guard uses the **numeric owner's** still-retained Original pointer,
including NONE/FINISH after CONTROL cleared the filter's pointer. It precedes
the first real check. A late regression requests cancellation without
observing it, rejects Original financing, and then finishes with stop NONE;
a separate live-successor query is also rejected with consumption0.

`reader_release_step` expands its real token/stage/thread proof, gate/load/
secondary check, one CAS, physical progress or token clear, and return. Its
portable96 and MSVC112 bounds include the latter's width selection, expected
read, native call/result/comparison and failure writeback. A generic reader
release loop is not hidden inside this bound. The selected empty wrapper
retains an independent context pin, so releasing its empty-source controller
reference is nonfinal; this qualification does not extend to an unrelated
caller without that pin. MSVC reader/refusal packets use their full256
local envelope. Extra ancestors require recounting.

Retained BEGIN has a real Original **forward** observation after complete
frame/context/current-launch selection and before its source-body entry.
It separates metadata proof from the actual BEGIN checks and acquisitions.
It is not executed as native END or used to refresh cleanup permission. Each
failed acquisition preserves only its real token, and the failure journal
blocks forward reuse. Legacy false-mode wrapper deltas and their constructor/
stream callers still require their own profiles; the earlier86 initializer
subtotal and tight old253 comments are not inherited automatically.

The native C11 layout was checked against the old source record:256 becomes
272 bytes, reader40 and thread8. This is layout evidence for that actual
compiler, not a source metric or a POSIX/MSVC size assertion. Existing
sizeof-based frame/host allocations, range geometry and accounted zero walks
use the expanded layout. INIT logical256 stays unchanged; its old legacy END72
bookkeeping is a separate term. In particular176+72 does not certify a common
source interval, create a cleanup credit or settle incoming carry.

These tables qualify local packets only. Full private-binding/public/stream
caller profiles, legacy constructor failure cleanup, actual incoming and
assigned SOURCE promises, performed lower and correlated carry remain required
in S.FIN and the containing closure rows. No test count, zero check, FINISH,
unused reservation or local upper closes those obligations.


## Observed native AUTO FINISH continuation

The canonical observed recovery route retains AUTO selection for its authentic
error drain. The previous blanket native-cleanup exclusion bypassed the
ACKED JOIN's finite FINISH and reached ordinary destruction. The production
prepared-AUTO fixture reproduced this on both CI TSan compilers and locally;
its strict FINISH count remains unchanged. A separate checkpoint now proves
the exact numeric owner/operation enters FINISH after Original ends, with its
initiating CANCEL/WORK result retained.

The new discriminator adds two native/observed flag reads and, only for an
observed native continuation, the existing `auto_error_drain` straight-line
owner/receipt/phase predicate. That predicate has no resource-sized loop;
its actual longest arm includes the embedded numeric owner and filter stage.
It precedes the selector's first genuine funding admission. The selector then
reuses its actual full binding, saved operation/cursor, ACKED receipt and quiet
FINISH checks; publication remains excluded by the saved error. Pending launch,
consume and ACK retain their existing distinct financier contracts.

This added pre-admission source interval must be included in the containing
framed CHILD census, including the predicate's failed arms and ancestor return.
It is not paid by the later selector charge and does not inherit the local
source-END bounds above. No new whole-call256 or assigned SOURCE/carry claim
is made here. The containing S.FIN qualification remains open.


## Canonical abort-observer retirement

The real observed Step now retains an observer disposal journal, the exact
transaction origin and a final-address resource receipt. Transaction malloc
publishes its real creator thread and saved allocation/operation tuple before
the transaction pointer. Cleanup birth proves the actual normal disposal
reason or registered C1 native DISPOSE lineage. Historical ownership does not
come from the current Original generation, a successful financial check or an
external mode flag. The live C1 EVALUATE proof remains strict.

Both actual disposal callers first drive the same observer kernel to its
READERS_DONE barrier, before differential or original relation retirement.
The receipt remains paused while those parent resources retire. The later
adapter resumes that same receipt and replaces the old raw BUILD/PREPARE/READY
discard trio. Each action handles one real relation-reader stage, plain token
stage, physical free, reservation transition, arena hold/GC step or fixed
metadata packet. Partial BUILD uses its actual initialized extent and never
reads an unfinished observer header. Header metadata credit moves to stable
session storage before physical header free; payload arrays precede their
credit release. The session's independent governor/arena lifetime protects the
qualified nonfinal reference releases.

A fresh finite financier attaches to the real Step for each action. Its384
capacity covers registration/proof/leaf schedules; separate128 admissions and
actual reader1 or arena128 admissions retain their own meanings. Normal
observe+step may consume128+128+128. A383 registration refusal consumes256 and
leaves the leaf untouched; no whole384 debit is invented. Pending financier
close remains real ownership if its saved generation no longer matches. An
actual foreign successor is neither charged nor finished by old settlement.
Native metadata2304 is still separate outer work, never observer body credit.

ACK clears at most16 bytes through an explicit counted byte loop per admitted
action. A later, separately admitted fixed packet publishes final inert state
and the actual Step ACK witness before its financial footer. Physical resource
completion is distinct from ACK completion and from successful return
observation. The normal DISCARD continuation remains structurally pending
after successful observer ACK until the actual parent transaction is freed.
Its native-thread mutation proof is separate from pending visibility. Parent
arrays cannot retire with an unacknowledged observer; borrowed transaction and
receipt references are invalidated in the mandatory header-free packet.
Raw observer mutators refuse the live journal, including the post-ACK parent
tail. A copied resource address or Original financing refuses before debit.

The actual production BASELINE is created after EVALUATE children. Tests first
initialize genuine public heads and add a pending insertion so BUILD owns real
readers. The128 public histories exercise partial allocation/initialization,
BUILD name/rows/readers, PREPARE reader/current/events and READY; CANCEL/WORK;
Original END/live successor at birth, barrier, physical rows, ACK, post-ACK and
header-free boundaries; one-short funding, refused ACK footer, explicit and
resolved-default governors and genuine peer reader/governor CAS loss. The peer cases also stop the
financier after real CAS loss. Exact old-head/input snapshots, seven-field
reports, successor consumption0, no premature events, later unique insertion
and signed deletion, checked destroy and a separately held17-byte peer
reservation provide independent content/lifetime oracles. NULL options resolve
a default governor; tests assert its actual distinct selection and separately
retain that real governor through checked destroy to verify its zero balance.
They do not claim a governorless public construction path. The test hook before
credit CAS creates a real peer reservation; it never writes fake counter or
owner states.

### Local source intervals and remaining containing work

The source census expands actual engine helpers and MSVC atomic fronts. The
kernel observes after its full resource quote, before native action, and around
the READERS_DONE barrier. It captures a typed ephemeral pointer to the selected
in-place progress receipt once. A refused return compares only that selected
reader/credit/hold/GC record, with no repeated observer-shape dispatch. A
captured header pointer is never read after its physical free: the comparison
is evaluated only for an incomplete result with unchanged phase and index.
The mandatory native journal remains published even if a subsequent real zero
refuses; that refused check is not treated as a fresh observation.

The explicit ACK byte loop has at most17 tests,16 address additions,16
increments and16 target/store pairs per action. Its local128 source envelope
includes fixed loop/control/footer fronts; final header/ACK publication has a
separate local64 envelope. These are source counts from actual operations,
not derived from bytes, admission price or capacity. They do not include an
arbitrary parent or financial-close return. Reader/CAS/refused-return and
native arena/relation joins require their own actual profiles; a successful
zero on one alternative does not certify a stopped alternative.

This unit establishes abort-observer integration and lifetime behavior. It
**does not qualify the whole parent disposal**: differential tails, recursive
roles, arrays, transaction header credit and old maintenance/controller tails
remain required C2a2 work. Current committed observed FINISH still calls its
old observer-finish/discard path before transaction disposal; preserving its
existing delivered semantics is not proof that this committed path is finite.
Outer snapshot loops, finance-close/parent/public refusal contours, actual
incoming/assigned SOURCE, performed lower and correlated carry remain OPEN.
No384 capacity, zero, FINISH, test count or local upper closes S.FIN. The
containing closure matrix and PR remain open until those runtime routes and
all remaining operator domains are completed and validated.

## Observer financier settlement

The canonical observer journal now retains a distinct close cursor and the
actual seven-field FINISH report. One synchronous Source action performs
FINISH, matching-generation detach, creation-reference release, or saved-value
consumption. FINISH accepts only its own completed generation; detach checks
owner, saved generation, inactivity and completed report in the same mutex.
An active or already-ended foreign successor leaves the attachment intact.
The generic controller APIs retain their existing behavior.

Each physical transition publishes its next cursor before BANK settlement.
Release clears the real pointer and saved ID before relinquishing the creation
reference, and subsequent turns read only the saved report. A NULL financier
does not erase the CONSUME/observation debt or permit another grant. A refused
footer retries the remaining stage rather than repeating FINISH, detach or
release. Diagnostic hooks run outside the Source callback and the close busy
guard protects their reentry. The original operation result remains separate
from the private grant's actual stop/protocol result.

The additive internal operator-close issuer uses208/224/80, preserving existing
snapshot-metadata callers. These are real Source coupons, not the observer's
384 Work capacity. The expanded FINISH callback has a local conservative
upper188 on C11/POSIX and196 with MSVC's atomic width dispatch. Its Core body
counts128/136: meaningful result initialization10; argument guards6; mutex
lock front14; owner/generation/active selection9; stop helper9; accounting4;
result selection5; completed report14; active store1; atomic exchange5/13;
kind/valid stores2; returned report14; unlock front14; returned result11.
The callback adds58 for payload/dispatch9, arguments/call6, received result10,
guards16, saved report14, cursor/valid stores2 and return1, then reserve2.
Ordinary FINISH is expanded rather than treated as a Native boundary.

The actual BANK CURRENT-to-CLOSE interval adds27 before callback entry and32
after its return. Thus the complete local interval is at most248/256 on those
platform profiles. DETACH, RELEASE and CONSUME callback envelopes are112,96
and24; their corresponding intervals fit176,160 and88. Report copies count
seven loads and seven stores; object padding is not logical Source work.
These are local production profiles, excluding diagnostic test callbacks.

This census does not qualify bootstrap from the preceding observer leaf,
generic BANK refusal/return ancestry, public return, or simultaneous carry.
In particular it does not borrow the canonical financial component's lower
170/12/90/60 or incoming-promise condition. S.FIN and parent closure remain open.

The public fixture retains the original128 histories and adds20: matching
stopped/already-ended grants plus actual BANK cancellation after each physical
stage and before each action. The real quiescent test reset preserves the
completed BANK report; it never resets Original. All148 histories assert
no new grant while close debt is retained, matching actual FINISH reports,
exactly one diagnostic completion per grant, unchanged Original reports and
successor consumption0, future unique signed events and actual governor
balances. Direct controller tests separately prove live and ended foreign
generation detach refusal, then clean up through the authentic current tuple.
